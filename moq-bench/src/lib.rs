// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Shared pieces of the `moq-bench-pub` / `moq-bench-sub` load generators:
//! the object payload header, the frame-size models, and slow-receiver modes.

use std::path::Path;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use bytes::{BufMut, Bytes, BytesMut};

/// Every object payload starts with this header so the subscriber can compute
/// one-way latency and detect loss without any side channel.
///
/// Layout (big endian): magic `MQB1` (4) | track index u32 | frame sequence
/// u64 | publisher send time, unix µs u64. The rest is zero padding.
pub const HEADER_LEN: usize = 24;
const MAGIC: &[u8; 4] = b"MQB1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectHeader {
    pub track: u32,
    pub seq: u64,
    pub send_unix_us: u64,
}

impl ObjectHeader {
    /// Build a payload of exactly `max(size, HEADER_LEN)` bytes.
    pub fn payload(&self, size: usize) -> Bytes {
        let size = size.max(HEADER_LEN);
        let mut buf = BytesMut::with_capacity(size);
        buf.put_slice(MAGIC);
        buf.put_u32(self.track);
        buf.put_u64(self.seq);
        buf.put_u64(self.send_unix_us);
        buf.resize(size, 0);
        buf.freeze()
    }

    pub fn parse(payload: &[u8]) -> Option<Self> {
        if payload.len() < HEADER_LEN || &payload[..4] != MAGIC {
            return None;
        }
        let u32_at =
            |i: usize| u32::from_be_bytes(payload[i..i + 4].try_into().unwrap_or_default());
        let u64_at =
            |i: usize| u64::from_be_bytes(payload[i..i + 8].try_into().unwrap_or_default());
        Some(Self {
            track: u32_at(4),
            seq: u64_at(8),
            send_unix_us: u64_at(16),
        })
    }
}

pub fn unix_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or_default()
}

/// One frame to publish: when (relative to the start of the stream), how big,
/// and whether it starts a new group.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Frame {
    pub offset: Duration,
    pub size: usize,
    pub key: bool,
}

/// Synthetic 30 fps-style stream with periodic keyframe bursts.
///
/// Each GOP of `fps * gop_secs` frames has one keyframe `key_ratio` times the
/// size of a delta frame, sized so the GOP averages `bitrate_bps`. Optional
/// uniform jitter of ±`jitter` (fraction) is applied per frame from a seeded
/// generator, so runs are repeatable.
#[derive(Debug, Clone)]
pub struct Synthetic {
    pub fps: f64,
    pub bitrate_bps: f64,
    pub gop_secs: f64,
    pub key_ratio: f64,
    pub jitter: f64,
    pub seed: u64,
}

impl Synthetic {
    pub fn frames(&self) -> impl Iterator<Item = Frame> {
        let gop = (self.fps * self.gop_secs).round().max(1.0) as u64;
        let gop_bytes = self.bitrate_bps / 8.0 * (gop as f64 / self.fps);
        let delta = gop_bytes / (gop as f64 - 1.0 + self.key_ratio);
        let key = delta * self.key_ratio;
        let frame_dur = 1.0 / self.fps;
        let jitter = self.jitter.clamp(0.0, 0.99);
        let mut rng = XorShift(self.seed.max(1));

        (0u64..).map(move |i| {
            let is_key = i % gop == 0;
            let base = if is_key { key } else { delta };
            let scale = 1.0 + jitter * (2.0 * rng.next_f64() - 1.0);
            Frame {
                offset: Duration::from_secs_f64(i as f64 * frame_dur),
                size: (base * scale).round().max(HEADER_LEN as f64) as usize,
                key: is_key,
            }
        })
    }
}

/// Frames replayed from an encoded file's packet trace, looping forever.
///
/// The trace is CSV `time_seconds,size_bytes,flags` as printed by
/// `ffprobe -select_streams v:0 -show_entries packet=dts_time,size,flags -of csv=p=0`.
/// Use decode timestamps so frames are sent in decode order. A `K` in flags
/// marks a keyframe, which starts a new group.
#[derive(Debug, Clone)]
pub struct Trace {
    frames: Vec<Frame>,
    period: Duration,
}

impl Trace {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read trace {}", path.display()))?;
        Self::parse(&text)
    }

    pub fn parse(text: &str) -> anyhow::Result<Self> {
        let mut raw = Vec::new();
        for (n, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let mut fields = line.split(',');
            let (Some(time), Some(size)) = (fields.next(), fields.next()) else {
                anyhow::bail!("trace line {}: expected time,size,flags", n + 1);
            };
            // ffprobe prints N/A for packets without a timestamp; skip them.
            let Ok(time) = time.trim().parse::<f64>() else {
                continue;
            };
            let size: usize = size
                .trim()
                .parse()
                .with_context(|| format!("trace line {}: bad size", n + 1))?;
            let key = fields.next().is_some_and(|f| f.contains('K'));
            raw.push((time, size, key));
        }
        anyhow::ensure!(!raw.is_empty(), "trace has no frames");

        let t0 = raw[0].0;
        let frames: Vec<Frame> = raw
            .iter()
            .map(|(t, size, key)| Frame {
                offset: Duration::from_secs_f64((t - t0).max(0.0)),
                size: (*size).max(HEADER_LEN),
                key: *key,
            })
            .collect();

        // Loop period: last offset plus the mean frame interval.
        let last = frames.last().map(|f| f.offset).unwrap_or_default();
        let mean = if frames.len() > 1 {
            last / (frames.len() as u32 - 1)
        } else {
            Duration::from_millis(33)
        };
        Ok(Self {
            frames,
            period: last + mean,
        })
    }

    pub fn into_frames(self) -> impl Iterator<Item = Frame> + Send {
        let n = self.frames.len() as u64;
        (0u64..).map(move |i| {
            let idx = (i % n) as usize;
            let lap = (i / n) as u32;
            let f = self.frames[idx];
            Frame {
                offset: f.offset + self.period * lap,
                // The first frame of every lap starts a group so loops are clean.
                key: f.key || idx == 0,
                ..f
            }
        })
    }

    pub fn mean_bitrate_bps(&self) -> f64 {
        let bytes: usize = self.frames.iter().map(|f| f.size).sum();
        bytes as f64 * 8.0 / self.period.as_secs_f64()
    }
}

struct XorShift(u64);

impl XorShift {
    fn next_f64(&mut self) -> f64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// How a subscriber session drains data.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DrainMode {
    /// Read as fast as possible.
    Normal,
    /// Read at most this many bits per second.
    Rate { bps: f64 },
    /// Alternate `on` of normal reading with `off` of no reading.
    Pause { on: Duration, off: Duration },
    /// Stop reading after `after`; resume after `duration` if given.
    Stall {
        after: Duration,
        duration: Option<Duration>,
    },
}

impl FromStr for DrainMode {
    type Err = anyhow::Error;

    /// `normal` | `rate:<mbps>` | `pause:<on_ms>:<off_ms>` |
    /// `stall:<after_ms>[:<duration_ms>]`
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        let ms = |v: &str| -> anyhow::Result<Duration> {
            Ok(Duration::from_millis(
                v.parse()
                    .with_context(|| format!("bad milliseconds: {v}"))?,
            ))
        };
        match parts.as_slice() {
            ["normal"] => Ok(Self::Normal),
            ["rate", mbps] => {
                let mbps: f64 = mbps.parse().context("bad rate")?;
                anyhow::ensure!(mbps > 0.0, "rate must be positive");
                Ok(Self::Rate {
                    bps: mbps * 1_000_000.0,
                })
            }
            ["pause", on, off] => Ok(Self::Pause {
                on: ms(on)?,
                off: ms(off)?,
            }),
            ["stall", after] => Ok(Self::Stall {
                after: ms(after)?,
                duration: None,
            }),
            ["stall", after, duration] => Ok(Self::Stall {
                after: ms(after)?,
                duration: Some(ms(duration)?),
            }),
            _ => anyhow::bail!(
                "drain mode must be normal | rate:<mbps> | pause:<on_ms>:<off_ms> | stall:<after_ms>[:<duration_ms>]"
            ),
        }
    }
}

impl std::fmt::Display for DrainMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Normal => write!(f, "normal"),
            Self::Rate { bps } => write!(f, "rate:{}", bps / 1_000_000.0),
            Self::Pause { on, off } => write!(f, "pause:{}:{}", on.as_millis(), off.as_millis()),
            Self::Stall {
                after,
                duration: None,
            } => write!(f, "stall:{}", after.as_millis()),
            Self::Stall {
                after,
                duration: Some(d),
            } => write!(f, "stall:{}:{}", after.as_millis(), d.as_millis()),
        }
    }
}

/// Spread `fraction` of sessions evenly over the global index space, so slow
/// sessions are interleaved across subscriber nodes rather than clustered.
pub fn is_slow(global_index: u64, fraction: f64) -> bool {
    let f = fraction.clamp(0.0, 1.0);
    ((global_index + 1) as f64 * f).floor() > (global_index as f64 * f).floor()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trips() {
        let h = ObjectHeader {
            track: 3,
            seq: 42,
            send_unix_us: 1_700_000_000_000_000,
        };
        let p = h.payload(100);
        assert_eq!(p.len(), 100);
        assert_eq!(ObjectHeader::parse(&p), Some(h));
        assert_eq!(h.payload(1).len(), HEADER_LEN);
        assert_eq!(ObjectHeader::parse(b"nope"), None);
    }

    #[test]
    fn synthetic_hits_target_bitrate() {
        let model = Synthetic {
            fps: 30.0,
            bitrate_bps: 4_000_000.0,
            gop_secs: 1.0,
            key_ratio: 10.0,
            jitter: 0.0,
            seed: 1,
        };
        let frames: Vec<Frame> = model.frames().take(300).collect();
        let bytes: usize = frames.iter().map(|f| f.size).sum();
        let bps = bytes as f64 * 8.0 / 10.0;
        assert!((bps - 4_000_000.0).abs() < 4_000.0, "{bps}");
        assert_eq!(frames.iter().filter(|f| f.key).count(), 10);
        assert!(frames[0].size > frames[1].size * 9);
        assert_eq!(frames[30].offset, Duration::from_secs(1));
    }

    #[test]
    fn trace_parses_ffprobe_csv_and_loops() {
        let trace =
            Trace::parse("0.000000,5000,K__\n0.040000,1000,__\nN/A,10,__\n0.080000,1000,__\n")
                .unwrap();
        let frames: Vec<Frame> = trace.into_frames().take(6).collect();
        assert!(frames[0].key && !frames[1].key);
        assert_eq!(frames[3].offset, Duration::from_millis(120));
        assert!(frames[3].key);
    }

    #[test]
    fn drain_modes_parse() {
        assert_eq!("normal".parse::<DrainMode>().unwrap(), DrainMode::Normal);
        assert_eq!(
            "rate:0.5".parse::<DrainMode>().unwrap(),
            DrainMode::Rate { bps: 500_000.0 }
        );
        assert_eq!(
            "stall:1000:200".parse::<DrainMode>().unwrap(),
            DrainMode::Stall {
                after: Duration::from_secs(1),
                duration: Some(Duration::from_millis(200))
            }
        );
        assert!("rate:-1".parse::<DrainMode>().is_err());
        assert!("bogus".parse::<DrainMode>().is_err());
    }

    #[test]
    fn slow_fraction_is_spread_evenly() {
        let slow: Vec<u64> = (0..8).filter(|i| is_slow(*i, 0.25)).collect();
        assert_eq!(slow, vec![3, 7]);
        assert_eq!((0..100).filter(|i| is_slow(*i, 0.0)).count(), 0);
        assert_eq!((0..100).filter(|i| is_slow(*i, 0.5)).count(), 50);
    }
}
