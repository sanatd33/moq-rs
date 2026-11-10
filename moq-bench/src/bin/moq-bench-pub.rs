// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Publishes timestamped synthetic or trace-replayed media for relay experiments.
//!
//! Each frame is one object; each keyframe starts a new group (one subgroup per
//! group, one QUIC stream per subgroup at the relay). Every payload begins with
//! a [`moq_bench::ObjectHeader`] carrying the wall-clock send time.

use std::net;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use moq_bench::{unix_us, Frame, ObjectHeader, Synthetic, Trace};
use moq_native_ietf::quic;
use moq_transport::{
    coding::TrackNamespace,
    serve::{self, Subgroup, SubgroupWriter},
    session::{Publisher, SessionId},
};
use url::Url;

#[derive(Parser, Clone)]
struct Cli {
    /// Relay URL, e.g. https://relay:4443
    url: Url,

    #[arg(long, default_value = "[::]:0")]
    bind: net::SocketAddr,

    #[command(flatten)]
    tls: moq_native_ietf::tls::Args,

    /// Track namespace to PUBLISH_NAMESPACE (slash separated).
    #[arg(long, default_value = "bench")]
    namespace: String,

    /// Number of tracks, named `<track-prefix>0..N`. Each carries the full stream.
    #[arg(long, default_value_t = 1)]
    tracks: u32,

    #[arg(long, default_value = "video")]
    track_prefix: String,

    /// Replay frame sizes and timing from an ffprobe packet trace instead of
    /// the synthetic model. See `Trace` in moq-bench/src/lib.rs.
    #[arg(long)]
    trace: Option<PathBuf>,

    /// Synthetic model: frames per second.
    #[arg(long, default_value_t = 30.0)]
    fps: f64,

    /// Synthetic model: mean bitrate per track, megabits per second.
    #[arg(long, default_value_t = 4.0)]
    bitrate_mbps: f64,

    /// Synthetic model: seconds between keyframes (one group per GOP).
    #[arg(long, default_value_t = 1.0)]
    gop_secs: f64,

    /// Synthetic model: keyframe size as a multiple of a delta frame.
    #[arg(long, default_value_t = 10.0)]
    key_ratio: f64,

    /// Synthetic model: uniform per-frame size jitter, as a fraction (0.1 = ±10%).
    #[arg(long, default_value_t = 0.0)]
    jitter: f64,

    #[arg(long, default_value_t = 1)]
    seed: u64,

    /// Publisher priority for every subgroup (0 = highest).
    #[arg(long, default_value_t = 128)]
    priority: u8,

    /// Stop after this many seconds. Runs until killed if omitted.
    #[arg(long)]
    duration_secs: Option<f64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,quinn=warn")),
        )
        .init();

    let cli = Cli::parse();
    anyhow::ensure!(cli.tracks > 0, "--tracks must be at least 1");

    let trace = cli.trace.as_deref().map(Trace::load).transpose()?;
    match &trace {
        Some(t) => tracing::info!(mean_mbps = t.mean_bitrate_bps() / 1e6, "replaying trace"),
        None => tracing::info!(
            fps = cli.fps,
            bitrate_mbps = cli.bitrate_mbps,
            "synthetic stream"
        ),
    }

    let tls = cli.tls.load()?;
    let quic = quic::Endpoint::new(quic::Config::new(cli.bind, None, tls)?)?;
    let (session, cid, transport) = quic.client.connect(&cli.url, None).await?;
    tracing::info!(cid, "connected");

    let (session, mut publisher) =
        Publisher::connect_with_session_id(session, SessionId::new(cid), transport)
            .await
            .context("failed to create MoQ Transport session")?;

    let (mut tracks_writer, _, tracks_reader) = serve::Tracks {
        namespace: TrackNamespace::from_utf8_path(&cli.namespace),
    }
    .produce();

    let mut writers = Vec::new();
    for i in 0..cli.tracks {
        let name = format!("{}{}", cli.track_prefix, i);
        let track = tracks_writer
            .create(name.as_str())
            .context("duplicate track")?;
        writers.push(track.subgroups()?);
    }

    let frames: Box<dyn Iterator<Item = Frame> + Send> = match trace {
        Some(trace) => Box::new(trace.into_frames()),
        None => Box::new(
            Synthetic {
                fps: cli.fps,
                bitrate_bps: cli.bitrate_mbps * 1e6,
                gop_secs: cli.gop_secs,
                key_ratio: cli.key_ratio,
                jitter: cli.jitter,
                seed: cli.seed,
            }
            .frames(),
        ),
    };
    let deadline = cli.duration_secs.map(Duration::from_secs_f64);

    tokio::select! {
        res = session.run() => res.context("session error")?,
        res = publisher.publish_namespace(tracks_reader) => res.context("failed to publish namespace")?,
        res = run(writers, frames, cli.priority, deadline) => res?,
    }

    Ok(())
}

/// Pace frames against absolute deadlines so scheduling hiccups do not
/// accumulate drift, writing each frame to every track.
async fn run(
    mut tracks: Vec<serve::SubgroupsWriter>,
    frames: Box<dyn Iterator<Item = Frame> + Send>,
    priority: u8,
    deadline: Option<Duration>,
) -> anyhow::Result<()> {
    let start = tokio::time::Instant::now();
    let mut current: Vec<Option<SubgroupWriter>> = tracks.iter().map(|_| None).collect();
    let mut group_id = 0u64;
    let mut late = 0u64;

    for (seq, frame) in frames.enumerate() {
        if deadline.is_some_and(|d| frame.offset >= d) {
            break;
        }

        let due = start + frame.offset;
        let now = tokio::time::Instant::now();
        if due > now {
            tokio::time::sleep_until(due).await;
        } else if now - due > Duration::from_millis(5) {
            late += 1;
            if late.is_power_of_two() {
                tracing::warn!(
                    late,
                    behind_ms = (now - due).as_millis() as u64,
                    "publisher running late"
                );
            }
        }

        if frame.key || current[0].is_none() {
            for (track, slot) in tracks.iter_mut().zip(current.iter_mut()) {
                // Dropping the previous writer ends that subgroup.
                *slot = Some(track.create(Subgroup {
                    group_id,
                    subgroup_id: 0,
                    priority,
                })?);
            }
            group_id += 1;
        }

        let send_unix_us = unix_us();
        for (i, slot) in current.iter_mut().enumerate() {
            let header = ObjectHeader {
                track: i as u32,
                seq: seq as u64,
                send_unix_us,
            };
            if let Some(writer) = slot {
                writer.write(header.payload(frame.size))?;
            }
        }
    }

    tracing::info!(groups = group_id, late, "publisher finished");
    Ok(())
}
