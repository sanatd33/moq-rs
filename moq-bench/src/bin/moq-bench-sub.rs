// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Runs many independent subscriber sessions against a relay and records
//! per-object latency, per-interval throughput, and subscription terminations.
//!
//! Each session has its own QUIC connection. A configurable fraction of
//! sessions drains slowly (rate-limited, intermittently paused, or stalled) by
//! pacing reads inside the transport, so QUIC flow control pushes back on the
//! relay exactly as a slow client would.
//!
//! Outputs in `--out-dir`, prefixed with `--label`:
//! - `<label>-objects.csv`: one row per received object.
//! - `<label>-intervals.jsonl`: per-session throughput and latency percentiles.
//! - `<label>-events.jsonl`: connects, subscription results and terminations.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::net;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use moq_bench::{is_slow, unix_us, DrainMode, ObjectHeader};
use moq_native_ietf::quic;
use moq_transport::{
    coding::TrackNamespace,
    instrument::TokenBucket,
    serve::{self, ServeError, TrackReader, TrackReaderMode},
    session::{SessionId, Subscriber},
};
use serde::Serialize;
use url::Url;

#[derive(Parser, Clone)]
struct Cli {
    /// Relay URL, e.g. https://relay:4443
    url: Url,

    #[arg(long, default_value = "[::]:0")]
    bind: net::SocketAddr,

    #[command(flatten)]
    tls: moq_native_ietf::tls::Args,

    /// Number of subscriber sessions in this process.
    #[arg(long, default_value_t = 1)]
    sessions: u64,

    /// Global index of this process's first session. Give each subscriber
    /// node a disjoint range so slow-session selection and session ids are
    /// consistent across nodes.
    #[arg(long, default_value_t = 0)]
    index_offset: u64,

    /// Sessions sharing one UDP socket / quinn endpoint.
    #[arg(long, default_value_t = 1)]
    sessions_per_endpoint: u64,

    /// Delay between session starts, to avoid a handshake storm.
    #[arg(long, default_value_t = 10)]
    ramp_ms: u64,

    #[arg(long, default_value = "bench")]
    namespace: String,

    #[arg(long, default_value = "video")]
    track_prefix: String,

    /// Tracks each normal session subscribes to (`<prefix>0..N`).
    #[arg(long, default_value_t = 1)]
    tracks: u32,

    /// Fraction of sessions that subscribe to `--heavy-tracks` tracks instead.
    #[arg(long, default_value_t = 0.0)]
    heavy_fraction: f64,

    #[arg(long, default_value_t = 8)]
    heavy_tracks: u32,

    /// Fraction of sessions that use `--slow-mode`.
    #[arg(long, default_value_t = 0.0)]
    slow_fraction: f64,

    /// normal | rate:<mbps> | pause:<on_ms>:<off_ms> | stall:<after_ms>[:<duration_ms>]
    #[arg(long, default_value = "normal")]
    slow_mode: DrainMode,

    /// Connection-level QUIC receive window in bytes (quinn default if unset).
    #[arg(long)]
    recv_window_bytes: Option<u64>,

    /// Per-stream QUIC receive window in bytes (quinn default if unset).
    #[arg(long)]
    stream_recv_window_bytes: Option<u64>,

    /// Re-subscribe this long after a subscription is terminated by the relay.
    /// Never re-subscribes if unset.
    #[arg(long)]
    resubscribe_ms: Option<u64>,

    /// Stop after this many seconds. Runs until Ctrl-C if omitted.
    #[arg(long)]
    duration_secs: Option<f64>,

    #[arg(long, default_value = "bench-out")]
    out_dir: PathBuf,

    /// Prefix for output files; defaults to `sub<index_offset>`.
    #[arg(long)]
    label: Option<String>,

    #[arg(long, default_value_t = 1000)]
    stats_interval_ms: u64,

    /// Skip the per-object CSV (it is ~30 rows/s per session-track).
    #[arg(long)]
    no_object_log: bool,
}

#[derive(Default)]
struct Interval {
    bytes: u64,
    objects: u64,
    latency_us: Vec<i64>,
    max_seq: Option<u64>,
}

struct SessionStats {
    index: u64,
    mode: DrainMode,
    tracks: u32,
    cid: Mutex<Option<String>>,
    interval: Mutex<Interval>,
    total_bytes: AtomicU64,
    total_objects: AtomicU64,
    terminations: AtomicU64,
}

impl SessionStats {
    fn record(&self, bytes: u64, latency_us: i64, seq: u64) {
        self.total_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.total_objects.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut i) = self.interval.lock() {
            i.bytes += bytes;
            i.objects += 1;
            i.latency_us.push(latency_us);
            i.max_seq = Some(i.max_seq.map_or(seq, |m| m.max(seq)));
        }
    }
}

#[derive(Serialize)]
struct IntervalRow<'a> {
    unix_ms: u64,
    session: u64,
    cid: Option<String>,
    mode: String,
    tracks: u32,
    bytes: u64,
    objects: u64,
    mbps: f64,
    latency_p50_us: Option<i64>,
    latency_p99_us: Option<i64>,
    latency_max_us: Option<i64>,
    max_seq: Option<u64>,
    total_bytes: u64,
    total_objects: u64,
    terminations: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    object_log_dropped: Option<&'a u64>,
}

#[derive(Serialize)]
struct Event<'a> {
    unix_ms: u64,
    session: u64,
    event: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    track: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
    /// PUBLISH_DONE / REQUEST_ERROR code when the relay closed a subscription.
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<u64>,
}

struct Events(Mutex<BufWriter<File>>);

impl Events {
    fn log(
        &self,
        session: u64,
        event: &str,
        track: Option<&str>,
        detail: Option<String>,
        code: Option<u64>,
    ) {
        let row = Event {
            unix_ms: unix_us() / 1000,
            session,
            event,
            track,
            detail,
            code,
        };
        if let (Ok(line), Ok(mut w)) = (serde_json::to_string(&row), self.0.lock()) {
            let _ = writeln!(w, "{line}");
            let _ = w.flush();
        }
    }
}

struct ObjectRow {
    session: u64,
    track: u32,
    group: u64,
    seq: u64,
    size: usize,
    send_us: u64,
    recv_us: u64,
}

struct Ctx {
    cli: Cli,
    events: Events,
    objects: Option<mpsc::SyncSender<ObjectRow>>,
    objects_dropped: AtomicU64,
}

impl Ctx {
    fn log_object(&self, row: ObjectRow) {
        if let Some(tx) = &self.objects {
            if tx.try_send(row).is_err() {
                self.objects_dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn,quinn=warn")),
        )
        .init();

    let cli = Cli::parse();
    anyhow::ensure!(cli.sessions > 0, "--sessions must be at least 1");
    anyhow::ensure!(
        cli.sessions_per_endpoint > 0,
        "--sessions-per-endpoint must be at least 1"
    );

    std::fs::create_dir_all(&cli.out_dir)?;
    let label = cli
        .label
        .clone()
        .unwrap_or_else(|| format!("sub{}", cli.index_offset));
    let path = |suffix: &str| cli.out_dir.join(format!("{label}-{suffix}"));

    let events = Events(Mutex::new(BufWriter::new(File::create(path(
        "events.jsonl",
    ))?)));

    let (objects, object_writer) = if cli.no_object_log {
        (None, None)
    } else {
        let (tx, rx) = mpsc::sync_channel::<ObjectRow>(1 << 16);
        let file = File::create(path("objects.csv"))?;
        let handle = std::thread::spawn(move || -> std::io::Result<()> {
            let mut w = BufWriter::with_capacity(1 << 20, file);
            writeln!(w, "session,track,group,seq,size,send_us,recv_us")?;
            for r in rx {
                writeln!(
                    w,
                    "{},{},{},{},{},{},{}",
                    r.session, r.track, r.group, r.seq, r.size, r.send_us, r.recv_us
                )?;
            }
            w.flush()
        });
        (Some(tx), Some(handle))
    };

    let ctx = Arc::new(Ctx {
        cli: cli.clone(),
        events,
        objects,
        objects_dropped: AtomicU64::new(0),
    });

    let mut quic_config = quic::Config::new(cli.bind, None, cli.tls.load()?)?;
    if cli.recv_window_bytes.is_some() || cli.stream_recv_window_bytes.is_some() {
        // quinn defaults: 1.25 MB stream window, connection window effectively unbounded.
        let stream = cli.stream_recv_window_bytes.unwrap_or(1_250_000);
        let connection = cli.recv_window_bytes.unwrap_or(u32::MAX as u64);
        quic_config =
            quic_config.with_client_receive_windows(quic::ReceiveWindows { connection, stream });
    }
    let tls = quic_config.tls.clone();
    let windows = quic_config.client_receive_windows;

    let mut stats = Vec::new();
    let mut sessions = FuturesUnordered::new();
    let mut aborts = Vec::new();
    let mut client = None;
    let mut first = Some(quic_config);

    for local in 0..cli.sessions {
        if local % cli.sessions_per_endpoint == 0 {
            let config = match first.take() {
                Some(c) => c,
                None => {
                    let mut c = quic::Config::new(cli.bind, None, tls.clone())?;
                    c.client_receive_windows = windows;
                    c
                }
            };
            client = Some(quic::Endpoint::new(config)?.client);
        }
        let Some(client) = client.clone() else {
            anyhow::bail!("no QUIC endpoint");
        };

        let index = cli.index_offset + local;
        let mode = if is_slow(index, cli.slow_fraction) {
            cli.slow_mode
        } else {
            DrainMode::Normal
        };
        let tracks = if is_heavy(index, cli.heavy_fraction) {
            cli.heavy_tracks
        } else {
            cli.tracks
        };
        let s = Arc::new(SessionStats {
            index,
            mode,
            tracks,
            cid: Mutex::new(None),
            interval: Default::default(),
            total_bytes: AtomicU64::new(0),
            total_objects: AtomicU64::new(0),
            terminations: AtomicU64::new(0),
        });
        stats.push(s.clone());

        let ctx = ctx.clone();
        let handle = tokio::spawn(async move {
            if let Err(err) = run_session(ctx.clone(), client, s).await {
                ctx.events
                    .log(index, "session_error", None, Some(format!("{err:#}")), None);
            }
        });
        aborts.push(handle.abort_handle());
        sessions.push(handle);

        if cli.ramp_ms > 0 {
            tokio::time::sleep(Duration::from_millis(cli.ramp_ms)).await;
        }
    }

    let interval_path = path("intervals.jsonl");
    let stats_task = {
        let ctx = ctx.clone();
        let stats = stats.clone();
        tokio::spawn(write_intervals(
            ctx,
            stats,
            interval_path,
            cli.stats_interval_ms,
        ))
    };

    let stop = async {
        match cli.duration_secs {
            Some(secs) => tokio::time::sleep(Duration::from_secs_f64(secs)).await,
            None => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    };
    tokio::select! {
        _ = stop => {},
        _ = async { while sessions.next().await.is_some() {} } => {},
    }

    // Stop sessions first: their tasks hold the object log sender, and the
    // writer thread only exits once every sender is gone.
    for abort in &aborts {
        abort.abort();
    }
    while sessions.next().await.is_some() {}

    // Final interval so totals reach disk, then close the object log.
    stats_task.abort();
    let _ = stats_task.await;
    let mut final_writer = BufWriter::new(
        std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(path("intervals.jsonl"))?,
    );
    flush_intervals(&ctx, &stats, &mut final_writer)?;
    final_writer.flush()?;

    let dropped = ctx.objects_dropped.load(Ordering::Relaxed);
    if dropped > 0 {
        tracing::warn!(dropped, "object log rows dropped; disk could not keep up");
    }
    drop(ctx);
    if let Some(handle) = object_writer {
        match handle.join() {
            Ok(res) => res?,
            Err(_) => anyhow::bail!("object log writer panicked"),
        }
    }

    Ok(())
}

/// Heavy sessions use a different stride than slow ones so the two sets are
/// not identical when both fractions are equal.
fn is_heavy(index: u64, fraction: f64) -> bool {
    is_slow(index.wrapping_mul(7919) % 1_000_003, fraction)
}

async fn write_intervals(
    ctx: Arc<Ctx>,
    stats: Vec<Arc<SessionStats>>,
    path: PathBuf,
    interval_ms: u64,
) -> anyhow::Result<()> {
    let mut w = BufWriter::new(File::create(path)?);
    let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms.max(10)));
    ticker.tick().await;
    loop {
        ticker.tick().await;
        flush_intervals(&ctx, &stats, &mut w)?;
        w.flush()?;
    }
}

fn flush_intervals(
    ctx: &Ctx,
    stats: &[Arc<SessionStats>],
    w: &mut impl Write,
) -> anyhow::Result<()> {
    static LAST: Mutex<Option<u64>> = Mutex::new(None);
    let now_ms = unix_us() / 1000;
    let elapsed_s = {
        let mut last = LAST.lock().map_err(|_| anyhow::anyhow!("poisoned"))?;
        let prev = last.replace(now_ms);
        prev.map_or(ctx.cli.stats_interval_ms as f64 / 1000.0, |p| {
            (now_ms.saturating_sub(p) as f64 / 1000.0).max(0.001)
        })
    };
    let dropped = ctx.objects_dropped.load(Ordering::Relaxed);

    for s in stats {
        let mut i = match s.interval.lock() {
            Ok(mut i) => std::mem::take(&mut *i),
            Err(_) => continue,
        };
        i.latency_us.sort_unstable();
        let pct = |p: f64| -> Option<i64> {
            if i.latency_us.is_empty() {
                return None;
            }
            let idx = ((i.latency_us.len() - 1) as f64 * p).round() as usize;
            i.latency_us.get(idx).copied()
        };
        let row = IntervalRow {
            unix_ms: now_ms,
            session: s.index,
            cid: s.cid.lock().ok().and_then(|c| c.clone()),
            mode: s.mode.to_string(),
            tracks: s.tracks,
            bytes: i.bytes,
            objects: i.objects,
            mbps: i.bytes as f64 * 8.0 / elapsed_s / 1e6,
            latency_p50_us: pct(0.5),
            latency_p99_us: pct(0.99),
            latency_max_us: i.latency_us.last().copied(),
            max_seq: i.max_seq,
            total_bytes: s.total_bytes.load(Ordering::Relaxed),
            total_objects: s.total_objects.load(Ordering::Relaxed),
            terminations: s.terminations.load(Ordering::Relaxed),
            object_log_dropped: (dropped > 0).then_some(&dropped),
        };
        serde_json::to_writer(&mut *w, &row)?;
        w.write_all(b"\n")?;
    }
    Ok(())
}

async fn run_session(
    ctx: Arc<Ctx>,
    client: quic::Client,
    stats: Arc<SessionStats>,
) -> anyhow::Result<()> {
    let cli = &ctx.cli;
    let index = stats.index;

    let (session, cid, transport) = client.connect(&cli.url, None).await?;
    if let Ok(mut c) = stats.cid.lock() {
        *c = Some(cid.clone());
    }
    let (session, subscriber) =
        Subscriber::connect_with_session_id(session, SessionId::new(cid.clone()), transport)
            .await
            .context("MoQT setup failed")?;

    let throttle = install_throttle(&subscriber, stats.mode);
    ctx.events.log(
        index,
        "connected",
        None,
        Some(format!(
            "cid={cid} mode={} tracks={}",
            stats.mode, stats.tracks
        )),
        None,
    );

    let namespace = TrackNamespace::from_utf8_path(&cli.namespace);
    let mut subscriptions = FuturesUnordered::new();
    for t in 0..stats.tracks {
        subscriptions.push(run_subscription(
            ctx.clone(),
            stats.clone(),
            subscriber.clone(),
            namespace.clone(),
            t,
        ));
    }

    tokio::select! {
        res = session.run() => {
            let detail = match res {
                Ok(()) => None,
                Err(err) => Some(err.to_string()),
            };
            ctx.events.log(index, "session_closed", None, detail, None);
        },
        _ = async { while subscriptions.next().await.is_some() {} } => {
            ctx.events.log(index, "all_subscriptions_ended", None, None, None);
        },
        _ = drive_throttle(throttle, stats.mode) => {},
    }
    Ok(())
}

/// Install the session's receive throttle. Returns it when a controller task
/// must toggle it over time.
fn install_throttle(subscriber: &Subscriber, mode: DrainMode) -> Option<Arc<TokenBucket>> {
    let bucket = match mode {
        DrainMode::Normal => return None,
        // 16 KiB of burst lets a rate-limited reader absorb small objects
        // without per-packet stalls while keeping the long-run rate exact.
        DrainMode::Rate { bps } => TokenBucket::new(Some((bps / 8.0) as u64), 16 * 1024),
        DrainMode::Pause { .. } | DrainMode::Stall { .. } => TokenBucket::new(None, 0),
    };
    let bucket = Arc::new(bucket);
    subscriber.set_recv_throttle(bucket.clone());
    Some(bucket)
}

async fn drive_throttle(throttle: Option<Arc<TokenBucket>>, mode: DrainMode) {
    let Some(bucket) = throttle else {
        return std::future::pending().await;
    };
    match mode {
        DrainMode::Pause { on, off } => loop {
            tokio::time::sleep(on).await;
            bucket.set_paused(true);
            tokio::time::sleep(off).await;
            bucket.set_paused(false);
        },
        DrainMode::Stall { after, duration } => {
            tokio::time::sleep(after).await;
            bucket.set_paused(true);
            if let Some(d) = duration {
                tokio::time::sleep(d).await;
                bucket.set_paused(false);
            }
            std::future::pending().await
        }
        DrainMode::Normal | DrainMode::Rate { .. } => std::future::pending().await,
    }
}

async fn run_subscription(
    ctx: Arc<Ctx>,
    stats: Arc<SessionStats>,
    mut subscriber: Subscriber,
    namespace: TrackNamespace,
    track: u32,
) {
    let name = format!("{}{}", ctx.cli.track_prefix, track);
    let index = stats.index;

    loop {
        let (writer, reader) = serve::Track::new(namespace.clone(), name.as_str()).produce();
        // Without this, streams that arrive in a burst (e.g. after a stall)
        // are cancelled locally and tear down the subscription.
        let writer = writer.deliver_all_subgroups();
        let recv = tokio::spawn(recv_track(ctx.clone(), stats.clone(), reader, track));

        let res = subscriber.subscribe(writer).await;
        recv.abort();
        let (event, code) = match &res {
            Ok(()) => ("subscription_done", None),
            Err(ServeError::Closed(code)) => ("subscription_terminated", Some(*code)),
            Err(_) => ("subscription_error", None),
        };
        if code.is_some() {
            stats.terminations.fetch_add(1, Ordering::Relaxed);
        }
        ctx.events.log(
            index,
            event,
            Some(&name),
            res.err().map(|e| e.to_string()),
            code,
        );

        match ctx.cli.resubscribe_ms {
            Some(ms) if code.is_some() => {
                tokio::time::sleep(Duration::from_millis(ms)).await;
                ctx.events
                    .log(index, "resubscribe", Some(&name), None, None);
            }
            _ => return,
        }
    }
}

async fn recv_track(
    ctx: Arc<Ctx>,
    stats: Arc<SessionStats>,
    reader: TrackReader,
    track: u32,
) -> anyhow::Result<()> {
    let mut subgroups = match reader.mode().await? {
        TrackReaderMode::Subgroups(subgroups) => subgroups,
        _ => anyhow::bail!("publisher must use subgroup streams"),
    };

    while let Some(mut subgroup) = subgroups.next().await? {
        let ctx = ctx.clone();
        let stats = stats.clone();
        tokio::spawn(async move {
            let group = subgroup.group_id;
            while let Ok(Some(mut object)) = subgroup.next().await {
                let Ok(payload) = object.read_all().await else {
                    break;
                };
                let recv_us = unix_us();
                let Some(header) = ObjectHeader::parse(&payload) else {
                    continue;
                };
                let latency = recv_us as i64 - header.send_unix_us as i64;
                stats.record(payload.len() as u64, latency, header.seq);
                ctx.log_object(ObjectRow {
                    session: stats.index,
                    track,
                    group,
                    seq: header.seq,
                    size: payload.len(),
                    send_us: header.send_unix_us,
                    recv_us,
                });
            }
        });
    }
    Ok(())
}
