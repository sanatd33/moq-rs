// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Measurement hooks for relay resource-isolation experiments.
//!
//! Everything here is off by default. A relay opts in with [`enable`] (per
//! session/subscription accounting, read with [`snapshot`]) and
//! [`set_send_rate_limit`] (a process-wide send cap). When neither is called the
//! hooks are a single `OnceLock::get()` returning `None`.
//!
//! The relay keeps no explicit per-subscriber queue: a downstream subscription
//! runs one task per in-flight subgroup, and each task holds a reader into the
//! shared subgroup cache. "Queued" bytes are therefore measured as payload
//! bytes present in those cached subgroups that the subscription has not yet
//! handed to QUIC. Bytes inside quinn's send buffers are not visible here.
//!
//! [`TokenBucket`] is also used on the client side to emulate slow receivers;
//! see [`crate::session::Subscriber::set_recv_throttle`].

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use tokio::time::Instant;

use serde::Serialize;
use tokio::sync::watch;

use crate::serve::{SubgroupReader, SubgroupsReader};

static REGISTRY: OnceLock<Registry> = OnceLock::new();
static SEND_LIMIT: OnceLock<TokenBucket> = OnceLock::new();

/// Turn on per-session accounting. Idempotent.
pub fn enable() {
    REGISTRY.get_or_init(Registry::default);
}

/// Cap the aggregate rate at which all downstream subgroup payload is handed
/// to QUIC, in bytes per second. Waiters are served in FIFO order.
///
/// Returns false if a limit was already installed.
pub fn set_send_rate_limit(bytes_per_sec: u64, burst_bytes: u64) -> bool {
    SEND_LIMIT
        .set(TokenBucket::new(Some(bytes_per_sec), burst_bytes))
        .is_ok()
}

/// The process-wide send limiter, if installed.
pub(crate) fn send_limit() -> Option<&'static TokenBucket> {
    SEND_LIMIT.get()
}

/// Control state for a [`TokenBucket`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ThrottleCtl {
    /// Bytes per second; `None` means unlimited.
    pub rate: Option<u64>,
    /// Maximum accumulated credit in bytes.
    pub burst: u64,
    /// While true, `acquire` blocks regardless of credit.
    pub paused: bool,
}

#[derive(Debug)]
struct BucketState {
    tokens: f64,
    last: Instant,
}

/// A token bucket whose waiters are served in FIFO order.
///
/// A request larger than the available credit is admitted once credit is
/// non-negative and drives the balance negative, so oversized objects are
/// delayed by their full cost without needing a burst that fits them.
#[derive(Debug)]
pub struct TokenBucket {
    // tokio's Mutex is fair, which gives FIFO admission across waiters.
    state: tokio::sync::Mutex<BucketState>,
    ctl: watch::Sender<ThrottleCtl>,
    waited_ns: AtomicU64,
}

impl TokenBucket {
    pub fn new(rate: Option<u64>, burst: u64) -> Self {
        let (ctl, _) = watch::channel(ThrottleCtl {
            rate,
            burst,
            paused: false,
        });
        Self {
            state: tokio::sync::Mutex::new(BucketState {
                tokens: burst as f64,
                last: Instant::now(),
            }),
            ctl,
            waited_ns: AtomicU64::new(0),
        }
    }

    pub fn ctl(&self) -> ThrottleCtl {
        *self.ctl.borrow()
    }

    pub fn set_rate(&self, rate: Option<u64>) {
        self.ctl.send_modify(|c| c.rate = rate);
    }

    pub fn set_paused(&self, paused: bool) {
        self.ctl.send_modify(|c| c.paused = paused);
    }

    /// Total time callers spent blocked in [`acquire`](Self::acquire).
    pub fn waited(&self) -> Duration {
        Duration::from_nanos(self.waited_ns.load(Ordering::Relaxed))
    }

    /// Wait until `n` bytes may pass.
    pub async fn acquire(&self, n: usize) {
        let start = Instant::now();
        let mut state = self.state.lock().await;
        let mut ctl_rx = self.ctl.subscribe();

        loop {
            let ctl = *ctl_rx.borrow_and_update();
            let now = Instant::now();
            let elapsed = now.duration_since(state.last).as_secs_f64();
            state.last = now;

            if ctl.paused {
                // Credit does not accrue while paused.
                let _ = ctl_rx.changed().await;
                continue;
            }

            let Some(rate) = ctl.rate.filter(|r| *r > 0) else {
                break;
            };

            state.tokens = (state.tokens + elapsed * rate as f64).min(ctl.burst as f64);
            if state.tokens >= 0.0 {
                state.tokens -= n as f64;
                break;
            }

            let wait = Duration::from_secs_f64(-state.tokens / rate as f64);
            tokio::select! {
                _ = tokio::time::sleep(wait) => {},
                _ = ctl_rx.changed() => {},
            }
        }

        let waited = start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.waited_ns.fetch_add(waited, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct Registry {
    next_id: AtomicU64,
    sessions: Mutex<HashMap<String, Arc<SessionCounters>>>,
    subscriptions: Mutex<HashMap<u64, Arc<SubscriptionProbe>>>,
}

#[derive(Default)]
struct SessionCounters {
    bytes_sent: AtomicU64,
    objects_sent: AtomicU64,
    subgroups_opened: AtomicU64,
    too_far_behind: AtomicU64,
}

struct SubscriptionProbe {
    session_id: String,
    track: String,
    counters: Arc<SessionCounters>,
    /// Live edge of the cached track, for lag measurement.
    edge: SubgroupsReader,
    subgroups: Mutex<HashMap<u64, SubgroupEntry>>,
}

struct SubgroupEntry {
    group_id: u64,
    reader: SubgroupReader,
    written: Arc<AtomicU64>,
}

/// Registered for the lifetime of one downstream subscription's
/// `serve_subgroups` loop. Deregisters on drop.
pub(crate) struct SubscriptionGuard {
    id: u64,
    probe: Arc<SubscriptionProbe>,
}

impl SubscriptionGuard {
    pub(crate) fn new(session_id: &str, edge: &SubgroupsReader) -> Option<Self> {
        let registry = REGISTRY.get()?;
        let counters = registry
            .sessions
            .lock()
            .ok()?
            .entry(session_id.to_string())
            .or_default()
            .clone();
        let probe = Arc::new(SubscriptionProbe {
            session_id: session_id.to_string(),
            track: format!("{}/{}", edge.namespace, edge.name),
            counters,
            edge: edge.clone(),
            subgroups: Default::default(),
        });
        let id = registry.next_id.fetch_add(1, Ordering::Relaxed);
        registry
            .subscriptions
            .lock()
            .ok()?
            .insert(id, probe.clone());
        Some(Self { id, probe })
    }

    /// Track one in-flight subgroup task.
    pub(crate) fn subgroup(&self, reader: &SubgroupReader) -> Option<SubgroupGuard> {
        let registry = REGISTRY.get()?;
        let id = registry.next_id.fetch_add(1, Ordering::Relaxed);
        let written = Arc::new(AtomicU64::new(0));
        self.probe.subgroups.lock().ok()?.insert(
            id,
            SubgroupEntry {
                group_id: reader.group_id,
                reader: reader.clone(),
                written: written.clone(),
            },
        );
        self.probe
            .counters
            .subgroups_opened
            .fetch_add(1, Ordering::Relaxed);
        Some(SubgroupGuard {
            id,
            probe: self.probe.clone(),
            written,
        })
    }

    pub(crate) fn too_far_behind(&self) {
        self.probe
            .counters
            .too_far_behind
            .fetch_add(1, Ordering::Relaxed);
    }
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        if let Some(registry) = REGISTRY.get() {
            if let Ok(mut subs) = registry.subscriptions.lock() {
                subs.remove(&self.id);
            }
        }
    }
}

/// Registered for the lifetime of one subgroup serving task.
pub(crate) struct SubgroupGuard {
    id: u64,
    probe: Arc<SubscriptionProbe>,
    written: Arc<AtomicU64>,
}

impl SubgroupGuard {
    pub(crate) fn wrote(&self, bytes: usize) {
        self.written.fetch_add(bytes as u64, Ordering::Relaxed);
        self.probe
            .counters
            .bytes_sent
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn object_done(&self) {
        self.probe
            .counters
            .objects_sent
            .fetch_add(1, Ordering::Relaxed);
    }
}

impl Drop for SubgroupGuard {
    fn drop(&mut self) {
        if let Ok(mut subgroups) = self.probe.subgroups.lock() {
            subgroups.remove(&self.id);
        }
    }
}

/// One downstream session's state at a sample instant.
#[derive(Debug, Clone, Default, Serialize)]
pub struct SessionSnapshot {
    pub session_id: String,
    /// Cumulative subgroup payload bytes handed to QUIC.
    pub bytes_sent: u64,
    pub objects_sent: u64,
    pub subgroups_opened: u64,
    /// Subscriptions ended because the FIN-ACK budget was exhausted.
    pub too_far_behind: u64,
    pub subscriptions: u64,
    pub subgroups_inflight: u64,
    /// Cached payload bytes of in-flight subgroups not yet handed to QUIC.
    pub queued_bytes: u64,
    /// Cached payload bytes referenced by in-flight subgroups (shared bytes
    /// counted once per referencing session).
    pub referenced_bytes: u64,
    /// Max over subscriptions of (live-edge group − oldest in-flight group).
    pub max_lag_groups: u64,
    /// Tracks this session is subscribed to.
    pub tracks: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Snapshot {
    /// Unix time in milliseconds.
    pub unix_ms: u64,
    /// Cached payload bytes referenced by any in-flight subgroup, counting
    /// each shared cache entry once.
    pub unique_referenced_bytes: u64,
    /// Sum of per-session `queued_bytes`.
    pub total_queued_bytes: u64,
    /// Cumulative time senders waited on the send limiter, if installed.
    pub send_limit_wait_ms: Option<u64>,
    pub sessions: Vec<SessionSnapshot>,
}

/// Sample current state. Returns `None` unless [`enable`] was called.
///
/// Sessions that have ended keep reporting their cumulative counters with
/// zero in-flight state, so per-run totals stay complete.
pub fn snapshot() -> Option<Snapshot> {
    let registry = REGISTRY.get()?;
    let unix_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default();

    let probes: Vec<Arc<SubscriptionProbe>> = registry
        .subscriptions
        .lock()
        .ok()?
        .values()
        .cloned()
        .collect();
    let counters: Vec<(String, Arc<SessionCounters>)> = registry
        .sessions
        .lock()
        .ok()?
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    let mut sessions: HashMap<String, SessionSnapshot> = counters
        .into_iter()
        .map(|(id, c)| {
            let snap = SessionSnapshot {
                session_id: id.clone(),
                bytes_sent: c.bytes_sent.load(Ordering::Relaxed),
                objects_sent: c.objects_sent.load(Ordering::Relaxed),
                subgroups_opened: c.subgroups_opened.load(Ordering::Relaxed),
                too_far_behind: c.too_far_behind.load(Ordering::Relaxed),
                ..Default::default()
            };
            (id, snap)
        })
        .collect();

    let mut unique: HashMap<usize, u64> = HashMap::new();
    let mut total_queued = 0;

    for probe in probes {
        // Copy out under the lock; sizing locks each subgroup's cache state.
        let entries: Vec<(u64, SubgroupReader, u64)> = match probe.subgroups.lock() {
            Ok(subgroups) => subgroups
                .values()
                .map(|e| {
                    (
                        e.group_id,
                        e.reader.clone(),
                        e.written.load(Ordering::Relaxed),
                    )
                })
                .collect(),
            Err(_) => continue,
        };

        let snap = sessions
            .entry(probe.session_id.clone())
            .or_insert_with(|| SessionSnapshot {
                session_id: probe.session_id.clone(),
                ..Default::default()
            });
        snap.subscriptions += 1;
        snap.tracks.push(probe.track.clone());
        snap.subgroups_inflight += entries.len() as u64;

        let mut oldest_group = None;
        for (group_id, reader, written) in &entries {
            let buffered = reader.buffered_bytes();
            snap.referenced_bytes += buffered;
            let queued = buffered.saturating_sub(*written);
            snap.queued_bytes += queued;
            total_queued += queued;
            unique.insert(reader.cache_id(), buffered);
            oldest_group = Some(oldest_group.map_or(*group_id, |g: u64| g.min(*group_id)));
        }

        if let (Some(oldest), Some((edge, _))) = (oldest_group, probe.edge.latest()) {
            snap.max_lag_groups = snap.max_lag_groups.max(edge.saturating_sub(oldest));
        }
    }

    let mut sessions: Vec<SessionSnapshot> = sessions.into_values().collect();
    sessions.sort_by(|a, b| a.session_id.cmp(&b.session_id));

    Some(Snapshot {
        unix_ms,
        unique_referenced_bytes: unique.values().sum(),
        total_queued_bytes: total_queued,
        send_limit_wait_ms: send_limit().map(|b| b.waited().as_millis() as u64),
        sessions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn token_bucket_paces_to_rate() {
        // 1000 B/s, no burst: ten 100-byte acquires take ~0.9s after the first.
        let bucket = TokenBucket::new(Some(1000), 0);
        let start = tokio::time::Instant::now();
        for _ in 0..10 {
            bucket.acquire(100).await;
        }
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(850), "{elapsed:?}");
        assert!(elapsed <= Duration::from_millis(1100), "{elapsed:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn token_bucket_unlimited_does_not_wait() {
        let bucket = TokenBucket::new(None, 0);
        let start = tokio::time::Instant::now();
        bucket.acquire(1 << 30).await;
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn token_bucket_pause_blocks_until_resumed() {
        let bucket = Arc::new(TokenBucket::new(None, 0));
        bucket.set_paused(true);

        let b = bucket.clone();
        let task = tokio::spawn(async move { b.acquire(1).await });
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(!task.is_finished());

        bucket.set_paused(false);
        task.await.unwrap();
    }
}
