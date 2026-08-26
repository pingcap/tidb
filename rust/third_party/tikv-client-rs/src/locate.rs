//! Dependency-free state from client-go's `internal/locate` package.
//!
//! The public source-shaped cache and sender façades live in `region_cache`,
//! `region_request`, and `tikv`; this module owns their private selector,
//! health, flow-accounting, and slow-score state.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::retry::RetryConfig;

use crate::kv::ReplicaReadType;
use rand::seq::SliceRandom;

pub(crate) const SLOW_SCORE_THRESHOLD: u64 = 80;
const SLOW_SCORE_INITIAL: u64 = 1;
const SLOW_SCORE_MAX: u64 = 100;
const SLOW_SCORE_INITIAL_TIMEOUT_US: u64 = 500_000;
const SLOW_SCORE_MAX_TIMEOUT_US: u64 = 30_000_000;
const SLIDING_WINDOW_SIZE: usize = 10;
const TIKV_SLOW_SCORE_DECAY_PER_SECOND: f64 = 20.0 / 60.0;
const TIKV_SLOW_SCORE_THRESHOLD: i64 = 80;
const TIKV_SLOW_SCORE_UPDATE_INTERVAL: Duration = Duration::from_millis(100);
const TIKV_SLOW_SCORE_ACTIVE_UPDATE_INTERVAL: Duration = Duration::from_secs(15);

/// client-go's private `accessMode`, used to index region-cache stores.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum AccessMode {
    TiKvOnly = 0,
    TiFlashOnly = 1,
    NumAccessMode = 2,
}

impl fmt::Display for AccessMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TiKvOnly => formatter.write_str("TiKvOnly"),
            Self::TiFlashOnly => formatter.write_str("TiFlashOnly"),
            Self::NumAccessMode => write!(formatter, "{}", *self as u8),
        }
    }
}

/// client-go's private `replicaFlowsType` used by store flow accounting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum ReplicaFlowsType {
    ToLeader = 0,
    ToFollower = 1,
    NumReplicaFlowsType = 2,
}

impl fmt::Display for ReplicaFlowsType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ToLeader => formatter.write_str("ToLeader"),
            Self::ToFollower => formatter.write_str("ToFollower"),
            Self::NumReplicaFlowsType => write!(formatter, "{}", *self as u8),
        }
    }
}

/// Fixed-size moving count/value window from `slow_score.go`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct CountSlidingWindow {
    average: u64,
    sum: u64,
    history: Vec<u64>,
}

impl CountSlidingWindow {
    pub(crate) const fn average(&self) -> u64 {
        self.average
    }

    pub(crate) const fn sum(&self) -> u64 {
        self.sum
    }

    /// Appends a value and returns the source relative gradient.
    pub(crate) fn append(&mut self, value: u64) -> f64 {
        let previous_average = self.average;
        if self.history.len() < SLIDING_WINDOW_SIZE {
            self.sum = self.sum.wrapping_add(value);
        } else {
            self.sum = self.sum.wrapping_sub(self.history[0]).wrapping_add(value);
            self.history.remove(0);
        }
        self.history.push(value);
        self.average = self.sum / self.history.len() as u64;
        if previous_average > 0 && value != previous_average {
            (value as f64 - previous_average as f64) / previous_average as f64
        } else {
            1e-6
        }
    }
}

/// Concurrent request counters plus the single-tick moving windows used to
/// derive client-side store slow scores. `record` may run concurrently with
/// another request; the periodic `update` serializes window mutation.
pub(crate) struct SlowScoreStat {
    average_score: AtomicU64,
    average_timecost_us: AtomicU64,
    interval_timecost_us: AtomicU64,
    interval_update_count: AtomicU64,
    timecost_window: Mutex<CountSlidingWindow>,
    update_count_window: Mutex<CountSlidingWindow>,
}

impl Default for SlowScoreStat {
    fn default() -> Self {
        Self {
            average_score: AtomicU64::new(0),
            average_timecost_us: AtomicU64::new(0),
            interval_timecost_us: AtomicU64::new(0),
            interval_update_count: AtomicU64::new(0),
            timecost_window: Mutex::new(CountSlidingWindow::default()),
            update_count_window: Mutex::new(CountSlidingWindow::default()),
        }
    }
}

impl SlowScoreStat {
    pub(crate) fn score(&self) -> u64 {
        self.average_score.load(Ordering::Acquire)
    }

    pub(crate) fn is_slow(&self) -> bool {
        self.score() >= SLOW_SCORE_THRESHOLD
    }

    /// Records one request duration, preserving source microsecond rounding.
    pub(crate) fn record(&self, timecost: Duration) {
        self.interval_update_count.fetch_add(1, Ordering::AcqRel);
        if self.average_timecost_us.load(Ordering::Acquire) == 0 {
            self.average_score
                .store(SLOW_SCORE_INITIAL, Ordering::Release);
            self.average_timecost_us
                .store(SLOW_SCORE_INITIAL_TIMEOUT_US, Ordering::Release);
            self.interval_timecost_us.store(
                u64::try_from(timecost.as_micros()).unwrap_or(u64::MAX),
                Ordering::Release,
            );
            return;
        }
        let timecost_us = u64::try_from(timecost.as_micros()).unwrap_or(u64::MAX);
        if timecost_us >= SLOW_SCORE_MAX_TIMEOUT_US {
            self.average_score.store(SLOW_SCORE_MAX, Ordering::Release);
            return;
        }
        self.interval_timecost_us
            .fetch_add(timecost_us, Ordering::AcqRel);
    }

    /// Performs client-go's periodic slow-score update.
    pub(crate) fn update(&self) {
        if self.average_timecost_us.load(Ordering::Acquire) == 0 {
            self.average_score
                .store(SLOW_SCORE_INITIAL, Ordering::Release);
            self.average_timecost_us
                .store(SLOW_SCORE_INITIAL_TIMEOUT_US, Ordering::Release);
            return;
        }

        let average_timecost = self.average_timecost_us.load(Ordering::Acquire);
        let update_count = self.interval_update_count.load(Ordering::Acquire);
        let interval_timecost = self.interval_timecost_us.load(Ordering::Acquire);
        let (update_gradient, timecost_gradient, window_average) = if update_count > 0 {
            let interval_average = interval_timecost / update_count;
            let update_gradient = self
                .update_count_window
                .lock()
                .unwrap()
                .append(update_count);
            let mut timecost_window = self.timecost_window.lock().unwrap();
            let timecost_gradient = timecost_window.append(interval_average);
            (
                update_gradient,
                timecost_gradient,
                timecost_window.average(),
            )
        } else {
            (1.0, 1.0, self.timecost_window.lock().unwrap().average())
        };

        let average_score = self.average_score.load(Ordering::Acquire);
        let score = if update_gradient + 0.1 <= 1e-9 && timecost_gradient - 0.1 >= 1e-9 {
            let rise_ratio = (timecost_gradient / update_gradient).abs().min(5.43);
            (average_score as f64 * rise_ratio + 1.0)
                .min(SLOW_SCORE_MAX as f64)
                .ceil() as u64
        } else {
            let cost_score = (1.0 + update_gradient.abs())
                .clamp(SLOW_SCORE_INITIAL as f64, 2.71)
                .ceil() as u64;
            if average_score <= SLOW_SCORE_INITIAL + cost_score {
                SLOW_SCORE_INITIAL
            } else {
                average_score - cost_score
            }
        };
        let _ = self.average_score.compare_exchange(
            average_score,
            score,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        let _ = self.average_timecost_us.compare_exchange(
            average_timecost,
            window_average,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        self.interval_timecost_us.store(0, Ordering::Release);
        self.interval_update_count.store(0, Ordering::Release);
    }

    pub(crate) fn mark_already_slow(&self) {
        self.average_score.store(SLOW_SCORE_MAX, Ordering::Release);
    }

    #[cfg(test)]
    fn reset(&mut self) {
        *self = Self {
            average_score: AtomicU64::new(SLOW_SCORE_INITIAL),
            ..Default::default()
        };
    }
}

/// Source-compatible combined client-side and TiKV-reported health state.
///
/// `StoreHealthStatus` is intentionally independent of the future store cache:
/// the owning `internal/locate` transcreation will attach one instance to every
/// resolved store and drive `tick` from its health loop.
pub(crate) struct StoreHealthStatus {
    client_side_slow_score: SlowScoreStat,
    tikv_side_slow_score: Mutex<TikvSideSlowScore>,
    is_slow: AtomicBool,
}

#[derive(Debug, Default)]
struct TikvSideSlowScore {
    has_feedback: bool,
    score: i64,
    last_update: Option<Instant>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct HealthStatusDetail {
    pub(crate) client_side_slow_score: i64,
    pub(crate) tikv_side_slow_score: i64,
}

impl HealthStatusDetail {
    pub(crate) fn is_slow(self) -> bool {
        self.client_side_slow_score >= SLOW_SCORE_THRESHOLD as i64
            || self.tikv_side_slow_score >= TIKV_SLOW_SCORE_THRESHOLD
    }
}

impl Default for StoreHealthStatus {
    fn default() -> Self {
        Self {
            client_side_slow_score: SlowScoreStat::default(),
            tikv_side_slow_score: Mutex::new(TikvSideSlowScore::default()),
            is_slow: AtomicBool::new(false),
        }
    }
}

impl StoreHealthStatus {
    pub(crate) fn is_slow(&self) -> bool {
        self.is_slow.load(Ordering::Acquire)
    }

    pub(crate) fn detail(&self) -> HealthStatusDetail {
        let tikv = self.tikv_side_slow_score.lock().unwrap();
        HealthStatusDetail {
            client_side_slow_score: self.client_side_slow_score.score() as i64,
            tikv_side_slow_score: tikv.score,
        }
    }

    pub(crate) fn record_client_side_latency(&self, timecost: Duration) {
        self.client_side_slow_score.record(timecost);
        self.update_slow_flag();
    }

    pub(crate) fn mark_already_slow(&self) {
        self.client_side_slow_score.mark_already_slow();
        self.update_slow_flag();
    }

    pub(crate) fn needs_active_feedback(&self, now: Instant) -> bool {
        let tikv = self.tikv_side_slow_score.lock().unwrap();
        tikv.has_feedback
            && tikv.score > 1
            && tikv.last_update.is_some_and(|last| {
                now.saturating_duration_since(last) >= TIKV_SLOW_SCORE_ACTIVE_UPDATE_INTERVAL
            })
    }

    /// Source `updateTiKVServerSideSlowScore`; `now` is supplied so callers
    /// and tests can preserve its timing gates deterministically.
    pub(crate) fn record_tikv_slow_score(&self, score: i64, now: Instant) {
        let mut tikv = self.tikv_side_slow_score.lock().unwrap();
        if tikv.score == score {
            if score > 1 {
                tikv.last_update = Some(now);
            }
            drop(tikv);
            self.update_slow_flag();
            return;
        }

        if tikv.last_update.is_some_and(|last| {
            now.saturating_duration_since(last) < TIKV_SLOW_SCORE_UPDATE_INTERVAL
        }) {
            drop(tikv);
            self.update_slow_flag();
            return;
        }
        tikv.has_feedback = true;
        tikv.score = score;
        tikv.last_update = Some(now);
        drop(tikv);
        self.update_slow_flag();
    }

    /// Source periodic `tick` without an active feedback RPC callback. When a
    /// reported score becomes stale, the source decays it linearly to one.
    pub(crate) fn tick(&self, now: Instant) {
        self.client_side_slow_score.update();
        let mut tikv = self.tikv_side_slow_score.lock().unwrap();
        if !tikv.has_feedback || tikv.score <= 1 {
            drop(tikv);
            self.update_slow_flag();
            return;
        }
        let Some(last_update) = tikv.last_update else {
            drop(tikv);
            self.update_slow_flag();
            return;
        };
        let elapsed = now.saturating_duration_since(last_update);
        if elapsed < TIKV_SLOW_SCORE_ACTIVE_UPDATE_INTERVAL {
            drop(tikv);
            self.update_slow_flag();
            return;
        }
        tikv.score =
            ((tikv.score as f64 - TIKV_SLOW_SCORE_DECAY_PER_SECOND * elapsed.as_secs_f64()).round()
                as i64)
                .max(1);
        tikv.last_update = Some(now);
        drop(tikv);
        self.update_slow_flag();
    }

    fn update_slow_flag(&self) {
        self.is_slow
            .store(self.detail().is_slow(), Ordering::Release);
    }
}

const SCORE_NOT_ATTEMPTED: u8 = 1 << 0;
const SCORE_NORMAL_PEER: u8 = 1 << 1;
const SCORE_PREFER_LEADER: u8 = 1 << 2;
const SCORE_LABEL_MATCHES: u8 = 1 << 3;
const SCORE_NOT_SLOW: u8 = 1 << 4;

/// Input snapshot for source `ReplicaSelectMixedStrategy` scoring. Store
/// resolution supplies these facts; keeping it value-based lets selection stay
/// deterministic and independent from cache locks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReplicaLiveness {
    Reachable,
    Unreachable,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReplicaCandidate {
    pub(crate) peer_id: u64,
    pub(crate) is_leader: bool,
    pub(crate) is_learner: bool,
    pub(crate) label_matches: bool,
    pub(crate) is_slow: bool,
    pub(crate) liveness: ReplicaLiveness,
    pub(crate) attempts: u8,
    pub(crate) data_is_not_ready: bool,
}

/// Per-request state kept by client-go's `replicaSelector`. It is distinct
/// from the stable user configuration: retry-plan clones retain it for one
/// logical request, while the next user request starts empty.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[doc(hidden)]
pub struct ReplicaSelectorState {
    attempts: HashMap<u64, u8>,
    attempted_time: HashMap<u64, Duration>,
    deadline_exceeded: HashSet<u64>,
    data_is_not_ready: HashSet<u64>,
    leader_busy_peer_id: Option<u64>,
    leader_busy_count: u8,
    leader_busy_probed: bool,
    suspect_not_leader: HashSet<u64>,
    force_leader: bool,
    no_leader: HashSet<u64>,
    server_busy: HashSet<u64>,
    busy_threshold_disabled: bool,
    pending_backoffs: HashMap<u64, PendingBackoff>,
}

const MAX_REPLICA_ATTEMPTS: u8 = 10;
const MAX_REPLICA_ATTEMPT_TIME: Duration = Duration::from_secs(50);

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingBackoff {
    config: RetryConfig,
    reason: String,
}

impl ReplicaSelectorState {
    pub(crate) fn attempts(&self, peer_id: u64) -> u8 {
        self.attempts.get(&peer_id).copied().unwrap_or_default()
    }

    pub(crate) fn data_is_not_ready(&self, peer_id: u64) -> bool {
        self.data_is_not_ready.contains(&peer_id)
    }

    pub(crate) fn attempted_time(&self, peer_id: u64) -> Duration {
        self.attempted_time
            .get(&peer_id)
            .copied()
            .unwrap_or_default()
    }

    pub(crate) fn record_attempt(&mut self, peer_id: u64) {
        self.attempts
            .entry(peer_id)
            .and_modify(|attempts| *attempts = attempts.saturating_add(1))
            .or_insert(1);
    }

    /// Source records wall-clock RPC duration against both the logical target
    /// and a forwarding proxy. Saturation keeps a pathological duration from
    /// wrapping the selector's 50-second exhaustion boundary.
    pub(crate) fn record_attempted_time(&mut self, peer_id: u64, duration: Duration) {
        self.attempted_time
            .entry(peer_id)
            .and_modify(|elapsed| *elapsed = elapsed.saturating_add(duration))
            .or_insert(duration);
    }

    pub(crate) fn is_exhausted(
        &self,
        peer_id: u64,
        max_attempts: u8,
        max_attempt_time: Option<Duration>,
    ) -> bool {
        self.attempts(peer_id) >= max_attempts
            || max_attempt_time.is_some_and(|limit| self.attempted_time(peer_id) >= limit)
    }

    pub(crate) fn mark_deadline_exceeded(&mut self, peer_id: u64) {
        self.deadline_exceeded.insert(peer_id);
    }

    pub(crate) fn deadline_exceeded(&self, peer_id: u64) -> bool {
        self.deadline_exceeded.contains(&peer_id)
    }

    pub(crate) fn has_deadline_exceeded(&self) -> bool {
        !self.deadline_exceeded.is_empty()
    }

    /// The source leader strategy permits ten sends or fifty cumulative RPC
    /// seconds, unless a selector-local error flag makes the peer unsuitable.
    pub(crate) fn is_leader_candidate(&self, peer_id: u64) -> bool {
        !self.is_exhausted(
            peer_id,
            MAX_REPLICA_ATTEMPTS,
            Some(MAX_REPLICA_ATTEMPT_TIME),
        ) && !self.deadline_exceeded(peer_id)
            && !self.has_no_leader(peer_id)
    }

    /// Source leader selection treats the busy-probe suspicion as a
    /// temporary skip layered on top of ordinary leader eligibility. Keeping
    /// the predicates separate is important because the mixed strategy may
    /// clear the suspicion and restore an otherwise healthy cached leader.
    pub(crate) fn is_leader_selectable(&self, peer_id: u64) -> bool {
        self.is_leader_candidate(peer_id) && !self.should_probe_busy_leader(peer_id)
    }

    pub(crate) fn mark_data_is_not_ready(&mut self, peer_id: u64) {
        self.data_is_not_ready.insert(peer_id);
    }

    pub(crate) fn total_attempts(&self) -> u32 {
        self.attempts
            .values()
            .map(|attempts| u32::from(*attempts))
            .sum()
    }

    /// Source stale reads probe an untried leader on their second selector
    /// attempt. `total_attempts == 1` corresponds to entering that attempt.
    pub(crate) fn should_probe_stale_leader(&self, leader_peer_id: u64) -> bool {
        self.total_attempts() == 1 && self.attempts(leader_peer_id) == 0
    }

    /// Source client-go#2028 workaround: two zero-wait busy replies from the
    /// cached leader make the next leader read probe a follower for a
    /// `NotLeader` hint. The counter is owned by the current cached leader.
    pub(crate) fn record_busy_leader(&mut self, leader_peer_id: u64) {
        if self.leader_busy_probed {
            return;
        }
        if self.leader_busy_peer_id != Some(leader_peer_id) {
            self.leader_busy_peer_id = Some(leader_peer_id);
            self.leader_busy_count = 0;
        }
        self.leader_busy_count = self.leader_busy_count.saturating_add(1);
        if self.leader_busy_count >= 2 {
            self.suspect_not_leader.insert(leader_peer_id);
            self.leader_busy_probed = true;
        }
    }

    pub(crate) fn should_probe_busy_leader(&self, leader_peer_id: u64) -> bool {
        self.suspect_not_leader.contains(&leader_peer_id)
    }

    /// Clears source's selector-local suspicion after every follower probe is
    /// exhausted. The shared region cache remains valid and the old leader is
    /// retried with the ordinary server-busy backoff behavior.
    pub(crate) fn restore_suspect_leader(&mut self, leader_peer_id: u64) -> bool {
        self.suspect_not_leader.remove(&leader_peer_id) && self.is_leader_candidate(leader_peer_id)
    }

    /// A concrete replacement leader switches client-go's selector to
    /// leader-read mode when that peer has not already been exhausted.
    pub(crate) fn record_not_leader(&mut self, target_peer_id: u64, leader_peer_id: u64) {
        self.no_leader.insert(target_peer_id);

        // `replica.onUpdateLeader` gives an exhausted hinted leader one final
        // chance, then clears only the NotLeader/suspect flags.
        if self.is_exhausted(
            leader_peer_id,
            MAX_REPLICA_ATTEMPTS,
            Some(MAX_REPLICA_ATTEMPT_TIME),
        ) {
            self.attempts
                .insert(leader_peer_id, MAX_REPLICA_ATTEMPTS - 1);
            self.attempted_time.remove(&leader_peer_id);
        }
        self.no_leader.remove(&leader_peer_id);
        self.suspect_not_leader.remove(&leader_peer_id);
        if self.is_leader_candidate(leader_peer_id) {
            self.force_leader = true;
        }
    }

    pub(crate) fn should_force_leader(&self, leader_peer_id: u64) -> bool {
        self.force_leader && self.is_leader_selectable(leader_peer_id)
    }

    /// A hintless NotLeader reply marks only the current selector's attempted
    /// peer. After the source scheduling backoff, leader-read selection must
    /// not immediately send to that known non-leader again.
    pub(crate) fn mark_no_leader(&mut self, peer_id: u64) {
        self.no_leader.insert(peer_id);
    }

    pub(crate) fn has_no_leader(&self, peer_id: u64) -> bool {
        self.no_leader.contains(&peer_id)
    }

    /// Source `onRegionNotFound` invalidates the shared region immediately
    /// but gives this selector one chance to retry an untried cached leader.
    /// This differs from flashback, which also clears the busy threshold.
    pub(crate) fn force_leader_after_region_not_found(&mut self, leader_peer_id: u64) -> bool {
        if self.attempts(leader_peer_id) != 0 {
            return false;
        }
        self.force_leader = true;
        true
    }

    pub(crate) fn record_server_busy(&mut self, peer_id: u64) {
        self.server_busy.insert(peer_id);
    }

    pub(crate) fn is_server_busy(&self, peer_id: u64) -> bool {
        self.server_busy.contains(&peer_id)
    }

    pub(crate) fn disable_busy_threshold(&mut self) {
        self.busy_threshold_disabled = true;
    }

    pub(crate) fn busy_threshold_disabled(&self) -> bool {
        self.busy_threshold_disabled
    }

    /// Defers a fast-retry delay until the selector returns to the same
    /// logical store. A newer error for that store replaces the old one.
    pub(crate) fn add_pending_backoff(
        &mut self,
        store_id: u64,
        config: RetryConfig,
        reason: String,
    ) {
        self.pending_backoffs
            .insert(store_id, PendingBackoff { config, reason });
    }

    pub(crate) fn take_pending_backoff(&mut self, store_id: u64) -> Option<(RetryConfig, String)> {
        self.pending_backoffs
            .remove(&store_id)
            .map(|pending| (pending.config, pending.reason))
    }

    /// Source charges one pending delay when selection is exhausted: the
    /// class with the largest base delay wins. It deliberately leaves the map
    /// intact because this selector terminates immediately afterward.
    pub(crate) fn largest_pending_backoff(&self) -> Option<(RetryConfig, String)> {
        self.pending_backoffs
            .values()
            .max_by_key(|pending| pending.config.base_ms)
            .map(|pending| (pending.config, pending.reason.clone()))
    }

    #[cfg(test)]
    pub(crate) fn pending_backoff_count(&self) -> usize {
        self.pending_backoffs.len()
    }

    /// Source `onFlashbackInProgress` abandons a replica read and retries
    /// through the leader without a busy threshold.
    pub(crate) fn force_leader_after_flashback(&mut self) {
        self.force_leader = true;
        self.disable_busy_threshold();
    }

    /// Source stale-read retries switch to a normal replica read once the
    /// leader has already been attempted and is neither timed out nor busy.
    pub(crate) fn should_retry_stale_as_replica(&self, leader_peer_id: u64) -> bool {
        self.attempts(leader_peer_id) > 0
            && !self.deadline_exceeded(leader_peer_id)
            && !self.is_server_busy(leader_peer_id)
    }
}

/// Pure source scoring policy for follower/mixed/prefer-leader reads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MixedReplicaSelection {
    pub(crate) read_type: ReplicaReadType,
    pub(crate) leader_only: bool,
    pub(crate) prefer_leader: bool,
    pub(crate) labels_requested: bool,
}

impl MixedReplicaSelection {
    /// Returns the highest source score and all tied candidates. Callers pick
    /// one tied candidate randomly, as client-go does, after this lock-free
    /// calculation.
    pub(crate) fn highest_scored<'a>(
        self,
        replicas: &'a [ReplicaCandidate],
    ) -> (u8, Vec<&'a ReplicaCandidate>) {
        let mut score = 0;
        let mut selected = Vec::new();
        for replica in replicas {
            let Some(candidate_score) = self.score(replica) else {
                continue;
            };
            if candidate_score > score {
                score = candidate_score;
                selected.clear();
                selected.push(replica);
            } else if candidate_score == score {
                selected.push(replica);
            }
        }
        (score, selected)
    }

    /// Source `ReplicaSelectMixedStrategy.next` tie behavior. The source
    /// deliberately randomizes only candidates with the same highest score.
    pub(crate) fn choose(self, replicas: &[ReplicaCandidate]) -> Option<ReplicaCandidate> {
        let (_, tied) = self.highest_scored(replicas);
        tied.choose(&mut rand::thread_rng()).copied().copied()
    }

    fn score(self, replica: &ReplicaCandidate) -> Option<u8> {
        let max_attempts = if replica.data_is_not_ready && !replica.is_leader {
            2
        } else {
            1
        };
        if replica.liveness == ReplicaLiveness::Unreachable
            || replica.attempts >= max_attempts
            || (self.leader_only && !replica.is_leader)
        {
            return None;
        }
        // Source's prefer-leader compatibility rule excludes slow followers.
        if self.prefer_leader && !replica.is_leader && replica.is_slow {
            return None;
        }

        let mut score = 0;
        if replica.label_matches {
            score |= SCORE_LABEL_MATCHES;
        }
        if replica.is_leader {
            if self.prefer_leader {
                if replica.is_slow {
                    score |= SCORE_NORMAL_PEER;
                } else {
                    score |= SCORE_PREFER_LEADER;
                }
            } else if matches!(
                self.read_type,
                ReplicaReadType::Mixed | ReplicaReadType::PreferLeader
            ) {
                if self.labels_requested {
                    score |= SCORE_PREFER_LEADER;
                } else {
                    score |= SCORE_NORMAL_PEER;
                }
            }
        } else if !matches!(self.read_type, ReplicaReadType::Learner) || replica.is_learner {
            score |= SCORE_NORMAL_PEER;
        }
        if !replica.is_slow {
            score |= SCORE_NOT_SLOW;
        }
        if replica.attempts == 0 {
            score |= SCORE_NOT_ATTEMPTED;
        }
        Some(score)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_access_mode_and_replica_flow_names_are_stable() {
        assert_eq!(AccessMode::TiKvOnly.to_string(), "TiKvOnly");
        assert_eq!(AccessMode::TiFlashOnly.to_string(), "TiFlashOnly");
        assert_eq!(AccessMode::NumAccessMode.to_string(), "2");
        assert_eq!(ReplicaFlowsType::ToLeader.to_string(), "ToLeader");
        assert_eq!(ReplicaFlowsType::ToFollower.to_string(), "ToFollower");
        assert_eq!(ReplicaFlowsType::NumReplicaFlowsType.to_string(), "2");
    }

    #[test]
    fn source_sliding_window_keeps_the_last_ten_values() {
        let mut window = CountSlidingWindow::default();
        assert_eq!(window.append(10), 1e-6);
        assert_eq!(window.average(), 10);
        assert_eq!(window.append(20), 1.0);
        for value in 30..=110 {
            window.append(value);
        }
        assert_eq!(window.sum(), (101u64..=110).sum::<u64>());
        assert_eq!(window.average(), (101..=110).sum::<u64>() / 10);
    }

    #[test]
    fn source_slow_score_rises_when_request_rate_drops_and_latency_rises() {
        let stat = SlowScoreStat::default();
        for _ in 0..10 {
            stat.record(Duration::from_micros(100));
        }
        stat.update();
        assert_eq!(stat.score(), SLOW_SCORE_INITIAL);

        stat.record(Duration::from_micros(1_000));
        stat.update();
        assert_eq!(stat.score(), 7);

        stat.mark_already_slow();
        assert!(stat.is_slow());
    }

    #[test]
    fn source_too_slow_request_forces_the_maximum_score() {
        let mut stat = SlowScoreStat::default();
        stat.record(Duration::from_micros(1));
        stat.record(Duration::from_secs(30));
        assert_eq!(stat.score(), SLOW_SCORE_MAX);
        stat.reset();
        assert_eq!(stat.score(), SLOW_SCORE_INITIAL);
    }

    #[test]
    fn source_store_health_gates_feedback_and_decays_stale_tikv_scores() {
        let health = StoreHealthStatus::default();
        let start = Instant::now();

        health.record_tikv_slow_score(80, start);
        assert_eq!(health.detail().tikv_side_slow_score, 80);
        assert!(health.is_slow());

        // Go ignores a changed score in its 100-ms update window.
        health.record_tikv_slow_score(1, start + Duration::from_millis(99));
        assert_eq!(health.detail().tikv_side_slow_score, 80);

        health.record_tikv_slow_score(40, start + Duration::from_millis(100));
        assert_eq!(health.detail().tikv_side_slow_score, 40);
        assert!(!health.is_slow());

        health.tick(start + Duration::from_secs(15) + Duration::from_millis(100));
        assert_eq!(health.detail().tikv_side_slow_score, 35);
        health.tick(start + Duration::from_secs(15 * 200));
        assert_eq!(health.detail().tikv_side_slow_score, 1);
    }

    #[test]
    fn source_store_health_combines_client_and_tikv_slow_scores() {
        let health = StoreHealthStatus::default();
        assert!(!health.is_slow());
        health.record_client_side_latency(Duration::from_micros(100));
        assert_eq!(health.detail().client_side_slow_score, 1);
        health.mark_already_slow();
        assert_eq!(health.detail().client_side_slow_score, 100);
        assert!(health.is_slow());
    }

    #[test]
    fn source_mixed_replica_score_prioritizes_matching_labels_over_leader() {
        let replicas = [
            ReplicaCandidate {
                peer_id: 1,
                is_leader: true,
                is_learner: false,
                label_matches: false,
                is_slow: false,
                liveness: ReplicaLiveness::Reachable,
                attempts: 0,
                data_is_not_ready: false,
            },
            ReplicaCandidate {
                peer_id: 2,
                is_leader: false,
                is_learner: false,
                label_matches: true,
                is_slow: false,
                liveness: ReplicaLiveness::Reachable,
                attempts: 0,
                data_is_not_ready: false,
            },
        ];
        let selection = MixedReplicaSelection {
            read_type: ReplicaReadType::Mixed,
            leader_only: false,
            prefer_leader: false,
            labels_requested: true,
        };
        let (score, selected) = selection.highest_scored(&replicas);
        assert_eq!(
            score,
            SCORE_LABEL_MATCHES | SCORE_NORMAL_PEER | SCORE_NOT_SLOW | SCORE_NOT_ATTEMPTED
        );
        assert_eq!(selected, [&replicas[1]]);
        assert_eq!(selection.choose(&replicas), Some(replicas[1]));
    }

    #[test]
    fn source_prefer_leader_skips_slow_followers_and_learner_mode_scores_learners() {
        let replicas = [
            ReplicaCandidate {
                peer_id: 1,
                is_leader: true,
                is_learner: false,
                label_matches: false,
                is_slow: true,
                liveness: ReplicaLiveness::Reachable,
                attempts: 0,
                data_is_not_ready: false,
            },
            ReplicaCandidate {
                peer_id: 2,
                is_leader: false,
                is_learner: false,
                label_matches: true,
                is_slow: false,
                liveness: ReplicaLiveness::Reachable,
                attempts: 0,
                data_is_not_ready: false,
            },
            ReplicaCandidate {
                peer_id: 3,
                is_leader: false,
                is_learner: true,
                label_matches: false,
                is_slow: false,
                liveness: ReplicaLiveness::Reachable,
                attempts: 0,
                data_is_not_ready: false,
            },
        ];
        let (_, selected) = MixedReplicaSelection {
            read_type: ReplicaReadType::PreferLeader,
            leader_only: false,
            prefer_leader: true,
            labels_requested: false,
        }
        .highest_scored(&replicas);
        assert_eq!(selected, [&replicas[1]]);

        let mut matching_slow_leader = replicas[0];
        matching_slow_leader.label_matches = true;
        let (score, selected) = MixedReplicaSelection {
            read_type: ReplicaReadType::PreferLeader,
            leader_only: false,
            prefer_leader: true,
            labels_requested: true,
        }
        .highest_scored(std::slice::from_ref(&matching_slow_leader));
        assert_eq!(selected, [&matching_slow_leader]);
        assert_eq!(
            score,
            SCORE_LABEL_MATCHES | SCORE_NORMAL_PEER | SCORE_NOT_ATTEMPTED
        );

        let learner_replicas = [
            ReplicaCandidate {
                peer_id: 1,
                is_leader: true,
                is_learner: false,
                label_matches: false,
                is_slow: false,
                liveness: ReplicaLiveness::Reachable,
                attempts: 0,
                data_is_not_ready: false,
            },
            ReplicaCandidate {
                peer_id: 2,
                is_leader: false,
                is_learner: false,
                label_matches: false,
                is_slow: false,
                liveness: ReplicaLiveness::Reachable,
                attempts: 0,
                data_is_not_ready: false,
            },
            ReplicaCandidate {
                peer_id: 3,
                is_leader: false,
                is_learner: true,
                label_matches: false,
                is_slow: false,
                liveness: ReplicaLiveness::Reachable,
                attempts: 0,
                data_is_not_ready: false,
            },
        ];
        let (_, selected) = MixedReplicaSelection {
            read_type: ReplicaReadType::Learner,
            leader_only: false,
            prefer_leader: false,
            labels_requested: false,
        }
        .highest_scored(&learner_replicas);
        assert_eq!(selected, [&learner_replicas[2]]);
    }

    #[test]
    fn source_mixed_selection_allows_unknown_but_not_unreachable_liveness() {
        let selection = MixedReplicaSelection {
            read_type: ReplicaReadType::Follower,
            leader_only: false,
            prefer_leader: false,
            labels_requested: false,
        };
        let unknown = ReplicaCandidate {
            peer_id: 2,
            is_leader: false,
            is_learner: false,
            label_matches: true,
            is_slow: false,
            liveness: ReplicaLiveness::Unknown,
            attempts: 0,
            data_is_not_ready: false,
        };
        assert_eq!(
            selection.choose(std::slice::from_ref(&unknown)),
            Some(unknown)
        );

        let unreachable = ReplicaCandidate {
            liveness: ReplicaLiveness::Unreachable,
            ..unknown
        };
        assert_eq!(selection.choose(&[unreachable]), None);
    }

    #[test]
    fn source_selector_state_allows_data_not_ready_follower_once_more_and_probes_leader() {
        let mut state = ReplicaSelectorState::default();
        state.record_attempt(2);
        state.mark_data_is_not_ready(2);
        assert!(state.should_probe_stale_leader(1));

        let selection = MixedReplicaSelection {
            read_type: ReplicaReadType::Follower,
            leader_only: false,
            prefer_leader: false,
            labels_requested: false,
        };
        let retryable_follower = ReplicaCandidate {
            peer_id: 2,
            is_leader: false,
            is_learner: false,
            label_matches: false,
            is_slow: false,
            liveness: ReplicaLiveness::Reachable,
            attempts: state.attempts(2),
            data_is_not_ready: state.data_is_not_ready(2),
        };
        assert_eq!(
            selection.choose(&[retryable_follower]),
            Some(retryable_follower)
        );

        state.record_attempt(2);
        let exhausted_follower = ReplicaCandidate {
            attempts: state.attempts(2),
            ..retryable_follower
        };
        assert_eq!(selection.choose(&[exhausted_follower]), None);
        assert!(!state.should_probe_stale_leader(1));
    }

    #[test]
    fn source_busy_leader_probe_is_second_rejection_and_is_scoped_to_leader() {
        let mut state = ReplicaSelectorState::default();
        state.record_busy_leader(1);
        assert!(!state.should_probe_busy_leader(1));
        // A leader change before the probe restarts the count.
        state.record_busy_leader(2);
        assert!(!state.should_probe_busy_leader(1));
        assert!(!state.should_probe_busy_leader(2));
        state.record_busy_leader(2);
        assert!(state.should_probe_busy_leader(2));

        // The probe is selector-global. A hinted leader clears its replica
        // flag, but this request never fires a second probe.
        state.record_not_leader(1, 2);
        assert!(!state.should_probe_busy_leader(2));
        state.record_busy_leader(2);
        state.record_busy_leader(2);
        assert!(!state.should_probe_busy_leader(2));

        let mut direct = ReplicaSelectorState::default();
        direct.record_busy_leader(1);
        assert!(!direct.should_probe_busy_leader(1));
        direct.record_busy_leader(1);
        assert!(direct.should_probe_busy_leader(1));
        assert!(!direct.should_probe_busy_leader(2));
    }

    #[test]
    fn source_suspect_leader_is_temporarily_skipped_then_restored() {
        let mut state = ReplicaSelectorState::default();
        state.record_attempt(1);
        state.record_attempt(1);
        state.record_busy_leader(1);
        state.record_busy_leader(1);

        // Go's `isLeaderCandidate` deliberately ignores the temporary
        // suspect flag; only leader selection applies that extra skip.
        assert!(state.is_leader_candidate(1));
        assert!(!state.is_leader_selectable(1));
        assert!(state.restore_suspect_leader(1));
        assert!(state.is_leader_selectable(1));

        let mut exhausted = ReplicaSelectorState::default();
        for _ in 0..10 {
            exhausted.record_attempt(1);
        }
        exhausted.record_busy_leader(1);
        exhausted.record_busy_leader(1);
        assert!(!exhausted.restore_suspect_leader(1));
        assert!(!exhausted.should_probe_busy_leader(1));
        assert!(!exhausted.is_leader_candidate(1));
    }

    #[test]
    fn source_not_leader_switches_to_an_untried_concrete_leader() {
        let mut state = ReplicaSelectorState::default();
        state.record_attempt(1);
        state.record_not_leader(1, 2);
        assert!(state.should_force_leader(2));
        for _ in 0..10 {
            state.record_attempt(2);
        }
        assert!(!state.should_force_leader(2));

        let mut exhausted = ReplicaSelectorState::default();
        exhausted.record_attempt(1);
        for _ in 0..10 {
            exhausted.record_attempt(2);
        }
        exhausted.record_not_leader(1, 2);
        assert!(exhausted.should_force_leader(2));
        assert_eq!(exhausted.attempts(2), 9);
    }

    #[test]
    fn source_not_leader_without_hint_marks_only_the_failed_peer() {
        let mut state = ReplicaSelectorState::default();
        state.mark_no_leader(1);
        assert!(state.has_no_leader(1));
        assert!(!state.has_no_leader(2));
    }

    #[test]
    fn source_busy_threshold_state_excludes_peers_and_can_fall_back_to_leader() {
        let mut state = ReplicaSelectorState::default();
        state.record_server_busy(2);
        assert!(state.is_server_busy(2));
        assert!(!state.busy_threshold_disabled());
        state.disable_busy_threshold();
        assert!(state.busy_threshold_disabled());
    }

    #[test]
    fn source_flashback_replica_read_forces_a_threshold_free_leader_retry() {
        let mut state = ReplicaSelectorState::default();
        state.force_leader_after_flashback();
        assert!(state.should_force_leader(1));
        assert!(state.busy_threshold_disabled());
    }

    #[test]
    fn source_stale_retry_uses_replica_read_after_the_leader_was_attempted() {
        let mut state = ReplicaSelectorState::default();
        assert!(!state.should_retry_stale_as_replica(1));
        state.record_attempt(1);
        assert!(state.should_retry_stale_as_replica(1));

        let mut deadline = state.clone();
        deadline.mark_deadline_exceeded(1);
        assert!(!deadline.should_retry_stale_as_replica(1));

        state.record_server_busy(1);
        assert!(!state.should_retry_stale_as_replica(1));
    }

    #[test]
    fn source_region_not_found_forces_only_an_untried_leader() {
        let mut state = ReplicaSelectorState::default();
        assert!(state.force_leader_after_region_not_found(1));
        assert!(state.should_force_leader(1));

        state.record_attempt(1);
        assert!(!state.force_leader_after_region_not_found(1));
        assert!(state.should_force_leader(1));
    }

    #[test]
    fn source_leader_exhaustion_combines_attempt_count_time_and_error_flags() {
        let mut attempts = ReplicaSelectorState::default();
        for _ in 0..9 {
            attempts.record_attempt(1);
        }
        assert!(attempts.is_leader_candidate(1));
        attempts.record_attempt(1);
        assert!(!attempts.is_leader_candidate(1));

        let mut elapsed = ReplicaSelectorState::default();
        elapsed.record_attempt(1);
        elapsed.record_attempted_time(1, Duration::from_secs(49));
        assert!(elapsed.is_leader_candidate(1));
        elapsed.record_attempted_time(1, Duration::from_secs(1));
        assert_eq!(elapsed.attempted_time(1), Duration::from_secs(50));
        assert!(!elapsed.is_leader_candidate(1));

        let mut deadline = ReplicaSelectorState::default();
        deadline.record_attempt(1);
        deadline.mark_deadline_exceeded(1);
        assert!(deadline.deadline_exceeded(1));
        assert!(deadline.has_deadline_exceeded());
        assert!(!deadline.is_leader_candidate(1));

        let mut not_leader = ReplicaSelectorState::default();
        not_leader.record_attempt(1);
        not_leader.mark_no_leader(1);
        assert!(!not_leader.is_leader_candidate(1));
    }

    #[test]
    fn source_pending_backoff_replaces_by_store_consumes_on_retry_and_chooses_largest() {
        use crate::retry::{
            BO_REGION_SCHEDULING, BO_TIKV_DISK_FULL, BO_TIKV_RPC, BO_TIKV_SERVER_BUSY,
        };

        let mut state = ReplicaSelectorState::default();
        assert_eq!(state.take_pending_backoff(1), None);
        assert_eq!(state.largest_pending_backoff(), None);

        state.add_pending_backoff(0, BO_REGION_SCHEDULING, "err-0".to_owned());
        state.add_pending_backoff(1, BO_TIKV_RPC, "err-1".to_owned());
        state.add_pending_backoff(2, BO_TIKV_DISK_FULL, "err-2".to_owned());
        state.add_pending_backoff(1, BO_TIKV_SERVER_BUSY, "err-3".to_owned());
        assert_eq!(state.pending_backoff_count(), 3);

        assert_eq!(
            state.take_pending_backoff(0),
            Some((BO_REGION_SCHEDULING, "err-0".to_owned()))
        );
        assert_eq!(state.pending_backoff_count(), 2);
        assert_eq!(state.take_pending_backoff(10), None);
        assert_eq!(
            state.largest_pending_backoff(),
            Some((BO_TIKV_SERVER_BUSY, "err-3".to_owned()))
        );
    }
}
