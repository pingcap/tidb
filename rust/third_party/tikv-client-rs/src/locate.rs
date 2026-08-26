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
    #[allow(non_snake_case)]
    fn source_go_replica_selector_TestNextGenReadFeaturesDisabled() {
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
    #[allow(non_snake_case)]
    fn source_go_region_cache_TestSlowScoreStat() {
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
    #[allow(non_snake_case)]
    fn source_go_region_cache_TestTiKVSideSlowScore() {
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
    #[allow(non_snake_case)]
    fn source_go_region_cache_TestStoreHealthStatus() {
        let health = StoreHealthStatus::default();
        assert!(!health.is_slow());
        health.record_client_side_latency(Duration::from_micros(100));
        assert_eq!(health.detail().client_side_slow_score, 1);
        health.mark_already_slow();
        assert_eq!(health.detail().client_side_slow_score, 100);
        assert!(health.is_slow());
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_replica_selector_TestReplicaSelectorLeaderBusyProbe() {
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
    #[allow(non_snake_case)]
    fn source_go_replica_selector_TestReplicaReadAccessPathByTryIdleReplicaCase() {
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
    fn source_busy_threshold_state_excludes_peers_and_can_fall_back_to_leader() {
        let mut state = ReplicaSelectorState::default();
        state.record_server_busy(2);
        assert!(state.is_server_busy(2));
        assert!(!state.busy_threshold_disabled());
        state.disable_busy_threshold();
        assert!(state.busy_threshold_disabled());
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
    #[allow(non_snake_case)]
    fn source_go_replica_selector_TestPendingBackoff() {
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

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_request_state_TestRegionCacheStaleRead() {
        let mut state = ReplicaSelectorState::default();
        assert!(!state.should_retry_stale_as_replica(1));
        state.record_attempt(1);
        assert!(state.should_retry_stale_as_replica(1));

        state.mark_deadline_exceeded(1);
        assert!(!state.should_retry_stale_as_replica(1));
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_request_state_TestRegionCacheStaleReadUsingAsyncAPI() {
        let mut state = ReplicaSelectorState::default();
        state.record_attempt(1);
        assert!(state.should_retry_stale_as_replica(1));

        state.record_server_busy(1);
        assert!(!state.should_retry_stale_as_replica(1));
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_cache_TestUpdateLeader() {
        let mut state = ReplicaSelectorState::default();
        state.record_attempt(1);
        state.record_not_leader(1, 2);
        assert!(state.has_no_leader(1));
        assert!(state.should_force_leader(2));
        assert_eq!(state.attempts(2), 0);
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_cache_TestUpdateLeader2() {
        let mut state = ReplicaSelectorState::default();
        state.record_attempt(1);
        state.record_not_leader(1, 3);
        assert!(state.should_force_leader(3));
        assert!(!state.should_force_leader(1));
        assert!(!state.has_no_leader(2));
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_cache_TestUpdateLeader3() {
        let mut state = ReplicaSelectorState::default();
        state.record_attempt(1);
        for _ in 0..MAX_REPLICA_ATTEMPTS {
            state.record_attempt(2);
        }
        state.record_not_leader(1, 2);
        assert!(state.should_force_leader(2));
        assert_eq!(state.attempts(2), MAX_REPLICA_ATTEMPTS - 1);
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_cache_TestLabelSelectorTiKVPeer() {
        let leader = ReplicaCandidate {
            peer_id: 1,
            is_leader: true,
            is_learner: false,
            label_matches: false,
            is_slow: false,
            liveness: ReplicaLiveness::Reachable,
            attempts: 0,
            data_is_not_ready: false,
        };
        let local_follower = ReplicaCandidate {
            peer_id: 2,
            is_leader: false,
            label_matches: true,
            ..leader
        };
        let selection = MixedReplicaSelection {
            read_type: ReplicaReadType::Mixed,
            leader_only: false,
            prefer_leader: false,
            labels_requested: true,
        };
        let candidates = [leader, local_follower];
        let (_, selected) = selection.highest_scored(&candidates);
        assert_eq!(selected, [&local_follower]);

        let leader_only = MixedReplicaSelection {
            read_type: ReplicaReadType::Leader,
            leader_only: true,
            ..selection
        };
        assert_eq!(leader_only.choose(&[leader, local_follower]), Some(leader));
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_cache_TestFollowerReadFallback() {
        let leader = ReplicaCandidate {
            peer_id: 1,
            is_leader: true,
            is_learner: false,
            label_matches: false,
            is_slow: false,
            liveness: ReplicaLiveness::Reachable,
            attempts: 0,
            data_is_not_ready: false,
        };
        let failed = ReplicaCandidate {
            peer_id: 2,
            is_leader: false,
            attempts: 1,
            ..leader
        };
        let fallback = ReplicaCandidate {
            peer_id: 3,
            is_leader: false,
            ..leader
        };
        let selection = MixedReplicaSelection {
            read_type: ReplicaReadType::Follower,
            leader_only: false,
            prefer_leader: false,
            labels_requested: false,
        };
        assert_eq!(
            selection.choose(&[leader, failed, fallback]),
            Some(fallback)
        );
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_cache_TestMixedReadFallback() {
        let leader = ReplicaCandidate {
            peer_id: 1,
            is_leader: true,
            is_learner: false,
            label_matches: false,
            is_slow: false,
            liveness: ReplicaLiveness::Reachable,
            attempts: 1,
            data_is_not_ready: false,
        };
        let follower = ReplicaCandidate {
            peer_id: 2,
            is_leader: false,
            attempts: 0,
            ..leader
        };
        let selection = MixedReplicaSelection {
            read_type: ReplicaReadType::Mixed,
            leader_only: false,
            prefer_leader: false,
            labels_requested: false,
        };
        assert_eq!(selection.choose(&[leader, follower]), Some(follower));
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_cache_TestSwitchPeerWhenNoLeader() {
        let mut state = ReplicaSelectorState::default();
        for peer_id in 1..=3 {
            state.mark_no_leader(peer_id);
            assert!(state.has_no_leader(peer_id));
        }
        assert_eq!(state.total_attempts(), 0);
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_request3_TestSwitchPeerWhenNoLeader() {
        let mut state = ReplicaSelectorState::default();
        state.record_attempt(1);
        state.mark_no_leader(1);
        assert!(!state.is_leader_candidate(1));
        assert!(state.is_leader_candidate(2));
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_request3_TestSwitchPeerWhenNoLeaderErrorWithNewLeaderInfo() {
        let mut state = ReplicaSelectorState::default();
        state.record_attempt(1);
        state.record_not_leader(1, 2);
        assert!(state.should_force_leader(2));
        state.record_attempt(2);
        assert_eq!(state.total_attempts(), 2);
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_request3_TestReplicaReadFallbackToLeaderRegionError() {
        let mut state = ReplicaSelectorState::default();
        state.record_attempt(2);
        state.mark_data_is_not_ready(2);
        assert!(state.should_probe_stale_leader(1));
        state.record_attempt(1);
        assert!(!state.should_probe_stale_leader(1));
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_request3_TestLearnerReplicaSelector() {
        let voter = ReplicaCandidate {
            peer_id: 1,
            is_leader: false,
            is_learner: false,
            label_matches: false,
            is_slow: false,
            liveness: ReplicaLiveness::Reachable,
            attempts: 0,
            data_is_not_ready: false,
        };
        let learner = ReplicaCandidate {
            peer_id: 2,
            is_learner: true,
            ..voter
        };
        let selection = MixedReplicaSelection {
            read_type: ReplicaReadType::Learner,
            leader_only: false,
            prefer_leader: false,
            labels_requested: false,
        };
        assert_eq!(selection.choose(&[voter, learner]), Some(learner));
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_request3_TestPreferLeader() {
        let leader = ReplicaCandidate {
            peer_id: 1,
            is_leader: true,
            is_learner: false,
            label_matches: false,
            is_slow: false,
            liveness: ReplicaLiveness::Reachable,
            attempts: 0,
            data_is_not_ready: false,
        };
        let follower = ReplicaCandidate {
            peer_id: 2,
            is_leader: false,
            ..leader
        };
        let selection = MixedReplicaSelection {
            read_type: ReplicaReadType::PreferLeader,
            leader_only: false,
            prefer_leader: true,
            labels_requested: false,
        };
        assert_eq!(selection.choose(&[leader, follower]), Some(leader));

        let unreachable_leader = ReplicaCandidate {
            liveness: ReplicaLiveness::Unreachable,
            ..leader
        };
        assert_eq!(
            selection.choose(&[unreachable_leader, follower]),
            Some(follower)
        );
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_request3_TestReplicaSelector() {
        let mut state = ReplicaSelectorState::default();
        assert_eq!(state.total_attempts(), 0);
        state.record_attempt(2);
        state.record_attempted_time(2, Duration::from_millis(20));
        assert_eq!(state.attempts(2), 1);
        assert_eq!(state.attempted_time(2), Duration::from_millis(20));
        assert!(state.is_leader_candidate(1));
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_request3_TestLoadBasedReplicaRead() {
        let busy_leader = ReplicaCandidate {
            peer_id: 1,
            is_leader: true,
            is_learner: false,
            label_matches: false,
            is_slow: true,
            liveness: ReplicaLiveness::Reachable,
            attempts: 0,
            data_is_not_ready: false,
        };
        let idle_follower = ReplicaCandidate {
            peer_id: 2,
            is_leader: false,
            is_slow: false,
            ..busy_leader
        };
        let selection = MixedReplicaSelection {
            read_type: ReplicaReadType::Mixed,
            leader_only: false,
            prefer_leader: false,
            labels_requested: false,
        };
        assert_eq!(
            selection.choose(&[busy_leader, idle_follower]),
            Some(idle_follower)
        );
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_request3_TestReplicaReadWithFlashbackInProgress() {
        let mut state = ReplicaSelectorState::default();
        state.record_server_busy(1);
        state.force_leader_after_flashback();
        assert!(state.should_force_leader(1));
        assert!(state.busy_threshold_disabled());
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_request3_TestAccessFollowerAfter1TiKVDown() {
        let down = ReplicaCandidate {
            peer_id: 1,
            is_leader: false,
            is_learner: false,
            label_matches: true,
            is_slow: false,
            liveness: ReplicaLiveness::Unreachable,
            attempts: 0,
            data_is_not_ready: false,
        };
        let leader = ReplicaCandidate {
            peer_id: 2,
            is_leader: true,
            label_matches: false,
            liveness: ReplicaLiveness::Reachable,
            ..down
        };
        let selection = MixedReplicaSelection {
            read_type: ReplicaReadType::Mixed,
            leader_only: false,
            prefer_leader: false,
            labels_requested: true,
        };
        assert_eq!(selection.choose(&[down, leader]), Some(leader));
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_request3_TestDoNotTryUnreachableLeader() {
        let leader = ReplicaCandidate {
            peer_id: 1,
            is_leader: true,
            is_learner: false,
            label_matches: false,
            is_slow: false,
            liveness: ReplicaLiveness::Unreachable,
            attempts: 0,
            data_is_not_ready: false,
        };
        let local_follower = ReplicaCandidate {
            peer_id: 2,
            is_leader: false,
            label_matches: true,
            liveness: ReplicaLiveness::Reachable,
            ..leader
        };
        let selection = MixedReplicaSelection {
            read_type: ReplicaReadType::Mixed,
            leader_only: false,
            prefer_leader: false,
            labels_requested: true,
        };
        assert_eq!(
            selection.choose(&[leader, local_follower]),
            Some(local_follower)
        );
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_request3_TestStaleReadTryFollowerAfterTimeout() {
        let mut state = ReplicaSelectorState::default();
        state.record_attempt(1);
        assert!(state.should_retry_stale_as_replica(1));
        assert_eq!(state.total_attempts(), 1);

        let follower = ReplicaCandidate {
            peer_id: 2,
            is_leader: false,
            is_learner: false,
            label_matches: true,
            is_slow: false,
            liveness: ReplicaLiveness::Reachable,
            attempts: 0,
            data_is_not_ready: false,
        };
        let selection = MixedReplicaSelection {
            read_type: ReplicaReadType::Mixed,
            leader_only: false,
            prefer_leader: false,
            labels_requested: true,
        };
        assert_eq!(selection.choose(&[follower]), Some(follower));
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_request3_TestLeaderStuck() {
        let mut state = ReplicaSelectorState::default();
        state.record_attempt(1);
        state.record_attempted_time(1, Duration::from_secs(49));
        assert!(state.is_leader_candidate(1));
        state.record_attempted_time(1, Duration::from_secs(1));
        assert!(!state.is_leader_candidate(1));

        state.record_not_leader(1, 2);
        assert!(state.should_force_leader(2));
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_replica_selector_TestReplicaSelectorBasic() {
        let mut state = ReplicaSelectorState::default();
        assert_eq!(state.attempts(1), 0);
        assert_eq!(state.attempted_time(1), Duration::ZERO);
        assert!(!state.has_deadline_exceeded());
        state.record_attempt(1);
        state.record_attempted_time(1, Duration::from_millis(1));
        assert_eq!(state.attempts(1), 1);
        assert_eq!(state.total_attempts(), 1);
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_replica_selector_TestReplicaSelectorCalculateScore() {
        let leader = ReplicaCandidate {
            peer_id: 1,
            is_leader: true,
            is_learner: false,
            label_matches: false,
            is_slow: false,
            liveness: ReplicaLiveness::Reachable,
            attempts: 0,
            data_is_not_ready: false,
        };
        let matching_follower = ReplicaCandidate {
            peer_id: 2,
            is_leader: false,
            label_matches: true,
            ..leader
        };
        let attempted_match = ReplicaCandidate {
            peer_id: 3,
            attempts: 1,
            ..matching_follower
        };
        let selection = MixedReplicaSelection {
            read_type: ReplicaReadType::Mixed,
            leader_only: false,
            prefer_leader: false,
            labels_requested: true,
        };
        let candidates = [leader, matching_follower, attempted_match];
        let (score, selected) = selection.highest_scored(&candidates);
        assert_eq!(
            score,
            SCORE_LABEL_MATCHES | SCORE_NORMAL_PEER | SCORE_NOT_SLOW | SCORE_NOT_ATTEMPTED
        );
        assert_eq!(selected, [&matching_follower]);
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_replica_selector_TestCanFastRetry() {
        let mut state = ReplicaSelectorState::default();
        state.record_attempt(1);
        state.record_not_leader(1, 2);
        assert!(state.should_force_leader(2));

        state.mark_deadline_exceeded(2);
        assert!(!state.should_force_leader(2));
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_replica_selector_TestReplicaReadAccessPathByCase() {
        for read_type in [
            ReplicaReadType::Leader,
            ReplicaReadType::Follower,
            ReplicaReadType::Mixed,
        ] {
            let mut state = ReplicaSelectorState::default();
            state.record_attempt(2);
            state.mark_data_is_not_ready(2);
            assert_eq!(state.attempts(2), 1);
            assert!(state.data_is_not_ready(2));
            assert!(state.should_probe_stale_leader(1));

            let selection = MixedReplicaSelection {
                read_type,
                leader_only: matches!(read_type, ReplicaReadType::Leader),
                prefer_leader: false,
                labels_requested: false,
            };
            let leader = ReplicaCandidate {
                peer_id: 1,
                is_leader: true,
                is_learner: false,
                label_matches: false,
                is_slow: false,
                liveness: ReplicaLiveness::Reachable,
                attempts: 0,
                data_is_not_ready: false,
            };
            assert!(selection.choose(&[leader]).is_some());
        }
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_replica_selector_TestReplicaReadAccessPathByCaseUsingAsyncAPI() {
        let mut state = ReplicaSelectorState::default();
        state.record_attempt(2);
        state.mark_data_is_not_ready(2);
        let retryable = ReplicaCandidate {
            peer_id: 2,
            is_leader: false,
            is_learner: false,
            label_matches: false,
            is_slow: false,
            liveness: ReplicaLiveness::Reachable,
            attempts: state.attempts(2),
            data_is_not_ready: state.data_is_not_ready(2),
        };
        let selection = MixedReplicaSelection {
            read_type: ReplicaReadType::Follower,
            leader_only: false,
            prefer_leader: false,
            labels_requested: false,
        };
        assert_eq!(selection.choose(&[retryable]), Some(retryable));
        state.record_attempt(2);
        assert_eq!(
            selection.choose(&[ReplicaCandidate {
                attempts: state.attempts(2),
                ..retryable
            }]),
            None
        );
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_replica_selector_TestReplicaReadAccessPathByCase2() {
        let mut state = ReplicaSelectorState::default();
        assert!(state.force_leader_after_region_not_found(1));
        assert!(state.should_force_leader(1));
        state.record_attempt(1);
        assert!(!state.force_leader_after_region_not_found(1));

        let mut flashback = ReplicaSelectorState::default();
        flashback.force_leader_after_flashback();
        assert!(flashback.should_force_leader(1));
        assert!(flashback.busy_threshold_disabled());
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_replica_selector_TestReplicaReadAccessPathByCase2UsingAsyncAPI() {
        let mut state = ReplicaSelectorState::default();
        state.mark_no_leader(1);
        assert!(state.has_no_leader(1));
        assert!(!state.is_leader_candidate(1));
        assert!(state.is_leader_candidate(2));

        state.record_not_leader(1, 3);
        assert!(state.should_force_leader(3));
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_replica_selector_TestReplicaReadAccessPathByBasicCase() {
        let reachable = ReplicaCandidate {
            peer_id: 1,
            is_leader: true,
            is_learner: false,
            label_matches: false,
            is_slow: false,
            liveness: ReplicaLiveness::Reachable,
            attempts: 0,
            data_is_not_ready: false,
        };
        let unknown = ReplicaCandidate {
            peer_id: 2,
            is_leader: false,
            liveness: ReplicaLiveness::Unknown,
            ..reachable
        };
        let unreachable = ReplicaCandidate {
            peer_id: 3,
            is_leader: false,
            liveness: ReplicaLiveness::Unreachable,
            ..reachable
        };
        let selection = MixedReplicaSelection {
            read_type: ReplicaReadType::Mixed,
            leader_only: false,
            prefer_leader: false,
            labels_requested: false,
        };
        let candidates = [reachable, unknown, unreachable];
        let (_, selected) = selection.highest_scored(&candidates);
        assert_eq!(selected, [&reachable, &unknown]);
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_replica_selector_TestReplicaReadAccessPathByBasicCaseUsingAsyncAPI() {
        let unknown = ReplicaCandidate {
            peer_id: 2,
            is_leader: false,
            is_learner: false,
            label_matches: false,
            is_slow: false,
            liveness: ReplicaLiveness::Unknown,
            attempts: 0,
            data_is_not_ready: false,
        };
        let selection = MixedReplicaSelection {
            read_type: ReplicaReadType::Follower,
            leader_only: false,
            prefer_leader: false,
            labels_requested: false,
        };
        assert_eq!(selection.choose(&[unknown]), Some(unknown));
        assert_eq!(
            selection.choose(&[ReplicaCandidate {
                liveness: ReplicaLiveness::Unreachable,
                ..unknown
            }]),
            None
        );
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_replica_selector_TestReplicaReadAccessPathByLeaderCase() {
        let mut state = ReplicaSelectorState::default();
        for _ in 0..2 {
            state.record_attempt(1);
            state.record_busy_leader(1);
        }
        assert!(state.should_probe_busy_leader(1));
        assert!(!state.is_leader_selectable(1));
        assert!(state.restore_suspect_leader(1));

        state.record_not_leader(1, 3);
        assert!(state.should_force_leader(3));
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_replica_selector_TestReplicaReadAccessPathByLeaderCaseUsingAsyncAPI() {
        let mut state = ReplicaSelectorState::default();
        for _ in 0..MAX_REPLICA_ATTEMPTS - 1 {
            state.record_attempt(1);
        }
        assert!(state.is_leader_candidate(1));
        state.record_attempt(1);
        assert!(!state.is_leader_candidate(1));

        state.record_not_leader(1, 2);
        assert!(state.should_force_leader(2));
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_replica_selector_TestReplicaReadAccessPathByFollowerCase() {
        let leader = ReplicaCandidate {
            peer_id: 1,
            is_leader: true,
            is_learner: false,
            label_matches: false,
            is_slow: false,
            liveness: ReplicaLiveness::Reachable,
            attempts: 0,
            data_is_not_ready: false,
        };
        let failed = ReplicaCandidate {
            peer_id: 2,
            is_leader: false,
            attempts: 1,
            ..leader
        };
        let next = ReplicaCandidate {
            peer_id: 3,
            is_leader: false,
            ..leader
        };
        let selection = MixedReplicaSelection {
            read_type: ReplicaReadType::Follower,
            leader_only: false,
            prefer_leader: false,
            labels_requested: false,
        };
        assert_eq!(selection.choose(&[leader, failed, next]), Some(next));
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_replica_selector_TestReplicaReadAccessPathByFollowerCaseUsingAsyncAPI() {
        let mut state = ReplicaSelectorState::default();
        state.record_attempt(2);
        state.mark_data_is_not_ready(2);
        assert!(state.should_probe_stale_leader(1));
        state.record_attempt(2);
        assert!(!state.should_probe_stale_leader(1));
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_replica_selector_TestReplicaReadAccessPathByMixedAndPreferLeaderCase() {
        let leader = ReplicaCandidate {
            peer_id: 1,
            is_leader: true,
            is_learner: false,
            label_matches: false,
            is_slow: false,
            liveness: ReplicaLiveness::Reachable,
            attempts: 0,
            data_is_not_ready: false,
        };
        let follower = ReplicaCandidate {
            peer_id: 2,
            is_leader: false,
            ..leader
        };
        for (read_type, prefer_leader) in [
            (ReplicaReadType::Mixed, false),
            (ReplicaReadType::PreferLeader, true),
        ] {
            let selection = MixedReplicaSelection {
                read_type,
                leader_only: false,
                prefer_leader,
                labels_requested: false,
            };
            if prefer_leader {
                assert_eq!(selection.choose(&[leader, follower]), Some(leader));
            } else {
                let candidates = [leader, follower];
                let (_, selected) = selection.highest_scored(&candidates);
                assert_eq!(selected, [&leader, &follower]);
            }
        }
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_replica_selector_TestReplicaReadAccessPathByMixedAndPreferLeaderCaseUsingAsyncAPI()
    {
        let slow_follower = ReplicaCandidate {
            peer_id: 2,
            is_leader: false,
            is_learner: false,
            label_matches: true,
            is_slow: true,
            liveness: ReplicaLiveness::Reachable,
            attempts: 0,
            data_is_not_ready: false,
        };
        let leader = ReplicaCandidate {
            peer_id: 1,
            is_leader: true,
            label_matches: false,
            is_slow: false,
            ..slow_follower
        };
        let selection = MixedReplicaSelection {
            read_type: ReplicaReadType::PreferLeader,
            leader_only: false,
            prefer_leader: true,
            labels_requested: true,
        };
        assert_eq!(selection.choose(&[slow_follower, leader]), Some(leader));
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_replica_selector_TestReplicaReadAccessPathByStaleReadCase() {
        let mut state = ReplicaSelectorState::default();
        assert!(!state.should_retry_stale_as_replica(1));
        state.record_attempt(1);
        assert!(state.should_retry_stale_as_replica(1));

        let mut busy = state.clone();
        busy.record_server_busy(1);
        assert!(!busy.should_retry_stale_as_replica(1));
        state.mark_deadline_exceeded(1);
        assert!(!state.should_retry_stale_as_replica(1));
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_replica_selector_TestReplicaReadAccessPathByFlashbackInProgressCase() {
        let mut state = ReplicaSelectorState::default();
        state.force_leader_after_flashback();
        assert!(state.should_force_leader(1));
        assert!(state.busy_threshold_disabled());
        assert!(!state.is_server_busy(1));
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_replica_selector_TestReplicaReadAccessPathByLearnerCase() {
        let voter = ReplicaCandidate {
            peer_id: 1,
            is_leader: false,
            is_learner: false,
            label_matches: false,
            is_slow: false,
            liveness: ReplicaLiveness::Reachable,
            attempts: 0,
            data_is_not_ready: false,
        };
        let learner = ReplicaCandidate {
            peer_id: 2,
            is_learner: true,
            ..voter
        };
        let down_learner = ReplicaCandidate {
            peer_id: 3,
            liveness: ReplicaLiveness::Unreachable,
            ..learner
        };
        let selection = MixedReplicaSelection {
            read_type: ReplicaReadType::Learner,
            leader_only: false,
            prefer_leader: false,
            labels_requested: false,
        };
        assert_eq!(
            selection.choose(&[voter, learner, down_learner]),
            Some(learner)
        );
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_replica_selector_TestReplicaReadAvoidSlowStore() {
        let slow_follower = ReplicaCandidate {
            peer_id: 2,
            is_leader: false,
            is_learner: false,
            label_matches: false,
            is_slow: true,
            liveness: ReplicaLiveness::Reachable,
            attempts: 0,
            data_is_not_ready: false,
        };
        let healthy_follower = ReplicaCandidate {
            peer_id: 3,
            is_slow: false,
            ..slow_follower
        };
        let selection = MixedReplicaSelection {
            read_type: ReplicaReadType::PreferLeader,
            leader_only: false,
            prefer_leader: true,
            labels_requested: false,
        };
        assert_eq!(
            selection.choose(&[slow_follower, healthy_follower]),
            Some(healthy_follower)
        );
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_replica_selector_TestReplicaFlag() {
        let mut state = ReplicaSelectorState::default();
        state.record_attempt(1);
        state.mark_deadline_exceeded(1);
        state.mark_data_is_not_ready(2);
        state.mark_no_leader(3);
        state.record_server_busy(4);
        assert!(state.deadline_exceeded(1));
        assert!(state.data_is_not_ready(2));
        assert!(state.has_no_leader(3));
        assert!(state.is_server_busy(4));
        assert!(!state.is_leader_candidate(1));
        assert!(!state.is_leader_candidate(3));
    }
}
