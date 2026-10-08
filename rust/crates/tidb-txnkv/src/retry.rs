// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! TiDB transaction retry arithmetic and a synchronous adapter to client-rust.
//!
//! `pkg/kv/txn.go` owns transaction retry counts and its small jitter bound.
//! Client-go owns storage retry schedules, budgets and history; region callers
//! below share that native owner while driving their own synchronous waits.

use std::sync::Arc;
use std::time::Duration;
use tikv_client::retry::{self, BackoffWait, RetryBackoffer, RetryConfig};
use tokio::sync::Mutex;

/// Initial exponential-backoff bound, in the effective sleep unit used by
/// `pkg/kv/txn.go` (`time.Millisecond`).
pub const RETRY_BACKOFF_BASE_MS: u64 = 1;

/// Maximum exponential-backoff bound, in milliseconds.
pub const RETRY_BACKOFF_CAP_MS: u64 = 100;

/// Campaign 11's per-region effective recovery sleep budget.
pub const REGION_RETRY_MAX_SLEEP: Duration = Duration::from_secs(20);

/// Pinned client-go region backoff categories.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum RegionBackoffKind {
    /// TiKV RPC transport failure.
    TikvRpc,
    /// TiFlash RPC transport failure, including MPP setup.
    TiFlashRpc,
    /// Epoch cache miss or stale TiKV epoch.
    RegionMiss,
    /// Election, split, merge, or read-index scheduling.
    RegionScheduling,
    /// TiKV admission control rejected the request.
    TikvServerBusy,
    /// TiKV reported a full disk.
    TikvDiskFull,
    /// Unsafe recovery is still in progress.
    RegionRecoveryInProgress,
    /// The raft command term became stale.
    StaleCommand,
    /// The new leader has not synchronized max timestamp.
    MaxTimestampNotSynced,
    /// The target region peer is not initialized.
    RegionNotInitialized,
    /// The selected peer is a witness.
    IsWitness,
    /// A read or write is blocked behind another transaction's lock.
    TxnLock,
    /// The same wait after a cheap resolve, which client-go starts far shorter
    /// because the common case is a lock that is already gone.
    TxnLockFast,
    /// CheckTxnStatus found a secondary lock whose primary record does not
    /// exist yet, which a concurrent prewrite resolves on its own.
    TxnNotFound,
    /// A PD region-lookup RPC failed (client-go `retry.BoPDRPC`).
    PdRpc,
}

impl RegionBackoffKind {
    const COUNT: usize = 15;
    const ALL: [Self; Self::COUNT] = [
        Self::TikvRpc,
        Self::TiFlashRpc,
        Self::RegionMiss,
        Self::RegionScheduling,
        Self::TikvServerBusy,
        Self::TikvDiskFull,
        Self::RegionRecoveryInProgress,
        Self::StaleCommand,
        Self::MaxTimestampNotSynced,
        Self::RegionNotInitialized,
        Self::IsWitness,
        Self::TxnLock,
        Self::TxnLockFast,
        Self::TxnNotFound,
        Self::PdRpc,
    ];

    /// Pinned client-go Config.name used in snapshot runtime statistics.
    pub(crate) const fn name(self) -> &'static str {
        self.config().name
    }

    const fn config(self) -> RetryConfig {
        match self {
            Self::TikvRpc => retry::BO_TIKV_RPC,
            Self::TiFlashRpc => retry::BO_TIFLASH_RPC,
            Self::RegionMiss => retry::BO_REGION_MISS,
            Self::RegionScheduling => retry::BO_REGION_SCHEDULING,
            Self::TikvServerBusy => retry::BO_TIKV_SERVER_BUSY,
            Self::TikvDiskFull => retry::BO_TIKV_DISK_FULL,
            Self::RegionRecoveryInProgress => retry::BO_REGION_RECOVERY_IN_PROGRESS,
            Self::StaleCommand => retry::BO_STALE_CMD,
            Self::MaxTimestampNotSynced => retry::BO_MAX_TS_NOT_SYNCED,
            Self::RegionNotInitialized => retry::BO_MAX_REGION_NOT_INITIALIZED,
            Self::IsWitness => retry::BO_IS_WITNESS,
            Self::TxnLock => retry::BO_TXN_LOCK,
            Self::TxnLockFast => retry::BO_TXN_LOCK_FAST,
            Self::TxnNotFound => retry::BO_TXN_NOT_FOUND,
            Self::PdRpc => retry::BO_PD_RPC,
        }
    }
}

/// Synchronous wait adapter over the native client-go retry owner.
/// All limits, delay schedules, fork history and diagnostics live in client-rust.
pub struct RegionBackoffBudget {
    owner: Arc<Mutex<RetryBackoffer>>,
    pending: Option<(RegionBackoffKind, BackoffWait)>,
}

impl std::fmt::Debug for RegionBackoffBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegionBackoffBudget")
            .field("total_sleep", &self.total_sleep())
            .field("remaining", &self.remaining())
            .finish_non_exhaustive()
    }
}

impl RegionBackoffBudget {
    /// Creates an effective sleep budget; the caller has already selected its limit.
    #[must_use]
    pub fn new(max_sleep: Duration) -> Self {
        let variables = tikv_client::kv::Variables {
            backoff_weight: 1,
            ..Default::default()
        };
        Self {
            owner: Arc::new(Mutex::new(RetryBackoffer::with_variables(
                Default::default(),
                duration_ms(max_sleep),
                Arc::new(variables),
            ))),
            pending: None,
        }
    }

    /// Creates the default effective per-region recovery budget.
    #[must_use]
    pub fn campaign_default() -> Self {
        Self::new(REGION_RETRY_MAX_SLEEP)
    }

    /// A resolver borrows this exact owner, including earlier waits and schedules.
    pub(crate) fn native_owner(&mut self) -> Arc<Mutex<RetryBackoffer>> {
        self.finish_wait(true);
        Arc::clone(&self.owner)
    }

    /// Fork the source owner instead of copying a second accounting structure.
    #[must_use]
    pub fn fork(&mut self) -> Self {
        self.finish_wait(true);
        let (fork, _) = self
            .owner
            .try_lock()
            .expect("retry owner is exclusively borrowed")
            .fork();
        Self {
            owner: Arc::new(Mutex::new(fork)),
            pending: None,
        }
    }

    /// Selects a wait from the native schedule; finish it after waiting.
    pub fn next_delay(
        &mut self,
        kind: RegionBackoffKind,
    ) -> Result<Duration, RegionBackoffExhausted> {
        self.next_delay_capped(kind, Duration::MAX)
    }

    /// Caps this wait by the observed lock TTL without resetting its schedule.
    pub fn next_delay_capped(
        &mut self,
        kind: RegionBackoffKind,
        max_sleep: Duration,
    ) -> Result<Duration, RegionBackoffExhausted> {
        self.finish_wait(true);
        let mut owner = self
            .owner
            .try_lock()
            .expect("retry owner is exclusively borrowed");
        let wait = owner
            .prepare_backoff(kind.config(), Some(duration_ms(max_sleep)), kind.name())
            .map_err(|_| Self::exhausted(&owner, kind))?;
        let duration = wait.duration();
        self.pending = Some((kind, wait));
        Ok(duration)
    }

    /// Complete an externally driven wait; interruption counts once and charges zero sleep.
    pub fn finish_wait(&mut self, completed: bool) {
        if let Some((_, wait)) = self.pending.take() {
            // This synchronous adapter has no kill handler. Call cancellation
            // is handled by the response owner before and during its wait.
            let _ = self
                .owner
                .try_lock()
                .expect("retry owner is exclusively borrowed")
                .finish_backoff(wait, completed);
        }
    }

    pub(crate) fn exhausted(
        owner: &RetryBackoffer,
        fallback: RegionBackoffKind,
    ) -> RegionBackoffExhausted {
        let kind = owner
            .longest_sleep_config()
            .and_then(|config| {
                RegionBackoffKind::ALL
                    .into_iter()
                    .find(|kind| kind.name() == config.name)
            })
            .unwrap_or(fallback);
        RegionBackoffExhausted {
            kind,
            max_sleep: Duration::from_millis(owner.max_sleep_ms()),
        }
    }

    #[cfg(test)]
    pub(crate) fn update_from_forked(&mut self, forked: &Self) {
        self.finish_wait(true);
        self.owner
            .try_lock()
            .expect("retry owner is exclusively borrowed")
            .update_using_forked(
                &forked
                    .owner
                    .try_lock()
                    .expect("retry owner is exclusively borrowed"),
            );
    }

    #[cfg(test)]
    pub(crate) fn runtime_stats(&self) -> impl Iterator<Item = (&'static str, u64, Duration)> {
        let owner = self
            .owner
            .try_lock()
            .expect("retry owner is exclusively borrowed");
        RegionBackoffKind::ALL
            .into_iter()
            .filter_map(|kind| {
                let pending = self
                    .pending
                    .as_ref()
                    .filter(|(pending, _)| *pending == kind);
                let count = owner.times_by_type().get(kind.name()).copied().unwrap_or(0)
                    + u64::from(pending.is_some());
                let sleep = Duration::from_millis(
                    owner.sleep_by_type().get(kind.name()).copied().unwrap_or(0),
                ) + pending.map_or(Duration::ZERO, |(_, wait)| wait.duration());
                (count != 0).then_some((kind.name(), count, sleep))
            })
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// Completed and currently reserved sleep, including excluded categories.
    #[must_use]
    pub fn total_sleep(&self) -> Duration {
        Duration::from_millis(
            self.owner
                .try_lock()
                .expect("retry owner is exclusively borrowed")
                .total_sleep_ms(),
        ) + self
            .pending
            .as_ref()
            .map_or(Duration::ZERO, |(_, wait)| wait.duration())
    }

    /// Remaining ordinary budget, excluding independently limited categories.
    #[must_use]
    pub fn remaining(&self) -> Duration {
        let owner = self
            .owner
            .try_lock()
            .expect("retry owner is exclusively borrowed");
        let pending = self
            .pending
            .as_ref()
            .filter(|(kind, _)| kind.config().excluded_budget_limit_ms.is_none())
            .map_or(0, |(_, wait)| duration_ms(wait.duration()));
        Duration::from_millis(
            owner.max_sleep_ms().saturating_sub(
                owner
                    .total_sleep_ms()
                    .saturating_sub(owner.excluded_sleep_ms())
                    .saturating_add(pending),
            ),
        )
    }
}
/// Exhaustion result returned before reserving a new sleep.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegionBackoffExhausted {
    /// Category which observed exhaustion.
    pub kind: RegionBackoffKind,
    /// Configured effective maximum.
    pub max_sleep: Duration,
}

const fn duration_ms(duration: Duration) -> u64 {
    let millis = duration.as_millis();
    if millis > u64::MAX as u128 {
        u64::MAX
    } else {
        millis as u64
    }
}

/// Returns whether a retryable failure should start another attempt.
///
/// `RunInNewTxn` iterates `i` over `0..MaxRetryCnt`.  A retryable failure at
/// an earlier index continues the loop; a failure at the final index leaves
/// that error as the return value after the range is exhausted.  This helper
/// captures that deterministic count/error boundary without pretending to be
/// the storage-facing transaction loop (which still owns begin, rollback,
/// commit, logging, and jittered sleep).
#[must_use]
pub const fn should_retry_after_failure(
    attempt: u32,
    max_retry_count: u32,
    retryable_error: bool,
) -> bool {
    retryable_error && attempt.saturating_add(1) < max_retry_count
}

/// Returns the exclusive upper bound passed to Go's `rand.Intn` by `BackOff`.
///
/// The source computes `min(cap, base * 2^attempts)`, then samples a jitter
/// in `[0, upper)`. Saturating the shift preserves the same capped result for
/// arbitrarily large `uint` attempts without reproducing floating-point
/// overflow or introducing a sleep/randomness dependency.
#[must_use]
pub fn retry_backoff_upper_bound_ms(attempts: u32) -> u64 {
    RETRY_BACKOFF_BASE_MS
        .checked_shl(attempts)
        .unwrap_or(u64::MAX)
        .min(RETRY_BACKOFF_CAP_MS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_and_interrupted_waits_keep_separate_counts_and_sleep() {
        let mut budget = RegionBackoffBudget::new(Duration::from_secs(20));
        assert_eq!(
            budget.next_delay(RegionBackoffKind::RegionMiss).unwrap(),
            Duration::from_millis(2)
        );
        budget.finish_wait(true);
        assert_eq!(
            budget.next_delay(RegionBackoffKind::RegionMiss).unwrap(),
            Duration::from_millis(4)
        );
        budget.finish_wait(false);
        assert_eq!(budget.total_sleep(), Duration::from_millis(2));
        assert_eq!(
            budget.runtime_stats().collect::<Vec<_>>(),
            vec![("regionMiss", 2, Duration::from_millis(2))]
        );
        // Go's canceled backoffFn does not advance its exponential schedule.
        assert_eq!(
            budget.next_delay(RegionBackoffKind::RegionMiss).unwrap(),
            Duration::from_millis(4)
        );
        budget.finish_wait(true);
        budget
            .next_delay(RegionBackoffKind::TikvServerBusy)
            .unwrap();
        budget.finish_wait(false);
        assert_eq!(budget.remaining(), Duration::from_millis(19_994));
        assert_eq!(
            budget.runtime_stats().collect::<Vec<_>>(),
            vec![
                ("regionMiss", 3, Duration::from_millis(6)),
                ("tikvServerBusy", 1, Duration::ZERO),
            ]
        );
    }

    #[test]
    fn tiflash_wait_uses_native_jitter_budget_and_error_identity() {
        let mut budget = RegionBackoffBudget::new(Duration::from_millis(1));
        let delay = budget.next_delay(RegionBackoffKind::TiFlashRpc).unwrap();
        assert!((Duration::from_millis(50)..Duration::from_millis(100)).contains(&delay));
        budget.finish_wait(false);
        assert_eq!(budget.total_sleep(), Duration::ZERO);
        let delay = budget.next_delay(RegionBackoffKind::TiFlashRpc).unwrap();
        assert!((Duration::from_millis(50)..Duration::from_millis(100)).contains(&delay));
        budget.finish_wait(true);
        let exhausted = budget
            .next_delay(RegionBackoffKind::TiFlashRpc)
            .unwrap_err();
        assert_eq!(exhausted.kind, RegionBackoffKind::TiFlashRpc);
        assert_eq!(
            budget.runtime_stats().collect::<Vec<_>>(),
            vec![("tiflashRPC", 2, delay)]
        );
    }

    #[test]
    fn forked_history_replaces_the_parent_without_replacing_its_schedule() {
        let mut parent = RegionBackoffBudget::new(Duration::from_secs(20));
        parent.next_delay(RegionBackoffKind::RegionMiss).unwrap();
        parent.finish_wait(true);
        let mut first = parent.fork();
        let mut last = parent.fork();
        assert_eq!(
            first.next_delay(RegionBackoffKind::RegionMiss).unwrap(),
            Duration::from_millis(2)
        );
        first.finish_wait(true);
        last.next_delay(RegionBackoffKind::StaleCommand).unwrap();
        last.finish_wait(true);
        parent.update_from_forked(&first);
        parent.update_from_forked(&last);
        assert_eq!(
            parent.runtime_stats().collect::<Vec<_>>(),
            vec![
                ("regionMiss", 1, Duration::from_millis(2)),
                ("staleCommand", 1, Duration::from_millis(2)),
            ]
        );
        assert_eq!(
            parent.next_delay(RegionBackoffKind::RegionMiss).unwrap(),
            Duration::from_millis(4)
        );
    }
}
