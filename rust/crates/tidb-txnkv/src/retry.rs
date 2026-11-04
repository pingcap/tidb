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

/// Synchronous wait adapter over the native client-go retry owner.
/// All limits, delay schedules, fork history and diagnostics live in client-rust.
pub struct RegionBackoffBudget {
    owner: Arc<Mutex<RetryBackoffer>>,
    pending: Option<(RetryConfig, BackoffWait)>,
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
        config: RetryConfig,
    ) -> Result<Duration, RegionBackoffExhausted> {
        self.next_delay_capped(config, Duration::MAX)
    }

    /// Caps this wait by the observed lock TTL without resetting its schedule.
    pub fn next_delay_capped(
        &mut self,
        config: RetryConfig,
        max_sleep: Duration,
    ) -> Result<Duration, RegionBackoffExhausted> {
        self.finish_wait(true);
        let mut owner = self
            .owner
            .try_lock()
            .expect("retry owner is exclusively borrowed");
        let wait = owner
            .prepare_backoff(config, Some(duration_ms(max_sleep)), config.name)
            .map_err(|_| Self::exhausted(&owner, config))?;
        let duration = wait.duration();
        self.pending = Some((config, wait));
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
        fallback: RetryConfig,
    ) -> RegionBackoffExhausted {
        // client-go `Backoffer.Backoff`: the error names the longest-slept
        // class.
        let config = owner.longest_sleep_config().unwrap_or(fallback);
        RegionBackoffExhausted {
            config,
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
        let mut names: Vec<&'static str> = owner.times_by_type().keys().copied().collect();
        if let Some((config, _)) = &self.pending {
            if !names.contains(&config.name) {
                names.push(config.name);
            }
        }
        names
            .into_iter()
            .filter_map(|name| {
                let pending = self.pending.as_ref().filter(|(pending, _)| pending.name == name);
                let count = owner.times_by_type().get(name).copied().unwrap_or(0)
                    + u64::from(pending.is_some());
                let sleep = Duration::from_millis(owner.sleep_by_type().get(name).copied().unwrap_or(0))
                    + pending.map_or(Duration::ZERO, |(_, wait)| wait.duration());
                (count != 0).then_some((name, count, sleep))
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
            .filter(|(config, _)| config.excluded_budget_limit_ms.is_none())
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
    /// client-go config of the class that exhausted the budget.
    pub config: RetryConfig,
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
            budget.next_delay(tikv_client::retry::BO_REGION_MISS).unwrap(),
            Duration::from_millis(2)
        );
        budget.finish_wait(true);
        assert_eq!(
            budget.next_delay(tikv_client::retry::BO_REGION_MISS).unwrap(),
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
            budget.next_delay(tikv_client::retry::BO_REGION_MISS).unwrap(),
            Duration::from_millis(4)
        );
        budget.finish_wait(true);
        budget
            .next_delay(tikv_client::retry::BO_TIKV_SERVER_BUSY)
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
        let delay = budget.next_delay(tikv_client::retry::BO_TIFLASH_RPC).unwrap();
        assert!((Duration::from_millis(50)..Duration::from_millis(100)).contains(&delay));
        budget.finish_wait(false);
        assert_eq!(budget.total_sleep(), Duration::ZERO);
        let delay = budget.next_delay(tikv_client::retry::BO_TIFLASH_RPC).unwrap();
        assert!((Duration::from_millis(50)..Duration::from_millis(100)).contains(&delay));
        budget.finish_wait(true);
        let exhausted = budget
            .next_delay(tikv_client::retry::BO_TIFLASH_RPC)
            .unwrap_err();
        assert_eq!(exhausted.config, tikv_client::retry::BO_TIFLASH_RPC);
        assert_eq!(
            budget.runtime_stats().collect::<Vec<_>>(),
            vec![("tiflashRPC", 2, delay)]
        );
    }

    #[test]
    fn forked_history_replaces_the_parent_without_replacing_its_schedule() {
        let mut parent = RegionBackoffBudget::new(Duration::from_secs(20));
        parent.next_delay(tikv_client::retry::BO_REGION_MISS).unwrap();
        parent.finish_wait(true);
        let mut first = parent.fork();
        let mut last = parent.fork();
        assert_eq!(
            first.next_delay(tikv_client::retry::BO_REGION_MISS).unwrap(),
            Duration::from_millis(2)
        );
        first.finish_wait(true);
        last.next_delay(tikv_client::retry::BO_STALE_CMD).unwrap();
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
            parent.next_delay(tikv_client::retry::BO_REGION_MISS).unwrap(),
            Duration::from_millis(4)
        );
    }
}
