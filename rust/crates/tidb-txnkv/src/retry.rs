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

//! Dependency-closed retry backoff arithmetic from `pkg/kv/txn.go`.
//!
//! TiDB's `BackOff` adds process-global randomness and sleeps. This crate owns
//! source-valid delay arithmetic and an injectable jitter seam, while actual
//! sleeping and cancellation remain with the response owner.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

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
}

impl RegionBackoffKind {
    const COUNT: usize = 9;

    const fn config(self) -> (u64, u64, bool) {
        match self {
            Self::RegionMiss | Self::RegionScheduling => (2, 500, false),
            Self::TikvServerBusy => (2_000, 10_000, true),
            Self::TikvDiskFull => (500, 5_000, false),
            Self::RegionRecoveryInProgress => (100, 10_000, true),
            Self::StaleCommand => (2, 1_000, false),
            Self::MaxTimestampNotSynced => (2, 500, false),
            Self::RegionNotInitialized => (2, 1_000, false),
            Self::IsWitness => (1_000, 10_000, true),
        }
    }
}

/// Source-shaped backoff budget arithmetic without sleeping.
///
/// Equal-jitter categories use a tiny injected-seed generator so tests can be
/// deterministic while every result remains in client-go's `[v/2, v)` range.
/// `TikvServerBusy` sleep is excluded from the effective 20-second budget up
/// to client-go's pinned 10-minute exclusion limit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegionBackoffBudget {
    max_sleep_ms: u64,
    total_sleep_ms: u64,
    excluded_sleep_ms: u64,
    attempts: [u32; RegionBackoffKind::COUNT],
    jitter_state: u64,
}

impl RegionBackoffBudget {
    /// Creates one per-region budget with a varying source-valid jitter stream.
    #[must_use]
    pub fn new(max_sleep: Duration) -> Self {
        Self {
            max_sleep_ms: duration_ms(max_sleep),
            total_sleep_ms: 0,
            excluded_sleep_ms: 0,
            attempts: [0; RegionBackoffKind::COUNT],
            jitter_state: entropy_seed(),
        }
    }

    /// Creates Campaign 11's 20-second effective budget for one failed region.
    #[must_use]
    pub fn campaign_default() -> Self {
        Self::new(REGION_RETRY_MAX_SLEEP)
    }

    /// Creates a deterministic test budget with an injected jitter seed.
    #[must_use]
    pub const fn with_jitter_seed(max_sleep: Duration, seed: u64) -> Self {
        Self {
            max_sleep_ms: duration_ms(max_sleep),
            total_sleep_ms: 0,
            excluded_sleep_ms: 0,
            attempts: [0; RegionBackoffKind::COUNT],
            jitter_state: seed,
        }
    }

    /// Reserves the next source-shaped delay without sleeping.
    pub fn next_delay(
        &mut self,
        kind: RegionBackoffKind,
    ) -> Result<Duration, RegionBackoffExhausted> {
        let effective_exhausted = self.effective_sleep_ms() >= self.max_sleep_ms;
        let excluded_exhausted =
            self.excluded_sleep_ms >= 600_000 && self.excluded_sleep_ms >= self.max_sleep_ms;
        if self.max_sleep_ms > 0 && (effective_exhausted || excluded_exhausted) {
            return Err(RegionBackoffExhausted {
                kind,
                max_sleep: Duration::from_millis(self.max_sleep_ms),
            });
        }

        let index = kind as usize;
        let attempt = self.attempts[index];
        let (base, cap, equal_jitter) = kind.config();
        let exponential = base.checked_shl(attempt).unwrap_or(u64::MAX).min(cap);
        let computed = if equal_jitter {
            let half = exponential / 2;
            half + self.next_jitter(exponential.saturating_sub(half))
        } else {
            exponential
        };
        let delay_ms = computed;

        self.attempts[index] = attempt.saturating_add(1);
        self.total_sleep_ms = self.total_sleep_ms.saturating_add(delay_ms);
        if kind == RegionBackoffKind::TikvServerBusy {
            self.excluded_sleep_ms = self.excluded_sleep_ms.saturating_add(delay_ms);
        }
        Ok(Duration::from_millis(delay_ms))
    }

    /// Returns all reserved sleep, including client-go-excluded busy sleep.
    #[must_use]
    pub const fn total_sleep(&self) -> Duration {
        Duration::from_millis(self.total_sleep_ms)
    }

    /// Returns the unspent effective budget.
    #[must_use]
    pub const fn remaining(&self) -> Duration {
        Duration::from_millis(self.max_sleep_ms.saturating_sub(self.effective_sleep_ms()))
    }

    const fn effective_sleep_ms(&self) -> u64 {
        self.total_sleep_ms.saturating_sub(self.excluded_sleep_ms)
    }

    fn next_jitter(&mut self, width: u64) -> u64 {
        if width == 0 {
            return 0;
        }
        let mut state = self.jitter_state;
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        self.jitter_state = state;
        state % width
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

fn entropy_seed() -> u64 {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0x9e37_79b9_7f4a_7c15);
    let sequence = SEQUENCE.fetch_add(0x9e37_79b9_7f4a_7c15, Ordering::Relaxed);
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_secs() ^ u64::from(duration.subsec_nanos())
        });
    let seed = sequence ^ time.rotate_left(17);
    if seed == 0 {
        0xa076_1d64_78bd_642f
    } else {
        seed
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
