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
//! TiDB's `BackOff` adds process-global randomness and sleeps, so that
//! orchestration stays outside this crate. The deterministic upper bound is
//! still a useful source-backed contract for clients that own the retry loop.

/// Initial exponential-backoff bound, in the effective sleep unit used by
/// `pkg/kv/txn.go` (`time.Millisecond`).
pub const RETRY_BACKOFF_BASE_MS: u64 = 1;

/// Maximum exponential-backoff bound, in milliseconds.
pub const RETRY_BACKOFF_CAP_MS: u64 = 100;

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
