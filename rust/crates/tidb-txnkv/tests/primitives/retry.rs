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

//! Direct source-shaped checks for the deterministic part of `pkg/kv/txn.go`.
//!
//! `BackOff` itself owns randomness and sleeping in the Go process. This ring
//! therefore claims only its exact capped exponential upper bound; retry
//! orchestration remains a session/storage responsibility.

use tidb_txnkv::{
    retry_backoff_upper_bound_ms, should_retry_after_failure, RETRY_BACKOFF_BASE_MS,
    RETRY_BACKOFF_CAP_MS,
};

/// Directly translates the bounds asserted by `pkg/kv/txn_test.go:28-37
/// TestBackOff` while keeping the random sleep outside `tidb-txnkv`.
#[test]
fn test_backoff_upper_bound() {
    assert_eq!(RETRY_BACKOFF_BASE_MS, 1);
    assert_eq!(RETRY_BACKOFF_CAP_MS, 100);
    assert_eq!(retry_backoff_upper_bound_ms(1), 2);
    assert_eq!(retry_backoff_upper_bound_ms(2), 4);
    assert_eq!(retry_backoff_upper_bound_ms(3), 8);
    assert_eq!(retry_backoff_upper_bound_ms(100_000), 100);
}

#[test]
fn test_backoff_bound_is_capped_without_shift_overflow() {
    assert_eq!(retry_backoff_upper_bound_ms(6), 64);
    assert_eq!(retry_backoff_upper_bound_ms(7), RETRY_BACKOFF_CAP_MS);
    assert_eq!(retry_backoff_upper_bound_ms(u32::MAX), RETRY_BACKOFF_CAP_MS);
}

/// Directly translates `pkg/kv/txn_test.go:39-69 TestRetryExceedCountError`'s
/// deterministic part: retryable failures continue for the first four
/// attempts of a five-attempt range, then the final retryable error is
/// returned; a non-retryable error returns immediately at any index.
#[test]
fn test_retry_exceed_count_boundary() {
    let max_retry_count = 5;
    for attempt in 0..4 {
        assert!(should_retry_after_failure(attempt, max_retry_count, true));
    }
    assert!(!should_retry_after_failure(4, max_retry_count, true));
    assert!(!should_retry_after_failure(0, max_retry_count, false));

    // The zero-based range performs exactly five attempts, with the
    // retryable error from the final attempt left for the caller to return.
    let mut attempts = 0;
    while should_retry_after_failure(attempts, max_retry_count, true) {
        attempts += 1;
    }
    assert_eq!(attempts + 1, max_retry_count);

    assert!(!should_retry_after_failure(0, 0, true));
    assert!(!should_retry_after_failure(u32::MAX, u32::MAX, true));
}
