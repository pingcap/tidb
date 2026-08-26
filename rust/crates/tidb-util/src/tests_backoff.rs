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

//! Ports of `pkg/util/backoff` unit tests from `origin/master`.

use crate::backoff::{new_exponential, Backoffer, Duration};

/// Port of `backoff_test.go` `TestExponential`.
///
/// Re-derived from `backoff.go`: `Backoff(0)` always resets to the base;
/// subsequent retries scale `next_backoff` by the multiplier, clamped at
/// `max_backoff`. Durations are signed nanosecond counts (`time.Duration`).
#[test]
fn exponential() {
    let mut backoffer = new_exponential(1, 1.0, 1);
    for retry_count in 0..10 {
        assert_eq!(backoffer.backoff(retry_count), 1);
    }

    let mut backoffer = new_exponential(1, 1.0, 10);
    for retry_count in 0..10 {
        assert_eq!(backoffer.backoff(retry_count), 1);
    }

    let mut backoffer = new_exponential(1, 2.0, 10);
    let expected: [Duration; 10] = [1, 2, 4, 8, 10, 10, 10, 10, 10, 10];
    for (retry_count, expected) in expected.into_iter().enumerate() {
        assert_eq!(backoffer.backoff(retry_count as isize), expected);
    }
}
