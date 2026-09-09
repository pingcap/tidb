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

//! Complete transcreation of `pkg/util/backoff`.
//!
//! Go's `time.Duration` is a signed nanosecond count, so this module keeps the
//! source `i64` domain instead of narrowing it to Rust's nonnegative
//! [`std::time::Duration`]. `BUILD.bazel`'s library and test declarations map
//! to this module and its unit tests; the source package has no TestMain,
//! benchmarks, build tags, generated files, fixtures, or other support data.

/// Go `time.Duration`, represented as its signed nanosecond count.
pub type Duration = i64;

/// Source `Backoffer` interface.
pub trait Backoffer {
    /// Returns the duration to wait for the `retry_count`-th retry.
    ///
    /// `retry_count` starts from zero.
    fn backoff(&mut self, retry_count: isize) -> Duration;
}

/// Source `Exponential` backoff without jitter.
#[derive(Clone, Default)]
pub struct Exponential {
    base_backoff: Duration,
    multiplier: f64,
    max_backoff: Duration,
    next_backoff: Duration,
}

/// Source `NewExponential`.
pub fn new_exponential(
    base_backoff: Duration,
    multiplier: f64,
    max_backoff: Duration,
) -> Exponential {
    Exponential {
        base_backoff,
        multiplier,
        max_backoff,
        next_backoff: base_backoff,
    }
}

impl Exponential {
    /// Source `(*Exponential).Backoff`.
    pub fn backoff(&mut self, retry_count: isize) -> Duration {
        if retry_count == 0 {
            self.next_backoff = self.base_backoff;
            return self.next_backoff;
        }

        // Rust's float-to-integer cast matches Go here: finite values truncate,
        // NaN becomes zero, and infinities or out-of-range values saturate.
        self.next_backoff =
            ((self.next_backoff as f64 * self.multiplier) as Duration).min(self.max_backoff);
        self.next_backoff
    }
}

impl Backoffer for Exponential {
    fn backoff(&mut self, retry_count: isize) -> Duration {
        Exponential::backoff(self, retry_count)
    }
}

#[cfg(test)]
mod tests {
    use super::{new_exponential, Duration};

    #[test]
    fn test_exponential() {
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
}
