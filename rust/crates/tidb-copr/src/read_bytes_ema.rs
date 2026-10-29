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

//! Concurrent read-byte exponential moving average translated from
//! `pkg/store/copr/ema.go`.
//!
//! This is the deterministic feedback state used by coprocessor byte paging.
//! Extracting it from the future iterator/RPC owner preserves the source's
//! synchronization and time-decay contract without inventing transport state.

use std::sync::Mutex;
use std::time::Duration;

const DEFAULT_TAU: Duration = Duration::from_secs(1);

#[derive(Debug)]
struct State {
    value: f64,
    last_observed_at: Duration,
}

/// Time-aware moving average of MVCC bytes read by a paging response.
///
/// Observation timestamps are durations from a fixed epoch compatible with
/// Go's zero `time.Time`, not process-relative elapsed time. The future clock
/// adapter must preserve that absolute/source-zero distance so a real first
/// observation has the source's effectively unit alpha while an actual zero
/// timestamp retains zero weight.
#[derive(Debug)]
pub struct ReadBytesEma {
    state: Mutex<State>,
}

impl ReadBytesEma {
    /// Creates an EMA with the source one-second decay constant.
    ///
    /// The seed is immediately visible to [`Self::predict`], but the first
    /// real observation replaces it because the source leaves its timestamp at
    /// zero and therefore computes an effectively unit alpha.
    #[must_use]
    pub fn new(seed_read_bytes: u64) -> Self {
        Self {
            state: Mutex::new(State {
                value: seed_read_bytes as f64,
                last_observed_at: Duration::ZERO,
            }),
        }
    }

    /// Adds one byte-count observation at `now`.
    ///
    /// Older timestamps are clamped to a zero delta and never rewind the last
    /// observation, exactly matching Go's `dt < 0` and `now.After` branches.
    pub fn observe(&self, bytes: u64, now: Duration) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let elapsed = now
            .checked_sub(state.last_observed_at)
            .unwrap_or(Duration::ZERO);
        let alpha = 1.0 - (-elapsed.as_secs_f64() / DEFAULT_TAU.as_secs_f64()).exp();
        state.value += alpha * (bytes as f64 - state.value);
        if now > state.last_observed_at {
            state.last_observed_at = now;
        }
    }

    /// Returns the current estimate using Rust's source-compatible conversion
    /// over the source-tested representable range.
    #[must_use]
    pub fn predict(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .value as u64
    }
}
