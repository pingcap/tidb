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

use std::collections::VecDeque;
use std::time::Duration;

const SLOW_SCORE_INITIAL: u64 = 1;
const SLOW_SCORE_THRESHOLD: u64 = 80;
const SLOW_SCORE_MAX: u64 = 100;
const INITIAL_TIMEOUT_MICROS: u64 = 500_000;
const MAX_TIMEOUT_MICROS: u64 = 30_000_000;
const SLIDING_WINDOW_SIZE: usize = 10;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct CountSlidingWindow {
    average: u64,
    sum: u64,
    history: VecDeque<u64>,
}

impl CountSlidingWindow {
    fn append(&mut self, value: u64) -> f64 {
        let previous_average = self.average;
        if self.history.len() == SLIDING_WINDOW_SIZE {
            self.sum -= self
                .history
                .pop_front()
                .expect("a full window has a first element");
        }
        self.sum = self.sum.saturating_add(value);
        self.history.push_back(value);
        self.average = self.sum / self.history.len() as u64;
        if previous_average > 0 && value != previous_average {
            (value as f64 - previous_average as f64) / previous_average as f64
        } else {
            1e-6
        }
    }
}

/// Client-observed latency/QPS trend copied from pinned client-go.
///
/// RegionCache is the sole mutator, so Rust needs no atomics inside the value.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SlowScoreStat {
    average_score: u64,
    average_time_cost_micros: u64,
    interval_time_cost_micros: u64,
    interval_update_count: u64,
    time_cost_window: CountSlidingWindow,
    update_count_window: CountSlidingWindow,
}

impl SlowScoreStat {
    /// Current client-side score.
    #[must_use]
    pub const fn score(&self) -> u64 {
        self.average_score
    }

    /// Whether the score meets client-go's inclusive slow threshold of 80.
    #[must_use]
    pub const fn is_slow(&self) -> bool {
        self.average_score >= SLOW_SCORE_THRESHOLD
    }

    /// Records one completed request duration in the current tick.
    pub fn record(&mut self, time_cost: Duration) {
        self.interval_update_count = self.interval_update_count.saturating_add(1);
        if self.average_time_cost_micros == 0 {
            self.average_score = SLOW_SCORE_INITIAL;
            self.average_time_cost_micros = INITIAL_TIMEOUT_MICROS;
            self.interval_time_cost_micros = duration_micros(time_cost);
            return;
        }
        let current = duration_micros(time_cost);
        if current >= MAX_TIMEOUT_MICROS {
            self.average_score = SLOW_SCORE_MAX;
            return;
        }
        self.interval_time_cost_micros = self.interval_time_cost_micros.saturating_add(current);
    }

    /// Advances one source timing tick.
    pub fn tick(&mut self) {
        if self.average_time_cost_micros == 0 {
            self.average_score = SLOW_SCORE_INITIAL;
            self.average_time_cost_micros = INITIAL_TIMEOUT_MICROS;
            return;
        }

        let mut update_gradient = 1.0;
        let mut time_gradient = 1.0;
        if self.interval_update_count > 0 {
            let interval_average = self.interval_time_cost_micros / self.interval_update_count;
            update_gradient = self.update_count_window.append(self.interval_update_count);
            time_gradient = self.time_cost_window.append(interval_average);
        }

        if update_gradient + 0.1 <= 1e-9 && time_gradient - 0.1 >= 1e-9 {
            let rise = (time_gradient / update_gradient).abs().min(5.43);
            self.average_score =
                ((self.average_score as f64 * rise + 1.0).min(SLOW_SCORE_MAX as f64)).ceil() as u64;
        } else {
            let cost = (1.0 + update_gradient.abs()).clamp(1.0, 2.71).ceil() as u64;
            self.average_score = if self.average_score <= SLOW_SCORE_INITIAL + cost {
                SLOW_SCORE_INITIAL
            } else {
                self.average_score - cost
            };
        }
        self.average_time_cost_micros = self.time_cost_window.average;
        self.interval_time_cost_micros = 0;
        self.interval_update_count = 0;
    }

    /// Marks a store immediately slow after a zero-estimate busy response.
    pub fn mark_already_slow(&mut self) {
        self.average_score = SLOW_SCORE_MAX;
    }
}

fn duration_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}
