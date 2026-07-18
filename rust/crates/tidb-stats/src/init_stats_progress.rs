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

//! Init-stats progress arithmetic from
//! `pkg/statistics/handle/initstats/load_stats_page.go`.
//!
//! The Go range worker converts completed and total task counts to `float64`,
//! scales by the configured step, and adds the starting percentage. This leaf
//! preserves only that deterministic scalar rule; worker goroutines, channels,
//! logging, and the global atomic percentage remain external.

/// Computes the source-shaped percentage after one or more completed tasks.
///
/// The count inputs are converted to `f64` before division, matching Go's
/// `float64(uint64)` coercion. A zero denominator intentionally follows IEEE
/// floating-point behavior (`NaN` for `0/0`, infinity for a nonzero numerator)
/// rather than introducing an integer divide-by-zero branch.
#[must_use]
pub fn init_stats_progress(
    complete_task_count: u64,
    total_task_count: u64,
    total_percentage_step: f64,
    total_percentage: f64,
) -> f64 {
    (complete_task_count as f64) / (total_task_count as f64) * total_percentage_step
        + total_percentage
}
