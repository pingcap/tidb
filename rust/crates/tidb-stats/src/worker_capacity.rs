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

//! Auto-analyze worker capacity metadata from
//! `pkg/statistics/handle/autoanalyze/refresher/worker.go`.
//!
//! The source admits a job while `len(runningJobs) < maxConcurrency` and
//! treats an unchanged concurrency setting as a no-op. This leaf keeps those
//! scalar transitions independent of goroutines, hooks, and job execution.

/// Returns whether one more job may be admitted under the source limit.
#[must_use]
pub fn worker_capacity_available(running_jobs: usize, max_concurrency: i64) -> bool {
    max_concurrency > 0 && running_jobs < max_concurrency as usize
}

/// Returns whether `UpdateConcurrency` needs to mutate worker state.
#[must_use]
pub const fn worker_concurrency_changed(old: i64, new: i64) -> bool {
    old != new
}
