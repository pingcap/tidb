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

//! Refresher queue-rebuild state from
//! `pkg/statistics/handle/autoanalyze/refresher/refresher.go`.
//!
//! The Go refresher rebuilds an already initialized priority queue when either
//! the parsed auto-analyze ratio or partition-prune mode changes. This leaf
//! keeps that scalar decision independent of sessions, queue workers, and
//! statistics handles.

/// Returns whether the source refresher should rebuild its queue.
#[must_use]
pub fn should_rebuild_queue(
    queue_initialized: bool,
    current_auto_analyze_ratio: f64,
    last_auto_analyze_ratio: f64,
    current_prune_mode: i64,
    last_prune_mode: i64,
) -> bool {
    queue_initialized
        && (current_auto_analyze_ratio != last_auto_analyze_ratio
            || current_prune_mode != last_prune_mode)
}
