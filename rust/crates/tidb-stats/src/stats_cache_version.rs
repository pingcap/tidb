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

//! Monotonic statistics-cache version advancement from
//! `pkg/statistics/handle/cache/statscacheinner.go`.
//!
//! The cache tracks the greatest table-statistics version observed during its
//! lifetime. This leaf owns only the caller-provided max/skip arithmetic;
//! atomic publication, cache backends, SQL loading, metrics, and Handle
//! lifecycle remain external.

/// Returns the cache's next maximum version after a table update.
///
/// When `skip_move_forward` is true, the current version is preserved even if
/// newer table versions are supplied. Otherwise, the greatest supplied version
/// advances the current value, and smaller versions never move it backward.
#[must_use]
pub fn max_stats_cache_version(
    current_version: u64,
    table_versions: &[u64],
    skip_move_forward: bool,
) -> u64 {
    if skip_move_forward {
        current_version
    } else {
        table_versions
            .iter()
            .copied()
            .fold(current_version, u64::max)
    }
}
