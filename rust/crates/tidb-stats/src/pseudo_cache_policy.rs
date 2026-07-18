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

//! Pseudo-statistics cache admission from `pkg/statistics/handle/handle.go`.
//!
//! TiDB admits pseudo-table statistics to the cache for non-partitioned tables
//! or while the cache has fewer than 64 entries, but never admits local/global
//! temporary-table statistics. This leaf owns only that scalar policy; pseudo
//! table construction, cache mutation, system-schema checks, failpoints, and
//! session/InfoSchema lifecycle remain external.

/// Source threshold below which partitioned pseudo tables may be cached.
pub const PSEUDO_CACHE_PARTITION_LIMIT: usize = 64;

/// Returns whether pseudo statistics should be inserted into the cache.
#[must_use]
pub const fn should_cache_pseudo_stats(
    is_partitioned: bool,
    cache_len: usize,
    is_temporary_table: bool,
) -> bool {
    if is_temporary_table {
        return false;
    }
    !is_partitioned || cache_len < PSEUDO_CACHE_PARTITION_LIMIT
}
