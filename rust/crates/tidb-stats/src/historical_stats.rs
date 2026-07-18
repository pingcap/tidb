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

//! Historical-statistics version selection from
//! `pkg/statistics/handle/history/history_stats.go`.
//!
//! The Go storage writer uses the table JSON version for a non-partitioned
//! table and the maximum partition version for a partitioned table. This leaf
//! keeps that scalar decision over caller-owned versions; JSON blocks,
//! timestamps, SQL, and storage/session execution remain external.

/// Selects the version written to historical statistics storage.
///
/// An empty partition list selects `table_version`. A non-empty list starts
/// from zero and returns its maximum, intentionally ignoring `table_version`
/// just like the source's partition loop.
#[must_use]
pub fn historical_stats_version(table_version: u64, partition_versions: &[u64]) -> u64 {
    if partition_versions.is_empty() {
        table_version
    } else {
        partition_versions.iter().copied().max().unwrap_or(0)
    }
}
