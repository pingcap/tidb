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

//! Statistics table-ID filter formatting from
//! `pkg/statistics/handle/cache/stats_table_row_cache.go`.
//!
//! The Go row-count cache builds an SQL `table_id in (...)` predicate from
//! caller-ordered signed IDs. This leaf owns only that deterministic text
//! boundary; the cache, SQL execution, and InfoSchema readers remain external.

/// Formats the source-shaped `table_id in (...)` predicate.
///
/// IDs retain caller order and use decimal signed formatting. An empty input
/// intentionally yields `table_id in ()`, matching the Go builder's direct
/// open/close behavior.
#[must_use]
pub fn build_in_table_ids_string(table_ids: &[i64]) -> String {
    let ids = table_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    format!("table_id in ({ids})")
}
