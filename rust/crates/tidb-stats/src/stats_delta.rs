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

//! Locked-statistics delta extraction from
//! `pkg/statistics/handle/lockstats/unlock_stats.go`.
//!
//! The Go owner executes `SELECT count, modify_count` and reads the first
//! result row, defaulting to zero values when no row exists. This leaf keeps
//! only that row-shape rule; SQL execution, session context, version stamping,
//! failpoints, and unlock mutations remain external boundaries.

/// Exact source query used to load a locked statistics delta.
pub const SELECT_DELTA_SQL: &str =
    "SELECT count, modify_count FROM mysql.stats_table_locked WHERE table_id = %?";

/// Caller-owned count and modification-count values read from one locked row.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct StatsDelta {
    /// Current row count reported by the locked-statistics row.
    pub count: i64,
    /// Current modification count reported by the locked-statistics row.
    pub modify_count: i64,
}

/// Extracts the first locked-statistics row, preserving source error behavior.
///
/// An empty successful result is the source's zero-value `(0, 0)` delta. Extra
/// rows are ignored, matching the source helper's first-row read.
pub fn stats_delta_from_rows<E>(rows: Result<&[(i64, i64)], E>) -> Result<StatsDelta, E> {
    let rows = rows?;
    Ok(rows
        .first()
        .map_or_else(StatsDelta::default, |&(count, modify_count)| StatsDelta {
            count,
            modify_count,
        }))
}
