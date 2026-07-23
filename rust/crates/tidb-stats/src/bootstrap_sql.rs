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

//! Statistics bootstrap SQL generation from `pkg/statistics/handle/bootstrap.go`.
//!
//! The Go owner builds exact `stats_meta` and `stats_histograms` queries for
//! full loads, table-ID loads, and closed-open paging ranges. This leaf keeps
//! only deterministic SQL text and validates option construction; Handle,
//! session, result decoding, cache population, and query execution remain
//! external boundaries.

const STATS_META_SELECT_PREFIX: &str =
    "select HIGH_PRIORITY version, table_id, modify_count, count, snapshot, last_stats_histograms_version from mysql.stats_meta";

const STATS_HISTOGRAMS_SELECT_PREFIX: &str = "select /*+ ORDER_INDEX(mysql.stats_histograms,tbl) */ HIGH_PRIORITY table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch, tot_col_size, stats_ver, correlation from mysql.stats_histograms";

/// Validated options for loading statistics-histogram rows.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct HistSqlOptions {
    mode: HistSqlMode,
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum HistSqlMode {
    TableIds(Vec<i64>),
    Paging { start: i64, end: i64 },
}

impl HistSqlOptions {
    /// Creates a non-paging option, preserving caller table-ID order.
    ///
    /// The Go constructor asserts that IDs are non-negative. Returning `None`
    /// makes that source contract explicit without allowing an invalid option
    /// to reach SQL generation.
    #[must_use]
    pub fn for_table_ids(table_ids: &[i64]) -> Option<Self> {
        if table_ids.iter().any(|id| *id < 0) {
            return None;
        }
        Some(Self {
            mode: HistSqlMode::TableIds(table_ids.to_vec()),
        })
    }

    /// Creates a paging option for a valid closed-open range.
    #[must_use]
    pub fn for_paging(start: i64, end: i64) -> Option<Self> {
        (start < end).then_some(Self {
            mode: HistSqlMode::Paging { start, end },
        })
    }
}

/// Generates the exact source `stats_meta` initialization query.
#[must_use]
pub fn gen_init_stats_meta_sql(table_ids: &[i64]) -> String {
    if table_ids.is_empty() {
        return STATS_META_SELECT_PREFIX.to_owned();
    }
    format!(
        "{STATS_META_SELECT_PREFIX} where table_id in ({})",
        tidb_util::slice::int64s_to_strings(table_ids).join(",")
    )
}

/// Generates the exact source `stats_histograms` initialization query.
#[must_use]
pub fn gen_init_stats_histograms_sql(options: &HistSqlOptions) -> String {
    let suffix = match &options.mode {
        HistSqlMode::TableIds(table_ids) if table_ids.is_empty() => String::new(),
        HistSqlMode::TableIds(table_ids) => format!(
            " where table_id in ({})",
            tidb_util::slice::int64s_to_strings(table_ids).join(",")
        ),
        HistSqlMode::Paging { start, end } => {
            format!(" where table_id >= {start} and table_id < {end}")
        }
    };
    format!("{STATS_HISTOGRAMS_SELECT_PREFIX}{suffix} order by table_id")
}
