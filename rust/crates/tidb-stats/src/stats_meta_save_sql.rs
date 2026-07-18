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

//! Batch `stats_meta` SQL assembly from
//! `pkg/statistics/handle/storage/save.go`.
//!
//! The Go save path formats caller-ordered metadata tuples and chooses an
//! optional `last_stats_histograms_version` column. This leaf preserves that
//! exact SQL text while leaving start-timestamp acquisition, execution, and
//! transaction/session ownership external.

/// Caller-owned scalar form of a Go `statstypes.MetaUpdate`.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct StatsMetaSaveUpdate {
    /// Physical table or partition ID.
    pub physical_id: i64,
    /// Current row count.
    pub count: i64,
    /// Current modification count.
    pub modify_count: i64,
}

impl StatsMetaSaveUpdate {
    /// Creates one source-shaped metadata update tuple.
    #[must_use]
    pub const fn new(physical_id: i64, count: i64, modify_count: i64) -> Self {
        Self {
            physical_id,
            count,
            modify_count,
        }
    }
}

/// Builds the exact `SaveMetaToStorage` INSERT/upsert SQL text.
///
/// Tuple order follows `updates` exactly. The empty-input result intentionally
/// retains Go's two spaces between `values` and `on duplicate key update`.
#[must_use]
pub fn stats_meta_save_sql(
    version: u64,
    refresh_last_hist_ver: bool,
    updates: &[StatsMetaSaveUpdate],
) -> String {
    let values = if refresh_last_hist_ver {
        updates
            .iter()
            .map(|update| {
                format!(
                    "({}, {}, {}, {}, {})",
                    version, update.physical_id, update.count, update.modify_count, version
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    } else {
        updates
            .iter()
            .map(|update| {
                format!(
                    "({}, {}, {}, {})",
                    version, update.physical_id, update.count, update.modify_count
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    };

    if refresh_last_hist_ver {
        format!(
            "insert into mysql.stats_meta (version, table_id, count, modify_count, last_stats_histograms_version) values {} on duplicate key update version = values(version), modify_count = values(modify_count), count = values(count), last_stats_histograms_version = values(last_stats_histograms_version)",
            values
        )
    } else {
        format!(
            "insert into mysql.stats_meta (version, table_id, count, modify_count) values {} on duplicate key update version = values(version), modify_count = values(modify_count), count = values(count)",
            values
        )
    }
}
