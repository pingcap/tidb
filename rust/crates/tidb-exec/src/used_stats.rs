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

//! Used-statistics slow-log formatting from `stmtctx.go`.
//!
//! This leaf owns the deterministic text boundary of
//! `UsedStatsInfoForTable.WriteToSlowLog`: pseudo versus real statistics
//! versions, realtime/modify counts, and sorted index/column load-status
//! entries. It intentionally uses the source fallback `ID <id>` because
//! table metadata/name lookup belongs to the statistics/session owners.
//! Collection, `FormatForExplain`, and slow-log I/O remain external.

use std::collections::BTreeMap;
use std::fmt::Write;

/// Statistics metadata used by the source slow-log formatter.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UsedStatsInfoForTable {
    /// Table or partition name.
    pub name: String,
    /// Statistics metadata version; zero means pseudo statistics.
    pub version: u64,
    /// Realtime row count.
    pub realtime_count: i64,
    /// Modified row count.
    pub modify_count: i64,
    /// Index ID to load-status text.
    pub index_stats_load_status: BTreeMap<i64, String>,
    /// Column ID to load-status text.
    pub column_stats_load_status: BTreeMap<i64, String>,
}

impl UsedStatsInfoForTable {
    /// Formats the source slow-log payload without performing I/O.
    #[must_use]
    pub fn write_to_slow_log(&self) -> String {
        let version = if self.version == 0 {
            "pseudo".to_owned()
        } else {
            self.version.to_string()
        };
        let mut output = format!(
            "{}:stats_meta_version={}[realtime_count={};modify_count={}]",
            self.name, version, self.realtime_count, self.modify_count
        );

        // The source returns immediately for pseudo statistics and therefore
        // never appends status sections in that mode.
        if self.version == 0 {
            return output;
        }
        if self.index_stats_load_status.is_empty() && self.column_stats_load_status.is_empty() {
            return output;
        }

        output.push('[');
        append_statuses(&mut output, &self.index_stats_load_status);
        output.push(']');
        output.push('[');
        append_statuses(&mut output, &self.column_stats_load_status);
        output.push(']');
        output
    }
}

fn append_statuses(output: &mut String, statuses: &BTreeMap<i64, String>) {
    let mut first = true;
    for (id, status) in statuses {
        if !first {
            output.push(',');
        }
        first = false;
        let _ = write!(output, "ID {}:{}", id, status);
    }
}
