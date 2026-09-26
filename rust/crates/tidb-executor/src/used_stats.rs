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

//! Used-statistics formatting from `stmtctx.go`.
//!
//! This leaf owns the deterministic text boundary of
//! `UsedStatsInfoForTable` slow-log and EXPLAIN payloads: pseudo versus real
//! statistics versions, realtime/modify counts, and sorted index/column
//! load-status entries. Callers supply schema-resolved names because table
//! metadata lookup belongs to the statistics/session owners.

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
    /// Formats the source EXPLAIN suffix for this table's recorded statuses.
    ///
    /// Go prints up to three statuses, columns before indexes, and then a
    /// count grouped by status for the remainder. Missing schema names fall
    /// back to `ID <id>`.
    #[must_use]
    pub fn format_for_explain(
        &self,
        column_names: &BTreeMap<i64, String>,
        index_names: &BTreeMap<i64, String>,
    ) -> String {
        if self.version == 0 {
            return "stats:pseudo".to_owned();
        }
        if self.column_stats_load_status.is_empty() && self.index_stats_load_status.is_empty() {
            return String::new();
        }

        let mut output = String::from("stats:partial[");
        let mut output_left = 3;
        let mut status_counts = BTreeMap::<String, usize>::new();
        let mut first = true;
        append_explain_statuses(
            &mut output,
            &mut first,
            &mut output_left,
            &mut status_counts,
            &self.column_stats_load_status,
            column_names,
        );
        append_explain_statuses(
            &mut output,
            &mut first,
            &mut output_left,
            &mut status_counts,
            &self.index_stats_load_status,
            index_names,
        );
        if !status_counts.is_empty() {
            if !first {
                output.push_str(", ");
            }
            output.push_str("...(more: ");
            for (index, (status, count)) in status_counts.iter().enumerate() {
                if index > 0 {
                    output.push_str(", ");
                }
                let _ = write!(output, "{count} {status}");
            }
            output.push(')');
        }
        output.push(']');
        output
    }

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

fn append_explain_statuses(
    output: &mut String,
    first: &mut bool,
    output_left: &mut usize,
    status_counts: &mut BTreeMap<String, usize>,
    statuses: &BTreeMap<i64, String>,
    names: &BTreeMap<i64, String>,
) {
    for (id, status) in statuses {
        if *output_left == 0 {
            *status_counts.entry(status.clone()).or_default() += 1;
            continue;
        }
        if !*first {
            output.push_str(", ");
        }
        *first = false;
        let name = names
            .get(id)
            .filter(|name| !name.is_empty())
            .cloned()
            .unwrap_or_else(|| format!("ID {id}"));
        let _ = write!(output, "{name}:{status}");
        *output_left -= 1;
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
