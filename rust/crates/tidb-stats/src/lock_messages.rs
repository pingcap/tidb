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

//! Stable skipped-table and skipped-partition diagnostics from
//! `pkg/statistics/handle/lockstats/lock_stats.go`.
//!
//! The Go helpers sort the skipped names before formatting them. This leaf
//! accepts caller-owned names and returns the deterministic diagnostic only;
//! lock/unlock mutations, SQL/session state, and transaction handling remain
//! external boundaries.

/// Formats a skipped-table diagnostic using the source's sorted-name and
/// singular/plural rules.
#[must_use]
pub fn generate_stable_skipped_tables_message<S: AsRef<str>>(
    table_count: usize,
    skipped_names: &[S],
    action: &str,
    status: &str,
) -> String {
    if skipped_names.is_empty() {
        return String::new();
    }

    let tables = sorted_names(skipped_names);
    let noun = if table_count > 1 { "tables" } else { "table" };
    let message = format!("skip {action} {status} {noun}: {tables}");
    if table_count > 1 && table_count > skipped_names.len() {
        format!("{message}, other tables {status} successfully")
    } else {
        message
    }
}

/// Formats a skipped-partition diagnostic using the source's sorted-name and
/// singular/plural rules.
#[must_use]
pub fn generate_stable_skipped_partitions_message<S: AsRef<str>>(
    partition_ids: &[i64],
    table_name: &str,
    skipped_names: &[S],
    action: &str,
    status: &str,
) -> String {
    if skipped_names.is_empty() {
        return String::new();
    }

    let partitions = sorted_names(skipped_names);
    let noun = if partition_ids.len() > 1 {
        "partitions"
    } else {
        "partition"
    };
    let message = format!("skip {action} {status} {noun} of table {table_name}: {partitions}");
    if partition_ids.len() > skipped_names.len() && partition_ids.len() > 1 {
        format!("{message}, other partitions {status} successfully")
    } else {
        message
    }
}

fn sorted_names<S: AsRef<str>>(names: &[S]) -> String {
    let mut sorted = names.iter().map(|name| name.as_ref()).collect::<Vec<_>>();
    sorted.sort_unstable();
    sorted.join(", ")
}
