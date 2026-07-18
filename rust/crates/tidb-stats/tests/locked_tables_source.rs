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

//! Source-backed tests for statistics-table lock filtering.

use std::collections::HashSet;

use tidb_stats::{get_locked_tables, SELECT_LOCKED_TABLES_SQL};

#[test]
fn source_locked_table_filter_returns_only_requested_locked_ids() {
    let locked = HashSet::from([1_i64, 2, -3]);
    let requested = [1, 2, 2, 3, -3, 9];
    assert_eq!(
        get_locked_tables(&locked, &requested),
        HashSet::from([1_i64, 2, -3])
    );
}

#[test]
fn source_locked_table_filter_empty_locked_set_is_empty() {
    let locked = HashSet::new();
    assert!(get_locked_tables(&locked, &[1, 2, 3]).is_empty());
    assert!(get_locked_tables(&locked, &[]).is_empty());
}

#[test]
fn source_locked_table_query_marker_is_exact() {
    assert_eq!(
        SELECT_LOCKED_TABLES_SQL,
        "SELECT table_id FROM mysql.stats_table_locked"
    );
}
