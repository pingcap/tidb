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

//! Source-backed tests for locked-statistics delta extraction.

use tidb_stats::{stats_delta_from_rows, StatsDelta, SELECT_DELTA_SQL};

#[test]
fn source_empty_delta_rows_use_zero_values() {
    let rows: &[(i64, i64)] = &[];
    assert_eq!(
        stats_delta_from_rows::<&str>(Ok(rows)),
        Ok(StatsDelta::default())
    );
}

#[test]
fn source_delta_rows_read_the_first_count_and_modify_count() {
    let rows = [(1_i64, 1_i64), (7, 9)];
    assert_eq!(
        stats_delta_from_rows::<&str>(Ok(&rows)),
        Ok(StatsDelta {
            count: 1,
            modify_count: 1,
        })
    );
}

#[test]
fn source_delta_query_errors_are_returned_unchanged() {
    assert_eq!(
        stats_delta_from_rows::<&str>(Err("test error")),
        Err("test error")
    );
}

#[test]
fn source_delta_query_marker_is_exact() {
    assert_eq!(
        SELECT_DELTA_SQL,
        "SELECT count, modify_count FROM mysql.stats_table_locked WHERE table_id = %?"
    );
}
