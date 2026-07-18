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

//! Source-backed tests for DDL statistics count/modify-count delta SQL.

use tidb_stats::{
    ddl_stats_delta_update, DdlStatsDeltaUpdate, EXISTING_STATS_META_DELTA_QUERY,
    LOCKED_STATS_DELTA_QUERY, MISSING_STATS_META_DELTA_QUERY,
};

#[test]
fn source_locked_and_missing_branches_preserve_sql_and_arguments() {
    let locked = ddl_stats_delta_update(true, Some((100, 200)), 42, 7, -3, 5);
    assert_eq!(
        locked,
        DdlStatsDeltaUpdate {
            query: LOCKED_STATS_DELTA_QUERY,
            start_ts: 42,
            count_value: -3,
            modify_count_value: 5,
            table_id: 7,
        }
    );
    assert_eq!(locked.parameters(), (42, -3, 5, 7));

    let missing = ddl_stats_delta_update(false, None, 43, 8, -3, -4);
    assert_eq!(missing.query, MISSING_STATS_META_DELTA_QUERY);
    assert_eq!(missing.parameters(), (43, -3, -4, 8));
}

#[test]
fn source_existing_branch_adds_before_sql_greatest_and_wraps() {
    let existing = ddl_stats_delta_update(false, Some((2, 3)), 44, 9, -5, -7);
    assert_eq!(existing.query, EXISTING_STATS_META_DELTA_QUERY);
    assert_eq!(existing.parameters(), (44, -3, -4, 9));

    let wrapping =
        ddl_stats_delta_update(false, Some((i64::MAX, i64::MIN)), u64::MAX, i64::MIN, 1, -1);
    assert_eq!(
        wrapping.parameters(),
        (u64::MAX, i64::MIN, i64::MAX, i64::MIN)
    );
}
