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

//! Source-backed tests for used-statistics slow-log formatting.

use std::collections::BTreeMap;

use tidb_exec::used_stats::UsedStatsInfoForTable;

#[test]
fn used_stats_slow_log_matches_source_cases() {
    // Source: pkg/sessionctx/stmtctx/stmtctx_test.go:592-636 and
    // pkg/sessionctx/stmtctx/stmtctx.go:1500-1520.
    let pseudo = UsedStatsInfoForTable {
        name: "t1".to_owned(),
        version: 0,
        realtime_count: 1000,
        modify_count: 100,
        ..UsedStatsInfoForTable::default()
    };
    assert_eq!(
        pseudo.write_to_slow_log(),
        "t1:stats_meta_version=pseudo[realtime_count=1000;modify_count=100]"
    );

    let real = UsedStatsInfoForTable {
        name: "orders".to_owned(),
        version: 5,
        realtime_count: 1_000_000,
        modify_count: 500,
        ..UsedStatsInfoForTable::default()
    };
    assert_eq!(
        real.write_to_slow_log(),
        "orders:stats_meta_version=5[realtime_count=1000000;modify_count=500]"
    );

    // The source prints index statuses before column statuses and falls back
    // to `ID <id>` when TableInfo is nil.
    let mut index_status = BTreeMap::new();
    index_status.insert(1, "allLoaded".to_owned());
    let mut column_status = BTreeMap::new();
    column_status.insert(2, "onlyCmsEvicted".to_owned());
    let with_status = UsedStatsInfoForTable {
        name: "t2".to_owned(),
        version: 10,
        realtime_count: 2000,
        modify_count: 0,
        index_stats_load_status: index_status,
        column_stats_load_status: column_status,
    };
    assert_eq!(
        with_status.write_to_slow_log(),
        "t2:stats_meta_version=10[realtime_count=2000;modify_count=0][ID 1:allLoaded][ID 2:onlyCmsEvicted]"
    );
}

#[test]
fn used_stats_status_ids_are_sorted_without_table_metadata() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:1525-1560. Go explicitly
    // sorts map keys before writing status entries; BTreeMap carries that
    // deterministic order in this dependency-closed value owner.
    let mut index_status = BTreeMap::new();
    index_status.insert(20, "second".to_owned());
    index_status.insert(3, "first".to_owned());
    let stats = UsedStatsInfoForTable {
        name: "t".to_owned(),
        version: 1,
        index_stats_load_status: index_status,
        ..UsedStatsInfoForTable::default()
    };
    assert_eq!(
        stats.write_to_slow_log(),
        "t:stats_meta_version=1[realtime_count=0;modify_count=0][ID 3:first,ID 20:second][]"
    );
}

#[test]
fn used_stats_explain_matches_source_status_order_and_truncation() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:1520-1620.
    let mut columns = BTreeMap::new();
    columns.insert(1, "allEvicted".to_owned());
    columns.insert(2, "onlyCmsEvicted".to_owned());
    columns.insert(3, "onlyHistRemained".to_owned());
    columns.insert(4, "allEvicted".to_owned());
    let mut indexes = BTreeMap::new();
    indexes.insert(7, "allEvicted".to_owned());
    let stats = UsedStatsInfoForTable {
        version: 10,
        column_stats_load_status: columns,
        index_stats_load_status: indexes,
        ..UsedStatsInfoForTable::default()
    };
    let column_names = BTreeMap::from([
        (1, "a".to_owned()),
        (2, "b".to_owned()),
        (3, "c".to_owned()),
        (4, "d".to_owned()),
    ]);
    let index_names = BTreeMap::from([(7, "idx".to_owned())]);
    assert_eq!(
        stats.format_for_explain(&column_names, &index_names),
        "stats:partial[a:allEvicted, b:onlyCmsEvicted, c:onlyHistRemained, ...(more: 2 allEvicted)]"
    );
}

#[test]
fn used_stats_explain_uses_source_pseudo_and_missing_name_rules() {
    let pseudo = UsedStatsInfoForTable::default();
    assert_eq!(
        pseudo.format_for_explain(&BTreeMap::new(), &BTreeMap::new()),
        "stats:pseudo"
    );

    let mut indexes = BTreeMap::new();
    indexes.insert(8, "missing".to_owned());
    let stats = UsedStatsInfoForTable {
        version: 1,
        index_stats_load_status: indexes,
        ..UsedStatsInfoForTable::default()
    };
    assert_eq!(
        stats.format_for_explain(&BTreeMap::new(), &BTreeMap::new()),
        "stats:partial[ID 8:missing]"
    );
}
