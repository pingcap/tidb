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

//! Go master `pkg/planner/cardinality/selectivity_test.go`'s
//! `TestIndexRangeEstimationWithTruncatedHandleRange` (94b834d94b6,
//! 12d24af9894): a non-unique index path whose ranges reach into the
//! appended handle columns estimates over ranges pruned to the declared
//! columns, with the handle predicates credited.

use crate::testkit::TestKit;

/// The estRows and operator info of the plan's IndexRangeScan.
fn index_scan_row(tk: &mut TestKit, sql: &str) -> (String, String) {
    let rows = tk.must_query(&format!("explain format='brief' {sql}"));
    rows.rows()
        .iter()
        .find(|row| row[0].contains("IndexRangeScan"))
        .map(|row| (row[1].clone(), row[4].clone()))
        .unwrap_or_else(|| panic!("no IndexRangeScan in plan for {sql:?}"))
}

/// Go `TestIndexRangeEstimationWithTruncatedHandleRange`.
#[test]
fn test_index_range_estimation_with_truncated_handle_range() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t(id bigint primary key clustered, a int, key ia(a))");
    tk.session().handle_ddl_events_for_test();
    let vals = (1..=100).map(|i| format!("({i}, {})", i % 10)).collect::<Vec<_>>().join(",");
    tk.must_exec(&format!("insert into t values {vals}"));
    tk.must_exec("analyze table t all columns");

    // Each distinct value of a has 10 rows. The estimate starts from the 10
    // rows of the a = 5 prefix, is damped by the handle column's selectivity
    // with exponential backoff, and is then aligned upward to stats.RowCount
    // without the SelectionFactor penalty.

    // Handle range with an exclusive low bound: the pruned range must not
    // collapse to the empty (5, 5].
    let (est_rows, op_info) = index_scan_row(&mut tk, "select * from t use index(ia) where a = 5 and id > 10");
    assert!(op_info.contains("range:(5 10,5 +inf]"), "execution range must keep the handle dimension: {op_info}");
    assert_eq!(est_rows, "10.00");

    // Handle range with an exclusive high bound: same shape as above.
    let (est_rows, op_info) = index_scan_row(&mut tk, "select * from t use index(ia) where a = 5 and id < 10");
    assert!(op_info.contains("range:[5 -inf,5 10)"), "execution range must keep the handle dimension: {op_info}");
    assert_eq!(est_rows, "10.00");

    // Point-bound handle predicates get real credit: 10 * sqrt(sel) = 1.41
    // aligns to the Selectivity() result of 2.00.
    let (est_rows, op_info) = index_scan_row(&mut tk, "select * from t use index(ia) where a = 5 and id in (11, 22)");
    assert!(op_info.contains("range:[5 11,5 11], [5 22,5 22]"), "execution range must keep the handle dimension: {op_info}");
    assert_eq!(est_rows, "2.00");

    // A point over the index column plus the full handle matches at most one
    // row, because the physical key of a non-unique index ends with the
    // complete handle.
    let (est_rows, op_info) = index_scan_row(&mut tk, "select * from t use index(ia) where a = 5 and id = 7");
    assert!(op_info.contains("range:[5 7,5 7]"), "execution range must keep the handle dimension: {op_info}");
    assert_eq!(est_rows, "1.00");

    // An unsigned int handle is never appended to the index columns, so its
    // predicates stay out of the index ranges and get no credit.
    tk.must_exec("create table tu(id bigint unsigned primary key clustered, a int, key ia(a))");
    tk.session().handle_ddl_events_for_test();
    tk.must_exec(&format!("insert into tu values {vals}"));
    tk.must_exec("analyze table tu all columns");
    let (est_rows, op_info) = index_scan_row(&mut tk, "select * from tu use index(ia) where a = 5 and id in (11, 22)");
    assert!(op_info.contains("range:[5,5]"), "unsigned handle must not extend the execution range: {op_info}");
    assert!(!op_info.contains("5 11"), "unsigned handle must not extend the execution range: {op_info}");
    assert_eq!(est_rows, "10.00");
    let (est_rows, op_info) = index_scan_row(&mut tk, "select * from tu use index(ia) where a = 5 and id > 10");
    assert!(op_info.contains("range:[5,5]"), "unsigned handle must not extend the execution range: {op_info}");
    assert_eq!(est_rows, "10.00");
}

/// Index-merge partial paths take the same estimate without
/// `adjustCountAfterAccess`, so its handle exception does not hide the
/// pruned range's bounds or the point cap. Expected rows are Go master's
/// (a testkit probe at 2b6fb9edcb0's Go master): a point over the index
/// column plus the full handle is capped at one row per point, and a pruned
/// exclusive handle bound keeps its prefix rows.
#[test]
fn index_merge_partials_cap_full_handle_points_and_keep_pruned_bounds() {
    let mut tk = TestKit::new();
    tk.must_exec("create table m2(id bigint primary key clustered, a int, b int, key ia(a), key ib(b))");
    tk.session().handle_ddl_events_for_test();
    let vals = (1..=100).map(|i| format!("({i},{},{})", i % 2, i % 2)).collect::<Vec<_>>().join(",");
    tk.must_exec(&format!("insert into m2 values {vals}"));
    tk.must_exec("analyze table m2 all columns");
    for (filter, expected) in [
        ("(a=1 and id = 15) or (b=0 and id = 16)", ["1.99", "1.99", "1.00", "1.00", "1.99"]),
        ("(a=1 and id > 10) or (b=0 and id < 5)", ["52.50", "52.50", "47.70", "11.18", "52.50"]),
        ("(a=1 and id in (15, 25)) or (b=0 and id in (16, 26))", ["3.96", "3.96", "2.00", "2.00", "3.96"]),
    ] {
        let sql = format!(
            "explain format='brief' select /*+ use_index_merge(m2,ia,ib) */ id from m2 use index(ia,ib) where {filter}"
        );
        let rows = tk.must_query(&sql);
        let est: Vec<&str> = rows.rows().iter().map(|row| row[1].as_str()).collect();
        assert_eq!(est, expected, "{sql}: {:?}", rows.rows());
    }
}

