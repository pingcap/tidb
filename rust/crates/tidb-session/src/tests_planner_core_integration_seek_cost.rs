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

//! Go master `pkg/planner/core/integration_test.go`'s
//! `TestIndexScanSeekCost` (1c0793ea576): a multi-range scan pays a seek per
//! range, and an IN-list prefix may be pruned into a filter.

use crate::testkit::TestKit;

fn rows_of(tk: &mut TestKit, sql: &str) -> Vec<String> {
    tk.must_query(sql).rows().iter().map(|row| row.join(" ")).collect()
}

/// Go `statstestutil.HandleNextDDLEventWithTxn(h)` after each CREATE (test
/// builds do not deliver DDL events to the stats handle).
fn create(tk: &mut TestKit, sql: &str) {
    tk.must_exec(sql);
    tk.session().handle_ddl_events_for_test();
}

/// Go `h.DumpStatsDeltaToKV(true)`, `analyze table`, then `h.Update`: a late
/// delta flush would raise the modify count and fall back to pseudo stats,
/// which disables pruning.
fn analyze(tk: &mut TestKit, table: &str) {
    tk.must_exec(&format!("flush stats_delta test.{table}"));
    tk.must_exec(&format!("analyze table {table}"));
    tk.session().update_stats_cache().unwrap();
}

fn sorted_rows_of(tk: &mut TestKit, sql: &str) -> Vec<String> {
    let mut rows = rows_of(tk, sql);
    rows.sort();
    rows
}

/// Go `TestIndexScanSeekCost`.
#[test]
fn test_index_scan_seek_cost() {
    let mut tk = TestKit::new();
    // Pruning needs fully loaded index stats.
    tk.must_exec("set @@tidb_stats_load_sync_wait = 60000");
    create(&mut tk, "create table t (a int, b int, c int, d int, key idx_abcd(a, b, c, d))");
    // Each value of `a` has only 25 rows, so most (a, b, c) point ranges are
    // nearly empty and a seek per range costs more than scanning and
    // filtering every row under `a`.
    tk.must_exec("set @@cte_max_recursion_depth = 100000");
    tk.must_exec(
        "insert into t with recursive cte(n) as (select 1 union all select n + 1 from cte where n < 50000)
		select n % 2000, n % 7 + 1, n % 11 + 1, n from cte",
    );
    analyze(&mut tk, "t");
    tk.must_exec("set @@tidb_cost_model_version = 2");
    // The IN-lists cover only part of the values of `b` and `c`, so the
    // residual filters on a pruned scan must reject rows.
    let r#where = "a = 1 and b in (1,2,3,4,5,6) and c in (1,2,3,4,5,6,7,8,9,10)";

    // Without an ordering requirement the seek cost of 60 (a, b, c) ranges
    // outweighs scanning the extra rows, so `c` is pruned from the ranges and
    // applied as a filter, leaving 6 (a, b) ranges.
    tk.must_query(&format!("explain format='brief' select a, b, c, d from t where {where}")).check([
        "IndexReader 278.05 root  index:Selection",
        "└─Selection 278.05 cop[tikv]  in(test.t.c, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)",
        "  └─IndexRangeScan 278.05 cop[tikv] table:t, index:idx_abcd(a, b, c, d) range:[1 1,1 1], [1 2,1 2], [1 3,1 3], [1 4,1 4], [1 5,1 5], [1 6,1 6], keep order:false",
    ]);

    // With ORDER BY d the full point ranges satisfy the order through a
    // per-range merge sort, so pruning must not be chosen: it would force a
    // Sort on top of the pruned scan.
    let full_ranges: Vec<String> = (1..=6)
        .flat_map(|b| (1..=10).map(move |c| format!("[1 {b} {c},1 {b} {c}]")))
        .collect();
    tk.must_query(&format!("explain format='brief' select a, b, c, d from t where {where} order by d limit 5")).check([
        "Limit 5.00 root  offset:0, count:5".to_owned(),
        "└─IndexReader 5.00 root  index:Limit".to_owned(),
        "  └─Limit 5.00 cop[tikv]  offset:0, count:5".to_owned(),
        format!(
            "    └─IndexRangeScan 5.00 cop[tikv] table:t, index:idx_abcd(a, b, c, d) range:{}, keep order:true",
            full_ranges.join(", ")
        ),
    ]);

    // Compare against a full table scan, which applies every predicate as a
    // filter.
    assert_eq!(
        sorted_rows_of(&mut tk, &format!("select a, b, c, d from t where {where}")),
        sorted_rows_of(&mut tk, &format!("select a, b, c, d from t ignore index(idx_abcd) where {where}")),
    );
    // The ordered result, unsorted, so a misordered keep-order scan is caught.
    assert_eq!(
        rows_of(&mut tk, &format!("select a, b, c, d from t where {where} order by d limit 5")),
        rows_of(&mut tk, &format!("select a, b, c, d from t ignore index(idx_abcd) where {where} order by d limit 5")),
    );

    // Many IN-lists ahead of a selective equality: 12*4*3*9 = 1296 ranges for
    // a lookup that returns nothing. Each shorter prefix scans only a few rows
    // per seek it saves, so pruning is allowed.
    create(&mut tk, "create table t2 (a int, b int, c int, d int, e int, f int, key idx(a, b, c, d, e, f))");
    tk.must_exec(
        "insert into t2 with recursive cte(n) as (select 1 union all select n + 1 from cte where n < 100000)
		select n % 1000, n % 5, n % 4, n % 10, n, n from cte",
    );
    // The leading column has only 2 values, so a shorter prefix would scan
    // far more rows per saved seek than the guard allows, and the full ranges
    // must be kept.
    create(&mut tk, "create table t3 (a int, b int, c int, d int, e int, f int, key idx(a, b, c, d, e, f))");
    tk.must_exec(
        "insert into t3 with recursive cte(n) as (select 1 union all select n + 1 from cte where n < 100000)
		select n % 2, n % 10, n % 1000, n, n, n from cte",
    );
    analyze(&mut tk, "t2");
    analyze(&mut tk, "t3");

    let where2 = "a in (1,2,3,4,5,6,7,8,9,10,11,12) and b in (1,2,3,4) and c in (1,2,3) and d in (1,2,3,4,5,6,7,8,9) and e = -1";
    tk.must_query(&format!("explain format='brief' select * from t2 where {where2}")).check([
        "IndexReader 1296.00 root  index:Selection",
        "└─Selection 1296.00 cop[tikv]  eq(test.t2.e, -1), in(test.t2.b, 1, 2, 3, 4), in(test.t2.c, 1, 2, 3), in(test.t2.d, 1, 2, 3, 4, 5, 6, 7, 8, 9)",
        "  └─IndexRangeScan 1296.00 cop[tikv] table:t2, index:idx(a, b, c, d, e, f) range:[1,1], [2,2], [3,3], [4,4], [5,5], [6,6], [7,7], [8,8], [9,9], [10,10], [11,11], [12,12], keep order:false",
    ]);
    tk.must_query(&format!("select * from t2 where {where2}")).check_empty();

    let where3 = "a = 1 and b in (1,2,3,4,5,6,7,8,9) and c in (1,3,5,7,9,11,13,15,17,19,21)";
    let t3_ranges: Vec<String> = (1..=9)
        .flat_map(|b| (1..=21).step_by(2).map(move |c| format!("[1 {b} {c},1 {b} {c}]")))
        .collect();
    // Only the operators and ranges are checked: the row estimate for t3
    // depends on analyze sampling.
    tk.must_query(&format!("explain format='brief' select * from t3 where {where3}")).check_at(
        &[0, 4],
        [
            "IndexReader index:IndexRangeScan".to_owned(),
            format!("└─IndexRangeScan range:{}, keep order:false", t3_ranges.join(", ")),
        ],
    );
    assert_eq!(
        sorted_rows_of(&mut tk, &format!("select * from t3 where {where3}")),
        sorted_rows_of(&mut tk, &format!("select * from t3 ignore index(idx) where {where3}")),
    );

    // For a non-unique index on a clustered composite handle, IdxCols has the
    // handle columns appended, so a cutoff can extend past the index
    // definition. Row estimation must project those ranges onto the index
    // columns. The 1200 (a,b,c,d) ranges prune to 6 (a,b) ranges.
    create(&mut tk, "create table t4 (a int, b int, c int, d int, e int, primary key(c, d) clustered, key idx(a, b))");
    tk.must_exec(
        "insert into t4 with recursive cte(n) as (select 1 union all select n + 1 from cte where n < 20000)
		select n % 1000, n % 7, n % 13, n, n from cte",
    );
    analyze(&mut tk, "t4");
    let where4 = "a = 1 and b in (1,2,3,4,5,6) and c in (1,2,3,4,5,6,7,8,9,10) and d in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)";
    tk.must_query(&format!("explain format='brief' select * from t4 use index(idx) where {where4}")).check_at(
        &[0, 4],
        [
            "IndexLookUp ",
            "├─Selection(Build) in(test.t4.c, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10), in(test.t4.d, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)",
            "│ └─IndexRangeScan range:[1 1,1 1], [1 2,1 2], [1 3,1 3], [1 4,1 4], [1 5,1 5], [1 6,1 6], keep order:false",
            "└─TableRowIDScan(Probe) keep order:false",
        ],
    );
    assert_eq!(
        sorted_rows_of(&mut tk, &format!("select * from t4 use index(idx) where {where4}")),
        sorted_rows_of(&mut tk, &format!("select * from t4 ignore index(idx) where {where4}")),
    );

    // An IndexJoin inner scan's seeks are charged once per build row by the
    // join (`indexJoinSeekingCostVer2`), so the scan itself must not charge
    // them again.
    let ij_rows = tk.must_query(
        "explain format='cost_trace' select /*+ inl_join(t3) */ * from t2 join t3 on t2.a = t3.a where t3.b in (1,2,3)",
    );
    let inner_scans: Vec<&Vec<String>> =
        ij_rows.rows().iter().filter(|row| row[0].contains("IndexRangeScan")).collect();
    assert!(!inner_scans.is_empty());
    for row in inner_scans {
        assert!(!row[3].contains("rangeSeek"), "{row:?}");
    }
    let single = tk.must_query("explain format='cost_trace' select * from t3 where a = 1 and b in (1,2,3)");
    assert!(single.rows()[1][3].contains("rangeSeek"), "{:?}", single.rows());
    // Multi-range table scans are charged the same way, so they don't look
    // cheaper than an equivalent multi-range index scan.
    let ts_rows = tk.must_query("explain format='cost_trace' select * from t4 where c in (1,2,3) and d > 5");
    let last = ts_rows.rows().last().expect("a plan");
    assert!(last[0].contains("TableRangeScan"), "{last:?}");
    assert!(last[3].contains("rangeSeek"), "{last:?}");
}
