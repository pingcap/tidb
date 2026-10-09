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

//! OR (union) IndexMerge partials over the table path: Go
//! `accessPathsForConds` builds an `IsCommonHandlePath` partial for a
//! clustered composite primary key, ranged over the PRIMARY index's columns,
//! and keeps a partial whose range is empty.

use crate::tests_support::row_text;
use crate::Session;

fn plan_tree(session: &mut Session, sql: &str) -> Vec<String> {
    row_text(session.run(&format!("explain format='plan_tree' {sql}")))
        .into_iter()
        .map(|row| format!("{} {} {}", row[0].trim(), row[2], row[3]))
        .collect()
}

/// `planner/core/casetest/index/index.test`'s clustered `t1`: each OR branch
/// on the primary key is a TableRangeScan partial, as TiDB records. Before,
/// only an integer handle could serve a branch, so the union was never built
/// and the whole table was read.
#[test]
fn a_clustered_common_handle_serves_an_or_branch() {
    let mut session = Session::new();
    session.run("set tidb_enable_clustered_index=on").unwrap();
    session
        .run("create table t1 (a int, b varchar(20), c decimal(40,10), d int, primary key(a,b), key(c))")
        .unwrap();
    session
        .run(r#"insert into t1 values (1,"111",1.1,11), (2,"222",2.2,12), (3,"333",3.3,13)"#)
        .unwrap();
    assert_eq!(
        plan_tree(
            &mut session,
            "select /*+ use_index_merge(t1 primary, c) */ * from t1 where t1.a >= 1 or t1.c = 2.2"
        ),
        vec![
            "IndexMerge  type: union",
            "├─TableRangeScan(Build) table:t1 range:[1,+inf], keep order:false, stats:pseudo",
            "├─IndexRangeScan(Build) table:t1, index:c(c) range:[2.2000000000,2.2000000000], keep order:false, stats:pseudo",
            "└─TableRowIDScan(Probe) table:t1 keep order:false, stats:pseudo",
        ]
    );
    assert_eq!(
        plan_tree(
            &mut session,
            "select /*+ use_index_merge(t1 primary, c) */ * from t1 where t1.a = 1 and t1.b = '111' or t1.c = 3.3"
        )[1],
        r#"├─TableRangeScan(Build) table:t1 range:[1 "111",1 "111"], keep order:false, stats:pseudo"#
    );
    assert_eq!(
        row_text(session.run(
            "select /*+ use_index_merge(t1 primary, c) */ a from t1 \
             where t1.a = 1 and t1.b = '111' or t1.c = 3.3 order by a"
        )),
        vec![vec!["1"], vec!["3"]]
    );
}

/// `planner/core/indexmerge_path.test`'s TestIssue52395, as TiDB records it.
/// The empty `col_38` range is still a partial (Go drops only a full range),
/// named TableFullScan as Go's `IsFullScan` names a scan with no ranges; it
/// had disqualified the union. It reads no keys, as Go's partial table
/// worker does (read as a full scan it returned both rows). The Projection's 7.32 rows are Go's
/// `Selectivity` with the MV index estimating `json_contains` (10 of 10000
/// rows, `getMaskAndSelectivityForMVIndex`) and the empty range clamped to
/// one row; `idx_17` leads with its hidden column, which the statistics
/// collection must resolve against every table column, not the pruned
/// schema. Before, the OR fell to the 0.8 default: 5325.47 rows.
#[test]
fn an_empty_common_handle_range_stays_an_or_partial() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t (col_37 json DEFAULT NULL, \
             col_38 timestamp NOT NULL DEFAULT '2010-07-09 00:00:00', \
             UNIQUE KEY idx_14 (col_38,(cast(col_37 as unsigned array))), \
             PRIMARY KEY (col_38) /*T![clustered_index] CLUSTERED */, \
             UNIQUE KEY idx_17 ((cast(col_37 as unsigned array)),col_38))",
        )
        .unwrap();
    session
        .run("INSERT INTO t VALUES ('[6085355592952464235]','1971-01-16 00:00:00'), (NULL,'1980-06-27 00:00:00')")
        .unwrap();
    let sql = "SELECT /*+ use_index_merge(t)*/ MIN(col_37) FROM t \
               WHERE col_38 BETWEEN '1984-12-13' AND '1975-01-28' \
               OR JSON_CONTAINS(col_37, '138480458355390957') \
               GROUP BY col_38 HAVING col_38 != '1988-03-22'";
    let rows = row_text(session.run(&format!("explain format='brief' {sql}")))
        .into_iter()
        .map(|row| format!("{} {} {}", row[0].trim(), row[1], row[3]))
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        vec![
            "Projection 7.32 ",
            "└─IndexMerge 6.66 ",
            "├─TableFullScan(Build) 0.00 table:t",
            "├─IndexRangeScan(Build) 10.00 table:t, index:idx_17(cast(`col_37` as unsigned array), col_38)",
            "└─Selection(Probe) 6.66 ",
            "└─TableRowIDScan 10.00 table:t",
        ]
    );
    assert!(row_text(session.run(sql)).is_empty());
}
