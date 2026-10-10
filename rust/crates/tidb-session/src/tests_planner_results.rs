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

//! Query answers `executor/executor.test` and `planner/core/integration.test`
//! record, each of which an operator-level port detail had made wrong.

use crate::tests_support::row_text;
use crate::Session;

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    row_text(session.run(sql))
}

/// Go `LogicalApply.PruneColumns` ends at `MergeSchema`: the Apply keeps
/// every column of both children, which its executor emits. Narrowing its
/// schema like a join's (`InlineProjection`) shifted every column above it,
/// so a correlated subquery answered with the outer row's own value
/// (TestIssue19372, TestCorrelatedAggregate).
#[test]
fn a_correlated_subquery_reads_its_own_column_above_the_apply() {
    let mut session = Session::new();
    session
        .run("create table t1 (c_int int, c_str varchar(40), key(c_str))")
        .unwrap();
    session.run("create table t2 like t1").unwrap();
    session
        .run("insert into t1 values (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    session.run("insert into t2 select * from t1").unwrap();
    assert_eq!(
        rows(
            &mut session,
            "select (select t2.c_str from t2 where t2.c_str <= t1.c_str and t2.c_int in (1, 2) order by t2.c_str limit 1) x from t1 order by c_int"
        ),
        vec![vec!["a"], vec!["a"], vec!["a"]]
    );
    assert_eq!(
        rows(
            &mut session,
            "select (select count(*) from t2 where t2.c_str <= t1.c_str) x from t1 order by c_str"
        ),
        vec![vec!["1"], vec!["2"], vec!["3"]]
    );

    session.run("create table tab(i int)").unwrap();
    session.run("create table tab2(j int)").unwrap();
    session.run("insert into tab values (1), (2), (3)").unwrap();
    session
        .run("insert into tab2 values (1), (2), (3), (15)")
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "select m.i, (select count(n.j) from tab2 where j = 15) as o from tab m, tab2 n group by 1 order by m.i"
        ),
        vec![vec!["1", "4"], vec!["2", "4"], vec!["3", "4"]]
    );
    assert_eq!(
        rows(
            &mut session,
            "select (select count(n.j) from tab2 where j = 15) as o from tab m, tab2 n order by m.i"
        ),
        vec![vec!["12"]]
    );
}

/// Go `countOriginalWithDistinct` keys a row by `evalAndEncode`, whose JSON
/// arm is `BinaryJSON.HashValue`: `2010` and `2010.000` are one value
/// (TestCountDistinctJSON).
#[test]
fn count_distinct_json_folds_equal_numbers() {
    let mut session = Session::new();
    session.run("create table t(j json)").unwrap();
    session
        .run("insert into t values ('2010'), ('2011'), ('2012'), ('2010.000'), (cast(18446744073709551615 as json)), (cast(18446744073709551616.000000 as json))")
        .unwrap();
    assert_eq!(
        rows(&mut session, "select count(distinct j) from t"),
        vec![vec!["5"]]
    );
}

/// Go `refineValueAndOp` (and the IN builder) turn a binary literal into a
/// string under the column's collation before building the range, so a
/// literal with a leading zero byte keeps it (TestIssue23846).
#[test]
fn a_hex_literal_with_a_leading_zero_byte_finds_its_row_by_index() {
    let mut session = Session::new();
    session
        .run("create table t(a varbinary(10), unique key(a))")
        .unwrap();
    session
        .run("insert into t values (0x00A4EEF4FA55D6706ED5)")
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "select count(*) from t where a = 0x00A4EEF4FA55D6706ED5"
        ),
        vec![vec!["1"]]
    );
    session
        .run("create table t2(a varbinary(10), key(a))")
        .unwrap();
    session
        .run("insert into t2 values (0x00A4), (0x41)")
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "select count(*) from t2 where a in (0x00A4, 0x0041)"
        ),
        vec![vec!["1"]]
    );
}

/// A Partial2 COUNT merges its children's partial counts as the Final one
/// does, so a COUNT pushed below a static-mode partition union sums
/// (TestFloorUnixTimestampPruning).
#[test]
fn count_over_a_static_partition_union_sums_the_partitions() {
    let mut session = Session::new();
    session
        .run("set @@tidb_partition_prune_mode = 'static'")
        .unwrap();
    session
        .run("create table tp (a int, b int) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20), partition p2 values less than (30))")
        .unwrap();
    session
        .run("insert into tp values (1, 1), (2, 2), (11, 3), (12, 4), (21, 5), (22, null)")
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "select count(*), count(b), sum(b), avg(b) from tp"
        ),
        vec![vec!["6", "5", "15", "3.0000"]]
    );
}
