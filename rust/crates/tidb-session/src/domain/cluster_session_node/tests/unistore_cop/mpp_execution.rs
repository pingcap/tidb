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

//! Go `pkg/executor/test/tiflashtest/tiflash_test.go` on the embedded store's
//! mock TiFlash stores: `MPPGather` dispatches every fragment to the stores,
//! whose tasks exchange rows and answer TiDB.

use super::*;

/// Go `UpdateTableReplicaInfo(sctx, tableID, true)` for `table`.
fn replica_available(stack: &UnistoreClusterStack, session: &mut Session, table: &str) {
    let table_id: i64 = displayed(rows(
        session,
        &format!("select tidb_table_id from information_schema.tables where table_schema = database() and table_name = '{table}'"),
    ))[0][0]
        .parse()
        .unwrap();
    stack
        .factory
        .ddl
        .execute(
            &tidb_ddl::cluster_ddl::DdlStatement::UpdateTiFlashReplicaStatus {
                table_id,
                available: true,
            },
            &tidb_sessionctx::StmtContext::for_query(),
        )
        .expect("the replica turns available");
}

/// Go `tk.MustQuery(sql).Check(testkit.Rows(...))`; this harness renders
/// NULL as `NULL` where Go's testkit prints `<nil>`.
fn check(session: &mut Session, sql: &str, expected: &[&[&str]]) {
    let got = displayed(rows(session, sql));
    let expected: Vec<Vec<String>> = expected
        .iter()
        .map(|row| row.iter().map(|value| (*value).to_owned()).collect())
        .collect();
    assert_eq!(got, expected, "{sql}");
}

/// Go `TestMppExecution`'s joins, aggregates and shuffles.
#[test]
fn test_mpp_execution() {
    let stack = super::tiflash_partition::tiflash_stack(2);
    let mut session = stack.factory.open_session(session_context(2151)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "use test");
    rows(&mut session, "drop table if exists t");
    rows(&mut session, "create table t(a int not null primary key, b int not null)");
    rows(&mut session, "alter table t set tiflash replica 2");
    replica_available(&stack, &mut session, "t");
    rows(&mut session, "insert into t values(1,0)");
    rows(&mut session, "insert into t values(2,0)");
    rows(&mut session, "insert into t values(3,0)");

    rows(&mut session, "create table t1(a int primary key, b int not null)");
    rows(&mut session, "alter table t1 set tiflash replica 2");
    replica_available(&stack, &mut session, "t1");
    rows(&mut session, "insert into t1 values(1,0)");
    rows(&mut session, "insert into t1 values(2,0)");
    rows(&mut session, "insert into t1 values(3,0)");

    rows(&mut session, "set @@session.tidb_isolation_read_engines=\"tiflash\"");
    rows(&mut session, "set @@session.tidb_allow_mpp=ON");
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=OFF");
    // mock executor does not support use outer table as build side for outer
    // join, so need to force the inner table as build side
    rows(&mut session, "set tidb_opt_mpp_outer_join_fixed_build_side=1");
    for _ in 0..20 {
        // test if it is stable.
        check(&mut session, "select count(*) from t1 , t where t1.a = t.a", &[&["3"]]);
    }
    // test multi-way join
    rows(&mut session, "create table t2(a int primary key, b int not null)");
    rows(&mut session, "alter table t2 set tiflash replica 1");
    replica_available(&stack, &mut session, "t2");

    rows(&mut session, "insert into t2 values(1,0)");
    rows(&mut session, "insert into t2 values(2,0)");
    rows(&mut session, "insert into t2 values(3,0)");
    check(&mut session, "select count(*) from t1 , t, t2 where t1.a = t.a and t2.a = t.a", &[&["3"]]);

    // test avg
    check(&mut session, "select avg(t1.a) from t1 , t where t1.a = t.a", &[&["2.0000"]]);
    // test proj and selection
    check(
        &mut session,
        "select count(*) from (select a * 2 as a from t1) t1 , (select b + 4 as a from t)t where t1.a = t.a",
        &[&["3"]],
    );

    // test shuffle hash join.
    rows(&mut session, "set @@session.tidb_broadcast_join_threshold_size=1");
    check(&mut session, "select count(*) from t1 , t where t1.a = t.a", &[&["3"]]);
    check(&mut session, "select count(*) from t1 , t, t2 where t1.a = t.a and t2.a = t.a", &[&["3"]]);

    // test agg by expression
    rows(&mut session, "insert into t1 values(4,0)");
    check(
        &mut session,
        "select count(*) k, t2.b from t1 left join t2 on t1.a = t2.a group by t2.b order by k",
        &[&["1", "NULL"], &["3", "0"]],
    );
    check(
        &mut session,
        "select count(*) k, t2.b+1 from t1 left join t2 on t1.a = t2.a group by t2.b+1 order by k",
        &[&["1", "NULL"], &["3", "1"]],
    );
    check(&mut session, "select count(*) k, t2.b * t2.a from t2 group by t2.b * t2.a", &[&["3", "0"]]);
    check(
        &mut session,
        "select count(*) k, t2.a/2 m from t2 group by t2.a / 2 order by m",
        &[&["1", "0.5000"], &["1", "1.0000"], &["1", "1.5000"]],
    );
    check(
        &mut session,
        "select count(*) k, t2.a div 2 from t2 group by t2.a div 2 order by k",
        &[&["1", "0"], &["2", "1"]],
    );
    // test task id for same start ts. (Go then checks
    // `physicalop.AllocMPPTaskID(tk.Session())`, a session-internal counter
    // this harness does not reach.)
    rows(&mut session, "begin");
    check(&mut session, "select count(*) from ( select * from t2 group by a, b) A group by A.b", &[&["3"]]);
    check(
        &mut session,
        "select count(*) from t1 where t1.a+100 > ( select count(*) from t2 where t1.a=t2.a and t1.b=t2.b) group by t1.b",
        &[&["4"]],
    );
    rows(&mut session, "commit");

    check(&mut session, "select avg(t.a) from t join t t1 on t.a = t1.a", &[&["2.0000"]]);

    rows(&mut session, "drop table if exists t");
    rows(
        &mut session,
        "create table t (c1 decimal(8, 5) not null, c2 decimal(9, 5), c3 decimal(9, 4) , c4 decimal(8, 4) not null)",
    );
    rows(&mut session, "alter table t set tiflash replica 1");
    replica_available(&stack, &mut session, "t");
    rows(&mut session, "insert into t values(1.00000,1.00000,1.0000,1.0000)");
    rows(&mut session, "insert into t values(1.00010,1.00010,1.0001,1.0001)");
    rows(&mut session, "insert into t values(1.00001,1.00001,1.0000,1.0002)");
    check(
        &mut session,
        "select t1.c1 from t t1 join t t2 on t1.c1 = t2.c1 order by t1.c1",
        &[&["1.00000"], &["1.00001"], &["1.00010"]],
    );
    check(
        &mut session,
        "select t1.c1 from t t1 join t t2 on t1.c1 = t2.c3 order by t1.c1",
        &[&["1.00000"], &["1.00000"], &["1.00010"]],
    );
    check(
        &mut session,
        "select t1.c4 from t t1 join t t2 on t1.c4 = t2.c3 order by t1.c4",
        &[&["1.0000"], &["1.0000"], &["1.0001"]],
    );
    // let this query choose hash join
    check(
        &mut session,
        "select /*+ nth_plan(2) */ t1.c1 from t t1 join t t2 on t1.c1 = t2.c3 order by t1.c1",
        &[&["1.00000"], &["1.00000"], &["1.00010"]],
    );
}

/// A session over `nodes` mock TiFlash stores in the `test` database.
fn tiflash_session(nodes: usize, connection_id: u64) -> (UnistoreClusterStack, Session) {
    let stack = super::tiflash_partition::tiflash_stack(nodes);
    let mut session = stack.factory.open_session(session_context(connection_id)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "use test");
    (stack, session)
}

/// Go `TestReadUnsigedPK`.
#[test]
fn test_read_unsiged_pk() {
    let (stack, mut session) = tiflash_session(2, 2152);
    rows(&mut session, "drop table if exists t");
    rows(&mut session, "drop table if exists t1");
    rows(&mut session, "create table t(a bigint unsigned not null primary key, b int not null)");
    rows(&mut session, "alter table t set tiflash replica 1");
    replica_available(&stack, &mut session, "t");
    rows(&mut session, "insert into t values(1,0)");
    rows(&mut session, "insert into t values(2,0)");
    rows(&mut session, "insert into t values(3,0)");
    rows(&mut session, "insert into t values(18446744073709551606,0)");
    rows(&mut session, "insert into t values(9223372036854775798,0)");

    rows(&mut session, "create table t1(a bigint unsigned not null primary key, b int not null)");
    rows(&mut session, "alter table t1 set tiflash replica 1");
    replica_available(&stack, &mut session, "t1");
    rows(&mut session, "insert into t1 values(1,0)");
    rows(&mut session, "insert into t1 values(2,0)");
    rows(&mut session, "insert into t1 values(3,0)");
    rows(&mut session, "insert into t1 values(18446744073709551606,0)");
    rows(&mut session, "insert into t1 values(9223372036854775798,0)");

    rows(&mut session, "set @@session.tidb_isolation_read_engines=\"tiflash\"");
    rows(&mut session, "set @@session.tidb_allow_mpp=ON");
    // unistore does not support later materialization
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");
    // mock executor does not support use outer table as build side for outer
    // join, so need to force the inner table as build side
    rows(&mut session, "set tidb_opt_mpp_outer_join_fixed_build_side=1");

    check(&mut session, "select count(*) from t1 , t where t1.a = t.a", &[&["5"]]);
    check(
        &mut session,
        "select count(*) from t1 , t where t1.a = t.a and ((t1.a < 9223372036854775800 and t1.a > 2) or (t1.a <= 1 and t1.a > -1))",
        &[&["3"]],
    );
}

/// Go `TestJoinRace`.
#[test]
fn test_join_race() {
    let (stack, mut session) = tiflash_session(2, 2153);
    rows(&mut session, "drop table if exists t");
    rows(&mut session, "create table t(a int not null, b int not null)");
    rows(&mut session, "alter table t set tiflash replica 1");
    replica_available(&stack, &mut session, "t");
    for values in ["(1,1)", "(2,1)", "(3,1)", "(1,2)", "(2,2)", "(3,2)", "(1,2)", "(2,2)", "(3,2)", "(1,3)", "(2,3)", "(3,4)"] {
        rows(&mut session, &format!("insert into t values{values}"));
    }
    rows(&mut session, "set @@session.tidb_isolation_read_engines=\"tiflash\"");
    rows(&mut session, "set @@session.tidb_enforce_mpp=ON");
    rows(&mut session, "set @@tidb_opt_broadcast_cartesian_join=0");
    // unistore does not support later materialization
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");
    check(
        &mut session,
        "select count(*) from (select count(a) x from t group by b) t1 join (select count(a) x from t group by b) t2 on t1.x > t2.x",
        &[&["6"]],
    );
}

/// Go `TestInjectExtraProj`.
#[test]
fn test_inject_extra_proj() {
    let (stack, mut session) = tiflash_session(2, 2154);
    rows(&mut session, "drop table if exists t");
    rows(&mut session, "create table t(a bigint(20))");
    rows(&mut session, "alter table t set tiflash replica 1");
    replica_available(&stack, &mut session, "t");
    for _ in 0..6 {
        rows(&mut session, "insert into t values (9223372036854775807)");
    }
    check(&mut session, "select avg(a) from t", &[&["9223372036854775807.0000"]]);
    check(
        &mut session,
        "select avg(a), a from t group by a",
        &[&["9223372036854775807.0000", "9223372036854775807"]],
    );
}

/// Go `TestMppEnum`.
#[test]
fn test_mpp_enum() {
    let (stack, mut session) = tiflash_session(2, 2155);
    rows(&mut session, "drop table if exists t");
    rows(&mut session, "create table t(a int not null primary key, b enum('aca','bca','zca'))");
    rows(&mut session, "alter table t set tiflash replica 1");
    replica_available(&stack, &mut session, "t");
    rows(&mut session, "insert into t values(1,'aca')");
    rows(&mut session, "insert into t values(2,'bca')");
    rows(&mut session, "insert into t values(3,'zca')");
    rows(&mut session, "set @@session.tidb_isolation_read_engines=\"tiflash\"");
    rows(&mut session, "set @@session.tidb_allow_mpp=ON");
    // unistore does not support later materialization
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");
    // mock executor does not support use outer table as build side for outer
    // join, so need to force the inner table as build side
    rows(&mut session, "set tidb_opt_mpp_outer_join_fixed_build_side=1");
    check(
        &mut session,
        "select t1.b from t t1 join t t2 on t1.a = t2.a order by t1.b",
        &[&["aca"], &["bca"], &["zca"]],
    );
}

/// Go `TestUnionWithEmptyDualTable`.
#[test]
fn test_union_with_empty_dual_table() {
    let (stack, mut session) = tiflash_session(2, 2156);
    rows(&mut session, "drop table if exists t");
    rows(&mut session, "drop table if exists t1");
    rows(&mut session, "create table t (a int not null, b int, c varchar(20))");
    rows(&mut session, "create table t1 (a int, b int not null, c double)");
    rows(&mut session, "alter table t set tiflash replica 1");
    rows(&mut session, "alter table t1 set tiflash replica 1");
    replica_available(&stack, &mut session, "t");
    replica_available(&stack, &mut session, "t1");
    rows(&mut session, "insert into t values(1,2,3)");
    rows(&mut session, "insert into t1 values(1,2,3)");
    rows(&mut session, "set @@session.tidb_isolation_read_engines=\"tiflash\"");
    // unistore does not support later materialization
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");
    rows(&mut session, "set @@session.tidb_enforce_mpp=ON");
    check(
        &mut session,
        "select count(*) from (select a , b from t union all select a , c from t1 where false) tt",
        &[&["1"]],
    );
}

/// Go `TestMppUnionAll`.
#[test]
fn test_mpp_union_all() {
    let (stack, mut session) = tiflash_session(2, 2157);
    rows(&mut session, "drop table if exists x1");
    rows(&mut session, "create table x1(a int , b int);");
    rows(&mut session, "alter table x1 set tiflash replica 2");
    rows(&mut session, "drop table if exists x2");
    rows(&mut session, "create table x2(a int , b int);");
    rows(&mut session, "alter table x2 set tiflash replica 2");
    replica_available(&stack, &mut session, "x1");
    replica_available(&stack, &mut session, "x2");

    rows(&mut session, "insert into x1 values (1, 1), (2, 2), (3, 3), (4, 4)");
    rows(&mut session, "insert into x2 values (5, 1), (2, 2), (3, 3), (4, 4)");
    // mock executor does not support use outer table as build side for outer
    // join, so need to force the inner table as build side
    rows(&mut session, "set tidb_opt_mpp_outer_join_fixed_build_side=1");

    // test join + union (join + select)
    check(
        &mut session,
        "select x1.a, x.a from x1 left join (select x2.b a, x1.b from x1 join x2 on x1.a = x2.b union all select * from x1 ) x on x1.a = x.a order by x1.a",
        &[&["1", "1"], &["1", "1"], &["2", "2"], &["2", "2"], &["3", "3"], &["3", "3"], &["4", "4"], &["4", "4"]],
    );
    check(
        &mut session,
        "select x1.a, x.a from x1 left join (select count(*) a, sum(b) b from x1 group by a union all select * from x2 ) x on x1.a = x.a order by x1.a",
        &[&["1", "1"], &["1", "1"], &["1", "1"], &["1", "1"], &["2", "2"], &["3", "3"], &["4", "4"]],
    );

    rows(&mut session, "drop table if exists x3");
    rows(&mut session, "create table x3(a int , b int);");
    rows(&mut session, "alter table x3 set tiflash replica 1");
    replica_available(&stack, &mut session, "x3");

    rows(&mut session, "insert into x3 values (2, 2), (2, 3), (2, 4)");
    // test nested union all
    check(
        &mut session,
        "select count(*) from (select a, b from x1 union all select a, b from x3 union all (select x1.a, x3.b from (select * from x3 union all select * from x2) x3 left join x1 on x3.a = x1.b)) x",
        &[&["14"]],
    );
    // test union all join union all
    check(
        &mut session,
        "select count(*) from (select * from x1 union all select * from x2 union all select * from x3) x join (select * from x1 union all select * from x2 union all select * from x3) y on x.a = y.b",
        &[&["29"]],
    );
    rows(&mut session, "set @@session.tidb_broadcast_join_threshold_count=100000");
    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/executor/internal/mpp/checkTotalMPPTasks", "return(6)").unwrap();
    check(
        &mut session,
        "select count(*) from (select * from x1 union all select * from x2 union all select * from x3) x join (select * from x1 union all select * from x2 union all select * from x3) y on x.a = y.b",
        &[&["29"]],
    );
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/executor/internal/mpp/checkTotalMPPTasks");

    rows(&mut session, "drop table if exists x4");
    rows(&mut session, "create table x4(a int not null, b int not null);");
    rows(&mut session, "alter table x4 set tiflash replica 1");
    replica_available(&stack, &mut session, "x4");

    rows(&mut session, "set @@tidb_enforce_mpp=1");
    rows(&mut session, "insert into x4 values (2, 2), (2, 3)");
    check(
        &mut session,
        "(select * from x1 union all select * from x4) order by a, b",
        &[&["1", "1"], &["2", "2"], &["2", "2"], &["2", "3"], &["3", "3"], &["4", "4"]],
    );
}

/// Go `TestAvgOverflow`.
#[test]
fn test_avg_overflow() {
    let (stack, mut session) = tiflash_session(2, 2158);
    // avg int
    rows(&mut session, "drop table if exists t");
    rows(&mut session, "create table t (a decimal(1,0))");
    rows(&mut session, "alter table t set tiflash replica 1");
    replica_available(&stack, &mut session, "t");
    rows(&mut session, "insert into t values(9)");
    for _ in 0..16 {
        rows(&mut session, "insert into t select * from t");
    }
    rows(&mut session, "set @@session.tidb_isolation_read_engines=\"tiflash\"");
    rows(&mut session, "set @@session.tidb_enforce_mpp=ON");
    // unistore does not support later materialization
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");
    check(&mut session, "select avg(a) from t group by a", &[&["9.0000"]]);
    rows(&mut session, "drop table if exists t");

    // avg decimal
    rows(&mut session, "drop table if exists td;");
    rows(&mut session, "create table td (col_bigint bigint(20), col_smallint smallint(6));");
    rows(&mut session, "alter table td set tiflash replica 1");
    replica_available(&stack, &mut session, "td");
    for values in [
        "(null, 22876)",
        "(9220557287087669248, 32767)",
        "(28030, 32767)",
        "(-3309864251140603904,32767)",
        "(4,0)",
        "(null,0)",
        "(4,-23828)",
        "(54720,32767)",
        "(0,29815)",
        "(10017,-32661)",
    ] {
        rows(&mut session, &format!("insert into td values {values};"));
    }
    rows(&mut session, "set @@session.tidb_isolation_read_engines=\"tiflash\"");
    rows(&mut session, "set @@session.tidb_enforce_mpp=ON");
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=OFF");
    check(
        &mut session,
        " SELECT AVG( col_bigint / col_smallint) AS field1 FROM td;",
        &[&["25769363061037.62077260"]],
    );
    rows(&mut session, "drop table if exists td;");
}

/// Go `TestMppApply`.
#[test]
fn test_mpp_apply() {
    let (stack, mut session) = tiflash_session(2, 2159);
    rows(&mut session, "drop table if exists x1");
    rows(&mut session, "create table x1(a int primary key, b int);");
    rows(&mut session, "alter table x1 set tiflash replica 1");
    replica_available(&stack, &mut session, "x1");
    rows(&mut session, "insert into x1 values(1, 1),(2, 10),(0,11);");

    rows(&mut session, "create table x2(a int primary key, b int);");
    rows(&mut session, "alter table x2 set tiflash replica 1");
    replica_available(&stack, &mut session, "x2");
    rows(&mut session, "insert into x2 values(1,2),(0,1),(2,-3);");
    rows(&mut session, "analyze table x1, x2;");

    rows(&mut session, "set @@session.tidb_isolation_read_engines=\"tiflash\"");
    // unistore does not support later materialization
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");
    rows(&mut session, "set @@session.tidb_allow_mpp=ON");
    // mock executor does not support use outer table as build side for outer
    // join, so need to force the inner table as build side
    rows(&mut session, "set tidb_opt_mpp_outer_join_fixed_build_side=1");
    // table full scan with correlated filter
    check(
        &mut session,
        "select /*+ agg_to_cop(), hash_agg()*/ count(*) from x1 where a >= any (select a from x2 where x1.a = x2.a) order by 1;",
        &[&["3"]],
    );
    // table range scan with correlated access conditions
    // A Go testkit probe: the TiFlash scan's range is decided by the
    // correlated access condition, resolved per outer row as the MPP tasks
    // are built (`constructMPPTasksImpl`).
    let plan: Vec<String> = displayed(rows(
        &mut session,
        "explain format='brief' select /*+ agg_to_cop(), hash_agg()*/ count(*) from x1 where b > any (select x2.a from x2 where x1.a = x2.a);",
    ))
    .into_iter()
    .map(|row| row.join(" "))
    .collect();
    assert_eq!(
        plan,
        [
            "HashAgg 1.00 root  funcs:count(1)->Column#10",
            "└─Apply 1.92 root  CARTESIAN semi join, left side:TableReader, other cond:or(gt(test.x1.b, Column#7), if(ne(Column#8, 0), NULL, 0))",
            "  ├─TableReader(Build) 2.40 root  MppVersion: 3, data:ExchangeSender",
            "  │ └─ExchangeSender 2.40 mpp[tiflash]  ExchangeType: PassThrough",
            "  │   └─Selection 2.40 mpp[tiflash]  if(isnull(test.x1.b), NULL, 1)",
            "  │     └─TableFullScan 3.00 mpp[tiflash] table:x1 keep order:false",
            "  └─Selection(Probe) 1.92 root  ne(Column#9, 0)",
            "    └─HashAgg 2.40 root  funcs:min(test.x2.a)->Column#7, funcs:sum(0)->Column#8, funcs:count(1)->Column#9",
            "      └─TableReader 2.40 root  MppVersion: 3, data:ExchangeSender",
            "        └─ExchangeSender 2.40 mpp[tiflash]  ExchangeType: PassThrough",
            "          └─TableRangeScan 2.40 mpp[tiflash] table:x2 range: decided by [eq(test.x1.a, test.x2.a)], keep order:false",
        ]
    );
    check(
        &mut session,
        "select /*+ agg_to_cop(), hash_agg()*/ count(*) from x1 where b > any (select x2.a from x2 where x1.a = x2.a);",
        &[&["2"]],
    );
}

/// Go `TestReadPartitionTable`.
#[test]
fn test_read_partition_table() {
    let (stack, mut session) = tiflash_session(2, 2160);
    rows(&mut session, "drop table if exists t");
    rows(&mut session, "create table t(a int not null primary key, b int not null) partition by hash(a) partitions 2");
    rows(&mut session, "alter table t set tiflash replica 1");
    replica_available(&stack, &mut session, "t");
    rows(&mut session, "insert into t values(1,0)");
    rows(&mut session, "insert into t values(2,0)");
    rows(&mut session, "insert into t values(3,0)");
    rows(&mut session, "set @@session.tidb_isolation_read_engines=\"tiflash\"");
    // unistore does not support later materialization
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");
    // mock executor does not support use outer table as build side for outer
    // join, so need to force the inner table as build side
    rows(&mut session, "set tidb_opt_mpp_outer_join_fixed_build_side=1");
    check(&mut session, "select /*+ STREAM_AGG() */ count(*) from t", &[&["3"]]);
    check(&mut session, "select * from t order by a", &[&["1", "0"], &["2", "0"], &["3", "0"]]);

    // test union scan
    rows(&mut session, "begin");
    rows(&mut session, "insert into t values(4,0)");
    check(&mut session, "select /*+ STREAM_AGG() */ count(*) from t", &[&["4"]]);
    rows(&mut session, "insert into t values(5,0)");
    check(&mut session, "select /*+ STREAM_AGG() */ count(*) from t", &[&["5"]]);
    rows(&mut session, "insert into t values(6,0)");
    check(&mut session, "select /*+ STREAM_AGG() */ count(*) from t", &[&["6"]]);
    // test dynamic prune + union scan
    rows(&mut session, "set tidb_partition_prune_mode=dynamic");
    check(&mut session, "select /*+ STREAM_AGG() */ count(*) from t", &[&["6"]]);
    // test dynamic prune + batch cop + union scan
    rows(&mut session, "set tidb_allow_batch_cop=2");
    check(&mut session, "select /*+ STREAM_AGG() */ count(*) from t", &[&["6"]]);
    rows(&mut session, "commit");
}

/// Go `TestTiflashEmptyDynamicPruneResult`.
#[test]
fn test_tiflash_empty_dynamic_prune_result() {
    let (stack, mut session) = tiflash_session(2, 2161);
    rows(&mut session, "drop table if exists t");
    rows(&mut session, "CREATE TABLE `IDT_RP24833` (  `COL1` bigint(16) DEFAULT '15' COMMENT 'NUMERIC UNIQUE INDEX',\n  `COL2` varchar(20) DEFAULT NULL,\n  `COL4` datetime DEFAULT NULL,\n  `COL3` bigint(20) DEFAULT NULL,\n  `COL5` float DEFAULT NULL,\n  KEY `UK_COL1` (`COL1`) /*!80000 INVISIBLE */\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY RANGE ((`COL1`-57))\n(PARTITION `P0` VALUES LESS THAN (-3503857335115112215),\n PARTITION `P1` VALUES LESS THAN (-2987877108151063747),\n PARTITION `P2` VALUES LESS THAN (-1981049919102122710),\n PARTITION `P3` VALUES LESS THAN (-1635802972727465681),\n PARTITION `P4` VALUES LESS THAN (1186020639986357714),\n PARTITION `P5` VALUES LESS THAN (1220018677454711359),\n PARTITION `PMX` VALUES LESS THAN (MAXVALUE));");
    rows(&mut session, "alter table IDT_RP24833 set tiflash replica 1");
    replica_available(&stack, &mut session, "IDT_RP24833");

    rows(&mut session, "insert into IDT_RP24833 values(-8448770111093677011, \"郇鋺篤堯擈斥鍮啸赠璭饱磟朅闑傒聎疫ᛄ怖霃\", \"8781-05-02 04:23:03\", -27252736532807028, -1.34554e38);");
    rows(&mut session, "set @@tidb_partition_prune_mode = 'dynamic';");
    rows(&mut session, "set @@session.tidb_isolation_read_engines=\"tiflash\";");
    // unistore does not support later materialization
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");
    rows(&mut session, "set @@session.tidb_allow_mpp=ON;");
    rows(&mut session, "set @@session.tidb_enforce_mpp = on;");
    check(&mut session, "select /*+ read_from_storage(tiflash[t1]) */  * from IDT_RP24833 partition(p3, p4) t1 where t1. col1 between -8448770111093677011 and -8448770111093677011;", &[]);
    check(&mut session, "select /*+ read_from_storage(tiflash[t2]) */  * from IDT_RP24833 partition(p2) t2 where t2. col1 <= -8448770111093677011;", &[]);
    check(&mut session, "select /*+ read_from_storage(tiflash[t1, t2]) */  * from IDT_RP24833 partition(p3, p4) t1 join IDT_RP24833 partition(p2) t2 on t1.col1 = t2.col1 where t1. col1 between -8448770111093677011 and -8448770111093677011 and t2. col1 <= -8448770111093677011;", &[]);
}

/// Go `TestMPPMultiDistinct3Stage` (`planner/core/casetest/enforcempp`): the
/// 3-stage multi-distinct plans (Go's `plan_tree` explain, plan ids
/// normalized as Go's `checkEnforceMPPPlanRows` does) and results over one
/// mock TiFlash store, against Go's recorded output.
#[test]
fn test_mpp_multi_distinct_3_stage() {
    let (stack, mut session) = tiflash_session(1, 2160);
    rows(&mut session, "drop table if exists t");
    rows(&mut session, "create table t(a int, b int, c int, d int);");
    rows(&mut session, "alter table t set tiflash replica 1");
    replica_available(&stack, &mut session, "t");
    rows(&mut session, "set @@session.tidb_opt_enable_three_stage_multi_distinct_agg=1");
    rows(&mut session, "set @@session.tidb_isolation_read_engines=\"tiflash\";");
    rows(&mut session, "set @@session.tidb_enforce_mpp=1");
    rows(&mut session, "set @@session.tidb_allow_mpp=ON;");
    // Go's `testKit.MustExec("split table t BETWEEN (0) AND (5000) REGIONS
    // 5;")` closes the statement without `Next`, and `SplitTableRegionExec`
    // splits only in `Next`: Go's test never splits (a Go probe that drains
    // the statement splits, and Go then fails "Cannot find region with TiFlash
    // peer" on the new regions, as this store does).
    rows(&mut session, "insert into t values(1000, 1000, 1000, 1),(1000, 1000, 1000, 1),(2000, 2000, 2000, 1),(2000, 2000, 2000, 1),(3000, 3000, 3000, 1),(3000, 3000, 3000, 1),(4000, 4000, 4000, 1),(4000, 4000, 4000, 1),(5000, 5000, 5000, 1),(5000, 5000, 5000, 1)");

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../../../../pkg/planner/core/casetest/enforcempp/testdata/enforce_mpp_suite_out.json"
    ))
    .unwrap();
    let cases = fixture
        .as_array()
        .unwrap()
        .iter()
        .find(|section| section["Name"] == "TestMPPMultiDistinct3Stage")
        .unwrap()["Cases"]
        .as_array()
        .unwrap();
    // Go `normalizeEnforceMPPPlanRows`: every `_<digits>` becomes `_N`.
    let normalize = |row: &str| {
        let mut out = String::with_capacity(row.len());
        let mut chars = row.chars().peekable();
        while let Some(c) = chars.next() {
            out.push(c);
            if c == '_' && chars.peek().is_some_and(char::is_ascii_digit) {
                out.push('N');
                while chars.peek().is_some_and(char::is_ascii_digit) {
                    chars.next();
                }
            }
        }
        out
    };
    let mut mismatches = Vec::new();
    for case in cases {
        let sql = case["SQL"].as_str().unwrap();
        let is_explain = sql.to_lowercase().trim().starts_with("explain");
        // Go `ConvertRowsToStrings`: a row's cells joined by a space, NULL
        // printed as `<nil>`.
        let got: Vec<String> = displayed(rows(&mut session, sql))
            .into_iter()
            .map(|row| row.into_iter().map(|cell| if cell == "NULL" { "<nil>".to_owned() } else { cell }).collect::<Vec<_>>().join(" "))
            .map(|row| if is_explain { normalize(&row) } else { row })
            .collect();
        let want: Vec<String> = case["Plan"]
            .as_array()
            .map(|plan| plan.iter().map(|row| row.as_str().unwrap().to_owned()).collect::<Vec<String>>())
            .unwrap_or_default()
            .into_iter()
            .map(|row: String| if is_explain { normalize(&row) } else { row })
            .collect();
        // Go `ConvertSQLWarnToStrings` of the statement's warnings.
        let got_warn: Vec<String> =
            displayed(rows(&mut session, "show warnings")).into_iter().map(|row| row[2].clone()).collect();
        let want_warn: Vec<String> = case["Warn"]
            .as_array()
            .map(|warn| warn.iter().map(|w| w.as_str().unwrap().to_owned()).collect())
            .unwrap_or_default();
        if got != want || got_warn != want_warn {
            mismatches.push(format!("{sql}\ngot:  {got:#?}\nwant: {want:#?}\ngot warnings:  {got_warn:?}\nwant warnings: {want_warn:?}"));
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n\n"));
}

/// Go `TestTiFlashVirtualColumn`: `MPPGather` computes a virtual column the
/// TiFlash scan answers by default (`FillVirtualColumnValue`).
#[test]
fn test_tiflash_virtual_column() {
    let (stack, mut session) = tiflash_session(2, 2161);
    rows(&mut session, "drop table if exists t1,t2,t3");
    rows(&mut session, "create table t1 (a bit(4), b bit(4), c bit(4) generated always as (a) virtual)");
    rows(&mut session, "alter table t1 set tiflash replica 1");
    replica_available(&stack, &mut session, "t1");
    rows(&mut session, "insert into t1(a,b) values(b'01',b'01'),(b'10',b'10'),(b'11',b'11')");

    rows(&mut session, "create table t2 (a int, b int, c int generated always as (a) virtual)");
    rows(&mut session, "alter table t2 set tiflash replica 1");
    replica_available(&stack, &mut session, "t2");
    rows(&mut session, "insert into t2(a,b) values(1,1),(2,2),(3,3)");

    rows(&mut session, "create table t3 (a bit(4), b bit(4), c bit(4) generated always as (b'01'+b'10') virtual)");
    rows(&mut session, "alter table t3 set tiflash replica 1");
    replica_available(&stack, &mut session, "t3");
    rows(&mut session, "insert into t3(a,b) values(b'01',b'01'),(b'10',b'10'),(b'11',b'11')");

    rows(&mut session, "set @@session.tidb_isolation_read_engines=\"tiflash\"");
    // unistore does not support later materialization
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");
    rows(&mut session, "set @@session.tidb_allow_mpp=ON");
    // mock executor does not support use outer table as build side for outer join, so need to
    // force the inner table as build side
    rows(&mut session, "set tidb_opt_mpp_outer_join_fixed_build_side=1");

    check(&mut session, "select /*+ hash_agg() */ count(*) from t1 where c > b'01'", &[&["2"]]);
    check(&mut session, "select /*+ hash_agg() */ count(*) from t2 where c > 1", &[&["2"]]);
    check(&mut session, "select /*+ hash_agg() */ count(*) from t3 where c > b'01'", &[&["3"]]);
}

/// Go `TestAggPushDownApplyAll`.
#[test]
fn test_agg_push_down_apply_all() {
    let (_stack, mut session) = tiflash_session(2, 2162);
    rows(&mut session, "drop table if exists foo");
    rows(&mut session, "drop table if exists bar");
    rows(&mut session, "create table foo(a int, b int)");
    rows(&mut session, "create table bar(a double not null, b decimal(65,0) not null)");
    rows(&mut session, "alter table foo set tiflash replica 1");
    rows(&mut session, "alter table bar set tiflash replica 1");
    rows(&mut session, "insert into foo values(0, NULL)");
    rows(&mut session, "insert into bar values(0, 0)");

    rows(&mut session, "set @@session.tidb_allow_mpp=1");
    rows(&mut session, "set @@session.tidb_enforce_mpp=1");
    // unistore does not support later materialization
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");

    check(&mut session, "select * from foo where a=all(select a from bar where bar.b=foo.b)", &[&["0", "NULL"]]);
}

/// Go `TestAggPushDownCountStar`.
#[test]
fn test_agg_push_down_count_star() {
    let (stack, mut session) = tiflash_session(2, 2163);
    rows(&mut session, "drop table if exists c");
    rows(&mut session, "drop table if exists o");
    rows(&mut session, "create table c(c_id bigint primary key)");
    rows(&mut session, "create table o(o_id bigint primary key, c_id bigint not null)");
    rows(&mut session, "alter table c set tiflash replica 1");
    replica_available(&stack, &mut session, "c");
    rows(&mut session, "alter table o set tiflash replica 1");
    replica_available(&stack, &mut session, "o");
    rows(&mut session, "insert into c values(1),(2),(3),(4),(5)");
    rows(&mut session, "insert into o values(1,1),(2,1),(3,2),(4,2),(5,2)");

    rows(&mut session, "set @@tidb_enforce_mpp=1");
    rows(&mut session, "set @@tidb_opt_agg_push_down=1");

    check(&mut session, "select count(*) from c, o where c.c_id=o.c_id", &[&["5"]]);
}

/// Go `TestIssue41014`.
#[test]
fn test_issue_41014() {
    let (_stack, mut session) = tiflash_session(2, 2164);
    rows(&mut session, "CREATE TABLE `tai1` (\n  `aid` int(11) DEFAULT NULL,\n  `rid` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin");
    rows(&mut session, "CREATE TABLE `tai2` (\n  `rid` int(11) DEFAULT NULL,\n  `prilan` varchar(20) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin");
    rows(&mut session, "alter table tai1 set tiflash replica 1");
    rows(&mut session, "alter table tai2 set tiflash replica 1");
    rows(&mut session, "alter table tai2 add index idx((lower(prilan)));");
    rows(&mut session, "set @@tidb_opt_distinct_agg_push_down = 1;");
    // Keep the estimate stable; this regression checks that a physical plan can be found.
    rows(&mut session, "set @@tidb_default_string_match_selectivity = 0.8;");

    let got: Vec<String> = displayed(rows(
        &mut session,
        "explain format='brief' select count(distinct tai1.aid) as cb from tai1 inner join tai2 on tai1.rid = tai2.rid where lower(prilan)  LIKE LOWER('%python%');",
    ))
    .into_iter()
    .map(|row| row.join(" "))
    .collect();
    assert_eq!(
        got,
        [
            "HashAgg 1.00 root  funcs:count(distinct test.tai1.aid)->Column#10",
            "└─HashJoin 9990.00 root  inner join, equal:[eq(test.tai2.rid, test.tai1.rid)]",
            "  ├─Selection(Build) 8000.00 root  like(lower(test.tai2.prilan), \"%python%\", 92)",
            "  │ └─Projection 10000.00 root  test.tai2.rid, lower(test.tai2.prilan)",
            "  │   └─TableReader 9990.00 root  data:Selection",
            "  │     └─Selection 9990.00 cop[tikv]  not(isnull(test.tai2.rid))",
            "  │       └─TableFullScan 10000.00 cop[tikv] table:tai2 keep order:false, stats:pseudo",
            "  └─TableReader(Probe) 9990.00 root  data:Selection",
            "    └─Selection 9990.00 cop[tikv]  not(isnull(test.tai1.rid))",
            "      └─TableFullScan 10000.00 cop[tikv] table:tai1 keep order:false, stats:pseudo",
        ]
    );
    check(
        &mut session,
        "select count(distinct tai1.aid) as cb from tai1 inner join tai2 on tai1.rid = tai2.rid where lower(prilan)  LIKE LOWER('%python%');",
        &[&["0"]],
    );
}

/// Go `TestMPP47766`.
#[test]
fn test_mpp_47766() {
    let (stack, mut session) = tiflash_session(1, 2165);
    rows(&mut session, "set @@session.tidb_allow_mpp=1");
    rows(&mut session, "set @@session.tidb_enforce_mpp=1");
    rows(&mut session, "set @@session.tidb_allow_tiflash_cop=off");

    rows(
        &mut session,
        "CREATE TABLE `traces` (  `test_time` timestamp NOT NULL,  `test_time_gen` date GENERATED ALWAYS AS (date(`test_time`)) VIRTUAL,  KEY `traces_date_idx` (`test_time_gen`))",
    );
    rows(&mut session, "alter table `traces` set tiflash replica 1");
    replica_available(&stack, &mut session, "traces");
    let explain = |session: &mut Session, sql: &str| -> Vec<String> {
        displayed(rows(session, sql)).into_iter().map(|row| row.join(" ")).collect()
    };
    assert_eq!(
        explain(&mut session, "explain select date(test_time), count(1) as test_date from `traces` group by 1"),
        [
            "Projection_4 8000.00 root  test.traces.test_time_gen->Column#6, Column#5",
            "└─HashAgg_8 8000.00 root  group by:test.traces.test_time_gen, funcs:count(1)->Column#5, funcs:firstrow(test.traces.test_time_gen)->test.traces.test_time_gen",
            "  └─TableReader_20 10000.00 root  MppVersion: 3, data:ExchangeSender_19",
            "    └─ExchangeSender_19 10000.00 mpp[tiflash]  ExchangeType: PassThrough",
            "      └─TableFullScan_18 10000.00 mpp[tiflash] table:traces keep order:false, stats:pseudo",
        ]
    );
    assert_eq!(
        explain(
            &mut session,
            "explain select /*+ read_from_storage(tiflash[traces]) */ date(test_time) as test_date, count(1) from `traces` group by 1"
        ),
        [
            "TableReader_31 8000.00 root  MppVersion: 3, data:ExchangeSender_30",
            "└─ExchangeSender_30 8000.00 mpp[tiflash]  ExchangeType: PassThrough",
            "  └─Projection_5 8000.00 mpp[tiflash]  date(test.traces.test_time)->Column#6, Column#5",
            "    └─Projection_26 8000.00 mpp[tiflash]  Column#5, test.traces.test_time",
            "      └─HashAgg_27 8000.00 mpp[tiflash]  group by:Column#14, funcs:sum(Column#15)->Column#5, funcs:firstrow(Column#16)->test.traces.test_time",
            "        └─ExchangeReceiver_29 8000.00 mpp[tiflash]  ",
            "          └─ExchangeSender_28 8000.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: Column#14, collate: binary]",
            "            └─HashAgg_25 8000.00 mpp[tiflash]  group by:Column#18, funcs:count(1)->Column#15, funcs:firstrow(Column#17)->Column#16",
            "              └─Projection_32 10000.00 mpp[tiflash]  test.traces.test_time->Column#17, date(test.traces.test_time)->Column#18",
            "                └─TableFullScan_15 10000.00 mpp[tiflash] table:traces keep order:false, stats:pseudo",
        ]
    );
}

/// Go `TestAggPushDownUnionAndMPP`.
#[test]
fn test_agg_push_down_union_and_mpp() {
    let (_stack, mut session) = tiflash_session(2, 2167);
    rows(&mut session, "create table t (a int, b int)");
    rows(&mut session, "alter table t set tiflash replica 1");
    for _ in 0..5 {
        rows(&mut session, "insert into t values (1, 1);");
    }
    rows(&mut session, "set @@tidb_allow_mpp=1;");
    rows(&mut session, "set @@tidb_enforce_mpp=1;");
    rows(&mut session, "set @@tidb_opt_agg_push_down=1");

    rows(&mut session, "create table c(c_id int)");
    rows(&mut session, "create table o(o_id int, c_id int)");
    rows(&mut session, "insert into c values(1),(1),(1),(1)");
    rows(&mut session, "insert into o values(1,1),(1,1),(1,2)");
    rows(&mut session, "alter table c set tiflash replica 1");
    rows(&mut session, "alter table o set tiflash replica 1");

    check(
        &mut session,
        "select a, count(*) from (select a, b from t union all select a, b from t) t group by a order by a limit 10;",
        &[&["1", "10"]],
    );
    check(&mut session, "select o.o_id, count(*) from c, o where c.c_id=o.o_id group by o.o_id", &[&["1", "12"]]);
}

/// Go `TestGroupStreamAggOnTiFlash`.
#[test]
fn test_group_stream_agg_on_tiflash() {
    let (stack, mut session) = tiflash_session(2, 2168);
    rows(&mut session, "drop table if exists foo");
    rows(&mut session, "create table foo(a int, b int, c int, d int, primary key(a,b,c,d))");
    rows(&mut session, "alter table foo set tiflash replica 1");
    rows(&mut session, "insert into foo values(1,2,3,1),(1,2,3,6),(1,2,3,5),(1,2,3,2),(1,2,3,4),(1,2,3,7),(1,2,3,3),(1,2,3,0)");
    replica_available(&stack, &mut session, "foo");
    rows(&mut session, "set @@tidb_allow_mpp=0");
    let sql = "select a,b,c,count(*) from foo group by a,b,c order by a,b,c";
    check(&mut session, sql, &[&["1", "2", "3", "8"]]);
    for row in displayed(rows(&mut session, &format!("explain {sql}"))) {
        let res = row.join(" ");
        // StreamAgg with group keys on TiFlash is not supported
        if res.contains("tiflash") {
            assert!(!res.contains("StreamAgg"), "{res}");
        }
    }
}

/// Go `TestIssue50358`.
#[test]
fn test_issue_50358() {
    let (stack, mut session) = tiflash_session(1, 2169);
    rows(&mut session, "drop table if exists t");
    rows(&mut session, "create table t(a int not null primary key, b int not null)");
    rows(&mut session, "alter table t set tiflash replica 1");
    replica_available(&stack, &mut session, "t");
    rows(&mut session, "insert into t values(1,0)");
    rows(&mut session, "insert into t values(2,0)");

    rows(&mut session, "drop table if exists t1");
    rows(&mut session, "create table t1(c int not null primary key)");
    rows(&mut session, "alter table t1 set tiflash replica 1");
    replica_available(&stack, &mut session, "t1");
    rows(&mut session, "insert into t1 values(3)");

    rows(&mut session, "set @@session.tidb_isolation_read_engines=\"tiflash\"");
    // unistore does not support later materialization
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");
    rows(&mut session, "set @@session.tidb_allow_mpp=ON");
    for _ in 0..20 {
        // test if it is stable.
        check(&mut session, "select 8 from t join t1", &[&["8"], &["8"]]);
    }
}

/// Go `TestNonsupportCharsetTable`.
#[test]
fn test_nonsupport_charset_table() {
    let (_stack, mut session) = tiflash_session(2, 2170);
    rows(&mut session, "drop table if exists t");
    rows(&mut session, "create table t(a int, b char(10) charset gbk collate gbk_bin)");
    assert_eq!(
        super::query_error(&mut session, "alter table t set tiflash replica 1"),
        (8200, "Unsupported `set TiFlash replica` settings for table contains gbk charset".to_owned())
    );

    rows(&mut session, "drop table if exists t");
    rows(&mut session, "create table t(a char(10) charset utf8)");
    rows(&mut session, "alter table t set tiflash replica 1");
}

/// Go `TestIssue57149`.
#[test]
fn test_issue_57149() {
    let (stack, mut session) = tiflash_session(1, 2166);
    rows(&mut session, "drop table if exists t;");
    rows(&mut session, "create table t1 (pk int  NOT NULL PRIMARY KEY AUTO_INCREMENT, i INT, j JSON)");
    rows(&mut session, "alter table test.t1 set tiflash replica 1");
    replica_available(&stack, &mut session, "t1");
    rows(&mut session, "INSERT INTO t1(i, j) VALUES (1, '{\"a\": 2}')");
    rows(&mut session, "analyze table test.t1");
    rows(&mut session, "set @@tidb_isolation_read_engines = 'tiflash'");
    // unistore does not support later materialization
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");
    rows(&mut session, "set @@tidb_enforce_mpp = 1");
    let result = rows(
        &mut session,
        "SELECT i, (j = (SELECT j FROM t1 WHERE j = CAST('null' AS JSON))) AS c11, (j = (SELECT j FROM t1 WHERE 1<>1)) AS c13 from t1 ORDER BY i limit 1",
    );
    assert_eq!(result.len(), 1);
}


/// Go `scalarExprSupportedByFlash` refuses `EQJson`: the JSON comparison of
/// `TestIssue57149` stays above the MPP reader, with Go's refusal warnings
/// (Go testkit probe `zzprobe/issue57149`).
#[test]
fn json_comparison_stays_above_the_mpp_reader_as_go() {
    let (stack, mut session) = tiflash_session(1, 2171);
    rows(&mut session, "create table t1 (pk int  NOT NULL PRIMARY KEY AUTO_INCREMENT, i INT, j JSON)");
    rows(&mut session, "alter table test.t1 set tiflash replica 1");
    replica_available(&stack, &mut session, "t1");
    rows(&mut session, "INSERT INTO t1(i, j) VALUES (1, '{\"a\": 2}')");
    rows(&mut session, "analyze table test.t1");
    rows(&mut session, "set @@tidb_isolation_read_engines = 'tiflash'");
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");
    rows(&mut session, "set @@tidb_enforce_mpp = 1");
    let text = |session: &mut Session, sql: &str| -> Vec<String> {
        displayed(rows(session, sql)).into_iter().map(|row| row.join(" ")).collect()
    };
    // Go's testkit prints `bigint(1)`; a tidb-server, whose
    // `deprecate-integer-display-length` defaults on, prints `bigint`.
    let refused =
        "Warning 1105 Scalar function 'eq'(signature: EQJson, return type: bigint) is not supported to push down to tiflash now.";
    assert_eq!(
        text(&mut session, "explain SELECT j FROM t1 WHERE j = CAST('null' AS JSON)"),
        [
            "Selection_9 1.00 root  eq(test.t1.j, cast(cast(\"null\", json BINARY), json BINARY))",
            "└─TableReader_8 1.00 root  MppVersion: 3, data:ExchangeSender_7",
            "  └─ExchangeSender_7 1.00 mpp[tiflash]  ExchangeType: PassThrough",
            "    └─TableFullScan_6 1.00 mpp[tiflash] table:t1 keep order:false, stats:partial[j:unInitialized]",
        ]
    );
    assert_eq!(text(&mut session, "show warnings"), [refused, refused]);
}

/// Go `TestCanGenerateTiFlashCopWithKeepOrer`.
#[test]
fn test_can_generate_tiflash_cop_with_keep_orer() {
    let (stack, mut session) = tiflash_session(1, 2172);
    rows(&mut session, "create table t1(c1 int, c2 int, primary key(c1));");
    rows(&mut session, "alter table t1 set tiflash replica 1;");
    replica_available(&stack, &mut session, "t1");
    rows(&mut session, "set tidb_isolation_read_engines = 'tiflash';");
    rows(&mut session, "set @@tidb_allow_batch_cop = 0;");

    let found = (0..100).any(|i| {
        let sql = format!(
            "explain format='brief' select /*+ nth_plan({i}), set_var(tidb_allow_tiflash_cop=on) */ * from t1 where c1 < 10 and c2 > 100 order by c1 limit 100"
        );
        let explain: String = displayed(rows(&mut session, &sql)).into_iter().map(|row| format!("{row:?}\t")).collect();
        explain.contains("TableRangeScan") && explain.contains("cop[tiflash]") && explain.contains("keep order:true")
    });
    // expected at least one plan contains TableRangeScan, cop[tiflash], and keep order:true"
    assert!(found);
}

/// Go `TestIssue59877`.
#[test]
fn test_issue_59877() {
    let (stack, mut session) = tiflash_session(1, 2173);
    rows(&mut session, "drop table if exists t1, t2, t3");
    for (table, id) in [("t1", "bigint"), ("t2", "bigint unsigned"), ("t3", "bigint")] {
        rows(&mut session, &format!("create table {table}(id {id}, v1 int)"));
        rows(&mut session, &format!("alter table {table} set tiflash replica 1"));
        replica_available(&stack, &mut session, table);
    }

    rows(&mut session, "set @@session.tidb_isolation_read_engines=\"tiflash\"");
    // unistore does not support later materialization
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");
    rows(&mut session, "set @@session.tidb_allow_mpp=ON");
    rows(&mut session, "set @@session.tidb_enforce_mpp=ON");
    rows(&mut session, "set tidb_broadcast_join_threshold_size=0");
    rows(&mut session, "set tidb_broadcast_join_threshold_count=0");
    rows(&mut session, "set tiflash_fine_grained_shuffle_stream_count=8");
    rows(&mut session, "set tidb_enforce_mpp=1");
    let got: Vec<String> = displayed(rows(
        &mut session,
        "explain format=\"brief\" select /*+ hash_join_build(t3) */ count(*) from t1 straight_join t2 on t1.id = t2.id straight_join t3 on t1.id = t3.id",
    ))
    .into_iter()
    .map(|row| row.join(" "))
    .collect();
    assert_eq!(
        got,
        [
            "HashAgg 1.00 root  funcs:count(Column#21)->Column#13",
            "└─TableReader 1.00 root  MppVersion: 3, data:ExchangeSender",
            "  └─ExchangeSender 1.00 mpp[tiflash]  ExchangeType: PassThrough",
            "    └─HashAgg 1.00 mpp[tiflash]  funcs:count(1)->Column#21",
            "      └─Projection 15609.38 mpp[tiflash]  test.t1.id, Column#17",
            "        └─HashJoin 15609.38 mpp[tiflash]  inner join, equal:[eq(test.t1.id, test.t3.id)]",
            "          ├─ExchangeReceiver(Build) 9990.00 mpp[tiflash]  ",
            "          │ └─ExchangeSender 9990.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: Column#20, collate: binary]",
            "          │   └─Projection 9990.00 mpp[tiflash]  test.t3.id, cast(test.t3.id, decimal(20,0))->Column#20",
            "          │     └─Selection 9990.00 mpp[tiflash]  not(isnull(test.t3.id))",
            "          │       └─TableFullScan 10000.00 mpp[tiflash] table:t3 keep order:false, stats:pseudo",
            "          └─Projection(Probe) 12487.50 mpp[tiflash]  test.t1.id, Column#17",
            "            └─HashJoin 12487.50 mpp[tiflash]  inner join, equal:[eq(test.t1.id, test.t2.id)]",
            "              ├─ExchangeReceiver(Build) 9990.00 mpp[tiflash]  ",
            "              │ └─ExchangeSender 9990.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: Column#16, collate: binary]",
            "              │   └─Projection 9990.00 mpp[tiflash]  test.t1.id, cast(test.t1.id, decimal(20,0))->Column#16",
            "              │     └─Selection 9990.00 mpp[tiflash]  not(isnull(test.t1.id))",
            "              │       └─TableFullScan 10000.00 mpp[tiflash] table:t1 keep order:false, stats:pseudo",
            "              └─ExchangeReceiver(Probe) 9990.00 mpp[tiflash]  ",
            "                └─ExchangeSender 9990.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: Column#17, collate: binary]",
            "                  └─Projection 9990.00 mpp[tiflash]  test.t2.id, cast(test.t2.id, decimal(20,0) UNSIGNED)->Column#17",
            "                    └─Selection 9990.00 mpp[tiflash]  not(isnull(test.t2.id))",
            "                      └─TableFullScan 10000.00 mpp[tiflash] table:t2 keep order:false, stats:pseudo",
        ]
    );
}

/// The four tables of Go's `TestTiFlashPartitionTable*` tests in `database`
/// (`tnormal` by `tnormal_ddl`), each with a hundred random rows; answers
/// the rows and the test's random source.
fn tiflash_partition_tables(
    stack: &UnistoreClusterStack,
    session: &mut Session,
    database: &str,
    tnormal_ddl: &str,
    row_count: usize,
    analyze: bool,
) -> (Vec<(u64, u64)>, impl FnMut() -> u64) {
    // Go `rand.Intn(400)`.
    let mut state = 0x2545_f491_4f6c_dd1d_u64;
    let mut intn = move || {
        state = state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
        (state >> 33) % 400
    };
    rows(session, &format!("create database {database}"));
    rows(session, &format!("use {database}"));
    rows(session, "create table thash (a int, b int) partition by hash(a) partitions 4");
    rows(
        session,
        "create table trange (a int, b int) partition by range(a) (
		partition p0 values less than (100), partition p1 values less than (200),
		partition p2 values less than (300), partition p3 values less than (400))",
    );
    let list_partitions: Vec<String> = (0..4)
        .map(|idx| {
            let values: Vec<String> = (0..400).filter(|i| i % 4 == idx).map(|i| i.to_string()).collect();
            values.join(", ")
        })
        .collect();
    rows(
        session,
        &format!(
            "create table tlist (a int, b int) partition by list(a) (
		partition p0 values in ({}), partition p1 values in ({}),
		partition p2 values in ({}), partition p3 values in ({}))",
            list_partitions[0], list_partitions[1], list_partitions[2], list_partitions[3]
        ),
    );
    rows(session, tnormal_ddl);
    for tbl in ["thash", "trange", "tlist", "tnormal"] {
        rows(session, &format!("alter table {tbl} set tiflash replica 1"));
        replica_available(stack, session, tbl);
    }
    let vals: Vec<(u64, u64)> = (0..row_count).map(|_| (intn(), intn())).collect();
    let values: Vec<String> = vals.iter().map(|(a, b)| format!("({a}, {b})")).collect();
    for tbl in ["thash", "trange", "tlist", "tnormal"] {
        rows(session, &format!("insert into {tbl} values {}", values.join(", ")));
        if analyze {
            rows(session, &format!("analyze table {tbl}"));
        }
    }
    (vals, intn)
}

/// Go `TestTiFlashPartitionTableShuffledHashAggregation`.
#[test]
fn test_tiflash_partition_table_shuffled_hash_aggregation() {
    let (stack, mut session) = tiflash_session(2, 2204);
    let (vals, mut intn) = tiflash_partition_tables(
        &stack,
        &mut session,
        "tiflash_partition_AGG",
        "create table tnormal (a int, b int) partition by hash(a) partitions 4",
        100,
        true,
    );
    rows(&mut session, "set @@session.tidb_isolation_read_engines='tiflash'");
    // unistore does not support later materialization
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");
    rows(&mut session, "set @@session.tidb_enforce_mpp=1");
    // mock executor does not support use outer table as build side for outer join, so need to
    // force the inner table as build side
    rows(&mut session, "set tidb_opt_mpp_outer_join_fixed_build_side=1");

    for _ in 0..2 {
        let (l, r) = (intn(), intn());
        let (l1, r1) = (l.min(r), l.max(r));
        let cond = format!("t1.b>={l1} and t1.b<={r1}");
        let res = vals.iter().filter(|(_, b)| (l1..=r1).contains(b)).count().to_string();
        for mode in ["static", "dynamic"] {
            rows(&mut session, &format!("set @@tidb_partition_prune_mode = '{mode}'"));
            for tbl in ["thash", "trange", "tlist", "tnormal"] {
                let q = format!("select /*+ HASH_AGG() */ count(*) from {tbl} t1 where {cond}");
                let plan = displayed(rows(&mut session, &format!("explain {q}")));
                assert!(plan.iter().any(|row| row[0].contains("HashAgg")), "{q}: {plan:?}");
                assert_eq!(displayed(rows(&mut session, &q)), [[res.clone()]], "{mode} {q}");
            }
        }
    }
}

/// Go `TestTiFlashPartitionTableBroadcastJoin`.
/// Go `TestTiFlashPartitionTableShuffledHashJoin` (skipped there as too
/// slow).
#[test]
fn test_tiflash_partition_table_shuffled_hash_join() {
    let (stack, mut session) = tiflash_session(2, 2209);
    let (vals, mut intn) = tiflash_partition_tables(
        &stack,
        &mut session,
        "tiflash_partition_SHJ",
        "create table tnormal (a int, b int)",
        100,
        true,
    );
    rows(&mut session, "SET tidb_enforce_mpp=1");
    rows(&mut session, "SET tidb_broadcast_join_threshold_count=0");
    rows(&mut session, "SET tidb_broadcast_join_threshold_size=0");
    rows(&mut session, "set @@session.tidb_isolation_read_engines='tiflash'");
    // mock executor does not support use outer table as build side for outer join, so need to
    // force the inner table as build side
    rows(&mut session, "set tidb_opt_mpp_outer_join_fixed_build_side=1");
    // unistore does not support later materialization
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");

    for _ in 0..2 {
        let mut lr = || {
            let (l, r) = (intn(), intn());
            (l.min(r), l.max(r))
        };
        let (l1, r1) = lr();
        let (l2, r2) = lr();
        let cond = format!("t1.b>={l1} and t1.b<={r1} and t2.b>={l2} and t2.b<={r2}");
        let res = vals
            .iter()
            .filter(|(_, b)| (l1..=r1).contains(b))
            .map(|(a1, _)| vals.iter().filter(|(a2, b)| a1 == a2 && (l2..=r2).contains(b)).count())
            .sum::<usize>()
            .to_string();
        for mode in ["static", "dynamic"] {
            rows(&mut session, &format!("set @@tidb_partition_prune_mode = '{mode}'"));
            for tbl in ["thash", "trange", "tlist", "tnormal"] {
                let q = format!("select count(*) from {tbl} t1 join {tbl} t2 on t1.a=t2.a where {cond}");
                assert_eq!(displayed(rows(&mut session, &q)), [[res.clone()]], "{mode} {q}");
            }
        }
    }
}

/// Go `TestTiFlashPartitionTableReader` (skipped there as too slow).
#[test]
fn test_tiflash_partition_table_reader() {
    let (stack, mut session) = tiflash_session(2, 2210);
    let (vals, mut intn) = tiflash_partition_tables(
        &stack,
        &mut session,
        "tiflash_partition_tablereader",
        "create table tnormal (a int, b int)",
        500,
        false,
    );
    // mock executor does not support use outer table as build side for outer join, so need to
    // force the inner table as build side
    rows(&mut session, "set tidb_opt_mpp_outer_join_fixed_build_side=1");
    // unistore does not support later materialization
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");
    rows(&mut session, "SET tidb_enforce_mpp=1");
    rows(&mut session, "set @@session.tidb_isolation_read_engines='tiflash'");
    for _ in 0..10 {
        let (l, r) = (intn(), intn());
        let (l, r) = (l.min(r), l.max(r));
        let mut res: Vec<Vec<String>> = vals
            .iter()
            .filter(|(a, _)| (l..=r).contains(a))
            .map(|(a, b)| vec![a.to_string(), b.to_string()])
            .collect();
        res.sort();
        for mode in ["static", "dynamic"] {
            rows(&mut session, &format!("set @@tidb_partition_prune_mode = '{mode}'"));
            for tbl in ["thash", "trange", "tlist", "tnormal"] {
                let q = format!("select * from {tbl} where a>={l} and a<={r}");
                let mut got = displayed(rows(&mut session, &q));
                got.sort();
                assert_eq!(got, res, "{mode} {q}");
            }
        }
    }
}

#[test]
fn test_tiflash_partition_table_broadcast_join() {
    let (stack, mut session) = tiflash_session(2, 2205);
    let (vals, mut intn) = tiflash_partition_tables(
        &stack,
        &mut session,
        "tiflash_partition_BCJ",
        "create table tnormal (a int, b int)",
        100,
        true,
    );
    rows(&mut session, "set @@session.tidb_isolation_read_engines='tiflash'");
    rows(&mut session, "set @@session.tidb_enforce_mpp=1");
    // mock executor does not support use outer table as build side for outer join, so need to
    // force the inner table as build side
    rows(&mut session, "set tidb_opt_mpp_outer_join_fixed_build_side=1");
    // unistore does not support later materialization
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");

    for _ in 0..2 {
        let mut lr = || {
            let (l, r) = (intn(), intn());
            (l.min(r), l.max(r))
        };
        let (l1, r1) = lr();
        let (l2, r2) = lr();
        let cond = format!("t1.b>={l1} and t1.b<={r1} and t2.b>={l2} and t2.b<={r2}");
        let res = vals
            .iter()
            .filter(|(_, b)| (l1..=r1).contains(b))
            .map(|(a1, _)| vals.iter().filter(|(a2, b)| a1 == a2 && (l2..=r2).contains(b)).count())
            .sum::<usize>()
            .to_string();
        for mode in ["static", "dynamic"] {
            rows(&mut session, &format!("set @@tidb_partition_prune_mode = '{mode}'"));
            for tbl in ["thash", "trange", "tlist", "tnormal"] {
                let q = format!("select count(*) from {tbl} t1 join {tbl} t2 on t1.a=t2.a where {cond}");
                assert_eq!(displayed(rows(&mut session, &q)), [[res.clone()]], "{mode} {q}");
            }
        }
    }
}

/// Go `TestMppTableReaderCacheForSingleSQL`.
#[test]
fn test_mpp_table_reader_cache_for_single_sql() {
    use std::sync::atomic::{AtomicI32, Ordering};
    use tidb_planner_core_operator_physicalop::fragment::mock;
    let (stack, mut session) = tiflash_session(1, 2206);
    rows(&mut session, "create table t(a int, b int, primary key(a))");
    rows(&mut session, "alter table t set tiflash replica 1");
    replica_available(&stack, &mut session, "t");

    rows(&mut session, "create table t2(a int, b int) partition by hash(b) partitions 4");
    rows(&mut session, "alter table t2 set tiflash replica 1");
    replica_available(&stack, &mut session, "t2");
    for i in 1..=5 {
        rows(&mut session, &format!("insert into t values({i}, {i})"));
    }
    for i in 1..=5 {
        rows(&mut session, &format!("insert into t2 values({i}, {i})"));
    }

    // unistore does not support later materialization
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");
    rows(&mut session, "set @@session.tidb_isolation_read_engines=\"tiflash\"");
    rows(&mut session, "set @@session.tidb_allow_mpp=ON");
    rows(&mut session, "set @@session.tidb_enforce_mpp=ON");
    rows(&mut session, "set @@session.tidb_max_chunk_size=32");

    // Test TableReader cache for single SQL.
    let test_cases: &[(&str, i32, i32)] = &[
        // Non-Partition
        // Cache hit
        ("select * from t", 0, 1),
        ("select * from t union select * from t", 1, 1),
        ("select * from t union select * from t t1 union select * from t t2", 2, 1),
        ("select * from t where b <= 3 union select * from t where b > 3", 1, 1), // both full range
        ("select * from t where a <= 3 union select * from t where a <= 3", 1, 1), // same range
        ("select * from t t1 join t t2 on t1.b=t2.b", 1, 1),
        // Cache miss
        ("select * from t union all select * from t", 0, 2), // different MPP task requests
        ("select * from t where a <= 3 union select * from t where a > 3", 0, 2), // different range
        // Partition
        // Cache hit
        ("select * from t2 union select * from t2", 1, 1),
        ("select * from t2 where b = 1 union select * from t2 where b = 5", 1, 1), // same partition, full range
        ("select * from t2 where b = 1 and a < 3 union select * from t2 where b = 5 and a < 3", 1, 1), // same partition, same range
        ("select * from t2 t1 join t2 t2 on t1.b=t2.b", 1, 1),
        ("select * from t2 t1 join t2 t2 on t1.b=t2.b where t1.a = 2 and t2.a = 2", 1, 1),
        // Cache miss
        ("select * from t2 union select * from t2 where b = 1", 0, 2), // different partition
        ("select * from t2 where b = 2 union select * from t2 where b = 1", 0, 2), // different partition
    ];

    let hit_num = Arc::new(AtomicI32::new(0));
    let miss_num = Arc::new(AtomicI32::new(0));
    let (hit, miss) = (Arc::clone(&hit_num), Arc::clone(&miss_num));
    mock::set_mpp_task_generator_table_reader_cache_hit(Some(Arc::new(move || {
        hit.fetch_add(1, Ordering::SeqCst);
    })));
    mock::set_mpp_task_generator_table_reader_cache_miss(Some(Arc::new(move || {
        miss.fetch_add(1, Ordering::SeqCst);
    })));
    for (sql, expect_hit_num, expect_miss_num) in test_cases {
        hit_num.store(0, Ordering::SeqCst);
        miss_num.store(0, Ordering::SeqCst);
        rows(&mut session, sql);
        assert_eq!(
            (hit_num.load(Ordering::SeqCst), miss_num.load(Ordering::SeqCst)),
            (*expect_hit_num, *expect_miss_num),
            "{sql}"
        );
    }
    mock::set_mpp_task_generator_table_reader_cache_hit(None);
    mock::set_mpp_task_generator_table_reader_cache_miss(None);
}

/// Go `TestTiflashSupportStaleRead`.
#[test]
fn test_tiflash_support_stale_read() {
    let (stack, mut session) = tiflash_session(2, 2203);
    rows(&mut session, "drop table if exists t");
    rows(&mut session, "create table t(a bigint(20))");
    rows(&mut session, "alter table t set tiflash replica 1");
    replica_available(&stack, &mut session, "t");
    std::thread::sleep(std::time::Duration::from_secs(2));
    for _ in 0..6 {
        rows(&mut session, "insert into t values (9223372036854775807)");
    }
    let explain = |session: &mut Session, sql: &str| -> String {
        displayed(rows(session, sql)).into_iter().map(|row| format!("{row:?}\n")).collect()
    };
    let res = explain(&mut session, "explain select avg(a) from t");
    assert!(res.contains("tiflash"), "{res}");
    assert!(!res.contains("tikv"), "{res}");
    rows(&mut session, "set transaction read only as of timestamp now(1)");
    let res = explain(&mut session, "explain select avg(a) from t");
    assert!(res.contains("tiflash"), "{res}");
    assert!(!res.contains("tikv"), "{res}");
}

/// Go `TestForbidTiFlashIfExtraPhysTableIDIsNeeded`.
#[test]
fn test_forbid_tiflash_if_extra_phys_table_id_is_needed() {
    let (stack, mut session) = tiflash_session(2, 2174);
    rows(&mut session, "drop table if exists t");
    rows(&mut session, "create table t(a int not null primary key, b int not null) partition by hash(a) partitions 2");
    rows(&mut session, "alter table t set tiflash replica 1");
    replica_available(&stack, &mut session, "t");
    rows(&mut session, "set tidb_partition_prune_mode=dynamic");
    rows(&mut session, "set tidb_enforce_mpp=1");
    let explain = |session: &mut Session, sql: &str| -> String {
        displayed(rows(session, sql)).into_iter().map(|row| format!("{row:?}\n")).collect()
    };

    let res = explain(&mut session, "explain select count(*) from t");
    assert!(res.contains("tiflash"), "{res}");
    assert!(!res.contains("tikv"), "{res}");

    let res = explain(&mut session, "explain select count(*) from t for update");
    assert!(!res.contains("tiflash"), "{res}");
    assert!(res.contains("tikv"), "{res}");

    rows(&mut session, "begin");
    let res = explain(&mut session, "explain select count(*) from t");
    assert!(res.contains("tiflash"), "{res}");
    assert!(!res.contains("tikv"), "{res}");
    rows(&mut session, "insert into t values(1,2)");
    let res = explain(&mut session, "explain select count(*) from t");
    assert!(res.contains("tiflash"), "{res}");
    assert!(!res.contains("tikv"), "{res}");
    rows(&mut session, "rollback");
}

/// Go `TestIndexMergeCarePreferTiflash`.
#[test]
fn test_index_merge_care_prefer_tiflash() {
    let (stack, mut session) = tiflash_session(1, 2175);
    rows(&mut session, "drop table if exists t");
    rows(
        &mut session,
        "CREATE TABLE `t` (\
         `i` bigint(20) NOT NULL, \
         `w` varchar(32) NOT NULL,\
         `l` varchar(32) NOT NULL,\
         `a` tinyint(4) NOT NULL DEFAULT '0',\
         `m` int(11) NOT NULL DEFAULT '0',\
         `s` int(11) NOT NULL DEFAULT '0',\
         PRIMARY KEY (`i`) /*T![clustered_index] NONCLUSTERED */,\
         KEY `idx_win_user_site_code` (`w`,`m`),\
         KEY `idx_lose_user_site_code` (`l`,`m`),\
         KEY `idx_win_site_code_status` (`w`,`a`),\
         KEY `idx_lose_site_code_status` (`l`,`a`)\
         )",
    );
    rows(&mut session, "alter table t set tiflash replica 1");
    replica_available(&stack, &mut session, "t");
    check(
        &mut session,
        "explain format=\"brief\" SELECT      /*+ read_from_storage(tiflash[a]) */ a.i FROM t a WHERE a.s = 0 AND a.a NOT IN (-1, 0) AND m >= 1726910326 AND m <= 1726910391 AND ( a.w IN ('1123') OR a.l IN ('1123'))",
        &[
            &["TableReader", "1.00", "root", "", "MppVersion: 3, data:ExchangeSender"],
            &["└─ExchangeSender", "1.00", "mpp[tiflash]", "", "ExchangeType: PassThrough"],
            &["  └─Projection", "1.00", "mpp[tiflash]", "", "test.t.i"],
            &["    └─Selection", "1.00", "mpp[tiflash]", "", "ge(test.t.m, 1726910326), le(test.t.m, 1726910391), not(in(test.t.a, -1, 0)), or(eq(test.t.w, \"1123\"), eq(test.t.l, \"1123\"))"],
            &["      └─TableFullScan", "10.00", "mpp[tiflash]", "table:a", "pushed down filter:eq(test.t.s, 0), keep order:false, stats:pseudo"],
        ],
    );
}

/// Go `TestTiFlashPlanCacheable`.
#[test]
fn test_tiflash_plan_cacheable() {
    let (stack, mut session) = tiflash_session(2, 2176);
    rows(&mut session, "set tidb_enable_prepared_plan_cache=1");
    rows(&mut session, "drop table if exists t;");
    rows(&mut session, "create table t(a int);");
    rows(&mut session, "set @@tidb_enable_collect_execution_info=0;");
    rows(&mut session, "alter table test.t set tiflash replica 1");
    replica_available(&stack, &mut session, "t");
    rows(&mut session, "set @@session.tidb_isolation_read_engines = 'tikv, tiflash'");
    // unistore does not support later materialization
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");
    rows(&mut session, "insert into t values(1);");
    rows(&mut session, "prepare stmt from 'select /*+ read_from_storage(tiflash[t]) */ * from t;';");
    check(&mut session, "execute stmt;", &[&["1"]]);
    check(&mut session, "select @@last_plan_from_cache", &[&["0"]]);
    check(&mut session, "execute stmt;", &[&["1"]]);
    // The TiFlash plan can not be cached.
    check(&mut session, "select @@last_plan_from_cache", &[&["0"]]);

    rows(&mut session, "prepare stmt from 'select /*+ read_from_storage(tikv[t]) */ * from t;';");
    check(&mut session, "execute stmt;", &[&["1"]]);
    check(&mut session, "select @@last_plan_from_cache", &[&["0"]]);
    check(&mut session, "execute stmt;", &[&["1"]]);
    // The TiKV plan can be cached.
    check(&mut session, "select @@last_plan_from_cache", &[&["1"]]);

    // test the mpp plan
    rows(&mut session, "set @@session.tidb_allow_mpp = 1;");
    rows(&mut session, "set @@session.tidb_enforce_mpp = 1;");
    rows(&mut session, "prepare stmt from 'select count(t1.a) from t t1 join t t2 on t1.a = t2.a where t1.a > ?;';");
    rows(&mut session, "set @a = 0;");
    check(&mut session, "execute stmt using @a;", &[&["1"]]);

    rows(&mut session, "set @a = 1;");
    check(&mut session, "execute stmt using @a;", &[&["0"]]);
    // The TiFlash plan can not be cached.
    check(&mut session, "select @@last_plan_from_cache", &[&["0"]]);
}

/// Go `checkMPPInExplain`.
fn check_mpp_in_explain(session: &mut Session, sql: &str) {
    let res = displayed(rows(session, sql))
        .iter()
        .map(|row| format!("[{}]\n", row.join(" ")))
        .collect::<String>();
    assert!(res.contains("mpp[tiflash]"), "{sql}: {res}");
}

/// Go `TestUnionScan`: an MPP read of a cached table, and of a partitioned
/// table inside a dirty transaction.
#[test]
fn test_union_scan() {
    let (stack, mut session) = tiflash_session(2, 2190);
    rows(&mut session, "set @@session.tidb_allow_mpp=1");
    rows(&mut session, "set @@session.tidb_enforce_mpp=1");
    rows(&mut session, "set @@session.tidb_allow_tiflash_cop=off");

    for x in 0..2 {
        rows(&mut session, "drop table if exists t");
        if x == 0 {
            // Test cache table.
            rows(&mut session, "create table t(a int not null primary key, b int not null)");
            rows(&mut session, "alter table t set tiflash replica 1");
            replica_available(&stack, &mut session, "t");
            rows(&mut session, "alter table t cache");
        } else {
            // Test dirty transaction.
            rows(&mut session, "create table t(a int not null primary key, b int not null) partition by hash(a) partitions 2");
            rows(&mut session, "alter table t set tiflash replica 1");
            replica_available(&stack, &mut session, "t");
        }

        let mut insert_str = "insert into t values(0, 0)".to_owned();
        for i in 1..10 {
            insert_str += &format!(",({i}, {i})");
        }
        rows(&mut session, &insert_str);

        if x != 0 {
            // Test dirty transaction.
            rows(&mut session, "begin");
        }

        // Test Basic.
        let sql = "select /*+ READ_FROM_STORAGE(tiflash[t]) */ count(1) from t";
        check_mpp_in_explain(&mut session, &format!("explain {sql}"));
        check(&mut session, sql, &[&["10"]]);

        // Test Delete.
        rows(&mut session, "delete from t where a = 0");

        let sql = "select  /*+ READ_FROM_STORAGE(tiflash[t]) */ count(1) from t";
        check_mpp_in_explain(&mut session, &format!("explain {sql}"));
        check(&mut session, sql, &[&["9"]]);

        let sql = "select  /*+ READ_FROM_STORAGE(tiflash[t]) */ a, b from t order by 1";
        check_mpp_in_explain(&mut session, &format!("explain {sql}"));
        check(&mut session, sql, &[&["1", "1"], &["2", "2"], &["3", "3"], &["4", "4"], &["5", "5"], &["6", "6"], &["7", "7"], &["8", "8"], &["9", "9"]]);

        // Test Insert.
        rows(&mut session, "insert into t values(100, 100)");

        let sql = "select  /*+ READ_FROM_STORAGE(tiflash[t]) */ count(1) from t";
        check_mpp_in_explain(&mut session, &format!("explain {sql}"));
        check(&mut session, sql, &[&["10"]]);

        let sql = "select  /*+ READ_FROM_STORAGE(tiflash[t]) */ a, b from t order by 1, 2";
        check_mpp_in_explain(&mut session, &format!("explain {sql}"));
        check(&mut session, sql, &[&["1", "1"], &["2", "2"], &["3", "3"], &["4", "4"], &["5", "5"], &["6", "6"], &["7", "7"], &["8", "8"], &["9", "9"], &["100", "100"]]);

        // Test Update
        rows(&mut session, "update t set b = 200 where a = 100");

        let sql = "select  /*+ READ_FROM_STORAGE(tiflash[t]) */ count(1) from t";
        check_mpp_in_explain(&mut session, &format!("explain {sql}"));
        check(&mut session, sql, &[&["10"]]);

        let sql = "select  /*+ READ_FROM_STORAGE(tiflash[t]) */ a, b from t order by 1, 2";
        check_mpp_in_explain(&mut session, &format!("explain {sql}"));
        check(&mut session, sql, &[&["1", "1"], &["2", "2"], &["3", "3"], &["4", "4"], &["5", "5"], &["6", "6"], &["7", "7"], &["8", "8"], &["9", "9"], &["100", "200"]]);

        if x != 0 {
            // Test dirty transaction.
            rows(&mut session, "commit");
        }

        let sql = "select  /*+ READ_FROM_STORAGE(tiflash[t]) */ count(1) from t";
        check_mpp_in_explain(&mut session, &format!("explain {sql}"));
        check(&mut session, sql, &[&["10"]]);

        if x == 0 {
            rows(&mut session, "alter table t nocache");
        }
    }
}

/// Go `TestPartitionTable`: the tasks an MPP read of partitioned tables
/// dispatches under dynamic pruning.
#[test]
fn test_partition_table() {
    let (stack, mut session) = tiflash_session(2, 2191);
    rows(&mut session, "drop table if exists t");
    rows(&mut session, "drop table if exists t1");
    rows(&mut session, "drop table if exists t2");
    rows(&mut session, "create table t(a int not null primary key, b int not null) partition by hash(a+1) partitions 4");
    rows(&mut session, "alter table t set tiflash replica 2");
    replica_available(&stack, &mut session, "t");
    rows(&mut session, "insert into t values(1,0)");
    rows(&mut session, "insert into t values(2,0)");
    rows(&mut session, "insert into t values(3,0)");
    rows(&mut session, "insert into t values(4,0)");
    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/executor/checkUseMPP", "return(true)").unwrap();
    rows(&mut session, "set @@session.tidb_isolation_read_engines=\"tiflash\"");
    rows(&mut session, "set @@session.tidb_allow_mpp=ON");
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=OFF");
    // mock executor does not support use outer table as build side for outer
    // join, so need to force the inner table as build side
    rows(&mut session, "set tidb_opt_mpp_outer_join_fixed_build_side=1");
    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/executor/internal/mpp/checkTotalMPPTasks", "return(2)").unwrap();
    check(&mut session, "select count(*) from t", &[&["4"]]);
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/executor/internal/mpp/checkTotalMPPTasks");
    rows(&mut session, "set @@session.tidb_partition_prune_mode='static-only'");
    check(&mut session, "select count(*) from t", &[&["4"]]);
    rows(&mut session, "set @@session.tidb_partition_prune_mode='dynamic-only'");

    rows(&mut session, "create table t1(a int not null primary key, b int not null) partition by hash(a) partitions 4");
    rows(&mut session, "alter table t1 set tiflash replica 2");
    replica_available(&stack, &mut session, "t1");
    rows(&mut session, "insert into t1 values(1,4)");
    rows(&mut session, "insert into t1 values(2,3)");
    rows(&mut session, "insert into t1 values(3,2)");
    rows(&mut session, "insert into t1 values(4,1)");

    rows(&mut session, "set @@session.tidb_isolation_read_engines=\"tiflash\"");
    rows(&mut session, "set @@session.tidb_allow_mpp=ON");
    // test if it is really work.
    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/executor/internal/mpp/checkTotalMPPTasks", "return(4)").unwrap();
    check(&mut session, "select count(*) from t1 , t where t1.a = t.a", &[&["4"]]);
    // test partition prune
    check(&mut session, "select count(*) from t1 , t where t1.a = t.a and t1.a < 2 and t.a < 2", &[&["1"]]);
    check(&mut session, "select count(*) from t1 , t where t1.a = t.a and t1.a < -1 and t.a < 2", &[&["0"]]);
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/executor/internal/mpp/checkTotalMPPTasks");
    // test multi-way join
    rows(&mut session, "create table t2(a int not null primary key, b int not null)");
    rows(&mut session, "alter table t2 set tiflash replica 2");
    replica_available(&stack, &mut session, "t2");

    rows(&mut session, "insert into t2 values(1,0)");
    rows(&mut session, "insert into t2 values(2,0)");
    rows(&mut session, "insert into t2 values(3,0)");
    rows(&mut session, "insert into t2 values(4,0)");
    // test with no partition table
    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/executor/internal/mpp/checkTotalMPPTasks", "return(5)").unwrap();
    check(&mut session, "select count(*) from t1 , t, t2 where t1.a = t.a and t2.a = t.a", &[&["4"]]);
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/executor/internal/mpp/checkTotalMPPTasks");

    rows(
        &mut session,
        "create table t3(a int not null, b int not null) PARTITION BY RANGE (b) (
		PARTITION p0 VALUES LESS THAN (1),
		PARTITION p1 VALUES LESS THAN (3),
		PARTITION p2 VALUES LESS THAN (5),
		PARTITION p3 VALUES LESS THAN (7)
	);",
    );
    rows(&mut session, "alter table t3 set tiflash replica 2");
    replica_available(&stack, &mut session, "t3");

    rows(&mut session, "insert into t3 values(1,0)");
    rows(&mut session, "insert into t3 values(2,2)");
    rows(&mut session, "insert into t3 values(3,4)");
    rows(&mut session, "insert into t3 values(4,6)");

    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/executor/internal/mpp/checkTotalMPPTasks", "return(4)").unwrap();
    check(&mut session, "select count(*) from t, t3 where t3.a = t.a and t3.b <= 4", &[&["3"]]);
    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/executor/internal/mpp/checkTotalMPPTasks", "return(3)").unwrap();
    check(&mut session, "select count(*) from t, t3 where t3.a = t.a and t3.b > 10", &[&["0"]]);
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/executor/internal/mpp/checkTotalMPPTasks");
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/executor/checkUseMPP");
}

/// Go `TestMppAggShouldAlignFinalMode`: the modes of a statically pruned
/// partition's two-phase aggregate.
#[test]
fn test_mpp_agg_should_align_final_mode() {
    let (stack, mut session) = tiflash_session(1, 2192);
    rows(
        &mut session,
        "create table t (  d date,  v int,  primary key(d, v)) partition by range columns (d) (  partition p1 values less than ('2023-07-02'),  partition p2 values less than ('2023-07-03'));",
    );
    rows(&mut session, "alter table t set tiflash replica 1");
    replica_available(&stack, &mut session, "t");
    rows(&mut session, "set tidb_partition_prune_mode='static';");
    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/expression/aggregation/show-agg-mode", "return(true)").unwrap();

    // unistore does not support later materialization
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");
    rows(&mut session, "set @@session.tidb_isolation_read_engines=\"tiflash\"");
    let got: Vec<String> = displayed(rows(
        &mut session,
        "explain format='brief' select 1 from (  select /*+ read_from_storage(tiflash[t]) */ sum(1)  from t where d BETWEEN '2023-07-01' and '2023-07-03' group by d) total;",
    ))
    .iter()
    .map(|row| row.join(" "))
    .collect();
    assert_eq!(
        got,
        [
            "Projection 400.00 root  1->Column#5",
            "└─HashAgg 400.00 root  group by:test.t.d, funcs:count(complete,1)->Column#9",
            "  └─PartitionUnion 400.00 root  ",
            "    ├─Projection 200.00 root  test.t.d",
            "    │ └─HashAgg 200.00 root  group by:test.t.d, funcs:firstrow(partial2,test.t.d)->test.t.d, funcs:count(final,Column#13)->Column#10",
            "    │   └─TableReader 200.00 root  MppVersion: 3, data:ExchangeSender",
            "    │     └─ExchangeSender 200.00 mpp[tiflash]  ExchangeType: PassThrough",
            "    │       └─HashAgg 200.00 mpp[tiflash]  group by:test.t.d, funcs:count(partial1,1)->Column#13",
            "    │         └─TableRangeScan 250.00 mpp[tiflash] table:t, partition:p1 range:[2023-07-01,2023-07-03], keep order:false, stats:pseudo",
            "    └─Projection 200.00 root  test.t.d",
            "      └─HashAgg 200.00 root  group by:test.t.d, funcs:firstrow(partial2,test.t.d)->test.t.d, funcs:count(final,Column#15)->Column#11",
            "        └─TableReader 200.00 root  MppVersion: 3, data:ExchangeSender",
            "          └─ExchangeSender 200.00 mpp[tiflash]  ExchangeType: PassThrough",
            "            └─HashAgg 200.00 mpp[tiflash]  group by:test.t.d, funcs:count(partial1,1)->Column#15",
            "              └─TableRangeScan 250.00 mpp[tiflash] table:t, partition:p2 range:[2023-07-01,2023-07-03], keep order:false, stats:pseudo",
        ]
    );

    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/expression/aggregation/show-agg-mode");
}

/// Go `TestIssue59703`: a coordinator whose `Execute` fails is unregistered.
#[test]
fn test_issue_59703() {
    let (stack, mut session) = tiflash_session(1, 2193);
    rows(&mut session, "drop table if exists t");
    rows(&mut session, "create table t(a int not null primary key, b int not null)");
    rows(&mut session, "alter table t set tiflash replica 1");
    replica_available(&stack, &mut session, "t");
    rows(&mut session, "insert into t values(1,0)");
    rows(&mut session, "insert into t values(2,0)");
    rows(&mut session, "set @@session.tidb_isolation_read_engines=\"tiflash\"");
    // unistore does not support later materialization
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");
    rows(&mut session, "set @@session.tidb_allow_mpp=ON");

    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/executor/internal/mpp/mpp_coordinator_execute_err", "return").unwrap();

    let (_, message) = query_error(&mut session, "select count(*) from t");
    assert!(message.contains("mock mpp error"), "{message}");
    assert_eq!(
        tidb_executor::mppcoordmanager::INSTANCE_MPP_COORDINATOR_MANAGER.get_coord_count(),
        0
    );
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/executor/internal/mpp/mpp_coordinator_execute_err");
}

/// The table of Go's `TestDispatchTaskRetry` and `TestMppVersionError`.
fn dispatch_retry_table(connection_id: u64) -> (UnistoreClusterStack, Session) {
    let (stack, mut session) = tiflash_session(2, connection_id);
    rows(&mut session, "drop table if exists t");
    rows(&mut session, "create table t(a int not null primary key, b int not null)");
    rows(&mut session, "alter table t set tiflash replica 1");
    rows(&mut session, "insert into t values(1,0)");
    rows(&mut session, "insert into t values(2,0)");
    rows(&mut session, "insert into t values(3,0)");
    rows(&mut session, "insert into t values(4,0)");
    replica_available(&stack, &mut session, "t");
    rows(&mut session, "set @@session.tidb_enforce_mpp=ON");
    (stack, session)
}

/// Go `TestDispatchTaskRetry`: a dispatch the store fails three times is
/// sent again until it is served.
#[test]
fn test_dispatch_task_retry() {
    let (_stack, mut session) = dispatch_retry_table(2194);
    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/mppDispatchTimeout", "3*return(true)").unwrap();
    check(&mut session, "select count(*) from t group by b", &[&["4"]]);
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/mppDispatchTimeout");
}

/// Go `TestTiFlashFallback`'s MPP dispatch arm: a dispatch that keeps
/// failing answers its own error, and the TiFlash server timeout once
/// `tidb_allow_fallback_to_tikv` names TiFlash.
#[test]
fn test_tiflash_fallback_mpp_dispatch() {
    let (_stack, mut session) = dispatch_retry_table(2198);
    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/executor/internal/mpp/ReduceCopNextMaxBackoff", "return(true)").unwrap();
    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/mppDispatchTimeout", "return(true)").unwrap();
    let sql = "select * from t t1 join t t2 on t1.a = t2.a";
    rows(&mut session, "set @@tidb_allow_fallback_to_tikv=''");
    let (code, _) = query_error(&mut session, sql);
    assert_ne!(code, tidb_errno::errcode::ErrTiFlashServerTimeout);
    rows(&mut session, "set @@tidb_allow_fallback_to_tikv='tiflash'");
    let (code, _) = query_error(&mut session, sql);
    assert_eq!(code, tidb_errno::errcode::ErrTiFlashServerTimeout);
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/mppDispatchTimeout");
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/executor/internal/mpp/ReduceCopNextMaxBackoff");
}

/// Go `physicalplantest`'s case loop over `plan_suite_out.json`: a `set` or
/// `insert` case is executed, every other case's rows
/// (`ConvertRowsToStrings`) and warnings are the recorded ones.
fn check_plan_suite(session: &mut Session, name: &str) {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../../../../pkg/planner/core/casetest/physicalplantest/testdata/plan_suite_out.json"
    ))
    .unwrap();
    let cases = fixture.as_array().unwrap().iter().find(|section| section["Name"] == name).unwrap()["Cases"]
        .as_array()
        .unwrap();
    let strings = |value: &serde_json::Value| -> Vec<String> {
        value
            .as_array()
            .map(|rows| rows.iter().map(|row| row.as_str().unwrap().to_owned()).collect())
            .unwrap_or_default()
    };
    let mut mismatches = Vec::new();
    for case in cases {
        let sql = case["SQL"].as_str().unwrap();
        if sql.starts_with("set") || sql.starts_with("insert") {
            for statement in sql.split(';').filter(|statement| !statement.trim().is_empty()) {
                rows(session, statement);
            }
            continue;
        }
        let got: Vec<String> = displayed(rows(session, sql)).into_iter().map(|row| row.join(" ")).collect();
        let got_warn: Vec<String> =
            displayed(rows(session, "show warnings")).into_iter().map(|row| row[2].clone()).collect();
        let (want, want_warn) = (strings(&case["Plan"]), strings(&case["Warn"]));
        if got != want || got_warn != want_warn {
            mismatches.push(format!("{sql}\ngot:  {got:#?}\nwant: {want:#?}\ngot warnings:  {got_warn:?}\nwant warnings: {want_warn:?}"));
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n\n"));
}

/// Go `GetMPPClient().GetMPPStoreCount()` of the stack's store.
fn mpp_store_count(stack: &UnistoreClusterStack) -> Result<usize, String> {
    stack.factory.mpp_client.as_ref().unwrap().get_mpp_store_count()
}

/// Go `TestMPPBCJModel`: with three MPP stores the planner does not choose
/// a broadcast join under `tidb_prefer_broadcast_join_by_exchange_data_size`.
#[test]
fn test_mpp_bcj_model() {
    let (stack, mut session) = tiflash_session(3, 2199);
    rows(&mut session, "create table t (a int, b int, c int, index idx_a(a), index idx_b(b))");
    rows(&mut session, "alter table t set tiflash replica 1");
    replica_available(&stack, &mut session, "t");
    assert_eq!(mpp_store_count(&stack), Ok(3));
    check_plan_suite(&mut session, "TestMPPBCJModel");
}

/// Go `TestMPPPreferBCJ`.
#[test]
fn test_mpp_prefer_bcj() {
    let (stack, mut session) = tiflash_session(3, 2200);
    rows(&mut session, "drop table if exists t1");
    rows(&mut session, "create table t1 (a int)");
    rows(&mut session, "drop table if exists t2");
    rows(&mut session, "create table t2 (b int)");
    rows(&mut session, "insert into t1 values (1);");
    rows(&mut session, "insert into t2 values (1), (2), (3), (4), (5), (6), (7), (8);");
    rows(&mut session, "alter table t1 set tiflash replica 1");
    replica_available(&stack, &mut session, "t1");
    rows(&mut session, "alter table t2 set tiflash replica 1");
    replica_available(&stack, &mut session, "t2");
    rows(&mut session, "analyze table t1 all columns");
    rows(&mut session, "analyze table t2 all columns");
    rows(&mut session, "set @@tidb_allow_mpp=1");
    rows(&mut session, "set @@tidb_enforce_mpp=1");
    check_plan_suite(&mut session, "TestMPPPreferBCJ");
}

/// Go `TestMPPBCJModelOneTiFlash`: with one MPP store the planner chooses
/// a broadcast join under `tidb_prefer_broadcast_join_by_exchange_data_size`.
#[test]
fn test_mpp_bcj_model_one_tiflash() {
    let (stack, mut session) = tiflash_session(1, 2201);
    rows(&mut session, "create table t (a int, b int, c int, index idx_a(a), index idx_b(b))");
    rows(&mut session, "alter table t set tiflash replica 1");
    replica_available(&stack, &mut session, "t");
    assert_eq!(mpp_store_count(&stack), Ok(1));
    session
        .node_conn()
        .execute("set @@session.tidb_prefer_broadcast_join_by_exchange_data_size=-1")
        .map(|_| ())
        .unwrap_err();
    session
        .node_conn()
        .execute("set @@session.tidb_prefer_broadcast_join_by_exchange_data_size=2")
        .map(|_| ())
        .unwrap_err();
    // no BCJ if `tidb_prefer_broadcast_join_by_exchange_data_size` is OFF
    rows(&mut session, "set @@session.tidb_broadcast_join_threshold_size=0");
    rows(&mut session, "set @@session.tidb_broadcast_join_threshold_count=0");
    check_plan_suite(&mut session, "TestMPPBCJModelOneTiFlash");
}

/// Go `TestMppVersionError`: a store that cancels the task's MPP version
/// fails the query, without a retry; one that serves it answers.
#[test]
fn test_mpp_version_error() {
    let (_stack, mut session) = dispatch_retry_table(2195);
    let newest = tidb_txnkv::MppVersion::NEWEST.as_i64();
    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/MppVersionError", &format!("return({})", newest + 1)).unwrap();
    query_error(&mut session, "select count(*) from t group by b");
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/MppVersionError");
    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/MppVersionError", &format!("return({})", newest)).unwrap();
    check(&mut session, "select count(*) from t group by b", &[&["4"]]);
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/MppVersionError");
}

/// Go `TestCancelMppTasks`: a kill ends a query whose root stream hangs.
#[test]
fn test_cancel_mpp_tasks() {
    let (stack, mut session) = tiflash_session(2, 2196);
    rows(&mut session, "drop table if exists t");
    rows(&mut session, "create table t(a int not null primary key, b int not null)");
    rows(&mut session, "alter table t set tiflash replica 1");
    rows(&mut session, "insert into t values(1,0)");
    rows(&mut session, "insert into t values(2,0)");
    rows(&mut session, "insert into t values(3,0)");
    rows(&mut session, "insert into t values(4,0)");
    replica_available(&stack, &mut session, "t");
    rows(&mut session, "set @@session.tidb_isolation_read_engines=\"tiflash\"");
    rows(&mut session, "set @@session.tidb_allow_mpp=ON");
    // unistore does not support later materialization
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");
    // mock executor does not support use outer table as build side for outer
    // join, so need to force the inner table as build side
    rows(&mut session, "set tidb_opt_mpp_outer_join_fixed_build_side=1");
    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/mppRecvHang", "return(true)").unwrap();
    let killer = session.routed_statement_memory().sql_killer().clone();
    let kill = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(1));
        killer.send_kill_signal(tidb_sqlkiller::KillSignal::QueryInterrupted);
    });
    let (code, _) = query_error(&mut session, "select count(*) from t as t1 , t where t1.a = t.a");
    assert_eq!(code, tidb_errno::errcode::ErrQueryInterrupted);
    kill.join().unwrap();
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/mppRecvHang");
}

/// Go `TestMPPMemoryTracker`: a response waiting for the gather is charged
/// to the statement.
#[test]
fn test_mpp_memory_tracker() {
    let (stack, mut session) = tiflash_session(2, 2202);
    rows(&mut session, "set tidb_mem_quota_query = 1 << 30");
    rows(&mut session, "set global tidb_mem_oom_action = 'CANCEL'");
    rows(&mut session, "create table t(a int);");
    rows(&mut session, "insert into t values (1);");
    rows(&mut session, "alter table t set tiflash replica 1");
    replica_available(&stack, &mut session, "t");
    rows(&mut session, "set tidb_enforce_mpp = on;");
    assert_eq!(displayed(rows(&mut session, "select * from t")), [["1"]]);
    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/executor/internal/mpp/testMPPOOMPanic", "return(true)").unwrap();
    let (code, _) = query_error(&mut session, "select * from t");
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/executor/internal/mpp/testMPPOOMPanic");
    assert_eq!(code, tidb_errno::errcode::ErrMemoryExceedForQuery);
}

/// Go `TestNoAliveTiFlashRetry`: a build of the batch tasks that finds no
/// alive TiFlash store keeps the tasks it located.
#[test]
fn test_no_alive_tiflash_retry() {
    let (stack, mut session) = tiflash_session(1, 2208);
    rows(&mut session, "drop table if exists t");
    rows(&mut session, "create table t(a int not null primary key, b int not null)");
    rows(&mut session, "alter table t set tiflash replica 1");
    replica_available(&stack, &mut session, "t");

    let mut dml = "insert into t values".to_owned();
    for i in 0..50 {
        dml += &format!("({i}, 0)");
        if i != 49 {
            dml += ",";
        }
    }
    rows(&mut session, &dml);

    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/store/copr/mockNoAliveTiFlash", "return(true)").unwrap();
    let count = displayed(rows(&mut session, "select count(*) from t"));
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/store/copr/mockNoAliveTiFlash");
    assert_eq!(count, [["50"]]);
}

/// A non-MPP TiFlash read is Go's `batchCop` or `cop` request by
/// `tidb_allow_batch_cop`, over a plain and a dynamically pruned partitioned
/// table: the plans and rows of a Go testkit run on two mock TiFlash stores
/// (`testdata/tiflash_batch_cop_go.txt`).
#[test]
fn a_tiflash_read_is_batch_cop_or_cop_like_go() {
    let (stack, mut session) = tiflash_session(2, 2231);
    rows(&mut session, "create table t(a int primary key, b int)");
    rows(
        &mut session,
        "create table pt(a int primary key, b int) partition by range(a) (partition p1 values less than (10), partition p2 values less than (20), partition p3 values less than (30))",
    );
    for table in ["t", "pt"] {
        rows(&mut session, &format!("alter table {table} set tiflash replica 1"));
        replica_available(&stack, &mut session, table);
        rows(&mut session, &format!("insert into {table} values(1,1),(2,10),(0,11),(11,3),(21,4)"));
    }
    rows(&mut session, "set @@tidb_partition_prune_mode = 'dynamic'");
    rows(&mut session, "set @@session.tidb_isolation_read_engines=\"tiflash\"");
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");
    rows(&mut session, "set @@session.tidb_allow_tiflash_cop=ON");
    rows(&mut session, "set @@session.tidb_allow_mpp=OFF");

    // Go's `Result.Sort` orders rows by their column strings.
    let go_rows = |session: &mut Session, sql: &str, sort: bool| -> Vec<String> {
        let mut rows = displayed(rows(session, sql));
        if sort {
            rows.sort();
        }
        rows.iter()
            .map(|row| format!("[{}]", row.join(" ")))
            .collect()
    };
    let fixture = include_str!("../../../../../testdata/tiflash_batch_cop_go.txt");
    let mut cases = 0;
    for case in fixture.split("== ").skip(1) {
        let mut lines = case.lines();
        let (mode, query) = lines.next().unwrap().split_once(" | ").unwrap();
        let (mut plan, mut result) = (Vec::new(), Vec::new());
        for line in lines {
            match line.strip_prefix("RES ") {
                Some(row) => result.push(row.to_owned()),
                None => plan.push(line.to_owned()),
            }
        }
        rows(&mut session, &format!("set @@tidb_allow_batch_cop = {mode}"));
        assert_eq!(
            go_rows(&mut session, &format!("explain format='brief' {query}"), false),
            plan,
            "{mode} | {query}"
        );
        assert_eq!(go_rows(&mut session, query, true), result, "{mode} | {query}");
        cases += 1;
    }
    assert_eq!(cases, 27);
}

/// Go `pkg/store`'s `TestStoreErr` and `TestStoreSwitchPeer` table: one row
/// read through batch cop from `stores` TiFlash stores.
fn store_err_table(stores: usize, connection_id: u64) -> (UnistoreClusterStack, Session) {
    let (stack, mut session) = tiflash_session(stores, connection_id);
    rows(&mut session, "create table t(a int not null, b int not null)");
    rows(&mut session, "alter table t set tiflash replica 1");
    rows(&mut session, "set @@session.tidb_allow_tiflash_cop=ON");
    replica_available(&stack, &mut session, "t");
    rows(&mut session, "insert into t values(1,0)");
    rows(&mut session, "set @@session.tidb_isolation_read_engines=\"tiflash\"");
    rows(&mut session, "set @@session.tidb_allow_mpp=OFF");
    (stack, session)
}

/// Go `TestStoreErr`: a canceled batch cop request is the statement's
/// `context.Canceled`, one failed send is retried, and a store that keeps
/// failing is the statement's error.
#[test]
fn test_store_err() {
    let (_stack, mut session) = store_err_table(1, 2233);

    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/BatchCopCancelled", "1*return(true)").unwrap();
    let (_, message) = query_error(&mut session, "select count(*) from t");
    // Go `errors.Cause(err) == context.Canceled`, under this tree's storage
    // error text.
    assert_eq!(message, "storage error: context canceled");

    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/BatchCopRpcErrtiflash0", "1*return(\"tiflash0\")").unwrap();
    check(&mut session, "select count(*) from t", &[&["1"]]);

    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/BatchCopRpcErrtiflash0", "return(\"tiflash0\")").unwrap();
    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/store/copr/ReduceCopNextMaxBackoff", "return(true)").unwrap();
    let (code, _) = query_error(&mut session, "select count(*) from t");
    assert_eq!(code, tidb_errno::errcode::ErrTiFlashServerTimeout);
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/store/copr/ReduceCopNextMaxBackoff");
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/BatchCopRpcErrtiflash0");
}

/// Go `TestStoreSwitchPeer`: a batch cop task whose store keeps failing is
/// rebuilt on the region's other TiFlash peer, and fails once that one
/// fails too.
#[test]
fn test_store_switch_peer() {
    let (_stack, mut session) = store_err_table(2, 2234);

    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/BatchCopRpcErrtiflash0", "return(\"tiflash0\")").unwrap();
    check(&mut session, "select count(*) from t", &[&["1"]]);

    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/BatchCopRpcErrtiflash1", "return(\"tiflash1\")").unwrap();
    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/store/copr/ReduceCopNextMaxBackoff", "return(true)").unwrap();
    let (code, _) = query_error(&mut session, "select count(*) from t");
    assert_eq!(code, tidb_errno::errcode::ErrTiFlashServerTimeout);
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/store/copr/ReduceCopNextMaxBackoff");
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/BatchCopRpcErrtiflash0");
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/BatchCopRpcErrtiflash1");
}

/// Go server `TestTiFlashFallback`'s batch cop arm (`testFallbackWork`): a
/// batch cop read whose store keeps failing is the statement's own error
/// without the fallback, the TiFlash server timeout `handleQuery` falls back
/// on with it, and the re-run without TiFlash answers from TiKV.
#[test]
fn test_tiflash_fallback_batch_cop() {
    let (stack, mut session) = tiflash_session(1, 2235);
    rows(&mut session, "set @@session.tidb_allow_tiflash_cop=ON");
    rows(&mut session, "set tidb_cost_model_version=1");
    rows(&mut session, "create table t(a int not null primary key, b int not null)");
    rows(&mut session, "alter table t set tiflash replica 1");
    replica_available(&stack, &mut session, "t");
    let values: Vec<String> = (0..50).map(|i| format!("({i}, 0)")).collect();
    rows(&mut session, &format!("insert into t values{}", values.join(",")));
    check(&mut session, "select count(*) from t", &[&["50"]]);

    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/store/copr/ReduceCopNextMaxBackoff", "return(true)").unwrap();
    rows(&mut session, "set @@tidb_allow_batch_cop=1");
    rows(&mut session, "set @@tidb_allow_mpp=0");
    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/BatchCopRpcErrtiflash0", "return(\"tiflash0\")").unwrap();
    let sql = "select count(*) from t";
    rows(&mut session, "set @@tidb_allow_fallback_to_tikv=''");
    let (code, _) = query_error(&mut session, sql);
    assert!(!session.tiflash_fallback_allowed(code));
    rows(&mut session, "set @@tidb_allow_fallback_to_tikv='tiflash'");
    let (code, _) = query_error(&mut session, sql);
    assert_eq!(code, tidb_errno::errcode::ErrTiFlashServerTimeout);
    assert!(session.tiflash_fallback_allowed(code));
    session.set_tiflash_fallback(true);
    check(&mut session, sql, &[&["50"]]);
    session.set_tiflash_fallback(false);
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/BatchCopRpcErrtiflash0");

    // Go `batchCopRecvTimeout`: a stream that ends in an error is the same
    // timeout.
    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/batchCopRecvTimeout", "return(true)").unwrap();
    rows(&mut session, "set @@tidb_allow_fallback_to_tikv=''");
    let (code, _) = query_error(&mut session, sql);
    assert!(!session.tiflash_fallback_allowed(code));
    rows(&mut session, "set @@tidb_allow_fallback_to_tikv='tiflash'");
    let (code, _) = query_error(&mut session, sql);
    assert_eq!(code, tidb_errno::errcode::ErrTiFlashServerTimeout);
    session.set_tiflash_fallback(true);
    check(&mut session, sql, &[&["50"]]);
    session.set_tiflash_fallback(false);
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/batchCopRecvTimeout");
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/store/copr/ReduceCopNextMaxBackoff");
}

/// Go `handleTaskOnce` and `handleStreamedBatchCopResponse`'s caller: a
/// TiFlash `cop` or `batchCop` request names no resource group while
/// resource control is off (TiFlash reads an empty name as "disabled"); a
/// TiKV request keeps the name.
#[test]
fn a_tiflash_request_names_no_resource_group_without_resource_control_like_go() {
    use tikv_client::proto::coprocessor;
    let (stack, mut session) = tiflash_session(2, 2232);
    rows(&mut session, "create table t(a int primary key, b int)");
    rows(&mut session, "alter table t set tiflash replica 1");
    replica_available(&stack, &mut session, "t");
    rows(&mut session, "insert into t values(1,1),(2,10)");
    rows(&mut session, "set @@session.tidb_allow_tiflash_cop=ON");
    rows(&mut session, "set @@session.tidb_allow_mpp=OFF");

    let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = std::sync::Arc::clone(&sent);
    stack.rpc.set_send_hook(Some(std::sync::Arc::new(move |request| {
        let any = request.as_any();
        let context = if let Some(cop) = any.downcast_ref::<coprocessor::Request>() {
            Some(("cop", cop.context.clone()))
        } else {
            any.downcast_ref::<tikv_client::tikv::BatchCoprocessorStreamRequest>()
                .map(|batch| ("batchCop", batch.request().context.clone()))
        };
        if let Some((kind, context)) = context {
            let context = context.unwrap();
            // A background session's read is not the statement's.
            if !context.request_source.starts_with("internal") {
                let name = context.resource_control_context.unwrap().resource_group_name;
                recorded.lock().unwrap().push((kind, name));
            }
        }
    })));
    let mut names = |session: &mut Session, engine: &str, batch_cop: u8| {
        rows(session, &format!("set @@session.tidb_isolation_read_engines='{engine}'"));
        rows(session, &format!("set @@tidb_allow_batch_cop = {batch_cop}"));
        sent.lock().unwrap().clear();
        assert_eq!(displayed(rows(session, "select count(*) from t")), [["2"]]);
        let mut names = std::mem::take(&mut *sent.lock().unwrap());
        names.dedup();
        names
    };
    let default = "default".to_owned();
    assert_eq!(names(&mut session, "tiflash", 0), [("cop", default.clone())]);
    assert_eq!(names(&mut session, "tiflash", 2), [("batchCop", default.clone())]);
    assert_eq!(names(&mut session, "tikv", 0), [("cop", default.clone())]);

    rows(&mut session, "set global tidb_enable_resource_control = off");
    assert_eq!(names(&mut session, "tiflash", 0), [("cop", String::new())]);
    assert_eq!(names(&mut session, "tiflash", 2), [("batchCop", String::new())]);
    assert_eq!(names(&mut session, "tikv", 0), [("cop", default)]);
    stack.rpc.set_send_hook(None);
}

/// Go `TestMPPRecovery`: a held read's error is recovered and the gather
/// re-executed, at most three times, and never under the TiFlash fallback.
#[test]
fn test_mpp_recovery() {

    let (stack, mut session) = tiflash_session(2, 2207);
    rows(&mut session, "create table t(a int, b int)");
    rows(&mut session, "alter table t set tiflash replica 1");
    replica_available(&stack, &mut session, "t");

    let mut check_strs = vec![vec!["0".to_owned(), "0".to_owned()]];
    let mut insert_str = "insert into t values(0, 0)".to_owned();
    for i in 1..1500 {
        insert_str += &format!(",({i}, {i})");
        check_strs.push(vec![i.to_string(), i.to_string()]);
    }
    rows(&mut session, &insert_str);
    rows(&mut session, "set @@session.tidb_isolation_read_engines=\"tiflash\"");
    // unistore does not support later materialization
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");
    let sql = "select * from t order by 1, 2";

    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/executor/internal/mpp/mpp_recovery_test_mock_enable", "return").unwrap();
    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/executor/internal/mpp/mpp_recovery_test_ignore_recovery_err", "return").unwrap();
    // Test different chunk size. And force one mpp err.
    {
        tidb_failpoint::enable("github.com/pingcap/tidb/pkg/executor/internal/mpp/mpp_recovery_test_max_err_times", "return(1)").unwrap();

        rows(&mut session, "set @@tidb_max_chunk_size = default");
        assert_eq!(displayed(rows(&mut session, sql)), check_strs);
        rows(&mut session, "set @@tidb_max_chunk_size = 32");
        assert_eq!(displayed(rows(&mut session, sql)), check_strs);

        tidb_failpoint::disable("github.com/pingcap/tidb/pkg/executor/internal/mpp/mpp_recovery_test_max_err_times");
    }

    // Test exceeds max recovery times. Default max times is 3.
    {
        tidb_failpoint::enable("github.com/pingcap/tidb/pkg/executor/internal/mpp/mpp_recovery_test_max_err_times", "return(5)").unwrap();

        rows(&mut session, "set @@tidb_max_chunk_size = 32");
        let (_, message) = query_error(&mut session, sql);
        assert!(message.contains("mock mpp err"), "{message}");

        tidb_failpoint::disable("github.com/pingcap/tidb/pkg/executor/internal/mpp/mpp_recovery_test_max_err_times");
    }

    {
        // When AllowFallbackToTiKV, mpp err recovery should be disabled.
        // So event we inject mock err multiple times, the query should be ok.
        rows(&mut session, "set @@tidb_allow_fallback_to_tikv = \"tiflash\"");
        tidb_failpoint::enable("github.com/pingcap/tidb/pkg/executor/internal/mpp/mpp_recovery_test_max_err_times", "return(5)").unwrap();

        rows(&mut session, "set @@tidb_max_chunk_size = 32");
        assert_eq!(displayed(rows(&mut session, sql)), check_strs);

        rows(&mut session, "set @@tidb_allow_fallback_to_tikv = default");
        tidb_failpoint::disable("github.com/pingcap/tidb/pkg/executor/internal/mpp/mpp_recovery_test_max_err_times");
    }

    // Test hold logic. Default hold 4 * MaxChunkSize rows.
    {
        tidb_failpoint::enable("github.com/pingcap/tidb/pkg/executor/internal/mpp/mpp_recovery_test_max_err_times", "return(0)").unwrap();

        rows(&mut session, "set @@tidb_max_chunk_size = 32");
        let expected_hold_size = 2;
        tidb_failpoint::enable("github.com/pingcap/tidb/pkg/executor/internal/mpp/mpp_recovery_test_hold_size", &format!("1*return({})", expected_hold_size)).unwrap();
        assert_eq!(displayed(rows(&mut session, sql)), check_strs);
        tidb_failpoint::disable("github.com/pingcap/tidb/pkg/executor/internal/mpp/mpp_recovery_test_hold_size");

        tidb_failpoint::disable("github.com/pingcap/tidb/pkg/executor/internal/mpp/mpp_recovery_test_max_err_times");
    }
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/executor/internal/mpp/mpp_recovery_test_ignore_recovery_err");
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/executor/internal/mpp/mpp_recovery_test_mock_enable");

    {
        // We have 2 mock tiflash, but the table is small, so only 1 tiflash node is in computation.
        rows(&mut session, "set @@tidb_max_chunk_size = 32");
        assert_eq!(displayed(rows(&mut session, sql)), check_strs);
    }

    rows(&mut session, "set @@tidb_max_chunk_size = default");
}

/// Go `TestMppGoroutinesExitFromErrors`: the non-root tasks' errors end a
/// query whose root stream hangs.
#[test]
fn test_mpp_goroutines_exit_from_errors() {
    let (stack, mut session) = tiflash_session(2, 2197);
    rows(&mut session, "drop table if exists t");
    rows(&mut session, "create table t(a int not null primary key, b int not null)");
    rows(&mut session, "alter table t set tiflash replica 1");
    replica_available(&stack, &mut session, "t");
    rows(&mut session, "insert into t values(1,0)");
    rows(&mut session, "insert into t values(2,0)");
    rows(&mut session, "insert into t values(3,0)");
    rows(&mut session, "drop table if exists t1");
    rows(&mut session, "create table t1(a int not null primary key, b int not null)");
    rows(&mut session, "alter table t1 set tiflash replica 1");
    replica_available(&stack, &mut session, "t1");
    rows(&mut session, "insert into t1 values(1,0)");
    rows(&mut session, "insert into t1 values(2,0)");
    rows(&mut session, "insert into t1 values(3,0)");
    rows(&mut session, "set @@session.tidb_isolation_read_engines=\"tiflash\"");
    rows(&mut session, "set @@session.tidb_allow_mpp=ON");
    // unistore does not support later materialization
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");
    // mock executor does not support use outer table as build side for outer
    // join, so need to force the inner table as build side
    rows(&mut session, "set tidb_opt_mpp_outer_join_fixed_build_side=1");
    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/executor/internal/mpp/mppNonRootTaskError", "return(true)").unwrap();
    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/mppRecvHang", "return(true)").unwrap();

    // generate 2 root tasks, one will hang and another will return errors
    let (code, _) = query_error(&mut session, "select count(*) from t as t1 , t where t1.a = t.a");
    assert_eq!(code, tidb_errno::errcode::ErrTiFlashServerTimeout);
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/executor/internal/mpp/mppNonRootTaskError");
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/mppRecvHang");
}

/// Go `TestTiflashPartitionTableScan`.
#[test]
fn test_tiflash_partition_table_scan() {
    let (stack, mut session) = tiflash_session(2, 2230);
    rows(
        &mut session,
        "create table t(\n    a int,\n    primary key(a)\n) partition by range(a) (\n    partition p1 values less than (10),\n    partition p2 values less than (20),\n    partition p3 values less than (30),\n    partition p4 values less than (40),\n    partition p5 values less than (50)\n);",
    );
    rows(&mut session, "alter table t set tiflash replica 1");
    replica_available(&stack, &mut session, "t");
    rows(&mut session, "insert into t values(1),(11),(21),(31),(41);");
    rows(&mut session, "set @@tidb_partition_prune_mode = 'dynamic';");
    rows(&mut session, "set @@session.tidb_isolation_read_engines=\"tiflash\";");
    // unistore does not support later materialization
    rows(&mut session, "set @@session.tidb_opt_enable_late_materialization=0");
    rows(&mut session, "set @@session.tidb_allow_tiflash_cop=ON");
    let count = |session: &mut Session| displayed(rows(session, "select count(*) from t where a < 12;"));
    let two = vec![vec!["2".to_owned()]];
    // MPP
    rows(&mut session, "set @@session.tidb_allow_mpp=ON;");
    assert_eq!(count(&mut session), two);

    // BatchCop
    rows(&mut session, "set @@session.tidb_allow_mpp=OFF;");
    rows(&mut session, "set @@tidb_allow_batch_cop = 2;");
    assert_eq!(count(&mut session), two);

    // test retry batch cop
    let busy_for_100ms = || {
        tidb_failpoint::enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/rpcServerBusy", "return(true)").unwrap();
        std::thread::spawn(|| {
            std::thread::sleep(std::time::Duration::from_millis(100));
            tidb_failpoint::disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/rpcServerBusy");
        })
    };
    // MPP
    let wg = busy_for_100ms();
    rows(&mut session, "set @@session.tidb_allow_mpp=ON;");
    assert_eq!(count(&mut session), two);
    wg.join().unwrap();

    // BatchCop
    let wg = busy_for_100ms();
    rows(&mut session, "set @@session.tidb_allow_mpp=OFF;");
    rows(&mut session, "set @@tidb_allow_batch_cop = 2;");
    assert_eq!(count(&mut session), two);
    wg.join().unwrap();
}

/// Go `config.UpdateGlobal` to a disaggregated TiFlash (with or without the
/// AutoScaler) and its deferred restore.
struct DisaggregatedTiFlash(std::sync::Arc<tidb_config::config_tree::config::Config>);

impl DisaggregatedTiFlash {
    fn update_global(use_auto_scaler: bool) -> Self {
        let previous = tidb_config::config_tree::config::get_global_config();
        let mut conf = (*previous).clone();
        conf.disaggregated_tiflash = true;
        conf.use_auto_scaler = use_auto_scaler;
        tidb_config::config_tree::config::store_global_config(conf);
        Self(previous)
    }
}

impl Drop for DisaggregatedTiFlash {
    fn drop(&mut self) {
        tidb_config::config_tree::config::store_global_config((*self.0).clone());
    }
}

/// The table Go's disaggregated tests read through TiFlash only.
fn disaggregated_table(stack: &UnistoreClusterStack, session: &mut Session) {
    rows(session, "drop table if exists t");
    rows(session, "create table t(c1 int)");
    rows(session, "alter table t set tiflash replica 1");
    replica_available(stack, session, "t");
    rows(session, "set @@session.tidb_isolation_read_engines=\"tiflash\"");
    // unistore does not support later materialization
    rows(session, "set @@session.tidb_opt_enable_late_materialization=0");
}

/// Go `TestDisaggregatedTiFlash`.
#[test]
fn test_disaggregated_tiflash() {
    let _config = DisaggregatedTiFlash::update_global(true);
    tidb_tiflashcompute::init_global_topo_fetcher(tidb_config::tiflash::TEST_AS_STR, "tmpAddr", "tmpClusterID", false)
        .unwrap();
    let (stack, mut session) = tiflash_session(2, 2236);
    disaggregated_table(&stack, &mut session);

    let (_, message) = query_error(&mut session, "select * from t;");
    // Expect error, because TestAutoScaler return empty topo.
    assert!(
        message.contains("Cannot find proper topo to dispatch MPPTask: topo from AutoScaler is empty"),
        "{message}"
    );

    tidb_tiflashcompute::init_global_topo_fetcher(tidb_config::tiflash::AWS_AS_STR, "tmpAddr", "tmpClusterID", false)
        .unwrap();
    let (code, message) = query_error(&mut session, "select * from t;");
    // Expect error, because AWSAutoScaler is not setup, so http request will fail.
    // Go's `[util:1815]Internal : ...` is its terror's text; the wire carries
    // the code and the message.
    assert_eq!((code, message.as_str()), (1815, "Internal : get tiflash_compute topology failed"));
}

/// Go `TestDisaggregatedTiFlashNonAutoScaler`.
#[test]
fn test_disaggregated_tiflash_non_auto_scaler() {
    let _config = DisaggregatedTiFlash::update_global(false);
    // Setting globalTopoFetcher to nil to can make sure cannot fetch topo from AutoScaler.
    let err = tidb_tiflashcompute::init_global_topo_fetcher(
        tidb_config::tiflash::INVALID_AS_STR,
        "tmpAddr",
        "tmpClusterID",
        false,
    )
    .unwrap_err();
    assert!(err.contains("unexpected topo fetch type. expect: mock or aws or gcp, got invalid"), "{err}");

    let (stack, mut session) = tiflash_session(2, 2237);
    disaggregated_table(&stack, &mut session);
    let (_, message) = query_error(&mut session, "select * from t;");
    // This error message means we use PD instead of AutoScaler.
    assert!(message.contains("tiflash_compute node is unavailable"), "{message}");
}

/// Go `TestTiFlashComputeDispatchPolicy`.
#[test]
fn test_tiflash_compute_dispatch_policy() {
    let _config = DisaggregatedTiFlash::update_global(false);
    let (stack, mut session) = tiflash_session(2, 2238);

    // Default policy is 'consistent_hash'
    check(&mut session, "select @@tiflash_compute_dispatch_policy", &[&["consistent_hash"]]);

    // tiflash_compute_dispatch_policy is global variable.
    rows(&mut session, "set @@session.tiflash_compute_dispatch_policy = 'consistent_hash';");
    check(&mut session, "select @@tiflash_compute_dispatch_policy", &[&["consistent_hash"]]);
    rows(&mut session, "set @@session.tiflash_compute_dispatch_policy = 'round_robin';");
    check(&mut session, "select @@tiflash_compute_dispatch_policy", &[&["round_robin"]]);
    let invalid = "unexpected tiflash_compute dispatch policy, expect [consistent_hash round_robin], got ";
    let (_, message) = query_error(&mut session, "set @@session.tiflash_compute_dispatch_policy = 'error_dispatch_policy';");
    assert_eq!(message, format!("{invalid}error_dispatch_policy"));

    // Invalid values.
    let (_, message) = query_error(&mut session, "set global tiflash_compute_dispatch_policy = 'error_dispatch_policy';");
    assert_eq!(message, format!("{invalid}error_dispatch_policy"));
    let (_, message) = query_error(&mut session, "set global tiflash_compute_dispatch_policy = '';");
    assert_eq!(message, invalid);
    let (_, message) = query_error(&mut session, "set global tiflash_compute_dispatch_policy = 100;");
    assert_eq!(message, format!("{invalid}100"));

    disaggregated_table(&stack, &mut session);
    tidb_tiflashcompute::init_global_topo_fetcher(tidb_config::tiflash::TEST_AS_STR, "tmpAddr", "tmpClusterID", false)
        .unwrap();

    // Valid values.
    let which = "github.com/pingcap/tidb/pkg/store/copr/testWhichDispatchPolicy";
    for (connection_id, use_as) in [(2239, true), (2240, false)] {
        let _config = DisaggregatedTiFlash::update_global(use_as);
        for p in tidb_tiflashcompute::DispatchPolicy::valid_names() {
            rows(&mut session, &format!("set global tiflash_compute_dispatch_policy = '{p}';"));
            let mut tk1 = stack.factory.open_session(session_context(connection_id)).unwrap();
            rows(&mut tk1, "use test");
            check(&mut tk1, "select @@tiflash_compute_dispatch_policy", &[&[p]]);
            let _fp = tidb_failpoint::enable_for_test(which, &format!("return(\"{p}\")"));
            rows(&mut tk1, "set @@session.tidb_isolation_read_engines=\"tiflash\"");
            rows(&mut tk1, "set @@session.tidb_opt_enable_late_materialization=0");
            let (_, message) = query_error(&mut tk1, "select * from t;");
            if use_as {
                // Expect error, because TestAutoScaler return empty topo.
                assert!(
                    message.contains("Cannot find proper topo to dispatch MPPTask: topo from AutoScaler is empty"),
                    "{message}"
                );
            } else {
                // This error message means we use PD instead of AutoScaler.
                assert!(message.contains("tiflash_compute node is unavailable"), "{message}");
            }
        }
    }
}
