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
        &format!("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = '{table}'"),
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

/// Go `TestMppUnionAll` (without its `checkTotalMPPTasks` failpoint run).
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
    check(
        &mut session,
        "select count(*) from (select * from x1 union all select * from x2 union all select * from x3) x join (select * from x1 union all select * from x2 union all select * from x3) y on x.a = y.b",
        &[&["29"]],
    );

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

