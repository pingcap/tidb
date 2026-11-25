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

//! Executable TiFlash metadata-copy and truncate row regressions. The
//! durable-DDL, mock/multi-node and schema-validation cases of the Go file are
//! not ported.

use crate::support::testkit::{enable_failpoint, TestKit};
use tidb_infoschema::TableEntry;

/// Go `external.GetTableByName(t, tk, db, table).Meta()`: the stored table.
fn kv_table(tk: &TestKit, table: &str) -> std::sync::Arc<tidb_table::tables::TableCommon> {
    match tk.get_table_by_name("test", table) {
        TableEntry::Kv(table) => table,
        _ => panic!("{table} is a stored table"),
    }
}

/// Go `tiflash_replica_test.go:477-493::TestTruncateTable2`, rows contract:
/// after inserting (1,1),(2,2) and truncating, `insert (3,3),(4,4)` and
/// `select *` answer exactly `3 3` / `4 4` -- the truncate emptied the rows
/// and the table stays live for writes.
#[test]
fn truncate_table_empties_the_rows_and_the_table_stays_live() {
    let mut tk = TestKit::new();
    tk.must_exec("create table truncate_table (c1 int, c2 int)");
    tk.must_exec("insert truncate_table values (1, 1), (2, 2)");

    tk.must_exec("truncate table truncate_table");

    tk.must_exec("insert truncate_table values (3, 3), (4, 4)");
    // Go :493: only the post-truncate rows are there.
    tk.must_query("select * from truncate_table").check(["3 3", "4 4"]);
}

/// Go `tiflash_replica_test.go:440-474::TestCreateTableWithLike2`, the TiFlash
/// arms. After both partitions of a hash-partitioned `t1` are mocked
/// available (`UpdateTableReplicaInfo`), `create table t2 like t1` copies
/// `TiFlashReplica.Count` and `LocationLabels` but lands with
/// `Available=false` and empty `AvailablePartitionIDs` (Go
/// `BuildTableInfoWithLike`, `pkg/ddl/create_table.go:1281-1289` keeps the
/// settings and strips the availability), while `t1` itself stays available
/// with both partition ids.
#[test]
fn create_table_like_copies_tiflash_replica_settings_clearing_availability() {
    // Test for table has tiflash  replica.
    let _store_count = enable_failpoint("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount", "return(true)");
    let mut tk = TestKit::new();
    tk.must_exec("drop table if exists t1,t2;");
    tk.must_exec("create table t1 (a int) partition by hash(a) partitions 2;");
    tk.must_exec("alter table t1 set tiflash replica 3 location labels 'a','b';");
    let t1 = kv_table(&tk, "t1");
    // Mock for all partitions replica was available.
    let partition_ids = t1
        .meta().read().get_partition_info()
        .expect("source table is partitioned")
        .read().definitions.snapshot()
        .iter()
        .map(|definition| definition.id)
        .collect::<Vec<_>>();
    assert_eq!(partition_ids.len(), 2);
    for &id in &partition_ids {
        tk.update_table_replica_info(id, true);
    }
    let replica = tidb_model::TiFlashReplicaInfo {
        count: 3,
        location_labels: vec!["a".to_owned(), "b".to_owned()].into(),
        available: true,
        available_partition_ids: partition_ids.clone().into(),
    };
    assert_eq!(kv_table(&tk, "t1").tiflash_replica(), Some(replica.clone()));

    tk.must_exec("create table t2 like t1");
    assert_eq!(
        kv_table(&tk, "t2").tiflash_replica(),
        Some(tidb_model::TiFlashReplicaInfo {
            count: 3,
            location_labels: vec!["a".to_owned(), "b".to_owned()].into(),
            available: false,
            available_partition_ids: Default::default(),
        })
    );
    // Test for not affecting the original table.
    assert_eq!(kv_table(&tk, "t1").tiflash_replica(), Some(replica.clone()));
}

/// Go `enforce_mpp_test.go::TestEnforceMPPWarning1`, cases 1-4: with MPP
/// enforced, the planner says why a table cannot be read from TiFlash -- no
/// replica, a replica not ready (`getPossibleAccessPaths`), TiFlash outside
/// `tidb_isolation_read_engines` (`FilterPathByIsolationRead`), and a hint
/// to read from TiKV (`buildDataSource`).
#[test]
fn enforced_mpp_says_why_tiflash_is_not_read_like_go() {
    let _store_count = enable_failpoint("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount", "return(true)");
    let mut tk = TestKit::new();
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t(a int, b int as (a+1), c enum('xx', 'yy'), d bit(1))");
    tk.must_exec("create index idx on t(a)");
    tk.must_exec("set @@tidb_allow_mpp=1");
    tk.must_exec("set @@tidb_enforce_mpp=1");
    let warnings = |tk: &mut TestKit, sql: &str| {
        tk.must_query(sql);
        tk.must_query("show warnings")
    };

    // 1. no replica
    warnings(&mut tk, "explain select count(*) from t where a=1").check([
        "Warning 1105 MPP mode may be blocked because there aren't tiflash replicas of table `t`.",
    ]);
    // 2. replica not ready
    tk.must_exec("alter table t set tiflash replica 1");
    warnings(&mut tk, "explain select count(*) from t where a=1").check([
        "Warning 1105 MPP mode may be blocked because tiflash replicas of table `t` not ready.",
    ]);
    let id = kv_table(&tk, "t").meta().read().id;
    tk.update_table_replica_info(id, true);
    // 3. isolation_engine not match
    tk.must_exec("set @@session.tidb_isolation_read_engines = 'tikv'");
    warnings(&mut tk, "explain select count(*) from t where a=1").check([
        "Warning 1105 MPP mode may be blocked because 'tidb_isolation_read_engines'(value: 'tikv') not match, need 'tiflash'.",
    ]);
    // 4. hint use tikv
    tk.must_exec("set @@session.tidb_isolation_read_engines = 'tikv, tiflash'");
    warnings(
        &mut tk,
        "explain select /*+ read_from_storage(tikv[t]) */ count(*) from t where a=1",
    )
    .check(["Warning 1105 MPP mode may be blocked because you have set a hint to read table `t` from TiKV."]);
}

fn table_id(tk: &mut TestKit, table: &str) -> String {
    tk.must_query(&format!(
        "select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = '{table}'"
    ))
    .rows()[0][0]
        .clone()
}

/// Go `onSetTableFlashReplica`, `onTruncateTable` and `createTable` set the
/// table's TiFlash placement rule through `infosync.ConfigureTiFlashPDForTable`
/// as the job runs; a zero count is the rule's delete.
#[test]
fn tiflash_replica_jobs_set_pd_rules_through_infosync() {
    use tidb_domain_infosync::tiflash_manager::{init_tiflash_replica_manager, MockTiFlashReplicaManagerCtx};
    let _store_count = enable_failpoint("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount", "return(true)");
    let mut tk = TestKit::new();
    let manager = std::sync::Arc::new(MockTiFlashReplicaManagerCtx::default());
    init_tiflash_replica_manager(Some(manager.clone()));
    tk.must_exec("drop table if exists t1");
    tk.must_exec("create table t1 (a int)");
    tk.must_exec("alter table t1 set tiflash replica 1");
    let first = table_id(&mut tk, "t1");
    tk.must_exec("truncate table t1");
    let second = table_id(&mut tk, "t1");
    tk.must_exec("alter table t1 set tiflash replica 0");
    assert_eq!(
        manager.calls(),
        [
            format!("set table-{first}-r 1"),
            format!("set table-{second}-r 1"),
            format!("set table-{second}-r 0"),
        ]
    );
}

/// Go `TestEnforceMPPWarning4` (`planner/core/casetest/enforcempp`): the
/// warnings Go's `tryToGetMppHashJoin` raises under `tidb_enforce_mpp`, from
/// `enforce_mpp_suite_out.json` (the plan rows are Go's `plan_tree` format and
/// are not compared here). Case 9 is Go master's wording, which names `full
/// outer join` beside `left outer join`.
#[test]
fn enforce_mpp_warning4() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("set tidb_hash_join_version=optimized");
    tk.must_exec("drop table if exists t");
    tk.must_exec("CREATE TABLE t(a int primary key)");
    tk.must_exec("drop table if exists s");
    tk.must_exec("CREATE TABLE s(a int primary key)");
    crate::support::testkit::set_tiflash_replica(&tk, "test", "t");
    crate::support::testkit::set_tiflash_replica(&tk, "test", "s");
    let hint = "MPP mode may be blocked because you have used hint to specify a join algorithm which is not supported by mpp now.";
    let cartesian_var = "MPP mode may be blocked because `Cartesian Product` is only supported by broadcast join, check value and documents of variable `tidb_opt_broadcast_cartesian_join`.";
    let cartesian_size = "MPP mode may be blocked because `Cartesian Product` is only supported by broadcast join, check value and documents of variables `tidb_broadcast_join_threshold_size` and `tidb_broadcast_join_threshold_count`.";
    let left = "MPP mode may be blocked because left conditions are only supported for `left outer join` and `full outer join` now.";
    let cases: Vec<(&str, Vec<&str>)> = vec![
        ("set @@tidb_allow_mpp=1;set @@tidb_enforce_mpp=1;", vec![]),
        ("explain format = 'brief' SELECT /*+ MERGE_JOIN(t,s) */ * from t join s using(a)", vec![hint, hint]),
        ("explain format = 'brief' SELECT /*+ INL_JOIN(t,s) */ * from t, s where t.a=s.a", vec![hint, hint]),
        ("explain format = 'brief' SELECT /*+ INL_HASH_JOIN(t,s) */ * from t join s using(a)", vec![hint, hint]),
        ("explain format = 'brief' SELECT /*+ HASH_JOIN(t,s) */ * from t join s using(a)", vec![]),
        ("set @@tidb_opt_broadcast_cartesian_join = 0", vec![]),
        ("explain format = 'brief' SELECT * from t join s", vec![cartesian_var, cartesian_var]),
        ("set @@tidb_broadcast_join_threshold_size = 0; set @@tidb_opt_broadcast_cartesian_join = 1", vec![]),
        ("explain format = 'brief' SELECT * from t join s", vec![cartesian_size, cartesian_size]),
        ("set @@tidb_broadcast_join_threshold_size = 104857600; set @@tidb_opt_broadcast_cartesian_join = 1", vec![]),
        ("explain format = 'brief' SELECT * from t join s", vec![]),
        ("set @@tidb_broadcast_join_threshold_size = 0; set @@tidb_opt_broadcast_cartesian_join = 2", vec![]),
        ("explain format = 'brief' SELECT * from t join s", vec![]),
        ("set @@tidb_broadcast_join_threshold_size = 104857600; set @@tidb_opt_broadcast_cartesian_join = 1;", vec![]),
        ("explain format = 'brief' select a from t where t.a>1 or t.a in (select a from t)", vec![]),
        ("explain format = 'brief' select a from t where t.a>1 or t.a not in (select a from t)", vec![]),
        ("explain format = 'brief' select a from t where t.a not in (select a from s where t.a<1)", vec![left, left]),
    ];
    for (sql, want) in cases {
        if sql.starts_with("set") {
            for statement in sql.split(';').map(str::trim).filter(|statement| !statement.is_empty()) {
                tk.must_exec(statement);
            }
            continue;
        }
        tk.must_query(sql);
        let warnings = tk.must_query("show warnings");
        let got: Vec<String> = warnings.rows().iter().map(|row| row[2].clone()).collect();
        assert_eq!(got, want, "{sql}");
    }
}

/// Go master `prepareMPPFullOuterJoinTest` (#70562).
fn mpp_full_outer_join_kit() -> TestKit {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    for sql in [
        "set tidb_cost_model_version=2",
        "set @@tidb_allow_mpp=1",
        "set @@tidb_enforce_mpp=1",
        "set @@tidb_enable_full_outer_join=1",
        "create table t1 (a int, b int)",
        "create table t2 (a int, b int)",
    ] {
        tk.must_exec(sql);
    }
    crate::support::testkit::set_tiflash_replica(&tk, "test", "t1");
    crate::support::testkit::set_tiflash_replica(&tk, "test", "t2");
    tk
}

/// Go master `findMPPFullOuterHashJoin`: the plan's TiFlash full outer hash
/// join, which must be an MPP shuffle join.
fn mpp_full_outer_hash_join(tk: &mut TestKit, sql: &str) -> Vec<String> {
    let plan: Vec<Vec<String>> = tk.must_query(&format!("explain format='brief' {sql}")).rows().to_vec();
    let join = plan
        .iter()
        .find(|row| row[0].contains("HashJoin") && row[2] == "mpp[tiflash]" && row[4].starts_with("full outer join"))
        .unwrap_or_else(|| panic!("expected MPP TiFlash full outer hash join: {plan:?}"));
    // A shuffle join hashes both inputs: no broadcast exchange below it.
    assert!(
        plan.iter().all(|row| !row[4].contains("ExchangeType: Broadcast")),
        "{plan:?}"
    );
    join.clone()
}

/// Go master `TestMPPFullOuterJoinToPB`'s planning half (#70562): the shuffle
/// hint plans an MPP full outer hash join keeping one left and one right
/// condition. The `ToPB` assertion needs a plan-to-protobuf seam this
/// harness lacks (and neither Go's nor Rust's mock TiFlash executes the join).
#[test]
fn mpp_full_outer_join_to_pb() {
    let mut tk = mpp_full_outer_join_kit();
    let join = mpp_full_outer_hash_join(
        &mut tk,
        "select /*+ shuffle_join(t1, t2), read_from_storage(tiflash[t1, t2]) */ * from t1 full outer join t2 on t1.a = t2.a and t1.b > 1 and t2.b > 1",
    );
    assert!(join[4].contains("left cond:[gt(test.t1.b, 1)]"), "{join:?}");
    assert!(join[4].contains("right cond:gt(test.t2.b, 1)"), "{join:?}");
}

/// Go master `TestMPPFullOuterJoinWithoutShuffleHint` (#70562): even with the
/// broadcast thresholds high, a full outer join plans as an MPP shuffle join
/// (`preferMppBCJ` answers false for it) and raises no warning.
#[test]
fn mpp_full_outer_join_without_shuffle_hint() {
    let mut tk = mpp_full_outer_join_kit();
    tk.must_exec("set @@tidb_broadcast_join_threshold_count=1000000000");
    tk.must_exec("set @@tidb_broadcast_join_threshold_size=1000000000");
    mpp_full_outer_hash_join(
        &mut tk,
        "select /*+ read_from_storage(tiflash[t1, t2]) */ * from t1 full outer join t2 on t1.a = t2.a and t1.b > 1 and t2.b > 1",
    );
    tk.must_query("show warnings").check(Vec::<&str>::new());
}

/// Go master `attach2TaskForMpp4PhysicalHashJoin` (#70562): a full outer
/// join keeps both sides' rows, so neither side's hash partition describes
/// its output; an aggregation grouped by a join key re-shuffles above it
/// (Go master testkit probe).
#[test]
fn mpp_full_outer_join_output_is_not_hash_partitioned() {
    let mut tk = mpp_full_outer_join_kit();
    let plan: Vec<Vec<String>> = tk
        .must_query("explain format='brief' select /*+ shuffle_join(t1, t2), read_from_storage(tiflash[t1, t2]) */ t1.a, count(*) from t1 full outer join t2 on t1.a = t2.a group by t1.a")
        .rows()
        .to_vec();
    let agg = plan.iter().position(|row| row[0].contains("HashAgg")).expect("aggregation");
    let join = plan.iter().position(|row| row[0].contains("HashJoin")).expect("join");
    assert!(
        plan[agg..join].iter().any(|row| row[0].contains("ExchangeSender")
            && row[4].contains("ExchangeType: HashPartition")
            && row[4].contains("Hash Cols: [name: test.t1.a")),
        "{plan:#?}"
    );
}
