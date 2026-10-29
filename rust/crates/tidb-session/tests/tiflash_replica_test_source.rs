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

//! Executable TiFlash metadata-copy and truncate row regressions.
//!
//! Eight ignored empty shells formerly represented upstream integration
//! obligations without executing any assertions. Their original source notes
//! remain in `rust/docs/parity/current-audit/tiflash-unverified-test-obligations.json`.
//! Live replica metadata and polling exist in tidb-exec and are tested there;
//! broader durable DDL, mock/multi-node and schema-validation obligations are
//! still unaccepted. These two tests do not certify the upstream DDL package.

use crate::support::testkit::{enable_failpoint, TestKit};
use tidb_executor::TableEntry;

/// Go `external.GetTableByName(t, tk, db, table).Meta()`: the stored table.
fn kv_table(tk: &TestKit, table: &str) -> std::sync::Arc<tidb_table::kv_table::KvTable> {
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
    let _store_count = enable_failpoint("mockTiFlashStoreCount", "return(true)");
    let mut tk = TestKit::new();
    tk.must_exec("drop table if exists t1,t2;");
    tk.must_exec("create table t1 (a int) partition by hash(a) partitions 2;");
    tk.must_exec("alter table t1 set tiflash replica 3 location labels 'a','b';");
    let t1 = kv_table(&tk, "t1");
    // Mock for all partitions replica was available.
    let partition_ids = t1
        .partition()
        .expect("source table is partitioned")
        .definitions
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
    let _store_count = enable_failpoint("mockTiFlashStoreCount", "return(true)");
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
