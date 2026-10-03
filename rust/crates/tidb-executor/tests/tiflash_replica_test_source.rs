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

use tidb_datatype::Datum;
use tidb_executor::{
    run_create_table_on, run_insert_on, run_select_on, run_truncate_table_in, Catalog,
    StmtContext,
};

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

fn int_rows(catalog: &Catalog, sql: &str) -> Vec<Vec<String>> {
    run_select_on(sql, catalog, &ctx())
        .expect("select succeeds")
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|datum| match &datum {
                    Datum::Int(value) => value.to_string(),
                    other => panic!("unexpected datum {other:?}"),
                })
                .collect()
        })
        .collect()
}

/// Go `tiflash_replica_test.go:487-501::TestTruncateTable2`, rows contract:
/// after inserting (1,1),(2,2) and truncating, `insert (3,3),(4,4)` and
/// `select *` answer exactly `3 3` / `4 4` — the truncate emptied the rows
/// and the table stays live for writes.
#[test]
fn truncate_table_empties_the_rows_and_the_table_stays_live() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table truncate_table (c1 int, c2 int)", &mut catalog)
        .expect("create succeeds");
    run_insert_on(
        "insert into truncate_table values (1, 1), (2, 2)",
        &mut catalog,
        &ctx(),
    )
    .expect("insert succeeds");

    run_truncate_table_in(
        "truncate table truncate_table",
        &mut catalog,
        "test",
        ctx().sql_mode(),
    )
    .expect("truncate succeeds");

    run_insert_on(
        "insert into truncate_table values (3, 3), (4, 4)",
        &mut catalog,
        &ctx(),
    )
    .expect("post-truncate insert succeeds");
    assert_eq!(
        int_rows(&catalog, "select * from truncate_table"),
        vec![vec!["3", "3"], vec!["4", "4"]],
        "Go :498-499: only the post-truncate rows are there"
    );
}

/// Go `tiflash_replica_test.go:437-475::TestCreateTableWithLike2`, the TiFlash
/// arms. After both partitions of a hash-partitioned `t1` are mocked
/// available (`UpdateTableReplicaInfo`), `create table t2 like t1` copies
/// `TiFlashReplica.Count` and `LocationLabels` but lands with
/// `Available=false` and empty `AvailablePartitionIDs` (Go
/// `BuildTableInfoWithLike`, `pkg/ddl/create_table.go:1281-1289` keeps the
/// settings and strips the availability), while `t1` itself stays available
/// with both partition ids.
#[test]
fn create_table_like_copies_tiflash_replica_settings_clearing_availability() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t1 (a int) partition by hash(a) partitions 2",
        &mut catalog,
    )
    .expect("source table is created");
    let partition_ids = match catalog.table_in("test", "t1") {
        Some(tidb_executor::TableEntry::Kv(table)) => table
            .partition()
            .expect("source table is partitioned")
            .definitions
            .iter()
            .map(|definition| definition.id)
            .collect::<Vec<_>>(),
        _ => panic!("t1 is a stored table"),
    };
    match catalog
        .table_mut_in("test", "t1")
        .expect("source table exists")
    {
        tidb_executor::TableEntry::Kv(table) => {
            std::sync::Arc::make_mut(table).set_tiflash_replica(Some(tidb_model::TiFlashReplicaInfo {
                count: 2,
                location_labels: vec!["zone".to_owned()].into(),
                available: true,
                available_partition_ids: partition_ids.clone().into(),
            }));
        }
        _ => panic!("t1 is a stored table"),
    }

    run_create_table_on("create table t2 like t1", &mut catalog)
        .expect("CREATE TABLE LIKE succeeds");

    let source = match catalog.table_in("test", "t1") {
        Some(tidb_executor::TableEntry::Kv(table)) => table,
        _ => panic!("t1 is a stored table"),
    };
    assert_eq!(
        source.tiflash_replica(),
        Some(&tidb_model::TiFlashReplicaInfo {
            count: 2,
            location_labels: vec!["zone".to_owned()].into(),
            available: true,
            available_partition_ids: partition_ids.into(),
        })
    );
    let copy = match catalog.table_in("test", "t2") {
        Some(tidb_executor::TableEntry::Kv(table)) => table,
        _ => panic!("t2 is a stored table"),
    };
    assert_eq!(
        copy.tiflash_replica(),
        Some(&tidb_model::TiFlashReplicaInfo {
            count: 2,
            location_labels: vec!["zone".to_owned()].into(),
            available: false,
            available_partition_ids: Default::default(),
        })
    );
}
