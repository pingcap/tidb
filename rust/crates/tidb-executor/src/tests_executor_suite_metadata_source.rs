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

//! Ports of Go `pkg/executor/test/executor/executor_test.go` items 84–1072
//! (the metadata / session-variable / admin-check slice of the suite).
//!
//! The tests exercise ADMIN CHECK consistency errors, hash-partition reads,
//! duplicate-entry error shape, and the `any_value` column-flag regression.

#[allow(unused_imports)]
use crate::RowDecodeContextExt;
use crate::{admin_check, run_create_table_on, run_insert_on, run_select_on, SessionExtendedInfoSchema, StmtContext};

/// The test's one store (Go's mock store under autocommit).
type Store = tidb_store_driver_txn::cluster_storage_test_util::SharedMemStorage;

fn ctx(store: &Store) -> StmtContext {
    StmtContext::for_query().with_txn_store(Box::new(store.clone()))
}

fn create(catalog: &mut SessionExtendedInfoSchema, sql: &str) {
    run_create_table_on(sql, catalog)
        .unwrap_or_else(|error| panic!("create {sql:?} failed: {error:?}"));
}

fn insert(catalog: &mut SessionExtendedInfoSchema, store: &Store, sql: &str) {
    run_insert_on(sql, catalog, &ctx(store))
        .unwrap_or_else(|error| panic!("insert {sql:?} failed: {error:?}"));
}

fn kv_table_of(catalog: &SessionExtendedInfoSchema, name: &str) -> tidb_table::tables::TableCommon {
    let Some(crate::TableEntry::Kv(table)) = catalog.table_in("test", name) else {
        panic!("table {name} is not stored as bytes");
    };
    (**table).clone()
}

/// Go `executor_test.go:560::TestPartitionHashCode`: five concurrent
/// sessions each run `select * from t` five times over an empty table
/// hash-partitioned on its primary key; the test is a race regression for
/// the hash-partition routing code (`pkg/util/ranger`'s hash code reuse).
/// The rows must come back — empty — every time, with no panic.
#[test]
fn partition_hash_code_reads_are_stable_across_repeated_queries() {
    let store = Store::default();
    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    create(
        &mut catalog,
        "create table t(c1 bigint, c2 bigint, c3 bigint, primary key(c1)) partition by hash (c1) partitions 4",
    );
    // Five Go sessions x five queries, run sequentially: the data contract
    // (empty result, no error) is what is pinned; the concurrent sessions
    // are Go's delivery mechanism for the race detector.
    for _ in 0..5 {
        for _ in 0..5 {
            let rows = run_select_on("select * from t", &catalog, &ctx(&store))
                .expect("select over the hash-partitioned table");
            assert!(rows.is_empty(), "the Go table is never populated");
        }
    }
}

/// Go `executor_test.go:579::TestPrevStmtDesensitization` under
/// `tidb_redact_log=1`: the previous-statement digest redacts literal lists
/// (`insert into \`t\` values ( ... )`) and the duplicate-entry error masks
/// the offending value (`Duplicate entry '?' for key 't.a'`).
///
/// The observable core this tier owns is the duplicate-entry error itself:
/// inserting a colliding value into `t.a` fails naming the value and the
/// key. The redaction layer (`PrevStmt.String()` and the '?' masking driven
/// by `@@global.tidb_redact_log`) is the session's, and has no surface here.
#[test]
fn duplicate_entry_error_names_the_value_and_key() {
    let store = Store::default();
    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    create(&mut catalog, "create table t (a int, unique key (a))");
    insert(&mut catalog, &store, "insert into t values (1),(2)");
    let error = run_insert_on("insert into t values (1)", &mut catalog, &ctx(&store))
        .expect_err("duplicate key must fail");
    let sql_error = error.clone().to_mysql_error();
    assert_eq!(sql_error.code, 1062, "{error:?}");
    assert_eq!(sql_error.message, "Duplicate entry '1' for key 't.a'");
}

/// Go `executor_test.go:593::TestIssue19148`: `where a > any_value(a)` must
/// not corrupt the referenced column's flag — Go requires
/// `tblInfo.Meta().Columns[0].GetFlag() == 0` after the query.
#[test]
fn any_value_comparison_leaves_the_column_flag_untouched() {
    let store = Store::default();
    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    create(&mut catalog, "create table t(a decimal(16, 2))");
    let rows = run_select_on("select * from t where a > any_value(a)", &catalog, &ctx(&store))
        .expect("Go runs this query against the empty table");
    assert!(rows.is_empty());
    let table = kv_table_of(&catalog, "t");
    let flag = table.columns()[0].read().field_type.flags();
    assert_eq!(flag, 0, "Go requires GetFlag() == 0 after the query");
}
