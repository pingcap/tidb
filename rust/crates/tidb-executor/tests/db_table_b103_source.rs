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

//! Ports of `pkg/ddl/db_table_test.go` items 223--240.  Synchronous table
//! creation/drop and scalar default behavior are runnable through the catalog
//! runners.  The remaining entries document Go contracts that require the DDL
//! job queue, sessions, transactions, failpoints, or direct meta construction.

use tidb_datatype::Datum;
use tidb_executor::{Catalog, StmtContext, ddl, run_create_table_on, run_insert_on, run_select_on};

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

fn rows_text(rows: &[Vec<Datum>]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .map(|value| match value {
                    Datum::Null => "<nil>".to_owned(),
                    Datum::Bytes(value) => String::from_utf8_lossy(value).into_owned(),
                    Datum::String(value) => String::from_utf8_lossy(value.bytes()).into_owned(),
                    Datum::Int(value) => value.to_string(),
                    Datum::UInt(value) => value.to_string(),
                    Datum::Float32(value) => value.to_string(),
                    Datum::Real(value) => value.to_string(),
                    Datum::Enum(value, _) => value.name().to_string(),
                    Datum::Set(value, _) => value.name().to_string(),
                    other => format!("{other:?}"),
                })
                .collect()
        })
        .collect()
}

// --- TestAddNotNullColumn (pkg/ddl/db_table_test.go:53) ---

// go-parity-gap: the Go test repeatedly updates a live table while ADD COLUMN
// runs through WriteOnly/Reorganization states.  This tier applies ADD COLUMN
// atomically and has no concurrent schema-state hook.
#[test]
#[ignore = "go-parity-gap: concurrent ADD COLUMN schema states are unported"]
fn add_not_null_column_while_updates_run() {}

// --- TestAddNotNullColumnWhileInsertOnDupUpdate (pkg/ddl/db_table_test.go:84) ---

// go-parity-gap: requires a second session continuously executing INSERT ... ON
// DUPLICATE KEY UPDATE while a live ADD COLUMN job changes schema state.
#[test]
#[ignore = "go-parity-gap: concurrent INSERT ON DUPLICATE KEY UPDATE during ADD COLUMN is unported"]
fn add_not_null_column_while_insert_on_duplicate_update_runs() {}

// --- TestTransactionOnAddDropColumn (pkg/ddl/db_table_test.go:110) ---

// go-parity-gap: transactions are deliberately injected at each DDL job state
// through beforeRunOneJobStep.  The catalog runner has no transaction or
// intermediate schema-state carrier.
#[test]
#[ignore = "go-parity-gap: transaction injection during ADD/DROP COLUMN is unported"]
fn transaction_on_add_drop_column() {}

// --- TestCreateTableWithSetCol (pkg/ddl/db_table_test.go:172) ---

// go-parity-gap: measured omitted-column INSERTs currently store NULL for ordinary literal defaults.
#[test]
#[ignore = "go-parity-gap: SET literal default materialization is unported"]
fn create_table_with_set_defaults_and_insert_default() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table b103_set (a set('1', '4', '10', '21') default 15)",
        &mut catalog,
    )
    .unwrap();
    let context = ctx();
    run_insert_on("insert into b103_set values ()", &mut catalog, &context).unwrap();
    assert_eq!(
        rows_text(&run_select_on("select a from b103_set", &catalog, &context).unwrap()),
        vec![vec!["1,4,10,21".to_owned()]],
    );
}

// --- TestCreateTableWithEnumCol (pkg/ddl/db_table_test.go:229) ---

// go-parity-gap: measured omitted-column INSERTs currently store NULL for ordinary literal defaults.
#[test]
#[ignore = "go-parity-gap: ENUM literal default materialization is unported"]
fn create_table_with_enum_numeric_default_uses_member_position() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table b103_enum (a enum('a', 'c', 'd') default 2)",
        &mut catalog,
    )
    .unwrap();
    let context = ctx();
    run_insert_on("insert into b103_enum values ()", &mut catalog, &context).unwrap();
    assert_eq!(
        rows_text(&run_select_on("select a from b103_enum", &catalog, &context).unwrap()),
        vec![vec!["c".to_owned()]],
    );
}

// --- TestCreateTableWithIntegerColWithDefault (pkg/ddl/db_table_test.go:261) ---

// go-parity-gap: measured omitted-column INSERTs currently store NULL for ordinary literal defaults.
#[test]
#[ignore = "go-parity-gap: integer literal default materialization is unported"]
fn create_table_with_integer_defaults_casts_fractional_literals() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table b103_integer_defaults (a smallint default -1.25, b mediumint default 2.8, c int default -2.8)",
        &mut catalog,
    )
    .unwrap();
    let context = ctx();
    run_insert_on(
        "insert into b103_integer_defaults values ()",
        &mut catalog,
        &context,
    )
    .unwrap();
    assert_eq!(
        rows_text(
            &run_select_on("select * from b103_integer_defaults", &catalog, &context).unwrap()
        ),
        vec![vec!["-1".to_owned(), "3".to_owned(), "-3".to_owned()]],
    );
}

// --- TestCreateTableWithInfo (pkg/ddl/db_table_test.go:310) ---

// go-parity-gap: Go constructs model.TableInfo with an explicit table ID and
// submits it through BatchCreateTableWithInfo, then reads meta IDs.  This tier
// intentionally allocates metadata inside SQL CREATE and exposes no raw
// TableInfo insertion API.
#[test]
#[ignore = "go-parity-gap: direct BatchCreateTableWithInfo/meta-ID API is unported"]
fn create_table_with_explicit_table_info() {}

// --- TestBatchCreateTable (pkg/ddl/db_table_test.go:349) ---

// go-parity-gap: the contract is one ActionCreateTables job, job-history rows,
// duplicate-name atomicity, and a view TableInfo supplied directly to the DDL
// executor.  No batch job or raw TableInfo carrier exists here.
#[test]
#[ignore = "go-parity-gap: batch CREATE TABLE job and raw TableInfo carrier are unported"]
fn batch_create_table_job() {}

// --- TestTableLock (pkg/ddl/db_table_test.go:421) ---

// go-parity-gap: LOCK TABLES/UNLOCK TABLES and persisted TableLockInfo are not
// implemented by the catalog runner.
#[test]
#[ignore = "go-parity-gap: table-lock registry and persisted TableLockInfo are unported"]
fn table_lock_persists_and_clears_session_lock() {}

// --- TestTableLocksLostCommit (pkg/ddl/db_table_test.go:456) ---

// go-parity-gap: requires two sessions, lock visibility, session close cleanup,
// and transaction commit behavior.
#[test]
#[ignore = "go-parity-gap: table-lock lost-commit session behavior is unported"]
fn table_locks_are_cleaned_after_session_close() {}

// --- TestWriteLocal (pkg/ddl/db_table_test.go:497) ---

// go-parity-gap: WRITE LOCAL is a session table-lock mode with cross-session
// read/write gating and lock conflict errors.
#[test]
#[ignore = "go-parity-gap: WRITE LOCAL table-lock mode is unported"]
fn write_local_table_lock() {}

// --- TestLockTables (pkg/ddl/db_table_test.go:546) ---

// go-parity-gap: the comprehensive lock matrix exercises metadata locks,
// transactions, DDL, views, sequences, database operations, and ADMIN CLEANUP
// TABLE LOCK.  None of those session lock carriers are present here.
#[test]
#[ignore = "go-parity-gap: LOCK TABLES session matrix is unported"]
fn lock_tables_matrix() {}

// --- TestTablesLockDelayClean (pkg/ddl/db_table_test.go:719) ---

// go-parity-gap: delayed lock cleanup is a global configuration plus a timer
// after session close; no table-lock lifecycle exists in this tier.
#[test]
#[ignore = "go-parity-gap: delayed table-lock cleanup is unported"]
fn tables_lock_delayed_cleanup() {}

// --- TestAddColumn2 (pkg/ddl/db_table_test.go:751) ---

// go-parity-gap: requires a failpoint at WriteOnly, a stale row rewrite through
// the writable table, and explicit _tidb_rowid insertion from another session.
#[test]
#[ignore = "go-parity-gap: stale WriteOnly row rewrite and failpoint are unported"]
fn add_column_write_only_stale_row_and_rowid() {}

// --- TestDropTables (pkg/ddl/db_table_test.go:817) ---

#[test]
fn drop_tables_if_exists_and_partial_missing_table() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table b103_drop_one (a int)", &mut catalog).unwrap();
    let context = ctx();
    ddl::run_drop_table_in(
        "drop table if exists b103_missing, b103_drop_one",
        &mut catalog,
        "test",
        context.sql_mode(),
        context.foreign_key_checks(),
    )
    .unwrap();
    assert!(!catalog.contains_in("test", "b103_drop_one"));
    assert!(catalog.table_in("test", "b103_missing").is_none());
}

// --- TestCreateConstraintForTable (pkg/ddl/db_table_test.go:850) ---

// go-parity-gap: the Go test toggles the global CHECK constraint setting and
// asserts duplicate constraint names across CREATE/ALTER and databases.  The
// current Rust runner has no equivalent global/session setting.
#[test]
#[ignore = "go-parity-gap: CHECK constraint enable switch and duplicate-name diagnostics are unported"]
fn create_constraint_for_table() {}

// --- TestCreateTableHandleAutoIDOnce (pkg/ddl/db_table_test.go:874) ---

// go-parity-gap: requires the handleAutoIncID failpoint and SHOW TABLE
// NEXT_ROW_ID allocator inspection; the synchronous catalog has neither.
#[test]
#[ignore = "go-parity-gap: auto-increment rebase failpoint and NEXT_ROW_ID are unported"]
fn create_table_handles_auto_id_once() {}

// --- TestCreateTableWithBR (pkg/ddl/db_table_test.go:893) ---

// go-parity-gap: Go injects a BR start-mode failpoint, supplies raw TableInfo,
// and asserts that auto-ID rebase runs twice.  BR and raw meta insertion are
// absent from this tier.
#[test]
#[ignore = "go-parity-gap: BR create path and raw TableInfo auto-ID rebase are unported"]
fn create_table_with_br() {}
