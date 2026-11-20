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

//! Go master `pkg/executor/test/writetest/mview_log_write_basic_test.go` and
//! `mview_log_write_test.go`: a base table's DML as its materialized view
//! log records it.

use super::*;

/// Go `testkit.Rows` check of the query's rows, each row's cells joined by
/// spaces.
fn go_check(tk: &mut Session, sql: &str) -> Vec<String> {
    super::refresh::go_check(tk, sql)
}

/// Go `MustQuery(sql).Sort().Check(...)`.
fn go_check_sorted(tk: &mut Session, sql: &str) -> Vec<String> {
    let mut rows = go_check(tk, sql);
    rows.sort();
    rows
}

/// Go `execAsMViewMaintenance`: the statement as materialized view
/// maintenance's restricted SQL.
fn exec_as_mview_maintenance(tk: &mut Session, sql: &str) {
    assert_eq!(exec_refresh_internal(tk, sql), "OK", "{sql}");
}

/// Go `tk.MustGetErrCode`.
fn must_get_err_code(tk: &mut Session, sql: &str, code: u16) {
    let answered = answer(tk, sql);
    assert!(answered.starts_with(&format!("ERR {code} ")), "{sql}: {answered}");
}

/// Go `require.ErrorContains(t, tk.ExecToErr(sql), expected)`: the
/// failure's `Error()` text.
fn error_contains(tk: &mut Session, sql: &str, expected: &str) {
    let error = exec_to_err(tk, sql);
    assert!(error.contains(expected), "{sql}: {error}");
}

/// Go `setLoadDataReader(tk, data)` then `tk.MustExec(sql)`: the LOAD DATA
/// LOCAL statement over the client's bytes, as the connection runs it.
fn load_data_local(tk: &mut Session, data: &str, sql: &str) {
    let stmt = tk.parse(sql).unwrap();
    // The connection accepts the LOCAL load, then runs it over the bytes.
    tk.node_conn().accept_local_infile_parsed(sql, &stmt).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    if let Err(error) = tk.node_conn().execute_local_infile_parsed(sql, &stmt, data.as_bytes()) {
        panic!("{sql}: {error:?}");
    }
}

/// A connection id for another test kit on the stack.
fn next_conn_id() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(4100);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}
/// Go `tk.MustQuery(sql).Rows()[0][column].(string)`.
fn cell(tk: &mut Session, sql: &str, column: usize) -> String {
    super::refresh::rows_of(tk, sql)[0][column].clone().expect("a non-NULL cell")
}

#[test]
fn test_m_log_insert() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int, c int)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (a, b, c)"]);
    // Single-row insert.
    must_exec_all(&mut tk, &["insert into t values (1, 10, 100)"]);
    assert_eq!(go_check(&mut tk, "select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 10 100 I 1"]);
    // Multi-row insert.
    exec_as_mview_maintenance(&mut tk, "delete from `$mlog$t`");
    must_exec_all(&mut tk, &["insert into t values (2,20,200), (3,30,300), (4,40,400)"]);
    assert_eq!(go_check(&mut tk, "select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t` order by a"), vec!["2 20 200 I 1", "3 30 300 I 1", "4 40 400 I 1"]);
    // Partial-column insert with DEFAULT value.
    must_exec_all(&mut tk, &["drop materialized view log on t"]);
    must_exec_all(&mut tk, &["drop table if exists t"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int, c int default 99)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (a, b, c)"]);
    must_exec_all(&mut tk, &["insert into t (a, b) values (5, 50)"]);
    assert_eq!(go_check(&mut tk, "select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["5 50 99 I 1"]);
}

#[test]
fn test_m_log_update() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int unique, c int)"]);
    must_exec_all(&mut tk, &["insert into t values (1,10,100), (2,20,200), (3,30,300)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (a, b, c)"]);
    // Single-row update.
    must_exec_all(&mut tk, &["update t set c=101 where a=1"]);
    assert_eq!(go_check_sorted(&mut tk, "select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 10 100 U -1", "1 10 101 U 1"]);
    // Multi-row update.
    exec_as_mview_maintenance(&mut tk, "delete from `$mlog$t`");
    must_exec_all(&mut tk, &["update t set c = c + 1 where a in (2, 3)"]);
    assert_eq!(go_check_sorted(&mut tk, "select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["2 20 200 U -1", "2 20 201 U 1", "3 30 300 U -1", "3 30 301 U 1"]);
}

#[test]
fn test_m_log_delete() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int unique, c int)"]);
    must_exec_all(&mut tk, &["insert into t values (1,10,100), (2,20,200), (3,30,300)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (a, b, c)"]);
    // Single-row delete.
    must_exec_all(&mut tk, &["delete from t where a=1"]);
    assert_eq!(go_check(&mut tk, "select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 10 100 D -1"]);
    // Multi-row delete.
    exec_as_mview_maintenance(&mut tk, "delete from `$mlog$t`");
    must_exec_all(&mut tk, &["delete from t where a in (2, 3)"]);
    assert_eq!(go_check_sorted(&mut tk, "select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["2 20 200 D -1", "3 30 300 D -1"]);
}

#[test]
fn test_m_log_update_pk() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int)"]);
    must_exec_all(&mut tk, &["insert into t values (1, 100)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (a, b)"]);
    // Updating the primary key triggers the handle-changed path:
    // RemoveRecord(old) + AddRecord(new, IsUpdate).
    must_exec_all(&mut tk, &["update t set a = 2 where a = 1"]);
    assert_eq!(go_check(&mut tk, "select a, b from t order by a"), vec!["2 100"]);
    assert_eq!(go_check_sorted(&mut tk, "select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 100 U -1", "2 100 U 1"]);
}

#[test]
fn test_m_log_replace_identical_row() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int)"]);
    must_exec_all(&mut tk, &["insert into t values (1, 100)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (a, b)"]);
    // REPLACE with an identical row: executor skips RemoveRecord + AddRecord.
    must_exec_all(&mut tk, &["replace into t values (1, 100)"]);
    // Mlog should be empty because the base table was not mutated.
    assert_eq!(go_check(&mut tk, "select * from `$mlog$t`"), Vec::<&str>::new());
}

#[test]
fn test_m_log_replace_pk_conflict() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int)"]);
    must_exec_all(&mut tk, &["insert into t values (1, 100)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (a, b)"]);
    // PK-only conflict: old row removed, new row added -> U -1, U 1.
    must_exec_all(&mut tk, &["replace into t values (1, 200)"]);
    assert_eq!(go_check(&mut tk, "select a, b from t order by a"), vec!["1 200"]);
    assert_eq!(go_check_sorted(&mut tk, "select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 100 U -1", "1 200 U 1"]);
}

#[test]
fn test_m_log_replace_uk_conflict() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int unique, c int)"]);
    must_exec_all(&mut tk, &["insert into t values (1, 10, 100)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (a, b, c)"]);
    // UK-only conflict (no PK conflict): old row (1,10,100) removed, new row (99,10,200) added.
    must_exec_all(&mut tk, &["replace into t values (99, 10, 200)"]);
    assert_eq!(go_check(&mut tk, "select a, b, c from t order by a"), vec!["99 10 200"]);
    assert_eq!(go_check_sorted(&mut tk, "select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 10 100 U -1", "99 10 200 U 1"]);
}

#[test]
fn test_m_log_replace_no_conflict() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (a, b)"]);
    // Multi-row REPLACE with no conflicts -> all logged as I 1.
    must_exec_all(&mut tk, &["replace into t values (1, 10), (2, 20)"]);
    assert_eq!(go_check(&mut tk, "select a, b from t order by a"), vec!["1 10", "2 20"]);
    assert_eq!(go_check(&mut tk, "select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t` order by a"), vec!["1 10 I 1", "2 20 I 1"]);
}

#[test]
fn test_m_log_replace_pk_and_uk_conflict() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int unique, c int)"]);
    // Seed rows before creating mlog so that seed inserts won't be logged.
    must_exec_all(&mut tk, &["insert into t values (1,10,100), (2,20,200)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (a, b, c)"]);
    // The first row conflicts with (1,10,100) on PK and with (2,20,200) on unique index.
    // The second row is new and should be inserted as is, even though its primary key value
    // conflicts with the old row that the first row removes.
    must_exec_all(&mut tk, &["replace into t values (1,20,999), (2,30,100)"]);
    assert_eq!(go_check(&mut tk, "select a, b, c from t order by a"), vec!["1 20 999", "2 30 100"]);
    assert_eq!(go_check_sorted(&mut tk, "select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 10 100 U -1", "1 20 999 U 1", "2 20 200 U -1", "2 30 100 I 1"]);
}

#[test]
fn test_m_log_insert_ignore() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int unique, c int)"]);
    must_exec_all(&mut tk, &["insert into t values (1,10,100)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (a, b, c)"]);
    // (1,11,111) conflicts on PK, (2,10,222) conflicts on unique index, only the last is inserted.
    must_exec_all(&mut tk, &["insert ignore into t values (1,11,111), (2,10,222), (3,30,333)"]);
    assert_eq!(go_check(&mut tk, "select a, b, c from t order by a"), vec!["1 10 100", "3 30 333"]);
    assert_eq!(go_check(&mut tk, "select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["3 30 333 I 1"]);
}

#[test]
fn test_m_log_iodku() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int unique, c int)"]);
    must_exec_all(&mut tk, &["insert into t values (1,10,100)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (a, b, c)"]);
    must_exec_all(&mut tk, &["insert into t values (1,10,101) on duplicate key update c=values(c)"]);
    assert_eq!(go_check(&mut tk, "select a, b, c from t order by a"), vec!["1 10 101"]);
    assert_eq!(go_check_sorted(&mut tk, "select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 10 100 U -1", "1 10 101 U 1"]);
}

#[test]
fn test_m_log_iodku_change_pk() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int unique, c int)"]);
    must_exec_all(&mut tk, &["insert into t values (1, 10, 100)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (a, b, c)"]);
    // IODKU that changes the primary key triggers the handle-changed path:
    // the old row is removed and the new row is added, both logged as U (update).
    must_exec_all(&mut tk, &["insert into t values (1, 10, 200) on duplicate key update a = 3, c = values(c)"]);
    assert_eq!(go_check(&mut tk, "select a, b, c from t order by a"), vec!["3 10 200"]);
    assert_eq!(go_check_sorted(&mut tk, "select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 10 100 U -1", "3 10 200 U 1"]);
}

#[test]
fn test_m_log_multi_row_iodku() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int)"]);
    must_exec_all(&mut tk, &["insert into t values (1,10), (2,20)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (a, b)"]);
    // Row (1,...) conflicts on PK -> update; row (3,...) is new -> insert.
    must_exec_all(&mut tk, &["insert into t values (1,11), (3,30) on duplicate key update b=values(b)"]);
    assert_eq!(go_check(&mut tk, "select a, b from t order by a"), vec!["1 11", "2 20", "3 30"]);
    assert_eq!(go_check_sorted(&mut tk, "select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 10 U -1", "1 11 U 1", "3 30 I 1"]);
}

#[test]
fn test_m_log_iodku_no_op() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int)"]);
    must_exec_all(&mut tk, &["insert into t values (1, 10)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (a, b)"]);
    // IODKU hits a duplicate key but the update is a no-op (b = b, no column touched).
    // Because no tracked column actually changes, mlog should be empty.
    must_exec_all(&mut tk, &["insert into t values (1, 10) on duplicate key update b = b"]);
    assert_eq!(go_check(&mut tk, "select a, b from t"), vec!["1 10"]);
    assert_eq!(go_check(&mut tk, "select * from `$mlog$t`"), Vec::<&str>::new());
}

#[test]
fn test_m_log_iodkupk_and_uk_conflict_diff_rows() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int unique, c int)"]);
    // Row A: a=1, b=10; Row B: a=2, b=20.
    must_exec_all(&mut tk, &["insert into t values (1,10,100), (2,20,200)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (a, b, c)"]);
    // Insert (1,20,999): PK conflicts with row A (a=1), UK conflicts with row B (b=20).
    // Unlike REPLACE (which deletes conflicting rows first), IODKU finds the PK conflict
    // and tries to update that row, but the update itself violates the UK constraint on
    // another row.
    must_get_err_code(&mut tk, "insert into t values (1,20,999) on duplicate key update b=values(b), c=values(c)", 1062);
    // Base table unchanged.
    assert_eq!(go_check(&mut tk, "select a, b, c from t order by a"), vec!["1 10 100", "2 20 200"]);
    // Mlog should be empty because the statement errored out.
    assert_eq!(go_check(&mut tk, "select * from `$mlog$t`"), Vec::<&str>::new());
}

#[test]
fn test_m_log_insert_generated_column() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t_gen (id bigint primary key,base int not null,gv int as (base + 1) virtual,gs int as (base + 2) stored)"]);
    must_exec_all(&mut tk, &["create materialized view log on t_gen (id, gv, gs)"]);
    must_exec_all(&mut tk, &["insert into t_gen(id, base) values (1, 10), (2, 20)"]);
    assert_eq!(go_check(&mut tk, "select id, gv, gs, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t_gen` order by id"), vec!["1 11 12 I 1", "2 21 22 I 1"]);
}

#[test]
fn test_m_log_insert_select() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table src (a int primary key, b int, c int)"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int, c int)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (a, b, c)"]);
    // (a) Basic INSERT ... SELECT with no conflicts — all rows logged as I.
    must_exec_all(&mut tk, &["insert into src values (1,10,100), (2,20,200)"]);
    must_exec_all(&mut tk, &["insert into t select * from src"]);
    assert_eq!(go_check(&mut tk, "select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t` order by a"), vec!["1 10 100 I 1", "2 20 200 I 1"]);
    // (b) INSERT IGNORE ... SELECT — conflicting rows are skipped.
    exec_as_mview_maintenance(&mut tk, "delete from `$mlog$t`");
    must_exec_all(&mut tk, &["delete from src"]);
    must_exec_all(&mut tk, &["insert into src values (1,11,111), (3,30,300)"]);
    must_exec_all(&mut tk, &["insert ignore into t select * from src"]);
    assert_eq!(go_check(&mut tk, "select a, b, c from t order by a"), vec!["1 10 100", "2 20 200", "3 30 300"]);
    assert_eq!(go_check(&mut tk, "select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["3 30 300 I 1"]);
    // (c) INSERT ... SELECT ... ON DUPLICATE KEY UPDATE — conflicting row updated, new row inserted.
    exec_as_mview_maintenance(&mut tk, "delete from `$mlog$t`");
    must_exec_all(&mut tk, &["delete from src"]);
    must_exec_all(&mut tk, &["insert into src values (1,11,111), (4,40,400)"]);
    must_exec_all(&mut tk, &["insert into t select * from src on duplicate key update b=values(b), c=values(c)"]);
    assert_eq!(go_check(&mut tk, "select a, b, c from t order by a"), vec!["1 11 111", "2 20 200", "3 30 300", "4 40 400"]);
    assert_eq!(go_check_sorted(&mut tk, "select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 10 100 U -1", "1 11 111 U 1", "4 40 400 I 1"]);
}

#[test]
fn test_m_log_replace_select() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table src (a int primary key, b int)"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int)"]);
    must_exec_all(&mut tk, &["insert into t values (1, 10)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (a, b)"]);
    // REPLACE ... SELECT: row (1,...) conflicts on PK → U; row (2,...) is new → I.
    must_exec_all(&mut tk, &["insert into src values (1, 11), (2, 20)"]);
    must_exec_all(&mut tk, &["replace into t select * from src"]);
    assert_eq!(go_check(&mut tk, "select a, b from t order by a"), vec!["1 11", "2 20"]);
    assert_eq!(go_check_sorted(&mut tk, "select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 10 U -1", "1 11 U 1", "2 20 I 1"]);
}

#[test]
fn test_m_log_load_data() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int, c int)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (a, b, c)"]);
    // Basic LOAD DATA with no conflicts — all rows logged as I.
    load_data_local(&mut tk, "1,10,100\n2,20,200\n", "load data local infile '/tmp/nonexistence.csv' into table t fields terminated by ',' (a, b, c)");
    assert_eq!(go_check(&mut tk, "select a, b, c from t order by a"), vec!["1 10 100", "2 20 200"]);
    assert_eq!(go_check(&mut tk, "select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t` order by a"), vec!["1 10 100 I 1", "2 20 200 I 1"]);
}

#[test]
fn test_m_log_load_data_ignore() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int unique, c int)"]);
    // Seed rows before creating mlog so that seed inserts won't be logged.
    must_exec_all(&mut tk, &["insert into t values (1,10,100)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (a, b, c)"]);
    load_data_local(&mut tk, "1,11,111\n2,10,222\n3,30,333\n", "load data local infile '/tmp/nonexistence.csv' ignore into table t fields terminated by ',' (a, b, c)");
    assert_eq!(go_check(&mut tk, "select a, b, c from t order by a"), vec!["1 10 100", "3 30 333"]);
    assert_eq!(go_check(&mut tk, "select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["3 30 333 I 1"]);
}

#[test]
fn test_m_log_load_data_replace_pk_and_uk_conflict() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int unique, c int)"]);
    must_exec_all(&mut tk, &["insert into t values (1,10,100), (2,20,200)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (a, b, c)"]);
    // The first row conflicts with (1,10,100) on PK and with (2,20,200) on unique index.
    // The second row is new and should be inserted as is, even though its primary key value
    // conflicts with the old row that the first row removes.
    load_data_local(&mut tk, "1,20,999\n2,30,100\n", "load data local infile '/tmp/nonexistence.csv' replace into table t fields terminated by ',' (a, b, c)");
    assert_eq!(go_check(&mut tk, "select a, b, c from t order by a"), vec!["1 20 999", "2 30 100"]);
    assert_eq!(go_check_sorted(&mut tk, "select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 10 100 U -1", "1 20 999 U 1", "2 20 200 U -1", "2 30 100 I 1"]);
}

#[test]
fn test_m_log_load_data_replace_conflict_add_failure_no_leak() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["set @@global.tidb_enable_check_constraint = 1"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int unique, c int, constraint chk_c check (c > 0))"]);
    must_exec_all(&mut tk, &["insert into t values (1,10,1), (2,20,1)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (a, b, c)"]);
    // First row removes old rows due to REPLACE conflicts but add fails with check constraint.
    // The second row is a plain insert and must still be marked as I (not leaked U).
    load_data_local(&mut tk, "1,20,-1\n3,30,1\n", "load data local infile '/tmp/nonexistence.csv' replace into table t fields terminated by ',' (a, b, c)");
    assert_eq!(go_check(&mut tk, "select a, b, c from t order by a"), vec!["3 30 1"]);
    assert_eq!(go_check_sorted(&mut tk, "select a, b, c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 10 1 U -1", "2 20 1 U -1", "3 30 1 I 1"]);
}

#[test]
fn test_m_log_multi_table_update() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t1 (a int primary key, b int)"]);
    must_exec_all(&mut tk, &["create table t2 (a int primary key, b int)"]);
    must_exec_all(&mut tk, &["insert into t1 values (1,10)"]);
    must_exec_all(&mut tk, &["insert into t2 values (1,100)"]);
    must_exec_all(&mut tk, &["create materialized view log on t1 (a, b)"]);
    must_exec_all(&mut tk, &["create materialized view log on t2 (a, b)"]);
    must_exec_all(&mut tk, &["update t1, t2 set t1.b=11, t2.b=111 where t1.a=t2.a"]);
    assert_eq!(go_check_sorted(&mut tk, "select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t1`"), vec!["1 10 U -1", "1 11 U 1"]);
    assert_eq!(go_check_sorted(&mut tk, "select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t2`"), vec!["1 100 U -1", "1 111 U 1"]);
}

#[test]
fn test_m_log_multi_table_delete() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t1 (a int primary key, b int, c int)"]);
    must_exec_all(&mut tk, &["create table t2 (a int primary key, b int, c int)"]);
    must_exec_all(&mut tk, &["insert into t1 values (1,10,100), (2,20,200)"]);
    must_exec_all(&mut tk, &["insert into t2 values (1,100,1000), (2,200,2000)"]);
    // Use different tracked columns for two tables to cover per-table mlog mapping in
    // multi-table DELETE, including non-handle tracked columns that used to be pruned.
    must_exec_all(&mut tk, &["create materialized view log on t1 (b)"]);
    must_exec_all(&mut tk, &["create materialized view log on t2 (c)"]);
    must_exec_all(&mut tk, &["delete t1, t2 from t1, t2 where t1.a=t2.a and t1.a=1"]);
    assert_eq!(go_check(&mut tk, "select a, b, c from t1 order by a"), vec!["2 20 200"]);
    assert_eq!(go_check(&mut tk, "select a, b, c from t2 order by a"), vec!["2 200 2000"]);
    assert_eq!(go_check(&mut tk, "select b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t1`"), vec!["10 D -1"]);
    assert_eq!(go_check(&mut tk, "select c, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t2`"), vec!["1000 D -1"]);
}

#[test]
fn test_m_log_skip_untracked_columns() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int, c int)"]);
    must_exec_all(&mut tk, &["insert into t values (1,100,1000)"]);
    // mlog tracks (a, b); c is untracked.
    must_exec_all(&mut tk, &["create materialized view log on t (a, b)"]);
    // Modifying an untracked column should not affect the mlog physical table.
    let show_before = cell(&mut tk, "show create table `$mlog$t`", 1);
    must_exec_all(&mut tk, &["alter table t modify column c bigint"]);
    let show_after = cell(&mut tk, "show create table `$mlog$t`", 1);
    assert_eq!(show_before, show_after);
    // Updating an untracked column should not produce any mlog entry.
    must_exec_all(&mut tk, &["update t set c=2000 where a=1"]);
    assert_eq!(go_check(&mut tk, "select * from `$mlog$t`"), Vec::<&str>::new());
    // Updating a tracked column should produce mlog entries.
    must_exec_all(&mut tk, &["update t set b=101 where a=1"]);
    assert_eq!(go_check_sorted(&mut tk, "select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 100 U -1", "1 101 U 1"]);
}

#[test]
fn test_m_log_partial_columns_mapping() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int, c int, d int)"]);
    // Track columns in a different order to verify mapping by column name.
    must_exec_all(&mut tk, &["create materialized view log on t (d, b)"]);
    must_exec_all(&mut tk, &["insert into t values (1,10,20,30)"]);
    assert_eq!(go_check(&mut tk, "select d, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["30 10 I 1"]);
    exec_as_mview_maintenance(&mut tk, "delete from `$mlog$t`");
    must_exec_all(&mut tk, &["update t set c=21 where a=1"]);
    assert_eq!(go_check(&mut tk, "select * from `$mlog$t`"), Vec::<&str>::new());
    must_exec_all(&mut tk, &["update t set b=11 where a=1"]);
    assert_eq!(go_check_sorted(&mut tk, "select d, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["30 10 U -1", "30 11 U 1"]);
    exec_as_mview_maintenance(&mut tk, "delete from `$mlog$t`");
    must_exec_all(&mut tk, &["update t set d=31 where a=1"]);
    assert_eq!(go_check_sorted(&mut tk, "select d, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["30 11 U -1", "31 11 U 1"]);
}

#[test]
fn test_m_log_tracked_reference_types() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (id int primary key, s varchar(20), txt text, d decimal(10,2), vb varbinary(20))"]);
    must_exec_all(&mut tk, &["create materialized view log on t (s, txt, d, vb)"]);
    must_exec_all(&mut tk, &["insert into t values (1, 'alpha', 'payload1', 12.34, 'bin1')"]);
    assert_eq!(go_check(&mut tk, "select s, txt, d, hex(vb), `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["alpha payload1 12.34 62696E31 I 1"]);
    exec_as_mview_maintenance(&mut tk, "delete from `$mlog$t`");
    must_exec_all(&mut tk, &["update t set s='beta', txt='payload2', d=56.78, vb='bin2' where id=1"]);
    assert_eq!(go_check_sorted(&mut tk, "select s, txt, d, hex(vb), `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["alpha payload1 12.34 62696E31 U -1", "beta payload2 56.78 62696E32 U 1"]);
    // No-reorg modify on a tracked column should also update the mlog physical column type.
    let show_before = cell(&mut tk, "show create table `$mlog$t`", 1);
    assert!(show_before.contains("`s` varchar(20)"), "{}", show_before);
    must_exec_all(&mut tk, &["alter table t modify column s varchar(40)"]);
    let show_after = cell(&mut tk, "show create table `$mlog$t`", 1);
    assert!(show_after.contains("`s` varchar(40)"), "{}", show_after);
    // Existing mlog rows are still readable after the type change.
    assert_eq!(go_check_sorted(&mut tk, "select s, txt, d, hex(vb), `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["alpha payload1 12.34 62696E31 U -1", "beta payload2 56.78 62696E32 U 1"]);
    exec_as_mview_maintenance(&mut tk, "delete from `$mlog$t`");
    must_exec_all(&mut tk, &[r#"update t set s='gamma' where id=1"#]);
    assert_eq!(go_check_sorted(&mut tk, "select s, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["beta U -1", "gamma U 1"]);
    // Reorg-required modify should be rejected for tracked columns on mlog-only base tables.
    error_contains(&mut tk, "alter table t modify column s varchar(5)", "only supports no-reorg compatible type changes for tracked columns");
}

#[test]
fn test_m_log_tracked_null_to_not_null_change_rejected() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (id int primary key, g int not null, c int)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (g, c)"]);
    must_exec_all(&mut tk, &["insert into t values (1, 1, null)"]);
    must_exec_all(&mut tk, &["update t set c = 10 where id = 1"]);
    error_contains(&mut tk, "alter table t modify column c bigint not null", "does not support changing tracked columns from NULL to NOT NULL");
}

#[test]
fn test_m_log_pruned_columns() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    // t.Run("delete")
    {
        must_exec_all(&mut tk, &["create table t_mlog_pruned_delete (a int, b int)"]);
        must_exec_all(&mut tk, &["create materialized view log on t_mlog_pruned_delete (a, b)"]);
        must_exec_all(&mut tk, &["insert into t_mlog_pruned_delete values (1,10)"]);
        exec_as_mview_maintenance(&mut tk, "delete from `$mlog$t_mlog_pruned_delete`");
        // Delete normally can prune non-handle/index columns, but mlog RemoveRecord reads
        // tracked columns by base offsets; pruning them would make mlog writing fail.
        must_exec_all(&mut tk, &["delete from t_mlog_pruned_delete where b=10"]);
        assert_eq!(go_check(&mut tk, "select a, b from t_mlog_pruned_delete"), Vec::<&str>::new());
        assert_eq!(go_check(&mut tk, "select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t_mlog_pruned_delete`"), vec!["1 10 D -1"]);
    }
    // t.Run("update")
    {
        must_exec_all(&mut tk, &["create table t_mlog_pruned_update (a int, b int)"]);
        must_exec_all(&mut tk, &["create materialized view log on t_mlog_pruned_update (a, b)"]);
        must_exec_all(&mut tk, &["insert into t_mlog_pruned_update values (1,10)"]);
        exec_as_mview_maintenance(&mut tk, "delete from `$mlog$t_mlog_pruned_update`");
        // Even if only column b is updated, UpdateRecord still needs full writable row data.
        // If update column pruning drops tracked columns, mlog writing would fail.
        must_exec_all(&mut tk, &["update t_mlog_pruned_update set b=11 where b=10"]);
        assert_eq!(go_check(&mut tk, "select a, b from t_mlog_pruned_update"), vec!["1 11"]);
        assert_eq!(go_check_sorted(&mut tk, "select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t_mlog_pruned_update`"), vec!["1 10 U -1", "1 11 U 1"]);
    }
}

#[test]
fn test_m_log_online_ddl_drop_tracked_column_rejected() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    let mut tk_ddl = super::create::new_mview_test_kit(&_stack, next_conn_id());
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (id int primary key, tracked int, untracked int)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (id, tracked)"]);
    must_exec_all(&mut tk, &["insert into t values (1, 10, 100)"]);
    exec_as_mview_maintenance(&mut tk, "delete from `$mlog$t`");
    must_exec_all(&mut tk_ddl, &["use test"]);
    must_exec_all(&mut tk_ddl, &["set tidb_mview_enable = on"]);
    error_contains(&mut tk_ddl, "alter table t drop column tracked", "Unsupported ALTER TABLE on base table column tracked referenced by materialized view log");
    // The tracked column remains valid after rejected DDL and mlog writing should work.
    must_exec_all(&mut tk, &["insert into t values (2, 20, 200)"]);
    assert_eq!(go_check_sorted(&mut tk, "select id, tracked, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["2 20 I 1"]);
}

#[test]
fn test_m_log_drop_tracked_column_rejected() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (id int primary key, tracked int, untracked int)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (id, tracked)"]);
    error_contains(&mut tk, "alter table t drop column tracked", "Unsupported ALTER TABLE on base table column tracked referenced by materialized view log");
    // DML remains writable and tracked-column change logs are still generated as expected.
    must_exec_all(&mut tk, &["insert into t values (1, 10, 100)"]);
    must_exec_all(&mut tk, &["update t set tracked = 11 where id = 1"]);
    must_exec_all(&mut tk, &["delete from t where id = 1"]);
    assert_eq!(go_check_sorted(&mut tk, "select id, tracked, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 10 I 1", "1 10 U -1", "1 11 D -1", "1 11 U 1"]);
    assert_eq!(go_check(&mut tk, "select count(*) from information_schema.columns where table_schema = 'test' and table_name = 't' and column_name = 'tracked'"), vec!["1"]);
}

#[test]
fn test_m_log_generated_column() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int, c int, d int as (b+c) stored)"]);
    // Track the stored generated column in the mlog.
    must_exec_all(&mut tk, &["create materialized view log on t (a, b, d)"]);
    must_exec_all(&mut tk, &["insert into t (a, b, c) values (1, 10, 20)"]);
    assert_eq!(go_check(&mut tk, "select a, b, d, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 10 30 I 1"]);
    exec_as_mview_maintenance(&mut tk, "delete from `$mlog$t`");
    must_exec_all(&mut tk, &["update t set b=11 where a=1"]);
    assert_eq!(go_check_sorted(&mut tk, "select a, b, d, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 10 30 U -1", "1 11 31 U 1"]);
}

#[test]
fn test_m_log_virtual_generated_column() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int, c int, d int as (b+c) virtual)"]);
    // Track the stored generated column in the mlog.
    must_exec_all(&mut tk, &["create materialized view log on t (a, b, d)"]);
    must_exec_all(&mut tk, &["insert into t (a, b, c) values (1, 10, 20)"]);
    assert_eq!(go_check(&mut tk, "select a, b, d, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 10 30 I 1"]);
    exec_as_mview_maintenance(&mut tk, "delete from `$mlog$t`");
    must_exec_all(&mut tk, &["update t set b=11 where a=1"]);
    assert_eq!(go_check_sorted(&mut tk, "select a, b, d, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 10 30 U -1", "1 11 31 U 1"]);
}

#[test]
fn test_m_log_alter_add_generated_column() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (id int primary key, tracked int)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (id, tracked)"]);
    must_exec_all(&mut tk, &["alter table t add column gv int as (tracked + 1) virtual"]);
    error_contains(&mut tk, "alter table t add column gs int as (tracked + 2) stored", "Adding generated stored column through ALTER TABLE");
    must_exec_all(&mut tk, &["insert into t (id, tracked) values (1, 10)"]);
    assert_eq!(go_check(&mut tk, "select id, tracked, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 10 I 1"]);
    assert_eq!(go_check(&mut tk, "select id, tracked, gv from t"), vec!["1 10 11"]);
    exec_as_mview_maintenance(&mut tk, "delete from `$mlog$t`");
    must_exec_all(&mut tk, &["update t set tracked=20 where id=1"]);
    assert_eq!(go_check_sorted(&mut tk, "select id, tracked, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 10 U -1", "1 20 U 1"]);
    assert_eq!(go_check(&mut tk, "select id, tracked, gv from t"), vec!["1 20 21"]);
}

#[test]
fn test_m_log_alter_generated_column_constraints() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (id int primary key, base int, gv int as (base + 1) virtual, gs int as (base + 2) stored)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (id, gv, gs)"]);
    error_contains(&mut tk, "alter table t rename column base to base2", "generated column dependency");
    error_contains(&mut tk, "alter table t modify column base bigint", "generated column dependency");
    error_contains(&mut tk, "alter table t modify column gv bigint", "Changing the STORED status");
}

#[test]
fn test_m_log_alter_modify_tracked_virtual_generated_column() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (id int primary key, base int, g int as (base + 1) virtual)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (id, g)"]);
    must_exec_all(&mut tk, &["alter table t modify column g int as (base + 2) virtual"]);
    must_exec_all(&mut tk, &["insert into t (id, base) values (1, 10)"]);
    assert_eq!(go_check(&mut tk, "select id, g, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 12 I 1"]);
    exec_as_mview_maintenance(&mut tk, "delete from `$mlog$t`");
    must_exec_all(&mut tk, &["update t set base=20 where id=1"]);
    assert_eq!(go_check_sorted(&mut tk, "select id, g, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 12 U -1", "1 22 U 1"]);
}

#[test]
fn test_m_log_alter_rename_tracked_generated_column_current_behavior() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (id int primary key, base int, g int as (base + 1) virtual)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (id, g)"]);
    error_contains(&mut tk, "alter table t rename column g to g2", "referenced by materialized view log");
}

#[test]
fn test_m_log_auto_increment() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int auto_increment primary key, b int)"]);
    // Track the auto-increment PK in the mlog.
    must_exec_all(&mut tk, &["create materialized view log on t (a, b)"]);
    must_exec_all(&mut tk, &["insert into t (b) values (10), (20)"]);
    assert_eq!(go_check(&mut tk, "select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t` order by a"), vec!["1 10 I 1", "2 20 I 1"]);
    exec_as_mview_maintenance(&mut tk, "delete from `$mlog$t`");
    must_exec_all(&mut tk, &["delete from t where a=1"]);
    assert_eq!(go_check(&mut tk, "select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 10 D -1"]);
    // REPLACE with auto-increment PK conflict.
    exec_as_mview_maintenance(&mut tk, "delete from `$mlog$t`");
    must_exec_all(&mut tk, &["replace into t values (2, 21)"]);
    assert_eq!(go_check_sorted(&mut tk, "select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["2 20 U -1", "2 21 U 1"]);
    // REPLACE with new auto-increment allocation.
    exec_as_mview_maintenance(&mut tk, "delete from `$mlog$t`");
    must_exec_all(&mut tk, &["replace into t (b) values (30)"]);
    assert_eq!(go_check(&mut tk, "select b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["30 I 1"]);
}

#[test]
fn test_m_log_partitioned_table_not_supported() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int, b int) partition by range (a) (partition p0 values less than (10),partition p1 values less than (maxvalue))"]);
    must_get_err_code(&mut tk, "create materialized view log on t (a, b)", 8200);
}

#[test]
fn test_m_log_transaction_rollback() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (a, b)"]);
    // BEGIN; INSERT; verify mlog has rows; ROLLBACK; mlog should be empty.
    must_exec_all(&mut tk, &["begin"]);
    must_exec_all(&mut tk, &["insert into t values (1, 10)"]);
    assert_eq!(go_check(&mut tk, "select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"), vec!["1 10 I 1"]);
    must_exec_all(&mut tk, &["rollback"]);
    assert_eq!(go_check(&mut tk, "select * from t"), Vec::<&str>::new());
    assert_eq!(go_check(&mut tk, "select * from `$mlog$t`"), Vec::<&str>::new());
}

#[test]
fn test_m_log_import_into_not_supported() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test"]);
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t (a int primary key, b int)"]);
    must_exec_all(&mut tk, &["create materialized view log on t (a, b)"]);
    must_get_err_code(&mut tk, "import into t from '/nonexistent.csv'", 1235);
}


/// Go `addColumnStateWriteReorgFailpoint`.
const ADD_COLUMN_STATE_WRITE_REORG_FAILPOINT: &str = "github.com/pingcap/tidb/pkg/ddl/onAddColumnStateWriteReorg";
/// Go `dropColumnStateWriteOnlyFailpoint`.
const DROP_COLUMN_STATE_WRITE_ONLY_FAILPOINT: &str = "github.com/pingcap/tidb/pkg/ddl/onDropColumnStateWriteOnly";

/// Go `ddlCtrl`: a DDL running in the background, held at a failpoint.
struct DdlCtrl {
    paused: Latch,
    resume: Latch,
    ddl: Option<std::thread::JoinHandle<String>>,
    _failpoint: tidb_failpoint::Guard,
}

/// Go `startDDLPausedAtFailpoint`.
fn start_ddl_paused_at_failpoint(mut tk_ddl: Session, failpoint: &str, ddl_sql: &str) -> DdlCtrl {
    let (paused, resume) = (Latch::default(), Latch::default());
    let guard = tidb_failpoint::enable_call_for_test(failpoint, {
        let (paused, resume) = (paused.clone(), resume.clone());
        move |_| {
            paused.open();
            resume.wait();
        }
    });
    let ddl_sql = ddl_sql.to_owned();
    let ddl = std::thread::spawn(move || answer(&mut tk_ddl, &ddl_sql));
    DdlCtrl { paused, resume, ddl: Some(ddl), _failpoint: guard }
}

impl DdlCtrl {
    /// Go `waitUntilPaused`.
    fn wait_until_paused(&self, desc: &str) {
        self.paused.wait_for(&format!("ddl failpoint: {desc}"));
    }

    /// Go `releaseAndWaitFinish`: idempotent.
    fn release_and_wait_finish(&mut self) {
        self.resume.open();
        if let Some(ddl) = self.ddl.take() {
            assert_eq!(ddl.join().unwrap(), "OK");
        }
    }
}

impl Drop for DdlCtrl {
    fn drop(&mut self) {
        self.resume.open();
    }
}

fn ddl_test_kit(stack: &UnistoreClusterStack) -> Session {
    let mut tk_ddl = super::create::new_mview_test_kit(stack, next_conn_id());
    must_exec_all(&mut tk_ddl, &["use test", "set tidb_mview_enable = on"]);
    tk_ddl
}

const MLOG_ROWS: &str = "select id, tracked, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`";

#[test]
fn test_m_log_online_ddl_add_untracked_column() {
    // Go `skipMLogTestRequiringMDLDisabled`: "MDL is always enabled and read only in nextgen".
    if tidb_config_kerneltype::is_next_gen() {
        return;
    }
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "use test",
        "set tidb_mview_enable = on",
        "set @@global.tidb_enable_metadata_lock=0",
        "create table t (id int primary key, tracked int, untracked int)",
        "create materialized view log on t (id, tracked)",
        "insert into t values (1, 10, 100)",
    ]);
    exec_as_mview_maintenance(&mut tk, "delete from `$mlog$t`");
    let mut ctrl = start_ddl_paused_at_failpoint(ddl_test_kit(&stack), ADD_COLUMN_STATE_WRITE_REORG_FAILPOINT, "alter table t add column c_new int default 0 after id");
    ctrl.wait_until_paused("add-column write-reorg");
    // "Update an untracked column during online DDL: mlog should stay empty."
    must_exec_all(&mut tk, &["update t set untracked = 101 where id = 1"]);
    assert_eq!(go_check(&mut tk, "select * from `$mlog$t`"), Vec::<String>::new());
    // "Insert during online DDL: mlog should still capture tracked columns."
    must_exec_all(&mut tk, &["insert into t (id, tracked, untracked) values (2, 20, 200)"]);
    assert_eq!(go_check(&mut tk, MLOG_ROWS), vec!["2 20 I 1"]);
    ctrl.release_and_wait_finish();
    assert_eq!(go_check(&mut tk, "select * from t order by id"), vec!["1 0 10 101", "2 0 20 200"]);
}

#[test]
fn test_m_log_add_column_rejects_non_public_base_column() {
    if tidb_config_kerneltype::is_next_gen() {
        return;
    }
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "use test",
        "set tidb_mview_enable = on",
        "set @@global.tidb_enable_metadata_lock=0",
        "create table t (id int primary key, tracked int)",
        "create materialized view log on t (id)",
    ]);
    let mut ctrl = start_ddl_paused_at_failpoint(ddl_test_kit(&stack), ADD_COLUMN_STATE_WRITE_REORG_FAILPOINT, "alter table t add column added int default 0");
    ctrl.wait_until_paused("base-table add-column write-reorg");
    // "The base column is non-public while ADD COLUMN is paused, so the mlog
    // should treat it as unavailable instead of adding a transient column."
    // Go `errno.ErrBadField`.
    must_get_err_code(&mut tk, "alter materialized view log on t add column (added)", 1054);
    ctrl.release_and_wait_finish();
    // "Once the base ADD COLUMN completes, the same column is public and can
    // be tracked by the mlog normally."
    must_exec_all(&mut tk, &["alter materialized view log on t add column (added)"]);
    assert_eq!(
        go_check(&mut tk, "select column_name from information_schema.columns where table_schema = 'test' and table_name = '$mlog$t' order by ordinal_position"),
        vec!["id", "added", "_MLOG$_DML_TYPE", "_MLOG$_OLD_NEW"]
    );
}

#[test]
fn test_m_log_online_ddl_add_tracked_column() {
    if tidb_config_kerneltype::is_next_gen() {
        return;
    }
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "use test",
        "set tidb_mview_enable = on",
        "set @@global.tidb_enable_metadata_lock=0",
        "create table t (id int primary key, tracked int, added int, untracked int)",
        "create materialized view log on t (id, tracked)",
        "insert into t values (1, 10, 100, 1000)",
    ]);
    exec_as_mview_maintenance(&mut tk, "delete from `$mlog$t`");
    let mut ctrl = start_ddl_paused_at_failpoint(ddl_test_kit(&stack), ADD_COLUMN_STATE_WRITE_REORG_FAILPOINT, "alter materialized view log on t add column (added)");
    ctrl.wait_until_paused("mlog add-column write-reorg");
    // "The new mlog column is not public yet, so mlog writing should continue
    // with the previous tracked column set instead of treating the transient
    // metadata as corrupt."
    must_exec_all(&mut tk, &["update t set tracked = 11 where id = 1"]);
    assert_eq!(go_check_sorted(&mut tk, MLOG_ROWS), vec!["1 10 U -1", "1 11 U 1"]);
    ctrl.release_and_wait_finish();
    exec_as_mview_maintenance(&mut tk, "delete from `$mlog$t`");
    must_exec_all(&mut tk, &["update t set untracked = 1001 where id = 1"]);
    assert_eq!(go_check(&mut tk, "select * from `$mlog$t`"), Vec::<String>::new());
    must_exec_all(&mut tk, &["update t set added = 101 where id = 1"]);
    assert_eq!(
        go_check_sorted(&mut tk, "select id, tracked, added, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t`"),
        vec!["1 11 100 U -1", "1 11 101 U 1"]
    );

    must_exec_all(&mut tk, &[
        "create table t_drop_race (id int primary key, tracked int, added int)",
        "create materialized view log on t_drop_race (id)",
    ]);
    let mut ctrl2 = start_ddl_paused_at_failpoint(ddl_test_kit(&stack), ADD_COLUMN_STATE_WRITE_REORG_FAILPOINT, "alter materialized view log on t_drop_race add column (added)");
    ctrl2.wait_until_paused("mlog add-column write-reorg before base drop");
    let mut tk_drop = ddl_test_kit(&stack);
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let drop = std::thread::spawn(move || {
        let _ = done_tx.send(exec_to_err(&mut tk_drop, "alter table t_drop_race drop column added"));
    });
    if let Ok(error) = done_rx.recv_timeout(std::time::Duration::from_millis(200)) {
        panic!("base drop column finished before mlog add completed: {error}");
    }
    ctrl2.release_and_wait_finish();
    let error = done_rx.recv_timeout(std::time::Duration::from_secs(10)).expect("timed out waiting base drop column to finish");
    drop.join().unwrap();
    assert!(error.contains("referenced by materialized view log"), "{error}");
    assert_eq!(
        go_check(&mut tk, "select column_name from information_schema.columns where table_schema = 'test' and table_name = '$mlog$t_drop_race' order by ordinal_position"),
        vec!["id", "added", "_MLOG$_DML_TYPE", "_MLOG$_OLD_NEW"]
    );
}

#[test]
fn test_m_log_online_ddl_drop_untracked_column() {
    if tidb_config_kerneltype::is_next_gen() {
        return;
    }
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "use test",
        "set tidb_mview_enable = on",
        "set @@global.tidb_enable_metadata_lock=0",
        "create table t (id int primary key, to_drop int, tracked int)",
        "create materialized view log on t (id, tracked)",
        "insert into t values (1, 100, 10)",
    ]);
    exec_as_mview_maintenance(&mut tk, "delete from `$mlog$t`");
    let mut ctrl = start_ddl_paused_at_failpoint(ddl_test_kit(&stack), DROP_COLUMN_STATE_WRITE_ONLY_FAILPOINT, "alter table t drop column to_drop");
    ctrl.wait_until_paused("drop-untracked-column write-only");
    // "Update a tracked column during online DDL should still emit update
    // logs." "Insert during online DDL should still emit insert logs."
    must_exec_all(&mut tk, &["update t set tracked = 11 where id = 1", "insert into t (id, tracked) values (2, 20)"]);
    assert_eq!(go_check_sorted(&mut tk, MLOG_ROWS), vec!["1 10 U -1", "1 11 U 1", "2 20 I 1"]);
    ctrl.release_and_wait_finish();
    assert_eq!(go_check(&mut tk, "select id, tracked from t order by id"), vec!["1 11", "2 20"]);
}

#[test]
fn test_m_log_update_tracked_generated_column_only() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test", "set tidb_mview_enable = on"]);
    for (name, table, kind) in [("stored", "t_stored", "stored"), ("virtual", "t_virtual", "virtual")] {
        // t.Run(ca.name)
        let mlog = format!("`$mlog${table}`");
        must_exec_all(&mut tk, &[
            &format!("create table {table} (a int primary key, b int, c int, d int as (b+c) {kind})"),
            &format!("create materialized view log on {table} (a, d)"),
            &format!("insert into {table} (a, b, c) values (1, 10, 20)"),
        ]);
        exec_as_mview_maintenance(&mut tk, &format!("delete from {mlog}"));
        must_exec_all(&mut tk, &[&format!("update {table} set b=11 where a=1")]);
        assert_eq!(
            go_check_sorted(&mut tk, &format!("select a, d, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from {mlog}")),
            vec!["1 30 U -1", "1 31 U 1"],
            "{name}"
        );
    }
}

#[test]
fn test_m_log_alter_drop_tracked_generated_column_current_behavior() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["use test", "set tidb_mview_enable = on"]);
    for (name, table, kind) in [("stored", "t_drop_stored", "stored"), ("virtual", "t_drop_virtual", "virtual")] {
        // t.Run(ca.name)
        must_exec_all(&mut tk, &[
            &format!("create table {table} (id int primary key, base int, g int as (base + 1) {kind})"),
            &format!("create materialized view log on {table} (id, g)"),
        ]);
        let error = exec_to_err(&mut tk, &format!("alter table {table} drop column g"));
        assert!(error.contains("referenced by materialized view log"), "{name}: {error}");
    }
}
