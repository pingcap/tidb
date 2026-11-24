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

//! Go `tests/realtikvtest/pipelineddmltest/pipelineddml_test.go`, over the
//! unistore cluster Go's `CreateMockStoreAndSetup` opens without
//! `-with-real-tikv`.

use super::sql_kit::{affected, error, q};
use super::*;

fn tk(stack: &UnistoreClusterStack, connection: u64) -> Session {
    // Go `realtikvtest.TestMain`: client-go's failpoints are enabled.
    tikv_client::tikv::enable_failpoints();
    super::sql_kit::session(stack, connection)
}

/// Go `tk.MustQuery(sql).Check(testkit.Rows(...))` with each row's cells
/// joined by spaces.
fn check(tk: &mut Session, sql: &str) -> Vec<String> {
    q(tk, sql).into_iter().map(|row| row.join(" ")).collect()
}

fn sorted(tk: &mut Session, sql: &str) -> Vec<String> {
    let mut rows = check(tk, sql);
    rows.sort();
    rows
}

fn warnings(tk: &mut Session) -> Vec<String> {
    check(tk, "show warnings")
}

fn assert_warns(tk: &mut Session, message: &str) {
    let warnings = warnings(tk);
    assert!(warnings.iter().any(|warning| warning.contains(message)), "{message}: {warnings:?}");
}

/// The `tikvclient/pipelinedMemDB*` flush thresholds Go's tests lower.
fn flush_thresholds(keys: u32, size: u32, force: u32) -> [tidb_failpoint::Guard; 3] {
    [
        tidb_failpoint::enable_for_test("tikvclient/pipelinedMemDBMinFlushKeys", &format!("return({keys})")),
        tidb_failpoint::enable_for_test("tikvclient/pipelinedMemDBMinFlushSize", &format!("return({size})")),
        tidb_failpoint::enable_for_test(
            "tikvclient/pipelinedMemDBForceFlushSizeThreshold",
            &format!("return({force})"),
        ),
    ]
}

/// Go `prepareData`.
fn prepare_data(tk: &mut Session) {
    rows(tk, "drop table if exists t, _t");
    rows(tk, "create table t (a int primary key, b int)");
    rows(tk, "create table _t like t");
    let mut results = Vec::new();
    for i in 0..100 {
        rows(tk, &format!("insert into t values ({i}, {i})"));
        results.push(format!("{i} {i}"));
    }
    assert_eq!(check(tk, "select * from t order by a asc"), results);
}

/// Go `compareTables`.
fn compare_tables(tk: &mut Session, t1: &str, t2: &str) {
    let left = sorted(tk, &format!("select * from {t1}"));
    let right = sorted(tk, &format!("select * from {t2}"));
    assert_eq!(left, right);
}

fn last_txn_info(tk: &mut Session) -> String {
    check(tk, "select @@tidb_last_txn_info").remove(0)
}

/// Go `TestVariable`'s hint half (the policy half is `tidb-variable`'s
/// `vars_typed::tests::test_variable`).
#[test]
fn test_variable() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = tk(&stack, 2001);
    rows(&mut tk, "set global tidb_pipelined_dml_resource_policy = 'custom{concurrency=64,write_throttle_ratio=0.4}'");
    let mut tk2 = tk_with(&stack, 2002);
    assert_eq!(
        check(&mut tk2, "select @@global.tidb_pipelined_dml_resource_policy"),
        ["custom{concurrency=64,write_throttle_ratio=0.4}"]
    );
    rows(&mut tk, "set global tidb_pipelined_dml_resource_policy = 'standard'");

    // Test hint
    rows(&mut tk, "create table t(id int)");
    rows(&mut tk, "insert into t values (1)");
    rows(&mut tk, "set @@tidb_dml_type=bulk");
    rows(&mut tk, "delete /*+ SET_VAR(tidb_pipelined_dml_resource_policy=\"custom{resolve_concurrency=333}\") */ from t");
    assert!(last_txn_info(&mut tk).contains("\"pipelined\":true"), "{}", last_txn_info(&mut tk));
    assert!(check(&mut tk, "select * from t").is_empty());
}

fn tk_with(stack: &UnistoreClusterStack, connection: u64) -> Session {
    super::sql_kit::session(stack, connection)
}

/// Go `TestPipelinedDMLPositive`: Go detects the pipelined commit by a
/// `pipelinedCommitFail` failpoint that panics; here it returns, and the
/// statement fails with the pipelined commit's error.
#[test]
fn test_pipelined_dml_positive() {
    let _fail = tidb_failpoint::enable_for_test("tikvclient/pipelinedCommitFail", "return");
    let (stack, _users) = cop_backed_stack();
    let mut tk = tk(&stack, 2003);
    rows(&mut tk, "drop table if exists t");
    rows(&mut tk, "create table t (a int, b int)");
    rows(&mut tk, "insert into t values(1, 1)");
    rows(&mut tk, "set session tidb_dml_type = bulk");
    for stmt in ["insert into t values(2, 2)", "update t set b = b + 1", "delete from t"] {
        let error = error(&mut tk, stmt);
        assert!(error.message.contains("pipelined DML commit failed"), "{stmt}: {}", error.message);
    }

    // enable by hint
    rows(&mut tk, "set @@tidb_dml_type = standard");
    for (dml, pipelined) in [
        ("update /*+ SET_VAR(tidb_dml_type=bulk) */ t set b = b + 1", true),
        ("insert /*+ SET_VAR(tidb_dml_type=bulk) */ into t select * from t", true),
        ("delete /*+ SET_VAR(tidb_dml_type=bulk) */ from t", true),
    ] {
        let error = error(&mut tk, dml);
        assert_eq!(error.message.contains("pipelined DML commit failed"), pipelined, "{dml}: {}", error.message);
        assert_eq!(check(&mut tk, "select @@tidb_dml_type"), ["standard"]);
    }
}

/// Go `TestPipelinedDMLNegative`, but for the internal SQL arm (Go sets
/// `InRestrictedSQL` on the test's session directly).
#[test]
fn test_pipelined_dml_negative() {
    let _flush = tidb_failpoint::enable_for_test("tikvclient/beforePipelinedFlush", "panic(\"pipelined memdb should not be enabled\")");
    let _commit = tidb_failpoint::enable_for_test("tikvclient/pipelinedCommitFail", "panic(\"pipelined memdb should not be enabled\")");
    let (stack, _users) = cop_backed_stack();
    let mut tk = tk(&stack, 2004);
    rows(&mut tk, "drop table if exists t");
    rows(&mut tk, "create table t (a int primary key, b int)");

    // tidb_dml_type is not set
    rows(&mut tk, "insert into t values(1, 1)");

    // not in auto-commit txn
    rows(&mut tk, "set session tidb_dml_type = bulk");
    rows(&mut tk, "begin");
    assert_warns(&mut tk, "Pipelined DML can only be used for auto-commit INSERT, REPLACE, UPDATE or DELETE. Fallback to standard mode");
    rows(&mut tk, "insert into t values(2, 2)");
    rows(&mut tk, "insert into t values(4, 4)");
    rows(&mut tk, "commit");

    // in a running txn
    rows(&mut tk, "set session tidb_dml_type = standard");
    rows(&mut tk, "begin");
    // turn on bulk dml in a txn doesn't affect the current txn.
    rows(&mut tk, "set session tidb_dml_type = bulk");
    rows(&mut tk, "insert into t values(5, 5)");
    rows(&mut tk, "commit");

    // it's a read statement
    assert_eq!(sorted(&mut tk, "select * from t"), ["1 1", "2 2", "4 4", "5 5"]);

    // for deprecated batch-dml
    rows(&mut tk, "set @@tidb_batch_delete = 1");
    rows(&mut tk, "set @@tidb_dml_batch_size = 1");
    rows(&mut tk, "set global tidb_enable_batch_dml = 1");
    rows(&mut tk, "insert into t values(7, 7)");
    assert_warns(&mut tk, "Pipelined DML can not be used with the deprecated Batch DML. Fallback to standard mode");
    rows(&mut tk, "set @@tidb_batch_delete = 0");
    rows(&mut tk, "set @@tidb_dml_batch_size = 0");
    rows(&mut tk, "set global tidb_enable_batch_dml = 0");

    // for explain and explain analyze
    rows(&mut tk, "explain insert into t values(8, 8)");
    // explain is read-only, so it doesn't warn.
    assert!(warnings(&mut tk).is_empty());
    rows(&mut tk, "begin");
    rows(&mut tk, "explain analyze insert into t values(9, 9)");
    rows(&mut tk, "commit");

    // disable MDL
    rows(&mut tk, "set global tidb_enable_metadata_lock = off");
    rows(&mut tk, "insert into t values(10, 10)");
    assert_warns(&mut tk, "Pipelined DML can not be used without Metadata Lock. Fallback to standard mode");
    rows(&mut tk, "set global tidb_enable_metadata_lock = on");

    // tidb_constraint_check_in_place = ON
    rows(&mut tk, "set @@tidb_constraint_check_in_place = 1");
    rows(&mut tk, "insert into t values(11, 11)");
    assert_warns(&mut tk, "Pipelined DML can not be used when tidb_constraint_check_in_place=ON. Fallback to standard mode");
    rows(&mut tk, "set @@tidb_constraint_check_in_place = 0");
}

/// Go `TestPipelinedDMLInsert`, without the region split.
#[test]
fn test_pipelined_dml_insert() {
    let _thresholds = flush_thresholds(10, 100, 10240);
    let (stack, _users) = cop_backed_stack();
    let mut tk = tk(&stack, 2005);
    prepare_data(&mut tk);
    rows(&mut tk, "set session tidb_dml_type = bulk");
    rows(&mut tk, "insert into _t select * from t");
    assert!(last_txn_info(&mut tk).contains("\"pipelined\":true"), "{}", last_txn_info(&mut tk));
    compare_tables(&mut tk, "t", "_t");

    assert_eq!(affected(&mut tk, "insert into t select a + 10000, b from t"), 100);
    assert_eq!(check(&mut tk, "select count(1) from t"), ["200"]);

    rows(&mut tk, "truncate table _t");
    assert_eq!(check(&mut tk, "select count(1) from _t"), ["0"]);
    assert_eq!(affected(&mut tk, "insert into _t select * from t"), 200);
    compare_tables(&mut tk, "t", "_t");
}

/// Go `TestPipelinedDMLInsertIgnore`.
#[test]
fn test_pipelined_dml_insert_ignore() {
    let _thresholds = flush_thresholds(10, 100, 10240);
    let (stack, _users) = cop_backed_stack();
    let mut tk = tk(&stack, 2006);
    prepare_data(&mut tk);
    rows(&mut tk, "set session tidb_dml_type = bulk");
    rows(&mut tk, "insert into _t values(0, -1), (19, -1), (29, -1), (39, -1), (49, -1), (59, -1), (69, -1), (79, -1), (89, -1), (99, -1)");
    assert_eq!(affected(&mut tk, "insert ignore into _t select * from t"), 90);
    assert_eq!(check(&mut tk, "select count(1) from _t"), ["100"]);
    assert_eq!(
        sorted(&mut tk, "select * from _t where a in (0, 19, 29, 39, 49, 59, 69, 79, 89, 99)"),
        ["0 -1", "19 -1", "29 -1", "39 -1", "49 -1", "59 -1", "69 -1", "79 -1", "89 -1", "99 -1"]
    );
}

/// Go `TestPipelinedDMLInsertOnDuplicateKeyUpdate`.
#[test]
fn test_pipelined_dml_insert_on_duplicate_key_update() {
    let _thresholds = flush_thresholds(10, 100, 10240);
    let (stack, _users) = cop_backed_stack();
    let mut tk = tk(&stack, 2007);
    prepare_data(&mut tk);
    rows(&mut tk, "insert into _t values(0, -1), (19, -1), (29, -1), (39, -1), (49, -1), (59, -1), (69, -1), (79, -1), (89, -1), (99, -1)");
    assert_eq!(affected(&mut tk, "insert into _t select * from t on duplicate key update b = values(b)"), 110);
    compare_tables(&mut tk, "t", "_t");
}

/// Go `TestPipelinedDMLInsertOnDuplicateKeyUpdateInTxn`.
#[test]
fn test_pipelined_dml_insert_on_duplicate_key_update_in_txn() {
    let _thresholds = flush_thresholds(10, 100, 10240);
    let (stack, _users) = cop_backed_stack();
    let mut tk = tk(&stack, 2008);
    rows(&mut tk, "drop table if exists t1");
    rows(&mut tk, "create table t1 (a int, b int, c varchar(128), unique index idx(b))");
    let values = (0..2000)
        .map(|i| {
            let b = if i == 1500 { 250 } else { i };
            format!("({i}, {b}, 'abcdefghijklmnopqrstuvwxyz1234567890,.?+-=_!@#$&*()_+')")
        })
        .collect::<Vec<_>>()
        .join(", ");
    rows(&mut tk, "set session tidb_dml_type = bulk");
    // Test insert meet duplicate key error.
    let error = error(&mut tk, &format!("insert into t1 values {values}"));
    assert_eq!(error.message, "Duplicate entry '250' for key 't1.idx'");
    assert_eq!(check(&mut tk, "select count(*) from t1"), ["0"]);
    // Test insert ignore
    rows(&mut tk, &format!("insert ignore into t1 values {values}"));
    assert_eq!(warnings(&mut tk), ["Warning 1062 Duplicate entry '250' for key 't1.idx'"]);
    assert_eq!(check(&mut tk, "select count(*) from t1"), ["1999"]);
    assert_eq!(check(&mut tk, "select a, b from t1 where a in (250, 1500)"), ["250 250"]);
    // Test replace into.
    rows(&mut tk, "delete from t1");
    rows(&mut tk, &format!("replace into t1 values {values}"));
    assert_eq!(check(&mut tk, "select count(*) from t1"), ["1999"]);
    assert_eq!(check(&mut tk, "select a, b from t1 where a in (250, 1500)"), ["1500 250"]);
    // Test insert on duplicate key update.
    rows(&mut tk, "delete from t1");
    rows(&mut tk, &format!("insert into t1 values {values} on duplicate key update b = values(b) + 2000"));
    assert_eq!(check(&mut tk, "select count(*) from t1"), ["1999"]);
    assert_eq!(check(&mut tk, "select a, b from t1 where a in (250, 1500)"), ["250 2250"]);
}

/// Go `TestPipelinedDMLDelete`, without the region split.
#[test]
fn test_pipelined_dml_delete() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = tk(&stack, 2009);
    prepare_data(&mut tk);
    rows(&mut tk, "set session tidb_dml_type = bulk");
    assert_eq!(affected(&mut tk, "delete from t where a % 2 = 0"), 50);
    assert!(last_txn_info(&mut tk).contains("\"pipelined\":true"), "{}", last_txn_info(&mut tk));
    assert_eq!(check(&mut tk, "select count(1) from t"), ["50"]);
    rows(&mut tk, "update t set a = a * 101");
    assert_eq!(affected(&mut tk, "delete from t where a % 2 = 1"), 50);
    assert_eq!(check(&mut tk, "select count(1) from t"), ["0"]);
}

/// Go `TestPipelinedDMLUpdate`, without the region split.
#[test]
fn test_pipelined_dml_update() {
    let _thresholds = flush_thresholds(10, 100, 10240);
    let (stack, _users) = cop_backed_stack();
    let mut tk = tk(&stack, 2010);
    prepare_data(&mut tk);
    assert_eq!(check(&mut tk, "select sum(b) from t"), ["4950"]);
    rows(&mut tk, "set session tidb_dml_type = bulk");
    assert_eq!(affected(&mut tk, "update t set b = b + 1"), 100);
    assert_eq!(check(&mut tk, "select sum(b) from t"), ["5050"]);
    rows(&mut tk, "update t set a = a * 100");
    assert_eq!(affected(&mut tk, "update t set b = b + 1"), 100);
    assert_eq!(check(&mut tk, "select sum(b) from t"), ["5150"]);
}

/// Go `TestPipelinedDMLCommitFailed`.
#[test]
fn test_pipelined_dml_commit_failed() {
    let _thresholds = flush_thresholds(10, 100, 10240);
    let _fail = tidb_failpoint::enable_for_test("tikvclient/pipelinedCommitFail", "return");
    let (stack, _users) = cop_backed_stack();
    let mut tk = tk(&stack, 2011);
    let mut tk1 = tk_with(&stack, 2012);
    prepare_data(&mut tk);
    rows(&mut tk, "set session tidb_dml_type = bulk");
    error(&mut tk, "insert into _t select * from t");
    assert!(check(&mut tk1, "select * from _t").is_empty());
    error(&mut tk, "insert into t select a + 100, b from t");
    assert_eq!(check(&mut tk1, "select count(1) from t"), ["100"]);
}

/// Go `TestPipelinedDMLCommitSkipSecondaries`.
#[test]
fn test_pipelined_dml_commit_skip_secondaries() {
    let _thresholds = flush_thresholds(10, 100, 10240);
    let _skip = tidb_failpoint::enable_for_test("tikvclient/pipelinedSkipResolveLock", "return");
    let (stack, _users) = cop_backed_stack();
    let mut tk = tk(&stack, 2013);
    prepare_data(&mut tk);
    rows(&mut tk, "set session tidb_dml_type = bulk");
    assert_eq!(affected(&mut tk, "insert into _t select * from t"), 100);
    compare_tables(&mut tk, "t", "_t");
    assert_eq!(affected(&mut tk, "insert into t select a + 100, b from t"), 100);
    assert_eq!(check(&mut tk, "select count(1) from t"), ["200"]);
}

/// Go `TestReplaceRowCheck`.
#[test]
fn test_replace_row_check() {
    let (stack, _users) = cop_backed_stack();
    let mut tk1 = tk(&stack, 2014);
    rows(&mut tk1, "drop table if exists t1, _t1");
    rows(&mut tk1, "create table t1(a int, b int)");
    rows(&mut tk1, "create table _t1(a int primary key, b int)");
    rows(&mut tk1, "insert into t1 values(1, 1), (2, 2), (1, 2), (2, 1)");
    rows(&mut tk1, "set session tidb_dml_type = bulk");
    rows(&mut tk1, "replace into _t1 select * from t1");
    rows(&mut tk1, "admin check table _t1");
    assert_eq!(sorted(&mut tk1, "select a from _t1"), ["1", "2"]);

    rows(&mut tk1, "truncate table _t1");
    rows(&mut tk1, "insert ignore into _t1 select * from t1");
    rows(&mut tk1, "admin check table _t1");
    assert_eq!(sorted(&mut tk1, "select a from _t1"), ["1", "2"]);

    rows(&mut tk1, "truncate table _t1");
    rows(&mut tk1, "insert into _t1 select * from t1 on duplicate key update b = values(b)");
    rows(&mut tk1, "admin check table _t1");
    assert_eq!(sorted(&mut tk1, "select a from _t1"), ["1", "2"]);
}

/// Go `TestDuplicateKeyErrorMessage`.
#[test]
fn test_duplicate_key_error_message() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = tk(&stack, 2015);
    rows(&mut tk, "drop table if exists t1");
    rows(&mut tk, "create table t1(a int primary key, b int)");
    rows(&mut tk, "insert into t1 values(1, 1)");
    let err1 = error(&mut tk, "insert into t1 values(1, 1)");
    rows(&mut tk, "set session tidb_dml_type = bulk");
    let err2 = error(&mut tk, "insert into t1 values(1, 1)");
    assert_eq!((err1.code, err1.message), (err2.code, err2.message));
}

/// Go `TestInsertIgnoreOnDuplicateKeyUpdate`.
#[test]
fn test_insert_ignore_on_duplicate_key_update() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = tk(&stack, 2016);
    rows(&mut tk, "set session tidb_dml_type = bulk");
    rows(&mut tk, "drop table if exists t1");
    rows(&mut tk, "create table t1(a int, b int, unique index u1(a, b), unique index u2(a))");
    rows(&mut tk, "insert into t1 values(0, 0), (1, 1)");
    rows(&mut tk, "insert ignore into t1 values (0, 2) ,(1, 3) on duplicate key update b = 5, a = 0");
    // if the statement execute successful, the following check should pass.
    assert_eq!(sorted(&mut tk, "select * from t1"), ["0 5", "1 1"]);
}

/// Go `TestConflictError`.
#[test]
fn test_conflict_error() {
    let _thresholds = flush_thresholds(10, 128, 128);
    let (stack, _users) = cop_backed_stack();
    let mut tk = tk(&stack, 2017);
    rows(&mut tk, "drop table if exists t1, _t1");
    rows(&mut tk, "create table t1(a int primary key, b int)");
    rows(&mut tk, "create table _t1(a int primary key, b int)");
    let values = (0..100).map(|i| format!("({i}, {i})")).collect::<Vec<_>>().join(",");
    rows(&mut tk, &format!("insert into t1 values{values}"));
    rows(&mut tk, "set session tidb_dml_type = bulk");
    rows(&mut tk, "insert into _t1 select * from t1");
    rows(&mut tk, "set session tidb_max_chunk_size = 32");
    let error = error(&mut tk, "insert into _t1 select * from t1 order by rand()");
    assert!(error.message.contains("Duplicate entry"), "{}", error.message);
}

/// Go `TestRejectUnsupportedTables`.
#[test]
fn test_reject_unsupported_tables() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = tk(&stack, 2018);
    rows(&mut tk, "set session tidb_dml_type = bulk");

    // FKs are not supported if foreign_key_checks=ON
    rows(&mut tk, "drop table if exists parent, child");
    rows(&mut tk, "create table parent(a int primary key)");
    rows(&mut tk, "create table child(a int, foreign key (a) references parent(a))");
    rows(&mut tk, "insert into parent values(1)");
    rows(&mut tk, "insert into child values(1)");
    let fk_warning = "Pipelined DML can not be used on table with foreign keys when foreign_key_checks = ON. Fallback to standard mode";
    assert_warns(&mut tk, fk_warning);
    let error = error(&mut tk, "insert into child values(2)");
    assert!(error.message.contains("foreign key constraint fails"), "{}", error.message);
    assert_warns(&mut tk, fk_warning);

    // test a delete sql that deletes two tables
    rows(&mut tk, "insert into parent values(2)");
    rows(&mut tk, "delete parent, child from parent left join child on parent.a = child.a");
    assert_warns(&mut tk, fk_warning);

    // swap the order of the two tables
    rows(&mut tk, "insert into parent values(3)");
    rows(&mut tk, "delete child, parent from child left join parent on parent.a = child.a");
    assert_warns(&mut tk, fk_warning);

    rows(&mut tk, "set @@foreign_key_checks=false");
    rows(&mut tk, "insert into parent values(4)");
    rows(&mut tk, "insert into child values(4)");
    assert!(warnings(&mut tk).is_empty());

    // temp tables are not supported
    rows(&mut tk, "create temporary table temp(a int)");
    rows(&mut tk, "insert into temp values(1)");
    assert_warns(&mut tk, "Pipelined DML can not be used on temporary tables. Fallback to standard mode");

    // cached tables are not supported
    rows(&mut tk, "create table cached(a int)");
    rows(&mut tk, "alter table cached cache");
    rows(&mut tk, "insert into cached values(1)");
    assert_warns(&mut tk, "Pipelined DML can not be used on cached tables. Fallback to standard mode");
}

/// Go `TestPipelinedDMLDisableRetry`: auto-retry for pipelined DML is
/// disabled, and the write conflict a Flush (not the commit) meets names
/// its table. Go sets the session's chunk size to 1 so each row is its own
/// flush; `tidb_max_chunk_size` cannot go below 32, so here the one batch
/// is one flush holding both rows, which meets the conflict the same way.
#[test]
fn test_pipelined_dml_disable_retry() {
    let _thresholds = flush_thresholds(1, 1, 1);
    let (stack, _users) = cop_backed_stack();
    let mut tk1 = tk(&stack, 2019);
    let mut tk2 = tk_with(&stack, 2020);
    rows(&mut tk1, "drop table if exists t1");
    rows(&mut tk1, "create table t1(a int primary key, b int)");
    // we need to avoid inserting *literals* into t, so let t2 be the source table.
    rows(&mut tk1, "create table t2(a int, b int)");
    rows(&mut tk1, "insert into t2 values (1, 1), (2, 1)");
    let pause = tidb_failpoint::enable_for_test("tikvclient/beforePipelinedFlush", "pause");
    rows(&mut tk1, "set session tidb_dml_type = bulk");
    let err = std::thread::scope(|scope| {
        let statement = scope.spawn(|| error(&mut tk1, "insert into t1 select * from t2 order by a"));
        std::thread::sleep(std::time::Duration::from_millis(500));
        rows(&mut tk2, "insert into t1 values (1,2)");
        drop(pause);
        statement.join().unwrap()
    });
    assert_eq!(err.code, 9007, "error: {}", err.message);
    assert!(err.message.contains("tableName=test.t1"), "error: {}", err.message);
}

/// client-go `TiKVPipelinedFlushLenHistogram`'s sample count: one per flushed
/// generation.
fn pipelined_flushes() -> u64 {
    tikv_client::metrics::global_metrics()
        .histogram("TiKVPipelinedFlushLenHistogram")
        .map_or(0, |histogram| histogram.get_sample_count())
}

/// Go `InsertExec.exec`'s `txn.MayFlush()` after each batch: a pipelined
/// statement whose MemDB passes the flush thresholds flushes while it runs,
/// once per batch, not only at its commit.
#[test]
fn a_pipelined_insert_flushes_each_batch_like_go() {
    let _thresholds = flush_thresholds(1, 1, 1);
    let (stack, _users) = cop_backed_stack();
    let mut tk = tk(&stack, 2021);
    prepare_data(&mut tk);
    rows(&mut tk, "set session tidb_dml_type = bulk");
    rows(&mut tk, "set session tidb_max_chunk_size = 32");
    let before = pipelined_flushes();
    assert_eq!(affected(&mut tk, "insert into _t select * from t"), 100);
    // 100 source rows in 32-row chunks are four batches; the commit flushes
    // nothing more once the last batch flushed.
    assert!(pipelined_flushes() - before >= 4, "{} flushes", pipelined_flushes() - before);
    compare_tables(&mut tk, "t", "_t");
}
