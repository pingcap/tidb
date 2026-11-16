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

//! Go `tests/realtikvtest/pessimistictest`, against the configured real TiKV cluster.

/// Go `pessimistictest.TestFirstStatementFail` (pessimistic_test.go).
#[test]
fn test_first_statement_fail() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists first");
    tk.must_exec("create table first (k int unique)");
    tk.must_exec("insert first values (1)");
    tk.must_exec("begin pessimistic");
    let err = tk.exec("insert first values (1)");
    assert!(err.is_err());
    tk.must_exec("insert first values (2)");
    tk.must_exec("commit");
}

/// Go `pessimistictest.TestKeyExistsCheck` (pessimistic_test.go).
#[test]
fn test_key_exists_check() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists chk");
    tk.must_exec("create table chk (k int primary key)");
    tk.must_exec("insert chk values (1)");
    tk.must_exec("delete from chk where k = 1");
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert chk values (1)");
    tk.must_exec("commit");
    let mut tk1 = store.new_tk();
    tk1.must_exec("use test");
    tk1.must_exec("begin optimistic");
    tk1.must_exec("insert chk values (1), (2), (3)");
    let err = tk1.exec("commit");
    assert!(err.is_err());
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert chk values (2)");
    tk.must_exec("commit");
}

/// Go `pessimistictest.TestInsertOnDup` (pessimistic_test.go).
#[test]
fn test_insert_on_dup() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk.must_exec("drop table if exists dup");
    tk.must_exec("create table dup (id int primary key, c int)");
    tk2.must_exec("insert dup values (1, 1)");
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert dup values (1, 1) on duplicate key update c = c + 1");
    tk.must_exec("commit");
    tk.check("select * from dup", &["1 2"]);
}

/// Go `pessimistictest.TestPointGetOverflow` (pessimistic_test.go).
#[test]
fn test_point_get_overflow() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("create table t(k tinyint, v int, unique key(k))");
    tk.must_exec("begin pessimistic");
    tk.must_exec("update t set v = 100 where k = -200;");
    tk.must_exec("update t set v = 100 where k in (-200, -400);");
}

/// Go `pessimistictest.TestSelectForUpdateNoWait` (pessimistic_test.go).
#[test]
fn test_select_for_update_no_wait() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    let mut tk3 = store.new_tk();
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk3.must_exec("use test");
    tk.must_exec("drop table if exists tk");
    tk.must_exec("create table tk (c1 int primary key, c2 int)");
    tk.must_exec("insert into tk values(1,1),(2,2),(3,3),(4,4),(5,5)");
    tk.must_exec("set @@autocommit = 0");
    tk2.must_exec("set @@autocommit = 0");
    tk3.must_exec("set @@autocommit = 0");
    tk.must_exec("begin pessimistic");
    tk.must_exec("select * from tk where c1 = 2 for update");
    tk2.must_exec("begin pessimistic");
    let err = tk2.exec("select * from tk where c1 = 2 for update nowait");
    assert!(err.is_err());
    tk.must_exec("commit");
    tk2.must_exec("select * from tk where c1 = 2 for update nowait");
    tk3.must_exec("begin pessimistic");
    let err = tk3.exec("select * from tk where c1 = 2 for update nowait");
    assert!(err.is_err());
    tk2.must_exec("commit");
    tk3.must_exec("select * from tk where c1 = 2 for update");
    tk3.must_exec("commit");
    tk.must_exec("commit");
    tk3.must_exec("begin pessimistic");
    tk3.must_exec("update tk set c2 = c2 + 1 where c1 = 3");
    tk2.must_exec("begin pessimistic");
    let err = tk2.exec("select * from tk where c1 = 3 for update nowait");
    assert!(err.is_err());
    tk3.must_exec("commit");
    tk2.must_exec("select * from tk where c1 = 3 for update nowait");
    tk2.must_exec("commit");
    tk.must_exec("commit");
    tk2.must_exec("commit");
    tk3.must_exec("commit");
    tk.must_exec("begin pessimistic");
    tk.must_exec("select * from tk where c1 >= 2 for update");
    tk2.must_exec("begin pessimistic");
    let err = tk2.exec("select * from tk where c1 = 2 for update nowait");
    assert!(err.is_err());
    let err = tk2.exec("select * from tk where c1 > 3 for update nowait");
    assert!(err.is_err());
    tk2.must_exec("select * from tk where c1 = 1 for update nowait");
    tk2.must_exec("commit");
    tk.check("select * from tk where c1 >= 2 for update", &["2 2", "3 4", "4 4", "5 5"]);
    tk.must_exec("commit");
    tk.must_exec("begin pessimistic");
    tk.must_exec("update tk set c2 = c2 + 10 where c1 > 3");
    tk3.must_exec("begin pessimistic");
    let err = tk3.exec("select * from tk where c1 = 5 for update nowait");
    assert!(err.is_err());
    tk3.must_exec("select * from tk where c1 = 1 for update nowait");
    tk.must_exec("commit");
    tk3.check("select * from tk where c1 > 3 for update nowait", &["4 14", "5 15"]);
    tk3.must_exec("commit");
    tk3.must_exec("begin pessimistic");
    tk3.must_exec("delete from tk where c1 <= 2");
    tk.must_exec("begin pessimistic");
    let err = tk.exec("select * from tk where c1 = 1 for update nowait");
    assert!(err.is_err());
    tk3.must_exec("commit");
    tk.check("select * from tk where c1 > 1 for update nowait", &["3 4", "4 14", "5 15"]);
    tk.must_exec("update tk set c2 = c2 + 1 where c1 = 5");
    tk2.must_exec("begin pessimistic");
    let err = tk2.exec("select * from tk where c1 = 5 for update nowait");
    assert!(err.is_err());
    tk.must_exec("commit");
    tk2.check("select * from tk where c1 = 5 for update nowait", &["5 16"]);
    tk2.must_exec("update tk set c2 = c2 + 1 where c1 = 5");
    tk2.check("select * from tk where c1 = 5 for update nowait", &["5 17"]);
    tk2.must_exec("commit");
}

/// Go `pessimistictest.TestPushConditionCheckForPessimisticTxn` (pessimistic_test.go).
#[test]
fn test_push_condition_check_for_pessimistic_txn() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    let mut tk1 = store.new_tk();
    tk1.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (i int key)");
    tk.must_exec("insert into t values (1)");
    tk.must_exec("set tidb_txn_mode = 'pessimistic'");
    tk.must_exec("begin");
    tk1.must_exec("delete from t where i = 1");
    tk.must_exec("insert into t values (1) on duplicate key update i = values(i)");
    tk.must_exec("commit");
    tk.check("select * from t", &["1"]);
    tk.must_exec("drop table if exists t");
}

/// Go `pessimistictest.TestBatchPointGetWriteConflict` (pessimistic_test.go).
#[test]
fn test_batch_point_get_write_conflict() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    let mut tk1 = store.new_tk();
    tk1.must_exec("use test");
    tk.must_exec("drop table if exists t;");
    tk.must_exec("create table t (i int primary key, c int);");
    tk.must_exec("insert t values (1, 1), (2, 2), (3, 3)");
    tk1.must_exec("begin pessimistic");
    tk1.check("select * from t where i = 1", &["1 1"]);
    tk.must_exec("update t set c = c + 1");
    tk1.check("select * from t where i in (1, 3) for update", &["1 2", "3 4"]);
    tk1.must_exec("commit");
}

/// Go `pessimistictest.TestPessimisticLockReadValue` (pessimistic_test.go).
#[test]
fn test_pessimistic_lock_read_value() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t;");
    tk.must_exec("create table t(i int, j int, k int, unique key uk(j));");
    tk.must_exec("insert into t values (1, 1, 1);");
    let mut tk1 = store.new_tk();
    tk1.must_exec("use test");
    tk1.must_exec("begin optimistic");
    tk1.check("select * from t where j = 1 for update", &["1 1 1"]);
    tk1.check("select * from t where j = 1 for update", &["1 1 1"]);
    tk1.must_exec("commit");
    let mut tk2 = store.new_tk();
    tk2.must_exec("use test");
    tk2.must_exec("begin pessimistic");
    tk2.check("select * from t where j = 1 for update", &["1 1 1"]);
    tk2.check("select * from t where j = 1 for update", &["1 1 1"]);
    tk2.must_exec("commit");
}

/// Go `pessimistictest.TestRCWaitTSOTwice` (pessimistic_test.go).
#[test]
fn test_rc_wait_tso_twice() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("create table t (i int key)");
    tk.must_exec("insert into t values (1)");
    tk.must_exec("set tidb_txn_mode = 'pessimistic'");
    tk.must_exec("set tx_isolation = 'read-committed'");
    tk.must_exec("set autocommit = 0");
    tk.check("select * from t where i = 1", &["1"]);
    tk.must_exec("rollback");
}

/// Go `pessimistictest.TestNonAutoCommitWithPessimisticMode` (pessimistic_test.go).
#[test]
fn test_non_auto_commit_with_pessimistic_mode() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk.must_exec("drop table if exists t1");
    tk.must_exec("create table t1 (c1 int primary key, c2 int)");
    tk.must_exec("insert into t1 values (1, 1)");
    tk.must_exec("set tidb_txn_mode = 'pessimistic'");
    tk.must_exec("set autocommit = 0");
    tk.check("select * from t1 where c2 = 1 for update", &["1 1"]);
    tk2.must_exec("insert into t1 values(2, 1)");
    tk.check("select * from t1 where c2 = 1 for update", &["1 1", "2 1"]);
    tk.must_exec("commit");
    tk2.must_exec("insert into t1 values(3, 1)");
    tk.must_exec("set tx_isolation = 'read-committed'");
    tk.check("select * from t1 where c2 = 1 for update", &["1 1", "2 1", "3 1"]);
    tk2.must_exec("insert into t1 values(4, 1)");
    tk.check("select * from t1 where c2 = 1 for update", &["1 1", "2 1", "3 1", "4 1"]);
    tk.must_exec("commit");
}

/// Go `pessimistictest.TestLockGotKeysInRC` (pessimistic_test.go).
#[test]
fn test_lock_got_keys_in_rc() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk.must_exec("set tx_isolation = 'READ-COMMITTED'");
    tk2.must_exec("set tx_isolation = 'READ-COMMITTED'");
    tk.must_exec("drop table if exists t1");
    tk.must_exec("create table t1 (c1 int primary key, c2 int, c3 int, unique key uk(c2))");
    tk.must_exec("insert into t1 values (1, 1, 1)");
    tk.must_exec("insert into t1 values (5, 5, 5)");
    tk.must_exec("insert into t1 values (10, 10, 10)");
    tk.must_exec("begin pessimistic");
    tk.check("select * from t1 where c1 in (2, 3) for update", &[]);
    tk.check("select * from t1 where c2 in (2, 3) for update", &[]);
    tk.check("select * from t1 where c1 = 2 for update", &[]);
    tk.check("select * from t1 where c2 = 2 for update", &[]);
    tk2.must_exec("begin pessimistic");
    tk2.must_exec("insert into t1 values(2, 2, 2)");
    tk2.must_exec("select * from t1 where c1 = 3 for update nowait");
    tk2.must_exec("select * from t1 where c2 = 3 for update nowait");
    tk.must_exec("rollback");
    tk2.must_exec("rollback");
}

/// Go `pessimistictest.TestBatchPointGetAlreadyLocked` (pessimistic_test.go).
#[test]
fn test_batch_point_get_already_locked() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (c1 int, c2 int, c3 int, primary key(c1, c2))");
    tk.must_exec("insert t values (1, 1, 1), (2, 2, 2)");
    tk.must_exec("begin pessimistic");
    tk.check("select * from t where c1 > 1 for update", &["2 2 2"]);
    tk.check("select * from t where (c1, c2) in ((2,2)) for update", &["2 2 2"]);
    tk.must_exec("commit");
}

/// Go `pessimistictest.TestRCSubQuery` (pessimistic_test.go).
#[test]
fn test_rc_sub_query() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk.must_exec("drop table if exists t, t1");
    tk.must_exec("create table `t` ( `c1` int(11) not null, `c2` int(11) default null, primary key (`c1`) )");
    tk.must_exec("insert into t values(1, 3)");
    tk.must_exec("create table `t1` ( `c1` int(11) not null, `c2` int(11) default null, primary key (`c1`) )");
    tk.must_exec("insert into t1 values(1, 3)");
    tk.must_exec("set transaction isolation level read committed");
    tk.must_exec("begin pessimistic");
    tk2.must_exec("update t1 set c2 = c2 + 1");
    tk.check("select * from t1 where c1 = (select 1) and 1=1;", &["1 4"]);
    tk.check("select * from t1 where c1 = (select c1 from t where c1 = 1) and 1=1;", &["1 4"]);
    tk.must_exec("rollback");
}

/// Go `pessimistictest.TestDupLockInconsistency` (pessimistic_test.go).
#[test]
fn test_dup_lock_inconsistency() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (a int, b int, index b (b))");
    tk.must_exec("insert t (a) values (1), (1)");
    tk.must_exec("begin pessimistic");
    tk.must_exec("update t, (select a from t) s set t.b = s.a");
    tk.must_exec("commit");
    tk.must_exec("admin check table t");
}

/// Go `pessimistictest.TestUseLockCacheInRCMode` (pessimistic_test.go).
#[test]
fn test_use_lock_cache_in_rc_mode() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    let mut tk3 = store.new_tk();
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk3.must_exec("use test");
    tk.must_exec("drop table if exists test_kill");
    tk.must_exec("CREATE TABLE SEQUENCE_VALUE_ITEM(SEQ_NAME varchar(60) NOT NULL, SEQ_ID decimal(18,0) DEFAULT NULL, PRIMARY KEY (SEQ_NAME))");
    tk.must_exec("create table t1(c1 int, c2 int, unique key(c1))");
    tk.must_exec("insert into sequence_value_item values(\"OSCurrentStep\", 0)");
    tk.must_exec("insert into t1 values(1, 1)");
    tk.must_exec("insert into t1 values(2, 2)");
    tk2.must_exec("set @@tx_isolation='READ-COMMITTED'");
    tk2.must_exec("set autocommit = 0");
    tk2.must_exec("SELECT SEQ_ID FROM SEQUENCE_VALUE_ITEM WHERE SEQ_NAME='OSCurrentStep' FOR UPDATE");
    tk2.must_exec("UPDATE SEQUENCE_VALUE_ITEM SET SEQ_ID=SEQ_ID+100 WHERE SEQ_NAME='OSCurrentStep'");
    tk2.must_exec("rollback");
    tk2.check("select * from t1 where c1 = 1 for update", &["1 1"]);
    tk2.must_exec("update t1 set c2 = c2 + 10 where c1 = 1");
    tk2.check("select * from t1 where c1 in (1, 2) for update", &["1 11", "2 2"]);
    tk2.must_exec("update t1 set c2 = c2 + 10 where c1 in (2)");
    tk2.check("select * from t1 where c1 in (1, 2) for update", &["1 11", "2 12"]);
    tk2.must_exec("commit");
    tk3.check("select * from SEQUENCE_VALUE_ITEM", &["OSCurrentStep 0"]);
    tk3.must_exec("set @@tx_isolation='READ-COMMITTED'");
    tk3.must_exec("begin");
    tk3.must_exec("SELECT SEQ_ID FROM SEQUENCE_VALUE_ITEM WHERE SEQ_NAME='OSCurrentStep' FOR UPDATE");
    tk3.must_exec("UPDATE SEQUENCE_VALUE_ITEM SET SEQ_ID=SEQ_ID+100 WHERE SEQ_NAME='OSCurrentStep'");
    tk3.check("select * from t1 where c1 = 1 for update", &["1 11"]);
    tk3.must_exec("update t1 set c2 = c2 + 10 where c1 = 1");
    tk3.check("select * from t1 where c1 in (1, 2) for update", &["1 21", "2 12"]);
    tk3.must_exec("update t1 set c2 = c2 + 10 where c1 in (2)");
    tk3.check("select * from t1 where c1 in (1, 2) for update", &["1 21", "2 22"]);
    tk3.must_exec("commit");
    tk.check("select * from SEQUENCE_VALUE_ITEM", &["OSCurrentStep 100"]);
    tk.check("select * from SEQUENCE_VALUE_ITEM where SEQ_ID = 100", &["OSCurrentStep 100"]);
    tk.check("select * from t1 where c1 = 2", &["2 22"]);
    tk.check("select * from t1 where c1 in (1, 2, 3)", &["1 21", "2 22"]);
    tk2.must_exec("set autocommit = 1");
    tk2.must_exec("set autocommit = 0");
    tk2.must_exec("SELECT SEQ_ID FROM SEQUENCE_VALUE_ITEM WHERE SEQ_NAME in ('OSCurrentStep') FOR UPDATE");
    tk2.must_exec("UPDATE SEQUENCE_VALUE_ITEM SET SEQ_ID=SEQ_ID+100 WHERE SEQ_NAME in ('OSCurrentStep')");
    tk2.check("select * from t1 where c1 in (1, 2, 3, 4, 5) for update", &["1 21", "2 22"]);
    tk2.must_exec("update t1 set c2 = c2 + 10 where c1 in (1, 2, 3, 4, 5)");
    tk2.check("select * from t1 where c1 in (1, 2, 3, 4, 5) for update", &["1 31", "2 32"]);
    tk2.must_exec("commit");
    tk2.must_exec("SELECT SEQ_ID FROM SEQUENCE_VALUE_ITEM WHERE SEQ_NAME in ('OSCurrentStep') FOR UPDATE");
    tk2.must_exec("UPDATE SEQUENCE_VALUE_ITEM SET SEQ_ID=SEQ_ID+100 WHERE SEQ_NAME in ('OSCurrentStep')");
    tk2.must_exec("rollback");
    tk.check("select * from SEQUENCE_VALUE_ITEM", &["OSCurrentStep 200"]);
    tk.check("select * from SEQUENCE_VALUE_ITEM where SEQ_NAME in ('OSCurrentStep')", &["OSCurrentStep 200"]);
    tk.check("select * from t1 where c1 in (1, 2, 3)", &["1 31", "2 32"]);
    tk.must_exec("rollback");
    tk2.must_exec("rollback");
    tk3.must_exec("rollback");
}

/// Go `pessimistictest.TestPointGetWithDeleteInMem` (pessimistic_test.go).
#[test]
fn test_point_get_with_delete_in_mem() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk.must_exec("drop table if exists uk");
    tk.must_exec("create table uk (c1 int primary key, c2 int, unique key uk(c2))");
    tk.must_exec("insert uk values (1, 77), (2, 88), (3, 99)");
    tk.must_exec("begin pessimistic");
    tk.must_exec("delete from uk where c1 = 1");
    tk.check("select * from uk where c2 = 77", &[]);
    tk.check("select * from uk where c2 in(77, 88, 99)", &["2 88", "3 99"]);
    tk.check("select * from uk", &["2 88", "3 99"]);
    tk.check("select * from uk where c2 = 77 for update", &[]);
    tk.check("select * from uk where c2 in(77, 88, 99) for update", &["2 88", "3 99"]);
    tk.must_exec("rollback");
    tk2.check("select * from uk where c1 = 1", &["1 77"]);
    tk.must_exec("begin");
    tk.must_exec("update uk set c1 = 10 where c1 = 1");
    tk.check("select * from uk where c2 = 77", &["10 77"]);
    tk.check("select * from uk where c2 in(77, 88, 99)", &["10 77", "2 88", "3 99"]);
    tk.must_exec("commit");
    tk2.check("select * from uk where c1 = 1", &[]);
    tk2.check("select * from uk where c2 = 77", &["10 77"]);
    tk2.check("select * from uk where c1 = 10", &["10 77"]);
    tk.must_exec("drop table if exists uk");
}

/// Go `pessimistictest.TestLazyUniquenessCheckForSimpleInserts` (pessimistic_test.go).
#[test]
fn test_lazy_uniqueness_check_for_simple_inserts() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk.must_exec("create table t(id int primary key, v int)");
    tk.must_exec("set @@tidb_constraint_check_in_place_pessimistic = 0");
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert into t values(1, 0)");
    tk2.must_exec("begin pessimistic");
    tk2.must_exec("insert into t values (1, 1)");
    tk2.must_exec("commit");
    let err = tk.exec("commit");
    assert!(err.as_ref().unwrap_err().contains("[kv:9007]Write conflict"), "{err:?}");
    tk.check("select * from t", &["1 1"]);
    tk.must_exec("admin check table t");
    tk.must_exec("create table t2(id int primary key, uk int, unique index(uk))");
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert into t2 values(1, 0)");
    tk2.must_exec("begin pessimistic");
    tk2.must_exec("insert into t2 values (2, 0)");
    tk2.must_exec("commit");
    let err = tk.exec("commit");
    assert!(err.as_ref().unwrap_err().contains("[kv:9007]Write conflict"), "{err:?}");
    tk.check("select * from t2", &["2 0"]);
    tk.must_exec("admin check table t2");
}

/// Go `pessimistictest.TestLazyUniquenessCheckForInsertIgnore` (pessimistic_test.go).
#[test]
fn test_lazy_uniqueness_check_for_insert_ignore() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("set @@tidb_constraint_check_in_place_pessimistic=0");
    tk.must_exec("create table t (id int primary key, uk int, unique key i1(uk))");
    tk.must_exec("insert into t values (1, 1)");
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert ignore into t values (1, 2)");
    tk.must_exec("commit");
    tk.check("select * from t", &["1 1"]);
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert ignore into t values (2, 1)");
    tk.must_exec("commit");
    tk.check("select * from t", &["1 1"]);
}

/// Go `pessimistictest.TestLazyUniquenessCheckWithSavepoint` (pessimistic_test.go).
#[test]
fn test_lazy_uniqueness_check_with_savepoint() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("set @@tidb_constraint_check_in_place_pessimistic=0");
    tk.must_exec("begin pessimistic");
    let err = tk.exec_to_err("savepoint s1");
    assert!(err.contains("savepoint is not supported in pessimistic transactions when in-place constraint check is disabled"), "{err:?}");
}

/// Go `pessimistictest.TestLockUnchangedRowKey` (pessimistic_test.go).
#[test]
fn test_lock_unchanged_row_key() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk.must_exec("drop table if exists unchanged");
    tk.must_exec("create table unchanged (id int primary key, c int)");
    tk.must_exec("insert unchanged values (1, 1), (2, 2)");
    tk.must_exec("begin pessimistic");
    tk.must_exec("update unchanged set c = 1 where id < 2");
    tk2.must_exec("begin pessimistic");
    tk2.exec_to_err("select * from unchanged where id = 1 for update nowait");
    tk.must_exec("rollback");
    tk2.must_exec("select * from unchanged where id = 1 for update nowait");
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert unchanged values (2, 2) on duplicate key update c = values(c)");
    tk2.exec_to_err("select * from unchanged where id = 2 for update nowait");
    tk.must_exec("commit");
    tk2.must_exec("select * from unchanged where id = 1 for update nowait");
    tk2.must_exec("rollback");

}

/// Go `pessimistictest.TestPessimisticUnionForUpdate` (pessimistic_test.go).
#[test]
fn test_pessimistic_union_for_update() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t(id int, v int, k int, primary key (id), key kk(k))");
    tk.must_exec("insert into t select 1, 1, 1");
    tk.must_exec("begin pessimistic");
    tk.must_exec("(select * from t where id between 0 and 1 for update) union all (select * from t where id between 0 and 1 for update)");
    tk.must_exec("update t set k = 2 where k = 1");
    tk.must_exec("commit");
    tk.must_exec("admin check table t");

}

/// Go `pessimistictest.TestBatchPointGetLockIndex` (pessimistic_test.go).
#[test]
fn test_batch_point_get_lock_index() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk.must_exec("drop table if exists t1");
    tk.must_exec("create table t1 (c1 int primary key, c2 int, c3 int, unique key uk(c2))");
    tk.must_exec("insert into t1 values (1, 1, 1)");
    tk.must_exec("insert into t1 values (5, 5, 5)");
    tk.must_exec("insert into t1 values (10, 10, 10)");
    tk.must_exec("begin pessimistic");
    tk.check("select * from t1 where c2 in (2, 3) for update", &[]);
    tk2.must_exec("set innodb_lock_wait_timeout = 1");
    tk2.must_exec("begin pessimistic");
    tk2.must_get_err_code("insert into t1 values(2, 2, 2)", 1205);
    tk2.must_get_err_code("select * from t1 where c2 = 3 for update nowait", 3572);
    tk.must_exec("rollback");
    tk2.must_exec("rollback");

}

/// Go `pessimistictest.TestRCIndexMerge` (pessimistic_test.go).
#[test]
fn test_rc_index_merge() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (id int primary key, v int, a int not null, b int not null,\n\t\tindex ia (a), index ib (b))");
    tk.must_exec("insert into t values (1, 10, 1, 1)");
    tk.must_exec("set transaction isolation level read committed");
    tk.must_exec("begin pessimistic");
    tk.check("select /*+ USE_INDEX_MERGE(t, ia, ib) */ * from t where a > 0 or b > 0", &["1 10 1 1"]);
    tk.check("select /*+ NO_INDEX_MERGE() */ * from t where a > 0 or b > 0", &["1 10 1 1"]);
    tk2.must_exec("update t set v = 11 where id = 1");
    let plan = tk.must_query("explain select /*+ USE_INDEX_MERGE(t, ia, ib) */ * from t where a > 0 or b > 0");
assert!(plan[0][0].contains("IndexMerge_"), "{plan:?}");
    tk.check("select /*+ USE_INDEX_MERGE(t, ia, ib) */ * from t where a > 0 or b > 0", &["1 11 1 1"]);
    tk.check("select /*+ NO_INDEX_MERGE() */ * from t where a > 0 or b > 0", &["1 11 1 1"]);

}

/// Go `pessimistictest.TestTxnMode`.
#[test]
fn test_txn_mode() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    for (begin, mode, pessimistic) in [
        ("pessimistic", "pessimistic", true), ("pessimistic", "optimistic", true),
        ("pessimistic", "", true), ("optimistic", "pessimistic", false),
        ("optimistic", "optimistic", false), ("optimistic", "", false),
        ("", "pessimistic", true), ("", "optimistic", false), ("", "", false),
    ] {
        tk.must_exec(&format!("set @@tidb_txn_mode = '{mode}'"));
        tk.must_exec(&format!("begin {begin}"));
        assert_eq!(tk.session().txn_mode().unwrap().is_pessimistic(), pessimistic);
        tk.must_exec("rollback");
    }
    tk.must_exec("set @@autocommit = 0");
    tk.must_exec("create table if not exists txn_mode (a int)");
    for (mode, pessimistic) in [("pessimistic", true), ("optimistic", false), ("", false)] {
        tk.must_exec(&format!("set @@tidb_txn_mode = '{mode}'"));
        tk.must_exec("rollback");
        tk.must_exec("insert txn_mode values (1)");
        assert_eq!(tk.session().txn_mode().unwrap().is_pessimistic(), pessimistic);
        tk.must_exec("rollback");
    }
    tk.must_exec("set @@global.tidb_txn_mode = 'pessimistic'");
    let mut tk1 = store.new_tk();
    tk1.must_exec("use test");
    tk1.check("select @@tidb_txn_mode", &["pessimistic"]);
    tk1.must_exec("set @@autocommit = 0");
    tk1.must_exec("insert txn_mode values (2)");
    assert!(tk1.session().txn_mode().unwrap().is_pessimistic());
    tk1.must_exec("set @@tidb_txn_mode = ''");
    tk1.must_exec("rollback");
    tk1.must_exec("insert txn_mode values (2)");
    assert!(!tk1.session().txn_mode().unwrap().is_pessimistic());
    tk1.must_exec("rollback");
}

/// Go `pessimistictest.TestDeadlock`.
#[test]
#[ignore = "deadlock (Go t.Skip)"]
fn test_deadlock() {}

/// Go `pessimistictest.TestPointGetKeyLock`.
#[test]
#[ignore = "deadlock (Go t.Skip)"]
fn test_point_get_key_lock() {}

/// Go `pessimistictest.TestSingleStatementRollback`: live TiKV cannot manipulate mock regions.
#[test]
#[ignore = "skip with tikv because cluster manipulate is not available (Go t.Skip)"]
fn test_single_statement_rollback() {}

/// Go `pessimistictest.TestAsyncCommitWithSchemaChange`: Go skips both storage modes.
#[test]
#[ignore = "This test is unstable as depending on time.Sleep (Go t.Skip)"]
fn test_async_commit_with_schema_change() {}

/// Go `pessimistictest.Test1PCWithSchemaChange`: Go skips both storage modes.
#[test]
#[ignore = "This test is unstable as depending on time.Sleep (Go t.Skip)"]
fn test1_pc_with_schema_change() {}
/// Go `pessimistictest.TestTransactionIsolationAndForeignKey` (pessimistic_test.go).
#[test]
fn test_transaction_isolation_and_foreign_key() {
    
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk.must_exec("drop table if exists t1,t2");
    tk.must_exec("create table t1 (id int primary key)");
    tk.must_exec("create table t2 (id int primary key, pid int, foreign key (pid) references t1(id) on delete cascade on update cascade)");
    tk.must_exec("insert into t1 values (1)");
    tk.must_exec("set tx_isolation = 'READ-COMMITTED'");
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert into t2 values (1,1)");
    tk.must_get_err_code("insert into t2 values (2,2)", 1452);
    tk2.must_exec("insert into t1 values (2)");
    tk.check("select * from t1", &["1", "2"]);
    tk.must_exec("insert into t2 values (2,2)");
    std::thread::scope(|scope| {
let worker = scope.spawn(|| {
    tk2.must_exec("delete from t1 where id=2");
    });
    std::thread::sleep(std::time::Duration::from_millis(10));
    tk.must_exec("commit");
    worker.join().unwrap();
});
    tk.check("select * from t1", &["1"]);
    tk.check("select * from t2", &["1 1"]);
    tk2.must_exec("delete from t1 where id=1");
    tk.check("select * from t1", &[]);
    tk.check("select * from t2", &[]);
    tk.must_exec("admin check table t1");
    tk.must_exec("admin check table t2");

}

/// Go `pessimistictest.TestIssue28011`.
#[test]
fn test_issue28011() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    for (query, expected) in [
        ("update t set b = 'x' where a = 'a'", ["a x", "b y", "c z"]),
        ("update t set b = 'x' where a in ('a', 'b', 'c')", ["a x", "b y", "c x"]),
        ("select a from t where a = 'a' for update", ["a x", "b y", "c z"]),
        ("select a from t where a in ('a', 'b', 'c') for update", ["a x", "b y", "c z"]),
    ] {
        tk.must_exec("drop table if exists t");
        tk.must_exec("create table t (a varchar(10) primary key nonclustered, b varchar(10))");
        tk.must_exec("insert into t values ('a', 'x'), ('b', 'x'), ('c', 'z')");
        tk.must_exec("begin");
        tk.must_exec(query);
        tk.check("select a from t", &["a", "b", "c"]);
        tk.must_exec("replace into t values ('b', 'y')");
        tk.check("select a from t", &["a", "b", "c"]);
        tk.check("select a, b from t order by a", &expected);
        tk.must_exec("commit");
        tk.check("select a, b from t order by a", &expected);
        tk.must_exec("admin check table t");
    }
}

/// Go `pessimistictest.TestMaxExecutionTimeWithSelectForUpdate`.
#[test]
fn test_max_execution_time_with_select_for_update() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists test_lock");
    tk.must_exec("create table test_lock (id int primary key, value int)");
    tk.must_exec("insert into test_lock values (1, 100)");
    let mut tk1 = store.new_tk();
    let mut tk2 = store.new_tk();
    tk1.must_exec("use test");
    tk2.must_exec("use test");
    tk2.must_exec("set innodb_lock_wait_timeout = 30");
    tk1.must_exec("begin pessimistic");
    tk1.check("select * from test_lock where id = 1 for update", &["1 100"]);
    for sql in ["select * from test_lock where id = 1 for update", "(select * from test_lock where id = 1 for update)"] {
        tk2.must_exec("begin pessimistic");
        tk2.must_exec("set max_execution_time = 1000");
        tk2.must_contain_err_msg(sql, "maximum statement execution time exceeded");
        tk2.must_exec("rollback");
    }
    tk2.must_exec("begin pessimistic");
    tk2.must_exec("set innodb_lock_wait_timeout = 1");
    tk2.must_exec("set max_execution_time = 300");
    tk2.must_get_err_code("update test_lock set value = value + 1 where id = 1", 1205);
    tk2.must_exec("rollback");
    tk1.must_exec("rollback");
}
/// Go `pessimistictest.TestInsertDupKeyAfterLock` (pessimistic_test.go).
#[test]
fn test_insert_dup_key_after_lock() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk2.must_exec("drop table if exists t1");
    tk2.must_exec("create table t1(c1 int primary key, c2 int, c3 int, unique key uk(c2));");
    tk2.must_exec("insert into t1 values(1, 2, 3);");
    tk2.must_exec("insert into t1 values(10, 20, 30);");
    tk.must_exec("begin pessimistic");
    tk.must_get_err_code("update t1 set c2 = 20 where c1 = 1;", 1062);
    tk.must_get_err_code("insert into t1 values(1, 15, 300);", 1062);
    tk.must_exec("commit");
    tk2.check("select * from t1", &["1 2 3", "10 20 30"]);
    tk.must_exec("begin pessimistic");
    tk.must_exec("select * from t1 for update");
    tk.must_get_err_code("insert into t1 values(1, 15, 300);", 1062);
    tk.must_exec("commit");
    tk2.check("select * from t1", &["1 2 3", "10 20 30"]);
    tk.must_exec("begin pessimistic");
    tk.must_exec("select * from t1 where c2 = 2 for update");
    tk.must_get_err_code("insert into t1 values(1, 15, 300);", 1062);
    tk.must_exec("commit");
    tk2.check("select * from t1", &["1 2 3", "10 20 30"]);
    tk.must_exec("begin pessimistic");
    tk.must_get_err_code("insert into t1 values(1, 15, 300);", 1062);
    tk.must_exec("insert into t1 values(5, 6, 7)");
    tk.must_get_err_code("insert into t1 values(6, 6, 7);", 1062);
    tk.must_exec("commit");
    tk2.check("select * from t1", &["1 2 3", "5 6 7", "10 20 30"]);
    tk.must_exec("begin pessimistic");
    tk.must_exec("delete from t1 where c2 > 2");
    tk.must_exec("insert into t1 values(10, 20, 500);");
    tk.must_get_err_code("insert into t1 values(20, 20, 30);", 1062);
    tk.must_get_err_code("insert into t1 values(1, 20, 30);", 1062);
    tk.must_exec("commit");
    tk2.check("select * from t1", &["1 2 3", "10 20 500"]);
    tk.must_exec("begin pessimistic");
    tk.must_get_err_code("update t1 set c2 = 20 where c1 >= 1 and c1 < 5;", 1062);
    tk.must_get_err_code("update t1 set c2 = 20 where c1 >= 1 and c1 < 50;", 1062);
    tk.must_get_err_code("insert into t1 values(1, 15, 300);", 1062);
    tk.must_exec("commit");
    tk2.check("select * from t1", &["1 2 3", "10 20 500"]);
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert into t1 values(5, 6, 7)");
    tk.must_exec("select * from t1 where c1 = 5 for update");
    tk.must_exec("select * from t1 where c1 = 6 for update");
    tk.must_exec("select * from t1 for update");
    tk.must_get_err_code("insert into t1 values(7, 6, 7)", 1062);
    tk.must_get_err_code("insert into t1 values(5, 8, 6)", 1062);
    tk.must_exec("select * from t1 where c1 = 5 for update");
    tk.must_exec("select * from t1 where c2 = 8 for update");
    tk.must_exec("select * from t1 for update");
    tk.must_exec("commit");
    tk2.check("select * from t1", &["1 2 3", "5 6 7", "10 20 500"]);
    tk.must_exec("begin optimistic");
    tk.check("select * from t1 where c1 = 1 for update", &["1 2 3"]);
    tk.must_exec("insert into t1 values(10, 10, 10)");
    tk.must_get_err_code("commit", 1062);

}

/// Go `pessimistictest.TestInsertDupKeyAfterLockBatchPointGet` (pessimistic_test.go).
#[test]
fn test_insert_dup_key_after_lock_batch_point_get() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk2.must_exec("drop table if exists t1");
    tk2.must_exec("create table t1(c1 int primary key, c2 int, c3 int, unique key uk(c2));");
    tk2.must_exec("insert into t1 values(1, 2, 3);");
    tk2.must_exec("insert into t1 values(10, 20, 30);");
    tk.must_exec("begin pessimistic");
    tk.must_get_err_code("update t1 set c2 = 20 where c1 in (1);", 1062);
    tk.must_get_err_code("insert into t1 values(1, 15, 300);", 1062);
    tk.must_exec("commit");
    tk2.check("select * from t1", &["1 2 3", "10 20 30"]);
    tk.must_exec("begin pessimistic");
    tk.must_exec("select * from t1 for update");
    tk.must_get_err_code("insert into t1 values(1, 15, 300);", 1062);
    tk.must_exec("commit");
    tk2.check("select * from t1", &["1 2 3", "10 20 30"]);
    tk.must_exec("begin pessimistic");
    tk.must_exec("select * from t1 where c2 in (2) for update");
    tk.must_get_err_code("insert into t1 values(1, 15, 300);", 1062);
    tk.must_exec("commit");
    tk2.check("select * from t1", &["1 2 3", "10 20 30"]);
    tk.must_exec("begin pessimistic");
    tk.must_get_err_code("insert into t1 values(1, 15, 300);", 1062);
    tk.must_exec("insert into t1 values(5, 6, 7)");
    tk.must_get_err_code("insert into t1 values(6, 6, 7);", 1062);
    tk.must_exec("commit");
    tk2.check("select * from t1", &["1 2 3", "5 6 7", "10 20 30"]);
    tk.must_exec("begin pessimistic");
    tk.must_exec("delete from t1 where c2 > 2");
    tk.must_exec("insert into t1 values(10, 20, 500);");
    tk.must_get_err_code("insert into t1 values(20, 20, 30);", 1062);
    tk.must_get_err_code("insert into t1 values(1, 20, 30);", 1062);
    tk.must_exec("commit");
    tk2.check("select * from t1", &["1 2 3", "10 20 500"]);
    tk.must_exec("begin pessimistic");
    tk.must_get_err_code("update t1 set c2 = 20 where c1 >= 1 and c1 < 5;", 1062);
    tk.must_get_err_code("update t1 set c2 = 20 where c1 >= 1 and c1 < 50;", 1062);
    tk.must_get_err_code("insert into t1 values(1, 15, 300);", 1062);
    tk.must_exec("commit");
    tk2.check("select * from t1", &["1 2 3", "10 20 500"]);
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert into t1 values(5, 6, 7)");
    tk.must_exec("select * from t1 where c1 in (5, 6) for update");
    tk.must_exec("select * from t1 where c1 = 6 for update");
    tk.must_exec("select * from t1 for update");
    tk.must_get_err_code("insert into t1 values(7, 6, 7)", 1062);
    tk.must_get_err_code("insert into t1 values(5, 8, 6)", 1062);
    tk.must_exec("select * from t1 where c2 = 8 for update");
    tk.must_exec("select * from t1 where c1 in (5, 8) for update");
    tk.must_exec("select * from t1 for update");
    tk.must_exec("commit");
    tk2.check("select * from t1", &["1 2 3", "5 6 7", "10 20 500"]);
    tk.must_exec("begin optimistic");
    tk.check("select * from t1 where c1 in (1) for update", &["1 2 3"]);
    tk.must_exec("insert into t1 values(10, 10, 10)");
    tk.must_get_err_code("commit", 1062);

}

/// Go `pessimistictest.TestPessimisticLockNonExistsKey` (pessimistic_test.go).
#[test]
fn test_pessimistic_lock_non_exists_key() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    let mut tk1 = store.new_tk();
    tk1.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (k int primary key, c int)");
    tk.must_exec("insert t values (1, 1), (3, 3), (5, 5)");
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert t values (8, 8)");
    tk.check("select c + 1 from t where k = 2 and c = 2 for update", &[]);
    let explain = tk.must_query("explain select c + 1 from t where k = 2 and c = 2 for update");
assert!(!explain[0][0].contains("UnionScan"), "{explain:?}");
    tk.check("select * from t where k in (4, 5, 7) for update", &["5 5"]);
    tk1.must_exec("begin pessimistic");
    tk1.must_get_err_code("select * from t where k = 2 for update nowait", 3572);
    tk1.must_get_err_code("select * from t where k = 4 for update nowait", 3572);
    tk1.must_get_err_code("select * from t where k = 7 for update nowait", 3572);
    tk.must_exec("rollback");
    tk1.must_exec("rollback");
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert t values (8, 8)");
    tk.must_exec("update t set c = c + 1 where k in (2, 3, 4) and c > 0");
    tk.must_exec("delete from t where k in (5, 6, 7) and c > 0");
    tk1.must_exec("begin pessimistic");
    tk1.must_get_err_code("select * from t where k = 2 for update nowait", 3572);
    tk1.must_get_err_code("select * from t where k = 6 for update nowait", 3572);
    tk.must_exec("rollback");
    tk1.must_exec("rollback");

}

/// Go `pessimistictest.TestGenerateColPointGet` (pessimistic_test.go).
#[test]
fn test_generate_col_point_get() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    
    for row_format in [1, 2] {
    tk.must_exec(&format!("set global tidb_row_format_version = {row_format}"));
    tk.must_exec("drop table if exists tu");
    tk.must_exec("CREATE TABLE `tu`(`x` int, `y` int, `z` int GENERATED ALWAYS AS (x + y) VIRTUAL, PRIMARY KEY (`x`), UNIQUE KEY `idz` (`z`))");
    tk.must_exec("insert into tu(x, y) values(1, 2);");
    tk.must_exec("begin pessimistic");
    tk.check("select * from tu where z = 3 for update", &["1 2 3"]);
    let mut tk2 = store.new_tk();
    tk2.must_exec("use test");
    tk2.must_exec("begin pessimistic");
    tk2.must_get_err_code("select * from tu where z = 3 for update nowait", 3572);
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert into tu(x, y) values(2, 2);");
    tk2.must_get_err_code("select * from tu where z = 4 for update nowait", 3572);
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk.check("select * from tu where z in (1, 3, 5) for update", &["1 2 3"]);
    tk2.must_exec("begin pessimistic");
    tk2.must_get_err_code("select x from tu where z in (3, 7, 9) for update nowait", 3572);
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert into tu(x, y) values(5, 6);");
    tk2.must_get_err_code("select * from tu where z = 11 for update nowait", 3572);
    tk.must_exec("commit");
    tk2.must_exec("commit");
    }
tk.must_exec("set global tidb_row_format_version = 2");

}

/// Go `pessimistictest.TestLazyUniquenessCheckWithInconsistentReadResult` (pessimistic_test.go).
#[test]
fn test_lazy_uniqueness_check_with_inconsistent_read_result() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk.must_exec("set @@tidb_constraint_check_in_place_pessimistic=0");
    
    tk.must_exec("create table t2 (id int primary key, uk int, unique key i1(uk))");
    tk.must_exec("insert into t2 values (1, 1)");
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert into t2 values (2, 1), (3, 3)");
    tk.check("select * from t2 use index(primary) for update", &["1 1", "2 1", "3 3"]);
    let err = tk.exec_to_err("commit");
    assert!(err.contains("Duplicate entry '1' for key 't2.i1'"), "{err:?}");
    tk.check("select * from t2 use index(primary)", &["1 1"]);
    tk.must_exec("admin check table t2");
    tk.must_exec("truncate table t2");
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert into t2 values (1, 1)");
    tk2.must_exec("insert into t2 values (2, 1)");
    tk.check("select * from t2 use index(primary) for update", &["1 1", "2 1"]);
    let err = tk.exec_to_err("commit");
    let reason = if cfg!(feature = "nextgen") { "reason=NotLockedKeyConflict" } else { "reason=LazyUniquenessCheck" };
assert!(err.contains(reason), "{err}");

}

/// Go `pessimistictest.TestForShareWithPromotionPlanCache` (pessimistic_test.go).
#[test]
fn test_for_share_with_promotion_plan_cache() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk1 = store.new_tk();
    tk1.must_exec("use test");
    tk.must_exec("use test");
    tk.must_exec("create table t(a int key, b int)");
    tk.must_exec("insert into t values(1, 10)");
    tk.must_exec("set innodb_lock_wait_timeout = 1");
    tk.must_exec("set @pk=1");
    tk.must_exec("set @@tidb_enable_noop_functions = 1");
    tk.must_exec("prepare st from 'select * from t where a=? for share'");
    tk.must_exec("execute st using @pk");
    tk.must_exec("begin");
    tk.must_exec("execute st using @pk");
    tk.check("select @@last_plan_from_cache", &["0"]);
    tk.must_exec("rollback");
    tk.must_exec("execute st using @pk");
    tk.must_exec("set @@tidb_enable_shared_lock_promotion = 1");
    tk.must_exec("execute st using @pk");
    tk.check("select @@last_plan_from_cache", &["0"]);
    tk.must_exec("execute st using @pk");
    tk.check("select @@last_plan_from_cache", &["1"]);
    tk.must_exec("begin");
    tk.must_exec("execute st using @pk");
    tk.check("select @@last_plan_from_cache", &["0"]);
    tk.must_exec("rollback");

}

/// Go `pessimistictest.TestConcurrentInsert` (pessimistic_test.go).
#[test]
fn test_concurrent_insert() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk1 = store.new_tk();
    let mut tk2 = store.new_tk();
    tk.must_exec("use test");
    tk1.must_exec("use test");
    tk2.must_exec("use test");
    tk.must_exec("drop table if exists tk");
    tk.must_exec("create table tk (c1 int primary key, c2 int)");
    tk.must_exec("insert tk values (1, 1)");
    tk.must_exec("create table tk1 (c1 int, c2 int)");
    tk1.must_exec("begin pessimistic");
    let for_update_ts_a = tk1.session().current_for_update_ts().value().max(tk1.session().current_tso().value());
    tk1.must_exec("select * from tk where c1 = 1 for update");
    let for_update_ts_b = tk1.session().current_for_update_ts().value().max(tk1.session().current_tso().value());
    assert_eq!(for_update_ts_b, for_update_ts_a);
    tk1.must_exec("select * from tk where c1 > 0 for update");
    let for_update_ts_c = tk1.session().current_for_update_ts().value().max(tk1.session().current_tso().value());
    assert!(for_update_ts_c > for_update_ts_b);
    tk2.must_exec("insert tk values (2, 2)");
    tk1.check("select * from tk for update", &["1 1", "2 2"]);
    tk2.must_exec("insert tk values (3, 3)");
    tk1.must_exec("update tk set c2 = c2 + 1");
    assert_eq!(tk1.affected_rows(), 3);
    tk2.must_exec("insert tk values (4, 4)");
    tk1.must_exec("delete from tk");
    assert_eq!(tk1.affected_rows(), 4);
    tk2.must_exec("insert tk values (5, 5)");
    tk1.must_exec("insert into tk1 select * from tk");
    assert_eq!(tk1.affected_rows(), 1);
    tk2.must_exec("insert tk values (6, 6)");
    tk1.must_exec("replace into tk1 select * from tk");
    assert_eq!(tk1.affected_rows(), 2);
    tk2.must_exec("insert tk values (7, 7)");
    tk1.must_exec("insert into tk1 select * from tk where c1 = 7");
    assert_eq!(tk1.affected_rows(), 1);
    tk1.must_exec("commit");

}

/// Go `pessimistictest.TestBankTransfer` (pessimistic_test.go).
#[test]
fn test_bank_transfer() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk.must_exec("drop table if exists accounts");
    tk.must_exec("create table accounts (id int primary key, c int)");
    tk.must_exec("insert accounts values (1, 100), (2, 100), (3, 100)");
    let (sync_tx, sync_rx) = std::sync::mpsc::sync_channel(0);
    tk.must_exec("begin pessimistic");
    tk.check("select * from accounts where id = 1 for update", &["1 100"]);
    let worker = std::thread::spawn(move || {
    tk2.must_exec("begin pessimistic");
    tk2.must_exec("select * from accounts where id = 2 for update");
    sync_rx.recv().unwrap();
    tk2.must_exec("select * from accounts where id = 3 for update");
    tk2.must_exec("update accounts set c = 50 where id = 2");
    tk2.must_exec("update accounts set c = 150 where id = 3");
    tk2.must_exec("commit");
    sync_rx.recv().unwrap();
    });
    sync_tx.send(()).unwrap();
    tk.check("select * from accounts where id = 2 for update", &["2 50"]);
    tk.must_exec("update accounts set c = 50 where id = 1");
    tk.must_exec("update accounts set c = 100 where id = 2");
    tk.must_exec("commit");
    sync_tx.send(()).unwrap();
    tk.check("select sum(c) from accounts", &["300"]);
    worker.join().unwrap();
}

/// Go `pessimistictest.TestOptimisticConflicts` (pessimistic_test.go).
#[test]
fn test_optimistic_conflicts() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk.must_exec("drop table if exists conflict");
    tk.must_exec("create table conflict (id int primary key, c int)");
    tk.must_exec("insert conflict values (1, 1)");
    tk.must_exec("begin pessimistic");
    tk.must_exec("select * from conflict where id = 1 for update");
    
    let worker = std::thread::spawn(move || {
    tk2.must_exec("update conflict set c = 3 where id = 1");
    tk2
    });
    std::thread::sleep(std::time::Duration::from_millis(10));
    tk.must_exec("update conflict set c = 2 where id = 1");
    tk.must_exec("commit");
    let mut tk2 = worker.join().unwrap();
    tk.check("select c from conflict where id = 1", &["3"]);
    tk.must_exec("begin pessimistic");
    tk.must_exec("update conflict set c = 4 where id = 1");
    tk2.must_exec("begin optimistic");
    tk2.must_exec("update conflict set c = 5 where id = 1");
    tk2.exec_to_err("commit");
    tk.must_exec("rollback");
    tk.must_exec("truncate table conflict");
    tk.must_exec("insert into conflict values (1, 2)");
    tk.must_exec("begin pessimistic");
    tk.must_exec("select * from conflict where id in (1, 2, 3)");
    tk2.must_exec("update conflict set c = c - 1");
    tk.check("select * from conflict where id = 1 for update", &["1 1"]);
    tk.must_exec("insert into conflict values (1, 999) on duplicate key update c = c + 2");
    tk.must_exec("commit");
    tk.check("select * from conflict", &["1 3"]);

}

/// Go `pessimistictest.TestSelectForUpdateWaitSeconds`.
#[test]
fn test_select_for_update_wait_seconds() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists tk");
    tk.must_exec("create table tk (c1 int primary key, c2 int)");
    tk.must_exec("insert into tk values(1,1),(2,2),(3,3),(4,4),(5,5)");
    let mut tk2 = store.new_tk();
    let mut others = [store.new_tk(), store.new_tk(), store.new_tk()];
    tk2.must_exec("use test");
    tk2.must_exec("begin pessimistic");
    for other in &mut others {
        other.must_exec("use test");
        other.must_exec("begin pessimistic");
    }
    tk2.must_exec("select * from tk where c1 = 5 or c1 = 1 for update");
    let start = std::time::Instant::now();
    let workers: Vec<_> = others.into_iter().zip([
        "select * from tk where c1 = 1 for update wait 1",
        "select * from tk where c1 >= 1 for update wait 1",
        "select c2 from tk where c1 in (1, 5) for update wait 1",
    ]).map(|(mut tk, sql)| std::thread::spawn(move || {
        let err = tk.exec_to_err(sql);
        (tk, err)
    })).collect();
    let mut others = Vec::new();
    for worker in workers {
        let (tk, err) = worker.join().unwrap();
        assert_eq!(err, "[tikv:1205]Lock wait timeout exceeded; try restarting transaction");
        others.push(tk);
    }
    assert!(start.elapsed().as_secs_f64() < 45.0);
    tk2.must_exec("commit");
    for tk in &mut others { tk.must_exec("rollback"); }
}

/// Go `pessimistictest.TestForShareWithPromotion`.
#[test]
fn test_for_share_with_promotion() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk1 = store.new_tk();
    tk1.must_exec("use test");
    tk.must_exec("use test");
    tk.must_exec("create table t(a int key, b int)");
    tk.must_exec("insert into t values(1, 10)");
    tk.must_exec("set innodb_lock_wait_timeout = 1");
    for (noop, upgrade) in [(false, false), (false, true), (true, false), (true, true)] {
        tk.must_exec(&format!("set @@tidb_enable_noop_functions = {noop}"));
        tk.must_exec(&format!("set @@tidb_enable_shared_lock_promotion = {upgrade}"));
        tk1.must_exec("begin");
        tk1.check("select * from t for update", &["1 10"]);
        tk.must_exec("begin");
        if upgrade {
            for sql in ["select * from t where a = 1 for share nowait", "select * from t for share nowait"] {
                tk.must_contain_err_msg(sql, "could not be acquired immediately and NOWAIT is set");
            }
            for sql in ["select * from t where a = 1 for share", "select * from t for share"] {
                tk.must_contain_err_msg(sql, "Lock wait timeout exceeded; try restarting transaction");
            }
        } else if noop {
            for sql in ["select * from t where a = 1 for share nowait", "select * from t where a = 1 for share", "select * from t for share", "select * from t"] {
                tk.check(sql, &["1 10"]);
            }
        } else {
            for sql in ["select * from t where a = 1 for share nowait", "select * from t for share", "select * from t for share nowait"] {
                tk.must_contain_err_msg(sql, "use tidb_enable_noop_functions to enable");
            }
        }
        tk.must_exec("rollback");
        tk1.must_exec("rollback");
    }
}
/// Go `pessimistictest.TestPessimisticSerializable` (pessimistic_test.go).
#[test]
fn test_pessimistic_serializable() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    let mut tk1 = store.new_tk();
    tk1.must_exec("use test");
    tk.must_exec("set tidb_txn_mode = 'pessimistic'");
    tk1.must_exec("set tidb_txn_mode = 'pessimistic'");
    tk.must_exec("drop table if exists test;");
    tk.must_exec("create table test (id int not null primary key, value int);");
    tk.must_exec("insert into test (id, value) values (1, 10);");
    tk.must_exec("insert into test (id, value) values (2, 20);");
    tk.must_exec("set tidb_skip_isolation_level_check = 1");
    tk1.must_exec("set tidb_skip_isolation_level_check = 1");
    tk.must_exec("set tx_isolation = 'SERIALIZABLE'");
    tk1.must_exec("set tx_isolation = 'SERIALIZABLE'");
    tk.must_exec("begin");
    tk1.must_exec("begin");
    tk.check("select * from test where value = 30;", &[]);
    tk1.must_exec("insert into test (id, value) values(3, 30);");
    tk1.must_exec("commit");
    tk.check("select * from test where mod(value, 3) = 0;", &[]);
    tk.must_exec("commit");
    tk.must_exec("truncate table test;");
    tk.must_exec("insert into test (id, value) values (1, 10);");
    tk.must_exec("insert into test (id, value) values (2, 20);");
    tk.must_exec("begin;");
    tk1.must_exec("begin;");
    tk.must_exec("update test set value = value + 10;");
    
    std::thread::scope(|scope| {
let worker = scope.spawn(|| {
    let _ = tk1.exec("delete from test where value = 20;");
    });
    tk.must_exec("commit;");
    worker.join().unwrap();
});
    tk1.must_exec("rollback;");
    tk.must_exec("truncate table test;");
    tk.must_exec("insert into test (id, value) values (1, 10);");
    tk.must_exec("insert into test (id, value) values (2, 20);");
    tk.must_exec("begin;");
    tk1.must_exec("begin;");
    tk.check("select * from test where id = 1;", &["1 10"]);
    tk1.check("select * from test where id = 1;", &["1 10"]);
    tk.must_exec("update test set value = 11 where id = 1;");
    std::thread::scope(|scope| {
let worker = scope.spawn(|| {
    let _ = tk1.exec("update test set value = 11 where id = 1;");
    });
    tk.must_exec("commit;");
    worker.join().unwrap();
});
    tk1.must_exec("rollback;");
    tk.must_exec("truncate table test;");
    tk.must_exec("insert into test (id, value) values (1, 10);");
    tk.must_exec("insert into test (id, value) values (2, 20);");
    tk.must_exec("begin;");
    tk1.must_exec("begin;");
    tk.check("select * from test where id = 1;", &["1 10"]);
    tk1.check("select * from test where id = 1;", &["1 10"]);
    tk1.check("select * from test where id = 2;", &["2 20"]);
    tk1.must_exec("update test set value = 12 where id = 1;");
    tk1.must_exec("update test set value = 18 where id = 1;");
    tk1.must_exec("commit;");
    tk.check("select * from test where id = 2;", &["2 20"]);
    tk.must_exec("commit;");
    tk.must_exec("truncate table test;");
    tk.must_exec("insert into test (id, value) values (1, 10);");
    tk.must_exec("insert into test (id, value) values (2, 20);");
    tk.must_exec("begin;");
    tk1.must_exec("begin;");
    tk.check("select * from test where mod(value, 5) = 0;", &["1 10", "2 20"]);
    tk1.must_exec("update test set value = 12 where value = 10;");
    tk1.must_exec("commit;");
    tk.check("select * from test where mod(value, 3) = 0;", &[]);
    tk.must_exec("commit;");
    tk.must_exec("truncate table test;");
    tk.must_exec("insert into test (id, value) values (1, 10);");
    tk.must_exec("insert into test (id, value) values (2, 20);");
    tk.must_exec("begin;");
    tk1.must_exec("begin;");
    tk.check("select * from test where id = 1;", &["1 10"]);
    tk1.check("select * from test;", &["1 10", "2 20"]);
    tk1.must_exec("update test set value = 12 where id = 1;");
    tk1.must_exec("update test set value = 18 where id = 1;");
    tk1.must_exec("commit;");
    let _ = tk.exec("delete from test where value = 20;");
    tk.must_exec("rollback;");
    tk.must_exec("truncate table test;");
    tk.must_exec("insert into test (id, value) values (1, 10);");
    tk.must_exec("insert into test (id, value) values (2, 20);");
    tk.must_exec("begin;");
    tk1.must_exec("begin;");
    tk.check("select * from test where id in (1,2);", &["1 10", "2 20"]);
    tk1.check("select * from test where id in (1,2);", &["1 10", "2 20"]);
    tk1.must_exec("update test set value = 11 where id = 1;");
    tk1.must_exec("update test set value = 21 where id = 2;");
    tk.must_exec("commit;");
    tk1.must_exec("commit;");
    tk.check("select * from test;", &["1 11", "2 21"]);
    tk.must_exec("truncate table test;");
    tk.must_exec("insert into test (id, value) values (1, 10);");
    tk.must_exec("insert into test (id, value) values (2, 20);");
    tk.must_exec("begin;");
    tk1.must_exec("begin;");
    tk.check("select * from test where mod(value, 3) = 0;", &[]);
    tk1.check("select * from test where mod(value, 5) = 0;", &["1 10", "2 20"]);
    tk.must_exec("insert into test (id, value) values(3, 30);");
    tk1.must_exec("insert into test (id, value) values(4, 60);");
    tk.must_exec("commit;");
    tk1.must_exec("commit;");
    tk.check("select * from test where mod(value, 3) = 0;", &["3 30", "4 60"]);

}

/// Go `pessimistictest.TestPessimisticReadCommitted` (pessimistic_test.go).
#[test]
fn test_pessimistic_read_committed() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    let mut tk1 = store.new_tk();
    tk1.must_exec("use test");
    tk.must_exec("set tidb_txn_mode = 'pessimistic'");
    tk1.must_exec("set tidb_txn_mode = 'pessimistic'");
    tk.must_exec("set @@tidb_pessimistic_txn_fair_locking = 0");
    tk1.must_exec("set @@tidb_pessimistic_txn_fair_locking = 0");
    tk.must_exec("drop table if exists t;");
    tk.must_exec("create table t(i int key);");
    tk.must_exec("insert into t values (1);");
    tk.check("select * from t", &["1"]);
    tk.must_exec("begin;");
    tk1.must_exec("begin;");
    tk.must_exec("update t set i = -i;");
    
    std::thread::scope(|scope| {
let worker = scope.spawn(|| {
    tk1.must_exec("update t set i = -i;");
    });
    tk.must_exec("commit;");
    worker.join().unwrap();
});
    tk1.must_exec("commit;");
    tk.must_exec("set tx_isolation = 'READ-COMMITTED'");
    tk1.must_exec("set tx_isolation = 'READ-COMMITTED'");
    tk.must_exec("drop table if exists t;");
    tk.must_exec("create table t(i int key);");
    tk.must_exec("insert into t values (1);");
    tk.check("select * from t", &["1"]);
    tk.must_exec("begin;");
    tk1.must_exec("begin;");
    tk.must_exec("update t set i = -i;");
    std::thread::scope(|scope| {
let worker = scope.spawn(|| {
    tk1.must_exec("update t set i = -i;");
    });
    tk.must_exec("commit;");
    worker.join().unwrap();
});
    tk1.must_exec("commit;");
    tk.must_exec("drop table if exists t;");
    tk.must_exec("create table t(i int key, j int unique key, k int, l int, m int, key (k));");
    tk.must_exec("insert into t values (1, 1, 1, 1, 1);");
    tk.must_exec("set tx_isolation = 'REPEATABLE-READ'");
    tk1.must_exec("set tx_isolation = 'REPEATABLE-READ'");
    tk.must_exec("set transaction isolation level read committed");
    tk.must_exec("begin;");
    tk.check("select l from t where l = 1", &["1"]);
    tk1.must_exec("update t set l = l + 1 where l = 1;");
    tk.check("select l from t where l = 2", &["2"]);
    tk1.must_exec("update t set l = l + 1 where l = 2;");
    tk.check("select l from t where l = 3", &["3"]);
    tk.check("select k from t where k = 1", &["1"]);
    tk1.must_exec("update t set k = k + 1 where k = 1;");
    tk.check("select k from t where k = 2", &["2"]);
    tk1.must_exec("update t set k = k + 1 where k = 2;");
    tk.check("select k from t where k = 3", &["3"]);
    tk.check("select m from t where k = 3", &["1"]);
    tk1.must_exec("update t set m = m + 1 where k = 3;");
    tk.check("select m from t where k = 3", &["2"]);
    tk1.must_exec("update t set m = m + 1 where k = 3;");
    tk.check("select m from t where k = 3", &["3"]);
    tk.check("select m from t where i = 1", &["3"]);
    tk1.must_exec("update t set m = m + 1 where i = 1;");
    tk.check("select m from t where i = 1", &["4"]);
    tk1.must_exec("update t set m = m + 1 where j = 1;");
    tk.check("select m from t where j = 1", &["5"]);
    tk1.must_exec("insert into t values (2, 2, 2, 2, 2);");
    tk.check("select m from t where i in (1, 2)", &["5", "2"]);
    tk1.must_exec("update t set m = m + 1 where i in (1, 2);");
    tk.check("select m from t where i in (1, 2)", &["6", "3"]);
    tk1.must_exec("update t set m = m + 1 where j in (1, 2);");
    tk.check("select m from t where j in (1, 2)", &["7", "4"]);
    tk.must_exec("commit;");
    tk.must_exec("set tx_isolation = 'READ-COMMITTED'");
    tk.must_exec("begin optimistic;");
    tk.check("select m from t where j in (1, 2)", &["7", "4"]);
    tk.must_exec("begin pessimistic;");
    tk.check("select m from t where j in (1, 2)", &["7", "4"]);
    tk.must_exec("commit;");

}

/// Go `pessimistictest.TestWaitLockKill`.
#[test]
fn test_wait_lock_kill() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk.must_exec("drop table if exists test_kill");
    tk.must_exec("create table test_kill (id int primary key, c int)");
    tk.must_exec("insert test_kill values (1, 1)");
    tk.must_exec("begin pessimistic");
    tk2.must_exec("set innodb_lock_wait_timeout = 50");
    tk2.must_exec("begin pessimistic");
    tk.must_query("select * from test_kill where id = 1 for update");
    let killer = tk2.session().sql_killer();
    let worker = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(500));
        killer.send_kill_signal(tidb_sqlkiller::KillSignal::QueryInterrupted);
        assert_eq!(killer.handle_signal().unwrap().code().value(), 1317);
    });
    tk2.must_get_err_code("update test_kill set c = c + 1 where id = 1", 1317);
    worker.join().unwrap();
    tk.must_exec("rollback");
}

/// Go `pessimistictest.setLockTTL` and `lockTTL.restore`.
struct LockTtl(u64);
impl LockTtl {
    /// Go `pessimistictest.setLockTTL`.
    fn set(value: u64) -> Self {
        Self(tikv_client::MANAGED_LOCK_TTL.swap(value, std::sync::atomic::Ordering::SeqCst))
    }
}
impl Drop for LockTtl {
    /// Go `pessimistictest.lockTTL.restore`.
    fn drop(&mut self) {
        tikv_client::MANAGED_LOCK_TTL.store(self.0, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Go `pessimistictest.TestKillStopTTLManager`.
#[test]
fn test_kill_stop_ttl_manager() {
    let _ttl = LockTtl::set(300);
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk.must_exec("drop table if exists test_kill");
    tk.must_exec("create table test_kill (id int primary key, c int)");
    tk.must_exec("insert test_kill values (1, 1)");
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk.must_query("select * from test_kill where id = 1 for update");
    let killer = tk.session().sql_killer();
    killer.send_kill_signal(tidb_sqlkiller::KillSignal::QueryInterrupted);
    assert_eq!(killer.handle_signal().unwrap().code().value(), 1317);
    tk2.must_exec("update test_kill set c = c + 1 where id = 1");
    killer.reset();
    tk.must_exec("rollback");
    tk2.must_exec("rollback");
}

/// Go `pessimistictest.TestKillWaitLockTxn`.
#[test]
fn test_kill_wait_lock_txn() {
    let _ttl = LockTtl::set(300);
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk.must_exec("drop table if exists test_kill");
    tk.must_exec("create table test_kill (id int primary key, c int)");
    tk.must_exec("insert test_kill values (1, 1)");
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk.must_query("select * from test_kill where id = 1 for update");
    let worker = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(20));
        tk2.must_exec("update test_kill set c = c + 1 where id = 1");
        tk2
    });
    std::thread::sleep(std::time::Duration::from_millis(100));
    let killer = tk.session().sql_killer();
    killer.send_kill_signal(tidb_sqlkiller::KillSignal::QueryInterrupted);
    assert_eq!(killer.handle_signal().unwrap().code().value(), 1317);
    let mut tk2 = worker.join().unwrap();
    let _ = tk.exec("rollback");
    killer.reset();
    tk.must_exec("rollback");
    tk2.must_exec("rollback");
}
/// Go `pessimistictest.TestLazyUniquenessCheck` (pessimistic_test.go).
#[test]
fn test_lazy_uniqueness_check() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk.must_exec("create table t(id int primary key, v int)");
    tk.must_exec("set @@tidb_constraint_check_in_place_pessimistic=0");
    
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert into t values (1, 1)");
    tk.check("select * from t for update", &["1 1"]);
    tk.must_exec("commit");
    tk.must_exec("admin check table t");
    tk.must_exec("truncate table t");
    tk2.must_exec("insert into t values (2, 1)");
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert into t values (2, 2)");
    tk2.must_exec("delete from t");
    tk.check("select * from t for update", &["2 2"]);
    tk.must_exec("commit");
    tk.must_exec("admin check table t");
    tk.must_exec("create table t2 (id int primary key, uk int, unique key i1(uk))");
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert into t2 values (1, 1)");
    tk.must_exec("update t2 set uk = uk + 1");
    
    tk2.must_exec("begin pessimistic");
    let worker = std::thread::spawn(move || {
    tk2.must_exec("update t2 set uk = uk + 10 where id = 1");
    tk2.must_exec("commit");
tk2
    });
    std::thread::sleep(std::time::Duration::from_millis(500));
    tk.must_exec("commit");
    let mut tk2 = worker.join().unwrap();
    tk.check("select * from t2", &["1 12"]);
    tk.must_exec("admin check table t");
    tk.must_exec("create table t3 (id int primary key, sk int, key i1(sk))");
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert into t3 values (1, 1)");
    tk2.must_exec("insert into t3 values (1, 2)");
    let err = tk.exec_to_err("commit");
    assert!(err.contains("[kv:9007]Write conflict"), "{err:?}");
    assert!(err.contains("reason=LazyUniquenessCheck"), "{err}");
    tk.must_exec("create table t4 (id int primary key, v int, key i1(v))");
    tk.must_exec("insert into t4 values (1, 1)");
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert into t4 values (1, 2), (2, 2)");
    tk.check("select * from t4 order by id", &["1 2", "2 2"]);
    let err = tk.exec_to_err("delete from t4 where id = 1");
    assert!(err.contains("transaction aborted because lazy uniqueness check is enabled and an error occurred: [kv:1062]Duplicate entry '1' for key 't4.PRIMARY'"), "{err:?}");
    tk.must_exec("commit");
    tk.must_exec("admin check table t4");
    tk.check("select * from t4 order by id", &["1 1"]);
    tk.must_exec("create table t5 (id int primary key, uk int, unique key i1(uk))");
    tk.must_exec("insert into t5 values (1, 1), (2, 2)");
    tk.must_exec("begin pessimistic");
    tk.must_exec("update t5 set uk = 2 where id = 1");
    tk2.must_exec("delete from t5 where uk = 2");
    tk.must_exec("select * from t5 for update");
    let err = tk.exec_to_err("commit");
    assert!(err.contains("[kv:9007]Write conflict"), "{err:?}");
    tk.must_exec("truncate table t5");
    tk.must_exec("insert into t5 values (1, 1)");
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert into t5 values (2, 1)");
    let err = tk.exec_to_err("delete from t5");
    assert!(err.contains("transaction aborted because lazy uniqueness check is enabled and an error occurred: [kv:1062]Duplicate entry '1' for key 't5.i1'"), "{err:?}");
    assert!(!tk.session().in_transaction());
    tk.must_exec("truncate table t5");
    tk.must_exec("insert into t5 values (1, 1), (2, 3)");
    tk.must_exec("begin pessimistic");
    tk.must_exec("update t5 set uk = 3 where id = 1");
    let err = tk.exec_to_err("commit");
    assert!(err.contains("Duplicate entry '3' for key 't5.i1'"), "{err:?}");
    tk.must_exec("admin check table t5");
    tk.must_exec("truncate table t5");
    tk.must_exec("insert into t5 values (1, 1)");
    tk.must_exec("begin pessimistic");
    tk.must_exec("update t5 set uk = 3 where id = 1");
    tk2.must_exec("insert into t5 values (2, 3)");
    let err = tk.exec_to_err("commit");
    assert!(err.contains("[kv:9007]Write conflict"), "{err:?}");
    tk.must_exec("admin check table t5");
    tk.must_exec("truncate table t5");
    tk.must_exec("insert into t5 values (1, 1), (2, 3)");
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert into t5 values (3, 1) on duplicate key update uk = 3");
    let err = tk.exec_to_err("commit");
    assert!(err.contains("Duplicate entry '3' for key 't5.i1'"), "{err:?}");
    tk.must_exec("admin check table t5");
    tk.must_exec("truncate table t5");
    tk.must_exec("insert into t5 values (1, 1)");
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert into t5 values (3, 1) on duplicate key update uk = 3");
    tk2.must_exec("insert into t5 values (2, 3)");
    let err = tk.exec_to_err("commit");
    assert!(err.contains("[kv:9007]Write conflict"), "{err:?}");
    tk.must_exec("admin check table t5");

}

/// Go `pessimistictest.TestLazyUniquenessCheckWithStatementRetry` (pessimistic_test.go).
#[test]
fn test_lazy_uniqueness_check_with_statement_retry() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk.must_exec("create table t5(id int primary key, uk int, unique key i1(uk))");
    tk.must_exec("set @@tidb_constraint_check_in_place_pessimistic=0");
    
    tk.must_exec("insert into t5 values (1, 1)");
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert into t5 values (3, 3)");
    tk2.must_exec("insert into t5 values (2, 3)");
    let err = tk.exec_to_err("update t5 set id = 10 where uk = 3");
    assert!(err.contains("transaction aborted because lazy uniqueness"), "{err:?}");
    assert!(err.contains("Duplicate entry '3' for key 't5.i1'"), "{err:?}");
    assert!(!tk.session().in_transaction());
    tk.must_exec("admin check table t5");
    tk.must_exec("truncate table t5");
    tk.must_exec("insert into t5 values (1, 1)");
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert into t5 values (3, 3)");
    tk2.must_exec("insert into t5 values (2, 3)");
    let err = tk.exec_to_err("update t5 set id = id + 10");
    assert!(err.contains("Duplicate entry '3' for key 't5.i1'"), "{err:?}");
    assert!(!tk.session().in_transaction());
    tk.must_exec("admin check table t5");

}

/// Go `pessimistictest.TestPlanCacheSchemaChange` (pessimistic_test.go).
#[test]
fn test_plan_cache_schema_change() {
    
    let store = realtikv!();
    let mut tmp = store.new_tk();
    tmp.must_exec("set tidb_enable_prepared_plan_cache=ON");
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    let mut tk3 = store.new_tk();
    
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk3.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (id int primary key, v int, unique index iv (v), vv int)");
    tk.must_exec("insert into t values(1, 1, 1), (2, 2, 2), (4, 4, 4)");
    tk.must_exec("set global tidb_ddl_enable_fast_reorg = 0");
    tk.must_exec("prepare update_stmt from 'update t set vv = vv + 1 where v = ?'");
    tk.must_exec("set @v = 1");
    tk.must_exec("execute update_stmt using @v");
    let stmt = tk2.prepare("update t set vv = vv + 1 where v = ?");
    tk2.execute_prepared(&stmt, &[tidb_param::PreparedValue::SignedLongLong(1)]).unwrap();
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk3.must_exec("alter table t drop index iv");
    tk3.must_exec("update t set v = 3 where v = 2");
    tk3.must_exec("update t set v = 5 where v = 4");
    tk.must_exec("set @v = 2");
    tk.must_exec("execute update_stmt using @v");
    assert_eq!(tk.affected_rows(), 0);
assert_eq!(tk.session().last_insert_id(), 0);
    tk.check("select @@last_plan_from_cache", &["0"]);
    tk.must_exec("set @v = 3");
    tk.must_exec("execute update_stmt using @v");
    assert_eq!(tk.affected_rows(), 1);
assert_eq!(tk.session().last_insert_id(), 0);
    tk.check("select @@last_plan_from_cache", &["1"]);
    tk2.execute_prepared(&stmt, &[tidb_param::PreparedValue::SignedLongLong(4)]).unwrap();
    assert_eq!(tk2.affected_rows(), 0);
assert_eq!(tk2.session().last_insert_id(), 0);
    tk2.check("select @@last_plan_from_cache", &["0"]);
    tk2.execute_prepared(&stmt, &[tidb_param::PreparedValue::SignedLongLong(5)]).unwrap();
    assert_eq!(tk2.affected_rows(), 1);
assert_eq!(tk2.session().last_insert_id(), 0);
    tk2.check("select @@last_plan_from_cache", &["1"]);
    tk.must_exec("commit");
    tk2.must_exec("commit");
    tk.check("select * from t", &["1 1 3", "2 3 3", "4 5 5"]);

}

/// Go `pessimistictest.TestPessimisticLockOnPartition` (pessimistic_test.go).
#[test]
fn test_pessimistic_lock_on_partition() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("create table if not exists forupdate_on_partition (\n  age int not null primary key,\n  nickname varchar(20) not null,\n  gender int not null default 0,\n  first_name varchar(30) not null default '',\n  last_name varchar(20) not null default '',\n  full_name varchar(60) as (concat(first_name, ' ', last_name)),\n  index idx_nickname (nickname)\n) partition by range (age) (\n  partition child values less than (18),\n  partition young values less than (30),\n  partition middle values less than (50),\n  partition old values less than (123)\n);");
    tk.must_exec("insert into forupdate_on_partition (`age`, `nickname`) values (25, 'cosven');");
    let mut tk1 = store.new_tk();
    tk1.must_exec("use test");
    tk.must_exec("begin pessimistic");
    tk.check("select * from forupdate_on_partition where age=25 for update", &["25 cosven 0    "]);
    tk1.must_exec("begin pessimistic");
    let (tx, rx) = std::sync::mpsc::sync_channel(5);
    let worker_tx = tx.clone();
let worker = std::thread::spawn(move || {
    tk1.must_exec("update forupdate_on_partition set first_name='sw' where age=25");
    worker_tx.send(0).unwrap();
    tk1.must_exec("commit");
    worker_tx.send(0).unwrap();
    tk1
});
    std::thread::sleep(std::time::Duration::from_millis(50));
    tx.send(1).unwrap();
    tk.must_exec("commit");
    assert_eq!(rx.recv().unwrap(), 1);
    assert_eq!(rx.recv().unwrap(), 0);
    rx.recv().unwrap();
let mut tk1 = worker.join().unwrap();
    tk.must_exec("begin pessimistic");
    tk.must_exec("update forupdate_on_partition set first_name='sw' where age=25");
    tk1.must_exec("begin pessimistic");
    let worker_tx = tx.clone();
let worker = std::thread::spawn(move || {
    tk1.must_exec("update forupdate_on_partition set first_name = 'xxx' where age=25");
    worker_tx.send(0).unwrap();
    tk1.must_exec("commit");
    worker_tx.send(0).unwrap();
    tk1
});
    std::thread::sleep(std::time::Duration::from_millis(50));
    tx.send(1).unwrap();
    tk.must_exec("commit");
    assert_eq!(rx.recv().unwrap(), 1);
    assert_eq!(rx.recv().unwrap(), 0);
    rx.recv().unwrap();
let mut tk1 = worker.join().unwrap();

}


/// Go `pessimistictest.TestAsyncRollBackNoWait` (pessimistic_test.go).
#[test]
#[ignore = "tk3 is blocking because tk2 didn't rollback itself (Go t.Skip)"]
fn test_async_roll_back_no_wait() {}
/// Go `pessimistictest.TestInnodbLockWaitTimeout` (pessimistic_test.go).
#[test]
fn test_innodb_lock_wait_timeout() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists tk");
    tk.must_exec("create table tk (c1 int primary key, c2 int)");
    tk.must_exec("insert into tk values(1,1),(2,2),(3,3),(4,4),(5,5)");
    tk.must_exec("set global innodb_lock_wait_timeout = 3");
    tk.check("show variables like \"innodb_lock_wait_timeout\"", &["innodb_lock_wait_timeout 50"]);
    let mut tk2 = store.new_tk();
    tk2.must_exec("use test");
    tk2.check("show variables like \"innodb_lock_wait_timeout\"", &["innodb_lock_wait_timeout 3"]);
    tk2.must_exec("set innodb_lock_wait_timeout = 2");
    tk2.check("show variables like \"innodb_lock_wait_timeout\"", &["innodb_lock_wait_timeout 2"]);
    tk2.must_exec("set innodb_lock_wait_timeout = 1073741825");
    tk2.check("show variables like \"innodb_lock_wait_timeout\"", &["innodb_lock_wait_timeout 1073741824"]);
    tk2.must_exec("set innodb_lock_wait_timeout = 2");
    let mut tk3 = store.new_tk();
    tk3.must_exec("use test");
    tk3.check("show variables like \"innodb_lock_wait_timeout\"", &["innodb_lock_wait_timeout 3"]);
    tk3.must_exec("set innodb_lock_wait_timeout = 1");
    tk3.check("show variables like \"innodb_lock_wait_timeout\"", &["innodb_lock_wait_timeout 1"]);
    tk2.must_exec("set @@autocommit = 0");
    tk3.must_exec("set @@autocommit = 0");
    let mut tk4 = store.new_tk();
    tk4.must_exec("use test");
    tk4.check("show variables like \"innodb_lock_wait_timeout\"", &["innodb_lock_wait_timeout 3"]);
    tk4.must_exec("set @@autocommit = 0");
    tk2.must_exec("begin pessimistic");
    tk2.must_exec("select * from tk where c1 = 1 for update");
    let mut tk5 = store.new_tk();
let worker3 = std::thread::spawn(move || {

    tk3.must_exec("begin pessimistic");
 tk3.must_get_err_code("select * from tk where c1 = 1 for update", 1205);
 tk3.must_exec("commit"); tk3
});
    let worker5 = std::thread::spawn(move || {
 tk5.must_exec("use test"); tk5.must_exec("set innodb_lock_wait_timeout = 2");
 tk5.must_exec("begin pessimistic");
 tk5.must_get_err_code("update tk set c2 = c2 - 1 where c1 = 1", 1205);
 tk5.must_exec("rollback");
});
    let mut tk3 = worker3.join().unwrap();
worker5.join().unwrap();
    tk4.must_exec("begin pessimistic");
    tk4.must_exec("update tk set c2 = c2 + 1 where c1 = 2");
    tk2.must_exec("set innodb_lock_wait_timeout = 1");
    tk2.check("show variables like \"innodb_lock_wait_timeout\"", &["innodb_lock_wait_timeout 1"]);
    let start = std::time::Instant::now();
    let err = tk2.exec("delete from tk where c1 = 2");
    assert!(start.elapsed() >= std::time::Duration::from_secs(1));
    assert!(start.elapsed() < std::time::Duration::from_secs(3));
    assert_eq!(err.unwrap_err(), "[tikv:1205]Lock wait timeout exceeded; try restarting transaction");
    tk4.must_exec("commit");
    tk.check("show variables like \"innodb_lock_wait_timeout\"", &["innodb_lock_wait_timeout 50"]);
    tk.check("select * from tk where c1 = 2", &["2 3"]);
    tk2.must_exec("update tk set c2 = c2 + 2 where c1 = 2");
    tk2.check("select * from tk where c1 = 2", &["2 5"]);
    tk3.must_exec("begin pessimistic");
    tk3.must_exec("select * from tk where c1 = 3 for update");
    let start = std::time::Instant::now();
    let err = tk2.exec("delete from tk where c1 = 3");
    assert!(start.elapsed() >= std::time::Duration::from_secs(1));
    assert!(start.elapsed() < std::time::Duration::from_secs(3));
    assert_eq!(err.unwrap_err(), "[tikv:1205]Lock wait timeout exceeded; try restarting transaction");
    tk2.must_exec("commit");
    tk3.must_exec("commit");
    tk.check("select * from tk where c1 = 1", &["1 1"]);
    tk.check("select * from tk where c1 = 2", &["2 5"]);
    tk.check("select * from tk where c1 = 3", &["3 3"]);
    tk.check("select * from tk where c1 = 4", &["4 4"]);
    tk.check("select * from tk where c1 = 5", &["5 5"]);
    tk.must_exec("drop table if exists tk");
    tk4.must_exec("commit");
    

}


/// Go `pessimistictest.mustExecAsync`: execute on the testkit's worker.
fn must_exec_async(mut tk: super::realtikvtest::Tk, sql: &str) -> std::sync::mpsc::Receiver<super::realtikvtest::Tk> {
    let sql = sql.to_owned();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        tk.must_exec(&sql);
        let _ = tx.send(tk);
    });
    rx
}

/// Go `pessimistictest.mustTimeout`.
fn must_timeout<T>(rx: &std::sync::mpsc::Receiver<T>, millis: u64) {
    assert!(matches!(rx.recv_timeout(std::time::Duration::from_millis(millis)), Err(std::sync::mpsc::RecvTimeoutError::Timeout)), "received signal when not expected");
}

/// Go `pessimistictest.mustRecv`.
fn must_recv<T>(rx: std::sync::mpsc::Receiver<T>) -> T {
    rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap_or_else(|_| panic!("signal not received after waiting for one second"))
}

/// Go `pessimistictest.mustLocked`.
fn must_locked(store: &super::realtikvtest::RealTiKV, sql: &str) {
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("begin pessimistic");
    tk.must_get_err_code(sql, 3572);
    tk.must_exec("rollback");
}
/// Go `pessimistictest.TestFairLockingBasic` (pessimistic_test.go).
#[test]
fn test_fair_locking_basic() {
    
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    let mut tk2 = store.new_tk();
    tk2.must_exec("use test");
    tk.must_exec("set @@tidb_pessimistic_txn_fair_locking = 1");
    tk.must_exec("create table t (id int primary key, k int unique, v int)");
    tk.must_exec("insert into t values (1, 1, 1)");
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk2.must_exec("update t set v = v + 1 where id = 1");
    let res = must_exec_async(tk, "update t set v = v + 1 where id = 1");
    must_timeout(&res, 100);
    tk2.must_exec("rollback");
    let mut tk = must_recv(res);
    tk.check("select * from t", &["1 1 2"]);
    tk.must_exec("commit");
    tk.check("select * from t", &["1 1 2"]);
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk2.must_exec("update t set v = v + 1 where id = 1");
    let res = must_exec_async(tk, "update t set v = v + 1 where id = 1");
    must_timeout(&res, 100);
    tk2.must_exec("commit");
    let mut tk = must_recv(res);
    tk.check("select * from t", &["1 1 4"]);
    tk.must_exec("commit");
    tk.check("select * from t", &["1 1 4"]);
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk2.must_exec("update t set v = v + 1 where id = 1");
    let res = must_exec_async(tk, "update t set v = v + 1 where k = 1");
    must_timeout(&res, 100);
    tk2.must_exec("commit");
    let mut tk = must_recv(res);
    tk.check("select * from t", &["1 1 6"]);
    tk.must_exec("commit");
    tk.check("select * from t", &["1 1 6"]);
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk2.must_exec("update t set v = v + 1 where id = 1");
    tk2.check("select * from t where k = 2 for update", &[]);
    let res = must_exec_async(tk, "update t set k = k + 1 where id = 1");
    must_timeout(&res, 100);
    tk2.must_exec("commit");
    let mut tk = must_recv(res);
    tk.check("select * from t", &["1 2 7"]);
    tk.must_exec("commit");
    tk.check("select * from t", &["1 2 7"]);
    let mut tk3 = store.new_tk();
    tk3.must_exec("use test");
    tk.must_exec("insert into t values (3, 3, 4), (4, 4, 4)");
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk2.must_exec("update t set v = v + 1 where id = 3");
    let res = must_exec_async(tk, "with c as (select /*+ MERGE() */ * from t where id = 3 for update) update c join t on c.v = t.v set t.v = t.v + 1");
    must_timeout(&res, 100);
    tk3.must_exec("insert into t values (5, 5, 5)");
    tk2.must_exec("commit");
    let mut tk = must_recv(res);
    tk.must_exec("commit");
    tk.check("select * from t", &["1 2 7", "3 3 6", "4 4 4", "5 5 6"]);
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk2.must_exec("select * from t where id = 4 for update");
    let res = must_exec_async(tk, "update t set v = v + 1");
    must_timeout(&res, 100);
    tk3.must_exec("insert into t values (6, 6, 6)");
    tk2.must_exec("commit");
    let mut tk = must_recv(res);
    tk.check("select * from t", &["1 2 8", "3 3 7", "4 4 5", "5 5 7", "6 6 7"]);
    tk.must_exec("commit");

}

/// Go `pessimistictest.TestFairLockingInsert` (pessimistic_test.go).
#[test]
fn test_fair_locking_insert() {
    
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    let mut tk2 = store.new_tk();
    tk2.must_exec("use test");
    tk.must_exec("set @@tidb_pessimistic_txn_fair_locking = 1");
    tk.must_exec("create table t (id int primary key, v int)");
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk2.must_exec("insert into t values (1, 20)");
    let (tx, ch) = std::sync::mpsc::channel();
std::thread::spawn(move || {
 tk.must_get_err_code("insert into t values (1, 10)", 1062);
 let _ = tx.send(tk);
});
    must_timeout(&ch, 100);
    tk2.must_exec("commit");
    let mut tk = must_recv(ch);
    tk.must_exec("rollback");
    tk.check("select * from t", &["1 20"]);
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk2.must_exec("delete from t where id = 1");
    let res = must_exec_async(tk, "insert into t values (1, 10)");
    must_timeout(&res, 100);
    tk2.must_exec("commit");
    let mut tk = must_recv(res);
    tk.must_exec("commit");
    tk.check("select * from t", &["1 10"]);

}

/// Go `pessimistictest.TestFairLockingLockWithConflictIdempotency` (pessimistic_test.go).
#[test]
fn test_fair_locking_lock_with_conflict_idempotency() {
    
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    let mut tk2 = store.new_tk();
    tk2.session().set_connection_id(0);
    tk2.must_exec("use test");
    tk.must_exec("set @@tidb_pessimistic_txn_fair_locking = 1");
    tk.must_exec("create table t (id int primary key, v int)");
    tk.must_exec("insert into t values (1, 1)");
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk2.must_exec("update t set v = v + 1 where id = 1");
    let rpc_fail_on_recv = tidb_failpoint::enable_for_test("tikvclient/rpcFailOnRecv", "2*return");
    let res = must_exec_async(tk, "update t set v = v + 10 where id = 1");
    must_timeout(&res, 100);
    tk2.must_exec("commit");
    let mut tk = must_recv(res);
    drop(rpc_fail_on_recv);
    tk.must_exec("commit");
    tk.check("select * from t", &["1 12"]);

}

/// Go `pessimistictest.TestFairLockingRetry` (pessimistic_test.go).
#[test]
fn test_fair_locking_retry() {
    
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    let mut tk2 = store.new_tk();
    tk2.must_exec("use test");
    
    tk.must_exec("set @@tidb_pessimistic_txn_fair_locking = 1");
    tk.must_exec("create table t1 (id int primary key, v int)");
    tk.must_exec("create table t2 (id int primary key, v int)");
    tk.must_exec("create table t3 (id int primary key, v int, v2 int)");
    tk.must_exec("insert into t1 values (1, 10)");
    tk.must_exec("insert into t2 values (10, 100), (11, 101)");
    tk.must_exec("insert into t3 values (100, 100, 100), (101, 200, 200)");
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk2.must_exec("update t3 set v2 = v2 + 1 where id = 100");
    let res = must_exec_async(tk, "\n\t\twith\n\t\t\tc1 as (select /*+ MERGE() */ * from t1 where id = 1),\n\t\t\tc2 as (select /*+ MERGE() */ t2.* from  c1 join t2 on c1.v = t2.id for update)\n\t\tupdate c2 join t3 on c2.v = t3.id set t3.v = t3.v + 1\n\t");
    must_timeout(&res, 50);
    let pessimistic_select_for_update_retry = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/pessimisticSelectForUpdateRetry", "pause");
    let pessimistic_dml_retry = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/pessimisticDMLRetry", "pause");
    tk2.must_exec("commit");
    must_timeout(&res, 50);
    must_locked(&store, "select * from t2 where id = 10 for update nowait");
    drop(pessimistic_select_for_update_retry);
    drop(pessimistic_dml_retry);
    let mut tk = must_recv(res);
    must_locked(&store, "select * from t2 where id = 10 for update nowait");
    tk.must_exec("commit");
    tk.check("select * from t3", &["100 101 101", "101 200 200"]);
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk2.must_exec("update t3 set v2 = v2 + 1 where id = 100");
    let res = must_exec_async(tk, "\n\t\twith\n\t\t\tc1 as (select /*+ MERGE() */ * from t1 where id = 1),\n\t\t\tc2 as (select /*+ MERGE() */ t2.* from  c1 join t2 on c1.v = t2.id for update)\n\t\tupdate c2 join t3 on c2.v = t3.id set t3.v = t3.v + 1\n\t");
    must_timeout(&res, 50);
    tk2.must_exec("update t1 set v = 11 where id = 1");
    let pessimistic_select_for_update_retry = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/pessimisticSelectForUpdateRetry", "pause");
    let pessimistic_dml_retry = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/pessimisticDMLRetry", "pause");
    tk2.must_exec("commit");
    must_timeout(&res, 50);
    must_locked(&store, "select * from t2 where id = 10 for update nowait");
    drop(pessimistic_select_for_update_retry);
    drop(pessimistic_dml_retry);
    let mut tk = must_recv(res);
    tk2.must_exec("begin pessimistic");
    tk2.check("select * from t2 where id = 10 for update", &["10 100"]);
    tk2.must_exec("rollback");
    must_locked(&store, "select * from t2 where id = 11 for update nowait");
    tk.must_exec("commit");
    tk.check("select * from t3", &["100 101 102", "101 201 200"]);

}


/// Go `config.RestoreFunc` around the async-commit client configuration.
pub(super) struct AsyncCommitConfig(std::sync::Arc<tikv_client::Config>);
impl AsyncCommitConfig {
    /// Go `config.UpdateGlobal`'s `TiKVClient.AsyncCommit` fields.
    pub(super) fn set(safe_window: std::time::Duration, allowed_clock_drift: std::time::Duration) -> Self {
        let previous = tikv_client::config::get_global_config();
        let mut next = (*previous).clone();
        next.tikv_client.async_commit.safe_window = safe_window;
        next.tikv_client.async_commit.allowed_clock_drift = allowed_clock_drift;
        tikv_client::config::store_global_config(next);
        Self(previous)
    }
}
impl Drop for AsyncCommitConfig {
    /// Go `config.RestoreFunc`.
    fn drop(&mut self) { tikv_client::config::store_global_config(self.0.clone()); }
}

/// Go `pessimistictest.createAsyncCommitTestKit`.
fn create_async_commit_test_kit(store: &super::realtikvtest::RealTiKV) -> super::realtikvtest::Tk {
    let mut tk = store.new_tk();
    tk.must_exec("set tidb_enable_async_commit = true");
    tk.must_exec("use test");
    tk
}
/// Go `pessimistictest.TestAsyncCommitCalTSFail` (pessimistic_test.go).
#[test]
fn test_async_commit_cal_ts_fail() {
    let _config = AsyncCommitConfig::set(std::time::Duration::from_millis(1000), std::time::Duration::ZERO);
    let store = realtikv!();
    let mut tk = create_async_commit_test_kit(&store);
    let mut tk2 = create_async_commit_test_kit(&store);
    tk.must_exec("drop table if exists tk");
    tk.must_exec("create table tk (c1 int primary key, c2 int)");
    tk.must_exec("insert into tk values (1, 1)");
    tk.must_exec("set tidb_enable_1pc = true");
    tk.must_exec("begin pessimistic");
    tk.check("select * from tk for update", &["1 1"]);
    let invalid = tidb_failpoint::enable_for_test("tikvclient/failCheckSchemaValid", "return");
    tk.exec_to_err("commit");
    drop(invalid);
    tk2.must_exec("set innodb_lock_wait_timeout = 5");
    tk2.must_exec("begin pessimistic");
    tk2.must_exec("update tk set c2 = c2 + 1");
    tk2.must_exec("commit");

}

/// Go `pessimistictest.TestAsyncCommitAndForeignKey` (pessimistic_test.go).
#[test]
fn test_async_commit_and_foreign_key() {
    let _config = AsyncCommitConfig::set(std::time::Duration::from_millis(1000), std::time::Duration::ZERO);
    let store = realtikv!();
    let mut tk = create_async_commit_test_kit(&store);
    tk.must_exec("drop table if exists t_parent, t_child");
    tk.must_exec("create table t_parent (id int primary key)");
    tk.must_exec("create table t_child (id int primary key, pid int, foreign key (pid) references t_parent(id) on delete cascade on update cascade)");
    tk.must_exec("insert into t_parent values (1),(2),(3),(4)");
    tk.must_exec("insert into t_child values (1,1),(2,2),(3,3)");
    tk.must_exec("set tidb_enable_1pc = true");
    tk.must_exec("begin pessimistic");
    tk.must_exec("delete from t_parent where id in (1,4)");
    tk.must_exec("update t_parent set id=22 where id=2");
    tk.must_exec("commit");
    tk.check("select * from t_parent order by id", &["3", "22"]);
    tk.check("select * from t_child order by id", &["2 22", "3 3"]);

}

/// Go `pessimistictest.TestSelectForUpdateConflictRetry` (pessimistic_test.go).
#[test]
fn test_select_for_update_conflict_retry() {
    let _config = AsyncCommitConfig::set(std::time::Duration::from_millis(500), std::time::Duration::ZERO);
    let store = realtikv!();
    let mut tk = create_async_commit_test_kit(&store);
    tk.must_exec("drop table if exists tk");
    tk.must_exec("create table tk (c1 int primary key, c2 int)");
    tk.must_exec("insert into tk values(1,1),(2,2)");
    let mut tk2 = create_async_commit_test_kit(&store);
    let mut tk3 = create_async_commit_test_kit(&store);
    tk2.must_exec("begin pessimistic");
    tk3.must_exec("begin pessimistic");
    tk2.must_exec("update tk set c2 = c2 + 1 where c1 = 1");
    tk3.must_exec("update tk set c2 = c2 + 1 where c2 = 2");
    let tikv = store.tikv_store().clone();
let (tx, rx) = std::sync::mpsc::channel();
let worker = std::thread::spawn(move || {
 tk3.must_exec("update tk set c2 = c2 + 1 where c1 = 1");
 let last_ts = tikv.get_low_resolution_timestamp_async().wait().unwrap();
 tx.send(last_ts).unwrap();
 tk3.must_exec("commit");
 tx.send(last_ts).unwrap();
});
    let tk2_last_ts = store.tikv_store().get_low_resolution_timestamp_async().wait().unwrap();
    tk2.must_exec("commit");
    let tk3_last_ts = rx.recv().unwrap();
    assert!(tk3_last_ts > tk2_last_ts);
    rx.recv().unwrap();
worker.join().unwrap();

}


/// Go `pessimistictest.TestIssue66571`.
#[test]
fn test_issue66571() {
    let store = realtikv!();
    for (init, conflict, tested, expected) in [
        ("", "", "update t set v = v + 1 where uk = 10", vec!["1 10 101"]),
        ("insert into t values (0, 0, 10)", "update t set v = 200 where uk = 10", "update t set v = v + 1 where uk = 10", vec!["0 0 10", "1 10 201"]),
        ("insert into t values (0, 0, 9)", "update t set v = 10 where id = 0", "with x as (select /*+ MERGE() */ * from t where id = 0) update x join t on x.v = t.uk set t.v = t.v + 1 where t.uk = 10", vec!["0 0 10", "1 10 101"]),
    ] {
        let mut tk = store.new_tk();
        let mut tk2 = store.new_tk();
        let _ttl = LockTtl::set(100);
        tk.must_exec("set @@tidb_pessimistic_txn_fair_locking=1");
        tk.must_exec("use test");
        tk.must_exec("drop table if exists t");
        tk.must_exec("create table t (id int primary key, uk int unique, v int)");
        if !init.is_empty() { tk.must_exec(init); }
        tk.must_exec("insert into t values (1, 10, 100)");
        tk2.must_exec("use test");
        tk2.must_exec("set innodb_lock_wait_timeout = 1");
        tk.must_exec("begin pessimistic");
        if !conflict.is_empty() {
            let mut tk3 = store.new_tk();
            tk3.must_exec("use test");
            tk3.must_exec(conflict);
        }
        tk.must_exec(tested);
        tk2.must_exec("begin pessimistic");
        tk2.must_get_err_code("update t set v = v + 2 where uk = 10", 1205);
        tk2.must_exec("commit");
        tk.must_exec("commit");
        tk.check("select * from t", &expected);
    }
}

/// Go `pessimistictest.TestEndTxnOnLockExpire`.
#[test]
fn test_end_txn_on_lock_expire() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("create table t(a int, b int)");
    tk.must_exec("prepare ps_commit from 'commit'");
    tk.must_exec("prepare ps_rollback from 'rollback'");
    let _ttl = LockTtl::set(300);
    let previous = tikv_client::config::get_global_config();
    let mut config = (*previous).clone();
    config.max_txn_ttl = 500;
    tikv_client::config::store_global_config(config);
    let _config = AsyncCommitConfig(previous);
    for end in ["commit", "execute ps_commit", "rollback", "execute ps_rollback"] {
        let _ = tk.exec("delete from t");
        let _ = tk.exec("insert into t values (1, 1)");
        let _ = tk.exec("begin pessimistic");
        let _ = tk.exec("update t set b = 10 where a = 1");
        std::thread::sleep(std::time::Duration::from_secs(1));
        tk.must_contain_err_msg("select * from t", "TTL manager has timed out");
        tk.must_exec(end);
    }
}
/// Go `pessimistictest.TestIssue40114` (pessimistic_test.go).
#[test]
fn test_issue40114() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    let mut tk2 = store.new_tk();
    tk2.must_exec("use test");
    tk.must_exec("create table t (id int primary key, v int)");
    tk.must_exec("insert into t values (1, 1), (2, 2)");
    let two_pc_request_batch_size_limit = tidb_failpoint::enable_for_test("tikvclient/twoPCRequestBatchSizeLimit", "return");
    let before_async_pessimistic_rollback = tidb_failpoint::enable_for_test("tikvclient/beforeAsyncPessimisticRollback", "return(\"skip\")");
    
    tk.must_exec("set @@innodb_lock_wait_timeout = 1");
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk2.must_exec("update t set v = v + 1 where id = 2");
    tk.must_get_err_code("delete from t where id = 1 or id = 2", 1205);
    tk2.must_exec("commit");
    std::thread::sleep(std::time::Duration::from_millis(50));
    tk2.must_exec("begin pessimistic");
    tk2.must_get_err_code("select * from t where id = 1 for update nowait", 3572);
    tk2.must_exec("rollback");
    tk.check("select @@tidb_current_ts = 0", &["0"]);
    tk.must_get_err_code("insert into t values (1, 2)", 1062);
    tk.must_exec("commit");
    tk.must_exec("admin check table t");
    tk.check("select * from t", &["1 1", "2 3"]);

}

/// Go `pessimistictest.TestIssue42937` (pessimistic_test.go).
#[test]
fn test_issue42937() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    let mut tk3 = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("set @@tidb_enable_async_commit = 0");
    tk.must_exec("set @@tidb_enable_1pc = 0");
    tk2.must_exec("use test");
    tk2.must_exec("set @@tidb_enable_async_commit = 0");
    tk2.must_exec("set @@tidb_enable_1pc = 0");
    tk3.must_exec("use test");
    tk.must_exec("create table t(id int primary key, v int unique)");
    tk.must_exec("insert into t values (1, 10), (2, 20), (3, 30), (4, 40)");
    tk.must_exec("create table t2 (id int primary key, v int)");
    tk.must_exec("insert into t2 values (1, 1), (2, 2)");
    let before_async_pessimistic_rollback = tidb_failpoint::enable_for_test("tikvclient/beforeAsyncPessimisticRollback", "return(\"skip\")");
    let two_pc_request_batch_size_limit = tidb_failpoint::enable_for_test("tikvclient/twoPCRequestBatchSizeLimit", "return");
    
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk2.must_exec("update t set v = v + 1 where id = 2");
    let two_pc_short_lock_ttl = tidb_failpoint::enable_for_test("tikvclient/twoPCShortLockTTL", "return");
    let short_pessimistic_lock_ttl = tidb_failpoint::enable_for_test("tikvclient/shortPessimisticLockTTL", "return");
    let ch = must_exec_async(tk, "\n\t\twith\n\t\t\tc as (select /*+ MERGE() */ v from t2 where id = 1 or id = 2)\n\t\tupdate c join t on c.v = t.id set t.v = t.v + 1");
    must_timeout(&ch, 100);
    tk3.must_exec("update t2 set v = v + 2");
    tk2.must_exec("commit");
    let mut tk = ch.recv().unwrap();
    tk.check("select id, v from t order by id", &["1 10", "2 20", "3 31", "4 41"]);
    tk.must_exec("update t set v = 0 where id = 1");
    let before_commit = tidb_failpoint::enable_for_test("tikvclient/beforeCommit", "1*return(\"delay(500)\")");
    
    let ch = must_exec_async(tk, "commit");
    must_timeout(&ch, 100);
    drop(two_pc_short_lock_ttl);
    drop(short_pessimistic_lock_ttl);
    tk2.must_exec("insert into t values (5, 11)");
    let mut tk = must_recv(ch);
    tk.must_exec("admin check table t");
    tk.check("select * from t order by id", &["1 0", "2 21", "3 31", "4 41", "5 11"]);

}


/// Go `config.RestoreFunc` around TiDB's server configuration.
pub(super) struct ConfigRestore(std::sync::Arc<tidb_config::config_tree::config::Config>);
impl ConfigRestore {
    /// Go `config.RestoreFunc`.
    pub(super) fn new() -> Self { Self(tidb_config::config_tree::config::get_global_config()) }
}
impl Drop for ConfigRestore {
    /// Go `config.StoreGlobalConfig`.
    fn drop(&mut self) { tidb_config::config_tree::config::store_global_config(self.0.clone()); }
}

/// Go `config.UpdateGlobal`'s PessimisticAutoCommit switch.
fn set_pessimistic_auto_commit(enabled: bool) {
    tidb_config::config_tree::config::update_global(|config| {
        config.pessimistic_txn.pessimistic_auto_commit = tidb_config::config_tree::AtomicBool::new(enabled);
    });
}

/// Go `pessimistictest.TestPessimisticAutoCommitTxn`.
#[test]
fn test_pessimistic_auto_commit_txn() {
    let store = realtikv!();
    let _config = ConfigRestore::new();
    set_pessimistic_auto_commit(false);
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("set tidb_txn_mode = 'pessimistic'");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (i int primary key)");
    tk.must_exec("insert into t values (1)");
    tk.must_exec("set autocommit = on");
    for enabled in [false, true] {
        set_pessimistic_auto_commit(enabled);
        let rows = tk.must_query("explain update t set i = -i");
        assert_eq!(rows[1].join(" ").contains("SelectLock"), enabled, "{rows:?}");
        let rows = tk.must_query("explain update t set i = -i where i = -1");
        let explain = rows[1].join(" ");
        assert!(explain.contains("handle:-1"), "{explain}");
        assert_eq!(explain.contains("handle:-1, lock"), enabled, "{explain}");
        let rows = tk.must_query("explain update t set i = -i where i in (-1, 1)");
        let explain = rows[1].join(" ");
        assert!(explain.contains("handle:[-1 1]"), "{explain}");
        assert_eq!(explain.contains(", lock"), enabled, "{explain}");
    }
}

/// Go `TestPessimisticAutoCommitStatementTypes.checkTxnMode`.
fn check_txn_mode(tk: &mut super::realtikvtest::Tk, sql: &str, expected: bool) {
    tk.must_exec("rollback");
    tk.must_exec(sql);
    assert_eq!(tk.session().statement_is_pessimistic(), expected, "{sql}");
    tk.must_exec("rollback");
}
/// Go `pessimistictest.TestPessimisticAutoCommitStatementTypes` (pessimistic_test.go).
#[test]
fn test_pessimistic_auto_commit_statement_types() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("set tidb_txn_mode = 'pessimistic'");
    tk.must_exec("set autocommit = on");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (id int primary key, val int)");
    let _config = ConfigRestore::new();
set_pessimistic_auto_commit(false);
    check_txn_mode(&mut tk, "INSERT INTO t VALUES (1, 10)", false);
    check_txn_mode(&mut tk, "UPDATE t SET val = 20 WHERE id = 1", false);
    check_txn_mode(&mut tk, "DELETE FROM t WHERE id = 1", false);
    check_txn_mode(&mut tk, "SELECT * FROM t", false);
    set_pessimistic_auto_commit(true);
    check_txn_mode(&mut tk, "INSERT INTO t VALUES (2, 20)", true);
    check_txn_mode(&mut tk, "UPDATE t SET val = 30 WHERE id = 2", true);
    check_txn_mode(&mut tk, "DELETE FROM t WHERE id = 2", true);
    check_txn_mode(&mut tk, "SELECT * FROM t FOR UPDATE", false);
    check_txn_mode(&mut tk, "EXPLAIN INSERT INTO t VALUES (3, 30)", true);
    check_txn_mode(&mut tk, "EXPLAIN UPDATE t SET val = 40 WHERE id = 3", true);
    check_txn_mode(&mut tk, "EXPLAIN DELETE FROM t WHERE id = 3", true);
    tk.must_exec("PREPARE insert_stmt FROM 'INSERT INTO t VALUES (?, ?)'");
    tk.must_exec("PREPARE update_stmt FROM 'UPDATE t SET val = ? WHERE id = ?'");
    tk.must_exec("PREPARE delete_stmt FROM 'DELETE FROM t WHERE id = ?'");
    tk.must_exec("PREPARE select_stmt FROM 'SELECT * FROM t WHERE id = ?'");
    tk.must_exec("SET @id1 = 4, @val1 = 40");
    check_txn_mode(&mut tk, "EXECUTE insert_stmt USING @id1, @val1", true);
    tk.must_exec("SET @id2 = 4, @val2 = 50");
    check_txn_mode(&mut tk, "EXECUTE update_stmt USING @val2, @id2", true);
    tk.must_exec("SET @id3 = 4");
    check_txn_mode(&mut tk, "EXECUTE delete_stmt USING @id3", true);
    tk.must_exec("SET @id4 = 1");
    check_txn_mode(&mut tk, "EXECUTE select_stmt USING @id4", false);

}

/// Go `pessimistictest.TestInnodbLockWaitTimeoutWaitStart` (pessimistic_test.go).
#[test]
fn test_innodb_lock_wait_timeout_wait_start() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists tk");
    tk.must_exec("create table tk (c1 int primary key, c2 int)");
    tk.must_exec("insert into tk values(1,1),(2,2),(3,3),(4,4),(5,5)");
    tk.must_exec("set global innodb_lock_wait_timeout = 1");
    let mut tk2 = store.new_tk();
    tk2.must_exec("use test");
    let mut tk3 = store.new_tk();
    tk3.must_exec("use test");
    tk2.check("show variables like \"innodb_lock_wait_timeout\"", &["innodb_lock_wait_timeout 1"]);
    tk3.must_exec("begin pessimistic");
    tk3.must_exec("select * from tk where c1 = 1 for update");
    tk2.must_exec("begin pessimistic");
    let conflict = tidb_failpoint::enable_for_test("tikvclient/PessimisticLockErrWriteConflict", "return");
let worker = std::thread::spawn(move || {
 let start = std::time::Instant::now();
 let error = tk2.exec_to_err("select * from tk where c1 = 1 for update");
 (tk2, error, start.elapsed())
});
    std::thread::sleep(std::time::Duration::from_millis(100));
    drop(conflict);
    let (mut tk2, error, duration) = worker.join().unwrap();
assert_eq!(error, "[tikv:1205]Lock wait timeout exceeded; try restarting transaction");
assert!(duration >= std::time::Duration::from_secs(1));
assert!(duration <= std::time::Duration::from_secs(3));
    tk2.must_exec("rollback");
    tk3.must_exec("commit");
    tk.must_exec("drop table if exists tk");

}

/// Go `pessimistictest.TestPessimisticCommitReadLock` (pessimistic_test.go).
#[test]
fn test_pessimistic_commit_read_lock() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    let mut tk1 = store.new_tk();
    tk1.must_exec("use test");
    tk.must_exec("set tidb_txn_mode = 'pessimistic'");
    tk1.must_exec("set tidb_txn_mode = 'pessimistic'");
    tk1.must_exec("set innodb_lock_wait_timeout = 2");
    tk.must_exec("drop table if exists t;");
    tk.must_exec("create table t(i int key primary key, j int);");
    tk.must_exec("insert into t values (1, 2);");
    tk.check("select * from t", &["1 2"]);
    tk.must_exec("begin;");
    tk.check("select * from t for update", &["1 2"]);
    tk1.must_exec("begin;");
    let worker = std::thread::spawn(move || {
 let ttl = tidb_failpoint::enable_for_test("tikvclient/txnNotFoundRetTTL", "return");
 tk1.must_exec("update t set j = j + 1 where i = 1");
 drop(ttl);
 tk1.must_exec("commit");
});
    std::thread::sleep(std::time::Duration::from_millis(50));
    tk.must_exec("commit");
    worker.join().unwrap();

}

/// Go `pessimistictest.TestRollbackWakeupBlockedTxn` (pessimistic_test.go).
#[test]
fn test_rollback_wakeup_blocked_txn() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk.must_exec("drop table if exists t1");
    tk.must_exec("create table t1 (c1 int primary key, c2 int, c3 int, unique key uk(c2))");
    tk.must_exec("insert into t1 values (1, 1, 1)");
    tk.must_exec("insert into t1 values (5, 5, 5)");
    tk.must_exec("insert into t1 values (10, 10, 10)");
    let txn_expire_ret_ttl = tidb_failpoint::enable_for_test("tikvclient/txnExpireRetTTL", "return");
    let get_txn_status_delay = tidb_failpoint::enable_for_test("tikvclient/getTxnStatusDelay", "return");
    tk.must_exec("begin pessimistic");
    tk2.must_exec("set innodb_lock_wait_timeout = 1");
    tk2.must_exec("begin pessimistic");
    tk.must_exec("update t1 set c3 = c3 + 1");
    let worker = std::thread::spawn(move || {
 let result = tk2.exec("update t1 set c3 = 100 where c1 = 1");
 (tk2, result)
});
    std::thread::sleep(std::time::Duration::from_millis(30));
    tk.must_exec("rollback");
    let (mut tk2, result) = worker.join().unwrap();
result.unwrap();
    tk2.must_exec("rollback");
    drop(txn_expire_ret_ttl);
    drop(get_txn_status_delay);

}

/// Go `pessimistictest.TestTxnWithExpiredPessimisticLocks` (pessimistic_test.go).
#[test]
fn test_txn_with_expired_pessimistic_locks() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t1");
    tk.must_exec("create table t1 (c1 int primary key, c2 int, c3 int, unique key uk(c2))");
    
    tk.must_exec("insert into t1 values (1, 1, 1)");
    tk.must_exec("insert into t1 values (5, 5, 5)");
    tk.must_exec("begin pessimistic");
    tk.check("select * from t1 where c1 in(1, 5) for update", &["1 1 1", "5 5 5"]);
    tk.session().txn_lock_expire().unwrap().store(1, std::sync::atomic::Ordering::SeqCst);
    tk.must_get_err_code("select * from t1 where c1 in(1, 5)", 8229);
    tk.must_exec("commit");
    tk.must_exec("begin pessimistic");
    tk.check("select * from t1 where c1 in(1, 5) for update", &["1 1 1", "5 5 5"]);
    tk.session().txn_lock_expire().unwrap().store(1, std::sync::atomic::Ordering::SeqCst);
    tk.must_get_err_code("update t1 set c2 = c2 + 1", 8229);
    tk.session().txn_lock_expire().unwrap().store(0, std::sync::atomic::Ordering::SeqCst);
    tk.must_exec("update t1 set c2 = c2 + 1");
    tk.must_exec("rollback");
    tk.must_exec("drop table if exists t1");
}

/// Go `pessimistictest.TestPointLockNonExistentKeyWithFairLockingUnderRC`.
#[test]
fn test_point_lock_non_existent_key_with_fair_locking_under_rc() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    tk.must_exec("set tx_isolation = 'READ-COMMITTED'");
    tk.must_exec("set @@tidb_pessimistic_txn_fair_locking=1");
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk.must_exec("create table t (a int primary key, b int)");
    tk.must_exec("begin pessimistic");
    tk.must_exec("select * from t where a = 1 for update");
    tk.must_exec("commit");
    let mut locked_with_conflict_counter = 0;
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk2.must_exec("insert into t values (1, 2)");
    let _fp = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/store/driver/txn/lockedWithConflictOccurs", "return");
    // Go failpoint.EnableWith invokes its action once while enabling, not at Inject.
    locked_with_conflict_counter += 1;
    let (send, recv) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let rows = tk.must_query("select * from t where a = 1 for update");
        send.send(rows).unwrap();
        tk
    });
    assert!(matches!(recv.recv_timeout(std::time::Duration::from_millis(100)), Err(std::sync::mpsc::RecvTimeoutError::Timeout)));
    tk2.must_exec("commit");
    assert_eq!(recv.recv_timeout(std::time::Duration::from_secs(1)).unwrap(), vec![vec!["1", "2"]]);
    let mut tk = worker.join().unwrap();
    assert_eq!(locked_with_conflict_counter, 1);
    tk.must_exec("commit");
    locked_with_conflict_counter = 0;
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk2.must_exec("delete from t where a = 1");
    let (send, recv) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let rows = tk.must_query("select * from t where a = 1 for update");
        send.send(rows).unwrap();
        tk
    });
    assert!(matches!(recv.recv_timeout(std::time::Duration::from_millis(100)), Err(std::sync::mpsc::RecvTimeoutError::Timeout)));
    tk2.must_exec("commit");
    assert!(recv.recv_timeout(std::time::Duration::from_secs(1)).unwrap().is_empty());
    let mut tk = worker.join().unwrap();
    assert_eq!(locked_with_conflict_counter, 0);
    tk.must_exec("commit");
}
/// Go `pessimistictest.TestRCPointWriteLockIfExists` (pessimistic_test.go).
#[test]
fn test_rc_point_write_lock_if_exists() {
    let _assert_lock_error = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/assertPessimisticLockErr", "return");
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk.must_exec("set transaction_isolation = 'READ-COMMITTED'");
    tk.must_exec("set innodb_lock_wait_timeout = 1");
    tk2.must_exec("set transaction_isolation = 'READ-COMMITTED'");
    tk2.must_exec("set innodb_lock_wait_timeout = 1");
    tk.must_exec("drop table if exists t1");
    tk.must_exec("create table t1(id1 int, id2 int, id3 int, PRIMARY KEY(id1), UNIQUE KEY udx_id2 (id2))");
    tk.must_exec("insert into t1 values (1, 1, 1)");
    tk.must_exec("insert into t1 values (10, 10, 10)");
    tk.check("show variables like 'transaction_isolation'", &["transaction_isolation READ-COMMITTED"]);
    let table_id: i64 = tk.must_query("select tidb_table_id from information_schema.tables where table_schema='test' and table_name='t1'")[0][0].parse().unwrap();
let idx_value = tidb_codec::encode_key(&[tidb_datatype::Datum::Int(1)]).unwrap();
let sec_idx_key1 = tidb_tablecodec::table_key::encode_index_seek_key(table_id, 1, &idx_value);
let key1 = tidb_tablecodec::table_key::encode_row_key_with_handle(table_id, &tidb_tablecodec::table_key::RecordHandle::Int(1));
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk.check("select * from t1 where id1 = 1 for update", &["1 1 1"]);
    assert!(tk.session().node_conn().get_key_in_pessimistic_lock_cache(&key1).is_some());
    tk2.must_get_err_code("update t1 set id3 = 100 where id1 = 1", 1205);
    tk.must_exec("rollback");
    tk2.must_exec("rollback");
    tk.must_exec("begin pessimistic");
    tk.check("select * from t1 where id1 = 1 for update", &["1 1 1"]);
    tk.must_exec("update t1 set id3 = 200 where id1 = 1");
    tk.must_exec("commit");
    tk2.check("select * from t1 where id1 = 1", &["1 1 200"]);
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk.must_exec("select id1+id2, id3*id3 from t1 where id1 = 1 for update");
    assert!(tk.session().node_conn().get_key_in_pessimistic_lock_cache(&key1).is_some());
    tk2.must_get_err_code("update t1 set id3 = 1000 where id1 = 1", 1205);
    tk.must_exec("rollback");
    tk2.must_exec("rollback");
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk.must_exec("select * from t1 where id1 = 20 for update");
    tk2.must_exec("select * from t1 where id1 = 20 for update");
    tk.must_exec("rollback");
    tk2.must_exec("rollback");
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk.must_exec("select * from t1 where id1 = 1 and id2 = 2 for update");
    tk2.must_get_err_code("update t1 set id3 = 300 where id1 = 1", 1205);
    tk.must_exec("rollback");
    tk2.must_exec("rollback");
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk.must_exec("insert into t1 values(15, 15, 15)");
    tk.must_exec("SELECT * FROM t1 WHERE id1 = 15 for update");
    tk.must_exec("update t1 set id3 = 100 where id1 = 15");
    tk2.must_get_err_code("SELECT * FROM t1 where id1 = 15 FOR UPDATE", 1205);
    tk.check("SELECT * FROM t1 WHERE id1 = 15", &["15 15 100"]);
    tk.must_exec("rollback");
    tk2.must_exec("rollback");
    tk.session().clear_assert_lock_errors();
    tk.must_exec("begin pessimistic");
    tk2.must_exec("update t1 set id3 = 100 where id1 = 10");
    tk.check("SELECT * FROM t1 WHERE id1 = 10 for update", &["10 10 100"]);
    tk.must_exec("commit");
    assert!(tk.session().assert_lock_errors().is_none());
    tk.must_exec("update t1 set id3 = 1 where id1 = 1");
    tk.must_exec("update t1 set id3 = 10 where id1 = 10");
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk.check("select * from t1 where id2 = 1 for update", &["1 1 1"]);
    let value = tk.session().node_conn().get_key_in_pessimistic_lock_cache(&sec_idx_key1).unwrap();
let handle = tidb_tablecodec::decode_handle_in_index_value(&value).unwrap().unwrap();
assert_eq!(handle.int_value(), Some(1));
assert!(tk.session().node_conn().get_key_in_pessimistic_lock_cache(&key1).is_some());
    tk2.must_get_err_code("update t1 set id3 = 100 where id2 = 1", 1205);
    tk.must_exec("rollback");
    tk2.must_exec("rollback");
    tk.must_exec("begin pessimistic");
    tk.check("select * from t1 where id2 = 1 for update", &["1 1 1"]);
    tk.must_exec("update t1 set id3 = 1000 where id1 = 1");
    tk.must_exec("commit");
    tk2.check("select * from t1 where id2 = 1", &["1 1 1000"]);
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk.check("select id2 from t1 where id2 = 1 for update", &["1"]);
    tk2.must_get_err_code("update t1 set id3 = 10000 where id2 = 1", 1205);
    tk.must_exec("rollback");
    tk2.must_exec("rollback");
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk.check("select id2*2 from t1 where id2 = 1 for update", &["2"]);
    tk2.must_get_err_code("update t1 set id3 = 10000 where id2 = 1", 1205);
    tk.must_exec("rollback");
    tk2.must_exec("rollback");
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk.must_exec("select * from t1 where id2 = 1 for update");
    tk2.must_get_err_code("update t1 set id3 = 10000 where id1 = 1", 1205);
    tk.must_exec("rollback");
    tk2.must_exec("rollback");
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk.must_exec("select * from t1 where id2 = 20 for update");
    tk2.must_exec("select * from t1 where id2 = 20 for update");
    tk.must_exec("rollback");
    tk2.must_exec("rollback");
    drop(_assert_lock_error);

}


/// Go `pessimistictest.TestStatementsInterruptedOnDisconnect` (server_test.go).
#[test]
fn test_statements_interrupted_on_disconnect() {
    let store = realtikv!();
    let mut admin = store.new_tk();
    admin.must_exec("use test");
    for (name, setup, prepared, select, multi) in [
        ("autocommit_update_query", "", false, false, false),
        ("autocommit_update_prepared", "", true, false, false),
        ("explicit_txn_update_query", "begin pessimistic", false, false, false),
        ("explicit_txn_update_prepared", "begin pessimistic", true, false, false),
        ("explicit_txn_select_for_update_query", "begin pessimistic", false, true, false),
        ("explicit_txn_select_for_update_prepared", "begin pessimistic", true, true, false),
        ("autocommit_off_update_query", "set autocommit = 0", false, false, false),
        ("multi_statement_prefetch", "begin pessimistic", false, false, true),
    ] {
        let table = format!("issue68682_{name}");
        admin.must_exec(&format!("drop table if exists {table}"));
        admin.must_exec(&format!("create table {table} (id int primary key, v int)"));
        admin.must_exec(&format!("insert into {table} values (1,0), (2,0), (3,0)"));
        let (mut victim, server) = store.new_tcp(multi);
        victim.must_exec("set tidb_txn_mode = 'pessimistic'");
        if !setup.is_empty() {
            victim.must_exec(setup);
            victim.must_exec(&format!("update {table} set v = 1 where id = 1"));
        }
        let id = victim.connection_id;
        let mut blocker = store.new_tk();
        blocker.must_exec("use test");
        blocker.must_exec("begin pessimistic");
        blocker.must_exec(&format!("update {table} set v = 2 where id = 2"));
        let sql = if select { format!("select * from {table} where id = 2 for update") }
            else if multi { format!("update {table} set v = 2 where id = 2; update {table} set v = 3 where id = 3") }
            else { format!("update {table} set v = 2 where id = 2") };
        let prepared_id = prepared.then(|| victim.prepare(&sql));
        let raw = victim.stream.try_clone().unwrap();
        let (send, recv) = std::sync::mpsc::channel();
        let client = std::thread::spawn(move || {
            if let Some(id) = prepared_id { victim.send_prepared(id); } else { victim.send_query(&sql); }
            victim.await_disconnect_error();
            send.send(()).unwrap();
        });
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            loop {
                let rows = admin.must_query(&format!("select count(*) from information_schema.data_lock_waits l join information_schema.tidb_trx trx on l.trx_id = trx.id where trx.session_id = {id}"));
                if rows[0][0] != "0" { break; }
                assert!(std::time::Instant::now() < deadline, "{name}: did not enter a TiKV lock wait");
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            raw.shutdown(std::net::Shutdown::Both).unwrap();
            recv.recv_timeout(std::time::Duration::from_secs(5)).expect("client did not fail after disconnect");
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while admin.must_query(&format!("select count(*) from information_schema.processlist where id = {id}"))[0][0] != "0" {
                assert!(std::time::Instant::now() < deadline, "{name}: disconnected session retained");
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            admin.must_exec("set innodb_lock_wait_timeout = 5");
            admin.must_exec(&format!("update {table} set v = v + 1 where id = 1"));
            admin.check(&format!("select sum(if(id=1,v,0)), sum(if(id=2,v,0)), sum(if(id=3,v,0)) from {table}"), &["1 0 0"]);
        }));
        let _ = raw.shutdown(std::net::Shutdown::Both);
        blocker.must_exec("rollback");
        client.join().unwrap();
        server.join().unwrap();
        if let Err(panic) = result { std::panic::resume_unwind(panic); }
    }
}
/// Go `pessimistictest.TestIssueBatchResolveLocks` (pessimistic_test.go).
#[test]
fn test_issue_batch_resolve_locks() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("create table t1 (id int primary key, v int)");
    tk.must_exec("create table t2 (id int primary key, v int)");
    tk.must_exec("create table t3 (id int primary key, v int)");
    tk.must_exec("insert into t1 values (1, 1), (2, 2)");
    tk.must_exec("insert into t2 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)");
    tk.must_exec("insert into t3 values (1, 1)");
    tk.must_exec("set @@tidb_enable_async_commit=0");
    tk.must_exec("set @@tidb_enable_1pc=0");
    let table_id: i64 = tk.must_query("select tidb_table_id from information_schema.tables where table_schema='test' and table_name='t2'")[0][0].parse().unwrap();
let key = tidb_tablecodec::table_key::encode_table_prefix(table_id);
{ use tikv_client::PdClient as _; store.tikv_store().block_on(store.tikv_store().store().pd_client().split_regions(vec![key])).unwrap(); }
    let mut tk2 = store.new_tk();
    tk2.must_exec("use test");
    let mut tk3 = store.new_tk();
    tk3.must_exec("use test");
    let _async_rollback = tidb_failpoint::enable_for_test("tikvclient/beforeAsyncPessimisticRollback", r#"return("skip")"#);
let _secondary = tidb_failpoint::enable_for_test("tikvclient/beforeCommitSecondaries", r#"return("skip")"#);
let _batch = tidb_failpoint::enable_for_test("tikvclient/twoPCRequestBatchSizeLimit", "return");
let _rollback = tidb_failpoint::enable_for_test("tikvclient/onRollback", r#"return("skipRollbackPessimisticLock")"#);
    tk.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk2.must_exec("update t2 set v = v + 1 where id = 2");
    let (send, recv) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
    tk.must_exec("\n\t\twith\n\t\t\tc as (select /*+ MERGE() */ v from t1 where id in (1, 2))\n\t\tupdate c join t2 on c.v = t2.id set t2.v = t2.v + 10");
    send.send(()).unwrap();
tk
});
    assert!(matches!(recv.recv_timeout(std::time::Duration::from_millis(100)), Err(std::sync::mpsc::RecvTimeoutError::Timeout)));
    tk3.must_exec("update t1 set v = v + 3");
    tk2.must_exec("commit");
    recv.recv_timeout(std::time::Duration::from_secs(10)).unwrap();
let mut tk = worker.join().unwrap();
    tk.must_exec("update t2 set v = v + 10 where id = 3");
    tk.must_exec("commit");
    tk.must_exec("begin pessimistic");
    tk.check("select * from t3 where id = 1 for update", &["1 1"]);
    tk.must_exec("rollback");
    must_locked(&store, "select * from t3 where id = 1 for update nowait");
    let current_ts = store.tikv_store().get_timestamp_async().wait().unwrap();
store.run_resolve_locks(current_ts);
    tk3.must_exec("begin pessimistic");
    tk3.check("select * from t3 where id = 1 for update nowait", &["1 1"]);
    tk3.must_exec("rollback");
    tk.check("select * from t2 order by id", &["1 1", "2 3", "3 13", "4 14", "5 15"]);

}

/// Go `pessimistictest.TestPessimisticTxn` (pessimistic_test.go).
#[test]
fn test_pessimistic_txn() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    let mut tk1 = store.new_tk();
    tk1.must_exec("use test");
    tk.must_exec("drop table if exists pessimistic");
    tk.must_exec("create table pessimistic (k int, v int)");
    tk.must_exec("insert into pessimistic values (1, 1)");
    tk1.must_exec("begin pessimistic");
    tk.must_exec("update pessimistic set v = 2 where v = 1");
    tk1.must_exec("update pessimistic set v = 3 where v = 1");
    assert_eq!(tk1.affected_rows(), 0);
    // Go `session.GetHistory(tk1.Session()).Count() == 0`: Go records a
    // statement only while `TxnCtx.CouldRetry`, which an explicit
    // transaction never is under the forced `tidb_disable_txn_auto_retry`;
    // Rust's autocommit retry re-runs the one statement
    // (`StatementLifecycle::may_retry_autocommit_statement`) and keeps no
    // history to count.
    tk1.check("select * from pessimistic for update", &["1 2"]);
    tk1.check("select * from pessimistic", &["1 1"]);
    tk1.must_exec("update pessimistic set v = 3 where v = 2");
    assert_eq!(tk1.affected_rows(), 1);
    tk.check("select * from pessimistic", &["1 2"]);
    tk1.must_exec("commit");
    tk1.check("select * from pessimistic", &["1 3"]);
    tk1.must_exec("begin pessimistic");
    tk1.must_exec("select * from pessimistic where k = 1 for update");
    let (send, recv) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
    tk.must_exec("update pessimistic set v = 5 where k = 1");
    send.send(()).unwrap();
tk
});
    std::thread::sleep(std::time::Duration::from_millis(10));
    tk1.must_exec("update pessimistic set v = 3 where k = 1");
    tk1.must_exec("commit");
    recv.recv_timeout(std::time::Duration::from_secs(10)).unwrap();
let mut tk = worker.join().unwrap();
    tk.check("select * from pessimistic", &["1 5"]);

}

