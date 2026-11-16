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

//! Go `tests/realtikvtest/txntest`, against the configured real TiKV cluster.

/// Go `txntest.TestP1DirtyRead` (isolation_test.go).
#[test]
fn test_p1_dirty_read() {
    let store = realtikv!();
    let mut session1 = store.new_tk();
    let mut session2 = store.new_tk();
    session1.must_exec("use test;");
    session2.must_exec("use test;");
    session1.must_exec("set tidb_txn_mode = 'optimistic'");
    session2.must_exec("set tidb_txn_mode = 'optimistic'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("begin;");
    session1.must_exec("update x set c = c+1 where id = 1;");
    session2.must_exec("begin;");
    session2.check("select c from x where id = 1;", &["1"]);
    session1.must_exec("commit;");
    session2.must_exec("commit;");
    session1.must_exec("set tidb_txn_mode = 'pessimistic'");
    session2.must_exec("set tidb_txn_mode = 'pessimistic'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("begin;");
    session1.must_exec("update x set c = c+1 where id = 1;");
    session2.must_exec("begin;");
    session2.check("select c from x where id = 1;", &["1"]);
    session1.must_exec("commit;");
    session2.must_exec("commit;");
    session1.must_exec("set tx_isolation = 'READ-COMMITTED'");
    session2.must_exec("set tx_isolation = 'READ-COMMITTED'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("begin;");
    session1.must_exec("update x set c = c+1 where id = 1;");
    session2.must_exec("begin;");
    session2.check("select c from x where id = 1;", &["1"]);
    session1.must_exec("commit;");
    session2.must_exec("commit;");
}

/// Go `txntest.TestP2NonRepeatableRead` (isolation_test.go).
#[test]
fn test_p2_non_repeatable_read() {
    let store = realtikv!();
    let mut session1 = store.new_tk();
    let mut session2 = store.new_tk();
    session1.must_exec("use test;");
    session2.must_exec("use test;");
    session1.must_exec("set tidb_txn_mode = 'optimistic'");
    session2.must_exec("set tidb_txn_mode = 'optimistic'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("drop table if exists y;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("create table y (id int primary key, c int);");
    session1.must_exec("insert into y values(1, 1);");
    session1.must_exec("begin;");
    session2.must_exec("begin;");
    session1.check("select c from x where id = 1;", &["1"]);
    session2.check("select c from x where id = 1;", &["1"]);
    session2.must_exec("update x set c = c+1 where id = 1;");
    session2.check("select c from y where id = 1;", &["1"]);
    session2.must_exec("update y set c = c+1 where id = 1;");
    session2.must_exec("commit;");
    session1.check("select c from y where id = 1;", &["1"]);
    session1.must_exec("commit;");
    session1.must_exec("set tidb_txn_mode = 'pessimistic'");
    session2.must_exec("set tidb_txn_mode = 'pessimistic'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("drop table if exists y;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("create table y (id int primary key, c int);");
    session1.must_exec("insert into y values(1, 1);");
    session1.must_exec("begin;");
    session2.must_exec("begin;");
    session1.check("select c from x where id = 1;", &["1"]);
    session2.check("select c from x where id = 1;", &["1"]);
    session2.must_exec("update x set c = c+1 where id = 1;");
    session2.check("select c from y where id = 1;", &["1"]);
    session2.must_exec("update y set c = c+1 where id = 1;");
    session2.must_exec("commit;");
    session1.check("select c from y where id = 1;", &["1"]);
    session1.must_exec("commit;");
    session1.must_exec("set tx_isolation = 'READ-COMMITTED'");
    session2.must_exec("set tx_isolation = 'READ-COMMITTED'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("drop table if exists y;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("create table y (id int primary key, c int);");
    session1.must_exec("insert into y values(1, 1);");
    session1.must_exec("begin;");
    session2.must_exec("begin;");
    session1.check("select c from x where id = 1;", &["1"]);
    session2.check("select c from x where id = 1;", &["1"]);
    session2.must_exec("update x set c = c+1 where id = 1;");
    session2.check("select c from y where id = 1;", &["1"]);
    session2.must_exec("update y set c = c+1 where id = 1;");
    session2.must_exec("commit;");
    session1.check("select c from y where id = 1;", &["2"]);
    session1.must_exec("commit;");
}

/// Go `txntest.TestP4LostUpdate` (isolation_test.go).
#[test]
fn test_p4_lost_update() {
    let store = realtikv!();
    let mut session1 = store.new_tk();
    let mut session2 = store.new_tk();
    session1.must_exec("use test;");
    session2.must_exec("use test;");
    session1.must_exec("set tidb_txn_mode = 'optimistic'");
    session2.must_exec("set tidb_txn_mode = 'optimistic'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("begin;");
    session1.check("select c from x where id = 1;", &["1"]);
    session2.must_exec("begin;");
    session2.check("select c from x where id = 1;", &["1"]);
    session2.must_exec("update x set c = c+1 where id = 1;");
    session2.must_exec("commit;");
    session1.must_exec("update x set c = c+1 where id = 1;");
    let err = session1.exec("commit;");
    assert!(err.is_err());
    session1.must_exec("set tidb_txn_mode = 'pessimistic'");
    session2.must_exec("set tidb_txn_mode = 'pessimistic'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("begin;");
    session1.check("select c from x where id = 1;", &["1"]);
    session2.must_exec("begin;");
    session2.check("select c from x where id = 1;", &["1"]);
    session2.must_exec("update x set c = c+1 where id = 1;");
    session2.must_exec("commit;");
    session1.must_exec("update x set c = c+1 where id = 1;");
    session1.must_exec("commit;");
    session1.must_exec("set tx_isolation = 'READ-COMMITTED'");
    session2.must_exec("set tx_isolation = 'READ-COMMITTED'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("begin;");
    session1.check("select c from x where id = 1;", &["1"]);
    session2.must_exec("begin;");
    session2.check("select c from x where id = 1;", &["1"]);
    session2.must_exec("update x set c = c+1 where id = 1;");
    session2.must_exec("commit;");
    session1.must_exec("update x set c = c+1 where id = 1;");
    session1.must_exec("commit;");
    session1.check("select * from x", &["1 3"]);
}

/// Go `txntest.TestP4CLostUpdate` (isolation_test.go).
#[test]
fn test_p4_c_lost_update() {

}

/// Go `txntest.TestA3Phantom` (isolation_test.go).
#[test]
fn test_a3_phantom() {
    let store = realtikv!();
    let mut session1 = store.new_tk();
    let mut session2 = store.new_tk();
    session1.must_exec("use test;");
    session2.must_exec("use test;");
    session1.must_exec("set tidb_txn_mode = 'optimistic'");
    session2.must_exec("set tidb_txn_mode = 'optimistic'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("begin;");
    session2.must_exec("begin;");
    session2.check("select c from x where id < 5;", &["1"]);
    session1.must_exec("insert into x values(2, 1);");
    session1.must_exec("commit;");
    session2.check("select c from x where id < 5;", &["1"]);
    session2.must_exec("commit;");
    session1.must_exec("set tidb_txn_mode = 'pessimistic'");
    session2.must_exec("set tidb_txn_mode = 'pessimistic'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("begin;");
    session2.must_exec("begin;");
    session2.check("select c from x where id < 5;", &["1"]);
    session1.must_exec("insert into x values(2, 1);");
    session1.must_exec("commit;");
    session2.check("select c from x where id < 5;", &["1"]);
    session2.must_exec("commit;");
    session1.must_exec("set tx_isolation = 'READ-COMMITTED'");
    session2.must_exec("set tx_isolation = 'READ-COMMITTED'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("begin;");
    session2.must_exec("begin;");
    session2.check("select c from x where id < 5;", &["1"]);
    session1.must_exec("insert into x values(2, 1);");
    session1.must_exec("commit;");
    session2.check("select c from x where id < 5;", &["1", "1"]);
    session2.must_exec("commit;");
}

/// Go `txntest.TestA5AReadSkew` (isolation_test.go).
#[test]
fn test_a5_a_read_skew() {
    let store = realtikv!();
    let mut session1 = store.new_tk();
    let mut session2 = store.new_tk();
    session1.must_exec("use test;");
    session2.must_exec("use test;");
    session1.must_exec("set tidb_txn_mode = 'optimistic'");
    session2.must_exec("set tidb_txn_mode = 'optimistic'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("drop table if exists y;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("create table y (id int primary key, c int);");
    session1.must_exec("insert into y values(1, 1);");
    session1.must_exec("begin;");
    session2.must_exec("begin;");
    session1.check("select c from x where id = 1;", &["1"]);
    session2.must_exec("update x set c = c+1 where id = 1;");
    session2.must_exec("update y set c = c+1 where id = 1;");
    session2.must_exec("commit;");
    session1.check("select c from y where id = 1;", &["1"]);
    session1.must_exec("commit;");
    session1.must_exec("set tidb_txn_mode = 'pessimistic'");
    session2.must_exec("set tidb_txn_mode = 'pessimistic'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("drop table if exists y;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("create table y (id int primary key, c int);");
    session1.must_exec("insert into y values(1, 1);");
    session1.must_exec("begin;");
    session2.must_exec("begin;");
    session1.check("select c from x where id = 1;", &["1"]);
    session2.must_exec("update x set c = c+1 where id = 1;");
    session2.must_exec("update y set c = c+1 where id = 1;");
    session2.must_exec("commit;");
    session1.check("select c from y where id = 1;", &["1"]);
    session1.must_exec("commit;");
    session1.must_exec("set tx_isolation = 'READ-COMMITTED'");
    session2.must_exec("set tx_isolation = 'READ-COMMITTED'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("drop table if exists y;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("create table y (id int primary key, c int);");
    session1.must_exec("insert into y values(1, 1);");
    session1.must_exec("begin;");
    session2.must_exec("begin;");
    session1.check("select c from x where id = 1;", &["1"]);
    session2.must_exec("update x set c = c+1 where id = 1;");
    session2.must_exec("update y set c = c+1 where id = 1;");
    session2.must_exec("commit;");
    session1.check("select c from y where id = 1;", &["2"]);
    session1.must_exec("commit;");
}

/// Go `txntest.TestA5BWriteSkew` (isolation_test.go).
#[test]
fn test_a5_b_write_skew() {
    let store = realtikv!();
    let mut session1 = store.new_tk();
    let mut session2 = store.new_tk();
    session1.must_exec("use test;");
    session2.must_exec("use test;");
    session1.must_exec("set tidb_txn_mode = 'optimistic'");
    session2.must_exec("set tidb_txn_mode = 'optimistic'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("drop table if exists y;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("create table y (id int primary key, c int);");
    session1.must_exec("insert into y values(1, 1);");
    session1.must_exec("begin;");
    session2.must_exec("begin;");
    session1.check("select c from x where id = 1;", &["1"]);
    session2.check("select c from y where id = 1;", &["1"]);
    session1.must_exec("update y set c = c+1 where id = 1;");
    session2.must_exec("update x set c = c+1 where id = 1;");
    session2.must_exec("commit;");
    session1.must_exec("commit;");
    session1.must_exec("set tidb_txn_mode = 'pessimistic'");
    session2.must_exec("set tidb_txn_mode = 'pessimistic'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("drop table if exists y;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("create table y (id int primary key, c int);");
    session1.must_exec("insert into y values(1, 1);");
    session1.must_exec("begin;");
    session2.must_exec("begin;");
    session1.check("select c from x where id = 1;", &["1"]);
    session2.check("select c from y where id = 1;", &["1"]);
    session1.must_exec("update y set c = c+1 where id = 1;");
    session2.must_exec("update x set c = c+1 where id = 1;");
    session2.must_exec("commit;");
    session1.must_exec("commit;");
    session1.must_exec("update y set id = 2 where id = 1;");
    session1.check("select id from x", &["1"]);
    session1.check("select id from y", &["2"]);
    session1.must_exec("begin;");
    session2.must_exec("begin;");
    session1.check("select id from x where id = 1;", &["1"]);
    session2.check("select id from y where id = 2;", &["2"]);
    session1.must_exec("update y set id = 1 where id = 2;");
    session2.must_exec("update x set id = 2 where id = 1;");
    session2.must_exec("commit;");
    session1.must_exec("commit;");
    session1.check("select id from x", &["2"]);
    session1.check("select id from y", &["1"]);
    session1.must_exec("set tx_isolation = 'READ-COMMITTED'");
    session2.must_exec("set tx_isolation = 'READ-COMMITTED'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("drop table if exists y;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("create table y (id int primary key, c int);");
    session1.must_exec("insert into y values(1, 1);");
    session1.must_exec("begin;");
    session2.must_exec("begin;");
    session1.check("select c from x where id = 1;", &["1"]);
    session2.check("select c from y where id = 1;", &["1"]);
    session1.must_exec("update y set c = c+1 where id = 1;");
    session2.must_exec("update x set c = c+1 where id = 1;");
    session2.must_exec("commit;");
    session1.must_exec("commit;");
    session1.must_exec("update y set id = 2 where id = 1;");
    session1.check("select id from x", &["1"]);
    session1.check("select id from y", &["2"]);
    session1.must_exec("begin;");
    session2.must_exec("begin;");
    session1.check("select id from x where id = 1;", &["1"]);
    session2.check("select id from y where id = 2;", &["2"]);
    session1.must_exec("update y set id = 1 where id = 2;");
    session2.must_exec("update x set id = 2 where id = 1;");
    session2.must_exec("commit;");
    session1.must_exec("commit;");
    session1.check("select id from x", &["2"]);
    session1.check("select id from y", &["1"]);
}

/// Go `txntest.TestReadAfterWrite` (isolation_test.go).
#[test]
fn test_read_after_write() {
    let store = realtikv!();
    let mut session1 = store.new_tk();
    let mut session2 = store.new_tk();
    session1.must_exec("use test;");
    session2.must_exec("use test;");
    session1.must_exec("set tidb_txn_mode = 'optimistic'");
    session2.must_exec("set tidb_txn_mode = 'optimistic'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("begin;");
    session1.must_exec("update x set c = c+1 where id = 1;");
    session1.must_exec("commit;");
    session2.must_exec("begin;");
    session2.check("select c from x where id = 1;", &["2"]);
    session2.must_exec("commit;");
    session1.must_exec("set tidb_txn_mode = 'pessimistic'");
    session2.must_exec("set tidb_txn_mode = 'pessimistic'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("begin;");
    session1.must_exec("update x set c = c+1 where id = 1;");
    session1.must_exec("commit;");
    session2.must_exec("begin;");
    session2.check("select c from x where id = 1;", &["2"]);
    session2.must_exec("commit;");
    session1.must_exec("set tx_isolation = 'READ-COMMITTED'");
    session2.must_exec("set tx_isolation = 'READ-COMMITTED'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("begin;");
    session1.must_exec("update x set c = c+1 where id = 1;");
    session1.must_exec("commit;");
    session2.must_exec("begin;");
    session2.check("select c from x where id = 1;", &["2"]);
    session2.must_exec("commit;");
}

/// Go `txntest.TestPhantomReadInInnodb` (isolation_test.go).
#[test]
fn test_phantom_read_in_innodb() {
    let store = realtikv!();
    let mut session1 = store.new_tk();
    let mut session2 = store.new_tk();
    session1.must_exec("use test;");
    session2.must_exec("use test;");
    session1.must_exec("set tidb_txn_mode = 'optimistic'");
    session2.must_exec("set tidb_txn_mode = 'optimistic'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("begin;");
    session1.check("select c from x where id < 5;", &["1"]);
    session2.must_exec("begin;");
    session2.must_exec("insert into x values(2, 1);");
    session2.must_exec("commit;");
    session1.must_exec("update x set c = c+1 where id < 5;");
    session1.check("select c from x where id < 5;", &["2"]);
    session1.must_exec("commit;");
    session1.must_exec("set tidb_txn_mode = 'pessimistic'");
    session2.must_exec("set tidb_txn_mode = 'pessimistic'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("begin;");
    session1.check("select c from x where id < 5;", &["1"]);
    session2.must_exec("begin;");
    session2.must_exec("insert into x values(2, 1);");
    session2.must_exec("commit;");
    session1.must_exec("update x set c = c+1 where id < 5;");
    session1.check("select c from x where id < 5;", &["2", "2"]);
    session1.must_exec("commit;");
    session1.must_exec("set tx_isolation = 'READ-COMMITTED'");
    session2.must_exec("set tx_isolation = 'READ-COMMITTED'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("begin;");
    session1.check("select c from x where id < 5;", &["1"]);
    session2.must_exec("begin;");
    session2.must_exec("insert into x values(2, 1);");
    session2.must_exec("commit;");
    session1.must_exec("update x set c = c+1 where id < 5;");
    session1.check("select c from x where id < 5;", &["2", "2"]);
    session1.must_exec("commit;");
}

/// Go `txntest.TestStatementErrorInTransaction` (txn_test.go).
#[test]
fn test_statement_error_in_transaction() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("create table statement_side_effect (c int primary key)");
    tk.must_exec("begin");
    tk.must_exec("insert into statement_side_effect values (1)");
    tk.exec_to_err("insert into statement_side_effect value (2),(3),(4),(1)");
    tk.check("select * from statement_side_effect", &["1"]);
    tk.must_exec("commit");
    tk.check("select * from statement_side_effect", &["1"]);
    tk.must_exec("drop table if exists test;");
    tk.must_exec("create table test (\n \t\t  a int(11) DEFAULT NULL,\n \t\t  b int(11) DEFAULT NULL\n \t) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;");
    tk.must_exec("insert into test values (1, 2), (1, 2), (1, 1), (1, 1);");
    tk.must_exec("start transaction;");
    tk.exec_to_err("update tset set b=11 where a=1 and b=2;");
    tk.must_exec("update test set b = 11 where a = 1 and b = 2;");
    tk.must_exec("rollback");
    tk.check("select * from test where a = 1 and b = 11", &[]);
}

/// Go `txntest.TestWriteConflictMessage` (txn_test.go).
#[test]
fn test_write_conflict_message() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    tk.must_exec("use test");
    tk2.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (c int primary key)");
    tk.must_exec("begin optimistic");
    tk2.must_exec("insert into t values (1)");
    tk.must_exec("insert into t values (1)");
    let err = tk.exec_to_err("commit");
    assert!(err.contains("Write conflict"), "{err:?}");
    assert!(err.contains("tableName=test.t, handle=1}"), "{err:?}");
    assert!(err.contains("reason=Optimistic"), "{err:?}");
    tk.must_exec("create table t2 (id varchar(30) primary key clustered)");
    tk.must_exec("begin optimistic");
    tk2.must_exec("insert into t2 values ('hello')");
    tk.must_exec("insert into t2 values ('hello')");
    let err = tk.exec_to_err("commit");
    assert!(err.contains("Write conflict"), "{err:?}");
    assert!(err.contains("tableName=test.t2, handle={hello}"), "{err:?}");
    assert!(err.contains("reason=Optimistic"), "{err:?}");
}

/// Go `txntest.TestDuplicateErrorMessage` (txn_test.go).
#[test]
fn test_duplicate_error_message() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut tk2 = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("set @@tx_isolation='read-committed'");
    tk2.must_exec("use test");
    tk.must_exec("set @@tidb_constraint_check_in_place_pessimistic=off");
    tk.must_exec("create table t (c int primary key, v int)");
    tk.must_exec("create table t2 (c int primary key, v int)");
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert into t values (1, 1)");
    tk2.must_exec("insert into t values (1, 1)");
    tk2.must_exec("insert into t2 values (1, 2)");
    tk.must_contain_err_msg("update t set v = v + 1 where c = 1", "Duplicate entry '1' for key 't.PRIMARY'");
    tk.must_exec("create table t3 (c int, v int, unique key i1(v))");
    tk.must_exec("create table t4 (c int, v int, unique key i1(v))");
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert into t3 values (1, 1)");
    tk2.must_exec("insert into t3 values (1, 1)");
    tk2.must_exec("insert into t4 values (1, 2)");
    tk.must_contain_err_msg("update t3 set c = c + 1 where v = 1", "Duplicate entry '1' for key 't3.i1'");
}

/// Go `txntest.TestAssertionWhenPessimisticLockLost` (txn_test.go).
#[test]
fn test_assertion_when_pessimistic_lock_lost() {
    let store = realtikv!();
    let mut tk1 = store.new_tk();
    let mut tk2 = store.new_tk();
    tk1.must_exec("set @@tidb_constraint_check_in_place_pessimistic=0");
    tk1.must_exec("set @@tidb_txn_assertion_level=strict");
    tk2.must_exec("set @@tidb_constraint_check_in_place_pessimistic=0");
    tk2.must_exec("set @@tidb_txn_assertion_level=strict");
    tk1.must_exec("use test");
    tk2.must_exec("use test");
    tk1.must_exec("create table t (id int primary key, val text)");
    tk1.must_exec("begin pessimistic");
    tk1.must_exec("select * from t where id = 1 for update");
    tk2.must_exec("begin pessimistic");
    tk2.must_exec("insert into t values (1, 'b')");
    tk2.must_exec("insert into t values (2, 'b')");
    tk2.must_exec("commit");
    tk1.must_exec("select * from t where id = 2 for update");
    tk1.must_exec("insert into t values (1, 'a') on duplicate key update val = concat(val, 'a')");
    let err = tk1.exec_to_err("commit");
    assert!(!err.contains("assertion"), "{err:?}");
}

/// Go `txntest.TestP0DirtyWrite` (isolation_test.go).
#[test]
fn test_p0_dirty_write() {
    let store = realtikv!();
    let mut session1 = store.new_tk();
    let mut session2 = store.new_tk();
    session1.must_exec("use test;");
    session2.must_exec("use test;");
    session1.must_exec("set tidb_txn_mode = 'optimistic'");
    session2.must_exec("set tidb_txn_mode = 'optimistic'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("begin;");
    session1.must_exec("update x set c = c+1 where id = 1;");
    session2.must_exec("begin;");
    session2.must_exec("update x set c = c+1 where id = 1;");
    session1.must_exec("commit;");
    let err = session2.exec("commit;");
    assert!(err.is_err());
    session1.must_exec("set tidb_txn_mode = 'pessimistic'");
    session2.must_exec("set tidb_txn_mode = 'pessimistic'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("begin;");
    session1.must_exec("update x set c = c+1 where id = 1;");
    session2.must_exec("begin;");
    
    std::thread::scope(|scope| {
let worker = scope.spawn(|| {
    session2.must_exec("update x set c = c+1 where id = 1;");
    });
    session1.must_exec("commit;");
    worker.join().unwrap();
});
    session2.must_exec("commit;");
    session1.must_exec("set tx_isolation = 'READ-COMMITTED'");
    session2.must_exec("set tx_isolation = 'READ-COMMITTED'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("begin;");
    session1.must_exec("update x set c = c+1 where id = 1;");
    session2.must_exec("begin;");
    std::thread::scope(|scope| {
let worker = scope.spawn(|| {
    session2.must_exec("update x set c = c+1 where id = 1;");
    });
    session1.must_exec("commit;");
    worker.join().unwrap();
});
    session2.must_exec("commit;");
    session2.check("select * from x", &["1 3"]);

}

/// Go `txntest.TestP3Phantom` (isolation_test.go).
#[test]
fn test_p3_phantom() {
    let store = realtikv!();
    let mut session1 = store.new_tk();
    let mut session2 = store.new_tk();
    let db_name = format!("test_p3_phantom_{}", std::process::id());
    session1.must_exec(&format!("drop database if exists {db_name}"));
    session1.must_exec(&format!("create database {db_name}"));
    
    session1.must_exec(&format!("use {db_name}"));
    session2.must_exec(&format!("use {db_name}"));
    session1.must_exec("set tidb_txn_mode = 'optimistic'");
    session2.must_exec("set tidb_txn_mode = 'optimistic'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("drop table if exists z;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("create table z (id int primary key, c int);");
    session1.must_exec("insert into z values(1, 1);");
    session1.must_exec("begin;");
    session2.must_exec("begin;");
    session1.check("select c from x where id < 5;", &["1"]);
    session2.must_exec("insert into x values(2, 1);");
    session2.check("select c from z where id = 1;", &["1"]);
    session2.must_exec("update z set c = c+1 where id = 1;");
    session2.must_exec("commit;");
    session1.check("select c from z where id = 1;", &["1"]);
    session1.must_exec("commit;");
    session1.must_exec("set tidb_txn_mode = 'pessimistic'");
    session2.must_exec("set tidb_txn_mode = 'pessimistic'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("drop table if exists z;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("create table z (id int primary key, c int);");
    session1.must_exec("insert into z values(1, 1);");
    session1.must_exec("begin;");
    session2.must_exec("begin;");
    session1.check("select c from x where id < 5;", &["1"]);
    session2.must_exec("insert into x values(2, 1);");
    session2.check("select c from z where id = 1;", &["1"]);
    session2.must_exec("update z set c = c+1 where id = 1;");
    session2.must_exec("commit;");
    session1.check("select c from z where id = 1;", &["1"]);
    session1.must_exec("commit;");
    session1.must_exec("set tx_isolation = 'READ-COMMITTED'");
    session2.must_exec("set tx_isolation = 'READ-COMMITTED'");
    session1.must_exec("drop table if exists x;");
    session1.must_exec("drop table if exists z;");
    session1.must_exec("create table x (id int primary key, c int);");
    session1.must_exec("insert into x values(1, 1);");
    session1.must_exec("create table z (id int primary key, c int);");
    session1.must_exec("insert into z values(1, 1);");
    session1.must_exec("begin;");
    session2.must_exec("begin;");
    session1.check("select c from x where id < 5;", &["1"]);
    session2.must_exec("insert into x values(2, 1);");
    session2.check("select c from z where id = 1;", &["1"]);
    session2.must_exec("update z set c = c+1 where id = 1;");
    session2.must_exec("commit;");
    session1.check("select c from z where id = 1;", &["2"]);
    session1.must_exec("commit;");
    session1.must_exec(&format!("drop database if exists {db_name}"));
}

/// Go `txntest.TestSelectForUpdateWriteConflict` (txn_test.go).
#[test]
fn test_select_for_update_write_conflict() {
    let store = realtikv!();
    let mut tk1 = store.new_tk();
    let mut tk2 = store.new_tk();
    tk1.must_exec("use test");
    tk2.must_exec("use test");
    tk1.must_exec("drop table if exists t");
    tk1.must_exec("create table t (id int primary key, val int)");
    tk1.must_exec("insert into t values (1, 100)");
    tk1.must_exec("begin optimistic");
    tk1.must_exec("select * from t where id = 1 for update");
    tk2.must_exec("begin optimistic");
    tk2.must_exec("update t set val = 200 where id = 1");
    tk1.must_exec("commit");
    let t2err = tk2.exec_to_err("commit");
    
    assert!(t2err.contains("Write conflict"), "{t2err}");
    tk1.check("select * from t where id = 1", &["1 100"]);

}

/// Go `txntest.TestInTxnPSProtoPointGet`.
#[test]
fn test_in_txn_ps_proto_point_get() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("create table t1(c1 int primary key, c2 int, c3 int)");
    tk.must_exec("insert into t1 values(1, 10, 100)");
    let id = tk.prepare("select c1, c2 from t1 where c1 = ?");
    let id_for_update = tk.prepare("select c1, c2 from t1 where c1 = ? for update");
    let params = [tidb_param::PreparedValue::SignedLongLong(1)];
    for _ in 0..2 {
        for stmt in [&id, &id_for_update] {
            assert_eq!(tk.execute_prepared(stmt, &params).unwrap(), vec![vec!["1", "10"]]);
        }
    }
    tk.must_exec("start transaction");
    for stmt in [&id, &id_for_update] {
        assert_eq!(tk.execute_prepared(stmt, &params).unwrap(), vec![vec!["1", "10"]]);
        assert!(tk.session().in_transaction());
    }
    tk.must_exec("update t1 set c2 = c2 + 1");
    for stmt in [&id, &id_for_update] {
        assert_eq!(tk.execute_prepared(stmt, &params).unwrap(), vec![vec!["1", "11"]]);
    }
    assert!(tk.session().in_transaction());
    tk.must_exec("commit");
}
/// Go `txntest.TestForeignKeySharedLockOptimisticReverseReferenceOrder` (shared_lock_test.go).
#[test]
fn test_foreign_key_shared_lock_optimistic_reverse_reference_order() {
    
    let store = realtikv!();
    let mut tk1 = store.new_tk();
    let mut tk2 = store.new_tk();
    tk1.must_exec("use test");
    tk2.must_exec("use test");
    tk1.must_exec("set @@tidb_foreign_key_check_in_shared_lock = ON");
    tk2.must_exec("set @@tidb_foreign_key_check_in_shared_lock = ON");
    prepare_foreign_key_tables(&mut tk1);
    tk1.must_exec("begin optimistic");
    tk2.must_exec("begin optimistic");
    tk1.must_exec("insert into child values (1, 1)");
    tk2.must_exec("insert into child values (3, 2)");
    tk1.must_exec("insert into child values (2, 2)");
    tk2.must_exec("insert into child values (4, 1)");
    tk1.must_exec("commit");
    let err = tk2.exec_to_err("commit");
    assert!(err.contains("Write conflict"), "{err:?}");
    tk1.check("select * from child order by id", &["1 1", "2 2"]);

}

/// Go `txntest.TestForeignKeySharedLockPessimisticReverseReferenceOrder` (shared_lock_test.go).
#[test]
fn test_foreign_key_shared_lock_pessimistic_reverse_reference_order() {
    
    let store = realtikv!();
    let mut tk1 = store.new_tk();
    let mut tk2 = store.new_tk();
    tk1.must_exec("use test");
    tk2.must_exec("use test");
    tk1.must_exec("set @@tidb_foreign_key_check_in_shared_lock = ON");
    tk2.must_exec("set @@tidb_foreign_key_check_in_shared_lock = ON");
    tk1.must_exec("set @@tidb_pessimistic_txn_fair_locking = 0");
    tk2.must_exec("set @@tidb_pessimistic_txn_fair_locking = 0");
    prepare_foreign_key_tables(&mut tk1);
    tk1.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk1.must_exec("insert into child values (1, 1)");
    tk2.must_exec("insert into child values (3, 2)");
    tk1.must_exec("insert into child values (2, 2)");
    tk2.must_exec("insert into child values (4, 1)");
    tk1.must_exec("commit");
    tk2.must_exec("commit");
    tk1.check("select * from child order by id", &["1 1", "2 2", "3 2", "4 1"]);

}

/// Go `txntest.prepareForeignKeyTables`.
fn prepare_foreign_key_tables(tk: &mut crate::realtikvtest::Tk) {
    tk.must_exec("drop table if exists child, parent");
    tk.must_exec("create table parent (id int primary key)");
    tk.must_exec("create table child (id int primary key, pid int, foreign key (pid) references parent(id))");
    tk.must_exec("insert into parent values (1), (2)");
}

/// Go `txntest.TestSharedLockBlockedByExclusiveLock`.
#[test]
fn test_shared_lock_blocked_by_exclusive_lock() {
    let store = realtikv!();
    let mut tk1 = store.new_tk();
    let mut tk2 = store.new_tk();
    let mut tk3 = store.new_tk();
    for tk in [&mut tk1, &mut tk2, &mut tk3] {
        tk.must_exec("use test");
        tk.must_exec("set @@tidb_foreign_key_check_in_shared_lock = ON");
        tk.must_exec("set @@tidb_pessimistic_txn_fair_locking = 0");
    }
    prepare_foreign_key_tables(&mut tk1);
    for tk in [&mut tk1, &mut tk2, &mut tk3] { tk.must_exec("begin pessimistic"); }
    tk1.must_exec("select * from parent where id=1 for update");
    let tk2_done = std::thread::spawn(move || { tk2.must_exec("insert into child values(1, 1)"); tk2 });
    let tk3_done = std::thread::spawn(move || { tk3.must_exec("insert into child values(2, 1)"); tk3 });
    std::thread::sleep(std::time::Duration::from_millis(500));
    assert!(!tk2_done.is_finished(), "tk2 should be blocked");
    assert!(!tk3_done.is_finished(), "tk3 should be blocked");
    tk1.must_exec("commit");
    let mut tk2 = tk2_done.join().unwrap();
    let mut tk3 = tk3_done.join().unwrap();
    tk1.check("select * from child", &[]);
    tk2.must_exec("commit");
    tk1.check("select * from child", &["1 1"]);
    tk3.must_exec("commit");
    tk1.check("select * from child", &["1 1", "2 1"]);
    tk1.must_exec("admin check table parent");
    tk1.must_exec("admin check table child");
}

/// Go `txntest.TestSharedLockBlockExclusiveLock`.
#[test]
fn test_shared_lock_block_exclusive_lock() {
    let store = realtikv!();
    let mut tk1 = store.new_tk();
    let mut tk2 = store.new_tk();
    let mut tk3 = store.new_tk();
    for tk in [&mut tk1, &mut tk2, &mut tk3] {
        tk.must_exec("use test");
        tk.must_exec("set @@tidb_foreign_key_check_in_shared_lock = ON");
        tk.must_exec("set @@tidb_pessimistic_txn_fair_locking = 0");
    }
    prepare_foreign_key_tables(&mut tk1);
    for tk in [&mut tk1, &mut tk2, &mut tk3] { tk.must_exec("begin pessimistic"); }
    tk2.must_exec("insert into child values(1, 1)");
    tk3.must_exec("insert into child values(2, 1)");
    let tk1_done = std::thread::spawn(move || { tk1.must_exec("select * from parent where id=1 for update"); tk1 });
    std::thread::sleep(std::time::Duration::from_millis(500));
    assert!(!tk1_done.is_finished(), "tk1 should be blocked");
    tk2.must_exec("commit");
    tk2.check("select * from child", &["1 1"]);
    std::thread::sleep(std::time::Duration::from_millis(500));
    assert!(!tk1_done.is_finished(), "tk1 should be blocked");
    tk3.must_exec("commit");
    tk3.check("select * from child", &["1 1", "2 1"]);
    let mut tk1 = tk1_done.join().unwrap();
    tk1.must_exec("commit");
    tk1.must_exec("admin check table parent");
    tk1.must_exec("admin check table child");
}

/// Go `txntest.TestSharedLockCascadeUpdateExplicitPessimisticTxn`.
#[test]
fn test_shared_lock_cascade_update_explicit_pessimistic_txn() {
    let store = realtikv!();
    for constraint_check_in_place in ["ON", "OFF"] {
        let mut tk = store.new_tk();
        tk.must_exec("use test");
        tk.must_exec("set @@global.tidb_enable_foreign_key=1");
        tk.must_exec("set @@foreign_key_checks=1");
        tk.must_exec("set @@tidb_foreign_key_check_in_shared_lock=ON");
        tk.must_exec(&format!("set @@tidb_constraint_check_in_place_pessimistic={constraint_check_in_place}"));
        tk.must_exec("drop table if exists c, p");
        tk.must_exec("create table p(id int primary key)");
        tk.must_exec("create table c(pid int, foreign key(pid) references p(id) on delete cascade on update cascade)");
        tk.must_exec("insert into p values (1)");
        tk.must_exec("insert into c values (1)");
        tk.must_exec("begin pessimistic");
        tk.must_exec("update p set id = 2 where id = 1");
        tk.check("select pid from c", &["2"]);
        tk.must_exec("commit");
        tk.check("select pid from c", &["2"]);
        tk.must_exec("delete from p where id = 2");
        tk.check("select count(*) from c", &["0"]);
        tk.must_exec("admin check table p");
        tk.must_exec("admin check table c");
        tk.must_exec("set @@global.tidb_enable_foreign_key=default");
    }
}
/// Go `txntest.TestStaleReadFutureTime`.
#[test]
fn test_stale_read_future_time() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("create table t (id int)");
    for sql in [
        "start transaction read only as of timestamp '2038-01-18 03:14:07'",
        "set transaction read only as of timestamp '2038-01-18 03:14:07'",
        "select * from t as of timestamp '2038-01-18 03:14:07'",
    ] {
        tk.must_contain_err_msg(sql, "cannot set read timestamp to a future time");
        assert!(!tk.session().in_transaction());
        assert_eq!(tk.session().vars().txn_read_ts(), 0);
    }
}

/// Go `txntest.TestStalePrepare`.
#[test]
fn test_stale_prepare() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (id int)");
    std::thread::sleep(std::time::Duration::from_millis(200));
    let stmt = tk.prepare("select * from t as of timestamp now(3) - interval 100000 microsecond order by id asc");
    tk.must_exec("prepare stmt from \"select * from t as of timestamp now(3) - interval 100000 microsecond order by id asc\"");
    let mut expected = Vec::<Vec<String>>::new();
    for i in 0..20 {
        tk.must_exec(&format!("insert into t values({i})"));
        std::thread::sleep(std::time::Duration::from_millis(200));
        expected.push(vec![i.to_string()]);
        assert_eq!(tk.execute_prepared(&stmt, &[]).unwrap(), expected);
        assert_eq!(tk.must_query("execute stmt"), expected);
    }
    tk.must_exec("drop table if exists t");
}

/// Go `txntest.TestPlanCacheWithStaleReadByBinaryProto`.
#[test]
fn test_plan_cache_with_stale_read_by_binary_proto() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("create table t1 (id int primary key, v int)");
    tk.must_exec("insert into t1 values(1, 10)");
    std::thread::sleep(std::time::Duration::from_millis(100));
    tk.must_exec("set @a=now(6)");
    std::thread::sleep(std::time::Duration::from_secs(1));
    tk.must_exec("update t1 set v=100 where id=1");
    let stmt1 = tk.prepare("select * from t1 as of timestamp @a where id=1");
    for _ in 0..3 {
        assert_eq!(tk.execute_prepared(&stmt1, &[]).unwrap(), vec![vec!["1", "10"]]);
    }
    let stmt2 = tk.prepare("select * from t1 where id=1");
    for _ in 0..2 {
        assert_eq!(tk.execute_prepared(&stmt2, &[]).unwrap(), vec![vec!["1", "100"]]);
    }
    tk.must_exec("set @@tx_read_ts=@a");
    assert_eq!(tk.execute_prepared(&stmt2, &[]).unwrap(), vec![vec!["1", "10"]]);
}

/// Go `txntest.TestAsOfTimestampCompatibility`.
#[test]
fn test_as_of_timestamp_compatibility() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    set_safe_point(&mut tk);
    tk.must_exec("use test");
    tk.must_exec("create table t5(id int)");
    std::thread::sleep(std::time::Duration::from_secs(1));
    let time1 = (chrono::Local::now() - chrono::Duration::milliseconds(500)).format("%Y-%m-%d %H:%M:%S%.3f").to_string();
    for sql in [format!("SET TRANSACTION READ ONLY AS OF TIMESTAMP '{time1}'"), format!("select * from t5 as of timestamp '{time1}'")] {
        for begin in [format!("START TRANSACTION READ ONLY AS OF TIMESTAMP '{time1}'"), "begin".into(), "start transaction".into()] {
            tk.must_exec(&begin);
            let err = tk.exec_to_err(&sql);
            assert!(err.contains("as of timestamp can't be set in transaction") || err.contains("Transaction characteristics can't be changed while a transaction is in progress"), "{err}");
            tk.must_exec("commit");
        }
    }
    tk.must_exec("create table test.table1 (id int primary key, a int)");
    std::thread::sleep(std::time::Duration::from_secs(1));
    let time1 = (chrono::Local::now() - chrono::Duration::milliseconds(500)).format("%Y-%m-%d %H:%M:%S%.3f").to_string();
    tk.must_exec(&format!("explain analyze select * from test.table1 as of timestamp '{time1}' where id = 1"));
    tk.must_exec("drop table if exists test.table1");
    tk.must_exec("drop table if exists t5");
}

/// Go `txntest` stale-read tests' `updateSafePoint` setup.
fn set_safe_point(tk: &mut crate::realtikvtest::Tk) {
    tk.must_exec("INSERT INTO mysql.tidb VALUES ('tikv_gc_safe_point', '20160102-15:04:05 -0700', 'All versions after safe point can be accessed. (DO NOT EDIT)') ON DUPLICATE KEY UPDATE variable_value = '20160102-15:04:05 -0700', comment = 'All versions after safe point can be accessed. (DO NOT EDIT)'");
}
/// Go `txntest.TestEntriesCountAndSize`.
#[test]
fn test_entries_count_and_size() {
    let store = realtikv!();
    let processes = store.processes();
    let mut tk = store.new_tk();
    let id = tk.session().vars().connection_id.unwrap();
    tk.must_exec("use test");
    tk.must_exec("create table t(a int)");
    tk.must_exec("begin pessimistic");
    for count in 1..=2 {
        tk.must_exec(&format!("insert into t(a) values ({count})"));
        assert_eq!(txn_info(&processes, id).unwrap().mem_buffer_keys, count);
    }
    tk.must_exec("commit");
}

/// Go `session.TxnInfo`, read through the session manager's same live transaction entry.
fn txn_info(processes: &tidb_sessmgr::ProcessRegistry, id: u64) -> Option<tidb_sessmgr::TransactionRow> {
    processes.transaction_snapshot().into_iter().find(|row| row.session_id == id)
}

/// Go `txntest.TestRunning`.
#[test]
fn test_running() {
    let store = realtikv!();
    let processes = store.processes();
    let mut tk = store.new_tk();
    let id = tk.session().vars().connection_id.unwrap();
    tk.must_exec("use test");
    tk.must_exec("create table t(a int)");
    tk.must_exec("insert into t(a) values (1)");
    tk.must_exec("begin pessimistic");
    let slow = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/session/mockStmtSlow", "return(200)");
    let worker = std::thread::spawn(move || {
        tk.must_exec("select * from t for update /* sleep */;");
        tk.must_exec("commit");
    });
    std::thread::sleep(std::time::Duration::from_millis(100));
    let info = txn_info(&processes, id);
    drop(slow);
    worker.join().unwrap();
    assert_eq!(info.unwrap().state, "Running");
}

/// Go `txntest.TestBlocked`.
#[test]
fn test_blocked() {
    let store = realtikv!();
    let processes = store.processes();
    let mut tk1 = store.new_tk();
    let mut tk2 = store.new_tk();
    let id = tk2.session().vars().connection_id.unwrap();
    tk1.must_exec("use test");
    tk2.must_exec("use test");
    tk1.must_exec("create table t(a int)");
    tk1.must_exec("insert into t(a) values (1)");
    tk1.must_exec("begin pessimistic");
    tk1.must_exec("select * from t where a = 1 for update");
    let worker = std::thread::spawn(move || {
        tk2.must_exec("begin pessimistic");
        tk2.must_exec("select * from t where a = 1 for update");
        tk2.must_exec("commit");
    });
    std::thread::sleep(std::time::Duration::from_millis(100));
    let info = txn_info(&processes, id).unwrap();
    tk1.must_exec("commit");
    worker.join().unwrap();
    assert_eq!(info.state, "LockWaiting");
    assert!(info.waiting_start.is_some());
}

/// Go `txntest.TestCommitting`.
#[test]
fn test_committing() {
    let store = realtikv!();
    let processes = store.processes();
    let mut tk1 = store.new_tk();
    let mut tk2 = store.new_tk();
    let id = tk2.session().vars().connection_id.unwrap();
    tk1.must_exec("use test");
    tk2.must_exec("use test");
    tk1.must_exec("create table t(a int)");
    tk1.must_exec("insert into t(a) values (1), (2)");
    tk1.must_exec("begin pessimistic");
    tk1.must_exec("select * from t where a = 1 for update");
    tk2.must_exec("begin pessimistic");
    assert!(txn_info(&processes, id).is_some());
    tk2.must_exec("select * from t where a = 2 for update");
    let slow = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/session/mockSlowCommit", "pause");
    let worker = std::thread::spawn(move || tk2.must_exec("commit"));
    std::thread::sleep(std::time::Duration::from_millis(100));
    let info = txn_info(&processes, id);
    drop(slow);
    tk1.must_exec("commit");
    worker.join().unwrap();
    assert_eq!(info.unwrap().state, "Committing");
}

/// Go `txntest.TestRollbackTxnState`.
#[test]
fn test_rollback_txn_state() {
    let store = realtikv!();
    let processes = store.processes();
    let mut tk = store.new_tk();
    let id = tk.session().vars().connection_id.unwrap();
    tk.must_exec("use test");
    tk.must_exec("create table t(a int)");
    tk.must_exec("insert into t(a) values (1), (2)");
    tk.must_exec("begin pessimistic");
    tk.must_exec("insert into t(a) values (3)");
    let slow = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/session/mockSlowRollback", "pause");
    let worker = std::thread::spawn(move || tk.must_exec("rollback"));
    std::thread::sleep(std::time::Duration::from_millis(100));
    // Capture before releasing the pause to avoid Go's race with txn.reset.
    let info = txn_info(&processes, id);
    drop(slow);
    worker.join().unwrap();
    assert_eq!(info.unwrap().state, "RollingBack");
}
/// Go `txntest.TestLockKeysInDML` (txn_test.go).
#[test]
fn test_lock_keys_in_dml() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("create table t1 (id int primary key);");
    tk.must_exec("create table t2 (id int primary key, foreign key fk (id) references t1(id));");
    tk.must_exec("insert into t1 values (1)");
    tk.must_exec("BEGIN");
    tk.must_exec("INSERT INTO t2 VALUES (1)");
    let start = std::time::Instant::now();
let mut tk2 = store.new_tk();
let worker = std::thread::spawn(move || {
    tk2.must_exec("use test");
    tk2.must_exec("BEGIN");
    tk2.exec_to_err("UPDATE t1 SET id = 2 WHERE id = 1");
    tk2.must_exec("COMMIT");
    std::time::Instant::now()
});
    let sleep_duration = std::time::Duration::from_millis(500);
std::thread::sleep(sleep_duration);
    tk.must_exec("COMMIT");
    let committed = worker.join().unwrap();
assert!(committed.duration_since(start) > sleep_duration);
    tk.check("SELECT * FROM t1", &["1"]);
    tk.check("SELECT * FROM t2", &["1"]);

}

/// Go `txntest.TestSelectLockForPartitionTable`.
#[test]
fn test_select_lock_for_partition_table() {
    let store = realtikv!();
    let mut tk1 = store.new_tk();
    let mut tk2 = store.new_tk();
    tk1.must_exec("use test");
    tk1.must_exec("create table t(a int, b int, c int, key idx(a, b, c)) PARTITION BY HASH (c) PARTITIONS 10");
    tk1.must_exec("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3)");
    tk1.must_exec("analyze table t");
    tk1.must_exec("begin");
    let sql = "select * from t use index(idx) where a = 1 and b = 1 order by a limit 1 for update";
    tk1.must_have_plan(sql, "IndexReader");
    tk1.must_exec(sql);
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || {
        tk2.must_exec("use test");
        tk2.must_exec("begin");
        tx.send(false).unwrap();
        tk2.must_exec(sql);
        tx.send(true).unwrap();
    });
    assert!(!rx.recv().unwrap());
    std::thread::sleep(std::time::Duration::from_millis(10));
    assert!(matches!(rx.try_recv(), Err(std::sync::mpsc::TryRecvError::Empty)));
    tk1.must_exec("commit");
    assert!(rx.recv().unwrap());
    worker.join().unwrap();
}

/// Go `txntest.TestValidateReadOnlyInStalenessTransaction` (stale_read_test.go).
#[test]
fn test_validate_read_only_in_staleness_transaction() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    set_safe_point(&mut tk);
    tk.must_exec("use test");
    tk.must_exec("create table t (id int)");
    tk.must_exec("create table t1 (id int)");
    std::thread::sleep(std::time::Duration::from_secs(1));
    tk.must_exec("PREPARE stmt1 FROM 'insert into t(id) values (5);'");
    tk.must_exec("PREPARE stmt2 FROM 'select * from t'");
    tk.must_exec("set @@tidb_enable_noop_functions=1");
    for (sql, valid, valid_without_start, error) in [
("select * from t;", true, false, ""),
("explain insert into t (id) values (1);", false, true, "GetForUpdateTS not supported for stalenessTxnProvider"),
("explain analyze insert into t (id) values (1);", false, false, "only support read-only statement during read-only staleness transactions"),
("explain analyze select * from t ", true, false, ""),
("EXECUTE stmt1;", false, false, "only support read-only statement during read-only staleness transactions"),
("EXECUTE stmt2;", true, false, ""),
("show tables;", true, false, ""),
("SELECT 1, 2 UNION SELECT 'a', 'b';", true, false, ""),
("insert into t (id) values (1);", false, false, "only support read-only statement during read-only staleness transactions"),
("delete from t where id =1", false, false, "only support read-only statement during read-only staleness transactions"),
("update t set id =2 where id =1", false, false, "only support read-only statement during read-only staleness transactions"),
("select * from t where id = 1", true, false, ""),
("select * from t where id in (1,2,3);", true, false, ""),
("SPLIT TABLE t BETWEEN (0) AND (1000000000) REGIONS 16;", true, false, ""),
("DO SLEEP(1);", true, false, ""),
("select * from t where id = 1 for update", false, false, "select lock hasn't been supported in stale read yet"),
("select * from t where id = 1 lock in share mode", false, false, "select lock hasn't been supported in stale read yet"),
("select * from t for update union select * from t;", false, false, "only support read-only statement during read-only staleness transactions"),
("replace into t(id) values (1)", false, false, "only support read-only statement during read-only staleness transactions"),
("LOAD DATA LOCAL INFILE '/mn/asa.csv' INTO TABLE t FIELDS TERMINATED BY x'2c' ENCLOSED BY b'100010' LINES TERMINATED BY '\r\n' IGNORE 1 LINES (id);", false, false, "only support read-only statement during read-only staleness transactions"),
("update t,t1 set t.id = 1,t1.id = 2 where t.1 = 2 and t1.id = 3;", false, false, "only support read-only statement during read-only staleness transactions"),
("delete t from t1 where t.id = t1.id", false, false, "only support read-only statement during read-only staleness transactions"),
("insert into t select * from t1;", false, false, "only support read-only statement during read-only staleness transactions")] {
        tk.must_exec("START TRANSACTION READ ONLY AS OF TIMESTAMP NOW(3) - INTERVAL 5 SECOND");
        if valid { tk.must_exec(sql); } else { tk.must_contain_err_msg(sql, error); }
        tk.must_exec("commit");
        tk.must_exec("set transaction read only as of timestamp NOW(3) - INTERVAL 5 SECOND");
        if valid || valid_without_start { tk.must_exec(sql); } else { tk.must_contain_err_msg(sql, error); }
        tk.must_exec("set transaction read only as of timestamp ''");
    }
}

/// Go `txntest.TestSpecialSQLInStalenessTxn` (stale_read_test.go).
#[test]
fn test_special_sql_in_staleness_txn() {
    use tidb_executor::sessionctx::SessionContext as _;
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("CREATE USER IF NOT EXISTS 'newuser' IDENTIFIED BY 'mypassword'");
    for (sql, same_session) in [
("create table t (id int, b int,INDEX(b));", false),
("SET GLOBAL sql_mode = 'STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER';", true),
("analyze table t", true),
("CREATE SESSION BINDING FOR  SELECT * FROM t WHERE b = 123 USING SELECT * FROM t IGNORE INDEX (b) WHERE b = 123;", true),
("CREATE GLOBAL BINDING FOR  SELECT * FROM t WHERE b = 123 USING SELECT * FROM t IGNORE INDEX (b) WHERE b = 123;", true),
("GRANT ALL ON test.* TO 'newuser';", false),
("REVOKE ALL ON test.* FROM 'newuser';", false)
] {
        std::thread::sleep(std::time::Duration::from_secs(1));
        tk.must_exec("START TRANSACTION READ ONLY AS OF TIMESTAMP NOW(3) - INTERVAL 100000 microsecond");
        assert!(tk.session().txn_is_staleness());
        tk.must_exec(sql);
        assert_eq!(tk.session().txn_is_staleness(), same_session, "{sql}");
    }
}
/// Go `txntest.TestTxnEntrySizeLimit` (txn_test.go).
#[test]
fn test_txn_entry_size_limit() {
    let store = realtikv!();
    let mut tk1 = store.new_tk();
    let mut tk2 = store.new_tk();
    tk1.must_exec("use test");
    tk2.must_exec("use test");
    tk1.must_exec("create table t (a int, b longtext)");
    tk1.must_contain_err_msg("insert into t values (1, repeat('a', 7340032))", "[kv:8025]entry too large, the max entry size is 6291456");
    tk1.must_exec("set session tidb_txn_entry_size_limit=8388608");
    tk1.must_exec("insert into t values (1, repeat('a', 7340032))");
    tk1.must_contain_err_msg("insert into t values (1, repeat('a', 9427968))", "[kv:8025]entry too large, the max entry size is 8388608");
    tk2.must_contain_err_msg("insert into t values (1, repeat('a', 7340032))", "[kv:8025]entry too large, the max entry size is 6291456");
    let mut tk3 = store.new_tk();
    tk3.must_exec("use test");
    tk3.must_contain_err_msg("insert into t values (1, repeat('a', 7340032))", "[kv:8025]entry too large, the max entry size is 6291456");
    tk1.must_contain_err_msg("alter table t modify column a varchar(255)", "[kv:8025]entry too large, the max entry size is 6291456");
    tk1.must_exec("set global tidb_txn_entry_size_limit=8388608");
    tk1.must_exec("alter table t modify column a varchar(255)");
    tk2.must_exec("alter table t modify column a int");
    tk2.must_contain_err_msg("insert into t values (1, repeat('a', 7340032))", "[kv:8025]entry too large, the max entry size is 6291456");
    tk3.must_contain_err_msg("insert into t values (1, repeat('a', 7340032))", "[kv:8025]entry too large, the max entry size is 6291456");
    let mut tk4 = store.new_tk();
    tk4.must_exec("use test");
    tk4.must_exec("insert into t values (2, repeat('b', 7340032))");
    tk1.must_exec("set global tidb_txn_entry_size_limit=0");
    tk1.must_contain_err_msg("alter table t modify column a varchar(255)", "[kv:8025]entry too large, the max entry size is 6291456");
    tk2.must_contain_err_msg("alter table t modify column a varchar(255)", "[kv:8025]entry too large, the max entry size is 6291456");
    tk3.must_contain_err_msg("alter table t modify column a varchar(255)", "[kv:8025]entry too large, the max entry size is 6291456");
    tk4.must_contain_err_msg("alter table t modify column a varchar(255)", "[kv:8025]entry too large, the max entry size is 6291456");
    tk1.must_exec("insert into t values (3, repeat('c', 7340032))");
    tk1.must_exec("set session tidb_txn_entry_size_limit=0");
    tk1.must_contain_err_msg("insert into t values (1, repeat('a', 7340032))", "[kv:8025]entry too large, the max entry size is 6291456");

}

/// Go `txntest.TestStaleSelect` (stale_read_test.go).
#[test]
fn test_stale_select() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (id int)");
    let tolerance = std::time::Duration::from_millis(50);
    std::thread::sleep(tolerance);
    tk.must_exec("insert into t values (1)");
    std::thread::sleep(tolerance);
    let time1 = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string();
    std::thread::sleep(tolerance);
    tk.must_exec("insert into t values (2)");
    std::thread::sleep(tolerance);
    let time2 = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string();
    std::thread::sleep(tolerance);
    tk.must_exec("insert into t values (3)");
    std::thread::sleep(tolerance);
    
    let stale_sql = format!("select * from t as of timestamp '{time1}'");
    tk.check(&stale_sql, &["1"]);
    tk.must_exec("begin");
    tk.exec_to_err(&stale_sql);
    tk.must_exec("commit");
    tk.must_exec(&format!("prepare s from \"{stale_sql}\""));
    tk.must_exec("execute s");
    tk.must_exec("begin");
    tk.exec_to_err(&stale_sql);
    tk.must_exec("commit");
    tk.must_exec(&format!("start transaction read only as of timestamp '{time2}'"));
    tk.exec_to_err(&stale_sql);
    tk.must_exec("commit");
    tk.must_exec("alter table t add column c int");
    std::thread::sleep(tolerance);
    tk.must_exec("insert into t values (4, 5)");
    std::thread::sleep(std::time::Duration::from_millis(10));
    tk.check("execute s", &["1"]);
    tk.must_exec("alter table t add column d int");
    tk.must_exec("insert into t values (4, 4, 4)");
    std::thread::sleep(tolerance);
    let time6 = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string();
    std::thread::sleep(tolerance);
    tk.must_exec("insert into t values (5, 5, 5)");
    std::thread::sleep(tolerance);
    tk.check(&format!("select * from t as of timestamp '{time6}' where c=5"), &["4 5 <nil>"]);
    tk.must_exec("drop table if exists t");

}


/// Go `txntest.TestSharedLockChildTableConflict` (shared_lock_test.go).
#[test]
fn test_shared_lock_child_table_conflict() {
    let store = realtikv!();
    let mut tk1 = store.new_tk();
    let mut tk2 = store.new_tk();
    let mut tk3 = store.new_tk();
    for tk in [&mut tk1, &mut tk2, &mut tk3] {
        tk.must_exec("use test");
        tk.must_exec("set @@tidb_foreign_key_check_in_shared_lock = ON");
        tk.must_exec("set @@tidb_pessimistic_txn_fair_locking = 0");
    }
    prepare_foreign_key_tables(&mut tk1);
    tk2.must_exec("begin pessimistic");
    tk3.must_exec("begin pessimistic");
    tk2.must_exec("insert into child values(1, 1)");
    let worker = std::thread::spawn(move || {
        tk3.exec_to_err("insert into child values(1, 2)");
        tk3
    });
    std::thread::sleep(std::time::Duration::from_millis(500));
    let blocked = !worker.is_finished();
    tk2.must_exec("commit");
    let mut tk3 = worker.join().unwrap();
    assert!(blocked, "tk3 should be blocked");
    tk3.must_exec("commit");
    tk2.check("select * from child", &["1 1"]);
    tk2.must_exec("admin check table parent");
    tk2.must_exec("admin check table child");
    tk1.must_exec("delete from child");
    tk1.must_exec("begin pessimistic");
    tk2.must_exec("begin pessimistic");
    tk3.must_exec("begin pessimistic");
    tk1.must_exec("select * from parent where id in (1, 2) for update");
    let (tx, rx) = std::sync::mpsc::channel();
    let workers: Vec<_> = [(tk2, 1), (tk3, 2)].into_iter().map(|(mut tk, parent)| {
        let tx = tx.clone();
        std::thread::spawn(move || {
            let result = tk.exec(&format!("insert into child values(1, {parent})"));
            tx.send((tk, parent, result)).unwrap();
        })
    }).collect();
    std::thread::sleep(std::time::Duration::from_millis(500));
    let blocked = workers.iter().all(|worker| !worker.is_finished());
    tk1.must_exec("commit");
    let (mut winner, parent, result) = rx.recv().unwrap();
    result.unwrap();
    winner.must_exec("commit");
    let (mut loser, _, result) = rx.recv().unwrap();
    assert!(result.is_err());
    loser.must_exec("commit");
    for worker in workers { worker.join().unwrap(); }
    assert!(blocked, "both child inserts should be blocked");
    tk1.check("select * from child", &[&format!("1 {parent}")]);
    tk1.must_exec("admin check table parent");
    tk1.must_exec("admin check table child");
}

/// Go `txntest.TestMemDBTracker` (txn_state_test.go).
#[test]
fn test_mem_db_tracker() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("create table t (id int)");
    tk.must_exec("begin");
    for _ in 0..1 << 10 { tk.must_exec("insert t (id) values (1)"); }
    let footprint = tk.session().mem_db_footprint().unwrap();
    assert!(footprint.bytes_consumed() > 1 << 14);
    assert!(footprint.bytes_consumed() < 1 << 18);
    for _ in 0..1 << 14 { tk.must_exec("insert t (id) values (1)"); }
    assert!(footprint.bytes_consumed() > 1 << 18);
}
/// Go `txntest.TestStalenessTransactionSchemaVer` (stale_read_test.go).
#[test]
fn test_staleness_transaction_schema_ver() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (id int primary key);");
    std::thread::sleep(std::time::Duration::from_millis(200));
    let schema_ver1 = tk.session().info_schema_version().unwrap();
    let time1 = (chrono::Local::now() - chrono::Duration::milliseconds(100)).format("%Y-%m-%d %H:%M:%S%.3f").to_string();
    tk.must_exec("alter table t add c int");
    let schema_ver2 = tk.session().info_schema_version().unwrap();
    assert!(schema_ver1 < schema_ver2);
    tk.must_exec(&format!("START TRANSACTION READ ONLY AS OF TIMESTAMP '{time1}'"));
    assert_eq!(tk.session().txn_info_schema_version().unwrap(), schema_ver1);
    tk.must_exec("commit");
    assert_eq!(tk.session().txn_info_schema_version().unwrap(), schema_ver2);
    tk.must_exec(&format!("SELECT * from t AS OF TIMESTAMP '{time1}'"));
    assert_eq!(tk.session().txn_info_schema_version().unwrap(), schema_ver2);
    tk.must_exec("drop table if exists t");

}


/// Go `TestStalenessAndHistoryRead.getCurrentTS` / `TestStaleReadCompatibility.getCurrentTime`.
fn current_ts(tk: &mut super::realtikvtest::Tk) -> u64 {
    tk.must_exec("begin");
    let ts = tk.must_query("select @@tidb_current_ts")[0][0].parse().unwrap();
    tk.must_exec("rollback");
    ts
}

/// Go `oracle.GetTimeFromTS` followed by `Time.Format` in the local timezone.
fn ts_time(ts: u64) -> chrono::DateTime<chrono::Local> {
    chrono::DateTime::from_timestamp_millis((ts >> 18) as i64).unwrap().with_timezone(&chrono::Local)
}

/// Go `txntest.TestSetTransactionInfoSchema` / `testSetTransactionInfoSchema`.
#[test]
fn test_set_transaction_info_schema() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    set_safe_point(&mut tk);
    for cache_size in [1 << 30, 0] {
        tk.must_exec(&format!("set @@global.tidb_schema_cache_size = {cache_size}"));
        tk.must_exec("use test");
        tk.must_exec("drop table if exists t");
        tk.must_exec("create table t (id int primary key)");
        let interval = std::time::Duration::from_millis(60);
        let schema1 = tk.session().txn_info_schema_version().unwrap();
        std::thread::sleep(interval);
        let time1 = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        std::thread::sleep(interval);
        tk.must_exec("alter table t add c int");
        let schema2 = tk.session().txn_info_schema_version().unwrap();
        let time2 = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        std::thread::sleep(interval);
        assert!(schema1 < schema2);
        tk.must_exec(&format!("SET TRANSACTION READ ONLY AS OF TIMESTAMP '{time1}'"));
        assert_eq!(tk.session().info_schema_version().unwrap(), schema1);
        tk.must_exec("select * from t");
        tk.must_exec("alter table t add d int");
        let schema3 = tk.session().txn_info_schema_version().unwrap();
        for (time, schema) in [(time1, schema1), (time2, schema2)] {
            tk.must_exec(&format!("SET TRANSACTION READ ONLY AS OF TIMESTAMP '{time}'"));
            tk.must_exec("begin");
            assert_eq!(tk.session().txn_info_schema_version().unwrap(), schema);
            tk.must_exec("commit");
        }
        assert_eq!(tk.session().txn_info_schema_version().unwrap(), schema3);
        tk.must_exec("drop table if exists t");
    }
}

/// Go `txntest.TestStaleTSO`.
#[test]
fn test_stale_tso() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (id int)");
    tk.must_exec("insert into t values(1)");
    let ts1: u64 = tk.must_query("select json_extract(@@tidb_last_txn_info, '$.commit_ts')")[0][0].parse().unwrap();
    let ts = loop {
        let ts = current_ts(&mut tk);
        if (ts >> 18) > (ts1 >> 18) + 1000 { break ts; }
        std::thread::sleep(std::time::Duration::from_millis(100));
    };
    let next_tso = ts + (10_000 << 18);
    let _stale = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/sessiontxn/staleread/mockStaleReadTSO", &format!("return({next_tso})"));
    for expr in ["now(3) - interval 10 second", "current_time() - interval 10 second", "curtime() - interval 10 second"] {
        tk.check(&format!("select * from t as of timestamp {expr} order by id asc"), &["1"]);
    }
    tk.must_exec("drop table if exists t");
}

/// Go `txntest.TestTimeBoundedStalenessTxn`.
#[test]
fn test_time_bounded_staleness_txn() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (id int primary key)");
    let now = chrono::Local::now().timestamp_millis() as u64;
    for (expr, seconds, order) in [
        ("tidb_bounded_staleness(NOW() - INTERVAL 20 SECOND, NOW())", 10, std::cmp::Ordering::Equal),
        ("tidb_bounded_staleness(NOW() - INTERVAL 10 SECOND, NOW())", 20, std::cmp::Ordering::Greater),
        ("tidb_bounded_staleness(NOW() - INTERVAL 20 SECOND, NOW() - INTERVAL 10 SECOND)", 5, std::cmp::Ordering::Less),
        ("NOW() - INTERVAL 5 SECOND", 10, std::cmp::Ordering::Greater),
        ("NOW() - INTERVAL 10 SECOND", 5, std::cmp::Ordering::Less),
    ] {
        let safe_ts = (now - seconds * 1000) << 18;
        let _safe = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/expression/injectSafeTS", &format!("return({safe_ts})"));
        tk.must_exec(&format!("START TRANSACTION READ ONLY AS OF TIMESTAMP {expr}"));
        assert_eq!((tk.session().current_tso().value() as u64).cmp(&safe_ts), order);
        tk.must_exec("commit");
    }
    tk.must_exec("drop table if exists t");
}
/// Go `txntest.TestStalenessAndHistoryRead` (stale_read_test.go).
#[test]
fn test_staleness_and_history_read() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    set_safe_point(&mut tk);
    let time1 = ts_time(current_ts(&mut tk));
    let time1_ts = (time1.timestamp_millis() as u64) << 18;
    let schema_ver1 = tk.session().info_schema_version().unwrap();
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (id int primary key);");
    tk.must_exec("drop table if exists t");
    let last_commit_ts: u64 = tk.must_query("select json_extract(@@tidb_last_txn_info, '$.commit_ts')")[0][0].parse().unwrap();
    let time2 = loop {
 let now = ts_time(current_ts(&mut tk));
 if now > ts_time(last_commit_ts) { break now; }
 std::thread::sleep(std::time::Duration::from_millis(10));
};
    let time2_ts = (time2.timestamp_millis() as u64) << 18;
    let schema_ver2 = tk.session().info_schema_version().unwrap();
    tk.must_exec(&format!("set @@tidb_snapshot=\"{}\"", time1.format("%Y-%m-%d %H:%M:%S%.3f")));
    assert_eq!(tk.session().vars().snapshot_ts(), time1_ts);
    assert!(tk.session().has_snapshot_info_schema());
    assert_eq!(tk.session().info_schema_version().unwrap(), schema_ver1);
    tk.must_exec(&format!("SET TRANSACTION READ ONLY AS OF TIMESTAMP '{}'", time2.format("%Y-%m-%d %H:%M:%S%.3f")));
    assert_eq!(tk.session().vars().snapshot_ts(), 0);
    assert!(tk.session().has_snapshot_info_schema());
    assert_eq!(tk.session().vars().txn_read_ts(), time2_ts);
    assert_eq!(tk.session().info_schema_version().unwrap(), schema_ver2);
    tk.must_exec(&format!("SET TRANSACTION READ ONLY AS OF TIMESTAMP '{}'", time1.format("%Y-%m-%d %H:%M:%S%.3f")));
    assert_eq!(tk.session().vars().txn_read_ts(), time1_ts);
    assert!(tk.session().has_snapshot_info_schema());
    assert_eq!(tk.session().info_schema_version().unwrap(), schema_ver1);
    tk.must_exec(&format!("set @@tidb_snapshot=\"{}\"", time2.format("%Y-%m-%d %H:%M:%S%.3f")));
    assert_eq!(tk.session().vars().txn_read_ts(), 0);
    assert_eq!(tk.session().vars().snapshot_ts(), time2_ts);
    assert!(tk.session().has_snapshot_info_schema());
    assert_eq!(tk.session().info_schema_version().unwrap(), schema_ver2);
    tk.must_exec(&format!("set @@tidb_snapshot=\"{}\"", time1.format("%Y-%m-%d %H:%M:%S%.3f")));
    assert_eq!(tk.session().vars().snapshot_ts(), time1_ts);
    assert!(tk.session().has_snapshot_info_schema());
    assert_eq!(tk.session().info_schema_version().unwrap(), schema_ver1);
    tk.must_exec(&format!("START TRANSACTION READ ONLY AS OF TIMESTAMP '{}'", time2.format("%Y-%m-%d %H:%M:%S%.3f")));
    assert_eq!(tk.session().vars().snapshot_ts(), 0);
    assert_eq!(tk.session().current_tso().value() as u64, time2_ts);
    assert!(!tk.session().has_snapshot_info_schema());
    assert_eq!(tk.session().info_schema_version().unwrap(), schema_ver2);
    tk.must_exec("commit");
    assert_eq!(tk.session().vars().snapshot_ts(), 0);
    assert!(!tk.session().has_snapshot_info_schema());
    assert_eq!(tk.session().info_schema_version().unwrap(), schema_ver2);
    tk.must_exec(&format!("START TRANSACTION READ ONLY AS OF TIMESTAMP '{}'", time2.format("%Y-%m-%d %H:%M:%S%.3f")));
    assert_eq!(tk.session().vars().snapshot_ts(), 0);
    assert_eq!(tk.session().current_tso().value() as u64, time2_ts);
    assert!(!tk.session().has_snapshot_info_schema());
    assert_eq!(tk.session().info_schema_version().unwrap(), schema_ver2);
    let err = tk.exec_to_err("set @@tidb_snapshot=\"2020-10-08 16:45:26\";");
    assert!(err.contains("Transaction characteristics can't be changed while a transaction is in progress"));
    assert_eq!(tk.session().vars().snapshot_ts(), 0);
    assert!(!tk.session().has_snapshot_info_schema());
    tk.must_exec("commit");
    tk.must_exec("START TRANSACTION");
    assert_eq!(tk.session().vars().txn_read_ts(), 0);
    let err = tk.exec_to_err("SET TRANSACTION READ ONLY AS OF TIMESTAMP '2020-10-08 16:46:26'");
    assert!(err.contains("Transaction characteristics can't be changed while a transaction is in progress"));
    assert_eq!(tk.session().vars().txn_read_ts(), 0);
    tk.must_exec("commit");

}


/// Go `txntest.TestSetTransactionReadOnlyAsOf`.
#[test]
fn test_set_transaction_read_only_as_of() {
    use chrono::TimeZone as _;
    let store = realtikv!();
    let mut tk = store.new_tk();
    set_safe_point(&mut tk);
    let exact = (chrono::Local.with_ymd_and_hms(2021, 4, 21, 0, 42, 12).unwrap().timestamp_millis() as u64) << 18;
    let bounded = (chrono::Utc.with_ymd_and_hms(2016, 9, 21, 9, 53, 4).unwrap().timestamp_millis() as u64) << 18;
    for (sql, expected, inject) in [
        ("SET TRANSACTION READ ONLY as of timestamp '2021-04-21 00:42:12'", exact, false),
        ("SET TRANSACTION READ ONLY as of timestamp tidb_bounded_staleness('2015-09-21 00:07:01', '2021-04-27 11:26:13')", bounded, true),
    ] {
        let _safe = inject.then(|| tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/expression/injectSafeTS", &format!("return({expected})")));
        tk.must_exec(sql);
        assert_eq!(tk.session().vars().txn_read_ts(), expected);
        tk.must_exec("begin");
        assert_eq!(tk.session().current_tso().value() as u64, expected);
        tk.must_exec("commit");
        assert_eq!(tk.session().vars().txn_read_ts(), 0);
        tk.must_exec("begin");
        assert_ne!(tk.session().current_tso().value() as u64, expected);
        tk.must_exec("commit");
    }
    tk.exec_to_err("SET TRANSACTION READ ONLY as of timestamp tidb_bounded_staleness(invalid1, invalid2')");
    assert_eq!(tk.session().vars().txn_read_ts(), 0);
    for _ in 0..2 {
        tk.must_exec("SET TRANSACTION READ ONLY as of timestamp '2021-04-21 00:42:12'");
        let error = tk.exec_to_err("START TRANSACTION READ ONLY AS OF TIMESTAMP '2020-09-06 00:00:00'");
        assert_eq!(error, "start transaction read only as of is forbidden after set transaction read only as of");
    }
    tk.must_exec("begin");
    assert_eq!(tk.session().vars().txn_read_ts(), exact);
    tk.must_exec("commit");
    tk.must_exec("START TRANSACTION READ ONLY AS OF TIMESTAMP '2020-09-06 00:00:00'");
}

/// Go `txntest.TestStaleReadNoExtraTSORequest`.
#[test]
fn test_stale_read_no_extra_tso_request() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    set_safe_point(&mut tk);
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (id int)");
    std::thread::sleep(std::time::Duration::from_secs(3));
    let no_tso = || tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/session/assertTSONotRequest", "return(true)");
    {
        let _guard = no_tso();
        tk.must_exec("select * from t as of timestamp NOW() - INTERVAL 2 SECOND");
    }
    tk.must_exec("set transaction read only as of timestamp NOW() - INTERVAL 2 SECOND");
    {
        let _guard = no_tso();
        tk.must_exec("select * from t");
    }
    {
        let _guard = no_tso();
        tk.must_exec("start transaction read only as of timestamp NOW() - INTERVAL 2 SECOND");
        tk.must_exec("select * from t");
        tk.must_exec("commit");
    }
    tk.must_exec("set transaction read only as of timestamp NOW() - INTERVAL 2 SECOND");
    {
        let _guard = no_tso();
        tk.must_exec("begin");
        tk.must_exec("select * from t");
        tk.must_exec("commit");
    }
    tk.must_exec("set @@tidb_read_staleness='-1'");
    let _guard = no_tso();
    tk.must_exec("select * from t");
}

/// Go `txntest.TestStaleSessionQuery`.
#[test]
fn test_stale_session_query() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    set_safe_point(&mut tk);
    tk.must_exec("use test");
    tk.must_exec("create table t10 (id int)");
    tk.must_exec("insert into t10 (id) values (1)");
    std::thread::sleep(std::time::Duration::from_secs(2));
    let now = chrono::Local::now().timestamp();
    tk.must_exec("set @@tidb_read_staleness='-1'");
    for first in [true, false] {
        if !first {
            tk.must_exec("begin");
            tk.must_exec("insert into t10(id) values (2)");
            tk.must_exec("commit");
            tk.must_exec("insert into t10(id) values (3)");
        }
        let _now = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/expression/injectNow", &format!("return({now})"));
        let _assert = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/assertStaleTSO", &format!("return({})", now - 1));
        assert_eq!(tk.must_query("select * from t10").len(), 1);
    }
    tk.must_exec("set @@tidb_read_staleness=''");
    assert_eq!(tk.must_query("select * from t10").len(), 3);
}

/// Go `parser.NormalizeDigest` used by the transaction-info tests.
fn sql_digest(sql: &str) -> String {
    tidb_sessmgr::normalize_statement_digest(sql).1.to_string()
}

/// Go `txntest.TestBasicTxnState`.
#[test]
fn test_basic_txn_state() {
    let store = realtikv!();
    let processes = store.processes();
    let mut tk = store.new_tk();
    let id = tk.session().vars().connection_id.unwrap();
    tk.must_exec("use test");
    tk.must_exec("create table t(a int)");
    tk.must_exec("insert into t(a) values (1)");
    assert!(txn_info(&processes, id).is_none());
    tk.must_exec("begin pessimistic");
    let start_ts: u64 = tk.must_query("select @@tidb_current_ts")[0][0].parse().unwrap();
    let pause = tidb_failpoint::enable_for_test("tikvclient/beforePessimisticLock", "pause");
    let worker = std::thread::spawn(move || {
        tk.must_exec("select * from t for update");
        tk
    });
    std::thread::sleep(std::time::Duration::from_millis(100));
    let locking = txn_info(&processes, id);
    drop(pause);
    let mut tk = worker.join().unwrap();
    let locking = locking.unwrap();
    let select_digest = sql_digest("select * from t for update");
    assert_eq!(locking.current_sql_digest.as_deref(), Some(select_digest.as_str()));
    assert_eq!(locking.state, "LockWaiting");
    assert!(locking.waiting_start.is_some());
    assert_eq!(locking.start_ts, start_ts);
    let idle = txn_info(&processes, id).unwrap();
    assert!(idle.current_sql_digest.is_none());
    assert_eq!(idle.state, "Idle");
    assert!(idle.waiting_start.is_none());
    assert_eq!(idle.start_ts, start_ts);
    let mut digests = vec![sql_digest("begin pessimistic"), sql_digest("select @@tidb_current_ts"), select_digest];
    assert_eq!(idle.all_sql_digests, digests);
    assert_eq!(idle.session_id, id);
    // The production-domain harness authenticates its testkit as root.
    assert_eq!(idle.user, "root");
    assert_eq!(idle.db, "test");
    let pause = tidb_failpoint::enable_for_test("tikvclient/beforePrewrite", "pause");
    let worker = std::thread::spawn(move || { tk.must_exec("commit"); tk });
    std::thread::sleep(std::time::Duration::from_millis(100));
    let committing = txn_info(&processes, id);
    drop(pause);
    let mut tk = worker.join().unwrap();
    let committing = committing.unwrap();
    let commit_digest = sql_digest("commit");
    assert_eq!(committing.current_sql_digest.as_deref(), Some(commit_digest.as_str()));
    assert_eq!(committing.state, "Committing");
    digests.push(commit_digest);
    assert_eq!(committing.all_sql_digests, digests);
    assert!(txn_info(&processes, id).is_none());
    let pause = tidb_failpoint::enable_for_test("tikvclient/beforePrewrite", "pause");
    let worker = std::thread::spawn(move || tk.must_exec("insert into t values (2)"));
    std::thread::sleep(std::time::Duration::from_millis(100));
    let committing = txn_info(&processes, id);
    drop(pause);
    worker.join().unwrap();
    let committing = committing.unwrap();
    let insert_digest = sql_digest("insert into t values (2)");
    assert_eq!(committing.state, "Committing");
    assert!(committing.waiting_start.is_none());
    assert!(committing.start_ts > start_ts);
    assert_eq!(committing.current_sql_digest.as_deref(), Some(insert_digest.as_str()));
    assert_eq!(committing.all_sql_digests, [insert_digest]);
    assert!(txn_info(&processes, id).is_none());
}

/// Go `txntest.TestTxnInfoWithPreparedStmt`.
#[test]
fn test_txn_info_with_prepared_stmt() {
    let store = realtikv!();
    let processes = store.processes();
    let mut tk = store.new_tk();
    let id = tk.session().vars().connection_id.unwrap();
    tk.must_exec("use test");
    tk.must_exec("create table t(a int)");
    tk.must_exec("prepare s1 from 'insert into t values (?)'");
    tk.must_exec("set @v = 1");
    tk.must_exec("begin pessimistic");
    let pause = tidb_failpoint::enable_for_test("tikvclient/beforePessimisticLock", "pause");
    let worker = std::thread::spawn(move || { tk.must_exec("execute s1 using @v"); tk });
    std::thread::sleep(std::time::Duration::from_millis(100));
    let info = txn_info(&processes, id);
    drop(pause);
    let mut tk = worker.join().unwrap();
    let digest = sql_digest("insert into t values (?)");
    assert_eq!(info.unwrap().current_sql_digest.as_deref(), Some(digest.as_str()));
    let info = txn_info(&processes, id).unwrap();
    assert!(info.current_sql_digest.is_none());
    assert_eq!(info.all_sql_digests, [sql_digest("begin pessimistic"), digest]);
    tk.must_exec("rollback");
}

/// Go `txntest.TestTxnInfoWithScalarSubquery`.
#[test]
fn test_txn_info_with_scalar_subquery() {
    let store = realtikv!();
    let processes = store.processes();
    let mut tk = store.new_tk();
    let id = tk.session().vars().connection_id.unwrap();
    tk.must_exec("use test");
    tk.must_exec("create table t (a int, b int)");
    tk.must_exec("insert into t values (1, 10), (2, 1)");
    tk.must_exec("begin pessimistic");
    let select = "select * from t where a = (select b from t where a = 2)";
    tk.must_exec(select);
    let pause = tidb_failpoint::enable_for_test("tikvclient/beforePessimisticLock", "pause");
    let worker = std::thread::spawn(move || {
        tk.must_exec("update t set b = b + 1 where a = (select b from t where a = 2)");
        tk
    });
    std::thread::sleep(std::time::Duration::from_millis(100));
    let info = txn_info(&processes, id);
    drop(pause);
    let mut tk = worker.join().unwrap();
    let update_digest = sql_digest("update t set b = b + 1 where a = (select b from t where a = 1)");
    let info = info.unwrap();
    assert_eq!(info.current_sql_digest.as_deref(), Some(update_digest.as_str()));
    assert_eq!(info.all_sql_digests, [sql_digest("begin pessimistic"), sql_digest(select), update_digest]);
    tk.must_exec("rollback");
}

/// Go `txntest.TestTxnInfoWithPSProtocol`.
#[test]
fn test_txn_info_with_ps_protocol() {
    let store = realtikv!();
    let processes = store.processes();
    let mut tk = store.new_tk();
    let id = tk.session().vars().connection_id.unwrap();
    tk.must_exec("use test");
    tk.must_exec("create table t (a int primary key)");
    let insert = tk.prepare("insert into t values (?)");
    let pause = tidb_failpoint::enable_for_test("tikvclient/beforePrewrite", "pause");
    let worker = std::thread::spawn(move || {
        tk.execute_prepared(&insert, &[tidb_param::PreparedValue::SignedLongLong(1)]).unwrap();
        tk
    });
    std::thread::sleep(std::time::Duration::from_millis(100));
    let info = txn_info(&processes, id);
    drop(pause);
    let mut tk = worker.join().unwrap();
    let info = info.unwrap();
    let digest = sql_digest("insert into t values (1)");
    assert!(info.start_ts > 0);
    assert_eq!(info.state, "Committing");
    assert_eq!(info.current_sql_digest.as_deref(), Some(digest.as_str()));
    assert_eq!(info.all_sql_digests, [digest]);
    assert!(txn_info(&processes, id).is_none());
    let select_sql = "select * from t where a = ?";
    let update_sql = "update t set a = a + 1 where a = ?";
    let select = tk.prepare(select_sql);
    let update = tk.prepare(update_sql);
    tk.must_exec("begin pessimistic");
    tk.execute_prepared(&select, &[tidb_param::PreparedValue::SignedLongLong(1)]).unwrap();
    let pause = tidb_failpoint::enable_for_test("tikvclient/beforePessimisticLock", "pause");
    let worker = std::thread::spawn(move || {
        tk.execute_prepared(&update, &[tidb_param::PreparedValue::SignedLongLong(1)]).unwrap();
        tk
    });
    std::thread::sleep(std::time::Duration::from_millis(100));
    let info = txn_info(&processes, id);
    drop(pause);
    let mut tk = worker.join().unwrap();
    let info = info.unwrap();
    let digest = sql_digest(update_sql);
    assert!(info.start_ts > 0);
    assert_eq!(info.current_sql_digest.as_deref(), Some(digest.as_str()));
    assert_eq!(info.state, "LockWaiting");
    assert!(info.waiting_start.is_some());
    assert_eq!(info.all_sql_digests, [sql_digest("begin pessimistic"), sql_digest(select_sql), digest]);
    tk.must_exec("rollback");
}

/// Go `txntest.TestStaleReadCompatibility`.
#[test]
fn test_stale_read_compatibility() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (id int)");
    tk.must_exec("insert into t(id) values (1)");
    std::thread::sleep(std::time::Duration::from_secs(2));
    let t1 = ts_time(current_ts(&mut tk));
    tk.must_exec("insert into t(id) values (2)");
    std::thread::sleep(std::time::Duration::from_secs(2));
    let t2 = ts_time(current_ts(&mut tk));
    tk.must_exec("insert into t(id) values (3)");
    let t1_str = t1.format("%Y-%m-%d %H:%M:%S").to_string();
    let t2_str = t2.format("%Y-%m-%d %H:%M:%S").to_string();
    tk.must_exec(&format!("set transaction read only as of timestamp '{t1_str}'"));
    tk.must_contain_err_msg(&format!("select * from t as of timestamp '{t1_str}'"), "invalid as of timestamp: can't use select as of while already set transaction as of");
    assert_eq!(tk.must_query("select * from t").len(), 3);
    tk.must_exec("set @@tidb_read_staleness='-1'");
    let _now = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/expression/injectNow", &format!("return({})", t1.timestamp()));
    tk.check("select * from t order by id", &["1"]);
    assert_eq!(tk.must_query(&format!("select * from t as of timestamp '{t2_str}'")).len(), 2);
    tk.must_exec(&format!("set transaction read only as of timestamp '{t2_str}'"));
    assert_eq!(tk.must_query("select * from t").len(), 2);
    tk.must_exec(&format!("start transaction read only as of timestamp '{t2_str}'"));
    assert_eq!(tk.must_query("select * from t").len(), 2);
    tk.must_exec("commit");
    tk.check("select * from t order by id", &["1"]);
    tk.must_exec("set @@tidb_read_staleness=''");
    assert_eq!(tk.must_query("select * from t").len(), 3);
    tk.must_exec("drop table if exists t");
}
/// Go `txntest.TestPessimisticLockLockView` (txn_test.go).
#[test]
fn test_pessimistic_lock_lock_view() {
    let store = realtikv!();
    pessimistic_lock_view(&store, false);
}

/// Go `txntest.TestPessimisticLockDataLockWaitsFromStorageWaitTable` (txn_test.go).
#[test]
fn test_pessimistic_lock_data_lock_waits_from_storage_wait_table() {
    let store = realtikv!();
    pessimistic_lock_view(&store, true);
}

/// Go `txntest.TestPessimisticLockLockView` and its storage-wait-table variant.
fn pessimistic_lock_view(store: &super::realtikvtest::RealTiKV, storage_wait_table: bool) {
    let _fp = storage_wait_table.then(|| tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/executor/dataLockWaitsSkipResolvingLocks", "return(true)"));
    let mut tk1 = store.new_tk();
    let mut tk2 = store.new_tk();
    let mut test_tk = store.new_tk();
    for tk in [&mut tk1, &mut tk2, &mut test_tk] { tk.must_exec("use test"); }
    tk1.must_exec("create table ordinary_lock_view (id int primary key, v int)");
    tk1.must_exec("insert into ordinary_lock_view values (1, 10)");
    let conn2 = tk2.must_query("select connection_id()")[0][0].clone();
    tk1.must_exec("begin pessimistic");
    tk1.must_exec("select * from ordinary_lock_view where id=1 for update");
    tk2.must_exec("begin pessimistic");
    let txn = tk2.session().current_tso().value();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let result = tk2.exec("select * from ordinary_lock_view where id=1 for update")
            .and_then(|_| tk2.exec("commit"));
        tx.send(result).unwrap();
    });
    let observed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            assert!(matches!(rx.try_recv(), Err(std::sync::mpsc::TryRecvError::Empty)),
                "SELECT FOR UPDATE finished before the lock wait was observed");
            let rows = test_tk.must_query(&format!("select TRX_ID, SESSION_ID from INFORMATION_SCHEMA.DATA_LOCK_WAITS as l left join INFORMATION_SCHEMA.TIDB_TRX as trx on l.trx_id = trx.id where l.trx_id = {txn} and trx.session_id = {conn2}"));
            if !rows.is_empty() {
                assert_eq!(rows, vec![vec![txn.to_string(), conn2.clone()]]);
                break;
            }
            assert!(std::time::Instant::now() < deadline, "lock wait was not published");
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }));
    // Go's cleanup releases the blocker even when an assertion fails.
    tk1.must_exec(if observed.is_ok() { "commit" } else { "rollback" });
    if observed.is_ok() { rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap().unwrap(); }
    worker.join().unwrap();
    if let Err(panic) = observed { std::panic::resume_unwind(panic); }
}

/// Go `txntest.TestSharedLockLockView` (shared_lock_test.go).
#[test]
fn test_shared_lock_lock_view() {
    let store = realtikv!();
    shared_lock_view(&store, false);
}

/// Go `txntest.TestSharedLockDataLockWaitsFromStorageWaitTable` (shared_lock_test.go).
#[test]
fn test_shared_lock_data_lock_waits_from_storage_wait_table() {
    let store = realtikv!();
    shared_lock_view(&store, true);
}

/// Go `txntest.TestSharedLockLockView` and its storage-wait-table variant.
fn shared_lock_view(store: &super::realtikvtest::RealTiKV, storage_wait_table: bool) {
    let _fp = storage_wait_table.then(|| tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/executor/dataLockWaitsSkipResolvingLocks", "return(true)"));
    let mut tk1 = store.new_tk();
    let mut tk2 = store.new_tk();
    let mut test_tk = store.new_tk();
    for tk in [&mut tk1, &mut tk2, &mut test_tk] { tk.must_exec("use test"); }
    for tk in [&mut tk1, &mut tk2] {
        tk.must_exec("set @@tidb_foreign_key_check_in_shared_lock = ON");
        tk.must_exec("set @@tidb_pessimistic_txn_fair_locking = 0");
    }
    prepare_foreign_key_tables(&mut tk1);
    let conn2 = tk2.must_query("select connection_id()")[0][0].clone();
    for shared_waiter in [true, false] {
        if storage_wait_table && !shared_waiter { break; }
        tk1.must_exec("begin pessimistic");
        tk1.must_exec(if shared_waiter { "select * from parent where id=1 for update" }
            else { "insert into child values (2, 1)" });
        tk2.must_exec("begin pessimistic");
        let txn = tk2.session().current_tso().value();
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let result = tk2.exec(if shared_waiter { "insert into child values (1, 1)" }
                else { "select * from parent where id=1 for update" })
                .and_then(|_| tk2.exec("commit"));
            tx.send(result).unwrap();
            tk2
        });
        let observed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if storage_wait_table {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
                loop {
                    assert!(matches!(rx.try_recv(), Err(std::sync::mpsc::TryRecvError::Empty)), "insert finished early");
                    let rows = test_tk.must_query(&format!("select TRX_ID, SESSION_ID from INFORMATION_SCHEMA.DATA_LOCK_WAITS as l left join INFORMATION_SCHEMA.TIDB_TRX as trx on l.trx_id = trx.id where l.trx_id = {txn} and trx.session_id = {conn2}"));
                    if !rows.is_empty() {
                        assert_eq!(rows, vec![vec![txn.to_string(), conn2.clone()]]);
                        break;
                    }
                    assert!(std::time::Instant::now() < deadline, "lock wait was not published");
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            } else {
                assert!(matches!(rx.recv_timeout(std::time::Duration::from_millis(500)), Err(std::sync::mpsc::RecvTimeoutError::Timeout)), "lock acquisition finished early");
                let waits = test_tk.must_query("select `key`, count(*) as `count` from INFORMATION_SCHEMA.DATA_LOCK_WAITS group by `key` order by `count` desc;");
                if shared_waiter { assert_eq!(waits.len(), 1); assert_eq!(waits[0][1], "1"); }
                else { assert!(!waits.is_empty()); }
                let key = &waits[0][0];
                let rows = test_tk.must_query(&format!("select TRX_ID, SESSION_ID from INFORMATION_SCHEMA.DATA_LOCK_WAITS as l left join INFORMATION_SCHEMA.TIDB_TRX as trx on l.trx_id = trx.id where l.key = \"{key}\""));
                if shared_waiter { assert_eq!(rows.len(), 1); } else { assert!(!rows.is_empty()); }
                assert_eq!(rows[0], vec![txn.to_string(), conn2.clone()]);
            }
        }));
        tk1.must_exec(if observed.is_ok() { "commit" } else { "rollback" });
        if observed.is_ok() { rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap().unwrap(); }
        tk2 = worker.join().unwrap();
        if let Err(panic) = observed { std::panic::resume_unwind(panic); }
        if shared_waiter { tk1.check("select * from child", &["1 1"]); }
    }
}
/// Go `txntest.TestReplicaReadEffectScope` (replica_read_test.go).
#[test]
fn test_replica_read_effect_scope() {
    use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
    use tikv_client::proto::{kvrpcpb, coprocessor};
    let _config = super::realtikv_pessimistictest::ConfigRestore::new();
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    let mut config = (*tidb_config::config_tree::config::get_global_config()).clone();
    config.performance.enable_async_batch_get = false;
    tidb_config::config_tree::config::store_global_config(Arc::new(config));
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (id int primary key, v int)");
    tk.must_exec("set session tidb_replica_read = 'follower'");
    tk.must_exec("set session tidb_enable_noop_functions='on'");
    for (sql, replica_read) in [
        ("SELECT * FROM t WHERE %s", true),
        ("SELECT * FROM t WHERE %s FOR UPDATE", false),
        ("SELECT * FROM t WHERE %s FOR SHARE", false),
        ("UPDATE t SET v = v + 1 WHERE %s", false),
        ("DELETE FROM t WHERE %s", false),
        ("INSERT INTO t SELECT id + 100, v + 100 FROM t WHERE %s", false),
        ("INSERT INTO t SELECT id + 100, v + 100 FROM t WHERE %s ON DUPLICATE KEY UPDATE v = VALUES(v)", false),
        ("REPLACE INTO t SELECT id, v + 100 FROM t WHERE %s", false),
    ] {
        for cond in ["id = 1", "id IN (1, 2)", "id > 1"] {
            let seen = Arc::new(AtomicUsize::new(0));
            let observed = seen.clone();
            let interceptor = tikv_client::new_rpc_interceptor("replica-read-effect-scope", move |_, req, next| {
                let context = if let Some(req) = req.as_any().downcast_ref::<kvrpcpb::GetRequest>() { req.context.as_ref() }
                    else if let Some(req) = req.as_any().downcast_ref::<kvrpcpb::BatchGetRequest>() { req.context.as_ref() }
                    else if let Some(req) = req.as_any().downcast_ref::<coprocessor::Request>() { req.context.as_ref() }
                    else { None };
                if let Some(context) = context {
                    assert_eq!(context.replica_read, replica_read);
                    observed.fetch_add(1, Ordering::SeqCst);
                }
                Box::pin(async move { next().await })
            });
            tk.must_exec("delete from t");
            tk.must_exec("insert into t values (1, 10), (2, 20), (3, 30)");
            let sql = sql.replace("%s", cond);
            tk.must_exec("begin pessimistic");
            tk.exec_with_interceptor(interceptor.clone(), &sql).unwrap();
            tk.must_exec("rollback");
            tk.exec_with_interceptor(interceptor, &sql).unwrap();
            assert!(seen.load(Ordering::SeqCst) > 0, "interceptor did not see {sql}");
        }
    }
}

/// Go `txntest.TestSetTransactionIsolationOneSho` (txn_test.go).
#[test]
fn test_set_transaction_isolation_one_sho() {
    use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (k int, v int)");
    tk.must_exec("insert t values (1, 42)");
    tk.must_exec("set tx_isolation = 'read-committed'");
    tk.check("select @@tx_isolation", &["READ-COMMITTED"]);
    tk.must_exec("set tx_isolation = 'repeatable-read'");
    tk.must_exec("set transaction isolation level read committed");
    tk.check("select @@tx_isolation_one_shot", &["READ-COMMITTED"]);
    tk.check("select @@tx_isolation", &["REPEATABLE-READ"]);
    for _ in 0..2 {
        let seen = Arc::new(AtomicUsize::new(0));
        let observed = seen.clone();
        let interceptor = tikv_client::new_rpc_interceptor("CheckSelectRequestHook", move |_, req, next| {
            if let Some(req) = req.as_any().downcast_ref::<tikv_client::proto::coprocessor::Request>() {
                assert_eq!(req.context.as_ref().unwrap().isolation_level, tikv_client::proto::kvrpcpb::IsolationLevel::Si as i32);
                observed.fetch_add(1, Ordering::SeqCst);
            }
            Box::pin(async move { next().await })
        });
        tk.exec_with_interceptor(interceptor, "select * from t where k = 1").unwrap();
        assert!(seen.load(Ordering::SeqCst) > 0);
    }
    tk.must_exec("begin");
    assert!(tk.exec("set transaction isolation level read committed").is_err());
}
/// Go `txntest.TestExactStalenessTransaction` (stale_read_test.go).
#[test]
fn test_exact_staleness_transaction() {
    use chrono::TimeZone as _;
    use tidb_executor::sessionctx::SessionContext as _;
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    let exact = "START TRANSACTION READ ONLY AS OF TIMESTAMP '2020-09-06 00:00:00';";
    let bounded = "START TRANSACTION READ ONLY AS OF TIMESTAMP tidb_bounded_staleness('2015-09-21 00:07:01', NOW());";
    for (pre, sql, stale, physical, zone) in [
        ("begin", exact, true, chrono::Local.with_ymd_and_hms(2020, 9, 6, 0, 0, 0).unwrap().timestamp_millis(), "sh"),
        (exact, "begin", false, 0, ""),
        ("begin", bounded, true, chrono::Local.with_ymd_and_hms(2015, 9, 21, 0, 7, 1).unwrap().timestamp_millis(), "bj"),
        (bounded, "begin", false, 0, ""),
    ] {
        let _scope = tidb_failpoint::enable_for_test("tikvclient/injectTxnScope", &format!("return(\"{zone}\")"));
        tk.must_exec(pre);
        tk.must_exec(sql);
        assert_eq!(tk.session().txn_is_staleness(), stale);
        let start = tk.session().current_tso().value() >> 18;
        if physical > 0 { assert_eq!(start, physical); }
        else { assert!((chrono::Local::now().timestamp_millis() - start).abs() < 1000); }
        tk.must_exec("commit");
    }
}

/// Go `txntest.TestSelectAsOf` (stale_read_test.go).
#[test]
fn test_select_as_of() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("INSERT INTO mysql.tidb VALUES ('tikv_gc_safe_point', '20160102-15:04:05 -0700', 'All versions after safe point can be accessed. (DO NOT EDIT)') ON DUPLICATE KEY UPDATE variable_value = '20160102-15:04:05 -0700', comment = 'All versions after safe point can be accessed. (DO NOT EDIT)'");
    tk.must_exec("drop table if exists t");
    tk.must_exec("drop table if exists b");
    tk.must_exec("create table t (id int primary key)");
    tk.must_exec("create table b (pid int primary key)");
    std::thread::sleep(std::time::Duration::from_secs(3));
    let now = chrono::Local::now();
    while (store.tikv_store().get_timestamp_async().wait().unwrap() >> 18) as i64 <= now.timestamp_millis() {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let now_str = now.format("%Y-%m-%d %H:%M:%S");
    for (set_txn, sql, error) in [
        (true, "select * from t;".to_owned(), ""),
        (true, format!("select * from t as of timestamp '{now_str}';"), "can't use select as of while already set transaction as of"),
        (false, format!("select * from t as of timestamp '{now_str}';"), ""),
        (false, format!("select * from t as of timestamp TIMESTAMP('{now_str}');"), ""),
    ] {
        if set_txn { tk.must_exec(&format!("set transaction read only as of timestamp '{now_str}';")); }
        let _assert = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/assertStaleTSO", &format!("return({})", if error.is_empty() { now.timestamp() } else { 0 }));
        if !error.is_empty() { tk.must_contain_err_msg(&sql, error); continue; }
        tk.must_exec(&sql);
        if set_txn { assert_eq!(tk.session().vars().txn_read_ts(), 0); }
    }
    for (sql, previous_seconds, error) in [
        ("select * from t as of timestamp NOW() - INTERVAL 2 SECOND;", 2, ""),
        ("select * from t as of timestamp TIMESTAMP(NOW() - INTERVAL 2 SECOND);", 2, ""),
        ("select * from t as of timestamp TIMESTAMP(NOW() - INTERVAL 1 SECOND), b as of timestamp TIMESTAMP(NOW() - INTERVAL 1 SECOND);", 1, ""),
        ("select * from t as of timestamp TIMESTAMP(NOW() - INTERVAL 1 SECOND), b as of timestamp TIMESTAMP('2020-09-06 00:00:00');", 0, "can not set different time in the as of"),
        ("select * from t as of timestamp TIMESTAMP(NOW() - INTERVAL 1 SECOND), b;", 0, "can not set different time in the as of"),
        ("select * from t, b as of timestamp TIMESTAMP(NOW() - INTERVAL 1 SECOND);", 0, "can not set different time in the as of"),
        ("select * from (select * from t as of timestamp TIMESTAMP(NOW() - INTERVAL 1 SECOND), b as of timestamp TIMESTAMP(NOW() - INTERVAL 1 SECOND)) as c, b;", 0, "can not set different time in the as of"),
        ("select * from (select * from t as of timestamp TIMESTAMP(NOW() - INTERVAL 2 SECOND), b as of timestamp TIMESTAMP(NOW() - INTERVAL 2 SECOND)) as c;", 2, ""),
        ("select * from (select * from t as of timestamp TIMESTAMP(NOW() - INTERVAL 20 SECOND), b as of timestamp TIMESTAMP(NOW() - INTERVAL 20 SECOND)) as c as of timestamp Now();", 0, "You have an error in your SQL syntax"),
    ] {
        let _now = (previous_seconds > 0).then(|| tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/expression/injectNow", &format!("return({})", now.timestamp())));
        let _assert = (previous_seconds > 0).then(|| tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/assertStaleTSO", &format!("return({})", now.timestamp() - previous_seconds)));
        if error.is_empty() { tk.must_exec(sql); } else { tk.must_contain_err_msg(sql, error); }
    }
    tk.must_exec("drop table if exists b");
    tk.must_exec("drop table if exists t");
}

/// Go `txntest.TestStaleReadNoBackoff` (stale_read_test.go).
#[test]
fn test_stale_read_no_backoff() {
    use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
    use tikv_client::proto::{kvrpcpb, errorpb};
    let _config = super::realtikv_pessimistictest::ConfigRestore::new();
    let mut config = (*tidb_config::config_tree::config::get_global_config()).clone();
    config.labels = std::collections::HashMap::from([("zone".into(), "us-east-1a".into())]);
    tidb_config::config_tree::config::store_global_config(Arc::new(config));
    assert_eq!(tidb_config::config_tree::config::get_txn_scope_from_config(), "us-east-1a");
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (id int primary key)");
    tk.must_exec("insert into t values (1)");
    tk.must_exec("set session tidb_read_staleness = -1");
    tk.must_exec("set session tidb_replica_read='closest-replicas'");
    std::thread::sleep(std::time::Duration::from_secs(1));
    let seen = Arc::new(AtomicUsize::new(0));
    let observed = seen.clone();
    let interceptor = tikv_client::new_rpc_interceptor("fail-stale-read", move |_, req, next| {
        if req.as_any().downcast_ref::<kvrpcpb::GetRequest>().and_then(|req| req.context.as_ref())
            .is_some_and(|ctx| ctx.stale_read && !ctx.is_retry_request) {
            observed.fetch_add(1, Ordering::SeqCst);
            return Box::pin(async { Ok(Box::new(kvrpcpb::GetResponse {
                region_error: Some(errorpb::Error { data_is_not_ready: Some(errorpb::DataIsNotReady::default()), ..Default::default() }),
                ..Default::default()
            }) as Box<dyn std::any::Any>) });
        }
        Box::pin(async move { next().await })
    });
    let rows = tk.exec_with_interceptor(interceptor, "explain analyze select * from t where id = 1").unwrap();
    let explain = rows.iter().flatten().cloned().collect::<Vec<_>>().join("\t");
    assert_eq!(seen.load(Ordering::SeqCst), 1);
    assert!(explain.contains("rpc_errors:{data_is_not_ready:1"), "{explain}");
    assert!(!explain.contains("dataNotReady_backoff"), "{explain}");
}
/// Go `txntest.TestDMLWithAddForeignKey` (txn_test.go).
#[test]
fn test_dml_with_add_foreign_key() {
    let _config = super::realtikv_pessimistictest::AsyncCommitConfig::set(std::time::Duration::from_secs(10), std::time::Duration::from_millis(500));
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("set global tidb_enable_1pc='OFF'");
    tk.must_exec("set global tidb_enable_metadata_lock='OFF'");
    tk.must_exec("set global tidb_enable_async_commit='ON'");
    let mut tk_dml = store.new_tk();
    let mut tk_ddl = store.new_tk();
    tk_dml.must_exec("use test");
    tk_ddl.must_exec("use test");
    tk_ddl.must_exec("create table parent (id int primary key, val int, index(val));");
    tk_ddl.must_exec("create table child (id int primary key, val int, index(val));");
    let _before = tidb_failpoint::enable_for_test("tikvclient/beforePrewrite", "pause");
    let _after = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterCheckForeignKeyConstrain", |_| {
        tidb_failpoint::disable("tikvclient/beforePrewrite");
    });
    let _commit = tidb_failpoint::enable_for_test("tikvclient/asyncCommitDoNothing", "pause");
    let dml = std::thread::spawn(move || tk_dml.exec("insert into child values (1, 1)"));
    let ddl = std::thread::spawn(move || {
        let result = tk_ddl.exec("alter table child add foreign key fk(val) references parent (val);");
        tidb_failpoint::disable("tikvclient/asyncCommitDoNothing");
        result
    });
    let err_dml = dml.join().unwrap();
    let err_ddl = ddl.join().unwrap();
    assert!(err_dml.is_err() || err_ddl.is_err());
}
/// Go `txntest.TestTxnGoString` (txn_test.go).
#[test]
fn test_txn_go_string() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists gostr");
    tk.must_exec("create table gostr (id int)");
    assert_eq!(tk.session().node_conn().txn_go_string(), "Txn{state=invalid}");
    tk.must_exec("begin");
    let ts = tk.session().current_tso().value();
    assert_eq!(tk.session().node_conn().txn_go_string(), format!("Txn{{state=valid, txnStartTS={ts}}}"));
    tk.must_exec("insert into gostr values (1)");
    assert_eq!(tk.session().node_conn().txn_go_string(), format!("Txn{{state=valid, txnStartTS={ts}}}"));
    tk.must_exec("rollback");
    assert_eq!(tk.session().node_conn().txn_go_string(), "Txn{state=invalid}");
}

/// Go `txntest.TestIssue62775` (txn_test.go).
#[test]
fn test_issue62775() {
    let _config = super::realtikv_pessimistictest::ConfigRestore::new();
    let mut config = (*tidb_config::config_tree::config::get_global_config()).clone();
    config.pessimistic_txn.pessimistic_auto_commit = tidb_config::config_tree::AtomicBool::new(true);
    tidb_config::config_tree::config::store_global_config(std::sync::Arc::new(config));
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    for parentheses in [false, true] {
        let column = if parentheses { "(v)" } else { "v" };
        for (create, prepare, condition, expected) in [
            ("create table t (id int primary key, v int)", "insert into t values (1, 10)", " where id = 1", vec![vec!["10".to_owned()]]),
            ("create table t (id varchar(64) primary key clustered, v int)", "insert into t values (\"a\", 10)", " where id = \"a\"", vec![vec!["10".to_owned()]]),
            ("create table t (id varchar(64) primary key nonclustered, v int)", "insert into t values (\"a\", 10)", " where id = \"a\"", vec![vec!["10".to_owned()]]),
            ("create table t (id int primary key, v int)", "insert into t values (1, 10), (2, 20)", "", vec![vec!["10".to_owned()], vec!["20".to_owned()]]),
        ] {
            tk.must_exec(create);
            tk.must_exec(prepare);
            let mut internal = store.new_tk();
            internal.session().set_restricted_sql(true);
            internal.session().set_connection_id(0);
            internal.session().set_internal_request_source(tidb_txnkv::INTERNAL_TXN_GC);
            internal.must_exec("use test");
            assert_eq!(internal.session().vars().last_commit_ts, 0);
            let sql = format!("select {column} from t{condition} for update");
            assert_eq!(internal.must_query(&sql), expected);
            assert_eq!(internal.session().vars().last_commit_ts, 0);
            internal.must_exec("update t set v = v + 1");
            assert_ne!(internal.session().vars().last_commit_ts, 0);
            tk.must_exec("drop table if exists t");
        }
    }
}
/// Go `config.UpdateGlobal` for the stale-read tests' Labels.
fn set_labels(labels: &[(&str, &str)]) {
    tidb_config::config_tree::config::update_global(|conf| {
        conf.labels = labels.iter().map(|(key, value)| ((*key).into(), (*value).into())).collect();
    });
}

/// Go `txntest.TestTxnScopeAndValidateReadTs` (stale_read_test.go).
#[test]
fn test_txn_scope_and_validate_read_ts() {
    let _config = super::realtikv_pessimistictest::ConfigRestore::new();
    set_labels(&[("zone", "bj")]);
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("create table t1 (id int primary key)");
    std::thread::sleep(std::time::Duration::from_secs(1));
    tk.check("select * from t1 AS OF TIMESTAMP NOW() where id = 1", &[]);
    for replica in ["closest-replicas", "follower", "closest-adaptive"] {
        tk.must_exec(&format!("set @@tidb_replica_read = '{replica}'"));
        tk.must_exec("begin");
        tk.check("select * from t1 where id = 1", &[]);
        tk.must_exec("commit");
    }
}

/// Go `txntest.TestStaleReadPrepare` (stale_read_test.go).
#[test]
fn test_stale_read_prepare() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (id int)");
    std::thread::sleep(std::time::Duration::from_secs(2));
    let _config = super::realtikv_pessimistictest::ConfigRestore::new();
    set_labels(&[("zone", "sh")]);
    let time1 = chrono::Local::now();
    let tso = (time1.timestamp() as u64 * 1000) << 18;
    let stamp = time1.format("%Y-%m-%d %H:%M:%S");
    std::thread::sleep(std::time::Duration::from_millis(200));
    let assertion = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/assertExecutePrepareStatementStalenessOption", &format!("return(\"{tso}_sh\")"));
    tk.must_exec(&format!("prepare p1 from \"select * from t as of timestamp '{stamp}'\""));
    tk.must_exec("execute p1");
    tk.must_exec("prepare p2 from \"select * from t\"");
    tk.must_exec(&format!("start transaction read only as of timestamp '{stamp}'"));
    tk.must_exec("execute p2");
    tk.must_exec("commit");
    tk.must_exec(&format!("set transaction read only as of timestamp '{stamp}'"));
    tk.must_exec("execute p2");
    drop(assertion);
    let stamp = time1.format("%Y-%m-%d %H:%M:%S%.3f");
    tk.must_exec(&format!("start transaction read only as of timestamp '{stamp}'"));
    assert!(tk.exec("execute p1").is_err());
    tk.must_exec("commit");
    tk.must_exec(&format!("set transaction read only as of timestamp '{stamp}'"));
    assert!(tk.exec("execute p1").is_err());
    tk.must_exec("execute p2");
    tk.must_exec("create table t1 (id int, v int)");
    tk.must_exec("insert into t1 values (1,10)");
    std::thread::sleep(std::time::Duration::from_millis(50));
    tk.must_exec("begin");
    tk.must_exec("set @a=tidb_parse_tso(@@tidb_current_ts)");
    tk.must_exec("commit");
    std::thread::sleep(std::time::Duration::from_millis(50));
    tk.must_exec("update t1 set v=100 where id=1");
    tk.check("select * from t1", &["1 100"]);
    tk.must_exec("prepare s1 from 'select * from t1 as of timestamp @a where id=1'");
    tk.check("execute s1", &["1 10"]);
    tk.must_exec("drop table if exists t");
}

/// Go `txntest.TestStaleReadKVRequest` (stale_read_test.go).
#[test]
fn test_stale_read_kv_request() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("INSERT INTO mysql.tidb VALUES ('tikv_gc_safe_point', '20160102-15:04:05 -0700', 'All versions after safe point can be accessed. (DO NOT EDIT)') ON DUPLICATE KEY UPDATE variable_value = '20160102-15:04:05 -0700', comment = 'All versions after safe point can be accessed. (DO NOT EDIT)'");
    tk.must_exec("use test");
    for table in ["t", "t1", "t2"] { tk.must_exec(&format!("drop table if exists {table}")); }
    tk.must_exec("create table t (id int primary key)");
    tk.must_exec("create table t1 (c int primary key, d int,e int,index idx_d(d),index idx_e(e))");
    std::thread::sleep(std::time::Duration::from_secs(2));
    let _config = super::realtikv_pessimistictest::ConfigRestore::new();
    set_labels(&[("zone", "sh")]);
    tk.must_exec("set @@tidb_replica_read='closest-replicas'");
    for mode in 0..3 {
        for (sql, assertion) in [
            ("select * from t", "github.com/pingcap/tidb/pkg/distsql/assertRequestBuilderReplicaOption"),
            ("select * from t where id = 1", "github.com/pingcap/tidb/pkg/executor/assertPointReplicaOption"),
            ("select * from t where id in (1,2,3)", "github.com/pingcap/tidb/pkg/executor/assertBatchPointReplicaOption"),
        ] {
            let _assertion = tidb_failpoint::enable_for_test(assertion, "return(\"sh\")");
            match mode {
                0 => tk.must_exec("START TRANSACTION READ ONLY AS OF TIMESTAMP NOW() - INTERVAL 1 SECOND"),
                1 => { tk.must_exec("SET TRANSACTION READ ONLY AS OF TIMESTAMP NOW() - INTERVAL 1 SECOND"); tk.must_exec("begin"); }
                _ => {},
            }
            tk.must_query(sql);
            if mode < 2 { tk.must_exec("commit"); }
        }
    }
    tk.must_exec("insert into t1 (c,d,e) values (1,1,1)");
    tk.must_exec("insert into t1 (c,d,e) values (2,3,5)");
    std::thread::sleep(std::time::Duration::from_secs(2));
    let tsv = (chrono::Local::now() - chrono::Duration::seconds(1)).format("%Y-%m-%d %H:%M:%S%.3f").to_string();
    tk.must_exec("insert into t1 (c,d,e) values (3,3,7)");
    tk.must_exec("insert into t1 (c,d,e) values (4,0,5)");
    tk.must_exec("insert into t1 (c,d,e) values (5,0,5)");
    for (sql, count) in [
        (format!("select * from t1 AS OF TIMESTAMP '{tsv}' use index (idx_d) where c < 5 and d < 5"), 2),
        (format!("select /*+ USE_INDEX_MERGE(t1, idx_d, idx_e) */ * from t1 AS OF TIMESTAMP '{tsv}' where c <5 and (d =5 or e=5)"), 1),
        (format!("select * from t1 AS OF TIMESTAMP '{tsv}' where c < 6"), 2),
        (format!("select /*+ USE_INDEX(t1, idx_d) */ d from t1 AS OF TIMESTAMP '{tsv}' where c < 5 and d < 1"), 0),
        (format!("select * from t1 AS OF TIMESTAMP '{tsv}' where c = 3"), 0),
        (format!("select * from t1 AS OF TIMESTAMP '{tsv}' where c in (3,4,5)"), 0),
    ] { assert_eq!(tk.must_query(&sql).len(), count, "{sql}"); }
    tk.must_exec("drop table if exists t1");
    tk.must_exec("drop table if exists t");
}

/// Go `txntest.TestStaleReadAllCombinations` (stale_read_test.go).
#[test]
fn test_stale_read_all_combinations() {
    let store = realtikv!();
    let _config = super::realtikv_pessimistictest::ConfigRestore::new();
    let mut tk = store.new_tk();
    tk.must_exec("INSERT INTO mysql.tidb VALUES ('tikv_gc_safe_point', '20160102-15:04:05 -0700', 'All versions after safe point can be accessed. (DO NOT EDIT)') ON DUPLICATE KEY UPDATE variable_value = '20160102-15:04:05 -0700', comment = 'All versions after safe point can be accessed. (DO NOT EDIT)'");
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (id int primary key, v int)");
    tk.must_exec("insert into t values (1, 10)");
    std::thread::sleep(std::time::Duration::from_secs(3));
    let first_time = ts_time(store.tikv_store().get_timestamp_async().wait().unwrap()) - chrono::Duration::milliseconds(1500);
    let mut external_ts = store.tikv_store().get_timestamp_async().wait().unwrap();
    std::thread::sleep(std::time::Duration::from_secs(3));
    tk.must_exec("insert into t values (2, 20)");
    let row2_created = ts_time(store.tikv_store().get_timestamp_async().wait().unwrap());
    std::thread::sleep(std::time::Duration::from_secs(3));
    let second_time = ts_time(store.tikv_store().get_timestamp_async().wait().unwrap()) - chrono::Duration::milliseconds(1500);
    let first = first_time.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
    let second = second_time.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
    let labels: &[&[(&str, &str)]] = &[&[], &[("zone", "bj")], &[("dc", "dc1")], &[("rack", "rack1")], &[("zone", "bj"), ("dc", "dc1"), ("rack", "rack1")]];
    let replicas = ["leader", "follower", "closest-replicas", "closest-adaptive"];
    // Go runs all relative-time cases first, so later cases cannot consume their timing buffer.
    for label in labels {
        set_labels(label);
        for replica in replicas {
            tk.must_exec(&format!("set @@tidb_replica_read='{replica}'"));
            let elapsed = ts_time(store.tikv_store().get_timestamp_async().wait().unwrap()) - row2_created;
            let staleness = elapsed.num_seconds() + 2;
            tk.must_exec(&format!("set @@tidb_read_staleness='-{staleness}'"));
            tk.check("select * from t", &["1 10"]);
            tk.must_exec("set @@tidb_read_staleness=''");
        }
    }
    for label in labels {
        set_labels(label);
        for replica in replicas {
            tk.must_exec(&format!("set @@tidb_replica_read='{replica}'"));
            tk.check(&format!("SELECT * FROM t AS OF TIMESTAMP '{first}'"), &["1 10"]);
            tk.check(&format!("SELECT * FROM t AS OF TIMESTAMP '{second}'"), &["1 10", "2 20"]);
            tk.must_exec(&format!("SET TRANSACTION READ ONLY AS OF TIMESTAMP '{first}'"));
            tk.check("select * from t", &["1 10"]);
            tk.must_exec("set @@tidb_enable_external_ts_read=1");
            tk.must_exec(&format!("set @@global.tidb_external_ts={external_ts}"));
            external_ts += 1;
            tk.check("select * from t", &["1 10"]);
            tk.must_exec("set @@tidb_enable_external_ts_read=0");
            tk.must_exec(&format!("set @@tidb_snapshot='{first}'"));
            tk.check("select * from t", &["1 10"]);
            tk.must_exec("set @@tidb_snapshot=''");
            for set_then_begin in [false, true] {
                tk.must_exec(&format!("set @@tidb_replica_read='{replica}'"));
                tk.must_exec(&format!("{} TRANSACTION READ ONLY AS OF TIMESTAMP '{first}'", if set_then_begin { "SET" } else { "START" }));
                if set_then_begin { tk.must_exec("BEGIN"); }
                tk.check("select * from t", &["1 10"]);
                tk.must_exec("commit");
            }
        }
    }
}
/// Go `txntest.TestStmtCtxStaleFlag` (stale_read_test.go).
#[test]
fn test_stmt_ctx_stale_flag() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (id int)");
    std::thread::sleep(std::time::Duration::from_secs(2));
    let time1 = ts_time(store.tikv_store().get_timestamp_async().wait().unwrap()).format("%Y-%m-%d %H:%M:%S").to_string();
    for (sql, has_stale_flag) in [
        (format!("select * from t as of timestamp '{time1}'"), true),
        ("select * from t".into(), false),
        (format!("start transaction read only as of timestamp '{time1}'"), false),
        ("select * from t".into(), true),
        ("commit".into(), false),
        (format!("set transaction read only as of timestamp '{time1}'"), false),
        ("select * from t".into(), true),
        ("select * from t".into(), false),
        (format!("prepare p from 'select * from t as of timestamp \"{time1}\"'"), false),
        ("execute p".into(), true),
        ("prepare p1 from 'select * from t'".into(), false),
        ("execute p1".into(), false),
        (format!("start transaction read only as of timestamp '{time1}'"), false),
        ("execute p1".into(), true),
        ("commit".into(), false),
    ] {
        // Preserve Go's literal path (its typo makes this particular hook inert in Go too).
        let _assertion = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/exector/assertStmtCtxIsStaleness", &format!("return({has_stale_flag})"));
        tk.must_exec(&sql);
        assert!(!tk.session().is_stmt_staleness(), "{sql}");
    }
    tk.must_exec("drop table if exists t");
}

/// Go `txntest.TestGetCachedStore` (isolation_test.go).
#[test]
fn test_get_cached_store() {
    let Ok(pd) = std::env::var("TIDB_REALTIKV_PD") else { return; };
    let first = tidb_txnkv::driver::tikv_store::TikvStore::connect(vec![pd.clone()], tikv_client::Config::default()).unwrap();
    let second = tidb_txnkv::driver::tikv_store::TikvStore::connect(vec![pd], tikv_client::Config::default()).unwrap();
    assert_eq!(first.authority_id(), second.authority_id());
    first.close().unwrap();
    assert!(second.store().is_closed());
}

/// Go `txntest.TestCheckTxnStatusOnOptimisticTxnBreakConsistency` (txn_test.go).
#[test]
fn test_check_txn_status_on_optimistic_txn_break_consistency() {
    use std::sync::{Arc, atomic::{AtomicBool, AtomicUsize, Ordering}};
    use std::time::Duration;
    use tikv_client::proto::kvrpcpb;
    let _config = super::realtikv_pessimistictest::AsyncCommitConfig::set(Duration::from_millis(500), Duration::ZERO);
    let store = realtikv!();
    let mut prepare1 = store.new_tk();
    let mut prepare2 = store.new_tk();
    let mut tk1 = store.new_tk();
    let mut tk2 = store.new_tk();
    for tk in [&mut prepare1, &mut prepare2, &mut tk1, &mut tk2] { tk.must_exec("use test"); }
    tk1.must_exec("create table t (id int primary key, v int)");
    tk1.must_exec("insert into t values (1, 10), (2, 20)");
    tk1.must_exec("create table t2 (id int primary key, v int unique)");
    tk1.must_exec("insert into t2 values (1, 10)");
    prepare1.must_exec("set @@tidb_enable_async_commit = 1");
    tk1.must_exec("set @@tidb_enable_async_commit = 0");
    let mut last_commit_ts = 0;
    let mut collision = false;
    for _ in 0..=1000 {
        prepare1.must_exec("update t set v = 10 where id = 1");
        let pause = tidb_failpoint::enable_for_test("tikvclient/beforePrewrite", "pause");
        let (send, recv) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            prepare1.must_exec("update t set v = v + 1 where id = 1");
            assert!(send.send(prepare1).is_ok());
        });
        prepare2.check("select * from t where id = 1", &["1 10"]);
        tk1.must_exec("begin optimistic");
        drop(pause);
        prepare1 = recv.recv_timeout(Duration::from_secs(1)).expect("prewrite did not resume");
        worker.join().unwrap();
        last_commit_ts = prepare1.must_query("select json_extract(@@tidb_last_txn_info, '$.commit_ts')")[0][0].parse().unwrap();
        let current_start_ts: u64 = tk1.must_query("select @@tidb_current_ts")[0][0].parse().unwrap();
        if current_start_ts == last_commit_ts { collision = true; break; }
        tk1.must_exec("rollback");
    }
    assert!(collision, "failed to construct the ts collision situation of async commit transaction");
    tk1.must_exec("update t set v = v + 100 where id = 1");
    tk1.must_exec("update t set v = v + 100 where id = 2");
    tk1.must_exec("update t2 set v = v + 1 where id = 1");
    let _ttl = tidb_failpoint::enable_for_test("tikvclient/twoPCShortLockTTL", "return");
    let _heartbeat = tidb_failpoint::enable_for_test("tikvclient/doNotKeepAlive", "return");
    let _batch = tidb_failpoint::enable_for_test("tikvclient/twoPCRequestBatchSizeLimit", "return");
    let _hook = tidb_failpoint::enable_for_test("tikvclient/onRPCFinishedHook", "return");
    let release = Arc::new(AtomicBool::new(false));
    let released = release.clone();
    let primary_count = Arc::new(AtomicUsize::new(0));
    let count = primary_count.clone();
    let interceptor = tikv_client::new_rpc_interceptor("onRPCFinishedHook", move |_, request, next| {
        let primary = if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::PrewriteRequest>() {
            assert_eq!(req.mutations.len(), 1, "twoPCRequestBatchSizeLimit must take effect");
            req.start_version == last_commit_ts && req.mutations[0].key == req.primary_lock
        } else { false };
        let count = count.clone();
        let released = released.clone();
        Box::pin(async move {
            // The response (not `Send`) leaves scope before the wait below.
            {
                let response = next().await;
                let handled = response.as_ref().ok().and_then(|response| response.downcast_ref::<kvrpcpb::PrewriteResponse>()).is_some_and(|response| response.region_error.is_none());
                if !(primary && handled && count.fetch_add(1, Ordering::SeqCst) == 0) {
                    return response;
                }
            }
            let deadline = std::time::Instant::now() + Duration::from_secs(20);
            while !released.load(Ordering::SeqCst) {
                assert!(std::time::Instant::now() < deadline, "test did not release primary prewrite");
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(tikv_client::Error::StringError("injected rpc error in onRPCFinishedHook".into()))
        })
    });
    let (send, recv) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || { send.send(tk1.exec_with_interceptor(interceptor, "commit")).unwrap(); });
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        assert!(matches!(recv.recv_timeout(Duration::from_millis(50)), Err(std::sync::mpsc::RecvTimeoutError::Timeout)));
        tk2.must_exec("update t set v = v + 1 where id = 2");
        tk2.must_exec("insert into t2 values (2, 11)");
        assert!(matches!(recv.recv_timeout(Duration::from_millis(50)), Err(std::sync::mpsc::RecvTimeoutError::Timeout)));
    }));
    release.store(true, Ordering::SeqCst);
    let outcome = recv.recv_timeout(Duration::from_secs(1)).expect("commit did not resume");
    worker.join().unwrap();
    if let Err(panic) = result { std::panic::resume_unwind(panic); }
    let error = outcome.expect_err("commit must conflict after lock resolution");
    assert!(error.contains("[kv:9007]"), "{error}");
    assert!(primary_count.load(Ordering::SeqCst) > 0);
    tk2.check("select * from t order by id", &["1 11", "2 21"]);
    tk2.must_exec("admin check table t2");
    tk2.check("select * from t2 order by id", &["1 10", "2 11"]);
}
