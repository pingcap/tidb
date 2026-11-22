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


//! Go `pkg/table/tables` cases (`tables_test.go`, `partition_test.go`) as the
//! executor's in-memory tier carried them -- through SQL rather than
//! `AddRecord` on a bare store -- over the cop-backed TestKit stack (X11).

use super::sql_kit::*;
use super::*;

fn one(tk: &mut Session, sql: &str) -> String {
    q(tk, sql)[0][0].clone()
}

#[test]
fn test_basic() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 840);
    exec_all(&mut tk, &[
        "create table t (a int primary key auto_increment, b varchar(255) unique)",
        "insert into t values (1, 'abc')",
        "update t set b = 'cba' where a = 1",
    ]);
    assert_eq!(q(&mut tk, "select a, b from t"), [["1", "cba"]]);
    exec_all(&mut tk, &["create table t2 (a int)", "delete from t"]);
    assert_eq!(one(&mut tk, "select count(*) from t"), "0");
}

#[test]
fn test_types() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 841);
    exec_all(&mut tk, &[
        "create table t (c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 text, c6 blob, c7 varchar(64), c10 decimal(10,1))",
        "insert into t values (1, 2, 3, 4, '5', '6', '7', 1.4)",
    ]);
    assert_eq!(q(&mut tk, "select c1, c5, c10 from t where c1 = 1"), [["1", "5", "1.4"]]);
}

#[test]
fn test_unique_index_multiple_null_entries() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 842);
    exec_all(&mut tk, &["create table t (a int primary key, b varchar(255) unique)", "insert into t values (1, null), (2, null)"]);
    assert_eq!(one(&mut tk, "select count(*) from t"), "2");
}

#[test]
fn test_unsigned_pk() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 843);
    exec_all(&mut tk, &["create table t (a bigint unsigned primary key, b varchar(255))", "insert into t values (1, 'abc')"]);
    let got = rows(&mut tk, "select a, b from t");
    assert!(matches!(got[0][0], Datum::UInt(1)), "{got:?}");
}

#[test]
fn test_iter_records() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 844);
    exec_all(&mut tk, &["create table t (a int primary key, b int)", "insert into t values (-1, 2), (2, null)"]);
    assert_eq!(q(&mut tk, "select a, b from t"), [["-1", "2"], ["2", "NULL"]]);
}

#[test]
fn test_constraint_check_for_unique_index() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 845);
    exec_all(&mut tk, &["create table t (id int primary key, k int not null, c varchar(20) not null, unique key uk(k, c))", "insert into t values (1, 1, 'tidb'), (2, 2, 'tidb')"]);
    assert_eq!(error(&mut tk, "update t set k = 1 where id = 2").code, 1062);
}

#[test]
fn partition_add_record_routes_rows_to_range_partitions() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 846);
    exec_all(&mut tk, &[
        "create table t (id int, index(id)) partition by range(id) (partition p0 values less than (6), partition p1 values less than (11), partition p2 values less than (16), partition p3 values less than (21))",
        "insert into t values (1), (7), (12), (16)",
    ]);
    assert_eq!(one(&mut tk, "select count(*) from t"), "4");
    for partition in ["p0", "p1", "p2", "p3"] {
        assert_eq!(one(&mut tk, &format!("select count(*) from t partition ({partition})")), "1", "{partition}");
    }
}

#[test]
fn hash_partition_add_record_routes_rows_to_all_hash_partitions() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 847);
    exec_all(&mut tk, &["create table t (id int, index(id)) partition by hash(id) partitions 4", "insert into t values (8), (-1), (3), (6)"]);
    for id in [8, -1, 3, 6] {
        assert_eq!(one(&mut tk, &format!("select count(*) from t where id = {id}")), "1", "id={id}");
    }
    for (partition, id) in [("p0", "8"), ("p1", "-1"), ("p3", "3"), ("p2", "6")] {
        assert_eq!(q(&mut tk, &format!("select id from t partition ({partition})")), [[id]], "{partition}");
    }
}

#[test]
fn locate_partition_routes_list_columns_rows() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 848);
    exec_all(&mut tk, &[
        "create table t (id bigint, type varchar(255)) partition by list columns(type) (partition push_event values in ('PushEvent'), partition watch_event values in ('WatchEvent'))",
        "insert into t values (1, 'PushEvent'), (2, 'WatchEvent'), (3, 'WatchEvent')",
    ]);
    assert_eq!(one(&mut tk, "select count(*) from t partition (watch_event)"), "2");
}

#[test]
fn key_partition_table_basic_is_carried_by_partition_sql_tests() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 849);
    exec_all(&mut tk, &[
        "create table t (col1 int not null, col2 int not null, col3 int not null, unique key(col3)) partition by key(col3) partitions 4",
        "insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4)",
    ]);
    assert_eq!(one(&mut tk, "select count(*) from t"), "4");
    assert_eq!(one(&mut tk, "select count(*) from t where col3 = 3"), "1");
}

#[test]
fn point_get_key_partitioning_returns_the_matching_row() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 850);
    exec_all(&mut tk, &[
        "create table t (a varchar(30) not null, b varchar(45) not null, c varchar(45) not null, primary key (b, a)) partition by key(b) partitions 5",
        "insert into t values ('Aa', 'Ab', 'Ac'), ('Ba', 'Bb', 'Bc')",
    ]);
    assert_eq!(q(&mut tk, "select * from t where b = 'Ab'"), [["Aa", "Ab", "Ac"]]);
}

#[test]
fn pruning_overflow_keeps_the_matching_row() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 851);
    exec_all(&mut tk, &[
        "create table t (a int not null, b bigint not null, primary key(a, b)) partition by hash((a * b)) partitions 13",
        "insert into t values (0, 3522101843073676459)",
    ]);
    assert_eq!(
        q(&mut tk, "select a, b from t where a in (0, 14158354938390, 0) and b in (3522101843073676459, -2846203247576845955, 838395691793635638)"),
        [["0", "3522101843073676459"]]
    );
}
