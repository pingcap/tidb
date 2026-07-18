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

//! Physical table-catalog DDL tests matching `pkg/ddl/table.go`.

use super::*;

#[test]
fn drop_table() {
    let mut db = Database::new();
    step(&mut db, "create table t1 (id int)");
    assert_eq!(step(&mut db, "drop table t1"), "OK");
    assert!(step(&mut db, "select * from t1").starts_with("UnknownTable"));

    assert!(step(&mut db, "drop table missing").starts_with("UnknownTable"));
    assert_eq!(step(&mut db, "drop table if exists missing"), "OK");

    // Missing-name handling is per object after validation: existing names on
    // either side are still dropped.
    step(&mut db, "create table ta (id int)");
    step(&mut db, "create table tc (id int)");
    assert!(step(&mut db, "drop table ta, tb, tc").starts_with("UnknownTable"));
    assert!(step(&mut db, "select * from ta").starts_with("UnknownTable"));
    assert!(step(&mut db, "select * from tc").starts_with("UnknownTable"));

    // Foreign-key validation covers the whole statement before mutation.
    step(&mut db, "create table p (id int primary key)");
    step(
        &mut db,
        "create table c (id int, pid int, foreign key (pid) references p(id))",
    );
    step(&mut db, "create table tx (id int)");
    assert_eq!(step(&mut db, "drop table tx, p"), "ForeignKeyViolation");
    assert_eq!(step(&mut db, "select * from tx"), "RS:");
    assert_eq!(step(&mut db, "select * from p"), "RS:");
    assert_eq!(step(&mut db, "drop table p, c"), "OK");
    assert!(step(&mut db, "select * from p").starts_with("UnknownTable"));

    // DROP crosses the DDL implicit-commit boundary before catalog mutation.
    step(&mut db, "create table td (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into td values (1)");
    step(&mut db, "drop table tx");
    step(&mut db, "rollback");
    assert!(step(&mut db, "select * from tx").starts_with("UnknownTable"));
}

#[test]
fn rename_table_multi_pair() {
    let mut db = Database::new();
    step(&mut db, "create table rna (v int)");
    step(&mut db, "create table rnb (v int)");
    step(&mut db, "insert into rna values (1)");
    step(&mut db, "insert into rnb values (2)");
    // Pairs apply sequentially in written order, correctly performing a
    // 3-way swap via the temporary name `rnc`.
    step(&mut db, "rename table rna to rnc, rnb to rna, rnc to rnb");
    assert_eq!(step(&mut db, "select * from rna"), "RS:2");
    assert_eq!(step(&mut db, "select * from rnb"), "RS:1");
}

/// `TRUNCATE TABLE` empties a table's rows while keeping its schema;
/// truncating an unknown table is a real error (both confirmed via `gorun`,
/// task #137). A subsequent `INSERT` still works against the now-empty table.
#[test]
fn truncate_table_exec() {
    let mut db = Database::new();
    step(&mut db, "create table t (a int)");
    step(&mut db, "insert into t values (1)");
    step(&mut db, "insert into t values (2)");
    assert_eq!(step(&mut db, "truncate table t"), "OK");
    assert_eq!(step(&mut db, "select count(*) from t"), "RS:0");
    assert_eq!(step(&mut db, "insert into t values (3)"), "OK");
    assert_eq!(step(&mut db, "select a from t"), "RS:3");
    assert_eq!(
        step(&mut db, "truncate table nosuch"),
        "UnknownTable(\"nosuch\")"
    );
    assert_eq!(step(&mut db, "truncate t"), "OK");
    assert_eq!(step(&mut db, "select count(*) from t"), "RS:0");
}
