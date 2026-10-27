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

//! The DDL-then-write cases of Go `pkg/executor/test/executor/executor_test.go`
//! (`TestIssue50043`, `TestIssue51324`), on the Go testkit port.

use crate::support::testkit::TestKit;

/// Go `tk.MustGetErrMsg(sql, "[table:<code>]<message>")`, asserted as the
/// wire code and the message.
#[track_caller]
fn must_get_err(tk: &mut TestKit, sql: &str, code: u16, message: &str) {
    assert_eq!(tk.must_get_err_code(sql, code).message, message, "sql: {sql}");
}

/// Go `executor_test.go:2386::testIssue50043WithInitSQL` with an empty init
/// SQL (`TestIssue50043`): after `alter column c2 drop default` on a
/// `decimal(37,17)` column, UPDATE and INSERT ... ON DUPLICATE KEY UPDATE
/// still write 5, and the value renders with the full 17-digit scale.
#[test]
fn issue50043_alter_drop_default_then_write() {
    let mut tk = TestKit::new();
    // Test simplified case by update.
    tk.must_exec(
        "create table t (c1 boolean ,c2 decimal ( 37 , 17 ), unique key idx1 (c1 ,c2),unique key idx2 ( c1 ));",
    );
    tk.must_exec("insert into t values (0,NULL);");
    tk.must_exec("alter table t alter column c2 drop default;");
    tk.must_exec("update t set c2 = 5 where c1 = 0;");
    tk.must_query("select * from t order by c1,c2")
        .check(["0 5.00000000000000000"]);

    // Test simplified case by insert on duplicate key update.
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (c1 boolean ,c2 decimal ( 37 , 17 ), unique key idx1 (c1 ,c2));");
    tk.must_exec("alter table t alter column c2 drop default;");
    tk.must_exec("alter table t add unique key idx4 ( c1 );");
    tk.must_exec("insert into t values (0, NULL), (1, 1);");
    tk.must_exec("insert into t values (0, 2) ,(1, 3) on duplicate key update c2 = 5;");
    tk.must_query("show warnings").check(Vec::<&str>::new());
    tk.must_query("select * from t order by c1,c2")
        .check(["0 5.00000000000000000", "1 5.00000000000000000"]);

    // Test Issue 50043.
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (c1 boolean ,c2 decimal ( 37 , 17 ), unique key idx1 (c1 ,c2));");
    tk.must_exec("alter table t alter column c2 drop default;");
    tk.must_exec("alter table t add unique key idx4 ( c1 );");
    tk.must_exec("insert into t values (0, NULL), (1, 1);");
    tk.must_exec(
        "insert ignore into t values (0, 2) ,(1, 3) on duplicate key update c2 = 5, c1 = 0",
    );
    tk.must_query("select * from t order by c1,c2")
        .check(["0 5.00000000000000000", "1 1.00000000000000000"]);
}

/// Go `executor_test.go:2418::TestIssue51324`: inserts missing values
/// against nullable columns default to NULL (including the `DEFAULT`
/// keyword), against key/not-null columns fail 1364 (`Field ... doesn't have
/// a default value`) or 1048 (`Column ... cannot be null`), `alter column ...
/// drop default` turns former defaults into 1364 errors, and under `insert
/// ignore` each of those errors is a warning and the row takes the column's
/// zero value (or its origin default for a column added later).
#[test]
fn issue51324_insert_default_and_null_contract() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (id int key, a int, b enum('a', 'b'))");
    must_get_err(&mut tk, "insert into t values ()", 1364, "Field 'id' doesn't have a default value");
    tk.must_exec("insert into t set id = 1");
    tk.must_exec("insert into t set id = 2, a = NULL, b = NULL");
    tk.must_exec("insert into t set id = 3, a = DEFAULT, b = DEFAULT");
    tk.must_query("select * from t order by id")
        .check(["1 <nil> <nil>", "2 <nil> <nil>", "3 <nil> <nil>"]);

    tk.must_exec("alter table t alter column a drop default");
    tk.must_exec("alter table t alter column b drop default");
    must_get_err(&mut tk, "insert into t set id = 4;", 1364, "Field 'a' doesn't have a default value");
    tk.must_exec("insert into t set id = 5, a = NULL, b = NULL;");
    must_get_err(
        &mut tk,
        "insert into t set id = 6, a = DEFAULT, b = DEFAULT;",
        1364,
        "Field 'a' doesn't have a default value",
    );
    tk.must_query("select * from t order by id")
        .check(["1 <nil> <nil>", "2 <nil> <nil>", "3 <nil> <nil>", "5 <nil> <nil>"]);

    tk.must_exec("insert ignore into t set id = 4;");
    tk.must_query("show warnings")
        .check(["Warning 1364 Field 'a' doesn't have a default value"]);
    tk.must_exec("insert ignore into t set id = 6, a = DEFAULT, b = DEFAULT;");
    tk.must_query("show warnings")
        .check(["Warning 1364 Field 'a' doesn't have a default value"]);
    tk.must_query("select * from t order by id").check([
        "1 <nil> <nil>",
        "2 <nil> <nil>",
        "3 <nil> <nil>",
        "4 <nil> <nil>",
        "5 <nil> <nil>",
        "6 <nil> <nil>",
    ]);
    tk.must_exec("update t set id = id + 10");
    tk.must_query("show warnings").check(Vec::<&str>::new());
    tk.must_query("select * from t order by id").check([
        "11 <nil> <nil>",
        "12 <nil> <nil>",
        "13 <nil> <nil>",
        "14 <nil> <nil>",
        "15 <nil> <nil>",
        "16 <nil> <nil>",
    ]);

    // Test not null case.
    tk.must_exec("drop table t");
    tk.must_exec("create table t (id int key, a int not null, b enum('a', 'b') not null)");
    must_get_err(&mut tk, "insert into t values ()", 1364, "Field 'id' doesn't have a default value");
    must_get_err(&mut tk, "insert into t set id = 1", 1364, "Field 'a' doesn't have a default value");
    must_get_err(&mut tk, "insert into t set id = 2, a = NULL, b = NULL", 1048, "Column 'a' cannot be null");
    must_get_err(&mut tk, "insert into t set id = 2, a = 2, b = NULL", 1048, "Column 'b' cannot be null");
    must_get_err(
        &mut tk,
        "insert into t set id = 3, a = DEFAULT, b = DEFAULT",
        1364,
        "Field 'a' doesn't have a default value",
    );
    tk.must_exec("alter table t alter column a drop default");
    tk.must_exec("alter table t alter column b drop default");
    tk.must_exec("insert ignore into t set id = 4;");
    tk.must_query("show warnings")
        .check(["Warning 1364 Field 'a' doesn't have a default value"]);
    tk.must_exec("insert ignore into t set id = 5, a = NULL, b = NULL;");
    tk.must_query("show warnings").check([
        "Warning 1048 Column 'a' cannot be null",
        "Warning 1048 Column 'b' cannot be null",
    ]);
    tk.must_exec("insert ignore into t set id = 6, a = 6,    b = NULL;");
    tk.must_query("show warnings")
        .check(["Warning 1048 Column 'b' cannot be null"]);
    tk.must_exec("insert ignore into t set id = 7, a = DEFAULT, b = DEFAULT;");
    tk.must_query("show warnings")
        .check(["Warning 1364 Field 'a' doesn't have a default value"]);
    tk.must_query("select * from t order by id")
        .check(["4 0 a", "5 0 ", "6 6 ", "7 0 a"]);

    // Test add column with OriginDefaultValue case.
    tk.must_exec("drop table t");
    tk.must_exec("create table t (id int, unique key idx (id))");
    tk.must_exec("insert into t values (1)");
    tk.must_exec("alter table t add column a int default 1");
    tk.must_exec("alter table t add column b int default null");
    tk.must_exec("alter table t add column c int not null");
    tk.must_exec("alter table t add column d int not null default 1");
    tk.must_exec("insert ignore into t (id) values (2)");
    tk.must_query("show warnings")
        .check(["Warning 1364 Field 'c' doesn't have a default value"]);
    tk.must_exec("insert ignore into t (id) values (1),(2) on duplicate key update id = id+10");
    tk.must_query("show warnings")
        .check(["Warning 1364 Field 'c' doesn't have a default value"]);
    tk.must_exec("alter table t alter column a drop default");
    tk.must_exec("alter table t alter column b drop default");
    tk.must_exec("alter table t alter column c drop default");
    tk.must_exec("alter table t alter column d drop default");
    tk.must_exec("insert ignore into t (id) values (3)");
    tk.must_query("show warnings").check([
        "Warning 1364 Field 'a' doesn't have a default value",
        "Warning 1364 Field 'b' doesn't have a default value",
        "Warning 1364 Field 'c' doesn't have a default value",
        "Warning 1364 Field 'd' doesn't have a default value",
    ]);
    tk.must_exec("insert ignore into t (id) values (11),(12),(3) on duplicate key update id = id+10");
    tk.must_query("show warnings").check([
        "Warning 1364 Field 'a' doesn't have a default value",
        "Warning 1364 Field 'b' doesn't have a default value",
        "Warning 1364 Field 'c' doesn't have a default value",
        "Warning 1364 Field 'd' doesn't have a default value",
    ]);
    tk.must_query("select * from t order by id")
        .check(["13 <nil> <nil> 0 0", "21 1 <nil> 0 1", "22 1 <nil> 0 1"]);
}
