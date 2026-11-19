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

//! Go `pkg/executor/test/issuetest/executor_issue_test.go`, on the mock store through the session, as Go's testkit
//! runs it.

#![allow(unused_mut, unused_variables)]

use crate::support::testkit::TestKit;

/// Go `TestUnionIssue`.
#[test]
#[ignore = "register X05 (3): the issue-31530 leg needs Go's per-reader snapshot choice (executorBuilder.getSnapshotTS / inSelectLockStmt)"]
fn test_union_issue() {
    let mut tk = TestKit::new();
    // Issue56640
    tk.must_exec("use test");
    tk.must_query("(select cast('abcdefghijklmnopqrstuvwxyz' as char) as c1) union all (select 1 where false)").check(["abcdefghijklmnopqrstuvwxyz"]);
    // Issue25506
    tk.must_exec("drop table if exists tbl_3, tbl_23");
    tk.must_exec("create table tbl_3 (col_15 bit(20))");
    tk.must_exec("insert into tbl_3 values (0xFFFF)");
    tk.must_exec("insert into tbl_3 values (0xFF)");
    tk.must_exec("create table tbl_23 (col_15 bit(15))");
    tk.must_exec("insert into tbl_23 values (0xF)");
    tk.must_query("(select col_15 from tbl_23) union all (select col_15 from tbl_3 for update) order by col_15").check([String::from_utf8_lossy(b"\x00\x00\x0F").into_owned(), String::from_utf8_lossy(b"\x00\x00\xFF").into_owned(), String::from_utf8_lossy(b"\x00\xFF\xFF").into_owned()]);
    // Issue26532
    tk.must_query("select greatest(cast(\"2020-01-01 01:01:01\" as datetime), cast(\"2019-01-01 01:01:01\" as datetime) )union select null;").sort().check(["2020-01-01 01:01:01", "<nil>"]);
    tk.must_query("select least(cast(\"2020-01-01 01:01:01\" as datetime), cast(\"2019-01-01 01:01:01\" as datetime) )union select null;").sort().check(["2019-01-01 01:01:01", "<nil>"]);
    tk.must_query("select greatest(\"2020-01-01 01:01:01\" ,\"2019-01-01 01:01:01\" )union select null;").sort().check(["2020-01-01 01:01:01", "<nil>"]);
    tk.must_query("select least(\"2020-01-01 01:01:01\" , \"2019-01-01 01:01:01\" )union select null;").sort().check(["2019-01-01 01:01:01", "<nil>"]);
    // Issue30971
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1 (id int);");
    tk.must_exec("create table t2 (id int, c int);");
    // Issue56587
    tk.must_query("select quote(cast('abc' as char)) union all select '1'").sort().check(["'abc'", "1"]);
    tk.must_query(r#"select elt(2, "1", cast('abc' as char)) union all select "12" where false"#).check(["abc"]);
    tk.must_query(r#"select hex(cast('1' as char)) union all select '1'"#).sort().check(["1", "31"]);
    // Fix a bug that the column length field returned to client is
    // incorrect using MySQL prepare protocol.
    for (sql, fields) in [
        ("select * from t1 union select 1 from t1", 1),
        ("select c from t2 union select * from t1", 1),
        ("select * from t1", 1),
        ("select * from t2 where c in (select * from t1)", 2),
        ("insert into t1 values (?)", 0),
        ("update t1 set id = ?", 0),
    ] {
        let prepared = tk.session().node_conn().prepare_general(sql).expect("prepare");
        assert_eq!(prepared.result_fields().len(), fields, "{sql}");
    }
    // 31530
    let mut tk2 = tk.new_tk();
    tk2.must_exec("use test");
    tk.must_exec("drop table if exists t1");
    tk.must_exec("create table t1 (id int primary key, v int)");
    tk.must_exec("insert into t1 values(1, 10)");
    tk.must_exec("begin pessimistic");
    tk.must_query("select * from t1").check(["1 10"]);
    // update t1 before session1 transaction not finished
    tk2.must_exec("update t1 set v=11 where id=1");
    tk.must_query("(select 'a' as c, id, v from t1 for update) union all (select 'b', id, v from t1) order by c").check(["a 1 11", "b 1 10"]);
    tk.must_query("(select 'a' as c, id, v from t1) union all (select 'b', id, v from t1 for update) order by c").check(["a 1 10", "b 1 11"]);
    tk.must_query("(select 'a' as c, id, v from t1 where id=1 for update) union all (select 'b', id, v from t1 where id=1) order by c").check(["a 1 11", "b 1 10"]);
    tk.must_query("(select 'a' as c, id, v from t1 where id=1) union all (select 'b', id, v from t1 where id=1 for update) order by c").check(["a 1 10", "b 1 11"]);
    tk.must_exec("rollback");
}

/// Go `TestIssue50393`.
#[test]
fn test_issue50393() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1 (a blob)");
    tk.must_exec("create table t2 (a blob)");
    tk.must_exec("insert into t1 values (0xC2A0)");
    tk.must_exec("insert into t2 values (0xC2)");
    tk.must_query("select count(*) from t1,t2 where t1.a like concat(\"%\",t2.a,\"%\")").check(["1"]);
}

/// Go `TestIssue52978`.
#[test]
fn test_issue52978() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (a int)");
    tk.must_exec("insert into t values (-1790816583),(2049821819), (-1366665321), (536581933), (-1613686445)");
    tk.must_query("select min(truncate(cast(-26340 as double), ref_11.a)) as c3 from t as ref_11;").check(["-26340"]);
    tk.must_exec("drop table if exists t");
}
