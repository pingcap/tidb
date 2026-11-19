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

//! Go `pkg/executor/test/seqtest/prepared_test.go` (`TestPrepareWithAggregation`, `TestPreparedDelete`, `TestPreparedInsert`, `TestPreparedLimitOffset`, `TestPreparedUpdate`, `TestPrepared`), on the mock store through the session, as Go's testkit
//! runs them.

#![allow(unused_mut, unused_variables, unused_imports)]

use crate::support::testkit::*;

/// Go `TestPrepareWithAggregation`.
#[test]
fn test_prepare_with_aggregation() {
    let mut flags = [false, true];
    for flag in flags.clone() {
        let mut tk = TestKit::new();
        tk.must_exec(&format!(
            r#"set @@tidb_enable_prepared_plan_cache={}"#,
            go_v(&flag)
        ));
        let mut se = tk.new_tk();
        let mut err: Option<tidb_terror::MysqlError> = None;
        assert!(err.is_none(), "{err:?}");
        tk = se;
        tk.must_exec("use test");
        tk.must_exec("drop table if exists t");
        tk.must_exec("create table t (id int primary key)");
        tk.must_exec("insert into t values (1), (2), (3)");
        tk.must_exec(r#"prepare stmt from 'select sum(id) from t where id = ?'"#);
        tk.must_exec(r#"set @id="1""#);
        let mut r = tk.must_query(r#"execute stmt using @id;"#);
        r.check(["1"]);
        r = tk.must_query(r#"execute stmt using @id;"#);
        r.check(["1"]);
    }
}

/// Go `TestPreparedDelete`.
#[test]
fn test_prepared_delete() {
    tidb_metrics::server::RESETTABLE_PLAN_CACHE_COUNTER_FORT_TEST
        .store(true, std::sync::atomic::Ordering::Relaxed);
    tidb_metrics::server::PLAN_CACHE_COUNTER.reset();
    let counter = tidb_metrics::server::PLAN_CACHE_COUNTER.with_label_values(&["prepare"]);
    let mut flags = [false, true];
    for flag in flags.clone() {
        let mut tk = TestKit::new();
        tk.must_exec(r#"set @@tidb_enable_non_prepared_plan_cache=0"#);
        tk.must_exec(&format!(
            r#"set @@tidb_enable_prepared_plan_cache={}"#,
            go_v(&flag)
        ));
        let mut err: Option<tidb_terror::MysqlError> = None;
        tk.must_exec("use test");
        tk.must_exec("drop table if exists prepare_test");
        tk.must_exec("create table prepare_test (id int PRIMARY KEY, c1 int)");
        tk.must_exec(r#"insert into prepare_test values (1, 1)"#);
        tk.must_exec(r#"insert into prepare_test values (2, 2)"#);
        tk.must_exec(r#"insert into prepare_test values (3, 3)"#);
        tk.must_exec(r#"prepare stmt_delete from 'delete from prepare_test where id = ?'"#);
        tk.must_exec(r#"set @a=1; execute stmt_delete using @a;"#);
        if flag {
            assert!(err.is_none(), "{err:?}");
            let mut hit = counter.get();
            assert_eq!((0 as f64), hit);
        }
        tk.must_exec(r#"set @a=2; execute stmt_delete using @a;"#);
        if flag {
            assert!(err.is_none(), "{err:?}");
            let mut hit = counter.get();
            assert_eq!((1 as f64), hit);
        }
        tk.must_exec(r#"set @a=3; execute stmt_delete using @a;"#);
        if flag {
            assert!(err.is_none(), "{err:?}");
            let mut hit = counter.get();
            assert_eq!((2 as f64), hit);
        }
        let mut result =
            tk.must_query_args("select id, c1 from prepare_test where id = ?", &[go_arg(1)]);
        result.check_empty();
        result = tk.must_query_args("select id, c1 from prepare_test where id = ?", &[go_arg(2)]);
        result.check_empty();
        result = tk.must_query_args("select id, c1 from prepare_test where id = ?", &[go_arg(3)]);
        result.check_empty();
    }
}

/// Go `TestPreparedInsert`.
#[test]
fn test_prepared_insert() {
    tidb_metrics::server::RESETTABLE_PLAN_CACHE_COUNTER_FORT_TEST
        .store(true, std::sync::atomic::Ordering::Relaxed);
    tidb_metrics::server::PLAN_CACHE_COUNTER.reset();
    let counter = tidb_metrics::server::PLAN_CACHE_COUNTER.with_label_values(&["prepare"]);
    let mut flags = [false, true];
    for flag in flags.clone() {
        let mut tk = TestKit::new();
        tk.must_exec(&format!(
            r#"set @@tidb_enable_prepared_plan_cache={}"#,
            go_v(&flag)
        ));
        let mut err: Option<tidb_terror::MysqlError> = None;
        tk.must_exec("use test");
        tk.must_exec("drop table if exists prepare_test");
        tk.must_exec("create table prepare_test (id int PRIMARY KEY, c1 int)");
        tk.must_exec(r#"prepare stmt_insert from 'insert into prepare_test values (?, ?)'"#);
        tk.must_exec(r#"set @a=1,@b=1; execute stmt_insert using @a, @b;"#);
        if flag {
            assert!(err.is_none(), "{err:?}");
            let mut hit = counter.get();
            assert_eq!((0 as f64), hit);
        }
        tk.must_exec(r#"set @a=2,@b=2; execute stmt_insert using @a, @b;"#);
        if flag {
            assert!(err.is_none(), "{err:?}");
            let mut hit = counter.get();
            assert_eq!((1 as f64), hit);
        }
        tk.must_exec(r#"set @a=3,@b=3; execute stmt_insert using @a, @b;"#);
        if flag {
            assert!(err.is_none(), "{err:?}");
            let mut hit = counter.get();
            assert_eq!((2 as f64), hit);
        }
        let mut result =
            tk.must_query_args("select id, c1 from prepare_test where id = ?", &[go_arg(1)]);
        result.check(["1 1"]);
        result = tk.must_query_args("select id, c1 from prepare_test where id = ?", &[go_arg(2)]);
        result.check(["2 2"]);
        result = tk.must_query_args("select id, c1 from prepare_test where id = ?", &[go_arg(3)]);
        result.check(["3 3"]);
        tk.must_exec(r#"prepare stmt_insert_select from 'insert into prepare_test (id, c1) select id + 100, c1 + 100 from prepare_test where id = ?'"#);
        tk.must_exec(r#"set @a=1; execute stmt_insert_select using @a;"#);
        if flag {
            assert!(err.is_none(), "{err:?}");
            let mut hit = counter.get();
            assert_eq!((2 as f64), hit);
        }
        tk.must_exec(r#"set @a=2; execute stmt_insert_select using @a;"#);
        if flag {
            assert!(err.is_none(), "{err:?}");
            let mut hit = counter.get();
            assert_eq!((3 as f64), hit);
        }
        tk.must_exec(r#"set @a=3; execute stmt_insert_select using @a;"#);
        if flag {
            assert!(err.is_none(), "{err:?}");
            let mut hit = counter.get();
            assert_eq!((4 as f64), hit);
        }
        result = tk.must_query_args(
            "select id, c1 from prepare_test where id = ?",
            &[go_arg(101)],
        );
        result.check(["101 101"]);
        result = tk.must_query_args(
            "select id, c1 from prepare_test where id = ?",
            &[go_arg(102)],
        );
        result.check(["102 102"]);
        result = tk.must_query_args(
            "select id, c1 from prepare_test where id = ?",
            &[go_arg(103)],
        );
        result.check(["103 103"]);
    }
}

/// Go `TestPreparedLimitOffset`.
#[test]
fn test_prepared_limit_offset() {
    let mut flags = [false, true];
    for flag in flags.clone() {
        let mut tk = TestKit::new();
        tk.must_exec(&format!(
            r#"set @@tidb_enable_prepared_plan_cache={}"#,
            go_v(&flag)
        ));
        tk.must_exec("use test");
        tk.must_exec("drop table if exists prepare_test");
        tk.must_exec("create table prepare_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1)");
        tk.must_exec("insert prepare_test (c1) values (1),(2),(NULL)");
        tk.must_exec(r#"prepare stmt_test_1 from 'select id from prepare_test limit ? offset ?'; set @a = 1, @b=1;"#);
        let mut r = tk.must_query(r#"execute stmt_test_1 using @a, @b;"#);
        r.check(["2"]);
        tk.must_exec(r#"set @a=1.1"#);
        let (_, mut err) = tk.exec_rs(r#"execute stmt_test_1 using @a, @b;"#);
        assert_eq!(
            err.as_ref().map(|e| e.code),
            Some(tidb_errno::errcode::ErrWrongArguments),
            "{err:?}"
        );
        tk.must_exec(r#"set @c="-1""#);
        (_, err) = tk.exec_rs("execute stmt_test_1 using @c, @c");
        assert_eq!(
            err.as_ref().map(|e| e.code),
            Some(tidb_errno::errcode::ErrWrongArguments),
            "{err:?}"
        );
        let (mut stmt_id, _, _, mut err) = tk.prepare_stmt("select id from prepare_test limit ?");
        assert!(err.is_none(), "{err:?}");
        let (mut rs, mut err) = tk.execute_prepared_stmt(stmt_id, &[go_arg(1)]);
        assert!(err.is_none(), "{err:?}");
    }
}

/// Go `TestPreparedUpdate`.
#[test]
fn test_prepared_update() {
    tidb_metrics::server::RESETTABLE_PLAN_CACHE_COUNTER_FORT_TEST
        .store(true, std::sync::atomic::Ordering::Relaxed);
    tidb_metrics::server::PLAN_CACHE_COUNTER.reset();
    let counter = tidb_metrics::server::PLAN_CACHE_COUNTER.with_label_values(&["prepare"]);
    let mut flags = [false, true];
    for flag in flags.clone() {
        let mut tk = TestKit::new();
        tk.must_exec(r#"set @@tidb_enable_non_prepared_plan_cache=0"#);
        tk.must_exec(&format!(
            r#"set @@tidb_enable_prepared_plan_cache={}"#,
            go_v(&flag)
        ));
        let mut err: Option<tidb_terror::MysqlError> = None;
        tk.must_exec("use test");
        tk.must_exec("drop table if exists prepare_test");
        tk.must_exec("create table prepare_test (id int PRIMARY KEY, c1 int)");
        tk.must_exec(r#"insert into prepare_test values (1, 1)"#);
        tk.must_exec(r#"insert into prepare_test values (2, 2)"#);
        tk.must_exec(r#"insert into prepare_test values (3, 3)"#);
        tk.must_exec(
            r#"prepare stmt_update from 'update prepare_test set c1 = c1 + ? where id = ?'"#,
        );
        tk.must_exec(r#"set @a=1,@b=100; execute stmt_update using @b,@a;"#);
        if flag {
            assert!(err.is_none(), "{err:?}");
            let mut hit = counter.get();
            assert_eq!((0 as f64), hit);
        }
        tk.must_exec(r#"set @a=2,@b=200; execute stmt_update using @b,@a;"#);
        if flag {
            assert!(err.is_none(), "{err:?}");
            let mut hit = counter.get();
            assert_eq!((1 as f64), hit);
        }
        tk.must_exec(r#"set @a=3,@b=300; execute stmt_update using @b,@a;"#);
        if flag {
            assert!(err.is_none(), "{err:?}");
            let mut hit = counter.get();
            assert_eq!((2 as f64), hit);
        }
        let mut result =
            tk.must_query_args("select id, c1 from prepare_test where id = ?", &[go_arg(1)]);
        result.check(["1 101"]);
        result = tk.must_query_args("select id, c1 from prepare_test where id = ?", &[go_arg(2)]);
        result.check(["2 202"]);
        result = tk.must_query_args("select id, c1 from prepare_test where id = ?", &[go_arg(3)]);
        result.check(["3 303"]);
    }
}

/// Go `TestPrepared`.
#[test]
fn test_prepared() {
    let mut flags = [false, true];
    for flag in flags.clone() {
        let mut tk = TestKit::new();
        tk.must_exec(&format!(
            r#"set @@tidb_enable_prepared_plan_cache={}"#,
            go_v(&flag)
        ));
        let mut err: Option<tidb_terror::MysqlError> = None;
        tk.must_exec("use test");
        tk.must_exec("drop table if exists prepare_test");
        tk.must_exec("create table prepare_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1)");
        tk.must_exec("insert prepare_test (c1) values (1),(2),(NULL)");
        tk.must_exec(r#"prepare stmt_test_1 from 'select id from prepare_test where id > ?';"#);
        tk.must_exec(r#"set @a = 1;"#);
        tk.must_exec(r#"execute stmt_test_1 using @a;"#);
        tk.must_exec(r#"prepare stmt_test_2 from 'select 1'"#);
        // Prepare multiple statement is not allowed.
        tk.must_get_err_code(r#"prepare stmt_test_3 from 'select id from prepare_test where id > ?;select id from prepare_test where id > ?;'"#, tidb_errno::errcode::ErrPrepareMulti);
        // The variable count does not match.
        tk.must_exec(
            r#"prepare stmt_test_4 from 'select id from prepare_test where id > ? and id < ?';"#,
        );
        tk.must_exec(r#"set @a = 1;"#);
        tk.must_get_err_code(
            r#"execute stmt_test_4 using @a;"#,
            tidb_errno::errcode::ErrWrongParamCount,
        );
        // Prepare and deallocate prepared statement immediately.
        tk.must_exec(r#"prepare stmt_test_5 from 'select id from prepare_test where id > ?';"#);
        tk.must_exec(r#"deallocate prepare stmt_test_5;"#);
        // Statement not found.
        err = tk.exec("deallocate prepare stmt_test_5").err();
        assert_eq!(
            err.as_ref().map(|e| e.code),
            Some(tidb_errno::errcode::ErrPreparedStmtNotFound),
            "{err:?}"
        );
        // incorrect SQLs in prepare. issue #3738, SQL in prepare stmt is parsed in DoPrepare.
        tk.must_get_err_msg(r#"prepare p from "delete from t where a = 7 or 1=1/*' and b = 'p'";"#, r#"[parser:1064]You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use near '/*' and b = 'p'' at line 1"#);
        // The `stmt_test5` should not be found.
        err = tk
            .exec(r#"set @a = 1; execute stmt_test_5 using @a;"#)
            .err();
        assert_eq!(
            err.as_ref().map(|e| e.code),
            Some(tidb_errno::errcode::ErrPreparedStmtNotFound),
            "{err:?}"
        );
        // Use parameter marker with argument will run prepared statement.
        let mut result = tk.must_query_args(
            "select distinct c1, c2 from prepare_test where c1 = ?",
            &[go_arg(1)],
        );
        result.check(["1 <nil>"]);
        // Call Session PrepareStmt directly to get stmtID.
        let mut query = "select c1, c2 from prepare_test where c1 = ?".to_owned();
        let (mut stmt_id, _, _, mut err) = tk.prepare_stmt(&query);
        assert!(err.is_none(), "{err:?}");
        let (mut rs, mut err) = tk.execute_prepared_stmt(stmt_id, &[go_arg(1)]);
        assert!(err.is_none(), "{err:?}");
        rs.clone().check(["1 <nil>"]);
        tk.must_exec("delete from prepare_test");
        query = "select c1 from prepare_test where c1 = (select c1 from prepare_test where c1 = ?)"
            .to_owned();
        (stmt_id, _, _, err) = tk.prepare_stmt(&query);
        assert!(err.is_none(), "{err:?}");
        let mut tk1 = tk.new_tk();
        tk1.must_exec(r#"set @@tidb_enable_prepared_plan_cache=true"#);
        tk1.must_exec("use test");
        tk1.must_exec("insert prepare_test (c1) values (3)");
        (rs, err) = tk.execute_prepared_stmt(stmt_id, &[go_arg(3)]);
        assert!(err.is_none(), "{err:?}");
        rs.clone().check(["3"]);
        tk.must_exec("delete from prepare_test");
        query = "select c1 from prepare_test where c1 = (select c1 from prepare_test where c1 = ?)"
            .to_owned();
        (stmt_id, _, _, err) = tk.prepare_stmt(&query);
        assert!(err.is_none(), "{err:?}");
        (rs, err) = tk.execute_prepared_stmt(stmt_id, &[go_arg(3)]);
        assert!(err.is_none(), "{err:?}");
        tk1.must_exec("insert prepare_test (c1) values (3)");
        (rs, err) = tk.execute_prepared_stmt(stmt_id, &[go_arg(3)]);
        assert!(err.is_none(), "{err:?}");
        rs.clone().check(["3"]);
        tk.must_exec("delete from prepare_test");
        query =
            "select c1 from prepare_test where c1 in (select c1 from prepare_test where c1 = ?)"
                .to_owned();
        (stmt_id, _, _, err) = tk.prepare_stmt(&query);
        assert!(err.is_none(), "{err:?}");
        (rs, err) = tk.execute_prepared_stmt(stmt_id, &[go_arg(3)]);
        assert!(err.is_none(), "{err:?}");
        tk1.must_exec("insert prepare_test (c1) values (3)");
        (rs, err) = tk.execute_prepared_stmt(stmt_id, &[go_arg(3)]);
        assert!(err.is_none(), "{err:?}");
        rs.clone().check(["3"]);
        tk.must_exec("begin");
        tk.must_exec("insert prepare_test (c1) values (4)");
        query = "select c1, c2 from prepare_test where c1 = ?".to_owned();
        (stmt_id, _, _, err) = tk.prepare_stmt(&query);
        assert!(err.is_none(), "{err:?}");
        tk.must_exec("rollback");
        (rs, err) = tk.execute_prepared_stmt(stmt_id, &[go_arg(4)]);
        assert!(err.is_none(), "{err:?}");
        rs.clone().check_empty();
        // Go compiles an `ast.ExecuteStmt` over this statement through
        // `executor.Compiler`, rebuilds its plan in a fresh transaction context
        // and reads one chunk; the session's EXECUTE runs that same path.
        (rs, err) = tk.execute_prepared_stmt(stmt_id, &[go_arg(1)]);
        assert!(err.is_none(), "{err:?}");
        // Make schema change.
        tk.must_exec("drop table if exists prepare2");
        tk.must_exec("create table prepare2 (a int)");
        // Should success as the changed schema do not affect the prepared statement.
        (rs, err) = tk.execute_prepared_stmt(stmt_id, &[go_arg(1)]);
        assert!(err.is_none(), "{err:?}");
        if !rs.is_nil() {}
        // Drop a column so the prepared statement become invalid.
        query = "select c1, c2 from prepare_test where c1 = ?".to_owned();
        (stmt_id, _, _, err) = tk.prepare_stmt(&query);
        assert!(err.is_none(), "{err:?}");
        tk.must_exec("alter table prepare_test drop column c2");
        (_, err) = tk.execute_prepared_stmt(stmt_id, &[go_arg(1)]);
        assert_eq!(
            err.as_ref().map(|e| e.code),
            Some(tidb_errno::errcode::ErrBadField),
            "{err:?}"
        );
        tk.must_exec("drop table prepare_test");
        (_, err) = tk.execute_prepared_stmt(stmt_id, &[go_arg(1)]);
        assert_eq!(
            err.as_ref().map(|e| e.code),
            Some(tidb_errno::errcode::ErrSchemaChanged),
            "{err:?}"
        );
        // issue 3381
        tk.must_exec("drop table if exists prepare3");
        tk.must_exec("create table prepare3 (a decimal(1))");
        tk.must_exec("prepare stmt from 'insert into prepare3 value(123)'");
        tk.must_exec_to_err("execute stmt");
        let (_, _, mut fields, mut err) = tk.prepare_stmt("select a from prepare3");
        assert!(err.is_none(), "{err:?}");
        assert_eq!("test", fields[0].schema.to_lowercase());
        assert_eq!("prepare3", fields[0].table.to_lowercase());
        assert_eq!("a", fields[0].name.to_lowercase());
        (_, _, fields, err) = tk.prepare_stmt("select a from prepare3 where ?");
        assert!(err.is_none(), "{err:?}");
        assert_eq!("test", fields[0].schema.to_lowercase());
        assert_eq!("prepare3", fields[0].table.to_lowercase());
        assert_eq!("a", fields[0].name.to_lowercase());
        (_, _, fields, err) = tk.prepare_stmt("select (1,1) in (select 1,1)");
        assert!(err.is_none(), "{err:?}");
        assert_eq!("", fields[0].schema.to_lowercase());
        assert_eq!("", fields[0].table.to_lowercase());
        assert_eq!("(1,1) in (select 1,1)", fields[0].name.to_lowercase());
        (_, _, fields, err) = tk.prepare_stmt(&format!(
            "{}{}",
            "select a from prepare3 where a = (", "select a from prepare2 where a = ?)"
        ));
        assert!(err.is_none(), "{err:?}");
        assert_eq!("test", fields[0].schema.to_lowercase());
        assert_eq!("prepare3", fields[0].table.to_lowercase());
        assert_eq!("a", fields[0].name.to_lowercase());
        (_, _, fields, err) = tk.prepare_stmt("select * from prepare3 as t1 join prepare3 as t2");
        assert!(err.is_none(), "{err:?}");
        assert_eq!("test", fields[0].schema.to_lowercase());
        assert_eq!("t1", fields[0].table.to_lowercase());
        assert_eq!("a", fields[0].name.to_lowercase());
        assert_eq!("test", fields[1].schema.to_lowercase());
        assert_eq!("t2", fields[1].table.to_lowercase());
        assert_eq!("a", fields[1].name.to_lowercase());
        (_, _, fields, err) = tk.prepare_stmt("update prepare3 set a = ?");
        assert!(err.is_none(), "{err:?}");
        assert_eq!(fields.len(), 0 as usize);
        // issue 8074
        tk.must_exec("drop table if exists prepare1;");
        tk.must_exec("create table prepare1 (a decimal(1))");
        tk.must_exec("insert into prepare1 values(1);");
        tk.must_get_err_msg("prepare stmt FROM @sql1", "[parser:1064]You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 1 column 4 near \"NULL\" ");
        tk.must_exec("SET @sql = 'update prepare1 set a=5 where a=?';");
        tk.must_exec("prepare stmt FROM @sql");
        tk.must_exec("set @var=1;");
        tk.must_exec("execute stmt using @var");
        tk.must_query("select a from prepare1;").check(["5"]);
        // issue 19371
        tk.must_exec("SET @sql = 'update prepare1 set a=a+1';");
        tk.must_exec("prepare stmt FROM @SQL");
        tk.must_exec("execute stmt");
        tk.must_query("select a from prepare1;").check(["6"]);
        tk.must_exec("prepare stmt FROM @Sql");
        tk.must_exec("execute stmt");
        tk.must_query("select a from prepare1;").check(["7"]);
        // Coverage.
        //nolint:constructor
        // Go then drives a zero `ExecuteExec` through Next/Close for coverage;
        // the executor has no TestKit-visible behavior.
        // issue 8065
        (stmt_id, _, _, err) = tk.prepare_stmt("select ? from dual");
        assert!(err.is_none(), "{err:?}");
        (_, err) = tk.execute_prepared_stmt(stmt_id, &[go_arg(1)]);
        assert!(err.is_none(), "{err:?}");
        (stmt_id, _, _, err) = tk.prepare_stmt("update prepare1 set a = ? where a = ?");
        assert!(err.is_none(), "{err:?}");
        (_, err) = tk.execute_prepared_stmt(stmt_id, &[go_arg(1), go_arg(1)]);
        assert!(err.is_none(), "{err:?}");
    }
}
