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
// See the License for the specific language governing permissions and
// limitations under the License.

//! `INSERT`/`UPDATE`/`DELETE` grammar tests.

use super::*;

/// Complete name-token boundary from
/// `admin_query_parser.go::parseCallStmt`: exactly two name components,
/// permissive first identifier-like tokens, and an intentionally unchecked
/// token after the qualifier dot.
#[test]
fn call_name_grammar_matches_go_hand_parser() {
    for (sql, expected) in [
        ("call p", "CALL `p`()"),
        ("call s.p", "CALL `s`.`p`()"),
        ("call 'x'()", "CALL `x`()"),
        ("call @x()", "CALL `x`()"),
        ("call @@x()", "CALL `@@x`()"),
        ("call s.select()", "CALL `s`.`select`()"),
        ("call s.1()", "CALL `s`.`1`()"),
        ("call s.@@x()", "CALL `s`.`@@x`()"),
        ("call s.-()", "CALL `s`.`-`()"),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }
    for sql in ["call select()", "call a.b.c()", "call s.()"] {
        assert!(parse(sql).is_err(), "{sql}");
    }
}

/// `pkg/parser/ast/dml_test.go::TestDMLVisitorCover`.
#[test]
fn test_dml_visitor_cover() {
    for sql in [
        "DELETE FROM t WHERE a = 1 ORDER BY b LIMIT 3",
        "SHOW TABLES LIKE 't%'",
        "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE t (a)",
        "IMPORT INTO t FROM '/tmp/t.csv'",
        "INSERT INTO t SELECT * FROM s WHERE a = 1",
        "SELECT a FROM t WHERE b = 1 GROUP BY a HAVING COUNT(*) > 0 ORDER BY a LIMIT 1",
        "SELECT 1 UNION SELECT 2",
        "UPDATE t SET a = a + 1 WHERE b = 2",
    ] {
        assert_full_visitor_traversal(sql);
    }
}

#[test]
fn dml_statements_use_one_outer_envelope() {
    for (sql, expected) in [
        ("insert into t values (1)", "INSERT"),
        ("update t set a = 1", "UPDATE"),
        ("delete from t", "DELETE"),
        ("load data infile '/tmp/t.csv' into table t", "LOAD DATA"),
    ] {
        let stmt = parse(sql).unwrap();
        let Stmt::Dml(dml) = stmt else {
            panic!("expected DML envelope for {sql}")
        };
        match (expected, dml.as_ref()) {
            ("INSERT", tidb_ast::DmlStmt::Insert(_))
            | ("UPDATE", tidb_ast::DmlStmt::Update(_))
            | ("DELETE", tidb_ast::DmlStmt::Delete(_))
            | ("LOAD DATA", tidb_ast::DmlStmt::LoadData(_)) => {}
            _ => panic!("wrong DML payload for {sql}"),
        }
    }

    let stmt = parse("insert into t select 1 union select 2").unwrap();
    let Stmt::Dml(dml) = stmt else {
        panic!("expected DML envelope")
    };
    let tidb_ast::DmlStmt::Insert(insert) = dml.as_ref() else {
        panic!("expected INSERT")
    };
    assert!(matches!(
        insert.source.as_deref(),
        Some(tidb_ast::QueryStmt::SetOpr(_))
    ));

    let with_source = "insert into t with cte(a) as (select 1) select a from cte";
    assert_eq!(
        r(with_source),
        "INSERT INTO `t` WITH `cte` (`a`) AS (SELECT 1) SELECT `a` FROM `cte`"
    );
    let stmt = parse(with_source).unwrap();
    let Stmt::Dml(dml) = stmt else {
        panic!("expected DML envelope")
    };
    let tidb_ast::DmlStmt::Insert(insert) = dml.as_ref() else {
        panic!("expected INSERT")
    };
    let Some(tidb_ast::QueryStmt::Select(source)) = insert.source.as_deref() else {
        panic!("expected a SELECT source")
    };
    assert!(
        source.with.is_some(),
        "WITH belongs to INSERT's query source"
    );
}

/// Go's `parseOptHints` is shared by `parseInsertStmt`, `parseUpdateStmt`,
/// and `parseDeleteStmt`: `IGNORE_PLAN_CACHE` is a nullary hint with optional
/// parentheses in every DML-header position (source: `pkg/parser/
/// hintparser.go:252-262`).  Keep all three owners covered so a SELECT-only
/// hint-model change cannot regress DML headers again.
#[test]
fn dml_header_ignore_plan_cache_hint() {
    for (sql, expected) in [
        (
            "insert /*+ ignore_plan_cache() */ into t values (1)",
            "INSERT /*+ IGNORE_PLAN_CACHE()*/ INTO `t` VALUES (1)",
        ),
        (
            "update /*+ ignore_plan_cache */ t set a=1",
            "UPDATE /*+ IGNORE_PLAN_CACHE()*/ `t` SET `a`=1",
        ),
        (
            "delete /*+ ignore_plan_cache() */ from t where a=1",
            "DELETE /*+ IGNORE_PLAN_CACHE()*/ FROM `t` WHERE `a`=1",
        ),
    ] {
        assert_eq!(r(sql), expected, "restore {sql}");
    }
}

#[test]
fn insert_parenthesized_query_source_preserves_insert_owned_braces() {
    // Go source: `pkg/parser/dml_parser.go:110-155`. The parentheses belong
    // to INSERT's result-set production, not to the query AST itself, so the
    // typed INSERT node owns their restore bit.
    for (sql, expected) in [
        (
            "insert ignore into t (select a from s)",
            "INSERT IGNORE INTO `t` (SELECT `a` FROM `s`)",
        ),
        (
            "replace into t (a) (select a from s)",
            "REPLACE INTO `t` (`a`) (SELECT `a` FROM `s`)",
        ),
        (
            "insert into t (with cte as (select a from s) select a from cte)",
            "INSERT INTO `t` (WITH `cte` AS (SELECT `a` FROM `s`) SELECT `a` FROM `cte`)",
        ),
    ] {
        assert_eq!(r(sql), expected, "restore {sql}");
    }

    let statement = parse("insert into t (select a from s)").unwrap();
    let Stmt::Dml(dml) = statement else {
        panic!("expected DML envelope");
    };
    let tidb_ast::DmlStmt::Insert(insert) = dml.as_ref() else {
        panic!("expected INSERT");
    };
    assert!(insert.source.is_some());
    assert!(insert.source_parenthesized);
}

#[test]
fn import_into_direct_go_grammar_translation() {
    for (sql, expected) in [
        (
            "import into t from '/file.csv'",
            "IMPORT INTO `t` FROM '/file.csv'",
        ),
        (
            "import into t () from '/file.csv'",
            "IMPORT INTO `t` FROM '/file.csv'",
        ),
        (
            "import into t (a,) from '/file.csv'",
            "IMPORT INTO `t` (`a`) FROM '/file.csv'",
        ),
        (
            "import into t (a,@1) set b=@1+100 from '/file.csv' format 'sql file' with detached, thread:=1",
            "IMPORT INTO `t` (`a`,@`1`) SET `b`=@`1`+100 FROM '/file.csv' FORMAT 'sql file' WITH detached, thread=1",
        ),
        (
            "import into t from select * from source with thread=1",
            "IMPORT INTO `t` FROM SELECT * FROM `source` WITH thread=1",
        ),
        (
            "import into t from (select * from source) with fields_terminated_by=_latin1'\\t'",
            "IMPORT INTO `t` FROM (SELECT * FROM `source`) WITH fields_terminated_by=_LATIN1'\t'",
        ),
    ] {
        assert_eq!(r(sql), expected, "restore {sql}");
    }

    let stmt = parse("import into t from '/file.csv'").expect("parse IMPORT INTO");
    let Stmt::Dml(dml) = stmt else {
        panic!("IMPORT INTO must use the DML outer envelope")
    };
    assert!(matches!(dml.as_ref(), tidb_ast::DmlStmt::ImportInto(_)));

    // Read directly from Go's `parseImportIntoStmt`: source-query imports
    // cannot carry a user-variable mapping or a SET assignment.
    for invalid in [
        "import into t (@1) from select * from source",
        "import into t set a=1 from select * from source",
        "import into t from (with c as (select 1) select * from c)",
        "import into t from '/file.csv' with thread=@v",
        "import into t from '/file.csv' with thread=1+1",
    ] {
        assert!(parse(invalid).is_err(), "must reject {invalid}");
    }
}

#[test]
fn batch_dml_preserves_the_go_nontransactional_wrapper() {
    for (sql, expected) in [
        (
            "batch on app.t.id limit 10 delete from t where id = 1",
            "BATCH ON `app`.`t`.`id` LIMIT 10 DELETE FROM `t` WHERE `id`=1",
        ),
        (
            "batch limit 9 dry run update t set id = id + 1",
            "BATCH LIMIT 9 DRY RUN UPDATE `t` SET `id`=`id`+1",
        ),
        (
            "batch on id limit 1 dry run query replace into target select * from source",
            "BATCH ON `id` LIMIT 1 DRY RUN QUERY REPLACE INTO `target` SELECT * FROM `source`",
        ),
    ] {
        assert_eq!(r(sql), expected, "restore {sql}");
    }

    let stmt = parse("batch limit 1 insert into t values (1)").expect("parse BATCH");
    let Stmt::Dml(dml) = stmt else {
        panic!("BATCH must use the DML outer envelope")
    };
    assert!(matches!(dml.as_ref(), tidb_ast::DmlStmt::Batch(_)));

    for invalid in [
        "batch limit 1 select 1",
        "batch limit 1 batch limit 1 delete from t",
        "batch on id delete from t",
        "batch limit x delete from t",
    ] {
        assert!(parse(invalid).is_err(), "must reject {invalid}");
    }
}

/// Complete table from Go `pkg/parser/parser_test.go`'s
/// `TestNonTransactionalDML`.
#[test]
fn nontransactional_dml_source_table() {
    let operations = [
        (
            "delete from t where c = 10",
            "DELETE FROM `t` WHERE `c`=10",
        ),
        ("update t set c = 10", "UPDATE `t` SET `c`=10"),
        (
            "insert into t1 select * from t2 where c = 10",
            "INSERT INTO `t1` SELECT * FROM `t2` WHERE `c`=10",
        ),
        (
            "insert into t1 select * from t2 where c = 10 on duplicate key update t1.val = t2.val",
            "INSERT INTO `t1` SELECT * FROM `t2` WHERE `c`=10 ON DUPLICATE KEY UPDATE `t1`.`val`=`t2`.`val`",
        ),
    ];
    let modes = [
        ("", ""),
        ("dry run ", "DRY RUN "),
        ("dry run query ", "DRY RUN QUERY "),
    ];
    for (operation, restored_operation) in operations {
        for (on_column, restored_on_column) in [("on c ", "ON `c` "), ("", "")] {
            for (mode, restored_mode) in modes {
                let sql = format!("batch {on_column}limit 10 {mode}{operation}");
                let expected = format!(
                    "BATCH {restored_on_column}LIMIT 10 {restored_mode}{restored_operation}"
                );
                assert_eq!(r(&sql), expected, "source SQL: {sql}");
            }
        }
    }
}

#[test]
fn dml_statements() {
    assert_eq!(
        r("insert into t values (1, 2, 3)"),
        "INSERT INTO `t` VALUES (1,2,3)"
    );
    assert_eq!(
        r("insert into t (a, b) values (1, 2), (3, 4)"),
        "INSERT INTO `t` (`a`,`b`) VALUES (1,2),(3,4)"
    );
    assert_eq!(
        r("update t set a = 1, b = 2 where c = 3"),
        "UPDATE `t` SET `a`=1, `b`=2 WHERE `c`=3"
    );
    // Multi-table UPDATE — a join source written directly after UPDATE,
    // with table-qualified SET targets (task #149).
    assert_eq!(
        r("update t1, t3 set t1.a = 40 where t3.a = 2"),
        "UPDATE (`t1`) JOIN `t3` SET `t1`.`a`=40 WHERE `t3`.`a`=2"
    );
    assert_eq!(
        r("update t1 join t2 on t1.a = t2.a set t1.b = t2.c"),
        "UPDATE `t1` JOIN `t2` ON `t1`.`a`=`t2`.`a` SET `t1`.`b`=`t2`.`c`"
    );
    assert_eq!(
        r("delete from t where a = 1"),
        "DELETE FROM `t` WHERE `a`=1"
    );
    // `IGNORE` modifier on UPDATE/DELETE (task #144).
    assert_eq!(
        r("update ignore t set name = 'x' where name = 'y'"),
        "UPDATE IGNORE `t` SET `name`=_UTF8MB4'x' WHERE `name`=_UTF8MB4'y'"
    );
    assert_eq!(
        r("delete ignore from t where a = 1"),
        "DELETE IGNORE FROM `t` WHERE `a`=1"
    );
    // Multi-table DELETE, both spellings (task #147). The comma-join FROM
    // restores with the left side parenthesized, matching real TiDB.
    assert_eq!(
        r("delete t1, t2 from t1, t2 where t1.a = t2.a"),
        "DELETE `t1`,`t2` FROM (`t1`) JOIN `t2` WHERE `t1`.`a`=`t2`.`a`"
    );
    assert_eq!(
        r("delete from t1, t2 using t1, t2 where t1.a = t2.a"),
        "DELETE FROM `t1`,`t2` USING (`t1`) JOIN `t2` WHERE `t1`.`a`=`t2`.`a`"
    );
    assert_eq!(
        r("delete t1 from t1 join t2 on t1.a = t2.a"),
        "DELETE `t1` FROM `t1` JOIN `t2` ON `t1`.`a`=`t2`.`a`"
    );
    assert_eq!(
        r("delete ignore t1, t2 from t1, t2 where t1.a = t2.a"),
        "DELETE IGNORE `t1`,`t2` FROM (`t1`) JOIN `t2` WHERE `t1`.`a`=`t2`.`a`"
    );
    // `VALUE` is an accepted synonym for `VALUES`, normalized on restore
    // (a high-frequency form in real TiDB tests — task #137).
    assert_eq!(r("insert into t value (3)"), "INSERT INTO `t` VALUES (3)");
    assert_eq!(
        r("insert into t2 value(1, 1), (2, 2)"),
        "INSERT INTO `t2` VALUES (1,1),(2,2)"
    );
    assert_eq!(
        r("insert into t (a,b) value(3,6)"),
        "INSERT INTO `t` (`a`,`b`) VALUES (3,6)"
    );
}

#[test]
fn single_table_update_accepts_bare_default_assignments() {
    assert_eq!(
        r("update t set a=default, b=1 where id=2"),
        "UPDATE `t` SET `a`=DEFAULT, `b`=1 WHERE `id`=2"
    );
    // `DEFAULT(column)` remains the general-expression form.
    assert_eq!(
        r("update t set a=default(b)"),
        "UPDATE `t` SET `a`=DEFAULT(`b`)"
    );
    assert_eq!(
        r("update t1, t2 set t1.a=default"),
        "UPDATE (`t1`) JOIN `t2` SET `t1`.`a`=DEFAULT"
    );
}

/// `TRUNCATE [TABLE] name` — the `TABLE` keyword is optional and always
/// restored (task #137, a common statement surfaced by the real-TiDB
/// corpus).
#[test]
fn test_ddl_truncate_table_stmt_restore() {
    assert_eq!(r("truncate table t"), "TRUNCATE TABLE `t`");
    assert_eq!(r("truncate t"), "TRUNCATE TABLE `t`");
    assert_eq!(r("truncate table a.t1"), "TRUNCATE TABLE `a`.`t1`");
}

/// `REPLACE` reuses the `INSERT ... VALUES` grammar (`INTO` optional and
/// always restored, `VALUE` synonym, column list) but restores as
/// `REPLACE INTO` (task #139, from the real-TiDB corpus). All
/// godump-verified.
#[test]
fn replace_statement() {
    assert_eq!(
        r("replace into t values (1, 2)"),
        "REPLACE INTO `t` VALUES (1,2)"
    );
    assert_eq!(r("replace t values (1)"), "REPLACE INTO `t` VALUES (1)");
    assert_eq!(
        r("replace into t (a, b) values (1, 2)"),
        "REPLACE INTO `t` (`a`,`b`) VALUES (1,2)"
    );
    assert_eq!(r("replace into t value (1)"), "REPLACE INTO `t` VALUES (1)");
    assert_eq!(
        r("replace into t values (1),(2)"),
        "REPLACE INTO `t` VALUES (1),(2)"
    );
}

/// `INSERT ... SELECT` / `REPLACE ... SELECT`: a query source replaces the
/// `VALUES` list, restored directly after the table/column list with no
/// `VALUES` keyword (task #140, from the real-TiDB corpus). All
/// godump-verified.
#[test]
fn insert_select() {
    assert_eq!(
        r("insert into t select a+8, a+8 from t"),
        "INSERT INTO `t` SELECT `a`+8,`a`+8 FROM `t`"
    );
    assert_eq!(
        r("insert into t (a, b) select x, y from s"),
        "INSERT INTO `t` (`a`,`b`) SELECT `x`,`y` FROM `s`"
    );
    assert_eq!(
        r("replace into t select * from t1 limit 1"),
        "REPLACE INTO `t` SELECT * FROM `t1` LIMIT 1"
    );
    // A UNION-bodied source round-trips as its own set operation.
    assert_eq!(
        r("insert into t select 1 union select 2"),
        "INSERT INTO `t` SELECT 1 UNION SELECT 2"
    );
}

/// `INSERT/REPLACE ... SET col=val` assignment form and bare `DEFAULT`
/// values (task #141, from the real-TiDB corpus). All godump-verified.
#[test]
fn insert_set_form_and_default() {
    assert_eq!(
        r("insert into t1 set b=default, a=1"),
        "INSERT INTO `t1` SET `b`=DEFAULT,`a`=1"
    );
    // SET targets use the same qualified ColumnName shape as Go's AST.
    assert_eq!(
        r("insert into t1 set t1.b=default, db.t1.a=1"),
        "INSERT INTO `t1` SET `t1`.`b`=DEFAULT,`db`.`t1`.`a`=1"
    );
    assert_eq!(
        r("insert into t2 set a=1,b=1 on duplicate key update a=1,b=1"),
        "INSERT INTO `t2` SET `a`=1,`b`=1 ON DUPLICATE KEY UPDATE `a`=1,`b`=1"
    );
    // Go's `parseAssignment` uses `parseExprOrDefault`, including in ON
    // DUPLICATE KEY UPDATE assignments.
    assert_eq!(
        r("insert into t2 values (1, 2) on duplicate key update b=default"),
        "INSERT INTO `t2` VALUES (1,2) ON DUPLICATE KEY UPDATE `b`=DEFAULT"
    );
    assert_eq!(
        r("replace into t set c1=1, c2=4"),
        "REPLACE INTO `t` SET `c1`=1,`c2`=4"
    );
    assert_eq!(
        r("replace replace_test set c1 = 6"),
        "REPLACE INTO `replace_test` SET `c1`=6"
    );
    // Bare DEFAULT as a VALUES element (distinct from DEFAULT(col)).
    assert_eq!(
        r("insert into t values (1, default)"),
        "INSERT INTO `t` VALUES (1,DEFAULT)"
    );
    assert_eq!(
        r("insert into t (id) values(-1),(default)"),
        "INSERT INTO `t` (`id`) VALUES (-1),(DEFAULT)"
    );
    // DEFAULT(col) still parses as the column-default function, unaffected.
    assert_eq!(
        r("insert into t values (default(a))"),
        "INSERT INTO `t` VALUES (DEFAULT(`a`))"
    );
}

/// A non-reserved keyword (`year`, `value`) is accepted as a column name in
/// an `INSERT` column list and a `SET`-assignment LHS (task #143, from the
/// real-TiDB corpus). All godump-verified.
#[test]
fn insert_keyword_column_names() {
    assert_eq!(
        r("insert into t (year, c) values (2046, space(256))"),
        "INSERT INTO `t` (`year`,`c`) VALUES (2046,SPACE(256))"
    );
    assert_eq!(
        r("insert into t (year) values (1)"),
        "INSERT INTO `t` (`year`) VALUES (1)"
    );
    assert_eq!(
        r("insert into t set year = 2020"),
        "INSERT INTO `t` SET `year`=2020"
    );
}

#[test]
fn update_derived_table_target_preserves_join_ast() {
    assert_eq!(
        r("update (select * from t) t set c1 = 1111111"),
        "UPDATE (SELECT * FROM `t`) AS `t` SET `c1`=1111111"
    );
    assert_eq!(
        r("update (select 1 as a) as t, t1 set t1.a=1"),
        "UPDATE (SELECT 1 AS `a`) AS `t`, `t1` SET `t1`.`a`=1"
    );
}

#[test]
fn with_update_and_delete_preserve_the_cte_owner() {
    assert_eq!(
        r("with cte(a) as (select 1) update cte set a=2"),
        "WITH `cte` (`a`) AS (SELECT 1) UPDATE `cte` SET `a`=2"
    );
    assert_eq!(
        r("with recursive cte(a) as (select 1 union select a + 1 from cte where a < 3) delete from cte"),
        "WITH RECURSIVE `cte` (`a`) AS (SELECT 1 UNION SELECT `a`+1 FROM `cte` WHERE `a`<3) DELETE FROM `cte`"
    );
}
