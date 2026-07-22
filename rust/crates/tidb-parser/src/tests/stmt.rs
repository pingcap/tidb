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

//! Remaining cross-domain statement contracts, transaction control, and the
//! honest rejection of unsupported statements.

use super::*;

#[test]
fn root_statement_preserves_source_text_and_position() {
    let mut statement = parse(" \tselect 1;  ").expect("statement parses");
    assert_eq!(statement.original_text(), b"select 1;");
    assert_eq!(statement.text(), b"select 1;");

    statement.set_origin_text_position(3);
    assert_eq!(statement.origin_text_position(), 3);

    let statements = parse_multi(" select 1;  SELECT 2").expect("statements parse");
    assert_eq!(statements.len(), 2);
    assert_eq!(statements[0].original_text(), b"select 1;");
    assert_eq!(statements[1].original_text(), b"SELECT 2");
}

#[test]
fn select_fields_preserve_their_exact_source_text() {
    fn field_texts(statement: &tidb_ast::Stmt) -> Vec<&[u8]> {
        let tidb_ast::Stmt::Query(query) = statement else {
            panic!("expected query statement");
        };
        let tidb_ast::QueryStmt::Select(select) = query.as_ref() else {
            panic!("expected SELECT statement");
        };
        (0..select.fields.len())
            .map(|index| select.fields.text(index).expect("field metadata"))
            .collect()
    }

    let statement = parse("select a from t").expect("SELECT parses");
    assert_eq!(field_texts(&statement), vec![b"a".as_slice()]);

    let statements =
        parse_multi("SELECT 'foo'; SELECT 'foo;bar','baz'; select 'foo' , 'bar' , 'baz' ;select 1")
            .expect("multi-statement SELECT parses");
    assert_eq!(field_texts(&statements[0]), vec![b"'foo'".as_slice()]);
    assert_eq!(
        field_texts(&statements[1]),
        vec![b"'foo;bar'".as_slice(), b"'baz'".as_slice()]
    );
    assert_eq!(
        field_texts(&statements[2]),
        vec![
            b"'foo'".as_slice(),
            b"'bar'".as_slice(),
            b"'baz'".as_slice(),
        ]
    );
    assert_eq!(field_texts(&statements[3]), vec![b"1".as_slice()]);
}

#[test]
fn nested_queries_preserve_their_exact_source_text() {
    fn scalar_subquery_text(statement: &tidb_ast::Stmt) -> &[u8] {
        let tidb_ast::Stmt::Query(query) = statement else {
            panic!("expected query statement");
        };
        let tidb_ast::QueryStmt::Select(select) = query.as_ref() else {
            panic!("expected SELECT statement");
        };
        let tidb_ast::SelectField::Expr { expr, .. } = &select.fields[0] else {
            panic!("expected expression field");
        };
        let tidb_ast::Expr::Binary(_, _, right) = expr else {
            panic!("expected comparison expression");
        };
        let tidb_ast::Expr::Subquery(subquery) = right.as_ref() else {
            panic!("expected scalar subquery");
        };
        subquery.text()
    }

    for (sql, expected) in [
        ("SELECT 1 > (select 1)", b"select 1".as_slice()),
        (
            "SELECT 1 > (select 1 union select 2)",
            b"select 1 union select 2".as_slice(),
        ),
    ] {
        let statement = parse(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        assert_eq!(scalar_subquery_text(&statement), expected, "{sql}");
    }

    let statement = parse("CREATE VIEW v AS SELECT * FROM t").expect("view parses");
    let tidb_ast::Stmt::Ddl(ddl) = statement else {
        panic!("expected DDL statement");
    };
    let tidb_ast::DdlStmt::CreateView(view) = ddl.as_ref() else {
        panic!("expected CREATE VIEW");
    };
    assert_eq!(view.query.text(), b"SELECT * FROM t");
}

#[test]
fn nested_statement_owners_preserve_go_source_text_boundaries() {
    let statement = parse("trace format = 'row' select a from t").expect("TRACE parses");
    let tidb_ast::Stmt::Admin(admin) = statement else {
        panic!("expected admin statement");
    };
    let tidb_ast::AdminStmt::Trace(trace) = admin.as_ref() else {
        panic!("expected TRACE");
    };
    assert_eq!(trace.statement.text(), b"select a from t");

    for (sql, expected_text) in [
        ("explain explore select 1", b"select 1".as_slice()),
        ("explain select 1", b"".as_slice()),
    ] {
        let statement = parse(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        let tidb_ast::Stmt::Admin(admin) = statement else {
            panic!("expected admin statement");
        };
        let tidb_ast::AdminStmt::Explain(explain) = admin.as_ref() else {
            panic!("expected EXPLAIN");
        };
        let inner = explain.statement().expect("nested EXPLAIN statement");
        assert_eq!(inner.text(), expected_text, "{sql}");
    }

    let statement =
        parse("create binding for select 1 using select 2").expect("CREATE BINDING parses");
    let tidb_ast::Stmt::Admin(admin) = statement else {
        panic!("expected admin statement");
    };
    let tidb_ast::AdminStmt::CreateBinding(binding) = admin.as_ref() else {
        panic!("expected CREATE BINDING");
    };
    let tidb_ast::CreateBindingSource::Statement { target } = &binding.source else {
        panic!("expected statement binding");
    };
    assert_eq!(target.origin.text(), b"select 1");
    assert_eq!(
        target.hinted.as_ref().expect("hinted statement").text(),
        b"select 2"
    );

    let statement =
        parse("create procedure p() begin select 1; end").expect("CREATE PROCEDURE parses");
    let tidb_ast::Stmt::Ddl(ddl) = statement else {
        panic!("expected DDL statement");
    };
    let tidb_ast::DdlStmt::CreateProcedure(procedure) = ddl.as_ref() else {
        panic!("expected CREATE PROCEDURE");
    };
    assert_eq!(procedure.body.text(), b"begin select 1; end");
}

#[test]
fn rejects_unsupported() {
    // Out-of-scope constructs error rather than mis-parse. (`TRUNCATE
    // TABLE` used to sit here but is now modelled — see `ddl` tests.)
    assert_eq!(
        r("grant select on *.* to u require ssl"),
        "GRANT SELECT ON *.* TO `u`@`%` REQUIRE SSL"
    );
    assert_eq!(r("show full tables"), "SHOW FULL TABLES");
}

/// Go's `DoStmt` owns a full comma-separated expression list even though its
/// executor discards result rows. Preserve those expressions so execution can
/// later implement side effects and warning behavior without reparsing SQL.
#[test]
fn do_restore_and_scope() {
    assert_eq!(r("do 1, sleep(1)"), "DO 1, SLEEP(1)");
    assert_eq!(
        r("do 1 in (select * from t)"),
        "DO 1 IN (SELECT * FROM `t`)"
    );
    assert_eq!(
        r("do @a := (select * from t where i = 1)"),
        "DO @`a`:=(SELECT * FROM `t` WHERE `i`=1)"
    );
    assert!(parse("do").is_err());
    assert!(parse("do 1,").is_err());
    assert!(parse("do 1 from t").is_err());
}

/// The selected Plan Replayer form owns its nested query as a `QueryStmt`,
/// preserving CTE/set-operation restore without opening up the independent
/// capture/load/file command families.
#[test]
fn plan_replayer_dump_explain_query_restore_and_scope() {
    assert_eq!(
        r("plan replayer dump explain with cte as (select 1) select * from cte union select 2"),
        "PLAN REPLAYER DUMP EXPLAIN WITH `cte` AS (SELECT 1) SELECT * FROM `cte` UNION SELECT 2"
    );

    let statement = parse("plan replayer dump explain select 1").expect("PLAN REPLAYER parses");
    let tidb_ast::Stmt::Admin(admin) = statement else {
        panic!("expected Admin envelope");
    };
    let tidb_ast::AdminStmt::PlanReplayer(replayer) = admin.as_ref() else {
        panic!("expected typed Plan Replayer dump/explain payload");
    };
    assert!(matches!(
        replayer.as_ref(),
        tidb_ast::PlanReplayerStmt::Dump {
            target,
            ..
        } if matches!(target.as_ref(), tidb_ast::PlanReplayerTarget::Statement(statement)
            if matches!(statement.as_ref(), tidb_ast::Stmt::Query(_)))
    ));

    assert_eq!(
        r("plan replayer load 'replayer.zip'"),
        "PLAN REPLAYER LOAD 'replayer.zip'"
    );
}
#[test]
fn stats_lock_restore_and_scope() {
    assert_eq!(
        r("lock stats t1, schema.t2 partition p0, p1"),
        "LOCK STATS `t1`, `schema`.`t2` PARTITION(`p0`, `p1`)"
    );
    assert_eq!(
        r("unlock stats t partition (p0, p1)"),
        "UNLOCK STATS `t` PARTITION(`p0`, `p1`)"
    );
}

/// `EXPLAIN` is a wrapper around an already-parseable DML/query/ALTER
/// statement, not a planner shortcut. These are direct vectors from Go's
/// `TestExplain` (`pkg/parser/parser_test.go`): parse/restore keeps its
/// default `row` format, the analysis flag, and an explicit format. The
/// executor deliberately rejects every wrapper because this seed has no
/// optimizer or plan renderer.
#[test]
fn explain_wrapper_restore_and_scope() {
    assert_eq!(r("explain select 1"), "EXPLAIN FORMAT = 'row' SELECT 1");
    assert_eq!(
        r("explain analyze format = verbose select 1"),
        "EXPLAIN ANALYZE FORMAT = 'verbose' SELECT 1"
    );
    assert_eq!(
        r("explain format = 'brief' insert into t values (1)"),
        "EXPLAIN FORMAT = 'brief' INSERT INTO `t` VALUES (1)"
    );
    assert_eq!(
        r("explain alter table t add column a int"),
        "EXPLAIN FORMAT = 'row' ALTER TABLE `t` ADD COLUMN `a` INT"
    );

    assert_eq!(
        r("explain for connection 1"),
        "EXPLAIN FORMAT = 'row' FOR CONNECTION 1"
    );
    assert_eq!(
        r("explain 'plan_digest'"),
        "EXPLAIN FORMAT = 'row' 'plan_digest'"
    );
    assert_eq!(r("explain explore 'digest'"), "EXPLAIN EXPLORE 'digest'");
}

#[test]
fn explain_digest_and_explore_source_rows() {
    for (sql, expected) in [
        ("EXPLAIN ANALYZE 'sqldigest'", "EXPLAIN ANALYZE 'sqldigest'"),
        (
            "EXPLAIN ANALYZE format='json' 'sqldigest'",
            "EXPLAIN ANALYZE FORMAT = 'json' 'sqldigest'",
        ),
        ("explain explore 'digestxxx'", "EXPLAIN EXPLORE 'digestxxx'"),
        (
            "explain explore replayer '/tmp/replayer.zip'",
            "EXPLAIN EXPLORE REPLAYER '/tmp/replayer.zip'",
        ),
        (
            "explain explore select 1 from t",
            "EXPLAIN EXPLORE SELECT 1 FROM `t`",
        ),
        (
            "explain explore select 1 from t1, t2",
            "EXPLAIN EXPLORE SELECT 1 FROM (`t1`) JOIN `t2`",
        ),
        (
            "explain explore select 1 from t where t1.a > (select max(a) from t2)",
            "EXPLAIN EXPLORE SELECT 1 FROM `t` WHERE `t1`.`a`>(SELECT MAX(`a`) FROM `t2`)",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }
}

/// Go shares `parseExplainStmt` across EXPLAIN/DESC/DESCRIBE. A query target
/// must therefore reach the EXPLAIN wrapper before the bare-table describe
/// fallback; DML remains outside this Query-envelope translation slice.
#[test]
fn desc_describe_query_targets_restore_as_explain() {
    assert_eq!(r("desc select 1"), "EXPLAIN FORMAT = 'row' SELECT 1");
    assert_eq!(
        r("describe format = brief select a from t"),
        "EXPLAIN FORMAT = 'brief' SELECT `a` FROM `t`"
    );
    assert_eq!(r("desc analyze select 1"), "EXPLAIN ANALYZE SELECT 1");
}

/// Go's `parseExplainStmt` maps standalone `DESC`/`DESCRIBE` and a bare
/// `EXPLAIN <table>` target to the same `SHOW COLUMNS` AST normal form. The
/// normal form always restores as `DESC`; it is not a query-plan wrapper.
#[test]
fn describe_table_restore_and_scope() {
    assert_eq!(r("desc sche.tabl"), "DESC `sche`.`tabl`");
    assert_eq!(r("desc sche.tabl colum"), "DESC `sche`.`tabl` `colum`");
    assert_eq!(r("describe sche.tabl colum"), "DESC `sche`.`tabl` `colum`");
    assert_eq!(r("explain sche.tabl"), "DESC `sche`.`tabl`");
    assert_eq!(
        r("explain format = brief sche.tabl colum"),
        "DESC `sche`.`tabl` `colum`"
    );
    assert_eq!(r("explain analyze sche.tabl"), "DESC `sche`.`tabl`");

    // `DESC` and `DESCRIBE` do not stop at the table fallback. Go shares the
    // full explain tail across all three leaders, so query/DML forms restore
    // as EXPLAIN rather than being misparsed as table names.
    assert_eq!(
        r("desc select * from t where a = 1"),
        "EXPLAIN FORMAT = 'row' SELECT * FROM `t` WHERE `a`=1"
    );
    assert_eq!(
        r("describe analyze update t set a = 1"),
        "EXPLAIN ANALYZE UPDATE `t` SET `a`=1"
    );
    assert_eq!(
        r("desc format = brief delete from t"),
        "EXPLAIN FORMAT = 'brief' DELETE FROM `t`"
    );

    // Go accepts `EXPLAIN TABLE <name>` as the distinct TABLE query form,
    // not the `DESC` fallback. Its checked integration-oracle row keeps this
    // form visible even though this seed has no dedicated AST variant.
    assert_eq!(r("explain table t"), "EXPLAIN FORMAT = 'row' TABLE `t`");
}

#[test]
fn load_stats_has_a_dedicated_admin_payload() {
    // Direct vector from Go's `pkg/parser/parser_test.go:1385`; the broader
    // integration corpus selector guards all checked `LOAD STATS` fixtures.
    let stmt = parse("load stats '/tmp/stats.json'").expect("LOAD STATS parses");
    assert_eq!(stmt.restore(), "LOAD STATS '/tmp/stats.json'");
    let tidb_ast::Stmt::Admin(admin) = stmt else {
        panic!("LOAD STATS must use the Admin envelope")
    };
    assert!(matches!(admin.as_ref(), tidb_ast::AdminStmt::LoadStats(_)));
    assert!(parse("load stats").is_err());
    assert!(parse("load stats ./stats.json").is_err());
}

#[test]
fn drop_stats_restores_typed_scopes() {
    assert_eq!(r("drop stats t1, db.t2"), "DROP STATS `t1`, `db`.`t2`");
    assert_eq!(r("drop stats t global"), "DROP STATS `t` GLOBAL");
    assert_eq!(
        r("drop stats t partition p0, p1"),
        "DROP STATS `t` PARTITION `p0`, `p1`"
    );
}
/// `USE dbname` — a single database identifier, restored back-quoted
/// (task #142, from the real-TiDB corpus). All godump-verified.
#[test]
fn use_statement() {
    assert_eq!(r("use test_db_3"), "USE `test_db_3`");
    assert_eq!(r("USE Issue32007"), "USE `Issue32007`");
    // A name that lexes as a reserved keyword still round-trips.
    assert_eq!(r("use `select`"), "USE `select`");
}

/// `PREPARE` / `EXECUTE` / `DEALLOCATE PREPARE` — prepared statements
/// (task #151, parse+restore only). The `PREPARE` SQL restores as a plain
/// single-quoted string (no `_UTF8MB4` prefix), `DROP PREPARE` normalizes
/// to `DEALLOCATE PREPARE`. All godump-verified.
#[test]
fn prepared_statements() {
    assert_eq!(
        r("prepare stmt from 'select 1'"),
        "PREPARE `stmt` FROM 'select 1'"
    );
    assert_eq!(r("prepare stmt from @s"), "PREPARE `stmt` FROM @`s`");
    assert_eq!(
        r("prepare stmt2 from 'select * from t where a in (?, ?)'"),
        "PREPARE `stmt2` FROM 'select * from t where a in (?, ?)'"
    );
    assert_eq!(r("execute stmt"), "EXECUTE `stmt`");
    assert_eq!(
        r("execute stmt using @a, @b"),
        "EXECUTE `stmt` USING @`a`,@`b`"
    );
    assert_eq!(r("deallocate prepare stmt"), "DEALLOCATE PREPARE `stmt`");
    assert_eq!(r("drop prepare stmt"), "DEALLOCATE PREPARE `stmt`");

    // Go parses an empty SQL string into PrepareStmt, then Restore rejects
    // the zero-value SQLText/SQLVar state. Keep parse and restore failures as
    // distinct boundaries instead of rejecting the grammar early.
    let empty = parse("prepare stmt from ''").expect("Go accepts the syntax");
    assert_eq!(
        empty.try_restore().unwrap_err(),
        "An error occurred while restore PrepareStmt"
    );

    // Go's grammar only produces user variables, but ExecuteStmt stores
    // []ExprNode. Preserve that production AST contract for rewritten and
    // hand-built trees instead of narrowing it to variable names.
    let mut statement =
        tidb_ast::Stmt::Session(tidb_ast::NodeBox::new(tidb_ast::SessionStmt::Execute {
            name: "stmt".to_owned(),
            using: vec![tidb_ast::Expr::Binary(
                tidb_ast::BinaryOp::Plus,
                Box::new(tidb_ast::Expr::Int("1".to_owned())),
                Box::new(tidb_ast::Expr::Int("2".to_owned())),
            )],
        }));
    assert_eq!(statement.restore(), "EXECUTE `stmt` USING 1+2");

    struct CountExpressions(usize);
    impl tidb_ast::Visitor for CountExpressions {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if node.is::<tidb_ast::Expr>() {
                self.0 += 1;
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut visitor = CountExpressions(0);
    assert!(tidb_ast::Visitable::accept(&mut statement, &mut visitor));
    assert_eq!(visitor.0, 3);
}

#[test]
fn current_time_functions() {
    // NOW always needs `()`; CURRENT_TIMESTAMP also parses bare (a real
    // MySQL grammar rule, confirmed via `godump restore`), and both
    // restore identically to their parenthesized form.
    assert_eq!(r("select now()"), "SELECT NOW()");
    assert_eq!(r("select now(3)"), "SELECT NOW(3)");
    assert_eq!(r("select current_timestamp"), "SELECT CURRENT_TIMESTAMP()");
    assert_eq!(
        r("select current_timestamp()"),
        "SELECT CURRENT_TIMESTAMP()"
    );
    assert_eq!(
        r("select current_timestamp(6)"),
        "SELECT CURRENT_TIMESTAMP(6)"
    );
    // Go's `parseCurrentFunc` accepts only an integer literal precision. A
    // leading sign or arithmetic expression is a syntax error, rather than
    // a generic expression argument (the original integration cases are
    // `CURRENT_TIME(-1)` and `CURRENT_TIMESTAMP(-1)`).
    for sql in [
        "select current_time(-1)",
        "select current_timestamp(-1)",
        "select current_time(1+1)",
        "select current_timestamp(1+1)",
    ] {
        assert!(
            parse(sql).is_err(),
            "Go rejects non-literal precision: {sql}"
        );
    }
    for (sql, expected) in [
        ("select current_time(0)", "SELECT CURRENT_TIME(0)"),
        (
            "select current_time(2147483647)",
            "SELECT CURRENT_TIME(2147483647)",
        ),
        (
            "select current_time(2147483648)",
            "SELECT CURRENT_TIME(2147483648)",
        ),
        ("select current_timestamp(0)", "SELECT CURRENT_TIMESTAMP(0)"),
        (
            "select current_timestamp(2147483647)",
            "SELECT CURRENT_TIMESTAMP(2147483647)",
        ),
        (
            "select current_timestamp(2147483648)",
            "SELECT CURRENT_TIMESTAMP(2147483648)",
        ),
    ] {
        assert_eq!(r(sql), expected);
    }
    // CURDATE/CURTIME (unlike CURRENT_DATE/CURRENT_TIME below) always
    // need `()` — confirmed via `godump restore`: bare `curdate`
    // parses as an ordinary column reference, and CURDATE(1) is a
    // parse error rather than a generic expression call.
    assert_eq!(r("select curdate()"), "SELECT CURDATE()");
    assert!(parse("select curdate(1)").is_err());
    assert_eq!(r("select curtime()"), "SELECT CURTIME()");
    assert_eq!(r("select curtime(3)"), "SELECT CURTIME(3)");
    // CURRENT_DATE/CURRENT_TIME/UTC_DATE/UTC_TIME/UTC_TIMESTAMP all
    // parse bare too, restoring identically to their parenthesized
    // form (the same rule as CURRENT_TIMESTAMP above).
    assert_eq!(r("select current_date"), "SELECT CURRENT_DATE()");
    assert_eq!(r("select current_date()"), "SELECT CURRENT_DATE()");
    assert_eq!(r("select current_time"), "SELECT CURRENT_TIME()");
    assert_eq!(r("select current_time(3)"), "SELECT CURRENT_TIME(3)");
    assert_eq!(r("select utc_date"), "SELECT UTC_DATE()");
    assert_eq!(r("select utc_date()"), "SELECT UTC_DATE()");
    assert_eq!(r("select utc_time"), "SELECT UTC_TIME()");
    assert_eq!(r("select utc_time(3)"), "SELECT UTC_TIME(3)");
    assert_eq!(r("select utc_timestamp"), "SELECT UTC_TIMESTAMP()");
    assert_eq!(r("select utc_timestamp(3)"), "SELECT UTC_TIMESTAMP(3)");
    // HOUR/MINUTE/SECOND are reserved lexer keywords (already used as
    // INTERVAL unit tokens) -- like YEAR/MONTH/DAY/QUARTER, they need
    // an explicit `is_scalar_kw_func` allow-list entry to parse as an
    // ordinary function call at all.
    assert_eq!(
        r("select hour(a), minute(a), second(a) from t"),
        "SELECT HOUR(`a`),MINUTE(`a`),SECOND(`a`) FROM `t`"
    );
}

/// Go retains explicit physical-primary-key layout in its DDL AST. The old
/// Rust parser took the generic `skip_to_item_end` path, so it accepted both
/// inputs while restoring them as ordinary primary keys; these source-backed
/// regressions require the storage mode to survive for both table and inline
/// forms (`pkg/parser/parser_test.go:3965-3970`).
#[test]
fn clustered_primary_key_restore() {
    assert_eq!(
        r("create table t (a int, b varchar(255), primary key(b, a) clustered)"),
        "CREATE TABLE `t` (`a` INT,`b` VARCHAR(255),PRIMARY KEY(`b`, `a`) CLUSTERED)"
    );
    assert_eq!(
        r("create table t (a int primary key nonclustered, b varchar(255))"),
        "CREATE TABLE `t` (`a` INT PRIMARY KEY NONCLUSTERED,`b` VARCHAR(255))"
    );
    let table_pk = parse("create table t (a int, primary key(a) clustered)").expect("parse");
    assert!(matches!(
        table_pk,
        tidb_ast::Stmt::Ddl(ref ddl)
            if matches!(ddl.as_ref(), tidb_ast::DdlStmt::CreateTable(table)
            if matches!(
                table.table_constraints.as_slice(),
                [tidb_ast::TableConstraint::Index(index)]
                    if index.kind == tidb_ast::IndexConstraintKind::PrimaryKey
                        && index.options.primary_key_storage
                            == Some(tidb_ast::PrimaryKeyStorage::Clustered)
            ))
    ));
    let inline_pk = parse("create table t (a int primary key nonclustered)").expect("parse");
    assert!(matches!(
        inline_pk,
        tidb_ast::Stmt::Ddl(ref ddl)
            if matches!(ddl.as_ref(), tidb_ast::DdlStmt::CreateTable(table)
            if matches!(
                table.columns[0].options.as_slice(),
                [tidb_ast::ColumnOption::InlineKey(tidb_ast::InlineKeyOption {
                    kind: tidb_ast::InlineKeyKind::Primary {
                        storage: Some(tidb_ast::PrimaryKeyStorage::NonClustered),
                    },
                    global: false,
                })]
            ))
    ));
    assert_eq!(
        r("create table t (a int, primary key(a) clustered using rtree)"),
        "CREATE TABLE `t` (`a` INT,PRIMARY KEY(`a`) CLUSTERED USING RTREE)"
    );
}

/// Go uses the same `IndexPartSpecification` for clustered primary keys and
/// ordinary indexes. Prefix lengths must therefore survive the typed table
/// key boundary instead of being treated as a secondary-index-only detail
/// (`tests/integrationtest/t/session/clustered_index.test:4,11,22,27`).
#[test]
fn clustered_primary_key_preserves_prefix_index_parts() {
    let sql =
        "create table t (a varchar(12), b int, primary key(a(3), b) clustered, key idx(a(2) desc))";
    assert_eq!(
        r(sql),
        "CREATE TABLE `t` (`a` VARCHAR(12),`b` INT,PRIMARY KEY(`a`(3), `b`) CLUSTERED,INDEX `idx`(`a`(2) DESC))"
    );
    let statement = parse(sql).expect("parse");
    let tidb_ast::Stmt::Ddl(ddl) = statement else {
        panic!("expected DDL envelope");
    };
    let tidb_ast::DdlStmt::CreateTable(table) = ddl.as_ref() else {
        panic!("expected CREATE TABLE");
    };
    let TableConstraint::Index(index) = &table.table_constraints[0] else {
        panic!("expected clustered primary key");
    };
    assert_eq!(
        index.options.primary_key_storage,
        Some(tidb_ast::PrimaryKeyStorage::Clustered)
    );
    assert_eq!(
        index.parts,
        vec![
            tidb_ast::IndexPart::Column {
                name: "a".to_string(),
                prefix_len: Some(3),
                desc: false,
            },
            tidb_ast::IndexPart::Column {
                name: "b".to_string(),
                prefix_len: None,
                desc: false,
            },
        ]
    );
    let TableConstraint::Index(index) = &table.table_constraints[1] else {
        panic!("expected ordinary index");
    };
    assert!(matches!(
        index.parts.as_slice(),
        [tidb_ast::IndexPart::Column {
            name,
            prefix_len: Some(2),
            desc: true,
        }] if name == "a"
    ));
}

#[test]
fn transactions() {
    // BEGIN and START TRANSACTION are synonyms -- both restore as
    // START TRANSACTION (confirmed via `godump restore`, not assumed).
    assert_eq!(r("begin"), "START TRANSACTION");
    assert_eq!(r("BEGIN"), "START TRANSACTION");
    assert_eq!(r("start transaction"), "START TRANSACTION");
    assert_eq!(r("commit"), "COMMIT");
    assert_eq!(r("rollback"), "ROLLBACK");
    for (sql, expected) in [
        ("COMMIT AND NO CHAIN", "COMMIT"),
        ("COMMIT NO RELEASE", "COMMIT"),
        ("COMMIT AND NO CHAIN NO RELEASE", "COMMIT"),
        ("COMMIT AND NO CHAIN RELEASE", "COMMIT RELEASE"),
        ("COMMIT AND CHAIN NO RELEASE", "COMMIT AND CHAIN"),
        ("ROLLBACK AND NO CHAIN", "ROLLBACK"),
        ("ROLLBACK NO RELEASE", "ROLLBACK"),
        ("ROLLBACK AND NO CHAIN NO RELEASE", "ROLLBACK"),
        ("ROLLBACK AND NO CHAIN RELEASE", "ROLLBACK RELEASE"),
        ("ROLLBACK AND CHAIN NO RELEASE", "ROLLBACK AND CHAIN"),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }
    assert!(parse("COMMIT AND CHAIN RELEASE").is_err());
    assert!(parse("ROLLBACK AND CHAIN RELEASE").is_err());
    // Savepoint names restore VERBATIM, case preserved, no
    // backtick-quoting (confirmed via `godump restore`, unlike a
    // plain table/column identifier).
    assert_eq!(r("savepoint sp1"), "SAVEPOINT sp1");
    assert_eq!(r("savepoint SP1"), "SAVEPOINT SP1");
    // S3 is a non-reserved TiDB keyword (external-storage syntax), but the
    // savepoint grammar accepts it as an unquoted identifier. This is
    // exercised by pkg/executor/test/txn/txn_test.go:TestTxnSavepoint1.
    assert_eq!(r("savepoint s3"), "SAVEPOINT s3");
    // `ROLLBACK TO`'s own `SAVEPOINT` keyword is optional and dropped
    // on restore either way.
    assert_eq!(r("rollback to sp1"), "ROLLBACK TO sp1");
    assert_eq!(r("rollback to savepoint sp1"), "ROLLBACK TO sp1");
    assert_eq!(r("rollback to s3"), "ROLLBACK TO s3");
    assert_eq!(r("release savepoint sp1"), "RELEASE SAVEPOINT sp1");
    assert_eq!(r("release savepoint s3"), "RELEASE SAVEPOINT s3");
    // Unlike `ROLLBACK TO`, `RELEASE`'s own `SAVEPOINT` keyword is
    // NOT optional -- a real ParseError, not silently accepted.
    assert!(parse("release sp1").is_err());
}

#[test]
fn session_statements_use_one_outer_envelope() {
    fn kind(stmt: &tidb_ast::SessionStmt) -> &'static str {
        match stmt {
            tidb_ast::SessionStmt::Use(_) => "use",
            tidb_ast::SessionStmt::Set(_) => "set",
            tidb_ast::SessionStmt::SetCharset { .. } => "set charset",
            tidb_ast::SessionStmt::SetResourceGroup(_) => "set resource group",
            tidb_ast::SessionStmt::SetSessionStates(_) => "set session states",
            tidb_ast::SessionStmt::Prepare { .. } => "prepare",
            tidb_ast::SessionStmt::Execute { .. } => "execute",
            tidb_ast::SessionStmt::Deallocate(_) => "deallocate",
            tidb_ast::SessionStmt::Begin(_) => "begin",
            tidb_ast::SessionStmt::Commit(_) => "commit",
            tidb_ast::SessionStmt::Rollback {
                savepoint: None, ..
            } => "rollback",
            tidb_ast::SessionStmt::Savepoint(_) => "savepoint",
            tidb_ast::SessionStmt::Rollback {
                savepoint: Some(_), ..
            } => "rollback to savepoint",
            tidb_ast::SessionStmt::ReleaseSavepoint(_) => "release savepoint",
            _ => panic!("source-owned session variant belongs in its leaf test"),
        }
    }

    for (sql, expected) in [
        ("use test", "use"),
        ("set autocommit = on", "set"),
        ("set names utf8", "set charset"),
        ("set resource group rg1", "set resource group"),
        ("set session_states 'serialized'", "set session states"),
        ("prepare p from 'select 1'", "prepare"),
        ("execute p using @v", "execute"),
        ("deallocate prepare p", "deallocate"),
        ("begin", "begin"),
        ("commit", "commit"),
        ("rollback", "rollback"),
        ("savepoint p", "savepoint"),
        ("rollback to p", "rollback to savepoint"),
        ("release savepoint p", "release savepoint"),
    ] {
        let stmt = parse(sql).expect("session statement parses");
        let tidb_ast::Stmt::Session(session) = stmt else {
            panic!("expected session envelope for {sql}");
        };
        assert_eq!(kind(session.as_ref()), expected);
    }
}

/// `BEGIN {OPTIMISTIC|PESSIMISTIC}` has a distinct AST payload and restore
/// form from an unqualified transaction start. Go's hand parser records the
/// mode in `ast.BeginStmt.Mode`; losing it would make parser-ring output
/// incorrectly collapse both statements into `START TRANSACTION`.
#[test]
fn transaction_begin_modes() {
    assert!(matches!(
        parse("begin optimistic"),
        Ok(tidb_ast::Stmt::Session(session))
            if matches!(session.as_ref(), tidb_ast::SessionStmt::Begin(begin)
                if begin.mode == tidb_ast::TransactionMode::Optimistic)
    ));
    assert!(matches!(
        parse("begin pessimistic"),
        Ok(tidb_ast::Stmt::Session(session))
            if matches!(session.as_ref(), tidb_ast::SessionStmt::Begin(begin)
                if begin.mode == tidb_ast::TransactionMode::Pessimistic)
    ));
    assert_eq!(r("begin optimistic"), "BEGIN OPTIMISTIC");
    assert_eq!(r("begin pessimistic"), "BEGIN PESSIMISTIC");
}

/// Go's `parseBeginStmt` has three AST-visible START TRANSACTION options.
/// `READ WRITE` and `WITH CONSISTENT SNAPSHOT` deliberately remain the
/// default payload because Go canonicalizes both to a bare start statement.
#[test]
fn start_transaction_options_preserve_go_ast_payload() {
    let causal_stmt =
        parse("start transaction with causal consistency only").expect("causal consistency parses");
    assert_eq!(
        causal_stmt.restore(),
        "START TRANSACTION WITH CAUSAL CONSISTENCY ONLY"
    );
    let tidb_ast::Stmt::Session(causal) = causal_stmt else {
        panic!("expected session statement");
    };
    let tidb_ast::SessionStmt::Begin(causal) = causal.as_ref() else {
        panic!("expected Begin");
    };
    assert!(causal.causal_consistency_only);
    assert!(!causal.read_only);
    assert!(causal.as_of.is_none());
    let read_only_stmt = parse("start transaction read only as of timestamp '2015-09-21 00:07:01'")
        .expect("read-only AS OF parses");
    assert_eq!(
        read_only_stmt.restore(),
        "START TRANSACTION READ ONLY AS OF TIMESTAMP _UTF8MB4'2015-09-21 00:07:01'"
    );
    let tidb_ast::Stmt::Session(read_only) = read_only_stmt else {
        panic!("expected session statement");
    };
    let tidb_ast::SessionStmt::Begin(read_only) = read_only.as_ref() else {
        panic!("expected Begin");
    };
    assert!(read_only.read_only);
    assert!(read_only.as_of.is_some());
    assert_eq!(r("start transaction read write"), "START TRANSACTION");
    assert_eq!(
        r("start transaction with consistent snapshot"),
        "START TRANSACTION"
    );
    assert!(parse("start transaction read only as of").is_err());
    assert!(parse("start transaction read only as of timestamp").is_err());
}
