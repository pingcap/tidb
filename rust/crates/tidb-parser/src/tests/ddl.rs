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

//! `CREATE`/`ALTER`/`RENAME`/`DROP TABLE` and the
//! `CREATE`/`ALTER`/`DROP SEQUENCE` statement-family tests.

use super::*;

macro_rules! assert_ddl_variant {
    ($sql:expr, $pattern:pat) => {{
        let Stmt::Ddl(ddl) = parse($sql).unwrap() else {
            panic!("expected DDL envelope for {}", $sql)
        };
        assert!(
            matches!(ddl.as_ref(), $pattern),
            "wrong DDL payload for {}",
            $sql
        );
    }};
}

#[test]
fn test_ddl_visitor_cover() {
    for sql in [
        "CREATE DATABASE d",
        "ALTER DATABASE d CHARACTER SET utf8mb4",
        "DROP DATABASE d",
        "DROP INDEX i ON t",
        "DROP TABLE t1, t2",
        "RENAME TABLE t1 TO t2",
        "TRUNCATE TABLE t",
        "ALTER TABLE t ADD COLUMN a INT DEFAULT 1",
        "ALTER TABLE t ATTRIBUTES='zone=sh', STATS_OPTIONS='sample=1'",
        "CREATE INDEX i ON t (a)",
        "CREATE TABLE t (a INT DEFAULT 1, CONSTRAINT c CHECK (a > 0))",
        "CREATE VIEW v AS SELECT 1",
    ] {
        assert_full_visitor_traversal(sql);
    }
}

#[test]
fn ddl_statements_use_one_outer_envelope() {
    assert_ddl_variant!(
        "create table ddl_envelope (a int)",
        tidb_ast::DdlStmt::CreateTable(_)
    );
    assert_ddl_variant!(
        "create index idx_a on ddl_envelope (a)",
        tidb_ast::DdlStmt::CreateIndex(_)
    );
    assert_ddl_variant!(
        "create view ddl_envelope as select 1",
        tidb_ast::DdlStmt::CreateView(_)
    );
    assert_ddl_variant!(
        "create database ddl_envelope",
        tidb_ast::DdlStmt::CreateDatabase { .. }
    );
    assert_ddl_variant!(
        "alter table ddl_envelope add b int",
        tidb_ast::DdlStmt::AlterTable(_)
    );
    assert_ddl_variant!(
        "rename table ddl_envelope to ddl_envelope_2",
        tidb_ast::DdlStmt::RenameTable(_)
    );
    assert_ddl_variant!("drop table ddl_envelope", tidb_ast::DdlStmt::DropTable(_));
    assert_ddl_variant!("drop tables ddl_envelope", tidb_ast::DdlStmt::DropTable(_));
    assert_ddl_variant!("drop view ddl_envelope", tidb_ast::DdlStmt::DropView { .. });
    assert_ddl_variant!(
        "drop database ddl_envelope",
        tidb_ast::DdlStmt::DropDatabase { .. }
    );
    assert_ddl_variant!(
        "drop resource group ddl_envelope",
        tidb_ast::DdlStmt::DropResourceGroup { .. }
    );
    assert_ddl_variant!(
        "truncate table ddl_envelope",
        tidb_ast::DdlStmt::TruncateTable(_)
    );
}

#[test]
fn drop_tables_plural_is_go_alias_for_drop_table() {
    // Go's `parseDropStmt` dispatches both `tableKwd` and `tables` to one
    // `DropTableStmt`; Restore deliberately canonicalizes the output.
    assert_eq!(
        r("drop tables if exists app.t, `u` restrict"),
        "DROP TABLE IF EXISTS `app`.`t`, `u`"
    );
    assert_eq!(
        r("drop temporary tables t1, t2 cascade"),
        "DROP TEMPORARY TABLE `t1`, `t2`"
    );
    assert_eq!(
        r("drop global temporary tables t1"),
        "DROP GLOBAL TEMPORARY TABLE `t1`"
    );
}

#[test]
fn lock_tables_leaf_grammar_matches_go_ast_restore_contract() {
    let mut parser = Parser::new("lock table `select` read local, app.t write, *.all_tables");
    let locks = parser
        .parse_lock_tables()
        .expect("parse Go LOCK TABLE[S] leaf grammar");
    assert!(parser.at_eof());
    assert_eq!(locks.len(), 3);
    assert_eq!(locks[0].table, vec!["select"]);
    assert_eq!(locks[0].lock_type, tidb_ast::TableLockType::ReadLocal);
    assert_eq!(locks[1].table, vec!["app", "t"]);
    assert_eq!(locks[1].lock_type, tidb_ast::TableLockType::Write);
    assert_eq!(locks[2].table, vec!["*", "all_tables"]);
    assert_eq!(locks[2].lock_type, tidb_ast::TableLockType::None);
    assert_eq!(
        Stmt::Ddl(tidb_ast::NodeBox::new(tidb_ast::DdlStmt::LockTables(
            Box::new(locks)
        )))
        .restore(),
        "LOCK TABLES `select` READ LOCAL, `app`.`t` WRITE, `*`.`all_tables` NONE"
    );

    let mut charset_name = Parser::new("lock table _utf8 read");
    let locks = charset_name
        .parse_lock_tables()
        .expect("Go accepts an underscoreCS table name");
    assert!(charset_name.at_eof());
    assert_eq!(locks[0].table, vec!["utf8"]);
    assert_eq!(
        Stmt::Ddl(tidb_ast::NodeBox::new(tidb_ast::DdlStmt::LockTables(
            Box::new(locks)
        )))
        .restore(),
        "LOCK TABLES `utf8` READ"
    );

    let mut unlock = Parser::new("unlock table");
    unlock
        .parse_unlock_tables()
        .expect("parse singular UNLOCK TABLE spelling");
    assert!(unlock.at_eof());
    assert_eq!(
        Stmt::Ddl(tidb_ast::NodeBox::new(tidb_ast::DdlStmt::UnlockTables)).restore(),
        "UNLOCK TABLES"
    );
}

#[test]
fn drop_index_leaf_grammar_preserves_typed_options() {
    let mut parser =
        Parser::new("drop index if exists idx_a on app.orders lock = exclusive algorithm inplace");
    let statement = parser
        .parse_drop_index()
        .expect("parse DROP INDEX leaf grammar");
    assert!(parser.at_eof());
    assert!(statement.if_exists);
    assert_eq!(statement.name, "idx_a");
    assert_eq!(statement.table, vec!["app", "orders"]);
    assert_eq!(
        statement.algorithm,
        Some(tidb_ast::DropIndexAlgorithm::Inplace)
    );
    assert_eq!(statement.lock, Some(tidb_ast::DropIndexLock::Exclusive));
    assert!(!statement.is_hypo);

    let hypo = parse("drop hypo index hypo_idx on app.orders").expect("parse DROP HYPO INDEX");
    let restored = hypo.restore();
    let Stmt::Ddl(ddl) = hypo else {
        panic!("expected DDL envelope for DROP HYPO INDEX")
    };
    let tidb_ast::DdlStmt::DropIndex(statement) = ddl.into_inner() else {
        panic!("expected DROP INDEX payload")
    };
    assert!(statement.is_hypo);
    assert_eq!(
        restored, "DROP INDEX `hypo_idx` ON `app`.`orders`",
        "Go's DropIndexStmt.Restore omits the execution-only HYPO flag"
    );

    let mut default_options = Parser::new("drop index idx_a on t algorithm default lock = default");
    let statement = default_options
        .parse_drop_index()
        .expect("parse Go-supported default options");
    assert!(default_options.at_eof());
    assert_eq!(statement.algorithm, None);
    assert_eq!(statement.lock, None);

    let mut invalid_algorithm = Parser::new("drop index idx_a on t algorithm unknown");
    assert!(invalid_algorithm.parse_drop_index().is_err());
    let mut invalid_lock = Parser::new("drop index idx_a on t lock unknown");
    assert!(invalid_lock.parse_drop_index().is_err());
}

#[test]
fn split_region_grammar_uses_typed_admin_and_ddl_envelopes() {
    assert_eq!(
        r("split table t index idx1 by (10000, 'abcd'), (10000000)"),
        "SPLIT TABLE `t` INDEX `idx1` BY (10000,_UTF8MB4'abcd'),(10000000)"
    );
    assert_eq!(
        r("split region for partition table t partition (p3, p4) between (100000000) and (1000000000) regions 5"),
        "SPLIT REGION FOR PARTITION TABLE `t` PARTITION(`p3`, `p4`) BETWEEN (100000000) AND (1000000000) REGIONS 5"
    );
    assert_eq!(
        r("alter table t split primary key between (0, 'a', 0) and (100000, 'z', 100000) regions 5"),
        "ALTER TABLE `t` SPLIT PRIMARY KEY BETWEEN (0,_UTF8MB4'a',0) AND (100000,_UTF8MB4'z',100000) REGIONS 5"
    );
    assert_eq!(
        r("alter table t split index idx_user_id between () and () regions 0"),
        "ALTER TABLE `t` SPLIT INDEX `idx_user_id` BETWEEN () AND () REGIONS 0"
    );

    let Stmt::Admin(admin) = parse("split table t by (1)").unwrap() else {
        panic!("standalone SPLIT must use the Admin envelope")
    };
    assert!(matches!(
        admin.as_ref(),
        tidb_ast::AdminStmt::SplitRegion(_)
    ));
    assert_ddl_variant!(
        "alter table t split index idx by (1)",
        tidb_ast::DdlStmt::AlterTable(_)
    );

    // `BY` requires one or more values in every tuple.  Empty bounds are a
    // separate Go-supported `BETWEEN` form, tested above.
    assert!(parse("split table t by ()").is_err());
    assert_eq!(
        r("split table t between (1) and (2) regions -1"),
        "SPLIT TABLE `t` BETWEEN (1) AND (2) REGIONS -1"
    );
    assert_eq!(
        r("split table t between (1) and (2) regions +2"),
        "SPLIT TABLE `t` BETWEEN (1) AND (2) REGIONS 2"
    );
    assert_eq!(
        r("split table t between (1) and (2) regions -9223372036854775808"),
        "SPLIT TABLE `t` BETWEEN (1) AND (2) REGIONS -9223372036854775808"
    );
    assert!(parse("split table t between (1) and (2) regions 9223372036854775808").is_err());
}

#[test]
fn create_view_core_grammar_preserves_go_defaults_and_query_shape() {
    assert_eq!(
        r("create view app.v (a, b) as select 1, 2 with local check option"),
        "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `app`.`v` (`a`,`b`) AS SELECT 1,2 WITH LOCAL CHECK OPTION"
    );
    assert_eq!(
        r("create or replace algorithm = merge view v as (select * from t union select * from u)"),
        "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS (SELECT * FROM `t` UNION SELECT * FROM `u`)"
    );
    assert_eq!(
        r("create algorithm = unexpected view v as select 1"),
        "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT 1"
    );

    let view = ddl_payload!(
        parse("create or replace algorithm = temptable view v(c) as (select 1)").unwrap(),
        CreateView
    );
    assert!(view.or_replace);
    assert_eq!(view.algorithm, tidb_ast::ViewAlgorithm::Temptable);
    assert_eq!(view.name, vec!["v"]);
    assert_eq!(view.columns, vec!["c"]);
    assert!(view.query_parenthesized);
    assert_eq!(view.check_option, tidb_ast::ViewCheckOption::Cascaded);

    assert_eq!(
        r("create definer = 'root' view v as select 1"),
        "CREATE ALGORITHM = UNDEFINED DEFINER = `root`@`%` SQL SECURITY DEFINER VIEW `v` AS SELECT 1"
    );
    assert_eq!(
        r("create definer = 'root'@'localhost' sql security invoker view v as select 1"),
        "CREATE ALGORITHM = UNDEFINED DEFINER = `root`@`localhost` SQL SECURITY INVOKER VIEW `v` AS SELECT 1"
    );
    assert_eq!(
        r("create definer = current_user() view v as select 1"),
        "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT 1"
    );

    let view = ddl_payload!(
        parse("create definer = 'root'@'localhost' sql security invoker view v as select 1")
            .unwrap(),
        CreateView
    );
    assert_eq!(view.definer.user, "root");
    assert_eq!(view.definer.host, "localhost");
    assert!(!view.definer.current_user);
    assert_eq!(view.security, tidb_ast::ViewSecurity::Invoker);
}

#[test]
fn create_database_options_restore_and_scope() {
    assert_eq!(
        r("create schema if not exists app"),
        "CREATE DATABASE IF NOT EXISTS `app`"
    );
    assert_eq!(r("create database app"), "CREATE DATABASE `app`");
    assert_eq!(r("create database 'app'"), "CREATE DATABASE `app`");
    assert_eq!(
        r("create database if not exists `app``archive`"),
        "CREATE DATABASE IF NOT EXISTS `app``archive`"
    );
    assert_eq!(
        r("create database plan_cache"),
        "CREATE DATABASE `plan_cache`"
    );
    assert_eq!(
        r("create database app character set utf8 collate utf8_bin"),
        "CREATE DATABASE `app` CHARACTER SET = utf8 COLLATE = utf8_bin"
    );
    assert_eq!(
        r("create database app char utf8 placement 123"),
        "CREATE DATABASE `app` CHARACTER SET = utf8 PLACEMENT POLICY = `123`"
    );
    assert_eq!(
        r("create database app default charset = 'utf8mb4' default collate utf8mb4_roman_ci"),
        "CREATE DATABASE `app` CHARACTER SET = utf8mb4 COLLATE = utf8mb4_roman_ci"
    );
    assert_eq!(
        r("create database app placement policy set default"),
        "CREATE DATABASE `app` PLACEMENT POLICY = `DEFAULT`"
    );
    assert_eq!(
        r("create database app encryption = 'y'"),
        "CREATE DATABASE `app` ENCRYPTION = 'y'"
    );
    assert_eq!(
        r("create database app set tiflash replica 2 location labels 'a', 'b'"),
        "CREATE DATABASE `app` SET TIFLASH REPLICA 2 LOCATION LABELS 'a', 'b'"
    );
    assert!(parse("create database app character set uft8").is_err());
    assert!(parse("create database app character set ucs2").is_err());
    assert!(parse("create database app default unsupported").is_err());
}

#[test]
fn test_alter_database_restore() {
    assert_eq!(
        r("alter database 'db1' charset utf8"),
        "ALTER DATABASE `db1` CHARACTER SET = utf8"
    );
    assert_eq!(
        r("alter database character utf8 placement p"),
        "ALTER DATABASE CHARACTER SET = utf8 PLACEMENT POLICY = `p`"
    );
    assert_eq!(
        r("alter database db1 default character set = utf8 collate = utf8_bin"),
        "ALTER DATABASE `db1` CHARACTER SET = utf8 COLLATE = utf8_bin"
    );
    assert_eq!(
        r("alter schema default collate = 'UTF8_BiN'"),
        "ALTER DATABASE COLLATE = utf8_bin"
    );
    assert_eq!(
        r("alter database db1 placement policy set default"),
        "ALTER DATABASE `db1` PLACEMENT POLICY = `DEFAULT`"
    );
    assert!(parse("alter database db1").is_err());
}

#[test]
fn test_ddl_drop_table_stmt_restore() {
    assert_eq!(r("drop table t"), "DROP TABLE `t`");
    assert_eq!(
        r("drop table if exists t1, t2"),
        "DROP TABLE IF EXISTS `t1`, `t2`"
    );
    // A qualified name path restores dot-joined, matching every other
    // table-name-path statement (`ALTER TABLE`/`RENAME TABLE`).
    assert_eq!(r("drop table db.t1"), "DROP TABLE `db`.`t1`");
    // `RESTRICT`/`CASCADE` parse but restore to nothing -- real
    // MySQL/TiDB enforce referential integrity unconditionally either
    // way, so neither changes behavior.
    assert_eq!(r("drop table t1, t2 restrict"), "DROP TABLE `t1`, `t2`");
    assert_eq!(r("drop table t1 cascade"), "DROP TABLE `t1`");
    // `TEMPORARY` / `GLOBAL TEMPORARY` modifiers restore before `TABLE`
    // (task #152).
    assert_eq!(
        r("drop temporary table if exists t1, t2"),
        "DROP TEMPORARY TABLE IF EXISTS `t1`, `t2`"
    );
    assert_eq!(
        r("DROP /*!40005 TEMPORARY */ TABLE IF EXISTS `test`"),
        "DROP TEMPORARY TABLE IF EXISTS `test`"
    );
    assert_eq!(
        r("drop global temporary table if exists temp"),
        "DROP GLOBAL TEMPORARY TABLE IF EXISTS `temp`"
    );
}

/// `DROP VIEW` (a name list, like `DROP TABLE`) and `DROP {DATABASE|SCHEMA}`
/// (a single name; both spellings restore as `DROP DATABASE`) — parse+restore
/// only, task #145. All godump-verified.
#[test]
fn drop_view_and_database() {
    assert_eq!(r("drop view v1"), "DROP VIEW `v1`");
    assert_eq!(
        r("drop view if exists v1, v2"),
        "DROP VIEW IF EXISTS `v1`, `v2`"
    );
    assert_eq!(r("drop view db.v"), "DROP VIEW `db`.`v`");
    assert_eq!(r("drop database db1"), "DROP DATABASE `db1`");
    assert_eq!(
        r("drop database if exists db1"),
        "DROP DATABASE IF EXISTS `db1`"
    );
    // `SCHEMA` is a synonym, restored as `DATABASE`.
    assert_eq!(
        r("drop schema if exists db1"),
        "DROP DATABASE IF EXISTS `db1`"
    );
}

/// `pkg/parser/ddl_drop_parser.go` broad source-owned name boundaries.
#[test]
fn drop_parser_source_boundaries() {
    for (sql, expected) in [
        ("DROP INDEX 1 ON 't'", "DROP INDEX `1` ON `t`"),
        ("RENAME TABLE 'a' TO 'b'", "RENAME TABLE `a` TO `b`"),
        ("DROP VIEW 'v' CASCADE", "DROP VIEW `v`"),
        ("TRUNCATE 't'", "TRUNCATE TABLE `t`"),
        (
            "DROP PROCEDURE IF EXISTS 'db'.'p'",
            "DROP PROCEDURE IF EXISTS `db`.`p`",
        ),
        (
            "DROP STATS 't' PARTITION @p",
            "DROP STATS `t` PARTITION `p`",
        ),
        (
            "DROP STATS t PARTITION max",
            "DROP STATS `t` PARTITION `max`",
        ),
        (
            "ANALYZE TABLE 't' PARTITION 'p' INDEX 'i',@j WITH 1 TOPN",
            "ANALYZE TABLE `t` PARTITION `p` INDEX `i`,`j` WITH 1 TOPN",
        ),
        (
            "ANALYZE TABLE t COLUMNS @a,'b'",
            "ANALYZE TABLE `t` COLUMNS `a`,`b`",
        ),
        (
            "ANALYZE TABLE t UPDATE HISTOGRAM ON @a,'b'",
            "ANALYZE TABLE `t` UPDATE HISTOGRAM ON `a`,`b`",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }

    assert!(parse("DROP STATS t PARTITION 'p'").is_err());
    for (sql, expected) in [
        (
            "DROP STATS t GLOBAL",
            "'DROP STATS ... GLOBAL' is deprecated and will be removed in a future release. Please use DROP STATS ... instead",
        ),
        (
            "DROP STATS t PARTITION p",
            "'DROP STATS ... PARTITION ...' is deprecated and will be removed in a future release.",
        ),
    ] {
        let output = parse_with_warnings(sql).unwrap();
        assert_eq!(output.warnings[0].message, expected, "{sql}");
    }
}
