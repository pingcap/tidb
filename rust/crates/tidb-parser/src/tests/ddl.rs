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

/// `pkg/parser/ast/ddl_test.go::TestDDLVisitorCover`.
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
        "CREATE INDEX i ON t (a)",
        "CREATE TABLE t (a INT DEFAULT 1, CONSTRAINT c CHECK (a > 0))",
        "CREATE VIEW v AS SELECT 1",
    ] {
        assert_full_visitor_traversal(sql);
    }
}

#[test]
fn alter_table_enable_disable_keys_match_go_owner_rows() {
    for (sql, restored, enabled) in [
        (
            "ALTER TABLE t ENABLE KEYS",
            "ALTER TABLE `t` ENABLE KEYS",
            true,
        ),
        (
            "ALTER TABLE t DISABLE KEYS",
            "ALTER TABLE `t` DISABLE KEYS",
            false,
        ),
    ] {
        let statement = parse(sql).expect("ALTER TABLE keys action parses");
        assert_eq!(statement.restore(), restored);
        let Stmt::Ddl(ddl) = statement else {
            panic!("expected DDL statement");
        };
        let tidb_ast::DdlStmt::AlterTable(alter) = ddl.into_inner() else {
            panic!("expected ALTER TABLE");
        };
        assert_eq!(
            alter.actions,
            vec![AlterTableAction::SetKeysEnabled(enabled)]
        );
    }

    assert_eq!(
        r("ALTER TABLE t ENABLE KEYS, COMMENT = 'cmt' PARTITION BY HASH(a)"),
        "ALTER TABLE `t` ENABLE KEYS, COMMENT = 'cmt' PARTITION BY HASH (`a`) PARTITIONS 1"
    );
    for sql in [
        "ALTER TABLE t ENABLE",
        "ALTER TABLE t DISABLE",
        "ALTER TABLE t ENABLE INDEX",
        "ALTER TABLE t DISABLE INDEX",
    ] {
        assert!(parse(sql).is_err(), "accepted invalid keys action: {sql}");
    }
}

#[test]
fn test_column_position_restore() {
    for (suffix, expected) in [
        ("", "ALTER TABLE `t` ADD COLUMN `a` VARCHAR(255)"),
        ("FIRST", "ALTER TABLE `t` ADD COLUMN `a` VARCHAR(255) FIRST"),
        (
            "AFTER b",
            "ALTER TABLE `t` ADD COLUMN `a` VARCHAR(255) AFTER `b`",
        ),
    ] {
        assert_eq!(
            r(&format!("ALTER TABLE t ADD COLUMN a VARCHAR(255) {suffix}")),
            expected
        );
    }
}

#[test]
fn test_alter_table_option_restore() {
    for (sql, expected) in [
        (
            "ALTER TABLE t ROW_FORMAT = COMPRESSED KEY_BLOCK_SIZE = 8",
            "ALTER TABLE `t` ROW_FORMAT = COMPRESSED KEY_BLOCK_SIZE = 8",
        ),
        (
            "ALTER TABLE t ROW_FORMAT = COMPRESSED, KEY_BLOCK_SIZE = 8",
            "ALTER TABLE `t` ROW_FORMAT = COMPRESSED, KEY_BLOCK_SIZE = 8",
        ),
    ] {
        assert_eq!(r(sql), expected);
    }
}

#[test]
fn test_alter_table_with_special_comment_restore() {
    let flags = tidb_ast::RestoreFlags::DEFAULT | tidb_ast::RestoreFlags::TIDB_SPECIAL_COMMENT;
    for (sql, expected) in [
        (
            "ALTER TABLE t PLACEMENT POLICY p1",
            "ALTER TABLE `t` /*T![placement] PLACEMENT POLICY = `p1` */",
        ),
        (
            "ALTER TABLE t PLACEMENT POLICY p1 COMMENT='aaa'",
            "ALTER TABLE `t` /*T![placement] PLACEMENT POLICY = `p1` */ COMMENT = 'aaa'",
        ),
        (
            "ALTER TABLE t PARTITION p0 PLACEMENT POLICY p1",
            "ALTER TABLE `t` /*T![placement] PARTITION `p0` PLACEMENT POLICY = `p1` */",
        ),
    ] {
        assert_eq!(
            parse(sql).expect("parse").restore_with_flags(flags),
            expected
        );
    }
}

macro_rules! ddl_payload {
    ($stmt:expr, $variant:ident) => {{
        let Stmt::Ddl(ddl) = $stmt else {
            panic!("expected DDL envelope")
        };
        let tidb_ast::DdlStmt::$variant(payload) = ddl.into_inner() else {
            panic!("expected {} payload", stringify!($variant))
        };
        payload
    }};
}

fn only_alter_action(statement: &tidb_ast::AlterTableStmt) -> AlterTableAction {
    let [action] = statement.actions.as_slice() else {
        panic!(
            "expected one ALTER TABLE action, got {}",
            statement.actions.len()
        );
    };
    action.clone()
}

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
    assert!(parse("split table t between (1) and (2) regions -1").is_err());
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
    assert!(parse("create database app default unsupported").is_err());
}

#[test]
fn test_alter_database_restore() {
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
fn create_table_columns() {
    // Column names, types (with args), and options are all captured.
    let stmt = parse("create table t (a int, b varchar(20), c decimal(10,2))").unwrap();
    let ct = ddl_payload!(stmt, CreateTable);
    assert_eq!(ct.name, vec!["t".to_string()]);
    let names: Vec<&str> = ct.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["a", "b", "c"]);
    assert_eq!(ct.columns[0].ty.name, "INT");
    assert!(ct.columns[0].ty.args.is_empty());
    assert_eq!(ct.columns[1].ty.name, "VARCHAR");
    assert_eq!(ct.columns[1].ty.args, vec![ColumnTypeArg::text("20")]);
    assert_eq!(ct.columns[2].ty.name, "DECIMAL");
    assert_eq!(
        ct.columns[2].ty.args,
        vec![ColumnTypeArg::text("10"), ColumnTypeArg::text("2")]
    );
    // Table-level constraints are not captured as columns.
    let stmt = parse("create table t (id int, name varchar(9), primary key (id))").unwrap();
    let ct = ddl_payload!(stmt, CreateTable);
    let names: Vec<&str> = ct.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["id", "name"]);
    // Column options: NOT NULL / PRIMARY KEY / AUTO_INCREMENT / DEFAULT.
    let stmt = parse(
            "create table if not exists t (id bigint primary key auto_increment, n int not null default 5)",
        )
        .unwrap();
    let ct = ddl_payload!(stmt, CreateTable);
    assert!(ct.if_not_exists);
    assert_eq!(
        ct.columns[0].options,
        vec![
            ColumnOption::InlineKey(InlineKeyOption::primary(None, false)),
            ColumnOption::AutoIncrement,
        ]
    );
    assert_eq!(ct.columns[1].options.len(), 2);
    assert_eq!(ct.columns[1].options[0], ColumnOption::NotNull);
    assert!(matches!(
        &ct.columns[1].options[1],
        ColumnOption::Default(Expr::Int(s)) if s == "5"
    ));
}

#[test]
fn binary_and_varbinary_column_types_restore_and_enforce_direct_grammar() {
    assert_eq!(
        r("create table t (a binary, b binary(16), c varbinary(255))"),
        "CREATE TABLE `t` (`a` BINARY,`b` BINARY(16),`c` VARBINARY(255))"
    );
    assert_eq!(
        r("alter table t modify c varbinary(8)"),
        "ALTER TABLE `t` MODIFY COLUMN `c` VARBINARY(8)"
    );

    // Direct binary declarations do not inherit the character-type modifier
    // grammar. Those `CHAR`/`VARCHAR ... BINARY` forms are a separate wave.
    assert!(parse("create table t (a varbinary)").is_err());
    assert!(parse("create table t (a binary(8, 1))").is_err());
    assert!(parse("create table t (a varbinary(8, 1))").is_err());
    assert!(parse("create table t (a binary unsigned)").is_err());
    assert!(parse("create table t (a varbinary(8) character set utf8)").is_err());
}

/// Go's hand-written field-type parser keeps byte-oriented BLOB spellings
/// separate from character-oriented TEXT spellings. The direct
/// `parseStringOptions` port now owns their binary/ASCII conversions, while
/// intrinsic BLOBs still reject a character-set clause.
#[test]
fn blob_and_text_family_column_types_restore_and_enforce_direct_grammar() {
    assert_eq!(
        r("create table t (a tinyblob, b blob(16), c mediumblob, d longblob, e tinytext, f mediumtext, g longtext)"),
        "CREATE TABLE `t` (`a` TINYBLOB,`b` BLOB(16),`c` MEDIUMBLOB,`d` LONGBLOB,`e` TINYTEXT,`f` MEDIUMTEXT,`g` LONGTEXT)"
    );
    assert_eq!(
        r("alter table t modify c mediumtext character set utf8mb4"),
        "ALTER TABLE `t` MODIFY COLUMN `c` MEDIUMTEXT CHARACTER SET UTF8MB4"
    );
    assert_eq!(
        r("create table t (a text byte,b longtext ascii,c mediumtext binary)"),
        "CREATE TABLE `t` (`a` BLOB,`b` LONGTEXT CHARACTER SET LATIN1,`c` MEDIUMTEXT BINARY)"
    );

    // Only ordinary BLOB/TEXT have Go's optional field length.  The wider
    // families and numeric modifiers are separate grammar, never generic
    // fallbacks.
    for sql in [
        "create table t (a tinyblob(1))",
        "create table t (a mediumblob(1))",
        "create table t (a longblob(1))",
        "create table t (a tinytext(1))",
        "create table t (a mediumtext(1))",
        "create table t (a longtext(1))",
        "create table t (a blob unsigned)",
        "create table t (a text zerofill)",
        "create table t (a blob character set utf8mb4)",
        "create table t (a tinyblob charset utf8mb4)",
    ] {
        assert!(parse(sql).is_err(), "must reject unrepresented form: {sql}");
    }
}

/// JSON is a dedicated TiDB field type, not an identifier-shaped fallback.
/// Its parser production sets binary collation internally and restore emits
/// the bare keyword (`pkg/parser/ddl_fieldtype_parser.go:243-249`;
/// `pkg/parser/types/field_type.go:671-672`).
#[test]
fn json_column_type_restore_and_scope() {
    assert_eq!(
        r("create table t (doc json)"),
        "CREATE TABLE `t` (`doc` JSON)"
    );
    assert_eq!(
        r("create table t (id int, doc json not null)"),
        "CREATE TABLE `t` (`id` INT,`doc` JSON NOT NULL)"
    );

    let stmt = parse("create table t (doc json)").expect("JSON column parses");
    let table = ddl_payload!(stmt, CreateTable);
    assert_eq!(table.columns[0].ty.name, "JSON");
    assert!(table.columns[0].ty.args.is_empty());
    assert!(!table.columns[0].ty.unsigned);
    assert!(!table.columns[0].ty.zerofill);

    // `JSON` has no type arguments or numeric modifiers in TiDB's dedicated
    // field-type production. Do not broaden adjacent type grammars here.
    assert!(parse("create table t (doc json(1))").is_err());
    assert!(parse("create table t (doc json unsigned)").is_err());
    assert!(parse("create table t (doc json character set utf8)").is_err());
    assert!(parse("create table t (doc json collate utf8mb4_bin)").is_err());
}

/// VECTOR is TiDB's Float32 vector field type. Its optional FLOAT/FLOAT4
/// element spelling is accepted but canonicalized away on restore; vector
/// indexes remain a separate AST/storage translation wave.
#[test]
fn vector_column_type_restore_and_scope() {
    assert_eq!(
        r("create table t (embedding vector)"),
        "CREATE TABLE `t` (`embedding` VECTOR)"
    );
    assert_eq!(
        r("create table t (embedding vector<float>(3))"),
        "CREATE TABLE `t` (`embedding` VECTOR(3))"
    );
    assert_eq!(
        r("alter table t modify embedding vector<float4>(16384)"),
        "ALTER TABLE `t` MODIFY COLUMN `embedding` VECTOR(16384)"
    );

    let stmt = parse("create table t (embedding vector(3))").expect("VECTOR column parses");
    let table = ddl_payload!(stmt, CreateTable);
    assert_eq!(table.columns[0].ty.name, "VECTOR");
    assert_eq!(table.columns[0].ty.args, [ColumnTypeArg::text("3")]);
    assert!(!table.columns[0].ty.unsigned);
    assert!(!table.columns[0].ty.zerofill);

    // VECTOR only has the Float32 element type and one optional dimension.
    // It has no character set or integer option grammar; COLLATE remains a
    // regular column option, as it does for TiDB's binary VECTOR FieldType.
    for sql in [
        "create table t (embedding vector<int>)",
        "create table t (embedding vector<double>)",
        "create table t (embedding vector<float8>)",
        "create table t (embedding vector<abc>)",
        "create table t (embedding vector(5)<float>)",
        "create table t (embedding vector(3, 4))",
        "create table t (embedding vector unsigned)",
        "create table t (embedding vector zerofill)",
        "create table t (embedding vector character set utf8mb4)",
    ] {
        assert!(parse(sql).is_err(), "must reject unrepresented form: {sql}");
    }
    assert_eq!(
        r("create table t (embedding vector collate utf8mb4_bin)"),
        "CREATE TABLE `t` (`embedding` VECTOR COLLATE utf8mb4_bin)"
    );
}

#[test]
fn alter_table_add_vector_index_preserves_expression_parts() {
    assert_eq!(
        r("alter table t add vector index ((vec_l2_distance(vec)))"),
        "ALTER TABLE `t` ADD VECTOR INDEX((VEC_L2_DISTANCE(`vec`)))"
    );
    let statement = parse("alter table t add vector index ((vec_cosine_distance(vec)))")
        .expect("ALTER VECTOR INDEX parses");
    let table = ddl_payload!(statement, AlterTable);
    assert!(matches!(
        only_alter_action(&table),
        tidb_ast::AlterTableAction::AddIndexConstraint(index)
            if index.kind == tidb_ast::IndexConstraintKind::Vector
                && matches!(index.parts.first(), Some(tidb_ast::IndexPart::Expr { .. }))
    ));
}

#[test]
fn create_table_like_preserves_the_reference_without_an_empty_column_list() {
    let stmt = parse("create table if not exists clone like source_schema.source").unwrap();
    let ct = ddl_payload!(stmt, CreateTable);
    assert_eq!(ct.name, vec!["clone"]);
    assert_eq!(
        ct.like_table,
        Some(vec!["source_schema".to_string(), "source".to_string()])
    );
    assert!(ct.columns.is_empty());
    assert!(ct.table_constraints.is_empty());
    assert!(ct.table_options.is_empty());
    assert_eq!(
        r("create table if not exists clone like source_schema.source"),
        "CREATE TABLE IF NOT EXISTS `clone` LIKE `source_schema`.`source`"
    );
    assert_eq!(
        r("create temporary table clone like source"),
        "CREATE TEMPORARY TABLE `clone` LIKE `source`"
    );

    assert_eq!(
        r("create global temporary table clone like source on commit delete rows"),
        "CREATE GLOBAL TEMPORARY TABLE `clone` LIKE `source` ON COMMIT DELETE ROWS"
    );
}

#[test]
fn create_table_restore() {
    // Restore matches the real Go parser byte-for-byte (verified via
    // `godump restore`): uppercase keywords, back-quoted names, no space
    // after the comma between type args or between columns.
    assert_eq!(
        parse("create table t (id int, name varchar(20))")
            .unwrap()
            .restore(),
        "CREATE TABLE `t` (`id` INT,`name` VARCHAR(20))"
    );
    assert_eq!(
        parse("create table t (id int not null, name varchar(20) not null default 'x')")
            .unwrap()
            .restore(),
        "CREATE TABLE `t` (`id` INT NOT NULL,`name` VARCHAR(20) NOT NULL DEFAULT _UTF8MB4'x')"
    );
    assert_eq!(
        parse("create table t (id bigint primary key auto_increment, amt decimal(10,2))")
            .unwrap()
            .restore(),
        "CREATE TABLE `t` (`id` BIGINT PRIMARY KEY AUTO_INCREMENT,`amt` DECIMAL(10,2))"
    );
    assert_eq!(
        parse("create table if not exists t (id int, v text)")
            .unwrap()
            .restore(),
        "CREATE TABLE IF NOT EXISTS `t` (`id` INT,`v` TEXT)"
    );
    assert_eq!(
        parse("create table t (id int, name char(1) null)")
            .unwrap()
            .restore(),
        "CREATE TABLE `t` (`id` INT,`name` CHAR(1) NULL)"
    );
    assert_eq!(
        parse("create table t (id int default 5, flag int not null)")
            .unwrap()
            .restore(),
        "CREATE TABLE `t` (`id` INT DEFAULT 5,`flag` INT NOT NULL)"
    );
    // A table-level composite PRIMARY KEY constraint.
    assert_eq!(
        parse("create table t (a int, b int, primary key (a, b))")
            .unwrap()
            .restore(),
        "CREATE TABLE `t` (`a` INT,`b` INT,PRIMARY KEY(`a`, `b`))"
    );
    assert_eq!(
        parse("create table t (a int, primary key (a))")
            .unwrap()
            .restore(),
        "CREATE TABLE `t` (`a` INT,PRIMARY KEY(`a`))"
    );
}

#[test]
fn create_table_nonreserved_keyword_column_names() {
    // Go's parser accepts a non-reserved keyword as a bare column name. This
    // is the source regression shape in pkg/parser/parser_test.go:2428.
    let sql = "create table MergeContextTest$Simple (value integer not null, status tinyint, primary key (value))";
    let stmt = parse(sql).unwrap();
    assert_eq!(
        stmt.restore(),
        "CREATE TABLE `MergeContextTest$Simple` (`value` INT NOT NULL,`status` TINYINT,PRIMARY KEY(`value`))"
    );
    let ct = ddl_payload!(stmt, CreateTable);
    let names: Vec<&str> = ct
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    assert_eq!(names, vec!["value", "status"]);

    // KEY remains a table-constraint starter rather than being reclassified
    // as a non-reserved column name.
    assert_eq!(
        r("create table t (value int, key idx_value (value))"),
        "CREATE TABLE `t` (`value` INT,INDEX `idx_value`(`value`))"
    );
    // A reserved token that is neither a supported constraint starter nor a
    // column name must fail rather than being skipped and erased.
    assert!(parse("create table t (a int, select int)").is_err());
}

#[test]
fn create_local_temporary_table_restore() {
    // Local TEMPORARY is a CREATE TABLE prefix and retains the same supported
    // body grammar, including IF NOT EXISTS and basic secondary indexes.
    assert_eq!(
        r("create temporary table tmp (a int, key idx_a (a))"),
        "CREATE TEMPORARY TABLE `tmp` (`a` INT,INDEX `idx_a`(`a`))"
    );
    assert_eq!(
        r("create temporary table if not exists tmp (a int)"),
        "CREATE TEMPORARY TABLE IF NOT EXISTS `tmp` (`a` INT)"
    );
    let stmt = ddl_payload!(
        parse("create temporary table tmp (a int)").unwrap(),
        CreateTable
    );
    assert_eq!(stmt.temporary, tidb_ast::CreateTableTemporary::Local);
    assert_eq!(
        r("create temporary table tmp like source_t"),
        "CREATE TEMPORARY TABLE `tmp` LIKE `source_t`"
    );
    assert_eq!(
        r("create global temporary table tmp (a int) on commit delete rows"),
        "CREATE GLOBAL TEMPORARY TABLE `tmp` (`a` INT) ON COMMIT DELETE ROWS"
    );
    assert_eq!(
        r("create global temporary table tmp (a int) on commit preserve rows"),
        "CREATE GLOBAL TEMPORARY TABLE `tmp` (`a` INT) ON COMMIT PRESERVE ROWS"
    );
    let stmt = ddl_payload!(
        parse("create global temporary table tmp (a int) on commit preserve rows").unwrap(),
        CreateTable
    );
    assert_eq!(stmt.temporary, tidb_ast::CreateTableTemporary::Global);
    assert!(!stmt.on_commit_delete);
    // Missing ON COMMIT remains invalid in the direct grammar. CTAS is a
    // shared `CreateTableStmt` result-set payload and is covered by
    // `ctas_source` for both ordinary and temporary table prefixes.
    for sql in [
        "create global temporary table tmp (a int)",
        "create temporary table tmp (a int) on commit delete rows",
    ] {
        assert!(parse(sql).is_err(), "unexpectedly accepted: {sql}");
    }
}

#[test]
fn create_table_composite_primary_key() {
    let stmt = parse("create table t (a int, b int, primary key (a, b))").unwrap();
    let ct = ddl_payload!(stmt, CreateTable);
    assert!(matches!(
        ct.table_constraints.as_slice(),
        [TableConstraint::Index(index)]
            if index.kind == tidb_ast::IndexConstraintKind::PrimaryKey
                && index.name.is_none()
                && index.parts == plain_key_parts(&["a", "b"])
    ));
    // A column-level PRIMARY KEY leaves the table-level constraint list
    // empty.
    let stmt = parse("create table t (id int primary key)").unwrap();
    let ct = ddl_payload!(stmt, CreateTable);
    assert_eq!(ct.table_constraints, vec![]);
    assert_eq!(
        ct.columns[0].options,
        vec![ColumnOption::InlineKey(InlineKeyOption::primary(
            None, false
        ))]
    );
}

#[test]
fn create_table_unique_key() {
    // Both `UNIQUE` and `UNIQUE KEY` parse to the same column option.
    for sql in [
        "create table t (id int primary key, email varchar(9) unique)",
        "create table t (id int primary key, email varchar(9) unique key)",
    ] {
        let stmt = parse(sql).unwrap();
        let ct = ddl_payload!(stmt, CreateTable);
        assert_eq!(
            ct.columns[1].options,
            vec![ColumnOption::InlineKey(InlineKeyOption::unique(false))]
        );
    }
    // A table-level composite UNIQUE constraint.
    let stmt = parse("create table t (a int, b int, unique key (a, b))").unwrap();
    let ct = ddl_payload!(stmt, CreateTable);
    assert!(matches!(
        ct.table_constraints.as_slice(),
        [TableConstraint::Index(index)]
            if index.kind == tidb_ast::IndexConstraintKind::Unique
                && index.name.is_none()
                && index.parts == plain_key_parts(&["a", "b"])
    ));
}

#[test]
fn create_table_named_constraints() {
    // `CONSTRAINT name` before PRIMARY KEY/UNIQUE names the constraint;
    // an inline name (no CONSTRAINT prefix) works the same way.
    let stmt = parse("create table t (a int, b int, constraint pk_ab primary key (a, b))").unwrap();
    let ct = ddl_payload!(stmt, CreateTable);
    assert!(matches!(
        ct.table_constraints.as_slice(),
        [TableConstraint::Index(index)]
            if index.kind == tidb_ast::IndexConstraintKind::PrimaryKey
                && index.name.as_deref() == Some("pk_ab")
                && index.parts == plain_key_parts(&["a", "b"])
    ));
    let stmt = parse("create table t (a int, b int, unique key idx_ab (a, b))").unwrap();
    let ct = ddl_payload!(stmt, CreateTable);
    assert!(matches!(
        ct.table_constraints.as_slice(),
        [TableConstraint::Index(index)]
            if index.kind == tidb_ast::IndexConstraintKind::Unique
                && index.name.as_deref() == Some("idx_ab")
                && index.parts == plain_key_parts(&["a", "b"])
    ));
    // A `CONSTRAINT` name wins over an inline index name when both are
    // given, matching the Go AST (confirmed via godump, not assumed).
    assert_eq!(
        r("create table t (a int, b int, constraint cn1 unique key idx1 (a, b))"),
        "CREATE TABLE `t` (`a` INT,`b` INT,UNIQUE `cn1`(`a`, `b`))"
    );
    // `CONSTRAINT` with no name (and no inline name) behaves as if
    // absent.
    assert_eq!(
        r("create table t (a int, constraint primary key (a))"),
        "CREATE TABLE `t` (`a` INT,PRIMARY KEY(`a`))"
    );
}

#[test]
fn create_table_secondary_index_restore() {
    // A table-level KEY/INDEX is a non-unique secondary index. Go restores
    // both spellings as INDEX and retains the declaration order alongside
    // the other table constraints.
    let stmt =
        parse("create table t (a int, b int, key idx_ab (a, b), primary key (a), index (b))")
            .unwrap();
    let ct = ddl_payload!(stmt, CreateTable);
    assert!(matches!(
        ct.table_constraints.as_slice(),
        [TableConstraint::Index(first), TableConstraint::Index(primary), TableConstraint::Index(last)]
            if first.kind == tidb_ast::IndexConstraintKind::Index
                && first.name.as_deref() == Some("idx_ab")
                && first.parts == plain_key_parts(&["a", "b"])
                && primary.kind == tidb_ast::IndexConstraintKind::PrimaryKey
                && primary.parts == plain_key_parts(&["a"])
                && last.kind == tidb_ast::IndexConstraintKind::Index
                && last.parts == plain_key_parts(&["b"])
    ));
    assert_eq!(
        r("create table t (a int, b int, key idx_ab (a, b), primary key (a), index (b))"),
        "CREATE TABLE `t` (`a` INT,`b` INT,INDEX `idx_ab`(`a`, `b`),PRIMARY KEY(`a`),INDEX(`b`))"
    );
    // CREATE TABLE and ALTER TABLE share the same source option overwrite
    // semantics for the currently representable secondary-index subset.
    assert_eq!(
        r("create table t (a int, index idx(a) comment 'old' comment 'new' global local invisible invisible where a > 1 where a > 2)"),
        "CREATE TABLE `t` (`a` INT,INDEX `idx`(`a`) COMMENT 'new' INVISIBLE WHERE `a`>2)"
    );
}

#[test]
fn create_table_index_constraint_options_and_kinds_match_go() {
    // All index-bearing constraint kinds now share the source-shaped option
    // envelope. The only remaining rejects here are unsupported GLOBAL/LOCAL
    // prefix forms, which are not Go parseConstraint productions.
    for (sql, expected) in [
        (
            "create table t (a int, key idx using btree (a))",
            "CREATE TABLE `t` (`a` INT,INDEX `idx`(`a`) USING BTREE)",
        ),
        (
            "create table t (a int, key idx (a) using btree)",
            "CREATE TABLE `t` (`a` INT,INDEX `idx`(`a`) USING BTREE)",
        ),
        (
            "create table t (a int, columnar index idx (a))",
            "CREATE TABLE `t` (`a` INT,COLUMNAR INDEX `idx`(`a`))",
        ),
        (
            "create table t (a int, key idx (a) clustered)",
            "CREATE TABLE `t` (`a` INT,INDEX `idx`(`a`) CLUSTERED)",
        ),
        (
            "create table t (a int, unique key idx (a) using btree)",
            "CREATE TABLE `t` (`a` INT,UNIQUE `idx`(`a`) USING BTREE)",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }
    for sql in [
        "create table t (a int, global index idx (a))",
        "create table t (a int, local index idx (a))",
    ] {
        assert!(parse(sql).is_err(), "unexpectedly accepted: {sql}");
    }
}

#[test]
fn create_table_vector_index_preserves_expression_parts() {
    // Direct from `tests/clusterintegrationtest/t/vector.test`: Go retains a
    // VECTOR INDEX as a distinct constraint and restores its functional key
    // part with the outer double parentheses.
    let stmt = parse("create table t (vec vector(3), vector index ((vec_l2_distance(vec))))")
        .expect("VECTOR INDEX parses");
    let ct = ddl_payload!(stmt, CreateTable);
    let TableConstraint::Index(index) = &ct.table_constraints[0] else {
        panic!("expected vector-index constraint")
    };
    assert_eq!(index.kind, tidb_ast::IndexConstraintKind::Vector);
    assert!(!index.if_not_exists);
    assert_eq!(index.parts.len(), 1);
    assert!(matches!(index.parts[0], tidb_ast::IndexPart::Expr { .. }));
    assert_eq!(
        r("create table t (vec vector(3), vector index ((vec_l2_distance(vec))))"),
        "CREATE TABLE `t` (`vec` VECTOR(3),VECTOR INDEX((VEC_L2_DISTANCE(`vec`))))"
    );
}

#[test]
fn create_table_check_constraint() {
    // No `[NOT] ENFORCED` written defaults to `ENFORCED` on restore,
    // matching the Go AST.
    let stmt = parse("create table t (a int, check (a > 0))").unwrap();
    let ct = ddl_payload!(stmt, CreateTable);
    let TableConstraint::Check(check) = &ct.table_constraints[0] else {
        panic!("expected a Check constraint")
    };
    assert_eq!(check.name, None);
    assert!(check.enforced);
    assert_eq!(
        r("create table t (a int, check (a > 0))"),
        "CREATE TABLE `t` (`a` INT,CHECK(`a`>0) ENFORCED)"
    );
    assert_eq!(
        r("create table t (a int, constraint ck1 check (a > 0))"),
        "CREATE TABLE `t` (`a` INT,CONSTRAINT `ck1` CHECK(`a`>0) ENFORCED)"
    );
    assert_eq!(
        r("create table t (a int, check (a > 0) not enforced)"),
        "CREATE TABLE `t` (`a` INT,CHECK(`a`>0) NOT ENFORCED)"
    );
    assert_eq!(
        r("create table t (a int, check (a > 0) enforced)"),
        "CREATE TABLE `t` (`a` INT,CHECK(`a`>0) ENFORCED)"
    );
    // A `CONSTRAINT` with no name (and no inline name — `CHECK` has none)
    // behaves as if absent.
    assert_eq!(
        r("create table t (a int, constraint check (a > 0))"),
        "CREATE TABLE `t` (`a` INT,CHECK(`a`>0) ENFORCED)"
    );
    // Table-level constraints restore in WRITTEN order (unlike a fixed
    // canonical order), confirmed via `godump restore` in both
    // directions: PRIMARY KEY/CHECK/UNIQUE, and the reverse.
    assert_eq!(
        r("create table t (a int, b int, primary key(a), check (a > 0), unique(b))"),
        "CREATE TABLE `t` (`a` INT,`b` INT,PRIMARY KEY(`a`),CHECK(`a`>0) ENFORCED,UNIQUE(`b`))"
    );
    assert_eq!(
        r("create table t (a int, b int, check (a > 0), unique(b), primary key(a))"),
        "CREATE TABLE `t` (`a` INT,`b` INT,CHECK(`a`>0) ENFORCED,UNIQUE(`b`),PRIMARY KEY(`a`))"
    );
}

#[test]
fn create_table_foreign_key() {
    // Unlike PRIMARY KEY/UNIQUE/CHECK, `CONSTRAINT` ALWAYS restores
    // here, even with no name written — a real asymmetry confirmed via
    // `godump restore`, not assumed.
    let stmt =
        parse("create table child (id int, pid int, foreign key (pid) references parent(id))")
            .unwrap();
    let ct = ddl_payload!(stmt, CreateTable);
    let TableConstraint::ForeignKey(fk) = &ct.table_constraints[0] else {
        panic!("expected a ForeignKey constraint")
    };
    assert_eq!(fk.name, None);
    assert_eq!(fk.parts, plain_key_parts(&["pid"]));
    assert_eq!(fk.reference.table, Some(vec!["parent".to_string()]));
    assert_eq!(fk.reference.parts, Some(plain_key_parts(&["id"])));
    assert_eq!(fk.reference.on_delete, None);
    assert_eq!(fk.reference.on_update, None);
    assert_eq!(
            r("create table child (id int, pid int, foreign key (pid) references parent(id))"),
            "CREATE TABLE `child` (`id` INT,`pid` INT,CONSTRAINT FOREIGN KEY (`pid`) REFERENCES `parent`(`id`))"
        );
    assert_eq!(
            r("create table child (id int, pid int, constraint fk1 foreign key (pid) references parent(id))"),
            "CREATE TABLE `child` (`id` INT,`pid` INT,CONSTRAINT `fk1` FOREIGN KEY (`pid`) REFERENCES `parent`(`id`))"
        );
    // An inline name (no `CONSTRAINT` prefix) works the same way.
    assert_eq!(
            r("create table child (id int, pid int, foreign key fk_pid (pid) references parent(id))"),
            "CREATE TABLE `child` (`id` INT,`pid` INT,CONSTRAINT `fk_pid` FOREIGN KEY (`pid`) REFERENCES `parent`(`id`))"
        );
    // Composite FK columns, and a db-qualified referenced table.
    assert_eq!(
            r("create table child (id int, a int, b int, foreign key (a, b) references parent(x, y))"),
            "CREATE TABLE `child` (`id` INT,`a` INT,`b` INT,CONSTRAINT FOREIGN KEY (`a`, `b`) REFERENCES `parent`(`x`, `y`))"
        );
    assert_eq!(
            r("create table child (id int, pid int, foreign key (pid) references db1.parent(id))"),
            "CREATE TABLE `child` (`id` INT,`pid` INT,CONSTRAINT FOREIGN KEY (`pid`) REFERENCES `db1`.`parent`(`id`))"
        );
    // Every ON DELETE/ON UPDATE action.
    for (src, want) in [
        ("on delete cascade", "ON DELETE CASCADE"),
        ("on delete set null", "ON DELETE SET NULL"),
        ("on delete restrict", "ON DELETE RESTRICT"),
        ("on delete no action", "ON DELETE NO ACTION"),
        ("on delete no", "ON DELETE NO ACTION"),
        ("on delete set default", "ON DELETE SET DEFAULT"),
        ("on update cascade", "ON UPDATE CASCADE"),
    ] {
        assert_eq!(
                r(&format!(
                    "create table child (id int, pid int, foreign key (pid) references parent(id) {src})"
                )),
                format!(
                    "CREATE TABLE `child` (`id` INT,`pid` INT,CONSTRAINT FOREIGN KEY (`pid`) REFERENCES `parent`(`id`) {want})"
                )
            );
    }
    // ON DELETE always restores BEFORE ON UPDATE, regardless of which
    // was written first (confirmed via `godump restore`, not assumed).
    assert_eq!(
            r("create table child (id int, pid int, foreign key (pid) references parent(id) on update set null on delete cascade)"),
            "CREATE TABLE `child` (`id` INT,`pid` INT,CONSTRAINT FOREIGN KEY (`pid`) REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE SET NULL)"
        );
    assert_eq!(
        r("create table child (id int, pid int, foreign key (pid) references parent(id) match full)"),
        "CREATE TABLE `child` (`id` INT,`pid` INT,CONSTRAINT FOREIGN KEY (`pid`) REFERENCES `parent`(`id`) MATCH FULL)"
    );
    assert_eq!(
        r("create table child (id int, pid int, foreign key (pid) references parent)"),
        "CREATE TABLE `child` (`id` INT,`pid` INT,CONSTRAINT FOREIGN KEY (`pid`) REFERENCES `parent`)"
    );
    assert!(parse("create table child (id int, pid int, foreign key (pid) references parent(id) on delete cascade on delete restrict)").is_err());
}

#[test]
fn create_table_charset_collate() {
    // `CHARACTER SET` canonicalizes to uppercase; `CHARSET` is an alias
    // that restores identically.
    let stmt = parse("create table t (a varchar(20) character set utf8mb4)").unwrap();
    let ct = ddl_payload!(stmt, CreateTable);
    assert_eq!(ct.columns[0].ty.charset, Some("UTF8MB4".to_string()));
    assert_eq!(
        r("create table t (a varchar(20) charset utf8mb4)"),
        "CREATE TABLE `t` (`a` VARCHAR(20) CHARACTER SET UTF8MB4)"
    );
    // Input case doesn't matter: charset always uppercases.
    assert_eq!(
        r("create table t (a varchar(20) character set UTF8MB4)"),
        "CREATE TABLE `t` (`a` VARCHAR(20) CHARACTER SET UTF8MB4)"
    );
    // COLLATE canonicalizes to lowercase — the opposite convention.
    let stmt = parse("create table t (a varchar(20) collate UTF8MB4_BIN)").unwrap();
    let ct = ddl_payload!(stmt, CreateTable);
    assert_eq!(
        ct.columns[0].options,
        vec![ColumnOption::Collate("utf8mb4_bin".to_string())]
    );
    // CHARACTER SET followed by COLLATE, together right after the type.
    assert_eq!(
        r("create table t (a varchar(20) character set utf8mb4 collate utf8mb4_bin)"),
        "CREATE TABLE `t` (`a` VARCHAR(20) CHARACTER SET UTF8MB4 COLLATE utf8mb4_bin)"
    );
    // COLLATE is positionally free relative to other options, unlike
    // CHARACTER SET (which must immediately follow the type).
    assert_eq!(
        r("create table t (a varchar(20) not null collate utf8mb4_bin)"),
        "CREATE TABLE `t` (`a` VARCHAR(20) NOT NULL COLLATE utf8mb4_bin)"
    );
    assert_eq!(
        r("create table t (a varchar(20) collate utf8mb4_bin not null)"),
        "CREATE TABLE `t` (`a` VARCHAR(20) COLLATE utf8mb4_bin NOT NULL)"
    );
    // A charset name that lexes as a keyword (ASCII, BINARY, ...) still
    // parses.
    assert_eq!(
        r("create table t (a varchar(20) character set ascii)"),
        "CREATE TABLE `t` (`a` VARCHAR(20) CHARACTER SET ASCII)"
    );
    // CHARACTER SET after another option is a real MySQL grammar error,
    // not silently accepted.
    assert!(parse("create table t (a varchar(20) not null character set utf8mb4)").is_err());
}

#[test]
fn create_table_date_time_types() {
    // Dates are plain string literals to this parser (no special
    // date-literal syntax), so DEFAULT '...' reuses the existing
    // string-literal DEFAULT parsing, unchanged.
    let stmt = parse("create table t (a date, b datetime, c time, d timestamp, e year)").unwrap();
    let ct = ddl_payload!(stmt, CreateTable);
    let names: Vec<&str> = ct.columns.iter().map(|c| c.ty.name.as_str()).collect();
    assert_eq!(names, vec!["DATE", "DATETIME", "TIME", "TIMESTAMP", "YEAR"]);
    assert_eq!(
        r("create table t (a datetime(3), b time(6), c timestamp(2))"),
        "CREATE TABLE `t` (`a` DATETIME(3),`b` TIME(6),`c` TIMESTAMP(2))"
    );
    assert_eq!(
        r("create table t (a date not null default '2021-01-01')"),
        "CREATE TABLE `t` (`a` DATE NOT NULL DEFAULT _UTF8MB4'2021-01-01')"
    );
}

#[test]
fn create_table_unsigned_zerofill() {
    let stmt = parse("create table t (a int unsigned, b int zerofill)").unwrap();
    let ct = ddl_payload!(stmt, CreateTable);
    assert!(ct.columns[0].ty.unsigned);
    assert!(!ct.columns[0].ty.zerofill);
    // ZEROFILL implies UNSIGNED even when only ZEROFILL was written.
    assert!(ct.columns[1].ty.unsigned);
    assert!(ct.columns[1].ty.zerofill);

    // TINYINT/SMALLINT/MEDIUMINT/FLOAT/DOUBLE are recognized types.
    let stmt =
        parse("create table t (a tinyint, b smallint, c mediumint, d float, e double)").unwrap();
    let ct = ddl_payload!(stmt, CreateTable);
    let names: Vec<&str> = ct.columns.iter().map(|c| c.ty.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["TINYINT", "SMALLINT", "MEDIUMINT", "FLOAT", "DOUBLE"]
    );
}

#[test]
fn create_table_comment() {
    let stmt =
        parse("create table t (id int comment 'the id', name varchar(9) not null comment 'nm')")
            .unwrap();
    let ct = ddl_payload!(stmt, CreateTable);
    assert_eq!(
        ct.columns[0].options,
        vec![ColumnOption::Comment("the id".to_string())]
    );
    assert_eq!(
        ct.columns[1].options,
        vec![
            ColumnOption::NotNull,
            ColumnOption::Comment("nm".to_string())
        ]
    );
    // COMMENT text restores as a plain string, unlike DEFAULT's
    // `_UTF8MB4`-prefixed literals.
    assert_eq!(
        r("create table t (id int comment 'it''s')"),
        "CREATE TABLE `t` (`id` INT COMMENT 'it''s')"
    );
    assert_eq!(
        r("create table t (v varchar(9) default 'x' comment 'c')"),
        "CREATE TABLE `t` (`v` VARCHAR(9) DEFAULT _UTF8MB4'x' COMMENT 'c')"
    );

    // Table-level COMMENT: captured regardless of whether `=` was
    // written, and always restores WITH `=` (spaced).
    let stmt = parse("create table t (id int) comment 'a table comment'").unwrap();
    let ct = ddl_payload!(stmt, CreateTable);
    assert_eq!(
        ct.table_options,
        vec![TableOption::Comment("a table comment".to_string())]
    );
    assert_eq!(
        r("create table t (id int) comment 'no equals'"),
        "CREATE TABLE `t` (`id` INT) COMMENT = 'no equals'"
    );
    assert_eq!(
        r("create table t (id int) comment='with equals'"),
        "CREATE TABLE `t` (`id` INT) COMMENT = 'with equals'"
    );
    // No table options at all: the list stays empty and restore omits it.
    let stmt = parse("create table t (id int)").unwrap();
    let ct = ddl_payload!(stmt, CreateTable);
    assert_eq!(ct.table_options, vec![]);
}

#[test]
fn create_table_options() {
    // ENGINE preserves its exact written case (MySQL/TiDB never
    // canonicalize it), while CHARACTER SET/CHARSET/COLLATE uppercase
    // and gain a `DEFAULT` prefix even when not written — all confirmed
    // via `godump restore`, not assumed.
    assert_eq!(
        r("create table t (a int) engine=innodb"),
        "CREATE TABLE `t` (`a` INT) ENGINE = innodb"
    );
    assert_eq!(
        r("create table t (a int) engine = InnoDB"),
        "CREATE TABLE `t` (`a` INT) ENGINE = InnoDB"
    );
    // A quoted engine name is equally accepted and restores the same as
    // the bare-word form.
    assert_eq!(
        r("create table t (a int) engine='InnoDB'"),
        "CREATE TABLE `t` (`a` INT) ENGINE = InnoDB"
    );
    assert_eq!(
        r("create table t (a int) charset=utf8mb4"),
        "CREATE TABLE `t` (`a` INT) DEFAULT CHARACTER SET = UTF8MB4"
    );
    assert_eq!(
        r("create table t (a int) character set utf8mb4"),
        "CREATE TABLE `t` (`a` INT) DEFAULT CHARACTER SET = UTF8MB4"
    );
    assert_eq!(
        r("create table t (a int) default character set = utf8mb4"),
        "CREATE TABLE `t` (`a` INT) DEFAULT CHARACTER SET = UTF8MB4"
    );
    // Table-level COLLATE uppercases — the OPPOSITE case convention
    // from a column's own COLLATE option, which lowercases.
    assert_eq!(
        r("create table t (a int) collate=utf8mb4_bin"),
        "CREATE TABLE `t` (`a` INT) DEFAULT COLLATE = UTF8MB4_BIN"
    );
    assert_eq!(
        r("create table t (a int) auto_increment=100"),
        "CREATE TABLE `t` (`a` INT) AUTO_INCREMENT = 100"
    );
    assert_eq!(
        r("create table t (a int) auto_increment 100"),
        "CREATE TABLE `t` (`a` INT) AUTO_INCREMENT = 100"
    );
    // Multiple options restore in WRITTEN order, not a fixed canonical
    // order (confirmed via `godump restore` on a reversed ordering too).
    assert_eq!(
        r("create table t (a int) engine=InnoDB auto_increment=100 \
                 default charset=utf8mb4 collate=utf8mb4_bin comment='hi'"),
        "CREATE TABLE `t` (`a` INT) ENGINE = InnoDB AUTO_INCREMENT = 100 \
             DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_BIN COMMENT = 'hi'"
    );
    assert_eq!(
        r("create table t (a int) comment='hi' collate=utf8mb4_bin \
                 charset=utf8mb4 auto_increment=100 engine=InnoDB"),
        "CREATE TABLE `t` (`a` INT) COMMENT = 'hi' DEFAULT COLLATE = UTF8MB4_BIN \
             DEFAULT CHARACTER SET = UTF8MB4 AUTO_INCREMENT = 100 ENGINE = InnoDB"
    );
    // A `,` between table options is accepted and dropped on restore.
    assert_eq!(
        r("create table t (a int) engine=MyISAM, auto_increment=5"),
        "CREATE TABLE `t` (`a` INT) ENGINE = MyISAM AUTO_INCREMENT = 5"
    );
    // ROW_FORMAT uppercases (same convention as ENGINE/CHARACTER SET/
    // COLLATE); the `=` is optional, same as every other option here.
    assert_eq!(
        r("create table t (a int) row_format=dynamic engine=InnoDB"),
        "CREATE TABLE `t` (`a` INT) ROW_FORMAT = DYNAMIC ENGINE = InnoDB"
    );
    assert_eq!(
        r("create table t (a int) row_format Compact"),
        "CREATE TABLE `t` (`a` INT) ROW_FORMAT = COMPACT"
    );
    assert_eq!(
        r("create table t (a int) key_block_size=8"),
        "CREATE TABLE `t` (`a` INT) KEY_BLOCK_SIZE = 8"
    );
    assert_eq!(
        r("create table t (a int) key_block_size 16"),
        "CREATE TABLE `t` (`a` INT) KEY_BLOCK_SIZE = 16"
    );
    // COMPRESSION, unlike ROW_FORMAT, preserves its string value's
    // case verbatim rather than uppercasing.
    assert_eq!(
        r("create table t (a int) compression='zlib'"),
        "CREATE TABLE `t` (`a` INT) COMPRESSION = 'zlib'"
    );
    assert_eq!(
        r("create table t (a int) compression 'ZLIB'"),
        "CREATE TABLE `t` (`a` INT) COMPRESSION = 'ZLIB'"
    );
    // TABLESPACE restores as a backtick-quoted identifier, the only
    // table option here that does.
    assert_eq!(
        r("create table t (a int) tablespace ts1"),
        "CREATE TABLE `t` (`a` INT) TABLESPACE = `ts1`"
    );
    assert_eq!(
        r("create table t (a int) tablespace=ts2"),
        "CREATE TABLE `t` (`a` INT) TABLESPACE = `ts2`"
    );
    // All four combine with the previously-modelled options and each
    // other, still restoring in WRITTEN order.
    assert_eq!(
        r("create table t (a int) row_format=dynamic key_block_size=4 comment='x'"),
        "CREATE TABLE `t` (`a` INT) ROW_FORMAT = DYNAMIC KEY_BLOCK_SIZE = 4 COMMENT = 'x'"
    );
    assert_eq!(
        r("create table t (a int) storage disk engine=InnoDB"),
        "CREATE TABLE `t` (`a` INT) STORAGE DISK ENGINE = InnoDB"
    );
    // `SHARD_ROW_ID_BITS`/`PRE_SPLIT_REGIONS`/`AUTO_ID_CACHE` are
    // TiDB-specific extensions, but restore as plain `KEYWORD = value`
    // with NO special-comment wrapping (confirmed via `godump restore`
    // -- this project's own oracle uses `format.DefaultRestoreFlags`,
    // not TiDB's own feature-ID special-comment mode).
    assert_eq!(
        r("create table t (a int) shard_row_id_bits = 4"),
        "CREATE TABLE `t` (`a` INT) SHARD_ROW_ID_BITS = 4"
    );
    assert_eq!(
        r("create table t (a int) shard_row_id_bits 4"),
        "CREATE TABLE `t` (`a` INT) SHARD_ROW_ID_BITS = 4"
    );
    assert_eq!(
        r("create table t (a int) pre_split_regions = 2"),
        "CREATE TABLE `t` (`a` INT) PRE_SPLIT_REGIONS = 2"
    );
    assert_eq!(
        r("create table t (a int) auto_id_cache = 100"),
        "CREATE TABLE `t` (`a` INT) AUTO_ID_CACHE = 100"
    );
    // `MAX_ROWS`/`MIN_ROWS`/`AVG_ROW_LENGTH`/`CHECKSUM`/`DELAY_KEY_WRITE`
    // are plain MySQL compatibility options, same restore convention.
    assert_eq!(
        r("create table t (a int) max_rows = 1000"),
        "CREATE TABLE `t` (`a` INT) MAX_ROWS = 1000"
    );
    assert_eq!(
        r("create table t (a int) min_rows = 10"),
        "CREATE TABLE `t` (`a` INT) MIN_ROWS = 10"
    );
    assert_eq!(
        r("create table t (a int) avg_row_length = 100"),
        "CREATE TABLE `t` (`a` INT) AVG_ROW_LENGTH = 100"
    );
    assert_eq!(
        r("create table t (a int) checksum = 1"),
        "CREATE TABLE `t` (`a` INT) CHECKSUM = 1"
    );
    assert_eq!(
        r("create table t (a int) delay_key_write = 1"),
        "CREATE TABLE `t` (`a` INT) DELAY_KEY_WRITE = 1"
    );
    // All 8 combine with the previously-modelled options, still
    // restoring in WRITTEN order.
    assert_eq!(
        r(
            "create table t (a int) engine = innodb shard_row_id_bits = 4, \
                 pre_split_regions = 2 comment = 'x'"
        ),
        "CREATE TABLE `t` (`a` INT) ENGINE = innodb SHARD_ROW_ID_BITS = 4 \
             PRE_SPLIT_REGIONS = 2 COMMENT = 'x'"
    );
    // `STATS_PERSISTENT`/`PACK_KEYS`: the parsed value (`DEFAULT`, `0`,
    // or `1`) is REQUIRED but entirely DISCARDED on restore -- real
    // TiDB's own `Restore()` always emits this exact FIXED string
    // (including a genuine trailing space baked into the comment
    // itself, confirmed via a byte-level `godump restore` check)
    // regardless of what was parsed.
    assert_eq!(
        r("create table t (a int) stats_persistent = 0"),
        "CREATE TABLE `t` (`a` INT) STATS_PERSISTENT = DEFAULT \
             /* TableOptionStatsPersistent is not supported */ "
    );
    assert_eq!(
        r("create table t (a int) stats_persistent default"),
        "CREATE TABLE `t` (`a` INT) STATS_PERSISTENT = DEFAULT \
             /* TableOptionStatsPersistent is not supported */ "
    );
    assert_eq!(
        r("create table t (a int) pack_keys = 1"),
        "CREATE TABLE `t` (`a` INT) PACK_KEYS = DEFAULT \
             /* TableOptionPackKeys is not supported */ "
    );
    // Combined with another option: the trailing space baked into the
    // comment PLUS the normal inter-option separator space together
    // produce a real DOUBLE space before whatever follows.
    assert_eq!(
        r("create table t (a int) stats_persistent=0 comment='x'"),
        "CREATE TABLE `t` (`a` INT) STATS_PERSISTENT = DEFAULT \
             /* TableOptionStatsPersistent is not supported */  COMMENT = 'x'"
    );
    assert_eq!(
        r("create table t (a int) stats_persistent = 0, pack_keys = 1"),
        "CREATE TABLE `t` (`a` INT) STATS_PERSISTENT = DEFAULT \
             /* TableOptionStatsPersistent is not supported */  PACK_KEYS = DEFAULT \
             /* TableOptionPackKeys is not supported */ "
    );
    // A value other than `DEFAULT`/an unsigned integer is a genuine
    // `ParseError` (confirmed via `godump restore`: a bare option with
    // no value, a non-numeric word, a negative number, and a string
    // literal all `ERR`).
    assert!(parse("create table t (a int) stats_persistent").is_err());
    assert!(parse("create table t (a int) stats_persistent=abc").is_err());
    assert!(parse("create table t (a int) pack_keys=-1").is_err());
    assert!(parse("create table t (a int) stats_persistent='x'").is_err());

    // AUTO_RANDOM_BASE/NODEGROUP/AUTOEXTEND_SIZE: integer-valued.
    assert_eq!(
        r("create table t (a int) auto_random_base=100"),
        "CREATE TABLE `t` (`a` INT) AUTO_RANDOM_BASE = 100"
    );
    assert_eq!(
        r("create table t (a int) nodegroup=1"),
        "CREATE TABLE `t` (`a` INT) NODEGROUP = 1"
    );
    assert_eq!(
        r("create table t (a int) autoextend_size=4096"),
        "CREATE TABLE `t` (`a` INT) AUTOEXTEND_SIZE = 4096"
    );
    // CONNECTION/PASSWORD: string-valued.
    assert_eq!(
        r("create table t (a int) connection='mysql://host/db'"),
        "CREATE TABLE `t` (`a` INT) CONNECTION = 'mysql://host/db'"
    );
    assert_eq!(
        r("create table t (a int) password='secret'"),
        "CREATE TABLE `t` (`a` INT) PASSWORD = 'secret'"
    );
    // STATS_AUTO_RECALC: unlike STATS_PERSISTENT/PACK_KEYS, the value is
    // genuinely PRESERVED on restore, not discarded.
    assert_eq!(
        r("create table t (a int) stats_auto_recalc=default"),
        "CREATE TABLE `t` (`a` INT) STATS_AUTO_RECALC = DEFAULT"
    );
    assert_eq!(
        r("create table t (a int) stats_auto_recalc=1"),
        "CREATE TABLE `t` (`a` INT) STATS_AUTO_RECALC = 1"
    );
    // DATA DIRECTORY / INDEX DIRECTORY: two-word keyword, string-valued.
    assert_eq!(
        r("create table t (a int) data directory='/data'"),
        "CREATE TABLE `t` (`a` INT) DATA DIRECTORY = '/data'"
    );
    assert_eq!(
        r("create table t (a int) index directory='/idx'"),
        "CREATE TABLE `t` (`a` INT) INDEX DIRECTORY = '/idx'"
    );
    // INSERT_METHOD: a bare word, uppercased like ROW_FORMAT.
    assert_eq!(
        r("create table t (a int) insert_method=first"),
        "CREATE TABLE `t` (`a` INT) INSERT_METHOD = FIRST"
    );

    // ENCRYPTION: string-literal ONLY (a bare identifier is a genuine
    // ParseError), value must be exactly Y/y/N/n, case PRESERVED verbatim
    // on restore. A leading DEFAULT is accepted but silently dropped.
    assert_eq!(
        r("create table t (a int) encryption='Y'"),
        "CREATE TABLE `t` (`a` INT) ENCRYPTION = 'Y'"
    );
    assert_eq!(
        r("create table t (a int) encryption='n'"),
        "CREATE TABLE `t` (`a` INT) ENCRYPTION = 'n'"
    );
    assert_eq!(
        r("create table t (a int) default encryption = 'Y'"),
        "CREATE TABLE `t` (`a` INT) ENCRYPTION = 'Y'"
    );
    assert!(parse("create table t (a int) encryption = 'x'").is_err());
    assert!(parse("create table t (a int) encryption = y").is_err());

    // SECONDARY_ENGINE: a bare identifier or a string literal, either way
    // normalized to a quoted string on restore; `= NULL` is a genuinely
    // distinct shape restoring as the bare keyword NULL.
    assert_eq!(
        r("create table t (a int) secondary_engine='engine1'"),
        "CREATE TABLE `t` (`a` INT) SECONDARY_ENGINE = 'engine1'"
    );
    assert_eq!(
        r("create table t (a int) secondary_engine = tiflash"),
        "CREATE TABLE `t` (`a` INT) SECONDARY_ENGINE = 'tiflash'"
    );
    assert_eq!(
        r("create table t (a int) secondary_engine=null"),
        "CREATE TABLE `t` (`a` INT) SECONDARY_ENGINE = NULL"
    );
    // SECONDARY_ENGINE_ATTRIBUTE: UNLIKE ENGINE_ATTRIBUTE, string literal
    // ONLY (a bare identifier is a genuine ParseError).
    assert_eq!(
        r("create table t (a int) secondary_engine_attribute='{\"key\":\"val\"}'"),
        "CREATE TABLE `t` (`a` INT) SECONDARY_ENGINE_ATTRIBUTE = '{\"key\":\"val\"}'"
    );
    assert!(parse("create table t (a int) secondary_engine_attribute = myattr").is_err());
    // ENGINE_ATTRIBUTE: a bare identifier OR a string literal, either way
    // normalized to a quoted string on restore.
    assert_eq!(
        r("create table t (a int) engine_attribute='{\"key\":\"val\"}'"),
        "CREATE TABLE `t` (`a` INT) ENGINE_ATTRIBUTE = '{\"key\":\"val\"}'"
    );
    assert_eq!(
        r("create table t (a int) engine_attribute = myattr"),
        "CREATE TABLE `t` (`a` INT) ENGINE_ATTRIBUTE = 'myattr'"
    );

    // PLACEMENT POLICY: a two-word keyword restoring as a backtick-quoted
    // identifier regardless of whether the value was written as a bare
    // word, a quoted string, or the literal DEFAULT (itself just another
    // identifier-shaped value here). `SET DEFAULT` and `= DEFAULT` are
    // equivalent forms; a leading DEFAULT before PLACEMENT is accepted
    // but silently dropped on restore (unlike CHARACTER SET/COLLATE,
    // which always force-add it).
    assert_eq!(
        r("create table t (a int) placement policy='p1'"),
        "CREATE TABLE `t` (`a` INT) PLACEMENT POLICY = `p1`"
    );
    assert_eq!(
        r("create table t (a int) placement policy=`p1`"),
        "CREATE TABLE `t` (`a` INT) PLACEMENT POLICY = `p1`"
    );
    assert_eq!(
        r("create table t (a int) placement policy = default"),
        "CREATE TABLE `t` (`a` INT) PLACEMENT POLICY = `DEFAULT`"
    );
    assert_eq!(
        r("create table t (a int) placement policy set default"),
        "CREATE TABLE `t` (`a` INT) PLACEMENT POLICY = `DEFAULT`"
    );
    assert_eq!(
        r("create table t (a int) default placement policy = p1"),
        "CREATE TABLE `t` (`a` INT) PLACEMENT POLICY = `p1`"
    );

    // STATS_BUCKETS/STATS_TOPN: unsigned integer only. Real TiDB's own
    // restore code has a DEFAULT-value branch, but the hand-written
    // parser never sets it (confirmed via `godump restore`: `= DEFAULT`
    // is a genuine ParseError for both), so it's not modelled here.
    assert_eq!(
        r("create table t (a int) stats_buckets = 100"),
        "CREATE TABLE `t` (`a` INT) STATS_BUCKETS = 100"
    );
    assert_eq!(
        r("create table t (a int) stats_buckets 100"),
        "CREATE TABLE `t` (`a` INT) STATS_BUCKETS = 100"
    );
    assert!(parse("create table t (a int) stats_buckets = default").is_err());
    assert_eq!(
        r("create table t (a int) stats_topn = 50"),
        "CREATE TABLE `t` (`a` INT) STATS_TOPN = 50"
    );
    assert!(parse("create table t (a int) stats_topn = default").is_err());
    // STATS_SAMPLE_RATE: accepts int/float/decimal, no range validation
    // at parse time (a leading `-` fails only because a single
    // numeric-literal TOKEN is expected, not a general expression).
    assert_eq!(
        r("create table t (a int) stats_sample_rate = 0.5"),
        "CREATE TABLE `t` (`a` INT) STATS_SAMPLE_RATE = 0.5"
    );
    assert_eq!(
        r("create table t (a int) stats_sample_rate = 1"),
        "CREATE TABLE `t` (`a` INT) STATS_SAMPLE_RATE = 1"
    );
    assert_eq!(
        r("create table t (a int) stats_sample_rate = 1.5"),
        "CREATE TABLE `t` (`a` INT) STATS_SAMPLE_RATE = 1.5"
    );
    assert!(parse("create table t (a int) stats_sample_rate = default").is_err());
    assert!(parse("create table t (a int) stats_sample_rate = -1").is_err());
    // STATS_COL_CHOICE/STATS_COL_LIST: string-literal ONLY (a bare
    // identifier is a genuine ParseError).
    assert_eq!(
        r("create table t (a int) stats_col_choice = 'LIST'"),
        "CREATE TABLE `t` (`a` INT) STATS_COL_CHOICE = 'LIST'"
    );
    assert!(parse("create table t (a int) stats_col_choice = list").is_err());
    assert_eq!(
        r("create table t (a int) stats_col_list = 'a,b,c'"),
        "CREATE TABLE `t` (`a` INT) STATS_COL_LIST = 'a,b,c'"
    );
    assert!(parse("create table t (a int) stats_col_list = abc").is_err());
    // All five combine, still restoring in WRITTEN order.
    assert_eq!(
        r(
            "create table t (a int) stats_buckets = 100 stats_topn = 20 \
                 stats_sample_rate = 0.3 stats_col_choice = 'LIST' stats_col_list = 'a,b'"
        ),
        "CREATE TABLE `t` (`a` INT) STATS_BUCKETS = 100 STATS_TOPN = 20 \
             STATS_SAMPLE_RATE = 0.3 STATS_COL_CHOICE = 'LIST' STATS_COL_LIST = 'a,b'"
    );

    // The TTL family — the only expression-valued table option. `TTL =
    // col + INTERVAL n unit` back-quotes the column (even a bare word)
    // and uppercases the unit; `TTL_ENABLE` accepts only 'ON'/'OFF'
    // (case-insensitive, normalized to uppercase); `TTL_JOB_INTERVAL` is
    // a validated duration string preserved verbatim. All godump-verified.
    assert_eq!(
        r("create table t (a int) ttl = `a` + interval 1 day"),
        "CREATE TABLE `t` (`a` INT) TTL = `a` + INTERVAL 1 DAY"
    );
    assert_eq!(
        r("create table t (a int) ttl = a + interval 7 day"),
        "CREATE TABLE `t` (`a` INT) TTL = `a` + INTERVAL 7 DAY"
    );
    assert_eq!(
        r("create table t (a int) ttl=`created_at` + INTERVAL 3 MONTH"),
        "CREATE TABLE `t` (`a` INT) TTL = `created_at` + INTERVAL 3 MONTH"
    );
    assert_eq!(
        r("create table t (a int) ttl_enable = 'ON'"),
        "CREATE TABLE `t` (`a` INT) TTL_ENABLE = 'ON'"
    );
    assert_eq!(
        r("create table t (a int) ttl_enable = 'off'"),
        "CREATE TABLE `t` (`a` INT) TTL_ENABLE = 'OFF'"
    );
    assert!(parse("create table t (a int) ttl_enable = 'yes'").is_err());
    assert_eq!(
        r("create table t (a int) ttl_job_interval = '1h'"),
        "CREATE TABLE `t` (`a` INT) TTL_JOB_INTERVAL = '1h'"
    );
    assert_eq!(
        r("create table t (a int) ttl_job_interval = '30m'"),
        "CREATE TABLE `t` (`a` INT) TTL_JOB_INTERVAL = '30m'"
    );
    assert!(parse("create table t (a int) ttl_job_interval = 'xyz'").is_err());
    // The three combine and interleave with other options, still in
    // WRITTEN order.
    assert_eq!(
        r("create table t (a int) ttl = `a` + interval 1 day \
                 ttl_enable = 'ON' ttl_job_interval = '1h'"),
        "CREATE TABLE `t` (`a` INT) TTL = `a` + INTERVAL 1 DAY \
             TTL_ENABLE = 'ON' TTL_JOB_INTERVAL = '1h'"
    );
    assert_eq!(
        r("create table t (a int) ttl = `a` + interval 1 day comment 'x'"),
        "CREATE TABLE `t` (`a` INT) TTL = `a` + INTERVAL 1 DAY COMMENT = 'x'"
    );
}

#[test]
fn alter_table() {
    // `ADD`/`DROP` alone (no `COLUMN`) restore identically to the
    // `COLUMN`-qualified form, matching the Go AST's normalization.
    assert_eq!(
        r("alter table t add column c int"),
        "ALTER TABLE `t` ADD COLUMN `c` INT"
    );
    assert_eq!(
        r("alter table t add c int"),
        "ALTER TABLE `t` ADD COLUMN `c` INT"
    );
    assert_eq!(
        r("alter table t add column c varchar(20) not null default 'x'"),
        "ALTER TABLE `t` ADD COLUMN `c` VARCHAR(20) NOT NULL DEFAULT _UTF8MB4'x'"
    );
    assert_eq!(
        r("alter table t add c int first"),
        "ALTER TABLE `t` ADD COLUMN `c` INT FIRST"
    );
    assert_eq!(
        r("alter table t add c int after b"),
        "ALTER TABLE `t` ADD COLUMN `c` INT AFTER `b`"
    );
    assert_eq!(
        r("alter table t drop column c"),
        "ALTER TABLE `t` DROP COLUMN `c`"
    );
    assert_eq!(r("alter table t drop c"), "ALTER TABLE `t` DROP COLUMN `c`");

    let stmt = parse("alter table t add c int after b").unwrap();
    let alt = ddl_payload!(stmt, AlterTable);
    assert_eq!(
        only_alter_action(&alt),
        AlterTableAction::AddColumn {
            if_not_exists: false,
            column: ColumnDef {
                qualifier: vec![],
                name: "c".to_string(),
                ty: ColumnType {
                    name: "INT".to_string(),
                    args: vec![],
                    unsigned: false,
                    zerofill: false,
                    binary: false,
                    charset: None,
                },
                options: vec![],
            },
            position: ColumnPosition::After("b".to_string()),
        }
    );

    assert_eq!(
        r("alter table t add constraint fk1 foreign key (a) references u (id)"),
        "ALTER TABLE `t` ADD CONSTRAINT `fk1` FOREIGN KEY (`a`) REFERENCES `u`(`id`)"
    );
}

#[test]
fn alter_table_multi_specs_preserve_go_order_and_separators() {
    assert_eq!(
        r("alter table t add column b int, drop column a"),
        "ALTER TABLE `t` ADD COLUMN `b` INT, DROP COLUMN `a`"
    );
    let statement = ddl_payload!(
        parse("alter table t add column b int, drop column a").unwrap(),
        AlterTable
    );
    assert!(matches!(
        statement.actions.as_slice(),
        [
            AlterTableAction::AddColumn { column, .. },
            AlterTableAction::DropColumn { name, .. }
        ] if column.name == "b" && name == "a"
    ));

    // Direct Go parser source rows: specs retain source order and ordinary
    // specs restore with comma-space separators.
    assert_eq!(
        r("alter table t add column a smallint unsigned, add column b smallint"),
        "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED, ADD COLUMN `b` SMALLINT"
    );
    assert_eq!(
        r("alter table t add constraint c_1 check (1+1) not enforced, add unique (a)"),
        "ALTER TABLE `t` ADD CONSTRAINT `c_1` CHECK(1+1) NOT ENFORCED, ADD UNIQUE(`a`)"
    );

    // Commas owned by one spec remain inside its typed payload; only the
    // comma left after that payload separates the following spec.
    assert_eq!(
        r("alter table t drop partition p0,p1, add column c int"),
        "ALTER TABLE `t` DROP PARTITION `p0`,`p1`, ADD COLUMN `c` INT"
    );
    assert_eq!(
        r("alter table t set tiflash replica 2 location labels 'a','b', add column c int"),
        "ALTER TABLE `t` SET TIFLASH REPLICA 2 LOCATION LABELS 'a', 'b', ADD COLUMN `c` INT"
    );

    // REMOVE PARTITIONING is Go's terminal AlterTablePartitionOpt: no comma
    // before it in either accepted input or canonical restore.
    assert_eq!(
        r("alter table t add column c int remove partitioning"),
        "ALTER TABLE `t` ADD COLUMN `c` INT REMOVE PARTITIONING"
    );
    assert!(parse("alter table t add column c int, remove partitioning").is_err());

    let empty = ddl_payload!(parse("alter table t").unwrap(), AlterTable);
    assert!(empty.actions.is_empty());
    assert_eq!(r("alter table t"), "ALTER TABLE `t`");
}

#[test]
fn alter_table_charset_collation_options_follow_go_option_order() {
    // These are direct source rows from
    // `tests/integrationtest/t/collation_misc.test`: Go's generic
    // `parseAlterTableOptions` consumes every adjacent option into ONE AST
    // spec and restores them in source order without commas.
    assert_eq!(
        r("alter table t1 collate uTf8mB4_uNiCoDe_Ci charset Utf8mB4 charset uTF8Mb4 collate UTF8MB4_BiN"),
        "ALTER TABLE `t1` DEFAULT COLLATE = UTF8MB4_UNICODE_CI DEFAULT CHARACTER SET = UTF8MB4 DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_BIN"
    );
    assert_eq!(
        r("alter table t char set utf8mb3 default collate utf8_bin"),
        "ALTER TABLE `t` CHARACTER SET UTF8 COLLATE UTF8_BIN"
    );
    assert_eq!(
        r("alter table d_n.t_n convert to char set default collate utf8mb4_0900_ai_ci"),
        "ALTER TABLE `d_n`.`t_n` CONVERT TO CHARACTER SET DEFAULT COLLATE UTF8MB4_0900_AI_CI"
    );
    assert!(parse("alter table t charset not_a_charset").is_err());

    let stmt = parse("alter table t collate utf8_bin charset utf8").unwrap();
    let alt = ddl_payload!(stmt, AlterTable);
    assert_eq!(
        only_alter_action(&alt),
        AlterTableAction::SetTableOptions {
            options: vec![
                TableOption::Collate("UTF8_BIN".to_string()),
                TableOption::CharacterSet("UTF8".to_string()),
            ],
        }
    );
}

#[test]
fn alter_table_affinity_keeps_the_go_string_literal_boundary() {
    // Direct source cases from `pkg/parser/parser_test.go:TestTableAffinityOption`.
    // Restore preserves the decoded literal spelling; semantic validation of
    // `table`/`partition`/`none` happens later in real TiDB DDL.
    assert_eq!(
        r("alter table t affinity 'TABLE'"),
        "ALTER TABLE `t` AFFINITY = 'TABLE'"
    );
    assert_eq!(
        r("alter table t affinity='it\\'s'"),
        "ALTER TABLE `t` AFFINITY = 'it''s'"
    );
    let stmt = parse("alter table t affinity = 'partition'").unwrap();
    let alt = ddl_payload!(stmt, AlterTable);
    assert_eq!(
        only_alter_action(&alt),
        AlterTableAction::SetAffinity {
            level: "partition".to_string(),
        }
    );
    for sql in [
        "alter table t affinity 1",
        "alter table t affinity = 1",
        "alter table t affinity",
        "alter table t affinity = table",
    ] {
        assert!(parse(sql).is_err(), "{sql}");
    }
}

#[test]
fn alter_table_add_check_constraint() {
    let stmt = parse("alter table t add constraint ck_a check (a > 0) not enforced").unwrap();
    let alt = ddl_payload!(stmt, AlterTable);
    assert!(matches!(
        only_alter_action(&alt),
        AlterTableAction::AddCheck(tidb_ast::CheckConstraintDefinition {
            name: Some(ref name),
            enforced: false,
            ..
        }) if name == "ck_a"
    ));
    assert_eq!(
        r("alter table t add check (a > 0)"),
        "ALTER TABLE `t` ADD CHECK(`a`>0) ENFORCED"
    );
    assert_eq!(
        r("alter table t add constraint ck_a check (a > 0) not enforced"),
        "ALTER TABLE `t` ADD CONSTRAINT `ck_a` CHECK(`a`>0) NOT ENFORCED"
    );
    // `CONSTRAINT` without a name is normalized away, matching the Go AST.
    assert_eq!(
        r("alter table t add constraint check (true)"),
        "ALTER TABLE `t` ADD CHECK(TRUE) ENFORCED"
    );
}

#[test]
fn alter_table_add_partition_count() {
    let stmt = parse("alter table t add partition partitions 8").unwrap();
    let alt = ddl_payload!(stmt, AlterTable);
    assert_eq!(
        only_alter_action(&alt),
        AlterTableAction::Partition(tidb_ast::AlterPartitionAction::Add {
            if_not_exists: false,
            no_write_to_binlog: false,
            spec: tidb_ast::AddPartitionSpec::Count(8),
        })
    );
    assert_eq!(
        r("alter table t add partition partitions 8"),
        "ALTER TABLE `t` ADD PARTITION PARTITIONS 8"
    );
    let stmt = ddl_payload!(
        parse("alter table t add partition if not exists no_write_to_binlog (partition p0 values less than (10), partition p1 values in ((2, 'x'), default), partition pmax values less than maxvalue comment='tail')").unwrap(),
        AlterTable
    );
    assert!(matches!(
        only_alter_action(&stmt),
        AlterTableAction::Partition(tidb_ast::AlterPartitionAction::Add {
            if_not_exists: true,
            no_write_to_binlog: true,
            spec: tidb_ast::AddPartitionSpec::Definitions(ref definitions),
        }) if definitions.len() == 3
    ));
    assert_eq!(
        r("alter table t add partition (partition p0 values less than (10), partition p1 values in ((2, 'x'), default), partition pmax values less than maxvalue comment='tail')"),
        "ALTER TABLE `t` ADD PARTITION (PARTITION `p0` VALUES LESS THAN (10), PARTITION `p1` VALUES IN ((2, _UTF8MB4'x'), DEFAULT), PARTITION `pmax` VALUES LESS THAN (MAXVALUE) COMMENT = 'tail')"
    );
    assert!(
        parse("alter table t add partition (partition p0 values less than (default))").is_err()
    );
}

#[test]
fn alter_table_partition_maintenance_actions_are_typed_and_restore_like_go() {
    let stmt = ddl_payload!(
        parse("alter table t reorganize partition no_write_to_binlog p0, p1 into (partition p01 values less than (20), partition pmax values less than maxvalue)").unwrap(),
        AlterTable
    );
    assert!(matches!(
        only_alter_action(&stmt),
        AlterTableAction::Partition(tidb_ast::AlterPartitionAction::Reorganize {
            no_write_to_binlog: true,
            ref names,
            ref definitions,
        }) if names == &["p0", "p1"] && definitions.len() == 2
    ));
    assert_eq!(
        r("alter table t reorganize partition no_write_to_binlog p0, p1 into (partition p01 values less than (20), partition pmax values less than maxvalue)"),
        "ALTER TABLE `t` REORGANIZE PARTITION NO_WRITE_TO_BINLOG `p0`,`p1` INTO (PARTITION `p01` VALUES LESS THAN (20), PARTITION `pmax` VALUES LESS THAN (MAXVALUE))"
    );
    assert_eq!(
        r("alter table t coalesce partition no_write_to_binlog 2"),
        "ALTER TABLE `t` COALESCE PARTITION NO_WRITE_TO_BINLOG 2"
    );
    assert_eq!(
        r("alter table t truncate partition p0, p1"),
        "ALTER TABLE `t` TRUNCATE PARTITION `p0`,`p1`"
    );
    assert_eq!(
        r("alter table t truncate partition all"),
        "ALTER TABLE `t` TRUNCATE PARTITION ALL"
    );
    assert_eq!(
        r("alter table t remove partitioning"),
        "ALTER TABLE `t` REMOVE PARTITIONING"
    );
    assert_eq!(
        r("alter table t optimize partition no_write_to_binlog p0,p1"),
        "ALTER TABLE `t` OPTIMIZE PARTITION NO_WRITE_TO_BINLOG `p0`,`p1`"
    );
    assert!(parse("alter table t coalesce partition p0").is_err());
    assert!(parse("alter table t reorganize partition p0").is_err());
    assert!(parse("alter table t truncate partition").is_err());
}

#[test]
fn alter_table_tiflash_replica_and_compact_are_typed_and_restore_like_go() {
    let stmt = ddl_payload!(
        parse("alter table t set hypo tiflash replica 2 location labels 'zone-a','zone-b'")
            .unwrap(),
        AlterTable
    );
    assert_eq!(
        only_alter_action(&stmt),
        AlterTableAction::SetTiFlashReplica {
            hypo: true,
            count: 2,
            labels: vec!["zone-a".to_owned(), "zone-b".to_owned()],
        }
    );
    // Go retains `Hypo` in its TiFlashReplicaSpec but omits it from restore.
    assert_eq!(
        r("alter table t set hypo tiflash replica 2 location labels 'zone-a','zone-b'"),
        "ALTER TABLE `t` SET TIFLASH REPLICA 2 LOCATION LABELS 'zone-a', 'zone-b'"
    );

    let stmt = ddl_payload!(
        parse("alter table db.t compact partition p1,p2 tiflash replica").unwrap(),
        AlterTable
    );
    assert_eq!(
        only_alter_action(&stmt),
        AlterTableAction::Compact {
            partitions: vec!["p1".to_owned(), "p2".to_owned()],
            replica_kind: CompactReplicaKind::TiFlash,
        }
    );
    assert_eq!(
        r("alter table db.t compact partition p1,p2 tiflash replica"),
        "ALTER TABLE `db`.`t` COMPACT PARTITION `p1`,`p2` TIFLASH REPLICA"
    );
    // The Go parser makes REPLICA optional after both engine spellings.
    assert_eq!(
        r("alter table t compact tikv"),
        "ALTER TABLE `t` COMPACT TIKV REPLICA"
    );
}

#[test]
fn alter_table_exchange_partition() {
    assert_eq!(
        r("alter table db.pt exchange partition p0 with table archive"),
        "ALTER TABLE `db`.`pt` EXCHANGE PARTITION `p0` WITH TABLE `archive`"
    );
    // Go normalizes an explicit WITH VALIDATION to its default omitted form.
    assert_eq!(
        r("alter table pt exchange partition p0 with table db.archive with validation"),
        "ALTER TABLE `pt` EXCHANGE PARTITION `p0` WITH TABLE `db`.`archive`"
    );
    assert_eq!(
        r("alter table pt exchange partition p0 with table archive without validation"),
        "ALTER TABLE `pt` EXCHANGE PARTITION `p0` WITH TABLE `archive` WITHOUT VALIDATION"
    );

    let stmt = ddl_payload!(
        parse("alter table pt exchange partition p0 with table db.archive without validation")
            .unwrap(),
        AlterTable
    );
    assert_eq!(
        only_alter_action(&stmt),
        AlterTableAction::Partition(tidb_ast::AlterPartitionAction::Exchange {
            partition: "p0".to_owned(),
            table: vec!["db".to_owned(), "archive".to_owned()],
            with_validation: false,
        })
    );

    // Each partition envelope remains one ordered spec; the outer statement
    // owns the comma between them.
    assert_eq!(
        r("alter table pt exchange partition p0 with table archive, drop partition p0"),
        "ALTER TABLE `pt` EXCHANGE PARTITION `p0` WITH TABLE `archive`, DROP PARTITION `p0`"
    );
}

#[test]
fn alter_table_drop_partition() {
    assert_eq!(
        r("alter table pt drop partition p0"),
        "ALTER TABLE `pt` DROP PARTITION `p0`"
    );
    assert_eq!(
        r("alter table pt drop partition p0, `p 1`"),
        "ALTER TABLE `pt` DROP PARTITION `p0`,`p 1`"
    );
    assert_eq!(
        r("alter table pt drop partition if exists p0"),
        "ALTER TABLE `pt` DROP PARTITION IF EXISTS `p0`"
    );

    let stmt = ddl_payload!(
        parse("alter table pt drop partition if exists p0, p1").unwrap(),
        AlterTable
    );
    assert_eq!(
        only_alter_action(&stmt),
        AlterTableAction::Partition(tidb_ast::AlterPartitionAction::Drop {
            if_exists: true,
            names: vec!["p0".to_owned(), "p1".to_owned()],
        })
    );

    assert!(parse("alter table pt drop partition").is_err());
}

#[test]
fn alter_table_modify_change_column() {
    // `MODIFY`/`CHANGE` alone (no `COLUMN`) restore identically to the
    // `COLUMN`-qualified form.
    assert_eq!(
        r("alter table t modify column c bigint"),
        "ALTER TABLE `t` MODIFY COLUMN `c` BIGINT"
    );
    assert_eq!(
        r("alter table t modify c bigint"),
        "ALTER TABLE `t` MODIFY COLUMN `c` BIGINT"
    );
    assert_eq!(
        r("alter table t modify c int first"),
        "ALTER TABLE `t` MODIFY COLUMN `c` INT FIRST"
    );
    assert_eq!(
        r("alter table t modify c int after b"),
        "ALTER TABLE `t` MODIFY COLUMN `c` INT AFTER `b`"
    );
    // CHANGE COLUMN renames: the old name, then the new column def.
    assert_eq!(
        r("alter table t change b c int"),
        "ALTER TABLE `t` CHANGE COLUMN `b` `c` INT"
    );
    assert_eq!(
        r("alter table t change column b c int"),
        "ALTER TABLE `t` CHANGE COLUMN `b` `c` INT"
    );
    assert_eq!(
        r("alter table t change b c int after a"),
        "ALTER TABLE `t` CHANGE COLUMN `b` `c` INT AFTER `a`"
    );

    let stmt = parse("alter table t change b c int").unwrap();
    let alt = ddl_payload!(stmt, AlterTable);
    assert_eq!(
        only_alter_action(&alt),
        AlterTableAction::ChangeColumn {
            old_name: "b".to_string(),
            column: ColumnDef {
                qualifier: vec![],
                name: "c".to_string(),
                ty: ColumnType {
                    name: "INT".to_string(),
                    args: vec![],
                    unsigned: false,
                    zerofill: false,
                    binary: false,
                    charset: None,
                },
                options: vec![],
            },
            position: ColumnPosition::Default,
        }
    );
}

#[test]
fn alter_table_rename() {
    // `TO`/`AS`/neither all restore identically as `RENAME AS`,
    // matching the Go AST's normalization.
    assert_eq!(
        r("alter table t rename to t2"),
        "ALTER TABLE `t` RENAME AS `t2`"
    );
    assert_eq!(
        r("alter table t rename t2"),
        "ALTER TABLE `t` RENAME AS `t2`"
    );
    assert_eq!(
        r("alter table t rename as t2"),
        "ALTER TABLE `t` RENAME AS `t2`"
    );
    let stmt = parse("alter table t rename to t2").unwrap();
    let alt = ddl_payload!(stmt, AlterTable);
    assert_eq!(
        only_alter_action(&alt),
        AlterTableAction::RenameTable {
            new_name: vec!["t2".to_string()],
        }
    );
}

#[test]
fn rename_table() {
    // A separate top-level statement from ALTER TABLE ... RENAME.
    assert_eq!(r("rename table t to t2"), "RENAME TABLE `t` TO `t2`");
    assert_eq!(
        r("rename table t to t2, u to u2"),
        "RENAME TABLE `t` TO `t2`, `u` TO `u2`"
    );
    let stmt = parse("rename table t to t2, u to u2").unwrap();
    let rt = ddl_payload!(stmt, RenameTable);
    assert_eq!(
        rt.pairs,
        vec![
            (vec!["t".to_string()], vec!["t2".to_string()]),
            (vec!["u".to_string()], vec!["u2".to_string()]),
        ]
    );
}

#[test]
fn alter_table_add_index() {
    // A bare `KEY` normalizes to `INDEX` on restore, matching the Go
    // AST — the opposite of CREATE TABLE's column-level UNIQUE, which
    // adds "KEY" rather than dropping it.
    assert_eq!(
        r("alter table t add index (a)"),
        "ALTER TABLE `t` ADD INDEX(`a`)"
    );
    assert_eq!(
        r("alter table t add key idx_a (a)"),
        "ALTER TABLE `t` ADD INDEX `idx_a`(`a`)"
    );
    assert_eq!(
        r("alter table t add constraint cn1 index idx1 (a)"),
        "ALTER TABLE `t` ADD INDEX `cn1`(`a`)"
    );
    assert_eq!(
        r("alter table t add index (a, b)"),
        "ALTER TABLE `t` ADD INDEX(`a`, `b`)"
    );
    assert_eq!(
        r("alter table t add key idx (`a`(0), b(16), (cast(j as signed array)) desc) comment 'note' global invisible where a > 1"),
        "ALTER TABLE `t` ADD INDEX `idx`(`a`, `b`(16), (CAST(`j` AS SIGNED ARRAY)) DESC) COMMENT 'note' GLOBAL INVISIBLE WHERE `a`>1"
    );
    // Go overwrites repeated scalar index options, including LOCAL's default
    // Global=false state.
    assert_eq!(
        r("alter table t add index idx(a) comment 'old' comment 'new' global local invisible invisible where a > 1 where b > 2"),
        "ALTER TABLE `t` ADD INDEX `idx`(`a`) COMMENT 'new' INVISIBLE WHERE `b`>2"
    );
    assert_eq!(
        r("alter table t add index ((json_type(doc)))"),
        "ALTER TABLE `t` ADD INDEX((JSON_TYPE(`doc`)))"
    );
    assert_eq!(
        r("alter table t add index i(a) using btree with parser p visible"),
        "ALTER TABLE `t` ADD INDEX `i`(`a`) USING BTREE WITH PARSER `p` VISIBLE"
    );
    assert_eq!(
        r("alter table t add vector index i(a)"),
        "ALTER TABLE `t` ADD VECTOR INDEX `i`(`a`)"
    );
    assert_eq!(
        r("alter table t add unique index (a)"),
        "ALTER TABLE `t` ADD UNIQUE(`a`)"
    );
    assert_eq!(
        r("alter table t add unique key idx_a (a)"),
        "ALTER TABLE `t` ADD UNIQUE `idx_a`(`a`)"
    );
    // A CONSTRAINT name wins over an inline index name when both are
    // given, matching CREATE TABLE's table-level constraints.
    assert_eq!(
        r("alter table t add constraint cn1 unique index idx1 (a)"),
        "ALTER TABLE `t` ADD UNIQUE `cn1`(`a`)"
    );

    let stmt = parse("alter table t add unique key idx_a (a)").unwrap();
    let alt = ddl_payload!(stmt, AlterTable);
    let AlterTableAction::AddIndexConstraint(index) = only_alter_action(&alt) else {
        panic!("expected ADD UNIQUE KEY constraint");
    };
    assert_eq!(index.kind, tidb_ast::IndexConstraintKind::Unique);
    assert_eq!(index.name.as_deref(), Some("idx_a"));
    assert_eq!(index.parts, plain_key_parts(&["a"]));
}

#[test]
fn alter_table_drop_index() {
    // `KEY` is an input alias, while Go's AST canonical restore always uses
    // `DROP INDEX`; `IF EXISTS` is preserved.
    assert_eq!(
        r("alter table t drop index idx_a"),
        "ALTER TABLE `t` DROP INDEX `idx_a`"
    );
    assert_eq!(
        r("alter table t drop key idx_a"),
        "ALTER TABLE `t` DROP INDEX `idx_a`"
    );
    assert_eq!(
        r("alter table t drop index if exists idx_a"),
        "ALTER TABLE `t` DROP INDEX IF EXISTS `idx_a`"
    );
    let stmt = ddl_payload!(
        parse("alter table t drop key if exists idx_a").unwrap(),
        AlterTable
    );
    assert_eq!(
        only_alter_action(&stmt),
        AlterTableAction::DropIndex {
            if_exists: true,
            name: "idx_a".to_string(),
        }
    );
    // `DROP PRIMARY KEY` is a distinct payload-free action, covered by its
    // source-owned test module rather than this generic DROP INDEX slice.
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

/// `BIT`/`BIT(n)` as a column type — see `Parser::parse_column_type`'s
/// own doc for the two `BIT`-specific restore/rejection rules, both
/// confirmed via `godump restore`: a bare `BIT` (no explicit length)
/// materializes the default length `BIT(1)` explicitly into the AST,
/// unlike every other type's own empty-args case; `BIT` never accepts
/// `UNSIGNED`/`ZEROFILL` (a genuine `ParseError` in real TiDB too — a
/// bit-string column is already unsigned by definition). No length
/// bounds validation at parse time (`BIT(0)`/`BIT(65)` both parse fine,
/// confirmed via `godump restore` — real TiDB's own 1-64 range is a
/// semantic/execution-time check, not a grammar restriction, matching
/// this crate's own established convention of storing numeric type args
/// as unvalidated raw digit text).
#[test]
fn bit_column_type() {
    assert_eq!(r("create table t (a bit)"), "CREATE TABLE `t` (`a` BIT(1))");
    assert_eq!(
        r("create table t (a bit(8))"),
        "CREATE TABLE `t` (`a` BIT(8))"
    );
    assert_eq!(
        r("create table t (a bit(64) not null default b'0')"),
        "CREATE TABLE `t` (`a` BIT(64) NOT NULL DEFAULT b'0')"
    );
    // No bounds validation at parse time.
    assert_eq!(
        r("create table t (a bit(0))"),
        "CREATE TABLE `t` (`a` BIT(0))"
    );
    assert_eq!(
        r("create table t (a bit(65))"),
        "CREATE TABLE `t` (`a` BIT(65))"
    );
    // `UNSIGNED`/`ZEROFILL` are never accepted on `BIT`.
    assert!(parse("create table t (a bit(1) unsigned)").is_err());
    assert!(parse("create table t (a bit zerofill)").is_err());
}

/// `ENUM`/`SET` column types (task #138, from the real-TiDB corpus): a
/// required parenthesized member list of string literals, restored as
/// single-quoted, escaped values (double-quoted input normalizes to
/// single; an embedded quote doubles). `UNSIGNED`/`ZEROFILL` never apply.
/// All godump-verified.
#[test]
fn enum_set_column_types() {
    assert_eq!(
        r("create table t (a enum('B','C'))"),
        "CREATE TABLE `t` (`a` ENUM('B','C'))"
    );
    assert_eq!(
        r("create table t (a enum('x') not null)"),
        "CREATE TABLE `t` (`a` ENUM('x') NOT NULL)"
    );
    assert_eq!(
        r("create table t (a set('a','b','c'))"),
        "CREATE TABLE `t` (`a` SET('a','b','c'))"
    );
    // Double-quoted input normalizes to single-quoted output.
    assert_eq!(
        r("create table t (a set(\"x\",\"y\"))"),
        "CREATE TABLE `t` (`a` SET('x','y'))"
    );
    // An embedded quote (written doubled) restores doubled.
    assert_eq!(
        r("create table t (a enum('a','b''c'))"),
        "CREATE TABLE `t` (`a` ENUM('a','b''c'))"
    );
    // An empty member and a member list with `DEFAULT` co-exist.
    assert_eq!(
        r("create table t (a enum('a','') default 'a')"),
        "CREATE TABLE `t` (`a` ENUM('a','') DEFAULT _UTF8MB4'a')"
    );
    // Go right-trims only ASCII spaces from decoded members. It retains
    // tabs and leading spaces, and retains ENUM/SET charset metadata while
    // suppressing that field-type attribute during restore; COLLATE is a
    // separate visible column option.
    assert_eq!(
        r("create table t (a enum('a ', 'b\\t', ' c ') charset utf8mb4 collate utf8mb4_general_ci, b set('a ', 'b\\t', ' c ') charset binary)"),
        "CREATE TABLE `t` (`a` ENUM('a','b\t',' c') COLLATE utf8mb4_general_ci,`b` SET('a','b\t',' c'))"
    );
    // ALTER TABLE reuses the same ColumnType parser, so its decoded members
    // receive the identical Go normalization.
    assert_eq!(
        r("alter table t change a aa enum('a   ', 'b\\t', ' c ')"),
        "ALTER TABLE `t` CHANGE COLUMN `a` `aa` ENUM('a','b\t',' c')"
    );
    // The member list is required; `UNSIGNED`/`ZEROFILL` are not accepted.
    assert!(parse("create table t (a enum)").is_err());
    assert!(parse("create table t (a enum('a') unsigned)").is_err());
    // Binary literal members decode to bytes before Go's field-type restore.
    assert_eq!(
        r("create table t (a enum(0x61, 'b'))"),
        "CREATE TABLE `t` (`a` ENUM('a','b'))"
    );
    assert_eq!(
        r("create table t (a set(b'01100001', 'b'))"),
        "CREATE TABLE `t` (`a` SET('a','b'))"
    );
    assert_eq!(
        r("create table t (a enum('a') binary)"),
        "CREATE TABLE `t` (`a` ENUM('a') BINARY)"
    );
}
