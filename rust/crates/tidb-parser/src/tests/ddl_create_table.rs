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

//! `CREATE TABLE` tests: the column-type families, index/constraint clauses,
//! and table options. Split out of `tests::ddl` for file size; every
//! assertion is character-identical to the original.

use super::ddl_alter_table::only_alter_action;
use super::*;

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

/// Exact rows from `pkg/parser/parser_test.go::TestCompatMariaDB`.
#[test]
fn test_compat_mariadb_source_rows() {
    for (sql, expected) in [
        (
            "CREATE TABLE uuid (uuid int)",
            "CREATE TABLE `uuid` (`uuid` INT)",
        ),
        (
            "CREATE TABLE t1 (a TEXT DEFAULT UUID())",
            "CREATE TABLE `t1` (`a` TEXT DEFAULT (UUID()))",
        ),
        (
            "CREATE TABLE t1 (pk varchar(36) DEFAULT uuid())",
            "CREATE TABLE `t1` (`pk` VARCHAR(36) DEFAULT (UUID()))",
        ),
        (
            "CREATE TABLE t1 AS SELECT uuid(), length(uuid())",
            "CREATE TABLE `t1` AS SELECT UUID(),LENGTH(UUID())",
        ),
        (
            "CREATE TABLE t4 (a INT(11) DEFAULT NULL, b BIGINT(20) DEFAULT uuid_short()) SELECT * FROM t3",
            "CREATE TABLE `t4` (`a` INT(11) DEFAULT NULL,`b` BIGINT(20) DEFAULT (UUID_SHORT())) AS SELECT * FROM `t3`",
        ),
        (
            "CREATE TABLE t (id int PRIMARY KEY) PAGE_CHECKSUM=1",
            "CREATE TABLE `t` (`id` INT PRIMARY KEY) PAGE_CHECKSUM = 1",
        ),
        (
            "CREATE TABLE t (id int PRIMARY KEY) PAGE_COMPRESSED=1",
            "CREATE TABLE `t` (`id` INT PRIMARY KEY) PAGE_COMPRESSED = 1",
        ),
        (
            "CREATE TABLE t (id int PRIMARY KEY) PAGE_COMPRESSION_LEVEL=1",
            "CREATE TABLE `t` (`id` INT PRIMARY KEY) PAGE_COMPRESSION_LEVEL = 1",
        ),
        (
            "CREATE TABLE t (id int PRIMARY KEY) TRANSACTIONAL=0",
            "CREATE TABLE `t` (`id` INT PRIMARY KEY) TRANSACTIONAL = 0",
        ),
        (
            "CREATE TABLE t (id int PRIMARY KEY) IETF_QUOTES=YES",
            "CREATE TABLE `t` (`id` INT PRIMARY KEY) IETF_QUOTES = YES",
        ),
        (
            "CREATE TABLE t (id int PRIMARY KEY) SEQUENCE=1",
            "CREATE TABLE `t` (`id` INT PRIMARY KEY) SEQUENCE = 1",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
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
