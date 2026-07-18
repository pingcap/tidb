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

//! Source-addressed column-option cases from Go
//! `HandParser.parseColumnDef` / `HandParser.parseColumnOptions`,
//! `pkg/parser/parser_test.go:TestDDL`, and
//! `pkg/parser/ast/ddl_test.go:TestDDLColumnOptionRestore`.

use super::*;

fn first_column_options(sql: &str) -> Vec<tidb_ast::ColumnOption> {
    let Stmt::Ddl(ddl) = parse(sql).expect("parse Go source row") else {
        panic!("expected DDL");
    };
    let tidb_ast::DdlStmt::CreateTable(table) = *ddl else {
        panic!("expected CREATE TABLE");
    };
    table.columns[0].options.clone()
}

#[test]
fn ddl_column_option_restore_source_table_is_exactly_represented() {
    // Every input/output pair in
    // `pkg/parser/ast/ddl_test.go:TestDDLColumnOptionRestore`. Go extracts
    // the first option from `CREATE TABLE child (id INT <case>)`; restore of
    // the enclosing statement exercises that same parser and AST contract
    // without a second, option-only grammar.
    for (source, restored) in [
        ("primary key", "PRIMARY KEY"),
        ("not null", "NOT NULL"),
        ("null", "NULL"),
        ("auto_increment", "AUTO_INCREMENT"),
        ("DEFAULT 10", "DEFAULT 10"),
        ("DEFAULT '10'", "DEFAULT _UTF8MB4'10'"),
        ("DEFAULT 'hello'", "DEFAULT _UTF8MB4'hello'"),
        ("DEFAULT 1.1", "DEFAULT 1.1"),
        ("DEFAULT NULL", "DEFAULT NULL"),
        ("DEFAULT ''", "DEFAULT _UTF8MB4''"),
        ("DEFAULT TRUE", "DEFAULT TRUE"),
        ("DEFAULT FALSE", "DEFAULT FALSE"),
        ("DEFAULT (colA)", "DEFAULT (`colA`)"),
        ("UNIQUE KEY", "UNIQUE KEY"),
        (
            "on update CURRENT_TIMESTAMP",
            "ON UPDATE CURRENT_TIMESTAMP()",
        ),
        ("comment 'hello'", "COMMENT 'hello'"),
        (
            "generated always as(id + 1)",
            "GENERATED ALWAYS AS(`id`+1) VIRTUAL",
        ),
        (
            "generated always as(id + 1) virtual",
            "GENERATED ALWAYS AS(`id`+1) VIRTUAL",
        ),
        (
            "generated always as(id + 1) stored",
            "GENERATED ALWAYS AS(`id`+1) STORED",
        ),
        ("REFERENCES parent(id)", "REFERENCES `parent`(`id`)"),
        ("COLLATE utf8_bin", "COLLATE utf8_bin"),
        // `tests/integrationtest/t/executor/partition/issues.test:73-85`
        // uses the quoted StringName form on a VARBINARY column.
        ("COLLATE 'binary'", "COLLATE binary"),
        ("STORAGE DEFAULT", "STORAGE DEFAULT"),
        ("STORAGE DISK", "STORAGE DISK"),
        ("STORAGE MEMORY", "STORAGE MEMORY"),
        ("AUTO_RANDOM (3)", "AUTO_RANDOM(3)"),
        ("AUTO_RANDOM", "AUTO_RANDOM"),
    ] {
        let sql = format!("CREATE TABLE child (id INT {source})");
        assert_eq!(
            r(&sql),
            format!("CREATE TABLE `child` (`id` INT {restored})"),
            "source option: {source}"
        );
    }
}

/// Exact CREATE TABLE row from
/// `tests/integrationtest/t/executor/partition/issues.test:73-85`.
/// The quoted `COLLATE 'binary'` on `VARBINARY` is the source-owned boundary
/// exercised here; retaining the complete row prevents a narrow option test
/// from silently drifting away from the integration fixture's surrounding
/// partition/table grammar.
#[test]
fn partition_issue_25030_quoted_binary_collate_restores_like_go() {
    let sql = "CREATE TABLE tbl_936 (col_5410 smallint NOT NULL, col_5411 double, col_5412 boolean NOT NULL DEFAULT 1, col_5413 set('Alice', 'Bob', 'Charlie', 'David') NOT NULL DEFAULT 'Charlie', col_5414 varbinary(147) COLLATE 'binary' DEFAULT 'bvpKgYWLfyuTiOYSkj', col_5415 timestamp NOT NULL DEFAULT '2021-07-06', col_5416 decimal(6, 6) DEFAULT 0.49, col_5417 text COLLATE utf8_bin, col_5418 float DEFAULT 2048.0762299371554, col_5419 int UNSIGNED NOT NULL DEFAULT 3152326370, PRIMARY KEY (col_5419)) PARTITION BY HASH (col_5419) PARTITIONS 3";
    let expected = "CREATE TABLE `tbl_936` (`col_5410` SMALLINT NOT NULL,`col_5411` DOUBLE,`col_5412` TINYINT(1) NOT NULL DEFAULT 1,`col_5413` SET('Alice','Bob','Charlie','David') NOT NULL DEFAULT _UTF8MB4'Charlie',`col_5414` VARBINARY(147) COLLATE binary DEFAULT _UTF8MB4'bvpKgYWLfyuTiOYSkj',`col_5415` TIMESTAMP NOT NULL DEFAULT _UTF8MB4'2021-07-06',`col_5416` DECIMAL(6,6) DEFAULT 0.49,`col_5417` TEXT COLLATE utf8_bin,`col_5418` FLOAT DEFAULT 2048.0762299371554,`col_5419` INT UNSIGNED NOT NULL DEFAULT 3152326370,PRIMARY KEY(`col_5419`)) PARTITION BY HASH (`col_5419`) PARTITIONS 3";
    assert_eq!(r(sql), expected);
}

#[test]
fn serial_expands_at_the_column_envelope_and_option_loop_boundaries() {
    // `pkg/parser/parser_test.go:TestDDL`'s SERIAL rows. The first is Go's
    // column-definition pseudo-type; the latter is `SERIAL DEFAULT VALUE`
    // from the shared option loop.
    for (sql, expected) in [
        (
            "create table t (a serial)",
            "CREATE TABLE `t` (`a` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE KEY)",
        ),
        (
            "create table t (a serial null)",
            "CREATE TABLE `t` (`a` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE KEY NULL)",
        ),
        (
            "create table t (a int serial default value null)",
            "CREATE TABLE `t` (`a` INT NOT NULL AUTO_INCREMENT UNIQUE KEY NULL)",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
    assert!(matches!(
        first_column_options("create table t (a int serial default value)")[..],
        [
            tidb_ast::ColumnOption::NotNull,
            tidb_ast::ColumnOption::AutoIncrement,
            tidb_ast::ColumnOption::InlineKey(tidb_ast::InlineKeyOption {
                kind: tidb_ast::InlineKeyKind::Unique,
                global: false,
            }),
        ]
    ));
}

#[test]
fn column_format_storage_and_secondary_attribute_are_typed_and_closed() {
    // Go `TestDDL` / `TestSecondaryEngineAttribute` accepted rows, and the
    // restore rows in `ast/ddl_test.go:TestDDLColumnOptionRestore`.
    for (sql, expected) in [
        (
            "create table t (a int column_format fixed storage disk)",
            "CREATE TABLE `t` (`a` INT COLUMN_FORMAT FIXED STORAGE DISK)",
        ),
        (
            "create table t (a int column_format default storage memory)",
            "CREATE TABLE `t` (`a` INT COLUMN_FORMAT DEFAULT STORAGE MEMORY)",
        ),
        (
            "create table t (a int secondary_engine_attribute '{\"key\":\"value\"}')",
            "CREATE TABLE `t` (`a` INT SECONDARY_ENGINE_ATTRIBUTE = '{\"key\":\"value\"}')",
        ),
        // Exact `TestDDL` column-format rows, including the shared ALTER
        // column definition route.
        (
            "create table t (a int column_format fixed)",
            "CREATE TABLE `t` (`a` INT COLUMN_FORMAT FIXED)",
        ),
        (
            "create table t (a int column_format default)",
            "CREATE TABLE `t` (`a` INT COLUMN_FORMAT DEFAULT)",
        ),
        (
            "create table t (a int column_format dynamic)",
            "CREATE TABLE `t` (`a` INT COLUMN_FORMAT DYNAMIC)",
        ),
        (
            "alter table t modify column a bigint column_format default",
            "ALTER TABLE `t` MODIFY COLUMN `a` BIGINT COLUMN_FORMAT DEFAULT",
        ),
        // Exact column-level `TestSecondaryEngineAttribute` accepted row.
        (
            "CREATE TABLE t (id INT SECONDARY_ENGINE_ATTRIBUTE='{\"key\":\"value\"}')",
            "CREATE TABLE `t` (`id` INT SECONDARY_ENGINE_ATTRIBUTE = '{\"key\":\"value\"}')",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
    for sql in [
        "create table t (a int column_format compressed)",
        "create table t (a int storage ssd)",
        // Exact column-level `TestSecondaryEngineAttribute` rejection rows.
        "CREATE TABLE t (id INT SECONDARY_ENGINE_ATTRIBUTE)",
        "CREATE TABLE t (id INT SECONDARY_ENGINE_ATTRIBUTE=)",
    ] {
        assert!(parse(sql).is_err(), "source SQL: {sql}");
    }
}

/// The column-level rows in Go `parser_test.go:TestSecondaryEngineAttribute`
/// share the same optional-equals and string-literal boundary as the table
/// option, but they are owned by `parseColumnOptions` and restore through a
/// distinct `ColumnOption` payload.  Keep this matrix separate from the
/// table/partition/index rows so the broad Go test remains honestly partial.
#[test]
fn go_parser_test_secondary_engine_attribute_column_rows_keep_scalar_boundaries() {
    let (sql, expected, value) = (
        r#"CREATE TABLE t (id INT SECONDARY_ENGINE_ATTRIBUTE='{"key":"value"}')"#,
        r#"CREATE TABLE `t` (`id` INT SECONDARY_ENGINE_ATTRIBUTE = '{"key":"value"}')"#,
        "{\"key\":\"value\"}",
    );
    assert_eq!(r(sql), expected, "source SQL: {sql}");
    assert_eq!(
        first_column_options(sql),
        vec![tidb_ast::ColumnOption::SecondaryEngineAttribute(
            value.to_owned()
        )],
        "source SQL: {sql}"
    );

    // Go's source row also accepts the no-equals spelling through the same
    // `StringLit` production; restore always inserts ` = `.
    let sql = "CREATE TABLE t (id INT SECONDARY_ENGINE_ATTRIBUTE '{\"key\":\"value2\"}')";
    assert_eq!(
        r(sql),
        "CREATE TABLE `t` (`id` INT SECONDARY_ENGINE_ATTRIBUTE = '{\"key\":\"value2\"}')"
    );
    assert_eq!(
        first_column_options(sql),
        vec![tidb_ast::ColumnOption::SecondaryEngineAttribute(
            "{\"key\":\"value2\"}".to_owned()
        )]
    );

    // The option is string-literal-only.  Bare identifiers, numeric values,
    // missing payloads, and a bare keyword all stop at the same parser
    // boundary as Go's `parseColumnOptions`.
    for sql in [
        "CREATE TABLE t (id INT SECONDARY_ENGINE_ATTRIBUTE = myattr)",
        "CREATE TABLE t (id INT SECONDARY_ENGINE_ATTRIBUTE = 1)",
        "CREATE TABLE t (id INT SECONDARY_ENGINE_ATTRIBUTE=)",
        "CREATE TABLE t (id INT SECONDARY_ENGINE_ATTRIBUTE)",
    ] {
        assert!(parse(sql).is_err(), "Go rejects column option SQL: {sql}");
    }
}

/// Scalar options that are easy to lose when the parser is split into
/// generated/default/key leaves stay source-addressed here.  The cases are
/// the direct column forms from `TestDDL`; the typed assertions make sure a
/// successful restore is not merely accepting and dropping the option.
#[test]
fn go_parser_test_ddl_scalar_column_options_are_retained_in_source_order() {
    for (sql, expected, options) in [
        (
            "CREATE TABLE t (a INT NOT NULL NULL)",
            "CREATE TABLE `t` (`a` INT NOT NULL NULL)",
            vec![
                tidb_ast::ColumnOption::NotNull,
                tidb_ast::ColumnOption::Null,
            ],
        ),
        (
            "CREATE TABLE t (a INT AUTO_INCREMENT COMMENT 'id')",
            "CREATE TABLE `t` (`a` INT AUTO_INCREMENT COMMENT 'id')",
            vec![
                tidb_ast::ColumnOption::AutoIncrement,
                tidb_ast::ColumnOption::Comment("id".to_owned()),
            ],
        ),
        (
            "CREATE TABLE t (a VARCHAR(8) COLLATE utf8_bin)",
            "CREATE TABLE `t` (`a` VARCHAR(8) COLLATE utf8_bin)",
            vec![tidb_ast::ColumnOption::Collate("utf8_bin".to_owned())],
        ),
        (
            "CREATE TABLE t (a INT COLUMN_FORMAT DYNAMIC STORAGE MEMORY)",
            "CREATE TABLE `t` (`a` INT COLUMN_FORMAT DYNAMIC STORAGE MEMORY)",
            vec![
                tidb_ast::ColumnOption::ColumnFormat(tidb_ast::ColumnFormat::Dynamic),
                tidb_ast::ColumnOption::Storage(tidb_ast::ColumnStorage::Memory),
            ],
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
        assert_eq!(first_column_options(sql), options, "source SQL: {sql}");
    }

    // Invalid values must not be silently treated as the end of the option
    // list, otherwise the enclosing CREATE TABLE would accept a truncated
    // definition that Go rejects.
    for sql in [
        "CREATE TABLE t (a INT NOT)",
        "CREATE TABLE t (a INT COMMENT)",
        "CREATE TABLE t (a VARCHAR(8) COLLATE no_such_collation)",
        "CREATE TABLE t (a INT COLUMN_FORMAT COMPRESSED)",
        "CREATE TABLE t (a INT STORAGE SSD)",
    ] {
        assert!(
            parse(sql).is_err(),
            "Go rejects scalar column option SQL: {sql}"
        );
    }
}

/// Go's `parseColumnOptions` has an intentionally asymmetric duplicate-
/// COLLATE rule: the yacc base production rejects a second COLLATE only when
/// the first column option is itself COLLATE. Once another option precedes it,
/// the recursive production does not update the duplicate flag and both
/// collations remain restore-visible.
#[test]
fn duplicate_column_collate_keeps_go_base_production_boundary() {
    assert!(parse("CREATE TABLE t (a VARCHAR(8) COLLATE utf8_bin COLLATE utf8mb4_bin)").is_err());
    assert!(
        parse("CREATE TABLE t (a VARCHAR(8) COLLATE utf8_bin NOT NULL COLLATE utf8mb4_bin)")
            .is_err()
    );

    let sql = "CREATE TABLE t (a VARCHAR(8) NOT NULL COLLATE utf8_bin COLLATE utf8mb4_bin)";
    assert_eq!(
        r(sql),
        "CREATE TABLE `t` (`a` VARCHAR(8) NOT NULL COLLATE utf8_bin COLLATE utf8mb4_bin)"
    );
    assert_eq!(
        first_column_options(sql),
        vec![
            tidb_ast::ColumnOption::NotNull,
            tidb_ast::ColumnOption::Collate("utf8_bin".to_owned()),
            tidb_ast::ColumnOption::Collate("utf8mb4_bin".to_owned()),
        ]
    );
}

#[test]
fn auto_random_retains_each_argument_and_uses_go_special_comment_restore() {
    // `pkg/parser/parser_test.go:TestDDL` and
    // `pkg/parser/ast/ddl_test.go:TestDDLColumnOptionRestore`.
    for (sql, expected, option) in [
        (
            "create table t (a bigint auto_random primary key)",
            "CREATE TABLE `t` (`a` BIGINT AUTO_RANDOM PRIMARY KEY)",
            tidb_ast::AutoRandomOption {
                shard_bits: None,
                range_bits: None,
            },
        ),
        (
            "create table t (a bigint auto_random(3) primary key)",
            "CREATE TABLE `t` (`a` BIGINT AUTO_RANDOM(3) PRIMARY KEY)",
            tidb_ast::AutoRandomOption {
                shard_bits: Some(3),
                range_bits: None,
            },
        ),
        (
            "create table t (a bigint auto_random(5, 53) primary key)",
            "CREATE TABLE `t` (`a` BIGINT AUTO_RANDOM(5, 53) PRIMARY KEY)",
            tidb_ast::AutoRandomOption {
                shard_bits: Some(5),
                range_bits: Some(53),
            },
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
        assert!(first_column_options(sql).contains(&tidb_ast::ColumnOption::AutoRandom(option)));
    }
    let statement = parse("create table t (a bigint auto_random(5, 53))").unwrap();
    assert_eq!(
        statement.restore_with_flags(
            tidb_ast::RestoreFlags::DEFAULT | tidb_ast::RestoreFlags::TIDB_SPECIAL_COMMENT,
        ),
        "CREATE TABLE `t` (`a` BIGINT /*T![auto_rand] AUTO_RANDOM(5, 53) */)"
    );
    for sql in [
        "create table t (a bigint auto_random())",
        "create table t (a bigint auto_random(3,))",
        "create table t (a bigint auto_random(3, 5, 7))",
    ] {
        assert!(parse(sql).is_err(), "source SQL: {sql}");
    }
}

#[test]
fn mariadb_row_markers_require_the_same_explicit_parser_mode_as_go() {
    // Every accepted row from
    // `pkg/parser/parser_test.go:TestSystemVersionedColumnMariaDBEnabled`.
    // The ALTER cases prove that the shared column entrypoint, rather than a
    // CREATE-only fork, reaches the generated-option leaf.
    for (sql, expected) in [
        (
            "CREATE TABLE t (a TIMESTAMP(6) GENERATED ALWAYS AS ROW START)",
            "CREATE TABLE `t` (`a` TIMESTAMP(6) GENERATED ALWAYS AS ROW START)",
        ),
        (
            "CREATE TABLE t (a TIMESTAMP(6) GENERATED ALWAYS AS ROW END)",
            "CREATE TABLE `t` (`a` TIMESTAMP(6) GENERATED ALWAYS AS ROW END)",
        ),
        (
            "CREATE TABLE t (a TIMESTAMP(6) NOT NULL GENERATED ALWAYS AS ROW START)",
            "CREATE TABLE `t` (`a` TIMESTAMP(6) NOT NULL GENERATED ALWAYS AS ROW START)",
        ),
        (
            "CREATE TABLE t (a TIMESTAMP(6) AS ROW START)",
            "CREATE TABLE `t` (`a` TIMESTAMP(6) GENERATED ALWAYS AS ROW START)",
        ),
        (
            "CREATE TABLE t (a TIMESTAMP(6) AS ROW END)",
            "CREATE TABLE `t` (`a` TIMESTAMP(6) GENERATED ALWAYS AS ROW END)",
        ),
        (
            "ALTER TABLE t MODIFY COLUMN a TIMESTAMP(6) GENERATED ALWAYS AS ROW START",
            "ALTER TABLE `t` MODIFY COLUMN `a` TIMESTAMP(6) GENERATED ALWAYS AS ROW START",
        ),
        (
            "ALTER TABLE t CHANGE COLUMN a a TIMESTAMP(6) GENERATED ALWAYS AS ROW END",
            "ALTER TABLE `t` CHANGE COLUMN `a` `a` TIMESTAMP(6) GENERATED ALWAYS AS ROW END",
        ),
        (
            "ALTER TABLE t ADD COLUMN a TIMESTAMP(6) GENERATED ALWAYS AS ROW START",
            "ALTER TABLE `t` ADD COLUMN `a` TIMESTAMP(6) GENERATED ALWAYS AS ROW START",
        ),
    ] {
        assert_eq!(
            crate::parse_with_mariadb(sql, true)
                .expect("Go MariaDB mode row marker")
                .restore(),
            expected,
            "source SQL: {sql}"
        );
    }

    // Every disabled-mode row from the companion Go test.
    for sql in [
        "CREATE TABLE t (a TIMESTAMP(6) GENERATED ALWAYS AS ROW START)",
        "CREATE TABLE t (a TIMESTAMP(6) GENERATED ALWAYS AS ROW END)",
        "ALTER TABLE t MODIFY COLUMN a TIMESTAMP(6) GENERATED ALWAYS AS ROW START",
        "ALTER TABLE t CHANGE COLUMN a a TIMESTAMP(6) GENERATED ALWAYS AS ROW END",
    ] {
        assert!(parse(sql).is_err(), "source SQL: {sql}");
    }

    assert!(crate::parse_with_mariadb(
        "create table t (a timestamp(6) generated always as row middle)",
        true,
    )
    .is_err());
}
