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

//! Default-restore original-test rows attributable to
//! `pkg/parser/ddl_index_parser.go`.
//!
//! The context-sensitive source rows live in [`super::restore_context`], so
//! each copied Go row has exactly one executable Rust owner.

use super::*;

type Case = (&'static str, bool, &'static str);

fn assert_cases(cases: &[Case]) {
    for (sql, accepted, expected) in cases {
        match (parse(sql), accepted) {
            (Ok(statement), true) => {
                assert_eq!(statement.restore(), *expected, "source SQL: {sql}")
            }
            (Err(_), false) => {}
            (Ok(statement), false) => {
                panic!(
                    "Go rejects but Rust accepted {sql}: {}",
                    statement.restore()
                )
            }
            (Err(error), true) => panic!("Go accepts but Rust rejected {sql}: {error:?}"),
        }
    }
}

fn assert_fragment_cases(
    sql_prefix: &str,
    sql_suffix: &str,
    expected_prefix: &str,
    expected_suffix: &str,
    cases: &[(&str, &str)],
) {
    for (source, expected) in cases {
        let sql = format!("{sql_prefix}{source}{sql_suffix}");
        let expected = format!("{expected_prefix}{expected}{expected_suffix}");
        assert_eq!(r(&sql), expected, "source fragment: {source}");
    }
}

/// Direct boundary cases from `ddl_index_parser.go`: every index name/part
/// slot uses Go `isIdentLike`, references use an exact `[schema.]table`, and
/// incomplete referential actions materialize the zero option instead of
/// becoming a Rust-only hard error.
#[test]
fn hand_index_parser_identifier_and_reference_boundaries_match_go() {
    for (sql, expected) in [
        (
            "create table t(a int, unique type (a), index ('a'))",
            "CREATE TABLE `t` (`a` INT,UNIQUE `type`(`a`),INDEX(`a`))",
        ),
        (
            "create table t(a int, constraint 'c' unique(a))",
            "CREATE TABLE `t` (`a` INT,UNIQUE `c`(`a`))",
        ),
        (
            "create table t(a int, foreign key(a) references x on delete)",
            "CREATE TABLE `t` (`a` INT,CONSTRAINT FOREIGN KEY (`a`) REFERENCES `x`)",
        ),
        (
            "create table t(a int, foreign key(a) references x on delete set)",
            "CREATE TABLE `t` (`a` INT,CONSTRAINT FOREIGN KEY (`a`) REFERENCES `x`)",
        ),
        (
            "create table t(a int, foreign key(a) references x match on delete cascade)",
            "CREATE TABLE `t` (`a` INT,CONSTRAINT FOREIGN KEY (`a`) REFERENCES `x` ON DELETE CASCADE)",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
    assert!(parse("create table t(a int, foreign key(a) references db.t.extra)").is_err());

    for (sql, expected) in [
        ("create index on t(a)", "CREATE INDEX `` ON `t` (`a`)"),
        ("create index 'i' on t(a)", "CREATE INDEX `i` ON `t` (`a`)"),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
    assert!(parse("create index i on db.t.extra(a)").is_err());
}

#[test]
fn hand_index_parser_source_warnings_match_go() {
    for (sql, expected) in [
        (
            "create table t(a int, foreign key(a) references x match full)",
            "The MATCH clause is parsed but ignored by all storage engines.",
        ),
        (
            "create table t(a int, foreign key(a) references x on delete set default)",
            "The SET DEFAULT clause is parsed but ignored by all storage engines.",
        ),
        (
            "create index i on t(a) with parser p",
            "The WITH PARASER clause is parsed but ignored by all storage engines.",
        ),
    ] {
        let output = parse_with_warnings(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        assert_eq!(output.warnings.len(), 1, "source SQL: {sql}");
        assert_eq!(output.warnings[0].message, expected, "source SQL: {sql}");
    }
}

/// All rows from Go `ast/ddl_test.go:68 TestDDLIndexColNameRestore`.
#[test]
fn go_ast_test_ddl_index_col_name_restore() {
    let cases = [
        ("(a + 1)", "(`a`+1)"),
        ("(1 * 1 + (1 + 1))", "(1*1+(1+1))"),
        ("((1 * 1 + (1 + 1)))", "((1*1+(1+1)))"),
    ];
    assert_eq!(cases.len(), 3);
    assert_fragment_cases(
        "CREATE INDEX idx ON t (",
        ") USING HASH",
        "CREATE INDEX `idx` ON `t` (",
        ") USING HASH",
        &cases,
    );
}

/// All rows from Go `ast/ddl_test.go:80 TestDDLIndexExprRestore`.
#[test]
fn go_ast_test_ddl_index_expr_restore() {
    let cases = [("world", "`world`"), ("world(2)", "`world`(2)")];
    assert_eq!(cases.len(), 2);
    assert_fragment_cases(
        "CREATE INDEX idx ON t (",
        ") USING HASH",
        "CREATE INDEX `idx` ON `t` (",
        ") USING HASH",
        &cases,
    );
}

/// All source actions from Go `ast/ddl_test.go:91 TestDDLOnDeleteRestore`.
#[test]
fn go_ast_test_ddl_on_delete_restore() {
    let actions = [
        ("on delete restrict", "ON DELETE RESTRICT"),
        ("on delete CASCADE", "ON DELETE CASCADE"),
        ("on delete SET NULL", "ON DELETE SET NULL"),
        ("on delete no action", "ON DELETE NO ACTION"),
    ];
    assert_eq!(actions.len(), 4);
    for (source, expected) in actions {
        for (clause, expected_clause) in [
            (source.to_string(), expected.to_string()),
            (
                format!("on update CASCADE {source}"),
                format!("{expected} ON UPDATE CASCADE"),
            ),
            (
                format!("{source} on update CASCADE"),
                format!("{expected} ON UPDATE CASCADE"),
            ),
        ] {
            let sql = format!("CREATE TABLE child (id INT, parent_id INT, INDEX par_ind (parent_id), FOREIGN KEY (parent_id) REFERENCES parent(id) {clause})");
            let restored = format!("CREATE TABLE `child` (`id` INT,`parent_id` INT,INDEX `par_ind`(`parent_id`),CONSTRAINT FOREIGN KEY (`parent_id`) REFERENCES `parent`(`id`) {expected_clause})");
            assert_eq!(r(&sql), restored, "source SQL: {sql}");
        }
    }
}

/// All source actions from Go `ast/ddl_test.go:106 TestDDLOnUpdateRestore`.
#[test]
fn go_ast_test_ddl_on_update_restore() {
    let actions = [
        ("ON UPDATE RESTRICT", "ON UPDATE RESTRICT"),
        ("on update CASCADE", "ON UPDATE CASCADE"),
        ("on update SET NULL", "ON UPDATE SET NULL"),
        ("on update no action", "ON UPDATE NO ACTION"),
    ];
    assert_eq!(actions.len(), 4);
    for (source, expected) in actions {
        for clause in [
            format!("ON DELETE CASCADE {source}"),
            format!("{source} ON DELETE CASCADE"),
            source.to_string(),
        ] {
            let sql = format!("CREATE TABLE child (id INT, parent_id INT, INDEX par_ind (parent_id), FOREIGN KEY (parent_id) REFERENCES parent(id) {clause})");
            let expected_clause = if clause.contains("ON DELETE") {
                format!("ON DELETE CASCADE {expected}")
            } else {
                expected.to_string()
            };
            let restored = format!("CREATE TABLE `child` (`id` INT,`parent_id` INT,INDEX `par_ind`(`parent_id`),CONSTRAINT FOREIGN KEY (`parent_id`) REFERENCES `parent`(`id`) {expected_clause})");
            assert_eq!(r(&sql), restored, "source SQL: {sql}");
        }
    }
}

/// All rows from Go `ast/ddl_test.go:121 TestDDLIndexOption`.
#[test]
fn go_ast_test_ddl_index_option() {
    let cases = [
        ("key_block_size=16", "KEY_BLOCK_SIZE=16"),
        ("USING HASH", "USING HASH"),
        ("comment 'hello'", "COMMENT 'hello'"),
        (
            "key_block_size=16 USING HASH",
            "KEY_BLOCK_SIZE=16 USING HASH",
        ),
        (
            "USING HASH KEY_BLOCK_SIZE=16",
            "KEY_BLOCK_SIZE=16 USING HASH",
        ),
        ("USING HASH COMMENT 'foo'", "USING HASH COMMENT 'foo'"),
        ("COMMENT 'foo'", "COMMENT 'foo'"),
        (
            "key_block_size = 32 using hash comment 'hello'",
            "KEY_BLOCK_SIZE=32 USING HASH COMMENT 'hello'",
        ),
        (
            "key_block_size=32 using btree comment 'hello'",
            "KEY_BLOCK_SIZE=32 USING BTREE COMMENT 'hello'",
        ),
    ];
    assert_eq!(cases.len(), 9);
    assert_fragment_cases(
        "CREATE INDEX idx ON t (a) ",
        "",
        "CREATE INDEX `idx` ON `t` (`a`) ",
        "",
        &cases,
    );
}

/// All rows from Go `ast/ddl_test.go:139 TestTableToTableRestore`.
#[test]
fn go_ast_test_table_to_table_restore() {
    let cases = [("t1 to t2", "`t1` TO `t2`")];
    assert_eq!(cases.len(), 1);
    assert_fragment_cases("RENAME TABLE ", "", "RENAME TABLE ", "", &cases);
}

/// All rows from Go `ast/ddl_test.go:149 TestDDLReferenceDefRestore`.
#[test]
fn go_ast_test_ddl_reference_def_restore() {
    let cases = [
        (
            "REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT",
            "REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT",
        ),
        (
            "REFERENCES parent(id) ON DELETE CASCADE",
            "REFERENCES `parent`(`id`) ON DELETE CASCADE",
        ),
        (
            "REFERENCES parent(id,hello) ON DELETE CASCADE",
            "REFERENCES `parent`(`id`, `hello`) ON DELETE CASCADE",
        ),
        (
            "REFERENCES parent(id,hello(12)) ON DELETE CASCADE",
            "REFERENCES `parent`(`id`, `hello`(12)) ON DELETE CASCADE",
        ),
        (
            "REFERENCES parent(id(8),hello(12)) ON DELETE CASCADE",
            "REFERENCES `parent`(`id`(8), `hello`(12)) ON DELETE CASCADE",
        ),
        ("REFERENCES parent(id)", "REFERENCES `parent`(`id`)"),
        ("REFERENCES parent((id+1))", "REFERENCES `parent`((`id`+1))"),
    ];
    assert_eq!(cases.len(), 7);
    assert_fragment_cases(
        "CREATE TABLE child (id INT, parent_id INT, INDEX par_ind (parent_id), FOREIGN KEY (parent_id) ",
        ")",
        "CREATE TABLE `child` (`id` INT,`parent_id` INT,INDEX `par_ind`(`parent_id`),CONSTRAINT FOREIGN KEY (`parent_id`) ",
        ")",
        &cases,
    );
}

/// The 33 default-restore rows from Go `ast/ddl_test.go:165
/// TestDDLConstraintRestore`.
#[test]
fn go_ast_test_ddl_constraint_restore() {
    let cases = [
        ("INDEX par_ind (parent_id)", "INDEX `par_ind`(`parent_id`)"),
        ("INDEX par_ind (parent_id(6))", "INDEX `par_ind`(`parent_id`(6))"),
        ("INDEX expr_ind ((id + parent_id))", "INDEX `expr_ind`((`id`+`parent_id`))"),
        ("INDEX expr_ind ((lower(id)))", "INDEX `expr_ind`((LOWER(`id`)))"),
        ("key par_ind (parent_id)", "INDEX `par_ind`(`parent_id`)"),
        ("key expr_ind ((lower(id)))", "INDEX `expr_ind`((LOWER(`id`)))"),
        ("unique par_ind (parent_id)", "UNIQUE `par_ind`(`parent_id`)"),
        ("unique key par_ind (parent_id)", "UNIQUE `par_ind`(`parent_id`)"),
        ("unique index par_ind (parent_id)", "UNIQUE `par_ind`(`parent_id`)"),
        ("unique expr_ind ((id + parent_id))", "UNIQUE `expr_ind`((`id`+`parent_id`))"),
        ("unique expr_ind ((lower(id)))", "UNIQUE `expr_ind`((LOWER(`id`)))"),
        ("unique key expr_ind ((id + parent_id))", "UNIQUE `expr_ind`((`id`+`parent_id`))"),
        ("unique key expr_ind ((lower(id)))", "UNIQUE `expr_ind`((LOWER(`id`)))"),
        ("unique index expr_ind ((id + parent_id))", "UNIQUE `expr_ind`((`id`+`parent_id`))"),
        ("unique index expr_ind ((lower(id)))", "UNIQUE `expr_ind`((LOWER(`id`)))"),
        ("fulltext key full_id (parent_id)", "FULLTEXT `full_id`(`parent_id`)"),
        ("fulltext INDEX full_id (parent_id)", "FULLTEXT `full_id`(`parent_id`)"),
        ("fulltext INDEX full_id ((parent_id+1))", "FULLTEXT `full_id`((`parent_id`+1))"),
        ("PRIMARY KEY (id)", "PRIMARY KEY(`id`)"),
        ("PRIMARY KEY (id) key_block_size = 32 using hash comment 'hello'", "PRIMARY KEY(`id`) KEY_BLOCK_SIZE=32 USING HASH COMMENT 'hello'"),
        ("PRIMARY KEY ((id+1))", "PRIMARY KEY((`id`+1))"),
        ("CONSTRAINT FOREIGN KEY (parent_id(2),hello(4)) REFERENCES parent(id) ON DELETE CASCADE", "CONSTRAINT FOREIGN KEY (`parent_id`(2), `hello`(4)) REFERENCES `parent`(`id`) ON DELETE CASCADE"),
        ("CONSTRAINT FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT", "CONSTRAINT FOREIGN KEY (`parent_id`) REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT"),
        ("CONSTRAINT FOREIGN KEY (parent_id(2),hello(4)) REFERENCES parent((id+1)) ON DELETE CASCADE", "CONSTRAINT FOREIGN KEY (`parent_id`(2), `hello`(4)) REFERENCES `parent`((`id`+1)) ON DELETE CASCADE"),
        ("CONSTRAINT FOREIGN KEY (parent_id) REFERENCES parent((id+1)) ON DELETE CASCADE ON UPDATE RESTRICT", "CONSTRAINT FOREIGN KEY (`parent_id`) REFERENCES `parent`((`id`+1)) ON DELETE CASCADE ON UPDATE RESTRICT"),
        ("CONSTRAINT fk_123 FOREIGN KEY (parent_id(2),hello(4)) REFERENCES parent(id) ON DELETE CASCADE", "CONSTRAINT `fk_123` FOREIGN KEY (`parent_id`(2), `hello`(4)) REFERENCES `parent`(`id`) ON DELETE CASCADE"),
        ("CONSTRAINT fk_123 FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT", "CONSTRAINT `fk_123` FOREIGN KEY (`parent_id`) REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT"),
        ("CONSTRAINT fk_123 FOREIGN KEY ((parent_id+1),hello(4)) REFERENCES parent(id) ON DELETE CASCADE", "CONSTRAINT `fk_123` FOREIGN KEY ((`parent_id`+1), `hello`(4)) REFERENCES `parent`(`id`) ON DELETE CASCADE"),
        ("CONSTRAINT fk_123 FOREIGN KEY ((parent_id+1)) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT", "CONSTRAINT `fk_123` FOREIGN KEY ((`parent_id`+1)) REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT"),
        ("FOREIGN KEY (parent_id(2),hello(4)) REFERENCES parent(id) ON DELETE CASCADE", "CONSTRAINT FOREIGN KEY (`parent_id`(2), `hello`(4)) REFERENCES `parent`(`id`) ON DELETE CASCADE"),
        ("FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT", "CONSTRAINT FOREIGN KEY (`parent_id`) REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT"),
        ("FOREIGN KEY ((parent_id+1),hello(4)) REFERENCES parent(id) ON DELETE CASCADE", "CONSTRAINT FOREIGN KEY ((`parent_id`+1), `hello`(4)) REFERENCES `parent`(`id`) ON DELETE CASCADE"),
        ("FOREIGN KEY ((parent_id+1)) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT", "CONSTRAINT FOREIGN KEY ((`parent_id`+1)) REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT"),
    ];
    assert_eq!(cases.len(), 33);
    assert_fragment_cases(
        "CREATE TABLE child (id INT, parent_id INT, ",
        ")",
        "CREATE TABLE `child` (`id` INT,`parent_id` INT,",
        ")",
        &cases,
    );
}

/// The `REFERENCES` row from Go `ast/ddl_test.go:217 TestDDLColumnOptionRestore`.
#[test]
fn go_ast_test_ddl_column_option_reference_row() {
    assert_cases(&[(
        "CREATE TABLE child (id INT REFERENCES parent(id))",
        true,
        "CREATE TABLE `child` (`id` INT REFERENCES `parent`(`id`))",
    )]);
}

/// The `REFERENCES` row from Go `ast/ddl_test.go:267 TestDDLColumnDefRestore`.
#[test]
fn go_ast_test_ddl_column_def_reference_row() {
    assert_cases(&[(
        "CREATE TABLE t (id INT(11) REFERENCES parent(id))",
        true,
        "CREATE TABLE `t` (`id` INT(11) REFERENCES `parent`(`id`))",
    )]);
}

/// Index/constraint rows from Go `ast/ddl_test.go:427 TestAlterTableSpecRestore`.
#[test]
fn go_ast_test_alter_table_spec_index_rows() {
    let fragments = [
        ("ADD CONSTRAINT INDEX par_ind (parent_id)", "ADD INDEX `par_ind`(`parent_id`)"),
        ("ADD CONSTRAINT INDEX par_ind (parent_id(6))", "ADD INDEX `par_ind`(`parent_id`(6))"),
        ("ADD CONSTRAINT key par_ind (parent_id)", "ADD INDEX `par_ind`(`parent_id`)"),
        ("ADD CONSTRAINT unique par_ind (parent_id)", "ADD UNIQUE `par_ind`(`parent_id`)"),
        ("ADD CONSTRAINT unique key par_ind (parent_id)", "ADD UNIQUE `par_ind`(`parent_id`)"),
        ("ADD CONSTRAINT unique index par_ind (parent_id)", "ADD UNIQUE `par_ind`(`parent_id`)"),
        ("ADD CONSTRAINT fulltext key full_id (parent_id)", "ADD FULLTEXT `full_id`(`parent_id`)"),
        ("ADD CONSTRAINT fulltext INDEX full_id (parent_id)", "ADD FULLTEXT `full_id`(`parent_id`)"),
        ("ADD CONSTRAINT PRIMARY KEY (id)", "ADD PRIMARY KEY(`id`)"),
        ("ADD CONSTRAINT PRIMARY KEY (id) key_block_size = 32 using hash comment 'hello'", "ADD PRIMARY KEY(`id`) KEY_BLOCK_SIZE=32 USING HASH COMMENT 'hello'"),
        ("ADD CONSTRAINT FOREIGN KEY (parent_id(2),hello(4)) REFERENCES parent(id) ON DELETE CASCADE", "ADD CONSTRAINT FOREIGN KEY (`parent_id`(2), `hello`(4)) REFERENCES `parent`(`id`) ON DELETE CASCADE"),
        ("ADD CONSTRAINT FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT", "ADD CONSTRAINT FOREIGN KEY (`parent_id`) REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT"),
        ("ADD CONSTRAINT fk_123 FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT", "ADD CONSTRAINT `fk_123` FOREIGN KEY (`parent_id`) REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT"),
    ];
    assert_eq!(fragments.len(), 13);
    assert_fragment_cases("ALTER TABLE t ", "", "ALTER TABLE `t` ", "", &fragments);
}

/// Three index-owned cases from Go `ast/ddl_test.go:655 TestIfExistsRestore`.
#[test]
fn go_ast_test_if_exists_index_rows() {
    assert_cases(&[
        ("create unique index if not exists idx on t(c)", true, "CREATE UNIQUE INDEX IF NOT EXISTS `idx` ON `t` (`c`)"),
        ("alter table t add key if not exists idx2(c2), add vector index if not exists idx3(c3), add columnar index if not exists idx4(c4)", true, "ALTER TABLE `t` ADD INDEX IF NOT EXISTS `idx2`(`c2`), ADD VECTOR INDEX IF NOT EXISTS `idx3`(`c3`), ADD COLUMNAR INDEX IF NOT EXISTS `idx4`(`c4`)"),
        ("alter table t add foreign key if not exists fk(c) references t2(c)", true, "ALTER TABLE `t` ADD CONSTRAINT `fk` FOREIGN KEY IF NOT EXISTS (`c`) REFERENCES `t2`(`c`)"),
    ]);
}

/// Index-level rows from Go `parser_test.go:8278 TestSecondaryEngineAttribute`.
#[test]
fn go_parser_test_secondary_engine_attribute_index_rows() {
    assert_cases(&[
        ("CREATE TABLE t (id INT,INDEX idx (id) INVISIBLE SECONDARY_ENGINE_ATTRIBUTE='{\"key1\":\"value1\"}')", true, "CREATE TABLE `t` (`id` INT,INDEX `idx`(`id`) INVISIBLE SECONDARY_ENGINE_ATTRIBUTE = '{\"key1\":\"value1\"}')"),
        ("CREATE TABLE t (id INT, INDEX idx (id) SECONDARY_ENGINE_ATTRIBUTE=)", false, ""),
        ("CREATE TABLE t (id INT, INDEX idx (id) SECONDARY_ENGINE_ATTRIBUTE)", false, ""),
        ("CREATE INDEX i ON t (a) SECONDARY_ENGINE_ATTRIBUTE = '{}'", true, "CREATE INDEX `i` ON `t` (`a`) SECONDARY_ENGINE_ATTRIBUTE = '{}'"),
        ("CREATE INDEX i ON t (a) SECONDARY_ENGINE_ATTRIBUTE '{}'", true, "CREATE INDEX `i` ON `t` (`a`) SECONDARY_ENGINE_ATTRIBUTE = '{}'"),
        ("CREATE INDEX i ON t (a) SECONDARY_ENGINE_ATTRIBUTE", false, ""),
    ]);
}

/// Every row from Go `parser_test.go:8425 TestPartialIndex`.
#[test]
fn go_parser_test_partial_index() {
    assert_cases(&[
        (
            "create table `t` (`id` int primary key,`col` int,index(`col`) where `col`>100)",
            true,
            "CREATE TABLE `t` (`id` INT PRIMARY KEY,`col` INT,INDEX(`col`) WHERE `col`>100)",
        ),
        (
            "create index `idx` on `t` (`col`) where `col`>100",
            true,
            "CREATE INDEX `idx` ON `t` (`col`) WHERE `col`>100",
        ),
        (
            "alter table `t` add index `idx`(`col`) where `col`>100",
            true,
            "ALTER TABLE `t` ADD INDEX `idx`(`col`) WHERE `col`>100",
        ),
    ]);
}

/// All 57 rows in the contiguous CREATE INDEX section of broad Go
/// `parser_test.go:2584 TestDDL` (currently around source line 3475).
///
/// This is the complete source-owned CREATE INDEX block, not a claim that the
/// index wave owns the rest of that mixed DDL table.
#[test]
fn go_parser_test_ddl_create_index_block() {
    let cases = [
        (
            "CREATE INDEX idx ON t (a)",
            true,
            "CREATE INDEX `idx` ON `t` (`a`)",
        ),
        (
            "CREATE INDEX IF NOT EXISTS idx ON t (a)",
            true,
            "CREATE INDEX IF NOT EXISTS `idx` ON `t` (`a`)",
        ),
        (
            "CREATE UNIQUE INDEX idx ON t (a)",
            true,
            "CREATE UNIQUE INDEX `idx` ON `t` (`a`)",
        ),
        (
            "CREATE UNIQUE INDEX IF NOT EXISTS idx ON t (a)",
            true,
            "CREATE UNIQUE INDEX IF NOT EXISTS `idx` ON `t` (`a`)",
        ),
        (
            "CREATE UNIQUE INDEX ident ON d_n.t_n ( ident , ident ASC ) TYPE BTREE",
            true,
            "CREATE UNIQUE INDEX `ident` ON `d_n`.`t_n` (`ident`, `ident`) USING BTREE",
        ),
        (
            "CREATE UNIQUE INDEX ident ON d_n.t_n ( ident , ident ASC ) TYPE HASH",
            true,
            "CREATE UNIQUE INDEX `ident` ON `d_n`.`t_n` (`ident`, `ident`) USING HASH",
        ),
        (
            "CREATE UNIQUE INDEX ident ON d_n.t_n ( ident , ident ASC ) TYPE RTREE",
            true,
            "CREATE UNIQUE INDEX `ident` ON `d_n`.`t_n` (`ident`, `ident`) USING RTREE",
        ),
        (
            "CREATE UNIQUE INDEX ident TYPE BTREE ON d_n.t_n ( ident , ident ASC )",
            true,
            "CREATE UNIQUE INDEX `ident` ON `d_n`.`t_n` (`ident`, `ident`) USING BTREE",
        ),
        (
            "CREATE UNIQUE INDEX ident USING BTREE ON d_n.t_n ( ident , ident ASC )",
            true,
            "CREATE UNIQUE INDEX `ident` ON `d_n`.`t_n` (`ident`, `ident`) USING BTREE",
        ),
        (
            "CREATE SPATIAL INDEX idx ON t (a)",
            true,
            "CREATE SPATIAL INDEX `idx` ON `t` (`a`)",
        ),
        (
            "CREATE SPATIAL INDEX IF NOT EXISTS idx ON t (a)",
            true,
            "CREATE SPATIAL INDEX IF NOT EXISTS `idx` ON `t` (`a`)",
        ),
        (
            "CREATE FULLTEXT INDEX idx ON t (a)",
            true,
            "CREATE FULLTEXT INDEX `idx` ON `t` (`a`)",
        ),
        (
            "CREATE FULLTEXT INDEX IF NOT EXISTS idx ON t (a)",
            true,
            "CREATE FULLTEXT INDEX IF NOT EXISTS `idx` ON `t` (`a`)",
        ),
        (
            "CREATE FULLTEXT INDEX idx ON t (a) WITH PARSER ident",
            true,
            "CREATE FULLTEXT INDEX `idx` ON `t` (`a`) WITH PARSER `ident`",
        ),
        (
            "CREATE FULLTEXT INDEX idx ON t (a) WITH PARSER ident comment 'string'",
            true,
            "CREATE FULLTEXT INDEX `idx` ON `t` (`a`) WITH PARSER `ident` COMMENT 'string'",
        ),
        (
            "CREATE FULLTEXT INDEX idx ON t (a) comment 'string' with parser ident",
            true,
            "CREATE FULLTEXT INDEX `idx` ON `t` (`a`) WITH PARSER `ident` COMMENT 'string'",
        ),
        (
            "CREATE FULLTEXT INDEX idx ON t (a) WITH PARSER ident comment 'string' lock default",
            true,
            "CREATE FULLTEXT INDEX `idx` ON `t` (`a`) WITH PARSER `ident` COMMENT 'string'",
        ),
        (
            "CREATE INDEX idx ON t (a) USING HASH",
            true,
            "CREATE INDEX `idx` ON `t` (`a`) USING HASH",
        ),
        (
            "CREATE INDEX idx ON t (a) COMMENT 'foo'",
            true,
            "CREATE INDEX `idx` ON `t` (`a`) COMMENT 'foo'",
        ),
        (
            "CREATE INDEX idx ON t (a) USING HASH COMMENT 'foo'",
            true,
            "CREATE INDEX `idx` ON `t` (`a`) USING HASH COMMENT 'foo'",
        ),
        (
            "CREATE INDEX idx ON t (a) LOCK=NONE",
            true,
            "CREATE INDEX `idx` ON `t` (`a`) LOCK = NONE",
        ),
        (
            "CREATE INDEX idx USING BTREE ON t (a) USING HASH COMMENT 'foo'",
            true,
            "CREATE INDEX `idx` ON `t` (`a`) USING HASH COMMENT 'foo'",
        ),
        (
            "CREATE INDEX idx USING BTREE ON t (a)",
            true,
            "CREATE INDEX `idx` ON `t` (`a`) USING BTREE",
        ),
        (
            "CREATE INDEX idx ON t ( a ) VISIBLE",
            true,
            "CREATE INDEX `idx` ON `t` (`a`) VISIBLE",
        ),
        (
            "CREATE INDEX idx ON t ( a ) INVISIBLE",
            true,
            "CREATE INDEX `idx` ON `t` (`a`) INVISIBLE",
        ),
        (
            "CREATE INDEX idx ON t ( a ) INVISIBLE VISIBLE",
            true,
            "CREATE INDEX `idx` ON `t` (`a`) VISIBLE",
        ),
        (
            "CREATE INDEX idx ON t ( a ) VISIBLE INVISIBLE",
            true,
            "CREATE INDEX `idx` ON `t` (`a`) INVISIBLE",
        ),
        (
            "CREATE INDEX idx ON t ( a ) USING HASH VISIBLE",
            true,
            "CREATE INDEX `idx` ON `t` (`a`) USING HASH VISIBLE",
        ),
        (
            "CREATE INDEX idx ON t ( a ) USING HASH INVISIBLE",
            true,
            "CREATE INDEX `idx` ON `t` (`a`) USING HASH INVISIBLE",
        ),
        (
            "CREATE VECTOR INDEX idx ON t (a) USING HNSW ",
            true,
            "CREATE VECTOR INDEX `idx` ON `t` (`a`) USING HNSW",
        ),
        (
            "CREATE VECTOR INDEX idx ON t (a, b) USING HNSW ",
            true,
            "CREATE VECTOR INDEX `idx` ON `t` (`a`, `b`) USING HNSW",
        ),
        (
            "CREATE VECTOR INDEX idx ON t ((VEC_COSINE_DISTANCE(a)))",
            true,
            "CREATE VECTOR INDEX `idx` ON `t` ((VEC_COSINE_DISTANCE(`a`)))",
        ),
        (
            "CREATE VECTOR INDEX idx ON t ((VEC_COSINE_DISTANCE(a))) TYPE BTREE",
            true,
            "CREATE VECTOR INDEX `idx` ON `t` ((VEC_COSINE_DISTANCE(`a`))) USING BTREE",
        ),
        (
            "CREATE VECTOR INDEX idx ON t USING HNSW ((VEC_COSINE_DISTANCE(a)))",
            false,
            "",
        ),
        (
            "CREATE VECTOR idx ON t ((VEC_COSINE_DISTANCE(a))) USING HNSW",
            false,
            "",
        ),
        (
            "CREATE VECTOR INDEX idx ON t ((VEC_COSINE_DISTANCE(a)), a) USING HNSW",
            true,
            "CREATE VECTOR INDEX `idx` ON `t` ((VEC_COSINE_DISTANCE(`a`)), `a`) USING HNSW",
        ),
        (
            "CREATE VECTOR INDEX idx ON t (a, (VEC_COSINE_DISTANCE(a))) USING HNSW",
            true,
            "CREATE VECTOR INDEX `idx` ON `t` (`a`, (VEC_COSINE_DISTANCE(`a`))) USING HNSW",
        ),
        (
            "CREATE VECTOR KEY idx ON t ((VEC_COSINE_DISTANCE(a))) USING HNSW",
            false,
            "",
        ),
        (
            "CREATE VECTOR INDEX idx ON t ((VEC_COSINE_DISTANCE(a))) USING HNSW",
            true,
            "CREATE VECTOR INDEX `idx` ON `t` ((VEC_COSINE_DISTANCE(`a`))) USING HNSW",
        ),
        (
            "CREATE VECTOR INDEX IF NOT EXISTS idx ON t ((VEC_COSINE_DISTANCE(a))) USING HNSW",
            true,
            "CREATE VECTOR INDEX IF NOT EXISTS `idx` ON `t` ((VEC_COSINE_DISTANCE(`a`))) USING HNSW",
        ),
        (
            "CREATE VECTOR INDEX IF NOT EXISTS idx ON t ((VEC_COSINE_DISTANCE(a))) TYPE HNSW",
            true,
            "CREATE VECTOR INDEX IF NOT EXISTS `idx` ON `t` ((VEC_COSINE_DISTANCE(`a`))) USING HNSW",
        ),
        (
            "CREATE VECTOR INDEX ident TYPE HNSW ON d_n.t_n ((VEC_COSINE_DISTANCE(a)))",
            true,
            "CREATE VECTOR INDEX `ident` ON `d_n`.`t_n` ((VEC_COSINE_DISTANCE(`a`))) USING HNSW",
        ),
        (
            "CREATE VECTOR INDEX idx USING HNSW ON t ((VEC_COSINE_DISTANCE(a)))",
            true,
            "CREATE VECTOR INDEX `idx` ON `t` ((VEC_COSINE_DISTANCE(`a`))) USING HNSW",
        ),
        (
            "CREATE VECTOR INDEX ident ON d_n.t_n ( ident , ident ASC ) TYPE HNSW",
            true,
            "CREATE VECTOR INDEX `ident` ON `d_n`.`t_n` (`ident`, `ident`) USING HNSW",
        ),
        (
            "CREATE UNIQUE INDEX ident USING HNSW ON d_n.t_n ( ident , ident ASC )",
            true,
            "CREATE UNIQUE INDEX `ident` ON `d_n`.`t_n` (`ident`, `ident`) USING HNSW",
        ),
        (
            "CREATE INDEX idx ON t ( a ) ALGORITHM = DEFAULT",
            true,
            "CREATE INDEX `idx` ON `t` (`a`)",
        ),
        (
            "CREATE INDEX idx ON t ( a ) ALGORITHM DEFAULT",
            true,
            "CREATE INDEX `idx` ON `t` (`a`)",
        ),
        (
            "CREATE INDEX idx ON t ( a ) ALGORITHM = INPLACE",
            true,
            "CREATE INDEX `idx` ON `t` (`a`) ALGORITHM = INPLACE",
        ),
        (
            "CREATE INDEX idx ON t ( a ) ALGORITHM INPLACE",
            true,
            "CREATE INDEX `idx` ON `t` (`a`) ALGORITHM = INPLACE",
        ),
        (
            "CREATE INDEX idx ON t ( a ) ALGORITHM = COPY",
            true,
            "CREATE INDEX `idx` ON `t` (`a`) ALGORITHM = COPY",
        ),
        (
            "CREATE INDEX idx ON t ( a ) ALGORITHM COPY",
            true,
            "CREATE INDEX `idx` ON `t` (`a`) ALGORITHM = COPY",
        ),
        (
            "CREATE INDEX idx ON t ( a ) ALGORITHM = DEFAULT LOCK = DEFAULT",
            true,
            "CREATE INDEX `idx` ON `t` (`a`)",
        ),
        (
            "CREATE INDEX idx ON t ( a ) LOCK = DEFAULT ALGORITHM = DEFAULT",
            true,
            "CREATE INDEX `idx` ON `t` (`a`)",
        ),
        (
            "CREATE INDEX idx ON t ( a ) ALGORITHM = INPLACE LOCK = EXCLUSIVE",
            true,
            "CREATE INDEX `idx` ON `t` (`a`) ALGORITHM = INPLACE LOCK = EXCLUSIVE",
        ),
        (
            "CREATE INDEX idx ON t ( a ) LOCK = EXCLUSIVE ALGORITHM = INPLACE",
            true,
            "CREATE INDEX `idx` ON `t` (`a`) ALGORITHM = INPLACE LOCK = EXCLUSIVE",
        ),
        ("CREATE INDEX idx ON t ( a ) ALGORITHM = ident", false, ""),
        ("CREATE INDEX idx ON t ( a ) ALGORITHM ident", false, ""),
    ];
    assert_eq!(cases.len(), 57);
    assert_cases(&cases);
}
