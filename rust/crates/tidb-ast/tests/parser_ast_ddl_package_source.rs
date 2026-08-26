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

//! Ports of `pkg/parser/ast/ddl_test.go` (origin/master).
//!
//! Every `runNodeRestoreTest` case drives the same extraction the Go
//! closure performs on the equivalent Rust node; the visitor row lives in
//! the in-crate `tests_ddl_package_source` module.

use crate::parser_ast_node_restore_source::{
    case, expect_ddl, run_node_restore_test, run_node_restore_test_with_flags,
    run_node_restore_test_with_flags_stmt_change, NodeRestoreCase,
};
use tidb_ast::{DdlStmt, RestoreContext, RestoreFlags, Stmt};

const SPECIAL_COMMENT: RestoreFlags = RestoreFlags::TIDB_SPECIAL_COMMENT;

fn whole_statement(stmt: &Stmt, context: &RestoreContext) -> String {
    stmt.restore_with_context(context)
}

fn first_create_index_part(stmt: &Stmt, _context: &RestoreContext) -> String {
    let DdlStmt::CreateIndex(index) = expect_ddl(stmt) else {
        panic!("expected CREATE INDEX, got {stmt:?}");
    };
    index.parts[0].restore()
}

fn index_options_fragment(stmt: &Stmt, _context: &RestoreContext) -> String {
    let DdlStmt::CreateIndex(index) = expect_ddl(stmt) else {
        panic!("expected CREATE INDEX, got {stmt:?}");
    };
    index.options.restore()
}

fn foreign_key_reference(stmt: &Stmt) -> &tidb_ast::ForeignKeyReference {
    match expect_ddl(stmt) {
        DdlStmt::CreateTable(create) => match &create.table_constraints[1] {
            tidb_ast::TableConstraint::ForeignKey(constraint) => &constraint.reference,
            other => panic!("expected a foreign-key constraint, got {other:?}"),
        },
        other => panic!("expected CREATE TABLE, got {other:?}"),
    }
}

fn rename_pair_fragment(stmt: &Stmt, _context: &RestoreContext) -> String {
    let DdlStmt::RenameTable(rename) = expect_ddl(stmt) else {
        panic!("expected RENAME TABLE, got {stmt:?}");
    };
    let (old, new) = &rename.pairs[0];
    let mut out = String::new();
    tidb_ast::push_name_path(&mut out, old);
    out.push_str(" TO ");
    tidb_ast::push_name_path(&mut out, new);
    out
}

/// `pkg/parser/ast/ddl_test.go::TestDDLIndexColNameRestore`.
#[test]
fn ddl_index_col_name_restore() {
    run_node_restore_test(
        "CREATE INDEX idx ON t (%s) USING HASH",
        &[
            case("(a + 1)", "(`a`+1)"),
            case("(1 * 1 + (1 + 1))", "(1*1+(1+1))"),
            case("((1 * 1 + (1 + 1)))", "((1*1+(1+1)))"),
        ],
        first_create_index_part,
    );
}

/// `pkg/parser/ast/ddl_test.go::TestDDLIndexExprRestore`.
#[test]
fn ddl_index_expr_restore() {
    run_node_restore_test(
        "CREATE INDEX idx ON t (%s) USING HASH",
        &[case("world", "`world`"), case("world(2)", "`world`(2)")],
        first_create_index_part,
    );
}

/// `pkg/parser/ast/ddl_test.go::TestDDLOnDeleteRestore`.
#[test]
fn ddl_on_delete_restore() {
    let cases = [
        case("on delete restrict", "ON DELETE RESTRICT"),
        case("on delete CASCADE", "ON DELETE CASCADE"),
        case("on delete SET NULL", "ON DELETE SET NULL"),
        case("on delete no action", "ON DELETE NO ACTION"),
    ];
    for template in [
        "CREATE TABLE child (id INT, parent_id INT, INDEX par_ind (parent_id), FOREIGN KEY (parent_id) REFERENCES parent(id) %s)",
        "CREATE TABLE child (id INT, parent_id INT, INDEX par_ind (parent_id), FOREIGN KEY (parent_id) REFERENCES parent(id) on update CASCADE %s)",
        "CREATE TABLE child (id INT, parent_id INT, INDEX par_ind (parent_id), FOREIGN KEY (parent_id) REFERENCES parent(id) %s on update CASCADE)",
    ] {
        run_node_restore_test(template, &cases, |stmt, _| {
            let reference = foreign_key_reference(stmt);
            format!(
                "ON DELETE {}",
                reference.on_delete.unwrap_or_default().sql()
            )
        });
    }
}

/// `pkg/parser/ast/ddl_test.go::TestDDLOnUpdateRestore`.
#[test]
fn ddl_on_update_restore() {
    let cases = [
        case("ON UPDATE RESTRICT", "ON UPDATE RESTRICT"),
        case("on update CASCADE", "ON UPDATE CASCADE"),
        case("on update SET NULL", "ON UPDATE SET NULL"),
        case("on update no action", "ON UPDATE NO ACTION"),
    ];
    for template in [
        "CREATE TABLE child ( id INT, parent_id INT, INDEX par_ind (parent_id), FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE %s )",
        "CREATE TABLE child ( id INT, parent_id INT, INDEX par_ind (parent_id), FOREIGN KEY (parent_id) REFERENCES parent(id) %s ON DELETE CASCADE)",
        "CREATE TABLE child ( id INT, parent_id INT, INDEX par_ind (parent_id), FOREIGN KEY (parent_id) REFERENCES parent(id)  %s )",
    ] {
        run_node_restore_test(template, &cases, |stmt, _| {
            let reference = foreign_key_reference(stmt);
            format!(
                "ON UPDATE {}",
                reference.on_update.unwrap_or_default().sql()
            )
        });
    }
}

/// `pkg/parser/ast/ddl_test.go::TestDDLIndexOption`.
#[test]
fn ddl_index_option() {
    run_node_restore_test(
        "CREATE INDEX idx ON t (a) %s",
        &[
            case("key_block_size=16", "KEY_BLOCK_SIZE=16"),
            case("USING HASH", "USING HASH"),
            case("comment 'hello'", "COMMENT 'hello'"),
            case(
                "key_block_size=16 USING HASH",
                "KEY_BLOCK_SIZE=16 USING HASH",
            ),
            case(
                "USING HASH KEY_BLOCK_SIZE=16",
                "KEY_BLOCK_SIZE=16 USING HASH",
            ),
            case("USING HASH COMMENT 'foo'", "USING HASH COMMENT 'foo'"),
            case("COMMENT 'foo'", "COMMENT 'foo'"),
            case(
                "key_block_size = 32 using hash comment 'hello'",
                "KEY_BLOCK_SIZE=32 USING HASH COMMENT 'hello'",
            ),
            case(
                "key_block_size=32 using btree comment 'hello'",
                "KEY_BLOCK_SIZE=32 USING BTREE COMMENT 'hello'",
            ),
        ],
        index_options_fragment,
    );
}

/// `pkg/parser/ast/ddl_test.go::TestTableToTableRestore`.
#[test]
fn table_to_table_restore() {
    run_node_restore_test(
        "rename table %s",
        &[case("t1 to t2", "`t1` TO `t2`")],
        rename_pair_fragment,
    );
}

/// `pkg/parser/ast/ddl_test.go::TestDDLReferenceDefRestore`.
#[test]
fn ddl_reference_def_restore() {
    run_node_restore_test(
        "CREATE TABLE child (id INT, parent_id INT, INDEX par_ind (parent_id), FOREIGN KEY (parent_id) %s)",
        &[
            case(
                "REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT",
                "REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT",
            ),
            case(
                "REFERENCES parent(id) ON DELETE CASCADE",
                "REFERENCES `parent`(`id`) ON DELETE CASCADE",
            ),
            case(
                "REFERENCES parent(id,hello) ON DELETE CASCADE",
                "REFERENCES `parent`(`id`, `hello`) ON DELETE CASCADE",
            ),
            case(
                "REFERENCES parent(id,hello(12)) ON DELETE CASCADE",
                "REFERENCES `parent`(`id`, `hello`(12)) ON DELETE CASCADE",
            ),
            case(
                "REFERENCES parent(id(8),hello(12)) ON DELETE CASCADE",
                "REFERENCES `parent`(`id`(8), `hello`(12)) ON DELETE CASCADE",
            ),
            case("REFERENCES parent(id)", "REFERENCES `parent`(`id`)"),
            case("REFERENCES parent((id+1))", "REFERENCES `parent`((`id`+1))"),
        ],
        |stmt, _| foreign_key_reference(stmt).restore(),
    );
}

/// `pkg/parser/ast/ddl_test.go::TestDDLConstraintRestore`.
#[test]
fn ddl_constraint_restore() {
    let cases = [
        case("INDEX par_ind (parent_id)", "INDEX `par_ind`(`parent_id`)"),
        case(
            "INDEX par_ind (parent_id(6))",
            "INDEX `par_ind`(`parent_id`(6))",
        ),
        case(
            "INDEX expr_ind ((id + parent_id))",
            "INDEX `expr_ind`((`id`+`parent_id`))",
        ),
        case(
            "INDEX expr_ind ((lower(id)))",
            "INDEX `expr_ind`((LOWER(`id`)))",
        ),
        case("key par_ind (parent_id)", "INDEX `par_ind`(`parent_id`)"),
        case(
            "key expr_ind ((lower(id)))",
            "INDEX `expr_ind`((LOWER(`id`)))",
        ),
        case("unique par_ind (parent_id)", "UNIQUE `par_ind`(`parent_id`)"),
        case(
            "unique key par_ind (parent_id)",
            "UNIQUE `par_ind`(`parent_id`)",
        ),
        case(
            "unique index par_ind (parent_id)",
            "UNIQUE `par_ind`(`parent_id`)",
        ),
        case(
            "unique expr_ind ((id + parent_id))",
            "UNIQUE `expr_ind`((`id`+`parent_id`))",
        ),
        case(
            "unique expr_ind ((lower(id)))",
            "UNIQUE `expr_ind`((LOWER(`id`)))",
        ),
        case(
            "unique key expr_ind ((id + parent_id))",
            "UNIQUE `expr_ind`((`id`+`parent_id`))",
        ),
        case(
            "unique key expr_ind ((lower(id)))",
            "UNIQUE `expr_ind`((LOWER(`id`)))",
        ),
        case(
            "unique index expr_ind ((id + parent_id))",
            "UNIQUE `expr_ind`((`id`+`parent_id`))",
        ),
        case(
            "unique index expr_ind ((lower(id)))",
            "UNIQUE `expr_ind`((LOWER(`id`)))",
        ),
        case(
            "fulltext key full_id (parent_id)",
            "FULLTEXT `full_id`(`parent_id`)",
        ),
        case(
            "fulltext INDEX full_id (parent_id)",
            "FULLTEXT `full_id`(`parent_id`)",
        ),
        case(
            "fulltext INDEX full_id ((parent_id+1))",
            "FULLTEXT `full_id`((`parent_id`+1))",
        ),
        case("PRIMARY KEY (id)", "PRIMARY KEY(`id`)"),
        case(
            "PRIMARY KEY (id) key_block_size = 32 using hash comment 'hello'",
            "PRIMARY KEY(`id`) KEY_BLOCK_SIZE=32 USING HASH COMMENT 'hello'",
        ),
        case("PRIMARY KEY ((id+1))", "PRIMARY KEY((`id`+1))"),
        case(
            "CONSTRAINT FOREIGN KEY (parent_id(2),hello(4)) REFERENCES parent(id) ON DELETE CASCADE",
            "CONSTRAINT FOREIGN KEY (`parent_id`(2), `hello`(4)) REFERENCES `parent`(`id`) ON DELETE CASCADE",
        ),
        case(
            "CONSTRAINT FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT",
            "CONSTRAINT FOREIGN KEY (`parent_id`) REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT",
        ),
        case(
            "CONSTRAINT FOREIGN KEY (parent_id(2),hello(4)) REFERENCES parent((id+1)) ON DELETE CASCADE",
            "CONSTRAINT FOREIGN KEY (`parent_id`(2), `hello`(4)) REFERENCES `parent`((`id`+1)) ON DELETE CASCADE",
        ),
        case(
            "CONSTRAINT FOREIGN KEY (parent_id) REFERENCES parent((id+1)) ON DELETE CASCADE ON UPDATE RESTRICT",
            "CONSTRAINT FOREIGN KEY (`parent_id`) REFERENCES `parent`((`id`+1)) ON DELETE CASCADE ON UPDATE RESTRICT",
        ),
        case(
            "CONSTRAINT fk_123 FOREIGN KEY (parent_id(2),hello(4)) REFERENCES parent(id) ON DELETE CASCADE",
            "CONSTRAINT `fk_123` FOREIGN KEY (`parent_id`(2), `hello`(4)) REFERENCES `parent`(`id`) ON DELETE CASCADE",
        ),
        case(
            "CONSTRAINT fk_123 FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT",
            "CONSTRAINT `fk_123` FOREIGN KEY (`parent_id`) REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT",
        ),
        case(
            "CONSTRAINT fk_123 FOREIGN KEY ((parent_id+1),hello(4)) REFERENCES parent(id) ON DELETE CASCADE",
            "CONSTRAINT `fk_123` FOREIGN KEY ((`parent_id`+1), `hello`(4)) REFERENCES `parent`(`id`) ON DELETE CASCADE",
        ),
        case(
            "CONSTRAINT fk_123 FOREIGN KEY ((parent_id+1)) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT",
            "CONSTRAINT `fk_123` FOREIGN KEY ((`parent_id`+1)) REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT",
        ),
        case(
            "FOREIGN KEY (parent_id(2),hello(4)) REFERENCES parent(id) ON DELETE CASCADE",
            "CONSTRAINT FOREIGN KEY (`parent_id`(2), `hello`(4)) REFERENCES `parent`(`id`) ON DELETE CASCADE",
        ),
        case(
            "FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT",
            "CONSTRAINT FOREIGN KEY (`parent_id`) REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT",
        ),
        case(
            "FOREIGN KEY ((parent_id+1),hello(4)) REFERENCES parent(id) ON DELETE CASCADE",
            "CONSTRAINT FOREIGN KEY ((`parent_id`+1), `hello`(4)) REFERENCES `parent`(`id`) ON DELETE CASCADE",
        ),
        case(
            "FOREIGN KEY ((parent_id+1)) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT",
            "CONSTRAINT FOREIGN KEY ((`parent_id`+1)) REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT",
        ),
    ];
    run_node_restore_test(
        "CREATE TABLE child (id INT, parent_id INT, %s)",
        &cases,
        |stmt, _| first_table_constraint(stmt).restore(),
    );

    let special_cases = [
        case(
            "PRIMARY KEY (id) CLUSTERED",
            "PRIMARY KEY(`id`) /*T![clustered_index] CLUSTERED */",
        ),
        case(
            "primary key (id) NONCLUSTERED",
            "PRIMARY KEY(`id`) /*T![clustered_index] NONCLUSTERED */",
        ),
        case(
            "PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */",
            "PRIMARY KEY(`id`) /*T![clustered_index] CLUSTERED */",
        ),
        case(
            "primary key (id) /*T![clustered_index] NONCLUSTERED */",
            "PRIMARY KEY(`id`) /*T![clustered_index] NONCLUSTERED */",
        ),
    ];
    run_node_restore_test_with_flags(
        "CREATE TABLE child (id INT, parent_id INT, %s)",
        &special_cases,
        SPECIAL_COMMENT,
        |stmt, context| first_table_constraint(stmt).restore_with_context(context),
    );
}

fn first_table_constraint(stmt: &Stmt) -> &tidb_ast::TableConstraint {
    match expect_ddl(stmt) {
        DdlStmt::CreateTable(create) => &create.table_constraints[0],
        other => panic!("expected CREATE TABLE, got {other:?}"),
    }
}

fn first_column_first_option(stmt: &Stmt) -> &tidb_ast::ColumnOption {
    match expect_ddl(stmt) {
        DdlStmt::CreateTable(create) => &create.columns[0].options[0],
        other => panic!("expected CREATE TABLE, got {other:?}"),
    }
}

/// `pkg/parser/ast/ddl_test.go::TestDDLColumnOptionRestore`.
#[test]
fn ddl_column_option_restore() {
    run_node_restore_test(
        "CREATE TABLE child (id INT %s)",
        &[
            case("primary key", "PRIMARY KEY"),
            case("not null", "NOT NULL"),
            case("null", "NULL"),
            case("auto_increment", "AUTO_INCREMENT"),
            case("DEFAULT 10", "DEFAULT 10"),
            case("DEFAULT '10'", "DEFAULT _UTF8MB4'10'"),
            case("DEFAULT 'hello'", "DEFAULT _UTF8MB4'hello'"),
            case("DEFAULT 1.1", "DEFAULT 1.1"),
            case("DEFAULT NULL", "DEFAULT NULL"),
            case("DEFAULT ''", "DEFAULT _UTF8MB4''"),
            case("DEFAULT TRUE", "DEFAULT TRUE"),
            case("DEFAULT FALSE", "DEFAULT FALSE"),
            case("DEFAULT (colA)", "DEFAULT (`colA`)"),
            case("UNIQUE KEY", "UNIQUE KEY"),
            case(
                "on update CURRENT_TIMESTAMP",
                "ON UPDATE CURRENT_TIMESTAMP()",
            ),
            case("comment 'hello'", "COMMENT 'hello'"),
            case(
                "generated always as(id + 1)",
                "GENERATED ALWAYS AS(`id`+1) VIRTUAL",
            ),
            case(
                "generated always as(id + 1) virtual",
                "GENERATED ALWAYS AS(`id`+1) VIRTUAL",
            ),
            case(
                "generated always as(id + 1) stored",
                "GENERATED ALWAYS AS(`id`+1) STORED",
            ),
            case("REFERENCES parent(id)", "REFERENCES `parent`(`id`)"),
            case("COLLATE utf8_bin", "COLLATE utf8_bin"),
            case("STORAGE DEFAULT", "STORAGE DEFAULT"),
            case("STORAGE DISK", "STORAGE DISK"),
            case("STORAGE MEMORY", "STORAGE MEMORY"),
            case("AUTO_RANDOM (3)", "AUTO_RANDOM(3)"),
            case("AUTO_RANDOM", "AUTO_RANDOM"),
        ],
        |stmt, context| first_column_first_option(stmt).restore_with_context(context),
    );
}

/// `pkg/parser/ast/ddl_test.go::TestGeneratedRestore` — generated-column
/// expressions strip schema/table qualifiers under the combined flags.
#[test]
fn generated_restore() {
    run_node_restore_test_with_flags_stmt_change(
        "CREATE TABLE child (id INT %s)",
        &[
            case(
                "generated always as(id + 1)",
                "GENERATED ALWAYS AS(`id`+1) VIRTUAL",
            ),
            case(
                "generated always as(id + 1) virtual",
                "GENERATED ALWAYS AS(`id`+1) VIRTUAL",
            ),
            case(
                "generated always as(id + 1) stored",
                "GENERATED ALWAYS AS(`id`+1) STORED",
            ),
            case(
                "generated always as(lower(id)) stored",
                "GENERATED ALWAYS AS(LOWER(`id`)) STORED",
            ),
            // Go's own oracle rewrites `child.id` to bare `id` here because
            // the schema/table components are dropped while restoring.
            case(
                "generated always as(lower(child.id)) stored",
                "GENERATED ALWAYS AS(LOWER(`id`)) STORED",
            ),
        ],
        RestoreFlags::DEFAULT
            | RestoreFlags::WITHOUT_SCHEMA_NAME
            | RestoreFlags::WITHOUT_TABLE_NAME,
        |stmt, context| first_column_first_option(stmt).restore_with_context(context),
    );
}

/// `pkg/parser/ast/ddl_test.go::TestDDLColumnDefRestore`.
#[test]
fn ddl_column_def_restore() {
    let cases: &[(&str, &str)] = &[
        ("id json", "`id` JSON"),
        ("id time(5)", "`id` TIME(5)"),
        ("id int(5) unsigned", "`id` INT(5) UNSIGNED"),
        (
            "id int(5) UNSIGNED ZEROFILL",
            "`id` INT(5) UNSIGNED ZEROFILL",
        ),
        ("id float(12,3)", "`id` FLOAT(12,3)"),
        ("id float", "`id` FLOAT"),
        ("id double(22,3)", "`id` DOUBLE(22,3)"),
        ("id double", "`id` DOUBLE"),
        ("id tinyint(4)", "`id` TINYINT(4)"),
        ("id smallint(6)", "`id` SMALLINT(6)"),
        ("id mediumint(9)", "`id` MEDIUMINT(9)"),
        ("id integer(11)", "`id` INT(11)"),
        ("id bigint(20)", "`id` BIGINT(20)"),
        ("id DATE", "`id` DATE"),
        ("id DATETIME", "`id` DATETIME"),
        ("id DECIMAL(4,2)", "`id` DECIMAL(4,2)"),
        ("id char(1)", "`id` CHAR(1)"),
        ("id varchar(10) BINARY", "`id` VARCHAR(10) BINARY"),
        ("id binary(1)", "`id` BINARY(1)"),
        ("id timestamp(2)", "`id` TIMESTAMP(2)"),
        ("id timestamp", "`id` TIMESTAMP"),
        ("id datetime(2)", "`id` DATETIME(2)"),
        ("id date", "`id` DATE"),
        ("id year", "`id` YEAR"),
        ("id INT", "`id` INT"),
        ("id INT NULL", "`id` INT NULL"),
        ("id enum('a','b')", "`id` ENUM('a','b')"),
        ("id enum('''a''','''b''')", "`id` ENUM('''a''','''b''')"),
        (
            "id enum('a\\nb','a\\tb','a\\rb')",
            "`id` ENUM('a\nb','a\tb','a\rb')",
        ),
        ("id enum('a','b') binary", "`id` ENUM('a','b') BINARY"),
        ("id enum(0x61, 0b01100010)", "`id` ENUM('a','b')"),
        ("id set('a','b')", "`id` SET('a','b')"),
        ("id set('''a''','''b''')", "`id` SET('''a''','''b''')"),
        (
            "id set('a\\nb','a''\t\\r\\nb','a\\rb')",
            "`id` SET('a\nb','a''\t\r\nb','a\rb')",
        ),
        (
            r#"id set("a'\nb","a'b\tc")"#,
            "`id` SET('a''\nb','a''b\tc')",
        ),
        ("id set('a','b') binary", "`id` SET('a','b') BINARY"),
        ("id set(0x61, 0b01100010)", "`id` SET('a','b')"),
        (
            "id TEXT CHARACTER SET UTF8 COLLATE UTF8_UNICODE_CI",
            "`id` TEXT CHARACTER SET UTF8 COLLATE utf8_unicode_ci",
        ),
        ("id text character set UTF8", "`id` TEXT CHARACTER SET UTF8"),
        ("id text charset UTF8", "`id` TEXT CHARACTER SET UTF8"),
        (
            "id varchar(50) collate UTF8MB4_CZECH_CI",
            "`id` VARCHAR(50) COLLATE utf8mb4_czech_ci",
        ),
        (
            "id varchar(50) collate utf8_bin",
            "`id` VARCHAR(50) COLLATE utf8_bin",
        ),
        (
            "c1 char(10) character set LATIN1 collate latin1_german1_ci",
            "`c1` CHAR(10) CHARACTER SET LATIN1 COLLATE latin1_german1_ci",
        ),
        ("id int(11) PRIMARY KEY", "`id` INT(11) PRIMARY KEY"),
        ("id int(11) NOT NULL", "`id` INT(11) NOT NULL"),
        ("id INT(11) NULL", "`id` INT(11) NULL"),
        ("id INT(11) auto_increment", "`id` INT(11) AUTO_INCREMENT"),
        ("id INT(11) DEFAULT 10", "`id` INT(11) DEFAULT 10"),
        (
            "id INT(11) DEFAULT '10'",
            "`id` INT(11) DEFAULT _UTF8MB4'10'",
        ),
        ("id INT(11) DEFAULT 1.1", "`id` INT(11) DEFAULT 1.1"),
        ("id INT(11) UNIQUE KEY", "`id` INT(11) UNIQUE KEY"),
        (
            "id INT(11) COLLATE ascii_bin",
            "`id` INT(11) COLLATE ascii_bin",
        ),
        (
            "id INT(11) on update CURRENT_TIMESTAMP",
            "`id` INT(11) ON UPDATE CURRENT_TIMESTAMP()",
        ),
        ("id INT(11) comment 'hello'", "`id` INT(11) COMMENT 'hello'"),
        (
            "id INT(11) generated always as(id + 1)",
            "`id` INT(11) GENERATED ALWAYS AS(`id`+1) VIRTUAL",
        ),
        (
            "id INT(11) REFERENCES parent(id)",
            "`id` INT(11) REFERENCES `parent`(`id`)",
        ),
        ("id bit", "`id` BIT(1)"),
        ("id bit(1)", "`id` BIT(1)"),
        ("id bit(64)", "`id` BIT(64)"),
        ("id tinyint", "`id` TINYINT"),
        ("id tinyint(255)", "`id` TINYINT(255)"),
        ("id bool", "`id` TINYINT(1)"),
        ("id boolean", "`id` TINYINT(1)"),
        ("id smallint", "`id` SMALLINT"),
        ("id smallint(255)", "`id` SMALLINT(255)"),
        ("id mediumint", "`id` MEDIUMINT"),
        ("id mediumint(255)", "`id` MEDIUMINT(255)"),
        ("id int", "`id` INT"),
        ("id int(255)", "`id` INT(255)"),
        ("id integer", "`id` INT"),
        ("id integer(255)", "`id` INT(255)"),
        ("id bigint", "`id` BIGINT"),
        ("id bigint(255)", "`id` BIGINT(255)"),
        ("id decimal", "`id` DECIMAL"),
        ("id decimal(10)", "`id` DECIMAL(10)"),
        ("id decimal(10,0)", "`id` DECIMAL(10,0)"),
        ("id decimal(65)", "`id` DECIMAL(65)"),
        ("id decimal(65,30)", "`id` DECIMAL(65,30)"),
        ("id dec(10,0)", "`id` DECIMAL(10,0)"),
        ("id numeric(10,0)", "`id` DECIMAL(10,0)"),
        ("id float(0)", "`id` FLOAT"),
        ("id float(24)", "`id` FLOAT"),
        ("id float(25)", "`id` DOUBLE"),
        ("id float(53)", "`id` DOUBLE"),
        ("id float(7,0)", "`id` FLOAT(7,0)"),
        ("id float(25,0)", "`id` FLOAT(25,0)"),
        ("id double(15,0)", "`id` DOUBLE(15,0)"),
        ("id double precision(15,0)", "`id` DOUBLE(15,0)"),
        ("id real(15,0)", "`id` DOUBLE(15,0)"),
        ("id year(4)", "`id` YEAR(4)"),
        ("id time", "`id` TIME"),
        ("id char", "`id` CHAR"),
        ("id char(0)", "`id` CHAR(0)"),
        ("id char(255)", "`id` CHAR(255)"),
        ("id national char(0)", "`id` CHAR(0)"),
        ("id binary", "`id` BINARY"),
        ("id varbinary(0)", "`id` VARBINARY(0)"),
        ("id varbinary(65535)", "`id` VARBINARY(65535)"),
        ("id tinyblob", "`id` TINYBLOB"),
        ("id tinytext", "`id` TINYTEXT"),
        ("id blob", "`id` BLOB"),
        ("id blob(0)", "`id` BLOB(0)"),
        ("id blob(65535)", "`id` BLOB(65535)"),
        ("id text(0)", "`id` TEXT(0)"),
        ("id text(65535)", "`id` TEXT(65535)"),
        ("id mediumblob", "`id` MEDIUMBLOB"),
        ("id mediumtext", "`id` MEDIUMTEXT"),
        ("id longblob", "`id` LONGBLOB"),
        ("id longtext", "`id` LONGTEXT"),
        ("id json", "`id` JSON"),
    ];
    let node_cases: Vec<NodeRestoreCase> = cases
        .iter()
        .map(|(source, expect)| case(source, expect))
        .collect();
    run_node_restore_test(
        "CREATE TABLE t (%s)",
        &node_cases,
        |stmt, _context| match expect_ddl(stmt) {
            DdlStmt::CreateTable(create) => create.columns[0].restore(),
            other => panic!("expected CREATE TABLE, got {other:?}"),
        },
    );
}

/// `pkg/parser/ast/ddl_test.go::TestDDLTruncateTableStmtRestore`.
#[test]
fn ddl_truncate_table_stmt_restore() {
    run_node_restore_test(
        "%s",
        &[
            case("truncate t1", "TRUNCATE TABLE `t1`"),
            case("truncate table t1", "TRUNCATE TABLE `t1`"),
            case("truncate a.t1", "TRUNCATE TABLE `a`.`t1`"),
        ],
        whole_statement,
    );
}

/// `pkg/parser/ast/ddl_test.go::TestDDLDropTableStmtRestore`.
#[test]
fn ddl_drop_table_stmt_restore() {
    run_node_restore_test(
        "%s",
        &[
            case("drop table t1", "DROP TABLE `t1`"),
            case("drop table if exists t1", "DROP TABLE IF EXISTS `t1`"),
            case("drop temporary table t1", "DROP TEMPORARY TABLE `t1`"),
            case(
                "drop temporary table if exists t1",
                "DROP TEMPORARY TABLE IF EXISTS `t1`",
            ),
            case(
                "DROP /*!40005 TEMPORARY */ TABLE IF EXISTS `test`",
                "DROP TEMPORARY TABLE IF EXISTS `test`",
            ),
        ],
        whole_statement,
    );
}

/// `pkg/parser/ast/ddl_test.go::TestColumnPositionRestore`.
#[test]
fn column_position_restore() {
    run_node_restore_test(
        "alter table t add column a varchar(255) %s",
        &[
            case("", ""),
            case("first", "FIRST"),
            case("after b", "AFTER `b`"),
        ],
        |stmt, _| match expect_ddl(stmt) {
            DdlStmt::AlterTable(alter) => match &alter.actions[0] {
                tidb_ast::AlterTableAction::AddColumn { position, .. } => position.restore(),
                other => panic!("expected ADD COLUMN action, got {other:?}"),
            },
            other => panic!("expected ALTER TABLE, got {other:?}"),
        },
    );
}

/// `pkg/parser/ast/ddl_test.go::TestAlterTableSpecRestore`.
#[test]
fn alter_table_spec_restore() {
    let cases = [
        case("ENGINE innodb", "ENGINE = innodb"),
        case("ENGINE = innodb", "ENGINE = innodb"),
        case("ENGINE = 'innodb'", "ENGINE = innodb"),
        case("ENGINE tokudb", "ENGINE = tokudb"),
        case("ENGINE = tokudb", "ENGINE = tokudb"),
        case("ENGINE = 'tokudb'", "ENGINE = tokudb"),
        case("DEFAULT CHARACTER SET utf8", "DEFAULT CHARACTER SET = UTF8"),
        case(
            "DEFAULT CHARACTER SET = utf8",
            "DEFAULT CHARACTER SET = UTF8",
        ),
        case("DEFAULT CHARSET utf8", "DEFAULT CHARACTER SET = UTF8"),
        case("DEFAULT CHARSET = utf8", "DEFAULT CHARACTER SET = UTF8"),
        case("DEFAULT COLLATE utf8_bin", "DEFAULT COLLATE = UTF8_BIN"),
        case("DEFAULT COLLATE = utf8_bin", "DEFAULT COLLATE = UTF8_BIN"),
        case("AUTO_INCREMENT 3", "AUTO_INCREMENT = 3"),
        case("AUTO_INCREMENT = 6", "AUTO_INCREMENT = 6"),
        case("COMMENT ''", "COMMENT = ''"),
        case("COMMENT 'system role'", "COMMENT = 'system role'"),
        case("COMMENT = 'system role'", "COMMENT = 'system role'"),
        case("AVG_ROW_LENGTH 12", "AVG_ROW_LENGTH = 12"),
        case("AVG_ROW_LENGTH = 6", "AVG_ROW_LENGTH = 6"),
        case("connection 'abc'", "CONNECTION = 'abc'"),
        case("CONNECTION = 'abc'", "CONNECTION = 'abc'"),
        case("checksum 1", "CHECKSUM = 1"),
        case("checksum = 0", "CHECKSUM = 0"),
        case("PASSWORD '123456'", "PASSWORD = '123456'"),
        case("PASSWORD = ''", "PASSWORD = ''"),
        case("compression 'NONE'", "COMPRESSION = 'NONE'"),
        case("compression = 'lz4'", "COMPRESSION = 'lz4'"),
        case("key_block_size 1024", "KEY_BLOCK_SIZE = 1024"),
        case("KEY_BLOCK_SIZE = 1024", "KEY_BLOCK_SIZE = 1024"),
        case("max_rows 1000", "MAX_ROWS = 1000"),
        case("max_rows = 1000", "MAX_ROWS = 1000"),
        case("min_rows 1000", "MIN_ROWS = 1000"),
        case("MIN_ROWS = 1000", "MIN_ROWS = 1000"),
        case("DELAY_KEY_WRITE 1", "DELAY_KEY_WRITE = 1"),
        case("DELAY_KEY_WRITE = 1000", "DELAY_KEY_WRITE = 1000"),
        case("ROW_FORMAT default", "ROW_FORMAT = DEFAULT"),
        case("ROW_FORMAT = default", "ROW_FORMAT = DEFAULT"),
        case("ROW_FORMAT = fixed", "ROW_FORMAT = FIXED"),
        case("ROW_FORMAT = compressed", "ROW_FORMAT = COMPRESSED"),
        case("ROW_FORMAT = compact", "ROW_FORMAT = COMPACT"),
        case("ROW_FORMAT = redundant", "ROW_FORMAT = REDUNDANT"),
        case("ROW_FORMAT = dynamic", "ROW_FORMAT = DYNAMIC"),
        case("ROW_FORMAT tokudb_default", "ROW_FORMAT = TOKUDB_DEFAULT"),
        case("ROW_FORMAT = tokudb_default", "ROW_FORMAT = TOKUDB_DEFAULT"),
        case("ROW_FORMAT = tokudb_fast", "ROW_FORMAT = TOKUDB_FAST"),
        case("ROW_FORMAT = tokudb_small", "ROW_FORMAT = TOKUDB_SMALL"),
        case("ROW_FORMAT = tokudb_zlib", "ROW_FORMAT = TOKUDB_ZLIB"),
        case("ROW_FORMAT = tokudb_zstd", "ROW_FORMAT = TOKUDB_ZSTD"),
        case("ROW_FORMAT = tokudb_quicklz", "ROW_FORMAT = TOKUDB_QUICKLZ"),
        case("ROW_FORMAT = tokudb_lzma", "ROW_FORMAT = TOKUDB_LZMA"),
        case("ROW_FORMAT = tokudb_snappy", "ROW_FORMAT = TOKUDB_SNAPPY"),
        case(
            "ROW_FORMAT = tokudb_uncompressed",
            "ROW_FORMAT = TOKUDB_UNCOMPRESSED",
        ),
        case("shard_row_id_bits 1", "SHARD_ROW_ID_BITS = 1"),
        case("shard_row_id_bits = 1", "SHARD_ROW_ID_BITS = 1"),
        case(
            "CONVERT TO CHARACTER SET utf8",
            "CONVERT TO CHARACTER SET UTF8",
        ),
        case("CONVERT TO CHARSET utf8", "CONVERT TO CHARACTER SET UTF8"),
        case(
            "CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin",
            "CONVERT TO CHARACTER SET UTF8 COLLATE UTF8_BIN",
        ),
        case(
            "CONVERT TO CHARSET utf8 COLLATE utf8_bin",
            "CONVERT TO CHARACTER SET UTF8 COLLATE UTF8_BIN",
        ),
        case(
            "ADD COLUMN (a SMALLINT UNSIGNED)",
            "ADD COLUMN (`a` SMALLINT UNSIGNED)",
        ),
        case(
            "ADD COLUMN (a SMALLINT UNSIGNED, b varchar(255))",
            "ADD COLUMN (`a` SMALLINT UNSIGNED, `b` VARCHAR(255))",
        ),
        case(
            "ADD COLUMN a SMALLINT UNSIGNED",
            "ADD COLUMN `a` SMALLINT UNSIGNED",
        ),
        case(
            "ADD COLUMN a SMALLINT UNSIGNED FIRST",
            "ADD COLUMN `a` SMALLINT UNSIGNED FIRST",
        ),
        case(
            "ADD COLUMN a SMALLINT UNSIGNED AFTER b",
            "ADD COLUMN `a` SMALLINT UNSIGNED AFTER `b`",
        ),
        case(
            "ADD COLUMN name mediumtext CHARACTER SET UTF8MB4 COLLATE utf8mb4_unicode_ci NOT NULL",
            "ADD COLUMN `name` MEDIUMTEXT CHARACTER SET UTF8MB4 COLLATE utf8mb4_unicode_ci NOT NULL",
        ),
        case(
            "ADD CONSTRAINT INDEX par_ind (parent_id)",
            "ADD INDEX `par_ind`(`parent_id`)",
        ),
        case(
            "ADD CONSTRAINT INDEX par_ind (parent_id(6))",
            "ADD INDEX `par_ind`(`parent_id`(6))",
        ),
        case(
            "ADD CONSTRAINT key par_ind (parent_id)",
            "ADD INDEX `par_ind`(`parent_id`)",
        ),
        case(
            "ADD CONSTRAINT unique par_ind (parent_id)",
            "ADD UNIQUE `par_ind`(`parent_id`)",
        ),
        case(
            "ADD CONSTRAINT unique key par_ind (parent_id)",
            "ADD UNIQUE `par_ind`(`parent_id`)",
        ),
        case(
            "ADD CONSTRAINT unique index par_ind (parent_id)",
            "ADD UNIQUE `par_ind`(`parent_id`)",
        ),
        case(
            "ADD CONSTRAINT fulltext key full_id (parent_id)",
            "ADD FULLTEXT `full_id`(`parent_id`)",
        ),
        case(
            "ADD CONSTRAINT fulltext INDEX full_id (parent_id)",
            "ADD FULLTEXT `full_id`(`parent_id`)",
        ),
        case(
            "ADD CONSTRAINT PRIMARY KEY (id)",
            "ADD PRIMARY KEY(`id`)",
        ),
        case(
            "ADD CONSTRAINT PRIMARY KEY (id) key_block_size = 32 using hash comment 'hello'",
            "ADD PRIMARY KEY(`id`) KEY_BLOCK_SIZE=32 USING HASH COMMENT 'hello'",
        ),
        case(
            "ADD CONSTRAINT FOREIGN KEY (parent_id(2),hello(4)) REFERENCES parent(id) ON DELETE CASCADE",
            "ADD CONSTRAINT FOREIGN KEY (`parent_id`(2), `hello`(4)) REFERENCES `parent`(`id`) ON DELETE CASCADE",
        ),
        case(
            "ADD CONSTRAINT FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT",
            "ADD CONSTRAINT FOREIGN KEY (`parent_id`) REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT",
        ),
        case(
            "ADD CONSTRAINT fk_123 FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT",
            "ADD CONSTRAINT `fk_123` FOREIGN KEY (`parent_id`) REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT",
        ),
        case("DROP COLUMN a", "DROP COLUMN `a`"),
        case("DROP COLUMN a RESTRICT", "DROP COLUMN `a`"),
        case("DROP COLUMN a CASCADE", "DROP COLUMN `a`"),
        case("DROP PRIMARY KEY", "DROP PRIMARY KEY"),
        case("drop index a", "DROP INDEX `a`"),
        case("drop key a", "DROP INDEX `a`"),
        case("drop FOREIGN key a", "DROP FOREIGN KEY `a`"),
        case(
            "MODIFY column a varchar(255)",
            "MODIFY COLUMN `a` VARCHAR(255)",
        ),
        case(
            "modify COLUMN a varchar(255) FIRST",
            "MODIFY COLUMN `a` VARCHAR(255) FIRST",
        ),
        case(
            "modify COLUMN a varchar(255) AFTER b",
            "MODIFY COLUMN `a` VARCHAR(255) AFTER `b`",
        ),
        case(
            "change column a b VARCHAR(255)",
            "CHANGE COLUMN `a` `b` VARCHAR(255)",
        ),
        case(
            "change COLUMN a b varchar(255) CHARACTER SET UTF8 BINARY",
            "CHANGE COLUMN `a` `b` VARCHAR(255) BINARY CHARACTER SET UTF8",
        ),
        case(
            "CHANGE column a b varchar(255) FIRST",
            "CHANGE COLUMN `a` `b` VARCHAR(255) FIRST",
        ),
        case(
            "change COLUMN a b varchar(255) AFTER c",
            "CHANGE COLUMN `a` `b` VARCHAR(255) AFTER `c`",
        ),
        case("RENAME db1.t1", "RENAME AS `db1`.`t1`"),
        case("RENAME to db1.t1", "RENAME AS `db1`.`t1`"),
        case("RENAME as t1", "RENAME AS `t1`"),
        case("ALTER a SET DEFAULT 1", "ALTER COLUMN `a` SET DEFAULT 1"),
        case("ALTER a DROP DEFAULT", "ALTER COLUMN `a` DROP DEFAULT"),
        case("ALTER COLUMN a SET DEFAULT 1", "ALTER COLUMN `a` SET DEFAULT 1"),
        case("ALTER COLUMN a DROP DEFAULT", "ALTER COLUMN `a` DROP DEFAULT"),
        case("LOCK=NONE", "LOCK = NONE"),
        case("LOCK=DEFAULT", "LOCK = DEFAULT"),
        case("LOCK=SHARED", "LOCK = SHARED"),
        case("LOCK=EXCLUSIVE", "LOCK = EXCLUSIVE"),
        case("RENAME KEY a TO b", "RENAME INDEX `a` TO `b`"),
        case("RENAME INDEX a TO b", "RENAME INDEX `a` TO `b`"),
        case("ADD PARTITION", "ADD PARTITION"),
        case(
            "ADD PARTITION ( PARTITION P1 VALUES LESS THAN (2010))",
            "ADD PARTITION (PARTITION `P1` VALUES LESS THAN (2010))",
        ),
        case(
            "ADD PARTITION ( PARTITION P2 VALUES LESS THAN MAXVALUE)",
            "ADD PARTITION (PARTITION `P2` VALUES LESS THAN (MAXVALUE))",
        ),
        case(
            "ADD PARTITION (\nPARTITION P1 VALUES LESS THAN (2010),\nPARTITION P2 VALUES LESS THAN (2015),\nPARTITION P3 VALUES LESS THAN MAXVALUE)",
            "ADD PARTITION (PARTITION `P1` VALUES LESS THAN (2010), PARTITION `P2` VALUES LESS THAN (2015), PARTITION `P3` VALUES LESS THAN (MAXVALUE))",
        ),
        case(
            "ADD PARTITION (PARTITION `p5` VALUES LESS THAN (2010) COMMENT 'AP_START \\' AP_END')",
            "ADD PARTITION (PARTITION `p5` VALUES LESS THAN (2010) COMMENT = 'AP_START '' AP_END')",
        ),
        case(
            "ADD PARTITION (PARTITION `p5` VALUES LESS THAN (2010) COMMENT = 'xxx')",
            "ADD PARTITION (PARTITION `p5` VALUES LESS THAN (2010) COMMENT = 'xxx')",
        ),
        case("coalesce partition 3", "COALESCE PARTITION 3"),
        case("drop partition p1", "DROP PARTITION `p1`"),
        case("TRUNCATE PARTITION p0", "TRUNCATE PARTITION `p0`"),
        case(
            "add stats_extended s1 cardinality(a,b)",
            "ADD STATS_EXTENDED `s1` CARDINALITY(`a`, `b`)",
        ),
        case(
            "add stats_extended if not exists s1 cardinality(a,b)",
            "ADD STATS_EXTENDED IF NOT EXISTS `s1` CARDINALITY(`a`, `b`)",
        ),
        case(
            "add stats_extended s1 correlation(a,b)",
            "ADD STATS_EXTENDED `s1` CORRELATION(`a`, `b`)",
        ),
        case(
            "add stats_extended if not exists s1 correlation(a,b)",
            "ADD STATS_EXTENDED IF NOT EXISTS `s1` CORRELATION(`a`, `b`)",
        ),
        case(
            "add stats_extended s1 dependency(a,b)",
            "ADD STATS_EXTENDED `s1` DEPENDENCY(`a`, `b`)",
        ),
        case(
            "add stats_extended if not exists s1 dependency(a,b)",
            "ADD STATS_EXTENDED IF NOT EXISTS `s1` DEPENDENCY(`a`, `b`)",
        ),
        case("drop stats_extended s1", "DROP STATS_EXTENDED `s1`"),
        case(
            "drop stats_extended if exists s1",
            "DROP STATS_EXTENDED IF EXISTS `s1`",
        ),
        case("placement policy p1", "PLACEMENT POLICY = `p1`"),
        case(
            "placement policy p1 comment='aaa'",
            "PLACEMENT POLICY = `p1` COMMENT = 'aaa'",
        ),
        case(
            "partition p0 placement policy p1",
            "PARTITION `p0` PLACEMENT POLICY = `p1`",
        ),
    ];
    run_node_restore_test("ALTER TABLE t %s", &cases, |stmt, _| {
        first_alter_action(stmt).restore()
    });
}

fn first_alter_action(stmt: &Stmt) -> &tidb_ast::AlterTableAction {
    match expect_ddl(stmt) {
        DdlStmt::AlterTable(alter) => &alter.actions[0],
        other => panic!("expected ALTER TABLE, got {other:?}"),
    }
}

/// `pkg/parser/ast/ddl_test.go::TestAlterTableWithSpecialCommentRestore`.
#[test]
fn alter_table_with_special_comment_restore() {
    run_node_restore_test_with_flags(
        "ALTER TABLE t %s",
        &[
            case(
                "placement policy p1",
                "/*T![placement] PLACEMENT POLICY = `p1` */",
            ),
            case(
                "placement policy p1 comment='aaa'",
                "/*T![placement] PLACEMENT POLICY = `p1` */ COMMENT = 'aaa'",
            ),
            case(
                "partition p0 placement policy p1",
                "/*T![placement] PARTITION `p0` PLACEMENT POLICY = `p1` */",
            ),
        ],
        SPECIAL_COMMENT,
        |stmt, context| first_alter_action(stmt).restore_with_context(context),
    );
}

/// `pkg/parser/ast/ddl_test.go::TestAlterTableOptionRestore`.
#[test]
fn alter_table_option_restore() {
    run_node_restore_test(
        "%s",
        &[
            case(
                "ALTER TABLE t ROW_FORMAT = COMPRESSED KEY_BLOCK_SIZE = 8",
                "ALTER TABLE `t` ROW_FORMAT = COMPRESSED KEY_BLOCK_SIZE = 8",
            ),
            case(
                "ALTER TABLE t ROW_FORMAT = COMPRESSED, KEY_BLOCK_SIZE = 8",
                "ALTER TABLE `t` ROW_FORMAT = COMPRESSED, KEY_BLOCK_SIZE = 8",
            ),
        ],
        whole_statement,
    );
}

/// `pkg/parser/ast/ddl_test.go::TestAdminRepairTableRestore`.
#[test]
fn admin_repair_table_restore() {
    run_node_restore_test(
        "%s",
        &[
            case(
                "ADMIN REPAIR TABLE t CREATE TABLE t (a int)",
                "ADMIN REPAIR TABLE `t` CREATE TABLE `t` (`a` INT)",
            ),
            case(
                "ADMIN REPAIR TABLE t CREATE TABLE t (a char(1), b int)",
                "ADMIN REPAIR TABLE `t` CREATE TABLE `t` (`a` CHAR(1),`b` INT)",
            ),
            case(
                "ADMIN REPAIR TABLE t CREATE TABLE t (a TINYINT UNSIGNED)",
                "ADMIN REPAIR TABLE `t` CREATE TABLE `t` (`a` TINYINT UNSIGNED)",
            ),
        ],
        whole_statement,
    );
}

/// `pkg/parser/ast/ddl_test.go::TestAdminOptimizeTableRestore`.
#[test]
fn admin_optimize_table_restore() {
    run_node_restore_test(
        "%s",
        &[
            case("OPTIMIZE TABLE t", "OPTIMIZE TABLE `t`"),
            case(
                "OPTIMIZE LOCAL TABLE t",
                "OPTIMIZE NO_WRITE_TO_BINLOG TABLE `t`",
            ),
            case(
                "OPTIMIZE NO_WRITE_TO_BINLOG TABLE t",
                "OPTIMIZE NO_WRITE_TO_BINLOG TABLE `t`",
            ),
            case("OPTIMIZE TABLE t1, t2", "OPTIMIZE TABLE `t1`, `t2`"),
            case("optimize table t1,t2", "OPTIMIZE TABLE `t1`, `t2`"),
            case("optimize tables t1, t2", "OPTIMIZE TABLE `t1`, `t2`"),
        ],
        whole_statement,
    );
}

/// `pkg/parser/ast/ddl_test.go::TestSequenceRestore`.
#[test]
fn sequence_restore() {
    run_node_restore_test(
        "%s",
        &[
            case("create sequence seq", "CREATE SEQUENCE `seq`"),
            case(
                "create sequence if not exists seq",
                "CREATE SEQUENCE IF NOT EXISTS `seq`",
            ),
            case(
                "create sequence if not exists seq increment 1",
                "CREATE SEQUENCE IF NOT EXISTS `seq` INCREMENT BY 1",
            ),
            case(
                "create sequence if not exists seq increment = 1",
                "CREATE SEQUENCE IF NOT EXISTS `seq` INCREMENT BY 1",
            ),
            case(
                "create sequence if not exists seq minvalue 1",
                "CREATE SEQUENCE IF NOT EXISTS `seq` MINVALUE 1",
            ),
            case(
                "create sequence if not exists seq minvalue = 1",
                "CREATE SEQUENCE IF NOT EXISTS `seq` MINVALUE 1",
            ),
            case(
                "create sequence if not exists seq nominvalue",
                "CREATE SEQUENCE IF NOT EXISTS `seq` NO MINVALUE",
            ),
            case(
                "create sequence if not exists seq no minvalue",
                "CREATE SEQUENCE IF NOT EXISTS `seq` NO MINVALUE",
            ),
            case(
                "create sequence if not exists seq maxvalue 1",
                "CREATE SEQUENCE IF NOT EXISTS `seq` MAXVALUE 1",
            ),
            case(
                "create sequence if not exists seq maxvalue = 1",
                "CREATE SEQUENCE IF NOT EXISTS `seq` MAXVALUE 1",
            ),
            case(
                "create sequence if not exists seq nomaxvalue",
                "CREATE SEQUENCE IF NOT EXISTS `seq` NO MAXVALUE",
            ),
            case(
                "create sequence if not exists seq no maxvalue",
                "CREATE SEQUENCE IF NOT EXISTS `seq` NO MAXVALUE",
            ),
            case(
                "create sequence if not exists seq start 1",
                "CREATE SEQUENCE IF NOT EXISTS `seq` START WITH 1",
            ),
            case(
                "create sequence if not exists seq start with 1",
                "CREATE SEQUENCE IF NOT EXISTS `seq` START WITH 1",
            ),
            case(
                "create sequence if not exists seq cache 1",
                "CREATE SEQUENCE IF NOT EXISTS `seq` CACHE 1",
            ),
            case(
                "create sequence if not exists seq nocache",
                "CREATE SEQUENCE IF NOT EXISTS `seq` NOCACHE",
            ),
            case(
                "create sequence if not exists seq no cache",
                "CREATE SEQUENCE IF NOT EXISTS `seq` NOCACHE",
            ),
            case(
                "create sequence if not exists seq cycle",
                "CREATE SEQUENCE IF NOT EXISTS `seq` CYCLE",
            ),
            case(
                "create sequence if not exists seq nocycle",
                "CREATE SEQUENCE IF NOT EXISTS `seq` NOCYCLE",
            ),
            case(
                "create sequence if not exists seq no cycle",
                "CREATE SEQUENCE IF NOT EXISTS `seq` NOCYCLE",
            ),
            case(
                "create sequence seq increment 1 minvalue 0 maxvalue 1000",
                "CREATE SEQUENCE `seq` INCREMENT BY 1 MINVALUE 0 MAXVALUE 1000",
            ),
            case(
                "create sequence seq minvalue 0 maxvalue 1000 increment 1",
                "CREATE SEQUENCE `seq` MINVALUE 0 MAXVALUE 1000 INCREMENT BY 1",
            ),
            case(
                "create sequence seq cache = 1 minvalue 0 maxvalue -1000",
                "CREATE SEQUENCE `seq` CACHE 1 MINVALUE 0 MAXVALUE -1000",
            ),
            case(
                "create sequence seq increment -1 minvalue 0 maxvalue -1000",
                "CREATE SEQUENCE `seq` INCREMENT BY -1 MINVALUE 0 MAXVALUE -1000",
            ),
            case(
                "create sequence seq nocycle nocache maxvalue 1000 cache 1",
                "CREATE SEQUENCE `seq` NOCYCLE NOCACHE MAXVALUE 1000 CACHE 1",
            ),
            case(
                "create sequence seq increment -1 no minvalue no maxvalue cache = 1",
                "CREATE SEQUENCE `seq` INCREMENT BY -1 NO MINVALUE NO MAXVALUE CACHE 1",
            ),
            case(
                "create sequence if not exists seq increment 1 minvalue 0 nomaxvalue cache 100 nocycle",
                "CREATE SEQUENCE IF NOT EXISTS `seq` INCREMENT BY 1 MINVALUE 0 NO MAXVALUE CACHE 100 NOCYCLE",
            ),
            case("drop sequence seq", "DROP SEQUENCE `seq`"),
            case("drop sequence seq, seq2", "DROP SEQUENCE `seq`, `seq2`"),
            case(
                "drop sequence if exists seq, seq2",
                "DROP SEQUENCE IF EXISTS `seq`, `seq2`",
            ),
            case(
                "drop sequence if exists seq",
                "DROP SEQUENCE IF EXISTS `seq`",
            ),
            case("drop sequence sequence", "DROP SEQUENCE `sequence`"),
        ],
        whole_statement,
    );
}

/// `pkg/parser/ast/ddl_test.go::TestIfExistsRestore` — both the plain and
/// the TiDB-special-comment renderings of every IF EXISTS modifier.
#[test]
fn if_exists_restore() {
    let rows: &[(&str, &str, &str)] = &[
        (
            "drop index if exists idx on t",
            "DROP INDEX IF EXISTS `idx` ON `t`",
            "DROP INDEX /*T! IF EXISTS  */`idx` ON `t`",
        ),
        (
            "create unique index if not exists idx on t(c)",
            "CREATE UNIQUE INDEX IF NOT EXISTS `idx` ON `t` (`c`)",
            "CREATE UNIQUE INDEX /*T! IF NOT EXISTS  */`idx` ON `t` (`c`)",
        ),
        (
            "alter table t add column if not exists c int",
            "ALTER TABLE `t` ADD COLUMN IF NOT EXISTS `c` INT",
            "ALTER TABLE `t` ADD COLUMN /*T! IF NOT EXISTS  */`c` INT",
        ),
        (
            "alter table t drop column if exists c",
            "ALTER TABLE `t` DROP COLUMN IF EXISTS `c`",
            "ALTER TABLE `t` DROP COLUMN /*T! IF EXISTS  */`c`",
        ),
        (
            "alter table t add key if not exists idx2(c2), add vector index if not exists idx3(c3), add columnar index if not exists idx4(c4)",
            "ALTER TABLE `t` ADD INDEX IF NOT EXISTS `idx2`(`c2`), ADD VECTOR INDEX IF NOT EXISTS `idx3`(`c3`), ADD COLUMNAR INDEX IF NOT EXISTS `idx4`(`c4`)",
            "ALTER TABLE `t` ADD INDEX/*T!  IF NOT EXISTS */ `idx2`(`c2`), ADD VECTOR INDEX/*T!  IF NOT EXISTS */ `idx3`(`c3`), ADD COLUMNAR INDEX/*T!  IF NOT EXISTS */ `idx4`(`c4`)",
        ),
        (
            "alter table t add foreign key if not exists fk(c) references t2(c)",
            "ALTER TABLE `t` ADD CONSTRAINT `fk` FOREIGN KEY IF NOT EXISTS (`c`) REFERENCES `t2`(`c`)",
            "ALTER TABLE `t` ADD CONSTRAINT `fk` FOREIGN KEY /*T! IF NOT EXISTS  */(`c`) REFERENCES `t2`(`c`)",
        ),
        (
            "alter table t drop index if exists idx",
            "ALTER TABLE `t` DROP INDEX IF EXISTS `idx`",
            "ALTER TABLE `t` DROP INDEX /*T! IF EXISTS  */`idx`",
        ),
        // FIXME upstream: supported in the AST but rejected by Go's parser.
        ("alter table t change column if exists c c2 int",
         "ALTER TABLE `t` CHANGE COLUMN IF EXISTS `c` `c2` INT",
         "ALTER TABLE `t` CHANGE COLUMN /*T! IF EXISTS  */`c` `c2` INT",),
        (
            "alter table t modify column if exists c int",
            "ALTER TABLE `t` MODIFY COLUMN IF EXISTS `c` INT",
            "ALTER TABLE `t` MODIFY COLUMN /*T! IF EXISTS  */`c` INT",
        ),
        (
            "alter table t add partition if not exists (partition p1 values less than (10))",
            "ALTER TABLE `t` ADD PARTITION IF NOT EXISTS (PARTITION `p1` VALUES LESS THAN (10))",
            "ALTER TABLE `t` ADD PARTITION/*T!  IF NOT EXISTS */ (PARTITION `p1` VALUES LESS THAN (10))",
        ),
        (
            "alter table t drop partition if exists p1, p2",
            "ALTER TABLE `t` DROP PARTITION IF EXISTS `p1`,`p2`",
            "ALTER TABLE `t` DROP PARTITION /*T! IF EXISTS  */`p1`,`p2`",
        ),
    ];
    for &(source, normal, special) in rows {
        run_node_restore_test_with_flags(
            "%s",
            &[case(source, normal)],
            RestoreFlags::DEFAULT,
            whole_statement,
        );
        run_node_restore_test_with_flags(
            "%s",
            &[case(source, special)],
            SPECIAL_COMMENT,
            whole_statement,
        );
    }
}

/// `pkg/parser/ast/ddl_test.go::TestAlterDatabaseRestore`.
#[test]
fn alter_database_restore() {
    let rows: &[(&str, RestoreFlags, &str)] = &[
        (
            "alter database db1 charset='ascii'",
            RestoreFlags::DEFAULT,
            "ALTER DATABASE `db1` CHARACTER SET = ascii",
        ),
        (
            "alter database db1 charset='ascii'",
            RestoreFlags::DEFAULT | SPECIAL_COMMENT,
            "ALTER DATABASE `db1` CHARACTER SET = ascii",
        ),
        (
            "alter database db1 collate='ascii_bin'",
            RestoreFlags::DEFAULT,
            "ALTER DATABASE `db1` COLLATE = ascii_bin",
        ),
        (
            "alter database db1 collate='ascii_bin'",
            RestoreFlags::DEFAULT | SPECIAL_COMMENT,
            "ALTER DATABASE `db1` COLLATE = ascii_bin",
        ),
        (
            "alter database db1 placement policy p1",
            RestoreFlags::DEFAULT,
            "ALTER DATABASE `db1` PLACEMENT POLICY = `p1`",
        ),
        (
            "alter database db1 placement policy p1",
            RestoreFlags::DEFAULT | SPECIAL_COMMENT,
            "/*T![placement] ALTER DATABASE `db1` PLACEMENT POLICY = `p1` */",
        ),
        (
            "alter database db1 placement policy p1 charset='ascii'",
            RestoreFlags::DEFAULT,
            "ALTER DATABASE `db1` PLACEMENT POLICY = `p1` CHARACTER SET = ascii",
        ),
        (
            "alter database db1 placement policy p1 charset='ascii'",
            RestoreFlags::DEFAULT | SPECIAL_COMMENT,
            "ALTER DATABASE `db1` /*T![placement] PLACEMENT POLICY = `p1` */ CHARACTER SET = ascii",
        ),
    ];
    for &(source, flags, expect) in rows {
        run_node_restore_test_with_flags("%s", &[case(source, expect)], flags, whole_statement);
    }
}

/// `pkg/parser/ast/ddl_test.go::TestCreatePlacementPolicyRestore`.
#[test]
fn create_placement_policy_restore() {
    let rows: &[(&str, RestoreFlags, &str)] = &[
        (
            r#"create placement policy p1 primary_region="r1" regions='r1,r2' followers=1"#,
            RestoreFlags::DEFAULT,
            "CREATE PLACEMENT POLICY `p1` PRIMARY_REGION = 'r1' REGIONS = 'r1,r2' FOLLOWERS = 1",
        ),
        (
            r#"create placement policy p1 primary_region="r1" regions='r1,r2' followers=1"#,
            RestoreFlags::DEFAULT | SPECIAL_COMMENT,
            "/*T![placement] CREATE PLACEMENT POLICY `p1` PRIMARY_REGION = 'r1' REGIONS = 'r1,r2' FOLLOWERS = 1 */",
        ),
        (
            r#"create placement policy if not exists p1 primary_region="r1" regions='r1,r2' followers=1"#,
            RestoreFlags::DEFAULT,
            "CREATE PLACEMENT POLICY IF NOT EXISTS `p1` PRIMARY_REGION = 'r1' REGIONS = 'r1,r2' FOLLOWERS = 1",
        ),
        (
            r#"create placement policy if not exists p1 primary_region="r1" regions='r1,r2' followers=1"#,
            RestoreFlags::DEFAULT | SPECIAL_COMMENT,
            "/*T![placement] CREATE PLACEMENT POLICY IF NOT EXISTS `p1` PRIMARY_REGION = 'r1' REGIONS = 'r1,r2' FOLLOWERS = 1 */",
        ),
        (
            "create or replace placement policy p1 followers=1",
            RestoreFlags::DEFAULT,
            "CREATE OR REPLACE PLACEMENT POLICY `p1` FOLLOWERS = 1",
        ),
        (
            "create or replace placement policy p1 followers=1",
            RestoreFlags::DEFAULT | SPECIAL_COMMENT,
            "/*T![placement] CREATE OR REPLACE PLACEMENT POLICY `p1` FOLLOWERS = 1 */",
        ),
    ];
    for &(source, flags, expect) in rows {
        run_node_restore_test_with_flags("%s", &[case(source, expect)], flags, whole_statement);
    }
}

/// `pkg/parser/ast/ddl_test.go::TestAlterPlacementPolicyRestore`.
#[test]
fn alter_placement_policy_restore() {
    let source = r#"alter placement policy p1 primary_region="r1" regions='r1,r2' followers=1"#;
    run_node_restore_test_with_flags(
        "%s",
        &[case(
            source,
            "ALTER PLACEMENT POLICY `p1` PRIMARY_REGION = 'r1' REGIONS = 'r1,r2' FOLLOWERS = 1",
        )],
        RestoreFlags::DEFAULT,
        whole_statement,
    );
    run_node_restore_test_with_flags(
        "%s",
        &[case(
            source,
            "/*T![placement] ALTER PLACEMENT POLICY `p1` PRIMARY_REGION = 'r1' REGIONS = 'r1,r2' FOLLOWERS = 1 */",
        )],
        SPECIAL_COMMENT,
        whole_statement,
    );
}

/// `pkg/parser/ast/ddl_test.go::TestDropPlacementPolicyRestore`.
#[test]
fn drop_placement_policy_restore() {
    run_node_restore_test_with_flags(
        "%s",
        &[case(
            "drop placement policy p1",
            "DROP PLACEMENT POLICY `p1`",
        )],
        RestoreFlags::DEFAULT,
        whole_statement,
    );
    run_node_restore_test_with_flags(
        "%s",
        &[case(
            "drop placement policy p1",
            "/*T![placement] DROP PLACEMENT POLICY `p1` */",
        )],
        SPECIAL_COMMENT,
        whole_statement,
    );
    run_node_restore_test_with_flags(
        "%s",
        &[case(
            "drop placement policy if exists p1",
            "DROP PLACEMENT POLICY IF EXISTS `p1`",
        )],
        RestoreFlags::DEFAULT,
        whole_statement,
    );
    run_node_restore_test_with_flags(
        "%s",
        &[case(
            "drop placement policy if exists p1",
            "/*T![placement] DROP PLACEMENT POLICY IF EXISTS `p1` */",
        )],
        SPECIAL_COMMENT,
        whole_statement,
    );
}

/// `pkg/parser/ast/ddl_test.go::TestRemovePlacementRestore`.
///
/// The oracle intentionally renders empty strings where every remaining
/// payload is a suppressed placement rule, so this uses the StmtChange
/// harness exactly as Go does.
#[test]
fn remove_placement_restore() {
    let flags = RestoreFlags::SKIP_PLACEMENT_RULE_FOR_RESTORE;
    let cases = [
        case(
            "CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY auto_increment, b varchar(255)) PLACEMENT POLICY=placement1;",
            "CREATE TABLE `t1` (`id` BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,`b` VARCHAR(255)) ",
        ),
        case(
            "CREATE TABLE `t1` (\n  `a` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p2` */",
            "CREATE TABLE `t1` (`a` INT(11) DEFAULT NULL) ENGINE = InnoDB DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_BIN ",
        ),
        case(
            "CREATE TABLE t4 (firstname VARCHAR(25) NOT NULL,lastname VARCHAR(25) NOT NULL,username VARCHAR(16) NOT NULL,email VARCHAR(35),joined DATE NOT NULL) PARTITION BY RANGE( YEAR(joined) ) (PARTITION p0 VALUES LESS THAN (1960) PLACEMENT POLICY=p1,PARTITION p1 VALUES LESS THAN (1970),PARTITION p2 VALUES LESS THAN (1980),PARTITION p3 VALUES LESS THAN (1990),PARTITION p4 VALUES LESS THAN MAXVALUE);",
            "CREATE TABLE `t4` (`firstname` VARCHAR(25) NOT NULL,`lastname` VARCHAR(25) NOT NULL,`username` VARCHAR(16) NOT NULL,`email` VARCHAR(35),`joined` DATE NOT NULL) PARTITION BY RANGE (YEAR(`joined`)) (PARTITION `p0` VALUES LESS THAN (1960) ,PARTITION `p1` VALUES LESS THAN (1970),PARTITION `p2` VALUES LESS THAN (1980),PARTITION `p3` VALUES LESS THAN (1990),PARTITION `p4` VALUES LESS THAN (MAXVALUE))",
        ),
        case("ALTER TABLE t3 PLACEMENT POLICY=DEFAULT;", "ALTER TABLE `t3`"),
        case("ALTER TABLE t1 PLACEMENT POLICY=p10", "ALTER TABLE `t1`"),
        case(
            "ALTER TABLE t1 PLACEMENT POLICY=p10, add d text(50)",
            "ALTER TABLE `t1` ADD COLUMN `d` TEXT(50)",
        ),
        case("alter table tp PARTITION p1 placement policy p2", ""),
        case(
            "alter table t add d text(50) PARTITION p1 placement policy p2",
            "ALTER TABLE `t` ADD COLUMN `d` TEXT(50)",
        ),
        case(
            "alter table tp set tiflash replica 1 PARTITION p1 placement policy p2",
            "ALTER TABLE `tp` SET TIFLASH REPLICA 1",
        ),
        case("ALTER DATABASE TestResetPlacementDB PLACEMENT POLICY SET DEFAULT", ""),
        case(
            "ALTER DATABASE TestResetPlacementDB PLACEMENT POLICY p1 charset utf8mb4",
            "ALTER DATABASE `TestResetPlacementDB`  CHARACTER SET = utf8mb4",
        ),
        case("/*T![placement] ALTER DATABASE `db1` PLACEMENT POLICY = `p1` */", ""),
        case(
            "ALTER PLACEMENT POLICY p3 PRIMARY_REGION='us-east-1' REGIONS='us-east-1,us-east-2,us-west-1';",
            "",
        ),
    ];
    run_node_restore_test_with_flags_stmt_change("%s", &cases, flags, whole_statement);
}

/// `pkg/parser/ast/ddl_test.go::TestFlashBackDatabaseRestore`.
#[test]
fn flash_back_database_restore() {
    run_node_restore_test(
        "%s",
        &[
            case("flashback database M", "FLASHBACK DATABASE `M`"),
            case("flashback schema M", "FLASHBACK DATABASE `M`"),
            case("flashback database M to n", "FLASHBACK DATABASE `M` TO `n`"),
            case("flashback schema M to N", "FLASHBACK DATABASE `M` TO `N`"),
        ],
        whole_statement,
    );
}

/// `pkg/parser/ast/ddl_test.go::TestTableOptionTTLRestore`.
#[test]
fn table_option_ttl_restore() {
    let rows: &[(&str, RestoreFlags, &str)] = &[
        (
            "create table t (created_at datetime) ttl = created_at + INTERVAL 1 YEAR",
            RestoreFlags::DEFAULT,
            "CREATE TABLE `t` (`created_at` DATETIME) TTL = `created_at` + INTERVAL 1 YEAR",
        ),
        (
            "create table t (created_at datetime) ttl = created_at + INTERVAL 1 YEAR",
            RestoreFlags::DEFAULT | SPECIAL_COMMENT,
            "CREATE TABLE `t` (`created_at` DATETIME) /*T![ttl] TTL = `created_at` + INTERVAL 1 YEAR */",
        ),
        (
            "alter table t ttl_enable = 'OFF'",
            RestoreFlags::DEFAULT,
            "ALTER TABLE `t` TTL_ENABLE = 'OFF'",
        ),
        (
            "alter table t ttl_enable = 'OFF'",
            RestoreFlags::DEFAULT | SPECIAL_COMMENT,
            "ALTER TABLE `t` /*T![ttl] TTL_ENABLE = 'OFF' */",
        ),
        (
            "alter table t remove ttl",
            RestoreFlags::DEFAULT,
            "ALTER TABLE `t` REMOVE TTL",
        ),
        (
            "alter table t remove ttl",
            RestoreFlags::DEFAULT | SPECIAL_COMMENT,
            "ALTER TABLE `t` /*T![ttl] REMOVE TTL */",
        ),
    ];
    for &(source, flags, expect) in rows {
        run_node_restore_test_with_flags("%s", &[case(source, expect)], flags, whole_statement);
    }
}

/// `pkg/parser/ast/ddl_test.go::TestTableOptionTTLRestoreWithTTLEnableOffFlag`.
#[test]
fn table_option_ttl_restore_with_ttl_enable_off_flag() {
    let flag = RestoreFlags::WITH_TTL_ENABLE_OFF;
    let rows: &[(&str, RestoreFlags, &str)] = &[
        (
            "create table t (created_at datetime) ttl = created_at + INTERVAL 1 YEAR",
            RestoreFlags::DEFAULT | flag,
            "CREATE TABLE `t` (`created_at` DATETIME) TTL = `created_at` + INTERVAL 1 YEAR TTL_ENABLE = 'OFF'",
        ),
        (
            "create table t (created_at datetime) ttl = created_at + INTERVAL 1 YEAR",
            RestoreFlags::DEFAULT | SPECIAL_COMMENT | flag,
            "CREATE TABLE `t` (`created_at` DATETIME) /*T![ttl] TTL = `created_at` + INTERVAL 1 YEAR */ /*T![ttl] TTL_ENABLE = 'OFF' */",
        ),
        (
            "alter table t ttl_enable = 'ON'",
            RestoreFlags::DEFAULT | flag,
            "ALTER TABLE `t`",
        ),
        (
            "alter table t ttl_enable = 'ON'",
            RestoreFlags::DEFAULT | SPECIAL_COMMENT | flag,
            "ALTER TABLE `t`",
        ),
        (
            "alter table t remove ttl",
            RestoreFlags::DEFAULT | flag,
            "ALTER TABLE `t` REMOVE TTL",
        ),
        (
            "alter table t remove ttl",
            RestoreFlags::DEFAULT | SPECIAL_COMMENT | flag,
            "ALTER TABLE `t` /*T![ttl] REMOVE TTL */",
        ),
        (
            "create table t (created_at datetime) ttl = created_at + INTERVAL 1 YEAR ttl_enable = 'ON'",
            RestoreFlags::DEFAULT | flag,
            "CREATE TABLE `t` (`created_at` DATETIME) TTL = `created_at` + INTERVAL 1 YEAR TTL_ENABLE = 'OFF'",
        ),
        (
            "create table t (created_at datetime) ttl = created_at + INTERVAL 1 YEAR ttl_enable = 'ON'",
            RestoreFlags::DEFAULT | SPECIAL_COMMENT | flag,
            "CREATE TABLE `t` (`created_at` DATETIME) /*T![ttl] TTL = `created_at` + INTERVAL 1 YEAR */ /*T![ttl] TTL_ENABLE = 'OFF' */",
        ),
        (
            "alter table t ttl_enable = 'ON' placement policy p1",
            RestoreFlags::DEFAULT | SPECIAL_COMMENT | flag,
            "ALTER TABLE `t` /*T![placement] PLACEMENT POLICY = `p1` */",
        ),
    ];
    for &(source, case_flags, expect) in rows {
        run_node_restore_test_with_flags_stmt_change(
            "%s",
            &[case(source, expect)],
            case_flags,
            whole_statement,
        );
    }
}

/// `pkg/parser/ast/ddl_test.go::TestPresplitIndexSpecialComments`.
#[test]
fn presplit_index_special_comments() {
    let special_cmt_flag = RestoreFlags::DEFAULT | SPECIAL_COMMENT;
    run_node_restore_test_with_flags(
        "%s",
        &[
            case(
                "ALTER TABLE t ADD INDEX (a) PRE_SPLIT_REGIONS = 4",
                "ALTER TABLE `t` ADD INDEX(`a`) /*T![pre_split] PRE_SPLIT_REGIONS = 4 */",
            ),
            case(
                "ALTER TABLE t ADD INDEX (a) PRE_SPLIT_REGIONS 4",
                "ALTER TABLE `t` ADD INDEX(`a`) /*T![pre_split] PRE_SPLIT_REGIONS = 4 */",
            ),
            case(
                "ALTER TABLE t ADD PRIMARY KEY (a) CLUSTERED PRE_SPLIT_REGIONS = 4",
                "ALTER TABLE `t` ADD PRIMARY KEY(`a`) /*T![clustered_index] CLUSTERED */ /*T![pre_split] PRE_SPLIT_REGIONS = 4 */",
            ),
            case(
                "ALTER TABLE t ADD PRIMARY KEY (a) PRE_SPLIT_REGIONS = 4 NONCLUSTERED",
                "ALTER TABLE `t` ADD PRIMARY KEY(`a`) /*T![clustered_index] NONCLUSTERED */ /*T![pre_split] PRE_SPLIT_REGIONS = 4 */",
            ),
            case(
                "ALTER TABLE t ADD INDEX (a) PRE_SPLIT_REGIONS = (between (1, 'a') and (2, 'b') regions 4);",
                "ALTER TABLE `t` ADD INDEX(`a`) /*T![pre_split] PRE_SPLIT_REGIONS = (BETWEEN (1,_UTF8MB4'a') AND (2,_UTF8MB4'b') REGIONS 4) */",
            ),
            case(
                "ALTER TABLE t ADD INDEX idx(a) pre_split_regions = 100, ADD INDEX idx2(b) pre_split_regions = (by(1),(2),(3))",
                "ALTER TABLE `t` ADD INDEX `idx`(`a`) /*T![pre_split] PRE_SPLIT_REGIONS = 100 */, ADD INDEX `idx2`(`b`) /*T![pre_split] PRE_SPLIT_REGIONS = (BY (1),(2),(3)) */",
            ),
            case(
                "ALTER TABLE t ADD INDEX (a) comment 'a' PRE_SPLIT_REGIONS = (between (1, 'a') and (2, 'b') regions 4);",
                "ALTER TABLE `t` ADD INDEX(`a`) COMMENT 'a' /*T![pre_split] PRE_SPLIT_REGIONS = (BETWEEN (1,_UTF8MB4'a') AND (2,_UTF8MB4'b') REGIONS 4) */",
            ),
        ],
        special_cmt_flag,
        whole_statement,
    );
}

/// `pkg/parser/ast/ddl_test.go::TestResourceGroupDDLStmtRestore`.
#[test]
fn resource_group_ddl_stmt_restore() {
    run_node_restore_test(
        "%s",
        &[
            case(
                "CREATE RESOURCE GROUP IF NOT EXISTS rg1 RU_PER_SEC = 500",
                "CREATE RESOURCE GROUP IF NOT EXISTS `rg1` RU_PER_SEC = 500",
            ),
            case(
                "CREATE RESOURCE GROUP IF NOT EXISTS rg1 RU_PER_SEC = UNLIMITED",
                "CREATE RESOURCE GROUP IF NOT EXISTS `rg1` RU_PER_SEC = UNLIMITED",
            ),
            case(
                "CREATE RESOURCE GROUP IF NOT EXISTS rg1 RU_PER_SEC = 500 BURSTABLE",
                "CREATE RESOURCE GROUP IF NOT EXISTS `rg1` RU_PER_SEC = 500, BURSTABLE = MODERATED",
            ),
            case(
                "CREATE RESOURCE GROUP IF NOT EXISTS rg1 RU_PER_SEC = 500 BURSTABLE=UNLIMITED",
                "CREATE RESOURCE GROUP IF NOT EXISTS `rg1` RU_PER_SEC = 500, BURSTABLE = UNLIMITED",
            ),
            case(
                "CREATE RESOURCE GROUP IF NOT EXISTS rg1 RU_PER_SEC = 500 BURSTABLE=MODERATED",
                "CREATE RESOURCE GROUP IF NOT EXISTS `rg1` RU_PER_SEC = 500, BURSTABLE = MODERATED",
            ),
            case(
                "CREATE RESOURCE GROUP IF NOT EXISTS rg1 RU_PER_SEC = 500 BURSTABLE=OFF",
                "CREATE RESOURCE GROUP IF NOT EXISTS `rg1` RU_PER_SEC = 500, BURSTABLE = OFF",
            ),
            case(
                "CREATE RESOURCE GROUP IF NOT EXISTS rg2 RU_PER_SEC = 600",
                "CREATE RESOURCE GROUP IF NOT EXISTS `rg2` RU_PER_SEC = 600",
            ),
            case(
                "CREATE RESOURCE GROUP IF NOT EXISTS rg3 RU_PER_SEC = 100 PRIORITY = HIGH",
                "CREATE RESOURCE GROUP IF NOT EXISTS `rg3` RU_PER_SEC = 100, PRIORITY = HIGH",
            ),
            case(
                "CREATE RESOURCE GROUP IF NOT EXISTS rg1 RU_PER_SEC = 500 QUERY_LIMIT=(EXEC_ELAPSED='60s', ACTION=COOLDOWN)",
                "CREATE RESOURCE GROUP IF NOT EXISTS `rg1` RU_PER_SEC = 500, QUERY_LIMIT = (EXEC_ELAPSED = '60s' ACTION = COOLDOWN)",
            ),
            case(
                "CREATE RESOURCE GROUP IF NOT EXISTS rg1 RU_PER_SEC = 500 QUERY_LIMIT=(ACTION=SWITCH_GROUP(rg2))",
                "CREATE RESOURCE GROUP IF NOT EXISTS `rg1` RU_PER_SEC = 500, QUERY_LIMIT = (ACTION = SWITCH_GROUP(`rg2`))",
            ),
        ],
        whole_statement,
    );

    run_node_restore_test(
        "%s",
        &[
            case(
                "ALTER RESOURCE GROUP rg1 QUERY_LIMIT=(EXEC_ELAPSED='60s', ACTION=KILL, WATCH=SIMILAR DURATION='10m')",
                "ALTER RESOURCE GROUP `rg1` QUERY_LIMIT = (EXEC_ELAPSED = '60s' ACTION = KILL WATCH = SIMILAR DURATION = '10m')",
            ),
            case(
                "ALTER RESOURCE GROUP rg1 QUERY_LIMIT=(EXEC_ELAPSED='1m', ACTION=SWITCH_GROUP(rg2), WATCH=SIMILAR DURATION='10m')",
                "ALTER RESOURCE GROUP `rg1` QUERY_LIMIT = (EXEC_ELAPSED = '1m' ACTION = SWITCH_GROUP(`rg2`) WATCH = SIMILAR DURATION = '10m')",
            ),
            case(
                "ALTER RESOURCE GROUP rg1 QUERY_LIMIT=NULL",
                "ALTER RESOURCE GROUP `rg1` QUERY_LIMIT = NULL",
            ),
            case(
                "ALTER RESOURCE GROUP `default` BACKGROUND=(TASK_TYPES='br,ddl')",
                "ALTER RESOURCE GROUP `default` BACKGROUND = (TASK_TYPES = 'br,ddl')",
            ),
            case(
                "ALTER RESOURCE GROUP `default` BACKGROUND=NULL",
                "ALTER RESOURCE GROUP `default` BACKGROUND = NULL",
            ),
            case(
                "ALTER RESOURCE GROUP `default` BACKGROUND=(TASK_TYPES='')",
                "ALTER RESOURCE GROUP `default` BACKGROUND = (TASK_TYPES = '')",
            ),
            case(
                "ALTER RESOURCE GROUP rg1 RU_PER_SEC=UNLIMITED",
                "ALTER RESOURCE GROUP `rg1` RU_PER_SEC = UNLIMITED",
            ),
            case(
                "ALTER RESOURCE GROUP rg1 RU_PER_SEC=500",
                "ALTER RESOURCE GROUP `rg1` RU_PER_SEC = 500",
            ),
            case(
                "ALTER RESOURCE GROUP rg1 BURSTABLE=UNLIMITED",
                "ALTER RESOURCE GROUP `rg1` BURSTABLE = UNLIMITED",
            ),
            case(
                "ALTER RESOURCE GROUP rg1 BURSTABLE=MODERATED",
                "ALTER RESOURCE GROUP `rg1` BURSTABLE = MODERATED",
            ),
            case(
                "ALTER RESOURCE GROUP rg1 BURSTABLE=OFF",
                "ALTER RESOURCE GROUP `rg1` BURSTABLE = OFF",
            ),
        ],
        whole_statement,
    );
}
