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

//! Source-owned field-type restore cases from Go's parser/AST suites.

use super::*;

fn parsed_column_type(sql: &str) -> ColumnType {
    let Stmt::Ddl(ddl) = parse(sql).expect("parse field-type source row") else {
        panic!("expected DDL statement");
    };
    let tidb_ast::DdlStmt::CreateTable(table) = ddl.into_inner() else {
        panic!("expected CREATE TABLE");
    };
    table.columns.into_iter().next().expect("source column").ty
}

/// Exact SQL rows from `pkg/parser/types/field_type_test.go:TestHasCharsetFromStmt`.
#[test]
fn parser_types_test_has_charset_from_stmt() {
    for (source_type, expected) in [
        ("int", false),
        ("real", false),
        ("float", false),
        ("bit", false),
        ("bool", false),
        ("char(1)", true),
        ("national char(1)", true),
        ("binary", false),
        ("varchar(1)", true),
        ("national varchar(1)", true),
        ("varbinary(1)", false),
        ("year", false),
        ("date", false),
        ("time", false),
        ("datetime", false),
        ("timestamp", false),
        ("blob", false),
        ("tinyblob", false),
        ("mediumblob", false),
        ("longblob", false),
        ("bit", false),
        ("text", true),
        ("tinytext", true),
        ("mediumtext", true),
        ("longtext", true),
        ("json", false),
        ("enum('1')", true),
        ("set('1')", true),
    ] {
        let sql = format!("CREATE TABLE t(a {source_type})");
        assert_eq!(parsed_column_type(&sql).has_charset(), expected, "{sql}");
    }
}

/// Exact SQL rows from `pkg/parser/types/field_type_test.go:TestEnumSetFlen`.
#[test]
fn parser_types_test_enum_set_flen() {
    for (source_type, expected) in [
        ("enum('a')", 1),
        ("enum('a', 'b')", 1),
        ("enum('a', 'bb')", 2),
        ("enum('a', 'b', 'c')", 1),
        ("enum('a', 'bb', 'c')", 2),
        ("enum('a', 'bb', 'c')", 2),
        ("enum('')", 0),
        ("enum('a', '')", 1),
        ("set('a')", 1),
        ("set('a', 'b')", 3),
        ("set('a', 'bb')", 4),
        ("set('a', 'b', 'c')", 5),
        ("set('a', 'bb', 'c')", 6),
        ("set('')", 0),
        ("set('a', '')", 2),
    ] {
        let sql = format!("CREATE TABLE t(a {source_type})");
        assert_eq!(
            parsed_column_type(&sql).enum_set_display_length(),
            Some(expected),
            "{sql}"
        );
    }
}

#[test]
fn existing_field_type_source_rows_stay_at_the_shared_column_boundary() {
    // `pkg/parser/parser_test.go:TestSimple` and
    // `pkg/parser/ast/ddl_test.go:TestDDLColumnDefRestore` rows. This is a
    // physical-owner regression: CREATE and ALTER must continue to consume
    // the same field-type parser after it leaves the generic table-DDL root.
    for (sql, expected) in [
        (
            "create table t (a int unsigned zerofill)",
            "CREATE TABLE `t` (`a` INT UNSIGNED ZEROFILL)",
        ),
        (
            "create table t (a float(25))",
            "CREATE TABLE `t` (`a` DOUBLE)",
        ),
        (
            "alter table t add column a decimal(10,2) unsigned",
            "ALTER TABLE `t` ADD COLUMN `a` DECIMAL(10,2) UNSIGNED",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

/// NATIONAL/NCHAR/NVARCHAR forms are owned by `pkg/parser/ddl_fieldtype_parser.go`'s
/// shared field-type leaf.  The infoschema integration fixture exercises the
/// NATIONAL CHAR/VARCHAR forms together with per-column charset/collation;
/// keep the compact source table here so CREATE and ALTER cannot drift.
#[test]
fn national_character_type_aliases_match_go() {
    for (sql, expected) in [
        (
            "create table t (a national char(1) charset ascii collate ascii_bin, b national varchar(1) charset ascii collate ascii_bin)",
            "CREATE TABLE `t` (`a` CHAR(1) CHARACTER SET ASCII COLLATE ascii_bin,`b` VARCHAR(1) CHARACTER SET ASCII COLLATE ascii_bin)",
        ),
        (
            "create table t (a national char varying(2), b national character varying(3), c nchar, d nvarchar(4))",
            "CREATE TABLE `t` (`a` VARCHAR(2),`b` VARCHAR(3),`c` CHAR,`d` VARCHAR(4))",
        ),
        (
            "alter table t add column a national varchar(5)",
            "ALTER TABLE `t` ADD COLUMN `a` VARCHAR(5)",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }

    // Go requires a length for NATIONAL VARCHAR/VARCHARACTER and NVARCHAR,
    // while NATIONAL CHAR/CHARACTER and NCHAR may omit it.
    assert!(parse("create table t (a national varchar)").is_err());
    assert!(parse("create table t (a nvarchar)").is_err());
}

/// Source-owned aliases from `pkg/parser/ddl_fieldtype_parser.go` and
/// `pkg/parser/parser_test.go:TestCompatTypes`. They must canonicalize in the
/// shared field-type layer so CREATE and every ALTER column production get
/// exactly the same Go spelling.
#[test]
fn field_type_compatibility_aliases_restore_like_go() {
    assert_eq!(
        r("create table t (a int1,b int2,c int3,d int4,e int8,f middleint,g float4,h float8,i double precision,j real,k fixed(9,2),l bool,m boolean)"),
        "CREATE TABLE `t` (`a` TINYINT,`b` SMALLINT,`c` MEDIUMINT,`d` INT,`e` BIGINT,`f` MEDIUMINT,`g` FLOAT,`h` DOUBLE,`i` DOUBLE,`j` DOUBLE,`k` DECIMAL(9,2),`l` TINYINT(1),`m` TINYINT(1))"
    );
    assert_eq!(
        r("alter table t add column a sql_tsi_year(4) zerofill"),
        "ALTER TABLE `t` ADD COLUMN `a` YEAR(4)"
    );

    // BOOL/BOOLEAN are their own Go parser branch: they materialize
    // TINYINT(1), but never accept an ordinary integer display width.
    assert!(parse("create table t (a bool(1))").is_err());
    // REAL's default (without the unmodelled REAL_AS_FLOAT session mode) is
    // a distinct branch and therefore does not consume DOUBLE's PRECISION.
    assert!(parse("create table t (a real precision)").is_err());
}

/// `parseFieldType` has one geometry storage type but accepts the reserved
/// POINT/SPATIAL spellings and the identifier aliases from
/// `geometryTypeNames`. Every accepted alias restores as `GEOMETRY`; the
/// literal `GEOMETRY` token itself is intentionally not in that Go map and is
/// rejected at the same parser boundary.
#[test]
fn field_type_geometry_aliases_restore_like_go() {
    assert_eq!(
        r("create table t (a point,b spatial,c linestring,d polygon,e multipoint,f multilinestring,g multipolygon,h geometrycollection)"),
        "CREATE TABLE `t` (`a` GEOMETRY,`b` GEOMETRY,`c` GEOMETRY,`d` GEOMETRY,`e` GEOMETRY,`f` GEOMETRY,`g` GEOMETRY,`h` GEOMETRY)"
    );
    assert!(parse("create table t (a geometry)").is_err());
}

/// Exact rows from `pkg/parser/parser_test.go:TestCompatTypes`. Keep this
/// selector separate from the broader alias test above: the ledger's COVERED
/// claim is valid only when every row in the original upstream table executes
/// unchanged through the shared CREATE TABLE field-type leaf.
#[test]
fn go_test_compat_types_rows_match() {
    let rows = [
        (
            "CREATE TABLE t(id INT PRIMARY KEY, c1 BOOL)",
            "CREATE TABLE `t` (`id` INT PRIMARY KEY,`c1` TINYINT(1))",
        ),
        (
            "CREATE TABLE t(id INT PRIMARY KEY, c1 BOOLEAN)",
            "CREATE TABLE `t` (`id` INT PRIMARY KEY,`c1` TINYINT(1))",
        ),
        (
            "CREATE TABLE t(id INT PRIMARY KEY, c1 CHARACTER VARYING(0))",
            "CREATE TABLE `t` (`id` INT PRIMARY KEY,`c1` VARCHAR(0))",
        ),
        (
            "CREATE TABLE t(id INT PRIMARY KEY, c1 FIXED)",
            "CREATE TABLE `t` (`id` INT PRIMARY KEY,`c1` DECIMAL)",
        ),
        (
            "CREATE TABLE t(id INT PRIMARY KEY, c1 FLOAT4)",
            "CREATE TABLE `t` (`id` INT PRIMARY KEY,`c1` FLOAT)",
        ),
        (
            "CREATE TABLE t(id INT PRIMARY KEY, c1 FLOAT8)",
            "CREATE TABLE `t` (`id` INT PRIMARY KEY,`c1` DOUBLE)",
        ),
        (
            "CREATE TABLE t(id INT PRIMARY KEY, c1 INT1)",
            "CREATE TABLE `t` (`id` INT PRIMARY KEY,`c1` TINYINT)",
        ),
        (
            "CREATE TABLE t(id INT PRIMARY KEY, c1 INT2)",
            "CREATE TABLE `t` (`id` INT PRIMARY KEY,`c1` SMALLINT)",
        ),
        (
            "CREATE TABLE t(id INT PRIMARY KEY, c1 INT3)",
            "CREATE TABLE `t` (`id` INT PRIMARY KEY,`c1` MEDIUMINT)",
        ),
        (
            "CREATE TABLE t(id INT PRIMARY KEY, c1 INT4)",
            "CREATE TABLE `t` (`id` INT PRIMARY KEY,`c1` INT)",
        ),
        (
            "CREATE TABLE t(id INT PRIMARY KEY, c1 INT8)",
            "CREATE TABLE `t` (`id` INT PRIMARY KEY,`c1` BIGINT)",
        ),
        (
            "CREATE TABLE t(id INT PRIMARY KEY, c1 LONG VARBINARY)",
            "CREATE TABLE `t` (`id` INT PRIMARY KEY,`c1` MEDIUMBLOB)",
        ),
        (
            "CREATE TABLE t(id INT PRIMARY KEY, c1 LONG VARCHAR)",
            "CREATE TABLE `t` (`id` INT PRIMARY KEY,`c1` MEDIUMTEXT)",
        ),
        (
            "CREATE TABLE t(id INT PRIMARY KEY, c1 LONG)",
            "CREATE TABLE `t` (`id` INT PRIMARY KEY,`c1` MEDIUMTEXT)",
        ),
        (
            "CREATE TABLE t(id INT PRIMARY KEY, c1 MIDDLEINT)",
            "CREATE TABLE `t` (`id` INT PRIMARY KEY,`c1` MEDIUMINT)",
        ),
        (
            "CREATE TABLE t(id INT PRIMARY KEY, c1 NUMERIC)",
            "CREATE TABLE `t` (`id` INT PRIMARY KEY,`c1` DECIMAL)",
        ),
    ];
    assert_eq!(rows.len(), 16, "TestCompatTypes source-row count drifted");
    for (sql, expected) in rows {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

/// Exact rows from `pkg/parser/parser_test.go:TestUUIDTypeMariaDBEnabled`
/// and `TestUUIDTypeMariaDBDisabled`. Go's MariaDB-only UUID pseudo-type is
/// normalized by the field-type parser itself to `CHAR(36)`; the same source
/// rows must remain rejected when the compatibility switch is off.
#[test]
fn mariadb_uuid_type_alias_matches_go() {
    for (sql, expected) in [
        (
            "CREATE TABLE t (id UUID)",
            "CREATE TABLE `t` (`id` CHAR(36))",
        ),
        (
            "CREATE TABLE t1 (a UUID, b VARCHAR(32) NOT NULL)",
            "CREATE TABLE `t1` (`a` CHAR(36),`b` VARCHAR(32) NOT NULL)",
        ),
        (
            "CREATE TABLE uuid (uuid UUID NOT NULL DEFAULT UUID())",
            "CREATE TABLE `uuid` (`uuid` CHAR(36) NOT NULL DEFAULT (UUID()))",
        ),
    ] {
        assert_eq!(
            parse_with_mariadb(sql, true)
                .expect("Go MariaDB-enabled UUID row parses")
                .restore(),
            expected,
            "source SQL: {sql}"
        );
    }

    // The disabled row is the exact `TestUUIDTypeMariaDBDisabled` source
    // contract: UUID is not a general TiDB field type.
    assert!(parse("CREATE TABLE t (id UUID)").is_err());
    // UUID's MariaDB branch fixes CHAR(36); it must not silently accept a
    // parenthesized length as if the input had used CHAR directly.
    assert!(parse_with_mariadb("CREATE TABLE t (id UUID(36))", true).is_err());
}

/// The field-type rows in `pkg/parser/parser_test.go:TestType` are a smaller
/// source-owned family than the full parser test.  Keep the original rows
/// together here so enum/set decoding, BLOB/TEXT lengths, YEAR's consumed
/// modifiers, NATIONAL aliases, and JSON's bare restore all execute through
/// the same `parseFieldType` leaf.  The remaining `TestType` rows (literal
/// lexing and unrelated statement grammar) stay outside this selector.
#[test]
fn go_test_type_field_type_rows_match() {
    let rows = [
        (
            "create table t (c1 enum('a', 'b'), c2 set('a', 'b'))",
            "CREATE TABLE `t` (`c1` ENUM('a','b'),`c2` SET('a','b'))",
        ),
        (
            "create table t (c1 enum('a  ', 'b\t'), c2 set('a  ', 'b\t'))",
            "CREATE TABLE `t` (`c1` ENUM('a','b\t'),`c2` SET('a','b\t'))",
        ),
        (
            "create table t (c1 enum('a', 'b') binary, c2 set('a', 'b') binary)",
            "CREATE TABLE `t` (`c1` ENUM('a','b') BINARY,`c2` SET('a','b') BINARY)",
        ),
        (
            "create table t (c1 enum(0x61, 'b'), c2 set(0x61, 'b'))",
            "CREATE TABLE `t` (`c1` ENUM('a','b'),`c2` SET('a','b'))",
        ),
        (
            "create table t (c1 enum(0b01100001, 'b'), c2 set(0b01100001, 'b'))",
            "CREATE TABLE `t` (`c1` ENUM('a','b'),`c2` SET('a','b'))",
        ),
        (
            "create table t (c1 blob(1024), c2 text(1024))",
            "CREATE TABLE `t` (`c1` BLOB(1024),`c2` TEXT(1024))",
        ),
        (
            "create table t (y year(4), y1 year)",
            "CREATE TABLE `t` (`y` YEAR(4),`y1` YEAR)",
        ),
        (
            "create table t (y year(4) unsigned zerofill zerofill, y1 year signed unsigned zerofill)",
            "CREATE TABLE `t` (`y` YEAR(4),`y1` YEAR)",
        ),
        (
            "create table t (c1 national char(2), c2 national varchar(2))",
            "CREATE TABLE `t` (`c1` CHAR(2),`c2` VARCHAR(2))",
        ),
        (
            "create table t (a JSON)",
            "CREATE TABLE `t` (`a` JSON)",
        ),
    ];
    assert_eq!(
        rows.len(),
        10,
        "TestType field-type source-row count drifted"
    );
    for (sql, expected) in rows {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
    assert!(parse("create table t (c1 enum)").is_err());
    assert!(parse("create table t (c1 set)").is_err());
}

/// `parseIntegerOptions` is an ordered state machine, not two optional
/// keywords. Preserve the independent final flags, including the deliberately
/// unusual but source-valid `ZEROFILL SIGNED` result.
#[test]
fn field_type_numeric_modifier_state_machine_matches_go() {
    assert_eq!(
        r("create table t (a int unsigned signed,b int signed unsigned,c int zerofill signed,d int zerofill signed unsigned,e boolean zerofill signed,f year(4) unsigned zerofill signed)"),
        "CREATE TABLE `t` (`a` INT,`b` INT UNSIGNED,`c` INT ZEROFILL,`d` INT UNSIGNED ZEROFILL,`e` TINYINT(1) ZEROFILL,`f` YEAR(4))"
    );
}

/// Direct source rows from `pkg/parser/ddl_fieldtype_parser.go`'s
/// `parseStringOptions`: modifier order is semantic, and the binary charset
/// is a storage-type normalization rather than merely another restore token.
#[test]
fn field_type_binary_string_options_match_go_normal_form() {
    assert_eq!(
        r("create table t (a varchar(10) binary,b varchar(10) charset utf8 binary,c varchar(10) binary charset utf8,d char(2) charset binary,e enum('a','b') binary,f text ascii,g text byte,h enum('a') charset binary)"),
        "CREATE TABLE `t` (`a` VARCHAR(10) BINARY,`b` VARCHAR(10) BINARY CHARACTER SET UTF8,`c` VARCHAR(10) BINARY CHARACTER SET UTF8,`d` BINARY(2),`e` ENUM('a','b') BINARY,`f` TEXT CHARACTER SET LATIN1,`g` BLOB,`h` ENUM('a'))"
    );
    assert_eq!(
        r("alter table t add column a varchar(10) charset binary"),
        "ALTER TABLE `t` ADD COLUMN `a` VARBINARY(10)"
    );
    assert!(parse("create table t (a int charset utf8)").is_err());
    assert!(parse("create table t (a int binary)").is_err());
    assert!(parse("create table t (a text unicode)").is_err());
    // Go's BYTE/ASCII alternatives return from `parseStringOptions`; a
    // following field-type modifier must therefore be handled by the outer
    // column-option grammar (where BINARY is not a legal generic option),
    // and rejected rather than silently folded into the same type.
    assert!(parse("create table t (a text byte binary)").is_err());
    assert!(parse("create table t (a text ascii binary)").is_err());
}

/// `pkg/parser/ddl_fieldtype_parser.go:parseEnumSetOptions` accepts hex and
/// bit-string members, decodes them to bytes, and lets `FieldType.Restore`
/// quote the decoded values. The same shared field-type leaf is used by
/// ALTER MODIFY/CHANGE, so these source rows must not be rejected merely
/// because the member was written as a binary literal.
#[test]
fn field_type_enum_set_binary_members_match_go() {
    assert_eq!(
        r("alter table t modify column a enum(0x61, b'01100010')"),
        "ALTER TABLE `t` MODIFY COLUMN `a` ENUM('a','b')"
    );
    assert_eq!(
        r("alter table t change column a b set('x', b'10101')"),
        "ALTER TABLE `t` CHANGE COLUMN `a` `b` SET('x','\u{15}')"
    );
    assert_eq!(
        r("create table t (a enum(x'61', 0b01100010))"),
        "CREATE TABLE `t` (`a` ENUM('a','b'))"
    );
}

/// Go's `parseEnumSetOptions` keeps invalid GBK octets in the AST and Go's
/// `Restore` writes them back verbatim. The ordinary Rust `String` restore is
/// intentionally not the lossless API for this case; `restore_bytes` is.
#[test]
fn enum_gbk_binary_members_restore_raw_bytes() {
    let first = parse("create table t(a enum('a', 0x91) charset gbk)").expect("parse");
    assert_eq!(
        first.restore_bytes(),
        b"CREATE TABLE `t` (`a` ENUM('a','\x91'))"
    );

    let second = parse("create table t (a enum('a', 0x91)) charset gbk").expect("parse");
    assert_eq!(
        second.restore_bytes(),
        b"CREATE TABLE `t` (`a` ENUM('a','\x91')) DEFAULT CHARACTER SET = GBK"
    );
}
