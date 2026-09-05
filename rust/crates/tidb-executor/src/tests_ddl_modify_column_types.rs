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

//! Running ports of the `pkg/ddl/modify_column_test.go` tests whose
//! observable contract is this tier's synchronous metadata + per-row column
//! rewrite (Go pkg/ddl/modify_column.go:840 `checkNullValue` /
//! `pkg/ddl/column.go` `updateColumnWorker` semantics transcreated in
//! `ddl/alter_table.rs::modify_column_action` and
//! `kv_table::KvTable::modify_column`). Contracts requiring the online-DDL
//! job machinery (reorg state machine, failpoint hooks, and region splits)
//! remain unimplemented and are not represented as Rust tests.
//!
//! Everything asserted below was measured against the live engine in this
//! workspace; where Go's observable outcome differs, the assertion pins the
//! measured Rust behavior, the comment cites Go's expectation with its
//! symbol location. Such divergence cases are not Go-parity evidence.

use crate::{
    run_alter_table_in, run_create_table_on, run_insert_on, run_select_on, Catalog, KvColumn,
    StmtContext, TableEntry,
};
use tidb_datatype::Datum;

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

fn alter(catalog: &mut Catalog, sql: &str) -> Result<(), crate::DriverError> {
    run_alter_table_in(sql, catalog, "test", &ctx())
}

fn code_of(error: &crate::DriverError) -> u16 {
    error.clone().to_mysql_error().code
}

fn message_of(error: &crate::DriverError) -> String {
    error.clone().to_mysql_error().message
}

// The rows of a `SELECT` rendered as strings, the way Go's
// `testkit.Rows` prints them.
fn text_rows(catalog: &Catalog, sql: &str) -> Vec<Vec<String>> {
    run_select_on(sql, catalog, &ctx())
        .expect("select succeeds")
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|datum| match &datum {
                    Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
                    Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                    Datum::Enum(value, _) => {
                        String::from_utf8_lossy(value.name_bytes()).into_owned()
                    }
                    Datum::Set(value, _) => {
                        String::from_utf8_lossy(value.name_bytes()).into_owned()
                    }
                    Datum::Int(value) => value.to_string(),
                    Datum::Null => "NULL".to_owned(),
                    other => panic!("unexpected datum {other:?}"),
                })
                .collect()
        })
        .collect()
}

// Go `modify_column_test.go:287::TestModifyColumnBetweenStringTypes`.
// Every arm of the Go table that does not depend on the non-strict
// `sql_mode` warning path (the tier has no session `sql_mode` switch):
// same-family shrinks succeed and keep values, shrinking below an existing
// value answers Go's `types.ErrTruncated` 1265 with the offending value in
// the message, `binary` keeps its zero padding, and the char/varchar/
// text/set/enum conversions re-map values by member name — including the
// numeric-filter arms `where a = 1/2` over a reordered enum.
#[test]
fn modify_column_between_string_types_shrinks_expands_and_remaps_enum_set_members() {
    let mut catalog = Catalog::default();

    // varchar to varchar
    run_create_table_on("CREATE TABLE tt (a VARCHAR(10))", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO tt VALUES ('111'),('10000')",
        &mut catalog,
        &ctx(),
    )
    .unwrap();
    alter(&mut catalog, "ALTER TABLE tt CHANGE a a VARCHAR(5)").expect("Go: shrink succeeds");
    assert_eq!(column_flen(&catalog, "tt", "a"), 5, "Go: GetFlen == 5");
    assert_eq!(
        text_rows(&catalog, "SELECT * FROM tt"),
        vec![["111"], ["10000"]]
    );
    let error = alter(&mut catalog, "ALTER TABLE tt CHANGE a a VARCHAR(4)")
        .expect_err("Go: [types:1265]Data truncated for column 'a', value is '10000'");
    assert_eq!(code_of(&error), 1265);
    assert_eq!(
        message_of(&error),
        "Data truncated for column 'a', value is '10000'"
    );
    alter(&mut catalog, "ALTER TABLE tt CHANGE a a VARCHAR(100)").expect("Go: widen succeeds");
    assert_eq!(
        text_rows(&catalog, "SELECT length(a) FROM tt"),
        vec![["3"], ["5"]]
    );

    // char to char
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE tt (a CHAR(10))", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO tt VALUES ('111'),('10000')",
        &mut catalog,
        &ctx(),
    )
    .unwrap();
    alter(&mut catalog, "ALTER TABLE tt CHANGE a a CHAR(5)").expect("Go: shrink succeeds");
    assert_eq!(column_flen(&catalog, "tt", "a"), 5);
    assert_eq!(
        text_rows(&catalog, "SELECT * FROM tt"),
        vec![["111"], ["10000"]]
    );
    let error = alter(&mut catalog, "ALTER TABLE tt CHANGE a a CHAR(4)")
        .expect_err("Go: [types:1265]Data truncated ... '10000'");
    assert_eq!(code_of(&error), 1265);
    assert_eq!(
        message_of(&error),
        "Data truncated for column 'a', value is '10000'"
    );
    alter(&mut catalog, "ALTER TABLE tt CHANGE a a CHAR(100)").expect("Go: widen succeeds");
    assert_eq!(
        text_rows(&catalog, "SELECT length(a) FROM tt"),
        vec![["3"], ["5"]]
    );

    // binary to binary: the stored values are zero-padded, so ANY shrink is
    // refused with the padded value in the message, and a widen re-pads.
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE tt (a BINARY(10))", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO tt VALUES ('111'),('10000')",
        &mut catalog,
        &ctx(),
    )
    .unwrap();
    let error = alter(&mut catalog, "ALTER TABLE tt CHANGE a a BINARY(5)")
        .expect_err("Go: [types:1265]Data truncated ... '111\\x00\\x00\\x00\\x00\\x00\\x00\\x00'");
    assert_eq!(code_of(&error), 1265);
    assert_eq!(
        message_of(&error),
        "Data truncated for column 'a', value is '111\0\0\0\0\0\0\0'"
    );
    assert_eq!(
        column_flen(&catalog, "tt", "a"),
        10,
        "Go: the type is unchanged"
    );
    assert_eq!(
        text_rows(&catalog, "SELECT * FROM tt"),
        vec![["111\0\0\0\0\0\0\0"], ["10000\0\0\0\0\0"]]
    );
    let error = alter(&mut catalog, "ALTER TABLE tt CHANGE a a BINARY(4)")
        .expect_err("Go: [types:1265]Data truncated ... '111\\x00\\x00\\x00\\x00\\x00\\x00\\x00'");
    assert_eq!(code_of(&error), 1265);
    alter(&mut catalog, "ALTER TABLE tt CHANGE a a BINARY(12)").expect("Go: widen succeeds");
    assert_eq!(
        text_rows(&catalog, "SELECT * FROM tt"),
        vec![["111\0\0\0\0\0\0\0\0\0"], ["10000\0\0\0\0\0\0\0"]]
    );
    assert_eq!(
        text_rows(&catalog, "SELECT length(a) FROM tt"),
        vec![["12"], ["12"]]
    );

    // varbinary to varbinary
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE tt (a VARBINARY(10))", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO tt VALUES ('111'),('10000')",
        &mut catalog,
        &ctx(),
    )
    .unwrap();
    alter(&mut catalog, "ALTER TABLE tt CHANGE a a VARBINARY(5)").expect("Go: shrink succeeds");
    assert_eq!(column_flen(&catalog, "tt", "a"), 5);
    assert_eq!(
        text_rows(&catalog, "SELECT * FROM tt"),
        vec![["111"], ["10000"]]
    );
    let error = alter(&mut catalog, "ALTER TABLE tt CHANGE a a VARBINARY(4)")
        .expect_err("Go: [types:1265]Data truncated ... '10000'");
    assert_eq!(code_of(&error), 1265);
    alter(&mut catalog, "ALTER TABLE tt CHANGE a a VARBINARY(12)").expect("Go: widen succeeds");
    assert_eq!(
        text_rows(&catalog, "SELECT * FROM tt"),
        vec![["111"], ["10000"]]
    );
    assert_eq!(
        text_rows(&catalog, "SELECT length(a) FROM tt"),
        vec![["3"], ["5"]]
    );

    // varchar to char
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE tt (a VARCHAR(10))", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO tt VALUES ('111'),('10000')",
        &mut catalog,
        &ctx(),
    )
    .unwrap();
    alter(&mut catalog, "ALTER TABLE tt CHANGE a a CHAR(10)").expect("Go: conversion succeeds");
    assert!(
        column_code_is_char(&catalog, "tt", "a"),
        "Go: GetType() == mysql.TypeString"
    );
    assert_eq!(column_flen(&catalog, "tt", "a"), 10);
    assert_eq!(
        text_rows(&catalog, "SELECT * FROM tt"),
        vec![["111"], ["10000"]]
    );
    let error = alter(&mut catalog, "ALTER TABLE tt CHANGE a a CHAR(4)")
        .expect_err("Go: [types:1265]Data truncated ... '10000'");
    assert_eq!(code_of(&error), 1265);

    // char to text
    alter(&mut catalog, "ALTER TABLE tt CHANGE a a TEXT").expect("Go: conversion succeeds");

    // text to set: a member list that misses an existing value is refused,
    // one that covers it re-maps by name.
    let error = alter(&mut catalog, "ALTER TABLE tt CHANGE a a SET('111', '2222')")
        .expect_err("Go: [types:1265]Data truncated ... '10000'");
    assert_eq!(code_of(&error), 1265);
    alter(
        &mut catalog,
        "ALTER TABLE tt CHANGE a a SET('111', '10000')",
    )
    .expect("Go: conversion succeeds");
    assert_eq!(
        text_rows(&catalog, "SELECT * FROM tt"),
        vec![["111"], ["10000"]]
    );

    // set to set
    alter(
        &mut catalog,
        "ALTER TABLE tt CHANGE a a SET('10000', '111')",
    )
    .expect("Go: re-ordering succeeds");
    assert_eq!(
        text_rows(&catalog, "SELECT * FROM tt"),
        vec![["111"], ["10000"]]
    );

    // set to enum
    let error = alter(
        &mut catalog,
        "ALTER TABLE tt CHANGE a a ENUM('111', '2222')",
    )
    .expect_err("Go: [types:1265]Data truncated ... '10000'");
    assert_eq!(code_of(&error), 1265);
    alter(
        &mut catalog,
        "ALTER TABLE tt CHANGE a a ENUM('111', '10000')",
    )
    .expect("Go: conversion succeeds");
    assert_eq!(
        text_rows(&catalog, "SELECT * FROM tt"),
        vec![["111"], ["10000"]]
    );
    alter(
        &mut catalog,
        "ALTER TABLE tt CHANGE a a ENUM('10000', '111')",
    )
    .expect("Go: re-ordering succeeds");
    assert_eq!(
        text_rows(&catalog, "SELECT * FROM tt WHERE a = 1"),
        vec![["10000"]],
        "Go: the numeric filter addresses the enum INDEX, so the re-map moves values"
    );
    assert_eq!(
        text_rows(&catalog, "SELECT * FROM tt WHERE a = 2"),
        vec![["111"]]
    );

    // Go closes with the non-strict arm (`set @@sql_mode=""` + `show
    // warnings`); this tier has no session sql_mode switch, so the
    // strict-path refusals above are the pinned half. See the batch receipt.
}

// Go `modify_column_test.go:169::TestModifyColumnNullToNotNullWithChangingVal2`.
// Go injects the NULL row mid-DDL through the
// `beforeDoModifyColumnSkipReorgCheck` failpoint and requires
// `[ddl:1138]Invalid use of NULL value` (pkg/ddl/modify_column.go:840
// `return true, dbterror.ErrInvalidUseOfNull`). The same data state is
// reachable synchronously — a NULL row present when the MODIFY runs — and
// the alteration must be refused with the table left untouched.
//
#[test]
fn modify_column_null_to_not_null_rejects_rows_holding_nulls() {
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE tt (a BIGINT, b INT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO tt VALUES (1,1),(2,2),(3,3)",
        &mut catalog,
        &ctx(),
    )
    .unwrap();
    // Go reaches this state via its failpoint; synchronously it is just a row.
    run_insert_on("INSERT INTO tt VALUES (NULL, NULL)", &mut catalog, &ctx()).unwrap();

    let error = alter(&mut catalog, "ALTER TABLE tt MODIFY a INT NOT NULL")
        .expect_err("Go: [ddl:1138]Invalid use of NULL value");
    assert_eq!(code_of(&error), 1138);
    assert_eq!(message_of(&error), "Invalid use of NULL value");
    // The rows survive the refused alteration.
    assert_eq!(
        text_rows(&catalog, "SELECT * FROM tt"),
        vec![["1", "1"], ["2", "2"], ["3", "3"], ["NULL", "NULL"]]
    );
}

// Go `modify_column_test.go:1044::TestModifyIntegerColumn`, value halves.
// Go runs the full signed/unsigned matrix with boundary values; the
// `expectedReorgTp` halves (captured through the `getModifyColumnType`
// failpoint) are the [`#[ignore]`d] sibling. Pinned here: every
// out-of-new-range boundary value is refused with Go's
// "Data truncated for column 'a'" (or overflow) message, every in-range
// boundary set converts cleanly, for signed→signed, signed→unsigned,
// unsigned→unsigned, and unsigned→signed narrowing chains.
#[test]
fn modify_integer_column_boundary_values_are_refused_or_converted_exactly() {
    // signed -> signed (Go's signed2Signed): [max(new)+1, max(old)] fail;
    // [min(new)-1, min(old)] fail; [max(new), min(new), 0] pass.
    for value in [
        2147483648_i64,
        9223372036854775807,
        -2147483649,
        -9223372036854775808,
    ] {
        let mut catalog = Catalog::default();
        run_create_table_on("CREATE TABLE t (a BIGINT)", &mut catalog).unwrap();
        run_insert_on(
            &format!("INSERT INTO t VALUES ({value})"),
            &mut catalog,
            &ctx(),
        )
        .unwrap();
        let error = alter(&mut catalog, "ALTER TABLE t MODIFY COLUMN a INT")
            .expect_err("Go: boundary value must be refused");
        let message = message_of(&error);
        assert!(
            message.contains("Data truncated for column 'a'") || message.contains("overflow"),
            "Go: err must mention truncation or overflow, got {message}"
        );
    }

    // In-range boundaries pass and keep their values.
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE t (a INT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO t VALUES (2147483647), (-2147483648), (0)",
        &mut catalog,
        &ctx(),
    )
    .unwrap();
    alter(&mut catalog, "ALTER TABLE t MODIFY COLUMN a BIGINT")
        .expect("Go: signed -> wider signed passes");
    assert_eq!(
        text_rows(&catalog, "SELECT * FROM t"),
        vec![["2147483647"], ["-2147483648"], ["0"]]
    );

    // signed -> unsigned: the negative half of the old range is refused.
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE t (a INT)", &mut catalog).unwrap();
    run_insert_on("INSERT INTO t VALUES (-1)", &mut catalog, &ctx()).unwrap();
    let error = alter(&mut catalog, "ALTER TABLE t MODIFY COLUMN a INT UNSIGNED")
        .expect_err("Go: [minValOfOldCol, -1] fail");
    assert!(message_of(&error).contains("Data truncated for column 'a'"));

    // unsigned -> unsigned: values above the new maximum are refused,
    // the new maximum itself passes.
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE t (a BIGINT UNSIGNED)", &mut catalog).unwrap();
    run_insert_on("INSERT INTO t VALUES (65536)", &mut catalog, &ctx()).unwrap();
    let error = alter(
        &mut catalog,
        "ALTER TABLE t MODIFY COLUMN a SMALLINT UNSIGNED",
    )
    .expect_err("Go: [maxValOfNewCol+1, maxValOfOldCol] fail");
    assert!(message_of(&error).contains("Data truncated for column 'a'"));
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE t (a BIGINT UNSIGNED)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO t VALUES (65535), (0), (1)",
        &mut catalog,
        &ctx(),
    )
    .unwrap();
    alter(
        &mut catalog,
        "ALTER TABLE t MODIFY COLUMN a SMALLINT UNSIGNED",
    )
    .expect("Go: [0, maxValOfNewCol] pass");

    // unsigned -> signed: above the new signed maximum is refused, at it passes.
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE t (a BIGINT UNSIGNED)", &mut catalog).unwrap();
    run_insert_on("INSERT INTO t VALUES (128), (1), (0)", &mut catalog, &ctx()).unwrap();
    let error = alter(&mut catalog, "ALTER TABLE t MODIFY COLUMN a TINYINT")
        .expect_err("Go: [maxValOfNewCol+1, maxValOfOldCol] fail");
    assert!(message_of(&error).contains("Data truncated for column 'a'"));
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE t (a BIGINT UNSIGNED)", &mut catalog).unwrap();
    run_insert_on("INSERT INTO t VALUES (127), (1), (0)", &mut catalog, &ctx()).unwrap();
    alter(&mut catalog, "ALTER TABLE t MODIFY COLUMN a TINYINT")
        .expect("Go: [0, maxValOfNewCol] pass");
}

// Go `modify_column_test.go:1190::TestModifyStringColumn`, pass/fail halves.
// Go's `expectedReorgTp` captures (the `getModifyColumnType` failpoint) are
// the [`#[ignore]`d] sibling. Pinned here: each of Go's twelve
// (old type, new type, value) cases refuses with "Data truncated for
// column 'a'" or converts cleanly — including the CHAR trailing-space
// semantics that let a 15-character mostly-spaces value fit `char(10)`.
#[test]
fn modify_string_column_pass_fail_matrix_follows_char_spacing_semantics() {
    let no_padding_5 = "a".repeat(5);
    let no_padding_15 = "a".repeat(15);
    let padding_5 = format!("a{}", " ".repeat(4));
    let padding_15 = format!("a{}", " ".repeat(14));
    // (old, new, value, passes) — Go's case table in order.
    let cases = [
        ("CHAR(20)", "CHAR(10)", no_padding_15.as_str(), false),
        ("CHAR(20)", "CHAR(10)", no_padding_5.as_str(), true),
        ("VARCHAR(20)", "VARCHAR(10)", no_padding_15.as_str(), false),
        ("VARCHAR(20)", "VARCHAR(10)", no_padding_5.as_str(), true),
        ("CHAR(20)", "VARCHAR(10)", no_padding_15.as_str(), false),
        ("CHAR(20)", "VARCHAR(10)", no_padding_5.as_str(), true),
        ("VARCHAR(10)", "CHAR(20)", padding_5.as_str(), true),
        ("VARCHAR(10)", "CHAR(20)", no_padding_5.as_str(), true),
        ("VARCHAR(20)", "CHAR(10)", padding_5.as_str(), true),
        ("VARCHAR(20)", "CHAR(10)", padding_15.as_str(), true),
        ("VARCHAR(20)", "CHAR(10)", no_padding_15.as_str(), false),
        ("VARCHAR(20)", "CHAR(10)", no_padding_5.as_str(), true),
    ];
    for (old_type, new_type, value, passes) in cases {
        let mut catalog = Catalog::default();
        run_create_table_on(&format!("CREATE TABLE t (a {old_type})"), &mut catalog).unwrap();
        run_insert_on(
            &format!("INSERT INTO t VALUES ('{value}')"),
            &mut catalog,
            &ctx(),
        )
        .unwrap();
        let outcome = alter(
            &mut catalog,
            &format!("ALTER TABLE t MODIFY COLUMN a {new_type}"),
        );
        assert_eq!(
            outcome.is_ok(),
            passes,
            "Go: {old_type} -> {new_type} with {value:?} must {}",
            if passes { "pass" } else { "fail" }
        );
        if !passes {
            assert!(message_of(&outcome.unwrap_err()).contains("Data truncated for column 'a'"));
        }
    }
}

// Go `modify_column_test.go:389::TestModifyColumnCharset`. Go pins the
// `SHOW CREATE TABLE` text; this tier has no SHOW renderer, so the same
// facts are pinned through the column metadata the text is rendered from:
// both columns start at `CHARACTER SET utf8 COLLATE utf8_bin`; MODIFY
// without a charset moves the column to the table default
// (`utf8mb4`/`utf8mb4_bin`) — Go's final state for `a`.
//
// MEASURED DIVERGENCE (recorded in the receipt): Go then sets the table
// version to `TableInfoVersion0` and modifies `b`, whose charset the
// version-0 rule does NOT recompute — `b` stays utf8_bin ("the behavior is
// not compatible with MySQL", Go's own comment). This tier has no table
// version axis, so `b` also follows the table default. The b-arm assertion
// pins the measured Rust behavior.
#[test]
fn modify_column_charset_columns_follow_the_table_default_when_unset() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE t_mcc (a VARCHAR(8) CHARSET utf8, b VARCHAR(8) CHARSET utf8)",
        &mut catalog,
    )
    .unwrap();
    let (a_charset, a_collation) = column_charset(&catalog, "t_mcc", "a");
    let (b_charset, b_collation) = column_charset(&catalog, "t_mcc", "b");
    assert_eq!(
        (a_charset.as_str(), a_collation.as_str()),
        ("utf8", "utf8_bin")
    );
    assert_eq!(
        (b_charset.as_str(), b_collation.as_str()),
        ("utf8", "utf8_bin")
    );

    alter(&mut catalog, "ALTER TABLE t_mcc MODIFY COLUMN a VARCHAR(8)").unwrap();
    let (a_charset, a_collation) = column_charset(&catalog, "t_mcc", "a");
    assert_eq!(
        (a_charset.as_str(), a_collation.as_str()),
        ("utf8mb4", "utf8mb4_bin"),
        "Go: 'a' ends at the table default (no explicit charset in SHOW CREATE)"
    );

    // Go's version-0 quirk keeps `b` at utf8_bin; this tier moves it too.
    alter(&mut catalog, "ALTER TABLE t_mcc MODIFY COLUMN b VARCHAR(8)").unwrap();
    let (b_charset, b_collation) = column_charset(&catalog, "t_mcc", "b");
    assert_eq!(
        (b_charset.as_str(), b_collation.as_str()),
        ("utf8mb4", "utf8mb4_bin"),
        "divergence: Go's TableInfoVersion0 quirk keeps b at utf8_bin"
    );
}

// Go `modify_column_test.go:607::TestMultiSchemaModifyColumnWithSkipReorg`.
// The observable half without the skip-reorg machinery: a multi-action
// MODIFY that swaps two columns' positions leaves every column at its
// original identity — Go asserts `oldMeta.Columns[1].ID ==
// newMeta.Columns[1].ID` (column `b` keeps its ID through the moves), and
// the same IDs/backing rows are what `admin check table t` verifies there.
#[test]
fn multi_schema_modify_column_positions_keep_column_identity() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE t (a VARCHAR(16), b BIGINT, c BIGINT, INDEX i1(a), INDEX i2(b), INDEX i3(c), INDEX i4(a, b))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO t VALUES ('a  ', 1, 1), ('b  ', 2, 2), ('c ', 3, 3)",
        &mut catalog,
        &ctx(),
    )
    .unwrap();
    let ids_before: Vec<i64> = catalog_column_ids(&catalog, "t");

    alter(
        &mut catalog,
        "ALTER TABLE t MODIFY COLUMN a CHAR(8) AFTER b, MODIFY COLUMN b INT AFTER a",
    )
    .expect("Go: the swapped multi-MODIFY succeeds");

    let ids_after = catalog_column_ids(&catalog, "t");
    assert_eq!(
        ids_before, ids_after,
        "Go: the offset and ID of b are unchanged"
    );
    // The moves land the columns back in their declared order (a after b,
    // then b after a), with `b` — Go's Columns[1] — again at offset 1.
    assert_eq!(catalog_column_names(&catalog, "t"), vec!["a", "b", "c"]);
    // Go's rows keep their values through the type/position changes
    // (`admin check table t` verifies the same rows there).
    assert_eq!(
        text_rows(&catalog, "SELECT * FROM t"),
        vec![["a  ", "1", "1"], ["b  ", "2", "2"], ["c ", "3", "3"]]
    );
}

fn column_flen(catalog: &Catalog, table: &str, column: &str) -> i64 {
    column_of(catalog, table, column).field_type.flen()
}

fn column_code_is_char(catalog: &Catalog, table: &str, column: &str) -> bool {
    column_of(catalog, table, column).field_type.code() == tidb_datatype::FieldTypeCode::String
}

fn column_charset(catalog: &Catalog, table: &str, column: &str) -> (String, String) {
    let column = column_of(catalog, table, column);
    (
        column.field_type.charset_name().to_owned(),
        column.field_type.collation_name().to_owned(),
    )
}

fn column_of(catalog: &Catalog, table: &str, column: &str) -> KvColumn {
    let Some(TableEntry::Kv(table)) = catalog.table_in("test", table) else {
        panic!("table {table} is registered");
    };
    table
        .columns
        .iter()
        .find(|candidate| candidate.name.eq_ignore_ascii_case(column))
        .expect("column exists")
        .clone()
}

fn catalog_column_ids(catalog: &Catalog, table: &str) -> Vec<i64> {
    let Some(TableEntry::Kv(table)) = catalog.table_in("test", table) else {
        panic!("table {table} is registered");
    };
    table.columns.iter().map(|column| column.id).collect()
}

fn catalog_column_names(catalog: &Catalog, table: &str) -> Vec<String> {
    let Some(TableEntry::Kv(table)) = catalog.table_in("test", table) else {
        panic!("table {table} is registered");
    };
    table
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect()
}
