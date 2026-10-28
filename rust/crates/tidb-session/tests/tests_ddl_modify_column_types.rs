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

//! Ports of the `pkg/ddl/modify_column_test.go` tests whose observable
//! contract is the final state of a MODIFY/CHANGE COLUMN job: the admission
//! checks (Go `pkg/ddl/column.go` `checkForNullValue`, `pkg/ddl/modify_column.go`)
//! and the per-row column rewrite (`updateColumnWorker`). Contracts requiring
//! the online-DDL job machinery (reorg state machine, failpoint hooks, and
//! region splits) remain unimplemented and are not represented as Rust tests.
//!
//! Where Go's observable outcome differs, the assertion pins the measured
//! Rust behavior and the comment cites Go's expectation. Such divergence
//! cases are not Go-parity evidence.

use crate::support::testkit::TestKit;
use tidb_executor::{KvColumn, TableEntry};

// Go `modify_column_test.go::TestModifyColumnBetweenStringTypes`: same-family
// shrinks succeed and keep values, shrinking below an existing value answers
// Go's `types.ErrTruncated` 1265 with the offending value in the message,
// `binary` keeps its zero padding, and the char/varchar/text/set/enum
// conversions re-map values by member name -- including the numeric-filter
// arms `where a = 1/2` over a reordered enum -- then the non-strict arm
// downgrades the refusal to a warning.
#[test]
fn modify_column_between_string_types_shrinks_expands_and_remaps_enum_set_members() {
    let mut tk = TestKit::new();

    // varchar to varchar
    tk.must_exec("CREATE TABLE tt (a VARCHAR(10))");
    tk.must_exec("INSERT INTO tt VALUES ('111'),('10000')");
    tk.must_exec("ALTER TABLE tt CHANGE a a VARCHAR(5)");
    assert_eq!(column_flen(&tk, "tt", "a"), 5, "Go: GetFlen == 5");
    tk.must_query("SELECT * FROM tt").check(["111", "10000"]);
    assert_eq!(
        tk.must_get_err_code("ALTER TABLE tt CHANGE a a VARCHAR(4)", 1265).message,
        "Data truncated for column 'a', value is '10000'"
    );
    tk.must_exec("ALTER TABLE tt CHANGE a a VARCHAR(100)");
    tk.must_query("SELECT length(a) FROM tt").check(["3", "5"]);

    // char to char
    tk.must_exec("DROP TABLE IF EXISTS tt");
    tk.must_exec("CREATE TABLE tt (a CHAR(10))");
    tk.must_exec("INSERT INTO tt VALUES ('111'),('10000')");
    tk.must_exec("ALTER TABLE tt CHANGE a a CHAR(5)");
    assert_eq!(column_flen(&tk, "tt", "a"), 5);
    tk.must_query("SELECT * FROM tt").check(["111", "10000"]);
    assert_eq!(
        tk.must_get_err_code("ALTER TABLE tt CHANGE a a CHAR(4)", 1265).message,
        "Data truncated for column 'a', value is '10000'"
    );
    tk.must_exec("ALTER TABLE tt CHANGE a a CHAR(100)");
    tk.must_query("SELECT length(a) FROM tt").check(["3", "5"]);

    // binary to binary: the stored values are zero-padded, so ANY shrink is
    // refused with the padded value in the message, and a widen re-pads.
    tk.must_exec("DROP TABLE IF EXISTS tt");
    tk.must_exec("CREATE TABLE tt (a BINARY(10))");
    tk.must_exec("INSERT INTO tt VALUES ('111'),('10000')");
    assert_eq!(
        tk.must_get_err_code("ALTER TABLE tt CHANGE a a BINARY(5)", 1265).message,
        "Data truncated for column 'a', value is '111\0\0\0\0\0\0\0'"
    );
    assert_eq!(column_flen(&tk, "tt", "a"), 10, "Go: the type is unchanged");
    tk.must_query("SELECT * FROM tt").check(["111\0\0\0\0\0\0\0", "10000\0\0\0\0\0"]);
    tk.must_get_err_code("ALTER TABLE tt CHANGE a a BINARY(4)", 1265);
    tk.must_exec("ALTER TABLE tt CHANGE a a BINARY(12)");
    tk.must_query("SELECT * FROM tt").check(["111\0\0\0\0\0\0\0\0\0", "10000\0\0\0\0\0\0\0"]);
    tk.must_query("SELECT length(a) FROM tt").check(["12", "12"]);

    // varbinary to varbinary
    tk.must_exec("DROP TABLE IF EXISTS tt");
    tk.must_exec("CREATE TABLE tt (a VARBINARY(10))");
    tk.must_exec("INSERT INTO tt VALUES ('111'),('10000')");
    tk.must_exec("ALTER TABLE tt CHANGE a a VARBINARY(5)");
    assert_eq!(column_flen(&tk, "tt", "a"), 5);
    tk.must_query("SELECT * FROM tt").check(["111", "10000"]);
    tk.must_get_err_code("ALTER TABLE tt CHANGE a a VARBINARY(4)", 1265);
    tk.must_exec("ALTER TABLE tt CHANGE a a VARBINARY(12)");
    tk.must_query("SELECT * FROM tt").check(["111", "10000"]);
    tk.must_query("SELECT length(a) FROM tt").check(["3", "5"]);

    // varchar to char
    tk.must_exec("DROP TABLE IF EXISTS tt");
    tk.must_exec("CREATE TABLE tt (a VARCHAR(10))");
    tk.must_exec("INSERT INTO tt VALUES ('111'),('10000')");
    tk.must_exec("ALTER TABLE tt CHANGE a a CHAR(10)");
    assert!(
        column_code_is_char(&tk, "tt", "a"),
        "Go: GetType() == mysql.TypeString"
    );
    assert_eq!(column_flen(&tk, "tt", "a"), 10);
    tk.must_query("SELECT * FROM tt").check(["111", "10000"]);
    tk.must_get_err_code("ALTER TABLE tt CHANGE a a CHAR(4)", 1265);

    // char to text
    tk.must_exec("ALTER TABLE tt CHANGE a a TEXT");

    // text to set: a member list that misses an existing value is refused,
    // one that covers it re-maps by name.
    tk.must_get_err_code("ALTER TABLE tt CHANGE a a SET('111', '2222')", 1265);
    tk.must_exec("ALTER TABLE tt CHANGE a a SET('111', '10000')");
    tk.must_query("SELECT * FROM tt").check(["111", "10000"]);

    // set to set
    tk.must_exec("ALTER TABLE tt CHANGE a a SET('10000', '111')");
    tk.must_query("SELECT * FROM tt").check(["111", "10000"]);

    // set to enum
    tk.must_get_err_code("ALTER TABLE tt CHANGE a a ENUM('111', '2222')", 1265);
    tk.must_exec("ALTER TABLE tt CHANGE a a ENUM('111', '10000')");
    tk.must_query("SELECT * FROM tt").check(["111", "10000"]);
    tk.must_exec("ALTER TABLE tt CHANGE a a ENUM('10000', '111')");
    // Go: the numeric filter addresses the enum INDEX, so the re-map moves
    // values.
    tk.must_query("SELECT * FROM tt WHERE a = 1").check(["10000"]);
    tk.must_query("SELECT * FROM tt WHERE a = 2").check(["111"]);

    // no-strict mode: the refusal becomes a warning.
    tk.must_exec("set @@sql_mode=''");
    tk.must_exec("alter table tt change a a enum('111', '2222')");
    tk.must_query("show warnings").check_with_sep(
        "|",
        ["Warning|1265|Data truncated for column 'a', value is '10000'"],
    );
}

// Go `pkg/ddl/column_modify_test.go::TestChangeColumn` pins the admission
// half: an existing NULL refuses `... not null` with
// "[ddl:1265]Data truncated for column 'a1' at row 1" (`checkForNullValue`
// with `isDataTruncated`, called from `pkg/ddl/modify_column.go`
// `GetModifiableColumnJob`, independent of the sql_mode).
//
// GAP: Go `modify_column_test.go::TestModifyColumnNullToNotNullWithChangingVal2`
// inserts the NULL AFTER admission, from the
// `beforeDoModifyColumnSkipReorgCheck` failpoint, so the job worker's own
// check answers "[ddl:1138]Invalid use of NULL value". The Rust job path has
// no such hook yet, so the 1138 arm is not ported.
#[test]
fn modify_column_null_to_not_null_rejects_rows_holding_nulls() {
    let mut tk = TestKit::new();
    tk.must_exec("CREATE TABLE tt (a BIGINT, b INT)");
    tk.must_exec("INSERT INTO tt VALUES (1,1),(2,2),(3,3)");
    tk.must_exec("INSERT INTO tt VALUES (NULL, NULL)");

    assert_eq!(
        tk.must_get_err_code("ALTER TABLE tt MODIFY a INT NOT NULL", 1265).message,
        "Data truncated for column 'a' at row 1"
    );
    // The rows survive the refused alteration.
    tk.must_query("SELECT * FROM tt").check(["1 1", "2 2", "3 3", "<nil> <nil>"]);
}

// Go `modify_column_test.go::TestModifyIntegerColumn`, value halves.
// Go runs the full signed/unsigned matrix with boundary values; the
// `expectedReorgTp` halves (captured through the `getModifyColumnType`
// failpoint) remain a separate coverage obligation. Pinned here: every
// out-of-new-range boundary value is refused with Go's
// "Data truncated for column 'a'" (or overflow) message, every in-range
// boundary set converts cleanly, for signed->signed, signed->unsigned,
// unsigned->unsigned, and unsigned->signed narrowing chains.
#[test]
fn modify_integer_column_boundary_values_are_refused_or_converted_exactly() {
    // Go `failedValue`: the error mentions truncation or overflow.
    #[track_caller]
    fn failed_value(tk: &mut TestKit, sql: &str) {
        let message = tk.must_exec_to_err(sql).message;
        assert!(
            message.contains("Data truncated for column 'a'") || message.contains("overflow"),
            "Go: err must mention truncation or overflow, got {message}"
        );
    }

    // signed -> signed (Go's signed2Signed): [max(new)+1, max(old)] fail;
    // [min(new)-1, min(old)] fail; [max(new), min(new), 0] pass.
    for value in [
        2147483648_i64,
        9223372036854775807,
        -2147483649,
        -9223372036854775808,
    ] {
        let mut tk = TestKit::new();
        tk.must_exec("CREATE TABLE t (a BIGINT)");
        tk.must_exec(&format!("INSERT INTO t VALUES ({value})"));
        failed_value(&mut tk, "ALTER TABLE t MODIFY COLUMN a INT");
    }

    // In-range boundaries pass and keep their values.
    let mut tk = TestKit::new();
    tk.must_exec("CREATE TABLE t (a INT)");
    tk.must_exec("INSERT INTO t VALUES (2147483647), (-2147483648), (0)");
    tk.must_exec("ALTER TABLE t MODIFY COLUMN a BIGINT");
    tk.must_query("SELECT * FROM t").check(["2147483647", "-2147483648", "0"]);

    // signed -> unsigned: the negative half of the old range is refused
    // ([minValOfOldCol, -1] fail).
    let mut tk = TestKit::new();
    tk.must_exec("CREATE TABLE t (a INT)");
    tk.must_exec("INSERT INTO t VALUES (-1)");
    failed_value(&mut tk, "ALTER TABLE t MODIFY COLUMN a INT UNSIGNED");

    // unsigned -> unsigned: values above the new maximum are refused
    // ([maxValOfNewCol+1, maxValOfOldCol] fail), the new maximum itself
    // passes ([0, maxValOfNewCol] pass).
    let mut tk = TestKit::new();
    tk.must_exec("CREATE TABLE t (a BIGINT UNSIGNED)");
    tk.must_exec("INSERT INTO t VALUES (65536)");
    failed_value(&mut tk, "ALTER TABLE t MODIFY COLUMN a SMALLINT UNSIGNED");
    let mut tk = TestKit::new();
    tk.must_exec("CREATE TABLE t (a BIGINT UNSIGNED)");
    tk.must_exec("INSERT INTO t VALUES (65535), (0), (1)");
    tk.must_exec("ALTER TABLE t MODIFY COLUMN a SMALLINT UNSIGNED");

    // unsigned -> signed: above the new signed maximum is refused, at it
    // passes.
    let mut tk = TestKit::new();
    tk.must_exec("CREATE TABLE t (a BIGINT UNSIGNED)");
    tk.must_exec("INSERT INTO t VALUES (128), (1), (0)");
    failed_value(&mut tk, "ALTER TABLE t MODIFY COLUMN a TINYINT");
    let mut tk = TestKit::new();
    tk.must_exec("CREATE TABLE t (a BIGINT UNSIGNED)");
    tk.must_exec("INSERT INTO t VALUES (127), (1), (0)");
    tk.must_exec("ALTER TABLE t MODIFY COLUMN a TINYINT");
}

// Go `modify_column_test.go::TestModifyStringColumn`, pass/fail halves.
// Go's `expectedReorgTp` captures (the `getModifyColumnType` failpoint) are
// a separate coverage obligation. Pinned here: each of Go's twelve
// (old type, new type, value) cases refuses with "Data truncated for
// column 'a'" or converts cleanly -- including the CHAR trailing-space
// semantics that let a 15-character mostly-spaces value fit `char(10)`.
#[test]
fn modify_string_column_pass_fail_matrix_follows_char_spacing_semantics() {
    let no_padding_5 = "a".repeat(5);
    let no_padding_15 = "a".repeat(15);
    let padding_5 = format!("a{}", " ".repeat(4));
    let padding_15 = format!("a{}", " ".repeat(14));
    // (old, new, value, passes) -- Go's case table in order.
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
    let mut tk = TestKit::new();
    for (old_type, new_type, value, passes) in cases {
        tk.must_exec("DROP TABLE IF EXISTS t");
        tk.must_exec(&format!("CREATE TABLE t (a {old_type})"));
        tk.must_exec(&format!("INSERT INTO t VALUES ('{value}')"));
        let outcome = tk.exec(&format!("ALTER TABLE t MODIFY COLUMN a {new_type}"));
        assert_eq!(
            outcome.is_ok(),
            passes,
            "Go: {old_type} -> {new_type} with {value:?} must {}",
            if passes { "pass" } else { "fail" }
        );
        if let Err(error) = outcome {
            assert!(error.message.contains("Data truncated for column 'a'"));
        }
    }
}

// Go `modify_column_test.go::TestModifyColumnCharset`. Go pins the
// `SHOW CREATE TABLE` text; the same facts are pinned here through the
// column metadata (`external.GetTableByName`) the text is rendered from:
// both columns start at `CHARACTER SET utf8 COLLATE utf8_bin`; MODIFY
// without a charset moves the column to the table default
// (`utf8mb4`/`utf8mb4_bin`) -- Go's final state for `a`.
//
// MEASURED DIVERGENCE: Go then sets the table version to
// `TableInfoVersion0` and modifies `b`, whose charset the version-0 rule
// does NOT recompute -- `b` stays utf8_bin ("the behavior is not compatible
// with MySQL", Go's own comment). Go mutates its in-memory `TableInfo` for
// that; this port has no such handle, so `b` also follows the table default.
// The b-arm assertion pins the measured Rust behavior.
#[test]
fn modify_column_charset_columns_follow_the_table_default_when_unset() {
    let mut tk = TestKit::new();
    tk.must_exec("CREATE TABLE t_mcc (a VARCHAR(8) CHARSET utf8, b VARCHAR(8) CHARSET utf8)");
    assert_eq!(column_charset(&tk, "t_mcc", "a"), ("utf8".to_owned(), "utf8_bin".to_owned()));
    assert_eq!(column_charset(&tk, "t_mcc", "b"), ("utf8".to_owned(), "utf8_bin".to_owned()));

    tk.must_exec("ALTER TABLE t_mcc MODIFY COLUMN a VARCHAR(8)");
    assert_eq!(
        column_charset(&tk, "t_mcc", "a"),
        ("utf8mb4".to_owned(), "utf8mb4_bin".to_owned()),
        "Go: 'a' ends at the table default (no explicit charset in SHOW CREATE)"
    );

    // Go's version-0 quirk keeps `b` at utf8_bin; this port moves it too.
    tk.must_exec("ALTER TABLE t_mcc MODIFY COLUMN b VARCHAR(8)");
    assert_eq!(
        column_charset(&tk, "t_mcc", "b"),
        ("utf8mb4".to_owned(), "utf8mb4_bin".to_owned()),
        "divergence: Go's TableInfoVersion0 quirk keeps b at utf8_bin"
    );
}

// Go `modify_column_test.go::TestMultiSchemaModifyColumnWithSkipReorg`:
// `b` keeps its ID through the swapped positions (`oldMeta.Columns[1].ID ==
// newMeta.Columns[1].ID`). The VARCHAR -> CHAR change of `a` is Go's
// `precheckForVarcharToChar`: trailing spaces in the data fail the check and
// fall back to a reorg under a fresh column ID; without them `a` keeps its ID
// too (gorun, read from `mysql.stats_histograms` after ANALYZE: ids 2, 3, 4
// with the trailing spaces, 1, 2, 3 without).
#[test]
fn multi_schema_modify_column_positions_keep_column_identity() {
    for (rows, ids, values) in [
        (
            "('a  ', 1, 1), ('b  ', 2, 2), ('c ', 3, 3)",
            vec![4, 2, 3],
            ["a 1 1", "b 2 2", "c 3 3"],
        ),
        (
            "('a', 1, 1), ('b', 2, 2), ('c', 3, 3)",
            vec![1, 2, 3],
            ["a 1 1", "b 2 2", "c 3 3"],
        ),
    ] {
        let mut tk = TestKit::new();
        tk.must_exec(
            "CREATE TABLE t (a VARCHAR(16), b BIGINT, c BIGINT, INDEX i1(a), INDEX i2(b), INDEX i3(c), INDEX i4(a, b))",
        );
        tk.must_exec(&format!("INSERT INTO t VALUES {rows}"));
        assert_eq!(column_ids(&tk, "t"), vec![1, 2, 3]);

        tk.must_exec("ALTER TABLE t MODIFY COLUMN a CHAR(8) AFTER b, MODIFY COLUMN b INT AFTER a");
        tk.must_exec("admin check table t");

        assert_eq!(column_ids(&tk, "t"), ids, "{rows}");
        // The moves land the columns back in their declared order (a after
        // b, then b after a), with `b` -- Go's Columns[1] -- again at offset 1.
        assert_eq!(column_names(&tk, "t"), vec!["a", "b", "c"]);
        // A non-binary CHAR reads without its trailing spaces.
        tk.must_query("SELECT * FROM t").check(values);
    }
}

/// Go `checkModifyTypes` refuses a clustered-handle type change once the
/// integer family itself changes, even when both the origin and target are
/// integer types.  Display-width-only edits keep the same type code and are
/// still accepted; the reorganization guard is specifically for the value
/// domain/encoding change (`int` -> `mediumint` here).
#[test]
fn clustered_handle_integer_type_changes_are_refused() {
    let mut tk = TestKit::new();
    tk.must_exec("CREATE TABLE t (id INT PRIMARY KEY, v INT)");

    // Go: [ddl:8200]Unsupported modify column: this column has primary key flag
    assert_eq!(
        tk.must_get_err_code("ALTER TABLE t MODIFY COLUMN id MEDIUMINT", 8200).message,
        "Unsupported modify column: this column has primary key flag"
    );

    // The metadata-only display-width spelling remains valid, matching Go's
    // integer normalization in needReorgToChange.
    tk.must_exec("ALTER TABLE t MODIFY COLUMN id INT(5)");
    assert_eq!(column_flen(&tk, "t", "id"), 5);
}

/// Go `external.GetModifyColumn(t, tk, "test", table, column, false)`.
fn column_of(tk: &TestKit, table: &str, column: &str) -> KvColumn {
    let TableEntry::Kv(table) = tk.get_table_by_name("test", table) else {
        panic!("table {table} is not a KV table");
    };
    table
        .columns()
        .iter()
        .find(|candidate| candidate.name.eq_ignore_ascii_case(column))
        .expect("column exists")
        .clone()
}

fn column_flen(tk: &TestKit, table: &str, column: &str) -> i64 {
    column_of(tk, table, column).field_type.flen()
}

fn column_code_is_char(tk: &TestKit, table: &str, column: &str) -> bool {
    column_of(tk, table, column).field_type.code() == tidb_datatype::FieldTypeCode::String
}

fn column_charset(tk: &TestKit, table: &str, column: &str) -> (String, String) {
    let column = column_of(tk, table, column);
    (
        column.field_type.charset_name().to_owned(),
        column.field_type.collation_name().to_owned(),
    )
}

/// Go `external.GetTableByName(t, tk, "test", table).Meta().Columns`' IDs.
fn column_ids(tk: &TestKit, table: &str) -> Vec<i64> {
    let TableEntry::Kv(table) = tk.get_table_by_name("test", table) else {
        panic!("table {table} is not a KV table");
    };
    table.columns().iter().map(|column| column.id).collect()
}

fn column_names(tk: &TestKit, table: &str) -> Vec<String> {
    let TableEntry::Kv(table) = tk.get_table_by_name("test", table) else {
        panic!("table {table} is not a KV table");
    };
    table.columns().iter().map(|column| column.name.clone()).collect()
}
