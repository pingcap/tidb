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
use tidb_infoschema::TableEntry;

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
fn column_of(tk: &TestKit, table: &str, column: &str) -> tidb_table::column::Column {
    let TableEntry::Kv(table) = tk.get_table_by_name("test", table) else {
        panic!("table {table} is not a KV table");
    };
    table
        .columns()
        .iter()
        .find(|candidate| candidate.read().name.original().eq_ignore_ascii_case(column))
        .expect("column exists")
        .clone()
}

fn column_flen(tk: &TestKit, table: &str, column: &str) -> i64 {
    column_of(tk, table, column).read().field_type.flen()
}

fn column_code_is_char(tk: &TestKit, table: &str, column: &str) -> bool {
    column_of(tk, table, column).read().field_type.code() == tidb_datatype::FieldTypeCode::String
}

fn column_charset(tk: &TestKit, table: &str, column: &str) -> (String, String) {
    let column = column_of(tk, table, column);
    let column = column.read();
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
    table.columns().iter().map(|column| column.read().id).collect()
}

fn column_names(tk: &TestKit, table: &str) -> Vec<String> {
    let TableEntry::Kv(table) = tk.get_table_by_name("test", table) else {
        panic!("table {table} is not a KV table");
    };
    table.columns().iter().map(|column| column.read().name.original().to_owned()).collect()
}

/// Go `TestModifyColumnCrossCharsetRequiresReorg` (`pkg/ddl/column_modify_test.go`):
/// a charset/collation change outside the direct-convertible set
/// (`canModifyCharsetAndCollateWithoutReorg`) rewrites the rows, so invalid
/// data is refused; a same-charset collation change checks the character
/// length, not the byte length; utf8 -> utf8mb4 stays a metadata change.
#[test]
fn modify_column_cross_charset_requires_reorg() {
    const ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD: u16 = 1366;
    const WARN_DATA_TRUNCATED: u16 = 1265;
    let mut tk = TestKit::new();
    tk.must_exec("use test");

    tk.must_exec("create table t0 (id int primary key, c binary(2))");
    tk.must_exec("insert into t0 values (1, x'FFFE')");
    tk.must_get_err_code(
        "alter table t0 modify column c varchar(10) character set utf8mb4 collate utf8mb4_general_ci",
        ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD,
    );

    tk.must_exec("create table t2 (id int primary key, c char(4) character set utf8mb4)");
    tk.must_exec("insert into t2 values (1, 'é')");
    tk.must_get_err_code("alter table t2 modify column c varchar(10) character set ascii", ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD);

    tk.must_exec("create table t3 (id int primary key, c char(4) character set utf8mb4)");
    tk.must_exec("insert into t3 values (1, 'éééé')");
    tk.must_get_err_code("alter table t3 modify column c varbinary(4)", WARN_DATA_TRUNCATED);

    tk.must_exec("create table t4 (id int primary key, c varchar(4) character set utf8mb4 collate utf8mb4_bin)");
    tk.must_exec("insert into t4 values (1, 'éééé')");
    tk.must_exec("alter table t4 modify column c varchar(4) character set utf8mb4 collate utf8mb4_general_ci");

    tk.must_exec("create table t5 (id int primary key, c varchar(10) character set utf8 collate utf8_bin, key idx_c(c))");
    tk.must_exec("insert into t5 values (1, 'abc')");
    tk.must_exec("alter table t5 modify column c varchar(10) character set utf8mb4 collate utf8mb4_bin");
    tk.must_query("select row_count from information_schema.ddl_jobs where table_name = 't5' order by job_id desc limit 1")
        .check(["0"]);
    tk.must_exec("alter table t5 modify column c varchar(5) character set utf8mb4 collate utf8mb4_bin");
    tk.must_query("select row_count from information_schema.ddl_jobs where table_name = 't5' order by job_id desc limit 1")
        .check(["0"]);
    tk.must_exec("admin check table t5");

    // Both length-preserving and narrowing conversions reject four-byte
    // UTF-8 data during reorg (Go master 307fb3d6ead).
    tk.must_exec("create table t6 (id int primary key, c varchar(10) character set utf8mb4 collate utf8mb4_bin)");
    tk.must_exec("insert into t6 values (1, '🐱')");
    tk.must_get_err_code(
        "alter table t6 modify column c varchar(10) character set utf8 collate utf8_bin",
        ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD,
    );
    tk.must_get_err_code(
        "alter table t6 modify column c varchar(5) character set utf8 collate utf8_bin",
        ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD,
    );
    tk.must_exec("update t6 set c = 'é'");
    tk.must_exec("alter table t6 modify column c varchar(10) character set utf8 collate utf8_bin");
    tk.must_query("select c from t6").check(["é"]);
}

/// Go master `TestModifyColumnCharsetClassification`
/// (`pkg/ddl/column_modify_test.go`, 307fb3d6ead): a charset change that
/// needs a rewrite refuses invalid data and leaves the rows as they were; a
/// character limit that cannot guarantee a byte limit is validated, not
/// rewritten; utf8 -> utf8mb4, an ENUM/SET charset change, TEXT -> BLOB and
/// an indexed collation change all succeed.
#[test]
fn test_modify_column_charset_classification() {
    const WARN_DATA_TRUNCATED: u16 = 1265;
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("set sql_mode = 'STRICT_ALL_TABLES'");
    for (name, old_type, new_type, value) in [
        ("binary", "binary(2)", "varchar(10) character set utf8mb4", "x'FFFE'"),
        ("utf8", "char(4) character set utf8mb4 collate utf8mb4_bin", "varchar(10) character set utf8 collate utf8_bin", "'😀'"),
        ("ascii", "char(4) character set utf8mb4", "varchar(10) character set ascii", "'é'"),
        ("bytes", "char(4) character set utf8mb4", "varbinary(4)", "'éééé'"),
    ] {
        tk.must_exec("drop table if exists t");
        tk.must_exec(&format!("create table t (c {old_type})"));
        tk.must_exec(&format!("insert into t values ({value})"));
        let before = tk.must_query("select hex(c) from t").rows().to_vec();
        tk.must_exec_to_err(&format!("alter table t modify c {new_type}"));
        assert_eq!(tk.must_query("select hex(c) from t").rows().to_vec(), before, "{name}");
    }

    // textByteLimit
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (c varchar(255) collate utf8mb4_bin)");
    tk.must_exec("insert into t values (repeat(convert(x'C385' using utf8mb4), 128))");
    tk.must_get_err_code("alter table t modify c tinytext collate utf8mb4_bin", WARN_DATA_TRUNCATED);
    tk.must_query("select data_type from information_schema.columns where table_schema = 'test' and table_name = 't'")
        .check(["varchar"]);
    tk.must_query("select char_length(c), length(c) from t").check(["128 256"]);
    let column_id = column_ids(&tk, "t")[0];
    tk.must_exec("delete from t");
    tk.must_exec("insert into t values (concat(repeat(convert(x'C385' using utf8mb4), 127), 'x'))");
    tk.must_exec("alter table t modify c tinytext collate utf8mb4_bin");
    tk.must_query("select length(c) from t").check(["255"]);
    assert_eq!(column_ids(&tk, "t")[0], column_id);

    tk.must_exec("drop table t");
    tk.must_exec("create table t (c varchar(5) character set utf8 collate utf8_bin primary key)");
    tk.must_exec("insert into t values ('éééé')");
    tk.must_exec("alter table t modify c varchar(5) character set utf8mb4 collate utf8mb4_bin");
    tk.must_exec("drop table t");
    tk.must_exec("create table t (c varchar(5) character set utf8mb4 collate utf8mb4_bin)");
    tk.must_exec("insert into t values ('éééé')");
    tk.must_exec("alter table t modify c varchar(4) character set utf8mb4 collate utf8mb4_bin");
    tk.must_query("select c from t").check(["éééé"]);
    tk.must_exec("admin check table t");
    tk.must_exec("drop table t");
    tk.must_exec("create table t (c set('', 'a') character set utf8 collate utf8_bin)");
    tk.must_exec("insert into t values (1)");
    tk.must_exec("alter table t modify c set('', 'a') character set utf8mb4 collate utf8mb4_bin");
    tk.must_query("select c+0 from t").check(["1"]);
    tk.must_exec("drop table t");
    tk.must_exec("create table t (c text character set utf8mb4)");
    tk.must_exec("insert into t values ('hello')");
    tk.must_exec("alter table t modify c blob");
    tk.must_query("select cast(c as char) from t").check(["hello"]);
    tk.must_exec("admin check table t");
    tk.must_exec("drop table t");
    tk.must_exec("create table t (c varchar(10) collate utf8_bin, index idx(c))");
    tk.must_exec("insert into t values ('a'), ('B')");
    tk.must_exec("alter table t modify c varchar(10) collate utf8_general_ci");
    tk.must_query("select c from t where c = 'A'").check(["a"]);
    tk.must_exec("admin check table t");
    // Go's `SetNewCollationEnabledForTest(false)` tail has no counterpart:
    // this port always runs the new collation framework.
}

/// Go master `TestModifyColumnPreserveOriginDefault`
/// (`pkg/ddl/modify_column_test.go`, 307fb3d6ead): a MODIFY that keeps the
/// row storage keeps the column's origin default, so rows written before
/// the column existed still read it; on an empty table an origin default
/// the narrowed type cannot hold is cleared.
#[test]
fn test_modify_column_preserve_origin_default() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");

    // varcharToTinyTextValidationOnly
    tk.must_exec("create table t (id int)");
    tk.must_exec("insert into t values (1)");
    tk.must_exec("alter table t add column c varchar(255) default 'saved'");
    let column_id = column_ids(&tk, "t")[1];
    tk.must_exec("alter table t modify column c tinytext");
    assert_eq!(column_ids(&tk, "t")[1], column_id);
    tk.must_exec("insert into t (id) values (2)");
    tk.must_query("select id, c from t order by id").check(["1 saved", "2 <nil>"]);
    tk.must_exec("admin check table t");

    // varcharNarrowingKeepsNullAndValueOriginDefault
    tk.must_exec("drop table t");
    tk.must_exec("create table t (id int)");
    tk.must_exec("insert into t values (1)");
    tk.must_exec("alter table t add column c1 varchar(255) default 'saved'");
    tk.must_exec("alter table t add column c2 varchar(255)");
    tk.must_exec("alter table t modify column c1 varchar(100)");
    tk.must_exec("alter table t modify column c2 varchar(100) default 'new'");
    tk.must_exec("insert into t (id) values (2)");
    tk.must_query("select id, c1, c2 from t order by id").check(["1 saved <nil>", "2 <nil> new"]);
    tk.must_exec("admin check table t");

    // indexReorgPreservesOriginDefault
    tk.must_exec("drop table t");
    tk.must_exec("create table t (id int)");
    tk.must_exec("insert into t values (1)");
    tk.must_exec("alter table t add column c char(10) collate utf8mb4_bin default 'saved'");
    tk.must_exec("alter table t add index idx(c)");
    tk.must_exec("alter table t modify column c varchar(20) collate utf8mb4_bin");
    tk.must_exec("insert into t (id) values (2)");
    tk.must_query("select id, c from t order by id").check(["1 saved", "2 <nil>"]);
    tk.must_exec("admin check table t");

    // emptyTableClearsOutOfRangeOriginDefault
    tk.must_exec("drop table t");
    tk.must_exec("create table t (id int)");
    tk.must_exec("alter table t add column c int default 1000");
    tk.must_exec("alter table t modify column c tinyint");
    assert!(column_of(&tk, "t", "c").read().get_origin_default_value().is_nil());
    tk.must_query("select * from t").check(Vec::<&str>::new());
    tk.must_query("show warnings").check(Vec::<&str>::new());
}

/// Go master `TestModifyColumnCheckQuotedIdentifiers`
/// (`pkg/ddl/modify_column_test.go`, 307fb3d6ead): the data check of a
/// validation-only MODIFY quotes identifiers holding backquotes.
#[test]
fn test_modify_column_check_quoted_identifiers() {
    let mut tk = TestKit::new();
    tk.must_exec("drop database if exists `db``1`");
    tk.must_exec("create database `db``1`");
    tk.must_exec("use `db``1`");

    // varcharToTinyText
    tk.must_exec("create table `t``1` (`c``1` varchar(255) charset utf8mb4)");
    tk.must_exec("insert into `t``1` values ('ok')");
    tk.must_exec("alter table `t``1` modify column `c``1` tinytext charset utf8mb4");
    tk.must_query("select `c``1` from `t``1`").check(["ok"]);
    tk.must_exec("admin check table `t``1`");
    // Values exceeding TINYTEXT's 255-byte limit are refused on quoted
    // identifiers too.
    tk.must_exec("create table `t``2` (`c``2` varchar(255) charset utf8mb4)");
    tk.must_exec(&format!("insert into `t``2` values ('{}')", "é".repeat(128)));
    let error = tk.must_exec_to_err("alter table `t``2` modify column `c``2` tinytext charset utf8mb4");
    assert!(error.message.contains("Data truncated"), "{}", error.message);

    // integerNarrowingAndNotNull
    tk.must_exec("create table `t``int` (`c``int` int)");
    tk.must_exec("insert into `t``int` values (42)");
    tk.must_exec("alter table `t``int` modify column `c``int` tinyint not null");
    tk.must_query("select `c``int` from `t``int`").check(["42"]);
    tk.must_exec("admin check table `t``int`");
    tk.must_exec("drop database if exists `db``1`");
}

/// Go master `TestModifyColumnReorgScheduling`'s metadata cases
/// (`pkg/ddl/modify_column_test.go`, 307fb3d6ead): MODIFY COLUMN reserves
/// reorg scheduling at submission and defers the distributed-reorg check to
/// its index reorg, so a metadata-only change succeeds with distributed
/// tasks on and fast reorg off, alone or in a multi-schema change.
#[test]
fn test_modify_column_reorg_scheduling_metadata_without_fast_reorg() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("set global tidb_enable_dist_task = on");
    tk.must_exec("set global tidb_ddl_enable_fast_reorg = off");
    for (sql, row) in [
        ("alter table t modify column a decimal(10,2) comment 'new'", "1.23"),
        ("alter table t modify column a decimal(10,2) comment 'new', add column b int", "1.23 <nil>"),
    ] {
        tk.must_exec("drop table if exists t");
        tk.must_exec("create table t (a decimal(10,2), key(a))");
        tk.must_exec("insert into t values (1.23)");
        tk.must_exec(sql);
        tk.must_query("select * from t").check([row]);
        tk.must_exec("admin check table t");
    }
    tk.must_exec("set global tidb_ddl_enable_fast_reorg = on");
    tk.must_exec("set global tidb_enable_dist_task = off");
}
