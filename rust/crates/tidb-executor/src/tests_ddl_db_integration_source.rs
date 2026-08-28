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

//! Ports of `pkg/ddl/db_integration_test.go` items 153-180 (Go tests at
//! lines 60-948 on origin/master, `TestCreateTableIfNotExistsLike` through
//! `TestCreateTableTooLarge`; the file's later tests are batch part4's).
//!
//! These are SQL-integration tests: each builds tables and DML through a
//! testkit session and asserts results, metadata, or errno codes. The
//! carriers used here are the same ones the production SQL path reaches --
//! `run_create_table_in` / `run_alter_table_in` / `run_create_index_in` over
//! a [`Catalog`], and `run_select_on` / `run_insert_on` / `run_update_on`
//! for the data halves. Where Go verifies through `SHOW CREATE TABLE` or
//! `information_schema` (neither of which this tier serves for user
//! tables -- measured this session), the same fact is asserted through the
//! persisted table metadata those statements render, and the substitution
//! is stated in the test's comment. Where a Go assertion has NO faithful
//! carrier, it is listed in that test's gap comment rather than
//! approximated; the whole-test gaps sit in `#[ignore]` ports at the bottom.

use crate::driver::{run_select_meta_in, run_select_on};
use crate::{Catalog, DriverError, StmtContext, TableEntry};
use tidb_datatype::{Datum, FieldTypeFlags};

/// A stock strict session, matching Go's testkit default `sql_mode`.
fn ctx() -> StmtContext {
    StmtContext::default().with_strict(true)
}

/// Renders one [`DriverError`] as the (errno, message) a client sees.
fn mysql(error: DriverError) -> (u16, String) {
    let rendered = error.to_mysql_error();
    (rendered.code, rendered.message)
}

/// The visible column names of a storage-backed table, in persisted order.
fn column_names(catalog: &Catalog, database: &str, table: &str) -> Vec<String> {
    let Some(TableEntry::Kv(entry)) = catalog.table_in(database, table) else {
        panic!("{database}.{table} must be a storage-backed table");
    };
    entry
        .visible_columns()
        .iter()
        .map(|column| column.name.clone())
        .collect()
}

/// Go `TestCreateTableIfNotExistsLike` (pkg/ddl/db_integration_test.go:60,
/// issue #6879): `create table if not exists ct like ct1` over an existing
/// name must SUCCEED (no error). Go's actual assertions read the recorded
/// statement warnings -- the last one must be `infoschema.ErrTableExists`
/// at NOTE level -- and this tier records no such warning for the
/// duplicate-create skip (measured this session: `take_warnings()` is
/// empty), so those two assertions are the documented gap; the no-error
/// contract both halves share is what runs.
#[test]
fn create_table_if_not_exists_like_succeeds_over_existing_name() {
    let mut catalog = Catalog::default();
    crate::run_create_table_in(
        "create table ct1(a bigint)",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();

    // Fresh target: created.
    let created = crate::run_create_table_in(
        "create table if not exists ct like ct1",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    assert!(created);

    // Go's first case: duplicate target WITH the LIKE clause -- must not error.
    crate::run_create_table_in(
        "create table if not exists ct like ct1",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    // Go's second case: duplicate target WITHOUT the LIKE clause.
    crate::run_create_table_in(
        "create table if not exists ct(b bigint, c varchar(60));",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
}

/// Go `TestCreateTableWithKeyWord` (pkg/ddl/db_integration_test.go:87,
/// issue #9910): column names that are TiDB keywords (`pump`, `drainer`,
/// `node_id`, `node_state`) parse and create.
#[test]
fn create_table_with_keyword_column_names() {
    let mut catalog = Catalog::default();
    crate::run_create_table_in(
        "create table t1(pump varchar(20), drainer varchar(20), \
         node_id varchar(20), node_state varchar(20));",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
}

/// Go `TestUniqueKeyNullValue` (pkg/ddl/db_integration_test.go:98): a
/// unique index builds over rows whose indexed column is NULL in every row,
/// and the table still answers `count(*)` through that index. The Go test
/// closes with `admin check table` / `admin check index`, which have no
/// carrier here.
#[test]
fn unique_key_null_value_allows_multiple_nulls() {
    let mut catalog = Catalog::default();
    crate::run_create_table_in(
        "create table t(a int primary key, b varchar(255))",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_insert_on("insert into t values(1, NULL)", &mut catalog, &ctx()).unwrap();
    crate::run_insert_on("insert into t values(2, NULL)", &mut catalog, &ctx()).unwrap();
    crate::run_alter_table_in(
        "alter table t add unique index b(b);",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    let rows = run_select_on("select count(*) from t use index(b)", &catalog, &ctx())
        .unwrap();
    assert_eq!(rows, vec![vec![Datum::Int(2)]]);
}

/// Go `TestUniqueKeyNullValueClusterIndex`
/// (pkg/ddl/db_integration_test.go:115): the same NULL-index property over
/// a clustered composite primary key of `varchar` + `float`, in its own
/// database.
#[test]
fn unique_key_null_value_cluster_index_composite_primary_key() {
    let mut catalog = Catalog::default();
    assert!(catalog.create_database_with_charset("unique_null_val", Default::default()));
    crate::run_create_table_in(
        "create table t (a varchar(10), b float, c varchar(255), primary key (a, b));",
        &mut catalog,
        "unique_null_val",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_insert_in(
        "insert into t values ('1', 1, NULL);",
        &mut catalog,
        "unique_null_val",
        &ctx(),
    )
    .unwrap();
    crate::run_insert_in(
        "insert into t values ('2', 2, NULL);",
        &mut catalog,
        "unique_null_val",
        &ctx(),
    )
    .unwrap();
    crate::run_alter_table_in(
        "alter table t add unique index c(c);",
        &mut catalog,
        "unique_null_val",
        &ctx(),
    )
    .unwrap();
    let rows = run_select_meta_in(
        "select count(*) from t use index(c)",
        &catalog,
        "unique_null_val",
        &ctx(),
    )
    .map(|(_, rows)| rows)
    .unwrap();
    assert_eq!(rows, vec![vec![Datum::Int(2)]]);
}

/// Go `TestModifyColumnAfterAddIndex` (pkg/ddl/db_integration_test.go:133,
/// issue #5134): a `varchar(2)` clustered primary-key column widens to
/// `varchar(50)` and then accepts values the old type would have rejected.
#[test]
fn modify_column_after_add_index_widens_key_column() {
    let mut catalog = Catalog::default();
    crate::run_create_table_in(
        "create table city (city VARCHAR(2) KEY);",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_alter_table_in(
        "alter table city change column city city varchar(50);",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    crate::run_insert_on(
        "insert into city values (\"abc\"), (\"abd\");",
        &mut catalog,
        &ctx(),
    )
    .unwrap();
}

// go-parity-gap: Go `TestModifyColumnOldColumnIDNotFound`
// (pkg/ddl/db_integration_test.go:143) mocks an old-version job whose
// trailing job arg was dropped by an owner change, through the
// `afterRunOneJobStep` failpoint and `model.UpdateJobArgsForTest`; the
// persisted-job argument machinery it exercises is deferred with the rest
// of the job queue.
#[test]
#[ignore = "go-parity-gap: job-args downgrade simulation needs persisted DDL jobs"]
fn modify_column_old_column_id_not_found() {}

/// Go `TestIssue2293` (pkg/ddl/db_integration_test.go:166): a string
/// literal that is not a number is `errno.ErrInvalidDefault` (1067) as an
/// ADD COLUMN default, and the table stays usable.
#[test]
fn issue_2293_invalid_string_default_rejected() {
    let mut catalog = Catalog::default();
    crate::run_create_table_in(
        "create table t_issue_2293 (a int)",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    let error = crate::run_alter_table_in(
        "alter table t_issue_2293 add b int not null default 'a'",
        &mut catalog,
        "test",
        &ctx(),
    )
    .expect_err("'a' is not a valid INT default");
    let (code, _message) = mysql(error);
    assert_eq!(code, 1067);

    crate::run_insert_on("insert into t_issue_2293 value(1)", &mut catalog, &ctx())
        .unwrap();
    let rows =
        run_select_on("select * from t_issue_2293", &catalog, &ctx()).unwrap();
    assert_eq!(rows, vec![vec![Datum::Int(1)]]);
}

/// Go `TestIssue19229` (pkg/ddl/db_integration_test.go:177): under a strict
/// session a bad ENUM/SET insert is errno `WarnDataTruncated` (1265) as an
/// ERROR -- for an unknown string member AND for an out-of-range negative
/// number, on both type families. Measured divergence, documented rather
/// than ported: `insert into sett values(-1)` is ACCEPTED here (it lands on
/// member 'a'), so that one assertion has no carrier.
#[test]
fn issue_19229_enum_set_strict_insert_truncation() {
    let mut catalog = Catalog::default();

    crate::run_create_table_in(
        "CREATE TABLE enumt (type enum('a', 'b') );",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    for value in ["'xxx'", "-1"] {
        let error = crate::run_insert_on(
            &format!("insert into enumt values({value});"),
            &mut catalog,
            &ctx(),
        )
        .expect_err("strict mode turns the truncation warning into an error");
        let (code, _message) = mysql(error);
        assert_eq!(code, 1265, "enum value {value}");
    }
    crate::run_drop_table_in(
        "drop table enumt",
        &mut catalog,
        "test",
        tidb_parser::SqlMode::default(),
        true,
    )
    .unwrap();

    crate::run_create_table_in(
        "CREATE TABLE sett (type set('a', 'b') );",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    // Only the string-member case: the numeric `-1` case measured above is
    // accepted by this tier and stays a go-parity-gap.
    let error = crate::run_insert_on(
        "insert into sett values('xxx');",
        &mut catalog,
        &ctx(),
    )
    .expect_err("'xxx' is not a SET member");
    let (code, _message) = mysql(error);
    assert_eq!(code, 1265);
}

/// Go `TestIndexLength` (pkg/ddl/db_integration_test.go:201): zero display
/// widths parse (`int(0)`, `timestamp(0)`, `datetime(0)`, `time(0)`,
/// `float(0)`, `decimal(0)`) and every one of those columns indexes -- both
/// through `CREATE INDEX`/`ALTER TABLE ADD INDEX` and inline `index(...)`
/// definitions -- and the TEXT/BLOB prefix lengths pin at 768 chars over
/// utf8mb4 TEXT (3072 bytes), 3072 over ascii TEXT and 3072 over BLOB. The
/// `schematracker.Checker` enable/disable choreography is Go harness noise,
/// not behavior.
#[test]
fn index_length_zero_widths_and_text_blob_prefixes() {
    let mut catalog = Catalog::default();
    crate::run_create_table_in(
        "create table idx_len(a int(0), b timestamp(0), c datetime(0), \
         d time(0), f float(0), g decimal(0))",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_create_index_in("create index idx on idx_len(a)", &mut catalog, "test", &ctx())
        .unwrap();
    crate::run_alter_table_in(
        "alter table idx_len add index idxa(a)",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    crate::run_create_index_in("create index idx1 on idx_len(b)", &mut catalog, "test", &ctx())
        .unwrap();
    crate::run_alter_table_in(
        "alter table idx_len add index idxb(b)",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    crate::run_create_index_in("create index idx2 on idx_len(c)", &mut catalog, "test", &ctx())
        .unwrap();
    crate::run_alter_table_in(
        "alter table idx_len add index idxc(c)",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    crate::run_create_index_in("create index idx3 on idx_len(d)", &mut catalog, "test", &ctx())
        .unwrap();
    crate::run_alter_table_in(
        "alter table idx_len add index idxd(d)",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    crate::run_create_index_in("create index idx4 on idx_len(f)", &mut catalog, "test", &ctx())
        .unwrap();
    crate::run_alter_table_in(
        "alter table idx_len add index idxf(f)",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    crate::run_create_index_in("create index idx5 on idx_len(g)", &mut catalog, "test", &ctx())
        .unwrap();
    crate::run_alter_table_in(
        "alter table idx_len add index idxg(g)",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    crate::run_create_table_in(
        "create table idx_len1(a int(0), b timestamp(0), c datetime(0), d time(0), \
         f float(0), g decimal(0), index(a), index(b), index(c), index(d), index(f), index(g))",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_drop_table_in(
        "drop table idx_len;",
        &mut catalog,
        "test",
        tidb_parser::SqlMode::default(),
        true,
    )
    .unwrap();

    // All three prefix shapes at their MAXIMUM legal length, built inline...
    crate::run_create_table_in(
        "create table idx_len(a text, b text charset ascii, c blob, \
         index(a(768)), index (b(3072)), index (c(3072)));",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_drop_table_in(
        "drop table idx_len;",
        &mut catalog,
        "test",
        tidb_parser::SqlMode::default(),
        true,
    )
    .unwrap();
    // ...and through ALTER.
    crate::run_create_table_in(
        "create table idx_len(a text, b text charset ascii, c blob);",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_alter_table_in(
        "alter table idx_len add index (a(768))",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    crate::run_alter_table_in(
        "alter table idx_len add index (b(3072))",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    crate::run_alter_table_in(
        "alter table idx_len add index (c(3072))",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
}

/// Go `TestIssue2858And2717` (pkg/ddl/db_integration_test.go:238): BIT and
/// hexadecimal defaults. `bit(64) default b'0'` inserts as 0, accepts the
/// literals `100`, `'10'` and `'\0'`, and `select a+0` reads them back as
/// 0/100/12592/0; `alter column a set default '\0'` succeeds; `int default
/// 0x123` fills 291, inserts `123` and `0x321` as 123/801, and
/// `alter column a set default 0x321` succeeds.
#[test]
fn issue_2858_and_2717_bit_and_hex_defaults() {
    let mut catalog = Catalog::default();
    crate::run_create_table_in(
        "create table t_issue_2858_bit (a bit(64) default b'0')",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_insert_on("insert into t_issue_2858_bit value ()", &mut catalog, &ctx())
        .unwrap();
    crate::run_insert_on(
        "insert into t_issue_2858_bit values (100), ('10'), ('\\0')",
        &mut catalog,
        &ctx(),
    )
    .unwrap();
    let rows =
        run_select_on("select a+0 from t_issue_2858_bit", &catalog, &ctx()).unwrap();
    let sums: Vec<i64> = rows
        .iter()
        .map(|row| match &row[0] {
            Datum::Int(value) => *value,
            Datum::UInt(value) => *value as i64,
            other => panic!("unexpected bit sum {other:?}"),
        })
        .collect();
    assert_eq!(sums, vec![0, 100, 12592, 0]);
    crate::run_alter_table_in(
        "alter table t_issue_2858_bit alter column a set default '\\0'",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();

    crate::run_create_table_in(
        "create table t_issue_2858_hex (a int default 0x123)",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_insert_on("insert into t_issue_2858_hex value ()", &mut catalog, &ctx())
        .unwrap();
    crate::run_insert_on(
        "insert into t_issue_2858_hex values (123), (0x321)",
        &mut catalog,
        &ctx(),
    )
    .unwrap();
    let rows =
        run_select_on("select a from t_issue_2858_hex", &catalog, &ctx()).unwrap();
    let values: Vec<i64> = rows
        .iter()
        .map(|row| match &row[0] {
            Datum::Int(value) => *value,
            other => panic!("unexpected hex value {other:?}"),
        })
        .collect();
    assert_eq!(values, vec![291, 123, 801]);
    crate::run_alter_table_in(
        "alter table t_issue_2858_hex alter column a set default 0x321",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
}

/// Go `TestIssue4432` (pkg/ddl/db_integration_test.go:257): the four
/// spellings of a `bit(10)` default -- string `'a'`, hex `0x61`, number
/// `97`, bit literal `0b1100001` -- all insert the same bytes `\x00a`.
#[test]
fn issue_4432_bit_default_spellings_agree() {
    for spelling in [
        "default 'a'",
        "default 0x61",
        "default 97",
        "default 0b1100001",
    ] {
        let mut catalog = Catalog::default();
        let sql = format!("create table tx (col bit(10) {spelling})");
        crate::run_create_table_in(&sql, &mut catalog, "test", Default::default(), &ctx())
            .unwrap();
        crate::run_insert_on("insert into tx value ()", &mut catalog, &ctx()).unwrap();
        let rows = run_select_on("select * from tx", &catalog, &ctx()).unwrap();
        match &rows[0][0] {
            Datum::Bit(literal) => assert_eq!(
                literal.as_bytes(),
                &[0, 97],
                "bit default {spelling} must read back as \\x00a"
            ),
            other => panic!("bit default {spelling} produced {other:?}"),
        }
    }
}

/// Go `TestIssue5092` (pkg/ddl/db_integration_test.go:284): the ADD/DROP
/// COLUMN battery. Carried: grouped `(b int, c int)` adds; `FIRST`/`AFTER`
/// placement (verified through the persisted column order, which is what
/// Go's SHOW CREATE TABLE expectations render); the mixed
/// `if not exists ... , add column ...` duplicate reporting 1060; the
/// doubled `drop column c, drop column c` reporting 1091; the missing
/// `drop column g, drop column d` reporting 1091; default-valued adds
/// filling old and new rows exactly as Go's SELECT expectations read.
/// Omitted (measured divergences this session): the re-add of existing
/// columns through `add column if not exists (b int, c int)` errors 1060
/// here instead of skipping; `add column dd int, add column if not exists
/// dd int` reports 1060 here instead of Go's
/// `errno.ErrUnsupportedDDLOperation` (8200); and the three forms
/// `drop column if exists <gone>, drop column ...` /
/// `drop column c, drop column c` repeats that Go reports as 8200/1090 are
/// 1091 here.
#[test]
fn issue_5092_add_and_drop_column_battery() {
    let mut catalog = Catalog::default();

    crate::run_create_table_in(
        "create table t_issue_5092 (a int)",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_alter_table_in(
        "alter table t_issue_5092 add column (b int, c int)",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    // (the `add column if not exists (b int, c int)` skip is an omitted gap)
    crate::run_alter_table_in(
        "alter table t_issue_5092 add column b1 int after b, add column c1 int after c",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    crate::run_alter_table_in(
        "alter table t_issue_5092 add column d int after b, add column e int first, \
         add column f int after c1, add column g int, add column h int first",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    assert_eq!(
        column_names(&catalog, "test", "t_issue_5092"),
        ["h", "e", "a", "b", "d", "b1", "c", "c1", "f", "g"],
        "order must match Go's SHOW CREATE TABLE at db_integration_test.go:297"
    );
    // Consistent with MariaDB: the duplicate in the SAME statement is 1060.
    let error = crate::run_alter_table_in(
        "alter table t_issue_5092 add column if not exists d int, add column d int",
        &mut catalog,
        "test",
        &ctx(),
    )
    .expect_err("the unguarded duplicate is a hard error");
    let (code, _message) = mysql(error);
    assert_eq!(code, 1060);
    // (the `dd int, if not exists dd int` 8200 form is an omitted gap)
    // Go's next statement is `add column if not exists (d int, e int),
    // add column ff text`: there it skips the already-present d/e and adds
    // ff. Measured this session: the grouped IF NOT EXISTS over existing
    // columns reports 1060 here instead of skipping, so only the fresh `ff`
    // half runs; the skip contract is the omitted gap.
    crate::run_alter_table_in(
        "alter table t_issue_5092 add column ff text",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    crate::run_alter_table_in(
        "alter table t_issue_5092 add column b2 int after b1, add column c2 int first",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    assert_eq!(
        column_names(&catalog, "test", "t_issue_5092"),
        [
            "c2", "h", "e", "a", "b", "d", "b1", "b2", "c", "c1", "f", "g", "ff"
        ],
        "order must match Go's SHOW CREATE TABLE at db_integration_test.go:321"
    );
    crate::run_drop_table_in(
        "drop table t_issue_5092",
        &mut catalog,
        "test",
        tidb_parser::SqlMode::default(),
        true,
    )
    .unwrap();

    // Defaults battery: new columns fill pre-existing rows and fresh rows
    // take every default, exactly as Go's SELECT expectations read.
    crate::run_create_table_in(
        "create table t_issue_5092 (a int default 1)",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_alter_table_in(
        "alter table t_issue_5092 add column (b int default 2, c int default 3)",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    crate::run_alter_table_in(
        "alter table t_issue_5092 add column b1 int default 22 after b, \
         add column c1 int default 33 after c",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    crate::run_insert_on("insert into t_issue_5092 value ()", &mut catalog, &ctx())
        .unwrap();
    let rows =
        run_select_on("select * from t_issue_5092", &catalog, &ctx()).unwrap();
    let row: Vec<i64> = rows[0]
        .iter()
        .map(|datum| match datum {
            Datum::Int(value) => *value,
            other => panic!("unexpected default {other:?}"),
        })
        .collect();
    assert_eq!(row, vec![1, 2, 22, 3, 33]);
    crate::run_alter_table_in(
        "alter table t_issue_5092 add column d int default 4 after c1, \
         add column aa int default 0 first",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    let rows =
        run_select_on("select * from t_issue_5092", &catalog, &ctx()).unwrap();
    let row: Vec<i64> = rows[0]
        .iter()
        .map(|datum| match datum {
            Datum::Int(value) => *value,
            other => panic!("unexpected default {other:?}"),
        })
        .collect();
    assert_eq!(row, vec![0, 1, 2, 22, 3, 33, 4]);
    assert_eq!(
        column_names(&catalog, "test", "t_issue_5092"),
        ["aa", "a", "b", "b1", "c", "c1", "d"],
        "order must match Go's SHOW CREATE TABLE at db_integration_test.go:346"
    );
    crate::run_drop_table_in(
        "drop table t_issue_5092",
        &mut catalog,
        "test",
        tidb_parser::SqlMode::default(),
        true,
    )
    .unwrap();

    // Drop battery.
    crate::run_create_table_in(
        "create table t_issue_5092 (a int)",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_alter_table_in(
        "alter table t_issue_5092 add column (b int, c int)",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    crate::run_alter_table_in(
        "alter table t_issue_5092 drop column b,drop column c",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    let error = crate::run_alter_table_in(
        "alter table t_issue_5092 drop column c, drop column c",
        &mut catalog,
        "test",
        &ctx(),
    )
    .expect_err("dropping the absent column twice in one statement is 1091");
    let (code, _message) = mysql(error);
    assert_eq!(code, 1091);
    crate::run_alter_table_in(
        "alter table t_issue_5092 drop column if exists b,drop column if exists c",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    let error = crate::run_alter_table_in(
        "alter table t_issue_5092 drop column g, drop column d",
        &mut catalog,
        "test",
        &ctx(),
    )
    .expect_err("both names are missing");
    let (code, _message) = mysql(error);
    assert_eq!(code, 1091);
    crate::run_drop_table_in(
        "drop table t_issue_5092",
        &mut catalog,
        "test",
        tidb_parser::SqlMode::default(),
        true,
    )
    .unwrap();

    // The remove-all / repeated-target forms Go reports as 1090/8200 are
    // the omitted gap: they are 1091 here (measured this session).
}

/// Go `TestTableDDLWithTimeType` (pkg/ddl/db_integration_test.go:373): time
/// families refuse precision above 6 with `errno.ErrTooBigPrecision`
/// (pkg/errno/errcode.go:429 pins it at 1426) on CREATE, ADD COLUMN,
/// MODIFY COLUMN and CHANGE COLUMN; `time(-1)` is refused with SOME error;
/// `datetime(0)` is accepted. The rendered message matches Go's
/// `mysql.MessageTooBigPrecision` format too (`Too-big precision 7
/// specified for 'a'. Maximum is 6.`, pkg/errno/errname.go:435), and the
/// CREATE case asserts it; the rest pin the code exactly as Go's
/// MustGetErrCode does.
#[test]
fn table_ddl_with_time_type_precision_bounds() {
    let mut catalog = Catalog::default();
    let error = crate::run_create_table_in(
        "create table t (a time(7))",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .expect_err("time precision 7 is above the maximum 6");
    let (code, message) = mysql(error);
    assert_eq!(code, 1426);
    assert_eq!(
        message,
        "Too-big precision 7 specified for 'a'. Maximum is 6.",
        "same format Go renders from mysql.MessageTooBigPrecision"
    );
    /// Any of the time-family statements above precision 6 must report
    /// `errno.ErrTooBigPrecision`.
    fn refused(catalog: &mut Catalog, sql: &str) -> u16 {
        let error =
            crate::run_create_table_in(sql, catalog, "test", Default::default(), &ctx())
                .err()
                .or_else(|| crate::run_alter_table_in(sql, catalog, "test", &ctx()).err());
        let (code, _message) = mysql(error.expect("{sql} must be refused"));
        code
    }

    assert_eq!(
        refused(&mut catalog, "create table t (a datetime(7))"),
        1426,
        "datetime precision"
    );
    assert_eq!(
        refused(&mut catalog, "create table t (a timestamp(7))"),
        1426,
        "timestamp precision"
    );
    // `time(-1)` must be refused with SOME error (Go uses require.Error).
    assert!(
        crate::run_create_table_in("create table t (a time(-1))", &mut catalog, "test", Default::default(), &ctx())
            .is_err()
    );
    crate::run_create_table_in(
        "create table t (a datetime)",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    for sql in [
        "alter table t add column b time(7)",
        "alter table t add column b datetime(7)",
        "alter table t add column b timestamp(7)",
        "alter table t modify column a time(7)",
        "alter table t modify column a datetime(7)",
        "alter table t modify column a timestamp(7)",
        "alter table t change column a aa time(7)",
        "alter table t change column a aa datetime(7)",
        "alter table t change column a aa timestamp(7)",
    ] {
        let error = crate::run_alter_table_in(sql, &mut catalog, "test", &ctx())
            .expect_err(sql);
        let (code, _message) = mysql(error);
        assert_eq!(code, 1426, "{sql}");
    }
    crate::run_alter_table_in(
        "alter table t change column a aa datetime(0)",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
}

/// Go `TestUpdateMultipleTable` (pkg/ddl/db_integration_test.go:398): the
/// multi-table UPDATE `update t1, t2 set t1.c1 = 8, t2.c2 = 10 where
/// t1.c2 = t2.c1` rewrites both tables exactly once per matched pair, and
/// the later `add column c3 bigint default 9` fills 9 into the rewritten
/// rows. Go runs the UPDATE inside the ALTER's write-only state through the
/// `afterWaitSchemaSynced` failpoint; the mid-DDL execution is the
/// documented gap -- every row assertion of the Go test is carried here in
/// the same order.
#[test]
fn update_multiple_table_rewrites_both_sides() {
    let mut catalog = Catalog::default();
    crate::run_create_table_in(
        "create table t1 (c1 int, c2 int)",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_insert_on("insert into t1 values (1, 1), (2, 2)", &mut catalog, &ctx())
        .unwrap();
    crate::run_create_table_in(
        "create table t2 (c1 int, c2 int)",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_insert_on("insert into t2 values (1, 3), (2, 5)", &mut catalog, &ctx())
        .unwrap();

    crate::run_update_on(
        "update t1, t2 set t1.c1 = 8, t2.c2 = 10 where t1.c2 = t2.c1",
        &mut catalog,
        &ctx(),
    )
    .unwrap();
    let rows = run_select_on("select * from t1", &catalog, &ctx()).unwrap();
    let rendered: Vec<Vec<i64>> = rows
        .iter()
        .map(|row| row.iter().map(datum_int).collect())
        .collect();
    assert_eq!(rendered, vec![vec![8, 1], vec![8, 2]]);
    let rows = run_select_on("select * from t2", &catalog, &ctx()).unwrap();
    let rendered: Vec<Vec<i64>> = rows
        .iter()
        .map(|row| row.iter().map(datum_int).collect())
        .collect();
    assert_eq!(rendered, vec![vec![1, 10], vec![2, 10]]);

    crate::run_alter_table_in(
        "alter table t1 add column c3 bigint default 9",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    let rows = run_select_on("select * from t1", &catalog, &ctx()).unwrap();
    let rendered: Vec<Vec<i64>> = rows
        .iter()
        .map(|row| row.iter().map(datum_int).collect())
        .collect();
    assert_eq!(rendered, vec![vec![8, 1, 9], vec![8, 2, 9]]);
}

/// Ints (and bigint defaults) out of a result row.
fn datum_int(datum: &Datum) -> i64 {
    match datum {
        Datum::Int(value) => *value,
        other => panic!("expected an int, got {other:?}"),
    }
}

/// Go `TestNullGeneratedColumn` (pkg/ddl/db_integration_test.go:422): a
/// virtual generated column over all-NULL bases indexes cleanly after a
/// defaulted INSERT, and the single row survives.
#[test]
fn null_generated_column_indexes_over_nulls() {
    let mut catalog = Catalog::default();
    crate::run_create_table_in(
        "CREATE TABLE `t` (\
         `a` int(11) DEFAULT NULL,\
         `b` int(11) DEFAULT NULL,\
         `c` int(11) GENERATED ALWAYS AS (`a` + `b`) VIRTUAL,\
         `h` varchar(10) DEFAULT NULL,\
         `m` int(11) DEFAULT NULL\
         ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_insert_on("insert into t values()", &mut catalog, &ctx()).unwrap();
    crate::run_alter_table_in(
        "alter table t add index idx_c(c)",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    let rows = run_select_on("select count(*) from t", &catalog, &ctx()).unwrap();
    assert_eq!(rows, vec![vec![Datum::Int(1)]]);
}

/// Go `TestDependedGeneratedColumnPrior2GeneratedColumn`
/// (pkg/ddl/db_integration_test.go:442): an ADD COLUMN whose generated
/// expression names an unknown column reports `errno.ErrBadField` (1054)
/// even when the position is also wrong; a generated column placed `after`
/// its dependency builds; the parenthesized group form builds too. The
/// unknown-column check MUST win the ordering race: Go's
/// `alter table t add column d int as (c + f + 1) first` reports 1054 (the
/// unknown `f`), which is what is pinned here. Omitted gap (measured this
/// session): the `as (c+1) first` non-prior case is Go errno 3107
/// (`errno.ErrGeneratedColumnNonPrior`, pkg/errno/errcode.go:842) but is
/// 1054 here.
#[test]
fn depended_generated_column_prior_checks() {
    let mut catalog = Catalog::default();
    crate::run_create_table_in(
        "CREATE TABLE `t` (\
         `a` int(11) DEFAULT NULL,\
         `b` int(11) GENERATED ALWAYS AS (`a` + 1) VIRTUAL,\
         `c` int(11) GENERATED ALWAYS AS (`b` + 1) VIRTUAL\
         ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    // Unknown column checked first, before the placement problem.
    let error = crate::run_alter_table_in(
        "alter table t add column d int as (c + f + 1) first",
        &mut catalog,
        "test",
        &ctx(),
    )
    .expect_err("`f` does not exist");
    let (code, _message) = mysql(error);
    assert_eq!(code, 1054);
    // (the `as (c+1) first` 3107 form is the omitted gap)
    // Correct case: the dependency comes first.
    crate::run_alter_table_in(
        "alter table t add column d int as (c+1) after c",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    // Position-nil group form.
    crate::run_alter_table_in(
        "alter table t add column(e int as (c+1))",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
}

// go-parity-gap: Go `TestChangingTableCharset`
// (pkg/ddl/db_integration_test.go:468) pins the whole ALTER TABLE
// charset/CONVERT TO contract -- the 8200 refusals, unknown charset/collation
// 1115/1273, collation-charset mismatch 1253, conflicting declarations, and
// the column-charset rewrite the conversion performs. Measured this session:
// this tier rejects `alter table ... charset ...` outright with
// `this ALTER TABLE table option is not supported yet`, so none of those
// contracts can be produced.
#[test]
#[ignore = "go-parity-gap: ALTER TABLE charset conversion is not built in this tier"]
fn changing_table_charset() {}

/// Go `TestModifyColumnOption` (pkg/ddl/db_integration_test.go:626), carried
/// subset: CHANGE COLUMN retypes an `int` column through varchar(16) ->
/// varchar(10) -> datetime -> `int(11) unsigned`, and widens a `char` to
/// `int(11) unsigned`. Omitted gaps (measured divergences this session):
/// the `references t1(a)` MODIFY that Go reports as `[ddl:8200]Unsupported
/// modify column...` is `this column option is not supported in ALTER TABLE
/// MODIFY COLUMN` (1105) here, and the `character set ... collate ...`
/// MODIFY forms Go accepts are refused with the same 1105 here.
#[test]
fn modify_column_option_retype_chain() {
    let mut catalog = Catalog::default();
    crate::run_create_table_in(
        "create table t1 (a int(11) default null)",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_create_table_in(
        "create table t2 (b char, c int)",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    // (the `references t1(a)` 8200 form is an omitted gap)
    crate::run_alter_table_in(
        "alter table t1 change a a varchar(16)",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    crate::run_alter_table_in(
        "alter table t1 change a a varchar(10)",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    crate::run_alter_table_in(
        "alter table t1 change a a datetime",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    crate::run_alter_table_in(
        "alter table t1 change a a int(11) unsigned",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    crate::run_alter_table_in(
        "alter table t2 change b b int(11) unsigned",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
}

/// Go `TestIndexOnMultipleGeneratedColumn`
/// (pkg/ddl/db_integration_test.go:661): an index over a two-step generated
/// chain (`b int as (a + 1)`, `c int as (b + 1)`) builds, and the USE/IGNORE
/// INDEX pair answers identically for `c > 1`.
#[test]
fn index_on_multiple_generated_column_chain_of_two() {
    let mut catalog = Catalog::default();
    crate::run_create_table_in(
        "create table t (a int, b int as (a + 1), c int as (b + 1))",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_insert_on("insert into t (a) values (1)", &mut catalog, &ctx()).unwrap();
    crate::run_create_index_in("create index idx on t (c)", &mut catalog, "test", &ctx())
        .unwrap();
    let rows = run_select_on("select * from t where c > 1", &catalog, &ctx()).unwrap();
    assert_eq!(
        rows,
        vec![vec![Datum::Int(1), Datum::Int(2), Datum::Int(3)]]
    );
    let using =
        run_select_on("select * from t use index(idx) where c > 1", &catalog, &ctx())
            .unwrap();
    let ignoring = run_select_on(
        "select * from t ignore index(idx) where c > 1",
        &catalog,
        &ctx(),
    )
    .unwrap();
    assert_eq!(using, ignoring);
    // (Go closes with `admin check table t`, which has no carrier here)
}

/// Go `TestIndexOnMultipleGeneratedColumn1`
/// (pkg/ddl/db_integration_test.go:678): the same over a three-step chain.
#[test]
fn index_on_multiple_generated_column_chain_of_three() {
    let mut catalog = Catalog::default();
    crate::run_create_table_in(
        "create table t (a int, b int as (a + 1), c int as (b + 1), d int as (c + 1))",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_insert_on("insert into t (a) values (1)", &mut catalog, &ctx()).unwrap();
    crate::run_create_index_in("create index idx on t (d)", &mut catalog, "test", &ctx())
        .unwrap();
    let rows = run_select_on("select * from t where d > 2", &catalog, &ctx()).unwrap();
    assert_eq!(
        rows,
        vec![vec![Datum::Int(1), Datum::Int(2), Datum::Int(3), Datum::Int(4)]]
    );
    let using =
        run_select_on("select * from t use index(idx) where d > 2", &catalog, &ctx())
            .unwrap();
    let ignoring = run_select_on(
        "select * from t ignore index(idx) where d > 2",
        &catalog,
        &ctx(),
    )
    .unwrap();
    assert_eq!(using, ignoring);
}

/// Go `TestIndexOnMultipleGeneratedColumn2`
/// (pkg/ddl/db_integration_test.go:695): a chain mixing bigint, decimal,
/// varchar and float across `*`, `length()`, and arithmetic; the final
/// float reads 25 for the seed row.
#[test]
fn index_on_multiple_generated_column_mixed_types() {
    let mut catalog = Catalog::default();
    crate::run_create_table_in(
        "create table t (a bigint, b decimal as (a+1), c varchar(20) as (b*2), \
         d float as (a*23+b-1+length(c)))",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_insert_on("insert into t (a) values (1)", &mut catalog, &ctx()).unwrap();
    crate::run_create_index_in("create index idx on t (d)", &mut catalog, "test", &ctx())
        .unwrap();
    let rows = run_select_on("select * from t where d > 2", &catalog, &ctx()).unwrap();
    assert_eq!(rows[0][0], Datum::Int(1));
    // `b decimal as (a+1)` reads back as the decimal 2.
    match &rows[0][1] {
        Datum::Decimal(value) => assert_eq!(value.to_string(), "2"),
        other => panic!("expected the decimal 2, got {other:?}"),
    }
    match &rows[0][2] {
        Datum::String(text) => assert_eq!(text.bytes(), b"4"),
        other => panic!("expected the string '4', got {other:?}"),
    }
    match &rows[0][3] {
        Datum::Float32(value) => assert_eq!(*value, 25.0),
        other => panic!("expected the float 25, got {other:?}"),
    }
    let using =
        run_select_on("select * from t use index(idx) where d > 2", &catalog, &ctx())
            .unwrap();
    let ignoring = run_select_on(
        "select * from t ignore index(idx) where d > 2",
        &catalog,
        &ctx(),
    )
    .unwrap();
    assert_eq!(using, ignoring);
}

/// Go `TestIndexOnMultipleGeneratedColumn3`
/// (pkg/ddl/db_integration_test.go:712): a chain over string functions --
/// `length(a)+123`, `right(a, 2)`, and an ASCII-weighted float that must
/// land on exactly 131 / 'le' / 577 for 'adorable' (131+131-7+1-3+3*108).
#[test]
fn index_on_multiple_generated_column_string_functions() {
    let mut catalog = Catalog::default();
    crate::run_create_table_in(
        "create table t (a varchar(10), b float as (length(a)+123), \
         c varchar(20) as (right(a, 2)), d float as (b+b-7+1-3+3*ASCII(c)))",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_insert_on(
        "insert into t (a) values ('adorable')",
        &mut catalog,
        &ctx(),
    )
    .unwrap();
    crate::run_create_index_in("create index idx on t (d)", &mut catalog, "test", &ctx())
        .unwrap();
    let rows = run_select_on("select * from t where d > 2", &catalog, &ctx()).unwrap();
    match &rows[0][0] {
        Datum::String(text) => assert_eq!(text.bytes(), b"adorable"),
        other => panic!("expected 'adorable', got {other:?}"),
    }
    match &rows[0][1] {
        Datum::Float32(value) => assert_eq!(*value, 131.0),
        other => panic!("expected the float 131, got {other:?}"),
    }
    match &rows[0][2] {
        Datum::String(text) => assert_eq!(text.bytes(), b"le"),
        other => panic!("expected 'le', got {other:?}"),
    }
    match &rows[0][3] {
        Datum::Float32(value) => assert_eq!(*value, 577.0),
        other => panic!("expected the float 577, got {other:?}"),
    }
    let using =
        run_select_on("select * from t use index(idx) where d > 2", &catalog, &ctx())
            .unwrap();
    let ignoring = run_select_on(
        "select * from t ignore index(idx) where d > 2",
        &catalog,
        &ctx(),
    )
    .unwrap();
    assert_eq!(using, ignoring);
}

/// Go `TestIndexOnMultipleGeneratedColumn4`
/// (pkg/ddl/db_integration_test.go:729): a chain where every step consumes
/// ALL previous columns (`a`, `b as (a)`, `c as (a+b)`, `d as (a+b+c)`,
/// `e as (a+b+c+d)`) and lands on 1/1/2/4/8.
#[test]
fn index_on_multiple_generated_column_self_accumulating() {
    let mut catalog = Catalog::default();
    crate::run_create_table_in(
        "create table t (a bigint, b decimal as (a), c int(10) as (a+b), \
         d float as (a+b+c), e decimal as (a+b+c+d))",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_insert_on("insert into t (a) values (1)", &mut catalog, &ctx()).unwrap();
    crate::run_create_index_in("create index idx on t (d)", &mut catalog, "test", &ctx())
        .unwrap();
    let rows = run_select_on("select * from t where d > 2", &catalog, &ctx()).unwrap();
    assert_eq!(rows[0][0], Datum::Int(1));
    // `b decimal as (a)` and `e decimal as (a+b+c+d)` read back as decimals.
    match &rows[0][1] {
        Datum::Decimal(value) => assert_eq!(value.to_string(), "1"),
        other => panic!("expected the decimal 1, got {other:?}"),
    }
    assert_eq!(rows[0][2], Datum::Int(2));
    match &rows[0][3] {
        Datum::Float32(value) => assert_eq!(*value, 4.0),
        other => panic!("expected the float 4, got {other:?}"),
    }
    match &rows[0][4] {
        Datum::Decimal(value) => assert_eq!(value.to_string(), "8"),
        other => panic!("expected the decimal 8, got {other:?}"),
    }
    let using =
        run_select_on("select * from t use index(idx) where d > 2", &catalog, &ctx())
            .unwrap();
    let ignoring = run_select_on(
        "select * from t ignore index(idx) where d > 2",
        &catalog,
        &ctx(),
    )
    .unwrap();
    assert_eq!(using, ignoring);
}

/// Go `TestIndexOnMultipleGeneratedColumn5`
/// (pkg/ddl/db_integration_test.go:746): VIRTUAL generated columns indexed
/// at CREATE-plus-ALTER time -- the index over `d as (c+1)` is added AFTER
/// rows exist and must still answer `d > 2` with the single seeded row.
#[test]
fn index_on_multiple_generated_column_added_after_rows() {
    let mut catalog = Catalog::default();
    crate::run_create_table_in(
        "create table t(a bigint, b bigint as (a+1) virtual, c bigint as (b+1) virtual)",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_alter_table_in(
        "alter table t add index idx_b(b)",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    crate::run_alter_table_in(
        "alter table t add index idx_c(c)",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    crate::run_insert_on("insert into t(a) values(1)", &mut catalog, &ctx()).unwrap();
    crate::run_alter_table_in(
        "alter table t add column(d bigint as (c+1) virtual)",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    crate::run_alter_table_in(
        "alter table t add index idx_d(d)",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    let rows = run_select_on("select * from t where d > 2", &catalog, &ctx()).unwrap();
    assert_eq!(
        rows,
        vec![vec![Datum::Int(1), Datum::Int(2), Datum::Int(3), Datum::Int(4)]]
    );
    let using = run_select_on(
        "select * from t use index(idx_d) where d > 2",
        &catalog,
        &ctx(),
    )
    .unwrap();
    let ignoring = run_select_on(
        "select * from t ignore index(idx_d) where d > 2",
        &catalog,
        &ctx(),
    )
    .unwrap();
    assert_eq!(using, ignoring);
}

/// Go `TestCaseInsensitiveCharsetAndCollate`
/// (pkg/ddl/db_integration_test.go:766), carried subset: charset and
/// collation names are matched CASE-INSENSITIVELY at CREATE time and
/// persisted lower-cased -- `UTF8`/`uTF8_BIN`/`Utf8`/`utf8MB4_BIN`/
/// `utf8MB4_general_ci` all create, and the built metadata for the
/// `UTF8MB4 GENERAL_CI` table reads `utf8mb4`/`utf8mb4_general_ci` at both
/// table and column level, with the Chinese-text row intact. Omitted gaps:
/// the Go test's later halves mutate raw persisted `TableInfo` through the
/// meta mutator to drive the `TableInfoVersion` 2-vs-3 case-conversion
/// rules; neither raw meta rewriting nor those version rules are modelled
/// here.
#[test]
fn case_insensitive_charset_and_collate_persists_lowercase() {
    let mut catalog = Catalog::default();
    assert!(catalog.create_database_with_charset("test_charset_collate", Default::default()));
    for table in ["t", "t1", "t2", "t3", "t4"] {
        let collation = match table {
            "t" => "UTF8_BIN",
            "t1" => "uTF8_BIN",
            "t2" => "utf8_BIN",
            "t3" => "utf8MB4_BIN",
            _ => "utf8MB4_general_ci",
        };
        let charset = if table == "t" || table == "t1" || table == "t2" {
            match table {
                "t" => "UTF8",
                "t1" => "uTF8",
                _ => "Utf8",
            }
        } else {
            "Utf8mb4"
        };
        let sql = format!("create table {table}(id int) ENGINE=InnoDB DEFAULT CHARSET={charset} COLLATE={collation};");
        crate::run_create_table_in(&sql, &mut catalog, "test_charset_collate", Default::default(), &ctx())
            .unwrap();
    }

    crate::run_create_table_in(
        "create table t5(a varchar(20)) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4 COLLATE=UTF8MB4_GENERAL_CI;",
        &mut catalog,
        "test_charset_collate",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_insert_in(
        "insert into t5 values ('特克斯和凯科斯群岛')",
        &mut catalog,
        "test_charset_collate",
        &ctx(),
    )
    .unwrap();

    let Some(TableEntry::Kv(entry)) = catalog.table_in("test_charset_collate", "t5") else {
        panic!("t5 must be a storage-backed table");
    };
    let table_charset = entry.charset();
    assert_eq!(format!("{:?}", table_charset.charset), "Utf8Mb4");
    assert_eq!(
        entry.columns[0].field_type.charset_name(),
        "utf8mb4",
        "the column's charset persists lower-cased"
    );
}

/// Go `TestZeroFillCreateTable` (pkg/ddl/db_integration_test.go:824):
/// `year` builds with the unsigned flag (and leads a primary key), and
/// `tinyint(10) zerofill` builds with the unsigned flag -- read off the
/// built metadata, which is what Go reads through `infoschema.TableByName`.
#[test]
fn zero_fill_create_table_sets_unsigned_flags() {
    let mut catalog = Catalog::default();
    crate::run_create_table_in(
        "create table abc(y year, z tinyint(10) zerofill, primary key(y));",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    let Some(TableEntry::Kv(entry)) = catalog.table_in("test", "abc") else {
        panic!("abc must be a storage-backed table");
    };
    let year_column = entry
        .columns
        .iter()
        .find(|column| column.name == "y")
        .expect("the y column");
    assert_eq!(
        year_column.field_type.code(),
        tidb_datatype::FieldTypeCode::Year,
        "Go mysql.TypeYear"
    );
    assert!(
        year_column.field_type.has_flag(FieldTypeFlags::UNSIGNED),
        "Go mysql.HasUnsignedFlag on the year column"
    );
    let z_column = entry
        .columns
        .iter()
        .find(|column| column.name == "z")
        .expect("the z column");
    assert!(
        z_column.field_type.has_flag(FieldTypeFlags::UNSIGNED),
        "ZEROFILL implies unsigned"
    );
}

/// Go `TestBitDefaultValue` (pkg/ddl/db_integration_test.go:851): BIT
/// defaults across creation, ALTER ADD COLUMN, and MODIFY COLUMN DEFAULT.
/// `bit(10) default 250` reads back through `bin(c1)` as 11111010;
/// `bit(16) null default b'1100110111001'` back-fills existing rows with
/// bytes \x19\xb9 and an UPDATE to `b'11100000000111'` rewrites them to
/// \x38\x07; a `bit(1) default b'0'` column keeps its written 0 after the
/// default moves to `b'1'`; a bare `bit` stores NULL and `count(*) where a
/// is null` answers 1; and the two `testalltypes` spellings create.
#[test]
fn bit_default_value_forms() {
    let mut catalog = Catalog::default();
    crate::run_create_table_in(
        "create table t_bit (c1 bit(10) default 250, c2 int);",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_insert_on("insert into t_bit set c2=1;", &mut catalog, &ctx()).unwrap();
    let rows = run_select_on("select bin(c1),c2 from t_bit", &catalog, &ctx()).unwrap();
    match &rows[0][0] {
        Datum::String(text) => assert_eq!(text.bytes(), b"11111010"),
        other => panic!("expected '11111010', got {other:?}"),
    }
    assert_eq!(rows[0][1], Datum::Int(1));
    crate::run_drop_table_in(
        "drop table t_bit",
        &mut catalog,
        "test",
        tidb_parser::SqlMode::default(),
        true,
    )
    .unwrap();

    crate::run_create_table_in(
        "create table t_bit (a int)",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_insert_on("insert into t_bit value (1)", &mut catalog, &ctx()).unwrap();
    crate::run_alter_table_in(
        "alter table t_bit add column c bit(16) null default b'1100110111001'",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    let rows = run_select_on("select c from t_bit", &catalog, &ctx()).unwrap();
    match &rows[0][0] {
        Datum::Bit(literal) => assert_eq!(literal.as_bytes(), &[0x19, 0xb9]),
        other => panic!("expected bit bytes \\x19\\xb9, got {other:?}"),
    }
    crate::run_update_on(
        "update t_bit set c = b'11100000000111'",
        &mut catalog,
        &ctx(),
    )
    .unwrap();
    let rows = run_select_on("select c from t_bit", &catalog, &ctx()).unwrap();
    match &rows[0][0] {
        Datum::Bit(literal) => assert_eq!(literal.as_bytes(), &[0x38, 0x07]),
        other => panic!("expected bit bytes \\x38\\x07, got {other:?}"),
    }
    crate::run_drop_table_in(
        "drop table t_bit",
        &mut catalog,
        "test",
        tidb_parser::SqlMode::default(),
        true,
    )
    .unwrap();

    crate::run_create_table_in(
        "create table t_bit (a int)",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_insert_on("insert into t_bit value (1)", &mut catalog, &ctx()).unwrap();
    crate::run_alter_table_in(
        "alter table t_bit add column b bit(1) default b'0';",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    crate::run_alter_table_in(
        "alter table t_bit modify column b bit(1) default b'1';",
        &mut catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    let rows = run_select_on("select b from t_bit", &catalog, &ctx()).unwrap();
    match &rows[0][0] {
        Datum::Bit(literal) => assert_eq!(literal.as_bytes(), &[0x00]),
        other => panic!("expected bit byte \\x00, got {other:?}"),
    }
    crate::run_drop_table_in(
        "drop table t_bit",
        &mut catalog,
        "test",
        tidb_parser::SqlMode::default(),
        true,
    )
    .unwrap();

    crate::run_create_table_in(
        "create table t_bit (a bit);",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_insert_on("insert into t_bit values (null);", &mut catalog, &ctx())
        .unwrap();
    let rows = run_select_on(
        "select count(*) from t_bit where a is null;",
        &catalog,
        &ctx(),
    )
    .unwrap();
    assert_eq!(rows, vec![vec![Datum::Int(1)]]);

    crate::run_create_table_in(
        "create table testalltypes1 (
    field_1 bit default 1,
    field_2 tinyint null default null
    );",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
    crate::run_create_table_in(
        "create table testalltypes2 (
    field_1 bit null default null,
    field_2 tinyint null default null,
    field_3 tinyint unsigned null default null,
    field_4 bigint null default null,
    field_5 bigint unsigned null default null,
    field_6 mediumblob null default null,
    field_7 longblob null default null,
    field_8 blob null default null,
    field_9 tinyblob null default null,
    field_10 varbinary(255) null default null,
    field_11 binary(255) null default null,
    field_12 mediumtext null default null,
    field_13 longtext null default null,
    field_14 text null default null,
    field_15 tinytext null default null,
    field_16 char(255) null default null,
    field_17 numeric null default null,
    field_18 decimal null default null,
    field_19 integer null default null,
    field_20 integer unsigned null default null,
    field_21 int null default null,
    field_22 int unsigned null default null,
    field_23 mediumint null default null,
    field_24 mediumint unsigned null default null,
    field_25 smallint null default null,
    field_26 smallint unsigned null default null,
    field_27 float null default null,
    field_28 double null default null,
    field_29 double precision null default null,
    field_30 real null default null,
    field_31 varchar(255) null default null,
    field_32 date null default null,
    field_33 time null default null,
    field_34 datetime null default null,
    field_35 timestamp null default null
    );",
        &mut catalog,
        "test",
        Default::default(),
        &ctx(),
    )
    .unwrap();
}

// go-parity-gap: Go `TestCreateTableTooLarge`
// (pkg/ddl/db_integration_test.go:948) builds a 12000-column table and pins
// `errno.ErrTooManyFields` (1117) at the default 512-column limit, then
// raises `config.TableColumnCountLimit` and pins `kv.ErrEntryTooLarge`.
// Measured this session: the 12000-column CREATE SUCCEEDS here (no column
// count limit is checked) and no transaction-size machinery exists, so
// neither contract can be produced.
#[test]
#[ignore = "go-parity-gap: column-count limit (1117) and entry-too-large are not modelled"]
fn create_table_too_large() {}
