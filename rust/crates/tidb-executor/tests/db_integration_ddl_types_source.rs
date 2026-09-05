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

//! Ports of the `pkg/ddl/db_integration_test.go` family (part3 items 153–180
//! of the package's `func Test*`/`func Benchmark*` declarations, sorted by
//! file and line), read from `origin/master`.
//!
//! The Go tests run whole statements through a mock-store TiDB and inspect
//! either the surviving rows, `SHOW CREATE TABLE`, or `TableInfo` read back
//! through the domain. This tier exposes the statement runners
//! (`run_create_table_on`, `run_alter_table_in`, the DML/SELECT drivers) and
//! the storage-backed catalog they populate, so a row-visible or errno-visible
//! contract is asserted directly; contracts that live in `SHOW CREATE TABLE`
//! text, `information_schema` retrievers, or direct meta mutation are named
//! as gaps. Every divergence found while porting is written in the test's
//! comment rather than papered over.

use tidb_datatype::{Datum, FieldTypeCode, FieldTypeFlags};
use tidb_executor::driver::Catalog;
use tidb_executor::{
    admin_check, ddl, run_create_table_on, run_insert_on, run_select_on, run_update_on, KvTable,
    RowDecodeContext, StmtContext, TableEntry,
};

/// The text of a string datum, however the codec chose to represent it
/// (crate `driver::tests::datum_text_for_test` is not visible here).
fn datum_text(value: &Datum) -> String {
    match value {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
        Datum::Int(i) => i.to_string(),
        Datum::UInt(u) => u.to_string(),
        Datum::Float32(f) => f.to_string(),
        Datum::Real(f) => f.to_string(),
        other => panic!("unexpected datum {other:?}"),
    }
}

fn rows_text(rows: &[Vec<Datum>]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(datum_text).collect())
        .collect()
}

/// The storage-backed table a test just built, cloned for `admin_check`.
fn kv_table(catalog: &Catalog, database: &str, name: &str) -> KvTable {
    match catalog.table_in(database, name) {
        Some(TableEntry::Kv(table)) => table.clone(),
        _ => panic!("expected a storage-backed table {database}.{name}"),
    }
}

fn column(catalog: &Catalog, database: &str, table_name: &str, name: &str) -> tidb_executor::KvColumn {
    let table = kv_table(catalog, database, table_name);
    table
        .columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(name))
        .unwrap_or_else(|| panic!("no column {name} in {database}.{table_name}"))
        .clone()
}

// --- TestCreateTableIfNotExistsLike (pkg/ddl/db_integration_test.go:60) ---
//
// Go creates `ct` and `ct1`, then `create table if not exists ct like ct1`
// and checks the session carries a Note-level `ErrTableExists` warning, and
// that the same holds for a plain (non-LIKE) duplicate. The suppression
// result (`Ok(false)`, never an error) is what this tier models; the
// Note-warning half is a GAP — `run_create_table_in` returns silently
// without appending the 1050 note (see the comment at the `name_taken`
// branch of `run_create_table_in`).
#[test]
fn create_table_if_not_exists_like_suppresses_and_copies() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table ct1(a bigint)", &mut catalog).unwrap();
    run_create_table_on("create table ct(a bigint)", &mut catalog).unwrap();
    // Duplicate create-table WITH the LIKE clause: suppressed, not an error.
    assert!(!run_create_table_on("create table if not exists ct like ct1", &mut catalog).unwrap());
    // The LIKE copy machinery itself (first creation) copies the structure.
    assert!(run_create_table_on("create table ct3 like ct1", &mut catalog).unwrap());
    let source = column(&catalog, "test", "ct1", "a");
    let copy = column(&catalog, "test", "ct3", "a");
    assert_eq!(source.field_type.code(), FieldTypeCode::LongLong);
    assert_eq!(copy.field_type.code(), source.field_type.code());
}

// --- TestCreateTableWithKeyWord (pkg/ddl/db_integration_test.go:87) ---
//
// Issue 9910: column names that are TiDB/system keywords must not break
// CREATE TABLE.
#[test]
fn create_table_with_keyword_column_names() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t1(pump varchar(20), drainer varchar(20), node_id varchar(20), \
         node_state varchar(20))",
        &mut catalog,
    )
    .unwrap();
    for name in ["pump", "drainer", "node_id", "node_state"] {
        assert_eq!(
            column(&catalog, "test", "t1", name).field_type.code(),
            FieldTypeCode::Varchar
        );
    }
}

// --- TestUniqueKeyNullValue (pkg/ddl/db_integration_test.go:98) ---
//
// Two rows with NULL in `b`, then `add unique index b(b)`: NULLs never
// collide, the index counts both rows, and `admin check table` / `admin
// check index t b` stay clean.
#[test]
fn unique_key_null_value_unique_index_allows_multiple_nulls() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t(a int primary key, b varchar(255))",
        &mut catalog,
    )
    .unwrap();
    let ctx = StmtContext::for_query();
    run_insert_on("insert into t values(1, NULL)", &mut catalog, &ctx).unwrap();
    run_insert_on("insert into t values(2, NULL)", &mut catalog, &ctx).unwrap();
    ddl::run_alter_table_in("alter table t add unique index b(b)", &mut catalog, "test", &ctx)
        .unwrap();
    assert_eq!(
        run_select_on("select count(*) from t use index(b)", &catalog, &ctx).unwrap(),
        vec![vec![Datum::Int(2)]],
    );
    let mut table = kv_table(&catalog, "test", "t");
    admin_check::check_table(&mut table, None, &RowDecodeContext::for_query(&ctx))
        .expect("admin check table t");
    admin_check::check_table(&mut table, Some("b"), &RowDecodeContext::for_query(&ctx))
        .expect("admin check index t b");
}

// --- TestUniqueKeyNullValueClusterIndex (pkg/ddl/db_integration_test.go:115) ---
//
// Same NULL rule against a CLUSTERED composite-PK table (`primary key (a,
// b)` over varchar+float): a unique index over `c` must admit two NULL rows.
// Go runs this in a dedicated database `unique_null_val`; this tier's tests
// have no CREATE DATABASE runner, so the table lives in `test` — the schema
// name carries no part of the contract.
#[test]
fn unique_key_null_value_cluster_index_unique_index_allows_nulls() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a varchar(10), b float, c varchar(255), primary key (a, b))",
        &mut catalog,
    )
    .unwrap();
    let ctx = StmtContext::for_query();
    run_insert_on("insert into t values ('1', 1, NULL)", &mut catalog, &ctx).unwrap();
    run_insert_on("insert into t values ('2', 2, NULL)", &mut catalog, &ctx).unwrap();
    ddl::run_alter_table_in("alter table t add unique index c(c)", &mut catalog, "test", &ctx)
        .unwrap();
    assert_eq!(
        run_select_on("select count(*) from t use index(c)", &catalog, &ctx).unwrap(),
        vec![vec![Datum::Int(2)]],
    );
    let mut table = kv_table(&catalog, "test", "t");
    admin_check::check_table(&mut table, None, &RowDecodeContext::for_query(&ctx))
        .expect("admin check table t");
    admin_check::check_table(&mut table, Some("c"), &RowDecodeContext::for_query(&ctx))
        .expect("admin check index t c");
}

// --- TestModifyColumnAfterAddIndex (pkg/ddl/db_integration_test.go:133) ---
//
// Issue 5134: `change column city city varchar(50)` widens a clustered
// VARCHAR(2) key in place, and rows that fit the new width insert fine.
#[test]
fn modify_column_after_add_index_widens_varchar_key() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table city (city VARCHAR(2) KEY)", &mut catalog).unwrap();
    let ctx = StmtContext::for_query();
    ddl::run_alter_table_in(
        "alter table city change column city city varchar(50)",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "insert into city values (\"abc\"), (\"abd\")",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(
        rows_text(&run_select_on("select city from city order by city", &catalog, &ctx).unwrap()),
        [["abc"], ["abd"]],
    );
}

// --- TestIssue2293 (pkg/ddl/db_integration_test.go:166) ---
//
// `add b int not null default 'a'` must be refused with
// ErrInvalidDefault (1067), and the table must keep working.
#[test]
fn issue_2293_invalid_string_default_for_int_rejected_1067() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t_issue_2293 (a int)", &mut catalog).unwrap();
    let ctx = StmtContext::for_query();
    assert!(matches!(
        ddl::run_alter_table_in(
            "alter table t_issue_2293 add b int not null default 'a'",
            &mut catalog,
            "test",
            &ctx
        ),
        Err(tidb_executor::DriverError::InvalidDefault(column)) if column == "b",
    ));
    run_insert_on("insert into t_issue_2293 value(1)", &mut catalog, &ctx).unwrap();
    assert_eq!(
        rows_text(
            &run_select_on("select * from t_issue_2293", &catalog, &ctx).unwrap()
        ),
        [["1"]],
    );
}

// --- TestIssue19229 (pkg/ddl/db_integration_test.go:177) ---
//
// Bad ENUM/SET inserts report Go's WarnDataTruncated (1265), including the
// numeric `-1` SET conversion whose failed unsigned cast must not be mistaken
// for the valid zero SET.
#[test]
fn issue_19229_enum_set_bad_values_truncate_1265() {
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE enumt (type enum('a', 'b') )", &mut catalog).unwrap();
    let ctx = StmtContext::for_query();
    for sql in ["insert into enumt values('xxx')", "insert into enumt values(-1)"] {
        assert!(
            matches!(
                run_insert_on(sql, &mut catalog, &ctx),
                Err(tidb_executor::DriverError::DataTruncatedAtRow { column, row: 1 }) if column == "type"
            ),
            "{sql}"
        );
    }
    run_create_table_on("CREATE TABLE sett (type set('a', 'b') )", &mut catalog).unwrap();
    assert!(matches!(
        run_insert_on("insert into sett values('xxx')", &mut catalog, &ctx),
        Err(tidb_executor::DriverError::DataTruncatedAtRow { column, row: 1 }) if column == "type"
    ));
    assert!(matches!(
        run_insert_on("insert into sett values(-1)", &mut catalog, &ctx),
        Err(tidb_executor::DriverError::DataTruncatedAtRow { column, row: 1 }) if column == "type"
    ));
}

// --- TestIndexLength (pkg/ddl/db_integration_test.go:201) ---
//
// Zero-fractional (flen 0) columns take indexes; TEXT/BLOB prefix indexes
// sit exactly at the 3072-byte limit (768 chars × 4 bytes utf8mb4, 3072 × 1
// ascii, 3072 blob bytes) both inline in CREATE TABLE and through ALTER ADD
// INDEX. (The Go test's schematracker plumbing is harness-side and has no
// counterpart here; every statement it runs is run below.)
#[test]
fn index_length_zero_flen_and_prefix_limits() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table idx_len(a int(0), b timestamp(0), c datetime(0), d time(0), f float(0), g decimal(0))",
        &mut catalog,
    )
    .unwrap();
    let ctx = StmtContext::for_query();
    for sql in [
        "create index idx on idx_len(a)",
        "alter table idx_len add index idxa(a)",
        "create index idx1 on idx_len(b)",
        "alter table idx_len add index idxb(b)",
        "create index idx2 on idx_len(c)",
        "alter table idx_len add index idxc(c)",
        "create index idx3 on idx_len(d)",
        "alter table idx_len add index idxd(d)",
        "create index idx4 on idx_len(f)",
        "alter table idx_len add index idxf(f)",
        "create index idx5 on idx_len(g)",
        "alter table idx_len add index idxg(g)",
    ] {
        ddl::run_create_index_in(sql, &mut catalog, "test", &ctx)
            .or_else(|_| ddl::run_alter_table_in(sql, &mut catalog, "test", &ctx))
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    run_create_table_on(
        "create table idx_len1(a int(0), b timestamp(0), c datetime(0), d time(0), f float(0), g decimal(0), index(a), index(b), index(c), index(d), index(f), index(g))",
        &mut catalog,
    )
    .unwrap();

    // Prefixes at exactly the limit, inline and via ALTER.
    let mut catalog2 = Catalog::default();
    run_create_table_on(
        "create table idx_len(a text, b text charset ascii, c blob, index(a(768)), index (b(3072)), index (c(3072)))",
        &mut catalog2,
    )
    .unwrap();
    let mut catalog3 = Catalog::default();
    run_create_table_on(
        "create table idx_len(a text, b text charset ascii, c blob)",
        &mut catalog3,
    )
    .unwrap();
    for sql in [
        "alter table idx_len add index (a(768))",
        "alter table idx_len add index (b(3072))",
        "alter table idx_len add index (c(3072))",
    ] {
        ddl::run_alter_table_in(sql, &mut catalog3, "test", &ctx)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
}

// --- TestIssue2858And2717 (pkg/ddl/db_integration_test.go:238) ---
//
// BIT(64) defaults/inserts round-trip (`select a+0` = 0, 100, 12592, 0;
// `'\0'` insertion is the all-zero row), `ALTER COLUMN SET DEFAULT` accepts
// both the bit and the hex spellings, and an INT default 0x123 materializes
// 291 with hex literal inserts 123 and 0x321 = 801.
#[test]
fn issue_2858_and_2717_bit_and_hex_defaults() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t_issue_2858_bit (a bit(64) default b'0')", &mut catalog)
        .unwrap();
    let ctx = StmtContext::for_query();
    run_insert_on("insert into t_issue_2858_bit value ()", &mut catalog, &ctx).unwrap();
    run_insert_on(
        "insert into t_issue_2858_bit values (100), ('10'), ('\\0')",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(
        rows_text(
            &run_select_on("select a+0 from t_issue_2858_bit", &catalog, &ctx).unwrap()
        ),
        [["0"], ["100"], ["12592"], ["0"]],
    );
    ddl::run_alter_table_in(
        "alter table t_issue_2858_bit alter column a set default '\\0'",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();

    run_create_table_on("create table t_issue_2858_hex (a int default 0x123)", &mut catalog)
        .unwrap();
    run_insert_on("insert into t_issue_2858_hex value ()", &mut catalog, &ctx).unwrap();
    run_insert_on(
        "insert into t_issue_2858_hex values (123), (0x321)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(
        rows_text(
            &run_select_on("select a from t_issue_2858_hex", &catalog, &ctx).unwrap()
        ),
        [["291"], ["123"], ["801"]],
    );
    ddl::run_alter_table_in(
        "alter table t_issue_2858_hex alter column a set default 0x321",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
}

// --- TestIssue4432 (pkg/ddl/db_integration_test.go:257) ---
//
// Every spelling of a BIT(10) default — string 'a', hex 0x61, decimal 97,
// binary 0b1100001 — materializes the same two bytes \x00a.
#[test]
fn issue_4432_bit_default_spellings() {
    let ctx = StmtContext::for_query();
    for def in [
        "create table tx (col bit(10) default 'a')",
        "create table tx (col bit(10) default 0x61)",
        "create table tx (col bit(10) default 97)",
        "create table tx (col bit(10) default 0b1100001)",
    ] {
        let mut catalog = Catalog::default();
        run_create_table_on(def, &mut catalog).unwrap();
        run_insert_on("insert into tx value ()", &mut catalog, &ctx).unwrap();
        let rows = run_select_on("select * from tx", &catalog, &ctx).unwrap();
        assert_eq!(rows.len(), 1, "{def}");
        let Datum::Bit(literal) = &rows[0][0] else {
            panic!("{def}: expected a bit datum, got {:?}", rows[0][0]);
        };
        assert_eq!(literal.as_bytes(), &[0, 97], "{def} \\x00a");
    }
}

// --- TestIssue5092 (pkg/ddl/db_integration_test.go:284) ---
//
// The add/drop-column grammar guards, and the settled row shape after
// FIRST/AFTER placements. PORTED: grouped `add column (b int, c int)`,
// per-spec FIRST/AFTER positions (row order verified through `select *`),
// the second-half defaults row `0 1 2 22 3 33 4`, duplicate drop = 1091,
// dropping every column (mixed IF EXISTS) = 1090, guarded drops succeed.
//
// KNOWN DIVERGENCES, not asserted (all captured during this port):
// * `add column if not exists (...)` is not consulted by this tier's ADD
//   COLUMN action, so Go's Note-suppressed forms
//   (`if not exists (b int, c int)` over existing b/c; the `(d int, e int)`
//   grouped spelling) error 1060 here, and Go's 8200 for the MIXED
//   `add column dd int, add column if not exists dd int` surfaces as plain
//   1060 too;
// * consequently the final SHOW CREATE TABLE column list (with `ff`) is not
//   reproduced; the Go-visible row ORDER the test pins is, via `select *`.
#[test]
fn issue_5092_add_drop_column_positions_and_guards() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t_issue_5092 (a int)", &mut catalog).unwrap();
    let ctx = StmtContext::for_query();
    ddl::run_alter_table_in(
        "alter table t_issue_5092 add column (b int, c int)",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    ddl::run_alter_table_in(
        "alter table t_issue_5092 add column b1 int after b, add column c1 int after c",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    ddl::run_alter_table_in(
        "alter table t_issue_5092 add column d int after b, add column e int first, \
         add column f int after c1, add column g int, add column h int first",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    let names: Vec<String> = match catalog.table_in("test", "t_issue_5092") {
        Some(TableEntry::Kv(table)) => {
            table.columns.iter().map(|c| c.name.clone()).collect()
        }
        _ => panic!(),
    };
    assert_eq!(
        names,
        ["h", "e", "a", "b", "d", "b1", "c", "c1", "f", "g"],
        "Go's SHOW CREATE TABLE column order"
    );
    assert!(matches!(
        ddl::run_alter_table_in(
            "alter table t_issue_5092 add column if not exists d int, add column d int",
            &mut catalog,
            "test",
            &ctx
        ),
        Err(tidb_executor::DriverError::DuplicateColumnName(_)),
    ), "Go: [schema:1060] duplicate column, exactly this statement errors");

    // The defaults half: every new column settles its default and row order
    // follows the placements, "0 1 2 22 3 33 4".
    let mut catalog2 = Catalog::default();
    run_create_table_on("create table t_issue_5092 (a int default 1)", &mut catalog2).unwrap();
    ddl::run_alter_table_in(
        "alter table t_issue_5092 add column (b int default 2, c int default 3)",
        &mut catalog2,
        "test",
        &ctx,
    )
    .unwrap();
    ddl::run_alter_table_in(
        "alter table t_issue_5092 add column b1 int default 22 after b, add column c1 int default 33 after c",
        &mut catalog2,
        "test",
        &ctx,
    )
    .unwrap();
    run_insert_on("insert into t_issue_5092 value ()", &mut catalog2, &ctx).unwrap();
    assert_eq!(
        rows_text(
            &run_select_on("select * from t_issue_5092", &catalog2, &ctx).unwrap()
        ),
        [["1", "2", "22", "3", "33"]],
    );
    ddl::run_alter_table_in(
        "alter table t_issue_5092 add column d int default 4 after c1, add column aa int default 0 first",
        &mut catalog2,
        "test",
        &ctx,
    )
    .unwrap();
    assert_eq!(
        rows_text(
            &run_select_on("select * from t_issue_5092", &catalog2, &ctx).unwrap()
        ),
        [["0", "1", "2", "22", "3", "33", "4"]],
    );

    // The drop guards, on a fresh table matching Go's state.
    let mut catalog3 = Catalog::default();
    run_create_table_on("create table t_issue_5092 (a int)", &mut catalog3).unwrap();
    ddl::run_alter_table_in(
        "alter table t_issue_5092 add column (b int, c int)",
        &mut catalog3,
        "test",
        &ctx,
    )
    .unwrap();
    assert!(matches!(
        ddl::run_alter_table_in(
            "alter table t_issue_5092 drop column c, drop column c",
            &mut catalog3,
            "test",
            &ctx
        ),
        Err(tidb_executor::DriverError::UnknownColumnInAlter(_))
    ), "Go: ErrCantDropFieldOrKey (1091)");
    ddl::run_alter_table_in(
        "alter table t_issue_5092 drop column if exists b,drop column if exists c",
        &mut catalog3,
        "test",
        &ctx,
    )
    .unwrap();
    assert!(matches!(
        ddl::run_alter_table_in(
            "alter table t_issue_5092 drop column g, drop column d",
            &mut catalog3,
            "test",
            &ctx
        ),
        Err(tidb_executor::DriverError::UnknownColumnInAlter(_))
    ), "Go: ErrCantDropFieldOrKey (1091)");

    // Dropping every column through a mixed IF EXISTS list is
    // ErrCantRemoveAllFields (1090).
    let mut catalog4 = Catalog::default();
    run_create_table_on("create table t_issue_5092 (a int)", &mut catalog4).unwrap();
    ddl::run_alter_table_in(
        "alter table t_issue_5092 add column (b int, c int)",
        &mut catalog4,
        "test",
        &ctx,
    )
    .unwrap();
    assert!(matches!(
        ddl::run_alter_table_in(
            "alter table t_issue_5092 drop column if exists a, drop column b, drop column c",
            &mut catalog4,
            "test",
            &ctx
        ),
        Err(tidb_executor::DriverError::CannotDropOnlyColumn { .. })
    ), "Go: ErrCantRemoveAllFields (1090)");
}

// --- TestTableDDLWithTimeType (pkg/ddl/db_integration_test.go:373) ---
//
// TIME/DATETIME/TIMESTAMP(7) is refused with ErrTooBigPrecision (1426)
// through CREATE TABLE and every ALTER spelling, `time(-1)` is refused by
// the parser, and `datetime(0)` is a legal CHANGE target.
#[test]
fn table_ddl_with_time_type_too_big_precision_1426() {
    let mut catalog = Catalog::default();
    for sql in [
        "create table t (a time(7))",
        "create table t (a datetime(7))",
        "create table t (a timestamp(7))",
    ] {
        assert!(
            matches!(
                run_create_table_on(sql, &mut catalog),
                Err(tidb_executor::DriverError::TooBigPrecision { precision: 7, maximum: 6, .. })
            ),
            "{sql}"
        );
    }
    assert!(
        run_create_table_on("create table t (a time(-1))", &mut catalog).is_err(),
        "time(-1) errors in Go too (require.Error)"
    );
    run_create_table_on("create table t (a datetime)", &mut catalog).unwrap();
    let ctx = StmtContext::for_query();
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
        assert!(
            matches!(
                ddl::run_alter_table_in(sql, &mut catalog, "test", &ctx),
                Err(tidb_executor::DriverError::TooBigPrecision { precision: 7, maximum: 6, .. })
            ),
            "{sql}"
        );
    }
    ddl::run_alter_table_in(
        "alter table t change column a aa datetime(0)",
        &mut catalog,
        "test",
        &ctx,
    )
    .expect("datetime(0) is a legal target");
}

// --- TestNullGeneratedColumn (pkg/ddl/db_integration_test.go:422) ---
//
// A VIRTUAL generated column over two NULL-able columns indexes cleanly
// when every row is written with no columns at all.
#[test]
fn null_generated_column_indexed_over_default_nulls() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE t (a int(11) DEFAULT NULL, b int(11) DEFAULT NULL, \
         c int(11) GENERATED ALWAYS AS (a + b) VIRTUAL, h varchar(10) DEFAULT NULL, \
         m int(11) DEFAULT NULL)",
        &mut catalog,
    )
    .unwrap();
    let ctx = StmtContext::for_query();
    run_insert_on("insert into t values()", &mut catalog, &ctx).unwrap();
    ddl::run_alter_table_in("alter table t add index idx_c(c)", &mut catalog, "test", &ctx)
        .unwrap();
    let mut table = kv_table(&catalog, "test", "t");
    admin_check::check_table(&mut table, None, &RowDecodeContext::for_query(&ctx))
        .expect("the NULL-backed generated index is consistent");
}

// --- TestDependedGeneratedColumnPrior2GeneratedColumn (pkg/ddl/db_integration_test.go:442) ---
//
// Order checks for a generated column added by ALTER. PORTED: an unknown
// dependency anywhere is ErrBadField (1054), the legal AFTER position
// commits, and the grouped spelling adds too.
// KNOWN DIVERGENCE, not asserted: Go refuses `add column d int as (c+1)
// FIRST` with ErrGeneratedColumnNonPrior (3107) — existence is checked
// against the WHOLE table before prior-ness — while this tier resolves
// against the columns PRECEDING the new one and reports 1054 for `c`
// instead. Both refuse; the codes differ. See the receipt.
#[test]
fn depended_generated_column_prior2_generated_column_checks() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE t (a int(11) DEFAULT NULL, b int(11) GENERATED ALWAYS AS (a + 1) VIRTUAL, \
         c int(11) GENERATED ALWAYS AS (b + 1) VIRTUAL)",
        &mut catalog,
    )
    .unwrap();
    let ctx = StmtContext::for_query();
    assert!(
        matches!(
            ddl::run_alter_table_in(
                "alter table t add column d int as (c + f + 1) first",
                &mut catalog,
                "test",
                &ctx
            ),
            Err(tidb_executor::DriverError::UnknownColumnInClause { .. }),
        ),
        "Go: ErrBadField, checked before the prior-order rule"
    );
    ddl::run_alter_table_in(
        "alter table t add column d int as (c+1) after c",
        &mut catalog,
        "test",
        &ctx,
    )
    .expect("the correct AFTER position commits");
    ddl::run_alter_table_in(
        "alter table t add column(e int as (c+1))",
        &mut catalog,
        "test",
        &ctx,
    )
    .expect("the grouped spelling commits");
}

// --- TestIndexOnMultipleGeneratedColumn (pkg/ddl/db_integration_test.go:661) ---
//
// An index over a generated column that depends on another generated column:
// builds, answers `c > 1`, and USE/IGNORE INDEX agree.
#[test]
fn index_on_multiple_generated_column_base() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int, b int as (a + 1), c int as (b + 1))",
        &mut catalog,
    )
    .unwrap();
    let ctx = StmtContext::for_query();
    run_insert_on("insert into t (a) values (1)", &mut catalog, &ctx).unwrap();
    ddl::run_create_index_in("create index idx on t (c)", &mut catalog, "test", &ctx).unwrap();
    assert_eq!(
        run_select_on("select * from t where c > 1", &catalog, &ctx).unwrap(),
        vec![vec![Datum::Int(1), Datum::Int(2), Datum::Int(3)]],
    );
    let via_index = run_select_on("select * from t use index(idx) where c > 1", &catalog, &ctx)
        .unwrap();
    let via_scan = run_select_on(
        "select * from t ignore index(idx) where c > 1",
        &catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(via_index, via_scan);
    let mut table = kv_table(&catalog, "test", "t");
    admin_check::check_table(&mut table, None, &RowDecodeContext::for_query(&ctx))
        .expect("admin check table t");
}

// --- TestIndexOnMultipleGeneratedColumn1 (pkg/ddl/db_integration_test.go:678) ---
//
// A three-level generated chain, indexed at its end.
#[test]
fn index_on_multiple_generated_column1_three_level_chain() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int, b int as (a + 1), c int as (b + 1), d int as (c + 1))",
        &mut catalog,
    )
    .unwrap();
    let ctx = StmtContext::for_query();
    run_insert_on("insert into t (a) values (1)", &mut catalog, &ctx).unwrap();
    ddl::run_alter_table_in("alter table t add index idx (d)", &mut catalog, "test", &ctx)
        .unwrap();
    assert_eq!(
        rows_text(&run_select_on("select * from t where d > 2", &catalog, &ctx).unwrap()),
        [["1", "2", "3", "4"]],
    );
    assert_eq!(
        run_select_on("select * from t use index(idx) where d > 2", &catalog, &ctx).unwrap(),
        run_select_on("select * from t ignore index(idx) where d > 2", &catalog, &ctx).unwrap(),
    );
}

// --- TestIndexOnMultipleGeneratedColumn2 (pkg/ddl/db_integration_test.go:695) ---
//
// Mixed numeric/text generated chain: bigint → decimal → varchar → float
// with `length()` in the last link; row "1 2 4 25".
#[test]
fn index_on_multiple_generated_column2_mixed_numeric_types() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a bigint, b decimal as (a+1), c varchar(20) as (b*2), d float as (a*23+b-1+length(c)))",
        &mut catalog,
    )
    .unwrap();
    let ctx = StmtContext::for_query();
    run_insert_on("insert into t (a) values (1)", &mut catalog, &ctx).unwrap();
    ddl::run_alter_table_in("alter table t add index idx (d)", &mut catalog, "test", &ctx)
        .unwrap();
    let rows = run_select_on("select * from t where d > 2", &catalog, &ctx).unwrap();
    assert_eq!(rows.len(), 1);
    assert!(matches!(&rows[0][0], Datum::Int(1)));
    assert!(matches!(&rows[0][3], Datum::Float32(value) if *value == 25.0));
    assert_eq!(
        run_select_on("select * from t use index(idx) where d > 2", &catalog, &ctx).unwrap(),
        run_select_on("select * from t ignore index(idx) where d > 2", &catalog, &ctx).unwrap(),
    );
}

// --- TestIndexOnMultipleGeneratedColumn3 (pkg/ddl/db_integration_test.go:712) ---
//
// String-function chain: `length(a)+123`, `right(a, 2)`, and the float
// `b+b-7+1-3+3*ASCII(c)` = 131+131-7+1-3+3*108 = 577; row
// "adorable 131 le 577".
#[test]
fn index_on_multiple_generated_column3_string_functions() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a varchar(10), b float as (length(a)+123), c varchar(20) as (right(a, 2)), d float as (b+b-7+1-3+3*ASCII(c)))",
        &mut catalog,
    )
    .unwrap();
    let ctx = StmtContext::for_query();
    run_insert_on("insert into t (a) values ('adorable')", &mut catalog, &ctx).unwrap();
    ddl::run_alter_table_in("alter table t add index idx (d)", &mut catalog, "test", &ctx)
        .unwrap();
    let rows = run_select_on("select * from t where d > 2", &catalog, &ctx).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(datum_text(&rows[0][0]), "adorable");
    assert!(matches!(&rows[0][1], Datum::Float32(value) if *value == 131.0));
    assert_eq!(datum_text(&rows[0][2]), "le");
    assert!(matches!(&rows[0][3], Datum::Float32(value) if *value == 577.0));
    assert_eq!(
        run_select_on("select * from t use index(idx) where d > 2", &catalog, &ctx).unwrap(),
        run_select_on("select * from t ignore index(idx) where d > 2", &catalog, &ctx).unwrap(),
    );
}

// --- TestIndexOnMultipleGeneratedColumn4 (pkg/ddl/db_integration_test.go:729) ---
//
// A self-feeding chain a → b(a) → c(a+b) → d(a+b+c) → e(a+b+c+d); row
// "1 1 2 4 8" with d float indexed.
#[test]
fn index_on_multiple_generated_column4_self_chain() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a bigint, b decimal as (a), c int(10) as (a+b), d float as (a+b+c), e decimal as (a+b+c+d))",
        &mut catalog,
    )
    .unwrap();
    let ctx = StmtContext::for_query();
    run_insert_on("insert into t (a) values (1)", &mut catalog, &ctx).unwrap();
    ddl::run_alter_table_in("alter table t add index idx (d)", &mut catalog, "test", &ctx)
        .unwrap();
    let rows = run_select_on("select * from t where d > 2", &catalog, &ctx).unwrap();
    assert_eq!(rows.len(), 1);
    assert!(matches!(&rows[0][0], Datum::Int(1)));
    assert!(matches!(&rows[0][3], Datum::Float32(value) if *value == 4.0));
    assert_eq!(
        run_select_on("select * from t use index(idx) where d > 2", &catalog, &ctx).unwrap(),
        run_select_on("select * from t ignore index(idx) where d > 2", &catalog, &ctx).unwrap(),
    );
}

// --- TestIndexOnMultipleGeneratedColumn5 (pkg/ddl/db_integration_test.go:746) ---
//
// VIRTUAL spellings throughout, indexes added across two ALTERs, and a
// generated column added to a table that already carries generated indexes;
// row "1 2 3 4".
#[test]
fn index_on_multiple_generated_column5_virtual_alter_add_index() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t(a bigint, b bigint as (a+1) virtual, c bigint as (b+1) virtual)",
        &mut catalog,
    )
    .unwrap();
    let ctx = StmtContext::for_query();
    ddl::run_alter_table_in("alter table t add index idx_b(b)", &mut catalog, "test", &ctx)
        .unwrap();
    ddl::run_alter_table_in("alter table t add index idx_c(c)", &mut catalog, "test", &ctx)
        .unwrap();
    run_insert_on("insert into t(a) values(1)", &mut catalog, &ctx).unwrap();
    ddl::run_alter_table_in(
        "alter table t add column(d bigint as (c+1) virtual)",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    ddl::run_alter_table_in("alter table t add index idx_d(d)", &mut catalog, "test", &ctx)
        .unwrap();
    assert_eq!(
        rows_text(&run_select_on("select * from t where d > 2", &catalog, &ctx).unwrap()),
        [["1", "2", "3", "4"]],
    );
    assert_eq!(
        run_select_on("select * from t use index(idx_d) where d > 2", &catalog, &ctx).unwrap(),
        run_select_on("select * from t ignore index(idx_d) where d > 2", &catalog, &ctx).unwrap(),
    );
}

// --- TestCaseInsensitiveCharsetAndCollate (pkg/ddl/db_integration_test.go:766) ---
//
// Uppercase charset/collation spellings are accepted and stored lowercased
// (all five t/t1..t4 tables, then t5's stored column charset). The
// version-gated halves — TableInfoVersion2 re-lowercasing and
// TableInfoVersion3 preserving the written case — need direct meta mutation
// through a Mutator this tier does not expose (documented below).
#[test]
fn case_insensitive_charset_and_collate_stored_lowercase() {
    let mut catalog = Catalog::default();
    for sql in [
        "create table t(id int) ENGINE=InnoDB DEFAULT CHARSET=UTF8 COLLATE=UTF8_BIN",
        "create table t1(id int) ENGINE=InnoDB DEFAULT CHARSET=UTF8 COLLATE=uTF8_BIN",
        "create table t2(id int) ENGINE=InnoDB DEFAULT CHARSET=Utf8 COLLATE=utf8_BIN",
        "create table t3(id int) ENGINE=InnoDB DEFAULT CHARSET=Utf8mb4 COLLATE=utf8MB4_BIN",
        "create table t4(id int) ENGINE=InnoDB DEFAULT CHARSET=Utf8mb4 COLLATE=utf8MB4_general_ci",
        "create table t5(a varchar(20)) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4 COLLATE=UTF8MB4_GENERAL_CI",
    ] {
        run_create_table_on(sql, &mut catalog)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    // Go: tblInfo.Charset == "utf8mb4" and the column's charset lowercased
    // after the FIRST load; here the created table carries them directly.
    let table = kv_table(&catalog, "test", "t5");
    let first = column(&catalog, "test", "t5", "a");
    assert_eq!(first.field_type.charset_name(), "utf8mb4");
    assert_eq!(first.field_type.collation_name(), "utf8mb4_general_ci");
    assert_eq!(table.name, "t5");
}

// --- TestZeroFillCreateTable (pkg/ddl/db_integration_test.go:824) ---
//
// `y year` carries the YEAR type with the UNSIGNED flag (plus PRIMARY KEY's
// NOT NULL), and `z tinyint(10) zerofill` carries UNSIGNED — the flags
// Go reads off `model.ColumnInfo`.
#[test]
fn zero_fill_create_table_flags() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table abc(y year, z tinyint(10) zerofill, primary key(y))",
        &mut catalog,
    )
    .unwrap();
    let year_col = column(&catalog, "test", "abc", "y");
    assert_eq!(year_col.field_type.code(), FieldTypeCode::Year);
    assert!(
        year_col.field_type.has_flag(FieldTypeFlags::UNSIGNED),
        "y year is unsigned"
    );
    let z_col = column(&catalog, "test", "abc", "z");
    assert!(
        z_col.field_type.has_flag(FieldTypeFlags::UNSIGNED),
        "tinyint(10) zerofill is unsigned"
    );
}

// --- TestBitDefaultValue (pkg/ddl/db_integration_test.go:851) ---
//
// BIT defaults materialize as the BIT bytes Go prints: bit(10) default 250
// reads `bin(c1)` = 11111010; `bit(16) default b'1100110111001'` reads
// \x19\xb9; updating it stores \x38\x07; the bit(1) MODIFY COLUMN default
// change does NOT rewrite the existing row (still \x00); a bare `bit`
// column accepts NULL; and the keyword-heavy testalltypes tables create.
#[test]
fn bit_default_value_roundtrip() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t_bit (c1 bit(10) default 250, c2 int)", &mut catalog)
        .unwrap();
    let ctx = StmtContext::for_query();
    run_insert_on("insert into t_bit set c2=1", &mut catalog, &ctx).unwrap();
    let rows = run_select_on("select bin(c1),c2 from t_bit", &catalog, &ctx).unwrap();
    assert_eq!(datum_text(&rows[0][0]), "11111010");
    assert!(matches!(&rows[0][1], Datum::Int(1)));

    let mut catalog2 = Catalog::default();
    run_create_table_on("create table t_bit (a int)", &mut catalog2).unwrap();
    run_insert_on("insert into t_bit value (1)", &mut catalog2, &ctx).unwrap();
    ddl::run_alter_table_in(
        "alter table t_bit add column c bit(16) null default b'1100110111001'",
        &mut catalog2,
        "test",
        &ctx,
    )
    .unwrap();
    assert_bit_bytes(&run_select_on("select c from t_bit", &catalog2, &ctx).unwrap()[0][0], &[25, 185]);
    run_update_on(
        "update t_bit set c = b'11100000000111'",
        &mut catalog2,
        &ctx,
    )
    .unwrap();
    assert_bit_bytes(&run_select_on("select c from t_bit", &catalog2, &ctx).unwrap()[0][0], &[56, 7]);

    // MODIFY COLUMN's new default applies to NEW rows; the row written
    // before it still reads \x00 (Go's select shows "\x00" for it).
    let mut catalog3 = Catalog::default();
    run_create_table_on("create table t_bit (a int)", &mut catalog3).unwrap();
    run_insert_on("insert into t_bit value (1)", &mut catalog3, &ctx).unwrap();
    ddl::run_alter_table_in(
        "alter table t_bit add column b bit(1) default b'0'",
        &mut catalog3,
        "test",
        &ctx,
    )
    .unwrap();
    ddl::run_alter_table_in(
        "alter table t_bit modify column b bit(1) default b'1'",
        &mut catalog3,
        "test",
        &ctx,
    )
    .unwrap();
    assert_bit_bytes(&run_select_on("select b from t_bit", &catalog3, &ctx).unwrap()[0][0], &[0]);

    // A bare `bit` column accepts an explicit NULL.
    let mut catalog4 = Catalog::default();
    run_create_table_on("create table t_bit (a bit)", &mut catalog4).unwrap();
    run_insert_on("insert into t_bit values (null)", &mut catalog4, &ctx).unwrap();
    assert_eq!(
        run_select_on("select count(*) from t_bit where a is null", &catalog4, &ctx).unwrap(),
        vec![vec![Datum::Int(1)]],
    );

    // The all-types spellings Go creates at the end of the test.
    run_create_table_on(
        "create table testalltypes1 (field_1 bit default 1, field_2 tinyint null default null)",
        &mut catalog4,
    )
    .unwrap();
    run_create_table_on(
        "create table testalltypes2 (field_1 bit null default null, \
         field_2 tinyint null default null, field_3 tinyint unsigned null default null, \
         field_4 bigint null default null, field_5 bigint unsigned null default null, \
         field_6 mediumblob null default null, field_7 longblob null default null, \
         field_8 blob null default null, field_9 tinyblob null default null, \
         field_10 varbinary(255) null default null, field_11 binary(255) null default null, \
         field_12 mediumtext null default null, field_13 longtext null default null, \
         field_14 text null default null, field_15 tinytext null default null, \
         field_16 char(255) null default null, field_17 numeric null default null, \
         field_18 decimal null default null, field_19 integer null default null, \
         field_20 integer unsigned null default null, field_21 int null default null, \
         field_22 int unsigned null default null, field_23 mediumint null default null, \
         field_24 mediumint unsigned null default null, field_25 smallint null default null, \
         field_26 smallint unsigned null default null, field_27 float null default null, \
         field_28 double null default null, field_29 double precision null default null, \
         field_30 real null default null, field_31 varchar(255) null default null, \
         field_32 date null default null, field_33 time null default null, \
         field_34 datetime null default null, field_35 timestamp null default null)",
        &mut catalog4,
    )
    .unwrap();
}

fn assert_bit_bytes(value: &Datum, expected: &[u8]) {
    match value {
        Datum::Bit(literal) => assert_eq!(literal.as_bytes(), expected),
        other => panic!("expected a bit datum, got {other:?}"),
    }
}

// --- TestModifyColumnOption (pkg/ddl/db_integration_test.go:626) ---
//
// The ACCEPTED half: CHANGE COLUMN rewrites a column's type through the
// sequence Go drives (int(11) → varchar(16) → varchar(10) → datetime →
// int(11) unsigned) and the same widening on a second table's char column.
// The coded clauses half (CHARACTER SET/COLLATE accepted; REFERENCES
// refused with "[ddl:8200]") is the #[ignore] documentary below.
#[test]
fn modify_column_option_change_type_sequence() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t1 (a int(11) default null)", &mut catalog).unwrap();
    run_create_table_on("create table t2 (b char, c int)", &mut catalog).unwrap();
    let ctx = StmtContext::for_query();
    for sql in [
        "alter table t1 change a a varchar(16)",
        "alter table t1 change a a varchar(10)",
        "alter table t1 change a a datetime",
        "alter table t1 change a a int(11) unsigned",
        "alter table t2 change b b int(11) unsigned",
    ] {
        ddl::run_alter_table_in(sql, &mut catalog, "test", &ctx)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
}

// --- go-parity-gap documentaries -------------------------------------------------

// go-parity-gap: needs the afterRunOneJobStep failpoint to strip a job's
// newest arg (simulating a v1→v2 job-version owner change mid-reorg), which
// is DDL job-args machinery this tier does not model
// (pkg/ddl/db_integration_test.go:143::TestModifyColumnOldColumnIDNotFound).
#[test]
#[ignore]
fn modify_column_old_column_id_not_found() {
    // Contract to restore: MODIFY a varchar(16) survives an owner change
    // whose job args lost their last element; the write-reorg phase still
    // completes.
}

// go-parity-gap: needs the afterWaitSchemaSynced failpoint to run a
// two-table UPDATE inside a schema state of the ADD COLUMN job — concurrency
// plus multi-table DML machinery outside this batch's tier
// (pkg/ddl/db_integration_test.go:398::TestUpdateMultipleTable).
#[test]
#[ignore]
fn update_multiple_table_mid_ddl() {
    // Contract to restore: while `alter table t1 add column c3 bigint
    // default 9` runs, an UPDATE over t1,t2 lands in WriteOnly state and the
    // final t1 rows read "8 1 9", "8 2 9".
}

// go-parity-gap: every ALTER TABLE charset/convert form this test drives is
// refused by this tier as "this ALTER TABLE table option is not supported
// yet" (captured), and the empty-charset meta mutation halves need a meta
// Mutator; the coded 8200/1253/1291/1391 contracts cannot be pinned
// (pkg/ddl/db_integration_test.go:468::TestChangingTableCharset).
#[test]
#[ignore]
fn changing_table_charset() {
    // Contract to restore: gbk refused 8200; '' charset 1291...; collate
    // mismatch 1253; convert-to updates table AND column charsets; column
    // charset survives a table-only `alter charset` (no column rewrite);
    // empty stored charsets backfill to the table default.
}

// go-parity-gap: MODIFY COLUMN's CHARACTER SET/COLLATE clause and the
// REFERENCES clause are refused as unsupported column options here (Go
// accepts the former and codes the latter [ddl:8200]); the accepted
// change-type sequence IS pinned above in
// modify_column_option_change_type_sequence
// (pkg/ddl/db_integration_test.go:626::TestModifyColumnOption).
#[test]
#[ignore]
fn modify_column_option_charset_and_references_clauses() {
    // Contract to restore: `modify column b char(1) character set utf8mb4
    // collate utf8mb4_general_ci` succeeds; `modify column c int references
    // t1(a)` errors whose text starts "[ddl:8200]".
}

// go-parity-gap: needs `config.GetGlobalConfig().TableColumnCountLimit`
// mutation (12 000 columns then a raised limit) and kv.ErrEntryTooLarge
// from a meta entry over the mem spec limit — config and txn-entry limits
// are outside this tier
// (pkg/ddl/db_integration_test.go:948::TestCreateTableTooLarge).
#[test]
#[ignore]
fn create_table_too_large() {
    // Contract to restore: 12 000 columns → ErrTooManyFields (1117); with
    // TableColumnCountLimit raised, the same CREATE fails with
    // kv.ErrEntryTooLarge when the meta entry is written.
}
