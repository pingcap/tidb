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

//! Behavioral tests retained from Go. Removed documentary entries are
//! indexed in rust/docs/parity/current-audit/comment-test-cleanup-validation.json.

use crate::support::testkit::TestKit;
use tidb_datatype::{Charset, Datum, FieldTypeCode, FieldTypeFlags};
use tidb_infoschema::TableEntry;
use tidb_table::kv_table::KvColumn;
use tidb_table::kv_table::KvTable;
use tidb_session::StmtResult;

/// Go `external.GetTableByName(t, tk, db, table).Meta()`: the storage-backed
/// table as the domain's current schema holds it.
fn kv_table(tk: &TestKit, database: &str, name: &str) -> KvTable {
    match tk.get_table_by_name(database, name) {
        TableEntry::Kv(table) => (*table).clone(),
        _ => panic!("expected a storage-backed table {database}.{name}"),
    }
}

/// One column of [`kv_table`], Go `tbl.Meta().Columns[i]`.
fn column(tk: &TestKit, database: &str, table_name: &str, name: &str) -> KvColumn {
    kv_table(tk, database, table_name)
        .columns()
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(name))
        .unwrap_or_else(|| panic!("no column {name} in {database}.{table_name}"))
        .clone()
}

/// The `show warnings` rows of the last statement carrying `code`.
fn warning_count(tk: &mut TestKit, code: &str) -> usize {
    tk.must_query("show warnings")
        .rows()
        .iter()
        .filter(|row| row[1] == code)
        .count()
}

/// Go `res := tk.MustQuery(a); tk.MustQuery(b).Check(res.Rows())`.
fn check_same_rows(tk: &mut TestKit, a: &str, b: &str) {
    let res = tk.must_query(a);
    assert_eq!(tk.must_query(b).rows(), res.rows(), "{a} vs {b}");
}

/// The raw bytes of the single BIT cell `sql` returns. Go compares them as
/// the string `testkit.Rows("\x19\xb9")`, which is not UTF-8, so they are
/// read off the datum.
fn bit_bytes(tk: &mut TestKit, sql: &str) -> Vec<u8> {
    match tk.session().run(sql) {
        Ok(StmtResult::Rows(rows)) => match &rows[0][0] {
            Datum::Bit(literal) => literal.as_bytes().to_vec(),
            other => panic!("{sql}: expected a bit datum, got {other:?}"),
        },
        other => panic!("{sql}: expected rows, got {other:?}"),
    }
}

// --- TestCreateTableIfNotExistsLike (pkg/ddl/db_integration_test.go:60) ---
//
// Go creates `ct` and `ct1`, then `create table if not exists ct like ct1`
// and checks the session carries a Note-level `ErrTableExists` warning, and
// that the same holds for a plain (non-LIKE) duplicate.
#[test]
fn create_table_if_not_exists_like_suppresses_and_copies() {
    let mut tk = TestKit::new();
    tk.must_exec("create table ct1(a bigint)");
    tk.must_exec("create table ct(a bigint)");
    // Duplicate create-table WITH the LIKE clause: suppressed, not an error;
    // the last warning is a Note-level ErrTableExists.
    tk.must_exec("create table if not exists ct like ct1");
    let warnings = tk.must_query("show warnings");
    let last = warnings.rows().last().expect("a warning");
    assert_eq!((last[0].as_str(), last[1].as_str()), ("Note", "1050"));
    // Plain duplicate CREATE TABLE: the last warning is ErrTableExists too.
    tk.must_exec("create table if not exists ct (a bigint)");
    let warnings = tk.must_query("show warnings");
    let last = warnings.rows().last().expect("a warning");
    assert_eq!(last[1], "1050");
    // The LIKE copy machinery itself (first creation) copies the structure.
    tk.must_exec("create table ct3 like ct1");
    let source = column(&tk, "test", "ct1", "a");
    let copy = column(&tk, "test", "ct3", "a");
    assert_eq!(source.field_type.code(), FieldTypeCode::LongLong);
    assert_eq!(copy.field_type.code(), source.field_type.code());
}

// --- TestCreateTableWithKeyWord (pkg/ddl/db_integration_test.go:87) ---
//
// Issue 9910: column names that are TiDB/system keywords must not break
// CREATE TABLE.
#[test]
fn create_table_with_keyword_column_names() {
    let mut tk = TestKit::new();
    tk.must_exec(
        "create table t1(pump varchar(20), drainer varchar(20), node_id varchar(20), \
         node_state varchar(20))",
    );
    for name in ["pump", "drainer", "node_id", "node_state"] {
        assert_eq!(
            column(&tk, "test", "t1", name).field_type.code(),
            FieldTypeCode::Varchar
        );
    }
}

// Go's `checkAndCreateNewColumn` demotes a duplicate ADD COLUMN guarded by
// IF NOT EXISTS to a Note 1060, and the grouped form continues with every
// non-duplicate column. This is the serial form of the owner behavior from
// `pkg/ddl/db_change_test.go:1556::TestDDLIfNotExists`.
#[test]
fn add_column_if_not_exists_skips_duplicates_and_continues_grouped_adds() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t(a int, b int)");

    let mut notes = 0;
    for sql in [
        "alter table t add column if not exists b int",
        "alter table t add column if not exists c int",
        "alter table t add column if not exists (b int, d int)",
    ] {
        tk.must_exec(sql);
        notes += warning_count(&mut tk, "1060");
    }

    let table = kv_table(&tk, "test", "t");
    assert_eq!(
        table
            .columns()
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b", "c", "d"],
    );
    assert_eq!(notes, 2, "each guarded duplicate files one Note 1060");
}

// --- TestUniqueKeyNullValue (pkg/ddl/db_integration_test.go:98) ---
//
// Two rows with NULL in `b`, then `add unique index b(b)`: NULLs never
// collide, the index counts both rows, and `admin check table` / `admin
// check index t b` stay clean.
#[test]
fn unique_key_null_value_unique_index_allows_multiple_nulls() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t(a int primary key, b varchar(255))");
    tk.must_exec("insert into t values(1, NULL)");
    tk.must_exec("insert into t values(2, NULL)");
    tk.must_exec("alter table t add unique index b(b)");
    tk.must_query("select count(*) from t use index(b)").check(["2"]);
    tk.must_exec("admin check table t");
    tk.must_exec("admin check index t b");
}

// --- TestUniqueKeyNullValueClusterIndex (pkg/ddl/db_integration_test.go:115) ---
//
// Same NULL rule against a composite-PK table (`primary key (a, b)` over
// varchar+float), in Go's dedicated database `unique_null_val`: a unique
// index over `c` must admit two NULL rows.
#[test]
fn unique_key_null_value_cluster_index_unique_index_allows_nulls() {
    let mut tk = TestKit::new();
    tk.must_exec("drop database if exists unique_null_val");
    tk.must_exec("create database unique_null_val");
    tk.must_exec("use unique_null_val");
    tk.must_exec("create table t (a varchar(10), b float, c varchar(255), primary key (a, b))");
    tk.must_exec("insert into t values ('1', 1, NULL)");
    tk.must_exec("insert into t values ('2', 2, NULL)");
    tk.must_exec("alter table t add unique index c(c)");
    tk.must_query("select count(*) from t use index(c)").check(["2"]);
    tk.must_exec("admin check table t");
    tk.must_exec("admin check index t c");
}

// --- TestModifyColumnAfterAddIndex (pkg/ddl/db_integration_test.go:133) ---
//
// Issue 5134: `change column city city varchar(50)` widens a clustered
// VARCHAR(2) key in place, and rows that fit the new width insert fine.
#[test]
fn modify_column_after_add_index_widens_varchar_key() {
    let mut tk = TestKit::new();
    tk.must_exec("create table city (city VARCHAR(2) KEY)");
    tk.must_exec("alter table city change column city city varchar(50)");
    tk.must_exec("insert into city values (\"abc\"), (\"abd\")");
    tk.must_query("select city from city order by city")
        .check(["abc", "abd"]);
}

// --- TestIssue2293 (pkg/ddl/db_integration_test.go:166) ---
//
// `add b int not null default 'a'` must be refused with
// ErrInvalidDefault (1067), and the table must keep working.
#[test]
fn issue_2293_invalid_string_default_for_int_rejected_1067() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t_issue_2293 (a int)");
    let error = tk.must_get_err_code("alter table t_issue_2293 add b int not null default 'a'", 1067);
    assert_eq!(error.message, "Invalid default value for 'b'");
    tk.must_exec("insert into t_issue_2293 value(1)");
    tk.must_query("select * from t_issue_2293").check(["1"]);
}

// --- TestIssue19229 (pkg/ddl/db_integration_test.go:177) ---
//
// Bad ENUM/SET inserts report Go's WarnDataTruncated (1265) under the
// testkit's default strict sql_mode, including the numeric `-1` SET
// conversion whose failed unsigned cast must not be mistaken for the valid
// zero SET.
#[test]
fn issue_19229_enum_set_bad_values_truncate_1265() {
    let mut tk = TestKit::new();
    tk.must_exec("CREATE TABLE enumt (type enum('a', 'b') )");
    for sql in ["insert into enumt values('xxx')", "insert into enumt values(-1)"] {
        let error = tk.must_get_err_code(sql, 1265);
        assert_eq!(error.message, "Data truncated for column 'type' at row 1", "{sql}");
    }
    tk.must_exec("drop table enumt");
    tk.must_exec("CREATE TABLE sett (type set('a', 'b') )");
    for sql in ["insert into sett values('xxx')", "insert into sett values(-1)"] {
        let error = tk.must_get_err_code(sql, 1265);
        assert_eq!(error.message, "Data truncated for column 'type' at row 1", "{sql}");
    }
    tk.must_exec("drop table sett");
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
    let mut tk = TestKit::new();
    for sql in [
        "create table idx_len(a int(0), b timestamp(0), c datetime(0), d time(0), f float(0), g decimal(0))",
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
        "create table idx_len1(a int(0), b timestamp(0), c datetime(0), d time(0), f float(0), g decimal(0), index(a), index(b), index(c), index(d), index(f), index(g))",
        // Prefixes at exactly the limit, inline and via ALTER.
        "drop table idx_len",
        "create table idx_len(a text, b text charset ascii, c blob, index(a(768)), index (b(3072)), index (c(3072)))",
        "drop table idx_len",
        "create table idx_len(a text, b text charset ascii, c blob)",
        "alter table idx_len add index (a(768))",
        "alter table idx_len add index (b(3072))",
        "alter table idx_len add index (c(3072))",
        "drop table idx_len",
    ] {
        tk.must_exec(sql);
    }
}

// Go's `checkIndexNameAndColumns` demotes a duplicate ordinary index name to
// Note 1061 when `CREATE INDEX` or `ALTER TABLE ... ADD INDEX` carries
// `IF NOT EXISTS`, without rebuilding the existing index.
#[test]
fn index_if_not_exists_skips_duplicate_create_and_alter() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t_index_guard (a int)");
    tk.must_exec("create index idx_a on t_index_guard (a)");
    let mut notes = 0;
    for sql in [
        "create index if not exists idx_a on t_index_guard (a)",
        "alter table t_index_guard add index if not exists idx_a (a)",
    ] {
        tk.must_exec(sql);
        notes += warning_count(&mut tk, "1061");
    }

    let table = kv_table(&tk, "test", "t_index_guard");
    assert_eq!(
        table
            .indexes()
            .iter()
            .filter(|index| index.name.eq_ignore_ascii_case("idx_a"))
            .count(),
        1,
        "guarded duplicate adds do not create another index"
    );
    assert_eq!(notes, 2, "CREATE and ALTER guards each file one Note 1061");
}

// --- TestIssue2858And2717 (pkg/ddl/db_integration_test.go:238) ---
//
// BIT(64) defaults/inserts round-trip (`select a+0` = 0, 100, 12592, 0;
// `'\0'` insertion is the all-zero row), `ALTER COLUMN SET DEFAULT` accepts
// both the bit and the hex spellings, and an INT default 0x123 materializes
// 291 with hex literal inserts 123 and 0x321 = 801.
#[test]
fn issue_2858_and_2717_bit_and_hex_defaults() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t_issue_2858_bit (a bit(64) default b'0')");
    tk.must_exec("insert into t_issue_2858_bit value ()");
    tk.must_exec("insert into t_issue_2858_bit values (100), ('10'), ('\\0')");
    tk.must_query("select a+0 from t_issue_2858_bit")
        .check(["0", "100", "12592", "0"]);
    tk.must_exec("alter table t_issue_2858_bit alter column a set default '\\0'");

    tk.must_exec("create table t_issue_2858_hex (a int default 0x123)");
    tk.must_exec("insert into t_issue_2858_hex value ()");
    tk.must_exec("insert into t_issue_2858_hex values (123), (0x321)");
    tk.must_query("select a from t_issue_2858_hex")
        .check(["291", "123", "801"]);
    tk.must_exec("alter table t_issue_2858_hex alter column a set default 0x321");
}

// --- TestIssue4432 (pkg/ddl/db_integration_test.go:257) ---
//
// Every spelling of a BIT(10) default — string 'a', hex 0x61, decimal 97,
// binary 0b1100001 — materializes the same two bytes \x00a.
#[test]
fn issue_4432_bit_default_spellings() {
    let mut tk = TestKit::new();
    for def in [
        "create table tx (col bit(10) default 'a')",
        "create table tx (col bit(10) default 0x61)",
        "create table tx (col bit(10) default 97)",
        "create table tx (col bit(10) default 0b1100001)",
    ] {
        tk.must_exec(def);
        tk.must_exec("insert into tx value ()");
        tk.must_query("select * from tx").check(["\x00a"]);
        tk.must_exec("drop table tx");
    }
}

// --- TestIssue5092 (pkg/ddl/db_integration_test.go:284) ---
//
// The add/drop-column grammar guards, and the settled row shape after
// FIRST/AFTER placements. PORTED: grouped `add column (b int, c int)`,
// per-spec FIRST/AFTER positions (column order read off the table's
// metadata), the second-half defaults row `0 1 2 22 3 33 4`, duplicate
// drop = 1091, dropping every column (mixed IF EXISTS) = 1090, guarded
// drops succeed.
//
// The mixed `add column if not exists d int, add column d int` form is
// reports 1060 from the unguarded duplicate during original-schema admission;
// two additions of the same new name instead conflict with 8200.
#[test]
fn issue_5092_add_drop_column_positions_and_guards() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t_issue_5092 (a int)");
    tk.must_exec("alter table t_issue_5092 add column (b int, c int)");
    tk.must_exec("alter table t_issue_5092 add column if not exists (b int, c int)");
    tk.must_exec("alter table t_issue_5092 add column b1 int after b, add column c1 int after c");
    tk.must_exec(
        "alter table t_issue_5092 add column d int after b, add column e int first, \
         add column f int after c1, add column g int, add column h int first",
    );
    let names: Vec<String> = kv_table(&tk, "test", "t_issue_5092")
        .columns()
        .iter()
        .map(|c| c.name.clone())
        .collect();
    assert_eq!(
        names,
        ["h", "e", "a", "b", "d", "b1", "c", "c1", "f", "g"],
        "Go's SHOW CREATE TABLE column order"
    );
    let error = tk.must_get_err_code(
        "alter table t_issue_5092 add column if not exists d int, add column d int",
        1060,
    );
    assert_eq!(error.message, "Duplicate column name 'd'");
    tk.must_get_err_code(
        "alter table t_issue_5092 add column dd int, add column if not exists dd int",
        8200,
    );
    tk.must_exec("drop table t_issue_5092");

    // The defaults half: every new column settles its default and row order
    // follows the placements, "0 1 2 22 3 33 4".
    tk.must_exec("create table t_issue_5092 (a int default 1)");
    tk.must_exec("alter table t_issue_5092 add column (b int default 2, c int default 3)");
    tk.must_exec(
        "alter table t_issue_5092 add column b1 int default 22 after b, add column c1 int default 33 after c",
    );
    tk.must_exec("insert into t_issue_5092 value ()");
    tk.must_query("select * from t_issue_5092").check(["1 2 22 3 33"]);
    tk.must_exec(
        "alter table t_issue_5092 add column d int default 4 after c1, add column aa int default 0 first",
    );
    tk.must_query("select * from t_issue_5092").check(["0 1 2 22 3 33 4"]);
    tk.must_exec("drop table t_issue_5092");

    // The drop guards.
    tk.must_exec("create table t_issue_5092 (a int)");
    tk.must_exec("alter table t_issue_5092 add column (b int, c int)");
    tk.must_exec("alter table t_issue_5092 drop column b, drop column c");
    // Go: both original columns are already absent.
    tk.must_get_err_code("alter table t_issue_5092 drop column c, drop column c", 1091);
    tk.must_exec("alter table t_issue_5092 drop column if exists b,drop column if exists c");
    // Go: ErrCantDropFieldOrKey (1091).
    tk.must_get_err_code("alter table t_issue_5092 drop column g, drop column d", 1091);
    tk.must_exec("drop table t_issue_5092");

    // Dropping every column through a mixed IF EXISTS list is
    // ErrCantRemoveAllFields (1090).
    tk.must_exec("create table t_issue_5092 (a int)");
    tk.must_exec("alter table t_issue_5092 add column (b int, c int)");
    let error = tk.must_get_err_code(
        "alter table t_issue_5092 drop column if exists a, drop column b, drop column c",
        1090,
    );
    assert_eq!(
        error.message,
        "You can't delete all columns with ALTER TABLE; use DROP TABLE instead"
    );
}

// --- TestTableDDLWithTimeType (pkg/ddl/db_integration_test.go:373) ---
//
// TIME/DATETIME/TIMESTAMP(7) is refused with ErrTooBigPrecision (1426)
// through CREATE TABLE and every ALTER spelling, `time(-1)` is refused by
// the parser, and `datetime(0)` is a legal CHANGE target.
#[test]
fn table_ddl_with_time_type_too_big_precision_1426() {
    fn too_big_precision(tk: &mut TestKit, sql: &str) {
        let error = tk.must_get_err_code(sql, 1426);
        assert!(
            error.message.starts_with("Too-big precision 7 specified for '")
                && error.message.ends_with("'. Maximum is 6."),
            "{sql}: {}",
            error.message
        );
    }
    let mut tk = TestKit::new();
    for sql in [
        "create table t (a time(7))",
        "create table t (a datetime(7))",
        "create table t (a timestamp(7))",
    ] {
        too_big_precision(&mut tk, sql);
    }
    assert!(
        tk.exec_to_err("create table t (a time(-1))").is_some(),
        "time(-1) errors in Go too (require.Error)"
    );
    tk.must_exec("create table t (a datetime)");
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
        too_big_precision(&mut tk, sql);
    }
    // datetime(0) is a legal target.
    tk.must_exec("alter table t change column a aa datetime(0)");
}

// --- TestNullGeneratedColumn (pkg/ddl/db_integration_test.go:422) ---
//
// A VIRTUAL generated column over two NULL-able columns indexes cleanly
// when every row is written with no columns at all.
#[test]
fn null_generated_column_indexed_over_default_nulls() {
    let mut tk = TestKit::new();
    tk.must_exec(
        "CREATE TABLE t (a int(11) DEFAULT NULL, b int(11) DEFAULT NULL, \
         c int(11) GENERATED ALWAYS AS (a + b) VIRTUAL, h varchar(10) DEFAULT NULL, \
         m int(11) DEFAULT NULL)",
    );
    tk.must_exec("insert into t values()");
    tk.must_exec("alter table t add index idx_c(c)");
    // The NULL-backed generated index is consistent.
    tk.must_exec("admin check table t");
}

// --- TestDependedGeneratedColumnPrior2GeneratedColumn (pkg/ddl/db_integration_test.go:442) ---
//
// Order checks for a generated column added by ALTER. PORTED: an unknown
// dependency anywhere is ErrBadField (1054), the legal AFTER position
// commits, and the grouped spelling adds too. Go refuses `add column d int
// as (c+1) FIRST` with ErrGeneratedColumnNonPrior (3107) — existence is
// checked against the WHOLE table before prior-ness.
#[test]
fn depended_generated_column_prior2_generated_column_checks() {
    let mut tk = TestKit::new();
    tk.must_exec(
        "CREATE TABLE t (a int(11) DEFAULT NULL, b int(11) GENERATED ALWAYS AS (a + 1) VIRTUAL, \
         c int(11) GENERATED ALWAYS AS (b + 1) VIRTUAL)",
    );
    // Go: ErrBadField, checked before the prior-order rule.
    tk.must_get_err_code("alter table t add column d int as (c + f + 1) first", 1054);
    // A later generated dependency is Go's 3107 after existence succeeds.
    tk.must_get_err_code("alter table t add column d int as (c+1) first", 3107);
    // The correct AFTER position commits.
    tk.must_exec("alter table t add column d int as (c+1) after c");
    // The grouped spelling commits.
    tk.must_exec("alter table t add column(e int as (c+1))");
}

// --- TestIndexOnMultipleGeneratedColumn (pkg/ddl/db_integration_test.go:661) ---
//
// An index over a generated column that depends on another generated column:
// builds, answers `c > 1`, and USE/IGNORE INDEX agree.
#[test]
fn index_on_multiple_generated_column_base() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int as (a + 1), c int as (b + 1))");
    tk.must_exec("insert into t (a) values (1)");
    tk.must_exec("create index idx on t (c)");
    tk.must_query("select * from t where c > 1").check(["1 2 3"]);
    check_same_rows(
        &mut tk,
        "select * from t use index(idx) where c > 1",
        "select * from t ignore index(idx) where c > 1",
    );
    tk.must_exec("admin check table t");
}

// --- TestIndexOnMultipleGeneratedColumn1 (pkg/ddl/db_integration_test.go:678) ---
//
// A three-level generated chain, indexed at its end.
#[test]
fn index_on_multiple_generated_column1_three_level_chain() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int as (a + 1), c int as (b + 1), d int as (c + 1))");
    tk.must_exec("insert into t (a) values (1)");
    tk.must_exec("alter table t add index idx (d)");
    tk.must_query("select * from t where d > 2").check(["1 2 3 4"]);
    check_same_rows(
        &mut tk,
        "select * from t use index(idx) where d > 2",
        "select * from t ignore index(idx) where d > 2",
    );
}

// --- TestIndexOnMultipleGeneratedColumn2 (pkg/ddl/db_integration_test.go:695) ---
//
// Mixed numeric/text generated chain: bigint → decimal → varchar → float
// with `length()` in the last link; row "1 2 4 25".
#[test]
fn index_on_multiple_generated_column2_mixed_numeric_types() {
    let mut tk = TestKit::new();
    tk.must_exec(
        "create table t (a bigint, b decimal as (a+1), c varchar(20) as (b*2), d float as (a*23+b-1+length(c)))",
    );
    tk.must_exec("insert into t (a) values (1)");
    tk.must_exec("alter table t add index idx (d)");
    tk.must_query("select * from t where d > 2")
        .check_at(&[0, 3], ["1 25"]);
    check_same_rows(
        &mut tk,
        "select * from t use index(idx) where d > 2",
        "select * from t ignore index(idx) where d > 2",
    );
}

// --- TestIndexOnMultipleGeneratedColumn3 (pkg/ddl/db_integration_test.go:712) ---
//
// String-function chain: `length(a)+123`, `right(a, 2)`, and the float
// `b+b-7+1-3+3*ASCII(c)` = 131+131-7+1-3+3*108 = 577; row
// "adorable 131 le 577".
#[test]
fn index_on_multiple_generated_column3_string_functions() {
    let mut tk = TestKit::new();
    tk.must_exec(
        "create table t (a varchar(10), b float as (length(a)+123), c varchar(20) as (right(a, 2)), d float as (b+b-7+1-3+3*ASCII(c)))",
    );
    tk.must_exec("insert into t (a) values ('adorable')");
    tk.must_exec("alter table t add index idx (d)");
    tk.must_query("select * from t where d > 2")
        .check(["adorable 131 le 577"]);
    check_same_rows(
        &mut tk,
        "select * from t use index(idx) where d > 2",
        "select * from t ignore index(idx) where d > 2",
    );
}

// --- TestIndexOnMultipleGeneratedColumn4 (pkg/ddl/db_integration_test.go:729) ---
//
// A self-feeding chain a → b(a) → c(a+b) → d(a+b+c) → e(a+b+c+d); row
// "1 1 2 4 8" with d float indexed.
#[test]
fn index_on_multiple_generated_column4_self_chain() {
    let mut tk = TestKit::new();
    tk.must_exec(
        "create table t (a bigint, b decimal as (a), c int(10) as (a+b), d float as (a+b+c), e decimal as (a+b+c+d))",
    );
    tk.must_exec("insert into t (a) values (1)");
    tk.must_exec("alter table t add index idx (d)");
    tk.must_query("select * from t where d > 2")
        .check_at(&[0, 3], ["1 4"]);
    check_same_rows(
        &mut tk,
        "select * from t use index(idx) where d > 2",
        "select * from t ignore index(idx) where d > 2",
    );
}

// --- TestIndexOnMultipleGeneratedColumn5 (pkg/ddl/db_integration_test.go:746) ---
//
// VIRTUAL spellings throughout, indexes added across two ALTERs, and a
// generated column added to a table that already carries generated indexes;
// row "1 2 3 4".
#[test]
fn index_on_multiple_generated_column5_virtual_alter_add_index() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t(a bigint, b bigint as (a+1) virtual, c bigint as (b+1) virtual)");
    tk.must_exec("alter table t add index idx_b(b)");
    tk.must_exec("alter table t add index idx_c(c)");
    tk.must_exec("insert into t(a) values(1)");
    tk.must_exec("alter table t add column(d bigint as (c+1) virtual)");
    tk.must_exec("alter table t add index idx_d(d)");
    tk.must_query("select * from t where d > 2").check(["1 2 3 4"]);
    check_same_rows(
        &mut tk,
        "select * from t use index(idx_d) where d > 2",
        "select * from t ignore index(idx_d) where d > 2",
    );
}

// --- TestCaseInsensitiveCharsetAndCollate (pkg/ddl/db_integration_test.go:766) ---
//
// Uppercase charset/collation spellings are accepted and stored lowercased
// (all five t/t1..t4 tables, then t5's stored column charset). The
// version-gated halves — TableInfoVersion2 re-lowercasing and
// TableInfoVersion3 preserving the written case — need direct meta mutation
// through a Mutator this tier does not expose.
#[test]
fn case_insensitive_charset_and_collate_stored_lowercase() {
    let mut tk = TestKit::new();
    for sql in [
        "create table t(id int) ENGINE=InnoDB DEFAULT CHARSET=UTF8 COLLATE=UTF8_BIN",
        "create table t1(id int) ENGINE=InnoDB DEFAULT CHARSET=UTF8 COLLATE=uTF8_BIN",
        "create table t2(id int) ENGINE=InnoDB DEFAULT CHARSET=Utf8 COLLATE=utf8_BIN",
        "create table t3(id int) ENGINE=InnoDB DEFAULT CHARSET=Utf8mb4 COLLATE=utf8MB4_BIN",
        "create table t4(id int) ENGINE=InnoDB DEFAULT CHARSET=Utf8mb4 COLLATE=utf8MB4_general_ci",
        "create table t5(a varchar(20)) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4 COLLATE=UTF8MB4_GENERAL_CI",
    ] {
        tk.must_exec(sql);
    }
    // Go: `external.GetTableByName(...).Meta()`: tblInfo.Charset == "utf8mb4"
    // and the column's charset lowercased.
    let table = kv_table(&tk, "test", "t5");
    let first = column(&tk, "test", "t5", "a");
    assert_eq!(first.field_type.charset_name(), "utf8mb4");
    assert_eq!(first.field_type.collation_name(), "utf8mb4_general_ci");
    assert_eq!(table.name(), "t5");
}

// --- TestZeroFillCreateTable (pkg/ddl/db_integration_test.go:824) ---
//
// `y year` carries the YEAR type with the UNSIGNED flag (plus PRIMARY KEY's
// NOT NULL), and `z tinyint(10) zerofill` carries UNSIGNED — the flags
// Go reads off `model.ColumnInfo`.
#[test]
fn zero_fill_create_table_flags() {
    let mut tk = TestKit::new();
    tk.must_exec("create table abc(y year, z tinyint(10) zerofill, primary key(y))");
    let year_col = column(&tk, "test", "abc", "y");
    assert_eq!(year_col.field_type.code(), FieldTypeCode::Year);
    assert!(
        year_col.field_type.has_flag(FieldTypeFlags::UNSIGNED),
        "y year is unsigned"
    );
    let z_col = column(&tk, "test", "abc", "z");
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
    let mut tk = TestKit::new();
    tk.must_exec("create table t_bit (c1 bit(10) default 250, c2 int)");
    tk.must_exec("insert into t_bit set c2=1");
    tk.must_query("select bin(c1),c2 from t_bit").check(["11111010 1"]);
    tk.must_exec("drop table t_bit");

    tk.must_exec("create table t_bit (a int)");
    tk.must_exec("insert into t_bit value (1)");
    tk.must_exec("alter table t_bit add column c bit(16) null default b'1100110111001'");
    assert_eq!(bit_bytes(&mut tk, "select c from t_bit"), [0x19, 0xb9]);
    tk.must_exec("update t_bit set c = b'11100000000111'");
    assert_eq!(bit_bytes(&mut tk, "select c from t_bit"), [0x38, 0x07]);
    tk.must_exec("drop table t_bit");

    // MODIFY COLUMN's new default applies to NEW rows; the row written
    // before it still reads \x00 (Go's select shows "\x00" for it).
    tk.must_exec("create table t_bit (a int)");
    tk.must_exec("insert into t_bit value (1)");
    tk.must_exec("alter table t_bit add column b bit(1) default b'0'");
    tk.must_exec("alter table t_bit modify column b bit(1) default b'1'");
    assert_eq!(bit_bytes(&mut tk, "select b from t_bit"), [0x00]);
    tk.must_exec("drop table t_bit");

    // A bare `bit` column accepts an explicit NULL.
    tk.must_exec("create table t_bit (a bit)");
    tk.must_exec("insert into t_bit values (null)");
    tk.must_query("select count(*) from t_bit where a is null").check(["1"]);

    // The all-types spellings Go creates at the end of the test.
    tk.must_exec("create table testalltypes1 (field_1 bit default 1, field_2 tinyint null default null)");
    tk.must_exec(
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
    );
}

// --- TestModifyColumnOption (pkg/ddl/db_integration_test.go:626) ---
//
// The ACCEPTED half: CHANGE COLUMN rewrites a column's type through the
// sequence Go drives (int(11) → varchar(16) → varchar(10) → datetime →
// int(11) unsigned) and the same widening on a second table's char column.
// The coded clauses half also pins CHARACTER SET/COLLATE acceptance and the
// exact 8200 REFERENCES refusal.
#[test]
fn modify_column_option_change_type_sequence() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t1 (a int(11) default null)");
    tk.must_exec("create table t2 (b char, c int)");
    for sql in [
        "alter table t1 change a a varchar(16)",
        "alter table t1 change a a varchar(10)",
        "alter table t1 change a a datetime",
        "alter table t1 change a a int(11) unsigned",
        "alter table t2 change b b int(11) unsigned",
    ] {
        tk.must_exec(sql);
    }
}

#[test]
fn modify_column_option_charset_and_references_match_go() {
    let mut tk = TestKit::new();
    tk.must_exec(
        "create table t1 (b char(1) default null) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_general_ci",
    );
    // Go accepts MODIFY CHARACTER SET/COLLATE.
    tk.must_exec("alter table t1 modify column b char(1) character set utf8mb4 collate utf8mb4_general_ci");

    tk.must_exec("create table t2 (b char(1) collate utf8mb4_general_ci)");
    // Go accepts MODIFY CHARACTER SET/COLLATE after an explicit COLLATE.
    tk.must_exec("alter table t2 modify b char(1) character set utf8mb4 collate utf8mb4_general_ci");

    tk.must_exec("create table t3 (a int)");
    tk.must_exec("create table t4 (c int)");
    // Go refuses MODIFY REFERENCES.
    let error = tk.must_get_err_code("alter table t4 modify column c int references t3(a)", 8200);
    assert_eq!(
        error.message,
        "Unsupported modify column: can't modify with references"
    );
}

/// Go `AlterTableCharsetAndCollate` runs `checkModifyCharsetAndCollation`:
/// a collation-only change outside utf8/utf8mb4 names the collations, not
/// the charset (gorun: `[ddl:8200]Unsupported modify change collate from
/// gbk_chinese_ci to gbk_bin`).
#[test]
fn alter_table_collate_refusal_names_the_collations_like_go() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a varchar(10)) charset gbk");
    let error = tk.must_get_err_code("alter table t collate gbk_bin", 8200);
    assert_eq!(error.message, "Unsupported modify change collate from gbk_chinese_ci to gbk_bin");
    assert_eq!(column(&tk, "test", "t", "a").field_type.collation_name(), "gbk_chinese_ci");
}

/// Go `checkModifyTypes` always applies `checkModifyCharsetAndCollation`.
/// A binary-charset `VARCHAR` renders as `VARBINARY`; accepting a later text
/// charset would reinterpret its bytes and mutate the schema even though Go
/// refuses each conversion with 8200.
#[test]
fn modify_column_refuses_binary_charset_conversion_without_mutation() {
    let mut tk = TestKit::new();
    tk.must_exec("create table binary_column (a varchar(10) character set binary)");

    for (charset, collation) in [
        ("utf8", "utf8_bin"),
        ("utf8mb4", "utf8mb4_bin"),
        ("latin1", "latin1_bin"),
    ] {
        let sql = format!(
            "alter table binary_column modify column a varchar(10) character set {charset} collate {collation}"
        );
        // Go refuses a binary-to-text charset conversion.
        let error = tk.must_get_err_code(&sql, 8200);
        assert_eq!(
            error.message,
            format!("Unsupported modify charset from binary to {charset}"),
            "{sql}"
        );

        let stored = column(&tk, "test", "binary_column", "a");
        assert_eq!(stored.field_type.charset_name(), "binary", "{sql}");
        assert_eq!(stored.field_type.collation_name(), "binary", "{sql}");
    }

    // The adjacent widening Go permits remains admitted.
    tk.must_exec("create table latin1_column (a varchar(10) character set latin1)");
    tk.must_exec("alter table latin1_column modify column a varchar(10) character set utf8mb4");

    // An actual row reorganization may perform the otherwise unsupported
    // charset change while it converts between VARCHAR and CHAR.
    tk.must_exec("create table binary_reorg (a varchar(10) character set binary)");
    tk.must_exec("alter table binary_reorg modify column a char(5) character set utf8mb4");
}

// --- go-parity-gap documentaries -------------------------------------------------

#[test]
// The parser rejects empty charset/collation tokens before the executor, so
// the Go 1115/1273 empty-value halves remain documentary gaps. The accepted
// non-empty validation and conversion clauses are executable below.
fn changing_table_charset_matches_go_validation_and_conversion() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t(a varchar(10), index i(a)) charset latin1 collate latin1_bin");
    tk.must_get_err_code("alter table t charset gbk", 8200);
    tk.must_get_err_code("alter table t charset utf8 collate latin1_bin", 1253);
    tk.must_get_err_code("alter table t charset utf8", 8200);
    tk.must_get_err_code(
        "alter table t charset utf8 collate utf8_bin collate utf8mb4_bin collate utf8_bin",
        1253,
    );
    tk.must_get_err_code(
        "alter table t charset latin1 charset utf8 charset utf8mb4 collate utf8_bin",
        1302,
    );

    // Go changes the table default charset.
    tk.must_exec("alter table t charset utf8mb4");
    let table = kv_table(&tk, "test", "t");
    assert_eq!(table.charset().charset, Charset::Utf8Mb4);
    assert_eq!(table.charset().collation, Charset::Utf8Mb4.default_collation());
    assert_eq!(column(&tk, "test", "t", "a").field_type.charset(), Charset::Latin1);

    tk.must_exec("create table utf8_table(a varchar(10)) charset utf8");
    // Go converts table and columns to utf8mb4.
    tk.must_exec("alter table utf8_table convert to charset utf8mb4");
    let table = kv_table(&tk, "test", "utf8_table");
    assert_eq!(table.charset().charset, Charset::Utf8Mb4);
    assert_eq!(column(&tk, "test", "utf8_table", "a").field_type.charset(), Charset::Utf8Mb4);

    tk.must_exec("create table ascii_table(a varchar(10) character set ascii) charset utf8mb4");
    tk.must_get_err_code("alter table ascii_table convert to charset utf8mb4", 8200);
}

// The column-count half of Go's TestCreateTableTooLarge mutates the global
// config, so it lives in the serial binary
// (`serial_db_integration_config_source.rs`).
