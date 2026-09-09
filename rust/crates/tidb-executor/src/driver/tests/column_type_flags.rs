//! The flags a column takes from its TYPE rather than from what was written,
//! end to end: `CREATE TABLE`, then the query whose ANSWER those flags decide.
//!
//! Go stamps them in `processColumnFlags` (`pkg/ddl/add_column.go:1297`) --
//! `BIT` is `UNSIGNED`, `YEAR` is `ZEROFILL` and therefore `UNSIGNED` -- and
//! its own comment says neither word ever appears in `SHOW CREATE TABLE`. So
//! the only way to observe them is arithmetic, which is why these assertions
//! run SQL rather than reading a field type: the flag is invisible in every
//! catalog surface and yet selects Go's unsigned integer signature.
//!
//! Every expectation below is quoted from a real (mock-backed) TiDB session
//! over the same table; see [`year_and_bit_columns_are_unsigned`].

use super::*;
use tidb_datatype::{FieldTypeCode, FieldTypeFlags};

#[test]
fn catalog_column_iteration_borrows_visible_metadata() {
    let mut catalog = flag_table();
    catalog.register(
        "memory",
        MemTable {
            columns: vec![("Label".into(), FieldType::new(FieldTypeCode::Varchar))],
            rows: Vec::new(),
        },
    );
    for name in ["y", "memory"] {
        let entry = catalog.table_in("test", name).unwrap();
        let owned = entry.column_types();
        assert_eq!(
            entry.column_names(),
            owned.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>()
        );
        for (offset, (name, ty)) in entry.columns().enumerate() {
            assert_eq!((name, ty), (owned[offset].0.as_str(), &owned[offset].1));
            let (source_name, source_type) = match entry {
                TableEntry::Kv(kv) => {
                    let column = &kv.visible_columns()[offset];
                    (column.name.as_str(), &column.field_type)
                }
                TableEntry::Mem(mem) => (mem.columns[offset].0.as_str(), &mem.columns[offset].1),
                _ => unreachable!(),
            };
            assert!(std::ptr::eq(name.as_ptr(), source_name.as_ptr()));
            assert!(std::ptr::eq(ty, source_type));
        }
    }
}

/// The five-column table both tests run against, with one row.
fn flag_table() -> Catalog {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE y (a YEAR, b BIT(8), c INT UNSIGNED, d INT ZEROFILL, e VARCHAR(5))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO y VALUES (1990, 1, 1, 1, 'x')",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    catalog
}

/// Captured from a real TiDB session over
/// `create table y (a year, b bit(8), c int unsigned, d int zerofill,
/// e varchar(5))` with the row `(1990, 1, 1, 1, 'x')`:
///
/// ```text
/// select a - 2000 from y   ERR   1690 BIGINT UNSIGNED value is out of range in '(test.y.a - 2000)'
/// select b - 2000 from y   ERR   1690 BIGINT UNSIGNED value is out of range in '(test.y.b - 2000)'
/// select c - 2000 from y   ERR   1690 BIGINT UNSIGNED value is out of range in '(test.y.c - 2000)'
/// select d - 2000 from y   ERR   1690 BIGINT UNSIGNED value is out of range in '(test.y.d - 2000)'
/// select a + 0    from y   1990
/// ```
///
/// The last row is the boundary: a rule that made every `YEAR` expression
/// overflow, rather than only the one whose result goes negative, would still
/// pass the four `ERR` rows. `a - 1000` (below) is the same boundary with the
/// subtraction kept -- it proves the SUBTRAHEND is not reinterpreted too.
///
/// Before Go's `processColumnFlags` was wired into this tier's `CREATE TABLE`,
/// a real `YEAR` column carried flags `0x0` and `select a - 2000 from y`
/// answered `-10`.
#[test]
fn year_and_bit_columns_are_unsigned() {
    let catalog = flag_table();
    let query = |sql: &str| run_select_on(sql, &catalog, &crate::StmtContext::for_query());

    // Go's `ErrDataOutOfRange` message is `%s value is out of range in '%s'`;
    // an unsigned underflow spells the first `%s` `BIGINT UNSIGNED` and the
    // second the offending expression, so every arm carries its own text.
    for (sql, message) in [
        (
            "SELECT a - 2000 FROM y",
            "BIGINT UNSIGNED value is out of range in '(test.y.a - 2000)'",
        ),
        (
            "SELECT b - 2000 FROM y",
            "BIGINT UNSIGNED value is out of range in '(test.y.b - 2000)'",
        ),
        // The two columns whose flags come from DECLARED modifiers instead;
        // they answered this before the port and must still answer it, so the
        // two flag sources are proven not to clobber each other.
        (
            "SELECT c - 2000 FROM y",
            "BIGINT UNSIGNED value is out of range in '(test.y.c - 2000)'",
        ),
        (
            "SELECT d - 2000 FROM y",
            "BIGINT UNSIGNED value is out of range in '(test.y.d - 2000)'",
        ),
    ] {
        let error = query(sql).expect_err(sql).to_mysql_error();
        assert_eq!(error.code, 1690, "{sql}");
        assert_eq!(error.message, message, "{sql}");
    }

    // The boundary rows: the same columns, in the same statement shape, whose
    // result stays in range.
    assert_eq!(
        query("SELECT a + 0 FROM y").unwrap(),
        vec![vec![Datum::UInt(1990)]]
    );
    assert_eq!(
        query("SELECT a - 1000 FROM y").unwrap(),
        vec![vec![Datum::UInt(990)]]
    );
    assert_eq!(
        query("SELECT b + 0 FROM y").unwrap(),
        vec![vec![Datum::UInt(1)]]
    );
    // A column that takes NO flag from its type is still ordinary signed
    // arithmetic -- the pass must not stamp UNSIGNED on everything.
    assert_eq!(
        query("SELECT LENGTH(e) - 2000 FROM y").unwrap(),
        vec![vec![Datum::Int(-1999)]]
    );
}

/// The same flags must stay INVISIBLE to every catalog surface, which is Go's
/// own note on `processColumnFlags`: "some types like bit and year, won't show
/// its unsigned flag in `show create table`".
///
/// Captured from the same session (which runs with the legacy integer display
/// width still on, so it prints the `(10)` a current server drops -- that
/// switch is `strict_integer_display_width` below and is orthogonal to the
/// flags this test is about):
///
/// ```text
/// SHOW CREATE TABLE y      `a` year(4) ... `b` bit(8) ... `c` int(10) unsigned
///                          `d` int(10) unsigned zerofill
/// information_schema.columns.COLUMN_TYPE
///                          a year(4) | b bit(8) | c int(10) unsigned
///                          d int(10) unsigned
/// ```
///
/// `type_desc` is what `SHOW` prints and `info_schema_str` is what
/// `COLUMN_TYPE` prints; the two differ on `zerofill` alone, and `d` is the
/// row that shows it. `a` and `b` are the rows this unit could have broken:
/// they now carry `UNSIGNED` (and `a` also `ZEROFILL`) and must still print
/// neither word, which is Go's own note quoted above. Reading the text
/// straight off the built column is what makes this a test of the FLAGS
/// rather than of the printer.
#[test]
fn the_new_flags_change_no_catalog_text() {
    let catalog = flag_table();
    let columns = catalog.table_in("test", "y").unwrap().column_types();
    // Both positions of the display-width switch, because the captured
    // session and a current server disagree only about that.
    for (strict, expected) in [
        (
            false,
            [
                ("a", "year(4)", "year(4)"),
                ("b", "bit(8)", "bit(8)"),
                ("c", "int(10) unsigned", "int(10) unsigned"),
                ("d", "int(10) unsigned zerofill", "int(10) unsigned"),
                ("e", "varchar(5)", "varchar(5)"),
            ],
        ),
        (
            true,
            [
                ("a", "year(4)", "year(4)"),
                ("b", "bit(8)", "bit(8)"),
                ("c", "int unsigned", "int unsigned"),
                // ZEROFILL keeps the width even under the strict switch, so
                // `d` is not simply `c` plus a word.
                ("d", "int(10) unsigned zerofill", "int(10) unsigned"),
                ("e", "varchar(5)", "varchar(5)"),
            ],
        ),
    ] {
        let printed: Vec<(String, String, String)> = columns
            .iter()
            .map(|(name, ft)| {
                (
                    name.clone(),
                    ft.type_desc(strict),
                    ft.info_schema_str(strict),
                )
            })
            .collect();
        for (index, (name, desc, info)) in expected.iter().enumerate() {
            assert_eq!(
                (
                    printed[index].0.as_str(),
                    printed[index].1.as_str(),
                    printed[index].2.as_str()
                ),
                (*name, *desc, *info),
                "strict_integer_display_width = {strict}"
            );
        }
    }

    // And the flen the printer read is the one the flag could have eaten:
    // `YEAR` is one of Go's `IsTypeInteger` types, so an UNSIGNED stamped
    // before the flen was settled would print `year(3)`.
    let (_, year) = &columns[0];
    assert_eq!(year.code(), FieldTypeCode::Year);
    assert_eq!(year.flen(), 4);
    assert!(year.has_flag(FieldTypeFlags::UNSIGNED));
    assert!(year.has_flag(FieldTypeFlags::ZEROFILL));
}
