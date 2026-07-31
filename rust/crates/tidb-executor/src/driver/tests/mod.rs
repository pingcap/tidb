//! The SQL-through-the-driver tests, grouped by the statement surface each
//! one exercises. Every test runs real SQL text end to end -- parse, plan,
//! execute over TiKV-format bytes -- so a group's name says which part of
//! that path it is holding still.
//!
//! This module owns only the two fixtures every group shares:
//! [`test_catalog`], the three-table catalog the queries run against, and
//! [`datum_text_for_test`], the printer an assertion compares against. The
//! assertions themselves live in the submodules.
//!
//! The groups mirror how Go splits `pkg/executor`'s own tests: statement
//! clauses, scan pushdown, aggregates, DML, joins, subqueries, key and index
//! behaviour, point gets, range scans, defaults, and set operations.

mod aggregates;
mod column_defaults;
mod dml;
mod index_ranges;
mod indexes;
mod joins;
mod point_get;
mod predicate_pushdown;
mod primary_keys;
mod select_clauses;
mod set_operations;
mod subqueries;
mod table_round_trip;

use super::*;

fn test_catalog() -> Catalog {
    use tidb_datatype::FieldTypeCode;
    let mut catalog = Catalog::default();
    catalog.register(
        "t",
        MemTable {
            columns: vec![
                ("a".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
                ("b".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
            ],
            rows: vec![
                vec![Datum::Int(1), Datum::Int(30)],
                vec![Datum::Int(2), Datum::Int(20)],
                vec![Datum::Int(3), Datum::Int(10)],
            ],
        },
    );
    catalog
}

/// The text of a string datum, however the codec chose to represent it.
fn datum_text_for_test(value: &Datum) -> String {
    match value {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
        other => panic!("expected a string datum, got {other:?}"),
    }
}
