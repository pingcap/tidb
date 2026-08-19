//! Reading back what was written: `INSERT` then `SELECT` over real
//! TiKV-format row bytes.
//!
//! These are the encode -> store -> scan -> decode tests, including the field
//! label a `COUNT(*)` keeps and the byte-level check that the stored keys and
//! values are the ones `pkg/tablecodec` would write. Mirrors Go
//! `pkg/table/tables`' round-trip tests over `pkg/executor`'s writer.

use super::*;

#[test]
fn select_from_table() {
    let catalog = test_catalog();
    // Column projection.
    assert_eq!(
        run_select_on(
            "SELECT a FROM t",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1)],
            vec![Datum::Int(2)],
            vec![Datum::Int(3)]
        ]
    );
    // Wildcard, qualified column, and an expression over columns.
    assert_eq!(
        run_select_on(
            "SELECT * FROM t WHERE t.a > 1",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(2), Datum::Int(20)],
            vec![Datum::Int(3), Datum::Int(10)],
        ]
    );
    assert_eq!(
        run_select_on(
            "SELECT a + b FROM t WHERE a = 2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(22)]]
    );
}

/// Regression for the label a `COUNT` field gets when no `AS` is
/// written: MySQL/Go name the output column after the SQL as WRITTEN
/// (`SelectField.Text`), not after the AST's normal form -- `COUNT(*)`
/// restores as `COUNT(1)` (both here and in Go's own hand-written
/// parser, which lowers a bare `*` argument to the literal `1`), but the
/// column label must stay `count(*)`. Before the fix this asserted
/// `field_name(0) == "COUNT(1)"`; after, it is the written text.
#[test]
fn count_star_field_keeps_its_written_label() {
    let catalog = test_catalog();
    let field_name = |sql: &str| {
        run_select_meta_on(sql, &catalog, &crate::StmtContext::for_query())
            .unwrap()
            .0
            .into_iter()
            .map(|(name, _)| name)
            .collect::<Vec<_>>()
    };

    assert_eq!(field_name("SELECT count(*) FROM t"), vec!["count(*)"]);
    assert_eq!(field_name("SELECT count(1) FROM t"), vec!["count(1)"]);
    assert_eq!(field_name("SELECT count(a) FROM t"), vec!["count(a)"]);
    assert_eq!(
        field_name("SELECT count(DISTINCT a) FROM t"),
        vec!["count(DISTINCT a)"]
    );
    assert_eq!(field_name("SELECT count(*) AS n FROM t"), vec!["n"]);
    // Same rule inside a derived table: the label becomes the derived
    // column name.
    assert_eq!(
        field_name("SELECT * FROM (SELECT count(*) FROM t) d"),
        vec!["count(*)"]
    );

    // The same root cause (the AST losing the original written text)
    // also surfaces in `ErrWrongGroupField`'s message, which Go quotes
    // with the field's written text too.
    let group_by_err = |sql: &str| {
        run_select_meta_on(sql, &catalog, &crate::StmtContext::for_query())
            .unwrap_err()
            .to_mysql_error()
            .message
    };
    assert_eq!(
        group_by_err("SELECT count(*) FROM t GROUP BY 1"),
        "Can't group on 'count(*)'"
    );
    assert_eq!(
        group_by_err("SELECT count(1) FROM t GROUP BY 1"),
        "Can't group on 'count(1)'"
    );
}

#[test]
fn insert_then_select_round_trip() {
    let mut catalog = test_catalog();
    // Full-row insert.
    assert_eq!(
        run_insert_on(
            "INSERT INTO t VALUES (4, 40), (5, 50)",
            &mut catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        2
    );
    // Column-list insert: unspecified column fills with NULL.
    assert_eq!(
        run_insert_on(
            "INSERT INTO t (a) VALUES (6)",
            &mut catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        1
    );
    assert_eq!(
        run_select_on(
            "SELECT a, b FROM t WHERE a > 3 ORDER BY a",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(4), Datum::Int(40)],
            vec![Datum::Int(5), Datum::Int(50)],
            vec![Datum::Int(6), Datum::Null],
        ]
    );
    // Arity mismatch and unknown table are rejected.
    assert!(run_insert_on(
        "INSERT INTO t (a) VALUES (1, 2)",
        &mut catalog,
        &crate::StmtContext::for_query()
    )
    .is_err());
    assert!(run_insert_on(
        "INSERT INTO missing VALUES (1)",
        &mut catalog,
        &crate::StmtContext::for_query()
    )
    .is_err());
}

/// The deployment-ladder proof: INSERT and SELECT round-trip through a
/// table whose rows are genuine TiKV-format bytes (record keys + v2 row
/// values), not a value matrix.
#[test]
fn sql_round_trips_through_real_tikv_bytes() {
    use crate::kv_table::{KvColumn, KvTable};
    use tidb_datatype::FieldTypeCode;
    let mut catalog = Catalog::default();
    catalog.register_kv(
        "kt",
        KvTable::new(
            77,
            vec![
                KvColumn {
                    name: "a".to_owned(),
                    id: 1,
                    field_type: FieldType::new(FieldTypeCode::LongLong),
                    column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                    default_value: None,
                    // A column present at CREATE TABLE has no pre-existing rows.
                    origin_default: None,
                    comment: String::new(),
                    generated: None,
                },
                KvColumn {
                    name: "b".to_owned(),
                    id: 2,
                    field_type: FieldType::new(FieldTypeCode::LongLong),
                    column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                    default_value: None,
                    // A column present at CREATE TABLE has no pre-existing rows.
                    origin_default: None,
                    comment: String::new(),
                    generated: None,
                },
            ],
        ),
    );

    assert_eq!(
        run_insert_on(
            "INSERT INTO kt VALUES (1, 10), (2, 20), (3, 30)",
            &mut catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        3
    );
    assert_eq!(
        run_select_on(
            "SELECT a, b FROM kt WHERE a > 1 ORDER BY b DESC",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(3), Datum::Int(30)],
            vec![Datum::Int(2), Datum::Int(20)],
        ]
    );
    assert_eq!(
        run_select_on(
            "SELECT a + b FROM kt WHERE a = 1",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(11)]]
    );
}
