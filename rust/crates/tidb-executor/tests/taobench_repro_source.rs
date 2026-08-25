// Temporary taobench gate reproduction: a prepared-style edge point read
// (`varchar type column = int literal`) over a NONCLUSTERED composite PK.

#![allow(missing_docs)]

use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_executor::driver::{run_select_on, Catalog};
use tidb_executor::kv_table::{KvColumn, KvIndex, KvTable};
use tidb_executor::StmtContext;

fn long_col(name: &str, id: i64) -> KvColumn {
    let mut ft = FieldType::new(FieldTypeCode::LongLong);
    ft.set_flen(20);
    ft.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    KvColumn { name: name.to_owned(), id, field_type: ft, 
        column_info_version: 1,
        default_value: None,
        origin_default: None,
        comment: String::new(),
        generated: None, }
}

fn varchar_col(name: &str, id: i64) -> KvColumn {
    let mut ft = FieldType::new(FieldTypeCode::Varchar);
    ft.set_flen(63);
    ft.set_collation(tidb_datatype::Collation::Utf8Mb4Bin);
    ft.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    KvColumn { name: name.to_owned(), id, field_type: ft, 
        column_info_version: 1,
        default_value: None,
        origin_default: None,
        comment: String::new(),
        generated: None, }
}

fn edge_table() -> KvTable {
    let mut table = KvTable::new(
        459,
        vec![
            long_col("id1", 1),
            long_col("id2", 2),
            varchar_col("type", 3),
            long_col("ts", 4),
            {
                let mut ft = FieldType::new(FieldTypeCode::Varchar);
                ft.set_flen(150);
                KvColumn { name: "value".to_owned(), id: 5, field_type: ft, 
        column_info_version: 1,
        default_value: None,
        origin_default: None,
        comment: String::new(),
        generated: None, }
            },
        ],
    );
    table.add_index(KvIndex {
        id: 1,
        name: "PRIMARY".to_owned(),
        comment: String::new(),
        unique: true,
        column_offsets: vec![0, 1, 2],
        prefix_lengths: vec![-1, -1, -1],
        visible: true,
        global: false,
        clustered_primary: false,
    }, false);
    table
        .insert_row(
            &[
                Datum::Int(848250056732),
                Datum::Int(1947761684552),
                Datum::new_string("3"),
                Datum::Int(1660627540589311589),
                Datum::new_string("hello"),
            ],
            &tidb_expr::NoColumns,
        )
        .unwrap();
    table
}

#[test]
fn edge_point_read_with_int_type_literal_returns_the_row() {
    let mut catalog = Catalog::default();
    catalog.register_kv("t", edge_table());
    let ctx = StmtContext::for_query();
    let rows = run_select_on(
        "SELECT ts FROM t WHERE id1 = 848250056732 AND id2 = 1947761684552 AND type = 3",
        &catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "the string '3' coerces to the integer 3 under MySQL comparison rules"
    );
}

#[test]
fn edge_point_read_without_type_filter_returns_the_row() {
    let mut catalog = Catalog::default();
    catalog.register_kv("t", edge_table());
    let ctx = StmtContext::for_query();
    let rows = run_select_on(
        "SELECT ts FROM t WHERE id1 = 848250056732 AND id2 = 1947761684552",
        &catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
}
