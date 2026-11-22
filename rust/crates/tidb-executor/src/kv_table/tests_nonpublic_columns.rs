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

use super::*;
use std::collections::BTreeMap;
#[allow(unused_imports)]
use tidb_table::tables::nonpublic_columns::*;
use tidb_datatype::FieldTypeCode;

fn column(name: &str, id: i64, code: FieldTypeCode, origin: Option<Datum>) -> tidb_table::column::Column {
    tidb_table::column::Column {
        info: tidb_model::GoShared::new(tidb_model::ColumnInfo {
            id: id,
            name: tidb_ast::CiString::new(name.to_owned()),
            field_type: FieldType::new(code),
            version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
            state: tidb_model::SchemaState::PUBLIC,
            ..tidb_model::ColumnInfo::default()
        }),
        generated: None,
        default_value: None,
        origin_default: origin,
    }
}

/// A column a DDL holds in `state`, casting the column at `dependency` when
/// it is a MODIFY COLUMN's changing column.
fn nonpublic(
    name: &str,
    id: i64,
    origin: Option<Datum>,
    state: tidb_model::SchemaState,
    dependency: Option<i64>,
) -> tidb_table::column::Column {
    let column = column(name, id, FieldTypeCode::LongLong, origin);
    {
        let mut info = column.info.write();
        info.state = state;
        info.change_state_info = dependency.map(|offset| {
            tidb_model::GoShared::new(tidb_model::column::ChangeStateInfo { dependency_column_offset: offset })
        });
    }
    column
}

/// The table both writes go through: `a`, `b`, then an added column `c`
/// and `b`'s changing column, at offsets 2 and 3 of `TableInfo.Columns`.
fn table_with(c_state: tidb_model::SchemaState) -> TableCommon {
    let mut table = TableCommon::new(
        42,
        vec![
            column("a", 1, FieldTypeCode::LongLong, None),
            column("b", 2, FieldTypeCode::LongLong, None),
        ],
    );
    table.add_nonpublic_column(nonpublic("c", 3, Some(Datum::Int(7)), c_state, None));
    table.add_nonpublic_column(nonpublic(
        "_Col$_b_0",
        4,
        None,
        tidb_model::SchemaState::WRITE_REORGANIZATION,
        Some(1),
    ));
    table
}

fn stored(
    table: &mut TableCommon,
    store: &mut dyn tidb_kv::storage::TableStorage,
    handle: &TableHandle,
) -> BTreeMap<i64, Datum> {
    let (_, entry) = table.stored_record(store, handle).unwrap().unwrap();
    let types = [1_i64, 2, 3, 4]
        .into_iter()
        .map(|id| (id, FieldType::new(FieldTypeCode::LongLong)))
        .collect();
    tidb_tablecodec::decode_table_row_to_map(&entry, &types, None).unwrap()
}

/// Go `AddRecord` / `updateRecord` / `RemoveRecord` over `WritableCols`:
/// an INSERT writes an added column's original default and a changing
/// column's cast of its dependency; an UPDATE keeps what the row holds
/// for the first and recasts the second; a delete-only column gets no
/// value.
#[test]
fn writes_carry_nonpublic_columns_like_go() {
    let mut table = table_with(tidb_model::SchemaState::WRITE_ONLY);
    let mut store = tidb_store_driver_txn::cluster_storage_test_util::storage(&[]).0;
    // Go `TableCommon.Columns`: one list in `TableInfo.Columns` order, the
    // public columns its `Cols()` prefix.
    assert_eq!(table.columns().len(), 2);
    let ids = |columns: &[tidb_table::column::Column]| columns.iter().map(|c| c.read().id).collect::<Vec<_>>();
    assert_eq!(ids(table.deletable_columns()), vec![1, 2, 3, 4]);
    assert_eq!(
        table.meta().read().columns.iter_deref().map(|c| c.read().offset).collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
    assert_eq!(table.writable_columns().map(|(offset, _)| offset).collect::<Vec<_>>(), vec![0, 1, 2, 3]);
    let ctx = crate::StmtContext::for_dml(false, false, false);
    let handle = table
        .insert_row_with_row_id_checked(&mut store, &[Datum::Int(1), Datum::Int(5)], None, 0, &ctx, false)
        .unwrap();
    let row = stored(&mut table, &mut store, &handle);
    assert_eq!(row.get(&3), Some(&Datum::Int(7)));
    assert_eq!(row.get(&4), Some(&Datum::Int(5)));

    table
        .update_row_with_old_context(&mut store, &handle,
            Some(&[Datum::Int(1), Datum::Int(5)]),
            &[Datum::Int(1), Datum::Int(9)],
            &ctx,
        )
        .unwrap();
    let row = stored(&mut table, &mut store, &handle);
    assert_eq!(row.get(&2), Some(&Datum::Int(9)));
    assert_eq!(row.get(&3), Some(&Datum::Int(7)));
    assert_eq!(row.get(&4), Some(&Datum::Int(9)));

    // Once delete-only, a column is no longer written.
    let mut later = table_with(tidb_model::SchemaState::DELETE_ONLY);
    assert_eq!(later.writable_columns().map(|(offset, _)| offset).collect::<Vec<_>>(), vec![0, 1, 3]);
    let handle = later
        .insert_row_with_row_id_checked(&mut store, &[Datum::Int(2), Datum::Int(3)],
            Some(2),
            0,
            &ctx,
            false,
        )
        .unwrap();
    let row = stored(&mut later, &mut store, &handle);
    assert_eq!(row.get(&3), None);
    assert_eq!(row.get(&4), Some(&Datum::Int(3)));
}

/// Go `mlogTable` (`pkg/table/tables/mview_log.go`): the MV log rows a base
/// table's writes append, as Go master writes them for the same statement
/// shapes (captured: INSERT logs `I,1`; a tracked UPDATE `U,-1` then `U,1`;
/// an untracked one nothing; DELETE `D,-1`; REPLACE's removal `U,-1` and its
/// insert `U,1`).
#[test]
fn mview_log_rows_follow_go_master() {
    use tidb_table::tables::mview_log::{wrap_table_with_materialized_view_log, MLogSourceStmt};
    let mut base = TableCommon::new(
        42,
        vec![
            column("a", 1, FieldTypeCode::LongLong, None),
            column("b", 2, FieldTypeCode::LongLong, None),
            column("c", 3, FieldTypeCode::LongLong, None),
        ],
    );
    base.update_meta_for_test(|meta| {
        meta.materialized_view_base = Some(tidb_model::GoShared::new(tidb_model::table::MaterializedViewBaseInfo {
            mlog_id: 43,
            ..Default::default()
        }));
    });
    let mut mlog = TableCommon::new(
        43,
        vec![
            column("b", 1, FieldTypeCode::LongLong, None),
            column("a", 2, FieldTypeCode::LongLong, None),
            column("_MLOG$_DML_TYPE", 3, FieldTypeCode::Varchar, None),
            column("_MLOG$_OLD_NEW", 4, FieldTypeCode::Tiny, None),
        ],
    );
    mlog.update_meta_for_test(|meta| {
        meta.materialized_view_log = Some(tidb_model::GoShared::new(tidb_model::table::MaterializedViewLogInfo {
            base_table_id: 42,
            columns: vec![tidb_ast::CiString::new("b"), tidb_ast::CiString::new("a")].into(),
            ..Default::default()
        }));
    });
    let mut store = tidb_store_driver_txn::cluster_storage_test_util::storage(&[]).0;
    let ctx = crate::StmtContext::for_dml(false, false, false);
    let row = |a: i64, b: i64, c: i64| [Datum::Int(a), Datum::Int(b), Datum::Int(c)];
    let mut insert = wrap_table_with_materialized_view_log(&base, mlog.clone(), MLogSourceStmt::Insert).unwrap();
    let marker = insert.take_removed_conflict();
    insert.add_record(&mut store, &row(1, 10, 100), false, marker, false, &ctx).unwrap();
    let mut update = wrap_table_with_materialized_view_log(&base, mlog.clone(), MLogSourceStmt::Update).unwrap();
    // `b` (offset 1) assigned: logged; only `c` (offset 2): not.
    update.update_record(&mut store, &row(1, 10, 100), &row(1, 11, 100), &|offset| offset == 1, false, &ctx).unwrap();
    update.update_record(&mut store, &row(1, 11, 100), &row(1, 11, 101), &|offset| offset == 2, false, &ctx).unwrap();
    let mut delete = wrap_table_with_materialized_view_log(&base, mlog.clone(), MLogSourceStmt::Delete).unwrap();
    delete.remove_record(&mut store, &row(1, 11, 101), &ctx).unwrap();
    let mut replace = wrap_table_with_materialized_view_log(&base, mlog.clone(), MLogSourceStmt::Replace).unwrap();
    replace.remove_record(&mut store, &row(2, 20, 200), &ctx).unwrap();
    for new in [row(2, 21, 200), row(3, 30, 300)] {
        // Go `mlogTable.AddRecord` takes the marker before the base add.
        let marker = replace.take_removed_conflict();
        replace.add_record(&mut store, &new, false, marker, false, &ctx).unwrap();
    }
    // The log's rows, in row-id order: tracked columns, type, old/new.
    let mut rows = Vec::new();
    for handle in 1..=8 {
        let Some((_, entry)) = mlog.stored_record(&mut store, &TableHandle::Int(handle)).unwrap() else { continue };
        let types = [(1, FieldType::new(FieldTypeCode::LongLong)), (2, FieldType::new(FieldTypeCode::LongLong)), (3, FieldType::new(FieldTypeCode::Varchar)), (4, FieldType::new(FieldTypeCode::Tiny))]
            .into_iter()
            .collect();
        let values = tidb_tablecodec::decode_table_row_to_map(&entry, &types, None).unwrap();
        rows.push(format!(
            "{}|{}|{}|{}",
            values[&1].sql_string().unwrap(),
            values[&2].sql_string().unwrap(),
            values[&3].sql_string().unwrap(),
            values[&4].sql_string().unwrap()
        ));
    }
    assert_eq!(rows, ["10|1|I|1", "10|1|U|-1", "11|1|U|1", "11|1|D|-1", "20|2|U|-1", "21|2|U|1", "30|3|I|1"]);
    // Go `WrapTableWithMaterializedViewLog`'s checks: the IDs must pair up.
    let mut other = mlog.clone();
    other.update_meta_for_test(|meta| meta.materialized_view_log.as_ref().unwrap().write().base_table_id = 7);
    assert!(wrap_table_with_materialized_view_log(&base, other, MLogSourceStmt::Insert).is_err());
}
