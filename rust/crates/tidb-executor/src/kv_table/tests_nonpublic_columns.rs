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
    let mut store = tidb_kv::storage::MemTableStorage::new();
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
