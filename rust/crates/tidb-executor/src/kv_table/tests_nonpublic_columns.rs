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
use std::sync::Arc;
#[allow(unused_imports)]
use tidb_table::kv_table::nonpublic_columns::*;
use tidb_datatype::FieldTypeCode;

fn column(name: &str, id: i64, code: FieldTypeCode, origin: Option<Datum>) -> KvColumn {
    KvColumn {
        name: name.to_owned(),
        id,
        field_type: FieldType::new(code),
        column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
        default_value: None,
        origin_default: origin,
        comment: String::new(),
        generated: None,
    }
}

fn stored(table: &mut KvTable, handle: &TableHandle) -> BTreeMap<i64, Datum> {
    let (_, entry) = table.stored_record(handle).unwrap().unwrap();
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
    let mut table = KvTable::new(
        42,
        vec![
            column("a", 1, FieldTypeCode::LongLong, None),
            column("b", 2, FieldTypeCode::LongLong, None),
        ],
    );
    table.add_nonpublic_column(NonPublicColumn {
        column: column("c", 3, FieldTypeCode::LongLong, Some(Datum::Int(7))),
        state: tidb_model::SchemaState::WRITE_ONLY,
        dependency: None,
    });
    table.add_nonpublic_column(NonPublicColumn {
        column: column("_Col$_b_0", 4, FieldTypeCode::LongLong, None),
        state: tidb_model::SchemaState::WRITE_REORGANIZATION,
        dependency: Some(1),
    });
    let ctx = crate::StmtContext::for_dml(false, false, false);
    let handle = table
        .insert_row_with_row_id_checked(&[Datum::Int(1), Datum::Int(5)], None, 0, &ctx, false)
        .unwrap();
    let row = stored(&mut table, &handle);
    assert_eq!(row.get(&3), Some(&Datum::Int(7)));
    assert_eq!(row.get(&4), Some(&Datum::Int(5)));

    table
        .update_row_with_old_context(
            &handle,
            Some(&[Datum::Int(1), Datum::Int(5)]),
            &[Datum::Int(1), Datum::Int(9)],
            &ctx,
        )
        .unwrap();
    let row = stored(&mut table, &handle);
    assert_eq!(row.get(&2), Some(&Datum::Int(9)));
    assert_eq!(row.get(&3), Some(&Datum::Int(7)));
    assert_eq!(row.get(&4), Some(&Datum::Int(9)));

    // Once delete-only, a column is no longer written.
    let mut later = table.clone_for_test_nonpublic();
    Arc::make_mut(later.nonpublic_columns_arc_mut())[0].state = tidb_model::SchemaState::DELETE_ONLY;
    let handle = later
        .insert_row_with_row_id_checked(
            &[Datum::Int(2), Datum::Int(3)],
            Some(2),
            0,
            &ctx,
            false,
        )
        .unwrap();
    let row = stored(&mut later, &handle);
    assert_eq!(row.get(&3), None);
    assert_eq!(row.get(&4), Some(&Datum::Int(3)));
}
