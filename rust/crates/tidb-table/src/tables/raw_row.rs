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

//! Go `tables.DecodeRawRowData`.

use std::{borrow::Cow, collections::BTreeMap};

use smallvec::SmallVec;
use tidb_datatype::{Datum, UNSPECIFIED_LENGTH};
use tidb_txnkv::CommonHandle;

use super::{KvTableError, TableCommon, TableHandle};

impl TableCommon {
    /// Go `DecodeRawRowData(ctx, tbl, h, cols, value)`: the values of the
    /// columns at `offsets` in a stored row. A column of the integer handle
    /// or a full column of the clustered handle comes from `handle`; a
    /// column the row does not store takes its current default
    /// (`GetColDefaultValue`), except a virtual generated column, which
    /// stays NULL.
    ///
    /// Go also fills a column under `ChangeStateInfo` from the column it
    /// changes; such a column is never public, so it is not among the
    /// columns a caller can name here.
    ///
    /// # Errors
    /// The row, the handle, or a default could not be decoded.
    pub fn decode_raw_row_data(
        &self,
        ctx: &dyn tidb_tblctx::MutateContext,
        handle: &TableHandle,
        offsets: &[usize],
        value: &[u8],
    ) -> Result<Vec<Datum>, KvTableError> {
        self.decode_raw_row(ctx, handle, offsets, Cow::Borrowed(value))
    }

    /// Decodes an owned KV value, letting wide strings share its allocation.
    pub fn decode_owned_raw_row_data(
        &self,
        ctx: &dyn tidb_tblctx::MutateContext,
        handle: &TableHandle,
        offsets: &[usize],
        value: Vec<u8>,
    ) -> Result<Vec<Datum>, KvTableError> {
        self.decode_raw_row(ctx, handle, offsets, Cow::Owned(value))
    }

    fn decode_raw_row(
        &self,
        ctx: &dyn tidb_tblctx::MutateContext,
        handle: &TableHandle,
        offsets: &[usize],
        value: Cow<'_, [u8]>,
    ) -> Result<Vec<Datum>, KvTableError> {
        let zone = ctx.session_zone();
        let mut row = vec![Datum::Null; offsets.len()];
        let mut col_tps = BTreeMap::new();
        let mut repeated = BTreeMap::new();
        let mut handle_columns = None;
        // Slots filled from the handle have no stored column to restore.
        let mut stored_ids = SmallVec::<[Option<i64>; 16]>::from_elem(None, offsets.len());
        for (i, &offset) in offsets.iter().enumerate() {
            let info = self.public_columns[offset].read();
            if self.pk_handle_offset == Some(offset) {
                let TableHandle::Int(h) = handle else {
                    return Err(KvTableError::Decode("int handle expected".to_owned()));
                };
                row[i] = if info.field_type.is_unsigned() { Datum::UInt(*h as u64) } else { Datum::Int(*h) };
                continue;
            }
            if let Some(idx) = self.common_handle_offsets.iter().position(|&o| o == offset) {
                if !info.field_type.need_restored_data_with_collation(self.use_new_collation)
                    && self.common_handle_prefix_lengths.get(idx).copied() == Some(UNSPECIFIED_LENGTH)
                {
                    // Go `containFullColInHandle` and `h.EncodedCol(idxInHandle)`.
                    let TableHandle::Common(encoded) = handle else {
                        return Err(KvTableError::Decode("common handle expected".to_owned()));
                    };
                    let columns = match &handle_columns {
                        Some(columns) => columns,
                        None => handle_columns.insert(common_handle_columns(encoded)?),
                    };
                    let col = columns
                        .get(idx)
                        .copied()
                        .flatten()
                        .ok_or_else(|| KvTableError::Decode("common handle column out of range".to_owned()))?;
                    let (_, datum) = tidb_codec::decode_one(col).map_err(|e| KvTableError::Decode(format!("{e:?}")))?;
                    row[i] = tidb_tablecodec::unflatten_datum(datum, &info.field_type, Some(&zone))
                        .map_err(|e| KvTableError::Decode(format!("{e:?}")))?;
                    continue;
                }
            }
            if col_tps.insert(info.id, info.field_type.clone()).is_some() {
                repeated.insert(info.id, i);
            }
            stored_ids[i] = Some(info.id);
        }
        let mut row_map = match value {
            Cow::Borrowed(value) => {
                tidb_tablecodec::decode_table_row_to_map(value, &col_tps, Some(&zone))
            }
            Cow::Owned(value) => {
                tidb_tablecodec::decode_owned_table_row_to_map(value, &col_tps, Some(&zone))
            }
        }
        .map_err(|e| KvTableError::Decode(format!("{e:?}")))?;
        for (i, (&offset, &id)) in offsets.iter().zip(&stored_ids).enumerate() {
            let Some(id) = id else { continue };
            if let Some(value) = row_map.get_mut(&id) {
                // Every occurrence except the last still needs the same stored value.
                row[i] = match repeated.get(&id) {
                    Some(&last) if last != i => value.clone(),
                    _ => std::mem::replace(value, Datum::Null),
                };
                continue;
            }
            let column = &self.public_columns[offset];
            let info = column.read();
            if info.is_virtual_generated() {
                continue;
            }
            row[i] = crate::column_default::get_col_default_value(
                column.default_value.as_ref(),
                &info.field_type,
                info.name.original(),
                info.version,
                info.field_type.flags() & tidb_datatype::FieldTypeFlags::NOT_NULL != 0,
                ctx,
                tidb_chunk::row::Row::empty(),
            )
            .map_err(KvTableError::from)?;
        }
        Ok(row)
    }
}

pub(super) fn common_handle_columns(
    encoded: &[u8],
) -> Result<SmallVec<[Option<&[u8]>; 16]>, KvTableError> {
    let mut columns = SmallVec::new();
    let mut start = 0_u16;
    CommonHandle::scan_encoded_columns(encoded, |column| {
        // CommonHandle keeps 16-bit endpoints, including wrapping for oversized input.
        let end = start.wrapping_add(column.len() as u16);
        columns.push(encoded.get(usize::from(start)..usize::from(end)));
        start = end;
    })
    .map_err(|e| KvTableError::Decode(format!("{e:?}")))?;
    Ok(columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::Column;
    use tidb_ast::CiString;
    use tidb_datatype::{FieldType, FieldTypeCode, SessionTimeZone};
    use tidb_model::{go_runtime::GoShared, ColumnInfo};
    use tidb_tblctx::StaticMutateContext;

    fn table() -> TableCommon {
        let columns = [FieldTypeCode::LongLong, FieldTypeCode::Blob]
            .into_iter()
            .enumerate()
            .map(|(i, code)| {
                Column::new(GoShared::new(ColumnInfo {
                    id: (i + 1) as i64,
                    name: CiString::new(format!("col{i}")),
                    field_type: FieldType::new(code),
                    ..ColumnInfo::default()
                }))
            })
            .collect();
        let mut table = TableCommon::with_collation(1, columns, false);
        table.set_common_handle_offsets(vec![0]);
        table
    }

    #[test]
    fn owned_raw_row_retains_repeated_stored_values() {
        let mut table = table();
        let context = StaticMutateContext::new();
        let zone = SessionTimeZone::utc();
        let row = [Datum::Int(71), Datum::Bytes(vec![0xf0; 8192])];
        let handle = table.handle_of_row(&row, &zone, 0, None).unwrap();
        for v2 in [false, true] {
            let encoded =
                tidb_tablecodec::encode_table_row(Some(&zone), &row[1..], &[2], v2, None)
                    .unwrap();
            let backing = encoded.as_ptr().addr()..encoded.as_ptr().addr() + encoded.len();
            let projection = [1, 0, 1];
            let expected = table
                .decode_raw_row_data(&context, &handle, &projection, &encoded)
                .unwrap();
            let decoded = table
                .decode_owned_raw_row_data(&context, &handle, &projection, encoded)
                .unwrap();
            assert_eq!(decoded, expected);
            assert_eq!(decoded[1], Datum::Int(71));
            for i in [0, 2] {
                let bytes = decoded[i].as_raw_bytes().unwrap();
                assert_eq!(bytes, row[1].as_raw_bytes().unwrap());
                assert!(backing.contains(&bytes.as_ptr().addr()));
                assert!(bytes.as_ptr().addr() + bytes.len() <= backing.end);
            }
        }
    }

    #[test]
    fn raw_row_validates_handle_when_a_handle_column_is_requested() {
        let table = table();
        let context = StaticMutateContext::new();
        let mut handle = tidb_codec::encode_key(&[Datum::Int(71)]).unwrap();
        handle.push(0xff);
        let handle = TableHandle::Common(handle);
        let value = [tidb_codec::NIL_FLAG];
        assert_eq!(
            table.decode_raw_row_data(&context, &handle, &[1], &value).unwrap(),
            [Datum::Null]
        );
        assert!(table.decode_raw_row_data(&context, &handle, &[0], &value).is_err());
    }
}
