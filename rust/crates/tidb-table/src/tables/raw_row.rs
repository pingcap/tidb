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

use std::collections::BTreeMap;

use tidb_datatype::{Datum, UNSPECIFIED_LENGTH};

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
        let zone = ctx.session_zone();
        let mut row = vec![Datum::Null; offsets.len()];
        let mut col_tps = BTreeMap::new();
        // Whether the column at `i` came from the handle.
        let mut from_handle = vec![false; offsets.len()];
        for (i, &offset) in offsets.iter().enumerate() {
            let info = self.columns[offset].read();
            if self.pk_handle_offset == Some(offset) {
                let TableHandle::Int(h) = handle else {
                    return Err(KvTableError::Decode("int handle expected".to_owned()));
                };
                row[i] = if info.field_type.is_unsigned() { Datum::UInt(*h as u64) } else { Datum::Int(*h) };
                from_handle[i] = true;
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
                    let common = tidb_txnkv::CommonHandle::new(encoded.clone())
                        .map_err(|e| KvTableError::Decode(format!("{e:?}")))?;
                    let col = common
                        .encoded_column(idx)
                        .ok_or_else(|| KvTableError::Decode("common handle column out of range".to_owned()))?;
                    let (_, datum) = tidb_codec::decode_one(col).map_err(|e| KvTableError::Decode(format!("{e:?}")))?;
                    row[i] = tidb_tablecodec::unflatten_datum(datum, &info.field_type, Some(&zone))
                        .map_err(|e| KvTableError::Decode(format!("{e:?}")))?;
                    from_handle[i] = true;
                    continue;
                }
            }
            col_tps.insert(info.id, info.field_type.clone());
        }
        let row_map = tidb_tablecodec::decode_table_row_to_map(value, &col_tps, Some(&zone))
            .map_err(|e| KvTableError::Decode(format!("{e:?}")))?;
        for (i, &offset) in offsets.iter().enumerate() {
            if from_handle[i] {
                continue;
            }
            let column = &self.columns[offset];
            let info = column.read();
            if let Some(v) = row_map.get(&info.id) {
                row[i] = v.clone();
                continue;
            }
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
