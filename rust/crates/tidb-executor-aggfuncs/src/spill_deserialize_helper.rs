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

//! Go `pkg/executor/aggfuncs/spill_deserialize_helper.go`: reads spilled
//! partial results back, one row of the spill column at a time.

use tidb_chunk::{column::VariableCells, ColumnRef};
use tidb_serialization::Cursor;

/// Go `deserializeHelper`.
pub struct DeserializeHelper<'a> {
    column: ColumnRef<'a>,
    cells: Option<VariableCells<'a>>,
    read_row_index: usize,
    total_row_cnt: usize,
}

impl<'a> DeserializeHelper<'a> {
    /// Go `newDeserializeHelper` over the spill column.
    pub(crate) fn new(column: ColumnRef<'a>, row_num: usize) -> Self {
        Self {
            cells: column
                .try_borrow_stable()
                .and_then(|column| column.try_variable_cells()),
            column,
            read_row_index: 0,
            total_row_cnt: row_num,
        }
    }

    /// Go's `s.pab.Reset(s.column, s.readRowIndex)` and the read that
    /// follows, or `None` once every row is read.
    pub(crate) fn read_row<T>(&mut self, read: impl FnOnce(&mut Cursor<'_>) -> T) -> Option<T> {
        self.next_row(read)
    }

    fn next_row<T>(&mut self, read: impl FnOnce(&mut Cursor<'_>) -> T) -> Option<T> {
        self.read_row_bytes(|bytes| read(&mut Cursor::new(bytes)))
    }

    /// Reads one cell with a fixed layout without using a cursor.
    pub(crate) fn read_row_bytes<T>(&mut self, read: impl FnOnce(&[u8]) -> T) -> Option<T> {
        if self.read_row_index >= self.total_row_cnt {
            return None;
        }
        // Go reads directly from the spill column. The logical row count
        // bounds the read, and a selection vector does not reorder the cells.
        let row = self.read_row_index;
        let value = if let Some(cells) = &self.cells {
            read(cells.get(row))
        } else {
            let column = self.column.read();
            let bytes = column.get_bytes(row);
            read(&bytes)
        };
        self.read_row_index += 1;
        Some(value)
    }

    /// Go `deserializePartialResult4Count`.
    pub(crate) fn deserialize_partial_result_4_count(&mut self) -> Option<i64> {
        self.read_row_bytes(|bytes| {
            i64::from_ne_bytes(*bytes.first_chunk::<8>().expect("COUNT spill cell"))
        })
    }

    /// Go `deserializePartialResult4SumFloat64`.
    pub(crate) fn deserialize_partial_result_4_sum_float64(
        &mut self,
    ) -> Option<crate::func_sum::PartialResult4SumFloat64> {
        self.next_row(|pab| crate::func_sum::PartialResult4SumFloat64 {
            val: pab.read_f64(),
            not_null_row_count: pab.read_i64(),
        })
    }

    /// Go `deserializePartialResult4SumDecimal`: the sum and its row count.
    pub(crate) fn deserialize_partial_result_4_sum_decimal(
        &mut self,
    ) -> Option<(tidb_datatype::MyDecimal, i64)> {
        self.next_row(|pab| (pab.read_my_decimal(), pab.read_i64()))
    }

    /// Go `deserializePartialResult4SumInt64`.
    pub(crate) fn deserialize_partial_result_4_sum_int64(&mut self) -> Option<(i64, i64)> {
        self.next_row(|pab| (pab.read_i64(), pab.read_i64()))
    }

    /// Go `deserializePartialResult4SumUint64`.
    pub(crate) fn deserialize_partial_result_4_sum_uint64(&mut self) -> Option<(u64, i64)> {
        self.next_row(|pab| (pab.read_u64(), pab.read_i64()))
    }

    /// Go `deserializePartialResult4AvgDecimal`: the sum and its count.
    pub(crate) fn deserialize_partial_result_4_avg_decimal(&mut self) -> Option<(tidb_datatype::MyDecimal, i64)> {
        self.next_row(|pab| (pab.read_my_decimal(), pab.read_i64()))
    }

    /// Go `deserializePartialResult4AvgFloat64`: the sum and its count.
    pub(crate) fn deserialize_partial_result_4_avg_float64(&mut self) -> Option<(f64, i64)> {
        self.next_row(|pab| (pab.read_f64(), pab.read_i64()))
    }

    /// Go `deserializePartialResult4BitFunc`.
    pub(crate) fn deserialize_partial_result_4_bit_func(&mut self) -> Option<u64> {
        self.next_row(|pab| pab.read_u64())
    }

    /// Go `deserializePartialResult4GroupConcat`: the buffer's bytes, `None`
    /// for Go's nil buffer.
    pub(crate) fn deserialize_partial_result_4_group_concat(&mut self) -> Option<Option<Vec<u8>>> {
        self.next_row(|pab| pab.read_bool().then(|| pab.read_bytes_buffer()))
    }

    /// Go `deserializePartialResult4JsonArrayagg`: interface values to the
    /// end of the cell.
    pub(crate) fn deserialize_partial_result_4_json_arrayagg(&mut self, dst: &mut crate::func_json_arrayagg::PartialResult4JsonArrayagg) -> Option<()> {
        self.next_row(|pab| {
            while pab.position() < pab.len() {
                dst.entries.push(crate::func_json_objectagg::RealJSONValue(Some(pab.read_interface())));
            }
        })
    }

    /// Go `deserializePartialResult4JsonObjectAgg`: key and value pairs to
    /// the end of the cell, charged as the update charges them.
    pub(crate) fn deserialize_partial_result_4_json_object_agg(&mut self, dst: &mut crate::func_json_objectagg::PartialResult4JsonObjectAgg) -> Option<i64> {
        self.next_row(|pab| {
            let mut mem_delta = 0;
            while pab.position() < pab.len() {
                let key = pab.read_string();
                let real_val = crate::func_json_objectagg::RealJSONValue(Some(pab.read_interface()));
                let (key_len, val_delta) = (key.len() as i64, crate::func_json_objectagg::get_val_mem_delta(&real_val));
                let (delta, insert) = dst.entries.set_ext(key, real_val);
                if insert {
                    mem_delta += key_len + val_delta + delta;
                }
            }
            mem_delta
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_chunk::chunk::Chunk;
    use tidb_datatype::{FieldType, FieldTypeCode};

    #[test]
    fn spill_reads_physical_cells_up_to_the_logical_row_count() {
        let fields = [FieldType::new(FieldTypeCode::Blob)];
        let mut chunk = Chunk::new_chunk_with_capacity(&fields, 3);
        for value in [7_i64, 99, 11] {
            chunk.append_bytes(0, &value.to_ne_bytes());
        }
        chunk.set_sel(Some(vec![2, 0]));
        for shared in [false, true] {
            let _alias = shared.then(|| chunk.alias());
            let mut helper = DeserializeHelper::new(chunk.column_ref(0), chunk.num_rows());
            assert_eq!(helper.deserialize_partial_result_4_count(), Some(7));
            assert_eq!(helper.deserialize_partial_result_4_count(), Some(99));
            assert_eq!(helper.deserialize_partial_result_4_count(), None);
        }
    }
}
