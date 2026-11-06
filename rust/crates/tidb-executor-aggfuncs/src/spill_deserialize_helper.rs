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

use tidb_chunk::chunk::Chunk;
use tidb_serialization::Cursor;

/// Go `deserializeHelper`.
pub struct DeserializeHelper<'a> {
    column: &'a Chunk,
    ordinal: usize,
    read_row_index: usize,
    total_row_cnt: usize,
}

impl<'a> DeserializeHelper<'a> {
    /// Go `newDeserializeHelper` over the spill column `ordinal` of `src`.
    pub(crate) fn new(src: &'a Chunk, ordinal: usize, row_num: usize) -> Self {
        Self {
            column: src,
            ordinal,
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
        if self.read_row_index >= self.total_row_cnt {
            return None;
        }
        let row = self.column.get_row(self.read_row_index);
        let bytes = row.get_bytes(self.ordinal);
        let mut pab = Cursor::new(&bytes);
        let value = read(&mut pab);
        self.read_row_index += 1;
        Some(value)
    }

    /// Go `deserializePartialResult4Count`.
    pub(crate) fn deserialize_partial_result_4_count(&mut self) -> Option<i64> {
        self.next_row(|pab| pab.read_i64())
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
}
