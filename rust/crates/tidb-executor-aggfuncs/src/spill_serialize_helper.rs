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

//! Go `pkg/executor/aggfuncs/spill_serialize_helper.go`: the bytes a
//! spilled partial result is stored as.

use tidb_serialization as util;

/// Go `SerializeHelper`: one reused buffer the encodings are written into.
pub struct SerializeHelper {
    buf: Vec<u8>,
}

impl Default for SerializeHelper {
    fn default() -> Self {
        Self::new()
    }
}

impl SerializeHelper {
    /// Go `NewSerializeHelper`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(64),
        }
    }

    /// Go `serializePartialResult4Count`.
    pub(crate) fn serialize_partial_result_4_count(&mut self, value: i64) -> &[u8] {
        self.buf.clear();
        util::serialize_i64(value, &mut self.buf);
        &self.buf
    }

    /// Go `serializePartialResult4SumFloat64`.
    pub(crate) fn serialize_partial_result_4_sum_float64(
        &mut self,
        value: &crate::func_sum::PartialResult4SumFloat64,
    ) -> &[u8] {
        self.buf.clear();
        util::serialize_f64(value.val, &mut self.buf);
        util::serialize_i64(value.not_null_row_count, &mut self.buf);
        &self.buf
    }

    /// Go `serializePartialResult4SumDecimal`.
    pub(crate) fn serialize_partial_result_4_sum_decimal(
        &mut self,
        val: &tidb_datatype::MyDecimal,
        not_null_row_count: i64,
    ) -> &[u8] {
        self.buf.clear();
        util::serialize_my_decimal(val, &mut self.buf);
        util::serialize_i64(not_null_row_count, &mut self.buf);
        &self.buf
    }

    /// Go `serializePartialResult4SumInt64`.
    pub(crate) fn serialize_partial_result_4_sum_int64(&mut self, val: i64, not_null_row_count: i64) -> &[u8] {
        self.buf.clear();
        util::serialize_i64(val, &mut self.buf);
        util::serialize_i64(not_null_row_count, &mut self.buf);
        &self.buf
    }

    /// Go `serializePartialResult4SumUint64`.
    pub(crate) fn serialize_partial_result_4_sum_uint64(&mut self, val: u64, not_null_row_count: i64) -> &[u8] {
        self.buf.clear();
        util::serialize_u64(val, &mut self.buf);
        util::serialize_i64(not_null_row_count, &mut self.buf);
        &self.buf
    }

    /// Go `serializePartialResult4AvgDecimal`.
    pub(crate) fn serialize_partial_result_4_avg_decimal(&mut self, sum: &tidb_datatype::MyDecimal, count: i64) -> &[u8] {
        self.buf.clear();
        util::serialize_my_decimal(sum, &mut self.buf);
        util::serialize_i64(count, &mut self.buf);
        &self.buf
    }

    /// Go `serializePartialResult4AvgFloat64`.
    pub(crate) fn serialize_partial_result_4_avg_float64(&mut self, sum: f64, count: i64) -> &[u8] {
        self.buf.clear();
        util::serialize_f64(sum, &mut self.buf);
        util::serialize_i64(count, &mut self.buf);
        &self.buf
    }

    /// Go `serializePartialResult4BitFunc`.
    pub(crate) fn serialize_partial_result_4_bit_func(&mut self, value: u64) -> &[u8] {
        self.buf.clear();
        util::serialize_u64(value, &mut self.buf);
        &self.buf
    }

    /// Go `serializePartialResult4GroupConcat`
    /// (`serializeBasePartialResult4GroupConcat`): whether a buffer exists,
    /// then its bytes.
    pub(crate) fn serialize_partial_result_4_group_concat(&mut self, buffer: Option<&[u8]>) -> &[u8] {
        self.buf.clear();
        util::serialize_bool(buffer.is_some(), &mut self.buf);
        if let Some(buffer) = buffer {
            util::serialize_bytes_buffer(buffer, &mut self.buf);
        }
        &self.buf
    }

    /// Go `serializePartialResult4JsonArrayagg`: each entry's interface
    /// encoding.
    pub(crate) fn serialize_partial_result_4_json_arrayagg(&mut self, value: &crate::func_json_arrayagg::PartialResult4JsonArrayagg) -> &[u8] {
        self.buf.clear();
        for value in &value.entries {
            crate::func_json_objectagg::serialize_interface(value, &mut self.buf);
        }
        &self.buf
    }

    /// Go `serializePartialResult4JsonObjectAgg`: each key, then its value's
    /// interface encoding.
    pub(crate) fn serialize_partial_result_4_json_object_agg(&mut self, value: &crate::func_json_objectagg::PartialResult4JsonObjectAgg) -> &[u8] {
        self.buf.clear();
        for (key, value) in value.entries.iter() {
            util::serialize_string(key, &mut self.buf);
            crate::func_json_objectagg::serialize_interface(value, &mut self.buf);
        }
        &self.buf
    }
}
