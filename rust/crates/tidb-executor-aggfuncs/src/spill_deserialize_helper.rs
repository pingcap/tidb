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
}
