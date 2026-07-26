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

//! `pkg/util/chunk/row.go`: the `Row`, a cursor into one row of a [`Chunk`].
//!
//! Go's `Row` is `{c *Chunk, idx int}` -- a lightweight pointer plus index.
//! This port models it as a borrow, `Row<'a>`, so a row cannot outlive its
//! chunk.
//!
//! Ported: the accessors a simple query needs -- `chunk`, `idx`, `len`,
//! `get_int64`/`get_uint64`/`get_float32`/`get_float64`, `get_bytes`/`get_raw`,
//! and `is_null`. DEFERRED (documented): the typed getters that need Time/
//! Duration/`MyDecimal`/JSON column support, `GetDatumRow`, `CopyConstruct`, and
//! a `str`-typed `GetString` (pending the crate-wide bytes-vs-str policy).

use crate::chunk::Chunk;

/// Go `chunk.Row`: a cursor to one row of a [`Chunk`].
#[derive(Clone, Copy, Debug)]
pub struct Row<'a> {
    chunk: &'a Chunk,
    idx: usize,
}

impl<'a> Row<'a> {
    /// Builds a row cursor at physical index `idx` of `chunk`.
    #[must_use]
    pub(crate) fn new(chunk: &'a Chunk, idx: usize) -> Self {
        Row { chunk, idx }
    }

    /// Go `Chunk`: the chunk this row belongs to.
    #[must_use]
    pub fn chunk(&self) -> &'a Chunk {
        self.chunk
    }

    /// Go `Idx`: the (physical) row index within the chunk.
    #[must_use]
    pub fn idx(&self) -> usize {
        self.idx
    }

    /// Go `Len`: the number of columns.
    #[must_use]
    pub fn len(&self) -> usize {
        self.chunk.num_cols()
    }

    /// Whether the row has no columns.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Go `GetInt64`.
    #[must_use]
    pub fn get_int64(&self, col_idx: usize) -> i64 {
        self.chunk.columns()[col_idx].get_int64(self.idx)
    }

    /// Go `GetUint64`.
    #[must_use]
    pub fn get_uint64(&self, col_idx: usize) -> u64 {
        self.chunk.columns()[col_idx].get_uint64(self.idx)
    }

    /// Go `GetFloat32`.
    #[must_use]
    pub fn get_float32(&self, col_idx: usize) -> f32 {
        self.chunk.columns()[col_idx].get_float32(self.idx)
    }

    /// Go `GetFloat64`.
    #[must_use]
    pub fn get_float64(&self, col_idx: usize) -> f64 {
        self.chunk.columns()[col_idx].get_float64(self.idx)
    }

    /// Go `GetBytes`: the raw bytes of a variable-length column's cell.
    #[must_use]
    pub fn get_bytes(&self, col_idx: usize) -> &'a [u8] {
        self.chunk.columns()[col_idx].get_bytes(self.idx)
    }

    /// Go `GetRaw`: the raw element bytes for either column kind.
    #[must_use]
    pub fn get_raw(&self, col_idx: usize) -> &'a [u8] {
        self.chunk.columns()[col_idx].get_raw(self.idx)
    }

    /// Go `IsNull`.
    #[must_use]
    pub fn is_null(&self, col_idx: usize) -> bool {
        self.chunk.columns()[col_idx].is_null(self.idx)
    }
}
