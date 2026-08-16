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

//! SEED of Go `pkg/sessionctx/variable`, covering `session.go`'s
//! `WriteStmtBufs`: the per-session scratch buffers insert/replace/delete/
//! update statements reuse across rows so row encoding and index-key
//! generation stop reallocating.
//!
//! This is the type `pkg/table/tblctx`'s `MutateBuffers` is built from, and
//! it had no Rust owner. `SessionVars.GetWriteStmtBufs`/`CleanBuffers` come
//! with the `SessionVars` batch; the buffers themselves live here where the
//! table-mutation runtime that fills them lives.

use tidb_datatype::Datum;

/// Go `WriteStmtBufs`.
#[derive(Clone, Debug, Default)]
pub struct WriteStmtBufs {
    /// Go `RowValBuf`, used by `tablecodec.EncodeRow` to reduce growth.
    pub row_val_buf: Vec<u8>,
    /// Go `AddRowValues`, temp insert-row values to reduce allocations when
    /// importing data.
    pub add_row_values: Vec<Datum>,
    /// Go `IndexValsBuf`, used by `index.FetchValues`.
    pub index_vals_buf: Vec<Datum>,
    /// Go `IndexKeyBuf`, used by `index.GenIndexKey`.
    pub index_key_buf: Vec<u8>,
}

impl WriteStmtBufs {
    /// Go's private `clean` (reached through `SessionVars.CleanBuffers`):
    /// drops every buffer back to nil.
    pub fn clean(&mut self) {
        self.row_val_buf = Vec::new();
        self.add_row_values = Vec::new();
        self.index_vals_buf = Vec::new();
        self.index_key_buf = Vec::new();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // clean drops the buffers entirely (Go sets them nil), releasing their
    // capacity rather than merely truncating.
    #[test]
    fn clean_releases_every_buffer() {
        let mut bufs = WriteStmtBufs::default();
        bufs.row_val_buf.extend_from_slice(&[1, 2, 3]);
        bufs.add_row_values.push(Datum::Int(1));
        bufs.index_vals_buf.push(Datum::Int(2));
        bufs.index_key_buf.extend_from_slice(&[4, 5]);

        bufs.clean();
        assert!(bufs.row_val_buf.is_empty());
        assert_eq!(bufs.row_val_buf.capacity(), 0);
        assert!(bufs.add_row_values.is_empty());
        assert_eq!(bufs.add_row_values.capacity(), 0);
        assert!(bufs.index_vals_buf.is_empty());
        assert!(bufs.index_key_buf.is_empty());
        assert_eq!(bufs.index_key_buf.capacity(), 0);
    }
}
