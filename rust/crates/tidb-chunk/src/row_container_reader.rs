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

//! `pkg/util/chunk/row_container_reader.go`: the forward-only reader over a
//! [`RowContainer`].
//!
//! It differs from [`crate::row_container::Iterator4RowContainer`] in what it
//! reads: the iterator fetches ONE ROW at a time (a whole spill-file seek per
//! row), the reader fetches a WHOLE CHUNK and walks it. For a spilled
//! container that is the difference between one read per row and one per
//! chunk, which is why the executors that scan a container end to end use
//! this one.
//!
//! FAITHFUL ADAPTATION (concurrency shape): Go spawns a goroutine that reads
//! chunks ahead into a buffered channel of rows, so decoding overlaps with
//! consumption, and therefore also needs a `context`, a `WaitGroup`, a
//! `Close` that cancels, and a finalizer that warns when `Close` was
//! forgotten. Nothing about WHICH rows come out, or in what order, depends on
//! that: this reader decodes the next chunk when the cursor reaches it. So
//! [`RowContainerReader::close`] has nothing to cancel and the finalizer has
//! nothing to warn about -- both are kept as no-op API so a caller written
//! against Go's shape still reads correctly.

use std::borrow::Cow;

use crate::chunk::Chunk;
use crate::row::Row;
use crate::row_container::RowContainer;

/// Go `rowContainerReader`.
pub struct RowContainerReader<'a> {
    rc: &'a RowContainer,
    /// The chunk the cursor is inside, decoded on demand.
    chunk: Option<Cow<'a, Chunk>>,
    chk_idx: usize,
    row_idx: usize,
    /// Set once the cursor has run off the end.
    ended: bool,
    /// Go `err`: set by the reading worker, read by `Error`.
    err: Option<String>,
}

impl<'a> RowContainerReader<'a> {
    /// Go `NewRowContainerReader`, whose last act is one `Next()` so that
    /// `Current()` already stands on the first row.
    #[must_use]
    pub fn new(rc: &'a RowContainer) -> Self {
        let mut reader = RowContainerReader {
            rc,
            chunk: None,
            chk_idx: 0,
            row_idx: 0,
            ended: false,
            err: None,
        };
        reader.load_chunk();
        reader
    }

    /// Decodes the chunk at `chk_idx`, skipping empty ones, and ends the
    /// iteration when there are no chunks left.
    fn load_chunk(&mut self) {
        loop {
            if self.chk_idx >= self.rc.num_chunks() {
                self.chunk = None;
                self.ended = true;
                return;
            }
            match self.rc.get_chunk(self.chk_idx) {
                Ok(chunk) => {
                    if chunk.num_rows() > 0 {
                        self.chunk = Some(chunk);
                        self.row_idx = 0;
                        return;
                    }
                    self.chk_idx += 1;
                }
                Err(error) => {
                    self.err = Some(error.to_string());
                    self.chunk = None;
                    self.ended = true;
                    return;
                }
            }
        }
    }

    /// Go `Current`.
    #[must_use]
    pub fn current(&self) -> Option<Row<'_>> {
        let chunk = self.chunk.as_ref()?;
        if self.ended || self.row_idx >= chunk.num_rows() {
            return None;
        }
        Some(chunk.get_row(self.row_idx))
    }

    /// Go `Next`: advance one row and return it.
    pub fn next_row(&mut self) -> Option<Row<'_>> {
        if self.ended {
            return None;
        }
        self.row_idx += 1;
        let past_end = match &self.chunk {
            Some(chunk) => self.row_idx >= chunk.num_rows(),
            None => true,
        };
        if past_end {
            self.chk_idx += 1;
            self.load_chunk();
        }
        self.current()
    }

    /// Go `End`: the invalid end position.
    #[must_use]
    pub fn end(&self) -> Option<Row<'_>> {
        None
    }

    /// Go `Error`.
    #[must_use]
    pub fn error(&self) -> Option<&str> {
        self.err.as_deref()
    }

    /// Go `Close`: cancels the reading goroutine and joins it. There is no
    /// goroutine here; the reader stops producing rows.
    pub fn close(&mut self) {
        self.ended = true;
        self.chunk = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row_container::RowContainer;
    use tidb_datatype::{FieldType, FieldTypeCode as C};
    use tidb_util::disk;

    use crate::test_temp_storage::guard as temp_dir_guard;

    use crate::test_temp_storage::scratch_dir as scratch_temp_dir;

    /// Go `insertBytesRowsIntoRowContainer`, with a deterministic byte pattern
    /// where Go uses `crypto/rand`: what matters is that the rows are of
    /// varying length and distinguishable.
    fn insert_bytes_rows(chk_count: usize, row_per_chk: usize) -> (RowContainer, Vec<Vec<u8>>) {
        let fields = vec![FieldType::new(C::Varchar).with_flen(4096)];
        let mut rc = RowContainer::new(&fields, chk_count);
        let mut all_rows = Vec::new();
        for c in 0..chk_count {
            let mut chk = Chunk::new_with_capacity(&fields, row_per_chk);
            for r in 0..row_per_chk {
                let n = c * row_per_chk + r;
                let bytes: Vec<u8> = (0..(n % 97)).map(|i| (n + i) as u8).collect();
                chk.append_bytes(0, &bytes);
                all_rows.push(bytes);
            }
            rc.add(chk).expect("add");
        }
        (rc, all_rows)
    }

    /// Go `TestRowContainerReaderInDisk`: every row of a SPILLED container
    /// comes back in order.
    #[test]
    fn the_reader_walks_a_spilled_container() {
        let _guard = temp_dir_guard();
        let dir = scratch_temp_dir("indisk");
        disk::set_temp_storage_path(&dir);

        let (mut rc, all_rows) = insert_bytes_rows(16, 16);
        rc.spill_to_disk();
        assert_eq!(rc.spill_error(), None);

        let mut reader = RowContainerReader::new(&rc);
        for (i, want) in all_rows.iter().enumerate() {
            let row = reader
                .current()
                .unwrap_or_else(|| panic!("row {i} missing"));
            assert_eq!(row.get_bytes(0), &want[..], "row {i}");
            reader.next_row();
        }
        assert!(reader.current().is_none(), "the reader must be exhausted");
        assert_eq!(reader.error(), None);
        reader.close();
        drop(rc);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The same walk over a container that never spilled: the reader borrows
    /// the live chunks instead of decoding them.
    #[test]
    fn the_reader_walks_an_in_memory_container() {
        let (rc, all_rows) = insert_bytes_rows(8, 8);
        let mut reader = RowContainerReader::new(&rc);
        for (i, want) in all_rows.iter().enumerate() {
            let row = reader
                .current()
                .unwrap_or_else(|| panic!("row {i} missing"));
            assert_eq!(row.get_bytes(0), &want[..], "row {i}");
            reader.next_row();
        }
        assert!(reader.current().is_none());
    }

    /// Go `TestCloseRowContainerReader`: closing part-way through is allowed
    /// and stops the reader.
    #[test]
    fn closing_the_reader_part_way_through_stops_it() {
        let _guard = temp_dir_guard();
        let dir = scratch_temp_dir("close");
        disk::set_temp_storage_path(&dir);

        let (mut rc, all_rows) = insert_bytes_rows(16, 16);
        rc.spill_to_disk();

        let mut reader = RowContainerReader::new(&rc);
        // Eight and a half chunks, as Go reads.
        for want in all_rows.iter().take(8 * 16 + 8) {
            assert_eq!(reader.current().expect("row").get_bytes(0), &want[..]);
            reader.next_row();
        }
        reader.close();
        assert!(reader.current().is_none(), "a closed reader yields nothing");
        drop(rc);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
