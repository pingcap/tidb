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
//! Go's goroutine and channel overlap decoding with consumption. That runtime
//! pipeline does not affect which rows appear or their order, so this reader
//! decodes one owned chunk when its cursor reaches it. Owning both a shallow
//! container handle and the current chunk also means a spill may happen
//! between chunks without invalidating the current row.

use crate::chunk::Chunk;
use crate::row::Row;
use crate::row_container::RowContainer;

/// Go `rowContainerReader`.
pub struct RowContainerReader {
    rc: RowContainer,
    /// The chunk the cursor is inside, decoded on demand.
    chunk: Option<Chunk>,
    chk_idx: usize,
    /// The chunk extent captured when the reader starts. Appends made after
    /// construction belong to a later scan, as in Go's bounded range loop.
    end_chk_idx: usize,
    row_idx: usize,
    /// Set once the cursor has run off the end.
    ended: bool,
    /// Go `err`: set by the reading worker, read by `Error`.
    err: Option<String>,
}

impl RowContainerReader {
    /// Go `NewRowContainerReader`, whose last act is one `Next()` so that
    /// `Current()` already stands on the first row.
    #[must_use]
    pub fn new(rc: &RowContainer) -> Self {
        let end_chk_idx = rc.num_chunks();
        let mut reader = RowContainerReader {
            rc: rc.shallow_copy(),
            chunk: None,
            chk_idx: 0,
            end_chk_idx,
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
            if self.chk_idx >= self.end_chk_idx {
                self.chunk = None;
                self.ended = true;
                return;
            }
            match self.rc.get_chunk_snapshot(self.chk_idx) {
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
    use std::sync::{mpsc, Arc};
    use std::time::Duration;
    use tidb_datatype::{FieldType, FieldTypeCode as C};

    /// Go `insertBytesRowsIntoRowContainer`, with a deterministic byte pattern
    /// where Go uses `crypto/rand`: what matters is that the rows are of
    /// varying length and distinguishable.
    fn insert_bytes_rows(chk_count: usize, row_per_chk: usize) -> (RowContainer, Vec<Vec<u8>>) {
        let fields = vec![FieldType::new(C::Varchar).with_flen(4096)];
        let mut rc = RowContainer::new(&fields, chk_count, crate::test_temp_storage::storage());
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
    }

    /// The same walk over a container that never spilled.
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

    #[test]
    fn the_reader_snapshots_its_chunk_extent_at_construction() {
        let fields = vec![FieldType::new(C::LongLong)];
        let mut rc = RowContainer::new(&fields, 2, crate::test_temp_storage::storage());
        let mut first = Chunk::new_with_capacity(&fields, 1);
        first.append_int64(0, 11);
        rc.add(first).expect("first chunk");

        let mut reader = RowContainerReader::new(&rc);
        let mut appended_later = Chunk::new_with_capacity(&fields, 1);
        appended_later.append_int64(0, 22);
        rc.add(appended_later).expect("later chunk");

        assert_eq!(reader.current().expect("first row").get_int64(0), 11);
        assert!(
            reader.next_row().is_none(),
            "a reader must not include chunks appended after it started"
        );
        assert_eq!(reader.error(), None);
    }

    /// Go `TestCloseRowContainerReader`: closing part-way through is allowed
    /// and stops the reader.
    #[test]
    fn closing_the_reader_part_way_through_stops_it() {
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
    }

    /// Go `TestReadAfterSpillWithRowContainerReader`: the reader owns its
    /// current chunk, so the shared container may spill before the next chunk
    /// is loaded and the remaining values still appear exactly once in order.
    #[test]
    fn the_reader_crosses_a_mid_read_spill() {
        let (mut rc, all_rows) = insert_bytes_rows(16, 16);
        let mut reader = RowContainerReader::new(&rc);
        for (i, want) in all_rows.iter().take(8 * 16).enumerate() {
            let row = reader
                .current()
                .unwrap_or_else(|| panic!("row {i} missing before spill"));
            assert_eq!(row.get_bytes(0), &want[..], "row {i}");
            reader.next_row();
        }

        rc.spill_to_disk();
        assert!(rc.already_spilled());

        for (i, want) in all_rows.iter().enumerate().skip(8 * 16) {
            let row = reader
                .current()
                .unwrap_or_else(|| panic!("row {i} missing after spill"));
            assert_eq!(row.get_bytes(0), &want[..], "row {i}");
            reader.next_row();
        }
        assert!(reader.current().is_none());
        assert_eq!(reader.error(), None);
        reader.close();
        rc.close();
    }

    /// Go `TestConcurrentSpillWithRowContainerReader`: a live reader keeps its
    /// current owned chunk valid while another shallow handle spills the shared
    /// container, then observes every remaining row exactly once and in order.
    #[test]
    fn a_live_reader_survives_a_concurrent_spill() {
        let (rc, all_rows) = insert_bytes_rows(16, 16);
        let reader_rc = rc.shallow_copy();
        let (loaded_tx, loaded_rx) = mpsc::channel();
        let (continue_tx, continue_rx) = mpsc::channel();
        let reader_thread = std::thread::spawn(move || {
            let mut reader = RowContainerReader::new(&reader_rc);
            let first = reader
                .current()
                .expect("the reader loads its first row before the spill")
                .get_bytes(0)
                .to_vec();
            loaded_tx.send(first).expect("report loaded row");
            continue_rx.recv().expect("continue after spill");

            let mut actual = Vec::new();
            while let Some(row) = reader.current() {
                actual.push(row.get_bytes(0).to_vec());
                reader.next_row();
            }
            (actual, reader.error().map(str::to_owned))
        });

        assert_eq!(
            loaded_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            all_rows[0]
        );
        let mut spilling = rc.shallow_copy();
        spilling.spill_to_disk();
        assert!(rc.already_spilled());
        continue_tx.send(()).expect("resume reader");
        let (actual, error) = reader_thread.join().expect("reader thread");
        assert_eq!(actual, all_rows);
        assert_eq!(error, None);
    }

    #[test]
    fn an_initial_chunk_error_is_latched_until_close() {
        let (mut rc, _) = insert_bytes_rows(2, 2);
        rc.spill_to_disk();
        rc.set_spill_error_for_test("reader get-chunk failure");

        let mut reader = RowContainerReader::new(&rc);
        assert!(reader.current().is_none());
        assert!(reader.next_row().is_none());
        assert!(reader.end().is_none());
        assert_eq!(reader.error(), Some("reader get-chunk failure"));
        reader.close();
        reader.close();
        assert!(reader.current().is_none());
        assert_eq!(reader.error(), Some("reader get-chunk failure"));
    }

    #[test]
    fn a_mid_stream_chunk_error_preserves_preceding_rows_and_then_latches() {
        let (mut rc, all_rows) = insert_bytes_rows(2, 2);
        let mut reader = RowContainerReader::new(&rc);
        rc.set_pre_spill(Arc::new(|| Err("reader mid-stream failure".to_owned())));
        rc.spill_to_disk();

        for want in all_rows.iter().take(2) {
            assert_eq!(
                reader.current().expect("preceding row").get_bytes(0),
                &want[..]
            );
            reader.next_row();
        }
        assert!(reader.current().is_none());
        assert!(reader.next_row().is_none());
        assert_eq!(reader.error(), Some("reader mid-stream failure"));
    }

    #[test]
    fn an_empty_reader_has_a_stable_end_and_close_is_idempotent() {
        let fields = vec![FieldType::new(C::LongLong)];
        let rc = RowContainer::new(&fields, 1, crate::test_temp_storage::storage());
        let mut reader = RowContainerReader::new(&rc);

        assert!(reader.current().is_none());
        assert!(reader.next_row().is_none());
        assert!(reader.end().is_none());
        assert_eq!(reader.error(), None);
        reader.close();
        reader.close();
        assert!(reader.current().is_none());
        assert!(reader.next_row().is_none());
        assert_eq!(reader.error(), None);
    }
}
