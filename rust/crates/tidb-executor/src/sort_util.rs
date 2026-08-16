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

//! Go `pkg/executor/sortexec`, covering `sort_util.go` -- the small shared
//! vocabulary the sort and TopN operators pass around: the spill status ladder,
//! the two spill errors, the per-partition row wrapper, and the `dataCursor`
//! that walks a spilled `DataInDiskByChunks` one chunk at a time.
//!
//! This crate ports `pkg/executor/sortexec` in pieces. COVERED here:
//! `sort_util.go`. Covered elsewhere in this crate: `sort.go` ->
//! [`crate::sort`], `sort_partition.go` -> [`crate::sort_partition`],
//! `topn.go` -> [`crate::topn`], `topn_spill.go` -> [`crate::topn_spill`],
//! `topn_chunk_heap.go` -> [`crate::topn_chunk_heap`], `multi_way_merge.go` ->
//! [`crate::multi_way_merge`]. NOT COVERED anywhere yet:
//! `parallel_sort_worker.go`, `parallel_sort_spill_helper.go`, and
//! `sort_spill.go` -- the parallel worker pipeline.
//!
//! NARROWINGS, by name:
//!
//! * `spillChunkSize` is not re-declared here. [`crate::sort_partition`]
//!   already owns the port's single definition, and this module re-exports it
//!   so a second, drifting copy cannot appear.
//! * `processPanicAndLog` is NOT ported: it recovers a Go panic and forwards it
//!   down a worker channel. This port propagates [`ExecError`] by return value
//!   and has no worker channels.
//! * `injectParallelSortRandomFail`, `injectErrorForIssue59655`, and
//!   `injectPanicForIssue63216` are NOT ported: they are `failpoint.Inject`
//!   sites, and this port has no failpoint runtime.
//! * `dataCursor` holds an OWNED [`Chunk`] and a row index where Go holds a
//!   `*chunk.Chunk` inside a `chunk.Iterator4Chunk`. Rust's
//!   `DataInDiskByChunks::get_chunk` hands back an owned chunk, and an
//!   iterator borrowing a field of the same struct would be self-referential.
//!   The observable cursor protocol (`begin`/`next`/`set_chunk`/`get_chk_id`)
//!   is unchanged; "empty row" becomes `None`.
//! * `rowWithPartition` and `rowWithError` carry a GENERIC payload rather than
//!   `chunk.Row`. A `chunk::Row<'_>` borrows its chunk, so a heap or a channel
//!   cannot hold one while the source that owns the chunk is advanced; the
//!   merge in [`crate::multi_way_merge`] therefore parameterizes the row.

use tidb_chunk::chunk::Chunk;
use tidb_chunk::chunk_in_disk::DataInDiskByChunks;
use tidb_chunk::row::Row;

use crate::executor::ExecError;

/// Go `spillChunkSize`: rows per chunk written to a spill file.
///
/// Re-exported rather than re-declared -- see the module narrowings.
pub use crate::sort_partition::SPILL_CHUNK_SIZE;

/// Go `signalCheckpointForSort`: how many row comparisons pass between two
/// SQL-killer signal checks.
///
/// The killer poll itself lives with the operators; this is the cadence they
/// share.
pub const SIGNAL_CHECKPOINT_FOR_SORT: u64 = 10240;

/// Go's `notSpilled`/`needSpill`/`inSpilling`/`spillTriggered` `iota` ladder:
/// where a partition or heap stands with respect to its spill.
///
/// The ladder only ever moves forward within one round of spilling.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum SpillStatus {
    /// Go `notSpilled`.
    #[default]
    NotSpilled,
    /// Go `needSpill`: the memory action has asked for a spill, which has not
    /// started yet.
    NeedSpill,
    /// Go `inSpilling`: a spill is running.
    InSpilling,
    /// Go `spillTriggered`: the data is on disk.
    SpillTriggered,
}

/// Go `errSpillEmptyChunk`.
#[must_use]
pub fn err_spill_empty_chunk() -> ExecError {
    ExecError::SpillFailed("can not spill empty chunk to disk".to_owned())
}

/// Go `errFailToAddChunk`.
#[must_use]
pub fn err_fail_to_add_chunk() -> ExecError {
    ExecError::SpillFailed("fail to add chunk".to_owned())
}

/// Go `rowWithPartition`: a row tagged with the partition (sorted run) it came
/// from, so a merge can pull the replacement from the right source.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowWithPartition<T> {
    /// Go `row`.
    pub row: T,
    /// Go `partitionID`.
    pub partition_id: usize,
}

/// Go `chunkWithMemoryUsage`: a chunk together with the memory charged for it,
/// which may exceed the chunk's own footprint.
pub struct ChunkWithMemoryUsage {
    /// Go `Chk`.
    pub chk: Chunk,
    /// Go `MemoryUsage`.
    pub memory_usage: i64,
}

/// Go `rowWithError`: one item of a worker's output stream -- either a row or
/// the failure that ended the stream.
///
/// Go models it as a struct with both fields and reads whichever is set; a Rust
/// enum makes the same protocol unrepresentable-in-error.
pub enum RowWithError<T> {
    /// Go `rowWithError{row: r}`.
    Row(T),
    /// Go `rowWithError{err: e}`.
    Err(ExecError),
}

/// Go `dataCursor`: a read cursor over one spilled `DataInDiskByChunks`,
/// holding the restored chunk and the position inside it.
///
/// Used only when spill is triggered.
pub struct DataCursor {
    /// Go `chkID`, `-1` until a chunk is set.
    chk_id: i64,
    chunk: Option<Chunk>,
    /// Index of the row [`DataCursor::begin`]/[`DataCursor::next`] last named.
    /// Held one past the end when the chunk is exhausted.
    row: usize,
}

impl Default for DataCursor {
    fn default() -> Self {
        DataCursor::new()
    }
}

impl DataCursor {
    /// Go `NewDataCursor`.
    #[must_use]
    pub fn new() -> Self {
        DataCursor {
            chk_id: -1,
            chunk: None,
            row: 0,
        }
    }

    /// Go `getChkID`: the id of the spilled chunk the cursor sits on, `-1`
    /// before the first [`DataCursor::set_chunk`].
    #[must_use]
    pub fn get_chk_id(&self) -> i64 {
        self.chk_id
    }

    /// Go `begin`: rewinds to the chunk's first row. `None` is Go's empty row.
    pub fn begin(&mut self) -> Option<Row<'_>> {
        self.row = 0;
        self.current()
    }

    /// Go `next`: advances one row. `None` is Go's empty row, and further calls
    /// keep returning `None` without walking off the chunk.
    ///
    /// Not [`Iterator::next`]: the yielded row borrows the cursor, so the
    /// lending shape `Iterator` cannot express. The Go name is kept.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Row<'_>> {
        self.row = self.row.saturating_add(1);
        self.current()
    }

    /// The row the cursor names, without moving it.
    fn current(&self) -> Option<Row<'_>> {
        let chunk = self.chunk.as_ref()?;
        if self.row < chunk.num_rows() {
            Some(chunk.get_row(self.row))
        } else {
            None
        }
    }

    /// Go `setChunk`: installs a restored chunk and its id.
    ///
    /// The cursor is left BEFORE the chunk's rows: Go's `ResetChunk` leaves
    /// `Iterator4Chunk` un-begun, so a `begin` must follow.
    pub fn set_chunk(&mut self, chk: Chunk, chk_id: i64) {
        self.chk_id = chk_id;
        self.chunk = Some(chk);
        // One past the end of an empty chunk, matching Go's "not yet begun"
        // iterator: `next` without a `begin` yields nothing.
        self.row = usize::MAX;
    }

    /// Drops the restored chunk, releasing its memory.
    pub fn clear(&mut self) {
        self.chunk = None;
        self.row = 0;
    }
}

/// Go `reloadCursor`: restores the NEXT spilled chunk into `cursor`.
///
/// Returns `false` once every spilled chunk has been consumed, which is how the
/// disk-backed merge learns a run is exhausted.
pub fn reload_cursor(
    cursor: &mut DataCursor,
    in_disk: &mut DataInDiskByChunks,
) -> Result<bool, ExecError> {
    let spilled_chk_num = i64::try_from(in_disk.num_chunks()).unwrap_or(i64::MAX);
    let restored_chk_id = cursor.get_chk_id() + 1;
    if restored_chk_id >= spilled_chk_num {
        // All data has been consumed.
        return Ok(false);
    }
    let chk = in_disk
        .get_chunk(usize::try_from(restored_chk_id).unwrap_or(0))
        .map_err(|err| ExecError::SpillFailed(err.to_string()))?;
    cursor.set_chunk(chk, restored_chk_id);
    Ok(true)
}

#[cfg(test)]
mod tests {
    //! NEW COVERAGE. Go exercises `dataCursor` only indirectly, through the
    //! spilled sort/TopN suites (`sort_spill_test.go`, `topn_spill_test.go`);
    //! these pin the cursor protocol itself.

    use super::*;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_util::disk::{SpillEncryptionMethod, SpillStorage, SpillStorageSpec};

    use std::sync::Arc;

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    fn chunk_of(vals: &[i64]) -> Chunk {
        let fields = [long()];
        let mut chk = Chunk::new(&fields, vals.len().max(1), vals.len().max(1));
        for &v in vals {
            chk.append_int64(0, v);
        }
        chk
    }

    fn storage(name: &str) -> Arc<SpillStorage> {
        let dir = std::env::temp_dir().join(format!("tidb_rust_sort_util_{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        Arc::new(
            SpillStorage::open(SpillStorageSpec {
                path: dir,
                quota_bytes: -1,
                encryption: SpillEncryptionMethod::Plaintext,
            })
            .expect("spill storage"),
        )
    }

    #[test]
    fn a_fresh_cursor_names_no_chunk() {
        let mut cursor = DataCursor::new();
        assert_eq!(cursor.get_chk_id(), -1);
        assert!(cursor.begin().is_none());
        assert!(cursor.next().is_none());
    }

    #[test]
    fn begin_then_next_walks_the_chunk_once() {
        let mut cursor = DataCursor::new();
        cursor.set_chunk(chunk_of(&[10, 20, 30]), 7);
        assert_eq!(cursor.get_chk_id(), 7);
        assert_eq!(cursor.begin().map(|r| r.get_int64(0)), Some(10));
        assert_eq!(cursor.next().map(|r| r.get_int64(0)), Some(20));
        assert_eq!(cursor.next().map(|r| r.get_int64(0)), Some(30));
        assert!(cursor.next().is_none());
        // Past the end the cursor stays put rather than wrapping.
        assert!(cursor.next().is_none());
    }

    #[test]
    fn a_set_chunk_is_not_yet_begun() {
        let mut cursor = DataCursor::new();
        cursor.set_chunk(chunk_of(&[1, 2]), 0);
        // Go's `ResetChunk` leaves the iterator before the first row, so a
        // `next` without a `begin` yields nothing.
        assert!(cursor.next().is_none());
        assert_eq!(cursor.begin().map(|r| r.get_int64(0)), Some(1));
    }

    #[test]
    fn an_empty_chunk_begins_at_nothing() {
        let mut cursor = DataCursor::new();
        cursor.set_chunk(chunk_of(&[]), 3);
        assert!(cursor.begin().is_none());
    }

    #[test]
    fn reload_cursor_walks_every_spilled_chunk_then_stops() {
        let fields = vec![long()];
        let mut in_disk = DataInDiskByChunks::new(fields, "", storage("reload"));
        in_disk.add(&chunk_of(&[1, 2])).expect("add");
        in_disk.add(&chunk_of(&[3])).expect("add");

        let mut cursor = DataCursor::new();
        let mut seen = Vec::new();
        while reload_cursor(&mut cursor, &mut in_disk).expect("reload") {
            let mut row = cursor.begin().map(|r| r.get_int64(0));
            while let Some(v) = row {
                seen.push(v);
                row = cursor.next().map(|r| r.get_int64(0));
            }
        }
        assert_eq!(seen, vec![1, 2, 3]);
        assert_eq!(cursor.get_chk_id(), 1);
        // Exhausted stays exhausted.
        assert!(!reload_cursor(&mut cursor, &mut in_disk).expect("reload"));
        in_disk.close();
    }

    #[test]
    fn reload_cursor_on_an_empty_disk_reports_exhausted() {
        let fields = vec![long()];
        let mut in_disk = DataInDiskByChunks::new(fields, "", storage("empty"));
        let mut cursor = DataCursor::new();
        assert!(!reload_cursor(&mut cursor, &mut in_disk).expect("reload"));
        assert_eq!(cursor.get_chk_id(), -1);
        in_disk.close();
    }

    #[test]
    fn the_spill_status_ladder_only_moves_forward() {
        assert_eq!(SpillStatus::default(), SpillStatus::NotSpilled);
        assert!(SpillStatus::NotSpilled < SpillStatus::NeedSpill);
        assert!(SpillStatus::NeedSpill < SpillStatus::InSpilling);
        assert!(SpillStatus::InSpilling < SpillStatus::SpillTriggered);
    }
}
