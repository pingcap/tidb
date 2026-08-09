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

//! `pkg/util/chunk/list.go`: `List`, an unbounded in-memory sequence of chunks.
//!
//! A `List` is what an operator writes rows into when it must hold more than
//! one chunk: it appends into the tail chunk until that chunk is at capacity,
//! then allocates the next one (reusing a [`Chunk`] from its own freelist when
//! [`List::reset`] put one there). Rows are addressed by [`RowPtr`], a
//! `(chunk index, row index)` pair that is only valid for the list that
//! returned it.
//!
//! # The memory-tracking rule is deliberately lagging
//!
//! Go consumes a chunk's `MemoryUsage` only once the list has moved PAST it --
//! `consumedIdx` is the index of the last chunk already accounted. The chunk
//! currently being filled is NOT tracked, so a list holding a single partial
//! chunk reports zero bytes. `Reset` closes that gap by accounting the final
//! chunk before recycling. This port keeps the lag exactly; smoothing it would
//! silently change every operator's spill threshold.

use crate::chunk::Chunk;
use crate::row::Row;
use std::sync::Arc;
use tidb_datatype::FieldType;
use tidb_util::memory::{Tracker, LABEL_FOR_CHUNK_LIST};

/// Go `chunk.RowPtrSize`: `unsafe.Sizeof(RowPtr{})`, two `uint32`s.
pub const ROW_PTR_SIZE: usize = size_of::<RowPtr>();

/// Go `chunk.RowPtr`: a row address inside one particular [`List`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowPtr {
    /// Go `ChkIdx`: the index of the chunk holding the row.
    pub chk_idx: u32,
    /// Go `RowIdx`: the row's index within that chunk.
    pub row_idx: u32,
}

impl RowPtr {
    /// Builds a row pointer.
    #[must_use]
    pub const fn new(chk_idx: u32, row_idx: u32) -> Self {
        RowPtr { chk_idx, row_idx }
    }
}

/// Go `chunk.List`: a growable sequence of chunks with a shared memory tracker.
pub struct List {
    field_types: Vec<FieldType>,
    init_chunk_size: usize,
    max_chunk_size: usize,
    length: usize,
    chunks: Vec<Chunk>,
    freelist: Vec<Chunk>,
    mem_tracker: Arc<Tracker>,
    /// Go `consumedIdx`: index in `chunks` whose memory has been accounted.
    /// `-1` means "nothing accounted yet", so this is signed.
    consumed_idx: isize,
}

/// The tracker is summarised by its consumed bytes; it carries a killer and a
/// parent chain that are not this list's state.
impl std::fmt::Debug for List {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("List")
            .field("init_chunk_size", &self.init_chunk_size)
            .field("max_chunk_size", &self.max_chunk_size)
            .field("length", &self.length)
            .field("num_chunks", &self.chunks.len())
            .field("freelist", &self.freelist.len())
            .field("consumed_idx", &self.consumed_idx)
            .field("bytes_consumed", &self.mem_tracker.bytes_consumed())
            .finish()
    }
}

impl List {
    /// Go `NewListWithMemTracker`.
    #[must_use]
    pub fn new_with_mem_tracker(
        field_types: &[FieldType],
        init_chunk_size: usize,
        max_chunk_size: usize,
        tracker: Arc<Tracker>,
    ) -> Self {
        List {
            field_types: field_types.to_vec(),
            init_chunk_size,
            max_chunk_size,
            length: 0,
            chunks: Vec::new(),
            freelist: Vec::new(),
            mem_tracker: tracker,
            consumed_idx: -1,
        }
    }

    /// Go `NewList`: a list with its own unlimited `LabelForChunkList` tracker.
    #[must_use]
    pub fn new(field_types: &[FieldType], init_chunk_size: usize, max_chunk_size: usize) -> Self {
        Self::new_with_mem_tracker(
            field_types,
            init_chunk_size,
            max_chunk_size,
            Tracker::new(LABEL_FOR_CHUNK_LIST, -1),
        )
    }

    /// Go `GetMemTracker`.
    #[must_use]
    pub fn mem_tracker(&self) -> &Arc<Tracker> {
        &self.mem_tracker
    }

    /// Go `Len`: the number of rows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Whether the list holds no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Go `NumChunks`.
    #[must_use]
    pub fn num_chunks(&self) -> usize {
        self.chunks.len()
    }

    /// Go `FieldTypes`.
    #[must_use]
    pub fn field_types(&self) -> &[FieldType] {
        &self.field_types
    }

    /// Go `NumRowsOfChunk`.
    #[must_use]
    pub fn num_rows_of_chunk(&self, chk_id: usize) -> usize {
        self.chunks[chk_id].num_rows()
    }

    /// Go `GetChunk`.
    #[must_use]
    pub fn get_chunk(&self, chk_idx: usize) -> &Chunk {
        &self.chunks[chk_idx]
    }

    /// Go `AppendRow`: copy `row` into the tail chunk, starting a new one when
    /// the tail is full or has already been accounted (`consumedIdx`).
    pub fn append_row(&mut self, row: Row<'_>) -> RowPtr {
        let mut chk_idx = self.chunks.len() as isize - 1;
        let needs_new_chunk = match usize::try_from(chk_idx) {
            Err(_) => true,
            Ok(tail) => {
                self.chunks[tail].num_rows() >= self.chunks[tail].capacity()
                    || chk_idx == self.consumed_idx
            }
        };
        if needs_new_chunk {
            let new_chunk = self.alloc_chunk();
            self.chunks.push(new_chunk);
            if chk_idx != self.consumed_idx {
                // Reachable only with a real tail: `chk_idx == -1` implies an
                // empty `chunks`, which only `new`/`reset`/`clear` produce and
                // all of them leave `consumed_idx == -1`.
                let tail = usize::try_from(chk_idx).expect("a -1 tail always equals consumed_idx");
                self.mem_tracker.consume(self.chunks[tail].memory_usage());
                self.consumed_idx = chk_idx;
            }
            chk_idx += 1;
        }
        let chk_idx = usize::try_from(chk_idx).expect("a chunk was just appended if needed");
        let row_idx = self.chunks[chk_idx].num_rows();
        self.chunks[chk_idx].append_row(row);
        self.length += 1;
        RowPtr {
            chk_idx: chk_idx as u32,
            row_idx: row_idx as u32,
        }
    }

    /// Go `Add`: take ownership of a whole chunk.
    ///
    /// # Panics
    /// Go panics on an empty chunk; so does this.
    pub fn add(&mut self, chk: Chunk) {
        assert!(
            chk.num_rows() != 0,
            "chunk appended to List should have at least 1 row"
        );
        let tail = self.chunks.len() as isize - 1;
        if self.consumed_idx != tail {
            let tail = usize::try_from(tail).expect("an unaccounted tail exists");
            self.mem_tracker.consume(self.chunks[tail].memory_usage());
            self.consumed_idx = tail as isize;
        }
        self.mem_tracker.consume(chk.memory_usage());
        self.consumed_idx += 1;
        self.length += chk.num_rows();
        self.chunks.push(chk);
    }

    /// Go `AllocChunk`: reuse a freelist chunk, else grow from the tail's
    /// shape, else build the first chunk from the field types.
    pub fn alloc_chunk(&mut self) -> Chunk {
        if let Some(mut chk) = self.freelist.pop() {
            self.mem_tracker.consume(-chk.memory_usage());
            chk.reset();
            return chk;
        }
        if let Some(tail) = self.chunks.last() {
            return tail.renew(self.max_chunk_size);
        }
        Chunk::new(&self.field_types, self.init_chunk_size, self.max_chunk_size)
    }

    /// Go `GetRow`.
    #[must_use]
    pub fn get_row(&self, ptr: RowPtr) -> Row<'_> {
        self.chunks[ptr.chk_idx as usize].get_row(ptr.row_idx as usize)
    }

    /// Retains the chunk owners needed by `ptr` without retaining a list lock.
    pub(crate) fn alias_row_owner(&mut self, ptr: RowPtr) -> (Chunk, usize) {
        let chunk = self.chunks[ptr.chk_idx as usize].alias_snapshot();
        (chunk, ptr.row_idx as usize)
    }

    /// Go `Reset`: account the unaccounted tail, then move every chunk to the
    /// freelist for reuse. The freelist chunks stay charged to the tracker
    /// until [`List::alloc_chunk`] takes one back.
    pub fn reset(&mut self) {
        let last_idx = self.chunks.len() as isize - 1;
        if last_idx != self.consumed_idx {
            let last_idx = usize::try_from(last_idx).expect("an unaccounted tail exists");
            self.mem_tracker
                .consume(self.chunks[last_idx].memory_usage());
        }
        self.freelist.append(&mut self.chunks);
        self.length = 0;
        self.consumed_idx = -1;
    }

    /// Go `Clear`: drop every chunk (freelist included) and zero the tracker.
    pub fn clear(&mut self) {
        self.mem_tracker.consume(-self.mem_tracker.bytes_consumed());
        self.freelist = Vec::new();
        self.chunks = Vec::new();
        self.length = 0;
        self.consumed_idx = -1;
    }

    /// Go `Walk`: call `walk_func` for every row in order, stopping at the
    /// first error.
    pub fn walk<E>(&self, mut walk_func: impl FnMut(Row<'_>) -> Result<(), E>) -> Result<(), E> {
        for chk in &self.chunks {
            for j in 0..chk.num_rows() {
                walk_func(chk.get_row(j))?;
            }
        }
        Ok(())
    }

    /// Go `l.freelist`: its length, which `list_test.go` asserts on directly.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn freelist_len(&self) -> usize {
        self.freelist.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{time_from_days, BinaryJSON, FieldTypeCode, MySqlDuration};

    /// Go `TestList` (`pkg/util/chunk/list_test.go`): append/reuse/`Add`
    /// bookkeeping and `Walk` order over a `NewList(fields, 2, 2)`.
    #[test]
    fn go_test_list() {
        let fields = vec![FieldType::new(FieldTypeCode::LongLong)];
        let mut l = List::new(&fields, 2, 2);
        let mut src_chunk = Chunk::new_with_capacity(&fields, 32);
        src_chunk.append_int64(0, 1);

        // Basic append: 5 rows into capacity-2 chunks is 3 chunks.
        for _ in 0..5 {
            l.append_row(src_chunk.get_row(0));
        }
        assert_eq!(l.num_chunks(), 3);
        assert_eq!(l.len(), 5);
        assert_eq!(l.freelist_len(), 0);

        // Chunk reuse: reset parks all three chunks on the freelist, and the
        // next five appends take them all back.
        l.reset();
        assert_eq!(l.freelist_len(), 3);
        for _ in 0..5 {
            l.append_row(src_chunk.get_row(0));
        }
        assert_eq!(l.freelist_len(), 0);

        // Add a whole chunk, then append: the appended row must start a NEW
        // chunk, because the added one is already accounted (`consumedIdx`).
        l.reset();
        let mut n_chunk = Chunk::new_with_capacity(&fields, 32);
        n_chunk.append_null(0);
        l.add(n_chunk);
        let ptr = l.append_row(src_chunk.get_row(0));
        assert_eq!(l.num_chunks(), 2);
        assert_eq!(ptr.chk_idx, 1);
        assert_eq!(ptr.row_idx, 0);
        assert_eq!(l.get_row(ptr).get_int64(0), 1);

        // Iteration order.
        l.reset();
        for i in 0..5 {
            let mut tmp = Chunk::new_with_capacity(&fields, 32);
            tmp.append_int64(0, i);
            l.append_row(tmp.get_row(0));
        }
        let mut results = Vec::new();
        l.walk(|r| {
            results.push(r.get_int64(0));
            Ok::<(), ()>(())
        })
        .expect("walk never fails here");
        assert_eq!(results, vec![0, 1, 2, 3, 4]);
    }

    /// Go `TestListMemoryUsage` (`pkg/util/chunk/list_test.go`): the tracker
    /// LAGS by one chunk -- a list holding a single partial chunk reports
    /// zero, `Reset` accounts it, and `Add` charges the added chunk on top.
    #[test]
    fn go_test_list_memory_usage() {
        let field_types = vec![
            FieldType::new(FieldTypeCode::Float),
            FieldType::new(FieldTypeCode::Varchar),
            FieldType::new(FieldTypeCode::Json),
            FieldType::new(FieldTypeCode::Datetime),
            FieldType::new(FieldTypeCode::Duration),
        ];
        let json_obj = BinaryJSON::parse("1").expect("valid JSON");
        let time_obj = time_from_days(2000 * 365);
        let duration_obj = MySqlDuration::from_nanoseconds(0, 0).expect("zero");

        let max_chunk_size = 2;
        let mut src_chk = Chunk::new_with_capacity(&field_types, max_chunk_size);
        src_chk.append_float32(0, 12.4);
        src_chk.append_string(1, "123");
        src_chk.append_json(2, &json_obj);
        src_chk.append_time(3, time_obj);
        src_chk.append_duration(4, duration_obj);

        let mut list = List::new(&field_types, max_chunk_size, max_chunk_size * 2);
        assert_eq!(list.mem_tracker().bytes_consumed(), 0);

        list.append_row(src_chk.get_row(0));
        assert_eq!(list.mem_tracker().bytes_consumed(), 0);

        let mem_usage = list.get_chunk(0).memory_usage();
        list.reset();
        assert_eq!(list.mem_tracker().bytes_consumed(), mem_usage);

        let src_usage = src_chk.memory_usage();
        list.add(src_chk);
        assert_eq!(list.mem_tracker().bytes_consumed(), mem_usage + src_usage);
    }
}
