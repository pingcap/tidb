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

//! `pkg/util/chunk/iterator.go`: the row iterators executors loop over.
//!
//! Go's protocol is
//!
//! ```text
//! for row := it.Begin(); row != it.End(); row = it.Next() { ... }
//! ```
//!
//! where `End()` is the zero `Row{}` sentinel. A Rust [`Row`] is a borrow with
//! no zero value, so END IS `None`: every position-returning method returns
//! `Option<Row>` and `end()` is `None`. That is the only structural change; the
//! cursor arithmetic below is Go's, line for line, including the deliberate
//! "one past the end" cursor parking that makes `Current()` return `End` after
//! the loop finishes.
//!
//! Ported: `Iterator4Slice`, `Iterator4Chunk`, `iterator4List`,
//! `iterator4RowPtr` and `multiIterator`.
//!
//! DEFERRED, documented: `iterator4RowContainer`, which is `row_container.go`'s
//! iterator and lands with that file. It is also the only iterator that can
//! FAIL (its `GetRow` reads a spill file), which is why [`ChunkIterator::error`]
//! exists here with a never-failing default -- `multiIterator` already
//! propagates it so the seam is in place.

use crate::chunk::Chunk;
use crate::list::{List, RowPtr};
use crate::row::Row;

/// Go `chunk.Iterator`.
///
/// Named `ChunkIterator` because `Iterator` is Rust's prelude trait; every
/// method is Go's, with `End` modelled as `None`.
pub trait ChunkIterator<'a> {
    /// Go `Begin`: reset the cursor and return the first row.
    fn begin(&mut self) -> Option<Row<'a>>;

    /// Go `Next`.
    fn next_row(&mut self) -> Option<Row<'a>>;

    /// Go `End`: the invalid end position.
    fn end(&self) -> Option<Row<'a>> {
        None
    }

    /// Go `Len`.
    fn len(&self) -> usize;

    /// Whether the iterator has no rows.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Go `Current`.
    fn current(&self) -> Option<Row<'a>>;

    /// Go `ReachEnd`: park the cursor past the last row.
    fn reach_end(&mut self);

    /// Go `Error`: the error that aborted iteration, if any.
    fn error(&self) -> Option<&str> {
        None
    }
}

/// Go `Iterator4Slice`: iterates a slice of rows.
///
/// Go pads this struct with `cpu.CacheLinePad` on both sides to keep the
/// cursor off a shared cache line. That is a false-sharing mitigation with no
/// semantic content, so it is not reproduced.
#[derive(Debug)]
pub struct Iterator4Slice<'a> {
    rows: Vec<Row<'a>>,
    cursor: usize,
}

impl<'a> Iterator4Slice<'a> {
    /// Go `NewIterator4Slice`.
    #[must_use]
    pub fn new(rows: Vec<Row<'a>>) -> Self {
        Iterator4Slice { rows, cursor: 0 }
    }

    /// Go `Reset`: replace the rows and rewind.
    pub fn reset(&mut self, rows: Vec<Row<'a>>) {
        self.rows = rows;
        self.cursor = 0;
    }
}

impl<'a> ChunkIterator<'a> for Iterator4Slice<'a> {
    fn begin(&mut self) -> Option<Row<'a>> {
        if self.len() == 0 {
            return self.end();
        }
        self.cursor = 1;
        Some(self.rows[0])
    }

    fn next_row(&mut self) -> Option<Row<'a>> {
        let len = self.len();
        if self.cursor >= len {
            self.cursor = len + 1;
            return self.end();
        }
        let row = self.rows[self.cursor];
        self.cursor += 1;
        Some(row)
    }

    fn current(&self) -> Option<Row<'a>> {
        if self.cursor == 0 || self.cursor > self.len() {
            return self.end();
        }
        Some(self.rows[self.cursor - 1])
    }

    fn reach_end(&mut self) {
        self.cursor = self.len() + 1;
    }

    fn len(&self) -> usize {
        self.rows.len()
    }
}

/// Go `Iterator4Chunk`: iterates the rows of one chunk.
#[derive(Debug)]
pub struct Iterator4Chunk<'a> {
    chk: &'a Chunk,
    cursor: usize,
    /// Go `numRows`: SNAPSHOTTED by `Begin`, where `Len` re-reads the chunk.
    /// The two can disagree if the chunk grows mid-iteration; Go relies on that
    /// snapshot to bound `Next`.
    num_rows: usize,
}

impl<'a> Iterator4Chunk<'a> {
    /// Go `NewIterator4Chunk`.
    #[must_use]
    pub fn new(chk: &'a Chunk) -> Self {
        Iterator4Chunk {
            chk,
            cursor: 0,
            num_rows: 0,
        }
    }

    /// Go `GetChunk`.
    #[must_use]
    pub fn get_chunk(&self) -> &'a Chunk {
        self.chk
    }

    /// Go `ResetChunk`.
    pub fn reset_chunk(&mut self, chk: &'a Chunk) {
        self.chk = chk;
    }
}

impl<'a> ChunkIterator<'a> for Iterator4Chunk<'a> {
    fn begin(&mut self) -> Option<Row<'a>> {
        self.num_rows = self.chk.num_rows();
        if self.num_rows == 0 {
            return self.end();
        }
        self.cursor = 1;
        Some(self.chk.get_row(0))
    }

    fn next_row(&mut self) -> Option<Row<'a>> {
        if self.cursor >= self.num_rows {
            self.cursor = self.num_rows + 1;
            return self.end();
        }
        let row = self.chk.get_row(self.cursor);
        self.cursor += 1;
        Some(row)
    }

    fn current(&self) -> Option<Row<'a>> {
        if self.cursor == 0 || self.cursor > self.len() {
            return self.end();
        }
        Some(self.chk.get_row(self.cursor - 1))
    }

    fn reach_end(&mut self) {
        self.cursor = self.len() + 1;
    }

    fn len(&self) -> usize {
        self.chk.num_rows()
    }
}

/// Go `iterator4List`: walks a [`List`] chunk by chunk.
#[derive(Debug)]
pub struct Iterator4List<'a> {
    li: &'a List,
    chk_cursor: usize,
    row_cursor: usize,
}

impl<'a> Iterator4List<'a> {
    /// Go `NewIterator4List`.
    #[must_use]
    pub fn new(li: &'a List) -> Self {
        Iterator4List {
            li,
            chk_cursor: 0,
            row_cursor: 0,
        }
    }
}

impl<'a> ChunkIterator<'a> for Iterator4List<'a> {
    fn begin(&mut self) -> Option<Row<'a>> {
        if self.li.num_chunks() == 0 {
            return self.end();
        }
        let chk = self.li.get_chunk(0);
        let row = chk.get_row(0);
        // A one-row first chunk is already exhausted, so the cursor advances
        // to the next chunk immediately.
        if chk.num_rows() == 1 {
            self.chk_cursor = 1;
            self.row_cursor = 0;
        } else {
            self.chk_cursor = 0;
            self.row_cursor = 1;
        }
        Some(row)
    }

    fn next_row(&mut self) -> Option<Row<'a>> {
        if self.chk_cursor >= self.li.num_chunks() {
            self.chk_cursor = self.li.num_chunks() + 1;
            return self.end();
        }
        let chk = self.li.get_chunk(self.chk_cursor);
        let row = chk.get_row(self.row_cursor);
        self.row_cursor += 1;
        if self.row_cursor == chk.num_rows() {
            self.row_cursor = 0;
            self.chk_cursor += 1;
        }
        Some(row)
    }

    fn current(&self) -> Option<Row<'a>> {
        if (self.chk_cursor == 0 && self.row_cursor == 0) || self.chk_cursor > self.li.num_chunks()
        {
            return self.end();
        }
        if self.row_cursor == 0 {
            let cur_chk = self.li.get_chunk(self.chk_cursor - 1);
            return Some(cur_chk.get_row(cur_chk.num_rows() - 1));
        }
        let cur_chk = self.li.get_chunk(self.chk_cursor);
        Some(cur_chk.get_row(self.row_cursor - 1))
    }

    fn reach_end(&mut self) {
        self.chk_cursor = self.li.num_chunks() + 1;
    }

    fn len(&self) -> usize {
        self.li.len()
    }
}

/// Go `iterator4RowPtr`: walks an arbitrary order of rows in a [`List`].
#[derive(Debug)]
pub struct Iterator4RowPtr<'a> {
    li: &'a List,
    ptrs: Vec<RowPtr>,
    cursor: usize,
}

impl<'a> Iterator4RowPtr<'a> {
    /// Go `NewIterator4RowPtr`.
    #[must_use]
    pub fn new(li: &'a List, ptrs: Vec<RowPtr>) -> Self {
        Iterator4RowPtr {
            li,
            ptrs,
            cursor: 0,
        }
    }
}

impl<'a> ChunkIterator<'a> for Iterator4RowPtr<'a> {
    fn begin(&mut self) -> Option<Row<'a>> {
        if self.len() == 0 {
            return self.end();
        }
        self.cursor = 1;
        Some(self.li.get_row(self.ptrs[0]))
    }

    fn next_row(&mut self) -> Option<Row<'a>> {
        let len = self.len();
        if self.cursor >= len {
            self.cursor = len + 1;
            return self.end();
        }
        let row = self.li.get_row(self.ptrs[self.cursor]);
        self.cursor += 1;
        Some(row)
    }

    fn current(&self) -> Option<Row<'a>> {
        if self.cursor == 0 || self.cursor > self.len() {
            return self.end();
        }
        Some(self.li.get_row(self.ptrs[self.cursor - 1]))
    }

    fn reach_end(&mut self) {
        self.cursor = self.len() + 1;
    }

    fn len(&self) -> usize {
        self.ptrs.len()
    }
}

/// Go `multiIterator`: concatenates several iterators into one.
///
/// Go's constructor DROPS every empty input, so `Len` is the sum of the kept
/// ones and `curPtr == numIter` is the single end condition.
pub struct MultiIterator<'a> {
    iters: Vec<Box<dyn ChunkIterator<'a> + 'a>>,
    num_iter: usize,
    length: usize,
    cur_ptr: usize,
    err: Option<String>,
}

impl<'a> MultiIterator<'a> {
    /// Go `NewMultiIterator`.
    #[must_use]
    pub fn new(iters: Vec<Box<dyn ChunkIterator<'a> + 'a>>) -> Self {
        let mut kept: Vec<Box<dyn ChunkIterator<'a> + 'a>> = Vec::new();
        let mut length = 0;
        for it in iters {
            if !it.is_empty() {
                length += it.len();
                kept.push(it);
            }
        }
        let num_iter = kept.len();
        MultiIterator {
            iters: kept,
            num_iter,
            length,
            cur_ptr: 0,
            err: None,
        }
    }
}

impl<'a> ChunkIterator<'a> for MultiIterator<'a> {
    fn len(&self) -> usize {
        self.length
    }

    fn begin(&mut self) -> Option<Row<'a>> {
        self.cur_ptr = 0;
        if self.num_iter > 0 {
            self.iters[0].begin();
        }
        self.current()
    }

    fn next_row(&mut self) -> Option<Row<'a>> {
        if self.cur_ptr == self.num_iter {
            return self.end();
        }
        let next = self.iters[self.cur_ptr].next_row();
        if next.is_some() {
            return next;
        }
        self.err = self.iters[self.cur_ptr].error().map(str::to_owned);
        if self.err.is_some() {
            self.reach_end();
            return self.end();
        }
        self.cur_ptr += 1;
        if self.cur_ptr == self.num_iter {
            return self.end();
        }
        self.iters[self.cur_ptr].begin()
    }

    /// Go additionally latches `it.err = it.curIter.Error()` here when the
    /// current row is `End`. `Current` is a read in this port (`&self`), and
    /// the latch is unobservable while `iterator4RowContainer` -- the only
    /// iterator that can produce an error -- is deferred: every iterator that
    /// exists here reports `None`. It moves back in with that file.
    fn current(&self) -> Option<Row<'a>> {
        if self.cur_ptr == self.num_iter {
            return self.end();
        }
        self.iters[self.cur_ptr].current()
    }

    fn reach_end(&mut self) {
        self.cur_ptr = self.num_iter;
    }

    fn error(&self) -> Option<&str> {
        self.err.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{FieldType, FieldTypeCode};

    fn long_long_fields() -> Vec<FieldType> {
        vec![FieldType::new(FieldTypeCode::LongLong)]
    }

    /// Go `checkEqual` (`pkg/util/chunk/iterator_test.go`).
    fn check_equal<'a>(it: &mut dyn ChunkIterator<'a>, exp: &[i64]) {
        assert_eq!(exp.len(), it.len());
        let mut i = 0;
        let mut row = it.begin();
        while row != it.end() {
            assert_eq!(row.expect("not end").get_int64(0), exp[i]);
            row = it.next_row();
            i += 1;
        }
        assert_eq!(i, exp.len());
    }

    /// Go `TestIteratorOnSel`: `Iterator4Chunk` walks the SELECTION, not the
    /// physical rows -- 1024 rows with the even ones selected yields 512.
    #[test]
    fn go_test_iterator_on_sel() {
        let fields = long_long_fields();
        let mut chk = Chunk::new(&fields, 32, 1024);
        let mut sel = Vec::new();
        for i in 0..1024i64 {
            chk.append_int64(0, i);
            if i % 2 == 0 {
                sel.push(i as usize);
            }
        }
        chk.set_sel(Some(sel));
        let mut it = Iterator4Chunk::new(&chk);
        let mut cnt = 0;
        let mut row = it.begin();
        while row != it.end() {
            assert_eq!(row.expect("not end").get_int64(0) % 2, 0);
            cnt += 1;
            row = it.next_row();
        }
        assert_eq!(cnt, 1024 / 2);
    }

    /// Go `TestMultiIterator`: concatenation drops empty inputs and joins the
    /// rest end to end, across chunk and list sources.
    ///
    /// The `iterator4RowContainer` cases of Go's test are omitted; that
    /// iterator is deferred with `row_container.go`.
    #[test]
    fn go_test_multi_iterator() {
        let empty = Chunk::default();
        let mut it = MultiIterator::new(vec![Box::new(Iterator4Chunk::new(&empty))]);
        assert_eq!(it.begin(), it.end());

        let empty_list = List::new(&[], 32, 1024);
        let mut it = MultiIterator::new(vec![
            Box::new(Iterator4Chunk::new(&empty)),
            Box::new(Iterator4List::new(&empty_list)),
        ]);
        assert_eq!(it.begin(), it.end());

        let fields = long_long_fields();
        let mut chk = Chunk::new(&fields, 32, 1024);
        let n = 10i64;
        let mut expected = Vec::new();
        for i in 0..n {
            chk.append_int64(0, i);
            expected.push(i);
        }
        let mut it = MultiIterator::new(vec![Box::new(Iterator4Chunk::new(&chk))]);
        check_equal(&mut it, &expected);

        let mut it = MultiIterator::new(vec![
            Box::new(Iterator4Chunk::new(&empty)),
            Box::new(Iterator4Chunk::new(&chk)),
            Box::new(Iterator4Chunk::new(&empty)),
        ]);
        check_equal(&mut it, &expected);

        let mut chk2 = Chunk::new(&fields, 32, 1024);
        for i in n..n * 2 {
            expected.push(i);
            chk2.append_int64(0, i);
        }
        let mut li = List::new(&fields, 32, 1024);
        li.add(chk2.clone());

        let mut it = MultiIterator::new(vec![
            Box::new(Iterator4Chunk::new(&chk)),
            Box::new(Iterator4Chunk::new(&chk2)),
        ]);
        check_equal(&mut it, &expected);

        let mut it = MultiIterator::new(vec![
            Box::new(Iterator4Chunk::new(&chk)),
            Box::new(Iterator4List::new(&li)),
        ]);
        check_equal(&mut it, &expected);

        // The mirror case: the list comes FIRST and holds the low rows.
        let mut li2 = List::new(&fields, 32, 1024);
        li2.add(chk.clone());
        let mut it = MultiIterator::new(vec![
            Box::new(Iterator4List::new(&li2)),
            Box::new(Iterator4Chunk::new(&chk2)),
        ]);
        check_equal(&mut it, &expected);
    }

    /// Go `TestIterator`: every iterator kind walks the same ten rows, and
    /// after `ReachEnd` reports `End` from `Current` while `Begin` still
    /// rewinds. The empty-source cases close the file.
    ///
    /// The `iterator4RowContainer` cases of Go's test are omitted; that
    /// iterator is deferred with `row_container.go`.
    #[test]
    fn go_test_iterator() {
        let fields = long_long_fields();
        let mut chk = Chunk::new(&fields, 32, 1024);
        let n = 10i64;
        let mut expected = Vec::new();
        for i in 0..n {
            chk.append_int64(0, i);
            expected.push(i);
        }

        let mut li = List::new(&fields, 1, 2);
        let mut li2 = List::new(&fields, 8, 16);
        let mut rows = Vec::new();
        let mut ptrs = Vec::new();
        let mut ptrs2 = Vec::new();
        for i in 0..n as usize {
            rows.push(chk.get_row(i));
            ptrs.push(li.append_row(chk.get_row(i)));
            ptrs2.push(li2.append_row(chk.get_row(i)));
        }

        {
            let mut it = Iterator4Slice::new(rows.clone());
            check_equal(&mut it, &expected);
            it.begin();
            for row in rows.iter().take(5) {
                assert_eq!(it.current(), Some(*row));
                it.next_row();
            }
            it.reach_end();
            assert_eq!(it.current(), it.end());
            assert_eq!(it.begin(), Some(rows[0]));
        }
        {
            let mut it = Iterator4Chunk::new(&chk);
            check_equal(&mut it, &expected);
            it.begin();
            for i in 0..5 {
                assert_eq!(it.current(), Some(chk.get_row(i)));
                it.next_row();
            }
            it.reach_end();
            assert_eq!(it.current(), it.end());
            assert_eq!(it.begin(), Some(chk.get_row(0)));
        }
        {
            let mut it = Iterator4List::new(&li);
            check_equal(&mut it, &expected);
            it.begin();
            for ptr in ptrs.iter().take(5) {
                assert_eq!(it.current(), Some(li.get_row(*ptr)));
                it.next_row();
            }
            it.reach_end();
            assert_eq!(it.current(), it.end());
            assert_eq!(it.begin(), Some(li.get_row(ptrs[0])));
        }
        {
            let mut it = Iterator4RowPtr::new(&li, ptrs.clone());
            check_equal(&mut it, &expected);
            it.begin();
            for ptr in ptrs.iter().take(5) {
                assert_eq!(it.current(), Some(li.get_row(*ptr)));
                it.next_row();
            }
            it.reach_end();
            assert_eq!(it.current(), it.end());
            assert_eq!(it.begin(), Some(li.get_row(ptrs[0])));
        }
        {
            // The same ten rows through a list with a DIFFERENT chunk layout
            // (init 8 / max 16 rather than 1 / 2), so the pointers differ.
            let mut it = Iterator4RowPtr::new(&li2, ptrs2.clone());
            check_equal(&mut it, &expected);
            it.begin();
            for ptr in ptrs2.iter().take(5) {
                assert_eq!(it.current(), Some(li2.get_row(*ptr)));
                it.next_row();
            }
            it.reach_end();
            assert_eq!(it.current(), it.end());
            assert_eq!(it.begin(), Some(li2.get_row(ptrs2[0])));
        }

        let mut it = Iterator4Slice::new(Vec::new());
        assert_eq!(it.begin(), it.end());
        let empty = Chunk::default();
        let mut it = Iterator4Chunk::new(&empty);
        assert_eq!(it.begin(), it.end());
        let empty_list = List::new(&[], 32, 1024);
        let mut it = Iterator4List::new(&empty_list);
        assert_eq!(it.begin(), it.end());
        let mut it = Iterator4RowPtr::new(&li, Vec::new());
        assert_eq!(it.begin(), it.end());
    }
}
