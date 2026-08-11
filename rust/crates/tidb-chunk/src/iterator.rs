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
//! `Iterator4Slice`, `Iterator4Chunk`, `iterator4List`, `iterator4RowPtr`,
//! `iterator4RowContainer`, and `multiIterator` are all present. The ordinary
//! sources implement [`ChunkIterator`] because their rows borrow storage that
//! outlives the iterator. A spilled row-container row instead borrows a decode
//! buffer owned by the iterator itself, so [`LendingIterator`] and
//! [`LendingMultiIterator`] provide the Rust-native common surface: each row
//! borrows only the call that returned it. This is an ownership adaptation,
//! not a semantic omission; concatenation order and the first spill-read error
//! are the same package contract.

use crate::chunk::Chunk;
use crate::list::{List, RowPtr};
use crate::row::Row;
use crate::row_container::{Iterator4RowContainer, RowContainer};

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

    /// This fixed-lifetime iterator family cannot own a row-container decode
    /// buffer. [`LendingMultiIterator`] is the common surface for fallible
    /// row-container composition and latches its error before advancing.
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

/// A Rust lending form of Go `chunk.Iterator`.
///
/// The enum keeps the source set closed inside `tidb-chunk`, which makes it
/// possible to return `Row<'_>` borrowing this cursor without an object-safe
/// generic-associated-type trait. Ordinary chunk/list variants still borrow
/// their original storage; the row-container variant may return a row from
/// its owned spill decode buffer.
pub enum LendingIterator<'a> {
    /// Go `Iterator4Slice`.
    Slice(Iterator4Slice<'a>),
    /// Go `Iterator4Chunk`.
    Chunk(Iterator4Chunk<'a>),
    /// Go `iterator4List`.
    List(Iterator4List<'a>),
    /// Go `iterator4RowPtr`.
    RowPtr(Iterator4RowPtr<'a>),
    /// Go `iterator4RowContainer`.
    RowContainer(Iterator4RowContainer<'a>),
}

impl<'a> LendingIterator<'a> {
    /// Wraps a slice iterator.
    #[must_use]
    pub fn slice(rows: Vec<Row<'a>>) -> Self {
        Self::Slice(Iterator4Slice::new(rows))
    }

    /// Wraps a chunk iterator.
    #[must_use]
    pub fn chunk(chunk: &'a Chunk) -> Self {
        Self::Chunk(Iterator4Chunk::new(chunk))
    }

    /// Wraps a list iterator.
    #[must_use]
    pub fn list(list: &'a List) -> Self {
        Self::List(Iterator4List::new(list))
    }

    /// Wraps a row-pointer iterator.
    #[must_use]
    pub fn row_ptrs(list: &'a List, ptrs: Vec<RowPtr>) -> Self {
        Self::RowPtr(Iterator4RowPtr::new(list, ptrs))
    }

    /// Wraps a row-container iterator, whether the container is in memory or
    /// spilled.
    #[must_use]
    pub fn row_container(container: &'a RowContainer) -> Self {
        Self::RowContainer(Iterator4RowContainer::new(container))
    }

    /// Go `Begin`.
    pub fn begin(&mut self) -> Option<Row<'_>> {
        match self {
            Self::Slice(iterator) => iterator.begin(),
            Self::Chunk(iterator) => iterator.begin(),
            Self::List(iterator) => iterator.begin(),
            Self::RowPtr(iterator) => iterator.begin(),
            Self::RowContainer(iterator) => iterator.begin(),
        }
    }

    /// Go `Next`.
    pub fn next_row(&mut self) -> Option<Row<'_>> {
        match self {
            Self::Slice(iterator) => iterator.next_row(),
            Self::Chunk(iterator) => iterator.next_row(),
            Self::List(iterator) => iterator.next_row(),
            Self::RowPtr(iterator) => iterator.next_row(),
            Self::RowContainer(iterator) => iterator.next_row(),
        }
    }

    /// Go `Current`.
    #[must_use]
    pub fn current(&self) -> Option<Row<'_>> {
        match self {
            Self::Slice(iterator) => iterator.current(),
            Self::Chunk(iterator) => iterator.current(),
            Self::List(iterator) => iterator.current(),
            Self::RowPtr(iterator) => iterator.current(),
            Self::RowContainer(iterator) => iterator.current(),
        }
    }

    /// Go `ReachEnd`.
    pub fn reach_end(&mut self) {
        match self {
            Self::Slice(iterator) => iterator.reach_end(),
            Self::Chunk(iterator) => iterator.reach_end(),
            Self::List(iterator) => iterator.reach_end(),
            Self::RowPtr(iterator) => iterator.reach_end(),
            Self::RowContainer(iterator) => iterator.reach_end(),
        }
    }

    /// Go `Len`.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Slice(iterator) => iterator.len(),
            Self::Chunk(iterator) => iterator.len(),
            Self::List(iterator) => iterator.len(),
            Self::RowPtr(iterator) => iterator.len(),
            Self::RowContainer(iterator) => iterator.len(),
        }
    }

    /// Whether the source contains no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Go `Error`. Only the row-container variant can fail.
    #[must_use]
    pub fn error(&self) -> Option<&str> {
        match self {
            Self::RowContainer(iterator) => iterator.error(),
            Self::Slice(_) | Self::Chunk(_) | Self::List(_) | Self::RowPtr(_) => None,
        }
    }
}

/// Go `multiIterator` over lending chunk and row-container sources.
///
/// Empty sources are removed at construction. If a spilled source fails,
/// iteration stops immediately, latches that error, and never advances into a
/// following source.
pub struct LendingMultiIterator<'a> {
    iters: Vec<LendingIterator<'a>>,
    length: usize,
    cur_ptr: usize,
    remaining_in_current: usize,
}

impl<'a> LendingMultiIterator<'a> {
    /// Go `NewMultiIterator`.
    #[must_use]
    pub fn new(iters: Vec<LendingIterator<'a>>) -> Self {
        let mut kept = Vec::new();
        let mut length = 0;
        for iterator in iters {
            if !iterator.is_empty() {
                length += iterator.len();
                kept.push(iterator);
            }
        }
        Self {
            iters: kept,
            length,
            cur_ptr: 0,
            remaining_in_current: 0,
        }
    }

    /// Go `Len`.
    #[must_use]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Whether all inputs are empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Go `Begin`.
    pub fn begin(&mut self) -> Option<Row<'_>> {
        self.cur_ptr = 0;
        if self.iters.is_empty() {
            return None;
        }
        if self.iters[0].error().is_some() {
            self.remaining_in_current = 0;
            return None;
        }
        self.remaining_in_current = self.iters[0].len();
        self.remaining_in_current -= 1;
        self.iters[0].begin()
    }

    /// Go `Next`.
    pub fn next_row(&mut self) -> Option<Row<'_>> {
        if self.cur_ptr >= self.iters.len() || self.iters[self.cur_ptr].error().is_some() {
            return None;
        }
        if self.remaining_in_current == 0 {
            self.cur_ptr += 1;
            if self.cur_ptr == self.iters.len() {
                return None;
            }
            self.remaining_in_current = self.iters[self.cur_ptr].len();
            self.remaining_in_current -= 1;
            return self.iters[self.cur_ptr].begin();
        }
        self.remaining_in_current -= 1;
        self.iters[self.cur_ptr].next_row()
    }

    /// Go `Current`.
    #[must_use]
    pub fn current(&self) -> Option<Row<'_>> {
        self.iters
            .get(self.cur_ptr)
            .and_then(LendingIterator::current)
    }

    /// Go `ReachEnd`.
    pub fn reach_end(&mut self) {
        self.cur_ptr = self.iters.len();
    }

    /// Go `Error`.
    #[must_use]
    pub fn error(&self) -> Option<&str> {
        self.iters
            .get(self.cur_ptr)
            .and_then(LendingIterator::error)
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
    /// rest end to end, across chunk and list sources. Row-container
    /// composition is pinned separately through the lending form below.
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
    /// rewinds. The empty-source cases close the file. Row-container
    /// iteration is pinned separately through the lending form below.
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

    fn collect_lending(iterator: &mut LendingMultiIterator<'_>) -> Vec<i64> {
        let mut values = Vec::new();
        if let Some(row) = iterator.begin() {
            values.push(row.get_int64(0));
        }
        while let Some(row) = iterator.next_row() {
            values.push(row.get_int64(0));
        }
        assert_eq!(iterator.error(), None);
        values
    }

    /// Go `TestSel`: a selected row container followed by a selected trailing
    /// chunk is one logical iterator both before and after the container
    /// spills. This is the composition `MergeJoinTable` relies on for an
    /// equal-key group that crosses a chunk boundary.
    #[test]
    fn selected_row_container_and_trailing_chunk_compose_before_and_after_spill() {
        let fields = long_long_fields();
        let mut container = RowContainer::new(&fields, 4, crate::test_temp_storage::storage());
        for base in (0..60i64).step_by(4) {
            let mut chunk = Chunk::new_with_capacity(&fields, 4);
            for value in base..base + 4 {
                chunk.append_int64(0, value);
            }
            chunk.set_sel(Some(vec![0, 2]));
            container.add(chunk).unwrap();
        }
        let mut trailing = Chunk::new_with_capacity(&fields, 4);
        for value in 60..64 {
            trailing.append_int64(0, value);
        }
        trailing.set_sel(Some(vec![0, 1, 2]));

        let mut expected: Vec<i64> = (0..60).step_by(2).collect();
        expected.extend([60, 61, 62]);
        {
            let mut iterator = LendingMultiIterator::new(vec![
                LendingIterator::row_container(&container),
                LendingIterator::chunk(&trailing),
            ]);
            assert_eq!(collect_lending(&mut iterator), expected);
        }

        container.spill_to_disk();
        assert!(container.already_spilled());
        {
            let mut iterator = LendingMultiIterator::new(vec![
                LendingIterator::row_container(&container),
                LendingIterator::chunk(&trailing),
            ]);
            assert_eq!(collect_lending(&mut iterator), expected);
        }
        container.close();
    }

    /// A nonempty failed row-container source stops a multi iterator before
    /// any later chunk. The read error remains available through `Error`,
    /// which is the merge join's disk-failure boundary.
    #[test]
    fn a_row_container_error_is_latched_before_the_following_source() {
        let fields = long_long_fields();
        let mut container = RowContainer::new(&fields, 4, crate::test_temp_storage::storage());
        let mut stored = Chunk::new_with_capacity(&fields, 4);
        stored.append_int64(0, 1);
        container.add(stored).unwrap();
        container.spill_to_disk();
        container.set_spill_error_for_test("injected iterator spill failure");
        assert_eq!(
            container.spill_error().as_deref(),
            Some("injected iterator spill failure")
        );

        let mut trailing = Chunk::new_with_capacity(&fields, 1);
        trailing.append_int64(0, 99);
        let mut iterator = LendingMultiIterator::new(vec![
            LendingIterator::row_container(&container),
            LendingIterator::chunk(&trailing),
        ]);
        assert!(iterator.begin().is_none());
        assert_eq!(iterator.error(), Some("injected iterator spill failure"));
        assert!(iterator.next_row().is_none());
        assert_eq!(iterator.error(), Some("injected iterator spill failure"));

        container.close();
    }

    /// Go `NewMultiIterator` removes every zero-length source. A historical
    /// spill error on an empty reset container therefore cannot mask a later
    /// valid source.
    #[test]
    fn an_empty_container_with_a_historical_error_is_omitted() {
        let fields = long_long_fields();
        let container = RowContainer::new(&fields, 4, crate::test_temp_storage::storage());
        container.set_spill_error_for_test("historical spill failure");

        let mut trailing = Chunk::new_with_capacity(&fields, 1);
        trailing.append_int64(0, 99);
        let mut iterator = LendingMultiIterator::new(vec![
            LendingIterator::row_container(&container),
            LendingIterator::chunk(&trailing),
        ]);

        assert_eq!(collect_lending(&mut iterator), vec![99]);
    }
}
