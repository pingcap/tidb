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

//! Ports of `pkg/util/chunk/iterator_test.go`.

use tidb_datatype::{FieldType, FieldTypeCode};

use crate::chunk::Chunk;
use crate::iterator::{
    ChunkIterator, Iterator4Chunk, Iterator4List, Iterator4RowPtr, Iterator4Slice, MultiIterator,
};
use crate::list::List;

fn int_fields() -> Vec<FieldType> {
    vec![FieldType::new(FieldTypeCode::LongLong)]
}

/// Go `TestIteratorOnSel` (iterator_test.go): iterating a chunk with a
/// selection visits only the selected rows, in selection order.
#[test]
fn iterator_on_sel() {
    let fields = int_fields();
    let mut chk = Chunk::new(&fields, 32, 1024);
    let mut sel = Vec::with_capacity(1024);
    for i in 0..1024 {
        chk.append_int64(0, i as i64);
        if i % 2 == 0 {
            sel.push(i);
        }
    }
    chk.set_sel(Some(sel));
    let mut it = Iterator4Chunk::new(&chk);
    let mut cnt = 0;
    let mut row = it.begin();
    while row.is_some() {
        assert_eq!(row.unwrap().get_int64(0) % 2, 0);
        cnt += 1;
        row = it.next_row();
    }
    assert_eq!(cnt, 1024 / 2);
}

/// Go `TestMultiIterator` (iterator_test.go). The row-container variants are
/// pinned by the lending-iterator contract tests; here the chunk and list
/// sources cover Go's concatenation-order contract.
#[test]
fn multi_iterator() {
    fn check_equal<'a>(mut it: MultiIterator<'a>, expected: &[i64]) {
        assert_eq!(expected.len(), ChunkIterator::len(&it));
        let mut collected = Vec::new();
        let mut row = it.begin();
        while let Some(r) = row {
            collected.push(r.get_int64(0));
            row = it.next_row();
        }
        assert_eq!(collected, expected);
    }

    let empty = Chunk::default();
    let mut it = MultiIterator::new(vec![Box::new(Iterator4Chunk::new(&empty))]);
    assert_eq!(it.end(), it.begin());

    let empty_list = List::new(&int_fields(), 1, 1);
    let mut it = MultiIterator::new(vec![
        Box::new(Iterator4Chunk::new(&empty)),
        Box::new(Iterator4List::new(&empty_list)),
    ]);
    assert_eq!(it.end(), it.begin());

    let fields = int_fields();
    let mut chk = Chunk::new(&fields, 32, 1024);
    let n = 10;
    let mut expected: Vec<i64> = Vec::with_capacity(n);
    for i in 0..n as i64 {
        chk.append_int64(0, i);
        expected.push(i);
    }
    let it = MultiIterator::new(vec![Box::new(Iterator4Chunk::new(&chk))]);
    check_equal(it, &expected);

    let it = MultiIterator::new(vec![
        Box::new(Iterator4Chunk::new(&empty)),
        Box::new(Iterator4Chunk::new(&chk)),
        Box::new(Iterator4Chunk::new(&empty)),
    ]);
    check_equal(it, &expected);

    let mut li = List::new(&fields, 32, 1024);
    let mut chk2 = Chunk::new(&fields, 32, 1024);
    for i in (n as i64)..((n * 2) as i64) {
        expected.push(i);
        chk2.append_int64(0, i);
    }
    li.add(chk2.clone());

    let it = MultiIterator::new(vec![
        Box::new(Iterator4Chunk::new(&chk)),
        Box::new(Iterator4Chunk::new(&chk2)),
    ]);
    check_equal(it, &expected);
    let it = MultiIterator::new(vec![
        Box::new(Iterator4Chunk::new(&chk)),
        Box::new(Iterator4List::new(&li)),
    ]);
    check_equal(it, &expected);

    li.clear();
    li.add(chk.clone());
    let it = MultiIterator::new(vec![
        Box::new(Iterator4List::new(&li)),
        Box::new(Iterator4Chunk::new(&chk2)),
    ]);
    // After `Clear` + `Add(chk)` the list holds 0..10 and chk2 holds 10..20,
    // which is exactly `expected`.
    check_equal(it, &expected);
}

/// Go `TestIterator` (iterator_test.go): every iterator flavor walks its
/// source, supports `Begin`/`Current`/`Next`/`ReachEnd`, and ends on an empty
/// source. The row-container flavor is exercised by the lending-iterator
/// contract tests.
#[test]
fn iterator_flavors() {
    let fields = int_fields();
    let mut chk = Chunk::new(&fields, 32, 1024);
    let n = 10;
    for i in 0..n as i64 {
        chk.append_int64(0, i);
    }

    let mut li = List::new(&fields, 1, 2);
    let mut li2 = List::new(&fields, 8, 16);
    let mut rows = Vec::with_capacity(n);
    let mut ptrs = Vec::with_capacity(n);
    let mut ptrs2 = Vec::with_capacity(n);
    for i in 0..n {
        rows.push(chk.get_row(i));
        ptrs.push(li.append_row(chk.get_row(i)));
        ptrs2.push(li2.append_row(chk.get_row(i)));
    }

    macro_rules! check_equal {
        ($it:expr, $expected:expr) => {{
            let mut it = $it;
            assert_eq!($expected.len(), ChunkIterator::len(&it));
            let mut collected = Vec::new();
            let mut row = it.begin();
            while let Some(r) = row {
                collected.push(r.get_int64(0));
                row = it.next_row();
            }
            assert_eq!(collected, $expected);
            it
        }};
    }

    // Slice iterator.
    let expected: Vec<i64> = (0..n as i64).collect();
    let mut it = check_equal!(Iterator4Slice::new(rows.clone()), expected);
    it.begin();
    for i in 0..5 {
        assert_eq!(it.current().unwrap().get_int64(0), rows[i].get_int64(0));
        it.next_row();
    }
    it.reach_end();
    assert_eq!(it.end(), it.current());
    let mut it = Iterator4Slice::new(rows.clone());
    assert_eq!(it.begin().unwrap().get_int64(0), rows[0].get_int64(0));

    // Chunk iterator.
    let mut it = check_equal!(Iterator4Chunk::new(&chk), expected);
    it.begin();
    for i in 0..5 {
        assert_eq!(it.current().unwrap().get_int64(0), chk.get_row(i).get_int64(0));
        it.next_row();
    }
    it.reach_end();
    assert_eq!(it.end(), it.current());
    let mut it = Iterator4Chunk::new(&chk);
    assert_eq!(it.begin().unwrap().get_int64(0), chk.get_row(0).get_int64(0));

    // List iterator.
    let mut it = check_equal!(Iterator4List::new(&li), expected);
    it.begin();
    for i in 0..5 {
        assert_eq!(
            it.current().unwrap().get_int64(0),
            li.get_row(ptrs[i]).get_int64(0)
        );
        it.next_row();
    }
    it.reach_end();
    assert_eq!(it.end(), it.current());
    let mut it = Iterator4List::new(&li);
    assert_eq!(
        it.begin().unwrap().get_int64(0),
        li.get_row(ptrs[0]).get_int64(0)
    );

    // RowPtr iterators over both chunk-size configurations.
    let mut it = check_equal!(Iterator4RowPtr::new(&li, ptrs.clone()), expected);
    it.begin();
    for i in 0..5 {
        assert_eq!(
            it.current().unwrap().get_int64(0),
            li.get_row(ptrs[i]).get_int64(0)
        );
        it.next_row();
    }
    it.reach_end();
    assert_eq!(it.end(), it.current());
    let mut it = Iterator4RowPtr::new(&li, ptrs.clone());
    assert_eq!(
        it.begin().unwrap().get_int64(0),
        li.get_row(ptrs[0]).get_int64(0)
    );

    let it = check_equal!(Iterator4RowPtr::new(&li2, ptrs2.clone()), expected);
    drop(it);

    // Empty sources all end immediately.
    let mut it = Iterator4Slice::new(vec![]);
    assert_eq!(it.end(), it.begin());
    let empty_chunk = Chunk::default();
    let mut it = Iterator4Chunk::new(&empty_chunk);
    assert_eq!(it.end(), it.begin());
    let empty_list = List::new(&fields, 1, 1);
    let mut it = Iterator4List::new(&empty_list);
    assert_eq!(it.end(), it.begin());
    let mut it = Iterator4RowPtr::new(&li, vec![]);
    assert_eq!(it.end(), it.begin());
}
