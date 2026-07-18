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

//! Direct source vectors for the transaction dirty/snapshot union iterator.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use tidb_txnkv::{Key, KvIterator, UnionIter};

#[derive(Clone, Debug)]
struct ErrorToken(Rc<()>);

impl ErrorToken {
    fn new() -> Self {
        Self(Rc::new(()))
    }

    fn is_same(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }
}

#[derive(Clone)]
struct Entry {
    key: Key,
    value: Vec<u8>,
}

fn entry(key: &str, value: &str) -> Entry {
    Entry {
        key: Key::from_bytes(key.as_bytes()),
        value: if value == "nil" {
            Vec::new()
        } else {
            value.as_bytes().to_vec()
        },
    }
}

struct MockIter {
    entries: Vec<Entry>,
    index: usize,
    injected_error: Rc<RefCell<Option<ErrorToken>>>,
    next_calls: Rc<Cell<usize>>,
    close_calls: Rc<Cell<usize>>,
}

impl MockIter {
    fn new(entries: Vec<Entry>) -> (Self, MockControl) {
        let next_calls = Rc::new(Cell::new(0));
        let close_calls = Rc::new(Cell::new(0));
        let injected_error = Rc::new(RefCell::new(None));
        let control = MockControl {
            next_calls: Rc::clone(&next_calls),
            close_calls: Rc::clone(&close_calls),
            injected_error: Rc::clone(&injected_error),
        };
        (
            Self {
                entries,
                index: 0,
                injected_error,
                next_calls,
                close_calls,
            },
            control,
        )
    }

    fn with_error(entries: Vec<Entry>, error: Option<ErrorToken>) -> (Self, MockControl) {
        let (iterator, control) = Self::new(entries);
        *iterator.injected_error.borrow_mut() = error;
        (iterator, control)
    }
}

struct MockControl {
    next_calls: Rc<Cell<usize>>,
    close_calls: Rc<Cell<usize>>,
    injected_error: Rc<RefCell<Option<ErrorToken>>>,
}

impl MockControl {
    fn inject_error(&self, error: Option<ErrorToken>) {
        *self.injected_error.borrow_mut() = error;
    }
}

impl KvIterator for MockIter {
    type Error = ErrorToken;

    fn valid(&self) -> bool {
        self.index < self.entries.len()
    }

    fn key(&self) -> &Key {
        &self.entries[self.index].key
    }

    fn value(&self) -> &[u8] {
        &self.entries[self.index].value
    }

    fn next(&mut self) -> Result<(), Self::Error> {
        self.next_calls.set(self.next_calls.get() + 1);
        if let Some(error) = self.injected_error.borrow().as_ref() {
            return Err(error.clone());
        }
        self.index += 1;
        Ok(())
    }

    fn close(&mut self) {
        self.close_calls.set(self.close_calls.get() + 1);
    }
}

fn reverse(mut entries: Vec<Entry>) -> Vec<Entry> {
    entries.reverse();
    entries
}

fn collect(iterator: &mut impl KvIterator<Error = ErrorToken>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut records = Vec::new();
    while iterator.valid() {
        records.push((
            iterator.key().as_bytes().to_vec(),
            iterator.value().to_vec(),
        ));
        iterator.next().expect("union iteration must succeed");
    }
    records
}

fn assert_union(dirty: Vec<Entry>, snapshot: Vec<Entry>, expected: Vec<Entry>) {
    for reverse_order in [false, true] {
        let dirty = if reverse_order {
            reverse(dirty.clone())
        } else {
            dirty.clone()
        };
        let snapshot = if reverse_order {
            reverse(snapshot.clone())
        } else {
            snapshot.clone()
        };
        let expected = if reverse_order {
            reverse(expected.clone())
        } else {
            expected.clone()
        };

        let (dirty, dirty_control) = MockIter::new(dirty);
        let (snapshot, snapshot_control) = MockIter::new(snapshot);
        let mut union = UnionIter::new(dirty, snapshot, reverse_order).unwrap();
        let actual = collect(&mut union);
        let expected: Vec<_> = expected
            .into_iter()
            .map(|entry| (entry.key.into_bytes(), entry.value))
            .collect();
        assert_eq!(actual, expected);

        assert_eq!(dirty_control.close_calls.get(), 0);
        assert_eq!(snapshot_control.close_calls.get(), 0);
        union.close();
        assert_eq!(dirty_control.close_calls.get(), 1);
        assert_eq!(snapshot_control.close_calls.get(), 1);
        union.close();
        assert_eq!(dirty_control.close_calls.get(), 1);
        assert_eq!(snapshot_control.close_calls.get(), 1);
    }
}

#[test]
fn test_union_iter_source_cases_forward_and_reverse() {
    let snapshot = vec![
        entry("k00", "v0"),
        entry("k01", "v1"),
        entry("k03", "v3"),
        entry("k06", "v6"),
        entry("k10", "v10"),
        entry("k12", "v12"),
        entry("k15", "v15"),
        entry("k16", "v16"),
    ];
    let dirty = vec![
        entry("k00", ""),
        entry("k000", ""),
        entry("k03", "x3"),
        entry("k05", "x5"),
        entry("k07", "x7"),
        entry("k08", "x8"),
    ];

    assert_union(
        dirty.clone(),
        vec![],
        vec![
            entry("k03", "x3"),
            entry("k05", "x5"),
            entry("k07", "x7"),
            entry("k08", "x8"),
        ],
    );
    assert_union(vec![], snapshot.clone(), snapshot.clone());
    assert_union(
        dirty,
        snapshot.clone(),
        vec![
            entry("k01", "v1"),
            entry("k03", "x3"),
            entry("k05", "x5"),
            entry("k06", "v6"),
            entry("k07", "x7"),
            entry("k08", "x8"),
            entry("k10", "v10"),
            entry("k12", "v12"),
            entry("k15", "v15"),
            entry("k16", "v16"),
        ],
    );

    let dirty = vec![
        entry("k03", "x3"),
        entry("k05", "x5"),
        entry("k07", "x7"),
        entry("k08", "x8"),
        entry("k17", "x17"),
        entry("k18", "x18"),
    ];
    assert_union(
        dirty,
        snapshot,
        vec![
            entry("k00", "v0"),
            entry("k01", "v1"),
            entry("k03", "x3"),
            entry("k05", "x5"),
            entry("k06", "v6"),
            entry("k07", "x7"),
            entry("k08", "x8"),
            entry("k10", "v10"),
            entry("k12", "v12"),
            entry("k15", "v15"),
            entry("k16", "v16"),
            entry("k17", "x17"),
            entry("k18", "x18"),
        ],
    );
}

#[test]
fn test_union_iter_source_error_identity_order_and_close() {
    struct Case {
        dirty: Vec<Entry>,
        snapshot: Vec<Entry>,
        fail_during_next: bool,
        dirty_error: bool,
        snapshot_error: bool,
        dirty_next_calls: usize,
        snapshot_next_calls: usize,
    }

    let cases = vec![
        Case {
            dirty: vec![entry("k0", ""), entry("k1", "v1")],
            snapshot: vec![],
            fail_during_next: false,
            dirty_error: true,
            snapshot_error: false,
            dirty_next_calls: 1,
            snapshot_next_calls: 0,
        },
        Case {
            dirty: vec![entry("k0", ""), entry("k1", "v1")],
            snapshot: vec![entry("k1", "x1")],
            fail_during_next: false,
            dirty_error: true,
            snapshot_error: false,
            dirty_next_calls: 1,
            snapshot_next_calls: 0,
        },
        Case {
            dirty: vec![entry("k0", "v1"), entry("k1", "v1")],
            snapshot: vec![entry("k0", "x0"), entry("k1", "x1")],
            fail_during_next: false,
            dirty_error: false,
            snapshot_error: true,
            dirty_next_calls: 0,
            snapshot_next_calls: 1,
        },
        Case {
            dirty: vec![entry("k0", ""), entry("k1", "v1")],
            snapshot: vec![entry("k0", "x0"), entry("k1", "x1")],
            fail_during_next: false,
            dirty_error: true,
            snapshot_error: false,
            dirty_next_calls: 1,
            snapshot_next_calls: 0,
        },
        Case {
            dirty: vec![entry("k0", ""), entry("k1", "v1")],
            snapshot: vec![entry("k0", "x0"), entry("k1", "x1")],
            fail_during_next: false,
            dirty_error: false,
            snapshot_error: true,
            dirty_next_calls: 1,
            snapshot_next_calls: 1,
        },
        Case {
            dirty: vec![entry("k0", "v0"), entry("k1", "v1")],
            snapshot: vec![entry("k1", "x1")],
            fail_during_next: true,
            dirty_error: true,
            snapshot_error: false,
            dirty_next_calls: 1,
            snapshot_next_calls: 0,
        },
        Case {
            dirty: vec![entry("k1", "v1")],
            snapshot: vec![entry("k0", "x0"), entry("k1", "x1")],
            fail_during_next: true,
            dirty_error: false,
            snapshot_error: true,
            dirty_next_calls: 0,
            snapshot_next_calls: 1,
        },
        Case {
            dirty: vec![entry("k0", "v0"), entry("k1", "v1")],
            snapshot: vec![entry("k1", "x1")],
            fail_during_next: true,
            dirty_error: true,
            snapshot_error: false,
            dirty_next_calls: 1,
            snapshot_next_calls: 0,
        },
        Case {
            dirty: vec![entry("k1", "v1")],
            snapshot: vec![entry("k0", "x0"), entry("k1", "x1")],
            fail_during_next: true,
            dirty_error: false,
            snapshot_error: true,
            dirty_next_calls: 0,
            snapshot_next_calls: 1,
        },
    ];

    for case in cases {
        let dirty_error = case.dirty_error.then(ErrorToken::new);
        let snapshot_error = case.snapshot_error.then(ErrorToken::new);

        if case.fail_during_next {
            let (dirty, dirty_control) = MockIter::new(case.dirty);
            let (snapshot, snapshot_control) = MockIter::new(case.snapshot);
            let mut union = UnionIter::new(dirty, snapshot, false).unwrap();
            dirty_control.inject_error(dirty_error.clone());
            snapshot_control.inject_error(snapshot_error.clone());
            let error = union
                .next()
                .expect_err("next must return the injected error");
            let expected = dirty_error.as_ref().or(snapshot_error.as_ref()).unwrap();
            assert!(error.is_same(expected));
            assert_eq!(dirty_control.next_calls.get(), case.dirty_next_calls);
            assert_eq!(snapshot_control.next_calls.get(), case.snapshot_next_calls);
            assert_eq!(dirty_control.close_calls.get(), 0);
            assert_eq!(snapshot_control.close_calls.get(), 0);
            union.close();
            assert_eq!(dirty_control.close_calls.get(), 1);
            assert_eq!(snapshot_control.close_calls.get(), 1);
        } else {
            let (dirty, dirty_control) = MockIter::with_error(case.dirty, dirty_error.clone());
            let (snapshot, snapshot_control) =
                MockIter::with_error(case.snapshot, snapshot_error.clone());
            let init_error = match UnionIter::new(dirty, snapshot, false) {
                Ok(_) => panic!("constructor must return the injected error"),
                Err(error) => error,
            };
            let (error, mut dirty, mut snapshot) = init_error.into_parts();
            let expected = dirty_error.as_ref().or(snapshot_error.as_ref()).unwrap();
            assert!(error.is_same(expected));
            assert_eq!(dirty_control.next_calls.get(), case.dirty_next_calls);
            assert_eq!(snapshot_control.next_calls.get(), case.snapshot_next_calls);
            assert_eq!(dirty_control.close_calls.get(), 0);
            assert_eq!(snapshot_control.close_calls.get(), 0);
            dirty.close();
            snapshot.close();
            assert_eq!(dirty_control.close_calls.get(), 1);
            assert_eq!(snapshot_control.close_calls.get(), 1);
        }
    }
}
