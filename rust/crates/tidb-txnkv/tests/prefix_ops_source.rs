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

//! Direct source tests for `pkg/util/prefix_helper.go`.

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;

use tidb_txnkv::driver::read::{
    TransactionBuffer, TransactionReadDriver, TransactionReadError, TransactionSnapshot,
};
use tidb_txnkv::{
    del_key_with_prefix, row_key_prefix_filter, scan_meta_with_prefix, BatchBufferGetter,
    BatchGetError, BatchGetOptions, BatchGetter, GetOptions, Getter, Key, KvIterator, ValueEntry,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TestError {
    NotFound,
    Next,
}

impl BatchGetError for TestError {
    fn is_not_found(&self) -> bool {
        *self == Self::NotFound
    }
}

impl TransactionReadError for TestError {
    fn not_found() -> Self {
        Self::NotFound
    }
}

struct VecIterator {
    entries: Vec<(Key, Vec<u8>)>,
    index: usize,
    next_error: Option<TestError>,
    close_calls: Rc<Cell<usize>>,
}

impl KvIterator for VecIterator {
    type Error = TestError;

    fn valid(&self) -> bool {
        self.index < self.entries.len()
    }

    fn key(&self) -> &Key {
        &self.entries[self.index].0
    }

    fn value(&self) -> &[u8] {
        &self.entries[self.index].1
    }

    fn next(&mut self) -> Result<(), Self::Error> {
        if let Some(error) = self.next_error.take() {
            return Err(error);
        }
        self.index += 1;
        Ok(())
    }

    fn close(&mut self) {
        self.close_calls.set(self.close_calls.get() + 1);
    }
}

struct MockBuffer {
    values: BTreeMap<Key, Vec<u8>>,
    next_error: Option<TestError>,
    close_calls: Rc<Cell<usize>>,
}

impl MockBuffer {
    fn new(close_calls: Rc<Cell<usize>>) -> Self {
        Self {
            values: BTreeMap::new(),
            next_error: None,
            close_calls,
        }
    }
}

impl Getter for MockBuffer {
    type Error = TestError;

    fn get(&mut self, key: &Key, _options: GetOptions) -> Result<ValueEntry, Self::Error> {
        self.values
            .get(key)
            .cloned()
            .map(|value| ValueEntry::new(value, 0))
            .ok_or(TestError::NotFound)
    }
}

impl BatchBufferGetter for MockBuffer {
    type Error = TestError;

    fn batch_get(
        &mut self,
        keys: &[Key],
        _options: BatchGetOptions,
    ) -> Result<HashMap<Key, ValueEntry>, Self::Error> {
        Ok(keys
            .iter()
            .filter_map(|key| {
                self.values
                    .get(key)
                    .map(|value| (key.clone(), ValueEntry::new(value.clone(), 0)))
            })
            .collect())
    }

    fn len(&self) -> usize {
        self.values.len()
    }
}

impl TransactionBuffer for MockBuffer {
    type Iter = VecIterator;

    fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), TestError> {
        self.values.insert(key, value);
        Ok(())
    }

    fn delete(&mut self, key: Key) -> Result<(), TestError> {
        self.values.insert(key, Vec::new());
        Ok(())
    }

    fn iter(&mut self, start: &Key, upper_bound: Option<&Key>) -> Result<Self::Iter, TestError> {
        Ok(VecIterator {
            entries: range_entries(&self.values, start, upper_bound),
            index: 0,
            next_error: self.next_error.take(),
            close_calls: Rc::clone(&self.close_calls),
        })
    }

    fn iter_reverse(
        &mut self,
        _start: Option<&Key>,
        _lower_bound: Option<&Key>,
    ) -> Result<Self::Iter, TestError> {
        unreachable!("prefix helpers only create forward iterators")
    }
}

struct MockSnapshot {
    values: BTreeMap<Key, Vec<u8>>,
    close_calls: Rc<Cell<usize>>,
}

impl MockSnapshot {
    fn new(entries: &[(&[u8], &[u8])], close_calls: Rc<Cell<usize>>) -> Self {
        Self {
            values: entries
                .iter()
                .map(|(key, value)| (Key::from_bytes(*key), value.to_vec()))
                .collect(),
            close_calls,
        }
    }
}

impl Getter for MockSnapshot {
    type Error = TestError;

    fn get(&mut self, key: &Key, _options: GetOptions) -> Result<ValueEntry, Self::Error> {
        self.values
            .get(key)
            .cloned()
            .map(|value| ValueEntry::new(value, 0))
            .ok_or(TestError::NotFound)
    }
}

impl BatchGetter for MockSnapshot {
    type Error = TestError;

    fn batch_get(
        &mut self,
        keys: &[Key],
        options: BatchGetOptions,
    ) -> Result<HashMap<Key, ValueEntry>, Self::Error> {
        Ok(keys
            .iter()
            .filter_map(|key| {
                self.get(key, options.into())
                    .ok()
                    .map(|value| (key.clone(), value))
            })
            .collect())
    }
}

impl TransactionSnapshot for MockSnapshot {
    type Iter = VecIterator;

    fn iter(&mut self, start: &Key, upper_bound: Option<&Key>) -> Result<Self::Iter, TestError> {
        Ok(VecIterator {
            entries: range_entries(&self.values, start, upper_bound),
            index: 0,
            next_error: None,
            close_calls: Rc::clone(&self.close_calls),
        })
    }

    fn iter_reverse(
        &mut self,
        _start: Option<&Key>,
        _lower_bound: Option<&Key>,
    ) -> Result<Self::Iter, TestError> {
        unreachable!("prefix helpers only create forward iterators")
    }
}

fn range_entries(
    values: &BTreeMap<Key, Vec<u8>>,
    start: &Key,
    upper_bound: Option<&Key>,
) -> Vec<(Key, Vec<u8>)> {
    values
        .iter()
        .filter(|(key, _)| *key >= start && upper_bound.is_none_or(|upper| *key < upper))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn key(bytes: impl Into<Vec<u8>>) -> Key {
    Key::from_bytes(bytes)
}

#[test]
fn delete_empty_transaction_matches_test_prefix_first_transaction() {
    let buffer_closes = Rc::new(Cell::new(0));
    let snapshot_closes = Rc::new(Cell::new(0));
    let buffer = MockBuffer::new(Rc::clone(&buffer_closes));
    let snapshot = MockSnapshot::new(&[], Rc::clone(&snapshot_closes));
    let mut transaction = TransactionReadDriver::new(buffer, snapshot);

    del_key_with_prefix(&mut transaction, &key(b"10000000".to_vec())).unwrap();

    assert_eq!(buffer_closes.get(), 1);
    assert_eq!(snapshot_closes.get(), 1);
}

#[test]
fn prefix_scan_and_delete_match_test_prefix() {
    let buffer_closes = Rc::new(Cell::new(0));
    let snapshot_closes = Rc::new(Cell::new(0));
    let buffer = MockBuffer::new(Rc::clone(&buffer_closes));
    let snapshot = MockSnapshot::new(
        &[
            (b"key-old".as_slice(), b"snapshot".as_slice()),
            (b"other".as_slice(), b"keep".as_slice()),
        ],
        Rc::clone(&snapshot_closes),
    );
    let mut transaction = TransactionReadDriver::new(buffer, snapshot);
    let exact_key = key(b"key100jfowi878230".to_vec());
    transaction
        .set(exact_key.clone(), b"val32dfaskli384757^*&%^".to_vec())
        .unwrap();

    let mut seen = Vec::new();
    scan_meta_with_prefix(&mut transaction, &exact_key, |current, value| {
        seen.push((current.clone(), value.to_vec()));
        true
    })
    .unwrap();
    assert_eq!(
        seen,
        vec![(exact_key.clone(), b"val32dfaskli384757^*&%^".to_vec())]
    );

    let mut rejected_calls = 0;
    scan_meta_with_prefix(&mut transaction, &exact_key, |_, _| {
        rejected_calls += 1;
        false
    })
    .unwrap();
    assert_eq!(rejected_calls, 1);

    del_key_with_prefix(&mut transaction, &key(b"key".to_vec())).unwrap();
    assert_eq!(
        transaction.get(&exact_key, GetOptions::default()),
        Err(TestError::NotFound)
    );
    assert_eq!(
        transaction.get(&key(b"key-old".to_vec()), GetOptions::default()),
        Err(TestError::NotFound)
    );
    assert_eq!(
        transaction
            .get(&key(b"other".to_vec()), GetOptions::default())
            .unwrap()
            .value,
        b"keep"
    );
    assert_eq!(buffer_closes.get(), 3);
    assert_eq!(snapshot_closes.get(), 3);
}

#[test]
fn prefix_filter_matches_embedded_nul_test_prefix_filter() {
    let mut row_key = b"test@#$%l(le[0]..prefix) 2uio".to_vec();
    row_key[8] = 0;
    row_key[9] = 0;
    let filter = row_key_prefix_filter(key(row_key.clone()));

    let mut child = row_key;
    child.extend_from_slice(b"akjdf3*(34");
    assert!(!filter(&key(child)));
    assert!(filter(&key(b"sjfkdlsaf".to_vec())));
}

#[test]
fn scan_propagates_next_error_and_still_closes_both_iterators() {
    let buffer_closes = Rc::new(Cell::new(0));
    let snapshot_closes = Rc::new(Cell::new(0));
    let mut buffer = MockBuffer::new(Rc::clone(&buffer_closes));
    buffer
        .values
        .insert(key(b"key-1".to_vec()), b"value".to_vec());
    buffer.next_error = Some(TestError::Next);
    let snapshot = MockSnapshot::new(&[], Rc::clone(&snapshot_closes));
    let mut transaction = TransactionReadDriver::new(buffer, snapshot);

    assert_eq!(
        scan_meta_with_prefix(&mut transaction, &key(b"key".to_vec()), |_, _| true),
        Err(TestError::Next)
    );
    assert_eq!(buffer_closes.get(), 1);
    assert_eq!(snapshot_closes.get(), 1);
}

#[test]
fn delete_next_error_closes_without_applying_collected_deletes() {
    let buffer_closes = Rc::new(Cell::new(0));
    let snapshot_closes = Rc::new(Cell::new(0));
    let mut buffer = MockBuffer::new(Rc::clone(&buffer_closes));
    let dirty_key = key(b"key-dirty".to_vec());
    buffer
        .values
        .insert(dirty_key.clone(), b"dirty-value".to_vec());
    buffer.next_error = Some(TestError::Next);
    let snapshot_key = key(b"key-snapshot".to_vec());
    let snapshot = MockSnapshot::new(
        &[(snapshot_key.as_bytes(), b"snapshot-value".as_slice())],
        Rc::clone(&snapshot_closes),
    );
    let mut transaction = TransactionReadDriver::new(buffer, snapshot);

    assert_eq!(
        del_key_with_prefix(&mut transaction, &key(b"key".to_vec())),
        Err(TestError::Next)
    );
    assert_eq!(
        transaction
            .get(&dirty_key, GetOptions::default())
            .unwrap()
            .value,
        b"dirty-value"
    );
    assert_eq!(
        transaction
            .get(&snapshot_key, GetOptions::default())
            .unwrap()
            .value,
        b"snapshot-value"
    );
    assert_eq!(buffer_closes.get(), 1);
    assert_eq!(snapshot_closes.get(), 1);
}

#[test]
fn scan_callback_panic_still_closes_both_iterators() {
    let buffer_closes = Rc::new(Cell::new(0));
    let snapshot_closes = Rc::new(Cell::new(0));
    let mut buffer = MockBuffer::new(Rc::clone(&buffer_closes));
    buffer
        .values
        .insert(key(b"key-1".to_vec()), b"value".to_vec());
    let snapshot = MockSnapshot::new(&[], Rc::clone(&snapshot_closes));
    let mut transaction = TransactionReadDriver::new(buffer, snapshot);

    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _ = scan_meta_with_prefix(&mut transaction, &key(b"key".to_vec()), |_, _| {
            panic!("injected callback panic")
        });
    }));

    assert!(panic.is_err());
    assert_eq!(buffer_closes.get(), 1);
    assert_eq!(snapshot_closes.get(), 1);
}

#[test]
fn dirty_tombstone_hides_the_matching_snapshot_prefix_key() {
    let buffer_closes = Rc::new(Cell::new(0));
    let snapshot_closes = Rc::new(Cell::new(0));
    let hidden = key(b"key-hidden".to_vec());
    let visible = key(b"key-visible".to_vec());
    let buffer = MockBuffer::new(Rc::clone(&buffer_closes));
    let snapshot = MockSnapshot::new(
        &[
            (hidden.as_bytes(), b"stale".as_slice()),
            (visible.as_bytes(), b"visible".as_slice()),
        ],
        Rc::clone(&snapshot_closes),
    );
    let mut transaction = TransactionReadDriver::new(buffer, snapshot);
    transaction.delete(hidden).unwrap();

    let mut seen = Vec::new();
    scan_meta_with_prefix(&mut transaction, &key(b"key".to_vec()), |current, value| {
        seen.push((current.clone(), value.to_vec()));
        true
    })
    .unwrap();

    assert_eq!(seen, vec![(visible, b"visible".to_vec())]);
    assert_eq!(buffer_closes.get(), 1);
    assert_eq!(snapshot_closes.get(), 1);
}

#[test]
fn empty_and_all_ff_prefixes_use_source_prefix_next_bounds() {
    let buffer_closes = Rc::new(Cell::new(0));
    let snapshot_closes = Rc::new(Cell::new(0));
    let mut buffer = MockBuffer::new(Rc::clone(&buffer_closes));
    for bytes in [
        Vec::new(),
        vec![0],
        vec![1],
        vec![0xff],
        vec![0xff, 0],
        vec![0xff, 1],
    ] {
        buffer.values.insert(key(bytes.clone()), bytes);
    }
    // Values are tombstones when empty, so give the empty key a visible value.
    buffer.values.insert(key(Vec::new()), b"empty-key".to_vec());
    let snapshot = MockSnapshot::new(&[], Rc::clone(&snapshot_closes));
    let mut transaction = TransactionReadDriver::new(buffer, snapshot);

    let mut empty_prefix_seen = Vec::new();
    scan_meta_with_prefix(&mut transaction, &key(Vec::new()), |current, _| {
        empty_prefix_seen.push(current.clone());
        true
    })
    .unwrap();
    assert_eq!(empty_prefix_seen, vec![key(Vec::new())]);

    let mut overflow_prefix_seen = Vec::new();
    scan_meta_with_prefix(&mut transaction, &key(vec![0xff]), |current, _| {
        overflow_prefix_seen.push(current.clone());
        true
    })
    .unwrap();
    assert_eq!(overflow_prefix_seen, vec![key(vec![0xff])]);
    assert_eq!(buffer_closes.get(), 2);
    assert_eq!(snapshot_closes.get(), 2);
}
