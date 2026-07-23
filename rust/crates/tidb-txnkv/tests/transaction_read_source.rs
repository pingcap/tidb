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

//! Complete direct translations of `pkg/store/driver/txn_test.go`.

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

use tidb_txnkv::driver::read::{
    SnapshotInterceptor, TransactionBuffer, TransactionReadDriver, TransactionReadError,
    TransactionSnapshot,
};
use tidb_txnkv::{
    BatchBufferGetter, BatchGetError, BatchGetOptions, BatchGetter, GetOptions, Getter, Key,
    KvIterator, ValueEntry,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReadError {
    NotFound,
    Intercepted,
}

impl BatchGetError for ReadError {
    fn is_not_found(&self) -> bool {
        *self == Self::NotFound
    }
}

impl TransactionReadError for ReadError {
    fn not_found() -> Self {
        Self::NotFound
    }
}

struct VecIterator {
    entries: Vec<(Key, Vec<u8>)>,
    index: usize,
    next_error: Option<ReadError>,
    close_calls: Rc<Cell<usize>>,
}

impl VecIterator {
    fn controlled(
        entries: Vec<(Key, Vec<u8>)>,
        next_error: Option<ReadError>,
        close_calls: Rc<Cell<usize>>,
    ) -> Self {
        Self {
            entries,
            index: 0,
            next_error,
            close_calls,
        }
    }
}

impl KvIterator for VecIterator {
    type Error = ReadError;

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
        if let Some(error) = self.next_error {
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
    iterator_next_error: Option<ReadError>,
    iterator_close_calls: Rc<Cell<usize>>,
}

impl Default for MockBuffer {
    fn default() -> Self {
        Self {
            values: BTreeMap::new(),
            iterator_next_error: None,
            iterator_close_calls: Rc::new(Cell::new(0)),
        }
    }
}

impl Getter for MockBuffer {
    type Error = ReadError;

    fn get(&mut self, key: &Key, _options: GetOptions) -> Result<ValueEntry, Self::Error> {
        self.values
            .get(key)
            .cloned()
            .map(|value| ValueEntry::new(value, 0))
            .ok_or(ReadError::NotFound)
    }
}

impl BatchBufferGetter for MockBuffer {
    type Error = ReadError;

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

    fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), ReadError> {
        self.values.insert(key, value);
        Ok(())
    }

    fn delete(&mut self, key: Key) -> Result<(), ReadError> {
        self.values.insert(key, Vec::new());
        Ok(())
    }

    fn iter(&mut self, start: &Key, upper_bound: Option<&Key>) -> Result<Self::Iter, ReadError> {
        Ok(VecIterator::controlled(
            range_entries(&self.values, Some(start), upper_bound, false),
            self.iterator_next_error,
            Rc::clone(&self.iterator_close_calls),
        ))
    }

    fn iter_reverse(
        &mut self,
        start: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> Result<Self::Iter, ReadError> {
        Ok(VecIterator::controlled(
            range_entries(&self.values, lower_bound, start, true),
            self.iterator_next_error,
            Rc::clone(&self.iterator_close_calls),
        ))
    }
}

struct MockSnapshot {
    values: BTreeMap<Key, Vec<u8>>,
    commit_ts: u64,
    iterator_close_calls: Rc<Cell<usize>>,
}

impl MockSnapshot {
    fn new(entries: &[(&str, &str)]) -> Self {
        Self {
            values: entries
                .iter()
                .map(|(key, value)| (k(key), value.as_bytes().to_vec()))
                .collect(),
            commit_ts: current_tso(),
            iterator_close_calls: Rc::new(Cell::new(0)),
        }
    }
}

impl Getter for MockSnapshot {
    type Error = ReadError;

    fn get(&mut self, key: &Key, options: GetOptions) -> Result<ValueEntry, Self::Error> {
        self.values
            .get(key)
            .cloned()
            .map(|value| {
                ValueEntry::new(
                    value,
                    if options.return_commit_ts {
                        self.commit_ts
                    } else {
                        0
                    },
                )
            })
            .ok_or(ReadError::NotFound)
    }
}

impl BatchGetter for MockSnapshot {
    type Error = ReadError;

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

    fn iter(&mut self, start: &Key, upper_bound: Option<&Key>) -> Result<Self::Iter, ReadError> {
        Ok(VecIterator::controlled(
            range_entries(&self.values, Some(start), upper_bound, false),
            None,
            Rc::clone(&self.iterator_close_calls),
        ))
    }

    fn iter_reverse(
        &mut self,
        start: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> Result<Self::Iter, ReadError> {
        Ok(VecIterator::controlled(
            range_entries(&self.values, lower_bound, start, true),
            None,
            Rc::clone(&self.iterator_close_calls),
        ))
    }
}

struct MockInterceptor {
    fail: bool,
}

impl SnapshotInterceptor<MockSnapshot> for MockInterceptor {
    type Iter = VecIterator;

    fn on_get(
        &mut self,
        snapshot: &mut MockSnapshot,
        key: &Key,
        options: GetOptions,
    ) -> Result<ValueEntry, ReadError> {
        if self.fail {
            Err(ReadError::Intercepted)
        } else {
            snapshot.get(key, options)
        }
    }

    fn on_batch_get(
        &mut self,
        snapshot: &mut MockSnapshot,
        keys: &[Key],
        options: BatchGetOptions,
    ) -> Result<HashMap<Key, ValueEntry>, ReadError> {
        if self.fail {
            Err(ReadError::Intercepted)
        } else {
            snapshot.batch_get(keys, options)
        }
    }

    fn on_iter(
        &mut self,
        snapshot: &mut MockSnapshot,
        start: &Key,
        upper_bound: Option<&Key>,
    ) -> Result<Self::Iter, ReadError> {
        if self.fail {
            Err(ReadError::Intercepted)
        } else {
            snapshot.iter(start, upper_bound)
        }
    }

    fn on_iter_reverse(
        &mut self,
        snapshot: &mut MockSnapshot,
        start: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> Result<Self::Iter, ReadError> {
        if self.fail {
            Err(ReadError::Intercepted)
        } else {
            snapshot.iter_reverse(start, lower_bound)
        }
    }
}

fn transaction(entries: &[(&str, &str)]) -> TransactionReadDriver<MockBuffer, MockSnapshot> {
    TransactionReadDriver::new(MockBuffer::default(), MockSnapshot::new(entries))
}

fn k(value: &str) -> Key {
    Key::from_bytes(value.as_bytes())
}

fn current_tso() -> u64 {
    let milliseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after the Unix epoch")
        .as_millis();
    u64::try_from(milliseconds)
        .expect("current milliseconds must fit u64")
        .checked_shl(18)
        .expect("current TSO physical component must fit u64")
}

fn assert_valid_commit_ts(commit_ts: u64) {
    assert!(commit_ts > 0);
    let commit_seconds = (commit_ts >> 18) / 1_000;
    let now_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after the Unix epoch")
        .as_secs();
    assert!(now_seconds.abs_diff(commit_seconds) <= 10);
}

fn range_entries(
    values: &BTreeMap<Key, Vec<u8>>,
    lower_bound: Option<&Key>,
    upper_bound: Option<&Key>,
    reverse: bool,
) -> Vec<(Key, Vec<u8>)> {
    let mut entries: Vec<_> = values
        .iter()
        .filter(|(key, _)| {
            lower_bound.is_none_or(|lower| *key >= lower)
                && upper_bound.is_none_or(|upper| *key < upper)
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    if reverse {
        entries.reverse();
    }
    entries
}

fn collect<I>(mut iterator: I) -> Vec<(String, String)>
where
    I: KvIterator<Error = ReadError>,
{
    let mut result = Vec::new();
    while iterator.valid() {
        result.push((
            String::from_utf8(iterator.key().as_bytes().to_vec()).unwrap(),
            String::from_utf8(iterator.value().to_vec()).unwrap(),
        ));
        iterator.next().unwrap();
    }
    iterator.close();
    result
}

#[test]
fn test_txn_get() {
    let mut txn = transaction(&[("k1", "v1")]);

    assert_eq!(
        txn.get(&k("k1"), GetOptions::default()),
        Ok(ValueEntry::new(b"v1".as_slice(), 0))
    );
    let entry = txn
        .get(&k("k1"), GetOptions::with_return_commit_ts())
        .unwrap();
    assert_eq!(entry.value, b"v1");
    assert_valid_commit_ts(entry.commit_ts);

    txn.set(k("k1"), b"v1+".to_vec()).unwrap();
    assert_eq!(
        txn.get(&k("k1"), GetOptions::default()),
        Ok(ValueEntry::new(b"v1+".as_slice(), 0))
    );
    assert_eq!(
        txn.get(&k("k1"), GetOptions::with_return_commit_ts()),
        Ok(ValueEntry::new(b"v1+".as_slice(), 0))
    );

    txn.set(k("k2"), b"v2+".to_vec()).unwrap();
    assert_eq!(
        txn.get(&k("k2"), GetOptions::default()),
        Ok(ValueEntry::new(b"v2+".as_slice(), 0))
    );

    txn.delete(k("k1")).unwrap();
    assert_eq!(
        txn.get(&k("k1"), GetOptions::default()),
        Err(ReadError::NotFound)
    );
    assert_eq!(
        txn.get(&k("kn"), GetOptions::default()),
        Err(ReadError::NotFound)
    );
    assert_eq!(
        txn.get(&k("k1"), GetOptions::with_return_commit_ts()),
        Err(ReadError::NotFound)
    );

    let mut txn = txn.with_snapshot_interceptor(MockInterceptor { fail: true });
    assert_eq!(
        txn.get(&k("k1"), GetOptions::default()),
        Err(ReadError::NotFound)
    );
    assert_eq!(
        txn.get(&k("k2"), GetOptions::default()),
        Ok(ValueEntry::new(b"v2+".as_slice(), 0))
    );
    assert_eq!(
        txn.get(&k("kn"), GetOptions::default()),
        Err(ReadError::Intercepted)
    );
}

#[test]
fn test_txn_batch_get() {
    let mut txn = transaction(&[("k1", "v1"), ("k2", "v2"), ("k3", "v3"), ("k4", "v4")]);
    let commit_ts = txn
        .get(&k("k1"), GetOptions::with_return_commit_ts())
        .unwrap()
        .commit_ts;
    assert_valid_commit_ts(commit_ts);

    let result = txn
        .batch_get(
            &[k("k1"), k("k2"), k("k3"), k("kn")],
            BatchGetOptions::default(),
        )
        .unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[&k("k1")], ValueEntry::new(b"v1".as_slice(), 0));
    assert_eq!(result[&k("k2")], ValueEntry::new(b"v2".as_slice(), 0));
    assert_eq!(result[&k("k3")], ValueEntry::new(b"v3".as_slice(), 0));

    let result = txn
        .batch_get(
            &[k("k1"), k("k2"), k("k3"), k("kn")],
            BatchGetOptions::with_return_commit_ts(),
        )
        .unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(
        result[&k("k1")],
        ValueEntry::new(b"v1".as_slice(), commit_ts)
    );
    assert_eq!(
        result[&k("k2")],
        ValueEntry::new(b"v2".as_slice(), commit_ts)
    );
    assert_eq!(
        result[&k("k3")],
        ValueEntry::new(b"v3".as_slice(), commit_ts)
    );

    txn.set(k("k1"), b"v1+".to_vec()).unwrap();
    txn.set(k("k4"), b"v4+".to_vec()).unwrap();
    txn.delete(k("k2")).unwrap();

    let result = txn
        .batch_get(
            &[k("k1"), k("k2"), k("k3"), k("k4"), k("kn")],
            BatchGetOptions::default(),
        )
        .unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[&k("k1")], ValueEntry::new(b"v1+".as_slice(), 0));
    assert_eq!(result[&k("k3")], ValueEntry::new(b"v3".as_slice(), 0));
    assert_eq!(result[&k("k4")], ValueEntry::new(b"v4+".as_slice(), 0));

    let result = txn
        .batch_get(&[k("k1"), k("k4")], BatchGetOptions::default())
        .unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[&k("k1")], ValueEntry::new(b"v1+".as_slice(), 0));
    assert_eq!(result[&k("k4")], ValueEntry::new(b"v4+".as_slice(), 0));

    let result = txn
        .batch_get(
            &[k("k1"), k("k2"), k("k3"), k("k4")],
            BatchGetOptions::with_return_commit_ts(),
        )
        .unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[&k("k1")], ValueEntry::new(b"v1+".as_slice(), 0));
    assert_eq!(
        result[&k("k3")],
        ValueEntry::new(b"v3".as_slice(), commit_ts)
    );
    assert_eq!(result[&k("k4")], ValueEntry::new(b"v4+".as_slice(), 0));

    let mut txn = txn.with_snapshot_interceptor(MockInterceptor { fail: true });
    for keys in [
        vec![k("k3")],
        vec![k("k1"), k("k3"), k("k4")],
        vec![k("k1"), k("k4"), k("kn")],
    ] {
        assert_eq!(
            txn.batch_get(&keys, BatchGetOptions::default()),
            Err(ReadError::Intercepted)
        );
    }
}

#[test]
fn test_txn_scan() {
    let mut txn = transaction(&[
        ("k1", "v1"),
        ("k3", "v3"),
        ("k5", "v5"),
        ("k7", "v7"),
        ("k9", "v9"),
    ]);

    assert_eq!(
        collect(txn.iter(&k("k3"), Some(&k("k9"))).unwrap()),
        [("k3", "v3"), ("k5", "v5"), ("k7", "v7")]
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
    );
    assert_eq!(
        collect(txn.iter_reverse(Some(&k("k9")), None).unwrap()),
        [("k7", "v7"), ("k5", "v5"), ("k3", "v3"), ("k1", "v1")]
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
    );
    assert_eq!(
        collect(txn.iter_reverse(Some(&k("k9")), Some(&k("k3"))).unwrap()),
        [("k7", "v7"), ("k5", "v5"), ("k3", "v3")]
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
    );

    txn.set(k("k1"), b"v1+".to_vec()).unwrap();
    txn.set(k("k3"), b"v3+".to_vec()).unwrap();
    txn.set(k("k31"), b"v31+".to_vec()).unwrap();
    txn.delete(k("k5")).unwrap();

    assert_eq!(
        collect(txn.iter(&k("k3"), Some(&k("k9"))).unwrap()),
        [("k3", "v3+"), ("k31", "v31+"), ("k7", "v7")]
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
    );
    assert_eq!(
        collect(txn.iter_reverse(Some(&k("k9")), None).unwrap()),
        [("k7", "v7"), ("k31", "v31+"), ("k3", "v3+"), ("k1", "v1+"),]
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
    );

    let mut txn = txn.with_snapshot_interceptor(MockInterceptor { fail: true });
    assert!(matches!(
        txn.iter(&k("k1"), Some(&k("k2"))),
        Err(ReadError::Intercepted)
    ));
}

#[test]
fn snapshot_interceptor_installs_late_and_replaces_in_place() {
    let mut txn = transaction(&[("snapshot", "value")]);
    txn.set(k("dirty"), b"uncommitted".to_vec()).unwrap();

    let mut txn = txn.with_snapshot_interceptor(MockInterceptor { fail: true });
    assert_eq!(
        txn.get(&k("snapshot"), GetOptions::default()),
        Err(ReadError::Intercepted)
    );
    assert_eq!(
        txn.get(&k("dirty"), GetOptions::default()),
        Ok(ValueEntry::new(b"uncommitted".as_slice(), 0))
    );

    let previous = txn
        .set_snapshot_interceptor(Some(MockInterceptor { fail: false }))
        .expect("the late interceptor must be replaced");
    assert!(previous.fail);
    assert_eq!(
        txn.get(&k("snapshot"), GetOptions::default()),
        Ok(ValueEntry::new(b"value".as_slice(), 0))
    );

    let previous = txn
        .set_snapshot_interceptor(None)
        .expect("the replacement interceptor must be cleared");
    assert!(!previous.fail);
    assert_eq!(
        txn.get(&k("snapshot"), GetOptions::default()),
        Ok(ValueEntry::new(b"value".as_slice(), 0))
    );
}

#[test]
fn snapshot_iterator_creation_failure_closes_dirty_iterator() {
    let buffer = MockBuffer::default();
    let dirty_close_calls = Rc::clone(&buffer.iterator_close_calls);
    let snapshot = MockSnapshot::new(&[("k1", "v1")]);
    let snapshot_close_calls = Rc::clone(&snapshot.iterator_close_calls);
    let mut txn = TransactionReadDriver::new(buffer, snapshot)
        .with_snapshot_interceptor(MockInterceptor { fail: true });

    assert!(matches!(
        txn.iter(&k("k1"), Some(&k("k2"))),
        Err(ReadError::Intercepted)
    ));
    assert_eq!(dirty_close_calls.get(), 1);
    assert_eq!(snapshot_close_calls.get(), 0);
}

#[test]
fn union_iterator_creation_failure_closes_both_inputs() {
    let mut buffer = MockBuffer::default();
    buffer.values.insert(k("k1"), Vec::new());
    buffer.iterator_next_error = Some(ReadError::Intercepted);
    let dirty_close_calls = Rc::clone(&buffer.iterator_close_calls);
    let snapshot = MockSnapshot::new(&[]);
    let snapshot_close_calls = Rc::clone(&snapshot.iterator_close_calls);
    let mut txn = TransactionReadDriver::new(buffer, snapshot);

    assert!(matches!(
        txn.iter(&k("k1"), Some(&k("k2"))),
        Err(ReadError::Intercepted)
    ));
    assert_eq!(dirty_close_calls.get(), 1);
    assert_eq!(snapshot_close_calls.get(), 1);
}
