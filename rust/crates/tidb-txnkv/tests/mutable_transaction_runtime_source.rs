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

//! Connected source tests for `pkg/store/driver/txn/unionstore_driver.go` and
//! its existing transaction/prefix consumers.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use tidb_txnkv::driver::read::{TransactionReadDriver, TransactionReadError, TransactionSnapshot};
use tidb_txnkv::{
    del_key_with_prefix, AssertionOp, BatchGetError, BatchGetOptions, BatchGetter, FlagsOp, Getter,
    Key, KeyFlags, KvIterator, MemBufferBackend, MemBufferDriver, StagingHandle, ValueEntry,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TestError {
    NotFound,
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

struct MapIterator {
    entries: Vec<(Key, Vec<u8>)>,
    index: usize,
    closed: bool,
}

impl MapIterator {
    fn new(entries: Vec<(Key, Vec<u8>)>) -> Self {
        Self {
            entries,
            index: 0,
            closed: false,
        }
    }
}

impl KvIterator for MapIterator {
    type Error = TestError;

    fn valid(&self) -> bool {
        !self.closed && self.index < self.entries.len()
    }

    fn key(&self) -> &Key {
        &self.entries[self.index].0
    }

    fn value(&self) -> &[u8] {
        &self.entries[self.index].1
    }

    fn next(&mut self) -> Result<(), Self::Error> {
        self.index += 1;
        Ok(())
    }

    fn close(&mut self) {
        self.closed = true;
    }
}

struct OwnedGetter {
    values: BTreeMap<Key, Vec<u8>>,
}

impl Getter for OwnedGetter {
    type Error = TestError;

    fn get(&mut self, key: &Key, _options: BatchGetOptions) -> Result<ValueEntry, Self::Error> {
        self.values
            .get(key)
            .cloned()
            .map(|value| ValueEntry::new(value, 0))
            .ok_or(TestError::NotFound)
    }
}

struct Stage {
    handle: StagingHandle,
    before_values: BTreeMap<Key, Vec<u8>>,
    before_flags: BTreeMap<Key, KeyFlags>,
    touched: BTreeSet<Key>,
}

#[derive(Default)]
struct FakeBackend {
    values: BTreeMap<Key, Vec<u8>>,
    flags: BTreeMap<Key, KeyFlags>,
    stages: Vec<Stage>,
    next_stage: usize,
}

impl FakeBackend {
    fn mark_touched(&mut self, key: &Key) {
        if let Some(stage) = self.stages.last_mut() {
            stage.touched.insert(key.clone());
        }
    }

    fn entries(
        values: &BTreeMap<Key, Vec<u8>>,
        start: Option<&Key>,
        upper_bound: Option<&Key>,
        reverse: bool,
    ) -> Vec<(Key, Vec<u8>)> {
        let mut entries: Vec<_> = values
            .iter()
            .filter(|(key, _)| {
                start.is_none_or(|start| *key >= start)
                    && upper_bound.is_none_or(|upper| *key < upper)
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        if reverse {
            entries.reverse();
        }
        entries
    }

    fn set_value(&mut self, key: Key, value: Vec<u8>) {
        self.mark_touched(&key);
        self.values.insert(key, value);
    }

    fn apply_flags(&mut self, key: &Key, operations: &[FlagsOp]) {
        self.mark_touched(key);
        let flags = self.flags.get(key).copied().unwrap_or_default();
        self.flags.insert(
            key.clone(),
            flags.apply_flags_ops(operations.iter().copied()),
        );
    }
}

impl MemBufferBackend for FakeBackend {
    type Error = TestError;
    type Iter = MapIterator;
    type SnapshotGetter = OwnedGetter;

    fn len(&self) -> usize {
        self.values.len()
    }

    fn size(&self) -> usize {
        self.values
            .iter()
            .map(|(key, value)| key.as_bytes().len() + value.len())
            .sum()
    }

    fn get(&mut self, key: &Key, _options: BatchGetOptions) -> Result<ValueEntry, Self::Error> {
        self.values
            .get(key)
            .cloned()
            .map(|value| ValueEntry::new(value, 0))
            .ok_or(TestError::NotFound)
    }

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
                    .cloned()
                    .map(|value| (key.clone(), ValueEntry::new(value, 0)))
            })
            .collect())
    }

    fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), Self::Error> {
        self.set_value(key, value);
        Ok(())
    }

    fn set_with_flags(
        &mut self,
        key: Key,
        value: Vec<u8>,
        operations: &[FlagsOp],
    ) -> Result<(), Self::Error> {
        self.set_value(key.clone(), value);
        self.apply_flags(&key, operations);
        Ok(())
    }

    fn delete(&mut self, key: Key) -> Result<(), Self::Error> {
        self.set_value(key, Vec::new());
        Ok(())
    }

    fn delete_with_flags(&mut self, key: Key, operations: &[FlagsOp]) -> Result<(), Self::Error> {
        self.set_value(key.clone(), Vec::new());
        self.apply_flags(&key, operations);
        Ok(())
    }

    fn remove_from_buffer(&mut self, key: &Key) {
        self.mark_touched(key);
        self.values.remove(key);
        self.flags.remove(key);
    }

    fn update_flags(&mut self, key: &Key, operations: &[FlagsOp]) {
        self.apply_flags(key, operations);
    }

    fn update_assertion_flags(&mut self, key: &Key, operation: AssertionOp) {
        self.mark_touched(key);
        let flags = self.flags.get(key).copied().unwrap_or_default();
        self.flags
            .insert(key.clone(), flags.apply_assertion_op(operation));
    }

    fn get_flags(&self, key: &Key) -> Result<KeyFlags, Self::Error> {
        Ok(self.flags.get(key).copied().unwrap_or_default())
    }

    fn staging(&mut self) -> StagingHandle {
        let handle = StagingHandle::new(self.next_stage);
        self.next_stage += 1;
        self.stages.push(Stage {
            handle,
            before_values: self.values.clone(),
            before_flags: self.flags.clone(),
            touched: BTreeSet::new(),
        });
        handle
    }

    fn cleanup(&mut self, handle: StagingHandle) {
        let stage = self.stages.pop().expect("active stage");
        assert_eq!(stage.handle, handle);
        self.values = stage.before_values;
        self.flags = stage.before_flags;
    }

    fn release(&mut self, handle: StagingHandle) {
        let stage = self.stages.pop().expect("active stage");
        assert_eq!(stage.handle, handle);
        if let Some(parent) = self.stages.last_mut() {
            parent.touched.extend(stage.touched);
        }
    }

    fn inspect_stage(&self, handle: StagingHandle, inspect: &mut dyn FnMut(&Key, KeyFlags, &[u8])) {
        let stage = self
            .stages
            .iter()
            .find(|stage| stage.handle == handle)
            .expect("active stage");
        for key in &stage.touched {
            let flags = self.flags.get(key).copied().unwrap_or_default();
            let value = self.values.get(key).map_or(&[][..], Vec::as_slice);
            inspect(key, flags, value);
        }
    }

    fn iter(&mut self, start: &Key, upper_bound: Option<&Key>) -> Result<Self::Iter, TestError> {
        Ok(MapIterator::new(Self::entries(
            &self.values,
            Some(start),
            upper_bound,
            false,
        )))
    }

    fn iter_reverse(
        &mut self,
        start: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> Result<Self::Iter, TestError> {
        Ok(MapIterator::new(Self::entries(
            &self.values,
            lower_bound,
            start,
            true,
        )))
    }

    fn snapshot_iter(&mut self, start: &Key, upper_bound: Option<&Key>) -> Self::Iter {
        MapIterator::new(Self::entries(&self.values, Some(start), upper_bound, false))
    }

    fn snapshot_iter_reverse(
        &mut self,
        start: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> Self::Iter {
        MapIterator::new(Self::entries(&self.values, lower_bound, start, true))
    }

    fn snapshot_getter(&mut self) -> Self::SnapshotGetter {
        OwnedGetter {
            values: self.values.clone(),
        }
    }

    fn get_local(&mut self, key: &Key) -> Result<Vec<u8>, Self::Error> {
        self.values.get(key).cloned().ok_or(TestError::NotFound)
    }
}

struct Snapshot {
    values: BTreeMap<Key, Vec<u8>>,
}

impl Snapshot {
    fn new(entries: &[(&str, &str)]) -> Self {
        Self {
            values: entries
                .iter()
                .map(|(key, value)| (k(key), value.as_bytes().to_vec()))
                .collect(),
        }
    }
}

impl Getter for Snapshot {
    type Error = TestError;

    fn get(&mut self, key: &Key, _options: BatchGetOptions) -> Result<ValueEntry, Self::Error> {
        self.values
            .get(key)
            .cloned()
            .map(|value| ValueEntry::new(value, 0))
            .ok_or(TestError::NotFound)
    }
}

impl BatchGetter for Snapshot {
    type Error = TestError;

    fn batch_get(
        &mut self,
        keys: &[Key],
        options: BatchGetOptions,
    ) -> Result<HashMap<Key, ValueEntry>, Self::Error> {
        Ok(keys
            .iter()
            .filter_map(|key| {
                self.get(key, options)
                    .ok()
                    .map(|value| (key.clone(), value))
            })
            .collect())
    }
}

impl TransactionSnapshot for Snapshot {
    type Iter = MapIterator;

    fn iter(&mut self, start: &Key, upper_bound: Option<&Key>) -> Result<Self::Iter, TestError> {
        Ok(MapIterator::new(FakeBackend::entries(
            &self.values,
            Some(start),
            upper_bound,
            false,
        )))
    }

    fn iter_reverse(
        &mut self,
        start: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> Result<Self::Iter, TestError> {
        Ok(MapIterator::new(FakeBackend::entries(
            &self.values,
            lower_bound,
            start,
            true,
        )))
    }
}

fn k(value: &str) -> Key {
    Key::from_bytes(value.as_bytes())
}

fn drain<I: KvIterator<Error = TestError>>(iterator: &mut I) -> Vec<(Key, Vec<u8>)> {
    let mut values = Vec::new();
    while iterator.valid() {
        values.push((iterator.key().clone(), iterator.value().to_vec()));
        iterator.next().unwrap();
    }
    iterator.close();
    values
}

#[test]
fn mem_buffer_driver_is_the_transaction_read_and_mutation_buffer() {
    let buffer = MemBufferDriver::new(FakeBackend::default(), false);
    let snapshot = Snapshot::new(&[
        ("a", "snapshot-a"),
        ("b", "snapshot-b"),
        ("d", "snapshot-d"),
    ]);
    let mut transaction = TransactionReadDriver::new(buffer, snapshot);

    transaction.set(k("a"), b"dirty-a".to_vec()).unwrap();
    transaction.delete(k("b")).unwrap();
    transaction.set(k("c"), b"dirty-c".to_vec()).unwrap();

    assert_eq!(
        transaction
            .get(&k("a"), BatchGetOptions::default())
            .unwrap()
            .value,
        b"dirty-a"
    );
    assert_eq!(
        transaction.get(&k("b"), BatchGetOptions::default()),
        Err(TestError::NotFound)
    );
    let batch = transaction
        .batch_get(
            &[k("a"), k("b"), k("c"), k("d"), k("missing")],
            BatchGetOptions::default(),
        )
        .unwrap();
    assert_eq!(batch.len(), 3);
    assert_eq!(batch[&k("a")].value.as_slice(), b"dirty-a");
    assert_eq!(batch[&k("c")].value.as_slice(), b"dirty-c");
    assert_eq!(batch[&k("d")].value.as_slice(), b"snapshot-d");

    let mut forward = transaction.iter(&k("a"), Some(&k("z"))).unwrap();
    assert_eq!(
        drain(&mut forward),
        vec![
            (k("a"), b"dirty-a".to_vec()),
            (k("c"), b"dirty-c".to_vec()),
            (k("d"), b"snapshot-d".to_vec()),
        ]
    );
    let mut reverse = transaction
        .iter_reverse(Some(&k("z")), Some(&k("a")))
        .unwrap();
    assert_eq!(
        drain(&mut reverse),
        vec![
            (k("d"), b"snapshot-d".to_vec()),
            (k("c"), b"dirty-c".to_vec()),
            (k("a"), b"dirty-a".to_vec()),
        ]
    );
}

#[test]
fn flags_assertions_and_stages_delegate_without_client_bit_layouts() {
    let mut buffer = MemBufferDriver::new(FakeBackend::default(), false);
    let key = k("staged");
    let stage = buffer.staging();
    buffer
        .set_with_flags(
            key.clone(),
            b"value".to_vec(),
            &[FlagsOp::SetPresumeKeyNotExists, FlagsOp::SetNeedLocked],
        )
        .unwrap();
    buffer.update_assertion_flags(&key, AssertionOp::AssertNotExist);

    let flags = buffer.get_flags(&key).unwrap();
    assert!(flags.has_presume_key_not_exists());
    assert!(flags.has_need_locked());
    assert!(flags.has_assert_not_exists());
    let mut inspected = Vec::new();
    buffer.inspect_stage(stage, |key, flags, value| {
        inspected.push((key.clone(), flags, value.to_vec()));
    });
    assert_eq!(inspected, vec![(key.clone(), flags, b"value".to_vec())]);

    buffer.cleanup(stage);
    assert_eq!(
        buffer.get_local(&key),
        Err(TestError::NotFound),
        "Cleanup restores both values and flags"
    );

    let committed = buffer.staging();
    buffer
        .delete_with_flags(key.clone(), &[FlagsOp::SetNeedConstraintCheckInPrewrite])
        .unwrap();
    buffer.release(committed);
    assert_eq!(buffer.get_local(&key).unwrap(), Vec::<u8>::new());
    assert!(buffer
        .get_flags(&key)
        .unwrap()
        .has_need_constraint_check_in_prewrite());
    buffer.remove_from_buffer(&key);
    assert!(buffer.is_empty());
}

#[test]
fn buffer_snapshot_views_are_stable_and_pipelined_views_are_empty() {
    let mut ordinary = MemBufferDriver::new(FakeBackend::default(), false);
    ordinary
        .set_with_flags(k("a"), b"one".to_vec(), &[])
        .unwrap();
    let mut getter = ordinary.snapshot_getter();
    let mut iterator = ordinary.snapshot_iter(&k("a"), Some(&k("z")));
    ordinary
        .set_with_flags(k("a"), b"two".to_vec(), &[])
        .unwrap();
    assert_eq!(
        getter
            .get(&k("a"), BatchGetOptions::default())
            .unwrap()
            .value,
        b"one"
    );
    assert_eq!(drain(&mut iterator), vec![(k("a"), b"one".to_vec())]);

    let mut pipelined = MemBufferDriver::new(FakeBackend::default(), true);
    pipelined
        .set_with_flags(k("a"), b"live".to_vec(), &[])
        .unwrap();
    assert_eq!(
        pipelined
            .get(&k("a"), BatchGetOptions::default())
            .unwrap()
            .value,
        b"live"
    );
    assert_eq!(
        pipelined
            .snapshot_getter()
            .get(&k("a"), BatchGetOptions::default()),
        Err(TestError::NotFound)
    );
    assert!(!pipelined.snapshot_iter(&k("a"), None).valid());
    assert!(!pipelined.snapshot_iter_reverse(None, None).valid());
}

#[test]
fn prefix_delete_consumes_the_same_mutable_driver_and_clones_before_mutation() {
    let buffer = MemBufferDriver::new(FakeBackend::default(), false);
    let snapshot = Snapshot::new(&[("meta/a", "one"), ("meta/b", "two"), ("other", "keep")]);
    let mut transaction = TransactionReadDriver::new(buffer, snapshot);
    transaction.set(k("meta/c"), b"three".to_vec()).unwrap();

    del_key_with_prefix(&mut transaction, &k("meta/")).unwrap();
    for key in [k("meta/a"), k("meta/b"), k("meta/c")] {
        assert_eq!(
            transaction.get(&key, BatchGetOptions::default()),
            Err(TestError::NotFound)
        );
    }
    assert_eq!(
        transaction
            .get(&k("other"), BatchGetOptions::default())
            .unwrap()
            .value,
        b"keep"
    );
}
