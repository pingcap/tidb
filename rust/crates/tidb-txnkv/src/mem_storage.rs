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

//! An in-process key/value store behind the [`Getter`]/[`Retriever`]/
//! [`Mutator`] contracts, so a reader can be written against the same traits a
//! real TiKV snapshot implements.
//!
//! This is the Rust counterpart of what Go reaches through
//! `pkg/store/mockstore` in tests: ordinary sorted-map storage of the real
//! encoded bytes, consumed through `kv.Retriever` rather than as a map. Code
//! written against these traits keeps working when a
//! transaction/region/coprocessor-backed snapshot replaces the container.
//!
//! NOT MODELLED (documented): MVCC versions, timestamps, locks, regions, and
//! the staging buffer. Every read sees the latest write immediately, which is
//! why this is a store for a single-process pipeline and not a transactional
//! snapshot. [`ValueEntry`]'s `commit_ts` is therefore always `0`.

use std::collections::BTreeMap;

use crate::batch_getter::{BatchGetError, GetOptions, Getter, ValueEntry};
use crate::iteration::KvIterator;
use crate::key::Key;
use crate::kv_api::{Mutator, Retriever};

/// The failures an in-memory read or iteration can report.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemStorageError {
    /// Go `kv.ErrNotExist`: the key has no value.
    NotFound,
    /// The iterator is exhausted, so it cannot advance further.
    InvalidIterator,
}

/// A sorted in-memory key/value store.
#[derive(Clone, Debug, Default)]
pub struct MemStorage {
    data: BTreeMap<Key, Vec<u8>>,
}

impl MemStorage {
    /// Builds an empty store.
    #[must_use]
    pub fn new() -> Self {
        MemStorage::default()
    }

    /// The number of stored keys.
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Whether the store holds no keys.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl BatchGetError for MemStorageError {
    fn is_not_found(&self) -> bool {
        matches!(self, MemStorageError::NotFound)
    }
}

impl Getter for MemStorage {
    type Error = MemStorageError;

    fn get(&mut self, key: &Key, _options: GetOptions) -> Result<ValueEntry, Self::Error> {
        self.data
            .get(key)
            .map(|value| ValueEntry::new(value.clone(), 0))
            .ok_or(MemStorageError::NotFound)
    }
}

impl Mutator for MemStorage {
    type Error = MemStorageError;

    fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), Self::Error> {
        self.data.insert(key, value);
        Ok(())
    }

    fn delete(&mut self, key: Key) -> Result<(), Self::Error> {
        self.data.remove(&key);
        Ok(())
    }
}

impl Retriever for MemStorage {
    type Iterator = MemIterator;

    fn iter(
        &mut self,
        key: Option<&Key>,
        upper_bound: Option<&Key>,
    ) -> Result<Self::Iterator, MemStorageError> {
        let entries = self
            .data
            .iter()
            .filter(|(stored, _)| in_range(stored, key, upper_bound))
            .map(|(stored, value)| (stored.clone(), value.clone()))
            .collect();
        Ok(MemIterator::new(entries))
    }

    fn iter_reverse(
        &mut self,
        key: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> Result<Self::Iterator, MemStorageError> {
        // Go's reverse iterator walks `[lowerBound, key)`, i.e. the exclusive
        // bound is the upper one, in descending key order.
        let mut entries: Vec<(Key, Vec<u8>)> = self
            .data
            .iter()
            .filter(|(stored, _)| in_range(stored, lower_bound, key))
            .map(|(stored, value)| (stored.clone(), value.clone()))
            .collect();
        entries.reverse();
        Ok(MemIterator::new(entries))
    }
}

/// Whether `key` lies in the half-open range `[start, end)`; `None` is Go's
/// `nil`, an unbounded end.
fn in_range(key: &Key, start: Option<&Key>, end: Option<&Key>) -> bool {
    if let Some(start) = start {
        if key < start {
            return false;
        }
    }
    if let Some(end) = end {
        if key >= end {
            return false;
        }
    }
    true
}

/// A snapshot iterator over a range of [`MemStorage`].
///
/// The entries are copied when the iterator is created, matching the source
/// contract that an iterator reads a stable view and is unaffected by later
/// writes.
#[derive(Debug)]
pub struct MemIterator {
    entries: Vec<(Key, Vec<u8>)>,
    position: usize,
}

impl MemIterator {
    fn new(entries: Vec<(Key, Vec<u8>)>) -> Self {
        MemIterator {
            entries,
            position: 0,
        }
    }
}

impl KvIterator for MemIterator {
    type Error = MemStorageError;

    fn valid(&self) -> bool {
        self.position < self.entries.len()
    }

    fn key(&self) -> &Key {
        &self.entries[self.position].0
    }

    fn value(&self) -> &[u8] {
        &self.entries[self.position].1
    }

    fn next(&mut self) -> Result<(), Self::Error> {
        if !self.valid() {
            return Err(MemStorageError::InvalidIterator);
        }
        self.position += 1;
        Ok(())
    }

    fn close(&mut self) {
        self.entries.clear();
        self.position = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(bytes: &[u8]) -> Key {
        Key::from_bytes(bytes.to_vec())
    }

    fn store() -> MemStorage {
        let mut storage = MemStorage::new();
        for byte in *b"abcd" {
            storage.set(key(&[byte]), vec![byte, byte]).unwrap();
        }
        storage
    }

    #[test]
    fn get_returns_the_value_or_not_found() {
        let mut storage = store();
        assert_eq!(
            storage.get(&key(b"b"), GetOptions::default()).unwrap(),
            ValueEntry::new(vec![b'b', b'b'], 0)
        );
        assert_eq!(
            storage.get(&key(b"z"), GetOptions::default()),
            Err(MemStorageError::NotFound)
        );
    }

    #[test]
    fn iter_walks_the_half_open_range_in_key_order() {
        let mut storage = store();
        let mut iterator = storage.iter(Some(&key(b"b")), Some(&key(b"d"))).unwrap();
        let mut seen = Vec::new();
        while iterator.valid() {
            seen.push((iterator.key().clone(), iterator.value().to_vec()));
            iterator.next().unwrap();
        }
        assert_eq!(
            seen,
            vec![(key(b"b"), vec![b'b', b'b']), (key(b"c"), vec![b'c', b'c']),],
            "the upper bound is exclusive"
        );
        assert_eq!(iterator.next(), Err(MemStorageError::InvalidIterator));
    }

    #[test]
    fn unbounded_iter_walks_everything_and_reverse_descends() {
        let mut storage = store();
        let mut forward = storage.iter(None, None).unwrap();
        let mut count = 0;
        while forward.valid() {
            count += 1;
            forward.next().unwrap();
        }
        assert_eq!(count, 4);

        let mut reverse = storage.iter_reverse(Some(&key(b"d")), None).unwrap();
        let mut seen = Vec::new();
        while reverse.valid() {
            seen.push(reverse.key().clone());
            reverse.next().unwrap();
        }
        assert_eq!(seen, vec![key(b"c"), key(b"b"), key(b"a")]);
    }

    #[test]
    fn an_iterator_is_unaffected_by_later_writes() {
        let mut storage = store();
        let mut iterator = storage.iter(None, None).unwrap();
        storage.set(key(b"e"), vec![b'e']).unwrap();
        storage.delete(key(b"a")).unwrap();

        let mut count = 0;
        while iterator.valid() {
            count += 1;
            iterator.next().unwrap();
        }
        assert_eq!(count, 4, "the iterator kept its own view");
        assert_eq!(storage.len(), 4, "the store itself changed");
    }
}
