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

//! The storage backend seam: the key/value surface a [`KvTable`] needs, as a
//! trait object, so the same table code runs over the in-process store today
//! and over a TiKV-backed transaction later.
//!
//! [`KvTable`]: crate::kv_table::KvTable
//!
//! # Why a seam and not the `tidb-txnkv` traits directly
//!
//! `tidb-txnkv` already spells the source contracts: `kv.Retriever`
//! ([`Retriever`]), `kv.Mutator` ([`Mutator`]) and `kv.Iterator`
//! ([`KvIterator`]). They are the right shape, but `Retriever::Iterator` is an
//! associated type and `Getter::Error` is an associated type, so a value held
//! behind them must be a *generic parameter*. Making `KvTable` generic would
//! push a type parameter through the catalog, the session, and every planner
//! and executor site that names a table -- a change with no behavioural
//! content. [`TableStorage`] is therefore the same four operations with the
//! associated types erased: one concrete error, one boxed iterator. Every
//! method maps 1:1 onto the source trait it comes from (see the table below),
//! so the real backend implements it by forwarding.
//!
//! | [`TableStorage`] | `tidb-txnkv` source | Go |
//! | --- | --- | --- |
//! | [`get`](TableStorage::get) | [`Getter::get`] | `kv.Retriever.Get` |
//! | [`set`](TableStorage::set) | [`Mutator::set`] | `kv.Mutator.Set` |
//! | [`delete`](TableStorage::delete) | [`Mutator::delete`] | `kv.Mutator.Delete` |
//! | [`iter`](TableStorage::iter) | [`Retriever::iter`] | `kv.Retriever.Iter` |
//!
//! [`get`](TableStorage::get) returns the value bytes rather than the source
//! `ValueEntry`, whose `commit_ts` this tier always reports as `0` and no
//! caller reads; a real backend that needs the timestamp widens the return
//! type without touching the call sites' `Ok(value)` arm shape.
//!
//! The remaining three methods are not source KV operations and are the
//! seam's honest divergences:
//!
//! * [`key_count`](TableStorage::key_count) backs `KvTable::len`, which this
//!   tier answers from the store's key count. TiKV has no exact count; a real
//!   backend either scans or reports an approximation.
//! * [`clear`](TableStorage::clear) backs `TRUNCATE`, which TiKV performs as
//!   an unsafe-destroy-range / new-table-id operation, not as "empty the
//!   container".
//! * [`clone_box`](TableStorage::clone_box) exists because `KvTable` is
//!   `Clone` (the catalog hands out copies). Cloning an in-process store
//!   copies bytes; a real backend clones a *handle* to shared storage, which
//!   is why the method is on the trait rather than a `Clone` bound.
//!
//! # What a TiKV-backed implementation still needs
//!
//! Nothing in this module reaches for it, and this round deliberately does not
//! wire it. What it will need, on top of implementing the four KV methods:
//!
//! * A per-statement transaction context. The methods here take `&mut self`
//!   and commit nothing; a real backend stages mutations in the transaction's
//!   `MemBuffer` and commits at statement/transaction end, so the seam's
//!   owner must hold the `Transaction`, not the table.
//! * A TSO for the read snapshot: `get`/`iter` must read at the statement's
//!   `start_ts` rather than "latest write wins", which is what the in-process
//!   store does.
//! * Region errors, lock conflicts and stale-region retries surfaced as a
//!   *retryable* failure -- [`StorageError::Retryable`] is the slot reserved
//!   for that; the in-process store never produces it.
//! * Backoff/deadline plumbing, which has no counterpart here.

use std::fmt;

use tidb_txnkv::{
    GetOptions, Getter, Key, KvIterator, MemStorage, MemStorageError, Mutator, Retriever,
};

/// A failure reported by a storage backend.
///
/// The variant names match [`MemStorageError`]'s so the text a
/// `KvTableError::Storage` carries is unchanged by the seam.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageError {
    /// Go `kv.ErrNotExist`: the key has no value.
    NotFound,
    /// The iterator is exhausted, so it cannot advance further.
    InvalidIterator,
    /// The backend refused the operation and the caller cannot retry.
    Backend(String),
    /// The backend refused the operation but the statement may retry: a region
    /// error, a stale epoch, or a resolvable lock. Never produced in-process.
    Retryable(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::NotFound => f.write_str("key not found"),
            StorageError::InvalidIterator => f.write_str("iterator is exhausted"),
            StorageError::Backend(message) => write!(f, "storage error: {message}"),
            StorageError::Retryable(message) => write!(f, "retryable storage error: {message}"),
        }
    }
}

impl From<MemStorageError> for StorageError {
    fn from(error: MemStorageError) -> Self {
        match error {
            MemStorageError::NotFound => StorageError::NotFound,
            MemStorageError::InvalidIterator => StorageError::InvalidIterator,
        }
    }
}

/// A forward iterator over a storage range, with the error type erased.
///
/// This is [`KvIterator`] with `Error` pinned to [`StorageError`], which makes
/// it object-safe.
pub trait StorageIterator {
    /// Whether the current position holds an entry (Go `Iterator.Valid`).
    fn valid(&self) -> bool;
    /// The key at the current position (Go `Iterator.Key`).
    fn key(&self) -> &Key;
    /// The value at the current position (Go `Iterator.Value`).
    fn value(&self) -> &[u8];
    /// Advances one entry (Go `Iterator.Next`).
    fn next(&mut self) -> Result<(), StorageError>;
    /// Releases the iterator (Go `Iterator.Close`).
    fn close(&mut self);
}

impl<I> StorageIterator for I
where
    I: KvIterator,
    StorageError: From<<I as KvIterator>::Error>,
{
    fn valid(&self) -> bool {
        KvIterator::valid(self)
    }

    fn key(&self) -> &Key {
        KvIterator::key(self)
    }

    fn value(&self) -> &[u8] {
        KvIterator::value(self)
    }

    fn next(&mut self) -> Result<(), StorageError> {
        KvIterator::next(self).map_err(StorageError::from)
    }

    fn close(&mut self) {
        KvIterator::close(self);
    }
}

/// The key/value backend a table's rows and index entries live in.
///
/// All four KV methods speak raw TiKV-format bytes: the caller encodes record
/// and index keys itself, exactly as Go's `tablecodec` does before handing
/// them to `kv.Retriever`/`kv.Mutator`. Nothing about row or index *layout*
/// belongs here.
pub trait TableStorage: fmt::Debug + Send {
    /// Reads one key, Go `kv.Retriever.Get`. Reports
    /// [`StorageError::NotFound`] when the key has no value, as Go returns
    /// `kv.ErrNotExist`.
    fn get(&mut self, key: &Key) -> Result<Vec<u8>, StorageError>;

    /// Writes one key, Go `kv.Mutator.Set`.
    fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), StorageError>;

    /// Removes one key, Go `kv.Mutator.Delete`. Removing an absent key
    /// succeeds, as it does in Go.
    fn delete(&mut self, key: Key) -> Result<(), StorageError>;

    /// Iterates `[start, upper_bound)` in key order, Go `kv.Retriever.Iter`.
    /// `None` is Go's `nil` unbounded end.
    fn iter(
        &mut self,
        start: Option<&Key>,
        upper_bound: Option<&Key>,
    ) -> Result<Box<dyn StorageIterator>, StorageError>;

    /// The number of stored keys. Divergence: see the module doc.
    fn key_count(&self) -> usize;

    /// Drops every key this backend holds. Divergence: see the module doc.
    fn clear(&mut self);

    /// Clones the backend behind the trait object. Divergence: see the module
    /// doc.
    fn clone_box(&self) -> Box<dyn TableStorage>;
}

impl Clone for Box<dyn TableStorage> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// The in-process backend: a `tidb-txnkv` [`MemStorage`] behind
/// [`TableStorage`].
///
/// NOT MODELLED (documented): MVCC versions, timestamps, locks, regions and
/// the staging buffer -- every read sees the latest write immediately.
#[derive(Clone, Debug, Default)]
pub struct MemTableStorage {
    inner: MemStorage,
}

impl MemTableStorage {
    /// Builds an empty in-process backend.
    #[must_use]
    pub fn new() -> Self {
        MemTableStorage::default()
    }
}

impl TableStorage for MemTableStorage {
    fn get(&mut self, key: &Key) -> Result<Vec<u8>, StorageError> {
        Getter::get(&mut self.inner, key, GetOptions::default())
            .map(|entry| entry.value.clone())
            .map_err(StorageError::from)
    }

    fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), StorageError> {
        Mutator::set(&mut self.inner, key, value).map_err(StorageError::from)
    }

    fn delete(&mut self, key: Key) -> Result<(), StorageError> {
        Mutator::delete(&mut self.inner, key).map_err(StorageError::from)
    }

    fn iter(
        &mut self,
        start: Option<&Key>,
        upper_bound: Option<&Key>,
    ) -> Result<Box<dyn StorageIterator>, StorageError> {
        Retriever::iter(&mut self.inner, start, upper_bound)
            .map(|iterator| Box::new(iterator) as Box<dyn StorageIterator>)
            .map_err(StorageError::from)
    }

    fn key_count(&self) -> usize {
        self.inner.len()
    }

    fn clear(&mut self) {
        self.inner = MemStorage::new();
    }

    fn clone_box(&self) -> Box<dyn TableStorage> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(bytes: &[u8]) -> Key {
        Key::from_bytes(bytes.to_vec())
    }

    #[test]
    fn point_write_read_delete_round_trips() {
        let mut store: Box<dyn TableStorage> = Box::new(MemTableStorage::new());
        assert_eq!(store.get(&key(b"a")), Err(StorageError::NotFound));
        store.set(key(b"a"), vec![1, 2]).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), vec![1, 2]);
        assert_eq!(store.key_count(), 1);
        store.delete(key(b"a")).unwrap();
        assert_eq!(store.get(&key(b"a")), Err(StorageError::NotFound));
        // Deleting an absent key succeeds, as Go's `Delete` does.
        store.delete(key(b"a")).unwrap();
    }

    #[test]
    fn iteration_is_half_open_and_ordered() {
        let mut store: Box<dyn TableStorage> = Box::new(MemTableStorage::new());
        for byte in *b"abcd" {
            store.set(key(&[byte]), vec![byte]).unwrap();
        }
        let mut iterator = store.iter(Some(&key(b"b")), Some(&key(b"d"))).unwrap();
        let mut seen = Vec::new();
        while iterator.valid() {
            seen.push(iterator.value()[0]);
            iterator.next().unwrap();
        }
        iterator.close();
        assert_eq!(seen, vec![b'b', b'c']);
        // Advancing past the end reports the source iterator error.
        let mut iterator = store.iter(Some(&key(b"z")), None).unwrap();
        assert_eq!(iterator.next(), Err(StorageError::InvalidIterator));
    }

    #[test]
    fn clone_box_copies_the_in_process_bytes() {
        let mut store: Box<dyn TableStorage> = Box::new(MemTableStorage::new());
        store.set(key(b"a"), vec![1]).unwrap();
        let mut copy = store.clone();
        copy.set(key(b"b"), vec![2]).unwrap();
        assert_eq!(store.key_count(), 1);
        assert_eq!(copy.key_count(), 2);
        copy.clear();
        assert_eq!(copy.key_count(), 0);
        assert_eq!(store.key_count(), 1);
    }
}
