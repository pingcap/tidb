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

//! The KV transaction Go `pkg/structure` reads and writes through (Go's
//! `kv.Retriever` / `kv.RetrieverMutator` handles of `NewStructure`).

use crate::StructureError;

/// Streaming raw iterator used by Go's `structure.ReverseHashIterator`.
///
/// `next` is fallible and must not be folded into iterator construction: Go
/// can return an error after filtering or decoding an earlier history entry.
pub trait RawKvIterator {
    /// The transaction's error.
    type Error;

    /// Whether the iterator currently points at an entry.
    fn valid(&self) -> bool;

    /// Encoded key at the current iterator position.
    fn key(&self) -> &[u8];

    /// Value at the current iterator position.
    fn value(&self) -> &[u8];

    /// Advances to the next entry.
    fn next(&mut self) -> Result<(), Self::Error>;
}

/// Callback shape for one raw half-open range entry.
pub type RawRangeVisitor<'a, E> = dyn FnMut(&[u8], &[u8]) -> Result<(), E> + 'a;

struct OwnedRawKvIterator<E> {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    position: usize,
    fail_next_at: Option<(usize, E)>,
}

/// A reverse iterator over materialized `entries` (in key order), optionally
/// stopping at an inclusive upper key; `fail_next_at` makes `next` fail at
/// that zero-based position, leaving the iterator in place.
#[must_use]
pub fn owned_reverse_iterator<E: Clone + 'static>(
    mut entries: Vec<(Vec<u8>, Vec<u8>)>,
    upper_inclusive: Option<&[u8]>,
    fail_next_at: Option<(usize, E)>,
) -> Box<dyn RawKvIterator<Error = E>> {
    if let Some(upper) = upper_inclusive {
        entries.retain(|(key, _)| key.as_slice() <= upper);
    }
    entries.reverse();
    Box::new(OwnedRawKvIterator {
        entries,
        position: 0,
        fail_next_at,
    })
}

impl<E: Clone> RawKvIterator for OwnedRawKvIterator<E> {
    type Error = E;

    fn valid(&self) -> bool {
        self.position < self.entries.len()
    }

    fn key(&self) -> &[u8] {
        &self.entries[self.position].0
    }

    fn value(&self) -> &[u8] {
        &self.entries[self.position].1
    }

    fn next(&mut self) -> Result<(), E> {
        if let Some((position, error)) = &self.fail_next_at {
            if *position == self.position {
                return Err(error.clone());
            }
        }
        self.position += 1;
        Ok(())
    }
}

/// The raw transactional capabilities Go's `structure.TxStructure` uses.
pub trait RawTransaction {
    /// The transaction's error, which also carries [`StructureError`] (Go's
    /// structure returns whichever `error` it met).
    type Error: From<StructureError> + Clone + 'static;

    /// Reads one encoded key. Missing keys are `None`.
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Writes one encoded key and value.
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Self::Error>;

    /// Deletes one encoded key. Deleting a missing key succeeds.
    fn delete(&mut self, key: &[u8]) -> Result<(), Self::Error>;

    /// Returns every encoded key/value pair beginning with `prefix`, in byte
    /// order and isolated from later mutation of the transaction.
    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error>;

    /// Constructs a reverse iterator over `prefix`, optionally stopping at an
    /// inclusive encoded upper key. Adapters should override this when their
    /// storage iterator can fail during `next`; the materializing default is
    /// exact for stores whose prefix scan is the only fallible operation.
    fn reverse_scan_prefix(
        &mut self,
        prefix: &[u8],
        upper_inclusive: Option<&[u8]>,
    ) -> Result<Box<dyn RawKvIterator<Error = Self::Error>>, Self::Error> {
        Ok(owned_reverse_iterator(
            self.scan_prefix(prefix)?,
            upper_inclusive,
            None,
        ))
    }

    /// Iterates the half-open encoded key range `[start, end)` in byte order.
    /// Implementations must stream rather than materialize the range: Go's
    /// `IterAllTables` exists specifically to avoid loading all metadata at
    /// once.
    fn iterate_range(
        &mut self,
        start: &[u8],
        end: &[u8],
        visit: &mut RawRangeVisitor<'_, Self::Error>,
    ) -> Result<(), Self::Error>;
}

/// Go `meta.NewMutator(txn)` borrows its transaction: the caller keeps it and
/// commits it afterwards, which is how `kv.RunInNewTxn`'s callback uses one.
/// A borrowed transaction is itself a transaction.
impl<T: RawTransaction + ?Sized> RawTransaction for &mut T {
    type Error = T::Error;

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        (**self).get(key)
    }

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Self::Error> {
        (**self).set(key, value)
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), Self::Error> {
        (**self).delete(key)
    }

    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error> {
        (**self).scan_prefix(prefix)
    }

    fn reverse_scan_prefix(
        &mut self,
        prefix: &[u8],
        upper_inclusive: Option<&[u8]>,
    ) -> Result<Box<dyn RawKvIterator<Error = Self::Error>>, Self::Error> {
        (**self).reverse_scan_prefix(prefix, upper_inclusive)
    }

    fn iterate_range(
        &mut self,
        start: &[u8],
        end: &[u8],
        visit: &mut RawRangeVisitor<'_, Self::Error>,
    ) -> Result<(), Self::Error> {
        (**self).iterate_range(start, end, visit)
    }
}
