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

//! TiDB-side mutable-buffer adapter translated from
//! `pkg/store/driver/txn/unionstore_driver.go`.
//!
//! [`MemBufferDriver`] is deliberately not an in-memory database. It wraps an
//! injected backend and owns only TiDB's typed transaction-buffer contract:
//! mutation, tombstones, flags, assertions, staging, ordered iteration, and
//! pipelined-DML snapshot suppression. A future TiKV client adapter supplies
//! the concrete backend and converts its errors at that boundary.

use std::collections::HashMap;
use std::marker::PhantomData;

use crate::batch_getter::{BatchBufferGetter, BatchGetOptions, GetOptions, Getter, ValueEntry};
use crate::driver::read::{TransactionBuffer, TransactionReadError};
use crate::iteration::KvIterator;
use crate::{AssertionOp, FlagsOp, Key, KeyFlags};

/// Opaque handle returned by one backend staging scope.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct StagingHandle(isize);

impl StagingHandle {
    /// Invalid staging handle, matching the Go zero value.
    pub const INVALID: Self = Self(0);
    /// Sentinel that always names the last active stage.
    pub const LAST_ACTIVE: Self = Self(-1);

    /// Wraps one backend-native staging index.
    #[must_use]
    pub const fn new(index: usize) -> Self {
        Self(index as isize)
    }

    /// Returns a positive backend-native staging index.
    #[must_use]
    pub fn index(self) -> Option<usize> {
        (self.0 > 0).then(|| self.0 as usize)
    }

    /// Returns the source signed representation.
    #[must_use]
    pub const fn raw(self) -> isize {
        self.0
    }
}

/// Concrete mutable storage operations required by [`MemBufferDriver`].
///
/// The backend speaks canonical TiDB types. This avoids embedding client-go
/// bit layouts or error enums in the transaction layer while leaving one
/// explicit place for a future TiKV adapter to perform those conversions.
pub trait MemBufferBackend {
    /// Canonical error returned to the TiDB transaction driver.
    type Error: TransactionReadError;
    /// Ordered iterator used by live and snapshot buffer scans.
    type Iter: KvIterator<Error = Self::Error>;
    /// Getter over a stable buffer snapshot.
    type SnapshotGetter: Getter<Error = Self::Error>;

    /// Number of keys retained by the buffer.
    fn len(&self) -> usize;
    /// Returns whether the buffer retains no keys.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Approximate memory occupied by the buffer.
    fn size(&self) -> usize;
    /// Reads one live buffered value, including an empty deletion tombstone.
    fn get(&mut self, key: &Key, options: GetOptions) -> Result<ValueEntry, Self::Error>;
    /// Reads live buffered values for the requested keys.
    fn batch_get(
        &mut self,
        keys: &[Key],
        options: BatchGetOptions,
    ) -> Result<HashMap<Key, ValueEntry>, Self::Error>;
    /// Inserts or replaces one value.
    fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), Self::Error>;
    /// Inserts or replaces one value and monotonically applies flags.
    fn set_with_flags(
        &mut self,
        key: Key,
        value: Vec<u8>,
        operations: &[FlagsOp],
    ) -> Result<(), Self::Error>;
    /// Stores an empty deletion tombstone.
    fn delete(&mut self, key: Key) -> Result<(), Self::Error>;
    /// Stores a deletion tombstone and monotonically applies flags.
    fn delete_with_flags(&mut self, key: Key, operations: &[FlagsOp]) -> Result<(), Self::Error>;
    /// Removes one key from the buffer instead of adding a tombstone.
    fn remove_from_buffer(&mut self, key: &Key);
    /// Applies non-assertion flags to one key.
    fn update_flags(&mut self, key: &Key, operations: &[FlagsOp]);
    /// Applies one assertion operation to one key.
    fn update_assertion_flags(&mut self, key: &Key, operation: AssertionOp);
    /// Returns the current typed flags for one key.
    fn get_flags(&self, key: &Key) -> Result<KeyFlags, Self::Error>;
    /// Starts one nested staging scope.
    fn staging(&mut self) -> StagingHandle;
    /// Rolls one staging scope back.
    fn cleanup(&mut self, handle: StagingHandle);
    /// Commits one staging scope into its parent.
    fn release(&mut self, handle: StagingHandle);
    /// Visits the entries owned by one staging scope.
    fn inspect_stage(&self, handle: StagingHandle, inspect: &mut dyn FnMut(&Key, KeyFlags, &[u8]));
    /// Creates a forward iterator over `[start, upper_bound)`.
    fn iter(&mut self, start: &Key, upper_bound: Option<&Key>) -> Result<Self::Iter, Self::Error>;
    /// Creates a reverse iterator below `start` and at or above `lower_bound`.
    fn iter_reverse(
        &mut self,
        start: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> Result<Self::Iter, Self::Error>;
    /// Creates a stable forward iterator over the current buffer snapshot.
    fn snapshot_iter(&mut self, start: &Key, upper_bound: Option<&Key>) -> Self::Iter;
    /// Creates a stable reverse iterator over the current buffer snapshot.
    fn snapshot_iter_reverse(
        &mut self,
        start: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> Self::Iter;
    /// Creates a stable getter over the current buffer snapshot.
    fn snapshot_getter(&mut self) -> Self::SnapshotGetter;
    /// Reads backend-local bytes without consulting a transaction snapshot.
    fn get_local(&mut self, key: &Key) -> Result<Vec<u8>, Self::Error>;
}

/// TiDB transaction-buffer adapter over an injected concrete backend.
pub struct MemBufferDriver<B> {
    backend: B,
    pipelined_dml: bool,
}

impl<B> MemBufferDriver<B> {
    /// Wraps one backend with source-shaped pipelined-DML snapshot behavior.
    #[must_use]
    pub const fn new(backend: B, pipelined_dml: bool) -> Self {
        Self {
            backend,
            pipelined_dml,
        }
    }

    /// Borrows the injected backend for client-specific operations.
    #[must_use]
    pub const fn backend(&self) -> &B {
        &self.backend
    }

    /// Mutably borrows the injected backend.
    pub const fn backend_mut(&mut self) -> &mut B {
        &mut self.backend
    }

    /// Returns the injected backend.
    pub fn into_backend(self) -> B {
        self.backend
    }

    /// Returns whether stable buffer snapshots are suppressed.
    #[must_use]
    pub const fn is_pipelined_dml(&self) -> bool {
        self.pipelined_dml
    }
}

impl<B: MemBufferBackend> MemBufferDriver<B> {
    /// Number of keys retained by the live buffer.
    #[must_use]
    pub fn len(&self) -> usize {
        self.backend.len()
    }

    /// Returns whether the live buffer contains no keys.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Approximate memory occupied by the live buffer.
    #[must_use]
    pub fn size(&self) -> usize {
        self.backend.size()
    }

    /// Removes one key without storing a deletion tombstone.
    pub fn remove_from_buffer(&mut self, key: &Key) {
        self.backend.remove_from_buffer(key);
    }

    /// Sets a value and applies typed non-assertion flags.
    pub fn set_with_flags(
        &mut self,
        key: Key,
        value: Vec<u8>,
        operations: &[FlagsOp],
    ) -> Result<(), B::Error> {
        self.backend.set_with_flags(key, value, operations)
    }

    /// Stores a tombstone and applies typed non-assertion flags.
    pub fn delete_with_flags(&mut self, key: Key, operations: &[FlagsOp]) -> Result<(), B::Error> {
        self.backend.delete_with_flags(key, operations)
    }

    /// Applies typed non-assertion flags.
    pub fn update_flags(&mut self, key: &Key, operations: &[FlagsOp]) {
        self.backend.update_flags(key, operations);
    }

    /// Applies one typed assertion operation.
    pub fn update_assertion_flags(&mut self, key: &Key, operation: AssertionOp) {
        self.backend.update_assertion_flags(key, operation);
    }

    /// Returns the current typed flags for one key.
    pub fn get_flags(&self, key: &Key) -> Result<KeyFlags, B::Error> {
        self.backend.get_flags(key)
    }

    /// Starts a nested staging scope.
    pub fn staging(&mut self) -> StagingHandle {
        self.backend.staging()
    }

    /// Rolls a staging scope back.
    pub fn cleanup(&mut self, handle: StagingHandle) {
        self.backend.cleanup(handle);
    }

    /// Commits a staging scope into its parent.
    pub fn release(&mut self, handle: StagingHandle) {
        self.backend.release(handle);
    }

    /// Visits every entry owned by one staging scope.
    pub fn inspect_stage(
        &self,
        handle: StagingHandle,
        mut inspect: impl FnMut(&Key, KeyFlags, &[u8]),
    ) {
        self.backend.inspect_stage(handle, &mut inspect);
    }

    /// Reads backend-local bytes without consulting a transaction snapshot.
    pub fn get_local(&mut self, key: &Key) -> Result<Vec<u8>, B::Error> {
        self.backend.get_local(key)
    }

    /// Creates a stable buffer-snapshot iterator, or an empty iterator for
    /// pipelined DML exactly like Go `memBuffer.SnapshotIter`.
    pub fn snapshot_iter(
        &mut self,
        start: &Key,
        upper_bound: Option<&Key>,
    ) -> MemBufferSnapshotIterator<B::Iter, B::Error> {
        if self.pipelined_dml {
            MemBufferSnapshotIterator::Empty(EmptyIterator::new())
        } else {
            MemBufferSnapshotIterator::Backend(self.backend.snapshot_iter(start, upper_bound))
        }
    }

    /// Creates a stable reverse buffer-snapshot iterator, or an empty iterator
    /// for pipelined DML.
    pub fn snapshot_iter_reverse(
        &mut self,
        start: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> MemBufferSnapshotIterator<B::Iter, B::Error> {
        if self.pipelined_dml {
            MemBufferSnapshotIterator::Empty(EmptyIterator::new())
        } else {
            MemBufferSnapshotIterator::Backend(
                self.backend.snapshot_iter_reverse(start, lower_bound),
            )
        }
    }

    /// Creates a stable buffer-snapshot getter, or an empty getter for
    /// pipelined DML exactly like Go `memBuffer.SnapshotGetter`.
    pub fn snapshot_getter(&mut self) -> MemBufferSnapshotGetter<B::SnapshotGetter, B::Error> {
        if self.pipelined_dml {
            MemBufferSnapshotGetter::Empty(PhantomData)
        } else {
            MemBufferSnapshotGetter::Backend(self.backend.snapshot_getter())
        }
    }
}

impl<B: MemBufferBackend> Getter for MemBufferDriver<B> {
    type Error = B::Error;

    fn get(&mut self, key: &Key, options: GetOptions) -> Result<ValueEntry, Self::Error> {
        self.backend.get(key, options)
    }
}

impl<B: MemBufferBackend> BatchBufferGetter for MemBufferDriver<B> {
    type Error = B::Error;

    fn batch_get(
        &mut self,
        keys: &[Key],
        options: BatchGetOptions,
    ) -> Result<HashMap<Key, ValueEntry>, Self::Error> {
        self.backend.batch_get(keys, options)
    }

    fn len(&self) -> usize {
        self.backend.len()
    }
}

impl<B: MemBufferBackend> TransactionBuffer for MemBufferDriver<B> {
    type Iter = B::Iter;

    fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), B::Error> {
        self.backend.set(key, value)
    }

    fn delete(&mut self, key: Key) -> Result<(), B::Error> {
        self.backend.delete(key)
    }

    fn iter(&mut self, start: &Key, upper_bound: Option<&Key>) -> Result<Self::Iter, B::Error> {
        self.backend.iter(start, upper_bound)
    }

    fn iter_reverse(
        &mut self,
        start: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> Result<Self::Iter, B::Error> {
        self.backend.iter_reverse(start, lower_bound)
    }
}

/// Stable snapshot getter selected from the backend or the pipelined empty
/// path.
pub enum MemBufferSnapshotGetter<G, E> {
    /// Backend-owned snapshot getter.
    Backend(G),
    /// Pipelined DML exposes no stable buffer snapshot.
    Empty(PhantomData<E>),
}

impl<G, E> Getter for MemBufferSnapshotGetter<G, E>
where
    G: Getter<Error = E>,
    E: TransactionReadError,
{
    type Error = E;

    fn get(&mut self, key: &Key, options: GetOptions) -> Result<ValueEntry, Self::Error> {
        match self {
            Self::Backend(getter) => getter.get(key, options),
            Self::Empty(_) => Err(empty_not_found()),
        }
    }
}

fn empty_not_found<E: TransactionReadError>() -> E {
    E::not_found()
}

/// Stable snapshot iterator selected from the backend or the pipelined empty
/// path.
pub enum MemBufferSnapshotIterator<I, E> {
    /// Backend-owned snapshot iterator.
    Backend(I),
    /// Pipelined DML exposes an always-invalid iterator.
    Empty(EmptyIterator<E>),
}

impl<I, E> KvIterator for MemBufferSnapshotIterator<I, E>
where
    I: KvIterator<Error = E>,
{
    type Error = E;

    fn valid(&self) -> bool {
        match self {
            Self::Backend(iterator) => iterator.valid(),
            Self::Empty(iterator) => iterator.valid(),
        }
    }

    fn key(&self) -> &Key {
        match self {
            Self::Backend(iterator) => iterator.key(),
            Self::Empty(iterator) => iterator.key(),
        }
    }

    fn value(&self) -> &[u8] {
        match self {
            Self::Backend(iterator) => iterator.value(),
            Self::Empty(iterator) => iterator.value(),
        }
    }

    fn next(&mut self) -> Result<(), Self::Error> {
        match self {
            Self::Backend(iterator) => iterator.next(),
            Self::Empty(iterator) => iterator.next(),
        }
    }

    fn close(&mut self) {
        match self {
            Self::Backend(iterator) => iterator.close(),
            Self::Empty(iterator) => iterator.close(),
        }
    }
}

/// Always-invalid iterator used by pipelined-DML snapshot views.
pub struct EmptyIterator<E>(PhantomData<E>);

impl<E> EmptyIterator<E> {
    const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<E> KvIterator for EmptyIterator<E> {
    type Error = E;

    fn valid(&self) -> bool {
        false
    }

    fn key(&self) -> &Key {
        panic!("key called on an invalid empty iterator")
    }

    fn value(&self) -> &[u8] {
        panic!("value called on an invalid empty iterator")
    }

    fn next(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn close(&mut self) {}
}
