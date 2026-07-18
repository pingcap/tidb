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

//! Transaction buffer/snapshot read composition translated from
//! `pkg/store/driver/txn/{batch_getter,scanner,snapshot,txn_driver,union_iter}.go`.
//!
//! This module is the production composition boundary over an injected storage
//! client. It deliberately owns no TiKV RPC, MVCC, timestamp-oracle, lock, or
//! commit implementation. A future client adapter supplies the buffer and
//! snapshot traits; the TiDB read semantics already execute here.

use std::collections::HashMap;
use std::marker::PhantomData;

use crate::batch_getter::{
    batch_get_from_layers, BatchBufferGetter, BatchGetError, BatchGetOptions, BatchGetter, Getter,
    ValueEntry,
};
use crate::error::{KvError, ERR_NOT_EXIST};
use crate::iteration::KvIterator;
use crate::key::Key;
use crate::union_iter::UnionIter;

/// Error behavior required by the transaction read path.
pub trait TransactionReadError: BatchGetError {
    /// Constructs the canonical TiDB not-found identity.
    fn not_found() -> Self;
}

impl TransactionReadError for KvError {
    fn not_found() -> Self {
        ERR_NOT_EXIST.clone()
    }
}

/// Mutable transaction-buffer behavior consumed by the read driver.
pub trait TransactionBuffer: Getter + BatchBufferGetter<Error = <Self as Getter>::Error> {
    /// Concrete ordered iterator over buffered entries, including tombstones.
    type Iter: KvIterator<Error = <Self as Getter>::Error>;

    /// Stores or replaces one dirty value.
    fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), <Self as Getter>::Error>;

    /// Stores the source empty-value deletion tombstone.
    fn delete(&mut self, key: Key) -> Result<(), <Self as Getter>::Error>;

    /// Iterates the half-open range `[start, upper_bound)`.
    fn iter(
        &mut self,
        start: &Key,
        upper_bound: Option<&Key>,
    ) -> Result<Self::Iter, <Self as Getter>::Error>;

    /// Iterates backward below `start`, including `lower_bound` when present.
    fn iter_reverse(
        &mut self,
        start: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> Result<Self::Iter, <Self as Getter>::Error>;
}

/// Snapshot behavior consumed by the read driver.
pub trait TransactionSnapshot: Getter + BatchGetter<Error = <Self as Getter>::Error> {
    /// Concrete ordered snapshot iterator.
    type Iter: KvIterator<Error = <Self as Getter>::Error>;

    /// Iterates the half-open range `[start, upper_bound)`.
    fn iter(
        &mut self,
        start: &Key,
        upper_bound: Option<&Key>,
    ) -> Result<Self::Iter, <Self as Getter>::Error>;

    /// Iterates backward below `start`, including `lower_bound` when present.
    fn iter_reverse(
        &mut self,
        start: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> Result<Self::Iter, <Self as Getter>::Error>;
}

/// Source-shaped snapshot interception boundary.
///
/// An installed interceptor replaces each snapshot operation, exactly as Go's
/// `kv.SnapshotInterceptor`; it may delegate to `snapshot`, return another
/// value/iterator, or return its own error.
pub trait SnapshotInterceptor<S>
where
    S: TransactionSnapshot,
{
    /// Iterator returned by intercepted scans.
    type Iter: KvIterator<Error = <S as Getter>::Error>;

    /// Replaces snapshot `Get`.
    fn on_get(
        &mut self,
        snapshot: &mut S,
        key: &Key,
        options: BatchGetOptions,
    ) -> Result<ValueEntry, <S as Getter>::Error>;

    /// Replaces snapshot `BatchGet`.
    fn on_batch_get(
        &mut self,
        snapshot: &mut S,
        keys: &[Key],
        options: BatchGetOptions,
    ) -> Result<HashMap<Key, ValueEntry>, <S as Getter>::Error>;

    /// Replaces snapshot `Iter`.
    fn on_iter(
        &mut self,
        snapshot: &mut S,
        start: &Key,
        upper_bound: Option<&Key>,
    ) -> Result<Self::Iter, <S as Getter>::Error>;

    /// Replaces snapshot `IterReverse`.
    fn on_iter_reverse(
        &mut self,
        snapshot: &mut S,
        start: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> Result<Self::Iter, <S as Getter>::Error>;
}

/// Marker used by [`TransactionReadDriver::new`] before an interceptor is
/// installed.
pub struct NoSnapshotInterceptor;

impl<S> SnapshotInterceptor<S> for NoSnapshotInterceptor
where
    S: TransactionSnapshot,
{
    type Iter = S::Iter;

    fn on_get(
        &mut self,
        snapshot: &mut S,
        key: &Key,
        options: BatchGetOptions,
    ) -> Result<ValueEntry, <S as Getter>::Error> {
        snapshot.get(key, options)
    }

    fn on_batch_get(
        &mut self,
        snapshot: &mut S,
        keys: &[Key],
        options: BatchGetOptions,
    ) -> Result<HashMap<Key, ValueEntry>, <S as Getter>::Error> {
        snapshot.batch_get(keys, options)
    }

    fn on_iter(
        &mut self,
        snapshot: &mut S,
        start: &Key,
        upper_bound: Option<&Key>,
    ) -> Result<Self::Iter, <S as Getter>::Error> {
        snapshot.iter(start, upper_bound)
    }

    fn on_iter_reverse(
        &mut self,
        snapshot: &mut S,
        start: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> Result<Self::Iter, <S as Getter>::Error> {
        snapshot.iter_reverse(start, lower_bound)
    }
}

/// Snapshot iterator selected from the underlying source or its interceptor.
pub enum SnapshotIterator<S, I> {
    /// Iterator returned by the snapshot itself.
    Source(S),
    /// Iterator returned by the installed interceptor.
    Intercepted(I),
}

/// Iterator that merges dirty-buffer entries with the selected snapshot or
/// intercepted snapshot stream.
pub type TransactionIterator<BufferIter, SnapshotIter, InterceptorIter> =
    UnionIter<BufferIter, SnapshotIterator<SnapshotIter, InterceptorIter>>;

/// Fallible transaction scan result over a buffer, snapshot, and optional
/// snapshot interceptor.
pub type TransactionIteratorResult<Buffer, Snapshot, Interceptor> = Result<
    TransactionIterator<
        <Buffer as TransactionBuffer>::Iter,
        <Snapshot as TransactionSnapshot>::Iter,
        <Interceptor as SnapshotInterceptor<Snapshot>>::Iter,
    >,
    <Buffer as Getter>::Error,
>;

impl<S, I> KvIterator for SnapshotIterator<S, I>
where
    S: KvIterator,
    I: KvIterator<Error = S::Error>,
{
    type Error = S::Error;

    fn valid(&self) -> bool {
        match self {
            Self::Source(iterator) => iterator.valid(),
            Self::Intercepted(iterator) => iterator.valid(),
        }
    }

    fn key(&self) -> &Key {
        match self {
            Self::Source(iterator) => iterator.key(),
            Self::Intercepted(iterator) => iterator.key(),
        }
    }

    fn value(&self) -> &[u8] {
        match self {
            Self::Source(iterator) => iterator.value(),
            Self::Intercepted(iterator) => iterator.value(),
        }
    }

    fn next(&mut self) -> Result<(), Self::Error> {
        match self {
            Self::Source(iterator) => iterator.next(),
            Self::Intercepted(iterator) => iterator.next(),
        }
    }

    fn close(&mut self) {
        match self {
            Self::Source(iterator) => iterator.close(),
            Self::Intercepted(iterator) => iterator.close(),
        }
    }
}

/// Connected TiDB transaction read path over one dirty buffer and snapshot.
pub struct TransactionReadDriver<B, S, I = NoSnapshotInterceptor> {
    buffer: B,
    snapshot: S,
    interceptor: Option<I>,
}

impl<B, S> TransactionReadDriver<B, S, NoSnapshotInterceptor> {
    /// Creates a transaction without a snapshot interceptor.
    pub fn new(buffer: B, snapshot: S) -> Self {
        Self {
            buffer,
            snapshot,
            interceptor: None,
        }
    }
}

impl<B, S, I> TransactionReadDriver<B, S, I> {
    /// Borrows the canonical mutable transaction buffer.
    #[must_use]
    pub const fn buffer(&self) -> &B {
        &self.buffer
    }

    /// Mutably borrows the canonical mutable transaction buffer.
    pub const fn buffer_mut(&mut self) -> &mut B {
        &mut self.buffer
    }

    /// Borrows the injected transaction snapshot.
    #[must_use]
    pub const fn snapshot(&self) -> &S {
        &self.snapshot
    }

    /// Returns the buffer, snapshot, and optional interceptor without losing
    /// ownership of any transaction state.
    pub fn into_parts(self) -> (B, S, Option<I>) {
        (self.buffer, self.snapshot, self.interceptor)
    }

    /// Installs a source-shaped snapshot interceptor while preserving dirty
    /// transaction state.
    pub fn with_snapshot_interceptor<J>(self, interceptor: J) -> TransactionReadDriver<B, S, J> {
        TransactionReadDriver {
            buffer: self.buffer,
            snapshot: self.snapshot,
            interceptor: Some(interceptor),
        }
    }

    /// Mutably borrows the installed interceptor.
    pub fn snapshot_interceptor_mut(&mut self) -> Option<&mut I> {
        self.interceptor.as_mut()
    }

    /// Installs, replaces, or clears the snapshot interceptor in place.
    ///
    /// This is the runtime shape of Go `SetOption(kv.SnapInterceptor, ...)` for
    /// an already-typed interceptor boundary. [`Self::with_snapshot_interceptor`]
    /// performs the one-time Rust type transition when the first interceptor
    /// has a different concrete type; later calls replace it without moving or
    /// rebuilding transaction state.
    pub fn set_snapshot_interceptor(&mut self, interceptor: Option<I>) -> Option<I> {
        std::mem::replace(&mut self.interceptor, interceptor)
    }
}

impl<B, S, I> TransactionReadDriver<B, S, I>
where
    B: TransactionBuffer,
    <B as Getter>::Error: TransactionReadError,
    S: TransactionSnapshot + Getter<Error = <B as Getter>::Error>,
    I: SnapshotInterceptor<S>,
{
    /// Gets one value with dirty-buffer precedence.
    ///
    /// A dirty tombstone never falls through to the snapshot. Missing buffer
    /// keys do fall through; an empty successful result from either layer is
    /// normalized to TiDB's not-found identity.
    pub fn get(
        &mut self,
        key: &Key,
        options: BatchGetOptions,
    ) -> Result<ValueEntry, <B as Getter>::Error> {
        let value = match self.buffer.get(key, options) {
            Ok(value) => value,
            Err(error) if error.is_not_found() => self.snapshot_get(key, options)?,
            Err(error) => return Err(error),
        };
        if value.value.is_empty() {
            return Err(<B as Getter>::Error::not_found());
        }
        Ok(value)
    }

    /// Batch-gets through the canonical buffer/snapshot merge authority.
    pub fn batch_get(
        &mut self,
        keys: &[Key],
        options: BatchGetOptions,
    ) -> Result<HashMap<Key, ValueEntry>, <B as Getter>::Error> {
        let mut snapshot = DriverBatchSnapshot {
            snapshot: &mut self.snapshot,
            interceptor: &mut self.interceptor,
        };
        batch_get_from_layers(
            &mut self.buffer,
            None::<&mut NoMiddleCache<<B as Getter>::Error>>,
            &mut snapshot,
            keys,
            options,
        )
    }

    /// Stores one uncommitted value.
    pub fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), <B as Getter>::Error> {
        self.buffer.set(key, value)
    }

    /// Stores one uncommitted deletion tombstone.
    pub fn delete(&mut self, key: Key) -> Result<(), <B as Getter>::Error> {
        self.buffer.delete(key)
    }

    /// Creates the connected forward dirty/snapshot union iterator.
    pub fn iter(
        &mut self,
        start: &Key,
        upper_bound: Option<&Key>,
    ) -> TransactionIteratorResult<B, S, I> {
        let mut dirty = self.buffer.iter(start, upper_bound)?;
        let snapshot = match self.snapshot_iter(start, upper_bound) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                dirty.close();
                return Err(error);
            }
        };
        Self::join_iterators(dirty, snapshot, false)
    }

    /// Creates the connected reverse dirty/snapshot union iterator.
    pub fn iter_reverse(
        &mut self,
        start: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> TransactionIteratorResult<B, S, I> {
        let mut dirty = self.buffer.iter_reverse(start, lower_bound)?;
        let snapshot = match self.snapshot_iter_reverse(start, lower_bound) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                dirty.close();
                return Err(error);
            }
        };
        Self::join_iterators(dirty, snapshot, true)
    }

    fn snapshot_get(
        &mut self,
        key: &Key,
        options: BatchGetOptions,
    ) -> Result<ValueEntry, <B as Getter>::Error> {
        match self.interceptor.as_mut() {
            Some(interceptor) => interceptor.on_get(&mut self.snapshot, key, options),
            None => self.snapshot.get(key, options),
        }
    }

    fn snapshot_iter(
        &mut self,
        start: &Key,
        upper_bound: Option<&Key>,
    ) -> Result<SnapshotIterator<S::Iter, I::Iter>, <B as Getter>::Error> {
        match self.interceptor.as_mut() {
            Some(interceptor) => interceptor
                .on_iter(&mut self.snapshot, start, upper_bound)
                .map(SnapshotIterator::Intercepted),
            None => self
                .snapshot
                .iter(start, upper_bound)
                .map(SnapshotIterator::Source),
        }
    }

    fn snapshot_iter_reverse(
        &mut self,
        start: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> Result<SnapshotIterator<S::Iter, I::Iter>, <B as Getter>::Error> {
        match self.interceptor.as_mut() {
            Some(interceptor) => interceptor
                .on_iter_reverse(&mut self.snapshot, start, lower_bound)
                .map(SnapshotIterator::Intercepted),
            None => self
                .snapshot
                .iter_reverse(start, lower_bound)
                .map(SnapshotIterator::Source),
        }
    }

    fn join_iterators(
        dirty: B::Iter,
        snapshot: SnapshotIterator<S::Iter, I::Iter>,
        reverse: bool,
    ) -> TransactionIteratorResult<B, S, I> {
        match UnionIter::new(dirty, snapshot, reverse) {
            Ok(iterator) => Ok(iterator),
            Err(error) => {
                let (source, mut dirty, mut snapshot) = error.into_parts();
                dirty.close();
                snapshot.close();
                Err(source)
            }
        }
    }
}

struct DriverBatchSnapshot<'a, S, I> {
    snapshot: &'a mut S,
    interceptor: &'a mut Option<I>,
}

impl<S, I> BatchGetter for DriverBatchSnapshot<'_, S, I>
where
    S: TransactionSnapshot,
    I: SnapshotInterceptor<S>,
{
    type Error = <S as Getter>::Error;

    fn batch_get(
        &mut self,
        keys: &[Key],
        options: BatchGetOptions,
    ) -> Result<HashMap<Key, ValueEntry>, Self::Error> {
        match self.interceptor.as_mut() {
            Some(interceptor) => interceptor.on_batch_get(self.snapshot, keys, options),
            None => self.snapshot.batch_get(keys, options),
        }
    }
}

struct NoMiddleCache<E>(PhantomData<E>);

impl<E> Getter for NoMiddleCache<E>
where
    E: BatchGetError,
{
    type Error = E;

    fn get(&mut self, _key: &Key, _options: BatchGetOptions) -> Result<ValueEntry, Self::Error> {
        unreachable!("the transaction driver has no middle-cache layer")
    }
}
