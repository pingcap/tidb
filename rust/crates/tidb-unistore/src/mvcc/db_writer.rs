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

//! Go `db_writer.go`: the seams between the MVCC store and its engines.
//!
//! `badger.DB` / `badger.Txn` (the committed-data engine) are not ported;
//! [`DBBundle`] and [`DBSnapshot`] carry the engine as a type parameter so
//! the bundle keeps Go's shape without this package inventing an engine.
//! The lock store is the real ported [`crate::MemStore`].

use std::sync::Mutex;

use tidb_proto::KvrpcContext;

use crate::lockstore::MemStore;

use super::Lock;

/// Go `DBWriter` (`db_writer.go:24`): how the store commits batches into the
/// data engine.
pub trait DbWriter {
    /// The batch this writer builds.
    type Batch: WriteBatch;
    /// This writer's failure.
    type Error;

    /// Go `Open`.
    fn open(&mut self);
    /// Go `Close`.
    fn close(&mut self);
    /// Go `Write(batch)`.
    fn write(&mut self, batch: Self::Batch) -> Result<(), Self::Error>;
    /// Go `DeleteRange(start, end, latchHandle)`.
    fn delete_range(
        &mut self,
        start: &[u8],
        end: &[u8],
        latch_handle: &mut dyn LatchHandle,
    ) -> Result<(), Self::Error>;
    /// Go `NewWriteBatch(startTS, commitTS, ctx)`.
    fn new_write_batch(&self, start_ts: u64, commit_ts: u64, ctx: &KvrpcContext) -> Self::Batch;
}

/// Go `LatchHandle` (`db_writer.go:32`).
pub trait LatchHandle {
    /// Go `AcquireLatches`.
    fn acquire_latches(&mut self, hash_vals: &[u64]);
    /// Go `ReleaseLatches`.
    fn release_latches(&mut self, hash_vals: &[u64]);
}

/// Go `WriteBatch` (`db_writer.go:37`): the five mutations a transaction's
/// life can put into one batch.
pub trait WriteBatch {
    /// Go `Prewrite(key, lock)`.
    fn prewrite(&mut self, key: &[u8], lock: &Lock);
    /// Go `Commit(key, lock)`.
    fn commit(&mut self, key: &[u8], lock: &Lock);
    /// Go `Rollback(key, deleteLock)` — Go spells the parameter `deleleLock`;
    /// the typo is Go's, the meaning is this spelling's.
    fn rollback(&mut self, key: &[u8], delete_lock: bool);
    /// Go `PessimisticLock(key, lock)`.
    fn pessimistic_lock(&mut self, key: &[u8], lock: &Lock);
    /// Go `PessimisticRollback(key)`.
    fn pessimistic_rollback(&mut self, key: &[u8]);
}

/// Go `DBBundle` (`db_writer.go:50`), generic over the unported data engine.
/// Built by struct literal, as Go builds it — the lock store has no default
/// because its arena is explicitly sized.
#[derive(Debug)]
pub struct DBBundle<E> {
    /// Go `DB *badger.DB` — the committed-data engine, caller-supplied.
    pub db: E,
    /// Go `LockStore`.
    pub lock_store: MemStore,
    /// Go `MemStoreMu`.
    pub mem_store_mu: Mutex<()>,
    /// Go `StateTS`.
    pub state_ts: u64,
}

/// Go `DBSnapshot` (`db_writer.go:57`): one engine read view beside the live
/// lock store.
#[derive(Debug)]
pub struct DBSnapshot<'a, T> {
    /// Go `Txn *badger.Txn` — the engine's read transaction.
    pub txn: T,
    /// Go `LockStore` — the LIVE store, not a copy; Go shares the pointer.
    pub lock_store: &'a MemStore,
}

impl<'a, T> DBSnapshot<'a, T> {
    /// Go `NewDBSnapshot(db)`, with the engine's read view supplied by the
    /// engine seam rather than opened here.
    pub fn new(txn: T, lock_store: &'a MemStore) -> Self {
        Self { txn, lock_store }
    }
}
