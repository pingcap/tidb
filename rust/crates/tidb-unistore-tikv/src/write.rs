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

//! Go `write.go`: a request's [`WriteBatch`] and the [`DbWriter`] whose two
//! workers apply batches -- `writeDBWorker` the engine entries, then
//! `writeLockWorker` the lock entries. A request hands its batch to a worker
//! and waits for it; each worker drains every batch queued meanwhile and
//! applies the group in one pass, so requests never contend for the writer.

use std::sync::mpsc;
use std::sync::{Arc, PoisonError, RwLock};

use tidb_proto::KvrpcOp;
use tidb_unistore_lockstore::{Hint, MemStore};
use tidb_unistore_mvcc::{encode_extra_txn_status_key, DbUserMeta, Lock};

use crate::mvcc_store::MemEngine;

/// Go `writeBatch` (`write.go`): what one request writes, applied by
/// [`MvccStore::write`] once its checks pass -- engine entries as
/// `(key, version, value, user meta)`, lock entries as a put or (`None`) a
/// delete.
#[derive(Debug, Default)]
pub struct WriteBatch {
    start_ts: u64,
    commit_ts: u64,
    pub(crate) db: Vec<DbEntry>,
    pub(crate) locks: Vec<LockEntry>,
}

impl WriteBatch {
    /// Go `dbWriter.NewWriteBatch(startTS, commitTS, ctx)`.
    pub(crate) fn new(start_ts: u64, commit_ts: u64) -> Self {
        Self {
            start_ts,
            commit_ts,
            ..Self::default()
        }
    }

    /// Go `writeDBBatch.set` for a committed delete marker, the entry
    /// `dbWriter.DeleteRange` writes one version above a key's newest.
    pub(crate) fn delete_version(&mut self, key: &[u8], version: u64) {
        self.db
            .push((key.to_vec(), version, Vec::new(), DbUserMeta::new(0, version)));
    }
}

impl tidb_unistore_mvcc::db_writer::WriteBatch for WriteBatch {
    /// Go `writeBatch.Prewrite`.
    fn prewrite(&mut self, key: &[u8], lock: &Lock) {
        self.locks.push((key.to_vec(), Some(lock.marshal_binary())));
    }

    /// Go `writeBatch.Commit`.
    fn commit(&mut self, key: &[u8], lock: &Lock) {
        let user_meta = DbUserMeta::new(self.start_ts, self.commit_ts);
        if lock.hdr.op == KvrpcOp::PessimisticLock as i32 as u8 {
            // "Write nothing as if PessimisticRollback is called."
        } else if lock.hdr.op != KvrpcOp::Lock as i32 as u8 {
            self.db
                .push((key.to_vec(), self.commit_ts, lock.value.clone(), user_meta));
        } else if key == lock.primary.as_slice() {
            let op_lock_key = encode_extra_txn_status_key(key, self.start_ts);
            self.db.push((op_lock_key, self.start_ts, Vec::new(), user_meta));
        }
        self.locks.push((key.to_vec(), None));
    }

    /// Go `writeBatch.Rollback`.
    fn rollback(&mut self, key: &[u8], delete_lock: bool) {
        let rollback_key = encode_extra_txn_status_key(key, self.start_ts);
        self.db.push((
            rollback_key,
            self.start_ts,
            Vec::new(),
            DbUserMeta::new(self.start_ts, 0),
        ));
        if delete_lock {
            self.locks.push((key.to_vec(), None));
        }
    }

    /// Go `writeBatch.PessimisticLock`.
    fn pessimistic_lock(&mut self, key: &[u8], lock: &Lock) {
        self.locks.push((key.to_vec(), Some(lock.marshal_binary())));
    }

    /// Go `writeBatch.PessimisticRollback`.
    fn pessimistic_rollback(&mut self, key: &[u8]) {
        self.locks.push((key.to_vec(), None));
    }
}

/// An engine entry: `(key, version, value, user meta)`.
pub(crate) type DbEntry = (Vec<u8>, u64, Vec<u8>, DbUserMeta);
/// A lock entry: a put, or (`None`) a delete.
pub(crate) type LockEntry = (Vec<u8>, Option<Vec<u8>>);

/// Go `writeDBBatch` / `writeLockBatch` in flight: the entries and the
/// requester's `wg`.
struct Job<T> {
    entries: Vec<T>,
    done: mpsc::SyncSender<()>,
}

/// Go `dbWriter`: the channels to its two workers (Go `Open`), closed when
/// the store goes (Go `Close`).
#[derive(Debug)]
pub(crate) struct DbWriter {
    db_ch: Option<mpsc::Sender<Job<DbEntry>>>,
    lock_ch: Option<mpsc::Sender<Job<LockEntry>>>,
    workers: Vec<std::thread::JoinHandle<()>>,
}

impl<T> std::fmt::Debug for Job<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Job").field("entries", &self.entries.len()).finish()
    }
}

impl DbWriter {
    /// Go `NewDBWriter` + `Open`: starts `writeDBWorker` over `engine` and
    /// `writeLockWorker` over `lock_store`.
    pub(crate) fn open(engine: Arc<RwLock<MemEngine>>, lock_store: Arc<MemStore>) -> Self {
        let (db_ch, db_rx) = mpsc::channel::<Job<DbEntry>>();
        let (lock_ch, lock_rx) = mpsc::channel::<Job<LockEntry>>();
        let db_worker = std::thread::Builder::new()
            .name("unistore-write-db".to_owned())
            .spawn(move || {
                // Go `writeDBWorker.run` / `updateBatchGroup`.
                while let Some(mut group) = drain(&db_rx) {
                    {
                        let mut engine = engine.write().unwrap_or_else(PoisonError::into_inner);
                        for job in &mut group {
                            for (key, version, value, meta) in job.entries.drain(..) {
                                engine.insert(key, version, value, meta);
                            }
                        }
                    }
                    finish(group);
                }
            })
            .expect("spawn the unistore db writer");
        let lock_worker = std::thread::Builder::new()
            .name("unistore-write-lock".to_owned())
            .spawn(move || {
                // Go `writeLockWorker.run`: one hint over the group.
                while let Some(group) = drain(&lock_rx) {
                    {
                        let mut writer = lock_store.writer();
                        let mut hint = Hint::new();
                        for job in &group {
                            for (key, value) in &job.entries {
                                match value {
                                    Some(value) => writer.put_with_hint(key, value, &mut hint),
                                    // "Ignore if the key doesn't exist".
                                    None => writer.delete_with_hint(key, &mut hint),
                                };
                            }
                        }
                    }
                    finish(group);
                }
            })
            .expect("spawn the unistore lock writer");
        Self {
            db_ch: Some(db_ch),
            lock_ch: Some(lock_ch),
            workers: vec![db_worker, lock_worker],
        }
    }

    /// Go `dbWriter.Write`: the engine entries, then the lock entries -- "We
    /// must delete lock after commit succeed, or there will be
    /// inconsistency."
    pub(crate) fn write(&self, batch: WriteBatch) {
        if !batch.db.is_empty() {
            send_and_wait(self.db_ch.as_ref(), batch.db);
        }
        if !batch.locks.is_empty() {
            send_and_wait(self.lock_ch.as_ref(), batch.locks);
        }
    }

    /// Go `deleteKeysInBatch`'s `writer.dbCh <- dbBatch; dbBatch.wg.Wait()`.
    pub(crate) fn write_db(&self, entries: Vec<DbEntry>) {
        if !entries.is_empty() {
            send_and_wait(self.db_ch.as_ref(), entries);
        }
    }
}

impl Drop for DbWriter {
    /// Go `dbWriter.Close`: the workers stop and are waited for.
    fn drop(&mut self) {
        self.db_ch = None;
        self.lock_ch = None;
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

fn send_and_wait<T>(ch: Option<&mpsc::Sender<Job<T>>>, entries: Vec<T>) {
    let (done, wait) = mpsc::sync_channel(1);
    ch.expect("the writer is open")
        .send(Job { entries, done })
        .expect("the unistore writer is running");
    wait.recv().expect("the unistore writer applied the batch");
}

/// Go's `case batch := <-w.batchCh` then `for range len(w.batchCh)`: the
/// first batch and every batch queued behind it; `None` once closed.
fn drain<T>(rx: &mpsc::Receiver<Job<T>>) -> Option<Vec<Job<T>>> {
    let first = rx.recv().ok()?;
    let mut group = vec![first];
    while let Ok(job) = rx.try_recv() {
        group.push(job);
    }
    Some(group)
}

/// Go `batch.wg.Done()` for each batch of the group.
fn finish<T>(group: Vec<Job<T>>) {
    for job in group {
        let _ = job.done.send(());
    }
}
