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

//! Go `pkg/store/mockstore/unistore/tikv/mvcc.go` — the MVCC store, OPTIMISTIC
//! slice.
//!
//! SEED of `tikv` (`mvcc.go` is 2,182 lines): what lands here is the
//! optimistic-transaction core with Go's bodies — `prewriteOptimistic` with
//! its three-stage conflict protocol, `Commit` with the lock-not-found
//! recovery, `Rollback`'s two-phase status scan, and the read path's lock
//! check. What does not, each named at its refusal or absence:
//!
//! * the PESSIMISTIC path (`prewritePessimistic`, `PessimisticLock`,
//!   `PessimisticRollback`, TTL/minCommitTS updates, lock waiting and
//!   deadlock detection) — a later course; `Prewrite` REFUSES a request with
//!   `for_update_ts > 0` by name;
//! * async commit and 1PC (`req.UseAsyncCommit` / `TryOnePc`, which need
//!   `pdClient.GetTS`) — refused by name;
//! * `CheckTxnStatus` / `ResolveLock` / scans — later courses.
//!
//! # The engine
//!
//! Go's committed data lives in badger, keyed `y.KeyWithTs(key, version)`.
//! Badger is unported; [`MemEngine`] is an in-memory ordered map with the
//! same visibility contract — a read at `ts` sees the newest version whose
//! ts is `<= ts` — playing the role Go's test badger instances play. The
//! MVCC LOGIC above it is Go's; only the storage under it is substituted,
//! and the substitution is this module's one structural narrowing.
//!
//! # Concurrency narrowings, by name
//!
//! `regCtx.AcquireLatches` / `ReleaseLatches` (region latches),
//! `lockWaiterManager.WakeUp`, `DeadlockDetectCli.CleanUp`, and
//! `atomic.AddInt64(regCtx.Diff(), ..)` guard multi-writer interleavings and
//! pessimistic waiters. This slice is single-writer (`&mut self`), which the
//! borrow checker enforces more strongly than the latches do; the calls are
//! therefore absent rather than stubbed. They return with the pessimistic
//! course, whose semantics need them.
//!
//! `reqCtx.buf` (Go's per-request lock-buffer reuse) is an allocation
//! detail with no observable content.

use std::collections::BTreeMap;

use tidb_proto::{KvrpcLockInfo, KvrpcMutation, KvrpcOp};

use crate::lockstore::MemStore;
use crate::mvcc::{decode_lock, encode_extra_txn_status_key, DbUserMeta, Lock, LockHdr};

/// The version-keyed committed store standing in for badger. Keys are
/// `(user key, version)`, iterated newest-first per key exactly as badger's
/// `KeyWithTs` ordering yields them.
#[derive(Debug, Default)]
pub struct MemEngine {
    /// `(key, ts)` → `(value, user meta)`. The ts is stored INVERTED so the
    /// natural ascending order of the map walks newest-first, as badger's
    /// descending version encoding does.
    entries: BTreeMap<(Vec<u8>, u64), (Vec<u8>, DbUserMeta)>,
}

impl MemEngine {
    fn set(&mut self, key: &[u8], version: u64, value: &[u8], meta: DbUserMeta) {
        self.entries
            .insert((key.to_vec(), !version), (value.to_vec(), meta));
    }

    /// Badger's read contract: the newest version with `ts <= read_ts`.
    fn get_at(&self, key: &[u8], read_ts: u64) -> Option<(&[u8], &DbUserMeta)> {
        self.entries
            .range((key.to_vec(), !read_ts)..)
            .next()
            .filter(|((entry_key, _), _)| entry_key == key)
            .map(|(_, (value, meta))| (value.as_slice(), meta))
    }

    /// Go `checkCommitted`'s all-versions walk: every version of `key`,
    /// newest first.
    fn versions<'a>(&'a self, key: &'a [u8]) -> impl Iterator<Item = &'a DbUserMeta> + 'a {
        self.entries
            .range((key.to_vec(), 0)..)
            .take_while(move |((entry_key, _), _)| entry_key == key)
            .map(|(_, (_, meta))| meta)
    }
}

/// The `kverrors` subset the optimistic slice raises, each Go error by name.
#[derive(Clone, Debug, PartialEq)]
pub enum KvError {
    /// `kverrors.ErrAlreadyRollback`.
    AlreadyRollback,
    /// `kverrors.ErrLockNotFound`.
    LockNotFound,
    /// `kverrors.ErrReplaced`.
    Replaced,
    /// `kverrors.ErrCommitExpire`.
    CommitExpire {
        /// `StartTs`.
        start_ts: u64,
        /// `CommitTs`.
        commit_ts: u64,
        /// `MinCommitTs`.
        min_commit_ts: u64,
        /// `Key`.
        key: Vec<u8>,
    },
    /// `kverrors.ErrConflict` with `WriteConflict_Optimistic`.
    Conflict {
        /// `StartTS`.
        start_ts: u64,
        /// `ConflictTS` — the conflicting version's START ts, Go's choice.
        conflict_ts: u64,
        /// `ConflictCommitTS`.
        conflict_commit_ts: u64,
        /// `Key`.
        key: Vec<u8>,
    },
    /// `kverrors.ErrKeyAlreadyExists`.
    KeyAlreadyExists {
        /// `Key`.
        key: Vec<u8>,
    },
    /// `kverrors.ErrAlreadyCommitted(commitTS)`.
    AlreadyCommitted(u64),
    /// `kverrors.BuildLockErr(key, lock)`: the blocking lock, kvrpcpb-shaped.
    Locked(Box<KvrpcLockInfo>),
    /// A refusal: the named Go symbol is a later course of this port.
    Unported(&'static str),
}

/// The `PrewriteRequest` fields the optimistic path reads.
#[derive(Clone, Debug, Default)]
pub struct PrewriteReq {
    /// `Mutations`, any order; `sortPrewrite` orders them here.
    pub mutations: Vec<KvrpcMutation>,
    /// `PrimaryLock`.
    pub primary_lock: Vec<u8>,
    /// `StartVersion`.
    pub start_version: u64,
    /// `LockTtl`.
    pub lock_ttl: u64,
    /// `ForUpdateTs`: non-zero selects the pessimistic path, which REFUSES.
    pub for_update_ts: u64,
    /// `MinCommitTs`.
    pub min_commit_ts: u64,
    /// `UseAsyncCommit` — refused by name.
    pub use_async_commit: bool,
    /// `TryOnePc` — refused by name.
    pub try_one_pc: bool,
    /// `Secondaries`.
    pub secondaries: Vec<Vec<u8>>,
}

/// Go `MVCCStore`, optimistic slice: the ported lock skiplist beside the
/// substituted data engine.
#[derive(Debug)]
pub struct MvccStore {
    /// Go `lockStore` — the REAL ported `lockstore.MemStore`.
    pub lock_store: MemStore,
    /// The committed-data engine standing in for badger.
    pub engine: MemEngine,
}

impl Default for MvccStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MvccStore {
    /// A store over fresh stores, Go's `NewTestStore` shape minus the badger
    /// directories.
    #[must_use]
    pub fn new() -> Self {
        Self {
            lock_store: MemStore::new(1 << 20),
            engine: MemEngine::default(),
        }
    }

    /// Go `MVCCStore.Prewrite` (`mvcc.go:761`), optimistic arm only.
    pub fn prewrite(&mut self, req: &PrewriteReq) -> Result<(), KvError> {
        // Go `sortPrewrite`.
        let mut mutations = req.mutations.clone();
        mutations.sort_by(|a, b| a.key.cmp(&b.key));
        if req.for_update_ts > 0 {
            return Err(KvError::Unported(
                "prewritePessimistic (mvcc.go:849) is a later course of this port",
            ));
        }
        if req.use_async_commit || req.try_one_pc {
            return Err(KvError::Unported(
                "async commit / 1PC need pdClient.GetTS (prewriteMutations, mvcc.go)",
            ));
        }
        self.prewrite_optimistic(&mutations, req)
    }

    /// Go `prewriteOptimistic` (`mvcc.go:791`).
    fn prewrite_optimistic(
        &mut self,
        mutations: &[KvrpcMutation],
        req: &PrewriteReq,
    ) -> Result<(), KvError> {
        let start_ts = req.start_version;
        // Stage 1 — "Must check the LockStore first."
        for mutation in mutations {
            if self
                .check_conflict_in_lock_store(&mutation.key, start_ts)?
                .is_some()
            {
                // duplicated command
                return Ok(());
            }
            if mutation.key == req.primary_lock {
                let status = self.check_extra_txn_status(&mutation.key, start_ts);
                if status.is_rollback {
                    return Err(KvError::AlreadyRollback);
                }
                if status.is_op_lock_committed() {
                    return Ok(()); // duplicated command
                }
            }
        }
        // Stage 2 — the write-CF conflict check over the latest versions.
        let items: Vec<Option<(Vec<u8>, DbUserMeta)>> = mutations
            .iter()
            .map(|mutation| {
                self.engine
                    .get_at(&mutation.key, u64::MAX)
                    .map(|(value, meta)| (value.to_vec(), meta.clone()))
            })
            .collect();
        for (mutation, item) in mutations.iter().zip(&items) {
            if let Some((_, meta)) = item {
                if meta.commit_ts() > start_ts {
                    return Err(KvError::Conflict {
                        start_ts,
                        conflict_ts: meta.start_ts(),
                        conflict_commit_ts: meta.commit_ts(),
                        key: mutation.key.clone(),
                    });
                }
            }
            if mutation.op == KvrpcOp::CheckNotExists as i32 {
                if let Some((value, _)) = item {
                    if !value.is_empty() {
                        return Err(KvError::KeyAlreadyExists {
                            key: mutation.key.clone(),
                        });
                    }
                }
            }
        }
        // Stage 3 — Go `prewriteMutations`: build each lock, batch, write.
        for mutation in mutations {
            if mutation.op == KvrpcOp::CheckNotExists as i32 {
                continue;
            }
            let lock = build_prewrite_lock(mutation, req);
            // Go `writeBatch.Prewrite` (`write.go:252`).
            self.lock_store_put(&mutation.key, &lock.marshal_binary());
        }
        Ok(())
    }

    /// Go `checkConflictInLockStore` (`mvcc.go`): a same-ts lock is a
    /// duplicate, any other live lock is `BuildLockErr`.
    fn check_conflict_in_lock_store(
        &self,
        key: &[u8],
        start_ts: u64,
    ) -> Result<Option<Lock>, KvError> {
        let buf = self.lock_bytes(key);
        if buf.is_empty() {
            return Ok(None);
        }
        let lock = decode_lock(&buf);
        if lock.hdr.start_ts == start_ts {
            return Ok(Some(lock));
        }
        Err(KvError::Locked(Box::new(lock.to_lock_info(key.to_vec()))))
    }

    /// Go `checkExtraTxnStatus` (`mvcc.go`): the extra-status record for
    /// `(key, startTS)` — commit ts zero is a rollback record.
    fn check_extra_txn_status(&self, key: &[u8], start_ts: u64) -> ExtraTxnStatus {
        let status_key = encode_extra_txn_status_key(key, start_ts);
        match self.engine.get_at(&status_key, u64::MAX) {
            None => ExtraTxnStatus::default(),
            Some((_, meta)) if meta.commit_ts() == 0 => ExtraTxnStatus {
                is_rollback: true,
                commit_ts: 0,
            },
            Some((_, meta)) => ExtraTxnStatus {
                is_rollback: false,
                commit_ts: meta.commit_ts(),
            },
        }
    }

    /// Go `MVCCStore.Commit` (`mvcc.go:1232`).
    pub fn commit(
        &mut self,
        keys: &[Vec<u8>],
        start_ts: u64,
        commit_ts: u64,
    ) -> Result<(), KvError> {
        let mut keys = keys.to_vec();
        keys.sort();
        struct Pending {
            key: Vec<u8>,
            lock: Lock,
        }
        let mut pending = Vec::with_capacity(keys.len());
        for key in &keys {
            let buf = self.lock_bytes(key);
            let lock_err = if buf.is_empty() {
                Some(KvError::LockNotFound)
            } else {
                let lock = decode_lock(&buf);
                if lock.hdr.start_ts == start_ts {
                    pending.push(Pending {
                        key: key.clone(),
                        lock,
                    });
                    None
                } else {
                    Some(KvError::Replaced)
                }
            };
            if let Some(lock_err) = lock_err {
                // Go: maybe a lock resolver committed it concurrently.
                self.handle_lock_not_found(key, start_ts, commit_ts)
                    .map_err(|_| lock_err)?;
                continue;
            }
            let lock = &pending.last().expect("just pushed").lock;
            if commit_ts < lock.hdr.min_commit_ts {
                return Err(KvError::CommitExpire {
                    start_ts,
                    commit_ts,
                    min_commit_ts: lock.hdr.min_commit_ts,
                    key: key.clone(),
                });
            }
        }
        // Go `writeBatch.Commit` (`write.go:256`), applied per key.
        for Pending { key, lock } in pending {
            let meta = DbUserMeta::new(start_ts, commit_ts);
            if lock.hdr.op == KvrpcOp::PessimisticLock as i32 as u8 {
                // "Write nothing as if PessimisticRollback is called."
            } else if lock.hdr.op != KvrpcOp::Lock as i32 as u8 {
                self.engine.set(&key, commit_ts, &lock.value, meta);
            } else if key == lock.primary {
                let status_key = encode_extra_txn_status_key(&key, start_ts);
                self.engine.set(&status_key, start_ts, &[], meta);
            }
            self.lock_store_delete(&key);
        }
        Ok(())
    }

    /// Go `handleLockNotFound`: at `commitTS`, a version whose start ts is
    /// ours means someone already committed us; anything else is the original
    /// lock error.
    fn handle_lock_not_found(
        &self,
        key: &[u8],
        start_ts: u64,
        commit_ts: u64,
    ) -> Result<(), KvError> {
        match self.engine.get_at(key, commit_ts) {
            Some((_, meta)) if meta.start_ts() == start_ts => Ok(()),
            _ => Err(KvError::LockNotFound),
        }
    }

    /// Go `MVCCStore.Rollback` (`mvcc.go:1320`): the two-phase status scan.
    pub fn rollback(&mut self, keys: &[Vec<u8>], start_ts: u64) -> Result<(), KvError> {
        let mut keys = keys.to_vec();
        keys.sort();
        let mut statuses = Vec::with_capacity(keys.len());
        let mut batch: Vec<RollbackAction> = Vec::new();
        for key in &keys {
            statuses.push(self.rollback_key_read_lock(&mut batch, key, start_ts));
        }
        for (key, status) in keys.iter().zip(&statuses) {
            match status {
                RollbackStatus::Done => {}
                RollbackStatus::NoLock | RollbackStatus::NewLock => {
                    // Go `rollbackKeyReadDB` via `checkCommitted`.
                    if let Some(commit_ts) = self.committed_version_of(key, start_ts) {
                        return Err(KvError::AlreadyCommitted(commit_ts));
                    }
                    batch.push(RollbackAction {
                        key: key.clone(),
                        delete_lock: false,
                    });
                }
            }
        }
        // Go `writeBatch.Rollback` (`write.go:271`), applied per action.
        for action in batch {
            let status_key = encode_extra_txn_status_key(&action.key, start_ts);
            self.engine
                .set(&status_key, start_ts, &[], DbUserMeta::new(start_ts, 0));
            if action.delete_lock {
                self.lock_store_delete(&action.key);
            }
        }
        Ok(())
    }

    /// Go `rollbackKeyReadLock` with `currentTs == 0`, the `Rollback` call
    /// shape. (The non-zero `currentTs` TTL arm belongs to `CheckTxnStatus`,
    /// a later course.)
    fn rollback_key_read_lock(
        &mut self,
        batch: &mut Vec<RollbackAction>,
        key: &[u8],
        start_ts: u64,
    ) -> RollbackStatus {
        let buf = self.lock_bytes(key);
        if buf.is_empty() {
            return RollbackStatus::NoLock;
        }
        let lock = decode_lock(&buf);
        if lock.hdr.start_ts < start_ts {
            // An OLD transaction's lock: ours may still arrive; write a
            // rollback record but leave the lock.
            batch.push(RollbackAction {
                key: key.to_vec(),
                delete_lock: false,
            });
            return RollbackStatus::Done;
        }
        if lock.hdr.start_ts == start_ts {
            // "We can not simply delete the lock because the prewrite may be
            // sent multiple times" — a rollback record AND the lock's death.
            batch.push(RollbackAction {
                key: key.to_vec(),
                delete_lock: true,
            });
            return RollbackStatus::Done;
        }
        RollbackStatus::NewLock
    }

    /// Go `checkCommitted`: the version whose START ts is the transaction's,
    /// if any, walking all versions.
    fn committed_version_of(&self, key: &[u8], start_ts: u64) -> Option<u64> {
        self.engine
            .versions(key)
            .find(|meta| meta.start_ts() == start_ts)
            .map(super::mvcc::DbUserMeta::commit_ts)
    }

    /// Go `MVCCStore.Get` → `GetPair` → `dbreader.Get` (`mvcc.go:1817`),
    /// snapshot-isolation arm with empty resolved/committed lists (the
    /// `ResolvedLocks` / `CommittedLocks` narrowing, named).
    pub fn get(&self, key: &[u8], version: u64) -> Result<Option<Vec<u8>>, KvError> {
        // Go `CheckKeysLock`: a live lock visible at the read version blocks
        // the read, unless it is a Lock-op or pessimistic lock.
        let buf = self.lock_bytes(key);
        if !buf.is_empty() {
            let lock = decode_lock(&buf);
            let invisible = lock.hdr.op == KvrpcOp::Lock as i32 as u8
                || lock.hdr.op == KvrpcOp::PessimisticLock as i32 as u8;
            if !invisible && lock.hdr.start_ts <= version {
                return Err(KvError::Locked(Box::new(lock.to_lock_info(key.to_vec()))));
            }
        }
        // Go `dbreader.Get(key, version)`: the newest committed version at or
        // below the read ts; an empty value is a delete, read as absent.
        Ok(self
            .engine
            .get_at(key, version)
            .map(|(value, _)| value.to_vec())
            .filter(|value| !value.is_empty()))
    }

    /// Go `store.lockStore.Get(key, buf)`: the lock bytes, empty when no
    /// lock — Go's `len(buf) == 0` test, kept as the emptiness of the copy.
    fn lock_bytes(&self, key: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        self.lock_store.get(key, &mut buf);
        buf
    }

    fn lock_store_put(&mut self, key: &[u8], value: &[u8]) {
        // Go `MemStore.Put` inserts or replaces; the ported `put` does both.
        self.lock_store.put(key, value);
    }

    fn lock_store_delete(&mut self, key: &[u8]) {
        self.lock_store.delete(key);
    }
}

/// Go `extraTxnStatus`.
#[derive(Clone, Copy, Debug, Default)]
struct ExtraTxnStatus {
    is_rollback: bool,
    commit_ts: u64,
}

impl ExtraTxnStatus {
    /// Go `isOpLockCommitted`: a committed Op_Lock leaves a status record
    /// with a non-zero commit ts.
    fn is_op_lock_committed(self) -> bool {
        self.commit_ts > 0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RollbackStatus {
    Done,
    NoLock,
    NewLock,
}

struct RollbackAction {
    key: Vec<u8>,
    delete_lock: bool,
}

/// Go `buildPrewriteLock` (`mvcc.go`), assertion arms excluded — assertions
/// arrive with the pessimistic course. Named narrowing:
/// `req.AssertionLevel` / `kverrors.ErrAssertionFailed`.
fn build_prewrite_lock(mutation: &KvrpcMutation, req: &PrewriteReq) -> Lock {
    Lock {
        hdr: LockHdr {
            start_ts: req.start_version,
            ttl: u32::try_from(req.lock_ttl).unwrap_or(u32::MAX),
            primary_len: u16::try_from(req.primary_lock.len())
                .expect("primary key fits u16, as Go's cast assumes"),
            min_commit_ts: req.min_commit_ts,
            use_async_commit: req.use_async_commit,
            secondary_num: u32::try_from(req.secondaries.len()).expect("secondary count fits u32"),
            op: u8::try_from(mutation.op).expect("an op byte"),
            for_update_ts: 0,
            has_old_ver: false,
        },
        primary: req.primary_lock.clone(),
        value: mutation.value.clone(),
        secondaries: req.secondaries.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // `TestBasicOptimistic` is TRANSCREATED from `tikv/mvcc_test.go:518`;
    // the rest are WRITTEN over the ported bodies — the upstream suite's
    // other optimistic tests ride pessimistic and CheckTxnStatus surfaces
    // that are later courses, and each test names the Go behavior it pins.

    fn put(key: &[u8], value: &[u8]) -> KvrpcMutation {
        KvrpcMutation {
            op: KvrpcOp::Put as i32,
            key: key.to_vec(),
            value: value.to_vec(),
            ..KvrpcMutation::default()
        }
    }

    fn must_prewrite_optimistic(
        store: &mut MvccStore,
        pk: &[u8],
        key: &[u8],
        value: &[u8],
        start_ts: u64,
        ttl: u64,
    ) {
        store
            .prewrite(&PrewriteReq {
                mutations: vec![put(key, value)],
                primary_lock: pk.to_vec(),
                start_version: start_ts,
                lock_ttl: ttl,
                ..PrewriteReq::default()
            })
            .expect("prewrite succeeds");
    }

    #[test]
    fn test_basic_optimistic() {
        // Go `TestBasicOptimistic` (`mvcc_test.go:518`): prewrite, commit at
        // 2, and a read at ts 1 sees NOTHING.
        let mut store = MvccStore::new();
        let (key1, val1) = (b"key1".as_slice(), b"val1".as_slice());
        must_prewrite_optimistic(&mut store, key1, key1, val1, 1, 200);
        store.commit(&[key1.to_vec()], 1, 2).expect("commits");
        assert_eq!(store.get(key1, 1).expect("no lock"), None);
        assert_eq!(store.get(key1, 2).expect("no lock"), Some(val1.to_vec()));
    }

    #[test]
    fn a_conflicting_commit_raises_gos_error_fields() {
        // `prewriteOptimistic`: a committed version NEWER than our start ts
        // is `ErrConflict{StartTS, ConflictTS: their START, ConflictCommitTS}`.
        let mut store = MvccStore::new();
        let key = b"k".as_slice();
        must_prewrite_optimistic(&mut store, key, key, b"v1", 10, 200);
        store.commit(&[key.to_vec()], 10, 20).expect("commits");
        let err = store
            .prewrite(&PrewriteReq {
                mutations: vec![put(key, b"v2")],
                primary_lock: key.to_vec(),
                start_version: 15,
                ..PrewriteReq::default()
            })
            .expect_err("write conflict");
        assert_eq!(
            err,
            KvError::Conflict {
                start_ts: 15,
                conflict_ts: 10,
                conflict_commit_ts: 20,
                key: key.to_vec(),
            }
        );
    }

    #[test]
    fn a_duplicated_prewrite_is_a_silent_ok() {
        // `checkConflictInLockStore`: a same-ts lock returns nil — the
        // command is a retry, not a conflict.
        let mut store = MvccStore::new();
        let key = b"dup".as_slice();
        must_prewrite_optimistic(&mut store, key, key, b"v", 5, 200);
        must_prewrite_optimistic(&mut store, key, key, b"v", 5, 200);
    }

    #[test]
    fn anothers_lock_blocks_prewrite_and_read() {
        let mut store = MvccStore::new();
        let key = b"locked".as_slice();
        must_prewrite_optimistic(&mut store, key, key, b"v", 7, 200);
        let err = store
            .prewrite(&PrewriteReq {
                mutations: vec![put(key, b"w")],
                primary_lock: key.to_vec(),
                start_version: 9,
                ..PrewriteReq::default()
            })
            .expect_err("their lock blocks us");
        let KvError::Locked(info) = err else {
            panic!("expected the lock, got {err:?}")
        };
        assert_eq!(info.lock_version, 7);
        // `CheckKeysLock`: a Put lock visible at the read version blocks a
        // read at or above its start ts — and not below.
        assert!(store.get(key, 8).is_err());
        assert!(store.get(key, 6).expect("below the lock").is_none());
    }

    #[test]
    fn commit_without_a_lock_recovers_only_an_already_committed_txn() {
        // `Commit` → `handleLockNotFound`: the committed-version probe
        // rescues a lock-resolver race; anything else is ErrLockNotFound.
        let mut store = MvccStore::new();
        let key = b"k".as_slice();
        must_prewrite_optimistic(&mut store, key, key, b"v", 3, 200);
        store.commit(&[key.to_vec()], 3, 4).expect("commits");
        // Re-committing the same txn: lock is gone, but the version with our
        // start ts exists — silently ok, Go's "already committed".
        store.commit(&[key.to_vec()], 3, 4).expect("idempotent");
        // A never-prewritten txn has nothing to recover.
        assert_eq!(
            store.commit(&[key.to_vec()], 99, 100),
            Err(KvError::LockNotFound)
        );
    }

    #[test]
    fn asking_to_commit_someone_elses_lock_is_replaced() {
        let mut store = MvccStore::new();
        let key = b"k".as_slice();
        must_prewrite_optimistic(&mut store, key, key, b"v", 30, 200);
        assert_eq!(store.commit(&[key.to_vec()], 8, 9), Err(KvError::Replaced));
    }

    #[test]
    fn commit_below_min_commit_ts_expires() {
        let mut store = MvccStore::new();
        let key = b"k".as_slice();
        store
            .prewrite(&PrewriteReq {
                mutations: vec![put(key, b"v")],
                primary_lock: key.to_vec(),
                start_version: 40,
                min_commit_ts: 50,
                ..PrewriteReq::default()
            })
            .expect("prewrites");
        assert_eq!(
            store.commit(&[key.to_vec()], 40, 45),
            Err(KvError::CommitExpire {
                start_ts: 40,
                commit_ts: 45,
                min_commit_ts: 50,
                key: key.to_vec(),
            })
        );
    }

    #[test]
    fn rollback_then_prewrite_is_already_rollback() {
        // `Rollback` writes the extra-status record; a later prewrite of the
        // PRIMARY finds it via `checkExtraTxnStatus` and refuses.
        let mut store = MvccStore::new();
        let key = b"k".as_slice();
        store
            .rollback(&[key.to_vec()], 60)
            .expect("rollback of nothing");
        let err = store
            .prewrite(&PrewriteReq {
                mutations: vec![put(key, b"v")],
                primary_lock: key.to_vec(),
                start_version: 60,
                ..PrewriteReq::default()
            })
            .expect_err("the rollback record wins");
        assert_eq!(err, KvError::AlreadyRollback);
    }

    #[test]
    fn rollback_of_our_own_lock_kills_it_and_leaves_a_record() {
        let mut store = MvccStore::new();
        let key = b"k".as_slice();
        must_prewrite_optimistic(&mut store, key, key, b"v", 70, 200);
        store.rollback(&[key.to_vec()], 70).expect("rolls back");
        assert!(store.lock_bytes(key).is_empty(), "the lock died");
        // And a re-sent prewrite of the same txn refuses.
        assert_eq!(
            store
                .prewrite(&PrewriteReq {
                    mutations: vec![put(key, b"v")],
                    primary_lock: key.to_vec(),
                    start_version: 70,
                    ..PrewriteReq::default()
                })
                .expect_err("refused"),
            KvError::AlreadyRollback
        );
    }

    #[test]
    fn rollback_of_a_committed_txn_reports_the_commit() {
        let mut store = MvccStore::new();
        let key = b"k".as_slice();
        must_prewrite_optimistic(&mut store, key, key, b"v", 80, 200);
        store.commit(&[key.to_vec()], 80, 81).expect("commits");
        assert_eq!(
            store.rollback(&[key.to_vec()], 80),
            Err(KvError::AlreadyCommitted(81))
        );
    }

    #[test]
    fn an_old_lock_survives_a_newer_txns_rollback() {
        // `rollbackKeyReadLock`: `lock.StartTS < startTS` writes OUR rollback
        // record but leaves THEIR lock — their prewrite may still commit.
        let mut store = MvccStore::new();
        let key = b"k".as_slice();
        must_prewrite_optimistic(&mut store, key, key, b"v", 90, 200);
        store.rollback(&[key.to_vec()], 95).expect("rolls back 95");
        assert!(!store.lock_bytes(key).is_empty(), "the old lock lives");
        store
            .commit(&[key.to_vec()], 90, 96)
            .expect("their commit lands");
        assert_eq!(store.get(key, 96).expect("v"), Some(b"v".to_vec()));
    }

    #[test]
    fn check_not_exists_sees_committed_values_and_deletions() {
        // `Op_CheckNotExists` over a committed value is `ErrKeyAlreadyExists`;
        // over a committed DELETE (empty value) it passes.
        let mut store = MvccStore::new();
        let key = b"k".as_slice();
        must_prewrite_optimistic(&mut store, key, key, b"v", 100, 200);
        store.commit(&[key.to_vec()], 100, 101).expect("commits");
        let check = KvrpcMutation {
            op: KvrpcOp::CheckNotExists as i32,
            key: key.to_vec(),
            ..KvrpcMutation::default()
        };
        assert_eq!(
            store
                .prewrite(&PrewriteReq {
                    mutations: vec![check.clone()],
                    primary_lock: key.to_vec(),
                    start_version: 110,
                    ..PrewriteReq::default()
                })
                .expect_err("exists"),
            KvError::KeyAlreadyExists { key: key.to_vec() }
        );
        // Delete it, then the check passes — and adds NO lock.
        store
            .prewrite(&PrewriteReq {
                mutations: vec![KvrpcMutation {
                    op: KvrpcOp::Del as i32,
                    key: key.to_vec(),
                    ..KvrpcMutation::default()
                }],
                primary_lock: key.to_vec(),
                start_version: 120,
                ..PrewriteReq::default()
            })
            .expect("delete prewrites");
        store.commit(&[key.to_vec()], 120, 121).expect("commits");
        store
            .prewrite(&PrewriteReq {
                mutations: vec![check],
                primary_lock: key.to_vec(),
                start_version: 130,
                ..PrewriteReq::default()
            })
            .expect("gone now");
        assert!(
            store.lock_bytes(key).is_empty(),
            "CheckNotExists adds no lock"
        );
    }

    #[test]
    fn the_pessimistic_and_async_paths_refuse_by_name() {
        let mut store = MvccStore::new();
        let refuse = store.prewrite(&PrewriteReq {
            for_update_ts: 5,
            ..PrewriteReq::default()
        });
        assert!(matches!(refuse, Err(KvError::Unported(_))));
        let refuse = store.prewrite(&PrewriteReq {
            use_async_commit: true,
            ..PrewriteReq::default()
        });
        assert!(matches!(refuse, Err(KvError::Unported(_))));
    }
}
