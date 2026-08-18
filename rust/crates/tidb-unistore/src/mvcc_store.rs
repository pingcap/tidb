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

use tidb_proto::{KvrpcLockInfo, KvrpcMutation, KvrpcOp, KvrpcTxnAction};

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
    /// `kverrors.ErrConflict`, carrying Go's `kvrpcpb.WriteConflict` reason.
    Conflict {
        /// `Reason`, the proto's `write_conflict::Reason`.
        reason: tidb_proto::kvrpcpb::write_conflict::Reason,
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
    /// `kverrors.ErrAssertionFailed`.
    AssertionFailed {
        /// `StartTS`.
        start_ts: u64,
        /// `Key`.
        key: Vec<u8>,
        /// `Assertion`, the `kvrpcpb.Assertion` value that failed.
        assertion: i32,
        /// `ExistingStartTS` — zero when the key does not exist.
        existing_start_ts: u64,
        /// `ExistingCommitTS` — zero when the key does not exist.
        existing_commit_ts: u64,
    },
    /// `kverrors.ErrAlreadyCommitted(commitTS)`.
    AlreadyCommitted(u64),
    /// `kverrors.BuildLockErr(key, lock)`: the blocking lock, kvrpcpb-shaped.
    Locked(Box<KvrpcLockInfo>),
    /// `kverrors.ErrPrimaryMismatch`: the checked key carries a lock whose
    /// primary is a DIFFERENT key.
    PrimaryMismatch {
        /// `Key`.
        key: Vec<u8>,
        /// `Lock`, kvrpcpb-shaped.
        lock: Box<KvrpcLockInfo>,
    },
    /// `kverrors.ErrTxnNotFound`.
    TxnNotFound {
        /// `PrimaryKey`.
        primary_key: Vec<u8>,
        /// `StartTS`.
        start_ts: u64,
    },
    /// Go `kverrors.ErrInvalidOp`: `Op_CheckNotExists` inside a pessimistic
    /// prewrite.
    InvalidOp,
    /// Go's literal `errors.New("pessimistic lock not found")`.
    PessimisticLockNotFound,
    /// Go's literal `errors.New("lock type not match")`.
    LockTypeNotMatch,
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
    /// `MaxCommitTs`: an async/1PC `minCommitTS` past this falls back to
    /// sync commit.
    pub max_commit_ts: u64,
    /// `UseAsyncCommit` — refused by name.
    pub use_async_commit: bool,
    /// `TryOnePc` — refused by name.
    pub try_one_pc: bool,
    /// `Secondaries`.
    pub secondaries: Vec<Vec<u8>>,
    /// `PessimisticActions`, positionally matching `mutations`; empty means
    /// none. Values are Go's `PrewriteRequest_DO_PESSIMISTIC_CHECK` /
    /// `DO_CONSTRAINT_CHECK` / `SKIP_PESSIMISTIC_CHECK`.
    pub pessimistic_actions: Vec<tidb_proto::KvrpcPessimisticAction>,
    /// `ForUpdateTsConstraints`: `(index, expected_for_update_ts)` pairs.
    pub for_update_ts_constraints: Vec<(usize, u64)>,
    /// `AssertionLevel`: any non-`Off` value makes prewrite verify each
    /// mutation's `Assertion` against the latest committed version. Go's
    /// unistore deliberately checks even at `Fast` level (its comment: don't
    /// assume the store skips), so only `Off` disables.
    pub assertion_level: i32,
}

/// Go `MVCCStore`, optimistic slice: the ported lock skiplist beside the
/// substituted data engine.
#[derive(Debug)]
pub struct MvccStore {
    /// Go `lockStore` — the REAL ported `lockstore.MemStore`.
    pub lock_store: MemStore,
    /// The committed-data engine standing in for badger.
    pub engine: MemEngine,
    /// Go `pdClient`, narrowed to the one call the store makes — `GetTS`
    /// for the async-commit / 1PC `minCommitTS`. Absent, those protocols
    /// refuse by name, exactly as before a PD is wired.
    pub pd: Option<std::sync::Arc<crate::tso::Tso>>,
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
            pd: None,
        }
    }

    /// A store with Go's `pdClient` seam filled: async-commit prewrites can
    /// draw their `minCommitTS` from this oracle.
    #[must_use]
    pub fn with_pd(pd: std::sync::Arc<crate::tso::Tso>) -> Self {
        Self {
            pd: Some(pd),
            ..Self::new()
        }
    }

    /// Go `MVCCStore.Prewrite` (`mvcc.go:761`), optimistic arm only.
    pub fn prewrite(&mut self, req: &PrewriteReq) -> Result<(), KvError> {
        // Go `sortPrewrite`.
        let mut mutations = req.mutations.clone();
        mutations.sort_by(|a, b| a.key.cmp(&b.key));
        if req.for_update_ts > 0 {
            return self.prewrite_pessimistic(&mutations, req);
        }
        self.prewrite_optimistic(&mutations, req)
    }

    /// Go `prewriteMutations`' head (`mvcc.go:935-956`): async commit and
    /// 1PC draw a `minCommitTS` from PD after the keys are decided — one
    /// past `MaxCommitTs` falls back to sync commit; otherwise it raises
    /// the request's `MinCommitTs`. 1PC's commit arm (`tryOnePC`) is a
    /// named narrowing: it needs the dbWriter's atomic commit.
    fn effective_prewrite_req(&self, req: &PrewriteReq) -> Result<PrewriteReq, KvError> {
        if !(req.use_async_commit || req.try_one_pc) {
            return Ok(req.clone());
        }
        let Some(pd) = &self.pd else {
            return Err(KvError::Unported(
                "async commit / 1PC need pdClient.GetTS (prewriteMutations, mvcc.go)",
            ));
        };
        let mut req = req.clone();
        let (physical, logical) = pd.get_ts();
        let min_commit_ts = crate::tso::compose_ts(physical, logical);
        if req.max_commit_ts > 0 && min_commit_ts > req.max_commit_ts {
            req.use_async_commit = false;
            req.try_one_pc = false;
        }
        if req.use_async_commit && min_commit_ts > req.min_commit_ts {
            req.min_commit_ts = min_commit_ts;
        }
        if req.try_one_pc {
            return Err(KvError::Unported(
                "1PC needs tryOnePC's atomic dbWriter commit (mvcc.go:1073)",
            ));
        }
        Ok(req)
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
                        reason: tidb_proto::kvrpcpb::write_conflict::Reason::Optimistic,
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
        let req = self.effective_prewrite_req(req)?;
        for (mutation, item) in mutations.iter().zip(&items) {
            if mutation.op == KvrpcOp::CheckNotExists as i32 {
                continue;
            }
            let lock = build_prewrite_lock(mutation, item.as_ref(), &req)?;
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

    /// Go `MVCCStore.Cleanup` (`mvcc.go:1564`): rollback of ONE key, except
    /// an unexpired lock refuses instead of dying — the TTL arm
    /// `rollbackKeyReadLock` takes only when `currentTs > 0`. Physical time
    /// is `ts >> 18`, Go's `oracle.ExtractPhysical`.
    pub fn cleanup(&mut self, key: &[u8], start_ts: u64, current_ts: u64) -> Result<(), KvError> {
        let buf = self.lock_bytes(key);
        let mut delete_lock = false;
        let mut check_db = buf.is_empty();
        if !buf.is_empty() {
            let lock = decode_lock(&buf);
            if lock.hdr.start_ts == start_ts {
                if current_ts > 0
                    && (start_ts >> 18) + u64::from(lock.hdr.ttl) >= (current_ts >> 18)
                {
                    return Err(KvError::Locked(Box::new(lock.to_lock_info(key.to_vec()))));
                }
                delete_lock = true;
            } else if lock.hdr.start_ts > start_ts {
                // A NEWER transaction's lock: the DB decides, exactly as
                // `rollbackStatusNewLock` sends rollback to `rollbackKeyReadDB`.
                check_db = true;
            }
            // An OLDER lock: write the rollback record and leave it standing,
            // Go's `lock.StartTS < startTS` arm.
        }
        if check_db {
            if let Some(commit_ts) = self.committed_version_of(key, start_ts) {
                return Err(KvError::AlreadyCommitted(commit_ts));
            }
            let status = self.check_extra_txn_status(key, start_ts);
            if status.is_op_lock_committed() {
                return Err(KvError::AlreadyCommitted(status.commit_ts));
            }
        }
        let status_key = encode_extra_txn_status_key(key, start_ts);
        self.engine
            .set(&status_key, start_ts, &[], DbUserMeta::new(start_ts, 0));
        if delete_lock {
            self.lock_store_delete(key);
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

    /// Go `MVCCStore.Get` at the default read context: snapshot isolation
    /// with empty resolved/committed lists.
    pub fn get(&self, key: &[u8], version: u64) -> Result<Option<Vec<u8>>, KvError> {
        self.get_with(&ReadContext::default(), key, version)
    }

    /// Go `MVCCStore.Get` → `GetPair` → `dbreader.Get` (`mvcc.go:1817`).
    /// Under snapshot isolation a COMMITTED lock (its start ts in the
    /// context's committed set) answers with the lock's own value — nil for
    /// a delete; `RCCheckTS` turns any unresolved write lock into a
    /// conflict; `RC` skips lock checks entirely and reads committed data.
    pub fn get_with(
        &self,
        ctx: &ReadContext,
        key: &[u8],
        version: u64,
    ) -> Result<Option<Vec<u8>>, KvError> {
        if ctx.is_snapshot_isolation() {
            let buf = self.lock_bytes(key);
            if !buf.is_empty() {
                if let Some(lock) = check_lock(
                    decode_lock(&buf),
                    key,
                    version,
                    &ctx.resolved_locks,
                    &ctx.committed_locks,
                )? {
                    return Ok(value_from_lock(&lock).map(<[u8]>::to_vec));
                }
            }
        } else if ctx.is_rc_check_ts() {
            let buf = self.lock_bytes(key);
            if !buf.is_empty() {
                check_lock_rc_check_ts(&decode_lock(&buf), key, version, &ctx.resolved_locks)?;
            }
        }
        // Go `dbreader.Get`: under `RcCheckTS` the read ts becomes
        // `math.MaxUint64` — the LATEST version is read, then
        // `CheckWriteItemForRcCheckTSRead` refuses one committed past the
        // caller's ts.
        if ctx.is_rc_check_ts() {
            return match self.engine.get_at(key, u64::MAX) {
                None => Ok(None),
                Some((value, meta)) => {
                    check_write_meta_rc_check_ts(version, meta)?;
                    Ok(Some(value.to_vec()).filter(|value| !value.is_empty()))
                }
            };
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

/// Go `InternalKeyPrefix` (`region.go:44`): scans never cross into the
/// store's internal keyspace.
pub const INTERNAL_KEY_PREFIX: &[u8] = &[0xff];

/// One `kvrpcpb.KvPair` reduced to what this slice produces: a key with a
/// value, or a key with the blocking lock as its error.
#[derive(Clone, Debug, PartialEq)]
pub struct KvPair {
    /// The key.
    pub key: Vec<u8>,
    /// The committed value; empty when `error` carries the answer.
    pub value: Vec<u8>,
    /// Go `Error *kvrpcpb.KeyError`, carried as the store error it converts
    /// from — a blocking lock under SI, an `RcCheckTs` conflict otherwise.
    pub error: Option<Box<KvError>>,
}

/// The `ScanRequest` fields Go's `Scan` reads.
#[derive(Clone, Debug, Default)]
pub struct ScanReq {
    /// `StartKey`.
    pub start_key: Vec<u8>,
    /// `EndKey`; empty falls to [`INTERNAL_KEY_PREFIX`] (the region-bound
    /// fallback is a narrowing: no region context in this slice).
    pub end_key: Vec<u8>,
    /// `Limit`.
    pub limit: u32,
    /// `Version`.
    pub version: u64,
    /// `SampleStep`.
    pub sample_step: u32,
    /// `Reverse`.
    pub reverse: bool,
}

/// Go `requestCtx.rpcCtx`, the slice the read paths consult: the isolation
/// level and the two timestamp sets `checkLock` switches on.
#[derive(Clone, Debug, Default)]
pub struct ReadContext {
    /// `IsolationLevel`, the `kvrpcpb` value: 0 SI, 1 RC, 2 RCCheckTS.
    pub isolation_level: i32,
    /// `ResolvedLocks`: locks with these start timestamps are IGNORED —
    /// the reader has proof they were rolled back or committed elsewhere.
    pub resolved_locks: Vec<u64>,
    /// `CommittedLocks`: locks with these start timestamps answer AS IF
    /// committed — the lock's own value serves the read.
    pub committed_locks: Vec<u64>,
}

impl ReadContext {
    /// Go `reqCtx.isSnapshotIsolation()`.
    fn is_snapshot_isolation(&self) -> bool {
        self.isolation_level == tidb_proto::KvrpcIsolationLevel::Si as i32
    }

    /// Go `reqCtx.isRcCheckTSIsolationLevel()`.
    fn is_rc_check_ts(&self) -> bool {
        self.isolation_level == tidb_proto::KvrpcIsolationLevel::RcCheckTs as i32
    }
}

/// Go `getValueFromLock` (`mvcc.go:1428`): only a Put lock owns a value; a
/// delete lock answers nil.
fn value_from_lock(lock: &Lock) -> Option<&[u8]> {
    (lock.hdr.op == KvrpcOp::Put as i32 as u8).then_some(lock.value.as_slice())
}

/// Go `checkLock` (`mvcc.go:1437`), whole: a resolved lock is ignored; a
/// visible Put/Del lock either answers as a COMMITTED lock (`Ok(Some)`,
/// when its start ts is in the committed set) or blocks; a read at
/// `maxSystemTS` (u64::MAX) SKIPS the PRIMARY lock, non-async-commit only,
/// so a latest-committed point read serves past its own transaction's
/// primary while a secondary still errors.
fn check_lock(
    lock: Lock,
    key: &[u8],
    start_ts: u64,
    resolved: &[u64],
    committed: &[u64],
) -> Result<Option<Lock>, KvError> {
    if resolved.contains(&lock.hdr.start_ts) {
        return Ok(None);
    }
    let lock_visible = lock.hdr.start_ts <= start_ts;
    let is_write_lock =
        lock.hdr.op == KvrpcOp::Put as i32 as u8 || lock.hdr.op == KvrpcOp::Del as i32 as u8;
    let is_primary_get = start_ts == u64::MAX && lock.primary == key && !lock.hdr.use_async_commit;
    if lock_visible && is_write_lock && !is_primary_get {
        if committed.contains(&lock.hdr.start_ts) {
            return Ok(Some(lock));
        }
        return Err(KvError::Locked(Box::new(lock.to_lock_info(key.to_vec()))));
    }
    Ok(None)
}

/// Go `dbreader.CheckWriteItemForRcCheckTSRead` (`db_reader.go:362`): the
/// reader serves the LATEST version under `RcCheckTS`, and one committed
/// past the caller's read ts is an `RcCheckTs` conflict — carrying NO key,
/// exactly Go's error shape.
fn check_write_meta_rc_check_ts(read_ts: u64, meta: &DbUserMeta) -> Result<(), KvError> {
    if meta.commit_ts() > read_ts {
        return Err(KvError::Conflict {
            reason: tidb_proto::kvrpcpb::write_conflict::Reason::RcCheckTs,
            start_ts: read_ts,
            conflict_ts: meta.start_ts(),
            conflict_commit_ts: meta.commit_ts(),
            key: Vec::new(),
        });
    }
    Ok(())
}

/// Go `checkLockForRcCheckTS` (`mvcc.go:1453`): any unresolved write lock —
/// visible or not — is an `RcCheckTs` write conflict.
fn check_lock_rc_check_ts(
    lock: &Lock,
    key: &[u8],
    start_ts: u64,
    resolved: &[u64],
) -> Result<(), KvError> {
    if resolved.contains(&lock.hdr.start_ts) {
        return Ok(());
    }
    let is_write_lock =
        lock.hdr.op == KvrpcOp::Put as i32 as u8 || lock.hdr.op == KvrpcOp::Del as i32 as u8;
    if !is_write_lock {
        return Ok(());
    }
    Err(KvError::Conflict {
        reason: tidb_proto::kvrpcpb::write_conflict::Reason::RcCheckTs,
        start_ts,
        conflict_ts: lock.hdr.start_ts,
        conflict_commit_ts: 0,
        key: key.to_vec(),
    })
}

impl MvccStore {
    /// Go `MVCCStore.BatchGet` at the default read context.
    #[must_use]
    pub fn batch_get(&self, keys: &[Vec<u8>], version: u64) -> Vec<KvPair> {
        self.batch_get_with(&ReadContext::default(), keys, version)
    }

    /// Go `MVCCStore.BatchGet` (`mvcc.go:1866`): under SI each key first
    /// meets the lock check — a blocked key becomes an ERROR PAIR (the
    /// batch never fails as a whole), a COMMITTED lock answers with the
    /// lock's value (a delete's nil value drops the pair), and the rest
    /// fall to the data read; `RCCheckTS` turns conflicts into error pairs;
    /// `RC` reads data only. Data pairs with empty values are dropped,
    /// Go's `len(value) != 0` guard.
    #[must_use]
    pub fn batch_get_with(&self, ctx: &ReadContext, keys: &[Vec<u8>], version: u64) -> Vec<KvPair> {
        let mut pairs = Vec::with_capacity(keys.len());
        let mut remain = Vec::with_capacity(keys.len());
        for key in keys {
            if ctx.is_snapshot_isolation() {
                let buf = self.lock_bytes(key);
                let checked = if buf.is_empty() {
                    Ok(None)
                } else {
                    check_lock(
                        decode_lock(&buf),
                        key,
                        version,
                        &ctx.resolved_locks,
                        &ctx.committed_locks,
                    )
                };
                match checked {
                    Err(err) => pairs.push(KvPair {
                        key: key.clone(),
                        value: Vec::new(),
                        error: Some(Box::new(err)),
                    }),
                    Ok(Some(lock)) => {
                        // Go appends only when `getValueFromLock` is non-nil.
                        if let Some(value) = value_from_lock(&lock) {
                            pairs.push(KvPair {
                                key: key.clone(),
                                value: value.to_vec(),
                                error: None,
                            });
                        }
                    }
                    Ok(None) => remain.push(key),
                }
            } else if ctx.is_rc_check_ts() {
                let buf = self.lock_bytes(key);
                let checked = if buf.is_empty() {
                    Ok(())
                } else {
                    check_lock_rc_check_ts(&decode_lock(&buf), key, version, &ctx.resolved_locks)
                };
                match checked {
                    Err(err) => pairs.push(KvPair {
                        key: key.clone(),
                        value: Vec::new(),
                        error: Some(Box::new(err)),
                    }),
                    Ok(()) => remain.push(key),
                }
            } else {
                remain.push(key);
            }
        }
        // Go `dbreader.BatchGet`: under `RcCheckTS` the latest version is
        // read and a too-new commit rides the pair as its error; either way
        // only non-empty values append (`len(value) != 0`).
        let read_ts = if ctx.is_rc_check_ts() {
            u64::MAX
        } else {
            version
        };
        for key in remain {
            if let Some((value, meta)) = self.engine.get_at(key, read_ts) {
                if !value.is_empty() {
                    let error = if ctx.is_rc_check_ts() {
                        check_write_meta_rc_check_ts(version, meta)
                            .err()
                            .map(Box::new)
                    } else {
                        None
                    };
                    pairs.push(KvPair {
                        key: key.clone(),
                        value: value.to_vec(),
                        error,
                    });
                }
            }
        }
        pairs
    }

    /// Go `MVCCStore.Scan` (`mvcc.go:1985`): lock errors collected over the
    /// range first, the committed data scanned second, then Go's merge —
    /// stable sort by key (reversed under `Reverse`), first pair per key
    /// wins (a lock pair beats the data pair, because it was appended
    /// first), empty values dropped, capped at the limit.
    #[must_use]
    pub fn scan(&self, req: &ScanReq) -> Vec<KvPair> {
        self.scan_with(&ReadContext::default(), req)
    }

    /// [`Self::scan`] under an explicit read context: `RC` collects no lock
    /// pairs at all; SI and `RCCheckTS` collect through their `checkLock`
    /// arms.
    #[must_use]
    pub fn scan_with(&self, ctx: &ReadContext, req: &ScanReq) -> Vec<KvPair> {
        let (start_key, end_key) = if req.reverse {
            // boundary: `reqCtx.regCtx.RawStart()` — no region context; an
            // empty reverse end stays empty, the keyspace floor.
            (req.end_key.clone(), req.start_key.clone())
        } else {
            let end = if req.end_key.is_empty() {
                INTERNAL_KEY_PREFIX.to_vec()
            } else {
                req.end_key.clone()
            };
            (req.start_key.clone(), end)
        };
        let mut limit = req.limit;
        let mut lock_pairs = Vec::new();
        if req.sample_step == 0 {
            if ctx.is_snapshot_isolation() || ctx.is_rc_check_ts() {
                let (lo, hi) = if start_key <= end_key {
                    (&start_key, &end_key)
                } else {
                    (&end_key, &start_key)
                };
                lock_pairs = self.collect_range_lock(ctx, req.version, lo, hi);
            }
        } else {
            limit *= req.sample_step;
        }
        // Go `kvScanProcessor` over `dbreader.Scan`: newest version at or
        // below the read ts per key, sampled every `sample_step`-th key.
        let mut scanned = Vec::new();
        let mut scan_cnt: u32 = 0;
        let mut visited: u32 = 0;
        let mut visit = |key: &[u8], value: &[u8]| -> bool {
            // Go's `dbreader.Scan` counts every PROCESSED entry against the
            // limit — sampled-out and deleted entries included. That is what
            // makes `SampleStep * Limit` visit exactly that many keys and
            // emit `Limit` of them.
            if visited >= limit {
                return false;
            }
            visited += 1;
            if req.sample_step > 0 {
                scan_cnt += 1;
                if !(scan_cnt - 1).is_multiple_of(req.sample_step) {
                    return true;
                }
            }
            scanned.push(KvPair {
                key: key.to_vec(),
                value: value.to_vec(),
                error: None,
            });
            true
        };
        // Go `dbreader.Scan` under `RcCheckTS` walks LATEST versions and
        // errors on one committed past the read ts; the Scan caller then
        // answers a SINGLE keyless error pair, discarding the lock pairs
        // and everything scanned so far.
        let read_ts = if ctx.is_rc_check_ts() {
            u64::MAX
        } else {
            req.version
        };
        let mut keys: Vec<Vec<u8>> = self.committed_keys_in(&start_key, &end_key);
        if req.reverse {
            keys.reverse();
        }
        for key in keys {
            if let Some((value, meta)) = self.engine.get_at(&key, read_ts) {
                if ctx.is_rc_check_ts() {
                    if let Err(err) = check_write_meta_rc_check_ts(req.version, meta) {
                        return vec![KvPair {
                            key: Vec::new(),
                            value: Vec::new(),
                            error: Some(Box::new(err)),
                        }];
                    }
                }
                if !visit(&key, value) {
                    break;
                }
            }
        }
        let mut pairs = lock_pairs;
        pairs.extend(scanned);
        pairs.sort_by(|a, b| {
            if req.reverse {
                b.key.cmp(&a.key)
            } else {
                a.key.cmp(&b.key)
            }
        });
        let mut valid = Vec::new();
        let mut prev: Option<&[u8]> = None;
        for pair in &pairs {
            if prev == Some(pair.key.as_slice()) {
                continue;
            }
            prev = Some(pair.key.as_slice());
            if pair.error.is_some() || !pair.value.is_empty() {
                valid.push(pair.clone());
                if valid.len() >= limit as usize {
                    break;
                }
            }
        }
        valid
    }

    /// Go `collectRangeLock` (`mvcc.go:2062`): every lock in `[start, end)`
    /// runs the isolation level's check — under SI a committed lock becomes
    /// a VALUE pair (nil for a delete, dropped later by the merge's
    /// empty-value guard) and a blocking lock an error pair; under
    /// `RCCheckTS` only conflicts surface.
    fn collect_range_lock(
        &self,
        ctx: &ReadContext,
        start_ts: u64,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Vec<KvPair> {
        let mut pairs = Vec::new();
        let mut it = self.lock_store.new_iterator();
        it.seek(start_key);
        while it.valid() {
            if it.key() >= end_key {
                break;
            }
            let lock = decode_lock(it.value());
            if ctx.is_snapshot_isolation() {
                match check_lock(
                    lock,
                    it.key(),
                    start_ts,
                    &ctx.resolved_locks,
                    &ctx.committed_locks,
                ) {
                    Ok(Some(lock)) => pairs.push(KvPair {
                        key: it.key().to_vec(),
                        // A deleted key's value is nil (Go's comment, kept).
                        value: value_from_lock(&lock)
                            .map(<[u8]>::to_vec)
                            .unwrap_or_default(),
                        error: None,
                    }),
                    Ok(None) => {}
                    Err(err) => pairs.push(KvPair {
                        key: it.key().to_vec(),
                        value: Vec::new(),
                        error: Some(Box::new(err)),
                    }),
                }
            } else if ctx.is_rc_check_ts() {
                if let Err(err) =
                    check_lock_rc_check_ts(&lock, it.key(), start_ts, &ctx.resolved_locks)
                {
                    pairs.push(KvPair {
                        key: it.key().to_vec(),
                        value: Vec::new(),
                        error: Some(Box::new(err)),
                    });
                }
            }
            it.next();
        }
        pairs
    }

    /// The distinct user keys with any committed version in `[start, end)`,
    /// ascending — the engine walk `dbreader`'s iterator performs.
    fn committed_keys_in(&self, start_key: &[u8], end_key: &[u8]) -> Vec<Vec<u8>> {
        let mut keys: Vec<Vec<u8>> = Vec::new();
        for (key, _) in self.engine.entries.range((start_key.to_vec(), 0)..) {
            if key.0.as_slice() >= end_key {
                break;
            }
            if keys.last().map(Vec::as_slice) != Some(key.0.as_slice()) {
                keys.push(key.0.clone());
            }
        }
        keys
    }
}

/// Go `TxnStatus` (`mvcc.go`): what `CheckTxnStatus` learned about a
/// transaction.
#[derive(Clone, Debug, PartialEq)]
pub struct TxnStatus {
    /// `commitTS`: non-zero means committed at that ts.
    pub commit_ts: u64,
    /// `action`: what the check DID about the lock.
    pub action: KvrpcTxnAction,
    /// `lockInfo`: the still-live lock, when one is.
    pub lock_info: Option<Box<KvrpcLockInfo>>,
}

/// The `CheckTxnStatusRequest` fields Go reads.
#[derive(Clone, Debug, Default)]
pub struct CheckTxnStatusReq {
    /// `PrimaryKey`.
    pub primary_key: Vec<u8>,
    /// `LockTs`.
    pub lock_ts: u64,
    /// `CallerStartTs`.
    pub caller_start_ts: u64,
    /// `CurrentTs`.
    pub current_ts: u64,
    /// `RollbackIfNotExist`.
    pub rollback_if_not_exist: bool,
    /// `ForceSyncCommit`.
    pub force_sync_commit: bool,
    /// `ResolvingPessimisticLock`.
    pub resolving_pessimistic_lock: bool,
}

/// Go `oracle.ExtractPhysical`: the millisecond half of a TSO.
const fn extract_physical(ts: u64) -> u64 {
    ts >> 18
}

/// Go `maxSystemTS` (`mvcc.go`).
const MAX_SYSTEM_TS: u64 = u64::MAX;

impl MvccStore {
    /// Go `MVCCStore.CheckTxnStatus` (`mvcc.go:497`): what a reader blocked
    /// by a lock asks about the lock's PRIMARY — alive, expired, committed,
    /// or gone — and what the store does about each answer.
    pub fn check_txn_status(&mut self, req: &CheckTxnStatusReq) -> Result<TxnStatus, KvError> {
        let buf = self.lock_bytes(&req.primary_key);
        if !buf.is_empty() {
            let mut lock = decode_lock(&buf);
            if lock.hdr.start_ts == req.lock_ts {
                if req.primary_key != lock.primary {
                    return Err(KvError::PrimaryMismatch {
                        key: req.primary_key.clone(),
                        lock: Box::new(lock.to_lock_info(req.primary_key.clone())),
                    });
                }
                // An async-commit lock is never rolled back or pushed here.
                // (Unreachable until the async course lands, ported whole.)
                if lock.hdr.use_async_commit && !req.force_sync_commit {
                    return Ok(TxnStatus {
                        commit_ts: 0,
                        action: KvrpcTxnAction::NoAction,
                        lock_info: Some(Box::new(lock.to_lock_info(req.primary_key.clone()))),
                    });
                }
                // TTL, in TSO PHYSICAL milliseconds — not raw ts arithmetic.
                if extract_physical(lock.hdr.start_ts) + u64::from(lock.hdr.ttl)
                    < extract_physical(req.current_ts)
                {
                    if req.resolving_pessimistic_lock
                        && lock.hdr.op == KvrpcOp::PessimisticLock as i32 as u8
                    {
                        // Go `batch.PessimisticRollback` (`write.go:284`):
                        // the lock dies, nothing else is written.
                        self.lock_store_delete(&req.primary_key);
                        return Ok(TxnStatus {
                            commit_ts: 0,
                            action: KvrpcTxnAction::TtlExpirePessimisticRollback,
                            lock_info: None,
                        });
                    }
                    // Go `batch.Rollback(key, true)`.
                    let status_key = encode_extra_txn_status_key(&req.primary_key, req.lock_ts);
                    self.engine.set(
                        &status_key,
                        req.lock_ts,
                        &[],
                        DbUserMeta::new(req.lock_ts, 0),
                    );
                    self.lock_store_delete(&req.primary_key);
                    return Ok(TxnStatus {
                        commit_ts: 0,
                        action: KvrpcTxnAction::TtlExpireRollback,
                        lock_info: None,
                    });
                }
                // Alive: maybe push the min commit ts forward.
                let mut action = KvrpcTxnAction::NoAction;
                if req.caller_start_ts == MAX_SYSTEM_TS {
                    action = KvrpcTxnAction::MinCommitTsPushed;
                } else if lock.hdr.min_commit_ts > 0 && !lock.hdr.use_async_commit {
                    action = KvrpcTxnAction::MinCommitTsPushed;
                    // "We *must* guarantee the invariance
                    // lock.minCommitTS >= callerStartTS + 1".
                    if lock.hdr.min_commit_ts < req.caller_start_ts + 1 {
                        lock.hdr.min_commit_ts =
                            std::cmp::max(req.caller_start_ts + 1, req.current_ts);
                        // Go persists through `batch.PessimisticLock`, which
                        // is a lock-store overwrite (`write.go:280`).
                        self.lock_store_put(&req.primary_key, &lock.marshal_binary());
                    }
                }
                return Ok(TxnStatus {
                    commit_ts: 0,
                    action,
                    lock_info: Some(Box::new(lock.to_lock_info(req.primary_key.clone()))),
                });
            }
        }
        // No lock of ours: committed, rolled back, op-lock committed, or gone.
        if let Some(commit_ts) = self.committed_version_of(&req.primary_key, req.lock_ts) {
            return Ok(TxnStatus {
                commit_ts,
                action: KvrpcTxnAction::NoAction,
                lock_info: None,
            });
        }
        let status = self.check_extra_txn_status(&req.primary_key, req.lock_ts);
        if status.is_rollback {
            return Ok(TxnStatus {
                commit_ts: 0,
                action: KvrpcTxnAction::NoAction,
                lock_info: None,
            });
        }
        if status.is_op_lock_committed() {
            return Ok(TxnStatus {
                commit_ts: status.commit_ts,
                action: KvrpcTxnAction::NoAction,
                lock_info: None,
            });
        }
        if req.rollback_if_not_exist {
            if req.resolving_pessimistic_lock {
                return Ok(TxnStatus {
                    commit_ts: 0,
                    action: KvrpcTxnAction::LockNotExistDoNothing,
                    lock_info: None,
                });
            }
            // Go `batch.Rollback(key, false)`: the tombstone that makes a
            // late prewrite refuse, without a lock to delete.
            let status_key = encode_extra_txn_status_key(&req.primary_key, req.lock_ts);
            self.engine.set(
                &status_key,
                req.lock_ts,
                &[],
                DbUserMeta::new(req.lock_ts, 0),
            );
            return Ok(TxnStatus {
                commit_ts: 0,
                action: KvrpcTxnAction::LockNotExistRollback,
                lock_info: None,
            });
        }
        Err(KvError::TxnNotFound {
            primary_key: req.primary_key.clone(),
            start_ts: req.lock_ts,
        })
    }
}

impl MvccStore {
    /// Go `MVCCStore.ResolveLock` (`mvcc.go`): commit or roll back every one
    /// of a transaction's locks in the range at once — what a reader does
    /// after `check_txn_status` told it the transaction's fate.
    ///
    /// An EMPTY key list means "find them yourself": Go scans the region's
    /// lock store for locks bearing the start ts. The region bounds narrow
    /// to the whole keyspace here (no region context in this slice, named).
    /// A key whose lock has since vanished or changed hands is silently
    /// skipped — resolution races are the normal case, not an error.
    pub fn resolve_lock(
        &mut self,
        lock_keys: &[Vec<u8>],
        start_ts: u64,
        commit_ts: u64,
    ) -> Result<(), KvError> {
        let mut keys = lock_keys.to_vec();
        if keys.is_empty() {
            let mut it = self.lock_store.new_iterator();
            it.seek(&[]);
            while it.valid() {
                let lock = decode_lock(it.value());
                if lock.hdr.start_ts == start_ts {
                    keys.push(it.key().to_vec());
                }
                it.next();
            }
            if keys.is_empty() {
                return Ok(());
            }
        }
        for key in &keys {
            let buf = self.lock_bytes(key);
            if buf.is_empty() {
                continue;
            }
            let lock = decode_lock(&buf);
            if lock.hdr.start_ts != start_ts {
                continue;
            }
            if commit_ts > 0 {
                // Go `batch.Commit` (`write.go:256`).
                let meta = DbUserMeta::new(start_ts, commit_ts);
                if lock.hdr.op == KvrpcOp::PessimisticLock as i32 as u8 {
                    // nothing: as if pessimistic-rollbacked
                } else if lock.hdr.op != KvrpcOp::Lock as i32 as u8 {
                    self.engine.set(key, commit_ts, &lock.value, meta);
                } else if *key == lock.primary {
                    let status_key = encode_extra_txn_status_key(key, start_ts);
                    self.engine.set(&status_key, start_ts, &[], meta);
                }
                self.lock_store_delete(key);
            } else {
                // Go `batch.Rollback(key, true)`.
                let status_key = encode_extra_txn_status_key(key, start_ts);
                self.engine
                    .set(&status_key, start_ts, &[], DbUserMeta::new(start_ts, 0));
                self.lock_store_delete(key);
            }
        }
        Ok(())
    }
}

/// The `PessimisticLockRequest` fields the no-wait slice reads.
#[derive(Clone, Debug, Default)]
pub struct PessimisticLockReq {
    /// `Mutations` — `Op_PessimisticLock` per key.
    pub mutations: Vec<KvrpcMutation>,
    /// `PrimaryLock`.
    pub primary_lock: Vec<u8>,
    /// `StartVersion`.
    pub start_version: u64,
    /// `ForUpdateTs`.
    pub for_update_ts: u64,
    /// `LockTtl`.
    pub lock_ttl: u64,
    /// `ReturnValues`.
    pub return_values: bool,
    /// `CheckExistence`.
    pub check_existence: bool,
    /// `LockOnlyIfExists`.
    pub lock_only_if_exists: bool,
    /// `Force` — Go `req.Force`: lock DESPITE a newer commit, answering the
    /// latest value and its commit_ts instead of a write conflict.
    pub force: bool,
}

/// What `PessimisticLock` answers under `WakeUpModeNormal`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PessimisticLockResult {
    /// `resp.Values`, when `return_values`.
    pub values: Vec<Vec<u8>>,
    /// `resp.NotFounds`, when `return_values` or `check_existence`.
    pub not_founds: Vec<bool>,
    /// `Value` — filled only for a `Force` request: the latest committed
    /// value of the FIRST key.
    pub value: Vec<u8>,
    /// `CommitTs` — the version that value committed at, `Force` only.
    pub commit_ts: u64,
}

impl MvccStore {
    /// Go `MVCCStore.PessimisticLock` → `pessimisticLockInner`
    /// (`mvcc.go:226`), the NO-WAIT slice under `WakeUpModeNormal`.
    ///
    /// Named narrowings: `lockWaiterManager` and
    /// `handleCheckPessimisticErr` — a conflicting lock answers its lock
    /// error immediately, Go's no-wait outcome, instead of parking a
    /// waiter; `WakeUpModeForceLock` and `req.Force` refuse by name.
    pub fn pessimistic_lock(
        &mut self,
        req: &PessimisticLockReq,
    ) -> Result<PessimisticLockResult, KvError> {
        let mut mutations = req.mutations.clone();
        if !req.return_values {
            mutations.sort_by(|a, b| a.key.cmp(&b.key));
        }
        let start_ts = req.start_version;
        if req.lock_only_if_exists && !req.return_values {
            return Err(KvError::Unported(
                "LockOnlyIfExists without ReturnValues: Go errors here by contract",
            ));
        }
        let mut dup = false;
        for mutation in &mutations {
            match self.check_conflict_in_lock_store(&mutation.key, start_ts) {
                // boundary: `handleCheckPessimisticErr` + `lockWaiterManager`
                // — the no-wait outcome is the lock error itself.
                Err(err) => return Err(err),
                Ok(Some(lock)) => {
                    if lock.hdr.op != KvrpcOp::PessimisticLock as i32 as u8 {
                        return Err(KvError::LockTypeNotMatch);
                    }
                    if lock.hdr.for_update_ts >= req.for_update_ts {
                        // A duplicate command; values may still be wanted.
                        dup = true;
                        break;
                    }
                    // A single-statement-rollback leftover: overwritable.
                }
                Ok(None) => {}
            }
            if mutation.key == req.primary_lock {
                let status = self.check_extra_txn_status(&mutation.key, start_ts);
                if status.is_rollback {
                    return Err(KvError::AlreadyRollback);
                }
                if status.is_op_lock_committed() {
                    dup = true;
                    break;
                }
            }
        }
        let items: Vec<Option<(Vec<u8>, DbUserMeta)>> = mutations
            .iter()
            .map(|mutation| {
                self.engine
                    .get_at(&mutation.key, u64::MAX)
                    .map(|(value, meta)| (value.to_vec(), meta.clone()))
            })
            .collect();
        if !dup {
            for (mutation, item) in mutations.iter().zip(&items) {
                let latest_extra = self.latest_extra_meta_for_key(&mutation.key);
                if let Some(lock) =
                    build_pessimistic_lock(mutation, item.as_ref(), latest_extra.as_ref(), req)?
                {
                    // Go `batch.PessimisticLock` (`write.go:280`).
                    self.lock_store_put(&mutation.key, &lock.marshal_binary());
                }
            }
        }
        let mut result = PessimisticLockResult::default();
        // Go's `req.Force` arm answers the FIRST key's latest committed
        // value and commit_ts alongside the granted lock.
        if req.force {
            if let Some(Some((value, meta))) = items.first() {
                result.value = value.clone();
                result.commit_ts = meta.commit_ts();
            }
        }
        if req.return_values || req.check_existence {
            for item in &items {
                match item {
                    None => {
                        if req.return_values {
                            result.values.push(Vec::new());
                        }
                        result.not_founds.push(true);
                    }
                    Some((value, _)) => {
                        if req.return_values {
                            result.values.push(value.clone());
                        }
                        result.not_founds.push(value.is_empty());
                    }
                }
            }
        }
        Ok(result)
    }

    /// Go `prewritePessimistic` (`mvcc.go:849`), whole.
    fn prewrite_pessimistic(
        &mut self,
        mutations: &[KvrpcMutation],
        req: &PrewriteReq,
    ) -> Result<(), KvError> {
        use tidb_proto::KvrpcPessimisticAction;
        let start_ts = req.start_version;
        let mut expected_for_update: std::collections::BTreeMap<usize, u64> =
            std::collections::BTreeMap::new();
        for (index, expected) in &req.for_update_ts_constraints {
            if *index >= mutations.len() {
                return Err(KvError::Unported(
                    "prewrite request invalid: for_update_ts constraint index out of range",
                ));
            }
            expected_for_update.insert(*index, *expected);
        }
        for (i, mutation) in mutations.iter().enumerate() {
            if mutation.op == KvrpcOp::CheckNotExists as i32 {
                return Err(KvError::InvalidOp);
            }
            let buf = self.lock_bytes(&mutation.key);
            let lock = if buf.is_empty() {
                None
            } else {
                Some(decode_lock(&buf))
            };
            let is_pessimistic_check = req
                .pessimistic_actions
                .get(i)
                .is_some_and(|action| *action == KvrpcPessimisticAction::DoPessimisticCheck);
            let need_constraint_check = req
                .pessimistic_actions
                .get(i)
                .is_some_and(|action| *action == KvrpcPessimisticAction::DoConstraintCheck);
            let lock_match = lock
                .as_ref()
                .is_some_and(|lock| lock.hdr.start_ts == start_ts);
            let lock_constraint_passes = match (expected_for_update.get(&i), &lock) {
                (Some(expected), Some(lock)) => lock.hdr.for_update_ts == *expected,
                _ => true,
            };
            if is_pessimistic_check {
                if lock.is_none() || !lock_match || !lock_constraint_passes {
                    return Err(KvError::PessimisticLockNotFound);
                }
                let lock = lock.as_ref().expect("checked above");
                if lock.hdr.op != KvrpcOp::PessimisticLock as i32 as u8 {
                    // Duplicated command.
                    return Ok(());
                }
                // Go keeps the LARGER ttl; the mutated request field becomes
                // a local since the batch build reads it through the lock.
            } else if need_constraint_check {
                if let Some((_, meta)) = self.engine.get_at(&mutation.key, u64::MAX) {
                    if meta.commit_ts() > start_ts {
                        return Err(KvError::Conflict {
                            reason:
                                tidb_proto::kvrpcpb::write_conflict::Reason::LazyUniquenessCheck,
                            start_ts,
                            conflict_ts: meta.start_ts(),
                            conflict_commit_ts: meta.commit_ts(),
                            key: mutation.key.clone(),
                        });
                    }
                }
            } else {
                // Non-pessimistic key in a pessimistic transaction.
                if let Some(mut existing) = lock {
                    if !lock_match {
                        // Go zeroes the TTL: the owning transaction is
                        // decided, so waiting on it is pointless.
                        existing.hdr.ttl = 0;
                        return Err(KvError::Locked(Box::new(
                            existing.to_lock_info(mutation.key.clone()),
                        )));
                    }
                    // Duplicate command.
                    return Ok(());
                }
            }
        }
        let items: Vec<Option<(Vec<u8>, DbUserMeta)>> = mutations
            .iter()
            .map(|mutation| {
                self.engine
                    .get_at(&mutation.key, u64::MAX)
                    .map(|(value, meta)| (value.to_vec(), meta.clone()))
            })
            .collect();
        let req = &self.effective_prewrite_req(req)?;
        for (mutation, item) in mutations.iter().zip(&items) {
            let pessimistic_ttl = {
                let buf = self.lock_bytes(&mutation.key);
                if buf.is_empty() {
                    0
                } else {
                    u64::from(decode_lock(&buf).hdr.ttl)
                }
            };
            let mut effective = req.clone();
            if pessimistic_ttl > effective.lock_ttl {
                effective.lock_ttl = pessimistic_ttl;
            }
            let lock = build_prewrite_lock(mutation, item.as_ref(), &effective)?;
            self.lock_store_put(&mutation.key, &lock.marshal_binary());
        }
        Ok(())
    }

    /// Go `MVCCStore.PessimisticRollback` (`mvcc.go:435`): only OUR
    /// pessimistic locks with a for-update ts at or below the request's die;
    /// everything else survives.
    pub fn pessimistic_rollback(&mut self, keys: &[Vec<u8>], start_ts: u64, for_update_ts: u64) {
        let mut keys = keys.to_vec();
        keys.sort();
        for key in &keys {
            let buf = self.lock_bytes(key);
            if buf.is_empty() {
                continue;
            }
            let lock = decode_lock(&buf);
            if lock.hdr.op == KvrpcOp::PessimisticLock as i32 as u8
                && lock.hdr.start_ts == start_ts
                && lock.hdr.for_update_ts <= for_update_ts
            {
                self.lock_store_delete(key);
            }
        }
    }

    /// Go `MVCCStore.TxnHeartBeat` (`mvcc.go:465`): advise the primary's TTL
    /// upward; never downward.
    pub fn txn_heart_beat(
        &mut self,
        primary: &[u8],
        start_ts: u64,
        advise_ttl: u64,
    ) -> Result<u64, KvError> {
        let buf = self.lock_bytes(primary);
        if !buf.is_empty() {
            let mut lock = decode_lock(&buf);
            if lock.hdr.start_ts == start_ts {
                if lock.primary != primary {
                    return Err(KvError::Unported(
                        "heartbeat on non-primary key: Go errors here by contract",
                    ));
                }
                if u64::from(lock.hdr.ttl) < advise_ttl {
                    lock.hdr.ttl = u32::try_from(advise_ttl).unwrap_or(u32::MAX);
                    self.lock_store_put(primary, &lock.marshal_binary());
                }
                return Ok(u64::from(lock.hdr.ttl));
            }
        }
        Err(KvError::Unported(
            "lock doesn't exists: Go errors here by contract",
        ))
    }

    /// Go `getLatestExtraMetaForKey` (`mvcc.go`): the newest extra-status
    /// record's meta for a key, used by fair locking to see whether ANOTHER
    /// transaction touched it — Go's own comment concedes rollback records
    /// are indistinguishable here and a bigger commit ts only causes an
    /// extra retry, which is safe.
    fn latest_extra_meta_for_key(&self, key: &[u8]) -> Option<DbUserMeta> {
        let start = encode_extra_txn_status_key(key, u64::MAX);
        let end = encode_extra_txn_status_key(key, 0);
        for ((entry_key, _), (_, meta)) in self.engine.entries.range((start, 0)..) {
            if entry_key > &end {
                break;
            }
            // Go filters to the table/meta extra prefixes ('u' and 'n').
            match entry_key.first() {
                Some(&b'u') | Some(&b'n') => return Some(meta.clone()),
                _ => continue,
            }
        }
        None
    }
}

/// Go `buildPessimisticLock` (`mvcc.go`), `WakeUpModeNormal` arm: a commit
/// newer than the for-update ts is a `PessimisticRetry` conflict; an
/// `Assertion_NotExist` over a value is `ErrKeyAlreadyExists`;
/// `doesNeedLock` may decline to lock at all (`LockOnlyIfExists` over an
/// absent or deleted key). `req.Force` and the force-lock conflict carry
/// are refused at the caller.
fn build_pessimistic_lock(
    mutation: &KvrpcMutation,
    item: Option<&(Vec<u8>, DbUserMeta)>,
    latest_extra: Option<&DbUserMeta>,
    req: &PessimisticLockReq,
) -> Result<Option<Lock>, KvError> {
    if let Some((value, meta)) = item {
        let mut meta = meta;
        if let Some(extra) = latest_extra {
            if extra.commit_ts() > meta.commit_ts() {
                meta = extra;
            }
        }
        // Go `buildPessimisticLock`: `Force` skips the conflict check whole.
        if !req.force && meta.commit_ts() > req.for_update_ts {
            return Err(KvError::Conflict {
                reason: tidb_proto::kvrpcpb::write_conflict::Reason::PessimisticRetry,
                start_ts: req.start_version,
                conflict_ts: meta.start_ts(),
                conflict_commit_ts: meta.commit_ts(),
                key: mutation.key.clone(),
            });
        }
        if mutation.assertion == tidb_proto::KvrpcAssertion::NotExist as i32 && !value.is_empty() {
            return Err(KvError::KeyAlreadyExists {
                key: mutation.key.clone(),
            });
        }
    }
    // Go `doesNeedLock`.
    if req.lock_only_if_exists {
        match item {
            None => return Ok(None),
            Some((value, _)) if value.is_empty() => return Ok(None),
            Some(_) => {}
        }
    }
    Ok(Some(Lock {
        hdr: LockHdr {
            start_ts: req.start_version,
            for_update_ts: req.for_update_ts,
            op: KvrpcOp::PessimisticLock as i32 as u8,
            ttl: u32::try_from(req.lock_ttl).unwrap_or(u32::MAX),
            primary_len: u16::try_from(req.primary_lock.len())
                .expect("primary key fits u16, as Go's cast assumes"),
            min_commit_ts: 0,
            use_async_commit: false,
            secondary_num: 0,
            has_old_ver: false,
        },
        primary: req.primary_lock.clone(),
        value: Vec::new(),
        secondaries: Vec::new(),
    }))
}

/// Go `SecondaryLocksStatus` (`mvcc.go`): what became of a transaction's
/// secondary keys — a commit timestamp, a rollback (zero), or the still-live
/// locks.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SecondaryLocksStatus {
    /// Non-zero: the transaction committed at this ts.
    pub commit_ts: u64,
    /// The still-live secondary locks, when neither committed nor rolled
    /// back.
    pub locks: Vec<KvrpcLockInfo>,
}

impl MvccStore {
    /// Go `MVCCStore.CheckSecondaryLocks` (`mvcc.go`): the async-commit
    /// resolver's question about a transaction's secondaries — and like the
    /// primary's status check, a mutation wearing a query's face. A key
    /// whose lock is gone answers from the committed record or the
    /// extra-status record; a key with NEITHER gains the rollback tombstone
    /// on the spot. A still-PESSIMISTIC lock among the secondaries is rolled
    /// back immediately — the prewrite never reached it, so the transaction
    /// cannot commit. Only when every key still holds our real lock do the
    /// locks come back as the answer.
    pub fn check_secondary_locks(
        &mut self,
        keys: &[Vec<u8>],
        start_ts: u64,
    ) -> Result<SecondaryLocksStatus, KvError> {
        let mut keys = keys.to_vec();
        keys.sort();
        let mut locks = Vec::with_capacity(keys.len());
        for key in &keys {
            let buf = self.lock_bytes(key);
            let lock = if buf.is_empty() {
                None
            } else {
                Some(decode_lock(&buf))
            };
            let ours = lock
                .as_ref()
                .is_some_and(|lock| lock.hdr.start_ts == start_ts);
            if !ours {
                if let Some(commit_ts) = self.committed_version_of(key, start_ts) {
                    return Ok(SecondaryLocksStatus {
                        commit_ts,
                        locks: Vec::new(),
                    });
                }
                let status = self.check_extra_txn_status(key, start_ts);
                if status.is_op_lock_committed() {
                    return Ok(SecondaryLocksStatus {
                        commit_ts: status.commit_ts,
                        locks: Vec::new(),
                    });
                }
                if !status.is_rollback {
                    // Go `batch.Rollback(key, false)`: the tombstone lands
                    // NOW, so the answer it gives is the answer that stays.
                    let status_key = encode_extra_txn_status_key(key, start_ts);
                    self.engine
                        .set(&status_key, start_ts, &[], DbUserMeta::new(start_ts, 0));
                }
                return Ok(SecondaryLocksStatus::default());
            }
            let lock = lock.expect("checked ours above");
            if lock.hdr.op == KvrpcOp::PessimisticLock as i32 as u8 {
                // Go `batch.Rollback(key, true)`: tombstone AND the lock's
                // death — the prewrite never replaced this lock, so the
                // transaction cannot commit.
                let status_key = encode_extra_txn_status_key(key, start_ts);
                self.engine
                    .set(&status_key, start_ts, &[], DbUserMeta::new(start_ts, 0));
                self.lock_store_delete(key);
                return Ok(SecondaryLocksStatus::default());
            }
            locks.push(lock.to_lock_info(key.clone()));
        }
        Ok(SecondaryLocksStatus {
            commit_ts: 0,
            locks,
        })
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

/// Go `buildPrewriteLock` (`mvcc.go`). Named narrowing: `rowcodec.IsRowKey`
/// plus `encodeFromOldRow` — the old-row-format re-encode of a row key's
/// value, which needs the row codec.
///
/// Assertions check BEFORE the `Op_Insert` tail, against the latest
/// committed version. Go's unistore always reads the Write CF, so unlike
/// TiKV it checks even at `AssertionLevel_Fast` — only `Off` disables
/// (the `mvcc.go` comment says not to assume Fast's skip).
///
/// `Op_Insert` is Go's tail: over an existing committed non-empty value it
/// is `ErrKeyAlreadyExists`, and otherwise it BECOMES a `Put` — the lock
/// never records Insert.
fn build_prewrite_lock(
    mutation: &KvrpcMutation,
    existing: Option<&(Vec<u8>, DbUserMeta)>,
    req: &PrewriteReq,
) -> Result<Lock, KvError> {
    if req.assertion_level != tidb_proto::KvrpcAssertionLevel::Off as i32 {
        match existing.filter(|(value, _)| !value.is_empty()) {
            None => {
                if mutation.assertion == tidb_proto::KvrpcAssertion::Exist as i32 {
                    return Err(KvError::AssertionFailed {
                        start_ts: req.start_version,
                        key: mutation.key.clone(),
                        assertion: mutation.assertion,
                        existing_start_ts: 0,
                        existing_commit_ts: 0,
                    });
                }
            }
            Some((_, meta)) => {
                if mutation.assertion == tidb_proto::KvrpcAssertion::NotExist as i32 {
                    return Err(KvError::AssertionFailed {
                        start_ts: req.start_version,
                        key: mutation.key.clone(),
                        assertion: mutation.assertion,
                        existing_start_ts: meta.start_ts(),
                        existing_commit_ts: meta.commit_ts(),
                    });
                }
            }
        }
    }
    let mut op = u8::try_from(mutation.op).expect("an op byte");
    if op == KvrpcOp::Insert as i32 as u8 {
        if let Some((value, _)) = existing {
            if !value.is_empty() {
                return Err(KvError::KeyAlreadyExists {
                    key: mutation.key.clone(),
                });
            }
        }
        op = KvrpcOp::Put as i32 as u8;
    }
    Ok(Lock {
        hdr: LockHdr {
            start_ts: req.start_version,
            ttl: u32::try_from(req.lock_ttl).unwrap_or(u32::MAX),
            primary_len: u16::try_from(req.primary_lock.len())
                .expect("primary key fits u16, as Go's cast assumes"),
            min_commit_ts: req.min_commit_ts,
            use_async_commit: req.use_async_commit,
            secondary_num: u32::try_from(req.secondaries.len()).expect("secondary count fits u32"),
            op,
            for_update_ts: req.for_update_ts,
            has_old_ver: false,
        },
        primary: req.primary_lock.clone(),
        value: mutation.value.clone(),
        secondaries: req.secondaries.clone(),
    })
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
                reason: tidb_proto::kvrpcpb::write_conflict::Reason::Optimistic,
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
    fn the_async_paths_refuse_by_name() {
        let mut store = MvccStore::new();
        let refuse = store.prewrite(&PrewriteReq {
            use_async_commit: true,
            ..PrewriteReq::default()
        });
        assert!(matches!(refuse, Err(KvError::Unported(_))));
    }

    #[test]
    fn a_pessimistic_transaction_locks_prewrites_and_commits() {
        // The full pessimistic round trip: acquire the lock, prewrite with
        // DO_PESSIMISTIC_CHECK, commit, read — and the prewrite keeps the
        // pessimistic lock's LARGER ttl, Go's do-not-shrink rule.
        use tidb_proto::KvrpcPessimisticAction;
        let mut store = MvccStore::new();
        let key = b"pess".as_slice();
        store
            .pessimistic_lock(&PessimisticLockReq {
                mutations: vec![KvrpcMutation {
                    op: KvrpcOp::PessimisticLock as i32,
                    key: key.to_vec(),
                    ..KvrpcMutation::default()
                }],
                primary_lock: key.to_vec(),
                start_version: 10,
                for_update_ts: 10,
                lock_ttl: 5000,
                ..PessimisticLockReq::default()
            })
            .expect("locks");
        let lock = decode_lock(&store.lock_bytes(key));
        assert_eq!(lock.hdr.op, KvrpcOp::PessimisticLock as i32 as u8);
        assert_eq!(lock.hdr.for_update_ts, 10);
        store
            .prewrite(&PrewriteReq {
                mutations: vec![put(key, b"v")],
                primary_lock: key.to_vec(),
                start_version: 10,
                for_update_ts: 10,
                lock_ttl: 100,
                pessimistic_actions: vec![KvrpcPessimisticAction::DoPessimisticCheck],
                ..PrewriteReq::default()
            })
            .expect("prewrites over the pessimistic lock");
        let lock = decode_lock(&store.lock_bytes(key));
        assert_eq!(lock.hdr.op, KvrpcOp::Put as i32 as u8, "now a real lock");
        assert_eq!(u64::from(lock.hdr.ttl), 5000, "the larger ttl survives");
        store.commit(&[key.to_vec()], 10, 12).expect("commits");
        assert_eq!(store.get(key, 12).expect("v"), Some(b"v".to_vec()));

        // And without the pessimistic lock, DO_PESSIMISTIC_CHECK refuses.
        let err = store
            .prewrite(&PrewriteReq {
                mutations: vec![put(b"other", b"v")],
                primary_lock: b"other".to_vec(),
                start_version: 20,
                for_update_ts: 20,
                pessimistic_actions: vec![KvrpcPessimisticAction::DoPessimisticCheck],
                ..PrewriteReq::default()
            })
            .expect_err("no lock to check");
        assert_eq!(err, KvError::PessimisticLockNotFound);
    }

    #[test]
    fn a_newer_commit_defeats_a_pessimistic_acquisition() {
        // `buildPessimisticLock`: a commit newer than the for-update ts is a
        // PessimisticRetry conflict; pessimistic rollback then leaves other
        // txns' locks alone.
        let mut store = MvccStore::new();
        let key = b"k".as_slice();
        must_prewrite_optimistic(&mut store, key, key, b"v", 10, 200);
        store.commit(&[key.to_vec()], 10, 30).expect("commits");
        let err = store
            .pessimistic_lock(&PessimisticLockReq {
                mutations: vec![KvrpcMutation {
                    op: KvrpcOp::PessimisticLock as i32,
                    key: key.to_vec(),
                    ..KvrpcMutation::default()
                }],
                primary_lock: key.to_vec(),
                start_version: 20,
                for_update_ts: 25,
                ..PessimisticLockReq::default()
            })
            .expect_err("the commit at 30 is newer than for-update 25");
        assert!(matches!(
            err,
            KvError::Conflict {
                reason: tidb_proto::kvrpcpb::write_conflict::Reason::PessimisticRetry,
                conflict_commit_ts: 30,
                ..
            }
        ));
        // Succeeds once the for-update ts sees the commit.
        store
            .pessimistic_lock(&PessimisticLockReq {
                mutations: vec![KvrpcMutation {
                    op: KvrpcOp::PessimisticLock as i32,
                    key: key.to_vec(),
                    ..KvrpcMutation::default()
                }],
                primary_lock: key.to_vec(),
                start_version: 20,
                for_update_ts: 35,
                ..PessimisticLockReq::default()
            })
            .expect("locks at 35");
        store.pessimistic_rollback(&[key.to_vec()], 20, 35);
        assert!(store.lock_bytes(key).is_empty(), "rolled back cleanly");
    }

    #[test]
    fn test_scan_sample_step() {
        // TRANSCREATED from Go `TestScanSampleStep` (`mvcc_test.go:1492`):
        // 1000 committed keys t0000..t0999, scan [t0100, t0900) step 10 —
        // 80 pairs at limit 100, each the i*10th key; 20 at limit 20.
        let mut store = MvccStore::new();
        let gen = |i: usize| format!("t{i:04}").into_bytes();
        for i in 0..1000 {
            let k = gen(i);
            must_prewrite_optimistic(&mut store, &k, &k, &k, 1, 200);
            store.commit(&[k], 1, 2).expect("commits");
        }
        let mut req = ScanReq {
            start_key: gen(100),
            end_key: gen(900),
            limit: 100,
            version: 2,
            sample_step: 10,
            ..ScanReq::default()
        };
        let pairs = store.scan(&req);
        assert_eq!(pairs.len(), 80);
        for (i, pair) in pairs.iter().enumerate() {
            assert_eq!(pair.key, gen(100 + i * 10));
        }
        req.limit = 20;
        let pairs = store.scan(&req);
        assert_eq!(pairs.len(), 20);
        for (i, pair) in pairs.iter().enumerate() {
            assert_eq!(pair.key, gen(100 + i * 10));
        }
    }

    #[test]
    fn scan_reports_locks_as_error_pairs_and_data_beside_them() {
        // `collectRangeLock` + the merge: a locked key yields an error pair
        // IN PLACE, other keys their values, sorted together.
        let mut store = MvccStore::new();
        for (key, ts) in [(b"a".as_slice(), 10_u64), (b"c", 12)] {
            must_prewrite_optimistic(&mut store, key, key, b"v", ts, 200);
            store.commit(&[key.to_vec()], ts, ts + 1).expect("commits");
        }
        must_prewrite_optimistic(&mut store, b"b", b"b", b"w", 15, 200);
        let pairs = store.scan(&ScanReq {
            start_key: b"a".to_vec(),
            end_key: b"z".to_vec(),
            limit: 10,
            version: 20,
            ..ScanReq::default()
        });
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0].key, b"a");
        assert!(pairs[0].error.is_none());
        assert_eq!(pairs[1].key, b"b");
        assert!(pairs[1].error.is_some(), "the lock is the answer for b");
        assert_eq!(pairs[2].key, b"c");
        // Below the lock's start ts the same scan sees no lock at all.
        let pairs = store.scan(&ScanReq {
            start_key: b"a".to_vec(),
            end_key: b"z".to_vec(),
            limit: 10,
            version: 14,
            ..ScanReq::default()
        });
        assert_eq!(pairs.len(), 2, "only committed data below the lock");
    }

    #[test]
    fn scan_drops_deleted_rows_and_respects_reverse() {
        let mut store = MvccStore::new();
        for key in [b"a".as_slice(), b"b", b"c"] {
            must_prewrite_optimistic(&mut store, key, key, b"v", 10, 200);
            store.commit(&[key.to_vec()], 10, 11).expect("commits");
        }
        // Delete b: a committed empty value, dropped by the valid-pairs walk.
        store
            .prewrite(&PrewriteReq {
                mutations: vec![KvrpcMutation {
                    op: KvrpcOp::Del as i32,
                    key: b"b".to_vec(),
                    ..KvrpcMutation::default()
                }],
                primary_lock: b"b".to_vec(),
                start_version: 20,
                ..PrewriteReq::default()
            })
            .expect("prewrites");
        store.commit(&[b"b".to_vec()], 20, 21).expect("commits");
        let forward = store.scan(&ScanReq {
            start_key: b"a".to_vec(),
            end_key: Vec::new(), // falls to INTERNAL_KEY_PREFIX
            limit: 10,
            version: 30,
            ..ScanReq::default()
        });
        assert_eq!(
            forward.iter().map(|p| p.key.clone()).collect::<Vec<_>>(),
            vec![b"a".to_vec(), b"c".to_vec()]
        );
        let reverse = store.scan(&ScanReq {
            start_key: b"z".to_vec(),
            end_key: b"a".to_vec(),
            limit: 10,
            version: 30,
            reverse: true,
            ..ScanReq::default()
        });
        assert_eq!(
            reverse.iter().map(|p| p.key.clone()).collect::<Vec<_>>(),
            vec![b"c".to_vec(), b"a".to_vec()]
        );
    }

    #[test]
    fn batch_get_isolates_lock_errors_per_key() {
        // `BatchGet` never fails as a whole: the locked key carries its
        // error, its neighbours their values, absents nothing.
        let mut store = MvccStore::new();
        must_prewrite_optimistic(&mut store, b"a", b"a", b"v", 10, 200);
        store.commit(&[b"a".to_vec()], 10, 11).expect("commits");
        must_prewrite_optimistic(&mut store, b"b", b"b", b"w", 15, 200);
        let pairs = store.batch_get(&[b"a".to_vec(), b"b".to_vec(), b"missing".to_vec()], 20);
        assert_eq!(pairs.len(), 2);
        // Go's order: lock-check pairs land during the key loop, data pairs
        // after it — the error pair precedes the value pair.
        assert_eq!(pairs[0].key, b"b");
        assert!(pairs[0].error.is_some());
        assert_eq!(pairs[1].key, b"a");
        assert_eq!(pairs[1].value, b"v");
    }

    #[test]
    fn test_check_txn_status() {
        // TRANSCREATED from Go `TestCheckTxnStatus` (`mvcc_test.go:619`),
        // the optimistic body — the mismatching-primary tail requires a
        // pessimistic lock and rides with that course.
        let mut store = MvccStore::new();
        let pk = b"tpk".as_slice();
        let mut start_ts = 1_u64;
        let caller_start_ts = 3_u64;
        let mut current_ts = 5_u64;
        let check = |store: &mut MvccStore, lock_ts, caller, current| {
            store
                .check_txn_status(&CheckTxnStatusReq {
                    primary_key: pk.to_vec(),
                    lock_ts,
                    caller_start_ts: caller,
                    current_ts: current,
                    rollback_if_not_exist: true,
                    ..CheckTxnStatusReq::default()
                })
                .expect("checks")
        };
        let ttl_of = |status: &TxnStatus| status.lock_info.as_ref().map_or(0, |l| l.lock_ttl);

        // Try to check a not exist thing.
        let status = check(&mut store, start_ts, caller_start_ts, current_ts);
        assert_eq!(ttl_of(&status), 0);
        assert_eq!(status.commit_ts, 0);
        assert_eq!(status.action, KvrpcTxnAction::LockNotExistRollback);

        // Using same startTs, prewrite fails: checkTxnStatus rollbacked it.
        let (val, lock_ttl, min_commit_ts) = (b"val".as_slice(), 100_u64, 20_u64);
        let err = store
            .prewrite(&PrewriteReq {
                mutations: vec![put(pk, val)],
                primary_lock: pk.to_vec(),
                start_version: start_ts,
                lock_ttl,
                min_commit_ts,
                ..PrewriteReq::default()
            })
            .expect_err("rollbacked");
        assert_eq!(err, KvError::AlreadyRollback);

        // Prewrite a large txn at start ts 2.
        start_ts = 2;
        store
            .prewrite(&PrewriteReq {
                mutations: vec![put(pk, val)],
                primary_lock: pk.to_vec(),
                start_version: start_ts,
                lock_ttl,
                min_commit_ts,
                ..PrewriteReq::default()
            })
            .expect("prewrites");
        let status = check(&mut store, start_ts, caller_start_ts, current_ts);
        assert_eq!(ttl_of(&status), lock_ttl);
        assert_eq!(status.commit_ts, 0);
        assert_eq!(status.action, KvrpcTxnAction::MinCommitTsPushed);

        // caller 25, current 25: minCommitTs 20 -> 26 (callerStartTs + 1).
        let new_caller_ts = 25_u64;
        let status = check(&mut store, start_ts, new_caller_ts, new_caller_ts);
        assert_eq!(ttl_of(&status), lock_ttl);
        assert_eq!(status.action, KvrpcTxnAction::MinCommitTsPushed);
        let lock = decode_lock(&store.lock_bytes(pk));
        assert_eq!(lock.hdr.start_ts, start_ts);
        assert_eq!(u64::from(lock.hdr.ttl), lock_ttl);
        assert_eq!(lock.hdr.min_commit_ts, new_caller_ts + 1);

        // caller 25 again: 25 < 26 already holds, no update.
        let status = check(&mut store, start_ts, new_caller_ts, new_caller_ts);
        assert_eq!(status.action, KvrpcTxnAction::MinCommitTsPushed);
        assert_eq!(
            decode_lock(&store.lock_bytes(pk)).hdr.min_commit_ts,
            new_caller_ts + 1
        );

        // current 25 < minCommitTs 26 < caller 35: pushed to 36.
        current_ts = 25;
        let new_caller_ts = 35_u64;
        let status = check(&mut store, start_ts, new_caller_ts, current_ts);
        assert_eq!(status.action, KvrpcTxnAction::MinCommitTsPushed);
        assert_eq!(
            decode_lock(&store.lock_bytes(pk)).hdr.min_commit_ts,
            new_caller_ts + 1
        );

        // current 40 has no effect: caller 35 < minCommitTs 36 already.
        current_ts = 40;
        let status = check(&mut store, start_ts, new_caller_ts, current_ts);
        assert_eq!(status.action, KvrpcTxnAction::MinCommitTsPushed);
        assert_eq!(
            decode_lock(&store.lock_bytes(pk)).hdr.min_commit_ts,
            new_caller_ts + 1
        );

        // Committing below minCommitTs 36 errors; at 41 it lands.
        assert!(store.commit(&[pk.to_vec()], start_ts, 35).is_err());
        store.commit(&[pk.to_vec()], start_ts, 41).expect("commits");

        // The committed answer.
        let status = check(&mut store, start_ts, 42, 42);
        assert_eq!(ttl_of(&status), 0);
        assert_eq!(status.commit_ts, 41);
        assert_eq!(status.action, KvrpcTxnAction::NoAction);
    }

    #[test]
    fn an_expired_lock_is_rolled_back_by_the_check() {
        // The TTL arm compares PHYSICAL milliseconds (ts >> 18), not raw
        // timestamps: a raw-ts comparison would expire nothing here.
        let mut store = MvccStore::new();
        let pk = b"pk".as_slice();
        let start_ts = 5_u64 << 18;
        store
            .prewrite(&PrewriteReq {
                mutations: vec![put(pk, b"v")],
                primary_lock: pk.to_vec(),
                start_version: start_ts,
                lock_ttl: 10,
                ..PrewriteReq::default()
            })
            .expect("prewrites");
        // physical(start)=5, ttl=10; current physical 20 > 15: expired.
        let status = store
            .check_txn_status(&CheckTxnStatusReq {
                primary_key: pk.to_vec(),
                lock_ts: start_ts,
                current_ts: 20 << 18,
                rollback_if_not_exist: true,
                ..CheckTxnStatusReq::default()
            })
            .expect("checks");
        assert_eq!(status.action, KvrpcTxnAction::TtlExpireRollback);
        assert!(store.lock_bytes(pk).is_empty(), "the lock died");
        // And the rollback record now refuses the transaction forever.
        assert_eq!(
            store
                .prewrite(&PrewriteReq {
                    mutations: vec![put(pk, b"v")],
                    primary_lock: pk.to_vec(),
                    start_version: start_ts,
                    ..PrewriteReq::default()
                })
                .expect_err("refused"),
            KvError::AlreadyRollback
        );
    }

    #[test]
    fn check_without_rollback_flag_is_txn_not_found() {
        let mut store = MvccStore::new();
        let err = store
            .check_txn_status(&CheckTxnStatusReq {
                primary_key: b"nothing".to_vec(),
                lock_ts: 9,
                ..CheckTxnStatusReq::default()
            })
            .expect_err("not found");
        assert_eq!(
            err,
            KvError::TxnNotFound {
                primary_key: b"nothing".to_vec(),
                start_ts: 9,
            }
        );
    }

    #[test]
    fn resolve_lock_commits_or_rolls_back_a_whole_transaction() {
        // The reader's follow-through after check_txn_status: commit_ts > 0
        // commits every lock of the txn, 0 rolls them back; an empty key
        // list finds the locks itself.
        let mut store = MvccStore::new();
        for key in [b"ra".as_slice(), b"rb"] {
            store
                .prewrite(&PrewriteReq {
                    mutations: vec![put(key, b"v")],
                    primary_lock: b"ra".to_vec(),
                    start_version: 7,
                    ..PrewriteReq::default()
                })
                .expect("prewrites");
        }
        // A bystander lock from another txn survives untouched.
        must_prewrite_optimistic(&mut store, b"rc", b"rc", b"w", 9, 200);
        store.resolve_lock(&[], 7, 8).expect("resolves by scan");
        assert_eq!(store.get(b"ra", 10).expect("v"), Some(b"v".to_vec()));
        assert_eq!(store.get(b"rb", 10).expect("v"), Some(b"v".to_vec()));
        assert!(store.get(b"rc", 10).is_err(), "the bystander still locks");

        // And the rollback direction, by explicit keys.
        let mut store = MvccStore::new();
        must_prewrite_optimistic(&mut store, b"rx", b"rx", b"v", 20, 200);
        store
            .resolve_lock(&[b"rx".to_vec()], 20, 0)
            .expect("rolls back");
        assert!(store.lock_bytes(b"rx").is_empty());
        assert_eq!(
            store
                .prewrite(&PrewriteReq {
                    mutations: vec![put(b"rx", b"v")],
                    primary_lock: b"rx".to_vec(),
                    start_version: 20,
                    ..PrewriteReq::default()
                })
                .expect_err("tombstoned"),
            KvError::AlreadyRollback
        );
    }

    #[test]
    fn test_check_secondary_locks_status() {
        // TRANSCREATED from Go `TestCheckSecondaryLocksStatus`
        // (`mvcc_test.go:729`), the optimistic body: commit at 3 for ts 1,
        // rollback at 5, commit at 9 for ts 7 -- each answer read back, plus
        // the tombstone the no-record arm writes.
        let mut store = MvccStore::new();
        let (pk, secondary, val) = (b"pk".as_slice(), b"secondary".as_slice(), b"val");
        must_prewrite_optimistic(&mut store, pk, secondary, val, 1, 200);
        store.commit(&[secondary.to_vec()], 1, 3).expect("commits");
        store
            .rollback(&[secondary.to_vec()], 5)
            .expect("rolls back");
        must_prewrite_optimistic(&mut store, pk, secondary, val, 7, 200);
        store.commit(&[secondary.to_vec()], 7, 9).expect("commits");

        // Lock is committed at 3.
        let status = store
            .check_secondary_locks(&[secondary.to_vec()], 1)
            .expect("checks");
        assert_eq!(status.locks.len(), 0);
        assert_eq!(status.commit_ts, 3);
        assert_eq!(store.get(secondary, 3).expect("v"), Some(val.to_vec()));

        // Op_Lock-free commit at 9 answers 9 for ts 7.
        let status = store
            .check_secondary_locks(&[secondary.to_vec()], 7)
            .expect("checks");
        assert_eq!(status.locks.len(), 0);
        assert_eq!(status.commit_ts, 9);

        // The rollback at 5 answers zero.
        let status = store
            .check_secondary_locks(&[secondary.to_vec()], 5)
            .expect("checks");
        assert_eq!(status, SecondaryLocksStatus::default());

        // A ts with no record at all gains the tombstone: the answer stays.
        let status = store
            .check_secondary_locks(&[secondary.to_vec()], 6)
            .expect("checks");
        assert_eq!(status, SecondaryLocksStatus::default());
        assert_eq!(
            store
                .prewrite(&PrewriteReq {
                    mutations: vec![put(secondary, val)],
                    primary_lock: secondary.to_vec(),
                    start_version: 6,
                    ..PrewriteReq::default()
                })
                .expect_err("tombstoned"),
            KvError::AlreadyRollback
        );

        // A live optimistic lock among the secondaries comes back AS a lock.
        must_prewrite_optimistic(&mut store, pk, b"live", b"v", 20, 200);
        let status = store
            .check_secondary_locks(&[b"live".to_vec()], 20)
            .expect("checks");
        assert_eq!(status.commit_ts, 0);
        assert_eq!(status.locks.len(), 1);
        assert_eq!(status.locks[0].lock_version, 20);
    }

    fn mutation(op: KvrpcOp, key: &[u8], value: &[u8]) -> KvrpcMutation {
        KvrpcMutation {
            op: op as i32,
            key: key.to_vec(),
            value: value.to_vec(),
            ..KvrpcMutation::default()
        }
    }

    fn must_prewrite_op(store: &mut MvccStore, pk: &[u8], m: KvrpcMutation, start_ts: u64) {
        store
            .prewrite(&PrewriteReq {
                mutations: vec![m],
                primary_lock: pk.to_vec(),
                start_version: start_ts,
                lock_ttl: 100,
                ..PrewriteReq::default()
            })
            .expect("prewrite succeeds");
    }

    fn must_get_rollback(store: &MvccStore, key: &[u8], start_ts: u64) {
        assert!(
            store.check_extra_txn_status(key, start_ts).is_rollback,
            "a rollback record must stand at ts {start_ts}"
        );
    }

    /// Go `TestRollback` (`mvcc_test.go:559`), including the pinned quirk:
    /// tikv collapses older rollback records, "but unistore will not" — the
    /// source's own comment — so BOTH records still answer.
    #[test]
    fn test_rollback() {
        let mut store = MvccStore::new();
        let (key, val) = (b"tkey".as_slice(), b"value".as_slice());
        must_prewrite_optimistic(&mut store, key, key, val, 1, 100);
        store.rollback(&[key.to_vec()], 1).expect("rolls back");
        must_prewrite_optimistic(&mut store, key, key, val, 2, 100);
        store.rollback(&[key.to_vec()], 2).expect("rolls back");
        assert!(store.check_extra_txn_status(key, 1).is_rollback);

        let (k, v) = (b"tk".as_slice(), b"v".as_slice());
        must_prewrite_optimistic(&mut store, k, k, v, 1, 100);
        store.rollback(&[k.to_vec()], 1).expect("rolls back");
        must_get_rollback(&store, k, 1);
        must_prewrite_optimistic(&mut store, k, k, v, 2, 100);
        store.rollback(&[k.to_vec()], 2).expect("rolls back");
        assert_eq!(store.get(k, 2).expect("readable"), None);
        must_get_rollback(&store, k, 2);
        // The uncollapsed older record: unistore keeps it.
        must_get_rollback(&store, k, 1);
    }

    /// Go `TestRollbackKey` (`mvcc_test.go:1096`): a rolled-back Op_Lock or
    /// Op_Del leaves the earlier committed value readable.
    #[test]
    fn test_rollback_key() {
        let mut store = MvccStore::new();
        let (k, v) = (b"tk".as_slice(), b"v".as_slice());
        must_prewrite_optimistic(&mut store, k, k, v, 5, 100);
        store.commit(&[k.to_vec()], 5, 10).expect("commits");

        must_prewrite_op(&mut store, k, mutation(KvrpcOp::Lock, k, b""), 15);
        store
            .rollback(&[k.to_vec()], 15)
            .expect("rolls back the lock");
        assert!(store.lock_bytes(k).is_empty(), "unlocked");
        assert_eq!(store.get(k, 16).expect("readable"), Some(v.to_vec()));

        must_prewrite_op(&mut store, k, mutation(KvrpcOp::Del, k, b""), 17);
        store
            .rollback(&[k.to_vec()], 17)
            .expect("rolls back the delete");
        assert_eq!(store.get(k, 18).expect("readable"), Some(v.to_vec()));
    }

    /// Go `TestCleanup` (`mvcc_test.go:1120`): the TTL arm — an unexpired
    /// lock refuses cleanup; expiry is physical (`ts >> 18`).
    #[test]
    fn test_cleanup() {
        let mut store = MvccStore::new();
        let (k, v) = (b"tk".as_slice(), b"v".as_slice());
        must_prewrite_optimistic(&mut store, k, k, v, 10, 100);
        assert!(!store.lock_bytes(k).is_empty());
        store
            .txn_heart_beat(k, 10, 100)
            .expect("the heart beat lands");
        // A SMALLER advise leaves the larger TTL standing.
        assert_eq!(
            store.txn_heart_beat(k, 10, 90).expect("still beats"),
            100,
            "the lock keeps the larger TTL"
        );
        // TTL not expired: refused, lock stands.
        assert!(matches!(store.cleanup(k, 10, 20), Err(KvError::Locked(_))));
        assert!(!store.lock_bytes(k).is_empty());
        // A DIFFERENT transaction's cleanup writes its own rollback record.
        store.cleanup(k, 11, 20).expect("a foreign ts cleans");
        // TTL expired (current physical 120 > start physical 0 + ttl 100).
        store
            .cleanup(k, 10, 120 << 18)
            .expect("the expired lock dies");
        assert!(store.lock_bytes(k).is_empty(), "unlocked");
    }

    /// Go `TestCommit` (`mvcc_test.go:1141`), with the source's own quirk
    /// comment kept: "Secondary Op_Lock keys could not be committed more
    /// than once on unistore".
    #[test]
    fn test_commit() {
        let mut store = MvccStore::new();
        let (k, v) = (b"tk".as_slice(), b"v".as_slice());
        assert!(store.commit(&[k.to_vec()], 1, 2).is_err(), "not prewritten");
        must_prewrite_optimistic(&mut store, k, k, v, 5, 100);
        assert!(store.commit(&[k.to_vec()], 4, 5).is_err(), "wrong start_ts");
        store.rollback(&[k.to_vec()], 5).expect("rolls back");
        assert!(
            store.commit(&[k.to_vec()], 5, 6).is_err(),
            "commit after rollback"
        );

        let (k1, v1, k2, k3) = (
            b"tk1".as_slice(),
            b"v".as_slice(),
            b"tk2".as_slice(),
            b"tk3".as_slice(),
        );
        must_prewrite_op(&mut store, k1, mutation(KvrpcOp::Put, k1, v1), 10);
        must_prewrite_op(&mut store, k1, mutation(KvrpcOp::Lock, k2, b""), 10);
        must_prewrite_op(&mut store, k1, mutation(KvrpcOp::Del, k3, b""), 10);
        for key in [k1, k2, k3] {
            store.commit(&[key.to_vec()], 10, 15).expect("commits");
        }
        assert_eq!(store.get(k1, 16).expect("readable"), Some(v1.to_vec()));
        assert_eq!(store.get(k2, 16).expect("readable"), None);
        assert_eq!(store.get(k3, 16).expect("readable"), None);
        // Commit again has no effect.
        store.commit(&[k1.to_vec()], 10, 15).expect("idempotent");
        // Secondary Op_Lock keys could not be committed more than once on
        // unistore (Go's own comment; the recommit of k2 stays skipped).
        store.commit(&[k3.to_vec()], 10, 15).expect("idempotent");
        assert_eq!(store.get(k1, 16).expect("readable"), Some(v1.to_vec()));

        // The rollback must fail: the transaction committed.
        assert!(store.rollback(&[k1.to_vec()], 10).is_err());
        assert_eq!(store.get(k1, 17).expect("readable"), Some(v1.to_vec()));

        // Rollback before prewrite leaves a record a later prewrite dies on.
        let kr = b"tkr".as_slice();
        store.rollback(&[kr.to_vec()], 5).expect("pre-rollback");
        assert!(
            store
                .prewrite(&PrewriteReq {
                    mutations: vec![mutation(KvrpcOp::Lock, kr, b"")],
                    primary_lock: kr.to_vec(),
                    start_version: 5,
                    lock_ttl: 100,
                    ..PrewriteReq::default()
                })
                .is_err(),
            "the standing rollback refuses the late prewrite"
        );
    }

    /// Go `TestMinCommitTs` (`mvcc_test.go:1191`): a pushed min_commit_ts
    /// refuses any commit at or below the push and admits the one above.
    #[test]
    fn test_min_commit_ts() {
        let mut store = MvccStore::new();
        let (k, v) = (b"tk".as_slice(), b"v".as_slice());
        store
            .prewrite(&PrewriteReq {
                mutations: vec![mutation(KvrpcOp::Put, k, v)],
                primary_lock: k.to_vec(),
                start_version: 10,
                lock_ttl: 100,
                min_commit_ts: 11,
                ..PrewriteReq::default()
            })
            .expect("prewrites");
        let status = store
            .check_txn_status(&CheckTxnStatusReq {
                primary_key: k.to_vec(),
                lock_ts: 10,
                caller_start_ts: 20,
                current_ts: 20,
                ..CheckTxnStatusReq::default()
            })
            .expect("the status answers");
        assert_eq!(status.action, KvrpcTxnAction::MinCommitTsPushed);
        assert!(store.commit(&[k.to_vec()], 10, 15).is_err());
        assert!(store.commit(&[k.to_vec()], 10, 20).is_err());
        store.commit(&[k.to_vec()], 10, 21).expect("above the push");
    }

    /// Go `TestOverwritePessimisitcLock` (`mvcc_test.go:592`, typo theirs):
    /// re-locking keeps the LARGEST for_update_ts.
    #[test]
    fn test_overwrite_pessimistic_lock() {
        let mut store = MvccStore::new();
        let key = b"key".as_slice();
        let lock_at = |store: &mut MvccStore, for_update_ts: u64| {
            store
                .pessimistic_lock(&PessimisticLockReq {
                    mutations: vec![mutation(KvrpcOp::PessimisticLock, key, b"")],
                    primary_lock: key.to_vec(),
                    start_version: 1,
                    for_update_ts,
                    lock_ttl: 100,
                    ..PessimisticLockReq::default()
                })
                .expect("locks");
        };
        lock_at(&mut store, 100);
        assert_eq!(decode_lock(&store.lock_bytes(key)).hdr.for_update_ts, 100);
        lock_at(&mut store, 107);
        assert_eq!(decode_lock(&store.lock_bytes(key)).hdr.for_update_ts, 107);
        lock_at(&mut store, 93);
        assert_eq!(
            decode_lock(&store.lock_bytes(key)).hdr.for_update_ts,
            107,
            "a smaller for_update_ts never rolls the lock back"
        );
    }

    const MAX_TS: u64 = u64::MAX;

    fn must_commit(store: &mut MvccStore, key: &[u8], start_ts: u64, commit_ts: u64) {
        store
            .commit(&[key.to_vec()], start_ts, commit_ts)
            .expect("commits");
    }

    fn must_get_val(store: &MvccStore, key: &[u8], value: &[u8], ts: u64) {
        assert_eq!(store.get(key, ts).expect("readable"), Some(value.to_vec()));
    }

    fn must_get_none(store: &MvccStore, key: &[u8], ts: u64) {
        assert_eq!(store.get(key, ts).expect("readable"), None);
    }

    fn must_get_err(store: &MvccStore, key: &[u8], ts: u64) {
        assert!(store.get(key, ts).is_err(), "the lock must refuse ts {ts}");
    }

    fn must_acquire_pessimistic_lock(
        store: &mut MvccStore,
        pk: &[u8],
        key: &[u8],
        start_ts: u64,
        for_update_ts: u64,
    ) {
        store
            .pessimistic_lock(&PessimisticLockReq {
                mutations: vec![mutation(KvrpcOp::PessimisticLock, key, b"")],
                primary_lock: pk.to_vec(),
                start_version: start_ts,
                for_update_ts,
                lock_ttl: 100,
                ..PessimisticLockReq::default()
            })
            .expect("locks");
    }

    fn must_prewrite_pessimistic(
        store: &mut MvccStore,
        pk: &[u8],
        m: KvrpcMutation,
        start_ts: u64,
        ttl: u64,
        for_update_ts: u64,
    ) {
        store
            .prewrite(&PrewriteReq {
                mutations: vec![m],
                primary_lock: pk.to_vec(),
                start_version: start_ts,
                lock_ttl: ttl,
                for_update_ts,
                pessimistic_actions: vec![tidb_proto::KvrpcPessimisticAction::DoPessimisticCheck],
                ..PrewriteReq::default()
            })
            .expect("the pessimistic prewrite lands");
    }

    /// Go `TestTxnPrewrite` (`mvcc_test.go:1027`): retries are idempotent,
    /// conflicts refuse, a rollback record kills the SAME ts's retry but not
    /// a later transaction.
    #[test]
    fn test_txn_prewrite() {
        let mut store = MvccStore::new();
        let (k, v) = (b"tk".as_slice(), b"v".as_slice());
        must_prewrite_op(&mut store, k, mutation(KvrpcOp::Put, k, v), 5);
        assert!(!store.lock_bytes(k).is_empty(), "locked");
        // Retry prewrite: idempotent.
        must_prewrite_op(&mut store, k, mutation(KvrpcOp::Put, k, v), 5);
        // Conflict with the standing lock.
        assert!(store
            .prewrite(&PrewriteReq {
                mutations: vec![mutation(KvrpcOp::Put, k, v)],
                primary_lock: k.to_vec(),
                start_version: 6,
                lock_ttl: 100,
                ..PrewriteReq::default()
            })
            .is_err());
        must_commit(&mut store, k, 5, 10);
        must_get_val(&store, k, v, 10);
        // Delayed prewrite after committing.
        assert!(store
            .prewrite(&PrewriteReq {
                mutations: vec![mutation(KvrpcOp::Put, k, v)],
                primary_lock: k.to_vec(),
                start_version: 5,
                lock_ttl: 100,
                ..PrewriteReq::default()
            })
            .is_err());
        assert!(store.lock_bytes(k).is_empty(), "unlocked");
        // Write conflict below the commit.
        assert!(store
            .prewrite(&PrewriteReq {
                mutations: vec![mutation(KvrpcOp::Put, k, v)],
                primary_lock: k.to_vec(),
                start_version: 6,
                lock_ttl: 100,
                ..PrewriteReq::default()
            })
            .is_err());
        assert!(store.lock_bytes(k).is_empty(), "unlocked");
        // Not a conflict above it.
        must_prewrite_op(&mut store, k, mutation(KvrpcOp::Lock, k, b""), 12);
        store.rollback(&[k.to_vec()], 12).expect("rolls back");
        // Cannot retry the SAME ts past its rollback record.
        assert!(store
            .prewrite(&PrewriteReq {
                mutations: vec![mutation(KvrpcOp::Put, k, b"")],
                primary_lock: k.to_vec(),
                start_version: 12,
                lock_ttl: 100,
                ..PrewriteReq::default()
            })
            .is_err());
        // A LATER transaction can.
        must_prewrite_op(&mut store, k, mutation(KvrpcOp::Del, k, b""), 13);
        store.rollback(&[k.to_vec()], 13).expect("rolls back");
        assert!(store.lock_bytes(k).is_empty(), "unlocked");
    }

    /// Go `TestPrewriteInsert` (`mvcc_test.go:1061`): Op_Insert refuses over
    /// a live value, is admitted over a delete, and a rollback record does
    /// not resurrect the deleted value.
    #[test]
    fn test_prewrite_insert() {
        let mut store = MvccStore::new();
        let (k1, v1, v2, v3) = (
            b"tk1".as_slice(),
            b"v1".as_slice(),
            b"v2".as_slice(),
            b"v3".as_slice(),
        );
        let insert = |store: &mut MvccStore, value: &[u8], ts: u64| {
            store.prewrite(&PrewriteReq {
                mutations: vec![mutation(KvrpcOp::Insert, k1, value)],
                primary_lock: k1.to_vec(),
                start_version: ts,
                lock_ttl: 100,
                ..PrewriteReq::default()
            })
        };
        must_prewrite_op(&mut store, k1, mutation(KvrpcOp::Put, k1, v1), 1);
        must_commit(&mut store, k1, 1, 2);
        assert!(
            matches!(
                insert(&mut store, v2, 3),
                Err(KvError::KeyAlreadyExists { .. })
            ),
            "insert over a live value is AlreadyExist"
        );
        must_prewrite_op(&mut store, k1, mutation(KvrpcOp::Del, k1, b""), 4);
        must_commit(&mut store, k1, 4, 5);
        insert(&mut store, v2, 6).expect("insert over a delete");
        must_commit(&mut store, k1, 6, 7);
        must_prewrite_op(&mut store, k1, mutation(KvrpcOp::Put, k1, v3), 8);
        store.rollback(&[k1.to_vec()], 8).expect("rolls back");
        assert!(matches!(
            insert(&mut store, v2, 9),
            Err(KvError::KeyAlreadyExists { .. })
        ));
        must_prewrite_op(&mut store, k1, mutation(KvrpcOp::Del, k1, b""), 10);
        must_commit(&mut store, k1, 10, 11);
        must_prewrite_op(&mut store, k1, mutation(KvrpcOp::Put, k1, v3), 12);
        store.rollback(&[k1.to_vec()], 12).expect("rolls back");
        insert(&mut store, v2, 13).expect("the rollback does not resurrect");
        must_commit(&mut store, k1, 13, 14);
        must_get_val(&store, k1, v2, 15);
    }

    /// Go `TestPessimiticTxnTTL` (typo theirs, `mvcc_test.go:531`): the
    /// pessimistic prewrite keeps the LARGER of the lock's TTL and its own.
    #[test]
    fn test_pessimistic_txn_ttl() {
        let mut store = MvccStore::new();
        let (key1, val1) = (b"key1".as_slice(), b"val1".as_slice());
        store
            .pessimistic_lock(&PessimisticLockReq {
                mutations: vec![mutation(KvrpcOp::PessimisticLock, key1, b"")],
                primary_lock: key1.to_vec(),
                start_version: 1,
                for_update_ts: 1,
                lock_ttl: 1000,
                ..PessimisticLockReq::default()
            })
            .expect("locks");
        must_prewrite_pessimistic(
            &mut store,
            key1,
            mutation(KvrpcOp::Put, key1, val1),
            1,
            500,
            1,
        );
        assert_eq!(
            u64::from(decode_lock(&store.lock_bytes(key1)).hdr.ttl),
            1000
        );

        let (key2, val2) = (b"key2".as_slice(), b"val2".as_slice());
        store
            .pessimistic_lock(&PessimisticLockReq {
                mutations: vec![mutation(KvrpcOp::PessimisticLock, key2, b"")],
                primary_lock: key2.to_vec(),
                start_version: 3,
                for_update_ts: 3,
                lock_ttl: 300,
                ..PessimisticLockReq::default()
            })
            .expect("locks");
        must_prewrite_pessimistic(
            &mut store,
            key2,
            mutation(KvrpcOp::Put, key2, val2),
            3,
            2000,
            3,
        );
        assert_eq!(
            u64::from(decode_lock(&store.lock_bytes(key2)).hdr.ttl),
            2000
        );
    }

    /// Go `TestBatchGet` (`mvcc_test.go:1439`): a locked key answers its
    /// error IN the pair while its neighbors answer their values.
    #[test]
    fn test_batch_get() {
        let mut store = MvccStore::new();
        for (key, value) in [
            (b"ta".as_slice(), b"1".as_slice()),
            (b"tb", b"2"),
            (b"tc", b"3"),
        ] {
            must_prewrite_op(&mut store, key, mutation(KvrpcOp::Put, key, value), 100);
            must_commit(&mut store, key, 100, 101);
        }
        must_prewrite_op(&mut store, b"ta", mutation(KvrpcOp::Put, b"ta", b"0"), 103);
        let pairs = store.batch_get(&[b"ta".to_vec(), b"tb".to_vec(), b"tc".to_vec()], 104);
        assert_eq!(pairs.len(), 3);
        assert!(pairs[0].error.is_some(), "the locked key answers its error");
        assert_eq!(pairs[1].value, b"2");
        assert_eq!(pairs[2].value, b"3");
    }

    /// Go `TestCommitPessimisticLock` (`mvcc_test.go:1451`): a foreign ts
    /// cannot commit the lock; its own can, and Op_Lock leaves no value.
    #[test]
    fn test_commit_pessimistic_lock() {
        let mut store = MvccStore::new();
        let k = b"ta".as_slice();
        must_acquire_pessimistic_lock(&mut store, k, k, 10, 10);
        assert!(store.commit(&[k.to_vec()], 20, 30).is_err());
        must_commit(&mut store, k, 10, 20);
        must_get_none(&store, k, 30);
    }

    /// Go `TestOpCheckNotExist` (`mvcc_test.go:1460`): the check reads the
    /// latest STATE — live is AlreadyExist, deleted or rolled-back is ok.
    #[test]
    fn test_op_check_not_exist() {
        let mut store = MvccStore::new();
        let (k, v) = (b"ta".as_slice(), b"v".as_slice());
        let check = |store: &mut MvccStore, ts: u64| {
            store.prewrite(&PrewriteReq {
                mutations: vec![mutation(KvrpcOp::CheckNotExists, k, b"")],
                primary_lock: k.to_vec(),
                start_version: ts,
                lock_ttl: 100,
                ..PrewriteReq::default()
            })
        };
        must_prewrite_op(&mut store, k, mutation(KvrpcOp::Put, k, v), 1);
        must_commit(&mut store, k, 1, 2);
        assert!(matches!(
            check(&mut store, 3),
            Err(KvError::KeyAlreadyExists { .. })
        ));
        must_prewrite_op(&mut store, k, mutation(KvrpcOp::Del, k, b""), 4);
        must_commit(&mut store, k, 4, 5);
        check(&mut store, 6).expect("a deleted key checks clean");
        must_prewrite_op(&mut store, k, mutation(KvrpcOp::Put, k, v), 7);
        store.rollback(&[k.to_vec()], 7).expect("rolls back");
        check(&mut store, 8).expect("a rolled-back write checks clean");
    }

    /// Go `TestPrimaryKeyOpLock` (`mvcc_test.go:907`): every Op_Lock commit
    /// leaves a recallable commit record while the VALUE timeline ignores
    /// the locks entirely.
    #[test]
    fn test_primary_key_op_lock() {
        let mut store = MvccStore::new();
        let (pk, val2) = (b"tpk".as_slice(), b"val2".as_slice());
        must_prewrite_op(&mut store, pk, mutation(KvrpcOp::Lock, pk, b""), 100);
        must_commit(&mut store, pk, 100, 101);
        let recall = |store: &mut MvccStore, lock_ts: u64| {
            store
                .check_txn_status(&CheckTxnStatusReq {
                    primary_key: pk.to_vec(),
                    lock_ts,
                    caller_start_ts: 130,
                    current_ts: 130,
                    ..CheckTxnStatusReq::default()
                })
                .expect("the status answers")
                .commit_ts
        };
        assert_eq!(recall(&mut store, 100), 101);

        must_prewrite_op(&mut store, pk, mutation(KvrpcOp::Put, pk, val2), 110);
        must_commit(&mut store, pk, 110, 111);
        must_prewrite_op(&mut store, pk, mutation(KvrpcOp::Lock, pk, b""), 120);
        must_commit(&mut store, pk, 120, 121);

        assert_eq!(recall(&mut store, 120), 121);
        assert_eq!(recall(&mut store, 110), 111);
        assert_eq!(recall(&mut store, 100), 101);

        must_get_none(&store, pk, 90);
        must_get_none(&store, pk, 110);
        must_get_val(&store, pk, val2, 111);
        must_get_val(&store, pk, val2, 130);
    }

    /// Go `TestMvccTxnRead` (`mvcc_test.go:957`), the read-visibility suite,
    /// including the max-ts quirks: reading at `u64::MAX` IGNORES the
    /// primary lock but ERRORS on a secondary, and sees past a pessimistic
    /// transaction's pre-commit lock.
    #[test]
    fn test_mvcc_txn_read() {
        let mut store = MvccStore::new();
        let (k1, v1) = (b"tk1".as_slice(), b"v1".as_slice());
        must_get_none(&store, k1, 1);

        must_prewrite_optimistic(&mut store, k1, k1, v1, 2, 10);
        store.rollback(&[k1.to_vec()], 2).expect("rolls back");
        must_get_none(&store, k1, 1);

        must_prewrite_op(&mut store, k1, mutation(KvrpcOp::Lock, k1, b""), 3);
        must_commit(&mut store, k1, 3, 4);
        must_get_none(&store, k1, 5);

        let (v, k2, v2) = (b"v".as_slice(), b"tk2".as_slice(), b"v2".as_slice());
        must_prewrite_op(&mut store, k1, mutation(KvrpcOp::Put, k1, v), 5);
        must_prewrite_op(&mut store, k1, mutation(KvrpcOp::Put, k2, v2), 5);
        must_get_none(&store, k1, 4);
        must_get_err(&store, k1, 7);
        // The max-ts split: the primary lock is ignored, the secondary errors.
        must_get_none(&store, k1, MAX_TS);
        must_get_err(&store, k2, MAX_TS);
        must_commit(&mut store, k1, 5, 10);
        must_commit(&mut store, k2, 5, 10);
        must_get_none(&store, k1, 3);
        must_get_none(&store, k1, 7);
        must_get_val(&store, k1, v, 13);
        must_get_val(&store, k2, v2, MAX_TS);

        must_prewrite_op(&mut store, k1, mutation(KvrpcOp::Del, k1, b""), 15);
        // The pending delete's primary lock is ignored at max ts.
        must_get_val(&store, k1, v, MAX_TS);
        must_commit(&mut store, k1, 15, 20);
        must_get_none(&store, k1, 3);
        must_get_none(&store, k1, 7);
        must_get_val(&store, k1, v, 13);
        must_get_val(&store, k1, v, 17);
        must_get_none(&store, k1, 23);

        // Intersecting pessimistic timestamps: T1(25..27), T2(23..31).
        must_prewrite_op(&mut store, k1, mutation(KvrpcOp::Put, k1, v), 25);
        must_commit(&mut store, k1, 25, 27);
        must_acquire_pessimistic_lock(&mut store, k1, k1, 23, 29);
        must_get_val(&store, k1, v, 30);
        must_prewrite_pessimistic(&mut store, k1, mutation(KvrpcOp::Del, k1, b""), 23, 100, 29);
        must_get_err(&store, k1, 30);
        // Max ts sees past even a lock whose start_ts is BELOW the latest
        // commit.
        must_get_val(&store, k1, v, MAX_TS);
        must_commit(&mut store, k1, 23, 31);
        must_get_val(&store, k1, v, 30);
        must_get_none(&store, k1, 32);
    }

    /// Go `TestPessimisticLockForce` (`mvcc_test.go:1476`): Force locks past
    /// a NEWER commit — the write conflict every ordinary acquisition would
    /// die on — answers that commit's value, and the transaction then
    /// prewrites and commits over it.
    #[test]
    fn test_pessimistic_lock_force() {
        let mut store = MvccStore::new();
        let (k, v, v2) = (b"ta".as_slice(), b"v".as_slice(), b"v2".as_slice());
        must_prewrite_op(&mut store, k, mutation(KvrpcOp::Put, k, v), 5);
        must_commit(&mut store, k, 5, 10);
        let result = store
            .pessimistic_lock(&PessimisticLockReq {
                mutations: vec![mutation(KvrpcOp::PessimisticLock, k, b"")],
                primary_lock: k.to_vec(),
                start_version: 1,
                for_update_ts: 1,
                lock_ttl: 100,
                force: true,
                ..PessimisticLockReq::default()
            })
            .expect("force locks past the newer commit");
        assert_eq!(result.value, v);
        assert_eq!(result.commit_ts, 10);
        let lock = decode_lock(&store.lock_bytes(k));
        assert_eq!(
            lock.hdr.op,
            KvrpcOp::PessimisticLock as i32 as u8,
            "the granted lock is pessimistic"
        );
        must_prewrite_pessimistic(&mut store, k, mutation(KvrpcOp::Put, k, v2), 1, 100, 10);
        must_commit(&mut store, k, 1, 11);
        assert!(store.lock_bytes(k).is_empty(), "unlocked");
        must_get_val(&store, k, v2, 13);
    }

    /// Go `TestResolveCommit` (`mvcc_test.go:1375`), the reachable arc: a
    /// foreign-ts resolve leaves the secondary lock standing, the right ts
    /// commits it, and neither a lock-not-found nor a replaced error leaks
    /// from re-commits around a NEWER transaction's lock. (The tail that
    /// deletes a committed version with raw badger surgery is engine
    /// surgery this substituted engine does not expose; the recommit error
    /// path it exercises is engine-specific and stays with badger.)
    #[test]
    fn test_resolve_commit() {
        let mut store = MvccStore::new();
        let (pk, v, sk) = (b"tpk".as_slice(), b"v".as_slice(), b"tsk".as_slice());
        must_acquire_pessimistic_lock(&mut store, pk, pk, 1, 1);
        must_acquire_pessimistic_lock(&mut store, pk, sk, 1, 1);
        must_prewrite_pessimistic(&mut store, pk, mutation(KvrpcOp::Put, pk, v), 1, 100, 1);
        must_prewrite_pessimistic(&mut store, pk, mutation(KvrpcOp::Put, sk, v), 1, 100, 1);

        must_commit(&mut store, pk, 1, 2);
        // A resolve naming the WRONG start ts leaves the lock standing.
        store
            .resolve_lock(&[sk.to_vec()], 2, 3)
            .expect("the foreign resolve is a no-op");
        assert!(!store.lock_bytes(sk).is_empty(), "still locked");
        store
            .resolve_lock(&[sk.to_vec()], 1, 2)
            .expect("the right ts resolves");
        // Re-commit reports neither lock-not-found nor replaced.
        must_commit(&mut store, sk, 1, 2);

        let (k2, v2) = (b"tk2".as_slice(), b"v2".as_slice());
        must_acquire_pessimistic_lock(&mut store, k2, k2, 3, 3);
        must_commit(&mut store, sk, 1, 2);
        must_prewrite_pessimistic(&mut store, k2, mutation(KvrpcOp::Put, k2, v2), 3, 100, 3);
        must_commit(&mut store, k2, 3, 4);
    }

    // Go `PrewriteOptimisticWithAssertion` (`mvcc_test.go:164`), narrowed to
    // the arguments `TestAssertion` actually varies (minCommitTs 0, no async
    // commit, no secondaries).
    #[allow(clippy::too_many_arguments)]
    fn prewrite_optimistic_with_assertion(
        store: &mut MvccStore,
        pk: &[u8],
        key: &[u8],
        value: &[u8],
        start_ts: u64,
        ttl: u64,
        assertion: tidb_proto::KvrpcAssertion,
        level: tidb_proto::KvrpcAssertionLevel,
    ) -> Result<(), KvError> {
        let mut m = mutation(KvrpcOp::Put, key, value);
        m.assertion = assertion as i32;
        store.prewrite(&PrewriteReq {
            mutations: vec![m],
            primary_lock: pk.to_vec(),
            start_version: start_ts,
            lock_ttl: ttl,
            assertion_level: level as i32,
            ..PrewriteReq::default()
        })
    }

    // Go `PrewritePessimisticWithAssertion` (`mvcc_test.go:194`): each
    // `isPessimisticLock` flag becomes DO_ or SKIP_PESSIMISTIC_CHECK.
    #[allow(clippy::too_many_arguments)]
    fn prewrite_pessimistic_with_assertion(
        store: &mut MvccStore,
        pk: &[u8],
        key: &[u8],
        value: &[u8],
        start_ts: u64,
        ttl: u64,
        is_pessimistic_lock: &[bool],
        for_update_ts: u64,
        assertion: tidb_proto::KvrpcAssertion,
        level: tidb_proto::KvrpcAssertionLevel,
    ) -> Result<(), KvError> {
        use tidb_proto::KvrpcPessimisticAction;
        let mut m = mutation(KvrpcOp::Put, key, value);
        m.assertion = assertion as i32;
        store.prewrite(&PrewriteReq {
            mutations: vec![m],
            primary_lock: pk.to_vec(),
            start_version: start_ts,
            lock_ttl: ttl,
            for_update_ts,
            pessimistic_actions: is_pessimistic_lock
                .iter()
                .map(|is_lock| {
                    if *is_lock {
                        KvrpcPessimisticAction::DoPessimisticCheck
                    } else {
                        KvrpcPessimisticAction::SkipPessimisticCheck
                    }
                })
                .collect(),
            assertion_level: level as i32,
            ..PrewriteReq::default()
        })
    }

    /// Go `TestAssertion` (`mvcc_test.go:1698`): with a non-Off level every
    /// prewrite flavor (optimistic, pessimistic-locked, pessimistic
    /// non-locked) refuses a wrong Exist/NotExist assertion with the
    /// existing version's timestamps; `AssertionLevel_Off` passes them all;
    /// correct assertions pass at `Strict`.
    #[test]
    fn test_assertion() {
        use tidb_proto::{KvrpcAssertion, KvrpcAssertionLevel};
        let mut store = MvccStore::new();
        for key in [b"k1".as_slice(), b"k2", b"k3"] {
            must_prewrite_optimistic(&mut store, b"k1", key, b"v", 1, 100);
            must_commit(&mut store, key, 1, 2);
        }

        #[allow(clippy::too_many_arguments)]
        fn check(
            result: Result<(), KvError>,
            disable: bool,
            start_ts: u64,
            key: &[u8],
            assertion: KvrpcAssertion,
            existing_start_ts: u64,
            existing_commit_ts: u64,
        ) {
            if disable {
                result.expect("AssertionLevel_Off must not check");
                return;
            }
            assert_eq!(
                result.expect_err("the assertion must fail"),
                KvError::AssertionFailed {
                    start_ts,
                    key: key.to_vec(),
                    assertion: assertion as i32,
                    existing_start_ts,
                    existing_commit_ts,
                }
            );
        }

        for disable in [false, true] {
            let level = if disable {
                KvrpcAssertionLevel::Off
            } else {
                KvrpcAssertionLevel::Strict
            };
            // Optimistic.
            let err = prewrite_optimistic_with_assertion(
                &mut store,
                b"k1",
                b"k1",
                b"v1",
                10,
                100,
                KvrpcAssertion::NotExist,
                level,
            );
            check(err, disable, 10, b"k1", KvrpcAssertion::NotExist, 1, 2);
            let err = prewrite_optimistic_with_assertion(
                &mut store,
                b"k11",
                b"k11",
                b"v11",
                10,
                100,
                KvrpcAssertion::Exist,
                level,
            );
            check(err, disable, 10, b"k11", KvrpcAssertion::Exist, 0, 0);

            // Pessimistic, over a pessimistic lock.
            must_acquire_pessimistic_lock(&mut store, b"k2", b"k2", 10, 10);
            let err = prewrite_pessimistic_with_assertion(
                &mut store,
                b"k2",
                b"k2",
                b"v2",
                10,
                100,
                &[true],
                10,
                KvrpcAssertion::NotExist,
                level,
            );
            check(err, disable, 10, b"k2", KvrpcAssertion::NotExist, 1, 2);
            must_acquire_pessimistic_lock(&mut store, b"k22", b"k22", 10, 10);
            let err = prewrite_pessimistic_with_assertion(
                &mut store,
                b"k22",
                b"k22",
                b"v22",
                10,
                100,
                &[true],
                10,
                KvrpcAssertion::Exist,
                level,
            );
            check(err, disable, 10, b"k22", KvrpcAssertion::Exist, 0, 0);

            // Pessimistic, non-pessimistic-lock key.
            let err = prewrite_pessimistic_with_assertion(
                &mut store,
                b"pk",
                b"k3",
                b"v3",
                10,
                100,
                &[false],
                10,
                KvrpcAssertion::NotExist,
                level,
            );
            check(err, disable, 10, b"k3", KvrpcAssertion::NotExist, 1, 2);
            let err = prewrite_pessimistic_with_assertion(
                &mut store,
                b"pk",
                b"k33",
                b"v33",
                10,
                100,
                &[false],
                10,
                KvrpcAssertion::Exist,
                level,
            );
            check(err, disable, 10, b"k33", KvrpcAssertion::Exist, 0, 0);
        }

        for key in [b"k1".as_slice(), b"k11", b"k2", b"k22", b"k3", b"k33"] {
            store.rollback(&[key.to_vec()], 10).expect("rolls back");
        }

        // Correct assertions pass at Strict, on every flavor.
        prewrite_optimistic_with_assertion(
            &mut store,
            b"k1",
            b"k1",
            b"v1",
            20,
            100,
            KvrpcAssertion::Exist,
            KvrpcAssertionLevel::Strict,
        )
        .expect("k1 exists");
        prewrite_optimistic_with_assertion(
            &mut store,
            b"k11",
            b"k11",
            b"v11",
            20,
            100,
            KvrpcAssertion::NotExist,
            KvrpcAssertionLevel::Strict,
        )
        .expect("k11 does not exist");
        must_acquire_pessimistic_lock(&mut store, b"k2", b"k2", 20, 10);
        prewrite_pessimistic_with_assertion(
            &mut store,
            b"k2",
            b"k2",
            b"v2",
            20,
            100,
            &[true],
            10,
            KvrpcAssertion::Exist,
            KvrpcAssertionLevel::Strict,
        )
        .expect("k2 exists");
        must_acquire_pessimistic_lock(&mut store, b"k22", b"k22", 20, 10);
        prewrite_pessimistic_with_assertion(
            &mut store,
            b"k22",
            b"k22",
            b"v22",
            20,
            100,
            &[true],
            10,
            KvrpcAssertion::NotExist,
            KvrpcAssertionLevel::Strict,
        )
        .expect("k22 does not exist");
        prewrite_pessimistic_with_assertion(
            &mut store,
            b"pk",
            b"k3",
            b"v3",
            20,
            100,
            &[false],
            10,
            KvrpcAssertion::Exist,
            KvrpcAssertionLevel::Strict,
        )
        .expect("k3 exists");
        prewrite_pessimistic_with_assertion(
            &mut store,
            b"pk",
            b"k33",
            b"v33",
            20,
            100,
            &[false],
            10,
            KvrpcAssertion::NotExist,
            KvrpcAssertionLevel::Strict,
        )
        .expect("k33 does not exist");
    }

    // Go `MustLoad` (`mvcc_test.go`): committed `key:value` pairs land at
    // `(startTS, commitTS)`. Go writes the db directly; prewrite + commit
    // produces the identical committed state through the ported surface.
    fn must_load(store: &mut MvccStore, start_ts: u64, commit_ts: u64, rows: &[&str]) {
        for row in rows {
            let (key, value) = row.split_once(':').expect("key:value row");
            must_prewrite_optimistic(
                store,
                key.as_bytes(),
                key.as_bytes(),
                value.as_bytes(),
                start_ts,
                100,
            );
            must_commit(store, key.as_bytes(), start_ts, commit_ts);
        }
    }

    // Go `kvGet` (`mvcc_test.go`): a point read under an explicit
    // resolved/committed context, snapshot isolation.
    fn kv_get(
        store: &MvccStore,
        key: &[u8],
        version: u64,
        resolved: &[u64],
        committed: &[u64],
    ) -> Result<Option<Vec<u8>>, KvError> {
        store.get_with(
            &ReadContext {
                isolation_level: tidb_proto::KvrpcIsolationLevel::Si as i32,
                resolved_locks: resolved.to_vec(),
                committed_locks: committed.to_vec(),
            },
            key,
            version,
        )
    }

    /// Go `TestAccessCommittedLocks` (`mvcc_test.go:1552`): a resolved lock
    /// is ignored, a committed lock answers with the LOCK'S OWN value (nil
    /// for a delete), an unlisted lock still blocks — through get, batch
    /// get, and scan.
    #[test]
    fn test_access_committed_locks() {
        let mut store = MvccStore::new();
        let (k0, v0) = (b"t0".as_slice(), b"v0".as_slice());
        must_load(&mut store, 10, 20, &["t0:v0"]);
        // Delete prewrite at 30.
        must_prewrite_op(&mut store, k0, mutation(KvrpcOp::Del, k0, b""), 30);
        must_get_err(&store, k0, 40);
        // Meet lock.
        assert!(kv_get(&store, k0, 40, &[20], &[]).is_err());
        assert!(kv_get(&store, k0, 40, &[20], &[20]).is_err());
        // Ignore lock.
        assert_eq!(
            kv_get(&store, k0, 40, &[30], &[]).expect("resolved lock is ignored"),
            Some(v0.to_vec())
        );
        // Access lock: the delete lock's value is nil.
        assert_eq!(
            kv_get(&store, k0, 40, &[], &[30]).expect("committed lock answers"),
            None
        );

        let (k1, v1) = (b"t1".as_slice(), b"v1".as_slice());
        must_prewrite_op(&mut store, k1, mutation(KvrpcOp::Put, k1, v1), 50);
        // Ignore lock: nothing committed underneath.
        assert_eq!(
            kv_get(&store, k1, 60, &[50], &[]).expect("resolved lock is ignored"),
            None
        );
        // Access lock: the put lock's value answers.
        assert_eq!(
            kv_get(&store, k1, 60, &[], &[50]).expect("committed lock answers"),
            Some(v1.to_vec())
        );

        // Locked (listed in neither set).
        let (k2, v2) = (b"t2".as_slice(), b"v2".as_slice());
        must_prewrite_op(&mut store, k2, mutation(KvrpcOp::Put, k2, v2), 70);
        // Lock for ignore.
        let (k3, v3) = (b"t3".as_slice(), b"v3".as_slice());
        must_prewrite_op(&mut store, k3, mutation(KvrpcOp::Put, k3, v3), 80);
        // No lock.
        let (k4, v4) = (b"t4".as_slice(), b"v4".as_slice());
        must_load(&mut store, 80, 90, &["t4:v4"]);

        let keys: Vec<Vec<u8>> = [k0, k1, k2, k3, k4].map(<[u8]>::to_vec).to_vec();
        let expected: [(&[u8], &[u8], bool); 3] =
            [(k1, v1, false), (k2, b"", true), (k4, v4, false)];
        let ctx = ReadContext {
            isolation_level: tidb_proto::KvrpcIsolationLevel::Si as i32,
            resolved_locks: vec![80],
            committed_locks: vec![30, 50],
        };
        let pairs = store.batch_get_with(&ctx, &keys, 100);
        assert_eq!(pairs.len(), expected.len());
        for (pair, (key, value, is_err)) in pairs.iter().zip(expected) {
            assert_eq!(pair.key, key);
            assert_eq!(pair.value, value);
            assert_eq!(pair.error.is_some(), is_err, "{:?}", pair.key);
        }

        let pairs = store.scan_with(
            &ctx,
            &ScanReq {
                start_key: b"t0".to_vec(),
                end_key: b"t5".to_vec(),
                limit: 100,
                version: 100,
                ..ScanReq::default()
            },
        );
        assert_eq!(pairs.len(), expected.len());
        for (pair, (key, value, is_err)) in pairs.iter().zip(expected) {
            assert_eq!(pair.key, key);
            assert_eq!(pair.value, value);
            assert_eq!(pair.error.is_some(), is_err, "{:?}", pair.key);
        }
    }

    /// Go `TestTiKVRCRead` (`mvcc_test.go:1646`): `RC` isolation skips every
    /// lock check — standing locks are read through to the committed data —
    /// on get, batch get, and scan.
    #[test]
    fn test_tikv_rc_read() {
        let mut store = MvccStore::new();
        let k1 = b"t1".as_slice();
        let (k2, v2) = (b"t2".as_slice(), b"v2".as_slice());
        let (k3, v3) = (b"t3".as_slice(), b"v3".as_slice());
        let (k4, v4) = (b"t4".as_slice(), b"v4".as_slice());
        must_load(&mut store, 10, 20, &["t1:v1", "t2:v2", "t3:v3"]);
        // Write to be read.
        must_prewrite_op(&mut store, k1, mutation(KvrpcOp::Put, k1, b"v11"), 30);
        must_commit(&mut store, k1, 30, 40);
        // Locks to be ignored.
        must_prewrite_op(&mut store, k2, mutation(KvrpcOp::Put, k2, v2), 50);
        must_prewrite_op(&mut store, k3, mutation(KvrpcOp::Del, k3, b""), 60);
        must_prewrite_op(&mut store, k4, mutation(KvrpcOp::Put, k4, v4), 70);

        let expected: [(&[u8], Option<&[u8]>); 4] = [
            (k1, Some(b"v11")),
            (k2, Some(v2)),
            (k3, Some(v3)),
            (k4, None),
        ];
        let ctx = ReadContext {
            isolation_level: tidb_proto::KvrpcIsolationLevel::Rc as i32,
            ..ReadContext::default()
        };
        // Get.
        for (key, value) in expected {
            assert_eq!(
                store
                    .get_with(&ctx, key, 80)
                    .expect("RC never meets a lock"),
                value.map(<[u8]>::to_vec),
                "{key:?}"
            );
        }
        // Batch get: k4 has no committed value and is dropped.
        let pairs = store.batch_get_with(&ctx, &[k1, k2, k3, k4].map(<[u8]>::to_vec), 80);
        assert_eq!(pairs.len(), 3);
        for pair in &pairs {
            let (_, value) = expected
                .iter()
                .find(|(key, _)| *key == pair.key)
                .expect("a known key");
            assert!(pair.error.is_none());
            assert_eq!(pair.value, value.expect("only committed keys answer"));
        }
        // Scan.
        let pairs = store.scan_with(
            &ctx,
            &ScanReq {
                start_key: b"t1".to_vec(),
                end_key: b"t4".to_vec(),
                limit: 100,
                version: 80,
                ..ScanReq::default()
            },
        );
        assert_eq!(pairs.len(), 3);
        for pair in &pairs {
            let (_, value) = expected
                .iter()
                .find(|(key, _)| *key == pair.key)
                .expect("a known key");
            assert!(pair.error.is_none());
            assert_eq!(pair.value, value.expect("only committed keys answer"));
        }
    }

    // Go `getConflictErr` (`mvcc_test.go:1690`): the first conflict error
    // among the scan pairs.
    fn get_conflict_err(pairs: &[KvPair]) -> Option<&KvError> {
        pairs
            .iter()
            .filter_map(|pair| pair.error.as_deref())
            .find(|error| matches!(error, KvError::Conflict { .. }))
    }

    /// Go `TestRcReadCheckTS` (`mvcc_test.go:1614`): under `RCCheckTS` a
    /// point read serves only data committed at or before the read ts — a
    /// NEWER committed version is a conflict carrying its timestamps, a
    /// standing write lock is a conflict carrying the lock's start ts —
    /// and a scan (either direction) surfaces the same conflicts.
    #[test]
    fn test_rc_read_check_ts() {
        let mut store = MvccStore::new();
        let (k1, v1) = (b"tk1".as_slice(), b"v1".as_slice());
        must_prewrite_optimistic(&mut store, k1, k1, v1, 1, 100);
        must_commit(&mut store, k1, 1, 2);
        let (k2, v2) = (b"tk2".as_slice(), b"v2".as_slice());
        must_prewrite_optimistic(&mut store, k2, k2, v2, 5, 100);
        must_commit(&mut store, k2, 5, 6);
        let (k3, v3) = (b"tk3".as_slice(), b"v3".as_slice());
        must_prewrite_optimistic(&mut store, k3, k3, v3, 10, 100);

        let ctx = ReadContext {
            isolation_level: tidb_proto::KvrpcIsolationLevel::RcCheckTs as i32,
            ..ReadContext::default()
        };
        assert_eq!(
            store.get_with(&ctx, k1, 3).expect("committed at 2 serves"),
            Some(v1.to_vec())
        );
        // A version committed PAST the read ts is a conflict with its
        // timestamps.
        let err = store.get_with(&ctx, k2, 3).expect_err("commit 6 > read 3");
        let KvError::Conflict {
            start_ts: 3,
            conflict_ts: 5,
            conflict_commit_ts: 6,
            ..
        } = err
        else {
            panic!("wrong conflict shape: {err:?}");
        };
        // A standing write lock is a conflict with the lock's start ts.
        let err = store.get_with(&ctx, k3, 3).expect_err("locked at 10");
        let KvError::Conflict {
            start_ts: 3,
            conflict_ts: 10,
            ..
        } = err
        else {
            panic!("wrong conflict shape: {err:?}");
        };

        let scan = |store: &MvccStore, version: u64, reverse: bool| {
            store.scan_with(
                &ctx,
                &ScanReq {
                    start_key: b"a".to_vec(),
                    end_key: b"z".to_vec(),
                    limit: 100,
                    version,
                    reverse,
                    ..ScanReq::default()
                },
            )
        };
        // The error is reported from the more recent version.
        let pairs = scan(&store, 3, false);
        let Some(KvError::Conflict {
            start_ts: 3,
            conflict_ts: 5,
            conflict_commit_ts: 6,
            ..
        }) = get_conflict_err(&pairs)
        else {
            panic!("wrong scan conflict: {pairs:?}");
        };
        // The error is reported from the lock.
        let pairs = scan(&store, 15, false);
        let Some(KvError::Conflict {
            start_ts: 15,
            conflict_ts: 10,
            ..
        }) = get_conflict_err(&pairs)
        else {
            panic!("wrong scan conflict: {pairs:?}");
        };
        // Reverse scans surface the same conflicts.
        assert!(get_conflict_err(&scan(&store, 3, true)).is_some());
        assert!(get_conflict_err(&scan(&store, 15, true)).is_some());
    }

    // Go `MustPrewriteOptimisticAsyncCommit` (`mvcc_test.go:127`).
    #[allow(clippy::too_many_arguments)]
    fn must_prewrite_optimistic_async_commit(
        store: &mut MvccStore,
        pk: &[u8],
        key: &[u8],
        value: &[u8],
        start_ts: u64,
        ttl: u64,
        min_commit_ts: u64,
        secondaries: &[&[u8]],
    ) {
        store
            .prewrite(&PrewriteReq {
                mutations: vec![put(key, value)],
                primary_lock: pk.to_vec(),
                start_version: start_ts,
                lock_ttl: ttl,
                min_commit_ts,
                use_async_commit: true,
                secondaries: secondaries.iter().map(|s| s.to_vec()).collect(),
                ..PrewriteReq::default()
            })
            .expect("the async-commit prewrite lands");
    }

    // Go `MVCCStore.getLock`, the test's read of a standing lock.
    fn get_lock(store: &MvccStore, key: &[u8]) -> Lock {
        let buf = store.lock_bytes(key);
        assert!(!buf.is_empty(), "a lock must stand at {key:?}");
        decode_lock(&buf)
    }

    /// Go `TestAsyncCommitPrewrite` (`mvcc_test.go:1524`): an async-commit
    /// prewrite records the secondaries on the PRIMARY lock only, marks
    /// every lock `UseAsyncCommit`, and stamps each with a PD-drawn
    /// `MinCommitTS` greater than zero.
    #[test]
    fn test_async_commit_prewrite() {
        let mut store = MvccStore::with_pd(std::sync::Arc::new(crate::tso::Tso::new()));
        let (pk, pk_val) = (b"tpk".as_slice(), b"tpkVal".as_slice());
        let (sec_key1, sec_val1) = (b"tSecKey1".as_slice(), b"secVal1".as_slice());
        let (sec_key2, sec_val2) = (b"tSecKey2".as_slice(), b"secVal2".as_slice());

        must_prewrite_optimistic_async_commit(
            &mut store,
            pk,
            pk,
            pk_val,
            1,
            100,
            0,
            &[sec_key1, sec_key2],
        );
        must_prewrite_optimistic_async_commit(&mut store, pk, sec_key1, sec_val1, 1, 100, 0, &[]);
        must_prewrite_optimistic_async_commit(&mut store, pk, sec_key2, sec_val2, 1, 100, 0, &[]);

        let pk_lock = get_lock(&store, pk);
        assert_eq!(pk_lock.hdr.secondary_num, 2);
        assert_eq!(pk_lock.secondaries[0], sec_key1);
        assert_eq!(pk_lock.secondaries[1], sec_key2);
        assert!(pk_lock.hdr.use_async_commit);
        assert!(pk_lock.hdr.min_commit_ts > 0);

        let sec_lock = get_lock(&store, sec_key2);
        assert_eq!(sec_lock.hdr.secondary_num, 0);
        assert_eq!(sec_lock.secondaries.len(), 0);
        assert!(sec_lock.hdr.use_async_commit);
        assert!(sec_lock.hdr.min_commit_ts > 0);
        assert_eq!(sec_lock.value, sec_val2);
    }
}
