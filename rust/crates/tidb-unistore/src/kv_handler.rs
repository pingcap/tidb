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

//! Go `pkg/store/mockstore/unistore/tikv/server.go` — the KV request
//! handlers: proto requests in, proto responses out, errors converted into
//! the `kvrpcpb.KeyError` shapes a TiKV client parses.
//!
//! Every handler opens Go's `requestCtx` first: the request's region context
//! is checked against the store's region manager, and a store mismatch, an
//! unknown region or a stale epoch answers the region error TiKV would,
//! which the client retries on fresh regions. Reads are bounded by the
//! addressed region.
//!
//! Handlers here: the transactional set (get, scan, batch-get, prewrite,
//! commit, cleanup, batch-rollback, pessimistic lock/rollback, txn heartbeat,
//! check-txn-status, check-secondary-locks, resolve-lock, buffer batch-get),
//! the GC set (scan-lock, physical scan-lock, GC, delete-range, unsafe
//! destroy-range), MVCC debugging (by key, by start ts), split-region, the
//! coprocessor with its store-batch tasks, and Go's stub answers (lock
//! observers, store safe ts, lock-wait history, raw TTL, import).
//!
//! # Narrowings, by name
//!
//! * `Server.Stop` / `refCount` / `ErrRetryable("server is closed")`: the
//!   handler lives as long as its owner; there is no stopped state.
//! * `KvFlush` (`MVCCStore.Flush`, pipelined DML's bulk load), MPP, batch
//!   coprocessor, the raw KV and debug services, and the deadlock detector
//!   RPCs: not ported.
//! * `KvPessimisticLock`'s lock waiting: see `mvcc_store`'s header.
//! * `extractRegionError` / `pberror.PBError`: the store raises no
//!   region errors of its own, so `convertToPBError`'s region arm is vacuous.

use std::sync::Arc;

use tidb_proto::coprocessor;
use tidb_proto::errorpb;
use tidb_proto::kvrpcpb;
use tidb_proto::KvrpcKeyError;

use crate::mvcc_store::{
    CheckTxnStatusReq, KvError, MvccInfo, MvccStore, PessimisticLockReq, PrewriteReq, ReadContext,
    ScanReq,
};
use crate::region_manager::{InProcessRegionManager, RegionCtx};

/// Go `convertToKeyError` (`server.go:1174`): every store error becomes the
/// `KeyError` arm a client's resolver switches on; anything unrecognized is
/// Go's `default:` — an `Abort` carrying the rendered message.
#[must_use]
pub fn convert_to_key_error(err: &KvError) -> KvrpcKeyError {
    match err {
        KvError::Locked(info) => KvrpcKeyError {
            locked: Some((**info).clone()),
            ..KvrpcKeyError::default()
        },
        KvError::KeyAlreadyExists { key } => KvrpcKeyError {
            already_exist: Some(kvrpcpb::AlreadyExist { key: key.clone() }),
            ..KvrpcKeyError::default()
        },
        KvError::AssertionFailed {
            start_ts,
            key,
            assertion,
            existing_start_ts,
            existing_commit_ts,
        } => KvrpcKeyError {
            assertion_failed: Some(kvrpcpb::AssertionFailed {
                start_ts: *start_ts,
                key: key.clone(),
                assertion: *assertion,
                existing_start_ts: *existing_start_ts,
                existing_commit_ts: *existing_commit_ts,
            }),
            ..KvrpcKeyError::default()
        },
        KvError::Conflict {
            reason,
            start_ts,
            conflict_ts,
            conflict_commit_ts,
            key,
        } => KvrpcKeyError {
            conflict: Some(kvrpcpb::WriteConflict {
                start_ts: *start_ts,
                conflict_ts: *conflict_ts,
                conflict_commit_ts: *conflict_commit_ts,
                key: key.clone(),
                reason: *reason as i32,
                ..kvrpcpb::WriteConflict::default()
            }),
            ..KvrpcKeyError::default()
        },
        KvError::CommitExpire {
            start_ts,
            commit_ts,
            min_commit_ts,
            key,
        } => KvrpcKeyError {
            commit_ts_expired: Some(kvrpcpb::CommitTsExpired {
                start_ts: *start_ts,
                attempted_commit_ts: *commit_ts,
                key: key.clone(),
                min_commit_ts: *min_commit_ts,
            }),
            ..KvrpcKeyError::default()
        },
        KvError::TxnNotFound {
            primary_key,
            start_ts,
        } => KvrpcKeyError {
            txn_not_found: Some(kvrpcpb::TxnNotFound {
                start_ts: *start_ts,
                primary_key: primary_key.clone(),
            }),
            ..KvrpcKeyError::default()
        },
        KvError::PrimaryMismatch { lock, .. } => KvrpcKeyError {
            primary_mismatch: Some(kvrpcpb::PrimaryMismatch {
                lock_info: Some((**lock).clone()),
            }),
            ..KvrpcKeyError::default()
        },
        KvError::Deadlock {
            lock_key,
            lock_ts,
            deadlock_key_hash,
            wait_chain,
        } => KvrpcKeyError {
            deadlock: Some(kvrpcpb::Deadlock {
                lock_key: lock_key.clone(),
                lock_ts: *lock_ts,
                deadlock_key_hash: *deadlock_key_hash,
                wait_chain: wait_chain
                    .iter()
                    .map(|entry| kvrpcpb::WaitForEntry {
                        txn: entry.txn,
                        wait_for_txn: entry.wait_for_txn,
                        key_hash: entry.key_hash,
                        key: entry.key.clone(),
                        resource_group_tag: entry.resource_group_tag.clone(),
                        ..kvrpcpb::WaitForEntry::default()
                    })
                    .collect(),
                ..kvrpcpb::Deadlock::default()
            }),
            ..KvrpcKeyError::default()
        },
        // Go's `default:` arm: `ErrAlreadyRollback`, `ErrLockNotFound`,
        // `ErrReplaced`, and the rest render as an Abort message.
        KvError::AlreadyRollback => abort("txn already rollbacked"),
        KvError::LockNotFound => abort("lock not found"),
        KvError::Replaced => abort("replaced by another transaction"),
        KvError::AlreadyCommitted(_) => abort("txn already committed"),
        KvError::InvalidOp => abort("invalid operation"),
        KvError::PessimisticLockNotFound => abort("pessimistic lock not found"),
        KvError::LockTypeNotMatch => abort("lock type not match"),
        KvError::Unported(message) => abort(message),
        KvError::Message(message) => abort(message.as_str()),
    }
}

fn abort(message: &str) -> KvrpcKeyError {
    KvrpcKeyError {
        abort: message.to_owned(),
        ..KvrpcKeyError::default()
    }
}

/// Go `requestCtx`: what a handler learned about the region it serves.
struct RequestCtx {
    /// `regCtx`, when the region checks passed.
    reg_ctx: Option<RegionCtx>,
    /// `rpcCtx`.
    rpc_ctx: kvrpcpb::Context,
}

impl RequestCtx {
    /// `regCtx.RawStart()`.
    fn raw_start(&self) -> &[u8] {
        self.reg_ctx.as_ref().map_or(&[], RegionCtx::raw_start)
    }

    /// `regCtx.RawEnd()`.
    fn raw_end(&self) -> &[u8] {
        self.reg_ctx.as_ref().map_or(&[], RegionCtx::raw_end)
    }

    /// The read paths' slice of the request: Go reads the isolation level
    /// and the lock sets straight off `rpcCtx`, and bounds its reader by
    /// the region.
    fn read_context(&self) -> ReadContext {
        ReadContext {
            isolation_level: self.rpc_ctx.isolation_level,
            region_raw_start: self.raw_start().to_vec(),
            region_raw_end: self.raw_end().to_vec(),
            resolved_locks: self.rpc_ctx.resolved_locks.clone(),
            committed_locks: self.rpc_ctx.committed_locks.clone(),
        }
    }
}

/// What `KvPessimisticLock` leaves for the wait: the waiter parked on the
/// key and the lock it waits for.
#[derive(Debug)]
pub struct PessimisticLockWait {
    waiter: std::sync::Arc<crate::lockwaiter::Waiter>,
    locked: Box<tidb_proto::KvrpcLockInfo>,
}

impl PessimisticLockWait {
    /// The waiter to wait on.
    #[must_use]
    pub fn waiter(&self) -> &std::sync::Arc<crate::lockwaiter::Waiter> {
        &self.waiter
    }
}

/// Go's handler prologue: `newRequestCtx`, then `if reqCtx.regErr != nil
/// { return &Response{RegionError: reqCtx.regErr}, nil }`.
macro_rules! region_checked {
    ($self:ident, $req:ident, $response:ty) => {{
        let (req_ctx, region_error) = $self.new_request_ctx($req.context.as_ref());
        if let Some(region_error) = region_error {
            return <$response>::from_region_error(region_error);
        }
        req_ctx
    }};
}

/// A response carrying only a region error.
trait FromRegionError {
    fn from_region_error(region_error: errorpb::Error) -> Self;
}

macro_rules! from_region_error {
    ($($response:ty),* $(,)?) => {
        $(impl FromRegionError for $response {
            fn from_region_error(region_error: errorpb::Error) -> Self {
                Self {
                    region_error: Some(region_error),
                    ..Self::default()
                }
            }
        })*
    };
}

from_region_error!(
    kvrpcpb::GetResponse,
    kvrpcpb::ScanResponse,
    kvrpcpb::BatchGetResponse,
    kvrpcpb::PrewriteResponse,
    kvrpcpb::CommitResponse,
    kvrpcpb::CleanupResponse,
    kvrpcpb::BatchRollbackResponse,
    kvrpcpb::PessimisticLockResponse,
    kvrpcpb::PessimisticRollbackResponse,
    kvrpcpb::TxnHeartBeatResponse,
    kvrpcpb::CheckTxnStatusResponse,
    kvrpcpb::CheckSecondaryLocksResponse,
    kvrpcpb::ResolveLockResponse,
    kvrpcpb::ScanLockResponse,
    kvrpcpb::DeleteRangeResponse,
    kvrpcpb::BufferBatchGetResponse,
    kvrpcpb::MvccGetByKeyResponse,
    kvrpcpb::MvccGetByStartTsResponse,
    coprocessor::Response,
);

/// Go `Server`: the KV service over the embedded store and its regions.
#[derive(Debug)]
pub struct KvHandler {
    /// Go `mvccStore`.
    pub store: MvccStore,
    /// Go `regionManager`.
    pub regions: Arc<InProcessRegionManager>,
}

impl Default for KvHandler {
    /// A fresh store over Go's single-store bootstrap.
    fn default() -> Self {
        Self::new(MvccStore::new(), Arc::new(InProcessRegionManager::new()))
    }
}

impl KvHandler {
    /// Go `NewServer(rm, pdClient, store, innerServer)`.
    #[must_use]
    pub fn new(store: MvccStore, regions: Arc<InProcessRegionManager>) -> Self {
        Self { store, regions }
    }

    /// Go `newRequestCtx`: the region and store checks every handler opens
    /// with; the second half is `reqCtx.regErr`.
    fn new_request_ctx(
        &self,
        ctx: Option<&kvrpcpb::Context>,
    ) -> (RequestCtx, Option<errorpb::Error>) {
        let rpc_ctx = ctx.cloned().unwrap_or_default();
        let (reg_ctx, mut reg_err) = match self.regions.get_region_from_ctx(&rpc_ctx) {
            Ok(region) => (Some(region), None),
            Err(region_error) => (None, Some(region_error)),
        };
        if let Err(region_error) = self.regions.get_store_info_from_ctx(&rpc_ctx) {
            reg_err = Some(region_error);
        }
        (RequestCtx { reg_ctx, rpc_ctx }, reg_err)
    }

    /// Go `Server.KvGet` (`server.go`).
    pub fn kv_get(&mut self, req: &kvrpcpb::GetRequest) -> kvrpcpb::GetResponse {
        let req_ctx = region_checked!(self, req, kvrpcpb::GetResponse);
        let ctx = req_ctx.read_context();
        match self
            .store
            .get_with_commit_ts(&ctx, &req.key, req.version, req.need_commit_ts)
        {
            Ok((value, commit_ts)) => kvrpcpb::GetResponse {
                value: value.unwrap_or_default(),
                not_found: false,
                commit_ts,
                ..kvrpcpb::GetResponse::default()
            },
            Err(err) => kvrpcpb::GetResponse {
                error: Some(convert_to_key_error(&err)),
                ..kvrpcpb::GetResponse::default()
            },
        }
    }

    /// Go `Server.KvScan`.
    pub fn kv_scan(&mut self, req: &kvrpcpb::ScanRequest) -> kvrpcpb::ScanResponse {
        let req_ctx = region_checked!(self, req, kvrpcpb::ScanResponse);
        let ctx = req_ctx.read_context();
        let pairs = self.store.scan_with(
            &ctx,
            &ScanReq {
                start_key: req.start_key.clone(),
                end_key: req.end_key.clone(),
                limit: req.limit,
                version: req.version,
                sample_step: req.sample_step,
                reverse: req.reverse,
            },
        );
        kvrpcpb::ScanResponse {
            pairs: pairs.into_iter().map(pair_to_proto).collect(),
            ..kvrpcpb::ScanResponse::default()
        }
    }

    /// Go `Server.KvBatchGet`.
    pub fn kv_batch_get(&mut self, req: &kvrpcpb::BatchGetRequest) -> kvrpcpb::BatchGetResponse {
        let req_ctx = region_checked!(self, req, kvrpcpb::BatchGetResponse);
        let ctx = req_ctx.read_context();
        let pairs = self
            .store
            .batch_get_with_commit_ts(&ctx, &req.keys, req.version, req.need_commit_ts)
            .into_iter()
            .map(pair_to_proto)
            .collect();
        kvrpcpb::BatchGetResponse {
            pairs,
            ..kvrpcpb::BatchGetResponse::default()
        }
    }

    /// Go `Server.KvPrewrite`.
    pub fn kv_prewrite(&mut self, req: &kvrpcpb::PrewriteRequest) -> kvrpcpb::PrewriteResponse {
        let _req_ctx = region_checked!(self, req, kvrpcpb::PrewriteResponse);
        let reduced = PrewriteReq {
            mutations: req.mutations.clone(),
            primary_lock: req.primary_lock.clone(),
            start_version: req.start_version,
            lock_ttl: req.lock_ttl,
            for_update_ts: req.for_update_ts,
            min_commit_ts: req.min_commit_ts,
            max_commit_ts: req.max_commit_ts,
            use_async_commit: req.use_async_commit,
            try_one_pc: req.try_one_pc,
            secondaries: req.secondaries.clone(),
            pessimistic_actions: req.pessimistic_actions().collect(),
            assertion_level: req.assertion_level,
            for_update_ts_constraints: req
                .for_update_ts_constraints
                .iter()
                .map(|constraint| {
                    (
                        usize::try_from(constraint.index).unwrap_or(usize::MAX),
                        constraint.expected_for_update_ts,
                    )
                })
                .collect(),
        };
        match self.store.prewrite_with_outcome(&reduced) {
            // Go `server.go:419-424`: the async-commit and 1PC context
            // fields ride the response.
            Ok(outcome) => kvrpcpb::PrewriteResponse {
                min_commit_ts: outcome.async_min_commit_ts,
                one_pc_commit_ts: outcome.one_pc_commit_ts,
                ..kvrpcpb::PrewriteResponse::default()
            },
            Err(err) => kvrpcpb::PrewriteResponse {
                errors: vec![convert_to_key_error(&err)],
                ..kvrpcpb::PrewriteResponse::default()
            },
        }
    }

    /// Go `Server.KvCommit`.
    pub fn kv_commit(&mut self, req: &kvrpcpb::CommitRequest) -> kvrpcpb::CommitResponse {
        let _req_ctx = region_checked!(self, req, kvrpcpb::CommitResponse);
        match self
            .store
            .commit(&req.keys, req.start_version, req.commit_version)
        {
            Ok(()) => kvrpcpb::CommitResponse::default(),
            Err(err) => kvrpcpb::CommitResponse {
                error: Some(convert_to_key_error(&err)),
                ..kvrpcpb::CommitResponse::default()
            },
        }
    }

    /// Go `Server.KvBatchRollback`.
    pub fn kv_batch_rollback(
        &mut self,
        req: &kvrpcpb::BatchRollbackRequest,
    ) -> kvrpcpb::BatchRollbackResponse {
        let _req_ctx = region_checked!(self, req, kvrpcpb::BatchRollbackResponse);
        match self.store.rollback(&req.keys, req.start_version) {
            Ok(()) => kvrpcpb::BatchRollbackResponse::default(),
            Err(err) => kvrpcpb::BatchRollbackResponse {
                error: Some(convert_to_key_error(&err)),
                ..kvrpcpb::BatchRollbackResponse::default()
            },
        }
    }

    /// Go `Server.KvPessimisticLock` without waiting: a conflicting lock
    /// answers its lock error at once.
    pub fn kv_pessimistic_lock(
        &mut self,
        req: &kvrpcpb::PessimisticLockRequest,
    ) -> kvrpcpb::PessimisticLockResponse {
        let (response, wait) = self.kv_pessimistic_lock_begin(req);
        match wait {
            Some(wait) => self.kv_pessimistic_lock_finish(
                req,
                response,
                wait,
                crate::lockwaiter::WaitResult {
                    wakeup_sleep_time: crate::lockwaiter::WAIT_TIMEOUT,
                    ..crate::lockwaiter::WaitResult::default()
                },
            ),
            None => response,
        }
    }

    /// Go `Server.KvPessimisticLock` up to `waiter.Wait()`: the lock attempt,
    /// and, on a conflict under a waiting request, the waiter to wait on --
    /// with the store released, so the lock holder can finish.
    pub fn kv_pessimistic_lock_begin(
        &mut self,
        req: &kvrpcpb::PessimisticLockRequest,
    ) -> (
        kvrpcpb::PessimisticLockResponse,
        Option<PessimisticLockWait>,
    ) {
        let (_, region_error) = self.new_request_ctx(req.context.as_ref());
        if let Some(region_error) = region_error {
            return (
                kvrpcpb::PessimisticLockResponse::from_region_error(region_error),
                None,
            );
        }
        self.pessimistic_lock_attempt(req, req.wait_timeout)
    }

    /// Go `Server.KvPessimisticLock` after `waiter.Wait()` answered `result`:
    /// a timeout answers the lock error, a deadlock its error, a wake-up
    /// under `Force` / `WakeUpModeForceLock` one more no-wait attempt, and
    /// anything else a write conflict at the store's latest commit ts.
    pub fn kv_pessimistic_lock_finish(
        &mut self,
        req: &kvrpcpb::PessimisticLockRequest,
        mut response: kvrpcpb::PessimisticLockResponse,
        wait: PessimisticLockWait,
        result: crate::lockwaiter::WaitResult,
    ) -> kvrpcpb::PessimisticLockResponse {
        let PessimisticLockWait { waiter, locked } = wait;
        self.store.deadlock_detector.clean_up_wait_for(
            req.start_version,
            waiter.lock_ts,
            waiter.key_hash,
        );
        self.store.lock_waiter_manager.clean_up(&waiter);
        if result.wakeup_sleep_time == crate::lockwaiter::WAIT_TIMEOUT {
            return response;
        }
        let force_lock =
            req.wake_up_mode == kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeForceLock as i32;
        if let Some(deadlock) = result.deadlock_resp {
            response.errors = vec![convert_to_key_error(&KvError::Deadlock {
                lock_key: locked.key.clone(),
                lock_ts: locked.lock_version,
                deadlock_key_hash: deadlock.deadlock_key_hash,
                wait_chain: deadlock.wait_chain,
            })];
            response.region_error = None;
            if force_lock {
                response.results = vec![kvrpcpb::PessimisticLockKeyResult {
                    r#type: kvrpcpb::PessimisticLockKeyResultType::LockResultFailed as i32,
                    ..kvrpcpb::PessimisticLockKeyResult::default()
                }];
            }
            return response;
        }
        #[allow(deprecated)]
        let force = req.force;
        if result.wakeup_sleep_time == crate::lockwaiter::WAKE_UP_THIS_WAITER
            && (force || force_lock)
        {
            let (retry, retry_wait) =
                self.pessimistic_lock_attempt(req, crate::lockwaiter::LOCK_NO_WAIT);
            debug_assert!(retry_wait.is_none(), "a no-wait attempt parks no waiter");
            let still_locked = retry
                .errors
                .first()
                .is_some_and(|error| error.locked.is_some());
            if retry.errors.is_empty() || !still_locked {
                return retry;
            }
            response = retry;
            tidb_log::warn("wakeup force lock request, try lock still failed", &[]);
        }
        // The key was rolled back; the exact commit ts is unknown, so the
        // store's latest serves, as Go's does.
        response.errors = vec![convert_to_key_error(&KvError::Conflict {
            reason: kvrpcpb::write_conflict::Reason::Unknown,
            start_ts: req.for_update_ts,
            conflict_ts: waiter.lock_ts,
            conflict_commit_ts: self.store.latest_ts(),
            key: Vec::new(),
        })];
        response
    }

    /// Go `Server.KvPessimisticLock`, the no-wait slice.
    fn pessimistic_lock_attempt(
        &mut self,
        req: &kvrpcpb::PessimisticLockRequest,
        wait_timeout: i64,
    ) -> (
        kvrpcpb::PessimisticLockResponse,
        Option<PessimisticLockWait>,
    ) {
        let force_lock =
            req.wake_up_mode == kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeForceLock as i32;
        // Go `pessimisticLockInner`'s guard, message verbatim: ForceLock
        // addresses exactly one key.
        if force_lock && req.mutations.len() > 1 {
            let response = kvrpcpb::PessimisticLockResponse {
                errors: vec![kvrpcpb::KeyError {
                    abort: "Trying to lock more than one key in WakeUpModeForceLock, \
                            which is not supported yet"
                        .to_owned(),
                    ..kvrpcpb::KeyError::default()
                }],
                ..kvrpcpb::PessimisticLockResponse::default()
            };
            return (response, None);
        }
        let reduced = PessimisticLockReq {
            mutations: req.mutations.clone(),
            primary_lock: req.primary_lock.clone(),
            start_version: req.start_version,
            for_update_ts: req.for_update_ts,
            lock_ttl: req.lock_ttl,
            return_values: req.return_values,
            check_existence: req.check_existence,
            lock_only_if_exists: req.lock_only_if_exists,
            // kvrpcpb marks Force deprecated (ForceLock supersedes it), but
            // Go unistore still reads req.Force -- so does this port.
            #[allow(deprecated)]
            force: req.force,
            wait_timeout,
            is_first_lock: req.is_first_lock,
            resource_group_tag: req
                .context
                .as_ref()
                .map(|context| context.resource_group_tag.clone())
                .unwrap_or_default(),
        };
        let (result, waiter) = self.store.pessimistic_lock_with_waiter(&reduced);
        let wait = match (&result, waiter) {
            (Err(KvError::Locked(locked)), Some(waiter)) => Some(PessimisticLockWait {
                waiter,
                locked: locked.clone(),
            }),
            (_, Some(waiter)) => {
                self.store.lock_waiter_manager.clean_up(&waiter);
                None
            }
            (_, None) => None,
        };
        let response = match result {
            Ok(result) => {
                // Go's ForceLock arm (`mvcc.go` `PessimisticLock`) answers one
                // `LockResultNormal` per granted key and, when the request
                // asked for values or existence, carries the row in that
                // result's `value`/`existence` rather than in the Normal-mode
                // `values`/`not_founds` arrays -- the client reads
                // `results[0]` in this mode. A conflict timestamp this
                // simplified store never grants past.
                let results = if force_lock {
                    req.mutations
                        .iter()
                        .enumerate()
                        .map(|(index, _)| {
                            let value = result.values.get(index).cloned().unwrap_or_default();
                            let existence = result
                                .not_founds
                                .get(index)
                                .map_or(!value.is_empty(), |not_found| !not_found);
                            kvrpcpb::PessimisticLockKeyResult {
                                r#type: kvrpcpb::PessimisticLockKeyResultType::LockResultNormal
                                    as i32,
                                value,
                                existence,
                                ..kvrpcpb::PessimisticLockKeyResult::default()
                            }
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                kvrpcpb::PessimisticLockResponse {
                    values: if force_lock {
                        Vec::new()
                    } else {
                        result.values
                    },
                    not_founds: if force_lock {
                        Vec::new()
                    } else {
                        result.not_founds
                    },
                    results,
                    ..kvrpcpb::PessimisticLockResponse::default()
                }
            }
            Err(err) => {
                // Go `MVCCStore.PessimisticLock`: an error under ForceLock
                // pads `results` with LockResultFailed so the reply always
                // answers about every requested key.
                let results = if force_lock {
                    req.mutations
                        .iter()
                        .map(|_| kvrpcpb::PessimisticLockKeyResult {
                            r#type: kvrpcpb::PessimisticLockKeyResultType::LockResultFailed as i32,
                            ..kvrpcpb::PessimisticLockKeyResult::default()
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                kvrpcpb::PessimisticLockResponse {
                    errors: vec![convert_to_key_error(&err)],
                    results,
                    ..kvrpcpb::PessimisticLockResponse::default()
                }
            }
        };
        (response, wait)
    }

    /// Go `Server.KvPessimisticRollback`.
    pub fn kv_pessimistic_rollback(
        &mut self,
        req: &kvrpcpb::PessimisticRollbackRequest,
    ) -> kvrpcpb::PessimisticRollbackResponse {
        let req_ctx = region_checked!(self, req, kvrpcpb::PessimisticRollbackResponse);
        if req.keys.is_empty() {
            self.store.pessimistic_rollback_with_scan_first(
                req_ctx.raw_start(),
                req_ctx.raw_end(),
                req.start_version,
                req.for_update_ts,
            );
        } else {
            self.store
                .pessimistic_rollback(&req.keys, req.start_version, req.for_update_ts);
        }
        kvrpcpb::PessimisticRollbackResponse::default()
    }

    /// Go `Server.KvTxnHeartBeat`.
    pub fn kv_txn_heart_beat(
        &mut self,
        req: &kvrpcpb::TxnHeartBeatRequest,
    ) -> kvrpcpb::TxnHeartBeatResponse {
        let _req_ctx = region_checked!(self, req, kvrpcpb::TxnHeartBeatResponse);
        match self
            .store
            .txn_heart_beat(&req.primary_lock, req.start_version, req.advise_lock_ttl)
        {
            Ok(ttl) => kvrpcpb::TxnHeartBeatResponse {
                lock_ttl: ttl,
                ..kvrpcpb::TxnHeartBeatResponse::default()
            },
            Err(err) => kvrpcpb::TxnHeartBeatResponse {
                error: Some(convert_to_key_error(&err)),
                ..kvrpcpb::TxnHeartBeatResponse::default()
            },
        }
    }

    /// Go `Server.KvCheckTxnStatus`.
    pub fn kv_check_txn_status(
        &mut self,
        req: &kvrpcpb::CheckTxnStatusRequest,
    ) -> kvrpcpb::CheckTxnStatusResponse {
        let _req_ctx = region_checked!(self, req, kvrpcpb::CheckTxnStatusResponse);
        let reduced = CheckTxnStatusReq {
            primary_key: req.primary_key.clone(),
            lock_ts: req.lock_ts,
            caller_start_ts: req.caller_start_ts,
            current_ts: req.current_ts,
            rollback_if_not_exist: req.rollback_if_not_exist,
            force_sync_commit: req.force_sync_commit,
            resolving_pessimistic_lock: req.resolving_pessimistic_lock,
        };
        match self.store.check_txn_status(&reduced) {
            Ok(status) => {
                // Go's response: the surviving lock's ttl rides LockTtl, the
                // lock itself LockInfo.
                let lock_ttl = status.lock_info.as_ref().map_or(0, |lock| lock.lock_ttl);
                kvrpcpb::CheckTxnStatusResponse {
                    lock_ttl,
                    commit_version: status.commit_ts,
                    action: status.action as i32,
                    lock_info: status.lock_info.map(|boxed| *boxed),
                    ..kvrpcpb::CheckTxnStatusResponse::default()
                }
            }
            Err(err) => kvrpcpb::CheckTxnStatusResponse {
                error: Some(convert_to_key_error(&err)),
                ..kvrpcpb::CheckTxnStatusResponse::default()
            },
        }
    }

    /// Go `Server.KvCheckSecondaryLocks`.
    pub fn kv_check_secondary_locks(
        &mut self,
        req: &kvrpcpb::CheckSecondaryLocksRequest,
    ) -> kvrpcpb::CheckSecondaryLocksResponse {
        let _req_ctx = region_checked!(self, req, kvrpcpb::CheckSecondaryLocksResponse);
        match self
            .store
            .check_secondary_locks(&req.keys, req.start_version)
        {
            Ok(status) => kvrpcpb::CheckSecondaryLocksResponse {
                locks: status.locks,
                commit_ts: status.commit_ts,
                ..kvrpcpb::CheckSecondaryLocksResponse::default()
            },
            Err(err) => kvrpcpb::CheckSecondaryLocksResponse {
                error: Some(convert_to_key_error(&err)),
                ..kvrpcpb::CheckSecondaryLocksResponse::default()
            },
        }
    }

    /// Go `Server.KvResolveLock`.
    pub fn kv_resolve_lock(
        &mut self,
        req: &kvrpcpb::ResolveLockRequest,
    ) -> kvrpcpb::ResolveLockResponse {
        let req_ctx = region_checked!(self, req, kvrpcpb::ResolveLockResponse);
        let (raw_start, raw_end) = (req_ctx.raw_start(), req_ctx.raw_end());
        let result = if req.txn_infos.is_empty() {
            self.store.resolve_lock_in_region(
                raw_start,
                raw_end,
                &req.keys,
                req.start_version,
                req.commit_version,
            )
        } else {
            req.txn_infos.iter().try_for_each(|txn_info| {
                self.store.resolve_lock_in_region(
                    raw_start,
                    raw_end,
                    &[],
                    txn_info.txn,
                    txn_info.status,
                )
            })
        };
        match result {
            Ok(()) => kvrpcpb::ResolveLockResponse::default(),
            Err(err) => kvrpcpb::ResolveLockResponse {
                error: Some(convert_to_key_error(&err)),
                ..kvrpcpb::ResolveLockResponse::default()
            },
        }
    }

    /// Go `Server.KvCleanup`: an already-committed transaction answers its
    /// commit ts instead of an error.
    pub fn kv_cleanup(&mut self, req: &kvrpcpb::CleanupRequest) -> kvrpcpb::CleanupResponse {
        let _req_ctx = region_checked!(self, req, kvrpcpb::CleanupResponse);
        match self
            .store
            .cleanup(&req.key, req.start_version, req.current_ts)
        {
            Ok(()) => kvrpcpb::CleanupResponse::default(),
            Err(KvError::AlreadyCommitted(commit_ts)) => kvrpcpb::CleanupResponse {
                commit_version: commit_ts,
                ..kvrpcpb::CleanupResponse::default()
            },
            Err(err) => kvrpcpb::CleanupResponse {
                error: Some(convert_to_key_error(&err)),
                ..kvrpcpb::CleanupResponse::default()
            },
        }
    }

    /// Go `Server.KvBufferBatchGet`.
    pub fn kv_buffer_batch_get(
        &mut self,
        req: &kvrpcpb::BufferBatchGetRequest,
    ) -> kvrpcpb::BufferBatchGetResponse {
        let _req_ctx = region_checked!(self, req, kvrpcpb::BufferBatchGetResponse);
        kvrpcpb::BufferBatchGetResponse {
            pairs: self
                .store
                .read_buffer_from_lock(req.version, &req.keys)
                .into_iter()
                .map(pair_to_proto)
                .collect(),
            ..kvrpcpb::BufferBatchGetResponse::default()
        }
    }

    /// Go `Server.KvScanLock`: the region's locks up to `max_version`; the
    /// request's own key range is not consulted.
    pub fn kv_scan_lock(&mut self, req: &kvrpcpb::ScanLockRequest) -> kvrpcpb::ScanLockResponse {
        let req_ctx = region_checked!(self, req, kvrpcpb::ScanLockResponse);
        let limit = usize::try_from(req.limit).unwrap_or(usize::MAX);
        kvrpcpb::ScanLockResponse {
            locks: self.store.scan_lock(
                req_ctx.raw_start(),
                req_ctx.raw_end(),
                req.max_version,
                limit,
            ),
            ..kvrpcpb::ScanLockResponse::default()
        }
    }

    /// Go `Server.PhysicalScanLock`: no region context.
    pub fn physical_scan_lock(
        &mut self,
        req: &kvrpcpb::PhysicalScanLockRequest,
    ) -> kvrpcpb::PhysicalScanLockResponse {
        let limit = usize::try_from(req.limit).unwrap_or(usize::MAX);
        kvrpcpb::PhysicalScanLockResponse {
            locks: self
                .store
                .physical_scan_lock(&req.start_key, req.max_ts, limit),
            ..kvrpcpb::PhysicalScanLockResponse::default()
        }
    }

    /// Go `Server.KvGC`: the safe point rises; Go checks no region here.
    pub fn kv_gc(&mut self, req: &kvrpcpb::GcRequest) -> kvrpcpb::GcResponse {
        self.store.update_safe_point(req.safe_point);
        kvrpcpb::GcResponse::default()
    }

    /// Go `Server.KvDeleteRange`: a failed delete is logged, not answered.
    pub fn kv_delete_range(
        &mut self,
        req: &kvrpcpb::DeleteRangeRequest,
    ) -> kvrpcpb::DeleteRangeResponse {
        let _req_ctx = region_checked!(self, req, kvrpcpb::DeleteRangeResponse);
        if let Err(err) = self.store.delete_range(&req.start_key, &req.end_key) {
            tidb_log::error(
                "delete range failed",
                &[tidb_log::Field::new(
                    "error",
                    tidb_log::Value::Str(format!("{err:?}")),
                )],
            );
        }
        kvrpcpb::DeleteRangeResponse::default()
    }

    /// Go `Server.UnsafeDestroyRange`: no region context.
    pub fn unsafe_destroy_range(
        &mut self,
        req: &kvrpcpb::UnsafeDestroyRangeRequest,
    ) -> kvrpcpb::UnsafeDestroyRangeResponse {
        self.store
            .delete_file_in_range(&req.start_key, &req.end_key);
        kvrpcpb::UnsafeDestroyRangeResponse::default()
    }

    /// Go `Server.MvccGetByKey`.
    pub fn mvcc_get_by_key(
        &mut self,
        req: &kvrpcpb::MvccGetByKeyRequest,
    ) -> kvrpcpb::MvccGetByKeyResponse {
        let _req_ctx = region_checked!(self, req, kvrpcpb::MvccGetByKeyResponse);
        kvrpcpb::MvccGetByKeyResponse {
            info: Some(mvcc_info_to_proto(self.store.mvcc_get_by_key(&req.key))),
            ..kvrpcpb::MvccGetByKeyResponse::default()
        }
    }

    /// Go `Server.MvccGetByStartTs`: the region's first key written by the
    /// start ts; none answers no info and no key.
    pub fn mvcc_get_by_start_ts(
        &mut self,
        req: &kvrpcpb::MvccGetByStartTsRequest,
    ) -> kvrpcpb::MvccGetByStartTsResponse {
        let req_ctx = region_checked!(self, req, kvrpcpb::MvccGetByStartTsResponse);
        match self
            .store
            .mvcc_get_by_start_ts(req_ctx.raw_start(), req_ctx.raw_end(), req.start_ts)
        {
            Some((info, key)) => kvrpcpb::MvccGetByStartTsResponse {
                info: Some(mvcc_info_to_proto(info)),
                key,
                ..kvrpcpb::MvccGetByStartTsResponse::default()
            },
            None => kvrpcpb::MvccGetByStartTsResponse::default(),
        }
    }

    /// Go `Server.SplitRegion`: the region manager checks the region itself.
    pub fn split_region(
        &mut self,
        req: &kvrpcpb::SplitRegionRequest,
    ) -> kvrpcpb::SplitRegionResponse {
        self.regions.split_region(req)
    }

    /// Go `Server.Coprocessor`: the region-checked request runs on the
    /// coprocessor handler, then its store-batch tasks run each as its own
    /// request.
    pub fn coprocessor(&mut self, req: &coprocessor::Request) -> coprocessor::Response {
        let _req_ctx = region_checked!(self, req, coprocessor::Response);
        let mut resp = crate::cophandler::handle_cop_request(&mut self.store, req);
        resp.batch_responses = self.store_batch_coprocessor(req);
        resp
    }

    /// Go `Server.StoreBatchCoprocessor`: each task is the request again
    /// with the task's ranges and region, and no tasks of its own.
    pub fn store_batch_coprocessor(
        &mut self,
        req: &coprocessor::Request,
    ) -> Vec<coprocessor::StoreBatchTaskResponse> {
        let mut batch_resps = Vec::with_capacity(req.tasks.len());
        for task in &req.tasks {
            let mut task_req = req.clone();
            task_req.tasks = Vec::new();
            task_req.is_cache_enabled = false;
            task_req.ranges = task.ranges.clone();
            let context = task_req.context.get_or_insert_with(Default::default);
            context.region_id = task.region_id;
            context.region_epoch = task.region_epoch.clone();
            context.peer = task.peer.clone();
            let resp = self.coprocessor(&task_req);
            batch_resps.push(coprocessor::StoreBatchTaskResponse {
                task_id: task.task_id,
                region_error: resp.region_error,
                locked: resp.locked,
                other_error: resp.other_error,
                exec_details_v2: resp.exec_details_v2,
                data: resp.data,
                ..coprocessor::StoreBatchTaskResponse::default()
            });
        }
        batch_resps
    }

    /// Go `Server.CheckLockObserver`: no observer is implemented, so the
    /// store reports clean.
    pub fn check_lock_observer(
        &mut self,
        _req: &kvrpcpb::CheckLockObserverRequest,
    ) -> kvrpcpb::CheckLockObserverResponse {
        kvrpcpb::CheckLockObserverResponse {
            is_clean: true,
            ..kvrpcpb::CheckLockObserverResponse::default()
        }
    }

    /// Go `Server.RegisterLockObserver`.
    pub fn register_lock_observer(
        &mut self,
        _req: &kvrpcpb::RegisterLockObserverRequest,
    ) -> kvrpcpb::RegisterLockObserverResponse {
        kvrpcpb::RegisterLockObserverResponse::default()
    }

    /// Go `Server.RemoveLockObserver`.
    pub fn remove_lock_observer(
        &mut self,
        _req: &kvrpcpb::RemoveLockObserverRequest,
    ) -> kvrpcpb::RemoveLockObserverResponse {
        kvrpcpb::RemoveLockObserverResponse::default()
    }

    /// Go `Server.GetStoreSafeTS`: zero.
    pub fn get_store_safe_ts(
        &mut self,
        _req: &kvrpcpb::StoreSafeTsRequest,
    ) -> kvrpcpb::StoreSafeTsResponse {
        kvrpcpb::StoreSafeTsResponse::default()
    }

    /// Go `Server.GetLockWaitHistory`: empty.
    pub fn get_lock_wait_history(
        &mut self,
        _req: &kvrpcpb::GetLockWaitHistoryRequest,
    ) -> kvrpcpb::GetLockWaitHistoryResponse {
        kvrpcpb::GetLockWaitHistoryResponse::default()
    }

    /// Go `Server.RawGetKeyTTL`: empty (Go's TODO).
    pub fn raw_get_key_ttl(
        &mut self,
        _req: &kvrpcpb::RawGetKeyTtlRequest,
    ) -> kvrpcpb::RawGetKeyTtlResponse {
        kvrpcpb::RawGetKeyTtlResponse::default()
    }

    /// Go `Server.KvImport`: empty (Go's TODO).
    pub fn kv_import(&mut self, _req: &kvrpcpb::ImportRequest) -> kvrpcpb::ImportResponse {
        kvrpcpb::ImportResponse::default()
    }
}

/// Go `MvccGetByKey`'s proto: the writes in the store's order and the
/// parallel `Values` list of each write's start ts and short value.
fn mvcc_info_to_proto(info: MvccInfo) -> kvrpcpb::MvccInfo {
    let writes: Vec<kvrpcpb::MvccWrite> = info
        .writes
        .into_iter()
        .map(|write| kvrpcpb::MvccWrite {
            r#type: write.op,
            start_ts: write.start_ts,
            commit_ts: write.commit_ts,
            short_value: write.short_value,
            ..kvrpcpb::MvccWrite::default()
        })
        .collect();
    let values = writes
        .iter()
        .map(|write| kvrpcpb::MvccValue {
            start_ts: write.start_ts,
            value: write.short_value.clone(),
        })
        .collect();
    kvrpcpb::MvccInfo {
        lock: info.lock.map(|lock| kvrpcpb::MvccLock {
            r#type: lock.op,
            start_ts: lock.start_ts,
            primary: lock.primary,
            short_value: lock.short_value,
            ..kvrpcpb::MvccLock::default()
        }),
        writes,
        values,
    }
}

/// A store pair into the proto pair, error arm included.
fn pair_to_proto(pair: crate::mvcc_store::KvPair) -> kvrpcpb::KvPair {
    kvrpcpb::KvPair {
        key: pair.key,
        value: pair.value,
        error: pair.error.map(|err| convert_to_key_error(&err)),
        commit_ts: pair.commit_ts,
        ..kvrpcpb::KvPair::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_proto::{KvrpcMutation, KvrpcOp};

    // All WRITTEN: server.go's own coverage is the RPC integration suites.
    // These pin the proto shapes a client parses.

    /// The bootstrap region's context, as a client addresses it.
    fn region_ctx() -> Option<kvrpcpb::Context> {
        Some(kvrpcpb::Context {
            region_id: crate::region_manager::BOOTSTRAP_REGION_ID,
            region_epoch: Some(tidb_proto::metapb::RegionEpoch {
                conf_ver: 1,
                version: 1,
            }),
            peer: Some(tidb_proto::metapb::Peer {
                id: crate::region_manager::BOOTSTRAP_PEER_ID,
                store_id: crate::region_manager::STORE_ID,
                ..tidb_proto::metapb::Peer::default()
            }),
            ..kvrpcpb::Context::default()
        })
    }

    fn put(key: &[u8], value: &[u8]) -> KvrpcMutation {
        KvrpcMutation {
            op: KvrpcOp::Put as i32,
            key: key.to_vec(),
            value: value.to_vec(),
            ..KvrpcMutation::default()
        }
    }

    #[test]
    fn a_transaction_runs_end_to_end_through_the_proto_surface() {
        // The exact request sequence a TiKV client issues: prewrite, commit,
        // get — all through proto messages.
        let mut handler = KvHandler::default();
        let key = b"k".to_vec();
        let resp = handler.kv_prewrite(&kvrpcpb::PrewriteRequest {
            context: region_ctx(),
            mutations: vec![put(&key, b"v")],
            primary_lock: key.clone(),
            start_version: 10,
            lock_ttl: 3000,
            ..kvrpcpb::PrewriteRequest::default()
        });
        assert!(resp.errors.is_empty(), "{:?}", resp.errors);
        let resp = handler.kv_commit(&kvrpcpb::CommitRequest {
            context: region_ctx(),
            keys: vec![key.clone()],
            start_version: 10,
            commit_version: 11,
            ..kvrpcpb::CommitRequest::default()
        });
        assert!(resp.error.is_none());
        let resp = handler.kv_get(&kvrpcpb::GetRequest {
            context: region_ctx(),
            key: key.clone(),
            version: 12,
            ..kvrpcpb::GetRequest::default()
        });
        assert!(resp.error.is_none());
        assert_eq!(resp.value, b"v");
    }

    #[test]
    fn a_blocked_get_answers_the_locked_key_error_a_client_resolves() {
        // The client's whole resolution protocol keys off `KeyError.locked`:
        // get blocks, check-txn-status expires the lock, resolve clears it,
        // the retried get answers.
        let mut handler = KvHandler::default();
        let key = b"k".to_vec();
        let start_ts = 5_u64 << 18;
        handler.kv_prewrite(&kvrpcpb::PrewriteRequest {
            context: region_ctx(),
            mutations: vec![put(&key, b"v")],
            primary_lock: key.clone(),
            start_version: start_ts,
            lock_ttl: 10,
            ..kvrpcpb::PrewriteRequest::default()
        });
        let resp = handler.kv_get(&kvrpcpb::GetRequest {
            context: region_ctx(),
            key: key.clone(),
            version: start_ts + 1,
            ..kvrpcpb::GetRequest::default()
        });
        let locked = resp.error.expect("locked").locked.expect("the lock info");
        assert_eq!(locked.lock_version, start_ts);
        // Status check at an expired current ts rolls the lock back...
        let resp = handler.kv_check_txn_status(&kvrpcpb::CheckTxnStatusRequest {
            context: region_ctx(),
            primary_key: key.clone(),
            lock_ts: start_ts,
            current_ts: 40 << 18,
            rollback_if_not_exist: true,
            ..kvrpcpb::CheckTxnStatusRequest::default()
        });
        assert_eq!(
            resp.action,
            tidb_proto::KvrpcTxnAction::TtlExpireRollback as i32
        );
        // ...and the retried read now answers cleanly: not found.
        let resp = handler.kv_get(&kvrpcpb::GetRequest {
            context: region_ctx(),
            key,
            version: 41 << 18,
            ..kvrpcpb::GetRequest::default()
        });
        assert!(resp.error.is_none());
        assert!(resp.value.is_empty());
    }

    #[test]
    fn a_write_conflict_carries_its_proto_reason() {
        let mut handler = KvHandler::default();
        let key = b"k".to_vec();
        handler.kv_prewrite(&kvrpcpb::PrewriteRequest {
            context: region_ctx(),
            mutations: vec![put(&key, b"v1")],
            primary_lock: key.clone(),
            start_version: 10,
            ..kvrpcpb::PrewriteRequest::default()
        });
        handler.kv_commit(&kvrpcpb::CommitRequest {
            context: region_ctx(),
            keys: vec![key.clone()],
            start_version: 10,
            commit_version: 20,
            ..kvrpcpb::CommitRequest::default()
        });
        let resp = handler.kv_prewrite(&kvrpcpb::PrewriteRequest {
            context: region_ctx(),
            mutations: vec![put(&key, b"v2")],
            primary_lock: key.clone(),
            start_version: 15,
            ..kvrpcpb::PrewriteRequest::default()
        });
        let conflict = resp.errors[0].conflict.as_ref().expect("a conflict");
        assert_eq!(conflict.conflict_commit_ts, 20);
        assert_eq!(
            conflict.reason,
            kvrpcpb::write_conflict::Reason::Optimistic as i32
        );
    }

    /// The context a client sends for the region holding the raw `key`.
    fn ctx_for(handler: &KvHandler, key: &[u8]) -> Option<kvrpcpb::Context> {
        let (region, leader) = handler
            .regions
            .get_region_by_key(&crate::region_manager::encode_region_key(key))
            .unwrap();
        Some(kvrpcpb::Context {
            region_id: region.id,
            region_epoch: region.region_epoch.clone(),
            peer: Some(tidb_proto::metapb::Peer {
                id: leader.id,
                store_id: leader.store_id,
                ..tidb_proto::metapb::Peer::default()
            }),
            ..kvrpcpb::Context::default()
        })
    }

    fn split_at(handler: &mut KvHandler, key: &[u8]) {
        let context = ctx_for(handler, key);
        let resp = handler.split_region(&kvrpcpb::SplitRegionRequest {
            context,
            split_keys: vec![key.to_vec()],
            ..kvrpcpb::SplitRegionRequest::default()
        });
        assert!(resp.region_error.is_none(), "{:?}", resp.region_error);
    }

    fn prewrite(handler: &mut KvHandler, mutations: Vec<KvrpcMutation>, start: u64) {
        let primary = mutations[0].key.clone();
        let context = ctx_for(handler, &primary);
        let resp = handler.kv_prewrite(&kvrpcpb::PrewriteRequest {
            context,
            mutations,
            primary_lock: primary,
            start_version: start,
            lock_ttl: 3000,
            ..kvrpcpb::PrewriteRequest::default()
        });
        assert!(resp.errors.is_empty(), "{:?}", resp.errors);
    }

    fn commit(handler: &mut KvHandler, keys: &[&[u8]], start: u64, commit_ts: u64) {
        let context = ctx_for(handler, keys[0]);
        let resp = handler.kv_commit(&kvrpcpb::CommitRequest {
            context,
            keys: keys.iter().map(|key| key.to_vec()).collect(),
            start_version: start,
            commit_version: commit_ts,
            ..kvrpcpb::CommitRequest::default()
        });
        assert!(resp.error.is_none(), "{:?}", resp.error);
    }

    fn put_committed(
        handler: &mut KvHandler,
        key: &[u8],
        value: &[u8],
        start: u64,
        commit_ts: u64,
    ) {
        prewrite(handler, vec![put(key, value)], start);
        commit(handler, &[key], start, commit_ts);
    }

    fn get(handler: &mut KvHandler, key: &[u8], version: u64) -> kvrpcpb::GetResponse {
        let context = ctx_for(handler, key);
        handler.kv_get(&kvrpcpb::GetRequest {
            context,
            key: key.to_vec(),
            version,
            ..kvrpcpb::GetRequest::default()
        })
    }

    fn scan_lock(
        handler: &mut KvHandler,
        key: &[u8],
        max_version: u64,
        limit: u32,
    ) -> Vec<Vec<u8>> {
        let context = ctx_for(handler, key);
        let resp = handler.kv_scan_lock(&kvrpcpb::ScanLockRequest {
            context,
            max_version,
            limit,
            ..kvrpcpb::ScanLockRequest::default()
        });
        assert!(resp.region_error.is_none() && resp.error.is_none());
        resp.locks.into_iter().map(|lock| lock.key).collect()
    }

    /// Go `newRequestCtx`: after a split the old epoch is stale, an absent
    /// context names no region, and the new regions serve their keys.
    #[test]
    fn a_stale_or_unknown_region_answers_the_region_error_tikv_would() {
        let mut handler = KvHandler::default();
        put_committed(&mut handler, b"x", b"v", 10, 11);
        split_at(&mut handler, b"m");

        let stale = handler.kv_get(&kvrpcpb::GetRequest {
            context: region_ctx(),
            key: b"x".to_vec(),
            version: 20,
            ..kvrpcpb::GetRequest::default()
        });
        let region_error = stale.region_error.expect("a stale epoch");
        let current = &region_error.epoch_not_match.unwrap().current_regions[0];
        assert_eq!(current.id, crate::region_manager::BOOTSTRAP_REGION_ID);
        assert_eq!(
            current.end_key,
            crate::region_manager::encode_region_key(b"m")
        );

        let unaddressed = handler.kv_get(&kvrpcpb::GetRequest {
            key: b"x".to_vec(),
            version: 20,
            ..kvrpcpb::GetRequest::default()
        });
        assert_eq!(
            unaddressed
                .region_error
                .unwrap()
                .region_not_found
                .unwrap()
                .region_id,
            0
        );

        let resp = get(&mut handler, b"x", 20);
        assert!(resp.region_error.is_none());
        assert_eq!(resp.value, b"v");
    }

    /// Go `KvScanLock`: the addressed region's locks started at or before
    /// `max_version`, the limit counting every lock visited.
    #[test]
    fn scan_lock_answers_the_regions_locks_up_to_max_version() {
        let mut handler = KvHandler::default();
        split_at(&mut handler, b"m");
        prewrite(&mut handler, vec![put(b"a", b"1")], 10);
        prewrite(&mut handler, vec![put(b"b", b"2")], 20);
        prewrite(&mut handler, vec![put(b"n", b"3")], 10);

        assert_eq!(scan_lock(&mut handler, b"a", 15, 10), vec![b"a".to_vec()]);
        assert_eq!(
            scan_lock(&mut handler, b"a", 100, 10),
            vec![b"a".to_vec(), b"b".to_vec()]
        );
        assert_eq!(scan_lock(&mut handler, b"a", 100, 1), vec![b"a".to_vec()]);
        assert!(scan_lock(&mut handler, b"a", 100, 0).is_empty());
        assert_eq!(scan_lock(&mut handler, b"n", 100, 10), vec![b"n".to_vec()]);
        let physical = handler.physical_scan_lock(&kvrpcpb::PhysicalScanLockRequest {
            max_ts: 100,
            start_key: b"b".to_vec(),
            limit: 10,
            ..kvrpcpb::PhysicalScanLockRequest::default()
        });
        let keys: Vec<_> = physical.locks.into_iter().map(|lock| lock.key).collect();
        assert_eq!(keys, vec![b"b".to_vec(), b"n".to_vec()]);
    }

    /// Go `KvResolveLock` with `TxnInfos`: every lock of each transaction
    /// inside the addressed region resolves; other regions keep theirs.
    #[test]
    fn resolve_lock_with_txn_infos_resolves_only_inside_the_region() {
        let mut handler = KvHandler::default();
        split_at(&mut handler, b"m");
        prewrite(&mut handler, vec![put(b"a", b"1")], 10);
        prewrite(&mut handler, vec![put(b"n", b"2")], 10);
        let context = ctx_for(&handler, b"a");
        let resp = handler.kv_resolve_lock(&kvrpcpb::ResolveLockRequest {
            context,
            txn_infos: vec![kvrpcpb::TxnInfo {
                txn: 10,
                status: 11,
                ..kvrpcpb::TxnInfo::default()
            }],
            ..kvrpcpb::ResolveLockRequest::default()
        });
        assert!(resp.error.is_none() && resp.region_error.is_none());
        assert!(scan_lock(&mut handler, b"a", 100, 10).is_empty());
        assert_eq!(scan_lock(&mut handler, b"n", 100, 10), vec![b"n".to_vec()]);
        assert_eq!(get(&mut handler, b"a", 20).value, b"1");
    }

    /// Go `KvGC`: the safe point only rises, and compaction keeps the newest
    /// version at or below it, drops a committed delete there, and drops
    /// rollback records older than it.
    #[test]
    fn kv_gc_raises_the_safe_point_and_compacts_old_versions() {
        let mut handler = KvHandler::default();
        put_committed(&mut handler, b"t1", b"v1", 10, 20);
        put_committed(&mut handler, b"t1", b"v2", 30, 40);
        let gc = |handler: &mut KvHandler, safe_point| {
            handler.kv_gc(&kvrpcpb::GcRequest {
                safe_point,
                ..kvrpcpb::GcRequest::default()
            })
        };
        gc(&mut handler, 35);
        assert_eq!(handler.store.safe_point(), 35);
        let commits = |handler: &KvHandler, key: &[u8]| -> Vec<u64> {
            handler
                .store
                .mvcc_get_by_key(key)
                .writes
                .iter()
                .map(|write| write.commit_ts)
                .collect()
        };
        assert_eq!(commits(&handler, b"t1"), vec![40, 20]);
        put_committed(&mut handler, b"t1", b"v3", 50, 60);
        gc(&mut handler, 70);
        assert_eq!(commits(&handler, b"t1"), vec![60]);

        prewrite(
            &mut handler,
            vec![KvrpcMutation {
                op: KvrpcOp::Del as i32,
                key: b"t1".to_vec(),
                ..KvrpcMutation::default()
            }],
            75,
        );
        commit(&mut handler, &[b"t1"], 75, 80);
        let context = ctx_for(&handler, b"t2");
        handler.kv_batch_rollback(&kvrpcpb::BatchRollbackRequest {
            context,
            keys: vec![b"t2".to_vec()],
            start_version: 85,
            ..kvrpcpb::BatchRollbackRequest::default()
        });
        assert_eq!(commits(&handler, b"t2"), vec![85], "the rollback record");
        gc(&mut handler, 90);
        assert!(commits(&handler, b"t1").is_empty(), "the delete is gone");
        assert!(commits(&handler, b"t2").is_empty(), "the rollback is gone");
        gc(&mut handler, 10);
        assert_eq!(handler.store.safe_point(), 90, "never lowered");
    }

    /// Go `dbWriter.DeleteRange`: later reads find nothing, the snapshot the
    /// key was written at still reads it; an empty end key deletes nothing.
    #[test]
    fn delete_range_hides_keys_from_later_reads_but_not_older_snapshots() {
        let mut handler = KvHandler::default();
        put_committed(&mut handler, b"t1", b"v1", 10, 20);
        put_committed(&mut handler, b"t2", b"v2", 10, 20);
        put_committed(&mut handler, b"t3", b"v3", 10, 20);
        let context = ctx_for(&handler, b"t1");
        let resp = handler.kv_delete_range(&kvrpcpb::DeleteRangeRequest {
            context: context.clone(),
            start_key: b"t1".to_vec(),
            end_key: b"t3".to_vec(),
            ..kvrpcpb::DeleteRangeRequest::default()
        });
        assert!(resp.region_error.is_none() && resp.error.is_empty());
        assert!(get(&mut handler, b"t1", 30).value.is_empty());
        assert!(get(&mut handler, b"t2", 30).value.is_empty());
        assert_eq!(get(&mut handler, b"t1", 20).value, b"v1");
        assert_eq!(get(&mut handler, b"t3", 30).value, b"v3");

        handler.kv_delete_range(&kvrpcpb::DeleteRangeRequest {
            context,
            start_key: b"t3".to_vec(),
            ..kvrpcpb::DeleteRangeRequest::default()
        });
        assert_eq!(get(&mut handler, b"t3", 30).value, b"v3");
    }

    /// Go `UnsafeDestroyRange` (`DeleteFileInRange`): every version in the
    /// range goes, and so do the range's records in the extra keyspace.
    #[test]
    fn unsafe_destroy_range_removes_the_range_and_its_extra_keyspace() {
        let mut handler = KvHandler::default();
        put_committed(&mut handler, b"t1", b"v1", 10, 20);
        put_committed(&mut handler, b"t5", b"v5", 10, 20);
        let context = ctx_for(&handler, b"t2");
        handler.kv_batch_rollback(&kvrpcpb::BatchRollbackRequest {
            context,
            keys: vec![b"t2".to_vec()],
            start_version: 30,
            ..kvrpcpb::BatchRollbackRequest::default()
        });
        handler.unsafe_destroy_range(&kvrpcpb::UnsafeDestroyRangeRequest {
            start_key: b"t1".to_vec(),
            end_key: b"t3".to_vec(),
            ..kvrpcpb::UnsafeDestroyRangeRequest::default()
        });
        assert!(handler.store.mvcc_get_by_key(b"t1").writes.is_empty());
        assert!(handler.store.mvcc_get_by_key(b"t2").writes.is_empty());
        assert_eq!(
            get(&mut handler, b"t1", 20).value,
            b"",
            "even old snapshots"
        );
        assert_eq!(get(&mut handler, b"t5", 30).value, b"v5");
    }

    /// Go `KvCleanup`: an already-committed transaction answers its commit
    /// ts, not an error.
    #[test]
    fn cleanup_answers_an_already_committed_transactions_commit_ts() {
        let mut handler = KvHandler::default();
        put_committed(&mut handler, b"k", b"v", 10, 20);
        let context = ctx_for(&handler, b"k");
        let resp = handler.kv_cleanup(&kvrpcpb::CleanupRequest {
            context,
            key: b"k".to_vec(),
            start_version: 10,
            ..kvrpcpb::CleanupRequest::default()
        });
        assert!(resp.error.is_none());
        assert_eq!(resp.commit_version, 20);
    }

    /// Go `PessimisticRollbackWithScanFirst`: no keys rolls back the
    /// transaction's pessimistic locks in the addressed region only.
    #[test]
    fn pessimistic_rollback_without_keys_rolls_back_the_regions_locks() {
        let mut handler = KvHandler::default();
        split_at(&mut handler, b"m");
        for key in [b"a".as_slice(), b"b", b"n"] {
            let context = ctx_for(&handler, key);
            let resp = handler.kv_pessimistic_lock(&kvrpcpb::PessimisticLockRequest {
                context,
                mutations: vec![KvrpcMutation {
                    op: KvrpcOp::PessimisticLock as i32,
                    key: key.to_vec(),
                    ..KvrpcMutation::default()
                }],
                primary_lock: b"a".to_vec(),
                start_version: 10,
                for_update_ts: 10,
                lock_ttl: 3000,
                ..kvrpcpb::PessimisticLockRequest::default()
            });
            assert!(resp.errors.is_empty(), "{:?}", resp.errors);
        }
        let context = ctx_for(&handler, b"a");
        handler.kv_pessimistic_rollback(&kvrpcpb::PessimisticRollbackRequest {
            context,
            start_version: 10,
            for_update_ts: 10,
            ..kvrpcpb::PessimisticRollbackRequest::default()
        });
        assert!(scan_lock(&mut handler, b"a", 100, 10).is_empty());
        assert_eq!(scan_lock(&mut handler, b"n", 100, 10), vec![b"n".to_vec()]);
    }

    /// Go `KvBufferBatchGet`: the transaction's own buffered puts and
    /// deletes, read back from its locks.
    #[test]
    fn buffer_batch_get_reads_a_transactions_own_locks() {
        let mut handler = KvHandler::default();
        prewrite(
            &mut handler,
            vec![
                put(b"a", b"v"),
                KvrpcMutation {
                    op: KvrpcOp::Del as i32,
                    key: b"b".to_vec(),
                    ..KvrpcMutation::default()
                },
            ],
            10,
        );
        prewrite(&mut handler, vec![put(b"c", b"w")], 20);
        let context = ctx_for(&handler, b"a");
        let resp = handler.kv_buffer_batch_get(&kvrpcpb::BufferBatchGetRequest {
            context,
            keys: vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()],
            version: 10,
            ..kvrpcpb::BufferBatchGetRequest::default()
        });
        let pairs: Vec<_> = resp
            .pairs
            .into_iter()
            .map(|pair| (pair.key, pair.value))
            .collect();
        assert_eq!(
            pairs,
            vec![(b"a".to_vec(), b"v".to_vec()), (b"b".to_vec(), Vec::new())]
        );
    }

    /// Go `MvccGetByStartTs`: only the addressed region is searched; the
    /// answer carries Go's parallel `Values` list.
    #[test]
    fn mvcc_get_by_start_ts_searches_only_the_region() {
        let mut handler = KvHandler::default();
        split_at(&mut handler, b"m");
        put_committed(&mut handler, b"n", b"v", 10, 20);
        let context = ctx_for(&handler, b"a");
        let left = handler.mvcc_get_by_start_ts(&kvrpcpb::MvccGetByStartTsRequest {
            context,
            start_ts: 10,
            ..kvrpcpb::MvccGetByStartTsRequest::default()
        });
        assert!(left.info.is_none() && left.key.is_empty());
        let context = ctx_for(&handler, b"n");
        let right = handler.mvcc_get_by_start_ts(&kvrpcpb::MvccGetByStartTsRequest {
            context,
            start_ts: 10,
            ..kvrpcpb::MvccGetByStartTsRequest::default()
        });
        assert_eq!(right.key, b"n");
        let info = right.info.unwrap();
        assert_eq!(info.writes.len(), 1);
        assert_eq!(info.values[0].start_ts, 10);
        assert_eq!(info.values[0].value, b"v");
    }

    /// Go `StoreBatchCoprocessor`: each task runs as its own request with
    /// the task's region, so a stale task answers its region error alone.
    #[test]
    fn store_batch_tasks_run_as_their_own_region_checked_requests() {
        let mut handler = KvHandler::default();
        let stale = region_ctx().unwrap();
        split_at(&mut handler, b"m");
        let left = ctx_for(&handler, b"a").unwrap();
        let resp = handler.coprocessor(&coprocessor::Request {
            context: Some(left.clone()),
            tp: crate::cophandler::REQ_TYPE_CHECKSUM,
            tasks: vec![
                coprocessor::StoreBatchTask {
                    region_id: left.region_id,
                    region_epoch: left.region_epoch.clone(),
                    peer: left.peer.clone(),
                    task_id: 1,
                    ..coprocessor::StoreBatchTask::default()
                },
                coprocessor::StoreBatchTask {
                    region_id: stale.region_id,
                    region_epoch: stale.region_epoch.clone(),
                    peer: stale.peer.clone(),
                    task_id: 2,
                    ..coprocessor::StoreBatchTask::default()
                },
            ],
            ..coprocessor::Request::default()
        });
        assert!(resp.region_error.is_none());
        assert!(!resp.data.is_empty());
        let batch: Vec<_> = resp
            .batch_responses
            .iter()
            .map(|task| {
                (
                    task.task_id,
                    task.region_error.is_some(),
                    task.data.is_empty(),
                )
            })
            .collect();
        assert_eq!(batch, vec![(1, false, false), (2, true, true)]);

        let stale_resp = handler.coprocessor(&coprocessor::Request {
            context: Some(stale),
            tp: crate::cophandler::REQ_TYPE_CHECKSUM,
            ..coprocessor::Request::default()
        });
        assert!(stale_resp.region_error.unwrap().epoch_not_match.is_some());
    }

    fn pessimistic_lock_request(
        handler: &KvHandler,
        key: &[u8],
        start: u64,
        wait_timeout: i64,
        is_first_lock: bool,
    ) -> kvrpcpb::PessimisticLockRequest {
        kvrpcpb::PessimisticLockRequest {
            context: ctx_for(handler, key),
            mutations: vec![KvrpcMutation {
                op: KvrpcOp::PessimisticLock as i32,
                key: key.to_vec(),
                ..KvrpcMutation::default()
            }],
            primary_lock: key.to_vec(),
            start_version: start,
            for_update_ts: start,
            lock_ttl: 3000,
            wait_timeout,
            is_first_lock,
            ..kvrpcpb::PessimisticLockRequest::default()
        }
    }

    /// Go `Server.KvPessimisticLock` waiting on a lock, run on its own
    /// thread with the store released, as Go's handler goroutine waits.
    fn lock_waiting(
        handler: &Arc<std::sync::Mutex<KvHandler>>,
        req: kvrpcpb::PessimisticLockRequest,
    ) -> std::thread::JoinHandle<kvrpcpb::PessimisticLockResponse> {
        let (response, wait) = handler.lock().unwrap().kv_pessimistic_lock_begin(&req);
        let wait = wait.expect("the request parks on the lock");
        let handler = Arc::clone(handler);
        std::thread::spawn(move || {
            let result = wait.waiter().wait();
            handler
                .lock()
                .unwrap()
                .kv_pessimistic_lock_finish(&req, response, wait, result)
        })
    }

    /// Go: the holder's commit wakes the oldest waiter, which answers a write
    /// conflict at the store's latest commit ts.
    #[test]
    fn a_waiting_pessimistic_lock_wakes_into_a_write_conflict_when_the_holder_commits() {
        let mut handler = KvHandler::default();
        let held = pessimistic_lock_request(&handler, b"k", 10, 0, true);
        assert!(handler.kv_pessimistic_lock(&held).errors.is_empty());
        let handler = Arc::new(std::sync::Mutex::new(handler));
        let waiting = {
            let guard = handler.lock().unwrap();
            pessimistic_lock_request(&guard, b"k", 20, 5000, true)
        };
        let waiter = lock_waiting(&handler, waiting);
        {
            let mut guard = handler.lock().unwrap();
            let context = ctx_for(&guard, b"k");
            let prewrite = guard.kv_prewrite(&kvrpcpb::PrewriteRequest {
                context: context.clone(),
                mutations: vec![put(b"k", b"v")],
                primary_lock: b"k".to_vec(),
                start_version: 10,
                for_update_ts: 10,
                lock_ttl: 3000,
                pessimistic_actions: vec![
                    kvrpcpb::prewrite_request::PessimisticAction::DoPessimisticCheck as i32,
                ],
                ..kvrpcpb::PrewriteRequest::default()
            });
            assert!(prewrite.errors.is_empty(), "{:?}", prewrite.errors);
            let commit = guard.kv_commit(&kvrpcpb::CommitRequest {
                context,
                keys: vec![b"k".to_vec()],
                start_version: 10,
                commit_version: 30,
                ..kvrpcpb::CommitRequest::default()
            });
            assert!(commit.error.is_none());
        }
        let response = waiter.join().unwrap();
        let conflict = response.errors[0]
            .conflict
            .as_ref()
            .expect("a write conflict");
        assert_eq!(
            (
                conflict.start_ts,
                conflict.conflict_ts,
                conflict.conflict_commit_ts
            ),
            (20, 10, 30)
        );
    }

    /// Go `WaitTimeout`: the waiter gives up and answers the lock error.
    #[test]
    fn a_waiting_pessimistic_lock_times_out_with_the_lock_error() {
        let mut handler = KvHandler::default();
        let held = pessimistic_lock_request(&handler, b"k", 10, 0, true);
        assert!(handler.kv_pessimistic_lock(&held).errors.is_empty());
        let handler = Arc::new(std::sync::Mutex::new(handler));
        let waiting = {
            let guard = handler.lock().unwrap();
            pessimistic_lock_request(&guard, b"k", 20, 50, true)
        };
        let started = std::time::Instant::now();
        let response = lock_waiting(&handler, waiting).join().unwrap();
        assert!(started.elapsed() >= std::time::Duration::from_millis(50));
        let locked = response.errors[0].locked.as_ref().expect("the lock error");
        assert_eq!(locked.lock_version, 10);
    }

    /// Go's deadlock detector: two transactions waiting for each other's
    /// keys -- the second to wait answers a deadlock with the wait chain.
    #[test]
    fn crossing_waits_answer_a_deadlock_with_its_wait_chain() {
        let mut handler = KvHandler::default();
        for (key, start) in [(b"k1", 10), (b"k2", 20)] {
            let req = pessimistic_lock_request(&handler, key, start, 0, true);
            assert!(handler.kv_pessimistic_lock(&req).errors.is_empty());
        }
        let handler = Arc::new(std::sync::Mutex::new(handler));
        let (first, second) = {
            let guard = handler.lock().unwrap();
            (
                pessimistic_lock_request(&guard, b"k2", 10, 5000, false),
                pessimistic_lock_request(&guard, b"k1", 20, 5000, false),
            )
        };
        let first_waiter = lock_waiting(&handler, first);
        let response = lock_waiting(&handler, second).join().unwrap();
        let deadlock = response.errors[0].deadlock.as_ref().expect("a deadlock");
        assert_eq!(deadlock.lock_ts, 10);
        assert_eq!(deadlock.lock_key, b"k1");
        assert_eq!(deadlock.wait_chain.len(), 2);
        // Release the first waiter: transaction 20 rolls back its lock.
        {
            let mut guard = handler.lock().unwrap();
            let context = ctx_for(&guard, b"k2");
            guard.kv_pessimistic_rollback(&kvrpcpb::PessimisticRollbackRequest {
                context,
                start_version: 20,
                for_update_ts: 20,
                keys: vec![b"k2".to_vec()],
                ..kvrpcpb::PessimisticRollbackRequest::default()
            });
        }
        let first_response = first_waiter.join().unwrap();
        assert!(first_response.errors[0].conflict.is_some());
    }
}
