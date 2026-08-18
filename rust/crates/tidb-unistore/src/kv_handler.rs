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
//! SEED of `server.go` (1,274 lines): the transactional command set lands —
//! get, scan, batch-get, prewrite, commit, batch-rollback, pessimistic
//! lock/rollback, txn heartbeat, check-txn-status, resolve-lock. Raw KV,
//! coprocessor dispatch, MPP, and the debug service are later courses.
//!
//! # Narrowings, by name
//!
//! * `newRequestCtx` / `regionCtx` / `reqCtx.regErr`: region-epoch
//!   validation against the region manager. This handler serves ONE region
//!   covering the whole keyspace, so the epoch cannot mismatch and the
//!   region-error arm of every response stays empty — exactly what Go
//!   produces for an unsplit bootstrap store. The region manager arrives
//!   with the region course.
//! * `svr.mvccStore.Prewrite`'s async/1PC response fields
//!   (`asyncMinCommitTS`, `onePCCommitTS`) follow the async course.
//! * `extractRegionError` / `pberror.PBError`: no region errors exist here
//!   yet, so `convertToPBError`'s region arm is vacuous.

use tidb_proto::kvrpcpb;
use tidb_proto::KvrpcKeyError;

use crate::mvcc_store::{
    CheckTxnStatusReq, KvError, MvccStore, PessimisticLockReq, PrewriteReq, ScanReq,
};

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
    }
}

fn abort(message: &str) -> KvrpcKeyError {
    KvrpcKeyError {
        abort: message.to_owned(),
        ..KvrpcKeyError::default()
    }
}

/// The in-process KV service over one whole-keyspace region.
#[derive(Debug, Default)]
pub struct KvHandler {
    /// The store beneath.
    pub store: MvccStore,
}

impl KvHandler {
    /// Go `Server.KvGet` (`server.go`).
    pub fn kv_get(&mut self, req: &kvrpcpb::GetRequest) -> kvrpcpb::GetResponse {
        match self.store.get(&req.key, req.version) {
            Ok(value) => kvrpcpb::GetResponse {
                value: value.unwrap_or_default(),
                not_found: false,
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
        let pairs = self.store.scan(&ScanReq {
            start_key: req.start_key.clone(),
            end_key: req.end_key.clone(),
            limit: req.limit,
            version: req.version,
            sample_step: req.sample_step,
            reverse: req.reverse,
        });
        kvrpcpb::ScanResponse {
            pairs: pairs.into_iter().map(pair_to_proto).collect(),
            ..kvrpcpb::ScanResponse::default()
        }
    }

    /// Go `Server.KvBatchGet`. The trimmed proto crate carries no
    /// `BatchGetResponse` (a `tidb-proto` curation boundary, named); the
    /// response's whole payload IS its pair list, returned directly.
    pub fn kv_batch_get(&mut self, req: &kvrpcpb::BatchGetRequest) -> Vec<kvrpcpb::KvPair> {
        self.store
            .batch_get(&req.keys, req.version)
            .into_iter()
            .map(pair_to_proto)
            .collect()
    }

    /// Go `Server.KvPrewrite`.
    pub fn kv_prewrite(&mut self, req: &kvrpcpb::PrewriteRequest) -> kvrpcpb::PrewriteResponse {
        let reduced = PrewriteReq {
            mutations: req.mutations.clone(),
            primary_lock: req.primary_lock.clone(),
            start_version: req.start_version,
            lock_ttl: req.lock_ttl,
            for_update_ts: req.for_update_ts,
            min_commit_ts: req.min_commit_ts,
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
        match self.store.prewrite(&reduced) {
            Ok(()) => kvrpcpb::PrewriteResponse::default(),
            Err(err) => kvrpcpb::PrewriteResponse {
                errors: vec![convert_to_key_error(&err)],
                ..kvrpcpb::PrewriteResponse::default()
            },
        }
    }

    /// Go `Server.KvCommit`.
    pub fn kv_commit(&mut self, req: &kvrpcpb::CommitRequest) -> kvrpcpb::CommitResponse {
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
        match self.store.rollback(&req.keys, req.start_version) {
            Ok(()) => kvrpcpb::BatchRollbackResponse::default(),
            Err(err) => kvrpcpb::BatchRollbackResponse {
                error: Some(convert_to_key_error(&err)),
                ..kvrpcpb::BatchRollbackResponse::default()
            },
        }
    }

    /// Go `Server.KvPessimisticLock`, the no-wait slice.
    pub fn kv_pessimistic_lock(
        &mut self,
        req: &kvrpcpb::PessimisticLockRequest,
    ) -> kvrpcpb::PessimisticLockResponse {
        let force_lock =
            req.wake_up_mode == kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeForceLock as i32;
        // Go `pessimisticLockInner`'s guard, message verbatim: ForceLock
        // addresses exactly one key.
        if force_lock && req.mutations.len() > 1 {
            return kvrpcpb::PessimisticLockResponse {
                errors: vec![kvrpcpb::KeyError {
                    abort: "Trying to lock more than one key in WakeUpModeForceLock, \
                            which is not supported yet"
                        .to_owned(),
                    ..kvrpcpb::KeyError::default()
                }],
                ..kvrpcpb::PessimisticLockResponse::default()
            };
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
        };
        match self.store.pessimistic_lock(&reduced) {
            Ok(result) => {
                // Go's ForceLock arm fills one LockResultNormal per granted
                // key; the client reads the TYPE (and a conflict timestamp
                // this simplified store never grants past).
                let results = if force_lock {
                    req.mutations
                        .iter()
                        .map(|_| kvrpcpb::PessimisticLockKeyResult {
                            r#type: kvrpcpb::PessimisticLockKeyResultType::LockResultNormal as i32,
                            ..kvrpcpb::PessimisticLockKeyResult::default()
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                kvrpcpb::PessimisticLockResponse {
                    values: result.values,
                    not_founds: result.not_founds,
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
        }
    }

    /// Go `Server.KvPessimisticRollback`.
    pub fn kv_pessimistic_rollback(
        &mut self,
        req: &kvrpcpb::PessimisticRollbackRequest,
    ) -> kvrpcpb::PessimisticRollbackResponse {
        self.store
            .pessimistic_rollback(&req.keys, req.start_version, req.for_update_ts);
        kvrpcpb::PessimisticRollbackResponse::default()
    }

    /// Go `Server.KvTxnHeartBeat`.
    pub fn kv_txn_heart_beat(
        &mut self,
        req: &kvrpcpb::TxnHeartBeatRequest,
    ) -> kvrpcpb::TxnHeartBeatResponse {
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
        match self
            .store
            .resolve_lock(&req.keys, req.start_version, req.commit_version)
        {
            Ok(()) => kvrpcpb::ResolveLockResponse::default(),
            Err(err) => kvrpcpb::ResolveLockResponse {
                error: Some(convert_to_key_error(&err)),
                ..kvrpcpb::ResolveLockResponse::default()
            },
        }
    }
}

/// A store pair into the proto pair, error arm included.
fn pair_to_proto(pair: crate::mvcc_store::KvPair) -> kvrpcpb::KvPair {
    kvrpcpb::KvPair {
        key: pair.key,
        value: pair.value,
        error: pair.error.map(|info| KvrpcKeyError {
            locked: Some(*info),
            ..KvrpcKeyError::default()
        }),
        ..kvrpcpb::KvPair::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_proto::{KvrpcMutation, KvrpcOp};

    // All WRITTEN: server.go's own coverage is the RPC integration suites.
    // These pin the proto shapes a client parses.

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
            mutations: vec![put(&key, b"v")],
            primary_lock: key.clone(),
            start_version: 10,
            lock_ttl: 3000,
            ..kvrpcpb::PrewriteRequest::default()
        });
        assert!(resp.errors.is_empty(), "{:?}", resp.errors);
        let resp = handler.kv_commit(&kvrpcpb::CommitRequest {
            keys: vec![key.clone()],
            start_version: 10,
            commit_version: 11,
            ..kvrpcpb::CommitRequest::default()
        });
        assert!(resp.error.is_none());
        let resp = handler.kv_get(&kvrpcpb::GetRequest {
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
            mutations: vec![put(&key, b"v")],
            primary_lock: key.clone(),
            start_version: start_ts,
            lock_ttl: 10,
            ..kvrpcpb::PrewriteRequest::default()
        });
        let resp = handler.kv_get(&kvrpcpb::GetRequest {
            key: key.clone(),
            version: start_ts + 1,
            ..kvrpcpb::GetRequest::default()
        });
        let locked = resp.error.expect("locked").locked.expect("the lock info");
        assert_eq!(locked.lock_version, start_ts);
        // Status check at an expired current ts rolls the lock back...
        let resp = handler.kv_check_txn_status(&kvrpcpb::CheckTxnStatusRequest {
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
            mutations: vec![put(&key, b"v1")],
            primary_lock: key.clone(),
            start_version: 10,
            ..kvrpcpb::PrewriteRequest::default()
        });
        handler.kv_commit(&kvrpcpb::CommitRequest {
            keys: vec![key.clone()],
            start_version: 10,
            commit_version: 20,
            ..kvrpcpb::CommitRequest::default()
        });
        let resp = handler.kv_prewrite(&kvrpcpb::PrewriteRequest {
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
}
