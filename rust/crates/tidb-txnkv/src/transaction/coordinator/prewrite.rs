// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Prewrite: publishing every mutation of one region batch, and narrowing the
//! faster-than-2PC protocol decision as TiKV answers.
//!
//! Go boundary: client-go's `prewrite.go` — `twoPhaseCommitter.buildPrewriteRequest`
//! and `prewrite1BatchReqHandler.handleSingleBatchSucceed` — plus the
//! `checkAsyncCommit` / `checkOnePC` admission checks from `2pc.go`.

use tidb_proto::{
    KvrpcAssertionLevel, KvrpcForUpdateTsConstraint, KvrpcKeyError, KvrpcPessimisticAction,
    KvrpcPrewriteRequest, KvrpcPrewriteResponse,
};

use crate::lock::{
    decode_blocking_lock_observation, pessimistic_prewrite_recovery_enabled,
    resolve_blocking_locks, BlockingLock, LockAdmissionError, LockRecoveryClient,
    LockRecoveryResult, TimestampSource,
};
use crate::region::RegionRecoveryLoader;
use crate::rpc::UnaryCallContext;

use super::super::command_client::{PublishedCommand, TransactionCommandClient};
use super::super::mutation::OptimisticMutation;
use super::super::region_batches::RegionMutationBatch;
use super::super::state::TransactionCause;
use super::{
    alive_retry_delay, classify_key_error, wait_with_call, RealOptimisticTransaction,
    ASYNC_COMMIT_KEYS_LIMIT, ASYNC_COMMIT_SAFE_WINDOW_MS, ASYNC_COMMIT_TOTAL_KEY_SIZE_LIMIT,
    TSO_LOGICAL_BITS,
};

impl<C, L, T> RealOptimisticTransaction<C, L, T>
where
    C: TransactionCommandClient + LockRecoveryClient,
    L: RegionRecoveryLoader,
    T: TimestampSource,
{
    /// Decides which faster-than-2PC protocols this exact mutation set may
    /// still attempt.
    ///
    /// Go `twoPhaseCommitter.checkAsyncCommit`/`checkOnePC`: the session
    /// permission is necessary but not sufficient, because an async-commit
    /// primary lock has to carry every secondary key and a lock that large
    /// would cost more to write and to recover than the saved round trip.
    pub(super) fn attempted_protocol(
        &self,
        mutations: &[OptimisticMutation],
        primary_key: &[u8],
    ) -> AttemptedProtocol {
        let total_key_bytes = mutations
            .iter()
            .map(|mutation| mutation.key().len() as u64)
            .sum::<u64>();
        let use_async_commit = self.protocol.async_commit
            && mutations.len() <= ASYNC_COMMIT_KEYS_LIMIT
            && total_key_bytes <= ASYNC_COMMIT_TOTAL_KEY_SIZE_LIMIT;
        AttemptedProtocol {
            use_async_commit,
            use_one_pc: self.protocol.one_pc,
            max_commit_ts: if use_async_commit {
                self.max_commit_ts()
            } else {
                0
            },
            one_pc_commit_ts: 0,
            secondaries: if use_async_commit {
                mutations
                    .iter()
                    .filter(|mutation| mutation.key() != primary_key)
                    .map(|mutation| mutation.key().to_vec())
                    .collect()
            } else {
                Vec::new()
            },
        }
    }

    /// The latest commit timestamp an async-commit prewrite may be granted.
    ///
    /// Go `calculateMaxCommitTS`: a synthetic "now" is derived from the elapsed
    /// wall time since the transaction opened, and the safe window is added on
    /// top. Bounding the commit timestamp is what keeps a schema version valid
    /// for the whole life of the commit even though no PD timestamp is taken.
    fn max_commit_ts(&self) -> u64 {
        let elapsed_ms = u64::try_from(self.opened_at.elapsed().as_millis()).unwrap_or(u64::MAX);
        let current_ts = (elapsed_ms << TSO_LOGICAL_BITS).saturating_add(self.start_ts);
        (ASYNC_COMMIT_SAFE_WINDOW_MS << TSO_LOGICAL_BITS).saturating_add(current_ts)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn prewrite_batch(
        &self,
        batch: &RegionMutationBatch,
        primary_key: &[u8],
        transaction_size: usize,
        lock_ttl_ms: u64,
        is_retry: bool,
        protocol: &AttemptedProtocol,
        call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcPrewriteResponse> {
        let mut request = KvrpcPrewriteRequest {
            mutations: batch
                .mutations()
                .iter()
                .map(OptimisticMutation::to_proto)
                .collect(),
            primary_lock: primary_key.to_vec(),
            start_version: self.start_ts,
            lock_ttl: lock_ttl_ms,
            txn_size: u64::try_from(transaction_size).unwrap_or(u64::MAX),
            min_commit_ts: self.start_ts.saturating_add(1),
            max_commit_ts: protocol.max_commit_ts,
            use_async_commit: protocol.use_async_commit,
            try_one_pc: protocol.use_one_pc,
            assertion_level: KvrpcAssertionLevel::Strict as i32,
            ..KvrpcPrewriteRequest::default()
        };
        // Only the primary lock names the secondaries; that is what makes the
        // primary the single entry point for recovering the transaction.
        if protocol.use_async_commit
            && batch
                .mutations()
                .iter()
                .any(|mutation| mutation.key() == primary_key)
        {
            request.secondaries = protocol.secondaries.clone();
        }
        if let Some(plan) = self.pessimistic.as_ref() {
            request.for_update_ts = plan.for_update_ts;
            request.min_commit_ts = plan.for_update_ts.saturating_add(1);
            request.pessimistic_actions = batch
                .mutations()
                .iter()
                .map(|mutation| {
                    if plan.locked_keys.contains(mutation.key()) {
                        KvrpcPessimisticAction::DoPessimisticCheck as i32
                    } else {
                        KvrpcPessimisticAction::SkipPessimisticCheck as i32
                    }
                })
                .collect();
            request.for_update_ts_constraints = batch
                .mutations()
                .iter()
                .enumerate()
                .filter_map(|(index, mutation)| {
                    let expected = *plan.for_update_ts_constraints.get(mutation.key())?;
                    Some(KvrpcForUpdateTsConstraint {
                        index: u32::try_from(index).unwrap_or(u32::MAX),
                        expected_for_update_ts: expected,
                    })
                })
                .collect();
        }
        request.context = None;
        let mut context = batch.context().clone();
        context.is_retry_request = is_retry;
        match self.runtime.client().try_borrow_mut() {
            Ok(mut client) => client.publish_prewrite(batch.address(), &request, &context, call),
            Err(_) => PublishedCommand::BeforePublication(
                "TiKV client is already borrowed while publishing Prewrite".to_owned(),
            ),
        }
    }

    pub(super) fn handle_prewrite_key_errors(
        &self,
        errors: &[KvrpcKeyError],
        context: &tidb_proto::KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<(), TransactionCause> {
        let mut eligible_locks = Vec::new();
        for error in errors {
            let Some(lock_info) = error.locked.as_ref() else {
                return Err(classify_key_error(error));
            };
            // A Go tidb-server sharing this cluster can leave a genuine
            // pessimistic lock on a key an optimistic prewrite needs, so the
            // observation is admitted under the wider blocking-lock protocol
            // and the narrower optimistic-only refusal is re-imposed below,
            // where it can still be the answer.
            let locks = decode_blocking_lock_observation(lock_info).map_err(|error| {
                TransactionCause::InvalidResponse {
                    detail: format!("invalid Prewrite lock observation: {error}"),
                }
            })?;
            for lock in locks {
                if let BlockingLock::Pessimistic(pessimistic) = &lock {
                    if !pessimistic_prewrite_recovery_enabled() {
                        return Err(TransactionCause::InvalidResponse {
                            detail: format!(
                                "invalid Prewrite lock observation: {}",
                                LockAdmissionError::Pessimistic(pessimistic.lock_type)
                            ),
                        });
                    }
                }
                // client-go `prewrite.go` `handleSingleBatch`: an optimistic
                // committer that meets a lock with a larger start TS will fail
                // with WriteConflict whatever the resolver decides, so the
                // error is constructed here rather than paid for in RPCs. The
                // lock's own protocol is irrelevant to that judgement — only
                // the committer's is — so this stays ahead of the split.
                if lock.txn_id() > self.start_ts {
                    return Err(TransactionCause::WriteConflict {
                        detail: format!(
                            "Prewrite observed newer {} lock txn_id={} start_ts={}",
                            lock.protocol_name(),
                            lock.txn_id(),
                            self.start_ts
                        ),
                    });
                }
                eligible_locks.push(lock);
            }
        }
        if eligible_locks.is_empty() {
            return Err(TransactionCause::InvalidResponse {
                detail: "Prewrite returned an empty KeyError set".to_owned(),
            });
        }
        // `resolve_blocking_locks` dispatches each lock to its own cleanup
        // protocol and is identical to `resolve_optimistic_locks` for an
        // all-optimistic set: the live-owner short circuit it adds keys off
        // `duration_to_last_update_ms`, which only a pessimistic lock reports.
        match resolve_blocking_locks(
            &self.runtime,
            &eligible_locks,
            self.start_ts,
            context,
            call,
            &self.timestamps,
        )
        .map_err(|error| TransactionCause::Lock {
            key: eligible_locks[0].key().to_vec(),
            detail: format!("Prewrite lock recovery failed: {error}"),
        })? {
            LockRecoveryResult::Resolved(_) => Ok(()),
            LockRecoveryResult::Alive(wait) if alive_retry_delay(wait) <= call.timeout() => {
                wait_with_call(call, alive_retry_delay(wait))?;
                Ok(())
            }
            LockRecoveryResult::Alive(wait) => Err(TransactionCause::Lock {
                key: eligible_locks[0].key().to_vec(),
                detail: format!(
                    "Prewrite lock remains alive for {wait:?}, beyond transaction deadline"
                ),
            }),
        }
    }
}

/// One commit's live protocol decision, narrowed as TiKV answers.
///
/// It only ever narrows: every observation can turn a protocol off, none can
/// turn one back on. That is what makes the fallback to normal two-phase commit
/// safe to take at any point during prewrite.
pub(super) struct AttemptedProtocol {
    pub(super) use_async_commit: bool,
    pub(super) use_one_pc: bool,
    pub(super) max_commit_ts: u64,
    pub(super) one_pc_commit_ts: u64,
    pub(super) secondaries: Vec<Vec<u8>>,
}

impl AttemptedProtocol {
    /// Whether a prewrite of this transaction, once published, may already have
    /// decided its outcome.
    ///
    /// Under 1PC the prewrite *is* the commit; under async commit a completed
    /// prewrite *is* the commit point. For both, a prewrite that loses its
    /// answer leaves the transaction undetermined rather than failed. Go
    /// `prewrite.go:352-361`: `if (c.isAsyncCommit() || c.isOnePC()) &&
    /// sender.GetRPCError() != nil && !c.isCanceled() { c.setUndeterminedErr(...) }`.
    pub(super) const fn commit_point_may_have_passed(&self) -> bool {
        self.use_async_commit || self.use_one_pc
    }

    /// Names the protocol in an operator-facing message.
    pub(super) const fn name(&self) -> &'static str {
        match (self.use_one_pc, self.use_async_commit) {
            (true, _) => "1PC",
            (false, true) => "async commit",
            (false, false) => "two-phase commit",
        }
    }

    /// Go `checkOnePCFallBack`: 1PC is a single-region protocol, so the moment
    /// the mutations need more than one region it is off — including when a
    /// region split discovers this mid-prewrite.
    pub(super) fn observe_batch_count(&mut self, batches: usize) {
        if batches > 1 {
            self.use_one_pc = false;
        }
    }

    /// Applies one successful prewrite response to the protocol decision.
    ///
    /// Go `prewrite1BatchReqHandler.handleSingleBatchSucceed`. TiKV signals
    /// refusal by omission: a zeroed `one_pc_commit_ts` under `try_one_pc`, or
    /// a zeroed `min_commit_ts` under `use_async_commit`, each means "finish
    /// this the normal way".
    pub(super) fn observe_prewrite_response(
        &mut self,
        response: &KvrpcPrewriteResponse,
    ) -> Result<(), TransactionCause> {
        if self.use_one_pc {
            if response.one_pc_commit_ts == 0 {
                if response.min_commit_ts != 0 {
                    return Err(TransactionCause::InvalidResponse {
                        detail: format!(
                            "1PC fallback must zero min_commit_ts, got {}",
                            response.min_commit_ts
                        ),
                    });
                }
                self.use_one_pc = false;
                // A 1PC fallback is TiKV declining to commit in the prewrite at
                // all, so async commit cannot rescue this transaction either.
                self.use_async_commit = false;
            } else if self.one_pc_commit_ts != 0 {
                return Err(TransactionCause::InvalidResponse {
                    detail: "1PC committed more than one prewrite batch".to_owned(),
                });
            } else {
                self.one_pc_commit_ts = response.one_pc_commit_ts;
            }
            return Ok(());
        }
        if response.one_pc_commit_ts != 0 {
            return Err(TransactionCause::InvalidResponse {
                detail: format!(
                    "TiKV committed a non-1PC transaction with 1PC at {}",
                    response.one_pc_commit_ts
                ),
            });
        }
        if self.use_async_commit && response.min_commit_ts == 0 {
            self.use_async_commit = false;
        }
        Ok(())
    }
}
