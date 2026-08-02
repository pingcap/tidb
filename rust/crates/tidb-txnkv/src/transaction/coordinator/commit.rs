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

//! Commit: allocating the commit timestamp, committing the primary key that
//! decides the transaction, then committing the secondaries — and the
//! async-commit / 1PC paths that reach the same decision without one of those
//! steps.
//!
//! Go boundary: client-go's `commit.go` (`twoPhaseCommitter.commitMutations`,
//! primary-first ordering, `CommitTsExpired` retry) and the `execute` driver in
//! `2pc.go` that sequences prewrite -> commit_ts -> primary -> secondaries.

use std::collections::VecDeque;

use tidb_proto::{KvrpcCommitRequest, KvrpcCommitRole, KvrpcCommitTsExpired};

use crate::lock::{LockRecoveryClient, TimestampSource};
use crate::region::RegionRecoveryLoader;
use crate::rpc::UnaryCallContext;

use super::super::command_client::{PublishedCommand, TransactionCommandClient};
use super::super::mutation::{validate_and_sort, MutationSetError, OptimisticMutation};
use super::super::region_batches::{group_keys, group_mutations};
use super::super::state::{
    CommittedProtocol, CommittedTransaction, CoordinatorState, OptimisticCommitOutcome,
    OptimisticTransactionReceipt, SecondaryCommitFailure, TransactionAttemptPhase,
    TransactionAttemptResult, TransactionCause, UndeterminedTransaction,
};
use super::{
    classify_key_error, record_attempt, transaction_lock_ttl_ms, OptimisticCoordinatorError,
    RealOptimisticTransaction, RecoveryPhase, MAX_COMMIT_TS_DRIFT_MS, MAX_LOCK_ATTEMPTS,
    TSO_LOGICAL_BITS,
};

impl<C, L, T> RealOptimisticTransaction<C, L, T>
where
    C: TransactionCommandClient + LockRecoveryClient,
    L: RegionRecoveryLoader,
    T: TimestampSource,
{
    /// Consumes this snapshot into one normal optimistic two-phase commit.
    pub fn commit(
        mut self,
        mutations: Vec<OptimisticMutation>,
        call: &UnaryCallContext,
    ) -> Result<OptimisticCommitOutcome, OptimisticCoordinatorError> {
        let mutations =
            validate_and_sort(mutations).map_err(OptimisticCoordinatorError::Mutations)?;
        let (mutations, primary_key) = self
            .pin_primary(mutations)
            .map_err(OptimisticCoordinatorError::Mutations)?;
        let actual_bytes = mutations
            .iter()
            .try_fold(0usize, |size, mutation| {
                size.checked_add(mutation.key().len())?
                    .checked_add(mutation.value().len())
            })
            .unwrap_or(usize::MAX);
        if mutations.len() > self.planned_mutation_count {
            return Err(OptimisticCoordinatorError::Mutations(
                MutationSetError::TooManyMutations {
                    count: mutations.len(),
                    limit: self.planned_mutation_count,
                },
            ));
        }
        if actual_bytes > self.planned_aggregate_bytes {
            return Err(OptimisticCoordinatorError::Mutations(
                MutationSetError::TransactionTooLarge {
                    size: actual_bytes,
                    limit: self.planned_aggregate_bytes,
                },
            ));
        }
        self.state
            .transition(CoordinatorState::Prewriting)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
        let mut receipt = OptimisticTransactionReceipt::new(
            self.authority_id,
            self.start_ts,
            primary_key.clone(),
            mutations.len(),
        );
        let lock_ttl_ms = transaction_lock_ttl_ms(self.opened_at, actual_bytes);
        receipt.lock_ttl_ms = lock_ttl_ms;
        let mut possibly_prewrite_keys = Vec::<Vec<u8>>::new();
        let mut min_commit_ts = self.start_ts.saturating_add(1);
        let mut protocol = self.attempted_protocol(&mutations, &primary_key);
        let mut queue = match group_mutations(&self.runtime, &mutations) {
            Ok(batches) => {
                protocol.observe_batch_count(batches.len());
                VecDeque::from(
                    batches
                        .into_iter()
                        .map(|batch| (batch, false))
                        .collect::<Vec<_>>(),
                )
            }
            Err(error) => {
                return Ok(self.rollback_after_failure(
                    receipt,
                    &[],
                    TransactionCause::Region {
                        detail: format!("initial region grouping failed: {error}"),
                    },
                ));
            }
        };
        let mut lock_attempts = 0usize;

        while let Some((batch, is_retry)) = queue.pop_front() {
            receipt.region_attempts.push(batch.region());
            let published_keys = batch.keys();
            match self.prewrite_batch(
                &batch,
                &primary_key,
                mutations.len(),
                lock_ttl_ms,
                is_retry,
                &protocol,
                call,
            ) {
                PublishedCommand::Response(response) => {
                    possibly_prewrite_keys.extend(published_keys.iter().cloned());
                    receipt
                        .prewrite_attempt_publications
                        .push(response.publication.clone());
                    if let Some(region_error) = response.response.region_error.as_ref() {
                        // Go `prewrite.go:436-443` (`handleRegionErr`): an
                        // `UndeterminedResult` under async commit or 1PC
                        // returns `ErrResultUndetermined` immediately, before
                        // any backoff or relocation. TiKV is saying it does not
                        // know whether this write applied, and for these
                        // protocols the write may have been the commit.
                        if response_is_undetermined(region_error)
                            && protocol.commit_point_may_have_passed()
                        {
                            let cause = TransactionCause::Region {
                                detail: format!(
                                    "Prewrite returned undetermined region error under {}: {region_error:?}",
                                    protocol.name()
                                ),
                            };
                            record_attempt(
                                &mut receipt,
                                TransactionAttemptPhase::Prewrite,
                                &published_keys,
                                &batch,
                                Some(response.publication.clone()),
                                TransactionAttemptResult::Ambiguous(cause.clone()),
                            );
                            return self.undetermined_prewrite(receipt, cause);
                        }
                        let region_cause = TransactionCause::Region {
                            detail: format!("Prewrite region retry: {region_error:?}"),
                        };
                        if let Err(cause) = self.recover_region_error(
                            RecoveryPhase::Forward,
                            region_error,
                            batch.attempt(),
                            call,
                        ) {
                            record_attempt(
                                &mut receipt,
                                TransactionAttemptPhase::Prewrite,
                                &published_keys,
                                &batch,
                                Some(response.publication.clone()),
                                TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                            );
                            return Ok(self.rollback_after_failure(
                                receipt,
                                &possibly_prewrite_keys,
                                cause,
                            ));
                        }
                        record_attempt(
                            &mut receipt,
                            TransactionAttemptPhase::Prewrite,
                            &published_keys,
                            &batch,
                            Some(response.publication.clone()),
                            TransactionAttemptResult::Retry(region_cause),
                        );
                        match group_mutations(&self.runtime, batch.mutations()) {
                            Ok(regrouped) => {
                                protocol.observe_batch_count(regrouped.len());
                                for regrouped_batch in regrouped.into_iter().rev() {
                                    queue.push_front((regrouped_batch, true));
                                }
                                continue;
                            }
                            Err(error) => {
                                return Ok(self.rollback_after_failure(
                                    receipt,
                                    &possibly_prewrite_keys,
                                    TransactionCause::Region {
                                        detail: format!("cannot regroup Prewrite keys: {error}"),
                                    },
                                ));
                            }
                        }
                    }
                    if !response.response.errors.is_empty() {
                        match self.handle_prewrite_key_errors(
                            &response.response.errors,
                            batch.context(),
                            call,
                        ) {
                            Ok(()) if lock_attempts < MAX_LOCK_ATTEMPTS => {
                                record_attempt(
                                    &mut receipt,
                                    TransactionAttemptPhase::Prewrite,
                                    &published_keys,
                                    &batch,
                                    Some(response.publication.clone()),
                                    TransactionAttemptResult::Retry(TransactionCause::Lock {
                                        key: primary_key.clone(),
                                        detail: "Prewrite lock resolved or waited; retrying at the same start_ts".to_owned(),
                                    }),
                                );
                                lock_attempts += 1;
                                queue.push_front((batch, true));
                                continue;
                            }
                            Ok(()) => {
                                let cause = TransactionCause::Lock {
                                    key: primary_key.clone(),
                                    detail: "Prewrite lock retry budget exhausted".to_owned(),
                                };
                                record_attempt(
                                    &mut receipt,
                                    TransactionAttemptPhase::Prewrite,
                                    &published_keys,
                                    &batch,
                                    Some(response.publication.clone()),
                                    TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                                );
                                return Ok(self.rollback_after_failure(
                                    receipt,
                                    &possibly_prewrite_keys,
                                    cause,
                                ));
                            }
                            Err(cause) => {
                                record_attempt(
                                    &mut receipt,
                                    TransactionAttemptPhase::Prewrite,
                                    &published_keys,
                                    &batch,
                                    Some(response.publication.clone()),
                                    TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                                );
                                return Ok(self.rollback_after_failure(
                                    receipt,
                                    &possibly_prewrite_keys,
                                    cause,
                                ));
                            }
                        }
                    }
                    if let Err(cause) = protocol.observe_prewrite_response(&response.response) {
                        record_attempt(
                            &mut receipt,
                            TransactionAttemptPhase::Prewrite,
                            &published_keys,
                            &batch,
                            Some(response.publication.clone()),
                            TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                        );
                        return Ok(self.rollback_after_failure(
                            receipt,
                            &possibly_prewrite_keys,
                            cause,
                        ));
                    }
                    min_commit_ts = min_commit_ts.max(response.response.min_commit_ts);
                    record_attempt(
                        &mut receipt,
                        TransactionAttemptPhase::Prewrite,
                        &published_keys,
                        &batch,
                        Some(response.publication.clone()),
                        TransactionAttemptResult::Confirmed,
                    );
                    receipt.prewrite_publications.push(response.publication);
                }
                PublishedCommand::BeforePublication(error) => {
                    let cause = TransactionCause::Transport {
                        detail: format!("Prewrite failed before publication: {error}"),
                    };
                    record_attempt(
                        &mut receipt,
                        TransactionAttemptPhase::Prewrite,
                        &published_keys,
                        &batch,
                        None,
                        TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                    );
                    return Ok(self.rollback_after_failure(
                        receipt,
                        &possibly_prewrite_keys,
                        cause,
                    ));
                }
                PublishedCommand::AfterPublication { publication, error } => {
                    possibly_prewrite_keys.extend(published_keys.iter().cloned());
                    receipt
                        .prewrite_attempt_publications
                        .push(publication.clone());
                    // Go `prewrite.go:352-361` (`prewrite1BatchReqHandler.drop`):
                    // an async-commit or 1PC committer whose prewrite got no
                    // answer calls `setUndeterminedErr`, because for these
                    // protocols a prewrite that got no answer may already have
                    // committed the transaction. `2pc.go:1717-1737` then runs
                    // cleanup only when `c.getUndeterminedErr() == nil`, so the
                    // locks are left for the lock resolver, which can read the
                    // primary lock's secondary list and decide correctly.
                    // Rolling back here would be a contradicting operation, not
                    // a retry.
                    if protocol.commit_point_may_have_passed() {
                        let cause = TransactionCause::Transport {
                            detail: format!(
                                "Prewrite completion failed after publication under {}: {error}",
                                protocol.name()
                            ),
                        };
                        record_attempt(
                            &mut receipt,
                            TransactionAttemptPhase::Prewrite,
                            &published_keys,
                            &batch,
                            Some(publication),
                            TransactionAttemptResult::Ambiguous(cause.clone()),
                        );
                        return self.undetermined_prewrite(receipt, cause);
                    }
                    let cause = TransactionCause::Transport {
                        detail: format!("Prewrite completion failed after publication: {error}"),
                    };
                    record_attempt(
                        &mut receipt,
                        TransactionAttemptPhase::Prewrite,
                        &published_keys,
                        &batch,
                        Some(publication),
                        TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                    );
                    return Ok(self.rollback_after_failure(
                        receipt,
                        &possibly_prewrite_keys,
                        cause,
                    ));
                }
            }
        }

        self.state
            .transition(CoordinatorState::Prewritten)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;

        let all_mutation_keys = mutations
            .iter()
            .map(|mutation| mutation.key().to_vec())
            .collect::<Vec<_>>();

        // 1PC: TiKV already committed every key while answering the prewrite,
        // so publishing a Commit would be a second, contradictory decision.
        if protocol.use_one_pc {
            if protocol.one_pc_commit_ts == 0 {
                return Ok(self.rollback_after_failure(
                    receipt,
                    &possibly_prewrite_keys,
                    TransactionCause::InvalidResponse {
                        detail: "1PC prewrite reported success without a commit timestamp"
                            .to_owned(),
                    },
                ));
            }
            receipt.commit_ts = protocol.one_pc_commit_ts;
            receipt.commit_protocol = CommittedProtocol::OnePc;
            self.state
                .transition(CoordinatorState::OnePcCommitted)
                .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
            self.state
                .transition(CoordinatorState::Committed)
                .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
            return Ok(OptimisticCommitOutcome::Committed(CommittedTransaction {
                receipt,
                secondary_failures: Vec::new(),
            }));
        }

        // Async commit: the completed prewrite is the commit point and
        // `max(min_commit_ts)` is the commit timestamp, so no second PD round
        // trip happens. The Commit commands below only make the decision
        // visible without a lock resolution; failing them cannot un-commit the
        // transaction, which is why they are reported as secondary failures.
        if protocol.use_async_commit {
            receipt.commit_ts = min_commit_ts;
            receipt.commit_protocol = CommittedProtocol::AsyncCommit;
            self.state
                .transition(CoordinatorState::AsyncCommitted)
                .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
            let failures = self.commit_secondaries(
                &all_mutation_keys,
                &primary_key,
                min_commit_ts,
                true,
                &mut receipt,
            );
            self.state
                .transition(CoordinatorState::Committed)
                .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
            return Ok(OptimisticCommitOutcome::Committed(CommittedTransaction {
                receipt,
                secondary_failures: failures,
            }));
        }

        let commit_ts = match self.commit_timestamp(min_commit_ts, call) {
            Ok(timestamp) => timestamp,
            Err(error) => {
                return Ok(self.rollback_after_failure(receipt, &possibly_prewrite_keys, error));
            }
        };
        receipt.commit_ts = commit_ts;

        self.state
            .transition(CoordinatorState::PrimaryCommitting)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;

        let committed_primary_batch_keys = match self.commit_primary(
            &all_mutation_keys,
            &primary_key,
            commit_ts,
            call,
            &mut receipt,
        ) {
            PrimaryResult::Committed(keys) => keys,
            PrimaryResult::DefinitiveFailure(error) => {
                return Ok(self.rollback_after_failure(receipt, &possibly_prewrite_keys, error));
            }
            PrimaryResult::Undetermined(error) => {
                self.state
                    .transition(CoordinatorState::Undetermined)
                    .map_err(|cause| OptimisticCoordinatorError::SnapshotGet(cause.to_string()))?;
                return Ok(OptimisticCommitOutcome::Undetermined(
                    UndeterminedTransaction {
                        receipt,
                        cause: error,
                    },
                ));
            }
        };
        self.state
            .transition(CoordinatorState::PrimaryCommitted)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;

        let secondary_keys = mutations
            .iter()
            .filter(|mutation| {
                !committed_primary_batch_keys
                    .iter()
                    .any(|key| key.as_slice() == mutation.key())
            })
            .map(|mutation| mutation.key().to_vec())
            .collect::<Vec<_>>();
        if !secondary_keys.is_empty() {
            self.state
                .transition(CoordinatorState::SecondariesCommitting)
                .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
        }
        let secondary_failures = self.commit_secondaries(
            &secondary_keys,
            &primary_key,
            receipt.commit_ts,
            false,
            &mut receipt,
        );
        self.state
            .transition(CoordinatorState::Committed)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
        Ok(OptimisticCommitOutcome::Committed(CommittedTransaction {
            receipt,
            secondary_failures,
        }))
    }

    fn pin_primary(
        &self,
        mutations: Vec<OptimisticMutation>,
    ) -> Result<(Vec<OptimisticMutation>, Vec<u8>), MutationSetError> {
        pin_primary(self.pinned_primary_key.as_deref(), mutations)
    }

    /// Reports a prewrite whose outcome cannot be known, without cleanup.
    ///
    /// Go `2pc.go:1717-1737`: `execute`'s defer runs `c.cleanup(ctx)` only when
    /// `c.getUndeterminedErr() == nil`. Issuing a BatchRollback against a
    /// transaction whose commit point may already have passed is precisely the
    /// operation client-go refuses to perform, so no key is touched here.
    fn undetermined_prewrite(
        &mut self,
        receipt: OptimisticTransactionReceipt,
        cause: TransactionCause,
    ) -> Result<OptimisticCommitOutcome, OptimisticCoordinatorError> {
        self.state
            .transition(CoordinatorState::Undetermined)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
        Ok(OptimisticCommitOutcome::Undetermined(
            UndeterminedTransaction { receipt, cause },
        ))
    }

    fn commit_timestamp(
        &self,
        minimum: u64,
        call: &UnaryCallContext,
    ) -> Result<u64, TransactionCause> {
        for _ in 0..MAX_LOCK_ATTEMPTS {
            if call.cancellation().is_cancelled() || call.timeout().is_zero() {
                return Err(TransactionCause::Transport {
                    detail: "commit timestamp allocation was cancelled".to_owned(),
                });
            }
            let timestamp =
                self.timestamps
                    .current_ts()
                    .map_err(|error| TransactionCause::Timestamp {
                        detail: format!("cannot allocate commit timestamp: {error}"),
                    })?;
            if call.cancellation().is_cancelled() || call.timeout().is_zero() {
                return Err(TransactionCause::Transport {
                    detail: "commit timestamp completed after cancellation".to_owned(),
                });
            }
            if timestamp > self.start_ts && timestamp >= minimum {
                return Ok(timestamp);
            }
        }
        Err(TransactionCause::Timestamp {
            detail: format!(
                "PD did not return commit_ts >= {minimum} and > {}",
                self.start_ts
            ),
        })
    }

    fn commit_primary(
        &mut self,
        primary_batch_keys: &[Vec<u8>],
        primary_key: &[u8],
        mut commit_ts: u64,
        call: &UnaryCallContext,
        receipt: &mut OptimisticTransactionReceipt,
    ) -> PrimaryResult {
        let mut attempt = 0usize;
        loop {
            let routes = match group_keys(&self.runtime, primary_batch_keys) {
                Ok(routes) => routes,
                Err(error) => {
                    return PrimaryResult::DefinitiveFailure(TransactionCause::Region {
                        detail: format!("primary Commit regroup failed: {error}"),
                    });
                }
            };
            let Some(route) = routes
                .into_iter()
                .find(|batch| batch.keys().iter().any(|key| key.as_slice() == primary_key))
            else {
                return PrimaryResult::DefinitiveFailure(TransactionCause::InvalidResponse {
                    detail: "primary Commit regroup lost deterministic primary key".to_owned(),
                });
            };
            receipt.region_attempts.push(route.region());
            let request = KvrpcCommitRequest {
                start_version: self.start_ts,
                keys: route.keys().to_vec(),
                commit_version: commit_ts,
                commit_role: KvrpcCommitRole::Primary as i32,
                primary_key: primary_key.to_vec(),
                use_async_commit: false,
                ..KvrpcCommitRequest::default()
            };
            let mut context = route.context().clone();
            context.is_retry_request = attempt > 0;
            let published = match self.runtime.client().try_borrow_mut() {
                Ok(mut client) => client.publish_commit(route.address(), &request, &context, call),
                Err(_) => PublishedCommand::BeforePublication(
                    "TiKV client is already borrowed while publishing primary Commit".to_owned(),
                ),
            };
            match published {
                PublishedCommand::BeforePublication(error) => {
                    let cause = TransactionCause::Transport {
                        detail: format!("primary Commit failed before publication: {error}"),
                    };
                    record_attempt(
                        receipt,
                        TransactionAttemptPhase::PrimaryCommit,
                        route.keys(),
                        &route,
                        None,
                        TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                    );
                    return PrimaryResult::DefinitiveFailure(cause);
                }
                PublishedCommand::AfterPublication { publication, error } => {
                    receipt.primary_publications.push(publication.clone());
                    let cause = TransactionCause::Transport {
                        detail: format!(
                            "primary Commit completion failed after publication: {error}"
                        ),
                    };
                    record_attempt(
                        receipt,
                        TransactionAttemptPhase::PrimaryCommit,
                        route.keys(),
                        &route,
                        Some(publication),
                        TransactionAttemptResult::Ambiguous(cause.clone()),
                    );
                    return PrimaryResult::Undetermined(cause);
                }
                PublishedCommand::Response(response) => {
                    receipt
                        .primary_publications
                        .push(response.publication.clone());
                    if let Some(region_error) = response.response.region_error.as_ref() {
                        if response_is_undetermined(region_error) {
                            let cause = TransactionCause::Region {
                                detail: format!("primary Commit returned undetermined region error: {region_error:?}"),
                            };
                            record_attempt(
                                receipt,
                                TransactionAttemptPhase::PrimaryCommit,
                                route.keys(),
                                &route,
                                Some(response.publication.clone()),
                                TransactionAttemptResult::Ambiguous(cause.clone()),
                            );
                            return PrimaryResult::Undetermined(cause);
                        }
                        if let Err(cause) = self.recover_region_error(
                            RecoveryPhase::Forward,
                            region_error,
                            route.attempt(),
                            call,
                        ) {
                            // A decoded region error definitively rejected this
                            // publication. Later local recovery failure cannot
                            // turn that rejected attempt into ambiguity.
                            record_attempt(
                                receipt,
                                TransactionAttemptPhase::PrimaryCommit,
                                route.keys(),
                                &route,
                                Some(response.publication.clone()),
                                TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                            );
                            return PrimaryResult::DefinitiveFailure(cause);
                        }
                        record_attempt(
                            receipt,
                            TransactionAttemptPhase::PrimaryCommit,
                            route.keys(),
                            &route,
                            Some(response.publication.clone()),
                            TransactionAttemptResult::Retry(TransactionCause::Region {
                                detail: format!("primary Commit region retry: {region_error:?}"),
                            }),
                        );
                        attempt = attempt.saturating_add(1);
                        continue;
                    }
                    if let Some(error) = response.response.error.as_ref() {
                        if let Some(expired) = error.commit_ts_expired.as_ref() {
                            let minimum = match validate_commit_ts_expired(
                                expired,
                                self.start_ts,
                                primary_key,
                                commit_ts,
                            ) {
                                Ok(minimum) => minimum,
                                Err(cause) => {
                                    record_attempt(
                                        receipt,
                                        TransactionAttemptPhase::PrimaryCommit,
                                        route.keys(),
                                        &route,
                                        Some(response.publication.clone()),
                                        TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                                    );
                                    return PrimaryResult::DefinitiveFailure(cause);
                                }
                            };
                            record_attempt(
                                receipt,
                                TransactionAttemptPhase::PrimaryCommit,
                                route.keys(),
                                &route,
                                Some(response.publication.clone()),
                                TransactionAttemptResult::Retry(TransactionCause::Timestamp {
                                    detail: format!(
                                        "primary Commit retry requires min_commit_ts {minimum}"
                                    ),
                                }),
                            );
                            match self.commit_timestamp(minimum, call) {
                                Ok(new_commit_ts) => {
                                    commit_ts = new_commit_ts;
                                    receipt.commit_ts = new_commit_ts;
                                    attempt = attempt.saturating_add(1);
                                    continue;
                                }
                                Err(cause) => return PrimaryResult::DefinitiveFailure(cause),
                            }
                        }
                        let cause = classify_key_error(error);
                        record_attempt(
                            receipt,
                            TransactionAttemptPhase::PrimaryCommit,
                            route.keys(),
                            &route,
                            Some(response.publication.clone()),
                            TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                        );
                        return PrimaryResult::DefinitiveFailure(cause);
                    }
                    record_attempt(
                        receipt,
                        TransactionAttemptPhase::PrimaryCommit,
                        route.keys(),
                        &route,
                        Some(response.publication.clone()),
                        TransactionAttemptResult::Confirmed,
                    );
                    return PrimaryResult::Committed(route.keys().to_vec());
                }
            }
        }
    }

    /// Commits keys whose outcome is already decided.
    ///
    /// The batch that happens to hold the primary key commits in the primary
    /// role — which only occurs on the async-commit path, where every key is
    /// passed in at once because the decision was already made at prewrite.
    fn commit_secondaries(
        &mut self,
        secondary_keys: &[Vec<u8>],
        primary_key: &[u8],
        commit_ts: u64,
        use_async_commit: bool,
        receipt: &mut OptimisticTransactionReceipt,
    ) -> Vec<SecondaryCommitFailure> {
        if secondary_keys.is_empty() {
            return Vec::new();
        }
        let cleanup_call = UnaryCallContext::with_timeout(self.timeout);
        let mut queue = match group_keys(&self.runtime, secondary_keys) {
            Ok(batches) => VecDeque::from(batches),
            Err(error) => {
                return vec![SecondaryCommitFailure {
                    keys: secondary_keys.to_vec(),
                    region: None,
                    address: None,
                    publication: None,
                    cause: TransactionCause::Region {
                        detail: format!("secondary grouping failed: {error}"),
                    },
                }];
            }
        };
        let mut failures = Vec::new();
        while let Some(batch) = queue.pop_front() {
            receipt.region_attempts.push(batch.region());
            let holds_primary = batch.keys().iter().any(|key| key.as_slice() == primary_key);
            let request = KvrpcCommitRequest {
                start_version: self.start_ts,
                keys: batch.keys().to_vec(),
                commit_version: commit_ts,
                commit_role: if holds_primary {
                    KvrpcCommitRole::Primary as i32
                } else {
                    KvrpcCommitRole::Secondary as i32
                },
                primary_key: primary_key.to_vec(),
                use_async_commit,
                ..KvrpcCommitRequest::default()
            };
            let published = match self.runtime.client().try_borrow_mut() {
                Ok(mut client) => {
                    client.publish_commit(batch.address(), &request, batch.context(), &cleanup_call)
                }
                Err(_) => PublishedCommand::BeforePublication(
                    "TiKV client is already borrowed while publishing secondary Commit".to_owned(),
                ),
            };
            match published {
                PublishedCommand::BeforePublication(error) => {
                    let cause = TransactionCause::Transport {
                        detail: format!("secondary Commit failed before publication: {error}"),
                    };
                    record_attempt(
                        receipt,
                        TransactionAttemptPhase::SecondaryCommit,
                        batch.keys(),
                        &batch,
                        None,
                        TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                    );
                    failures.push(SecondaryCommitFailure {
                        keys: batch.keys().to_vec(),
                        region: Some(batch.region()),
                        address: Some(batch.address().to_owned()),
                        publication: None,
                        cause,
                    });
                }
                PublishedCommand::AfterPublication { publication, error } => {
                    receipt
                        .secondary_attempt_publications
                        .push(publication.clone());
                    let cause = TransactionCause::Transport {
                        detail: format!(
                            "secondary Commit completion failed after publication: {error}"
                        ),
                    };
                    record_attempt(
                        receipt,
                        TransactionAttemptPhase::SecondaryCommit,
                        batch.keys(),
                        &batch,
                        Some(publication.clone()),
                        TransactionAttemptResult::Ambiguous(cause.clone()),
                    );
                    failures.push(SecondaryCommitFailure {
                        keys: batch.keys().to_vec(),
                        region: Some(batch.region()),
                        address: Some(batch.address().to_owned()),
                        publication: Some(publication),
                        cause,
                    });
                }
                PublishedCommand::Response(response) => {
                    receipt
                        .secondary_attempt_publications
                        .push(response.publication.clone());
                    if let Some(region_error) = response.response.region_error.as_ref() {
                        match self.recover_region_error(
                            RecoveryPhase::Secondary,
                            region_error,
                            batch.attempt(),
                            &cleanup_call,
                        ) {
                            Ok(()) => match group_keys(&self.runtime, batch.keys()) {
                                Ok(regrouped) => {
                                    record_attempt(
                                        receipt,
                                        TransactionAttemptPhase::SecondaryCommit,
                                        batch.keys(),
                                        &batch,
                                        Some(response.publication.clone()),
                                        TransactionAttemptResult::Retry(TransactionCause::Region {
                                            detail: format!(
                                                "secondary Commit region retry: {region_error:?}"
                                            ),
                                        }),
                                    );
                                    for item in regrouped.into_iter().rev() {
                                        queue.push_front(item);
                                    }
                                    continue;
                                }
                                Err(error) => {
                                    let cause = TransactionCause::Region {
                                        detail: format!("secondary Commit regroup failed: {error}"),
                                    };
                                    record_attempt(
                                        receipt,
                                        TransactionAttemptPhase::SecondaryCommit,
                                        batch.keys(),
                                        &batch,
                                        Some(response.publication.clone()),
                                        TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                                    );
                                    failures.push(SecondaryCommitFailure {
                                        keys: batch.keys().to_vec(),
                                        region: Some(batch.region()),
                                        address: Some(batch.address().to_owned()),
                                        publication: Some(response.publication.clone()),
                                        cause,
                                    });
                                }
                            },
                            Err(cause) => {
                                record_attempt(
                                    receipt,
                                    TransactionAttemptPhase::SecondaryCommit,
                                    batch.keys(),
                                    &batch,
                                    Some(response.publication.clone()),
                                    TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                                );
                                failures.push(SecondaryCommitFailure {
                                    keys: batch.keys().to_vec(),
                                    region: Some(batch.region()),
                                    address: Some(batch.address().to_owned()),
                                    publication: Some(response.publication.clone()),
                                    cause,
                                });
                            }
                        }
                    } else if let Some(error) = response.response.error.as_ref() {
                        let cause = classify_key_error(error);
                        record_attempt(
                            receipt,
                            TransactionAttemptPhase::SecondaryCommit,
                            batch.keys(),
                            &batch,
                            Some(response.publication.clone()),
                            TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                        );
                        failures.push(SecondaryCommitFailure {
                            keys: batch.keys().to_vec(),
                            region: Some(batch.region()),
                            address: Some(batch.address().to_owned()),
                            publication: Some(response.publication.clone()),
                            cause,
                        });
                    } else {
                        record_attempt(
                            receipt,
                            TransactionAttemptPhase::SecondaryCommit,
                            batch.keys(),
                            &batch,
                            Some(response.publication.clone()),
                            TransactionAttemptResult::Confirmed,
                        );
                        receipt.secondary_publications.push(response.publication);
                    }
                }
            }
        }
        failures
    }
}

/// Decides one commit's primary key and guarantees prewrite will write it.
///
/// Go `twoPhaseCommitter.primary()` (`2pc.go:779-787`):
///
/// > if len(c.primaryKey) == 0 { return c.mutations.GetKey(0) }
/// > return c.primaryKey
///
/// A pessimistic transaction pinned its primary at the first locking statement,
/// and every lock it holds — plus the TTL heartbeat refreshing them — already
/// names that key. Re-deriving the primary from mutation order here would leave
/// two keys each claiming to be primary: the heartbeat would refresh one while
/// the other, the real recovery entry point, expired and was resolved as
/// abandoned. That is a torn transaction, and it is why the plan carries the
/// pinned key rather than defaulting.
///
/// A pinned primary this transaction locked but never wrote is not in the
/// mutation set, and a primary prewrite never writes is no recovery entry point
/// at all. Go covers that with an `Op_Lock` mutation (`2pc.go`
/// `initKeysAndMutations`: `} else if it.Flags().HasLocked() { op =
/// kvrpcpb.Op_Lock }`) and so do we — which is why nothing downstream needs a
/// "the primary is missing" branch.
fn pin_primary(
    pinned: Option<&[u8]>,
    mutations: Vec<OptimisticMutation>,
) -> Result<(Vec<OptimisticMutation>, Vec<u8>), MutationSetError> {
    let Some(pinned) = pinned else {
        let primary_key = mutations[0].key().to_vec();
        return Ok((mutations, primary_key));
    };
    let primary_key = pinned.to_vec();
    if mutations
        .iter()
        .any(|mutation| mutation.key() == primary_key.as_slice())
    {
        return Ok((mutations, primary_key));
    }
    let mut extended = mutations;
    extended.push(OptimisticMutation::lock_only(primary_key.clone())?);
    Ok((validate_and_sort(extended)?, primary_key))
}

enum PrimaryResult {
    Committed(Vec<Vec<u8>>),
    DefinitiveFailure(TransactionCause),
    Undetermined(TransactionCause),
}

/// TiKV explicitly saying "I do not know whether this write applied".
///
/// Go `commit.go:151-158` uses it on the primary Commit and
/// `prewrite.go:436-443` on prewrite; the signal is the same either way, so it
/// is one predicate serving both callers rather than a primary-only helper the
/// prewrite path could forget to consult.
fn response_is_undetermined(error: &tidb_proto::RegionError) -> bool {
    error.undetermined_result.is_some()
}

fn validate_commit_ts_expired(
    expired: &KvrpcCommitTsExpired,
    start_ts: u64,
    primary_key: &[u8],
    attempted_commit_ts: u64,
) -> Result<u64, TransactionCause> {
    let latest_pinned_min_commit_ts = attempted_commit_ts
        .saturating_add(MAX_COMMIT_TS_DRIFT_MS.saturating_mul(1_u64 << TSO_LOGICAL_BITS));
    if expired.start_ts != start_ts
        || expired.attempted_commit_ts != attempted_commit_ts
        || expired.key != primary_key
        || expired.min_commit_ts <= attempted_commit_ts
        || expired.min_commit_ts > latest_pinned_min_commit_ts
    {
        return Err(TransactionCause::InvalidResponse {
            detail: format!(
                "CommitTsExpired violates pinned primary retry contract: {expired:?}; attempted_commit_ts={attempted_commit_ts}, latest_min_commit_ts={latest_pinned_min_commit_ts}"
            ),
        });
    }
    Ok(expired.min_commit_ts)
}

#[cfg(test)]
mod tests {
    use super::super::super::mutation::OptimisticMutationKind;
    use super::*;

    fn sorted(keys: &[&[u8]]) -> Vec<OptimisticMutation> {
        validate_and_sort(
            keys.iter()
                .map(|key| OptimisticMutation::put_existing(key.to_vec(), b"v".to_vec()).unwrap())
                .collect(),
        )
        .unwrap()
    }

    /// Go `2pc.go:779-787`: `if len(c.primaryKey) == 0 { return
    /// c.mutations.GetKey(0) }; return c.primaryKey`.
    #[test]
    fn pinned_pessimistic_primary_survives_mutation_order() {
        // k5 was locked first, so it is the primary of record even though k3
        // sorts smaller. This is the torn-transaction case: before the pin was
        // carried, prewrite named k3 while the heartbeat refreshed k5.
        let mutations = sorted(&[b"k3", b"k5"]);
        let (result, primary) = pin_primary(Some(b"k5"), mutations).unwrap();
        assert_eq!(primary, b"k5".to_vec());
        assert_eq!(result.len(), 2, "no Op_Lock is needed when k5 is written");
        assert!(result.iter().any(|m| m.key() == b"k5"));
    }

    /// Go `2pc.go:697-698` sets `c.primaryKey` only when it is still empty, so
    /// a transaction that never locked keeps mutation order as its primary.
    #[test]
    fn unpinned_transaction_still_uses_the_smallest_key() {
        let mutations = sorted(&[b"k3", b"k5"]);
        let (result, primary) = pin_primary(None, mutations).unwrap();
        assert_eq!(primary, b"k3".to_vec());
        assert_eq!(result.len(), 2);
    }

    /// Go `2pc.go` `initKeysAndMutations`: a locked-but-unwritten key becomes
    /// an `Op_Lock` mutation, so the primary lock always gets written.
    #[test]
    fn primary_locked_but_never_written_is_prewritten_as_op_lock() {
        // `SELECT ... FOR UPDATE` on k9, then `UPDATE` of k3 only.
        let mutations = sorted(&[b"k3"]);
        let (result, primary) = pin_primary(Some(b"k9"), mutations).unwrap();
        assert_eq!(primary, b"k9".to_vec());
        assert_eq!(result.len(), 2);
        let lock = result
            .iter()
            .find(|m| m.key() == b"k9")
            .expect("the pinned primary must be prewritten");
        assert_eq!(lock.kind(), OptimisticMutationKind::LockOnly);
        assert_eq!(lock.to_proto().op, tidb_proto::KvrpcOp::Lock as i32);
        assert!(lock.value().is_empty());
        // The added mutation must not break the sorted invariant the region
        // grouping depends on.
        assert!(result.windows(2).all(|w| w[0].key() < w[1].key()));
    }

    /// The whole point of the pin: whatever key the caller's TTL heartbeat
    /// refreshes is the key prewrite names as `primary_lock`.
    #[test]
    fn heartbeat_key_and_prewrite_primary_are_the_same_key() {
        for heartbeat_key in [b"k1".as_slice(), b"k5", b"k9"] {
            let (result, primary) =
                pin_primary(Some(heartbeat_key), sorted(&[b"k3", b"k5"])).unwrap();
            assert_eq!(primary, heartbeat_key.to_vec());
            assert!(
                result.iter().any(|m| m.key() == heartbeat_key),
                "the refreshed key must be prewritten"
            );
        }
    }

    #[test]
    fn commit_ts_expired_retry_is_pinned_to_exact_attempt_and_one_hour() {
        let start_ts = 10_u64 << TSO_LOGICAL_BITS;
        let attempted_commit_ts = start_ts + ((2 * MAX_COMMIT_TS_DRIFT_MS) << TSO_LOGICAL_BITS);
        let within_hour = attempted_commit_ts + ((MAX_COMMIT_TS_DRIFT_MS - 1) << TSO_LOGICAL_BITS);
        let valid = KvrpcCommitTsExpired {
            start_ts,
            attempted_commit_ts,
            key: b"primary".to_vec(),
            min_commit_ts: within_hour,
        };
        assert_eq!(
            validate_commit_ts_expired(&valid, start_ts, b"primary", attempted_commit_ts),
            Ok(within_hour)
        );

        let mut wrong_attempt = valid.clone();
        wrong_attempt.attempted_commit_ts += 1;
        assert!(matches!(
            validate_commit_ts_expired(&wrong_attempt, start_ts, b"primary", attempted_commit_ts),
            Err(TransactionCause::InvalidResponse { .. })
        ));

        let mut beyond_pin = valid;
        beyond_pin.min_commit_ts =
            attempted_commit_ts + ((MAX_COMMIT_TS_DRIFT_MS + 1) << TSO_LOGICAL_BITS);
        assert!(matches!(
            validate_commit_ts_expired(&beyond_pin, start_ts, b"primary", attempted_commit_ts),
            Err(TransactionCause::InvalidResponse { .. })
        ));
    }

    fn protocol(one_pc: bool, async_commit: bool) -> super::super::prewrite::AttemptedProtocol {
        super::super::prewrite::AttemptedProtocol {
            use_async_commit: async_commit,
            use_one_pc: one_pc,
            max_commit_ts: 0,
            one_pc_commit_ts: 0,
            secondaries: Vec::new(),
        }
    }

    /// Go `prewrite.go:352-361`: `if (c.isAsyncCommit() || c.isOnePC()) &&
    /// sender.GetRPCError() != nil && !c.isCanceled() { c.setUndeterminedErr }`.
    /// Under 1PC the prewrite IS the commit; under async commit a completed
    /// prewrite IS the commit point.
    #[test]
    fn a_lost_prewrite_answer_is_undetermined_only_under_one_pc_or_async_commit() {
        assert!(protocol(true, false).commit_point_may_have_passed());
        assert!(protocol(false, true).commit_point_may_have_passed());
        assert!(protocol(true, true).commit_point_may_have_passed());
        // Plain 2PC has decided nothing at prewrite, so a lost answer is a
        // definitive failure and rolling back is correct — client-go's
        // `2pc.go:1689-1698` cleanup path.
        assert!(!protocol(false, false).commit_point_may_have_passed());
    }

    /// Once TiKV declines 1PC and async commit, no commit point has passed and
    /// the rollback branch is correct again.
    #[test]
    fn a_protocol_that_fell_back_to_two_phase_commit_is_determinate_again() {
        let mut fell_back = protocol(true, true);
        fell_back
            .observe_prewrite_response(&tidb_proto::KvrpcPrewriteResponse::default())
            .unwrap();
        assert!(!fell_back.commit_point_may_have_passed());
        assert_eq!(fell_back.name(), "two-phase commit");
    }

    #[test]
    fn protocol_names_itself_for_the_operator() {
        assert_eq!(protocol(true, true).name(), "1PC");
        assert_eq!(protocol(false, true).name(), "async commit");
        assert_eq!(protocol(false, false).name(), "two-phase commit");
    }

    /// The state machine has to admit the new edge, or the routing would turn
    /// a wrong rollback into a hard error instead of an honest verdict.
    #[test]
    fn prewriting_may_reach_undetermined_without_a_primary_commit() {
        let mut state = CoordinatorState::Prewriting;
        assert_eq!(state.transition(CoordinatorState::Undetermined), Ok(()));
        // Undetermined is terminal: nothing may follow it, least of all a
        // rollback. Go `2pc.go:1717-1737` skips cleanup for exactly this case.
        let mut terminal = CoordinatorState::Undetermined;
        assert!(terminal.transition(CoordinatorState::RollingBack).is_err());
        assert!(terminal.transition(CoordinatorState::Committed).is_err());
    }

    #[test]
    fn only_explicit_undetermined_response_is_undetermined() {
        assert!(!response_is_undetermined(
            &tidb_proto::RegionError::default()
        ));
        let undetermined = tidb_proto::RegionError {
            undetermined_result: Some(tidb_proto::errorpb::UndeterminedResult::default()),
            ..tidb_proto::RegionError::default()
        };
        assert!(response_is_undetermined(&undetermined));
    }
}
