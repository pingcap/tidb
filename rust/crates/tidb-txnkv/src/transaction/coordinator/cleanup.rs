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

//! The two ways a transaction ends without a commit: finishing a statement that
//! turned out to have nothing to write, and rolling back every key a failed
//! prewrite may already have locked.
//!
//! Go boundary: client-go's `cleanup.go` — `twoPhaseCommitter.cleanupMutations`
//! publishing BatchRollback per region, its own backoff budget, and per-batch
//! failures reported rather than swallowed.

use std::collections::VecDeque;

use tidb_proto::KvrpcBatchRollbackRequest;

use crate::lock::{LockRecoveryClient, TimestampSource};
use crate::region::RegionRecoveryLoader;
use crate::rpc::UnaryCallContext;

use super::super::command_client::{PublishedCommand, TransactionCommandClient};
use super::super::region_batches::group_keys;
use super::super::state::{
    CleanupBatchFailure, CleanupFailedTransaction, CoordinatorState, OptimisticCommitOutcome,
    OptimisticTransactionReceipt, OptimisticTransactionState, ReadOnlyTransaction,
    RolledBackTransaction, TransactionAttemptPhase, TransactionAttemptResult, TransactionCause,
};
use super::{
    classify_key_error, record_attempt, OptimisticCoordinatorError, RealOptimisticTransaction,
    RecoveryPhase,
};

impl<C, L, T> RealOptimisticTransaction<C, L, T>
where
    C: TransactionCommandClient + LockRecoveryClient,
    L: RegionRecoveryLoader,
    T: TimestampSource,
{
    /// Completes a missing or unchanged UPDATE with no write publication.
    pub fn finish_without_writes(
        mut self,
    ) -> Result<ReadOnlyTransaction, OptimisticCoordinatorError> {
        self.state
            .transition(CoordinatorState::ReadOnly)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
        Ok(ReadOnlyTransaction {
            authority_id: self.authority_id,
            start_ts: self.start_ts,
            state: OptimisticTransactionState::ReadOnly,
            snapshot_reads: self.snapshot_reads,
        })
    }

    pub(super) fn rollback_after_failure(
        &mut self,
        mut receipt: OptimisticTransactionReceipt,
        keys: &[Vec<u8>],
        cause: TransactionCause,
    ) -> OptimisticCommitOutcome {
        if let Err(error) = self.state.transition(CoordinatorState::RollingBack) {
            return OptimisticCommitOutcome::CleanupFailed(CleanupFailedTransaction {
                receipt,
                cause,
                cleanup_failures: vec![CleanupBatchFailure {
                    keys: keys.to_vec(),
                    region: None,
                    address: None,
                    publication: None,
                    cause: error,
                }],
            });
        }
        let cleanup_failures = self.rollback_keys(keys, &mut receipt);
        if cleanup_failures.is_empty() {
            if let Err(error) = self.state.transition(CoordinatorState::RolledBack) {
                return OptimisticCommitOutcome::CleanupFailed(CleanupFailedTransaction {
                    receipt,
                    cause,
                    cleanup_failures: vec![CleanupBatchFailure {
                        keys: keys.to_vec(),
                        region: None,
                        address: None,
                        publication: None,
                        cause: error,
                    }],
                });
            }
            OptimisticCommitOutcome::RolledBack(RolledBackTransaction { receipt, cause })
        } else {
            let _ = self.state.transition(CoordinatorState::CleanupFailed);
            OptimisticCommitOutcome::CleanupFailed(CleanupFailedTransaction {
                receipt,
                cause,
                cleanup_failures,
            })
        }
    }

    fn rollback_keys(
        &mut self,
        keys: &[Vec<u8>],
        receipt: &mut OptimisticTransactionReceipt,
    ) -> Vec<CleanupBatchFailure> {
        if keys.is_empty() {
            return Vec::new();
        }
        let cleanup_call = UnaryCallContext::with_timeout(self.timeout);
        let mut queue = match group_keys(&self.runtime, keys) {
            Ok(batches) => VecDeque::from(batches),
            Err(error) => {
                return vec![CleanupBatchFailure {
                    keys: keys.to_vec(),
                    region: None,
                    address: None,
                    publication: None,
                    cause: TransactionCause::Region {
                        detail: format!("rollback grouping failed: {error}"),
                    },
                }];
            }
        };
        let mut failures = Vec::new();
        while let Some(batch) = queue.pop_front() {
            receipt.region_attempts.push(batch.region());
            let request = KvrpcBatchRollbackRequest {
                start_version: self.start_ts,
                keys: batch.keys().to_vec(),
                ..KvrpcBatchRollbackRequest::default()
            };
            let published = match self.runtime.client().try_borrow_mut() {
                Ok(mut client) => client.publish_batch_rollback(
                    batch.address(),
                    &request,
                    batch.context(),
                    &cleanup_call,
                ),
                Err(_) => PublishedCommand::BeforePublication(
                    "TiKV client is already borrowed while publishing BatchRollback".to_owned(),
                ),
            };
            match published {
                PublishedCommand::BeforePublication(error) => {
                    let cause = TransactionCause::Transport {
                        detail: format!("BatchRollback failed before publication: {error}"),
                    };
                    record_attempt(
                        receipt,
                        TransactionAttemptPhase::BatchRollback,
                        batch.keys(),
                        &batch,
                        None,
                        TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                    );
                    failures.push(CleanupBatchFailure {
                        keys: batch.keys().to_vec(),
                        region: Some(batch.region()),
                        address: Some(batch.address().to_owned()),
                        publication: None,
                        cause,
                    });
                }
                PublishedCommand::AfterPublication { publication, error } => {
                    receipt
                        .rollback_attempt_publications
                        .push(publication.clone());
                    let cause = TransactionCause::Transport {
                        detail: format!(
                            "BatchRollback completion failed after publication: {error}"
                        ),
                    };
                    record_attempt(
                        receipt,
                        TransactionAttemptPhase::BatchRollback,
                        batch.keys(),
                        &batch,
                        Some(publication.clone()),
                        TransactionAttemptResult::Ambiguous(cause.clone()),
                    );
                    failures.push(CleanupBatchFailure {
                        keys: batch.keys().to_vec(),
                        region: Some(batch.region()),
                        address: Some(batch.address().to_owned()),
                        publication: Some(publication),
                        cause,
                    });
                }
                PublishedCommand::Response(response) => {
                    receipt
                        .rollback_attempt_publications
                        .push(response.publication.clone());
                    if let Some(region_error) = response.response.region_error.as_ref() {
                        match self.recover_region_error(
                            RecoveryPhase::Cleanup,
                            region_error,
                            batch.attempt(),
                            &cleanup_call,
                        ) {
                            Ok(()) => match group_keys(&self.runtime, batch.keys()) {
                                Ok(regrouped) => {
                                    record_attempt(
                                        receipt,
                                        TransactionAttemptPhase::BatchRollback,
                                        batch.keys(),
                                        &batch,
                                        Some(response.publication.clone()),
                                        TransactionAttemptResult::Retry(TransactionCause::Region {
                                            detail: format!(
                                                "BatchRollback region retry: {region_error:?}"
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
                                        detail: format!("BatchRollback regroup failed: {error}"),
                                    };
                                    record_attempt(
                                        receipt,
                                        TransactionAttemptPhase::BatchRollback,
                                        batch.keys(),
                                        &batch,
                                        Some(response.publication.clone()),
                                        TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                                    );
                                    failures.push(CleanupBatchFailure {
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
                                    TransactionAttemptPhase::BatchRollback,
                                    batch.keys(),
                                    &batch,
                                    Some(response.publication.clone()),
                                    TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                                );
                                failures.push(CleanupBatchFailure {
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
                            TransactionAttemptPhase::BatchRollback,
                            batch.keys(),
                            &batch,
                            Some(response.publication.clone()),
                            TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                        );
                        failures.push(CleanupBatchFailure {
                            keys: batch.keys().to_vec(),
                            region: Some(batch.region()),
                            address: Some(batch.address().to_owned()),
                            publication: Some(response.publication.clone()),
                            cause,
                        });
                    } else {
                        record_attempt(
                            receipt,
                            TransactionAttemptPhase::BatchRollback,
                            batch.keys(),
                            &batch,
                            Some(response.publication.clone()),
                            TransactionAttemptResult::Confirmed,
                        );
                        receipt.rollback_publications.push(response.publication);
                    }
                }
            }
        }
        failures
    }
}
