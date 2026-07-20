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

use crate::region::RegionVerId;
use crate::rpc::TransactionBatchPublication;

/// Typed error identity used by executor/server mapping and receipts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransactionCause {
    /// Insert assertion observed an existing encoded key.
    AlreadyExists {
        /// Exact encoded existing key.
        key: Vec<u8>,
        /// TiKV diagnostic detail.
        detail: String,
    },
    /// A strict mutation assertion failed.
    AssertionFailed {
        /// Exact encoded assertion key.
        key: Vec<u8>,
        /// TiKV diagnostic detail.
        detail: String,
    },
    /// TiKV reported an optimistic write conflict.
    WriteConflict {
        /// TiKV conflict diagnostic.
        detail: String,
    },
    /// A lock prevented a determinate mutation result.
    Lock {
        /// Exact encoded locked key.
        key: Vec<u8>,
        /// Lock recovery diagnostic.
        detail: String,
    },
    /// Region routing or a definitive region response failed.
    Region {
        /// Region recovery diagnostic.
        detail: String,
    },
    /// Physical transport or local completion failed.
    Transport {
        /// Physical transport diagnostic.
        detail: String,
    },
    /// PD timestamp allocation failed.
    Timestamp {
        /// PD timestamp diagnostic.
        detail: String,
    },
    /// A response violated the bounded transaction contract.
    InvalidResponse {
        /// Exact bounded-contract violation.
        detail: String,
    },
}

impl std::fmt::Display for TransactionCause {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyExists { detail, .. }
            | Self::AssertionFailed { detail, .. }
            | Self::WriteConflict { detail }
            | Self::Lock { detail, .. }
            | Self::Region { detail }
            | Self::Transport { detail }
            | Self::Timestamp { detail }
            | Self::InvalidResponse { detail } => formatter.write_str(detail),
        }
    }
}

/// One secondary key batch not confirmed committed synchronously.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecondaryCommitFailure {
    /// Exact encoded secondary keys whose commit was not confirmed.
    pub keys: Vec<Vec<u8>>,
    /// Exact region epoch used by the last attempt.
    pub region: Option<RegionVerId>,
    /// Physical leader address used by the last attempt.
    pub address: Option<String>,
    /// Publication identity when admission reached BatchCommands.
    pub publication: Option<TransactionBatchPublication>,
    /// Typed failure identity.
    pub cause: TransactionCause,
}

/// One rollback batch whose exact keys were not confirmed cleaned.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupBatchFailure {
    /// Exact possibly-prewritten keys still outstanding.
    pub keys: Vec<Vec<u8>>,
    /// Exact region epoch used by the last cleanup attempt.
    pub region: Option<RegionVerId>,
    /// Physical leader address used by the last cleanup attempt.
    pub address: Option<String>,
    /// Publication identity when admission reached BatchCommands.
    pub publication: Option<TransactionBatchPublication>,
    /// Typed cleanup failure.
    pub cause: TransactionCause,
}

/// One real snapshot Get publication retained by a zero-write completion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotReadReceipt {
    /// Encoded key read at the transaction start timestamp.
    pub key: Vec<u8>,
    /// Exact region epoch used for the successful Get.
    pub region: RegionVerId,
    /// Physical BatchCommands publication.
    pub publication: TransactionBatchPublication,
}

/// Transaction command represented by one causal physical attempt receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionAttemptPhase {
    /// Optimistic lock creation.
    Prewrite,
    /// Commit of the current batch containing the deterministic primary key.
    PrimaryCommit,
    /// Synchronous commit of a non-primary batch.
    SecondaryCommit,
    /// Synchronous cleanup of possibly-prewritten keys.
    BatchRollback,
}

/// Definitive or ambiguous result attached to one physical attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransactionAttemptResult {
    /// The command was accepted without a region or key error.
    Confirmed,
    /// A decoded, definitive response requires a newly-routed retry.
    Retry(TransactionCause),
    /// A decoded response or pre-publication failure ended this command.
    DefinitiveFailure(TransactionCause),
    /// Publication occurred but no definitive TiKV result was decoded.
    Ambiguous(TransactionCause),
}

/// Causal route and result for one concrete BatchCommands attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionAttemptReceipt {
    /// Logical transaction command.
    pub phase: TransactionAttemptPhase,
    /// Exact encoded keys sent on this attempt.
    pub keys: Vec<Vec<u8>>,
    /// Exact region epoch used to build the request context.
    pub region: RegionVerId,
    /// Physical leader address selected for this attempt.
    pub address: String,
    /// Publication identity, absent only when admission failed before publication.
    pub publication: Option<TransactionBatchPublication>,
    /// Definitive retry/failure/success or ambiguous completion classification.
    pub result: TransactionAttemptResult,
}

/// Truthful terminal classification at the normal-2PC publication boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OptimisticTransactionState {
    /// No write command was published.
    ReadOnly,
    /// The primary commit response was confirmed and secondaries were attempted.
    Committed,
    /// No primary attempt may have committed and every possibly-prewritten key was rolled back.
    RolledBack,
    /// Pre-primary cleanup did not confirm rollback of every possibly prewritten key.
    CleanupFailed,
    /// A primary Commit was published but its outcome could not be determined.
    Undetermined,
}

/// Physical transaction evidence retained for a caller or live receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptimisticTransactionReceipt {
    /// Shared process authority identity that opened this transaction.
    pub authority_id: u64,
    /// Real PD start timestamp used by reads and prewrites.
    pub start_ts: u64,
    /// Real PD commit timestamp, zero until the commit phase begins.
    pub commit_ts: u64,
    /// Lexicographically smallest encoded mutation key.
    pub primary_key: Vec<u8>,
    /// Number of immutable mutations admitted by the transaction.
    pub mutation_count: usize,
    /// Source-shaped lock TTL including size and elapsed read time.
    pub lock_ttl_ms: u64,
    /// Exact region epochs observed while grouping or regrouping.
    pub region_attempts: Vec<RegionVerId>,
    /// Successful Prewrite publications.
    pub prewrite_publications: Vec<TransactionBatchPublication>,
    /// Every Prewrite publication, including definitive region retries.
    pub prewrite_attempt_publications: Vec<TransactionBatchPublication>,
    /// Every primary Commit publication, including definitive region retries.
    pub primary_publications: Vec<TransactionBatchPublication>,
    /// Confirmed secondary Commit publications.
    pub secondary_publications: Vec<TransactionBatchPublication>,
    /// Every secondary Commit publication, including region retries.
    pub secondary_attempt_publications: Vec<TransactionBatchPublication>,
    /// Confirmed BatchRollback publications.
    pub rollback_publications: Vec<TransactionBatchPublication>,
    /// Every BatchRollback publication, including region retries.
    pub rollback_attempt_publications: Vec<TransactionBatchPublication>,
    /// Causal phase/key/region/address/publication/result history for every write attempt.
    pub attempt_history: Vec<TransactionAttemptReceipt>,
}

impl OptimisticTransactionReceipt {
    pub(super) fn new(
        authority_id: u64,
        start_ts: u64,
        primary_key: Vec<u8>,
        mutation_count: usize,
    ) -> Self {
        Self {
            authority_id,
            start_ts,
            commit_ts: 0,
            primary_key,
            mutation_count,
            lock_ttl_ms: 0,
            region_attempts: Vec::new(),
            prewrite_publications: Vec::new(),
            prewrite_attempt_publications: Vec::new(),
            primary_publications: Vec::new(),
            secondary_publications: Vec::new(),
            secondary_attempt_publications: Vec::new(),
            rollback_publications: Vec::new(),
            rollback_attempt_publications: Vec::new(),
            attempt_history: Vec::new(),
        }
    }
}

/// A confirmed primary commit with synchronous secondary attempts complete.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedTransaction {
    /// Complete physical receipt.
    pub receipt: OptimisticTransactionReceipt,
    /// Failures from secondary commits. The primary remains committed.
    pub secondary_failures: Vec<SecondaryCommitFailure>,
}

/// A definitive non-commit whose exact keys were all confirmed rolled back.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RolledBackTransaction {
    /// Complete physical receipt.
    pub receipt: OptimisticTransactionReceipt,
    /// Original failure that triggered rollback.
    pub cause: TransactionCause,
}

/// A definitive non-commit followed by incomplete cleanup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupFailedTransaction {
    /// Partial physical receipt.
    pub receipt: OptimisticTransactionReceipt,
    /// Original failure that triggered rollback.
    pub cause: TransactionCause,
    /// Exact cleanup failures observed synchronously.
    pub cleanup_failures: Vec<CleanupBatchFailure>,
}

/// A published primary whose response was lost or otherwise ambiguous.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UndeterminedTransaction {
    /// Partial physical receipt including the primary publication identity.
    pub receipt: OptimisticTransactionReceipt,
    /// Ambiguous transport/completion failure.
    pub cause: TransactionCause,
}

/// Terminal outcome returned by a consuming write commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OptimisticCommitOutcome {
    /// Primary confirmed committed; secondary failures cannot change that fact.
    Committed(CommittedTransaction),
    /// No primary attempt may have committed and cleanup confirmed rollback.
    RolledBack(RolledBackTransaction),
    /// Every published primary attempt was definitively rejected, but cleanup was incomplete.
    CleanupFailed(CleanupFailedTransaction),
    /// Primary was published and the result is not safe to retry.
    Undetermined(UndeterminedTransaction),
}

impl OptimisticCommitOutcome {
    /// Truthful terminal classification.
    #[must_use]
    pub const fn state(&self) -> OptimisticTransactionState {
        match self {
            Self::Committed(_) => OptimisticTransactionState::Committed,
            Self::RolledBack(_) => OptimisticTransactionState::RolledBack,
            Self::CleanupFailed(_) => OptimisticTransactionState::CleanupFailed,
            Self::Undetermined(_) => OptimisticTransactionState::Undetermined,
        }
    }

    /// Physical transaction receipt for this exact outcome.
    #[must_use]
    pub const fn receipt(&self) -> &OptimisticTransactionReceipt {
        match self {
            Self::Committed(outcome) => &outcome.receipt,
            Self::RolledBack(outcome) => &outcome.receipt,
            Self::CleanupFailed(outcome) => &outcome.receipt,
            Self::Undetermined(outcome) => &outcome.receipt,
        }
    }
}

/// Explicit zero-write completion for missing or unchanged point UPDATE.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadOnlyTransaction {
    /// Shared process authority identity that served this transaction.
    pub authority_id: u64,
    /// Real PD snapshot timestamp used by this transaction.
    pub start_ts: u64,
    /// No Prewrite, Commit, or BatchRollback was published.
    pub state: OptimisticTransactionState,
    /// Every real snapshot Get performed before the zero-write finish.
    pub snapshot_reads: Vec<SnapshotReadReceipt>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CoordinatorState {
    New,
    Reading,
    Prewriting,
    Prewritten,
    PrimaryCommitting,
    PrimaryCommitted,
    SecondariesCommitting,
    Committed,
    RollingBack,
    RolledBack,
    CleanupFailed,
    Undetermined,
    ReadOnly,
}

impl CoordinatorState {
    pub(super) fn transition(&mut self, next: Self) -> Result<(), TransactionCause> {
        let valid = matches!(
            (*self, next),
            (Self::New, Self::Reading)
                | (Self::Reading, Self::Reading)
                | (Self::New | Self::Reading, Self::Prewriting)
                | (Self::Prewriting, Self::Prewritten)
                | (Self::Prewritten, Self::PrimaryCommitting)
                | (
                    Self::PrimaryCommitting,
                    Self::PrimaryCommitted | Self::RollingBack | Self::Undetermined
                )
                | (Self::Prewriting | Self::Prewritten, Self::RollingBack)
                | (Self::RollingBack, Self::RolledBack | Self::CleanupFailed)
                | (
                    Self::PrimaryCommitted,
                    Self::SecondariesCommitting | Self::Committed
                )
                | (Self::SecondariesCommitting, Self::Committed)
                | (Self::New | Self::Reading, Self::ReadOnly)
        );
        if !valid {
            return Err(TransactionCause::InvalidResponse {
                detail: format!("invalid optimistic 2PC state transition {self:?} -> {next:?}"),
            });
        }
        *self = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_states_do_not_conflate_cleanup_or_publication_ambiguity() {
        let receipt = OptimisticTransactionReceipt::new(9, 7, b"p".to_vec(), 2);
        let rolled_back = OptimisticCommitOutcome::RolledBack(RolledBackTransaction {
            receipt: receipt.clone(),
            cause: TransactionCause::WriteConflict {
                detail: "conflict".to_owned(),
            },
        });
        let cleanup_failed = OptimisticCommitOutcome::CleanupFailed(CleanupFailedTransaction {
            receipt: receipt.clone(),
            cause: TransactionCause::WriteConflict {
                detail: "conflict".to_owned(),
            },
            cleanup_failures: vec![CleanupBatchFailure {
                keys: vec![b"p".to_vec()],
                region: None,
                address: Some("127.0.0.1:1".to_owned()),
                publication: None,
                cause: TransactionCause::Transport {
                    detail: "rollback timeout".to_owned(),
                },
            }],
        });
        let undetermined = OptimisticCommitOutcome::Undetermined(UndeterminedTransaction {
            receipt,
            cause: TransactionCause::Transport {
                detail: "primary response lost".to_owned(),
            },
        });
        assert_eq!(rolled_back.state(), OptimisticTransactionState::RolledBack);
        assert_eq!(
            cleanup_failed.state(),
            OptimisticTransactionState::CleanupFailed
        );
        assert_eq!(
            undetermined.state(),
            OptimisticTransactionState::Undetermined
        );
    }

    #[test]
    fn coordinator_state_machine_rejects_skipped_publication_boundaries() {
        let mut state = CoordinatorState::New;
        state.transition(CoordinatorState::Reading).unwrap();
        state.transition(CoordinatorState::Prewriting).unwrap();
        assert!(state
            .transition(CoordinatorState::PrimaryCommitted)
            .is_err());
        state.transition(CoordinatorState::Prewritten).unwrap();
        state
            .transition(CoordinatorState::PrimaryCommitting)
            .unwrap();
        state
            .transition(CoordinatorState::PrimaryCommitted)
            .unwrap();
        state
            .transition(CoordinatorState::SecondariesCommitting)
            .unwrap();
        state.transition(CoordinatorState::Committed).unwrap();
    }
}
