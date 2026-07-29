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

/// The protocol a transaction actually committed under.
///
/// This is an observation, not a request: a transaction permitted to use 1PC or
/// async commit still records `TwoPhase` when TiKV refused, so a receipt never
/// claims a commit point the cluster did not grant.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CommittedProtocol {
    /// Prewrite, a PD commit timestamp, then Commit.
    #[default]
    TwoPhase,
    /// Prewrite alone, committed at `max(min_commit_ts)` with no second TSO.
    AsyncCommit,
    /// TiKV committed the whole transaction inside the prewrite response.
    OnePc,
}

/// Physical transaction evidence retained for a caller or live receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptimisticTransactionReceipt {
    /// Shared process authority identity that opened this transaction.
    pub authority_id: u64,
    /// Real PD start timestamp used by reads and prewrites.
    pub start_ts: u64,
    /// Commit timestamp, zero until the commit phase begins. Allocated by PD
    /// for a two-phase commit, and derived from the prewrite responses for
    /// async commit and 1PC.
    pub commit_ts: u64,
    /// Which commit protocol the cluster actually granted.
    pub commit_protocol: CommittedProtocol,
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
    /// Starts one transaction's evidence from the facts known at Prewrite: who
    /// opened it, at which timestamp, under which primary key, and how many
    /// mutations it admitted. Every physical publication is appended as it
    /// happens, so a fresh receipt records an attempt that has published
    /// nothing yet.
    #[must_use]
    pub fn new(
        authority_id: u64,
        start_ts: u64,
        primary_key: Vec<u8>,
        mutation_count: usize,
    ) -> Self {
        Self {
            authority_id,
            start_ts,
            commit_ts: 0,
            commit_protocol: CommittedProtocol::TwoPhase,
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
    /// TiKV committed the whole single-region transaction inside its prewrite.
    OnePcCommitted,
    /// The completed async-commit prewrite is itself the commit point.
    AsyncCommitted,
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
                // 1PC and async commit reach their commit point at the
                // prewrite boundary, so neither passes through a primary
                // Commit that could still fail the transaction.
                | (Self::Prewritten, Self::OnePcCommitted | Self::AsyncCommitted)
                | (Self::OnePcCommitted, Self::Committed)
                | (
                    Self::AsyncCommitted,
                    Self::SecondariesCommitting | Self::Committed
                )
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

    /// The lifecycle a multi-statement transaction needs: read, read again, and
    /// only then prewrite -- all on one transaction, so the prewrite carries
    /// the timestamp the first read used. Re-entering `Reading` is what makes
    /// a session hold one `start_ts` across its statements, and it must not
    /// become a way around the publication boundaries.
    #[test]
    fn a_read_phase_may_be_re_entered_and_still_reach_the_write_phase() {
        let mut state = CoordinatorState::New;
        for _ in 0..4 {
            state
                .transition(CoordinatorState::Reading)
                .expect("a further statement of the same transaction reads again");
        }
        state
            .transition(CoordinatorState::Prewriting)
            .expect("COMMIT prewrites the transaction the statements read on");
        state.transition(CoordinatorState::Prewritten).unwrap();
        state
            .transition(CoordinatorState::PrimaryCommitting)
            .unwrap();
        state
            .transition(CoordinatorState::PrimaryCommitted)
            .unwrap();
        state.transition(CoordinatorState::Committed).unwrap();
    }

    /// Re-entry ends where the transaction does: once a terminal state is
    /// reached, no further read or write may reuse this `start_ts`.
    #[test]
    fn a_finished_transaction_can_never_re_enter_the_read_phase() {
        for terminal in [
            CoordinatorState::ReadOnly,
            CoordinatorState::Committed,
            CoordinatorState::RolledBack,
            CoordinatorState::CleanupFailed,
            CoordinatorState::Undetermined,
        ] {
            for next in [
                CoordinatorState::Reading,
                CoordinatorState::Prewriting,
                CoordinatorState::PrimaryCommitting,
            ] {
                let mut state = terminal;
                assert!(
                    state.transition(next).is_err(),
                    "{terminal:?} -> {next:?} must be refused"
                );
                assert_eq!(state, terminal, "a refused transition changes nothing");
            }
        }
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
