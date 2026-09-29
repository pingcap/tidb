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

//! Concrete real-PD/TiKV normal optimistic two-phase commit.

mod command_client;
mod coordinator;
mod mutation;
mod mutation_plan;
mod pessimistic;
mod schema_lease;
mod state;

pub use command_client::{
    DetachedCommitCompletion, PublishedCommand, TransactionBatchGetFuture,
    TransactionBatchGetRequest, TransactionCommandClient, TransactionCommitRequest,
    TransactionPrewriteRequest,
};
pub use coordinator::{
    CommitProtocol, OptimisticCoordinatorError, PdLockTimestampSource,
    ProductionOptimisticTransaction, ProductionPessimisticTransaction, RealOptimisticTransaction,
    RealOptimisticTransactionOpener, SnapshotGetResult, SnapshotScanRegion, StorePdCapability,
    StoreWriteClient, StoreWriteLoader,
};
pub use mutation::{
    MutationSetError, OptimisticMutation, OptimisticMutationKind, MAX_OPTIMISTIC_KEY_BYTES,
    MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES, MAX_OPTIMISTIC_VALUE_BYTES,
};
pub use mutation_plan::MutationPlan;
pub use pessimistic::{
    AcquiredLocks, DeadlockDetail, DeadlockWaitChainItem, LockWaitTime, PessimisticLockFailure,
    RealPessimisticTransaction,
};
pub use schema_lease::{SchemaLease, SchemaLeaseChecker, SchemaLeaseError};
pub use state::{
    CleanupBatchFailure, CleanupFailedTransaction, CommittedProtocol, CommittedTransaction,
    OptimisticCommitOutcome, OptimisticTransactionReceipt, OptimisticTransactionState,
    ReadOnlyTransaction, RolledBackTransaction, SecondaryCommitFailure, TransactionAttemptPhase,
    TransactionAttemptReceipt, TransactionAttemptResult, TransactionCause, UndeterminedTransaction,
};

/// Client-rust driver undergoing live-boundary validation.
pub mod client;
