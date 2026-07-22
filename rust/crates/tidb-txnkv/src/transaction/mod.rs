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
mod mutation_buffer;
mod region_batches;
mod state;

pub use command_client::{PublishedCommand, TransactionCommandClient};
pub use coordinator::{
    OptimisticCoordinatorError, PdLockTimestampSource, ProductionOptimisticTransaction,
    RealOptimisticTransaction, RealOptimisticTransactionOpener, SnapshotGetResult,
};
pub use mutation::{
    MutationSetError, OptimisticMutation, OptimisticMutationKind, MAX_OPTIMISTIC_KEY_BYTES,
    MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES, MAX_OPTIMISTIC_VALUE_BYTES,
};
pub use mutation_buffer::{MutationBufferError, TransactionMutationBuffer};
pub use region_batches::RegionMutationBatch;
pub use state::{
    CleanupBatchFailure, CleanupFailedTransaction, CommittedTransaction, OptimisticCommitOutcome,
    OptimisticTransactionReceipt, OptimisticTransactionState, ReadOnlyTransaction,
    RolledBackTransaction, SecondaryCommitFailure, SnapshotReadReceipt, TransactionAttemptPhase,
    TransactionAttemptReceipt, TransactionAttemptResult, TransactionCause, UndeterminedTransaction,
};
