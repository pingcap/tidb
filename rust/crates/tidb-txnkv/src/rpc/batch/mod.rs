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

//! BatchCommands scheduling authority.
//!
//! This first rotation owns pure priority, grouping, policy, and observation
//! state. The exact streaming wire and connection lifecycle are added only
//! after this authority and asynchronous completion are integrated.

mod batch_rollback;
mod commit;
mod coprocessor;
mod get;
mod inflight;
mod observability;
mod prewrite;
mod priority_queue;
mod scheduler;
mod transport;
mod wire;

pub(in crate::rpc) use transport::{BatchStreamEvent, BatchTransportState};

pub use coprocessor::BatchCoprocessorPending;

pub(in crate::rpc) use batch_rollback::entry as batch_rollback_entry;
pub(in crate::rpc) use commit::entry as commit_entry;
pub(in crate::rpc) use get::entry as get_entry;
pub(in crate::rpc) use prewrite::entry as prewrite_entry;

pub use inflight::{
    BatchInflightError, BatchInflightTable, BatchPublishError, BatchRetirementReport, BatchRoute,
    PendingBatchCommand,
};
pub use observability::{
    normalize_observed_sent_ns, terminal_outcome, BatchRequestObservation, BatchRequestOutcome,
    BatchRequestProgress, BatchRequestStage, BatchRequestState, BatchStreamState,
    BatchTerminalError,
};
pub use priority_queue::{PriorityItem, PriorityQueue};
pub use scheduler::{
    BatchEntry, BatchEntryCompletion, BatchGroup, BatchGroups, BatchPolicyOptions, BatchScheduler,
    BatchTrigger, ConsumedBatchGroups, ScheduledEntry, BATCH_POLICY_BASIC, BATCH_POLICY_CUSTOM,
    BATCH_POLICY_POSITIVE, BATCH_POLICY_STANDARD, DEFAULT_BATCH_POLICY, HIGH_TASK_PRIORITY,
};
pub use transport::{BatchCommandCompletion, BatchCommandEntry, BatchPublicationReceipt};
pub use wire::{
    BatchCommandTag, BatchEnvelopeKind, BatchWireError, BatchWireRequest, BatchWireResponse,
    OpaqueBatchCommand,
};
