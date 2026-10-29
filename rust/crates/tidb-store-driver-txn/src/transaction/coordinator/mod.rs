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

//! TiDB store driver over the single client-rust transaction engine.
mod opener;
pub use super::client::{
    RealTransaction, SnapshotGetResult,
};
pub use opener::{RealOptimisticTransactionOpener, StoreClient};

/// Which faster-than-2PC commit protocols a transaction may attempt.
///
/// These are permissions, not decisions: TiKV can refuse either protocol on any
/// prewrite response, and the coordinator then finishes the transaction as a
/// normal two-phase commit. Both flags come from the session
/// (`@@tidb_enable_async_commit`, `@@tidb_enable_1pc`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CommitProtocol {
    /// `@@tidb_enable_async_commit`: commit at the completed prewrite, using
    /// `max(min_commit_ts)` instead of a second PD round trip.
    pub async_commit: bool,
    /// `@@tidb_enable_1pc`: let TiKV commit a single-region transaction inside
    /// the prewrite itself, so no Commit command is ever published.
    pub one_pc: bool,
}

impl CommitProtocol {
    /// The protocol set of a transaction that must use normal two-phase commit.
    #[must_use]
    pub const fn two_phase_only() -> Self {
        Self {
            async_commit: false,
            one_pc: false,
        }
    }
}

/// The one production transaction: real TiKV transport, real PD-backed region
/// topology, and real PD timestamps.
pub type ProductionTransaction = RealTransaction<tikv_client::PdRpcClient>;
