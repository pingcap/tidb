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
    ClientTransaction as RealOptimisticTransaction, SnapshotGetResult, SnapshotScanRegion,
};
use super::mutation::MutationSetError;
use crate::gc_state::VisibilityError;
use crate::rpc::TonicCoprocessorClient;
use crate::PdRegionLoader;
pub use opener::{
    PdLockTimestampSource, RealOptimisticTransactionOpener, StorePdCapability, StoreWriteClient,
    StoreWriteLoader,
};
use std::fmt;

/// Concrete process/session authority errors rejected before a transaction opens.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OptimisticCoordinatorError {
    /// Typed TiDB error returned while staging a native buffer write.
    Storage(crate::KvError),
    /// PD and RegionCache must describe the same real cluster.
    ClusterMismatch {
        /// Cluster ID reported by the sole PD worker.
        pd: u64,
        /// Cluster ID reported by the shared RegionCache loader.
        region_cache: u64,
    },
    /// Real cluster identity cannot be zero.
    ZeroClusterId,
    /// Real PD timestamp allocation failed.
    Timestamp(String),
    /// The caller supplied an invalid mutation set.
    Mutations(MutationSetError),
    /// A real snapshot Get could not produce a determinate result.
    SnapshotGet(String),
    /// A snapshot exhausted a client-go retry category; retain its SQL identity.
    SnapshotBackoff {
        /// Longest effective backoff category.
        kind: crate::region::RegionBackoffKind,
        /// Source backoff diagnostic for unregistered categories.
        detail: String,
    },
    /// The data this transaction read may already have been garbage-collected.
    ///
    /// This is deliberately its own variant rather than a `SnapshotGet` string:
    /// it is terminal, never retryable at the same `start_ts`, and it maps to
    /// its own SQL error tier. Folding it into the generic bucket is what makes
    /// a GC-overtaken read look like a transport hiccup worth retrying.
    Visibility(VisibilityError),
    /// The txn safe point could not be loaded when the authority was built.
    GcState(String),
}

impl fmt::Display for OptimisticCoordinatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => error.fmt(formatter),
            Self::ClusterMismatch { pd, region_cache } => write!(
                formatter,
                "PD cluster {pd} does not match RegionCache cluster {region_cache}"
            ),
            Self::ZeroClusterId => {
                formatter.write_str("real optimistic 2PC requires a nonzero cluster ID")
            }
            Self::Timestamp(error) => write!(formatter, "PD timestamp allocation failed: {error}"),
            Self::Mutations(error) => error.fmt(formatter),
            Self::SnapshotGet(error) => write!(formatter, "snapshot Get failed: {error}"),
            Self::SnapshotBackoff { kind, detail } => {
                crate::to_tidb_driver_error(&crate::StorageDriverError::from_backoff(*kind, detail))
                    .fmt(formatter)
            }
            Self::Visibility(error) => error.fmt(formatter),
            Self::GcState(error) => write!(formatter, "txn safe point unavailable: {error}"),
        }
    }
}

impl std::error::Error for OptimisticCoordinatorError {}

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

/// The one production pessimistic transaction.
pub type ProductionPessimisticTransaction = super::RealPessimisticTransaction<
    TonicCoprocessorClient,
    PdRegionLoader,
    crate::pd_capability::CapabilityTimestampSource<tidb_pd_client::PdClient>,
>;

/// The one production transaction: real TiKV transport, real PD-backed region
/// topology, and real PD timestamps.
pub type ProductionOptimisticTransaction = RealOptimisticTransaction<
    TonicCoprocessorClient,
    PdRegionLoader,
    crate::pd_capability::CapabilityTimestampSource<tidb_pd_client::PdClient>,
>;
