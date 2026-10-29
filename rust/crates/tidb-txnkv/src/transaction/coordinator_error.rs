// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Why a transaction could not be opened or staged: the errors the
//! coordinator and the store-error conversion share.

use std::fmt;

use super::mutation::MutationSetError;
use crate::gc_state::VisibilityError;

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
        kind: crate::retry::RegionBackoffKind,
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
            // client-go's backoffer returns its config's sentinel error when the
            // budget runs out; TiDB's `ToTiDBErr` maps it at the SQL boundary.
            Self::SnapshotBackoff { kind, detail } => {
                crate::StorageDriverError::from_backoff(*kind, detail).fmt(formatter)
            }
            Self::Visibility(error) => error.fmt(formatter),
            Self::GcState(error) => write!(formatter, "txn safe point unavailable: {error}"),
        }
    }
}

impl std::error::Error for OptimisticCoordinatorError {}
