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

use super::RegionVerId;

/// Concrete failure reported by an injected region metadata loader.
///
/// The identity is owned by the loader implementation. This boundary preserves
/// it together with the original message without defining PD transport or RPC
/// categories in `tidb-txnkv`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegionLoadError {
    identity: String,
    message: String,
}

impl RegionLoadError {
    /// Creates a loader failure with its implementation-defined identity.
    pub fn new(identity: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            identity: identity.into(),
            message: message.into(),
        }
    }

    /// Returns the implementation-defined error identity.
    #[must_use]
    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// Returns the concrete loader error message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for RegionLoadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.identity, self.message)
    }
}

impl std::error::Error for RegionLoadError {}

/// Fail-closed errors from the bounded single-region route.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegionRouteError {
    /// The caller supplied an empty or reversed range.
    InvalidRange,
    /// One request crosses the selected region boundary.
    MultiRegion,
    /// The injected metadata loader failed.
    Loader(RegionLoadError),
    /// Loaded metadata did not contain the key used to load it.
    LoadedRegionDoesNotContainKey {
        /// Identity returned by the loader.
        region: RegionVerId,
    },
    /// Loaded metadata has a finite end that is not after its start.
    InvalidRegionBounds {
        /// Identity returned by the loader.
        region: RegionVerId,
    },
    /// A range walk could not advance beyond the current cursor.
    NonProgressingRegion {
        /// Exact region that returned the repeated or backward boundary.
        region: RegionVerId,
    },
    /// A batch scan returned no regions without reporting an error.
    EmptyBatchLoad,
    /// A batch scan repeated its split key and could not consume input.
    NonProgressingBatchScan {
        /// Repeated exclusive end boundary.
        split_key: Vec<u8>,
    },
    /// Loaded and cached regions do not cover every requested range.
    BatchScanGap,
    /// A later fragment overlaps or leaves a gap before the walk cursor.
    DiscontinuousRegion {
        /// Exact region whose start did not equal the prior region end.
        region: RegionVerId,
    },
    /// Loaded metadata is older than the cached version for the same region.
    StaleRegionEpoch {
        /// Loaded identity.
        loaded: RegionVerId,
        /// Current cached identity.
        cached: RegionVerId,
    },
    /// TiKV returned the same region identity more than once in one replacement.
    DuplicateReplacementRegion {
        /// Duplicated region identity.
        region: RegionVerId,
    },
    /// The selected region has no declared leader.
    MissingLeader,
    /// The leader peer references no store.
    MissingStore(u64),
    /// The selected store has no address.
    MissingAddress(u64),
    /// The store epoch changed after the region snapshot was built.
    StaleStoreEpoch {
        /// Store identifier.
        store_id: u64,
        /// Epoch captured in the region snapshot.
        expected: u64,
        /// Current store epoch.
        actual: u64,
    },
    /// Only an ordinary leader read is admitted.
    UnsupportedReadPolicy,
    /// A selected RPC attempt has not recorded its completion yet.
    AttemptStillPending {
        /// Exact region bound to the selector.
        region: RegionVerId,
        /// Peer owning the outstanding attempt.
        peer_id: u64,
    },
    /// PD cluster identity was not configured on the sender.
    MissingClusterId,
}

impl std::fmt::Display for RegionRouteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Loader(error) => write!(formatter, "region loader failed: {error}"),
            _ => write!(formatter, "{self:?}"),
        }
    }
}

impl std::error::Error for RegionRouteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Loader(error) => Some(error),
            _ => None,
        }
    }
}
