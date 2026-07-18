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

/// Fail-closed errors from the bounded single-region route.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegionRouteError {
    /// The caller supplied an empty or reversed range.
    InvalidRange,
    /// One request crosses the selected region boundary.
    MultiRegion,
    /// The injected metadata loader failed.
    Loader(String),
    /// Loaded metadata is older than the cached version for the same region.
    StaleRegionEpoch {
        /// Loaded identity.
        loaded: RegionVerId,
        /// Current cached identity.
        cached: RegionVerId,
    },
    /// The expected region version no longer matches the selected location.
    StaleRequestEpoch {
        /// Version carried by the request task.
        expected: RegionVerId,
        /// Version currently selected by the cache.
        actual: RegionVerId,
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
    /// PD cluster identity was not configured on the sender.
    MissingClusterId,
    /// Request context was already attached by an earlier dispatch.
    ContextAlreadyAttached,
}

impl std::fmt::Display for RegionRouteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for RegionRouteError {}

/// One route-validation or typed direct-unary failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegionSendError<E> {
    /// Request dispatch stopped before the direct-unary boundary.
    Route(RegionRouteError),
    /// The direct-unary client returned its original typed error.
    DirectUnary(E),
}

impl<E> From<RegionRouteError> for RegionSendError<E> {
    fn from(error: RegionRouteError) -> Self {
        Self::Route(error)
    }
}

impl<E: std::fmt::Display> std::fmt::Display for RegionSendError<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Route(error) => error.fmt(formatter),
            Self::DirectUnary(error) => error.fmt(formatter),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for RegionSendError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Route(error) => Some(error),
            Self::DirectUnary(error) => Some(error),
        }
    }
}
