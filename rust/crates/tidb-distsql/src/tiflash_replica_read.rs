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

//! TiFlash replica-read policy translated directly from
//! `pkg/util/tiflash/tiflash_replica_read.go`.

/// Session spelling for [`TiFlashReplicaRead::AllReplicas`].
pub const ALL_REPLICAS: &str = "all_replicas";
/// Session spelling for [`TiFlashReplicaRead::ClosestAdaptive`].
pub const CLOSEST_ADAPTIVE: &str = "closest_adaptive";
/// Session spelling for [`TiFlashReplicaRead::ClosestReplicas`].
pub const CLOSEST_REPLICAS: &str = "closest_replicas";

/// Maximum remote regions per TiFlash node under closest-replicas policy.
pub const MAX_REMOTE_READ_COUNT_PER_NODE_FOR_CLOSEST_REPLICAS: usize = 3;

/// Policy used to select TiFlash nodes for analytic reads.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(isize)]
pub enum TiFlashReplicaRead {
    /// Use every available TiFlash node.
    #[default]
    AllReplicas = 0,
    /// Prefer the local zone, falling back to other zones when required.
    ClosestAdaptive = 1,
    /// Use the local zone and reject excessive remote-region access.
    ClosestReplicas = 2,
}

impl TiFlashReplicaRead {
    /// Source `IsAllReplicas` predicate.
    #[must_use]
    pub const fn is_all_replicas(self) -> bool {
        matches!(self, Self::AllReplicas)
    }

    /// Source `IsClosestReplicas` predicate.
    #[must_use]
    pub const fn is_closest_replicas(self) -> bool {
        matches!(self, Self::ClosestReplicas)
    }

    /// Source `GetTiFlashReplicaRead` conversion.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AllReplicas => ALL_REPLICAS,
            Self::ClosestAdaptive => CLOSEST_ADAPTIVE,
            Self::ClosestReplicas => CLOSEST_REPLICAS,
        }
    }

    /// Source `GetTiFlashReplicaReadByStr` conversion, including its fallback.
    #[must_use]
    pub fn from_source_str(value: &str) -> Self {
        match value {
            ALL_REPLICAS => Self::AllReplicas,
            CLOSEST_ADAPTIVE => Self::ClosestAdaptive,
            CLOSEST_REPLICAS => Self::ClosestReplicas,
            _ => Self::AllReplicas,
        }
    }

    /// Raw integer form of `GetTiFlashReplicaRead`, retaining Go's default arm.
    #[must_use]
    pub const fn source_str_from_raw(value: isize) -> &'static str {
        match value {
            1 => CLOSEST_ADAPTIVE,
            2 => CLOSEST_REPLICAS,
            _ => ALL_REPLICAS,
        }
    }
}
