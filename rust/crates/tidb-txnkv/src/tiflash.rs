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

//! Complete transcreation of `pkg/util/tiflash`.
//!
//! The Go package contains one production file and no tests, benchmarks, fuzz
//! targets, generated files, or support assets. Its Bazel dependency on
//! `pkg/sessionctx/vardef` supplies the three session spellings consumed here.
//!
//! Go defines `ReplicaRead` as an alias of `int`, so values outside the three
//! named constants remain constructible and fall back to `all_replicas`.
//! Keeping the raw `isize` in a newtype preserves that open integer domain;
//! a closed Rust enum would silently narrow the source contract.

/// Maximum remote regions per TiFlash node under closest-replicas policy.
pub const MAX_REMOTE_READ_COUNT_PER_NODE_FOR_CLOSEST_REPLICAS: usize = 3;

/// Policy used to select TiFlash nodes for analytic reads.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[repr(transparent)]
pub struct ReplicaRead(pub isize);

#[allow(non_upper_case_globals)]
impl ReplicaRead {
    /// Use every available TiFlash node.
    pub const AllReplicas: Self = Self(0);
    /// Prefer the local zone, falling back to other zones when required.
    pub const ClosestAdaptive: Self = Self(1);
    /// Use the local zone and reject excessive remote-region access.
    pub const ClosestReplicas: Self = Self(2);

    /// Source `IsAllReplicas` predicate.
    #[must_use]
    pub fn is_all_replicas(self) -> bool {
        self.0 == Self::AllReplicas.0
    }

    /// Source `IsClosestReplicas` predicate.
    #[must_use]
    pub fn is_closest_replicas(self) -> bool {
        self.0 == Self::ClosestReplicas.0
    }
}

/// Returns the session spelling for a TiFlash replica-read policy.
#[must_use]
pub fn get_tiflash_replica_read(policy: ReplicaRead) -> &'static str {
    match policy.0 {
        1 => tidb_vardef::tidb_vars::CLOSEST_ADAPTIVE_STR,
        2 => tidb_vardef::tidb_vars::CLOSEST_REPLICAS_STR,
        _ => tidb_vardef::tidb_vars::ALL_REPLICA_STR,
    }
}

/// Parses a session spelling, falling back to all replicas.
#[must_use]
pub fn get_tiflash_replica_read_by_str(value: &str) -> ReplicaRead {
    match value {
        tidb_vardef::tidb_vars::ALL_REPLICA_STR => ReplicaRead::AllReplicas,
        tidb_vardef::tidb_vars::CLOSEST_ADAPTIVE_STR => ReplicaRead::ClosestAdaptive,
        tidb_vardef::tidb_vars::CLOSEST_REPLICAS_STR => ReplicaRead::ClosestReplicas,
        _ => ReplicaRead::AllReplicas,
    }
}
