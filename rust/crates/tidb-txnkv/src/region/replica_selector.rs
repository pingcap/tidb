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

/// client-go replica-read policy discriminants.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ReplicaReadMode {
    /// Ordinary leader read.
    #[default]
    Leader,
    /// Follower read.
    Follower,
    /// Mixed leader/follower read.
    Mixed,
    /// Learner read.
    Learner,
    /// Prefer leader with fallback.
    PreferLeader,
}

/// Read and forwarding policy presented to replica selection.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReadPolicy {
    /// Replica selection mode.
    pub mode: ReplicaReadMode,
    /// Whether stale-read semantics are active.
    pub stale_read: bool,
    /// Whether request forwarding/proxy selection is enabled.
    pub forwarding: bool,
    /// Deterministic tie-break seed for equally ranked replicas.
    ///
    /// The owner advances this seed at its query/snapshot boundary. The
    /// request-scoped selector keeps it stable while the not-attempted score
    /// rotates retries.
    pub selection_seed: u32,
}
