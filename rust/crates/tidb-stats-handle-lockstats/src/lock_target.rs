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

//! The tables a `LOCK STATS` / `UNLOCK STATS` statement names, as Go's
//! `pkg/executor/lockstats` executors hand them to the lock handle.

/// One table target retained from a parsed lock statement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatsLockTarget {
    /// Schema name after default-schema resolution.
    pub schema: String,
    /// Table name as written.
    pub table: String,
    /// Named partitions, if this is a partition-only operation.
    pub partitions: Vec<String>,
}

/// One parsed `LOCK STATS` or `UNLOCK STATS` statement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterStatsLockStatement {
    /// `true` for LOCK and `false` for UNLOCK.
    pub lock: bool,
    /// Targets in source order.
    pub targets: Vec<StatsLockTarget>,
}
