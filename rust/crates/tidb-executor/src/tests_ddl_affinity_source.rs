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

#![allow(missing_docs)]

//! GO PORT of `pkg/ddl/affinity_test.go` (items 1-5 of the pkg/ddl.part1
//! slice, read from `origin/master`).
//!
//! The Go file's unit half (items 1-3) drives
//! `pkg/ddl/affinity.go:59 buildAffinityGroupDefinitions` through its
//! `BuildAffinityGroupDefinitionsForTest` export, with the group-ID helpers
//! `GetTableAffinityGroupID` (affinity.go:33) and
//! `GetPartitionAffinityGroupID` (affinity.go:39) naming the produced groups
//! and a `tikv.Codec` mock (`mockCodec`, affinity_test.go:36) expanding the
//! key ranges. The interaction half (items 4-5) runs full DDL jobs against a
//! mock-store cluster plus the PD HTTP affinity-group sync.
//!
//! None of that surface exists on the Rust side yet: no crate in this
//! workspace transcreates `pkg/ddl/affinity.go` (the only affinity shapes
//! ported are the `TableAffinityInfo` metadata struct in `tidb-model`), and
//! the DDL job/PD machinery the interaction tests need is out of this tier.
//! Every test below is therefore a documentary `#[ignore]` naming its gap;
//! none approximates the Go expectations.

/// GO PORT of `pkg/ddl/affinity_test.go:46
/// TestAffinityBuildGroupDefinitionsTable`.
///
/// Go pins that a table-level affinity (`TableAffinityLevelTable`) on
/// `TableInfo{ID: 123}` yields exactly one group keyed
/// `_tidb_t_123` whose single key range is
/// `mockCodec.EncodeRegionRange(EncodeTablePrefix(123),
/// EncodeTablePrefix(124))` — with the mock's `k:` prefix visible.
#[test]
#[ignore = "go-parity-gap: pkg/ddl/affinity.go:59 buildAffinityGroupDefinitions and its group-ID helpers (affinity.go:33/:39) are not transcreated in this workspace; no PD AffinityGroupKeyRange type exists to assert against"]
fn affinity_build_group_definitions_table() {}

/// GO PORT of `pkg/ddl/affinity_test.go:64
/// TestAffinityBuildGroupDefinitionsPartition`.
///
/// Go pins partition-level affinity (`TableAffinityLevelPartition`) on
/// `TableInfo{ID: 50}` with partition definitions `[{ID: 1}, {ID: 3}]`: two
/// groups, `_tidb_pt_50_p1` covering table-prefix(1)..table-prefix(2) and
/// `_tidb_pt_50_p3` covering table-prefix(3)..table-prefix(4), each through
/// the mock codec.
#[test]
#[ignore = "go-parity-gap: pkg/ddl/affinity.go:59 buildAffinityGroupDefinitions is not transcreated; the partition group-ID format and per-partition key ranges have no Rust counterpart"]
fn affinity_build_group_definitions_partition() {}

/// GO PORT of `pkg/ddl/affinity_test.go:91
/// TestAffinityBuildGroupDefinitionsPartitionMissing`.
///
/// Go pins the error path: partition-level affinity on a table whose
/// `Partition` is nil (no definitions reachable) makes
/// `buildAffinityGroupDefinitions` return
/// "partition affinity requires partition definitions ..." (affinity.go:74).
#[test]
#[ignore = "go-parity-gap: pkg/ddl/affinity.go:59 buildAffinityGroupDefinitions is not transcreated; its missing-partition-definitions error path has no Rust counterpart"]
fn affinity_build_group_definitions_partition_missing() {}

/// GO PORT of `pkg/ddl/affinity_test.go:173 TestAffinityPDInteraction`.
///
/// Go runs `ALTER TABLE ... ATTRIBUTES`-style affinity DDL against a mock
/// store and asserts, through `affinityGroupCheck` cases (affinity_test.go:56)
/// and PD HTTP fakes, which affinity groups exist after each statement,
/// including range counts per group and a job/state-machine driven recreate.
#[test]
#[ignore = "go-parity-gap: needs the DDL job state machine plus the PD HTTP affinity-group sync (infosync) and a mock-store cluster; none of that machinery is transcreated in this tier"]
fn affinity_pd_interaction() {}

/// GO PORT of `pkg/ddl/affinity_test.go:258 TestAffinityDropDatabase`.
///
/// Go pins that dropping a database removes the affinity groups of every
/// table in it, observed through the PD-synced group list after the DDL job
/// completes.
#[test]
#[ignore = "go-parity-gap: needs the DDL job pipeline for DROP DATABASE plus the PD affinity-group sync; neither is transcreated in this tier"]
fn affinity_drop_database() {}
