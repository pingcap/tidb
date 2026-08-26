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

//! Remaining slices of Go `pkg/meta/meta_test.go` that no other Rust test in
//! this crate pins. Every other `func TestXxx` in that file is already ported;
//! see `rust/testport/receipts/b039.md` for the full mapping table.
//!
//! The four functions below all depend on infrastructure that lives outside
//! the `tidb-meta` crate (Go's `GetStarterBootstrapVersion` mutator API and
//! the session-bootstrap + InfoSchemaV2 pipeline), so they are recorded as
//! ignored go-parity gaps rather than approximated with hand-rolled stand-ins.

/// Go `TestMeta` (`pkg/meta/meta_test.go:241`), starter-bootstrap slice:
/// after finishing bootstrap, `GetStarterBootstrapVersion` returns 0,
/// `FinishStarterBootstrap(1)` then reads back 1, and
/// `FinishStarterBootstrap(10)` reads back 10. Go stores the version as a raw
/// decimal string under `mStarterBootstrapKey = []byte("StarterBootstrapKey")`
/// (`pkg/meta/meta.go:2074-2076`). The Rust [`tidb_meta::transaction::Mutator`]
/// has no starter-bootstrap accessor yet, so the behavior cannot be pinned
/// without inventing an API.
#[test]
#[ignore = "go-parity-gap: Mutator has no GetStarterBootstrapVersion/FinishStarterBootstrap equivalent"]
fn meta_starter_bootstrap_round_trip() {
    // Go source: pkg/meta/meta_test.go TestMeta, lines asserting
    // GetStarterBootstrapVersion/FinishStarterBootstrap.
}

/// Go `TestInfoSchemaV2SpecialAttributeCorrectnessAfterBootstrap`
/// (`pkg/meta/meta_test.go:1109`): create a database plus a table carrying
/// partition / TiFlash-replica / table-lock / placement-policy / TTL /
/// affinity attributes through the meta mutator, run full session bootstrap,
/// and assert `InfoSchema.ListTablesWithSpecialAttribute` returns exactly that
/// table for each of the six attribute selectors. Requires session
/// bootstrap (`session.BootstrapSession`) and the InfoSchemaV2 cache, neither
/// of which exists in this workspace yet.
#[test]
#[ignore = "go-parity-gap: needs session.BootstrapSession + InfoSchemaV2 ListTablesWithSpecialAttribute pipeline"]
fn infoschema_v2_special_attribute_correctness_after_bootstrap() {
    // Golden assertions live verbatim in Go pkg/meta/meta_test.go:1109.
}

/// Go `TestInfoSchemaV2DataFieldsCorrectnessAfterBootstrap`
/// (`pkg/meta/meta_test.go:1201`): after persisting a database and a
/// partitioned table and bootstrapping a session, exercise every InfoSchemaV2
/// lookup path -- `TableByID` (byID traversal), `TableByName` (byName),
/// cached `TableByID`, `SchemaByName`, `SchemaByID`, and
/// `FindTableByPartitionID` (pid2tid) -- and require identical metadata.
/// Requires the same session-bootstrap + InfoSchemaV2 infrastructure.
#[test]
#[ignore = "go-parity-gap: needs session.BootstrapSession + InfoSchemaV2 TableByID/TableByName/FindTableByPartitionID"]
fn infoschema_v2_data_fields_correctness_after_bootstrap() {
    // Golden assertions live verbatim in Go pkg/meta/meta_test.go:1201.
}

/// Go `TestInfoSchemaMiscFieldsCorrectnessAfterBootstrap`
/// (`pkg/meta/meta_test.go:1284`): persist a policy, two resource groups, a
/// database, an FK-bearing table with a placement-policy reference and a
/// local-temporary table, bootstrap a session, and verify `AllPlacementPolicies`,
/// `AllResourceGroups` (sorted by name), `GetTableReferredForeignKeys`, and
/// `HasTemporaryTable`. Requires the same session-bootstrap + InfoSchema
/// surface.
#[test]
#[ignore = "go-parity-gap: needs session.BootstrapSession + InfoSchema policy/group/FK/temp-table catalogs"]
fn infoschema_misc_fields_correctness_after_bootstrap() {
    // Golden assertions live verbatim in Go pkg/meta/meta_test.go:1284.
}
