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

//! Gap tests for Go `pkg/executor/show_placement_test.go` (items 593-599)
//! and `pkg/executor/show_placement_labels_test.go:26` (item 592). All of
//! them render placement rules through the SHOW executor
//! (`fetchShowPlacement*`, pkg/executor/show_placement.go:113/:140) against
//! PD-backed placement policies, with privilege checks and a mockable PD
//! HTTP client — none of which this tier models.

/// Go `pkg/executor/show_placement_labels_test.go:26::TestShowPlacementLabelsBuilder`:
/// `showPlacementLabelsResultBuilder` accumulates store label lists
/// (`AppendStoreLabels` over BinaryJSON arrays) and `BuildRows` emits one
/// sorted (key, sorted-distinct-values JSON array) pair per label key,
/// skipping stores with nil labels (pkg/executor/show_placement.go:47/:82).
#[test]
#[ignore = "go-parity-gap: showPlacementLabelsResultBuilder (pkg/executor/show_placement.go:47/:82) has no Rust counterpart"]
fn placement_labels_builder_aggregates_and_sorts_store_labels() {}

/// Go `pkg/executor/show_placement_test.go:35::TestShowPlacement`: `show
/// placement` renders every policy, DB, table, and partition carrying
/// placement settings as `TARGET CONSTRAINTS... SCHEDULE STATE` rows
/// (sorted), with `NULL` schedule state for policies and LIKE/WHERE
/// filtering (`like 'POLICY%'`, `where Target='POLICY pb1'`). Needs
/// placement-policy DDL objects and the SHOW executor.
#[test]
#[ignore = "go-parity-gap: SHOW PLACEMENT rendering (pkg/executor/show_placement.go:113) over placement policies is unported"]
fn show_placement_lists_policies_databases_tables_and_partitions() {}

/// Go
/// `pkg/executor/show_placement_test.go:119::TestShowPlacementPrivilege`:
/// before any grant, an unprivileged session sees only policy rows; after
/// `grant select on test.t1/test.t3/db2.t1` the matching DATABASE/TABLE/
/// PARTITION rows appear — visibility of non-policy targets follows table
/// privileges.
#[test]
#[ignore = "go-parity-gap: placement visibility filtered by user privileges (pkg/executor/show_placement.go, privilege manager) is unported"]
fn show_placement_hides_tables_without_privileges() {}

/// Go
/// `pkg/executor/show_placement_test.go:184::TestShowPlacementForDB`: `show
/// placement for database <db>` errors with `[schema:1049]` for unknown
/// databases, answers nothing for a DB without placement, and renders the
/// SCHEDULED state when the rule is replicated.
#[test]
#[ignore = "go-parity-gap: show placement for database (pkg/executor/show_placement.go:140 fetchShowPlacementForDB) is unported"]
fn show_placement_for_db_reports_schedule_state() {}

/// Go
/// `pkg/executor/show_placement_test.go:210::TestShowPlacementForTableAndPartition`:
/// `show placement for table t` shows only the TABLE-level rule (partitions
/// with their own policy are not listed), `table t partition p` resolves a
/// partition's inherited or custom rule, unknown tables error `[schema:1146]`
/// and unknown partitions `[table:1735]`, and `db2.t1` qualification works.
#[test]
#[ignore = "go-parity-gap: per-table/partition placement resolution with schema/table errors (pkg/executor/show_placement.go:140) is unported"]
fn show_placement_for_table_and_partition_resolves_rules() {}

/// Go
/// `pkg/executor/show_placement_test.go:289::TestShowPlacementForDBPrivilege`:
/// `show placement for database db2` fails with `ErrDBaccessDenied` until
/// ANY of the eight db/table privileges is granted, and the denial returns
/// after REVOKE — while plain `show placement` keeps showing policy rows.
#[test]
#[ignore = "go-parity-gap: DB-access privilege checks on placement queries (ErrDBaccessDenied path, privilege manager) are unported"]
fn show_placement_for_db_requires_any_database_privilege() {}

/// Go
/// `pkg/executor/show_placement_test.go:370::TestShowPlacementForTableAndPartitionPrivilege`:
/// `show placement for table …` fails with `ErrTableaccessDenied("SHOW", …)`
/// before grants, succeeds per-granted-table after each of create/alter/
/// drop/select/insert/delete, and the bare `show placement` list grows by
/// the granted table's rows only.
#[test]
#[ignore = "go-parity-gap: table-access privilege checks on placement queries (ErrTableaccessDenied path, privilege manager) are unported"]
fn show_placement_for_table_requires_any_table_privilege() {}

/// Go
/// `pkg/executor/show_placement_test.go:497::TestShowPlacementHandleRegionStatus`:
/// placement schedule state is derived from PD's replicated-state per key
/// range (`GetRegionsReplicatedStateByKeyRange` via
/// `infosync.SetPDHttpCliForTest`): PENDING/INPROGRESS/REPLICATED map to
/// PENDING/INPROGRESS/SCHEDULED, and a TABLE rolls up to PENDING when any
/// partition is pending while per-partition queries keep their own state.
#[test]
#[ignore = "go-parity-gap: PD replicated-state rollup (infosync PD HTTP client seam, pkg/executor/show_placement.go region status helpers) is unported"]
fn show_placement_derives_region_status_from_pd() {}
