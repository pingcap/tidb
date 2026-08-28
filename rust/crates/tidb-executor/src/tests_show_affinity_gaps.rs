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

//! Gap tests for Go `pkg/executor/show_affinity_test.go` (items 587-589).
//! All three drive `SHOW AFFINITY`, whose executor
//! (`ShowExec.fetchShowAffinity`, pkg/executor/show_affinity.go:31) joins
//! the catalog's affinity annotations with PD's affinity-group states
//! fetched through the PD HTTP client (`affinity.SetPDClientForTest`).
//! This tier has no SHOW executor, no affinity DDL options, and no PD
//! client seam.

/// Go `pkg/executor/show_affinity_test.go:43::TestShowAffinity`: `show
/// affinity` lists one row per affinity-bearing table (table-level rows
/// with NULL partition name) and per partition for partition-level
/// affinity, never lists non-affinity tables, and supports LIKE and WHERE
/// filtering (`Table_name =/!=/like/in`, `Partition_name != ''`, `Db_name =
/// …`) exactly like a virtual table. Needs affinity metadata in the catalog
/// and the SHOW executor.
#[test]
#[ignore = "go-parity-gap: SHOW AFFINITY (pkg/executor/show_affinity.go:31) with LIKE/WHERE filtering needs the SHOW executor and affinity catalog metadata, both unported"]
fn show_affinity_lists_and_filters_affinity_tables_and_partitions() {}

/// Go
/// `pkg/executor/show_affinity_test.go:190::TestShowAffinityColumns`: with
/// a mocked PD client returning the table's affinity group
/// (`ddl.GetTableAffinityGroupID(tbl.ID)`), the row renders exactly 8
/// columns: db, table, NULL partition, leader store `1`, voter list
/// `1,2,3`, status `Stable`, region count `10`, affinity region count `9`.
/// Needs the PD HTTP client seam and the row builder.
#[test]
#[ignore = "go-parity-gap: the affinity-group state join (affinity.SetPDClientForTest + GetAllAffinityGroups, pkg/domain/affinity) has no Rust counterpart"]
fn show_affinity_renders_eight_columns_from_pd_state() {}

/// Go
/// `pkg/executor/show_affinity_test.go:252::TestShowAffinityNullStatus`:
/// when PD reports no state for the group, all five PD-derived columns
/// (leader, voters, status, region count, affinity region count) render as
/// native NULL rather than strings.
#[test]
#[ignore = "go-parity-gap: the PD-missing-group NULL rendering in fetchShowAffinity (pkg/executor/show_affinity.go:31) is unported"]
fn show_affinity_renders_nulls_when_pd_has_no_group() {}
