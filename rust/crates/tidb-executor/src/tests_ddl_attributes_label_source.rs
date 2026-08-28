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

//! GO PORT of `pkg/ddl/attributes_sql_test.go` (items 6-13 of the
//! pkg/ddl.part1 slice, read from `origin/master`).
//!
//! The Go tests all run DDL against a mock-store cluster
//! (`testkit.CreateMockStore`) with `infosync.GlobalInfoSyncerInit` wired to a
//! PD/etcd fake, execute `ALTER TABLE ... ATTRIBUTES=...` / partition
//! variants, and assert the resulting label-rule state through
//! `select * from information_schema.attributes` — rows whose third column is
//! the restored attribute string and whose fourth column is the key-range
//! JSON over the table/partition IDs (`MockGC` at attributes_sql_test.go:39
//! keeps dropped ranges alive long enough to observe).
//!
//! The DDL-to-label-rule pipeline those observations flow through (the DDL
//! job hook that turns a finished statement into a `label.RulePatch` and
//! pushes it to PD, plus the `information_schema.attributes` table itself) is
//! not transcreated in this tier. What IS transcreated is the isolated
//! `pkg/ddl/label` layer the pipeline feeds — `Rule::apply_attributes_spec`,
//! `Rule::reset` (hex key ranges over table prefixes, keyspace-aware or not)
//! and `NewRulePatch` in `crate::ddl_label` — and those are pinned by
//! `ddl_label.rs`'s inline tests ported from `pkg/ddl/label`'s own unit
//! tests (`TestApplyAttributesSpec`, `TestReset`, ...). These eight
//! SQL-level tests stay documentary `#[ignore]`s; none of their assertions
//! are approximated.

/// GO PORT of `pkg/ddl/attributes_sql_test.go:59
/// TestAlterTablePartitionAttributes`.
///
/// Go pins: per-partition `attributes` with spaces and without `=`, reset to
/// `DEFAULT`, table-level attributes, and — the load-bearing part — that the
/// key-range JSON of the surviving table rule CHANGES as partitions p4/p5 are
/// added, dropped and truncated (physical IDs move) while the rule count
/// stays 1.
#[test]
#[ignore = "go-parity-gap: the DDL hook that pushes label-rule patches to PD and the information_schema.attributes reader are not transcreated; only the isolated Rule::apply_attributes_spec/Reset primitives exist (crate::ddl_label)"]
fn alter_table_partition_attributes() {}

/// GO PORT of `pkg/ddl/attributes_sql_test.go:122 TestTruncateTable`.
///
/// Go pins that `TRUNCATE TABLE` keeps both rules (table + partition p0) but
/// rewrites their key ranges to the NEW physical ids, so the JSON differs
/// from the pre-truncate rows.
#[test]
#[ignore = "go-parity-gap: needs the DDL-to-PD label-rule pipeline; this tier's run_truncate_table_in rewrites the catalog but reaches no label rule"]
fn attributes_truncate_table() {}

/// GO PORT of `pkg/ddl/attributes_sql_test.go:167 TestRenameTable`.
///
/// Go pins that `RENAME TABLE` moves the rules to the new schema/table name
/// (rule IDs `schema/<db>/<table>` re-derived, key ranges unchanged).
#[test]
#[ignore = "go-parity-gap: needs the DDL-to-PD label-rule pipeline (rule IDs are re-derived on rename inside the DDL job, which is not transcreated)"]
fn attributes_rename_table() {}

/// GO PORT of `pkg/ddl/attributes_sql_test.go:227 TestRecoverTable`.
///
/// Go pins that `RECOVER TABLE` (off the GC safe-point/job history) restores
/// the table's attribute rules, including after `RECOVER TABLE BY JOB`.
#[test]
#[ignore = "go-parity-gap: RECOVER TABLE needs the DDL job history and GC safe-point machinery; neither is transcreated in this tier"]
fn attributes_recover_table() {}

/// GO PORT of `pkg/ddl/attributes_sql_test.go:266 TestFlashbackTable`.
///
/// Go pins that `FLASHBACK TABLE TO TOMBSTONE` restores attribute rules for a
/// dropped table within the GC lifetime.
#[test]
#[ignore = "go-parity-gap: FLASHBACK TABLE needs the DDL job pipeline over tombstone meta and GC safe points; not transcreated in this tier"]
fn attributes_flashback_table() {}

/// GO PORT of `pkg/ddl/attributes_sql_test.go:324 TestDropTable`.
///
/// Go pins that `DROP TABLE` deletes the table's rules while its key ranges
/// still exist, and that they stay deleted after GC `DeleteRanges`.
#[test]
#[ignore = "go-parity-gap: the rule deletion side of DROP TABLE lives in the DDL-to-PD label pipeline, which is not transcreated"]
fn attributes_drop_table() {}

/// GO PORT of `pkg/ddl/attributes_sql_test.go:377 TestCreateWithSameName`.
///
/// Go pins the recreate cycle: drop a table with attributes, recreate the
/// same name with different partition-level attributes, and verify GC of the
/// old ranges does not disturb the new rules.
#[test]
#[ignore = "go-parity-gap: needs the GC worker (MockGCWorker.DeleteRanges) plus the DDL-to-PD label pipeline; not transcreated in this tier"]
fn attributes_create_with_same_name() {}

/// GO PORT of `pkg/ddl/attributes_sql_test.go:441 TestPartition`.
///
/// Go pins multi-partition attribute rules: one rule per attributed
/// partition plus the table rule, `drop partition` removing exactly p0's
/// rule, the table rule's ranges shrinking to the remaining partitions, and
/// `add partition`/`truncate partition` re-deriving the range JSON.
#[test]
#[ignore = "go-parity-gap: the DDL hook that pushes label-rule patches to PD and the information_schema.attributes reader are not transcreated"]
fn attributes_partition_rules() {}
