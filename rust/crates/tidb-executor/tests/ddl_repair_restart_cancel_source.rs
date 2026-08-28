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

//! Ports of Go `pkg/ddl/repair_table_test.go`, `pkg/ddl/restart_test.go` and
//! the `TestCancelAddIndexJobError` half of `pkg/ddl/rollingback_test.go`
//! (pkg/ddl batch). All six drive the live DDL job machinery -- repair
// mode, interrupted-job restart, and admin-cancel-with-failpoint -- through
// a mockstore + domain stack that this tier does not carry. Each is
// recorded as an explicit gap with the contract re-derived from the Go
// source. Nothing is approximated.

/// Go `TestRepairTable` (`pkg/ddl/repair_table_test.go:39`): outside
/// REPAIR MODE `admin repair table` is refused with
/// "[ddl:8215]Failed to repair table: TiDB is not in REPAIR MODE"; then, in
/// repair mode, in order: empty repair list, database not in the list,
/// table not in the list (8215 each with its own suffix), a repaired table
/// is invisible to readers (1146), a CREATE under the repaired name is
/// "[ddl:1103]Incorrect table name 'other_table'%!(EXTRA string=this table
/// is in repair)" (the %!(EXTRA ...) is Go's own fmt artifact in the
/// golden string), a lost column ("Column c has lost"), changed column type
/// ("Column a type should be the same"), lost index ("Index a has lost")
/// and changed index type ("Index b type should be the same") are all
/// refused; a matching
/// repair succeeds, name matching is case-insensitive against the list
/// while the repaired name keeps its case, repair is refused before
/// FetchAllSchemasWithTables ran, and a successful repair keeps table id,
/// column ids, index id and auto-inc id while swapping column types
/// (verified through SHOW CREATE TABLE text).
// go-parity-gap: no REPAIR MODE, no admin repair statement, and no
// domainutil.RepairInfo equivalent in the Rust tier.
#[test]
#[ignore]
fn repair_table_gates_on_repair_mode_and_preserves_ids() {
}

/// Go `TestRepairTableWithPartition`
/// (`pkg/ddl/repair_table_test.go:181`): repairing a RANGE-partitioned
// table keeps the partition layout (some old partitions may be lost, new
// ones may be added) with the same id-preservation contract as the
// non-partitioned repair above.
// go-parity-gap: no REPAIR MODE / admin repair carrier.
#[test]
#[ignore]
fn repair_table_with_partition_keeps_the_range_layout() {
}

/// Go `TestSchemaResume` (`pkg/ddl/restart_test.go:109`): an interrupted
/// CREATE SCHEMA job (worker killed mid-flight via `testRunInterruptedJob`)
/// is picked up again by a restarted owner and completes to StatePublic;
/// the same holds for the DROP SCHEMA job built by `buildDropSchemaJob`,
/// ending at StateNone.
// go-parity-gap: no DDL job queue, owner manager or worker-restart loop in
// the Rust tier.
#[test]
#[ignore]
fn schema_resume_completes_interrupted_create_and_drop_jobs() {
}

/// Go `TestStat` (`pkg/ddl/restart_test.go:133`): while a DROP SCHEMA job
// runs, restarting the DDL workers on every schema lease tick never
// regresses `getDDLSchemaVer`, and the job finishes without error.
// go-parity-gap: no DDL job machinery (see TestSchemaResume above).
#[test]
#[ignore]
fn stat_schema_version_never_regresses_across_worker_restarts() {
}

/// Go `TestTableResume` (`pkg/ddl/restart_test.go:162`): interrupted CREATE
/// TABLE and DROP TABLE jobs resume to StatePublic / StateNone exactly like
/// the schema-level jobs.
// go-parity-gap: no DDL job machinery.
#[test]
#[ignore]
fn table_resume_completes_interrupted_create_and_drop_jobs() {
}

/// Go `TestCancelAddIndexJobError` (`pkg/ddl/rollingback_test.go:34`): with
/// the `mockConvertAddIdxJob2RollbackJobError` failpoint on,
/// `admin cancel ddl jobs <id>` issued at StateDeleteOnly drives the ADD
/// INDEX job into rollback; the conversion-to-rollback error path marks the
/// job cancelled-with-error, and after `tidb_ddl_error_count_limit` retries
/// the final state reports the conversion failure instead of hanging.
// go-parity-gap: no failpoint hooks (afterWaitSchemaSynced,
// mockConvertAddIdxJob2RollbackJobError), no admin cancel surface, no DDL
// job state machine in the Rust tier.
#[test]
#[ignore]
fn cancel_add_index_job_survives_the_rollback_conversion_error() {
}
