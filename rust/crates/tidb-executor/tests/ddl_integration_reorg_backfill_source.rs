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

//! Port ledger for `pkg/ddl/integration_test.go` (pkg/ddl.part7 items
//! 396-400 of the local enumeration, lines 61-239). All five are mockstore
//! SQL tests over the DDL job lifecycle; the crate's DDL tier applies
//! metadata directly with no job queue, so the four runnable-in-Go tests are
//! documentary gap ports and the nextgen-gated one inherits Go's own skip.

/// GO PORT of `pkg/ddl/integration_test.go:61 TestDDLStatementsBackFill`.
///
/// Re-derived contract: of `alter table t modify column a bigint`,
/// `modify column b char(255)`, `create table t1`, and
/// `alter table t1 drop primary key`, none enters write-reorganization;
/// `modify column a varchar(100)` (int -> varchar needs a data rewrite),
/// `add index idx_a(a)`, and `add primary key(b) nonclustered` do —
/// observed through the `afterWaitSchemaSynced` failpoint flipping a flag
/// when the job's SchemaState is StateWriteReorganization.
#[test]
#[ignore = "go-parity-gap: needs the DDL job state machine (SchemaState write-reorg transitions); the crate applies metadata directly with no job states"]
fn ddl_statements_backfill_exactly_when_write_reorganization_is_entered() {}

/// GO PORT of `pkg/ddl/integration_test.go:92 TestPartialIndex`.
///
/// Re-derived contract: `create table t (a int, b int, key(b) where a = 1)`
/// is ACCEPTED (partial indexes exist), while a partial index whose WHERE
/// references an unknown column, or a PRIMARY KEY with a WHERE, fails with
/// `dbterror.ErrUnsupportedAddPartialIndex`; the literal/column-type matrix
/// requires the comparison literal's type to match the column's type family
/// (int literal for int/year columns, string for strings, float for
/// float/double, binary literal for binary columns, nothing for other
/// types), with timestamp/datetime/date/time accepting string literals and
/// enum/set accepting int and string literals — anything else is refused.
///
/// go-parity-gap note: the Rust carrier REFUSES every partial index at
/// definition time (`reject_partial_index`, src/ddl/indexes.rs:113-117, also
/// wired into CREATE TABLE constraints and ALTER TABLE ADD INDEX), so not a
/// single matrix row is executable here; pinning Go's accept side against a
/// refuse-everything carrier would be an approximation, so this stays a
/// documented divergence instead of a test.
#[test]
#[ignore = "go-parity-gap: Rust reject_partial_index (src/ddl/indexes.rs:113-117) refuses all partial indexes; Go's accept/validation matrix (pkg/ddl/index.go) is not transcreated"]
fn partial_index_accepts_only_type_matched_where_comparisons() {}

/// GO PORT of `pkg/ddl/integration_test.go:178
/// TestDropTableAdminCheckTableFastCheckTable`.
///
/// Re-derived contract: with `config.CheckTableBeforeDrop` enabled, DROP
/// TABLE runs the admin-check/fast-check path first (one session with
/// `tidb_fast_check_table` off, one on) and the drop still succeeds.
#[test]
#[ignore = "go-parity-gap: config.CheckTableBeforeDrop and the drop-time fast-check path are not transcreated"]
fn drop_table_with_check_before_drop_runs_the_fast_check() {}

/// GO PORT of `pkg/ddl/integration_test.go:209 TestMaintainAffectColumns`.
///
/// Re-derived contract: a partial index over `col2` records
/// `AffectColumn[0].Offset` = col2's position, and that offset is
/// maintained as `add column col1 int first` shifts it to 1,
/// `add column col3 int after col1` shifts it to 2, and
/// `drop column col1` brings it back to 1 — index metadata offsets track
/// column insertion/removal positions.
#[test]
#[ignore = "go-parity-gap: the partial-index carrier refuses the table (src/ddl/indexes.rs:113) and affect-column offset maintenance on add/drop column is not transcreated"]
fn maintain_affect_columns_tracks_offsets_across_column_changes() {}

/// GO PORT of `pkg/ddl/integration_test.go:239
/// TestJobVersionAndGlobalIndexV1SupportForNextGen`.
///
/// Re-derived contract: Go itself skips this test unless
/// `kerneltype.IsNextGen()` ("nextgen only"); on nextgen it sets the global
/// config's store to TiKV for DDL init and checks job-version and
/// global-index V1 support interactions. The crate has no kernel-type
/// switch and no job-version surface, so there is nothing to pin.
#[test]
#[ignore = "go-parity-gap: Go test is nextgen-kernel-gated (t.Skip otherwise) and drives DDL job-version/global-index V1 machinery the crate does not carry"]
fn job_version_and_global_index_v1_support_is_nextgen_gated() {}
