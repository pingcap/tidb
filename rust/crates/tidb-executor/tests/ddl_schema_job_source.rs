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

//! Ports of Go `pkg/ddl/schema_test.go` and `pkg/ddl/schema_version_test.go`
//! (pkg/ddl batch). The schema tests drive `DoDDLJobWrapper` -- the raw DDL
// job submission API -- against a mockstore domain, and the schema-version
//! test pins a nextgen-only predicate; none of that machinery exists in
// this tier, so each test is recorded as an explicit gap with the contract
// re-derived from the Go source. Nothing is approximated.

/// Go `TestSchema` (`pkg/ddl/schema_test.go:221`): create schema (job done,
/// StatePublic), create two tables under it (100 and 1034 rows via direct
/// `AddRecord`), drop the schema and observe the tables' keys removed from
/// the delete-range set; dropping a NON-EXISTENT schema by hand-built job
/// (Version 1, ActionDropSchema) fails with
/// `infoschema.ErrDatabaseDropExists`; dropping an empty database succeeds
/// with `testCheckJobDone(..., isRollbackDone=false)`.
// go-parity-gap: no DoDDLJobWrapper/job-history surface, no mockstore
// meta KV to read job state from.
#[test]
#[ignore]
fn schema_create_drop_with_tables_tracks_job_history() {
}

/// Go `TestSchemaWaitJob` (`pkg/ddl/schema_test.go:294`): a SECOND DDL
/// instance (`ddl.NewDDL` + Start) that is NOT the owner must still accept
/// a job submission; the non-owner retires, and `DoDDLJobWrapper` of a
/// CREATE SCHEMA over an existing id ends in the history as a cancelled /
/// rollback-done job (`testCheckJobCancelled`).
// go-parity-gap: no owner-manager election or second-instance harness.
#[test]
#[ignore]
fn schema_wait_job_reports_cancelled_history_from_a_non_owner() {
}

/// Go `TestRenameTableAutoIDs` (`pkg/ddl/schema_test.go:379`): rename table
// across schemas under concurrent uncommitted writers with AUTO_ID_CACHE
// 100 -- row ids keep allocating from the same counter across the
/// infoschema v1->v2 switch (issue #46904 fix), the rename job is observed
/// in `admin show ddl jobs` at states running/public/done/synced, and the
/// final `_tidb_rowid` sequence of the renamed table is exactly
/// 13,14,15,17,19,51,53,54,56,58,59,60 with its `a` values.
// go-parity-gap: no DDL job lifecycle to wait on, no multi-session
// infoschema-version switch harness, no `_tidb_rowid` allocator exposed at
// this tier.
#[test]
#[ignore]
fn rename_table_auto_ids_survive_the_infoschema_version_switch() {
}

/// Go `TestShouldCheckAssumedServer` (`pkg/ddl/schema_version_test.go:26`):
/// `shouldCheckAssumedServer` (`pkg/ddl/schema_version.go:406`) is FALSE
/// unconditionally on the CLASSIC kernel, and on NEXTGEN it is
/// `metadef.IsReservedID(job.TableID)` (`pkg/meta/metadef/system.go:162`):
/// TableID 100 -> false, TableID `metadef.ReservedGlobalIDUpperBound` ->
/// true.
// go-parity-gap: no Rust carrier of shouldCheckAssumedServer or the
// kernel-type switch it gates on.
#[test]
#[ignore]
fn should_check_assumed_server_is_nextgen_and_reserved_id_gated() {
}
