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

//! Ports of Go `pkg/ddl/stat_test.go` (master): `TestGetDDLInfo` (`:34`) and
//! `TestIssue42268` (`:92`). Go reads the DDL job queue through
//! `mysql.tidb_ddl_job` system-table rows and failpoint hooks; none of that
//! machinery is transcreated in this tier, so each test is recorded as an
//! explicit gap with the contract re-derived from the Go source. Nothing is
//! approximated.

/// Go `TestGetDDLInfo` (`pkg/ddl/stat_test.go:34`): hand-inserted
/// `mysql.tidb_ddl_job` rows (encoded `model.Job` metas: a CreateSchema job
/// and an AddIndex job over schema 2) are read back by `ddl.GetDDLInfo` in
/// job-ID order with `ReorgHandle` nil for the non-reorg one -- the session
/// transaction sees its own uncommitted inserts (the test runs inside
/// `begin`/`rollback`).
// go-parity-gap: no `mysql.tidb_ddl_job` system table, no job-meta encoder
// wired to a readable queue, and no `ddl.GetDDLInfo` carrier
// (`pkg/ddl/stat.go` is not transcreated).
#[test]
#[ignore]
fn get_ddl_info_reads_pending_jobs_from_the_system_table() {
}

/// Go `TestIssue42268` (`pkg/ddl/stat_test.go:92`): during DROP TABLE, at
/// every intermediate schema state (DeleteOnly/WriteOnly/WriteReorganization,
/// hooked via the `beforeRunOneJobStep` failpoint), `admin show ddl jobs`
/// must already report the table NAME `t_0` in the job row -- not just its
/// ID (issue 42268: the name went missing mid-drop).
// go-parity-gap: no `admin show ddl jobs` result builder, no DDL job
// lifecycle, and no failpoint hooks in this tier.
#[test]
#[ignore]
fn admin_show_ddl_jobs_keeps_the_table_name_during_drop() {
}
