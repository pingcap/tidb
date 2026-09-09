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

//! `#[ignore]` gap ports of Go `pkg/ddl/util/util_test.go` (part-17 items
//! `TestFolderNotEmpty` :28, `TestHasSysDB` :39 and `TestPauseRunningJob`
//! :78; the package's `TestMain` at `pkg/ddl/util/main_test.go:24` is
//! harness-only setup — goleak and test-setup init — with nothing to pin).
//!
//! The three functions under test live in `pkg/ddl/util/util.go` —
//! `FolderNotEmpty` :443, `HasSysDB` :76 and `PauseRunningJob` :86 — and this
//! tier transcreates none of them: no GC/etcd worker consumes a
//! folder-emptiness check, no admin-pause DDL path composes the job-state
//! predicates, and no DDL-history filter asks whether a job involves a system
//! database. The building blocks exist one layer down (`tidb_model::Job::{`
//! `is_pausing`, `is_paused`, `is_pausable`}` for Go `Job.IsPausing`/`
//! `IsPaused`/`IsPausable` (`pkg/meta/model/job.go:763-775`),
//! `tidb_metadef::is_system_related_db` for Go
//! `pkg/meta/metadef/db.go:56`, and the `ErrCannotPauseDDLJob` 8260 /
//! `ErrPausedDDLJob` 8262 codes in `tidb-error`), but the composed
//! `pkg/ddl/util` contracts themselves are unported, so each test records the
//! gap instead of re-testing the primitives under my own composition.
//! Nothing is approximated.

/// Go `util_test.go:28-37::TestFolderNotEmpty`. `FolderNotEmpty` (Go
/// `pkg/ddl/util/util.go:443`, an `os.ReadDir` + `len(entries) > 0`) answers
/// false for an empty directory, false for a missing one (the read error is
/// swallowed), and true once any file exists inside.
// go-parity-gap: no port of pkg/ddl/util's FolderNotEmpty; the tier has no
// consumer for it (Go's callers sit in the GC/etcd machinery).
#[test]
#[ignore]
fn folder_not_empty_reports_whether_a_directory_has_entries() {
}

/// Go `util_test.go:39-63::TestHasSysDB`. `HasSysDB` (Go
/// `pkg/ddl/util/util.go:76`) walks `job.GetInvolvingSchemaInfo()` and
/// answers true iff any entry's database is system-related
/// (`metadef.IsSystemRelatedDB`, `pkg/meta/metadef/db.go:56`: `mysql`, `sys`,
/// or the workload schema): a lone `test` database is false, `mysql` alone is
/// true, and `test` mixed with `sys` is true.
// go-parity-gap: no port of pkg/ddl/util's HasSysDB; the tier's job model
// carries InvolvingSchemaInfo but nothing asks the system-db question.
#[test]
#[ignore]
fn has_sys_db_detects_a_system_database_in_the_job_schemas() {
}

/// Go `util_test.go:78-135::TestPauseRunningJob` (four subtests).
/// `PauseRunningJob(job, byWho)` (Go `pkg/ddl/util/util.go:86`):
/// `JobStateQueueing` (not started) becomes `JobStatePausing` with
/// `AdminOperator = byWho` and no error; an already-`Pausing` job (paused by
/// system) returns `dbterror.ErrPausedDDLJob` (8262) unchanged; a `Paused`
/// job likewise returns 8262 unchanged; and a `Done`/`StatePublic` job is not
/// pausable at all (`Job.IsPausable`, `pkg/meta/model/job.go:769`) and
/// returns `dbterror.ErrCannotPauseDDLJob` (8260) with message
/// `state [done] or schema state [public]`, leaving job state and operator
/// untouched.
// go-parity-gap: no port of pkg/ddl/util's PauseRunningJob; the tier has no
// admin pause DDL entry point composing the job-state predicates.
#[test]
#[ignore]
fn pause_running_job_moves_runnable_jobs_to_pausing_and_refuses_the_rest() {
}
