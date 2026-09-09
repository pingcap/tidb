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

//! `#[ignore]` gap ports of the failure-injection family of
//! `pkg/ddl/tests/serial/serial_test.go` in this batch:
//! `TestCancelAddIndexPanic` (:435), `TestCancelJobByErrorCountLimit`
//! (:977), `TestTruncateTableUpdateSchemaVersionErr` (:994),
//! `TestCanceledJobTakeTime` (:1013), `TestCreateTableNoBlock` (:1378), plus
//! `pkg/ddl/tests/serial/main_test.go:39::TestMain` (recorded in the receipt
//! as skipped-reason: it is harness setup only).

/// Go `serial_test.go:435-465::TestCancelAddIndexPanic`: with
/// `errorMockPanic` enabled and a `beforeRunOneJobStep` hook that cancels
/// the ADD INDEX job the moment it reaches
/// `StateWriteReorganization` with a non-zero snapshot, the ALTER answers
/// `[ddl:8214]Cancelled DDL job...` instead of surfacing the injected panic.
// go-parity-gap: the online-DDL job queue, its state machine and the
// failpoint hooks are not transcreated; this tier applies DDL synchronously
// (crate::ddl module doc).
#[test]
#[ignore]
fn cancelling_an_add_index_in_write_reorganization_answers_8214() {
}

/// Go `serial_test.go:977-992::TestCancelJobByErrorCountLimit`: with
/// `mockExceedErrorLimit` enabled and
/// `@@global.tidb_ddl_error_count_limit = 16`, a CREATE TABLE whose job
/// fails repeatedly answers
/// `[ddl:-1]DDL job rollback, error msg: mock do job error`.
// go-parity-gap: the DDL job retry/rollback loop and the
// mockExceedErrorLimit failpoint do not exist in this tier.
#[test]
#[ignore]
fn exceeding_the_ddl_error_count_limit_rolls_the_job_back() {
}

/// Go `serial_test.go:994-1011::TestTruncateTableUpdateSchemaVersionErr`:
/// with `mockTruncateTableUpdateVersionError` enabled and the error-count
/// limit at 5, TRUNCATE TABLE answers
/// `[ddl:-1]DDL job rollback, error msg: mock update version error`; with
/// the failpoint disabled the same TRUNCATE succeeds.
// go-parity-gap: the version-update failure injection and job rollback need
// the online-DDL job queue.
#[test]
#[ignore]
fn truncate_table_version_failure_rolls_the_job_back_then_retry_succeeds() {
}

/// Go `serial_test.go:1013-1042::TestCanceledJobTakeTime`: a hook deletes
/// the job's table behind its back; the follow-up ALTER then fails with
/// ErrNoSuchTable and must return FASTER than
/// `ddl.WaitTimeWhenErrorOccurred` (1s), because the missing-table error is
/// not retryable and cancels the job immediately.
// go-parity-gap: no job retry pacing (`ddl.WaitTimeWhenErrorOccurred`) and
// no meta mutator to delete a table behind a job's back.
#[test]
#[ignore]
fn canceling_an_unretryable_job_does_not_wait_for_the_retry_interval() {
}

/// Go `serial_test.go:1378-1394::TestCreateTableNoBlock`: with
/// `checkOwnerCheckAllVersionsWaitTime` pinned and the error-count limit at
/// 1, a CREATE TABLE whose owner-wait keeps failing answers an error rather
/// than blocking (the wait loop respects the deadline).
// go-parity-gap: no owner/version-check wait loop in this tier's
// synchronous DDL.
#[test]
#[ignore]
fn create_table_fails_fast_when_the_owner_wait_keeps_failing() {
}
