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

//! Documented go-parity-gap ports of the first three
//! `pkg/ddl/multi_schema_change_test.go` tests in this batch (the file's
//! remaining tests belong to later parts of the batch split; read from the
//! origin/master snapshot). All three drive the multi-schema job state
//! machine through cancel hooks (`afterWaitSchemaSynced` /
//! `newCancelJobHook`) and parallel double-submission
//! (`putTheSameDDLJobTwice`) — machinery of the online-DDL job queue this
//! tier does not build (the `crate::ddl` module doc: DDL applies
//! synchronously to metadata). Each port carries the re-derived contract
//! with Go symbol locations.

/// Go `multi_schema_change_test.go:39::TestMultiSchemaChangeAddColumnsCancelled`.
/// Go submits `ALTER TABLE t ADD COLUMN b..d` (3 sub-jobs) and cancels the
/// job through the hook when sub-job[1] (`c`) reaches
/// `StateWriteReorganization`; the statement must answer
/// `errno.ErrCancelledDDLJob`, `hook.MustCancelDone` must hold, and the
/// table must be back to its pre-DDL content (`select * from t` → `1`) —
/// a cancelled multi-schema ADD COLUMN rolls back cleanly with no partial
/// columns left public.
// go-parity-gap: multi-schema sub-job state transitions and cancellation
// need the online-DDL job queue; this tier applies DDL synchronously.
#[test]
#[ignore]
fn multi_schema_change_add_columns_cancelled_rolls_back_cleanly() {
}

/// Go `multi_schema_change_test.go:62::TestMultiSchemaChangeAddColumnsParallel`.
/// The same multi-schema ALTER is submitted twice
/// (`putTheSameDDLJobTwice`): with `IF NOT EXISTS` on both added columns
/// the second run converges — two warnings `Note 1060 Duplicate column
/// name 'b'/'c'`, exactly one copy of each column, rows `1 2 3`; without
/// `IF NOT EXISTS` the duplicate submission answers
/// `errno.ErrDupFieldName`. Duplicate-column tolerance and the
/// double-submission harness are job-queue semantics.
// go-parity-gap: parallel duplicate submission and its warning/duplicate
// outcomes need the online-DDL job queue.
#[test]
#[ignore]
fn multi_schema_change_add_columns_parallel_converges_or_reports_duplicates() {
}

/// Go `multi_schema_change_test.go:84::TestMultiSchemaChangeDropColumnsCancelled`.
/// Cancelling during `StateDeleteReorganization` FAILS (the drop is already
/// past its last undo point: `MustCancelFailed`, statement succeeds, and
/// `select * from t` shows only column `c`'s value `3`); cancelling in
/// `StatePublic` SUCCEEDS (`errno.ErrCancelledDDLJob`, `MustCancelDone`)
/// and restores all four columns (`1 2 3 4`). The asymmetric
/// cancelability of drop-column sub-jobs across states is the contract.
// go-parity-gap: state-dependent cancellation of drop-column sub-jobs
// needs the online-DDL job queue.
#[test]
#[ignore]
fn multi_schema_change_drop_columns_cancel_is_state_dependent() {
}
