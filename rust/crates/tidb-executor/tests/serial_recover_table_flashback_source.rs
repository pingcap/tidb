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

//! `#[ignore]` gap ports of the recover/flashback family of
//! `pkg/ddl/tests/serial/serial_test.go` in this batch:
//! `TestRecoverTableWithTTL` (:467), `TestRecoverTableByJobID` (:531),
//! `TestRecoverTableUsesRealStartTSForQueuedDropTable` (:647),
//! `TestFlashbackDatabaseUsesRealStartTSForQueuedDropSchema` (:762),
//! `TestRecoverTableByJobIDFail` (:854) and
//! `TestRecoverTableByTableNameFail` (:920).
//!
//! Go drives RECOVER TABLE / FLASHBACK through the online-DDL job queue over
//! a mock store, gated by `mysql.tidb` GC variables (`tikv_gc_safe_point`,
//! `tikv_gc_enable`) and emulator-GC switches. This tier's recover support
//! is the statement-level pre-checks only (`crate::ddl_exec`'s
//! `RecoverTableCheck`, from Go `executeRecoverTable` `pkg/ddl/executor.go`
//! :434/:448); the job machinery, GC worker and schema-history scan do not
//! exist, so every test below carries its re-derived Go contract and an
//! honest gap note. Nothing is approximated.

/// Go `serial_test.go:467-529::TestRecoverTableWithTTL`: a dropped table
/// whose columns carry a TTL policy (`TTL=`t`+INTERVAL 1 DAY`) recovers —
/// by name, by job id, and by FLASHBACK TABLE/DATABASE — with
/// `SHOW CREATE TABLE` re-printing `TTL=`t` + INTERVAL 1 DAY`,
/// `TTL_ENABLE='OFF'` and `TTL_JOB_INTERVAL='24h'` (the recovery resets
/// TTL_ENABLE to OFF).
// go-parity-gap: RECOVER/FLASHBACK job execution, the DDL history scan
// (GetDropOrTruncateTableInfoFromJobs) and the GC safe-point table are not
// transcreated; this tier only models the recover pre-checks.
#[test]
#[ignore]
fn recover_table_with_ttl_keeps_the_ttl_policy_but_disables_it() {
}

/// Go `serial_test.go:531-645::TestRecoverTableByJobID`: `recover table by
/// job <id>` answers `can not get 'tikv_gc_safe_point'` before the variable
/// exists, succeeds once it is set before the drop's safe point, refuses a
/// snapshot older than the safe point ("snapshot is older than GC safe
/// point"), refuses a name already taken (`infoschema.ErrTableExists`),
/// fails for a nonexistent job, preserves rows and auto-ids across the
/// recovery, re-enables GC when it was enabled before, and recovers a
/// TRUNCATEd table under a new name.
// go-parity-gap: no recover-table job execution, no `mysql.tidb` GC
// variable carrier, no DDL history.
#[test]
#[ignore]
fn recover_table_by_job_id_gates_on_gc_safe_point_and_preserves_autoids() {
}

/// Go `serial_test.go:647-760::TestRecoverTableUsesRealStartTSForQueuedDropTable`:
/// when a drop-table job is QUEUED behind a submitted drop-column job, the
/// drop job's `RealStartTS` (read back via `ddl.GetHistoryJobByID`) is
/// GREATER than its `StartTS`, and the recovery uses that real start time so
/// the recovered table keeps the pre-drop column set (`id`, `col_b` — the
/// recovered snapshot predates the column drop) both in
/// `information_schema.columns` and in the recovered rows; the recover job's
/// BinlogInfo.TableInfo lists exactly those columns.
// go-parity-gap: no DDL job queue (waitJobSubmitted /
// beforeLoadAndDeliverJobs failpoints), no job history, no
// RealStartTS bookkeeping in this tier.
#[test]
#[ignore]
fn recover_table_uses_the_real_start_ts_of_a_queued_drop_job() {
}

/// Go `serial_test.go:762-852::TestFlashbackDatabaseUsesRealStartTSForQueuedDropSchema`:
/// the schema-level twin — a FLASHBACK DATABASE of a schema whose drop job
/// queued behind another DDL recovers the tables from the real start-time
/// snapshot, keeping the pre-drop columns and rows.
// go-parity-gap: no flashback-database job execution, no job history, no
// RealStartTS bookkeeping in this tier.
#[test]
#[ignore]
fn flashback_database_uses_the_real_start_ts_of_a_queued_drop_schema() {
}

/// Go `serial_test.go:854-918::TestRecoverTableByJobIDFail`: recovering
/// through injected commit errors (`tikvclient/mockCommitError` +
/// `mockRecoverTableCommitErr` enabled from the beforeRunOneJobStep hook)
/// still succeeds, and GC ends ENABLED after the recovery (`gcutil.CheckGCEnable`
/// true) with rows and auto-ids preserved.
// go-parity-gap: no recover-table job execution and no commit-error
// failpoint hooks in this tier.
#[test]
#[ignore]
fn recover_table_survives_injected_commit_errors_and_reenables_gc() {
}

/// Go `serial_test.go:920-975::TestRecoverTableByTableNameFail`: the
/// by-name spelling (`recover table t_recover`) has the same commit-error
/// resilience, GC re-enable and data/auto-id preservation contract.
// go-parity-gap: no recover-table job execution and no commit-error
// failpoint hooks in this tier.
#[test]
#[ignore]
fn recover_table_by_name_survives_injected_commit_errors_and_reenables_gc() {
}
