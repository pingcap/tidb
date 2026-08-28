// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, 2.0 (the "License");
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

//! Ports of the three `pkg/ddl/db_change_failpoints_test.go` tests assigned to
//! this batch (origin/master).
//!
//! The Go file's whole surface is failpoint-driven DDL-job behavior — job
//! args surviving an injected `updateVersionAndTableInfo` failure, parallel
//! TiFlash-replica updates, parallel flashback. None of these have a
//! counterpart in this tier (no job queue, no failpoint injection points, no
//! TiFlash replica bookkeeping), so each is recorded as an `#[ignore]`d gap
//! with its re-derived contract.

/// `db_change_failpoints_test.go:37::TestModifyColumnTypeArgs` — job raw args
/// survive a failed `updateVersionAndTableInfo`.
#[test]
#[ignore = "go-parity-gap: injects mockUpdateVersionAndTableInfoErr on the second call and reads the persisted history job's args; no job persistence here (Go db_change_failpoints_test.go:37-72)"]
fn modify_column_type_args_survive_a_failed_version_update() {
    // Derivation: t_modify_column_args (a int, unique(a));
    // `alter table .. modify column a varchar(16)` fails with
    // "[ddl:-1]mock update version and tableInfo error,jobID=<id>"; the
    // table meta keeps exactly 1 column and 1 index, and the history job's
    // ModifyColumnArgs has ChangingColumn == nil and ChangingIdxs == nil —
    // the failure must not have written CTC artifacts into the job args.
}

/// `db_change_failpoints_test.go:74::TestParallelUpdateTableReplica`.
#[test]
#[ignore = "go-parity-gap: races two UpdateTableReplicaInfo calls under mockTiFlashStoreCount; TiFlash replica bookkeeping is not modeled here (Go db_change_failpoints_test.go:74-109)"]
fn parallel_update_table_replica_rejects_the_second_update() {
    // Derivation: t1 (a int) with `alter table t1 set tiflash replica 3
    // location labels 'a','b'`; two racing
    // DDLExecutor().UpdateTableReplicaInfo(.., available = true) calls — the
    // first succeeds, the second fails
    // "[ddl:-1]the replica available status of table t1 is already updated".
}

/// `db_change_failpoints_test.go:111::TestParallelFlashbackTable`.
#[test]
#[ignore = "go-parity-gap: FLASHBACK TABLE needs GC safe-point bookkeeping, autoid mock and parallel job control; none modeled here (Go db_change_failpoints_test.go:111-160)"]
fn parallel_flashback_table_rejects_the_duplicate_name() {
    // Derivation: with emulator GC disabled and a safe point 48h in the
    // past: after `drop table t`, two racing `flashback table t to
    // t_flashback` — one succeeds, the other fails "[schema:1050]Table
    // 't_flashback' already exists"; the rename variant
    // (`flashback table t_flashback` vs `.. to t_flashback2`) fails the same
    // way.
}
