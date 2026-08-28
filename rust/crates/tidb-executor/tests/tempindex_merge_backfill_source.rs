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

//! Ports of Go `pkg/ddl/tests/indexmerge/merge_test.go::TestAddIndexMergeDeleteNullUnique`
//! through `::TestAddIndexInsertAfterReorgSkipCheck` (items 743-759 of the
//! pkg/ddl `Test*` enumeration). Every one of them drives the TEMP INDEX
//! backfill-merge process: the ADD INDEX job walks Go's schema states
//! (DeleteOnly -> WriteOnly -> WriteReorganization, `pkg/meta/model/job.go`)
//! while a second session writes ordinary and temp-index entries, and the
//! failpoints `mockDMLExecution` / `mockDMLExecutionMerging`
//! (`pkg/ddl/index.go:2916-2930,3855`), `afterWaitSchemaSynced`
//! (`pkg/ddl/job_scheduler.go:644`), `beforeRunOneJobStep` /
//! `afterRunOneJobStep` (`pkg/ddl/job_worker.go:642,652`),
//! `beforeRunReorgJobAndHandleErr` and the two reorg-skip probes
//! (`pkg/ddl/index.go:1903,1934,2019`) interleave those writes with the
//! `BackfillState` machine Running -> ReadyToMerge -> Merging
//! (`pkg/meta/model/reorg.go:33-44`).
//!
//! The Rust tier (`tidb-executor`) performs a synchronous backfill inside
//! `run_create_index_in` (`src/ddl/indexes.rs:38`): there is no job state
//! machine, no temp index, no `BackfillState`, no failpoint hooks and no
//! second session, so none of these contracts can run here. Each test below
//! is recorded as an explicit gap with the contract re-derived from the Go
//! source and cited. Nothing is approximated.

/// Go `TestAddIndexMergeDeleteNullUnique`
/// (`pkg/ddl/tests/indexmerge/merge_test.go:354`): `t(id int primary key,
/// a int default 0)` holds rows `(1,1)` and `(2,NULL)`; while the ADD UNIQUE
/// INDEX idx(a) job runs, `MockDMLExecution` (the `mockDMLExecution`
/// failpoint fires exactly once, `1*return(true)->return(false)`,
/// `pkg/ddl/index.go:2916`) deletes row id=2. The backfill saw `(2,NULL)`
/// and wrote a temp-index entry for it; the merge must drop the entry whose
/// row was deleted and must NOT fail the NULL unique entry, so the finished
/// table holds exactly 1 row and `admin check table t` passes.
// go-parity-gap: no temp-index merge carrier -- no BackfillState machine
// (pkg/meta/model/reorg.go:33-44), no mockDMLExecution failpoint
// (pkg/ddl/index.go:2916), no second session.
#[test]
#[ignore]
fn add_index_merge_delete_null_unique() {
}

/// Go `TestAddIndexMergeDoubleDelete`
/// (`pkg/ddl/tests/indexmerge/merge_test.go:376`): empty
/// `t(id int primary key, a int default 0)`; the WriteOnly state inserts
/// `(1,1)`, then `MockDMLExecution` runs `delete id=1`, `insert (2,1)`,
/// `delete id=2` -- two delete marks over the SAME temp-index value a=1
/// across two rows. ADD UNIQUE INDEX idx(a) must tolerate the double delete
/// (the merge applies both marks without a missing-key error) and finish
/// with `count(1) = 0`; `admin check table t` passes.
// go-parity-gap: same missing temp-index merge carrier as
// add_index_merge_delete_null_unique.
#[test]
#[ignore]
fn add_index_merge_double_delete() {
}

/// Go `TestAddIndexMergeConflictWithPessimistic`
/// (`pkg/ddl/tests/indexmerge/merge_test.go:412`, classic kernel only -- it
/// skips itself under next-gen where MDL is always on, :414-416): the
/// WriteOnly state updates a=2; a pessimistic txn (`begin pessimistic`,
/// update a=3) starts once the job reaches BackfillStateReadyToMerge with
/// `tidb_enable_metadata_lock` switched OFF (:433). The ALTER must still be
/// BLOCKED by the pessimistic txn -- the 300ms timer expires before the
/// `afterCommit` signal fires -- and only after `rollback` does the ALTER
/// finish. Final state: `select * from t` is exactly `1 2` (the WriteOnly
/// update survived, the rolled-back pessimistic update did not) and
/// `admin check table t` passes. `CheckBackfillJobFinishInterval` is
/// shortened to 50ms for the merge poll (`pkg/ddl/dist_owner.go:23`).
// go-parity-gap: needs BackfillStateReadyToMerge + a pessimistic-lock
// transaction carrier + job blocking; neither exists in this tier.
#[test]
#[ignore]
fn add_index_merge_conflict_with_pessimistic() {
}

/// Go `TestAddIndexMergeInsertOnMerging`
/// (`pkg/ddl/tests/indexmerge/merge_test.go:536`): DeleteOnly inserts
/// `(5,5)`, WriteOnly inserts `(5,7)` then deletes `b=7`; once
/// `beforeRunReorgJobAndHandleErr` observes `BackfillStateMerging`
/// (`pkg/ddl/index.go:2019`), `insert (5,8)` fails with
/// `[kv:1062]Duplicate entry '5' for key 't.idx'` (the merged temp index
/// already holds a=5) while `insert (5,8) on duplicate key update a=6`
/// succeeds by updating the EXISTING row to `(6,5)`. Final `select * from t`
/// is exactly `6 5`; `admin check table t` passes.
// go-parity-gap: BackfillStateMerging probe (beforeRunReorgJobAndHandleErr,
// pkg/ddl/index.go:2019) and temp-index duplicate detection have no Rust
// carrier.
#[test]
#[ignore]
fn add_index_merge_insert_on_merging() {
}

/// Go `TestAddIndexMergeReplaceOnMerging`
/// (`pkg/ddl/tests/indexmerge/merge_test.go:579`): row `(5,5)`;
/// `MockDMLExecution` deletes `b=5` during backfill; in
/// `BackfillStateMerging` a `replace into t values (5,8)` succeeds. The
/// merge overwrites the temp entry so the final table is exactly `5 8`;
/// `admin check table t` passes.
// go-parity-gap: same missing merge-phase carrier
// (pkg/ddl/index.go:2019 + mockDMLExecution pkg/ddl/index.go:2916).
#[test]
#[ignore]
fn add_index_merge_replace_on_merging() {
}

/// Go `TestAddIndexMergeInsertToDeletedTempIndex`
/// (`pkg/ddl/tests/indexmerge/merge_test.go:612`): row `(5,5)`; in the
/// WriteOnly state tk1 deletes `b=5`, then with
/// `tidb_constraint_check_in_place = true` inserts `(5,8)` (succeeds), a
/// second `(5,8)` fails (in-place unique check), and after switching back to
/// `in_place = false` a third `(5,8)` STILL fails -- the temp index detects
/// the duplicate without the in-place check. ADD UNIQUE INDEX idx(a)
/// finishes with the table exactly `5 8`; `admin check table t` passes.
// go-parity-gap: WriteOnly-state temp index writes +
// tidb_constraint_check_in_place interplay are not modelled in this tier.
#[test]
#[ignore]
fn add_index_merge_insert_to_deleted_temp_index() {
}

/// Go `TestAddIndexMergeReplaceDelete`
/// (`pkg/ddl/tests/indexmerge/merge_test.go:650`): DeleteOnly inserts
/// `(1,1)`; `skipReorgWorkForTempIndex` is forced false so the merge phase
/// runs, and `MockDMLExecutionMerging` (the `mockDMLExecutionMerging`
/// failpoint, `pkg/ddl/index.go:3855`) replaces `(2,1)` then deletes id=2
/// during Merging. ADD UNIQUE INDEX idx(a) ends with ZERO rows
/// (`select * from t` is empty); `admin check table t` passes.
// go-parity-gap: mockDMLExecutionMerging (pkg/ddl/index.go:3855) and the
// merge phase itself have no Rust carrier.
#[test]
#[ignore]
fn add_index_merge_replace_delete() {
}

/// Go `TestAddIndexMergeDeleteDifferentHandle`
/// (`pkg/ddl/tests/indexmerge/merge_test.go:686`): row `(1,'a')`; during
/// WriteReorganization tk1 inserts `(2,'a')` and replaces `(3,'a')`, while
/// `MockDMLExecution` deletes id=1 "too late" -- the duplicate index value
/// 'a' (held by ids 2 and 3) can no longer be removed by the backfill.
/// ADD UNIQUE INDEX idx(c) must fail with `ErrDupEntry` 1062
/// (`pkg/errno/errcode.go:83`), and the surviving table is exactly `3 a`
/// (replace removed id=2); `admin check table t` passes.
// go-parity-gap: no backfill duplicate-error carrier (the Rust
// run_create_index_in is synchronous and holds no concurrent writers).
#[test]
#[ignore]
fn add_index_merge_delete_different_handle() {
}

/// Go `TestAddIndexDecodeTempIndexCommonHandle`
/// (`pkg/ddl/tests/indexmerge/merge_test.go:727`):
/// `t(id_a bigint, id_b char(20), c char(20), primary key (id_a, id_b))`
/// -- a CLUSTERED common handle spanning two columns of mixed types. Rows
/// `(2,'id_2','char_2')` and `(3,'id_3','char_3')` written during
/// WriteReorganization land in the temp index keyed by the multi-column
/// common handle; ADD UNIQUE INDEX idx(c) must decode them back to the
/// right handles, ending with all three rows
/// `1 id_1 char_1 / 2 id_2 char_2 / 3 id_3 char_3`; `admin check table t`
/// passes.
// go-parity-gap: temp-index common-handle decoding has no Rust carrier
// (src/ddl/indexes.rs backfills synchronously from row data only).
#[test]
#[ignore]
fn add_index_decode_temp_index_common_handle() {
}

/// Go `TestAddIndexInsertIgnoreOnBackfill`
/// (`pkg/ddl/tests/indexmerge/merge_test.go:761`): empty
/// `t(id int primary key, b int)`; during WriteReorganization tk1 runs
/// `insert ignore (1,1)`, `insert ignore (2,2)`, then
/// `update t set b = null where id = 1`. ADD UNIQUE INDEX idx(b) must
/// tolerate the NULL unique value left by the update (the temp entry for
/// b=1 of id=1 is deleted by the same statement) and finish with exactly
/// `1 <nil>` and `2 2`; `admin check table t` passes.
// go-parity-gap: concurrent insert-ignore during a reorganization window is
// unmodelable without the job state machine.
#[test]
#[ignore]
fn add_index_insert_ignore_on_backfill() {
}

/// Go `TestAddIndexMultipleDelete`
/// (`pkg/ddl/tests/indexmerge/merge_test.go:793`): rows id=1..6 all with
/// b=1; DeleteOnly deletes ids 4,5,6; WriteOnly deletes ids 2,3;
/// `MockDMLExecution` deletes id=1. Every one of the six temp-index entries
/// for b=1 ends up deleted-marked; ADD UNIQUE INDEX idx(b) must finish with
/// an EMPTY table (`select * from t` has no rows) instead of reporting a
/// duplicate; `admin check table t` passes.
// go-parity-gap: same missing temp-index merge carrier.
#[test]
#[ignore]
fn add_index_multiple_delete() {
}

/// Go `TestAddIndexDuplicateAndWriteConflict`
/// (`pkg/ddl/tests/indexmerge/merge_test.go:829`): row `(1,1)`; in the
/// WriteOnly state (observed by `afterRunOneJobStep`,
/// `pkg/ddl/job_worker.go:652`) tk1 inserts the conflicting `(2,1)`. The
/// duplicate makes the job roll back, the hook then cancels it via
/// `admin cancel ddl jobs <id>`, and the ALTER statement must answer
/// `ErrCancelledDDLJob` 8214 (`pkg/errno/errcode.go:1116`). The table keeps
/// both rows `1 1` and `2 1`; `admin check table t` passes.
// go-parity-gap: no job lifecycle (rollback + admin cancel) carrier in this
// tier; the Rust CREATE INDEX answers 1062 directly instead of 8214.
#[test]
#[ignore]
fn add_index_duplicate_and_write_conflict() {
}

/// Go `TestAddIndexUpdateUntouchedValues`
/// (`pkg/ddl/tests/indexmerge/merge_test.go:862`): row `(1,1,1)` in
/// `t(id int primary key, b int, k int)`; during WriteReorganization a txn
/// updates the UNTOUCHED column `k=k+1` (idx(b) does not cover k) and then
/// inserts `(2,1,2)`, which must NOT be refused with Go's "invalid temp
/// index value" (the untouched update rewrites the row's temp-index entry
/// unchanged). The ALTER itself answers `ErrDupEntry` 1062 because b=1 now
/// repeats; the final table is `1 1 2` and `2 1 2`; `admin check table t`
/// passes.
// go-parity-gap: no temp-index "invalid temp index value" check or
// concurrent-txn carrier (pkg/ddl/index.go temp index decode path).
#[test]
#[ignore]
fn add_index_update_untouched_values() {
}

/// Go `TestAddUniqueIndexFalsePositiveDuplicate`
/// (`pkg/ddl/tests/indexmerge/merge_test.go:898`): `t(a bigint, b
/// varchar(221) not null default 'dup', unique key exist_idx(b), primary
/// key (a))` holds `(1,'1')` and `(2,'dup')`; `MockDMLExecution` replaces
/// `(3,'dup')` during the ADD UNIQUE INDEX idx(b) backfill. The duplicate
/// b='dup' the backfill observes must be recognized as the row's OWN
/// already-committed entry (false positive through exist_idx) rather than a
/// conflict: the ALTER SUCCEEDS and `admin check table t` passes.
// go-parity-gap: the false-positive duplicate resolution runs inside Go's
// temp-index merge; no Rust carrier.
#[test]
#[ignore]
fn add_unique_index_false_positive_duplicate() {
}

/// Go `TestAddIndexSkipReorgCheck`
/// (`pkg/ddl/tests/indexmerge/merge_test.go:921`): the ADD INDEX job may
/// skip the whole backfill when the table is empty
/// (`afterCheckTableReorgCanSkip` fires, `pkg/ddl/index.go:1903`) and skip
/// the temp-index reorg when no temp entries exist
/// (`afterCheckTempIndexReorgCanSkip`, `pkg/ddl/index.go:1934`). The Go
/// test pins all three outcomes: empty table -> BOTH hooks fire; table with
/// one pre-existing row -> table reorg NOT skipped but temp-index reorg
/// still skipped; a row inserted DURING WriteReorganization -> neither
/// skipped, and the result serves rows `1` and `2`.
// go-parity-gap: the skip decision lives in Go's reorg loop
// (pkg/ddl/index.go:1894-1936); the Rust tier has no reorg loop to skip.
#[test]
#[ignore]
fn add_index_skip_reorg_check() {
}

/// Go `TestAddIndexInsertAfterReorgSkipCheck`
/// (`pkg/ddl/tests/indexmerge/merge_test.go:968`): a row inserted from
/// INSIDE the `afterCheckTableReorgCanSkip` hook must still appear after
/// the ADD INDEX finishes (`select * from t` is `1`), and after TRUNCATE
/// the same must hold for a row inserted inside
/// `afterCheckTempIndexReorgCanSkip` during ADD INDEX idx2 (result `2`):
/// skipping the reorg check must not lose rows that race the skip window.
// go-parity-gap: same missing reorg-skip hooks
// (pkg/ddl/index.go:1903,1934).
#[test]
#[ignore]
fn add_index_insert_after_reorg_skip_check() {
}
