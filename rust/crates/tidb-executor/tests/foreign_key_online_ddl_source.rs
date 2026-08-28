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

//! Port ledger for the online-DDL halves of `pkg/ddl/foreign_key_test.go`
//! (`pkg/ddl.part6` batch b105; this file carries that Go file's items 323,
//! 324, 326, 327, 328 and 329 of the pkg/ddl enumeration -- item 325's
//! guard core and item 330 are functionally ported in their own files).
//!
//! Six of the file's eight Go tests assert behavior DURING a DDL job's
//! life -- cross-session interleavings through the `beforeRunOneJobStep`
//! failpoint, mid-schema-state visibility, or job-history checks through
//! `ExecutorForTest` helpers (`testCreateForeignKey`, `testCheckJobDone`).
//! The DDL job queue and the online schema states are not transcreated, so
//! those tests have no carrier here. Where the END-STATE rule a test
//! converges to IS transcreated, it is pinned elsewhere and named below.

/// GO PORT of `pkg/ddl/foreign_key_test.go:110 TestForeignKey`.
///
/// Re-derived contract: `testCreateForeignKey` (foreign_key_test.go:37-71)
/// submits an `ActionAddForeignKey` job for `c1_fk` (child `t.c1` ->
/// `t2.c1`, OnDelete=Cascade, OnUpdate=SetNull) over a table that already
/// carries `idx_fk (c1)`; after the job finishes `getForeignKey(t,
/// "c1_fk")` (foreign_key_test.go:99-113) finds the constraint in StatePublic
/// on the reloaded table; `testDropForeignKey` then removes it and the
/// second hook observes it gone; `testCheckJobDone`/`checkJobWithHistory`
/// verify the finished jobs' history rows.
#[test]
#[ignore = "go-parity-gap: needs ExecutorForTest job submission, afterWaitSchemaSynced hooks and job history -- the DDL job machinery is not transcreated"]
fn foreign_key_add_and_drop_round_trip_through_job_history() {}

/// GO PORT of `pkg/ddl/foreign_key_test.go:209
/// TestTruncateOrDropTableWithForeignKeyReferred2`.
///
/// Re-derived contract: a `TRUNCATE`/`DROP` of `t1` racing a queued create of
/// `t2` that declares `foreign key fk(b) references t1(id)` ends with
/// `[ddl:1701]Cannot truncate a table referenced in a foreign key constraint
/// (`test`.`t2` CONSTRAINT `fk`)` -- Go's
/// `checkTruncateTableWithForeignKeys` refuses to truncate a table another
/// table's stored constraint still refers to.
#[test]
#[ignore = "go-parity-gap: run_truncate_table_in has no 1701 referenced-table guard (Go checkTruncateTableWithForeignKeys), and the test's cross-session race needs the job queue"]
fn truncate_of_a_referenced_table_is_1701() {}

/// GO PORT of `pkg/ddl/foreign_key_test.go:295
/// TestDropDatabaseWithForeignKeyReferred2`.
///
/// Re-derived contract: with `test2.t3` referencing `test.t2` across
/// schemas, `drop database test` must refuse with
/// `[ddl:3730]Cannot drop table 't2' referenced by a foreign key constraint
/// 'fk_b' on table 't3'.`, and only after `t3` is dropped does the database
/// drop succeed. The cross-schema referred-FK guard on DROP DATABASE and the
/// failpoint interleave are not transcreated.
#[test]
#[ignore = "go-parity-gap: the 3730 cross-schema referred-FK guard on DROP DATABASE (Go checkDropDatabaseWithForeignKey) is not transcreated, and the race needs the job queue"]
fn drop_database_with_a_referred_foreign_key_is_3730() {}

/// GO PORT of `pkg/ddl/foreign_key_test.go:334 TestAddForeignKey2`.
///
/// Re-derived contract: while `alter table t2 drop index b` is in flight, a
/// queued `alter table t2 add foreign key (b) references t1(id)` fails with
/// `[ddl:-1]Failed to add the foreign key constraint. Missing index for
/// 'fk_1' foreign key columns in the table 't2'` -- with the covering index
/// gone the constraint cannot be added.
#[test]
#[ignore = "go-parity-gap: ADD FOREIGN KEY's missing-index refusal runs through the job queue in the Go test; this tier's ADD FOREIGN KEY path carries no missing-covering-index error yet"]
fn add_foreign_key_without_a_covering_index_fails() {}

/// GO PORT of `pkg/ddl/foreign_key_test.go:365 TestAddForeignKey3`.
///
/// Re-derived contract: during `alter table t2 add foreign key (id)
/// references t1(id) on delete cascade`, rows written while the constraint
/// is in StateWriteOnly/StateWriteReorganization are ALREADY checked --
/// inserting `(10,10)` into `t2` fails 1452 and deleting `t1` id=1 fails
/// 1451, once per state, both naming ``test`.`t2` ... `fk_1` ... ON DELETE
/// CASCADE`` -- and the final data is unchanged.
///
/// The END-state halves of this contract (a violating child insert is 1452,
/// a referenced parent delete is 1451, with the constraint's full text in
/// the message) are transcreated and pinned by
/// `crates/tidb-session/src/tests_foreign_key.rs`
/// (`a_child_row_must_reference_an_existing_parent_row`,
/// `a_referenced_parent_row_cannot_be_deleted_without_an_action`); the
/// mid-DDL-state reach is the gap.
#[test]
#[ignore = "go-parity-gap: the WriteOnly/WriteReorganization mid-DDL states that make the constraint bite before it is public are not transcreated"]
fn add_foreign_key_checks_rows_in_write_only_and_reorg_states() {}

/// GO PORT of `pkg/ddl/foreign_key_test.go:407
/// TestForeignKeyInWriteOnlyMode`.
///
/// Re-derived contract: while `create table child ... foreign key (pid)
/// references parent(id) on delete cascade` sits in StateDeleteOnly, the
/// table is INVISIBLE to a normal session -- insert/update/delete/delete-join
/// against `test.child` all fail with "Table 'test.child' doesn't exist" --
/// and the create then completes normally.
#[test]
#[ignore = "go-parity-gap: the StateDeleteOnly invisibility of an in-flight table belongs to the online schema states, which are not transcreated"]
fn foreign_key_child_table_is_invisible_in_delete_only_state() {}
