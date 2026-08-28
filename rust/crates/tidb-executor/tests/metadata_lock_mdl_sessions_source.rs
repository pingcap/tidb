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

//! Ports of Go `pkg/ddl/tests/metadatalock/mdl_test.go::TestMDLBasicSelect`
//! through `::TestMDLUpdateEtcdFail` (items 761-798 of the pkg/ddl `Test*`
//! enumeration) and the package's `main_test.go::TestMain`. Every MDL test
//! drives at least two concurrent sessions against a mock SERVER
//! (`server.CreateMockServer` + `InfoSyncer().SetSessionManager`,
//! mdl_test.go:37-40), opens a transaction in one session, runs DDL in the
//! other, and pins WHO WAITED for WHOM with wall-clock timestamps taken
//! around the `commit`/DDL-completion points.
//!
//! The Rust `tidb-executor` tier owns a single-owner synchronous `Catalog`
//! (`src/driver/catalog.rs`): there is no server, no session manager, no
//! transaction snapshot and no DDL job queue, so an metadata lock cannot be
//! taken, held or waited on. Each test below records the gap with the
//! contract re-derived from the Go source and cited. Nothing is
//! approximated. The static sysvar definition itself
//! (`tidb_enable_metadata_lock`, global scope, default ON) is pinned by
//! `tidb-session`'s catalog (`src/sysvar/catalog/ddl_schema.rs:291-299`,
//! against `pkg/sessionctx/variable/sysvar.go:1747`); this crate cannot
//! reach it (no tidb-session dependency), so `TestSwitchMDL` is a gap here.

/// Go `TestMDLBasicSelect` (`pkg/ddl/tests/metadatalock/mdl_test.go:36`):
/// session 1 does `begin; select * from t` (the read registers the table's
/// MDL), session 2 then runs `alter table test.t add column b int`, which
/// must BLOCK. After 2s the txn is still open; `ts1` (commit time) is
/// strictly BEFORE `ts2` (ALTER completion): the DDL finishes only after
/// the commit released the lock.
// go-parity-gap: needs a mock server + two sessions + a blocking DDL wait
// queue; the tier has a single synchronous Catalog and no txn layer.
#[test]
#[ignore]
fn mdl_basic_select() {
}

/// Go `TestMDLBasicInsert` (`pkg/ddl/tests/metadatalock/mdl_test.go:73`):
/// same contract as `mdl_basic_select` with `insert into t values (2)` as
/// the locking statement.
// go-parity-gap: same missing multi-session MDL carrier.
#[test]
#[ignore]
fn mdl_basic_insert() {
}

/// Go `TestMDLBasicUpdate` (`pkg/ddl/tests/metadatalock/mdl_test.go:110`):
/// same contract with `update t set a = 2` as the locking statement.
// go-parity-gap: same missing multi-session MDL carrier.
#[test]
#[ignore]
fn mdl_basic_update() {
}

/// Go `TestMDLBasicDelete` (`pkg/ddl/tests/metadatalock/mdl_test.go:147`):
/// same contract with `delete from t` as the locking statement.
// go-parity-gap: same missing multi-session MDL carrier.
#[test]
#[ignore]
fn mdl_basic_delete() {
}

/// Go `TestMDLBasicPointGet` (`pkg/ddl/tests/metadatalock/mdl_test.go:184`):
/// same contract with a point-get read (`select * from t where a = 1` over
/// a unique key): even a single-row point read registers the MDL that
/// blocks the ALTER.
// go-parity-gap: same missing multi-session MDL carrier.
#[test]
#[ignore]
fn mdl_basic_point_get() {
}

/// Go `TestMDLBasicBatchPointGet`
/// (`pkg/ddl/tests/metadatalock/mdl_test.go:221`): same contract with a
/// batch point get (`select * from t where a in (12, 22)` -- a read that
/// matches NO rows) still registers the MDL and blocks the ALTER.
// go-parity-gap: same missing multi-session MDL carrier.
#[test]
#[ignore]
fn mdl_basic_batch_point_get() {
}

/// Go `TestMDLAddForeignKey` (`pkg/ddl/tests/metadatalock/mdl_test.go:259`):
/// session 1 begins and inserts into t2; session 2's
/// `alter table test.t2 add foreign key (id) references t1(id)` blocks on
/// the MDL until the commit, and only THEN fails with EXACTLY
/// "[ddl:1452]Cannot add or update a child row: a foreign key constraint
/// fails (`test`.`t2`, CONSTRAINT `fk_1` FOREIGN KEY (`id`) REFERENCES
/// `t1` (`id`))" -- the generated constraint name `fk_1` included; ts1 < ts2.
// go-parity-gap: MDL blocking + the FK self-check error carrier both live
// outside this tier.
#[test]
#[ignore]
fn mdl_add_foreign_key() {
}

/// Go `TestMDLRRUpdateSchema`
/// (`pkg/ddl/tests/metadatalock/mdl_test.go:299`): a REPEATABLE-READ txn
/// interacting with four DDLs, each in its own txn: after `alter ... add
/// column b int` commits, the open txn keeps serving the OLD row `1` until
/// commit, and only the NEXT txn sees `1 <nil>`; after `alter ... add index
/// idx(a)` commits, reading `use index(idx)` INSIDE the old txn answers
/// ErrKeyDoesNotExist 1176 (`pkg/errno/errcode.go:197`) while the plain
/// select still works; a reorg `modify column a char(10)` makes mid-txn
/// selects answer ErrInfoSchemaChanged 8028 (`pkg/errno/errcode.go:982`);
/// a non-reorg `modify column a char(20)` is served by the old schema with
/// no error; after each commit the new schema serves.
// go-parity-gap: no per-txn infoschema snapshot + statement-refresh
// machinery in this tier.
#[test]
#[ignore]
fn mdl_rr_update_schema() {
}

/// Go `TestMDLRCUpdateSchema`
/// (`pkg/ddl/tests/metadatalock/mdl_test.go:346`): the READ-COMMITTED
/// variant: with `set @@transaction_isolation='READ-COMMITTED'`, each new
/// statement refreshes to the latest schema, so after the `add index
/// idx(a)` DDL commits, `select * from t use index(idx)` INSIDE the open
/// txn SUCCEEDS with rows `1 <nil>` (no 1176), and even the reorg `modify
/// column a char(10)` is served without 8028 -- the isolation-level
/// counterpart of `mdl_rr_update_schema`.
// go-parity-gap: no isolation-level-dependent schema refresh carrier.
#[test]
#[ignore]
fn mdl_rc_update_schema() {
}

/// Go `TestMDLAutoCommitReadOnly`
/// (`pkg/ddl/tests/metadatalock/mdl_test.go:394`): an autocommit
/// `select sleep(2) from t` does NOT block the concurrent ALTER: ts1 (after
/// the select) is strictly AFTER ts2 (after the DDL) -- the DDL finishes
/// first. Autocommit read-only statements take no blocking MDL.
// go-parity-gap: no autocommit-statement MDL accounting carrier.
#[test]
#[ignore]
fn mdl_auto_commit_read_only() {
}

/// Go `TestMDLAnalyze` (`pkg/ddl/tests/metadatalock/mdl_test.go:431`):
/// `begin; analyze table t; select sleep(2); commit` running concurrently
/// with `alter table test.t add column b int`: ts1 (after commit) is AFTER
/// ts2 (after the DDL) -- ANALYZE, though run inside an explicit txn, does
/// not hold an MDL that blocks the ALTER.
// go-parity-gap: no ANALYZE-in-txn MDL exemption carrier.
#[test]
#[ignore]
fn mdl_analyze() {
}

/// Go `TestMDLAnalyzePartition`
/// (`pkg/ddl/tests/metadatalock/mdl_test.go:471`): same contract on a
/// RANGE-partitioned table under `tidb_partition_prune_mode='dynamic'`:
/// `analyze table t` + `analyze table t partition p1` in a txn do not
/// block `alter table test.t drop partition p2` (ts1 > ts2).
// go-parity-gap: same missing ANALYZE MDL exemption carrier, plus
// dynamic-prune partition ANALYZE.
#[test]
#[ignore]
fn mdl_analyze_partition() {
}

/// Go `TestMDLAutoCommitNonReadOnly`
/// (`pkg/ddl/tests/metadatalock/mdl_test.go:513`): the counterpart of
/// `mdl_auto_commit_read_only`: an autocommit WRITE
/// (`insert into t select sleep(2) from t`) DOES block the concurrent
/// ALTER: ts1 < ts2 -- the DDL waits for the writing statement.
// go-parity-gap: no autocommit-statement MDL accounting carrier.
#[test]
#[ignore]
fn mdl_auto_commit_non_read_only() {
}

/// Go `TestMDLLocalTemporaryTable`
/// (`pkg/ddl/tests/metadatalock/mdl_test.go:550`): a `create temporary
/// table t(a int)` shadows the base table inside session 1; a txn writing
/// the TEMP table (`insert into t values (2)`) must NOT block
/// `alter table test.t add column b int` on the BASE table (ts1 > ts2 --
/// the ALTER completes during session 1's 2s sleep), and the txn's own
/// reads keep seeing only the temp rows `1`, `2`.
// go-parity-gap: no local-temporary-table session layer and no MDL
// exemption for it.
#[test]
#[ignore]
fn mdl_local_temporary_table() {
}

/// Go `TestMDLGlobalTemporaryTable`
/// (`pkg/ddl/tests/metadatalock/mdl_test.go:594`): a txn inserting into a
/// `create global temporary table ... on commit delete rows` table must
/// NOT block `alter ... add column b int` (ts1 > ts2); after commit (and
/// the implicit row wipe), the NEXT txn sees the new shape: inserting
/// `(2, null, null)` and reading back `2 <nil> <nil>` after a further
/// `add column c int` commits.
// go-parity-gap: no global-temporary-table carrier and no MDL exemption.
#[test]
#[ignore]
fn mdl_global_temporary_table() {
}

/// Go `TestMDLCacheTable` (`pkg/ddl/tests/metadatalock/mdl_test.go:640`):
/// after `alter table t cache`, a txn that READ the cached table (twice)
/// DOES block `alter table test.t nocache`: ts1 (commit) < ts2 (DDL done).
// go-parity-gap: cache-table state + its MDL interaction are not modelled
// (the tier only refuses DDL on cached tables: src/ddl/indexes.rs:84).
#[test]
#[ignore]
fn mdl_cache_table() {
}

/// Go `TestMDLStaleRead` (`pkg/ddl/tests/metadatalock/mdl_test.go:682`):
/// `start transaction read only as of timestamp NOW() - INTERVAL 1 SECOND`
/// takes NO metadata lock: the concurrent `alter ... add column b int`
/// completes immediately (no goroutine/timing needed), and the stale-read
/// txn keeps answering the old single-column row `1` until commit.
// go-parity-gap: no stale-read txn carrier in this tier.
#[test]
#[ignore]
fn mdl_stale_read() {
}

/// Go `TestMDLTiDBSnapshot` (`pkg/ddl/tests/metadatalock/mdl_test.go:709`):
/// a txn reading under `set @@tidb_snapshot = NOW() - INTERVAL 1 SECOND`
/// (with the mock store's GC safe point pre-seeded into mysql.tidb) takes
/// no MDL: the concurrent ALTER finishes first (ts1 > ts2), and the
/// snapshot reads still return `1`.
// go-parity-gap: no tidb_snapshot carrier and no GC safe-point bootstrap.
#[test]
#[ignore]
fn mdl_tidb_snapshot() {
}

/// Go `TestMDLPartitionTable`
/// (`pkg/ddl/tests/metadatalock/mdl_test.go:760`): a txn reading a
/// HASH-partitioned table (`partition by hash(a) partitions 10`) registers
/// the MDL and blocks `alter table test.t add column b int`: ts1 < ts2.
// go-parity-gap: same missing multi-session MDL carrier (partitioned
// variant).
#[test]
#[ignore]
fn mdl_partition_table() {
}

/// Go `TestMDLPreparePlanBlockDDL`
/// (`pkg/ddl/tests/metadatalock/mdl_test.go:797`): `prepare stmt from
/// 'select * from t where a >= ?'`; inside a txn the first `execute using
/// @a` returns `1 2 3 4` and blocks the concurrent ALTER (ts1 < ts2 --
/// commit happens first); a `prepare` of the SAME name issued WHILE the
/// DDL is blocked is allowed, and after commit the prepared statement
/// serves the NEW shape `1 <nil> / 2 <nil> / 3 <nil> / 4 <nil>`.
// go-parity-gap: no prepared-statement + plan-cache + MDL carrier.
#[test]
#[ignore]
fn mdl_prepare_plan_block_ddl() {
}

/// Go `TestMDLPreparePlanCacheInvalid`
/// (`pkg/ddl/tests/metadatalock/mdl_test.go:841`): a prepare issued while
/// the ALTER add-column is BLOCKED behind the open txn produces a plan
/// that must NOT survive the commit: after commit, `execute stmt_test_1
/// using @a` serves the new shape `1 <nil> / ... / 4 <nil>` (the plan was
/// invalidated by the schema change).
// go-parity-gap: no prepared-plan invalidation carrier.
#[test]
#[ignore]
fn mdl_prepare_plan_cache_invalid() {
}

/// Go `TestMDLPreparePlanCacheExecute`
/// (`pkg/ddl/tests/metadatalock/mdl_test.go:883`): a cached UPDATE plan
/// (`update t set a = ? where a = ?`) executed inside a fresh txn:
/// the first execute cannot reuse the pre-txn plan (`@@last_plan_from_cache`
/// = 0), the second cannot either because the table turned dirty (0), the
/// third CAN (1) -- and THAT cached execution registers the MDL that blocks
/// `alter table test.t add index idx(a)` until the commit; the DDL
/// goroutine only proceeds after the channel releases it; `admin check
/// table t` passes at the end.
// go-parity-gap: no plan cache, dirty-table tracking or MDL carrier.
#[test]
#[ignore]
fn mdl_prepare_plan_cache_execute() {
}

/// Go `TestMDLPreparePlanCacheExecute2`
/// (`pkg/ddl/tests/metadatalock/mdl_test.go:938`): with a cached SELECT
/// plan and an open txn on t2, the concurrent `alter table test.t add
/// index idx(a)` is NOT blocked (wg.Wait returns before the execute): the
/// in-txn `execute stmt_test_1 using @a` then answers
/// `@@last_plan_from_cache` = 0 because the schema changed under the txn.
// go-parity-gap: no plan-cache schema-invalidation carrier.
#[test]
#[ignore]
fn mdl_prepare_plan_cache_execute_2() {
}

/// Go `TestMDLPreparePlanCacheExecuteInsert`
/// (`pkg/ddl/tests/metadatalock/mdl_test.go:983`): an INSERT prepared plan
/// interacting with the temp-index merge phase: when the ADD INDEX job
/// reaches WriteReorganization the hook runs `begin; delete a=4; execute
/// insert_stmt` (plan NOT from cache, 0) and commits, then in
/// `MockDMLExecutionMerging` the execute must be RE-PLANNED (0 again)
/// because the schema changed since the last plan; `admin check table t`
/// passes.
// go-parity-gap: needs plan cache + merge-phase hook
// (pkg/ddl/index.go:3855) + MDL, none carried here.
#[test]
#[ignore]
fn mdl_prepare_plan_cache_execute_insert() {
}

/// Go `TestMDLDisable2Enable`
/// (`pkg/ddl/tests/metadatalock/mdl_test.go:1058`, classic kernel only --
/// skips itself under next-gen, :1059-1061): two txns open while
/// `tidb_enable_metadata_lock` is OFF; the DDL session turns it ON and runs
/// `alter ... add index idx(a)`. The txn that started WITHOUT MDL gets NO
/// lock registration and its `commit` answers ErrInfoSchemaChanged 8028
/// (`pkg/errno/errcode.go:982`); the other txn (which read the table under
/// the NEW setting) commits fine.
// go-parity-gap: no global MDL toggle runtime, no commit-time
// schema-version check carrier.
#[test]
#[ignore]
fn mdl_disable_2_enable() {
}

/// Go `TestMDLEnable2Disable`
/// (`pkg/ddl/tests/metadatalock/mdl_test.go:1101`, classic only): the
/// mirror of `mdl_disable_2_enable`: txns open under MDL ON; the DDL
/// session turns it OFF before the ALTER; the first txn's commit still
/// answers 8028 (its lock was registered but the disabled gate ignores it),
/// the second commits.
// go-parity-gap: same missing MDL toggle runtime.
#[test]
#[ignore]
fn mdl_enable_2_disable() {
}

/// Go `TestSwitchMDL` (`pkg/ddl/tests/metadatalock/mdl_test.go:1144`):
/// `set global tidb_enable_metadata_lock = 0` then `1` -- each followed by
/// `show global variables like 'tidb_enable_metadata_lock'` answering OFF
/// and ON. The sysvar EXISTS in the workspace with global scope and
/// default ON (`tidb-session/src/sysvar/catalog/ddl_schema.rs:291-299`,
/// mirroring `pkg/sessionctx/variable/sysvar.go:1747` and its SetGlobal ->
/// `SwitchMDL` hook), but the read/write runtime lives in tidb-session,
/// which tidb-executor does not depend on.
// go-parity-gap: no set/show-global-variables runtime inside this crate;
// the static definition is pinned out-of-gate by tidb-session's catalog.
#[test]
#[ignore]
fn switch_mdl() {
}

/// Go `TestMDLViewItself` (`pkg/ddl/tests/metadatalock/mdl_test.go:1181`):
/// a txn reading `select * from v` registers the MDL for the VIEW itself
/// and blocks `drop view test.v`: ts1 < ts2.
// go-parity-gap: no view catalog + MDL carrier in this tier.
#[test]
#[ignore]
fn mdl_view_itself() {
}

/// Go `TestMDLViewBaseTable`
/// (`pkg/ddl/tests/metadatalock/mdl_test.go:1219`): a txn reading a view
/// also registers the MDL on the view's BASE table: the concurrent
/// `alter table test.t add column b int` blocks until commit (ts1 < ts2).
// go-parity-gap: no view-to-base-table MDL propagation carrier.
#[test]
#[ignore]
fn mdl_view_base_table() {
}

/// Go `TestMDLSavePoint` (`pkg/ddl/tests/metadatalock/mdl_test.go:1257`):
/// `savepoint s1` / `rollback to s1` inside a READING txn does not release
/// the MDL -- the concurrent ALTER add-column still blocks until commit
/// (ts1 < ts2), and the table then reads `1`. Second phase: a fresh txn
/// that has only done `begin; savepoint s2` (NO read yet, hence no MDL)
/// does not block the concurrent `add column b int`; the txn's next select
/// serves the NEW shape `1 <nil>`, `rollback to s2` keeps `1 <nil>`, and
/// commit persists `1 <nil>` -- a savepoint rollback never undoes another
/// session's schema change.
// go-parity-gap: no savepoint or MDL runtime in this tier.
#[test]
#[ignore]
fn mdl_savepoint() {
}

/// Go `TestMDLTableCreate` (`pkg/ddl/tests/metadatalock/mdl_test.go:1308`):
/// inside an open txn, `select * from t1` fails ErrNoSuchTable 1146
/// (`pkg/errno/errcode.go:167`) BOTH before and after another session's
/// `create table test.t1(a int)` commits -- an open txn never gains sight
/// of objects created after it started, and creating a NEW table takes no
/// MDL from unrelated txns.
// go-parity-gap: no multi-session snapshot-isolated schema visibility.
#[test]
#[ignore]
fn mdl_table_create() {
}

/// Go `TestMDLTableDrop` (`pkg/ddl/tests/metadatalock/mdl_test.go:1329`):
/// a txn that only did `begin` (no read, hence NO MDL on t) does not block
/// another session's `drop table test.t`; the txn's subsequent
/// `select * from t` answers ErrNoSuchTable 1146.
// go-parity-gap: same missing multi-session schema visibility carrier.
#[test]
#[ignore]
fn mdl_table_drop() {
}

/// Go `TestMDLDatabaseCreate`
/// (`pkg/ddl/tests/metadatalock/mdl_test.go:1348`): inside the open txn,
/// after another session creates database test2 and its table t:
/// `use test2` answers ErrBadDB 1049 (`pkg/errno/errcode.go:70`) and
/// `select * from test2.t` answers 1146 -- new databases are invisible to
/// the stale txn.
// go-parity-gap: same missing multi-session schema visibility carrier.
#[test]
#[ignore]
fn mdl_database_create() {
}

/// Go `TestMDLDatabaseDrop`
/// (`pkg/ddl/tests/metadatalock/mdl_test.go:1367`): after another session
/// drops database test mid-txn, `use test` succeeds (re-selecting an
/// existing name resolves fresh) but `select * from t` answers 1146.
// go-parity-gap: same missing multi-session schema visibility carrier.
#[test]
#[ignore]
fn mdl_database_drop() {
}

/// Go `TestMDLRenameTable`
/// (`pkg/ddl/tests/metadatalock/mdl_test.go:1387`): inside the open txn,
/// after another session renames test.t -> test.t1 (then test.t1 ->
/// test2.t1), BOTH the old name (`select * from t`) and the new name
/// (`select * from t1`, `select * from test2.t1`) answer ErrNoSuchTable
/// 1146 -- a rename is invisible in both directions to the stale txn.
// go-parity-gap: same missing multi-session schema visibility carrier.
#[test]
#[ignore]
fn mdl_rename_table() {
}

/// Go `TestMDLPrepareFail`
/// (`pkg/ddl/tests/metadatalock/mdl_test.go:1415`): preparing
/// `select b from t` against a table with only column `a` fails at
/// PREPARE time (unknown column), and the later concurrent
/// `alter table test.t add column c int` of another session proceeds
/// unaffected -- the prepare-time schema check is independent of in-flight
/// DDL.
// go-parity-gap: no PrepareStmt carrier in this tier.
#[test]
#[ignore]
fn mdl_prepare_fail() {
}

/// Go `TestMDLUpdateEtcdFail`
/// (`pkg/ddl/tests/metadatalock/mdl_test.go:1428`): with failpoint
/// `mockUpdateMDLToETCDError` set to `3*return(true)` -- injected in
/// `MemSyncer.UpdateSelfVersion`, `pkg/ddl/schemaver/mem_syncer.go:67` --
/// the first three schema-version publications fail with "mock update mdl
/// to etcd error"; the ALTER `add column c int` must STILL succeed
/// (the publication retries survive the mocked failures).
// go-parity-gap: no schema-version syncer (etcd or mem) carrier in this
// tier.
#[test]
#[ignore]
fn mdl_update_etcd_fail() {
}
