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

//! Ports of Go `pkg/ddl/tiflash_replica_test.go` (part-17 items
//! `TestSetTableFlashReplica` :54 through `TestTruncateTable2` :477-576).
//!
//! Go drives every test through a mock store plus the
//! `infoschema/mockTiFlashStoreCount` failpoint (`return(true)`, spoofing two
//! TiFlash stores) and, from :193 on, a real gRPC status server and
//! `infosync.NewMockTiFlash`. This tier has none of that machinery: no
//! failpoints, no TiFlash store-count check, no replica poller, no
//! `ALTER TABLE ... SET TIFLASH REPLICA` carrier (the measured answer for
//! every shape is the generic `1105 this ALTER TABLE action is not supported
//! yet`, the catch-all arm of `ddl/alter_table.rs:359`), no
//! `DDLExecutor.UpdateTableReplicaInfo`, and the live table model
//! (`crate::kv_table::KvTable`) carries no TiFlash replica metadata at all.
//! The TiFlash-dependent contracts are therefore `#[ignore]`d gap tests with
//! their Go sources re-derived; the one carrier-independent slice —
//! `TestTruncateTable2`'s rows contract — runs. Nothing is approximated.

use tidb_datatype::Datum;
use tidb_executor::{
    run_create_table_on, run_insert_on, run_select_on, run_truncate_table_in, Catalog,
    StmtContext,
};

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

fn int_rows(catalog: &Catalog, sql: &str) -> Vec<Vec<String>> {
    run_select_on(sql, catalog, &ctx())
        .expect("select succeeds")
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|datum| match &datum {
                    Datum::Int(value) => value.to_string(),
                    other => panic!("unexpected datum {other:?}"),
                })
                .collect()
        })
        .collect()
}

/// Go `tiflash_replica_test.go:487-501::TestTruncateTable2`, rows contract:
/// after inserting (1,1),(2,2) and truncating, `insert (3,3),(4,4)` and
/// `select *` answer exactly `3 3` / `4 4` — the truncate emptied the rows
/// and the table stays live for writes.
#[test]
fn truncate_table_empties_the_rows_and_the_table_stays_live() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table truncate_table (c1 int, c2 int)", &mut catalog)
        .expect("create succeeds");
    run_insert_on(
        "insert into truncate_table values (1, 1), (2, 2)",
        &mut catalog,
        &ctx(),
    )
    .expect("insert succeeds");

    run_truncate_table_in(
        "truncate table truncate_table",
        &mut catalog,
        "test",
        ctx().sql_mode(),
    )
    .expect("truncate succeeds");

    run_insert_on(
        "insert into truncate_table values (3, 3), (4, 4)",
        &mut catalog,
        &ctx(),
    )
    .expect("post-truncate insert succeeds");
    assert_eq!(
        int_rows(&catalog, "select * from truncate_table"),
        vec![vec!["3", "3"], vec!["4", "4"]],
        "Go :498-499: only the post-truncate rows are there"
    );
}

/// Go `tiflash_replica_test.go:54-192::TestSetTableFlashReplica`. With two
/// spoofed TiFlash stores: `alter table t_flash set tiflash replica 2
/// location labels 'a','b'` persists `TiFlashReplica{Count: 2,
/// LocationLabels: [a b]}` on plain and hash-partitioned tables and replica 0
/// clears it; `UpdateTableReplicaInfo(tableID, true/false)` flips
/// `Available` for the table; for a partitioned table per-partition ids
/// accumulate into `AvailablePartitionIDs` in first-set order, all-three
/// makes `Available` true, un-setting one makes it false again; an unknown id
/// answers `[schema:1146]Table which ID = 9223372036854775807 does not
/// exist.`; `FindTableByPartitionID` resolves a partition id to the parent
/// table but a table id to nil; and replica 2 over zero stores is refused
/// `the tiflash replica count: 2 should be less than the total tiflash
/// server count: 0`.
// go-parity-gap: no SET TIFLASH REPLICA carrier, no UpdateTableReplicaInfo,
// no store-count spoof, no TiFlash replica metadata on the live table.
#[test]
#[ignore]
fn set_table_flash_replica_records_and_clears_replica_settings() {
}

/// Go `tiflash_replica_test.go:193-217::TestInfoSchemaForTiFlashReplica`.
/// After `alter table t set tiflash replica 2 location labels 'a','b'`,
/// `information_schema.tiflash_replica` reports `test t 2 a,b 0 0`; flipping
/// `TiFlashReplica.Available` in the meta (via a raw `meta.Mutator`
/// `UpdateTable` + `dom.Reload()`) makes the same query report `... 1 0` —
/// progress stays 0 until the poller reports otherwise.
// go-parity-gap: no mock TiFlash status server, no SET TIFLASH REPLICA
// carrier, no information_schema.tiflash_replica surface, no domain reload.
#[test]
#[ignore]
fn infoschema_tiflash_replica_reports_the_persisted_replica_state() {
}

/// Go `tiflash_replica_test.go:219-250::TestSetTiFlashReplicaForTemporaryTable`.
/// Go answers `alter table temp set tiflash replica 1` on a GLOBAL temporary
/// table `[ddl:1562]` (`ErrOptOnTemporaryTable`) and on a LOCAL temporary
/// table `[ddl:8200]` (`ErrUnsupportedDDLOperation`); a normal table accepts
/// the replica; and `create global temporary table temp like normal` /
/// `create temporary table temp like normal` copies carry NO replica, so
/// `REPLICA_COUNT` has no row for them.
// go-parity-gap: documented divergence — this tier answers the generic 1105
// ALTER refusal for every table shape (ddl/alter_table.rs:359), so neither
// the Go error codes nor the accepting case can be pinned.
#[test]
#[ignore]
fn set_tiflash_replica_on_temporary_tables_answers_go_errors() {
}

/// Go `tiflash_replica_test.go:252-292::TestSetTiFlashReplicaForAddGBKColumn`.
/// With a TiFlash replica set on the table: adding one GBK column is
/// `[ddl:8200]unsupported add column 'c1' when altering 't' with TiFlash
/// replicas and gbk encoding`, two such columns are
/// `errno.ErrUnsupportedDDLOperation`; the same holds for a table CREATED
/// `charset = gbk` (its default-charset additions are refused) except an
/// explicit `character set utf8` column is accepted; identical arms for
/// GB18030 with the `gb18030 encoding` message.
// go-parity-gap: Go's gate is `checkUnsupportedCharsetForTiFlash`
// (pkg/ddl/add_column.go:186, raising ErrUnsupportedAddColumn at :189) over a
// table whose TiFlashReplica is set — a carrier this tier lacks, so neither
// the refusals nor the accepting utf8 arm can be pinned.
#[test]
#[ignore]
fn add_gbk_or_gb18030_columns_with_a_tiflash_replica_answers_8200() {
}

/// Go `tiflash_replica_test.go:294-323::TestSetTableFlashReplicaForSystemTable`.
/// As root, `alter table <t> set tiflash replica 1` over every table of
/// `MySQL`/`INFORMATION_SCHEMA`/`PERFORMANCE_SCHEMA`/`METRICS_SCHEMA`/`SYS`:
/// MySQL/SYS non-views answer
/// `[ddl:8200]Unsupported \`set TiFlash replica\` settings for system table
/// and memory table`, views answer `ErrWrongObject`, and the other three
/// schemas answer `[planner:1142]ALTER command denied to user 'root'@'%' for
/// table '<name>'` (lower-cased name).
// go-parity-gap: no authed multi-schema ALTER surface, no privilege
// evaluation, no SET TIFLASH REPLICA carrier.
#[test]
#[ignore]
fn set_tiflash_replica_on_system_tables_answers_per_database_errors() {
}

/// Go `tiflash_replica_test.go:325-373::TestSkipSchemaChecker` (its nextgen
/// skip guard at :327 does not apply to this classic-only tier). With
/// `tidb_enable_metadata_lock=0`: an uncommitted txn survives a concurrent
/// `alter table t1 set tiflash replica 2 ...` (ActionSetTiFlashReplica) and a
/// concurrent `UpdateTableReplicaInfo` (ActionUpdateTiFlashReplicaStatus) —
/// the schema checker is SKIPPED for those two actions — while a concurrent
/// `alter table t1 add column b int` makes the commit fail
/// `domain.ErrInfoSchemaChanged` (the checker is not skipped, and when the
/// infoschema change forces a full reload the commit may surface exactly that
/// error).
// go-parity-gap: no concurrent-session transaction validation, no metadata
// lock switch, no SET TIFLASH REPLICA / UpdateTableReplicaInfo carriers.
#[test]
#[ignore]
fn tiflash_replica_ddls_skip_the_schema_checker_but_add_column_does_not() {
}

/// Go `tiflash_replica_test.go:375-436::TestCreateTableWithLike2`, the four
/// failpoint arms. `create table t2 like t1` is fired by a
/// `beforeRunOneJobStep` failpoint the moment the source reaches
/// `StateDeleteOnly` of an ADD/DROP COLUMN or ADD/DROP INDEX job; each copy
/// then succeeds, accepts further ALTERs, and its visible column count
/// equals its `Meta().Columns` count (the ADD INDEX arm additionally requires
/// every copied index to be `model.StatePublic`).
// go-parity-gap: no online-DDL job queue or state machine, no failpoints, no
// backgroundExec session.
#[test]
#[ignore]
fn create_table_like_started_during_non_public_source_schema_changes() {
}

/// Go `tiflash_replica_test.go:437-475::TestCreateTableWithLike2`, the TiFlash
/// arms. After both partitions of a hash-partitioned `t1` are mocked
/// available (`UpdateTableReplicaInfo`), `create table t2 like t1` copies
/// `TiFlashReplica.Count` and `LocationLabels` but lands with
/// `Available=false` and empty `AvailablePartitionIDs` (Go
/// `BuildTableInfoWithLike`, `pkg/ddl/create_table.go:1281-1289` keeps the
/// settings and strips the availability), while `t1` itself stays available
/// with both partition ids.
// go-parity-gap: documented divergence — Go's LIKE copies the replica
// settings minus availability; this tier's `KvTable::create_like`
// (kv_table.rs:860) carries no TiFlash replica metadata at all, so a copy
// drops the settings outright, and the UpdateTableReplicaInfo setup itself is
// unavailable. Nothing is approximated.
#[test]
#[ignore]
fn create_table_like_copies_tiflash_replica_settings_clearing_availability() {
}

/// Go `tiflash_replica_test.go:503-576::TestTruncateTable2`, the slices this
/// tier cannot answer. (a) :503-507: the truncated table is a NEW table with
/// a strictly greater id (`newTblInfo.ID > oldTblID`); (b) :509-530 under
/// `EmulatorGCEnable`: the OLD table's key range is physically emptied by the
/// background GC worker (`require.Eventually` no key with the old table
/// prefix remains); (c) :532-561 with a replica mocked available: TRUNCATE
/// TABLE keeps `Count`/`LocationLabels` but clears `Available` and
/// `AvailablePartitionIDs` (Go `pkg/ddl/table.go:560-561`), TRUNCATE
/// PARTITION keeps the untouched partition's id in `AvailablePartitionIDs`
/// (`[p1]` after truncating p0) and a second truncate of p0 leaves that
/// unchanged.
// go-parity-gap: documented divergence — this tier truncates IN PLACE
// (KvTable::truncate, kv_table.rs:1670, keeps the table id), and has no GC
// worker, no key-range iteration, and no TiFlash replica metadata, so none
// of (a)-(c) can be pinned.
#[test]
#[ignore]
fn truncate_table_reassigns_the_id_and_clears_tiflash_availability() {
}
