// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Inc. 2.0 (the "License");
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

//! Ports of `pkg/ddl/db_cache_test.go` (read from origin/master) onto this
//! crate's cache-table tier: the `ALTER TABLE [NO]CACHE` DDL surface
//! (`crate::ddl::table_cache`) and the `KvTable` cache-status it flips
//! (`crate::kv_table::cache`).
//!
//! Go drives these through a full cluster (`testkit.CreateMockStoreAndDomain`)
//! and asserts the persisted `TableInfo.TableCacheStatusType` via
//! `checkTableCacheStatus` (`db_cache_test.go:27`). These tests exercise the
//! equivalent metadata and size-admission behavior owned by this crate.

use tidb_datatype::Datum;
use tidb_expr::NoColumns;

use crate::driver::{TableEntry, DEFAULT_DATABASE};
use crate::{
    run_alter_table_in, run_create_table_on, run_drop_table_in, Catalog, KvTable, StmtContext,
};

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

fn create(catalog: &mut Catalog, sql: &str) {
    run_create_table_on(sql, catalog).unwrap_or_else(|error| panic!("{sql}: {error}"));
}

fn alter(catalog: &mut Catalog, sql: &str) -> Result<(), crate::DriverError> {
    run_alter_table_in(sql, catalog, DEFAULT_DATABASE, &ctx())
}

fn drop(catalog: &mut Catalog, sql: &str) {
    run_drop_table_in(
        sql,
        catalog,
        DEFAULT_DATABASE,
        tidb_parser::SqlMode::default(),
        true,
    )
    .unwrap_or_else(|error| panic!("{sql}: {error}"));
}

/// The `KvTable` a test asserts state against (Go's `checkTableCacheStatus`
/// reads `tb.Meta()`; this tier reads the catalog's `KvTable`).
fn kv<'a>(catalog: &'a Catalog, name: &str) -> &'a KvTable {
    match catalog.get_table_for_test(name) {
        Some(TableEntry::Kv(table)) => table,
        _ => panic!("{name} is not a KV table"),
    }
}

fn kv_mut<'a>(catalog: &'a mut Catalog, name: &str) -> &'a mut KvTable {
    match catalog.table_mut_in(DEFAULT_DATABASE, name) {
        Some(TableEntry::Kv(table)) => table,
        _ => panic!("{name} is not a KV table"),
    }
}

fn code_of(error: crate::DriverError) -> u16 {
    error.to_mysql_error().code
}

/// `db_cache_test.go:41::TestAlterTableCache`, portable halves.
///
/// Go's sequence and every refusal it pins, minus the two transaction blocks
/// that need the schema-lease machinery this tier does not have (see the
/// `#[ignore]`d sibling below). Each assertion cites its Go statement.
#[test]
fn alter_table_cache_pins_enable_disable_and_the_refusals_around_them() {
    let mut catalog = Catalog::default();

    // `alter table t1 ca` -> ErrParse (1064): still a syntax error here.
    create(
        &mut catalog,
        "create table t1 (n int auto_increment primary key)",
    );
    let error = alter(&mut catalog, "alter table t1 ca").unwrap_err();
    assert_eq!(code_of(error), 1064, "Go: errno.ErrParse");

    // `alter table t2 cache` with no such table -> ErrNoSuchTable (1146).
    let error = alter(&mut catalog, "alter table t2 cache").unwrap_err();
    assert_eq!(code_of(error), 1146, "Go: errno.ErrNoSuchTable");

    // cache -> TableCacheStatusEnable (checkTableCacheStatus at :57).
    alter(&mut catalog, "alter table t1 cache").unwrap();
    assert!(
        kv(&catalog, "t1").is_cached(),
        "Go expects TableCacheStatusEnable"
    );
    // ... and NOCACHE back to disable, which Go's later transaction block
    // relies on (`alter table t1 nocache` committing cleanly).
    alter(&mut catalog, "alter table t1 nocache").unwrap();
    assert!(
        !kv(&catalog, "t1").is_cache_table(),
        "Go expects TableCacheStatusDisable after NOCACHE"
    );
    drop(&mut catalog, "drop table if exists t1");

    // [Go's two transaction blocks here (metadata-lock rollback with
    // domain.ErrInfoSchemaChanged, then the schema-checker-skip commit) need
    // the schema-version lease; the `#[ignore]`d sibling below carries them.]

    // Cache status survives a `CREATE TABLE t3 LIKE t`: the like-copy resets
    // the cache/replica status (Go BuildTableInfoWithLike), asserted at :109-111
    // -- `t` stays Enable, `t3` comes up Disable.
    create(&mut catalog, "create table t (a int)");
    alter(&mut catalog, "alter table t cache").unwrap();
    assert!(kv(&catalog, "t").is_cached());
    create(&mut catalog, "create table t3 like t");
    assert!(
        !kv(&catalog, "t3").is_cache_table(),
        "Go expects TableCacheStatusDisable for the LIKE copy"
    );

    // `alter table t cache` on a missing table is still 1146, then caching a
    // fresh table twice is fine ("Multiple alter cache is okay", :89-91).
    // Go NOCACHEs before dropping (:97-98) — like Go, this tier refuses DROP on
    // a still-cached table.
    alter(&mut catalog, "alter table t nocache").unwrap();
    drop(&mut catalog, "drop table if exists t");
    let error = alter(&mut catalog, "alter table t cache").unwrap_err();
    assert_eq!(code_of(error), 1146);
    create(&mut catalog, "create table t (a int)");
    alter(&mut catalog, "alter table t cache").unwrap();
    alter(&mut catalog, "alter table t cache").unwrap();
    assert!(kv(&catalog, "t").is_cached());
    // Go's :95-96 again: NOCACHE, then the drop that lets the temporary
    // fixtures reuse the name.
    alter(&mut catalog, "alter table t nocache").unwrap();
    drop(&mut catalog, "drop table if exists t");

    // A LOCAL temporary table refuses `ALTER TABLE ... CACHE` with
    // ErrUnsupportedDDLOperation (8200, :99); the DDL-job guard fires before
    // any option is read (`refuse_local_temporary_table_ddl`).
    create(
        &mut catalog,
        "create temporary table t (id int primary key auto_increment, u int unique, v int)",
    );
    let error = alter(&mut catalog, "alter table t cache").unwrap_err();
    assert_eq!(
        code_of(error),
        8200,
        "Go: errno.ErrUnsupportedDDLOperation for a local temporary table"
    );
    drop(&mut catalog, "drop table if exists t");

    // A GLOBAL temporary table refuses it with
    // ErrOptOnTemporaryTable("alter temporary table cache") (8006, :104).
    create(
        &mut catalog,
        "create global temporary table tmp1 (id int not null primary key, code int not null, \
         value int default null, unique key code(code)) on commit delete rows",
    );
    let error = alter(&mut catalog, "alter table tmp1 cache").unwrap_err();
    let mysql = error.to_mysql_error();
    assert_eq!(mysql.code, 8006, "Go: dbterror.ErrOptOnTemporaryTable");
    assert!(
        mysql.message.contains("alter temporary table cache"),
        "Go formats the operation into the message, got {:?}",
        mysql.message
    );
}

/// `db_cache_test.go:151::TestCacheTableSizeLimit`, admission half.
///
/// Go fills `cache_t1` past the 64 MiB limit (1024 iterations x 64 rows of
/// ~1 KiB) so `alter table cache_t1 cache` fails with ErrOptOnCacheTable
/// (8242), while the half-full `cache_t2` caches fine. The encoded image this
/// tier measures is the same one `KvTable::enable_cache` scans, so the rows
/// are written through it directly — the same call the INSERT path makes —
/// in 64-row batches exactly like Go's `insert into cache_t1 select * from
/// tmp`.
#[test]
fn cache_table_size_limit_admits_only_tables_within_the_limit() {
    const BATCHES: usize = 1024;
    const ROWS_PER_BATCH: usize = 64;
    const FILLER: [u8; 1024] = [b'x'; 1024];

    let mut catalog = Catalog::default();
    create(
        &mut catalog,
        "create table cache_t1 (c1 int, c varchar(1024))",
    );
    create(
        &mut catalog,
        "create table cache_t2 (c1 int, c varchar(1024))",
    );

    let fill = |catalog: &mut Catalog, name: &str, batches: usize, start: &mut i64| {
        for _ in 0..batches {
            let table = kv_mut(catalog, name);
            for _ in 0..ROWS_PER_BATCH {
                *start += 1;
                table
                    .insert_row(
                        &[Datum::Int(*start), Datum::Bytes(FILLER.to_vec())],
                        &NoColumns,
                    )
                    .unwrap();
            }
        }
    };
    // > 64 MiB into cache_t1: 1024 batches x 64 rows x ~1 KiB.
    fill(&mut catalog, "cache_t1", BATCHES, &mut 0);
    // ~32 MiB into cache_t2 (Go fills cache_t2 to 900 batches' worth at :169-170).
    fill(&mut catalog, "cache_t2", 512, &mut 1_000_000);

    // The oversize table is refused with Go's ErrOptOnCacheTable.
    let error = alter(&mut catalog, "alter table cache_t1 cache").unwrap_err();
    assert_eq!(
        code_of(error),
        8242,
        "Go: errno.ErrOptOnCacheTable for an oversize cache"
    );
    assert!(!kv(&catalog, "cache_t1").is_cache_table());

    // The within-limit table caches successfully (:174).
    alter(&mut catalog, "alter table cache_t2 cache").unwrap();
    assert!(
        kv(&catalog, "cache_t2").is_cached(),
        "Go expects the half-full table to cache"
    );
}
