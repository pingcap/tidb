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

//! The cached-table (`ALTER TABLE CACHE`/`NOCACHE`) gate: ported from Go
//! `pkg/ddl/db_cache_test.go` (`TestAlterTableCache`, `TestCacheTableSizeLimit`,
//! `TestAlterTableNoCacheRemovesTableCacheMeta`, `TestIssue34069`).
//!
//! Go asserts the cache status through `checkTableCacheStatus`, which reloads
//! the domain and reads `tb.Meta().TableCacheStatusType`. This tier's catalog
//! IS that metadata, so the ports read
//! [`crate::KvTable::is_cached`] — the same
//! `TableInfo.TableCacheStatusType` value — straight off the table entry.
//! The admission rules themselves are Go `(*executor).AlterTableCache`
//! (`pkg/ddl/executor.go:6940`): idempotent when already cached, refused in
//! system databases, refused for every temporary scope with
//! `ErrOptOnTemporaryTable("alter temporary table cache")` (`executor.go:6954`),
//! refused for a partitioned table with `"partition mode"` (`executor.go:6958`),
//! and refused over the 64 MiB limit with `"table too large"`
//! (`executor.go:6966`).

use crate::{
    Catalog, DriverError, SchemaErrorKind, StmtContext, DEFAULT_DATABASE, TableEntry,
};

/// Runs an `ALTER TABLE` against the default database, Go's `tk.MustExec` /
/// `tk.ExecToErr`.
fn alter(sql: &str, catalog: &mut Catalog) -> Result<(), DriverError> {
    crate::ddl::run_alter_table_in(sql, catalog, DEFAULT_DATABASE, &StmtContext::for_query())
}

fn create(sql: &str, catalog: &mut Catalog) {
    crate::run_create_table_on(sql, catalog)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
}

/// The `KvTable` a test asserts cache status against, Go's
/// `external.GetTableByName`.
fn kv<'a>(catalog: &'a Catalog, name: &str) -> &'a crate::KvTable {
    match catalog.table_in(DEFAULT_DATABASE, name) {
        Some(TableEntry::Kv(table)) => table,
        _ => panic!("{name} is not a KV table"),
    }
}

/// Port of `pkg/ddl/db_cache_test.go::TestAlterTableCache` (line 41): the
/// CACHE/NOCACHE mode round trip and its admission guards, asserted the way
/// Go asserts them through `checkTableCacheStatus` and `MustGetErrCode` /
/// `MustGetErrMsg`.
///
/// Unported legs, named so nothing is silently dropped:
/// - the mid-transaction `ErrInfoSchemaChanged` leg (and its
///   `kerneltype.IsClassic()` split) needs Go's schema-lease verifier across
///   two sessions; this tier has no lease and the catalog cannot go stale
///   mid-transaction;
/// - `mysql.table_cache_meta` lock rows are written by Go's
///   `AlterTableCache` (`executor.go:6970`) but this tier models no system
///   tables (see the NOCACHE gap test below).
#[test]
fn alter_table_cache_round_trip_and_guards() {
    let mut catalog = Catalog::default();

    create("CREATE TABLE t1 (n INT AUTO_INCREMENT PRIMARY KEY)", &mut catalog);

    // A truncated CACHE keyword is a parse error, Go's
    // `tk.MustGetErrCode("alter table t1 ca", errno.ErrParse)`.
    assert!(matches!(alter("ALTER TABLE t1 ca", &mut catalog), Err(DriverError::Parse(_))));

    // Caching an unknown table is 1146, Go's
    // `tk.MustGetErrCode("alter table t2 cache", errno.ErrNoSuchTable)`.
    assert!(matches!(
        alter("ALTER TABLE t2 cache", &mut catalog),
        Err(DriverError::Schema(SchemaErrorKind::UnknownTable(_)))
    ));

    // CACHE enables the persisted status; NOCACHE clears it.
    alter("ALTER TABLE t1 cache", &mut catalog).expect("alter table t1 cache");
    assert!(kv(&catalog, "t1").is_cached());
    alter("ALTER TABLE t1 nocache", &mut catalog).expect("alter table t1 nocache");
    assert!(!kv(&catalog, "t1").is_cached());
    assert!(!kv(&catalog, "t1").is_cache_table());

    // "Multiple alter cache is okay" (Go's own comment): an already-cached
    // table takes a repeat CACHE as the no-op Go's `executor.go:6946`
    // early return makes it.
    create("CREATE TABLE t (a INT)", &mut catalog);
    alter("ALTER TABLE t cache", &mut catalog).expect("first cache");
    alter("ALTER TABLE t cache", &mut catalog).expect("second cache");
    assert!(kv(&catalog, "t").is_cached());
    alter("ALTER TABLE t nocache", &mut catalog).expect("nocache");

    // A LOCAL temporary table refuses every ALTER before any option is read
    // (Go's ErrUnsupportedDDLOperation for `alter table t cache`; this tier's
    // refusal is `crate::ddl::refuse_local_temporary_table_ddl`).
    create(
        "CREATE TEMPORARY TABLE tmp_local (id INT PRIMARY KEY AUTO_INCREMENT, u INT UNIQUE, v INT)",
        &mut catalog,
    );
    assert!(matches!(
        alter("ALTER TABLE tmp_local cache", &mut catalog),
        Err(DriverError::UnsupportedLocalTempTableDDL(_))
    ));

    // A GLOBAL temporary table reaches the DDL package and is refused with
    // Go's exact argument: `ErrOptOnTemporaryTable("alter temporary table
    // cache")`, `executor.go:6954`.
    create(
        "CREATE GLOBAL TEMPORARY TABLE tmp1 (id INT NOT NULL PRIMARY KEY, code INT NOT NULL, \
         value INT DEFAULT NULL, UNIQUE KEY code (code)) ON COMMIT DELETE ROWS",
        &mut catalog,
    );
    assert!(matches!(
        alter("ALTER TABLE tmp1 cache", &mut catalog),
        Err(DriverError::OptOnTemporaryTable("alter temporary table cache"))
    ));

    // `CREATE TABLE ... LIKE` copies the structure but NOT the cache status:
    // Go resets `TableCacheStatusType` in `BuildTableInfoWithLike`, asserted
    // at the end of `TestAlterTableCache` (`db_cache_test.go:105-111`).
    create("CREATE TABLE t_like (a INT)", &mut catalog);
    alter("ALTER TABLE t_like cache", &mut catalog).expect("cache the like source");
    create("CREATE TABLE t_like_copy LIKE t_like", &mut catalog);
    assert!(kv(&catalog, "t_like").is_cached());
    assert!(!kv(&catalog, "t_like_copy").is_cached());
}

/// Port of the admission half of
/// `pkg/ddl/db_cache_test.go::TestCacheTableSizeLimit` (line 151): a table
/// whose encoded image exceeds Go's `checkCacheTableSize` limit refuses
/// `ALTER TABLE ... CACHE` with `ErrOptOnCacheTable("table too large")`
/// (`executor.go:6966`), and a table built just under the limit the same way
/// is accepted.
///
/// The fixtures keep Go's exact shape: one `tmp` table of 64 rows holding a
/// 1024-byte `varchar`, 1024 `INSERT ... SELECT` statements into `cache_t1`
/// (just over the 64 MiB limit of `CACHE_TABLE_SIZE_LIMIT`), and the copy into
/// `cache_t2` at iteration 900 (just under it).
///
/// Unported tail: Go then grows `cache_t2` further and requires reads to
/// eventually set `StmtCtx.ReadFromTableCache` (the asynchronous cache load)
/// before INSERT is forbidden with `ErrOptOnCacheTable`. That leg is
/// environment-dependent even in Go — the test returns early when 200 polling
/// iterations do not observe a cached read — and this tier models no
/// asynchronous cache loading or cached-table insert guard (the cache status
/// is metadata-only, see `kv_table.rs` on `cache_status`), so the leg has no
/// faithful equivalent to assert.
#[test]
fn cache_table_size_limit_admits_under_and_refuses_over() {
    const ROW_TEXT: &str = "x"; // repeated to 1024 bytes below, Go's repeat('x', 1024)
    let mut catalog = Catalog::default();

    create("CREATE TABLE cache_t1 (c1 INT, c VARCHAR(1024))", &mut catalog);
    create("CREATE TABLE cache_t2 (c1 INT, c VARCHAR(1024))", &mut catalog);
    create("CREATE TABLE tmp (c1 INT, c VARCHAR(1024))", &mut catalog);

    let ctx = StmtContext::for_query();
    let insert = |sql: &str, catalog: &mut Catalog| {
        crate::run_insert_on(sql, catalog, &ctx)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
    };
    let row = |i: usize| format!("INSERT INTO tmp VALUES ({i}, '{}')", ROW_TEXT.repeat(1024));
    for i in 0..64 {
        insert(&row(i), &mut catalog);
    }

    // Make cache_t1 larger than the limit, Go's "Make the cache_t1 size
    // large than 64K" loop (the limit itself is 64 MiB).
    for i in 0..1024 {
        if i == 900 {
            insert("INSERT INTO cache_t2 SELECT * FROM cache_t1", &mut catalog);
        }
        insert("INSERT INTO cache_t1 SELECT * FROM tmp", &mut catalog);
    }

    // Over the limit: refused.
    assert!(matches!(
        alter("ALTER TABLE cache_t1 cache", &mut catalog),
        Err(DriverError::OperationOnCachedTable("table too large"))
    ));
    // Under the limit: accepted.
    alter("ALTER TABLE cache_t2 cache", &mut catalog).expect("cache the under-limit table");
    assert!(kv(&catalog, "cache_t2").is_cached());
    assert!(!kv(&catalog, "cache_t1").is_cached());
}

/// `pkg/ddl/db_cache_test.go::TestAlterTableNoCacheRemovesTableCacheMeta`
/// (line 116, issue #66042): after a SELECT takes the cached-table READ lock,
/// `ALTER TABLE NOCACHE` must DELETE the table's row from
/// `mysql.table_cache_meta` (verifying `tid`, `lock_type` ∈ {NONE, READ}
/// before, zero rows after) and leave `TableCacheStatusType` disabled.
// go-parity-gap: the pinned observable is a row of the mysql.table_cache_meta
// system table; this crate models no system tables and no lease lock rows
// (`KvTable::disable_cache` clears only the persisted status flag).
#[test]
#[ignore = "go-parity-gap: mysql.table_cache_meta lease rows are not modeled in this crate"]
fn alter_table_nocache_removes_the_cache_meta_row() {}

/// `pkg/ddl/db_cache_test.go::TestIssue34069` (line 212): with the security
/// enhanced mode (SEM) enabled — for both the V1 and V2 semaphore
/// configurations via `sem.SwitchToSEMForTest` — `ALTER TABLE t_34069 cache`
/// on a plain table must SUCCEED for an authenticated root session; SEM's
/// extra restrictions must not reject the cache alter.
// go-parity-gap: SEM (`pkg/util/sem`) is an auth/privilege layer living in
// front of the DDL executor; this crate models no sessions, users, or SEM
// gate, so "SEM enabled but allowed" has nothing to exercise.
#[test]
#[ignore = "go-parity-gap: the SEM security gate is not modeled in this crate"]
fn sem_enabled_still_allows_alter_table_cache() {}
