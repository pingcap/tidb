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

//! Ports of `pkg/ddl/db_cache_test.go`: the `ALTER TABLE [NO]CACHE` DDL
//! surface and the persisted `TableInfo.TableCacheStatusType` it flips.
//!
//! Go drives these through `testkit.CreateMockStoreAndDomain` and asserts the
//! persisted status via `checkTableCacheStatus` (`db_cache_test.go:33`), which
//! reads `external.GetTableByName(...).Meta().TableCacheStatusType`; these
//! tests run the same statements through the testkit and read the same field
//! with `tk.get_table_by_name`.

use crate::support::testkit::TestKit;
use tidb_infoschema::TableEntry;
use tidb_model::TableCacheStatusType;

/// Go `checkTableCacheStatus(t, tk, dbName, tableName, status)`.
#[track_caller]
fn check_table_cache_status(tk: &TestKit, db: &str, table: &str, status: TableCacheStatusType) {
    match tk.get_table_by_name(db, table) {
        TableEntry::Kv(kv) => assert_eq!(kv.cache_status(), status, "{db}.{table}"),
        _ => panic!("{db}.{table} is not a KV table"),
    }
}

/// Statement cases from Go `TestAlterTableCache`. The two transaction
/// blocks involving schema leases are separate coverage obligations.
#[test]
fn alter_table_cache_pins_enable_disable_and_the_refusals_around_them() {
    let mut tk = TestKit::new();

    tk.must_exec("create table t1 (n int auto_increment primary key)");
    tk.must_get_err_code("alter table t1 ca", 1064); // errno.ErrParse
    tk.must_get_err_code("alter table t2 cache", 1146); // errno.ErrNoSuchTable
    tk.must_exec("alter table t1 cache");
    check_table_cache_status(&tk, "test", "t1", TableCacheStatusType::ENABLE);
    // ... and NOCACHE back to disable, which Go's later transaction block
    // relies on (`alter table t1 nocache` committing cleanly).
    tk.must_exec("alter table t1 nocache");
    check_table_cache_status(&tk, "test", "t1", TableCacheStatusType::DISABLE);
    tk.must_exec("drop table if exists t1");

    // Go additionally tests MDL rollback with ErrInfoSchemaChanged and
    // schema-checker-skip commit. Neither is asserted by this case.

    // The like-copy resets the cache/replica status (Go
    // BuildTableInfoWithLike), asserted at the end of Go's test: `t` stays
    // Enable, `t3` comes up Disable.
    tk.must_exec("create table t (a int)");
    tk.must_exec("alter table t cache");
    check_table_cache_status(&tk, "test", "t", TableCacheStatusType::ENABLE);
    tk.must_exec("create table t3 like t");
    check_table_cache_status(&tk, "test", "t3", TableCacheStatusType::DISABLE);

    // `alter table t cache` on a missing table is 1146, then caching a fresh
    // table repeatedly is fine ("Multiple alter cache is okay"). Go NOCACHEs
    // before dropping: DROP refuses a still-cached table.
    tk.must_exec("alter table t nocache");
    tk.must_exec("drop table if exists t");
    tk.must_get_err_code("alter table t cache", 1146);
    tk.must_exec("create table t (a int)");
    tk.must_exec("alter table t cache");
    tk.must_exec("alter table t cache");
    check_table_cache_status(&tk, "test", "t", TableCacheStatusType::ENABLE);
    tk.must_exec("alter table t nocache");
    tk.must_exec("drop table if exists t");

    // A LOCAL temporary table refuses `ALTER TABLE ... CACHE` with
    // ErrUnsupportedDDLOperation (8200).
    tk.must_exec("create temporary table t (id int primary key auto_increment, u int unique, v int)");
    tk.must_get_err_code("alter table t cache", 8200);
    tk.must_exec("drop table if exists t");

    // A GLOBAL temporary table refuses it with
    // ErrOptOnTemporaryTable("alter temporary table cache") (8006).
    tk.must_exec(
        "create global temporary table tmp1 (id int not null primary key, code int not null, \
         value int default null, unique key code(code)) on commit delete rows",
    );
    let error = tk.must_get_err_code("alter table tmp1 cache", 8006);
    assert!(
        error.message.contains("alter temporary table cache"),
        "Go formats the operation into the message, got {:?}",
        error.message
    );
}

/// `db_cache_test.go::TestCacheTableSizeLimit`, admission half.
///
/// Go fills `cache_t1` past the 64 MiB limit (1024 iterations of
/// `insert into cache_t1 select * from tmp`, 64 rows of ~1 KiB each) so
/// `alter table cache_t1 cache` fails with ErrOptOnCacheTable (8242), while
/// `cache_t2`, filled from `cache_t1` at iteration 900, caches fine.
#[test]
fn cache_table_size_limit_admits_only_tables_within_the_limit() {
    let mut tk = TestKit::new();
    tk.must_exec("create table cache_t1 (c1 int, c varchar(1024))");
    tk.must_exec("create table cache_t2 (c1 int, c varchar(1024))");
    tk.must_exec("create table tmp (c1 int, c varchar(1024))");
    for i in 0..64 {
        tk.must_exec(&format!("insert into tmp values ({i}, repeat('x', 1024))"));
    }
    for i in 0..1024 {
        tk.must_exec("insert into cache_t1 select * from tmp");
        if i == 900 {
            tk.must_exec("insert into cache_t2 select * from cache_t1");
        }
    }

    // The oversize table is refused with Go's ErrOptOnCacheTable.
    tk.must_get_err_code("alter table cache_t1 cache", 8242);
    check_table_cache_status(&tk, "test", "cache_t1", TableCacheStatusType::DISABLE);

    // The within-limit table caches successfully.
    tk.must_exec("alter table cache_t2 cache");
    check_table_cache_status(&tk, "test", "cache_t2", TableCacheStatusType::ENABLE);
}
