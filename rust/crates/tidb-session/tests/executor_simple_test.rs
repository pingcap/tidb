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

//! Go `pkg/executor/simple_test.go`'s `REFRESH STATS` tests, on the mock
//! store through the session, as Go's testkit runs them.

use std::sync::Arc;

use crate::support::testkit::*;
use tidb_infoschema::TableEntry;

/// Go `handle.GetPhysicalTableStats(tbl.Meta().ID, tbl.Meta())`.
fn physical_table_stats(tk: &TestKit, table: &str) -> Arc<tidb_stats::Table> {
    let TableEntry::Kv(tbl) = tk.get_table_by_name("test", table) else {
        panic!("test.{table} is not a storage-backed table");
    };
    tk.stats_table_for_test(tbl.table_id())
        .unwrap_or_else(|| panic!("test.{table} has no statistics"))
}

/// Go `stats.GetIdx(1)`.
fn idx1(stats: &tidb_stats::Table) -> Option<tidb_stats::SharedIndex> {
    stats.hist_coll.get_index(1)
}

/// Go `TestRefreshTableStats`.
#[test]
fn test_refresh_table_stats() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1 (a int, b int, index idx(a))");
    tk.must_exec("insert into t1 values (1,1), (2,2), (3,3)");
    tk.must_exec("create table t2 (a int, b int, index idx(a))");
    tk.must_exec("insert into t2 values (1,1), (2,2), (3,3)");
    tk.must_exec("analyze table t1, t2 all columns with 1 topn, 2 buckets");

    let tbl1_stats = physical_table_stats(&tk, "t1");
    let tbl2_stats = physical_table_stats(&tk, "t2");
    tk.must_exec("refresh stats t1, test.t1");
    let tbl1_stats_updated = physical_table_stats(&tk, "t1");
    let tbl2_stats_updated = physical_table_stats(&tk, "t2");
    assert!(!Arc::ptr_eq(&tbl1_stats, &tbl1_stats_updated));
    assert!(
        idx1(&tbl1_stats_updated).is_none(),
        "index stats shouldn't be loaded in lite mode"
    );
    assert!(Arc::ptr_eq(&tbl2_stats, &tbl2_stats_updated));
    tk.must_exec("REFRESH STATS *.* FULL");
    let tbl1_stats_updated = physical_table_stats(&tk, "t1");
    assert!(
        idx1(&tbl1_stats_updated).is_some(),
        "index stats should be loaded in full mode"
    );
    let tbl2_stats_updated = physical_table_stats(&tk, "t2");
    assert!(!Arc::ptr_eq(&tbl2_stats, &tbl2_stats_updated));
    assert!(
        idx1(&tbl2_stats_updated).is_some(),
        "index stats should be loaded in full mode"
    );
}

/// Go `TestRefreshStatsWarningsForMissingObjects`.
#[test]
fn test_refresh_stats_warnings_for_missing_objects() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (a int)");

    tk.must_exec("refresh stats missing_db.*");
    tk.must_query("show warnings")
        .check(["Warning 1049 Unknown database 'missing_db'"]);

    tk.must_exec("refresh stats test.t_missing, test.t");
    tk.must_query("show warnings")
        .check(["Warning 1146 Table 'test.t_missing' doesn't exist"]);

    tk.must_exec("refresh stats t, t1");
    tk.must_query("show warnings")
        .check(["Warning 1146 Table 'test.t1' doesn't exist"]);

    tk.must_exec("refresh stats t");
    tk.must_query("show warnings").check_empty();
}

/// Go `TestRefreshAllNonExistentTables`.
#[test]
fn test_refresh_all_non_existent_tables() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t1");
    tk.must_exec("create table t1 (a int, b int, index idx(a))");
    tk.must_exec("insert into t1 values (1,1), (2,2), (3,3)");
    tk.must_exec("analyze table t1 all columns with 1 topn, 2 buckets");

    let tbl1_stats = physical_table_stats(&tk, "t1");
    tk.must_exec("refresh stats missing_db.*, t2");
    let tbl1_stats_updated = physical_table_stats(&tk, "t1");
    assert!(Arc::ptr_eq(&tbl1_stats, &tbl1_stats_updated));
}

/// Go `TestRefreshStatsNoTables`.
#[test]
fn test_refresh_stats_no_tables() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");

    tk.must_exec("refresh stats *.*");
}

/// Go `TestRefreshStatsRequiresDefaultDB`.
#[test]
fn test_refresh_stats_requires_default_db() {
    // Go `testkit.NewTestKit` has no current database; this kit's session
    // starts in `test`, so dropping the database it switched to clears it.
    let mut tk = TestKit::new();
    tk.must_exec("create database refresh_no_db");
    tk.must_exec("use refresh_no_db");
    tk.must_exec("drop database refresh_no_db");
    tk.must_query("select database()").check(["<nil>"]);
    // Go `plannererrors.ErrNoDB`.
    tk.must_get_err_code("refresh stats t1", 1046);
}

/// Go `TestRefreshStatsWhenDatabaseIsEmpty`.
#[test]
fn test_refresh_stats_when_database_is_empty() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");

    tk.must_exec("refresh stats test.*");
    tk.must_query("show warnings").check_empty();
}

/// Go `TestRefreshStatsWithFullMode`, up to the closing
/// `handle.TableStatsFromStorage` reload, which has no session-level hook.
#[test]
fn test_refresh_stats_with_full_mode() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1 (a int, b int, index idx(a))");
    tk.must_exec("insert into t1 values (1,1), (2,2), (3,3)");
    tk.must_exec("analyze table t1 all columns with 1 topn, 2 buckets");

    let stats_before_refresh = physical_table_stats(&tk, "t1");
    tk.must_exec("refresh stats t1");
    let stats_after_default_refresh = physical_table_stats(&tk, "t1");
    assert!(!Arc::ptr_eq(
        &stats_before_refresh,
        &stats_after_default_refresh
    ));
    assert!(
        idx1(&stats_after_default_refresh).is_none(),
        "index stats should not be loaded in lite mode"
    );

    tk.must_exec("select * from t1 where a = 1");
    let stats_after_select = physical_table_stats(&tk, "t1");
    assert!(
        !Arc::ptr_eq(&stats_before_refresh, &stats_after_select),
        "stats versuon should not be changed after select"
    );
    assert!(
        idx1(&stats_after_select).is_some(),
        "index stats will be loaded after select"
    );

    tk.must_exec("refresh stats t1");
    let stats_after_default_refresh = physical_table_stats(&tk, "t1");
    assert!(!Arc::ptr_eq(
        &stats_before_refresh,
        &stats_after_default_refresh
    ));
    assert!(
        idx1(&stats_after_default_refresh).is_none(),
        "index stats should be removed in lite mode"
    );

    // Issue a full refresh to ensure the index stats are loaded.
    tk.must_exec("refresh stats t1 full");
    let stats_after_full_refresh = physical_table_stats(&tk, "t1");
    assert!(!Arc::ptr_eq(
        &stats_after_default_refresh,
        &stats_after_full_refresh
    ));
    let index = idx1(&stats_after_full_refresh).expect("index stats should be loaded in full mode");
    let index_version_after_full_refresh = {
        let index = index.read().unwrap();
        assert_eq!(
            index.histogram.buckets.len(),
            1,
            "buckets should be loaded in full mode"
        );
        index.histogram.last_update_version
    };

    // Insert additional data and run ANALYZE again.
    tk.must_exec("insert into t1 values (4,4), (5,5)");
    // Analyze loads statistics based on the current state of the in-memory
    // stats (all statistics have been loaded) while running in lite mode.
    tk.must_exec("analyze table t1 all columns with 1 topn, 2 buckets");
    let stats_after_analyze = physical_table_stats(&tk, "t1");
    assert!(!Arc::ptr_eq(
        &stats_after_full_refresh,
        &stats_after_analyze
    ));
    let index = idx1(&stats_after_analyze).expect("analyze keeps the index loaded");
    let index = index.read().unwrap();
    assert_eq!(
        index.histogram.buckets.len(),
        2,
        "buckets should be loaded in full mode"
    );
    assert!(
        index.histogram.last_update_version > index_version_after_full_refresh,
        "index stats should be updated"
    );
}

/// Go `TestRefreshStatsWithLiteMode`, up to the closing
/// `handle.TableStatsFromStorage` reload, which has no session-level hook.
#[test]
fn test_refresh_stats_with_lite_mode() {
    let _config = update_global_config(|config| config.performance.lite_init_stats = false);

    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1 (a int, b int, index idx(a))");
    tk.must_exec("insert into t1 values (1,1), (2,2), (3,3)");
    tk.must_exec("analyze table t1 all columns with 1 topn, 2 buckets");

    let stats_before_refresh = physical_table_stats(&tk, "t1");
    tk.must_exec("refresh stats t1");
    let stats_after_full_refresh = physical_table_stats(&tk, "t1");
    assert!(!Arc::ptr_eq(
        &stats_before_refresh,
        &stats_after_full_refresh
    ));
    assert!(
        idx1(&stats_after_full_refresh).is_some(),
        "index stats should be loaded in full mode"
    );

    // Run a lite refresh and verify the index stats remain unloaded.
    tk.must_exec("refresh stats t1 lite");
    let stats_after_lite_refresh = physical_table_stats(&tk, "t1");
    assert!(!Arc::ptr_eq(
        &stats_after_full_refresh,
        &stats_after_lite_refresh
    ));
    assert!(
        idx1(&stats_after_lite_refresh).is_none(),
        "index stats should not be loaded in lite mode"
    );

    // Insert additional data and run ANALYZE again.
    tk.must_exec("insert into t1 values (4,4), (5,5)");
    // Analyze loads all statistics when running in full mode.
    tk.must_exec("analyze table t1 all columns with 1 topn, 2 buckets");
    let stats_after_analyze = physical_table_stats(&tk, "t1");
    assert!(!Arc::ptr_eq(
        &stats_after_full_refresh,
        &stats_after_analyze
    ));
    let index =
        idx1(&stats_after_analyze).expect("analyze loads all statistics when running in full mode");
    let index = index.read().unwrap();
    assert_eq!(
        index.histogram.buckets.len(),
        2,
        "buckets should be loaded in full mode"
    );
    assert!(
        index.histogram.last_update_version > 0,
        "index stats should be updated"
    );
}
