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

//! Go `tests/realtikvtest/addindextest3`, over [`crate::realtikvtest`].

use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, Once};

use crate::realtikvtest::Tk;

/// Go `tk.MustQuery(sql).Check(testkit.Rows(rows...))`.
fn must_query_check(tk: &mut Tk, sql: &str, rows: &[&str]) {
    let got: Vec<String> = tk.must_query(sql).into_iter().map(|row| row.join(" ")).collect();
    assert_eq!(got, rows, "{sql}");
}

/// Go `tk.MustGetErrMsg(sql, msg)`.
fn must_get_err_msg(tk: &mut Tk, sql: &str, msg: &str) {
    assert_eq!(tk.exec_to_err(sql), msg, "{sql}");
}

/// The comments column of `admin show ddl jobs 1`, which a classic-kernel
/// ingest job reports as `ingest`.
fn assert_last_job_ingest(tk: &mut Tk) {
    let rows = tk.must_query("admin show ddl jobs 1;");
    assert_eq!(rows.len(), 1);
    assert!(rows[0][12].contains("ingest"), "{}", rows[0][12]);
}

fn use_addindexlit(tk: &mut Tk) {
    tk.must_exec("drop database if exists addindexlit;");
    tk.must_exec("create database addindexlit;");
    tk.must_exec("use addindexlit;");
}

/// Go `ingest.ForceSyncFlagForTest` set for a test, restored on drop.
struct ForceSync(bool);

impl ForceSync {
    fn on() -> Self {
        Self(tidb_ddl_ingest::backend::FORCE_SYNC_FLAG_FOR_TEST.swap(true, Ordering::SeqCst))
    }
}

impl Drop for ForceSync {
    fn drop(&mut self) {
        tidb_ddl_ingest::backend::FORCE_SYNC_FLAG_FOR_TEST.store(self.0, Ordering::SeqCst);
    }
}

fn lit_env() -> &'static tidb_ddl_ingest::env::LightningEnv {
    tidb_ddl_ingest::env::lightning_env().expect("the ingest environment")
}

/// Go `TestAddIndexIngestMemoryUsage`. Go's `ingestctrl.LastAlloc` check is
/// Pebble's manual allocator, which the extsort engine has no counterpart of.
#[test]
fn test_add_index_ingest_memory_usage() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    use_addindexlit(&mut tk);
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");

    tk.must_exec("create table t (a int, b int, c int);");
    let values: Vec<String> = (0..100).map(|i| format!("({i}, {i}, {i})")).collect();
    tk.must_exec(&format!("insert into t values {};", values.join(",")));
    assert_eq!(lit_env().mem_root.current_usage(), 0);
    tk.must_exec("alter table t add index idx(a);");
    tk.must_exec("alter table t add unique index idx1(b);");
    tk.must_exec("admin check table t;");
    assert_eq!(lit_env().mem_root.current_usage(), 0);
}

/// Go `TestAddIndexIngestLimitOneBackend`.
#[test]
fn test_add_index_ingest_limit_one_backend() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    use_addindexlit(&mut tk);
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("create table t (a int, b int);");
    tk.must_exec("insert into t values (1, 1), (2, 2), (3, 3);");

    let mut tk2 = store.new_tk();
    tk2.must_exec("use addindexlit;");
    tk2.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk2.must_exec("create table t2 (a int, b int);");
    tk2.must_exec("insert into t2 values (1, 1), (2, 2), (3, 3);");

    std::thread::scope(|wg| {
        wg.spawn(|| tk.must_exec("alter table t add index idx(a);"));
        wg.spawn(|| tk2.must_exec("alter table t2 add index idx_b(b);"));
    });
    let rows = tk.must_query("admin show ddl jobs 2;");
    assert_eq!(rows.len(), 2);
    assert!(rows[0][12].contains("ingest"), "{:?}", rows[0]);
    assert!(rows[1][12].contains("ingest"), "{:?}", rows[1]);
    assert_eq!(rows[0][7], "3");
    assert_eq!(rows[1][7], "3");

    tk.must_exec("set @@global.tidb_enable_dist_task = 0;");
    // TODO(lance6716): dist_task also need this

    // test cancel is timely
    let (enter_tx, enter) = std::sync::mpsc::sync_channel::<()>(1);
    let block_once = AtomicBool::new(false);
    let _hook = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/beforeExecuteRegionJob",
        move |arg| {
            if block_once.swap(true, Ordering::SeqCst) {
                return;
            }
            let ctx = arg.downcast_mut::<tidb_gocontext::Context>().unwrap();
            let _ = enter_tx.try_send(());
            let _ = ctx.sleep(std::time::Duration::from_secs(20));
        },
    );
    std::thread::scope(|wg| {
        let ddl = wg.spawn(|| tk2.exec("alter table t add index idx_ba(b, a);"));
        enter.recv().unwrap();
        let job_id = tk.must_query("admin show ddl jobs 1;")[0][0].clone();
        let now = std::time::Instant::now();
        tk.must_exec(&format!("admin cancel ddl jobs {job_id}"));
        let err = ddl.join().unwrap().expect_err("the cancelled ADD INDEX fails");
        assert!(err.contains("Cancelled DDL job"), "{err}");
        // cancel should be timely
        assert!(now.elapsed().as_secs_f64() < 30.0, "{:?}", now.elapsed());
    });
}

/// Go `TestAddIndexIngestWriterCountOnPartitionTable`.
#[test]
fn test_add_index_ingest_writer_count_on_partition_table() {
    let _restore = tidb_dxf_testutil::reduce_check_interval_for_test();
    let store = realtikv!();
    let mut tk = store.new_tk();
    use_addindexlit(&mut tk);
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");

    tk.must_exec("create table t (a int primary key) partition by hash(a) partitions 32;");
    let values: Vec<String> = (0..100).map(|i| format!("({i})")).collect();
    tk.must_exec(&format!("insert into t values {}", values.join(",")));
    tk.must_exec("alter table t add index idx(a);");
    assert_last_job_ingest(&mut tk);
}

/// Go `TestIngestMVIndexOnPartitionTable`.
#[test]
fn test_ingest_mv_index_on_partition_table() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let cases = [
        "alter table t add index idx((cast(a as signed array)));",
        "alter table t add unique index idx(pk, (cast(a as signed array)));",
    ];
    for c in cases {
        use_addindexlit(&mut tk);
        tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");

        tk.must_exec("drop table if exists t");
        tk.must_exec("create table t (pk int primary key, a json) partition by hash(pk) partitions 4;");
        tk.must_exec("");
        let add_index_done = AtomicBool::new(false);
        let mut internal_tk = store.new_tk();
        std::thread::scope(|wg| {
            let add_index_done = &add_index_done;
            wg.spawn(move || {
                let mut n = 10240;
                internal_tk.must_exec("use addindexlit;");
                for i in 0..1024 {
                    internal_tk.must_exec(&format!("insert into t values ({n}, '[{n}, {}, {}]')", n + 1, n + 2));
                    internal_tk.must_exec(&format!("delete from t where pk = {}", n - 10));
                    internal_tk.must_exec(&format!(
                        "update t set a = '[{}, {}, {}]' where pk = {}",
                        n - 3,
                        n - 2,
                        n + 1000,
                        n - 5
                    ));
                    n += 1;
                    if i > 256 && add_index_done.load(Ordering::SeqCst) {
                        break;
                    }
                }
            });
            tk.must_exec(c);
            assert_last_job_ingest(&mut tk);
            add_index_done.store(true, Ordering::SeqCst);
        });
        tk.must_exec("admin check table t");
    }
}

/// Go `TestAddIndexIngestAdjustBackfillWorkerCountFail`.
#[test]
fn test_add_index_ingest_adjust_backfill_worker_count_fail() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    use_addindexlit(&mut tk);
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");

    struct Restore(Option<usize>);
    impl Drop for Restore {
        fn drop(&mut self) {
            *tidb_ddl_ingest::config::IMPORTER_RANGE_CONCURRENCY_FOR_TEST.write().unwrap() = self.0;
        }
    }
    let _restore = Restore(tidb_ddl_ingest::config::IMPORTER_RANGE_CONCURRENCY_FOR_TEST.write().unwrap().replace(2));
    tk.must_exec("set @@tidb_ddl_reorg_worker_cnt = 20;");

    tk.must_exec("create table t (a int primary key);");
    let values: Vec<String> = (0..20).map(|i| format!("({i}000)")).collect();
    tk.must_exec(&format!("insert into t values {}", values.join(",")));
    must_query_check(&mut tk, "split table t between (0) and (20000) regions 20;", &["19 1"]);
    tk.must_exec("alter table t add index idx(a);");
    assert_last_job_ingest(&mut tk);
}

/// Go `TestAddIndexIngestEmptyTable`.
#[test]
fn test_add_index_ingest_empty_table() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    use_addindexlit(&mut tk);
    tk.must_exec("create table t (a int);");
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("alter table t add index idx(a);");
    assert_last_job_ingest(&mut tk);
}

/// Go `TestAddIndexIngestRestoredData`.
#[test]
fn test_add_index_ingest_restored_data() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    use_addindexlit(&mut tk);
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");

    tk.must_exec(
        "
		CREATE TABLE tbl_5 (
		  col_21 time DEFAULT '04:48:17',
		  col_22 varchar(403) COLLATE utf8_unicode_ci DEFAULT NULL,
		  col_23 year(4) NOT NULL,
		  col_24 char(182) CHARACTER SET gbk COLLATE gbk_chinese_ci NOT NULL,
		  col_25 set('Alice','Bob','Charlie','David') COLLATE utf8_unicode_ci DEFAULT NULL,
		  PRIMARY KEY (col_24(3)) /*T![clustered_index] CLUSTERED */,
		  KEY idx_10 (col_22)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
	",
    );
    tk.must_exec("INSERT INTO tbl_5 VALUES ('15:33:15','&U+x1',2007,'','Bob');");
    tk.must_exec("alter table tbl_5 add unique key idx_13 ( col_23 );");
    tk.must_exec("admin check table tbl_5;");
    assert_last_job_ingest(&mut tk);
}

/// Go `TestAddIndexIngestUniqueKey`.
#[test]
fn test_add_index_ingest_unique_key() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    use_addindexlit(&mut tk);
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");

    tk.must_exec("create table t (a int primary key, b int);");
    tk.must_exec("insert into t values (1, 1), (10000, 1);");
    tk.must_exec("split table t by (5000);");
    must_get_err_msg(&mut tk, "alter table t add unique index idx(b);", "[kv:1062]Duplicate entry '1' for key 't.idx'");

    tk.must_exec("drop table t;");
    tk.must_exec("create table t (a varchar(255) primary key, b int);");
    tk.must_exec("insert into t values ('a', 1), ('z', 1);");
    tk.must_exec("split table t by ('m');");
    must_get_err_msg(&mut tk, "alter table t add unique index idx(b);", "[kv:1062]Duplicate entry '1' for key 't.idx'");

    tk.must_exec("drop table t;");
    tk.must_exec("create table t (a varchar(255) primary key, b int, c char(5));");
    tk.must_exec("insert into t values ('a', 1, 'c1'), ('d', 2, 'c1'), ('x', 1, 'c2'), ('z', 1, 'c1');");
    tk.must_exec("split table t by ('m');");
    must_get_err_msg(&mut tk, "alter table t add unique index idx(b, c);", "[kv:1062]Duplicate entry '1-c1' for key 't.idx'");
}

/// Go `TestAddIndexSplitTableRanges`.
#[test]
fn test_add_index_split_table_ranges() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    use_addindexlit(&mut tk);
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");

    tk.must_exec("create table t (a int primary key, b int);");
    for i in 0..8 {
        tk.must_exec(&format!("insert into t values ({}, {});", i * 10000, i * 10000));
    }
    must_query_check(&mut tk, "split table t between (0) and (80000) regions 7;", &["6 1"]);

    let limit = "github.com/pingcap/tidb/pkg/ddl/setLimitForLoadTableRanges";
    let _fp = tidb_failpoint::enable_for_test(limit, "return(4)");
    tk.must_exec("alter table t add index idx(b);");
    tk.must_exec("admin check table t;");
    tidb_failpoint::enable(limit, "return(7)").unwrap();
    tk.must_exec("alter table t add index idx_2(b);");
    tk.must_exec("admin check table t;");

    tk.must_exec("drop table t;");
    tk.must_exec("create table t (a int primary key, b int);");
    for i in 0..8 {
        tk.must_exec(&format!("insert into t values ({}, {});", i * 10000, i * 10000));
    }
    must_query_check(&mut tk, "split table t by (10000),(20000),(30000),(40000),(50000),(60000);", &["6 1"]);
    tidb_failpoint::enable(limit, "return(4)").unwrap();
    tk.must_exec("alter table t add unique index idx(b);");
    tk.must_exec("admin check table t;");
}

/// Go `TestAddIndexLoadTableRangeError`.
#[test]
fn test_add_index_load_table_range_error() {
    let _restore = tidb_dxf_testutil::reduce_check_interval_for_test();
    let store = realtikv!();
    let mut tk = store.new_tk();
    use_addindexlit(&mut tk);
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("set global tidb_enable_dist_task=off;"); // Use checkpoint manager.

    tk.must_exec("create table t (a int primary key, b int);");
    for i in 0..8 {
        tk.must_exec(&format!("insert into t values ({}, {});", i * 10000, i * 10000));
    }
    must_query_check(&mut tk, "split table t by (10000),(20000),(30000),(40000),(50000),(60000);", &["6 1"]);

    let _limit = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/setLimitForLoadTableRanges", "return(3)");
    let batch_cnt = AtomicI32::new(0);
    let _pd = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeLoadRangeFromPD", move |mock_err| {
        if batch_cnt.fetch_add(1, Ordering::SeqCst) + 1 == 2 {
            *mock_err.downcast_mut::<bool>().unwrap() = true;
        }
    });
    let _sync = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/ingest/forceSyncFlagForTest", "return");
    tk.must_exec("alter table t add unique index idx(b);");
    tk.must_exec("admin check table t;");
}

/// Go `TestAddIndexMockFlushError`.
#[test]
fn test_add_index_mock_flush_error() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    use_addindexlit(&mut tk);
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("set global tidb_enable_dist_task = off;");

    tk.must_exec("create table t (a int primary key, b int);");
    for i in 0..4 {
        tk.must_exec(&format!("insert into t values ({}, {});", i * 10000, i * 10000));
    }
    {
        let _fp = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/ingest/mockFlushError", "1*return");
        tk.must_exec("alter table t add index idx(a);");
    }
    tk.must_exec("admin check table t;");
    let rows = tk.must_query("admin show ddl jobs 1;");
    assert!(rows[0][12].contains("ingest"), "{}", rows[0][12]);
}

/// Go `TestAddIndexDiskQuotaTS`.
#[test]
fn test_add_index_disk_quota_ts() {
    let _restore = tidb_dxf_testutil::reduce_check_interval_for_test();
    let store = realtikv!();
    let mut tk = store.new_tk();

    tk.must_exec("set @@global.tidb_enable_dist_task = 0;");
    add_index_disk_quota_ts(&mut tk);
    tk.must_exec("set @@global.tidb_enable_dist_task = 1;");
    add_index_disk_quota_ts(&mut tk);
}

/// Go `testAddIndexDiskQuotaTS`.
fn add_index_disk_quota_ts(tk: &mut Tk) {
    use_addindexlit(tk);
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("set @@tidb_ddl_reorg_worker_cnt=1;");

    tk.must_exec("create table t(id int primary key, b int, k int);");
    must_query_check(tk, "split table t by (30000);", &["1 1"]);
    tk.must_exec("insert into t values(1, 1, 1);");
    tk.must_exec("insert into t values(100000, 1, 1);");

    let _force = ForceSync::on();
    tk.must_exec("alter table t add index idx_test(b);");
    tk.must_exec("update t set b = b + 1;");

    let counter = Arc::new(AtomicI64::new(0));
    let mismatches = Arc::new(AtomicI64::new(0));
    let _start = {
        let counter = Arc::clone(&counter);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/wrapInBeginRollbackStartTS", move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
        })
    };
    let _after = {
        let counter = Arc::clone(&counter);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/wrapInBeginRollbackAfterFn", move |_| {
            counter.fetch_sub(1, Ordering::SeqCst);
        })
    };
    // Go's `assert` records a failure without stopping the import.
    let _ready = {
        let (counter, mismatches) = (Arc::clone(&counter), Arc::clone(&mismatches));
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ingestor/ingestctrl/ReadyForImportEngine", move |_| {
            if counter.load(Ordering::SeqCst) != 0 {
                mismatches.fetch_add(1, Ordering::SeqCst);
            }
        })
    };
    tk.must_exec("alter table t add index idx_test2(b);");
    assert_eq!(mismatches.load(Ordering::SeqCst), 0);
}

/// Go `TestAddIndexAdvanceWatermarkFailed`.
#[test]
fn test_add_index_advance_watermark_failed() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("set @@tidb_ddl_reorg_worker_cnt=1;");
    tk.must_exec("set @@global.tidb_enable_dist_task = 0;");

    tk.must_exec("create table t(id int primary key, b int, k int);");
    must_query_check(&mut tk, "split table t by (30000);", &["1 1"]);
    tk.must_exec("insert into t values(1, 1, 1);");
    tk.must_exec("insert into t values(100000, 1, 2);");
    let _force = ForceSync::on();
    let alloc = "github.com/pingcap/tidb/pkg/ddl/ingest/mockAfterImportAllocTSFailed";
    let _alloc = tidb_failpoint::enable_for_test(alloc, "2*return");
    tk.must_exec("alter table t add index idx(b);");
    tk.must_exec("admin check table t;");
    tk.must_exec("update t set b = b + 1;");

    tk.must_exec("alter table t drop index idx;");
    tidb_failpoint::enable(alloc, "2*return").unwrap();
    tk.must_get_err_code("alter table t add unique index idx(b);", tidb_errno::errcode::ErrDupEntry);

    tidb_failpoint::enable(alloc, "1*return").unwrap();
    let _set_ts = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ingestor/ingestctrl/afterSetTSBeforeImportEngine", "1*return");
    tk.must_get_err_code("alter table t add unique index idx(b);", tidb_errno::errcode::ErrDupEntry);
}

/// Go `TestAddIndexTempDirDataRemoved`: it removes the engine's SST files
/// before Pebble's `mergeSSTs`; the extsort engine merges no SST files.
#[test]
#[ignore = "Pebble's mergeSSTs: the extsort engine has no SST merge"]
fn test_add_index_temp_dir_data_removed() {}

/// Go `TestMockMemoryUsedUp`, which Go skips.
#[test]
#[ignore = "TODO(tangenta): support memory tracking later (Go t.Skip)"]
fn test_mock_memory_used_up() {}

/// Go `TestAddIndexRemoteDuplicateCheck`.
#[test]
fn test_add_index_remote_duplicate_check() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    use_addindexlit(&mut tk);
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("set @@tidb_ddl_reorg_worker_cnt=1;");
    tk.must_exec("set @@global.tidb_enable_dist_task = 0;");

    tk.must_exec("create table t(id int primary key, b int, k int);");
    must_query_check(&mut tk, "split table t by (30000);", &["1 1"]);
    tk.must_exec("insert into t values(1, 1, 1);");
    tk.must_exec("insert into t values(100000, 1, 1);");

    let _force = ForceSync::on();
    must_get_err_msg(&mut tk, "alter table t add unique index idx(b);", "[kv:1062]Duplicate entry '1' for key 't.idx'");
}

/// Go `TestAddIndexRecoverOnDuplicateCheck`.
#[test]
fn test_add_index_recover_on_duplicate_check() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("set @@global.tidb_ddl_enable_fast_reorg = on;");
    tk.must_exec("set @@global.tidb_enable_dist_task = on;");
    tk.must_exec("create table t (a int);");
    tk.must_exec("insert into t values (1), (2), (3);");

    let _fp = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/ingest/mockCollectRemoteDuplicateRowsFailed", "1*return");
    tk.must_exec("alter table t add unique index idx(a);");
}

/// Go `TestAddIndexBackfillLostUpdate`.
#[test]
fn test_add_index_backfill_lost_update() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    use_addindexlit(&mut tk);
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");

    tk.must_exec("create table t(id int primary key, b int, k int);");

    let tk1 = Arc::new(Mutex::new(store.new_tk()));
    tk1.lock().unwrap().must_exec("use addindexlit;");

    // Go's `assert` records a failure without stopping the job.
    let errors = Arc::new(Mutex::new(Vec::new()));
    let run_dml = Arc::new(AtomicBool::new(false));
    let _step = {
        let (tk1, errors, run_dml) = (Arc::clone(&tk1), Arc::clone(&errors), Arc::clone(&run_dml));
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", move |job| {
            if !errors.lock().unwrap().is_empty() || run_dml.load(Ordering::SeqCst) {
                return;
            }
            if job.downcast_ref::<tidb_model::Job>().unwrap().schema_state == tidb_model::SchemaState::WRITE_REORGANIZATION {
                if let Err(err) = tk1.lock().unwrap().exec("insert into t values (1, 1, 1);") {
                    errors.lock().unwrap().push(err);
                }
                // row: [h1 -> 1]
                // idx: []
                // tmp: [1 -> h1]
                run_dml.store(true, Ordering::SeqCst);
            }
        })
    };
    let run_dml_once = Once::new();
    let _after_reorg = {
        let (tk1, errors) = (Arc::clone(&tk1), Arc::clone(&errors));
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterRunReorgJobAndHandleErr", move |_| {
            run_dml_once.call_once(|| {
                let mut tk1 = tk1.lock().unwrap();
                for sql in [
                    "update t set b = 2 where id = 1;",
                    // row: [h1 -> 2]
                    // idx: [1 -> h1]
                    // tmp: [1 -> (h1,h1d), 2 -> h1]
                    "begin;",
                    "insert into t values (2, 1, 2);",
                    // row: [h1 -> 2, h2 -> 1]
                    // idx: [1 -> h1]
                    // tmp: [1 -> (h1,h1d,h2), 2 -> h1]
                    "delete from t where id = 2;",
                    // row: [h1 -> 2]
                    // idx: [1 -> h1]
                    // tmp: [1 -> (h1,h1d,h2,h2d), 2 -> h1]
                    "commit;",
                ] {
                    if let Err(err) = tk1.exec(sql) {
                        errors.lock().unwrap().push(err);
                    }
                }
            });
        })
    };
    tk.must_exec("alter table t add unique index idx(b);");
    tk.must_exec("admin check table t;");
    must_query_check(&mut tk, "select * from t;", &["1 2 1"]);
    assert!(errors.lock().unwrap().is_empty(), "{:?}", errors.lock().unwrap());
}

/// Go `TestAddIndexIngestFailures`.
#[test]
fn test_add_index_ingest_failures() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    use_addindexlit(&mut tk);
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");

    tk.must_exec("create table t(id int primary key, b int, k int);");
    tk.must_exec("insert into t values (1, 1, 1);");

    // Test precheck failed.
    {
        let _fp = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/ingest/mockIngestCheckEnvFailed", "1*return");
        must_get_err_msg(&mut tk, "alter table t add index idx(b);", "[ddl:8256]Check ingest environment failed: mock error");
    }

    tk.must_exec("set global tidb_enable_dist_task=on;");
    // Test reset engine failed.
    {
        let _fp = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/ingest/mockResetEngineFailed", "1*return");
        tk.must_exec("alter table t add index idx(b);");
    }
    tk.must_exec("set global tidb_enable_dist_task=off;");
}

/// Go `TestAddIndexImportFailed`.
#[test]
fn test_add_index_import_failed() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    use_addindexlit(&mut tk);
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("set global tidb_enable_dist_task=off;");

    tk.must_exec("create table t (a int, b int);");
    for i in 0..10 {
        tk.must_exec(&format!("insert into t values ({i}, {i})"));
    }

    {
        let _fp = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ingestor/ingestctrl/mockWritePeerErr", "1*return");
        tk.must_exec("alter table t add index idx(a);");
    }
    tk.must_exec("admin check table t;");
}

/// Go `TestAddEmptyMultiValueIndex`.
#[test]
fn test_add_empty_multi_value_index() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    use_addindexlit(&mut tk);
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("set global tidb_enable_dist_task=off;");

    tk.must_exec("create table t(j json);");
    tk.must_exec(r#"insert into t(j) values ('{"string":[]}');"#);
    tk.must_exec("alter table t add index ((cast(j->'$.string' as char(10) array)));");
    tk.must_exec("admin check table t;");
}

/// Go `TestAddUniqueIndexDuplicatedError`.
#[test]
fn test_add_unique_index_duplicated_error() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("DROP TABLE IF EXISTS `b1cce552` ");
    tk.must_exec("CREATE TABLE `b1cce552` (\n  `f5d9aecb` timestamp DEFAULT '2031-12-22 06:44:52',\n  `d9337060` varchar(186) DEFAULT 'duplicatevalue',\n  `4c74082f` year(4) DEFAULT '1977',\n  `9215adc3` tinytext DEFAULT NULL,\n  `85ad5a07` decimal(5,0) NOT NULL DEFAULT '68649',\n  `8c60260f` varchar(130) NOT NULL DEFAULT 'drfwe301tuehhkmk0jl79mzekuq0byg',\n  `8069da7b` varchar(90) DEFAULT 'ra5rhqzgjal4o47ppr33xqjmumpiiillh7o5ajx7gohmuroan0u',\n  `91e218e1` tinytext DEFAULT NULL,\n  PRIMARY KEY (`8c60260f`,`85ad5a07`) /*T![clustered_index] CLUSTERED */,\n  KEY `d88975e1` (`8069da7b`)\n);");
    tk.must_exec("INSERT INTO `b1cce552` (`f5d9aecb`, `d9337060`, `4c74082f`, `9215adc3`, `85ad5a07`, `8c60260f`, `8069da7b`, `91e218e1`) VALUES ('2031-12-22 06:44:52', 'duplicatevalue', 2028, NULL, 846, 'N6QD1=@ped@owVoJx', '9soPM2d6H', 'Tv%'), ('2031-12-22 06:44:52', 'duplicatevalue', 2028, NULL, 9052, '_HWaf#gD!bw', '9soPM2d6H', 'Tv%');");
    tk.must_get_err_code(
        "ALTER TABLE `b1cce552` ADD unique INDEX `65290727` (`4c74082f`, `d9337060`, `8069da7b`);",
        tidb_errno::errcode::ErrDupEntry,
    );
}

/// Go `TestFirstLitSlowStart`. Go's `slowCreateFS` slows Pebble's file
/// system; the extsort engine opens none.
#[test]
fn test_first_lit_slow_start() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    use_addindexlit(&mut tk);
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("set global tidb_enable_dist_task=off;");

    tk.must_exec("create table t(a int, b int);");
    tk.must_exec("insert into t values (1, 1), (2, 2), (3, 3);");
    tk.must_exec("create table t2(a int, b int);");
    tk.must_exec("insert into t2 values (1, 1), (2, 2), (3, 3);");

    let mut tk1 = store.new_tk();
    tk1.must_exec("use addindexlit;");

    let _create = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/ingest/beforeCreateLocalBackend", "1*return()");
    let _resign = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/ownerResignAfterDispatchLoopCheck", "return()");
    let _slow = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ingestor/ingestctrl/slowCreateFS", "return()");

    std::thread::scope(|wg| {
        wg.spawn(|| tk.must_exec("alter table t add unique index idx(a);"));
        wg.spawn(|| tk1.must_exec("alter table t2 add unique index idx(a);"));
    });
}

/// Go `TestConcFastReorg`.
#[test]
fn test_conc_fast_reorg() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    use_addindexlit(&mut tk);
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");

    let tbl_num = 10;
    for i in 0..tbl_num {
        tk.must_exec(&format!("create table t{i}(a int);"));
    }
    let tks: Vec<Tk> = (0..tbl_num).map(|_| store.new_tk()).collect();
    std::thread::scope(|wg| {
        for (i, mut tk2) in tks.into_iter().enumerate() {
            wg.spawn(move || {
                tk2.must_exec("use addindexlit;");
                tk2.must_exec(&format!("insert into t{i} values (1), (2), (3);"));
                if i % 2 == 0 {
                    tk2.must_exec(&format!("alter table t{i} add index idx(a);"));
                } else {
                    tk2.must_exec(&format!("alter table t{i} add unique index idx(a);"));
                }
            });
        }
    });
}

/// Go `TestIssue55808`.
#[test]
fn test_issue55808() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    use_addindexlit(&mut tk);
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("set global tidb_enable_dist_task = off;");
    tk.must_exec("set global tidb_ddl_error_count_limit = 0");

    let retry = &tidb_ingestor_ingestctrl::local::MAX_WRITE_AND_INGEST_RETRY_TIMES;
    let backup = retry.swap(1, Ordering::SeqCst);

    tk.must_exec("create table t (a int primary key, b int);");
    for i in 0..4 {
        tk.must_exec(&format!("insert into t values ({}, {});", i * 10000, i * 10000));
    }
    let err = {
        let _fp = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ingestor/ingestctrl/doIngestFailed", "return()");
        tk.exec_to_err("alter table t add index idx(a);")
    };
    retry.store(backup, Ordering::SeqCst);
    tk.must_exec("set global tidb_ddl_error_count_limit = default;");
    assert!(err.contains("injected error"), "{err}");
}

/// Go `TestAddIndexBackfillLostTempIndexValues`.
#[test]
fn test_add_index_backfill_lost_temp_index_values() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    use_addindexlit(&mut tk);
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("set global tidb_enable_dist_task = 0;");

    tk.must_exec("create table t(id int primary key, b int not null default 0);");
    tk.must_exec("insert into t values (1, 0);");

    let tk1 = Arc::new(Mutex::new(store.new_tk()));
    tk1.lock().unwrap().must_exec("use addindexlit;");
    // Go's `assert` records a failure without stopping the job.
    let errors = Arc::new(Mutex::new(Vec::new()));
    let exec = |tk1: &Mutex<Tk>, errors: &Mutex<Vec<String>>, sql: &str| {
        if let Err(err) = tk1.lock().unwrap().exec(sql) {
            errors.lock().unwrap().push(err);
        }
    };

    let _scan = {
        let (tk1, errors) = (Arc::clone(&tk1), Arc::clone(&errors));
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeAddIndexScan", move |_| {
            for sql in [
                "insert into t values (2, 0);",
                "delete from t where id = 1;",
                "insert into t values (3, 0);",
                "delete from t where id = 2;",
            ] {
                exec(&tk1, &errors, sql);
            }
        })
    };

    let before_ingest_once = Once::new();
    let _ingest = {
        let (tk1, errors) = (Arc::clone(&tk1), Arc::clone(&errors));
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/ingest/beforeBackendIngest", move |_| {
            before_ingest_once.call_once(|| {
                exec(&tk1, &errors, "insert into t(id) values (4);");
                exec(&tk1, &errors, "delete from t where id = 3;");
            });
        })
    };
    let rows = Arc::new(Mutex::new(Vec::new()));
    let before_merge_once = Once::new();
    let _merge = {
        let (tk1, errors, rows) = (Arc::clone(&tk1), Arc::clone(&errors), Arc::clone(&rows));
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeBackfillMerge", move |_| {
            before_merge_once.call_once(|| {
                *rows.lock().unwrap() = tk1.lock().unwrap().must_query("select * from t use index();");
                exec(&tk1, &errors, "insert into t values (3, 0);");
            });
        })
    };

    let _skip = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/skipReorgWorkForTempIndex", "return(false)");

    must_get_err_msg(&mut tk, "alter table t add unique index idx(b);", "[kv:1062]Duplicate entry '0' for key 't.idx'");
    let rows = rows.lock().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "4");

    tk.must_exec("admin check table t;");
    assert!(errors.lock().unwrap().is_empty(), "{:?}", errors.lock().unwrap());
}

/// Go `TestAddIndexInsertSameOriginIndexValue`.
#[test]
fn test_add_index_insert_same_origin_index_value() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    use_addindexlit(&mut tk);
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("set global tidb_enable_dist_task = 0;");

    tk.must_exec("create table t(id int primary key, b int not null default 0);");
    tk.must_exec("insert into t values (1, 0);");

    let tk1 = Arc::new(Mutex::new(store.new_tk()));
    tk1.lock().unwrap().must_exec("use addindexlit;");
    let errors = Arc::new(Mutex::new(Vec::new()));

    let before_ingest_once = Once::new();
    let _ingest = {
        let (tk1, errors) = (Arc::clone(&tk1), Arc::clone(&errors));
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/ingest/beforeBackendIngest", move |_| {
            before_ingest_once.call_once(|| {
                let mut tk1 = tk1.lock().unwrap();
                for sql in ["delete from t where id = 1;", "insert into t values (1, 0);"] {
                    if let Err(err) = tk1.exec(sql) {
                        errors.lock().unwrap().push(err);
                    }
                }
            });
        })
    };
    let before_merge_once = Once::new();
    let _merge = {
        let (tk1, errors) = (Arc::clone(&tk1), Arc::clone(&errors));
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeBackfillMerge", move |_| {
            before_merge_once.call_once(|| match tk1.lock().unwrap().exec("insert into t (id) values (1);") {
                Err(err) if err.contains("Duplicate entry") => {}
                other => errors.lock().unwrap().push(format!("expected a duplicate entry: {other:?}")),
            });
        })
    };
    tk.must_exec("alter table t add unique index idx(b);");
    assert!(errors.lock().unwrap().is_empty(), "{:?}", errors.lock().unwrap());
}

/// Go `TestIngestConcurrentJobCleanupRace`: parallel add index jobs don't
/// cause panic due to cleanup race (issues #44137 and #44140).
#[test]
fn test_ingest_concurrent_job_cleanup_race() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    use_addindexlit(&mut tk);
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("set global tidb_enable_dist_task = off;"); // Ensure both use local ingest

    // Create two tables with enough data to ensure ingest mode is used
    tk.must_exec("create table t1 (a int primary key, b int);");
    tk.must_exec("create table t2 (a int primary key, b int);");
    for i in 0..100 {
        tk.must_exec(&format!("insert into t1 values ({i}, {i});"));
        tk.must_exec(&format!("insert into t2 values ({i}, {i});"));
    }

    // Ensure both jobs enter ingest and overlap at the ingest stage.
    let (barrier_tx, barrier) = crossbeam_channel::bounded::<()>(0);
    let barrier_tx = Mutex::new(Some(barrier_tx));
    let ingest_calls = Arc::new(AtomicI32::new(0));
    let ingest_timed_out = Arc::new(AtomicBool::new(false));
    let _ingest = {
        let (ingest_calls, ingest_timed_out) = (Arc::clone(&ingest_calls), Arc::clone(&ingest_timed_out));
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/ingest/beforeBackendIngest", move |_| {
            if ingest_calls.fetch_add(1, Ordering::SeqCst) + 1 == 2 {
                // close(ingestBarrier)
                barrier_tx.lock().unwrap().take();
            }
            if barrier.recv_timeout(std::time::Duration::from_secs(10)) == Err(crossbeam_channel::RecvTimeoutError::Timeout) {
                ingest_timed_out.store(true, Ordering::SeqCst);
            }
        })
    };

    // Start two add index jobs concurrently
    let mut tk1 = store.new_tk();
    tk1.must_exec("use addindexlit;");
    let mut tk2 = store.new_tk();
    tk2.must_exec("use addindexlit;");
    let (err1, err2) = std::thread::scope(|wg| {
        let a = wg.spawn(|| tk1.exec("alter table t1 add index idx1(b);"));
        let b = wg.spawn(|| tk2.exec("alter table t2 add index idx2(b);"));
        (a.join().unwrap(), b.join().unwrap())
    });

    // Verify no panic occurred and both indexes created successfully
    err1.unwrap();
    err2.unwrap();

    assert!(!ingest_timed_out.load(Ordering::SeqCst), "ingest jobs did not overlap");
    assert!(ingest_calls.load(Ordering::SeqCst) >= 2, "both jobs should enter ingest");

    // Verify both jobs used ingest mode
    let rows = tk.must_query("admin show ddl jobs 2;");
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert!(row[12].contains("ingest"), "both jobs should use ingest mode: {}", row[12]);
    }

    tk.must_exec("admin check table t1;");
    tk.must_exec("admin check table t2;");
}

/// Go `TestIngestGCSafepointBlocking`: add index uses the TS from the
/// checkpoint and registers its internal session's start TS (issues #40074
/// and #40081).
#[test]
fn test_ingest_gc_safepoint_blocking() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    use_addindexlit(&mut tk);
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("set global tidb_enable_dist_task = off;");

    tk.must_exec("create table t (a int primary key, b int);");
    for i in 0..100 {
        tk.must_exec(&format!("insert into t values ({i}, {i});"));
    }

    // Track that internal session TS is registered for GC blocking
    let internal_ts_registered = Arc::new(AtomicBool::new(false));
    let _start = {
        let registered = Arc::clone(&internal_ts_registered);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/wrapInBeginRollbackStartTS", move |start_ts| {
            if *start_ts.downcast_ref::<u64>().unwrap() > 0 {
                registered.store(true, Ordering::SeqCst);
            }
        })
    };

    // Force sync mode to ensure we go through the checkpoint/TS path
    let _force = ForceSync::on();

    tk.must_exec("alter table t add index idx(b);");

    // Assert that internal session TS was registered (this is what blocks GC)
    assert!(internal_ts_registered.load(Ordering::SeqCst), "internal session start TS should be registered for GC blocking");

    tk.must_exec("admin check table t;");

    // Verify data consistency
    let rs1 = tk.must_query("select count(*) from t use index(idx);");
    let rs2 = tk.must_query("select count(*) from t ignore index(idx);");
    assert_eq!(rs1[0][0], rs2[0][0]);
    assert_eq!(rs1[0][0], "100");

    // Verify the job used ingest mode
    assert_last_job_ingest(&mut tk);
}

/// Go `TestIngestCancelCleanupOrder`: canceling add index after ingest starts
/// doesn't panic (issues #43323 and #43326).
#[test]
fn test_ingest_cancel_cleanup_order() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    use_addindexlit(&mut tk);
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("set global tidb_enable_dist_task = off;");

    tk.must_exec("create table t (a int primary key, b int);");
    for i in 0..100 {
        tk.must_exec(&format!("insert into t values ({i}, {i});"));
    }

    let tk_cancel = Mutex::new(store.new_tk());
    tk_cancel.lock().unwrap().must_exec("use addindexlit;");

    // Cancel after backfill starts (WriteReorganization state with running backfill)
    let job_id = Arc::new(AtomicI64::new(0));
    let backfill_started = Arc::new(AtomicBool::new(false));
    let _delivery = {
        let job_id = Arc::clone(&job_id);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeDeliveryJob", move |job| {
            let job = job.downcast_ref::<tidb_model::Job>().unwrap();
            if job.type_ == tidb_model::ActionType::ACTION_ADD_INDEX {
                job_id.store(job.id, Ordering::SeqCst);
            }
        })
    };
    let cancel_once = Once::new();
    let cancel_issued = Arc::new(AtomicBool::new(false));
    let (cancel_err_tx, cancel_err) = std::sync::mpsc::sync_channel::<Result<(), String>>(1);
    let _ingest = {
        let (job_id, backfill_started, cancel_issued) =
            (Arc::clone(&job_id), Arc::clone(&backfill_started), Arc::clone(&cancel_issued));
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/ingest/beforeBackendIngest", move |_| {
            backfill_started.store(true, Ordering::SeqCst);
            let id = job_id.load(Ordering::SeqCst);
            if id <= 0 {
                return;
            }
            cancel_once.call_once(|| {
                cancel_issued.store(true, Ordering::SeqCst);
                let result = tk_cancel.lock().unwrap().exec(&format!("admin cancel ddl jobs {id}")).map(drop);
                let _ = cancel_err_tx.try_send(result);
            });
        })
    };

    let err = tk.exec_to_err("alter table t add index idx(b);");
    assert!(err.contains("Cancelled DDL job"), "{err}");

    // Key assertion: backfill actually started before cancel
    assert!(backfill_started.load(Ordering::SeqCst), "backfill should have started before cancel");
    assert!(cancel_issued.load(Ordering::SeqCst), "cancel should be issued after ingest starts");
    cancel_err
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("cancel exec should have finished")
        .unwrap();

    // Verify no panic occurred and table is consistent
    tk.must_exec("admin check table t;");

    // Verify backend context is cleaned up properly
    assert_eq!(lit_env().disk_root.count(), 0, "backend context should be cleaned up after cancel");
}

/// Go `TestMergeTempIndexSplitConflictTxn`.
#[test]
fn test_merge_temp_index_split_conflict_txn() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    use_addindexlit(&mut tk);
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("set global tidb_enable_dist_task = off;");

    tk.must_exec("create table t (a int primary key, b int);");

    let tk1 = Arc::new(Mutex::new(store.new_tk()));
    tk1.lock().unwrap().must_exec("use addindexlit;");
    let errors = Arc::new(Mutex::new(Vec::new()));

    let run_insert = Arc::new(AtomicBool::new(false));
    let _synced = {
        let (tk1, errors, run_insert) = (Arc::clone(&tk1), Arc::clone(&errors), Arc::clone(&run_insert));
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", move |job| {
            if !errors.lock().unwrap().is_empty() || run_insert.load(Ordering::SeqCst) {
                return;
            }
            if job.downcast_ref::<tidb_model::Job>().unwrap().schema_state == tidb_model::SchemaState::WRITE_REORGANIZATION {
                for i in 0..4 {
                    if let Err(err) = tk1.lock().unwrap().exec(&format!("insert into t values ({i}, {i});")) {
                        errors.lock().unwrap().push(err);
                    }
                }
                run_insert.store(true, Ordering::SeqCst);
            }
        })
    };

    let run_update = Arc::new(AtomicBool::new(false));
    let _merging = {
        let (tk1, errors, run_update) = (Arc::clone(&tk1), Arc::clone(&errors), Arc::clone(&run_update));
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/mockDMLExecutionMergingInTxn", move |_| {
            if !errors.lock().unwrap().is_empty() || run_update.load(Ordering::SeqCst) {
                return;
            }
            for i in 0..4 {
                if let Err(err) = tk1.lock().unwrap().exec(&format!("update t set b = b+10 where a = {i};")) {
                    errors.lock().unwrap().push(err);
                }
            }
            run_update.store(true, Ordering::SeqCst);
        })
    };

    tk.must_exec("alter table t add index idx(b);");
    tk.must_exec("admin check table t;");
    must_query_check(&mut tk, "select * from t;", &["0 10", "1 11", "2 12", "3 13"]);
    assert!(errors.lock().unwrap().is_empty(), "{:?}", errors.lock().unwrap());
}

// Go `temp_index_test.go`.

/// Go `TestMergeTempIndexBasic`'s cases.
struct MergeTempIndexCase {
    name: &'static str,
    create_table: &'static str,
    create_index: &'static str,
    admin_check: &'static str,
    init_op: &'static [&'static str],
    incr_op: &'static [&'static str],
    read_idx_row_cnt: &'static [&'static str],
    merge_idx_cnt: &'static [&'static str],
    expected_err: &'static str,
}

/// Go `TestMergeTempIndexBasic`.
#[test]
fn test_merge_temp_index_basic() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("set sql_mode=''");
    // Go's run reaches this test with the DXF on, by its package's test
    // order; nextest runs the tests in another order, after tests that turn
    // it off.
    tk.must_exec("set global tidb_enable_dist_task = on;");

    let test_cases = [
        MergeTempIndexCase {
            name: "basic",
            create_table: "create table t (a int primary key, b int);",
            create_index: "create index idx on t(b);",
            admin_check: "admin check index t idx;",
            init_op: &["insert into t values (1, 1);"],
            incr_op: &["insert into t values (2, 2), (3, 3);"],
            read_idx_row_cnt: &["1"],
            merge_idx_cnt: &["2"],
            expected_err: "",
        },
        MergeTempIndexCase {
            name: "unique index",
            create_table: "create table t (a int primary key, b int);",
            create_index: "create unique index idx on t(b);",
            admin_check: "admin check index t idx;",
            init_op: &["insert into t values (1, 1);"],
            incr_op: &["insert into t values (2, 1);"],
            expected_err: "[kv:1062]Duplicate entry '1' for key 't.idx'",
            read_idx_row_cnt: &["1"],
            merge_idx_cnt: &["0"],
        },
        MergeTempIndexCase {
            name: "partitioned table",
            create_table: "create table t (a int primary key, b int) partition by hash(a) partitions 3;",
            create_index: "create index idx on t(b);",
            admin_check: "admin check index t idx;",
            init_op: &["insert into t values (1, 1), (2, 2), (3, 3), (4, 4);"],
            incr_op: &["insert into t values (5, 5), (6, 6), (7, 7);"],
            read_idx_row_cnt: &["1", "2", "1"],
            merge_idx_cnt: &["1", "1", "1"],
            expected_err: "",
        },
        MergeTempIndexCase {
            name: "global index on partitioned table",
            create_table: "create table t (a int primary key, b int) partition by hash(a) partitions 3;",
            create_index: "create index idx on t(b) global;",
            admin_check: "admin check index t idx;",
            init_op: &["insert into t values (1, 1), (2, 2), (3, 3), (4, 4);"],
            incr_op: &["insert into t values (5, 5), (6, 6), (7, 7);"],
            read_idx_row_cnt: &["1", "2", "1"],
            merge_idx_cnt: &["3"],
            expected_err: "",
        },
        MergeTempIndexCase {
            name: "multi-schema change",
            create_table: "create table t (a int primary key, b int, c int);",
            create_index: "alter table t add index idx(b), add index idx2(c);",
            admin_check: "admin check table t;",
            init_op: &["insert into t values (1, 1, 1);"],
            incr_op: &["insert into t values (2, 2, 2), (3, 3, 3);"],
            read_idx_row_cnt: &["1"],
            merge_idx_cnt: &["2", "2"],
            expected_err: "",
        },
        MergeTempIndexCase {
            name: "modify column with index covered",
            create_table: "create table t (a int primary key, b int, c int, index idx(b));",
            create_index: "alter table t modify column b smallint;",
            admin_check: "admin check table t;",
            init_op: &["insert into t values (1, 1, 1);"],
            incr_op: &["insert into t values (2, 2, 2), (3, 3, 3);"],
            read_idx_row_cnt: &["1"],
            merge_idx_cnt: &["2"],
            expected_err: "",
        },
    ];

    for tc in test_cases {
        tk.must_exec("drop database if exists test;");
        tk.must_exec("create database test;");
        tk.must_exec("use test;");
        tk.must_exec(
            "create view all_global_tasks as select * from mysql.tidb_global_task union all select * from mysql.tidb_global_task_history;",
        );
        tk.must_exec(
            "create view all_subtasks as select * from mysql.tidb_background_subtask union all select * from mysql.tidb_background_subtask_history;",
        );

        tk.must_exec(tc.create_table);
        for data in tc.init_op {
            tk.must_exec(data);
        }

        let job_id = Arc::new(AtomicI64::new(0));
        let _step = {
            let job_id = Arc::clone(&job_id);
            tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", move |job| {
                let job = job.downcast_ref::<tidb_model::Job>().unwrap();
                if job_id.load(Ordering::SeqCst) == 0
                    || job.type_ == tidb_model::ActionType::ACTION_ADD_INDEX
                    || job.type_ == tidb_model::ActionType::ACTION_MODIFY_COLUMN
                    || job.type_ == tidb_model::ActionType::ACTION_MULTI_SCHEMA_CHANGE
                {
                    job_id.store(job.id, Ordering::SeqCst);
                }
            })
        };
        let run_insert = Arc::new(AtomicBool::new(false));
        let tk2 = Mutex::new(store.new_tk());
        let _ingest = {
            let run_insert = Arc::clone(&run_insert);
            let incr_op = tc.incr_op;
            tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/ingest/beforeBackendIngest", move |_| {
                if !run_insert.load(Ordering::SeqCst) {
                    let mut tk2 = tk2.lock().unwrap();
                    tk2.must_exec("use test");
                    for data in incr_op {
                        tk2.must_exec(data);
                    }
                    run_insert.store(true, Ordering::SeqCst);
                }
            })
        };
        if tc.expected_err.is_empty() {
            tk.must_exec(tc.create_index);
            tk.must_exec(tc.admin_check);
        } else {
            must_get_err_msg(&mut tk, tc.create_index, tc.expected_err);
        }

        let task_key = tidb_ddl::index::TaskKeyBuilder::new().build(job_id.load(Ordering::SeqCst));

        assert!(run_insert.load(Ordering::SeqCst), "{}: never ran incrOp: {}", tc.name, tc.incr_op[0]);
        let query = format!("select id from all_global_tasks where task_key like '%{task_key}%' order by id");
        let task_id_rows = tk.must_query(&query);
        assert_eq!(task_id_rows.len(), 2, "{}: {query}", tc.name);
        let (task_id, merge_task_id) = (&task_id_rows[0][0], &task_id_rows[1][0]);
        let read_idx_cnt_sql =
            format!("select json_extract(summary, '$.row_count') from all_subtasks where task_key = {task_id} and step = 1");
        must_query_check(&mut tk, &read_idx_cnt_sql, tc.read_idx_row_cnt);
        let merge_cnt_sql =
            format!("select json_extract(summary, '$.row_count') from all_subtasks where task_key = {merge_task_id} and step = 4");
        must_query_check(&mut tk, &merge_cnt_sql, tc.merge_idx_cnt);
    }
}

/// Go `TestMergeTempIndexStuck`.
#[test]
fn test_merge_temp_index_stuck() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t(id int primary key, a bigint)");
    let (worker_num, batch_num, pk_begin, pk_end, row_num) = (5, 10, 0, 50, 100);
    let exec_cnt = AtomicI64::new(0);
    let values: Vec<String> = (0..row_num).map(|i| format!("({i}, {i})")).collect();
    tk.must_exec(&format!("insert into t values {}", values.join(",")));
    let get_sql_stmt = |pk: i32, val_num: i32| {
        let vals: Vec<String> = (pk..pk + val_num)
            .map(|i| {
                let a = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as i64;
                format!("({i}, {a})")
            })
            .collect();
        format!("INSERT INTO t (id, a) VALUES {} ON DUPLICATE KEY UPDATE `a` = VALUES(`a`);", vals.join(", "))
    };
    // start workload
    let (ch_pk_tx, ch_pk) = crossbeam_channel::bounded::<i32>(worker_num);
    let (ch_pk_finish_tx, ch_pk_finish) = crossbeam_channel::bounded::<i32>(worker_num);
    let (done_tx, done) = crossbeam_channel::bounded::<()>(0);
    let tks: Vec<Tk> = (0..worker_num).map(|_| store.new_tk()).collect();
    std::thread::scope(|wg| {
        for mut tk in tks {
            let (ch_pk, ch_pk_finish_tx, done, exec_cnt, get_sql_stmt) =
                (ch_pk.clone(), ch_pk_finish_tx.clone(), done.clone(), &exec_cnt, &get_sql_stmt);
            tk.must_exec("use test");
            wg.spawn(move || loop {
                crossbeam_channel::select! {
                    recv(ch_pk) -> pk => {
                        let Ok(pk) = pk else { return };
                        tk.must_exec(&get_sql_stmt(pk, batch_num));
                        crossbeam_channel::select! {
                            send(ch_pk_finish_tx, pk) -> _ => { exec_cnt.fetch_add(1, Ordering::SeqCst); }
                            recv(done) -> _ => return,
                        }
                    }
                    recv(done) -> _ => return,
                }
            });
        }
        let mut pk = pk_begin;
        while pk < pk_end {
            ch_pk_tx.send(pk).unwrap();
            pk += batch_num;
        }
        {
            let (ch_pk_tx, done) = (ch_pk_tx.clone(), done.clone());
            wg.spawn(move || loop {
                crossbeam_channel::select! {
                    recv(ch_pk_finish) -> pk => {
                        let Ok(pk) = pk else { return };
                        crossbeam_channel::select! {
                            send(ch_pk_tx, pk) -> _ => {}
                            recv(done) -> _ => return,
                        }
                    }
                    recv(done) -> _ => return,
                }
            });
        }

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while exec_cnt.load(Ordering::SeqCst) < 5000 {
            assert!(std::time::Instant::now() < deadline, "the workload ran {} batches", exec_cnt.load(Ordering::SeqCst));
            std::thread::sleep(std::time::Duration::from_millis(300));
        }
        tk.must_exec("alter table t add index idx_a(a);");
        tk.must_exec("admin check index t idx_a;");
        // stopWorkload: close(done)
        drop(done_tx);
    });
    tk.must_exec("drop table t");
}

// Go `functional_test.go`.

/// Go `TestTiDBEncodeKeyTempIndexKey`.
#[test]
#[ignore = "TIDB_ENCODE_INDEX_KEY is not ported (register row E26)"]
fn test_tidb_encode_key_temp_index_key() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("create table t (a int primary key, b int);");
    tk.must_exec("insert into t values (1, 1);");
    let run_dml = Arc::new(AtomicBool::new(false));
    let tk2 = Mutex::new(store.new_tk());
    let _step = {
        let run_dml = Arc::clone(&run_dml);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", move |job| {
            let job = job.downcast_ref::<tidb_model::Job>().unwrap();
            if !run_dml.load(Ordering::SeqCst)
                && job.type_ == tidb_model::ActionType::ACTION_ADD_INDEX
                && job.schema_state == tidb_model::SchemaState::WRITE_ONLY
            {
                let mut tk2 = tk2.lock().unwrap();
                tk2.must_exec("use test");
                tk2.must_exec("insert into t values (2, 2);");
                run_dml.store(true, Ordering::SeqCst);
            }
        })
    };
    tk.must_exec("create index idx on t(b);");
    assert!(run_dml.load(Ordering::SeqCst));

    let rs = tk.must_query("select tidb_mvcc_info(tidb_encode_index_key('test', 't', 'idx', 1, 1));")[0][0].clone();
    assert_eq!(rs.matches("writes").count(), 1, "{rs}");
    let rs = tk.must_query("select tidb_mvcc_info(tidb_encode_index_key('test', 't', 'idx', 2, 2));")[0][0].clone();
    assert_eq!(rs.matches("writes").count(), 2, "{rs}");
}

/// Go `TestAddIndexPresplitIndexRegions`.
#[test]
fn test_add_index_presplit_index_regions() {
    let _restore = tidb_dxf_testutil::reduce_check_interval_for_test();
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");

    let split_key_hex: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let _presplit = {
        let split_key_hex = Arc::clone(&split_key_hex);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforePresplitIndex", move |split_keys| {
            split_key_hex.lock().unwrap().extend(split_keys.downcast_ref::<Vec<Vec<u8>>>().unwrap().iter().cloned());
        })
    };
    let check_split_keys = |idx_id: i64, count: usize, reset: bool| {
        let mut keys = split_key_hex.lock().unwrap();
        let cnt = keys
            .iter()
            .filter(|k| tidb_tablecodec::table_key::decode_index_id(k).is_ok_and(|index_id| index_id == idx_id))
            .count();
        assert_eq!(cnt, count, "{keys:?}");
        if reset {
            keys.clear();
        }
    };
    let mut idx_id = 0;
    let mut next_idx_id = || {
        idx_id += 1;
        idx_id
    };
    let temp = tidb_tablecodec::table_key::TEMP_INDEX_PREFIX;

    tk.must_exec("create table t (a int primary key, b int);");
    for i in 0..10 {
        tk.must_exec(&format!("insert into t values ({0}, {0});", 10000 * i));
    }
    assert_eq!(tk.must_query("show table t regions;").len(), 1);
    tk.must_exec("set @@global.tidb_ddl_enable_fast_reorg = off;");
    tk.must_exec("set @@global.tidb_enable_dist_task = off;");
    tk.must_exec("alter table t add index idx(b) pre_split_regions = 4;");
    check_split_keys(next_idx_id(), 3, true);
    tk.must_exec("drop index idx on t;");
    tk.must_exec("alter table t add index idx(b) pre_split_regions = (by (10000), (20000), (30000));");
    check_split_keys(next_idx_id(), 3, true);
    tk.must_exec("drop index idx on t;");
    tk.must_exec("alter table t add index idx(b) /*T![pre_split] pre_split_regions = (by (10000), (20000), (30000)) */;");
    check_split_keys(next_idx_id(), 3, true);
    tk.must_exec("drop index idx on t;");
    tk.must_exec("alter table t add index idx(b) pre_split_regions = (between (0) and (10 * 10000) regions 3);");
    check_split_keys(next_idx_id(), 2, true);
    tk.must_exec("drop index idx on t;");
    tk.must_exec("set @@global.tidb_ddl_enable_fast_reorg = on;");

    tk.must_exec("alter table t add index idx(b) pre_split_regions = (by (10000), (20000), (30000));");
    let next_id = next_idx_id();
    check_split_keys(next_id, 0, false);
    check_split_keys(temp | next_id, 3, true);

    tk.must_exec("set @@global.tidb_ddl_enable_fast_reorg = off;");

    // Test partition tables.
    idx_id = 0;
    let mut next_idx_id = || {
        idx_id += 1;
        idx_id
    };
    tk.must_exec("drop table t;");
    tk.must_exec("create table t (a int primary key, b int) partition by hash(a) partitions 4;");
    for i in 0..10 {
        tk.must_exec(&format!("insert into t values ({0}, {0});", 10000 * i));
    }
    tk.must_exec("alter table t add index idx(b) pre_split_regions = (by (10000), (20000), (30000));");
    check_split_keys(next_idx_id(), 3 * 4, true);
    tk.must_exec("drop index idx on t;");
    tk.must_exec("alter table t add index idx(b) pre_split_regions = (between (0) and (10 * 10000) regions 3);");
    check_split_keys(next_idx_id(), 2 * 4, true);
    tk.must_exec("drop index idx on t;");
    tk.must_exec("set @@global.tidb_ddl_enable_fast_reorg = on;");
    tk.must_exec("alter table t add index idx(b) pre_split_regions = (by (10000), (20000), (30000));");
    check_split_keys(next_idx_id(), 0, false);
    check_split_keys(temp | 3, 12, true);
    tk.must_exec("drop index idx on t;");
    tk.must_exec("set @@global.tidb_ddl_enable_fast_reorg = off;");

    idx_id = 0;
    tk.must_exec("drop table t;");
    tk.must_exec("set @@global.tidb_ddl_enable_fast_reorg = on;");
    tk.must_exec("set @@global.tidb_enable_dist_task = off;");
    tk.must_exec(
        "create table t (a int, b int) partition by range (b) (partition p0 values less than (10),   partition p1 values less than (maxvalue));",
    );
    tk.must_exec("alter table t add unique index p_a (a) global pre_split_regions = (by (5), (15));");
    idx_id += 1;
    check_split_keys(temp | idx_id, 2, true);
}

/// Go `TestAddIndexPresplitFunctional`.
#[test]
fn test_add_index_presplit_functional() {
    let _restore = tidb_dxf_testutil::reduce_check_interval_for_test();
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("create table t (a int primary key, b int);");

    must_get_err_msg(
        &mut tk,
        "alter table t add index idx(b) pre_split_regions = (between (0) and (10 * 10000) regions 0);",
        "Split index region num should be greater than 0",
    );
    must_get_err_msg(
        &mut tk,
        "alter table t add index idx(b) pre_split_regions = (between (0) and (10 * 10000) regions 10000);",
        "Split index region num exceeded the limit 1000",
    );
    {
        let _fp = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/mockSplitIndexRegionAndWaitErr", "2*return");
        tk.must_exec("alter table t add index idx(b) pre_split_regions = (between (0) and (10 * 10000) regions 3);");
    }

    tk.must_exec("drop table t;");
    tk.must_exec("create table t (a bigint primary key, b int);");
    tk.must_exec("insert into t values (1, 1), (10, 1);");
    tk.must_exec("alter table t add index idx(b) pre_split_regions = (between (1) and (2) regions 3);");
    tk.must_exec("drop table t;");
}

// Go `functional_test.go`.

/// Go `TestDDLTestEstimateTableRowSize`.
#[test]
fn test_ddl_test_estimate_table_row_size() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test;");
    tk.must_exec("create table t (a int, b int);");
    tk.must_exec("insert into t values (1, 1);");

    let table_id = |tk: &mut Tk| -> i64 {
        tk.must_query("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 't'")[0][0]
            .parse()
            .unwrap()
    };
    let estimate = |id: i64| tidb_ddl::index::estimate_table_row_size(store.tikv_store(), store.ddl_session_pool().as_ref(), id);
    let id = table_id(&mut tk);
    assert_eq!(estimate(id), 0); // No data in information_schema.columns.
    tk.must_exec("analyze table t all columns;");
    assert_eq!(estimate(id), 16);

    tk.must_exec("alter table t add column c varchar(255);");
    tk.must_exec("update t set c = repeat('a', 50) where a = 1;");
    tk.must_exec("analyze table t all columns;");
    assert_eq!(estimate(id), 67);

    tk.must_exec("drop table t;");
    tk.must_exec("create table t (id bigint primary key, b text) partition by hash(id) partitions 4;");
    for i in 1..10 {
        tk.must_exec(&format!("insert into t values ({}, repeat('a', 10))", i * 10000));
    }
    assert_eq!(tk.must_query("split table t between (0) and (1000000) regions 2;"), vec![vec!["4".to_owned(), "1".to_owned()]]);
    tk.must_exec("set global tidb_analyze_skip_column_types=`json,blob,mediumblob,longblob`");
    tk.must_exec("analyze table t all columns;");
    let id = table_id(&mut tk);
    assert_eq!(estimate(id), 19);
    let pids = tk.must_query("select tidb_partition_id from information_schema.partitions where table_schema = 'test' and table_name = 't'");
    assert_eq!(pids.len(), 4);
    // Go estimates a partition by its `Meta().ID`, which is the partitioned
    // table's.
    for _ in pids {
        assert_eq!(estimate(id), 19);
    }
}

/// Go `TestBackendCtxConcurrentUnregister`.
#[test]
fn test_backend_ctx_concurrent_unregister() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test;");
    tk.must_exec("create table t (a int);");
    let real_job = Arc::new(Mutex::new(None::<tidb_model::Job>));
    let _synced = {
        let real_job = Arc::clone(&real_job);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", move |job| {
            let job = job.downcast_mut::<tidb_model::Job>().unwrap();
            if job.state == tidb_model::JobState::DONE && job.type_ == tidb_model::ActionType::ACTION_ADD_INDEX {
                *real_job.lock().unwrap() = Some(job.clone());
            }
        })
    };
    tk.must_exec("alter table t add index idx(a);");
    let real_job = real_job.lock().unwrap().clone().expect("the add index job");

    let concurrency =
        real_job.reorg_meta.as_ref().unwrap().read().get_concurrency(tidb_ddl::index::reorg_defaults()) as usize;
    let bd = tidb_ddl_ingest::backend_mgr::create_local_backend(store.tikv_store().clone(), &real_job, false, false, concurrency, 0)
        .unwrap();
    let ctx = tidb_gocontext::Context::background();
    let b_ctx = tidb_ddl_ingest::backend_mgr::BackendCtxBuilder::new(ctx.clone(), store.tikv_store().clone(), real_job)
        .build(Some(Arc::clone(&bd)))
        .unwrap();
    let idx_ids = [1, 2, 3, 4, 5, 6, 7];
    let uniques = vec![false; idx_ids.len()];
    b_ctx.register(&idx_ids, &uniques, &tidb_model::TableInfo::default()).unwrap();

    std::thread::scope(|scope| {
        for _ in 0..3 {
            scope.spawn(|| {
                b_ctx.finish_and_unregister_engines(&ctx, tidb_ddl_ingest::backend::UnregisterOpt::CLOSE_ENGINES).unwrap();
            });
        }
    });
    b_ctx.close();
    bd.close().unwrap();
}
