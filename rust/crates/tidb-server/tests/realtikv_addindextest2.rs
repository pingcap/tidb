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

//! Go `tests/realtikvtest/addindextest2`, over [`crate::realtikvtest`]:
//! `alter_job_test.go` and `global_sort_test.go`. Go sorts in a fake GCS
//! server; these tests sort in a local `file://` directory.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Once};

type Pipe = Arc<operator::AsyncPipeline>;

/// Go `TestAlterJobOnDXF`.
#[test]
fn test_alter_job_on_dxf() {
    let _cpu = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(16)");
    let _restore = tidb_dxf_testutil::reduce_check_interval_for_test();
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("drop database if exists test;");
    tk.must_exec("create database test;");
    tk.must_exec("use test;");
    tk.must_exec("set global tidb_enable_dist_task=1;");
    tk.must_exec("create table t1(a bigint auto_random primary key);");
    for _ in 0..16 {
        tk.must_exec("insert into t1 values (), (), (), ()");
    }
    tk.must_exec("split table t1 between (3) and (8646911284551352360) regions 50;");
    tk.must_exec("set @@tidb_ddl_reorg_worker_cnt = 1");
    tk.must_exec("set @@tidb_ddl_reorg_batch_size = 32");
    tk.must_exec(r#"set @@global.tidb_cloud_storage_uri = """#);
    tk.must_exec("set global tidb_ddl_reorg_max_write_speed = 16");

    // Go's `require` runs in the hooks; here they record what they saw and
    // the test's thread asserts it.
    let pipe_sizes = Arc::new(Mutex::new(None::<(i32, i32)>));
    let _closed = {
        let pipe_sizes = Arc::clone(&pipe_sizes);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterPipeLineClose", move |pipe| {
            let pipe = pipe.downcast_ref::<Pipe>().unwrap();
            let (reader, writer) = pipe.get_reader_and_writer();
            *pipe_sizes.lock().unwrap() =
                Some((reader.unwrap().worker_pool_size(), writer.unwrap().worker_pool_size()));
        })
    };
    let finished_subtasks = Arc::new(Mutex::new(Vec::<i64>::new()));
    let _finish = {
        let finished_subtasks = Arc::clone(&finished_subtasks);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/mockDMLExecutionAddIndexSubTaskFinish", move |be| {
            let be = be.downcast_ref::<Option<Arc<tidb_ingestor_ingestctrl::local::Backend>>>().unwrap();
            finished_subtasks.lock().unwrap().push(be.as_ref().unwrap().get_write_speed_limit());
        })
    };
    let modified = Arc::new(AtomicBool::new(false));
    let _modify = {
        let modified = Arc::clone(&modified);
        tidb_failpoint::enable_call_for_test(
            "github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/afterDetectAndHandleParamModify",
            move |_| modified.store(true, Ordering::SeqCst),
        )
    };
    let once = Once::new();
    let batch_size_after_alter = Arc::new(Mutex::new(None::<i64>));
    let tk1 = Mutex::new(store.new_tk());
    let _scan = {
        let (modified, batch_size_after_alter) = (Arc::clone(&modified), Arc::clone(&batch_size_after_alter));
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/scanRecordExec", move |reorg_meta| {
            let reorg_meta = reorg_meta.downcast_ref::<tidb_model::GoShared<tidb_model::DDLReorgMeta>>().unwrap().clone();
            once.call_once(|| {
                let mut tk1 = tk1.lock().unwrap();
                let rows = tk1.must_query("select job_id from mysql.tidb_ddl_job");
                assert_eq!(rows.len(), 1);
                tk1.must_exec(&format!("admin alter ddl jobs {} thread = 8, batch_size = 256, max_write_speed=1024", rows[0][0]));
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
                while !modified.load(Ordering::SeqCst) {
                    assert!(std::time::Instant::now() < deadline, "the task executor saw the modification");
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
                *batch_size_after_alter.lock().unwrap() =
                    Some(reorg_meta.read().get_batch_size(tidb_ddl::index::reorg_defaults()));
            });
        })
    };
    tk.must_exec("alter table t1 add index idx(a);");
    assert_eq!(*pipe_sizes.lock().unwrap(), Some((4, 6)));
    assert_eq!(*finished_subtasks.lock().unwrap(), vec![1024]);
    assert!(modified.load(Ordering::SeqCst));
    assert_eq!(*batch_size_after_alter.lock().unwrap(), Some(256));
    tk.must_exec("admin check index t1 idx;");
    tk.must_exec("set global tidb_ddl_reorg_max_write_speed = 0");
}

/// Go `TestAlterThreadRightAfterJobFinish`.
#[test]
fn test_alter_thread_right_after_job_finish() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("set global tidb_enable_dist_task=0;");
    tk.must_exec("drop table if exists t;");
    tk.must_exec("create table t (c1 int primary key, c2 int)");
    tk.must_exec("insert t values (1, 1), (2, 2), (3, 3);");
    let updated = Arc::new(AtomicBool::new(false));
    let tk2 = Mutex::new(store.new_tk());
    let _cancelled = {
        let updated = Arc::clone(&updated);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/checkJobCancelled", move |job| {
            let job = job.downcast_ref::<tidb_model::Job>().unwrap();
            if !updated.load(Ordering::SeqCst)
                && job.type_ == tidb_model::ActionType::ACTION_ADD_INDEX
                && job.schema_state == tidb_model::SchemaState::WRITE_REORGANIZATION
            {
                updated.store(true, Ordering::SeqCst);
                tk2.lock().unwrap().must_exec(&format!("admin alter ddl jobs {} thread = 1", job.id));
            }
        })
    };
    let pipe_closed = Arc::new(AtomicBool::new(false));
    let _closed = {
        let pipe_closed = Arc::clone(&pipe_closed);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterPipeLineClose", move |_| {
            pipe_closed.store(true, Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_secs(5));
        })
    };
    let on_update_job_param = AtomicUsize::new(0);
    let _update = {
        let pipe_closed = Arc::clone(&pipe_closed);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/onUpdateJobParam", move |_| {
            if on_update_job_param.fetch_add(1, Ordering::SeqCst) == 0 {
                while !pipe_closed.load(Ordering::SeqCst) {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            }
        })
    };
    tk.must_exec("alter table t add index idx(c2)");
    assert!(updated.load(Ordering::SeqCst));
    assert!(pipe_closed.load(Ordering::SeqCst));
    tk.must_exec("set global tidb_enable_dist_task=1;");
}

/// Go `defer vardef.CloudStorageURI.Store("")`.
struct ResetCloudStorageUri;

impl Drop for ResetCloudStorageUri {
    fn drop(&mut self) {
        tidb_vardef::CLOUD_STORAGE_URI.write().unwrap().clear();
    }
}

/// Go `genServerWithStorage`: a sort store and its URI. The directory goes
/// away with the returned guard.
fn gen_server_with_storage() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}/sorted/addindex", dir.path().display());
    (dir, uri)
}

/// Go `handle.GetCloudStorageURI(ctx, store)`: the URI with the cluster ID
/// path.
fn cluster_cloud_storage_uri() -> String {
    tidb_dxf_handle::get_cloud_storage_uri(tidb_dxf_handle::cluster_id())
}

fn ext_store(sort_storage_uri: &str) -> std::sync::Arc<dyn tidb_objstore::storeapi::Storage> {
    tidb_dxf_handle::new_obj_store(&tidb_gocontext::Context::background(), sort_storage_uri).unwrap()
}

/// Go `checkFileCleaned`.
fn check_file_cleaned(job_id: i64, task_id: i64, sort_storage_uri: &str) {
    let store = ext_store(sort_storage_uri);
    for id in [job_id, task_id] {
        let prefix = id.to_string();
        let files = tidb_ingestor_simplesst::get_all_file_names(
            &tidb_gocontext::Context::background(),
            store.as_ref(),
            &[prefix.as_str()],
        )
        .unwrap();
        assert!(job_id > 0);
        assert!(files.is_empty(), "files under {prefix}: {files:?}");
    }
}

/// Go `checkFileExist`: some file under `dir` (or its partitioned dirs) has
/// `keyword` in its name.
fn check_file_exist(sort_storage_uri: &str, dir: &str, keyword: &str) {
    let store = ext_store(sort_storage_uri);
    let files =
        tidb_ingestor_simplesst::get_all_file_names(&tidb_gocontext::Context::background(), store.as_ref(), &[dir])
            .unwrap();
    assert!(files.iter().any(|f| f.contains(keyword)), "no file of {dir} has {keyword}: {files:?}");
}

/// Go `checkDataAndShowJobs`.
fn check_data_and_show_jobs(tk: &mut crate::realtikvtest::Tk, count: usize) {
    tk.must_exec("admin check table t;");
    let rows = tk.must_query("admin show ddl jobs 1;");
    assert_eq!(rows.len(), 1);
    assert!(rows[0][12].contains("ingest"), "{}", rows[0][12]);
    assert!(rows[0][12].contains("cloud"), "{}", rows[0][12]);
    assert_eq!(rows[0][7], count.to_string());
}

/// Go `checkExternalFields` (`testutils.AssertExternalField`): every
/// subtask meta in the table leaves its `external:"true"` fields to the
/// external file.
fn check_external_fields(tk: &mut crate::realtikvtest::Tk) {
    for row in tk.must_query("select meta from mysql.tidb_background_subtask") {
        let meta: tidb_ddl::backfilling_dist_executor::BackfillSubTaskMeta = serde_json::from_str(&row[0]).unwrap();
        assert!(!meta.external_path.is_empty(), "{}", row[0]);
        assert!(meta.range_job_keys.is_empty() && meta.range_split_keys.is_empty(), "{}", row[0]);
        assert!(meta.data_files.is_empty() && meta.stat_files.is_empty(), "{}", row[0]);
        assert!(meta.meta_groups.is_empty() && meta.ele_ids.is_empty(), "{}", row[0]);
        assert_eq!(meta.sorted_kv_meta, tidb_ingestor_globalsort::SortedKVMeta::default(), "{}", row[0]);
    }
}

/// Go `getTaskID`.
fn get_task_id(job_id: i64) -> i64 {
    let mgr = tidb_dxf_storage::get_task_manager().unwrap();
    let key = tidb_ddl::index::TaskKeyBuilder::new().build(job_id);
    mgr.get_task_by_key_with_history(&tidb_gocontext::Context::background(), &key).unwrap().base.id
}

/// Records the last job of Go's `afterWaitSchemaSynced` call.
fn record_job_id() -> (Arc<std::sync::atomic::AtomicI64>, tidb_failpoint::Guard) {
    let job_id = Arc::new(std::sync::atomic::AtomicI64::new(0));
    let guard = {
        let job_id = Arc::clone(&job_id);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", move |job| {
            job_id.store(job.downcast_ref::<tidb_model::Job>().unwrap().id, Ordering::SeqCst);
        })
    };
    (job_id, guard)
}

/// Go `TestGlobalSortBasic`.
#[test]
fn test_global_sort_basic() {
    let (_server, cloud_storage_uri) = gen_server_with_storage();
    let store = realtikv!();
    let mut tk = store.new_tk();
    // Go's unbuffered `ch`: the cleanup waits until the test receives.
    let (cleanup_tx, cleanup_rx) = std::sync::mpsc::sync_channel::<()>(0);
    let _batch = {
        let tx = Mutex::new(cleanup_tx.clone());
        tidb_failpoint::enable_call_for_test(
            "github.com/pingcap/tidb/pkg/dxf/framework/scheduler/processCleanupTaskBatch",
            move |_| {
                let _ = tx.lock().unwrap().send(());
            },
        )
    };
    let _wait = {
        let tx = Mutex::new(cleanup_tx);
        tidb_failpoint::enable_call_for_test(
            "github.com/pingcap/tidb/pkg/dxf/framework/scheduler/WaitCleanUpFinished",
            move |_| {
                let _ = tx.lock().unwrap().send(());
            },
        )
    };
    tk.must_exec("drop database if exists addindexlit;");
    tk.must_exec("create database addindexlit;");
    tk.must_exec("use addindexlit;");
    tk.must_exec("set @@global.tidb_ddl_enable_fast_reorg = 1;");
    tk.must_exec(&format!(r#"set @@global.tidb_cloud_storage_uri = "{cloud_storage_uri}""#));
    let _reset = ResetCloudStorageUri;
    let cloud_storage_uri = cluster_cloud_storage_uri();

    tk.must_exec("create table t (a int, b int, c int);");
    let size = 100;
    let values = (0..size).map(|i| format!("({i}, {i}, {i})")).collect::<Vec<_>>().join(",");
    tk.must_exec(&format!("insert into t values {values};"));

    let (job_id, _job) = record_job_id();
    let streaming = Arc::new(Mutex::new(Vec::<bool>::new()));
    let _streaming = {
        let streaming = Arc::clone(&streaming);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/checkEnableStreaming", move |enabled| {
            streaming.lock().unwrap().push(*enabled.downcast_ref::<bool>().unwrap());
        })
    };
    let wait_cleanup = || {
        for _ in 0..2 {
            cleanup_rx.recv_timeout(std::time::Duration::from_secs(60)).expect("the task was cleaned up");
        }
    };

    tk.must_exec("alter table t add index idx(a);");
    check_data_and_show_jobs(&mut tk, size);
    check_external_fields(&mut tk);
    let task_id = get_task_id(job_id.load(Ordering::SeqCst));
    check_file_exist(&cloud_storage_uri, &task_id.to_string(), "/plan/ingest");
    wait_cleanup();
    check_file_cleaned(job_id.load(Ordering::SeqCst), task_id, &cloud_storage_uri);

    let _merge = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/forceMergeSort", "return()");
    tk.must_exec("alter table t add index idx1(a);");
    check_data_and_show_jobs(&mut tk, size);
    check_external_fields(&mut tk);
    let task_id = get_task_id(job_id.load(Ordering::SeqCst));
    check_file_exist(&cloud_storage_uri, &task_id.to_string(), "/plan/ingest");
    check_file_exist(&cloud_storage_uri, &task_id.to_string(), "/plan/merge-sort");
    wait_cleanup();
    check_file_cleaned(job_id.load(Ordering::SeqCst), task_id, &cloud_storage_uri);

    tk.must_exec("alter table t add unique index idx2(a);");
    check_data_and_show_jobs(&mut tk, size);
    check_external_fields(&mut tk);
    let task_id = get_task_id(job_id.load(Ordering::SeqCst));
    check_file_exist(&cloud_storage_uri, &task_id.to_string(), "/plan/ingest");
    check_file_exist(&cloud_storage_uri, &task_id.to_string(), "/plan/merge-sort");
    wait_cleanup();
    check_file_cleaned(job_id.load(Ordering::SeqCst), task_id, &cloud_storage_uri);
    // Go: "streaming should be enabled with global sort".
    let streaming = streaming.lock().unwrap();
    assert!(!streaming.is_empty() && streaming.iter().all(|enabled| *enabled), "{streaming:?}");
}

/// Go `TestGlobalSortMultiSchemaChange`.
#[test]
fn test_global_sort_multi_schema_change() {
    let _batch = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/mockRegionBatch", "return(1)");
    let (_server, cloud_storage_uri) = gen_server_with_storage();
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("drop database if exists addindexlit;");
    tk.must_exec("create database addindexlit;");
    tk.must_exec("use addindexlit;");
    tk.must_exec("create table t_rowid (a int, b bigint, c varchar(255));");
    tk.must_exec("create table t_int_handle (a bigint primary key, b varchar(255));");
    tk.must_exec("create table t_common_handle (a int, b bigint, c varchar(255), primary key (a, c) clustered);");
    tk.must_exec("create table t_partition (a bigint primary key, b int, c char(10)) partition by hash(a) partitions 2;");
    for i in 0..10 {
        tk.must_exec(&format!("insert into t_rowid values ({i}, {i}, '{i}');"));
        tk.must_exec(&format!("insert into t_int_handle values ({i}, '{i}');"));
        tk.must_exec(&format!("insert into t_common_handle values ({i}, {i}, '{i}');"));
        tk.must_exec(&format!("insert into t_partition values ({i}, {i}, '{i}');"));
    }
    tk.must_exec("create table t_dup (a int, b bigint);");
    tk.must_exec("insert into t_dup values (1, 2), (2, 2);");
    tk.must_exec("create table t_dup_2 (a int primary key, b bigint);");
    tk.check("split table t_dup_2 between (0) and (80000) regions 7;", &["6 1"]);
    tk.must_exec("insert into t_dup_2 values (1, 2), (79999, 2);");

    let table_names = ["t_rowid", "t_int_handle", "t_common_handle", "t_partition"];
    let cases = [
        ("txn_backfill", "0", "0", String::new()),
        ("ingest_backfill", "1", "0", String::new()),
        ("ingest_dist_backfill", "1", "1", String::new()),
        ("ingest_dist_gs_backfill", "1", "1", cloud_storage_uri),
    ];
    for (name, enable_fast_reorg, enable_dist_task, cloud_storage_uri) in cases {
        let streaming = Arc::new(Mutex::new(Vec::<bool>::new()));
        let _streaming = {
            let streaming = Arc::clone(&streaming);
            tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/checkEnableStreaming", move |enabled| {
                streaming.lock().unwrap().push(*enabled.downcast_ref::<bool>().unwrap());
            })
        };
        tk.must_exec(&format!("set @@global.tidb_ddl_enable_fast_reorg = {enable_fast_reorg};"));
        tk.must_exec(&format!("set @@global.tidb_enable_dist_task = {enable_dist_task};"));
        tk.must_exec(&format!("set @@global.tidb_cloud_storage_uri = '{cloud_storage_uri}';"));
        for table in table_names {
            tk.must_exec(&format!("alter table {table} add index idx_1(a), add index idx_2(b, a);"));
            tk.must_exec(&format!("admin check table {table};"));
            tk.must_exec(&format!("alter table {table} drop index idx_1, drop index idx_2;"));
        }
        tk.must_contain_err_msg(
            "alter table t_dup add index idx(a), add unique index idx2(b);",
            "Duplicate entry '2' for key 't_dup.idx2'",
        );
        tk.must_contain_err_msg("alter table t_dup_2 add unique index idx2(b);", "Duplicate entry '2' for key 't_dup_2.idx2'");
        let expected = !cloud_storage_uri.is_empty();
        assert!(streaming.lock().unwrap().iter().all(|enabled| *enabled == expected), "{name}: {:?}", streaming.lock().unwrap());
    }
    tk.must_exec("set @@global.tidb_enable_dist_task = 1;");
    tk.must_exec("set @@global.tidb_cloud_storage_uri = '';");
}

/// Go `TestAddIndexIngestShowReorgTp`.
#[test]
fn test_add_index_ingest_show_reorg_tp() {
    let (_server, cloud_storage_uri) = gen_server_with_storage();
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("drop database if exists addindexlit;");
    tk.must_exec("create database addindexlit;");
    tk.must_exec("use addindexlit;");
    tk.must_exec(&format!("set @@global.tidb_cloud_storage_uri = '{cloud_storage_uri}';"));
    tk.must_exec("set @@global.tidb_enable_dist_task = 0;");
    tk.must_exec("set @@global.tidb_ddl_enable_fast_reorg = 1;");

    tk.must_exec("create table t (a int);");
    tk.must_exec("alter table t add index idx(a);");
    tk.check("select * from t use index(idx);", &[]);
    tk.must_exec("alter table t drop index idx;");

    tk.must_exec("insert into t values (1), (2), (3);");
    tk.must_exec("set @@global.tidb_enable_dist_task = 0;");
    tk.must_exec("alter table t add index idx(a);");

    let rows = tk.must_query("admin show ddl jobs 1;");
    assert_eq!(rows.len(), 1);
    let (job_type, row_cnt) = (&rows[0][12], &rows[0][7]);
    assert!(job_type.contains("ingest"), "{job_type}");
    assert!(!job_type.contains("cloud"), "{job_type}");
    assert_eq!(row_cnt, "3");
    tk.must_exec("set @@global.tidb_enable_dist_task = 1;");
    tk.must_exec("set @@global.tidb_cloud_storage_uri = '';");
}

/// Go `TestGlobalSortDuplicateErrMsg`. Go turns `ddl.EnableSplitTableRegion`
/// on for the partitioned case; this node's `split-table` is on already.
#[test]
fn test_global_sort_duplicate_err_msg() {
    let _restore = tidb_dxf_testutil::reduce_check_interval_for_test();
    let (_server, cloud_storage_uri) = gen_server_with_storage();
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("drop database if exists addindexlit;");
    tk.must_exec("create database addindexlit;");
    tk.must_exec("use addindexlit;");
    tk.must_exec("set @@global.tidb_ddl_enable_fast_reorg = 1;");
    tk.must_exec(&format!(r#"set @@global.tidb_cloud_storage_uri = "{cloud_storage_uri}""#));
    let _reset = ResetCloudStorageUri;
    tk.must_exec("set @@session.tidb_scatter_region = 'table'");
    let _batch = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/mockRegionBatch", "return(1)");
    // Go `testErrStep`: the step of the last subtask run (Go's `errP` is
    // never nil).
    let test_err_step = Arc::new(Mutex::new(tidb_dxf::STEP_INIT));
    let _after_run = {
        let test_err_step = Arc::clone(&test_err_step);
        tidb_failpoint::enable_call_ref_for_test::<tidb_dxf_taskexecutor::AfterRunSubtask>(
            "github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/afterRunSubtask",
            Arc::new(move |e, _, _| *test_err_step.lock().unwrap() = e.get_task_base().step),
        )
    };
    let cases = [
        (
            "varchar index",
            "create table t (id int, data varchar(255));",
            "",
            "insert into t values (1, '1'), (2, '1');",
            "alter table t add unique index idx(data);",
            "[kv:1062]Duplicate entry '1' for key 't.idx'",
        ),
        (
            "int index on multi regions",
            "create table t (a int primary key, b int);",
            "split table t between (0) and (4000) regions 4;",
            "insert into t values (1, 1), (1001, 1), (2001, 2001), (4001, 1);",
            "alter table t add unique index idx(b);",
            "[kv:1062]Duplicate entry '1' for key 't.idx'",
        ),
        (
            "combined index",
            "create table t (id int, data varchar(255));",
            "",
            "insert into t values (1, '1'), (1, '1');",
            "alter table t add unique index idx(id, data);",
            "[kv:1062]Duplicate entry '1-1' for key 't.idx'",
        ),
        (
            "multi value index",
            "create table t (id int, data json);",
            "",
            r#"insert into t values (1, '{"code":[1,1]}'), (2, '{"code":[1,1]}');"#,
            "alter table t add unique index idx( (CAST(data->'$.code' AS UNSIGNED ARRAY)));",
            "[kv:1062]Duplicate entry '1' for key 't.idx'",
        ),
        (
            "global index",
            "create table t (k int, c int) partition by list (k) (partition odd values in (1,3,5,7,9), partition even values in (2,4,6,8,10));",
            "",
            "insert into t values (1, 1), (2, 1)",
            "alter table t add unique index idx(c) global",
            "[kv:1062]Duplicate entry '1' for key 't.idx'",
        ),
    ];
    let check_subtask_step_and_reset = |name: &str, expected: tidb_dxf::Step| {
        let mut step = test_err_step.lock().unwrap();
        assert_eq!(*step, expected, "{name}");
        *step = tidb_dxf::STEP_INIT;
    };
    for (name, create_table_sql, split_table_sql, init_data_sql, add_unique_key_sql, err_msg) in cases {
        tk.must_exec(create_table_sql);
        tk.must_exec(init_data_sql);
        let multiple_regions = !split_table_sql.is_empty() || create_table_sql.contains("partition");
        if !split_table_sql.is_empty() {
            tk.check(split_table_sql, &["3 1"]);
        }
        if create_table_sql.contains("partition") {
            assert_eq!(tk.must_query("show table t regions").len(), 2, "{name}");
        }

        // 1. read index
        tk.must_contain_err_msg(add_unique_key_sql, err_msg);
        check_subtask_step_and_reset(
            name,
            if multiple_regions { tidb_dxf::BACKFILL_STEP_WRITE_AND_INGEST } else { tidb_dxf::BACKFILL_STEP_READ_INDEX },
        );
        // Go `checkRedactMsgAndReset`.
        tk.must_exec("set global tidb_redact_log = on;");
        tk.must_contain_err_msg(add_unique_key_sql, "[kv:1062]Duplicate entry '?' for key 't.idx'");
        tk.must_exec("set global tidb_redact_log = off;");
        *test_err_step.lock().unwrap() = tidb_dxf::STEP_INIT;

        // 2. merge sort
        let ignore_dup =
            tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/ignoreReadIndexDupKey", "return(true)");
        let force_merge = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/forceMergeSort", "return()");
        tk.must_contain_err_msg(add_unique_key_sql, err_msg);
        check_subtask_step_and_reset(name, tidb_dxf::BACKFILL_STEP_MERGE_SORT);

        // 3. cloud import
        drop(force_merge);
        tk.must_contain_err_msg(add_unique_key_sql, err_msg);
        check_subtask_step_and_reset(name, tidb_dxf::BACKFILL_STEP_WRITE_AND_INGEST);
        drop(ignore_dup);
        tk.must_exec("drop table if exists t");
    }
    tk.must_exec("set @@session.tidb_scatter_region = ''");
}

/// Go `TestGlobalSortAddIndexRecoverFromRetryableError`: when meeting a
/// retryable error, the subtask and the job are idempotent.
#[test]
fn test_global_sort_add_index_recover_from_retryable_error() {
    let (_server, cloud_storage_uri) = gen_server_with_storage();
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("drop database if exists addindexlit;");
    tk.must_exec("create database addindexlit;");
    tk.must_exec("use addindexlit;");
    tk.must_exec("set @@global.tidb_ddl_enable_fast_reorg = 1;");
    tk.must_exec(&format!(r#"set @@global.tidb_cloud_storage_uri = "{cloud_storage_uri}""#));
    let _merge = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/forceMergeSort", "return()");
    for fp in [
        "github.com/pingcap/tidb/pkg/ddl/mockCheckDuplicateForUniqueIndexError",
        "github.com/pingcap/tidb/pkg/ddl/mockCloudImportRunSubtaskError",
        "github.com/pingcap/tidb/pkg/ddl/mockMergeSortRunSubtaskError",
    ] {
        tk.must_exec("drop table if exists t;");
        tk.must_exec("create table t (a int);");
        tk.must_exec("insert into t values (1), (2), (3);");
        let guard = tidb_failpoint::enable_for_test(fp, "1*return");
        tk.must_exec("alter table t add unique index idx(a);");
        drop(guard);
    }
    tk.must_exec("set @@global.tidb_cloud_storage_uri = '';");
}

/// Go `TestIngestUseGivenTS`. Go reads the index key it encodes; this test
/// reads the index's only entry for `a = 1` from the index's first key.
#[test]
fn test_ingest_use_given_ts() {
    let (_server, cloud_storage_uri) = gen_server_with_storage();
    let store = realtikv!();
    let use_cloud_storage = Arc::new(AtomicBool::new(false));
    let _loaded = {
        let use_cloud_storage = Arc::clone(&use_cloud_storage);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterLoadCloudStorageURI", move |job| {
            let job = job.downcast_ref::<tidb_model::Job>().unwrap();
            use_cloud_storage
                .store(job.reorg_meta.as_ref().is_some_and(|meta| meta.read().use_cloud_storage), Ordering::SeqCst);
        })
    };
    let mut tk = store.new_tk();
    tk.must_exec("drop database if exists addindexlit;");
    tk.must_exec("create database addindexlit;");
    tk.must_exec("use addindexlit;");
    tk.must_exec("set global tidb_ddl_enable_fast_reorg = on;");
    tk.must_exec(&format!("set @@global.tidb_cloud_storage_uri = '{cloud_storage_uri}';"));

    // Go `oracle.GoTimeToTS(time.Now())`.
    let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    let preset_ts = now_ms << 18;
    {
        let _ts = tidb_failpoint::enable_for_test(
            "github.com/pingcap/tidb/pkg/ddl/mockTSForGlobalSort",
            &format!("return({preset_ts})"),
        );
        tk.must_exec("create table t (a int);");
        tk.must_exec("insert into t values (1), (2), (3);");
        tk.must_exec("alter table t add index idx(a);");
    }

    let table = store.table_info("addindexlit", "t");
    let index_id = table.indices.iter_deref().find(|index| index.read().name.lowercase() == "idx").unwrap().read().id;
    let prefix = tidb_tablecodec::table_key::encode_table_index_prefix(table.id, index_id);
    let end = tidb_tablecodec::table_key::encode_table_index_prefix(table.id, index_id + 1);
    let index_key = tokio::runtime::Runtime::new().unwrap().block_on(async {
        let client = tikv_client::TransactionClient::new(vec![std::env::var("TIDB_REALTIKV_PD").unwrap()]).await.unwrap();
        let ts = client.current_timestamp().await.unwrap();
        let mut snapshot = client.snapshot(ts, tikv_client::TransactionOptions::new_optimistic().read_only());
        let key: Vec<u8> = snapshot.scan_keys(prefix..end, 1).await.unwrap().next().unwrap().into();
        key
    });
    let mvcc = store.region_store().mvcc_get_by_key(&index_key).unwrap();
    let info = mvcc.info.expect("the index key has MVCC info");
    assert!(!info.writes.is_empty());
    assert_eq!(info.writes[0].commit_ts, preset_ts);
    assert!(use_cloud_storage.load(Ordering::SeqCst));
    tk.must_exec("set @@global.tidb_cloud_storage_uri = '';");
}

/// Go `TestAlterJobOnDXFWithGlobalSort`. Go also changes the merge sort's
/// concurrency and checks its pool (`mergeOverlappingFiles`); the ported
/// merge operator has no tunable pool, so only the read step is altered.
#[test]
fn test_alter_job_on_dxf_with_global_sort() {
    let _cpu = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(16)");
    let _restore = tidb_dxf_testutil::reduce_check_interval_for_test();
    let (_server, cloud_storage_uri) = gen_server_with_storage();
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("set global tidb_ddl_enable_fast_reorg = on;");
    tk.must_exec(&format!("set @@global.tidb_cloud_storage_uri = '{cloud_storage_uri}';"));
    tk.must_exec("drop database if exists testalter;");
    tk.must_exec("create database testalter;");
    tk.must_exec("use testalter;");
    tk.must_exec("create table gsort(a bigint auto_random primary key);");
    for _ in 0..16 {
        tk.must_exec("insert into gsort values (), (), (), ()");
    }
    tk.must_exec("split table gsort between (3) and (8646911284551352360) regions 50;");
    tk.must_exec("set @@tidb_ddl_reorg_worker_cnt = 1");
    tk.must_exec("set @@tidb_ddl_reorg_batch_size = 32");
    tk.must_exec("set global tidb_ddl_reorg_max_write_speed = '256MiB'");

    let _merge = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/forceMergeSort", "return(true)");
    let modified_read_index = Arc::new(AtomicBool::new(false));
    let _modify = {
        let modified_read_index = Arc::clone(&modified_read_index);
        tidb_failpoint::enable_call_for_test(
            "github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/afterDetectAndHandleParamModify",
            move |step| {
                if *step.downcast_ref::<tidb_dxf::Step>().unwrap() == tidb_dxf::BACKFILL_STEP_READ_INDEX {
                    modified_read_index.store(true, Ordering::SeqCst);
                }
            },
        )
    };
    let pipe_sizes = Arc::new(Mutex::new(None::<(i32, i32)>));
    let _closed = {
        let pipe_sizes = Arc::clone(&pipe_sizes);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterPipeLineClose", move |pipe| {
            let pipe = pipe.downcast_ref::<Pipe>().unwrap();
            let (reader, writer) = pipe.get_reader_and_writer();
            *pipe_sizes.lock().unwrap() =
                Some((reader.unwrap().worker_pool_size(), writer.unwrap().worker_pool_size()));
        })
    };
    // Change the batch size and concurrency during table scanning.
    let once = Once::new();
    let batch_size_after_alter = Arc::new(Mutex::new(None::<i64>));
    let tk1 = Mutex::new(store.new_tk());
    let _scan = {
        let (modified_read_index, batch_size_after_alter) =
            (Arc::clone(&modified_read_index), Arc::clone(&batch_size_after_alter));
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/scanRecordExec", move |reorg_meta| {
            let reorg_meta = reorg_meta.downcast_ref::<tidb_model::GoShared<tidb_model::DDLReorgMeta>>().unwrap().clone();
            once.call_once(|| {
                let mut tk1 = tk1.lock().unwrap();
                let rows = tk1.must_query("select job_id from mysql.tidb_ddl_job");
                assert_eq!(rows.len(), 1);
                tk1.must_exec(&format!("admin alter ddl jobs {} thread = 8, batch_size = 256", rows[0][0]));
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
                while !modified_read_index.load(Ordering::SeqCst) {
                    assert!(std::time::Instant::now() < deadline, "the task executor saw the modification");
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
                *batch_size_after_alter.lock().unwrap() =
                    Some(reorg_meta.read().get_batch_size(tidb_ddl::index::reorg_defaults()));
            });
        })
    };
    tk.must_exec("alter table gsort add index idx(a)");
    // Global sort uses equal reader and writer count.
    assert_eq!(*pipe_sizes.lock().unwrap(), Some((8, 8)));
    assert!(modified_read_index.load(Ordering::SeqCst));
    assert_eq!(*batch_size_after_alter.lock().unwrap(), Some(256));
    tk.must_exec("admin check index gsort idx");
    tk.must_exec("set global tidb_ddl_reorg_max_write_speed = 0");
    tk.must_exec("set @@global.tidb_cloud_storage_uri = '';");
}

/// Go `TestDXFAddIndexRealtimeSummary`. Go also checks each step's GET and
/// PUT request counts, which its GCS client records per HTTP request; no
/// ported object store records requests (`AccessStats::rec_request` has no
/// caller), so only the byte counts are checked.
#[test]
fn test_dxf_add_index_realtime_summary() {
    let (_server, cloud_storage_uri) = gen_server_with_storage();
    let _cpu = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(16)");
    let _restore = tidb_dxf_testutil::reduce_check_interval_for_test();
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("set global tidb_ddl_enable_fast_reorg = on;");
    tk.must_exec(&format!("set @@global.tidb_cloud_storage_uri = '{cloud_storage_uri}';"));
    tk.must_exec("use test;");
    tk.must_exec("drop table if exists t;");
    tk.must_exec("create table t (id varchar(255), b int, c int, primary key(id) clustered);");
    tk.must_exec("insert into t values ('a',1,1),('b',2,2),('c',3,3);");

    let job_id = Arc::new(std::sync::atomic::AtomicI64::new(0));
    let _step = {
        let job_id = Arc::clone(&job_id);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", move |job| {
            let job = job.downcast_ref::<tidb_model::Job>().unwrap();
            if job.type_ == tidb_model::ActionType::ACTION_ADD_INDEX {
                job_id.store(job.id, Ordering::SeqCst);
            }
        })
    };
    let _merge = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/forceMergeSort", "return()");
    tk.must_exec("alter table t add index idx(c);");
    let task_id = tk.must_query(&format!(
        "with global_tasks as (table mysql.tidb_global_task union table mysql.tidb_global_task_history)
         select id from global_tasks where task_key like concat('%/', '{}');",
        job_id.load(Ordering::SeqCst)
    ))[0][0]
        .clone();
    let mut get_summary = |step: i64| -> (i64, i64) {
        let rows = tk.must_query(&format!(
            "with subtasks as (table mysql.tidb_background_subtask union table mysql.tidb_background_subtask_history)
             select
                 json_extract(summary, '$.read_bytes'),
                 json_extract(summary, '$.bytes')
             from subtasks where task_key = '{task_id}' and step = {step};"
        ));
        assert_eq!(rows.len(), 1, "step {step}: {rows:?}");
        let n = |i: usize| rows[0][i].parse::<i64>().unwrap();
        (n(0), n(1))
    };
    let (read_bytes, bytes) = get_summary(1);
    // The bytes of reading table records.
    assert!(read_bytes > 0);
    // The bytes of writing index records.
    assert!(bytes > 0);

    // 0, not suitable for merge sort.
    assert_eq!(get_summary(2), (0, 0));
    assert_eq!(get_summary(3), (0, 0));
    tk.must_exec("set @@global.tidb_cloud_storage_uri = '';");
}

/// Go `TestSplitRangeForTable` and `TestSplitRangeForPartitionTable`.
fn split_range_cases(create_table_sql: &str, insert: impl Fn(usize) -> String, table: &str, global_index: bool) {
    let (_server, cloud_storage_uri) = gen_server_with_storage();
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("drop database if exists addindexlit;");
    tk.must_exec("create database addindexlit;");
    tk.must_exec("use addindexlit;");
    tk.must_exec("set @@global.tidb_ddl_enable_fast_reorg = 1;");
    tk.must_exec(create_table_sql);
    for i in 0..1024 {
        tk.must_exec(&insert(i));
    }
    let add_cnt = Arc::new(AtomicUsize::new(0));
    let remove_cnt = Arc::new(AtomicUsize::new(0));
    let _add = {
        let add_cnt = Arc::clone(&add_cnt);
        tidb_failpoint::enable_call_for_test(
            "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/AddPartitionRangeForTable",
            move |_| {
                add_cnt.fetch_add(1, Ordering::SeqCst);
            },
        )
    };
    let _remove = {
        let remove_cnt = Arc::clone(&remove_cnt);
        tidb_failpoint::enable_call_for_test(
            "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/RemovePartitionRangeRequest",
            move |_| {
                remove_cnt.fetch_add(1, Ordering::SeqCst);
            },
        )
    };
    let cases = [("local ingest", "off", String::new()), ("dxf ingest", "on", String::new()), ("dxf global-sort", "on", cloud_storage_uri)];
    for (i, (name, enable_dist_task, global_sort)) in cases.into_iter().enumerate() {
        tk.must_exec(&format!("set global tidb_enable_dist_task = {enable_dist_task};"));
        tk.must_exec(&format!("set global tidb_cloud_storage_uri = '{global_sort}';"));
        // 1. A small table (one region, under 100) skips the force split.
        add_cnt.store(0, Ordering::SeqCst);
        remove_cnt.store(0, Ordering::SeqCst);
        tk.must_exec(&format!("alter table {table} add index i_small_{i}(c)"));
        assert_eq!(add_cnt.load(Ordering::SeqCst), 0, "small table should skip force split in {name}");
        assert_eq!(remove_cnt.load(Ordering::SeqCst), 0);
        tk.must_exec(&format!("alter table {table} drop index i_small_{i}"));
        // 2. A large table, mocked by lowering the threshold; the local
        // backend logic is checked once. Go's subtest disables the failpoint
        // when it ends.
        if name == "local ingest" {
            let _threshold = tidb_failpoint::enable_call_for_test(
                "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/ForcePartitionRegionThreshold",
                |threshold| *threshold.downcast_mut::<usize>().unwrap() = 0,
            );
            add_cnt.store(0, Ordering::SeqCst);
            remove_cnt.store(0, Ordering::SeqCst);
            tk.must_exec(&format!("alter table {table} add index i_large_{i}(c)"));
            assert!(add_cnt.load(Ordering::SeqCst) > 0, "large table should trigger force split in {name}");
            assert_eq!(add_cnt.load(Ordering::SeqCst), remove_cnt.load(Ordering::SeqCst));
            tk.must_exec(&format!("alter table {table} drop index i_large_{i}"));
            if global_index {
                // 3. A global index.
                add_cnt.store(0, Ordering::SeqCst);
                remove_cnt.store(0, Ordering::SeqCst);
                tk.must_exec(&format!("alter table {table} add index gi(c) global"));
                assert!(add_cnt.load(Ordering::SeqCst) > 0);
                assert_eq!(add_cnt.load(Ordering::SeqCst), remove_cnt.load(Ordering::SeqCst));
                tk.must_exec(&format!("alter table {table} drop index gi"));
            }
        }
    }
    tk.must_exec("set global tidb_enable_dist_task = on;");
    tk.must_exec("set global tidb_cloud_storage_uri = '';");
}

/// Go `TestSplitRangeForTable`.
#[test]
fn test_split_range_for_table() {
    split_range_cases("CREATE TABLE t (c int)", |i| format!("INSERT INTO t VALUES ({i})"), "t", false);
}

/// Go `TestSplitRangeForPartitionTable`.
#[test]
fn test_split_range_for_partition_table() {
    split_range_cases(
        "CREATE TABLE tp (id int primary key, c int) PARTITION BY HASH (id) PARTITIONS 2",
        |i| format!("INSERT INTO tp VALUES ({i}, {i})"),
        "tp",
        true,
    );
}
