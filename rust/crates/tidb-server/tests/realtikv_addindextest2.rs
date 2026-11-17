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
//! `alter_job_test.go`. Its `global_sort_test.go` needs global sort.

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
