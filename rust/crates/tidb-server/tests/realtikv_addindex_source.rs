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

//! Go `tests/realtikvtest/addindextest1`, over [`crate::realtikvtest`].

use crate::realtikvtest::Tk;

/// Go `addindextest1.checkTmpDDLDir`: no stale file under a `tmp_ddl-` dir.
fn check_tmp_ddl_dir() {
    fn walk(dir: &std::path::Path, root: &std::path::Path) {
        let Ok(entries) = std::fs::read_dir(dir) else { return };
        for entry in entries {
            let path = entry.unwrap().path();
            let rel = path.strip_prefix(root).unwrap().to_string_lossy().into_owned();
            if path.is_dir() {
                walk(&path, root);
            } else {
                // we only checks whether there is a stale file, empty dir is allowed.
                assert!(!rel.contains("/tmp_ddl-"), "stale file found: {rel}");
            }
        }
    }
    let tmp_dir = std::path::PathBuf::from(&tidb_config::config_tree::config::get_global_config().temp_dir);
    println!("{}", tmp_dir.display());
    walk(&tmp_dir, &tmp_dir);
}

/// Go `addindextest1.TestAddIndexDistBasic`.
#[test]
fn test_add_index_dist_basic() {
    // mock that we only have 1 cpu, add-index task can be scheduled as usual
    let _num_cpu = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(1)");
    let _last_task =
        tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/dxf/framework/storage/testSetLastTaskID", "return(true)");
    let store = realtikv!();

    let mut tk = store.new_tk();
    tk.must_exec("drop database if exists test;");
    tk.must_exec("create database test;");
    tk.must_exec("use test;");
    tk.must_exec("set global tidb_enable_dist_task=1;");

    let bak = tidb_vardef::get_ddl_reorg_worker_counter();
    tk.must_exec("set global tidb_ddl_reorg_worker_cnt = 111");
    tk.must_exec("set @@tidb_ddl_reorg_worker_cnt = 111");
    assert_eq!(tidb_vardef::get_ddl_reorg_worker_counter(), 111);
    tk.must_exec("create table t(a bigint auto_random primary key) partition by hash(a) partitions 20;");
    for _ in 0..5 {
        tk.must_exec("insert into t values (), (), (), (), (), ()");
    }
    tk.must_exec("split table t between (3) and (8646911284551352360) regions 50;");
    tk.must_exec("alter table t add index idx(a);");
    tk.must_exec("admin check index t idx;");
    let task_mgr = tidb_dxf_storage::get_task_manager().unwrap();
    let ctx = tidb_gocontext::Context::background();
    let task = task_mgr
        .get_task_by_id_with_history(&ctx, tidb_dxf_storage::TEST_LAST_TASK_ID.load(std::sync::atomic::Ordering::SeqCst))
        .unwrap();
    assert_eq!(task.base.required_slots, 1);

    tk.must_exec(&format!("set global tidb_ddl_reorg_worker_cnt = {bak}"));
    tk.must_exec(&format!("set @@tidb_ddl_reorg_worker_cnt = {bak}"));
    assert_eq!(tidb_vardef::get_ddl_reorg_worker_counter(), bak);

    tk.must_exec("create table t1(a bigint auto_random primary key);");
    for _ in 0..4 {
        tk.must_exec("insert into t1 values (), (), (), (), (), ()");
    }
    tk.must_exec("split table t1 between (3) and (8646911284551352360) regions 50;");
    tk.must_exec("alter table t1 add index idx(a);");
    tk.must_exec("admin check index t1 idx;");

    let counter = std::sync::atomic::AtomicI32::new(0);
    let after_run = tidb_failpoint::enable_call_ref_for_test::<tidb_dxf_taskexecutor::AfterRunSubtask>(
        "github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/afterRunSubtask",
        std::sync::Arc::new(move |_, err_p, _| {
            if counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1 == 1 {
                *err_p = Some(std::sync::Arc::new(tidb_gocontext::CtxError::Canceled));
            }
        }),
    );
    tk.must_exec("alter table t1 add index idx1(a);");
    tk.must_exec("admin check index t1 idx1;");
    drop(after_run);

    {
        let _panic = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/injectPanicForTableScan", "return()");
        tk.exec_to_err("alter table t1 add index idx2(a);");
    }

    let _fault = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/mockIndexIngestWorkerFault", |_| {
        panic!("mock panic");
    });
    tk.exec_to_err("alter table t1 add index idx2(a);");

    tk.must_exec("set global tidb_enable_dist_task=0;");
    check_tmp_ddl_dir();
}

/// Go `addindextest1.TestAddIndexForCurrentTimestampColumn`.
#[test]
fn test_add_index_for_current_timestamp_column() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("drop database if exists addindexlit;");
    tk.must_exec("create database addindexlit;");
    tk.must_exec("use addindexlit;");
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("set global tidb_enable_dist_task = on;");
    tk.must_exec("create table t (a timestamp default current_timestamp);");
    tk.must_exec("insert into t values ();");
    tk.must_exec("alter table t add index idx(a);");
    tk.must_exec("admin check table t;");
}

/// Go `addindextest1.TestAddUKErrorMessage`.
#[test]
fn test_add_uk_error_message() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("drop database if exists addindexlit;");
    tk.must_exec("create database addindexlit;");
    tk.must_exec("use addindexlit;");
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("set global tidb_enable_dist_task = on;");
    tk.must_exec("create table t (a int primary key, b int);");
    tk.must_exec("insert into t values (5, 1), (10005, 1), (20005, 1), (30005, 1);");
    tk.must_exec("split table t between (1) and (100001) regions 10;");
    let error = tk.exec_to_err("alter table t add unique index uk(b);");
    assert!(error.contains("Duplicate entry '1' for key 't.uk'"), "{error}");
}

/// Go `addindextest1.TestAddIndexInvalidDistTaskVariableSetting`.
#[test]
fn test_add_index_invalid_dist_task_variable_setting() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("drop database if exists addindexlit;");
    tk.must_exec("create database addindexlit;");
    tk.must_exec("use addindexlit;");
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=off;");
    tk.must_exec("set global tidb_enable_dist_task = on;");
    tk.must_exec("create table t (a int);");
    // errno.ErrUnsupportedDDLOperation
    tk.must_get_err_code("alter table t add index idx(a);", 8200);
    tk.must_get_err_code("alter table t add column b int, add index idx(a);", 8200);
    tk.must_exec("alter table t add column b int, add column c int;");
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("set global tidb_enable_dist_task = off;");
}

/// Go `addindextest1.TestAddIndexDistCancel`.
#[test]
fn test_add_index_dist_cancel() {
    // Go restores them in `t.Cleanup`; the test runs in its own process.
    let _restore = tidb_dxf_testutil::reduce_check_interval_for_test();
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("drop database if exists addindexlit;");
    tk.must_exec("create database addindexlit;");
    tk.must_exec("use addindexlit;");
    tk.must_exec("create table t (a int, b int);");
    tk.must_exec("insert into t values (1, 1), (2, 2), (3, 3);");

    let mut tk2 = store.new_tk();
    tk2.must_exec("use addindexlit;");
    tk2.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("set @@global.tidb_enable_dist_task = 1;");
    tk2.must_exec("create table t2 (a int, b int);");
    tk2.must_exec("insert into t2 values (1, 1), (2, 2), (3, 3);");

    let counter = std::sync::Arc::new(std::sync::atomic::AtomicI32::new(0));
    let enable_trigger = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let target_job_id = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
    let running_done = {
        let (enable_trigger, target_job_id) = (std::sync::Arc::clone(&enable_trigger), std::sync::Arc::clone(&target_job_id));
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterBackfillStateRunningDone", move |job| {
            let job = job.downcast_ref::<tidb_model::Job>().unwrap();
            // fail for one index when finish backfill, and check row count right
            if counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1 == 1 {
                target_job_id.store(job.id, std::sync::atomic::Ordering::SeqCst);
                enable_trigger.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        })
    };
    let update_job = tidb_failpoint::enable_call_ref_for_test::<tidb_ddl::cluster_ddl::AfterUpdateJobToTable>(
        "github.com/pingcap/tidb/pkg/ddl/afterUpdateJobToTable",
        std::sync::Arc::new(move |job, err_p| {
            if enable_trigger.load(std::sync::atomic::Ordering::SeqCst)
                && job.id == target_job_id.load(std::sync::atomic::Ordering::SeqCst)
            {
                *err_p = Some(tidb_ddl_build::plan_error::DdlPlanError::Unsupported("mock error".to_owned()));
                enable_trigger.store(false, std::sync::atomic::Ordering::SeqCst);
            }
        }),
    );
    std::thread::scope(|wg| {
        wg.spawn(|| tk.must_exec("alter table t add index idx(a);"));
        wg.spawn(|| tk2.must_exec("alter table t2 add index idx_b(b);"));
    });
    let rows = tk.must_query("admin show ddl jobs 2;");
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert!(rows[0][12].contains("ingest"), "{:?}", rows[0]);
    assert!(rows[1][12].contains("ingest"), "{:?}", rows[1]);
    assert_eq!(rows[0][7], "3");
    assert_eq!(rows[1][7], "3");
    drop(running_done);
    drop(update_job);

    // test cancel is timely
    let (enter_tx, enter) = std::sync::mpsc::sync_channel::<()>(1);
    let _hook = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/beforeExecuteRegionJob",
        move |arg| {
            let ctx = arg.downcast_mut::<tidb_gocontext::Context>().unwrap();
            let _ = enter_tx.try_send(());
            let _ = ctx.sleep(std::time::Duration::from_secs(30));
        },
    );
    std::thread::scope(|scope| {
        let ddl = scope.spawn(|| tk2.exec("alter table t add index idx_ba(b, a);"));
        enter.recv().expect("a region job started");
        let job_id = tk.must_query("admin show ddl jobs 1;")[0][0].clone();
        let now = std::time::Instant::now();
        tk.must_exec(&format!("admin cancel ddl jobs {job_id}"));
        let error = ddl.join().unwrap().expect_err("the cancelled ADD INDEX fails");
        assert!(error.contains("Cancelled DDL job"), "{error}");
        // cancel should be timely
        assert!(now.elapsed().as_secs_f64() < 20.0, "{:?}", now.elapsed());
    });
}

/// Go `addindextest1.TestAddIndexDistCancelWithPartition`.
#[test]
fn test_add_index_dist_cancel_with_partition() {
    // Go restores them in `t.Cleanup`; the test runs in its own process.
    let _restore = tidb_dxf_testutil::reduce_check_interval_for_test();
    let store = realtikv!();
    let mut tk = store.new_tk();
    let tk1 = std::sync::Mutex::new(store.new_tk());
    tk.must_exec("drop database if exists test;");
    tk.must_exec("create database test;");
    tk.must_exec("use test;");
    tk.must_exec("set global tidb_enable_dist_task=1;");

    tk.must_exec("create table t(a bigint auto_random primary key) partition by hash(a) partitions 8;");
    for _ in 0..4 {
        tk.must_exec("insert into t values (), (), (), (), (), ()");
    }
    tk.must_exec("split table t between (3) and (8646911284551352360) regions 50;");

    let once = std::sync::Once::new();
    let _finish = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/mockDMLExecutionAddIndexSubTaskFinish",
        move |_| {
            once.call_once(|| {
                let mut tk1 = tk1.lock().unwrap();
                let row = tk1.must_query("select job_id from mysql.tidb_ddl_job");
                assert_eq!(row.len(), 1);
                tk1.must_exec(&format!("admin cancel ddl jobs {}", row[0][0]));
            });
        },
    );

    tk.exec_to_err("alter table t add index idx(a);");
    tk.must_exec("admin check table t;");
    tk.must_exec("alter table t add index idx2(a);");
    tk.must_exec("admin check table t;");
    tk.must_exec("set global tidb_enable_dist_task=0;");
}

/// Go `TestAddIndexDistAutoPauseOnKVDiskFull`'s `runAutoPauseCase`.
fn run_auto_pause_case(tk: &mut Tk, alter_sql: &str, check_sql: &str, verify_job: impl Fn(&tidb_model::Job)) {
    let failpoint_name = "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/WriteToTiKVNotEnoughDiskSpace";
    let fp = tidb_failpoint::enable_for_test(failpoint_name, "return()");
    match tk.exec_coded(alter_sql) {
        Ok(_) => panic!("{alter_sql}: expected an error"),
        Err((code, message)) => {
            assert_eq!(code, tidb_errno::errcode::ErrDDLAutoPausedByKVDiskFull, "unexpected error: {message}");
        }
    }
    drop(fp);

    let rows = tk.must_query("select job_id, job_meta from mysql.tidb_ddl_job");
    assert_eq!(rows.len(), 1);
    let job_id = rows[0][0].clone();
    let mut job = tidb_model::Job::default();
    job.decode(rows[0][1].as_bytes()).unwrap();
    assert!(job.is_paused_by_system_for_kv_disk_full(), "job: {job:?}");
    let reason = job.pause_reason.as_ref().expect("a pause reason").read().message.to_string();
    assert!(reason.contains("TiKV disk full"), "{reason}");
    assert_eq!(job.error_count, 0);
    verify_job(&job);

    tk.must_exec(&format!("admin resume ddl jobs {job_id}"));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while tk.exec(check_sql).is_err() {
        assert!(std::time::Instant::now() < deadline, "{check_sql} never succeeded");
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
}

/// Go `addindextest1.TestAddIndexDistAutoPauseOnKVDiskFull`.
#[test]
fn test_add_index_dist_auto_pause_on_kv_disk_full() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("drop database if exists test;");
    tk.must_exec("create database test;");
    tk.must_exec("use test;");
    tk.must_exec("set global tidb_enable_dist_task=1;");
    let _run_step = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", |job| {
        let job = job.downcast_ref::<tidb_model::Job>().unwrap();
        if job.state == tidb_model::JobState::PAUSING && job.has_pause_reason(tidb_model::job::JOB_PAUSE_REASON_KV_DISK_FULL) {
            std::thread::sleep(std::time::Duration::from_millis(300));
        }
    });

    // single add index
    tk.must_exec("drop table if exists t;");
    tk.must_exec("create table t(a int, b int);");
    tk.must_exec("insert into t values (1, 1), (2, 2), (3, 3);");
    run_auto_pause_case(&mut tk, "alter table t add index idx_b(b);", "admin check index t idx_b;", |job| {
        assert_eq!(job.type_, tidb_model::ActionType::ACTION_ADD_INDEX);
    });

    // multi-schema add index
    tk.must_exec("drop table if exists t_multi;");
    tk.must_exec("create table t_multi(a int, b int, c int);");
    tk.must_exec("insert into t_multi values (1, 1, 1), (2, 2, 2), (3, 3, 3);");
    run_auto_pause_case(
        &mut tk,
        "alter table t_multi add column d int default 0, add index idx_b(b);",
        "admin check index t_multi idx_b;",
        |job| {
            assert_eq!(job.type_, tidb_model::ActionType::ACTION_MULTI_SCHEMA_CHANGE);
            let info = job.multi_schema_info.as_ref().expect("multi-schema info").read();
            assert_eq!(info.sub_jobs.len(), 2);
            assert_eq!(info.sub_jobs.get(0).unwrap().read().type_, tidb_model::ActionType::ACTION_ADD_COLUMN);
            assert_eq!(info.sub_jobs.get(1).unwrap().read().type_, tidb_model::ActionType::ACTION_ADD_INDEX);
        },
    );
    assert_eq!(tk.must_query("select d from t_multi order by a;"), vec![vec!["0".to_owned()]; 3]);
    tk.must_exec("set global tidb_enable_dist_task=0;");
}

/// Go `addindextest1.TestAddIndexDistPauseAndResume`, which Go skips.
#[test]
#[ignore = "unstable (Go t.Skip, TODO(tangenta))"]
fn test_add_index_dist_pause_and_resume() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let tk1 = std::sync::Arc::new(std::sync::Mutex::new(store.new_tk()));
    tk.must_exec("drop database if exists test;");
    tk.must_exec("create database test;");
    tk.must_exec("use test;");

    tk.must_exec("create table t(a bigint auto_random primary key) partition by hash(a) partitions 8;");
    for _ in 0..4 {
        tk.must_exec("insert into t values (), (), (), (), (), ()");
    }
    tk.must_exec("split table t between (3) and (8646911284551352360) regions 50;");

    let only_job_id = |tk1: &mut Tk| {
        let row = tk1.must_query("select job_id from mysql.tidb_ddl_job");
        assert_eq!(row.len(), 1);
        row[0][0].clone()
    };
    let (sync_tx, sync_rx) = crossbeam_channel::bounded::<()>(0);
    let counter = std::sync::atomic::AtomicI32::new(0);
    let finish = {
        let tk1 = std::sync::Arc::clone(&tk1);
        tidb_failpoint::enable_call_for_test(
            "github.com/pingcap/tidb/pkg/ddl/mockDMLExecutionAddIndexSubTaskFinish",
            move |_| {
                if counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1 <= 3 {
                    let mut tk1 = tk1.lock().unwrap();
                    let job_id = only_job_id(&mut tk1);
                    tk1.must_exec(&format!("admin pause ddl jobs {job_id}"));
                    drop(tk1);
                    let _ = sync_rx.recv();
                }
            },
        )
    };
    let paused_state = {
        let tk1 = std::sync::Arc::clone(&tk1);
        tidb_failpoint::enable_call_for_test(
            "github.com/pingcap/tidb/pkg/dxf/framework/scheduler/mockDMLExecutionOnPausedState",
            move |_| {
                let mut tk1 = tk1.lock().unwrap();
                let job_id = only_job_id(&mut tk1);
                tk1.must_exec(&format!("admin resume ddl jobs {job_id}"));
            },
        )
    };
    let sync_pause = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/syncDDLTaskPause", move |_| {
        // make sure the task is paused.
        let _ = sync_tx.send(());
    });
    tk.must_exec("set global tidb_enable_dist_task=1;");
    tk.must_exec("alter table t add index idx1(a);");
    tk.must_exec("admin check table t;");
    drop(finish);
    drop(paused_state);

    // dist task succeed, job paused and resumed.
    let _schema_synced = {
        let tk1 = std::sync::Arc::clone(&tk1);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", move |job| {
            if job.downcast_ref::<tidb_model::Job>().unwrap().is_paused() {
                let mut tk1 = tk1.lock().unwrap();
                let job_id = only_job_id(&mut tk1);
                tk1.must_exec(&format!("admin resume ddl jobs {job_id}"));
            }
        })
    };
    let once = std::sync::Once::new();
    let pause_after = {
        let tk1 = std::sync::Arc::clone(&tk1);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/pauseAfterDistTaskFinished", move |_| {
            once.call_once(|| {
                let mut tk1 = tk1.lock().unwrap();
                let job_id = only_job_id(&mut tk1);
                tk1.must_exec(&format!("admin pause ddl jobs {job_id}"));
            });
        })
    };
    tk.must_exec("alter table t add index idx3(a);");
    tk.must_exec("admin check table t;");
    drop(pause_after);
    drop(sync_pause);
    tk.must_exec("set global tidb_enable_dist_task=0;");
}

/// Go `addindextest1.TestAddIndexDistLockAcquireFailed`.
#[test]
fn test_add_index_dist_lock_acquire_failed() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("set global tidb_enable_dist_task = on;");
    let retryable_errs = ["requested lease not found", "mvcc: required revision has been compacted"];
    for err_str in retryable_errs {
        tk.must_exec("drop table if exists t;");
        tk.must_exec("create table t (a int, b int);");
        tk.must_exec("insert into t values (1, 1);");
        let once = std::sync::Once::new();
        let _fp = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/owner/mockAcquireDistLockFailed", move |err_p| {
            once.call_once(|| {
                *err_p.downcast_mut::<Option<String>>().unwrap() = Some(err_str.to_owned());
            });
        });
        tk.must_exec("alter table t add index idx(b);");
    }
    tk.must_exec("set global tidb_enable_dist_task = off;");
}

/// Go `addindextest1.TestAddIndexScheduleAway`.
#[test]
fn test_add_index_schedule_away() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("set global tidb_enable_dist_task = on;");
    tk.must_exec("drop table if exists t;");
    tk.must_exec("create table t (a int, b int);");
    tk.must_exec("insert into t values (1, 1);");

    let job_id = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
    // Acquire the job ID.
    let _run_step = {
        let job_id = std::sync::Arc::clone(&job_id);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", move |job| {
            let job = job.downcast_ref::<tidb_model::Job>().unwrap();
            if job.type_ == tidb_model::ActionType::ACTION_ADD_INDEX {
                job_id.store(job.id, std::sync::atomic::Ordering::SeqCst);
            }
        })
    };
    // Do not balance subtasks automatically.
    let _no_slots = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/dxf/framework/scheduler/mockNoEnoughSlots", "return");
    let (after_cancel_tx, after_cancel) = std::sync::mpsc::sync_channel::<()>(1);
    // Capture the cancel operation from checkBalanceLoop.
    let _after_cancel = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/afterCancelSubtaskExec",
        move |_| {
            let _ = after_cancel_tx.try_send(());
        },
    );
    let once = std::sync::Once::new();
    let after_cancel = std::sync::Mutex::new(after_cancel);
    // The connection opens here: the store is not shared across threads.
    let tk1 = std::sync::Mutex::new(store.new_tk());
    let _finish = {
        let job_id = std::sync::Arc::clone(&job_id);
        tidb_failpoint::enable_call_for_test(
            "github.com/pingcap/tidb/pkg/ddl/mockDMLExecutionAddIndexSubTaskFinish",
            move |_| {
                once.call_once(|| {
                    let mut tk1 = tk1.lock().unwrap();
                    tk1.must_exec("use test");
                    let job_id = job_id.load(std::sync::atomic::Ordering::SeqCst);
                    // Go restores ':4000', the test server's exec ID: this node's.
                    let exec_id = tk1.must_query(&format!(
                        "select exec_id from mysql.tidb_background_subtask where task_key in
                            (select id from mysql.tidb_global_task where task_key like '%{job_id}')"
                    ))[0][0]
                        .clone();
                    tk1.must_exec(&format!(
                        "update mysql.tidb_background_subtask set exec_id = 'other' where task_key in
                            (select id from mysql.tidb_global_task where task_key like '%{job_id}')"
                    ));
                    after_cancel.lock().unwrap().recv().unwrap();
                    tk1.must_exec(&format!(
                        "update mysql.tidb_background_subtask set exec_id = '{exec_id}' where task_key in
                            (select id from mysql.tidb_global_task where task_key like '%{job_id}')"
                    ));
                });
            },
        )
    };
    // Go's `require` runs in the executor's goroutine; here the executor's
    // observations are asserted on the test's thread.
    let canceled = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let _after_run = {
        let canceled = std::sync::Arc::clone(&canceled);
        tidb_failpoint::enable_call_ref_for_test::<tidb_dxf_taskexecutor::AfterRunSubtask>(
            "github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/afterRunSubtask",
            std::sync::Arc::new(move |_, _, ctx| {
                let cause = ctx.cause();
                canceled.lock().unwrap().push((
                    ctx.err().is_some(),
                    cause.is_some_and(|cause| {
                        cause.downcast_ref::<tidb_gocontext::CtxError>() == Some(&tidb_gocontext::CtxError::Canceled)
                    }),
                ));
            }),
        )
    };
    tk.must_exec("alter table t add index idx(b);");
    assert_ne!(job_id.load(std::sync::atomic::Ordering::SeqCst), 0);
    let canceled = canceled.lock().unwrap();
    assert!(!canceled.is_empty());
    assert!(canceled.iter().all(|&(err, cause_canceled)| err && cause_canceled), "{canceled:?}");
    tk.must_exec("set global tidb_enable_dist_task = off;");
}

/// Go `addindextest1.TestAddIndexDistCleanUpBlock`.
#[test]
fn test_add_index_dist_clean_up_block() {
    let restore = tidb_dxf::set_max_concurrent_task_for_test(1);
    let _num_cpu = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(1)");
    let (ch_tx, ch) = crossbeam_channel::bounded::<()>(0);
    let _clean_up = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/dxf/framework/scheduler/doCleanupTask",
        move |_| {
            let _ = ch.recv();
        },
    );
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("drop database if exists test;");
    tk.must_exec("create database test;");
    tk.must_exec("use test;");
    tk.must_exec("set global tidb_enable_dist_task=1;");
    let tks: Vec<Tk> = (0..4).map(|_| store.new_tk()).collect();
    std::thread::scope(|wg| {
        for (i, mut tk) in tks.into_iter().enumerate() {
            wg.spawn(move || {
                tk.must_exec(&format!("create table test.t{i} (a int, b int);"));
                tk.must_exec(&format!("insert into test.t{i} values (1, 1);"));
                tk.must_exec(&format!("alter table test.t{i} add index idx(b);"));
            });
        }
    });
    drop(ch_tx);
    tk.must_exec("set global tidb_enable_dist_task=0;");
    restore();
}

/// Go `addindextest1.TestUseClusterIdInGlobalSortPath`.
#[test]
fn test_use_cluster_id_in_global_sort_path() {
    let store = realtikv!();
    *tidb_vardef::CLOUD_STORAGE_URI.write().unwrap() = "s3://bucket/path/to/folder?access-key=aaaaa&secret-access-key=bbbbb&endpoint=http://abc.com&force-path-style=false&region=Beijing&provider=aws".to_owned();
    let path = tidb_dxf_handle::get_cloud_storage_uri(Some(store.tikv_store().cluster_id()));
    assert_eq!(
        path,
        format!(
            "s3://bucket/path/to/folder/dxf/{}/?access-key=aaaaa&secret-access-key=bbbbb&endpoint=http://abc.com&force-path-style=false&region=Beijing&provider=aws",
            store.tikv_store().cluster_id()
        )
    );
    // Without a cluster ID.
    assert_eq!(
        tidb_dxf_handle::get_cloud_storage_uri(None),
        "s3://bucket/path/to/folder/dxf/?access-key=aaaaa&secret-access-key=bbbbb&endpoint=http://abc.com&force-path-style=false&region=Beijing&provider=aws"
    );
    tidb_vardef::CLOUD_STORAGE_URI.write().unwrap().clear();
}
