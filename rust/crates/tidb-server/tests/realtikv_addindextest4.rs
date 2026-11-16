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

//! Go `tests/realtikvtest/addindextest4`, over [`crate::realtikvtest`].

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use crate::realtikvtest::Tk;

/// Go `TestAddIndexIngestRecoverPartition`, which Go skips.
#[test]
#[ignore = "the test is too hacky, use another way to test (Go t.Skip)"]
fn test_add_index_ingest_recover_partition() {}

/// Go `TestMultiSchemaChangeTwoIndexes`.
#[test]
#[ignore = "TIDB_ENCODE_INDEX_KEY is not ported (register row E26)"]
fn test_multi_schema_change_two_indexes() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test;");
    tk.must_exec("set @@global.tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("set @@global.tidb_enable_dist_task=on;");

    let create_tables = [
        "create table t (id int, b int, c int, primary key(id) clustered);",
        "create table t (id int, b int, c int, primary key(id) clustered) partition by range(id) (
			PARTITION p0 VALUES LESS THAN (10),
			PARTITION p1 VALUES LESS THAN MAXVALUE
		)",
    ];
    let create_indexes = [
        "alter table t add unique index b(b), add index c(c);",
        "alter table t add unique index b(b) global, add index c(c) global;",
    ];

    for i in 0..create_tables.len() {
        tk.must_exec("drop table if exists t;");
        tk.must_exec(create_tables[i]);
        tk.must_exec("insert into t values (1,1,1)");

        // Go's `assert` records a failure without stopping the job.
        let errors = Arc::new(Mutex::new(Vec::<String>::new()));
        let hex_key = Arc::new(Mutex::new(String::new()));
        let run_dml_before_scan = Arc::new(AtomicBool::new(false));
        let tk1 = Arc::new(Mutex::new(store.new_tk()));
        let _scan = {
            let (tk1, errors, hex_key, run) = (Arc::clone(&tk1), Arc::clone(&errors), Arc::clone(&hex_key), Arc::clone(&run_dml_before_scan));
            tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeAddIndexScan", move |_| {
                if run.swap(true, Ordering::SeqCst) {
                    return;
                }
                let mut tk1 = tk1.lock().unwrap();
                tk1.must_exec("use test;");
                let key = tk1.must_query("select tidb_encode_index_key('test', 't', 'b', 1, null);")[0][0].clone();
                *hex_key.lock().unwrap() = key.clone();
                for sql in ["delete from t where id = 1;", "insert into t values (2,1,1);"] {
                    if let Err(err) = tk1.exec(sql) {
                        errors.lock().unwrap().push(err);
                    }
                }
                eprintln!("after first insertion {}", tk1.must_query(&format!("select tidb_mvcc_info('{key}')"))[0][0]);
                if let Err(err) = tk1.exec("delete from t where id = 2;") {
                    errors.lock().unwrap().push(err);
                }
                eprintln!("after second insertion {}", tk1.must_query(&format!("select tidb_mvcc_info('{key}')"))[0][0]);
            })
        };
        let run_dml_before_merge = Arc::new(AtomicBool::new(false));
        let tk2 = Arc::new(Mutex::new(store.new_tk()));
        let _merge = {
            let (tk2, errors, hex_key, run) = (Arc::clone(&tk2), Arc::clone(&errors), Arc::clone(&hex_key), Arc::clone(&run_dml_before_merge));
            tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeBackfillMerge", move |_| {
                if run.swap(true, Ordering::SeqCst) {
                    return;
                }
                let mut tk2 = tk2.lock().unwrap();
                tk2.must_exec("use test;");
                if let Err(err) = tk2.exec("insert into t values (3, 1, 1);") {
                    errors.lock().unwrap().push(err);
                }
                let key = hex_key.lock().unwrap().clone();
                eprintln!("after third insertion {}", tk2.must_query(&format!("select tidb_mvcc_info('{key}')"))[0][0]);
            })
        };

        let _skip = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/skipReorgWorkForTempIndex", "return(false)");

        tk.must_exec(create_indexes[i]);
        tk.must_exec("admin check table t;");
        assert!(errors.lock().unwrap().is_empty(), "{:?}", errors.lock().unwrap());
    }
}

/// Go `TestFixAdminAlterDDLJobs`.
#[test]
fn test_fix_admin_alter_ddl_jobs() {
    let store = realtikv!();
    let mut tk1 = store.new_tk();
    tk1.must_exec("use test");
    tk1.must_exec("create table t (a int);");
    tk1.must_exec("insert into t values (1);");
    tk1.must_exec("set @@global.tidb_enable_dist_task=off;");
    let _interval = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/updateProgressIntervalInMs", "return(100)");

    struct TestCase {
        stuck_fp: &'static str,
        check_reorg_meta_fp: &'static str,
        sql: &'static str,
        set_vars: &'static str,
        revert_vars: &'static str,
    }
    let test_cases = [
        TestCase {
            stuck_fp: "github.com/pingcap/tidb/pkg/ddl/mockIndexIngestWorkerFault",
            check_reorg_meta_fp: "github.com/pingcap/tidb/pkg/ddl/checkReorgConcurrency",
            sql: "alter table t add index idx_a(a)",
            set_vars: "",
            revert_vars: "",
        },
        TestCase {
            stuck_fp: "github.com/pingcap/tidb/pkg/ddl/mockUpdateColumnWorkerStuck",
            check_reorg_meta_fp: "github.com/pingcap/tidb/pkg/ddl/checkReorgWorkerCnt",
            sql: "alter table t modify a varchar(30)",
            set_vars: "",
            revert_vars: "",
        },
        TestCase {
            stuck_fp: "github.com/pingcap/tidb/pkg/ddl/mockAddIndexTxnWorkerStuck",
            check_reorg_meta_fp: "github.com/pingcap/tidb/pkg/ddl/checkReorgWorkerCnt",
            sql: "alter table t add index idx(a)",
            set_vars: "set @@global.tidb_ddl_enable_fast_reorg=off",
            revert_vars: "set @@global.tidb_ddl_enable_fast_reorg=on",
        },
    ];

    for tc in test_cases {
        if !tc.set_vars.is_empty() {
            tk1.must_exec(tc.set_vars);
        }
        let (ch_tx, ch) = crossbeam_channel::bounded::<()>(0);
        let _stuck = tidb_failpoint::enable_call_for_test(tc.stuck_fp, move |_| {
            let _ = ch.recv();
        });
        let real_worker_cnt = Arc::new(AtomicI64::new(0));
        let real_batch_size = Arc::new(AtomicI64::new(0));
        let real_max_write_speed = Arc::new(AtomicI64::new(0));
        let mut tk2 = store.new_tk();
        std::thread::scope(|wg| {
            let ddl = wg.spawn(|| tk1.must_exec(tc.sql));
            let _check = {
                let (worker_cnt, batch_size, max_write_speed) =
                    (Arc::clone(&real_worker_cnt), Arc::clone(&real_batch_size), Arc::clone(&real_max_write_speed));
                tidb_failpoint::enable_call_for_test(tc.check_reorg_meta_fp, move |job| {
                    let job = job.downcast_ref::<tidb_model::Job>().unwrap();
                    let meta = job.reorg_meta.as_ref().unwrap().read();
                    let defaults = tidb_model::DDLReorgProcessDefaults::new(
                        || i64::from(tidb_vardef::get_ddl_reorg_worker_counter()),
                        || i64::from(tidb_vardef::get_ddl_reorg_batch_size()),
                    );
                    worker_cnt.store(meta.get_concurrency(defaults), Ordering::SeqCst);
                    batch_size.store(meta.get_batch_size(defaults), Ordering::SeqCst);
                    max_write_speed.store(meta.get_max_write_speed(), Ordering::SeqCst);
                })
            };

            let job_id = loop {
                let row = tk2.must_query("select job_id from mysql.tidb_ddl_job");
                if row.len() == 1 {
                    break row[0][0].clone();
                }
            };
            let (worker_cnt, batch_size, max_write_speed) = (7, 89, 1011);
            tk2.must_exec(&format!("admin alter ddl jobs {job_id} thread = {worker_cnt}"));
            tk2.must_exec(&format!("admin alter ddl jobs {job_id} batch_size = {batch_size}"));
            tk2.must_exec(&format!("admin alter ddl jobs {job_id} max_write_speed = {max_write_speed}"));
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            while !(real_worker_cnt.load(Ordering::SeqCst) == worker_cnt
                && real_batch_size.load(Ordering::SeqCst) == batch_size
                && real_max_write_speed.load(Ordering::SeqCst) == max_write_speed)
            {
                assert!(
                    std::time::Instant::now() < deadline,
                    "{}: thread {} batch_size {} max_write_speed {}",
                    tc.stuck_fp,
                    real_worker_cnt.load(Ordering::SeqCst),
                    real_batch_size.load(Ordering::SeqCst),
                    real_max_write_speed.load(Ordering::SeqCst)
                );
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            drop(ch_tx);
            ddl.join().unwrap();
        });
        if !tc.revert_vars.is_empty() {
            tk1.must_exec(tc.revert_vars);
        }
    }
    tk1.must_exec("set @@global.tidb_enable_dist_task = on;");
    tk1.must_exec("drop table t;");
}

/// Go `TestMultiSchemaChangeAnalyzeOnlyOnce`.
#[test]
fn test_multi_schema_change_analyze_only_once() {
    let store = realtikv!();
    let mut tk1 = store.new_tk();
    tk1.must_exec("use test");
    tk1.must_exec("set @@tidb_stats_update_during_ddl = true;");
    let mut db_cnt = 0;

    let mut check_fn = |tk1: &mut Tk, sql: &str, contain_res: &str| {
        db_cnt += 1;
        let db_name = format!("test_{db_cnt}");
        tk1.must_exec(&format!("drop database if exists {db_name}"));
        tk1.must_exec(&format!("create database {db_name}"));
        tk1.must_exec(&format!("use {db_name}"));
        tk1.must_exec("create table t (a bigint, b bigint, c bigint, d bigint, key i_a(a), key i_b(b), key i_c(c));");
        tk1.must_exec("insert into t values (1, 1, 11111, 1);");
        let begin = tk1.must_query("select now();")[0][0].clone();
        tk1.must_exec(sql);
        let analyze_status_rs = tk1.must_query(&format!(
            "show analyze status where start_time >= '{begin}' and table_schema = '{db_name}';"
        ));
        tk1.must_exec(&format!("drop database {db_name}"));
        if contain_res.is_empty() {
            assert_eq!(analyze_status_rs.len(), 0, "{sql}: {analyze_status_rs:?}");
            return;
        }
        assert_eq!(analyze_status_rs.len(), 1, "{sql}: {analyze_status_rs:?}");
        assert!(analyze_status_rs[0][3].contains(contain_res), "{sql}: {analyze_status_rs:?}");
    };

    // Index reorg.
    check_fn(&mut tk1, "alter table t modify column a int unsigned", "a");
    check_fn(&mut tk1, "alter table t add index i_a_2(a), add index i_b_2(b), modify column c char(5), modify column d char(5)", "all columns");
    check_fn(&mut tk1, "alter table t modify column c int, modify column a char(5), add index i_d_1(d)", "all columns");
    check_fn(&mut tk1, "alter table t modify column c char(5), modify column a int, modify column b int", "all columns");
    check_fn(&mut tk1, "alter table t modify column a bigint, modify column c char(5), modify column b int unsigned", "all columns");
    check_fn(&mut tk1, "alter table t modify column a char(5), modify column d char(5)", "all columns");
    check_fn(&mut tk1, "alter table t modify column a int, modify column b int unsigned", "all columns");

    // No index reorg.
    check_fn(&mut tk1, "alter table t modify column a int", "");
    check_fn(&mut tk1, "alter table t modify column a bigint", "");
    check_fn(&mut tk1, "alter table t modify column a int, modify column d char(5)", "");
    check_fn(&mut tk1, "alter table t modify column a int, modify column d int unsigned", "");
}

/// Go `setupAddIndexClassicVars`.
fn setup_add_index_classic_vars(tk: &mut Tk, dist_task_on: bool) {
    tk.must_exec("set global tidb_ddl_enable_fast_reorg = 1");
    if dist_task_on {
        tk.must_exec("set global tidb_enable_dist_task = 1");
    } else {
        tk.must_exec("set global tidb_enable_dist_task = 0");
    }
}

/// Go `TestAddIndexResumesFromCheckpointAfterPartialImport`.
#[test]
fn test_add_index_resumes_from_checkpoint_after_partial_import() {
    let store = realtikv!();
    for dist_task_on in [false, true] {
        let mut tk = store.new_tk();
        tk.must_exec("use test");

        setup_add_index_classic_vars(&mut tk, dist_task_on);
        tidb_ddl_ingest::backend::FORCE_SYNC_FLAG_FOR_TEST.store(true, Ordering::SeqCst);

        tk.must_exec("drop table if exists t");
        tk.must_exec("create table t (a bigint primary key, b bigint)");
        for i in 0..2000 {
            tk.must_exec(&format!("insert into t values ({i}, {i})"));
        }

        // Fire once: make the subtask fail before checkpoint is updated, so task will be restarted from checkpoint.
        let _fp = tidb_failpoint::enable_for_test(
            "github.com/pingcap/tidb/pkg/ddl/ingest/ddlIngestFailOnceBeforeCheckpointUpdated",
            "1*return",
        );

        tk.must_exec("alter table t add unique index idx_b(b)");

        let tbl_cnt = tk.must_query("select count(*) from t")[0][0].clone();
        let idx_cnt = tk.must_query("select count(*) from t use index(idx_b)")[0][0].clone();
        assert_eq!(tbl_cnt, idx_cnt, "dist_task_on={dist_task_on}");

        tk.must_exec("admin check table t");
    }
}

/// Go `TestAddIndexResumesFromCheckpointAfterPartialScan`.
#[test]
fn test_add_index_resumes_from_checkpoint_after_partial_scan() {
    let store = realtikv!();
    for dist_task_on in [false, true] {
        let mut tk = store.new_tk();
        tk.must_exec("use test");

        setup_add_index_classic_vars(&mut tk, dist_task_on);
        tidb_ddl_ingest::backend::FORCE_SYNC_FLAG_FOR_TEST.store(true, Ordering::SeqCst);

        tk.must_exec("drop table if exists t");
        tk.must_exec("create table t (a bigint primary key, b bigint)");
        for i in 0..2000 {
            tk.must_exec(&format!("insert into t values ({i}, {i})"));
        }

        // Let the first chunk fetch in scanRecords succeed, then inject an
        // error on the second fetch; the retry must restart without a flushed
        // chunk advancing the checkpoint.
        let _fp = tidb_failpoint::enable_for_test(
            "github.com/pingcap/tidb/pkg/ddl/mockScanRecordPartialError",
            "1*return(false)->1*return(true)",
        );

        tk.must_exec("alter table t add unique index idx_b(b)");

        let tbl_cnt = tk.must_query("select count(*) from t")[0][0].clone();
        let idx_cnt = tk.must_query("select count(*) from t use index(idx_b)")[0][0].clone();
        assert_eq!(tbl_cnt, idx_cnt, "dist_task_on={dist_task_on}");

        tk.must_exec("admin check table t");
    }
}

/// Go `TestAddIndexShowAnalyzeProgress`.
#[test]
fn test_add_index_show_analyze_progress() {
    let store = realtikv!();
    let mut tk1 = store.new_tk();
    tk1.must_exec("use test");
    tk1.must_exec("create table t (a int, b int, key idx_b(b));");
    tk1.must_exec("insert into t values (1, 1), (2, 2), (3, 3);");
    tk1.must_exec("set @@tidb_stats_update_during_ddl = 1;");
    let begin = Arc::new(Mutex::new(tk1.must_query("select now();")[0][0].clone()));
    let job_id = Arc::new(AtomicI64::new(0));
    let modify_column_job = |job_id: &Arc<AtomicI64>| {
        let job_id = Arc::clone(job_id);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", move |job| {
            let job = job.downcast_ref::<tidb_model::Job>().unwrap();
            if job_id.load(Ordering::SeqCst) == 0 && job.type_ == tidb_model::ActionType::ACTION_MODIFY_COLUMN {
                job_id.store(job.id, Ordering::SeqCst);
            }
        })
    };
    let _step = modify_column_job(&job_id);
    // Go's `require` runs in the analyze goroutine; here its observations
    // are asserted on the test's thread.
    let observed = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
    let tk2 = Mutex::new(store.new_tk());
    let _save = {
        let (observed, job_id, begin) = (Arc::clone(&observed), Arc::clone(&job_id), Arc::clone(&begin));
        tidb_failpoint::enable_call_for_test(
            "github.com/pingcap/tidb/pkg/statistics/handle/storage/saveAnalyzeResultToStorage",
            move |_| {
                let mut tk2 = tk2.lock().unwrap();
                tk2.must_exec("use test");
                let begin = begin.lock().unwrap().clone();
                let analyze_status_rs = tk2.must_query(&format!("show analyze status where start_time >= '{begin}';"));
                let show_rs = tk2.must_query(&format!(
                    "admin show ddl jobs where job_id = {}",
                    job_id.load(Ordering::SeqCst)
                ));
                observed.lock().unwrap().push((analyze_status_rs[0][7].clone(), show_rs[0][12].clone()));
            },
        )
    };
    let check_observed = || {
        let mut observed = observed.lock().unwrap();
        assert!(!observed.is_empty(), "analyzed");
        for (state, show) in observed.drain(..) {
            assert_eq!(state, "running");
            assert!(show.contains("analyzing"), "{show}");
        }
    };
    tk1.must_exec("alter table t modify column b char(16);");
    check_observed();

    tk1.must_exec("drop table if exists t;");
    tk1.must_exec("create table t (a int, b int, key idx_b(b));");
    tk1.must_exec("insert into t values (1, 1), (2, 2), (3, 3);");
    *begin.lock().unwrap() = tk1.must_query("select now();")[0][0].clone();
    job_id.store(0, Ordering::SeqCst);
    let _after = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterAnalyzeTable", |err| {
        *err.downcast_mut::<Option<tidb_terror::MysqlError>>().unwrap() = Some(tidb_terror::MysqlError::unknown("mock err"));
    });
    tk1.must_exec("alter table t modify column b char(16);");
    check_observed();
    let show = tk1.must_query(&format!("admin show ddl jobs where job_id = {}", job_id.load(Ordering::SeqCst)))[0][12].clone();
    assert!(show.contains("analyze_failed"), "{show}");
}

/// Go `TestAnalyzeTimeout`.
#[test]
fn test_analyze_timeout() {
    let store = realtikv!();
    let mut tk1 = store.new_tk();
    tk1.must_exec("use test");
    tk1.must_exec("drop table if exists t_timeout;");
    tk1.must_exec("create table t_timeout (a int, b varchar(16), key idx_b(b));");
    tk1.must_exec("insert into t_timeout values (1, '1'), (2, '2'), (3, '3');");
    tk1.must_exec("set @@tidb_stats_update_during_ddl = 1;");

    let job_id = Arc::new(AtomicI64::new(0));
    let job_of = |job_id: &Arc<AtomicI64>, tp: tidb_model::ActionType| {
        let job_id = Arc::clone(job_id);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", move |job| {
            let job = job.downcast_ref::<tidb_model::Job>().unwrap();
            if job_id.load(Ordering::SeqCst) == 0 && job.type_ == tp {
                job_id.store(job.id, Ordering::SeqCst);
            }
        })
    };
    let step = job_of(&job_id, tidb_model::ActionType::ACTION_MODIFY_COLUMN);

    struct Restore(u64);
    impl Drop for Restore {
        fn drop(&mut self) {
            tidb_ddl::DEFAULT_ANALYZE_CHECK_INTERVAL_MILLIS.store(self.0, Ordering::SeqCst);
        }
    }
    let _restore = Restore(tidb_ddl::DEFAULT_ANALYZE_CHECK_INTERVAL_MILLIS.swap(10, Ordering::SeqCst));

    let (analyzed_tx, analyzed_notify) = crossbeam_channel::bounded::<()>(0);
    let analyzed_tx = Mutex::new(Some(analyzed_tx));
    let _after = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterAnalyzeTable", move |_| {
        // wait an extra second because analyze start_time is compared at second granularity
        std::thread::sleep(std::time::Duration::from_secs(1));
        analyzed_tx.lock().unwrap().take();
    });
    let _before = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeAnalyzeTable", |_| {
        std::thread::sleep(std::time::Duration::from_millis(100));
    });
    let _timeout = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/mockAnalyzeTimeout", "return(1)");

    tk1.must_exec("alter table t_timeout modify column b char(16);");

    let eventually = |tk1: &mut Tk, timeout_secs: u64, mut f: Box<dyn FnMut(&mut Tk) -> bool + '_>| {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
        while !f(tk1) {
            assert!(std::time::Instant::now() < deadline, "condition never held");
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
    };
    let show_has_timeout = |tk1: &mut Tk, job_id: i64| {
        if job_id == 0 {
            return false;
        }
        let rows = tk1.must_query(&format!("admin show ddl jobs where job_id = {job_id}"));
        !rows.is_empty() && rows[0][12].contains("analyze_timeout")
    };
    eventually(&mut tk1, 30, Box::new(|tk1| show_has_timeout(tk1, job_id.load(Ordering::SeqCst))));
    eventually(&mut tk1, 60, Box::new(|tk1| !tk1.must_query("show stats_meta where table_name = 't_timeout'").is_empty()));
    eventually(&mut tk1, 30, Box::new(|_| matches!(analyzed_notify.try_recv(), Err(crossbeam_channel::TryRecvError::Disconnected))));

    drop(step);
    job_id.store(0, Ordering::SeqCst);
    let _step = job_of(&job_id, tidb_model::ActionType::ACTION_ADD_INDEX);
    tk1.must_exec("alter table t_timeout add index new_idx_b(b);");
    assert!(job_id.load(Ordering::SeqCst) > 0);
    eventually(&mut tk1, 30, Box::new(|tk1| show_has_timeout(tk1, job_id.load(Ordering::SeqCst))));
    eventually(&mut tk1, 60, Box::new(|tk1| !tk1.must_query("show stats_meta where table_name = 't_timeout'").is_empty()));
}

/// Go `TestCancelAfterReorgTimeout`.
#[test]
fn test_cancel_after_reorg_timeout() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test;");

    tk.must_exec(
        "create view all_global_tasks as select * from mysql.tidb_global_task union all select * from mysql.tidb_global_task_history;",
    );
    tk.must_exec("create table t (a int, b int);");
    tk.must_exec("insert into t values (1, 1);");

    // Mock subtask executor encounter the same error continuously.
    let (meet_err_tx, meet_err) = crossbeam_channel::bounded::<()>(0);
    let after_meet_err = AtomicBool::new(false);
    let _run = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/beforeReadIndexStepExecRunSubtask",
        move |err| {
            *err.downcast_mut::<Option<String>>().unwrap() = Some("mock err".to_owned());
            if !after_meet_err.swap(true, Ordering::SeqCst) {
                let _ = meet_err_tx.send(());
            }
        },
    );
    let _interval = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/updateProgressIntervalInMs", "return(10)"); // Speed up the test.
    let job_id = Arc::new(AtomicI64::new(0));
    let _step = {
        let job_id = Arc::clone(&job_id);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", move |job| {
            let job = job.downcast_ref::<tidb_model::Job>().unwrap();
            if job.type_ == tidb_model::ActionType::ACTION_ADD_INDEX {
                job_id.store(job.id, Ordering::SeqCst);
            }
        })
    };
    let (timeout_tx, timeout) = crossbeam_channel::bounded::<()>(0);
    let tk1 = Mutex::new(store.new_tk());
    let _submitted = {
        let job_id = Arc::clone(&job_id);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/dxf/framework/handle/afterDXFTaskSubmitted", move |_| {
            let _ = meet_err.recv();
            let _ = timeout.recv();
            let mut tk1 = tk1.lock().unwrap();
            tk1.must_exec("use test;");
            tk1.must_exec(&format!("admin cancel ddl jobs {};", job_id.load(Ordering::SeqCst)));
        })
    };
    let after_timeout = AtomicBool::new(false);
    let _timeout = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/onRunReorgJobTimeout", move |_| {
        if !after_timeout.swap(true, Ordering::SeqCst) {
            let _ = timeout_tx.send(());
        }
    });
    tk.must_get_err_code("alter table t add index idx(a);", tidb_errno::errcode::ErrCancelledDDLJob);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let result = tk.must_query("select state from all_global_tasks;");
        assert!(!result.is_empty());
        if ["succeed", "reverted", "failed"].contains(&result[0][0].as_str()) {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "{result:?}");
        std::thread::sleep(std::time::Duration::from_millis(300));
    }
}

// Go `partial_index_test.go`.

/// Go `validatePartialIndexExists`: the index carries a condition.
fn validate_partial_index_exists(tk: &mut Tk, table_name: &str, index_name: &str) {
    let rows = tk.must_query(&format!(
        "select predicate from information_schema.tidb_indexes where table_schema = 'test' and table_name = '{table_name}' and key_name = '{index_name}'"
    ));
    assert!(!rows.is_empty(), "{table_name}.{index_name} exists");
    assert!(!rows[0][0].is_empty() && rows[0][0] != "<nil>", "{table_name}.{index_name}: {rows:?}");
}

/// Go `findIndex(...) == nil`.
fn index_missing(tk: &mut Tk, table_name: &str, index_name: &str) -> bool {
    tk.must_query(&format!(
        "select 1 from information_schema.tidb_indexes where table_schema = 'test' and table_name = '{table_name}' and key_name = '{index_name}'"
    ))
    .is_empty()
}

/// Go `TestPartialIndexDDL`.
#[test]
fn test_partial_index_ddl() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let check_kv = |tk: &mut Tk, table: &str, index: &str, expected: usize| {
        assert_eq!(store.index_kv_count(tk, table, index), expected, "{table}.{index}");
    };

    // TestCreatePartialIndex
    tk.must_exec("use test");
    tk.must_exec("create table t1 (col1 int primary key, col2 int, key idx(col2) where col1 > 100);");
    validate_partial_index_exists(&mut tk, "t1", "idx");
    tk.must_exec("create table t2 (col1 int primary key, col2 int);");
    tk.must_exec("create index idx on t2(col2) where col1 > 100;");
    validate_partial_index_exists(&mut tk, "t2", "idx");
    tk.must_exec("create table t3 (col1 int primary key, col2 int);");
    tk.must_exec("alter table t3 add index idx(col2) where col1 > 100;");
    validate_partial_index_exists(&mut tk, "t3", "idx");
    tk.must_exec("create table t4 like t1");
    validate_partial_index_exists(&mut tk, "t4", "idx");
    for t in ["t4", "t3", "t2", "t1"] {
        tk.must_exec(&format!("drop table if exists {t};"));
    }

    // TestValidationInPartialIndex
    tk.must_exec("use test");
    tk.must_exec("create table t(col1 int primary key, col2 int, col3 varchar(255), col4 int as (col2 + 1));");
    let test_cases = [
        ("t(col2) where col2 = 1;", 0),
        ("t(col2) where col1 != 1;", 0),
        ("t(col2) where col1 > 1;", 0),
        ("t(col2) where col1 IS NULL;", 0),
        ("t(col2) where col1 IS NOT NULL;", 0),
        ("t(col2) where col1 IN (1,2,3,4,5);", 8200),
        ("t(col2) where col1 LIKE '1%';", 8200),
        ("t(col2) where col1 > col2;", 8200),
        ("t(col2) where col1 = NOW();", 8200),
        ("t(col2) where col1 = (select 1);", 8200),
        ("t(col2) where col4 = 4;", 8200),
    ];
    for (idx_def, error_code) in test_cases {
        let sql = format!("create index idx on {idx_def}");
        if error_code != 0 {
            tk.must_get_err_code(&sql, error_code);
            continue;
        }
        tk.must_exec(&sql);
        validate_partial_index_exists(&mut tk, "t", "idx");
        tk.must_exec("drop index idx on t;");
    }
    tk.must_exec("drop table if exists t;");

    // TestIndexManagementForPartialIndex
    tk.must_exec("use test");
    tk.must_exec("create table t(col1 int primary key, col2 int, col3 int);");
    tk.must_exec("create index idx on t(col3) where col2 = 1;");
    validate_partial_index_exists(&mut tk, "t", "idx");
    tk.must_exec("alter table t rename index idx to idx2;");
    validate_partial_index_exists(&mut tk, "t", "idx2");
    let first_column = |tk: &mut Tk, index: &str| {
        tk.must_query(&format!(
            "select column_name from information_schema.tidb_indexes where table_schema = 'test' and table_name = 't' and key_name = '{index}' and seq_in_index = 1"
        ))[0][0]
            .clone()
    };
    tk.must_exec("alter table t change column col3 col4 int;");
    validate_partial_index_exists(&mut tk, "t", "idx2");
    assert_eq!(first_column(&mut tk, "idx2"), "col4");
    tk.must_exec("alter table t modify column col4 int unsigned;");
    validate_partial_index_exists(&mut tk, "t", "idx2");
    assert_eq!(first_column(&mut tk, "idx2"), "col4");
    tk.must_exec("alter table t drop column col4;");
    assert!(index_missing(&mut tk, "t", "idx2"));
    tk.must_exec("create index idx on t(col2) where col2 = 1;");
    validate_partial_index_exists(&mut tk, "t", "idx");
    tk.must_exec("drop index idx on t;");
    assert!(index_missing(&mut tk, "t", "idx"));
    tk.must_exec("drop table if exists t;");

    // TestManipulateColumnReferencedByPartialIndex
    let referenced = tidb_errno::errcode::ErrModifyColumnReferencedByPartialCondition;
    tk.must_exec("use test");
    tk.must_exec("create table t(col1 int primary key, col2 int, col3 int);");
    tk.must_exec("create index idx on t(col3) where col2 = 1;");
    validate_partial_index_exists(&mut tk, "t", "idx");
    tk.must_get_err_code("alter table t drop column col2;", referenced);
    tk.must_get_err_code("alter table t change column col2 col4 int;", referenced);
    tk.must_get_err_code("alter table t modify column col2 int unsigned;", referenced);
    tk.must_exec("drop table t;");
    tk.must_exec("create table t(col1 int primary key, col2 int, col3 int, key t(col3) where col2 = 1);");
    tk.must_get_err_code("alter table t drop column col2;", referenced);
    tk.must_get_err_code("alter table t change column col2 col4 int;", referenced);
    tk.must_get_err_code("alter table t modify column col2 int unsigned;", referenced);
    tk.must_exec("drop table t;");

    // TestPartialIndexCanOnlyBeCreatedWithFastReorg
    tk.must_exec("use test;");
    tk.must_exec("create table t (a int, b int, c int, primary key (a))");
    tk.must_exec("insert into t(a, b, c) values (1, 2, 3), (2, 3, 4), (3, 4, 5)");
    tk.must_exec("set global tidb_ddl_enable_fast_reorg = 0");
    tk.must_get_err_code("alter table t add index idx1(a) where c > 3", 8200);
    tk.must_exec("set global tidb_ddl_enable_fast_reorg = 1");
    tk.must_exec("alter table t add index idx0(a)");
    check_kv(&mut tk, "t", "idx0", 3);
    assert_eq!(tk.must_query("admin show ddl jobs 1")[0][7], "3");
    tk.must_exec("drop table if exists t;");

    // TestAddPartialIndex
    tk.must_exec("use test;");
    tk.must_exec("create table t (a int, b int, c int, primary key (a))");
    tk.must_exec("insert into t(a, b, c) values (1, 2, 3), (2, 3, 4), (3, 4, 5)");
    // Partial index with proper condition
    tk.must_exec("alter table t add index idx1(a) where c >= 5");
    check_kv(&mut tk, "t", "idx1", 1);
    assert_eq!(tk.must_query("admin show ddl jobs 1")[0][7], "3");
    // Partial index with condition that no row meets
    tk.must_exec("alter table t add index idx2(a) where c >= 6");
    check_kv(&mut tk, "t", "idx2", 0);
    assert_eq!(tk.must_query("admin show ddl jobs 1")[0][7], "3");
    // Multi-schema change add multiple partial indexes
    tk.must_exec("alter table t add index idx5(a) where c >= 4, add index idx6(a) where c >= 5");
    check_kv(&mut tk, "t", "idx5", 2);
    check_kv(&mut tk, "t", "idx6", 1);
    assert_eq!(tk.must_query("admin show ddl jobs 1")[1][7], "3");
    // Pushdown is disabled for `not` function, so the DDL cannot pushdown
    tk.must_exec("INSERT INTO mysql.expr_pushdown_blacklist VALUES('not','tikv','');");
    tk.must_exec("ADMIN reload expr_pushdown_blacklist;");
    tk.must_exec("alter table t add index idx7(a) where b is not null;");
    check_kv(&mut tk, "t", "idx7", 3);
    tk.must_exec("DELETE FROM mysql.expr_pushdown_blacklist WHERE name='not' AND store_type='tikv';");
    // Multi-schema change add multiple indexes, including partial index and normal index
    tk.must_exec("alter table t add index idx8(a), add index idx9(a) where c >= 5");
    check_kv(&mut tk, "t", "idx8", 3);
    check_kv(&mut tk, "t", "idx9", 1);
    assert_eq!(tk.must_query("admin show ddl jobs 1")[1][7], "3");
    // Create index on table with `_tidb_rowid` column
    tk.must_exec("create table t1 (a int, b int, c int)");
    tk.must_exec("insert into t1(a, b, c) values (1, 2, 3), (2, 3, 4), (3, 4, 5)");
    tk.must_exec("alter table t1 add index idx1(a) where a > 1");
    check_kv(&mut tk, "t1", "idx1", 2);
    // Create normal index, the row count is still correct.
    tk.must_exec("alter table t1 add index idx2(a)");
    check_kv(&mut tk, "t1", "idx2", 3);
    assert_eq!(tk.must_query("admin show ddl jobs 1")[0][7], "3");
    tk.must_exec("drop table if exists t;");

    // TestValidateColumnExistsInAddIndex
    tk.must_exec("use test;");
    tk.must_exec("create table t (a int, b int);");
    tk.must_exec("alter table t add index idx_b(b) where a = 1;");
    tk.must_get_err_code("alter table t add index idx_b_2(b) where c = 1;", 8200);
    tk.must_exec("drop table if exists t;");
}
