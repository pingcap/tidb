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

//! Go master `pkg/ddl/tests/materializedviewlog/mview_output_column_test.go`
//! and `mview_output_log_test.go`.

use super::create::new_mview_test_kit;
use super::*;

/// Go `outputLogAlterSQL`.
const OUTPUT_LOG_ALTER_SQL: &str = "alter table t_output_batch modify column a bigint not null, modify column b bigint not null";

#[test]
fn test_materialized_view_output_log_modify_column_runtime_dependency() {
    for cache_size in [0_i64, 1 << 30] {
        // t.Run(fmt.Sprintf("schema_cache_size=%d", cacheSize))
        let (stack, mut tk) = mview_output_log_session();
        must_exec_all(&mut tk, &[
            &format!("set @@global.tidb_schema_cache_size={cache_size}"),
            "create table t_output_batch (a int not null, b int not null)",
            "create materialized view log on t_output_batch (a, b)",
            "create materialized view mv_output_known (k, l, cnt) as select a, b, count(1) from t_output_batch group by a, b",
            "create materialized view log on mv_output_known (k, l)",
        ]);
        let mut tk_alter = new_mview_test_kit(&stack, 3201);
        // Go `buildOutputLogModifyJob`.
        let job = capture_ddl_job(&mut tk_alter, OUTPUT_LOG_ALTER_SQL);
        must_exec_all(&mut tk, &[
            // "This sibling MV is absent from the submitted ALTER's
            // scheduling set."
            "create materialized view mv_output_late (k, l, cnt) as select a, b, count(1) from t_output_batch group by a, b",
            // "Only the second, SkipVersion sub-job modifies this sibling
            // and its MLog."
            "create materialized view mv_output_late_b (l, cnt) as select b, count(1) from t_output_batch group by b",
        ]);
        for info in job.job.get_involving_schema_info().snapshot().iter() {
            assert!(info.table.to_string() != "mv_output_late" && info.table.to_string() != "mv_output_late_b", "cache_size={cache_size}");
        }
        let table_id = job.job.table_id;
        let alter_job_id = Arc::new(std::sync::atomic::AtomicI64::new(0));
        let (boundary, resume) = (Latch::default(), Latch::default());
        let paused = std::sync::atomic::AtomicBool::new(false);
        let _boundary = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeTransitOneJobStep", {
            let (boundary, resume, alter_job_id) = (boundary.clone(), resume.clone(), Arc::clone(&alter_job_id));
            move |arg| {
                let job_w = arg.downcast_mut::<tidb_model::JobW>().unwrap();
                let job = job_w.job.as_ref().unwrap().read();
                if job.type_ != tidb_model::ActionType::ACTION_MULTI_SCHEMA_CHANGE || job.table_id != table_id || job.is_rollingback() {
                    return;
                }
                let Some(info) = &job.multi_schema_info else { return };
                let info = info.read();
                if !info.revertible || info.sub_jobs.iter_deref().any(|sub| sub.read().revertible || sub.read().is_finished()) {
                    return;
                }
                if !paused.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    alter_job_id.store(job.id, std::sync::atomic::Ordering::SeqCst);
                    boundary.open();
                    resume.wait();
                }
            }
        });
        struct Release(Latch);
        impl Drop for Release {
            fn drop(&mut self) {
                self.0.open();
            }
        }
        let release = Release(resume);
        let alter = std::thread::spawn(move || deliver_captured_job(&mut tk_alter, job));
        boundary.wait_for("ALTER at its batch boundary");
        // "CREATE MLog can run because the late MV is outside the submitted
        // set."
        must_exec_all(&mut tk, &[
            "create materialized view log on mv_output_late (k, l)",
            "create materialized view log on mv_output_late_b (l)",
        ]);
        let names = [
            "$mlog$t_output_batch",
            "mv_output_known",
            "$mlog$mv_output_known",
            "mv_output_late",
            "$mlog$mv_output_late",
            "mv_output_late_b",
            "$mlog$mv_output_late_b",
        ];
        // "Prime V2 caches before applying the diff."
        let mut affected: Vec<i64> = names.iter().map(|name| latest_table(&stack, name).id).collect();
        drop(release);
        alter.join().unwrap().expect("the ALTER finishes");
        let history_meta = single_value(
            &mut tk,
            &format!("select hex(job_meta) from mysql.tidb_ddl_history where job_id = {}", alter_job_id.load(std::sync::atomic::Ordering::SeqCst)),
        );
        let bytes: Vec<u8> = (0..history_meta.len()).step_by(2).map(|i| u8::from_str_radix(&history_meta[i..i + 2], 16).unwrap()).collect();
        let mut history = tidb_model::Job::default();
        history.decode(&bytes).unwrap();
        let version = history.binlog_info.as_ref().unwrap().read().schema_version;
        let domain = tk.domain_services().unwrap().domain.upgrade().unwrap();
        let ts = domain.transactions.fresh_timestamp().unwrap();
        let mut meta = domain.get_snapshot_meta(ts).unwrap();
        let diff = tidb_infoschema::cluster_catalog::with_meta_reader(&mut meta, |reader| reader.schema_diff(version))
            .unwrap()
            .expect("the ALTER's diff");
        let mut got: Vec<i64> = diff.affected_options.iter_deref().map(|option| option.read().table_id).collect();
        affected.sort_unstable();
        got.sort_unstable();
        assert_eq!(got, affected, "cache_size={cache_size}");
        assert_eq!(
            answer(
                &mut tk,
                "select distinct data_type from information_schema.columns where table_schema='test' and table_name in ('t_output_batch', '$mlog$t_output_batch', 'mv_output_known', '$mlog$mv_output_known', 'mv_output_late', '$mlog$mv_output_late', 'mv_output_late_b', '$mlog$mv_output_late_b') and column_name in ('a', 'b', 'k', 'l')"
            ),
            r#"RS:[["bigint"]]"#,
            "cache_size={cache_size}"
        );
    }
}

#[test]
fn test_create_materialized_view_log_refresh_lock_lifetime() {
    for rollback in [false, true] {
        // t.Run(fmt.Sprintf("rollback=%t", rollback))
        let (stack, mut tk) = mview_output_log_session();
        must_exec_all(&mut tk, &[
            "create table t_mlog_lock (a int not null, b int not null)",
            "create materialized view log on t_mlog_lock (a, b)",
            "create materialized view mv_mlog_lock (a, s, cnt) as select a, sum(b), count(1) from t_mlog_lock group by a",
        ]);
        let mv = latest_table(&stack, "mv_mlog_lock");
        let schema_id = stack.factory.catalog.get_latest().schema_by_name("test").unwrap().read().id;
        let lock_name = format!("mv_refresh_{schema_id}_{}", mv.id);
        let (paused, resume) = (Latch::default(), Latch::default());
        let signalled = std::sync::atomic::AtomicBool::new(false);
        let _sync = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeWaitSchemaSynced", {
            let (paused, resume) = (paused.clone(), resume.clone());
            move |arg| {
                let (job, _) = arg.downcast_mut::<(tidb_model::Job, i64)>().unwrap();
                if job.type_ == tidb_model::ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG
                    && job.table_name.to_string() == "$mlog$mv_mlog_lock"
                    && !signalled.swap(true, std::sync::atomic::Ordering::SeqCst)
                {
                    paused.open();
                    resume.wait();
                }
            }
        });
        let failure = rollback
            .then(|| tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/mockInsertMLogPurgeTableNotExists", "return(true)"));
        struct Release(Latch);
        impl Drop for Release {
            fn drop(&mut self) {
                self.0.open();
            }
        }
        let release = Release(resume);
        let ddl = std::thread::spawn(move || {
            let result = answer(&mut tk, "create materialized view log on mv_mlog_lock (a, s, cnt)");
            (tk, result)
        });
        paused.wait_for("CREATE MLog schema sync");
        let mut tk_lock = new_mview_test_kit(&stack, 3202);
        // "The job must hold the refresh lock after its metadata transaction
        // commits."
        let refused = answer(&mut tk_lock, "refresh materialized view mv_mlog_lock fast");
        assert!(refused.starts_with("ERR 8182 "), "rollback={rollback}: {refused}");
        drop(release);
        let (mut tk, result) = ddl.join().unwrap();
        if rollback {
            assert!(result.contains("tidb_mlog_purge_info"), "{result}");
            drop(failure);
            assert_eq!(answer(&mut tk, "show tables like '$mlog$mv_mlog_lock'"), "RS:[]");
        } else {
            assert_eq!(result, "OK");
        }
        // "the worker must release the lock before reuse"
        assert_eq!(single_value(&mut tk_lock, &format!("select get_lock('{lock_name}', 1)")), "1");
        assert_eq!(single_value(&mut tk_lock, &format!("select release_lock('{lock_name}')")), "1");
        if rollback {
            must_exec_all(&mut tk, &["create materialized view log on mv_mlog_lock (a, s, cnt)"]);
        }
    }
}

#[test]
fn test_materialized_view_log_on_ready_materialized_view() {
    let (stack, mut tk) = mview_output_log_session();
    must_exec_all(&mut tk, &[
        "create table t_mv_mlog_ddl (a int not null, b int not null)",
        "insert into t_mv_mlog_ddl values (1, 10)",
        "create materialized view log on t_mv_mlog_ddl (a, b)",
        "create materialized view mv_mlog_ddl (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_mlog_ddl group by a",
        "create materialized view log on mv_mlog_ddl (a)",
    ]);
    let (mlog, mv) = (latest_table(&stack, "$mlog$mv_mlog_ddl"), latest_table(&stack, "mv_mlog_ddl"));
    assert_eq!(mlog_id(&mv), mlog.id);
    assert_eq!(mlog.materialized_view_log.as_ref().unwrap().read().base_table_id, mv.id);
    let show = |tk: &mut Session| {
        let rows = answer(tk, "show create materialized view log on mv_mlog_ddl");
        let rows: Vec<Vec<String>> = serde_json::from_str(rows.strip_prefix("RS:").unwrap()).unwrap();
        rows[0][1].clone()
    };
    assert!(show(&mut tk).contains("CREATE MATERIALIZED VIEW LOG ON `mv_mlog_ddl` (`a`)"));
    must_exec_all(&mut tk, &["alter materialized view log on mv_mlog_ddl add column (s, cnt)"]);
    assert!(show(&mut tk).contains("CREATE MATERIALIZED VIEW LOG ON `mv_mlog_ddl` (`a`, `s`, `cnt`)"));
    must_exec_all(&mut tk, &["alter materialized view log on mv_mlog_ddl purge next date_add(now(), interval 1 hour)"]);
    assert_eq!(
        single_value(&mut tk, &format!("select NEXT_PURGE_UNIX_SECONDS is not null from mysql.tidb_mlog_purge_info where MLOG_ID={}", mlog.id)),
        "1"
    );
    must_exec_all(&mut tk, &["purge materialized view log on mv_mlog_ddl"]);
    for sql in ["drop materialized view mv_mlog_ddl", "drop materialized view if exists mv_mlog_ddl"] {
        exec_err_contains(&mut tk, sql, "DROP MATERIALIZED VIEW with materialized view log");
    }
    let after = latest_table(&stack, "mv_mlog_ddl");
    assert_eq!(after.id, mv.id);
    assert_eq!(mlog_id(&after), mlog.id);
    let purge_count = format!("select count(*) from mysql.tidb_mlog_purge_info where MLOG_ID={}", mlog.id);
    assert_eq!(single_value(&mut tk, &purge_count), "1");
    assert_eq!(answer(&mut tk, "select * from mv_mlog_ddl"), r#"RS:[["1","10","1"]]"#);
    must_exec_all(&mut tk, &["drop materialized view log on mv_mlog_ddl"]);
    let after = latest_table(&stack, "mv_mlog_ddl");
    assert!(after.materialized_view_base.is_none() || mlog_id(&after) == 0);
    assert_eq!(single_value(&mut tk, &purge_count), "0");
    assert_eq!(answer(&mut tk, "select * from mv_mlog_ddl"), r#"RS:[["1","10","1"]]"#);
    must_exec_all(&mut tk, &["drop materialized view mv_mlog_ddl"]);
    assert_eq!(answer(&mut tk, "show tables like 'mv_mlog_ddl'"), "RS:[]");
    assert_eq!(answer(&mut tk, "show tables like '$mlog$mv_mlog_ddl'"), "RS:[]");
    must_exec_all(&mut tk, &["drop materialized view log on t_mv_mlog_ddl", "drop table t_mv_mlog_ddl"]);
}

#[test]
fn test_drop_materialized_view_rechecks_owned_log() {
    let (stack, mut tk) = mview_output_log_session();
    must_exec_all(&mut tk, &[
        "create table t_drop_mv_mlog (a int)",
        "create materialized view log on t_drop_mv_mlog (a)",
        "create materialized view mv_drop_mlog (a, cnt) as select a, count(1) from t_drop_mv_mlog group by a",
    ]);
    let mv_id = latest_table(&stack, "mv_drop_mlog").id;
    let (create_started, allow_create) = (Latch::default(), Latch::default());
    let _pause = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", {
        let (started, allow) = (create_started.clone(), allow_create.clone());
        move |job| {
            let job = job.downcast_mut::<tidb_model::Job>().unwrap();
            if job.type_ != tidb_model::ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG || job.table_name.to_string() != "$mlog$mv_drop_mlog" {
                return;
            }
            started.open();
            allow.wait();
        }
    });
    struct Allow(Latch);
    impl Drop for Allow {
        fn drop(&mut self) {
            self.0.open();
        }
    }
    let allow_create = Allow(allow_create);
    let mut tk_create = new_mview_test_kit(&stack, 3203);
    let create = std::thread::spawn(move || answer(&mut tk_create, "create materialized view log on mv_drop_mlog (a, cnt)"));
    create_started.wait_for("CREATE MATERIALIZED VIEW LOG worker");
    // "Enqueue DROP with a stale executor precheck, behind the blocked
    // CREATE."
    let mut tk_drop = new_mview_test_kit(&stack, 3204);
    let drop_mv = std::thread::spawn(move || answer(&mut tk_drop, "drop materialized view mv_drop_mlog"));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while single_value(&mut tk, "select count(*) from mysql.tidb_ddl_job") != "2" {
        assert!(std::time::Instant::now() < deadline, "the DROP was never queued");
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    drop(allow_create);
    assert_eq!(create.join().unwrap(), "OK");
    let error = drop_mv.join().unwrap();
    assert!(error.contains("DROP MATERIALIZED VIEW with materialized view log"), "{error}");
    let mv = latest_table(&stack, "mv_drop_mlog");
    assert_eq!(mv.id, mv_id);
    assert!(mv.materialized_view_base.is_some());
    let mlog = stack.factory.catalog.get_latest().table_info_by_id(mlog_id(&mv)).expect("the view's log").read().clone();
    assert_eq!(mlog.materialized_view_log.as_ref().expect("a log").read().base_table_id, mv_id);
    assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mlog_purge_info where MLOG_ID={}", mlog.id)), "1");
    must_exec_all(&mut tk, &["drop materialized view log on mv_drop_mlog", "drop materialized view mv_drop_mlog"]);
}
