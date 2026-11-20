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

//! Go master `pkg/ddl/tests/materializedviewlog/materialized_view_create_test.go`.

use super::create::persisted_table;
use super::*;

fn mview_test_kit() -> (UnistoreClusterStack, Arc<ConfiguredUserStore>, Session) {
    let (stack, users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["set tidb_mview_enable = on", "use test"]);
    (stack, users, tk)
}

fn region_names(tk: &mut Session, table: &str) -> Vec<String> {
    let rows = answer(tk, &format!("show table `{table}` regions"));
    let rows: Vec<Vec<Option<String>>> = serde_json::from_str(rows.strip_prefix("RS:").unwrap_or_else(|| panic!("{rows}"))).unwrap();
    rows.into_iter().map(|row| row[1].clone().unwrap_or_default()).collect()
}

fn show_create(tk: &mut Session, table: &str) -> String {
    let rows = answer(tk, &format!("show create table `{table}`"));
    let rows: Vec<Vec<String>> = serde_json::from_str(rows.strip_prefix("RS:").unwrap()).unwrap();
    rows[0][1].clone()
}

#[test]
fn test_create_materialized_view_log_pre_split_options() {
    // Go sets `ddl.EnableSplitTableRegion`; this node splits tables when
    // `split-table` is on, its default.
    assert!(tidb_config::config_tree::config::get_global_config().split_table);
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "set @@session.tidb_scatter_region='table'",
        "create table t_mlog_presplit (a int, b int)",
        "create materialized view log on t_mlog_presplit (a) shard_row_id_bits = 2 pre_split_regions = 2 purge next date_add(now(), interval 1 hour)",
    ]);
    let create = show_create(&mut tk, "$mlog$t_mlog_presplit");
    assert!(create.contains("SHARD_ROW_ID_BITS=2") && create.contains("PRE_SPLIT_REGIONS=2"), "{create}");
    let mlog = latest_table(&stack, "$mlog$t_mlog_presplit");
    assert_eq!((mlog.shard_row_id_bits, mlog.pre_split_regions), (2, 2));
    let names = region_names(&mut tk, "$mlog$t_mlog_presplit");
    for handle in ["2305843009213693952", "4611686018427387904", "6917529027641081856"] {
        assert!(names.contains(&format!("t_{}_r_{handle}", mlog.id)), "{names:?}");
    }

    // "The MV physical table follows the same pre-split region flow as its
    // MLog."
    must_exec_all(&mut tk, &[
        "create table t_mv_presplit (a int, b int)",
        "create materialized view log on t_mv_presplit (a)",
        "create materialized view mv_presplit (a, cnt) shard_row_id_bits = 2 pre_split_regions = 2 as select a, count(1) from t_mv_presplit group by a",
    ]);
    let create = show_create(&mut tk, "mv_presplit");
    assert!(create.contains("SHARD_ROW_ID_BITS=2") && create.contains("PRE_SPLIT_REGIONS=2"), "{create}");
    let mv = latest_table(&stack, "mv_presplit");
    assert_eq!((mv.shard_row_id_bits, mv.pre_split_regions), (2, 2));
    let names = region_names(&mut tk, "mv_presplit");
    for handle in ["2305843009213693952", "4611686018427387904", "6917529027641081856"] {
        assert!(names.contains(&format!("t_{}_r_{handle}", mv.id)), "{names:?}");
    }
}

fn purge_check(tk: &mut Session, mlog_id: i64, columns: &str) -> String {
    answer(tk, &format!("select {columns} from mysql.tidb_mlog_purge_info where MLOG_ID = {mlog_id}"))
}

#[test]
fn test_create_materialized_view_log_purge_info_next_unix_seconds_uses_utc() {
    let (stack, _users, mut tk) = mview_test_kit();
    let mlog_id = |stack: &UnistoreClusterStack, base: &str| latest_table(stack, &format!("$mlog${base}")).id;
    must_exec_all(&mut tk, &[
        "set time_zone = '+08:00'",
        "create table t_purge_schedule_next (a int)",
        "create materialized view log on t_purge_schedule_next (a) purge next cast('2030-01-02 10:00:00' as datetime)",
    ]);
    assert_eq!(
        purge_check(&mut tk, mlog_id(&stack, "t_purge_schedule_next"), "NEXT_PURGE_UNIX_SECONDS = 1893578400, NEXT_PURGE_UNIX_SECONDS = 1893549600"),
        r#"RS:[["1","0"]]"#
    );
    must_exec_all(&mut tk, &[
        "create table t_purge_schedule_start (a int)",
        "create materialized view log on t_purge_schedule_start (a) purge start with cast('2030-01-02 10:00:00' as datetime) next cast('2030-01-03 10:00:00' as datetime)",
    ]);
    assert_eq!(
        purge_check(&mut tk, mlog_id(&stack, "t_purge_schedule_start"), "NEXT_PURGE_UNIX_SECONDS = 1893578400, NEXT_PURGE_UNIX_SECONDS = 1893636000"),
        r#"RS:[["1","0"]]"#
    );
    must_exec_all(&mut tk, &[
        "set time_zone = 'America/Los_Angeles'",
        "create table t_purge_dst_gap (a int)",
        "create materialized view log on t_purge_dst_gap (a) purge next cast('2021-03-14 02:30:00' as datetime)",
    ]);
    assert_eq!(purge_check(&mut tk, mlog_id(&stack, "t_purge_dst_gap"), "NEXT_PURGE_UNIX_SECONDS = 1615689000"), r#"RS:[["1"]]"#);
    must_exec_all(&mut tk, &[
        "create table t_purge_utc_now (a int)",
        "create materialized view log on t_purge_utc_now (a) purge next date_add(now(), interval 40 minute)",
    ]);
    assert_eq!(
        purge_check(
            &mut tk,
            mlog_id(&stack, "t_purge_utc_now"),
            "NEXT_PURGE_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 30 minute), NEXT_PURGE_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 50 minute)"
        ),
        r#"RS:[["1","1"]]"#
    );
    let _now = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/expression/injectNow", "return(1636275480)");
    must_exec_all(&mut tk, &[
        "set time_zone = 'America/Los_Angeles'",
        "create table t_purge_dst_fallback (a int)",
        "create materialized view log on t_purge_dst_fallback (a) purge next date_add(now(), interval 5 minute)",
    ]);
    assert_eq!(purge_check(&mut tk, mlog_id(&stack, "t_purge_dst_fallback"), "NEXT_PURGE_UNIX_SECONDS = 1636275780"), r#"RS:[["1"]]"#);
}

#[test]
fn test_create_materialized_view_log_purge_info_failure_rollback() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &["create table t (a int not null, b int not null)"]);
    let _missing = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/mockInsertMLogPurgeTableNotExists", "return(true)");
    exec_err_contains(&mut tk, "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)", "tidb_mlog_purge_info");
    assert_eq!(answer(&mut tk, "show tables like '$mlog$t'"), "RS:[]");
    assert_eq!(single_value(&mut tk, "select count(*) from mysql.tidb_mlog_purge_info"), "0");
    assert!(latest_table(&stack, "t").materialized_view_base.is_none());
}

/// Go's `beforeRunOneJobStep` callback pausing the second attempt of a
/// rolling-back CREATE MATERIALIZED VIEW LOG.
fn pause_second_log_rollback_attempt() -> (Latch, Latch, Arc<std::sync::atomic::AtomicI64>, tidb_failpoint::Guard) {
    let (started, allow) = (Latch::default(), Latch::default());
    let mlog_id = Arc::new(std::sync::atomic::AtomicI64::new(0));
    let attempts = std::sync::atomic::AtomicI64::new(0);
    let guard = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", {
        let (started, allow, mlog_id) = (started.clone(), allow.clone(), Arc::clone(&mlog_id));
        move |job| {
            let job = job.downcast_mut::<tidb_model::Job>().unwrap();
            if job.type_ != tidb_model::ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG || !job.is_rollingback() {
                return;
            }
            if attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1 != 2 {
                return;
            }
            mlog_id.store(job.table_id, std::sync::atomic::Ordering::SeqCst);
            started.open();
            allow.wait();
        }
    });
    (started, allow, mlog_id, guard)
}

#[test]
fn test_create_materialized_view_log_rollback_purge_info_failure_rolls_back_metadata() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &["create table t_create_mlog_rollback_atomic (a int not null, b int not null)"]);
    let db_id = stack.factory.catalog.get_latest().schema_by_name("test").unwrap().read().id;
    let base_id = latest_table(&stack, "t_create_mlog_rollback_atomic").id;
    let _upsert = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/mockInsertMLogPurgeTableNotExists", "1*return(true)");
    let cleanup = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/ddl/mockDeleteMaterializedViewLogPurgeInfoErr",
        r#"1*return("mock purge info delete error")"#,
    );
    let (retry_started, allow_retry, mlog_id, _pause) = pause_second_log_rollback_attempt();
    let mut inspect = stack.factory.open_session(session_context(2901)).unwrap();
    let ddl = std::thread::spawn(move || {
        answer(&mut tk, "create materialized view log on t_create_mlog_rollback_atomic (a, b) purge next date_add(now(), interval 1 hour)")
    });
    retry_started.wait_for("CREATE MATERIALIZED VIEW LOG rollback retry");
    let mlog_id = mlog_id.load(std::sync::atomic::Ordering::SeqCst);
    assert_ne!(mlog_id, 0);
    assert!(persisted_table(&mut inspect, db_id, mlog_id).is_none());
    assert!(persisted_table(&mut inspect, db_id, base_id).unwrap().materialized_view_base.is_none());
    drop(cleanup);
    allow_retry.open();
    let result = ddl.join().unwrap();
    assert!(result.starts_with("ERR"), "{result}");
}

#[test]
fn test_create_materialized_view_log_rollback_update_schema_version_failure_retries() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &["create table t_create_mlog_rollback_retry (a int not null, b int not null)"]);
    let db_id = stack.factory.catalog.get_latest().schema_by_name("test").unwrap().read().id;
    let base_id = latest_table(&stack, "t_create_mlog_rollback_retry").id;
    let _upsert = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/mockInsertMLogPurgeTableNotExists", "1*return(true)");
    let injected = std::sync::atomic::AtomicBool::new(false);
    let _version = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeUpdateSchemaVersion", move |call| {
        let (job, error) = call.downcast_mut::<(tidb_model::Job, Option<String>)>().unwrap();
        if job.type_ == tidb_model::ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG
            && job.is_rollback_done()
            && injected.compare_exchange(false, true, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_ok()
        {
            *error = Some("mock materialized view log rollback schema version error".to_owned());
        }
    });
    let (retry_started, allow_retry, mlog_id, _pause) = pause_second_log_rollback_attempt();
    let mut inspect = stack.factory.open_session(session_context(2902)).unwrap();
    must_exec_all(&mut inspect, &["set tidb_mview_enable = on", "use test"]);
    let ddl = std::thread::spawn(move || {
        let result = answer(&mut tk, "create materialized view log on t_create_mlog_rollback_retry (a, b) purge next date_add(now(), interval 1 hour)");
        (tk, result)
    });
    retry_started.wait_for("CREATE MATERIALIZED VIEW LOG rollback retry");
    let mlog_id = mlog_id.load(std::sync::atomic::Ordering::SeqCst);
    assert_ne!(mlog_id, 0);
    assert_eq!(single_value(&mut inspect, &format!("select count(*) from mysql.tidb_mlog_purge_info where mlog_id = {mlog_id}")), "0");
    assert!(persisted_table(&mut inspect, db_id, mlog_id).is_none());
    assert!(persisted_table(&mut inspect, db_id, base_id).unwrap().materialized_view_base.is_none());
    allow_retry.open();
    let (mut tk, result) = ddl.join().unwrap();
    assert!(result.starts_with("ERR"), "{result}");
    assert_eq!(answer(&mut tk, "show tables like '$mlog$t_create_mlog_rollback_retry'"), "RS:[]");
    // "A full InfoSchema reload can hide an invalid rollback diff."
    let domain = tk.domain_services().unwrap().domain.upgrade().unwrap();
    let ts = domain.transactions.fresh_timestamp().unwrap();
    let mut meta = domain.get_snapshot_meta(ts).unwrap();
    let diff = tidb_infoschema::cluster_catalog::with_meta_reader(&mut meta, |reader| {
        let version = reader.schema_version()?;
        reader.schema_diff(version)
    })
    .unwrap()
    .expect("the rollback's diff");
    assert_eq!(diff.action_type, tidb_model::ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG);
    assert_eq!(diff.old_table_id, mlog_id);
    assert_eq!(diff.table_id, 0);
}

#[test]
fn test_create_materialized_view_log_privilege() {
    let (stack, users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &["create table t_create_mlog_priv (a int)"]);
    let names = ["u_create_mlog_no_create", "u_create_mlog_no_select", "u_create_mlog_table_create", "u_create_mlog_ok"];
    for user in names {
        // The node logs in by password; Go's testkit `Auth` takes none.
        must_exec_all(&mut tk, &[&format!("create user '{user}'@'%' identified by 'abc'")]);
    }
    let login = |connection_id: u64, user: &str| {
        let mut session = stack
            .factory
            .open_session(super::super::super::node_fixture::session_context_with_identity(connection_id, log_in(&users, user)))
            .unwrap();
        must_exec_all(&mut session, &["set tidb_mview_enable = on"]);
        session
    };
    must_exec_all(&mut tk, &["grant select on test.t_create_mlog_priv to 'u_create_mlog_no_create'@'%'"]);
    let error = answer(&mut login(2903, "u_create_mlog_no_create"), "create materialized view log on test.t_create_mlog_priv (a)");
    assert!(error.contains("CREATE MATERIALIZED VIEW LOG command denied") && error.contains("t_create_mlog_priv"), "{error}");
    assert!(!error.contains("$mlog$"), "{error}");

    must_exec_all(&mut tk, &["grant create view on test.* to 'u_create_mlog_no_select'@'%'"]);
    exec_err_contains(&mut login(2904, "u_create_mlog_no_select"), "create materialized view log on test.t_create_mlog_priv (a)", "SELECT command denied");

    must_exec_all(&mut tk, &[
        "grant create view on test.* to 'u_create_mlog_ok'@'%'",
        "grant select on test.t_create_mlog_priv to 'u_create_mlog_ok'@'%'",
    ]);
    must_exec_all(&mut login(2905, "u_create_mlog_ok"), &["create materialized view log on test.t_create_mlog_priv (a)"]);

    must_exec_all(&mut tk, &[
        "grant create view on test.t_create_mlog_priv to 'u_create_mlog_table_create'@'%'",
        "grant select on test.t_create_mlog_priv to 'u_create_mlog_table_create'@'%'",
    ]);
    let error = answer(&mut login(2906, "u_create_mlog_table_create"), "create materialized view log on test.t_create_mlog_priv (a)");
    assert!(error.contains("CREATE MATERIALIZED VIEW LOG command denied") && error.contains("t_create_mlog_priv"), "{error}");
    assert!(!error.contains("$mlog$"), "{error}");
    for user in names {
        must_exec_all(&mut tk, &[&format!("drop user '{user}'@'%'")]);
    }
}

#[test]
fn test_create_materialized_view_and_log() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "set div_precision_increment = 9",
        "set time_zone = 'Asia/Shanghai'",
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10), (1, 5), (2, 7)",
        "set tidb_mview_enable = off",
    ]);
    exec_err_contains(&mut tk, "create materialized view log on t (a, b)", "tidb_mview_enable");
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
        "set tidb_mview_enable = off",
    ]);
    exec_err_contains(&mut tk, "create materialized view mv_disabled (a, s, cnt) as select a, sum(b), count(1) from t group by a", "tidb_mview_enable");
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
    ]);
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), r#"RS:[["1","15","2"],["2","7","1"]]"#);
    let (base, mlog, mv) = (latest_table(&stack, "t"), latest_table(&stack, "$mlog$t"), latest_table(&stack, "mv"));
    assert_eq!(mlog_id(&base), mlog.id);
    assert!(mview_ids(&base).contains(&mv.id));
    assert_eq!(mlog.materialized_view_log.as_ref().unwrap().read().base_table_id, base.id);
    assert_eq!(dependent_mview_ids(&mlog), vec![mv.id]);
    let info = mv.materialized_view.as_ref().unwrap().read().clone();
    assert_eq!(info.init_build_state, tidb_model::table::MViewInitBuildState::READY);
    assert_eq!(info.definition_div_precision_increment, 9);
    assert_eq!(info.base_table_ids.iter().copied().collect::<Vec<_>>(), vec![base.id]);
    assert_eq!(info.refresh_method, "FAST");
    assert!(info.refresh_start_with.is_empty());
    assert_eq!(info.refresh_next, "DATE_ADD(NOW(), INTERVAL 1 HOUR)");
    assert_eq!((info.alert_warning_sec, info.alert_overdue_sec, info.alert_refresh_failed), (0, 0, false));
    assert_eq!(info.definition_sql_mode, tk.vars().sql_mode().0);
    assert_eq!(info.refresh_schedule_sql_mode, tk.vars().sql_mode().0);
    assert_eq!(info.definition_time_zone.name.to_string(), "Asia/Shanghai");
    assert_eq!(info.definition_time_zone.offset, 0);
    assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mlog_purge_info where mlog_id = {}", mlog.id)), "1");
    assert_eq!(
        single_value(&mut tk, &format!("select last_success_read_tso > 0 from mysql.tidb_mview_refresh_info where mview_id = {}", mv.id)),
        "1"
    );
}

#[test]
fn test_create_materialized_view_log_if_not_exists_with_ddl_checker() {
    // Go's `mockstore.WithDDLChecker()` re-checks each DDL's schema; the
    // statements and answers are what this port compares.
    let (_stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t (a int, b int)",
        "create materialized view log if not exists on t (a)",
        "create materialized view log if not exists on t (b)",
    ]);
    assert_eq!(answer(&mut tk, "show warnings"), r#"RS:[["Note","1050","Table 'test.$mlog$t' already exists"]]"#);
    assert!(answer(&mut tk, "create materialized view log on t (b)").starts_with("ERR 1050 "));
    assert!(answer(&mut tk, "create materialized view log if not exists on missing (a)").starts_with("ERR 1146 "));
}
