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


//! Tests of `tidb_executor::stmt_ctx_build` over a session.

use tidb_planner::stmt_ctx::PlannerStmtCtx as _;
use super::*;
use tidb_executor::stmt_ctx_build::ResetContextOfStmt;


#[test]
fn index_usage_collector_tracks_execution_info_switch_per_statement() {
    let global = tidb_stats_handle_usage_indexusage::Collector::new();
    global.start_worker();
    let mut session = Session::new();
    session
        .run("SET tidb_enable_collect_execution_info = ON")
        .unwrap();
    session.set_session_index_usage_collector(global.spawn_session_collector());
    assert!(session
        .statement_context(false)
        .index_usage_collector()
        .is_some());
    assert!(session
        .statement_context(false)
        .executor_runtime_stats_source()
        .is_some());

    session
        .run("SET tidb_enable_collect_execution_info = OFF")
        .unwrap();
    assert!(session
        .statement_context(false)
        .index_usage_collector()
        .is_none());
    assert!(session
        .statement_context(false)
        .executor_runtime_stats_source()
        .is_none());

    session
        .run("SET tidb_enable_collect_execution_info = ON")
        .unwrap();
    assert!(session
        .statement_context(false)
        .index_usage_collector()
        .is_some());

    drop(session);
    global.close();
}

#[test]
fn mpp_enforcement_requires_both_session_switches() {
    let mut session = Session::new();
    let state = |session: &Session| {
        let context = session.statement_context(false);
        let options = &context.optimizer_cost_env().session;
        (options.mpp_allowed, options.mpp_enforced)
    };
    session.run("SET tidb_allow_mpp = ON").unwrap();
    session.run("SET tidb_enforce_mpp = OFF").unwrap();
    assert_eq!(state(&session), (true, false));
    session.run("SET tidb_enforce_mpp = ON").unwrap();
    let original = session.statement_context(false);
    assert_eq!(state(&session), (true, true));
    session.run("SET tidb_allow_mpp = OFF").unwrap();
    assert_eq!(state(&session), (false, false));
    assert!(original.optimizer_cost_env().session.mpp_allowed);
    assert!(original.optimizer_cost_env().session.mpp_enforced);
    session.run("SET tidb_allow_mpp = ON").unwrap();
    assert_eq!(state(&session), (true, true));
    session.run("SET tidb_enforce_mpp = OFF").unwrap();
    session.run("SET tidb_allow_mpp = OFF").unwrap();
    assert_eq!(state(&session), (false, false));
}

#[test]
fn cardinality_estimator_options_follow_session_variables() {
    let mut session = Session::new();
    session.run("SET time_zone = '+00:00'").unwrap();
    let original = session.statement_context(false);
    let original_options = original
        .optimizer_cost_env()
        .session
        .estimator_options
        .clone();
    assert_eq!(
        original_options.time_zone.as_ref(),
        &session.session_time_zone()
    );
    assert_eq!(original_options.risk_eq_skew_ratio, 0.0);
    assert_eq!(original_options.risk_range_skew_ratio, 0.0);
    assert!(original_options.allow_use_modify_count);
    session.run("SET time_zone = '+08:00'").unwrap();

    session
        .run("SET tidb_opt_risk_eq_skew_ratio = 0.25")
        .unwrap();
    session
        .run("SET tidb_opt_risk_range_skew_ratio = 0.5")
        .unwrap();
    session
        .run("SET tidb_opt_objective = 'determinate'")
        .unwrap();

    let options = session
        .statement_context(false)
        .optimizer_cost_env()
        .session
        .estimator_options
        .clone();
    assert_eq!(options.time_zone.as_ref(), &session.session_time_zone());
    assert_ne!(options.time_zone, original_options.time_zone);
    assert_eq!(options.risk_eq_skew_ratio, 0.25);
    assert_eq!(options.risk_range_skew_ratio, 0.5);
    assert!(!options.allow_use_modify_count);
    assert_eq!(
        original.optimizer_cost_env().session.estimator_options,
        original_options
    );
}

#[test]
fn group_ndv_skew_ratio_is_snapshotted_for_planning() {
    let mut session = Session::new();
    let original = session.statement_context(false);
    assert_eq!(
        original.optimizer_cost_env().session.group_ndv_skew_ratio,
        tidb_vardef::defaults::DEF_OPT_RISK_GROUP_NDV_SKEW_RATIO
    );

    session
        .run("SET tidb_opt_group_ndv_skew_ratio = 0.4")
        .unwrap();
    session
        .run("SET tidb_opt_scale_ndv_skew_ratio = 0.6")
        .unwrap();
    let current_context = session.statement_context(false);
    let current = current_context.optimizer_cost_env();
    assert_eq!(current.session.group_ndv_skew_ratio, 0.4);
    assert_eq!(current.session.scale_ndv_skew_ratio, 0.6);
    assert_eq!(
        original.optimizer_cost_env().session.scale_ndv_skew_ratio,
        1.0
    );
    assert_eq!(current.session.shuffle_options.group_ndv_skew_ratio, 0.4);
    assert_eq!(
        original.optimizer_cost_env().session.group_ndv_skew_ratio,
        tidb_vardef::defaults::DEF_OPT_RISK_GROUP_NDV_SKEW_RATIO
    );
}

#[test]
fn correlation_estimator_options_follow_session_variables() {
    let mut session = Session::new();
    let original = session.statement_context(false);
    let original_options = original.optimizer_cost_env().session.correlation_options;
    assert_eq!(original_options, Default::default());

    session
        .run("SET tidb_opt_enable_correlation_adjustment = OFF")
        .unwrap();
    session
        .run("SET tidb_opt_correlation_threshold = 0.75")
        .unwrap();
    session
        .run("SET tidb_opt_correlation_exp_factor = 3")
        .unwrap();

    let options = session
        .statement_context(false)
        .optimizer_cost_env()
        .session
        .correlation_options;
    assert!(!options.enabled);
    assert_eq!(options.threshold, 0.75);
    assert_eq!(options.exponent, 3);
    assert_eq!(
        original.optimizer_cost_env().session.correlation_options,
        original_options
    );
}

#[test]
fn ordering_index_selectivity_threshold_is_snapshotted_per_statement() {
    let mut session = Session::new();
    let original = session.statement_context(false);
    assert_eq!(original.ordering_index_selectivity_threshold(), 0.0);

    session
        .run("SET tidb_opt_ordering_index_selectivity_threshold = 0.1")
        .unwrap();

    assert_eq!(
        session
            .statement_context(false)
            .ordering_index_selectivity_threshold(),
        0.1
    );
    assert_eq!(original.ordering_index_selectivity_threshold(), 0.0);
}

#[test]
fn range_quota_uses_session_snapshot_and_refreshes_after_set() {
    let mut session = crate::Session::new();
    let original = session.statement_context(false);
    assert_eq!(original.range_max_size(), 67_108_864);
    for quota in [1_i64, 0, i64::MAX] {
        session
            .run(&format!("SET tidb_opt_range_max_size = {quota}"))
            .unwrap();
        assert_eq!(session.statement_context(false).range_max_size(), quota);
        assert_eq!(session.statement_context(true).range_max_size(), quota);
    }
    assert_eq!(original.range_max_size(), 67_108_864);
}


#[test]
fn selected_record_keys_reach_the_owner_from_query_and_dml_contexts() {
    use tidb_executor::select_lock::SelectLockExec;
    use tidb_sessionctx::select_lock::SelectedLockKeys;
    use tidb_executor::{Executor, ExecutorMeta, TableDualExec};

    let mut session = Session::new();
    assert!(session
        .statement_context(false)
        .selected_lock_keys()
        .is_none());
    session.set_selected_lock_keys(Some(SelectedLockKeys::default()));
    for is_dml in [false, true] {
        let context = session.statement_context(is_dml);
        let selected = context
            .selected_lock_keys()
            .expect("owner installed collector");
        let source = TableDualExec::new(ExecutorMeta::new(Default::default(), 0, 1, 1), 1);
        let mut exec = SelectLockExec::new(
            Box::new(source),
            vec![Box::new(|_| Ok(Some(b"selected-record".to_vec())))],
            selected,
        );
        let mut chunk = exec.new_chunk();
        exec.open().unwrap();
        exec.next(&mut chunk).unwrap();
        assert!(session.take_selected_lock_keys().is_empty());
        exec.next(&mut chunk).unwrap();
        assert_eq!(
            session.take_selected_lock_keys(),
            [b"selected-record".to_vec()]
        );
        exec.close().unwrap();
    }
    session.set_selected_lock_keys(None);
    assert!(session
        .statement_context(false)
        .selected_lock_keys()
        .is_none());
    assert!(session.take_selected_lock_keys().is_empty());
}

#[test]
fn no_decorrelate_in_select_reaches_the_statement_context() {
    let mut session = Session::new();
    assert!(!session
        .statement_context(false)
        .enable_no_decorrelate_in_select());
    session
        .run("set tidb_opt_enable_no_decorrelate_in_select = on")
        .unwrap();
    assert!(session
        .statement_context(false)
        .enable_no_decorrelate_in_select());
}

/// Go `ResetContextOfStmt` sets `sc.Priority` from the statement's own
/// modifier and `sc.NotFillCache` from a SELECT's `SQL_NO_CACHE`; both
/// then ride `SetFromSessionVars` into every coprocessor request.
/// Go `ResetContextOfStmt` ends with `if ForcePriority != NoPriority {
/// sc.Priority = ForcePriority }`: `tidb_force_priority` overrides the
/// statement's own modifier, and `NO_PRIORITY` leaves it.
#[test]
fn force_priority_overrides_the_statement_priority_like_go() {
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            tidb_vardef::FORCE_PRIORITY.store(0, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let _restore = Restore;
    let session = Session::new();
    let stmt = session.parse("select high_priority id from t").expect("parses");
    tidb_vardef::FORCE_PRIORITY.store(
        tidb_mysql::Priority::Low.as_enum(),
        std::sync::atomic::Ordering::SeqCst,
    );
    assert_eq!(
        session.statement_context_for_stmt(&stmt, false).statement_priority(),
        tidb_ast::StatementPriority::Low
    );
    tidb_vardef::FORCE_PRIORITY.store(0, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        session.statement_context_for_stmt(&stmt, false).statement_priority(),
        tidb_ast::StatementPriority::High
    );
}

#[test]
fn statement_priority_and_no_cache_reach_the_statement_context() {
    let session = Session::new();
    let parse = |sql: &str| session.parse(sql).expect("the statement parses");

    let ctx = session.statement_context_for_stmt(
        &parse("select high_priority sql_no_cache id from t"),
        false,
    );
    assert_eq!(ctx.statement_priority(), tidb_ast::StatementPriority::High);
    assert!(ctx.not_fill_cache());

    let ctx =
        session.statement_context_for_stmt(&parse("update low_priority t set id = 1"), true);
    assert_eq!(ctx.statement_priority(), tidb_ast::StatementPriority::Low);
    assert!(
        !ctx.not_fill_cache(),
        "NotFillCache is a SELECT-only modifier"
    );

    // Go's `*ast.LoadDataStmt` arm reads the dedicated `LowPriority` word.
    let ctx = session.statement_context_for_stmt(
        &parse("load data low_priority infile '/a.csv' into table t"),
        true,
    );
    assert_eq!(ctx.statement_priority(), tidb_ast::StatementPriority::Low);

    let ctx = session.statement_context_for_stmt(&parse("select id from t"), false);
    assert_eq!(ctx.statement_priority(), tidb_ast::StatementPriority::None);
    assert!(!ctx.not_fill_cache());
}

#[test]
fn result_materialization_retains_the_statement_context_tracker() {
    let session = Session::new();
    let context = session.statement_context(false);
    let executing_memory = context.statement_memory();
    let (retained_memory, _, _) = session.result_materialization_authority().into_parts();

    assert!(Arc::ptr_eq(
        executing_memory.stmt_tracker(),
        retained_memory.stmt_tracker(),
    ));
}

/// Go `SetFromSessionVars`: `Request.Concurrency` is the session's
/// `tidb_distsql_scan_concurrency`, so a statement context built after
/// `SET` carries the new value.
#[test]
fn distsql_scan_concurrency_reaches_the_statement_context() {
    let mut session = Session::new();
    assert_eq!(
        session.statement_context(false).dist_sql_scan_concurrency(),
        15
    );
    session
        .run("SET tidb_distsql_scan_concurrency = 3")
        .unwrap();
    assert_eq!(
        session.statement_context(false).dist_sql_scan_concurrency(),
        3
    );
}

#[test]
fn read_consistency_session_distinguishes_stale_and_snapshot_modes() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE read_consistency (id INT)")
        .unwrap();
    session
        .run("INSERT INTO read_consistency VALUES (1)")
        .unwrap();
    // A real TSO from the store: the start of an explicit transaction.
    session.run("BEGIN").unwrap();
    let crate::StmtResult::Rows(rows) = session.run("SELECT @@tidb_current_ts").unwrap() else {
        panic!("expected rows");
    };
    session.run("COMMIT").unwrap();
    let ts = String::from_utf8(rows[0][0].sql_bytes().unwrap()).unwrap();
    assert!(
        !session
            .statement_context(false)
            .snapshot_read_options(0.0)
            .is_staleness
    );
    session
        .run(&format!("START TRANSACTION READ ONLY AS OF TIMESTAMP {ts}"))
        .unwrap();
    let retained = session.statement_context(false);
    assert!(retained.snapshot_read_options(0.0).is_staleness);
    // Go's stale provider routes the scope; `tidb_snapshot` reads through
    // the same historical schema without stale routing.
    let ts: u64 = ts.parse().unwrap();
    session.vars.restore_snapshot_ts(ts);
    assert!(
        !session
            .statement_context(false)
            .snapshot_read_options(0.0)
            .is_staleness,
        "tidb_snapshot shares the catalog adapter, not stale routing"
    );
    session.vars.restore_snapshot_ts(0);
    session.run("COMMIT").unwrap();
    assert!(
        !session
            .statement_context(false)
            .snapshot_read_options(0.0)
            .is_staleness
    );
    assert!(retained.snapshot_read_options(0.0).is_staleness);
}

#[test]
fn coprocessor_read_policy_retains_settings_and_hint_deadline() {
    let mut session = Session::new();
    let initial = session.statement_context(false);
    assert_eq!(initial.coprocessor_read_policy(), &Default::default());
    session
        .run("SET tidb_adaptive_closest_read_threshold = 8193")
        .unwrap();
    session
        .run("SET tidb_load_based_replica_read_threshold = '1.234567s'")
        .unwrap();
    session.run("SET tikv_client_read_timeout = 731").unwrap();
    session.run("SET max_execution_time = 919").unwrap();
    let retained = session.statement_context(false);
    assert_eq!(
        retained.coprocessor_read_policy().closest_read_threshold,
        8193
    );
    assert_eq!(
        retained.coprocessor_read_policy().busy_threshold_ns,
        1_234_567_000
    );
    assert_eq!(retained.coprocessor_read_policy().read_timeout_ms, 731);
    assert_eq!(retained.max_execution_time_ms(), 919);
    for class in [
        tidb_executor::StatementClass::Insert,
        tidb_executor::StatementClass::Update,
        tidb_executor::StatementClass::Delete,
        tidb_executor::StatementClass::Other,
        tidb_executor::StatementClass::LoadData,
    ] {
        assert_eq!(
            retained
                .clone()
                .with_statement_class(class)
                .max_execution_time_ms(),
            0
        );
    }
    assert_eq!(session.statement_context(true).max_execution_time_ms(), 0);
    session
        .run("SET tidb_adaptive_closest_read_threshold = 0")
        .unwrap();
    session
        .run("SET tidb_load_based_replica_read_threshold = '999us'")
        .unwrap();
    session.run("SET tikv_client_read_timeout = 0").unwrap();
    let next = session.statement_context(false);
    assert_eq!(next.coprocessor_read_policy().closest_read_threshold, 0);
    assert_eq!(next.coprocessor_read_policy().busy_threshold_ns, 999_000);
    assert_eq!(next.coprocessor_read_policy().read_timeout_ms, 0);
    assert_eq!(
        retained.coprocessor_read_policy().closest_read_threshold,
        8193
    );
    assert_eq!(initial.coprocessor_read_policy(), &Default::default());
    session.stmt_hints.has_max_execution_time = true;
    session.stmt_hints.max_execution_time = 17;
    assert_eq!(session.statement_context(false).max_execution_time_ms(), 17);
    assert_eq!(session.statement_context(true).max_execution_time_ms(), 0);
}

#[test]
fn index_lookup_pushdown_uses_one_typed_statement_snapshot() {
    let mut session = Session::new();
    session
        .run("SET tidb_index_lookup_pushdown_policy = 'force'")
        .unwrap();
    session
        .run("SET transaction_isolation = 'READ-COMMITTED'")
        .unwrap();
    session.run("SET tidb_replica_read = 'follower'").unwrap();
    session.run("SET tidb_max_keys_read = 7").unwrap();

    let context = session.statement_context(false);
    let snapshot = context.index_lookup_push_down_session();
    assert_eq!(
        snapshot.policy,
        tidb_planner::access_path::IndexLookupPushDownPolicy::Force
    );
    assert!(!snapshot.repeatable_read);
    assert!(!snapshot.leader_read);
    assert_eq!(snapshot.max_keys_read, 7);
    assert!(!snapshot.staleness);
    assert!(!snapshot.historical_read);
}

#[test]
fn unsafe_generated_column_substitution_uses_the_session_snapshot() {
    let mut session = Session::new();
    assert!(!session.statement_context(false).enable_unsafe_substitute());

    session
        .run("SET tidb_enable_unsafe_substitute = ON")
        .unwrap();
    assert!(session.statement_context(false).enable_unsafe_substitute());
}

#[test]
fn semi_join_rewrite_uses_the_session_snapshot() {
    let mut session = Session::new();
    assert!(!session.statement_context(false).enable_semi_join_rewrite());

    session
        .run("SET tidb_opt_enable_semi_join_rewrite = ON")
        .unwrap();
    assert!(session.statement_context(false).enable_semi_join_rewrite());
}

#[test]
fn skew_distinct_agg_uses_the_session_snapshot() {
    let mut session = Session::new();
    assert!(!session.statement_context(false).enable_skew_distinct_agg());

    session.run("SET tidb_opt_skew_distinct_agg = ON").unwrap();
    assert!(session.statement_context(false).enable_skew_distinct_agg());
}

#[test]
fn ddl_job_metadata_uses_the_session_snapshot() {
    let mut session = Session::new();
    session.set_connection_id(77);
    session.run("SET tidb_cdc_write_source = 9").unwrap();
    session
        .run("SET tidb_ddl_reorg_priority = 'PRIORITY_HIGH'")
        .unwrap();
    session.run("SET tidb_session_alias = 'ddl-owner'").unwrap();

    let context = session.ddl_statement_context();
    assert_eq!(context.ddl_connection_id(), 77);
    assert_eq!(context.ddl_cdc_write_source(), 9);
    assert_eq!(context.ddl_reorg_priority(), 2);
    assert_eq!(context.ddl_session_alias(), "ddl-owner");
    let original_mode = context.ddl_sql_mode();
    for (mode, expected) in [("", 0), ("ANSI_QUOTES", tidb_mysql::ModeANSIQuotes.0)] {
        session.run(&format!("SET sql_mode = '{mode}'")).unwrap();
        for is_dml in [false, true] {
            assert_eq!(session.statement_context(is_dml).ddl_sql_mode(), expected);
        }
        // Go's captured Job.SQLMode is independent of later SETs.
        assert_eq!(context.ddl_sql_mode(), original_mode);
    }
}
