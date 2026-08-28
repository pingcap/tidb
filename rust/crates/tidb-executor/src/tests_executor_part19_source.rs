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

//! Ports of the deterministic `pkg/executor.part19` slice: Go test items
//! 1081–1140. The runnable tests use the executor's prepared-plan and fast-DML
//! boundaries. Session account state, plan-replayer files, DDL history,
//! transactions, remote requests, and goroutine/failpoint observations stay
//! explicit parity gaps rather than being approximated at this crate boundary.

use std::sync::Arc;

use tidb_datatype::Datum;

use crate::{
    Catalog, DEFAULT_DATABASE, PreparedPlanCacheEnvironment, StmtContext,
    build_prepared_select_plan, run_create_table_on, run_fast_prepared_insert,
    run_fast_prepared_update, run_insert_on, run_prepared_select, run_select_on,
};

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

fn prepared_catalog() -> Catalog {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE prepared_part19 (id INT PRIMARY KEY, v INT NOT NULL)",
        &mut catalog,
    )
    .expect("prepared_part19 creates");
    run_insert_on(
        "INSERT INTO prepared_part19 VALUES (1, 10), (2, 20), (3, 30)",
        &mut catalog,
        &ctx(),
    )
    .expect("prepared_part19 rows insert");
    catalog
}

fn prepared_select_plan(
    plan_sql: &str,
    parameter_count: usize,
    catalog: &Catalog,
) -> Arc<crate::PreparedSelectPlan> {
    let statement = tidb_parser::parse(plan_sql).expect("prepared statement parses");
    Arc::new(
        build_prepared_select_plan(
            &statement,
            parameter_count,
            catalog,
            DEFAULT_DATABASE,
            &ctx(),
        )
        .expect("prepared statement is cacheable"),
    )
}

fn cached_select_rows(
    plan: &Arc<crate::PreparedSelectPlan>,
    values: &[Datum],
    catalog: &mut Catalog,
) -> (bool, Vec<Vec<Datum>>) {
    let execution = plan
        .bind(
            values,
            catalog,
            DEFAULT_DATABASE,
            &ctx(),
            &PreparedPlanCacheEnvironment::default(),
        )
        .expect("prepared values bind");
    let cache_hit = execution.cache_hit();
    let (_, rows) = run_prepared_select(&execution, catalog, DEFAULT_DATABASE, &ctx())
        .expect("prepared statement runs")
        .expect("schema is unchanged");
    (cache_hit, rows)
}

fn parse_update(sql: &str) -> tidb_ast::UpdateStmt {
    let statement = tidb_parser::parse(sql).expect("UPDATE parses");
    let tidb_ast::Stmt::Dml(dml) = &statement else {
        panic!("expected DML statement");
    };
    let tidb_ast::DmlStmt::Update(update) = &**dml else {
        panic!("expected UPDATE statement");
    };
    update.as_ref().clone()
}

/// `pkg/executor/test/passwordtest/password_management_test.go:131::TestPasswordManagement`.
#[test]
#[ignore = "go-parity-gap: password validation, reuse history, expiration, account locking, and failed-login attributes require tidb-session account state"]
fn password_management_validates_history_expiration_and_failed_login_state() {}

/// `password_management_test.go:251::TestFailedLoginTrackingBasic`.
#[test]
#[ignore = "go-parity-gap: authentication failure counters and timed account locks are owned by tidb-session Auth, not tidb-executor"]
fn failed_login_tracking_locks_and_unlocks_accounts() {}

/// `password_management_test.go:355::TestFailedLoginTracking`.
#[test]
#[ignore = "go-parity-gap: failed-login tracking needs the mysql.user attribute store and authentication handshake"]
fn failed_login_tracking_enforces_the_account_lock_window() {}

/// `password_management_test.go:605::TestFailedLoginTrackingAlterUser`.
#[test]
#[ignore = "go-parity-gap: ALTER USER failed-login options and privilege-cache refresh are not executor-local surfaces"]
fn alter_user_updates_failed_login_tracking_options() {}

/// `password_management_test.go:774::TestFailedLoginTrackingCheckPrivilges`.
#[test]
#[ignore = "go-parity-gap: failed-login privilege checks run through tidb-session account authorization"]
fn failed_login_tracking_checks_account_privileges() {}

/// `password_management_test.go:786::TestUserPassword`.
#[test]
#[ignore = "go-parity-gap: CREATE USER, SET PASSWORD, authentication hashes, and password history require the session account subsystem"]
fn user_password_management_matches_mysql_account_behavior() {}

/// `password_management_test.go:829::TestPasswordExpiredAndTacking`.
#[test]
#[ignore = "go-parity-gap: password expiration and sandbox authentication state are session lifecycle behavior"]
fn expired_passwords_enter_and_leave_sandbox_mode() {}

/// `password_management_test.go:862::TestPasswordMySQLCompatibility`.
#[test]
#[ignore = "go-parity-gap: MySQL-compatible password metadata and authentication are not modeled by tidb-executor"]
fn password_management_matches_mysql_compatibility_cases() {}

/// `pkg/executor/test/plancache/main_test.go:27::TestMain`.
#[test]
#[ignore = "skipped-reason: Go TestMain only installs goleak/configuration hooks and has no SQL behavior to port"]
fn plancache_main_is_bootstrap_only() {}

/// `pkg/executor/test/plancache/plan_cache_test.go:36::TestPointGetPreparedPlan`.
#[test]
fn point_get_prepared_plan_rebinds_values_and_reports_hits() {
    let mut catalog = prepared_catalog();
    let plan = prepared_select_plan("SELECT v FROM prepared_part19 WHERE id = ?", 1, &catalog);
    let (first_hit, first_rows) = cached_select_rows(&plan, &[Datum::Int(1)], &mut catalog);
    assert!(!first_hit);
    assert_eq!(first_rows, vec![vec![Datum::Int(10)]]);

    let (second_hit, second_rows) = cached_select_rows(&plan, &[Datum::Int(3)], &mut catalog);
    assert!(second_hit);
    assert_eq!(second_rows, vec![vec![Datum::Int(30)]]);
}

/// `plan_cache_test.go:195::TestPointGetPreparedPlanWithCommitMode`.
#[test]
#[ignore = "go-parity-gap: autocommit/read timestamps, concurrent sessions, and write-conflict commit behavior require a transaction-aware session"]
fn point_get_prepared_plan_uses_the_transaction_read_path() {}

/// `plan_cache_test.go:263::TestPointUpdatePreparedPlan`.
#[test]
fn point_update_prepared_plan_reuses_the_fast_update_shape() {
    let mut catalog = prepared_catalog();
    let update = parse_update("UPDATE prepared_part19 SET v = v + ? WHERE id = ?");
    let changed = run_fast_prepared_update(
        &update,
        &[Datum::Int(5), Datum::Int(2)],
        &mut catalog,
        DEFAULT_DATABASE,
        &ctx(),
    )
    .expect("fast prepared UPDATE runs")
    .expect("point UPDATE shape is supported");
    assert_eq!(changed, 1);
    assert_eq!(
        run_select_on(
            "SELECT v FROM prepared_part19 WHERE id = 2",
            &catalog,
            &ctx(),
        )
        .expect("updated row reads"),
        vec![vec![Datum::Int(25)]]
    );
}

/// `plan_cache_test.go:369::TestPointUpdatePreparedPlanWithCommitMode`.
#[test]
#[ignore = "go-parity-gap: pessimistic transaction snapshots and concurrent write-conflict retries require tidb-session transactions"]
fn point_update_prepared_plan_uses_the_transaction_write_path() {}

/// `plan_cache_test.go:452::TestPreparedPlanCachePlanSelectionRegressions`.
#[test]
fn prepared_plan_cache_selection_rebinds_a_range_without_replanning_the_shape() {
    let mut catalog = prepared_catalog();
    let plan = prepared_select_plan(
        "SELECT v FROM prepared_part19 WHERE id BETWEEN ? AND ? ORDER BY id",
        2,
        &catalog,
    );
    let (first_hit, first_rows) =
        cached_select_rows(&plan, &[Datum::Int(1), Datum::Int(2)], &mut catalog);
    assert!(!first_hit);
    assert_eq!(first_rows, vec![vec![Datum::Int(10)], vec![Datum::Int(20)]]);

    let (second_hit, second_rows) =
        cached_select_rows(&plan, &[Datum::Int(2), Datum::Int(3)], &mut catalog);
    assert!(second_hit);
    assert_eq!(
        second_rows,
        vec![vec![Datum::Int(20)], vec![Datum::Int(30)]]
    );
}

/// `plan_cache_test.go:463::TestPreparedPlanCacheSessionInteractions`.
#[test]
#[ignore = "go-parity-gap: prepared-plan cache interactions with system variables, bindings, and foreign-key metadata require tidb-session"]
fn prepared_plan_cache_tracks_session_interactions() {}

/// `plan_cache_test.go:475::TestPreparedPlanCacheClusterIndex`.
#[test]
#[ignore = "go-parity-gap: clustered-index session mode, @@last_plan_from_cache, and EXPLAIN FOR CONNECTION are not executor-local"]
fn prepared_plan_cache_handles_clustered_index_sessions() {}

/// `plan_cache_test.go:574::TestPreparedPlanCacheOperators`.
#[test]
fn prepared_plan_cache_reuses_a_parameterized_operator_tree() {
    let mut catalog = prepared_catalog();
    let plan = prepared_select_plan(
        "SELECT v FROM prepared_part19 WHERE id > ? ORDER BY id",
        1,
        &catalog,
    );
    let (first_hit, first_rows) = cached_select_rows(&plan, &[Datum::Int(1)], &mut catalog);
    assert!(!first_hit);
    assert_eq!(first_rows, vec![vec![Datum::Int(20)], vec![Datum::Int(30)]]);

    let (second_hit, second_rows) = cached_select_rows(&plan, &[Datum::Int(2)], &mut catalog);
    assert!(second_hit);
    assert_eq!(second_rows, vec![vec![Datum::Int(30)]]);
}

/// `pkg/executor/test/planreplayer/main_test.go:27::TestMain`.
#[test]
#[ignore = "skipped-reason: Go TestMain only installs goleak/configuration hooks and has no behavior to port"]
fn planreplayer_main_is_bootstrap_only() {}

/// `plan_replayer_test.go:110::TestPlanReplayer`.
#[test]
#[ignore = "go-parity-gap: PLAN REPLAYER DUMP, schema/statistics capture, and zip-file generation require tidb-session/domain and external storage"]
fn plan_replayer_dump_explain_captures_schema_and_statistics() {}

/// `plan_replayer_test.go:152::TestPlanReplayerLoadTiFlashPlanWithHypoReplica`.
#[test]
#[ignore = "go-parity-gap: plan-replayer LOAD and hypothetical TiFlash replica metadata are session/domain surfaces"]
fn plan_replayer_load_restores_a_tiflash_plan() {}

/// `plan_replayer_test.go:202::TestPlanReplayerCaptureSEM`.
#[test]
#[ignore = "go-parity-gap: SEM configuration, capture status tables, and plan-replayer domain workers are unported here"]
fn plan_replayer_capture_supports_sem() {}

/// `plan_replayer_test.go:226::TestPlanReplayerCapture`.
#[test]
#[ignore = "go-parity-gap: capture tasks, SQL digests, historical statistics, and domain workers require tidb-session/domain"]
fn plan_replayer_capture_collects_and_drains_tasks() {}

/// `plan_replayer_test.go:279::TestPlanReplayerContinuesCapture`.
#[test]
#[ignore = "go-parity-gap: continuous capture and historical-statistics session variables are not executor-local"]
fn plan_replayer_continues_capture_after_a_query() {}

/// `plan_replayer_test.go:315::TestPlanReplayerDumpSingle`.
#[test]
#[ignore = "go-parity-gap: single-statement plan-replayer zip layout is generated by the session/domain storage path"]
fn plan_replayer_dump_single_has_the_expected_zip_files() {}

/// `plan_replayer_test.go:355::TestExplainExploreReplayer`.
#[test]
#[ignore = "go-parity-gap: EXPLAIN EXPLORE REPLAYER loads a domain-generated archive and is not an executor API"]
fn explain_explore_replayer_reads_a_dumped_plan() {}

/// `plan_replayer_test.go:389::TestPlanReplayerDumpPresignedURLOutput`.
#[test]
#[ignore = "go-parity-gap: presigned external-storage URLs are produced by the session/domain plan-replayer executor"]
fn plan_replayer_dump_reports_a_presigned_url() {}

/// `plan_replayer_test.go:418::TestPlanReplayerDumpMultipleError`.
#[test]
#[ignore = "go-parity-gap: PLAN REPLAYER DUMP statement-list validation belongs to tidb-session's statement dispatcher"]
fn plan_replayer_dump_multiple_rejects_invalid_statements() {}

/// `plan_replayer_test.go:434::TestPlanReplayerDumpMultiple`.
#[test]
#[ignore = "go-parity-gap: multi-statement plan-replayer archive assembly and cross-database catalog capture require tidb-session/domain"]
fn plan_replayer_dump_multiple_writes_each_statement() {}

/// `pkg/executor/test/recovertest/main_test.go:27::TestMain`.
#[test]
#[ignore = "skipped-reason: Go TestMain only installs goleak/configuration hooks and has no behavior to port"]
fn recovertest_main_is_bootstrap_only() {}

/// `recover_test.go:46::TestRecoverTable`.
#[test]
#[ignore = "go-parity-gap: RECOVER TABLE requires DDL history, GC safe-point metadata, and asynchronous schema jobs"]
fn recover_table_restores_dropped_table_data_and_auto_id() {}

/// `recover_test.go:156::TestFlashbackTable`.
#[test]
#[ignore = "go-parity-gap: FLASHBACK TABLE requires DDL history, MVCC timestamps, GC safe points, and DDL jobs"]
fn flashback_table_restores_dropped_and_truncated_tables() {}

/// `recover_test.go:273::TestRecoverTempTable`.
#[test]
#[ignore = "go-parity-gap: temporary/global-temporary table DDL history is owned by tidb-session and the DDL subsystem"]
fn recover_temp_table_rejects_temporary_tables() {}

/// `recover_test.go:299::TestRecoverTableMeetError`.
#[test]
#[ignore = "go-parity-gap: injected DDL job failures and schema-version cleanup require the unported DDL worker"]
fn recover_table_propagates_ddl_job_errors() {}

/// `recover_test.go:328::TestRecoverTablePrivilege`.
#[test]
#[ignore = "go-parity-gap: RECOVER/FLASHBACK privilege checks run in tidb-session account authorization"]
fn recover_table_checks_drop_create_and_select_privileges() {}

/// `recover_test.go:365::TestRecoverClusterMeetError`.
#[test]
#[ignore = "go-parity-gap: cluster flashback, TiKV capability checks, GC safe points, and system-table guards are not executor-local"]
fn flashback_cluster_reports_safety_and_privilege_errors() {}

/// `recover_test.go:418::TestFlashbackWithSafeTs`.
#[test]
#[ignore = "go-parity-gap: flashback safe-timestamp comparison and retry injection require the TiKV/DDL integration surface"]
fn flashback_cluster_honors_the_resolved_safe_timestamp() {}

/// `recover_test.go:479::TestFlashbackTSOWithSafeTs`.
#[test]
#[ignore = "go-parity-gap: TSO flashback and safe-timestamp validation require the unported DDL/TiKV integration"]
fn flashback_cluster_tso_honors_the_resolved_safe_timestamp() {}

/// `recover_test.go:540::TestFlashbackRetryGetMinSafeTime`.
#[test]
#[ignore = "go-parity-gap: asynchronous safe-time retry and failpoint timing require the DDL worker"]
fn flashback_cluster_retries_min_safe_time() {}

/// `recover_test.go:575::TestFlashbackSchema`.
#[test]
#[ignore = "go-parity-gap: schema flashback is a DDL-history and privilege operation outside the executor driver"]
fn flashback_schema_restores_all_tables_and_privileges() {}

/// `recover_test.go:654::TestFlashbackSchemaWithManyTables`.
#[test]
#[ignore = "go-parity-gap: large schema flashback transaction sizing and asynchronous DDL are unported"]
fn flashback_schema_handles_many_tables() {}

/// `recover_test.go:724::TestFlashbackClusterWithManyDBs`.
#[test]
#[ignore = "go-parity-gap: large cluster flashback requires DDL history batching, TiKV timestamps, and concurrent sessions"]
fn flashback_cluster_handles_many_databases() {}

/// `pkg/executor/test/seqtest/main_test.go:25::TestMain`.
#[test]
#[ignore = "skipped-reason: Go TestMain only installs sequential-suite configuration and leak hooks"]
fn seqtest_main_is_bootstrap_only() {}

/// `pkg/executor/test/seqtest/prepared_test.go:39::TestPrepared`.
#[test]
fn prepared_statement_select_reuses_the_executor_plan() {
    let mut catalog = prepared_catalog();
    let plan = prepared_select_plan(
        "SELECT id, v FROM prepared_part19 WHERE id = ?",
        1,
        &catalog,
    );
    let (first_hit, rows) = cached_select_rows(&plan, &[Datum::Int(1)], &mut catalog);
    assert!(!first_hit);
    assert_eq!(rows, vec![vec![Datum::Int(1), Datum::Int(10)]]);
    let (second_hit, rows) = cached_select_rows(&plan, &[Datum::Int(2)], &mut catalog);
    assert!(second_hit);
    assert_eq!(rows, vec![vec![Datum::Int(2), Datum::Int(20)]]);
}

/// `prepared_test.go:268::TestPreparedLimitOffset`.
#[test]
fn prepared_limit_offset_binds_integer_parameters() {
    let mut catalog = prepared_catalog();
    let plan = prepared_select_plan(
        "SELECT id FROM prepared_part19 ORDER BY id LIMIT ? OFFSET ?",
        2,
        &catalog,
    );
    let (hit, rows) = cached_select_rows(&plan, &[Datum::Int(1), Datum::Int(1)], &mut catalog);
    assert!(!hit);
    assert_eq!(rows, vec![vec![Datum::Int(2)]]);
}

/// `prepared_test.go:300::TestPrepareWithAggregation`.
#[test]
fn prepared_aggregation_rebinds_its_filter_parameter() {
    let mut catalog = prepared_catalog();
    let plan = prepared_select_plan(
        "SELECT SUM(v) FROM prepared_part19 WHERE id > ?",
        1,
        &catalog,
    );
    let (first_hit, rows) = cached_select_rows(&plan, &[Datum::Int(1)], &mut catalog);
    assert!(!first_hit);
    assert_eq!(
        rows,
        vec![vec![Datum::Decimal(tidb_datatype::Decimal::from_int(50))]]
    );
    let (second_hit, rows) = cached_select_rows(&plan, &[Datum::Int(2)], &mut catalog);
    assert!(second_hit);
    assert_eq!(
        rows,
        vec![vec![Datum::Decimal(tidb_datatype::Decimal::from_int(30))]]
    );
}

/// `prepared_test.go:328::TestPreparedInsert`.
#[test]
fn prepared_insert_writes_bound_values() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE prepared_insert_part19 (id VARCHAR(16) PRIMARY KEY, v INT)",
        &mut catalog,
    )
    .expect("prepared insert table creates");
    let statement = tidb_parser::parse("INSERT INTO prepared_insert_part19 (id, v) VALUES (?, ?)")
        .expect("prepared INSERT parses");
    let tidb_ast::Stmt::Dml(dml) = &statement else {
        panic!("expected INSERT DML");
    };
    let tidb_ast::DmlStmt::Insert(insert) = &**dml else {
        panic!("expected INSERT statement");
    };
    let result = run_fast_prepared_insert(
        insert,
        &[Datum::Bytes(b"k1".to_vec()), Datum::Int(42)],
        &mut catalog,
        DEFAULT_DATABASE,
        &ctx(),
    )
    .expect("prepared INSERT runs")
    .expect("INSERT shape is supported");
    assert_eq!(result.0, 1);
    assert_eq!(
        run_select_on(
            "SELECT v FROM prepared_insert_part19 WHERE id = 'k1'",
            &catalog,
            &ctx(),
        )
        .expect("inserted row reads"),
        vec![vec![Datum::Int(42)]]
    );
}

/// `prepared_test.go:405::TestPreparedUpdate`.
#[test]
fn prepared_update_rebinds_assignment_and_handle_parameters() {
    let mut catalog = prepared_catalog();
    let update = parse_update("UPDATE prepared_part19 SET v = v + ? WHERE id = ?");
    let changed = run_fast_prepared_update(
        &update,
        &[Datum::Int(7), Datum::Int(3)],
        &mut catalog,
        DEFAULT_DATABASE,
        &ctx(),
    )
    .expect("prepared UPDATE runs")
    .expect("UPDATE shape is supported");
    assert_eq!(changed, 1);
    assert_eq!(
        run_select_on(
            "SELECT v FROM prepared_part19 WHERE id = 3",
            &catalog,
            &ctx(),
        )
        .expect("updated row reads"),
        vec![vec![Datum::Int(37)]]
    );
}

/// `prepared_test.go:457::TestIssue21884`.
#[test]
#[ignore = "go-parity-gap: NOW() freshness across repeated session PREPARE/EXECUTE calls requires session clock and result-set lifecycle"]
fn prepared_update_recomputes_statement_time_values() {}

/// `prepared_test.go:478::TestPreparedDelete`.
#[test]
#[ignore = "go-parity-gap: the executor driver has no fast prepared DELETE carrier; full behavior requires session prepared-statement dispatch"]
fn prepared_delete_rebinds_the_handle_parameter() {}

/// `prepared_test.go:530::TestPrepareDealloc`.
#[test]
#[ignore = "go-parity-gap: PREPARE/DEALLOCATE registries and session plan-cache ownership are tidb-session surfaces"]
fn prepared_deallocate_releases_session_plan_cache_entries() {}

/// `prepared_test.go:579::TestPreparedIssue8153`.
#[test]
#[ignore = "go-parity-gap: parameterized ORDER BY/GROUP BY positional-reference rules are resolved by session PREPARE semantics"]
fn prepared_order_and_group_parameters_follow_mysql_rules() {}

/// `prepared_test.go:639::TestPreparedIssue17419`.
#[test]
#[ignore = "go-parity-gap: process information, expensive-query handling, and prepared execution sessions require tidb-session/server"]
fn prepared_execution_updates_process_information() {}

/// `pkg/executor/test/seqtest/seq_executor_test.go:63::TestEarlyClose`.
#[test]
#[ignore = "go-parity-gap: distributed record-set cancellation and coprocessor goroutine cleanup require TiKV request lifecycle hooks"]
fn early_close_cancels_distributed_reads_without_leaks() {}

/// `seq_executor_test.go:134::TestShow`.
#[test]
#[ignore = "go-parity-gap: SHOW and SHOW CREATE statement dispatch plus session statistics are outside tidb-executor"]
fn show_statements_render_metadata_and_variables() {}

/// `seq_executor_test.go:612::TestShowStatsHealthy`.
#[test]
#[ignore = "go-parity-gap: statistics delta flushing, analyze, and SHOW STATS_HEALTHY require tidb-session/domain statistics handles"]
fn show_stats_healthy_tracks_analyze_and_delta_updates() {}

/// `seq_executor_test.go:643::TestIndexDoubleReadClose`.
#[test]
#[ignore = "go-parity-gap: multi-region TiKV splitting and index-lookup worker shutdown require the remote storage client"]
fn index_double_read_closes_workers_early() {}

/// `seq_executor_test.go:680::TestIndexMergeReaderClose`.
#[test]
#[ignore = "go-parity-gap: index-merge worker failpoints and goroutine inspection require the distributed executor"]
fn index_merge_reader_closes_workers_after_start_failure() {}

/// `seq_executor_test.go:704::TestParallelHashAggClose`.
#[test]
#[ignore = "go-parity-gap: parallel hash-aggregation failpoint and worker cleanup are not exposed by the local driver carrier"]
fn parallel_hash_aggregation_closes_after_worker_error() {}

/// `seq_executor_test.go:731::TestUnparallelHashAggClose`.
#[test]
#[ignore = "go-parity-gap: unparallel hash-aggregation error injection and goroutine cleanup require executor failpoints"]
fn unparallel_hash_aggregation_closes_after_worker_error() {}

/// `seq_executor_test.go:766::TestAdminShowNextID`.
#[test]
#[ignore = "go-parity-gap: ADMIN SHOW NEXT_ROW_ID reads session infoschema and auto-ID allocator metadata, including sequence state"]
fn admin_show_next_id_reports_allocator_state() {}
