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

//! Regressions exercise completed SQL, never injected statement records.
use crate::*;
use tidb_util::topsql_stmtstats::{global_aggregator, Collector, StatementStatsMap};

#[test]
fn observation_batch_completed_sql_reaches_summary_reader() {
    let mut session = Session::new();
    session.set_user("root@%".into(), "root@localhost".into());
    session.run("SELECT 918273 + 4").unwrap();
    let (_, rows) = tests_support::query_text(
        &mut session,
        "SELECT DIGEST_TEXT, EXEC_COUNT FROM information_schema.statements_summary",
    );
    assert!(
        rows.iter().any(|row| row[0].contains("select ? + ?")),
        "completed SQL missing from real summary reader: {rows:?}"
    );
}

#[derive(Default)]
struct Capture(std::sync::Mutex<StatementStatsMap>);
impl Collector for Capture {
    fn collect_stmt_stats_map(&self, data: &StatementStatsMap) {
        self.0.lock().unwrap().merge(data);
    }
}

#[test]
fn observation_batch_completed_sql_reaches_topsql_aggregator() {
    use tidb_util::topsql_state::{disable_top_sql, enable_top_sql};
    enable_top_sql();
    let capture = Arc::new(Capture::default());
    let collector: Arc<dyn Collector> = capture.clone();
    let aggregator = global_aggregator();
    aggregator.register_collector(collector.clone());
    let mut session = Session::new();
    session.set_user("root@%".into(), "root@localhost".into());
    session.run("SELECT 765432 + 9").unwrap();
    let stats = Arc::clone(&session.statement_stats);
    assert!(!stats.finished());
    drop(session);
    assert!(stats.finished());
    aggregator.drain_and_push_stmt_stats();
    assert!(!aggregator.contains_stats(&stats));
    aggregator.unregister_collector(&collector);
    disable_top_sql();
    let (_, digest) = normalize_statement_digest("SELECT 765432 + 9");
    let data = capture.0.lock().unwrap();
    assert!(
        data.iter()
            .any(|(key, item)| key.sql_digest.as_bytes() == digest.as_bytes()
                && item.exec_count > 0
                && item.sum_duration_ns > 0),
        "real SQL never reached execution counters: {data:?}"
    );
}

fn summary_execution_count(sql: &str) -> u64 {
    let (_, digest) = normalize_statement_digest(sql);
    tidb_stmtsummary::statement_summary::STMT_SUMMARY_BY_DIGEST_MAP
        .summary_map_values()
        .iter()
        .map(|record| {
            let record = record.lock().unwrap();
            if record.digest == digest.as_str() {
                record.cumulative.exec_count as u64
            } else {
                0
            }
        })
        .sum()
}

#[test]
fn observation_batch_streaming_result_publishes_once_at_close() {
    let mut session = Session::new();
    session.set_user("root@%".into(), "root@localhost".into());
    let sql = "SELECT 123 AS observation_close";
    let before = summary_execution_count(sql);
    let stmt = session.parse_statement(sql).unwrap();
    let StatementExecution::Rows(mut result) =
        session.execute_record_set_parsed(stmt, sql).unwrap()
    else {
        panic!("query result expected");
    };
    assert_eq!(summary_execution_count(sql), before);
    let mut chunk = result.new_chunk();
    result.next(&mut chunk).unwrap();
    result.next(&mut chunk).unwrap();
    result.finish().unwrap();
    assert_eq!(
        summary_execution_count(sql),
        before,
        "executor finish precedes record-set close"
    );
    result.close().unwrap();
    result.close().unwrap();
    drop(result);
    assert_eq!(summary_execution_count(sql), before + 1);
}

#[test]
fn observation_batch_internal_sql_is_not_reported_as_a_user() {
    let mut session = Session::new();
    let sql = "SELECT 456 AS observation_internal";
    let before = summary_execution_count(sql);
    session.run(sql).unwrap();
    assert_eq!(summary_execution_count(sql), before);
}

#[test]
fn observation_batch_persistent_sql_reads_memory_and_disk_and_flushes() {
    use tidb_stmtsummary::v2::stmtsummary as persistent;
    let previous = tidb_config::config_tree::config::get_global_config();
    let previous_summary = persistent::global_stmt_summary();
    let path = std::env::temp_dir().join(format!("tidb-observation-{}.log", std::process::id()));
    let _ = std::fs::remove_file(&path);
    tidb_config::config_tree::config::update_global(|config| {
        config.instance.stmt_summary_enable_persistent = true;
        config.instance.stmt_summary_filename = path.display().to_string();
    });
    persistent::setup(&persistent::Config {
        filename: path.display().to_string(),
        ..persistent::Config::default()
    })
    .unwrap();
    let mut session = Session::new();
    session.set_user("root@%".into(), "root@localhost".into());
    let sql = "SELECT 789 AS observation_persistent";
    let (_, digest) = normalize_statement_digest(sql);
    session.run(sql).unwrap();
    let (_, current) = tests_support::query_text(
        &mut session,
        &format!(
            "SELECT DIGEST FROM information_schema.STATEMENTS_SUMMARY WHERE DIGEST='{digest}'"
        ),
    );
    persistent::global_stmt_summary()
        .unwrap()
        .rotate(chrono::Utc::now());
    session.run(sql).unwrap();
    let (_, history) = tests_support::query_text(&mut session, &format!("SELECT DIGEST FROM information_schema.STATEMENTS_SUMMARY_HISTORY WHERE DIGEST='{digest}'"));
    let cumulative_error = session
        .run("SELECT * FROM information_schema.TIDB_STATEMENTS_STATS")
        .unwrap_err();
    persistent::close();
    let disk = std::fs::read_to_string(&path).unwrap();
    tidb_config::config_tree::config::store_global_config((*previous).clone());
    persistent::set_global_stmt_summary(previous_summary);
    let _ = std::fs::remove_file(path);
    assert_eq!(current.len(), 1);
    assert_eq!(
        history.len(),
        2,
        "history must combine rotated disk and active memory"
    );
    assert!(
        disk.contains(digest.as_str()),
        "shutdown must flush real SQL"
    );
    assert_eq!(cumulative_error.clone().to_mysql_error().code, 1235);
    assert!(cumulative_error
        .to_string()
        .contains("cumulative statement summary"));
}

#[test]
fn observation_batch_disabled_summary_clears_commit_predecessor() {
    use tidb_stmtsummary::v2::stmtsummary::{enabled, set_enabled};
    let mut session = Session::new();
    session.set_user("root@%".into(), "root@localhost".into());
    session.run("SELECT 1 AS observation_enabled").unwrap();
    assert!(session.previous_summary_statement.is_some());
    let sql = "SELECT 2 AS observation_disabled";
    let before = summary_execution_count(sql);
    let previous = enabled();
    set_enabled(false);
    let result = session.run(sql);
    let cleared = session.previous_summary_statement.is_none();
    set_enabled(previous);
    result.unwrap();
    assert!(cleared);
    assert_eq!(summary_execution_count(sql), before);
}

#[test]
fn observation_batch_reader_filters_statements_by_authenticated_user() {
    let mut root = Session::new();
    root.set_user("root@%".into(), "root@localhost".into());
    let sql = "SELECT 314159 AS observation_private";
    root.run(sql).unwrap();
    let (_, digest) = normalize_statement_digest(sql);
    let mut alice = Session::new();
    alice.set_user(
        "observation_alice@%".into(),
        "observation_alice@localhost".into(),
    );
    let (_, rows) = tests_support::query_text(
        &mut alice,
        &format!(
            "SELECT DIGEST FROM information_schema.STATEMENTS_SUMMARY WHERE DIGEST='{digest}'"
        ),
    );
    assert!(
        rows.is_empty(),
        "user without PROCESS must not see root's statements"
    );
}

#[test]
fn observation_batch_prepared_execution_records_original_sql() {
    let mut session = Session::new();
    session.set_user("root@%".into(), "root@localhost".into());
    let sql = "SELECT ? AS observation_prepared";
    let prepared = session.prepare_ast(sql).unwrap();
    let before = summary_execution_count(sql);
    session
        .run_prepared_with_result_authority(&prepared, &[Datum::Int(42)])
        .unwrap();
    assert_eq!(summary_execution_count(sql), before + 1);
}

#[test]
fn observation_batch_routed_completion_waits_for_durable_outcome() {
    let mut session = Session::new();
    session.set_user("root@%".into(), "root@localhost".into());
    let sql = "SET @observation_durable = 19";
    let stmt = session.parse_statement(sql).unwrap();
    let before = summary_execution_count(sql);
    session.begin_routed_statement_observation(sql, &stmt);
    session.run(sql).unwrap();
    assert_eq!(
        summary_execution_count(sql),
        before,
        "scratch SQL success is not a durable completion"
    );
    session.finish_routed_statement_observation(false, 0);
    session.finish_routed_statement_observation(false, 0);
    assert_eq!(summary_execution_count(sql), before + 1);
    let (_, digest) = normalize_statement_digest(sql);
    let (_, rows) = tests_support::query_text(
        &mut session,
        &format!(
            "SELECT SUM_ERRORS FROM information_schema.STATEMENTS_SUMMARY WHERE DIGEST='{digest}'"
        ),
    );
    assert_eq!(rows, [["1"]]);
}

#[test]
fn observation_batch_sql_execute_uses_retained_statement_digest() {
    let mut session = Session::new();
    session.set_user("root@%".into(), "root@localhost".into());
    let sql = "SELECT ? AS observation_named_execute";
    session
        .run("PREPARE observation_named FROM 'SELECT ? AS observation_named_execute'")
        .unwrap();
    session.run("SET @observation_named_arg=42").unwrap();
    let before = summary_execution_count(sql);
    session
        .run("EXECUTE observation_named USING @observation_named_arg")
        .unwrap();
    session
        .run("EXECUTE observation_named USING @observation_named_arg")
        .unwrap();
    assert_eq!(summary_execution_count(sql), before + 2);
}

#[test]
fn observation_batch_persistent_instance_getters_read_live_config() {
    use tidb_config::config_tree::config::{get_global_config, store_global_config, update_global};
    let previous = get_global_config();
    update_global(|config| {
        config.instance.stmt_summary_enable_persistent = true;
        config.instance.stmt_summary_filename = "observation-instance.log".into();
        config.instance.stmt_summary_file_max_days = 7;
        config.instance.stmt_summary_file_max_size = 8;
        config.instance.stmt_summary_file_max_backups = 9;
    });
    let session = Session::new();
    let values: Vec<_> = [
        "tidb_stmt_summary_enable_persistent",
        "tidb_stmt_summary_filename",
        "tidb_stmt_summary_file_max_days",
        "tidb_stmt_summary_file_max_size",
        "tidb_stmt_summary_file_max_backups",
    ]
    .into_iter()
    .map(|name| session.vars.get_global(name))
    .collect();
    update_global(|config| config.instance.stmt_summary_enable_persistent = false);
    let fallback = session
        .vars
        .get_global("tidb_stmt_summary_enable_persistent");
    store_global_config((*previous).clone());
    assert_eq!(
        values.into_iter().map(Result::unwrap).collect::<Vec<_>>(),
        ["ON", "observation-instance.log", "7", "8", "9"]
    );
    assert_eq!(fallback.unwrap(), "OFF");
}

#[test]
fn observation_batch_dml_scratch_success_waits_for_durable_failure() {
    let mut session = Session::new();
    session.set_user("root@%".into(), "root@localhost".into());
    session
        .run("CREATE TABLE observation_durable_dml (id BIGINT PRIMARY KEY)")
        .unwrap();
    let sql = "INSERT INTO observation_durable_dml VALUES (11)";
    let stmt = session.parse_statement(sql).unwrap();
    let before = summary_execution_count(sql);
    session.begin_routed_statement_observation(sql, &stmt);
    session.run_parsed(stmt.clone(), sql).unwrap();
    assert_eq!(summary_execution_count(sql), before);
    session.finish_routed_statement_observation(false, 0);
    assert_eq!(summary_execution_count(sql), before + 1);
    let (_, digest) = normalize_statement_digest(sql);
    let (_, rows) = tests_support::query_text(&mut session, &format!("SELECT SUM_ERRORS,AVG_AFFECTED_ROWS FROM information_schema.STATEMENTS_SUMMARY WHERE DIGEST='{digest}'"));
    assert_eq!(rows, [["1", "0"]]);
}

#[test]
fn observation_batch_durable_named_execute_keeps_retained_sql() {
    let mut session = Session::new();
    session.set_user("root@%".into(), "root@localhost".into());
    session
        .run("CREATE TABLE observation_named_dml (id BIGINT PRIMARY KEY)")
        .unwrap();
    let sql = "INSERT INTO observation_named_dml VALUES (?)";
    session
        .run("PREPARE observation_write FROM 'INSERT INTO observation_named_dml VALUES (?)'")
        .unwrap();
    session.run("SET @observation_write_arg=19").unwrap();
    let execute_sql = "EXECUTE observation_write USING @observation_write_arg";
    let stmt = session.parse_statement(execute_sql).unwrap();
    let before = summary_execution_count(sql);
    session.begin_routed_statement_observation(execute_sql, &stmt);
    session.run(execute_sql).unwrap();
    assert_eq!(summary_execution_count(sql), before);
    session.finish_routed_statement_observation(true, 1);
    assert_eq!(summary_execution_count(sql), before + 1);
    let (_, digest) = normalize_statement_digest(sql);
    let (_, rows) = tests_support::query_text(&mut session, &format!("SELECT PREPARED,AVG_AFFECTED_ROWS FROM information_schema.STATEMENTS_SUMMARY WHERE DIGEST='{digest}'"));
    assert_eq!(rows, [["1", "1"]]);
}

#[test]
fn observation_batch_failed_physical_planning_is_not_an_execution() {
    use tidb_util::topsql_state::{disable_top_sql, enable_top_sql};
    enable_top_sql();
    let mut session = Session::new();
    let sql = "SELECT * FROM observation_missing_compile_table";
    let (_, digest) = normalize_statement_digest(sql);
    assert!(session.run(sql).is_err());
    let data = session.statement_stats.take();
    disable_top_sql();
    assert!(
        !data
            .iter()
            .any(|(key, item)| key.sql_digest.as_bytes() == digest.as_bytes()
                && item.exec_count != 0),
        "failed compilation counted as execution: {data:?}"
    );
}

#[test]
fn observation_batch_tables_and_phase_times_follow_real_statement() {
    let mut session = Session::new();
    session.set_user("root@%".into(), "root@localhost".into());
    session
        .run("CREATE TABLE observation_attribution_table (id INT)")
        .unwrap();
    let sql = "SELECT a.id FROM observation_attribution_table a JOIN observation_attribution_table b ON a.id=b.id";
    session.run(sql).unwrap();
    let (_, digest) = normalize_statement_digest(sql);
    let records =
        tidb_stmtsummary::statement_summary::STMT_SUMMARY_BY_DIGEST_MAP.summary_map_values();
    let record = records
        .iter()
        .find(|record| record.lock().unwrap().digest == digest.as_str())
        .unwrap()
        .lock()
        .unwrap();
    let (tables, parse, compile) = (
        record.table_names.clone(),
        record.cumulative.sum_parse_latency,
        record.cumulative.sum_compile_latency,
    );
    drop(record);
    assert_eq!(tables, "test.observation_attribution_table");
    assert!(parse > std::time::Duration::ZERO);
    assert!(compile > std::time::Duration::ZERO);
}

#[test]
fn observation_batch_real_query_records_parse_and_compile_phases() {
    let mut session = Session::new();
    session.set_user("root@%".into(), "root@localhost".into());
    let sql = "SELECT 901 AS observation_phase_clock";
    session.run(sql).unwrap();
    let (_, digest) = normalize_statement_digest(sql);
    let records =
        tidb_stmtsummary::statement_summary::STMT_SUMMARY_BY_DIGEST_MAP.summary_map_values();
    let record = records
        .iter()
        .find(|record| record.lock().unwrap().digest == digest.as_str())
        .unwrap()
        .lock()
        .unwrap();
    let (parse, compile) = (
        record.cumulative.sum_parse_latency,
        record.cumulative.sum_compile_latency,
    );
    drop(record);
    assert!(parse > std::time::Duration::ZERO);
    assert!(compile > std::time::Duration::ZERO);
}

#[test]
fn observation_batch_prepared_cache_keeps_tables_and_one_execution_per_call() {
    use tidb_util::topsql_state::{disable_top_sql, enable_top_sql};
    enable_top_sql();
    let mut session = Session::new();
    session.set_user("root@%".into(), "root@localhost".into());
    session
        .run("CREATE TABLE observation_prepared_table (id INT PRIMARY KEY)")
        .unwrap();
    session
        .run("INSERT INTO observation_prepared_table VALUES (1)")
        .unwrap();
    let sql = "SELECT id FROM observation_prepared_table WHERE id=?";
    let prepared = session.prepare_ast(sql).unwrap();
    session.statement_stats.take();
    for _ in 0..2 {
        session
            .run_prepared_with_result_authority(&prepared, &[Datum::Int(1)])
            .unwrap();
    }
    let cached = session.found_in_plan_cache;
    let data = session.statement_stats.take();
    disable_top_sql();
    let (_, digest) = normalize_statement_digest(sql);
    let item = data
        .iter()
        .find(|(key, _)| key.sql_digest.as_bytes() == digest.as_bytes())
        .unwrap()
        .1;
    assert!(cached, "second execution must exercise retained plan path");
    assert_eq!(item.exec_count, 2);
    assert_eq!(item.duration_count, 2);
    let records =
        tidb_stmtsummary::statement_summary::STMT_SUMMARY_BY_DIGEST_MAP.summary_map_values();
    let record = records
        .iter()
        .find(|record| record.lock().unwrap().digest == digest.as_str())
        .unwrap()
        .lock()
        .unwrap();
    let (tables, compile) = (
        record.table_names.clone(),
        record.cumulative.sum_compile_latency,
    );
    drop(record);
    assert_eq!(tables, "test.observation_prepared_table");
    assert!(compile > std::time::Duration::ZERO);
}

#[test]
fn observation_batch_frontend_parse_is_transferred_and_not_reused() {
    let mut session = Session::new();
    session.set_user("root@%".into(), "root@localhost".into());
    let sql = "SELECT 902 AS observation_frontend_parse";
    let stmt = session.parse_at_statement_boundary(sql).unwrap();
    let opened = session.open_record_set_parsed(stmt, sql).unwrap();
    let crate::StatementExecution::Rows(mut result) = opened.attach(&mut session) else {
        panic!("rows expected");
    };
    result.close().unwrap();
    drop(result);
    let (_, digest) = normalize_statement_digest(sql);
    let records =
        tidb_stmtsummary::statement_summary::STMT_SUMMARY_BY_DIGEST_MAP.summary_map_values();
    let record = records
        .iter()
        .find(|record| record.lock().unwrap().digest == digest.as_str())
        .unwrap()
        .lock()
        .unwrap();
    let parse = record.cumulative.sum_parse_latency;
    drop(record);
    assert!(parse > std::time::Duration::ZERO);
    assert!(session.pending_observation_parse.is_none());
    let prepared = session
        .prepare_ast("SELECT ? AS observation_no_parse")
        .unwrap();
    session
        .run_prepared_with_result_authority(&prepared, &[Datum::Int(1)])
        .unwrap();
    let (_, digest) = normalize_statement_digest("SELECT ? AS observation_no_parse");
    let records =
        tidb_stmtsummary::statement_summary::STMT_SUMMARY_BY_DIGEST_MAP.summary_map_values();
    let record = records
        .iter()
        .find(|record| record.lock().unwrap().digest == digest.as_str())
        .unwrap()
        .lock()
        .unwrap();
    let parse = record.cumulative.sum_parse_latency;
    drop(record);
    assert_eq!(parse, std::time::Duration::ZERO);
}

#[test]
fn observation_batch_prelock_breakpoint_does_not_suppress_execution_counter() {
    use tidb_util::topsql_state::{disable_top_sql, enable_top_sql};
    enable_top_sql();
    let mut session = Session::new();
    // Cluster pre-lock runs before the fused Session executor and consumes
    // the breakpoint; SQL observation must retain its independent boundary.
    session.begin_external_executor_breakpoint_scope(true);
    session.notify_before_executor_first_run();
    let sql = "SELECT 903 AS observation_prelock_count";
    session.run(sql).unwrap();
    session.end_external_executor_breakpoint_scope();
    let data = session.statement_stats.take();
    disable_top_sql();
    let (_, digest) = normalize_statement_digest(sql);
    let item = data
        .iter()
        .find(|(key, _)| key.sql_digest.as_bytes() == digest.as_bytes())
        .unwrap()
        .1;
    assert_eq!(item.exec_count, 1);
    assert_eq!(item.duration_count, 1);
}

#[test]
fn observation_batch_routed_prelock_failure_counts_one_execution() {
    use tidb_util::topsql_state::{disable_top_sql, enable_top_sql};
    enable_top_sql();
    let mut session = Session::new();
    let sql = "UPDATE observation_prelock_failure SET id=1";
    let stmt = session.parse_statement(sql).unwrap();
    session.begin_routed_statement_observation(sql, &stmt);
    session.notify_before_executor_first_run();
    session.notify_before_executor_first_run();
    session.finish_routed_statement_observation(false, 0);
    session.finish_routed_statement_observation(false, 0);
    let data = session.statement_stats.take();
    disable_top_sql();
    let (_, digest) = normalize_statement_digest(sql);
    let item = data
        .iter()
        .find(|(key, _)| key.sql_digest.as_bytes() == digest.as_bytes())
        .unwrap()
        .1;
    assert_eq!(item.exec_count, 1);
    assert_eq!(item.duration_count, 1);
}

fn observation_plan_samples(sql: &str) -> (String, String) {
    let (_, digest) = normalize_statement_digest(sql);
    let records =
        tidb_stmtsummary::statement_summary::STMT_SUMMARY_BY_DIGEST_MAP.summary_map_values();
    let record = records
        .iter()
        .find(|record| record.lock().unwrap().digest == digest.as_str())
        .unwrap()
        .lock()
        .unwrap();
    (
        record.cumulative.sample_plan.clone(),
        record.cumulative.sample_binary_plan.clone(),
    )
}

#[test]
fn observation_plan_batch_encoded_plan_reaches_summary() {
    let mut session = Session::new();
    session.set_user("root@%".into(), "root@localhost".into());
    session
        .run("CREATE TABLE observation_encoded (id INT)")
        .unwrap();
    let sql = "SELECT id FROM observation_encoded WHERE id > 71";
    session.run(sql).unwrap();
    let (plan, _) = observation_plan_samples(sql);
    let decoded = String::from_utf8(tidb_util::plancodec::decode_plan(plan).unwrap()).unwrap();
    assert!(
        decoded.contains("observation_encoded"),
        "missing encoded plan: {decoded}"
    );
    assert!(
        decoded.contains("Selection"),
        "missing physical operator: {decoded}"
    );
}

#[test]
fn observation_plan_batch_binary_switch_is_live_across_sessions() {
    let mut session = Session::new();
    session.set_user("root@%".into(), "root@localhost".into());
    let globals = session.vars.global_sysvars();
    let mut setter = Session::new();
    setter.vars.seed_from_globals(globals).unwrap();
    setter
        .vars
        .set_global("tidb_generate_binary_plan", "OFF".into())
        .unwrap();
    let sql = "SELECT 810 AS observation_binary_disabled";
    session.run(sql).unwrap();
    let (_, disabled) = observation_plan_samples(sql);
    setter
        .vars
        .set_global("tidb_generate_binary_plan", "ON".into())
        .unwrap();
    let sql = "SELECT 811 AS observation_binary_enabled";
    session.run(sql).unwrap();
    let (_, enabled) = observation_plan_samples(sql);
    assert!(
        disabled.is_empty(),
        "GLOBAL OFF must suppress the sample binary plan"
    );
    assert!(
        !enabled.is_empty(),
        "existing sessions must observe GLOBAL ON"
    );
}

#[test]
fn observation_plan_batch_binary_prepared_keeps_summary_plan() {
    let mut session = Session::new();
    session.set_user("root@%".into(), "root@localhost".into());
    session
        .run("CREATE TABLE observation_binary_prepared (id INT PRIMARY KEY)")
        .unwrap();
    let sql = "SELECT id FROM observation_binary_prepared WHERE id=?";
    let prepared = session.prepare_ast(sql).unwrap();
    session.set_binary_prepared_execution(true);
    session
        .run_prepared_with_result_authority(&prepared, &[Datum::Int(72)])
        .unwrap();
    let (encoded, binary) = observation_plan_samples(sql);
    assert!(
        !encoded.is_empty(),
        "prepared summary lost its encoded plan"
    );
    assert!(tidb_util::plancodec::decode_binary_plan(binary)
        .unwrap()
        .contains("observation_binary_prepared"));
    assert!(
        session
            .process_plan_info
            .lock()
            .unwrap()
            .brief_binary_plan
            .is_empty(),
        "binary EXECUTE process-list policy must be preserved"
    );
}

fn observation_plan_toggle(sql: &str, begin_enabled: bool, end_enabled: bool) -> StatementStatsMap {
    use tidb_util::topsql_state::{disable_top_sql, enable_top_sql};
    disable_top_sql();
    let mut session = Session::new();
    session
        .run("CREATE TABLE observation_toggle (id INT PRIMARY KEY)")
        .unwrap();
    session
        .run("INSERT INTO observation_toggle VALUES (1)")
        .unwrap();
    session.statement_stats.take();
    if begin_enabled {
        enable_top_sql();
    }
    let stmt = session.parse_statement(sql).unwrap();
    let StatementExecution::Rows(mut result) =
        session.execute_record_set_parsed(stmt, sql).unwrap()
    else {
        panic!("expected rows")
    };
    if end_enabled {
        enable_top_sql();
    } else {
        disable_top_sql();
    }
    result.close().unwrap();
    drop(result);
    disable_top_sql();
    session.statement_stats.take()
}

#[test]
fn observation_plan_batch_topsql_enable_during_scan_keeps_begin() {
    let sql = "SELECT id FROM observation_toggle";
    let data = observation_plan_toggle(sql, false, true);
    let (_, digest) = normalize_statement_digest(sql);
    let item = data
        .iter()
        .find(|(key, _)| key.sql_digest.as_bytes() == digest.as_bytes())
        .expect("execution was lost when enabled after begin")
        .1;
    assert_eq!((item.exec_count, item.duration_count), (1, 1));
}

#[test]
fn observation_plan_batch_topsql_enable_during_fast_plan_keeps_finish() {
    let sql = "SELECT 812 AS observation_fast_toggle";
    let data = observation_plan_toggle(sql, false, true);
    let (_, digest) = normalize_statement_digest(sql);
    let item = data
        .iter()
        .find(|(key, _)| key.sql_digest.as_bytes() == digest.as_bytes())
        .expect("fast execution finish lost across toggle")
        .1;
    assert_eq!((item.exec_count, item.duration_count), (0, 1));
}

#[test]
fn observation_plan_batch_topsql_disabled_and_disable_mid_execution() {
    for (sql, begin_enabled, end_enabled, expected) in [
        (
            "SELECT id FROM observation_toggle",
            false,
            false,
            Some((1, 0)),
        ),
        (
            "SELECT id FROM observation_toggle WHERE id=1",
            false,
            false,
            None,
        ),
        (
            "SELECT 813 AS observation_fast_disabled",
            false,
            false,
            None,
        ),
        (
            "SELECT id FROM observation_toggle",
            true,
            false,
            Some((1, 0)),
        ),
    ] {
        let data = observation_plan_toggle(sql, begin_enabled, end_enabled);
        let (_, digest) = normalize_statement_digest(sql);
        let actual = data
            .iter()
            .find(|(key, _)| key.sql_digest.as_bytes() == digest.as_bytes())
            .map(|(_, item)| (item.exec_count, item.duration_count));
        assert_eq!(
            actual, expected,
            "{sql}, begin={begin_enabled}, end={end_enabled}"
        );
    }
}

#[test]
fn observation_plan_batch_join_cte_and_dml_keep_tree_ownership() {
    let mut session = Session::new();
    session.set_user("root@%".into(), "root@localhost".into());
    session
        .run("CREATE TABLE observation_tree (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session
        .run("INSERT INTO observation_tree VALUES (1,2)")
        .unwrap();
    for (sql, expected) in [
        (
            "SELECT a.id FROM observation_tree a JOIN observation_tree b ON a.v=b.v",
            "(Build)",
        ),
        (
            "WITH c AS (SELECT id FROM observation_tree) SELECT * FROM c",
            "observation_tree",
        ),
        ("UPDATE observation_tree SET v=3 WHERE id=1", "Update"),
        ("DELETE FROM observation_tree WHERE id=1", "Delete"),
        ("INSERT INTO observation_tree SELECT 4,5", "Insert"),
    ] {
        session.run(sql).unwrap();
        let (encoded, binary) = observation_plan_samples(sql);
        let text = String::from_utf8(tidb_util::plancodec::decode_plan(encoded).unwrap()).unwrap();
        assert!(text.contains(expected), "{sql}: {text}");
        assert!(
            !text.contains("UnknownPlanID"),
            "unrecognized physical operator: {text}"
        );
        assert!(!tidb_util::plancodec::decode_binary_plan(binary)
            .unwrap()
            .is_empty());
    }
}

#[test]
fn observation_plan_batch_disabled_set_skips_begin_but_dml_does_not() {
    tidb_util::topsql_state::disable_top_sql();
    let mut session = Session::new();
    session
        .run("CREATE TABLE observation_fast_dml (id INT PRIMARY KEY)")
        .unwrap();
    session.statement_stats.take();
    for (sql, expected) in [
        ("SET @observation_fast=1", 0),
        ("SET sql_mode=''", 0),
        ("SET NAMES utf8mb4", 0),
        ("SET NAMES utf8mb4, autocommit=1", 0),
        ("INSERT INTO observation_fast_dml VALUES (1)", 1),
        ("UPDATE observation_fast_dml SET id=2 WHERE id=1", 1),
        ("DELETE FROM observation_fast_dml WHERE id=2", 1),
    ] {
        if sql.starts_with("SET") {
            assert_eq!(session.parse_statement(sql).unwrap().label(), "Set");
        }
        session.run(sql).unwrap();
        let data = session.statement_stats.take();
        let (_, digest) = normalize_statement_digest(sql);
        let item = data
            .iter()
            .find(|(key, _)| key.sql_digest.as_bytes() == digest.as_bytes());
        assert_eq!(
            item.map_or(0, |(_, item)| item.exec_count),
            expected,
            "{sql}: {data:?}"
        );
        assert!(data.values().all(|item| item.duration_count == 0));
    }
}

#[test]
fn cte_scope_batch_summary_attributes_real_table_shadowed_in_sibling() {
    let mut session = Session::new();
    session.set_user("root@%".into(), "root@localhost".into());
    session.run("CREATE TABLE cte_observation (a INT)").unwrap();
    let sql = "SELECT t.a FROM cte_observation t JOIN (WITH cte_observation AS (SELECT 1 AS a) SELECT a FROM cte_observation) d ON TRUE";
    session.run(sql).unwrap();
    let (_, digest) = normalize_statement_digest(sql);
    let records =
        tidb_stmtsummary::statement_summary::STMT_SUMMARY_BY_DIGEST_MAP.summary_map_values();
    let record = records
        .iter()
        .find(|r| r.lock().unwrap().digest == digest.as_str())
        .unwrap()
        .lock()
        .unwrap();
    assert_eq!(record.table_names, "test.cte_observation");
}

#[test]
fn ddl_visit_batch_summary_keeps_both_rename_tables_and_like_source() {
    let mut session = Session::new();
    session.set_user("root@%".into(), "root@localhost".into());
    session
        .run("CREATE TABLE ddl_visit_source (a INT)")
        .unwrap();
    for (sql, expected) in [
        (
            "CREATE TABLE ddl_visit_copy LIKE ddl_visit_source",
            "test.ddl_visit_copy,test.ddl_visit_source",
        ),
        (
            "RENAME TABLE ddl_visit_copy TO ddl_visit_destination",
            "test.ddl_visit_copy,test.ddl_visit_destination",
        ),
        (
            "TRUNCATE TABLE ddl_visit_destination",
            "test.ddl_visit_destination",
        ),
    ] {
        session.run(sql).unwrap();
        let (_, digest) = normalize_statement_digest(sql);
        let records =
            tidb_stmtsummary::statement_summary::STMT_SUMMARY_BY_DIGEST_MAP.summary_map_values();
        let record = records
            .iter()
            .find(|r| r.lock().unwrap().digest == digest.as_str())
            .unwrap()
            .lock()
            .unwrap();
        assert_eq!(record.table_names, expected, "{sql}");
    }
}
