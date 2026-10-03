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
