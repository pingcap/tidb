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

use super::*;

/// The session's cop client capability is shared, and a read that met
/// another user of it answered "unary client is already in use by another
/// response": the transport took it with `try_lock` and held it across the
/// whole RPC. Go's client is concurrent and never refuses a caller. This is
/// `executor/foreign_key`'s script, whose first `count(*)` failed every time.
#[test]
fn a_read_never_finds_the_shared_cop_client_in_use() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(140)).unwrap();
    rows(&mut session, "SET @@foreign_key_checks = 1");
    rows(
        &mut session,
        "CREATE TABLE test.t1 (id INT AUTO_INCREMENT KEY, b INT, INDEX (b))",
    );
    rows(
        &mut session,
        "CREATE TABLE test.t2 (id INT, b INT, \
         FOREIGN KEY fk (b) REFERENCES test.t1 (b) ON UPDATE SET NULL)",
    );
    rows(
        &mut session,
        "INSERT INTO test.t1 (b) VALUES (1), (2), (3), (4), (5), (6), (7), (8)",
    );
    for _ in 0..12 {
        rows(
            &mut session,
            "INSERT INTO test.t1 (b) SELECT id FROM test.t1",
        );
    }
    assert_eq!(
        displayed(rows(&mut session, "SELECT COUNT(*) FROM test.t1")),
        [["32768"]]
    );
}

#[test]
fn global_binding_reload_decodes_timestamps_in_its_session_timezone() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(124))
        .expect("session");
    rows(&mut session, "SET GLOBAL time_zone = '+08:00'");
    rows(&mut session, "SET SESSION time_zone = '+00:00'");
    rows(&mut session, "INSERT INTO mysql.bind_info (original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation, source, sql_digest) VALUES ('select ?','SELECT 1','test','enabled','2026-09-07 00:00:00.123456','2026-09-07 01:00:00.654321','utf8mb4','utf8mb4_bin','manual','binding_timezone_probe')");
    let bindings = displayed(rows(&mut session, "SHOW GLOBAL BINDINGS"));
    let binding = bindings
        .iter()
        .find(|row| row[9] == "binding_timezone_probe")
        .expect("committed binding appears in the shared cache");
    assert_eq!(binding[4], "2026-09-07 08:00:00.123456");
    assert_eq!(binding[5], "2026-09-07 09:00:00.654321");
}

#[test]
fn global_bindings_do_not_depend_on_unrelated_staged_writes() {
    let (stack, _users) = cop_backed_stack();
    let mut writer = stack
        .factory
        .open_session(session_context(117))
        .expect("writer");
    rows(
        &mut writer,
        "CREATE TABLE test.binding_visibility (id BIGINT PRIMARY KEY, v INT)",
    );
    rows(
        &mut writer,
        "INSERT INTO test.binding_visibility VALUES (1,20),(2,30),(3,40),(4,50),(5,60),(6,70)",
    );
    // A committed storage row, as if a peer had created the global binding.
    // The matcher must not infer its existence from the reader's write buffer.
    rows(&mut writer, "INSERT INTO mysql.bind_info (original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation, source, sql_digest) VALUES ('select v from test.binding_visibility where v = ?', 'SELECT v FROM test.binding_visibility USE INDEX () WHERE v = 20', 'test', 'enabled', '2026-09-07 00:00:00', '2026-09-07 00:00:00', 'utf8mb4', 'utf8mb4_bin', 'manual', 'binding_visibility_probe')");
    let mut reader = stack
        .factory
        .open_session(session_context(118))
        .expect("reader");
    reader
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin reader");
    for dirty in [true, false] {
        if dirty {
            rows(
                &mut reader,
                "UPDATE test.binding_visibility SET v=v+1 WHERE id>1",
            );
        } else {
            reader
                .node_conn()
                .control_transaction("ROLLBACK")
                .expect("rollback reader");
            reader
                .node_conn()
                .control_transaction("BEGIN PESSIMISTIC")
                .expect("begin reader");
        }
        assert_eq!(
            displayed(rows(
                &mut reader,
                "SELECT v FROM test.binding_visibility WHERE v=20"
            )),
            [["20"]]
        );
        assert_eq!(
            displayed(rows(&mut reader, "SELECT @@last_plan_from_binding")),
            [["1"]],
            "global binding must apply with dirty={dirty}"
        );
    }
    reader
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("rollback reader");

    let assert_binding = |reader: &mut crate::Session, expected: &str, phase: &str| {
        let (result, ops) = tidb_kv::storage::capture_storage_ops(|| {
            rows(reader, "SELECT v FROM test.binding_visibility WHERE v=20")
        });
        assert_eq!(displayed(result), [["20"]]);
        assert_eq!(
            ops.cop_scans, 1,
            "only the user table may be scanned: {ops:?}"
        );
        assert_eq!(
            displayed(rows(reader, "SELECT @@last_plan_from_binding")),
            [[expected]],
            "{phase}"
        );
    };
    // Uncommitted binding writes must not enter any session's planner image.
    writer
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin writer");
    rows(
        &mut writer,
        "UPDATE mysql.bind_info SET status='disabled', update_time='2026-09-07 00:00:01' WHERE sql_digest='binding_visibility_probe'",
    );
    assert_binding(&mut reader, "1", "reader during uncommitted disable");
    assert_binding(&mut writer, "1", "writer during uncommitted disable");
    writer
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("rollback writer");
    assert_binding(&mut reader, "1", "after rollback");

    for (version, (status, expected)) in [("disabled", "0"), ("enabled", "1"), ("deleted", "0")]
        .into_iter()
        .enumerate()
    {
        writer
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin writer");
        rows(&mut writer, &format!("UPDATE mysql.bind_info SET status='{status}', update_time='2026-09-07 00:00:0{}' WHERE sql_digest='binding_visibility_probe'", version + 1));
        writer
            .node_conn()
            .control_transaction("COMMIT")
            .expect("commit writer");
        for _ in 0..3 {
            assert_binding(&mut reader, expected, status);
        }
    }
}

/// Go `PlanReplayerExec`: `PLAN REPLAYER DUMP EXPLAIN` writes the archive
/// into the global storage's replayer directory (Go's files, the table's
/// schema, statistics and explain), records the dump in
/// `mysql.plan_replayer_status` and answers the file token, which
/// `@@tidb_last_plan_replayer_token` then shows; `CAPTURE` registers a task
/// once, and `CAPTURE REMOVE` deletes it.
#[test]
fn plan_replayer_dump_writes_gos_archive_and_status() {
    let base = std::env::temp_dir().join(format!("plan-replayer-dump-{}", std::process::id()));
    tidb_planner_extstore::set_global_ext_storage_for_test(Some(Arc::new(
        tidb_objstore::LocalStorage::new(&base.to_string_lossy()).unwrap(),
    )));
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack.factory.open_session(session_context(77)).expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "CREATE TABLE replayed (a INT, b INT, INDEX ia(a))");
    rows(&mut session, "INSERT INTO replayed VALUES (1, 1), (2, 2)");
    rows(&mut session, "ANALYZE TABLE replayed");
    let answer = displayed(rows(&mut session, "PLAN REPLAYER DUMP EXPLAIN SELECT * FROM replayed WHERE a = 1"));
    assert_eq!(answer.len(), 1);
    assert_eq!(answer[0][0], "File token");
    let token = answer[0][1].clone();
    assert!(token.starts_with("replayer_") && token.ends_with(".zip"), "{token}");
    assert_eq!(displayed(rows(&mut session, "SELECT @@tidb_last_plan_replayer_token")), [[token.clone()]]);
    let archive = std::fs::read(base.join("replayer").join(&token)).expect("the archive is written");
    let files: std::collections::BTreeMap<String, Vec<u8>> =
        tidb_replayer::zip::read_zip(&archive).unwrap().into_iter().collect();
    for name in [
        "sql_meta.toml",
        "config.toml",
        "meta.txt",
        "schema/test.replayed.schema.txt",
        "schema/schema_meta.txt",
        "table_tiflash_replica.txt",
        "stats/test.replayed.json",
        "statsMem/test.replayed.txt",
        "variables.toml",
        "sql/sql0.sql",
        "session_bindings.sql",
        "global_bindings.sql",
        "explain.txt",
        "debug_trace/debug_trace0.json",
    ] {
        assert!(files.contains_key(name), "missing {name}: {:?}", files.keys().collect::<Vec<_>>());
    }
    let text = |name: &str| String::from_utf8(files[name].clone()).unwrap();
    assert_eq!(text("sql/sql0.sql"), "SELECT * FROM replayed WHERE a = 1");
    assert!(!text("sql_meta.toml").contains("enableHistoricalStats"), "{}", text("sql_meta.toml"));
    assert!(text("schema/test.replayed.schema.txt").starts_with("create database if not exists `test`; use `test`;CREATE TABLE `replayed`"));
    assert!(text("explain.txt").contains("IndexLookUp") || text("explain.txt").contains("Reader"), "{}", text("explain.txt"));
    let stats: serde_json::Value = serde_json::from_slice(&files["stats/test.replayed.json"]).unwrap();
    assert_eq!(stats["table_name"], "replayed", "{stats}");
    assert!(text("statsMem/test.replayed.txt").starts_with("[INDEX]\nia="), "{}", text("statsMem/test.replayed.txt"));
    assert_eq!(
        displayed(rows(&mut session, "SELECT origin_sql, token, fail_reason FROM mysql.plan_replayer_status")),
        [["SELECT * FROM replayed WHERE a = 1".to_owned(), token, "NULL".to_owned()]]
    );

    rows(&mut session, "PLAN REPLAYER CAPTURE '123' '123'");
    assert_eq!(
        displayed(rows(&mut session, "SELECT sql_digest, plan_digest FROM mysql.plan_replayer_task")),
        [["123", "123"]]
    );
    let again = session.run("PLAN REPLAYER CAPTURE '123' '123'").unwrap_err();
    assert_eq!(again.to_mysql_error().message, "plan replayer capture task already exists");
    rows(&mut session, "PLAN REPLAYER CAPTURE REMOVE '123' '123'");
    assert!(rows(&mut session, "SELECT * FROM mysql.plan_replayer_task").is_empty());
    // Go master `buildPlanReplayer` (#72730): both digests reach internal SQL,
    // so only hexadecimal ones (or `*` for the plan) are accepted.
    for (sql, message) in [
        (
            "PLAN REPLAYER CAPTURE 'x'' or ''1''=''1' '123'",
            "invalid SQL digest: expected a non-empty hexadecimal string of at most 128 characters",
        ),
        (
            "PLAN REPLAYER CAPTURE REMOVE '' '123'",
            "invalid SQL digest: expected a non-empty hexadecimal string of at most 128 characters",
        ),
        (
            "PLAN REPLAYER CAPTURE 'abc' 'x'' or ''1''=''1'",
            "invalid plan digest: expected a non-empty hexadecimal string of at most 128 characters or '*'",
        ),
    ] {
        assert_eq!(session.run(sql).unwrap_err().to_mysql_error().message, message, "{sql}");
    }
    let long = "a".repeat(129);
    assert!(session.run(&format!("PLAN REPLAYER CAPTURE '{long}' '*'")).is_err());
    rows(&mut session, "PLAN REPLAYER CAPTURE 'ABCdef0' '*'");
    rows(&mut session, "PLAN REPLAYER CAPTURE REMOVE 'ABCdef0' '*'");
    assert!(rows(&mut session, "SELECT * FROM mysql.plan_replayer_task").is_empty());
    tidb_planner_extstore::set_global_ext_storage_for_test(None);
    std::fs::remove_dir_all(&base).unwrap();
}

/// Go `TestIssue46197`: `PLAN REPLAYER DUMP EXPLAIN 'file'` asks the client
/// for the file, then `DumpSQLsFromFile` dumps each `;`-separated statement
/// and keeps the token; an empty file dumps nothing, a statement that does
/// not parse fails with Go's message, and the slow-query form (which Go's
/// builder ignores) answers "sql is empty".
#[test]
fn plan_replayer_dump_of_a_client_file_runs_gos_file_dump() {
    let base = std::env::temp_dir().join(format!("plan-replayer-file-{}", std::process::id()));
    tidb_planner_extstore::set_global_ext_storage_for_test(Some(Arc::new(
        tidb_objstore::LocalStorage::new(&base.to_string_lossy()).unwrap(),
    )));
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack.factory.open_session(session_context(79)).expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "CREATE TABLE t1 (a INT, b INT)");
    let replayer_files = || std::fs::read_dir(base.join("replayer")).map_or(0, |dir| dir.count());
    let mut run_with_file = |session: &mut Session, sql: &str, data: &[u8]| {
        let stmt = session.parse_at_statement_boundary(sql).unwrap();
        assert!(session.node_conn().execute_stmt(sql, &stmt).unwrap().is_none(), "{sql} answers no rows");
        assert_eq!(session.local_infile_request().as_deref(), Some("a.txt"));
        session.node_conn().finish_local_infile(data)
    };

    run_with_file(&mut session, "plan replayer dump explain 'a.txt'", b"select * from t1;").unwrap();
    let token = displayed(rows(&mut session, "select @@tidb_last_plan_replayer_token"))[0][0].clone();
    assert!(token.starts_with("replayer_") && token.ends_with(".zip"), "{token}");
    let archive = std::fs::read(base.join("replayer").join(&token)).expect("the archive is written");
    let files: std::collections::BTreeMap<String, Vec<u8>> =
        tidb_replayer::zip::read_zip(&archive).unwrap().into_iter().collect();
    assert_eq!(String::from_utf8(files["sql/sql0.sql"].clone()).unwrap(), "select * from t1");
    assert!(files.contains_key("schema/test.t1.schema.txt"), "{:?}", files.keys().collect::<Vec<_>>());

    // Two statements, newlines trimmed.
    run_with_file(&mut session, "plan replayer dump explain 'a.txt'", b"select a from t1;\nselect b from t1;\n")
        .unwrap();
    let token = displayed(rows(&mut session, "select @@tidb_last_plan_replayer_token"))[0][0].clone();
    let archive = std::fs::read(base.join("replayer").join(&token)).unwrap();
    let files: std::collections::BTreeMap<String, Vec<u8>> =
        tidb_replayer::zip::read_zip(&archive).unwrap().into_iter().collect();
    assert_eq!(String::from_utf8(files["sql/sql0.sql"].clone()).unwrap(), "select a from t1");
    assert_eq!(String::from_utf8(files["sql/sql1.sql"].clone()).unwrap(), "select b from t1");

    // An empty file: the file was created, nothing is dumped, the token stays.
    let before = replayer_files();
    run_with_file(&mut session, "plan replayer dump explain 'a.txt'", b"").unwrap();
    assert_eq!(replayer_files(), before + 1);
    assert_eq!(displayed(rows(&mut session, "select @@tidb_last_plan_replayer_token"))[0][0], token);

    let error = run_with_file(&mut session, "plan replayer dump explain 'a.txt'", b"selec 1;").unwrap_err();
    assert!(error.message.starts_with("parse sql error, sql:selec 1, err:[parser:1064]"), "{}", error.message);

    let error = session.run("plan replayer dump explain slow query where a > 1 limit 2").unwrap_err();
    assert_eq!(error.to_mysql_error().message, "plan replayer: sql is empty");

    tidb_planner_extstore::set_global_ext_storage_for_test(None);
    std::fs::remove_dir_all(&base).unwrap();
}

/// Go `PlanReplayerLoadInfo.Update` (`TestPlanReplayerLoad`'s round
/// trip): `PLAN REPLAYER LOAD 'file'` asks the client for the archive, then
/// recreates the dumped table, its statistics and its global binding,
/// turns auto-analyze off with Go's warning; an empty path fails in the
/// executor and an empty file loads nothing.
#[test]
fn plan_replayer_load_restores_a_dump_like_go() {
    let base = std::env::temp_dir().join(format!("plan-replayer-load-{}", std::process::id()));
    tidb_planner_extstore::set_global_ext_storage_for_test(Some(Arc::new(
        tidb_objstore::LocalStorage::new(&base.to_string_lossy()).unwrap(),
    )));
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack.factory.open_session(session_context(80)).expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "CREATE TABLE replayed_load (a INT, b INT, INDEX ia(a))");
    rows(&mut session, "INSERT INTO replayed_load VALUES (1, 1), (2, 2), (3, 3)");
    rows(&mut session, "ANALYZE TABLE replayed_load");
    rows(
        &mut session,
        "CREATE GLOBAL BINDING FOR SELECT * FROM replayed_load WHERE a = 1 USING SELECT * FROM replayed_load USE INDEX (ia) WHERE a = 1",
    );
    let answer = displayed(rows(&mut session, "PLAN REPLAYER DUMP EXPLAIN SELECT * FROM replayed_load WHERE a = 1"));
    let archive = std::fs::read(base.join("replayer").join(&answer[0][1])).expect("the archive is written");
    rows(&mut session, "DROP GLOBAL BINDING FOR SELECT * FROM replayed_load WHERE a = 1");
    rows(&mut session, "DROP TABLE replayed_load");
    rows(&mut session, "SET GLOBAL tidb_enable_auto_analyze = ON");
    rows(&mut session, "USE mysql");

    let mut run_with_file = |session: &mut Session, sql: &str, data: &[u8]| {
        let stmt = session.parse_at_statement_boundary(sql).unwrap();
        assert!(session.node_conn().execute_stmt(sql, &stmt).unwrap().is_none(), "{sql} answers no rows");
        assert_eq!(session.local_infile_request().as_deref(), Some("r.zip"));
        session.node_conn().finish_local_infile(data)
    };
    // An empty file loads nothing.
    run_with_file(&mut session, "plan replayer load 'r.zip'", b"").unwrap();
    assert!(session.run("select * from test.replayed_load").is_err());

    run_with_file(&mut session, "plan replayer load 'r.zip'", &archive).unwrap();
    let warnings = displayed(rows(&mut session, "show warnings"));
    assert_eq!(
        warnings.last().map(|warning| warning[2].as_str()),
        Some("`PLAN REPLAYER LOAD` sets @@global.tidb_enable_auto_analyze=OFF to keep restored statistics stable; re-enable it manually if needed"),
        "{warnings:?}"
    );
    assert_eq!(displayed(rows(&mut session, "select @@global.tidb_enable_auto_analyze")), [["0"]]);
    // The schema file's `use` moved the session to the dump's database.
    assert_eq!(displayed(rows(&mut session, "select database()")), [["test"]]);
    assert_eq!(displayed(rows(&mut session, "select count(*) from replayed_load")), [["0"]]);
    assert_eq!(
        displayed(rows(
            &mut session,
            "select modify_count, count from mysql.stats_meta m join information_schema.tables t on m.table_id = t.tidb_table_id where t.table_name = 'replayed_load'",
        )),
        [["0", "3"]]
    );
    let bindings = displayed(rows(&mut session, "show global bindings"));
    assert!(
        bindings.iter().any(|binding| binding[1].contains("USE INDEX (`ia`)") && binding[2] == "test"),
        "{bindings:?}"
    );

    let error = session.run("plan replayer load ''").unwrap_err();
    assert_eq!(error.to_mysql_error().message, "plan replayer: file path is empty");
    let error = run_with_file(&mut session, "plan replayer load 'r.zip'", b"not a zip").unwrap_err();
    assert_eq!(error.message, "zip: not a valid zip file");

    tidb_planner_extstore::set_global_ext_storage_for_test(None);
    std::fs::remove_dir_all(&base).unwrap();
}

/// Go `TestExtractPlanWithoutHistoryView`,
/// `TestExtractWithoutStmtSummaryPersistedEnabled` and
/// `TestExtractHandlePlanTask`: the extract handle dumps the plans the
/// statement summary recorded in the range, with their schemas, statistics,
/// variables and bindings, into the extract directory; the history view
/// needs persisted statement summaries.
#[test]
fn extract_handle_dumps_the_summarized_plans_like_go() {
    let base = std::env::temp_dir().join(format!("extract-task-{}", std::process::id()));
    tidb_planner_extstore::set_global_ext_storage_for_test(Some(Arc::new(
        tidb_objstore::LocalStorage::new(&base.to_string_lossy()).unwrap(),
    )));
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    crate::domain::cluster_session_node::setup_extract_handle(&stack.factory);
    let handle = stack.factory.get_extract_handle().expect("the extract handle is set up");
    let now = || chrono::Local::now().fixed_offset();

    // Without the history view an empty range still dumps a package.
    let mut task = tidb_domain::extract::new_extract_plan_task(now(), now());
    task.use_history_view = false;
    let name = handle.extract_task(&task).unwrap();
    assert!(name.starts_with("extract_") && name.ends_with(".zip"), "{name}");
    assert!(base.join("extract").join(&name).exists());

    // The history view needs persisted statement summaries.
    task.use_history_view = true;
    assert_eq!(
        handle.extract_task(&task).unwrap_err().to_string(),
        "tidb_stmt_summary_enable_persistent should be enabled for extract task"
    );

    let mut session = stack.factory.open_session(session_context(81)).expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "CREATE TABLE extracted (id INT)");
    rows(&mut session, "CREATE VIEW extracted_v AS SELECT id FROM extracted");
    let start = now() - chrono::Duration::seconds(1);
    rows(&mut session, "SELECT * FROM extracted");
    rows(&mut session, "SELECT * FROM extracted_v");
    let end = now() + chrono::Duration::seconds(1);
    let mut task = tidb_domain::extract::new_extract_plan_task(start, end);
    task.use_history_view = false;
    let name = handle.extract_task(&task).unwrap();
    let archive = std::fs::read(base.join("extract").join(&name)).expect("the package is written");
    let files: std::collections::BTreeMap<String, Vec<u8>> =
        tidb_replayer::zip::read_zip(&archive).unwrap().into_iter().collect();
    let text = |name: &str| String::from_utf8(files[name].clone()).unwrap();
    for name in [
        "config.toml",
        "meta.txt",
        "extract_meta.txt",
        "schema/schema_meta.txt",
        "schema/test.extracted.schema.txt",
        "view/test.extracted_v.view.txt",
        "table_tiflash_replica.txt",
        "variables.toml",
        "global_bindings.sql",
        "stats/test.extracted.json",
    ] {
        assert!(files.contains_key(name), "missing {name}: {:?}", files.keys().collect::<Vec<_>>());
    }
    assert_eq!(text("extract_meta.txt"), "SkipStats = \"false\"\ntaskType = \"Plan\"\n");
    let records: Vec<serde_json::Value> = files
        .iter()
        .filter(|(name, _)| name.starts_with("SQLs/"))
        .map(|(_, bytes)| serde_json::from_slice(bytes).unwrap())
        .collect();
    let sqls: std::collections::BTreeSet<String> =
        records.iter().map(|record| record["sql"].as_str().unwrap().to_owned()).collect();
    assert!(sqls.contains("SELECT * FROM extracted"), "{records:?}");
    assert!(sqls.contains("SELECT * FROM extracted_v"), "{records:?}");
    for record in &records {
        assert_eq!(record["schema"], "test");
        assert!(!record["plan"].as_str().unwrap().is_empty(), "{record}");
        assert!(!record["plan"].as_str().unwrap().ends_with('\n'), "{record}");
    }

    // STATEMENTS_SUMMARY is the current window only: a statement an expired
    // window recorded is in the history view, not in this package.
    rows(&mut session, "SET GLOBAL tidb_stmt_summary_refresh_interval = 1");
    rows(&mut session, "SELECT id FROM extracted WHERE id = 42");
    std::thread::sleep(std::time::Duration::from_millis(2200));
    rows(&mut session, "SELECT 1");
    let expired = displayed(rows(
        &mut session,
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATEMENTS_SUMMARY_HISTORY WHERE QUERY_SAMPLE_TEXT = 'SELECT id FROM extracted WHERE id = 42'",
    ));
    assert_eq!(expired, [["1"]]);
    let mut current = tidb_domain::extract::new_extract_plan_task(start, now() + chrono::Duration::seconds(1));
    current.use_history_view = false;
    let name = handle.extract_task(&current).unwrap();
    let archive = std::fs::read(base.join("extract").join(&name)).unwrap();
    let files: std::collections::BTreeMap<String, Vec<u8>> =
        tidb_replayer::zip::read_zip(&archive).unwrap().into_iter().collect();
    let sqls: Vec<String> = files
        .iter()
        .filter(|(name, _)| name.starts_with("SQLs/"))
        .map(|(_, bytes)| serde_json::from_slice::<serde_json::Value>(bytes).unwrap()["sql"].as_str().unwrap().to_owned())
        .collect();
    assert!(!sqls.iter().any(|sql| sql.contains("id = 42")), "{sqls:?}");
    rows(&mut session, "SET GLOBAL tidb_stmt_summary_refresh_interval = DEFAULT");
    let mut task = tidb_domain::extract::new_extract_plan_task(start, now() + chrono::Duration::seconds(1));
    task.use_history_view = false;

    // SkipStats leaves the statistics out.
    task.skip_stats = true;
    let name = handle.extract_task(&task).unwrap();
    let archive = std::fs::read(base.join("extract").join(&name)).unwrap();
    let files: std::collections::BTreeMap<String, Vec<u8>> =
        tidb_replayer::zip::read_zip(&archive).unwrap().into_iter().collect();
    assert!(!files.keys().any(|name| name.starts_with("stats/")), "{:?}", files.keys().collect::<Vec<_>>());
    assert_eq!(String::from_utf8(files["extract_meta.txt"].clone()).unwrap(), "SkipStats = \"true\"\ntaskType = \"Plan\"\n");

    tidb_planner_extstore::set_global_ext_storage_for_test(None);
    std::fs::remove_dir_all(&base).unwrap();
}

/// Go's capture path: `PLAN REPLAYER CAPTURE` reloads the handle's tasks,
/// a matching statement's `FinishExecuteStmt` sends a dump task carrying its
/// own variables, statement and encoded plan, a dump worker writes the
/// archive on a system session and records the status, and the dump-file
/// GC removes expired files on its lease.
#[test]
fn plan_replayer_capture_dumps_a_matching_statement_and_gc_expires_files() {
    let base = std::env::temp_dir().join(format!("plan-replayer-capture-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let storage = Arc::new(tidb_objstore::LocalStorage::new(&base.to_string_lossy()).unwrap());
    tidb_planner_extstore::set_global_ext_storage_for_test(Some(storage.clone() as tidb_planner_extstore::ExtStorage));
    let nanos = |at: chrono::DateTime<chrono::Utc>| at.timestamp_nanos_opt().unwrap();
    let month_ago = nanos(chrono::Utc::now() - chrono::Duration::days(30));
    let old_dump = format!("replayer/replayer_old_{month_ago}.zip");
    let old_extract = format!("extract/extract_old_{month_ago}.zip");
    let fresh_dump = format!("replayer/replayer_fresh_{}.zip", nanos(chrono::Utc::now()));
    for name in [&old_dump, &old_extract, &fresh_dump] {
        std::fs::create_dir_all(base.join(name).parent().unwrap()).unwrap();
        std::fs::write(base.join(name), b"x").unwrap();
    }
    tidb_vardef::set_plan_replayer_gc_lease(200_000_000);
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    crate::domain::cluster_session_node::start_plan_replayer_handle(&stack.factory);
    let mut session = stack.factory.open_session(session_context(78)).expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "CREATE TABLE captured (a INT, b INT, INDEX ia(a))");
    rows(&mut session, "INSERT INTO captured VALUES (1, 1), (2, 2)");
    let sql = "SELECT * FROM captured WHERE a = 1";
    let (_, digest) = tidb_parser::normalize_digest(sql);
    let digest = digest.as_str().to_owned();
    rows(&mut session, &format!("PLAN REPLAYER CAPTURE '{digest}' '*'"));
    // Go `registerCaptureTask` reloads the handle's tasks at once.
    let handle = stack.factory.plan_replayer_handle().expect("the handle started");
    assert_eq!(
        handle.collector.get_tasks(),
        [tidb_replayer::PlanReplayerTaskKey { sql_digest: digest.clone(), plan_digest: "*".to_owned() }]
    );
    // The capturing session's own value reaches the dump.
    rows(&mut session, "SET @@tidb_opt_agg_push_down = ON");
    assert_eq!(displayed(rows(&mut session, sql)), [["1", "1"]]);
    let status = format!("SELECT plan_digest, origin_sql, token, fail_reason FROM mysql.plan_replayer_status WHERE sql_digest = '{digest}'");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let row = loop {
        let found = displayed(rows(&mut session, &status));
        if let Some(row) = found.into_iter().next() {
            break row;
        }
        assert!(std::time::Instant::now() < deadline, "no capture status record");
        std::thread::sleep(std::time::Duration::from_millis(50));
    };
    assert_ne!(row[0], "");
    assert_eq!(row[1], sql);
    assert_eq!(row[3], "NULL");
    let token = row[2].clone();
    assert!(token.starts_with("capture_normal_replayer_") && token.ends_with(".zip"), "{token}");
    let archive = std::fs::read(base.join("replayer").join(&token)).expect("the archive is written");
    let files: std::collections::BTreeMap<String, Vec<u8>> =
        tidb_replayer::zip::read_zip(&archive).unwrap().into_iter().collect();
    let text = |name: &str| String::from_utf8(files[name].clone()).unwrap();
    assert_eq!(text("sql/sql0.sql"), sql);
    assert!(text("sql_meta.toml").contains("isCapture = \"true\""), "{}", text("sql_meta.toml"));
    assert!(text("sql_meta.toml").contains(&format!("sqlDigest = \"{digest}\"")), "{}", text("sql_meta.toml"));
    assert!(text("variables.toml").contains("tidb_opt_agg_push_down = \"ON\""));
    // The encoded plan, decoded, instead of a fresh EXPLAIN.
    assert!(!files.contains_key("explain.txt"));
    assert!(text("explain/sql.txt").contains("ia"), "{}", text("explain/sql.txt"));

    // Go `checkPlanReplayerContinuesCapture`: every SELECT/DML sends once per
    // key; other statements send nothing.
    rows(&mut session, "SET @@tidb_enable_plan_replayer_continuous_capture = ON");
    let continuous = "SELECT b FROM captured WHERE a = 2";
    rows(&mut session, continuous);
    rows(&mut session, continuous);
    rows(&mut session, "SET @unrelated = 1");
    assert_eq!(session.vars.plan_replayer_finished_task_key.len(), 1, "{:?}", session.vars.plan_replayer_finished_task_key);
    let (_, continuous_digest) = tidb_parser::normalize_digest(continuous);
    let status = format!(
        "SELECT token FROM mysql.plan_replayer_status WHERE sql_digest = '{}'",
        continuous_digest.as_str()
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let token = loop {
        if let Some(row) = displayed(rows(&mut session, &status)).into_iter().next() {
            break row[0].clone();
        }
        assert!(std::time::Instant::now() < deadline, "no continuous capture status record");
        std::thread::sleep(std::time::Duration::from_millis(50));
    };
    assert!(token.starts_with("capture_normal_replayer_"), "{token}");
    // The GC lease passes: expired files go, fresh ones stay.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while base.join(&old_dump).exists() || base.join(&old_extract).exists() {
        assert!(std::time::Instant::now() < deadline, "the dump-file GC never ran");
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert!(base.join(&fresh_dump).exists());
    assert!(base.join("replayer").join(&token).exists());
    tidb_planner_extstore::set_global_ext_storage_for_test(None);
    std::fs::remove_dir_all(&base).unwrap();
}

/// Go `getSessionFactoryInternal`: an internal session uses the default
/// coprocessor encoding (`EnableChunkRPC = false`), a client session the
/// chunk one.
#[test]
fn internal_sessions_use_the_default_coprocessor_encoding() {
    let (stack, _users) = cop_backed_stack();
    let internal = stack.factory.open_system_session().expect("system session opens");
    assert_eq!(internal.vars.get_system("tidb_enable_chunk_rpc").unwrap(), "OFF");
    assert!(!internal.statement_context_of(false, false, true).enable_chunk_rpc());
    let mut client = stack.factory.open_session(session_context(85)).expect("session opens");
    assert_eq!(client.vars.get_system("tidb_enable_chunk_rpc").unwrap(), "ON");
    assert!(client.statement_context_of(false, false, true).enable_chunk_rpc());
    // Go `SessionVars.EnableChunkRPC` follows `SET tidb_enable_chunk_rpc`.
    assert!(client.statement_context_of(true, false, false).enable_chunk_rpc());
    rows(&mut client, "SET tidb_enable_chunk_rpc = 0");
    // A query's context and a write's (whose reads ask the stores too).
    assert!(!client.statement_context_of(false, false, true).enable_chunk_rpc());
    assert!(!client.statement_context_of(true, false, false).enable_chunk_rpc());
}

/// The probe-33 regression: a derived table whose inner SELECT plans as a
/// partial-aggregate push (root HashAgg over `TableReader(data:HashAgg)`)
/// must still answer the aggregate, not the bare scan rows. Go returns
/// `((1,30),(2,120))`; the broken handoff returned five raw rows.
#[test]
fn a_derived_aggregate_over_the_coprocessor_answers_its_output() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(7))
        .expect("session opens");
    rows(&mut session, "CREATE TABLE test.rep (g int, v int)");
    rows(
        &mut session,
        "INSERT INTO test.rep VALUES (1, 10), (1, 20), (2, 40), (2, 80), (1, 0)",
    );

    let inner = displayed(rows(
        &mut session,
        "SELECT g, sum(v) AS t FROM test.rep GROUP BY g ORDER BY g",
    ));
    assert_eq!(
        inner,
        [["1", "30"], ["2", "120"]],
        "the inner aggregate alone must already be right"
    );

    let derived = displayed(rows(
        &mut session,
        "SELECT * FROM (SELECT g, sum(v) AS t FROM test.rep GROUP BY g) s ORDER BY g",
    ));
    assert_eq!(
        derived,
        [["1", "30"], ["2", "120"]],
        "the derived consumer must see the aggregate, not the scan rows"
    );

    // The COUNT(*) shape panicked the worker thread on the live node
    // (chunk column index out of bounds); here a panic fails the test.
    let counted = displayed(rows(
        &mut session,
        "SELECT * FROM (SELECT g, count(*) AS c FROM test.rep GROUP BY g) s ORDER BY g",
    ));
    assert_eq!(counted, [["1", "3"], ["2", "2"]]);

    // The desc keep-order Limit rides the REVERSED region walk -- the
    // shape whose first live draft returned NOTHING because the reverse
    // scan's caller-swaps-the-bounds contract was missed.
    rows(
        &mut session,
        "CREATE TABLE test.walk (id bigint primary key, v int)",
    );
    rows(
        &mut session,
        "INSERT INTO test.walk VALUES (1, 10), (2, 20), (3, 30), (5, 50), (100, 1)",
    );
    let descending = displayed(rows(
        &mut session,
        "SELECT id FROM test.walk WHERE id > 1 ORDER BY id DESC LIMIT 2",
    ));
    assert_eq!(
        descending,
        [["100"], ["5"]],
        "the desc keep-order Limit must answer the LARGEST ids over the region walk"
    );

    // The covering-index COUNT rides an [IndexScan, Aggregation] DAG:
    // the region decodes the indexed values out of the KEY and counts
    // them, Go's PhysicalIndexReader carrying the partial stage.
    rows(&mut session, "CREATE INDEX walk_v ON test.walk (v)");
    let counted_over_index = displayed(rows(
        &mut session,
        "SELECT count(v) FROM test.walk WHERE v > 5",
    ));
    assert_eq!(counted_over_index, [["4"]]);

    // A full covering SUM uses the unordered Global aggregate contract. Its
    // input is indexed in the pruned scan schema, so lowering must translate
    // that offset back through the table schema before reading index keys.
    let summed_over_index = displayed(rows(&mut session, "SELECT sum(v) FROM test.walk"));
    assert_eq!(summed_over_index, [["111"]]);
}

/// The REST of Go `fieldTypeFromPBColumn`: flag is not the only field the
/// decode needs.
///
/// `elems` decides what an ENUM/SET ordinal means, and `decimal` the scale a
/// DECIMAL and the fsp a TIME/DATETIME read back with. The scan path rebuilt
/// none of them, so this pins each against the point-get path, which builds
/// its types from the catalog and therefore never lost them. A disagreement
/// here is the same class of bug as the unsigned one: one table, two paths,
/// two answers.
#[test]
fn the_scan_path_decodes_elems_and_scale_like_the_point_get_path() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(37))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.tt (id int primary key, en enum('alpha','beta','gamma'), \
         st set('p','q','r'), dc decimal(12,4), yr year, tm time(3), dt datetime(6))",
    );
    rows(
        &mut session,
        "INSERT INTO test.tt VALUES (1, 'gamma', 'q,r', 12345.6789, 2024, \
         '12:34:56.789', '2024-03-04 05:06:07.891011')",
    );

    let columns = "en, st, dc, yr, tm, dt";
    // `id = 1` is a point get; `id > 0 AND id < 2` is a scan of the same row.
    let point_get = displayed(rows(
        &mut session,
        &format!("SELECT {columns} FROM test.tt WHERE id = 1"),
    ));
    let scanned = displayed(rows(
        &mut session,
        &format!("SELECT {columns} FROM test.tt WHERE id > 0 AND id < 2"),
    ));
    assert_eq!(point_get, scanned, "the two paths must read one row alike");
    assert_eq!(
        scanned,
        [[
            "gamma".to_owned(),
            "q,r".to_owned(),
            "12345.6789".to_owned(),
            "2024".to_owned(),
            "12:34:56.789".to_owned(),
            "2024-03-04 05:06:07.891011".to_owned(),
        ]],
        "and both must read what was stored"
    );
}

#[test]
fn global_statistics_and_session_bindings_match_go() {
    assert_global_statistics_and_session_bindings_match_go(1, 111);
}

#[test]
fn concurrent_global_statistics_and_session_bindings_match_go() {
    assert_global_statistics_and_session_bindings_match_go(2, 112);
}

/// Go `PBToExpr` decodes every signature the pushdown catalog admits
/// (`distsql_builtin.go` `getSignatureByPB`); CASE/IF/IFNULL pushed into a
/// coprocessor Selection were refused with 1105 before.
#[test]
fn pushed_conditional_signatures_evaluate_in_the_coprocessor_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut s = stack
        .factory
        .open_session(session_context(902))
        .expect("session");
    rows(
        &mut s,
        "CREATE TABLE test.pc (id INT PRIMARY KEY, a INT, b INT, d DATETIME, j JSON)",
    );
    s.node_conn()
        .execute_write(
            "INSERT INTO test.pc VALUES (1, 1, NULL, '2024-01-02 03:04:05', '\"x\"'), \
         (2, 5, 7, NULL, NULL), (3, NULL, 2, '2020-05-06 00:00:00', '\"yy\"')",
    )
    .expect("insert");
    let ids = |s: &mut crate::Session, sql: &str| {
        displayed(rows(s, sql)).into_iter().map(|row| row[0].clone()).collect::<Vec<_>>()
    };
    assert_eq!(ids(&mut s, "SELECT id FROM test.pc WHERE CASE WHEN a > 2 THEN a ELSE 0 END > 1 ORDER BY id"), ["2"]);
    assert_eq!(ids(&mut s, "SELECT id FROM test.pc WHERE IF(a IS NULL, b, a) = 2 ORDER BY id"), ["3"]);
    assert_eq!(ids(&mut s, "SELECT id FROM test.pc WHERE IFNULL(b, 0) < 3 ORDER BY id"), ["1", "3"]);
    assert_eq!(ids(&mut s, "SELECT id FROM test.pc WHERE CAST(d AS SIGNED) > 20210000000000 ORDER BY id"), ["1"]);
    assert_eq!(ids(&mut s, "SELECT id FROM test.pc WHERE IFNULL(CAST(j AS CHAR), 'n') = '\"yy\"' ORDER BY id"), ["3"]);
    assert_eq!(ids(&mut s, "SELECT id FROM test.pc WHERE IF(a > 2, 'yes', 'no') = 'yes' ORDER BY id"), ["2"]);
    assert_eq!(ids(&mut s, "SELECT id FROM test.pc WHERE CASE WHEN a > 2 THEN 'yes' ELSE 'no' END = 'yes' ORDER BY id"), ["2"]);
    // Go wraps non-integer conditions with typed IsTrueWithNull before
    // the control builtin reads EvalInt, so a nonzero fraction stays TRUE.
    assert_eq!(ids(&mut s, "SELECT id FROM test.pc WHERE CASE WHEN a / 10 THEN 1 ELSE 0 END = 1 ORDER BY id"), ["1", "2"]);
}

#[test]
fn generated_write_policy_reaches_cluster_storage_and_prepared_execution() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(990)).unwrap();
    rows(&mut session, "USE test");
    rows(&mut session, "SET sql_mode='STRICT_TRANS_TABLES'");
    rows(&mut session, "CREATE TABLE generated_policy (id INT PRIMARY KEY, a INT, b TINYINT AS (a) STORED, KEY idx(b))");
    let error = session
        .node_conn()
        .execute_write("INSERT INTO generated_policy(id,a) VALUES (1,1000)")
        .unwrap_err();
    assert_eq!(error.code, 1264);
    assert_eq!(
        displayed(rows(&mut session, "SELECT count(*) FROM generated_policy")),
        [["0"]]
    );
    rows(&mut session, "SET sql_mode=''");
    rows(
        &mut session,
        "INSERT INTO generated_policy(id,a) VALUES (1,1000)",
    );
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [[
            "Warning",
            "1264",
            "Out of range value for column 'b' at row 1"
        ]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT a,b FROM generated_policy FORCE INDEX(idx) WHERE b=127"
        )),
        [["1000", "127"]]
    );
    let prepared = session
        .node_conn().prepare_general("UPDATE generated_policy SET a=? WHERE id=1")
        .unwrap();
    rows(&mut session, "SET sql_mode='STRICT_TRANS_TABLES'");
    let error = session
        .node_conn().execute_general(
            &prepared,
            &[tidb_param::PreparedValue::SignedLongLong(2000)],
        )
        .err()
        .expect("prepared UPDATE uses its execution-time statement policy");
    assert_eq!(error.code, 1264);
    assert_eq!(
        displayed(rows(&mut session, "SELECT a,b FROM generated_policy")),
        [["1000", "127"]]
    );
    rows(&mut session, "SET sql_mode=''");
    session
        .node_conn().execute_general(
            &prepared,
            &[tidb_param::PreparedValue::SignedLongLong(2000)],
        )
        .unwrap();
    // Go answers a non-strict UPDATE's generated-column overflow with the
    // conversion's own warning, not INSERT's 1264: `castColumnValue` hands
    // `ErrOverflow` ("constant %v overflows %s", 1690) to
    // `TypeCtx.HandleTruncate`, which under TruncateAsWarning appends it and
    // returns nil, so `handleUpdateError`'s 1264 mapping never sees an
    // error. Captured from Go TiDB on this exact sequence:
    // `[[Warning 1690 constant 2000 overflows tinyint]]`.
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [["Warning", "1690", "constant 2000 overflows tinyint"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT a,b FROM generated_policy")),
        [["2000", "127"]]
    );
}

#[test]
fn binding_maintenance_preserves_usage_and_collects_tombstones_without_writes() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(128)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.binding_maintenance (id INT PRIMARY KEY, v INT, KEY iv(v))",
    );
    rows(
        &mut session,
        "INSERT INTO test.binding_maintenance VALUES (1,10)",
    );
    rows(&mut session, "CREATE GLOBAL BINDING FOR SELECT * FROM test.binding_maintenance WHERE v=10 USING SELECT * FROM test.binding_maintenance USE INDEX(iv) WHERE v=10");
    rows(
        &mut session,
        "SELECT * FROM test.binding_maintenance WHERE v=10",
    );
    let bindings = stack.factory.binding_handle().unwrap();
    let owner = bindings.cache().load();
    let binding = owner
        .get_all_bindings()
        .into_iter()
        .find(|b| b.usage_snapshot().last_used_at.is_some())
        .expect("matching records shared usage");
    let digest = binding.usage_snapshot().sql_digest;
    bindings.reload(false).unwrap();
    assert!(Arc::ptr_eq(&owner, &bindings.cache().load()));
    assert!(Arc::ptr_eq(&binding, &owner.get_binding(&digest).unwrap()));
    rows(&mut session, "SET GLOBAL tidb_enable_binding_usage=OFF");
    bindings.write_usage().unwrap();
    assert!(binding.usage_snapshot().last_saved_at.is_none());
    rows(&mut session, "SET GLOBAL tidb_enable_binding_usage=ON");
    bindings.write_usage().expect("usage storage transaction");
    assert!(binding.usage_snapshot().last_saved_at.is_some());
    assert_eq!(displayed(rows(&mut session, &format!("SELECT last_used_date IS NOT NULL FROM mysql.bind_info WHERE sql_digest='{digest}'"))), [["1"]]);
    rows(&mut session, "INSERT INTO mysql.bind_info (original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation, source, sql_digest) VALUES ('aged binding', 'SELECT 1', 'test', 'deleted', '2001-01-01 00:00:00', '2001-01-01 00:00:00', 'utf8mb4', 'utf8mb4_bin', 'manual', 'aged_binding_maintenance')");
    rows(&mut session, "INSERT INTO mysql.bind_info (original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation, source, sql_digest) VALUES ('young binding', 'SELECT 1', 'test', 'deleted', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'utf8mb4', 'utf8mb4_bin', 'manual', 'young_binding_maintenance')");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT COUNT(*) FROM mysql.bind_info WHERE sql_digest='aged_binding_maintenance'"
        )),
        [["1"]]
    );
    bindings.gc().expect("owner GC independent of writes");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT COUNT(*) FROM mysql.bind_info WHERE sql_digest='aged_binding_maintenance'"
        )),
        [["0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT COUNT(*) FROM mysql.bind_info WHERE sql_digest='young_binding_maintenance'"
        )),
        [["1"]]
    );
}

#[test]
fn deferred_uniqueness_batch_binary_prepared_dml_shares_abort_policy() {
    use tidb_param::PreparedValue;
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(944)).unwrap();
    rows(&mut session, "CREATE DATABASE deferred_binary");
    rows(&mut session, "USE deferred_binary");
    rows(&mut session, "CREATE TABLE t (id INT PRIMARY KEY)");
    rows(
        &mut session,
        "SET tidb_constraint_check_in_place_pessimistic=OFF",
    );
    let insert = session.node_conn().prepare_general("INSERT INTO t VALUES (?)").unwrap();
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    session
        .node_conn().execute_general(&insert, &[PreparedValue::SignedLongLong(1)])
        .unwrap();
    assert!(session
        .node_conn()
        .execute("SELECT missing FROM t")
        .is_err());
    assert!(
        session.in_transaction(),
        "ordinary query errors do not invoke DML abort policy"
    );
    let error = match session.node_conn().execute_general(&insert, &[PreparedValue::SignedLongLong(1)]) {
        Err(error) => error,
        Ok(_) => panic!("a local duplicate must fail"),
    };
    assert_eq!(error.code, 8147, "{error:?}");
    assert!(!session.in_transaction());
    assert!(session.node().txn.explicit().is_none());
    assert!(session.node().buffer.is_empty());
    assert!(rows(&mut session, "SELECT * FROM t").is_empty());
}

/// Go `validateCommentLength` and `checkTooBigFieldLengthAndTryAutoConvert`,
/// from `ddl/db.test`: strict mode refuses an over-long table (1628), field
/// (1629) or index (1688) comment; otherwise the statement warns and keeps
/// the first bytes. The warnings a plan-time build raises (ADD COLUMN, ADD
/// INDEX, MODIFY) reach `SHOW WARNINGS` as Go's do. A GLOBAL temporary
/// table's allocator starts at its `AUTO_INCREMENT` (Go
/// `NewAllocatorFromTempTblInfo`). This node accepted every long comment
/// silently, answered ALTER TABLE COMMENT with 1105, dropped plan-time
/// warnings and started the temporary table at 1.
#[test]
fn comment_lengths_and_plan_time_warnings_follow_the_sql_mode_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(995)).unwrap();
    let field = "b".repeat(1025);
    let table = "b".repeat(2049);
    rows(&mut session, "CREATE TABLE test.t (c INT, d INT, e INT)");
    for (sql, code, message) in [
        (
            format!("CREATE TABLE test.t1 (c INT) COMMENT = '{table}'"),
            1628,
            "Comment for table 't1' is too long (max = 2048)",
        ),
        (
            format!("CREATE TABLE test.t1 (c INT COMMENT '{field}')"),
            1629,
            "Comment for field 'c' is too long (max = 1024)",
        ),
        (
            format!("CREATE TABLE test.t1 (c INT, KEY (c) COMMENT '{field}')"),
            1688,
            "Comment for index 'c' is too long (max = 1024)",
        ),
        (
            format!("ALTER TABLE test.t ADD COLUMN c1 INT COMMENT '{field}'"),
            1629,
            "Comment for field 'c1' is too long (max = 1024)",
        ),
        (
            format!("ALTER TABLE test.t ADD KEY (e) COMMENT '{field}'"),
            1688,
            "Comment for index 'e' is too long (max = 1024)",
        ),
        (
            format!("ALTER TABLE test.t COMMENT = '{table}'"),
            1628,
            "Comment for table 't' is too long (max = 2048)",
        ),
    ] {
        let error = session.node_conn().execute_write(&sql).unwrap_err();
        assert_eq!((error.code, error.message.as_str()), (code, message));
    }
    rows(&mut session, "SET @@sql_mode = ''");
    for (sql, warning) in [
        (
            format!("CREATE TABLE test.t1 (c INT) COMMENT = '{table}'"),
            [
                "Warning",
                "1628",
                "Comment for table 't1' is too long (max = 2048)",
            ],
        ),
        (
            format!("ALTER TABLE test.t ADD COLUMN c1 INT COMMENT '{field}'"),
            [
                "Warning",
                "1629",
                "Comment for field 'c1' is too long (max = 1024)",
            ],
        ),
        (
            format!("CREATE INDEX i1 ON test.t (d) COMMENT '{field}'"),
            [
                "Warning",
                "1688",
                "Comment for index 'i1' is too long (max = 1024)",
            ],
        ),
        (
            "CREATE TABLE test.v (a VARCHAR(200))".to_owned(),
            ["", "", ""],
        ),
        (
            "ALTER TABLE test.v MODIFY a VARCHAR(70000000)".to_owned(),
            [
                "Warning",
                "1246",
                "Converting column 'a' from VARCHAR to TEXT",
            ],
        ),
    ] {
        session.node_conn().execute_write(&sql).unwrap();
        let warnings = displayed(rows(&mut session, "SHOW WARNINGS"));
        if warning[0].is_empty() {
            assert!(warnings.is_empty(), "{sql}: {warnings:?}");
        } else {
            assert_eq!(warnings, [warning], "{sql}");
        }
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT length(table_comment) FROM information_schema.tables \
             WHERE table_schema = 'test' AND table_name = 't1'"
        )),
        [["2048"]]
    );
    rows(
        &mut session,
        "CREATE GLOBAL TEMPORARY TABLE test.g (id INT PRIMARY KEY AUTO_INCREMENT) \
         AUTO_INCREMENT = 100 ON COMMIT DELETE ROWS",
    );
    rows(&mut session, "BEGIN");
    rows(&mut session, "INSERT INTO test.g VALUES (NULL)");
    assert_eq!(
        displayed(rows(&mut session, "SELECT id FROM test.g")),
        [["100"]]
    );
    rows(&mut session, "COMMIT");
}

/// Go `corColInFilter`: a reader whose pushed conditions read a correlated
/// column re-describes them on every `Open`, so each Apply iteration sends its
/// own binding. Constant propagation turns `c3 = 1 and c3 = t2.c3` into a
/// pushed `eq(t2.c3, 1)`; this node described it once, while the binding was
/// still NULL, and every probe counted nothing
/// (`planner/core/casetest/index/index`).
#[test]
fn a_pushed_correlated_condition_carries_each_iterations_binding() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1001)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.t1 (c1 INT, c2 INT, c3 INT, PRIMARY KEY (c1), KEY (c2))",
    );
    rows(
        &mut session,
        "INSERT INTO test.t1 VALUES (1, 1, 1), (2, 2, 2)",
    );
    rows(
        &mut session,
        "CREATE TABLE test.t2 (c1 INT, c2 INT, c3 INT)",
    );
    rows(
        &mut session,
        "INSERT INTO test.t2 VALUES (1, 1, 1), (2, 2, 2)",
    );
    for sql in [
        "SELECT (SELECT count(*) FROM test.t1 WHERE c2 = 1 AND c2 = t2.c3) FROM test.t2 ORDER BY c1",
        "SELECT (SELECT count(*) FROM test.t1 WHERE c3 = 1 AND c3 = t2.c3) FROM test.t2 ORDER BY c1",
        "SELECT (SELECT count(*) FROM test.t1 USE INDEX (PRIMARY) WHERE c3 = 1 AND c3 = t2.c3) \
         FROM test.t2 ORDER BY c1",
    ] {
        assert_eq!(displayed(rows(&mut session, sql)), [["1"], ["0"]], "{sql}");
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT * FROM test.t2 WHERE c1 < ALL(SELECT /*+ use_index_merge(t1) */ c1 FROM test.t1 \
             WHERE (c1 = 10 AND c1 = t2.c3 OR c2 = 1 AND c2 = t2.c3) AND reverse(c3)) ORDER BY c1"
        )),
        [["2", "2", "2"]]
    );
}

/// Go `GetMergeJoin` returns nil for ENUM/SET join keys before it reaches
/// `getEnforcedMergeJoin` (issues 24473/25669: the index orders an ENUM by
/// its ordinal, the merge compares it by name). The port refused only the
/// naturally ordered form, so a MERGE_JOIN hint built a merge join over the
/// ordinal-ordered index and dropped every row whose name order disagreed.
#[test]
fn merge_join_hint_does_not_merge_enum_or_set_keys() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1045)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    for column in [
        "ENUM('y','b','1','x','0','null')",
        "SET('y','b','1','x','0','null')",
    ] {
        session
            .node_conn()
            .execute_write("DROP TABLE IF EXISTS mjx")
            .unwrap();
        session
            .node_conn()
            .execute_write(&format!(
                "CREATE TABLE mjx (a {column} DEFAULT NULL, KEY a (a))"
            ))
            .unwrap();
        session
            .node_conn()
            .execute_write("INSERT INTO mjx VALUES ('x'), ('x'), ('b'), ('y')")
            .unwrap();
        let mut answer = displayed(rows(
            &mut session,
            "SELECT /*+ merge_join (t2,t3) */ t2.a, t3.a FROM mjx t2 INNER JOIN mjx t3 ON t2.a = t3.a",
        ));
        answer.sort();
        assert_eq!(
            answer,
            [
                ["b", "b"],
                ["x", "x"],
                ["x", "x"],
                ["x", "x"],
                ["x", "x"],
                ["y", "y"]
            ],
            "{column}"
        );
    }
}

/// Go's coprocessor TopN admits rows ONE at a time (a tie never evicts, so the
/// earliest rows survive), lays survivors out by heap push/fix, and orders
/// them with its pdqsort. The cop-layer executor instead loaded the whole
/// child chunk, heapified and popped, which kept different tied rows and
/// printed them in a different order (`[1, 8, 3]` for Go's `[5, 1, 3]`); both
/// final sorts were stable where Go's pdqsort is not beyond 12 rows.
#[test]
fn cop_top_n_ties_follow_go_heap_layout_and_pdqsort() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1057)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE tn (a INT, b CHAR)")
        .unwrap();
    session
        .node_conn()
        .execute_write(
            "INSERT INTO tn VALUES (1,'s'),(2,'b'),(1,'c'),(2,'e'),(1,'a'),(1,'x'),(1,'y'),(1,'z')",
        )
        .unwrap();
    let ids = |session: &mut Session, sql: &str| -> Vec<String> {
        displayed(rows(session, sql))
            .into_iter()
            .map(|row| row[0].clone())
            .collect()
    };
    // Expected: Go 1.27 on this branch (`TestZZ` probes).
    for (sql, expected) in [
        (
            "SELECT _tidb_rowid FROM tn ORDER BY a LIMIT 3",
            vec!["5", "1", "3"],
        ),
        (
            "SELECT _tidb_rowid FROM tn ORDER BY a LIMIT 2",
            vec!["3", "1"],
        ),
        (
            "SELECT _tidb_rowid FROM tn ORDER BY a LIMIT 4",
            vec!["6", "5", "3", "1"],
        ),
        (
            "SELECT _tidb_rowid FROM tn ORDER BY a LIMIT 5",
            vec!["7", "6", "3", "1", "5"],
        ),
        (
            "SELECT _tidb_rowid FROM tn ORDER BY a LIMIT 6",
            vec!["8", "7", "3", "1", "5", "6"],
        ),
        (
            "SELECT _tidb_rowid FROM tn WHERE a = 1 ORDER BY a LIMIT 3",
            vec!["1", "3", "5"],
        ),
    ] {
        assert_eq!(ids(&mut session, sql), expected, "{sql}");
    }
    // Beyond twelve survivors the order is pdqsort's, not a stable sort's.
    session
        .node_conn()
        .execute_write("CREATE TABLE tm (a INT, b CHAR)")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO tm VALUES (1,'s'),(2,'b'),(1,'c'),(2,'e'),(1,'a')")
        .unwrap();
    for _ in 0..3 {
        session
            .node_conn()
            .execute_write("INSERT INTO tm SELECT * FROM tm")
            .unwrap();
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT b, COUNT(*) FROM (SELECT b FROM tm ORDER BY a LIMIT 20 OFFSET 2) s \
             GROUP BY b ORDER BY b",
        )),
        [["a", "6"], ["c", "7"], ["s", "7"]]
    );
}

/// Go `ResetContextOfStmt` unwraps an EXPLAIN before its kind switch, so an
/// EXPLAINed DML is still a DML to `checkStableResultMode`: with
/// `tidb_enable_ordered_result_mode` on, its read keeps no order (the
/// ResultReorder rule is not applied), as the DML itself would.
#[test]
fn explained_dml_is_exempt_from_ordered_result_mode() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1107)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE teo (a INT PRIMARY KEY, b INT, c INT, KEY(b))")
        .unwrap();
    session
        .node_conn()
        .execute_write("SET tidb_enable_ordered_result_mode = 1")
        .unwrap();
    for sql in [
        "EXPLAIN FORMAT='verbose' UPDATE teo SET a = a + 1",
        "EXPLAIN FORMAT='verbose' INSERT INTO teo SELECT * FROM teo",
    ] {
        let plan = displayed(rows(&mut session, sql));
        let scan = plan
            .iter()
            .find(|row| row[0].contains("TableFullScan"))
            .unwrap_or_else(|| panic!("{sql}: {plan:?}"));
        assert!(scan[5].contains("keep order:false"), "{sql}: {scan:?}");
    }
}

/// Go summarizes an EXECUTE under the digest of the text as PREPAREd
/// (`select ?`); the port ran a marker-aliased copy (`select ? as `?``) and
/// summarized that (`TestStmtSummaryPreparedStatements`).
#[test]
fn prepared_execute_is_summarized_under_the_prepared_text() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1109)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("SET GLOBAL tidb_enable_stmt_summary = 1")
        .unwrap();
    session
        .node_conn()
        .execute_write("PREPARE stmt FROM 'select ?'")
        .unwrap();
    session
        .node_conn()
        .execute_write("SET @number = 1")
        .unwrap();
    rows(&mut session, "EXECUTE stmt USING @number");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT exec_count FROM information_schema.statements_summary WHERE digest_text LIKE 'select ?'",
        )),
        [["1"]]
    );
    session
        .node_conn()
        .execute_write("SET GLOBAL tidb_enable_stmt_summary = DEFAULT")
        .unwrap();
}

/// Go `buildExplain`: `EXPLAIN '<plan digest>'` explains the statement
/// summary's sample of that digest with its plan's hints
/// (`getHintedStmtThroughPlanDigest`), and a digest it has no record of is
/// `can't find any plans for '<digest>'` (captured with `./gorun`).
#[test]
fn explain_plan_digest_explains_the_hinted_summary_statement() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1112)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("SET GLOBAL tidb_enable_stmt_summary = 1")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE b(id INT PRIMARY KEY, y INT, z INT, KEY(y))")
        .unwrap();
    let operators = |rows: Vec<Vec<String>>| -> Vec<String> {
        rows.into_iter().map(|row| row[0].clone()).collect()
    };
    let plain = operators(displayed(rows(&mut session, "explain select * from b where y = 3")));
    rows(&mut session, "select * from b where y = 3");
    let digest = displayed(rows(
        &mut session,
        "SELECT plan_digest FROM information_schema.statements_summary \
         WHERE digest_text = 'select * from `b` where `y` = ?'",
    ));
    let digest = &digest[0][0];
    assert_eq!(
        operators(displayed(rows(&mut session, &format!("explain '{digest}'")))),
        plain
    );
    let error = match session.node_conn().execute("explain 'ffff'") {
        Ok(_) => panic!("an unrecorded digest has no plan"),
        Err(error) => error,
    };
    assert_eq!(error.message, "can't find any plans for 'ffff'");
}

/// Go keys a statement-summary row by the plan digest: the sha256 of the
/// normalized select plan (`NormalizeFlatPlan`). The expected digests below
/// are what Go 1.27 on this branch records for the same statements, so the
/// normalization (constants as `?`, no ranges or statistics, children by plan
/// type, casts as `cast(arg, type)`) has to match byte for byte.
#[test]
fn statement_summary_plan_digests_match_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1111)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("SET GLOBAL tidb_enable_stmt_summary = 1")
        .unwrap();
    for ddl in [
        "CREATE TABLE a(id INT PRIMARY KEY, x INT, y INT)",
        "CREATE TABLE b(id INT PRIMARY KEY, y INT, z INT, KEY(y))",
        "CREATE TABLE c(id INT PRIMARY KEY, z INT)",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    for sql in [
        "select * from a where id = 1",
        "select * from a where id in (1,2,3)",
        "select * from a where x > 5",
        "select x, count(*) from a group by x",
        "select * from a join b on a.id=b.id",
        "select * from a join b on a.x=b.y where a.y > 3",
        "select * from b where y = 3",
        "select y from b where y > 3",
        "select * from a order by x limit 5",
        "select * from a where x in (1,2,3) and y = 2",
        "select /*+ merge_join(a,b) */ * from a join b on a.id=b.id",
        "select /*+ inl_join(b) */ * from a join b on a.x = b.id",
        "select * from a left join b on a.x=b.id",
        "insert into a select * from b",
        "update a set x=1 where y=2",
        "delete from a where id=3",
        "select sum(x), max(y) from a where y < 10",
        "select * from a union all select * from a",
        "select * from (select * from a where x>1) t where y<2",
        "select a.x, sum(b.z) from a join b on a.x = b.id group by a.x",
        "select * from a where id = 1 or id = 2",
        "select x from a where y = 1 and x = 2 and id > 3",
        "select count(*) from a",
        "select * from b where y in (1,2,3,4,5)",
        "select * from b where y in (1,2)",
    ] {
        if sql.starts_with("select") {
            rows(&mut session, sql);
        } else {
            session.node_conn().execute_write(sql).unwrap();
        }
    }
    let recorded: std::collections::BTreeMap<String, String> = displayed(rows(
        &mut session,
        "SELECT digest_text, plan_digest FROM information_schema.statements_summary \
         WHERE schema_name = 'test' AND digest_text NOT LIKE '%statements_summary%' \
         AND digest_text NOT LIKE 'set %' AND digest_text NOT LIKE 'drop%' \
         AND digest_text NOT LIKE 'create%'",
    ))
    .into_iter()
    .map(|row| (row[0].clone(), row[1].clone()))
    .collect();
    let expected: [(&str, &str); 23] = [
        (
            "delete from `a` where `id` = ?",
            "d29970167a003b98ca8a6f2e0daa0f807bf4f5ff5281dc96c28072e66a005520",
        ),
        (
            "insert into `a` select * from `b`",
            "b1aae3f2a4e07761a960a05c09c85e92d6bfa24d1152aee6e8f6d0b8791fe0bf",
        ),
        (
            "select * from ( select * from `a` where `x` > ? ) `t` where `y` < ?",
            "3273be776060fd8de6854902b84505c0a93b636e33d05b1e2654d6824b4120f5",
        ),
        (
            "select * from `a` join `b` on `a` . `id` = `b` . `id`",
            "0e1e7baf142e077d27a48d405e83fba1813fb6bf368841cfdeb65103737ecfa9",
        ),
        (
            "select * from `a` join `b` on `a` . `x` = `b` . `id`",
            "dfdf2e6d50fccfde4254aba057430f6915f4b8d34e331004ad50c8d9c15ef7db",
        ),
        (
            "select * from `a` join `b` on `a` . `x` = `b` . `y` where `a` . `y` > ?",
            "dd97e4de0b662db2162372f0771c79ae0790d5eaa208bd7582cd847f9427eb9a",
        ),
        (
            "select * from `a` left join `b` on `a` . `x` = `b` . `id`",
            "6cfb251e6cd76579b9580f81774b14f96840c31341976137c358bae9913edeb4",
        ),
        (
            "select * from `a` order by `x` limit ?",
            "1067bf004a3e0c7d495f241d6ba34dfb0536c1a0e2174991bb51731e2e8dab86",
        ),
        (
            "select * from `a` union all select * from `a`",
            "52b9478f1a9ef9515ee3d887f91906e4c1babb1bb492062dc4868ad48fb29308",
        ),
        (
            "select * from `a` where `id` = ?",
            "d29970167a003b98ca8a6f2e0daa0f807bf4f5ff5281dc96c28072e66a005520",
        ),
        (
            "select * from `a` where `id` = ? or `id` = ?",
            "014262c6f5bf53dd777929675ecf581b0aaed890b1816a2694e26e2649a4b623",
        ),
        (
            "select * from `a` where `id` in ( ... )",
            "014262c6f5bf53dd777929675ecf581b0aaed890b1816a2694e26e2649a4b623",
        ),
        (
            "select * from `a` where `x` > ?",
            "cf192d061522e508cd53b9d808c92b98bb0e8d2b93e70cfce94f00762c2ff934",
        ),
        (
            "select * from `a` where `x` in ( ... ) and `y` = ?",
            "83088a56b3604f0c1be1c19dd53d73b4b25ce2ffb40e7e565b530290e0c09ebd",
        ),
        (
            "select * from `b` where `y` = ?",
            "034a2651f373598c1085024d7e26a83d9f25cac34059d9cb9ae7cbd85c03bbc1",
        ),
        (
            "select * from `b` where `y` in ( ... )",
            "034a2651f373598c1085024d7e26a83d9f25cac34059d9cb9ae7cbd85c03bbc1",
        ),
        (
            "select `a` . `x` , sum ( `b` . `z` ) from `a` join `b` on `a` . `x` = `b` . `id` group by `a` . `x`",
            "cbf0060b5b52e0fb7ad74523a31c9739f0695e785722514a95f315e04637687b",
        ),
        (
            "select `x` , count ( ? ) from `a` group by `x`",
            "7e7b9248c5c70eaa7ecbc3a4677aafe4604c242e9d31d5e0952aeeb2426435c2",
        ),
        (
            "select `x` from `a` where `y` = ? and `x` = ? and `id` > ?",
            "84a2beb6b1d5300acdddcb8c4c56feae6c05689a8c5ef82d3ad05b4cb4fd98ee",
        ),
        (
            "select `y` from `b` where `y` > ?",
            "3da471c073d10289b7157ef67bb3c857bba94a88dd172aff63880d597718fa82",
        ),
        (
            "select count ( ? ) from `a`",
            "99c64d1aeb0cc1b601f09cc860790095f2dcb28f30b2242d3d2124bad53d8be9",
        ),
        (
            "select sum ( `x` ) , max ( `y` ) from `a` where `y` < ?",
            "7dfbcadf84bb9d1cd3251022e81c8ff61ba3c33265ec0132b0b85d0ad5e70a35",
        ),
        (
            "update `a` set `x` = ? where `y` = ?",
            "2cf881ae462c644791a5060e3d943ca47a14a58334581d0791f302411d979ef5",
        )
    ];
    for (text, digest) in expected {
        assert_eq!(
            recorded.get(text).map(String::as_str),
            Some(digest),
            "{text}"
        );
    }
    session
        .node_conn()
        .execute_write("SET GLOBAL tidb_enable_stmt_summary = DEFAULT")
        .unwrap();
}

/// More operator shapes than `statement_summary_plan_digests_match_go`: window
/// and Shuffle, TopN, aggregates, semi-join rewrites, apply, CTE, unique-index
/// point gets and in-list collapsing. Expected digests are the Go oracle's,
/// recorded under the replay's session setup (`tidb_hash_join_concurrency=1`).
#[test]
fn statement_summary_plan_digests_match_go_for_more_shapes() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1114)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("SET @@tidb_hash_join_concurrency = 1")
        .unwrap();
    session
        .node_conn()
        .execute_write("SET GLOBAL tidb_enable_stmt_summary = 1")
        .unwrap();
    for ddl in [
        "CREATE TABLE a(id INT PRIMARY KEY, x INT, y INT, KEY(x), KEY(y))",
        "CREATE TABLE b(id INT PRIMARY KEY, y INT, z INT, KEY(y))",
        "CREATE TABLE c(id INT PRIMARY KEY, z INT)",
        "CREATE TABLE u(id INT, k INT, v VARCHAR(20), UNIQUE KEY uk(k), KEY(v))",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    for sql in [
        "select * from u where k = 1",
        "select * from u where k in (1,2,3)",
        "select * from u where v = 'a'",
        "select * from u where v like 'a%'",
        "select id, row_number() over (partition by x order by y) from a",
        "select x, y from a order by x desc limit 3",
        "select * from a where x = 1 or y = 2",
        "select /*+ use_index_merge(a) */ * from a where x = 1 or y = 2",
        "select x, count(distinct y) from a group by x",
        "select /*+ stream_agg() */ x, sum(y) from a group by x",
        "select * from a where id in (select id from b)",
        "select * from a where id not in (select id from b)",
        "select * from a where exists (select 1 from b where b.id = a.id)",
        "select * from a where x > (select avg(z) from b)",
        "with t as (select x, y from a where y > 1) select * from t join b on t.x = b.id",
        "select distinct x from a",
        "select x + y, abs(x) from a where y between 1 and 10",
        "select * from a where x in (1,2,3,4,5,6,7,8)",
        "select * from a where x in (1,2)",
        "select * from a where id = 1 and x = 2",
        "select * from a where x is null",
        "select * from a where x = y",
        "select a.*, b.z from a join b on a.id = b.id join c on b.id = c.id",
        "select * from a join b on a.x = b.y join c on b.z = c.id where a.y > 1",
        "select /*+ hash_join(a, b) */ * from a join b on a.x = b.y",
        "select * from a right join b on a.id = b.id",
        "select * from a where id > 5 order by id limit 2",
        "select * from a order by x limit 5 offset 2",
        "select max(x), min(y) from a",
        "select * from a union select * from a",
        "select count(*) from a where x > 1 group by y having count(*) > 1",
        "select x from a where x = 1 and y in (select y from b where z = 3)",
    ] {
        rows(&mut session, sql);
    }
    let recorded: std::collections::BTreeMap<String, String> = displayed(rows(
        &mut session,
        "SELECT digest_text, plan_digest FROM information_schema.statements_summary \
         WHERE schema_name = 'test' AND digest_text NOT LIKE '%statements_summary%' \
         AND digest_text NOT LIKE 'set %' AND digest_text NOT LIKE 'drop%' \
         AND digest_text NOT LIKE 'create%'",
    ))
    .into_iter()
    .map(|row| (row[0].clone(), row[1].clone()))
    .collect();
    let expected: [(&str, &str); 30] = [
        (
            "select * from `a` join `b` on `a` . `x` = `b` . `y`",
            "9422e139f8cb8589192103534e1e6b7513fdb528bd83699e698657939ff6cd02",
        ),
        (
            "select * from `a` join `b` on `a` . `x` = `b` . `y` join `c` on `b` . `z` = `c` . `id` where `a` . `y` > ?",
            "c3c1da9ec3073353f92673b3ab1ff31baab954c50fc75814d3ef8e49288d6bf7",
        ),
        (
            "select * from `a` order by `x` limit ? offset ?",
            "aa7c930b8637b9589808d46b5c9ce524a4c0e9dc7e74f37b10e9f01736c04277",
        ),
        (
            "select * from `a` right join `b` on `a` . `id` = `b` . `id`",
            "76c89b9f77f013ca7de0de19a7751008715a384774c35d909e5a9f53d7ff1bc4",
        ),
        (
            "select * from `a` union select * from `a`",
            "9457d36f71476a29dac88288c79fbe278a9c265697092e04b7742f1437703b99",
        ),
        (
            "select * from `a` where `id` = ? and `x` = ?",
            "73937a629211beecade7391365368dbae3d44cc9dba7f53a398f0a5bb700f5e9",
        ),
        (
            "select * from `a` where `id` > ? order by `id` limit ?",
            "0c92331e892af0dfa50609875e3a5be6f7082541ce234cf295c484723078562b",
        ),
        (
            "select * from `a` where `id` in ( select `id` from `b` )",
            "0e1e7baf142e077d27a48d405e83fba1813fb6bf368841cfdeb65103737ecfa9",
        ),
        (
            "select * from `a` where `id` not in ( select `id` from `b` )",
            "9e0ede8bc4e81b6419df9f23b5fb65c4d1d20ed4eeddb99e44235f4c7c3aaeec",
        ),
        (
            "select * from `a` where `x` = ? or `y` = ?",
            "83d9e13aa02429530a794d7501b4aee4ffd0489c651eb2241d3df9e6b2d1d176",
        ),
        (
            "select * from `a` where `x` = `y`",
            "827fa0ddfdb6da9011b78e3562dfa97d75121112e5d9e020864424d3ba866607",
        ),
        (
            "select * from `a` where `x` > ( select avg ( `z` ) from `b` )",
            "04500b49f759e055d5625ad5c4f472f1a4b5c98d9d187400d2aae27d80b5bbd9",
        ),
        (
            "select * from `a` where `x` in ( ... )",
            "eff50ddd607cd7f575c486db304b1f63e65767db109024ea8561b00936a14317",
        ),
        (
            "select * from `a` where `x` is ?",
            "eff50ddd607cd7f575c486db304b1f63e65767db109024ea8561b00936a14317",
        ),
        (
            "select * from `a` where exists ( select ? from `b` where `b` . `id` = `a` . `id` )",
            "c1197fa02e2d5137b3eb5d9b60937668235224282121c7b810e5b7432342b199",
        ),
        (
            "select * from `u` where `k` = ?",
            "29ff78a0ff02e393c46a1134a52a665f9ce7231719e3b0bdf7a0d10945791661",
        ),
        (
            "select * from `u` where `k` in ( ... )",
            "23b7d175a9eb2de45a422305816f197a8d786b3e998d5644844887f7e4b81534",
        ),
        (
            "select * from `u` where `v` = ?",
            "75babbf1e7a60c27a17d0d5b91af7fe8ab5c7fd63eaf416ec81a81e7f52a8543",
        ),
        (
            "select * from `u` where `v` like ?",
            "f98956390255d7c8956504748cebf67e79a106eef191142abac58e4c235d2877",
        ),
        (
            "select `a` . * , `b` . `z` from `a` join `b` on `a` . `id` = `b` . `id` join `c` on `b` . `id` = `c` . `id`",
            "1297ae489c76eb4abc807eddb49a48161e773ea24ff48ace1191d467e0b6f4d7",
        ),
        (
            "select `id` , `row_number` ( ) `over` ( partition by `x` order by `y` ) from `a`",
            "c3ab2025bc7e7467f794ba33577ba173b8173268701bba1fcc971f573dbf4d58",
        ),
        (
            "select `x` + `y` , `abs` ( `x` ) from `a` where `y` between ? and ?",
            "b25f5fbd41ce96ba4c8f158836787f68fc6cac1abad3537d981f6e77c837e94d",
        ),
        (
            "select `x` , `y` from `a` order by `x` desc limit ?",
            "50529e8ff3c19402089a106189d1964052b89078992a617f528106b7ead33818",
        ),
        (
            "select `x` , count ( distinct `y` ) from `a` group by `x`",
            "e6362a0035c238cf0862091d697b6ba96457074ab7cd731904fab7437d2630cc",
        ),
        (
            "select `x` , sum ( `y` ) from `a` group by `x`",
            "235573aa0e74336de87fe3b3b98e601f3c89939d34c78da0f81bd6dc180e370b",
        ),
        (
            "select `x` from `a` where `x` = ? and `y` in ( select `y` from `b` where `z` = ? )",
            "6e118d7bc99503a32f679dd6027d7853057856fee1dbc3328d3575df43c02427",
        ),
        (
            "select count ( ? ) from `a` where `x` > ? group by `y` having count ( ? ) > ?",
            "7e6637b5bc2f3412427d21fda3a187866e7ec58eff40c39b65d0a950a74ebf98",
        ),
        (
            "select distinct `x` from `a`",
            "1cb38efe73f80d5bc368f7fb165b838c01361bcc3093827c0401d291a2c6f7db",
        ),
        (
            "select max ( `x` ) , min ( `y` ) from `a`",
            "bc3ae90d6c3a5cee3cf518d5bd81e7a0096506588c76a1ad2f6be97770983075",
        ),
        (
            "with `t` as ( select `x` , `y` from `a` where `y` > ? ) select * from `t` join `b` on `t` . `x` = `b` . `id`",
            "47c44b86fea9d9ae5e725900bd99ba2eb80ec49b4a758d7d85ee70bc54fabed4",
        )
    ];
    for (text, digest) in expected {
        assert_eq!(
            recorded.get(text).map(String::as_str),
            Some(digest),
            "{text}"
        );
    }
    session
        .node_conn()
        .execute_write("SET GLOBAL tidb_enable_stmt_summary = DEFAULT")
        .unwrap();
}

/// Go runs `checkLockClauseTables` in `Preprocess`, before `tryPointGetPlan`:
/// an `OF` target naming an aliased table by its base name is accepted with
/// warning 1105, the alias itself is silent, and an unknown target is error
/// 1109 -- on the autocommit point-get fast path as much as in the planner.
/// Expected values: Go 1.27 on this branch (`TestZZ` probe).
#[test]
fn for_update_of_checks_its_targets_on_the_point_get_fast_path() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1114)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE tfo (id INT PRIMARY KEY, a INT)")
        .unwrap();
    let warnings =
        |session: &mut Session| -> Vec<Vec<String>> { displayed(rows(session, "SHOW WARNINGS")) };
    rows(
        &mut session,
        "SELECT ue.id FROM tfo ue WHERE ue.id = 1 FOR UPDATE OF tfo",
    );
    assert_eq!(
        warnings(&mut session),
        [[
            "Warning",
            "1105",
            "FOR UPDATE OF references the base table name while the table is aliased. Use the alias 'ue' in OF to make the lock target explicit."
        ]]
    );
    rows(
        &mut session,
        "SELECT ue.id FROM tfo ue WHERE ue.id = 1 FOR UPDATE OF ue",
    );
    assert!(warnings(&mut session).is_empty());
    let error = session
        .node_conn()
        .execute("SELECT ue.id FROM tfo ue WHERE ue.id = 1 FOR UPDATE OF zz")
        .err()
        .map(|error| format!("{error:?}"))
        .unwrap_or_default();
    assert!(
        error.contains("Unknown table 'zz' in locking clause"),
        "an unresolvable OF target is error 1109, got {error}"
    );
}

/// Go charges a plan by the sum of every operator's `MemoryUsage()` and the
/// plan cache refuses one above `tidb_plan_cache_max_plan_size`
/// (`plan_cacheable_checker.go:588`). The sizes below were measured on Go 1.27
/// of this branch for each statement over `t(a int, b int, c varchar(20))` and
/// `ti(id int primary key, k int, k2 int, v int, key(k), key(k2))`; a plan
/// under 3% above Go's size must still be cached and one 3% below must be
/// refused, which fails if any operator's own terms go missing (the partition
/// info Go holds in the reader and the scan, the scan's filter conditions, the
/// properties, the names).
#[test]
fn plan_cache_admission_follows_go_plan_sizes() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1116)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE t (a INT, b INT, c VARCHAR(20))")
        .unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE ti (id INT PRIMARY KEY, k INT, k2 INT, v INT, KEY(k), KEY(k2))",
        )
        .unwrap();
    session
        .node_conn()
        .execute_write("SET @@tidb_hash_join_concurrency = 1")
        .unwrap();
    let cases: [(&str, i64); 15] = [
        ("select * from t where a=1", 13_753),
        ("select a from t", 8_406),
        ("select * from t where a<10 order by b limit 5", 15_385),
        ("select a, count(*) from t group by a", 12_488),
        ("select * from t order by a", 9_245),
        ("select * from t limit 3", 9_144),
        ("select max(a) from t", 16_911),
        ("select k from ti where k=1", 8_056),
        ("select * from ti where k=1", 12_892),
        (
            "select /*+ use_index_merge(ti) */ * from ti where k=1 or k2=2",
            36_936,
        ),
        ("select a from t union all select b from t", 17_523),
        ("select * from ti where k>2 and v<5", 19_328),
        ("select distinct a from t", 10_385),
        ("select * from t where a in (select k from ti)", 32_745),
        (
            "select * from t t1 where exists (select 1 from ti where ti.k=t1.a)",
            25_883,
        ),
    ];
    let cached_at = |session: &mut Session, sql: &str, max: i64| -> bool {
        session
            .node_conn()
            .execute_write(&format!("SET @@tidb_plan_cache_max_plan_size = {max}"))
            .unwrap();
        session
            .node_conn()
            .execute_write(&format!("PREPARE sz FROM '{sql}'"))
            .unwrap();
        rows(session, "EXECUTE sz");
        rows(session, "EXECUTE sz");
        let cached = displayed(rows(session, "SELECT @@last_plan_from_cache"))[0][0] == "1";
        rows(session, "DEALLOCATE PREPARE sz");
        cached
    };
    for (sql, go_size) in cases {
        let above = go_size + go_size * 3 / 100;
        let below = go_size - go_size * 3 / 100;
        assert!(
            cached_at(&mut session, sql, above),
            "{sql}: a plan Go sizes at {go_size} must fit under {above}"
        );
        assert!(
            !cached_at(&mut session, sql, below),
            "{sql}: a plan Go sizes at {go_size} must not fit under {below}"
        );
    }
}

/// `TestPreparedPlanCacheLargePlan`: fifteen UNION ALL legs (1,289,867 bytes
/// to Go) fit the 2 MiB default, thirty (2,579,567) do not until the limit is
/// raised.
#[test]
fn plan_cache_refuses_the_thirty_leg_union_by_default() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1117)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE tu (a INT, b INT, c VARCHAR(2048))")
        .unwrap();
    let leg = "select * from tu, tu t1 where t1.c=space(2048) and tu.c=space(2048) and tu.a=t1.b";
    let union = |legs: usize| vec![leg; legs].join(" union all ");
    let cached = |session: &mut Session, legs: usize| -> bool {
        session
            .node_conn()
            .execute_write(&format!("PREPARE big FROM '{}'", union(legs)))
            .unwrap();
        rows(session, "EXECUTE big");
        rows(session, "EXECUTE big");
        let cached = displayed(rows(session, "SELECT @@last_plan_from_cache"))[0][0] == "1";
        rows(session, "DEALLOCATE PREPARE big");
        cached
    };
    assert!(cached(&mut session, 15));
    assert!(!cached(&mut session, 30));
    session
        .node_conn()
        .execute_write("SET @@tidb_plan_cache_max_plan_size = 1073741824")
        .unwrap();
    assert!(cached(&mut session, 30));
}

/// Go `ScalarSubQueryExpr` (`tidb_opt_enable_non_eval_scalar_subquery`):
/// plain EXPLAIN registers an uncorrelated scalar / EXISTS subquery without
/// running it and plans the outer query over `ScalarQueryCol#N`
/// placeholders. `s` holds two rows, so running `(SELECT a FROM s)` fails;
/// EXPLAIN ANALYZE still runs it. A Projection prints the placeholder's
/// embedded NULL constant. Expected rows: gorun on this branch.
#[test]
fn non_eval_scalar_subquery_explains_placeholders_as_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(2009)).unwrap();
    for sql in [
        "USE test",
        "CREATE TABLE t (a INT, b INT, KEY(a))",
        "CREATE TABLE s (a INT, b VARCHAR(10))",
        "INSERT INTO s VALUES (1, 'x'), (2, 'y')",
    ] {
        session.node_conn().execute_write(sql).unwrap();
    }
    let plan = |session: &mut Session, sql: &str| -> Vec<String> {
        displayed(rows(session, &format!("EXPLAIN {sql}")))
            .into_iter()
            .map(|row| row.join("|"))
            .collect()
    };
    let scalar = "SELECT * FROM t WHERE a > (SELECT a FROM s)";
    assert_eq!(query_error(&mut session, &format!("EXPLAIN {scalar}")).0, 1242);

    session
        .node_conn()
        .execute_write("SET tidb_opt_enable_non_eval_scalar_subquery = ON")
        .unwrap();
    for (sql, expected) in [
        (
            scalar,
            vec![
                "Selection_13|8000.00|root||gt(test.t.a, ScalarQueryCol#9)",
                "└─TableReader_15|10000.00|root||data:TableFullScan_14",
                "  └─TableFullScan_14|10000.00|cop[tikv]|table:t|keep order:false, stats:pseudo",
                "ScalarSubQuery_10|N/A|root||Output: ScalarQueryCol#9",
                "└─MaxOneRow_6|1.00|root||",
                "  └─TableReader_9|2.00|root||data:TableFullScan_8",
                "    └─TableFullScan_8|2.00|cop[tikv]|table:s|keep order:false, stats:pseudo",
            ],
        ),
        (
            "SELECT * FROM t WHERE NOT EXISTS (SELECT a FROM s)",
            vec![
                "Selection_10|8000.00|root||not(istrue_with_null(ScalarQueryCol#9))",
                "└─TableReader_12|10000.00|root||data:TableFullScan_11",
                "  └─TableFullScan_11|10000.00|cop[tikv]|table:t|keep order:false, stats:pseudo",
                "ScalarSubQuery_7|N/A|root||Output: ScalarQueryCol#9",
                "└─TableReader_6|10000.00|root||data:TableFullScan_5",
                "  └─TableFullScan_5|10000.00|cop[tikv]|table:s|keep order:false, stats:pseudo",
            ],
        ),
        (
            "SELECT (SELECT a, b FROM s) = (1, 'x') FROM t",
            vec![
                "Projection_12|10000.00|root||and(eq(<nil>, 1), eq(<nil>, x))->Column#15",
                "└─IndexReader_16|10000.00|root||index:IndexFullScan_15",
                "  └─IndexFullScan_15|10000.00|cop[tikv]|table:t, index:a(a)|keep order:false, stats:pseudo",
                "ScalarSubQuery_11|N/A|root||Output: ScalarQueryCol#13, ScalarQueryCol#14",
                "└─MaxOneRow_7|1.00|root||",
                "  └─TableReader_10|2.00|root||data:TableFullScan_9",
                "    └─TableFullScan_9|2.00|cop[tikv]|table:s|keep order:false, stats:pseudo",
            ],
        ),
        (
            "SELECT (SELECT b FROM s) FROM t",
            vec![
                "Projection_12|10000.00|root||<nil>->Column#14",
                "└─IndexReader_16|10000.00|root||index:IndexFullScan_15",
                "  └─IndexFullScan_15|10000.00|cop[tikv]|table:t, index:a(a)|keep order:false, stats:pseudo",
                "ScalarSubQuery_11|N/A|root||Output: ScalarQueryCol#13",
                "└─MaxOneRow_7|1.00|root||",
                "  └─TableReader_10|2.00|root||data:TableFullScan_9",
                "    └─TableFullScan_9|2.00|cop[tikv]|table:s|keep order:false, stats:pseudo",
            ],
        ),
    ] {
        assert_eq!(plan(&mut session, sql), expected, "{sql}");
    }
    assert_eq!(
        query_error(&mut session, &format!("EXPLAIN ANALYZE {scalar}")).0,
        1242
    );
    // A row comparison keeps each side where it was written, and a width
    // mismatch reports the LEFT side's width.
    for (sql, expected) in [
        ("SELECT (SELECT a, b FROM s WHERE a = 1) < (2, 'a')", "1"),
        ("SELECT (2, 'a') < (SELECT a, b FROM s WHERE a = 1)", "0"),
    ] {
        assert_eq!(displayed(rows(&mut session, sql)), [[expected]], "{sql}");
    }
    for (sql, width) in [("SELECT 3 = (1, 2)", 1), ("SELECT (1, 2) = 3", 2)] {
        assert_eq!(
            query_error(&mut session, sql),
            (1241, format!("Operand should contain {width} column(s)")),
            "{sql}"
        );
    }
}

/// Join candidates spend the plan ids Go's constructors do even when a hint or
/// a refused child property drops them afterwards: the hash joins are built
/// before a `MERGE_JOIN` hint returns the merge joins, a skipped hash join and
/// a vetoed natural merge join are built then discarded, and an index join's
/// four variants are all built. Expected rows: Go 1.27 on this branch
/// (`TestZZ` probe `HJ1`).
#[test]
fn join_hints_spend_the_plan_ids_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1126)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    for ddl in [
        "CREATE TABLE tja (id INT PRIMARY KEY, x INT, y INT, KEY(x), KEY(y))",
        "CREATE TABLE tjb (id INT PRIMARY KEY, y INT, z INT, KEY(y))",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    let plan = |session: &mut Session, sql: &str| -> Vec<String> {
        displayed(rows(session, &format!("EXPLAIN FORMAT = 'verbose' {sql}")))
            .into_iter()
            .filter(|row| row.len() > 5 && row[3] == "root")
            .map(|row| format!("{} {} {}", row[0], row[1], row[2]))
            .collect()
    };
    assert_eq!(
        plan(
            &mut session,
            "SELECT /*+ MERGE_JOIN(tja, tjb) */ * FROM tja JOIN tjb ON tja.x = tjb.id"
        )[..4],
        [
            "MergeJoin_12 12487.50 7621831.92",
            "├─TableReader_29(Build) 10000.00 429797.30",
            "└─Sort_27(Probe) 9990.00 7182059.61",
            "  └─TableReader_23 9990.00 462937.25",
        ]
    );
    assert_eq!(
        plan(
            &mut session,
            "SELECT /*+ NO_HASH_JOIN(tja, tjb) */ * FROM tja JOIN tjb ON tja.x = tjb.id"
        )[..3],
        [
            "IndexJoin_13 12487.50 5564987.43",
            "├─TableReader_47(Build) 9990.00 462937.25",
            "└─TableReader_52(Probe) 9990.00 18.78",
        ]
    );
    assert_eq!(
        plan(
            &mut session,
            "SELECT /*+ NO_MERGE_JOIN(tja, tjb) */ * FROM tja JOIN tjb ON tja.x = tjb.id"
        )[1..3],
        [
            "├─TableReader_26(Build) 9990.00 462937.25",
            "└─TableReader_37(Probe) 10000.00 429797.30",
        ]
    );
}

/// `EXPLAIN FORMAT='verbose'` records Go's path-pruning and heuristic-selection
/// notes (`getPruningInfo`, `derivePathStatsAndTryHeuristics`) for `SHOW
/// WARNINGS`, and an index point get draws no scan id before its own. Expected
/// notes and ids: Go 1.27 on this branch (`TestZZ` probes `NP1` and `NP3`).
#[test]
fn verbose_explain_records_path_pruning_and_heuristic_notes_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1130)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    for ddl in [
        "CREATE TABLE note_t (a INT PRIMARY KEY, b INT, c INT, d INT, UNIQUE KEY ub (b), KEY ic (c))",
        "CREATE TABLE note_u (a INT, b INT, c INT, KEY ia (a), KEY ib (b), KEY iab (a, b))",
        "INSERT INTO note_t VALUES (1,1,1,1),(2,2,2,2),(3,3,3,3),(4,4,4,4),(5,5,5,5)",
        "INSERT INTO note_u VALUES (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)",
        "ANALYZE TABLE note_t, note_u",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    let notes = |session: &mut Session, sql: &str| -> Vec<String> {
        rows(session, &format!("EXPLAIN FORMAT = 'verbose' {sql}"));
        displayed(rows(session, "SHOW WARNINGS"))
            .into_iter()
            .filter(|row| row[0] == "Note")
            .map(|row| row[2].clone())
            .collect()
    };
    assert_eq!(
        notes(&mut session, "SELECT * FROM note_u WHERE a = 1 AND b = 2"),
        ["[note_u,iab] remain after pruning paths for note_u given Prop{SortItems: [], TaskTp: rootTask}"]
    );
    assert_eq!(
        notes(&mut session, "SELECT * FROM note_u WHERE b > 1 ORDER BY b"),
        [
            "[note_u,ib] remain after pruning paths for note_u given Prop{SortItems: [], TaskTp: rootTask}",
            "[note_u,ib] remain after pruning paths for note_u given Prop{SortItems: [{test.note_u.b asc}], TaskTp: rootTask}",
        ]
    );
    assert_eq!(
        notes(&mut session, "SELECT * FROM note_t WHERE b = 2 AND c > 0"),
        ["unique index ub of note_t is selected since the path only has point ranges with double scan"]
    );
    assert_eq!(
        notes(&mut session, "SELECT * FROM note_t WHERE a = 1 AND c > 0"),
        ["handle of note_t is selected since the path only has point ranges"]
    );
    // A join with no filter still estimates each index's full range, so an
    // ordered request prunes the indexes against the table path.
    assert_eq!(
        notes(
            &mut session,
            "SELECT * FROM note_t JOIN note_u ON note_t.a = note_u.a"
        )[..1],
        ["[note_t] remain after pruning paths for note_t given Prop{SortItems: [{test.note_t.a asc}], TaskTp: rootTask}"]
    );
    let point_get_ids = displayed(rows(
        &mut session,
        "EXPLAIN FORMAT = 'verbose' SELECT * FROM note_t WHERE b = 2 AND c > 0",
    ))
    .into_iter()
    .map(|row| row[0].trim_start_matches([' ', '└', '─']).to_string())
    .collect::<Vec<_>>();
    assert_eq!(point_get_ids, ["Selection_6", "Point_Get_5"]);
}

/// `EXPLAIN FORMAT='hint'` generates the hints that reproduce the plan
/// (`GenHintsFromFlatPlan`) followed by the statement's own. Expected rows: Go
/// 1.27 on this branch (`TestZZ` probes `HT1` and `HT2`).
#[test]
fn explain_format_hint_reproduces_the_plan_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1131)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    for ddl in [
        "CREATE TABLE hint_t1 (a INT PRIMARY KEY, b INT, c INT, d INT, KEY ib (b), KEY ic (c), KEY ibc (b, c))",
        "CREATE TABLE hint_t2 (a INT PRIMARY KEY, b INT, c INT, KEY ib (b))",
        "INSERT INTO hint_t1 VALUES (1,1,1,1),(2,2,2,2),(3,3,3,3),(4,4,4,4),(5,5,5,5)",
        "INSERT INTO hint_t2 VALUES (1,1,1),(2,2,2),(3,3,3)",
        "ANALYZE TABLE hint_t1, hint_t2",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    let hint = |session: &mut Session, sql: &str| -> String {
        displayed(rows(session, &format!("EXPLAIN FORMAT = 'hint' {sql}")))[0][0].clone()
    };
    assert_eq!(
        hint(&mut session, "SELECT * FROM hint_t1 WHERE a > 2 ORDER BY a"),
        "use_index(@`sel_1` `test`.`hint_t1`), order_index(@`sel_1` `test`.`hint_t1` `primary`)"
    );
    assert_eq!(
        hint(&mut session, "SELECT b FROM hint_t1 WHERE b > 1"),
        "use_index(@`sel_1` `test`.`hint_t1` `ib`), no_order_index(@`sel_1` `test`.`hint_t1` `ib`)"
    );
    assert_eq!(
        hint(&mut session, "SELECT * FROM hint_t1 WHERE b = 1 OR c = 2"),
        "use_index_merge(@`sel_1` `hint_t1` `ib`, `ic`)"
    );
    assert_eq!(
        hint(
            &mut session,
            "SELECT /*+ max_execution_time(1000) use_index(hint_t1, ib) */ * FROM hint_t1 WHERE b > 1"
        ),
        "use_index(@`sel_1` `test`.`hint_t1` `ib`), no_order_index(@`sel_1` `test`.`hint_t1` `ib`), \
         max_execution_time(1000), use_index(`hint_t1` `ib`)"
    );
    assert_eq!(
        hint(&mut session, "UPDATE hint_t1 SET c = 1 WHERE b = 2"),
        "use_index(@`upd_1` `test`.`hint_t1`), no_order_index(@`upd_1` `test`.`hint_t1` `primary`)"
    );
    // A point get is a complete plan that needs no hint.
    assert_eq!(hint(&mut session, "SELECT * FROM hint_t1 WHERE a = 1"), "");
}

/// The statement summary records Go's `PLAN_HINT` (the hints reproducing the
/// plan plus the statement's own of the kinds `getEncodedPlan` keeps) and the
/// statement's binding text and digest, with the default database filled in.
/// Expected values: Go 1.27 on this branch (`TestZZ` probe `SS4`).
#[test]
fn statement_summary_records_plan_hint_and_binding_text_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1132)).unwrap();
    session
        .node_conn()
        .execute_write("CREATE DATABASE IF NOT EXISTS ss_hint")
        .unwrap();
    session.node_conn().execute_write("USE ss_hint").unwrap();
    for ddl in [
        "CREATE TABLE st (a INT PRIMARY KEY, b INT, c INT, KEY ib (b))",
        "CREATE TABLE su (a INT, b INT, KEY ia (a))",
        "INSERT INTO st VALUES (1,1,1),(2,2,2),(3,3,3)",
        "ANALYZE TABLE st",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    rows(&mut session, "SELECT /*+ use_index(st, ib) max_execution_time(1000) */ * FROM st WHERE b > 1");
    rows(&mut session, "INSERT INTO su SELECT a, b FROM st");
    let summary = displayed(rows(
        &mut session,
        "SELECT binding_digest_text, plan_hint FROM information_schema.statements_summary \
         WHERE schema_name = 'ss_hint' AND (digest_text LIKE 'select%' OR digest_text LIKE 'insert into `su` select%') \
         ORDER BY digest_text",
    ));
    assert_eq!(
        summary,
        [
            [
                "insert into `ss_hint` . `su` select `a` , `b` from `ss_hint` . `st`",
                "use_index(@`sel_1` `ss_hint`.`st` `ib`), no_order_index(@`sel_1` `ss_hint`.`st` `ib`)"
            ],
            [
                "select * from `ss_hint` . `st` where `b` > ?",
                "use_index(@`sel_1` `ss_hint`.`st` `ib`), no_order_index(@`sel_1` `ss_hint`.`st` `ib`), \
                 max_execution_time(1000)"
            ],
        ]
    );
}

/// Go `selectResult.fetchResp` merges every coprocessor response's details
/// into `StmtCtx.ExecDetails`, which the summary reads as `SUM_COP_TASK_NUM`
/// and the key counts. Expected values: Go 1.27 on this branch (`TestZZ` probe
/// `CS1`); point gets never reach the coprocessor.
#[test]
fn statement_summary_records_cop_task_details_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1133)).unwrap();
    session
        .node_conn()
        .execute_write("CREATE DATABASE IF NOT EXISTS ss_cop")
        .unwrap();
    session.node_conn().execute_write("USE ss_cop").unwrap();
    for ddl in [
        "CREATE TABLE ct (a INT PRIMARY KEY, b INT, c INT, KEY ib (b))",
        "CREATE TABLE cu (a INT, b INT, KEY ia (a))",
        "INSERT INTO ct VALUES (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)",
        "INSERT INTO cu VALUES (1,1),(2,2)",
        "ANALYZE TABLE ct, cu",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    for query in [
        "SELECT a FROM ct WHERE b > 1",
        "SELECT * FROM ct JOIN cu ON ct.a = cu.a WHERE ct.b > 0",
        "SELECT COUNT(*) FROM ct GROUP BY b",
        "SELECT * FROM ct WHERE a = 1",
        "SELECT * FROM ct",
    ] {
        rows(&mut session, query);
    }
    let summary = displayed(rows(
        &mut session,
        "SELECT digest_text, sum_cop_task_num, avg_total_keys, max_total_keys, \
         avg_processed_keys, max_processed_keys FROM information_schema.statements_summary \
         WHERE schema_name = 'ss_cop' AND digest_text LIKE 'select%' ORDER BY digest_text",
    ));
    assert_eq!(
        summary,
        [
            ["select * from `ct`", "1", "0", "0", "5", "5"],
            [
                "select * from `ct` join `cu` on `ct` . `a` = `cu` . `a` where `ct` . `b` > ?",
                "2", "0", "0", "7", "7"
            ],
            ["select * from `ct` where `a` = ?", "0", "0", "0", "0", "0"],
            ["select `a` from `ct` where `b` > ?", "1", "0", "0", "4", "4"],
            ["select count ( ? ) from `ct` group by `b`", "1", "0", "0", "5", "5"],
        ]
    );
}

/// Go plans an `UPDATE`/`DELETE` read like a SELECT and reads it through the
/// same coprocessor `TableReader`, so its responses reach `StmtCtx.ExecDetails`
/// exactly as a query's do; a point write never reaches the coprocessor.
/// Expected values: the Go server on TiKV (same statements, `nn_dml.ct`),
/// whose cop task counts and processed keys this in-process store reports
/// alike (TiKV's MVCC `total_keys` are TiKV's own and are not compared).
#[test]
fn a_dml_read_reaches_the_coprocessor_like_a_select() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1135)).unwrap();
    session
        .node_conn()
        .execute_write("CREATE DATABASE IF NOT EXISTS ss_dml")
        .unwrap();
    session.node_conn().execute_write("USE ss_dml").unwrap();
    for statement in [
        "CREATE TABLE ct (a INT PRIMARY KEY, b INT, c INT, KEY ib (b))",
        "INSERT INTO ct VALUES (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)",
        "ANALYZE TABLE ct",
        "UPDATE ct SET c = c + 1 WHERE c > 1",
        "DELETE FROM ct WHERE c = 3",
        "UPDATE ct SET c = 0 WHERE a = 1",
    ] {
        session.node_conn().execute_write(statement).unwrap();
    }
    let summary = displayed(rows(
        &mut session,
        "SELECT digest_text, sum_cop_task_num, avg_processed_keys, max_processed_keys, \
         avg_affected_rows FROM information_schema.statements_summary \
         WHERE schema_name = 'ss_dml' AND (digest_text LIKE 'update%' \
         OR digest_text LIKE 'delete%') ORDER BY digest_text",
    ));
    assert_eq!(
        summary,
        [
            ["delete from `ct` where `c` = ?", "1", "5", "5", "1"],
            ["update `ct` set `c` = ? where `a` = ?", "0", "0", "0", "1"],
            [
                "update `ct` set `c` = `c` + ? where `c` > ?",
                "1",
                "5",
                "5",
                "4"
            ],
        ]
    );
}

/// `exhaustPhysicalPlans4LogicalApply` builds the embedded hash join before
/// the Apply itself, so the Apply's id is one past the hash join's. Expected
/// ids: Go 1.27 on this branch (`TestZZ` probe `AP1`).
#[test]
fn apply_enumeration_spends_the_embedded_hash_join_id() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1129)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    for ddl in [
        "SET @@session.tidb_opt_insubq_to_join_and_agg = 0",
        "CREATE TABLE apply_ids (a INT PRIMARY KEY, b INT, c INT, KEY idx (b))",
        "INSERT INTO apply_ids VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 3, 4), (5, 3, 5)",
        "ANALYZE TABLE apply_ids",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    let plan = displayed(rows(
        &mut session,
        "EXPLAIN FORMAT = 'verbose' SELECT t.c IN (SELECT COUNT(*) FROM apply_ids s, apply_ids t1 \
         WHERE s.b = t.a AND s.b = 3 AND s.a = t1.a) FROM apply_ids t",
    ));
    let ids: Vec<&str> = plan
        .iter()
        .filter(|row| row.len() > 5 && row[3] == "root")
        .map(|row| row[0].trim_start_matches([' ', '├', '└', '─', '│']))
        .collect();
    assert_eq!(&ids[..2], ["Projection_15", "Apply_18"]);
}

/// Statements both engines reject (or, for the one MODIFY, accept) used to be
/// skipped by the corpus replay as "both rejected", which hid wrong codes. Each
/// expectation below is what Go on this branch answers (probe sets `PL3`-`PL5`).
#[test]
fn rejected_statements_answer_with_go_codes_and_messages() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1130)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    for ddl in [
        "CREATE TABLE ge (a INT, b INT, c INT AS (a + b))",
        "CREATE TABLE tv0 (a INT PRIMARY KEY, b INT)",
        "CREATE VIEW vv AS SELECT * FROM tv0",
        "CREATE TABLE pt (a INT, b VARCHAR(10)) PARTITION BY RANGE (a) \
         (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20), \
         PARTITION p2 VALUES LESS THAN (30))",
        "CREATE TABLE pt3 (a INT, b VARCHAR(10))",
        "INSERT INTO pt VALUES (1, 'a'), (2, 'b'), (11, 'c'), (21, 'd')",
        "INSERT INTO pt3 VALUES (1, 'a'), (2, 'b'), (11, 'c'), (21, 'd')",
        "CREATE TABLE tc1 (c VARCHAR(10) COLLATE utf8_general_ci, d INT)",
        "CREATE TABLE tc2 (c VARCHAR(10) COLLATE utf8_unicode_ci, d INT)",
        "CREATE TABLE dz (a INT) PARTITION BY RANGE (10 DIV a) \
         (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
        "ALTER TABLE tv0 MODIFY tv0.b BIGINT",
        "CREATE TABLE jd8 (id INT PRIMARY KEY, j2 JSON DEFAULT (JSON_ARRAY(id, id * 2)))",
        "CREATE VIEW v_a AS SELECT * FROM tv0",
        "CREATE VIEW v_b AS SELECT * FROM v_a",
        "CREATE TABLE sd (c INT, c1 VARCHAR(32) DEFAULT (STR_TO_DATE('01-01', '%Y-%m-%d')))",
    ] {
        session
            .node_conn()
            .execute_write(ddl)
            .unwrap_or_else(|error| panic!("{ddl}: {error:?}"));
    }
    let write_error = |session: &mut Session, sql: &str| {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        (error.code, error.message)
    };
    for (sql, code, message) in [
        (
            "INSERT INTO ge (c) SELECT a, b FROM ge",
            1136,
            "Column count doesn't match value count at row 1",
        ),
        (
            "INSERT INTO ge SELECT * FROM ge",
            3105,
            "The value specified for generated column 'c' in table 'ge' is not allowed.",
        ),
        (
            "INSERT INTO tv0 VALUES (11, (SELECT NOT b))",
            1054,
            "Unknown column 'b' in 'field list'",
        ),
        (
            "ALTER TABLE vv ADD COLUMN x INT",
            1347,
            "'test.vv' is not BASE TABLE",
        ),
        ("ALTER TABLE vv CACHE", 1347, "'test.vv' is not BASE TABLE"),
        (
            "CREATE TABLE tq (a INT) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (a))",
            1054,
            "Unknown column 'a' in 'expression'",
        ),
        (
            "CREATE USER pu2 ATTRIBUTE '{\"name\": \"Tom\", age: 19}'",
            3140,
            "Invalid JSON text: The document root must not be followed by other values.",
        ),
        (
            "CREATE TABLE ie (c1 INT, KEY (c_not_exist))",
            1072,
            "column does not exist: c_not_exist",
        ),
        (
            "ALTER TABLE tv0 MODIFY testx.tv0.b BIGINT",
            1102,
            "Incorrect database name 'testx'",
        ),
        (
            "ALTER TABLE tv0 MODIFY t.b BIGINT",
            1103,
            "Incorrect table name 't'",
        ),
        (
            "ALTER TABLE pt IMPORT PARTITION p1 TABLESPACE",
            8200,
            "Unsupported Unsupported/unknown ALTER TABLE specification",
        ),
        (
            "ALTER TABLE pt STATS_OPTIONS = 'buckets=10'",
            8200,
            "Unsupported Unsupported/unknown ALTER TABLE specification",
        ),
        (
            "CREATE SPATIAL INDEX s1 ON pt (a)",
            8200,
            "SPATIAL index is not supported",
        ),
        // Go `getDefaultValue` evaluates a non-call DEFAULT with
        // `EvalSimpleAst`, against no columns: a column it names is unknown
        // whether or not the table has it.
        (
            "CREATE TABLE q1 (x INT, y INT DEFAULT (x + 1))",
            1054,
            "Unknown column 'x' in 'expression'",
        ),
        (
            "CREATE TABLE q3 (x INT, y INT DEFAULT (x))",
            1054,
            "Unknown column 'x' in 'expression'",
        ),
        (
            "ALTER TABLE pt ADD COLUMN z INT DEFAULT (a + 1)",
            1054,
            "Unknown column 'a' in 'expression'",
        ),
        // Go `expressionRewriter.requirePlanCtx` with no plan.
        (
            "CREATE TABLE q4 (a INT DEFAULT (@@max_connections))",
            1105,
            "planCtx is required when rewriting node: '*ast.VariableExpr', accessing system variable requires plan context",
        ),
        (
            "CREATE TABLE q6 (a INT DEFAULT ((SELECT 1)))",
            1105,
            "planCtx is required when rewriting node: '*ast.SubqueryExpr'",
        ),
        (
            "ALTER TABLE ge ADD COLUMN cc INT, AUTO_ID_CACHE = 10",
            8200,
            "Unsupported multi schema change for modify auto id cache",
        ),
        (
            "UPDATE pt PARTITION (p0, p_non), pt3 SET pt.a = 40 WHERE pt3.a = 2",
            1735,
            "Unknown partition 'p_non' in table 'pt'",
        ),
        (
            "UPDATE pt PARTITION (p0), pt3 SET pt.a = 15 WHERE pt3.a = 1",
            1748,
            "Found a row not matching the given partition set",
        ),
        (
            "ALTER DATABASE test SET TIFLASH REPLICA 1 SET TIFLASH REPLICA 2",
            8200,
            "Unsupported multi schema change for set tiflash replica",
        ),
        (
            "CREATE TABLE mvpk (f1 JSON, PRIMARY KEY mvi ((CAST(CAST(f1 AS UNSIGNED ARRAY) AS UNSIGNED ARRAY))))",
            1235,
            "This version of TiDB doesn't yet support 'Use of CAST( .. AS .. ARRAY) outside of functional index in CREATE(non-SELECT)/ALTER TABLE or in general expressions'",
        ),
        (
            "CREATE TABLE mvfk (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, doc JSON, \
             FOREIGN KEY fk_user_id ((CAST(doc->'$[*]' AS SIGNED ARRAY))) REFERENCES tv0 (a))",
            1072,
            "Key column '_V$_fk_user_id_0' doesn't exist in table",
        ),
        (
            "CREATE TABLE ivn (id INT UNSIGNED) PARTITION BY RANGE (id) INTERVAL (1000000) \
             FIRST PARTITION LESS THAN (NULL) LAST PARTITION LESS THAN (10000000)",
            8200,
            "Unsupported INTERVAL partitioning: Error when generating partition values",
        ),
        (
            "CREATE TABLE ivn (id INT UNSIGNED) PARTITION BY RANGE (id) INTERVAL (1000000) \
             FIRST PARTITION LESS THAN (0) LAST PARTITION LESS THAN (NULL)",
            8200,
            "Unsupported INTERVAL: expr () not matching FIRST + n INTERVALs (0 + n * 1000000)",
        ),
        (
            "CREATE OR REPLACE VIEW v_a AS SELECT * FROM v_b",
            1146,
            "Table 'test.v_a' doesn't exist",
        ),
        (
            "INSERT INTO sd (c) VALUES (1)",
            1292,
            "Incorrect datetime value: '2001-01-00 00:00:00'",
        ),
        (
            format!(
                "ALTER TABLE pt ADD PARTITION (PARTITION p5 VALUES LESS THAN (100) COMMENT '{}')",
                "b".repeat(1100)
            )
            .as_str(),
            1793,
            "Comment for table partition 'p5' is too long (max = 1024)",
        ),
        (
            "INSERT INTO jd8 (id) VALUES (1)",
            1054,
            "Unknown column 'id' in 'expression'",
        ),
        (
            "CREATE OR REPLACE VIEW tv0 AS SELECT * FROM tv0",
            1347,
            "'test.tv0' is not VIEW",
        ),
        (
            "ALTER TABLE tv0 ENGINE_ATTRIBUTE = '{', ENGINE_ATTRIBUTE = '{\"storage_class\": \"STANDARD\"}'",
            8270,
            "Invalid engine attribute format: 'unexpected end of JSON input'",
        ),
        (
            "ALTER TABLE tv0 ENGINE_ATTRIBUTE = '{\"storage_class\": \"STANDARD\"}', STORAGE_CLASS = 'IA'",
            8271,
            "Invalid storage class: can not specify 'ENGINE_ATTRIBUTE' and 'STORAGE_CLASS' together",
        ),
        (
            "ALTER TABLE tv0 ADD INDEX i2 (b), MODIFY COLUMN b INT NULL DEFAULT 1 AFTER b",
            8200,
            "Unsupported operate same column 'b'",
        ),
        (
            "ALTER TABLE tv0 MODIFY COLUMN b INT AUTO_INCREMENT DEFAULT (STR_TO_DATE('1980-01-01', '%Y-%m-%d'))",
            1067,
            "Invalid default value for 'b'",
        ),
        (
            "SET @@sql_mode = 'STRICT_ALL_TABLES,ERROR_FOR_DIVISION_BY_ZERO'",
            0,
            "",
        ),
    ] {
        if code == 0 {
            session.node_conn().execute_write(sql).unwrap();
            continue;
        }
        assert_eq!(write_error(&mut session, sql), (code, message.to_owned()), "{sql}");
    }
    assert_eq!(
        write_error(&mut session, "INSERT INTO dz VALUES (NULL), (0), (1)"),
        (1365, "Division by 0".to_owned())
    );
    for (sql, code, message) in [
        (
            "SELECT t.upper(1) FROM nonexist",
            1305,
            "FUNCTION t.upper does not exist",
        ),
        (
            "SELECT * FROM tc1 WHERE c < ANY (SELECT c FROM tc2)",
            1267,
            "Illegal mix of collations (utf8_general_ci,IMPLICIT) and (utf8_unicode_ci,IMPLICIT) for operation '<'",
        ),
        (
            "SELECT * FROM tc1 WHERE (c, d) = ANY (SELECT c FROM tc2)",
            1241,
            "Operand should contain 2 column(s)",
        ),
        (
            "SELECT a, SUM(b) OVER (PARTITION BY zz) FROM tv0",
            1054,
            "Unknown column 'zz' in 'window partition by'",
        ),
        (
            "SELECT a, SUM(b) OVER (ORDER BY zz) FROM tv0",
            1054,
            "Unknown column 'zz' in 'window order by'",
        ),
        (
            "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT SUM(n) + 1 FROM c WHERE n < 3) SELECT * FROM c",
            3575,
            "Recursive Common Table Expression 'c' can contain neither aggregation nor window functions in recursive query block",
        ),
        (
            "SELECT CONCAT(b, GROUP_CONCAT(b)) FROM tv0 WHERE xxx = xxx",
            1054,
            "Unknown column 'xxx' in 'where clause'",
        ),
        (
            "SELECT 'a' COLLATE utf8mb4_bin UNION SELECT 'b' COLLATE utf8mb4_general_ci",
            1271,
            "Illegal mix of collations for operation 'UNION'",
        ),
    ] {
        assert_eq!(query_error(&mut session, sql), (code, message.to_owned()), "{sql}");
    }
}

/// `EXPLAIN FOR CONNECTION` renders the plan the connection's previous
/// statement published, under the analyze columns, as Go does: a statement
/// that was itself an EXPLAIN leaves an `Explain` plan that renders nothing,
/// and only brief/row/verbose are admitted. Captured from the oracle.
#[test]
fn explain_for_connection_renders_the_previous_plan() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1142)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE efc (a INT PRIMARY KEY, b INT)")
        .unwrap();
    let id = displayed(rows(&mut session, "select connection_id()"))[0][0].clone();
    rows(&mut session, "select * from efc where a = 1");
    let plan = displayed(rows(&mut session, &format!("explain for connection {id}")));
    assert_eq!(plan.len(), 1, "{plan:?}");
    assert!(plan[0][0].starts_with("Point_Get_"), "{plan:?}");
    assert_eq!(plan[0][3], "root");
    assert_eq!(plan[0][4], "table:efc");
    // The EXPLAIN FOR statement is itself an EXPLAIN: its own plan is empty.
    let after = displayed(rows(
        &mut session,
        &format!("explain format='brief' for connection {id}"),
    ));
    assert!(after.is_empty(), "{after:?}");
    rows(&mut session, "select * from efc where a = 1");
    let (_, message) = query_error(&mut session, &format!("explain format='json' for connection {id}"));
    assert!(
        message.contains("explain format 'json' for connection is not supported now"),
        "{message}"
    );
    let (code, message) = query_error(&mut session, "explain for connection 99999");
    assert_eq!((code, message.as_str()), (1094, "Unknown thread id: 99999"));
}

/// A CTE definition keeps its storage id in every format
/// (`CTEDefinition.ExplainID`), and the implicit casts a builtin's arguments
/// carry no longer decide push-down: `round(dt)` and `conv(int, ...)` read a
/// cast the catalog now lowers natively, while `period_add` has no pb code in
/// Go and stays above the reader. Each outcome was captured from the oracle.
#[test]
fn cte_ids_and_cast_wrapped_pushdown_follow_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1136)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE pc (i INT NOT NULL, s VARCHAR(20) NOT NULL, dt DATETIME NOT NULL)",
        )
        .unwrap();
    let plan = |session: &mut Session, sql: &str| {
        displayed(rows(session, sql))
            .into_iter()
            .map(|row| row.join(" "))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let cte = plan(
        &mut session,
        "explain format='plan_tree' with c as (select i from pc where i < 10 order by i limit 1) \
         select * from c where c.i < 18 union select * from c where c.i > 0",
    );
    assert!(
        cte.lines()
            .any(|line| line.starts_with("CTE_0 root") && line.contains("Non-Recursive CTE")),
        "{cte}"
    );
    for (sql, store) in [
        ("round(dt)", "cop[tikv]"),
        ("conv(i, i, i)", "cop[tikv]"),
        ("period_add(i, i)", "root"),
    ] {
        let text = plan(
            &mut session,
            &format!("explain format='brief' select i from pc where {sql}"),
        );
        let selection = text
            .lines()
            .find(|line| line.contains("Selection ") && line.contains(store))
            .is_some();
        assert!(selection, "{sql}\n{text}");
    }
}

/// Go's PREPARE of a statement it does not plan-cache adds only that note:
/// a FLUSH STATS_DELTA's missing-table warning belongs to EXECUTE (captured
/// from Go on this branch).
#[test]
fn prepare_shows_only_the_plan_cache_note() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1910)).unwrap();
    session
        .node_conn().prepare_general("FLUSH STATS_DELTA test.nosuch")
        .unwrap();
    assert_eq!(
        session
            .warnings()
            .iter()
            .map(|warning| warning.code())
            .collect::<Vec<_>>(),
        vec![1105]
    );
}

/// The TiFlash replica manager's status update runs as Go's persisted
/// `ActionUpdateTiFlashReplicaStatus` job (`UpdateTableReplicaInfo` queues it
/// and the owner applies it): the new status is published, and a status
/// that already holds queues nothing.
#[test]
fn tiflash_replica_status_runs_as_a_persisted_job_like_go() {
    // Go `TestSetTableFlashReplica` enables `mockTiFlashStoreCount` so the
    // replica count passes `checkTiFlashReplicaCount` without TiFlash stores.
    let _guard = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount", "return(true)");
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1949)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    rows(&mut session, "create table r (a int)");
    rows(&mut session, "alter table r set tiflash replica 1");
    let table_id: i64 = displayed(rows(
        &mut session,
        "select tidb_table_id from information_schema.tables where table_name = 'r'",
    ))[0][0]
        .parse()
        .unwrap();
    let job = |session: &mut _| -> (i64, String) {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        (
            row[0].parse().unwrap(),
            [1, 2, 3, 4, 11].map(|column| row[column].clone()).join("|"),
        )
    };
    let update = |available: bool| {
        stack.factory.ddl.update_table_replica_info(table_id, available,
            &tidb_sessionctx::StmtContext::for_query(),
        )
    };
    update(true).unwrap();
    let (updated, kind) = job(&mut session);
    assert_eq!(kind, "test|r|update tiflash replica status|public|synced");
    // `information_schema.tiflash_replica` is read on the DDL owner's status
    // port, which this in-process node does not serve; the replica info is
    // what that reader would return.
    let available = || {
        let (_, info) = stack.factory.catalog_snapshot().table_info_by_name("test", "r").unwrap();
        let available = info.read().tiflash_replica.as_ref().unwrap().read().available;
        available
    };
    assert!(available());
    update(true).unwrap();
    assert_eq!(
        job(&mut session).0,
        updated,
        "the status already holds: no job"
    );
    let missing = stack.factory.ddl.update_table_replica_info(table_id + 1000, true,
        &tidb_sessionctx::StmtContext::for_query(),
    );
    assert!(missing.is_err());
}

/// A conjunct the cop predicate description cannot express must never change
/// an answer. Go pushes down exactly the conditions `CanExprsPushDown` admits
/// and the coprocessor Selection applies all of them; the request carries every
/// condition `ExprToPB` converts or none of them. Every shape
/// below is compared with the same predicate evaluated as a plain expression
/// over the same rows.
#[test]
fn a_conjunct_the_cop_description_cannot_express_never_changes_an_answer() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(61))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.dropped (id int primary key, a int, b int, c int, \
         s varchar(10), KEY iab (a, b), KEY iabc (a, b, c))",
    );
    rows(
        &mut session,
        "INSERT INTO test.dropped VALUES \
         (1, 1, 0, 5, 'x'), (2, 1, 1, 6, 'y'), (3, 2, 0, 7, 'z'), (4, 2, 1, NULL, NULL), \
         (5, 3, NULL, 9, 'w'), (6, 3, 2, 1, ''), (7, NULL, 1, 2, 'v'), (8, 4, 4, 4, 'u'), \
         (9, 5, 0, 0, 'x'), (10, 5, 7, 3, 'q')",
    );
    const PREDICATES: &[&str] = &[
        // A bare column and its negation: no comparison to describe.
        "b",
        "NOT b",
        "a AND b",
        "a OR b",
        // Arithmetic and conditional shapes.
        "a + b > 3",
        "b * 2 = c",
        "CASE WHEN b > 0 THEN a > 1 ELSE FALSE END",
        "COALESCE(b, 0) > 0",
        "IF(b > 0, a, c) > 2",
        "a IN (b, 1)",
        "b IS TRUE",
        "(b IS NOT TRUE)",
        "b IS FALSE",
        "a - 1 = 0",
        "a DIV 2 = 1",
        "LENGTH(s) > 2",
        // A column against a column and a function of a column.
        "a = b",
        "a < c",
        "ABS(a - b) = 1",
        "s > 'u' AND b",
        // Mixed: one describable conjunct beside one that is not.
        "a >= 2 AND b",
        "a >= 2 AND a + b > 3",
        "a < 5 AND COALESCE(b, 0) > 0",
    ];
    for predicate in PREDICATES {
        // The reference keeps every row's verdict, evaluated by the root
        // projection over the very same rows.
        let verdicts = displayed(rows(
            &mut session,
            &format!(
                "SELECT id, a, b, c, ({predicate}) IS TRUE FROM test.dropped \
                 USE INDEX() ORDER BY id"
            ),
        ));
        let kept: Vec<&Vec<String>> = verdicts.iter().filter(|row| row[4] == "1").collect();

        // 1. A pushed partial aggregate.
        let pushed_count = displayed(rows(
            &mut session,
            &format!("SELECT count(*) FROM test.dropped WHERE {predicate}"),
        ));
        assert_eq!(
            pushed_count,
            vec![vec![kept.len().to_string()]],
            "`{predicate}`: count(*)"
        );

        // 2. A covering index read, the filter riding the index Selection.
        let covering = displayed(rows(
            &mut session,
            &format!(
                "SELECT a, b, c FROM test.dropped USE INDEX (iabc) WHERE {predicate} \
                 ORDER BY id"
            ),
        ));
        let mut expected: Vec<Vec<String>> = kept
            .iter()
            .map(|row| vec![row[1].clone(), row[2].clone(), row[3].clone()])
            .collect();
        let mut got = covering;
        expected.sort();
        got.sort();
        assert_eq!(got, expected, "`{predicate}`: the covering index read");

        // 3. A pushed limit counts rows after the Selection.
        let limited = displayed(rows(
            &mut session,
            &format!("SELECT id FROM test.dropped WHERE {predicate} ORDER BY id LIMIT 2"),
        ));
        let expected_limit: Vec<Vec<String>> = kept
            .iter()
            .take(2)
            .map(|row| vec![row[0].clone()])
            .collect();
        assert_eq!(limited, expected_limit, "`{predicate}`: the pushed limit");

        // 4. A pushed TopN.
        let topn = displayed(rows(
            &mut session,
            &format!("SELECT id FROM test.dropped WHERE {predicate} ORDER BY c, id LIMIT 3"),
        ));
        let mut by_c: Vec<&&Vec<String>> = kept.iter().collect();
        by_c.sort_by_key(|row| {
            (
                row[3].parse::<i64>().map_or((0, 0), |c| (1, c)),
                row[0].parse::<i64>().unwrap(),
            )
        });
        let expected_topn: Vec<Vec<String>> = by_c
            .iter()
            .take(3)
            .map(|row| vec![row[0].clone()])
            .collect();
        assert_eq!(topn, expected_topn, "`{predicate}`: the pushed TopN");
    }
}

/// Go's TiFlash MPP planning (`convertToTableScan`'s MPP branch, the MPP
/// arms above it, `postOptimize`'s `handleFineGrainedShuffle`), captured from
/// Go with hypothetical TiFlash replicas: with TiFlash cop banned (the
/// default) a TiFlash read is an MPP task converted to root, and the
/// selection, projection, two-phase aggregation and window run in the MPP
/// fragment -- the enforced exchange compressed FAST, the final aggregation
/// under Go's extra projection, the window's stretch at `stream_count: 8`.
#[test]
fn tiflash_reads_plan_as_go_mpp_fragments() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(112))
        .expect("session opens");
    for sql in [
        "CREATE TABLE test.t (a int not null, b bigint, c int not null)",
        "CREATE TABLE test.s (a int not null, b int)",
        "ALTER TABLE test.t SET HYPO TIFLASH REPLICA 1",
        "ALTER TABLE test.s SET HYPO TIFLASH REPLICA 1",
    ] {
        session.node_conn().execute_write(sql).expect(sql);
    }
    let reader = |rows: &str| {
        [
            format!("TableReader|{rows}|root||MppVersion: 3, data:ExchangeSender"),
            format!("└─ExchangeSender|{rows}|mpp[tiflash]||ExchangeType: PassThrough"),
        ]
    };
    let cases: [(&str, Vec<String>); 5] = [
        (
            // `preferMppBCJ`: the build side is small enough to broadcast.
            "SELECT * FROM test.t JOIN test.s ON t.a = s.a",
            reader("12500.00")
                .into_iter()
                .chain(
                    [
                        "  └─HashJoin|12500.00|mpp[tiflash]||inner join, equal:[eq(test.t.a, test.s.a)]",
                        "    ├─ExchangeReceiver(Build)|10000.00|mpp[tiflash]||",
                        "    │ └─ExchangeSender|10000.00|mpp[tiflash]||ExchangeType: Broadcast, Compression: FAST",
                        "    │   └─TableFullScan|10000.00|mpp[tiflash]|table:t|keep order:false, stats:pseudo",
                        "    └─TableFullScan(Probe)|10000.00|mpp[tiflash]|table:s|keep order:false, stats:pseudo",
                    ]
                    .map(str::to_owned),
                )
                .collect(),
        ),
        (
            "SELECT * FROM test.t",
            reader("10000.00")
                .into_iter()
                .chain(["  └─TableFullScan|10000.00|mpp[tiflash]|table:t|keep order:false, stats:pseudo".to_owned()])
                .collect(),
        ),
        (
            "SELECT a FROM test.t WHERE b > 1",
            reader("3333.33")
                .into_iter()
                .chain(
                    [
                        "  └─Projection|3333.33|mpp[tiflash]||test.t.a",
                        "    └─Selection|3333.33|mpp[tiflash]||gt(test.t.b, 1)",
                        "      └─TableFullScan|10000.00|mpp[tiflash]|table:t|keep order:false, stats:pseudo",
                    ]
                    .map(str::to_owned),
                )
                .collect(),
        ),
        (
            "SELECT a, count(*) FROM test.t GROUP BY a",
            reader("8000.00")
                .into_iter()
                .chain(
                    [
                        "  └─Projection|8000.00|mpp[tiflash]||test.t.a, Column#6",
                        "    └─Projection|8000.00|mpp[tiflash]||Column#6, test.t.a",
                        "      └─HashAgg|8000.00|mpp[tiflash]||group by:test.t.a, funcs:sum(Column#13)->Column#6, funcs:firstrow(test.t.a)->test.t.a",
                        "        └─ExchangeReceiver|8000.00|mpp[tiflash]||",
                        "          └─ExchangeSender|8000.00|mpp[tiflash]||ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.t.a, collate: binary]",
                        "            └─HashAgg|8000.00|mpp[tiflash]||group by:test.t.a, funcs:count(1)->Column#13",
                        "              └─TableFullScan|10000.00|mpp[tiflash]|table:t|keep order:false, stats:pseudo",
                    ]
                    .map(str::to_owned),
                )
                .collect(),
        ),
        (
            "SELECT a, row_number() OVER (PARTITION BY a ORDER BY b) FROM test.t",
            reader("10000.00")
                .into_iter()
                .chain(
                    [
                        "  └─Projection|10000.00|mpp[tiflash]||test.t.a, Column#7->Column#9, stream_count: 8",
                        "    └─Window|10000.00|mpp[tiflash]||row_number()->Column#7 over(partition by test.t.a order by test.t.b rows between current row and current row), stream_count: 8",
                        "      └─Sort|10000.00|mpp[tiflash]||test.t.a, test.t.b, stream_count: 8",
                        "        └─ExchangeReceiver|10000.00|mpp[tiflash]||stream_count: 8",
                        "          └─ExchangeSender|10000.00|mpp[tiflash]||ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.t.a, collate: binary], stream_count: 8",
                        "            └─TableFullScan|10000.00|mpp[tiflash]|table:t|keep order:false, stats:pseudo",
                    ]
                    .map(str::to_owned),
                )
                .collect(),
        ),
    ];
    let mut cases: Vec<(&str, Vec<String>)> = cases.into_iter().collect();
    cases.push((
        // `countStarRewrite`: count(*) over a TiFlash full scan counts the
        // narrowest NOT NULL column.
        "SELECT count(*) FROM test.t",
        [
            "HashAgg|1.00|root||funcs:count(Column#8)->Column#6",
            "└─TableReader|1.00|root||MppVersion: 3, data:ExchangeSender",
            "  └─ExchangeSender|1.00|mpp[tiflash]||ExchangeType: PassThrough",
            "    └─HashAgg|1.00|mpp[tiflash]||funcs:count(test.t.a)->Column#8",
            "      └─TableFullScan|10000.00|mpp[tiflash]|table:t|keep order:false, stats:pseudo",
        ]
        .map(str::to_owned)
        .to_vec(),
    ));
    cases.push((
        // An outer join broadcasts its inner side (`fixedBuildSide`).
        "SELECT * FROM test.t LEFT JOIN test.s ON t.a = s.a",
        reader("12500.00")
            .into_iter()
            .chain(
                [
                    "  └─HashJoin|12500.00|mpp[tiflash]||left outer join, left side:TableFullScan, equal:[eq(test.t.a, test.s.a)]",
                    "    ├─ExchangeReceiver(Build)|10000.00|mpp[tiflash]||",
                    "    │ └─ExchangeSender|10000.00|mpp[tiflash]||ExchangeType: Broadcast, Compression: FAST",
                    "    │   └─TableFullScan|10000.00|mpp[tiflash]|table:s|keep order:false, stats:pseudo",
                    "    └─TableFullScan(Probe)|10000.00|mpp[tiflash]|table:t|keep order:false, stats:pseudo",
                ]
                .map(str::to_owned),
            )
            .collect(),
    ));
    for (query, expected) in cases {
        let plan: Vec<String> = displayed(rows(&mut session, &format!("EXPLAIN FORMAT='brief' {query}")))
            .into_iter()
            .map(|row| row.join("|"))
            .collect();
        assert_eq!(plan, expected, "{query}");
    }
    // countStarRewrite's fallbacks: no NOT NULL fixed-width column keeps the
    // row id the scan reads; a narrower NOT NULL column beats the int handle.
    for sql in [
        "CREATE TABLE test.u (x varchar(10), y int)",
        "CREATE TABLE test.v (id int primary key, z tinyint not null)",
        "ALTER TABLE test.u SET HYPO TIFLASH REPLICA 1",
        "ALTER TABLE test.v SET HYPO TIFLASH REPLICA 1",
    ] {
        session.node_conn().execute_write(sql).expect(sql);
    }
    for (query, expected) in [
        ("SELECT count(*) FROM test.u", "funcs:count(test.u._tidb_rowid)->Column#7"),
        ("SELECT count(*) FROM test.v", "funcs:count(test.v.z)->Column#6"),
    ] {
        let plan = displayed(rows(&mut session, &format!("EXPLAIN FORMAT='brief' {query}")));
        assert!(
            plan.iter().any(|row| row[2] == "mpp[tiflash]" && row[4] == expected),
            "{query}: {plan:?}"
        );
    }
    // The reader names the session's `ChooseMppVersion()`.
    session.node_conn().execute_write("SET mpp_version = 1").unwrap();
    assert_eq!(
        displayed(rows(&mut session, "EXPLAIN FORMAT='brief' SELECT * FROM test.t"))[0][4],
        "MppVersion: 1, data:ExchangeSender"
    );
    session.node_conn().execute_write("SET mpp_version = UNSPECIFIED").unwrap();
    // Neither side fits the broadcast thresholds: both sides hash-exchange
    // by the join keys, collated as the equal condition derives (binary).
    session
        .node_conn()
        .execute_write("SET tidb_broadcast_join_threshold_size = 0, tidb_broadcast_join_threshold_count = 0")
        .unwrap();
    let plan: Vec<String> = displayed(rows(
        &mut session,
        "EXPLAIN FORMAT='brief' SELECT * FROM test.t JOIN test.s ON t.a = s.a",
    ))
    .into_iter()
    .map(|row| row.join("|"))
    .collect();
    let expected: Vec<String> = reader("12500.00")
        .into_iter()
        .chain(
            [
                "  └─HashJoin|12500.00|mpp[tiflash]||inner join, equal:[eq(test.t.a, test.s.a)]",
                "    ├─ExchangeReceiver(Build)|10000.00|mpp[tiflash]||",
                "    │ └─ExchangeSender|10000.00|mpp[tiflash]||ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.t.a, collate: binary]",
                "    │   └─TableFullScan|10000.00|mpp[tiflash]|table:t|keep order:false, stats:pseudo",
                "    └─ExchangeReceiver(Probe)|10000.00|mpp[tiflash]||",
                "      └─ExchangeSender|10000.00|mpp[tiflash]||ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.s.a, collate: binary]",
                "        └─TableFullScan|10000.00|mpp[tiflash]|table:s|keep order:false, stats:pseudo",
            ]
            .map(str::to_owned),
        )
        .collect();
    assert_eq!(plan, expected);
}

/// Go `clientConn.handleQuery`'s TiFlash fallback, the session's half:
/// `tidb_allow_fallback_to_tikv` naming TiFlash admits a TiFlash server
/// timeout; the re-run plans without TiFlash; and its fresh statement context
/// appends the parser's warnings, then the timeout as an Error, behind its
/// own once it has run (`handleStmt`'s `warns`).
#[test]
fn a_tiflash_fallback_rerun_reads_tikv_and_carries_the_timeout() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(114))
        .expect("session opens");
    for sql in [
        "CREATE TABLE test.fb (a int)",
        "ALTER TABLE test.fb SET HYPO TIFLASH REPLICA 1",
    ] {
        session.node_conn().execute_write(sql).expect(sql);
    }
    assert!(!session.tiflash_fallback_allowed(tidb_errno::errcode::ErrTiFlashServerTimeout));
    session
        .node_conn()
        .execute_write("SET tidb_allow_fallback_to_tikv = 'tiflash'")
        .unwrap();
    assert!(session.tiflash_fallback_allowed(tidb_errno::errcode::ErrTiFlashServerTimeout));
    assert!(!session.tiflash_fallback_allowed(tidb_errno::errcode::ErrWriteConflict));

    let reader_task = |session: &mut Session| {
        displayed(rows(session, "EXPLAIN FORMAT='brief' SELECT * FROM test.fb"))
            .into_iter()
            .map(|row| row[2].clone())
            .collect::<Vec<_>>()
    };
    assert!(reader_task(&mut session).iter().any(|task| task == "mpp[tiflash]"));
    session.set_tiflash_fallback(true);
    assert!(reader_task(&mut session).iter().all(|task| task != "mpp[tiflash]"));
    session.set_tiflash_fallback(false);
    assert!(reader_task(&mut session).iter().any(|task| task == "mpp[tiflash]"));

    // The statement alone: its parser warning.
    let sql = "SELECT /*+ WRONG_HINT_SYNTAX(xyz) */ 1";
    let run = |session: &mut Session, reopen: bool| {
        use crate::domain::resultset_source::ResultSetSource;
        let stmt = session.parse_at_statement_boundary(sql).expect("parses");
        if reopen {
            let timeout = tidb_store_driver_error::sql_query_error::SqlQueryError::new(
                tidb_errno::errcode::ErrTiFlashServerTimeout,
                *b"HY000",
                "TiFlash server timeout",
            );
            session.reopen_statement_boundary(Some(&timeout));
        }
        let mut result = session
            .node_conn()
            .execute_stmt(sql, &stmt)
            .expect("runs")
            .expect("a result set");
        let source = result.source();
        while !source.next_batch(8).expect("batch").is_empty() {}
        source.finish().expect("finish");
        source.close().expect("close");
        drop(result);
        rows(session, "SHOW WARNINGS")
    };
    let parser_warnings = run(&mut session, false);
    assert!(!parser_warnings.is_empty(), "the hint warns");
    let mut expected = parser_warnings;
    expected.push(vec![
        Datum::new_string_datum("Error"),
        Datum::UInt(u64::from(tidb_errno::errcode::ErrTiFlashServerTimeout)),
        Datum::new_string_datum("TiFlash server timeout"),
    ]);
    assert_eq!(run(&mut session, true), expected);

    // Go `executePlanCacheStmt`: the binary re-run's statement context
    // starts empty (no parser warnings travel), and the timeout is appended
    // once the re-run has answered.
    let statement = session
        .node_conn()
        .prepare_general("SELECT a FROM test.fb")
        .expect("prepares");
    session.set_tiflash_fallback(true);
    {
        let crate::domain::session_api::GeneralExecuteOutcome::Rows(mut result) = session
            .node_conn()
            .execute_general(&statement, &[])
            .expect("the TiKV re-run")
        else {
            panic!("SELECT returns rows")
        };
        use crate::domain::resultset_source::ResultSetSource;
        let source = result.source();
        while !source.next_batch(8).expect("batch").is_empty() {}
        source.finish().expect("finish");
        source.close().expect("close");
    }
    session.append_statement_error(&tidb_store_driver_error::sql_query_error::SqlQueryError::new(
        tidb_errno::errcode::ErrTiFlashServerTimeout,
        *b"HY000",
        "TiFlash server timeout",
    ));
    session.set_tiflash_fallback(false);
    assert_eq!(rows(&mut session, "SHOW WARNINGS"), expected[expected.len() - 1..]);
}

/// Go `SQLBindExec.reloadBindings`' full load (`LoadFromStorageToCache(true,
/// ...)`): a stored binding older than the incremental load's boundary
/// reaches the cache only once the bindings are reloaded (checked against
/// Go: the restored row is listed only after `ADMIN RELOAD BINDINGS`).
#[test]
fn admin_reload_bindings_loads_every_stored_binding() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(96)).expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "CREATE TABLE rb (a INT, b INT, KEY ia (a))");
    rows(
        &mut session,
        "CREATE GLOBAL BINDING FOR SELECT * FROM rb WHERE a = 1 USING SELECT /*+ use_index(rb, ia) */ * FROM rb WHERE a = 1",
    );
    rows(&mut session, "CREATE TABLE bind_copy LIKE mysql.bind_info");
    rows(&mut session, "INSERT INTO bind_copy SELECT * FROM mysql.bind_info WHERE source = 'manual'");
    rows(&mut session, "DROP GLOBAL BINDING FOR SELECT * FROM rb WHERE a = 1");
    rows(&mut session, "DELETE FROM mysql.bind_info WHERE source = 'manual'");
    rows(
        &mut session,
        "UPDATE bind_copy SET update_time = '2000-01-01 00:00:00', create_time = '2000-01-01 00:00:00'",
    );
    rows(&mut session, "INSERT INTO mysql.bind_info SELECT * FROM bind_copy");
    assert!(displayed(rows(&mut session, "SHOW GLOBAL BINDINGS")).is_empty());
    rows(&mut session, "ADMIN RELOAD BINDINGS");
    let listed = displayed(rows(&mut session, "SHOW GLOBAL BINDINGS"));
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(listed[0][3], "enabled");
}

/// A prepared point UPDATE or DELETE caches Go's fast point plan: Go's
/// `Optimize` tries `TryFastPlan` when the plan cache builds a plan too, and a
/// hit rebuilds the cached point plan's key from the EXECUTE parameters
/// (`RebuildPlan4CachedPlan` -> `buildRangesForPointGet`), over an int handle,
/// a unique index and a clustered common handle alike. Captured from a Go
/// tidb-server over the wire: the second EXECUTE is a hit whose plan is the
/// fast plan's (`Update_3` / `Delete_2` over `Point_Get_1`).
#[test]
fn a_prepared_point_write_caches_its_fast_point_plan_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1143)).unwrap();
    for sql in [
        "USE test",
        "create table t (id int primary key, uk int, b int, unique key(uk))",
        "create table c (a varchar(10), x int, b int, primary key(a, x) clustered)",
        "insert into t values (1,10,0),(2,20,0)",
        "insert into c values ('a',1,0),('b',2,0)",
    ] {
        session.node_conn().execute_write(sql).unwrap();
    }
    rows(&mut session, "set @b=5, @i=1, @u=10, @a='a', @x=1");
    let id = displayed(rows(&mut session, "select connection_id()"))[0][0].clone();
    for (name, sql, args, plan) in [
        ("s1", "update t set b=? where id=?", "@b, @i", [("Update_3", ""), ("└─Point_Get_1", "table:t")]),
        (
            "s2",
            "update t set b=? where uk=?",
            "@b, @u",
            [("Update_3", ""), ("└─Point_Get_1", "table:t, index:uk(uk)")],
        ),
        (
            "s3",
            "update c set b=? where a=? and x=?",
            "@b, @a, @x",
            [("Update_3", ""), ("└─Point_Get_1", "table:c, clustered index:PRIMARY(a, x)")],
        ),
        (
            "s4",
            "delete from t where uk=?",
            "@u",
            [("Delete_2", ""), ("└─Point_Get_1", "table:t, index:uk(uk)")],
        ),
    ] {
        rows(&mut session, &format!("prepare {name} from '{sql}'"));
        rows(&mut session, &format!("execute {name} using {args}"));
        rows(&mut session, &format!("execute {name} using {args}"));
        assert_eq!(
            displayed(rows(&mut session, "select @@last_plan_from_cache")),
            [["1"]],
            "{name}"
        );
        rows(&mut session, &format!("execute {name} using {args}"));
        let explained = displayed(rows(&mut session, &format!("explain for connection {id}")));
        let shape: Vec<(String, String)> = explained
            .iter()
            .map(|row| (row[0].clone(), row[4].clone()))
            .collect();
        let expected: Vec<(String, String)> = plan
            .iter()
            .map(|(op, info)| (op.to_string(), info.to_string()))
            .collect();
        assert_eq!(shape, expected, "{name}");
    }
    assert_eq!(
        displayed(rows(&mut session, "select * from t")),
        [["2", "20", "0"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "select * from c")),
        [["a", "1", "5"], ["b", "2", "0"]]
    );
}

/// Go's process plan follows every compiled statement: a SET, USE, DDL or
/// transaction-control statement's plan renders no rows under `EXPLAIN FOR
/// CONNECTION`, while a binary-protocol EXECUTE's plan is nil
/// (`runStmt`'s `currentPlan = nil` for `execStmt.Name == ""`) and
/// `SetProcessInfo` keeps the last valid plan. Captured from a Go
/// tidb-server over the wire.
#[test]
fn explain_for_connection_follows_gos_process_plan() {
    use tidb_param::PreparedValue;
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1151)).unwrap();
    for sql in [
        "USE test",
        "create table t (id int primary key, uk int, b int, unique key(uk))",
        "insert into t values (1,10,0),(2,20,0),(3,30,0)",
    ] {
        session.node_conn().execute_write(sql).unwrap();
    }
    let id = displayed(rows(&mut session, "select connection_id()"))[0][0].clone();
    let mut target = stack.factory.open_session(session_context(1152)).unwrap();
    let ids = |target: &mut crate::Session| -> Vec<String> {
        displayed(rows(target, &format!("explain for connection {id}")))
            .into_iter()
            .map(|row| row[0].clone())
            .collect()
    };
    for sql in ["set @x=1", "use test", "create table if not exists t2 (a int)", "begin", "commit"] {
        rows(&mut session, "select * from t where b > 0");
        rows(&mut session, sql);
        assert!(ids(&mut target).is_empty(), "{sql}: {:?}", ids(&mut target));
    }
    rows(&mut session, "do 1");
    assert_eq!(ids(&mut target), ["Projection_3", "└─TableDual_4"]);
    // The binary EXECUTEs keep `do 1`'s plan.
    let update = session.node_conn().prepare_general("update t set b=? where id=?").unwrap();
    let point = session.node_conn().prepare_general("select b from t where uk=?").unwrap();
    for _ in 0..3 {
        drop(
            session
                .node_conn()
                .execute_general(&update, &[PreparedValue::SignedLongLong(5), PreparedValue::SignedLongLong(1)])
                .unwrap(),
        );
        assert_eq!(ids(&mut target), ["Projection_3", "└─TableDual_4"]);
        drop(session.node_conn().execute_general(&point, &[PreparedValue::SignedLongLong(10)]).unwrap());
        assert_eq!(ids(&mut target), ["Projection_3", "└─TableDual_4"]);
    }
}

/// Go folds the information functions as `NewFunction` builds them, against
/// the session: the plan carries their values. Captured from a Go
/// tidb-server over the wire (`<nil>` before a database is selected).
#[test]
fn information_functions_fold_into_the_plan_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1153)).unwrap();
    let id = displayed(rows(&mut session, "select connection_id()"))[0][0].clone();
    let info = |session: &mut crate::Session, sql: &str| -> String {
        displayed(rows(session, sql))[0].last().unwrap().clone()
    };
    assert_eq!(info(&mut session, "explain select connection_id()"), format!("{id}->Column#1"));
    assert_eq!(info(&mut session, "explain select database()"), "<nil>->Column#1");
    assert_eq!(
        info(&mut session, "explain select current_role(), current_resource_group()"),
        "NONE->Column#1, default->Column#2"
    );
    rows(&mut session, "use test");
    assert_eq!(info(&mut session, "explain select database(), schema()"), "test->Column#1, test->Column#2");
}

/// Go's `plannercore.DefaultDisabledLogicalRulesList` is package-level and
/// `BootstrapSession` loads it from `mysql.opt_rule_blacklist`: an `ADMIN
/// RELOAD` on one connection changes what another plans, and the list in the
/// table applies once the server loads it at start. Go tidb-server capture
/// over two empty tables: the join plans as `HashJoin_9`, and as `HashJoin_7`
/// with `join_reorder` blacklisted.
#[test]
fn the_pushdown_blacklists_are_the_servers_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut writer = stack.factory.open_session(session_context(161)).unwrap();
    let mut reader = stack.factory.open_session(session_context(162)).unwrap();
    rows(&mut writer, "USE test");
    rows(&mut reader, "USE test");
    rows(&mut writer, "CREATE TABLE s (a INT, b INT)");
    rows(&mut writer, "CREATE TABLE t (a INT, b INT, KEY (b))");
    let join = |session: &mut Session| {
        displayed(rows(session, "EXPLAIN SELECT t._tidb_rowid FROM t JOIN s ON s.b = t.b"))[0][0].clone()
    };
    assert_eq!(join(&mut reader), "HashJoin_9");
    rows(&mut writer, "INSERT INTO mysql.opt_rule_blacklist VALUES ('join_reorder')");
    rows(&mut writer, "ADMIN RELOAD OPT_RULE_BLACKLIST");
    assert_eq!(join(&mut reader), "HashJoin_7");
    // What `BootstrapSession` loads at start.
    stack.factory.pushdown_blacklists.publish_rules(Default::default());
    assert_eq!(join(&mut reader), "HashJoin_9");
    crate::domain::cluster_session_node::load_pushdown_blacklists(&stack.factory).unwrap();
    assert_eq!(join(&mut reader), "HashJoin_7");
}

/// Go `exploreEnforcedPlan` (fix control 46177, default on): with it off, a
/// DataSource whose unenforced best task is valid returns it without pricing
/// the enforced (sorted) plan, so `TestIssue46177`'s queries keep the
/// ordered index scan. Expected plans are a Go tidb-server's.
#[test]
fn fix_control_46177_off_skips_the_enforced_data_source_plan_as_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(46177)).expect("session opens");
    rows(&mut session, "use test");
    rows(
        &mut session,
        "CREATE TABLE sbtest (id int(10) unsigned NOT NULL AUTO_INCREMENT, k int(10) unsigned NOT NULL DEFAULT '0', \
         c char(120) NOT NULL DEFAULT '', pad char(60) NOT NULL DEFAULT '', \
         PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */, KEY k (k))",
    );
    let window = "explain format='brief' select row_number() over(order by a.k) from (select * from sbtest where id<10) a";
    let stream_agg = "explain format='brief' select /*+ stream_agg() */ count(1) from sbtest where id<1 group by k";
    let explain = |session: &mut Session, sql: &str| -> Vec<String> {
        displayed(rows(session, sql)).into_iter().map(|row| row.join("|")).collect()
    };
    assert_eq!(
        explain(&mut session, window),
        [
            "Projection|10.00|root||Column#7->Column#8",
            "└─Window|10.00|root||row_number()->Column#7 over(order by test.sbtest.k rows between current row and current row)",
            "  └─Sort|10.00|root||test.sbtest.k",
            "    └─TableReader|10.00|root||data:TableRangeScan",
            "      └─TableRangeScan|10.00|cop[tikv]|table:sbtest|range:[0,10), keep order:false, stats:pseudo",
        ]
    );
    assert_eq!(
        explain(&mut session, stream_agg),
        [
            "StreamAgg|1.00|root||group by:test.sbtest.k, funcs:count(1)->Column#6",
            "└─Sort|1.00|root||test.sbtest.k",
            "  └─TableReader|1.00|root||data:TableRangeScan",
            "    └─TableRangeScan|1.00|cop[tikv]|table:sbtest|range:[0,1), keep order:false, stats:pseudo",
        ]
    );
    rows(&mut session, "set @@tidb_opt_fix_control='46177:OFF'");
    assert_eq!(
        explain(&mut session, window),
        [
            "Projection|10.00|root||Column#7->Column#8",
            "└─Window|10.00|root||row_number()->Column#7 over(order by test.sbtest.k rows between current row and current row)",
            "  └─IndexReader|10.00|root||index:Selection",
            "    └─Selection|10.00|cop[tikv]||lt(test.sbtest.id, 10)",
            "      └─IndexFullScan|10000.00|cop[tikv]|table:sbtest, index:k(k)|keep order:true, stats:pseudo",
        ]
    );
    assert_eq!(
        explain(&mut session, stream_agg),
        [
            "StreamAgg|1.00|root||group by:test.sbtest.k, funcs:count(Column#7)->Column#6",
            "└─IndexReader|1.00|root||index:StreamAgg",
            "  └─StreamAgg|1.00|cop[tikv]||group by:test.sbtest.k, funcs:count(1)->Column#7",
            "    └─Selection|1.00|cop[tikv]||lt(test.sbtest.id, 1)",
            "      └─IndexFullScan|10000.00|cop[tikv]|table:sbtest, index:k(k)|keep order:true, stats:pseudo",
        ]
    );
}

/// Go `buildDataSource` leaves a for-update read in an explicit transaction
/// its paths (`isForUpdateReadInExplicitTxn`), `findBestTask4LogicalDataSource`
/// filters them by the isolation read engines when TiKV is one, `Optimize`
/// tries the fast plan only then, a point get is a candidate before the
/// store hint skips its path, and a projection asks for an MPP child only
/// while TiFlash is still an isolation read engine. The fixture is a Go
/// testkit capture of the same statements over an available TiFlash
/// replica: each plan row's id, task and lock, and each warning.
#[test]
fn a_for_update_read_plans_over_a_tiflash_replica_like_go() {
    let mut got: Vec<String> = Vec::new();
    let clip = |line: String, width: usize| line.chars().take(width).collect::<String>();
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(4201)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    rows(&mut session, "create table t(id int primary key, a int, key(a))");
    rows(&mut session, "insert into t values (1,1),(2,2)");
    session
        .node_conn()
        .execute_write("ALTER TABLE test.t SET HYPO TIFLASH REPLICA 1")
        .unwrap();
    let mut run = |q: &str| {
        use crate::domain::resultset_source::ResultSetSource;
        let mut result = match session.node_conn().execute(q) {
            Ok(result) => result,
            Err(error) => {
                got.push(clip(format!("{q} => ERR {}", error.message).replace("[planner:1815]", ""), 150));
                return;
            }
        };
        let source = result.source();
        let mut all = Vec::new();
        loop {
            match source.next_batch(8) {
                Ok(batch) if batch.is_empty() => break,
                Ok(batch) => all.extend(batch),
                Err(error) => {
                    got.push(clip(format!("{q} => ERR {}", error.message).replace("[planner:1815]", ""), 150));
                    return;
                }
            }
        }
        let _ = source.finish();
        drop(result);
        if !q.starts_with("explain") {
            got.push(format!("{q} => OK"));
            return;
        }
        let statement = q
            .replace("explain format = 'brief' ", "")
            .replace("/*+ read_from_storage(tiflash[t]) */", "/*H*/");
        for row in displayed(all) {
            let row = row.join(" ");
            let mut fields = row.split_whitespace();
            let id = fields.next().unwrap_or_default();
            let rest = fields
                .filter(|field| {
                    ["cop", "mpp", "batch", "root"].iter().any(|task| field.starts_with(task)) || field.contains("lock")
                })
                .collect::<Vec<_>>()
                .join(" ");
            got.push(format!("   {:<45} {id} {rest}", clip(statement.clone(), 45)));
        }
        for row in displayed(rows(&mut session, "show warnings")) {
            let warning = row.join(" ").replace(['[', ']'], "");
            let warning = warning.split_whitespace().collect::<Vec<_>>().join(" ");
            got.push(format!("    {}", clip(format!("WARN {warning}"), 120)));
        }
    };
    let queries = [
        "explain format = 'brief' select * from t where id = 1 for update",
        "explain format = 'brief' select * from t where a = 1 for update",
        "explain format = 'brief' select * from t for update",
        "explain format = 'brief' select /*+ read_from_storage(tiflash[t]) */ * from t for update",
        "explain format = 'brief' select /*+ read_from_storage(tiflash[t]) */ * from t where id = 1 for update",
        "explain format = 'brief' select /*+ read_from_storage(tiflash[t]) */ * from t where id > 0 for update",
        "explain format = 'brief' select * from t",
    ];
    for engines in ["tikv,tiflash", "tiflash", "tikv"] {
        for mpp in ["0", "1"] {
            run(&format!("set tidb_isolation_read_engines = '{engines}'"));
            run(&format!("set tidb_enforce_mpp = {mpp}"));
            for q in queries {
                run(q);
            }
            run("begin");
            for q in queries {
                run(q);
            }
            run("rollback");
        }
    }
    let want = include_str!("../../../../../testdata/for_update_read_tiflash_go.txt");
    assert_eq!(got.join("\n"), want.trim_end_matches('\n'));
}

/// Go `TestAlternativeEngineRestrictedRounds`' plans
/// (`casetest/mpp/testdata/integration_suite_out.json`): the mixed-engine
/// round-1 plan gives way to the cheaper fully-TiFlash plan when the
/// tikv-only / tiflash-only rounds run, and survives a `READ_FROM_STORAGE`
/// hint or a missing TiFlash replica.
#[test]
fn the_engine_restricted_rounds_pick_gos_plans() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(4202)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    // Go `createEngineRoundTables`.
    rows(&mut session, "create table alt_engine_flash(a int, b int, c int)");
    let flash = (0..5000).map(|i| format!("({}, {i}, {})", i % 50, i % 10)).collect::<Vec<_>>().join(",");
    rows(&mut session, &format!("insert into alt_engine_flash values {flash}"));
    rows(&mut session, "create table alt_engine_kv(a int primary key, b int)");
    let kv = (0..2000).map(|i| format!("({i}, {})", i % 50)).collect::<Vec<_>>().join(",");
    rows(&mut session, &format!("insert into alt_engine_kv values {kv}"));
    rows(&mut session, "create table alt_engine_norep(a int primary key, b int)");
    rows(&mut session, "insert into alt_engine_norep values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)");
    rows(&mut session, "analyze table alt_engine_flash, alt_engine_kv, alt_engine_norep");
    for table in ["alt_engine_flash", "alt_engine_kv"] {
        session
            .node_conn()
            .execute_write(&format!("ALTER TABLE test.{table} SET HYPO TIFLASH REPLICA 1"))
            .unwrap();
    }
    let suites: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../../../../pkg/planner/core/casetest/mpp/testdata/integration_suite_out.json"
    ))
    .unwrap();
    let cases = suites
        .as_array()
        .unwrap()
        .iter()
        .find(|suite| suite["Name"] == "TestAlternativeEngineRestrictedRounds")
        .unwrap()["Cases"]
        .as_array()
        .unwrap();
    let mut failures = Vec::new();
    for case in cases {
        let sql = case["SQL"].as_str().unwrap();
        if sql.starts_with("set ") {
            rows(&mut session, sql);
            continue;
        }
        let got: Vec<String> = displayed(rows(&mut session, sql)).iter().map(|row| row.join(" ")).collect();
        let want: Vec<String> =
            case["Plan"].as_array().into_iter().flatten().map(|line| line.as_str().unwrap().to_owned()).collect();
        if got != want {
            failures.push(format!("{sql}\n  got  {got:#?}\n  want {want:#?}"));
        }
    }
    assert!(failures.is_empty(), "{} of {} cases\n{}", failures.len(), cases.len(), failures.join("\n"));
}

/// Go `getInternalSession`: the internal session a restricted statement runs
/// on copies the issuer's `OptimizerUseInvisibleIndexes`, and its `clean`
/// appends the internal session's warnings to the issuer's statement unless
/// the caller passed `ExecOptionIgnoreWarning`. Rows, plans and warnings are
/// a Go testkit capture over `tk.Session().GetRestrictedSQLExecutor()`.
#[test]
fn a_restricted_statement_inherits_from_and_warns_its_issuer_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1291)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE ri (a INT, b INT, INDEX ia (a) INVISIBLE)")
        .unwrap();
    let source = tidb_txnkv::INTERNAL_TXN_OTHERS;
    let explain = |session: &mut Session| {
        displayed(
            session
                .exec_restricted_sql_rows(source, "explain format='brief' select a from test.ri where a = 1")
                .unwrap(),
        )
        .into_iter()
        .map(|row| row.join("|"))
        .collect::<Vec<_>>()
    };
    assert_eq!(
        explain(&mut session),
        [
            "TableReader|10.00|root||data:Selection",
            "└─Selection|10.00|cop[tikv]||eq(test.ri.a, 1)",
            "  └─TableFullScan|10000.00|cop[tikv]|table:ri|keep order:false, stats:pseudo",
        ]
    );
    rows(&mut session, "set @@tidb_opt_use_invisible_indexes = 1");
    assert_eq!(
        explain(&mut session),
        [
            "IndexReader|10.00|root||index:IndexRangeScan",
            "└─IndexRangeScan|10.00|cop[tikv]|table:ri, index:ia(a)|range:[1,1], keep order:false, stats:pseudo",
        ]
    );
    rows(&mut session, "set @@tidb_opt_use_invisible_indexes = 0");

    let truncating = "select concat(cast('a' as signed))";
    let truncated = vec![vec![
        Datum::new_string_datum("Warning"),
        Datum::UInt(1292),
        Datum::new_string_datum("Truncated incorrect INTEGER value: 'a'"),
    ]];
    assert_eq!(displayed(session.exec_restricted_sql_rows(source, truncating).unwrap()), [["0"]]);
    assert_eq!(rows(&mut session, "SHOW WARNINGS"), truncated);

    rows(&mut session, "select 1");
    let context = tikv_client::util::with_internal_source_type(&tikv_client::trace::TraceContext::new(), source);
    let ignore_warning: tidb_sqlexec::OptionFuncAlias = Arc::new(tidb_sqlexec::exec_option_ignore_warning);
    let (ignored, _) = session
        .restricted_sql_executor()
        .unwrap()
        .exec_restricted_sql(&context, &[ignore_warning], truncating, &[])
        .unwrap();
    assert_eq!(displayed(ignored), [["0"]]);
    session.append_restricted_sql_warnings();
    assert!(rows(&mut session, "SHOW WARNINGS").is_empty());
}

/// Go `withRestrictedSQLExecutor` on a statistics session: the statement
/// runs on that session under `ExecOptionUseCurSession` and on a fresh
/// internal session otherwise, either way under its `ExecOption`
/// (`ExecOptionWithSnapshot`, `GetPartitionPruneModeOption`), which is
/// restored afterwards; an ANALYZE the session issues is an auto analyze
/// because the session is `InRestrictedSQL`. Rows are a Go testkit capture
/// over `statsutil.ExecWithOpts` on a `StatsHandle().SPool()` session.
#[test]
fn a_statistics_session_runs_restricted_sql_under_its_exec_option_like_go() {
    let (stack, _users) = cop_backed_stack();
    let factory = stack.factory;
    let mut client = factory.open_session(session_context(1293)).unwrap();
    rows(&mut client, "USE test");
    rows(&mut client, "CREATE TABLE rs (a INT)");
    rows(&mut client, "INSERT INTO rs VALUES (1)");
    rows(&mut client, "BEGIN");
    rows(&mut client, "SELECT * FROM rs");
    let ts: u64 = displayed(rows(&mut client, "SELECT @@tidb_current_ts"))[0][0]
        .parse()
        .unwrap();
    rows(&mut client, "ROLLBACK");
    rows(&mut client, "UPDATE rs SET a = 2");
    let cur: tidb_sqlexec::OptionFuncAlias = Arc::new(tidb_sqlexec::exec_option_use_cur_session);
    let pool = factory.advanced_sys_session_pool();
    tidb_stats_handle_util::call_with_sctx(
        pool.as_ref(),
        |context| {
            let run = |options: &[tidb_sqlexec::OptionFuncAlias], sql: &str| {
                displayed(tidb_stats_handle_util::exec_with_opts(context, options, sql, &[]).unwrap().0)
                    .into_iter()
                    .map(|row| row.join("|"))
                    .collect::<Vec<_>>()
            };
            let vars =
                "select concat(@@tidb_snapshot), concat(@@tidb_partition_prune_mode), concat(@@tidb_analyze_version)";
            run(&[cur.clone()], "set @x = 7");
            assert_eq!(run(&[cur.clone()], "select concat(@x)"), ["7"]);
            assert_eq!(run(&[], "select concat(@x)"), ["NULL"]);
            assert_eq!(run(&[cur.clone()], vars), ["|dynamic|2"]);
            let snapshot = tidb_sqlexec::exec_option_with_snapshot(ts);
            assert_eq!(run(&[cur.clone(), snapshot.clone()], "select concat(a) from test.rs"), ["1"]);
            assert_eq!(run(&[snapshot], "select concat(a) from test.rs"), ["1"]);
            assert_eq!(run(&[cur.clone()], "select concat(a) from test.rs"), ["2"]);
            assert_eq!(
                run(
                    &[cur.clone(), tidb_sqlexec::get_partition_prune_mode_option("static")],
                    "select concat(@@tidb_partition_prune_mode)"
                ),
                ["static"]
            );
            assert_eq!(run(&[cur.clone()], vars), ["|dynamic|2"]);
            run(&[cur.clone()], "analyze table test.rs");
            Ok(())
        },
        &[],
    )
    .unwrap();
    rows(&mut client, "ANALYZE TABLE rs");
    assert_eq!(
        displayed(rows(
            &mut client,
            "select job_info from mysql.analyze_jobs where table_name = 'rs' order by id"
        )),
        [
            ["auto analyze table all columns with 256 buckets, 100 topn, 1 samplerate"],
            ["analyze table all columns with 256 buckets, 100 topn, 1 samplerate"],
        ]
    );
}

/// Go testkit probe `zzprobe/r01.TestRestrictedCurrentSession`: current
/// execution sees the issuer's uncommitted write/user variables, pool
/// execution sees committed rows, and mutation stays in the issuer.
#[test]
fn test_restricted_current_session() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1791)).unwrap();
    for sql in [
        "USE test",
        "CREATE TABLE r01 (a INT)",
        "INSERT INTO r01 VALUES (1)",
        "SET @x=7",
        "BEGIN",
        "INSERT INTO r01 VALUES (2)",
    ] {
        rows(&mut session, sql);
    }
    let context = tikv_client::util::with_internal_source_type(
        &tikv_client::trace::TraceContext::new(),
        tidb_txnkv::INTERNAL_TXN_OTHERS,
    );
    let cur: tidb_sqlexec::OptionFuncAlias = Arc::new(tidb_sqlexec::exec_option_use_cur_session);
    // Go TestRUV2SessionParserTotalDoesNotLeakAcrossStandaloneParse,
    // "current-session restricted sql restores outer ruv2 metrics".
    let outer_metrics = Arc::new(tidb_execdetails::ruv2_metrics::RUV2Metrics::new());
    session.last_ruv2_metrics = Some(Arc::clone(&outer_metrics));
    {
        use tidb_sqlexec::RestrictedSqlExecutor;
        let exec = session.get_restricted_sql_executor();
        let sql = "select concat(count(*)), concat(@x), database() from test.r01";
        assert_eq!(
            displayed(
                exec.exec_restricted_sql(&context, &[cur.clone()], sql, &[])
                    .unwrap()
                    .0
            ),
            [["2", "7", "test"]]
        );
        assert_eq!(
            displayed(exec.exec_restricted_sql(&context, &[], sql, &[]).unwrap().0),
            [["1", "NULL", "NULL"]]
        );
        exec.exec_restricted_sql(&context, &[cur.clone()], "set @x = %?", &[8_i64.into()])
            .unwrap();
    }
    assert_eq!(session.vars.request_source_type, tidb_txnkv::INTERNAL_TXN_OTHERS);
    assert!(Arc::ptr_eq(session.last_ruv2_metrics.as_ref().unwrap(), &outer_metrics));
    assert!(!outer_metrics.bypass());
    rows(&mut session, "CREATE TEMPORARY TABLE r01_temp (a INT)");
    rows(&mut session, "INSERT INTO r01_temp VALUES (9)");
    assert_eq!(
        displayed(
            session
                .execute_restricted_sql(&context, &[cur.clone()], "select a from r01_temp", &[])
                .unwrap()
                .0
        ),
        [["9"]]
    );
    assert!(!session.vars.restricted_sql);
    assert_eq!(displayed(rows(&mut session, "select @x")), [["8"]]);
    // The Go probe executes SHOW while the issuer still owns its write.
    // Its nested restricted SELECT must leave that transaction open.
    assert_eq!(
        displayed(rows(&mut session, "SHOW TABLE STATUS LIKE 'r01'"))[0][0],
        "r01"
    );
    rows(&mut session, "ROLLBACK");
    assert_eq!(displayed(rows(&mut session, "select * from r01")), [["1"]]);
    rows(&mut session, "SET sql_mode='ANSI_QUOTES'");
    {
        use tidb_sqlexec::RestrictedSqlExecutor;
        let executor = session.get_restricted_sql_executor();
        let statement = executor.parse_with_params(&context, "SELECT \"a\" FROM r01", &[]).unwrap();
        let tidb_ast::Stmt::Query(query) = statement else { panic!("SELECT") };
        let tidb_ast::QueryStmt::Select(select) = query.as_ref() else { panic!("SELECT") };
        assert!(matches!(&select.fields.fields()[0], tidb_ast::SelectField::Expr {
            expr: tidb_ast::Expr::Column(parts), ..
        } if parts == &["a"]));
    }
    rows(&mut session, "SET sql_mode=DEFAULT");
    let options = [
        cur.clone(),
        tidb_sqlexec::get_partition_prune_mode_option("static"),
        tidb_sqlexec::get_analyze_snapshot_option(false),
    ];
    assert!(session
        .execute_restricted_sql(&context, &options, "select * from test.absent_r01", &[])
        .is_err());
    assert_eq!(
        displayed(rows(&mut session, "select @@tidb_partition_prune_mode")),
        [["dynamic"]]
    );
    assert!(!session.vars.restricted_sql);
    assert_eq!(
        displayed(rows(&mut session, "SHOW TABLE STATUS LIKE 'r01'"))[0][0],
        "r01"
    );
}

/// Go `pessimistictest.TestPlanCacheSchemaChange`'s text-protocol half over
/// unistore: Go `ResetContextOfStmt` classifies an `EXECUTE` by its prepared
/// statement, so a prepared UPDATE in a pessimistic transaction reads at the
/// for-update timestamp (`executorBuilder.getSnapshotTS`), cached plan or
/// not, and sees the rows another session committed after the transaction
/// began. Read at the start timestamp, the cached `EXECUTE` matched no row.
#[test]
fn a_prepared_update_reads_at_the_for_update_ts_like_go() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut tmp = factory.open_session(session_context(2101)).expect("session");
    tmp.node_conn()
        .execute_write("set global tidb_enable_prepared_plan_cache=ON")
        .expect("set")
        .expect("set");
    let mut tk = factory.open_session(session_context(2102)).expect("session");
    let mut tk3 = factory.open_session(session_context(2103)).expect("session");
    for session in [&mut tk, &mut tk3] {
        session.node_conn().execute_write("USE test").expect("use").expect("use");
    }
    let write = |session: &mut Session, sql: &str| {
        session.node_conn().execute_write(sql).expect(sql).expect(sql).affected_rows
    };
    write(&mut tk, "create table t (id int primary key, v int, unique index iv (v), vv int)");
    write(&mut tk, "insert into t values(1, 1, 1), (2, 2, 2), (4, 4, 4)");
    write(&mut tk, "prepare update_stmt from 'update t set vv = vv + 1 where v = ?'");
    write(&mut tk, "set @v = 1");
    write(&mut tk, "execute update_stmt using @v");
    tk.node_conn().control_transaction("BEGIN PESSIMISTIC").expect("begin");
    write(&mut tk3, "alter table t drop index iv");
    write(&mut tk3, "update t set v = 3 where v = 2");
    write(&mut tk3, "update t set v = 5 where v = 4");
    write(&mut tk, "set @v = 2");
    assert_eq!(write(&mut tk, "execute update_stmt using @v"), 0);
    assert_eq!(displayed(rows(&mut tk, "select @@last_plan_from_cache")), [["0"]]);
    write(&mut tk, "set @v = 3");
    assert_eq!(write(&mut tk, "execute update_stmt using @v"), 1);
    assert_eq!(displayed(rows(&mut tk, "select @@last_plan_from_cache")), [["1"]]);
    tk.node_conn().control_transaction("COMMIT").expect("commit");
    assert_eq!(
        displayed(rows(&mut tk, "select * from t order by id")),
        [["1", "1", "2"], ["2", "3", "3"], ["4", "5", "4"]]
    );
}

/// Go `ExecuteInternal` sets `InRestrictedSQL` for its statement and restores
/// it: an internal statement on a user session is restricted SQL (no user
/// statement sample), and the session is a user session again afterwards.
#[test]
fn execute_internal_runs_restricted_on_a_user_session() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = stack.factory.open_session(session_context(91)).expect("session opens");
    let group = tk.active_resource_group().to_owned();
    let select = || {
        tidb_metrics::server::QUERY_DURATION_HISTOGRAM
            .with_label_values(&["Select", "", &group])
            .get_sample_count()
    };
    let context = tikv_client::util::with_internal_source_type(
        &tikv_client::trace::TraceContext::new(),
        tidb_txnkv::INTERNAL_TXN_OTHERS,
    );
    assert!(!tk.vars.restricted_sql);
    let before = select();
    crate::domain::cluster_session_node::execute_internal_under(&mut tk, &context, "select 1").unwrap();
    assert_eq!(select(), before);
    assert!(!tk.vars.restricted_sql);
    rows(&mut tk, "select 1");
    assert_eq!(select(), before + 1);
}
