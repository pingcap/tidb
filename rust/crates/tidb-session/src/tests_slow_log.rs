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

//! Go `ExecStmt.LogSlowQuery`, `logGeneralQuery` and `recordLastQueryInfo`
//! end to end: statements write the slow log and the general log through the
//! process loggers, `SLOW_QUERY` reads this node's own slow log back, `ADMIN
//! SHOW SLOW` lists what the slow log selected, and `@@tidb_last_query_info`
//! follows the statements.
//!
//! Each test installs the process loggers over its own files; nextest runs
//! every test in its own process, so they do not share them.

use crate::tests_support::row_text;
use crate::Session;

fn one(session: &mut Session, sql: &str) -> String {
    row_text(session.run(sql))
        .into_iter()
        .next()
        .and_then(|row| row.into_iter().next())
        .unwrap_or_default()
}

/// A fresh directory holding this test's log files.
fn log_dir(test: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "tidb-session-slow-log-{test}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Go `logutil.InitLogger` with dedicated slow and general log files.
fn install_loggers(dir: &std::path::Path) -> (String, String) {
    let slow = dir.join("tidb-slow.log").to_string_lossy().into_owned();
    let general = dir.join("tidb-general.log").to_string_lossy().into_owned();
    let file = tidb_util::logutil::FileLogConfig {
        filename: dir.join("tidb.log").to_string_lossy().into_owned(),
        max_size: 300,
        ..Default::default()
    };
    let config = tidb_util::logutil::new_log_config(
        "info",
        tidb_util::logutil::DEFAULT_LOG_FORMAT,
        &slow,
        &general,
        file,
        false,
    );
    tidb_util::logutil::init_logger(&config).unwrap();
    (slow, general)
}

fn read(path: &str) -> String {
    let _ = tidb_util::logutil::sync();
    std::fs::read_to_string(path).unwrap_or_default()
}

/// A statement over `tidb_slow_log_threshold` is written to the slow log in
/// Go's entry format, and `SLOW_QUERY` parses this node's own file back.
#[test]
fn slow_query_reads_back_the_nodes_own_slow_log() {
    let dir = log_dir("read-back");
    let (slow, _) = install_loggers(&dir);
    let mut session = Session::new();
    session
        .run("SET GLOBAL tidb_slow_log_threshold = 0")
        .unwrap();
    session
        .run("CREATE TABLE slow_t (a INT PRIMARY KEY, b INT)")
        .unwrap();
    session
        .run("INSERT INTO slow_t VALUES (1, 10), (2, 20)")
        .unwrap();
    session.run("SELECT b FROM slow_t WHERE a = 2").unwrap();

    let log = read(&slow);
    let entry_start = log
        .rfind("# Time: ")
        .expect("the SELECT wrote a slow-log entry");
    let entry = &log[entry_start..];
    let fields: Vec<&str> = entry
        .lines()
        .filter_map(|line| line.strip_prefix("# "))
        .map(|line| line.split(':').next().unwrap_or_default())
        .collect();
    // Go `SlowLogFormat`'s order for a statement with no user, connection,
    // cop task or plan text.
    let expected_prefix = [
        "Time",
        "Txn_start_ts",
        "Query_time",
        "Parse_time",
        "Compile_time",
        "Rewrite_time",
        "Optimize_time",
        "Wait_TS",
        "DB",
    ];
    assert_eq!(&fields[..expected_prefix.len()], expected_prefix, "{entry}");
    assert!(entry.contains("# Is_internal: false\n"), "{entry}");
    assert!(entry.contains("# Succ: true\n"), "{entry}");
    assert!(entry.contains("# Result_rows: 1\n"), "{entry}");
    assert!(
        entry.ends_with("SELECT b FROM slow_t WHERE a = 2;\n"),
        "{entry}"
    );

    session
        .run(&format!("SET tidb_slow_query_file = '{slow}'"))
        .unwrap();
    let rows = row_text(session.run(
        "SELECT query, db, succ, result_rows, is_internal, digest = '' \
         FROM information_schema.slow_query WHERE query LIKE 'SELECT b FROM slow_t%'",
    ));
    assert_eq!(
        rows,
        vec![vec![
            "SELECT b FROM slow_t WHERE a = 2;".to_owned(),
            "test".to_owned(),
            "1".to_owned(),
            "1".to_owned(),
            "0".to_owned(),
            "0".to_owned(),
        ]]
    );
    // The digest column carries the statement's digest.
    let digest = one(
        &mut session,
        "SELECT digest FROM information_schema.slow_query WHERE query LIKE 'SELECT b FROM slow_t%'",
    );
    assert_eq!(
        digest,
        tidb_parser::normalize_digest("SELECT b FROM slow_t WHERE a = 2")
            .1
            .to_string()
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Go's triggers: the threshold (no entry under it), `tidb_enable_slow_log`
/// (no entry when off), and `tidb_slow_log_rules` (rules replace the
/// threshold).
#[test]
fn slow_log_triggers_follow_threshold_switch_and_rules() {
    let dir = log_dir("triggers");
    let (slow, _) = install_loggers(&dir);
    let mut session = Session::new();
    session
        .run("SET GLOBAL tidb_slow_log_threshold = 100000")
        .unwrap();
    session.run("SELECT 'under threshold'").unwrap();
    assert!(!read(&slow).contains("under threshold"));

    session
        .run("SET GLOBAL tidb_slow_log_threshold = 0")
        .unwrap();
    session
        .run("SET GLOBAL tidb_enable_slow_log = OFF")
        .unwrap();
    session.run("SELECT 'slow log off'").unwrap();
    assert!(!read(&slow).contains("slow log off"));

    session.run("SET GLOBAL tidb_enable_slow_log = ON").unwrap();
    session.run("SELECT 'slow log on'").unwrap();
    assert!(read(&slow).contains("SELECT 'slow log on';"));

    // With rules, the threshold no longer decides: `succ:false` selects only
    // failed statements.
    session
        .run("SET tidb_slow_log_rules = 'succ:false'")
        .unwrap();
    session.run("SELECT 'rule skips me'").unwrap();
    assert!(!read(&slow).contains("rule skips me"));
    session
        .run("CREATE TABLE rule_t (a INT PRIMARY KEY)")
        .unwrap();
    session.run("INSERT INTO rule_t VALUES (1)").unwrap();
    assert!(session
        .run("INSERT INTO rule_t VALUES (1) /* dup */")
        .is_err());
    let log = read(&slow);
    assert!(
        log.contains("INSERT INTO rule_t VALUES (1) /* dup */;"),
        "{log}"
    );
    assert!(log.contains("# Succ: false\n"), "{log}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Go `domain.LogSlowQuery`: a statement the slow log selected lands in the
/// `ADMIN SHOW SLOW` memory with its digest, database and success.
#[test]
fn admin_show_slow_lists_statements_the_slow_log_selected() {
    let dir = log_dir("admin-show-slow");
    let _ = install_loggers(&dir);
    let mut session = Session::new();
    session
        .run("SET GLOBAL tidb_slow_log_threshold = 0")
        .unwrap();
    session.run("SELECT 'admin show slow probe'").unwrap();
    let rows = row_text(session.run("ADMIN SHOW SLOW RECENT 10"));
    let row = rows
        .iter()
        // Go keeps `SlowQueryLogItems.SQL`, without the log's `;`.
        .find(|row| row[0] == "SELECT 'admin show slow probe'")
        .expect("the statement is in the recent list");
    // SUCC, DB, INTERNAL and DIGEST.
    assert_eq!(row[4], "1");
    assert_eq!(row[8], "test");
    assert_eq!(row[11], "0");
    assert_eq!(
        row[12],
        tidb_parser::normalize_digest("SELECT 'admin show slow probe'")
            .1
            .to_string()
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Go `logGeneralQuery`: with `tidb_general_log` on, each statement is one
/// `GENERAL_LOG` entry on the general logger; off, none.
#[test]
fn general_log_writes_each_statement_while_on() {
    let dir = log_dir("general");
    let (_, general) = install_loggers(&dir);
    let mut session = Session::new();
    session.run("SELECT 'general log off'").unwrap();
    assert!(!read(&general).contains("general log off"));

    session.run("SET GLOBAL tidb_general_log = ON").unwrap();
    session.run("SELECT 'general\tlog\non'").unwrap();
    let log = read(&general);
    let line = log
        .lines()
        .find(|line| line.contains("GENERAL_LOG") && line.contains("SELECT 'general"))
        .expect("a GENERAL_LOG entry for the SELECT");
    for field in [
        "[conn=",
        "[session_alias=",
        "[user=",
        "[schemaVersion=",
        "[txnStartTS=",
        "[forUpdateTS=",
        "[isReadConsistency=false]",
        "[currentDB=test]",
        "[isPessimistic=false]",
        "[sessionTxnMode=PESSIMISTIC]",
    ] {
        assert!(line.contains(field), "{field} in {line}");
    }
    // Go spells a literal holding a tab or newline in hex in the
    // statement's `Text()`, and logs the original bytes, quoted, as
    // `originText` (captured from Go's own GENERAL_LOG line).
    assert!(
        line.contains("[sql=\"SELECT 0x67656e6572616c096c6f670a6f6e\"]"),
        "{line}"
    );
    assert!(
        line.contains("[originText=\"\\\"SELECT 'general\\\\tlog\\\\non'\\\"\"]"),
        "{line}"
    );

    // Crucial statements go to the background log instead.
    session.run("CREATE TABLE general_t (a INT)").unwrap();
    assert!(!read(&general).contains("CREATE TABLE general_t"));
    let _ = std::fs::remove_dir_all(&dir);
}

/// Go `recordLastQueryInfo`: a DML statement (and a SELECT) records the
/// transaction timestamps, its RU and, when it failed, its error; a
/// statement that is neither, or that failed to compile, leaves the value.
#[test]
fn last_query_info_follows_statements_and_failures() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE lqi_t (a INT PRIMARY KEY)")
        .unwrap();
    session.run("INSERT INTO lqi_t VALUES (1)").unwrap();
    let after_insert = one(&mut session, "SELECT @@tidb_last_query_info");
    let parsed: serde_json::Value = serde_json::from_str(&after_insert).unwrap();
    assert_eq!(parsed["txn_scope"], "global");
    assert!(parsed.get("error").is_none(), "{after_insert}");
    assert_eq!(
        parsed["for_update_ts"], parsed["start_ts"],
        "{after_insert}"
    );

    assert!(session.run("INSERT INTO lqi_t VALUES (1)").is_err());
    // SET is not recorded: the failure's record survives it.
    session.run("SET @x = 1").unwrap();
    let after_failure = one(&mut session, "SELECT @@tidb_last_query_info");
    let parsed: serde_json::Value = serde_json::from_str(&after_failure).unwrap();
    assert_eq!(
        parsed["error"], "[kv:1062]Duplicate entry '1' for key 'lqi_t.PRIMARY'",
        "{after_failure}"
    );

    // `SELECT @@...` is itself a SELECT and replaced the failure's record.
    let after_select = one(&mut session, "SELECT @@tidb_last_query_info");
    assert!(!after_select.contains("\"error\""), "{after_select}");

    // A statement that fails to compile is never finished.
    assert!(session.run("INSERT INTO lqi_t VALUES (1)").is_err());
    assert!(session.run("SELECT * FROM no_such_table").is_err());
    assert!(one(&mut session, "SELECT @@tidb_last_query_info").contains("\"error\""));
}

/// The last slow-log entry whose statement line is `sql;`.
fn entry_for(log: &str, sql: &str) -> String {
    let statement = format!("\n{sql};\n");
    let end = log
        .rfind(&statement)
        .unwrap_or_else(|| panic!("no slow-log entry for {sql}: {log}"))
        + statement.len();
    let start = log[..end].rfind("# Time: ").expect("the entry's header");
    log[start..end].to_owned()
}

/// The value of `# key: value`, or of `key: value` inside a line carrying
/// several (`Optimize_time: .. Opt_logical: ..`).
fn field<'a>(entry: &'a str, key: &str) -> Option<&'a str> {
    let needle = format!("{key}: ");
    entry.lines().find_map(|line| {
        let line = line.strip_prefix("# ")?;
        let at = line
            .match_indices(&needle)
            .find(|(at, _)| *at == 0 || line[..*at].ends_with(' '))?
            .0;
        let value = &line[at + needle.len()..];
        Some(value.split(' ').next().unwrap_or(value))
    })
}

fn seconds(entry: &str, key: &str) -> f64 {
    field(entry, key)
        .unwrap_or_else(|| panic!("{key} in {entry}"))
        .parse()
        .unwrap_or_else(|_| panic!("{key} is a number in {entry}"))
}

/// Go `SessionVars.RewritePhaseInfo`, `DurationOptimizer` and the statement
/// ledger of used statistics: an optimized SELECT reports a nonzero rewrite,
/// optimization and each optimizer phase, bounded by its compile time; an
/// uncorrelated subquery is counted and timed as a pre-processed subquery;
/// and `Stats` names every table planning read statistics for.
#[test]
fn slow_log_reports_planning_phases_and_used_stats() {
    let dir = log_dir("planning-phases");
    let (slow, _) = install_loggers(&dir);
    let mut session = Session::new();
    session
        .run("SET GLOBAL tidb_slow_log_threshold = 0")
        .unwrap();
    session
        .run("CREATE TABLE phase_t (a INT PRIMARY KEY, b INT, KEY ib (b))")
        .unwrap();
    session
        .run("INSERT INTO phase_t VALUES (1, 10), (2, 20)")
        .unwrap();
    let sql = "SELECT a FROM phase_t WHERE b > 5 AND a < (SELECT MAX(a) FROM phase_t)";
    session.run(sql).unwrap();

    let entry = entry_for(&read(&slow), sql);
    let compile = seconds(&entry, "Compile_time");
    let rewrite = seconds(&entry, "Rewrite_time");
    let optimize = seconds(&entry, "Optimize_time");
    assert!(rewrite > 0.0 && rewrite <= compile, "{entry}");
    assert!(optimize > 0.0 && optimize <= compile, "{entry}");
    for phase in ["Opt_logical", "Opt_physical", "Opt_stats_derive"] {
        let value = seconds(&entry, phase);
        assert!(value > 0.0 && value <= optimize, "{phase} in {entry}");
    }
    // `StatsDerive` is the first part of `PhysicalOpt`.
    assert!(
        seconds(&entry, "Opt_stats_derive") <= seconds(&entry, "Opt_physical"),
        "{entry}"
    );
    // The subquery ran while the outer query was being built.
    assert_eq!(field(&entry, "Preproc_subqueries"), Some("1"), "{entry}");
    assert!(
        seconds(&entry, "Preproc_subqueries_time") <= rewrite,
        "{entry}"
    );
    // Go `initStats`: never analyzed, so pseudo statistics with
    // `PseudoRowCount` rows.
    assert!(
        entry.contains(
            "# Stats: phase_t:stats_meta_version=pseudo[realtime_count=10000;modify_count=0]\n"
        ),
        "{entry}"
    );
    // Go `Session.Parse` counts the statement's parse into its RU v2.
    assert!(
        field(&entry, "Request_unit_v2_detail").is_some()
            && entry.contains("session_parser_total:1"),
        "{entry}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Go `GetTblInfoForUsedStatsByPhysicalID`: a partitioned table's own id is
/// `<table> global`, each statically pruned partition `<table> <partition>`.
#[test]
fn slow_log_stats_names_partitions_like_go() {
    let dir = log_dir("partition-stats");
    let (slow, _) = install_loggers(&dir);
    let mut session = Session::new();
    session
        .run("SET GLOBAL tidb_slow_log_threshold = 0")
        .unwrap();
    session
        .run("CREATE TABLE part_t (a INT, b INT) PARTITION BY HASH(a) PARTITIONS 2")
        .unwrap();
    session
        .run("SET tidb_partition_prune_mode = 'dynamic'")
        .unwrap();
    let dynamic = "SELECT b FROM part_t WHERE b > 1";
    session.run(dynamic).unwrap();
    session
        .run("SET tidb_partition_prune_mode = 'static'")
        .unwrap();
    let static_sql = "SELECT b FROM part_t WHERE b > 2";
    session.run(static_sql).unwrap();

    let log = read(&slow);
    let dynamic_entry = entry_for(&log, dynamic);
    assert!(
        dynamic_entry.contains(
            "# Stats: part_t global:stats_meta_version=pseudo[realtime_count=10000;modify_count=0]\n"
        ),
        "{dynamic_entry}"
    );
    let static_entry = entry_for(&log, static_sql);
    assert!(
        static_entry.contains(
            "# Stats: part_t p0:stats_meta_version=pseudo[realtime_count=10000;modify_count=0],\
             part_t p1:stats_meta_version=pseudo[realtime_count=10000;modify_count=0]\n"
        ),
        "{static_entry}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Go `ResetContextOfStmt` resets the session memory tracker's peak, so a
/// statement's `Mem_max` is its own, not the connection's.
#[test]
fn slow_log_mem_max_is_the_statements_peak() {
    let dir = log_dir("mem-max");
    let (slow, _) = install_loggers(&dir);
    let mut session = Session::new();
    session
        .run("CREATE TABLE mem_t (a INT PRIMARY KEY, b VARCHAR(64))")
        .unwrap();
    let values = (1..=300)
        .map(|i| format!("({i}, REPEAT('x', 60))"))
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!("INSERT INTO mem_t VALUES {values}"))
        .unwrap();
    session
        .run("SET GLOBAL tidb_slow_log_threshold = 0")
        .unwrap();
    let heavy = "SELECT b FROM mem_t ORDER BY b DESC, a";
    session.run(heavy).unwrap();
    let light = "SELECT 'light'";
    session.run(light).unwrap();

    let log = read(&slow);
    let heavy_max: i64 = field(&entry_for(&log, heavy), "Mem_max")
        .expect("the sort used memory")
        .parse()
        .unwrap();
    let light_entry = entry_for(&log, light);
    let light_max = field(&light_entry, "Mem_max").map_or(0, |value| value.parse::<i64>().unwrap());
    assert!(
        light_max < heavy_max,
        "{light_max} < {heavy_max}: {light_entry}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Go `FinishExecuteStmt(origTxnCtx.StartTS, ...)` and `TxnCtx.IsExplicit`:
/// the slow-log entry of a statement carries the transaction context in force
/// when it began, which outlives a `COMMIT`/`ROLLBACK` that ends it and is not
/// the one a `BEGIN` opens. Captured on a Go tidb-server over TiKV
/// (`Txn_start_ts` / `IsExplicitTxn`): `begin` 0/true, the statements and
/// the `commit` the transaction's start/true, an implicit-commit `begin` the
/// ended transaction's start/true, and a `commit` with no transaction 0/false.
#[test]
fn slow_log_keeps_the_transaction_a_statement_ended() {
    let dir = log_dir("txn-context");
    let (slow, _) = install_loggers(&dir);
    let mut session = Session::new();
    session
        .run("SET GLOBAL tidb_slow_log_threshold = 0")
        .unwrap();
    session
        .run("CREATE TABLE slow_txn (a INT PRIMARY KEY)")
        .unwrap();
    for sql in [
        "BEGIN",
        "INSERT INTO slow_txn VALUES (1)",
        "COMMIT",
        "BEGIN",
        "INSERT INTO slow_txn VALUES (2)",
        "ROLLBACK",
        "BEGIN",
        "INSERT INTO slow_txn VALUES (3)",
        "BEGIN",
        "COMMIT",
        "COMMIT",
    ] {
        session.run(sql).unwrap();
    }
    let log = read(&slow);
    let entries: Vec<(u64, String, String)> = log
        .split("# Time: ")
        .skip(1)
        .map(|entry| {
            let field = |name: &str| {
                entry
                    .lines()
                    .find_map(|line| line.strip_prefix(&format!("# {name}: ")))
                    .unwrap_or_default()
                    .to_owned()
            };
            let query = entry.lines().last().unwrap_or_default().to_owned();
            (
                field("Txn_start_ts").parse().unwrap(),
                field("IsExplicitTxn"),
                query,
            )
        })
        .filter(|(_, _, query)| {
            query.starts_with("BEGIN")
                || query.starts_with("INSERT INTO slow_txn")
                || query.starts_with("COMMIT")
                || query.starts_with("ROLLBACK")
        })
        .collect();
    let explicit: Vec<&str> = entries.iter().map(|(_, e, _)| e.as_str()).collect();
    assert_eq!(
        explicit,
        ["true", "true", "true", "true", "true", "true", "true", "true", "true", "true", "false"],
        "{entries:?}"
    );
    let ts: Vec<u64> = entries.iter().map(|(ts, _, _)| *ts).collect();
    assert_eq!(ts[0], 0, "BEGIN: {entries:?}");
    assert_ne!(ts[1], 0, "{entries:?}");
    assert_eq!(ts[2], ts[1], "COMMIT keeps its transaction: {entries:?}");
    assert_eq!(ts[3], 0, "{entries:?}");
    assert_ne!(ts[4], 0, "{entries:?}");
    assert_eq!(ts[5], ts[4], "ROLLBACK keeps its transaction: {entries:?}");
    assert_eq!(ts[6], 0, "{entries:?}");
    assert_ne!(ts[7], 0, "{entries:?}");
    assert_eq!(
        ts[8], ts[7],
        "an implicit commit keeps the ended transaction: {entries:?}"
    );
    assert_ne!(ts[9], 0, "{entries:?}");
    assert_ne!(ts[9], ts[7], "the BEGIN's own transaction: {entries:?}");
    assert_eq!(ts[10], 0, "no transaction: {entries:?}");
}
