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

//! The `mysql` schema as an OBJECT: what selecting it does, and what naming
//! its tables does.
//!
//! Mirrors the schemas Go's `pkg/session/bootstrap.go` creates, as seen
//! through `tidb_executor::Catalog::default`. Every assertion below was
//! CAPTURED from a real TiDB over `rust/difftests/gorun` before it was
//! written -- the case-preserving `DATABASE()`, the failed `USE` leaving the
//! session where it was, and the 61-table `SHOW TABLES` -- because two of
//! them are the opposite of what the obvious guess would be.

use crate::tests_support::*;
use crate::*;

/// The gap this module exists for: `USE mysql` must SUCCEED.
///
/// Captured from Go:
///
/// ```text
/// select database();  -> test
/// use mysql;          -> OK
/// select database();  -> mysql
/// ```
///
/// It is one statement, but a `USE` that fails is not one wrong answer. The
/// session stays on the previous schema, so every later unqualified name
/// resolves there -- the statement is accepted-then-discarded and the
/// statements behind it silently answer against the wrong database. The
/// classified `executor/admin` divergence was exactly that: `admin check
/// table t` after a refused `use mysql` checked the `t` of the PREVIOUS
/// schema and reported success where TiDB reports 1146.
#[test]
fn use_mysql_selects_the_system_schema() {
    let mut session = Session::new();
    assert_eq!(
        scalar_text(&mut session, "SELECT DATABASE()").unwrap(),
        "test"
    );

    session.run("USE mysql").unwrap();
    assert_eq!(session.current_database(), "mysql");
    assert_eq!(
        scalar_text(&mut session, "SELECT DATABASE()").unwrap(),
        "mysql"
    );
}

/// Schema names match case-insensitively, and `DATABASE()` reports the name
/// AS WRITTEN, not the catalog's stored spelling.
///
/// Captured from Go:
///
/// ```text
/// use MySQL;          -> OK
/// select database();  -> MySQL
/// ```
///
/// The written case is the surprising half. Go's `USE` resolves against the
/// lower form and then stores what the user typed in `SessionVars.CurrentDB`,
/// so `DATABASE()` echoes `MySQL` rather than normalising to `mysql`.
#[test]
fn use_matches_the_schema_name_case_insensitively_and_keeps_what_was_written() {
    let mut session = Session::new();
    session.run("USE MySQL").unwrap();
    assert_eq!(
        scalar_text(&mut session, "SELECT DATABASE()").unwrap(),
        "MySQL"
    );

    session.run("USE MYSQL").unwrap();
    assert_eq!(
        scalar_text(&mut session, "SELECT DATABASE()").unwrap(),
        "MYSQL"
    );
}

/// A `USE` of a name that really does not exist fails AND leaves the session
/// on the schema it was already using.
///
/// Captured from Go:
///
/// ```text
/// use MySQL;          -> OK
/// select database();  -> MySQL
/// use nosuchdb;       -> ERR   (1049 Unknown database 'nosuchdb')
/// select database();  -> MySQL
/// ```
///
/// This pins the half of the `USE` contract that is NOT a bug. It is tempting
/// to read "a failed `USE` leaves the session pointed at the old schema" as
/// the defect, but Go does exactly that; the defect was only ever that
/// `mysql` was not a name that existed. Anything that "fixed" the retention
/// would diverge from TiDB.
#[test]
fn a_failed_use_leaves_the_session_on_its_previous_schema() {
    let mut session = Session::new();
    session.run("USE MySQL").unwrap();

    let error = session.run("USE nosuchdb").unwrap_err().to_mysql_error();
    assert_eq!(error.code, 1049);
    assert_eq!(error.message, "Unknown database 'nosuchdb'");

    assert_eq!(
        scalar_text(&mut session, "SELECT DATABASE()").unwrap(),
        "MySQL"
    );
}

/// The whole point of the object being EMPTY: naming a bootstrap table
/// REFUSES. Every one of them is absent, and no absent table can answer
/// emptily -- which is what serving a fabricated zero-row `mysql.user` would
/// have done to every privilege query in the corpus.
///
/// Go serves all of these for real (captured: `select count(*) from
/// mysql.user` answers 1, `mysql.tidb` answers 6), so this is a refusal, not
/// parity.
///
/// The errno is where it gets interesting, and the two arms below disagree
/// on purpose:
///
/// * `ADMIN CHECK TABLE` refuses with Go's own **1146**
///   `Table 'mysql.user' doesn't exist`, because `admin_check_arm` resolves
///   through `SchemaErrorKind::UnknownTable`. That is the arm the classified
///   `executor/admin` divergence ran through, which is why selecting the
///   schema is enough to close it.
/// * `SELECT` refuses with **1146** too, as Go does. It answered a generic
///   1105 until the planner's table lookup
///   (`tidb_executor::driver::from` and the DML paths beside it) was moved
///   onto `SchemaErrorKind::UnknownTable`; that divergence was pinned here
///   rather than approved, and is now closed.
///
/// FLIPS TO SUPPORT when the `mysql.*` bootstrap tables are ported into this
/// tier: each name below then has to return rows, and this test is the list
/// of what to convert.
#[test]
fn the_bootstrap_tables_are_refused_by_name() {
    let mut session = Session::new();
    session.run("USE mysql").unwrap();

    // FLIPPED TO SUPPORT: `mysql.user` is a real bootstrapped table now
    // (`crate::bootstrap` runs Go `metadef.CreateUserTable` plus
    // `doDMLWorks`' root row, and `crate::user_table` keeps it written by
    // the account statements). Go's captured `select count(*) from
    // mysql.user` answers 1 on a fresh cluster -- the bootstrap root row --
    // and so does this tier.
    assert_eq!(
        row_text(session.run("SELECT count(*) FROM mysql.user")),
        [["1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT Host, User, plugin FROM user")),
        [["%", "root", "mysql_native_password"]]
    );

    // A sample across the families Go's `show tables` in `mysql` lists:
    // privileges, TiDB's own metadata, and statistics.
    for table in ["db", "tables_priv", "global_priv", "tidb", "stats_meta"] {
        let error = session
            .run(&format!("ADMIN CHECK TABLE {table}"))
            .unwrap_err()
            .to_mysql_error();
        assert_eq!(
            error.code, 1146,
            "unqualified `{table}` in the mysql schema should be 1146"
        );
        assert_eq!(
            error.message,
            format!("Table 'mysql.{table}' doesn't exist")
        );

        // Both spellings of the name reach the same lookup, and both now
        // report Go's own ErrTableNotExists.
        for sql in [
            format!("SELECT * FROM {table}"),
            format!("SELECT * FROM mysql.{table}"),
        ] {
            let error = session.run(&sql).unwrap_err().to_mysql_error();
            assert_eq!(error.code, 1146, "`{sql}` should refuse");
            assert_eq!(
                error.message,
                format!("Table 'mysql.{table}' doesn't exist")
            );
        }
    }
}

/// `mysql` is listed among the schemas, since it now is one.
///
/// Captured from Go, `select schema_name from information_schema.schemata`:
/// `INFORMATION_SCHEMA;METRICS_SCHEMA;PERFORMANCE_SCHEMA;mysql;sys;test`.
/// This tier lists the three of those six it has -- `METRICS_SCHEMA`,
/// `PERFORMANCE_SCHEMA` and `sys` are absent, a documented divergence on
/// `Catalog::default` -- with `INFORMATION_SCHEMA` first, which is the
/// ordering Go's `fetchShowDatabases` imposes.
#[test]
fn the_system_schema_is_listed_among_the_databases() {
    let mut session = Session::new();
    let names: Vec<String> = row_text(session.run("SHOW DATABASES"))
        .into_iter()
        .map(|row| row[0].clone())
        .collect();
    assert_eq!(names, vec!["INFORMATION_SCHEMA", "mysql", "test"]);

    let names: Vec<String> = row_text(
        session.run("SELECT SCHEMA_NAME FROM information_schema.schemata ORDER BY SCHEMA_NAME"),
    )
    .into_iter()
    .map(|row| row[0].clone())
    .collect();
    assert_eq!(names, vec!["INFORMATION_SCHEMA", "mysql", "test"]);
}

/// DIVERGENCE, pinned: enumerating `mysql` under-reports.
///
/// Captured from Go, `use mysql; show tables;` returns 61 names --
/// `advisory_locks` through `user`. This tier returns the THREE it stores,
/// bootstrapped by `crate::bootstrap`. Under-reporting an enumeration is the
/// price of refusing every absent name in it (see
/// [`the_bootstrap_tables_are_refused_by_name_with_1146`]); the alternative,
/// fabricating empty tables so the count looks right, would turn a loud 1146
/// into a silent zero-row answer.
///
/// FLIPS TO SUPPORT as the bootstrap tables land: this count rises toward 61.
/// It has risen twice so far -- `bind_info` for GLOBAL bindings, then the two
/// blacklist tables `ADMIN RELOAD` reads (`crate::blacklist`) -- and each
/// arrival is a feature that needed the table, not a name added to make the
/// count look better.
#[test]
fn enumerating_the_system_schema_under_reports() {
    let mut session = Session::new();
    session.run("USE mysql").unwrap();
    let stored = [
        ["bind_info"],
        ["expr_pushdown_blacklist"],
        ["opt_rule_blacklist"],
        ["user"],
    ];
    assert_eq!(row_text(session.run("SHOW TABLES")), stored);

    assert_eq!(
        row_text(
            session.run(
                "SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = 'mysql'"
            )
        ),
        stored
    );
}

/// DIVERGENCE, pinned: `DROP DATABASE mysql` is accepted here.
///
/// Captured from Go:
///
/// ```text
/// drop database mysql;  -> [ddl:8267]Drop 'mysql' database is forbidden
/// ```
///
/// The refusal belongs in the `DropDatabase` statement arm that calls
/// `Catalog::drop_database`, in `tidb_session::dispatch`, which a parallel
/// unit owns; `drop_database` returns a bare `bool` and cannot carry 8267 on
/// its own. `DROP DATABASE information_schema` has the same hole today, so
/// this widens a pre-existing gap by one name rather than opening a class.
/// Nothing in the integration corpus drops either schema, so the gap is
/// unmeasured as well as unfixed.
///
/// FLIPS TO SUPPORT when the guard lands: assert `8267` and
/// `Drop 'mysql' database is forbidden` instead of `Ok`.
#[test]
fn dropping_the_mysql_schema_is_not_refused_yet() {
    let mut session = Session::new();
    assert!(session.run("DROP DATABASE mysql").is_ok());
    // And the object really is gone, which is what makes it a divergence
    // rather than a cosmetic one: the `USE` this unit fixed fails again.
    let error = session.run("USE mysql").unwrap_err().to_mysql_error();
    assert_eq!(error.code, 1049);
}

/// `information_schema.tables` lists the schema's OWN tables -- all of them,
/// served here or not -- as Go's registry does.
///
/// Go registers every memory table with an id from `tableIDMap`
/// (`pkg/infoschema/tables.go:253`, offsets from `InformationSchemaDBID` =
/// `1<<62 | 1`) and `setDataFromOneTable` reports it as `SYSTEM VIEW` with
/// zero storage numbers. The ids are observable -- `infoschema/v2` filters
/// `where TIDB_TABLE_ID = 4611686018427387967` and expects
/// `CLUSTER_STATEMENTS_SUMMARY_HISTORY`, which is `(1<<62|1) + 62`. Listing
/// a table is METADATA from Go's own source; QUERYING an unserved one still
/// refuses, which keeps the loud 1146 boundary the module doc explains.
#[test]
fn information_schema_lists_its_own_tables_with_gos_ids() {
    let mut session = Session::new();

    assert_eq!(
        row_text(session.run(
            "SELECT TABLE_SCHEMA, TABLE_NAME, TIDB_TABLE_ID FROM information_schema.tables \
             WHERE TIDB_TABLE_ID = 4611686018427387967"
        )),
        vec![vec![
            "INFORMATION_SCHEMA",
            "CLUSTER_STATEMENTS_SUMMARY_HISTORY",
            "4611686018427387967"
        ]]
    );
    // The row shape is Go's memory-table one: SYSTEM VIEW, InnoDB, zeroes.
    assert_eq!(
        row_text(session.run(
            "SELECT TABLE_TYPE, ENGINE, TABLE_ROWS FROM information_schema.tables \
             WHERE TABLE_NAME = 'TABLES' AND TABLE_SCHEMA = 'INFORMATION_SCHEMA'"
        )),
        vec![vec!["SYSTEM VIEW", "InnoDB", "0"]]
    );
    // Go's own gaps stay gaps: offset 14 was removed in issue 9154, so no id
    // lands there.
    assert!(row_text(session.run(
        "SELECT TABLE_NAME FROM information_schema.tables \
         WHERE TIDB_TABLE_ID = 4611686018427387919"
    ))
    .is_empty());
    // And an unserved listed table still refuses at the query, not the list.
    assert!(session
        .run("SELECT * FROM information_schema.CLUSTER_STATEMENTS_SUMMARY_HISTORY")
        .is_err());
}

/// `pkg/util/workloadrepo/workloadTables` reads these ten memory tables
/// through ordinary internal SQL. Each source name therefore has to be a
/// real planner-visible table; `MEMORY_USAGE` additionally has Go's one-row
/// process snapshot instead of the old fabricated empty result.
#[test]
fn workload_repository_source_tables_are_planner_visible() {
    let mut session = Session::new();
    for table in [
        "TIDB_INDEX_USAGE",
        "TIDB_STATEMENTS_STATS",
        "CLIENT_ERRORS_SUMMARY_BY_HOST",
        "CLIENT_ERRORS_SUMMARY_BY_USER",
        "CLIENT_ERRORS_SUMMARY_GLOBAL",
        "PROCESSLIST",
        "DATA_LOCK_WAITS",
        "TIDB_TRX",
        "MEMORY_USAGE",
        "DEADLOCKS",
    ] {
        let output = session
            .run_with_columns(&format!("SELECT * FROM information_schema.{table} LIMIT 0"))
            .unwrap_or_else(|error| panic!("{table} must be queryable: {error:?}"));
        let StmtOutput::Rows { columns, .. } = output else {
            panic!("{table} must return a row result");
        };
        assert!(!columns.is_empty(), "{table} must expose Go's columns");
    }

    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns("SELECT * FROM information_schema.MEMORY_USAGE")
        .unwrap()
    else {
        panic!("MEMORY_USAGE must return rows");
    };
    assert_eq!(columns.len(), 11);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 11);
}

#[derive(Clone)]
struct MockDataLockWaits(Vec<DataLockWait>);

impl DataLockWaitsProvider for MockDataLockWaits {
    fn lock_waits(&self) -> Result<Vec<DataLockWait>, String> {
        Ok(self.0.clone())
    }
}

/// Pinned Go `TestTestDataLockWaits` / `TestDataLockWaitsPrivilege`: PROCESS
/// gates the table, keys are uppercase hex, wait transaction IDs are unsigned,
/// and a valid resource tag yields the lowercase SQL digest.
#[test]
fn data_lock_waits_reads_the_storage_provider_with_go_privilege_and_encoding() {
    let mut tag = tidb_txnkv::ResourceGroupTagBuilder::new(None);
    tag.set_sql_digest(&[0xab, 0xcd]);
    let provider = std::sync::Arc::new(MockDataLockWaits(vec![DataLockWait {
        txn: 1,
        wait_for_txn: 2,
        key: b"key1".to_vec(),
        resource_group_tag: tag.encode_tag_with_key(&[]),
    }]));

    let mut session = Session::new();
    session.set_user("alice@%".to_owned(), "alice@127.0.0.1".to_owned());
    session.set_data_lock_waits_provider(provider);
    let error = session
        .run("SELECT * FROM information_schema.DATA_LOCK_WAITS")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1227);

    session.set_process_privilege(true);
    assert_eq!(
        row_text(session.run(
            "SELECT `KEY`, TRX_ID, CURRENT_HOLDING_TRX_ID, SQL_DIGEST, SQL_DIGEST_TEXT \
             FROM information_schema.DATA_LOCK_WAITS"
        )),
        [["6B657931", "1", "2", "abcd", "NULL"]]
    );
}

/// Pinned Go `tidbTrxTableRetriever` reads the session manager's live
/// transaction list, keeps at most 50 statement digests per transaction, and
/// applies the same `PROCESS`/own-user visibility rule as `SHOW PROCESSLIST`.
#[test]
fn tidb_trx_reads_live_transactions_with_go_visibility_and_digest_history() {
    let registry = process::ProcessRegistry::default();

    let mut alice = Session::new();
    alice.set_user("alice@%".to_owned(), "alice@10.0.0.1".to_owned());
    let alice_guard = registry.register(
        11,
        "alice".to_owned(),
        "10.0.0.1:1".to_owned(),
        "test".to_owned(),
        None,
    );
    alice.attach_process(11, alice_guard);
    alice.run("BEGIN").unwrap();

    let mut bob = Session::new();
    bob.set_user("bob@%".to_owned(), "bob@10.0.0.2".to_owned());
    let bob_guard = registry.register(
        22,
        "bob".to_owned(),
        "10.0.0.2:2".to_owned(),
        "test".to_owned(),
        None,
    );
    bob.attach_process(22, bob_guard);
    bob.run("BEGIN").unwrap();

    let own = row_text(bob.run(
        "SELECT SESSION_ID, USER, STATE, ALL_SQL_DIGESTS \
         FROM information_schema.TIDB_TRX ORDER BY SESSION_ID",
    ));
    assert_eq!(own.len(), 1);
    assert_eq!(own[0][0], "22");
    assert_eq!(own[0][1], "bob");
    assert_eq!(own[0][2], "Running");
    let (_, begin_digest) = tidb_parser::normalize_digest("BEGIN");
    let (_, select_digest) = tidb_parser::normalize_digest(
        "SELECT SESSION_ID, USER, STATE, ALL_SQL_DIGESTS \
         FROM information_schema.TIDB_TRX ORDER BY SESSION_ID",
    );
    assert_eq!(
        own[0][3],
        serde_json::to_string(&vec![begin_digest.to_string(), select_digest.to_string()]).unwrap()
    );

    bob.set_process_privilege(true);
    let all = row_text(
        bob.run("SELECT SESSION_ID, USER FROM information_schema.TIDB_TRX ORDER BY SESSION_ID"),
    );
    assert_eq!(all, [["11", "alice"], ["22", "bob"]]);

    for _ in 0..60 {
        bob.run("SELECT 1").unwrap();
    }
    let history = registry
        .transaction_snapshot()
        .into_iter()
        .find(|transaction| transaction.session_id == 22)
        .unwrap()
        .all_sql_digests;
    assert_eq!(history.len(), 50);
}

/// Pinned Go `setDataFromIndexUsage` enumerates the catalog's integer-handle
/// primary key as index ID zero, then every `TableInfo.Indices` entry, and
/// joins both to Domain's node-global collector.
#[test]
fn tidb_index_usage_reads_the_shared_collector_for_every_catalog_index() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE usage_t (id INT PRIMARY KEY, v INT, INDEX MixedCase(v))")
        .unwrap();
    let (table_id, secondary_id) = {
        let catalog = session.shared_catalog();
        let catalog = catalog.lock().unwrap();
        let tidb_executor::TableEntry::Kv(table) = catalog.table_in("test", "usage_t").unwrap()
        else {
            panic!("usage_t must be a stored table");
        };
        (table.table_id, table.indexes()[0].id)
    };

    let collector = std::sync::Arc::new(tidb_stats::index_usage::IndexUsageCollector::new());
    collector.start_worker();
    let mut pending = collector.spawn_session_collector();
    pending.update(
        table_id,
        secondary_id,
        &tidb_stats::new_index_usage_sample(7, 11, 13, 20),
    );
    pending.flush();
    for _ in 0..100 {
        if collector
            .get_index_usage(table_id, secondary_id)
            .query_total
            == 7
        {
            break;
        }
        std::thread::yield_now();
    }
    session.set_index_usage_collector(std::sync::Arc::clone(&collector));

    let (_, rows) = query_text(
        &mut session,
        "SELECT INDEX_NAME, QUERY_TOTAL, KV_REQ_TOTAL, ROWS_ACCESS_TOTAL, \
         PERCENTAGE_ACCESS_50_100, LAST_ACCESS_TIME \
         FROM information_schema.TIDB_INDEX_USAGE \
         WHERE TABLE_SCHEMA='test' AND TABLE_NAME='usage_t' ORDER BY INDEX_NAME",
    );
    collector.close();

    assert_eq!(rows.len(), 2);
    assert_eq!(&rows[0][..5], ["mixedcase", "7", "11", "13", "1"]);
    assert_ne!(rows[0][5], "<nil>");
    assert_eq!(rows[1], ["primary", "0", "0", "0", "0", "<nil>"]);
}

/// Pinned Go routes `TIDB_STATEMENTS_STATS` through the ordinary cumulative
/// statement-summary reader, including the full session query path used by
/// workload-repository sampling.
#[test]
fn tidb_statements_stats_reads_the_global_cumulative_summary() {
    use std::sync::Arc;
    use std::time::Duration;

    use tidb_stmtsummary::statement_summary::{
        EncodedPlanError, StmtExecInfo, StmtExecLazyInfo, StmtSummaryStmtCtx,
        STMT_SUMMARY_BY_DIGEST_MAP,
    };

    #[derive(Debug)]
    struct LazyInfo;

    impl StmtExecLazyInfo for LazyInfo {
        fn original_sql(&self) -> String {
            "select 1".to_owned()
        }

        fn encoded_plan(&self) -> Result<(String, String), EncodedPlanError> {
            Ok((String::new(), String::new()))
        }

        fn binary_plan(&self) -> String {
            String::new()
        }

        fn plan_digest(&self) -> String {
            String::new()
        }

        fn binding_sql_and_digest(&self) -> (String, String) {
            (String::new(), String::new())
        }
    }

    STMT_SUMMARY_BY_DIGEST_MAP.clear();
    let mut stmt_ctx = StmtSummaryStmtCtx::new();
    stmt_ctx.stmt_type = "Select".to_owned();
    STMT_SUMMARY_BY_DIGEST_MAP.add_statement(&StmtExecInfo {
        schema_name: "test".to_owned(),
        charset: "utf8mb4".to_owned(),
        collation: "utf8mb4_bin".to_owned(),
        normalized_sql: "select ?".to_owned(),
        digest: "workloadrepo-provider-regression".to_owned(),
        prev_sql: String::new(),
        prev_sql_digest: String::new(),
        plan_digest: String::new(),
        user: "root".to_owned(),
        total_latency: Duration::from_millis(2),
        parse_latency: Duration::ZERO,
        compile_latency: Duration::ZERO,
        stmt_ctx: Arc::new(stmt_ctx),
        cop_tasks: None,
        exec_detail: tidb_exec::exec_details::ExecDetails::default(),
        mem_max: 0,
        mem_arbitration: 0.0,
        disk_max: 0,
        start_time: chrono::Utc::now(),
        is_internal: false,
        succeed: true,
        plan_in_cache: false,
        plan_in_binding: false,
        exec_retry_count: 0,
        exec_retry_time: Duration::ZERO,
        write_sql_resp_duration: Duration::ZERO,
        result_rows: 1,
        tikv_exec_details: None,
        prepared: false,
        keyspace_name: String::new(),
        keyspace_id: 0,
        resource_group_name: "default".to_owned(),
        ru_detail: None,
        total_ru_v2: 0.0,
        cpu_usages: tidb_util::ppcpuusage::CpuUsages::default(),
        plan_cache_unqualified: String::new(),
        lazy_info: Arc::new(LazyInfo),
    });

    let mut session = Session::new();
    let (_, rows) = query_text(
        &mut session,
        "SELECT STMT_TYPE, SCHEMA_NAME, DIGEST_TEXT, EXEC_COUNT, RESULT_ROWS \
         FROM information_schema.TIDB_STATEMENTS_STATS \
         WHERE DIGEST='workloadrepo-provider-regression'",
    );
    STMT_SUMMARY_BY_DIGEST_MAP.clear();

    assert_eq!(rows, [["Select", "test", "select ?", "1", "1"]]);
}

/// Pinned Go exposes the `pkg/errno` instance counters, lets a user read only
/// their own per-user rows without PROCESS, and clears all three scopes with
/// `FLUSH CLIENT_ERRORS_SUMMARY`.
#[test]
fn client_errors_summary_uses_the_shared_counters_and_process_rules() {
    use tidb_error::tidb::infoschema::{flush_stats, increment_error, increment_warning};

    flush_stats();
    increment_error(1064, "alice", "host-a");
    increment_warning(1064, "alice", "host-a");
    increment_error(1146, "bob", "host-b");

    let mut session = Session::new();
    session.set_user("alice@%".to_owned(), "alice@host-a".to_owned());
    assert!(session
        .run("SELECT * FROM information_schema.CLIENT_ERRORS_SUMMARY_GLOBAL")
        .is_err());
    assert!(session
        .run("SELECT * FROM information_schema.CLIENT_ERRORS_SUMMARY_BY_HOST")
        .is_err());

    let (_, rows) = query_text(
        &mut session,
        "SELECT USER, ERROR_NUMBER, ERROR_COUNT, WARNING_COUNT, FIRST_SEEN, LAST_SEEN \
         FROM information_schema.CLIENT_ERRORS_SUMMARY_BY_USER ORDER BY USER, ERROR_NUMBER",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][..4], ["alice", "1064", "1", "1"]);
    assert_ne!(rows[0][4], "<nil>");
    assert_ne!(rows[0][5], "<nil>");

    session.set_process_privilege(true);
    let (_, rows) = query_text(
        &mut session,
        "SELECT ERROR_NUMBER, ERROR_COUNT, WARNING_COUNT \
         FROM information_schema.CLIENT_ERRORS_SUMMARY_GLOBAL ORDER BY ERROR_NUMBER",
    );
    assert_eq!(rows, [["1064", "1", "1"], ["1146", "1", "0"]]);

    session.run("FLUSH CLIENT_ERRORS_SUMMARY").unwrap();
    let (_, rows) = query_text(
        &mut session,
        "SELECT * FROM information_schema.CLIENT_ERRORS_SUMMARY_GLOBAL",
    );
    assert!(rows.is_empty());
}
