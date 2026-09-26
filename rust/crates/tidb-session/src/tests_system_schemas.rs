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
    for table in ["db", "tables_priv", "global_priv"] {
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
/// It has risen three times so far -- `bind_info` for GLOBAL bindings, the two
/// blacklist tables `ADMIN RELOAD` reads (`crate::blacklist`), and now the
/// statistics pair `stats_meta` + `stats_table_locked` that `ANALYZE` and
/// `SHOW STATS_*` require -- and each arrival is a feature that needed the
/// table, not a name added to make the count look better.
#[test]
fn enumerating_the_system_schema_under_reports() {
    let mut session = Session::new();
    session.run("USE mysql").unwrap();
    let stored = [
        ["bind_info"],
        ["expr_pushdown_blacklist"],
        ["opt_rule_blacklist"],
        ["stats_meta"],
        ["stats_table_locked"],
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

/// `DROP DATABASE mysql` is refused before the catalog mutation, matching Go.
///
/// Captured from Go:
///
/// ```text
/// drop database mysql;  -> [ddl:8267]Drop 'mysql' database is forbidden
/// ```
///
#[test]
fn dropping_the_mysql_schema_is_refused() {
    let mut session = Session::new();
    for statement in ["DROP DATABASE mysql", "DROP DATABASE IF EXISTS mysql"] {
        let error = session.run(statement).unwrap_err().to_mysql_error();
        assert_eq!(error.code, 8267, "{statement}");
        assert_eq!(
            error.message, "Drop 'mysql' database is forbidden",
            "{statement}"
        );
    }
    // The refusal must leave the bootstrap schema available to the session.
    session.run("USE mysql").unwrap();
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
        key: b"t\x80\x00\x00\x00\x00\x00\x03\xe7_r\x80\x00\x00\x00\x00\x00\x00\x01".to_vec(),
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
            "SELECT `KEY`, KEY_INFO, TRX_ID, CURRENT_HOLDING_TRX_ID, SQL_DIGEST, SQL_DIGEST_TEXT \
             FROM information_schema.DATA_LOCK_WAITS"
        )),
        [[
            "7480000000000003E75F728000000000000001",
            r#"{"handle_type":"int","handle_value":"1","table_id":999}"#,
            "1",
            "2",
            "abcd",
            "NULL"
        ]]
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

    // Go LazyTxn.onStmtStart records one digest per execution, not per
    // SetProcessInfo publication before planning and while draining results.
    let history_len = || {
        registry
            .transaction_snapshot()
            .into_iter()
            .find(|transaction| transaction.session_id == 22)
            .unwrap()
            .all_sql_digests
            .len()
    };
    let before = history_len();
    for _ in 0..2 {
        let running = bob.retain_process_statement("SELECT 1");
        bob.run("SELECT 1").unwrap();
        drop(running);
    }
    assert_eq!(history_len(), before + 2);

    let running = bob.retain_process_statement("SELECT 1");
    for _ in 0..2 {
        bob.run("SELECT 1").unwrap();
    }
    drop(running);
    assert_eq!(history_len(), before + 4);

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

    let collector = std::sync::Arc::new(tidb_stats_handle_usage_indexusage::Collector::new());
    collector.start_worker();
    let mut pending = collector.spawn_session_collector();
    pending.update(
        table_id,
        secondary_id,
        tidb_stats_handle_usage_indexusage::new_sample(7, 11, 13, 20),
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

/// Go `TestIndexUsageReporterWithRealData` exercises the reporter through a
/// point-read executor. The in-process Rust store can verify statement and
/// row accounting for the clustered-handle case; it has no KV RPCs to count.
#[test]
fn tidb_index_usage_records_a_clustered_point_read() {
    use std::sync::Arc;

    let collector = Arc::new(tidb_stats_handle_usage_indexusage::Collector::new());
    collector.start_worker();
    let mut session = Session::new();
    session.set_index_usage_collector(Arc::clone(&collector));
    session.set_session_index_usage_collector(collector.spawn_session_collector());
    session
        .run("CREATE TABLE usage_point (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session
        .run("INSERT INTO usage_point VALUES (1, 7)")
        .unwrap();

    let (table_id, primary_id) = {
        let catalog = session.shared_catalog();
        let catalog = catalog.lock().unwrap();
        let tidb_executor::TableEntry::Kv(table) = catalog.table_in("test", "usage_point").unwrap()
        else {
            panic!("usage_point must be a stored table");
        };
        (table.table_id, 0)
    };
    assert_eq!(
        row_text(session.run("SELECT v FROM usage_point WHERE id = 1")),
        [["7"]]
    );

    drop(session);
    collector.close();

    let usage = collector.get_index_usage(table_id, primary_id);
    assert_eq!(usage.query_total, 1);
    assert_eq!(usage.kv_req_total, 0);
    assert_eq!(usage.row_access_total, 1);
    assert_eq!(usage.percentage_access, [0, 1, 0, 0, 0, 0, 0]);
}

/// Go `TestIndexUsageReporterWithClusterIndex` exercises clustered integer
/// and common handles, plus a nonclustered string primary index. A hinted OR
/// IndexMerge combines integer-handle ranges with a secondary-index range.
#[test]
fn tidb_index_usage_records_clustered_handle_and_primary_index_reads() {
    use std::sync::Arc;

    let collector = Arc::new(tidb_stats_handle_usage_indexusage::Collector::new());
    collector.start_worker();
    let mut session = Session::new();
    session.set_index_usage_collector(Arc::clone(&collector));
    session.set_session_index_usage_collector(collector.spawn_session_collector());
    session
        .run("CREATE TABLE usage_cluster_int (id INT PRIMARY KEY, a INT)")
        .unwrap();
    session
        .run("CREATE TABLE usage_cluster_common (id CHAR(255) PRIMARY KEY, a INT)")
        .unwrap();
    session
        .run("CREATE TABLE usage_cluster_nonclustered (id CHAR(255) PRIMARY KEY NONCLUSTERED, a INT)")
        .unwrap();
    session
        .run("CREATE TABLE usage_cluster_merge (id INT PRIMARY KEY, a INT, UNIQUE KEY idx_a(a))")
        .unwrap();

    let integer_rows = (0..100)
        .map(|id| format!("({id}, {id})"))
        .collect::<Vec<_>>()
        .join(", ");
    let common_rows = (0..100)
        .map(|id| format!("('{id}', {id})"))
        .collect::<Vec<_>>()
        .join(", ");
    session
        .run(&format!(
            "INSERT INTO usage_cluster_int VALUES {integer_rows}"
        ))
        .unwrap();
    session
        .run(&format!(
            "INSERT INTO usage_cluster_common VALUES {common_rows}"
        ))
        .unwrap();
    session
        .run(&format!(
            "INSERT INTO usage_cluster_nonclustered VALUES {common_rows}"
        ))
        .unwrap();
    session
        .run(&format!(
            "INSERT INTO usage_cluster_merge VALUES {integer_rows}"
        ))
        .unwrap();
    for table in [
        "usage_cluster_int",
        "usage_cluster_common",
        "usage_cluster_nonclustered",
        "usage_cluster_merge",
    ] {
        session.run(&format!("ANALYZE TABLE {table}")).unwrap();
    }

    let table_index = |name: &str| {
        let catalog = session.shared_catalog();
        let catalog = catalog.lock().unwrap();
        let tidb_executor::TableEntry::Kv(table) = catalog.table_in("test", name).unwrap() else {
            panic!("{name} must be a stored table");
        };
        let primary_id = table
            .indexes()
            .iter()
            .find(|index| index.name.eq_ignore_ascii_case("primary"))
            .map_or(0, |index| index.id);
        (table.table_id, primary_id)
    };
    let int_key = table_index("usage_cluster_int");
    let common_key = table_index("usage_cluster_common");
    let nonclustered_key = table_index("usage_cluster_nonclustered");
    let merge_key = table_index("usage_cluster_merge");
    let merge_index_id = {
        let catalog = session.shared_catalog();
        let catalog = catalog.lock().unwrap();
        let tidb_executor::TableEntry::Kv(table) =
            catalog.table_in("test", "usage_cluster_merge").unwrap()
        else {
            panic!("usage_cluster_merge must be a stored table");
        };
        table
            .indexes()
            .iter()
            .find(|index| index.name.eq_ignore_ascii_case("idx_a"))
            .expect("idx_a metadata")
            .id
    };

    let mut common_range_ids = (0..100)
        .map(|id| id.to_string())
        .filter(|id| id.as_str() >= "30")
        .collect::<Vec<_>>();
    common_range_ids.sort();
    let common_range_rows = common_range_ids
        .iter()
        .map(|id| vec![id.clone()])
        .collect::<Vec<_>>();
    let int_range_rows = (30..100).map(|id| vec![id.to_string()]).collect::<Vec<_>>();
    let int_batch_rows = [1, 3, 5, 9]
        .map(|id| vec![id.to_string(), id.to_string()])
        .to_vec();
    let common_batch_rows = ["1", "3", "5", "9"]
        .map(|id| vec![id.to_owned(), id.to_owned()])
        .to_vec();
    let merge_rows = (0..100)
        .filter(|id| *id < 5 || *id >= 30)
        .map(|id| vec![id.to_string(), id.to_string()])
        .collect::<Vec<_>>();
    let cases = vec![
        (
            "SELECT id FROM usage_cluster_int WHERE id >= 30",
            "TableReader",
            int_range_rows,
        ),
        (
            "SELECT id FROM usage_cluster_common WHERE id >= \"30\"",
            "TableReader",
            common_range_rows.clone(),
        ),
        (
            "SELECT id FROM usage_cluster_nonclustered WHERE id >= \"30\"",
            "IndexRangeScan",
            common_range_rows,
        ),
        (
            "SELECT * FROM usage_cluster_int WHERE id = 1",
            "Point_Get",
            vec![vec!["1".to_owned(), "1".to_owned()]],
        ),
        (
            "SELECT * FROM usage_cluster_common WHERE id = \"1\"",
            "Point_Get",
            vec![vec!["1".to_owned(), "1".to_owned()]],
        ),
        (
            "SELECT * FROM usage_cluster_int WHERE id IN (1, 3, 5, 9)",
            "Batch_Point_Get",
            int_batch_rows,
        ),
        (
            "SELECT * FROM usage_cluster_common WHERE id IN (\"1\", \"3\", \"5\", \"9\")",
            "Batch_Point_Get",
            common_batch_rows,
        ),
        (
            "SELECT /*+ USE_INDEX_MERGE(usage_cluster_merge) */ * FROM usage_cluster_merge WHERE id >= 30 OR id < 5 OR a >= 50",
            "IndexMerge",
            merge_rows,
        ),
    ];

    for (index, (sql, expected_plan, expected_rows)) in cases.iter().enumerate() {
        let plan = row_text(session.run(&format!("EXPLAIN {sql}")));
        assert!(
            plan.iter()
                .any(|row| row.first().is_some_and(|name| name.contains(expected_plan))),
            "expected {expected_plan} for {sql}: {plan:?}"
        );
        let assert_rows = |mut actual: Vec<Vec<String>>| {
            let mut expected = expected_rows.clone();
            if *expected_plan == "IndexMerge" {
                actual.sort();
                expected.sort();
            }
            assert_eq!(actual, expected, "{sql}");
        };
        assert_rows(row_text(session.run(sql)));

        let statement = format!("cluster_index_usage_{index}");
        session
            .run(&format!("PREPARE {statement} FROM '{sql}'"))
            .unwrap();
        for _ in 0..2 {
            assert_rows(row_text(session.run(&format!("EXECUTE {statement}"))));
        }
        session
            .run(&format!("DEALLOCATE PREPARE {statement}"))
            .unwrap();
    }

    drop(session);
    collector.close();

    let int_usage = collector.get_index_usage(int_key.0, int_key.1);
    assert_eq!(int_usage.query_total, 9, "{int_usage:?}");
    assert_eq!(int_usage.row_access_total, 225, "{int_usage:?}");
    assert_eq!(int_usage.kv_req_total, 0);
    assert_eq!(int_usage.percentage_access, [0, 3, 3, 0, 0, 3, 0]);

    let common_usage = collector.get_index_usage(common_key.0, common_key.1);
    assert_eq!(common_usage.query_total, 9, "{common_usage:?}");
    assert_eq!(common_usage.row_access_total, 243, "{common_usage:?}");
    assert_eq!(common_usage.kv_req_total, 0);
    assert_eq!(common_usage.percentage_access, [0, 3, 3, 0, 0, 3, 0]);

    let nonclustered_usage = collector.get_index_usage(nonclustered_key.0, nonclustered_key.1);
    assert_eq!(nonclustered_usage.query_total, 3, "{nonclustered_usage:?}");
    assert_eq!(
        nonclustered_usage.row_access_total, 228,
        "{nonclustered_usage:?}"
    );
    assert_eq!(nonclustered_usage.kv_req_total, 0);
    assert_eq!(nonclustered_usage.percentage_access, [0, 0, 0, 0, 0, 3, 0]);

    let merge_pk_usage = collector.get_index_usage(merge_key.0, merge_key.1);
    assert_eq!(merge_pk_usage.query_total, 3, "{merge_pk_usage:?}");
    assert_eq!(merge_pk_usage.row_access_total, 225, "{merge_pk_usage:?}");
    assert_eq!(merge_pk_usage.kv_req_total, 0);
    assert_eq!(merge_pk_usage.percentage_access, [0, 0, 3, 0, 0, 3, 0]);

    let merge_secondary_usage = collector.get_index_usage(merge_key.0, merge_index_id);
    assert_eq!(
        merge_secondary_usage.query_total, 3,
        "{merge_secondary_usage:?}"
    );
    assert_eq!(
        merge_secondary_usage.row_access_total, 150,
        "{merge_secondary_usage:?}"
    );
    assert_eq!(merge_secondary_usage.kv_req_total, 0);
    assert_eq!(
        merge_secondary_usage.percentage_access,
        [0, 0, 0, 0, 0, 3, 0]
    );
}

/// Go `TestIndexUsageReporterWithPartitionTable` checks that reads on a
/// partition-local index are attributed to the logical table and index for
/// range, point, batch-point, and prepared executions.
#[test]
fn tidb_index_usage_records_a_partition_local_index_read() {
    use std::sync::Arc;

    let collector = Arc::new(tidb_stats_handle_usage_indexusage::Collector::new());
    collector.start_worker();
    let mut session = Session::new();
    session.set_index_usage_collector(Arc::clone(&collector));
    session.set_session_index_usage_collector(collector.spawn_session_collector());
    session
        .run(
            "CREATE TABLE usage_partition_local (id INT, UNIQUE KEY idx_id(id)) \
             PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), \
             PARTITION p1 VALUES LESS THAN (20), PARTITION p2 VALUES LESS THAN (50), \
             PARTITION pmax VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    let rows = (0..100)
        .map(|id| format!("({id})"))
        .collect::<Vec<_>>()
        .join(", ");
    session
        .run(&format!("INSERT INTO usage_partition_local VALUES {rows}"))
        .unwrap();
    session
        .run("SET @@tidb_partition_prune_mode = 'static'")
        .unwrap();
    session.run("ANALYZE TABLE usage_partition_local").unwrap();

    let (table_id, index_id) = {
        let catalog = session.shared_catalog();
        let catalog = catalog.lock().unwrap();
        let tidb_executor::TableEntry::Kv(table) =
            catalog.table_in("test", "usage_partition_local").unwrap()
        else {
            panic!("usage_partition_local must be a stored table");
        };
        (table.table_id, table.indexes()[0].id)
    };
    let range_sql = "SELECT id FROM usage_partition_local WHERE id >= 30";
    let selection_sql = "SELECT id FROM usage_partition_local WHERE id - 95 >= 0 AND id >= 90";
    let point_sql = "SELECT * FROM usage_partition_local WHERE id = 1";
    let batch_sql = "SELECT * FROM usage_partition_local WHERE id IN (1, 3, 5, 9)";
    let explain =
        |session: &mut Session, sql: &str| row_text(session.run(&format!("EXPLAIN {sql}")));

    let range_plan = explain(&mut session, range_sql);
    assert!(
        range_plan
            .first()
            .and_then(|row| row.first())
            .is_some_and(|name| name.starts_with("PartitionUnion")),
        "a range spanning two partitions must use PartitionUnion: {range_plan:?}"
    );
    assert!(range_plan.iter().any(|row| row
        .get(3)
        .is_some_and(|objects| objects.contains("index:idx_id"))));
    let range_result = row_text(session.run(range_sql));
    assert_eq!(
        range_result,
        (30..100).map(|id| vec![id.to_string()]).collect::<Vec<_>>()
    );

    let selection_plan = explain(&mut session, selection_sql);
    assert!(
        selection_plan.iter().any(|row| row
            .get(3)
            .is_some_and(|objects| objects.contains("index:idx_id"))),
        "the single-partition range must use idx_id: {selection_plan:?}"
    );
    assert_eq!(
        row_text(session.run(selection_sql)),
        (95..100).map(|id| vec![id.to_string()]).collect::<Vec<_>>()
    );

    let point_plan = explain(&mut session, point_sql);
    assert!(
        point_plan
            .first()
            .and_then(|row| row.first())
            .is_some_and(|name| name.starts_with("Point_Get")),
        "a unique partition-local equality should use Point_Get: {point_plan:?}"
    );
    assert_eq!(row_text(session.run(point_sql)), [["1"]]);

    let batch_plan = explain(&mut session, batch_sql);
    assert!(
        batch_plan
            .first()
            .and_then(|row| row.first())
            .is_some_and(|name| name.starts_with("Batch_Point_Get")),
        "a unique partition-local IN predicate should use Batch_Point_Get: {batch_plan:?}"
    );
    assert_eq!(
        row_text(session.run(batch_sql)),
        [["1"], ["3"], ["5"], ["9"]]
    );

    for (statement, sql, expected) in [
        (
            "partition_range",
            range_sql,
            (30..100).map(|id| vec![id.to_string()]).collect::<Vec<_>>(),
        ),
        (
            "partition_selection",
            selection_sql,
            (95..100).map(|id| vec![id.to_string()]).collect::<Vec<_>>(),
        ),
        ("partition_point", point_sql, vec![vec!["1".to_owned()]]),
        (
            "partition_batch",
            batch_sql,
            vec![
                vec!["1".to_owned()],
                vec!["3".to_owned()],
                vec!["5".to_owned()],
                vec!["9".to_owned()],
            ],
        ),
    ] {
        session
            .run(&format!("PREPARE {statement} FROM '{sql}'"))
            .unwrap();
        for _ in 0..2 {
            assert_eq!(
                row_text(session.run(&format!("EXECUTE {statement}"))),
                expected
            );
        }
        session
            .run(&format!("DEALLOCATE PREPARE {statement}"))
            .unwrap();
    }

    drop(session);
    collector.close();

    let usage = collector.get_index_usage(table_id, index_id);
    assert_eq!(
        usage.query_total, 12,
        "range={range_plan:?}; selection={selection_plan:?}; point={point_plan:?}; batch={batch_plan:?}; usage={usage:?}"
    );
    assert_eq!(usage.row_access_total, 255, "usage={usage:?}");
    assert_eq!(usage.kv_req_total, 0, "in-process scans issue no KV RPCs");
    assert_eq!(usage.percentage_access, [0, 3, 3, 0, 3, 3, 3]);
}

/// Go `TestIndexUsageReporterWithRealData` exercises index readers, lookup,
/// point readers, and a hinted AND IndexMerge over analyzed unique indexes.
#[test]
fn tidb_index_usage_records_real_data_reads() {
    use std::sync::Arc;

    let collector = Arc::new(tidb_stats_handle_usage_indexusage::Collector::new());
    collector.start_worker();
    let mut session = Session::new();
    session.set_index_usage_collector(Arc::clone(&collector));
    session.set_session_index_usage_collector(collector.spawn_session_collector());
    session
        .run(
            "CREATE TABLE usage_real_data (id_1 INT, id_2 INT, \
             UNIQUE KEY idx_1(id_1), UNIQUE KEY idx_2(id_2))",
        )
        .unwrap();
    let rows = (0..100)
        .map(|id| format!("({id}, {id})"))
        .collect::<Vec<_>>()
        .join(", ");
    session
        .run(&format!("INSERT INTO usage_real_data VALUES {rows}"))
        .unwrap();
    session.run("ANALYZE TABLE usage_real_data").unwrap();

    let (table_id, index_1_id, index_2_id) = {
        let catalog = session.shared_catalog();
        let catalog = catalog.lock().unwrap();
        let tidb_executor::TableEntry::Kv(table) =
            catalog.table_in("test", "usage_real_data").unwrap()
        else {
            panic!("usage_real_data must be a stored table");
        };
        let index_id = |name: &str| {
            table
                .indexes()
                .iter()
                .find(|index| index.name.eq_ignore_ascii_case(name))
                .unwrap_or_else(|| panic!("missing {name} index"))
                .id
        };
        (table.table_id, index_id("idx_1"), index_id("idx_2"))
    };

    let cases = vec![
        (
            "SELECT id_1 FROM usage_real_data WHERE id_1 >= 30",
            "IndexReader",
            (30..100).map(|id| vec![id.to_string()]).collect::<Vec<_>>(),
        ),
        (
            "SELECT id_1 FROM usage_real_data WHERE id_1 - 95 >= 0 AND id_1 >= 90",
            "IndexReader",
            (95..100).map(|id| vec![id.to_string()]).collect::<Vec<_>>(),
        ),
        (
            "SELECT id_2 FROM usage_real_data USE INDEX (idx_1) \
             WHERE id_1 >= 30 AND id_1 - 50 >= 0",
            "IndexLookUp",
            (50..100).map(|id| vec![id.to_string()]).collect::<Vec<_>>(),
        ),
        (
            "SELECT /*+ USE_INDEX_MERGE(usage_real_data, idx_1, idx_2) */ id_2 \
             FROM usage_real_data WHERE id_1 >= 30 AND id_2 >= 80 AND id_2 - 95 >= 0",
            "IndexMerge",
            (95..100).map(|id| vec![id.to_string()]).collect::<Vec<_>>(),
        ),
        (
            "SELECT * FROM usage_real_data WHERE id_1 = 1",
            "Point_Get",
            vec![vec!["1".to_owned(), "1".to_owned()]],
        ),
        (
            "SELECT id_1 FROM usage_real_data \
             WHERE id_1 = 1 OR id_1 = 50 OR id_1 = 25",
            "Batch_Point_Get",
            vec![
                vec!["1".to_owned()],
                vec!["25".to_owned()],
                vec!["50".to_owned()],
            ],
        ),
    ];
    for (sql, expected_plan, expected_rows) in &cases {
        let plan = row_text(session.run(&format!("EXPLAIN {sql}")));
        assert!(
            plan.iter()
                .any(|row| row.first().is_some_and(|name| name.contains(expected_plan))),
            "expected {expected_plan} for {sql}: {plan:?}"
        );
        assert_eq!(row_text(session.run(sql)), *expected_rows, "{sql}");
    }

    for (index, (sql, _, expected_rows)) in cases.iter().enumerate() {
        let statement = format!("usage_real_data_{index}");
        session
            .run(&format!("PREPARE {statement} FROM '{sql}'"))
            .unwrap();
        for _ in 0..2 {
            assert_eq!(
                row_text(session.run(&format!("EXECUTE {statement}"))),
                *expected_rows,
                "{sql}"
            );
        }
        session
            .run(&format!("DEALLOCATE PREPARE {statement}"))
            .unwrap();
    }

    drop(session);
    collector.close();

    let index_1_usage = collector.get_index_usage(table_id, index_1_id);
    assert_eq!(index_1_usage.query_total, 18, "{index_1_usage:?}");
    assert_eq!(index_1_usage.row_access_total, 672, "{index_1_usage:?}");
    assert_eq!(index_1_usage.kv_req_total, 0);
    assert_eq!(index_1_usage.percentage_access, [0, 3, 3, 3, 0, 9, 0]);

    let index_2_usage = collector.get_index_usage(table_id, index_2_id);
    assert_eq!(index_2_usage.query_total, 3, "{index_2_usage:?}");
    assert_eq!(index_2_usage.row_access_total, 60, "{index_2_usage:?}");
    assert_eq!(index_2_usage.kv_req_total, 0);
    assert_eq!(index_2_usage.percentage_access, [0, 0, 0, 0, 3, 0, 0]);
}

/// Go `generateIndexMergePath` does not build OR or normal-index AND merges on
/// LOCAL temporary tables. GLOBAL temporary tables remain a separate case.
#[test]
fn tidb_index_merge_is_not_planned_for_local_temporary_tables() {
    let mut session = Session::new();
    session
        .run("CREATE TEMPORARY TABLE index_merge_tmp (a INT, b INT, KEY ia(a), KEY ib(b))")
        .unwrap();

    for sql in [
        "SELECT /*+ USE_INDEX_MERGE(index_merge_tmp, ia, ib) */ * \
         FROM index_merge_tmp WHERE a = 1 OR b = 2",
        "SELECT /*+ USE_INDEX_MERGE(index_merge_tmp, ia, ib) */ * \
         FROM index_merge_tmp WHERE a = 1 AND b = 2",
    ] {
        let plan = row_text(session.run(&format!("EXPLAIN {sql}")));
        assert!(
            !plan
                .iter()
                .any(|row| row.first().is_some_and(|line| line.contains("IndexMerge"))),
            "IndexMerge must not be generated for a LOCAL temporary table: {plan:?}"
        );
    }
}

/// Go `TestIndexUsageReporterWithGlobalIndex` checks global-index point reads
/// on a partitioned table. The reporter must still publish under the logical
/// table ID and the global index's metadata ID.
#[test]
fn tidb_index_usage_records_a_partitioned_global_index_point_read() {
    use std::sync::Arc;

    let collector = Arc::new(tidb_stats_handle_usage_indexusage::Collector::new());
    collector.start_worker();
    let mut session = Session::new();
    session.set_index_usage_collector(Arc::clone(&collector));
    session.set_session_index_usage_collector(collector.spawn_session_collector());
    session
        .run(
            "CREATE TABLE usage_global_index (pk INT PRIMARY KEY, id INT, \
             UNIQUE KEY idx_id(id) GLOBAL) PARTITION BY RANGE (pk) (\
             PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20), \
             PARTITION p2 VALUES LESS THAN (50), PARTITION pmax VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    let rows = (0..100)
        .map(|id| format!("({id}, {id})"))
        .collect::<Vec<_>>()
        .join(", ");
    session
        .run(&format!("INSERT INTO usage_global_index VALUES {rows}"))
        .unwrap();
    session
        .run("SET @@tidb_partition_prune_mode = 'static'")
        .unwrap();
    session.run("ANALYZE TABLE usage_global_index").unwrap();

    let (table_id, index_id) = {
        let catalog = session.shared_catalog();
        let catalog = catalog.lock().unwrap();
        let tidb_executor::TableEntry::Kv(table) =
            catalog.table_in("test", "usage_global_index").unwrap()
        else {
            panic!("usage_global_index must be a stored table");
        };
        let index = table
            .indexes()
            .iter()
            .find(|index| index.name.eq_ignore_ascii_case("idx_id"))
            .expect("global index metadata");
        assert!(index.unique && index.global);
        (table.table_id, index.id)
    };
    let plan = row_text(
        session
            .run("EXPLAIN SELECT pk, id FROM usage_global_index USE INDEX (idx_id) WHERE id = 1"),
    );
    assert!(
        plan.first()
            .and_then(|row| row.first())
            .is_some_and(|name| name.starts_with("Point_Get")),
        "a unique global-index point predicate should use Point_Get: {plan:?}"
    );
    assert!(
        plan.iter().any(|row| row
            .get(3)
            .is_some_and(|objects| objects.contains("index:idx_id"))),
        "global-index point read must use idx_id: {plan:?}"
    );
    assert_eq!(
        row_text(
            session.run("SELECT pk, id FROM usage_global_index USE INDEX (idx_id) WHERE id = 1",)
        ),
        [["1", "1"]]
    );
    let primary_hint_plan = row_text(
        session
            .run("EXPLAIN SELECT pk, id FROM usage_global_index USE INDEX (PRIMARY) WHERE id = 1"),
    );
    assert!(
        primary_hint_plan
            .first()
            .and_then(|row| row.first())
            .is_some_and(|name| name.starts_with("PartitionUnion")),
        "an index hint that excludes the global index must keep static pruning: {primary_hint_plan:?}"
    );
    session.run("SET @@sql_select_limit = 1").unwrap();
    let select_limit_plan = row_text(
        session
            .run("EXPLAIN SELECT pk, id FROM usage_global_index USE INDEX (idx_id) WHERE id = 1"),
    );
    assert!(
        select_limit_plan
            .first()
            .and_then(|row| row.first())
            .is_some_and(|name| name.starts_with("PartitionUnion")),
        "sql_select_limit suppresses Go's fast-plan path: {select_limit_plan:?}"
    );
    session.run("SET @@sql_select_limit = DEFAULT").unwrap();
    session
        .run("SET @@tidb_opt_fix_control = '52592:ON'")
        .unwrap();
    let fix_control_plan = row_text(
        session
            .run("EXPLAIN SELECT pk, id FROM usage_global_index USE INDEX (idx_id) WHERE id = 1"),
    );
    assert!(
        fix_control_plan
            .first()
            .and_then(|row| row.first())
            .is_some_and(|name| name.starts_with("PartitionUnion")),
        "FIX_52592 suppresses Go's fast-plan path: {fix_control_plan:?}"
    );
    session.run("SET @@tidb_opt_fix_control = DEFAULT").unwrap();
    let batch_plan = row_text(
        session
            .run("EXPLAIN SELECT pk, id FROM usage_global_index IGNORE INDEX (PRIMARY) WHERE id IN (1, 3, 5, 9)"),
    );
    assert!(
        batch_plan
            .first()
            .and_then(|row| row.first())
            .is_some_and(|name| name.starts_with("Batch_Point_Get")),
        "a static unique global-index IN predicate should use Batch_Point_Get: {batch_plan:?}"
    );
    assert!(
        batch_plan.iter().any(|row| row
            .get(3)
            .is_some_and(|objects| objects.contains("index:idx_id"))),
        "global-index batch point read must use idx_id: {batch_plan:?}"
    );
    assert_eq!(
        row_text(session.run(
            "SELECT pk, id FROM usage_global_index IGNORE INDEX (PRIMARY) WHERE id IN (1, 3, 5, 9)",
        )),
        [["1", "1"], ["3", "3"], ["5", "5"], ["9", "9"]]
    );

    session
        .run("PREPARE global_point FROM 'SELECT pk, id FROM usage_global_index USE INDEX (idx_id) WHERE id = 1'")
        .unwrap();
    for _ in 0..2 {
        assert_eq!(row_text(session.run("EXECUTE global_point")), [["1", "1"]]);
    }
    session.run("DEALLOCATE PREPARE global_point").unwrap();
    session
        .run("PREPARE global_batch FROM 'SELECT pk, id FROM usage_global_index IGNORE INDEX (PRIMARY) WHERE id IN (1, 3, 5, 9)'")
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            row_text(session.run("EXECUTE global_batch")),
            [["1", "1"], ["3", "3"], ["5", "5"], ["9", "9"]]
        );
    }
    session.run("DEALLOCATE PREPARE global_batch").unwrap();

    drop(session);
    collector.close();

    let usage = collector.get_index_usage(table_id, index_id);
    assert_eq!(
        usage.query_total, 6,
        "plan={plan:?}; batch_plan={batch_plan:?}; usage={usage:?}"
    );
    assert_eq!(usage.row_access_total, 15);
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
