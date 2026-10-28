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

//! Memory tables are read where the plan reads them, not where the statement
//! text names them (Go `buildMemTable` + `MemTableReaderExec`). Every expected
//! answer below was captured from Go TiDB.

use crate::tests_support::*;
use crate::*;

fn error_of(session: &mut Session, sql: &str) -> String {
    session
        .run(sql)
        .err()
        .unwrap_or_else(|| panic!("{sql} was accepted"))
        .to_string()
}

/// A write whose read half scans a memory table: Go retrieves the rows when
/// the scan is read, whatever statement the scan sits under. The statement
/// text decided which tables were read before, and only a SELECT was walked,
/// so every one of these failed with a missing memory table.
#[test]
fn writes_read_memory_tables() {
    let mut session = Session::new();
    session.run("create database memtbl").unwrap();
    session.run("use memtbl").unwrap();
    session
        .run("create table t (a bigint primary key, n varchar(64))")
        .unwrap();

    // INSERT ... SELECT.
    session
        .run(
            "insert into t select 1, schema_name from information_schema.schemata \
             where schema_name = 'memtbl'",
        )
        .unwrap();
    assert_eq!(row_text(session.run("select * from t")), [["1", "memtbl"]]);

    // An UPDATE assigning a scalar subquery, which runs while it is planned.
    session
        .run(
            "update t set a = (select count(*) from information_schema.schemata \
             where schema_name = 'memtbl') + 10",
        )
        .unwrap();
    assert_eq!(row_text(session.run("select * from t")), [["11", "memtbl"]]);

    // A DELETE filtered by an IN subquery.
    session
        .run(
            "delete from t where n in (select schema_name from information_schema.schemata \
             where schema_name = 'memtbl')",
        )
        .unwrap();
    assert_eq!(row_text(session.run("select count(*) from t")), [["0"]]);

    // REPLACE ... SELECT, then a multi-table UPDATE joining the memory table.
    session
        .run(
            "replace into t select 5, schema_name from information_schema.schemata \
             where schema_name = 'memtbl'",
        )
        .unwrap();
    assert_eq!(row_text(session.run("select * from t")), [["5", "memtbl"]]);
    session
        .run(
            "update t join information_schema.schemata s on s.schema_name = t.n \
             set t.a = 6",
        )
        .unwrap();
    assert_eq!(row_text(session.run("select * from t")), [["6", "memtbl"]]);

    // ON DUPLICATE KEY UPDATE assigning a subquery over a memory table.
    session
        .run(
            "insert into t values (6, 'x') on duplicate key update n = \
             (select max(schema_name) from information_schema.schemata \
             where schema_name = 'memtbl')",
        )
        .unwrap();
    assert_eq!(row_text(session.run("select * from t")), [["6", "memtbl"]]);

    // A locking read whose subquery scans one.
    assert_eq!(
        row_text(session.run(
            "select a from t where exists (select 1 from information_schema.schemata \
             where schema_name = 'memtbl') for update"
        )),
        [["6"]]
    );
}

/// The plan, not the statement text, resolves a memory table, so a missing one
/// is reported as the planner reports every missing table: the tables of a
/// FROM clause resolve left to right, and the schema is named in lower case
/// (captured: `INFORMATION_SCHEMA.nonexistent` answers
/// `information_schema.nonexistent`, a bare name under `USE PERFORMANCE_SCHEMA`
/// answers `performance_schema.NonExistent`).
#[test]
fn a_missing_memory_table_is_reported_as_the_planner_resolves_it() {
    let mut session = Session::new();
    for (sql, expected) in [
        (
            "select * from INFORMATION_SCHEMA.nonexistent",
            "Table 'information_schema.nonexistent' doesn't exist",
        ),
        (
            "select * from Information_Schema.NonExistent",
            "Table 'information_schema.NonExistent' doesn't exist",
        ),
        (
            "select * from PERFORMANCE_SCHEMA.nonexistent",
            "Table 'performance_schema.nonexistent' doesn't exist",
        ),
        (
            "select * from metrics_SCHEMA.nonexistent",
            "Table 'metrics_schema.nonexistent' doesn't exist",
        ),
        (
            "select * from MySQL.nonexistent",
            "Table 'mysql.nonexistent' doesn't exist",
        ),
        (
            "select * from information_schema.nonexistent join test.x",
            "Table 'information_schema.nonexistent' doesn't exist",
        ),
        (
            "select * from test.x join information_schema.nonexistent",
            "Table 'test.x' doesn't exist",
        ),
    ] {
        assert_eq!(error_of(&mut session, sql), expected, "{sql}");
    }
    session.run("use information_schema").unwrap();
    assert_eq!(
        error_of(&mut session, "select * from NonExistent"),
        "Table 'information_schema.NonExistent' doesn't exist"
    );
    session.run("use PERFORMANCE_SCHEMA").unwrap();
    assert_eq!(
        error_of(&mut session, "select * from NonExistent"),
        "Table 'performance_schema.NonExistent' doesn't exist"
    );
}

/// The tables a scan retrieves itself are exactly the ones `table_rows`
/// answers, and no table the session's families answer is among them (their
/// arms would be shadowed).
#[test]
fn the_tables_a_scan_retrieves_itself_are_the_ones_table_rows_answers() {
    let catalog = tidb_executor::Catalog::mock();
    let ctx = tidb_executor::StmtContext::default();
    let visibility = tidb_executor::infoschema_reader::SchemaVisibility::unrestricted();
    for table in tidb_executor::infoschema_reader::SERVED_TABLES {
        assert!(
            tidb_executor::infoschema_reader::table_rows(table, &catalog, &visibility, &ctx)
                .is_some(),
            "{table}"
        );
    }
    for table in [
        "PROCESSLIST",
        "TIDB_TRX",
        "STATEMENTS_SUMMARY",
        "SLOW_QUERY",
        "CLUSTER_INFO",
        "DDL_JOBS",
        "USER_PRIVILEGES",
    ] {
        assert!(
            !tidb_executor::infoschema_reader::serves_table(table),
            "{table}"
        );
        assert!(
            tidb_executor::infoschema_reader::table_rows(table, &catalog, &visibility, &ctx)
                .is_none(),
            "{table}"
        );
    }
}

/// A scan has at most one retriever: the families serve disjoint tables, and
/// a table none of them builds reads as empty, as Go's `memtableRetriever`
/// does for a table it has no case for.
#[test]
fn a_scan_has_at_most_one_retriever() {
    use tidb_executor::mem_table_retrieval::{MemTableRead, MemTableRetriever};
    let session = Session::new();
    let read = |db: &str, table: &str| MemTableRead {
        plan_id: 1,
        db_name: db.to_owned(),
        table_name: table.to_owned(),
        columns: Vec::new(),
        column_ids: Vec::new(),
        extractor: None,
        query_time_range: Default::default(),
        is_cluster_scan: false,
    };
    let retrievers: Vec<Box<dyn MemTableRetriever>> = vec![
        Box::new(
            tidb_executor::memtable_retrievers::InformationSchemaRetriever::new(
                session.schema_visibility(),
                None,
                "default".to_owned(),
            ),
        ),
        Box::new(tidb_executor::memtable_reader::InstanceStateRetriever::new(
            session.viewer_state(),
        )),
        Box::new(
            tidb_executor::memtable_retrievers::CatalogTablesRetriever::new(
                session.schema_visibility(),
                false,
            ),
        ),
        Box::new(crate::account_state::AccountRetriever::new(
            session.account_state(),
        )),
        Box::new(crate::account_state::VariablesRetriever::new(
            session.variables_state(),
        )),
        Box::new(tidb_executor::cluster_state::ClusterTablesRetriever::new(
            session.viewer_state(),
        )),
    ];
    let servers = |db: &str, table: &str| {
        retrievers
            .iter()
            .filter(|retriever| retriever.serves(&read(db, table)))
            .count()
    };
    for (db, table, expected) in [
        ("information_schema", "SCHEMATA", 1),
        ("information_schema", "TABLES", 1),
        ("information_schema", "COLUMNS", 1),
        ("information_schema", "PROCESSLIST", 1),
        ("information_schema", "TRX_SUMMARY", 1),
        ("information_schema", "STATEMENTS_SUMMARY", 1),
        ("information_schema", "CLIENT_ERRORS_SUMMARY_GLOBAL", 1),
        ("information_schema", "MEMORY_USAGE", 1),
        ("information_schema", "TIDB_INDEX_USAGE", 1),
        ("information_schema", "USER_PRIVILEGES", 1),
        ("information_schema", "USER_ATTRIBUTES", 1),
        ("information_schema", "VARIABLES_INFO", 1),
        ("information_schema", "PROFILING", 1),
        ("information_schema", "SLOW_QUERY", 1),
        ("information_schema", "DDL_JOBS", 1),
        ("information_schema", "ANALYZE_STATUS", 1),
        ("information_schema", "INSPECTION_RULES", 1),
        ("information_schema", "METRICS_SUMMARY", 1),
        ("information_schema", "TIKV_STORE_STATUS", 1),
        ("information_schema", "TIKV_REGION_STATUS", 1),
        ("information_schema", "TABLE_STORAGE_STATS", 1),
        ("information_schema", "ATTRIBUTES", 1),
        ("information_schema", "PLACEMENT_POLICIES", 1),
        ("performance_schema", "session_variables", 1),
        ("information_schema", "CLUSTER_PROCESSLIST", 1),
        ("information_schema", "CLUSTER_SLOW_QUERY", 1),
        ("information_schema", "TIDB_TRX", 1),
        ("information_schema", "DEADLOCKS", 1),
        ("information_schema", "DATA_LOCK_WAITS", 1),
        ("information_schema", "CLUSTER_INFO", 1),
        ("information_schema", "CLUSTER_CONFIG", 1),
        ("information_schema", "CLUSTER_LOG", 1),
        ("information_schema", "TIDB_SERVERS_INFO", 1),
        ("information_schema", "TIDB_HOT_REGIONS_HISTORY", 1),
        ("information_schema", "NO_SUCH_TABLE", 0),
    ] {
        assert_eq!(servers(db, table), expected, "{db}.{table}");
    }
}

/// A memory table is retrieved when its scan is first read (Go
/// `MemTableReaderExec.Next` calls the retriever, which checks the privilege
/// it needs): a viewer without PROCESS is refused by a read of `CLUSTER_INFO`
/// and not by a statement whose plan never reads it.
#[test]
fn a_scan_that_is_never_read_checks_no_privilege() {
    let registry = crate::privilege::PrivilegeRegistry::default();
    let mut admin = bootstrap_session(&registry);
    admin.run("create user 'reader'@'%'").unwrap();
    let mut reader = authenticated_session(&registry, "reader", "%");
    assert!(
        error_of(&mut reader, "select * from information_schema.cluster_info")
            .contains("PROCESS")
    );
    assert_eq!(
        row_text(reader.run(
            "select count(*) from (select * from information_schema.cluster_info limit 0) t"
        )),
        [["0"]]
    );
    assert_eq!(
        row_text(reader.run(
            "select count(*) from information_schema.cluster_info where 1 = 0"
        )),
        [["0"]]
    );
}

/// A multi-table UPDATE or DELETE joining a memory table: the plan reads the
/// memory table's own columns, with no hidden handle (it stores no rows), so
/// the write splits each read row by those widths. `7 columns, expected 9`
/// was the width of a stored table's part applied to the virtual one. The
/// answers are Go's, for a heap table and a clustered one as the target.
#[test]
fn multi_table_writes_join_memory_tables() {
    let mut session = Session::new();
    session.run("create database memtbl").unwrap();
    session.run("use memtbl").unwrap();
    session
        .run("create table t5 (a int, n varchar(64))")
        .unwrap();
    session
        .run("create table t6 (a bigint primary key, n varchar(64))")
        .unwrap();
    session
        .run("insert into t5 values (1, 'memtbl'), (2, 'nomatch'), (3, 'memtbl')")
        .unwrap();
    session
        .run("insert into t6 values (1, 'memtbl'), (2, 'nomatch')")
        .unwrap();
    let rows = |session: &mut Session, table: &str| {
        row_text(session.run(&format!("select * from {table} order by a")))
    };
    session
        .run("delete t5 from t5 join information_schema.schemata s on s.schema_name = t5.n")
        .unwrap();
    assert_eq!(rows(&mut session, "t5"), [["2", "nomatch"]]);
    session
        .run("insert into t5 values (1, 'memtbl'), (4, 'memtbl')")
        .unwrap();
    session
        .run(
            "delete t5 from information_schema.schemata s join t5 on s.schema_name = t5.n \
             where t5.a = 4",
        )
        .unwrap();
    assert_eq!(rows(&mut session, "t5"), [["1", "memtbl"], ["2", "nomatch"]]);
    session
        .run(
            "update t5 join information_schema.schemata s on s.schema_name = t5.n \
             set t5.a = t5.a + 100",
        )
        .unwrap();
    assert_eq!(rows(&mut session, "t5"), [["2", "nomatch"], ["101", "memtbl"]]);
    session
        .run(
            "delete t6, t5 from t6 join t5 on t5.a = 101 + 0 * t6.a \
             join information_schema.schemata s on s.schema_name = t6.n",
        )
        .unwrap();
    assert_eq!(rows(&mut session, "t5"), [["2", "nomatch"]]);
    assert_eq!(rows(&mut session, "t6"), [["2", "nomatch"]]);
    session
        .run("delete from t6 using t6 join information_schema.schemata s on s.schema_name = t6.n")
        .unwrap();
    assert_eq!(rows(&mut session, "t6"), [["2", "nomatch"]]);
}

/// Go `setDataFromIndexUsage` lists every table of every schema the user may
/// see, the memory schemas' included, with each table's `TableInfo` indexes
/// (gorun as root).
#[test]
fn index_usage_lists_the_memory_schemas_indexes_like_go() {
    let mut session = Session::new();
    assert_eq!(
        crate::tests_support::row_text(session.run(
            "select table_schema, table_name, index_name from information_schema.tidb_index_usage \
             where table_schema in ('INFORMATION_SCHEMA','PERFORMANCE_SCHEMA','METRICS_SCHEMA')"
        )),
        vec![
            vec!["INFORMATION_SCHEMA", "CLUSTER_SLOW_QUERY", "primary"],
            vec!["PERFORMANCE_SCHEMA", "events_statements_summary_by_digest", "schema_name"],
            vec!["PERFORMANCE_SCHEMA", "status_by_connection", "primary"],
        ]
    );
}
