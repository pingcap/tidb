//! DDL on this node: the shapes the cluster path cannot express (refused by
//! their own reason), and the two catalogs that have to catch up after one
//! that runs -- the node's immediately, the connection's at its next
//! statement, and never inside an explicit transaction.

use tidb_ddl::real_tikv_ddl::ClusterDdlReport;
use tidb_privilege::PrivilegeTypeExt;
use super::super::*;
use super::node_fixture::*;
use crate::domain::configured_user_store::ConfiguredUserStore;
use tidb_ddl::real_tikv_ddl::cluster_ddl_error;
use tidb_store_driver::sql_query_error::SqlQueryError;
use std::sync::atomic::Ordering;
use tidb_datatype::Datum;
use tidb_store_driver::pessimistic_lock_error::commit_outcome_to_sql_error;
use tidb_ddl::real_tikv_ddl::classify_session_ddl_commit_error;
use tidb_txnkv::retry::RegionBackoffKind;
use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, OptimisticTransactionReceipt, RolledBackTransaction, TransactionCause,
    UndeterminedTransaction,
};

/// Supplies the authority's report without reimplementing its DDL planner.
/// The session must preserve every warning, irrespective of report variant.
struct ReportDdl {
    statement: DdlStatement,
    report: ClusterDdlReport,
}

impl ClusterDdl for ReportDdl {
    // A cluster without TiFlash stores.
    fn tiflash_store_count(&self) -> Result<u64, SqlQueryError> {
        Ok(0)
    }

    fn execute(
        &self,
        statement: &DdlStatement,
        _job_context: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        match (statement, &self.statement) {
            (
                DdlStatement::DropTables { names, if_exists },
                DdlStatement::DropTables {
                    names: expected,
                    if_exists: expected_if_exists,
                },
            ) => {
                assert_eq!((names, if_exists), (expected, expected_if_exists));
            }
            (
                DdlStatement::AcceptedNoOp {
                    schema,
                    table,
                    warning,
                },
                DdlStatement::AcceptedNoOp {
                    schema: expected_schema,
                    table: expected_table,
                    warning: expected_warning,
                },
            ) => {
                assert_eq!(
                    (schema, table, warning),
                    (expected_schema, expected_table, expected_warning)
                );
            }
            _ => panic!("unexpected DDL route: {statement:?}"),
        }
        Ok(self.report.clone())
    }
}

#[test]
fn ddl_reports_preserve_warning_order_levels_and_statement_lifetime() {
    use tidb_ddl::real_tikv_ddl::DdlWarningLevel::{Note, Warning};

    let node = MockNode::start();
    let notes = vec![
        tidb_context::SqlWarn::with_code(Note, 1051, "Unknown table 'app.absent_a'"),
        tidb_context::SqlWarn::with_code(Note, 1051, "Unknown table 'app.absent_b'"),
    ];
    // An applied multi-drop and an entirely suppressed multi-drop carry the
    // same ordered notes. Neither report variant determines the level.
    for applied in [true, false] {
        let report = if applied {
            ClusterDdlReport::Applied {
                schema_version: 12,
                created_id: None,
                warnings: notes.clone(),
            }
        } else {
            ClusterDdlReport::AlreadySatisfied {
                detail: "missing tables".to_owned(),
                warnings: notes.clone(),
            }
        };
        let mut names = vec![
            ("app".to_owned(), "absent_a".to_owned()),
            ("app".to_owned(), "absent_b".to_owned()),
        ];
        let sql = if applied {
            names.insert(0, ("app".to_owned(), "t".to_owned()));
            "DROP TABLE IF EXISTS t, absent_a, absent_b"
        } else {
            "DROP TABLE IF EXISTS absent_a, absent_b"
        };
        let mut session = open_session_on_with_ddl(
            &node,
            Arc::new(ReportDdl {
                statement: DdlStatement::DropTables {
                    names,
                    if_exists: true,
                },
                report,
            }),
        );
        session
            .node_conn()
            .execute_write(sql)
            .expect("route multi-table DROP");
        let expected: Vec<_> = notes
            .iter()
            .map(|note| {
                vec![
                    Datum::new_string("Note"),
                    Datum::UInt(u64::from(note.code())),
                    Datum::new_string(note.message()),
                ]
            })
            .collect();
        assert_eq!(rows(&mut session, "SHOW WARNINGS"), expected);
        assert_eq!(rows(&mut session, "SHOW WARNINGS"), expected);
        rows(&mut session, "SELECT 1");
        assert!(rows(&mut session, "SHOW WARNINGS").is_empty());
    }

    let message = "ALTER TABLE WITH VALIDATION is currently unsupported".to_owned();
    let mut session = open_session_on_with_ddl(
        &node,
        Arc::new(ReportDdl {
            statement: DdlStatement::AcceptedNoOp {
                schema: "app".to_owned(),
                table: "t".to_owned(),
                warning: Some((8200, message.clone())),
            },
            report: ClusterDdlReport::AlreadySatisfied {
                detail: "no change".to_owned(),
                warnings: vec![tidb_context::SqlWarn::with_code(Warning, 8200, message.clone())],
            },
        }),
    );
    session
        .node_conn()
        .execute_write("ALTER TABLE t WITH VALIDATION")
        .expect("route option-only ALTER");
    assert_eq!(
        rows(&mut session, "SHOW WARNINGS"),
        vec![vec![
            Datum::new_string("Warning"),
            Datum::UInt(8200),
            Datum::new_string(message),
        ]]
    );
    assert_eq!(node.ddl.applied.load(Ordering::Acquire), 0);
    assert_eq!(node.catalog.load().schema_version, 11);
}

struct UndeterminedDdl(SqlQueryError);

impl ClusterDdl for UndeterminedDdl {
    // A cluster without TiFlash stores.
    fn tiflash_store_count(&self) -> Result<u64, SqlQueryError> {
        Ok(0)
    }

    fn execute(
        &self,
        _: &DdlStatement,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(self.0.clone())
    }
}

#[test]
fn full_cluster_ddl_keeps_an_undetermined_verdict_connection_fatal() {
    let outcome = OptimisticCommitOutcome::Undetermined(UndeterminedTransaction {
        receipt: OptimisticTransactionReceipt::new(1, 2, b"key".to_vec(), 1),
        cause: TransactionCause::Transport {
            detail: "commit response lost".to_owned(),
        },
    });
    let lock_error = commit_outcome_to_sql_error(&outcome)
        .expect_err("an unknown commit verdict must not answer success");
    let ddl_error = classify_session_ddl_commit_error(61, lock_error);
    let query_error = cluster_ddl_error(ddl_error);
    let node = MockNode::start();
    let mut session = open_session_on_with_ddl(&node, Arc::new(UndeterminedDdl(query_error)));
    let query_error = session
        .node_conn()
        .execute_write("CREATE DATABASE uncertain")
        .expect_err("the DDL cannot answer success when its commit response was lost");
    assert_undetermined_closes_without_packet(&query_error);
}

#[test]
fn full_cluster_ddl_keeps_a_region_backoff_error_coded_on_the_wire() {
    let outcome = OptimisticCommitOutcome::RolledBack(RolledBackTransaction {
        receipt: OptimisticTransactionReceipt::new(1, 2, b"key".to_vec(), 1),
        cause: TransactionCause::BackoffExhausted {
            kind: RegionBackoffKind::RegionScheduling,
            detail: "regionScheduling backoffer exhausted".to_owned(),
        },
    });
    let lock_error = commit_outcome_to_sql_error(&outcome)
        .expect_err("an exhausted region backoff cannot answer success");
    let query_error = cluster_ddl_error(classify_session_ddl_commit_error(61, lock_error));
    assert_eq!(
        query_error.code,
        tidb_error::tidb::errcode::ErrRegionUnavailable
    );
    assert_query_error_packet(
        &query_error,
        tidb_error::tidb::errcode::ErrRegionUnavailable,
        "Region is unavailable",
    );
}

#[test]
fn auto_random_rebase_errors_keep_their_tidb_codes_on_the_cluster_wire() {
    let invalid = cluster_ddl_error(tidb_ddl::real_tikv_ddl::ClusterDdlError::Plan(
        tidb_ddl::cluster_ddl::DdlPlanError::from(
            tidb_dbterror::ERR_INVALID_AUTO_RANDOM.generate_with_stack_by_args(&[
                ("alter auto_random_base of a non auto_random table".to_owned()).into(),
            ]),
        ),
    ));
    assert_query_error_packet(
        &invalid,
        8216,
        "Invalid auto random: alter auto_random_base of a non auto_random table",
    );

    let zero = cluster_ddl_error(tidb_ddl::real_tikv_ddl::ClusterDdlError::Plan(
        tidb_ddl::cluster_ddl::DdlPlanError::from(
            tidb_error::catalog::autoid::ERR_AUTOINC_READ_FAILED.generate_with_stack_by_args(&[]),
        ),
    ));
    assert_query_error_packet(
        &zero,
        1467,
        "Failed to read auto-increment value from storage engine",
    );
}

/// Every stored-schema change shape, single or bundled, reaches the catalog
/// writer rather than a generic unsupported route.
#[test]
fn ddl_shapes_reach_the_catalog_writer() {
    let (mut session, node) = open_session();
    // Go gives standalone and single-action ALTER index changes the same DDL
    // job. Both spellings must therefore reach the catalog writer rather than
    // the generic unsupported route. This mock has no rows to walk, so its
    // source-shaped backfill refusal is the routing witness.
    for sql in [
        "CREATE INDEX i ON t (v)",
        "ALTER TABLE t ADD INDEX i (v)",
        "ALTER TABLE t DROP INDEX i",
        // Routed to the catalog writer since their courses landed; this
        // mock does not model them, and saying so IS the routing witness.
        "ALTER TABLE t ADD COLUMN w BIGINT",
        "ALTER TABLE t ADD COLUMN w BIGINT, DROP COLUMN v",
        "ALTER TABLE t ADD COLUMN w BIGINT, ADD INDEX iw (w)",
        // Go runs every spec's executor function before `DoDDLJob` refuses
        // the RENAME's job beside the column's sub-job; see
        // `multi_schema_change_refuses_a_rename_beside_another_spec`.
        "ALTER TABLE t ADD COLUMN w BIGINT, RENAME TO t2",
        "TRUNCATE TABLE t",
    ] {
        let routed = session
            .node_conn()
            .execute_write(sql)
            .expect_err("the mock writer holds no rows");
        assert!(
            routed
                .message
                .contains("cannot model an index change's backfill")
                || routed
                    .message
                    .contains("does not model column or truncate changes"),
            "{sql} must reach the catalog writer: {}",
            routed.message
        );
    }
    // Nothing was published: a refusal happens before the writer commits
    // anything at all.
    assert_eq!(node.ddl.applied.load(Ordering::Acquire), 0);
    assert_eq!(node.catalog.load().schema_version, 11);
}

/// A coded DEFAULT refusal keeps its errno, SQLSTATE, and source message when
/// the full cluster session turns DDL admission into its client-facing error.
#[test]
fn cluster_create_default_errors_reach_the_client_unchanged() {
    let (mut session, node) = open_session();
    for (sql, code, state, message) in [
        (
            "CREATE TABLE bad_function (a INT DEFAULT (ABS(1)))",
            3770,
            *b"HY000",
            "Default value expression of column 'a' contains a disallowed function: `abs`.",
        ),
        (
            "CREATE TABLE bad_fsp (ts TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP)",
            1067,
            *b"42000",
            "Invalid default value for 'ts'",
        ),
    ] {
        // Go refuses the DEFAULT in the DDL executor (`BuildTableInfoWithStmt`
        // via `executor.CreateTable`), after `DDLExec.Next`'s implicit commit
        // and before any job is published.
        let error = session
            .node_conn()
            .execute_write(sql)
            .expect_err("the invalid default is refused before catalog publication");
        assert_eq!((error.code, error.state), (code, state), "{sql}");
        assert_eq!(error.message, message, "{sql}");
    }

    assert_eq!(node.ddl.applied.load(Ordering::Acquire), 0);
    assert_eq!(node.catalog.load().schema_version, 11);
}

/// The unit this mode gained: a `CREATE TABLE` issued through the wide-SQL
/// session executes as a cluster catalog change, and the SAME connection
/// can then write and read the new table -- which it can only do if its
/// own tables were rebuilt.
#[test]
fn create_table_runs_and_the_same_connection_uses_the_new_table() {
    let (mut session, node) = open_session();
    let outcome = session
        .node_conn()
        .execute_write("CREATE TABLE fresh (id BIGINT PRIMARY KEY, v BIGINT)")
        .expect("the catalog change runs")
        .expect("a DDL answers with an OK packet");
    assert_eq!(outcome.affected_rows, 0);
    assert_eq!(node.ddl.applied.load(Ordering::Acquire), 1);
    assert_eq!(node.catalog.load().schema_version, 12);

    session
        .node_conn()
        .execute_write("INSERT INTO fresh (id, v) VALUES (1, 10), (2, 20)")
        .expect("the new table takes writes on the same connection");
    assert_eq!(
        rows(&mut session, "SELECT id, v FROM fresh ORDER BY id"),
        vec![
            vec![Datum::Int(1), Datum::Int(10)],
            vec![Datum::Int(2), Datum::Int(20)]
        ]
    );
    // The rows went through the ordinary write path, into the mock
    // cluster, and every statement's snapshot was finished.
    assert_eq!(node.rows(), 2);
    assert_eq!(node.live.load(Ordering::Acquire), 0);
    // The connection's older tables survived the rebuild.
    assert!(rows(&mut session, "SELECT id FROM t").is_empty());
}

/// Pinned `ddlHandlerImpl.HandleDDLEvent` logs a subscriber failure and
/// returns nil. A statistics restricted-session failure after the catalog
/// commit therefore cannot turn a successfully published DDL into an error.
#[test]
fn ddl_success_is_not_replaced_by_a_statistics_subscriber_error() {
    let (mut session, node) = open_session();
    node.fail_next_begin.store(true, Ordering::Release);

    let outcome = session
        .node_conn()
        .execute_write("CREATE TABLE stats_subscriber_failure (id BIGINT)")
        .expect("subscriber errors are ignored after the DDL commit")
        .expect("DDL returns an OK packet");

    assert_eq!(outcome.affected_rows, 0);
    assert!(
        node.catalog
            .load()
            .find_table("app", "stats_subscriber_failure")
            .is_some(),
        "the catalog change remains published"
    );
}

/// `DROP TABLE` removes the table from the connection's own catalog too,
/// so the next statement naming it fails as an unknown table rather than
/// reading a table the cluster no longer has.
#[test]
fn drop_table_removes_it_from_the_connections_own_catalog() {
    let (mut session, node) = open_session();
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");
    session
        .node_conn()
        .execute_write("DROP TABLE t")
        .expect("the catalog change runs");
    assert_eq!(node.catalog.load().schema_version, 12);

    let Err(error) = session.node_conn().execute("SELECT id FROM t") else {
        panic!("a dropped table must not answer");
    };
    assert!(
        error.message.to_lowercase().contains("t"),
        "unexpected error: {}",
        error.message
    );
    // The sibling table is untouched.
    assert!(rows(&mut session, "SELECT id FROM g").is_empty());
}

/// `ALTER TABLE ... RENAME TO` must take the same cluster catalog route as
/// the Go rename job: existing rows stay under the stable table ID, while the
/// next statement resolves only the new name.
#[test]
fn alter_table_rename_rebuilds_the_connection_catalog_without_losing_rows() {
    let (mut session, node) = open_session();
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed the table before renaming it");
    session
        .node_conn()
        .execute_write("ALTER TABLE t RENAME TO renamed")
        .expect("the rename runs through the catalog writer");
    assert_eq!(node.ddl.applied.load(Ordering::Acquire), 1);
    assert_eq!(node.catalog.load().schema_version, 12);
    assert!(session.node_conn().execute("SELECT id FROM t").is_err());
    assert_eq!(
        rows(&mut session, "SELECT id, v FROM renamed"),
        vec![vec![Datum::Int(1), Datum::Int(10)]],
        "a rename changes metadata, not the table's physical identity"
    );
}

/// Go treats every pair in one `RENAME TABLE` statement as one DDL job: it
/// validates the full sequence, publishes one schema version, then exposes
/// every renamed table together.  The cluster session must not reject the
/// second pair merely because its catalog writer used to model only a
/// single-table diff.
#[test]
fn rename_table_multiple_pairs_publish_one_catalog_change() {
    let (mut session, node) = open_session();
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed the first table");
    session
        .node_conn()
        .execute_write("INSERT INTO g (id, grp) VALUES (2, 20)")
        .expect("seed the second table");

    session
        .node_conn()
        .execute_write("RENAME TABLE t TO t_renamed, g TO g_renamed")
        .expect("one multi-pair rename DDL job runs");

    assert_eq!(node.ddl.applied.load(Ordering::Acquire), 1);
    assert_eq!(node.catalog.load().schema_version, 12);
    assert!(session.node_conn().execute("SELECT id FROM t").is_err());
    assert!(session.node_conn().execute("SELECT id FROM g").is_err());
    assert_eq!(
        rows(&mut session, "SELECT id, v FROM t_renamed"),
        vec![vec![Datum::Int(1), Datum::Int(10)]]
    );
    assert_eq!(
        rows(&mut session, "SELECT id, grp FROM g_renamed"),
        vec![vec![Datum::Int(2), Datum::Int(20)]]
    );
}

#[test]
fn rename_table_later_pair_failure_leaves_every_prior_pair_unpublished() {
    let (mut session, node) = open_session();
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed the first table");
    session
        .node_conn()
        .execute_write("INSERT INTO g (id, grp) VALUES (2, 20)")
        .expect("seed the second table");

    let error = session
        .node_conn()
        .execute_write("RENAME TABLE t TO t_renamed, g TO t_renamed")
        .expect_err("the occupied second target rejects the complete DDL job");
    assert!(error.message.contains("already exists"), "{error:?}");
    assert_eq!(node.ddl.applied.load(Ordering::Acquire), 0);
    assert_eq!(node.catalog.load().schema_version, 11);
    assert_eq!(
        rows(&mut session, "SELECT id, v FROM t"),
        vec![vec![Datum::Int(1), Datum::Int(10)]]
    );
    assert_eq!(
        rows(&mut session, "SELECT id, grp FROM g"),
        vec![vec![Datum::Int(2), Datum::Int(20)]]
    );
    assert!(session
        .node_conn()
        .execute("SELECT id FROM t_renamed")
        .is_err());
}

/// A second connection, opened before the DDL, notices it at its next
/// statement: the node's catalog moved, so the connection rebuilds its
/// tables rather than serving the schema it opened with.
#[test]
fn a_second_connection_sees_the_new_table_after_the_ddl() {
    let node = MockNode::start();
    let mut author = open_session_on(&node);
    let mut peer = open_session_on(&node);
    // The peer is live and bound to the pre-DDL catalog.
    assert!(rows(&mut peer, "SELECT id FROM t").is_empty());

    author
        .node_conn()
        .execute_write("CREATE TABLE shared (id BIGINT PRIMARY KEY, v BIGINT)")
        .expect("the catalog change runs");
    author
        .node_conn()
        .execute_write("INSERT INTO shared (id, v) VALUES (5, 50)")
        .expect("the author writes the new table");

    assert_eq!(
        rows(&mut peer, "SELECT id, v FROM shared"),
        vec![vec![Datum::Int(5), Datum::Int(50)]],
        "a connection that outlived the DDL must serve the new table"
    );
}

/// `CREATE DATABASE` and `DROP DATABASE` route the same way, and `USE`
/// reaches a database this node created.
#[test]
fn create_and_drop_database_route_to_the_catalog_writer() {
    let (mut session, node) = open_session();
    session
        .node_conn()
        .execute_write("CREATE DATABASE extra CHARACTER SET utf8 COLLATE utf8_general_ci")
        .expect("the catalog change runs");
    session
        .node_conn()
        .execute_write("USE extra")
        .expect("USE extra");
    session
        .node_conn()
        .execute_write("CREATE TABLE here (id BIGINT PRIMARY KEY)")
        .expect("a table in the new database");
    assert!(rows(&mut session, "SELECT id FROM here").is_empty());
    let catalog = node.catalog.load();
    let database = catalog
        .databases
        .iter()
        .find(|database| database.info.name.lowercase() == "extra")
        .expect("the created database is published");
    assert_eq!(database.info.charset, "utf8");
    assert_eq!(database.info.collate, "utf8_general_ci");
    assert_eq!(database.tables[0].charset, "utf8");
    assert_eq!(database.tables[0].collate, "utf8_general_ci");
    drop(catalog);

    session
        .node_conn()
        .execute_write("DROP DATABASE extra")
        .expect("the catalog change runs");
    assert!(session.node_conn().execute("SELECT id FROM here").is_err());
    assert_eq!(node.ddl.applied.load(Ordering::Acquire), 3);
}

/// `IF NOT EXISTS` on an object that already exists writes nothing and
/// still answers with an OK packet, as Go does.
#[test]
fn an_already_satisfied_ddl_publishes_nothing() {
    let (mut session, node) = open_session();
    let outcome = session
        .node_conn()
        .execute_write("CREATE TABLE IF NOT EXISTS t (id BIGINT PRIMARY KEY)")
        .expect("an IF NOT EXISTS no-op succeeds")
        .expect("it answers with an OK packet");
    assert_eq!(outcome.affected_rows, 0);
    assert_eq!(node.ddl.applied.load(Ordering::Acquire), 0);
    assert_eq!(node.catalog.load().schema_version, 11);
}

/// DDL commits an open transaction first, as MySQL and Go both do. The
/// staged writes are published rather than lost, and the transaction is
/// over when the DDL runs.
#[test]
fn a_ddl_implicitly_commits_the_open_transaction() {
    let (mut session, node) = open_session();
    session
        .node_conn()
        .control_transaction("BEGIN")
        .expect("begin");
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("staged insert");
    assert_eq!(node.rows(), 0);

    session
        .node_conn()
        .execute_write("CREATE TABLE after (id BIGINT PRIMARY KEY)")
        .expect("the catalog change runs");
    assert_eq!(node.rows(), 1, "the DDL committed the open transaction");
    assert_eq!(node.publications.load(Ordering::Acquire), 1);
    assert!(
        !session.in_transaction(),
        "the implicit commit ends the transaction"
    );
    // And the new table is usable straight away, which it could not be if
    // the connection still believed it was inside the old transaction.
    assert!(rows(&mut session, "SELECT id FROM after").is_empty());
}

/// DDL privilege admission belongs before the implicit commit and before the
/// cluster catalog writer. A skip-grant-table identity crosses the same door
/// but deliberately bypasses its grant lookup.
#[test]
fn ddl_privileges_are_checked_before_cluster_commit_and_skip_grant_bypasses_them() {
    let node = MockNode::start();
    assert!(node.accounts.live.create_user("low", "%", ""));
    // Make `app` visible without granting CREATE or DROP.
    node.accounts
        .live
        .grant_db("low", "%", "app", tidb_mysql::CreateTMPTablePriv.mask());
    let normal_store = ConfiguredUserStore::from_accounts(node.accounts.live.clone());
    let normal_identity = normal_store
        .authenticate_native("low", "127.0.0.1", &SALT, &[])
        .expect("the zero-DDL-privilege account authenticates normally");
    let mut normal =
        open_session_on_with_context(&node, session_context_with_identity(21, normal_identity));
    normal
        .node_conn()
        .execute_write("USE app")
        .expect("app is visible");
    normal
        .node_conn()
        .control_transaction("BEGIN")
        .expect("begin");

    let denied = normal
        .node_conn()
        .execute_write("CREATE TABLE blocked (id BIGINT PRIMARY KEY)")
        .expect_err("CREATE must be checked before the cluster DDL seam");
    assert_eq!((denied.code, denied.state), (1142, *b"42000"));
    assert_eq!(
        denied.message,
        "CREATE command denied to user 'low'@'%' for table 'blocked'"
    );
    assert!(
        normal.in_transaction(),
        "a denied DDL must not implicitly commit the caller's transaction"
    );
    assert_eq!(node.ddl.applied.load(Ordering::Acquire), 0);
    normal
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("rollback");

    for (sql, database) in [
        ("CREATE DATABASE blocked_db", "blocked_db"),
        ("DROP DATABASE app", "app"),
    ] {
        let denied = normal
            .node_conn()
            .execute_write(sql)
            .expect_err("database DDL needs its schema-scoped privilege");
        assert_eq!((denied.code, denied.state), (1044, *b"42000"), "{sql}");
        assert_eq!(
            denied.message,
            format!("Access denied for user 'low'@'%' to database '{database}'"),
            "{sql}"
        );
    }
    assert_eq!(node.ddl.applied.load(Ordering::Acquire), 0);

    let bypass_store =
        ConfiguredUserStore::from_accounts(node.accounts.live.clone()).with_skip_grant_table(true);
    let bypass_identity = bypass_store
        .authenticate_native("ghost", "127.0.0.1", &SALT, b"ignored")
        .expect("skip-grant-table admits an identity without an account row");
    let mut bypass =
        open_session_on_with_context(&node, session_context_with_identity(22, bypass_identity));
    bypass
        .node_conn()
        .execute_write("CREATE TABLE app.allowed (id BIGINT PRIMARY KEY)")
        .expect("skip-grant-table bypasses DDL authorization");
    assert_eq!(node.ddl.applied.load(Ordering::Acquire), 1);
}

/// Inside an explicit transaction the connection keeps the schema its
/// `BEGIN` saw, exactly as it keeps its snapshot: a peer's DDL must not
/// change the tables a running transaction reads.
#[test]
fn a_transaction_keeps_the_schema_its_begin_saw() {
    let node = MockNode::start();
    let mut reader = open_session_on(&node);
    let mut author = open_session_on(&node);
    reader
        .node_conn()
        .control_transaction("BEGIN")
        .expect("begin");
    assert!(rows(&mut reader, "SELECT id FROM t").is_empty());

    author
        .node_conn()
        .execute_write("DROP TABLE t")
        .expect("the peer drops the table");

    assert!(
        rows(&mut reader, "SELECT id FROM t").is_empty(),
        "a statement inside BEGIN must keep the schema BEGIN saw"
    );
    reader
        .node_conn()
        .control_transaction("COMMIT")
        .expect("commit");
    // Once the transaction is over the connection follows the node again.
    assert!(reader.node_conn().execute("SELECT id FROM t").is_err());
}

/// Every catalog refusal already rendered Go's own message, but only two
/// variants carried a CODE, so the rest reached a client as 1105 -- the code
/// a MySQL client reads as "unknown error" rather than the specific one it
/// switches on. `pkg/errno` names each; these pin the pairing.
#[test]
fn a_catalog_refusal_carries_the_code_mysql_clients_switch_on() {
    use tidb_ddl::cluster_ddl::DdlPlanError;

    let coded = |error: DdlPlanError| {
        let query_error = cluster_ddl_error(tidb_ddl::real_tikv_ddl::ClusterDdlError::Plan(error));
        (query_error.code, query_error.message)
    };

    let (code, message) = coded(DdlPlanError::from(
        tidb_error::catalog::infoschema::ERR_DATABASE_NOT_EXISTS
            .generate_with_stack_by_args(&[("nope".to_owned()).into()]),
    ));
    assert_eq!(code, 1049);
    assert_eq!(message, "Unknown database 'nope'");

    let (code, message) = coded(DdlPlanError::from(
        tidb_error::catalog::infoschema::ERR_DATABASE_EXISTS
            .generate_with_stack_by_args(&[("dup".to_owned()).into()]),
    ));
    assert_eq!(code, 1007);
    assert_eq!(message, "Can't create database 'dup'; database exists");

    let (code, message) = coded(DdlPlanError::from(
        tidb_error::catalog::infoschema::ERR_TABLE_DROP_EXISTS.generate_with_stack_by_args(&[
            format!("{}.{}", ("s".to_owned()), ("t".to_owned())).into(),
        ]),
    ));
    assert_eq!(code, 1051);
    assert_eq!(message, "Unknown table 's.t'");

    let (code, message) =
        coded(DdlPlanError::from(
            tidb_error::catalog::infoschema::ERR_TABLE_EXISTS.generate_with_stack_by_args(&[
                format!("{}.{}", ("s".to_owned()), ("t".to_owned())).into(),
            ]),
        ));
    assert_eq!(code, 1050);
    assert_eq!(message, "Table 's.t' already exists");

    let (code, message) = coded(DdlPlanError::from(
        tidb_error::catalog::infoschema::ERR_COLUMN_EXISTS
            .generate_with_stack_by_args(&[("a".to_owned()).into()]),
    ));
    assert_eq!(code, 1060);
    assert_eq!(message, "Duplicate column name 'a'");

    let (code, message) = coded(DdlPlanError::from(
        tidb_dbterror::ERR_DUP_KEY_NAME
            .generate_with_stack_by_args(&[("ix".to_owned()).into()]),
    ));
    assert_eq!(code, 1061);
    assert_eq!(message, "Duplicate key name 'ix'");

    let (code, message) = coded(DdlPlanError::from(
        tidb_dbterror::ERR_KEY_COLUMN_DOES_NOT_EXITS
            .generate_with_stack(format!("column does not exist: {}", ("c".to_owned()))),
    ));
    assert_eq!((code, message.as_str()), (1072, "column does not exist: c"));
}

/// REPRODUCTION PROBE (rung 8's 9007-at-COMMIT): a schema move landing under
/// an open transaction must not fail its COMMIT with the narrow guard's
/// "Write conflict, please retry the transaction" -- with MDL on (the
/// default), Go's `validator.Check` skips the delta comparison entirely
/// (`pkg/infoschema/isvalidator/validator.go:236-241`), because the DDL
/// waits for the transaction instead.
#[test]
fn commit_survives_a_schema_move_under_the_transaction() {
    let (mut session, node) = open_session();
    session
        .node_conn()
        .control_transaction("BEGIN")
        .expect("begin");
    let statement = session
        .node_conn().prepare_general("UPDATE t SET v = 90 WHERE id = 9")
        .expect("prepare");
    session
        .node_conn().execute_general(&statement, &[])
        .expect("update inside the transaction");

    // The reloader's shape: a Go peer's DDL bumps the cluster catalog while
    // this transaction is open. The catalog CONTENT is unchanged -- rung 8's
    // failing workload never touches the DDL'd table.
    let mut moved = (*node.catalog.load()).clone();
    moved.schema_version += 1;
    node.catalog.store(moved);

    let outcome = session.node_conn().control_transaction("COMMIT");
    assert!(
        outcome.is_ok(),
        "an unrelated schema move must not fail COMMIT: {:?}",
        outcome.err()
    );
}

/// With `enable-table-lock` on, LOCK TABLES is a cluster lock job: the lock
/// is stored on the table (`onLockTables`), another connection meets it as
/// `ErrTableLocked` (8020), UNLOCK releases it, and a closing connection
/// releases what it holds through the same job (`session.Close`).
#[test]
fn lock_tables_run_as_a_cluster_lock_job() {
    let previous = tidb_config::config_tree::config::get_global_config();
    tidb_config::config_tree::config::update_global(|config| config.enable_table_lock = true);
    let stored_lock = |node: &MockNode| {
        node.catalog
            .load()
            .find_table("app", "t")
            .and_then(|(_, table)| table.lock.as_ref().map(|lock| lock.read().clone()))
    };
    let (mut first, node) = open_session();
    let mut second = open_session_on_with_context(&node, session_context(2));
    first
        .node_conn()
        .execute_write("LOCK TABLES t WRITE")
        .expect("lock");
    let lock = stored_lock(&node).expect("the lock is stored on the table");
    assert_eq!(lock.tp, tidb_ast::TableLockType::WRITE);
    assert_eq!(lock.state, tidb_model::TableLockState::PUBLIC);
    assert_eq!(
        lock.sessions
            .iter()
            .map(|session| session.session_id)
            .collect::<Vec<_>>(),
        [1]
    );
    let Err(error) = second.node_conn().execute("SELECT id FROM app.t") else {
        panic!("another connection must meet the WRITE lock");
    };
    assert_eq!(error.code, 8020, "{}", error.message);
    // The holder still writes its own table.
    first
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("the lock holder writes");

    first
        .node_conn()
        .execute_write("UNLOCK TABLES")
        .expect("unlock");
    assert!(stored_lock(&node).is_none());
    rows(&mut second, "SELECT id FROM app.t");

    // Go `CleanupTableLock`: the IsCleanup job drops every holder's lock.
    first
        .node_conn()
        .execute_write("LOCK TABLES t WRITE")
        .expect("lock to clean");
    second
        .node_conn()
        .execute_write("ADMIN CLEANUP TABLE LOCK app.t")
        .expect("cleanup");
    assert!(stored_lock(&node).is_none());
    first
        .node_conn()
        .execute_write("UNLOCK TABLES")
        .expect("unlock after cleanup");

    first
        .node_conn()
        .execute_write("LOCK TABLES t READ")
        .expect("lock again");
    assert!(stored_lock(&node).is_some());
    drop(first);
    assert!(
        stored_lock(&node).is_none(),
        "a closing connection releases its locks through the cluster job"
    );
    tidb_config::config_tree::config::store_global_config((*previous).clone());
}

/// Go resolves the table before the planner's privilege check: an
/// unsupported ALTER specification on a missing table answers 1146 even to
/// a user without ALTER, and 1142 on a table that exists (captured from Go
/// on this branch).
#[test]
fn an_unsupported_alter_spec_checks_the_table_before_privileges() {
    let node = MockNode::start();
    assert!(node.accounts.live.create_user("low", "%", ""));
    node.accounts
        .live
        .grant_db("low", "%", "app", tidb_mysql::SelectPriv.mask());
    // The mock node's `app` schema already holds a table `t`.
    let store = ConfiguredUserStore::from_accounts(node.accounts.live.clone());
    let identity = store
        .authenticate_native("low", "127.0.0.1", &SALT, &[])
        .expect("the SELECT-only account authenticates");
    let mut low = open_session_on_with_context(&node, session_context_with_identity(22, identity));
    low.node_conn().execute_write("USE app").unwrap();
    let missing = low
        .node_conn()
        .execute_write("ALTER TABLE nosuch REBUILD PARTITION p0")
        .unwrap_err();
    assert_eq!(
        (missing.code, missing.message.as_str()),
        (1146, "Table 'app.nosuch' doesn't exist")
    );
    let denied = low
        .node_conn()
        .execute_write("ALTER TABLE t REBUILD PARTITION p0")
        .unwrap_err();
    assert_eq!(
        (denied.code, denied.message.as_str()),
        (1142, "ALTER command denied to user 'low'@'%' for table 't'")
    );
}

/// Go's preprocessor resolves the table before the planner's privilege
/// check: a missing table is 1146 to an account holding any privilege on it,
/// and a 1142 naming the statement type (and the table as written) to one
/// holding none (captured from Go on this branch).
#[test]
fn a_missing_ddl_table_is_masked_only_for_an_account_holding_nothing() {
    let node = MockNode::start();
    assert!(node.accounts.live.create_user("low", "%", ""));
    node.accounts
        .live
        .grant_db("low", "%", "app", tidb_mysql::SelectPriv.mask());
    assert!(node.accounts.live.create_user("none", "%", ""));
    let store = ConfiguredUserStore::from_accounts(node.accounts.live.clone());
    let mut low = open_session_on_with_context(
        &node,
        session_context_with_identity(
            23,
            store
                .authenticate_native("low", "127.0.0.1", &SALT, &[])
                .unwrap(),
        ),
    );
    low.node_conn().execute_write("USE app").unwrap();
    for sql in [
        "ALTER TABLE missing ADD COLUMN c INT",
        "CREATE INDEX i ON missing (a)",
        "DROP INDEX i ON missing",
        "TRUNCATE TABLE missing",
        "CREATE TABLE t3 LIKE missing",
    ] {
        let error = low.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (1146, "Table 'app.missing' doesn't exist"),
            "{sql}"
        );
    }
    let mut none = open_session_on_with_context(
        &node,
        session_context_with_identity(
            24,
            store
                .authenticate_native("none", "127.0.0.1", &SALT, &[])
                .unwrap(),
        ),
    );
    for (sql, verb) in [
        ("ALTER TABLE app.Missing ADD COLUMN c INT", "ALTER"),
        ("CREATE INDEX i ON app.Missing (a)", "CREATE"),
        ("DROP INDEX i ON app.Missing", "SELECT"),
        ("TRUNCATE TABLE app.Missing", "SELECT"),
    ] {
        let error = none.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message),
            (
                1142,
                format!("{verb} command denied to user 'none'@'%' for table 'Missing'")
            ),
            "{sql}"
        );
    }
    let error = none
        .node_conn()
        .execute_write("CREATE TABLE app.t9 LIKE app.Missing")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1146, "Table 'app.Missing' doesn't exist")
    );
}
