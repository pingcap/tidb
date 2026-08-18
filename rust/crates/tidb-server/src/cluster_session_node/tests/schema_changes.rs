//! DDL on this node: the shapes the cluster path cannot express (refused by
//! their own reason), and the two catalogs that have to catch up after one
//! that runs -- the node's immediately, the connection's at its next
//! statement, and never inside an explicit transaction.

use super::super::*;
use super::node_fixture::*;
use crate::configured_user_store::ConfiguredUserStore;
use crate::sql_node::{cluster_ddl_error, SqlQueryError};
use std::sync::atomic::Ordering;
use tidb_datatype::Datum;
use tidb_exec::pessimistic_lock_error::commit_outcome_to_sql_error;
use tidb_exec::real_tikv_ddl::classify_session_ddl_commit_error;
use tidb_session::privilege::GlobalPriv;
use tidb_txnkv::region::RegionBackoffKind;
use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, OptimisticTransactionReceipt, RolledBackTransaction, TransactionCause,
    UndeterminedTransaction,
};

struct UndeterminedDdl(SqlQueryError);

impl ClusterDdl for UndeterminedDdl {
    fn execute(&self, _: &DdlStatement) -> Result<ClusterDdlReport, SqlQueryError> {
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
    let invalid = cluster_ddl_error(tidb_exec::real_tikv_ddl::ClusterDdlError::Plan(
        tidb_exec::cluster_ddl::DdlPlanError::InvalidAutoRandom(
            "alter auto_random_base of a non auto_random table".to_owned(),
        ),
    ));
    assert_query_error_packet(
        &invalid,
        8216,
        "Invalid auto random: alter auto_random_base of a non auto_random table",
    );

    let zero = cluster_ddl_error(tidb_exec::real_tikv_ddl::ClusterDdlError::Plan(
        tidb_exec::cluster_ddl::DdlPlanError::AutoIdReadFailed,
    ));
    assert_query_error_packet(
        &zero,
        1467,
        "Failed to read auto-increment value from storage engine",
    );
}

/// A stored-schema change the cluster DDL path cannot express keeps a
/// precise refusal -- and it names its own reason rather than a generic
/// unsupported error.
#[test]
fn a_ddl_shape_the_cluster_path_cannot_express_is_refused_precisely() {
    let (mut session, node) = open_session();
    for (sql, expected) in [
        // ADD/DROP COLUMN, TRUNCATE, and all-column multi-action ALTERs
        // moved to the catalog writer as their courses landed; the mock's
        // named non-modeling answer is their routing witness below. A
        // multi-action ALTER MIXING a column change with an index change
        // remains genuinely inexpressible: the column half folds over one
        // TableInfo, the index half needs a backfill job.
        (
            "ALTER TABLE t ADD COLUMN w BIGINT, ADD INDEX iw (w)",
            "one atomic multi-schema DDL job",
        ),
        (
            "CREATE TABLE fk (id BIGINT PRIMARY KEY, other BIGINT, \
             FOREIGN KEY (other) REFERENCES t (id))",
            "not supported by this node",
        ),
        (
            "CREATE TABLE parts (id BIGINT PRIMARY KEY) PARTITION BY HASH (id) PARTITIONS 2",
            "not supported by this node",
        ),
    ] {
        let error = session
            .execute_write(sql)
            .expect_err("an inexpressible schema change must be refused");
        let message = error.message.clone();
        assert!(
            message.contains(expected),
            "unexpected refusal for {sql}: {message}"
        );
    }
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
        "TRUNCATE TABLE t",
    ] {
        let routed = session
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
fn cluster_create_default_errors_cross_the_schema_route_unchanged() {
    let (session, node) = open_session();
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
        let Err(error) = session.schema_route(sql) else {
            panic!("the invalid default must be refused before catalog publication: {sql}");
        };
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
        .execute_write("CREATE TABLE fresh (id BIGINT PRIMARY KEY, v BIGINT)")
        .expect("the catalog change runs")
        .expect("a DDL answers with an OK packet");
    assert_eq!(outcome.affected_rows, 0);
    assert_eq!(node.ddl.applied.load(Ordering::Acquire), 1);
    assert_eq!(node.catalog.load().schema_version, 12);

    session
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

#[test]
fn modify_auto_random_bits_rebuilds_the_connections_cluster_table() {
    let (mut session, node) = open_session();
    session
        .execute_write("CREATE TABLE ar_cluster (id BIGINT AUTO_RANDOM(5) PRIMARY KEY, v BIGINT)")
        .expect("create the AUTO_RANDOM table");
    session
        .execute_write("ALTER TABLE ar_cluster MODIFY COLUMN id BIGINT AUTO_RANDOM(8)")
        .expect("increase the persisted shard width");
    let shown = rows(&mut session, "SHOW CREATE TABLE ar_cluster");
    let Datum::Bytes(definition) = &shown[0][1] else {
        panic!("SHOW CREATE TABLE returns text: {:?}", shown[0][1]);
    };
    assert!(String::from_utf8_lossy(definition).contains("AUTO_RANDOM(8)"));
    assert_eq!(node.catalog.load().schema_version, 13);

    let error = session
        .execute_write("ALTER TABLE ar_cluster MODIFY COLUMN id BIGINT AUTO_RANDOM(7)")
        .expect_err("decreasing the random width remains invalid");
    assert_eq!(error.code, 8216);
}

/// `DROP TABLE` removes the table from the connection's own catalog too,
/// so the next statement naming it fails as an unknown table rather than
/// reading a table the cluster no longer has.
#[test]
fn drop_table_removes_it_from_the_connections_own_catalog() {
    let (mut session, node) = open_session();
    session
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");
    session
        .execute_write("DROP TABLE t")
        .expect("the catalog change runs");
    assert_eq!(node.catalog.load().schema_version, 12);

    let Err(error) = session.execute("SELECT id FROM t") else {
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
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed the table before renaming it");
    session
        .execute_write("ALTER TABLE t RENAME TO renamed")
        .expect("the rename runs through the catalog writer");
    assert_eq!(node.ddl.applied.load(Ordering::Acquire), 1);
    assert_eq!(node.catalog.load().schema_version, 12);
    assert!(session.execute("SELECT id FROM t").is_err());
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
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed the first table");
    session
        .execute_write("INSERT INTO g (id, grp) VALUES (2, 20)")
        .expect("seed the second table");

    session
        .execute_write("RENAME TABLE t TO t_renamed, g TO g_renamed")
        .expect("one multi-pair rename DDL job runs");

    assert_eq!(node.ddl.applied.load(Ordering::Acquire), 1);
    assert_eq!(node.catalog.load().schema_version, 12);
    assert!(session.execute("SELECT id FROM t").is_err());
    assert!(session.execute("SELECT id FROM g").is_err());
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
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed the first table");
    session
        .execute_write("INSERT INTO g (id, grp) VALUES (2, 20)")
        .expect("seed the second table");

    let error = session
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
    assert!(session.execute("SELECT id FROM t_renamed").is_err());
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
        .execute_write("CREATE TABLE shared (id BIGINT PRIMARY KEY, v BIGINT)")
        .expect("the catalog change runs");
    author
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
        .execute_write("CREATE DATABASE extra CHARACTER SET utf8 COLLATE utf8_general_ci")
        .expect("the catalog change runs");
    session.execute_write("USE extra").expect("USE extra");
    session
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
        .execute_write("DROP DATABASE extra")
        .expect("the catalog change runs");
    assert!(session.execute("SELECT id FROM here").is_err());
    assert_eq!(node.ddl.applied.load(Ordering::Acquire), 3);
}

/// `IF NOT EXISTS` on an object that already exists writes nothing and
/// still answers with an OK packet, as Go does.
#[test]
fn an_already_satisfied_ddl_publishes_nothing() {
    let (mut session, node) = open_session();
    let outcome = session
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
    session.control_transaction("BEGIN").expect("begin");
    session
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("staged insert");
    assert_eq!(node.rows(), 0);

    session
        .execute_write("CREATE TABLE after (id BIGINT PRIMARY KEY)")
        .expect("the catalog change runs");
    assert_eq!(node.rows(), 1, "the DDL committed the open transaction");
    assert_eq!(node.publications.load(Ordering::Acquire), 1);
    assert!(
        !session.session.in_transaction(),
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
        .grant_db("low", "%", "app", GlobalPriv::CreateTemporaryTables.mask());
    let normal_store = ConfiguredUserStore::from_accounts(node.accounts.live.clone());
    let normal_identity = normal_store
        .authenticate_native("low", "127.0.0.1", &SALT, &[])
        .expect("the zero-DDL-privilege account authenticates normally");
    let mut normal =
        open_session_on_with_context(&node, session_context_with_identity(21, normal_identity));
    normal.execute_write("USE app").expect("app is visible");
    normal.control_transaction("BEGIN").expect("begin");

    let denied = normal
        .execute_write("CREATE TABLE blocked (id BIGINT PRIMARY KEY)")
        .expect_err("CREATE must be checked before the cluster DDL seam");
    assert_eq!((denied.code, denied.state), (1142, *b"42000"));
    assert_eq!(
        denied.message,
        "CREATE command denied to user 'low'@'%' for table 'blocked'"
    );
    assert!(
        normal.session.in_transaction(),
        "a denied DDL must not implicitly commit the caller's transaction"
    );
    assert_eq!(node.ddl.applied.load(Ordering::Acquire), 0);
    normal.control_transaction("ROLLBACK").expect("rollback");

    for (sql, database) in [
        ("CREATE DATABASE blocked_db", "blocked_db"),
        ("DROP DATABASE app", "app"),
    ] {
        let denied = normal
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
    reader.control_transaction("BEGIN").expect("begin");
    assert!(rows(&mut reader, "SELECT id FROM t").is_empty());

    author
        .execute_write("DROP TABLE t")
        .expect("the peer drops the table");

    assert!(
        rows(&mut reader, "SELECT id FROM t").is_empty(),
        "a statement inside BEGIN must keep the schema BEGIN saw"
    );
    reader.control_transaction("COMMIT").expect("commit");
    // Once the transaction is over the connection follows the node again.
    assert!(reader.execute("SELECT id FROM t").is_err());
}
