//! `ANALYZE TABLE` on this node: that it reaches the statistics seam at all,
//! that Go's INSERT-and-SELECT privilege gate is applied before the seam is
//! reached, and that the clauses this node does not run are refused by name.
//!
//! The mock analyzer refuses every table, which is what makes "reached the
//! seam" and "refused before the seam" two different sentences a test can
//! tell apart.

use super::super::*;
use super::mock_cluster::*;
use super::mock_seams::*;
use super::node_fixture::*;
use crate::configured_user_store::ConfiguredUserStore;
use crate::sql_node::{ConnectionCancellation, ConnectionClose};
use std::net::SocketAddr;
use tidb_session::privilege::GlobalPriv;

/// `ANALYZE TABLE` reaches the statistics seam rather than the ordinary
/// statement path.
///
/// The mock analyzer refuses by naming the table, so the assertion is
/// that ITS refusal is what the client is told: had the statement stayed
/// on the ordinary path it would have come back as an unsupported
/// administrative statement instead, which is a different sentence and
/// would have left the cluster's statistics silently untouched.
#[test]
fn analyze_table_routes_to_the_statistics_seam() {
    let (mut session, _node) = open_session();
    let refusal = session
        .execute_write("ANALYZE TABLE t")
        .expect_err("the mock node has no statistics to store")
        .message;
    assert!(
        refusal.contains("the mock node stores no statistics for"),
        "the statistics seam's own refusal must reach the client: {refusal}"
    );
    assert!(
        refusal.contains("`t`"),
        "the refusal must name the table: {refusal}"
    );
}

/// Opens a connection authenticated as `user`, which is how a test says
/// "somebody other than root".
fn open_session_as(node: &MockNode, user: &str) -> ClusterServerSession {
    let cluster = Arc::clone(&node.cluster);
    let factory = ClusterSessionFactory::new(
        Arc::new(MockTransactions(cluster)),
        Arc::clone(&node.ddl) as Arc<dyn ClusterDdl>,
        Arc::clone(&node.accounts) as Arc<dyn ClusterAccountWriter>,
        Arc::clone(&node.sysvars) as Arc<dyn crate::cluster_sysvar_seam::ClusterSysvarWriter>,
        Arc::new(MockAnalyze) as Arc<dyn ClusterAnalyze>,
        Arc::clone(&node.catalog),
        node.accounts.live.clone(),
        node.sysvars.live.clone(),
        Arc::new(SharedStats::new(
            tidb_exec::stats_watch::StatsSnapshot::new(),
        )),
        Arc::new(crate::cluster_session::LocalTableAutoIds::default()),
    );
    let users =
        ConfiguredUserStore::parse(&format!("{user}\t%\tmysql_native_password\t{ABC_HASH}\n"))
            .expect("configured user store");
    let identity = users
        .authenticate_native(user, "127.0.0.1", &SALT, &scramble(b"abc", &SALT))
        .expect("authenticated identity");
    let peer_addr: SocketAddr = "127.0.0.1:4001".parse().expect("peer address");
    let mut session = factory
        .open_session(SessionContext {
            connection_id: 2,
            peer_addr,
            identity,
            cancellation: ConnectionCancellation::default(),
            close: ConnectionClose::default(),
        })
        .expect("the cluster session opens");
    session.execute_write("USE app").expect("USE app");
    session
}

/// An account with no privilege on the table cannot `ANALYZE` it.
///
/// The statistics an `ANALYZE` writes are not metadata: a TopN entry in
/// `mysql.stats_top_n` is an ACTUAL COLUMN VALUE. Letting any
/// authenticated connection analyze any table therefore hands out the
/// table's contents, which is why Go requires INSERT and SELECT on it
/// (`planbuilder.go:3205` `requireInsertAndSelectPriv`).
///
/// The assertion is on the seam as much as on the error: the mock
/// analyzer refuses every table by name, so reaching it at all would show
/// up here as ITS message rather than the access-denied one.
#[test]
fn analyze_without_privileges_on_the_table_is_refused_before_the_seam() {
    let node = MockNode::start();
    node.accounts.live.create_user("low", "%", "");
    let mut session = open_session_as(&node, "low");
    let refusal = session
        .execute_write("ANALYZE TABLE t")
        .expect_err("an account with no privilege on `t` may not analyze it");
    // Captured from a real TiDB, for a user with no privileges at all and
    // for a SELECT-only user alike -- INSERT is the visitInfo Go appends
    // first.
    assert_eq!(refusal.code, 1142);
    assert_eq!(refusal.state, *b"42000");
    assert_eq!(
        refusal.message,
        "INSERT command denied to user 'low'@'%' for table 't'"
    );
}

/// SELECT alone is not enough, which is Go's answer too: the INSERT the
/// statement needs is the one that writes `mysql.stats_*`.
#[test]
fn analyze_with_only_select_on_the_table_is_still_refused() {
    let node = MockNode::start();
    node.accounts.live.create_user("ro", "%", "");
    node.accounts
        .live
        .grant_table("ro", "%", "app", "t", GlobalPriv::Select.mask());
    let mut session = open_session_as(&node, "ro");
    let refusal = session
        .execute_write("ANALYZE TABLE t")
        .expect_err("SELECT alone does not carry an ANALYZE");
    assert_eq!(refusal.code, 1142);
    assert_eq!(
        refusal.message,
        "INSERT command denied to user 'ro'@'%' for table 't'"
    );
}

/// INSERT and SELECT on the table carry it, and the statement then
/// reaches the statistics seam -- the grant does not have to be global.
#[test]
fn analyze_with_insert_and_select_on_the_table_reaches_the_seam() {
    let node = MockNode::start();
    node.accounts.live.create_user("rw", "%", "");
    node.accounts.live.grant_table(
        "rw",
        "%",
        "app",
        "t",
        GlobalPriv::Select.mask() | GlobalPriv::Insert.mask(),
    );
    let mut session = open_session_as(&node, "rw");
    let refusal = session
        .execute_write("ANALYZE TABLE t")
        .expect_err("the mock node has no statistics to store")
        .message;
    assert!(
        refusal.contains("the mock node stores no statistics for"),
        "a privileged account must reach the statistics seam: {refusal}"
    );
}

/// The clauses of `ANALYZE TABLE` this node does not run are refused at
/// admission -- before a transaction is opened -- and each names itself.
#[test]
fn analyze_clauses_this_node_does_not_run_are_refused_by_name() {
    let (mut session, _node) = open_session();
    for (sql, expected) in [
        ("ANALYZE TABLE t INDEX i", "INDEX"),
        ("ANALYZE TABLE t PREDICATE COLUMNS", "every column"),
        ("ANALYZE TABLE t WITH 3 CMSKETCH DEPTH", "CMSketch"),
    ] {
        let refusal = session
            .execute_write(sql)
            .expect_err("this clause is not one the node runs")
            .message;
        assert!(
            refusal.contains(expected),
            "`{sql}` must be refused by naming `{expected}`: {refusal}"
        );
    }
}
