//! `CREATE USER` and its siblings on this node: that they reach the account
//! seam rather than the connection's own in-memory table, and what a failed
//! persist leaves in the cluster and in the live table.
//!
//! The seam's ordering is stated in [`crate::cluster_account_seam`]; these
//! tests assert the node's half of it.

use super::super::*;
use super::node_fixture::*;
use crate::cluster_account_seam::{ClusterAccountWriter, PendingAccountChange};
use std::sync::atomic::Ordering;
use tidb_session::privilege::PrivilegeRegistry;

struct UndeterminedAccountWriter(PrivilegeRegistry);

impl ClusterAccountWriter for UndeterminedAccountWriter {
    fn begin(&self) -> Result<Box<dyn PendingAccountChange>, String> {
        Ok(Box::new(UndeterminedAccountChange(self.0.clone())))
    }
}

struct UndeterminedAccountChange(PrivilegeRegistry);

impl PendingAccountChange for UndeterminedAccountChange {
    fn registry(&self) -> PrivilegeRegistry {
        self.0.clone()
    }

    fn commit(self: Box<Self>) -> Result<Vec<String>, crate::sql_node::SqlQueryError> {
        Err(undetermined_cluster_commit_error("account change"))
    }
}

#[test]
fn account_commit_keeps_an_undetermined_verdict_connection_fatal() {
    let node = MockNode::start();
    let writer = Arc::new(UndeterminedAccountWriter(node.accounts.stored.clone()));
    let mut session = open_session_on_with_accounts(&node, writer);
    let query_error = session
        .execute_write("CREATE USER 'uncertain'@'%'")
        .expect_err("the account change cannot answer success after losing its commit response");
    assert_undetermined_closes_without_packet(&query_error);
}

/// An account statement reaches the account seam -- not the catalog
/// writer, and not the session's own in-memory table alone -- and what it
/// did becomes the cluster's stored accounts.
#[test]
fn an_account_statement_is_persisted_and_then_published() {
    let (mut session, node) = open_session();
    session
        .execute_write("CREATE USER 'bob'@'%' IDENTIFIED BY 'pw'")
        .expect("the account statement is routed rather than refused");
    // The cluster stores it, which is the whole point: a node that only
    // changed its own copy would answer OK about an account nowhere else
    // has.
    assert!(
        node.accounts.stored.user_exists("bob", "%"),
        "the cluster did not gain the account"
    );
    // And the node's live table has it too, so the next connection can log
    // in as it without waiting for a reload.
    assert!(node.accounts.live.user_exists("bob", "%"));
    // `CREATE USER` is a DDL node in the parser, so it would otherwise
    // reach the catalog writer; it must not.
    assert_eq!(node.ddl.applied.load(Ordering::Acquire), 0);
}

/// The failure invariant: a persist that fails leaves neither the cluster
/// nor the node's live table changed, and the client is told.
#[test]
fn a_failed_persist_changes_neither_the_cluster_nor_the_live_table() {
    let (mut session, node) = open_session();
    node.accounts.persists.store(false, Ordering::Release);
    let error = session
        .execute_write("CREATE USER 'bob'@'%'")
        .expect_err("a failed persist must fail the statement");
    assert!(error.message.contains("rejected"), "{}", error.message);
    assert!(!node.accounts.stored.user_exists("bob", "%"));
    assert!(!node.accounts.live.user_exists("bob", "%"));
    // The connection is left reading the live table, not the scratch copy
    // the failed statement mutated -- otherwise this session would keep
    // answering as if the account existed.
    assert!(session.execute("SHOW GRANTS FOR 'bob'@'%'").is_err());
}

/// A statement the driver itself rejects never reaches storage, and leaves
/// the connection reading the live table.
#[test]
fn a_statement_the_driver_rejects_never_reaches_the_cluster() {
    let (mut session, node) = open_session();
    session
        .execute_write("CREATE USER 'bob'@'%'")
        .expect("the first CREATE USER succeeds");
    session
        .execute_write("CREATE USER 'bob'@'%'")
        .expect_err("a duplicate account must be refused by the driver");
    assert!(node.accounts.stored.user_exists("bob", "%"));
}
