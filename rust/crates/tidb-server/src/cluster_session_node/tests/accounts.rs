//! `CREATE USER` and its siblings on this node: that they reach the account
//! seam rather than the connection's own in-memory table, and what a failed
//! persist leaves in the cluster and in the live table.
//!
//! The seam's ordering is stated in [`crate::cluster_account_seam`]; these
//! tests assert the node's half of it.

use super::super::*;
use super::node_fixture::*;
use crate::cluster_account_seam::{ClusterAccountWriter, PendingAccountChange};
use crate::ConfiguredUserStore;
use std::sync::atomic::Ordering;
use tidb_session::privilege::PrivilegeRegistry;
use tidb_txnkv::region::RegionBackoffKind;
use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, OptimisticTransactionReceipt, RolledBackTransaction, TransactionCause,
};

/// The cluster factory consumes the same authenticated bypass marker as the
/// pipeline factory, including while an account statement temporarily swaps
/// the session onto a cluster-loaded scratch registry.
#[test]
fn skip_grant_table_bypasses_cluster_session_authorization_without_detaching_account_state() {
    let node = MockNode::start();
    // Recovery boot deliberately leaves the live authorization cache empty;
    // account statements still begin from the persisted mysql.* image.
    node.accounts
        .live
        .replace_from(&PrivilegeRegistry::bootstrapped_from([]));
    assert!(node.accounts.live.create_user("low", "%", ""));
    assert!(node.accounts.stored.create_user("low", "%", ""));

    let normal_store = ConfiguredUserStore::from_accounts(node.accounts.live.clone());
    let normal_identity = normal_store
        .authenticate_native("low", "127.0.0.1", &SALT, &[])
        .expect("the zero-privilege account authenticates normally");
    let mut normal =
        open_session_on_with_context(&node, session_context_with_identity(10, normal_identity));
    let denied = normal
        .execute_write("CREATE ROLE normal_denied")
        .expect_err("a normal zero-privilege account must be denied");
    assert_eq!(denied.code, 1227, "{}", denied.message);
    assert!(!node.accounts.stored.user_exists("normal_denied", "%"));

    // Empty the live cache again after the normal control. Root exists only
    // in stored mysql.*, which the account writer reloads into statement
    // scratch; the GRANT below proves recovery mode did not lose storage.
    node.accounts
        .live
        .replace_from(&PrivilegeRegistry::bootstrapped_from([]));
    let bypass_store =
        ConfiguredUserStore::from_accounts(node.accounts.live.clone()).with_skip_grant_table(true);
    let bypass_identity = bypass_store
        .authenticate_native("ghost", "127.0.0.1", &SALT, b"not-a-password")
        .expect("skip-grant-table admits an identity with no account row");
    let mut bypass =
        open_session_on_with_context(&node, session_context_with_identity(11, bypass_identity));
    bypass
        .execute_write("CREATE ROLE r_skip")
        .expect("the account scratch registry retains the session bypass policy");
    assert!(node.accounts.stored.is_role("r_skip", "%"));
    assert!(node.accounts.live.is_role("r_skip", "%"));
    bypass
        .execute_write("GRANT r_skip TO root")
        .expect("the bypassed cluster account statement still persists role storage");
    let root = ("root".to_owned(), "%".to_owned());
    let role = ("r_skip".to_owned(), "%".to_owned());
    assert!(node.accounts.stored.has_role(&root, &role));
    assert!(node.accounts.live.has_role(&root, &role));
}

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

struct BackoffAccountWriter(PrivilegeRegistry);

impl ClusterAccountWriter for BackoffAccountWriter {
    fn begin(&self) -> Result<Box<dyn PendingAccountChange>, String> {
        let scratch = PrivilegeRegistry::default();
        scratch.replace_from(&clone_registry(&self.0));
        Ok(Box::new(BackoffAccountChange(scratch)))
    }
}

struct BackoffAccountChange(PrivilegeRegistry);

impl PendingAccountChange for BackoffAccountChange {
    fn registry(&self) -> PrivilegeRegistry {
        self.0.clone()
    }

    fn commit(self: Box<Self>) -> Result<Vec<String>, crate::sql_node::SqlQueryError> {
        let outcome = OptimisticCommitOutcome::RolledBack(RolledBackTransaction {
            receipt: OptimisticTransactionReceipt::new(1, 2, b"key".to_vec(), 1),
            cause: TransactionCause::BackoffExhausted {
                kind: RegionBackoffKind::RegionMiss,
                detail: "regionMiss backoffer exhausted".to_owned(),
            },
        });
        Err(
            crate::sql_node::cluster_commit_error(&outcome, "account change")
                .expect("an exhausted region backoff cannot answer success"),
        )
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

#[test]
fn account_commit_keeps_a_backoff_driver_error_coded_on_the_wire() {
    let node = MockNode::start();
    let writer = Arc::new(BackoffAccountWriter(node.accounts.stored.clone()));
    let mut session = open_session_on_with_accounts(&node, writer);
    let query_error = session
        .execute_write("CREATE USER 'busy'@'%'")
        .expect_err("the account change cannot answer success after a rolled-back commit");
    assert_eq!(
        query_error.code,
        tidb_error::tidb::errcode::ErrRegionUnavailable
    );
    assert_query_error_packet(
        &query_error,
        tidb_error::tidb::errcode::ErrRegionUnavailable,
        "Region is unavailable",
    );
    assert!(!node.accounts.stored.user_exists("busy", "%"));
    assert!(!node.accounts.live.user_exists("busy", "%"));
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
