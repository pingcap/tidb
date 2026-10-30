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

//! Go's one statement path on this node:
//! `Compile` -- the preprocessor, `PlanBuilder.Build` and `CheckPrivilege`
//! over its visit infos -- answers before any executor runs, and the
//! executors that commit the user's transaction (`DDLExec`, `GrantExec`,
//! `RevokeExec`, `SimpleExec.autoNewTxn`) do so before their own checks.

use super::super::*;
use super::node_fixture::*;
use super::unistore_cop::{cop_backed_stack, displayed};
use crate::domain::configured_user_store::ConfiguredUserStore;

/// The salt every unistore login in this module answers with.
const LOGIN_SALT: &[u8; 20] = b"01234567890123456789";

/// A unistore node with `test.<table>` and a `low` account that may write it
/// and nothing else, plus a root connection and one connection as `low`.
fn unistore_with_low_account(
    table: &str,
    low: &str,
) -> (
    crate::domain::unistore_node::UnistoreClusterStack,
    Session,
    Session,
) {
    let (stack, users) = cop_backed_stack();
    let mut root = stack.factory.open_session(session_context(1)).unwrap();
    rows(&mut root, &format!("CREATE TABLE test.{table} (a INT PRIMARY KEY)"));
    rows(&mut root, &format!("CREATE USER '{low}'@'%'"));
    rows(
        &mut root,
        &format!("GRANT INSERT, SELECT ON test.* TO '{low}'@'%'"),
    );
    let identity = users
        .authenticate_native(low, "127.0.0.1", LOGIN_SALT, b"")
        .expect("the new account logs in");
    let low = stack
        .factory
        .open_session(session_context_with_identity(2, identity))
        .unwrap();
    (stack, root, low)
}

fn count(session: &mut Session, table: &str) -> String {
    displayed(rows(session, &format!("SELECT count(*) FROM test.{table}")))[0][0].clone()
}

/// Go checks a GRANT ROLE's and a RENAME USER's privileges in `Compile`
/// (`buildSimple`'s `ROLE_ADMIN` and `CreateUserPriv` visit infos,
/// `planbuilder.go`), before `SimpleExec.autoNewTxn` commits: a denied one
/// leaves the user's transaction open with its writes.
#[test]
fn a_compile_time_account_denial_leaves_the_transaction_open() {
    let (_stack, mut root, mut low) = unistore_with_low_account("cp_accounts", "cp_low");
    low.node_conn().control_transaction("BEGIN").unwrap();
    low.node_conn()
        .execute_write("INSERT INTO test.cp_accounts VALUES (1)")
        .unwrap();
    for sql in [
        "RENAME USER 'cp_low'@'%' TO 'cp_low2'@'%'",
        "GRANT 'cp_role' TO 'cp_low'@'%'",
    ] {
        let denied = low.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(denied.code, 1227, "{sql}: {}", denied.message);
        assert!(low.in_transaction(), "{sql} must not end the transaction");
        assert_eq!(count(&mut root, "cp_accounts"), "0", "{sql} committed");
    }
    low.node_conn().control_transaction("COMMIT").unwrap();
    assert_eq!(count(&mut root, "cp_accounts"), "1");
}

/// Go checks CREATE USER's privileges inside `executeCreateUser`
/// (`simple.go:1053`), after `autoNewTxn` committed (`simple.go:151`): the
/// denied statement has already committed the user's transaction.
#[test]
fn an_executor_time_create_user_denial_commits_first() {
    let (_stack, mut root, mut low) = unistore_with_low_account("cp_create_user", "cp_low");
    low.node_conn().control_transaction("BEGIN").unwrap();
    low.node_conn()
        .execute_write("INSERT INTO test.cp_create_user VALUES (1)")
        .unwrap();
    let denied = low
        .node_conn()
        .execute_write("CREATE USER 'cp_new'@'%'")
        .unwrap_err();
    assert_eq!(denied.code, 1227, "{}", denied.message);
    assert!(!low.in_transaction());
    assert_eq!(count(&mut root, "cp_create_user"), "1");
}

/// Go's `autoNewTxn` (`simple.go:3564`) lists neither `SetPwdStmt` nor
/// `SetDefaultRoleStmt`: both write the `mysql.*` tables through a system
/// session and leave the user's transaction open.
#[test]
fn set_password_and_set_default_role_leave_the_transaction_open() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1)).unwrap();
    let mut peer = stack.factory.open_session(session_context(2)).unwrap();
    rows(&mut session, "CREATE TABLE test.cp_pw (a INT PRIMARY KEY)");
    rows(&mut session, "CREATE USER 'cp_pw'@'%'");
    session.node_conn().control_transaction("BEGIN").unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO test.cp_pw VALUES (1)")
        .unwrap();
    for sql in [
        "SET PASSWORD FOR 'cp_pw'@'%' = 'changed'",
        "SET DEFAULT ROLE ALL TO 'cp_pw'@'%'",
    ] {
        session.node_conn().execute_write(sql).unwrap();
        assert!(
            session.in_transaction(),
            "{sql} must not end the transaction"
        );
        assert_eq!(count(&mut peer, "cp_pw"), "0", "{sql} committed");
    }
    assert_eq!(
        displayed(rows(
            &mut peer,
            "SELECT authentication_string <> '' FROM mysql.user WHERE User = 'cp_pw'"
        )),
        [["1"]],
        "the system session committed the password itself"
    );
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    assert_eq!(count(&mut peer, "cp_pw"), "0");
}

/// Go refuses CHECK PARTITION and the STATS_PERSISTENT option inside
/// `executor.AlterTable`'s spec walk (`ddl/executor.go:1767`, `:1952`),
/// after `DDLExec.Next`'s `NewTxnInStmt` (`executor/ddl.go:151`) committed
/// the user's transaction.
#[test]
fn an_executor_time_alter_refusal_commits_the_open_transaction_first() {
    let (mut session, node) = open_session();
    for (id, sql, message) in [
        (1, "ALTER TABLE t CHECK PARTITION p0", "Unsupported check partition"),
        (
            2,
            "ALTER TABLE t STATS_PERSISTENT = DEFAULT",
            "This type of ALTER TABLE is currently unsupported",
        ),
    ] {
        session.node_conn().control_transaction("BEGIN").unwrap();
        session
            .node_conn()
            .execute_write(&format!("INSERT INTO t (id, v) VALUES ({id}, 10)"))
            .unwrap();
        let refused = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!((refused.code, refused.message.as_str()), (8200, message));
        assert_eq!(node.rows(), id, "{sql} commits the transaction first");
        assert!(!session.in_transaction(), "{sql}");
    }
}

/// Go `DDLExec.Next` commits the open transaction with `NewTxnInStmt`
/// (`executor/ddl.go:151`) before the job, and a commit whose verdict was
/// lost is the statement's error: the connection closes without a packet and
/// no job runs.
#[test]
fn an_undetermined_implicit_commit_is_connection_fatal_and_runs_no_job() {
    let (mut session, node) = open_session();
    session.node_conn().control_transaction("BEGIN").unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .unwrap();
    node.fail_next_commit_undetermined
        .store(true, std::sync::atomic::Ordering::Release);
    let error = session
        .node_conn()
        .execute_write("CREATE TABLE after_lost (id BIGINT PRIMARY KEY)")
        .unwrap_err();
    assert_undetermined_closes_without_packet(&error);
    assert_eq!(
        node.ddl.applied.load(std::sync::atomic::Ordering::Acquire),
        0
    );
}

/// Restores the process-wide REPAIR MODE flag a test switched on.
struct RepairModeOn;

impl RepairModeOn {
    fn enter() -> Self {
        tidb_domainutil::REPAIR_INFO.set_repair_mode(true);
        Self
    }
}

impl Drop for RepairModeOn {
    fn drop(&mut self) {
        tidb_domainutil::REPAIR_INFO.set_repair_mode(false);
    }
}

/// Go refuses `ADMIN REPAIR TABLE` in the preprocessor
/// (`checkRepairTableGrammar`, `preprocess.go:1407`): outside REPAIR MODE,
/// and inside it with an empty repair list, each with its own reason, before
/// the planner's SUPER check.
#[test]
fn repair_table_is_refused_by_the_preprocessor() {
    let node = MockNode::start();
    assert!(node.accounts.live.create_user("low", "%", ""));
    let store = ConfiguredUserStore::from_accounts(node.accounts.live.clone());
    let mut low = open_session_on_with_context(
        &node,
        session_context_with_identity(
            31,
            store
                .authenticate_native("low", "127.0.0.1", &SALT, &[])
                .unwrap(),
        ),
    );
    let repair = "ADMIN REPAIR TABLE app.t CREATE TABLE app.t (id BIGINT PRIMARY KEY)";
    let refused = low.node_conn().execute_write(repair).unwrap_err();
    assert_eq!(
        (refused.code, refused.message.as_str()),
        (8215, "Failed to repair table: TiDB is not in REPAIR MODE")
    );
    let _repair_mode = RepairModeOn::enter();
    assert!(tidb_domainutil::REPAIR_INFO
        .get_repair_table_list()
        .is_empty());
    let refused = low.node_conn().execute_write(repair).unwrap_err();
    assert_eq!(
        (refused.code, refused.message.as_str()),
        (8215, "Failed to repair table: repair list is empty")
    );
}

/// Go `AnalyzeExec.Next` (`pkg/executor/analyze.go:300`) never calls
/// `NewTxnInStmt`: an ANALYZE inside a transaction leaves it open, and a
/// later ROLLBACK takes back the writes before it.
#[test]
fn analyze_leaves_the_users_transaction_open() {
    let (_stack, mut root, _low) = unistore_with_low_account("cp_analyze", "cp_analyze_low");
    root.node_conn().control_transaction("BEGIN").unwrap();
    root.node_conn()
        .execute_write("INSERT INTO test.cp_analyze VALUES (1)")
        .unwrap();
    root.node_conn()
        .execute_write("ANALYZE TABLE test.cp_analyze")
        .unwrap();
    assert!(
        root.in_transaction(),
        "ANALYZE must not end the transaction"
    );
    root.node_conn().control_transaction("ROLLBACK").unwrap();
    assert_eq!(count(&mut root, "cp_analyze"), "0");
}
