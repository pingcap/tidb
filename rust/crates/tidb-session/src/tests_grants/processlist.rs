//! `SHOW [FULL] PROCESSLIST`, `information_schema.processlist` and `KILL`,
//! and the `PROCESS`/`SUPER`/`CONNECTION_ADMIN` privileges that gate them --
//! Go `pkg/executor/show.go`'s processlist path and
//! `pkg/privilege/privileges`' request verification.

use crate::tests_support::*;
use crate::*;

/// SHOW WARNINGS / SHOW ERRORS, checked against captured TiDB output.
///
/// NOT PORTED from Go's own suites: the warnings raised by evaluation
/// (`1/0` is 1365 there) and by write-time truncation, because this tier
/// does not yet produce those warnings -- only the preprocessor gate and
/// the failed-statement error reach the buffer here. The filter forms of
/// both statements are refused, not ignored.
/// Captured from TiDB (`show processlist` on a fresh testkit session):
///
/// ```text
/// Id  User  Host  db    Command  Time  State       Info
/// 1               test  Query    0     autocommit  show processlist
/// ```
///
/// with column types `Id BIGINT`, `User/Host/db/Command/State VARCHAR`,
/// `Time INT`, `Info STRING` -- and `show full processlist` differing only
/// in that `Info` is not truncated to 100 runes.
///
/// A session with no server front lists exactly itself, which is what
/// this checks; the whole-server list is covered over TCP in
/// `tidb-server`'s `pipeline_mysql_client_source` test.
#[test]
fn show_processlist_lists_this_session() {
    let mut session = Session::new();
    let StmtOutput::Rows { columns, rows } = session.run_with_columns("show processlist").unwrap()
    else {
        panic!("SHOW PROCESSLIST answers with rows");
    };
    assert_eq!(
        columns
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        vec!["Id", "User", "Host", "db", "Command", "Time", "State", "Info"]
    );
    let text: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|v| datum_text(v).unwrap_or_else(|| "NULL".to_owned()))
                .collect()
        })
        .collect();
    assert_eq!(
        text,
        vec![vec![
            "0".to_owned(),
            String::new(),
            String::new(),
            "test".to_owned(),
            "Query".to_owned(),
            "0".to_owned(),
            "autocommit".to_owned(),
            "show processlist".to_owned(),
        ]]
    );
}

/// Captured from TiDB: `SHOW PROCESSLIST` truncates `Info` to 100 runes
/// and `SHOW FULL PROCESSLIST` does not.
#[test]
fn show_full_processlist_does_not_truncate_info() {
    let registry = process::ProcessRegistry::default();
    let mut session = Session::new();
    let guard = registry.register(1, String::new(), String::new(), "test".to_owned(), None);
    session.attach_process(1, guard);
    // A peer connection, which is the row whose Info the SHOW truncates
    // (the running SHOW is this session's own Info).
    let _peer = registry.register(
        9,
        "alice".to_owned(),
        "10.0.0.1:33".to_owned(),
        "test".to_owned(),
        None,
    );
    let long = format!("select /* {} */ 1", "x".repeat(200));
    registry.statement_started(9, &long, "autocommit");
    let short = row_text(session.run("show processlist"));
    assert_eq!(short.len(), 2);
    assert_eq!(short[1][0], "9");
    assert_eq!(short[1][1], "alice");
    assert_eq!(short[1][2], "10.0.0.1:33");
    assert_eq!(short[1][4], "Query");
    assert_eq!(short[1][7].chars().count(), 100);
    // This session's own row reports the SHOW it is running.
    assert_eq!(short[0][7], "show processlist");
    let full = row_text(session.run("show full processlist"));
    assert_eq!(full[1][7], long);
    assert_eq!(full[0][7], "show full processlist");
}

/// Go `setDataForProcessList` / `fetchShowProcessList`: without the
/// `PROCESS` privilege a session sees only its own connections, on both
/// `SHOW PROCESSLIST` and `information_schema.PROCESSLIST`; with it, all
/// of them.
#[test]
fn process_privilege_gates_visibility_on_both_surfaces() {
    let registry = process::ProcessRegistry::default();
    let mut session = Session::new();
    session.set_user("bob@%".to_owned(), "bob@10.0.0.1".to_owned());
    let guard = registry.register(
        1,
        "bob".to_owned(),
        "10.0.0.1:1".to_owned(),
        "test".to_owned(),
        None,
    );
    session.attach_process(1, guard);
    let _alice = registry.register(
        2,
        "alice".to_owned(),
        "10.0.0.2:2".to_owned(),
        "test".to_owned(),
        None,
    );

    // No PROCESS privilege: only bob's own row.
    let show = row_text(session.run("show processlist"));
    assert_eq!(show.len(), 1);
    assert_eq!(show[0][1], "bob");
    let table = row_text(session.run("select * from information_schema.processlist"));
    assert_eq!(table.len(), 1);
    assert_eq!(table[0][1], "bob");

    // With PROCESS: every connection, on both surfaces.
    session.set_process_privilege(true);
    let show = row_text(session.run("show processlist"));
    assert_eq!(show.len(), 2);
    let table = row_text(session.run("select * from information_schema.processlist"));
    assert_eq!(table.len(), 2);
}

/// CAPTURED (`pkg/infoschema/tables.go` `tableProcesslistCols`): the
/// exact column list and order of `information_schema.PROCESSLIST`,
/// which is 12 columns wider than `SHOW PROCESSLIST`'s 8.
#[test]
fn information_schema_processlist_has_the_captured_column_list() {
    let mut session = Session::new();
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns("select * from information_schema.processlist")
        .unwrap()
    else {
        panic!("PROCESSLIST answers with rows");
    };
    assert_eq!(
        columns
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "ID",
            "USER",
            "HOST",
            "DB",
            "COMMAND",
            "TIME",
            "STATE",
            "INFO",
            "DIGEST",
            "MEM",
            "MEM_ARBITRATION",
            "MEM_WAIT_ARBITRATE_START",
            "MEM_WAIT_ARBITRATE_BYTES",
            "DISK",
            "TxnStart",
            "RESOURCE_GROUP",
            "SESSION_ALIAS",
            "ROWS_AFFECTED",
            "TIDB_CPU",
            "TIKV_CPU",
        ]
    );
    assert_eq!(rows.len(), 1);
}

/// `WHERE` over the virtual table runs through the ordinary plan, exactly
/// as it does for the other `information_schema` tables.
#[test]
fn information_schema_processlist_where_filters_by_user() {
    let registry = process::ProcessRegistry::default();
    let mut session = Session::new();
    session.set_user("root@%".to_owned(), "root@127.0.0.1".to_owned());
    session.set_process_privilege(true);
    let guard = registry.register(
        1,
        "root".to_owned(),
        "127.0.0.1:1".to_owned(),
        "test".to_owned(),
        None,
    );
    session.attach_process(1, guard);
    let _alice = registry.register(
        2,
        "alice".to_owned(),
        "10.0.0.2:2".to_owned(),
        "test".to_owned(),
        None,
    );
    let rows = row_text(
        session.run("select id, user from information_schema.processlist where user = 'alice'"),
    );
    assert_eq!(rows, vec![vec!["2".to_owned(), "alice".to_owned()]]);
}

/// Captured from TiDB: `KILL <unknown id>` is NOT an error -- it answers
/// OK having done nothing (1094 belongs to EXPLAIN FOR CONNECTION).
#[test]
fn kill_answers_ok_and_reaches_only_live_connections() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Default)]
    struct Counter {
        queries: AtomicUsize,
        connections: AtomicUsize,
    }
    impl process::ProcessKillTarget for Counter {
        fn cancel_query(&self) {
            self.queries.fetch_add(1, Ordering::AcqRel);
        }
        fn kill_connection(&self) {
            self.connections.fetch_add(1, Ordering::AcqRel);
        }
    }
    let registry = process::ProcessRegistry::default();
    let target = Arc::new(Counter::default());
    let mut session = Session::new();
    let guard = registry.register(
        5,
        "alice".to_owned(),
        String::new(),
        "test".to_owned(),
        Some(target.clone()),
    );
    session.attach_process(5, guard);
    // KILL answers with an affected-row count, which the wire front turns
    // into the OK packet Go sends.
    assert_eq!(
        session.statement_kind("kill 999999").unwrap(),
        StmtKind::Write
    );
    assert_eq!(session.run("kill 999999").unwrap(), StmtResult::Affected(0));
    assert_eq!(target.connections.load(Ordering::Acquire), 0);
    // Killing one's own query is legal and only cancels the statement.
    assert_eq!(
        session.run("kill query 5").unwrap(),
        StmtResult::Affected(0)
    );
    assert_eq!(target.queries.load(Ordering::Acquire), 1);
    assert_eq!(
        session.run("kill connection 5").unwrap(),
        StmtResult::Affected(0)
    );
    assert_eq!(target.connections.load(Ordering::Acquire), 1);
    // Go accepts CONNECTION_ID() and rejects any other expression.
    assert_eq!(
        session.run("kill query connection_id()").unwrap(),
        StmtResult::Affected(0)
    );
    assert_eq!(target.queries.load(Ordering::Acquire), 2);
    assert!(session.run("kill query 1 + 1").is_err());
}

/// Go `planbuilder.go`'s `*ast.KillStmt` case: a session may always KILL
/// its OWN connection, but killing a peer logged in as a DIFFERENT user
/// is refused with `ErrSpecificAccessDenied` (1227) unless the caller
/// holds SUPER. Granting SUPER then lets the same KILL through.
#[test]
fn kill_of_another_users_connection_requires_super() {
    let registry = process::ProcessRegistry::default();
    let privs = privilege::PrivilegeRegistry::default();
    // bob cannot create his own account or grant himself SUPER -- that is
    // the escalation the account gates refuse -- so the server provisions
    // both, as a real deployment does before anybody logs in.
    let mut boot = bootstrap_session(&privs);
    boot.run("CREATE USER 'bob'@'%'").unwrap();

    let mut victim = authenticated_session(&privs, "root", "%");
    victim.set_user("root@%".to_owned(), "root@10.0.0.1".to_owned());
    let victim_guard = registry.register(
        1,
        "root".to_owned(),
        "10.0.0.1:1".to_owned(),
        "test".to_owned(),
        None,
    );
    victim.attach_process(1, victim_guard);

    let mut bob = authenticated_session(&privs, "bob", "%");
    bob.set_user("bob@%".to_owned(), "bob@10.0.0.2".to_owned());
    let bob_guard = registry.register(
        2,
        "bob".to_owned(),
        "10.0.0.2:2".to_owned(),
        "test".to_owned(),
        None,
    );
    bob.attach_process(2, bob_guard);

    // Killing one's own connection never needs a privilege.
    assert_eq!(
        bob.run("kill 2").unwrap(),
        StmtResult::Affected(0),
        "KILL of one's own connection is always allowed"
    );

    // Killing root's connection without SUPER is refused.
    match bob.run("kill 1") {
        Err(DriverError::KillAccessDenied) => {}
        other => panic!("expected KillAccessDenied, got {other:?}"),
    }

    // Granting SUPER lets the same KILL through.
    boot.run("GRANT SUPER ON *.* TO 'bob'@'%'").unwrap();
    assert_eq!(bob.run("kill 1").unwrap(), StmtResult::Affected(0));
}

/// The gate Go actually writes is the DYNAMIC `CONNECTION_ADMIN`; SUPER
/// passes only as its fallback. So `CONNECTION_ADMIN` ALONE -- with no
/// SUPER anywhere -- must open the same KILL, and revoking it must close
/// it again.
#[test]
fn kill_of_another_users_connection_accepts_connection_admin() {
    let registry = process::ProcessRegistry::default();
    let privs = privilege::PrivilegeRegistry::default();
    let mut boot = bootstrap_session(&privs);
    boot.run("CREATE USER 'bob'@'%'").unwrap();

    let mut victim = authenticated_session(&privs, "root", "%");
    victim.set_user("root@%".to_owned(), "root@10.0.0.1".to_owned());
    let victim_guard = registry.register(
        1,
        "root".to_owned(),
        "10.0.0.1:1".to_owned(),
        "test".to_owned(),
        None,
    );
    victim.attach_process(1, victim_guard);

    let mut bob = authenticated_session(&privs, "bob", "%");
    bob.set_user("bob@%".to_owned(), "bob@10.0.0.2".to_owned());
    let bob_guard = registry.register(
        2,
        "bob".to_owned(),
        "10.0.0.2:2".to_owned(),
        "test".to_owned(),
        None,
    );
    bob.attach_process(2, bob_guard);

    match bob.run("kill 1") {
        Err(DriverError::KillAccessDenied) => {}
        other => panic!("expected KillAccessDenied, got {other:?}"),
    }

    boot.run("GRANT CONNECTION_ADMIN ON *.* TO 'bob'@'%'")
        .unwrap();
    assert_eq!(
        bob.run("kill 1").unwrap(),
        StmtResult::Affected(0),
        "CONNECTION_ADMIN alone authorizes KILL of a peer's connection"
    );
    // The dynamic privilege is the ONLY thing bob holds: no static
    // privilege was granted along the way.
    assert_eq!(
        row_text(bob.run("SHOW GRANTS FOR 'bob'@'%'")),
        [
            ["GRANT USAGE ON *.* TO 'bob'@'%'"],
            ["GRANT CONNECTION_ADMIN ON *.* TO 'bob'@'%'"],
        ]
    );

    boot.run("REVOKE CONNECTION_ADMIN ON *.* FROM 'bob'@'%'")
        .unwrap();
    match bob.run("kill 1") {
        Err(DriverError::KillAccessDenied) => {}
        other => panic!("expected KillAccessDenied after REVOKE, got {other:?}"),
    }
}

/// `PROCESS` granted through `GRANT` (not the test-only
/// [`Session::set_process_privilege`] override) gates `SHOW PROCESSLIST`
/// visibility exactly the same way, wiring the registry all the way to
/// the process-list filter.
#[test]
fn grant_process_gates_processlist_visibility() {
    let registry = process::ProcessRegistry::default();
    let privs = privilege::PrivilegeRegistry::default();
    let mut boot = bootstrap_session(&privs);
    boot.run("CREATE USER 'bob'@'%'").unwrap();
    let mut session = authenticated_session(&privs, "bob", "%");
    session.set_user("bob@%".to_owned(), "bob@10.0.0.1".to_owned());
    let guard = registry.register(
        1,
        "bob".to_owned(),
        "10.0.0.1:1".to_owned(),
        "test".to_owned(),
        None,
    );
    session.attach_process(1, guard);
    let _alice = registry.register(
        2,
        "alice".to_owned(),
        "10.0.0.2:2".to_owned(),
        "test".to_owned(),
        None,
    );

    assert_eq!(row_text(session.run("show processlist")).len(), 1);

    boot.run("GRANT PROCESS ON *.* TO 'bob'@'%'").unwrap();
    assert_eq!(row_text(session.run("show processlist")).len(), 2);
}
