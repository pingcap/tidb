//! `SHOW [FULL] PROCESSLIST`, `information_schema.processlist` and `KILL`,
//! and the `PROCESS`/`SUPER`/`CONNECTION_ADMIN` privileges that gate them --
//! Go `pkg/executor/show.go`'s processlist path and
//! `pkg/privilege/privileges`' request verification.

use crate::tests_support::*;
use crate::*;

// Config and SEM are process-wide in Go and Rust. Run their cases in a child
// process so normal parallel tests cannot observe a temporary policy.
fn isolated_process_admin_case(name: &str) -> bool {
    if std::env::var("TIDB_PROCESS_ADMIN_CASE").as_deref() == Ok(name) {
        return true;
    }
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &format!("tests_grants::processlist::{name}"),
            "--nocapture",
        ])
        .env("TIDB_PROCESS_ADMIN_CASE", name)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    false
}

#[derive(Default)]
struct KillCounter(std::sync::atomic::AtomicUsize);

impl process::ProcessKillTarget for KillCounter {
    fn cancel_query(&self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    fn kill_connection(&self) {
        self.cancel_query();
    }
}

#[test]
fn process_admin_batch_kill_configuration() {
    if !isolated_process_admin_case("process_admin_batch_kill_configuration") {
        return;
    }
    use tidb_config::config_tree::config::update_global;
    let registry = process::ProcessRegistry::default();
    let target = Arc::new(KillCounter::default());
    let mut session = Session::new();
    session.attach_process(
        7,
        registry.register(
            7,
            String::new(),
            String::new(),
            String::new(),
            Some(target.clone()),
        ),
    );
    update_global(|cfg| {
        cfg.enable_global_kill = false;
        cfg.compatible_kill_query = false;
    });
    session.run("KILL QUERY 7").unwrap();
    assert_eq!(session.warnings()[0].message, "Invalid operation. Please use 'KILL TIDB [CONNECTION | QUERY] [connectionID | CONNECTION_ID()]' instead");
    assert_eq!(target.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    session.run("KILL TIDB QUERY 7").unwrap();
    assert!(session.warnings().is_empty());
    assert_eq!(target.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    update_global(|cfg| cfg.compatible_kill_query = true);
    session.run("KILL QUERY 7").unwrap();
    assert_eq!(target.0.load(std::sync::atomic::Ordering::SeqCst), 2);
    update_global(|cfg| cfg.enable_global_kill = true);
    session.run("KILL TIDB QUERY 7").unwrap();
    assert!(session.warnings()[0]
        .message
        .contains("truncated ConnectionID"));
    assert_eq!(target.0.load(std::sync::atomic::Ordering::SeqCst), 2);
    for (id, suffix) in [(1_u64 << 63, "int64"), (1_u64 << 32, "uint32")] {
        session.run(&format!("KILL {id}")).unwrap();
        assert_eq!(
            session.warnings()[0].message,
            format!("Parse ConnectionID failed: unexpected connectionID exceeds {suffix}")
        );
    }
}

#[test]
fn process_admin_batch_connection_id_expression_bypasses_numeric_parser() {
    let registry = process::ProcessRegistry::default();
    let target = Arc::new(KillCounter::default());
    let mut session = Session::new();
    session.attach_process(
        7,
        registry.register(
            7,
            String::new(),
            String::new(),
            String::new(),
            Some(target.clone()),
        ),
    );
    session.run("KILL QUERY CONNECTION_ID()").unwrap();
    assert_eq!(target.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(session.warnings().is_empty());
}

#[test]
fn process_admin_batch_no_manager_skips_numeric_parser() {
    let mut session = Session::new();
    for sql in ["KILL 7", "KILL 9223372036854775808", "KILL CONNECTION_ID()"] {
        session.run(sql).unwrap();
        assert!(
            session.warnings().is_empty(),
            "{sql}: {:?}",
            session.warnings()
        );
    }
}

#[test]
fn process_admin_batch_authorization_precedes_numeric_parser() {
    let registry = process::ProcessRegistry::default();
    let privileges = privilege::PrivilegeRegistry::default();
    let mut boot = bootstrap_session(&privileges);
    boot.run("CREATE USER bob").unwrap();
    let mut bob = authenticated_session(&privileges, "bob", "%");
    bob.attach_process(
        2,
        registry.register(2, "bob".into(), String::new(), String::new(), None),
    );
    let _victim = registry.register(
        7,
        "root".into(),
        "127.0.0.1:1234".into(),
        String::new(),
        None,
    );
    assert!(matches!(
        bob.run("KILL 7"),
        Err(DriverError::KillAccessDenied)
    ));
}

#[test]
fn process_admin_batch_sem_target_defaults_and_caller_active_roles() {
    if !isolated_process_admin_case(
        "process_admin_batch_sem_target_defaults_and_caller_active_roles",
    ) {
        return;
    }
    let registry = process::ProcessRegistry::default();
    let privileges = privilege::PrivilegeRegistry::default();
    let mut boot = bootstrap_session(&privileges);
    for sql in [
        "CREATE USER bob",
        "CREATE USER protected",
        "CREATE ROLE restricted_target, killer",
        "GRANT RESTRICTED_USER_ADMIN ON *.* TO restricted_target",
        "GRANT restricted_target TO protected",
        "SET DEFAULT ROLE restricted_target TO protected",
        "GRANT SUPER ON *.* TO bob",
        "GRANT RESTRICTED_CONNECTION_ADMIN ON *.* TO killer",
        "GRANT killer TO bob",
    ] {
        boot.run(sql).unwrap();
    }
    // Connection IDs Go's allocator would issue on this server: they encode
    // the standalone server ID that `KILL` checks before reaching the local
    // registry.
    let local = |local_conn_id| {
        tidb_util::globalconn::Gcid {
            is_64bits: false,
            local_conn_id,
            server_id: tidb_util::globalconn::SERVER_ID_FOR_STANDALONE,
        }
        .to_conn_id()
    };
    let (bob_id, victim, protected_id) = (local(1), local(2), local(3));
    let kill_victim = format!("KILL {victim}");
    let mut bob = authenticated_session(&privileges, "bob", "%");
    bob.attach_process(
        bob_id,
        registry.register(bob_id, "bob".into(), "127.0.0.1:12".into(), String::new(), None),
    );
    let target = Arc::new(KillCounter::default());
    let _victim = registry.register(
        victim,
        "protected".into(),
        "[::1]:1234".into(),
        String::new(),
        Some(target.clone()),
    );
    tidb_util::sem::enable();
    assert!(
        matches!(bob.run(&kill_victim), Err(DriverError::SpecificAccessDenied(name)) if name == "RESTRICTED_CONNECTION_ADMIN")
    );
    assert_eq!(target.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    bob.run("SET ROLE killer").unwrap();
    bob.run(&kill_victim).unwrap();
    assert_eq!(target.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    bob.run("SET ROLE NONE").unwrap();
    assert!(matches!(
        bob.run(&kill_victim),
        Err(DriverError::SpecificAccessDenied(_))
    ));
    let mut same_user = authenticated_session(&privileges, "protected", "%");
    same_user.attach_process(
        protected_id,
        registry.register(protected_id, "protected".into(), String::new(), String::new(), None),
    );
    same_user.run(&kill_victim).unwrap();
    assert_eq!(target.0.load(std::sync::atomic::Ordering::SeqCst), 2);
    // Granted but non-default target roles do not protect the target.
    privileges.set_default_roles(&("protected".into(), "%".into()), &[]);
    bob.run(&kill_victim).unwrap();
    assert_eq!(target.0.load(std::sync::atomic::Ordering::SeqCst), 3);
    // Go matches global_grants independently of the selected user row.
    privileges.create_user("protected", "::1", "");
    privileges.grant_dynamic("protected", "%", "RESTRICTED_USER_ADMIN", false);
    assert!(matches!(
        bob.run(&kill_victim),
        Err(DriverError::SpecificAccessDenied(_))
    ));
    tidb_util::sem::disable();
    bob.run(&kill_victim).unwrap();
    assert_eq!(target.0.load(std::sync::atomic::Ordering::SeqCst), 4);
}

#[test]
fn process_admin_batch_processlist_reads_target_statement_state() {
    use tidb_util::memory::Tracker;
    let registry = process::ProcessRegistry::default();
    let mut observer = Session::new();
    observer.attach_process(
        2,
        registry.register(2, String::new(), String::new(), String::new(), None),
    );
    let peer = registry.register(4, String::new(), String::new(), String::new(), None);
    let mem = Tracker::new(1, -1);
    let disk = Tracker::new(2, -1);
    mem.consume(123);
    disk.consume(456);
    peer.set_trackers(mem.clone(), disk.clone());
    registry.statement_started_with_digest(
        4,
        "execute prepared using @arg",
        Some("prepared-sql-digest"),
        "autocommit",
    );
    let ts = (1_609_459_200_123_u64 << 18) + 9;
    registry.statement_metadata(
        4,
        ts,
        "target_group".into(),
        "target_alias".into(),
        tidb_parser::RedactMode::Disabled,
        Default::default(),
    );
    registry.statement_affected_rows(4, 17);
    observer.run("SET time_zone='+08:00'").unwrap();
    let sql = "SELECT DIGEST,MEM,DISK,TxnStart,RESOURCE_GROUP,SESSION_ALIAS,ROWS_AFFECTED FROM information_schema.PROCESSLIST WHERE ID=4";
    let mut expected = vec![vec![
        "prepared-sql-digest".to_owned(),
        "123".into(),
        "456".into(),
        format!("01-01 08:00:00.123({ts})"),
        "target_group".into(),
        "target_alias".into(),
        "17".into(),
    ]];
    assert_eq!(row_text(observer.run(sql)), expected);
    observer.run("SET time_zone='America/Los_Angeles'").unwrap();
    mem.consume(-23);
    disk.consume(-56);
    expected[0][1] = "100".into();
    expected[0][2] = "400".into();
    expected[0][3] = format!("12-31 16:00:00.123({ts})");
    assert_eq!(row_text(observer.run(sql)), expected);
}

#[test]
fn process_admin_batch_processlist_without_statement_context() {
    let registry = process::ProcessRegistry::default();
    let mut observer = Session::new();
    observer.attach_process(
        2,
        registry.register(2, String::new(), String::new(), String::new(), None),
    );
    let _peer = registry.register(4, String::new(), String::new(), String::new(), None);
    assert_eq!(
        row_text(
            observer.run(
                "SELECT MEM,DISK,ROWS_AFFECTED FROM information_schema.PROCESSLIST WHERE ID=4"
            )
        ),
        [["0", "0", "NULL"]]
    );
}

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
    // A local connection ID is one Go's allocator would hand out on this
    // server: it encodes the standalone server ID, which `KILL` compares
    // against before touching the local registry (`executeKillStmt`). An ID
    // naming server 0 is never local in Go standalone; Go treats it as remote
    // and refuses it as "Unexpected ZERO ServerID".
    let own = tidb_util::globalconn::Gcid {
        is_64bits: false,
        local_conn_id: 3,
        server_id: tidb_util::globalconn::SERVER_ID_FOR_STANDALONE,
    }
    .to_conn_id();
    let guard = registry.register(
        own,
        "alice".to_owned(),
        String::new(),
        "test".to_owned(),
        Some(target.clone()),
    );
    session.attach_process(own, guard);
    // Even 32-bit IDs are valid under Go global kill; odd IDs are truncated.
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
        session.run(&format!("kill query {own}")).unwrap(),
        StmtResult::Affected(0)
    );
    assert_eq!(target.queries.load(Ordering::Acquire), 1);
    assert_eq!(
        session.run(&format!("kill connection {own}")).unwrap(),
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
        4,
        "root".to_owned(),
        "10.0.0.1:1".to_owned(),
        "test".to_owned(),
        None,
    );
    victim.attach_process(4, victim_guard);

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
    match bob.run("kill 4") {
        Err(DriverError::KillAccessDenied) => {}
        other => panic!("expected KillAccessDenied, got {other:?}"),
    }

    // Granting SUPER lets the same KILL through.
    boot.run("GRANT SUPER ON *.* TO 'bob'@'%'").unwrap();
    assert_eq!(bob.run("kill 4").unwrap(), StmtResult::Affected(0));
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
        4,
        "root".to_owned(),
        "10.0.0.1:1".to_owned(),
        "test".to_owned(),
        None,
    );
    victim.attach_process(4, victim_guard);

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

    match bob.run("kill 4") {
        Err(DriverError::KillAccessDenied) => {}
        other => panic!("expected KillAccessDenied, got {other:?}"),
    }

    boot.run("GRANT CONNECTION_ADMIN ON *.* TO 'bob'@'%'")
        .unwrap();
    assert_eq!(
        bob.run("kill 4").unwrap(),
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
    match bob.run("kill 4") {
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

#[test]
fn cluster_lifecycle_batch_auto_analyze_kill_requires_connection_admin() {
    use tidb_stats_handle_util::GLOBAL_AUTO_ANALYZE_PROCESS_LIST;
    let registry = process::ProcessRegistry::default();
    let privileges = privilege::PrivilegeRegistry::default();
    let mut boot = bootstrap_session(&privileges);
    boot.run("CREATE USER bob").unwrap();
    let mut bob = authenticated_session(&privileges, "bob", "%");
    bob.attach_process(
        2,
        registry.register(2, "bob".into(), String::new(), String::new(), None),
    );
    let id = 0x76543210;
    GLOBAL_AUTO_ANALYZE_PROCESS_LIST.tracker(id);
    let denied = bob.run(&format!("KILL {id}"));
    boot.run("GRANT CONNECTION_ADMIN ON *.* TO bob").unwrap();
    let allowed = bob.run(&format!("KILL {id}"));
    GLOBAL_AUTO_ANALYZE_PROCESS_LIST.untracker(id);
    assert!(matches!(denied, Err(DriverError::KillAccessDenied)));
    allowed.unwrap();
}

#[test]
fn cluster_peer_batch_remote_id_never_uses_local_registry() {
    let registry = process::ProcessRegistry::default();
    let target = Arc::new(KillCounter::default());
    let mut session = Session::new();
    session.server_id_getter = Arc::new(|| 1);
    let remote = tidb_util::globalconn::Gcid {
        server_id: 2,
        local_conn_id: 42,
        is_64bits: false,
    }
    .to_conn_id();
    session.attach_process(
        remote,
        registry.register(
            remote,
            String::new(),
            String::new(),
            String::new(),
            Some(target.clone()),
        ),
    );
    session.run(&format!("KILL QUERY {remote}")).unwrap();
    assert_eq!(target.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(session
        .warnings()
        .iter()
        .any(|w| w.message.starts_with("KILL remote connection failed:")));
}
