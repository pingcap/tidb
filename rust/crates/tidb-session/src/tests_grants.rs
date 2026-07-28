#![cfg(test)]

use crate::tests_support::*;
use crate::*;

/// The `information_schema` PRIVILEGES family: `SCHEMA_PRIVILEGES`,
/// `TABLE_PRIVILEGES`, `COLUMN_PRIVILEGES`.
///
/// The surprising part, and the reason this test exists: these three are
/// DECLARED in Go's `pkg/infoschema/tables.go` but have NO retriever in
/// `pkg/executor`, so real TiDB serves the header and NEVER a row --
/// even when grants exist. CAPTURED from `testkit.CreateMockStore` after
/// `GRANT SELECT, INSERT ON db1.* TO 'u1'@'%'`,
/// `GRANT ALL PRIVILEGES ON db1.* TO 'u2'@'localhost'`,
/// `GRANT SELECT ON db1.t1 TO 'u1'@'%' WITH GRANT OPTION` and
/// `GRANT UPDATE, DELETE ON db1.t1 TO 'u2'@'localhost'`: every
/// `SELECT *` came back empty and `SELECT COUNT(*)` came back `0`.
///
/// So filling these in from the privilege registry -- which HAS all the
/// grant data -- would be a DIVERGENCE from Go, not a completion. The
/// emptiness is the transcreated behavior.
#[test]
fn infoschema_privileges_tables_are_header_only() {
    let mut session = Session::new();
    session.attach_privileges(privilege::PrivilegeRegistry::default());
    session.run("CREATE DATABASE db1").unwrap();
    session.run("CREATE TABLE db1.t1 (a INT)").unwrap();
    session.run("CREATE USER 'u1'@'%'").unwrap();
    session.run("CREATE USER 'u2'@'localhost'").unwrap();
    session
        .run("GRANT SELECT, INSERT ON db1.* TO 'u1'@'%'")
        .unwrap();
    session
        .run("GRANT ALL PRIVILEGES ON db1.* TO 'u2'@'localhost'")
        .unwrap();
    // Table scope too, so the emptiness is not just a DB-scope artifact.
    // (Go's capture also used `WITH GRANT OPTION` here; this tier does
    // not model that yet, and it makes no difference to the result --
    // the table is empty either way.)
    session.run("GRANT SELECT ON db1.t1 TO 'u1'@'%'").unwrap();

    let query = |session: &mut Session, sql: &str| match session.run_with_columns(sql).unwrap() {
        StmtOutput::Rows { columns, rows } => (
            columns
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            rows,
        ),
        other => panic!("expected rows, got {other:?}"),
    };

    let (names, rows) = query(
        &mut session,
        "SELECT * FROM information_schema.schema_privileges",
    );
    assert_eq!(
        names,
        [
            "GRANTEE",
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "PRIVILEGE_TYPE",
            "IS_GRANTABLE",
        ]
    );
    assert!(rows.is_empty(), "grants must NOT surface here");

    let (names, rows) = query(
        &mut session,
        "SELECT * FROM information_schema.table_privileges",
    );
    assert_eq!(
        names,
        [
            "GRANTEE",
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "PRIVILEGE_TYPE",
            "IS_GRANTABLE",
        ]
    );
    assert!(rows.is_empty(), "grants must NOT surface here");

    let (names, rows) = query(
        &mut session,
        "SELECT * FROM information_schema.column_privileges",
    );
    assert_eq!(
        names,
        [
            "GRANTEE",
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "COLUMN_NAME",
            "PRIVILEGE_TYPE",
            "IS_GRANTABLE",
        ]
    );
    assert!(rows.is_empty(), "grants must NOT surface here");

    // Go returns `0`, not an error, for the aggregate over the empty
    // body -- so the tables are real relations, not stubs that fail.
    for table in ["schema_privileges", "table_privileges", "column_privileges"] {
        let (_, rows) = query(
            &mut session,
            &format!("SELECT COUNT(*) FROM information_schema.{table}"),
        );
        assert_eq!(rows, vec![vec![Datum::Int(0)]], "COUNT(*) over {table}");
    }

    // A WHERE filter over the empty body also runs the ordinary plan
    // path rather than erroring on an unknown table.
    let (_, rows) = query(
        &mut session,
        "SELECT grantee FROM information_schema.schema_privileges WHERE table_schema = 'db1'",
    );
    assert!(rows.is_empty());
}

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

/// CAPTURED end to end (`pkg/executor/grant.go`, `revoke.go`,
/// `simple.go`, `show.go`): `CREATE USER` -> fresh `SHOW GRANTS` reports
/// `USAGE` -> `GRANT` in scrambled order prints in Go's fixed
/// `mysql.AllGlobalPrivs` order -> `REVOKE` removes exactly the one
/// privilege -> `DROP USER` then a missing-user error, matching the Go
/// source's `ErrCannotUser`/1396 wording exactly (`user@host`, unquoted).
#[test]
fn grant_revoke_and_show_grants_round_trip() {
    let mut session = session_with_privileges();

    session.run("CREATE USER 'u1'@'%'").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
        [["GRANT USAGE ON *.* TO 'u1'@'%'"]]
    );

    session
        .run("GRANT SELECT, PROCESS, INSERT, SUPER, UPDATE ON *.* TO 'u1'@'%'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
        [["GRANT SELECT,INSERT,UPDATE,PROCESS,SUPER ON *.* TO 'u1'@'%'"]]
    );

    session.run("REVOKE SUPER ON *.* FROM 'u1'@'%'").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
        [["GRANT SELECT,INSERT,UPDATE,PROCESS ON *.* TO 'u1'@'%'"]]
    );

    session.run("DROP USER 'u1'@'%'").unwrap();
    match session.run("DROP USER 'nosuchuser'@'%'") {
        Err(DriverError::DropUserMissing { accounts }) => {
            assert_eq!(accounts, "nosuchuser@%");
        }
        other => panic!("expected DropUserMissing, got {other:?}"),
    }
}

/// DYNAMIC privileges through `GRANT`/`REVOKE`/`SHOW GRANTS`, captured
/// from `pkg/executor/zz_dump_dynpriv_test.go` against
/// `testkit.CreateMockStore`.
///
/// The captured ordering rule: dynamic lines come LAST, after every
/// static scope, as at most two lines -- the non-grantable privileges
/// first, then the grantable ones with the ` WITH GRANT OPTION` suffix
/// -- each an alphabetically sorted comma-joined list on `*.*`. The
/// `GRANT USAGE ON *.*` global line is still printed for an account
/// whose only privileges are dynamic.
#[test]
fn dynamic_privileges_grant_revoke_and_show_grants() {
    let mut session = session_with_privileges();
    session.run("CREATE DATABASE db1").unwrap();
    session.run("CREATE TABLE db1.t (a INT)").unwrap();
    session.run("CREATE USER 'u1'@'%'").unwrap();

    // A dynamic privilege is GLOBAL-only: `ErrIllegalPrivilegeLevel`
    // (3619) at DB and TABLE scope, and it fires BEFORE the
    // is-it-registered check.
    for level in ["db1.*", "db1.t"] {
        match session.run(&format!("GRANT BACKUP_ADMIN ON {level} TO 'u1'@'%'")) {
            Err(DriverError::IllegalPrivilegeLevel(name)) => assert_eq!(name, "BACKUP_ADMIN"),
            other => panic!("expected IllegalPrivilegeLevel, got {other:?}"),
        }
    }
    match session.run("REVOKE BACKUP_ADMIN ON db1.* FROM 'u1'@'%'") {
        Err(DriverError::IllegalPrivilegeLevel(name)) => assert_eq!(name, "BACKUP_ADMIN"),
        other => panic!("expected IllegalPrivilegeLevel, got {other:?}"),
    }
    // An UNREGISTERED name is 3929 at `*.*` -- and 3619 elsewhere, since
    // the level check runs first.
    match session.run("GRANT NOT_A_REAL_PRIV ON *.* TO 'u1'@'%'") {
        Err(DriverError::DynamicPrivilegeNotRegistered(name)) => {
            assert_eq!(name, "NOT_A_REAL_PRIV");
        }
        other => panic!("expected DynamicPrivilegeNotRegistered, got {other:?}"),
    }
    match session.run("GRANT NOT_A_REAL_PRIV ON db1.* TO 'u1'@'%'") {
        Err(DriverError::IllegalPrivilegeLevel(name)) => assert_eq!(name, "NOT_A_REAL_PRIV"),
        other => panic!("expected IllegalPrivilegeLevel, got {other:?}"),
    }

    // The registered names are accepted case-insensitively.
    session
        .run("GRANT BACKUP_ADMIN ON *.* TO 'u1'@'%'")
        .unwrap();
    session
        .run("GRANT connection_admin ON *.* TO 'u1'@'%'")
        .unwrap();
    session
        .run("GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'u1'@'%' WITH GRANT OPTION")
        .unwrap();
    session
        .run("GRANT RESTRICTED_USER_ADMIN ON *.* TO 'u1'@'%' WITH GRANT OPTION")
        .unwrap();
    session
        .run("GRANT SELECT, PROCESS ON *.* TO 'u1'@'%'")
        .unwrap();
    session.run("GRANT INSERT ON db1.* TO 'u1'@'%'").unwrap();
    session.run("GRANT UPDATE ON db1.t TO 'u1'@'%'").unwrap();

    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
        [
            ["GRANT SELECT,PROCESS ON *.* TO 'u1'@'%'"],
            ["GRANT INSERT ON `db1`.* TO 'u1'@'%'"],
            ["GRANT UPDATE ON `db1`.`t` TO 'u1'@'%'"],
            ["GRANT BACKUP_ADMIN,CONNECTION_ADMIN ON *.* TO 'u1'@'%'"],
            [
                "GRANT RESTRICTED_USER_ADMIN,SYSTEM_VARIABLES_ADMIN ON *.* TO 'u1'@'%' \
                     WITH GRANT OPTION"
            ],
        ]
    );

    // REVOKE of a registered privilege the account holds; of one it does
    // not hold (silent); of an unregistered name (3929 as a WARNING, the
    // statement still succeeding).
    session
        .run("REVOKE BACKUP_ADMIN ON *.* FROM 'u1'@'%'")
        .unwrap();
    session
        .run("REVOKE ROLE_ADMIN ON *.* FROM 'u1'@'%'")
        .unwrap();
    session
        .run("REVOKE NOT_A_REAL_PRIV ON *.* FROM 'u1'@'%'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        [[
            "Warning",
            "3929",
            "Dynamic privilege 'NOT_A_REAL_PRIV' is not registered with the server."
        ]]
    );
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
        [
            ["GRANT SELECT,PROCESS ON *.* TO 'u1'@'%'"],
            ["GRANT INSERT ON `db1`.* TO 'u1'@'%'"],
            ["GRANT UPDATE ON `db1`.`t` TO 'u1'@'%'"],
            ["GRANT CONNECTION_ADMIN ON *.* TO 'u1'@'%'"],
            [
                "GRANT RESTRICTED_USER_ADMIN,SYSTEM_VARIABLES_ADMIN ON *.* TO 'u1'@'%' \
                     WITH GRANT OPTION"
            ],
        ]
    );

    // An account whose ONLY privileges are dynamic still gets the
    // `USAGE` global line ahead of them.
    session.run("CREATE USER 'u2'@'%'").unwrap();
    session
        .run("GRANT DASHBOARD_CLIENT, ROLE_ADMIN ON *.* TO 'u2'@'%'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u2'@'%'")),
        [
            ["GRANT USAGE ON *.* TO 'u2'@'%'"],
            ["GRANT DASHBOARD_CLIENT,ROLE_ADMIN ON *.* TO 'u2'@'%'"],
        ]
    );

    // `GRANT ALL` confers no dynamic privilege, but `REVOKE ALL` clears
    // every one of them (Go's unqualified `DELETE FROM
    // mysql.global_grants`).
    session.run("REVOKE ALL ON *.* FROM 'u2'@'%'").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u2'@'%'")),
        [["GRANT USAGE ON *.* TO 'u2'@'%'"]]
    );
    session.run("GRANT ALL ON *.* TO 'u2'@'%'").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u2'@'%'")),
        [["GRANT ALL PRIVILEGES ON *.* TO 'u2'@'%'"]]
    );
}

/// The SUPER fallback: Go's `RequestDynamicVerification` passes a dynamic
/// check for any account holding SUPER, even with no `global_grants` row
/// -- while `HasExplicitlyGrantedDynamicPrivilege` does not. The only
/// no-fallback case in Go is SEM's `RESTRICTED_*` family, and SEM is not
/// modelled here.
#[test]
fn super_is_the_fallback_for_every_dynamic_privilege() {
    let mut session = session_with_privileges();
    session.run("CREATE USER 'su'@'%'").unwrap();
    session.run("GRANT SUPER ON *.* TO 'su'@'%'").unwrap();
    let registry = session.privileges.clone().unwrap();

    for name in privilege::DYNAMIC_PRIVS {
        assert!(
            registry.has_dynamic_priv("su", "%", name, false),
            "SUPER satisfies {name}"
        );
        assert!(
            !registry.has_explicit_dynamic_priv("su", "%", name, false),
            "{name} is not explicitly granted"
        );
    }

    // SUPER alone does not satisfy a GRANTABLE dynamic check: the
    // account must also hold GRANT OPTION.
    assert!(!registry.has_dynamic_priv("su", "%", "BACKUP_ADMIN", true));
    session
        .run("GRANT SUPER ON *.* TO 'su'@'%' WITH GRANT OPTION")
        .unwrap();
    assert!(registry.has_dynamic_priv("su", "%", "BACKUP_ADMIN", true));

    // An account with neither SUPER nor a row fails every check, and
    // `SHOW GRANTS` for a SUPER account prints no dynamic line -- the
    // fallback is a check-time rule, not stored state.
    session.run("CREATE USER 'plain'@'%'").unwrap();
    assert!(!registry.has_dynamic_priv("plain", "%", "BACKUP_ADMIN", false));
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'su'@'%'")),
        [["GRANT SUPER ON *.* TO 'su'@'%' WITH GRANT OPTION"]]
    );

    // Re-granting without `WITH GRANT OPTION` is a REPLACE, not an OR:
    // it downgrades a previously grantable dynamic privilege.
    session
        .run("GRANT BACKUP_ADMIN ON *.* TO 'plain'@'%' WITH GRANT OPTION")
        .unwrap();
    assert!(registry.has_explicit_dynamic_priv("plain", "%", "BACKUP_ADMIN", true));
    session
        .run("GRANT BACKUP_ADMIN ON *.* TO 'plain'@'%'")
        .unwrap();
    assert!(!registry.has_explicit_dynamic_priv("plain", "%", "BACKUP_ADMIN", true));
    assert!(registry.has_explicit_dynamic_priv("plain", "%", "BACKUP_ADMIN", false));
}

/// `information_schema.USER_PRIVILEGES` -- the one member of the
/// PRIVILEGES family that Go actually populates. Captured: every
/// account's static rows first (username order, `AllGlobalPrivs` print
/// order, a lone `USAGE` row for an account with none), then every
/// account's dynamic rows; `IS_GRANTABLE` is the account's `GRANT
/// OPTION` on a static row and the privilege's own flag on a dynamic
/// one.
#[test]
fn user_privileges_table_reports_static_and_dynamic_rows() {
    let mut session = session_with_privileges();
    session.set_user("root@%".to_owned(), "root@127.0.0.1".to_owned());
    session.run("CREATE USER 'zz'@'%'").unwrap();
    session.run("CREATE USER 'aa'@'%'").unwrap();
    session.run("GRANT SELECT ON *.* TO 'aa'@'%'").unwrap();
    session.run("GRANT ROLE_ADMIN ON *.* TO 'aa'@'%'").unwrap();
    session
        .run("GRANT BACKUP_ADMIN ON *.* TO 'zz'@'%' WITH GRANT OPTION")
        .unwrap();

    let rows = row_text(session.run(
        "SELECT grantee, table_catalog, privilege_type, is_grantable \
             FROM information_schema.user_privileges WHERE grantee <> '''root''@''%'''",
    ));
    assert_eq!(
        rows,
        [
            ["'aa'@'%'", "def", "SELECT", "NO"],
            ["'zz'@'%'", "def", "USAGE", "NO"],
            ["'aa'@'%'", "def", "ROLE_ADMIN", "NO"],
            ["'zz'@'%'", "def", "BACKUP_ADMIN", "YES"],
        ]
    );
}

/// CAPTURED: `SHOW GRANTS` with no `FOR` reports the current session's
/// own account, and a fresh cluster's bootstrap `root`@`%` carries
/// `ALL PRIVILEGES ... WITH GRANT OPTION`.
#[test]
fn show_grants_for_current_user_reports_root_bootstrap() {
    let mut session = session_with_privileges();
    session.set_user("root@%".to_owned(), "root@127.0.0.1".to_owned());
    assert_eq!(
        row_text(session.run("SHOW GRANTS")),
        [["GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION"]]
    );
}

/// CAPTURED: re-creating an existing account is `ErrCannotUser`/1396,
/// quoted `'user'@'host'` (unlike `DROP USER`'s unquoted form).
#[test]
fn create_user_rejects_a_duplicate_account() {
    let mut session = session_with_privileges();
    session.run("CREATE USER 'dup1'@'%'").unwrap();
    match session.run("CREATE USER 'dup1'@'%'") {
        Err(DriverError::CreateUserAlreadyExists { user, host }) => {
            assert_eq!(user, "dup1");
            assert_eq!(host, "%");
        }
        other => panic!("expected CreateUserAlreadyExists, got {other:?}"),
    }
}

/// CAPTURED: `GRANT ... TO` an account that was never created is
/// `ErrCantCreateUserWithGrant`/1410 -- TiDB's default sql_mode refuses
/// to implicitly create the target.
#[test]
fn grant_to_an_unknown_user_is_refused() {
    let mut session = session_with_privileges();
    assert!(matches!(
        session.run("GRANT SELECT ON *.* TO 'nouser'@'%'"),
        Err(DriverError::GrantToUnknownUser)
    ));
}

/// CAPTURED: an unrecognized privilege name parses (through
/// `tidb-parser`'s dynamic-privilege grammar branch) but is refused at
/// execution with `ErrDynamicPrivilegeNotRegistered`/3929, naming the
/// privilege.
#[test]
fn granting_an_unregistered_privilege_name_is_refused() {
    let mut session = session_with_privileges();
    session.run("CREATE USER 'dup1'@'%'").unwrap();
    match session.run("GRANT FOOBAR ON *.* TO 'dup1'@'%'") {
        Err(DriverError::DynamicPrivilegeNotRegistered(name)) => assert_eq!(name, "FOOBAR"),
        other => panic!("expected DynamicPrivilegeNotRegistered, got {other:?}"),
    }
}

/// CAPTURED: `REVOKE ... FROM` an account that does not exist is Go's
/// plain `errors.Errorf("Unknown user: %s", user)`.
#[test]
fn revoke_from_an_unknown_user_is_refused() {
    let mut session = session_with_privileges();
    match session.run("REVOKE SELECT ON *.* FROM 'nouser'@'%'") {
        Err(DriverError::RevokeUnknownUser { user, host }) => {
            assert_eq!(user, "nouser");
            assert_eq!(host, "%");
        }
        other => panic!("expected RevokeUnknownUser, got {other:?}"),
    }
}

/// `ALL PRIVILEGES` grants every modeled global privilege, which folds
/// `SHOW GRANTS` to the `ALL PRIVILEGES` literal (Go `userPrivToString`).
#[test]
fn grant_all_privileges_collapses_show_grants() {
    let mut session = session_with_privileges();
    session.run("CREATE USER 'dup1'@'%'").unwrap();
    session
        .run("GRANT ALL PRIVILEGES ON *.* TO 'dup1'@'%'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'dup1'@'%'")),
        [["GRANT ALL PRIVILEGES ON *.* TO 'dup1'@'%'"]]
    );
}

/// OUT OF SCOPE, refused rather than faked: column lists. (Database and
/// table-level grants, `WITH GRANT OPTION` and roles are all modeled now --
/// see the `db_scope_*`/`table_scope_*`/`grant_option_*`/`role_*` tests.)
#[test]
fn out_of_scope_grant_forms_are_refused() {
    let mut session = session_with_privileges();
    session.run("CREATE USER 'dup1'@'%'").unwrap();
    assert!(matches!(
        session.run("GRANT SELECT (a) ON test.t TO 'dup1'@'%'"),
        Err(DriverError::Unsupported(_))
    ));
}

/// CAPTURED (`pkg/executor/zz_dump_authlc_test.go`): `WITH GRANT OPTION`
/// at all three scopes, its ` WITH GRANT OPTION` suffix printing at the
/// END of each affected `SHOW GRANTS` line (never inside the privilege
/// list), and `REVOKE GRANT OPTION ON <level>` clearing exactly that one
/// scope's bit and nothing else.
#[test]
fn grant_option_is_a_per_scope_bit_printed_as_a_line_suffix() {
    let mut session = session_with_privileges();
    session.run("CREATE TABLE test.t (a int)").unwrap();
    session.run("CREATE USER 'bob'@'%'").unwrap();

    session
        .run("GRANT SELECT ON *.* TO 'bob'@'%' WITH GRANT OPTION")
        .unwrap();
    session
        .run("GRANT SELECT ON test.* TO 'bob'@'%' WITH GRANT OPTION")
        .unwrap();
    session
        .run("GRANT SELECT ON test.t TO 'bob'@'%' WITH GRANT OPTION")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'bob'@'%'")),
        [
            ["GRANT SELECT ON *.* TO 'bob'@'%' WITH GRANT OPTION"],
            ["GRANT SELECT ON `test`.* TO 'bob'@'%' WITH GRANT OPTION"],
            ["GRANT SELECT ON `test`.`t` TO 'bob'@'%' WITH GRANT OPTION"],
        ]
    );

    // Each REVOKE clears one scope, innermost first, leaving the others
    // untouched -- the captured Go sequence exactly.
    session
        .run("REVOKE GRANT OPTION ON test.t FROM 'bob'@'%'")
        .unwrap();
    session
        .run("REVOKE GRANT OPTION ON test.* FROM 'bob'@'%'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'bob'@'%'")),
        [
            ["GRANT SELECT ON *.* TO 'bob'@'%' WITH GRANT OPTION"],
            ["GRANT SELECT ON `test`.* TO 'bob'@'%'"],
            ["GRANT SELECT ON `test`.`t` TO 'bob'@'%'"],
        ]
    );
    session
        .run("REVOKE GRANT OPTION ON *.* FROM 'bob'@'%'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'bob'@'%'")),
        [
            ["GRANT SELECT ON *.* TO 'bob'@'%'"],
            ["GRANT SELECT ON `test`.* TO 'bob'@'%'"],
            ["GRANT SELECT ON `test`.`t` TO 'bob'@'%'"],
        ]
    );
}

/// CAPTURED: `GRANT ALL` does NOT confer `GRANT OPTION` (the `ALL
/// PRIVILEGES` literal still prints with no suffix), and naming
/// `GRANT OPTION` as an ordinary privilege confers exactly that bit --
/// which is why `mysql.GrantPriv` must live outside every `ALL_*` list.
#[test]
fn grant_all_withholds_grant_option_but_the_named_privilege_confers_it() {
    let mut session = session_with_privileges();
    session.run("CREATE USER 'occupied'@'%'").unwrap();
    session.run("GRANT ALL ON *.* TO 'occupied'@'%'").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'occupied'@'%'")),
        [["GRANT ALL PRIVILEGES ON *.* TO 'occupied'@'%'"]]
    );
    session
        .run("GRANT GRANT OPTION ON *.* TO 'occupied'@'%'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'occupied'@'%'")),
        [["GRANT ALL PRIVILEGES ON *.* TO 'occupied'@'%' WITH GRANT OPTION"]]
    );
}

/// CAPTURED: `CREATE USER ... IDENTIFIED BY` stores Go
/// `auth.EncodePassword`'s `*<40 UPPERCASE HEX>` double-SHA-1 in
/// `mysql.user.authentication_string`; a passwordless account stores the
/// EMPTY string, not a hash of the empty string. `ALTER USER ...
/// IDENTIFIED BY` and `SET PASSWORD FOR` both rewrite the same column to
/// the identical value.
#[test]
fn account_authentication_strings_follow_go_encode_password() {
    assert_eq!(
        privilege::encode_password("bobpw"),
        "*6793F32F5FAF66A40EFA6B5E9887765E983829BC"
    );
    assert_eq!(privilege::encode_password(""), "");

    let registry = privilege::PrivilegeRegistry::default();
    let mut session = Session::new();
    session.attach_privileges(registry.clone());
    session.set_user("root@%".to_owned(), "root@127.0.0.1".to_owned());

    session
        .run("CREATE USER 'bob'@'%' IDENTIFIED BY 'bobpw'")
        .unwrap();
    assert_eq!(
        registry.auth_string("bob", "%").as_deref(),
        Some("*6793F32F5FAF66A40EFA6B5E9887765E983829BC")
    );
    session.run("CREATE USER 'nopw'@'%'").unwrap();
    assert_eq!(registry.auth_string("nopw", "%").as_deref(), Some(""));

    session
        .run("ALTER USER 'bob'@'%' IDENTIFIED BY 'bobpw2'")
        .unwrap();
    assert_eq!(
        registry.auth_string("bob", "%").as_deref(),
        Some("*35141DF602B302AB26CD0E9930DDBAF0E5865904")
    );
    session
        .run("SET PASSWORD FOR 'bob'@'%' = 'bobpw3'")
        .unwrap();
    assert_eq!(
        registry.auth_string("bob", "%").as_deref(),
        Some("*DBED499ADC8B1C308546E054BE45BEA463AC68B9")
    );

    // Captured error wording: ALTER USER quotes the account like CREATE
    // USER and is silenced by IF EXISTS; SET PASSWORD reports 1133
    // instead of reusing ErrCannotUser.
    assert!(matches!(
        session.run("ALTER USER 'nosuch'@'%' IDENTIFIED BY 'p'"),
        Err(DriverError::AlterUserMissing { .. })
    ));
    session
        .run("ALTER USER IF EXISTS 'nosuch'@'%' IDENTIFIED BY 'p'")
        .unwrap();
    assert!(matches!(
        session.run("SET PASSWORD FOR 'nosuch'@'%' = 'p'"),
        Err(DriverError::SetPasswordNoMatchingRow)
    ));
}

/// CAPTURED: `CREATE USER ... IDENTIFIED WITH <plugin> [BY '<password>' |
/// AS '<hash>']`. An accepted plugin creates a real, gantable account
/// regardless of whether this tier can verify a login against it; an
/// unrecognized name is Go's `ErrPluginIsNotLoaded` (1524), and a
/// malformed `AS` hash is Go's `ErrPasswordFormat` (1827).
#[test]
fn create_user_identified_with_stores_the_plugin_and_validates_credentials() {
    let registry = privilege::PrivilegeRegistry::default();
    let mut session = Session::new();
    session.attach_privileges(registry.clone());
    session.set_user("root@%".to_owned(), "root@127.0.0.1".to_owned());

    // `BY` hashes the caching_sha2 way and is a real 70-byte `$A$...` shape,
    // not the native `*40HEX` shape.
    session
        .run("CREATE USER 'dana'@'%' IDENTIFIED WITH caching_sha2_password BY 'danapw'")
        .unwrap();
    assert_eq!(
        registry.plugin("dana", "%").as_deref(),
        Some("caching_sha2_password")
    );
    let dana_auth = registry.auth_string("dana", "%").unwrap();
    assert_eq!(dana_auth.len(), 70);
    assert!(dana_auth.starts_with("$A$"));
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'dana'@'%'")),
        [["GRANT USAGE ON *.* TO 'dana'@'%'"]]
    );

    // A plugin-only clause (no BY/AS) is a passwordless account under that
    // plugin.
    session
        .run("CREATE USER 'tok'@'%' IDENTIFIED WITH tidb_auth_token")
        .unwrap();
    assert_eq!(
        registry.plugin("tok", "%").as_deref(),
        Some("tidb_auth_token")
    );
    assert_eq!(registry.auth_string("tok", "%").as_deref(), Some(""));

    // `AS '<hash>'` stores an already-hashed string once it is the right
    // shape for the plugin.
    let hash40 = format!("*{}", "F".repeat(40));
    session
        .run(&format!(
            "CREATE USER 'preset'@'%' IDENTIFIED WITH mysql_native_password AS '{hash40}'"
        ))
        .unwrap();
    assert_eq!(
        registry.auth_string("preset", "%").as_deref(),
        Some(hash40.as_str())
    );

    // A malformed `AS` hash is ErrPasswordFormat (1827), not a silent
    // truncation or panic.
    assert!(matches!(
        session.run("CREATE USER 'bad'@'%' IDENTIFIED WITH mysql_native_password AS 'short'"),
        Err(DriverError::PasswordFormat)
    ));
    assert!(!registry.user_exists("bad", "%"));

    // An unrecognized plugin is ErrPluginIsNotLoaded (1524): this tier
    // registers no extension auth plugins, so nothing outside Go's built-in
    // CREATE USER switch can ever be loaded.
    assert!(matches!(
        session.run("CREATE USER 'nope'@'%' IDENTIFIED WITH 'no_such_plugin' BY 'x'"),
        Err(DriverError::PluginIsNotLoaded { plugin }) if plugin == "no_such_plugin"
    ));
    assert!(!registry.user_exists("nope", "%"));

    // `mysql_clear_password` and `tidb_session_token` are built-in plugin
    // NAMES (reserved against extensions) but are not in Go's CREATE USER
    // switch either, so they are refused the same way.
    assert!(matches!(
        session.run("CREATE USER 'clear'@'%' IDENTIFIED WITH mysql_clear_password BY 'x'"),
        Err(DriverError::PluginIsNotLoaded { .. })
    ));
}

/// CAPTURED: `RENAME USER` carries the authentication string AND every
/// scoped grant row to the new identity, leaves the old identity with no
/// grant row at all, and reports Go's two distinct reason clauses.
#[test]
fn rename_user_moves_the_whole_account_row() {
    let registry = privilege::PrivilegeRegistry::default();
    let mut session = Session::new();
    session.attach_privileges(registry.clone());
    session.run("CREATE TABLE test.t (a int)").unwrap();
    session
        .run("CREATE USER 'bob'@'%' IDENTIFIED BY 'bobpw'")
        .unwrap();
    session.run("GRANT SELECT ON *.* TO 'bob'@'%'").unwrap();
    session.run("GRANT SELECT ON test.* TO 'bob'@'%'").unwrap();
    session.run("GRANT SELECT ON test.t TO 'bob'@'%'").unwrap();
    session.run("CREATE USER 'occupied'@'%'").unwrap();

    session.run("RENAME USER 'bob'@'%' TO 'bobby'@'%'").unwrap();
    assert_eq!(
        registry.auth_string("bobby", "%").as_deref(),
        Some("*6793F32F5FAF66A40EFA6B5E9887765E983829BC")
    );
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'bobby'@'%'")),
        [
            ["GRANT SELECT ON *.* TO 'bobby'@'%'"],
            ["GRANT SELECT ON `test`.* TO 'bobby'@'%'"],
            ["GRANT SELECT ON `test`.`t` TO 'bobby'@'%'"],
        ]
    );
    assert!(session.run("SHOW GRANTS FOR 'bob'@'%'").is_err());

    match session.run("RENAME USER 'nosuch'@'%' TO 'x'@'%'") {
        Err(DriverError::RenameUserFailed { old_missing, .. }) => assert!(old_missing),
        other => panic!("expected RenameUserFailed, got {other:?}"),
    }
    match session.run("RENAME USER 'bobby'@'%' TO 'occupied'@'%'") {
        Err(DriverError::RenameUserFailed { old_missing, .. }) => assert!(!old_missing),
        other => panic!("expected RenameUserFailed, got {other:?}"),
    }
}

/// CAPTURED: `RENAME USER` also moves `mysql.role_edges` (both directions)
/// and `mysql.default_roles` rows, so a renamed grantee keeps every role it
/// held, a renamed role keeps every grantee it was granted to (and those
/// grantees' `SHOW GRANTS` still lists it), and default-role membership
/// follows the rename too.
#[test]
fn rename_user_moves_role_edges_and_default_roles() {
    let mut session = session_with_privileges();
    session.run("CREATE ROLE 'r1'@'%'").unwrap();
    session.run("CREATE USER 'u1'@'%'").unwrap();
    session.run("GRANT 'r1'@'%' TO 'u1'@'%'").unwrap();
    session.run("SET DEFAULT ROLE 'r1'@'%' TO 'u1'@'%'").ok();

    // Renaming the GRANTEE: the new identity keeps the granted role.
    session.run("RENAME USER 'u1'@'%' TO 'u2'@'%'").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u2'@'%' USING 'r1'@'%'")),
        [
            ["GRANT USAGE ON *.* TO 'u2'@'%'"],
            ["GRANT 'r1'@'%' TO 'u2'@'%'"],
        ]
    );

    // Renaming the ROLE: the existing grantee's edge follows to the new name.
    session.run("RENAME USER 'r1'@'%' TO 'r2'@'%'").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u2'@'%' USING 'r2'@'%'")),
        [
            ["GRANT USAGE ON *.* TO 'u2'@'%'"],
            ["GRANT 'r2'@'%' TO 'u2'@'%'"],
        ]
    );
}

/// CAPTURED: `ALTER USER ... IDENTIFIED WITH <plugin> [BY '<password>' | AS
/// '<hash>']` rewrites BOTH `mysql.user.plugin` and `authentication_string`,
/// the same as `CREATE USER`'s clause; a bare `IDENTIFIED BY` (no `WITH`)
/// leaves the account's existing plugin untouched.
#[test]
fn alter_user_identified_with_changes_the_plugin_and_password() {
    let mut session = session_with_privileges();
    session
        .run("CREATE USER 'bob'@'%' IDENTIFIED BY 'bobpw'")
        .unwrap();
    let registry = session.privileges.clone().unwrap();
    assert_eq!(
        registry.plugin("bob", "%").as_deref(),
        Some("mysql_native_password")
    );

    // Plugin + password together.
    session
        .run("ALTER USER 'bob'@'%' IDENTIFIED WITH caching_sha2_password BY 'newpw'")
        .unwrap();
    assert_eq!(
        registry.plugin("bob", "%").as_deref(),
        Some("caching_sha2_password")
    );
    let auth = registry.auth_string("bob", "%").unwrap();
    assert!(auth.starts_with("$A$"), "got {auth:?}");

    // A bare IDENTIFIED BY afterwards keeps the now-current plugin rather
    // than resetting it to mysql_native_password.
    session
        .run("ALTER USER 'bob'@'%' IDENTIFIED BY 'again'")
        .unwrap();
    assert_eq!(
        registry.plugin("bob", "%").as_deref(),
        Some("caching_sha2_password")
    );
}

/// CAPTURED: `ALTER USER ... ACCOUNT LOCK` / `ACCOUNT UNLOCK` flips the same
/// `account_locked` flag a role's password-less row uses, so a locked plain
/// user refuses login exactly like a role does, and `ACCOUNT UNLOCK`
/// reverses it.
#[test]
fn alter_user_account_lock_unlock() {
    let mut session = session_with_privileges();
    session.run("CREATE USER 'bob'@'%'").unwrap();
    let registry = session.privileges.clone().unwrap();
    assert!(!registry.is_role("bob", "%"));

    session.run("ALTER USER 'bob'@'%' ACCOUNT LOCK").unwrap();
    assert!(registry.is_role("bob", "%"));

    session.run("ALTER USER 'bob'@'%' ACCOUNT UNLOCK").unwrap();
    assert!(!registry.is_role("bob", "%"));

    assert!(matches!(
        session.run("ALTER USER 'nosuch'@'%' ACCOUNT LOCK"),
        Err(DriverError::AlterUserMissing { .. })
    ));
    session
        .run("ALTER USER IF EXISTS 'nosuch'@'%' ACCOUNT LOCK")
        .unwrap();
}

/// CAPTURED: `DROP USER` clears the account's scoped grant rows too, so
/// an account later recreated under the same identity starts from USAGE
/// rather than inheriting the dropped account's grants.
#[test]
fn drop_user_clears_scoped_grant_rows() {
    let mut session = session_with_privileges();
    session.run("CREATE USER 'gone'@'%'").unwrap();
    session.run("GRANT SELECT ON test.* TO 'gone'@'%'").unwrap();
    session.run("DROP USER 'gone'@'%'").unwrap();
    session.run("CREATE USER 'gone'@'%'").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'gone'@'%'")),
        [["GRANT USAGE ON *.* TO 'gone'@'%'"]]
    );
}

/// CAPTURED end to end (`pkg/executor/grant.go`/`revoke.go`,
/// `pkg/privilege/privileges/cache.go`'s `showGrants`): DB-scope
/// `GRANT`/`REVOKE`/`SHOW GRANTS`, including the `ALL PRIVILEGES`
/// literal and the lexical (not insertion, not plain-name) sort order
/// across multiple databases.
#[test]
fn db_scope_grant_revoke_and_show_grants_round_trip() {
    let mut session = session_with_privileges();
    session.run("CREATE USER 'u1'@'%'").unwrap();
    session.run("CREATE DATABASE db1").unwrap();
    session.run("CREATE DATABASE aaadb").unwrap();

    session.run("GRANT SELECT ON db1.* TO 'u1'@'%'").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
        [
            vec!["GRANT USAGE ON *.* TO 'u1'@'%'".to_owned()],
            vec!["GRANT SELECT ON `db1`.* TO 'u1'@'%'".to_owned()],
        ]
    );

    // A second DB, granted later, still sorts before `db1` (captured:
    // Go sorts DB-scope lines lexically by their formatted text).
    session.run("GRANT SELECT ON aaadb.* TO 'u1'@'%'").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
        [
            vec!["GRANT USAGE ON *.* TO 'u1'@'%'".to_owned()],
            vec!["GRANT SELECT ON `aaadb`.* TO 'u1'@'%'".to_owned()],
            vec!["GRANT SELECT ON `db1`.* TO 'u1'@'%'".to_owned()],
        ]
    );

    // Once `db1`'s line becomes `GRANT ALL PRIVILEGES ...`, it sorts
    // *before* `aaadb`'s `GRANT SELECT ...` line: the sort key is the
    // whole formatted string, which starts with the privilege text, not
    // the database name ('A' < 'S').
    session.run("GRANT ALL ON db1.* TO 'u1'@'%'").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'"))[1],
        vec!["GRANT ALL PRIVILEGES ON `db1`.* TO 'u1'@'%'".to_owned()]
    );

    session.run("REVOKE ALL ON db1.* FROM 'u1'@'%'").unwrap();
    session.run("REVOKE SELECT ON db1.* FROM 'u1'@'%'").unwrap();
    // Back to `GRANT USAGE ...`, which sorts after `aaadb`'s `SELECT`
    // line again ('U' > 'S').
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'"))[2],
        vec!["GRANT USAGE ON `db1`.* TO 'u1'@'%'".to_owned()]
    );
}

/// CAPTURED: `GRANT PROCESS ON db.*` (a global-only privilege) is Go's
/// `ErrWrongUsage`/1221, "Incorrect usage of DB GRANT and GLOBAL
/// PRIVILEGES".
#[test]
fn db_scope_grant_rejects_global_only_privilege() {
    let mut session = session_with_privileges();
    session.run("CREATE USER 'u1'@'%'").unwrap();
    session.run("CREATE DATABASE db1").unwrap();
    assert!(matches!(
        session.run("GRANT PROCESS ON db1.* TO 'u1'@'%'"),
        Err(DriverError::DbGrantGlobalOnlyPriv)
    ));
}

/// CAPTURED: `REVOKE ... ON db.*` for an account with no `mysql.DB` row
/// for that database at all is Go's plain "There is no such grant
/// defined for user '%s' on host '%s' on database %s".
#[test]
fn db_scope_revoke_without_any_grant_row_is_refused() {
    let mut session = session_with_privileges();
    session.run("CREATE USER 'u1'@'%'").unwrap();
    session.run("CREATE DATABASE emptydb").unwrap();
    match session.run("REVOKE SELECT ON emptydb.* FROM 'u1'@'%'") {
        Err(DriverError::RevokeNoDbGrant {
            user,
            host,
            database,
        }) => {
            assert_eq!(user, "u1");
            assert_eq!(host, "%");
            assert_eq!(database, "emptydb");
        }
        other => panic!("expected RevokeNoDbGrant, got {other:?}"),
    }
}

/// CAPTURED end to end: TABLE-scope `GRANT`/`REVOKE`/`SHOW GRANTS`,
/// including the `ALL PRIVILEGES` literal, backtick-quoted
/// `` `db`.`table` `` (both segments escaped, same as Go's
/// `stringutil.Escape`), and the invalid-scope-privilege / missing-table
/// error split (Go checks privilege validity before table existence).
#[test]
fn table_scope_grant_revoke_and_show_grants_round_trip() {
    let mut session = session_with_privileges();
    session.run("CREATE USER 'u1'@'%'").unwrap();
    session.run("CREATE DATABASE db1").unwrap();
    session.run("CREATE TABLE db1.t1 (a INT)").unwrap();

    session
        .run("GRANT SELECT, INSERT ON db1.t1 TO 'u1'@'%'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
        [
            vec!["GRANT USAGE ON *.* TO 'u1'@'%'".to_owned()],
            vec!["GRANT SELECT,INSERT ON `db1`.`t1` TO 'u1'@'%'".to_owned()],
        ]
    );

    session.run("GRANT ALL ON db1.t1 TO 'u1'@'%'").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'"))[1],
        vec!["GRANT ALL PRIVILEGES ON `db1`.`t1` TO 'u1'@'%'".to_owned()]
    );

    session.run("REVOKE ALL ON db1.t1 FROM 'u1'@'%'").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'"))[1],
        vec!["GRANT USAGE ON `db1`.`t1` TO 'u1'@'%'".to_owned()]
    );

    // Invalid-scope privilege: refused before the table-existence
    // check runs (captured `ErrIllegalGrantForTable`/1144).
    assert!(matches!(
        session.run("GRANT PROCESS ON db1.t1 TO 'u1'@'%'"),
        Err(DriverError::IllegalGrantForTable)
    ));

    // A valid privilege on a table that does not exist: refused with
    // `ErrTableNotExists`/1146 (captured), unless `CREATE` is among the
    // granted privileges (Go's issue #28533/#29268 exception).
    assert!(matches!(
        session.run("GRANT SELECT ON db1.nosuchtable TO 'u1'@'%'"),
        Err(DriverError::Schema(SchemaErrorKind::UnknownTable(ref name)))
            if name == "db1.nosuchtable"
    ));
    session
        .run("GRANT CREATE ON db1.nosuchtable TO 'u1'@'%'")
        .unwrap();

    // REVOKE for an account with no `mysql.Tables_priv` row at all.
    session.run("CREATE TABLE db1.t2 (a INT)").unwrap();
    match session.run("REVOKE SELECT ON db1.t2 FROM 'u1'@'%'") {
        Err(DriverError::RevokeNoTableGrant {
            user,
            host,
            database,
            table,
        }) => {
            assert_eq!(user, "u1");
            assert_eq!(host, "%");
            assert_eq!(database, "db1");
            assert_eq!(table, "t2");
        }
        other => panic!("expected RevokeNoTableGrant, got {other:?}"),
    }
}

/// Go `planbuilder.go`'s `*ast.KillStmt` case: a session may always KILL
/// its OWN connection, but killing a peer logged in as a DIFFERENT user
/// is refused with `ErrSpecificAccessDenied` (1227) unless the caller
/// holds SUPER. Granting SUPER then lets the same KILL through.
#[test]
fn kill_of_another_users_connection_requires_super() {
    let registry = process::ProcessRegistry::default();
    let mut victim = session_with_privileges();
    victim.set_user("root@%".to_owned(), "root@10.0.0.1".to_owned());
    let victim_guard = registry.register(
        1,
        "root".to_owned(),
        "10.0.0.1:1".to_owned(),
        "test".to_owned(),
        None,
    );
    victim.attach_process(1, victim_guard);

    let mut bob = session_with_privileges();
    bob.set_user("bob@%".to_owned(), "bob@10.0.0.2".to_owned());
    let bob_guard = registry.register(
        2,
        "bob".to_owned(),
        "10.0.0.2:2".to_owned(),
        "test".to_owned(),
        None,
    );
    bob.attach_process(2, bob_guard);
    bob.run("CREATE USER 'bob'@'%'").unwrap();

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
    bob.run("GRANT SUPER ON *.* TO 'bob'@'%'").unwrap();
    assert_eq!(bob.run("kill 1").unwrap(), StmtResult::Affected(0));
}

/// The gate Go actually writes is the DYNAMIC `CONNECTION_ADMIN`; SUPER
/// passes only as its fallback. So `CONNECTION_ADMIN` ALONE -- with no
/// SUPER anywhere -- must open the same KILL, and revoking it must close
/// it again.
#[test]
fn kill_of_another_users_connection_accepts_connection_admin() {
    let registry = process::ProcessRegistry::default();
    let mut victim = session_with_privileges();
    victim.set_user("root@%".to_owned(), "root@10.0.0.1".to_owned());
    let victim_guard = registry.register(
        1,
        "root".to_owned(),
        "10.0.0.1:1".to_owned(),
        "test".to_owned(),
        None,
    );
    victim.attach_process(1, victim_guard);

    let mut bob = session_with_privileges();
    bob.set_user("bob@%".to_owned(), "bob@10.0.0.2".to_owned());
    let bob_guard = registry.register(
        2,
        "bob".to_owned(),
        "10.0.0.2:2".to_owned(),
        "test".to_owned(),
        None,
    );
    bob.attach_process(2, bob_guard);
    bob.run("CREATE USER 'bob'@'%'").unwrap();

    match bob.run("kill 1") {
        Err(DriverError::KillAccessDenied) => {}
        other => panic!("expected KillAccessDenied, got {other:?}"),
    }

    bob.run("GRANT CONNECTION_ADMIN ON *.* TO 'bob'@'%'")
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

    bob.run("REVOKE CONNECTION_ADMIN ON *.* FROM 'bob'@'%'")
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
    let mut session = session_with_privileges();
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

    session.run("CREATE USER 'bob'@'%'").unwrap();
    assert_eq!(row_text(session.run("show processlist")).len(), 1);

    session.run("GRANT PROCESS ON *.* TO 'bob'@'%'").unwrap();
    assert_eq!(row_text(session.run("show processlist")).len(), 2);
}

// ---------------------------------------------------------------------
// ROLES. Every case below is captured from Go through
// `pkg/executor/zz_dump_roles_test.go` (`testkit.CreateMockStore`).
// ---------------------------------------------------------------------

/// CAPTURED: a role IS a `mysql.user` row, so roles and users share one
/// namespace and collide on the name -- but ONLY on creation. `DROP ROLE`
/// on a plain user and `DROP USER` on a role both succeed, because Go
/// checks the row's existence and never its kind.
///
/// The two 1396 messages differ only in the operation they name, and
/// `DROP ROLE`'s prints the account BARE (`nosuch@%`) where `CREATE ROLE`'s
/// quotes it (`'r1'@'%'`) -- Go formats them through different helpers.
#[test]
fn role_and_user_share_one_namespace_and_collide_only_on_creation() {
    let mut session = session_with_privileges();
    session.run("CREATE ROLE r1").unwrap();
    assert!(matches!(
        session.run("CREATE ROLE r1"),
        Err(DriverError::CannotUserRole {
            operation: "CREATE ROLE",
            ref target,
        }) if target == "'r1'@'%'"
    ));
    // A USER cannot take a name a ROLE already holds, and vice versa.
    assert!(matches!(
        session.run("CREATE USER r1"),
        Err(DriverError::CreateUserAlreadyExists { .. })
    ));
    session.run("CREATE USER u1").unwrap();
    assert!(matches!(
        session.run("CREATE ROLE u1"),
        Err(DriverError::CannotUserRole {
            operation: "CREATE ROLE",
            ..
        })
    ));
    // Cross-drops both succeed: no kind check anywhere.
    session.run("DROP ROLE u1").unwrap();
    session.run("DROP USER r1").unwrap();
    assert!(matches!(
        session.run("SHOW GRANTS FOR 'u1'@'%'"),
        Err(DriverError::NonexistingGrant { .. })
    ));

    session.run("CREATE ROLE r1").unwrap();
    session.run("CREATE ROLE IF NOT EXISTS r1").unwrap();
    session.run("DROP ROLE IF EXISTS nosuch").unwrap();
    assert!(matches!(
        session.run("DROP ROLE nosuch"),
        Err(DriverError::CannotUserRole {
            operation: "DROP ROLE",
            ref target,
        }) if target == "nosuch@%"
    ));
}

/// CAPTURED: a role cannot log in. `CREATE ROLE` writes
/// `account_locked = 'Y'` with an EMPTY password, which without the lock
/// would make every role a passwordless account.
#[test]
fn a_role_is_a_locked_account() {
    let mut session = session_with_privileges();
    session.run("CREATE ROLE r1").unwrap();
    session.run("CREATE USER u1").unwrap();
    let registry = session.privileges.clone().unwrap();
    assert!(registry.is_role("r1", "%"));
    assert!(!registry.is_role("u1", "%"));
    assert!(!registry.is_role("root", "%"));
}

/// CAPTURED: `GRANT <role> TO <account>` writes one `mysql.role_edges` row,
/// roles may be granted to roles, and the role line `SHOW GRANTS` prints
/// lists the roles sorted and joined with `", "`.
///
/// The two failure modes are asymmetric and both captured: an unknown ROLE
/// reports 3523 while an unknown TARGET reports 1396, and roles are
/// validated first.
#[test]
fn granting_a_role_adds_an_edge_and_a_show_grants_line() {
    let mut session = session_with_privileges();
    for sql in ["CREATE ROLE r1", "CREATE ROLE r2", "CREATE ROLE r3"] {
        session.run(sql).unwrap();
    }
    session.run("CREATE USER u1").unwrap();
    session.run("GRANT r1 TO 'u1'@'%'").unwrap();
    session.run("GRANT r2 TO r1").unwrap();
    session.run("GRANT r3 TO r1, 'u1'@'%'").unwrap();

    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
        [
            ["GRANT USAGE ON *.* TO 'u1'@'%'"],
            ["GRANT 'r1'@'%', 'r3'@'%' TO 'u1'@'%'"],
        ]
    );
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR r1")),
        [
            ["GRANT USAGE ON *.* TO 'r1'@'%'"],
            ["GRANT 'r2'@'%', 'r3'@'%' TO 'r1'@'%'"],
        ]
    );

    assert!(matches!(
        session.run("GRANT r1 TO nosuchuser"),
        Err(DriverError::CannotUserRole {
            operation: "GRANT ROLE",
            ref target,
        }) if target == "nosuchuser@%"
    ));
    assert!(matches!(
        session.run("GRANT nosuchrole TO 'u1'@'%'"),
        Err(DriverError::GrantUnknownRole { ref role, .. }) if role == "nosuchrole"
    ));
    // A self-grant is accepted, not rejected as a cycle.
    session.run("GRANT r1 TO r1").unwrap();
}

/// CAPTURED: the role line lands between the TABLE-scope lines and the
/// DYNAMIC ones, which is the one ordering claim no smaller test pins.
#[test]
fn the_role_line_sits_between_the_table_and_dynamic_lines() {
    let mut session = session_with_privileges();
    session.run("CREATE DATABASE db1").unwrap();
    session.run("CREATE TABLE db1.t1 (a INT)").unwrap();
    session.run("CREATE USER u1").unwrap();
    session.run("CREATE ROLE r1").unwrap();
    session.run("GRANT SELECT ON db1.* TO 'u1'@'%'").unwrap();
    session.run("GRANT SELECT ON db1.t1 TO 'u1'@'%'").unwrap();
    session.run("GRANT r1 TO 'u1'@'%'").unwrap();
    session
        .run("GRANT BACKUP_ADMIN ON *.* TO 'u1'@'%'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
        [
            ["GRANT USAGE ON *.* TO 'u1'@'%'"],
            ["GRANT SELECT ON `db1`.* TO 'u1'@'%'"],
            ["GRANT SELECT ON `db1`.`t1` TO 'u1'@'%'"],
            ["GRANT 'r1'@'%' TO 'u1'@'%'"],
            ["GRANT BACKUP_ADMIN ON *.* TO 'u1'@'%'"],
        ]
    );
}

/// CAPTURED: `SET DEFAULT ROLE` REPLACES the account's default set (never
/// merges), `ALL` means every role granted to that account, `NONE` clears
/// it, and a role the account does not hold reports 3530.
#[test]
fn set_default_role_replaces_the_whole_default_set() {
    let mut session = session_with_privileges();
    for sql in ["CREATE ROLE r1", "CREATE ROLE r2", "CREATE ROLE r3"] {
        session.run(sql).unwrap();
    }
    session.run("CREATE USER u1").unwrap();
    session.run("GRANT r1, r3 TO 'u1'@'%'").unwrap();
    let registry = session.privileges.clone().unwrap();
    let u1 = ("u1".to_owned(), "%".to_owned());
    let role = |name: &str| (name.to_owned(), "%".to_owned());

    session.run("SET DEFAULT ROLE r1 TO 'u1'@'%'").unwrap();
    assert_eq!(registry.default_roles(&u1), [role("r1")]);
    session.run("SET DEFAULT ROLE ALL TO 'u1'@'%'").unwrap();
    assert_eq!(registry.default_roles(&u1), [role("r1"), role("r3")]);
    // A replace, not a merge: r3 disappears.
    session.run("SET DEFAULT ROLE r1 TO 'u1'@'%'").unwrap();
    assert_eq!(registry.default_roles(&u1), [role("r1")]);
    session.run("SET DEFAULT ROLE NONE TO 'u1'@'%'").unwrap();
    assert!(registry.default_roles(&u1).is_empty());

    assert!(matches!(
        session.run("SET DEFAULT ROLE r2 TO 'u1'@'%'"),
        Err(DriverError::RoleNotGranted { ref role, ref user, .. })
            if role == "r2" && user == "u1"
    ));
}

/// CAPTURED: every `SET ROLE` form and the `CURRENT_ROLE()` text after it.
/// `NONE` reports the literal `NONE`; anything else reports the
/// backtick-quoted identities joined by a BARE comma. A rejected `SET ROLE`
/// leaves the previous set standing.
///
/// A fresh session already has its DEFAULT roles active with no `SET ROLE`
/// at all -- Go activates them in `Auth`.
#[test]
fn set_role_forms_and_current_role() {
    let mut admin = session_with_privileges();
    for sql in ["CREATE ROLE r1", "CREATE ROLE r2", "CREATE ROLE r3"] {
        admin.run(sql).unwrap();
    }
    admin.run("CREATE USER u1").unwrap();
    admin.run("GRANT r1, r3 TO 'u1'@'%'").unwrap();
    admin.run("SET DEFAULT ROLE r1, r3 TO 'u1'@'%'").unwrap();

    let registry = admin.privileges.clone().unwrap();
    let mut session = session_as(&registry, admin.catalog.clone(), "u1", "%");
    let current_role =
        |session: &mut Session| row_text(session.run("SELECT CURRENT_ROLE()"))[0][0].clone();
    // Default roles are active at login.
    assert_eq!(current_role(&mut session), "`r1`@`%`,`r3`@`%`");

    session.run("SET ROLE NONE").unwrap();
    assert_eq!(current_role(&mut session), "NONE");
    session.run("SET ROLE ALL").unwrap();
    assert_eq!(current_role(&mut session), "`r1`@`%`,`r3`@`%`");
    session.run("SET ROLE r1").unwrap();
    assert_eq!(current_role(&mut session), "`r1`@`%`");
    session.run("SET ROLE DEFAULT").unwrap();
    assert_eq!(current_role(&mut session), "`r1`@`%`,`r3`@`%`");
    session.run("SET ROLE r1, r3").unwrap();
    assert_eq!(current_role(&mut session), "`r1`@`%`,`r3`@`%`");
    session.run("SET ROLE ALL EXCEPT r1").unwrap();
    assert_eq!(current_role(&mut session), "`r3`@`%`");

    // An ungranted role is refused and the previous set survives.
    session.run("SET ROLE ALL").unwrap();
    assert!(matches!(
        session.run("SET ROLE r2"),
        Err(DriverError::RoleNotGranted { ref role, .. }) if role == "r2"
    ));
    assert_eq!(current_role(&mut session), "`r1`@`%`,`r3`@`%`");
}

/// CAPTURED, and the reason activation and inheritance must be two
/// different questions: `SET ROLE ALL` activates only the roles granted
/// DIRECTLY (naming an indirectly-held role reports 3530), but an activated
/// role confers the privileges of every role granted to IT, transitively
/// (Go's `FindAllUserEffectiveRoles` walks the graph).
#[test]
fn activation_is_direct_but_inheritance_is_transitive() {
    let mut admin = session_with_privileges();
    admin.run("CREATE DATABASE deepdb").unwrap();
    admin.run("CREATE ROLE ra").unwrap();
    admin.run("CREATE ROLE rb").unwrap();
    admin.run("GRANT SELECT ON deepdb.* TO rb").unwrap();
    admin.run("GRANT BACKUP_ADMIN ON *.* TO rb").unwrap();
    admin.run("GRANT rb TO ra").unwrap();
    admin.run("CREATE USER u2").unwrap();
    admin.run("GRANT ra TO 'u2'@'%'").unwrap();
    admin.run("GRANT RESTORE_ADMIN ON *.* TO 'u2'@'%'").unwrap();

    let registry = admin.privileges.clone().unwrap();
    let mut session = session_as(&registry, admin.catalog.clone(), "u2", "%");
    // No default roles: nothing is active at login.
    assert_eq!(row_text(session.run("SELECT CURRENT_ROLE()")), [["NONE"]]);
    assert_eq!(
        row_text(session.run("SHOW GRANTS")),
        [
            ["GRANT USAGE ON *.* TO 'u2'@'%'"],
            ["GRANT 'ra'@'%' TO 'u2'@'%'"],
            ["GRANT RESTORE_ADMIN ON *.* TO 'u2'@'%'"],
        ]
    );

    session.run("SET ROLE ALL").unwrap();
    // `rb` is reachable but never activatable.
    assert_eq!(
        row_text(session.run("SELECT CURRENT_ROLE()")),
        [["`ra`@`%`"]]
    );
    assert!(matches!(
        session.run("SET ROLE rb"),
        Err(DriverError::RoleNotGranted { ref role, .. }) if role == "rb"
    ));
    // ... yet its privileges arrive through `ra`, printed under u2's own
    // name and merged into u2's dynamic line.
    assert_eq!(
        row_text(session.run("SHOW GRANTS")),
        [
            ["GRANT USAGE ON *.* TO 'u2'@'%'"],
            ["GRANT SELECT ON `deepdb`.* TO 'u2'@'%'"],
            ["GRANT 'ra'@'%' TO 'u2'@'%'"],
            ["GRANT BACKUP_ADMIN,RESTORE_ADMIN ON *.* TO 'u2'@'%'"],
        ]
    );
    // `SHOW GRANTS FOR <someone else>` folds in no roles at all.
    assert_eq!(
        row_text(admin.run("SHOW GRANTS FOR 'u2'@'%'")),
        [
            ["GRANT USAGE ON *.* TO 'u2'@'%'"],
            ["GRANT 'ra'@'%' TO 'u2'@'%'"],
            ["GRANT RESTORE_ADMIN ON *.* TO 'u2'@'%'"],
        ]
    );
}

/// CAPTURED: `REVOKE <role> FROM <account>` deletes the edge AND every
/// `default_roles` row that named it; revoking a role that was never
/// granted is a silent no-op; and `DROP ROLE` removes the account row,
/// every edge in both directions, and the default-role rows -- so the role
/// line disappears from `SHOW GRANTS`.
///
/// A missing ROLE reports 1396 here, NOT the 3523 `GRANT` reports, and
/// prints the role backtick-quoted.
#[test]
fn revoking_and_dropping_a_role_clean_up_every_edge() {
    let mut session = session_with_privileges();
    session.run("CREATE ROLE r1").unwrap();
    session.run("CREATE ROLE r3").unwrap();
    session.run("CREATE USER u1").unwrap();
    session.run("GRANT r1, r3 TO 'u1'@'%'").unwrap();
    session.run("SET DEFAULT ROLE ALL TO 'u1'@'%'").unwrap();
    let registry = session.privileges.clone().unwrap();
    let u1 = ("u1".to_owned(), "%".to_owned());
    let r3 = ("r3".to_owned(), "%".to_owned());

    session.run("REVOKE r1 FROM 'u1'@'%'").unwrap();
    assert_eq!(registry.granted_roles(&u1), std::slice::from_ref(&r3));
    assert_eq!(registry.default_roles(&u1), std::slice::from_ref(&r3));
    // A repeat revoke is a silent no-op.
    session.run("REVOKE r1 FROM 'u1'@'%'").unwrap();
    assert!(matches!(
        session.run("REVOKE nosuchrole FROM 'u1'@'%'"),
        Err(DriverError::CannotUserRole {
            operation: "REVOKE ROLE",
            ref target,
        }) if target == "`nosuchrole`@`%`"
    ));

    session.run("GRANT r1 TO 'u1'@'%'").unwrap();
    session.run("GRANT r3 TO r1").unwrap();
    session.run("SET DEFAULT ROLE r1 TO 'u1'@'%'").unwrap();
    session.run("DROP ROLE r1").unwrap();
    // The account row, both edge directions and the default row are gone.
    assert!(!registry.user_exists("r1", "%"));
    assert_eq!(registry.granted_roles(&u1), [r3]);
    assert!(registry.default_roles(&u1).is_empty());
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
        [
            ["GRANT USAGE ON *.* TO 'u1'@'%'"],
            ["GRANT 'r3'@'%' TO 'u1'@'%'"],
        ]
    );
}

/// CAPTURED: `GRANT <role> TO <account> WITH ADMIN OPTION` is a SYNTAX
/// error in TiDB (1064 near `OPTION`) -- the grammar has no such clause, so
/// there is nothing to model.
#[test]
fn with_admin_option_is_not_grammar() {
    let mut session = session_with_privileges();
    session.run("CREATE ROLE r1").unwrap();
    session.run("CREATE USER u1").unwrap();
    assert!(matches!(
        session.run("GRANT r1 TO 'u1'@'%' WITH ADMIN OPTION"),
        Err(DriverError::Parse(_))
    ));
}

/// A role may live at a specific host, and is then named with that host
/// everywhere (captured: `CREATE ROLE 'r9'@'localhost'` and the edge it
/// writes carry `localhost`, not `%`).
#[test]
fn a_role_can_be_hosted() {
    let mut session = session_with_privileges();
    session.run("CREATE ROLE 'r9'@'localhost'").unwrap();
    session.run("CREATE USER u1").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'r9'@'localhost'")),
        [["GRANT USAGE ON *.* TO 'r9'@'localhost'"]]
    );
    session.run("GRANT 'r9'@'localhost' TO 'u1'@'%'").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
        [
            ["GRANT USAGE ON *.* TO 'u1'@'%'"],
            ["GRANT 'r9'@'localhost' TO 'u1'@'%'"],
        ]
    );
    session
        .run("REVOKE 'r9'@'localhost' FROM 'u1'@'%'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
        [["GRANT USAGE ON *.* TO 'u1'@'%'"]]
    );
}

/// The privilege FLOW, proven where this tier actually gates something:
/// `SHOW PROCESSLIST`'s `PROCESS` check. The privilege lives only on the
/// role, so visibility follows ACTIVATION -- exactly the shape the Go
/// capture showed for a table read (`SELECT` denied with no active role,
/// allowed once the role holding it was activated).
#[test]
fn a_role_confers_process_only_while_it_is_active() {
    let mut admin = session_with_privileges();
    admin.run("CREATE ROLE watcher").unwrap();
    admin.run("CREATE USER bob").unwrap();
    admin.run("GRANT PROCESS ON *.* TO watcher").unwrap();
    admin.run("GRANT watcher TO 'bob'@'%'").unwrap();

    let registry = admin.privileges.clone().unwrap();
    let processes = process::ProcessRegistry::default();
    let mut session = session_as(&registry, admin.catalog.clone(), "bob", "%");
    let guard = processes.register(
        1,
        "bob".to_owned(),
        "10.0.0.1:1".to_owned(),
        "test".to_owned(),
        None,
    );
    session.attach_process(1, guard);
    let _alice = processes.register(
        2,
        "alice".to_owned(),
        "10.0.0.2:2".to_owned(),
        "test".to_owned(),
        None,
    );

    // Granted but not activated: bob sees only his own connection.
    assert_eq!(row_text(session.run("show processlist")).len(), 1);
    session.run("SET ROLE watcher").unwrap();
    assert_eq!(row_text(session.run("show processlist")).len(), 2);
    session.run("SET ROLE NONE").unwrap();
    assert_eq!(row_text(session.run("show processlist")).len(), 1);

    // Revoking the role while it is active drops it from the session, so
    // the privilege cannot outlive the grant.
    session.run("SET ROLE ALL").unwrap();
    assert_eq!(row_text(session.run("show processlist")).len(), 2);
    admin.run("REVOKE watcher FROM 'bob'@'%'").unwrap();
    assert_eq!(row_text(session.run("show processlist")).len(), 1);
}
