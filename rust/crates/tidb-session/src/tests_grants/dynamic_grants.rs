//! Dynamic privileges: the `mysql.global_grants` rows, their own
//! `WITH GRANT OPTION` flag, `SUPER` as the fallback, and how
//! `information_schema.user_privileges` reports them.

use crate::tests_support::*;
use crate::*;

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

/// Go's raw cache does not turn a bare `mysql.GrantPriv` bit into a
/// USER_PRIVILEGES row. `USAGE` is emitted only for an account with no
/// non-dynamic privilege bits at all, and is never grantable; once a named
/// privilege exists, GRANT OPTION only changes that named row's flag.
#[test]
fn user_privileges_does_not_render_grant_option_as_usage() {
    let mut session = session_with_privileges();
    session.set_user("root@%".to_owned(), "root@127.0.0.1".to_owned());
    session.run("CREATE USER 'grant_only'@'%'").unwrap();
    session
        .run("GRANT GRANT OPTION ON *.* TO 'grant_only'@'%'")
        .unwrap();

    let rows = row_text(session.run(
        "SELECT privilege_type, is_grantable \
         FROM information_schema.USER_PRIVILEGES \
         WHERE grantee = '''grant_only''@''%'''",
    ));
    assert!(
        rows.is_empty(),
        "a lone GRANT OPTION is not a row: {rows:?}"
    );

    session
        .run("GRANT SELECT ON *.* TO 'grant_only'@'%'")
        .unwrap();
    assert_eq!(
        row_text(session.run(
            "SELECT privilege_type, is_grantable \
             FROM information_schema.USER_PRIVILEGES \
             WHERE grantee = '''grant_only''@''%'''",
        )),
        [["SELECT", "YES"]]
    );
}

#[test]
fn user_privileges_uses_raw_host_patterns_and_mysql_schema_select() {
    let mut admin = session_with_privileges();
    for sql in [
        "CREATE USER 'alice'@'%'",
        "CREATE USER 'alice'@'localhost'",
        "CREATE USER 'bob'@'%'",
    ] {
        admin.run(sql).unwrap();
    }
    let registry = admin.privileges.clone().unwrap();
    let mut alice = Session::new();
    // The login host is the address the raw USER_PRIVILEGES cache matches;
    // CURRENT_USER holds the most-specific mysql.user row selected by auth.
    alice.set_user("alice@localhost".to_owned(), "alice@127.0.0.1".to_owned());
    alice.attach_privileges(registry.clone());

    let own_rows = row_text(
        alice.run("SELECT grantee FROM information_schema.USER_PRIVILEGES ORDER BY grantee"),
    );
    assert_eq!(
        own_rows,
        [["'alice'@'%'"], ["'alice'@'localhost'"]],
        "the raw cache exposes every matching host-pattern row, not merely the canonical one"
    );

    admin.run("GRANT SELECT ON `my%`.* TO 'alice'@'%'").unwrap();
    let all_rows = row_text(
        alice.run("SELECT grantee FROM information_schema.USER_PRIVILEGES ORDER BY grantee"),
    );
    assert!(
        all_rows.iter().any(|row| row[0] == "'bob'@'%'"),
        "SELECT on mysql.* exposes every account row: {all_rows:?}"
    );
    assert!(
        all_rows.iter().any(|row| row[0] == "'root'@'%'"),
        "a wildcard mysql.db row independently matches alice@% even though mysql.user selected localhost: {all_rows:?}"
    );

    admin.run("CREATE ROLE 'reader'@'%'").unwrap();
    admin
        .run("GRANT SELECT ON `my%`.* TO 'reader'@'%'")
        .unwrap();
    admin
        .run("GRANT 'reader'@'%' TO 'alice'@'localhost'")
        .unwrap();
    alice.run("SET ROLE 'reader'@'%'").unwrap();
    // Remove alice's own schema privilege so only the active role can make
    // the raw USER_PRIVILEGES table show every account.
    admin
        .run("REVOKE SELECT ON `my%`.* FROM 'alice'@'%'")
        .unwrap();
    let role_rows = row_text(
        alice.run("SELECT grantee FROM information_schema.USER_PRIVILEGES ORDER BY grantee"),
    );
    assert!(
        role_rows.iter().any(|row| row[0] == "'bob'@'%'"),
        "an active role's wildcard mysql.db row uses the same raw matching semantics: {role_rows:?}"
    );
}
