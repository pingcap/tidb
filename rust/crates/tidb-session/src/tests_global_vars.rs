#![cfg(test)]

//! `SET GLOBAL` / `SELECT @@global.x` / `SHOW GLOBAL VARIABLES`: the
//! GLOBAL-scope sysvar tier over [`vars::GlobalSysvars`]. See that module's
//! doc comment for the inheritance rule these tests capture: a session's
//! copy is made from the shared table once, at connect.

use crate::tests_support::*;
use crate::*;

/// Two sessions sharing one [`vars::GlobalSysvars`] table, standing in for
/// two connections through the same [`crate::PipelineSessionFactory`
/// (`tidb-server`)]. Root is bootstrapped with every privilege on both, so a
/// `SET GLOBAL` is not itself blocked by the privilege gate under test
/// elsewhere in this file.
fn two_sessions_sharing_globals() -> (Session, Session, vars::GlobalSysvars) {
    let globals = vars::GlobalSysvars::new();
    let registry = privilege::PrivilegeRegistry::default();
    let catalog: SharedCatalog = std::sync::Arc::new(std::sync::Mutex::new(Catalog::default()));

    let mut first = Session::with_catalog(catalog.clone());
    first.set_user("root@%".to_owned(), "root@%".to_owned());
    first.attach_privileges(registry.clone());
    first.attach_globals(globals.clone());

    let mut second = Session::with_catalog(catalog);
    second.set_user("root@%".to_owned(), "root@%".to_owned());
    second.attach_privileges(registry);
    second.attach_globals(globals.clone());

    (first, second, globals)
}

/// The MySQL inheritance rule, captured end to end through `SET`/`SELECT`
/// rather than the unit-level `vars` module: `SET GLOBAL` on one session is
/// visible to a peer's `@@global.x` immediately, but the peer's own plain
/// `@@x` (its session copy, made at connect) does not move -- and a THIRD
/// session opened after the `SET GLOBAL` inherits the new value into ITS
/// session copy.
#[test]
fn set_global_is_visible_to_a_peer_only_through_the_global_form() {
    let (mut first, mut second, globals) = two_sessions_sharing_globals();

    assert_eq!(
        second.run("SELECT @@autocommit").unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string("ON")]])
    );

    first.run("SET GLOBAL autocommit = OFF").unwrap();

    // The peer's own session copy is untouched...
    assert_eq!(
        second.run("SELECT @@autocommit").unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string("ON")]])
    );
    // ...but the peer's @@global read sees it immediately.
    assert_eq!(
        second.run("SELECT @@global.autocommit").unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string("OFF")]])
    );

    // A brand new session opened AFTER the SET GLOBAL inherits it as its own
    // session default -- the same snapshot-at-connect step
    // `PipelineSessionFactory::open_session` performs via `attach_globals`.
    let mut fresh = Session::new();
    fresh.attach_globals(globals);
    assert_eq!(
        fresh.run("SELECT @@autocommit").unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string("OFF")]])
    );
}

/// `SHOW GLOBAL VARIABLES` reads the shared table live; `SHOW SESSION
/// VARIABLES` (and the unqualified default) reads the session's own copy --
/// so the two diverge after a session-only `SET` exactly as they do after a
/// `SET GLOBAL`.
#[test]
fn show_global_and_session_variables_diverge() {
    let mut session = Session::new();
    session.attach_privileges(privilege::PrivilegeRegistry::default());
    session.attach_globals(vars::GlobalSysvars::new());

    session.run("SET autocommit = OFF").unwrap();
    assert_eq!(
        row_text(session.run("SHOW VARIABLES LIKE 'autocommit'")),
        [["autocommit", "OFF"]]
    );
    // The session-only SET never touched the shared table.
    assert_eq!(
        row_text(session.run("SHOW GLOBAL VARIABLES LIKE 'autocommit'")),
        [["autocommit", "ON"]]
    );

    session.run("SET GLOBAL autocommit = OFF").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GLOBAL VARIABLES LIKE 'autocommit'")),
        [["autocommit", "OFF"]]
    );
}

/// Go's `ErrLocalVariable` (1228): `SET GLOBAL` on a SESSION-only variable.
#[test]
fn set_global_on_a_session_only_variable_is_rejected() {
    let mut session = session_with_privileges();
    session.attach_globals(vars::GlobalSysvars::new());
    let error = session.run("SET GLOBAL debug_sync = 'x'").unwrap_err();
    let mysql = error.to_mysql_error();
    assert_eq!(mysql.code, 1228, "{mysql:?}");
}

/// Go's `ErrGlobalVariable` (1229): `SET SESSION` (the unqualified form,
/// here) on a GLOBAL-only variable.
#[test]
fn set_session_on_a_global_only_variable_is_rejected() {
    let mut session = session_with_privileges();
    session.attach_globals(vars::GlobalSysvars::new());
    let error = session
        .run("SET default_password_lifetime = 5")
        .unwrap_err();
    let mysql = error.to_mysql_error();
    assert_eq!(mysql.code, 1229, "{mysql:?}");
}

/// Go's `ErrIncorrectGlobalLocalVar` (1238), read side: `SELECT
/// @@global.x` on a SESSION-only variable has no GLOBAL copy to read.
#[test]
fn reading_at_global_scope_on_a_session_only_variable_is_rejected() {
    let mut session = session_with_privileges();
    session.attach_globals(vars::GlobalSysvars::new());
    let error = session.run("SELECT @@global.debug_sync").unwrap_err();
    let mysql = error.to_mysql_error();
    assert_eq!(mysql.code, 1238, "{mysql:?}");
}

/// Go's `ErrSpecificAccessDenied` (1227): `SET GLOBAL` needs SUPER or the
/// dynamic `SYSTEM_VARIABLES_ADMIN` privilege. A freshly created account
/// with neither is refused; granting `SYSTEM_VARIABLES_ADMIN` admits it.
#[test]
fn set_global_requires_super_or_system_variables_admin() {
    let registry = privilege::PrivilegeRegistry::default();
    let catalog: SharedCatalog = std::sync::Arc::new(std::sync::Mutex::new(Catalog::default()));

    let mut root = session_as(&registry, catalog.clone(), "root", "%");
    root.run("CREATE USER 'plain'@'%'").unwrap();

    let mut plain = session_as(&registry, catalog, "plain", "%");
    plain.attach_globals(vars::GlobalSysvars::new());
    let error = plain.run("SET GLOBAL autocommit = OFF").unwrap_err();
    let mysql = error.to_mysql_error();
    assert_eq!(mysql.code, 1227, "{mysql:?}");

    root.run("GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'plain'@'%'")
        .unwrap();
    // Re-open the session so the newly granted dynamic privilege is what
    // this connection's identity resolves to (matches how every other
    // privilege check in this tier is exercised after a GRANT).
    let mut plain = session_as(&registry, root.shared_catalog(), "plain", "%");
    plain.attach_globals(vars::GlobalSysvars::new());
    plain.run("SET GLOBAL autocommit = OFF").unwrap();
}
