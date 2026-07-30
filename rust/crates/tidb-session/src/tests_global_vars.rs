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
/// session copy. (`autocommit` is `TypeBool`, so the reads report Go's
/// integer domain, `1`/`0`, while the stored form stays `ON`/`OFF`.)
#[test]
fn set_global_is_visible_to_a_peer_only_through_the_global_form() {
    let (mut first, mut second, globals) = two_sessions_sharing_globals();

    assert_eq!(
        second.run("SELECT @@autocommit").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );

    first.run("SET GLOBAL autocommit = OFF").unwrap();

    // The peer's own session copy is untouched...
    assert_eq!(
        second.run("SELECT @@autocommit").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );
    // ...but the peer's @@global read sees it immediately.
    assert_eq!(
        second.run("SELECT @@global.autocommit").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(0)]])
    );

    // A brand new session opened AFTER the SET GLOBAL inherits it as its own
    // session default -- the same snapshot-at-connect step
    // `PipelineSessionFactory::open_session` performs via `attach_globals`.
    let mut fresh = Session::new();
    fresh.attach_globals(globals);
    assert_eq!(
        fresh.run("SELECT @@autocommit").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(0)]])
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

/// `tidb_enable_table_partition` and `tidb_enable_list_partition` name a
/// feature that is now ALWAYS ON, and their `Validation` closures say so in
/// two different ways: the first rewrites any assignment to `ON` and warns
/// when someone tried to turn it off, the second refuses. Captured through
/// `gorun`, for both scopes:
///
/// ```text
/// set tidb_enable_table_partition=off;        show warnings;
///   Warning|1105|tidb_enable_table_partition is always turned on. ...
/// show variables like 'tidb_enable_table_partition';        -> ON
/// set global tidb_enable_table_partition=off;
/// show global variables like 'tidb_enable_table_partition'; -> ON
/// set tidb_enable_list_partition=on;          show warnings;
///   Warning|1681|tidb_enable_list_partition is deprecated and will be removed in a future release.
/// set tidb_enable_list_partition=off;
///   Error 1105 (HY000): tidb_enable_list_partition is now always on, and cannot be turned off
/// show variables like 'tidb_enable_list_partition';         -> ON
/// ```
#[test]
fn the_partition_switches_are_always_on() {
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();

    session
        .run("SET tidb_enable_table_partition = off")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        vec![vec![
            "Warning".to_owned(),
            "1105".to_owned(),
            "tidb_enable_table_partition is always turned on. This variable has been deprecated \
             and will be removed in the future releases"
                .to_owned(),
        ]]
    );
    assert_eq!(
        row_text(session.run("SHOW VARIABLES LIKE 'tidb_enable_table_partition'")),
        vec![vec![
            "tidb_enable_table_partition".to_owned(),
            "ON".to_owned()
        ]]
    );
    session
        .run("SET GLOBAL tidb_enable_table_partition = off")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GLOBAL VARIABLES LIKE 'tidb_enable_table_partition'")),
        vec![vec![
            "tidb_enable_table_partition".to_owned(),
            "ON".to_owned()
        ]]
    );

    session.run("SET tidb_enable_list_partition = on").unwrap();
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        vec![vec![
            "Warning".to_owned(),
            "1681".to_owned(),
            "tidb_enable_list_partition is deprecated and will be removed in a future release."
                .to_owned(),
        ]]
    );
    let refused = session
        .run("SET tidb_enable_list_partition = off")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(refused.code, 1105, "{refused:?}");
    assert_eq!(
        refused.message,
        "tidb_enable_list_partition is now always on, and cannot be turned off"
    );
    assert_eq!(
        row_text(session.run("SHOW VARIABLES LIKE 'tidb_enable_list_partition'")),
        vec![vec![
            "tidb_enable_list_partition".to_owned(),
            "ON".to_owned()
        ]]
    );
}

/// `tidb_session_alias` is cut to 64 RUNES and then stripped of trailing
/// spaces, because it labels log lines as an identifier. Captured through
/// `gorun`: `set @@tidb_session_alias='abc  '` reads back as `abc`.
#[test]
fn a_session_alias_is_cut_to_64_runes_and_trimmed() {
    let mut session = Session::new();

    let long = "0123456789".repeat(7);
    session
        .run(&format!("SET @@tidb_session_alias = '{long}'"))
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT @@tidb_session_alias")),
        vec![vec![long[..64].to_owned()]]
    );

    // Runes, not bytes: 65 three-byte characters lose exactly the last one.
    let chinese = "中文测试1中文测试2中文测试3中文测试4中文测试5中文测试6中文测试7中文测试8中文测试9中文测试0中文测试a中文测试b中文测试c";
    session
        .run(&format!("SET @@tidb_session_alias = '{chinese}'"))
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT @@tidb_session_alias")),
        vec![vec![chinese.chars().take(64).collect::<String>()]]
    );

    session.run("SET @@tidb_session_alias = 'abc  '").unwrap();
    assert_eq!(
        row_text(session.run("SELECT @@tidb_session_alias")),
        vec![vec!["abc".to_owned()]]
    );

    // The 64-rune cut lands inside a run of spaces, and the identifier trim
    // then removes all of them.
    session
        .run("SET @@tidb_session_alias = 'abc                                                                    1'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT @@tidb_session_alias")),
        vec![vec!["abc".to_owned()]]
    );
}
