//! Roles: a role IS a locked `mysql.user` row, plus the `mysql.role_edges`
//! graph, `mysql.default_roles`, `SET ROLE`, and the transitive privilege
//! walk Go does in `FindAllUserEffectiveRoles`.

use crate::tests_support::*;
use crate::*;

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
