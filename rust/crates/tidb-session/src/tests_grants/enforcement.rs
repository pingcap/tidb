//! The privilege GATES on the account statements: who may run
//! `CREATE`/`DROP`/`ALTER`/`RENAME USER`, `SET PASSWORD`, `GRANT`/`REVOKE`,
//! `GRANT`/`REVOKE <role>`, `SET DEFAULT ROLE`, `SHOW GRANTS FOR` and
//! `SHOW CREATE USER FOR`.
//!
//! Go words each denial itself (`executor/simple.go`'s account executors and
//! `planner/core/planbuilder.go`'s `visitInfo`), so every test here pins the
//! ERRNO-bearing `DriverError` variant and, where the wording carries Go's
//! own capitalization quirk, the exact text.
//!
//! Every test also carries its POSITIVE control: the same statement run by
//! an account that does hold the privilege must still succeed, because a
//! gate that denies everything is not a port of Go's gate.

use crate::tests_support::*;
use crate::*;

/// The account table a real deployment provisions before anybody logs in,
/// plus an unrestricted bootstrap session on it.
fn provisioned() -> (privilege::PrivilegeRegistry, Session) {
    let privs = privilege::PrivilegeRegistry::default();
    let mut boot = bootstrap_session(&privs);
    boot.run("CREATE USER 'bob'@'%'").unwrap();
    boot.run("CREATE USER 'admin'@'%'").unwrap();
    boot.run("CREATE USER 'victim'@'%'").unwrap();
    boot.run("GRANT ALL PRIVILEGES ON *.* TO 'admin'@'%' WITH GRANT OPTION")
        .unwrap();
    (privs, boot)
}

/// The errno and message the client actually sees, which is what Go's
/// wording is pinned against.
fn denied(session: &mut Session, sql: &str) -> (u16, String) {
    match session.run(sql) {
        Err(error) => {
            let rendered = error.to_mysql_error();
            (rendered.code, rendered.message)
        }
        Ok(output) => panic!("{sql} must be denied, got {output:?}"),
    }
}

/// Go `ErrSpecificAccessDenied` (1227).
fn specific(privileges: &str) -> (u16, String) {
    (
        1227,
        format!(
            "Access denied; you need (at least one of) the {privileges} \
             privilege(s) for this operation"
        ),
    )
}

/// Go `ErrPrivilegeCheckFail` (8121), whose message begins lowercase.
fn check_fail(privilege: &str) -> (u16, String) {
    (8121, format!("privilege check for '{privilege}' fail"))
}

/// Go `ErrDBaccessDenied` (1044).
fn db_denied(user: &str, host: &str, database: &str) -> (u16, String) {
    (
        1044,
        format!("Access denied for user '{user}'@'{host}' to database '{database}'"),
    )
}

/// Go `executeCreateUser`/`executeDropUser` (`executor/simple.go` around
/// lines 1051 and 2519): without `INSERT`/`DELETE` on `mysql.user` and
/// without the global `CREATE USER` privilege, an account cannot create or
/// drop accounts -- including itself out of trouble or `root` out of
/// existence. The privilege text is Go's own argument verbatim, capitalized
/// inconsistently between the two statements.
#[test]
fn create_and_drop_user_need_create_user_authority() {
    let (privs, mut boot) = provisioned();
    let mut bob = authenticated_session(&privs, "bob", "%");

    assert_eq!(
        denied(&mut bob, "CREATE USER 'mallory'@'%' IDENTIFIED BY 'x'"),
        specific("CREATE User")
    );
    assert_eq!(
        denied(&mut bob, "CREATE ROLE 'r1'"),
        specific("CREATE ROLE or CREATE USER")
    );
    assert_eq!(
        denied(&mut bob, "DROP USER 'victim'@'%'"),
        specific("CREATE USER")
    );
    assert_eq!(
        denied(&mut bob, "DROP ROLE 'victim'@'%'"),
        specific("DROP ROLE or CREATE USER")
    );
    assert_eq!(
        denied(&mut bob, "RENAME USER 'victim'@'%' TO 'looted'@'%'"),
        specific("CREATE USER")
    );
    // Nothing was written on the way to any of those refusals.
    assert!(!privs.user_exists("mallory", "%"));
    assert!(privs.user_exists("victim", "%"));

    // POSITIVE CONTROL: an account holding the privilege still runs them.
    let mut admin = authenticated_session(&privs, "admin", "%");
    admin.run("CREATE USER 'mallory'@'%'").unwrap();
    admin.run("CREATE ROLE 'r1'").unwrap();
    admin
        .run("RENAME USER 'mallory'@'%' TO 'moved'@'%'")
        .unwrap();
    admin.run("DROP USER 'moved'@'%'").unwrap();
    assert!(!privs.user_exists("moved", "%"));

    // ... and so does the server's own unrestricted bootstrap session.
    boot.run("CREATE USER 'provisioned'@'%'").unwrap();
    assert!(privs.user_exists("provisioned", "%"));
}

/// Go's shared `SYSTEM_USER` guard (`executor/simple.go` around line 2563):
/// `CREATE USER` authority is not enough to drop an account that itself
/// holds `SYSTEM_USER` -- and because SUPER is the fallback for every
/// dynamic privilege, "holds SYSTEM_USER" includes every SUPER account,
/// which is why Go's message names both.
#[test]
fn dropping_a_system_user_needs_system_user_too() {
    let (privs, mut boot) = provisioned();
    boot.run("CREATE USER 'super'@'%'").unwrap();
    // SUPER is not a substitute for the STATIC `CREATE USER` privilege --
    // only for a DYNAMIC one -- so a caller who is to pass both gates needs
    // both grants.
    boot.run("GRANT SUPER, CREATE USER ON *.* TO 'super'@'%'")
        .unwrap();
    boot.run("CREATE USER 'creator'@'%'").unwrap();
    boot.run("GRANT CREATE USER ON *.* TO 'creator'@'%'")
        .unwrap();

    let mut creator = authenticated_session(&privs, "creator", "%");
    assert_eq!(
        denied(&mut creator, "DROP USER 'super'@'%'"),
        specific("SYSTEM_USER or SUPER")
    );
    assert!(privs.user_exists("super", "%"));

    // POSITIVE CONTROL: an ordinary account is still droppable, and a
    // caller who holds SYSTEM_USER (here through SUPER) may drop the
    // protected one.
    creator.run("DROP USER 'victim'@'%'").unwrap();
    let mut superuser = authenticated_session(&privs, "super", "%");
    superuser.run("DROP USER 'bob'@'%'").unwrap();
    assert!(!privs.user_exists("bob", "%"));
}

/// Go `collectVisitInfoFromGrantStmt`/`collectVisitInfoFromRevokeStmt`
/// (`planner/core/planbuilder.go`): to GRANT you must hold the privilege
/// you are granting AND the `GRANT OPTION` at that scope. Without them,
/// `GRANT ALL PRIVILEGES ON *.* TO 'self'@'%' WITH GRANT OPTION` -- the
/// one-statement takeover -- is refused.
#[test]
fn grant_and_revoke_need_the_privilege_itself_plus_grant_option() {
    let (privs, mut boot) = provisioned();
    let mut bob = authenticated_session(&privs, "bob", "%");

    // The takeover statement. `ALL PRIVILEGES` expands to Go's
    // `AllGlobalPrivs`, so the first missing one is `Select`.
    assert_eq!(
        denied(
            &mut bob,
            "GRANT ALL PRIVILEGES ON *.* TO 'bob'@'%' WITH GRANT OPTION"
        ),
        check_fail("Select")
    );
    assert_eq!(
        denied(&mut bob, "GRANT SELECT ON *.* TO 'bob'@'%'"),
        check_fail("Select")
    );
    assert_eq!(
        denied(&mut bob, "REVOKE SELECT ON *.* FROM 'admin'@'%'"),
        check_fail("Select")
    );
    // A dynamic privilege is verified as ITSELF, with its own grant option,
    // and `GRANT` names that missing grant option (1227) where `REVOKE`
    // falls to the generic 8121.
    assert_eq!(
        denied(&mut bob, "GRANT CONNECTION_ADMIN ON *.* TO 'bob'@'%'"),
        specific("GRANT OPTION")
    );
    assert_eq!(
        denied(&mut bob, "REVOKE CONNECTION_ADMIN ON *.* FROM 'admin'@'%'"),
        check_fail("[CONNECTION_ADMIN]")
    );
    assert!(!privs.has_global_priv("bob", "%", privilege::GlobalPriv::Select));

    // Holding the privilege WITHOUT the grant option is still not enough --
    // that is the second half of Go's rule, and the one that keeps a
    // read-only account from spreading its own SELECT.
    boot.run("GRANT SELECT ON *.* TO 'bob'@'%'").unwrap();
    assert_eq!(
        denied(&mut bob, "GRANT SELECT ON *.* TO 'victim'@'%'"),
        check_fail("Grant Option")
    );

    // POSITIVE CONTROL: with both, the same statement lands.
    let mut admin = authenticated_session(&privs, "admin", "%");
    admin.run("GRANT SELECT ON *.* TO 'victim'@'%'").unwrap();
    assert!(privs.has_global_priv("victim", "%", privilege::GlobalPriv::Select));
    admin.run("REVOKE SELECT ON *.* FROM 'victim'@'%'").unwrap();
    assert!(!privs.has_global_priv("victim", "%", privilege::GlobalPriv::Select));
}

/// Go `planbuilder.go`'s `*ast.GrantRoleStmt`/`*ast.RevokeRoleStmt` cases
/// (around lines 3775 and 3783): the DYNAMIC `ROLE_ADMIN`, whose SUPER
/// fallback is why the message names both.
#[test]
fn role_grants_need_role_admin() {
    let (privs, mut boot) = provisioned();
    boot.run("CREATE ROLE 'r1'").unwrap();
    let mut bob = authenticated_session(&privs, "bob", "%");

    assert_eq!(
        denied(&mut bob, "GRANT 'r1'@'%' TO 'bob'@'%'"),
        specific("SUPER or ROLE_ADMIN")
    );
    assert_eq!(
        denied(&mut bob, "REVOKE 'r1'@'%' FROM 'bob'@'%'"),
        specific("SUPER or ROLE_ADMIN")
    );
    assert!(!privs.has_role(
        &("bob".to_owned(), "%".to_owned()),
        &("r1".to_owned(), "%".to_owned())
    ));

    // POSITIVE CONTROL: `ROLE_ADMIN` alone opens both, with no static
    // privilege anywhere.
    boot.run("GRANT ROLE_ADMIN ON *.* TO 'bob'@'%'").unwrap();
    bob.run("GRANT 'r1'@'%' TO 'bob'@'%'").unwrap();
    assert!(privs.has_role(
        &("bob".to_owned(), "%".to_owned()),
        &("r1".to_owned(), "%".to_owned())
    ));
    bob.run("REVOKE 'r1'@'%' FROM 'bob'@'%'").unwrap();
}

/// Go `executeAlterUser` (`executor/simple.go` around line 1941) and
/// `executeSetPwd` (around line 2905): rewriting ANOTHER account's password
/// needs ALTER USER authority (`ALTER USER`) or SUPER (`SET PASSWORD`), while
/// rewriting one's OWN with a bare `IDENTIFIED BY` needs nothing -- the
/// self-service carve-out a sandboxed session depends on.
#[test]
fn changing_another_accounts_password_is_privileged_but_ones_own_is_not() {
    let (privs, mut boot) = provisioned();
    let mut bob = authenticated_session(&privs, "bob", "%");

    assert_eq!(
        denied(&mut bob, "ALTER USER 'victim'@'%' IDENTIFIED BY 'stolen'"),
        specific("CREATE USER")
    );
    assert_eq!(
        denied(&mut bob, "SET PASSWORD FOR 'victim'@'%' = 'stolen'"),
        db_denied("bob", "%", "mysql")
    );
    // A statement-level option makes even a SELF `ALTER USER` privileged,
    // which is Go's `alterUserHasPrivilegedOptions`.
    assert_eq!(
        denied(&mut bob, "ALTER USER 'bob'@'%' ACCOUNT UNLOCK"),
        specific("CREATE USER")
    );
    assert!(privs
        .auth_string("victim", "%")
        .is_some_and(|hash| hash.is_empty()));

    // POSITIVE CONTROL: bob rotates his OWN password, both ways.
    bob.run("ALTER USER 'bob'@'%' IDENTIFIED BY 'mine'")
        .unwrap();
    bob.run("SET PASSWORD = 'mine2'").unwrap();
    bob.run("SET PASSWORD FOR CURRENT_USER() = 'mine3'")
        .unwrap();
    assert_eq!(
        privs.auth_string("bob", "%"),
        Some(privilege::encode_password("mine3"))
    );

    // POSITIVE CONTROL: SUPER opens the cross-account form.
    boot.run("GRANT SUPER ON *.* TO 'bob'@'%'").unwrap();
    bob.run("SET PASSWORD FOR 'victim'@'%' = 'reset'").unwrap();
    assert_eq!(
        privs.auth_string("victim", "%"),
        Some(privilege::encode_password("reset"))
    );
}

/// Go `executor/show.go`'s `fetchShowCreateUser` (around line 1873) and
/// `fetchShowGrants` (around line 2018). The first renders
/// `IDENTIFIED WITH ... AS '<hash>'`, so it needs the `SELECT` on
/// `mysql.user` that would let the caller read the stored hash directly;
/// the second enumerates an account's privileges and needs `SELECT` on the
/// whole `mysql` schema.
#[test]
fn reading_another_accounts_credentials_and_grants_is_privileged() {
    let (privs, mut boot) = provisioned();
    boot.run("ALTER USER 'victim'@'%' IDENTIFIED BY 'secret'")
        .unwrap();
    let mut bob = authenticated_session(&privs, "bob", "%");

    assert_eq!(
        denied(&mut bob, "SHOW CREATE USER 'victim'@'%'"),
        (
            1142,
            "SELECT command denied to user 'bob'@'%' for table 'User'".to_owned(),
        )
    );
    assert_eq!(
        denied(&mut bob, "SHOW GRANTS FOR 'victim'@'%'"),
        db_denied("bob", "%", "mysql")
    );

    // POSITIVE CONTROL: both self forms stay open -- an account may always
    // read its own -- and an account with the privilege reads anyone's.
    assert!(bob.run("SHOW CREATE USER 'bob'@'%'").is_ok());
    assert!(bob.run("SHOW CREATE USER CURRENT_USER()").is_ok());
    assert!(bob.run("SHOW GRANTS").is_ok());
    assert!(bob.run("SHOW GRANTS FOR CURRENT_USER()").is_ok());

    let mut admin = authenticated_session(&privs, "admin", "%");
    let shown = row_text(admin.run("SHOW CREATE USER 'victim'@'%'"));
    assert!(
        shown[0][0].contains(&privilege::encode_password("secret")),
        "{shown:?}"
    );
}

/// Go `executeSetDefaultRole` (`executor/simple.go` around line 445):
/// setting one's OWN default roles is unprivileged; setting anyone else's
/// needs `UPDATE` on `mysql.default_roles`, else the global `CREATE USER`
/// privilege.
#[test]
fn set_default_role_for_another_account_needs_create_user() {
    let (privs, mut boot) = provisioned();
    boot.run("CREATE ROLE 'r1'").unwrap();
    boot.run("GRANT ROLE_ADMIN ON *.* TO 'bob'@'%'").unwrap();
    let mut bob = authenticated_session(&privs, "bob", "%");
    bob.run("GRANT 'r1'@'%' TO 'bob'@'%'").unwrap();
    bob.run("GRANT 'r1'@'%' TO 'victim'@'%'").unwrap();

    assert_eq!(
        denied(&mut bob, "SET DEFAULT ROLE ALL TO 'victim'@'%'"),
        specific("CREATE USER")
    );
    assert!(privs
        .default_roles(&("victim".to_owned(), "%".to_owned()))
        .is_empty());

    // POSITIVE CONTROL: bob's own default roles, and an admin's reach over
    // everyone's.
    bob.run("SET DEFAULT ROLE ALL TO 'bob'@'%'").unwrap();
    assert!(!privs
        .default_roles(&("bob".to_owned(), "%".to_owned()))
        .is_empty());
    let mut admin = authenticated_session(&privs, "admin", "%");
    admin.run("SET DEFAULT ROLE ALL TO 'victim'@'%'").unwrap();
    assert!(!privs
        .default_roles(&("victim".to_owned(), "%".to_owned()))
        .is_empty());
}
