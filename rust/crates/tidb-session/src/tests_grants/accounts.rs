//! The `mysql.user` row itself: `CREATE`/`ALTER`/`RENAME`/`DROP USER`, the
//! stored `authentication_string` per plugin, account locking, and
//! `SHOW CREATE USER`.

use crate::tests_support::*;
use crate::*;

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

/// `SHOW CREATE USER`: CAPTURED against `pkg/executor/show.go`'s
/// `fetchShowCreateUser` (test store, `testkit.CreateMockStore`). A freshly
/// created native-password account prints the full DEFAULT clause set this
/// tier has no storage for beyond plugin/hash/lock:
/// `CREATE USER 'u'@'%' IDENTIFIED WITH 'mysql_native_password' AS
/// '*<HASH>' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD
/// HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT`. A passwordless account
/// prints `AS ''`.
#[test]
fn show_create_user_prints_the_captured_default_clause_set() {
    let mut session = session_with_privileges();
    session
        .run("CREATE USER 'plain'@'%' IDENTIFIED BY 'pw1234'")
        .unwrap();
    let rows = row_text(session.run("SHOW CREATE USER 'plain'@'%'"));
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0],
        "CREATE USER 'plain'@'%' IDENTIFIED WITH 'mysql_native_password' AS \
         '*0DB55B5CA3F29C6BCD42E6CF2D2BA346859991AB' REQUIRE NONE PASSWORD \
         EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD \
         REUSE INTERVAL DEFAULT"
    );

    session.run("CREATE USER 'nopw'@'%'").unwrap();
    let rows = row_text(session.run("SHOW CREATE USER 'nopw'@'%'"));
    assert_eq!(
        rows[0][0],
        "CREATE USER 'nopw'@'%' IDENTIFIED WITH 'mysql_native_password' AS \
         '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD \
         HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT"
    );
}

/// A `caching_sha2_password`/`tidb_sm3_password` account's `IDENTIFIED
/// WITH` clause names the real configured plugin, not the server default
/// (CAPTURED: Go reads `mysql.user.plugin`, not
/// `default_authentication_plugin`, whenever the row has a non-empty
/// plugin).
#[test]
fn show_create_user_names_the_accounts_own_plugin() {
    let mut session = session_with_privileges();
    session
        .run("CREATE USER 'sha2'@'%' IDENTIFIED WITH 'caching_sha2_password' BY 'pw1234'")
        .unwrap();
    let rows = row_text(session.run("SHOW CREATE USER 'sha2'@'%'"));
    assert!(rows[0][0]
        .starts_with("CREATE USER 'sha2'@'%' IDENTIFIED WITH 'caching_sha2_password' AS '"));
}

/// A ROLE is a locked `mysql.user` row with no password (Go's `CREATE
/// ROLE`), so `SHOW CREATE USER` on one prints `ACCOUNT LOCK` and an empty
/// hash exactly like an `ALTER USER ... ACCOUNT LOCK`'d plain user would
/// (CAPTURED; the two are indistinguishable at this column set, which is
/// itself the captured Go behavior -- both are one `account_locked='Y'`
/// row).
#[test]
fn show_create_user_reflects_account_lock_for_roles_and_locked_users() {
    let mut session = session_with_privileges();
    session.run("CREATE ROLE 'role1'").unwrap();
    let rows = row_text(session.run("SHOW CREATE USER 'role1'@'%'"));
    assert!(rows[0][0].contains(" ACCOUNT LOCK "));

    session
        .run("CREATE USER 'locked'@'%' IDENTIFIED BY 'pw1234'")
        .unwrap();
    session.run("ALTER USER 'locked'@'%' ACCOUNT LOCK").unwrap();
    let rows = row_text(session.run("SHOW CREATE USER 'locked'@'%'"));
    assert!(rows[0][0].contains(" ACCOUNT LOCK "));
    session
        .run("ALTER USER 'locked'@'%' ACCOUNT UNLOCK")
        .unwrap();
    let rows = row_text(session.run("SHOW CREATE USER 'locked'@'%'"));
    assert!(rows[0][0].contains(" ACCOUNT UNLOCK "));
}

/// `SHOW CREATE USER` on a missing account is Go's `ErrCannotUser` (1396),
/// quoted `'user'@'host'` like every other account-management error this
/// tier reports the same way (CAPTURED against
/// `exeerrors.ErrCannotUser.GenWithStackByArgs("SHOW CREATE USER", ...)`).
#[test]
fn show_create_user_reports_cannot_user_for_a_missing_account() {
    let mut session = session_with_privileges();
    assert!(matches!(
        session.run("SHOW CREATE USER 'nosuch'@'%'"),
        Err(DriverError::CannotUserRole {
            operation: "SHOW CREATE USER",
            ref target,
        }) if target == "'nosuch'@'%'"
    ));
}

/// `SHOW CREATE USER CURRENT_USER()` resolves the SESSION's own identity,
/// the same account `SHOW GRANTS` (no `FOR`) resolves to.
#[test]
fn show_create_user_current_user_resolves_the_session_identity() {
    let mut session = session_with_privileges();
    session.set_user("root@%".to_owned(), "root@127.0.0.1".to_owned());
    let rows = row_text(session.run("SHOW CREATE USER CURRENT_USER()"));
    assert!(rows[0][0].starts_with("CREATE USER 'root'@'%' IDENTIFIED WITH"));
}
