//! Password locking and expiry: the `User_attributes` JSON Go stores, the
//! clauses `SHOW CREATE USER` prints back, and the sandboxed session an
//! expired password produces.

use crate::tests_support::*;
use crate::*;

/// `FAILED_LOGIN_ATTEMPTS` / `PASSWORD_LOCK_TIME` storage, exactly as Go's
/// `mysql.user.user_attributes -> '$.Password_locking'` holds it.
///
/// Every expectation is one row of Go's own `TestFailedLoginTrackingBasic`
/// (`pkg/executor/test/passwordtest/password_management_test.go`), read back
/// through this tier's `password_locking` accessor rather than through JSON.
#[test]
fn password_locking_options_store_gos_values_and_merge_on_alter() {
    let mut session = session_with_privileges();
    let registry = session.privileges.clone().unwrap();
    let locking = |user: &str| registry.password_locking(user, "localhost");

    for (sql, user, attempts, lock_days) in [
        (
            "CREATE USER 'u1'@'localhost' FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 3",
            "u1",
            3,
            3,
        ),
        (
            "CREATE USER 'u2'@'localhost' FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME UNBOUNDED",
            "u2",
            3,
            -1,
        ),
        (
            "CREATE USER 'u3'@'localhost' FAILED_LOGIN_ATTEMPTS 3",
            "u3",
            3,
            0,
        ),
        (
            "CREATE USER 'u4'@'localhost' PASSWORD_LOCK_TIME 3",
            "u4",
            0,
            3,
        ),
        (
            "CREATE USER 'u5'@'localhost' PASSWORD_LOCK_TIME UNBOUNDED",
            "u5",
            0,
            -1,
        ),
    ] {
        session.run(sql).unwrap();
        let stored = locking(user).unwrap_or_else(|| panic!("{sql}"));
        assert_eq!(stored.failed_login_attempts, attempts, "{sql}");
        assert_eq!(stored.password_lock_time_days, lock_days, "{sql}");
        assert_eq!(stored.failed_login_count, 0, "{sql}");
    }

    // ALTER merges over the CURRENT values: an option the statement did not
    // write keeps what the account already had (Go's
    // `readPasswordLockingInfo` + `alterUserFailedLoginJSON`).
    for (sql, user, attempts, lock_days) in [
        (
            "ALTER USER 'u3'@'localhost' PASSWORD_LOCK_TIME 6",
            "u3",
            3,
            6,
        ),
        (
            "ALTER USER 'u4'@'localhost' FAILED_LOGIN_ATTEMPTS 4",
            "u4",
            4,
            3,
        ),
        (
            "ALTER USER 'u5'@'localhost' ACCOUNT UNLOCK FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 6",
            "u5",
            3,
            6,
        ),
    ] {
        session.run(sql).unwrap();
        let stored = locking(user).unwrap_or_else(|| panic!("{sql}"));
        assert_eq!(stored.failed_login_attempts, attempts, "{sql}");
        assert_eq!(stored.password_lock_time_days, lock_days, "{sql}");
    }

    // Captured: zeroing BOTH options drops the whole `Password_locking`
    // object (`user_attributes` reads NULL), and writing one of them back
    // brings the object back with the other at 0.
    session
        .run("ALTER USER 'u4'@'localhost' PASSWORD_LOCK_TIME 0 FAILED_LOGIN_ATTEMPTS 0")
        .unwrap();
    assert_eq!(locking("u4"), None);
    session
        .run("ALTER USER 'u4'@'localhost' ACCOUNT UNLOCK")
        .unwrap();
    assert_eq!(locking("u4"), None);
    session
        .run("ALTER USER 'u4'@'localhost' PASSWORD_LOCK_TIME 6")
        .unwrap();
    let stored = locking("u4").expect("Password_locking is back");
    assert_eq!(stored.failed_login_attempts, 0);
    assert_eq!(stored.password_lock_time_days, 6);
}

/// `SHOW CREATE USER`'s two new clause families, byte for byte against Go's
/// captured lines.
#[test]
fn show_create_user_prints_the_lockout_and_expiry_clauses() {
    let mut session = session_with_privileges();
    let show = |session: &mut Session, user: &str| {
        row_text(session.run(&format!("SHOW CREATE USER {user}")))[0][0].clone()
    };
    let head = "IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE";
    let tail = "PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT";

    for (sql, user, line) in [
        (
            "CREATE USER e1 PASSWORD EXPIRE",
            "e1",
            format!("CREATE USER 'e1'@'%' {head} PASSWORD EXPIRE ACCOUNT UNLOCK {tail}"),
        ),
        (
            "CREATE USER e2 PASSWORD EXPIRE DEFAULT",
            "e2",
            format!("CREATE USER 'e2'@'%' {head} PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK {tail}"),
        ),
        (
            "CREATE USER e3 PASSWORD EXPIRE NEVER",
            "e3",
            format!("CREATE USER 'e3'@'%' {head} PASSWORD EXPIRE NEVER ACCOUNT UNLOCK {tail}"),
        ),
        (
            "CREATE USER e4 PASSWORD EXPIRE INTERVAL 7 DAY",
            "e4",
            format!(
                "CREATE USER 'e4'@'%' {head} PASSWORD EXPIRE INTERVAL 7 DAY ACCOUNT UNLOCK {tail}"
            ),
        ),
        (
            "CREATE USER e5",
            "e5",
            format!("CREATE USER 'e5'@'%' {head} PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK {tail}"),
        ),
        (
            "CREATE USER e6 PASSWORD EXPIRE INTERVAL 5 DAY FAILED_LOGIN_ATTEMPTS 2 PASSWORD_LOCK_TIME 3",
            "e6",
            format!(
                "CREATE USER 'e6'@'%' {head} PASSWORD EXPIRE INTERVAL 5 DAY ACCOUNT UNLOCK {tail} \
                 FAILED_LOGIN_ATTEMPTS 2 PASSWORD_LOCK_TIME 3"
            ),
        ),
        (
            "CREATE USER e7 FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME UNBOUNDED",
            "e7",
            format!(
                "CREATE USER 'e7'@'%' {head} PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK {tail} \
                 FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME UNBOUNDED"
            ),
        ),
    ] {
        session.run(sql).unwrap();
        assert_eq!(show(&mut session, user), line, "{sql}");
    }

    // ALTER walks e5 through the whole family, and storing a password clears
    // the expired flag (all captured).
    for (sql, line) in [
        (
            "ALTER USER e5 PASSWORD EXPIRE INTERVAL 3 DAY",
            format!(
                "CREATE USER 'e5'@'%' {head} PASSWORD EXPIRE INTERVAL 3 DAY ACCOUNT UNLOCK {tail}"
            ),
        ),
        (
            "ALTER USER e5 PASSWORD EXPIRE NEVER",
            format!("CREATE USER 'e5'@'%' {head} PASSWORD EXPIRE NEVER ACCOUNT UNLOCK {tail}"),
        ),
        (
            "ALTER USER e5 PASSWORD EXPIRE DEFAULT",
            format!("CREATE USER 'e5'@'%' {head} PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK {tail}"),
        ),
        (
            "ALTER USER e5 PASSWORD EXPIRE",
            format!("CREATE USER 'e5'@'%' {head} PASSWORD EXPIRE ACCOUNT UNLOCK {tail}"),
        ),
    ] {
        session.run(sql).unwrap();
        assert_eq!(show(&mut session, "e5"), line, "{sql}");
    }
    session.run("ALTER USER e5 IDENTIFIED BY 'pw2'").unwrap();
    let line = show(&mut session, "e5");
    assert!(
        line.contains("PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK"),
        "storing a password unexpires it: {line}"
    );

    // A ROLE is Go's `Password_expired='Y'` row, so it prints the bare
    // clause -- the divergence this tier used to carry.
    session.run("CREATE ROLE r1").unwrap();
    assert_eq!(
        show(&mut session, "r1"),
        format!("CREATE USER 'r1'@'%' {head} PASSWORD EXPIRE ACCOUNT LOCK {tail}")
    );
}

/// Go rejects `PASSWORD EXPIRE INTERVAL n DAY` outside `1 ..= 65535` with
/// `ErrWrongValue2("DAY", n)` (1525) before writing any row.
#[test]
fn password_expire_interval_out_of_range_is_rejected_and_creates_nothing() {
    let mut session = session_with_privileges();
    assert!(matches!(
        session.run("CREATE USER bad PASSWORD EXPIRE INTERVAL 0 DAY"),
        Err(DriverError::PasswordExpireIntervalOutOfRange { days: 0 })
    ));
    assert!(!session.privileges.clone().unwrap().user_exists("bad", "%"));
}

/// Go's `TiDBContext.checkSandBoxMode`: a session admitted with an expired
/// password may run `SET PASSWORD` and `ALTER USER` and nothing else, and the
/// statement that stores a new password lets it out again.
#[test]
fn a_sandboxed_session_may_only_fix_its_own_password() {
    let mut session = session_with_privileges();
    session
        .run("CREATE USER sandboxed IDENTIFIED BY 'old'")
        .unwrap();
    let registry = session.privileges.clone().unwrap();

    let mut sandbox = session_as(&registry, session.catalog.clone(), "sandboxed", "%");
    sandbox.enable_sandbox_mode();
    assert!(sandbox.in_sandbox_mode());
    for sql in ["SELECT 1", "SHOW DATABASES", "BEGIN", "SET @x = 1"] {
        assert!(
            matches!(sandbox.run(sql), Err(DriverError::MustChangePassword)),
            "{sql} must be gated"
        );
    }
    // A syntax error is still a syntax error: Go gates the PARSED statement,
    // so parsing fails first.
    assert!(matches!(
        sandbox.run("SELECT FROM WHERE"),
        Err(DriverError::Parse(_))
    ));

    sandbox.run("SET PASSWORD = 'fixed'").unwrap();
    assert!(!sandbox.in_sandbox_mode());
    sandbox.run("SELECT 1").unwrap();
    assert_eq!(
        registry.auth_string("sandboxed", "%"),
        Some(privilege::encode_password("fixed"))
    );

    // `ALTER USER ... IDENTIFIED BY` is the other way out.
    let mut sandbox = session_as(&registry, session.catalog.clone(), "sandboxed", "%");
    sandbox.enable_sandbox_mode();
    assert!(matches!(
        sandbox.run("SELECT 1"),
        Err(DriverError::MustChangePassword)
    ));
    sandbox
        .run("ALTER USER CURRENT_USER() IDENTIFIED BY 'again'")
        .unwrap();
    assert!(!sandbox.in_sandbox_mode());
    sandbox.run("SELECT 1").unwrap();
}
