// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Account DDL, authentication, role, and SHOW CREATE USER parser tests.

use super::*;

#[test]
fn account_ddl_statements_use_the_ddl_envelope() {
    for sql in [
        "create user ddl_envelope",
        "alter user ddl_envelope identified by 'p'",
        "drop user ddl_envelope",
    ] {
        assert!(matches!(parse(sql), Ok(Stmt::Ddl(_))), "{sql}");
    }
}

/// `CREATE USER` is a parse/restore statement in this seed, not an implicit
/// user-catalog implementation. These vectors come from Go `TestPrivilege`
/// (`pkg/parser/parser_test.go:5314-5331`).
#[test]
fn create_user_auth_restore_and_scope() {
    assert_eq!(r("create user u"), "CREATE USER `u`@`%`");
    assert_eq!(
        r("create user if not exists 'root'@'localhost' identified by 'new-password'"),
        "CREATE USER IF NOT EXISTS `root`@`localhost` IDENTIFIED BY 'new-password'"
    );
    assert_eq!(
        r("create user 'root'@'localhost' identified by password 'hashstring'"),
        "CREATE USER `root`@`localhost` IDENTIFIED WITH 'mysql_native_password' AS 'hashstring'"
    );
    assert_eq!(
        r("create user u1 identified with mysql_native_password, u2 identified with 'caching_sha2_password' by 'p2', u3 identified with authentication_ldap_simple as 'uid=u3,dc=example'"),
        "CREATE USER `u1`@`%` IDENTIFIED WITH 'mysql_native_password', `u2`@`%` IDENTIFIED WITH 'caching_sha2_password' BY 'p2', `u3`@`%` IDENTIFIED WITH 'authentication_ldap_simple' AS 'uid=u3,dc=example'"
    );
    assert_eq!(
        r("CREATE USER 'sha_test3'@'localhost' IDENTIFIED WITH 'caching_sha2_password' AS 0x24412430303524255B03496C662C1055127B3B654A2F04207D01485276703644704B76303247474564416A516662346C5868646D32764C6B514F43585A473779565947514F34"),
        "CREATE USER `sha_test3`@`localhost` IDENTIFIED WITH 'caching_sha2_password' AS '$A$005$%[\u{3}Ilf,\u{10}U\u{12}{;eJ/\u{4} }\u{1}HRvp6DpKv02GGEdAjQfb4lXhdm2vLkQOCXZG7yVYGQO4'"
    );

    let stmt = parse("create user u identified by 'p', v identified with plugin as 'h'")
        .expect("CREATE USER parses");
    let Stmt::Ddl(ddl) = stmt else {
        panic!("expected DDL envelope");
    };
    let tidb_ast::DdlStmt::CreateUser {
        if_not_exists,
        users,
        tls_options,
        resource_options,
        password_options,
        comment_or_attribute,
        resource_group,
    } = ddl.into_inner()
    else {
        panic!("expected CreateUser");
    };
    assert!(!if_not_exists);
    assert_eq!(users.len(), 2);
    assert!(tls_options.is_empty());
    assert!(resource_options.is_empty());
    assert!(password_options.is_empty());
    assert!(comment_or_attribute.is_none());
    assert!(resource_group.is_none());
    assert!(matches!(
        &users[0].auth,
        Some(tidb_ast::CreateUserAuth::By(password)) if password == "p"
    ));
    assert!(matches!(
        &users[1].auth,
        Some(tidb_ast::CreateUserAuth::With {
            plugin,
            credential: Some(tidb_ast::CreateUserCredential::As(hash)),
        }) if plugin == "plugin" && hash == "h"
    ));

    assert_eq!(
        r("create user u identified by 'p' password expire interval 3 day password history 5 password reuse interval default account lock failed_login_attempts 3 password_lock_time unbounded attribute '{\"role\": \"reader\"}'"),
        "CREATE USER `u`@`%` IDENTIFIED BY 'p' PASSWORD EXPIRE INTERVAL 3 DAY PASSWORD HISTORY 5 PASSWORD REUSE INTERVAL DEFAULT ACCOUNT LOCK FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME UNBOUNDED ATTRIBUTE '{\"role\": \"reader\"}'"
    );

    for sql in [
        "create user u identified by 'p' retain current password",
        "create user u require ssl ssl",
        "create user u password history -5",
    ] {
        assert!(parse(sql).is_err(), "unexpectedly accepted: {sql}");
    }
}

#[test]
fn alter_user_auth_restore_and_scope() {
    assert_eq!(
        r("alter user if exists 'app'@'localhost' identified by 'secret', app2 identified with mysql_native_password as 'hash'"),
        "ALTER USER IF EXISTS `app`@`localhost` IDENTIFIED BY 'secret', `app2`@`%` IDENTIFIED WITH 'mysql_native_password' AS 'hash'"
    );
    assert_eq!(
        r("alter user app identified by password 'hash'"),
        "ALTER USER `app`@`%` IDENTIFIED WITH 'mysql_native_password' AS 'hash'"
    );
    assert_eq!(
        r("alter user current_user identified by 'secret'"),
        "ALTER USER CURRENT_USER IDENTIFIED BY 'secret'"
    );
    assert_eq!(
        r("alter user user() identified by 'secret'"),
        "ALTER USER USER() IDENTIFIED BY 'secret'"
    );
    assert_eq!(
        r("alter user current_user() identified by 'secret'"),
        "ALTER USER CURRENT_USER IDENTIFIED BY 'secret'"
    );
    assert_eq!(r("alter user app"), "ALTER USER `app`@`%`");
    for sql in [
        "alter user user() identified by password 'hash'",
        "alter user user()",
        "alter user user() identified with p",
    ] {
        assert!(parse(sql).is_err(), "{sql}");
    }
}

#[test]
fn alter_user_resource_group_restore_and_scope() {
    assert_eq!(
        r("alter user app resource group rg1"),
        "ALTER USER `app`@`%` RESOURCE GROUP `rg1`"
    );
    assert_eq!(
        r("alter user if exists 'app'@'localhost', app2 resource group 'rg 2'"),
        "ALTER USER IF EXISTS `app`@`localhost`, `app2`@`%` RESOURCE GROUP `rg 2`"
    );
    let Stmt::Ddl(ddl) = parse("alter user app resource group rg1").unwrap() else {
        panic!("expected DDL envelope");
    };
    let tidb_ast::DdlStmt::AlterUser(statement) = ddl.into_inner() else {
        panic!("expected ALTER USER payload");
    };
    assert_eq!(statement.users.len(), 1);
    assert!(statement.password_options.is_empty());
    assert_eq!(statement.resource_group.as_deref(), Some("rg1"));

    for sql in [
        "alter user app resource group",
        "alter user app resource group select",
        "alter user app resource group rg1 account lock",
    ] {
        assert!(parse(sql).is_err(), "unexpectedly accepted: {sql}");
    }
}

#[test]
fn rename_user_restore_and_scope() {
    assert_eq!(
        r("rename user 'root'@'localhost' to 'root'@'%'"),
        "RENAME USER `root`@`localhost` TO `root`@`%`"
    );
    assert_eq!(
        r("rename user u1 to u2, 'u3'@'LOCALHOST' to u4@localhost"),
        "RENAME USER `u1`@`%` TO `u2`@`%`, `u3`@`localhost` TO `u4`@`localhost`"
    );

    let Stmt::Ddl(ddl) = parse("rename user u1 to u2, u3 to u4").unwrap() else {
        panic!("expected DDL envelope");
    };
    let tidb_ast::DdlStmt::RenameUser { pairs } = ddl.into_inner() else {
        panic!("expected RENAME USER payload");
    };
    assert_eq!(pairs.len(), 2);
    assert_eq!(pairs[0].old_user.user, "u1");
    assert_eq!(pairs[0].old_user.host, "%");
    assert_eq!(pairs[1].new_user.user, "u4");
    assert_eq!(pairs[1].new_user.host, "%");

    for sql in [
        "rename user",
        "rename user u1",
        "rename user u1 to",
        "rename user u1 to u2,",
        "rename user u1 to u2 identified by 'secret'",
    ] {
        assert!(parse(sql).is_err(), "unexpectedly accepted: {sql}");
    }
}

#[test]
fn alter_user_password_expire_restore_and_scope() {
    assert_eq!(
        r("alter user app password expire"),
        "ALTER USER `app`@`%` PASSWORD EXPIRE"
    );
    assert_eq!(
        r("alter user 'app'@'localhost' password expire default"),
        "ALTER USER `app`@`localhost` PASSWORD EXPIRE DEFAULT"
    );
    assert_eq!(
        r("alter user app password expire never"),
        "ALTER USER `app`@`%` PASSWORD EXPIRE NEVER"
    );
    assert_eq!(
        r("alter user app identified by 'secret' password expire interval 3 day"),
        "ALTER USER `app`@`%` IDENTIFIED BY 'secret' PASSWORD EXPIRE INTERVAL 3 DAY"
    );

    let Stmt::Ddl(ddl) = parse("alter user app password expire interval 7 day").unwrap() else {
        panic!("expected DDL envelope")
    };
    let tidb_ast::DdlStmt::AlterUser(statement) = ddl.into_inner() else {
        panic!("expected ALTER USER payload")
    };
    assert_eq!(statement.users.len(), 1);
    assert_eq!(
        statement.password_options,
        vec![tidb_ast::CreateUserPasswordOption::Expire(
            tidb_ast::AlterUserPasswordExpire::Interval(7)
        )]
    );
    assert_eq!(
        r("alter user app password history 3 password reuse interval 4 day password require current default account unlock"),
        "ALTER USER `app`@`%` PASSWORD HISTORY 3 PASSWORD REUSE INTERVAL 4 DAY PASSWORD REQUIRE CURRENT DEFAULT ACCOUNT UNLOCK"
    );
}

#[test]
fn alter_user_dual_password_restore_and_scope() {
    assert_eq!(
        r("alter user app identified by 'secret' retain current password"),
        "ALTER USER `app`@`%` IDENTIFIED BY 'secret' RETAIN CURRENT PASSWORD"
    );
    assert_eq!(
        r("alter user app discard old password"),
        "ALTER USER `app`@`%` DISCARD OLD PASSWORD"
    );
    assert_eq!(
        r("alter user user() identified by 'secret' retain current password"),
        "ALTER USER USER() IDENTIFIED BY 'secret' RETAIN CURRENT PASSWORD"
    );
    assert_eq!(
        r("alter user user() discard old password"),
        "ALTER USER USER() DISCARD OLD PASSWORD"
    );

    for sql in [
        "alter user app identified by 'secret' discard old password",
        "alter user app identified with p as 'hash' retain current password",
        "alter user app retain current password",
    ] {
        assert!(parse(sql).is_err(), "{sql}");
    }
}

#[test]
fn alter_user_full_source_order_and_typed_options() {
    let sql = "alter user if exists dpm1 discard old password, dpm3 require issuer 'issuer' subject 'subject' cipher 'cipher' with max_queries_per_hour 1 max_updates_per_hour 2 max_connections_per_hour 3 max_user_connections 5 password history 7 password reuse interval 8 day failed_login_attempts 9 password_lock_time unbounded account unlock comment 'rotation finished' resource group rg1";
    assert_eq!(
        r(sql),
        "ALTER USER IF EXISTS `dpm1`@`%` DISCARD OLD PASSWORD, `dpm3`@`%` REQUIRE ISSUER 'issuer' AND SUBJECT 'subject' AND CIPHER 'cipher' WITH MAX_QUERIES_PER_HOUR 1 MAX_UPDATES_PER_HOUR 2 MAX_CONNECTIONS_PER_HOUR 3 MAX_USER_CONNECTIONS 5 PASSWORD HISTORY 7 PASSWORD REUSE INTERVAL 8 DAY FAILED_LOGIN_ATTEMPTS 9 PASSWORD_LOCK_TIME UNBOUNDED ACCOUNT UNLOCK COMMENT 'rotation finished' RESOURCE GROUP `rg1`"
    );

    let Stmt::Ddl(ddl) = parse(sql).unwrap() else {
        panic!("expected DDL envelope")
    };
    let tidb_ast::DdlStmt::AlterUser(statement) = ddl.into_inner() else {
        panic!("expected ALTER USER payload")
    };
    assert!(statement.if_exists);
    assert_eq!(statement.users.len(), 2);
    assert_eq!(statement.tls_options.len(), 3);
    assert_eq!(statement.resource_options.len(), 4);
    assert_eq!(statement.password_options.len(), 5);
    assert_eq!(
        statement.comment_or_attribute,
        Some(tidb_ast::CreateUserCommentOrAttribute::Comment(
            "rotation finished".to_string()
        ))
    );
    assert_eq!(statement.resource_group.as_deref(), Some("rg1"));

    assert_eq!(
        r("alter user dpattr attribute '{\"access\": 1}'"),
        "ALTER USER `dpattr`@`%` ATTRIBUTE '{\"access\": 1}'"
    );
    for (sql, expected) in [
        (
            "alter user maint_auth require none",
            "ALTER USER `maint_auth`@`%` REQUIRE NONE",
        ),
        (
            "alter user maint_auth require x509",
            "ALTER USER `maint_auth`@`%` REQUIRE X509",
        ),
        (
            "alter user maint_auth require ssl",
            "ALTER USER `maint_auth`@`%` REQUIRE SSL",
        ),
        (
            "alter user limits with max_queries_per_hour 9223372036854775807",
            "ALTER USER `limits`@`%` WITH MAX_QUERIES_PER_HOUR 9223372036854775807",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }
    assert!(parse("alter user limits with max_queries_per_hour 9223372036854775808").is_err());
    assert_eq!(
        r("alter user limits password history 9223372036854775808"),
        "ALTER USER `limits`@`%` PASSWORD HISTORY 9223372036854775807"
    );
    assert_eq!(r("alter user app require"), "ALTER USER `app`@`%`");
    assert_eq!(r("alter user app with"), "ALTER USER `app`@`%`");

    for sql in [
        "alter user app require ssl ssl",
        "alter user app require x509 and x509",
        "alter user app require none none",
        "alter user app with max_user_connections",
    ] {
        assert!(parse(sql).is_err(), "unexpectedly accepted: {sql}");
    }
}

#[test]
fn create_role_restore_uses_strict_role_identity() {
    assert_eq!(
        r("create role if not exists role1, 'role2'@LOCALHOST"),
        "CREATE ROLE IF NOT EXISTS `role1`@`%`, `role2`@`localhost`"
    );
    let Stmt::Ddl(ddl) = parse("create role r1, r2@localhost").unwrap() else {
        panic!("expected DDL envelope");
    };
    let tidb_ast::DdlStmt::CreateRole {
        if_not_exists,
        roles,
    } = ddl.into_inner()
    else {
        panic!("expected CreateRole");
    };
    assert!(!if_not_exists);
    assert_eq!(roles.len(), 2);
    assert_eq!(roles[0].role, "r1");
    assert_eq!(roles[0].host, "%");
    assert_eq!(roles[1].role, "r2");
    assert_eq!(roles[1].host, "localhost");

    for sql in [
        "create role resource",
        "create role r1 identified by 'password'",
        "create role r1 require ssl",
    ] {
        assert!(parse(sql).is_err(), "unexpectedly accepted: {sql}");
    }
}

#[test]
fn drop_user_and_role() {
    assert_eq!(r("drop user u1"), "DROP USER `u1`@`%`");
    assert_eq!(
        r("drop user if exists u1@LOCALHOST, u2"),
        "DROP USER IF EXISTS `u1`@`localhost`, `u2`@`%`"
    );
    assert_eq!(
        r("drop user 'admin'@'1.2.3.4'"),
        "DROP USER `admin`@`1.2.3.4`"
    );
    assert_eq!(r("drop role r1"), "DROP ROLE `r1`@`%`");
    assert_eq!(
        r("drop role if exists 'aa@bb', r2"),
        "DROP ROLE IF EXISTS `aa@bb`@`%`, `r2`@`%`"
    );
    assert_eq!(r("drop user current_user"), "DROP USER CURRENT_USER");
    assert_eq!(r("drop user current_user()"), "DROP USER CURRENT_USER");
}

#[test]
fn show_create_user_restore_and_scope() {
    assert_eq!(
        r("show create user 'root'@'localhost'"),
        "SHOW CREATE USER `root`@`localhost`"
    );
    assert_eq!(
        r("show create user current_user"),
        "SHOW CREATE USER CURRENT_USER"
    );
    assert!(parse("show create user if not exists").is_err());
}
