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

//! SET parser/restore tests mirroring `pkg/parser/set_explain_parser.go`.

use super::*;

#[test]
fn user_variable_assignments_restore_as_their_own_typed_set() {
    assert_eq!(r("set @x = 5"), "SET @`x`=5");
    assert_eq!(r("set @x := 5"), "SET @`x`=5");
    assert_eq!(r("set @X = 1 + 2"), "SET @`X`=1+2");
    assert_eq!(r("set @x = @y"), "SET @`x`=@`y`");
    assert_eq!(r("set @xx.xx = 666"), "SET @`xx.xx`=666");
    assert_eq!(
        r("set @a = 1, @b := @a + 2, @a = @b + 3"),
        "SET @`a`=1, @`b`=@`a`+2, @`a`=@`b`+3"
    );
    assert!(parse("set @a = 1, @@session.autocommit = 0").is_err());
    assert!(parse("set xx.xx.xx = 666").is_err());
}

/// `SET PASSWORD` is a distinct typed session statement, rather than a
/// generic system-variable assignment. The vectors are from TiDB's parser
/// tests (`pkg/parser/parser_test.go:1441-1442,5363-5364`), including the
/// dual-password form used by the static integration selector.
#[test]
fn set_password_restore_and_shape() {
    assert_eq!(r("set password = 'password'"), "SET PASSWORD='password'");
    assert_eq!(
        r("set password for 'root'@'localhost' = 'password'"),
        "SET PASSWORD FOR `root`@`localhost`='password'"
    );
    assert_eq!(
        r("set password for current_user() = password('new') retain current password"),
        "SET PASSWORD FOR CURRENT_USER='new' RETAIN CURRENT PASSWORD"
    );

    let statement = parse("set password for user = 'new' retain current password")
        .expect("SET PASSWORD parses");
    let tidb_ast::Stmt::Session(session) = statement else {
        panic!("expected Session envelope");
    };
    let tidb_ast::SessionStmt::SetPassword(set_password) = session.as_ref() else {
        panic!("expected typed SET PASSWORD");
    };
    assert!(set_password.user.is_some());
    assert_eq!(set_password.password, "new");
    assert!(set_password.retain_current_password);

    assert!(parse("set password = password(1)").is_err());
    assert!(parse("set password for = 'new'").is_err());
}

/// `SET ROLE` and `SET DEFAULT ROLE` are role-management statements, not
/// generic system-variable assignments. The canonical vectors are TiDB's
/// `TestParser` rows (`pkg/parser/parser_test.go:1468-1472`), while the
/// complete selection grammar comes from its hand parser
/// (`pkg/parser/set_explain_parser.go:350-415`).
#[test]
fn set_role_restore_and_shape() {
    assert_eq!(r("set role `role1`"), "SET ROLE `role1`@`%`");
    assert_eq!(r("set role default"), "SET ROLE DEFAULT");
    assert_eq!(r("set role none"), "SET ROLE NONE");
    assert_eq!(r("set role all"), "SET ROLE ALL");
    assert_eq!(r("set role resource"), "SET ROLE `resource`@`%`");
    assert_eq!(
        r("set role resource@LOCALHOST"),
        "SET ROLE `resource`@`localhost`"
    );
    assert_eq!(r("set role current_user"), "SET ROLE ``");
    assert_eq!(
        r("set role all except `role1`, `role2`"),
        "SET ROLE ALL EXCEPT `role1`@`%`, `role2`@`%`"
    );
    assert_eq!(
        r("set default role administrator, developer to `joe`@`10.0.0.1`, app"),
        "SET DEFAULT ROLE `administrator`@`%`, `developer`@`%` TO `joe`@`10.0.0.1`, `app`@`%`"
    );
    assert_eq!(
        r("set default role all to app"),
        "SET DEFAULT ROLE ALL TO `app`@`%`"
    );
    assert_eq!(
        r("set default role none to current_user()"),
        "SET DEFAULT ROLE NONE TO CURRENT_USER"
    );
    assert_eq!(
        r("set default role none to user()"),
        "SET DEFAULT ROLE NONE TO CURRENT_USER"
    );

    let statement = parse("set role all except `role1`@localhost, `role2`")
        .expect("SET ROLE ALL EXCEPT parses");
    let tidb_ast::Stmt::Session(session) = statement else {
        panic!("expected Session envelope");
    };
    let tidb_ast::SessionStmt::SetRole(set_role) = session.as_ref() else {
        panic!("expected typed SET ROLE");
    };
    assert!(matches!(
        &set_role.selection,
        tidb_ast::SetRoleSelection::AllExcept(roles)
            if roles.len() == 2 && roles[0].role == "role1" && roles[0].host == "localhost"
    ));

    let statement = parse("set default role r to user").expect("SET DEFAULT ROLE parses");
    let tidb_ast::Stmt::Session(session) = statement else {
        panic!("expected Session envelope");
    };
    let tidb_ast::SessionStmt::SetDefaultRole(set_default_role) = session.as_ref() else {
        panic!("expected typed SET DEFAULT ROLE");
    };
    assert!(matches!(
        &set_default_role.selection,
        tidb_ast::DefaultRoleSelection::Roles(roles) if roles.len() == 1 && roles[0].role == "r"
    ));
    assert_eq!(set_default_role.users.len(), 1);

    // Do not accept malformed role selections by falling back to generic SET.
    assert!(parse("set role all except").is_err());
    assert!(parse("set default role all").is_err());
    assert!(parse("set default role role to").is_err());
    // DEFAULT selects the dedicated mode; the malformed composed spelling
    // must not fall back into an ordinary account identity.
    assert!(parse("set role default@").is_err());
}

#[test]
fn system_variable_basics_preserve_source_names() {
    assert_eq!(
        r("set timestamp = 1700000000"),
        "SET @@SESSION.`timestamp`=1700000000"
    );
    assert_eq!(
        r("set TIMESTAMP = 1700000000"),
        "SET @@SESSION.`TIMESTAMP`=1700000000"
    );
    assert_eq!(
        r("set time_zone = '+00:00'"),
        "SET @@SESSION.`time_zone`=_UTF8MB4'+00:00'"
    );
}

/// Source rows from `TestSimple` and `TestSetVariable` retain scope, dotted
/// names, assignment order, and SET-specific value forms.
#[test]
fn system_variable_assignments() {
    assert_eq!(
        r("set @@local.sql_log_bin := 0, @@global.autocommit = default"),
        "SET @@SESSION.`sql_log_bin`=0, @@GLOBAL.`autocommit`=DEFAULT"
    );
    assert_eq!(
        r("set @@instance.tidb_mem_quota_query = 128"),
        "SET @@INSTANCE.`tidb_mem_quota_query`=128"
    );
    assert_eq!(
        r("set @@character_set_results = binary, @@autocommit = on"),
        "SET @@SESSION.`character_set_results`=_UTF8MB4'BINARY', @@SESSION.`autocommit`=_UTF8MB4'ON'"
    );
    assert_eq!(
        r("set @@session.autocommit = off"),
        "SET @@SESSION.`autocommit`=`off`"
    );

    for (sql, expected) in [
        ("set xx.xx = 666", "SET @@SESSION.`xx.xx`=666"),
        ("set session xx.xx = 666", "SET @@SESSION.`xx.xx`=666"),
        ("set local xx.xx = 666", "SET @@SESSION.`xx.xx`=666"),
        ("set global xx.xx = 666", "SET @@GLOBAL.`xx.xx`=666"),
        ("set instance xx.xx = 666", "SET @@INSTANCE.`xx.xx`=666"),
        ("set @@xx.xx = 666", "SET @@SESSION.`xx.xx`=666"),
        ("set @@session.xx.xx = 666", "SET @@SESSION.`xx.xx`=666"),
        ("set @@local.xx.xx = 666", "SET @@SESSION.`xx.xx`=666"),
        ("set @@global.xx.xx = 666", "SET @@GLOBAL.`xx.xx`=666"),
        ("set @@instance.xx.xx = 666", "SET @@INSTANCE.`xx.xx`=666"),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }

    let stmt = parse("set @@global.autocommit = default").expect("system SET parses");
    let tidb_ast::Stmt::Session(session) = stmt else {
        panic!("expected session statement");
    };
    let tidb_ast::SessionStmt::Set(set) = session.as_ref() else {
        panic!("expected generic SET");
    };
    assert!(matches!(
        set.assignments.as_slice(),
        [tidb_ast::SystemVariableAssignment {
            scope: tidb_ast::SystemVariableScope::Global,
            name,
            value: tidb_ast::SetVariableValue::Default,
        }] if name == "autocommit"
    ));
    assert!(parse("set @@xx.xx.xx = 666").is_err());
}

#[test]
fn charset_commands_are_typed_and_canonical() {
    assert_eq!(
        r("set names UTF8MB3 collate utf8_roman_ci"),
        "SET NAMES 'utf8' COLLATE 'utf8_roman_ci'"
    );
    assert_eq!(r("set names binary"), "SET NAMES 'binary'");
    assert_eq!(r("set names default"), "SET NAMES DEFAULT");
    // Go accepts COLLATE DEFAULT but leaves no ExtendValue in the AST.
    assert_eq!(r("set names utf8 collate default"), "SET NAMES 'utf8'");
    for sql in [
        "set character set 'utf8mb4'",
        "set char set utf8mb4",
        "set charset utf8mb4",
    ] {
        assert_eq!(r(sql), "SET CHARSET 'utf8mb4'");
        assert!(matches!(
            parse(sql),
            Ok(tidb_ast::Stmt::Session(session))
                if matches!(session.as_ref(), tidb_ast::SessionStmt::SetCharset {
                    kind: tidb_ast::CharsetSetKind::Charset,
                    ..
                })
        ));
    }
    assert!(parse("set names unknown_charset").is_err());
    assert!(parse("set character utf8").is_err());
    assert!(parse("set charset utf8 collate utf8_general_ci").is_err());
    assert_eq!(
        r("set names utf8, autocommit = 1"),
        "SET NAMES 'utf8', @@SESSION.`autocommit`=1"
    );
    assert!(parse("set names utf8; set autocommit = 1").is_err());
}

#[test]
fn resource_group_and_session_states_restore_and_shape() {
    assert_eq!(r("set resource group rg1"), "SET RESOURCE GROUP `rg1`");
    assert_eq!(r("set resource group ``"), "SET RESOURCE GROUP ``");
    assert_eq!(
        r("set resource group default"),
        "SET RESOURCE GROUP `default`"
    );
    assert_eq!(
        r("set session_states '{\"rs-group\":\"test\"}'"),
        "SET SESSION_STATES '{\"rs-group\":\"test\"}'"
    );

    let statement = parse("set resource group `rg2`").expect("SET RESOURCE GROUP parses");
    let tidb_ast::Stmt::Session(session) = statement else {
        panic!("expected Session envelope");
    };
    let tidb_ast::SessionStmt::SetResourceGroup(resource_group) = session.as_ref() else {
        panic!("expected typed SET RESOURCE GROUP");
    };
    assert_eq!(resource_group.name, "rg2");

    let statement = parse("set session_states 'serialized'").expect("SET SESSION_STATES parses");
    let tidb_ast::Stmt::Session(session) = statement else {
        panic!("expected Session envelope");
    };
    let tidb_ast::SessionStmt::SetSessionStates(session_states) = session.as_ref() else {
        panic!("expected typed SET SESSION_STATES");
    };
    assert_eq!(session_states.session_states, "serialized");

    assert!(parse("set resource group").is_err());
    assert!(parse("set resource group x y").is_err());
    assert!(parse("set session_states").is_err());
    assert!(parse("set session_states 1").is_err());
    assert!(parse("set session_states now()").is_err());
}

#[test]
fn transaction_set_sugar_restores_as_system_variables() {
    for (sql, expected) in [
        (
            "set session transaction isolation level repeatable read",
            "SET @@SESSION.`tx_isolation`=_UTF8MB4'REPEATABLE-READ'",
        ),
        (
            "set session transaction isolation level read committed",
            "SET @@SESSION.`tx_isolation`=_UTF8MB4'READ-COMMITTED'",
        ),
        (
            "set session transaction isolation level read uncommitted",
            "SET @@SESSION.`tx_isolation`=_UTF8MB4'READ-UNCOMMITTED'",
        ),
        (
            "set session transaction isolation level serializable",
            "SET @@SESSION.`tx_isolation`=_UTF8MB4'SERIALIZABLE'",
        ),
        (
            "set transaction isolation level repeatable read",
            "SET @@SESSION.`tx_isolation_one_shot`=_UTF8MB4'REPEATABLE-READ'",
        ),
        (
            "set transaction read only",
            "SET @@SESSION.`tx_read_only`=_UTF8MB4'1'",
        ),
        (
            "set transaction read write",
            "SET @@SESSION.`tx_read_only`=_UTF8MB4'0'",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }

    assert_eq!(
        r("set global transaction isolation level repeatable read"),
        "SET @@GLOBAL.`tx_isolation`=_UTF8MB4'REPEATABLE-READ'"
    );
    assert!(parse("set transaction isolation level read committed, read only").is_err());
}
