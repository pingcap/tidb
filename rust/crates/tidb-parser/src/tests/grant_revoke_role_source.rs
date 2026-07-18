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

//! Direct rows from `pkg/parser/parser_test.go:TestDDL` for Go's no-`ON`
//! `GrantRoleStmt` and `RevokeRoleStmt` branches.

use super::*;

#[test]
fn grant_revoke_role_restore_like_go_testddl() {
    assert_eq!(
        r("grant 'role1', 'role2' to 'user1'@'LOCalhost', 'user2'@'LOcalhost'"),
        "GRANT `role1`@`%`, `role2`@`%` TO `user1`@`localhost`, `user2`@`localhost`"
    );
    assert_eq!(
        r("grant 'app_read'@'%', 'app_write'@'%' to current_user()"),
        "GRANT `app_read`@`%`, `app_write`@`%` TO CURRENT_USER"
    );
    assert_eq!(
        r("revoke 'role1', 'role2' from 'user1'@'localhost', 'user2'@'localhost'"),
        "REVOKE `role1`@`%`, `role2`@`%` FROM `user1`@`localhost`, `user2`@`localhost`"
    );
}

#[test]
fn grant_revoke_role_keep_typed_role_and_user_lists() {
    let statement = parse("grant r1@LOCALHOST, 'r2' to current_user, 'u'@'LOCALHOST'")
        .expect("parse Go GRANT ROLE form");
    let tidb_ast::Stmt::Admin(admin) = statement else {
        panic!("expected administrative statement");
    };
    let tidb_ast::AdminStmt::GrantRole(grant) = admin.as_ref() else {
        panic!("expected typed GRANT ROLE statement");
    };
    assert_eq!(grant.roles[0].role, "r1");
    assert_eq!(grant.roles[0].host, "localhost");
    assert_eq!(grant.roles[1].role, "r2");
    assert_eq!(grant.roles[1].host, "%");
    assert!(grant.users[0].current_user);
    assert_eq!(grant.users[1].host, "localhost");
}

#[test]
fn grant_revoke_role_reject_invalid_role_and_missing_targets() {
    for sql in [
        "grant current_user to u",
        "revoke current_user from u",
        "grant r1 to",
        "revoke r1 from",
    ] {
        assert!(parse(sql).is_err(), "source SQL: {sql}");
    }
}
