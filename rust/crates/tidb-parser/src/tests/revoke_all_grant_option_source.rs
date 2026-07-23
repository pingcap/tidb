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

//! Direct `TestPrivilege` rows for Go's special
//! `REVOKE ALL [PRIVILEGES], GRANT OPTION FROM ...` conversion.

use super::*;

#[test]
fn revoke_all_grant_option_restore_like_go() {
    for (sql, expected) in [
        (
            "revoke all privileges, grant option from u1",
            "REVOKE ALL, GRANT OPTION ON *.* FROM `u1`@`%`",
        ),
        (
            "revoke all privileges, grant option from u1, u2, u3",
            "REVOKE ALL, GRANT OPTION ON *.* FROM `u1`@`%`, `u2`@`%`, `u3`@`%`",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn revoke_all_grant_option_has_typed_global_level() {
    let statement = parse("REVOKE ALL PRIVILEGES, GRANT OPTION FROM ss1").expect("parse");
    let Stmt::Admin(admin) = statement else {
        panic!("expected administrative statement");
    };
    let AdminStmt::Revoke(revoke) = admin.as_ref() else {
        panic!("expected typed REVOKE statement");
    };
    assert!(revoke.object_type.is_none());
    assert_eq!(revoke.level, tidb_ast::GrantLevel::Global);
    assert_eq!(revoke.privileges.len(), 2);
    assert_eq!(revoke.privileges[0].name, "ALL");
    assert_eq!(revoke.privileges[1].name, "GRANT OPTION");
    assert_eq!(revoke.users[0].user.user, "ss1");
}

#[test]
fn revoke_all_grant_option_rejects_near_misses() {
    for sql in [
        "revoke all from u",
        "revoke all, grant option, select from u",
        "revoke all privileges, grant option from",
    ] {
        assert!(parse(sql).is_err(), "source SQL must be rejected: {sql}");
    }
}
