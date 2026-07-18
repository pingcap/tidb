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

//! Direct source rows for Go's identifier-only dynamic privilege revoke path.

use super::*;

#[test]
fn revoke_dynamic_privileges_restore_like_go() {
    for (sql, expected) in [
        (
            "REVOKE BACKUP_Admin,system_variables_admin ON executor__revoke.* FROM dyn",
            "REVOKE BACKUP_ADMIN, SYSTEM_VARIABLES_ADMIN ON `executor__revoke`.* FROM `dyn`@`%`",
        ),
        (
            "REVOKE BACKUP_Admin ON *.* FROM dyn",
            "REVOKE BACKUP_ADMIN ON *.* FROM `dyn`@`%`",
        ),
        (
            "REVOKE BACKUP_ADMIN, SYSTEM_VARIABLES_ADMIN ON *.* FROM dyn",
            "REVOKE BACKUP_ADMIN, SYSTEM_VARIABLES_ADMIN ON *.* FROM `dyn`@`%`",
        ),
        (
            "REVOKE BACKUP_ADMIN, SYSTEM_VARIABLES_ADMIN, SELECT, INSERT ON *.* FROM dyn",
            "REVOKE BACKUP_ADMIN, SYSTEM_VARIABLES_ADMIN, SELECT, INSERT ON *.* FROM `dyn`@`%`",
        ),
        (
            "REVOKE BACKUP_ADMIN, SELECT, GRANT OPTION ON *.* FROM dyn",
            "REVOKE BACKUP_ADMIN, SELECT, GRANT OPTION ON *.* FROM `dyn`@`%`",
        ),
        (
            "REVOKE bogus ON *.* FROM dyn",
            "REVOKE BOGUS ON *.* FROM `dyn`@`%`",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn revoke_resource_group_privileges_restore_like_go() {
    for (sql, expected) in [
        (
            "REVOKE RESOURCE_GROUP_ADMIN ON *.* FROM resource_group_admin",
            "REVOKE RESOURCE_GROUP_ADMIN ON *.* FROM `resource_group_admin`@`%`",
        ),
        (
            "REVOKE RESOURCE_GROUP_USER ON *.* FROM resource_group_user",
            "REVOKE RESOURCE_GROUP_USER ON *.* FROM `resource_group_user`@`%`",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn revoke_dynamic_privileges_are_typed_without_widening_role_forms() {
    let statement = parse("REVOKE BACKUP_ADMIN, SELECT ON *.* FROM dyn").expect("parse");
    let Stmt::Admin(admin) = statement else {
        panic!("expected administrative statement");
    };
    let AdminStmt::Revoke(revoke) = admin.as_ref() else {
        panic!("expected typed REVOKE statement");
    };
    assert!(revoke.privileges[0].dynamic);
    assert!(!revoke.privileges[1].dynamic);
    assert!(parse("REVOKE 'role1' FROM dyn").is_ok());
    let statement = parse("REVOKE BACKUP_ADMIN FROM dyn").expect("role-shaped form parses");
    assert!(matches!(
        statement,
        Stmt::Admin(admin) if matches!(admin.as_ref(), AdminStmt::RevokeRole(_))
    ));
    let statement = parse("REVOKE bogus ON *.* FROM dyn").expect("arbitrary dynamic privilege");
    let Stmt::Admin(admin) = statement else {
        panic!("expected administrative statement");
    };
    let AdminStmt::Revoke(revoke) = admin.as_ref() else {
        panic!("expected typed REVOKE statement");
    };
    assert!(revoke.privileges[0].dynamic);
    assert_eq!(revoke.privileges[0].name, "BOGUS");
}
