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

//! GRANT, REVOKE, and SHOW GRANTS parser tests.

use super::*;

#[test]
fn grant_privileges_restore_with_typed_account_authentication() {
    assert_eq!(
        r("grant select(id), create view, backup_admin on db.t to u, 'v'@'LOCALHOST' with grant option"),
        "GRANT SELECT (`id`), CREATE VIEW, BACKUP_ADMIN ON `db`.`t` TO `u`@`%`, `v`@`localhost` WITH GRANT OPTION"
    );
    assert_eq!(
        r("grant all privileges on * to current_user"),
        "GRANT ALL ON * TO CURRENT_USER"
    );
    assert_eq!(
        r("grant select on t to u identified by 'secret'"),
        "GRANT SELECT ON `t` TO `u`@`%` IDENTIFIED BY 'secret'"
    );
    assert_eq!(
        r("grant select on t to u require ssl"),
        "GRANT SELECT ON `t` TO `u`@`%` REQUIRE SSL"
    );
}

#[test]
fn revoke_standard_privileges_restore_and_reject_unowned_forms() {
    assert_eq!(
        r("revoke select(id), create user on db.t from u, 'v'@'LOCALHOST'"),
        "REVOKE SELECT (`id`), CREATE USER ON `db`.`t` FROM `u`@`%`, `v`@`localhost`"
    );
    assert_eq!(
        r("revoke all privileges on * from current_user"),
        "REVOKE ALL ON * FROM CURRENT_USER"
    );
    assert_eq!(
        r("revoke backup_admin on *.* from u"),
        "REVOKE BACKUP_ADMIN ON *.* FROM `u`@`%`"
    );
    assert_eq!(
        r("revoke all privileges, grant option from u"),
        "REVOKE ALL, GRANT OPTION ON *.* FROM `u`@`%`"
    );
}

#[test]
fn show_grants_restore_optional_user_and_roles() {
    assert_eq!(r("show grants"), "SHOW GRANTS");
    assert_eq!(
        r("show grants for 'u'@'LOCALHOST' using 'r1', r2"),
        "SHOW GRANTS FOR `u`@`localhost` USING `r1`@`%`, `r2`@`%`"
    );
    assert!(parse("show grants using r1").is_err());
}
