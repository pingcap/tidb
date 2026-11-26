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

//! GRANT / REVOKE with an object type, and with a column list outside TABLE
//! scope, answering as Go over unistore (`tools/xtake/a05.sql`).

use crate::support::testkit::TestKit;

/// Go's GrantExec / RevokeExec never read `ObjectType`: `ON TABLE`,
/// `ON FUNCTION` and `ON PROCEDURE` name a table-level target. A column list
/// at DB or global scope fails GRANT's `checkAndInitColumnPriv` (1146 for
/// the table named `""`), while REVOKE revokes the plain privilege.
#[test]
fn grant_and_revoke_ignore_the_object_type() {
    let mut tk = TestKit::new();
    tk.must_exec("create database sa");
    tk.must_exec("create table sa.t (a int, b int)");
    tk.must_exec("create user u");
    tk.must_exec("grant select on table sa.t to u");
    tk.must_query("show grants for u").check(["GRANT USAGE ON *.* TO `u`@`%`", "GRANT SELECT ON `sa`.`t` TO `u`@`%`"]);
    tk.must_exec("revoke select on table sa.t from u");
    tk.must_query("show grants for u").check(["GRANT USAGE ON *.* TO `u`@`%`"]);
    tk.must_get_err_code("grant execute on function sa.f to u", 1144);
    tk.must_contain_err_msg("grant select on function sa.f to u", "Table 'sa.f' doesn't exist");
    tk.must_exec("grant select on procedure sa.t to u");
    tk.must_query("show grants for u").check(["GRANT USAGE ON *.* TO `u`@`%`", "GRANT SELECT ON `sa`.`t` TO `u`@`%`"]);
    tk.must_exec("revoke select on procedure sa.t from u");

    tk.must_contain_err_msg("grant select (b) on sa.* to u", "Table 'sa.' doesn't exist");
    tk.must_contain_err_msg("grant select (b) on *.* to u", "Table 'test.' doesn't exist");
    tk.must_exec("grant select on sa.* to u");
    tk.must_exec("revoke select (b) on sa.* from u");
    tk.must_query("show grants for u").check(["GRANT USAGE ON *.* TO `u`@`%`"]);
    tk.must_exec("grant select on *.* to u");
    tk.must_exec("revoke select (b) on *.* from u");
    tk.must_query("show grants for u").check(["GRANT USAGE ON *.* TO `u`@`%`"]);
}
