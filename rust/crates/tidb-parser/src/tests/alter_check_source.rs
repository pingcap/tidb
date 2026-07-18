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

//! Direct ports of `TestDDL`'s `parseAlterAlter` CHECK-enforcement rows.

use super::*;

#[test]
fn alter_check_testddl_rows_match_go_restore() {
    for (sql, expected) in [
        (
            "ALTER TABLE t_n ALTER CHECK ident ENFORCED",
            "ALTER TABLE `t_n` ALTER CHECK `ident` ENFORCED",
        ),
        (
            "ALTER TABLE t_n ALTER CHECK ident NOT ENFORCED",
            "ALTER TABLE `t_n` ALTER CHECK `ident` NOT ENFORCED",
        ),
        (
            "ALTER TABLE t_n ALTER CONSTRAINT ident enforced",
            "ALTER TABLE `t_n` ALTER CHECK `ident` ENFORCED",
        ),
        (
            "ALTER TABLE t_n ALTER CHECK ident not enforced",
            "ALTER TABLE `t_n` ALTER CHECK `ident` NOT ENFORCED",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }
}

#[test]
fn alter_check_requires_enforcement_payload() {
    for sql in [
        "ALTER TABLE t_n ALTER CONSTRAINT ident",
        "ALTER TABLE t_n ALTER CHECK ident DISABLED",
    ] {
        assert!(parse(sql).is_err(), "{sql}");
    }
}
