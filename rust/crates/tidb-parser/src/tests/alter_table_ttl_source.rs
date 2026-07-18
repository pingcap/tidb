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

//! Direct TestTTLTableOption ALTER coverage.

use super::*;

#[test]
fn alter_table_ttl_testttltableoption_rows_match_go_restore() {
    for (sql, expected) in [
        (
            "alter table t TTL = created_at + INTERVAL 1 MONTH",
            "ALTER TABLE `t` TTL = `created_at` + INTERVAL 1 MONTH",
        ),
        (
            "alter table t TTL = created_at + INTERVAL 1 MONTH TTL_ENABLE 'OFF' TTL_JOB_INTERVAL '1h'",
            "ALTER TABLE `t` TTL = `created_at` + INTERVAL 1 MONTH TTL_ENABLE = 'OFF' TTL_JOB_INTERVAL = '1h'",
        ),
        ("alter table t remove ttl", "ALTER TABLE `t` REMOVE TTL"),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn alter_table_ttl_uses_special_comments_when_requested() {
    let statement = parse("alter table t ttl_enable='on' remove ttl").expect("parse");
    assert_eq!(
        statement.restore_with_flags(
            tidb_ast::RestoreFlags::DEFAULT | tidb_ast::RestoreFlags::TIDB_SPECIAL_COMMENT,
        ),
        "ALTER TABLE `t` /*T![ttl] TTL_ENABLE = 'ON' */, /*T![ttl] REMOVE TTL */"
    );
}
