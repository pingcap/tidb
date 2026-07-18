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

//! Direct ports of `TestDDL`'s `parseAlterAlter` column-default rows.

use super::*;

#[test]
fn alter_column_default_testddl_rows_match_go_restore() {
    for (sql, expected) in [
        (
            "ALTER TABLE t ALTER COLUMN a SET DEFAULT 1",
            "ALTER TABLE `t` ALTER COLUMN `a` SET DEFAULT 1",
        ),
        (
            "ALTER TABLE t ALTER a SET DEFAULT 1",
            "ALTER TABLE `t` ALTER COLUMN `a` SET DEFAULT 1",
        ),
        (
            "ALTER TABLE t ALTER COLUMN a SET DEFAULT (CURRENT_TIMESTAMP())",
            "ALTER TABLE `t` ALTER COLUMN `a` SET DEFAULT (CURRENT_TIMESTAMP())",
        ),
        (
            "ALTER TABLE t ALTER COLUMN a SET DEFAULT (NOW())",
            "ALTER TABLE `t` ALTER COLUMN `a` SET DEFAULT (NOW())",
        ),
        (
            "ALTER TABLE t ALTER COLUMN a SET DEFAULT (1+1)",
            "ALTER TABLE `t` ALTER COLUMN `a` SET DEFAULT (1+1)",
        ),
        (
            "ALTER TABLE t ALTER COLUMN a SET DEFAULT (1)",
            "ALTER TABLE `t` ALTER COLUMN `a` SET DEFAULT 1",
        ),
        (
            "ALTER TABLE t ALTER COLUMN a DROP DEFAULT",
            "ALTER TABLE `t` ALTER COLUMN `a` DROP DEFAULT",
        ),
        (
            "ALTER TABLE t ALTER a DROP DEFAULT",
            "ALTER TABLE `t` ALTER COLUMN `a` DROP DEFAULT",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }
}

#[test]
fn alter_column_default_preserves_go_literal_boundary() {
    for sql in [
        "ALTER TABLE t ALTER COLUMN a SET DEFAULT CURRENT_TIMESTAMP",
        "ALTER TABLE t ALTER COLUMN a SET DEFAULT NOW()",
        "ALTER TABLE t ALTER COLUMN a SET DEFAULT 1+1",
    ] {
        assert!(parse(sql).is_err(), "{sql}");
    }
}
