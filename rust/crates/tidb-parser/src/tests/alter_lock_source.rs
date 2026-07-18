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

//! Direct ports of `parseAlterTableOptions` LOCK rows from `TestDDL`.

use super::*;

#[test]
fn alter_lock_testddl_rows_match_go_restore() {
    for (input, output) in [
        ("lock=none", "LOCK = NONE"),
        ("lock=default", "LOCK = DEFAULT"),
        ("lock=shared", "LOCK = SHARED"),
        ("lock=exclusive", "LOCK = EXCLUSIVE"),
        ("lock none", "LOCK = NONE"),
        ("lock default", "LOCK = DEFAULT"),
        ("lock shared", "LOCK = SHARED"),
        ("lock exclusive", "LOCK = EXCLUSIVE"),
        ("LOCK=NONE", "LOCK = NONE"),
        ("LOCK=DEFAULT", "LOCK = DEFAULT"),
        ("LOCK=SHARED", "LOCK = SHARED"),
        ("LOCK=EXCLUSIVE", "LOCK = EXCLUSIVE"),
    ] {
        let sql = format!("ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, {input}");
        assert_eq!(
            r(&sql),
            format!("ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED, {output}"),
            "{sql}"
        );
    }
}

#[test]
fn alter_lock_unblocks_testddl_drop_check_prefix() {
    assert_eq!(
        r("ALTER TABLE t_n LOCK = DEFAULT, DROP CHECK ident"),
        "ALTER TABLE `t_n` LOCK = DEFAULT, DROP CHECK `ident`"
    );
}

#[test]
fn alter_lock_rejects_unknown_lock_type() {
    for sql in ["ALTER TABLE t LOCK", "ALTER TABLE t LOCK = first"] {
        assert!(parse(sql).is_err(), "{sql}");
    }
}
