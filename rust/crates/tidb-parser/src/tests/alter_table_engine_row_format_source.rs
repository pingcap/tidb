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

//! The comma-separated `ENGINE`/`ROW_FORMAT` ALTER option row from
//! `tests/integrationtest/t/util/admin.test:16`.

use super::*;

#[test]
fn alter_table_engine_row_format_restores_like_go() {
    assert_eq!(
        r("ALTER TABLE t1 engine=innodb, ROW_FORMAT=DYNAMIC"),
        "ALTER TABLE `t1` ENGINE = innodb, ROW_FORMAT = DYNAMIC"
    );
}

#[test]
fn alter_table_engine_row_format_keeps_option_boundaries() {
    for sql in [
        "ALTER TABLE t1 ENGINE",
        "ALTER TABLE t1 ROW_FORMAT =",
        "ALTER TABLE t1 ENGINE = , ROW_FORMAT = DYNAMIC",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
}
