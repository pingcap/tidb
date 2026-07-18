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

//! Source-backed `parseDropDatabase` / `DropDatabaseStmt.Restore` coverage.
//!
//! The ordinary TestDDL rows use a plain identifier, while the integration
//! row uses `plan_cache`, a reserved keyword. Go's parser consumes that token
//! directly in `parseDropDatabase`, so the Rust port must keep this broader
//! identifier-like slot local to DROP DATABASE.

use super::*;

#[test]
fn drop_database_source_rows_restore_like_go() {
    let cases = [
        ("drop database xxx", "DROP DATABASE `xxx`"),
        (
            "drop database if exists xxx",
            "DROP DATABASE IF EXISTS `xxx`",
        ),
        ("drop schema xxx", "DROP DATABASE `xxx`"),
        ("drop schema if exists xxx", "DROP DATABASE IF EXISTS `xxx`"),
        (
            "drop database if exists plan_cache",
            "DROP DATABASE IF EXISTS `plan_cache`",
        ),
    ];
    for (sql, expected) in cases {
        assert_eq!(r(sql), expected, "source row: {sql}");
    }
}

#[test]
fn drop_database_source_rejects_if_not_exists() {
    for sql in [
        "drop database if not exists xxx",
        "drop schema if not exists xxx",
    ] {
        assert!(parse(sql).is_err(), "Go rejects source row: {sql}");
    }
}
