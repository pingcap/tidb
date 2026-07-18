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

//! Direct source rows for Go's `SHOW STATS_LOCKED` identifier entry.

use super::*;

/// Exact `TestDBAStmt` rows at `pkg/parser/parser_test.go:1349-1350`.
#[test]
fn show_stats_locked_restores_original_go_rows() {
    assert_eq!(r("show stats_locked"), "SHOW STATS_LOCKED");
    assert_eq!(
        r("show stats_locked where table_name = 't'"),
        "SHOW STATS_LOCKED WHERE `table_name`=_UTF8MB4't'"
    );
}

/// Go's shared `parseShowLikeOrWhere` gives LIKE a simple expression and
/// WHERE a full expression, while retaining a distinct typed payload.
#[test]
fn show_stats_locked_preserves_shared_show_filters() {
    assert_eq!(
        r("show stats_locked like 'table_%'"),
        "SHOW STATS_LOCKED LIKE _UTF8MB4'table_%'"
    );
    assert_eq!(
        r("show stats_locked where table_name like '%'"),
        "SHOW STATS_LOCKED WHERE `table_name` LIKE _UTF8MB4'%'"
    );

    let tidb_ast::Stmt::Admin(admin) =
        parse("show stats_locked where table_name = 't'").expect("parse")
    else {
        panic!("expected SHOW administrative envelope");
    };
    let tidb_ast::AdminStmt::ShowStatsLocked(locked) = admin.as_ref() else {
        panic!("expected typed SHOW STATS_LOCKED");
    };
    assert!(matches!(
        &locked.filter,
        Some(tidb_ast::ShowStatsLockedFilter::Where(_))
    ));

    for sql in [
        "show stats_locked like",
        "show stats_locked where",
        "show stats_locked like 't%' where table_name = 't'",
        "show stats_meta where table_name = 't'",
    ] {
        assert!(parse(sql).is_err(), "outside this STATS_LOCKED leaf: {sql}");
    }
}
