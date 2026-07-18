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

//! Direct source rows for Go's `SHOW STATS_BUCKETS` identifier entry.

use super::*;

/// Exact `TestDBAStmt` rows at `pkg/parser/parser_test.go:1355-1356`.
#[test]
fn show_stats_buckets_restores_original_go_rows() {
    assert_eq!(r("show stats_buckets"), "SHOW STATS_BUCKETS");
    assert_eq!(
        r("show stats_buckets where col_name = 'a'"),
        "SHOW STATS_BUCKETS WHERE `col_name`=_UTF8MB4'a'"
    );
}

#[test]
fn show_stats_buckets_preserves_its_own_filter_payload() {
    assert_eq!(
        r("show stats_buckets like 'col%'"),
        "SHOW STATS_BUCKETS LIKE _UTF8MB4'col%'"
    );
    let tidb_ast::Stmt::Admin(admin) =
        parse("show stats_buckets where db_name = 'test'").expect("parse")
    else {
        panic!("expected SHOW administrative envelope");
    };
    let tidb_ast::AdminStmt::ShowStatsBuckets(buckets) = admin.as_ref() else {
        panic!("expected typed SHOW STATS_BUCKETS");
    };
    assert!(matches!(
        &buckets.filter,
        Some(tidb_ast::ShowStatsBucketsFilter::Where(_))
    ));
    for sql in [
        "show stats_buckets like",
        "show stats_buckets where",
        "show stats_buckets like 'x%' where col_name = 'a'",
    ] {
        assert!(
            parse(sql).is_err(),
            "outside this STATS_BUCKETS leaf: {sql}"
        );
    }
}
