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

use super::*;

/// The Go parser's `parseSubquery`/`maybeParseUnion` pair keeps a leading
/// parenthesized set operation as one `SetOprStmt` with `IsInBraces=true`.
/// Its trailing statement-level ORDER BY is consequently restored before the
/// closing parenthesis, rather than after the whole wrapper.
#[test]
fn parenthesized_setopr_keeps_statement_wrapper_and_tail() {
    let statement = parse("(select 1 union select 2) order by 1").expect("parse");
    let restored = statement.restore();
    let Stmt::Query(query) = statement else {
        panic!("expected query statement")
    };
    let tidb_ast::QueryStmt::SetOpr(setopr) = query.into_inner() else {
        panic!("expected set operation")
    };
    assert!(setopr.is_in_braces);
    assert_eq!(setopr.terms.len(), 2);
    assert_eq!(setopr.order_by.len(), 1);
    assert_eq!(setopr.limit, None);
    assert_eq!(restored, "(SELECT 1 UNION SELECT 2 ORDER BY 1)");
}

#[test]
fn parenthesized_setopr_without_outer_tail_restores_losslessly() {
    assert_eq!(
        r("(select 1 union all select 2)"),
        "(SELECT 1 UNION ALL SELECT 2)"
    );
}
