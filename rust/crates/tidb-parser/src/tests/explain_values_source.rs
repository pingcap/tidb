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
use tidb_ast::{QueryStmt, SelectStatementKind};

/// `tests/integrationtest/t/planner/core/plan.test:172` exercises Go's
/// parenthesized `VALUES` subquery through the EXPLAIN wrapper. The source
/// has two structural pairs, while `ast.SelectStmt.Restore` emits one pair
/// around the values query and folds its outer ORDER BY inside it.
#[test]
fn explain_parenthesized_values_restores_like_go() {
    let statement = parse("EXPLAIN FORMAT = TRADITIONAL ((VALUES ROW ()) ORDER BY 1)")
        .expect("parse EXPLAIN parenthesized VALUES");
    assert_eq!(
        statement.restore(),
        "EXPLAIN FORMAT = 'TRADITIONAL' (VALUES ROW() ORDER BY 1)"
    );

    let Stmt::Admin(admin) = statement else {
        panic!("expected EXPLAIN admin statement")
    };
    let AdminStmt::Explain(explain) = *admin else {
        panic!("expected typed EXPLAIN statement")
    };
    let Stmt::Query(query) = *explain.statement else {
        panic!("expected query target")
    };
    let QueryStmt::Select(values) = *query else {
        panic!("expected VALUES query target")
    };
    assert_eq!(values.kind, SelectStatementKind::Values);
    assert!(values.is_in_braces);
    assert_eq!(values.values, vec![Vec::<Expr>::new()]);
    assert_eq!(values.order_by.len(), 1);
}
