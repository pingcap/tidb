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
use tidb_ast::{Expr, QueryStmt, Stmt};

/// Go's `parseExistsSubquery` accepts a top-level set operation in the
/// `EXISTS` body. Keep that shape typed as `QueryStmt::SetOpr` instead of
/// flattening it into the first `SelectStmt` and silently dropping rows.
#[test]
fn exists_set_operation_restores_like_go() {
    let statement = parse(
        "select * from (select (92 / 4) as c4) as subq_0 where exists (\nselect 1 as c0\nunion all\nselect 1 as c0 from (t0 as ref_88) where (subq_0.c4) >= (subq_0.c4)\n)",
    )
    .expect("parse EXISTS set operation");
    assert_eq!(
        statement.restore(),
        "SELECT * FROM (SELECT (92/4) AS `c4`) AS `subq_0` WHERE EXISTS (SELECT 1 AS `c0` UNION ALL SELECT 1 AS `c0` FROM `t0` AS `ref_88` WHERE (`subq_0`.`c4`)>=(`subq_0`.`c4`))"
    );

    let Stmt::Query(query) = statement else {
        panic!("expected query statement")
    };
    let QueryStmt::Select(select) = query.into_inner() else {
        panic!("expected outer SELECT")
    };
    let Some(Expr::Exists { subquery, .. }) = select.where_clause else {
        panic!("expected EXISTS predicate")
    };
    assert!(matches!(*subquery, QueryStmt::SetOpr(_)));
}
