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

//! Execution boundary for scalar subqueries with set-operation bodies.

use super::*;

#[test]
fn scalar_union_subquery_dispatches_through_query_rows() {
    // `UNION` deduplicates the two constant rows, leaving one scalar value.
    // This proves the executor dispatches the widened `QueryStmt::SetOpr`
    // scalar body rather than rejecting it at the old `SelectStmt` seam.
    let mut db = Database::new();
    step(&mut db, "create table scalar_union_outer (id int)");
    step(&mut db, "insert into scalar_union_outer values (1)");
    assert_eq!(
        step(
            &mut db,
            "select (select 1 union select 1) from scalar_union_outer",
        ),
        "RS:1"
    );
}
