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

//! Go's predicate pushdown through `UNION ALL`, and the `TableDual` a branch
//! folds to when the pushed predicate lands on a projected constant.
//!
//! Three Go rules compose here, and all three had to be ported for the
//! recorded `explain_easy` plan to appear:
//!
//! * `LogicalUnionAll.PredicatePushDown` (`logical_union_all.go:45`) offers
//!   every predicate to EVERY term and keeps none itself.
//! * `LogicalProjection.PredicatePushDown` -> `breakDownPredicates` ->
//!   `ColumnSubstituteImpl` (`logical_projection.go:93,647`) rewrites the
//!   predicate through the term's own select list, so `c > 0` becomes
//!   `0 > 0` over a term projecting the literal `0`.
//! * `AddSelection` (`logical_plans_misc.go:85`) runs
//!   `shortCircuitLogicalConstants` (`rule_predicate_simplification.go:535`),
//!   which collapses a conjunct list to a single false predicate, and then
//!   `Conds2TableDual` (`expression_util.go:24`) replaces the child with a
//!   zero-row `LogicalTableDual` rather than a `Selection` admitting nothing.

#![cfg(test)]

use crate::tests_support::*;
use crate::*;

/// The plan rows of one statement as `|`-joined text.
fn plan(session: &mut Session, sql: &str) -> Vec<String> {
    row_text(session.run(sql))
        .into_iter()
        .map(|row| row.join("|"))
        .collect()
}

fn fixture() -> Session {
    let mut session = Session::new();
    session.run("create table u(a int, b int)").unwrap();
    session
}

/// The predicate reaches BOTH terms, so each one filters at its own leaf and
/// no `Selection` is left above the union.
#[test]
fn a_predicate_reaches_every_union_all_term() {
    let mut session = fixture();
    let rows = plan(
        &mut session,
        "explain select * from (select a, b from u union all select a, b from u) x where b > 0",
    );
    let text = rows.join("\n");
    assert!(
        rows.first().is_some_and(|row| row.starts_with("Union")),
        "the union is the top operator -- no Selection is left above it:\n{text}"
    );
    assert_eq!(
        text.matches("gt(test.u.b, 0)").count(),
        2,
        "both terms carry the pushed predicate at their own leaf:\n{text}"
    );
}

/// A term whose projection defines the column as a constant folds to Go's
/// zero-row `TableDual`, and the other term still filters normally.
#[test]
fn a_constant_term_folds_to_a_table_dual() {
    let mut session = fixture();
    let rows = plan(
        &mut session,
        "explain select * from (select a, 0 c from u union all select a, b from u) x where c > 0",
    );
    let text = rows.join("\n");
    assert!(
        text.contains("TableDual"),
        "`0 > 0` is const-false, so that term is a zero-row dual:\n{text}"
    );
    assert!(
        text.contains("gt(test.u.b, 0)"),
        "the other term still filters on its own column:\n{text}"
    );
}

/// Go's `Conds2TableDual` is not union-specific: a constant-false `WHERE`
/// plans as a dual wherever it appears, instead of a `Selection` over a scan
/// that admits nothing.
#[test]
fn a_constant_false_where_plans_as_a_dual() {
    let mut session = fixture();
    for sql in [
        "explain select * from u where 0",
        "explain select * from u where 1 > 2",
    ] {
        let text = plan(&mut session, sql).join("\n");
        assert!(
            text.contains("TableDual"),
            "`{sql}` admits nothing, so Go plans a dual:\n{text}"
        );
    }
}
