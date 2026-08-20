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

//! Real-TiDB parity for selectivity on tables that were never analyzed.
//!
//! `estRows` already matches TiDB wherever a histogram exists, so this file
//! builds the comparison where the gap was: **unanalyzed tables with
//! multi-condition predicates**, where every per-condition number comes from
//! the pseudo estimator and the only thing that decides the answer is how
//! `Selectivity` combines them.
//!
//! Every `TiDB` column below is a captured `EXPLAIN` `estRows` from a real
//! TiDB server (repo-root `gorun`, unistore) on
//!
//! ```sql
//! create table t(a int, b int, c varchar(32), d int unique, e int, f int);
//! ```
//!
//! with no `ANALYZE`, so the pseudo table row count is 10000 and every plan
//! reports `stats:pseudo`. The capture is the contract: these assertions are
//! pinned to TiDB's printed numbers, not to this port's own output.
//!
//! ```text
//! a = 1                                          10.00
//! a = 1 and b = 2                                 1.00
//! a = 1 and b = 2 and e = 3                       1.00
//! a in (1,2,3)                                   30.00
//! a in (1,2,3) and b in (4,5)                     1.00
//! a > 1                                        3333.33
//! a > 1 and b > 2                              1111.11
//! a between 1 and 5                             250.00
//! a between 1 and 5 and b between 2 and 9         6.25
//! c like 'a%'                                   250.00
//! a = 1 and c like 'a%'                           1.00
//! c like '%a%'                                 1000.00
//! c not like '%a%'                             9000.00
//! c like '%a%' and c like '%b%'                1000.00
//! a + b > 1                                    8000.00
//! a = 1 or b = 2                                 19.99
//! 64 x (a != k)                                8000.00
//! 64 x (a != k) and d = 7                         1.00
//! 64 x (a != k) and b > 3                      3333.33
//! ```
//!
//! The last three rows have 64 conditions, which is what pushes
//! `Selectivity` down the `len(exprs) > 63` arm into `pseudoSelectivity`
//! (`selectivity.go:69-73`); the rest exercise the node-product tail.

use tidb_planner::cardinality::pseudo::{
    pseudo_row_count_by_scalar_ranges, pseudo_selectivity, PseudoBoundKind, PseudoColumn,
    PseudoFunctionKind, PseudoIndex, PseudoPredicate, ScalarRange,
};
use tidb_planner::selectivity_greedy::{
    combine_selectivity, ConditionKind, SelectivityDefaults, StatsNode, StatsNodeType,
};

/// TiDB's pseudo table row count for a table with no statistics.
const PSEUDO_ROWS: f64 = 10000.0;

/// Formats like TiDB's `EXPLAIN` does, so the assertion compares the same
/// text the capture recorded.
fn explain_rows(rows: f64) -> String {
    format!("{rows:.2}")
}

fn point(value: f64) -> ScalarRange {
    ScalarRange::new(value, value, PseudoBoundKind::Value, PseudoBoundKind::Value)
}

fn closed(low: f64, high: f64) -> ScalarRange {
    ScalarRange::new(low, high, PseudoBoundKind::Value, PseudoBoundKind::Value)
}

fn greater_than(low: f64) -> ScalarRange {
    ScalarRange::new(
        low,
        0.0,
        PseudoBoundKind::Value,
        PseudoBoundKind::MaxValue,
    )
}

/// Selectivity of one column node, the way `Selectivity` builds it:
/// `GetRowCountByColumnRanges(...) / coll.RealtimeCount`
/// (`selectivity.go:130-136`).
fn column_node_selectivity(ranges: &[ScalarRange]) -> f64 {
    pseudo_row_count_by_scalar_ranges(ranges, PSEUDO_ROWS) / PSEUDO_ROWS
}

/// Runs the combination tail over `covered` column nodes plus `leftover`
/// conditions that no node covers, and returns `estRows`.
fn est_rows(covered: &[f64], leftover: &[ConditionKind]) -> String {
    let mut nodes: Vec<StatsNode> = covered
        .iter()
        .enumerate()
        .map(|(index, selectivity)| StatsNode {
            selectivity: *selectivity,
            ..StatsNode::new(
                StatsNodeType::Column,
                index as i64 + 1,
                1_i64 << index,
                1,
            )
        })
        .collect();
    let mut conditions = vec![ConditionKind::Other; covered.len()];
    conditions.extend_from_slice(leftover);
    let selectivity = combine_selectivity(
        &mut nodes,
        &conditions,
        1.0,
        PSEUDO_ROWS as i64,
        SelectivityDefaults::default(),
    );
    explain_rows(selectivity * PSEUDO_ROWS)
}

#[test]
fn unanalyzed_equality_conjunctions_match_tidb_est_rows() {
    let eq = column_node_selectivity(&[point(1.0)]);

    // Single equality is the control: one node, no combination at all.
    assert_eq!(est_rows(&[eq], &[]), "10.00", "a = 1");

    // Two and three ANDed equalities. 0.001^2 = 1e-6 is below the one-row
    // floor `max(ret, 1/RealtimeCount)` (`selectivity.go:428`), so both land
    // on exactly one row -- and 0.001^3 lands there too, which is why TiDB
    // prints the same 1.00 for both.
    assert_eq!(est_rows(&[eq, eq], &[]), "1.00", "a = 1 and b = 2");
    assert_eq!(
        est_rows(&[eq, eq, eq], &[]),
        "1.00",
        "a = 1 and b = 2 and e = 3"
    );
}

#[test]
fn unanalyzed_in_and_between_conjunctions_match_tidb_est_rows() {
    let in_three = column_node_selectivity(&[point(1.0), point(2.0), point(3.0)]);
    let in_two = column_node_selectivity(&[point(4.0), point(5.0)]);
    assert_eq!(est_rows(&[in_three], &[]), "30.00", "a in (1,2,3)");
    assert_eq!(
        est_rows(&[in_three, in_two], &[]),
        "1.00",
        "a in (1,2,3) and b in (4,5)"
    );

    let between_a = column_node_selectivity(&[closed(1.0, 5.0)]);
    let between_b = column_node_selectivity(&[closed(2.0, 9.0)]);
    assert_eq!(est_rows(&[between_a], &[]), "250.00", "a between 1 and 5");
    assert_eq!(
        est_rows(&[between_a, between_b], &[]),
        "6.25",
        "a between 1 and 5 and b between 2 and 9"
    );
}

#[test]
fn unanalyzed_open_range_conjunctions_match_tidb_est_rows() {
    let gt = column_node_selectivity(&[greater_than(1.0)]);
    assert_eq!(est_rows(&[gt], &[]), "3333.33", "a > 1");
    // The product loop is the only thing separating 3333.33 from 1111.11.
    assert_eq!(est_rows(&[gt, gt], &[]), "1111.11", "a > 1 and b > 2");
}

#[test]
fn unanalyzed_string_matches_take_the_default_selectivities() {
    // `c like 'a%'` is a *prefix* match, so ranger builds a bounded column
    // range and it becomes an ordinary between-rate node, not a leftover.
    let prefix_like = column_node_selectivity(&[closed(0.0, 1.0)]);
    assert_eq!(est_rows(&[prefix_like], &[]), "250.00", "c like 'a%'");

    let eq = column_node_selectivity(&[point(1.0)]);
    assert_eq!(
        est_rows(&[eq, prefix_like], &[]),
        "1.00",
        "a = 1 and c like 'a%'"
    );

    // A non-prefix `LIKE` builds no range and reaches the leftover block,
    // where it takes `GetStrMatchDefaultSelectivity()` = 0.1. If that default
    // were the general 0.8 factor this would print 8000.00.
    assert_eq!(
        est_rows(&[], &[ConditionKind::StringMatch(None)]),
        "1000.00",
        "c like '%a%'"
    );
    assert_eq!(
        est_rows(&[], &[ConditionKind::NegatedStringMatch(None)]),
        "9000.00",
        "c not like '%a%'"
    );

    // The leftover block charges `minSelectivity` ONCE for the whole
    // remaining mask (`selectivity.go:414-427`), not once per condition:
    // two non-prefix LIKEs still print 1000.00, not 100.00.
    assert_eq!(
        est_rows(
            &[],
            &[
                ConditionKind::StringMatch(None),
                ConditionKind::StringMatch(None)
            ]
        ),
        "1000.00",
        "c like '%a%' and c like '%b%'"
    );

    // An equality node plus one leftover string match: 0.001 * 0.1.
    assert_eq!(
        est_rows(&[eq], &[ConditionKind::StringMatch(None)]),
        "1.00",
        "a = 1 and c like '%a%'"
    );
}

#[test]
fn unanalyzed_opaque_expression_takes_the_selection_factor() {
    // `a + b > 1` builds no range for either column, so it is a leftover
    // `Other` and takes `SelectivityFactor` = 0.8.
    assert_eq!(
        est_rows(&[], &[ConditionKind::Other]),
        "8000.00",
        "a + b > 1"
    );
}

#[test]
fn unanalyzed_disjunction_uses_the_independence_formula() {
    let eq = column_node_selectivity(&[point(1.0)]);
    // `sel(A or B) = sel(A) + sel(B) - sel(A)*sel(B)` (`selectivity.go:331`).
    let dnf = eq + eq - eq * eq;
    assert_eq!(est_rows(&[dnf], &[]), "19.99", "a = 1 or b = 2");
}

/// Lowercased column names for the `pseudoSelectivity` fixtures.
fn plain_column(name: &str) -> PseudoColumn {
    PseudoColumn {
        lower_name: name.to_owned(),
        unique_key_flag: false,
    }
}

fn unique_column(name: &str) -> PseudoColumn {
    PseudoColumn {
        lower_name: name.to_owned(),
        unique_key_flag: true,
    }
}

fn resolved(kind: PseudoFunctionKind, column: PseudoColumn) -> PseudoPredicate {
    PseudoPredicate::Resolved {
        kind,
        column: Some(column),
    }
}

fn pseudo_est_rows(predicates: &[PseudoPredicate], indexes: &[PseudoIndex]) -> String {
    explain_rows(pseudo_selectivity(predicates, indexes, PSEUDO_ROWS as i64, 0.8) * PSEUDO_ROWS)
}

#[test]
fn more_than_63_conditions_take_pseudo_selectivity() {
    // 64 x `a != k`. `ne` is in neither switch arm, so `minFactor` never
    // moves off `SelectivityFactor` and `colExists` stays empty.
    let not_equal: Vec<PseudoPredicate> = (0..64)
        .map(|_| resolved(PseudoFunctionKind::Other, plain_column("a")))
        .collect();
    assert_eq!(pseudo_est_rows(&not_equal, &[]), "8000.00", "64 x a != k");

    // Adding one ordering predicate drops `minFactor` to 1/pseudoLessRate.
    let mut with_ordering = not_equal.clone();
    with_ordering.push(resolved(PseudoFunctionKind::Ordering, plain_column("b")));
    assert_eq!(
        pseudo_est_rows(&with_ordering, &[]),
        "3333.33",
        "64 x a != k and b > 3"
    );

    // Adding an equality on a UNIQUE column returns 1/RealtimeCount
    // immediately, discarding all 64 other conditions (`pseudo.go:60-62`).
    let mut with_unique = not_equal.clone();
    with_unique.push(resolved(PseudoFunctionKind::Equality, unique_column("d")));
    assert_eq!(
        pseudo_est_rows(&with_unique, &[]),
        "1.00",
        "64 x a != k and d = 7"
    );

    // Order does not matter: the shortcut fires wherever the unique equality
    // sits, so a later ordering predicate cannot raise the estimate back.
    let mut unique_first = vec![resolved(PseudoFunctionKind::Equality, unique_column("d"))];
    unique_first.extend(with_ordering.clone());
    assert_eq!(pseudo_est_rows(&unique_first, &[]), "1.00", "unique first");
}

#[test]
fn pseudo_selectivity_equality_and_index_arms() {
    // A plain equality with no unique flag: `minFactor` = 1/1000.
    let equality = [resolved(PseudoFunctionKind::Equality, plain_column("a"))];
    assert_eq!(pseudo_est_rows(&equality, &[]), "10.00", "a = 1");

    // An `Unresolved` predicate is skipped before the switch, so it cannot
    // lower `minFactor` even though it is a real condition.
    let unresolved = [PseudoPredicate::Unresolved, PseudoPredicate::Unresolved];
    assert_eq!(pseudo_est_rows(&unresolved, &[]), "8000.00", "unresolved");

    // An equality whose column has no statistics entry still charges 1/1000
    // -- the source updates `minFactor` before the nil check
    // (`pseudo.go:55-58`) -- but contributes nothing to `colExists`.
    let missing = [PseudoPredicate::Resolved {
        kind: PseudoFunctionKind::Equality,
        column: None,
    }];
    assert_eq!(pseudo_est_rows(&missing, &[]), "10.00", "missing column");

    // A composite UNIQUE index needs EVERY one of its columns in `colExists`.
    let composite = PseudoIndex {
        unique: true,
        column_lower_names: vec!["a".to_owned(), "b".to_owned()],
    };
    let only_a = [resolved(PseudoFunctionKind::Equality, plain_column("a"))];
    assert_eq!(
        pseudo_est_rows(&only_a, std::slice::from_ref(&composite)),
        "10.00",
        "prefix of a composite unique index is not enough"
    );

    let both = [
        resolved(PseudoFunctionKind::Equality, plain_column("a")),
        resolved(PseudoFunctionKind::Equality, plain_column("b")),
    ];
    assert_eq!(
        pseudo_est_rows(&both, std::slice::from_ref(&composite)),
        "1.00",
        "full cover of a composite unique index"
    );

    // A non-unique index covering the same columns changes nothing.
    let non_unique = PseudoIndex {
        unique: false,
        column_lower_names: vec!["a".to_owned(), "b".to_owned()],
    };
    assert_eq!(
        pseudo_est_rows(&both, &[non_unique]),
        "10.00",
        "non-unique index is not a shortcut"
    );
}

#[test]
fn string_match_defaults_follow_the_session_variable() {
    // The shipped value is 0 -> 0.1 / 0.9.
    let shipped = SelectivityDefaults::default();
    assert!((shipped.str_match_default - 0.1).abs() < f64::EPSILON);
    assert!((shipped.negate_str_match_default - 0.9).abs() < f64::EPSILON);

    // 0.8 is the backward-compatibility sentinel: BOTH sides stay 0.8 rather
    // than the negated side becoming 0.2 (`session.go:3688-3692`).
    let legacy = SelectivityDefaults::from_session(0.8, 0.8);
    assert!((legacy.str_match_default - 0.8).abs() < f64::EPSILON);
    assert!((legacy.negate_str_match_default - 0.8).abs() < f64::EPSILON);

    // Any other explicit value negates normally.
    let explicit = SelectivityDefaults::from_session(0.25, 0.8);
    assert!((explicit.str_match_default - 0.25).abs() < f64::EPSILON);
    assert!((explicit.negate_str_match_default - 0.75).abs() < f64::EPSILON);
}
