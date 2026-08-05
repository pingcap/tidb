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

//! WHICH JOIN ALGORITHM THE STATEMENT NAMED, and what that does to the
//! candidate list at one join site.
//!
//! # The Go this ports
//!
//! `LogicalJoin.SetPreferredJoinTypeAndOrder`
//! (`pkg/planner/core/operator/logicalop/logical_join.go:1596`) turns the
//! statement's `/*+ ... */` join hints into a `PreferJoinType` bitmask on each
//! `LogicalJoin`, by matching each hint's table list against the ALIAS of
//! either side:
//!
//! ```text
//! lhsAlias := util.ExtractTableAlias(p.Children()[0], p.QueryBlockOffset())
//! rhsAlias := util.ExtractTableAlias(p.Children()[1], p.QueryBlockOffset())
//! if hintInfo.IfPreferHashJoin(lhsAlias) { p.PreferJoinType |= utilhint.PreferHashJoin }
//! if hintInfo.IfPreferHashJoin(rhsAlias) { p.PreferJoinType |= utilhint.PreferHashJoin }
//! ```
//!
//! EITHER side matching is enough -- `TIDB_HJ(t1, t2)` and `TIDB_HJ(t2)` set
//! the same bit on the join over `t1` and `t2`. `ExtractTableAlias` answers
//! only for a subtree that exposes ONE relation, which is why a side reading
//! two tables is never a hint's target here.
//!
//! `exhaustPhysicalPlans4LogicalJoin` then reads that mask, and three of its
//! arms decide the WHOLE candidate list before any cost is computed:
//!
//! ```text
//! hashJoins, forced := getHashJoins(super, prop)
//! if forced && len(hashJoins) > 0 { return hashJoins, true, nil }
//! ...
//! mergeJoins := physicalop.GetMergeJoin(...)
//! if (p.PreferJoinType&h.PreferMergeJoin) > 0 && len(mergeJoins) > 0 {
//!     return mergeJoins, true, nil
//! }
//! ```
//!
//! and `handleForceIndexJoinHints`, which for a site with an `INL_JOIN`-family
//! hint returns `forced, true` -- the index-join candidates ALONE -- as soon as
//! one of them is valid.
//!
//! # Why this module exists, MEASURED
//!
//! It was found, not predicted. Growing the leaf's ordered-index enumeration
//! (`crate::driver::access::leaf_index_path`'s order request) added FOUR merge
//! joins the `join_shape` casetest counts as EXTRA -- merges TiDB does not
//! record -- and all four are `topn_push_down`'s
//!
//! ```sql
//! select /*+ TIDB_INLJ(t2) */ * from t t1 join t t2 on t1.a = t2.a limit 5;
//! select /*+ TIDB_HJ(t1, t2) */ * from t t1 join t t2 on t1.a = t2.a limit 5;
//! ```
//!
//! and their `left join ... where t2.a is null` variants. TiDB records an
//! `IndexJoin` and a `HashJoin` for them. It does not reach those plans by
//! COSTING the merge join and finding it dearer: the hint arms above return
//! before a merge candidate is ever built. The fifth statement of the same
//! group, `TIDB_SMJ(t1, t2)`, is the one TiDB DOES record as a `MergeJoin`
//! over two `IndexFullScan ... keep order:true` -- the same three statements
//! separated by nothing but their hint.
//!
//! # What this module answers, and what it does NOT
//!
//! One question: MAY a merge join be built at this site. The families this
//! tier chooses between afterwards are unchanged, and the index-join search in
//! [`super::join_search`] still asks Go's own enumeration.
//!
//! NAMED RESIDUE. `handleForceIndexJoinHints` drops the other families only
//! when an index-join candidate is VALID, and this tier decides the merge
//! before its children exist, so it cannot yet ask whether one is. The answer
//! here is therefore the fail-closed one: an `INL_JOIN`-family hint naming a
//! side drops the merge whether or not the index join then materialises. That
//! is Go's answer at every site of the enrolled replay -- verified by the
//! numbers, which do not move outside the four statements above -- and it is
//! the direction that cannot invent a merge join, only decline one.

use tidb_ast::{Hint, HintKind, SelectStmt};

/// The statement's join-method hints, reduced to the table lists the merge
/// decision reads.
///
/// Each list holds the LOWERCASED table names the hint named, in written
/// order. A hint with no tables (`HASH_JOIN()`) contributes an empty list and
/// therefore matches nothing, which is Go's `MatchTableName` over an empty
/// `PlanHints` slice.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct JoinMethodHints {
    /// `MERGE_JOIN` / `TIDB_SMJ` -- Go `PlanHints.SortMergeJoin`.
    merge: Vec<String>,
    /// `NO_MERGE_JOIN` -- Go `PlanHints.NoMergeJoin`.
    no_merge: Vec<String>,
    /// `HASH_JOIN` / `TIDB_HJ` -- Go `PlanHints.HashJoin`.
    hash: Vec<String>,
    /// `INL_JOIN` / `TIDB_INLJ` / `INL_HASH_JOIN` / `INL_MERGE_JOIN` -- Go
    /// `PlanHints.IndexJoin`'s three table lists, which
    /// `hasForceIndexJoinFamilyHint` reads as one.
    index: Vec<String>,
}

impl JoinMethodHints {
    /// The hints written on `select`'s own `/*+ ... */` comment.
    ///
    /// A hint this tier does not model as a join method contributes nothing,
    /// which is the same answer as no hint at all -- the safe direction, since
    /// every rule below only ever REMOVES a candidate.
    pub(crate) fn of_select(select: &SelectStmt) -> Self {
        let mut hints = Self::default();
        for hint in &select.hints {
            let Some(tables) = hinted_tables(hint) else {
                continue;
            };
            // Go lowercases every hint name in its own lexer, and matches the
            // table names case-insensitively through `MatchTableName`.
            let list = match hint.name.to_ascii_lowercase().as_str() {
                "merge_join" | "tidb_smj" => &mut hints.merge,
                "no_merge_join" => &mut hints.no_merge,
                "hash_join" | "tidb_hj" => &mut hints.hash,
                "inl_join" | "tidb_inlj" | "inl_hash_join" | "inl_merge_join" => &mut hints.index,
                _ => continue,
            };
            list.extend(tables);
        }
        hints
    }

    /// Whether this statement named any join method at all, which is Go's
    /// `hintInfo == nil` short circuit.
    pub(crate) fn is_empty(&self) -> bool {
        self.merge.is_empty()
            && self.no_merge.is_empty()
            && self.hash.is_empty()
            && self.index.is_empty()
    }

    /// Go's `MatchTableName`: does `list` name either side's alias.
    fn matches(list: &[String], sides: (Option<&str>, Option<&str>)) -> bool {
        [sides.0, sides.1]
            .into_iter()
            .flatten()
            .any(|alias| list.iter().any(|hinted| hinted.eq_ignore_ascii_case(alias)))
    }

    /// Whether a MERGE join may be built at a site whose two sides expose the
    /// aliases `sides` -- `None` for a side that exposes anything other than
    /// exactly one relation, which is Go's `ExtractTableAlias` answering nil.
    ///
    /// The order of the tests is Go's own order in
    /// `exhaustPhysicalPlans4LogicalJoin`, and it is load-bearing: a
    /// `MERGE_JOIN` hint wins over every other, because its arm returns the
    /// merge candidates before the rest of the function runs.
    /// `prop_is_empty` is `prop.IsSortItemEmpty()` for the property this join
    /// was asked to produce, which is the ONE input the hash arm needs beyond
    /// the names: `getHashJoins` opens with "hash join doesn't promise any
    /// orders" and returns nothing under a non-empty one, so `forced &&
    /// len(hashJoins) > 0` cannot fire there and the merge candidate survives
    /// its own hint.
    pub(crate) fn merge_join_allowed(
        &self,
        sides: (Option<&str>, Option<&str>),
        prop_is_empty: bool,
    ) -> bool {
        if Self::matches(&self.merge, sides) {
            // `if (p.PreferJoinType&h.PreferMergeJoin) > 0 && len(mergeJoins)
            // > 0 { return mergeJoins, true, nil }` -- and, one layer down,
            // `GetMergeJoin`'s own "Some MERGE_JOIN and NO_MERGE_JOIN hints
            // conflict, NO_MERGE_JOIN is ignored".
            return true;
        }
        if Self::matches(&self.no_merge, sides) {
            // `GetMergeJoin`: `if p.PreferJoinType&h.PreferNoMergeJoin > 0 {
            // if p.PreferJoinType&h.PreferMergeJoin == 0 { return nil } }`.
            return false;
        }
        if prop_is_empty && Self::matches(&self.hash, sides) {
            // `hashJoins, forced := getHashJoins(super, prop); if forced &&
            // len(hashJoins) > 0 { return hashJoins, true, nil }` -- the merge
            // candidates are never built.
            //
            // `getHashJoins` returns NOTHING under a non-empty property ("hash
            // join doesn't promise any orders"), so `len(hashJoins) > 0` fails
            // there and the merge survives -- which is what `prop_is_empty`
            // above carries.
            return false;
        }
        // `handleForceIndexJoinHints` returning `forced, true`. See the
        // module doc's NAMED RESIDUE for why this does not first check that an
        // index join is available.
        !Self::matches(&self.index, sides)
    }
}

/// Go's `util.ExtractTableAlias` for both children of `join`.
///
/// ```text
/// if len(p.OutputNames()) > 0 && p.OutputNames()[0].TblName.L != "" {
///     firstName := p.OutputNames()[0]
///     for _, name := range p.OutputNames() {
///         if name.TblName.L != firstName.TblName.L { return nil }
///     }
///     return &h.HintedTable{ TblName: firstName.TblName, ... }
/// }
/// return nil
/// ```
///
/// Every output name sharing ONE table name is exactly "this subtree exposes
/// one relation", which is what [`side_relations`] answers; a side reading two
/// tables is `nil` there and no hint can name it.
pub(crate) fn side_aliases(join: &tidb_ast::Join) -> (Option<String>, Option<String>) {
    let left = side_relations(&join.left);
    let right = join.right.as_ref().and_then(side_relations);
    (left, right)
}

/// The ONE relation a `FROM` subtree exposes, by the name a column reference
/// reaches it under, or `None` when it exposes any other number.
fn side_relations(node: &tidb_ast::JoinNode) -> Option<String> {
    match node {
        tidb_ast::JoinNode::Table(table_ref) => table_ref
            .alias
            .clone()
            .or_else(|| table_ref.name.last().cloned()),
        tidb_ast::JoinNode::Derived { alias, .. } => {
            alias.clone().filter(|alias| !alias.is_empty())
        }
        // A join exposes its two sides, so it is never one relation -- except
        // for the parser's single-relation wrapper, which IS its one child.
        tidb_ast::JoinNode::Join(inner) => match &inner.right {
            Some(_) => None,
            None => side_relations(&inner.left),
        },
    }
}

/// The tables one hint named, or `None` for a hint shape that names none.
fn hinted_tables(hint: &Hint) -> Option<Vec<String>> {
    match &hint.kind {
        HintKind::Tables { tables, .. } => Some(
            tables
                .iter()
                .filter(|table| !table.name.is_empty())
                .map(|table| table.name.to_ascii_lowercase())
                .collect(),
        ),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hints_of(sql: &str) -> JoinMethodHints {
        let tidb_ast::Stmt::Query(query) = tidb_parser::parse(sql).expect("parses") else {
            panic!("not a query");
        };
        let tidb_ast::QueryStmt::Select(select) = &*query else {
            panic!("not a select");
        };
        JoinMethodHints::of_select(select)
    }

    /// `TIDB_HJ` is Go's `HintHJFromMySQL`, the same list as `HASH_JOIN`, and
    /// a site EITHER of whose sides it names loses its merge candidate.
    #[test]
    fn a_hash_join_hint_naming_either_side_drops_the_merge() {
        let hints = hints_of("select /*+ TIDB_HJ(t1, t2) */ * from t t1 join t t2 on t1.a = t2.a");
        assert!(!hints.is_empty());
        assert!(!hints.merge_join_allowed((Some("t1"), Some("t2")), true));
        assert!(!hints.merge_join_allowed((Some("t1"), Some("t9")), true));
        assert!(!hints.merge_join_allowed((None, Some("t2")), true));
        // Under a NON-empty property `getHashJoins` produces nothing, so the
        // hint cannot short-circuit and the merge candidate stands.
        assert!(hints.merge_join_allowed((Some("t1"), Some("t2")), false));
        // A site neither side of which the hint names keeps its merge.
        assert!(hints.merge_join_allowed((Some("t8"), Some("t9")), true));
        // A side that exposes no single relation is Go's nil alias.
        assert!(hints.merge_join_allowed((None, None), true));
    }

    /// `TIDB_INLJ` names the INNER side, and `handleForceIndexJoinHints`
    /// returns the index-join candidates alone.
    #[test]
    fn an_index_join_hint_drops_the_merge() {
        let hints = hints_of("select /*+ TIDB_INLJ(t2) */ * from t t1 join t t2 on t1.a = t2.a");
        assert!(!hints.merge_join_allowed((Some("t1"), Some("t2")), true));
        assert!(hints.merge_join_allowed((Some("t1"), Some("t3")), true));
    }

    /// The merge hint is read FIRST, so it wins over every other -- including
    /// a `NO_MERGE_JOIN` on the same site, which Go answers with a warning and
    /// then ignores.
    #[test]
    fn a_merge_hint_outranks_the_others() {
        let hints = hints_of(
            "select /*+ TIDB_SMJ(t1, t2), TIDB_HJ(t1) */ * from t t1 join t t2 on t1.a = t2.a",
        );
        assert!(hints.merge_join_allowed((Some("t1"), Some("t2")), true));
        let hints = hints_of(
            "select /*+ MERGE_JOIN(t1), NO_MERGE_JOIN(t1) */ * from t t1 join t t2 on t1.a = t2.a",
        );
        assert!(hints.merge_join_allowed((Some("t1"), Some("t2")), true));
    }

    /// `NO_MERGE_JOIN` alone removes the candidate.
    #[test]
    fn a_no_merge_hint_removes_the_candidate() {
        let hints =
            hints_of("select /*+ NO_MERGE_JOIN(t2) */ * from t t1 join t t2 on t1.a = t2.a");
        assert!(!hints.merge_join_allowed((Some("t1"), Some("t2")), true));
    }

    /// A statement with no join-method hint decides nothing, which is what
    /// keeps this module off every site that does not write one.
    #[test]
    fn a_statement_without_join_hints_is_empty() {
        let hints = hints_of("select * from t t1 join t t2 on t1.a = t2.a");
        assert!(hints.is_empty());
        assert!(hints.merge_join_allowed((Some("t1"), Some("t2")), true));
        // A hint this module does not model as a join method is not one.
        let hints =
            hints_of("select /*+ USE_INDEX(t1, idx) */ * from t t1 join t t2 on t1.a = t2.a");
        assert!(hints.is_empty());
    }
}
