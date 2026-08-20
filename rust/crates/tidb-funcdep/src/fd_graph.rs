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

//! Go `pkg/planner/funcdep/fd_graph.go`: the functional-dependency graph a
//! query block's columns form, and the closure computation that answers "does
//! this set of columns determine that one".
//!
//! A functional dependency `{a} --> {b}` says two rows agreeing on `a` agree
//! on `b`. The graph stores four kinds of edge, distinguished by two flags:
//!
//!  * STRICT `{a} --> {b}`: `a` determines `b` outright.
//!  * LAX `{a} ~~> {b}`: `a` determines `b` EXCEPT that NULL determinants may
//!    repeat freely. This is what a nullable UNIQUE key gives -- `UNIQUE(a)`
//!    permits many rows with `a IS NULL`, each with its own `b`. A lax edge
//!    becomes strict the moment its determinant is known NOT NULL, which is
//!    [`FdSet::make_not_null`] and is why a `WHERE` that proves `a` non-null
//!    promotes the key.
//!  * EQUIVALENCE `{a,b} == {a,b}`: mutual determination, stored as one
//!    superset edge pointing at itself since equivalence is reflexive and
//!    transitive.
//!  * CONSTANT `{} --> {a}`: an edge with an empty determinant.
//!
//! Lax edges are NOT transitive (a NULL in the middle breaks the chain), which
//! is why [`FdSet::closure_of_lax`] takes exactly one lax step, while
//! [`FdSet::closure_of_strict`] iterates to a fixed point.
//!
//! The set keeps itself minimal on every insert: a new edge that implies an
//! existing one replaces it, an implied new edge is dropped, and constants and
//! equivalences are propagated into the edges already present. That
//! normalization is not an optimization -- Go's own test table asserts on the
//! resulting edge list, so the shrinking rules are behavior.

use super::ColSet;

/// One dependency edge. `strict`/`equiv` together name the four kinds
/// described in the module doc.
#[derive(Clone, Debug)]
struct FdEdge {
    /// The determinants.
    from: ColSet,
    /// The dependencies.
    to: ColSet,
    strict: bool,
    equiv: bool,
}

/// A dependency hidden by an outer join until a later predicate rejects the
/// NULL-extended rows. This is Go `FDSet.ncEdges`: the dependency itself is
/// ordinary, but it becomes visible only when `condition` contains a column
/// proved non-null above the join.
#[derive(Clone, Debug)]
struct ConditionalFd {
    edge: FdEdge,
    condition: ColSet,
}

impl FdEdge {
    /// Go `fdEdge.isConstant`: `{} --> {...}`.
    fn is_constant(&self) -> bool {
        self.from.is_empty()
    }

    /// Go `fdEdge.isEquivalence`: `{xyz} == {xyz}`.
    #[cfg(test)]
    fn is_equivalence(&self) -> bool {
        self.equiv && self.from == self.to
    }

    /// Go `fdEdge.implies`: whether this edge is at least as strong as
    /// `other`, which is how the set stays minimal.
    ///
    /// The lax/lax case is special. `A ~~> C` IS stronger than `AB ~~> C` (a
    /// smaller determinant), but `A ~~> BC` is NOT stronger than `A ~~> C`:
    /// promoting the second to strict only needs `A` non-null, while the first
    /// needs `A` and `B` non-null. So two lax edges may only absorb each other
    /// when their dependency sides are identical.
    fn implies(&self, other: &FdEdge) -> bool {
        let self_is_lax = !self.equiv && !self.strict;
        let other_is_lax = !other.equiv && !other.strict;
        if self_is_lax && other_is_lax {
            return self.from.subset_of(&other.from) && self.to == other.to;
        }
        if self.from.subset_of(&other.from) && other.to.subset_of(&self.to) {
            return (self.strict || !other.strict) && (self.equiv || !other.equiv);
        }
        false
    }

    /// Go `fdEdge.removeColumnsFromSide`: drops constant columns from the
    /// determinant of a STRICT edge (a constant tells the determinant nothing
    /// apart), returning whether the edge became a constant one.
    ///
    /// This must not be applied to a lax edge: a lax determinant may be NULL,
    /// and dropping a column from it would claim a determination the NULL rows
    /// do not have.
    fn remove_columns_from_side(&mut self, constants: &ColSet) -> bool {
        if self.from.intersects(constants) {
            self.from = self.from.difference(constants);
        }
        self.is_constant()
    }

    /// Go `fdEdge.removeColumnsToSide`: drops constant columns from the
    /// dependency side, returning whether nothing is left to determine.
    fn remove_columns_to_side(&mut self, constants: &ColSet) -> bool {
        if self.to.intersects(constants) {
            self.to = self.to.difference(constants);
        }
        self.to.is_empty()
    }
}

impl std::fmt::Display for FdEdge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let arrow = match (self.equiv, self.strict) {
            (true, _) => "==",
            (false, true) => "-->",
            (false, false) => "~~>",
        };
        write!(f, "{}{}{}", self.from, arrow, self.to)
    }
}

/// The functional dependencies holding over one relation's columns.
#[derive(Clone, Default, Debug)]
pub struct FdSet {
    edges: Vec<FdEdge>,
    conditional_edges: Vec<ConditionalFd>,
    /// Go `FDSet.NotNullCols`: the columns known never to be NULL, kept so a
    /// lax edge added LATER can still be promoted.
    not_null_cols: ColSet,
}

/// The three source decisions consumed by Go `FDSet.MakeOuterJoin`.
#[derive(Clone, Copy, Default, Debug)]
pub struct OuterJoinOptions {
    /// Go `ArgOpts.SkipFDRule331`: the join carries no cross-side equality, or
    /// a filter reads a preserved-side column outside the equated set, so the
    /// combined `outer --> inner` edge rule 3.3.1 would not hold.
    pub skip_rule_331: bool,
    /// Go `ArgOpts.OnlyInnerFilter`: every `ON` conjunct reads only
    /// NULL-supplying-side columns, so the filter behaves as a selection
    /// pushed under the join.
    pub only_inner_filter: bool,
    /// Go `ArgOpts.InnerIsFalse`: that inner-only filter is a constant FALSE,
    /// which fixes every NULL-supplying column to the NULL constant.
    pub inner_is_false: bool,
}

impl FdSet {
    /// An empty set: no edges, nothing known NOT NULL.
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `FDSet.closureOfStrict`: every column `cols` determines through
    /// strict and equivalence edges, iterated to a fixed point.
    ///
    /// The equivalence case tests INTERSECTION rather than subset: an
    /// equivalence is stored as one superset edge `{a,b,c} == {a,b,c}`, and
    /// reaching any one member must reach the rest.
    pub fn closure_of_strict(&self, cols: &ColSet) -> ColSet {
        let mut result = cols.clone();
        let mut index = 0usize;
        while index < self.edges.len() {
            let edge = &self.edges[index];
            if edge.strict && edge.from.subset_of(&result) && !edge.to.subset_of(&result) {
                result.union_with(&edge.to);
                index = 0;
                continue;
            }
            if edge.equiv && edge.from.intersects(&result) && !edge.to.subset_of(&result) {
                result.union_with(&edge.to);
                index = 0;
                continue;
            }
            index += 1;
        }
        result
    }

    /// Go `FDSet.closureOfLax`: the strict closure, widened by ONE lax step.
    ///
    /// Lax dependencies do not compose -- `a ~~> b` and `b ~~> c` do not give
    /// `a ~~> c`, because the rows where `b` is NULL are unconstrained -- so
    /// this deliberately does not iterate lax edges, only the equivalences
    /// that widen which lax edges are reachable at all.
    pub fn closure_of_lax(&self, cols: &ColSet) -> ColSet {
        let mut reached = cols.clone();
        let mut index = 0usize;
        while index < self.edges.len() {
            let edge = &self.edges[index];
            if edge.equiv && edge.from.intersects(&reached) && !edge.to.subset_of(&reached) {
                reached.union_with(&edge.to);
                index = 0;
                continue;
            }
            if !edge.strict
                && !edge.equiv
                && edge.from.subset_of(&reached)
                && !edge.to.subset_of(&reached)
            {
                reached.union_with(&edge.to);
            }
            index += 1;
        }
        reached.union_with(&self.closure_of_strict(cols));
        reached
    }

    /// Go `FDSet.ClosureOfEquivalence`: the equivalence classes `cols` touches.
    fn closure_of_equivalence(&self, cols: &ColSet) -> ColSet {
        let mut result = cols.clone();
        for edge in &self.edges {
            if edge.equiv && edge.from.intersects(&result) && !edge.to.subset_of(&result) {
                result.union_with(&edge.to);
            }
        }
        result
    }

    /// Go `FDSet.InClosure`: whether `set_b` is inferable from `set_a`. A
    /// short-circuiting [`Self::closure_of_strict`].
    fn in_closure(&self, set_a: &ColSet, set_b: &ColSet) -> bool {
        if set_b.subset_of(set_a) {
            return true;
        }
        let mut closure = set_a.clone();
        let mut index = 0usize;
        while index < self.edges.len() {
            let edge = &self.edges[index];
            if edge.strict && edge.from.subset_of(&closure) && !edge.to.subset_of(&closure) {
                closure.union_with(&edge.to);
                if set_b.subset_of(&closure) {
                    return true;
                }
                index = 0;
                continue;
            }
            if edge.equiv && edge.from.intersects(&closure) && !edge.to.subset_of(&closure) {
                closure.union_with(&edge.to);
                if set_b.subset_of(&closure) {
                    return true;
                }
                index = 0;
                continue;
            }
            index += 1;
        }
        false
    }

    /// Go `FDSet.ReduceCols`: the minimal determinant equivalent to `cols`.
    ///
    /// Given `A --> B`, the determinant `{A,B}` says no more than `{A}`, so
    /// `B` is dropped. Members are tried in order and put back when the rest
    /// cannot derive them.
    pub fn reduce_cols(&self, cols: &ColSet) -> ColSet {
        let mut removed = ColSet::default();
        let mut result = cols.clone();
        for value in cols.iter() {
            removed.insert(value);
            result.remove(value);
            if !self.in_closure(&result, &removed) {
                removed.remove(value);
                result.insert(value);
            }
        }
        result
    }

    /// Go `FDSet.AddStrictFunctionalDependency`.
    pub fn add_strict(&mut self, from: ColSet, to: ColSet) {
        self.add_functional_dependency(from, to, true, false);
    }

    /// Go `FDSet.AddLaxFunctionalDependency`.
    pub fn add_lax(&mut self, from: ColSet, to: ColSet) {
        self.add_functional_dependency(from, to, false, false);
    }

    /// Go `FDSet.addFunctionalDependency`: insert an edge, keeping the set
    /// minimal.
    ///
    /// A trivial edge is refused, the determinant is reduced, and then the new
    /// edge either replaces the existing edges it implies, is swallowed by an
    /// existing stronger one, or merges into an existing strict edge with the
    /// same determinant (`A --> B` plus `A --> C` is `A --> BC`).
    fn add_functional_dependency(
        &mut self,
        from: ColSet,
        mut to: ColSet,
        strict: bool,
        equiv: bool,
    ) {
        if to.subset_of(&from) {
            return;
        }
        if to.intersects(&from) {
            to.difference_with(&from);
        }
        let from = self.reduce_cols(&from);
        let new_edge = FdEdge {
            from: from.clone(),
            to: to.clone(),
            strict,
            equiv,
        };

        let mut kept: Vec<FdEdge> = Vec::with_capacity(self.edges.len() + 1);
        let mut added = false;
        for mut edge in std::mem::take(&mut self.edges) {
            if new_edge.implies(&edge) {
                if !added {
                    edge = new_edge.clone();
                    added = true;
                }
                // A later edge the new one also implies is dropped: the
                // replacement above already stands for it.
                else {
                    continue;
                }
            } else if !added {
                if edge.implies(&new_edge) {
                    added = true;
                } else if edge.strict && !edge.equiv && edge.from == from {
                    edge.to.union_with(&to);
                    added = true;
                }
            }
            kept.push(edge);
        }
        self.edges = kept;
        if !added {
            self.edges.push(new_edge);
        }
    }

    /// Go `FDSet.addEquivalence`: merge `cols` into the equivalence closure it
    /// touches, then let that closure simplify the edges already present.
    fn add_equivalence_closure(&mut self, cols: ColSet) {
        let equiv_closure = self.closure_of_equivalence(&cols);
        self.edges.push(FdEdge {
            from: equiv_closure.clone(),
            to: equiv_closure.clone(),
            strict: true,
            equiv: true,
        });

        let mut add_const = false;
        // Every edge but the one just appended.
        let mut index = 0usize;
        while index + 1 < self.edges.len() {
            let edge = &mut self.edges[index];
            let mut drop_edge = false;
            if edge.is_constant() {
                // One member of an equivalence class being constant makes all
                // of them constant.
                if edge.to.intersects(&equiv_closure) && !equiv_closure.subset_of(&edge.to) {
                    add_const = true;
                }
            } else if edge.from.subset_of(&equiv_closure) {
                // An equivalence edge is subsumed by the superset edge
                // appended above; a plain dependency loses whatever of its
                // dependency side the closure already covers, since
                // equivalence is the stronger relationship.
                drop_edge = edge.equiv || edge.remove_columns_to_side(&equiv_closure);
            }
            if drop_edge {
                self.edges.remove(index);
                continue;
            }
            index += 1;
        }
        if add_const {
            self.add_constants(equiv_closure.clone());
        }
        // A new class inherits the not-null attribute of the class it merged
        // with, which may promote lax edges.
        if self.not_null_cols.intersects(&equiv_closure) {
            self.make_not_null(equiv_closure);
        }
    }

    /// Go `FDSet.AddEquivalence`: `from` and `to` denote the same value.
    pub fn add_equivalence(&mut self, from: ColSet, to: ColSet) {
        if to.subset_of(&from) {
            return;
        }
        self.add_equivalence_closure(from.union(&to));
    }

    /// Go `FDSet.AddConstants`: every listed column holds one value for all
    /// rows, which propagates through the strict closure and then simplifies
    /// the determinant of every strict edge and the dependency side of every
    /// edge.
    pub fn add_constants(&mut self, constants: ColSet) {
        if constants.is_empty() {
            return;
        }
        let cols = self.closure_of_strict(&constants);
        self.edges.push(FdEdge {
            from: ColSet::default(),
            to: cols.clone(),
            strict: true,
            equiv: false,
        });

        // Every edge but the one just appended.
        let mut index = 0usize;
        while index + 1 < self.edges.len() {
            let edge = &mut self.edges[index];
            let mut should_remove = false;
            if !edge.equiv {
                // Both sides are simplified: a strict edge whose determinant
                // is entirely constant determines only constants, and any
                // edge loses the constants from its dependency side.
                if edge.strict && edge.remove_columns_from_side(&cols) {
                    should_remove = true;
                }
                if edge.remove_columns_to_side(&cols) {
                    should_remove = true;
                }
            }
            if should_remove {
                self.edges.remove(index);
                continue;
            }
            index += 1;
        }
    }

    /// Go `FDSet.ConstantCols`: the columns fixed to one value.
    pub fn constant_cols(&self) -> ColSet {
        self.edges
            .iter()
            .find(|edge| edge.is_constant())
            .map(|edge| edge.to.clone())
            .unwrap_or_default()
    }

    /// Go `FDSet.EquivalenceCols`: one set per equivalence class.
    #[cfg(test)]
    fn equivalence_cols(&self) -> Vec<&ColSet> {
        self.edges
            .iter()
            .filter(|edge| edge.is_equivalence())
            .map(|edge| &edge.from)
            .collect()
    }

    /// Go `FDSet.MakeNotNull`: record that `not_null_cols` can never be NULL,
    /// and promote every lax edge whose determinant that covers.
    ///
    /// This is the whole reason a `WHERE` matters to this rule: a nullable
    /// `UNIQUE(a,b)` contributes only `{a,b} ~~> {rest}`, and a predicate
    /// proving `a` non-null turns it into `{a,b} --> {rest}`, which makes
    /// `GROUP BY a,b` a candidate key.
    pub fn make_not_null(&mut self, not_null_cols: ColSet) {
        let mut not_null_cols = not_null_cols;
        not_null_cols.union_with(&self.not_null_cols);
        let mut not_null_set = self.closure_of_equivalence(&not_null_cols);

        // Go `ncEdges`: an outer join hides a constant/equivalence until an
        // upper predicate rejects at least one NULL-extended column. Waking
        // an equivalence can widen the not-null class and wake another edge,
        // so restart after every such widening.
        let mut index = 0usize;
        while index < self.conditional_edges.len() {
            if !self.conditional_edges[index]
                .condition
                .intersects(&not_null_set)
            {
                index += 1;
                continue;
            }
            let conditional = self.conditional_edges.remove(index);
            let edge = conditional.edge;
            if edge.is_constant() {
                self.add_constants(edge.to);
            } else if edge.equiv {
                self.add_equivalence(edge.from, edge.to);
                let widened = self.closure_of_equivalence(&not_null_set);
                if widened != not_null_set {
                    not_null_set = widened;
                    index = 0;
                }
            } else {
                self.add_functional_dependency(edge.from, edge.to, edge.strict, false);
            }
        }

        let mut index = 0usize;
        while index < self.edges.len() {
            let edge = &self.edges[index];
            if edge.strict {
                index += 1;
                continue;
            }
            if edge.from.subset_of(&not_null_set) {
                let (from, to) = (edge.from.clone(), edge.to.clone());
                // Adding the strict form rebuilds the edge list (the lax one
                // is implied by it and drops out), so restart the scan.
                self.add_strict(from, to);
                index = 0;
                continue;
            }
            index += 1;
        }
        self.not_null_cols = not_null_set;
    }

    /// Go `FDSet.MakeCartesianProduct`: the dependencies of `T1 x T2`.
    ///
    /// Two independent relations cannot imply anything about each other, so
    /// the edges simply concatenate -- except constants, which must merge into
    /// the single constant edge this representation keeps.
    pub fn make_cartesian_product(&mut self, rhs: &FdSet) {
        for edge in &rhs.edges {
            if edge.is_constant() {
                self.add_constants(edge.to.clone());
            } else {
                self.edges.push(edge.clone());
            }
        }
        self.conditional_edges
            .extend(rhs.conditional_edges.iter().cloned());
        self.not_null_cols.union_with(&rhs.not_null_cols);
    }

    /// Every column named by an ordinary edge. This mirrors Go `AllCols`;
    /// equivalence edges name their set once, while all other edges contribute
    /// both sides.
    fn all_cols(&self) -> ColSet {
        let mut cols = ColSet::default();
        for edge in &self.edges {
            cols.union_with(&edge.from);
            if !edge.equiv {
                cols.union_with(&edge.to);
            }
        }
        cols
    }

    /// Go `FindPrimaryKey`: a strict determinant whose closure covers every
    /// column currently represented by the relation.
    pub fn primary_key(&self) -> Option<ColSet> {
        let all_cols = self.all_cols();
        self.edges
            .iter()
            .filter(|edge| edge.strict && !edge.equiv)
            .find(|edge| all_cols.subset_of(&self.closure_of_strict(&edge.from)))
            .map(|edge| edge.from.clone())
    }

    fn add_conditional(
        &mut self,
        from: ColSet,
        to: ColSet,
        condition: ColSet,
        strict: bool,
        equiv: bool,
    ) {
        self.conditional_edges.push(ConditionalFd {
            edge: FdEdge {
                from,
                to,
                strict,
                equiv,
            },
            condition,
        });
    }

    /// Go `FDSet.MakeOuterJoin`, with `self` as the row-preserving side and
    /// `inner` as the NULL-supplying side.
    ///
    /// The shape is deliberately the source algebra rather than a list of
    /// query exceptions: preserved-side dependencies survive; nullable-side
    /// strict dependencies weaken unless their determinant contains a
    /// declared non-null column; join equivalences become directional; and
    /// dependencies requiring NULL rejection stay hidden until a later
    /// selection wakes them.
    pub fn make_outer_join(
        &mut self,
        inner: &FdSet,
        filter: &FdSet,
        outer_cols: &ColSet,
        inner_cols: &ColSet,
        options: OuterJoinOptions,
    ) {
        let left_key = self.primary_key();
        let right_key = inner.primary_key();
        let original_left = self.clone();
        let original_right = inner.clone();

        for edge in &inner.edges {
            if edge.is_constant() || edge.equiv {
                continue;
            }
            if !edge.strict || edge.from.intersects(&inner.not_null_cols) {
                self.add_functional_dependency(
                    edge.from.clone(),
                    edge.to.clone(),
                    edge.strict,
                    false,
                );
            } else {
                self.add_lax(edge.from.clone(), edge.to.clone());
            }
        }
        self.conditional_edges
            .extend(inner.conditional_edges.iter().cloned());

        let mut combined_from = ColSet::default();
        let mut combined_to = ColSet::default();
        for edge in &filter.edges {
            if edge.is_constant() {
                self.add_conditional(
                    edge.from.clone(),
                    edge.to.clone(),
                    inner_cols.clone(),
                    edge.strict,
                    edge.equiv,
                );
                continue;
            }
            if !edge.equiv {
                // An outer-join filter does not create an unconditional
                // strict/lax dependency of its own.
                continue;
            }

            let right_equiv = edge.from.intersection(inner_cols);
            let left_equiv = edge.from.intersection(outer_cols);

            if !options.skip_rule_331 && !left_equiv.is_empty() && !right_equiv.is_empty() {
                combined_from.union_with(&left_equiv);
                combined_to.union_with(&right_equiv);
            }

            let right_all = original_right.all_cols();
            let left_all = original_left.all_cols();
            if right_all.subset_of(&original_right.closure_of_strict(&right_equiv))
                && left_all.subset_of(&original_left.closure_of_strict(&left_equiv))
            {
                self.add_strict(
                    original_left.reduce_cols(&left_equiv),
                    right_all.union(&left_all),
                );
            }

            // The NULL-supplying value still laxly determines the preserved
            // value one column at a time.
            for right in right_equiv.iter() {
                for left in left_equiv.iter() {
                    self.add_lax(ColSet::of([right]), ColSet::of([left]));
                }
            }
            self.add_conditional(left_equiv, right_equiv, inner_cols.clone(), true, true);
        }

        if !options.skip_rule_331 {
            self.add_strict(combined_from, combined_to);
        }

        if let (Some(left), Some(right)) = (left_key, right_key) {
            self.add_strict(left.union(&right), outer_cols.union(inner_cols));
        }

        if options.only_inner_filter {
            if options.inner_is_false {
                self.add_constants(inner_cols.clone());
            } else {
                for edge in &filter.edges {
                    if edge.strict && (edge.equiv || edge.is_constant()) {
                        self.add_functional_dependency(
                            edge.from.clone(),
                            edge.to.clone(),
                            edge.strict,
                            edge.equiv,
                        );
                    }
                }
                for edge in &inner.edges {
                    self.add_functional_dependency(
                        edge.from.clone(),
                        edge.to.clone(),
                        edge.strict,
                        edge.equiv,
                    );
                }
            }
        }

        self.not_null_cols.union_with(&filter.not_null_cols);
        self.not_null_cols.difference_with(inner_cols);
    }
}

impl std::fmt::Display for FdSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, edge) in self.edges.iter().enumerate() {
            if index != 0 {
                f.write_str(", ")?;
            }
            write!(f, "{edge}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds an edge list directly, as Go's tests do, to exercise the closure
    /// computations over a set the insertion rules would have normalized.
    fn strict_edge(from: &[i64], to: &[i64]) -> FdEdge {
        FdEdge {
            from: ColSet::of(from.iter().copied()),
            to: ColSet::of(to.iter().copied()),
            strict: true,
            equiv: false,
        }
    }

    fn sorted(set: &ColSet) -> Vec<i64> {
        set.iter().collect()
    }

    /// Go `TestAddStrictFunctionalDependency`: `AB --> CDEFG` implies both
    /// `AB --> CD` and `AB --> EF`, in either insertion order.
    #[test]
    fn add_strict_keeps_only_the_strongest() {
        for order in [[0usize, 1, 2], [1, 0, 2]] {
            let inputs = [
                (vec![1, 2], vec![3, 4, 5, 6, 7]),
                (vec![1, 2], vec![3, 4]),
                (vec![1, 2], vec![5, 6]),
            ];
            let mut fd = FdSet::new();
            for which in order {
                let (from, to) = &inputs[which];
                fd.add_strict(ColSet::of(from.clone()), ColSet::of(to.clone()));
            }
            assert_eq!(fd.edges.len(), 1);
            assert_eq!(sorted(&fd.edges[0].from), vec![1, 2]);
            assert_eq!(sorted(&fd.edges[0].to), vec![3, 4, 5, 6, 7]);
        }
    }

    /// Go `TestFDSet_ClosureOf`.
    #[test]
    fn closure_of_strict_is_transitive() {
        let mut fd = FdSet::new();
        fd.edges = vec![
            strict_edge(&[1, 2], &[3, 4]),
            strict_edge(&[1, 2], &[5, 6]),
            strict_edge(&[2], &[6, 7]),
            strict_edge(&[1], &[4, 5, 8]),
        ];
        assert_eq!(
            sorted(&fd.closure_of_strict(&ColSet::of([1]))),
            vec![1, 4, 5, 8]
        );
        assert_eq!(
            sorted(&fd.closure_of_strict(&ColSet::of([1, 2]))),
            vec![1, 2, 3, 4, 5, 6, 7, 8]
        );
    }

    /// Go `TestFDSet_ReduceCols`: `{A,B}` reduces to `{A}` because
    /// `A --> CD`, `C --> DE` and `CE --> B` chain to `B`.
    #[test]
    fn reduce_cols_drops_derivable_determinants() {
        let mut fd = FdSet::new();
        fd.edges = vec![
            strict_edge(&[1], &[3, 4]),
            strict_edge(&[3], &[4, 5]),
            strict_edge(&[3, 5], &[2]),
        ];
        assert_eq!(sorted(&fd.reduce_cols(&ColSet::of([1, 2]))), vec![1]);
    }

    /// Go `TestFDSet_InClosure`: a dependency side may be torn apart, a
    /// determinant may not.
    #[test]
    fn in_closure_matches_go() {
        let mut fd = FdSet::new();
        fd.edges = vec![
            strict_edge(&[1, 2], &[3, 4]),
            strict_edge(&[1, 2], &[5, 6]),
            strict_edge(&[2], &[6, 7]),
        ];
        assert!(!fd.in_closure(&ColSet::of([1]), &ColSet::of([6])));
        assert!(fd.in_closure(&ColSet::of([2]), &ColSet::of([7])));
        assert!(fd.in_closure(&ColSet::of([1, 2]), &ColSet::of([5])));
        assert!(fd.in_closure(&ColSet::of([1, 2]), &ColSet::of([6, 7])));
        assert!(fd.in_closure(&ColSet::of([1, 2]), &ColSet::of([4, 6])));
        assert!(fd.in_closure(&ColSet::of([1, 2]), &ColSet::of([5, 7])));
        assert!(!fd.in_closure(&ColSet::of([1, 2]), &ColSet::of([5, 7, 8])));

        fd.edges.push(strict_edge(&[2], &[3, 8]));
        assert!(fd.in_closure(&ColSet::of([1, 2]), &ColSet::of([5, 7, 8])));
    }

    /// Go `TestFDSet_AddConstant`.
    #[test]
    fn add_constant_absorbs_and_simplifies() {
        let mut fd = FdSet::new();
        assert_eq!(fd.constant_cols().to_string(), "()");

        fd.add_constants(ColSet::of([1, 2]));
        assert_eq!(fd.edges.len(), 1);
        assert!(fd.edges[0].strict && !fd.edges[0].equiv);
        assert_eq!(fd.edges[0].from.to_string(), "()");
        assert_eq!(fd.edges[0].to.to_string(), "(1,2)");
        assert_eq!(fd.constant_cols().to_string(), "(1,2)");

        fd.add_constants(ColSet::of([3]));
        assert_eq!(fd.edges.len(), 1);
        assert_eq!(fd.edges[0].to.to_string(), "(1-3)");
        assert_eq!(fd.constant_cols().to_string(), "(1-3)");

        fd.add_strict(ColSet::of([3, 4]), ColSet::of([5, 6]));
        assert_eq!(fd.edges.len(), 2);
        assert_eq!(fd.edges[0].to.to_string(), "(1-3)");
        // The constant `3` is dropped from the determinant.
        assert_eq!(fd.edges[1].from.to_string(), "(4)");
        assert_eq!(fd.edges[1].to.to_string(), "(5,6)");

        fd.add_lax(ColSet::of([7]), ColSet::of([5, 6]));
        assert_eq!(fd.edges.len(), 3);
        assert!(!fd.edges[2].strict && !fd.edges[2].equiv);
        assert_eq!(fd.edges[2].from.to_string(), "(7)");
        assert_eq!(fd.edges[2].to.to_string(), "(5,6)");

        // `{4}` constant makes `{4} --> {5,6}` constant too, and the lax edge
        // then determines nothing that is not already constant.
        fd.add_constants(ColSet::of([4]));
        assert_eq!(fd.edges.len(), 1);
        assert_eq!(fd.edges[0].from.to_string(), "()");
        assert_eq!(fd.edges[0].to.to_string(), "(1-6)");
        assert_eq!(fd.constant_cols().to_string(), "(1-6)");
    }

    /// Go `TestFDSet_LaxImplies`: two lax edges absorb each other only when
    /// their dependency sides are identical.
    #[test]
    fn lax_implies_only_on_equal_dependency_side() {
        let mut fd = FdSet::new();
        fd.add_lax(ColSet::of([1]), ColSet::of([2, 3]));
        fd.add_lax(ColSet::of([1]), ColSet::of([2]));
        assert_eq!(fd.to_string(), "(1)~~>(2,3), (1)~~>(2)");

        let mut fd = FdSet::new();
        fd.add_lax(ColSet::of([1]), ColSet::of([2]));
        fd.add_lax(ColSet::of([1]), ColSet::of([2, 3]));
        assert_eq!(fd.to_string(), "(1)~~>(2), (1)~~>(2,3)");

        let mut fd = FdSet::new();
        fd.add_lax(ColSet::of([1]), ColSet::of([3]));
        fd.add_lax(ColSet::of([1, 2]), ColSet::of([3]));
        assert_eq!(fd.to_string(), "(1)~~>(3)");

        let mut fd = FdSet::new();
        fd.add_lax(ColSet::of([1]), ColSet::of([3, 4]));
        fd.add_lax(ColSet::of([1, 2]), ColSet::of([3]));
        assert_eq!(fd.to_string(), "(1)~~>(3,4), (1,2)~~>(3)");
    }

    /// Go `TestFDSet_AddEquivalence`: equivalence classes merge, extend the
    /// constant closure, and then collapse the strict edges into it.
    #[test]
    fn add_equivalence_merges_classes() {
        let mut fd = FdSet::new();
        assert_eq!(fd.equivalence_cols().len(), 0);

        fd.add_equivalence(ColSet::of([1]), ColSet::of([2]));
        assert_eq!(fd.edges.len(), 1);
        assert_eq!(fd.equivalence_cols().len(), 1);
        assert!(fd.edges[0].strict && fd.edges[0].equiv);
        assert_eq!(fd.edges[0].from.to_string(), "(1,2)");
        assert_eq!(fd.edges[0].to.to_string(), "(1,2)");

        fd.add_equivalence(ColSet::of([3]), ColSet::of([4]));
        assert_eq!(fd.edges.len(), 2);
        assert_eq!(fd.equivalence_cols().len(), 2);
        assert_eq!(fd.edges[1].from.to_string(), "(3,4)");

        fd.add_constants(ColSet::of([4, 5]));
        assert_eq!(fd.edges.len(), 3);
        assert!(fd.edges[2].strict && !fd.edges[2].equiv);
        assert_eq!(fd.edges[2].from.to_string(), "()");
        // `{c,d} == {c,d}` extends the constant closure from `{d,e}` to
        // `{c,d,e}`.
        assert_eq!(fd.edges[2].to.to_string(), "(3-5)");
        assert_eq!(fd.constant_cols().to_string(), "(3-5)");

        fd.add_strict(ColSet::of([2, 3]), ColSet::of([5, 6]));
        assert_eq!(fd.edges.len(), 4);
        assert!(fd.edges[3].strict && !fd.edges[3].equiv);
        assert_eq!(fd.edges[3].from.to_string(), "(2)");
        assert_eq!(fd.edges[3].to.to_string(), "(5,6)");

        // `b == d` bridges the two classes; the merged class extends the
        // constant closure, which in turn swallows the strict edge.
        fd.add_equivalence(ColSet::of([2]), ColSet::of([3]));
        assert_eq!(fd.edges.len(), 2);
        assert_eq!(fd.equivalence_cols().len(), 1);
        assert_eq!(fd.equivalence_cols()[0].to_string(), "(1-4)");
        assert_eq!(fd.constant_cols().to_string(), "(1-6)");
    }

    /// A nullable UNIQUE key is lax and does NOT determine the rest, until a
    /// predicate proves the nullable member non-null. This is the
    /// `ONLY_FULL_GROUP_BY` promotion in its smallest form.
    #[test]
    fn not_null_promotes_a_lax_key() {
        // `t(a INT NULL, b INT NOT NULL, c INT, UNIQUE(a,b))` as ids 1,2,3:
        // the key is lax, and `b`'s declared NOT NULL alone does not promote
        // it -- the WHOLE determinant has to be non-null.
        let mut fd = FdSet::new();
        fd.add_lax(ColSet::of([1, 2]), ColSet::of([1, 2, 3]));
        fd.make_not_null(ColSet::of([2]));
        assert!(!ColSet::of([3]).subset_of(&fd.closure_of_strict(&ColSet::of([1, 2]))));
        // The lax step alone already reaches it, which is why the checker must
        // use the STRICT closure and not this one.
        assert!(ColSet::of([3]).subset_of(&fd.closure_of_lax(&ColSet::of([1, 2]))));

        // `WHERE a IS NOT NULL` supplies the rest of the determinant.
        fd.make_not_null(ColSet::of([1]));
        assert!(ColSet::of([3]).subset_of(&fd.closure_of_strict(&ColSet::of([1, 2]))));
    }
}
