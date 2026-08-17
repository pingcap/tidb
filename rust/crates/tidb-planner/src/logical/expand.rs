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

//! Go `pkg/planner/core/operator/logicalop/logical_expand.go`: `LogicalExpand`,
//! the operator that REPLICATES each input row once per rollup grouping set.
//!
//! SEED of `pkg/planner/core`. `LogicalExpand` was a
//! [`crate::logical::TodoLogicalOp`] before this batch.
//!
//! This file MERGES the crate's former `logical_expand` identity leaf: its
//! `LogicalExpandIdentity` modelled the generated `Hash64`/`Equals` over
//! normalised column adapters, and that contract now runs on the real operator
//! as [`LogicalExpand::hash64`] and [`LogicalExpand::equals`]. The adapter
//! module is gone; it had no consumer outside this crate.
//!
//! # Narrowings, by name
//!
//! * `RollupGroupingSets expression.GroupingSets`. `pkg/expression`'s
//!   `GroupingSets`/`GroupingSet`/`GroupingExprs` are NOT transcreated. Every
//!   body in this Go file reads a grouping set through exactly one method —
//!   `GroupingSet.AllColIDs()` — and the file-level `AllSetsColIDs()` is their
//!   union, so [`RollupGroupingSet`] carries that column-id set and nothing
//!   else. What is lost is the EXPRESSION form of a set, which only
//!   `expression.GroupingSets.DistinctSize` and the grouping-set builder in
//!   `pkg/expression/grouping_sets.go` need.
//! * `DistinctSize`, `RollupGroupingIDs` and `RollupID2GIDS` are the three
//!   outputs of `expression.GroupingSets.DistinctSize()`, which is that same
//!   untranscreated file. They are FIELDS here, filled by whoever builds the
//!   operator; [`LogicalExpand::gen_level_projections`] therefore reads them
//!   rather than recomputing them, and says so.
//! * `TrySubstituteExprWithGroupingSetCol` and
//!   `ResolveGroupingFuncArgsInGroupBy` compare with
//!   `expression.Expression.CanonicalHashCode`, which normalises a commutative
//!   function's argument order and is not transcreated. Both bodies below
//!   compare `HashCode` instead, so a COMMUTED group-by expression is not
//!   recognised — a false negative, which leaves the expression unsubstituted
//!   rather than pointing it at the wrong column.
//! * `ExtractFD` forwards to the schema producer's; `pkg/planner/funcdep` is
//!   not transcreated (see [`crate::logical::BaseLogicalPlan`]).
//! * `GroupingMode tipb.GroupingMode`. `tidb-expr` models the same three modes
//!   for the `GROUPING` function itself, but keeps them in a private module, so
//!   [`GroupingMode`] here carries them again with the same wire discriminants.
//!   Only two of the three are decisions in this file: `ModeNumericSet` reads
//!   the id mapping, and everything else takes the bitmask.

use std::collections::{BTreeMap, BTreeSet};

use tidb_datatype::Datum;
use tidb_expr::column::Column;
use tidb_expr::constant::Constant;
use tidb_expr::expression::{CorrelatedColumn, Expression};
use tidb_expr::schema::Schema;
use tidb_expr::simple_expr::extract_cor_columns;

use crate::hash_equaler::{new_hash_equaler, Hasher, NIL_FLAG, NOT_NIL_FLAG};
use crate::logical::{schema_producer, BaseLogicalPlan};
use crate::plan_base::PlanError;

/// Go `tipb.GroupingMode`, which `LogicalExpand.GroupingMode` carries.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum GroupingMode {
    /// Go `tipb.GroupingMode_ModeBitAnd`: a grouping id is a bitmask over the
    /// distinct group-by columns.
    #[default]
    BitAnd,
    /// Go `tipb.GroupingMode_ModeNumericCmp`.
    NumericCmp,
    /// Go `tipb.GroupingMode_ModeNumericSet`: a grouping id is the grouping
    /// set's own index, used when more than 64 sets make a bitmask impossible.
    NumericSet,
}

/// Go `expression.GroupingSet` as `logical_expand.go` uses it: the set of
/// column unique ids the set groups by. See this module's header.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RollupGroupingSet {
    /// Go `GroupingSet.AllColIDs()`.
    pub col_ids: BTreeSet<i64>,
}

impl RollupGroupingSet {
    /// A grouping set over the given column unique ids.
    #[must_use]
    pub fn new(col_ids: impl IntoIterator<Item = i64>) -> Self {
        Self {
            col_ids: col_ids.into_iter().collect(),
        }
    }
}

/// Go `logicalop.LogicalExpand` (`logical_expand.go:32`).
#[derive(Clone, Debug, Default)]
pub struct LogicalExpand {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
    /// Go `DistinctGroupByCol`: the distinct group-by columns, which the
    /// projection below this operator materialises even for a bare column.
    /// Their ORDER is the bit order of every grouping id.
    pub distinct_group_by_col: Vec<Column>,
    /// Go `DistinctGbyColNames`, as bare names — `types.FieldName` is carried
    /// by [`crate::plan_base::BasePlan`] elsewhere, and this list is used only
    /// for building output names.
    pub distinct_gby_col_names: Vec<String>,
    /// Go `DistinctGbyExprs`: the ORIGINAL group-by expressions, kept so
    /// `grouping(a+b)` can be resolved back to its projected column.
    pub distinct_gby_exprs: Vec<Expression>,
    /// Go `DistinctSize`; see this module's header.
    pub distinct_size: i64,
    /// Go `RollupGroupingSets`.
    pub rollup_grouping_sets: Vec<RollupGroupingSet>,
    /// Go `RollupID2GIDS`: for `ModeNumericSet`, each column's grouping ids.
    pub rollup_id_to_gids: BTreeMap<i64, BTreeSet<u64>>,
    /// Go `RollupGroupingIDs`, one per grouping set in order.
    pub rollup_grouping_ids: Vec<u64>,
    /// Go `LevelExprs`, the per-grouping-set projection.
    ///
    /// `None` is Go's NIL, which means `GenLevelProjections` has not run yet
    /// and which [`Self::extract_correlated_cols`] tests for by name.
    pub level_exprs: Option<Vec<Vec<Expression>>>,
    /// Go `ExtraGroupingColNames`, e.g. `"gid"` and `"gpos"`.
    pub extra_grouping_col_names: Vec<String>,
    /// Go `GroupingMode`.
    pub grouping_mode: Option<GroupingMode>,
    /// Go `GID *expression.Column`: the generated grouping-id column, BOXED as
    /// Go's own pointer is — a `Column` is wide, and this operator is a rare
    /// variant of a hot enum.
    pub gid: Option<Box<Column>>,
    /// Go `GIDName`.
    pub gid_name: Option<String>,
    /// Go `GPos`: the generated grouping-position column, present only when two
    /// grouping sets are duplicates of each other.
    pub gpos: Option<Box<Column>>,
    /// Go `GPosName`.
    pub gpos_name: Option<String>,
}

impl LogicalExpand {
    /// Go `plancodec.TypeExpand`.
    pub const TYPE: &'static str = "Expand";

    /// Go `LogicalExpand.Init(ctx, offset)` (`logical_expand.go:65`).
    #[must_use]
    pub fn new(base: BaseLogicalPlan) -> Self {
        Self {
            base,
            ..Self::default()
        }
    }

    /// Go `LogicalExpand.PredicatePushDown(predicates)`
    /// (`logical_expand.go:75`): NOTHING crosses an Expand.
    ///
    /// Go's own argument: an Expand sets a grouping column to NULL for the sets
    /// that do not group by it, so the column's NULLABILITY changes across this
    /// operator; any predicate that reaches here is either an aggregate filter
    /// or a group-by-item filter, and neither is sound below. Go calls the base
    /// body with `nil` — which pushes nothing and returns nothing — and then
    /// appends the whole input, so the caller re-attaches a Selection ABOVE.
    #[must_use]
    pub fn predicate_push_down(&self, predicates: Vec<Expression>) -> Vec<Expression> {
        predicates
    }

    /// Go `LogicalExpand.PruneColumns(parentUsedCols)`'s LOCAL half
    /// (`logical_expand.go:95`): the distinct group-by columns are APPENDED to
    /// the parent's set before it reaches the child.
    ///
    /// They must survive: the aggregate above reads them, and the level
    /// projections below name them.
    #[must_use]
    pub fn prune_columns_local(&self, parent_used_cols: &[Column]) -> Vec<Column> {
        let mut used = parent_used_cols.to_vec();
        used.extend(self.distinct_group_by_col.iter().cloned());
        used
    }

    /// The rest of `LogicalExpand.PruneColumns` (`logical_expand.go:99`): this
    /// operator's OWN schema loses every column the widened set does not name.
    ///
    /// Go deletes from `p.Schema()` and `p.OutputNames()` in lockstep, walking
    /// backwards so the indices stay valid. Returns the removed positions, in
    /// descending order, so the driver can delete the matching output names —
    /// the names live on [`crate::plan_base::BasePlan`], not here.
    ///
    /// Note that the level projections are NOT rebuilt: Go's comment says they
    /// are generated after column pruning, "so when do the rule_column_pruning
    /// here, we just prune the schema is enough".
    pub fn prune_schema(schema: &mut Schema, used_cols: &[Column]) -> Vec<usize> {
        let used = schema_producer::get_used_list(used_cols, schema);
        let mut pruned = Vec::new();
        for i in (0..used.len()).rev() {
            if !used[i] {
                pruned.push(i);
                schema.columns.remove(i);
            }
        }
        pruned
    }

    /// Go `LogicalExpand.BuildKeyInfo(selfSchema, _)`
    /// (`logical_expand.go:389`): an Expand emits one row PER GROUPING SET, so
    /// nothing that was a key upstream still is.
    pub fn build_key_info(self_schema: &mut Schema) {
        self_schema.set_keys(Vec::new());
        self_schema.set_unique_keys(Vec::new());
    }

    /// Go `LogicalExpand.ExtractCorrelatedCols()` (`logical_expand.go:138`).
    ///
    /// Go distinguishes a NIL `LevelExprs` — the level projections have not been
    /// generated, which is the state during subquery building — from a generated
    /// one, and answers nothing in the first case because generating them
    /// produces no correlated column.
    #[must_use]
    pub fn extract_correlated_cols(&self) -> Vec<CorrelatedColumn> {
        let Some(levels) = &self.level_exprs else {
            return Vec::new();
        };
        let mut cor_cols = Vec::with_capacity(levels.first().map_or(0, Vec::len));
        for level in levels {
            for expr in level {
                cor_cols.extend(extract_cor_columns(expr));
            }
        }
        cor_cols
    }

    /// Go `LogicalExpand.GetUsedCols()` (`logical_expand.go:182`): NOTHING.
    ///
    /// Go's reason, kept because it is the whole content of the body: an Expand
    /// "just replicates the child's schema by defined grouping sets", so passing
    /// the parent's used set down is enough and the operator adds nothing of its
    /// own.
    #[must_use]
    pub const fn get_used_cols() -> Vec<Column> {
        Vec::new()
    }

    /// Go `LogicalExpand.GenLevelProjections()` (`logical_expand.go:191`): one
    /// projection per grouping set, in which a column this set does not group by
    /// becomes a typed NULL and the trailing generated columns become constants.
    ///
    /// BOUNDARY: Go's first two lines call
    /// `expression.GroupingSets.AllSetsColIDs()` and
    /// `expression.GroupingSets.DistinctSize()`, of which only the former is
    /// dependency-closed here (it is the union of [`RollupGroupingSet::col_ids`]).
    /// `DistinctSize` fills [`Self::distinct_size`], [`Self::rollup_grouping_ids`]
    /// and [`Self::rollup_id_to_gids`]; this body READS those three rather than
    /// recomputing them, so the builder must have filled them.
    ///
    /// The duplicate-set case is what decides the schema layout: with duplicates
    /// the last TWO schema columns are `gid` and `gpos`, otherwise only the last
    /// is `gid`. Go indexes those positions unconditionally; a schema too narrow
    /// to hold them leaves the level projections untouched here.
    pub fn gen_level_projections(&mut self, schema: &Schema) {
        let grouping_set_cols: BTreeSet<i64> = self
            .rollup_grouping_sets
            .iter()
            .flat_map(|set| set.col_ids.iter().copied())
            .collect();
        let has_duplicate_grouping_set =
            self.rollup_grouping_sets.len() as i64 != self.distinct_size;
        let generated = if has_duplicate_grouping_set { 2 } else { 1 };
        if schema.columns.len() < generated {
            return;
        }
        let non_gen_cols = &schema.columns[..schema.columns.len() - generated];
        let gid_col = &schema.columns[schema.columns.len() - generated];

        let mut levels = self.level_exprs.take().unwrap_or_default();
        for offset in 0..self.rollup_grouping_sets.len() {
            let cur_set = &self.rollup_grouping_sets[offset];
            let mut level_proj = Vec::with_capacity(schema.columns.len());
            for one_col in non_gen_cols {
                if grouping_set_cols.contains(&one_col.unique_id)
                    && !cur_set.col_ids.contains(&one_col.unique_id)
                {
                    // Go `expression.NewNullWithFieldType(col.RetType.Clone())`:
                    // the un-needed column of this set is projected as NULL, and
                    // it keeps the column's own type so the union of the levels
                    // is still well typed.
                    let mut null = Constant::new_null();
                    null.ret_type = one_col.ret_type.clone();
                    level_proj.push(Expression::Constant(null));
                } else {
                    level_proj.push(Expression::Column(one_col.clone()));
                }
            }
            let gid = match self.grouping_mode {
                Some(GroupingMode::NumericSet) => self
                    .generate_grouping_id_increment_mode_numeric_set(offset)
                    .unwrap_or_default(),
                _ => self.generate_grouping_id_mode_bit_and(cur_set),
            };
            level_proj.push(Expression::Constant(uint64_const(gid, gid_col)));
            if has_duplicate_grouping_set {
                let gpos_col = &schema.columns[schema.columns.len() - 1];
                level_proj.push(Expression::Constant(uint64_const(offset as u64, gpos_col)));
            }
            levels.push(level_proj);
        }
        self.level_exprs = Some(levels);
    }

    /// Go `LogicalExpand.GenerateGroupingIDModeBitAnd(oneSet)`
    /// (`logical_expand.go:349`): a bitmask over [`Self::distinct_group_by_col`]
    /// in which a bit is 1 when this grouping set NEEDS that column.
    ///
    /// The columns are read from the highest position down, shifting left each
    /// step, so column 0 lands in the low bit: `{a, c}` out of `(a, b, c)` is
    /// `101`. Two grouping sets that name the same columns therefore share a
    /// grouping id, which is exactly what a duplicate set is.
    #[must_use]
    pub fn generate_grouping_id_mode_bit_and(&self, one_set: &RollupGroupingSet) -> u64 {
        let mut res = 0_u64;
        for col in self.distinct_group_by_col.iter().rev() {
            res <<= 1;
            if one_set.col_ids.contains(&col.unique_id) {
                res |= 1;
            }
        }
        res
    }

    /// Go `LogicalExpand.GenerateGroupingIDIncrementModeNumericSet(offset)`
    /// (`logical_expand.go:375`): the grouping id is the set's own INDEX, taken
    /// from the mapping the builder stored.
    ///
    /// This mode exists because more than 64 grouping sets do not fit in a
    /// `uint64` bitmask. Go indexes `RollupGroupingIDs` and panics past its end;
    /// this returns `None`.
    #[must_use]
    pub fn generate_grouping_id_increment_mode_numeric_set(&self, offset: usize) -> Option<u64> {
        self.rollup_grouping_ids.get(offset).copied()
    }

    /// Go `LogicalExpand.GenerateGroupingMarks(sourceCols)`
    /// (`logical_expand.go:246`): the per-argument meta the `GROUPING` function
    /// evaluates against the grouping id.
    ///
    /// In `ModeBitAnd` a mark is the single bit of that column, so
    /// `groupingID & mark > 0` means the column is present and hence NOT
    /// grouped. In `ModeNumericSet` it is instead the set of grouping ids the
    /// column appears in, and the test is membership. A source column that is
    /// not a group-by column marks as 0 / the empty set, which is Go's answer
    /// too — a missing key reads as the zero value.
    ///
    /// `GROUPING(x, y, z)` is `GROUPING(x) << 2 + GROUPING(y) << 1 + GROUPING(z)`,
    /// which is why one mark is returned PER argument and why the function takes
    /// at most 64 of them.
    #[must_use]
    pub fn generate_grouping_marks(&self, source_cols: &[Column]) -> Vec<BTreeSet<u64>> {
        if matches!(self.grouping_mode, Some(GroupingMode::NumericSet)) {
            return source_cols
                .iter()
                .map(|col| {
                    self.rollup_id_to_gids
                        .get(&col.unique_id)
                        .cloned()
                        .unwrap_or_default()
                })
                .collect();
        }
        source_cols
            .iter()
            .map(|one_col| {
                let mut res = 0_u64;
                for col in self.distinct_group_by_col.iter().rev() {
                    res <<= 1;
                    if col.unique_id == one_col.unique_id {
                        res |= 1;
                    }
                }
                BTreeSet::from([res])
            })
            .collect()
    }

    /// Go `LogicalExpand.TrySubstituteExprWithGroupingSetCol(expr)`
    /// (`logical_expand.go:290`): map an ORIGINAL group-by expression onto the
    /// column the projection below materialised it as.
    ///
    /// Returns the substituted column and whether it was found; an unfound
    /// expression comes back unchanged, as in Go. See this module's header for
    /// the `CanonicalHashCode` narrowing.
    #[must_use]
    pub fn try_substitute_expr_with_grouping_set_col(
        &self,
        expr: &Expression,
    ) -> (Expression, bool) {
        match self.find_distinct_gby_expr(expr) {
            Some(i) => (
                Expression::Column(self.distinct_group_by_col[i].clone()),
                true,
            ),
            None => (expr.clone(), false),
        }
    }

    /// Go `LogicalExpand.ResolveGroupingFuncArgsInGroupBy(args)`
    /// (`logical_expand.go:304`): every `GROUPING` argument must name a group-by
    /// item, either as the original expression or as the column that expression
    /// was already rewritten to.
    ///
    /// The second case is not a fallback but the common one: by the time
    /// `grouping(year)` is built for `... group by year, country with rollup
    /// order by grouping(year)`, `year` has already become the grouping-set
    /// column through the first select item.
    ///
    /// Go raises `plannererrors.ErrFieldInGroupingNotGroupBy` naming the
    /// argument's index; that error code is not transcreated, so the message
    /// carries the same `#index`.
    pub fn resolve_grouping_func_args_in_group_by(
        &self,
        grouping_func_args: &[Expression],
    ) -> Result<Vec<Column>, PlanError> {
        let distinct_ids: BTreeSet<i64> = self
            .distinct_group_by_col
            .iter()
            .map(|col| col.unique_id)
            .collect();
        let mut rewritten = Vec::with_capacity(grouping_func_args.len());
        for (arg_idx, one_arg) in grouping_func_args.iter().enumerate() {
            if let Some(ref_pos) = self.find_distinct_gby_expr(one_arg) {
                rewritten.push(self.distinct_group_by_col[ref_pos].clone());
                continue;
            }
            match one_arg {
                Expression::Column(col) if distinct_ids.contains(&col.unique_id) => {
                    rewritten.push(col.clone());
                }
                _ => {
                    return Err(PlanError::internal(format!(
                        "ErrFieldInGroupingNotGroupBy: #{arg_idx}"
                    )))
                }
            }
        }
        Ok(rewritten)
    }

    /// The `DistinctGbyExprs` lookup both substitution bodies share.
    fn find_distinct_gby_expr(&self, expr: &Expression) -> Option<usize> {
        self.distinct_gby_exprs
            .iter()
            .take(self.distinct_group_by_col.len())
            .position(|one| schema_producer::expressions_equal(expr, one))
    }

    /// Go `LogicalExpand.Hash64(h)`
    /// (`logicalop/hash64_equals_generated.go`): the schema producer, then the
    /// distinct group-by columns and expressions, the distinct size, the
    /// grouping sets, the level projections, and the two generated columns.
    ///
    /// This is the merged form of the crate's former
    /// `logical_expand::LogicalExpandIdentity`.
    #[must_use]
    pub fn hash64(&self, schema: Option<&Schema>) -> u64 {
        let mut hasher = new_hash_equaler();
        hasher.hash_string(Self::TYPE);
        hasher.hash_uint64(schema_producer::schema_hash64(schema));
        hasher.hash_int(self.distinct_group_by_col.len() as i64);
        for col in &self.distinct_group_by_col {
            hash_column(&mut hasher, col);
        }
        hasher.hash_int(self.distinct_gby_exprs.len() as i64);
        for expr in &self.distinct_gby_exprs {
            let mut expr = expr.clone();
            hasher.hash_bytes(expr.hash_code());
        }
        hasher.hash_int64(self.distinct_size);
        hasher.hash_int(self.rollup_grouping_sets.len() as i64);
        for set in &self.rollup_grouping_sets {
            hasher.hash_int(set.col_ids.len() as i64);
            for id in &set.col_ids {
                hasher.hash_int64(*id);
            }
        }
        // Go's generated body frames a nil slice apart from an empty one, which
        // for `LevelExprs` is the load-bearing distinction of
        // `ExtractCorrelatedCols`.
        match &self.level_exprs {
            None => hasher.hash_byte(NIL_FLAG),
            Some(levels) => {
                hasher.hash_byte(NOT_NIL_FLAG);
                hasher.hash_int(levels.len() as i64);
                for level in levels {
                    hasher.hash_int(level.len() as i64);
                    for expr in level {
                        let mut expr = expr.clone();
                        hasher.hash_bytes(expr.hash_code());
                    }
                }
            }
        }
        hash_column_option(&mut hasher, self.gid.as_deref());
        hash_column_option(&mut hasher, self.gpos.as_deref());
        hasher.sum64()
    }

    /// Go `LogicalExpand.Equals(other)`, over the same fields
    /// [`Self::hash64`] folds in.
    #[must_use]
    pub fn equals(
        &self,
        self_schema: Option<&Schema>,
        other: &Self,
        other_schema: Option<&Schema>,
    ) -> bool {
        schema_producer::schema_equals(self_schema, other_schema)
            && columns_equal(&self.distinct_group_by_col, &other.distinct_group_by_col)
            && schema_producer::expression_lists_equal(
                &self.distinct_gby_exprs,
                &other.distinct_gby_exprs,
            )
            && self.distinct_size == other.distinct_size
            && self.rollup_grouping_sets == other.rollup_grouping_sets
            && level_exprs_equal(self.level_exprs.as_deref(), other.level_exprs.as_deref())
            && column_options_equal(self.gid.as_deref(), other.gid.as_deref())
            && column_options_equal(self.gpos.as_deref(), other.gpos.as_deref())
    }

    /// This operator's own fields with NO children; see
    /// [`crate::logical::LogicalPlan::clone_shallow`].
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        Self {
            base: self.base.shell(),
            distinct_group_by_col: self.distinct_group_by_col.clone(),
            distinct_gby_col_names: self.distinct_gby_col_names.clone(),
            distinct_gby_exprs: self.distinct_gby_exprs.clone(),
            distinct_size: self.distinct_size,
            rollup_grouping_sets: self.rollup_grouping_sets.clone(),
            rollup_id_to_gids: self.rollup_id_to_gids.clone(),
            rollup_grouping_ids: self.rollup_grouping_ids.clone(),
            level_exprs: self.level_exprs.clone(),
            extra_grouping_col_names: self.extra_grouping_col_names.clone(),
            grouping_mode: self.grouping_mode,
            gid: self.gid.clone(),
            gid_name: self.gid_name.clone(),
            gpos: self.gpos.clone(),
            gpos_name: self.gpos_name.clone(),
        }
    }
}

/// Go `expression.NewUInt64ConstWithFieldType(v, ft.Clone())`.
fn uint64_const(value: u64, like: &Column) -> Constant {
    let mut constant = Constant::new_null();
    constant.value = Datum::UInt(value);
    // Go clones the generated column's own `RetType`; a column with none (a nil
    // pointer in Go) leaves the constant's default declared type.
    if let Some(ret_type) = like.ret_type.clone() {
        constant.ret_type = Some(ret_type);
    }
    constant
}

/// Go `Column.Hash64` as the generated bodies consume it: the identity triple.
fn hash_column(hasher: &mut impl Hasher, column: &Column) {
    hasher.hash_int64(column.id);
    hasher.hash_int64(column.unique_id);
    hasher.hash_int64(column.index);
}

fn hash_column_option(hasher: &mut impl Hasher, column: Option<&Column>) {
    match column {
        Some(column) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            hash_column(hasher, column);
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
}

fn columns_equal(left: &[Column], right: &[Column]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            left.id == right.id && left.unique_id == right.unique_id && left.index == right.index
        })
}

fn column_options_equal(left: Option<&Column>, right: Option<&Column>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => {
            columns_equal(std::slice::from_ref(left), std::slice::from_ref(right))
        }
        _ => false,
    }
}

fn level_exprs_equal(left: Option<&[Vec<Expression>]>, right: Option<&[Vec<Expression>]>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| schema_producer::expression_lists_equal(left, right))
        }
        _ => false,
    }
}
