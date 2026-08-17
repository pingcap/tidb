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

//! Go `pkg/planner/core/operator/logicalop/logical_window.go`:
//! `LogicalWindow`, plus the `WindowFrame` and `FrameBound` types that file
//! also declares.
//!
//! SEED of `pkg/planner/core`. `LogicalWindow` was a
//! [`crate::logical::TodoLogicalOp`] before this batch.
//!
//! This file MERGES the crate's former `window_frame` identity leaf: its
//! `FrameBoundIdentity`/`WindowFrameIdentity` modelled Go's HANDWRITTEN
//! `Hash64`/`Equals` over a normalised column adapter, and those contracts now
//! run on the real [`FrameBound`]/[`WindowFrame`] as [`FrameBound::hash64`],
//! [`FrameBound::equals`], [`WindowFrame::hash64`] and [`WindowFrame::equals`].
//! The adapter module is gone; it had no consumer outside this crate.
//!
//! # Narrowings, by name
//!
//! * `PartitionBy`/`OrderBy` are `[]property.SortItem`, whose `Col` is a whole
//!   `*expression.Column`. This crate's [`crate::physical_property::SortItem`]
//!   narrows that to the `UniqueID`, which is enough for a required ORDER but
//!   not for `GetPartitionByCols`, which hands the columns to
//!   `expression.NewSchema`. So the window carries [`WindowSortItem`], which
//!   keeps the column.
//! * `FrameBound.CmpFuncs []expression.CompareFunc` is a slice of FUNCTION
//!   POINTERS, and both Go's `Hash64` and its `Equals` compare them by
//!   `fmt.Sprintf("%p", f)` — the address, not the behaviour. There is no
//!   transcreated `expression.CompareFunc`, so [`FrameBound::cmp_func_tokens`]
//!   carries exactly the identity token Go hashes.
//! * `ToPB` needs `base.BuildPBContext` and `tipb`; `GetPartitionKeys` needs
//!   `property.MPPPartitionColumn` and
//!   `property.GetCollateIDByNameForPartition`; `ReplaceExprColumns` needs
//!   `ruleutil.ResolveExprAndReplace`. None is transcreated.
//! * `GetGroupNDVs` is vacuous: [`StatsInfo`] has no `GroupNDVs` field.

use tidb_datatype::EvalType;
use tidb_expr::aggregation::WindowFuncDesc;
use tidb_expr::column::Column;
use tidb_expr::expr_util::normal_form::expr_from_schema;
use tidb_expr::expression::{CorrelatedColumn, Expression};
use tidb_expr::schema::Schema;
use tidb_expr::simple_expr::{extract_columns, extract_cor_columns};

use crate::hash_equaler::{new_hash_equaler, Hasher, NIL_FLAG, NOT_NIL_FLAG};
use crate::logical::schema_producer;
use crate::logical::BaseLogicalPlan;
use crate::plan_base::PossiblePropertiesInfo;
use crate::stats_info::StatsInfo;

/// Go `ast.FrameType`, as `WindowFrame.Type` uses it.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum FrameType {
    /// Go `ast.Rows`: bounds count physical row positions.
    #[default]
    Rows,
    /// Go `ast.Ranges`: bounds measure the `ORDER BY` key's own value distance.
    Ranges,
    /// Go `ast.Groups`: bounds count peer groups.
    Groups,
}

/// Go `ast.BoundType`, as `FrameBound.Type` uses it.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum BoundType {
    /// Go `ast.Following`.
    Following,
    /// Go `ast.Preceding`.
    Preceding,
    /// Go `ast.CurrentRow`.
    #[default]
    CurrentRow,
}

/// Go `tipb.RangeCmpDataType`, which `FrameBound` carries "for passing
/// information to tiflash".
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum RangeCmpDataType {
    /// Go's zero value, before `UpdateCmpFuncsAndCmpDataType` runs.
    #[default]
    Int,
    /// Go `tipb.RangeCmpDataType_Float`.
    Float,
    /// Go `tipb.RangeCmpDataType_Decimal`.
    Decimal,
    /// Go `tipb.RangeCmpDataType_DateTime`.
    DateTime,
    /// Go `tipb.RangeCmpDataType_Duration`.
    Duration,
}

/// Go `property.SortItem` as `LogicalWindow` uses it: a whole column plus a
/// direction. See this module's header for why
/// [`crate::physical_property::SortItem`] cannot stand in.
///
/// No `PartialEq`: [`Column`] has none, and Go compares these items by
/// `UniqueID` anyway — see [`LogicalWindow::equal_order_by`].
#[derive(Clone, Debug)]
pub struct WindowSortItem {
    /// Go `SortItem.Col`.
    pub col: Column,
    /// Go `SortItem.Desc`.
    pub desc: bool,
}

impl WindowSortItem {
    /// A sort item over `col`, ascending when `desc` is false.
    #[must_use]
    pub const fn new(col: Column, desc: bool) -> Self {
        Self { col, desc }
    }
}

/// Go `logicalop.FrameBound` (`logical_window.go:88`): one boundary of a
/// window frame.
#[derive(Clone, Debug, Default)]
pub struct FrameBound {
    /// Go `Type`.
    pub bound_type: BoundType,
    /// Go `UnBounded`.
    pub unbounded: bool,
    /// Go `Num`.
    pub num: u64,
    /// Go `CalcFuncs`: for a RANGE frame, the `date_add`/`date_sub` or
    /// plus/minus expression that turns the current row's key into this
    /// bound's key.
    pub calc_funcs: Vec<Expression>,
    /// Go `CompareCols`: the `ORDER BY` column cast to the type the comparison
    /// needs.
    pub compare_cols: Vec<Expression>,
    /// Go `CmpFuncs`, by the identity Go itself compares; see this module's
    /// header.
    pub cmp_func_tokens: Vec<String>,
    /// Go `CmpDataType`.
    pub cmp_data_type: RangeCmpDataType,
    /// Go `IsExplicitRange`: this range appears in the SQL rather than being
    /// the implicit default.
    pub is_explicit_range: bool,
}

impl FrameBound {
    /// Go `FrameBound.UpdateCmpFuncsAndCmpDataType(cmpDataType)`
    /// (`logical_window.go:207`): pick the comparison the frame's key type
    /// needs.
    ///
    /// Go's own note on the fall-through: a type that matches no arm is
    /// IGNORED rather than rejected, because a genuinely bad explicit `RANGE`
    /// has already raised an error before the logical plan is built — its
    /// example is a `RANGE` frame over a `text` `ORDER BY` key.
    ///
    /// Go also assigns `fb.CmpFuncs[0]`, which panics on an empty slice; here
    /// the token slot is written only when it exists, and the data type is set
    /// either way — the data type is what TiFlash reads.
    pub fn update_cmp_funcs_and_cmp_data_type(&mut self, cmp_data_type: EvalType) {
        let (token, data_type) = match cmp_data_type {
            EvalType::Int => ("CompareInt", RangeCmpDataType::Int),
            EvalType::Datetime | EvalType::Timestamp => ("CompareTime", RangeCmpDataType::DateTime),
            EvalType::Duration => ("CompareDuration", RangeCmpDataType::Duration),
            EvalType::Real => ("CompareReal", RangeCmpDataType::Float),
            EvalType::Decimal => ("CompareDecimal", RangeCmpDataType::Decimal),
            EvalType::String | EvalType::Json | EvalType::VectorFloat32 => return,
        };
        if let Some(slot) = self.cmp_func_tokens.first_mut() {
            *slot = token.to_owned();
        }
        self.cmp_data_type = data_type;
    }

    /// Go `FrameBound.Hash64(h)` (`logical_window.go:112`), field for field and
    /// in Go's order. Each of the three slices is preceded by a nil/not-nil
    /// marker and then its length, so an empty slice and an absent one differ.
    #[must_use]
    pub fn hash64(&self) -> u64 {
        let mut hasher = new_hash_equaler();
        hasher.hash_int(self.bound_type as i64);
        hasher.hash_bool(self.unbounded);
        hasher.hash_uint64(self.num);
        hash_expression_list(&mut hasher, &self.calc_funcs);
        hash_expression_list(&mut hasher, &self.compare_cols);
        // Go treats a nil `CmpFuncs` as absent; an empty Vec is the closest
        // this port has, and it takes the nil branch for the same reason a nil
        // slice does — there is nothing to hash.
        if self.cmp_func_tokens.is_empty() {
            hasher.hash_byte(NIL_FLAG);
        } else {
            hasher.hash_byte(NOT_NIL_FLAG);
            hasher.hash_int(self.cmp_func_tokens.len() as i64);
            for token in &self.cmp_func_tokens {
                hasher.hash_string(token);
            }
        }
        hasher.hash_int64(self.cmp_data_type as i64);
        hasher.hash_bool(self.is_explicit_range);
        hasher.sum64()
    }

    /// Go `FrameBound.Equals(other)` (`logical_window.go:148`).
    #[must_use]
    pub fn equals(&self, other: &Self) -> bool {
        self.bound_type == other.bound_type
            && self.unbounded == other.unbounded
            && self.num == other.num
            && schema_producer::expression_lists_equal(&self.calc_funcs, &other.calc_funcs)
            && schema_producer::expression_lists_equal(&self.compare_cols, &other.compare_cols)
            && self.cmp_func_tokens == other.cmp_func_tokens
            && self.cmp_data_type == other.cmp_data_type
            && self.is_explicit_range == other.is_explicit_range
    }
}

/// Go `logicalop.WindowFrame` (`logical_window.go:44`).
#[derive(Clone, Debug, Default)]
pub struct WindowFrame {
    /// Go `Type`.
    pub frame_type: FrameType,
    /// Go `Start`.
    pub start: Option<FrameBound>,
    /// Go `End`.
    pub end: Option<FrameBound>,
}

impl WindowFrame {
    /// Go `WindowFrame.Hash64(h)` (`logical_window.go:50`).
    ///
    /// Go's body has a quirk that is reproduced rather than repaired: when
    /// `Start` is nil it hashes the NIL flag and then `End`, and when `Start`
    /// is present it hashes `Start` and NOTHING for `End`. Only one bound is
    /// ever folded in. Silently "fixing" it would make two frames that Go
    /// hashes alike hash differently here, which is the failure mode a memo
    /// cannot survive.
    #[must_use]
    pub fn hash64(&self) -> u64 {
        let mut hasher = new_hash_equaler();
        hasher.hash_int(self.frame_type as i64);
        match &self.start {
            Some(start) => {
                hasher.hash_byte(NOT_NIL_FLAG);
                hasher.hash_uint64(start.hash64());
            }
            None => {
                hasher.hash_byte(NIL_FLAG);
                if let Some(end) = &self.end {
                    hasher.hash_uint64(end.hash64());
                }
            }
        }
        hasher.sum64()
    }

    /// Go `WindowFrame.Equals(other)` (`logical_window.go:62`), which — unlike
    /// `Hash64` — does compare BOTH bounds.
    #[must_use]
    pub fn equals(&self, other: &Self) -> bool {
        self.frame_type == other.frame_type
            && bounds_equal(self.start.as_ref(), other.start.as_ref())
            && bounds_equal(self.end.as_ref(), other.end.as_ref())
    }
}

/// Go `FrameBound.Equals` lifted over the nil cases, which is what
/// `WindowFrame.Equals` calls.
fn bounds_equal(left: Option<&FrameBound>, right: Option<&FrameBound>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => left.equals(right),
        _ => false,
    }
}

/// Go's generated slice framing: a nil/not-nil marker, the length, then each
/// element's own hash.
fn hash_expression_list(hasher: &mut impl Hasher, exprs: &[Expression]) {
    if exprs.is_empty() {
        hasher.hash_byte(NIL_FLAG);
        return;
    }
    hasher.hash_byte(NOT_NIL_FLAG);
    hasher.hash_int(exprs.len() as i64);
    for expr in exprs {
        let mut expr = expr.clone();
        hasher.hash_bytes(expr.hash_code());
    }
}

/// Go `logicalop.LogicalWindow` (`logical_window.go:34`).
#[derive(Clone, Debug, Default)]
pub struct LogicalWindow {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
    /// Go `WindowFuncDescs`, in output-column order — they occupy the LAST
    /// `len(WindowFuncDescs)` columns of this operator's schema.
    pub window_func_descs: Vec<WindowFuncDesc>,
    /// Go `PartitionBy`.
    pub partition_by: Vec<WindowSortItem>,
    /// Go `OrderBy`.
    pub order_by: Vec<WindowSortItem>,
    /// Go `Frame`.
    pub frame: Option<WindowFrame>,
}

impl LogicalWindow {
    /// Go `plancodec.TypeWindow`.
    pub const TYPE: &'static str = "Window";

    /// Go `LogicalWindow.Init(ctx, offset)` (`logical_window.go:283`).
    #[must_use]
    pub fn new(base: BaseLogicalPlan, window_func_descs: Vec<WindowFuncDesc>) -> Self {
        Self {
            base,
            window_func_descs,
            partition_by: Vec::new(),
            order_by: Vec::new(),
            frame: None,
        }
    }

    /// Go `LogicalWindow.GetPartitionBy()` (`logical_window.go:495`).
    #[must_use]
    pub fn get_partition_by(&self) -> &[WindowSortItem] {
        &self.partition_by
    }

    /// Go `LogicalWindow.GetPartitionByCols()` (`logical_window.go:601`).
    #[must_use]
    pub fn get_partition_by_cols(&self) -> Vec<Column> {
        self.partition_by
            .iter()
            .map(|item| item.col.clone())
            .collect()
    }

    /// Go `LogicalWindow.GetWindowResultColumns()` (`logical_window.go:558`):
    /// the TRAILING columns of the schema, one per window function.
    ///
    /// Go slices `p.Schema()` directly and panics if the schema is narrower
    /// than the descriptor list; this returns the whole schema in that case,
    /// which cannot silently name the wrong columns.
    #[must_use]
    pub fn get_window_result_columns<'a>(&self, schema: &'a Schema) -> &'a [Column] {
        let start = schema
            .columns
            .len()
            .saturating_sub(self.window_func_descs.len());
        &schema.columns[start..]
    }

    /// Go `LogicalWindow.PredicatePushDown(predicates)`
    /// (`logical_window.go:334`): a predicate may cross a window ONLY if every
    /// column it reads is a `PARTITION BY` column.
    ///
    /// That is the whole soundness argument: filtering by a partition key
    /// removes whole partitions, and a window function's value within a
    /// surviving partition is unaffected. Filtering by anything else would
    /// change the frame the remaining rows see.
    ///
    /// Returns `(can_be_pushed, cannot_be_pushed)`; the second half stays
    /// above this window.
    #[must_use]
    pub fn predicate_push_down(
        &self,
        predicates: &[Expression],
    ) -> (Vec<Expression>, Vec<Expression>) {
        let partition_cols = Schema::new(self.get_partition_by_cols());
        let mut can_be_pushed = Vec::with_capacity(predicates.len());
        let mut cannot_be_pushed = Vec::with_capacity(predicates.len());
        for cond in predicates {
            if expr_from_schema(cond, &partition_cols) {
                can_be_pushed.push(cond.clone());
            } else {
                cannot_be_pushed.push(cond.clone());
            }
        }
        (can_be_pushed, cannot_be_pushed)
    }

    /// Go `LogicalWindow.PruneColumns(parentUsedCols)`'s LOCAL half
    /// (`logical_window.go:352`): the child cannot be asked for this window's
    /// OWN result columns, so they are removed from the parent's set first;
    /// what the window itself reads is then added.
    ///
    /// The driver prunes `children[0]` with the result, then rebuilds this
    /// operator's schema as the pruned child's schema with the window result
    /// columns appended — see [`Self::rebuild_schema_after_pruning`].
    #[must_use]
    pub fn prune_columns_local(&self, parent_used_cols: &[Column], schema: &Schema) -> Vec<Column> {
        let window_columns = self.get_window_result_columns(schema);
        let mut used: Vec<Column> = parent_used_cols
            .iter()
            .filter(|col| {
                !window_columns
                    .iter()
                    .any(|window_col| window_col.unique_id == col.unique_id)
            })
            .cloned()
            .collect();
        self.extract_used_cols(&mut used);
        used
    }

    /// Go's schema rebuild at the end of `PruneColumns`
    /// (`logical_window.go:375`): the pruned child's schema, then the window
    /// result columns appended.
    #[must_use]
    pub fn rebuild_schema_after_pruning(
        &self,
        pruned_child_schema: &Schema,
        window_columns: &[Column],
    ) -> Schema {
        let mut schema = pruned_child_schema.clone();
        schema.columns.extend(window_columns.iter().cloned());
        schema
    }

    /// Go `LogicalWindow.extractUsedCols(parentUsedCols)`
    /// (`logical_window.go:585`): every column the window functions' arguments
    /// read, plus every `PARTITION BY` and `ORDER BY` column, APPENDED to what
    /// the caller already had.
    pub fn extract_used_cols(&self, parent_used_cols: &mut Vec<Column>) {
        for desc in &self.window_func_descs {
            for arg in &desc.base.args {
                parent_used_cols.extend(extract_columns(arg));
            }
        }
        for item in self.partition_by.iter().chain(&self.order_by) {
            parent_used_cols.push(item.col.clone());
        }
    }

    /// Go `LogicalWindow.DeriveStats(childStats, selfSchema, _, reloads)`
    /// (`logical_window.go:398`): the row count passes through — a window
    /// function adds columns, never rows — the child's columns keep their
    /// NDVs, and each window result column is assumed DISTINCT PER ROW.
    pub fn derive_stats(
        &mut self,
        child_stats: &[StatsInfo],
        self_schema: &Schema,
        reloads: &[bool],
    ) -> Option<(StatsInfo, bool)> {
        let reload = reloads.len() == 1 && reloads[0];
        if !reload {
            if let Some(existing) = self.base.base.stats_info() {
                return Some((existing.clone(), false));
            }
        }
        let child = child_stats.first()?;
        let child_len = self_schema
            .columns
            .len()
            .saturating_sub(self.window_func_descs.len());
        let mut ndvs = Vec::with_capacity(self_schema.columns.len());
        for (i, column) in self_schema.columns.iter().enumerate() {
            if i < child_len {
                // Go indexes a missing key as 0.
                ndvs.push((
                    column.unique_id,
                    child
                        .col_ndvs()
                        .get(&column.unique_id)
                        .copied()
                        .unwrap_or(0.0),
                ));
            } else {
                ndvs.push((column.unique_id, child.row_count()));
            }
        }
        let stats = StatsInfo::new(child.row_count(), ndvs);
        self.base.base.set_stats(Some(stats.clone()));
        Some((stats, true))
    }

    /// Go `LogicalWindow.PreparePossibleProperties(_, infos)`
    /// (`logical_window.go:437`): a window OFFERS
    /// `PARTITION BY ++ ORDER BY` as a single order, because that is the order
    /// its execution already requires.
    ///
    /// Note that this is offered even when both lists are empty — Go builds the
    /// `Orders` slice unconditionally, so it holds one EMPTY order rather than
    /// none.
    pub fn prepare_possible_properties(
        &mut self,
        child: Option<&PossiblePropertiesInfo>,
    ) -> PossiblePropertiesInfo {
        let has_tiflash = child.is_some_and(|info| info.has_tiflash);
        self.base.set_has_tiflash(has_tiflash);
        let order = self
            .partition_by
            .iter()
            .chain(&self.order_by)
            .map(|item| item.col.clone())
            .collect();
        PossiblePropertiesInfo {
            orders: vec![order],
            has_tiflash,
        }
    }

    /// Go `LogicalWindow.ExtractCorrelatedCols()` (`logical_window.go:471`):
    /// every window function argument, then BOTH frame bounds' `CalcFuncs`.
    ///
    /// `CompareCols` is deliberately absent — Go does not walk it, because a
    /// compare column is derived from an `ORDER BY` column and cannot be
    /// correlated on its own.
    #[must_use]
    pub fn extract_correlated_cols(&self) -> Vec<CorrelatedColumn> {
        let mut cor_cols = Vec::with_capacity(self.window_func_descs.len());
        for desc in &self.window_func_descs {
            for arg in &desc.base.args {
                cor_cols.extend(extract_cor_columns(arg));
            }
        }
        if let Some(frame) = &self.frame {
            for bound in [frame.start.as_ref(), frame.end.as_ref()]
                .into_iter()
                .flatten()
            {
                for expr in &bound.calc_funcs {
                    cor_cols.extend(extract_cor_columns(expr));
                }
            }
        }
        cor_cols
    }

    /// Go `LogicalWindow.EqualPartitionBy(newWindow)`
    /// (`logical_window.go:500`): the same SET of partition columns, by unique
    /// id — order does NOT matter, because partitioning is unordered.
    #[must_use]
    pub fn equal_partition_by(&self, other: &Self) -> bool {
        if self.partition_by.len() != other.partition_by.len() {
            return false;
        }
        let ids: std::collections::BTreeSet<i64> = self
            .partition_by
            .iter()
            .map(|item| item.col.unique_id)
            .collect();
        other
            .partition_by
            .iter()
            .all(|item| ids.contains(&item.col.unique_id))
    }

    /// Go `LogicalWindow.EqualOrderBy(ctx, newWindow)`
    /// (`logical_window.go:516`): the same SEQUENCE of columns AND directions —
    /// here order does matter.
    ///
    /// Go compares with `item.Col.Equal(ctx, other)`, which for two columns is
    /// `UniqueID` equality (`pkg/expression/column.go:327`) and needs no
    /// context.
    #[must_use]
    pub fn equal_order_by(&self, other: &Self) -> bool {
        self.order_by.len() == other.order_by.len()
            && self
                .order_by
                .iter()
                .zip(&other.order_by)
                .all(|(a, b)| a.col.unique_id == b.col.unique_id && a.desc == b.desc)
    }

    /// Go `LogicalWindow.EqualFrame(ctx, newWindow)`
    /// (`logical_window.go:530`): the frame type and both bounds' shape, then
    /// the `CalcFuncs` element for element.
    ///
    /// This is NOT [`WindowFrame::equals`]: Go compares only `Type`,
    /// `UnBounded`, `Num` and `CalcFuncs` here, deliberately ignoring
    /// `CompareCols`, `CmpFuncs` and `CmpDataType`, because two windows that
    /// agree on the first four describe the same frame and the rest is derived.
    #[must_use]
    pub fn equal_frame(&self, other: &Self) -> bool {
        let (left, right) = match (&self.frame, &other.frame) {
            (None, None) => return true,
            (Some(left), Some(right)) => (left, right),
            _ => return false,
        };
        if left.frame_type != right.frame_type {
            return false;
        }
        for (a, b) in [(&left.start, &right.start), (&left.end, &right.end)] {
            let (Some(a), Some(b)) = (a, b) else {
                if a.is_some() != b.is_some() {
                    return false;
                }
                continue;
            };
            if a.bound_type != b.bound_type || a.unbounded != b.unbounded || a.num != b.num {
                return false;
            }
            if !schema_producer::expression_lists_equal(&a.calc_funcs, &b.calc_funcs) {
                return false;
            }
        }
        true
    }

    /// Go `LogicalWindow.CheckComparisonForTiFlash(frameBound)`
    /// (`logical_window.go:568`): TiFlash cannot compare a `Duration` against a
    /// `Datetime`/`Timestamp`, in either direction, so such a frame must not be
    /// pushed to it.
    ///
    /// A bound with no `CompareCols` is not a range bound and is always fine.
    /// Go reads `p.OrderBy[0]` unconditionally and would panic on an unordered
    /// window with compare columns, which cannot arise; `true` is returned in
    /// that case rather than panicking.
    #[must_use]
    pub fn check_comparison_for_tiflash(&self, frame_bound: &FrameBound) -> bool {
        if frame_bound.compare_cols.is_empty() {
            return true;
        }
        let Some(order_by) = self.order_by.first() else {
            return true;
        };
        let Some(order_type) = order_by.col.ret_type.as_ref().map(|ty| ty.eval_type()) else {
            return true;
        };
        let Some(calc_type) = frame_bound
            .calc_funcs
            .first()
            .and_then(|expr| expr.static_type().map(|ty| ty.eval_type()))
        else {
            return true;
        };
        let is_time = |ty: EvalType| matches!(ty, EvalType::Datetime | EvalType::Timestamp);
        !((order_type == EvalType::Duration && is_time(calc_type))
            || (calc_type == EvalType::Duration && is_time(order_type)))
    }

    /// This operator's own fields with NO children; see
    /// [`crate::logical::LogicalPlan::clone_shallow`].
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        Self {
            base: self.base.shell(),
            window_func_descs: self.window_func_descs.clone(),
            partition_by: self.partition_by.clone(),
            order_by: self.order_by.clone(),
            frame: self.frame.clone(),
        }
    }
}
