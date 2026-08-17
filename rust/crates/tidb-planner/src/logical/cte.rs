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

//! Go `pkg/planner/core/operator/logicalop/logical_cte.go`: `LogicalCTE` and
//! the `CTEClass` it points at, and
//! `pkg/planner/core/operator/logicalop/logical_cte_table.go`:
//! `LogicalCTETable`, the reference to a materialised CTE.
//!
//! SEED of `pkg/planner/core`. Both were [`crate::logical::TodoLogicalOp`]
//! before this batch.
//!
//! # Where this port DOES share, and why
//!
//! The [`crate::logical`] header's rule is that the CHILD edge is owned, never
//! `Rc`. A CTE is the one place in `logicalop` where Go shares deliberately and
//! the sharing is load-bearing rather than incidental:
//!
//! * `LogicalCTE.Cte *CTEClass` is one class per CTE and one `LogicalCTE` per
//!   REFERENCE to it. `PredicatePushDown` appends to `Cte.PushDownPredicates`
//!   from every reference, and `DeriveStats` reads the accumulated list once;
//!   an owned copy per reference would silently lose every predicate but one.
//! * `LogicalCTE.SeedStat *property.StatsInfo` is aliased by every
//!   `LogicalCTETable` for the same storage. Go's `DeriveStats` writes
//!   `*p.SeedStat = *resStat` — assigning THROUGH the pointer, with the source
//!   comment "Changing the pointer so that SeedStat in LogicalCTETable can get
//!   the new stat".
//!
//! Both are therefore [`Rc`]`<`[`RefCell`]`<...>>` here. That is Go's model,
//! not a relaxation of the child rule: neither is a child edge, and neither is
//! reached by a tree walk.
//!
//! # Narrowings, by name
//!
//! * `DeriveStats` calls `utilfuncp.DoOptimize` — it OPTIMISES the seed and
//!   recursive parts in place before reading their stats. There is no optimizer
//!   driver in this crate (see the [`crate`] header), so
//!   [`LogicalCTE::derive_stats`] takes the seed's and the recursive part's
//!   already-derived profiles and does the part that is Go's own arithmetic.
//! * `PredicatePushDown` composes with
//!   `ruleutil.ResolveExprAndReplace(expr, p.Cte.ColumnMap)` and
//!   `expression.ComposeCNFCondition`; the DECISION is
//!   [`LogicalCTE::predicate_push_down`] and the composition is the driver's.
//! * `CTEClass.MemoryUsage` sums `PhysicalPlan.MemoryUsage`, which is not
//!   transcreated.
//! * `LogicalCTE.OnlyUsedAsStorage` drives `p.SetChildren(SeedPartLogicalPlan)`
//!   inside `DeriveStats`; that re-parenting moves a plan and belongs to the
//!   driver, so the flag is carried and the move is not performed here.

use std::cell::RefCell;
use std::rc::Rc;

use tidb_expr::column::{Column, CorrelatedColumn};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::simple_expr::extract_cor_columns;

use crate::logical::{BaseLogicalPlan, LogicalPlan};
use crate::plan_base::PossiblePropertiesInfo;
use crate::stats_info::StatsInfo;

/// Go `logicalop.CTEClass` (`logical_cte.go:51`): "holds the information and
/// plan for a CTE".
///
/// Go's own note: most fields mirror `cteInfo`, but `cteInfo` is used while
/// BUILDING the plan and `CTEClass` is used for building the executor too.
#[derive(Debug, Default)]
pub struct CteClass {
    /// Go `IsDistinct`: the union between the seed and the recursive part is
    /// `DISTINCT` rather than `ALL`.
    pub is_distinct: bool,
    /// Go `SeedPartLogicalPlan`.
    ///
    /// Boxed because a plan cannot contain itself by value. This is a separate
    /// tree ROOT, not a child edge — no walk on [`LogicalPlan`] descends into
    /// it, and it must be torn down with
    /// [`LogicalPlan::dismantle`](crate::logical::LogicalPlan::dismantle)
    /// separately.
    pub seed_part_logical_plan: Option<Box<LogicalPlan>>,
    /// Go `RecursivePartLogicalPlan`, `None` for a non-recursive CTE.
    pub recursive_part_logical_plan: Option<Box<LogicalPlan>>,
    /// Go `IDForStorage`.
    pub id_for_storage: i32,
    /// Go `OptFlag`: the optimiser flag set for the whole CTE.
    pub opt_flag: u64,
    /// Go `HasLimit`.
    pub has_limit: bool,
    /// Go `LimitBeg`.
    pub limit_beg: u64,
    /// Go `LimitEnd`.
    pub limit_end: u64,
    /// Go `IsInApply`.
    pub is_in_apply: bool,
    /// Go `PushDownPredicates`: "may be push-downed by different references",
    /// which is why the class is shared; see this module's header.
    pub push_down_predicates: Vec<Expression>,
    /// Go `ColumnMap`, keyed by the column's `HashCode`.
    pub column_map: std::collections::BTreeMap<Vec<u8>, Column>,
    /// Go `IsOuterMostCTE`.
    pub is_outer_most_cte: bool,
}

/// What [`LogicalCTE::predicate_push_down`] resolved.
///
/// Go's function ALWAYS returns `predicates` to the parent — a CTE never
/// absorbs a filter on its parent's behalf, it only records a copy for the seed
/// to be re-optimised with. These variants say what gets recorded.
#[derive(Clone, Debug)]
pub enum CtePredicatePushDown {
    /// Go's two early returns (`logical_cte.go:105`, `:108`): a recursive CTE
    /// ("Doesn't support recursive CTE yet") or a non-outermost one records
    /// NOTHING.
    Unsupported,
    /// Go's `len(pushedPredicates) == 0` branch (`logical_cte.go:124`): every
    /// candidate was dropped, so a literal `1` is recorded. That constant is
    /// what makes the accumulated DNF in `DeriveStats` degenerate to "no
    /// filter" — one reference with no predicate must not let the others'
    /// predicates restrict the shared seed.
    RecordAlwaysTrue,
    /// The predicates to record, in order, after
    /// `ruleutil.ResolveExprAndReplace` through `Cte.ColumnMap` and
    /// `ComposeCNFCondition`; see this module's header.
    Record(Vec<Expression>),
}

/// Go `logicalop.LogicalCTE` (`logical_cte.go:34`).
#[derive(Clone, Debug, Default)]
pub struct LogicalCTE {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
    /// Go `Cte *CTEClass`; see this module's header for the `Rc`.
    pub cte: Option<Rc<RefCell<CteClass>>>,
    /// Go `CteAsName`.
    pub cte_as_name: String,
    /// Go `CteName`.
    pub cte_name: String,
    /// Go `SeedStat *property.StatsInfo`, aliased by every
    /// [`LogicalCTETable`] for the same storage.
    pub seed_stat: Option<Rc<RefCell<StatsInfo>>>,
    /// Go `OnlyUsedAsStorage`.
    pub only_used_as_storage: bool,
}

impl LogicalCTE {
    /// Go `plancodec.TypeCTE`.
    pub const TYPE: &'static str = "CTE";

    /// Go `LogicalCTE.Init(ctx, offset)` (`logical_cte.go:45`).
    #[must_use]
    pub fn new(base: BaseLogicalPlan, cte: Rc<RefCell<CteClass>>) -> Self {
        Self {
            base,
            cte: Some(cte),
            cte_as_name: String::new(),
            cte_name: String::new(),
            seed_stat: None,
            only_used_as_storage: false,
        }
    }

    /// Go `LogicalCTE.PredicatePushDown(predicates)`
    /// (`logical_cte.go:103`); see [`CtePredicatePushDown`].
    ///
    /// The correlated-column filter is Go's own caution, kept with its reason:
    /// "The filter might change the correlated status of the cte. We forbid the
    /// push down that makes the change for now." It applies only when the CTE
    /// is NOT inside an apply — inside one, correlation is expected.
    #[must_use]
    pub fn predicate_push_down(&self, predicates: &[Expression]) -> CtePredicatePushDown {
        let Some(cte) = &self.cte else {
            return CtePredicatePushDown::Unsupported;
        };
        let cte = cte.borrow();
        if cte.recursive_part_logical_plan.is_some() || !cte.is_outer_most_cte {
            return CtePredicatePushDown::Unsupported;
        }
        let mut pushed: Vec<Expression> = predicates.to_vec();
        if !cte.is_in_apply {
            pushed.retain(|pred| extract_cor_columns(pred).is_empty());
        }
        if pushed.is_empty() {
            return CtePredicatePushDown::RecordAlwaysTrue;
        }
        CtePredicatePushDown::Record(pushed)
    }

    /// Go `LogicalCTE.PruneColumns(_)` (`logical_cte.go:132`), whose whole body
    /// is `return p, nil`.
    ///
    /// Go's comment says why, and it is not an omission: "LogicalCTE just do an
    /// empty function call. It's logical optimize is indivisual phase." The
    /// seed is optimised as its own plan, so this operator's parent cannot
    /// prune through it.
    #[must_use]
    pub const fn prune_columns_local() -> bool {
        false
    }

    /// Go `LogicalCTE.PushDownTopN(topN)` (`logical_cte.go:139`): a TopN never
    /// enters a CTE, it is simply ATTACHED above it.
    ///
    /// See [`crate::logical::LogicalTopN::attach_child`], which is where the
    /// TopN may still collapse into a limit.
    #[must_use]
    pub fn push_down_topn(self, topn: Option<crate::logical::LogicalTopN>) -> LogicalPlan {
        let plan = LogicalPlan::CTE(self);
        match topn {
            Some(topn) => topn.attach_child(plan),
            None => plan,
        }
    }

    /// Go `LogicalCTE.DeriveStats(_, selfSchema, _, reloads)`'s ARITHMETIC
    /// (`logical_cte.go:167`), once the caller has optimised the seed and the
    /// recursive part; see this module's header for `utilfuncp.DoOptimize`.
    ///
    /// The mapping is POSITIONAL: this operator's i-th output column takes its
    /// NDV from the seed plan's i-th column, because a CTE reference renames
    /// but does not reorder. The recursive part's NDVs are ADDED on top, in the
    /// same positional way.
    ///
    /// Row counts: a non-distinct recursive CTE adds the recursive part's rows;
    /// a `DISTINCT` one takes `distinct_row_count`, which is Go's
    /// `cardinality.EstimateColsNDVWithMatchedLen` over this operator's whole
    /// schema — a NAMED boundary, and `None` leaves the seed's count rather
    /// than guessing.
    ///
    /// This also writes THROUGH [`Self::seed_stat`], which is what makes every
    /// [`LogicalCTETable`] for the same storage see the seed profile.
    pub fn derive_stats(
        &mut self,
        seed: &StatsInfo,
        seed_schema: &Schema,
        recursive: Option<(&StatsInfo, &Schema)>,
        self_schema: &Schema,
        distinct_row_count: Option<f64>,
        reloads: &[bool],
    ) -> (StatsInfo, bool) {
        let reload = reloads.len() == 1 && reloads[0];
        if !reload {
            if let Some(existing) = self.base.base.stats_info() {
                return (existing.clone(), false);
            }
        }
        if let Some(seed_stat) = &self.seed_stat {
            *seed_stat.borrow_mut() = seed.clone();
        }
        let mut row_count = seed.row_count();
        let mut ndvs: Vec<(i64, f64)> = self_schema
            .columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let from_seed = seed_schema
                    .columns
                    .get(i)
                    .and_then(|seed_col| seed.col_ndvs().get(&seed_col.unique_id))
                    .copied()
                    .unwrap_or(0.0);
                (col.unique_id, from_seed)
            })
            .collect();
        if let Some((recur, recur_schema)) = recursive {
            for (i, entry) in ndvs.iter_mut().enumerate() {
                entry.1 += recur_schema
                    .columns
                    .get(i)
                    .and_then(|recur_col| recur.col_ndvs().get(&recur_col.unique_id))
                    .copied()
                    .unwrap_or(0.0);
            }
            let is_distinct = self
                .cte
                .as_ref()
                .is_some_and(|cte| cte.borrow().is_distinct);
            if is_distinct {
                if let Some(distinct) = distinct_row_count {
                    row_count = distinct;
                }
            } else {
                row_count += recur.row_count();
            }
        }
        let stats = StatsInfo::new(row_count, ndvs);
        self.base.base.set_stats(Some(stats.clone()));
        (stats, true)
    }

    /// Go `LogicalCTE.PreparePossibleProperties(_, childrenProperties)`
    /// (`logical_cte.go:239`): a CTE offers NO order, and its TiFlash capability
    /// comes from its children when it HAS any and from the SEED plan
    /// otherwise.
    ///
    /// Note the difference from every other operator: Go ignores nil children
    /// entirely here and takes the first NON-NIL one's answer as the seed of
    /// the conjunction, rather than starting from `len(children) > 0`. A CTE
    /// whose children are all nil falls through to the seed.
    pub fn prepare_possible_properties(
        &mut self,
        children_properties: &[Option<PossiblePropertiesInfo>],
        seed_has_tiflash: bool,
    ) -> PossiblePropertiesInfo {
        let mut has_tiflash = false;
        let mut has_valid_child = false;
        for child in children_properties.iter().flatten() {
            if has_valid_child {
                has_tiflash = has_tiflash && child.has_tiflash;
            } else {
                has_tiflash = child.has_tiflash;
                has_valid_child = true;
            }
        }
        if !has_valid_child {
            has_tiflash = seed_has_tiflash;
        }
        self.base.set_has_tiflash(has_tiflash);
        PossiblePropertiesInfo {
            orders: Vec::new(),
            has_tiflash,
        }
    }

    /// Go `LogicalCTE.ExtractCorrelatedCols()` (`logical_cte.go:271`): the
    /// correlated columns of the SEED subtree, then of the recursive one.
    ///
    /// This is the only operator whose correlated columns come from a plan that
    /// is not one of its children; see [`extract_correlated_cols_for_plan`].
    #[must_use]
    pub fn extract_correlated_cols(&self) -> Vec<CorrelatedColumn> {
        let Some(cte) = &self.cte else {
            return Vec::new();
        };
        let cte = cte.borrow();
        let mut cor_cols = Vec::new();
        if let Some(seed) = &cte.seed_part_logical_plan {
            cor_cols.extend(extract_correlated_cols_for_plan(seed));
        }
        if let Some(recursive) = &cte.recursive_part_logical_plan {
            cor_cols.extend(extract_correlated_cols_for_plan(recursive));
        }
        cor_cols
    }

    /// This operator's own fields with NO children; see
    /// [`crate::logical::LogicalPlan::clone_shallow`].
    ///
    /// The `Rc`s are cloned as HANDLES, which is exactly Go's pointer copy: a
    /// shallow copy of a `LogicalCTE` still refers to the same `CTEClass`.
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        Self {
            base: self.base.shell(),
            cte: self.cte.clone(),
            cte_as_name: self.cte_as_name.clone(),
            cte_name: self.cte_name.clone(),
            seed_stat: self.seed_stat.clone(),
            only_used_as_storage: self.only_used_as_storage,
        }
    }
}

/// Go `logicalop.LogicalCTETable` (`logical_cte_table.go:24`): a reference to
/// the materialised result of a [`LogicalCTE`].
#[derive(Clone, Debug, Default)]
pub struct LogicalCTETable {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
    /// Go `SeedStat`, the SAME profile the producing [`LogicalCTE`] writes
    /// through; see that module header.
    pub seed_stat: Option<Rc<RefCell<StatsInfo>>>,
    /// Go `Name`.
    pub name: String,
    /// Go `IDForStorage`.
    pub id_for_storage: i32,
    /// Go `SeedSchema`, used "only in columnStatsUsageCollector to get column
    /// mapping".
    pub seed_schema: Option<Schema>,
}

impl LogicalCTETable {
    /// Go `plancodec.TypeCTETable`.
    pub const TYPE: &'static str = "CTETable";

    /// Go `LogicalCTETable.Init(ctx, offset)` (`logical_cte_table.go:36`).
    #[must_use]
    pub fn new(base: BaseLogicalPlan, seed_stat: Rc<RefCell<StatsInfo>>) -> Self {
        Self {
            base,
            seed_stat: Some(seed_stat),
            name: String::new(),
            id_for_storage: 0,
            seed_schema: None,
        }
    }

    /// Go `LogicalCTETable.DeriveStats(_, _, _, reloads)`
    /// (`logical_cte_table.go:64`): the seed's profile, adopted whole.
    ///
    /// There is no arithmetic: this operator READS the storage the producing
    /// CTE wrote, so its profile IS the seed's. `None` means the seed has not
    /// been derived yet, which is Go's nil `SeedStat` and would set nil stats.
    pub fn derive_stats(&mut self, reloads: &[bool]) -> Option<(StatsInfo, bool)> {
        let reload = reloads.len() == 1 && reloads[0];
        if !reload {
            if let Some(existing) = self.base.base.stats_info() {
                return Some((existing.clone(), false));
            }
        }
        let stats = self.seed_stat.as_ref()?.borrow().clone();
        self.base.base.set_stats(Some(stats.clone()));
        Some((stats, true))
    }

    /// This operator's own fields with NO children; see
    /// [`crate::logical::LogicalPlan::clone_shallow`].
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        Self {
            base: self.base.shell(),
            seed_stat: self.seed_stat.clone(),
            name: self.name.clone(),
            id_for_storage: self.id_for_storage,
            seed_schema: self.seed_schema.clone(),
        }
    }
}

/// Go `coreusage.ExtractCorrelatedCols4LogicalPlan(p)`
/// (`pkg/planner/util/coreusage/`): every correlated column of `plan` and of
/// every plan below it.
///
/// Written with an explicit stack; see the [`crate::logical`] header for why a
/// tree walk here is never recursive.
#[must_use]
pub fn extract_correlated_cols_for_plan(plan: &LogicalPlan) -> Vec<CorrelatedColumn> {
    let mut cor_cols = Vec::new();
    plan.walk_preorder(&mut |node| cor_cols.extend(node.extract_correlated_cols()));
    cor_cols
}

/// Go `logicalop.GetHasTiFlash(lp)` (`logical_plans_misc.go:128`): the
/// `hasTiFlash` bit `PreparePossibleProperties` left on a plan's base.
///
/// Go returns false for a nil plan; `None` is that nil.
#[must_use]
pub fn get_has_tiflash(plan: Option<&LogicalPlan>) -> bool {
    plan.is_some_and(|plan| plan.base().has_tiflash())
}
