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

//! Coprocessor Selection lowering for planner-owned physical conditions.
//!
//! Go decides predicate placement before `executorBuilder.build`: the
//! conditions `PushDownExprs` admitted ride the reader's cop Selection, and
//! `PbConverter.ExprToPB` converts each of them. This module keeps exactly
//! those retained expressions (never a subset of them) for in-process
//! evaluation of local/staged rows and for the request builder, which
//! converts all of them or, when one has no TiPB form, none -- so what a
//! coprocessor receipt covers is always the complete condition list.
use std::collections::HashSet;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::Datum;
use tidb_expr::expression::Expression;
use tidb_expr::expr_to_pb::{PbConverter, PbConverterContext};
use tidb_expr::truthy_of;
use tidb_exprctx::Columns;

use tidb_driver_error::ExecError;

/// The conjuncts a scan agreed to apply itself, with both the description a
/// lowering reads and the expressions an in-process source evaluates.
#[derive(Clone, Debug)]
pub struct PushedScanFilter {
    filters: Vec<Expression>,
    fast_paths: Vec<Option<FastScanFilter>>,
    /// Whether every condition converts to TiPB under the statement's
    /// context (Go `ExprsToPBList` succeeding): only then may a request carry
    /// the Selection and the operators above it.
    pushable: bool,
}

/// A pushed predicate whose per-row work can be reduced without changing the
/// expression evaluator's SQL semantics. The key is built once when the scan
/// is accepted, rather than once for every row and every `IN` literal.
#[derive(Clone, Debug)]
enum FastScanFilter {
    StringIn {
        column_offset: usize,
        collator: tidb_datatype::Collator,
        /// Go's `builtinInStringSig` builds a `set.StringSet` once and does
        /// hash membership per row. Keep the same O(1)-average lookup shape;
        /// sorting the keys and binary-searching them makes large `IN` lists
        /// (such as hbx-web3's maker list) needlessly O(log n) per row.
        keys: HashSet<Vec<u8>>,
        negated: bool,
    },
    Like {
        column_offset: usize,
        pattern: Vec<u8>,
        escape: u8,
        collation: tidb_charset::Collation,
        negated: bool,
    },
}

impl PushedScanFilter {
    /// Builds the exact conditions already selected by a physical reader.
    ///
    /// These expressions need no second AST pushdown decision.  They are
    /// evaluated by the source for every local/staged row and translated to
    /// the remote request when the expression has a TiKV representation.
    #[must_use]
    pub fn from_physical_conditions(
        mut filters: Vec<Expression>,
        ctx: &tidb_sessionctx::StmtContext,
    ) -> Self {
        // The source evaluates what a coprocessor rebuilds from the request.
        filters
            .iter_mut()
            .for_each(tidb_expr::scalar_function::into_coprocessor_form);
        let pushable = all_describable(&filters, ctx);
        let fast_paths = filters
            .iter()
            .map(FastScanFilter::from_expression)
            .collect();
        Self {
            filters,
            fast_paths,
            pushable,
        }
    }

    /// Whether every locally evaluated physical condition converts to TiPB,
    /// so a coprocessor Selection built from them is the complete condition
    /// list. A backend can only report on the conditions it was sent; this is
    /// what keeps a condition without a TiPB form from being mistaken for one
    /// the coprocessor applied.
    #[must_use]
    pub fn fully_pushable(&self) -> bool {
        self.pushable
    }

    /// The physical conditions in `WHERE` order -- all of them -- for the
    /// request builder's `ExprToPB` conversion.
    #[must_use]
    pub fn conditions(&self) -> &[Expression] {
        &self.filters
    }

    /// Go `corColInDistPlan` over these conditions: whether one reads a
    /// correlated column, whose value changes with every Apply iteration.
    #[must_use]
    pub fn cor_col_in_dist_plan(&self) -> bool {
        self.filters
            .iter()
            .any(|filter| !tidb_expr::simple_expr::extract_cor_columns(filter).is_empty())
    }

    /// Go `corColInFilter`: a reader converts its conditions on every `Open`
    /// (`ConstructListBasedDistExec`), so a correlated column is written to
    /// the request with the value bound for this iteration rather than the
    /// one it held when the executor was built. The conditions share the
    /// correlated binding, so only the conversion verdict is refreshed.
    pub fn redescribe(&mut self, ctx: &tidb_sessionctx::StmtContext) {
        self.pushable = all_describable(&self.filters, ctx);
    }

    /// The built expressions the accepting source evaluates.
    #[must_use]
    pub fn filters(&self) -> &[Expression] {
        &self.filters
    }

    /// Go's physical Selection conditions after expression rewriting.
    ///
    /// Go SplitCNFItems exposes the compiled conditions, including BETWEEN's
    /// two bounds and the casts already inserted by expression construction.
    /// Scan descriptions are not an authority for executable expressions.
    pub fn selection_conditions(&self) -> Vec<Expression> {
        self.filters
            .iter()
            .flat_map(tidb_expr::expr_util::split_cnf_items)
            .collect()
    }

    /// Adds conditions from a later offer using Go `RemoveDupExprs` identity
    /// and mutable-effect rules.
    pub fn conjoin(&mut self, additional: &Self) {
        let mut existing: HashSet<Vec<u8>> = self
            .filters
            .iter_mut()
            .map(|filter| filter.hash_code().to_vec())
            .collect();
        for filter in &additional.filters {
            let mut filter = filter.clone();
            tidb_expr::scalar_function::into_coprocessor_form(&mut filter);
            if !existing.insert(filter.hash_code().to_vec())
                && !tidb_expr::expr_util::is_mutable_effects_expr(&filter)
            {
                continue;
            }
            self.fast_paths
                .push(FastScanFilter::from_expression(&filter));
            self.filters.push(filter);
        }
        // The later offer was judged under its own statement context, where
        // Go's `expr_pushdown_blacklist` refused what TiKV must not see: one
        // refused condition keeps the whole Selection local.
        self.pushable &= additional.pushable;
    }

    /// Whether anything was pushed at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }

    /// Whether `row` satisfies every pushed conjunct.
    ///
    /// The row is evaluated by the same expression evaluator `SelectionExec`
    /// uses, including MySQL's three-valued logic, so moving a conjunct into
    /// the scan cannot change what it means.
    pub fn matches<C: Columns>(
        &self,
        ctx: &C,
        row: tidb_chunk::row::Row<'_>,
    ) -> Result<bool, ExecError> {
        for (filter, fast_path) in self.filters.iter().zip(&self.fast_paths) {
            if let Some(fast_path) = fast_path {
                if !fast_path.matches(row) {
                    return Ok(false);
                }
                continue;
            }
            let value = filter
                .eval(ctx, row)
                .map_err(tidb_driver_error::coprocessor_evaluation_error)?;
            if truthy_of(&value)? != Some(true) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn remapped_columns(&self, keep: &[usize]) -> Option<Self> {
        let mut filters = self.filters.clone();
        for filter in &mut filters {
            remap_expression(filter, keep)?;
        }
        Some(Self {
            fast_paths: filters
                .iter()
                .map(FastScanFilter::from_expression)
                .collect(),
            filters,
            pushable: self.pushable,
        })
    }
}

/// Go `PushDownExprs(.., kv.TiKV)` then `PbConverter.ExprToPB` over one
/// retained condition: under the statement's context (blacklist, parameters,
/// correlated bindings) or, for a caller with no statement, from its
/// literals alone.
#[must_use]
pub fn condition_to_pb(
    expression: &Expression,
    context: Option<&dyn PbConverterContext>,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<tidb_proto::tipb::Expr> {
    let (ctx, bind_values): (&dyn Columns, bool) = match context {
        Some(context) => (context, true),
        None => (&tidb_exprctx::NoColumns, false),
    };
    if !tidb_expr::infer_pushdown::can_exprs_push_down(
        std::slice::from_ref(expression),
        tidb_kv::store_type::StoreType::TiKv,
        ctx.expr_pushdown_blacklist(),
    ) {
        return None;
    }
    PbConverter::new(ctx, bind_values, zone).expr_to_pb(expression)
}

/// Whether `expression` converts to TiPB under `context` (Go `ExprToPB`
/// answering a condition rather than nil).
pub fn pb_condition_exists(expression: &Expression, context: &tidb_sessionctx::StmtContext) -> bool {
    condition_to_pb(expression, Some(context), &context.time_zone()).is_some()
}

fn all_describable(filters: &[Expression], ctx: &tidb_sessionctx::StmtContext) -> bool {
    filters
        .iter()
        .flat_map(tidb_expr::expr_util::split_cnf_items)
        .all(|filter| pb_condition_exists(&filter, ctx))
}

impl FastScanFilter {
    fn from_expression(expression: &Expression) -> Option<Self> {
        let Expression::ScalarFunction(function) = expression else {
            return None;
        };
        let (function, negated) = if function.func_name.lowercase() == "not" {
            let [Expression::ScalarFunction(inner)] = function.args.as_slice() else {
                return None;
            };
            (inner, true)
        } else {
            (function, false)
        };
        let name = function.func_name.lowercase();
        if !matches!(name, "in" | "like") {
            return None;
        }
        let column = function.args.first()?.as_column()?;
        if !column.get_static_type()?.is_string() {
            return None;
        }
        let column_offset = usize::try_from(column.index).ok()?;
        let collation = function.derived_collation();
        // Go's IN hash set retains ConstStrict arguments only. LIKE's cache
        // for context-dependent arguments belongs to the execution context,
        // not this retained filter. Derive immutable specializations from
        // the compiled expression, never the description's saved values.
        if name == "like" {
            let [_, Expression::Constant(pattern), Expression::Constant(escape)] =
                function.args.as_slice()
            else {
                return None;
            };
            let Datum::Int(escape) = escape.literal_value()? else {
                return None;
            };
            return Some(Self::Like {
                column_offset,
                pattern: pattern.literal_value()?.as_raw_bytes()?.to_vec(),
                escape: *escape as u8,
                collation,
                negated,
            });
        }
        if function.args.len() < 2 {
            return None;
        }
        let collator = tidb_datatype::get_collator(collation.name());
        let keys = function.args[1..]
            .iter()
            .map(|argument| match argument {
                Expression::Constant(constant) => constant
                    .literal_value()?
                    .as_raw_bytes()
                    .map(|bytes| collator.key(bytes)),
                _ => None,
            })
            .collect::<Option<HashSet<_>>>()?;
        Some(Self::StringIn {
            column_offset,
            collator,
            keys,
            negated,
        })
    }

    fn matches(&self, row: tidb_chunk::row::Row<'_>) -> bool {
        match self {
            Self::StringIn {
                column_offset,
                collator,
                keys,
                negated,
            } => {
                if row.is_null(*column_offset) {
                    // `NULL IN (...)` and `NULL NOT IN (...)` are both NULL,
                    // which a scan filter must reject just like `truthy_of`.
                    return false;
                }
                let key = collator.key(row.get_string(*column_offset).as_bytes());
                let found = keys.contains(&key);
                if *negated {
                    !found
                } else {
                    found
                }
            }
            Self::Like {
                column_offset,
                pattern,
                escape,
                collation,
                negated,
            } => {
                if row.is_null(*column_offset) {
                    return false;
                }
                let matched = tidb_expr::like_match_with_collation(
                    row.get_string(*column_offset).as_bytes(),
                    pattern,
                    Some(*escape),
                    *collation,
                );
                if *negated {
                    !matched
                } else {
                    matched
                }
            }
        }
    }
}

/// [`remap_expression`] for an index Selection: only a direct `IS NULL`
/// column argument may use the prefix-only layout `null_only`, matching Go's
/// `IsIndexCoveringCondition`; every other column reads the `keep` layout.
pub fn remap_index_expression(
    expression: &mut Expression,
    keep: &[usize],
    null_only: &[usize],
) -> Option<()> {
    let position = |column: &mut tidb_expr::column::Column, layout: &[usize]| -> Option<()> {
        let old = usize::try_from(column.index).ok()?;
        column.index = i64::try_from(layout.iter().position(|kept| *kept == old)?).ok()?;
        Some(())
    };
    match expression {
        Expression::Column(column) => position(column, keep)?,
        Expression::ScalarFunction(function) => {
            if function.func_name.lowercase() == "isnull" {
                if let [Expression::Column(column)] = function.args.as_mut_slice() {
                    return position(column, null_only);
                }
            }
            for argument in &mut function.args {
                remap_index_expression(argument, keep, null_only)?;
            }
        }
        Expression::Constant(_) | Expression::CorrelatedColumn(_) | Expression::ScalarSubQuery(_) => {}
    }
    Some(())
}

/// Rewrites `expression`'s column offsets into the kept-column row space
/// `keep`; `None` when it reads a dropped column.
pub fn remap_expression(expression: &mut Expression, keep: &[usize]) -> Option<()> {
    match expression {
        Expression::Column(column) => {
            let old = usize::try_from(column.index).ok()?;
            column.index = i64::try_from(keep.iter().position(|kept| *kept == old)?).ok()?;
        }
        Expression::ScalarFunction(function) => {
            for argument in &mut function.args {
                remap_expression(argument, keep)?;
            }
        }
        // A correlated column reads the outer row, not a scan column, so
        // pruning the scan's columns leaves it as it is.
        Expression::Constant(_) | Expression::CorrelatedColumn(_) | Expression::ScalarSubQuery(_) => {}
    }
    Some(())
}

/// A one-row staging area an in-process source filters through.
///
/// A source holds its rows as `Datum`s but the evaluator reads chunk rows, so
/// each candidate row is appended here, tested, and only then copied into the
/// output chunk.
pub struct ScanFilterProbe {
    filter: PushedScanFilter,
    ctx: tidb_sessionctx::StmtContext,
    scratch: Chunk,
}

impl ScanFilterProbe {
    /// Go `corColInFilter` on `Open`: re-describes the conditions when one
    /// reads a correlated column, so the request carries this iteration's
    /// binding. Returns whether the descriptions were rebuilt.
    pub fn redescribe_correlated(&mut self) -> bool {
        if !self.filter.cor_col_in_dist_plan() {
            return false;
        }
        self.filter.redescribe(&self.ctx);
        true
    }

    /// A probe evaluating `filter` under `ctx` over the one-row `scratch`.
    pub fn new(filter: PushedScanFilter, ctx: tidb_sessionctx::StmtContext, scratch: Chunk) -> Self {
        Self {
            filter,
            ctx,
            scratch,
        }
    }

    /// Adds a later accepted offer to the existing conjunction.
    ///
    /// The additional filter is compiled against the source's current row
    /// space, which is also the row space described by `scratch`.
    pub fn conjoin(&mut self, additional: &PushedScanFilter) {
        self.filter.conjoin(additional);
    }

    /// Whether `row` passes every pushed conjunct.
    pub fn admits(&mut self, row: &[Datum]) -> Result<bool, ExecError> {
        self.scratch.reset();
        for (column, value) in row.iter().enumerate() {
            self.scratch.append_datum(column, value);
        }
        // The filter evaluates in the coprocessor's seat, so its warnings
        // follow TiKV's reporting contract: distinct messages per response,
        // not one per row (see `StmtContext::enter_cop_eval`).
        let _cop = self.ctx.enter_cop_eval();
        self.filter.matches(&self.ctx, self.scratch.get_row(0))
    }

    /// This probe over the kept-column row space `keep`.
    pub fn remapped_columns(&self, keep: &[usize], scratch: Chunk) -> Option<Self> {
        Some(Self {
            filter: self.filter.remapped_columns(keep)?,
            ctx: self.ctx.clone(),
            scratch,
        })
    }

    /// The probe filter's conditions.
    pub fn conditions(&self) -> &[Expression] {
        self.filter.conditions()
    }

    /// Whether every condition of the probe filter converts to TiPB.
    pub fn fully_pushable(&self) -> bool {
        self.filter.fully_pushable()
    }
}
