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
//! [`ScanPredicate`] is the converter's parse of one condition, produced at
//! conversion time and never retained by a scan.
use std::collections::HashSet;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::expression::Expression;
use tidb_expr::pushdown_catalog::PbConverterContext;
use tidb_expr::{truthy_of, Columns};

use crate::executor::ExecError;

/// The comparison operators a scan filter accepts, which are exactly the ones
/// the bounded TiKV Selection lowering speaks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanComparisonOp {
    /// `=`
    Eq,
    /// `!=` / `<>`
    Ne,
    /// `<`
    Lt,
    /// `<=`
    Le,
    /// `>`
    Gt,
    /// `>=`
    Ge,
}

/// One pushed conjunct, described independently of how it is evaluated.
///
/// A description is what a coprocessor lowering reads; the paired
/// [`Expression`] is what an in-process source evaluates. Both halves describe
/// the same conjunct, so a lowering may refuse any subset of them without
/// changing an answer -- see [`PushedScanFilter`].
///
/// # The `collation` a string comparison runs in
///
/// It is the FUNCTION's derived collation, not the column's declared one. Go
/// derives it over ALL the arguments (`expression/collation.go:290` for
/// `ast.In`, `:282` for `ast.Like`) and writes it onto the function's own
/// `FieldType`, which is what `ExprToPB` sends and what TiKV picks its
/// collator from. The two differ whenever an argument carries an explicit
/// `COLLATE`: that is EXPLICIT coercibility, and it outranks the column's
/// IMPLICIT.
///
/// Every variant that compares strings therefore CARRIES that collation
/// rather than letting each consumer re-derive it from `column_type`. Five
/// consumers had re-derived it, and each got a different answer than the
/// expression beside it; carrying it is what makes "the description and the
/// expression agree" true by construction instead of by repetition.
///
/// Each description is derived from the already-built physical expression,
/// so this collation is the function collation the planner retained.
#[derive(Clone, Debug, PartialEq)]
pub enum ScanPredicate {
    /// A column compared with a constant, in either operand order.
    Compare(ScanComparison),
    /// A comparison between two columns of the scanned table.
    ///
    /// Go's `scalarFuncToPBExpr` sends both `ColumnRef` children when the
    /// comparison is supported by TiKV. Keeping this separate from
    /// [`ScanComparison`] preserves the latter's literal-oriented contract
    /// for callers that build range predicates.
    ColumnCompare(ScanColumnComparison),
    /// `column IS NULL`, or `column IS NOT NULL` when `negated`.
    IsNull {
        /// Zero-based offset of the column in the scan's output row.
        column_offset: u32,
        /// The column's declared type.
        column_type: FieldType,
        /// `true` for the `IS NOT NULL` spelling.
        negated: bool,
    },
    /// `column IN (constants)`, or `NOT IN` when `negated`. The list is never
    /// empty and never holds [`Datum::Null`]: a NULL member makes the whole
    /// predicate's three-valued result depend on the non-matching case, which
    /// is not a shape this description promises, so such a conjunct stays
    /// residual.
    In {
        /// Zero-based offset of the column in the scan's output row.
        column_offset: u32,
        /// The column's declared type.
        column_type: FieldType,
        /// The constant list, in source order.
        literals: Vec<Datum>,
        /// `true` for the `NOT IN` spelling.
        negated: bool,
        /// The collation the comparison RUNS IN -- see [`ScanPredicate`]'s
        /// note on it.
        collation: tidb_datatype::Collation,
    },
    /// A pushable string scalar tested against non-NULL string constants.
    ///
    /// Go chooses `IN`'s signature from the tested expression's evaluation
    /// type, so this is not limited to a bare column: calls such as
    /// `SUBSTRING(column, 1, 2)` retain their complete TiPB scalar tree.
    ScalarIn {
        /// The string expression being tested.
        tested: tidb_expr::pushdown_catalog::PbScalar,
        /// The non-empty constant list, in source order.
        literals: Vec<Datum>,
        /// `true` for the `NOT IN` spelling.
        negated: bool,
        /// The collation the comparison RUNS IN -- see [`ScanPredicate`]'s
        /// note on it.
        collation: tidb_datatype::Collation,
    },
    /// `string_column LIKE constant_pattern` with a constant escape byte.
    /// `NOT LIKE` is represented by the existing [`Self::Not`] wrapper, as
    /// Go rewrites it to `UnaryNotInt(LikeSig(...))` before protobuf lowering.
    Like {
        /// Zero-based offset of the tested column in the scan output.
        column_offset: u32,
        /// The column's declared string type and collation.
        column_type: FieldType,
        /// Pattern bytes exactly as parsed from the SQL literal.
        pattern: Vec<u8>,
        /// Explicit escape byte, or the session default supplied by rewrite.
        escape: u8,
        /// The collation the comparison RUNS IN -- see [`ScanPredicate`]'s
        /// note on it.
        collation: tidb_datatype::Collation,
    },
    /// A builtin function call whose `tipb.ScalarFuncSig` the push-down
    /// catalog resolved and whose signature TiKV evaluates.
    ///
    /// The whole call -- name, signature, and the operand tree -- lives in
    /// [`tidb_expr::pushdown_catalog`], which is also what decides whether
    /// TiKV may evaluate it; nothing here holds a second opinion about
    /// either. A call reaching this variant has already been admitted;
    /// whether it can also be *encoded* is the lowering's own question
    /// (`pushdown_catalog::to_pb`), and refusing there costs network only.
    Builtin(tidb_expr::pushdown_catalog::PbScalar),
    /// One source conjunct that Go expands into multiple Selection
    /// conditions. The current producer is non-negated `BETWEEN`, whose
    /// lower and upper comparisons remain one local filter.
    And(Vec<ScanPredicate>),
    /// A disjunction of two or more descriptions, flattened out of the source
    /// `OR` chain in source order.
    Or(Vec<ScanPredicate>),
    /// `NOT description`.
    Not(Box<ScanPredicate>),
}

/// A column-versus-constant comparison.
///
/// This names the scan input offset, the operator, the constant, and which
/// side the column was written on, which is everything
/// `PhysicalSelection.ToPB` needs and nothing that ties the description to
/// in-process evaluation.
#[derive(Clone, Debug, PartialEq)]
pub struct ScanComparison {
    /// Zero-based offset of the column in the scan's output row.
    pub column_offset: u32,
    /// The column's declared type, which decides whether a lowering may
    /// treat the comparison as the signed-BIGINT shape TiKV accepts.
    pub column_type: FieldType,
    /// The constant's type after Go-equivalent constant folding and any
    /// comparison-domain cast. TiPB carries this metadata on the literal
    /// leaf, so retaining only the [`Datum`] is not sufficient for DECIMAL
    /// precision/scale or temporal FSP.
    pub literal_type: FieldType,
    /// The comparison operator, as written.
    pub op: ScanComparisonOp,
    /// The already-evaluated constant operand. Never [`Datum::Null`]: a NULL
    /// comparison is unknown for every row, which is not the "filter" shape
    /// this split describes, so such a conjunct stays residual.
    pub literal: Datum,
    /// `true` when the column was written on the left (`a > 5`), `false` for
    /// the flipped spelling (`5 < a`). The lowering preserves operand order
    /// rather than canonicalizing it, as the source protobuf does.
    pub column_on_left: bool,
    /// The collation the comparison RUNS IN -- see [`ScanPredicate`]'s note
    /// on it. Meaningful only for a string comparison.
    pub collation: tidb_datatype::Collation,
}

/// A source-ordered column-to-column comparison.
#[derive(Clone, Debug, PartialEq)]
pub struct ScanColumnComparison {
    /// Zero-based offset of the left operand in the scan output.
    pub left_offset: u32,
    /// Declared type of the left operand.
    pub left_type: FieldType,
    /// Zero-based offset of the right operand in the scan output.
    pub right_offset: u32,
    /// Declared type of the right operand.
    pub right_type: FieldType,
    /// The comparison operator, as written.
    pub op: ScanComparisonOp,
}

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
        collation: tidb_datatype::Collation,
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
    pub(crate) fn from_physical_conditions(
        mut filters: Vec<Expression>,
        ctx: &crate::StmtContext,
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
    pub(crate) fn fully_pushable(&self) -> bool {
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
    pub(crate) fn has_correlated_columns(&self) -> bool {
        self.filters
            .iter()
            .any(|filter| !tidb_expr::simple_expr::extract_cor_columns(filter).is_empty())
    }

    /// Go `corColInFilter`: a reader converts its conditions on every `Open`
    /// (`ConstructListBasedDistExec`), so a correlated column is written to
    /// the request with the value bound for this iteration rather than the
    /// one it held when the executor was built. The conditions share the
    /// correlated binding, so only the conversion verdict is refreshed.
    pub(crate) fn redescribe(&mut self, ctx: &crate::StmtContext) {
        self.pushable = all_describable(&self.filters, ctx);
    }

    /// The built expressions the accepting source evaluates.
    #[must_use]
    pub(crate) fn filters(&self) -> &[Expression] {
        &self.filters
    }

    /// Go's physical Selection conditions after expression rewriting.
    ///
    /// Go SplitCNFItems exposes the compiled conditions, including BETWEEN's
    /// two bounds and the casts already inserted by expression construction.
    /// Scan descriptions are not an authority for executable expressions.
    pub(crate) fn selection_conditions(&self) -> Vec<Expression> {
        self.filters
            .iter()
            .flat_map(tidb_expr::expr_util::split_cnf_items)
            .collect()
    }

    /// Adds conditions from a later offer using Go `RemoveDupExprs` identity
    /// and mutable-effect rules.
    fn conjoin(&mut self, additional: &Self) {
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
                .map_err(crate::driver::coprocessor_evaluation_error)?;
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

/// Go `PbConverter.ExprToPB` over one retained condition, first stage: the
/// converter's parse of it under the statement's context (blacklist,
/// parameters, correlated bindings) or, for a caller with no statement, from
/// its strict literals alone. The request builder encodes the result against
/// the scan's column descriptors and keeps none of it.
pub fn describe_request_condition(
    expression: &Expression,
    context: Option<&dyn PbConverterContext>,
) -> Result<ScanPredicate, ExecError> {
    match context {
        Some(context) => describe_execution_condition(expression, context),
        None => describe_compiled_condition(expression)
            .ok_or_else(|| ExecError::unsupported("cop predicate lowering")),
    }
}

/// Whether `expression` converts to TiPB under `context` (Go `ExprToPB`
/// answering a condition rather than nil).
pub(crate) fn pb_condition_exists(expression: &Expression, context: &crate::StmtContext) -> bool {
    describe_execution_condition(expression, context).is_ok()
}

fn all_describable(filters: &[Expression], ctx: &crate::StmtContext) -> bool {
    filters
        .iter()
        .flat_map(tidb_expr::expr_util::split_cnf_items)
        .all(|filter| describe_execution_condition(&filter, ctx).is_ok())
}

/// Go `ExprToPB` reads the compiled function and its arguments. This immutable
/// description adapter copies strict literals without rewriting or folding SQL.
fn describe_compiled_condition(expression: &Expression) -> Option<ScanPredicate> {
    describe_condition(expression, None).ok()
}

/// Go PbConverter owns the current statement context. Lower the retained
/// expression directly; do not rewrite a clone to replace its leaves.
pub(crate) fn describe_execution_condition(
    expression: &Expression,
    context: &dyn PbConverterContext,
) -> Result<ScanPredicate, ExecError> {
    if !crate::pushdown_blacklist::compiled_blacklist_admits(
        expression,
        context,
        tidb_expr::infer_pushdown::PushDownStore::TiKv,
    ) {
        return Err(ExecError::unsupported("cop predicate is disabled for TiKV"));
    }
    describe_condition(expression, Some(context))
}

fn describe_condition(
    expression: &Expression,
    context: Option<&dyn PbConverterContext>,
) -> Result<ScanPredicate, ExecError> {
    use tidb_expr::pushdown_catalog::{from_expression, from_expression_in, PbScalar};

    fn unsupported() -> ExecError {
        ExecError::unsupported("cop predicate lowering")
    }

    fn scalar(
        expression: &Expression,
        context: Option<&dyn PbConverterContext>,
    ) -> Result<PbScalar, ExecError> {
        match context {
            Some(context) => from_expression_in(expression, context)?,
            None => from_expression(expression),
        }
        .ok_or_else(unsupported)
    }

    fn column(expression: &Expression) -> Option<(u32, FieldType)> {
        let column = expression.as_column()?;
        Some((
            u32::try_from(column.index).ok()?,
            column.get_static_type()?.clone(),
        ))
    }
    fn literal(
        expression: &Expression,
        context: Option<&dyn PbConverterContext>,
    ) -> Result<(Datum, FieldType), ExecError> {
        let (value, field_type) = match expression {
            Expression::Constant(constant) => {
                let value = match context {
                    Some(context) => constant.eval_in(context)?,
                    None => constant.literal_value().ok_or_else(unsupported)?.clone(),
                };
                (value, constant.ret_type.as_ref())
            }
            Expression::CorrelatedColumn(column) if context.is_some() => {
                (column.eval(), column.column.ret_type.as_ref())
            }
            _ => return Err(unsupported()),
        };
        Ok((value, field_type.ok_or_else(unsupported)?.clone()))
    }

    let Expression::ScalarFunction(function) = expression else {
        return Err(unsupported());
    };
    let name = function.func_name.lowercase();
    let args = function.args.as_slice();
    let collation = function.derived_collation();
    match (name, args) {
        ("not", [inner]) => Ok(ScanPredicate::Not(Box::new(describe_condition(
            inner, context,
        )?))),
        ("and" | "or", _) => {
            let branches = args
                .iter()
                .map(|branch| describe_condition(branch, context))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(if name == "and" {
                ScanPredicate::And(branches)
            } else {
                ScanPredicate::Or(branches)
            })
        }
        ("isnull", [argument]) => {
            if let Some((column_offset, column_type)) = column(argument) {
                Ok(ScanPredicate::IsNull {
                    column_offset,
                    column_type,
                    negated: false,
                })
            } else {
                Ok(ScanPredicate::Builtin(scalar(expression, context)?))
            }
        }
        ("like", [tested, pattern, escape]) => {
            let (column_offset, column_type) = column(tested).ok_or_else(unsupported)?;
            if !column_type.is_string() {
                return Err(unsupported());
            }
            let (pattern, _) = literal(pattern, context)?;
            let (Datum::Int(escape), _) = literal(escape, context)? else {
                return Err(unsupported());
            };
            Ok(ScanPredicate::Like {
                column_offset,
                column_type,
                pattern: pattern.as_raw_bytes().ok_or_else(unsupported)?.to_vec(),
                escape: escape as u8,
                collation,
            })
        }
        ("in", [tested, members @ ..]) if !members.is_empty() => {
            let Ok(literals) = members
                .iter()
                .map(|member| literal(member, context))
                .collect::<Result<Vec<_>, _>>()
            else {
                // A member that is not a literal (`a IN (b, 1)`): Go converts
                // the whole `in` call, every argument cast to the comparison
                // type, through the generic signature rows.
                return match scalar(expression, context)? {
                    call @ PbScalar::Call { .. } => Ok(ScanPredicate::Builtin(call)),
                    _ => Err(unsupported()),
                };
            };
            if let Some((column_offset, column_type)) = column(tested) {
                return Ok(ScanPredicate::In {
                    column_offset,
                    column_type,
                    collation,
                    literals: literals.into_iter().map(|(value, _)| value).collect(),
                    negated: false,
                });
            }
            let tested = scalar(tested, context)?;
            if tested.eval_type() != tidb_datatype::EvalType::String
                || literals.iter().any(|(value, field_type)| {
                    field_type.eval_type() != tidb_datatype::EvalType::String
                        || value.as_raw_bytes().is_none()
                })
            {
                return Err(unsupported());
            }
            Ok(ScanPredicate::ScalarIn {
                tested,
                literals: literals.into_iter().map(|(value, _)| value).collect(),
                negated: false,
                collation,
            })
        }
        ("eq" | "ne" | "lt" | "le" | "gt" | "ge", [left, right]) => {
            let op = match name {
                "eq" => ScanComparisonOp::Eq,
                "ne" => ScanComparisonOp::Ne,
                "lt" => ScanComparisonOp::Lt,
                "le" => ScanComparisonOp::Le,
                "gt" => ScanComparisonOp::Gt,
                _ => ScanComparisonOp::Ge,
            };
            if let (Some((left_offset, left_type)), Some((right_offset, right_type))) =
                (column(left), column(right))
            {
                return Ok(ScanPredicate::ColumnCompare(ScanColumnComparison {
                    left_offset,
                    left_type,
                    right_offset,
                    right_type,
                    op,
                }));
            }
            if let Some((offset, field_type)) = column(left).or_else(|| column(right)) {
                let column_on_left = column(left).is_some();
                let constant = if column_on_left { right } else { left };
                // The other side is a literal, or a call that the generic
                // catalog conversion below describes whole (`b * 2 = c`).
                if let Ok((literal, literal_type)) = literal(constant, context) {
                    return Ok(ScanPredicate::Compare(ScanComparison {
                        column_offset: offset,
                        column_type: field_type,
                        literal_type,
                        op,
                        literal,
                        column_on_left,
                        collation,
                    }));
                }
            }
            // Neither operand is a bare column (an arithmetic or builtin side,
            // as in `sbig + 1 > 999`). Go pushes the comparison expression
            // whole (`scalarExprSupportedByTiKV` walks the tree), so describe
            // it as a builtin call and let the catalog's comparison rows
            // encode it, instead of refusing here.
            match scalar(expression, context)? {
                call @ PbScalar::Call { .. } => Ok(ScanPredicate::Builtin(call)),
                _ => Err(unsupported()),
            }
        }
        _ => match scalar(expression, context)? {
            call @ PbScalar::Call { .. } => Ok(ScanPredicate::Builtin(call)),
            _ => Err(unsupported()),
        },
    }
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
pub(crate) fn remap_index_expression(
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
        Expression::Constant(_) | Expression::CorrelatedColumn(_) => {}
    }
    Some(())
}

pub(crate) fn remap_expression(expression: &mut Expression, keep: &[usize]) -> Option<()> {
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
        Expression::Constant(_) | Expression::CorrelatedColumn(_) => {}
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
    ctx: crate::StmtContext,
    scratch: Chunk,
}

impl ScanFilterProbe {
    /// Go `corColInFilter` on `Open`: re-describes the conditions when one
    /// reads a correlated column, so the request carries this iteration's
    /// binding. Returns whether the descriptions were rebuilt.
    pub(crate) fn redescribe_correlated(&mut self) -> bool {
        if !self.filter.has_correlated_columns() {
            return false;
        }
        self.filter.redescribe(&self.ctx);
        true
    }

    pub(crate) fn new(filter: PushedScanFilter, ctx: crate::StmtContext, scratch: Chunk) -> Self {
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
    pub(crate) fn conjoin(&mut self, additional: &PushedScanFilter) {
        self.filter.conjoin(additional);
    }

    /// Whether `row` passes every pushed conjunct.
    pub(crate) fn admits(&mut self, row: &[Datum]) -> Result<bool, ExecError> {
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

    pub(crate) fn remapped_columns(&self, keep: &[usize], scratch: Chunk) -> Option<Self> {
        Some(Self {
            filter: self.filter.remapped_columns(keep)?,
            ctx: self.ctx.clone(),
            scratch,
        })
    }

    pub(crate) fn conditions(&self) -> &[Expression] {
        self.filter.conditions()
    }

    pub(crate) fn fully_pushable(&self) -> bool {
        self.filter.fully_pushable()
    }
}

/// Builders of the retained physical conditions the scan tests offer.
#[cfg(test)]
pub(crate) mod fixtures {
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_expr::expression::{Expression, ScalarFunction};

    /// A scan-output column at `offset`.
    pub(crate) fn column(offset: usize, field_type: FieldType) -> Expression {
        column_with_id(offset, offset as i64 + 1, field_type)
    }

    /// A scan-output column at `offset` with the given plan column id, which
    /// survives a remap of the offset.
    pub(crate) fn column_with_id(
        offset: usize,
        unique_id: i64,
        field_type: FieldType,
    ) -> Expression {
        let mut column = tidb_expr::column::Column::new(unique_id, field_type);
        column.index = offset as i64;
        Expression::Column(column)
    }

    /// A strict literal.
    pub(crate) fn literal(value: Datum, field_type: FieldType) -> Expression {
        Expression::Constant(tidb_expr::constant::Constant::new(value, field_type))
    }

    /// A call of the builtin `name`.
    pub(crate) fn call(name: &str, args: Vec<Expression>) -> Expression {
        Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new(name),
            FieldType::new(FieldTypeCode::LongLong),
            args,
        ))
    }

    /// `column OP literal` (or `literal OP column`), `name` one of `eq`,
    /// `ne`, `lt`, `le`, `gt`, `ge`.
    pub(crate) fn compare(
        offset: usize,
        column_type: FieldType,
        name: &str,
        value: Datum,
        literal_type: FieldType,
        column_on_left: bool,
    ) -> Expression {
        let column = column(offset, column_type);
        let literal = literal(value, literal_type);
        call(
            name,
            if column_on_left {
                vec![column, literal]
            } else {
                vec![literal, column]
            },
        )
    }

    /// `column IS NULL`, or `column IS NOT NULL` when `negated`.
    pub(crate) fn is_null(offset: usize, field_type: FieldType, negated: bool) -> Expression {
        let test = call("isnull", vec![column(offset, field_type)]);
        if negated {
            call("not", vec![test])
        } else {
            test
        }
    }

    /// The scan-output offsets `expression` reads, in traversal order.
    pub(crate) fn column_offsets(expression: &Expression) -> Vec<usize> {
        tidb_expr::simple_expr::extract_columns(expression)
            .into_iter()
            .map(|column| usize::try_from(column.index).expect("a scan offset"))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_txnkv::Key;

    use tidb_kv::cluster_storage::{
        ClusterSnapshot, ClusterTableStorage, MutationBuffer, SnapshotPairs,
    };
    use crate::driver::{run_select_on, Catalog};
    use crate::kv_table::{KvColumn, KvTable};
    use tidb_kv::storage::StorageError;

    /// A snapshot over a fixed map: the committed half of a cluster read.
    #[derive(Debug, Default)]
    struct MockSnapshot {
        data: BTreeMap<Vec<u8>, Vec<u8>>,
    }

    impl ClusterSnapshot for MockSnapshot {
        fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
            Ok(self.data.get(key.as_bytes()).cloned())
        }

        fn scan(
            &mut self,
            start: &Key,
            end: &Key,
            limit: Option<usize>,
        ) -> Result<SnapshotPairs, StorageError> {
            Ok(self
                .data
                .range(start.as_bytes().to_vec()..end.as_bytes().to_vec())
                .take(limit.unwrap_or(usize::MAX))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        }
    }

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::LongLong)
    }

    fn column(name: &str, id: i64) -> KvColumn {
        KvColumn {
            name: name.to_owned(),
            id,
            field_type: long(),
            column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
            default_value: None,
            origin_default: None,
            comment: String::new(),
            generated: None,
        }
    }

    /// Publishes everything the buffer stages into the snapshot and empties
    /// it: what COMMIT does to the two halves of a cluster read.
    fn commit(buffer: &MutationBuffer, snapshot: &Arc<Mutex<MockSnapshot>>) {
        let mut snapshot = snapshot.lock().unwrap();
        for (key, value) in buffer.snapshot() {
            match value {
                Some(value) => snapshot.data.insert(key.as_bytes().to_vec(), value),
                None => snapshot.data.remove(key.as_bytes()),
            };
        }
        buffer.reset();
    }

    /// Go `PushDownExprs` consults `mysql.expr_pushdown_blacklist` for every
    /// Selection offered to a scan. A later offer's conditions joined to an
    /// earlier one keep that verdict: a blacklisted function stays a local
    /// condition and never reaches the coprocessor request.
    #[test]
    fn a_later_selection_offer_keeps_the_pushdown_blacklist() {
        use tidb_expr::expression::Expression;
        let gt = |column_id: i64, index: i64, value: i64| {
            let mut column = tidb_expr::column::Column::new(column_id, long());
            column.index = index;
            Expression::ScalarFunction(tidb_expr::expression::ScalarFunction::new(
                tidb_ast::CiString::new("gt"),
                long(),
                vec![
                    Expression::Column(column),
                    Expression::Constant(tidb_expr::constant::Constant::new(
                        Datum::Int(value),
                        long(),
                    )),
                ],
            ))
        };
        let mut blacklist = tidb_expr::infer_pushdown::ExprPushDownBlacklist::new();
        blacklist.insert(
            "gt".to_owned(),
            tidb_expr::infer_pushdown::blacklist_store_mask("tikv"),
        );
        let blocked = crate::StmtContext::for_query()
            .with_pushdown_blacklists((Arc::new(blacklist), Arc::default()));
        let open = crate::StmtContext::for_query();
        let mut first = super::PushedScanFilter::from_physical_conditions(vec![gt(1, 0, 5)], &open);
        assert!(first.fully_pushable());
        let later = super::PushedScanFilter::from_physical_conditions(vec![gt(2, 1, 6)], &blocked);
        assert!(!later.fully_pushable(), "the blacklist refuses `gt`");
        first.conjoin(&later);
        assert_eq!(first.filters().len(), 2, "both conditions still filter");
        assert_eq!(
            first.conditions().len(),
            2,
            "the request builder is offered every condition, never a subset"
        );
        assert!(
            !first.fully_pushable(),
            "one refused condition keeps the whole Selection local"
        );
    }

    /// A pushed predicate must filter the transaction's own staged rows too.
    ///
    /// Over `ClusterTableStorage` a scan produces the snapshot merged with the
    /// session's staged mutation buffer. Pushing a conjunct into the scan
    /// removes it from the `Selection` above, so the scan becomes the only
    /// place it is applied: a staged row that fails it must not be returned,
    /// and a staged row that satisfies it must not be lost -- including when
    /// a staged UPDATE moves a row across the predicate's boundary and when a
    /// staged DELETE removes a row the committed half still holds.
    #[test]
    fn a_pushed_predicate_filters_staged_rows_as_well_as_committed_ones() {
        let snapshot = Arc::new(Mutex::new(MockSnapshot::default()));
        let handle: Arc<Mutex<dyn ClusterSnapshot>> = Arc::clone(&snapshot) as _;
        let buffer = MutationBuffer::new();
        let mut table = KvTable::with_storage(
            91,
            vec![column("a", 1), column("b", 2)],
            Box::new(ClusterTableStorage::new(buffer.clone(), handle)),
        );

        // Committed half: one row above the predicate, one below.
        let committed_low = table
            .insert_row(&[Datum::Int(1), Datum::Int(10)], &tidb_expr::NoColumns)
            .unwrap();
        table
            .insert_row(&[Datum::Int(9), Datum::Int(90)], &tidb_expr::NoColumns)
            .unwrap();
        let committed_moved = table
            .insert_row(&[Datum::Int(2), Datum::Int(20)], &tidb_expr::NoColumns)
            .unwrap();
        commit(&buffer, &snapshot);
        assert!(buffer.is_empty(), "nothing is staged after the commit");

        // Staged half, all inside one open transaction:
        //   * an INSERT that satisfies `a > 5`,
        //   * an INSERT that does not,
        //   * an UPDATE that lifts a committed row across the boundary,
        //   * a DELETE of a committed row that satisfies it.
        table
            .insert_row(&[Datum::Int(7), Datum::Int(70)], &tidb_expr::NoColumns)
            .unwrap();
        table
            .insert_row(&[Datum::Int(3), Datum::Int(30)], &tidb_expr::NoColumns)
            .unwrap();
        table
            .update_row_with_context(
                &committed_moved,
                &[Datum::Int(8), Datum::Int(80)],
                &crate::StmtContext::for_dml(false, false, false),
            )
            .unwrap();
        table
            .delete_row_with_context(
                &committed_low,
                &crate::StmtContext::for_dml(false, false, false),
            )
            .unwrap();
        assert!(!buffer.is_empty(), "the writes are staged, not committed");

        let mut catalog = Catalog::mock();
        catalog.register_kv("t", table);
        let ctx = crate::StmtContext::for_query_in_dirty_transaction(&[91]);
        assert_eq!(
            run_select_on("SELECT a, b FROM t WHERE a > 5 ORDER BY a", &catalog, &ctx).unwrap(),
            vec![
                vec![Datum::Int(7), Datum::Int(70)],
                vec![Datum::Int(8), Datum::Int(80)],
                vec![Datum::Int(9), Datum::Int(90)],
            ],
            "staged inserts and updates are filtered by the pushed predicate, \
             not waved through it"
        );
        // The residual half of a split predicate still runs above the scan.
        assert_eq!(
            run_select_on(
                "SELECT a, b FROM t WHERE a > 5 AND b + 1 < 80 ORDER BY a",
                &catalog,
                &ctx
            )
            .unwrap(),
            vec![vec![Datum::Int(7), Datum::Int(70)]]
        );
        // A staged row the predicate excludes is not reachable by any spelling.
        assert_eq!(
            run_select_on("SELECT a FROM t WHERE a = 3", &catalog, &ctx).unwrap(),
            vec![vec![Datum::Int(3)]],
            "and it is still there when the predicate selects it"
        );
        // The composed descriptions carry the same obligation, because they are
        // removed from the `Selection` in exactly the same way: a staged row
        // must be tested by the pushed `IN` and the pushed `OR` too.
        assert_eq!(
            run_select_on(
                "SELECT a FROM t WHERE a IN (3, 7, 8) ORDER BY a",
                &catalog,
                &ctx
            )
            .unwrap(),
            vec![
                vec![Datum::Int(3)],
                vec![Datum::Int(7)],
                vec![Datum::Int(8)],
            ],
            "the staged insert, the staged update's new value, and nothing else"
        );
        assert_eq!(
            run_select_on(
                "SELECT a FROM t WHERE a = 1 OR a = 8 ORDER BY a",
                &catalog,
                &ctx
            )
            .unwrap(),
            vec![vec![Datum::Int(8)]],
            "the staged DELETE removed the row `a = 1` matched, and the staged \
             UPDATE created the row `a = 8` matches"
        );
        // A pushed BUILTIN carries the same obligation, and widening what may
        // be pushed widens what the obligation covers -- so it is re-proved
        // here rather than assumed. `MOD(a, 4)` is 0 (false) for `a = 8`, the
        // value a staged UPDATE created, and truthy for the staged INSERTs; a
        // source that filtered the snapshot half only would return `a = 8`.
        assert_eq!(
            run_select_on("SELECT a FROM t WHERE mod(a, 4) ORDER BY a", &catalog, &ctx).unwrap(),
            vec![
                vec![Datum::Int(3)],
                vec![Datum::Int(7)],
                vec![Datum::Int(9)],
            ],
            "the staged UPDATE's new value `a = 8` fails the pushed builtin and \
             must not be returned, while the staged inserts that pass it are"
        );
    }

    /// The whole predicate must survive when nothing is pushed, and the split
    /// must not change any answer a `Selection` alone produced.
    #[test]
    fn splitting_a_where_does_not_change_its_result() {
        let mut table = KvTable::new(92, vec![column("a", 1), column("b", 2)]);
        for (a, b) in [(1, 10), (5, 50), (7, 70), (9, 90)] {
            table
                .insert_row(&[Datum::Int(a), Datum::Int(b)], &tidb_expr::NoColumns)
                .unwrap();
        }
        let mut catalog = Catalog::mock();
        catalog.register_kv("t", table);
        let ctx = crate::StmtContext::for_query();
        let cases: [(&str, Vec<Vec<Datum>>); 5] = [
            (
                "SELECT a FROM t WHERE a > 5",
                vec![vec![Datum::Int(7)], vec![Datum::Int(9)]],
            ),
            (
                "SELECT a FROM t WHERE 5 < a",
                vec![vec![Datum::Int(7)], vec![Datum::Int(9)]],
            ),
            (
                "SELECT a FROM t WHERE a > 5 AND b < 80",
                vec![vec![Datum::Int(7)]],
            ),
            // A disjunction now pushes as one conjunct, and must answer the
            // same rows it did as a residual `Selection`.
            (
                "SELECT a FROM t WHERE a = 1 OR a = 9",
                vec![vec![Datum::Int(1)], vec![Datum::Int(9)]],
            ),
            // Mixed: `a > 1` pushes, the arithmetic stays above.
            (
                "SELECT a FROM t WHERE a > 1 AND b + 1 > 60",
                vec![vec![Datum::Int(7)], vec![Datum::Int(9)]],
            ),
        ];
        for (sql, expected) in cases {
            assert_eq!(
                run_select_on(sql, &catalog, &ctx).unwrap(),
                expected,
                "{sql}"
            );
        }
    }

    /// The composed predicates, over a table with NULLs, against the answer
    /// MySQL's three-valued logic gives.
    ///
    /// This is the pushed form and the local form agreeing: each `WHERE` below
    /// moves into the scan as one described conjunct, so the scan is the only
    /// place it is applied, and the expected rows are the ones the same
    /// predicate produces as a `Selection` -- including the cases where
    /// UNKNOWN, not FALSE, is the reason a row is absent.
    #[test]
    fn the_composed_predicates_keep_mysqls_three_valued_answers() {
        let mut table = KvTable::new(93, vec![column("a", 1), column("b", 2)]);
        for (a, b) in [
            (Datum::Int(1), Datum::Int(10)),
            (Datum::Int(2), Datum::Null),
            (Datum::Null, Datum::Int(30)),
            (Datum::Int(4), Datum::Int(40)),
        ] {
            table.insert_row(&[a, b], &tidb_expr::NoColumns).unwrap();
        }
        let mut catalog = Catalog::mock();
        catalog.register_kv("t", table);
        let ctx = crate::StmtContext::for_query();
        let int = |value: i64| vec![Datum::Int(value)];
        let cases: [(&str, Vec<Vec<Datum>>); 10] = [
            ("SELECT a FROM t WHERE a IS NULL", vec![vec![Datum::Null]]),
            (
                "SELECT a FROM t WHERE a IS NOT NULL",
                vec![int(1), int(2), int(4)],
            ),
            (
                "SELECT a FROM t WHERE b IS NULL",
                // The row whose `b` is NULL, whatever its `a` is.
                vec![int(2)],
            ),
            ("SELECT a FROM t WHERE a IN (1, 4)", vec![int(1), int(4)]),
            // `NOT IN` is UNKNOWN for the NULL row, so it is absent -- and it
            // would be present if `NOT IN` were pushed as a plain negation of
            // membership over a NULL-blind test.
            ("SELECT a FROM t WHERE a NOT IN (1, 4)", vec![int(2)]),
            ("SELECT a FROM t WHERE a = 1 OR a = 4", vec![int(1), int(4)]),
            // `OR` over a branch that is UNKNOWN for the NULL row: TRUE wins.
            (
                "SELECT a FROM t WHERE a = 1 OR a IS NULL",
                vec![int(1), vec![Datum::Null]],
            ),
            ("SELECT a FROM t WHERE NOT a = 1", vec![int(2), int(4)]),
            (
                "SELECT a FROM t WHERE a > -1 AND a < 3",
                vec![int(1), int(2)],
            ),
            // A pushed disjunction beside a residual conjunct: both apply.
            (
                "SELECT a FROM t WHERE (a = 1 OR a = 4) AND b + 1 > 20",
                vec![int(4)],
            ),
        ];
        for (sql, expected) in cases {
            assert_eq!(
                run_select_on(sql, &catalog, &ctx).unwrap(),
                expected,
                "{sql}"
            );
        }
    }

    /// A pushed builtin call answers the rows the same call answers as a
    /// `Selection`, including where the answer is NULL rather than false.
    ///
    /// This is the local half of the newly pushed math family: the conjunct is
    /// removed from the `Selection` above the scan, so the scan is the only
    /// place it is applied, and every expectation below is the row set MySQL's
    /// three-valued logic gives for the same predicate.
    #[test]
    fn a_pushed_math_builtin_answers_what_the_same_selection_answered() {
        let mut table = KvTable::new(94, vec![column("a", 1), column("b", 2)]);
        for (a, b) in [
            (Datum::Int(0), Datum::Int(3)),
            (Datum::Int(1), Datum::Int(0)),
            (Datum::Int(2), Datum::Int(7)),
            (Datum::Int(7), Datum::Int(7)),
            (Datum::Null, Datum::Int(1)),
        ] {
            table.insert_row(&[a, b], &tidb_expr::NoColumns).unwrap();
        }
        let mut catalog = Catalog::mock();
        catalog.register_kv("t", table);
        let ctx = crate::StmtContext::for_query();
        let int = |value: i64| vec![Datum::Int(value)];
        let cases: [(&str, Vec<Vec<Datum>>); 8] = [
            // `SIN(0)` is 0, which is false; the NULL row is UNKNOWN.
            ("SELECT a FROM t WHERE sin(a)", vec![int(1), int(2), int(7)]),
            // `MOD(a, 3)` is 0 for a = 0 and a = 3, and NULL for the NULL row.
            (
                "SELECT a FROM t WHERE mod(a, 3)",
                vec![int(1), int(2), int(7)],
            ),
            // A zero divisor is NULL in MySQL, so no row qualifies -- and it
            // must not be an error either, on the scan or above it.
            ("SELECT a FROM t WHERE mod(a, 0)", vec![]),
            // `ACOS` of anything outside [-1, 1] is NULL, not an error, and
            // `ACOS(1)` is exactly 0, which is false -- so only `a = 0`
            // (`ACOS(0)` = pi/2) qualifies.
            ("SELECT a FROM t WHERE acos(a)", vec![int(0)]),
            // `ROUND` over an integer column keeps the integer domain.
            (
                "SELECT a FROM t WHERE round(a)",
                vec![int(1), int(2), int(7)],
            ),
            // `PI()` is a nonzero constant, so it selects every row -- the NULL
            // one included, since the predicate does not read the column.
            (
                "SELECT a FROM t WHERE pi()",
                vec![int(0), int(1), int(2), int(7), vec![Datum::Null]],
            ),
            // `POW(a, 2)` is 0 only for a = 0.
            (
                "SELECT a FROM t WHERE pow(a, 2)",
                vec![int(1), int(2), int(7)],
            ),
            // A pushed builtin beside a pushed comparison and a residual
            // conjunct: all three still apply.
            (
                "SELECT a FROM t WHERE sin(a) AND a > 1 AND b + 1 > 7",
                vec![int(2), int(7)],
            ),
        ];
        for (sql, expected) in cases {
            assert_eq!(
                run_select_on(sql, &catalog, &ctx).unwrap(),
                expected,
                "{sql}"
            );
        }
    }

    /// A call the catalog does not hold stays above the scan, and so does one
    /// whose argument is not describable -- neither may quietly become a
    /// pushed conjunct, because the store would then evaluate what only this
    /// engine evaluates.
    #[test]
    fn a_call_outside_the_catalog_stays_above_the_scan() {
        let mut table = KvTable::new(95, vec![column("a", 1), column("b", 2)]);
        for a in [1_i64, 2, 3] {
            table
                .insert_row(&[Datum::Int(a), Datum::Int(a * 10)], &tidb_expr::NoColumns)
                .unwrap();
        }
        let mut catalog = Catalog::mock();
        catalog.register_kv("t", table);
        let ctx = crate::StmtContext::for_query();
        // `TAN` is deliberately absent from Go's TiKV whitelist (the comment
        // cites Rust's LLVM math precision), `ABS` is on it but has no catalog
        // row yet, and `SIN(a + 1)` has an argument the description cannot
        // carry. All three must still answer correctly.
        for (sql, expected) in [
            ("SELECT a FROM t WHERE tan(a)", vec![1_i64, 2, 3]),
            ("SELECT a FROM t WHERE abs(a)", vec![1, 2, 3]),
            ("SELECT a FROM t WHERE sin(a + 1)", vec![1, 2, 3]),
            ("SELECT a FROM t WHERE mod(a, b)", vec![1, 2, 3]),
        ] {
            assert_eq!(
                run_select_on(sql, &catalog, &ctx).unwrap(),
                expected
                    .into_iter()
                    .map(|a| vec![Datum::Int(a)])
                    .collect::<Vec<_>>(),
                "{sql}"
            );
        }
    }

    /// The planner splits a Selection per conjunct (Go `PushDownExprs`); the
    /// request builder then converts the planner's pushed list whole. The two
    /// must agree: a condition the planner admits for TiKV is one `ExprToPB`
    /// converts, or the executor would silently keep a Selection the plan says
    /// the coprocessor evaluates.
    #[test]
    fn the_converter_expresses_every_condition_the_planner_admits() {
        use tidb_datatype::{FieldType, FieldTypeCode};
        let int = FieldType::new(FieldTypeCode::LongLong);
        let text = FieldType::new(FieldTypeCode::Varchar).with_flen(10);
        let columns = vec![
            ("a".to_owned(), int.clone()),
            ("b".to_owned(), int.clone()),
            ("c".to_owned(), int.clone()),
            ("s".to_owned(), text),
            (
                "u".to_owned(),
                FieldType::new(FieldTypeCode::LongLong)
                    .with_flags(tidb_datatype::FieldTypeFlags::UNSIGNED),
            ),
            (
                "d".to_owned(),
                FieldType::new(FieldTypeCode::Datetime).with_decimal(0),
            ),
        ];
        let context = crate::StmtContext::for_query();
        let resolver = crate::driver::TableResolver {
            database: Some("test"),
            table_name: "t",
            columns: &columns,
            constant_context: context.clone(),
            zone: context.session_zone(),
            no_unsigned_subtraction: false,
            div_precision_increment: 4,
            clause_message: "expression",
            subquery_constants: &[],
        };
        const PREDICATES: &[&str] = &[
            "a > 5",
            "5 < a",
            "a = b",
            "a <> c",
            "a IS NULL",
            "a IS NOT NULL",
            "a IN (1, 2, 3)",
            "a NOT IN (1, 2)",
            "s = 'x'",
            "s IN ('x', 'y')",
            "s LIKE 'x%'",
            "s NOT LIKE '%y'",
            "a BETWEEN 1 AND 9",
            "a > 1 AND b < 3",
            "a = 1 OR b = 2",
            "NOT (a > 1)",
            "a + b > 3",
            "b * 2 = c",
            "a - 1 = 0",
            "ABS(a - b) = 1",
            "COALESCE(b, 0) > 0",
            "IF(b > 0, a, c) > 2",
            "CASE WHEN b > 0 THEN a > 1 ELSE FALSE END",
            "a IN (b, 1)",
            "b IS TRUE",
            "b IS NOT TRUE",
            "u > 2",
            "u >= 9223372036854775808",
            "d > '2024-01-01'",
            "a = '5'",
            "a > 1.5",
            "s > 'u' AND a > 1",
            "UPPER(s) = 'X'",
            "LENGTH(s) > 2",
            "SUBSTRING(s, 1, 2) = 'ab'",
            "a % 3 = 0",
            "a DIV 2 = 1",
            "IFNULL(a, 0) = 0",
            "LEAST(a, b) > 1",
            "a IN (1, 2) AND s LIKE 'a%'",
        ];
        let mut refused = Vec::new();
        for sql in PREDICATES {
            let select = tidb_parser::parse(&format!("SELECT {sql}")).expect("parses");
            let tidb_ast::Stmt::Query(query) = select else {
                panic!("not a query");
            };
            let tidb_ast::QueryStmt::Select(select) = &*query else {
                panic!("not a select");
            };
            let Some(tidb_ast::SelectField::Expr { expr, .. }) = select.fields.fields().first()
            else {
                panic!("no field");
            };
            let Ok(mut condition) = tidb_expr::rewriter::rewrite_expr_resolved(expr, &resolver)
            else {
                continue;
            };
            tidb_expr::scalar_function::into_coprocessor_form(&mut condition);
            if !tidb_planner::pushdown::can_exprs_push_down_tikv(std::slice::from_ref(&condition)) {
                continue;
            }
            if super::describe_request_condition(&condition, Some(&context)).is_err() {
                refused.push(*sql);
            }
        }
        // The planner's admission is Go's name whitelist; the catalog holds
        // the signatures converted so far. These are the admitted shapes it
        // does not hold yet: their Selection is evaluated locally, whole (never
        // a subset), while EXPLAIN keeps Go's cop Selection. The list may only
        // shrink -- a new entry is a planner/converter disagreement.
        const NOT_CONVERTED_YET: &[&str] = &[];
        assert_eq!(
            refused, NOT_CONVERTED_YET,
            "the planner admits for TiKV what ExprToPB refuses"
        );
    }
}
