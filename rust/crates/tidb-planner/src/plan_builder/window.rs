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

//! The WINDOW stage of `buildSelect`, and the last clause of the SELECT spine.
//!
//! Go `pkg/planner/core/logical_plan_builder.go`, located by reading
//! `buildSelect` (`:4254`) at `:4392` and `:4541-4571` and following every
//! call it makes:
//!
//! | Go symbol | line | here |
//! | --- | --- | --- |
//! | `buildWindowSpecs` | `:7426` | [`PlanBuilder::build_window_specs`] |
//! | `resolveWindowSpec` | `:7392` | [`resolve_window_spec`] |
//! | `mergeWindowSpec` | `:7410` | [`merge_window_spec`] |
//! | `specEqual` | `:7297` | [`spec_equal`] |
//! | `appendIfAbsentWindowSpec` | `:7290` | [`append_if_absent_window_spec`] |
//! | `handleDefaultFrame` | `:7239` | [`PlanBuilder::handle_default_frame`] |
//! | `groupWindowFuncs` | `:7315` | [`PlanBuilder::group_window_funcs`] |
//! | `extractWindowFuncs` | `:7230` | [`extract_window_funcs`] |
//! | `getAllByItems` | `:7006` | [`all_by_items`] |
//! | `restoreByItemText` | `:7017` | [`by_item_text`] |
//! | `compareItems` | `:7027` | [`compare_items`] |
//! | `sortWindowSpecs` | `:7049` | [`sort_window_specs`] |
//! | `buildWindowFunctions` | `:7064` | [`PlanBuilder::build_window_functions`] |
//! | `buildProjectionForWindow` | `:6728` | [`PlanBuilder::build_projection_for_window`] |
//! | `buildArgs4WindowFunc` | `:6798` | [`PlanBuilder::build_args_for_window_func`] |
//! | `buildByItemsForWindow` | `:6826` | [`PlanBuilder::build_by_items_for_window`] |
//! | `checkWindowFuncArgs` | `:6981` | [`PlanBuilder::check_window_func_args`] |
//! | `checkOriginWindowFuncs` | `:7141` | [`PlanBuilder::check_origin_window_funcs`] |
//! | `checkOriginWindowSpec` | `:7164` | [`PlanBuilder::check_origin_window_spec`] |
//! | `checkOriginWindowFrameBound` | `:7196` | [`PlanBuilder::check_origin_window_frame_bound`] |
//! | `buildWindowFunctionFrame` | `:6966` | [`PlanBuilder::build_window_function_frame`] |
//! | `buildWindowFunctionFrameBound` | `:6873` | [`PlanBuilder::build_window_function_frame_bound`] |
//! | `getWindowName` | `:6716` | [`window_name`] |
//! | `detectSelectWindow` | `planbuilder.go` | [`detect_select_window`] |
//!
//! # 1. Pointer identity, without pointers
//!
//! `groupWindowFuncs` returns `map[*ast.WindowSpec][]*ast.WindowFuncExpr`
//! plus an ordered `[]*ast.WindowSpec`. The KEY is a spec's ADDRESS, and that
//! is load-bearing in both directions:
//!
//! * two window functions each writing an INLINE `OVER (PARTITION BY a)` take
//!   `spec := &windowFunc.Spec` — two different addresses, so two groups and
//!   two `LogicalWindow` operators, even though the specs are textually equal;
//! * two window functions naming the SAME `WINDOW w AS (...)` share the one
//!   `b.windowSpecs[w]` pointer, so they group together — and when
//!   `handleDefaultFrame` rewrites the frame for one of them, `updatedSpecMap`
//!   re-shares the rewritten spec through `specEqual` so they group together
//!   again.
//!
//! [`SpecArena`] reproduces both: every spec that enters the grouping is
//! pushed into one `Vec` and thereafter named by its [`SpecId`] index. An
//! index is as stable as an address and, unlike a structural key, keeps two
//! equal-looking specs apart — which a `HashMap<WindowDef, _>` would not.
//!
//! # 2. The window marker
//!
//! Go's `windowMapper map[*ast.WindowFuncExpr]int` is
//! [`MarkerKind::Window`], per [`super::marker`]'s spec:
//! [`extract_window_funcs`] substitutes `#win#k` for the k-th window call in
//! the select list, and [`PlanBuilder::window_marker_columns`] binds that kind
//! to the built `LogicalWindow`'s own output columns. Go's second
//! `buildProjection` pass (`considerWindow == true`, `:1791`) is then the
//! ordinary projection over the substituted fields, so this module adds no
//! second projection entry point.
//!
//! # 3. Boundaries, by exact Go symbol
//!
//! Each is a dependency that is genuinely absent, not a body skipped.
//!
//! * `resolveWindowFunction` (`:3048`). Its body drives
//!   `havingWindowAndOrderbyExprResolver` (`:2723`) in `inWindowSpec` /
//!   `inWindowFunc` mode and then calls `appendAuxiliaryFieldsForSubqueries`
//!   (`:3101`). What it PRODUCES for this stage is `windowAggMap` — the
//!   aggregates written INSIDE a window function's arguments, `PARTITION BY`
//!   or `ORDER BY`. Neither the auxiliary-subquery-field machinery nor the
//!   two-operator ordering it implies (aggregation BELOW the window) is
//!   ported, so [`PlanBuilder::build_window_stage`] REFUSES a window whose
//!   arguments or by-items contain an aggregate, naming the symbol. A window
//!   over plain columns and scalar expressions is unaffected. Rule 7: binding
//!   such an aggregate to the wrong operator is a silent wrong answer.
//! * `evalAstExprWithPlanCtx` (`:6900`), the constant folder an EXPLICIT
//!   `RANGE n PRECEDING` / `n FOLLOWING` bound needs. Reproduced only for a
//!   bound the expression rewriter already yields as an
//!   [`Expression::Constant`]; anything else is refused rather than framed
//!   wrongly.
//! * `expression.GetCmpFunction` / `expression.GetAccurateCmpType`
//!   (`builtin_compare.go:1489` / `:1420`). Not transcreated. Every call this
//!   stage makes compares two expressions of the SAME `FieldType` — `(col,
//!   col)` for a `RANGE CURRENT ROW` bound, and `(col, calcFunc)` for an
//!   explicit one, where `NewFunctionBase` was handed `col.RetType`. For that
//!   case `GetAccurateCmpType` reduces to a pure function of the one field
//!   type, which is [`cmp_type_for_same_field_type`]; it is not a general
//!   stand-in and takes ONE type for that reason.
//! * `ast.Groups` (`checkOriginWindowSpec`'s `ErrNotSupportedYet("GROUPS")`).
//!   [`tidb_ast::FrameKind`] has only `Rows` and `Range`, so a `GROUPS` frame
//!   is unrepresentable and the check is vacuous rather than omitted.
//! * `expression.ParamMarkerInPrepareChecker`. There is no prepared-statement
//!   surface in this crate, so `NewWindowFuncDesc`'s `skipCheckArgs` is always
//!   `false` — the STRICTER arm, which validates the constant arguments.
//! * `itemTransformer` (`:2380`) as `buildByItemsForWindow` applies it: a bare
//!   integer in a window `PARTITION BY` / `ORDER BY` becomes an
//!   `ast.PositionExpr`, whose resolution against the window's child schema is
//!   not ported. [`PlanBuilder::build_by_items_for_window`] refuses one.
//!
//! # 4. Session variables
//!
//! `handleDefaultFrame` reads `EnablePipelinedWindowExec`;
//! [`PlanBuilder::enable_pipelined_window_exec`] carries it with Go's own
//! default (`ON`).

use std::collections::{BTreeMap, BTreeSet};

use tidb_ast::{
    Expr, FrameBound as AstFrameBound, FrameKind, OrderItem, SelectStmt, WindowDef,
    WindowFrame as AstWindowFrame, WindowOver, WindowSpec,
};
use tidb_datatype::{EvalType, FieldName, FieldType, FieldTypeCode};
use tidb_expr::aggregation::{need_frame, use_default_frame, WindowFuncDesc};
use tidb_expr::column::Column;
use tidb_expr::expr_util::get_uint64_from_constant;
use tidb_expr::expression::Expression;
use tidb_expr::new_function::new_function_base;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;

use super::aggregation::{has_window_flag, is_aggregate_call, visit_exprs, walk_exprs};
use super::catalog::TableSource;
use super::marker::{self, MarkerKind, PlanMarker};
use super::{snapshot_schema_and_names, ClauseCode, PlanBuilder, PlanError, ProjectionField};
use crate::logical::projection::LogicalProjection;
use crate::logical::rule::flags;
use crate::logical::window::{
    BoundType, FrameBound, FrameType, LogicalWindow, WindowFrame, WindowSortItem,
};
use crate::logical::LogicalPlan;

// ***** the spec arena: Go's `*ast.WindowSpec` identity *****

/// Go `groupWindowFuncs`' `(groupedWindow, orderedSpec)` pair, flattened into
/// `orderedSpec` order. The `Vec<usize>` indexes the window-function list, and
/// is EMPTY for an unused named spec.
pub type WindowGroups = Vec<(SpecId, Vec<usize>)>;

/// Go `buildProjectionForWindow`'s four results: the projection, the
/// `PARTITION BY` items, the `ORDER BY` items and the rewritten arguments.
pub type WindowProjection = (
    LogicalPlan,
    Vec<WindowSortItem>,
    Vec<WindowSortItem>,
    Vec<Expression>,
);

/// One spec's place in [`SpecArena`]. This is Go's `*ast.WindowSpec`, and the
/// ONLY thing this module uses to tell two specs apart; see section 1.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SpecId(pub usize);

/// A window specification together with the NAME its errors are reported
/// under — Go's `ast.WindowSpec.Name.O`, which `getWindowName` reads.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct NamedWindowSpec {
    /// Go `WindowSpec.Name.O`; empty for an inline `OVER (...)`.
    pub name: String,
    /// Go's `PartitionBy` / `OrderBy` / `Frame`, plus `Ref` as
    /// [`WindowDef::base`].
    pub def: WindowDef,
}

impl NamedWindowSpec {
    /// A named spec, as one `WINDOW name AS (...)` entry declares it.
    #[must_use]
    pub const fn new(name: String, def: WindowDef) -> Self {
        Self { name, def }
    }

    /// An anonymous spec, as an inline `OVER (...)` writes it.
    #[must_use]
    pub fn anonymous(def: WindowDef) -> Self {
        Self {
            name: String::new(),
            def,
        }
    }
}

/// The owner of every `ast.WindowSpec` value the grouping sees, standing in
/// for Go's heap. See section 1 for why an index and not a structural key.
#[derive(Clone, Debug, Default)]
pub struct SpecArena {
    specs: Vec<NamedWindowSpec>,
}

impl SpecArena {
    /// An empty arena.
    #[must_use]
    pub const fn new() -> Self {
        Self { specs: Vec::new() }
    }

    /// Interns `spec` under a FRESH identity — Go's `&ast.WindowSpec{...}`.
    pub fn intern(&mut self, spec: NamedWindowSpec) -> SpecId {
        self.specs.push(spec);
        SpecId(self.specs.len() - 1)
    }

    /// The spec `id` names. Panics only on an id from another arena, which
    /// this module never constructs.
    #[must_use]
    pub fn get(&self, id: SpecId) -> &NamedWindowSpec {
        &self.specs[id.0]
    }

    /// How many specs have been interned.
    #[must_use]
    pub fn len(&self) -> usize {
        self.specs.len()
    }

    /// Whether nothing has been interned.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.specs.is_empty()
    }
}

/// Go `appendIfAbsentWindowSpec(specs, ns)` (`:7290`), whose
/// `slices.Contains` is over POINTERS.
fn append_if_absent_window_spec(specs: &mut Vec<SpecId>, id: SpecId) {
    if !specs.contains(&id) {
        specs.push(id);
    }
}

/// Go `specEqual(s1, s2)` (`:7297`), which compares the two specs' RESTORED
/// SQL text.
///
/// `// boundary:` `ast.WindowSpec.Restore`. [`tidb_ast`] does not export a
/// per-expression restore, so equality is taken on the AST VALUE. That is at
/// least as fine-grained as Go's: two specs whose restore text agrees are
/// structurally equal here, because the restore is injective over the fields
/// [`WindowDef`] carries. Go's `(nil, non-nil) -> false` arm is the
/// `Option` mismatch.
#[must_use]
pub fn spec_equal(left: Option<&WindowDef>, right: Option<&WindowDef>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => left == right,
        _ => false,
    }
}

// ***** named window specs: buildWindowSpecs and its resolution *****

/// Go `mergeWindowSpec(spec, ref)` (`:7410`): folds the referenced window's
/// `PARTITION BY` / `ORDER BY` into `spec` and clears the reference.
///
/// # Errors
///
/// `ErrWindowNoInherentFrame`, `ErrWindowNoChildPartitioning` and
/// `ErrWindowNoRedefineOrderBy`, in Go's order.
pub fn merge_window_spec(
    spec: &mut WindowDef,
    spec_name: &str,
    reference: &NamedWindowSpec,
) -> Result<(), PlanError> {
    if reference.def.spec.frame.is_some() {
        return Err(PlanError::internal(format!(
            "Window '{}' has a frame definition, so cannot be referenced by another window",
            reference.name
        )));
    }
    if !spec.spec.partition_by.is_empty() {
        return Err(PlanError::internal(
            "You cannot use the window's own partitioning clause when the window is referenced",
        ));
    }
    if !reference.def.spec.order_by.is_empty() {
        if !spec.spec.order_by.is_empty() {
            return Err(PlanError::internal(format!(
                "Window '{}' cannot inherit '{}' since both contain an ORDER BY clause",
                window_name(spec_name),
                reference.name
            )));
        }
        spec.spec.order_by = reference.def.spec.order_by.clone();
    }
    // Go assigns `spec.PartitionBy = ref.PartitionBy` UNCONDITIONALLY — the
    // guard above has already established that `spec`'s own is nil, so this is
    // an inheritance and not an overwrite.
    spec.spec.partition_by = reference.def.spec.partition_by.clone();
    spec.base = None;
    Ok(())
}

/// Go `resolveWindowSpec(spec, specs, inStack)` (`:7392`): resolves one named
/// window's `Ref` chain, depth first, refusing a cycle.
///
/// Recursion here is over the WINDOW clause's reference graph, which the
/// `in_stack` guard bounds by the number of named windows in one statement —
/// not over a plan tree, so [`crate::logical::fold`] does not apply.
///
/// # Errors
///
/// `ErrWindowCircularityInWindowGraph`, `ErrWindowNoSuchWindow`, or any error
/// [`merge_window_spec`] raises.
fn resolve_window_spec(
    name_lower: &str,
    specs: &mut BTreeMap<String, NamedWindowSpec>,
    in_stack: &mut BTreeSet<String>,
) -> Result<(), PlanError> {
    if in_stack.contains(name_lower) {
        return Err(PlanError::internal(
            "There is a circularity in the window dependency graph",
        ));
    }
    let Some(entry) = specs.get(name_lower) else {
        return Ok(());
    };
    let Some(reference) = entry.def.base.clone() else {
        return Ok(());
    };
    let reference_lower = reference.to_ascii_lowercase();
    if !specs.contains_key(&reference_lower) {
        return Err(PlanError::internal(format!(
            "Window name '{reference}' is not defined"
        )));
    }
    in_stack.insert(name_lower.to_owned());
    let resolved = resolve_window_spec(&reference_lower, specs, in_stack);
    in_stack.remove(name_lower);
    resolved?;

    // Both entries live in the same map, so the referenced one is cloned out
    // before the referring one is borrowed mutably. Go aliases them instead;
    // nothing observes the difference, because `mergeWindowSpec` only READS
    // the reference.
    let reference_spec = specs[&reference_lower].clone();
    let mut entry = specs[name_lower].clone();
    merge_window_spec(&mut entry.def, &entry.name, &reference_spec)?;
    specs.insert(name_lower.to_owned(), entry);
    Ok(())
}

/// Go `getWindowName(name)` (`:6716`).
#[must_use]
pub fn window_name(name: &str) -> &str {
    if name.is_empty() {
        "<unnamed window>"
    } else {
        name
    }
}

/// Go `detectSelectWindow(sel)` (`planbuilder.go`): a window call anywhere in
/// the select list or the ORDER BY.
#[must_use]
pub fn detect_select_window(select: &SelectStmt) -> bool {
    let fields = select.fields.fields().iter().any(|field| match field {
        tidb_ast::SelectField::Expr { expr, .. } => has_window_flag(expr),
        tidb_ast::SelectField::Wildcard(_) => false,
    });
    fields
        || select
            .order_by
            .iter()
            .any(|item| has_window_flag(&item.expr))
}

// ***** the window function calls extracted from the select list *****

/// Go `*ast.WindowFuncExpr` (`ast/functions.go`), as this stage reads it.
#[derive(Clone, Debug, PartialEq)]
pub struct WindowFuncCall {
    /// Go `Name`.
    pub name: String,
    /// Go `Args`.
    pub args: Vec<Expr>,
    /// Go `Distinct`.
    pub distinct: bool,
    /// Go `IgnoreNull`.
    pub ignore_null: bool,
    /// Go `FromLast`.
    pub from_last: bool,
    /// Go `Spec`: `OVER w` is [`WindowOver::Name`] (Go's `Spec.Name`), and
    /// `OVER (w ...)` is [`WindowOver::Def`] with a base (Go's `Spec.Ref`).
    pub over: WindowOver,
}

/// Go `extractWindowFuncs(fields)` (`:7230`) over `WindowFuncExtractor`,
/// with `windowMapper`'s key substituted in as a [`MarkerKind::Window`]
/// marker; see section 2.
///
/// Go's extractor does NOT descend into a window call's own arguments (its
/// `Enter` returns `true` for `*ast.WindowFuncExpr`), so a window function
/// nested inside another's arguments is never separately collected. The
/// `true` returned below is that.
#[must_use]
pub fn extract_window_funcs(fields: &mut [ProjectionField]) -> Vec<WindowFuncCall> {
    let mut found = Vec::new();
    for field in fields.iter_mut() {
        visit_exprs(&mut field.expr, &mut |node| {
            let Expr::Window {
                name,
                args,
                distinct,
                ignore_nulls,
                from_last,
                over,
            } = node
            else {
                return false;
            };
            let call = WindowFuncCall {
                name: name.clone(),
                args: args.clone(),
                distinct: *distinct,
                ignore_null: *ignore_nulls,
                from_last: *from_last,
                over: over.clone(),
            };
            marker::substitute(node, PlanMarker::new(MarkerKind::Window, found.len()));
            found.push(call);
            true
        });
    }
    found
}

// ***** spec ordering: getAllByItems / compareItems / sortWindowSpecs *****

/// Go `getAllByItems(itemsBuf, spec)` (`:7006`): the spec's `PARTITION BY`
/// items followed by its `ORDER BY` items.
#[must_use]
pub fn all_by_items(spec: &WindowSpec) -> Vec<OrderItem> {
    let mut items: Vec<OrderItem> = spec
        .partition_by
        .iter()
        .map(|expr| OrderItem {
            expr: expr.clone(),
            // Go's `PartitionBy.Items` are `*ast.ByItem` whose `Desc` the
            // parser leaves false; `compareItems` compares that field.
            desc: false,
        })
        .collect();
    items.extend(spec.order_by.iter().cloned());
    items
}

/// Go `restoreByItemText(item)` (`:7017`).
///
/// `// boundary:` `ast.ExprNode.Restore`, which [`tidb_ast`] does not export
/// per expression. The canonical AST rendering stands in. It is a total,
/// deterministic key over the same values, so [`compare_items`] stays a strict
/// weak ordering; the resulting spec ORDER can differ from Go's for two specs
/// whose restore texts and debug renderings sort differently. Go's own comment
/// says the sort exists so "we could add less `Sort` operator in physical
/// plan" — it is an optimisation over an arbitrary starting order, not a
/// semantic. Recorded rather than hidden.
#[must_use]
pub fn by_item_text(item: &OrderItem) -> String {
    format!("{:?}", item.expr)
}

/// Go `compareItems(lItems, rItems)` (`:7027`).
#[must_use]
pub fn compare_items(left: &[OrderItem], right: &[OrderItem]) -> bool {
    for (left_item, right_item) in left.iter().zip(right) {
        match by_item_text(left_item).cmp(&by_item_text(right_item)) {
            std::cmp::Ordering::Equal => {}
            other => return other.is_lt(),
        }
        // Go `compareBool(l.Desc, r.Desc)`: false sorts before true.
        if left_item.desc != right_item.desc {
            return !left_item.desc;
        }
    }
    left.len() < right.len()
}

/// Go `sortWindowSpecs(groupedFuncs, orderedSpec)` (`:7049`).
///
/// Go's comparator is `!compareItems(l, r)`, i.e. a REVERSED alphabetical
/// order, over a STABLE sort. `sort_by` is Rust's stable sort, and the
/// comparator below is Go's `less` verbatim: `less(i, j)` is true when
/// `compareItems` is false, which is `Ordering::Less`.
#[must_use]
pub fn sort_window_specs(arena: &SpecArena, ordered: &[SpecId]) -> Vec<SpecId> {
    let mut windows = ordered.to_vec();
    windows.sort_by(|left, right| {
        let left_items = all_by_items(&arena.get(*left).def.spec);
        let right_items = all_by_items(&arena.get(*right).def.spec);
        if !compare_items(&left_items, &right_items) {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Greater
        }
    });
    windows
}

// ***** GetAccurateCmpType, for the one shape this stage needs *****

/// Go `expression.GetAccurateCmpType(ctx, lhs, rhs)`
/// (`builtin_compare.go:1420`) SPECIALISED to `lhs` and `rhs` of the SAME
/// `FieldType`, which is the only shape a window frame bound produces; see
/// section 3.
///
/// Go's `getBaseCmpType` falls through to `ETReal` for a `DATETIME` /
/// `TIMESTAMP` / `DATE` pair — none of the string, int, decimal or
/// year-vs-date arms matches — and no override in `GetAccurateCmpType`
/// rescues it, because both the `ETString`-and-time arm and the
/// temporal-column-vs-constant arm require a constant on one side. A `RANGE`
/// frame over a datetime `ORDER BY` key therefore compares as REAL. That is a
/// Go quirk visible through the built `FrameBound`, and it is reproduced here
/// rather than repaired.
#[must_use]
pub fn cmp_type_for_same_field_type(field_type: &FieldType) -> EvalType {
    let code = field_type.code();
    let eval_type = field_type.eval_type();
    // `lft.GetType() == rft.GetType() == TypeUnspecified` -> `ETString`.
    if code == FieldTypeCode::Unspecified {
        return EvalType::String;
    }
    if eval_type == EvalType::VectorFloat32 {
        return EvalType::VectorFloat32;
    }
    if code == FieldTypeCode::Json {
        return EvalType::Json;
    }
    // `lhs.IsStringKind() && rhs.IsStringKind()` -> `ETString`; a string-kind
    // field type is never `IsTypeTime`, so the time override cannot fire.
    if matches!(eval_type, EvalType::String | EvalType::Json) {
        return EvalType::String;
    }
    // `(lhs == ETInt || lft.Hybrid()) && (rhs == ETInt || rft.Hybrid())`.
    if eval_type == EvalType::Int || is_hybrid(code) {
        return EvalType::Int;
    }
    if eval_type == EvalType::Decimal {
        return EvalType::Decimal;
    }
    // `lhsFieldType.GetType() == TypeDuration && rhs likewise` -> `ETDuration`.
    if code == FieldTypeCode::Duration {
        return EvalType::Duration;
    }
    // Everything left — REAL, DOUBLE, FLOAT, and the temporal-with-date types
    // — takes `getBaseCmpType`'s trailing `return types.ETReal`.
    EvalType::Real
}

/// Go `types.FieldType.Hybrid()`: the types stored as an integer but printed
/// as a string.
const fn is_hybrid(code: FieldTypeCode) -> bool {
    matches!(
        code,
        FieldTypeCode::Enum | FieldTypeCode::Bit | FieldTypeCode::Set
    )
}

/// Go `expression.GetCmpFunction`'s RESULT identity
/// (`builtin_compare.go:1489`), as [`FrameBound::cmp_func_tokens`] carries it;
/// see [`crate::logical::window`]'s header for why a token and not a function
/// pointer.
#[must_use]
pub fn cmp_func_token(eval_type: EvalType) -> &'static str {
    match eval_type {
        EvalType::Int => "CompareInt",
        EvalType::Real => "CompareReal",
        EvalType::Decimal => "CompareDecimal",
        EvalType::String => "CompareString",
        EvalType::Duration => "CompareDuration",
        EvalType::Datetime | EvalType::Timestamp => "CompareTime",
        EvalType::Json => "CompareJSON",
        EvalType::VectorFloat32 => "CompareVectorFloat32",
    }
}

// ***** the builder methods *****

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// Go `buildWindowSpecs(sel.WindowSpecs)` (`:7426`), which `buildSelect`
    /// calls at `:4541` — AFTER `HAVING` and before the grouping.
    ///
    /// # Errors
    ///
    /// `ErrWindowDuplicateName`, or any error [`resolve_window_spec`] raises.
    pub fn build_window_specs(&mut self, specs: &[(String, WindowDef)]) -> Result<(), PlanError> {
        let mut resolved: BTreeMap<String, NamedWindowSpec> = BTreeMap::new();
        for (name, def) in specs {
            let lower = name.to_ascii_lowercase();
            if resolved.contains_key(&lower) {
                return Err(PlanError::internal(format!(
                    "Window '{name}' is defined twice"
                )));
            }
            resolved.insert(lower, NamedWindowSpec::new(name.clone(), def.clone()));
        }
        // Go iterates `specsMap`, whose order is RANDOM. The result does not
        // depend on it: `resolveWindowSpec` resolves each reference chain
        // depth-first and clears `Ref` as it merges, so a spec already
        // resolved by an earlier chain is a no-op the second time. Iterating
        // in name order is therefore the same fixpoint, deterministically.
        let names: Vec<String> = resolved.keys().cloned().collect();
        let mut in_stack = BTreeSet::new();
        for name in names {
            resolve_window_spec(&name, &mut resolved, &mut in_stack)?;
        }
        self.window_specs = resolved;
        Ok(())
    }

    /// Go `handleDefaultFrame(spec, windowFuncName)` (`:7239`): the frame a
    /// window function gets when the SQL does not fix one, and the frame
    /// erasures TiDB applies regardless.
    ///
    /// Returns Go's `(newSpec, updated)`. The four rules, in Go's order:
    ///
    /// 1. a frame-needing function with an `ORDER BY` and NO frame gets
    ///    `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`;
    /// 2. a frame-needing function whose frame is unbounded on BOTH sides has
    ///    its frame ERASED, that being equivalent to no frame at all — note
    ///    Go tests only `UnBounded` on each end and not the bound TYPE, so
    ///    the syntactically impossible `UNBOUNDED FOLLOWING ... UNBOUNDED
    ///    PRECEDING` would also erase; `checkOriginWindowSpec` has already
    ///    rejected it;
    /// 3. a NON-frame-needing function with any frame has it erased, with a
    ///    NOTE (`ErrWindowFunctionIgnoresFrame`);
    /// 4. and then, only when `EnablePipelinedWindowExec` is on, a
    ///    non-frame-needing function with a `UseDefaultFrame` entry
    ///    (`ROW_NUMBER`) gets that fixed frame instead.
    ///
    /// Rule 3's erasure happens BEFORE rule 4 assigns, so `ROW_NUMBER OVER (w
    /// ROWS ...)` ends with the fixed `ROWS BETWEEN CURRENT ROW AND CURRENT
    /// ROW`, not with the written frame.
    #[must_use]
    pub fn handle_default_frame(&self, spec: &WindowDef, func_name: &str) -> (WindowDef, bool) {
        let needs_frame = need_frame(func_name);
        if needs_frame && spec.spec.frame.is_none() && !spec.spec.order_by.is_empty() {
            let mut new_spec = spec.clone();
            new_spec.spec.frame = Some(AstWindowFrame {
                kind: FrameKind::Range,
                start: AstFrameBound::UnboundedPreceding,
                end: AstFrameBound::CurrentRow,
            });
            return (new_spec, true);
        }
        if needs_frame {
            if let Some(frame) = &spec.spec.frame {
                if is_unbounded(&frame.start) && is_unbounded(&frame.end) {
                    let mut new_spec = spec.clone();
                    new_spec.spec.frame = None;
                    return (new_spec, true);
                }
            }
            return (spec.clone(), false);
        }

        let mut updated = false;
        let mut new_spec = spec.clone();
        if new_spec.spec.frame.is_some() {
            // `// boundary:` `StmtCtx.AppendNote(ErrWindowFunctionIgnoresFrame)`
            // (`:7268`). There is no statement-context warning sink in this
            // crate; the ERASURE, which is what the plan shows, is applied.
            new_spec.spec.frame = None;
            updated = true;
        }
        if self.enable_pipelined_window_exec {
            if let Some(default) = use_default_frame(func_name) {
                new_spec.spec.frame = Some(AstWindowFrame {
                    kind: if default.rows {
                        FrameKind::Rows
                    } else {
                        FrameKind::Range
                    },
                    start: if default.start_is_current_row {
                        AstFrameBound::CurrentRow
                    } else {
                        AstFrameBound::UnboundedPreceding
                    },
                    end: if default.end_is_current_row {
                        AstFrameBound::CurrentRow
                    } else {
                        AstFrameBound::UnboundedFollowing
                    },
                });
                updated = true;
            }
        }
        if updated {
            (new_spec, true)
        } else {
            (spec.clone(), false)
        }
    }

    /// Go `groupWindowFuncs(windowFuncs)` (`:7315`), returning the grouping in
    /// Go's `orderedSpec` order together with the [`SpecArena`] that owns the
    /// specs; see section 1.
    ///
    /// The `Vec<usize>` of each entry indexes `window_funcs`. An EMPTY one is
    /// Go's `groupedWindow[spec] = nil` for an unused named spec, which
    /// `buildWindowFunctions` still validates — rule 3's Option/empty
    /// distinction does not apply, because Go writes a nil slice precisely to
    /// mean "this group has no functions" and reads it back with `len(funcs)
    /// == 0`.
    ///
    /// # Errors
    ///
    /// `ErrWindowNoSuchWindow`, or any error [`merge_window_spec`] raises.
    pub fn group_window_funcs(
        &self,
        window_funcs: &[WindowFuncCall],
    ) -> Result<(SpecArena, WindowGroups), PlanError> {
        let mut arena = SpecArena::new();
        // Go's `b.windowSpecs[name]` pointers, interned ONCE so that every
        // function naming the same window shares one identity.
        let mut named: BTreeMap<String, SpecId> = BTreeMap::new();
        for (lower, spec) in &self.window_specs {
            named.insert(lower.clone(), arena.intern(spec.clone()));
        }
        let mut updated_spec_map: BTreeMap<String, Vec<SpecId>> = BTreeMap::new();
        let mut grouped: BTreeMap<SpecId, Vec<usize>> = BTreeMap::new();
        let mut ordered: Vec<SpecId> = Vec::new();

        for (position, window_func) in window_funcs.iter().enumerate() {
            let inline = match &window_func.over {
                // Go `windowFunc.Spec.Name.L == ""`: an inline spec, possibly
                // REFERENCING a named one through `Spec.Ref`.
                WindowOver::Def(def) => Some(def.clone()),
                WindowOver::Name(_) => None,
            };
            if let Some(mut def) = inline {
                if let Some(reference) = def.base.clone() {
                    let lower = reference.to_ascii_lowercase();
                    let Some(reference_spec) = self.window_specs.get(&lower) else {
                        return Err(PlanError::internal(format!(
                            "Window name '{}' is not defined",
                            window_name(&reference)
                        )));
                    };
                    merge_window_spec(&mut def, "", reference_spec)?;
                }
                let (def, _) = self.handle_default_frame(&def, &window_func.name);
                // Go takes `&windowFunc.Spec` — a fresh address per window
                // function even when two are textually identical.
                let id = arena.intern(NamedWindowSpec::anonymous(def));
                grouped.entry(id).or_default().push(position);
                append_if_absent_window_spec(&mut ordered, id);
                continue;
            }

            let WindowOver::Name(name) = &window_func.over else {
                unreachable!("the inline arm returned above");
            };
            let lower = name.to_ascii_lowercase();
            let Some(&spec_id) = named.get(&lower) else {
                return Err(PlanError::internal(format!(
                    "Window name '{name}' is not defined"
                )));
            };
            let (new_spec, updated) =
                self.handle_default_frame(&arena.get(spec_id).def, &window_func.name);
            if !updated {
                grouped.entry(spec_id).or_default().push(position);
                append_if_absent_window_spec(&mut ordered, spec_id);
                continue;
            }
            let named_spec = NamedWindowSpec::new(arena.get(spec_id).name.clone(), new_spec);
            let bucket = updated_spec_map.entry(lower).or_default();
            let existing = bucket
                .iter()
                .copied()
                .find(|id| spec_equal(Some(&arena.get(*id).def), Some(&named_spec.def)));
            let updated_id = match existing {
                Some(id) => id,
                None => {
                    let id = arena.intern(named_spec);
                    bucket.push(id);
                    id
                }
            };
            grouped.entry(updated_id).or_default().push(position);
            append_if_absent_window_spec(&mut ordered, updated_id);
        }

        // `:7373` "Unused window specs should also be checked in
        // b.buildWindowFunctions, so we add them to `groupedWindow` with empty
        // window functions." A named spec that some function REWROTE through
        // `handleDefaultFrame` is not unused, which is what the
        // `updatedSpecMap` lookup excludes.
        for (lower, &spec_id) in &named {
            if !grouped.contains_key(&spec_id) && !updated_spec_map.contains_key(lower) {
                grouped.insert(spec_id, Vec::new());
                append_if_absent_window_spec(&mut ordered, spec_id);
            }
        }

        let groups = ordered
            .into_iter()
            .map(|id| {
                let funcs = grouped.get(&id).cloned().unwrap_or_default();
                (id, funcs)
            })
            .collect();
        Ok((arena, groups))
    }

    /// Go `resolveWindowFunction(ctx, sel, p)` (`:3048`), the COLUMN half:
    /// "resolve the columns that don't exist in select fields".
    ///
    /// A window function is built ABOVE the select list's projection, so every
    /// column its arguments, `PARTITION BY` or `ORDER BY` names must be
    /// projected — `SELECT SUM(a) OVER (ORDER BY b)` has no `b` in its select
    /// list, and without this pass `b` is simply not there any more.
    ///
    /// The resolution is `havingWindowAndOrderbyExprResolver`'s
    /// `resolveFieldsFirst == false` arm (`:2857`), which for `fieldList`,
    /// `windowOrderByClause` and `partitionByClause` resolves against the
    /// PLAN ONLY and never falls back to the select fields. `resolveFromPlan`
    /// (`:2800`) then appends an `Auxiliary` select field per reference —
    /// UNCONDITIONALLY, so two references to one column append two fields —
    /// and records `colMapper[node] = index`, which is
    /// [`MarkerKind::Column`] here.
    ///
    /// `// narrowing:` Go runs this at `:4397`, BEFORE
    /// `resolveHavingAndOrderBy`; here it runs after, so that a window
    /// function hoisted out of `ORDER BY` into a hidden field (Go's own
    /// `:3075` `OrderBy` loop) is already present and gets resolved by the
    /// same pass. Only the ORDER of the appended hidden fields differs, and
    /// `buildSelect`'s `:4620` trailing projection trims all of them.
    ///
    /// `// boundary:` `appendAuxiliaryFieldsForSubqueries` (`:3101`), the
    /// SUBQUERY half of the same Go function. A window argument containing a
    /// correlated subquery keeps its outer reference here rather than gaining
    /// an auxiliary field.
    ///
    /// # Errors
    ///
    /// `ErrUnknownColumn` for a reference that names neither this block nor an
    /// outer one.
    pub fn resolve_window_function(
        &self,
        windows: &mut [(String, WindowDef)],
        fields: &mut Vec<ProjectionField>,
        names: &[FieldName],
    ) -> Result<(), PlanError> {
        // `// boundary:` the `windowAggMap` half of this same Go function; see
        // section 3. The refusal is made HERE, before
        // `extractAggFuncsInSelectFields` (`:4487`) turns those aggregates
        // into markers and the shape stops being recognisable.
        for field in fields.iter() {
            if has_window_flag(&field.expr) && expr_contains_aggregate(&field.expr) {
                return Err(PlanError::internal(
                    "an aggregate in a select field that also carries a window function is not ported: resolveWindowFunction (logical_plan_builder.go:3048)",
                ));
            }
        }
        for (_, def) in windows.iter() {
            let aggregated = def.spec.partition_by.iter().any(expr_contains_aggregate)
                || def
                    .spec
                    .order_by
                    .iter()
                    .any(|item| expr_contains_aggregate(&item.expr));
            if aggregated {
                return Err(PlanError::internal(
                    "an aggregate inside a named WINDOW specification is not ported: resolveWindowFunction (logical_plan_builder.go:3048)",
                ));
            }
        }

        let mut position = 0;
        while position < fields.len() {
            if !has_window_flag(&fields[position].expr) {
                position += 1;
                continue;
            }
            let mut expr = fields[position].expr.clone();
            self.resolve_window_expr_columns(&mut expr, fields, names)?;
            fields[position].expr = expr;
            position += 1;
        }
        // `:3069` `for _, spec := range sel.WindowSpecs`: a named spec is
        // resolved whether or not any function uses it.
        for (_, def) in windows.iter_mut() {
            for expr in &mut def.spec.partition_by {
                self.resolve_window_expr_columns(expr, fields, names)?;
            }
            for item in &mut def.spec.order_by {
                self.resolve_window_expr_columns(&mut item.expr, fields, names)?;
            }
        }
        Ok(())
    }

    /// One expression's worth of [`Self::resolve_window_function`].
    fn resolve_window_expr_columns(
        &self,
        expr: &mut Expr,
        fields: &mut Vec<ProjectionField>,
        names: &[FieldName],
    ) -> Result<(), PlanError> {
        let mut error = None;
        visit_exprs(expr, &mut |node| {
            if error.is_some() {
                return true;
            }
            // A marker some earlier pass substituted is already bound.
            if PlanMarker::from_expr(node).is_some() {
                return true;
            }
            let Expr::Column(path) = node else {
                return false;
            };
            if super::find_field_name(names, path).is_some() {
                let index = fields.len();
                fields.push(ProjectionField {
                    expr: node.clone(),
                    alias: None,
                    text: None,
                    hidden: true,
                });
                marker::substitute(node, PlanMarker::new(MarkerKind::Column, index));
                return true;
            }
            // `:2871` "If we can't find it any where, it may be a correlated
            // column" — left alone for the rewriter's outer-scope pass.
            if self
                .outer_names
                .iter()
                .any(|scope| super::find_field_name(scope, path).is_some())
            {
                return true;
            }
            error = Some(PlanError::internal(format!(
                "Unknown column '{}' in 'field list'",
                path.join(".")
            )));
            true
        });
        error.map_or(Ok(()), Err)
    }

    /// Go `checkOriginWindowFrameBound(bound, spec, orderByItems)` (`:7196`).
    ///
    /// # Errors
    ///
    /// `ErrWindowRowsIntervalUse`, `ErrWindowFrameIllegal`,
    /// `ErrWindowRangeFrameOrderType`, `ErrWindowRangeFrameNumericType` and
    /// `ErrWindowRangeFrameTemporalType`, in Go's order.
    pub fn check_origin_window_frame_bound(
        &self,
        bound: &AstFrameBound,
        spec: &NamedWindowSpec,
        order_by: &[WindowSortItem],
    ) -> Result<(), PlanError> {
        let Some(offset) = bound_offset(bound) else {
            // `bound.Type == ast.CurrentRow || bound.UnBounded`.
            return Ok(());
        };
        let Some(frame) = &spec.def.spec.frame else {
            return Ok(());
        };
        let unit = interval_unit(offset);
        if frame.kind == FrameKind::Rows {
            if unit.is_some() {
                return Err(PlanError::internal(format!(
                    "Window '{}' with ROWS frame requires an integer frame value",
                    window_name(&spec.name)
                )));
            }
            match self.frame_offset_uint(offset) {
                Some((_, false)) => return Ok(()),
                _ => {
                    return Err(PlanError::internal(format!(
                        "Window '{}' has an illegal frame definition",
                        window_name(&spec.name)
                    )))
                }
            }
        }

        if order_by.len() != 1 {
            return Err(PlanError::internal(format!(
                "Window '{}' with RANGE N PRECEDING/FOLLOWING frame requires exactly one ORDER BY expression, of numeric or temporal type",
                window_name(&spec.name)
            )));
        }
        let order_type = order_by[0]
            .col
            .get_static_type()
            .map_or(FieldTypeCode::Unspecified, FieldType::code);
        let is_numeric = order_type.is_type_numeric();
        let is_temporal = order_type.is_type_temporal();
        if !is_numeric && !is_temporal {
            return Err(PlanError::internal(format!(
                "Window '{}' with RANGE N PRECEDING/FOLLOWING frame requires exactly one ORDER BY expression, of numeric or temporal type",
                window_name(&spec.name)
            )));
        }
        if unit.is_some() && !is_temporal {
            return Err(PlanError::internal(format!(
                "Window '{}' with RANGE frame has ORDER BY expression of numeric type, INTERVAL bound value not allowed",
                window_name(&spec.name)
            )));
        }
        if unit.is_none() && !is_numeric {
            return Err(PlanError::internal(format!(
                "Window '{}' with RANGE frame has ORDER BY expression of datetime type. Only INTERVAL bound value allowed",
                window_name(&spec.name)
            )));
        }
        Ok(())
    }

    /// Go `checkOriginWindowSpec(spec, orderByItems)` (`:7164`): the frame's
    /// own legality, independent of the function it belongs to.
    ///
    /// `ast.Groups` is unrepresentable here; see section 3.
    ///
    /// # Errors
    ///
    /// `ErrWindowFrameStartIllegal`, `ErrWindowFrameEndIllegal`,
    /// `ErrWindowFrameIllegal`, and whatever
    /// [`Self::check_origin_window_frame_bound`] raises.
    pub fn check_origin_window_spec(
        &self,
        spec: &NamedWindowSpec,
        order_by: &[WindowSortItem],
    ) -> Result<(), PlanError> {
        let Some(frame) = &spec.def.spec.frame else {
            return Ok(());
        };
        let start = &frame.start;
        let end = &frame.end;
        let illegal = |what: &str| {
            PlanError::internal(format!(
                "Window '{}' has an illegal frame {what}",
                window_name(&spec.name)
            ))
        };
        if matches!(start, AstFrameBound::UnboundedFollowing) {
            return Err(illegal("start"));
        }
        if matches!(end, AstFrameBound::UnboundedPreceding) {
            return Err(illegal("end"));
        }
        let start_following = matches!(
            start,
            AstFrameBound::Following(_) | AstFrameBound::UnboundedFollowing
        );
        let end_preceding = matches!(
            end,
            AstFrameBound::Preceding(_) | AstFrameBound::UnboundedPreceding
        );
        if start_following && (end_preceding || matches!(end, AstFrameBound::CurrentRow)) {
            return Err(illegal("definition"));
        }
        if (start_following || matches!(start, AstFrameBound::CurrentRow)) && end_preceding {
            return Err(illegal("definition"));
        }
        self.check_origin_window_frame_bound(start, spec, order_by)?;
        self.check_origin_window_frame_bound(end, spec, order_by)
    }

    /// Go `checkOriginWindowFuncs(funcs, orderByItems)` (`:7141`): the
    /// modifiers TiDB does not implement, plus each function's ORIGINAL spec
    /// re-checked — the grouped spec differs from what was written, so
    /// `checkOriginWindowSpec` must see the written one.
    ///
    /// # Errors
    ///
    /// `ErrNotSupportedYet` for `IGNORE NULLS`, `DISTINCT` and `FROM LAST`,
    /// then [`Self::check_origin_window_spec`]'s errors.
    pub fn check_origin_window_funcs(
        &self,
        funcs: &[&WindowFuncCall],
        order_by: &[WindowSortItem],
    ) -> Result<(), PlanError> {
        for func in funcs {
            if func.ignore_null {
                return Err(PlanError::internal(
                    "function IGNORE NULLS has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions",
                ));
            }
            if func.distinct {
                return Err(PlanError::internal(
                    "function <window function>(DISTINCT ..) has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions",
                ));
            }
            if func.from_last {
                return Err(PlanError::internal(
                    "function FROM LAST has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions",
                ));
            }
            let spec = match &func.over {
                WindowOver::Name(name) => {
                    match self.window_specs.get(&name.to_ascii_lowercase()) {
                        Some(spec) => spec.clone(),
                        // Go indexes `b.windowSpecs[f.Spec.Name.L]` and would
                        // dereference nil; `groupWindowFuncs` has already
                        // raised `ErrWindowNoSuchWindow` for a missing name,
                        // so this arm is unreachable through
                        // `buildWindowFunctions`.
                        None => continue,
                    }
                }
                WindowOver::Def(def) => NamedWindowSpec::anonymous(def.clone()),
            };
            self.check_origin_window_spec(&spec, order_by)?;
        }
        Ok(())
    }

    /// The offset expression of a frame bound as a `u64`, standing for Go
    /// `getUintFromNode(ctx, expr, false)` (`util.go`), whose third result
    /// "is the expected type" is `None` here.
    ///
    /// Go's own function reads `*ast.ValueExpr` and `*ast.ParamMarkerExpr`
    /// directly. Rewriting against an EMPTY scope is the same restriction by
    /// another road: a bound offset that names a column does not build, and so
    /// reports `None` exactly as Go's `default: return 0, false, false` does.
    fn frame_offset_uint(&self, offset: &Expr) -> Option<(u64, bool)> {
        let expr = self
            .rewrite_scalar(offset, &Schema::default(), &[], &BTreeMap::new())
            .ok()?;
        get_uint64_from_constant(&expr, self.ctx)
    }

    /// Go `buildWindowFunctionFrameBound(ctx, spec, orderByItems, boundClause)`
    /// (`:6873`).
    ///
    /// The derivation, in Go's order:
    ///
    /// * an UNBOUNDED bound carries nothing else and returns immediately;
    /// * under `ROWS`, `CURRENT ROW` likewise, and an offset becomes
    ///   `Num` through `getUintFromNode` — whose failure Go DISCARDS
    ///   (`numRows, _, _`), leaving `Num` zero, because
    ///   `checkOriginWindowFrameBound` has already rejected a bad one. That
    ///   discard is reproduced;
    /// * under `RANGE`, `CalcFuncs` and `CmpFuncs` get ONE entry per ORDER BY
    ///   item. For `CURRENT ROW` each entry is the order column itself
    ///   compared with itself; for an explicit offset there is exactly one
    ///   entry, `col ± offset` (or `date_add`/`date_sub` for an `INTERVAL`),
    ///   with the sign flipped when the ORDER BY is descending.
    ///
    /// # Errors
    ///
    /// `ErrWindowRangeBoundNotConstant` and `ErrWindowFrameIllegal`, plus the
    /// refusal named in section 3 when the offset does not fold to a constant.
    pub fn build_window_function_frame_bound(
        &self,
        spec: &NamedWindowSpec,
        order_by: &[WindowSortItem],
        bound: &AstFrameBound,
    ) -> Result<FrameBound, PlanError> {
        let frame_kind = spec
            .def
            .spec
            .frame
            .as_ref()
            .map_or(FrameKind::Rows, |frame| frame.kind);
        let mut built = FrameBound {
            bound_type: bound_type_of(bound),
            unbounded: is_unbounded(bound),
            is_explicit_range: false,
            ..FrameBound::default()
        };
        if built.unbounded {
            return Ok(built);
        }

        if frame_kind == FrameKind::Rows {
            if built.bound_type == BoundType::CurrentRow {
                return Ok(built);
            }
            let offset = bound_offset(bound).expect("a non-unbounded, non-current-row bound");
            built.num = self.frame_offset_uint(offset).map_or(0, |(value, _)| value);
            return Ok(built);
        }

        built.calc_funcs = Vec::with_capacity(order_by.len());
        built.cmp_func_tokens = Vec::with_capacity(order_by.len());
        if built.bound_type == BoundType::CurrentRow {
            for item in order_by {
                let eval_type = item
                    .col
                    .get_static_type()
                    .map_or(EvalType::Real, cmp_type_for_same_field_type);
                built.calc_funcs.push(Expression::Column(item.col.clone()));
                built
                    .cmp_func_tokens
                    .push(cmp_func_token(eval_type).to_owned());
            }
            return Ok(built);
        }

        // `checkOriginWindowFrameBound` has established `len(orderByItems) ==
        // 1` for an explicit RANGE bound before this runs.
        let Some(item) = order_by.first() else {
            return Err(PlanError::internal(format!(
                "Window '{}' with RANGE N PRECEDING/FOLLOWING frame requires exactly one ORDER BY expression, of numeric or temporal type",
                window_name(&spec.name)
            )));
        };
        let col = item.col.clone();
        let offset = bound_offset(bound).expect("a non-unbounded, non-current-row bound");
        let (value_expr, unit) = match interval_unit(offset) {
            Some(unit) => (interval_value(offset), Some(unit.to_owned())),
            None => (offset, None),
        };

        // `// boundary:` `evalAstExprWithPlanCtx` (`:6900`). Only a bound the
        // rewriter yields as a folded constant is accepted; see section 3.
        let constant = self
            .rewrite_scalar(value_expr, &Schema::default(), &[], &BTreeMap::new())
            .map_err(|_| {
                PlanError::internal(format!(
                    "Window '{}' has a non-constant frame bound",
                    window_name(&spec.name)
                ))
            })?;
        if !matches!(constant, Expression::Constant(_)) {
            return Err(PlanError::internal(format!(
                "Window '{}' has a non-constant frame bound: evalAstExprWithPlanCtx (logical_plan_builder.go:6900) is not ported",
                window_name(&spec.name)
            )));
        }
        // Go evaluates the constant as an INT and rejects a negative, a NULL
        // or an evaluation error with `ErrWindowFrameIllegal`. A negative
        // signed datum is exactly what `get_uint64_from_constant` refuses.
        match get_uint64_from_constant(&constant, self.ctx) {
            Some((_, false)) => {}
            _ => {
                return Err(PlanError::internal(format!(
                    "Window '{}' has an illegal frame definition",
                    window_name(&spec.name)
                )))
            }
        }

        built.is_explicit_range = true;
        let desc = item.desc;
        // "When the order is asc: `+` for following, and `-` for the
        // preceding. When the order is desc, `+` becomes `-` and vice-versa."
        let subtract = (!desc && built.bound_type == BoundType::Preceding)
            || (desc && built.bound_type == BoundType::Following);
        let ret_type = col
            .get_static_type()
            .cloned()
            .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
        let args = match &unit {
            Some(unit) => vec![
                Expression::Column(col.clone()),
                constant,
                Expression::Constant(tidb_expr::constant::Constant::new(
                    tidb_datatype::Datum::Bytes(unit.clone().into_bytes()),
                    FieldType::new(FieldTypeCode::Varchar),
                )),
            ],
            None => vec![Expression::Column(col.clone()), constant],
        };
        let func_name = match (&unit, subtract) {
            (Some(_), true) => "date_sub",
            (Some(_), false) => "date_add",
            (None, true) => "minus",
            (None, false) => "plus",
        };
        let calc = new_function_base(self.ctx, func_name, ret_type.clone(), args)?;
        built.calc_funcs.push(calc);
        built.cmp_func_tokens.push(String::new());
        // Go `GetAccurateCmpType(col, bound.CalcFuncs[0])`; the calc function
        // was built WITH `col.RetType`, so both sides carry one field type.
        built.update_cmp_funcs_and_cmp_data_type(cmp_type_for_same_field_type(&ret_type));
        Ok(built)
    }

    /// Go `buildWindowFunctionFrame(ctx, spec, orderByItems)` (`:6966`).
    ///
    /// Rule 3: Go returns a nil `*WindowFrame` for a spec with no frame
    /// clause, and `LogicalWindow.Frame` being nil is what the whole optimizer
    /// reads as "the partition is the frame". That is [`Option`], not an
    /// empty [`WindowFrame`].
    ///
    /// # Errors
    ///
    /// Either bound's error.
    pub fn build_window_function_frame(
        &self,
        spec: &NamedWindowSpec,
        order_by: &[WindowSortItem],
    ) -> Result<Option<WindowFrame>, PlanError> {
        let Some(frame) = &spec.def.spec.frame else {
            return Ok(None);
        };
        let start = self.build_window_function_frame_bound(spec, order_by, &frame.start)?;
        let end = self.build_window_function_frame_bound(spec, order_by, &frame.end)?;
        Ok(Some(WindowFrame {
            frame_type: match frame.kind {
                FrameKind::Rows => FrameType::Rows,
                FrameKind::Range => FrameType::Ranges,
            },
            start: Some(start),
            end: Some(end),
        }))
    }

    /// Go `buildArgs4WindowFunc(ctx, p, args, aggMap)` (`:6798`): the argument
    /// expressions `checkWindowFuncArgs` type-checks against, WITHOUT
    /// projecting them.
    ///
    /// Go allocates a fresh column for any argument that is neither a column
    /// nor a constant, "because we only want to return the args used in window
    /// function"; the column's only role is to carry the argument's type.
    ///
    /// # Errors
    ///
    /// Any argument's build error.
    pub fn build_args_for_window_func(
        &mut self,
        args: &[Expr],
        schema: &Schema,
        names: &[FieldName],
        markers: &BTreeMap<MarkerKind, Vec<Column>>,
    ) -> Result<Vec<Expression>, PlanError> {
        self.opt_flag |= flags::ELIMINATE_PROJECTION;
        let mut built = Vec::with_capacity(args.len());
        for arg in args {
            let expr = self.rewrite_scalar(arg, schema, names, markers)?;
            if matches!(expr, Expression::Column(_) | Expression::Constant(_)) {
                built.push(expr);
                continue;
            }
            let ret_type = expr
                .static_type()
                .cloned()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            built.push(Expression::Column(Column::new(
                self.column_ids.alloc(),
                ret_type,
            )));
        }
        Ok(built)
    }

    /// Go `checkWindowFuncArgs(ctx, p, windowFuncExprs, windowAggMap)`
    /// (`:6981`): "we need to check the func args first before we check the
    /// window spec".
    ///
    /// # Errors
    ///
    /// `ErrNotSupportedYet` for `GROUP_CONCAT`, `ErrWrongArguments` when
    /// `NewWindowFuncDesc` rejects the arguments, and any argument's build
    /// error.
    pub fn check_window_func_args(
        &mut self,
        window_funcs: &[WindowFuncCall],
        schema: &Schema,
        names: &[FieldName],
        markers: &BTreeMap<MarkerKind, Vec<Column>>,
    ) -> Result<(), PlanError> {
        for func in window_funcs {
            if func.name.eq_ignore_ascii_case("group_concat") {
                return Err(PlanError::internal(
                    "function group_concat as window function has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions",
                ));
            }
            let args = self.build_args_for_window_func(&func.args, schema, names, markers)?;
            // `// boundary:` `ParamMarkerInPrepareChecker`; see section 3. The
            // `false` is Go's non-prepared arm, which DOES check.
            let desc = WindowFuncDesc::new(self.ctx, &func.name, args, false)
                .map_err(|error| PlanError::internal(error.to_string()))?;
            if desc.is_none() {
                return Err(PlanError::internal(format!(
                    "Incorrect arguments to {}",
                    func.name.to_ascii_lowercase()
                )));
            }
        }
        Ok(())
    }

    /// Go `buildByItemsForWindow(ctx, p, proj, items, retItems, aggMap)`
    /// (`:6826`): rewrites one by-item list against the child and makes sure
    /// each resulting column is projected.
    ///
    /// # Errors
    ///
    /// Any item's build error, or the `itemTransformer` refusal in section 3.
    #[allow(clippy::too_many_arguments)]
    pub fn build_by_items_for_window(
        &mut self,
        items: &[OrderItem],
        schema: &Schema,
        names: &[FieldName],
        markers: &BTreeMap<MarkerKind, Vec<Column>>,
        proj_exprs: &mut Vec<Expression>,
        proj_schema: &mut Schema,
        proj_names: &mut Vec<FieldName>,
        sort_items: &mut Vec<WindowSortItem>,
    ) -> Result<(), PlanError> {
        for item in items {
            if matches!(item.expr, Expr::Int(_)) {
                // `// boundary:` `itemTransformer` (`:2380`) as
                // `buildByItemsForWindow` applies it; see section 3.
                return Err(PlanError::internal(
                    "a positional window PARTITION BY / ORDER BY item is not ported: itemTransformer (logical_plan_builder.go:2380)",
                ));
            }
            let built = self.rewrite_scalar(&item.expr, schema, names, markers)?;
            // `:6845` an item whose type is NULL contributes no sort key at
            // all — Go `continue`s, so it is neither projected nor ordered on.
            if built
                .static_type()
                .is_some_and(|ft| ft.code() == FieldTypeCode::Null)
            {
                continue;
            }
            if let Expression::Column(column) = &built {
                sort_items.push(WindowSortItem::new(column.clone(), item.desc));
                if !proj_schema.contains(column) {
                    proj_exprs.push(built.clone());
                    proj_names.push(FieldName::default());
                    proj_schema.append([column.clone()]);
                }
                continue;
            }
            let ret_type = built
                .static_type()
                .cloned()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            proj_exprs.push(built);
            proj_names.push(FieldName::default());
            let column = Column::new(self.column_ids.alloc(), ret_type);
            proj_schema.append([column.clone()]);
            sort_items.push(WindowSortItem::new(column, item.desc));
        }
        Ok(())
    }

    /// Go `buildProjectionForWindow(ctx, p, spec, args, aggMap)` (`:6728`):
    /// "builds the projection for expressions in the window specification that
    /// is not a column, so after the projection, window functions only needs
    /// to deal with columns".
    ///
    /// Returns Go's four results: the projection, the `PARTITION BY` sort
    /// items, the `ORDER BY` sort items, and the rewritten arguments.
    ///
    /// # Errors
    ///
    /// Any by-item's or argument's build error.
    pub fn build_projection_for_window(
        &mut self,
        plan: LogicalPlan,
        spec: &NamedWindowSpec,
        args: &[Expr],
        markers: &BTreeMap<MarkerKind, Vec<Column>>,
    ) -> Result<WindowProjection, PlanError> {
        self.opt_flag |= flags::ELIMINATE_PROJECTION;
        // Rule 3 of [`super`]: both snapshots precede the move.
        let (schema, names) = snapshot_schema_and_names(&plan);

        let mut proj_exprs: Vec<Expression> = schema
            .columns
            .iter()
            .map(|column| Expression::Column(column.clone()))
            .collect();
        let mut proj_schema = Schema::new(schema.columns.clone());
        let mut proj_names = names.clone();

        let mut partition_by = Vec::new();
        self.cur_clause = ClauseCode::PartitionBy;
        self.build_by_items_for_window(
            &partition_by_items(&spec.def.spec),
            &schema,
            &names,
            markers,
            &mut proj_exprs,
            &mut proj_schema,
            &mut proj_names,
            &mut partition_by,
        )?;
        let mut order_by = Vec::new();
        self.cur_clause = ClauseCode::WindowOrderBy;
        self.build_by_items_for_window(
            &spec.def.spec.order_by,
            &schema,
            &names,
            markers,
            &mut proj_exprs,
            &mut proj_schema,
            &mut proj_names,
            &mut order_by,
        )?;

        let mut new_args = Vec::with_capacity(args.len());
        for arg in args {
            let built = self.rewrite_scalar(arg, &schema, &names, markers)?;
            match &built {
                Expression::Constant(_) => {
                    new_args.push(built);
                    continue;
                }
                Expression::Column(column) => {
                    if !proj_schema.contains(column) {
                        proj_exprs.push(built.clone());
                        proj_names.push(FieldName::default());
                        proj_schema.append([column.clone()]);
                    }
                    new_args.push(built);
                    continue;
                }
                _ => {}
            }
            let ret_type = built
                .static_type()
                .cloned()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            proj_exprs.push(built);
            proj_names.push(FieldName::default());
            let column = Column::new(self.column_ids.alloc(), ret_type);
            proj_schema.append([column.clone()]);
            new_args.push(Expression::Column(column));
        }

        let mut projection = LogicalProjection::new(self.base(LogicalProjection::TYPE), proj_exprs);
        projection.base.set_children(vec![plan]);
        projection.base.base.set_schema(Some(proj_schema));
        projection.base.base.set_output_names(proj_names);
        Ok((
            LogicalPlan::Projection(projection),
            partition_by,
            order_by,
            new_args,
        ))
    }

    /// Go `buildWindowFunctions(ctx, p, groupedFuncs, orderedSpec, aggMap)`
    /// (`:7064`): one `LogicalWindow` per group, stacked in
    /// [`sort_window_specs`] order.
    ///
    /// Returns the plan and Go's `windowMapper` as the columns
    /// [`MarkerKind::Window`] binds to — index `k` is the k-th call
    /// [`extract_window_funcs`] found.
    ///
    /// # Errors
    ///
    /// Every error the per-group checks and the frame build raise.
    pub fn build_window_functions(
        &mut self,
        mut plan: LogicalPlan,
        window_funcs: &[WindowFuncCall],
        arena: &SpecArena,
        groups: &WindowGroups,
        markers: &BTreeMap<MarkerKind, Vec<Column>>,
    ) -> Result<(LogicalPlan, BTreeMap<usize, Column>), PlanError> {
        if self.building_cte {
            if let Some(cte) = self.outer_ctes.last_mut() {
                cte.contain_recursive_forbidden_operator = true;
            }
        }
        let ordered: Vec<SpecId> =
            sort_window_specs(arena, &groups.iter().map(|(id, _)| *id).collect::<Vec<_>>());
        let by_spec: BTreeMap<SpecId, &Vec<usize>> =
            groups.iter().map(|(id, funcs)| (*id, funcs)).collect();

        let mut window_columns: BTreeMap<usize, Column> = BTreeMap::new();
        for spec_id in ordered {
            let spec = arena.get(spec_id);
            let positions: &[usize] = by_spec.get(&spec_id).map_or(&[], |funcs| funcs.as_slice());
            let funcs: Vec<&WindowFuncCall> = positions
                .iter()
                .map(|index| &window_funcs[*index])
                .collect();
            let args: Vec<Expr> = funcs
                .iter()
                .flat_map(|func| func.args.iter().cloned())
                .collect();

            let (projected, partition_by, order_by, args) =
                self.build_projection_for_window(plan, spec, &args, markers)?;
            if funcs.is_empty() {
                // `:7079` "len(funcs) == 0 indicates this an unused named
                // window spec, so we just check for its validity and don't
                // have to build plan for it." Go `continue`s with `p`
                // UNCHANGED, discarding the projection it just built; here the
                // child is taken back out of it, which is the same discard.
                self.check_origin_window_spec(spec, &order_by)?;
                plan = take_only_child(projected);
                continue;
            }
            self.check_origin_window_funcs(&funcs, &order_by)?;
            let frame = self.build_window_function_frame(spec, &order_by)?;

            let (child_schema, child_names) = snapshot_schema_and_names(&projected);
            let mut schema_columns = child_schema.columns.clone();
            let mut output_names = child_names;
            let mut descs = Vec::with_capacity(funcs.len());
            let mut consumed = 0usize;
            for (func, position) in funcs.iter().zip(positions) {
                let arg_count = func.args.len();
                let func_args = args[consumed..consumed + arg_count].to_vec();
                consumed += arg_count;
                let desc = WindowFuncDesc::new(self.ctx, &func.name, func_args, false)
                    .map_err(|error| PlanError::internal(error.to_string()))?;
                let Some(mut desc) = desc else {
                    return Err(PlanError::internal(format!(
                        "Incorrect arguments to {}",
                        func.name.to_ascii_lowercase()
                    )));
                };
                desc.base
                    .wrap_cast_for_agg_args(self.ctx)
                    .map_err(|error| PlanError::internal(error.to_string()))?;
                let mut column = Column::new(self.column_ids.alloc(), desc.base.ret_type.clone());
                column.index = schema_columns.len() as i64;
                window_columns.insert(*position, column.clone());
                schema_columns.push(column);
                output_names.push(FieldName::default());
                descs.push(desc);
            }

            let mut window = LogicalWindow::new(self.base(LogicalWindow::TYPE), descs);
            window.partition_by = partition_by;
            window.order_by = order_by;
            window.frame = frame;
            window.base.set_children(vec![projected]);
            window
                .base
                .base
                .set_schema(Some(Schema::new(schema_columns)));
            window.base.base.set_output_names(output_names);
            plan = LogicalPlan::Window(window);
        }
        Ok((plan, window_columns))
    }

    /// The [`MarkerKind::Window`] binding: index `k` is the k-th call
    /// [`extract_window_funcs`] found, so the vector must be DENSE.
    ///
    /// A group that never built (there is none once
    /// [`Self::build_window_functions`] returns `Ok`) would leave a hole; the
    /// fallback column keeps the vector dense so a marker never silently
    /// resolves to a NEIGHBOUR's column.
    #[must_use]
    pub fn window_marker_columns(count: usize, columns: &BTreeMap<usize, Column>) -> Vec<Column> {
        (0..count)
            .map(|index| {
                columns
                    .get(&index)
                    .cloned()
                    .unwrap_or_else(|| Column::new(0, FieldType::new(FieldTypeCode::LongLong)))
            })
            .collect()
    }

    /// `buildSelect`'s whole window stage (`:4541-4571`), as one call: the
    /// spec table, the extraction, the argument check, the grouping, and the
    /// operators.
    ///
    /// Returns the plan and the [`MarkerKind::Window`] columns the caller
    /// binds before the second projection. `fields` is mutated in place, with
    /// each window call replaced by its marker.
    ///
    /// [`Self::resolve_window_function`] and [`Self::build_window_specs`] must
    /// have run first, exactly as `buildSelect` orders them.
    ///
    /// # Errors
    ///
    /// Every error this module raises, plus the `resolveWindowFunction`
    /// refusal named in section 3.
    pub fn build_window_stage(
        &mut self,
        plan: LogicalPlan,
        fields: &mut [ProjectionField],
        markers: &BTreeMap<MarkerKind, Vec<Column>>,
    ) -> Result<(LogicalPlan, Vec<Column>), PlanError> {
        let window_funcs = extract_window_funcs(fields);
        let (schema, names) = snapshot_schema_and_names(&plan);
        // `:4550` "we need to check the func args first before we check the
        // window spec".
        self.check_window_func_args(&window_funcs, &schema, &names, markers)?;
        let (arena, groups) = self.group_window_funcs(&window_funcs)?;
        let (plan, columns) =
            self.build_window_functions(plan, &window_funcs, &arena, &groups, markers)?;
        Ok((
            plan,
            Self::window_marker_columns(window_funcs.len(), &columns),
        ))
    }
}

// ***** small AST predicates *****

/// Go `ast.FrameBound.UnBounded`.
#[must_use]
pub fn is_unbounded(bound: &AstFrameBound) -> bool {
    matches!(
        bound,
        AstFrameBound::UnboundedPreceding | AstFrameBound::UnboundedFollowing
    )
}

/// Go `ast.FrameBound.Type`.
#[must_use]
pub fn bound_type_of(bound: &AstFrameBound) -> BoundType {
    match bound {
        AstFrameBound::UnboundedPreceding | AstFrameBound::Preceding(_) => BoundType::Preceding,
        AstFrameBound::CurrentRow => BoundType::CurrentRow,
        AstFrameBound::UnboundedFollowing | AstFrameBound::Following(_) => BoundType::Following,
    }
}

/// Go `ast.FrameBound.Expr`, present only for an explicit offset.
#[must_use]
pub fn bound_offset(bound: &AstFrameBound) -> Option<&Expr> {
    match bound {
        AstFrameBound::Preceding(expr) | AstFrameBound::Following(expr) => Some(expr.as_ref()),
        _ => None,
    }
}

/// Go `ast.FrameBound.Unit != ast.TimeUnitInvalid`. This AST carries the unit
/// INSIDE the offset expression, as [`Expr::Interval`].
#[must_use]
pub fn interval_unit(offset: &Expr) -> Option<&str> {
    match offset {
        Expr::Interval { unit, .. } => Some(unit.as_str()),
        _ => None,
    }
}

/// The magnitude of an [`Expr::Interval`], or the expression itself.
#[must_use]
pub fn interval_value(offset: &Expr) -> &Expr {
    match offset {
        Expr::Interval { value, .. } => value.as_ref(),
        other => other,
    }
}

/// Go `ast.HasWindowFlag(expr)` AFTER [`extract_window_funcs`] has run: the
/// window call is a [`MarkerKind::Window`] marker by then, so "carries a
/// window function" is "carries such a marker, at any depth".
#[must_use]
pub fn expr_carries_window_marker(expr: &Expr) -> bool {
    let mut found = false;
    walk_exprs(expr, &mut |node| {
        if PlanMarker::index_of_kind(node, MarkerKind::Window).is_some() {
            found = true;
            return true;
        }
        false
    });
    found
}

/// Whether `expr` contains an aggregate call anywhere, which is what
/// `resolveWindowFunction`'s `windowAggMap` would key on.
fn expr_contains_aggregate(expr: &Expr) -> bool {
    let mut found = false;
    walk_exprs(expr, &mut |node| {
        if is_aggregate_call(node) {
            found = true;
            return true;
        }
        false
    });
    found
}

/// Takes a single-child operator's child back out, for Go's "build it and
/// then `continue`" discard in `buildWindowFunctions`.
fn take_only_child(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Projection(mut projection) => {
            let mut children = std::mem::take(projection.base.children_mut());
            if children.is_empty() {
                LogicalPlan::Projection(projection)
            } else {
                children.remove(0)
            }
        }
        other => other,
    }
}

/// Go `spec.PartitionBy.Items` as [`OrderItem`]s, which is the shape
/// `buildByItemsForWindow` takes for BOTH lists. Go's `ByItem.Desc` is false
/// for every `PARTITION BY` entry.
#[must_use]
pub fn partition_by_items(spec: &WindowSpec) -> Vec<OrderItem> {
    spec.partition_by
        .iter()
        .map(|expr| OrderItem {
            expr: expr.clone(),
            desc: false,
        })
        .collect()
}
