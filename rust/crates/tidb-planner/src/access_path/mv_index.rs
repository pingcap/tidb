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

//! The multi-valued index half of Go `pkg/planner/core/indexmerge_path.go`.
//!
//! A multi-valued (MV) index files one entry per array element, so a scan of
//! it can return one row several times and misses every row whose array is
//! empty. Go therefore reads it only through an IndexMerge whose partial
//! paths each scan ONE element value: `v MEMBER OF (a)` is one partial,
//! `json_overlaps(a, '[1,2]')` a union of two, `json_contains(a, '[1,2]')` an
//! intersection of two. These are the function boundaries Go draws, under
//! their Go names.
//!
//! One representation difference: Go's expression tree carries the implicit
//! `cast(x, json BINARY)` that `newBaseBuiltinFuncWithTp` wraps around a JSON
//! function's non-JSON argument, and `unwrapJSONCast` strips it again. This
//! port's tree holds the argument itself, so [`unwrap_json_argument`] reads a
//! non-JSON argument as that cast's operand, and [`json_array_expr_to_exprs`]
//! builds the cast Go would have built before evaluating it.

use std::collections::{BTreeMap, HashMap};

use super::index_merge::{
    enumerated_index_mask, estimate_partial_index_ranges, index_merge_hint_allows,
    IndexMergePartial, IndexMergePath,
};
use super::{AccessPathDerivationContext, IndexPathState};
use crate::logical::DataSource;
use crate::plan_base::PlanError;
use tidb_datatype::{Datum, EvalType, FieldType, FieldTypeCode};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;

/// Go's access-filter types (`unspecifiedFilterTp` is the absent value).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AccessFilterType {
    /// Go `eqOrInOnNonMVColTp`.
    EqOrInOnNonMvCol,
    /// Go `multiValuesOROnMVColTp`: `json_overlaps` with several values.
    MultiValuesOrOnMvCol,
    /// Go `multiValuesANDOnMVColTp`: `json_contains` with several values.
    MultiValuesAndOnMvCol,
    /// Go `singleValueOnMVColTp`.
    SingleValueOnMvCol,
}

/// Go `PrepareIdxColsAndUnwrapArrayType`: the index's columns from the
/// table's `TblColsByID`, an ARRAY column replaced by a clone typed as its
/// element (with the binary collation). `check_only_1_array` refuses an
/// index without exactly one ARRAY column.
pub(crate) fn prepare_idx_cols_and_unwrap_array_type(
    ds: &DataSource,
    index: &crate::plan_builder::catalog::SourceIndex,
    check_only_1_array: bool,
) -> Option<Vec<Column>> {
    let mut array_columns = 0;
    let mut columns = Vec::with_capacity(index.columns.len());
    for key in &index.columns {
        let column = ds.table_columns.get(key.offset)?.clone();
        if column.get_static_type().is_some_and(FieldType::is_array) {
            array_columns += 1;
        }
        columns.push(unwrap_array_column(column));
    }
    if check_only_1_array && array_columns != 1 {
        return None;
    }
    Some(columns)
}

/// `PrepareIdxColsAndUnwrapArrayType`'s per-column step: an ARRAY column
/// becomes a clone typed as its element, with the binary collation (`JSON-
/// ARRAY(INT) --> INT`); any other column is returned as is. A partial scan
/// over an MV index reads and decodes its keys with this type.
pub(crate) fn unwrap_array_column(mut column: Column) -> Column {
    if let Some(mut element) = column
        .get_static_type()
        .filter(|field| field.is_array())
        .map(FieldType::array_type)
    {
        element.set_charset_name("binary");
        element.set_collation_name("binary");
        column.ret_type = Some(element);
    }
    column
}

/// Go `unwrapJSONCast`: the operand of a cast with a JSON result. Go names
/// every cast `cast`; this port names the node by its target, and
/// `CAST(... AS ... ARRAY)` (a JSON-typed array) is `cast_array`.
fn unwrap_json_cast(expr: &Expression) -> Option<&Expression> {
    let Expression::ScalarFunction(function) = expr else {
        return None;
    };
    if !matches!(function.func_name.lowercase(), "cast_json" | "cast_array")
        || function.get_static_type()?.eval_type() != EvalType::Json
    {
        return None;
    }
    function.args.first()
}

/// Go `unwrapJSONCast` over a JSON function's argument. Go wraps a non-JSON
/// argument (or a JSON one marked parse-to-JSON) in the implicit cast that
/// `unwrapJSONCast` then strips; that argument is therefore the value itself.
fn unwrap_json_argument(expr: &Expression) -> Option<&Expression> {
    let wrapped_by_go = expr.static_type().is_none_or(|field| {
        field.code() != FieldTypeCode::Json
            || field.has_flag(tidb_datatype::FieldTypeFlags::PARSE_TO_JSON)
    });
    if wrapped_by_go {
        return Some(expr);
    }
    unwrap_json_cast(expr)
}

/// Go `jsonArrayExpr2Exprs`: `cast('[1, 2, 3]' as JSON)` as the constants
/// `1, 2, 3` of `target_type`; a scalar is one constant. `None` when the
/// argument is not an immutable JSON value or an element does not convert.
fn json_array_expr_to_exprs(
    ctx: &AccessPathDerivationContext<'_>,
    expr: &Expression,
    target_type: &FieldType,
) -> Option<Vec<Expression>> {
    // The argument as Go's tree holds it: wrapped in its implicit JSON cast.
    let wrapped = tidb_expr::aggregation::wrap_cast::wrap_with_cast_as_json(expr.clone()).ok()?;
    if !tidb_expr::expr_util::predicates::is_immutable_func(&wrapped)
        || wrapped.static_type()?.eval_type() != EvalType::Json
    {
        return None;
    }
    let Ok(Datum::Json(json)) = (ctx.expression_evaluator)(&wrapped) else {
        return None;
    };
    if json.type_code() != tidb_datatype::JSON_TYPE_CODE_ARRAY {
        return json_value_to_expr(&json, target_type).map(|single| vec![single]);
    }
    let count = json.element_count().ok()?;
    (0..count)
        .map(|position| json_value_to_expr(&json.array_get(position).ok()??, target_type))
        .collect()
}

/// Go `jsonValue2Expr`.
fn json_value_to_expr(
    value: &tidb_datatype::BinaryJSON,
    target_type: &FieldType,
) -> Option<Expression> {
    let datum = tidb_expr::convert_json_to_type(value, target_type).ok()?;
    Some(Expression::Constant(tidb_expr::constant::Constant::new(
        datum,
        target_type.clone(),
    )))
}

/// Go `isSafeTypeConversion4MVIndexRange`: no conversion is trusted when
/// building ranges over an MV index ("converting '1' to 1 to access INT
/// MVIndex may cause some wrong result").
fn is_safe_type_conversion_for_mv_index_range(value: &Expression, index_type: &FieldType) -> bool {
    value
        .static_type()
        .is_some_and(|field| field.eval_type() == index_type.eval_type())
}

/// The JSON document a `json_contains`/`json_overlaps`/`json_memberof` call
/// on the MV column reads, and the values it tests, with Go's filter type.
fn mv_column_values(
    ctx: &AccessPathDerivationContext<'_>,
    function: &tidb_expr::scalar_function::ScalarFunction,
    target_json_path: &Expression,
    json_type: &FieldType,
) -> Option<(Vec<Expression>, AccessFilterType)> {
    match function.func_name.lowercase() {
        // (1 member of a)
        "json_memberof" => {
            if !target_json_path.equal(function.args.get(1)?) {
                return None;
            }
            let value = unwrap_json_argument(function.args.first()?)?;
            Some((vec![value.clone()], AccessFilterType::SingleValueOnMvCol))
        }
        // json_contains(a, '1')
        "json_contains" => {
            if !target_json_path.equal(function.args.first()?) {
                return None;
            }
            let values = json_array_expr_to_exprs(ctx, function.args.get(1)?, json_type)?;
            (!values.is_empty()).then_some((values, AccessFilterType::MultiValuesAndOnMvCol))
        }
        // json_overlaps(a, '1') or json_overlaps('1', a)
        "json_overlaps" => {
            let other = if function.args.first()?.equal(target_json_path) {
                function.args.get(1)?
            } else if function.args.get(1)?.equal(target_json_path) {
                function.args.first()?
            } else {
                return None;
            };
            let values = json_array_expr_to_exprs(ctx, other, json_type)?;
            // Forbid an empty array for safety.
            (!values.is_empty()).then_some((values, AccessFilterType::MultiValuesOrOnMvCol))
        }
        _ => None,
    }
}

/// Go `checkAccessFilter4IdxCol`: whether `filter` can access `idx_col`, and
/// how.
pub(crate) fn check_access_filter_for_idx_col(
    ctx: &AccessPathDerivationContext<'_>,
    filter: &Expression,
    idx_col: &Column,
) -> Option<AccessFilterType> {
    let Expression::ScalarFunction(function) = filter else {
        return None;
    };
    // The virtual column on the MV index.
    if let Some(virtual_expr) = idx_col.virtual_expr.as_deref() {
        let target_json_path = unwrap_json_cast(virtual_expr)?;
        let json_type = idx_col.get_static_type()?.array_type();
        let (values, filter_type) = mv_column_values(ctx, function, target_json_path, &json_type)?;
        let index_type = idx_col.get_static_type()?;
        if !values
            .iter()
            .all(|value| is_safe_type_conversion_for_mv_index_range(value, index_type))
        {
            return None;
        }
        // A one-value json_contains/json_overlaps is `1 member of (a)`.
        return Some(
            if matches!(
                filter_type,
                AccessFilterType::MultiValuesOrOnMvCol | AccessFilterType::MultiValuesAndOnMvCol
            ) && values.len() == 1
            {
                AccessFilterType::SingleValueOnMvCol
            } else {
                filter_type
            },
        );
    }
    let is_idx_col = |expr: &Expression| {
        matches!(expr, Expression::Column(column) if column.unique_id == idx_col.unique_id)
    };
    match function.func_name.lowercase() {
        "in" => {
            let [first, rest @ ..] = function.args.as_slice() else {
                return None;
            };
            (!rest.is_empty()
                && is_idx_col(first)
                && rest.iter().all(|arg| matches!(arg, Expression::Constant(_))))
            .then_some(AccessFilterType::EqOrInOnNonMvCol)
        }
        "eq" => {
            let [left, right] = function.args.as_slice() else {
                return None;
            };
            let column = match (left, right) {
                (Expression::Column(_), Expression::Constant(_)) => left,
                (Expression::Constant(_), Expression::Column(_)) => right,
                _ => return None,
            };
            is_idx_col(column).then_some(AccessFilterType::EqOrInOnNonMvCol)
        }
        _ => None,
    }
}

fn is_mv_virtual_column(column: &Column) -> bool {
    column
        .virtual_expr
        .as_deref()
        .and_then(Expression::static_type)
        .is_some_and(FieldType::is_array)
}

/// Go `collectFilters4MVIndex`: for `idx(x, cast(a as array), z)`, the
/// leading run of index columns each matched by one filter becomes the
/// access filters; the rest remain.
pub(crate) fn collect_filters_for_mv_index(
    ctx: &AccessPathDerivationContext<'_>,
    filters: &[Expression],
    idx_cols: &[Column],
) -> (Vec<Expression>, Vec<Expression>, Option<AccessFilterType>) {
    let mut access_type = None;
    let mut used_as_access = vec![false; filters.len()];
    let mut access_filters = Vec::new();
    for column in idx_cols {
        let mut found = false;
        for (position, filter) in filters.iter().enumerate() {
            if used_as_access[position] {
                continue;
            }
            if let Some(filter_type) = check_access_filter_for_idx_col(ctx, filter, column) {
                access_filters.push(filter.clone());
                used_as_access[position] = true;
                found = true;
                // An access filter on the MV column overrides a normal one.
                if access_type.is_none() || access_type == Some(AccessFilterType::EqOrInOnNonMvCol) {
                    access_type = Some(filter_type);
                }
                break;
            }
        }
        if !found {
            break;
        }
    }
    let remaining = filters
        .iter()
        .zip(&used_as_access)
        .filter(|(_, used)| !**used)
        .map(|(filter, _)| filter.clone())
        .collect();
    (access_filters, remaining, access_type)
}

/// Go `CollectFilters4MVIndexMutations`: like
/// [`collect_filters_for_mv_index`], but every filter on the MV column is a
/// separate mutation of its access slot (`(2 member of a) and (1 member of
/// a)` can only be combined at run time by intersecting handles).
fn collect_filters_for_mv_index_mutations(
    ctx: &AccessPathDerivationContext<'_>,
    filters: &[Expression],
    idx_cols: &[Column],
) -> (Vec<Expression>, Option<usize>, Vec<Expression>) {
    let mut used_as_access = vec![false; filters.len()];
    let mut mutations = Vec::new();
    let mut mv_col_offset = None;
    let mut access_filters = Vec::new();
    for (offset, column) in idx_cols.iter().enumerate() {
        let mut found = false;
        for (position, filter) in filters.iter().enumerate() {
            if used_as_access[position] {
                continue;
            }
            if check_access_filter_for_idx_col(ctx, filter, column).is_none() {
                continue;
            }
            if is_mv_virtual_column(column) {
                mutations.push(filter.clone());
                if mv_col_offset.is_none() {
                    mv_col_offset = Some(offset);
                    access_filters.push(filter.clone());
                }
            } else {
                // Go keeps collecting (no break) so every MV mutation is seen.
                access_filters.push(filter.clone());
            }
            used_as_access[position] = true;
            found = true;
        }
        if !found {
            break;
        }
    }
    (access_filters, mv_col_offset, mutations)
}

/// Go `buildPartialPath4MVIndex`: one partial path over the MV index whose
/// access filters must ALL become access conditions.
fn build_partial_path_for_mv_index(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
    access_filters: &[Expression],
    idx_cols: &[Column],
    index_pos: usize,
) -> Result<Option<IndexMergePartial>, PlanError> {
    let index = &ds.indexes[index_pos];
    let columns = idx_cols
        .iter()
        .zip(&index.columns)
        .map(|(column, key)| {
            // A full-length prefix is no prefix, as in IndexInfo2Cols.
            let length = if column.get_static_type().is_some_and(|field| field.flen() == key.length)
            {
                tidb_datatype::UNSPECIFIED_LENGTH
            } else {
                key.length
            };
            (column.clone(), length)
        })
        .collect::<Vec<_>>();
    let (cols, lengths): (Vec<_>, Vec<_>) = columns.iter().cloned().unzip();
    let detached = ctx
        .detach_index_range(access_filters, &cols, &lengths)
        .map_err(|error| match error {
            crate::ranger::points::PointBuilderError::Eval(error) => error.into(),
            crate::ranger::points::PointBuilderError::Value(error) => {
                PlanError::internal_coded(error.to_string())
            }
            crate::ranger::points::PointBuilderError::Unsupported(message) => {
                PlanError::unsupported_type(message)
            }
        })?;
    // Not all filters are used in this case.
    if detached.access_conds.len() != access_filters.len() || !detached.remained_conds.is_empty() {
        return Ok(None);
    }
    let filled = super::ordinary::FilledIndexPath {
        full_columns: columns.iter().cloned().map(Some).collect(),
        columns,
        detached,
        index_filters: Vec::new(),
        table_filters: Vec::new(),
        // Go never sets CountAfterIndex on an MV partial path.
        count_after_index: Some(0.0),
        correlated_access_count: 0,
    };
    let rows = estimate_partial_index_ranges(ds, index, &filled, ctx)?;
    Ok(Some(IndexMergePartial {
        index_pos,
        path: IndexPathState {
            filled: Some(filled),
            declared_columns: None,
            row_estimate: Some(crate::cardinality::row_count_column::RowEstimate::new(
                rows, rows, rows,
            )),
            is_single_scan: Some(false),
        },
    }))
}

/// Go `buildPartialPaths4MVIndex`: one partial path per value the MV
/// column's access filter tests, and whether they intersect
/// (`json_contains`) rather than unite.
fn build_partial_paths_for_mv_index(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
    access_filters: &[Expression],
    idx_cols: &[Column],
    index_pos: usize,
    use_plan_cache: bool,
    noncacheable: &mut Option<String>,
) -> Result<Option<(Vec<IndexMergePartial>, bool)>, PlanError> {
    let Some(vir_col_id) = idx_cols.iter().position(is_mv_virtual_column) else {
        return Ok(None);
    };
    // Without a filter on the virtual column a scan would lose every row
    // whose array is empty.
    let Some(Expression::ScalarFunction(function)) = access_filters.get(vir_col_id) else {
        return Ok(None);
    };
    let vir_col = &idx_cols[vir_col_id];
    let Some(json_type) = vir_col.get_static_type().map(FieldType::array_type) else {
        return Ok(None);
    };
    let Some(target_json_path) = vir_col.virtual_expr.as_deref().and_then(unwrap_json_cast)
    else {
        return Ok(None);
    };
    // Go `jsonArrayExpr2Exprs(..., checkForSkipPlanCache = true)`: the plan
    // depends on the array's VALUES, so it must not be reused for others.
    if matches!(function.func_name.lowercase(), "json_contains" | "json_overlaps")
        && tidb_expr::expr_util::predicates::maybe_over_optimized_4_plan_cache(
            use_plan_cache,
            &function.args,
        )
    {
        *noncacheable = Some(format!(
            "{} function with immutable parameters can affect index selection",
            function.func_name.lowercase()
        ));
    }
    let Some((values, filter_type)) = mv_column_values(ctx, function, target_json_path, &json_type)
    else {
        return Ok(None);
    };
    let is_intersection = filter_type == AccessFilterType::MultiValuesAndOnMvCol;
    let Some(index_type) = vir_col.get_static_type() else {
        return Ok(None);
    };
    if !values
        .iter()
        .all(|value| is_safe_type_conversion_for_mv_index_range(value, index_type))
    {
        return Ok(None);
    }
    let mut partials = Vec::with_capacity(values.len());
    for value in values {
        // Rewrite the JSON function to EQ to calculate the range:
        // `(1 member of j)` -> `j = 1`.
        let eq = tidb_expr::new_function::new_function(
            &tidb_expr::NoColumns,
            "eq",
            FieldType::new(FieldTypeCode::Tiny),
            vec![Expression::Column(vir_col.clone()), value],
        )?;
        let mut access = access_filters.to_vec();
        access[vir_col_id] = eq;
        let Some(partial) = build_partial_path_for_mv_index(ds, ctx, &access, idx_cols, index_pos)?
        else {
            return Ok(None);
        };
        partials.push(partial);
    }
    Ok(Some((partials, is_intersection)))
}

/// Go `cardinality.CalcTotalSelectivityForMVIdxPath`. Every partial these
/// generators build reads the virtual column (or is an ordinary index), so
/// each selectivity is over the table's realtime count -- Go's "case 1".
fn calc_total_selectivity_for_mv_idx_path(
    ds: &DataSource,
    partials: &[IndexMergePartial],
    is_intersection: bool,
) -> f64 {
    let realtime_count = ds.table_stats.as_ref().map_or(0.0, |stats| stats.row_count());
    let selectivities = partials.iter().map(|partial| {
        let rows = partial.path.count_after_access().unwrap_or(0.0);
        if realtime_count > 0.0 {
            (rows / realtime_count).clamp(0.0, 1.0)
        } else {
            0.0
        }
    });
    if is_intersection {
        selectivities.product()
    } else {
        selectivities.fold(0.0, |total, sel| (sel + total) - total * sel)
    }
}

/// Go `buildPartialPathUp4MVIndex`.
fn build_partial_path_up_for_mv_index(
    ds: &DataSource,
    partials: Vec<IndexMergePartial>,
    is_intersection: bool,
    table_filters: Vec<Expression>,
    noncacheable_reason: Option<String>,
) -> IndexMergePath {
    let realtime_count = ds.table_stats.as_ref().map_or(0.0, |stats| stats.row_count());
    let count_after_access =
        realtime_count * calc_total_selectivity_for_mv_idx_path(ds, &partials, is_intersection);
    IndexMergePath {
        partials,
        count_after_access,
        table_filters,
        is_intersection,
        access_mv_index: true,
        noncacheable_reason,
    }
}

/// Go `splitIndexFilterConditions` over an MV index's columns.
fn split_index_filter_conditions(
    ds: &DataSource,
    index_pos: usize,
    conditions: Vec<Expression>,
    ctx: &AccessPathDerivationContext<'_>,
) -> (Vec<Expression>, Vec<Expression>) {
    conditions.into_iter().partition(|condition| {
        crate::logical::data_source::index_covers_condition(
            ds,
            &ds.indexes[index_pos],
            condition,
            ctx.opt_prefix_index_single_scan,
        )
    })
}

/// The enumerated MV indexes an IndexMerge may read, in path order.
fn possible_mv_indexes(ds: &DataSource) -> Vec<usize> {
    let enumerated = enumerated_index_mask(ds);
    ds.indexes
        .iter()
        .enumerate()
        .filter(|(position, index)| {
            enumerated[*position]
                && index.is_multi_valued
                && index_merge_hint_allows(ds, &index.name)
        })
        .map(|(position, _)| position)
        .collect()
}

/// Go `generateANDIndexMerge4MVIndex`: an IndexMerge over ONE MV index for
/// `json_memberof` / `json_overlaps` / `json_contains` filters.
///
/// ```text
/// select * from t where json_contains(a, '[1, 2, 3]')
///   IndexMerge(AND)
///     IndexRangeScan(a, [1,1])
///     IndexRangeScan(a, [2,2])
///     IndexRangeScan(a, [3,3])
///     TableRowIdScan(t)
/// ```
pub(crate) fn generate_and_index_merge_for_mv_index(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
    filters: &[Expression],
    use_plan_cache: bool,
) -> Result<Vec<IndexMergePath>, PlanError> {
    let mut paths = Vec::new();
    for index_pos in possible_mv_indexes(ds) {
        let Some(idx_cols) = prepare_idx_cols_and_unwrap_array_type(ds, &ds.indexes[index_pos], true)
        else {
            continue;
        };
        let (access_filters, remaining, _) = collect_filters_for_mv_index(ctx, filters, &idx_cols);
        if access_filters.is_empty() {
            continue;
        }
        let mut noncacheable = None;
        let Some((mut partials, is_intersection)) = build_partial_paths_for_mv_index(
            ds,
            ctx,
            &access_filters,
            &idx_cols,
            index_pos,
            use_plan_cache,
            &mut noncacheable,
        )?
        else {
            continue;
        };
        // All partials read the same index, so the first one's columns
        // classify the remaining filters; a union needs the index filters on
        // every partial for correctness.
        let (index_filters, table_filters) =
            split_index_filter_conditions(ds, index_pos, remaining, ctx);
        for partial in &mut partials {
            if let Some(filled) = partial.path.filled.as_mut() {
                filled.index_filters.extend(index_filters.iter().cloned());
            }
        }
        paths.push(build_partial_path_up_for_mv_index(
            ds,
            partials,
            is_intersection,
            table_filters,
            noncacheable,
        ));
    }
    Ok(paths)
}

/// Go `generateMVIndexMergePartialPaths4And`: the partial paths of every MV
/// index whose filters can join an intersection, with the access conditions
/// they consumed. Two MV indexes whose access filters are the same keep only
/// the one with the lower count.
fn generate_mv_index_merge_partial_paths_for_and(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
    filters: &[Expression],
    use_plan_cache: bool,
    noncacheable: &mut Option<String>,
) -> Result<(Vec<IndexMergePartial>, HashMap<Vec<u8>, Expression>), PlanError> {
    struct Record {
        origin_offset: usize,
        paths: Vec<IndexMergePartial>,
        count_after_access: f64,
    }
    let mut used_access_conds = HashMap::new();
    let mut by_access_filters: BTreeMap<Vec<u8>, Record> = BTreeMap::new();
    for (origin_offset, index_pos) in possible_mv_indexes(ds).into_iter().enumerate() {
        let Some(idx_cols) = prepare_idx_cols_and_unwrap_array_type(ds, &ds.indexes[index_pos], true)
        else {
            continue;
        };
        let (mut access_filters, mv_col_offset, mutations) =
            collect_filters_for_mv_index_mutations(ctx, filters, &idx_cols);
        if access_filters.is_empty() {
            continue;
        }
        // Every hash code, before the access filters are mutated.
        let mut all_hash_codes = access_filters
            .iter()
            .map(Expression::canonical_hash_code)
            .chain(mutations.iter().skip(1).map(Expression::canonical_hash_code))
            .collect::<Vec<_>>();
        let mut paths_for_index = Vec::new();
        for mutation in &mutations {
            if let Some(offset) = mv_col_offset {
                access_filters[offset] = mutation.clone();
            }
            let Some((partials, is_intersection)) = build_partial_paths_for_mv_index(
                ds,
                ctx,
                &access_filters,
                &idx_cols,
                index_pos,
                use_plan_cache,
                noncacheable,
            )?
            else {
                continue;
            };
            // One partial, or an intersection, merges into the outer
            // intersection: And(p1, p2, Or(p3)) => And(p1, p2, p3).
            if partials.len() == 1 || is_intersection {
                for filter in &access_filters {
                    used_access_conds.insert(filter.canonical_hash_code(), filter.clone());
                }
                paths_for_index.extend(partials);
            }
        }
        all_hash_codes.sort();
        let key = all_hash_codes.concat();
        let count_after_access = ds.table_stats.as_ref().map_or(0.0, |stats| stats.row_count())
            * calc_total_selectivity_for_mv_idx_path(ds, &paths_for_index, true);
        let replace = by_access_filters
            .get(&key)
            .is_none_or(|record| record.count_after_access > count_after_access);
        if replace {
            by_access_filters.insert(
                key,
                Record {
                    origin_offset,
                    paths: paths_for_index,
                    count_after_access,
                },
            );
        }
    }
    let mut records = by_access_filters.into_values().collect::<Vec<_>>();
    records.sort_by_key(|record| record.origin_offset);
    Ok((
        records.into_iter().flat_map(|record| record.paths).collect(),
        used_access_conds,
    ))
}

/// Go `generateANDIndexMerge4ComposedIndex`: one intersection over several
/// MV indexes, or MV and ordinary indexes together.
///
/// ```text
/// IndexMerge(AND-INTERSECTION)
///   IndexRangeScan(mv-index-a)(1)
///   IndexRangeScan(mv-index-a)(2)
///   IndexRangeScan(non-mv-index-if-any)(?)
///   TableRowIdScan(t)
/// ```
pub(crate) fn generate_and_index_merge_for_composed_index(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
    filters: &[Expression],
    use_plan_cache: bool,
) -> Result<Option<IndexMergePath>, PlanError> {
    if filters.is_empty() || ds.enumerated_paths.len() <= 1 {
        return Ok(None);
    }
    // At least one hinted MV index path.
    let enumerated = enumerated_index_mask(ds);
    let has_mv = ds.indexes.iter().enumerate().any(|(position, index)| {
        enumerated[position] && index.is_multi_valued && index_merge_hint_allows(ds, &index.name)
    });
    if !has_mv {
        return Ok(None);
    }
    let mut noncacheable = None;
    let (mv_partials, mut used_access) =
        generate_mv_index_merge_partial_paths_for_and(ds, ctx, filters, use_plan_cache, &mut noncacheable)?;
    if mv_partials.is_empty() {
        return Ok(None);
    }
    let normal_partials =
        super::index_merge::generate_normal_index_partial_paths_for_and(ds, ctx, &mut used_access);
    // A multi-normal-index merge was handled before; this is a multi-MV or a
    // mixed one.
    let composed =
        mv_partials.len() > 1 || (mv_partials.len() == 1 && !normal_partials.is_empty());
    if !composed {
        return Ok(None);
    }
    let mut combined = normal_partials;
    combined.extend(mv_partials);
    let remained = filters
        .iter()
        .filter(|filter| !used_access.contains_key(&filter.canonical_hash_code()))
        .cloned()
        .collect::<Vec<_>>();
    // Index filters for each path; an intersection needs a filter on only
    // one path, so a filter some path evaluates leaves the table filters.
    let mut in_index_filter = std::collections::HashSet::new();
    for partial in &mut combined {
        let (index_filters, _) =
            split_index_filter_conditions(ds, partial.index_pos, remained.clone(), ctx);
        for filter in &index_filters {
            in_index_filter.insert(filter.canonical_hash_code());
        }
        if let Some(filled) = partial.path.filled.as_mut() {
            filled.index_filters.extend(index_filters);
        }
    }
    let table_filters = remained
        .into_iter()
        .filter(|filter| !in_index_filter.contains(&filter.canonical_hash_code()))
        .collect();
    Ok(Some(build_partial_path_up_for_mv_index(
        ds,
        combined,
        true,
        table_filters,
        noncacheable,
    )))
}

/// Go `cleanAccessPathForMVIndexHint`: once a forced MV index has an
/// IndexMerge path, every path not reading it is dropped.
pub(crate) fn clean_access_path_for_mv_index_hint(
    ds: &DataSource,
    paths: &mut Vec<super::DerivedAccessPath>,
) {
    let forced: std::collections::BTreeSet<usize> = ds
        .indexes
        .iter()
        .enumerate()
        .filter(|(_, index)| index.is_multi_valued && ds.forced_index_ids.contains(&index.id))
        .map(|(position, _)| position)
        .collect();
    if forced.is_empty() {
        return;
    }
    let reads_forced = |path: &super::DerivedAccessPath| match path {
        super::DerivedAccessPath::IndexMerge(path) => path
            .partials
            .iter()
            .any(|partial| forced.contains(&partial.index_pos)),
        _ => false,
    };
    if paths.iter().any(reads_forced) {
        paths.retain(reads_forced);
    }
}

/// Go `unfinishedAccessPath` for one MV index candidate of one OR branch.
pub(crate) struct UnfinishedMvPath {
    usable_filters: Vec<Expression>,
    need_keep_filter: bool,
}

/// Go `initUnfinishedPathsFromExpr` for an MV index candidate: case 2
/// (`collectFilters4MVIndex` yields an OR or single-value access) or case 3
/// (one usable filter per index column, gathered column by column). `None`
/// when no filter is usable.
pub(crate) fn init_unfinished_mv_path(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
    index_pos: usize,
    expr: &Expression,
) -> Option<UnfinishedMvPath> {
    let idx_cols = prepare_idx_cols_and_unwrap_array_type(ds, &ds.indexes[index_pos], false)?;
    let cnf_items = tidb_expr::expr_util::normal_form::split_cnf_items(expr);
    let unpushable = cnf_items.iter().any(|item| {
        !crate::pushdown::can_exprs_push_down(
            std::slice::from_ref(item),
            tidb_expr::infer_pushdown::PushDownStore::TiKv,
            ctx.expr_pushdown_blacklist,
        )
    });
    // Case 2: the previous logic, which must build a valid range at once.
    let (access, remaining, access_type) = collect_filters_for_mv_index(ctx, &cnf_items, &idx_cols);
    if !access.is_empty()
        && matches!(
            access_type,
            Some(AccessFilterType::MultiValuesOrOnMvCol | AccessFilterType::SingleValueOnMvCol)
        )
    {
        return Some(UnfinishedMvPath {
            usable_filters: access,
            need_keep_filter: !remaining.is_empty(),
        });
    }
    // Case 3: collect access filters column by column; only the OR list's
    // union semantics is handled here, so json_contains is excluded.
    let mut has_usable_filter = false;
    let mut collected = vec![false; cnf_items.len()];
    let mut usable_filters = Vec::new();
    for column in &idx_cols {
        for (position, item) in cnf_items.iter().enumerate() {
            if collected[position] {
                continue;
            }
            if matches!(
                check_access_filter_for_idx_col(ctx, item, column),
                Some(
                    AccessFilterType::EqOrInOnNonMvCol
                        | AccessFilterType::MultiValuesOrOnMvCol
                        | AccessFilterType::SingleValueOnMvCol
                )
            ) {
                usable_filters.push(item.clone());
                has_usable_filter = true;
                collected[position] = true;
                break;
            }
        }
    }
    has_usable_filter.then(|| UnfinishedMvPath {
        usable_filters,
        need_keep_filter: unpushable || collected.contains(&false),
    })
}

impl UnfinishedMvPath {
    /// Go `mergeANDItemIntoUnfinishedIndexMergePath`: a top-level AND item's
    /// usable filters for the same candidate join this branch's.
    pub(crate) fn merge_and_item(&mut self, item: UnfinishedMvPath) {
        self.usable_filters.extend(item.usable_filters);
    }
}

/// Go `buildIntoAccessPath`'s MV arm: the alternative's partial paths (one
/// per value, uniting) and whether the OR source filter must be kept.
pub(crate) fn build_mv_alternative(
    ds: &DataSource,
    ctx: &AccessPathDerivationContext<'_>,
    index_pos: usize,
    unfinished: &UnfinishedMvPath,
    use_plan_cache: bool,
) -> Result<Option<(Vec<IndexMergePartial>, bool)>, PlanError> {
    let Some(idx_cols) = prepare_idx_cols_and_unwrap_array_type(ds, &ds.indexes[index_pos], true)
    else {
        return Ok(None);
    };
    let (access, remaining, _) =
        collect_filters_for_mv_index(ctx, &unfinished.usable_filters, &idx_cols);
    if access.is_empty() {
        return Ok(None);
    }
    let mut noncacheable = None;
    let Some((partials, is_intersection)) = build_partial_paths_for_mv_index(
        ds,
        ctx,
        &access,
        &idx_cols,
        index_pos,
        use_plan_cache,
        &mut noncacheable,
    )?
    else {
        return Ok(None);
    };
    if is_intersection && partials.len() > 1 {
        return Ok(None);
    }
    Ok(Some((
        partials,
        !remaining.is_empty() || unfinished.need_keep_filter,
    )))
}

/// Go `CalcTotalSelectivityForMVIdxPath(..., isIntersection = false)` over
/// the decided partial paths of an OR IndexMerge, as row counts.
pub(crate) fn union_selectivity(ds: &DataSource, rows: impl Iterator<Item = f64>) -> f64 {
    let realtime_count = ds.table_stats.as_ref().map_or(0.0, |stats| stats.row_count());
    rows.map(|rows| {
        if realtime_count > 0.0 {
            (rows / realtime_count).clamp(0.0, 1.0)
        } else {
            0.0
        }
    })
    .fold(0.0, |total, sel| (sel + total) - total * sel)
}
