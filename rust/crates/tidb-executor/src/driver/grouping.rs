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

//! `GROUP BY ... WITH ROLLUP` and the `GROUPING()` calls that read it, which
//! is Go's Expand operator seen from this tier.
//!
//! Go replicates each input row once per grouping set and aggregates the
//! result in one pass. This runs one aggregation pass per grouping-set prefix
//! `(g1..gk)`, `k = n..0`, over the materialized source instead -- logically
//! the same thing, and it makes the `GROUPING()` bitmask trivial, since the
//! pass already knows which columns it rolled up ([`GroupingSpec::mask_for`]).
//!
//! The two halves are one concern: a `GROUPING()` call is hoisted into an
//! aggregation output column at build time ([`add_grouping_column`]) and the
//! bitmask is written into that column at run time
//! ([`run_rollup_aggregate`]), so the spec recorded by one is exactly what the
//! other reads.

use super::*;
/// One `GROUPING(c1, ..., cn)` call hoisted into an aggregation output column.
///
/// Go computes `GROUPING` from the `gid` column Expand attaches to every
/// replicated row; this seed's rollup runs one aggregation pass per grouping
/// set, so the pass itself already knows which columns are rolled up and the
/// bitmask is filled straight into the output row.
#[derive(Clone, Debug)]
pub(crate) struct GroupingSpec {
    /// The aggregation output column this call's value is written into.
    out_index: usize,
    /// Each argument's position in the `GROUP BY` list, in argument order.
    /// The LEFTMOST argument owns the HIGHEST bit (captured from real TiDB:
    /// with `GROUP BY a, b WITH ROLLUP`, the `b`-only subtotal row reports
    /// `GROUPING(a,b) = 1` and `GROUPING(b,a) = 2`).
    group_positions: Vec<usize>,
}

impl GroupingSpec {
    /// The bitmask this call reports for a pass that groups by the first `k`
    /// `GROUP BY` expressions, i.e. one where positions `k..` are rolled up.
    fn mask_for_prefix(&self, k: usize) -> u64 {
        let width = self.group_positions.len();
        self.group_positions
            .iter()
            .enumerate()
            .filter(|(_, &position)| position >= k)
            .map(|(arg, _)| 1u64 << (width - 1 - arg))
            .sum()
    }
}

/// Go `resolveGroupingTraverseAction.Transform`: outside aggregate arguments,
/// replace a complete grouping expression first; otherwise descend through a
/// scalar function and replace the grouped columns it reads.
fn replace_grouping_expr(
    expression: &mut Expression,
    group_hashes: &[Vec<u8>],
    group_keys: &[Expression],
) {
    let mut hashed = expression.clone();
    if let Some(position) = group_hashes
        .iter()
        .position(|candidate| candidate.as_slice() == hashed.hash_code())
    {
        *expression = group_keys[position].clone();
        return;
    }
    if let Expression::ScalarFunction(function) = expression {
        for argument in &mut function.args {
            replace_grouping_expr(argument, group_hashes, group_keys);
        }
    }
}

/// The `GROUPING(...)` arguments when `expr` IS such a call, else `None`.
pub(crate) fn grouping_call_args(expr: &tidb_ast::Expr) -> Option<&[tidb_ast::Expr]> {
    match expr {
        tidb_ast::Expr::Func { name, args, .. } if name.eq_ignore_ascii_case("grouping") => {
            Some(args)
        }
        _ => None,
    }
}

/// Whether `expr` mentions `GROUPING()` anywhere the aggregate path can reach
/// it. The recursion covers the same shapes [`substitute_aggregates`] walks;
/// a `GROUPING` buried in a shape neither one descends into is not detected
/// and simply evaluates as an unknown function, as it does today.
pub(crate) fn expr_has_grouping(expr: &tidb_ast::Expr) -> bool {
    use tidb_ast::Expr;
    if grouping_call_args(expr).is_some() {
        return true;
    }
    match expr {
        Expr::Paren(inner) | Expr::Unary(_, inner) => expr_has_grouping(inner),
        Expr::Binary(_, lhs, rhs) => expr_has_grouping(lhs) || expr_has_grouping(rhs),
        Expr::Func { args, .. } => args.iter().any(expr_has_grouping),
        _ => false,
    }
}

/// Whether the statement writes `GROUPING()` in any clause the aggregate path
/// evaluates.
pub(crate) fn select_has_grouping(select: &tidb_ast::SelectStmt) -> bool {
    select.fields.fields().iter().any(|field| match field {
        SelectField::Expr { expr, .. } => expr_has_grouping(expr),
        SelectField::Wildcard { .. } => false,
    }) || select.having.as_ref().is_some_and(expr_has_grouping)
        || select
            .order_by
            .iter()
            .any(|item| expr_has_grouping(&item.expr))
}

/// The output type Go gives a `GROUPING()` column: `BIGINT UNSIGNED`, flen 20,
/// with the binary flag (captured from real TiDB: `tp=8 flag=160 flen=20`).
fn grouping_result_type() -> FieldType {
    let mut ftype = FieldType::new(FieldTypeCode::LongLong);
    ftype.add_flags(FieldTypeFlags::UNSIGNED | FieldTypeFlags::BINARY);
    ftype.set_flen(20);
    ftype
}

/// Resolves each `GROUPING()` argument to its position in the `GROUP BY` list.
///
/// Go rejects an argument that is not grouped with `ErrFieldInGroupingNotGroupBy`
/// (3602), naming the argument's 0-based position.
fn grouping_arg_positions(
    args: &[tidb_ast::Expr],
    group_by_exprs: &[String],
) -> Result<Vec<usize>, DriverError> {
    let mut positions = Vec::with_capacity(args.len());
    for (arg, expr) in args.iter().enumerate() {
        let expression = expr.restore();
        let position = group_by_exprs
            .iter()
            .position(|candidate| candidate.eq_ignore_ascii_case(&expression))
            .ok_or(DriverError::FieldInGroupingNotGroupBy(arg))?;
        positions.push(position);
    }
    Ok(positions)
}

/// Adds a `GROUPING()` call as an aggregation output column and returns that
/// column's name and INDEX -- the index matters because a repeated call text
/// reuses the existing column rather than adding one, so a caller that
/// reserved the next index for it must read the real one back.
///
/// The column is a placeholder as far as the aggregation is concerned -- a
/// `FIRST_ROW` over the constant `0`, so the column exists and every group
/// produces exactly one value -- and [`run_rollup_aggregate`] overwrites it
/// with the per-grouping-set bitmask. Repeating the same call text reuses the
/// column already added, as the aggregate path does for a repeated aggregate.
pub(crate) fn add_grouping_column(
    args: &[tidb_ast::Expr],
    display: String,
    agg_funcs: &mut Vec<AggFunc>,
    names: &mut Vec<String>,
    types: &mut Vec<FieldType>,
    grouping_specs: &mut Vec<GroupingSpec>,
    group_by_exprs: &[String],
) -> Result<(String, usize), DriverError> {
    if let Some(index) = names
        .iter()
        .position(|name| name.eq_ignore_ascii_case(&display))
    {
        if grouping_specs.iter().any(|spec| spec.out_index == index) {
            return Ok((display, index));
        }
    }
    let group_positions = grouping_arg_positions(args, group_by_exprs)?;
    let placeholder = Expression::Constant(tidb_expr::constant::Constant::new(
        Datum::Int(0),
        FieldType::new(FieldTypeCode::LongLong),
    ));
    agg_funcs.push(AggFunc {
        kind: AggKind::FirstRow,
        arg: Some(placeholder),
        extra_args: Vec::new(),
        distinct: false,
        order_by: Vec::new(),
        arg_orig_name: String::new(),
    });
    grouping_specs.push(GroupingSpec {
        out_index: names.len(),
        group_positions,
    });
    let index = names.len();
    names.push(display.clone());
    types.push(grouping_result_type());
    Ok((display, index))
}

/// Runs `GROUP BY g1..gn WITH ROLLUP` by materializing the source rows once
/// and aggregating every grouping-set prefix `(g1..gk)`, `k = n..0`, over
/// them -- logically what Go's Expand operator does by replicating each input
/// row once per grouping set. Like Go's projection below Expand, this first
/// appends one column per distinct grouping expression. Each pass NULLs only
/// those grouping-key copies; aggregate arguments continue reading the
/// original source columns. That distinction is observable when a column is
/// both grouped and aggregated: `SUM(b) ... GROUP BY b WITH ROLLUP` must keep
/// `b` in the grand total.
///
/// Row order: Go's hash aggregation over Expand output emits rollup rows in a
/// NONDETERMINISTIC order (verified against real TiDB -- the order changes
/// across runs), so only the row multiset is contractual and `ORDER BY` is the
/// only ordering guarantee. This tier emits full groups first (first-seen
/// order), then each shorter prefix's subtotals, then the grand total. An
/// empty source yields no rows at all -- not even the grand total -- because
/// Expand replicates zero rows (unlike a scalar aggregate).
pub(crate) fn run_rollup_aggregate(
    source: Box<dyn Executor>,
    group_by: &[Expression],
    agg_funcs: &[AggFunc],
    out_schema: &Schema,
    out_types: &[FieldType],
    grouping_specs: &[GroupingSpec],
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    // Materialize the source once; every prefix pass replays these rows.
    let mut source_schema = source.schema().clone();
    let source_types = source.ret_field_types().to_vec();
    let rows = drain_executor_rows(source, &source_types, &ctx.statement_memory())?;

    // Go `buildExpand` evaluates each distinct GROUP BY expression into a
    // fresh projection column before Expand. Restore duplicate group items as
    // repeated references to that one column, rather than evaluating a
    // side-effecting expression more than once per source row.
    let source_width = source_types.len();
    let mut distinct_hashes: Vec<Vec<u8>> = Vec::new();
    let mut distinct_group_by = Vec::new();
    let mut group_key_indices = Vec::with_capacity(group_by.len());
    let mut group_keys = Vec::with_capacity(group_by.len());
    let mut next_unique_id = source_schema
        .columns
        .iter()
        .map(|column| column.unique_id)
        .max()
        .unwrap_or(0);
    for expression in group_by {
        let mut hashed = expression.clone();
        let hash = hashed.hash_code().to_vec();
        let distinct_index = match distinct_hashes.iter().position(|seen| *seen == hash) {
            Some(index) => index,
            None => {
                let field_type = expression.static_type().cloned().ok_or_else(|| {
                    DriverError::Parse("GROUP BY expression has no result type".to_string())
                })?;
                next_unique_id = next_unique_id.checked_add(1).ok_or_else(|| {
                    DriverError::Parse("GROUP BY column identity overflow".to_string())
                })?;
                let index = distinct_group_by.len();
                let mut column = Column::new(next_unique_id, field_type.clone());
                column.index = i64::try_from(source_width + index).map_err(|_| {
                    DriverError::Parse("GROUP BY column index overflow".to_string())
                })?;
                source_schema.append([column]);
                distinct_hashes.push(hash);
                distinct_group_by.push(expression.clone());
                index
            }
        };
        let column = source_schema.columns[source_width + distinct_index].clone();
        group_key_indices.push(distinct_index);
        group_keys.push(Expression::Column(column));
    }

    let group_hashes: Vec<Vec<u8>> = group_key_indices
        .iter()
        .map(|index| distinct_hashes[*index].clone())
        .collect();
    let mut rollup_agg_funcs = agg_funcs.to_vec();
    for function in &mut rollup_agg_funcs {
        // Aggregate arguments keep reading original source columns. FIRST_ROW
        // represents projection expressions outside aggregates, which Go
        // rewrites onto Expand's grouping-key copies.
        if matches!(function.kind, AggKind::FirstRow) {
            if let Some(argument) = &mut function.arg {
                replace_grouping_expr(argument, &group_hashes, &group_keys);
            }
        }
    }

    let mut eval_chunk = tidb_chunk::chunk::Chunk::new_with_capacity(&source_types, rows.len());
    for row in &rows {
        for (column, value) in row.iter().enumerate() {
            eval_chunk.append_datum(column, value);
        }
    }
    let mut expanded_rows = Vec::with_capacity(rows.len());
    for (row_index, mut row) in rows.into_iter().enumerate() {
        let source_row = eval_chunk.get_row(row_index);
        for expression in &distinct_group_by {
            row.push(
                expression
                    .eval(ctx, source_row)
                    .map_err(|error| DriverError::Exec(ExecError::Eval(error)))?,
            );
        }
        expanded_rows.push(row);
    }
    let mut out_rows: Vec<Vec<Datum>> = Vec::new();
    if !expanded_rows.is_empty() {
        for k in (0..=group_keys.len()).rev() {
            let mut pass_rows = expanded_rows.clone();
            let mut kept = vec![false; distinct_group_by.len()];
            for &index in &group_key_indices[..k] {
                kept[index] = true;
            }
            for row in &mut pass_rows {
                for (index, keep) in kept.iter().enumerate() {
                    if !keep {
                        row[source_width + index] = Datum::Null;
                    }
                }
            }
            let pass_source = Box::new(MemTableSourceExec::new(
                ExecutorMeta::new(source_schema.clone(), 1, INIT_CAP, MAX_CHUNK_SIZE),
                pass_rows,
            ));
            let agg = HashAggExec::new(
                ExecutorMeta::new(out_schema.clone(), 2, INIT_CAP, MAX_CHUNK_SIZE),
                group_keys[..k].to_vec(),
                rollup_agg_funcs.clone(),
                pass_source,
                ctx.clone(),
                ctx.statement_memory(),
            );
            // This pass rolls up positions `k..`, which IS the grouping bit
            // each GROUPING() call reports -- the one thing that distinguishes
            // a subtotal's NULL from a data NULL.
            let mut pass_out =
                drain_executor_rows(Box::new(agg), out_types, &ctx.statement_memory())?;
            for spec in grouping_specs {
                let mask = Datum::UInt(spec.mask_for_prefix(k));
                for row in &mut pass_out {
                    row[spec.out_index] = mask.clone();
                }
            }
            out_rows.extend(pass_out);
        }
    }
    Ok(Box::new(MemTableSourceExec::new(
        ExecutorMeta::new(out_schema.clone(), 2, INIT_CAP, MAX_CHUNK_SIZE),
        out_rows,
    )))
}
