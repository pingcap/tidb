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

/// Output columns whose values the rollup pass fills after aggregation.
pub(crate) struct RollupOutputMetadata<'a> {
    pub grouping_specs: &'a [GroupingSpec],
    pub group_carriers: &'a [(usize, usize)],
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
    group_by_names: &[String],
) -> Result<Vec<usize>, DriverError> {
    let mut positions = Vec::with_capacity(args.len());
    for (arg, expr) in args.iter().enumerate() {
        let tidb_ast::Expr::Column(path) = expr else {
            return Err(DriverError::FieldInGroupingNotGroupBy(arg));
        };
        let name = path.last().cloned().unwrap_or_default();
        let position = group_by_names
            .iter()
            .position(|candidate| candidate.eq_ignore_ascii_case(&name))
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
    group_by_names: &[String],
) -> Result<(String, usize), DriverError> {
    if let Some(index) = names
        .iter()
        .position(|name| name.eq_ignore_ascii_case(&display))
    {
        if grouping_specs.iter().any(|spec| spec.out_index == index) {
            return Ok((display, index));
        }
    }
    let group_positions = grouping_arg_positions(args, group_by_names)?;
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
/// row once per grouping set. The rolled-up columns are NULLed in the
/// materialized SOURCE rows, so every expression over them (the `FIRST_ROW`
/// carriers, `a+1`, a `HAVING` reference) evaluates against NULL exactly as
/// it does over Expand's replicated rows; a genuinely-NULL data value and a
/// rollup NULL are then indistinguishable in the output, as in TiDB (captured
/// from real TiDB: `a=1` rows `(b=1,c=10)`/`(b=NULL,c=20)` yield both
/// `[1 NULL 20]` and the subtotal `[1 NULL 30]`). `GROUPING()` is what tells
/// the two apart, and each pass fills its `grouping_specs` columns with the
/// bitmask for the grouping set that pass computes.
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
    outputs: RollupOutputMetadata<'_>,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    // A source column can be NULLed directly. A derived GROUP BY expression
    // is represented by its matching FIRST_ROW output carrier instead: Go's
    // Expand projects that derived value then NULLs the projection, never the
    // columns the expression happened to read.
    let mut group_cols = Vec::with_capacity(group_by.len());
    for expr in group_by {
        let column = match expr {
            Expression::Column(col) => Some(usize::try_from(col.index).map_err(|_| {
                DriverError::Parse("GROUP BY column has no source index".to_string())
            })?),
            _ => None,
        };
        group_cols.push(column);
    }

    // Materialize the source once; every prefix pass replays these rows.
    let source_schema = source.schema().clone();
    let source_types = source.ret_field_types().to_vec();
    let rows = drain_executor_rows(source, &source_types)?;

    let mut out_rows: Vec<Vec<Datum>> = Vec::new();
    if !rows.is_empty() {
        for k in (0..=group_cols.len()).rev() {
            let mut pass_rows = rows.clone();
            for row in &mut pass_rows {
                for &idx in &group_cols[k..] {
                    if let Some(idx) = idx {
                        row[idx] = Datum::Null;
                    }
                }
            }
            let pass_source = Box::new(MemTableSourceExec::new(
                ExecutorMeta::new(source_schema.clone(), 1, INIT_CAP, MAX_CHUNK_SIZE),
                pass_rows,
            ));
            let agg = HashAggExec::new(
                ExecutorMeta::new(out_schema.clone(), 2, INIT_CAP, MAX_CHUNK_SIZE),
                group_by[..k].to_vec(),
                agg_funcs.to_vec(),
                pass_source,
                ctx.clone(),
                ctx.statement_memory(),
            );
            // This pass rolls up positions `k..`, which IS the grouping bit
            // each GROUPING() call reports -- the one thing that distinguishes
            // a subtotal's NULL from a data NULL.
            let mut pass_out = drain_executor_rows(Box::new(agg), out_types)?;
            for spec in outputs.grouping_specs {
                let mask = Datum::UInt(spec.mask_for_prefix(k));
                for row in &mut pass_out {
                    row[spec.out_index] = mask.clone();
                }
            }
            for &(output, group_position) in outputs.group_carriers {
                if group_position >= k {
                    for row in &mut pass_out {
                        row[output] = Datum::Null;
                    }
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
