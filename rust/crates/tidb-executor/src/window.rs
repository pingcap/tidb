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

//! The frame-less RANKING window functions -- `ROW_NUMBER()`, `RANK()`,
//! `DENSE_RANK()` and `NTILE(n)` `OVER ([PARTITION BY ...] [ORDER BY ...])`,
//! including named windows (`WINDOW w AS (...)`).
//!
//! Go computes these in `pkg/executor/window.go` over a sorted child, one
//! `aggfuncs` ranking function per partition (`func_rank.go`,
//! `func_ntile.go`). This crate's driver materializes the source rows before
//! the projection runs, so the same values come out of a simpler shape: group
//! the materialized rows by the `PARTITION BY` key, stable-sort each partition
//! by the window's own `ORDER BY`, walk it once per function, and append the
//! results as extra source columns named `__window_<i>`. Each `Expr::Window`
//! in the select list / `ORDER BY` is then rewritten to read its appended
//! column, so the ordinary projection, outer `ORDER BY` and `LIMIT` pipeline
//! runs unchanged -- which is also why the outer `ORDER BY` sorts the
//! already-computed window values, as Go does (confirmed against Go: `... FROM
//! t ORDER BY 3 DESC` reorders rows whose `ROW_NUMBER` was computed under the
//! window's own order).
//!
//! Semantics confirmed against Go (`TestZZDumpWindow` capture, since removed):
//!
//! * `RANK` is peer-aware and SKIPS: ties share the lower rank and the next
//!   distinct value jumps to its 1-based position (`1,2,2,4,5`); `DENSE_RANK`
//!   does not skip (`1,2,2,3,4`); `ROW_NUMBER` ignores peers entirely.
//! * Peers are rows equal on EVERY window `ORDER BY` key. With NO `ORDER BY`
//!   at all every row of the partition is a peer, so `RANK`/`DENSE_RANK`
//!   return `1` for all of them.
//! * `NTILE(k)` over a partition of `n` rows uses `quotient = n / k` and
//!   `remainder = n % k`: the FIRST `remainder` buckets hold `quotient + 1`
//!   rows and the rest hold `quotient` (`n = 5, k = 2` -> `1,1,1,2,2`), and
//!   when `k > n` the surplus buckets stay empty (`n = 3, k = 5` -> `1,2,3`).
//! * Result type is `LONGLONG(21)` for all four: `NOT NULL` for the three
//!   ranking functions, `UNSIGNED BINARY` (nullable) for `NTILE`.
//!
//! SLICE SCOPE: exactly these four functions, frame-less. Window AGGREGATES
//! (`SUM(x) OVER ...`), the value family (`LAG`/`LEAD`/`FIRST_VALUE`/...) and
//! the remaining distribution functions (`PERCENT_RANK`/`CUME_DIST`) are
//! refused with [`SLICE_MESSAGE`], as is a window function combined with
//! `GROUP BY`/aggregation. An explicit `ROWS`/`RANGE` frame is ACCEPTED and
//! ignored, because Go ignores it for every ranking function too -- refusing
//! it would be a divergence, not a restriction.

use crate::driver::{row_chunk, DriverError, FromScope, FromTable};
use crate::StmtContext;
use std::any::Any;
use tidb_ast::{Expr, OrderItem, SelectField, SelectStmt, WindowDef, WindowOver, WindowSpec};
use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_expr::rewriter::{rewrite_expr_resolved, ColumnResolver};

/// What this build refuses, naming the slice it does implement.
pub(crate) const SLICE_MESSAGE: &str =
    "only the frame-less ranking window functions ROW_NUMBER(), \
     RANK(), DENSE_RANK() and NTILE(n) are supported";

/// The prefix of the synthetic column each computed window call is read from.
const WINDOW_COLUMN_PREFIX: &str = "__window_";

/// One window call to compute: the AST node as written (the key the rewrite
/// matches on) plus its fully resolved specification.
pub(crate) struct WindowCall {
    /// The `Expr::Window` node exactly as it appears in the query.
    node: Expr,
    /// The uppercase function name.
    name: String,
    /// `NTILE`'s bucket count, already validated; `None` for the others and
    /// for `NTILE(NULL)`, whose result is `NULL` for every row.
    buckets: Option<u64>,
    /// Whether the call is `NTILE` (which `buckets == None` alone cannot say).
    is_ntile: bool,
    /// The specification after named-window resolution.
    spec: WindowSpec,
}

impl WindowCall {
    /// The result type Go's `NewWindowFuncDesc` infers for this function.
    fn result_type(&self) -> FieldType {
        let mut field_type = FieldType::new(FieldTypeCode::LongLong);
        field_type.set_flen(21);
        field_type.set_decimal(0);
        if self.is_ntile {
            // Go's `typeInfer4Ntile`: binary charset plus UNSIGNED, and
            // deliberately no NOT NULL (`NTILE(NULL)` is all NULLs).
            field_type.add_flags(FieldTypeFlags::BINARY | FieldTypeFlags::UNSIGNED);
        } else {
            field_type.add_flags(FieldTypeFlags::NOT_NULL);
        }
        field_type
    }
}

/// Collects every `Expr::Window` node inside `expr`, in written order.
fn windows_in(expr: &Expr) -> Vec<Expr> {
    struct Collector {
        found: Vec<Expr>,
    }
    impl tidb_ast::Visitor for Collector {
        fn enter(&mut self, node: &mut dyn Any) -> bool {
            if let Some(Expr::Window { .. }) = node.downcast_ref::<Expr>() {
                let window = node.downcast_ref::<Expr>().expect("checked above").clone();
                self.found.push(window);
                // A window function may not nest another one, so its children
                // hold nothing more to collect.
                return true;
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn Any) -> bool {
            true
        }
    }
    let mut collector = Collector { found: Vec::new() };
    let mut owned = expr.clone();
    tidb_ast::Visitable::accept(&mut owned, &mut collector);
    collector.found
}

/// The function name of the first window call in `expr`, lowercased the way
/// Go's `ErrWindowInvalidWindowFuncUse` reports it.
fn first_window_name(expr: &Expr) -> Option<String> {
    windows_in(expr).first().map(|window| match window {
        Expr::Window { name, .. } => name.to_lowercase(),
        _ => unreachable!("windows_in only yields Expr::Window"),
    })
}

/// Every expression a window function may legally appear in: the select list
/// and the `ORDER BY`, in that order.
fn window_bearing_exprs(select: &SelectStmt) -> impl Iterator<Item = &Expr> {
    select
        .fields
        .fields()
        .iter()
        .filter_map(|field| match field {
            SelectField::Expr { expr, .. } => Some(expr),
            SelectField::Wildcard(_) => None,
        })
        .chain(select.order_by.iter().map(|item| &item.expr))
}

/// Whether the select list or `ORDER BY` carries a window function.
///
/// An `ORDER BY`-only window (`... ORDER BY ROW_NUMBER() OVER (ORDER BY v)`)
/// counts: Go computes and sorts by it without projecting it, and so does
/// this stage -- the value lands in a synthetic column the projection simply
/// does not read.
pub(crate) fn select_has_window(select: &SelectStmt) -> bool {
    window_bearing_exprs(select).any(|expr| !windows_in(expr).is_empty())
}

/// Go rejects a window function outside the select list / `ORDER BY` with
/// `ErrWindowInvalidWindowFuncUse` (3593) -- `WHERE`, `GROUP BY` and `HAVING`
/// alike, whether or not the query has any other window function.
pub(crate) fn reject_windows_outside_select_list(select: &SelectStmt) -> Result<(), DriverError> {
    let elsewhere = select
        .where_clause
        .iter()
        .chain(select.having.iter())
        .chain(select.group_by.iter().map(|item| &item.expr));
    for expr in elsewhere {
        if let Some(name) = first_window_name(expr) {
            return Err(DriverError::WindowInvalidWindowFuncUse(name));
        }
    }
    Ok(())
}

/// Resolves an `OVER` clause against the query's `WINDOW` clause.
///
/// A bare or parenthesized name inherits that window's specification; a
/// parenthesized name may EXTEND it, under Go's `mergeWindowSpec` rules: an
/// extension may never define its own `PARTITION BY` (3581) and may only add
/// an `ORDER BY` when the base chain has none (3583).
fn resolve_over(
    over: &WindowOver,
    named: &[(String, WindowDef)],
) -> Result<WindowSpec, DriverError> {
    let def = match over {
        WindowOver::Name(name) => WindowDef {
            base: Some(name.clone()),
            spec: WindowSpec::default(),
        },
        WindowOver::Def(def) => def.clone(),
    };
    resolve_def(&def, named, &mut Vec::new())
}

/// Resolves one definition, following its `base` chain. `seen` carries the
/// names already on the chain so a cycle stops instead of recursing forever.
fn resolve_def(
    def: &WindowDef,
    named: &[(String, WindowDef)],
    seen: &mut Vec<String>,
) -> Result<WindowSpec, DriverError> {
    let Some(base_name) = &def.base else {
        return Ok(def.spec.clone());
    };
    if seen.iter().any(|name| name.eq_ignore_ascii_case(base_name)) {
        return Err(DriverError::WindowCircularity);
    }
    seen.push(base_name.clone());
    let base_def = named
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(base_name))
        .map(|(_, def)| def)
        .ok_or_else(|| DriverError::WindowNoSuchWindow(base_name.clone()))?;
    let base = resolve_def(base_def, named, seen)?;
    if !def.spec.partition_by.is_empty() {
        return Err(DriverError::WindowNoChildPartitioning);
    }
    if !def.spec.order_by.is_empty() && !base.order_by.is_empty() {
        return Err(DriverError::WindowNoRedefineOrderBy(base_name.clone()));
    }
    Ok(WindowSpec {
        partition_by: base.partition_by,
        order_by: if def.spec.order_by.is_empty() {
            base.order_by
        } else {
            def.spec.order_by.clone()
        },
        // A frame is inert for every function this slice implements, so the
        // base's and the extension's are equally ignored.
        frame: def.spec.frame.clone().or(base.frame),
    })
}

/// Collects the DISTINCT window calls of the select list, in written order,
/// resolving and validating each one's `OVER` clause.
///
/// Two textually identical calls share one computed column; a call that this
/// slice does not implement is refused here, before any row is read.
pub(crate) fn collect_window_calls(select: &SelectStmt) -> Result<Vec<WindowCall>, DriverError> {
    let mut calls: Vec<WindowCall> = Vec::new();
    for expr in window_bearing_exprs(select) {
        for node in windows_in(expr) {
            if calls.iter().any(|call| call.node == node) {
                continue;
            }
            calls.push(build_call(node, select)?);
        }
    }
    Ok(calls)
}

/// Validates one window call and resolves its specification.
fn build_call(node: Expr, select: &SelectStmt) -> Result<WindowCall, DriverError> {
    let Expr::Window {
        name,
        args,
        distinct,
        ignore_nulls,
        from_last,
        over,
    } = &node
    else {
        unreachable!("collect_window_calls only yields Expr::Window");
    };
    if *distinct || *ignore_nulls || *from_last {
        return Err(DriverError::Unsupported(SLICE_MESSAGE));
    }
    let upper = name.to_uppercase();
    let is_ntile = upper == "NTILE";
    if !matches!(
        upper.as_str(),
        "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "NTILE"
    ) {
        return Err(DriverError::Unsupported(SLICE_MESSAGE));
    }
    // Go's `NewWindowFuncDesc` validates NTILE's bucket count in the planner:
    // it must be a constant, NULL or a positive integer -- anything else
    // (`0`, a negative, a column reference) is `ErrWrongArguments` (1210).
    let buckets = if is_ntile {
        if args.len() != 1 {
            return Err(DriverError::WrongArguments("ntile"));
        }
        match constant_bucket_count(&args[0]) {
            Some(BucketCount::Null) => None,
            Some(BucketCount::Positive(count)) => Some(count),
            None => return Err(DriverError::WrongArguments("ntile")),
        }
    } else {
        // The ranking functions take no arguments; Go's parser already
        // enforces that, so anything else here is out of this slice.
        if !args.is_empty() {
            return Err(DriverError::Unsupported(SLICE_MESSAGE));
        }
        None
    };
    let spec = resolve_over(over, &select.windows)?;
    Ok(WindowCall {
        node,
        name: upper,
        buckets,
        is_ntile,
        spec,
    })
}

/// `NTILE`'s validated argument.
enum BucketCount {
    /// `NTILE(NULL)`: accepted, and every row's result is `NULL`.
    Null,
    /// A positive constant bucket count.
    Positive(u64),
}

/// Reads `NTILE`'s bucket count from a constant argument, or `None` when the
/// argument is not a constant Go would accept.
fn constant_bucket_count(arg: &Expr) -> Option<BucketCount> {
    match arg {
        Expr::Null => Some(BucketCount::Null),
        Expr::Int(text) => text
            .parse::<u64>()
            .ok()
            .filter(|count| *count > 0)
            .map(BucketCount::Positive),
        _ => None,
    }
}

/// Computes every call's per-row value over `rows`, in `rows` order.
///
/// The returned rows are `rows` with one appended datum per call, and the
/// returned scope is `scope` plus the matching synthetic columns.
pub(crate) fn compute_windows(
    calls: &[WindowCall],
    rows: Vec<Vec<Datum>>,
    scope: &FromScope,
    ctx: &StmtContext,
) -> Result<(Vec<Vec<Datum>>, FromScope), DriverError> {
    let resolver = crate::driver::scope_resolver(scope);
    let field_types: Vec<FieldType> = scope
        .column_list()
        .into_iter()
        .map(|(_, field_type)| field_type)
        .collect();
    let mut computed: Vec<Vec<Datum>> = Vec::with_capacity(calls.len());
    for call in calls {
        computed.push(compute_one(call, &rows, &field_types, &resolver, ctx)?);
    }
    let mut out_rows = rows;
    for (row_index, row) in out_rows.iter_mut().enumerate() {
        for values in &computed {
            row.push(values[row_index].clone());
        }
    }
    let mut out_scope = scope.clone();
    let offset = scope.width();
    out_scope.tables.push(FromTable {
        name: String::new(),
        columns: calls
            .iter()
            .enumerate()
            .map(|(i, call)| (window_column_name(i), call.result_type()))
            .collect(),
        offset,
    });
    Ok((out_rows, out_scope))
}

/// The synthetic column the `i`th window call's value lands in.
fn window_column_name(index: usize) -> String {
    format!("{WINDOW_COLUMN_PREFIX}{index}")
}

/// Computes one call's value for every row, in source-row order.
fn compute_one(
    call: &WindowCall,
    rows: &[Vec<Datum>],
    field_types: &[FieldType],
    resolver: &impl ColumnResolver,
    ctx: &StmtContext,
) -> Result<Vec<Datum>, DriverError> {
    let partition_keys = eval_keys(&call.spec.partition_by, rows, field_types, resolver, ctx)?;
    let order_exprs: Vec<Expr> = call
        .spec
        .order_by
        .iter()
        .map(|item: &OrderItem| item.expr.clone())
        .collect();
    let order_keys = eval_keys(&order_exprs, rows, field_types, resolver, ctx)?;

    // Partition on the hash encoding of the key datums, exactly as the hash
    // aggregation groups rows, keeping each partition's rows in source order.
    let mut partitions: std::collections::HashMap<Vec<u8>, Vec<usize>> =
        std::collections::HashMap::new();
    for (index, key) in partition_keys.iter().enumerate() {
        let mut encoded = Vec::new();
        for datum in key {
            encoded.extend_from_slice(&tidb_codec::hash_code(datum));
            encoded.push(0xff); // separator: key parts are length-coded
        }
        partitions.entry(encoded).or_default().push(index);
    }

    let mut values = vec![Datum::Null; rows.len()];
    for indices in partitions.values_mut() {
        sort_partition(indices, &order_keys, &call.spec.order_by)?;
        rank_partition(call, indices, &order_keys, &mut values);
    }
    Ok(values)
}

/// Evaluates one key expression list for every row.
fn eval_keys(
    exprs: &[Expr],
    rows: &[Vec<Datum>],
    field_types: &[FieldType],
    resolver: &impl ColumnResolver,
    ctx: &StmtContext,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    let mut rewritten = Vec::with_capacity(exprs.len());
    for expr in exprs {
        rewritten.push(
            rewrite_expr_resolved(expr, resolver)
                .map_err(|e| DriverError::Exec(crate::ExecError::Eval(e)))?,
        );
    }
    let mut keys = Vec::with_capacity(rows.len());
    for row in rows {
        let chunk = row_chunk(row, field_types)?;
        let mut key = Vec::with_capacity(rewritten.len());
        for expr in &rewritten {
            key.push(
                expr.eval(ctx, chunk.get_row(0))
                    .map_err(|e| DriverError::Exec(crate::ExecError::Eval(e)))?,
            );
        }
        keys.push(key);
    }
    Ok(keys)
}

/// Stable-sorts one partition's row indices by the window's `ORDER BY`.
///
/// The sort is stable, so rows tied on every key keep their source order --
/// which is what makes `ROW_NUMBER` over ties deterministic here.
fn sort_partition(
    indices: &mut [usize],
    order_keys: &[Vec<Datum>],
    order_by: &[OrderItem],
) -> Result<(), DriverError> {
    if order_by.is_empty() {
        return Ok(());
    }
    let mut failure = None;
    indices.sort_by(|left, right| {
        for (position, item) in order_by.iter().enumerate() {
            let ordering = match tidb_expr::compare_datums(
                &order_keys[*left][position],
                &order_keys[*right][position],
            ) {
                Ok(ordering) => ordering,
                Err(error) => {
                    failure = Some(error);
                    std::cmp::Ordering::Equal
                }
            };
            if ordering != std::cmp::Ordering::Equal {
                return if item.desc {
                    ordering.reverse()
                } else {
                    ordering
                };
            }
        }
        std::cmp::Ordering::Equal
    });
    match failure {
        Some(error) => Err(DriverError::Exec(crate::ExecError::Eval(error))),
        None => Ok(()),
    }
}

/// Writes one partition's ranking values into `values`, at each row's own
/// source position.
fn rank_partition(
    call: &WindowCall,
    indices: &[usize],
    order_keys: &[Vec<Datum>],
    values: &mut [Datum],
) {
    // Rows with no window `ORDER BY` are all peers of each other, which is
    // exactly what an empty key compares as.
    let peers = |left: usize, right: usize| order_keys[left] == order_keys[right];
    match call.name.as_str() {
        "ROW_NUMBER" => {
            for (position, index) in indices.iter().enumerate() {
                values[*index] = Datum::Int(position as i64 + 1);
            }
        }
        "RANK" => {
            let mut rank = 1i64;
            for (position, index) in indices.iter().enumerate() {
                if position > 0 && !peers(indices[position - 1], *index) {
                    rank = position as i64 + 1;
                }
                values[*index] = Datum::Int(rank);
            }
        }
        "DENSE_RANK" => {
            let mut rank = 1i64;
            for (position, index) in indices.iter().enumerate() {
                if position > 0 && !peers(indices[position - 1], *index) {
                    rank += 1;
                }
                values[*index] = Datum::Int(rank);
            }
        }
        "NTILE" => {
            let Some(buckets) = call.buckets else {
                // NTILE(NULL): every row is NULL, and `values` already is.
                return;
            };
            let total = indices.len() as u64;
            let quotient = total / buckets;
            let remainder = total % buckets;
            let mut bucket = 1u64;
            let mut taken = 0u64;
            for index in indices {
                // The first `remainder` buckets take one extra row; a bucket
                // that would be empty (more buckets than rows) is skipped.
                let mut size = quotient + u64::from(bucket <= remainder);
                while size == 0 {
                    bucket += 1;
                    size = quotient + u64::from(bucket <= remainder);
                }
                values[*index] = Datum::UInt(bucket);
                taken += 1;
                if taken == size {
                    bucket += 1;
                    taken = 0;
                }
            }
        }
        _ => unreachable!("build_call rejects every other function name"),
    }
}

/// Rewrites `select` so each computed window call reads its appended column.
///
/// Both the select list and the `ORDER BY` are rewritten, so ordering by a
/// window function -- directly, or through a select alias the driver already
/// substitutes -- reads the computed value instead of recomputing it.
pub(crate) fn rewrite_windows(select: &SelectStmt, calls: &[WindowCall]) -> SelectStmt {
    struct Replacer<'a> {
        calls: &'a [WindowCall],
    }
    impl tidb_ast::Visitor for Replacer<'_> {
        fn enter(&mut self, node: &mut dyn Any) -> bool {
            let Some(expr) = node.downcast_mut::<Expr>() else {
                return false;
            };
            if !matches!(expr, Expr::Window { .. }) {
                return false;
            }
            if let Some(index) = self.calls.iter().position(|call| &call.node == expr) {
                *expr = Expr::Column(vec![window_column_name(index)]);
            }
            true
        }

        fn leave(&mut self, _node: &mut dyn Any) -> bool {
            true
        }
    }
    let mut rewritten = select.clone();
    let mut replacer = Replacer { calls };
    for field in rewritten.fields.fields_mut() {
        if let SelectField::Expr { expr, .. } = field {
            tidb_ast::Visitable::accept(expr, &mut replacer);
        }
    }
    for item in &mut rewritten.order_by {
        tidb_ast::Visitable::accept(&mut item.expr, &mut replacer);
    }
    rewritten
}
