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
//! Semantics confirmed against Go (`TestZZDumpWindow2` capture, since
//! removed) for the FRAMED families:
//!
//! * The DEFAULT frame (no `ROWS`/`RANGE` written) is `RANGE BETWEEN
//!   UNBOUNDED PRECEDING AND CURRENT ROW`, and `RANGE`'s `CURRENT ROW` is
//!   PEER-INCLUSIVE: `SUM(v) OVER (PARTITION BY g ORDER BY v)` over
//!   `10,20,20,30` yields `10,50,50,80` -- the two tied rows share the sum
//!   that INCLUDES both of them, not a row-by-row running total. With NO
//!   window `ORDER BY` every row is a peer, so the frame is the whole
//!   partition and every row shows the partition total.
//! * `LAST_VALUE` under the default frame therefore returns the CURRENT PEER
//!   GROUP's last row, not the partition's last (`10,20,20,30` -> `10,20,20,
//!   30`); `... ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`
//!   is what returns the partition's last row (`30,30,30,30`).
//! * An EMPTY frame (`ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING` at the
//!   partition's first row, or any `ROWS BETWEEN 2 FOLLOWING AND 1
//!   FOLLOWING`) yields `SUM`/`AVG`/`MIN`/`MAX`/`FIRST_VALUE`/`LAST_VALUE`/
//!   `NTH_VALUE` = `NULL` but `COUNT` = `0`.
//! * `LAG`/`LEAD` IGNORE the frame entirely -- they address the sorted
//!   partition directly. The offset defaults to 1, `LAG(v, 0)` is the current
//!   row, and an out-of-range position yields the third argument when written
//!   and `NULL` otherwise. A written default MERGES into the result type
//!   (Go `typeInfer4LeadLag` -> `InferType4ControlFuncs`): `LAG(int_col, 2,
//!   -1)` is `BIGINT`, `LAG(int_col, 1, 'zz')` is a `VARCHAR`.
//! * `FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE` DO respect the frame;
//!   `NTH_VALUE(v, n)` is `NULL` when the frame holds fewer than `n` rows,
//!   and `n` must be a positive integer constant (`NTH_VALUE(v, 0)` is Go's
//!   `ErrWrongArguments`).
//! * A window `ORDER BY` sorts NULLs FIRST ascending / LAST descending, and
//!   all NULL keys are peers of each other.
//!
//! Frame VALIDATION is the planner's, so it fires for EVERY function -- a
//! ranking function with a bad frame errors even though the frame is then
//! ignored (confirmed: `ROW_NUMBER() OVER (... ROWS BETWEEN CURRENT ROW AND
//! 1 PRECEDING)` is 3586, `RANK() OVER (PARTITION BY g RANGE BETWEEN 1
//! PRECEDING AND CURRENT ROW)` is 3587).
//!
//! SLICE SCOPE: the four ranking functions (frame-less, as above), the value
//! family `LAG`/`LEAD`/`FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE`, and the
//! aggregates `SUM`/`COUNT`/`AVG`/`MIN`/`MAX` as window functions, over the
//! default frame or an explicit `ROWS BETWEEN`. Still refused: the
//! distribution functions (`PERCENT_RANK`/`CUME_DIST`), every other
//! aggregate as a window function, a window function combined with `GROUP
//! BY`/aggregation, and an explicit `RANGE` frame carrying an `N
//! PRECEDING`/`N FOLLOWING` VALUE bound ([`RANGE_OFFSET_MESSAGE`]) -- a
//! `RANGE` frame built only from `UNBOUNDED PRECEDING`/`CURRENT ROW`/
//! `UNBOUNDED FOLLOWING` needs no value arithmetic and IS implemented, since
//! it is exactly the peer-based default frame written out.
//!
//! Result TYPES follow Go's `baseFuncDesc.TypeInfer`: `COUNT` is a NOT NULL
//! `BIGINT(21)`, `SUM` a `DECIMAL` (`DOUBLE` for a real argument), `AVG` a
//! `DECIMAL` scaled by `div_precision_increment` (`DOUBLE` for a real
//! argument), and `MIN`/`MAX`/`FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE`/`LAG`/
//! `LEAD` carry the argument's own type. As on the GROUP BY path this stage
//! shares, the DISPLAY metadata Go derives on top (a `SUM`'s `flen` of
//! `arg_flen + 21`, an `AVG`'s scale) is a documented deferral: the type
//! CODE is faithful, the width is not.

use crate::driver::{row_chunk, DriverError, FromScope, FromTable};
use crate::hash_agg::{aggregate_values, AggKind};
use crate::StmtContext;
use std::any::Any;
use tidb_ast::{
    Expr, FrameBound, FrameKind, OrderItem, SelectField, SelectStmt, WindowDef, WindowFrame,
    WindowOver, WindowSpec,
};
use tidb_datatype::{agg_field_type, Datum, FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_expr::rewriter::{rewrite_expr_resolved, ColumnResolver};

/// What this build refuses, naming the slice it does implement.
pub(crate) const SLICE_MESSAGE: &str =
    "only the ranking (ROW_NUMBER, RANK, DENSE_RANK, NTILE), value (LAG, LEAD, \
     FIRST_VALUE, LAST_VALUE, NTH_VALUE) and aggregate (SUM, COUNT, AVG, MIN, MAX) \
     window functions are supported";

/// What a `LAG`/`LEAD` whose default WIDENS the result type is refused with:
/// Go casts both the argument and the default to the merged type, and that
/// cast is a separate unit.
pub(crate) const LAG_LEAD_CAST_MESSAGE: &str =
    "LAG/LEAD with a default whose type does not match the argument's is not \
     yet supported, because the result would need a cast";

/// What a `RANGE` frame with a VALUE offset bound is refused with: those need
/// Go's per-type range arithmetic over the single `ORDER BY` key, which this
/// slice does not implement.
pub(crate) const RANGE_OFFSET_MESSAGE: &str =
    "a RANGE frame with an N PRECEDING/N FOLLOWING value bound is not yet \
     supported; use a ROWS frame or the default frame";

/// The prefix of the synthetic column each computed window call is read from.
const WINDOW_COLUMN_PREFIX: &str = "__window_";

/// One window call to compute: the AST node as written (the key the rewrite
/// matches on) plus its classified function and fully resolved specification.
pub(crate) struct WindowCall {
    /// The `Expr::Window` node exactly as it appears in the query.
    node: Expr,
    /// Which function, with each constant argument already folded.
    kind: WindowKind,
    /// The specification after named-window resolution.
    spec: WindowSpec,
    /// The frame every non-ranking function evaluates over, already validated
    /// and defaulted.
    frame: Frame,
}

/// The window function itself, with each constant argument already folded.
enum WindowKind {
    /// `ROW_NUMBER()`.
    RowNumber,
    /// `RANK()`.
    Rank,
    /// `DENSE_RANK()`.
    DenseRank,
    /// `NTILE(n)`; `None` is `NTILE(NULL)`, whose result is `NULL` everywhere.
    Ntile(Option<u64>),
    /// `SUM`/`COUNT`/`AVG`/`MIN`/`MAX` over the frame. `arg` is the argument
    /// as written -- `COUNT(*)`'s is the literal `1` the parser substitutes,
    /// so no absent-argument case survives here.
    Agg {
        /// The uppercase aggregate name.
        name: String,
        /// The argument expression.
        arg: Expr,
    },
    /// `FIRST_VALUE(v)` / `LAST_VALUE(v)` / `NTH_VALUE(v, n)` over the frame.
    Value {
        /// The value expression.
        arg: Expr,
        /// Which row of the frame to read.
        pick: Pick,
    },
    /// `LAG(v[, n[, default]])` / `LEAD(...)`, which ignore the frame.
    LagLead {
        /// The value expression.
        arg: Expr,
        /// `true` for `LAG` (look backwards), `false` for `LEAD`.
        is_lag: bool,
        /// The row offset; Go defaults it to 1, and `0` is the current row.
        offset: u64,
        /// The out-of-range default expression, `None` when unwritten (which
        /// makes an out-of-range position `NULL`).
        default: Option<Expr>,
    },
}

/// Which row of the frame a value function reads.
enum Pick {
    /// The `n`th row counting from the frame's start, 1-based --
    /// `FIRST_VALUE` is `Nth(1)`.
    Nth(u64),
    /// The frame's last row.
    Last,
}

/// One frame boundary with its offset already folded to a constant.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Bound {
    /// The partition's first row.
    UnboundedPreceding,
    /// `N` positions (or, under `RANGE`, `N` of value) before the current row.
    Preceding(u64),
    /// The current row -- under `RANGE`, its whole PEER GROUP.
    CurrentRow,
    /// `N` positions after the current row.
    Following(u64),
    /// The partition's last row.
    UnboundedFollowing,
}

impl Bound {
    /// The bound's place in Go's `UNBOUNDED PRECEDING < N PRECEDING < CURRENT
    /// ROW < N FOLLOWING < UNBOUNDED FOLLOWING` order, which decides whether a
    /// frame's `start` may precede its `end` REGARDLESS of the offsets' own
    /// values.
    fn rank(self) -> u8 {
        match self {
            Bound::UnboundedPreceding => 0,
            Bound::Preceding(_) => 1,
            Bound::CurrentRow => 2,
            Bound::Following(_) => 3,
            Bound::UnboundedFollowing => 4,
        }
    }

    /// Whether this bound carries a VALUE offset, which is what makes a
    /// `RANGE` frame need the sort key's own arithmetic.
    fn has_offset(self) -> bool {
        matches!(self, Bound::Preceding(_) | Bound::Following(_))
    }
}

/// The resolved frame a non-ranking window function evaluates over.
///
/// The two variants are the two ways a boundary is measured: `Rows` counts
/// physical positions in the sorted partition, `Peers` counts PEER GROUPS --
/// which is what `RANGE`'s `CURRENT ROW` means, and therefore what the
/// implicit default frame means.
enum Frame {
    /// `ROWS BETWEEN start AND end`.
    Rows {
        /// The starting boundary.
        start: Bound,
        /// The ending boundary.
        end: Bound,
    },
    /// `RANGE BETWEEN start AND end` built only from peer-based bounds, and
    /// the implicit default frame (`UNBOUNDED PRECEDING` .. `CURRENT ROW`).
    Peers {
        /// The starting boundary.
        start: Bound,
        /// The ending boundary.
        end: Bound,
    },
}

impl Frame {
    /// The half-open `[start, end)` position range this frame covers for the
    /// row at `position` of a partition of `total` rows, whose own peer group
    /// spans the half-open range `peers`.
    ///
    /// An empty frame comes back as an empty range; every out-of-partition
    /// offset is clamped, so `ROWS BETWEEN 1 PRECEDING AND CURRENT ROW` at the
    /// partition's first row is `[0, 1)` rather than an error.
    fn range(&self, position: usize, total: usize, peers: (usize, usize)) -> (usize, usize) {
        let total_i = total as i128;
        let (low, high) = match self {
            Frame::Rows { start, end } => {
                let position = position as i128;
                // Each bound names a row; the half-open end is one past the
                // row the END bound names.
                let at = |bound: &Bound, unbounded_low: i128, unbounded_high: i128| match bound {
                    Bound::UnboundedPreceding => unbounded_low,
                    Bound::Preceding(n) => position - i128::from(*n),
                    Bound::CurrentRow => position,
                    Bound::Following(n) => position + i128::from(*n),
                    Bound::UnboundedFollowing => unbounded_high,
                };
                (at(start, 0, total_i), at(end, -1, total_i - 1) + 1)
            }
            Frame::Peers { start, end } => {
                let at = |bound: &Bound, current: usize| match bound {
                    Bound::UnboundedPreceding => 0,
                    Bound::CurrentRow => current,
                    Bound::UnboundedFollowing => total,
                    // `build_frame` refuses an offset bound under RANGE.
                    Bound::Preceding(_) | Bound::Following(_) => {
                        unreachable!("a peer frame carries no offset bound")
                    }
                };
                (at(start, peers.0) as i128, at(end, peers.1) as i128)
            }
        };
        let low = low.clamp(0, total_i) as usize;
        let high = high.clamp(0, total_i) as usize;
        (low, high.max(low))
    }
}

/// Folds one frame bound's offset, which Go requires to be a non-negative
/// integer constant (`ErrWindowFrameIllegal`, 3586 -- a fractional or NULL
/// offset lands here; a negative one is already a parse error).
fn build_bound(bound: &FrameBound) -> Result<Bound, DriverError> {
    let offset = |expr: &Expr| match expr {
        Expr::Int(text) => text.parse::<u64>().ok(),
        _ => None,
    };
    Ok(match bound {
        FrameBound::UnboundedPreceding => Bound::UnboundedPreceding,
        FrameBound::CurrentRow => Bound::CurrentRow,
        FrameBound::UnboundedFollowing => Bound::UnboundedFollowing,
        FrameBound::Preceding(expr) => {
            Bound::Preceding(offset(expr).ok_or(DriverError::WindowFrameIllegal)?)
        }
        FrameBound::Following(expr) => {
            Bound::Following(offset(expr).ok_or(DriverError::WindowFrameIllegal)?)
        }
    })
}

/// Validates a window's frame clause and resolves it, defaulting an unwritten
/// one to `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.
///
/// This runs for EVERY window function, ranking ones included: Go validates
/// the spec in the planner, so a bad frame is an error even where the frame is
/// then ignored.
fn build_frame(spec: &WindowSpec) -> Result<Frame, DriverError> {
    let Some(WindowFrame { kind, start, end }) = &spec.frame else {
        return Ok(Frame::Peers {
            start: Bound::UnboundedPreceding,
            end: Bound::CurrentRow,
        });
    };
    let start = build_bound(start)?;
    let end = build_bound(end)?;
    if start.rank() > end.rank() {
        return Err(DriverError::WindowFrameIllegal);
    }
    match kind {
        FrameKind::Rows => Ok(Frame::Rows { start, end }),
        FrameKind::Range if !start.has_offset() && !end.has_offset() => {
            // Only peer-based bounds: the default frame, written out.
            Ok(Frame::Peers { start, end })
        }
        FrameKind::Range => {
            // Go checks the ORDER BY shape before anything else about a value
            // bound, so this error wins over the refusal below.
            if spec.order_by.len() != 1 {
                return Err(DriverError::WindowRangeFrameOrderType);
            }
            Err(DriverError::Unsupported(RANGE_OFFSET_MESSAGE))
        }
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
        // An extension's own frame overrides the base's; Go's own restriction
        // on redefining a base's frame is not modelled here.
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
    // A ranking function takes no arguments; Go's parser already enforces
    // that, so a stray one here is out of this slice.
    let no_args = |kind: WindowKind| {
        if args.is_empty() {
            Ok(kind)
        } else {
            Err(DriverError::Unsupported(SLICE_MESSAGE))
        }
    };
    let upper = name.to_uppercase();
    let kind = match upper.as_str() {
        "ROW_NUMBER" => no_args(WindowKind::RowNumber)?,
        "RANK" => no_args(WindowKind::Rank)?,
        "DENSE_RANK" => no_args(WindowKind::DenseRank)?,
        // Go's `NewWindowFuncDesc` validates NTILE's bucket count in the
        // planner: it must be a constant, NULL or a positive integer --
        // anything else (`0`, a negative, a column) is `ErrWrongArguments`.
        "NTILE" => {
            if args.len() != 1 {
                return Err(DriverError::WrongArguments("ntile"));
            }
            match constant_bucket_count(&args[0]) {
                Some(BucketCount::Null) => WindowKind::Ntile(None),
                Some(BucketCount::Positive(count)) => WindowKind::Ntile(Some(count)),
                None => return Err(DriverError::WrongArguments("ntile")),
            }
        }
        "SUM" | "COUNT" | "AVG" | "MIN" | "MAX" => {
            // `COUNT(*)` reaches here as `COUNT(1)`, so one argument is the
            // only shape; `COUNT(DISTINCT a, b)` already failed on `distinct`.
            let [arg] = args.as_slice() else {
                return Err(DriverError::Unsupported(SLICE_MESSAGE));
            };
            WindowKind::Agg {
                name: upper.clone(),
                arg: arg.clone(),
            }
        }
        "FIRST_VALUE" | "LAST_VALUE" | "NTH_VALUE" => {
            let (arg, pick) = match (upper.as_str(), args.as_slice()) {
                ("FIRST_VALUE", [arg]) => (arg, Pick::Nth(1)),
                ("LAST_VALUE", [arg]) => (arg, Pick::Last),
                // Go validates NTH_VALUE's position like NTILE's bucket
                // count: a positive integer constant, or `ErrWrongArguments`.
                ("NTH_VALUE", [arg, position]) => match constant_bucket_count(position) {
                    Some(BucketCount::Positive(n)) => (arg, Pick::Nth(n)),
                    _ => return Err(DriverError::WrongArguments("nth_value")),
                },
                _ => return Err(DriverError::Unsupported(SLICE_MESSAGE)),
            };
            WindowKind::Value {
                arg: arg.clone(),
                pick,
            }
        }
        "LAG" | "LEAD" => {
            let (arg, rest) = args
                .split_first()
                .ok_or(DriverError::Unsupported(SLICE_MESSAGE))?;
            // Go's `NewWindowFuncDesc` requires a non-negative integer
            // constant offset; the parser has already rejected a negative one.
            let offset = match rest.first() {
                None => 1,
                Some(Expr::Int(text)) => text
                    .parse::<u64>()
                    .map_err(|_| DriverError::WrongArguments("lag/lead"))?,
                Some(_) => return Err(DriverError::WrongArguments("lag/lead")),
            };
            WindowKind::LagLead {
                arg: arg.clone(),
                is_lag: upper == "LAG",
                offset,
                default: rest.get(1).cloned(),
            }
        }
        _ => return Err(DriverError::Unsupported(SLICE_MESSAGE)),
    };
    let spec = resolve_over(over, &select.windows)?;
    let frame = build_frame(&spec)?;
    Ok(WindowCall {
        node,
        kind,
        spec,
        frame,
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
    let mut columns: Vec<(String, FieldType)> = Vec::with_capacity(calls.len());
    for (index, call) in calls.iter().enumerate() {
        let (values, result_type) = compute_one(call, &rows, &field_types, &resolver, ctx)?;
        computed.push(values);
        columns.push((window_column_name(index), result_type));
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
        database: None,
        columns,
        offset,
    });
    Ok((out_rows, out_scope))
}

/// The synthetic column the `i`th window call's value lands in.
fn window_column_name(index: usize) -> String {
    format!("{WINDOW_COLUMN_PREFIX}{index}")
}

/// Computes one call's value for every row, in source-row order, together
/// with the result type Go's `NewWindowFuncDesc` infers for it.
fn compute_one(
    call: &WindowCall,
    rows: &[Vec<Datum>],
    field_types: &[FieldType],
    resolver: &impl ColumnResolver,
    ctx: &StmtContext,
) -> Result<(Vec<Datum>, FieldType), DriverError> {
    // The function's own argument (and LAG/LEAD's default) is evaluated per
    // row up front, against the SOURCE row -- the same scope the partition and
    // order keys resolve in.
    let arg_exprs: Vec<Expr> = call.kind.value_args().into_iter().cloned().collect();
    let (arg_values, arg_types) = eval_args(&arg_exprs, rows, field_types, resolver, ctx)?;
    let result_type = call.kind.result_type(&arg_types)?;

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
        match &call.kind {
            WindowKind::RowNumber
            | WindowKind::Rank
            | WindowKind::DenseRank
            | WindowKind::Ntile(_) => rank_partition(&call.kind, indices, &order_keys, &mut values),
            _ => evaluate_partition(call, indices, &order_keys, &arg_values, &mut values)?,
        }
    }
    Ok((values, result_type))
}

impl WindowKind {
    /// The argument expressions this function evaluates per row: the value
    /// argument first, then `LAG`/`LEAD`'s out-of-range default. The ranking
    /// functions have none (`NTILE`'s and `NTH_VALUE`'s counts are constants
    /// already folded at build time, so they are not row expressions).
    fn value_args(&self) -> Vec<&Expr> {
        match self {
            WindowKind::RowNumber
            | WindowKind::Rank
            | WindowKind::DenseRank
            | WindowKind::Ntile(_) => Vec::new(),
            WindowKind::Agg { arg, .. } | WindowKind::Value { arg, .. } => vec![arg],
            WindowKind::LagLead { arg, default, .. } => match default {
                Some(default) => vec![arg, default],
                None => vec![arg],
            },
        }
    }

    /// Go `baseFuncDesc.TypeInfer` for this function, given its already
    /// resolved argument types.
    fn result_type(&self, arg_types: &[Option<FieldType>]) -> Result<FieldType, DriverError> {
        // The argument's own type, which every value function carries through
        // (Go's `typeInfer4MaxMin` tail: clone it and drop NOT NULL, since an
        // out-of-frame or out-of-range position is NULL).
        let carried = |index: usize| {
            let mut field_type = arg_types
                .get(index)
                .cloned()
                .flatten()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            field_type.del_flags(FieldTypeFlags::NOT_NULL);
            field_type
        };
        Ok(match self {
            WindowKind::RowNumber | WindowKind::Rank | WindowKind::DenseRank => {
                let mut field_type = FieldType::new(FieldTypeCode::LongLong);
                field_type.set_flen(21);
                field_type.set_decimal(0);
                field_type.add_flags(FieldTypeFlags::NOT_NULL);
                field_type
            }
            WindowKind::Ntile(_) => {
                // Go's `typeInfer4Ntile`: binary charset plus UNSIGNED, and
                // deliberately no NOT NULL (`NTILE(NULL)` is all NULLs).
                let mut field_type = FieldType::new(FieldTypeCode::LongLong);
                field_type.set_flen(21);
                field_type.set_decimal(0);
                field_type.add_flags(FieldTypeFlags::BINARY | FieldTypeFlags::UNSIGNED);
                field_type
            }
            // The aggregates share the GROUP BY path's inference, and with it
            // that path's documented deferral of Go's display metadata.
            WindowKind::Agg { name, .. } => {
                let placeholder = tidb_expr::expression::Expression::Constant(
                    tidb_expr::constant::Constant::new(Datum::Null, carried(0)),
                );
                crate::driver::agg_kind_and_type(name, &placeholder)?.1
            }
            WindowKind::Value { .. } => carried(0),
            // Go `typeInfer4LeadLag`: the argument's own type without a
            // written default, and the MERGE of the argument's and the
            // default's types with one.
            WindowKind::LagLead { default: None, .. } => carried(0),
            WindowKind::LagLead {
                default: Some(_), ..
            } => {
                let argument = carried(0);
                let merged = agg_field_type(&[argument.clone(), carried(1)]);
                // When the merge WIDENS past the argument's own type Go casts
                // both the argument and the default to it (`LAG(int_col, 1,
                // 'zz')` returns strings, not integers). That cast is a
                // separate unit, so the widening case is refused rather than
                // silently answered in the argument's own domain.
                if merged.code() != argument.code() {
                    return Err(DriverError::Unsupported(LAG_LEAD_CAST_MESSAGE));
                }
                merged
            }
        })
    }
}

/// Writes one partition's FRAMED (or, for `LAG`/`LEAD`, position-addressed)
/// values into `values`, at each row's own source position.
///
/// `indices` is the partition in window `ORDER BY` order; `arg_values` holds
/// each argument's per-row value indexed by SOURCE position.
fn evaluate_partition(
    call: &WindowCall,
    indices: &[usize],
    order_keys: &[Vec<Datum>],
    arg_values: &[Vec<Datum>],
    values: &mut [Datum],
) -> Result<(), DriverError> {
    let total = indices.len();
    // The peer group each position belongs to, as a half-open range. With no
    // window ORDER BY every key is empty, so the whole partition is one peer
    // group -- which is exactly why an unordered window frames the partition.
    let mut peers = vec![(0usize, total); total];
    let mut group_start = 0;
    for position in 1..=total {
        let ends =
            position == total || order_keys[indices[position]] != order_keys[indices[position - 1]];
        if ends {
            for entry in &mut peers[group_start..position] {
                *entry = (group_start, position);
            }
            group_start = position;
        }
    }

    let arg_at = |slot: usize, position: usize| arg_values[slot][indices[position]].clone();
    for position in 0..total {
        let target = indices[position];
        values[target] = match &call.kind {
            WindowKind::LagLead {
                is_lag,
                offset,
                default,
                ..
            } => {
                // LAG/LEAD address the sorted partition directly, ignoring the
                // frame entirely (confirmed against Go).
                let offset = i128::from(*offset);
                let signed = position as i128 + if *is_lag { -offset } else { offset };
                match usize::try_from(signed).ok().filter(|at| *at < total) {
                    Some(at) => arg_at(0, at),
                    // The default is a constant, so any row carries its value.
                    None if default.is_some() => arg_at(1, position),
                    None => Datum::Null,
                }
            }
            WindowKind::Agg { name, arg: _ } => {
                let (low, high) = call.frame.range(position, total, peers[position]);
                let kind = agg_kind(name);
                aggregate_values(&kind, (low..high).map(|at| Some(arg_at(0, at))))
                    .map_err(DriverError::Exec)?
            }
            WindowKind::Value { pick, .. } => {
                let (low, high) = call.frame.range(position, total, peers[position]);
                let at = match pick {
                    Pick::Last => high.checked_sub(1).filter(|at| *at >= low),
                    Pick::Nth(n) => usize::try_from(low as u128 + u128::from(*n) - 1)
                        .ok()
                        .filter(|at| *at < high),
                };
                // An empty frame, or a frame shorter than NTH_VALUE's
                // position, is NULL.
                at.map_or(Datum::Null, |at| arg_at(0, at))
            }
            WindowKind::RowNumber
            | WindowKind::Rank
            | WindowKind::DenseRank
            | WindowKind::Ntile(_) => unreachable!("the ranking functions take `rank_partition`"),
        };
    }
    Ok(())
}

/// The [`AggKind`] one aggregate name folds its frame with. `build_call` has
/// already refused every other name.
fn agg_kind(name: &str) -> AggKind {
    match name {
        "COUNT" => AggKind::Count,
        "SUM" => AggKind::Sum,
        "AVG" => AggKind::Avg,
        "MIN" => AggKind::Min,
        "MAX" => AggKind::Max,
        _ => unreachable!("build_call rejects every other aggregate name"),
    }
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

/// Evaluates a window function's own argument expressions for every row.
///
/// Unlike [`eval_keys`], which is indexed by row, the result is indexed by
/// ARGUMENT then row -- the shape the frame evaluator reads, since it walks
/// one argument across many rows rather than one row across many keys. The
/// second half is each argument's static type, which the result-type
/// inference needs.
#[allow(clippy::type_complexity)]
fn eval_args(
    exprs: &[Expr],
    rows: &[Vec<Datum>],
    field_types: &[FieldType],
    resolver: &impl ColumnResolver,
    ctx: &StmtContext,
) -> Result<(Vec<Vec<Datum>>, Vec<Option<FieldType>>), DriverError> {
    let mut values = Vec::with_capacity(exprs.len());
    let mut types = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let rewritten = rewrite_expr_resolved(expr, resolver)
            .map_err(|e| DriverError::Exec(crate::ExecError::Eval(e)))?;
        types.push(rewritten.static_type().cloned());
        let mut column = Vec::with_capacity(rows.len());
        for row in rows {
            let chunk = row_chunk(row, field_types)?;
            column.push(
                rewritten
                    .eval(ctx, chunk.get_row(0))
                    .map_err(|e| DriverError::Exec(crate::ExecError::Eval(e)))?,
            );
        }
        values.push(column);
    }
    Ok((values, types))
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
    kind: &WindowKind,
    indices: &[usize],
    order_keys: &[Vec<Datum>],
    values: &mut [Datum],
) {
    // Rows with no window `ORDER BY` are all peers of each other, which is
    // exactly what an empty key compares as.
    let peers = |left: usize, right: usize| order_keys[left] == order_keys[right];
    match kind {
        WindowKind::RowNumber => {
            for (position, index) in indices.iter().enumerate() {
                values[*index] = Datum::Int(position as i64 + 1);
            }
        }
        WindowKind::Rank => {
            let mut rank = 1i64;
            for (position, index) in indices.iter().enumerate() {
                if position > 0 && !peers(indices[position - 1], *index) {
                    rank = position as i64 + 1;
                }
                values[*index] = Datum::Int(rank);
            }
        }
        WindowKind::DenseRank => {
            let mut rank = 1i64;
            for (position, index) in indices.iter().enumerate() {
                if position > 0 && !peers(indices[position - 1], *index) {
                    rank += 1;
                }
                values[*index] = Datum::Int(rank);
            }
        }
        WindowKind::Ntile(buckets) => {
            let Some(buckets) = *buckets else {
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
        WindowKind::Agg { .. } | WindowKind::Value { .. } | WindowKind::LagLead { .. } => {
            unreachable!("the framed functions take `evaluate_partition`")
        }
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
