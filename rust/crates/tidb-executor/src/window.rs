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
//! Semantics confirmed against Go (`TestZZDumpWindow3` capture, since
//! removed) for the value-measured `RANGE` frame, the DISTRIBUTION functions
//! and the GROUPED shape:
//!
//! * `RANGE BETWEEN N PRECEDING AND N FOLLOWING` measures the boundary as a
//!   VALUE of the single `ORDER BY` key, so a TIE shares one frame and a GAP
//!   shrinks it: over `k = 1,3,3,7,8` with `v = 10..50`, `2 PRECEDING AND
//!   CURRENT ROW` is `10,60,60,40,90` -- the `k = 7` row sees only itself
//!   because nothing lies in `[5,7]`. `0 PRECEDING AND 0 FOLLOWING` is still
//!   the whole peer group, and a FRACTIONAL offset is legal (only `ROWS`
//!   demands an integer).
//! * Under `ORDER BY ... DESC` the sign FLIPS: `N PRECEDING` reaches the
//!   LARGER keys, which are the ones that sort earlier. Same fixture,
//!   `ORDER BY k DESC RANGE BETWEEN 2 PRECEDING AND CURRENT ROW`, the `k = 7`
//!   row sums `{8,7}` = `90`.
//! * NULL keys form a frame of their OWN: they peer with each other and with
//!   nothing else, so over `NULL,NULL,1,2,5` the two NULL rows see only each
//!   other and no real key ever includes them -- which falls out of NULL
//!   comparing below every value in both the boundary arithmetic (`NULL - 1`
//!   is `NULL`) and the boundary search.
//! * Go validates the `ORDER BY` shape in the planner and its errors OUTRANK
//!   everything else about the bound: not exactly one key, or a key that is
//!   neither numeric nor temporal, is 3587 -- even for an `INTERVAL` bound;
//!   a numeric bound over a temporal key is 3588; an `INTERVAL` bound over a
//!   numeric key is 3589.
//! * `PERCENT_RANK` is `(RANK() - 1) / (rows - 1)` and `CUME_DIST` is the
//!   fraction of the partition at or before the current PEER GROUP, so over
//!   `10,20,20,30` they are `0, 1/3, 1/3, 1` and `.25, .75, .75, 1`. A
//!   single-row partition is `PERCENT_RANK` 0 (not NaN) and `CUME_DIST` 1,
//!   and with NO `ORDER BY` every row is a peer, so every row is `0` / `1`.
//!   Both IGNORE the frame but still VALIDATE it.
//! * A window function combined with `GROUP BY` computes over the
//!   POST-aggregation rows, so its own `ORDER BY`/`PARTITION BY`/argument may
//!   name an aggregate (`RANK() OVER (ORDER BY SUM(v))`) and a `HAVING` that
//!   removed a group means the window never counts it.
//! * `LAG`/`LEAD` with a WIDENING default reads BOTH operands through the
//!   merged type, so `LAG(int_col, 1, 'zz')` returns the STRINGS `'zz'`,
//!   `'10'`, ... The value argument goes through the merged type's DOMAIN
//!   only ([`coerce_to_domain`]) while the default constant goes through the
//!   full type, which is why `LAG(int_col, 1, 1.5)` prints `1.5` and `10`
//!   rather than the scale-padded `10.0`. A NULL default does not widen at
//!   all (Go's `InferType4ControlFuncs` drops NULL-typed operands).
//!
//! Semantics confirmed against Go (`TestZZDumpWindow4` capture, since
//! removed) for the INTERVAL frame, the nested/rollup shapes, the extra
//! aggregates and the base-window edge:
//!
//! * A `RANGE` bound written `INTERVAL n unit` measures the frame in
//!   CALENDAR units over the temporal `ORDER BY` key: the boundary is the
//!   key moved by `DATE_ADD`/`DATE_SUB`'s own arithmetic, so `INTERVAL 1
//!   MONTH` is a month field increment rather than 30 days, and the boundary
//!   is INCLUSIVE (over `2020-01-01 00:00`, `2020-01-01 12:00`, two
//!   `2020-01-02 00:00` ties and `2020-01-05`, `INTERVAL 1 DAY PRECEDING`
//!   sums `10,30,100,100,50`). `DESC` flips the sign exactly as a numeric
//!   bound's does, a NULL key still frames only the other NULL keys, a
//!   composite unit (`INTERVAL '1 2' DAY_HOUR`) works, and a start ranking
//!   after its end is an empty frame. A `DATE` key reads as midnight, so an
//!   `INTERVAL 2 HOUR` bound reaches nothing before it.
//! * A window function may sit inside a LARGER select expression (`RANK()
//!   OVER (...) + 1`, `CONCAT('#', ROW_NUMBER() OVER w)`), including over a
//!   grouped query, where Go evaluates it in the projection ABOVE the window
//!   operator -- so an aggregate may appear both inside the window's spec and
//!   around the call (`SUM(v) + ROW_NUMBER() OVER (ORDER BY g)`).
//! * `GROUP BY ... WITH ROLLUP` combined with a window computes the window
//!   over the rollup's OUTPUT rows, supergroup rows included: a subtotal row
//!   joins the partition its own (non-NULLed) key names and the grand total
//!   sits alone in the all-NULL partition, so `SUM(SUM(v)) OVER (PARTITION
//!   BY a)` DOUBLE-counts each group against its own subtotal row (captured
//!   `60` for `a = 1`, whose rows total 30). `GROUPING()` is what tells a
//!   rollup NULL from a data NULL, and a window may partition by it.
//! * Go's window allowlist covers every aggregate except `GROUP_CONCAT`
//!   ("[planner:1235]... 'group_concat as window function'"), and DISTINCT
//!   inside any window call is 1235 too. `BIT_AND`/`BIT_OR`/`BIT_XOR` fold
//!   to the operator's IDENTITY over an empty or all-NULL frame rather than
//!   NULL -- and their result column is a SIGNED `BIGINT(21) NOT NULL`, so
//!   an all-NULL `BIT_AND` reads back as `-1`. The variance family shares
//!   Go's one incremental accumulator (`func_varpop.go`): the POPULATION
//!   forms (`VAR_POP`/`VARIANCE`, `STDDEV_POP`/`STDDEV`/`STD`) divide by the
//!   frame's row count and are `0` for a single row, the SAMPLE forms
//!   (`VAR_SAMP`, `STDDEV_SAMP`) divide by `count - 1` and are NULL there;
//!   all four are a nullable `DOUBLE(23)`.
//! * A named window may EXTEND another (`WINDOW w AS (PARTITION BY g), w2 AS
//!   (w ORDER BY v)`), in a chain and in either written order. `OVER w`
//!   USES a window rather than extending it, so a window with a frame may be
//!   used directly but never INHERITED (3582); an extension may not define
//!   partitioning (3581), may not add an `ORDER BY` its base already has
//!   (3583, naming the extending window), and a cycle is 3580.
//!
//! SLICE SCOPE: the ranking functions (frame-less, as above), the
//! distribution functions `PERCENT_RANK`/`CUME_DIST`, the value family
//! `LAG`/`LEAD`/`FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE`, and the aggregates
//! `SUM`/`COUNT`/`AVG`/`MIN`/`MAX`/`BIT_AND`/`BIT_OR`/`BIT_XOR`/`VAR_POP`/
//! `VAR_SAMP`/`STDDEV_POP`/`STDDEV_SAMP` (with `VARIANCE`/`STDDEV`/`STD` as
//! the parser's aliases) as window functions, over the default frame, an
//! explicit `ROWS BETWEEN`, or a `RANGE BETWEEN` with peer, numeric VALUE or
//! `INTERVAL` bounds -- alone, inside a larger select expression, over a
//! `GROUP BY`, or over a `GROUP BY ... WITH ROLLUP`. `JSON_ARRAYAGG`,
//! `JSON_OBJECTAGG`, `APPROX_COUNT_DISTINCT` and `APPROX_PERCENTILE` fold
//! their frame through the same accumulators the GROUP BY path uses, so a
//! frame applies to them exactly as it does to `SUM`.
//! `GROUP_CONCAT(...) OVER (...)` parses (the parser accepts an optional
//! `OVER` on `GROUP_CONCAT` just as Go's grammar does) and is refused HERE,
//! in `build_call`, with the same 1235 `'group_concat as window function'`
//! Go answers -- a plan-time rejection, not a parser-side one.
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
use crate::hash_agg::{aggregate_rows, AggKind};
use crate::StmtContext;
use std::any::Any;
use tidb_ast::{
    Expr, FrameBound, FrameKind, OrderItem, SelectField, SelectStmt, WindowDef, WindowFrame,
    WindowOver, WindowSpec,
};
use tidb_datatype::{Datum, EvalType, FieldType, FieldTypeCode, FieldTypeFlags, TimeType};
use tidb_expr::rewriter::{rewrite_expr_resolved, ColumnResolver};

/// What this build refuses, naming the slice it does implement.
pub(crate) const SLICE_MESSAGE: &str =
    "only the ranking (ROW_NUMBER, RANK, DENSE_RANK, NTILE), value (LAG, LEAD, \
     FIRST_VALUE, LAST_VALUE, NTH_VALUE) and aggregate (SUM, COUNT, AVG, MIN, MAX) \
     window functions are supported";

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
    /// `PERCENT_RANK()`: `(RANK() - 1) / (rows - 1)`, and `0` for a
    /// single-row partition (Go's `func_percent_rank.go`).
    PercentRank,
    /// `CUME_DIST()`: the fraction of the partition at or before the current
    /// row's PEER GROUP (Go's `func_cume_dist.go`).
    CumeDist,
    /// `NTILE(n)`; `None` is `NTILE(NULL)`, whose result is `NULL` everywhere.
    Ntile(Option<u64>),
    /// `SUM`/`COUNT`/`AVG`/`MIN`/`MAX` over the frame. `arg` is the argument
    /// as written -- `COUNT(*)`'s is the literal `1` the parser substitutes,
    /// so no absent-argument case survives here.
    Agg {
        /// The uppercase aggregate name.
        name: String,
        /// The argument expressions: one for most aggregates, two for
        /// `JSON_OBJECTAGG(key, value)` and `APPROX_PERCENTILE(v, pct)`, and
        /// any number for `APPROX_COUNT_DISTINCT(a, b, ...)`.
        args: Vec<Expr>,
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
    /// `RANGE BETWEEN start AND end` where at least one bound carries a VALUE
    /// offset, so the boundary is a VALUE of the single `ORDER BY` key rather
    /// than a position: the frame holds every row whose key lies within the
    /// offset of the current row's key.
    Range {
        /// The starting boundary.
        start: RangeBound,
        /// The ending boundary.
        end: RangeBound,
    },
}

/// One boundary of a value-measured `RANGE` frame.
#[derive(Clone)]
enum RangeBound {
    /// The partition's first row.
    UnboundedPreceding,
    /// The current row's whole PEER GROUP (its first row as a start bound,
    /// one past its last as an end bound).
    CurrentRow,
    /// A key value `offset` BEFORE the current row's, in the window's own
    /// direction -- under `ORDER BY ... DESC` "before" means a LARGER key.
    Preceding(Offset),
    /// A key value `offset` AFTER the current row's, in the same sense.
    Following(Offset),
    /// The partition's last row.
    UnboundedFollowing,
}

/// A `RANGE` bound's offset as written.
#[derive(Clone)]
enum Offset {
    /// A plain numeric constant, which only a numeric `ORDER BY` key accepts.
    Value(Datum),
    /// `INTERVAL n unit`, which only a temporal `ORDER BY` key accepts: the
    /// boundary is the current row's key moved by that many calendar units.
    Interval {
        /// The interval's magnitude, already folded to a constant.
        amount: Datum,
        /// The unit keyword as the parser canonicalizes it (`DAY`, `MONTH`,
        /// `DAY_HOUR`, ...).
        unit: String,
    },
}

impl RangeBound {
    /// Whether this bound is written as `INTERVAL n unit`, which is what
    /// decides between Go's 3588 and 3589 against the key's own type.
    fn is_interval(&self) -> bool {
        matches!(
            self,
            RangeBound::Preceding(Offset::Interval { .. })
                | RangeBound::Following(Offset::Interval { .. })
        )
    }
}

impl Frame {
    /// The half-open `[start, end)` position range this frame covers for the
    /// row at `position` of a partition of `total` rows, whose own peer group
    /// spans the half-open range `peers`.
    ///
    /// An empty frame comes back as an empty range; every out-of-partition
    /// offset is clamped, so `ROWS BETWEEN 1 PRECEDING AND CURRENT ROW` at the
    /// partition's first row is `[0, 1)` rather than an error.
    fn range(
        &self,
        position: usize,
        total: usize,
        peers: (usize, usize),
        keys: Option<&RangeKeys<'_>>,
    ) -> Result<(usize, usize), DriverError> {
        if let Frame::Range { start, end } = self {
            let keys = keys.expect("a value-measured RANGE frame carries its sorted key column");
            return keys.bounds(start, end, position, total, peers);
        }
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
            Frame::Range { .. } => unreachable!("handled above"),
        };
        let low = low.clamp(0, total_i) as usize;
        let high = high.clamp(0, total_i) as usize;
        Ok((low, high.max(low)))
    }
}

/// The single `ORDER BY` key a value-measured `RANGE` frame is computed
/// against, in the partition's sorted order, plus that order's direction.
struct RangeKeys<'a> {
    /// The key value at each sorted partition position.
    keys: &'a [Datum],
    /// Whether the window's own `ORDER BY` is descending, which flips what
    /// `PRECEDING` means: under `DESC` the preceding rows hold LARGER keys.
    desc: bool,
    /// The key's own derived collation, which decides which keys the boundary
    /// seek has already passed. Only a string key can reach here at all --
    /// [`check_range_key`] refuses a non-numeric, non-temporal key -- so this
    /// is `utf8mb4_bin` in practice, but it is the key's OWN collation rather
    /// than a hard-coded one for the same reason the peer groups are.
    collation: tidb_datatype::Collation,
}

impl RangeKeys<'_> {
    /// The frame's half-open `[start, end)` position range, following Go's
    /// `rangeFrameWindowProcessor`: a boundary is the first sorted position
    /// whose key has passed the boundary VALUE (the current row's key moved
    /// by the offset), so ties on the key are naturally peer-inclusive and a
    /// gap in the key values simply yields a shorter frame.
    fn bounds(
        &self,
        start: &RangeBound,
        end: &RangeBound,
        position: usize,
        total: usize,
        peers: (usize, usize),
    ) -> Result<(usize, usize), DriverError> {
        let low = match start {
            RangeBound::UnboundedPreceding => 0,
            RangeBound::CurrentRow => peers.0,
            RangeBound::UnboundedFollowing => total,
            RangeBound::Preceding(offset) | RangeBound::Following(offset) => {
                let bound = self.boundary_value(
                    position,
                    offset,
                    matches!(start, RangeBound::Preceding(_)),
                )?;
                // Go's `getStartOffset`: skip while the key still lies BEFORE
                // the boundary in the window's direction; the first key that
                // has reached it starts the frame.
                self.seek(total, |key| self.before(key, &bound))?
            }
        };
        let high = match end {
            RangeBound::UnboundedPreceding => 0,
            RangeBound::CurrentRow => peers.1,
            RangeBound::UnboundedFollowing => total,
            RangeBound::Preceding(offset) | RangeBound::Following(offset) => {
                let bound =
                    self.boundary_value(position, offset, matches!(end, RangeBound::Preceding(_)))?;
                // Go's `getEndOffset`: the frame ends at the first key that
                // lies strictly PAST the boundary.
                self.seek(total, |key| Ok(!self.before(&bound, key)?))?
            }
        };
        Ok((low, high.max(low)))
    }

    /// The boundary VALUE for the row at `position`: its own key moved by
    /// `offset`, in the direction `preceding` names. Under `DESC` the sign
    /// flips, which is exactly what makes `2 PRECEDING` reach the LARGER keys
    /// that sort earlier.
    fn boundary_value(
        &self,
        position: usize,
        offset: &Offset,
        preceding: bool,
    ) -> Result<Datum, DriverError> {
        let key = &self.keys[position];
        if key.is_null() {
            // Go's calc function propagates NULL, and a NULL boundary
            // compares below every real key -- which is why the NULL rows
            // form a frame of their own.
            return Ok(Datum::Null);
        }
        // `PRECEDING` moves the key BACKWARDS along the window's own
        // direction, so under `DESC` it moves FORWARDS in value -- the one
        // rule both offset kinds share.
        let subtract = preceding != self.desc;
        match offset {
            Offset::Value(offset) => {
                let op = if subtract {
                    tidb_ast::BinaryOp::Minus
                } else {
                    tidb_ast::BinaryOp::Plus
                };
                tidb_expr::apply_binary(op, key.clone(), offset.clone())
            }
            // Go's `getIntervalBoundValue` builds a `DATE_ADD`/`DATE_SUB`
            // call over the key, so the boundary follows the SAME calendar
            // arithmetic -- `INTERVAL 1 MONTH` is a month field increment,
            // not 30 days.
            Offset::Interval { amount, unit } => {
                tidb_expr::date_add_interval(unit, key, amount, if subtract { -1 } else { 1 })
            }
        }
        .map_err(|e| DriverError::Exec(crate::ExecError::Eval(e)))
    }

    /// Whether `left` lies before `right` in the window's own direction.
    fn before(&self, left: &Datum, right: &Datum) -> Result<bool, DriverError> {
        let ordering = tidb_expr::compare_datums_with_collation(left, right, self.collation)
            .map_err(|e| DriverError::Exec(crate::ExecError::Eval(e)))?;
        Ok(if self.desc {
            ordering.is_gt()
        } else {
            ordering.is_lt()
        })
    }

    /// The first position whose key stops satisfying `skip`, or `total`.
    fn seek(
        &self,
        total: usize,
        skip: impl Fn(&Datum) -> Result<bool, DriverError>,
    ) -> Result<usize, DriverError> {
        for position in 0..total {
            if !skip(&self.keys[position])? {
                return Ok(position);
            }
        }
        Ok(total)
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
    check_frame_shape(start, end)?;
    if matches!(kind, FrameKind::Range) && (has_offset(start) || has_offset(end)) {
        // A value-measured RANGE frame: its offsets are key VALUES, so they
        // are neither counted nor required to be integers, and Go validates
        // the ORDER BY shape before anything else about them.
        let range_start = build_range_bound(start)?;
        let range_end = build_range_bound(end)?;
        if spec.order_by.len() != 1 {
            return Err(DriverError::WindowRangeFrameOrderType);
        }
        return Ok(Frame::Range {
            start: range_start,
            end: range_end,
        });
    }
    let start = build_bound(start)?;
    let end = build_bound(end)?;
    match kind {
        FrameKind::Rows => Ok(Frame::Rows { start, end }),
        // Only peer-based bounds: the default frame, written out.
        FrameKind::Range => Ok(Frame::Peers { start, end }),
    }
}

/// Go's `checkOriginWindowSpec` frame-shape rules, in Go's own order and
/// BEFORE any bound's offset is folded -- which is what decides the error for
/// `ROWS BETWEEN UNBOUNDED FOLLOWING AND 1.5 FOLLOWING` (3584, not 3586).
///
/// These four rules replace a `start` / `end` rank comparison: they cover
/// every rank inversion AND the two rank-EQUAL frames a rank test cannot see
/// -- `UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING` and `UNBOUNDED PRECEDING
/// AND UNBOUNDED PRECEDING`, both of which Go refuses and which otherwise
/// silently produce an empty frame (NULL) for every row.
fn check_frame_shape(start: &FrameBound, end: &FrameBound) -> Result<(), DriverError> {
    if matches!(start, FrameBound::UnboundedFollowing) {
        return Err(DriverError::WindowFrameStartIllegal);
    }
    if matches!(end, FrameBound::UnboundedPreceding) {
        return Err(DriverError::WindowFrameEndIllegal);
    }
    let start_following = matches!(start, FrameBound::Following(_));
    let end_preceding = matches!(end, FrameBound::Preceding(_));
    if start_following && (end_preceding || matches!(end, FrameBound::CurrentRow)) {
        return Err(DriverError::WindowFrameIllegal);
    }
    if (start_following || matches!(start, FrameBound::CurrentRow)) && end_preceding {
        return Err(DriverError::WindowFrameIllegal);
    }
    Ok(())
}

/// Whether a frame bound carries any offset expression at all, which is what
/// separates a value-measured `RANGE` frame from the peer-based one.
fn has_offset(bound: &FrameBound) -> bool {
    matches!(bound, FrameBound::Preceding(_) | FrameBound::Following(_))
}

/// Folds one value-measured `RANGE` bound's offset.
///
/// Go requires a constant here and the parser has already rejected a column
/// reference, so the only shapes left are numeric literals and `INTERVAL n
/// unit` -- the latter carried as [`Offset::Interval`] rather than refused
/// outright, because the `ORDER BY` key's own type decides which of Go's
/// errors (3587/3588/3589) wins over the refusal.
fn build_range_bound(bound: &FrameBound) -> Result<RangeBound, DriverError> {
    let offset = |expr: &Expr| -> Result<Offset, DriverError> {
        Ok(match expr {
            // `INTERVAL '1 2' DAY_HOUR` carries a STRING magnitude, which
            // `DATE_ADD` parses per unit, so the amount is folded to a datum
            // rather than a number here.
            Expr::Interval { value, unit } => Offset::Interval {
                amount: match value.as_ref() {
                    Expr::Int(text) => Datum::Int(
                        text.parse::<i64>()
                            .map_err(|_| DriverError::WindowFrameIllegal)?,
                    ),
                    Expr::String(text) => Datum::new_string(text.clone()),
                    Expr::Decimal(text) => {
                        Datum::Decimal(tidb_datatype::Decimal::from_literal(text))
                    }
                    _ => return Err(DriverError::WindowFrameIllegal),
                },
                unit: unit.clone(),
            },
            Expr::Int(text) => Offset::Value(Datum::Int(
                text.parse::<i64>()
                    .map_err(|_| DriverError::WindowFrameIllegal)?,
            )),
            Expr::Decimal(text) => {
                Offset::Value(Datum::Decimal(tidb_datatype::Decimal::from_literal(text)))
            }
            Expr::Float(value) => Offset::Value(Datum::Real(*value)),
            _ => return Err(DriverError::WindowFrameIllegal),
        })
    };
    Ok(match bound {
        FrameBound::UnboundedPreceding => RangeBound::UnboundedPreceding,
        FrameBound::CurrentRow => RangeBound::CurrentRow,
        FrameBound::UnboundedFollowing => RangeBound::UnboundedFollowing,
        FrameBound::Preceding(expr) => RangeBound::Preceding(offset(expr)?),
        FrameBound::Following(expr) => RangeBound::Following(offset(expr)?),
    })
}

/// Go's `checkOriginWindowFrameBound` tail for a value-measured `RANGE`
/// frame, which needs the `ORDER BY` key's own type and therefore runs once
/// the source scope is known rather than at build time.
///
/// The refusal of an `INTERVAL` bound comes LAST, so Go's own errors win: a
/// string key with an `INTERVAL` bound is 3587, not the refusal.
fn check_range_key(frame: &Frame, key_type: Option<&FieldType>) -> Result<(), DriverError> {
    let Frame::Range { start, end } = frame else {
        return Ok(());
    };
    let code = key_type
        .map(FieldType::code)
        .ok_or(DriverError::WindowRangeFrameOrderType)?;
    let (numeric, temporal) = (code.is_type_numeric(), code.is_type_temporal());
    if !numeric && !temporal {
        return Err(DriverError::WindowRangeFrameOrderType);
    }
    for bound in [start, end] {
        if bound.is_interval() && !temporal {
            return Err(DriverError::WindowRangeFrameNumericType);
        }
        let has_offset = matches!(bound, RangeBound::Preceding(_) | RangeBound::Following(_));
        if !bound.is_interval() && has_offset && !numeric {
            return Err(DriverError::WindowRangeFrameTemporalType);
        }
    }
    Ok(())
}

/// Collects every `Expr::Window` node inside `expr`, in written order.
pub(crate) fn windows_in(expr: &Expr) -> Vec<Expr> {
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
    match over {
        // `OVER w` USES the named window rather than extending it, so the
        // restrictions on an extension (3581/3582/3583) do not apply -- a
        // window with a frame may be used directly, only not inherited.
        WindowOver::Name(name) => {
            let def = named
                .iter()
                .find(|(have, _)| have.eq_ignore_ascii_case(name))
                .map(|(_, def)| def)
                .ok_or_else(|| DriverError::WindowNoSuchWindow(name.clone()))?;
            resolve_def(def, name, named, &mut vec![name.clone()])
        }
        // An inline `OVER (w ...)` has no name of its own, which Go reports
        // as `<unnamed window>` in its messages.
        WindowOver::Def(def) => resolve_def(def, UNNAMED_WINDOW, named, &mut Vec::new()),
    }
}

/// How Go names a window written inline in an `OVER (...)` clause.
const UNNAMED_WINDOW: &str = "<unnamed window>";

/// Resolves one definition, following its `base` chain. `seen` carries the
/// names already on the chain so a cycle stops instead of recursing forever.
fn resolve_def(
    def: &WindowDef,
    def_name: &str,
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
    let base = resolve_def(base_def, base_name, named, seen)?;
    if !def.spec.partition_by.is_empty() {
        return Err(DriverError::WindowNoChildPartitioning);
    }
    if !def.spec.order_by.is_empty() && !base.order_by.is_empty() {
        return Err(DriverError::WindowNoRedefineOrderBy {
            window: def_name.to_owned(),
            base: base_name.clone(),
        });
    }
    // Go `mergeWindowSpec`: a base that defines a frame cannot be referenced
    // at all, so a resolved base never carries one and the extension's own
    // frame is the only one left.
    if base.frame.is_some() {
        return Err(DriverError::WindowNoInheritFrame(base_name.clone()));
    }
    Ok(WindowSpec {
        partition_by: base.partition_by,
        order_by: if def.spec.order_by.is_empty() {
            base.order_by
        } else {
            def.spec.order_by.clone()
        },
        frame: def.spec.frame.clone(),
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

/// Whether `name` is one of the synthetic columns a computed window call
/// lands in, which the aggregate path's own clause rewriting must leave
/// alone.
pub(crate) fn is_window_column(name: &str) -> bool {
    name.starts_with(WINDOW_COLUMN_PREFIX)
}

/// The call index one synthetic window column names.
pub(crate) fn window_column_index(name: &str) -> Option<usize> {
    name.strip_prefix(WINDOW_COLUMN_PREFIX)?.parse().ok()
}

/// Hoists every window call out of `select`'s select list and `ORDER BY`,
/// leaving a reference to its computed column behind.
///
/// This is the aggregate path's entry: the window stage there runs over the
/// aggregation's OUTPUT rows, so every expression INSIDE a window call
/// (`RANK() OVER (ORDER BY SUM(v))`) must first be rewritten to read that
/// aggregation's own output columns -- which is what `substitute` does.
///
/// The returned statement's window nodes are replaced by
/// `__window_<i>` column references, so the caller's ordinary
/// aggregate-output machinery handles them like any other column.
pub(crate) fn hoist_windows(
    select: &SelectStmt,
    mut substitute: impl FnMut(&Expr) -> Result<Expr, DriverError>,
) -> Result<(Vec<WindowCall>, SelectStmt), DriverError> {
    let mut originals: Vec<Expr> = Vec::new();
    for expr in window_bearing_exprs(select) {
        for node in windows_in(expr) {
            if !originals.contains(&node) {
                originals.push(node);
            }
        }
    }
    // A named window's own specification names the same aggregation output
    // columns, so it is substituted once here rather than per reference.
    let mut resolved_select = select.clone();
    for (_, def) in &mut resolved_select.windows {
        substitute_spec(&mut def.spec, &mut substitute)?;
    }
    let mut calls = Vec::with_capacity(originals.len());
    for node in &originals {
        let mut substituted = node.clone();
        let Expr::Window { args, over, .. } = &mut substituted else {
            unreachable!("windows_in only yields Expr::Window");
        };
        for arg in args.iter_mut() {
            *arg = substitute(arg)?;
        }
        if let WindowOver::Def(def) = over {
            substitute_spec(&mut def.spec, &mut substitute)?;
        }
        calls.push(build_call(substituted, &resolved_select)?);
    }
    let mut rewritten = select.clone();
    let mut replacer = NodeReplacer {
        originals: &originals,
    };
    for field in rewritten.fields.fields_mut() {
        if let SelectField::Expr { expr, .. } = field {
            tidb_ast::Visitable::accept(expr, &mut replacer);
        }
    }
    for item in &mut rewritten.order_by {
        tidb_ast::Visitable::accept(&mut item.expr, &mut replacer);
    }
    Ok((calls, rewritten))
}

/// Runs `substitute` over every expression of one window specification.
fn substitute_spec(
    spec: &mut WindowSpec,
    substitute: &mut impl FnMut(&Expr) -> Result<Expr, DriverError>,
) -> Result<(), DriverError> {
    for expr in &mut spec.partition_by {
        *expr = substitute(expr)?;
    }
    for item in &mut spec.order_by {
        item.expr = substitute(&item.expr)?;
    }
    Ok(())
}

/// Replaces each window node of `originals` with its computed column.
struct NodeReplacer<'a> {
    originals: &'a [Expr],
}

impl tidb_ast::Visitor for NodeReplacer<'_> {
    fn enter(&mut self, node: &mut dyn Any) -> bool {
        let Some(expr) = node.downcast_mut::<Expr>() else {
            return false;
        };
        if !matches!(expr, Expr::Window { .. }) {
            return false;
        }
        if let Some(index) = self.originals.iter().position(|node| node == expr) {
            *expr = Expr::Column(vec![window_column_name(index)]);
        }
        true
    }

    fn leave(&mut self, _node: &mut dyn Any) -> bool {
        true
    }
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
    // Go `checkWindowFuncArgs`: `GROUP_CONCAT` is the one aggregate its
    // window allowlist never accepts, checked by NAME before the DISTINCT
    // check below (captured: "[planner:1235]This version of TiDB doesn't
    // yet support 'group_concat as window function'"). The parser accepts
    // `GROUP_CONCAT(...) OVER (...)` -- this is a plan-time rejection, not
    // a parse error, matching Go exactly.
    if name.eq_ignore_ascii_case("GROUP_CONCAT") {
        return Err(DriverError::NotSupportedYet(
            "group_concat as window function",
        ));
    }
    // Go `checkOriginWindowFuncs`: DISTINCT inside a window call is refused
    // outright, whatever the function (captured: "[planner:1235]This version
    // of TiDB doesn't yet support '<window function>(DISTINCT ..)'").
    if *distinct {
        return Err(DriverError::NotSupportedYet(
            "<window function>(DISTINCT ..)",
        ));
    }
    if *ignore_nulls || *from_last {
        return Err(DriverError::unsupported(SLICE_MESSAGE));
    }
    // A ranking function takes no arguments; Go's parser already enforces
    // that, so a stray one here is out of this slice.
    let no_args = |kind: WindowKind| {
        if args.is_empty() {
            Ok(kind)
        } else {
            Err(DriverError::unsupported(SLICE_MESSAGE))
        }
    };
    let upper = name.to_uppercase();
    let kind = match upper.as_str() {
        "ROW_NUMBER" => no_args(WindowKind::RowNumber)?,
        "RANK" => no_args(WindowKind::Rank)?,
        "DENSE_RANK" => no_args(WindowKind::DenseRank)?,
        "PERCENT_RANK" => no_args(WindowKind::PercentRank)?,
        "CUME_DIST" => no_args(WindowKind::CumeDist)?,
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
        "SUM" | "COUNT" | "AVG" | "MIN" | "MAX" | "BIT_AND" | "BIT_OR" | "BIT_XOR" | "VAR_POP"
        | "VAR_SAMP" | "STDDEV_POP" | "STDDEV_SAMP" | "JSON_ARRAYAGG" => {
            // `COUNT(*)` reaches here as `COUNT(1)`, so one argument is the
            // only shape; `COUNT(DISTINCT a, b)` already failed on `distinct`.
            let [arg] = args.as_slice() else {
                return Err(DriverError::unsupported(SLICE_MESSAGE));
            };
            WindowKind::Agg {
                name: upper.clone(),
                args: vec![arg.clone()],
            }
        }
        // The two-argument aggregates (the parser has already fixed
        // `JSON_OBJECTAGG`'s arity) and the variadic one.
        "JSON_OBJECTAGG" | "APPROX_PERCENTILE" | "APPROX_COUNT_DISTINCT" => {
            if args.is_empty() {
                return Err(DriverError::unsupported(SLICE_MESSAGE));
            }
            WindowKind::Agg {
                name: upper.clone(),
                args: args.clone(),
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
                _ => return Err(DriverError::unsupported(SLICE_MESSAGE)),
            };
            WindowKind::Value {
                arg: arg.clone(),
                pick,
            }
        }
        "LAG" | "LEAD" => {
            let (arg, rest) = args
                .split_first()
                .ok_or(DriverError::unsupported(SLICE_MESSAGE))?;
            // Go's `NewWindowFuncDesc` requires a non-negative integer
            // constant offset; the parser has already rejected a negative
            // one. UNLIKE `NTILE`/`NTH_VALUE`, Go's OWN grammar restricts
            // `LAG`/`LEAD`'s second argument to a `NumLiteral` (unsigned
            // int/float literal or a param marker) -- see
            // `pkg/parser/expr_func_parser.go`'s `parseLeadLagFuncCall`.
            // A boolean literal can never reach `GetUint64FromConstant`
            // here at all; `LAG(k, TRUE)` is a Go-side SQL syntax error
            // (1064), confirmed against real TiDB, not a semantic one. So
            // this site does NOT get the shared `constant_uint` boolean
            // reader: doing so would make this engine silently ACCEPT a
            // query Go rejects at parse time.
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
        _ => return Err(DriverError::unsupported(SLICE_MESSAGE)),
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

/// Reads a constant the way Go's `expression.GetUint64FromConstant` does:
/// `NULL`, a non-negative integer literal, or a boolean literal. Go's parser
/// lowers `TRUE`/`FALSE` to `Int64` constants `1`/`0` (the same shape as
/// `GROUP BY TRUE`), so every site that reads a constant integer window
/// argument must accept a boolean literal exactly like an integer one.
///
/// Returns `None` when `arg` is not a constant Go would accept here at all;
/// `Some(None)` for `NULL`; `Some(Some(n))` for a resolved value.
fn constant_uint(arg: &Expr) -> Option<Option<u64>> {
    match arg {
        Expr::Null => Some(None),
        Expr::Bool(value) => Some(Some(u64::from(*value))),
        Expr::Int(text) => text.parse::<u64>().ok().map(Some),
        _ => None,
    }
}

/// Reads `NTILE`'s (and `NTH_VALUE`'s position) bucket count from a constant
/// argument, or `None` when the argument is not a constant Go would accept,
/// or resolves to `0` (Go rejects `0` but allows `NULL`).
fn constant_bucket_count(arg: &Expr) -> Option<BucketCount> {
    match constant_uint(arg)? {
        None => Some(BucketCount::Null),
        Some(0) => None,
        Some(count) => Some(BucketCount::Positive(count)),
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
        func_deps: Default::default(),
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
    let (arg_values, arg_types, rewritten_args) =
        eval_args(&arg_exprs, rows, field_types, resolver, ctx)?;
    let result_type = call.kind.result_type(&arg_types, &rewritten_args)?;
    // An aggregate's fold is resolved once, from the same name-and-arguments
    // pair the GROUP BY path uses, so it can never differ between the two
    // surfaces (and `APPROX_PERCENTILE`'s constant percentage is read here,
    // exactly once, rather than per frame).
    let agg_kind = match &call.kind {
        WindowKind::Agg { name, .. } => {
            Some(crate::driver::agg_kind_and_type(name, &rewritten_args)?.0)
        }
        _ => None,
    };

    let (partition_keys, partition_collations) =
        eval_keys(&call.spec.partition_by, rows, field_types, resolver, ctx)?;
    let order_exprs: Vec<Expr> = call
        .spec
        .order_by
        .iter()
        .map(|item: &OrderItem| item.expr.clone())
        .collect();
    let (order_keys, order_collations) = eval_keys(&order_exprs, rows, field_types, resolver, ctx)?;

    // A value-measured RANGE frame needs the single ORDER BY key's own type,
    // which is only known here -- Go checks it in the planner, so it fires
    // before any row is produced either way.
    if matches!(call.frame, Frame::Range { .. }) {
        let key_type = match order_exprs.first() {
            Some(expr) => rewrite_expr_resolved(expr, resolver)
                .map_err(|e| DriverError::Exec(crate::ExecError::Eval(e)))?
                .static_type()
                .cloned(),
            None => None,
        };
        check_range_key(&call.frame, key_type.as_ref())?;
    }

    // Partition on the hash encoding of the key datums, exactly as the hash
    // aggregation groups rows, keeping each partition's rows in source order.
    // A STRING key contributes its COLLATION's sort key, which is what makes
    // `PARTITION BY ci_col` over `a, A, b, B` produce two partitions rather
    // than four (Go's `VecGroupChecker` compares `ConvertByCollationStr`).
    let mut partitions: std::collections::HashMap<Vec<u8>, Vec<usize>> =
        std::collections::HashMap::new();
    for (index, key) in partition_keys.iter().enumerate() {
        let mut encoded = Vec::new();
        for (datum, collation) in key.iter().zip(&partition_collations) {
            encoded.extend_from_slice(&crate::hash_agg::group_key_part(collation, datum));
            encoded.push(0xff); // separator: key parts are length-coded
        }
        partitions.entry(encoded).or_default().push(index);
    }

    // Go `buildLeadLag` (`aggfuncs/builder.go:823-831`) converts a CONSTANT
    // default to the merged type ONCE, at build time
    // (`et.Value.ConvertTo(evalCtx.TypeCtx(), aggFuncDesc.RetTp)`); a default
    // that is NOT a constant is never converted at all -- it is read through
    // the evaluator exactly like the value argument, and therefore keeps its
    // own temporal kind and scale. Both halves are captured from TiDB:
    // `LAG(datetime6_col, 1, datetime0_col)` answers `2020-01-01 10:20:30`
    // for the out-of-range row rather than the merged type's `.000000`, while
    // `LAG(decimal_10_2_col, 1, 1.5)` answers the scale-padded `1.50`.
    let constant_default = rewritten_args.get(1).is_none_or(column_free);

    let mut values = vec![Datum::Null; rows.len()];
    for indices in partitions.values_mut() {
        sort_partition(indices, &order_keys, &order_collations, &call.spec.order_by)?;
        let peers = peer_groups(indices, &order_keys, &order_collations)?;
        // The sorted single ORDER BY key a value-measured RANGE frame is
        // computed against, under that key's own collation; the other frame
        // kinds never look at it.
        let range_column: Option<Vec<Datum>> =
            matches!(call.frame, Frame::Range { .. }).then(|| {
                indices
                    .iter()
                    .map(|index| order_keys[*index][0].clone())
                    .collect()
            });
        let range_keys = range_column.as_ref().map(|keys| RangeKeys {
            keys,
            desc: call.spec.order_by[0].desc,
            collation: order_collations
                .first()
                .copied()
                .unwrap_or(tidb_datatype::Collation::DEFAULT),
        });
        match &call.kind {
            WindowKind::RowNumber
            | WindowKind::Rank
            | WindowKind::DenseRank
            | WindowKind::PercentRank
            | WindowKind::CumeDist
            | WindowKind::Ntile(_) => rank_partition(&call.kind, indices, &peers, &mut values),
            _ => evaluate_partition(
                call,
                indices,
                &PartitionGeometry {
                    peers: &peers,
                    range_keys,
                    constant_default,
                },
                &arg_values,
                agg_kind.as_ref().map(|kind| {
                    (
                        kind,
                        tidb_expr::Columns::div_precision_increment(ctx),
                        rewritten_args.first().map_or(
                            tidb_datatype::Collation::DEFAULT,
                            tidb_expr::collation_derive::collation_of_node,
                        ),
                    )
                }),
                &result_type,
                &mut values,
            )?,
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
            | WindowKind::PercentRank
            | WindowKind::CumeDist
            | WindowKind::Ntile(_) => Vec::new(),
            WindowKind::Agg { args, .. } => args.iter().collect(),
            WindowKind::Value { arg, .. } => vec![arg],
            WindowKind::LagLead { arg, default, .. } => match default {
                Some(default) => vec![arg, default],
                None => vec![arg],
            },
        }
    }

    /// Go `baseFuncDesc.TypeInfer` for this function, given its already
    /// resolved argument types.
    fn result_type(
        &self,
        arg_types: &[Option<FieldType>],
        arg_exprs: &[tidb_expr::expression::Expression],
    ) -> Result<FieldType, DriverError> {
        // Go `typeInfer4MaxMin` (`aggregation/base_func.go:361-384`), which
        // every window VALUE function shares -- `FIRST_VALUE`, `LAST_VALUE`,
        // `NTH_VALUE`, and `LEAD`/`LAG` without a written default. Its head
        // rewrites the ARGUMENT (a scalar `FLOAT` is cast to `DOUBLE`) and so
        // lives where arguments are built; its tail is the result type:
        //
        // ```go
        // a.RetTp = a.Args[0].GetType(ctx.GetEvalCtx())
        // if a.Name == ast.AggFuncMax || ... || a.Name == ast.WindowFuncLag {
        //     a.RetTp = a.Args[0].GetType(ctx.GetEvalCtx()).Clone()
        //     a.RetTp.DelFlag(mysql.NotNullFlag)
        // }
        // // issue #13027, #13961
        // if (a.RetTp.GetType() == mysql.TypeEnum || a.RetTp.GetType() == mysql.TypeSet) &&
        //     (a.Name != ast.AggFuncFirstRow && a.Name != ast.AggFuncMax && a.Name != ast.AggFuncMin) {
        //     a.RetTp = types.NewFieldTypeBuilder().SetType(mysql.TypeString).SetFlen(mysql.MaxFieldCharLength).BuildP()
        // }
        // ```
        //
        // NOT NULL goes because an out-of-frame or out-of-range position is
        // NULL. The ENUM/SET rewrite is UNCONDITIONAL here: the three names
        // Go excludes -- `FIRSTROW`, `MAX`, `MIN` -- are aggregates, and none
        // of them reaches this closure (the windowed `MAX`/`MIN` take the
        // `WindowKind::Agg` arm, which is why `MAX(enum_col) OVER ()` still
        // reports `enum('x','y','z')` where `LAG(enum_col, 1)` reports
        // `char(255)`; both captured from TiDB via `gorun`).
        //
        // Go's `NewFieldTypeBuilder` starts from a ZERO field type, so the
        // rewritten result keeps a scale of 0 rather than an unspecified one.
        let carried = |index: usize| {
            let mut field_type = arg_types
                .get(index)
                .cloned()
                .flatten()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            field_type.del_flags(FieldTypeFlags::NOT_NULL);
            if matches!(field_type.code(), FieldTypeCode::Enum | FieldTypeCode::Set) {
                let mut rewritten = FieldType::new(FieldTypeCode::String);
                // `mysql.MaxFieldCharLength`.
                rewritten.set_flen(255);
                rewritten.set_decimal(0);
                return rewritten;
            }
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
            // Go's `typeInfer4PercentRank` / `typeInfer4CumeDist`: a NOT NULL
            // `DOUBLE` with no fixed scale. The two differ only in `flen`,
            // because Go's percent-rank inference writes the real width into
            // the FLAG field rather than the length -- captured as written.
            WindowKind::PercentRank | WindowKind::CumeDist => {
                let mut field_type = FieldType::new(FieldTypeCode::Double);
                field_type.set_flen(if matches!(self, WindowKind::CumeDist) {
                    23
                } else {
                    tidb_datatype::UNSPECIFIED_LENGTH
                });
                field_type.set_decimal(tidb_datatype::UNSPECIFIED_FSP);
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
            // that path's documented deferral of Go's display metadata --
            // and then Go rewrites the NULLABILITY on top of it.
            //
            // `aggregation.NewWindowFuncDesc` (`window_func.go:62-76`) ends
            // with a switch whose listed arm calls `RetTp.SetFlag(NotNullFlag)`
            // and whose default calls `DelFlag(NotNullFlag)`. `SetFlag`
            // REPLACES the whole mask, so a windowed BIT_AND loses the
            // UnsignedFlag its grouped twin keeps -- which is why the same
            // all-NULL identity prints `18446744073709551615` when grouped
            // and `-1` in a window (both captured from TiDB).
            WindowKind::Agg { name, .. } => {
                let mut field_type = crate::driver::agg_kind_and_type(name, arg_exprs)?.1;
                if matches!(
                    name.to_ascii_uppercase().as_str(),
                    "COUNT" | "APPROX_COUNT_DISTINCT" | "BIT_AND" | "BIT_OR" | "BIT_XOR"
                ) {
                    field_type.set_flags(FieldTypeFlags::NOT_NULL);
                } else {
                    field_type.del_flags(FieldTypeFlags::NOT_NULL);
                }
                return Ok(field_type);
            }
            WindowKind::Value { .. } => carried(0),
            // Go `typeInfer4LeadLag` (`aggregation/base_func.go:431-439`):
            // without a written default it is `typeInfer4MaxMin`, the same
            // carried argument type every other value function takes.
            WindowKind::LagLead { default: None, .. } => carried(0),
            // WITH a written default it is not a merge of its own at all --
            // Go's whole body is
            //
            // ```go
            // // Merge the type of first and third argument.
            // a.RetTp, _ = expression.InferType4ControlFuncs(ctx, a.Name, a.Args[0], a.Args[2])
            // ```
            //
            // the SAME call `IF`, `IFNULL`, `COALESCE` and `CASE WHEN` make
            // (`builtin_control.go:241`, whose doc line names `LEAD and LAG`
            // among its callers). So it is shared here for the same reason Go
            // shares it: a second copy of this merge is a copy that drifts.
            // The one this replaced had drifted in four places, each with a
            // captured witness --
            //
            //  * it re-derived the width even when only ONE operand was
            //    typed, where Go copies that operand WHOLE:
            //    `LAG(NULL, 1, int_unsigned_col)` is `int(10) unsigned`, and
            //    the re-derivation added back a sign digit the column does
            //    not spend.
            //  * it kept the operand's own ENUM type, where Go rewrites a
            //    lone enum branch to the VARCHAR its signature returns:
            //    `LAG(enum_col, 1, NULL)` reads `x`, not the empty string.
            //  * it skipped `TryToFixFlenOfDatetime`, so a `DATE`/`TIME` pair
            //    merged to `datetime(6)` reported the TIME's width of 17
            //    rather than the canonical 26.
            //  * it left an all-NULL result unsized instead of Go's `(0, 0)`.
            WindowKind::LagLead {
                is_lag,
                default: Some(_),
                ..
            } => {
                // `value_args` puts the value argument and the default in
                // exactly Go's `a.Args[0], a.Args[2]` order and nothing else.
                let name = if *is_lag { "lag" } else { "lead" };
                let mut merged = tidb_expr::rewriter::infer_type4_control_funcs(name, arg_exprs)
                    // Go always has a static type for both; `None` is
                    // only this tier's "I could not type the arguments".
                    .unwrap_or_else(|| carried(0));
                // Go `aggregation.NewWindowFuncDesc`'s own `LEAD`/`LAG` arm
                // (`window_func.go:66-74`), which runs on whatever
                // `typeInfer4LeadLag` returned. `SetFlag` REPLACES the mask,
                // so a written default beside a NOT NULL argument does not
                // merely add NOT NULL -- it DROPS UNSIGNED, which is why
                // `LAG(bigint_unsigned_not_null, 1, other_not_null)` prints
                // `-1` where the same call over NULLABLE columns prints
                // `18446744073709551615` (both captured from TiDB).
                let both_not_null = arg_exprs.iter().all(|expr| {
                    expr.static_type()
                        .is_some_and(|ft| ft.has_flag(FieldTypeFlags::NOT_NULL))
                });
                if both_not_null {
                    merged.set_flags(FieldTypeFlags::NOT_NULL);
                } else {
                    merged.del_flags(FieldTypeFlags::NOT_NULL);
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
    geometry: &PartitionGeometry<'_>,
    arg_values: &[Vec<Datum>],
    // The aggregate fold, when this is an aggregate call, paired with the
    // session's `div_precision_increment` -- the two things AVG's final
    // division needs, and needed only together.
    // ... plus the ARGUMENT's derived collation, which MIN/MAX orders under.
    agg: Option<(&AggKind, u32, tidb_datatype::Collation)>,
    result_type: &FieldType,
    values: &mut [Datum],
) -> Result<(), DriverError> {
    let total = indices.len();
    let PartitionGeometry {
        peers,
        range_keys,
        constant_default,
    } = geometry;

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
                    // Go reads the value argument AS the merged result type
                    // (`buildValueEvaluator(RetTp)`), so a widening default
                    // pulls the argument into the wider domain too:
                    // `LAG(int_col, 1, 'zz')` yields the STRING `'10'`.
                    Some(at) => coerce_to_domain(arg_at(0, at), result_type),
                    // Go converts the default CONSTANT to the full merged
                    // type (`Value.ConvertTo(RetTp)` in `buildLeadLag`), so
                    // its own display metadata applies to it.
                    None if default.is_some() => {
                        let written = arg_at(1, position);
                        if *constant_default {
                            coerce_to_type(written, result_type)
                        } else {
                            coerce_to_domain(written, result_type)
                        }
                    }
                    None => Datum::Null,
                }
            }
            WindowKind::Agg { .. } => {
                let (low, high) =
                    call.frame
                        .range(position, total, peers[position], range_keys.as_ref())?;
                let (kind, div_precision_increment, collation) =
                    agg.expect("an aggregate window call resolves its kind");
                // The extra arguments a frame row contributes: only
                // `JSON_OBJECTAGG`'s value and `APPROX_COUNT_DISTINCT`'s
                // further tuple members reach the accumulator, since
                // `APPROX_PERCENTILE`'s percentage already rides the kind.
                let extras = |at: usize| match kind {
                    AggKind::JsonObjectAgg { .. } => vec![arg_at(1, at)],
                    AggKind::ApproxCountDistinct => {
                        (1..arg_values.len()).map(|slot| arg_at(slot, at)).collect()
                    }
                    _ => Vec::new(),
                };
                let rows = (low..high).map(|at| match kind {
                    // Go's multi-argument distinct encoding: the row is
                    // skipped entirely as soon as ANY argument is NULL, and
                    // the surviving tuple is dedup-keyed as a whole.
                    AggKind::ApproxCountDistinct => (
                        Some(approx_distinct_tuple(
                            (0..arg_values.len()).map(|slot| arg_at(slot, at)),
                        )),
                        Vec::new(),
                    ),
                    _ => (Some(arg_at(0, at)), extras(at)),
                });
                aggregate_rows(kind, rows, div_precision_increment, collation)
                    .map_err(DriverError::Exec)?
            }
            WindowKind::Value { pick, .. } => {
                let (low, high) =
                    call.frame
                        .range(position, total, peers[position], range_keys.as_ref())?;
                let at = match pick {
                    Pick::Last => high.checked_sub(1).filter(|at| *at >= low),
                    Pick::Nth(n) => usize::try_from(low as u128 + u128::from(*n) - 1)
                        .ok()
                        .filter(|at| *at < high),
                };
                // An empty frame, or a frame shorter than NTH_VALUE's
                // position, is NULL. The row that IS in frame is read through
                // the result's own domain, exactly as `LAG`/`LEAD` reads its
                // -- Go gives `firstValue`/`lastValue`/`nthValue` the SAME
                // `buildValueEvaluator(RetTp)` (`func_value.go:274`, `:321`,
                // `:366`) over the SAME `WrapCastForAggArgs`-wrapped argument.
                // The two only ever differ when the inferred result is not the
                // argument's own type, which for these three is exactly the
                // ENUM/SET rewrite: `FIRST_VALUE(enum_col)` is a `char(255)`
                // holding the LABEL, not the enum's own value-and-name pair.
                at.map_or(Datum::Null, |at| {
                    coerce_to_domain(arg_at(0, at), result_type)
                })
            }
            WindowKind::RowNumber
            | WindowKind::Rank
            | WindowKind::DenseRank
            | WindowKind::PercentRank
            | WindowKind::CumeDist
            | WindowKind::Ntile(_) => unreachable!("the ranking functions take `rank_partition`"),
        };
    }
    Ok(())
}

/// Converts a value into `target`'s DOMAIN, ignoring its display metadata --
/// Go's `buildValueEvaluator(RetTp)`, which reads an argument through the
/// merged type's eval kind (`EvalString`, `EvalDecimal`, ...) rather than
/// through a width-and-scale-applying conversion. That distinction is
/// visible: `LAG(int_col, 1, 1.5)` returns `10`, not the scale-padded
/// `10.0` the full `DECIMAL(12,1)` would produce.
///
/// A TEMPORAL operand is read even more literally than that. Go does not hand
/// the raw operand to the evaluator at all: `WrapCastForAggArgs`
/// (`aggregation/base_func.go:496-538`) wraps it FIRST, and the two temporal
/// wrappers decline to wrap far more often than they wrap --
///
/// ```go
/// func WrapWithCastAsTime(ctx BuildContext, expr Expression, tp *types.FieldType) Expression {
///     exprTp := expr.GetType(ctx.GetEvalCtx()).GetType()
///     if tp.GetType() == exprTp {
///         return expr
///     } else if (exprTp == mysql.TypeDate || exprTp == mysql.TypeTimestamp) && tp.GetType() == mysql.TypeDatetime {
///         return expr
///     }
///     switch x := expr.GetType(ctx.GetEvalCtx()).EvalType(); x {
///     ...
///     case types.ETDatetime, types.ETTimestamp, types.ETDuration:
///         tp.SetDecimal(expr.GetType(ctx.GetEvalCtx()).GetDecimal())
/// ```
///
/// -- and `WrapWithCastAsDuration` returns a `TIME` operand unwrapped the same
/// way. So a temporal operand keeps its own SCALE, and (through the second
/// shortcut) its own KIND: TiDB answers `2020-01-01 10:20:30.123456` for
/// `LAG(datetime6_col, 1)` and a bare `2020-03-04` for the DATE operand of a
/// `LAG(date_col, 1, time3_col)` whose result type is `datetime(3)` -- both
/// captured via `gorun`. Where the wrappers DO wrap, the cast's fsp is taken
/// from the SOURCE, never from the merged result, which is why
/// `LAG(time3_col, 1, date_col)` answers `... 01:02:03.456` and not the
/// result type's six digits.
///
/// This applies to the DEFAULT operand too, and for the same reason: only a
/// CONSTANT default gets `Value.ConvertTo(RetTp)` in `buildLeadLag`, and a
/// constant reaches here as a string or a number rather than as a temporal
/// datum.
fn coerce_to_domain(value: Datum, target: &FieldType) -> Datum {
    let temporal_target = matches!(
        target.eval_type(),
        EvalType::Datetime | EvalType::Timestamp | EvalType::Duration
    );
    if temporal_target {
        let unwrapped = match &value {
            Datum::Time(time) => {
                time.kind() == time_kind_of(target.code())
                    // `WrapWithCastAsTime`'s second shortcut.
                    || (target.code() == FieldTypeCode::Datetime
                        && matches!(time.kind(), TimeType::Date | TimeType::Timestamp))
            }
            // `WrapWithCastAsDuration`'s only shortcut.
            Datum::Duration(_) => target.code() == FieldTypeCode::Duration,
            _ => false,
        };
        if unwrapped {
            return value;
        }
    }
    let mut domain = FieldType::new(target.code());
    domain.set_flen(tidb_datatype::UNSPECIFIED_LENGTH);
    // The cast Go builds when it does wrap takes its fsp from the SOURCE. The
    // two wrappers' tables agree on every temporal source, and a temporal
    // result type can only be reached from a temporal operand, so the
    // temporal rows are the whole reachable table.
    domain.set_decimal(match (&value, temporal_target) {
        (Datum::Time(time), true) => i64::from(time.fsp()),
        (Datum::Duration(duration), true) => i64::from(duration.fsp()),
        _ => tidb_datatype::UNSPECIFIED_FSP,
    });
    if target.is_unsigned() {
        domain.add_flags(FieldTypeFlags::UNSIGNED);
    }
    coerce_to_type(value, &domain)
}

/// Whether an expression reads no COLUMN, which is exactly when Go's
/// `FoldConstant` has already collapsed it into an `*expression.Constant` by
/// the time `buildLeadLag` type-asserts on it. A non-deterministic call is
/// still folded (into a constant carrying a deferred expression), which is
/// why `LAG(datetime6_col, 1, NOW())` pads its default to `.000000` exactly
/// like a written literal would -- captured from TiDB via `gorun`.
fn column_free(expr: &tidb_expr::expression::Expression) -> bool {
    use tidb_expr::expression::Expression;
    match expr {
        Expression::Column(_) | Expression::CorrelatedColumn(_) => false,
        Expression::Constant(_) => true,
        Expression::ScalarFunction(function) => function.args.iter().all(column_free),
    }
}

/// The `TimeType` a temporal field-type code stores, so a `DATE` operand and
/// a `DATE` result compare equal the way Go compares `tp.GetType()` with the
/// expression's own `GetType()`.
fn time_kind_of(code: FieldTypeCode) -> TimeType {
    match code {
        FieldTypeCode::Date => TimeType::Date,
        FieldTypeCode::Timestamp => TimeType::Timestamp,
        _ => TimeType::DateTime,
    }
}

/// Converts a value into `target` exactly, leaving it untouched when the
/// conversion fails (Go's `buildLeadLag` keeps the original constant when
/// `ConvertTo` errors).
fn coerce_to_type(value: Datum, target: &FieldType) -> Datum {
    if value.is_null() {
        return value;
    }
    // An INTEGER result is never range-converted, in EITHER role. Go's
    // `value4Int` stores what `EvalInt` handed it -- the raw 64-bit pattern
    // -- and appends it with `AppendInt64`, leaving the RESULT column's
    // UNSIGNED flag to decide how it prints; and a constant default whose
    // `ConvertTo` overflows is KEPT as written and then read the same way.
    // Both roads end at the bit pattern, never at a saturated bound, and the
    // merged type is exactly where the two signednesses meet:
    // `LAG(bigint_unsigned_not_null, 1, other_unsigned_not_null)` merges to a
    // SIGNED result (Go's `SetFlag(NotNullFlag)` drops UNSIGNED) and TiDB
    // answers `-6`/`-1`, not the clamped `9223372036854775807`.
    if target.eval_type() == EvalType::Int {
        match (&value, target.is_unsigned()) {
            (Datum::UInt(bits), false) => return Datum::Int(*bits as i64),
            (Datum::Int(bits), true) => return Datum::UInt(*bits as u64),
            (Datum::Int(_) | Datum::UInt(_), _) => return value,
            // A non-integer datum still converts: Go reaches an integer
            // result from one only through `ConvertTo`/`EvalInt`, both of
            // which round rather than reinterpret.
            _ => {}
        }
    }
    match value.convert_to(target, tidb_datatype::DEFAULT_STATEMENT_FLAGS) {
        Ok(converted) => converted.value,
        Err(_) => value,
    }
}

/// The single datum an `APPROX_COUNT_DISTINCT` row contributes: Go encodes
/// every argument into one buffer with `evalAndEncode` and hashes it as a
/// unit, and drops the row entirely when any argument is NULL
/// (`approxCountDistinctOriginal`'s `hasNull` guard). Each argument's raw
/// per-type encoding (`crate::hash_agg::approx_count_distinct_encode`, the
/// same one the GROUP BY path uses) is appended with no separator, matching
/// Go's own unframed concatenation.
fn approx_distinct_tuple(values: impl IntoIterator<Item = Datum>) -> Datum {
    let mut buffer = Vec::new();
    for value in values {
        if value == Datum::Null {
            return Datum::Null;
        }
        let Ok(key) = crate::hash_agg::approx_count_distinct_encode(&value) else {
            return Datum::Null;
        };
        buffer.extend_from_slice(&key);
    }
    Datum::Bytes(buffer)
}

/// Evaluates one key expression list for every row.
fn eval_keys(
    exprs: &[Expr],
    rows: &[Vec<Datum>],
    field_types: &[FieldType],
    resolver: &impl ColumnResolver,
    ctx: &StmtContext,
) -> Result<(Vec<Vec<Datum>>, Vec<tidb_datatype::Collation>), DriverError> {
    let mut rewritten = Vec::with_capacity(exprs.len());
    for expr in exprs {
        rewritten.push(
            rewrite_expr_resolved(expr, resolver)
                .map_err(|e| DriverError::Exec(crate::ExecError::Eval(e)))?,
        );
    }
    // Each key's DERIVED collation, which is what identifies two string
    // values as the same partition and as peers of each other -- Go builds
    // the group checker's and the ranking functions' comparators from the
    // by-item's own `RetType`, so a `_ci` key groups `'a'` with `'A'`.
    let collations = rewritten
        .iter()
        .map(tidb_expr::collation_derive::collation_of_node)
        .collect();
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
    Ok((keys, collations))
}

/// Whether two key rows are the SAME key -- the peer/partition identity, taken
/// under each key part's own derived collation.
///
/// This is the equality half of [`tidb_expr::compare_datums_with_collation`],
/// which is what Go's `VecGroupChecker` gets by comparing
/// `codec.ConvertByCollationStr` sort keys rather than raw bytes.
fn keys_equal(
    left: &[Datum],
    right: &[Datum],
    collations: &[tidb_datatype::Collation],
) -> Result<bool, DriverError> {
    for ((l, r), collation) in left.iter().zip(right).zip(collations) {
        let ordering = tidb_expr::compare_datums_with_collation(l, r, *collation)
            .map_err(|e| DriverError::Exec(crate::ExecError::Eval(e)))?;
        if !ordering.is_eq() {
            return Ok(false);
        }
    }
    Ok(true)
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
) -> Result<
    (
        Vec<Vec<Datum>>,
        Vec<Option<FieldType>>,
        Vec<tidb_expr::expression::Expression>,
    ),
    DriverError,
> {
    let mut values = Vec::with_capacity(exprs.len());
    let mut types = Vec::with_capacity(exprs.len());
    let mut rewritten_exprs = Vec::with_capacity(exprs.len());
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
        rewritten_exprs.push(rewritten);
    }
    Ok((values, types, rewritten_exprs))
}

/// Stable-sorts one partition's row indices by the window's `ORDER BY`.
///
/// The sort is stable, so rows tied on every key keep their source order --
/// which is what makes `ROW_NUMBER` over ties deterministic here.
fn sort_partition(
    indices: &mut [usize],
    order_keys: &[Vec<Datum>],
    order_collations: &[tidb_datatype::Collation],
    order_by: &[OrderItem],
) -> Result<(), DriverError> {
    if order_by.is_empty() {
        return Ok(());
    }
    let mut failure = None;
    indices.sort_by(|left, right| {
        for (position, item) in order_by.iter().enumerate() {
            let ordering = match tidb_expr::compare_datums_with_collation(
                &order_keys[*left][position],
                &order_keys[*right][position],
                order_collations[position],
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

/// One partition's frame GEOMETRY: which sorted positions are peers of each
/// other, plus the sorted `ORDER BY` key a value-measured `RANGE` frame
/// measures against. Both are computed once per partition and read by every
/// row of it.
struct PartitionGeometry<'a> {
    /// Each sorted position's peer group, as a half-open position range.
    peers: &'a [(usize, usize)],
    /// The single sorted key a `RANGE` frame seeks in; `None` for every other
    /// frame kind, which never looks at a key VALUE.
    range_keys: Option<RangeKeys<'a>>,
    /// Whether `LAG`/`LEAD`'s written default is a CONSTANT -- Go's
    /// `buildLeadLag` converts only that one to the merged type.
    constant_default: bool,
}

/// The peer group each SORTED POSITION of a partition belongs to, as a
/// half-open position range.
///
/// Two rows are peers when their window `ORDER BY` keys are equal under those
/// keys' OWN collations -- Go compares collation sort keys here, so on a `_ci`
/// key `'a'` and `'A'` are one peer group and share one `RANK`, one
/// `CUME_DIST` and one `RANGE ... CURRENT ROW` frame. With no window
/// `ORDER BY` every key is empty, so the whole partition is one peer group --
/// which is exactly why an unordered window frames the partition.
fn peer_groups(
    indices: &[usize],
    order_keys: &[Vec<Datum>],
    order_collations: &[tidb_datatype::Collation],
) -> Result<Vec<(usize, usize)>, DriverError> {
    let total = indices.len();
    let mut peers = vec![(0usize, total); total];
    let mut group_start = 0;
    for position in 1..=total {
        let ends = position == total
            || !keys_equal(
                &order_keys[indices[position]],
                &order_keys[indices[position - 1]],
                order_collations,
            )?;
        if ends {
            for entry in &mut peers[group_start..position] {
                *entry = (group_start, position);
            }
            group_start = position;
        }
    }
    Ok(peers)
}

/// Writes one partition's ranking values into `values`, at each row's own
/// source position.
fn rank_partition(
    kind: &WindowKind,
    indices: &[usize],
    peer_groups: &[(usize, usize)],
    values: &mut [Datum],
) {
    // Two SORTED POSITIONS are peers when they share a peer group, which
    // [`peer_groups`] already computed under each key's own collation -- so a
    // `_ci` `ORDER BY` key makes `'a'` and `'A'` one rank, exactly as the
    // frame's `CURRENT ROW` already treats them. Rows with no window
    // `ORDER BY` all land in the single whole-partition group.
    let peers = |left: usize, right: usize| peer_groups[left] == peer_groups[right];
    match kind {
        WindowKind::RowNumber => {
            for (position, index) in indices.iter().enumerate() {
                values[*index] = Datum::Int(position as i64 + 1);
            }
        }
        WindowKind::Rank => {
            let mut rank = 1i64;
            for (position, index) in indices.iter().enumerate() {
                if position > 0 && !peers(position - 1, position) {
                    rank = position as i64 + 1;
                }
                values[*index] = Datum::Int(rank);
            }
        }
        WindowKind::DenseRank => {
            let mut rank = 1i64;
            for (position, index) in indices.iter().enumerate() {
                if position > 0 && !peers(position - 1, position) {
                    rank += 1;
                }
                values[*index] = Datum::Int(rank);
            }
        }
        // Both distribution functions are PEER-based: every row of a peer
        // group shares one value, computed from where the group starts
        // (`PERCENT_RANK`) or ends (`CUME_DIST`).
        WindowKind::PercentRank | WindowKind::CumeDist => {
            let total = indices.len();
            let mut group_start = 0;
            for position in 1..=total {
                if position < total && peers(position - 1, position) {
                    continue;
                }
                let value = if matches!(kind, WindowKind::CumeDist) {
                    position as f64 / total as f64
                } else if total <= 1 {
                    // Go's `percentRank`: a single-row partition divides by
                    // zero rows, and the answer is 0 rather than NaN.
                    0.0
                } else {
                    group_start as f64 / (total - 1) as f64
                };
                for index in &indices[group_start..position] {
                    values[*index] = Datum::Real(value);
                }
                group_start = position;
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

#[cfg(test)]
mod constant_uint_tests {
    use super::*;

    /// `constant_uint` must resolve `TRUE`/`FALSE` to `1`/`0` exactly like
    /// Go's `GetUint64FromConstant` resolves an `Int64` constant -- proving
    /// `FALSE` is rejected downstream because it resolves to the disallowed
    /// value `0` (the same ground as `NTILE(0)`), not because it fails to
    /// parse as a constant at all (that "wrong reason" is what a bare
    /// `Err(DriverError::WrongArguments(_))` match cannot distinguish, since
    /// both paths produce the same error variant).
    #[test]
    fn bool_literals_resolve_like_go_int64_constants() {
        assert_eq!(constant_uint(&Expr::Bool(true)), Some(Some(1)));
        assert_eq!(constant_uint(&Expr::Bool(false)), Some(Some(0)));
        assert_eq!(constant_uint(&Expr::Null), Some(None));
        assert_eq!(constant_uint(&Expr::Int("5".to_string())), Some(Some(5)));
        // A column reference is not a constant at all -- genuinely `None`,
        // the OTHER ground `NTILE(k)` is rejected on.
        assert_eq!(constant_uint(&Expr::Column(vec!["k".to_string()])), None);
    }

    #[test]
    fn ntile_bucket_count_accepts_true_rejects_false_on_the_zero_ground() {
        assert!(matches!(
            constant_bucket_count(&Expr::Bool(true)),
            Some(BucketCount::Positive(1))
        ));
        assert!(matches!(
            constant_bucket_count(&Expr::Null),
            Some(BucketCount::Null)
        ));
        // `FALSE` reaches `constant_uint` fine (it IS a constant, resolving
        // to `0`), and is rejected by the zero check inside
        // `constant_bucket_count` -- the same branch that rejects
        // `NTILE(0)` -- not by `constant_uint` returning `None`.
        assert_eq!(constant_uint(&Expr::Bool(false)), Some(Some(0)));
        assert!(constant_bucket_count(&Expr::Bool(false)).is_none());
    }
}
