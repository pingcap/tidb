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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Window specifications and frames (`OVER` clause), mirroring Go's window
//! clause AST in `pkg/parser/ast/dml.go`.

use super::*;

/// A window function's `(PARTITION BY expr, ... ORDER BY expr [ASC|DESC],
/// ... [ROWS ...])` specification. `partition_by`/`order_by` are both
/// optional (empty `OVER ()` computes over the whole relation in scan
/// order); `frame` is likewise optional (`None` means the default
/// `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` frame every window
/// AGGREGATE/`FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE` already falls back to
/// — see `tidb_exec`'s `Database::compute_window` for the exact rule).
/// Confirmed via `gorun`, not assumed: a frame clause parses on EVERY
/// window function (not just the frame-eligible ones) but has NO effect
/// on `ROW_NUMBER`/`RANK`/`DENSE_RANK`/`PERCENT_RANK`/`CUME_DIST`/`NTILE`/
/// `LAG`/`LEAD` — those simply ignore it — so this field is stored
/// uniformly for every `Expr::Window`, not restricted to a subset of
/// function names at the AST level.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct WindowSpec {
    /// The `PARTITION BY` expressions, empty if unwritten.
    pub partition_by: Vec<Expr>,
    /// The `ORDER BY` items, empty if unwritten.
    pub order_by: Vec<OrderItem>,
    /// The explicit frame clause, if written.
    pub frame: Option<WindowFrame>,
}

/// A window definition: either fresh (`base: None`), or NAMING an earlier
/// window it extends (`base: Some(name)`) — used both for one entry of a
/// top-level `WINDOW name AS (...)` clause and for a parenthesized `OVER
/// (...)` reference. Real MySQL/TiDB restricts what an extension's OWN
/// `spec` may add on top of the named base (confirmed via `gorun`, NOT a
/// parse-time restriction — the grammar accepts any combination
/// syntactically, so this project parses broadly here too and validates
/// only when a window function is actually resolved, see
/// `tidb_exec::window`): `spec.partition_by` must always be empty when
/// `base` is `Some` (a base's `PARTITION BY` can never be re-specified,
/// regardless of whether the base itself has one); `spec.order_by` may be
/// non-empty only if the base doesn't already have its own; `spec.frame`
/// may be `Some` only if the base doesn't already have one either — and,
/// transitively, a base that ITSELF extends another window chains the
/// same rules (confirmed via `gorun`: named windows may reference an
/// EARLIER-OR-LATER one by name — order in the `WINDOW` clause doesn't
/// matter — but a self-referencing or circular chain is a genuine error).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct WindowDef {
    /// The named window this one extends, if any.
    pub base: Option<String>,
    /// This definition's own (possibly empty) specification.
    pub spec: WindowSpec,
}

/// How a window function's `OVER` clause refers to its specification —
/// confirmed via `godump restore` these restore DIFFERENTLY even when
/// semantically equivalent: a bare name has no parentheses at all
/// (`OVER w`), while EVERY other form is parenthesized, whether empty
/// (`OVER ()`), fully inline (`OVER (PARTITION BY ...)`), or naming a
/// base window with or without its own extension (`OVER (w)`/
/// `OVER (w ORDER BY ...)`).
#[derive(Debug, Clone, PartialEq)]
pub enum WindowOver {
    /// `OVER name` — a bare window name, no parentheses.
    Name(String),
    /// `OVER (...)` — parenthesized: fully inline when `base` is `None`,
    /// otherwise naming (and optionally extending) an earlier window.
    Def(WindowDef),
}

/// Restores a window definition's own BODY (no enclosing parentheses,
/// added by the caller — shared by a top-level `WINDOW name AS (...)`
/// entry and a parenthesized `OVER (...)` reference alike): an optional
/// leading base-window name, then the spec's own `PARTITION BY`/
/// `ORDER BY`/frame clauses.
pub(crate) fn restore_window_def(def: &WindowDef, out: &mut String) {
    let mut sep = "";
    if let Some(base) = &def.base {
        out.push_str(&back_quote(base));
        sep = " ";
    }
    // The Go AST writes a plain " " separator before each present clause
    // (never baked into a clause's own restore) — confirmed via `godump
    // restore`: `PARTITION BY` items join with `, ` but `ORDER BY` items
    // join with `,` (no space), an asymmetry that must be encoded
    // exactly, not "fixed" to be consistent.
    if !def.spec.partition_by.is_empty() {
        out.push_str(sep);
        out.push_str("PARTITION BY ");
        for (i, e) in def.spec.partition_by.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            e.restore_into(out);
        }
        sep = " ";
    }
    if !def.spec.order_by.is_empty() {
        out.push_str(sep);
        out.push_str("ORDER BY ");
        for (i, item) in def.spec.order_by.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            item.restore_into(out);
        }
        sep = " ";
    }
    if let Some(frame) = &def.spec.frame {
        out.push_str(sep);
        frame.restore_into(out);
    }
}

/// A `ROWS`/`RANGE BETWEEN <start> AND <end>` window frame — restricts a
/// window AGGREGATE's or `FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE`'s frame
/// to a bounded range around the current row, instead of the implicit
/// default frame. `ROWS` is a PHYSICAL row-offset range (confirmed via
/// `gorun`: two rows TIED on `ORDER BY` still get their OWN distinct
/// `ROWS`-frame value); `RANGE` is a VALUE-distance range against the
/// single `ORDER BY` key's own value instead (see [`FrameKind`]'s own
/// doc) — the implicit default frame is itself equivalent to `RANGE
/// BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, confirmed via `gorun`
/// to produce IDENTICAL results to leaving the frame unwritten entirely.
/// The single-bound shorthand (`ROWS 3 PRECEDING`/`RANGE 3 PRECEDING`) is
/// normalized at parse time to the full `BETWEEN <bound> AND CURRENT
/// ROW` form for EITHER kind — real TiDB's own restore does the same
/// (confirmed via `godump`), so there is only ONE shape to model per
/// kind, not two.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowFrame {
    /// `ROWS` or `RANGE`.
    pub kind: FrameKind,
    /// The frame's starting boundary.
    pub start: FrameBound,
    /// The frame's ending boundary.
    pub end: FrameBound,
}

impl WindowFrame {
    fn restore_into(&self, out: &mut String) {
        out.push_str(match self.kind {
            FrameKind::Rows => "ROWS BETWEEN ",
            FrameKind::Range => "RANGE BETWEEN ",
        });
        self.start.restore_into(out);
        out.push_str(" AND ");
        self.end.restore_into(out);
    }
}

/// Which of the two frame KINDS a [`WindowFrame`] uses — both share the
/// exact same [`FrameBound`] grammar syntactically, but the two commit to
/// a genuinely different notion of a bound's "distance": `Rows` counts
/// physical row positions; `Range` measures the SORT KEY's own value,
/// requiring EXACTLY one `ORDER BY` column when a bound is an actual
/// `Preceding`/`Following` offset (confirmed via `gorun`: real TiDB
/// rejects `RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING` outright with
/// zero or multiple `ORDER BY` columns) — `UnboundedPreceding`/
/// `CurrentRow`/`UnboundedFollowing`-only bounds need no arithmetic at
/// all, so those work under any number of `ORDER BY` columns, same as
/// `Rows`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameKind {
    /// Bounds count physical row positions relative to the current row.
    Rows,
    /// Bounds measure the single `ORDER BY` key's own value distance from
    /// the current row's key.
    Range,
}

/// One frame boundary, shared by BOTH `ROWS` and `RANGE` (see
/// [`FrameKind`]). Ordered `UnboundedPreceding < Preceding < CurrentRow <
/// Following < UnboundedFollowing` — a frame whose `start` ranks AFTER
/// its `end` in this order is a genuine execution-time error REGARDLESS
/// of the `Preceding`/`Following` offset's own value (confirmed via
/// `gorun` for both kinds: `ROWS`/`RANGE BETWEEN CURRENT ROW AND 1
/// PRECEDING` errors even though both bounds are individually valid),
/// whereas two bounds of the SAME kind with an offset that happens to
/// produce an empty range at runtime (`ROWS`/`RANGE BETWEEN 2 FOLLOWING
/// AND 1 FOLLOWING`) is NOT a static error — it silently yields an empty
/// frame (`NULL` for an aggregate) for every row where it applies, also
/// confirmed via `gorun` for both kinds.
#[derive(Debug, Clone, PartialEq)]
pub enum FrameBound {
    /// The partition's first row.
    UnboundedPreceding,
    /// `N` rows before the current row (`N` may be any expression,
    /// though real usage is always a non-negative integer literal).
    Preceding(Box<Expr>),
    /// The current row itself.
    CurrentRow,
    /// `N` rows after the current row.
    Following(Box<Expr>),
    /// The partition's last row.
    UnboundedFollowing,
}

impl FrameBound {
    fn restore_into(&self, out: &mut String) {
        match self {
            FrameBound::UnboundedPreceding => out.push_str("UNBOUNDED PRECEDING"),
            FrameBound::Preceding(n) => {
                n.restore_into(out);
                out.push_str(" PRECEDING");
            }
            FrameBound::CurrentRow => out.push_str("CURRENT ROW"),
            FrameBound::Following(n) => {
                n.restore_into(out);
                out.push_str(" FOLLOWING");
            }
            FrameBound::UnboundedFollowing => out.push_str("UNBOUNDED FOLLOWING"),
        }
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for WindowSpec {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            partition_by,
            order_by,
            frame,
        } = self;
        for value in partition_by.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        for value in order_by.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = frame.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = partition_by;
        let _ = order_by;
        let _ = frame;
        visitor.leave(self)
    }
}

impl crate::Visitable for WindowDef {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { base, spec } = self;
        if !crate::Visitable::accept(spec, visitor) {
            return false;
        }
        let _ = base;
        let _ = spec;
        visitor.leave(self)
    }
}

impl crate::Visitable for WindowOver {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Name(field_0) => {
                let _ = field_0;
            }
            Self::Def(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for WindowFrame {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { kind, start, end } = self;
        if !crate::Visitable::accept(kind, visitor) {
            return false;
        }
        if !crate::Visitable::accept(start, visitor) {
            return false;
        }
        if !crate::Visitable::accept(end, visitor) {
            return false;
        }
        let _ = kind;
        let _ = start;
        let _ = end;
        visitor.leave(self)
    }
}

impl crate::Visitable for FrameKind {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Rows => {}
            Self::Range => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for FrameBound {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::UnboundedPreceding => {}
            Self::Preceding(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::CurrentRow => {}
            Self::Following(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::UnboundedFollowing => {}
        }
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
