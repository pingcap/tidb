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

//! Set-operation statements (`UNION` / `EXCEPT` / `INTERSECT` chains) and
//! their restore, mirroring Go's `SetOprStmt` family in `pkg/parser/ast/dml.go`.

use super::*;

/// A set-operation statement: a chain of `SELECT` terms joined by set operators,
/// with optional statement-level `ORDER BY` / `LIMIT`.
#[derive(Debug, Clone, PartialEq)]
pub struct SetOprStmt {
    /// A leading `WITH` clause. TiDB attaches a CTE prefix to the entire
    /// set-operation wrapper (`WITH c AS (...) SELECT ... UNION SELECT ...`),
    /// rather than pretending it belongs only to the first `SELECT` term.
    /// Keeping that ownership here preserves both restore and the eventual
    /// query-planning scope boundary.
    pub with: Option<WithClause>,
    /// Whether the whole set-operation statement was enclosed in source
    /// parentheses. TiDB keeps this bit on `SetOprStmt` itself, so a
    /// statement-level ORDER BY/LIMIT restores inside the same pair.
    pub is_in_braces: bool,
    /// The terms; the first has `op == None`, each later term carries the
    /// operator that joins it to the accumulated result.
    pub terms: Vec<SetOprTerm>,
    /// A statement-level `ORDER BY`.
    pub order_by: Vec<OrderItem>,
    /// A statement-level `LIMIT`.
    pub limit: Option<Limit>,
    /// A statement-level locking clause — see [`SelectStmt::lock`]'s own
    /// doc for why this is a SEPARATE field from any individual term's
    /// own `lock` rather than always attaching to the last term.
    pub lock: Option<SelectLock>,
    /// An `ORDER BY` written outside this statement's own parentheses.
    pub outer_order_by: Vec<OrderItem>,
    /// A `LIMIT` written outside this statement's own parentheses.
    pub outer_limit: Option<Limit>,
    /// A locking clause written outside this statement's own parentheses.
    pub outer_lock: Option<SelectLock>,
}

impl SetOprStmt {
    pub(crate) fn restore_into_with_context(&self, out: &mut String, context: &RestoreContext) {
        let scoped_context = self
            .with
            .as_ref()
            .map(|with| with.restore_into_with_context(out, context));
        if self.with.is_some() {
            out.push(' ');
        }
        let context = scoped_context.as_ref().unwrap_or(context);
        if self.is_in_braces {
            out.push('(');
        }
        for term in &self.terms {
            if let Some(op) = &term.op {
                out.push(' ');
                out.push_str(op.restore());
                out.push(' ');
            }
            if term.in_braces {
                out.push('(');
                term.body.restore_into_with_context(out, context);
                out.push(')');
            } else {
                term.body.restore_into_with_context(out, context);
            }
        }
        // A statement-level lock (see `SelectStmt::lock`'s own doc for why
        // this is a separate field) prints BEFORE `ORDER BY`/`LIMIT` here
        // — the OPPOSITE order from a plain `SelectStmt`'s own `lock`
        // (see that impl's own comment) — confirmed via `godump restore`:
        // `t1 UNION t2 LIMIT 1 FOR UPDATE` and `t1 UNION t2 FOR UPDATE
        // LIMIT 1` both restore identically as `... FOR UPDATE ... LIMIT
        // 1`, never the reverse.
        restore_lock(out, &self.lock);
        restore_order_by(out, &self.order_by);
        restore_limit(out, &self.limit);
        let has_inner_tail =
            self.lock.is_some() || !self.order_by.is_empty() || self.limit.is_some();
        // With no inner tail, TiDB folds an outer tail into the preserved
        // parentheses. When both tails exist, the closing parenthesis remains
        // their ownership boundary and the outer tail stays outside.
        if !has_inner_tail {
            restore_lock(out, &self.outer_lock);
            restore_order_by(out, &self.outer_order_by);
            restore_limit(out, &self.outer_limit);
        }
        if self.is_in_braces {
            out.push(')');
        }
        if has_inner_tail {
            restore_lock(out, &self.outer_lock);
            restore_order_by(out, &self.outer_order_by);
            restore_limit(out, &self.outer_limit);
        }
    }

    /// The `SELECT` used for output-column naming/shape — real MySQL/
    /// TiDB always uses the FIRST term's own column list, even when that
    /// term is itself a parenthesized [`SetOprTermBody::Nested`] set
    /// operation (in which case ITS OWN first term is used, recursively
    /// — a `SetOprStmt` always has at least one term, see
    /// `SetOprTerm::body`'s own doc for why `Nested` can only ever arise
    /// from a parenthesized term, never as the sole content of a
    /// statement, so this recursion always bottoms out).
    pub fn representative_select(&self) -> &SelectStmt {
        self.terms[0].body.representative_select()
    }
}

/// One term of a [`SetOprStmt`].
#[derive(Debug, Clone, PartialEq)]
pub struct SetOprTerm {
    /// The operator joining this term to the previous ones (`None` on the first).
    pub op: Option<SetOp>,
    /// Whether the term was parenthesized (preserved on restore). Always
    /// `true` when `body` is [`SetOprTermBody::Nested`] — that variant
    /// can only arise from a parenthesized term in the first place.
    pub in_braces: bool,
    /// The term's own body.
    pub body: SetOprTermBody,
}

/// The body of one [`SetOprTerm`]: either a plain `SELECT`, or — only
/// reachable when the term was parenthesized — a NESTED set operation
/// (`t1 UNION (t2 UNION ALL t3)`), preserving its own scoped `ORDER BY`/
/// `LIMIT` distinct from the outer statement's own (confirmed via
/// `godump restore`: `t1 UNION (t2 UNION ALL t3 ORDER BY x LIMIT 5)`
/// applies `ORDER BY x LIMIT 5` to just the `(t2 UNION ALL t3)` group
/// before folding it into the outer `UNION`, not to the whole
/// statement). Mirrors real TiDB's own `ast.SetOprSelectList` wrapper
/// (`pkg/parser/select_clauses_parser.go`'s `parseSetOprRest`), which
/// nests a fresh `SetOprSelectList` INTO the parent's flat `Selects`
/// list instead of flattening a parenthesized child — this crate's own
/// flat `Vec<SetOprTerm>` model otherwise has no way to represent that a
/// specific sub-run of terms was independently grouped.
#[derive(Debug, Clone, PartialEq)]
pub enum SetOprTermBody {
    /// A plain `SELECT` term. Boxed to keep this enum's own size close
    /// to `Nested`'s (a bare `SelectStmt` is over 1KB, dwarfing a
    /// `Box<SetOprStmt>` pointer) — the SAME reason [`QueryStmt::Select`]
    /// boxes its own `SelectStmt`.
    Select(Box<SelectStmt>),
    /// A parenthesized nested set operation.
    Nested(Box<SetOprStmt>),
}

impl SetOprTermBody {
    fn restore_into_with_context(&self, out: &mut String, context: &RestoreContext) {
        match self {
            SetOprTermBody::Select(sel) => sel.restore_into_with_context(out, context),
            SetOprTermBody::Nested(so) => so.restore_into_with_context(out, context),
        }
    }

    /// See [`SetOprStmt::representative_select`]'s own doc.
    pub fn representative_select(&self) -> &SelectStmt {
        match self {
            SetOprTermBody::Select(sel) => sel,
            SetOprTermBody::Nested(so) => so.representative_select(),
        }
    }
}

/// A set operator with its all/distinct modifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOp {
    /// `UNION` (`all` = `UNION ALL`).
    Union {
        /// Whether `ALL` was specified (keep duplicates).
        all: bool,
    },
    /// `EXCEPT`.
    Except {
        /// Whether `ALL` was specified.
        all: bool,
    },
    /// `INTERSECT`.
    Intersect {
        /// Whether `ALL` was specified.
        all: bool,
    },
}

impl SetOp {
    fn restore(&self) -> &'static str {
        match self {
            SetOp::Union { all: false } => "UNION",
            SetOp::Union { all: true } => "UNION ALL",
            SetOp::Except { all: false } => "EXCEPT",
            SetOp::Except { all: true } => "EXCEPT ALL",
            SetOp::Intersect { all: false } => "INTERSECT",
            SetOp::Intersect { all: true } => "INTERSECT ALL",
        }
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for SetOprStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            with,
            is_in_braces,
            terms,
            order_by,
            limit,
            lock,
            outer_order_by,
            outer_limit,
            outer_lock,
        } = self;
        if let Some(value) = with.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        for value in terms.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        for value in order_by.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = limit.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = lock.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        for value in outer_order_by.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = outer_limit.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = outer_lock.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = with;
        let _ = is_in_braces;
        let _ = terms;
        let _ = order_by;
        let _ = limit;
        let _ = lock;
        let _ = outer_order_by;
        let _ = outer_limit;
        let _ = outer_lock;
        visitor.leave(self)
    }
}

impl crate::Visitable for SetOprTerm {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            op,
            in_braces,
            body,
        } = self;
        if let Some(value) = op.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if !crate::Visitable::accept(body, visitor) {
            return false;
        }
        let _ = op;
        let _ = in_braces;
        let _ = body;
        visitor.leave(self)
    }
}

impl crate::Visitable for SetOprTermBody {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Select(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Nested(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for SetOp {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Union { all } => {
                let _ = all;
            }
            Self::Except { all } => {
                let _ = all;
            }
            Self::Intersect { all } => {
                let _ = all;
            }
        }
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
