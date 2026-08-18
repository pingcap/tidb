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

//! `SELECT` statements (select list, `WITH`/`ORDER BY`/`GROUP BY`/`LIMIT`/lock
//! clauses) and their restore. Set operations, window clauses, hints, and the
//! `FROM` join tree live in the sibling modules below.

mod hint;
mod set_opr;
mod table_ref;
mod window;

pub use hint::*;
pub use set_opr::*;
pub use table_ref::*;
pub use window::*;

use crate::util::{back_quote, escape_string_literal};
use crate::{
    Expr, LoadDataFields, LoadDataLines, NodeText, QueryStmt, RestoreContext, StatementPriority,
};

/// The source spelling of a [`SelectStmt`].
///
/// Go represents `TABLE t` and `VALUES ROW(...)` as `SelectStmt`s whose
/// `Kind` preserves their compact spellings rather than restoring either as a
/// synthetic `SELECT`. Keeping that discriminator prevents parser-only
/// desugaring from silently changing SQL text while still allowing supported
/// forms to share the surrounding query envelope.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SelectStatementKind {
    /// The ordinary `SELECT ...` form.
    #[default]
    Select,
    /// The shorthand `TABLE table_name ...` form.
    Table,
    /// The standalone `VALUES ROW(...), ...` form.
    Values,
}

/// A `SELECT`, `TABLE`, or standalone `VALUES` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    /// The SQL statement form preserved by restore.
    pub kind: SelectStatementKind,
    /// Whether the query was enclosed by the source grammar's own
    /// parenthesized-query wrapper.  Go keeps this bit on `ast.SelectStmt`
    /// (`IsInBraces`) even for a sole `VALUES` query; retaining it here is
    /// needed by the EXPLAIN/VALUES corpus leaf without pretending that a
    /// one-term query is a set operation.
    pub is_in_braces: bool,
    /// A `WITH` clause, if this query is a plain `SELECT`. A leading CTE
    /// before an outer `UNION`/`EXCEPT`/`INTERSECT` belongs to
    /// [`SetOprStmt::with`] instead, matching TiDB's query ownership.
    pub with: Option<WithClause>,
    /// A `/*+ ... */` optimizer-hint comment's own hints, in written
    /// order — only recognized directly after the `SELECT` keyword
    /// (matching [`tidb_lexer`]'s own `HINTED_KEYWORDS`; `INSERT`/
    /// `UPDATE`/`DELETE`/`CREATE`/`REPLACE`/`PARTITION` are also real
    /// hint-eligible positions in real TiDB, but a deliberately deferred
    /// follow-up here — see [`Hint`]'s own doc for the modelled-shape
    /// scope boundary). Empty if no hint comment was written.
    pub hints: Vec<Hint>,
    /// Optional MySQL statement priority.
    pub priority: StatementPriority,
    /// `SQL_SMALL_RESULT`.
    pub sql_small_result: bool,
    /// `SQL_BIG_RESULT`.
    pub sql_big_result: bool,
    /// `SQL_BUFFER_RESULT`.
    pub sql_buffer_result: bool,
    /// `SQL_NO_CACHE` (Go defaults the cache flag to enabled).
    pub sql_no_cache: bool,
    /// `STRAIGHT_JOIN` select modifier.
    pub straight_join: bool,
    /// Whether `SQL_CALC_FOUND_ROWS` was specified — a `SELECT`
    /// modifier, syntactically independent of and freely orderable
    /// with `DISTINCT`/`ALL` (real TiDB's own `parseSelectOpts`,
    /// `pkg/parser/select_parser.go`, accepts every SELECT-level
    /// modifier keyword in ANY order via one shared loop). Restore
    /// always prints it in a FIXED position BEFORE `DISTINCT`/`ALL`
    /// regardless of the order it was WRITTEN in — confirmed via
    /// `godump restore`: both `SELECT SQL_CALC_FOUND_ROWS DISTINCT a
    /// ...` and `SELECT DISTINCT SQL_CALC_FOUND_ROWS a ...` restore
    /// identically as `SELECT SQL_CALC_FOUND_ROWS DISTINCT a ...` (real
    /// TiDB's own `SelectStmt.Restore` writes it unconditionally before
    /// `Distinct`/`ExplicitAll`, both stored as separate fields set
    /// during parsing, not preserving relative write order).
    pub calc_found_rows: bool,
    /// Whether `DISTINCT` was specified.
    pub distinct: bool,
    /// Whether an explicit `ALL` was specified (preserved on restore).
    pub all: bool,
    /// The select list.
    pub fields: SelectFieldList,
    /// The expression lists for standalone `VALUES ROW(...), ...`. Each
    /// element is one row and preserves its zero-or-more expressions exactly;
    /// empty rows are valid in this grammar. Empty for `SELECT` and `TABLE`.
    pub values: Vec<Vec<Expr>>,
    /// The `FROM` clause join tree, if any.
    pub from: Option<Join>,
    /// The `WHERE` predicate, if any.
    pub where_clause: Option<Expr>,
    /// The `GROUP BY` expressions.
    pub group_by: Vec<GroupByItem>,
    /// Whether a trailing `WITH ROLLUP` was written (real MySQL/TiDB
    /// grammar requires at least one `GROUP BY` item first — a bare
    /// `GROUP BY WITH ROLLUP` is a genuine `ParseError` — so this is
    /// always `false` when `group_by` is empty). Confirmed via `gorun`
    /// that this has a REAL, multi-level semantic effect on real TiDB's
    /// own result rows (super-aggregate rows, one per `GROUP BY` prefix
    /// length, with the rolled-up columns showing `NULL`) — this crate's
    /// own execution deliberately does NOT replicate that (a clean
    /// `Unsupported` rather than a partial/incorrect implementation),
    /// while parse/restore fidelity is complete.
    pub rollup: bool,
    /// The `HAVING` predicate, if any.
    pub having: Option<Expr>,
    /// The `WINDOW name AS (...), ...` clause's own named-window
    /// definitions, in written order (order doesn't affect resolution —
    /// a later entry may reference an earlier OR later one by name,
    /// confirmed via `gorun` — but written order is preserved for
    /// restore fidelity).
    pub windows: Vec<(String, WindowDef)>,
    /// The `ORDER BY` items.
    pub order_by: Vec<OrderItem>,
    /// The `LIMIT` clause, if any.
    pub limit: Option<Limit>,
    /// A `FOR UPDATE` / `FOR SHARE` / `LOCK IN SHARE MODE` locking
    /// clause, if any. `ORDER BY`/`LIMIT`/this clause may be written in
    /// ANY relative order (confirmed via `godump restore`: `LIMIT 1
    /// ORDER BY a` and `FOR UPDATE ORDER BY a` both parse, each
    /// restoring in the FIXED canonical order this struct's own fields
    /// are always printed in, regardless of how they were written) —
    /// `tidb_parser`'s own tail-parsing loop reflects this directly
    /// rather than assuming a fixed grammar order. For a `UNION`/
    /// `EXCEPT`/`INTERSECT` query, a trailing locking clause after the
    /// LAST (unparenthesized) term is ambiguous between "this term's
    /// own" and "the whole statement's own" — real MySQL/TiDB always
    /// resolves it as the LATTER (confirmed via `godump restore`), so
    /// [`SetOprStmt`] carries its OWN separate `lock` field for exactly
    /// that position; this field here is only ever populated for a
    /// plain `SELECT`, or for a NON-final term of a set operation
    /// (disambiguated by a following set operator, so real MySQL/TiDB
    /// DOES attach it to that individual term there — confirmed via
    /// `godump restore`: `t1 FOR UPDATE UNION t2` restores with `FOR
    /// UPDATE` immediately after `t1`, not hoisted to the end).
    pub lock: Option<SelectLock>,
    /// The complete trailing `INTO OUTFILE` payload.
    pub into_outfile: Option<SelectIntoOption>,
    /// `INTO @var [, @var ...]` — Go `SelectIntoVars`. Restore note: Go's
    /// `SelectIntoOption.Restore` errors on this type ("Unsupported
    /// SelectionInto type"); this port restores the natural MySQL text
    /// instead, a divergence visible only through a path Go cannot take.
    pub into_vars: Vec<String>,
}

/// Go's `SelectIntoOption` payload. Only OUTFILE is restorable in Go; its
/// optional FIELDS and LINES clauses share the LOAD DATA grammar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectIntoOption {
    /// Decoded output path.
    pub file_name: String,
    /// Optional field delimiters.
    pub fields: LoadDataFields,
    /// Optional line delimiters.
    pub lines: LoadDataLines,
}

impl SelectIntoOption {
    fn restore_into(&self, out: &mut String) {
        out.push_str("INTO OUTFILE '");
        out.push_str(&escape_string_literal(&self.file_name));
        out.push('\'');
        self.fields.restore_into(out);
        self.lines.restore_into(out);
    }
}

impl SelectStmt {
    /// Restores only this query's select field list using Go's
    /// `ast.FieldList.Restore` separator (`", "`). The enclosing
    /// `SelectStmt.Restore` intentionally uses a bare comma, so parser tests
    /// that own the FieldList node need this distinct boundary.
    pub fn restore_field_list(&self) -> String {
        let mut out = String::new();
        for (index, field) in self.fields.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            field.restore_into(&mut out);
        }
        out
    }

    pub(crate) fn restore_into_with_context(&self, out: &mut String, context: &RestoreContext) {
        if self.kind == SelectStatementKind::Table {
            // Go's `parseTableStmt` constructs the same select-shaped AST
            // (wildcard field plus one table source), but its Kind restores
            // `TABLE` rather than an equivalent `SELECT * FROM` spelling.
            if let Some(with) = &self.with {
                let scoped = with.restore_into_with_context(out, context);
                out.push(' ');
                self.restore_table_body(out, &scoped);
                return;
            }
            self.restore_table_body(out, context);
            return;
        }
        if self.kind == SelectStatementKind::Values {
            if self.is_in_braces {
                out.push('(');
            }
            out.push_str("VALUES ");
            for (row_index, row) in self.values.iter().enumerate() {
                if row_index > 0 {
                    out.push_str(", ");
                }
                out.push_str("ROW(");
                for (value_index, value) in row.iter().enumerate() {
                    if value_index > 0 {
                        out.push(',');
                    }
                    value.restore_into_with_context(out, context);
                }
                out.push(')');
            }
            restore_order_by(out, &self.order_by);
            restore_limit(out, &self.limit);
            restore_lock(out, &self.lock);
            if let Some(into) = &self.into_outfile {
                out.push(' ');
                into.restore_into(out);
            }
            if !self.into_vars.is_empty() {
                out.push_str(" INTO ");
                for (position, name) in self.into_vars.iter().enumerate() {
                    if position > 0 {
                        out.push_str(", ");
                    }
                    out.push('@');
                    out.push_str(name);
                }
            }
            if self.is_in_braces {
                out.push(')');
            }
            return;
        }
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
        out.push_str("SELECT ");
        self.priority.restore_into(out);
        if self.sql_small_result {
            out.push_str("SQL_SMALL_RESULT ");
        }
        if self.sql_big_result {
            out.push_str("SQL_BIG_RESULT ");
        }
        if self.sql_buffer_result {
            out.push_str("SQL_BUFFER_RESULT ");
        }
        if self.sql_no_cache {
            out.push_str("SQL_NO_CACHE ");
        }
        if self.calc_found_rows {
            out.push_str("SQL_CALC_FOUND_ROWS ");
        }
        if !self.hints.is_empty() {
            out.push_str("/*+ ");
            for (i, h) in self.hints.iter().enumerate() {
                if i > 0 {
                    out.push(' ');
                }
                h.restore_into(out);
            }
            out.push_str("*/ ");
        }
        if self.distinct {
            out.push_str("DISTINCT ");
        } else if self.all {
            out.push_str("ALL ");
        }
        if self.straight_join {
            out.push_str("STRAIGHT_JOIN ");
        }
        for (i, f) in self.fields.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            f.restore_into_with_context(out, context);
        }
        if let Some(from) = &self.from {
            out.push_str(" FROM ");
            from.restore_into_with_context(out, context);
        } else if self.where_clause.is_some() {
            // `WHERE` requires a table, so a table-less query with a predicate
            // restores the placeholder `FROM DUAL` (SelectStmt.Restore in
            // pkg/parser/ast/dml.go).
            out.push_str(" FROM DUAL");
        }
        if let Some(w) = &self.where_clause {
            out.push_str(" WHERE ");
            w.restore_into_with_context(out, context);
        }
        if !self.group_by.is_empty() {
            out.push_str(" GROUP BY ");
            for (i, item) in self.group_by.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                restore_by_item_expr_with_context(&item.expr, out, context);
                // An explicit `ASC` restores identically to no direction
                // at all (confirmed via `godump restore`) — only `DESC`
                // ever shows up in the output, even though `tidb_exec`
                // still needs to tell the two apart (see
                // `tidb_ast::GroupByItem`'s own doc).
                if item.desc == Some(true) {
                    out.push_str(" DESC");
                }
            }
            if self.rollup {
                out.push_str(" WITH ROLLUP");
            }
        }
        if let Some(h) = &self.having {
            out.push_str(" HAVING ");
            h.restore_into_with_context(out, context);
        }
        if !self.windows.is_empty() {
            out.push_str(" WINDOW ");
            for (i, (name, def)) in self.windows.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&back_quote(name));
                out.push_str(" AS (");
                restore_window_def(def, out);
                out.push(')');
            }
        }
        restore_order_by(out, &self.order_by);
        restore_limit(out, &self.limit);
        // A plain `SELECT`'s own lock prints AFTER `ORDER BY`/`LIMIT`
        // (real SQL clause order — see `SelectStmt::lock`'s own doc for
        // the opposite `SetOprStmt`-level order).
        restore_lock(out, &self.lock);
        if let Some(into) = &self.into_outfile {
            out.push(' ');
            into.restore_into(out);
        }
        if self.is_in_braces {
            out.push(')');
        }
    }

    fn restore_table_body(&self, out: &mut String, context: &RestoreContext) {
        if self.is_in_braces {
            out.push('(');
        }
        out.push_str("TABLE ");
        self.from
            .as_ref()
            .expect("TABLE statements always own one table source")
            .restore_into_with_context(out, context);
        restore_order_by(out, &self.order_by);
        restore_limit(out, &self.limit);
        restore_lock(out, &self.lock);
        if let Some(into) = &self.into_outfile {
            out.push(' ');
            into.restore_into(out);
        }
        if self.is_in_braces {
            out.push(')');
        }
    }
}

/// A `FOR UPDATE` / `FOR SHARE` / `LOCK IN SHARE MODE` locking clause.
/// `LOCK IN SHARE MODE` is real MySQL's older, simpler syntax for `FOR
/// SHARE` and normalizes to it on restore (confirmed via `godump
/// restore`) — modelled as the same `Share` variant rather than a
/// separate one; `LOCK IN SHARE MODE` itself never accepts `OF`/
/// `NOWAIT`/`SKIP LOCKED` (confirmed via `godump restore`: both are
/// genuine `ParseError`s on that form), so a parser that only reaches
/// those through the `FOR UPDATE`/`FOR SHARE` spelling naturally never
/// needs to reject them separately for the `LOCK IN SHARE MODE` spelling.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectLock {
    /// `UPDATE` or `SHARE`.
    pub kind: LockKind,
    /// The `OF table[, table...]` clause's table names (dotted paths,
    /// e.g. `["db", "t"]`), empty if not given.
    pub of: Vec<Vec<String>>,
    /// `NOWAIT` / `SKIP LOCKED`, if given.
    pub wait: LockWait,
}

/// [`SelectLock`]'s own lock strength.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockKind {
    /// `FOR UPDATE`.
    Update,
    /// `FOR SHARE` (or the equivalent `LOCK IN SHARE MODE` spelling — see
    /// [`SelectLock`]'s own doc).
    Share,
}

/// [`SelectLock`]'s own wait behavior when a row is already locked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockWait {
    /// Neither `NOWAIT` nor `SKIP LOCKED` was written (block and wait,
    /// real MySQL/TiDB's own default).
    Default,
    /// `NOWAIT`.
    NoWait,
    /// `SKIP LOCKED`.
    SkipLocked,
    /// `WAIT N`, accepted only by `FOR UPDATE`.
    Wait(u64),
}

/// Restores a locking clause (nothing if absent) — shared by
/// [`SelectStmt`] and [`SetOprStmt`], which call it at different points
/// relative to `ORDER BY`/`LIMIT` (see each impl's own comment).
/// `LOCK IN SHARE MODE` always restores as `FOR SHARE` (see
/// [`SelectLock`]'s own doc — both spellings share one
/// `LockKind::Share` variant, so there's no separate case to handle
/// here).
fn restore_lock(out: &mut String, lock: &Option<SelectLock>) {
    let Some(lock) = lock else { return };
    out.push_str(match lock.kind {
        LockKind::Update => " FOR UPDATE",
        LockKind::Share => " FOR SHARE",
    });
    if !lock.of.is_empty() {
        out.push_str(" OF ");
        for (i, path) in lock.of.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            for (j, part) in path.iter().enumerate() {
                if j > 0 {
                    out.push('.');
                }
                out.push_str(&back_quote(part));
            }
        }
    }
    match lock.wait {
        LockWait::Default => {}
        LockWait::NoWait => out.push_str(" NOWAIT"),
        LockWait::SkipLocked => out.push_str(" SKIP LOCKED"),
        LockWait::Wait(seconds) => {
            out.push_str(" WAIT ");
            out.push_str(&seconds.to_string());
        }
    }
}

/// A `WITH [RECURSIVE] name [(col, ...)] AS (query) [, ...]` clause. When
/// `recursive` is `true`, an individual CTE within the clause may (but need
/// not — `b AS (SELECT * FROM a)` inside a `WITH RECURSIVE` clause is
/// ordinary and non-self-referencing, confirmed via `gorun`) actually be
/// recursive: see `tidb_exec::Database`'s own recursive-CTE evaluation doc
/// for the exact rules (base term, recursive term(s), the `UNION`/`UNION
/// ALL` fixpoint iteration, and the scope boundaries — self-join, an
/// aggregate, `DISTINCT`, or `ORDER BY` inside a recursive term are all
/// genuine `ERR`s in real TiDB, confirmed via `gorun`, not silently
/// accepted here either).
#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
    /// Whether `RECURSIVE` was specified.
    pub recursive: bool,
    /// Each named common table expression, in written order (a later one
    /// may reference an earlier one, even when non-recursive).
    pub ctes: Vec<Cte>,
}

impl WithClause {
    /// Restores the CTE prefix itself, without the separating space before
    /// the query it prefixes. Shared by plain `SELECT` and `SetOprStmt` so
    /// their syntax cannot drift.
    pub(crate) fn restore_into_with_context(
        &self,
        out: &mut String,
        context: &RestoreContext,
    ) -> RestoreContext {
        out.push_str("WITH ");
        if self.recursive {
            out.push_str("RECURSIVE ");
        }
        let mut scoped = context.clone();
        for (i, cte) in self.ctes.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            out.push_str(&back_quote(&cte.name));
            if !cte.columns.is_empty() {
                out.push_str(" (");
                for (j, col) in cte.columns.iter().enumerate() {
                    if j > 0 {
                        out.push_str(", ");
                    }
                    out.push_str(&back_quote(col));
                }
                out.push(')');
            }
            out.push_str(" AS (");
            scoped = scoped.with_cte(&cte.name);
            cte.query.restore_into_with_context(out, &scoped);
            out.push(')');
        }
        scoped
    }
}

/// One `name [(col, ...)] AS (query)` common table expression. `query` is
/// `QueryStmt::Select` for a plain (non-`UNION`) body, or `QueryStmt::SetOpr` for a
/// `UNION`/`UNION ALL`-joined body (needed for `WITH RECURSIVE`'s `base
/// UNION [ALL] recursive` shape, but also legal for an ordinary
/// non-recursive CTE) — never any other `Stmt` variant, since
/// `parse_select_or_setopr` (the only parser entry point that builds this
/// field) can only ever produce one of those two.
#[derive(Debug, Clone, PartialEq)]
pub struct Cte {
    /// The CTE's name, referenceable in `FROM` like an ordinary table.
    pub name: String,
    /// An explicit column rename list, if given (`WITH a (m, n) AS
    /// ...`); empty if the query's own column names/aliases are used
    /// as-is.
    pub columns: Vec<String>,
    /// The CTE's own query.
    pub query: crate::NodeBox<QueryStmt>,
}

/// One `ORDER BY` item.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderItem {
    /// The ordering expression.
    pub expr: Expr,
    /// Whether descending (`DESC`); ascending is the default and omitted on
    /// restore.
    pub desc: bool,
}

impl OrderItem {
    pub(crate) fn restore_into(&self, out: &mut String) {
        restore_by_item_expr(&self.expr, out);
        if self.desc {
            out.push_str(" DESC");
        }
    }
}

/// Restores a `GROUP BY`/`ORDER BY` item's own expression. A bare `TRUE`/
/// `FALSE` literal restores as its plain integer form (`1`/`0`) in these two
/// positions specifically — confirmed via `godump restore`: `GROUP BY true`
/// / `ORDER BY true` restore as `GROUP BY 1`/`ORDER BY 1`, even though
/// `Expr::Bool` restores as `TRUE`/`FALSE` everywhere else (`SELECT TRUE`,
/// `WHERE TRUE`, ...). This is purely a restore-text quirk, not a semantic
/// judgment of positional validity — `FALSE` restores as `0` here too, even
/// though position `0` is itself a runtime error (see
/// `tidb_exec::order::positional`). Every other expression restores
/// normally.
fn restore_by_item_expr(expr: &Expr, out: &mut String) {
    restore_by_item_expr_with_context(expr, out, &RestoreContext::default());
}

fn restore_by_item_expr_with_context(expr: &Expr, out: &mut String, context: &RestoreContext) {
    match expr {
        Expr::Bool(b) => out.push_str(if *b { "1" } else { "0" }),
        _ => expr.restore_into_with_context(out, context),
    }
}

/// Restores an `ORDER BY` clause (nothing if empty).
fn restore_order_by(out: &mut String, items: &[OrderItem]) {
    if items.is_empty() {
        return;
    }
    out.push_str(" ORDER BY ");
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        item.restore_into(out);
    }
}

/// One `GROUP BY` item. Unlike [`OrderItem`]'s own plain `bool` (`ORDER
/// BY` never distinguishes an unwritten direction from an explicit
/// `ASC`), `desc` here is a THREE-state `Option<bool>` — `None` (no
/// direction written at all) is the only case real MySQL/TiDB executes
/// normally by default; an EXPLICIT `ASC` (`Some(false)`) is rejected at
/// EXECUTION time exactly the same way `DESC` (`Some(true)`) is
/// (confirmed via `gorun`: both produce `[expression:1235] function
/// GROUP BY expr ASC|DESC has only noop implementation in tidb now`,
/// unless the `tidb_enable_noop_functions` session variable — not
/// modelled by this crate at all — is set), so this crate's own
/// `tidb_exec` must be able to tell "no direction" apart from "ASC was
/// explicitly written" even though restore itself only ever shows `DESC`
/// (`ASC`, written or not, restores identically to no direction at all).
#[derive(Debug, Clone, PartialEq)]
pub struct GroupByItem {
    /// The grouping expression.
    pub expr: Expr,
    /// `None` if no direction was written; `Some(true)` for `DESC`,
    /// `Some(false)` for an explicit `ASC` — see this struct's own doc
    /// for why the explicit-`ASC` case must be distinguishable from "no
    /// direction" despite restoring identically.
    pub desc: Option<bool>,
}

/// A `LIMIT` clause.
#[derive(Debug, Clone, PartialEq)]
pub struct Limit {
    /// The optional offset (`LIMIT offset, count`).
    pub offset: Option<Expr>,
    /// The row count.
    pub count: Expr,
}

impl Limit {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str(" LIMIT ");
        if let Some(off) = &self.offset {
            off.restore_into(out);
            out.push(',');
        }
        self.count.restore_into(out);
    }
}

/// Restores a `LIMIT` clause (nothing if absent).
fn restore_limit(out: &mut String, limit: &Option<Limit>) {
    if let Some(l) = limit {
        l.restore_into(out);
    }
}

/// One entry in a select list.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectField {
    /// `*`, or a qualified `t.*` / `db.t.*` wildcard.
    Wildcard(Vec<String>),
    /// An expression, optionally aliased with `AS`.
    Expr {
        /// The projected expression.
        expr: Expr,
        /// The `AS` alias, if present.
        alias: Option<String>,
    },
}

/// A SELECT/RETURNING field list together with the source metadata Go embeds
/// in each `SelectField` node.
///
/// The semantic AST remains a contiguous `SelectField` slice for planners and
/// executors. Parser-only source context is stored beside that slice under the
/// same owner, avoiding wrappers throughout every consumer.
#[derive(Debug, Clone, Default)]
pub struct SelectFieldList {
    fields: Vec<SelectField>,
    text: Vec<NodeText>,
    written_literal: Vec<bool>,
    projection_offsets: Vec<Option<usize>>,
}

impl SelectFieldList {
    /// The select fields as a contiguous slice.
    #[must_use]
    pub fn fields(&self) -> &[SelectField] {
        &self.fields
    }

    /// The select fields as a mutable slice, for a pass that rewrites a
    /// field's expression in place. The count and the per-field source text
    /// are unchanged, so the restore metadata stays aligned.
    pub fn fields_mut(&mut self) -> &mut [SelectField] {
        &mut self.fields
    }

    /// Appends a field with empty source metadata.
    pub fn push(&mut self, field: SelectField) {
        self.written_literal.push(was_written_as_literal(&field));
        self.projection_offsets.push(None);
        self.fields.push(field);
        self.text.push(NodeText::default());
    }

    /// Appends a field and records its exact source bytes.
    pub fn push_with_text(&mut self, field: SelectField, source: impl Into<Vec<u8>>) {
        self.push_with_text_and_projection_offset(field, source, None);
    }

    /// Appends a parsed field with its source bytes and the decoded byte
    /// length of the first adjacent string literal, when concatenation
    /// occurred.
    pub fn push_with_text_and_projection_offset(
        &mut self,
        field: SelectField,
        source: impl Into<Vec<u8>>,
        projection_offset: Option<usize>,
    ) {
        let mut text = NodeText::default();
        text.set_text(None, source);
        self.written_literal.push(was_written_as_literal(&field));
        self.projection_offsets.push(projection_offset);
        self.fields.push(field);
        self.text.push(text);
    }

    /// Returns decoded source text for field `index`.
    pub fn text(&self, index: usize) -> Option<&[u8]> {
        self.text.get(index).map(NodeText::text)
    }

    /// Returns exact source bytes for field `index`.
    pub fn original_text(&self, index: usize) -> Option<&[u8]> {
        self.text.get(index).map(NodeText::original_text)
    }

    /// Whether field `index` was WRITTEN as a literal -- whether the parsed
    /// expression, looked through parentheses and a unary `+`, was one of
    /// Go's `driver.ValueExpr` nodes.
    ///
    /// Go asks this of `field.Expr` in
    /// `buildProjectionFieldNameFromExpressions`, which decides whether an
    /// unaliased column is named by the literal's VALUE or by the field's
    /// source text. A `SelectField`'s expression is rewritten in place by
    /// later passes -- variable binding substitutes `@@x` with its value,
    /// subquery folding substitutes `(select 1)` with `1` -- so by the time
    /// the name is chosen the expression alone can no longer tell a written
    /// literal from a computed one. Recorded here at construction, beside the
    /// source text and under the same alignment guarantee, it can.
    #[must_use]
    pub fn written_literal(&self, index: usize) -> bool {
        self.written_literal.get(index).copied().unwrap_or(false)
    }

    /// Decoded byte length of the first adjacent string literal for field
    /// `index`, or `None` when the field was not formed by concatenation.
    #[must_use]
    pub fn projection_offset(&self, index: usize) -> Option<usize> {
        self.projection_offsets.get(index).copied().flatten()
    }
}

/// Whether `field`'s expression, after Go's
/// `getInnerFromParenthesesAndUnaryPlus`, is one of its `driver.ValueExpr`
/// literals.
fn was_written_as_literal(field: &SelectField) -> bool {
    let SelectField::Expr { expr, .. } = field else {
        return false;
    };
    let mut inner = expr;
    while let Expr::Paren(next) | Expr::Unary(crate::UnaryOp::Plus, next) = inner {
        inner = next;
    }
    matches!(
        inner,
        Expr::Null
            | Expr::Int(_)
            | Expr::Decimal(_)
            | Expr::Float(_)
            | Expr::Hex(_)
            | Expr::Bit(_)
            | Expr::String(_)
            | Expr::RawString(_)
            | Expr::Bool(_)
    )
}

impl From<Vec<SelectField>> for SelectFieldList {
    fn from(fields: Vec<SelectField>) -> Self {
        let text = vec![NodeText::default(); fields.len()];
        let written_literal = fields.iter().map(was_written_as_literal).collect();
        let projection_offsets = vec![None; fields.len()];
        Self {
            fields,
            text,
            written_literal,
            projection_offsets,
        }
    }
}

impl std::ops::Deref for SelectFieldList {
    type Target = [SelectField];

    fn deref(&self) -> &Self::Target {
        &self.fields
    }
}

impl std::ops::DerefMut for SelectFieldList {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.fields
    }
}

impl IntoIterator for SelectFieldList {
    type Item = SelectField;
    type IntoIter = std::vec::IntoIter<SelectField>;

    fn into_iter(self) -> Self::IntoIter {
        self.fields.into_iter()
    }
}

impl<'a> IntoIterator for &'a SelectFieldList {
    type Item = &'a SelectField;
    type IntoIter = std::slice::Iter<'a, SelectField>;

    fn into_iter(self) -> Self::IntoIter {
        self.fields.iter()
    }
}

impl<'a> IntoIterator for &'a mut SelectFieldList {
    type Item = &'a mut SelectField;
    type IntoIter = std::slice::IterMut<'a, SelectField>;

    fn into_iter(self) -> Self::IntoIter {
        self.fields.iter_mut()
    }
}

impl PartialEq for SelectFieldList {
    fn eq(&self, other: &Self) -> bool {
        self.fields == other.fields
    }
}

impl SelectField {
    pub(crate) fn restore_into(&self, out: &mut String) {
        self.restore_into_with_context(out, &RestoreContext::default());
    }

    pub(crate) fn restore_into_with_context(&self, out: &mut String, context: &RestoreContext) {
        match self {
            SelectField::Wildcard(path) => {
                for q in path {
                    out.push_str(&back_quote(q));
                    out.push('.');
                }
                out.push('*');
            }
            SelectField::Expr { expr, alias } => {
                expr.restore_into_with_context(out, context);
                // An alias whose text is the empty string (`` `` ``)
                // restores identically to no alias at all — confirmed via
                // `godump restore` and matching `SelectField.Restore` in
                // real TiDB's own `pkg/parser/ast/dml.go` (`AsName` is a
                // plain, non-optional `CIStr` there, so "absent" and
                // "written empty" are the SAME value; this AST's own
                // `Option<String>` distinguishes them at the type level,
                // but restore collapses both to nothing).
                if let Some(a) = alias.as_deref().filter(|a| !a.is_empty()) {
                    out.push_str(" AS ");
                    out.push_str(&back_quote(a));
                }
            }
        }
    }
}

/// Restores a `PARTITION (name, ...)` clause (nothing if `names` is
/// empty) — shared by [`TableRef`] and `tidb_ast::InsertStmt`, which both
/// accept it at different points in their own grammar. NO space between
/// `PARTITION` and the opening paren (confirmed via `godump restore`,
/// unlike an index hint's own space before its paren list).
pub(crate) fn restore_partition_clause(out: &mut String, names: &[String]) {
    if names.is_empty() {
        return;
    }
    out.push_str(" PARTITION(");
    for (i, name) in names.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(&back_quote(name));
    }
    out.push(')');
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for SelectStatementKind {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Select => {}
            Self::Table => {}
            Self::Values => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for SelectStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            kind,
            is_in_braces,
            with,
            hints,
            priority,
            sql_small_result,
            sql_big_result,
            sql_buffer_result,
            sql_no_cache,
            straight_join,
            calc_found_rows,
            distinct,
            all,
            fields,
            values,
            from,
            where_clause,
            group_by,
            rollup,
            having,
            windows,
            order_by,
            limit,
            lock,
            into_outfile,
            into_vars: _,
        } = self;
        if !crate::Visitable::accept(kind, visitor) {
            return false;
        }
        if let Some(value) = with.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        for value in hints.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if !crate::Visitable::accept(priority, visitor) {
            return false;
        }
        for value in fields.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        for value in values.iter_mut() {
            for value in value.iter_mut() {
                if !crate::Visitable::accept(value, visitor) {
                    return false;
                }
            }
        }
        if let Some(value) = from.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = where_clause.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        for value in group_by.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = having.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        for value in windows.iter_mut() {
            if !crate::Visitable::accept(&mut value.1, visitor) {
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
        if let Some(value) = into_outfile.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = kind;
        let _ = is_in_braces;
        let _ = with;
        let _ = hints;
        let _ = priority;
        let _ = sql_small_result;
        let _ = sql_big_result;
        let _ = sql_buffer_result;
        let _ = sql_no_cache;
        let _ = straight_join;
        let _ = calc_found_rows;
        let _ = distinct;
        let _ = all;
        let _ = fields;
        let _ = values;
        let _ = from;
        let _ = where_clause;
        let _ = group_by;
        let _ = rollup;
        let _ = having;
        let _ = windows;
        let _ = order_by;
        let _ = limit;
        let _ = lock;
        let _ = into_outfile;
        visitor.leave(self)
    }
}

impl crate::Visitable for SelectIntoOption {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            file_name,
            fields,
            lines,
        } = self;
        if !crate::Visitable::accept(fields, visitor) {
            return false;
        }
        if !crate::Visitable::accept(lines, visitor) {
            return false;
        }
        let _ = file_name;
        let _ = fields;
        let _ = lines;
        visitor.leave(self)
    }
}

impl crate::Visitable for SelectLock {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { kind, of, wait } = self;
        if !crate::Visitable::accept(kind, visitor) {
            return false;
        }
        if !crate::Visitable::accept(wait, visitor) {
            return false;
        }
        let _ = kind;
        let _ = of;
        let _ = wait;
        visitor.leave(self)
    }
}

impl crate::Visitable for LockKind {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Update => {}
            Self::Share => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for LockWait {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Default => {}
            Self::NoWait => {}
            Self::SkipLocked => {}
            Self::Wait(_) => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for WithClause {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { recursive, ctes } = self;
        for value in ctes.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = recursive;
        let _ = ctes;
        visitor.leave(self)
    }
}

impl crate::Visitable for Cte {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            name,
            columns,
            query,
        } = self;
        if !crate::Visitable::accept(query.as_mut(), visitor) {
            return false;
        }
        let _ = name;
        let _ = columns;
        let _ = query;
        visitor.leave(self)
    }
}

impl crate::Visitable for OrderItem {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { expr, desc } = self;
        if !crate::Visitable::accept(expr, visitor) {
            return false;
        }
        let _ = expr;
        let _ = desc;
        visitor.leave(self)
    }
}

impl crate::Visitable for GroupByItem {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { expr, desc } = self;
        if !crate::Visitable::accept(expr, visitor) {
            return false;
        }
        let _ = expr;
        let _ = desc;
        visitor.leave(self)
    }
}

impl crate::Visitable for Limit {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { offset, count } = self;
        if let Some(value) = offset.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if !crate::Visitable::accept(count, visitor) {
            return false;
        }
        let _ = offset;
        let _ = count;
        visitor.leave(self)
    }
}

impl crate::Visitable for SelectField {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Wildcard(field_0) => {
                let _ = field_0;
            }
            Self::Expr { expr, alias } => {
                if !crate::Visitable::accept(expr, visitor) {
                    return false;
                }
                let _ = expr;
                let _ = alias;
            }
        }
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
