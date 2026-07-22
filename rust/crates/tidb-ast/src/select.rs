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

//! `SELECT` statements (select list, `FROM` join tree, set operations,
//! `WITH`/window/lock clauses) and their restore.

use crate::util::{back_quote, escape_string_literal};
use crate::{Expr, LoadDataFields, LoadDataLines, NodeText, QueryStmt, StatementPriority};

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
}

impl SetOprStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        if let Some(with) = &self.with {
            with.restore_into(out);
            out.push(' ');
        }
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
                term.body.restore_into(out);
                out.push(')');
            } else {
                term.body.restore_into(out);
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
        if self.is_in_braces {
            out.push(')');
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
    fn restore_into(&self, out: &mut String) {
        match self {
            SetOprTermBody::Select(sel) => sel.restore_into(out),
            SetOprTermBody::Nested(so) => so.restore_into(out),
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

    pub(crate) fn restore_into(&self, out: &mut String) {
        if self.kind == SelectStatementKind::Table {
            // Go's `parseTableStmt` constructs the same select-shaped AST
            // (wildcard field plus one table source), but its Kind restores
            // `TABLE` rather than an equivalent `SELECT * FROM` spelling.
            if let Some(with) = &self.with {
                with.restore_into(out);
                out.push(' ');
            }
            out.push_str("TABLE ");
            self.from
                .as_ref()
                .expect("TABLE statements always own one table source")
                .restore_into(out);
            restore_order_by(out, &self.order_by);
            restore_limit(out, &self.limit);
            restore_lock(out, &self.lock);
            if let Some(into) = &self.into_outfile {
                out.push(' ');
                into.restore_into(out);
            }
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
                    value.restore_into(out);
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
            if self.is_in_braces {
                out.push(')');
            }
            return;
        }
        if let Some(with) = &self.with {
            with.restore_into(out);
            out.push(' ');
        }
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
            f.restore_into(out);
        }
        if let Some(from) = &self.from {
            out.push_str(" FROM ");
            from.restore_into(out);
        } else if self.where_clause.is_some() {
            // `WHERE` requires a table, so a table-less query with a predicate
            // restores the placeholder `FROM DUAL` (SelectStmt.Restore in
            // pkg/parser/ast/dml.go).
            out.push_str(" FROM DUAL");
        }
        if let Some(w) = &self.where_clause {
            out.push_str(" WHERE ");
            w.restore_into(out);
        }
        if !self.group_by.is_empty() {
            out.push_str(" GROUP BY ");
            for (i, item) in self.group_by.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                restore_by_item_expr(&item.expr, out);
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
            h.restore_into(out);
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
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("WITH ");
        if self.recursive {
            out.push_str("RECURSIVE ");
        }
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
            cte.query.restore_into(out);
            out.push(')');
        }
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
    match expr {
        Expr::Bool(b) => out.push_str(if *b { "1" } else { "0" }),
        _ => expr.restore_into(out),
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
}

impl SelectFieldList {
    /// Appends a field with empty source metadata.
    pub fn push(&mut self, field: SelectField) {
        self.fields.push(field);
        self.text.push(NodeText::default());
    }

    /// Appends a field and records its exact source bytes.
    pub fn push_with_text(&mut self, field: SelectField, source: impl Into<Vec<u8>>) {
        let mut text = NodeText::default();
        text.set_text(None, source);
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
}

impl From<Vec<SelectField>> for SelectFieldList {
    fn from(fields: Vec<SelectField>) -> Self {
        let text = vec![NodeText::default(); fields.len()];
        Self { fields, text }
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
        match self {
            SelectField::Wildcard(path) => {
                for q in path {
                    out.push_str(&back_quote(q));
                    out.push('.');
                }
                out.push('*');
            }
            SelectField::Expr { expr, alias } => {
                expr.restore_into(out);
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

/// A table reference in `FROM` (Phase 0: a name path with optional alias).
#[derive(Debug, Clone, PartialEq)]
pub struct TableRef {
    /// The dotted name path, e.g. `["db", "t"]` or `["t"]`.
    pub name: Vec<String>,
    /// An optional `PARTITION (name, ...)` clause restricting which
    /// partitions the query targets — empty if not written. Parses
    /// BEFORE the alias (confirmed via `godump restore`: `t PARTITION
    /// (p0) AS x` is the only order accepted; `t AS x PARTITION (p0)` is
    /// a genuine `ParseError`) — the OPPOSITE relative position from
    /// [`TableRef::hints`], which parse AFTER the alias. Partition names
    /// must be PLAIN identifiers, unlike an index hint's own
    /// keyword-tolerant names (confirmed via `godump restore`:
    /// `PARTITION (key)`/`PARTITION (primary)`/`PARTITION (asc)` are all
    /// genuine `ParseError`s, though `key`/`primary`/`asc` are all valid
    /// INDEX hint names). ALWAYS `Unsupported` at execution time if
    /// non-empty, unconditionally (unlike `hints`' own narrower
    /// divergence): this crate never implements `CREATE TABLE ...
    /// PARTITION BY` at all, so EVERY table here is permanently
    /// "non-partitioned" — real MySQL/TiDB's own error for this exact
    /// situation (`PARTITION () clause on non partitioned table`,
    /// confirmed via `gorun`) therefore applies universally, with no
    /// per-table validation needed the way index-hint names would need.
    pub partitions: Vec<String>,
    /// The table alias, if present.
    pub alias: Option<String>,
    /// An `AS OF TIMESTAMP expr` clause — TiDB's own stale-read/time-travel
    /// syntax, reading the table as of a historical MVCC snapshot rather
    /// than the current one. Mutually exclusive with `alias` at the SAME
    /// grammar position (confirmed via `godump restore`: `t AS OF
    /// TIMESTAMP @a AS x` and `t x AS OF TIMESTAMP @a` are BOTH genuine
    /// `ParseError`s, in either order) — read directly from real TiDB's
    /// own hand-written parser (`pkg/parser/join_parser.go`'s inline
    /// handling, `if p.peek().Tp == asof { ... } else if
    /// p.accept(as) { ...alias... }`) and restore
    /// (`pkg/parser/ast/dml.go`'s `TableSource.Restore`: name →
    /// partitions → alias → `AS OF TIMESTAMP` → index hints →
    /// `TABLESAMPLE`, the SAME relative order this field and
    /// [`TableRef::sample`] already restore in). Only ever attaches to a
    /// plain table reference, never a derived table (matching
    /// [`TableRef::sample`]'s own scope). ALWAYS `Unsupported` at
    /// execution time, unconditionally — the SAME precedent
    /// [`TableRef::sample`] already established: real MVCC historical
    /// reads have no analogue in this crate's plain, single-version
    /// `Vec<Row>` table representation.
    pub as_of: Option<Box<Expr>>,
    /// `USE`/`FORCE`/`IGNORE INDEX` hints, in written order — MULTIPLE
    /// hints may stack on one table (confirmed via `godump restore`:
    /// `t USE INDEX (a) IGNORE INDEX (b)`), each a complete, independent
    /// unit (a following scope qualifier cannot be chained onto a
    /// PRIOR hint without repeating its own `USE`/`FORCE`/`IGNORE
    /// INDEX` keyword — confirmed `!ERR` otherwise). NOT VALIDATED
    /// against this table's own real indexes at execution time — a
    /// deliberate, narrower divergence from real MySQL/TiDB (which
    /// errors `Key '...' doesn't exist in table '...'` for an unknown
    /// name): this crate's `Table` doesn't track index NAMES at all
    /// currently (only unnamed `PRIMARY`/`UNIQUE` column-index groups
    /// for conflict detection), and index hints never affect a
    /// full-table-scan executor's own RESULT rows either way, matching
    /// the same "parsed but not read at execution" precedent most
    /// `CREATE TABLE` options already follow.
    pub hints: Vec<IndexHint>,
    /// An optional `TABLESAMPLE` clause — see [`TableSample`]'s own doc.
    /// Parses AFTER `hints` (confirmed via `godump restore`: `t USE INDEX
    /// (a) TABLESAMPLE REGION ()` is the only order accepted).
    pub sample: Option<TableSample>,
}

impl TableRef {
    pub(crate) fn restore_into(&self, out: &mut String) {
        for (i, part) in self.name.iter().enumerate() {
            if i > 0 {
                out.push('.');
            }
            out.push_str(&back_quote(part));
        }
        restore_partition_clause(out, &self.partitions);
        // See `SelectField::restore_into`'s own comment: an empty-string
        // alias restores identically to no alias, matching real TiDB's
        // `TableSource.Restore`.
        if let Some(a) = self.alias.as_deref().filter(|a| !a.is_empty()) {
            out.push_str(" AS ");
            out.push_str(&back_quote(a));
        }
        if let Some(ts) = &self.as_of {
            out.push_str(" AS OF TIMESTAMP ");
            ts.restore_into(out);
        }
        for hint in &self.hints {
            out.push(' ');
            out.push_str(match hint.kind {
                IndexHintKind::Use => "USE INDEX",
                IndexHintKind::Force => "FORCE INDEX",
                IndexHintKind::Ignore => "IGNORE INDEX",
            });
            out.push_str(match hint.scope {
                IndexHintScope::All => "",
                IndexHintScope::Join => " FOR JOIN",
                IndexHintScope::OrderBy => " FOR ORDER BY",
                IndexHintScope::GroupBy => " FOR GROUP BY",
            });
            out.push_str(" (");
            for (i, name) in hint.indexes.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                out.push_str(&back_quote(name));
            }
            out.push(')');
        }
        if let Some(s) = &self.sample {
            out.push(' ');
            s.restore_into(out);
        }
    }
}

/// `TABLESAMPLE [SYSTEM|BERNOULLI|REGION] (expr [PERCENT|ROWS])
/// [REPEATABLE(seed)]` — a TiDB-specific table-source suffix for
/// approximate, storage-region-boundary-based row sampling. Read directly
/// from real TiDB's own hand-written parser
/// (`pkg/parser/join_parser.go`'s inline `TABLESAMPLE` parsing block,
/// which attaches this ONLY to a plain `*ast.TableName`, never a derived
/// table) and AST restore (`pkg/parser/ast/dml.go`'s `TableSample.Restore`)
/// rather than guessed from restore text alone: the written `REGIONS`
/// spelling (this crate's own [`SampleMethod::Region`]) always restores as
/// the singular `REGION`, confirmed via `godump restore`.
///
/// ALWAYS `Unsupported` at execution time, unconditionally — the SAME
/// precedent [`TableRef::partitions`] already established, for a similar
/// reason: confirmed via `gorun` that `TABLESAMPLE` has a REAL semantic
/// effect on real TiDB's own result rows (tied to actual TiKV storage
/// region boundaries — `SELECT a FROM t TABLESAMPLE REGIONS()` returned
/// only 1 of 5 rows in one probe), which this crate's in-memory `Vec<Row>`
/// table representation has no analogue for; faithfully reproducing it
/// would need a genuine storage-region model, a much larger undertaking
/// than parse/restore fidelity. `SYSTEM`/`BERNOULLI` are read but
/// (confirmed via `gorun`) always reject at EXECUTION time in real TiDB
/// too, since TiKV has no notion of either sampling method — so rejecting
/// unconditionally here doesn't narrow real TiDB's own accepted behavior.
#[derive(Debug, Clone, PartialEq)]
pub struct TableSample {
    /// The sampling method, if written.
    pub method: Option<SampleMethod>,
    /// The sample-size expression, if written (`TABLESAMPLE ()` — an
    /// empty parenthesized clause — is real, valid grammar).
    pub expr: Option<Box<Expr>>,
    /// The `PERCENT`/`ROWS` unit qualifying `expr`, if written.
    pub unit: Option<SampleUnit>,
    /// The `REPEATABLE(seed)` clause's seed expression, if written.
    pub repeatable: Option<Box<Expr>>,
}

impl TableSample {
    fn restore_into(&self, out: &mut String) {
        out.push_str("TABLESAMPLE ");
        match self.method {
            Some(SampleMethod::Bernoulli) => out.push_str("BERNOULLI "),
            Some(SampleMethod::System) => out.push_str("SYSTEM "),
            Some(SampleMethod::Region) => out.push_str("REGION "),
            None => {}
        }
        out.push('(');
        if let Some(e) = &self.expr {
            e.restore_into(out);
        }
        match self.unit {
            Some(SampleUnit::Percent) => out.push_str(" PERCENT"),
            Some(SampleUnit::Rows) => out.push_str(" ROWS"),
            None => {}
        }
        out.push(')');
        if let Some(seed) = &self.repeatable {
            out.push_str(" REPEATABLE(");
            seed.restore_into(out);
            out.push(')');
        }
    }
}

/// A [`TableSample`] sampling method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SampleMethod {
    /// `SYSTEM` — always rejected at execution time in real TiDB too.
    System,
    /// `BERNOULLI` — always rejected at execution time in real TiDB too.
    Bernoulli,
    /// Written as `REGION` or `REGIONS` — both restore as `REGION`.
    Region,
}

/// A [`TableSample`] sample-size unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SampleUnit {
    /// `PERCENT`.
    Percent,
    /// `ROWS`.
    Rows,
}

/// One `USE`/`FORCE`/`IGNORE INDEX` hint — see [`TableRef::hints`]'s own
/// doc for the execution-time scope boundary.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexHint {
    /// `USE` / `FORCE` / `IGNORE`.
    pub kind: IndexHintKind,
    /// The optional `FOR JOIN`/`FOR ORDER BY`/`FOR GROUP BY` scope
    /// qualifier restricting when the hint applies; `All` if omitted.
    pub scope: IndexHintScope,
    /// The hinted index names — may be EMPTY (`USE INDEX ()` is real,
    /// valid MySQL grammar meaning "use no index at all," confirmed via
    /// `godump restore`), and each name may be a keyword-shaped
    /// identifier (`primary`, `key`, `asc`, ... all confirmed valid via
    /// `godump restore`).
    pub indexes: Vec<String>,
}

/// [`IndexHint`]'s own kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexHintKind {
    /// `USE INDEX` (or the `USE KEY` synonym, which normalizes to this on
    /// restore, confirmed via `godump restore`).
    Use,
    /// `FORCE INDEX` (or `FORCE KEY`).
    Force,
    /// `IGNORE INDEX` (or `IGNORE KEY`).
    Ignore,
}

/// [`IndexHint`]'s own optional scope qualifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexHintScope {
    /// No `FOR ...` qualifier written — the hint applies everywhere.
    All,
    /// `FOR JOIN`.
    Join,
    /// `FOR ORDER BY`.
    OrderBy,
    /// `FOR GROUP BY`.
    GroupBy,
}

/// One optimizer hint from a `SELECT`'s own `/*+ ... */` comment (see
/// [`SelectStmt::hints`]'s own doc for the position boundary). Real TiDB
/// has a genuinely separate, dedicated hint grammar covering roughly 30
/// distinct hint names across many argument shapes (`pkg/parser/
/// hintparser.go`, ~1200 lines of its own hand-written mini-parser); this
/// models the shapes confirmed — via a stratified sample of real TiDB's
/// own integration-test corpus, read directly rather than guessed, and
/// grown incrementally as new samples surfaced more shapes — to cover
/// the overwhelming majority of real-world hint occurrences by volume:
/// join/aggregate-pushdown hints taking a bare table list
/// (`INL_JOIN`/`INL_HASH_JOIN`/`INL_MERGE_JOIN`/`HASH_JOIN`/
/// `HASH_JOIN_BUILD`/`HASH_JOIN_PROBE`/`MERGE_JOIN`/`TIDB_SMJ`/
/// `TIDB_INLJ`/`TIDB_HJ`/`LEADING`, the last requiring at least one
/// table — real TiDB's own `LEADING(table|(...), ...)` grammar also
/// allows a nested parenthesized sub-list per element and a
/// hint-level `@qb` prefix, represented by [`HintKind::Leading`];
/// `LEADING()` remains a genuine `ParseError` here
/// (empty), which real TiDB itself silently drops with a warning
/// rather than erroring, confirmed via `godump restore`), index hints taking a table plus an
/// index-name list (`USE_INDEX`/`USE_INDEX_MERGE`/`IGNORE_INDEX`/
/// `INDEX_LOOKUP_PUSHDOWN`/`ORDER_INDEX` — an EMPTY table list, e.g.
/// `USE_INDEX()`, is likewise a genuine `ParseError` here; real TiDB's
/// own `parseIndexLevelHint` treats it as a syntax error internally too,
/// then silently drops the hint rather than propagating it, the SAME
/// narrower divergence as `LEADING()`), `SET_VAR(name = value)`,
/// `TRUE`/`FALSE`-argument hints (`USE_TOJA`/`USE_CASCADES` — anything
/// other than the `TRUE`/`FALSE` keyword is a genuine `ParseError` here,
/// matching this doc's own narrower, `ParseError`-over-silent-drop
/// convention rather than real TiDB's own silent drop-with-warning),
/// argument-less hints with an optional empty `(...)`
/// (`STREAM_AGG`/`HASH_AGG`/`AGG_TO_COP`/`NO_DECORRELATE`/
/// `NO_INDEX_MERGE`/`IGNORE_PLAN_CACHE`/`LIMIT_TO_COP`/`USE_PLAN_CACHE`/
/// `SEMI_JOIN_REWRITE`/`STRAIGHT_JOIN`), `RESOURCE_GROUP(name)`, `MAX_EXECUTION_TIME`/
/// `NTH_PLAN`'s own `([@qb_name] N)` numeric-argument shape, and
/// `QB_NAME(name [, view...])` (see [`HintKind::QbName`]'s own doc).
/// `MERGE` is a special, isolated case straddling both: it PARSES a
/// table list exactly like `MERGE_JOIN` above, but ALWAYS restores as
/// bare `MERGE()`, discarding the parsed tables entirely — confirmed via
/// `godump restore` (real TiDB's own restore code puts `merge` in its
/// argument-less bucket even though parsing dispatches it through the
/// table-list path) — so [`Hint::kind`] is [`HintKind::Nullary`] for it
/// despite consuming table-list syntax at parse time. A hint name real
/// TiDB's own lexer doesn't recognize AT ALL, or recognizes but always
/// treats as unsupported regardless of args (`NO_MERGE`, a real but
/// genuinely DIFFERENT MySQL compatibility hint distinct from the real,
/// content-bearing `NO_MERGE_JOIN`), is silently DROPPED, matching real
/// TiDB's own behavior exactly (`Parser::is_recognized_hint_token_name`/
/// `is_always_unsupported_hint_name`, called from
/// `Parser::parse_hint_comment` — see their own docs for the exact
/// verified name lists). `READ_FROM_STORAGE` and the handful of other
/// REAL, recognized hint names this crate hasn't implemented full
/// grammar for yet (its own `HintKind` variant/dispatch arm) stay
/// genuine `ParseError`s here instead — a real, narrower, deliberate
/// scope boundary: these DO carry real content in real TiDB (confirmed
/// via `godump restore`), so silently dropping them would risk
/// discarding it rather than a safe `ParseError`.
#[derive(Debug, Clone, PartialEq)]
pub struct Hint {
    /// The canonical (uppercase) hint name.
    pub name: String,
    /// The hint's own argument shape and payload.
    pub kind: HintKind,
}

/// [`Hint`]'s own argument shape.
#[derive(Debug, Clone, PartialEq)]
pub enum HintKind {
    /// An optional query-block name beyond an optional `(...)` — ALWAYS
    /// restores WITH parens regardless of whether the source wrote them
    /// or not (confirmed via `godump restore`: bare `STRAIGHT_JOIN` and
    /// `STRAIGHT_JOIN()` both restore as `STRAIGHT_JOIN()`).
    Nullary {
        /// The optional `@query_block_name` inside the parentheses.
        qb_name: Option<String>,
    },
    /// `NAME([@qb_name] table1, table2, ...)` — a join or aggregate-
    /// pushdown hint; the table list may be empty (`NAME()`). The
    /// leading query-block name is OPTIONAL and, unlike
    /// [`HintTable`]'s own per-table `@qb_name` SUFFIX, is a hint-level
    /// PREFIX read directly from `pkg/parser/hintparser.go`'s
    /// `parseTableLevelHint` (calls the SAME shared `parseQBName()`
    /// [`HintKind::Number`] and [`HintKind::QbName`] already use,
    /// immediately after `(`, before the table list) — confirmed via
    /// `godump restore`: `HASH_JOIN(@sel_1 t2)` restores as `` HASH_JOIN
    /// (@`sel_1` `t2`) ``, the qb name back-quoted, space-separated,
    /// BEFORE the tables. `LEADING` uses [`HintKind::Leading`] because its
    /// recursive groups and optional prefix have a distinct restore shape.
    Tables {
        /// The optional leading query-block name.
        qb_name: Option<String>,
        /// The hinted tables, in written order.
        tables: Vec<HintTable>,
    },
    /// `LEADING([@qb_name] table|(...), ...)` keeps its recursive join-order
    /// tree so restore preserves nested parenthesized groups exactly like
    /// Go's `ast.LeadingList`, rather than flattening the table payload.
    Leading {
        /// Optional hint-level query-block prefix.
        qb_name: Option<String>,
        /// Top-level leading elements in written order.
        elements: Vec<LeadingElement>,
    },
    /// `NAME([@qb_name] table [, idx1, idx2, ...])` — an index hint; `indexes` may
    /// be empty (bare `NAME(table)` is valid, unlike
    /// [`crate::IndexHint`]'s own `USE INDEX ()` shape, which requires
    /// the parens even when empty — these are syntactically unrelated
    /// grammars that happen to share a similar purpose).
    Index {
        /// Optional query-block scope prefix shared by Go's complete
        /// `parseIndexLevelHint` family.
        qb_name: Option<String>,
        /// The hinted table.
        table: HintTable,
        /// The hinted index names, in written order.
        indexes: Vec<String>,
    },
    /// `SET_VAR(name = value)`. `value` always restores as a quoted
    /// string regardless of whether the source wrote it quoted or bare
    /// (confirmed via `godump restore`: `SET_VAR(x=0)` restores as
    /// `SET_VAR(x = '0')`) — `var_name` itself restores UNQUOTED, no
    /// back-quoting (a real asymmetry: real TiDB's own restore uses
    /// `WritePlain` for the name but `WriteString` for the value).
    SetVar {
        /// The session variable's name, as written.
        var_name: String,
        /// The assigned value's own text, as written (case/quoting as
        /// typed — restore always re-quotes it regardless).
        value: String,
    },
    /// `NAME(TRUE)` / `NAME(FALSE)` — `USE_TOJA`/`USE_CASCADES`. Restores
    /// as uppercase `TRUE`/`FALSE` regardless of the source's own casing
    /// (confirmed via `godump restore`: `USE_TOJA(true)` restores as
    /// `USE_TOJA(TRUE)`). Anything other than the `TRUE`/`FALSE` keyword
    /// inside the parens (`USE_TOJA(1)`, `USE_TOJA()`) is a genuine
    /// `ParseError` here — real TiDB itself silently drops the whole
    /// hint with a warning instead, the SAME narrower,
    /// `ParseError`-over-silent-drop convention already applied to
    /// `LEADING()`.
    Bool {
        /// Optional hint-level query block.
        qb_name: Option<String>,
        /// Boolean argument.
        value: bool,
    },
    /// `NAME(identifier)` — `RESOURCE_GROUP`. A single BARE identifier
    /// argument, always back-quoted on restore (real TiDB's own
    /// `WriteName`, confirmed via `godump restore`: `RESOURCE_GROUP(rg1)`
    /// restores as `` RESOURCE_GROUP(`rg1`) ``) — a genuinely narrower
    /// shape than [`HintTable`] (no `@qb_name` suffix accepted on the
    /// argument itself; `RESOURCE_GROUP(rg1@sel_1)` is real TiDB's own
    /// silent-drop-with-warning case, confirmed via `godump restore`:
    /// the whole hint vanishes from restore — this project's own
    /// narrower `ParseError`-over-silent-drop convention applies here
    /// too).
    Name {
        /// Optional hint-level query block.
        qb_name: Option<String>,
        /// Identifier argument.
        name: String,
    },
    /// `QUERY_TYPE([@qb] OLAP|OLTP)`.
    Keyword {
        /// Optional hint-level query block.
        qb_name: Option<String>,
        /// Canonical keyword value.
        value: String,
    },
    /// `MEMORY_QUOTA([@qb] n MB|GB)`, stored in bytes.
    MemoryQuota {
        /// Optional hint-level query block.
        qb_name: Option<String>,
        /// Quota in bytes.
        bytes: i64,
    },
    /// `TIME_RANGE(from, to)`.
    TimeRange {
        /// Inclusive start text.
        from: String,
        /// Inclusive end text.
        to: String,
    },
    /// `NAME([@qb_name] N)` — `MAX_EXECUTION_TIME`/`NTH_PLAN`, a plain
    /// integer argument with an OPTIONAL leading query-block name
    /// (confirmed via `godump restore`: `MAX_EXECUTION_TIME(@sel_1 10)`
    /// restores as `` MAX_EXECUTION_TIME(@`sel_1` 10) ``, the qb name
    /// back-quoted, space-separated, BEFORE the number — unlike
    /// [`HintTable`]'s own `name@qb_name` SUFFIX shape, this is a
    /// PREFIX). Read directly from `pkg/parser/hintparser.go`'s
    /// `parseMaxExecTimeHint`/`parseNthPlanHint`, both of which call the
    /// SAME shared `parseQBName()` immediately after `(`, before the
    /// mandatory integer.
    Number {
        /// The optional leading query-block name.
        qb_name: Option<String>,
        /// The integer argument.
        value: i64,
    },
    /// `QB_NAME(name [, ViewNameList])` — defines a query-block name,
    /// optionally scoped to a dot-separated view path. `ViewNameList` is
    /// `ViewName ('.' ViewName)*`; each entry is either
    /// `name[@sel_N]` or a bare `@sel_N`. This mirrors
    /// `pkg/parser/hintparser.go`'s `parseQBNameHint`, whose planner
    /// consumes the path one view at a time during nested-view
    /// resolution.
    ///
    /// Restore is a real asymmetry from every OTHER hint here: `qb_name`
    /// restores WITHOUT a leading `@`, and the separator before the path
    /// is `` " , " `` (a space, then comma-space). Individual path
    /// entries restore with `` ". " `` between them, exactly as
    /// `TableOptimizerHint.Restore` does in `pkg/parser/ast/misc.go`.
    QbName {
        /// The query-block name being defined.
        qb_name: String,
        /// The optional nested-view path, in written order. A bare
        /// `@sel_N` entry has an empty [`HintTable::name`] and a present
        /// [`HintTable::qb_name`].
        views: Vec<HintTable>,
    },
    /// `NAME([@qb_name] STORE_TYPE[table, ...], STORE_TYPE2[table, ...],
    /// ...)` — `READ_FROM_STORAGE`. `STORE_TYPE` is always `TIKV` or
    /// `TIFLASH`, canonicalized to uppercase on restore regardless of
    /// how it was written (confirmed via `godump restore`:
    /// `read_from_storage(TiKv[t1])` restores as
    /// `` READ_FROM_STORAGE(TIKV[`t1`]) ``, real TiDB's own hand-written
    /// parser DOES preserve the written case internally, per
    /// `pkg/parser/hintparser.go`'s own `parseStorageHint` comment, but
    /// its `Restore` uses `WriteKeyWord`, which always uppercases). A
    /// genuine, real STRUCTURAL asymmetry from every other [`HintKind`]
    /// here: real TiDB's own `parseStorageHint` produces ONE
    /// `ast.TableOptimizerHint` PER storage-type group (all sharing the
    /// SAME `qb_name`, parsed once before the group loop), each
    /// restoring as its OWN, SEPARATE `` READ_FROM_STORAGE(...) ``
    /// occurrence — confirmed via `godump restore`:
    /// `read_from_storage(tikv[t1], tiflash[t2])` restores as
    /// `` READ_FROM_STORAGE(TIKV[`t1`]) READ_FROM_STORAGE(TIFLASH[`t2`])``,
    /// TWO separate hint blocks from the ONE written occurrence — so
    /// [`Hint::restore_into`] special-cases this variant to bypass its
    /// own generic `NAME(...)` wrapper and print `groups.len()` of them
    /// directly, space-separated, rather than trying to force this
    /// one-write/many-print shape through the uniform
    /// one-`Hint`-per-`parse_one_hint`-call model every other variant
    /// here already assumes (hints have no execution semantics in this
    /// crate, so a single `Hint` printing multiple blocks is
    /// byte-identical to the alternative of `parse_hint_comment`
    /// pushing multiple `Hint`s per occurrence — the smaller, more
    /// targeted change).
    ReadFromStorage {
        /// The optional leading query-block name, shared by every group.
        qb_name: Option<String>,
        /// Each `(STORE_TYPE, tables)` group, in written order.
        groups: Vec<(String, Vec<HintTable>)>,
    },
}

/// One table argument inside a [`Hint`] — a NARROWER shape than
/// [`crate::TableRef`] (no alias; a hint table argument is a bare name plus
/// optional schema, query-block, and partition qualifiers).
#[derive(Debug, Clone, PartialEq)]
pub struct HintTable {
    /// An optional `db.` schema qualifier, restoring as `` `db`. ``
    /// before the table name (confirmed via `godump restore`:
    /// `` READ_FROM_STORAGE(TIKV[`s`.`t`]) `` preserves the schema).
    /// Only ever populated by [`HintKind::ReadFromStorage`]'s own
    /// parsing so far — every OTHER hint table list in the real-TiDB
    /// integration-test corpus this project measures coverage against
    /// only ever uses unqualified names, so `None` elsewhere.
    pub db_name: Option<String>,
    /// The table name. It is empty only for a bare `@sel_N` entry in
    /// [`HintKind::QbName`]'s view path; restore preserves that empty
    /// slot as ```` before the query-block suffix.
    pub name: String,
    /// An optional `@query_block_name` suffix, restoring as `` @`name` ``
    /// (confirmed via `godump restore`: the query-block name is
    /// back-quoted too, not bare).
    pub qb_name: Option<String>,
    /// Optional partition list.
    pub partitions: Vec<String>,
}

/// One recursive element of a `LEADING` hint. Go retains nested lists in
/// `ast.LeadingList` while also exposing a flattened `Tables` convenience
/// slice; restore uses this tree, so the Rust AST does too.
#[derive(Debug, Clone, PartialEq)]
pub enum LeadingElement {
    /// A single hinted table.
    Table(HintTable),
    /// A parenthesized nested leading group.
    Group(Vec<LeadingElement>),
}

fn restore_leading_elements(out: &mut String, elements: &[LeadingElement]) {
    for (index, element) in elements.iter().enumerate() {
        if index > 0 {
            out.push_str(", ");
        }
        match element {
            LeadingElement::Table(table) => table.restore_into(out),
            LeadingElement::Group(group) => {
                out.push('(');
                restore_leading_elements(out, group);
                out.push(')');
            }
        }
    }
}

impl Hint {
    pub(crate) fn restore_into(&self, out: &mut String) {
        // `ReadFromStorage` bypasses the generic `NAME(...)` wrapper
        // below entirely — see its own doc for why one written
        // occurrence restores as MULTIPLE separate `NAME(...)` blocks.
        if let HintKind::ReadFromStorage { qb_name, groups } = &self.kind {
            for (i, (store, tables)) in groups.iter().enumerate() {
                if i > 0 {
                    out.push(' ');
                }
                out.push_str(&self.name);
                out.push('(');
                if let Some(qb) = qb_name {
                    out.push('@');
                    out.push_str(&back_quote(qb));
                    out.push(' ');
                }
                out.push_str(store);
                // The brackets themselves are only written when at
                // least one table is present — confirmed via `godump
                // restore`: a bare `TIKV` with no list at all restores
                // as bare `TIKV`, not `TIKV[]` (real TiDB's own
                // `TableOptimizerHint.Restore` only ever writes `[`/`]`
                // from INSIDE its loop over `n.Tables`, never
                // unconditionally).
                if !tables.is_empty() {
                    out.push('[');
                    for (j, t) in tables.iter().enumerate() {
                        if j > 0 {
                            out.push_str(", ");
                        }
                        t.restore_into(out);
                    }
                    out.push(']');
                }
                out.push(')');
            }
            return;
        }
        out.push_str(&self.name);
        out.push('(');
        match &self.kind {
            HintKind::Nullary { qb_name } => {
                if let Some(qb_name) = qb_name {
                    out.push('@');
                    out.push_str(&back_quote(qb_name));
                }
            }
            HintKind::Tables { qb_name, tables } => {
                if let Some(qb) = qb_name {
                    out.push('@');
                    out.push_str(&back_quote(qb));
                    // Unconditional, even when `tables` is empty —
                    // matches real TiDB's own `TableOptimizerHint
                    // .Restore`, which writes this space right after
                    // the qb name whenever it's present, before ever
                    // checking whether any table follows.
                    out.push(' ');
                }
                for (i, t) in tables.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    t.restore_into(out);
                }
            }
            HintKind::Leading { qb_name, elements } => {
                if let Some(qb) = qb_name {
                    out.push('@');
                    out.push_str(&back_quote(qb));
                    out.push(' ');
                }
                restore_leading_elements(out, elements);
            }
            HintKind::Index {
                qb_name,
                table,
                indexes,
            } => {
                if let Some(qb) = qb_name {
                    out.push('@');
                    out.push_str(&back_quote(qb));
                    out.push(' ');
                }
                table.restore_into(out);
                if !indexes.is_empty() {
                    out.push(' ');
                    for (i, idx) in indexes.iter().enumerate() {
                        if i > 0 {
                            out.push_str(", ");
                        }
                        out.push_str(&back_quote(idx));
                    }
                }
            }
            HintKind::SetVar { var_name, value } => {
                out.push_str(var_name);
                out.push_str(" = '");
                out.push_str(&escape_string_literal(value));
                out.push('\'');
            }
            HintKind::Bool { qb_name, value } => {
                if let Some(qb_name) = qb_name {
                    out.push('@');
                    out.push_str(&back_quote(qb_name));
                    out.push(' ');
                }
                out.push_str(if *value { "TRUE" } else { "FALSE" });
            }
            HintKind::Name { qb_name, name } => {
                if let Some(qb_name) = qb_name {
                    out.push('@');
                    out.push_str(&back_quote(qb_name));
                    out.push(' ');
                }
                out.push_str(&back_quote(name));
            }
            HintKind::Keyword { qb_name, value } => {
                if let Some(qb_name) = qb_name {
                    out.push('@');
                    out.push_str(&back_quote(qb_name));
                    out.push(' ');
                }
                out.push_str(value);
            }
            HintKind::MemoryQuota { qb_name, bytes } => {
                if let Some(qb_name) = qb_name {
                    out.push('@');
                    out.push_str(&back_quote(qb_name));
                    out.push(' ');
                }
                out.push_str(&(bytes / 1_048_576).to_string());
                out.push_str(" MB");
            }
            HintKind::TimeRange { from, to } => {
                out.push('\'');
                out.push_str(&escape_string_literal(from));
                out.push_str("', '");
                out.push_str(&escape_string_literal(to));
                out.push('\'');
            }
            HintKind::Number { qb_name, value } => {
                if let Some(qb) = qb_name {
                    out.push('@');
                    out.push_str(&back_quote(qb));
                    out.push(' ');
                }
                out.push_str(&value.to_string());
            }
            HintKind::QbName { qb_name, views } => {
                out.push_str(&back_quote(qb_name));
                if !views.is_empty() {
                    out.push_str(" , ");
                    for (i, view) in views.iter().enumerate() {
                        if i > 0 {
                            out.push_str(". ");
                        }
                        view.restore_into(out);
                    }
                }
            }
            HintKind::ReadFromStorage { .. } => {
                unreachable!("handled by the early return above")
            }
        }
        out.push(')');
    }
}

impl HintTable {
    fn restore_into(&self, out: &mut String) {
        if let Some(db) = &self.db_name {
            out.push_str(&back_quote(db));
            out.push('.');
        }
        out.push_str(&back_quote(&self.name));
        if let Some(qb) = &self.qb_name {
            out.push('@');
            out.push_str(&back_quote(qb));
        }
        if !self.partitions.is_empty() {
            out.push_str(" PARTITION(");
            for (index, partition) in self.partitions.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                out.push_str(&back_quote(partition));
            }
            out.push(')');
        }
    }
}

/// A `FROM`-clause join node, mirroring the Go AST's `Join`. A single table is
/// represented as `Join { left: Table(..), right: None, .. }`; each additional
/// table (comma or explicit `JOIN`) nests the accumulated tree on the left.
#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    /// The left operand.
    pub left: JoinNode,
    /// The right operand; `None` marks the single-table wrapper.
    pub right: Option<JoinNode>,
    /// The join type.
    pub tp: JoinType,
    /// A `STRAIGHT_JOIN`.
    pub straight: bool,
    /// The `ON` condition, if any.
    pub on: Option<Expr>,
    /// The `USING (...)` column names, if any.
    pub using: Vec<String>,
    /// Whether this is a `NATURAL JOIN` (implicitly joining on every
    /// column name common to both sides) — `on`/`using` are always empty
    /// when this is `true` (real MySQL/TiDB rejects combining `NATURAL`
    /// with an explicit `ON`/`USING`, confirmed via `godump restore`).
    /// Only plain/`LEFT`/`RIGHT` may be `NATURAL` — `NATURAL INNER
    /// JOIN`/`NATURAL CROSS JOIN`/`NATURAL STRAIGHT_JOIN` are all
    /// genuine `ParseError`s (confirmed via `godump restore`), even
    /// though a bare `NATURAL JOIN` (no `INNER` prefix) uses the SAME
    /// [`JoinType::Cross`] a plain `INNER`/`CROSS`/bare `JOIN` does.
    pub natural: bool,
}

impl Join {
    /// Mirrors `Join.Restore` in `pkg/parser/ast/dml.go` (read directly, not
    /// guessed — a real, non-obvious rule): the left operand is parenthesized
    /// when it is itself a join, UNLESS `use_comma_join` applies, in which
    /// case a comma-chain of DERIVED tables restores with a plain `, `
    /// separator and no wrapping parens at all — matching how
    /// `Parser::parse_from` itself builds a comma chain (each continuation
    /// wraps the PRIOR accumulated join in a fresh `JoinNode::Join{ right:
    /// None }` single-operand wrapper before attaching the new right operand;
    /// see that function's own doc), confirmed via `godump restore`:
    /// `(SELECT 1 a) x, (SELECT 2 a) y` restores as-is (no parens, no `JOIN`
    /// keyword), but `t1, t2` (plain tables, not derived) restores as
    /// `(t1) JOIN t2` — the comma syntax is preserved ONLY when the
    /// immediately preceding term (the accumulated join's own left operand)
    /// is a `SELECT`-in-parens derived table, not a plain table reference.
    /// The right operand is parenthesized whenever it is a join, regardless.
    pub(crate) fn restore_into(&self, out: &mut String) {
        let left_is_join = self.left.is_join();
        let use_comma_join = matches!(
            &self.left,
            JoinNode::Join(inner)
                if inner.right.is_none() && matches!(inner.left, JoinNode::Derived { .. })
        );
        if left_is_join && !use_comma_join {
            out.push('(');
        }
        self.left.restore_into(out);
        if left_is_join && !use_comma_join {
            out.push(')');
        }
        let Some(right) = &self.right else {
            return; // single-table wrapper
        };
        if self.natural {
            out.push_str(" NATURAL");
        }
        match self.tp {
            JoinType::Left => out.push_str(" LEFT"),
            JoinType::Right => out.push_str(" RIGHT"),
            JoinType::Cross => {}
        }
        if self.straight {
            out.push_str(" STRAIGHT_JOIN ");
        } else if use_comma_join {
            out.push_str(", ");
        } else {
            out.push_str(" JOIN ");
        }
        let right_is_join = right.is_join();
        if right_is_join {
            out.push('(');
        }
        right.restore_into(out);
        if right_is_join {
            out.push(')');
        }
        if let Some(on) = &self.on {
            out.push_str(" ON ");
            on.restore_into(out);
        }
        if !self.using.is_empty() {
            out.push_str(" USING (");
            for (i, name) in self.using.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&back_quote(name));
            }
            out.push(')');
        }
    }
}

/// An operand of a [`Join`].
#[derive(Debug, Clone, PartialEq)]
pub enum JoinNode {
    /// A table reference.
    Table(TableRef),
    /// A derived table `(SELECT ...) AS alias` — `subquery` may itself be a
    /// `UNION`/`EXCEPT`/`INTERSECT`-joined set operation
    /// (`(SELECT ... UNION [ALL] SELECT ...) alias`, confirmed via `godump
    /// restore`), the SAME `QueryStmt::Select`-or-`QueryStmt::SetOpr` shape
    /// [`Cte::query`]'s own doc already established for a CTE's own
    /// definition — parsed by the SAME `parse_select_or_setopr`.
    Derived {
        /// The subquery.
        subquery: crate::NodeBox<QueryStmt>,
        /// The alias — grammatically OPTIONAL for a plain derived table
        /// (confirmed via `godump restore`: `SELECT * FROM (SELECT 1)`
        /// alone, no alias at all, is valid and restores unchanged), but
        /// mandatory when `lateral` is `true` (real TiDB's own
        /// `parseLateralTableSource`, `pkg/parser/join_parser.go`, always
        /// requires one) — the parser enforces that distinction, this
        /// field's own type doesn't.
        alias: Option<String>,
        /// `LATERAL (subquery) ...` — the subquery may reference columns of
        /// tables preceding it in the same `FROM` clause (confirmed via
        /// `godump restore`: `pkg/parser/join_parser.go`'s
        /// `parseLateralTableSource`). Real TiDB's own execution treats
        /// this as a correlated, per-outer-row re-evaluation; this crate's
        /// execution engine is unconditionally `Unsupported` for it (see
        /// `tidb_exec`'s own `build_node`), the SAME "real semantic effect,
        /// no cheap representation" scope cut already applied to
        /// `TABLESAMPLE`/`AS OF TIMESTAMP`.
        lateral: bool,
        /// An optional `(col1, col2, ...)` alias list renaming the
        /// subquery's own output columns positionally — grammatically
        /// valid ONLY when `lateral` is `true` (confirmed via `godump
        /// restore`: the SAME shape on a non-`LATERAL` derived table,
        /// `(SELECT 1) AS dt(c1)`, is a genuine `ParseError` — real TiDB's
        /// own `parseTableSource` only calls the column-list parser from
        /// inside `parseLateralTableSource`). Empty when omitted.
        column_names: Vec<String>,
    },
    /// A nested join.
    Join(Box<Join>),
}

impl JoinNode {
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self {
            JoinNode::Table(t) => t.restore_into(out),
            JoinNode::Derived {
                subquery,
                alias,
                lateral,
                column_names,
            } => {
                if *lateral {
                    out.push_str("LATERAL ");
                }
                out.push('(');
                subquery.restore_into(out);
                out.push(')');
                // No alias at all (`None`) omits the clause entirely, the
                // SAME restore as an explicit-but-empty alias TEXT (``
                // `` ``) — see `SelectField::restore_into`'s own comment
                // for why an empty name string ALSO omits its clause,
                // matching real TiDB's `TableSource.Restore`.
                if let Some(alias) = alias {
                    if !alias.is_empty() {
                        out.push_str(" AS ");
                        out.push_str(&back_quote(alias));
                        if !column_names.is_empty() {
                            out.push('(');
                            for (i, c) in column_names.iter().enumerate() {
                                if i > 0 {
                                    out.push_str(", ");
                                }
                                out.push_str(&back_quote(c));
                            }
                            out.push(')');
                        }
                    }
                }
            }
            JoinNode::Join(j) => j.restore_into(out),
        }
    }

    fn is_join(&self) -> bool {
        matches!(self, JoinNode::Join(_))
    }
}

/// The kind of a join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// `JOIN` / `INNER JOIN` / `CROSS JOIN` / comma join.
    Cross,
    /// `LEFT [OUTER] JOIN`.
    Left,
    /// `RIGHT [OUTER] JOIN`.
    Right,
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
        let _ = with;
        let _ = is_in_braces;
        let _ = terms;
        let _ = order_by;
        let _ = limit;
        let _ = lock;
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

impl crate::Visitable for TableRef {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            name,
            partitions,
            alias,
            as_of,
            hints,
            sample,
        } = self;
        if let Some(value) = as_of.as_mut() {
            if !crate::Visitable::accept(value.as_mut(), visitor) {
                return false;
            }
        }
        for value in hints.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = sample.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = name;
        let _ = partitions;
        let _ = alias;
        let _ = as_of;
        let _ = hints;
        let _ = sample;
        visitor.leave(self)
    }
}

impl crate::Visitable for TableSample {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            method,
            expr,
            unit,
            repeatable,
        } = self;
        if let Some(value) = method.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = expr.as_mut() {
            if !crate::Visitable::accept(value.as_mut(), visitor) {
                return false;
            }
        }
        if let Some(value) = unit.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = repeatable.as_mut() {
            if !crate::Visitable::accept(value.as_mut(), visitor) {
                return false;
            }
        }
        let _ = method;
        let _ = expr;
        let _ = unit;
        let _ = repeatable;
        visitor.leave(self)
    }
}

impl crate::Visitable for SampleMethod {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::System => {}
            Self::Bernoulli => {}
            Self::Region => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for SampleUnit {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Percent => {}
            Self::Rows => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for IndexHint {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            kind,
            scope,
            indexes,
        } = self;
        if !crate::Visitable::accept(kind, visitor) {
            return false;
        }
        if !crate::Visitable::accept(scope, visitor) {
            return false;
        }
        let _ = kind;
        let _ = scope;
        let _ = indexes;
        visitor.leave(self)
    }
}

impl crate::Visitable for IndexHintKind {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Use => {}
            Self::Force => {}
            Self::Ignore => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for IndexHintScope {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::All => {}
            Self::Join => {}
            Self::OrderBy => {}
            Self::GroupBy => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for Hint {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { name, kind } = self;
        if !crate::Visitable::accept(kind, visitor) {
            return false;
        }
        let _ = name;
        let _ = kind;
        visitor.leave(self)
    }
}

impl crate::Visitable for HintKind {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Nullary { qb_name } => {
                let _ = qb_name;
            }
            Self::Tables { qb_name, tables } => {
                for value in tables.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = qb_name;
                let _ = tables;
            }
            Self::Leading { qb_name, elements } => {
                for value in elements.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = qb_name;
                let _ = elements;
            }
            Self::Index {
                qb_name,
                table,
                indexes,
            } => {
                if !crate::Visitable::accept(table, visitor) {
                    return false;
                }
                let _ = qb_name;
                let _ = table;
                let _ = indexes;
            }
            Self::SetVar { var_name, value } => {
                let _ = var_name;
                let _ = value;
            }
            Self::Bool { qb_name, value } => {
                let _ = qb_name;
                let _ = value;
            }
            Self::Name { qb_name, name } => {
                let _ = qb_name;
                let _ = name;
            }
            Self::Keyword { qb_name, value } => {
                let _ = qb_name;
                let _ = value;
            }
            Self::MemoryQuota { qb_name, bytes } => {
                let _ = qb_name;
                let _ = bytes;
            }
            Self::TimeRange { from, to } => {
                let _ = from;
                let _ = to;
            }
            Self::Number { qb_name, value } => {
                let _ = qb_name;
                let _ = value;
            }
            Self::QbName { qb_name, views } => {
                for value in views.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = qb_name;
                let _ = views;
            }
            Self::ReadFromStorage { qb_name, groups } => {
                for value in groups.iter_mut() {
                    for value in &mut value.1.iter_mut() {
                        if !crate::Visitable::accept(value, visitor) {
                            return false;
                        }
                    }
                }
                let _ = qb_name;
                let _ = groups;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for HintTable {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            db_name,
            name,
            qb_name,
            partitions,
        } = self;
        let _ = db_name;
        let _ = name;
        let _ = qb_name;
        let _ = partitions;
        visitor.leave(self)
    }
}

impl crate::Visitable for LeadingElement {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Table(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Group(field_0) => {
                for value in field_0.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for Join {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            left,
            right,
            tp,
            straight,
            on,
            using,
            natural,
        } = self;
        if !crate::Visitable::accept(left, visitor) {
            return false;
        }
        if let Some(value) = right.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if !crate::Visitable::accept(tp, visitor) {
            return false;
        }
        if let Some(value) = on.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = left;
        let _ = right;
        let _ = tp;
        let _ = straight;
        let _ = on;
        let _ = using;
        let _ = natural;
        visitor.leave(self)
    }
}

impl crate::Visitable for JoinNode {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Table(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Derived {
                subquery,
                alias,
                lateral,
                column_names,
            } => {
                if !crate::Visitable::accept(subquery.as_mut(), visitor) {
                    return false;
                }
                let _ = subquery;
                let _ = alias;
                let _ = lateral;
                let _ = column_names;
            }
            Self::Join(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for JoinType {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Cross => {}
            Self::Left => {}
            Self::Right => {}
        }
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
