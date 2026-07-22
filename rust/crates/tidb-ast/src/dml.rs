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

//! `INSERT`/`UPDATE`/`DELETE` statements and their restore.

use crate::select::restore_partition_clause;
use crate::util::{back_quote, escape_string_literal, push_name_path, redact_url};
use crate::{
    ColumnOrUserVar, Expr, Hint, Join, Limit, LoadDataOption, OrderItem, QueryStmt, SelectField,
    TableRef,
};

/// MySQL statement priority shared by SELECT, INSERT/REPLACE, UPDATE, and
/// DELETE. Go stores this as `mysql.PriorityEnum`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StatementPriority {
    /// No explicit priority.
    #[default]
    None,
    /// `LOW_PRIORITY`.
    Low,
    /// `HIGH_PRIORITY`.
    High,
    /// `DELAYED`.
    Delayed,
}

/// TiDB physical table distribution request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributeTableStmt {
    /// Target table path.
    pub table: Vec<String>,
    /// Optional partition list.
    pub partitions: Vec<String>,
    /// Optional distribution rule.
    pub rule: Option<String>,
    /// Optional target engine.
    pub engine: Option<String>,
    /// Optional operation timeout.
    pub timeout: Option<String>,
}

impl DistributeTableStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("DISTRIBUTE TABLE ");
        push_name_path(out, &self.table);
        restore_partition_clause(out, &self.partitions);
        for (name, value) in [
            ("RULE", &self.rule),
            ("ENGINE", &self.engine),
            ("TIMEOUT", &self.timeout),
        ] {
            if let Some(value) = value {
                out.push(' ');
                out.push_str(name);
                out.push_str(" = '");
                out.push_str(&escape_string_literal(value));
                out.push('\'');
            }
        }
    }
}

impl StatementPriority {
    pub(crate) fn restore_into(self, out: &mut String) {
        out.push_str(match self {
            Self::None => "",
            Self::Low => "LOW_PRIORITY ",
            Self::High => "HIGH_PRIORITY ",
            Self::Delayed => "DELAYED ",
        });
    }
}

/// An `INSERT`, `REPLACE`, or `INSERT ... SELECT` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt {
    /// Optimizer hints immediately after `INSERT` or `REPLACE`.
    pub hints: Vec<Hint>,
    /// Optional MySQL priority modifier.
    pub priority: StatementPriority,
    /// Whether `IGNORE` was specified: a row that conflicts with an existing
    /// `PRIMARY KEY` value is silently skipped (the existing row is kept
    /// unchanged) rather than raising a duplicate-key error.
    pub ignore: bool,
    /// The target table name path.
    pub table: Vec<String>,
    /// An optional `PARTITION (name, ...)` clause restricting which
    /// partitions the insert targets — empty if not written. See
    /// [`TableRef::partitions`]'s own doc for the shared grammar and
    /// this crate's own execution-time scope boundary (this project
    /// never implements table partitioning at all, so ANY non-empty
    /// list is always `Unsupported` at execution).
    pub partitions: Vec<String>,
    /// The optional explicit column list.
    pub columns: Vec<String>,
    /// Whether a column list was written, including the valid empty `()` form.
    pub columns_specified: bool,
    /// Typed assignment targets for `INSERT ... SET`. Empty for every other
    /// insert form. Unlike [`InsertStmt::columns`], these preserve a written
    /// table/schema qualifier (`t.c` or `db.t.c`) exactly as Go's
    /// `InsertStmt.Columns []*ColumnName` does for the `SET` production.
    pub set_columns: Vec<Vec<String>>,
    /// The `VALUES` rows. Empty when this is an `INSERT ... SELECT`
    /// instead (see [`InsertStmt::source`]) — the two forms are mutually
    /// exclusive, matching real TiDB's own `Lists` vs `Select`.
    pub rows: Vec<Vec<Expr>>,
    /// An `INSERT ... SELECT` / `REPLACE ... SELECT` query source, if the
    /// statement inserts the rows produced by a query rather than a
    /// literal `VALUES` list. A [`QueryStmt`] restored directly
    /// after the table/column list with no `VALUES` keyword. When
    /// present, `rows` is empty.
    pub source: Option<Box<QueryStmt>>,
    /// Whether [`Self::source`] was enclosed in the source-position
    /// parentheses accepted by TiDB's `InsertStmt` grammar: `INSERT INTO t
    /// (SELECT ...)` or `INSERT INTO t (a) (SELECT ...)`. This is separate
    /// from a parenthesized subquery *inside* the query source: the outer
    /// pair belongs to the INSERT production and Go's `InsertStmt.Restore`
    /// preserves it.
    pub source_parenthesized: bool,
    /// The `ON DUPLICATE KEY UPDATE` assignments, if present. An assignment's
    /// value may reference `VALUES(col)` (restored as `Expr::Func{name:
    /// "VALUES", ..}`), the row that would have been inserted.
    pub on_duplicate: Vec<Assignment>,
    /// Optional MySQL 8.0.19 row alias following VALUES or SET.
    pub row_alias: Option<String>,
    /// Optional positional column aliases attached to `row_alias`.
    pub column_aliases: Vec<String>,
    /// Optional TiDB `RETURNING` projection.
    pub returning: Vec<SelectField>,
    /// Whether the values were written in `SET col=val, ...` assignment
    /// form rather than `[cols] VALUES (...)`. The two are equivalent —
    /// the parser keeps its typed LHS paths in [`InsertStmt::set_columns`]
    /// and its RHS values in one [`InsertStmt::rows`] entry — but real TiDB
    /// restores the `SET` form verbatim (`SET a=1,b=DEFAULT`, confirmed via
    /// `godump restore`), so this flag selects that restore path. A
    /// `SET`-form insert always has exactly one row.
    pub set_syntax: bool,
    /// Whether this is a `REPLACE` rather than an `INSERT` — real TiDB's
    /// own `InsertStmt.IsReplace`, since the two share the entire
    /// `[cols] VALUES rows` grammar. A `REPLACE` restores as `REPLACE INTO`
    /// (confirmed via `godump restore`) and, on a `PRIMARY KEY`/`UNIQUE`
    /// conflict, DELETES the conflicting row(s) and inserts the new one
    /// (rather than erroring/skipping/updating). It never carries
    /// `IGNORE` or `ON DUPLICATE KEY UPDATE`.
    pub replace: bool,
}

impl InsertStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        if self.replace {
            out.push_str("REPLACE ");
        } else {
            out.push_str("INSERT ");
        }
        restore_dml_hints(out, &self.hints);
        self.priority.restore_into(out);
        if self.ignore {
            out.push_str("IGNORE ");
        }
        out.push_str("INTO ");
        push_name_path(out, &self.table);
        restore_partition_clause(out, &self.partitions);
        if self.set_syntax {
            // `SET col=val, ...` — zip the typed assignment paths with the
            // single row's values. No leading column-list parens.
            out.push_str(" SET ");
            let row = self.rows.first().expect("SET-form insert has one row");
            for (i, (c, v)) in self.set_columns.iter().zip(row).enumerate() {
                if i > 0 {
                    out.push(',');
                }
                push_name_path(out, c);
                out.push('=');
                v.restore_into(out);
            }
            restore_insert_tail(self, out);
            return;
        }
        if self.columns_specified {
            out.push_str(" (");
            for (i, c) in self.columns.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&back_quote(c));
            }
            out.push(')');
        }
        if let Some(source) = &self.source {
            // `INSERT ... SELECT`: the query source restores directly
            // after the table/column list, with a single separating
            // space and no `VALUES` keyword.
            out.push(' ');
            if self.source_parenthesized {
                out.push('(');
            }
            source.restore_into(out);
            if self.source_parenthesized {
                out.push(')');
            }
        } else {
            out.push_str(" VALUES ");
            for (i, row) in self.rows.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push('(');
                for (j, e) in row.iter().enumerate() {
                    if j > 0 {
                        out.push(',');
                    }
                    e.restore_into(out);
                }
                out.push(')');
            }
        }
        restore_insert_tail(self, out);
    }
}

/// The source of an `IMPORT INTO` statement.
#[derive(Debug, Clone, PartialEq)]
pub enum ImportSource {
    /// A literal external path and its optional declared file format.
    File {
        /// Decoded source path.
        path: String,
        /// Optional decoded `FORMAT` value.
        format: Option<String>,
    },
    /// A query source. `parenthesized` corresponds to Go's `IsInBraces`
    /// restore-visible query flag when the source was written `FROM (SELECT
    /// ...)`.
    Select {
        /// Source query.
        query: Box<QueryStmt>,
        /// Whether the source query was enclosed in parentheses.
        parenthesized: bool,
    },
}

/// TiDB's `IMPORT INTO` parser/restore envelope.
///
/// This type deliberately models grammar and canonical SQL only. Importing
/// files, source-query execution, and every import option's operational
/// meaning require TiDB's distributed Lightning/import job protocol, so the
/// seed executor rejects this statement before opening an implicit
/// transaction.
#[derive(Debug, Clone, PartialEq)]
pub struct ImportIntoStmt {
    /// Destination table name path.
    pub table: Vec<String>,
    /// Optional source-column / user-variable mappings.
    pub columns_and_user_vars: Vec<ColumnOrUserVar>,
    /// Optional `SET column=expression` mappings for file imports.
    pub column_assignments: Vec<Assignment>,
    /// Exactly one file or query source.
    pub source: ImportSource,
    /// Optional parser-level `WITH` options in written order.
    pub options: Vec<LoadDataOption>,
}

impl ImportIntoStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("IMPORT INTO ");
        push_name_path(out, &self.table);
        if !self.columns_and_user_vars.is_empty() {
            out.push_str(" (");
            for (index, column) in self.columns_and_user_vars.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                column.restore_into(out);
            }
            out.push(')');
        }
        if !self.column_assignments.is_empty() {
            out.push_str(" SET ");
            for (index, assignment) in self.column_assignments.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                assignment.restore_into(out);
            }
        }
        out.push_str(" FROM ");
        match &self.source {
            ImportSource::File { path, format } => {
                // `ImportIntoStmt.Path` is restored with Go's raw
                // `WriteString` path, unlike a normal expression string
                // literal (which gains `_UTF8MB4`).
                out.push('\'');
                out.push_str(&escape_string_literal(path));
                out.push('\'');
                if let Some(format) = format {
                    out.push_str(" FORMAT ");
                    out.push('\'');
                    out.push_str(&escape_string_literal(format));
                    out.push('\'');
                }
            }
            ImportSource::Select {
                query,
                parenthesized,
            } => {
                if *parenthesized {
                    out.push('(');
                }
                query.restore_into(out);
                if *parenthesized {
                    out.push(')');
                }
            }
        }
        if !self.options.is_empty() {
            out.push_str(" WITH ");
            for (index, option) in self.options.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                option.restore_into(out);
            }
        }
    }

    /// Restores the statement after redacting external-storage credentials.
    pub fn secure_text(&self) -> String {
        let mut redacted = self.clone();
        if let ImportSource::File { path, .. } = &mut redacted.source {
            *path = redact_url(path);
        }
        for option in &mut redacted.options {
            if option.name.eq_ignore_ascii_case("cloud_storage_uri") {
                if let Some(value) = &mut option.value {
                    let redacted_value = match value {
                        Expr::String(text) | Expr::RawString(text) => Some(redact_url(text)),
                        Expr::CharsetString { value, .. } => Some(redact_url(value)),
                        _ => None,
                    };
                    if let Some(redacted_value) = redacted_value {
                        // Go rebuilds this option with `NewValueExpr(value,
                        // "", "")`, so it restores as a plain quoted string.
                        *value = Expr::RawString(redacted_value);
                    }
                }
            }
        }
        let mut out = String::new();
        redacted.restore_into(&mut out);
        out
    }
}

/// An `UPDATE ... SET` statement — single-table or multi-table (see
/// [`UpdateKind`]).
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStmt {
    /// Optimizer hints immediately after `UPDATE`.
    pub hints: Vec<Hint>,
    /// Optional MySQL priority modifier.
    pub priority: StatementPriority,
    /// Whether `UPDATE IGNORE` was written — a row whose update would raise
    /// an error (e.g. a duplicate-key or data-conversion error) is skipped
    /// (with a warning in real MySQL) rather than aborting the statement.
    /// Restored as `UPDATE IGNORE`.
    pub ignore: bool,
    /// The single target table or the multi-table join.
    pub kind: UpdateKind,
    /// The `SET` assignments (each column optionally table-qualified for the
    /// multi-table form, e.g. `t1.a = 40`).
    pub assignments: Vec<Assignment>,
    /// The `WHERE` predicate, if any.
    pub where_clause: Option<Expr>,
    /// The optional single-table `ORDER BY` tail.
    pub order_by: Vec<OrderItem>,
    /// The optional single-table `LIMIT` tail.
    pub limit: Option<Limit>,
    /// Optional TiDB `RETURNING` projection.
    pub returning: Vec<SelectField>,
}

/// The shape of an `UPDATE`: an ordinary single-table update, or a
/// multi-table update whose `SET` assignments write to columns of several
/// joined tables.
#[derive(Debug, Clone, PartialEq)]
pub enum UpdateKind {
    /// `UPDATE [IGNORE] tbl SET ...` — the one target table (which may carry
    /// an alias/hints/partition).
    Single(TableRef),
    /// `UPDATE [IGNORE] join SET ...` — the joined row source (the same
    /// grammar as a `SELECT`'s `FROM`, written directly after `UPDATE` with
    /// no `FROM` keyword). Boxed to keep the two variants similarly sized.
    Multi {
        /// The joined row source.
        from: Box<Join>,
        /// Whether the outer table-reference grammar contained a comma.
        /// TiDB rejects `ORDER BY` and `LIMIT` only for this form, while an
        /// explicit `JOIN` remains eligible for those tails.
        comma_join: bool,
    },
}

impl UpdateStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("UPDATE ");
        restore_dml_hints(out, &self.hints);
        self.priority.restore_into(out);
        if self.ignore {
            out.push_str("IGNORE ");
        }
        match &self.kind {
            UpdateKind::Single(table) => table.restore_into(out),
            UpdateKind::Multi { from, .. } => from.restore_into(out),
        }
        out.push_str(" SET ");
        for (i, a) in self.assignments.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            a.restore_into(out);
        }
        if let Some(w) = &self.where_clause {
            out.push_str(" WHERE ");
            w.restore_into(out);
        }
        restore_dml_order_limit(out, &self.order_by, &self.limit);
        restore_returning(out, &self.returning);
    }
}

/// A `DELETE` statement — single-table or multi-table (see [`DeleteKind`]).
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStmt {
    /// Optimizer hints immediately after `DELETE`.
    pub hints: Vec<Hint>,
    /// Optional MySQL priority modifier.
    pub priority: StatementPriority,
    /// Whether `QUICK` was written.
    pub quick: bool,
    /// Whether `DELETE IGNORE` was written — an error that would abort a
    /// row's deletion (e.g. a foreign-key restriction) is turned into a
    /// skipped row plus a warning. Restored as `DELETE IGNORE`.
    pub ignore: bool,
    /// The single-table target or the multi-table target list + join.
    pub kind: DeleteKind,
    /// The `WHERE` predicate, if any.
    pub where_clause: Option<Expr>,
    /// The optional single-table `ORDER BY` tail.
    pub order_by: Vec<OrderItem>,
    /// The optional single-table `LIMIT` tail.
    pub limit: Option<Limit>,
    /// Optional TiDB `RETURNING` projection.
    pub returning: Vec<SelectField>,
}

/// The shape of a `DELETE`: an ordinary single-table delete, or a
/// multi-table delete that removes rows from several tables named in a
/// target list, joined by a `FROM`/`USING` clause.
#[derive(Debug, Clone, PartialEq)]
pub enum DeleteKind {
    /// `DELETE [IGNORE] FROM tbl [WHERE]` — the one target table (which may
    /// carry an alias/hints/partition, unlike a multi-table target).
    Single(TableRef),
    /// A multi-table delete. `targets` are the tables whose matching rows
    /// are removed; `from` is the joined row source. The two spellings —
    /// `DELETE targets FROM join` (`using = false`) and `DELETE FROM
    /// targets USING join` (`using = true`) — are equivalent and each
    /// restores in its own written form (confirmed via `godump restore`).
    Multi {
        /// The tables to delete matching rows from, in written order.
        targets: Vec<Vec<String>>,
        /// Whether the `USING` spelling was written (targets before `FROM`
        /// vs. `FROM` targets before `USING`).
        using: bool,
        /// The joined row source (the same grammar as a `SELECT`'s `FROM`).
        /// Boxed to keep [`DeleteKind`]'s two variants similarly sized (a
        /// `Join` is large; the common single-table delete pays nothing).
        from: Box<Join>,
    },
}

impl DeleteStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("DELETE ");
        restore_dml_hints(out, &self.hints);
        self.priority.restore_into(out);
        if self.quick {
            out.push_str("QUICK ");
        }
        if self.ignore {
            out.push_str("IGNORE ");
        }
        match &self.kind {
            DeleteKind::Single(table) => {
                out.push_str("FROM ");
                table.restore_into(out);
            }
            DeleteKind::Multi {
                targets,
                using,
                from,
            } => {
                if *using {
                    out.push_str("FROM ");
                    restore_target_list(out, targets);
                    out.push_str(" USING ");
                } else {
                    restore_target_list(out, targets);
                    out.push_str(" FROM ");
                }
                from.restore_into(out);
            }
        }
        if let Some(w) = &self.where_clause {
            out.push_str(" WHERE ");
            w.restore_into(out);
        }
        restore_dml_order_limit(out, &self.order_by, &self.limit);
        restore_returning(out, &self.returning);
    }
}

/// Appends Go's canonical optimizer-hint block shared by INSERT, UPDATE, and
/// DELETE. Their hand parsers all call `parseOptHints` directly after the
/// statement verb.
fn restore_dml_hints(out: &mut String, hints: &[Hint]) {
    if hints.is_empty() {
        return;
    }
    out.push_str("/*+ ");
    for (index, hint) in hints.iter().enumerate() {
        if index > 0 {
            out.push(' ');
        }
        hint.restore_into(out);
    }
    out.push_str("*/ ");
}

/// Restores the fixed `ORDER BY` then `LIMIT` tail used by UPDATE and DELETE.
/// Unlike SELECT's tail loop, TiDB's DML parser accepts each clause at most
/// once and in this grammar order.
fn restore_dml_order_limit(out: &mut String, order_by: &[OrderItem], limit: &Option<Limit>) {
    if !order_by.is_empty() {
        out.push_str(" ORDER BY ");
        for (index, item) in order_by.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            item.restore_into(out);
        }
    }
    if let Some(limit) = limit {
        out.push_str(" LIMIT ");
        if let Some(offset) = &limit.offset {
            offset.restore_into(out);
            out.push(',');
        }
        limit.count.restore_into(out);
    }
}

/// Restores a multi-table `DELETE` target list: comma-joined (no space),
/// each a back-quoted name path — `` `t1`,`t2` ``.
fn restore_target_list(out: &mut String, targets: &[Vec<String>]) {
    for (i, t) in targets.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        push_name_path(out, t);
    }
}

fn restore_insert_tail(statement: &InsertStmt, out: &mut String) {
    if let Some(alias) = &statement.row_alias {
        out.push_str(" AS ");
        out.push_str(&back_quote(alias));
        if !statement.column_aliases.is_empty() {
            out.push('(');
            for (index, column) in statement.column_aliases.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                out.push_str(&back_quote(column));
            }
            out.push(')');
        }
    }
    if !statement.on_duplicate.is_empty() {
        out.push_str(" ON DUPLICATE KEY UPDATE ");
        for (index, assignment) in statement.on_duplicate.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            assignment.restore_into(out);
        }
    }
    restore_returning(out, &statement.returning);
}

fn restore_returning(out: &mut String, fields: &[SelectField]) {
    if fields.is_empty() {
        return;
    }
    out.push_str(" RETURNING ");
    for (index, field) in fields.iter().enumerate() {
        if index > 0 {
            out.push_str(", ");
        }
        field.restore_into(out);
    }
}

/// A single `col = value` assignment in `UPDATE ... SET`.
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    /// The assigned column's name path.
    pub col: Vec<String>,
    /// The assigned value.
    pub value: Expr,
}

impl Assignment {
    pub(crate) fn restore_into(&self, out: &mut String) {
        push_name_path(out, &self.col);
        out.push('=');
        self.value.restore_into(out);
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for StatementPriority {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::None => {}
            Self::Low => {}
            Self::High => {}
            Self::Delayed => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for DistributeTableStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            table,
            partitions,
            rule,
            engine,
            timeout,
        } = self;
        let _ = table;
        let _ = partitions;
        let _ = rule;
        let _ = engine;
        let _ = timeout;
        visitor.leave(self)
    }
}

impl crate::Visitable for InsertStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            hints,
            priority,
            ignore,
            table,
            partitions,
            columns,
            columns_specified,
            set_columns,
            rows,
            source,
            source_parenthesized,
            on_duplicate,
            row_alias,
            column_aliases,
            returning,
            set_syntax,
            replace,
        } = self;
        for value in hints.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if !crate::Visitable::accept(priority, visitor) {
            return false;
        }
        for value in rows.iter_mut() {
            for value in value.iter_mut() {
                if !crate::Visitable::accept(value, visitor) {
                    return false;
                }
            }
        }
        if let Some(value) = source.as_mut() {
            if !crate::Visitable::accept(value.as_mut(), visitor) {
                return false;
            }
        }
        for value in on_duplicate.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        for value in returning.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = hints;
        let _ = priority;
        let _ = ignore;
        let _ = table;
        let _ = partitions;
        let _ = columns;
        let _ = columns_specified;
        let _ = set_columns;
        let _ = rows;
        let _ = source;
        let _ = source_parenthesized;
        let _ = on_duplicate;
        let _ = row_alias;
        let _ = column_aliases;
        let _ = returning;
        let _ = set_syntax;
        let _ = replace;
        visitor.leave(self)
    }
}

impl crate::Visitable for ImportSource {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::File { path, format } => {
                let _ = path;
                let _ = format;
            }
            Self::Select {
                query,
                parenthesized,
            } => {
                if !crate::Visitable::accept(query.as_mut(), visitor) {
                    return false;
                }
                let _ = query;
                let _ = parenthesized;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ImportIntoStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            table,
            columns_and_user_vars,
            column_assignments,
            source,
            options,
        } = self;
        for value in columns_and_user_vars.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        for value in column_assignments.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if !crate::Visitable::accept(source, visitor) {
            return false;
        }
        for value in options.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = table;
        let _ = columns_and_user_vars;
        let _ = column_assignments;
        let _ = source;
        let _ = options;
        visitor.leave(self)
    }
}

impl crate::Visitable for UpdateStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            hints,
            priority,
            ignore,
            kind,
            assignments,
            where_clause,
            order_by,
            limit,
            returning,
        } = self;
        for value in hints.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if !crate::Visitable::accept(priority, visitor) {
            return false;
        }
        if !crate::Visitable::accept(kind, visitor) {
            return false;
        }
        for value in assignments.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = where_clause.as_mut() {
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
        for value in returning.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = hints;
        let _ = priority;
        let _ = ignore;
        let _ = kind;
        let _ = assignments;
        let _ = where_clause;
        let _ = order_by;
        let _ = limit;
        let _ = returning;
        visitor.leave(self)
    }
}

impl crate::Visitable for UpdateKind {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Single(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Multi { from, comma_join } => {
                if !crate::Visitable::accept(from.as_mut(), visitor) {
                    return false;
                }
                let _ = from;
                let _ = comma_join;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for DeleteStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            hints,
            priority,
            quick,
            ignore,
            kind,
            where_clause,
            order_by,
            limit,
            returning,
        } = self;
        for value in hints.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if !crate::Visitable::accept(priority, visitor) {
            return false;
        }
        if !crate::Visitable::accept(kind, visitor) {
            return false;
        }
        if let Some(value) = where_clause.as_mut() {
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
        for value in returning.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = hints;
        let _ = priority;
        let _ = quick;
        let _ = ignore;
        let _ = kind;
        let _ = where_clause;
        let _ = order_by;
        let _ = limit;
        let _ = returning;
        visitor.leave(self)
    }
}

impl crate::Visitable for DeleteKind {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Single(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Multi {
                targets,
                using,
                from,
            } => {
                if !crate::Visitable::accept(from.as_mut(), visitor) {
                    return false;
                }
                let _ = targets;
                let _ = using;
                let _ = from;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for Assignment {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { col, value } = self;
        if !crate::Visitable::accept(value, visitor) {
            return false;
        }
        let _ = col;
        let _ = value;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
