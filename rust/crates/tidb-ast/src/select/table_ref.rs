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

//! The `FROM` clause: table references, `TABLESAMPLE`, and the join tree,
//! mirroring Go's `TableSource`/`TableName`/`Join` in `pkg/parser/ast/dml.go`.

use super::*;

/// A table reference in `FROM`.
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
    pub(crate) fn restore_into_with_context(&self, out: &mut String, context: &RestoreContext) {
        if self.name.len() == 1 {
            if let Some(database) = context.default_db_for_table(&self.name[0], false) {
                out.push_str(&back_quote(database));
                out.push('.');
            }
        }
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
    /// Whether this join subtree was explicitly parenthesized in the source.
    /// Go uses this bit to prevent cross-join rotation across a name-scope
    /// boundary; restoration derives the visible parentheses from the tree.
    pub explicit_parens: bool,
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
    pub(crate) fn restore_into_with_context(&self, out: &mut String, context: &RestoreContext) {
        let left_is_join = self.left.is_join();
        let use_comma_join = matches!(
            &self.left,
            JoinNode::Join(inner)
                if inner.right.is_none() && matches!(inner.left, JoinNode::Derived { .. })
        );
        if left_is_join && !use_comma_join {
            out.push('(');
        }
        self.left.restore_into_with_context(out, context);
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
        right.restore_into_with_context(out, context);
        if right_is_join {
            out.push(')');
        }
        if let Some(on) = &self.on {
            out.push_str(" ON ");
            on.restore_into_with_context(out, context);
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
        /// this as a correlated, per-outer-row re-evaluation -- Go's
        /// `buildLateralJoin` builds a `LogicalApply` with `InnerJoin` --
        /// which `tidb_executor`'s own `build_lateral_join` now reproduces.
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
    pub(crate) fn restore_into_with_context(&self, out: &mut String, context: &RestoreContext) {
        match self {
            JoinNode::Table(t) => t.restore_into_with_context(out, context),
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
                subquery.restore_into_with_context(out, context);
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
            JoinNode::Join(j) => j.restore_into_with_context(out, context),
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
            explicit_parens,
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
        let _ = explicit_parens;
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
