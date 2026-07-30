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

//! Index hints and optimizer hints, mirroring Go's `IndexHint` in
//! `pkg/parser/ast/dml.go` and the `TableOptimizerHint` grammar in
//! `pkg/parser/hintparser.y`.

use super::*;

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

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

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
// END GENERATED AST VISITOR IMPLEMENTATIONS
