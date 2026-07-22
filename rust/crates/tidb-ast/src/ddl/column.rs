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

//! Shared `CREATE`/`ALTER TABLE` column-definition AST and restore boundary.

use crate::util::{back_quote, escape_string_literal};
use crate::{Expr, PrimaryKeyStorage, RestoreContext, RestoreFlags};

use super::{CheckConstraintDefinition, ColumnType, ForeignKeyReference};

const FEATURE_AUTO_RANDOM: &str = "auto_rand";

/// One column definition: `name type [options...]`.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    /// Optional schema/table qualifier accepted on a column definition.
    /// Go keeps these components on `ast.ColumnName` and restores each one as
    /// a separately quoted path segment.
    pub qualifier: Vec<String>,
    /// The column name.
    pub name: String,
    /// The declared type.
    pub ty: ColumnType,
    /// Column options, in the order they were written.
    pub options: Vec<ColumnOption>,
}

impl ColumnDef {
    /// `` `name` TYPE[(args)] [OPTION ...]`` — each option is joined by a
    /// single leading space, in the order they were parsed (matching the Go
    /// AST, which has no canonical option order of its own).
    fn restore_into(&self, out: &mut String) {
        self.restore_into_with_name_path(out, &self.qualifier, RestoreContext::default());
    }

    pub(super) fn restore_into_with_context(&self, out: &mut String, context: RestoreContext) {
        if context.flags() == RestoreFlags::DEFAULT {
            self.restore_into(out);
            return;
        }
        self.restore_into_with_name_path(out, &self.qualifier, context);
    }

    /// Restores a column definition whose source name had a qualifier path,
    /// as accepted by Go's qualified CREATE/ALTER column productions.
    pub(crate) fn restore_into_with_name_path(
        &self,
        out: &mut String,
        qualifier: &[String],
        context: RestoreContext,
    ) {
        for (index, part) in qualifier
            .iter()
            .chain(std::iter::once(&self.name))
            .enumerate()
        {
            if index > 0 {
                out.push('.');
            }
            out.push_str(&back_quote(part));
        }
        out.push(' ');
        self.ty.restore_into(out);
        for opt in &self.options {
            out.push(' ');
            opt.restore_into_with_context(out, context);
        }
    }

    /// Restores this column into a byte-preserving sink. The normal option
    /// payloads are UTF-8 strings today; the declared type owns the only
    /// parser path that can contain arbitrary member bytes.
    pub(crate) fn restore_into_bytes(&self, out: &mut Vec<u8>, context: RestoreContext) {
        for (index, part) in self
            .qualifier
            .iter()
            .chain(std::iter::once(&self.name))
            .enumerate()
        {
            if index > 0 {
                out.push(b'.');
            }
            out.extend_from_slice(back_quote(part).as_bytes());
        }
        out.push(b' ');
        self.ty.restore_into_bytes(out);
        for opt in &self.options {
            out.push(b' ');
            let mut text = String::new();
            opt.restore_into_with_context(&mut text, context);
            out.extend_from_slice(text.as_bytes());
        }
    }
}

/// A column option (`ColumnDef`'s modifiers).
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnOption {
    /// An inline `PRIMARY KEY`/`KEY` or `UNIQUE [KEY]` option.  The Go AST
    /// uses one `ColumnOption` shape for each of these with two independent
    /// fields: primary-key storage and the `GLOBAL` marker.  Keep those
    /// orthogonal facts together rather than making storage/locality create
    /// an expanding family of enum variants.
    InlineKey(InlineKeyOption),
    /// `NOT NULL`.
    NotNull,
    /// `NULL`.
    Null,
    /// `AUTO_INCREMENT`.
    AutoIncrement,
    /// `DEFAULT <expr>`.
    Default(Expr),
    /// `GENERATED ALWAYS AS(<expr>) [VIRTUAL|STORED]`.
    ///
    /// Go stores this as one `ColumnOptionGenerated` payload: `stored` is
    /// false both when `VIRTUAL` was explicit and when it was omitted.  Keep
    /// that single typed representation rather than treating `AS` as a
    /// parser-only decoration, because the same `ColumnDef` is shared by
    /// `CREATE TABLE` and all column-bearing `ALTER TABLE` actions.
    Generated {
        /// The generated expression.
        expression: Expr,
        /// Exact trimmed source text of the generated expression.
        expression_text: Vec<u8>,
        /// Whether the source selected physical `STORED` materialization;
        /// false restores as Go's canonical `VIRTUAL` default.
        stored: bool,
    },
    /// `ON UPDATE CURRENT_TIMESTAMP[(fsp)]` or `ON UPDATE CURRENT_DATE`.
    ///
    /// The parser normalizes Go's supported aliases (`NOW`, `LOCALTIME`,
    /// `LOCALTIMESTAMP`, and `CURDATE`) to the canonical function names
    /// before they reach this source-shaped AST payload.
    OnUpdate(Expr),
    /// `COMMENT '...'` — the comment text, unlike `DEFAULT`'s string
    /// literals, restores as a plain quoted string with no `_UTF8MB4`
    /// charset-introducer prefix.
    Comment(String),
    /// `COLLATE name` — a standalone column-level option (positionally free,
    /// unlike `ColumnType::charset`'s `CHARACTER SET`, which may only
    /// immediately follow the type). Canonically lowercased, matching the
    /// Go AST — the opposite case convention from `charset`'s uppercase.
    Collate(String),
    /// `[CONSTRAINT [name]] CHECK (<expr>) [[NOT] ENFORCED]` declared in a
    /// column definition.  The payload is shared with table/ALTER checks,
    /// but it remains a column option so option order and Go's `NOT NULL`
    /// injection behavior stay observable.
    Check(CheckConstraintDefinition),
    /// `REFERENCES table (parts...) [MATCH ...] [ON ...]`.  Go models a
    /// column-level reference as the same `ast.ReferenceDef` payload used by
    /// table-level foreign keys; keep that shared, lossless representation
    /// instead of flattening it into a special one-column constraint.
    Reference(ForeignKeyReference),
    /// `COLUMN_FORMAT {DEFAULT|FIXED|DYNAMIC}`.  This is a storage-layout
    /// declaration, not an arbitrary keyword string: TiDB's hand parser
    /// accepts exactly these three values and the AST restore owns their
    /// canonical uppercase spelling.
    ColumnFormat(ColumnFormat),
    /// `STORAGE {DEFAULT|DISK|MEMORY}`. Go parses and retains this AST
    /// payload even though storage engines emit a warning that they ignore
    /// it; the Rust parser has no warning channel, so the lossless AST is
    /// the complete parser-level contract here.
    Storage(ColumnStorage),
    /// TiDB's `AUTO_RANDOM[(shard_bits[, range_bits])]` column attribute.
    /// `None` is Go's `types.UnspecifiedLength`; each written argument stays
    /// independently observable, rather than being flattened into text.
    AutoRandom(AutoRandomOption),
    /// `SECONDARY_ENGINE_ATTRIBUTE [=] 'json'` on a column.
    SecondaryEngineAttribute(String),
    /// MariaDB system-versioned column marker, enabled only under the
    /// parser's MariaDB mode. It has no execution semantics by itself.
    MariaDbRowStart,
    /// MariaDB system-versioned column marker, enabled only under the
    /// parser's MariaDB mode. It has no execution semantics by itself.
    MariaDbRowEnd,
}

/// The closed vocabulary accepted after `COLUMN_FORMAT`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnFormat {
    /// `DEFAULT`.
    Default,
    /// `FIXED`.
    Fixed,
    /// `DYNAMIC`.
    Dynamic,
}

impl ColumnFormat {
    fn keyword(self) -> &'static str {
        match self {
            Self::Default => "DEFAULT",
            Self::Fixed => "FIXED",
            Self::Dynamic => "DYNAMIC",
        }
    }
}

/// The closed vocabulary accepted after `STORAGE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnStorage {
    /// `DEFAULT`.
    Default,
    /// `DISK`.
    Disk,
    /// `MEMORY`.
    Memory,
}

impl ColumnStorage {
    fn keyword(self) -> &'static str {
        match self {
            Self::Default => "DEFAULT",
            Self::Disk => "DISK",
            Self::Memory => "MEMORY",
        }
    }
}

/// One typed `AUTO_RANDOM` argument list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutoRandomOption {
    /// Number of shard bits, omitted for bare `AUTO_RANDOM`.
    pub shard_bits: Option<u64>,
    /// Number of range bits, omitted unless the two-argument form is used.
    pub range_bits: Option<u64>,
}

/// The key class selected by an inline column option.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InlineKeyKind {
    /// `PRIMARY KEY` (or its Go-compatible `KEY` spelling), with the
    /// optional TiDB storage layout that only this key class accepts.
    Primary {
        /// Explicit `CLUSTERED` or `NONCLUSTERED`; absent for the Go default.
        storage: Option<PrimaryKeyStorage>,
    },
    /// `UNIQUE` or `UNIQUE KEY`; Go restores either spelling as `UNIQUE KEY`.
    Unique,
}

/// One inline primary/unique key option, preserving all AST-visible facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineKeyOption {
    /// Whether this is a primary or unique key and any primary-only storage.
    pub kind: InlineKeyKind,
    /// Whether Go's `parseGlobalLocalOption` saw `GLOBAL`. `LOCAL` is
    /// intentionally represented by `false`, exactly like an omitted suffix:
    /// Go consumes it but leaves `ColumnOption.StrValue` empty.
    pub global: bool,
}

impl InlineKeyOption {
    /// Constructs an inline primary-key option.
    pub fn primary(storage: Option<PrimaryKeyStorage>, global: bool) -> Self {
        Self {
            kind: InlineKeyKind::Primary { storage },
            global,
        }
    }

    /// Constructs an inline unique-key option.
    pub fn unique(global: bool) -> Self {
        Self {
            kind: InlineKeyKind::Unique,
            global,
        }
    }

    /// Whether this option is the column's primary key.
    pub fn is_primary(&self) -> bool {
        matches!(self.kind, InlineKeyKind::Primary { .. })
    }

    /// Whether this option is the column's unique key.
    pub fn is_unique(&self) -> bool {
        matches!(self.kind, InlineKeyKind::Unique)
    }

    fn restore_into(&self, out: &mut String) {
        match self.kind {
            InlineKeyKind::Primary { storage } => {
                out.push_str("PRIMARY KEY");
                if let Some(storage) = storage {
                    out.push(' ');
                    out.push_str(storage.sql());
                }
            }
            InlineKeyKind::Unique => out.push_str("UNIQUE KEY"),
        }
        if self.global {
            // Go's ColumnOption.Restore writes this as an ordinary keyword,
            // unlike table/index-option GLOBAL which uses a feature comment.
            out.push_str(" GLOBAL");
        }
    }

    fn restore_into_with_context(&self, out: &mut String, context: RestoreContext) {
        match self.kind {
            InlineKeyKind::Primary {
                storage: Some(storage),
            } => {
                out.push_str("PRIMARY KEY ");
                context.write_with_tidb_special_comment(out, "clustered_index", |out| {
                    out.push_str(storage.sql());
                });
            }
            _ => self.restore_into(out),
        }
        if matches!(self.kind, InlineKeyKind::Primary { storage: Some(_) }) && self.global {
            out.push_str(" GLOBAL");
        }
    }
}

impl ColumnOption {
    /// Whether this is an inline primary-key option, irrespective of its
    /// physical storage or `GLOBAL` modifier.
    pub fn is_inline_primary_key(&self) -> bool {
        matches!(self, Self::InlineKey(key) if key.is_primary())
    }

    /// Whether this is an inline unique-key option, irrespective of its
    /// `GLOBAL` modifier.
    pub fn is_inline_unique_key(&self) -> bool {
        matches!(self, Self::InlineKey(key) if key.is_unique())
    }

    fn restore_into(&self, out: &mut String) {
        match self {
            ColumnOption::InlineKey(key) => key.restore_into(out),
            ColumnOption::NotNull => out.push_str("NOT NULL"),
            ColumnOption::Null => out.push_str("NULL"),
            ColumnOption::AutoIncrement => out.push_str("AUTO_INCREMENT"),
            ColumnOption::Default(e) => {
                out.push_str("DEFAULT ");
                restore_column_default_expression(e, out);
            }
            ColumnOption::Generated {
                expression, stored, ..
            } => {
                out.push_str("GENERATED ALWAYS AS(");
                expression.restore_into(out);
                if *stored {
                    out.push_str(") STORED");
                } else {
                    out.push_str(") VIRTUAL");
                }
            }
            ColumnOption::OnUpdate(expression) => {
                out.push_str("ON UPDATE ");
                expression.restore_into(out);
            }
            ColumnOption::Comment(text) => {
                out.push_str("COMMENT '");
                out.push_str(&escape_string_literal(text));
                out.push('\'');
            }
            ColumnOption::Collate(name) => {
                out.push_str("COLLATE ");
                out.push_str(name);
            }
            ColumnOption::Check(check) => check.restore_into(out),
            ColumnOption::Reference(reference) => reference.restore_into(out),
            ColumnOption::ColumnFormat(format) => {
                out.push_str("COLUMN_FORMAT ");
                out.push_str(format.keyword());
            }
            ColumnOption::Storage(storage) => {
                out.push_str("STORAGE ");
                out.push_str(storage.keyword());
            }
            ColumnOption::AutoRandom(option) => restore_auto_random(out, option),
            ColumnOption::SecondaryEngineAttribute(value) => {
                out.push_str("SECONDARY_ENGINE_ATTRIBUTE = '");
                out.push_str(&escape_string_literal(value));
                out.push('\'');
            }
            ColumnOption::MariaDbRowStart => out.push_str("GENERATED ALWAYS AS ROW START"),
            ColumnOption::MariaDbRowEnd => out.push_str("GENERATED ALWAYS AS ROW END"),
        }
    }

    fn restore_into_with_context(&self, out: &mut String, context: RestoreContext) {
        match self {
            ColumnOption::InlineKey(key) => key.restore_into_with_context(out, context),
            ColumnOption::Generated {
                expression, stored, ..
            } => {
                out.push_str("GENERATED ALWAYS AS(");
                expression.restore_into_with_context(out, context);
                out.push_str(if *stored { ") STORED" } else { ") VIRTUAL" });
            }
            ColumnOption::OnUpdate(expression) => {
                out.push_str("ON UPDATE ");
                expression.restore_into_with_context(out, context);
            }
            ColumnOption::Check(check) => check.restore_into_with_context(out, context),
            ColumnOption::AutoRandom(option) => {
                context.write_with_tidb_special_comment(out, FEATURE_AUTO_RANDOM, |out| {
                    restore_auto_random(out, option)
                });
            }
            _ => self.restore_into(out),
        }
    }
}

fn restore_auto_random(out: &mut String, option: &AutoRandomOption) {
    out.push_str("AUTO_RANDOM");
    if let Some(shard_bits) = option.shard_bits {
        out.push('(');
        out.push_str(&shard_bits.to_string());
        if let Some(range_bits) = option.range_bits {
            out.push_str(", ");
            out.push_str(&range_bits.to_string());
        }
        out.push(')');
    }
}

/// Restores a column `DEFAULT` expression using Go's
/// `ast.ColumnOption.Restore` contract.
///
/// TiDB deliberately prints one outer pair of parentheses around function
/// defaults other than `CURRENT_TIMESTAMP` (and around a column reference),
/// even though the AST expression itself has no outer `Paren` node after the
/// DDL parser canonicalizes it.  Keep this at the shared column-option
/// boundary: generated columns, ordinary expressions, and `ON UPDATE` use
/// their own restore contracts and must not inherit this formatting rule.
fn restore_column_default_expression(expression: &Expr, out: &mut String) {
    let outer_parentheses = matches!(
        expression,
        Expr::Func { name, .. } if !name.eq_ignore_ascii_case("CURRENT_TIMESTAMP")
    ) || matches!(expression, Expr::Column(_));
    if outer_parentheses {
        out.push('(');
    }
    expression.restore_into(out);
    if outer_parentheses {
        out.push(')');
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for ColumnDef {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            qualifier,
            name,
            ty,
            options,
        } = self;
        if !crate::Visitable::accept(ty, visitor) {
            return false;
        }
        for value in options.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = qualifier;
        let _ = name;
        let _ = ty;
        let _ = options;
        visitor.leave(self)
    }
}

impl crate::Visitable for ColumnOption {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::InlineKey(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::NotNull => {}
            Self::Null => {}
            Self::AutoIncrement => {}
            Self::Default(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Generated {
                expression, stored, ..
            } => {
                if !crate::Visitable::accept(expression, visitor) {
                    return false;
                }
                let _ = expression;
                let _ = stored;
            }
            Self::OnUpdate(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Comment(field_0) => {
                let _ = field_0;
            }
            Self::Collate(field_0) => {
                let _ = field_0;
            }
            Self::Check(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Reference(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ColumnFormat(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Storage(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::AutoRandom(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::SecondaryEngineAttribute(field_0) => {
                let _ = field_0;
            }
            Self::MariaDbRowStart => {}
            Self::MariaDbRowEnd => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ColumnFormat {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Default => {}
            Self::Fixed => {}
            Self::Dynamic => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ColumnStorage {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Default => {}
            Self::Disk => {}
            Self::Memory => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AutoRandomOption {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            shard_bits,
            range_bits,
        } = self;
        let _ = shard_bits;
        let _ = range_bits;
        visitor.leave(self)
    }
}

impl crate::Visitable for InlineKeyKind {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Primary { storage } => {
                if let Some(value) = storage.as_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = storage;
            }
            Self::Unique => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for InlineKeyOption {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { kind, global } = self;
        if !crate::Visitable::accept(kind, visitor) {
            return false;
        }
        let _ = kind;
        let _ = global;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
