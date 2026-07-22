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

//! Source-shaped index metadata shared by TiDB's DDL statement families.
//!
//! Go keeps the index class, every [`IndexOptions`] field, and the online DDL
//! modifiers as typed AST state.  This leaf deliberately introduces that full
//! vocabulary without yet changing an existing statement envelope: the parser
//! and executor can switch to one lossless contract atomically.

use crate::{IndexType, PrimaryKeyStorage, ReferentialAction};

use super::{
    back_quote, escape_string_literal, push_index_parts, push_name_path, Expr, IndexPart,
    RestoreContext, SplitOption,
};

const FEATURE_CLUSTERED_INDEX: &str = "clustered_index";
const FEATURE_GLOBAL_INDEX: &str = "global_index";
const FEATURE_PRE_SPLIT: &str = "pre_split";
const FEATURE_TIDB: &str = "";

/// The source index class carried by Go's `ast.IndexKeyType` and
/// `ast.ConstraintType` index variants.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IndexKind {
    /// An ordinary `INDEX` (or input alias `KEY`).
    #[default]
    Ordinary,
    /// `UNIQUE INDEX`.
    Unique,
    /// `SPATIAL INDEX`.
    Spatial,
    /// `FULLTEXT INDEX`.
    Fulltext,
    /// `VECTOR INDEX`.
    Vector,
    /// `COLUMNAR INDEX`.
    Columnar,
}

impl IndexKind {
    /// Returns the canonical SQL prefix for this index kind.
    pub fn sql(self) -> &'static str {
        match self {
            Self::Ordinary => "INDEX",
            Self::Unique => "UNIQUE INDEX",
            Self::Spatial => "SPATIAL INDEX",
            Self::Fulltext => "FULLTEXT INDEX",
            Self::Vector => "VECTOR INDEX",
            Self::Columnar => "COLUMNAR INDEX",
        }
    }
}

/// The non-default visibility stored by Go's `ast.IndexOption.Visibility`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexVisibility {
    /// `VISIBLE`.
    Visible,
    /// `INVISIBLE`.
    Invisible,
}

impl IndexVisibility {
    /// Returns the canonical SQL spelling for this visibility mode.
    pub fn sql(self) -> &'static str {
        match self {
            Self::Visible => "VISIBLE",
            Self::Invisible => "INVISIBLE",
        }
    }
}

/// The non-default online DDL algorithm from Go's `AlgorithmType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexAlgorithm {
    /// `ALGORITHM = COPY`.
    Copy,
    /// `ALGORITHM = INPLACE`.
    Inplace,
    /// `ALGORITHM = INSTANT`.
    Instant,
}

impl IndexAlgorithm {
    /// Returns the canonical SQL spelling for this online DDL algorithm.
    pub fn sql(self) -> &'static str {
        match self {
            Self::Copy => "COPY",
            Self::Inplace => "INPLACE",
            Self::Instant => "INSTANT",
        }
    }
}

/// The non-default online DDL lock from Go's `LockType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexLock {
    /// `LOCK = NONE`.
    None,
    /// `LOCK = SHARED`.
    Shared,
    /// `LOCK = EXCLUSIVE`.
    Exclusive,
}

impl IndexLock {
    /// Returns the canonical SQL spelling for this online DDL lock mode.
    pub fn sql(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Shared => "SHARED",
            Self::Exclusive => "EXCLUSIVE",
        }
    }
}

/// Online DDL modifiers shared by standalone `CREATE INDEX` and `DROP INDEX`.
///
/// Go keeps `DEFAULT` internally but its AST restore omits it.  Absence makes
/// that canonical form structural, while retaining all observable alternatives.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IndexOnlineDdl {
    /// Optional non-default algorithm.
    pub algorithm: Option<IndexAlgorithm>,
    /// Optional non-default lock mode.
    pub lock: Option<IndexLock>,
}

impl IndexOnlineDdl {
    /// Restores the canonical online DDL modifier list.
    pub fn restore(&self) -> String {
        let mut out = String::new();
        self.restore_into(&mut out);
        out
    }

    /// Appends Go's canonical algorithm-then-lock order without a leading
    /// separator and reports whether it wrote any bytes.
    pub(crate) fn restore_into(&self, out: &mut String) -> bool {
        let mut wrote = false;
        if let Some(algorithm) = self.algorithm {
            out.push_str("ALGORITHM = ");
            out.push_str(algorithm.sql());
            wrote = true;
        }
        if let Some(lock) = self.lock {
            if wrote {
                out.push(' ');
            }
            out.push_str("LOCK = ");
            out.push_str(lock.sql());
            wrote = true;
        }
        wrote
    }
}

/// The two source forms of `PRE_SPLIT_REGIONS` in Go's `IndexOption`.
///
/// A bare count is not a `SplitOption` boundary payload, while the
/// parenthesized form is. Keeping the distinction prevents a parser from
/// inventing a boundary tuple for `PRE_SPLIT_REGIONS = 3`.
#[derive(Debug, Clone, PartialEq)]
pub enum IndexPreSplitRegions {
    /// `PRE_SPLIT_REGIONS = count`. Go canonicalizes a zero count to the
    /// empty parenthesized form because its `SplitOption.Num` is zero.
    Count(i64),
    /// `PRE_SPLIT_REGIONS = (<split option>)`.
    Boundaries(SplitOption),
}

impl IndexPreSplitRegions {
    fn restore_into(&self, out: &mut String) {
        match self {
            Self::Count(count) if *count != 0 => out.push_str(&count.to_string()),
            Self::Count(_) => out.push_str("()"),
            Self::Boundaries(option) => {
                out.push('(');
                option.restore_into(out);
                out.push(')');
            }
        }
    }
}

/// Every source-visible field in Go's `ast.IndexOption` that this AST can
/// represent using its existing expression and split-boundary values.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct IndexOptions {
    /// `KEY_BLOCK_SIZE = value`; zero is represented by absence because Go
    /// omits it on restore.
    pub key_block_size: Option<u64>,
    /// `USING <method>`.
    pub index_type: Option<IndexType>,
    /// `WITH PARSER name`.
    pub parser_name: Option<String>,
    /// `COMMENT 'text'`.
    pub comment: Option<String>,
    /// Explicit `VISIBLE` or `INVISIBLE`; Go's default is absent.
    pub visibility: Option<IndexVisibility>,
    /// Explicit primary-key storage mode, shared with the existing typed
    /// `CLUSTERED`/`NONCLUSTERED` representation.
    pub primary_key_storage: Option<PrimaryKeyStorage>,
    /// `GLOBAL`; Go accepts `LOCAL` but records it as this false default.
    pub global: bool,
    /// `PRE_SPLIT_REGIONS = ...`, retaining its bare-count versus
    /// parenthesized split-payload source distinction.
    pub pre_split_regions: Option<IndexPreSplitRegions>,
    /// `SECONDARY_ENGINE_ATTRIBUTE = 'json'`.
    pub secondary_engine_attribute: Option<String>,
    /// `ADD_COLUMNAR_REPLICA_ON_DEMAND`. Go stores an integer marker and
    /// restores the clause only when it is positive.
    pub add_columnar_replica_on_demand: i64,
    /// `WHERE expression` for a partial index.
    pub condition: Option<Expr>,
}

impl IndexOptions {
    /// Mirrors Go's `IndexOption.IsEmpty` guard used by statement restore.
    ///
    /// The source currently does not count
    /// `ADD_COLUMNAR_REPLICA_ON_DEMAND` for this decision.  Preserve that
    /// observable behavior rather than inventing a more intuitive one.
    pub(crate) fn is_empty_for_statement_restore(&self) -> bool {
        self.primary_key_storage.is_none()
            && self.key_block_size.unwrap_or_default() == 0
            && self.index_type.is_none()
            && self.parser_name.as_deref().is_none_or(str::is_empty)
            && self.comment.as_deref().is_none_or(str::is_empty)
            && !self.global
            && self.visibility.is_none()
            && self.pre_split_regions.is_none()
            && self
                .secondary_engine_attribute
                .as_deref()
                .is_none_or(str::is_empty)
            && self.condition.is_none()
    }

    /// Restores the canonical source-visible index option list.
    pub fn restore(&self) -> String {
        let mut out = String::new();
        self.restore_into(&mut out);
        out
    }

    /// Restores this option list with a statement-wide source-formatting
    /// context.
    pub fn restore_with_context(&self, context: RestoreContext) -> String {
        let mut out = String::new();
        self.restore_into_with_context(&mut out, context);
        out
    }

    /// Appends ordinary canonical index options and reports whether it wrote
    /// any bytes.
    pub(crate) fn restore_into(&self, out: &mut String) -> bool {
        self.restore_into_with_context(out, RestoreContext::default())
    }

    /// Appends Go's fixed canonical option order under `context` and reports
    /// whether it wrote any bytes.
    pub(crate) fn restore_into_with_context(
        &self,
        out: &mut String,
        context: RestoreContext,
    ) -> bool {
        let mut wrote = false;
        let mut push_separator = |out: &mut String| {
            if wrote {
                out.push(' ');
            }
            wrote = true;
        };

        if self.add_columnar_replica_on_demand > 0 {
            push_separator(out);
            out.push_str("ADD_COLUMNAR_REPLICA_ON_DEMAND");
        }
        if let Some(storage) = self.primary_key_storage {
            push_separator(out);
            context.write_with_tidb_special_comment(out, FEATURE_CLUSTERED_INDEX, |out| {
                out.push_str(match storage {
                    PrimaryKeyStorage::Clustered => "CLUSTERED",
                    PrimaryKeyStorage::NonClustered => "NONCLUSTERED",
                });
            });
        }
        if let Some(size) = self.key_block_size.filter(|size| *size > 0) {
            push_separator(out);
            out.push_str("KEY_BLOCK_SIZE=");
            out.push_str(&size.to_string());
        }
        if let Some(index_type) = self.index_type {
            push_separator(out);
            out.push_str("USING ");
            out.push_str(index_type.sql());
        }
        if let Some(parser_name) = self.parser_name.as_deref().filter(|name| !name.is_empty()) {
            push_separator(out);
            out.push_str("WITH PARSER `");
            out.push_str(&parser_name.replace('`', "``"));
            out.push('`');
        }
        if let Some(comment) = self
            .comment
            .as_deref()
            .filter(|comment| !comment.is_empty())
        {
            push_separator(out);
            out.push_str("COMMENT '");
            out.push_str(&escape_string_literal(comment));
            out.push('\'');
        }
        if self.global {
            push_separator(out);
            context.write_with_tidb_special_comment(out, FEATURE_GLOBAL_INDEX, |out| {
                out.push_str("GLOBAL");
            });
        }
        if let Some(visibility) = self.visibility {
            push_separator(out);
            out.push_str(visibility.sql());
        }
        if let Some(pre_split_regions) = &self.pre_split_regions {
            push_separator(out);
            context.write_with_tidb_special_comment(out, FEATURE_PRE_SPLIT, |out| {
                out.push_str("PRE_SPLIT_REGIONS = ");
                pre_split_regions.restore_into(out);
            });
        }
        if let Some(attribute) = self
            .secondary_engine_attribute
            .as_deref()
            .filter(|attribute| !attribute.is_empty())
        {
            push_separator(out);
            out.push_str("SECONDARY_ENGINE_ATTRIBUTE = '");
            out.push_str(&escape_string_literal(attribute));
            out.push('\'');
        }
        if let Some(condition) = &self.condition {
            push_separator(out);
            out.push_str("WHERE ");
            condition.restore_into(out);
        }
        wrote
    }
}

/// Go's index-bearing `ast.ConstraintType` variants.
///
/// This intentionally differs from [`IndexKind`]: standalone `CREATE INDEX`
/// restores `FULLTEXT INDEX`, while a table/ALTER constraint restores only
/// `FULLTEXT`; the Go AST has distinct source shapes for those two routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexConstraintKind {
    /// `PRIMARY KEY`.
    PrimaryKey,
    /// `KEY`.
    Key,
    /// `INDEX`.
    Index,
    /// `UNIQUE`.
    Unique,
    /// `UNIQUE KEY`.
    UniqueKey,
    /// `UNIQUE INDEX`.
    UniqueIndex,
    /// `FULLTEXT`.
    Fulltext,
    /// `VECTOR INDEX`.
    Vector,
    /// `COLUMNAR INDEX`.
    Columnar,
}

impl IndexConstraintKind {
    fn sql(self) -> &'static str {
        match self {
            Self::PrimaryKey => "PRIMARY KEY",
            Self::Key => "KEY",
            Self::Index => "INDEX",
            Self::Unique => "UNIQUE",
            Self::UniqueKey => "UNIQUE KEY",
            Self::UniqueIndex => "UNIQUE INDEX",
            Self::Fulltext => "FULLTEXT",
            Self::Vector => "VECTOR INDEX",
            Self::Columnar => "COLUMNAR INDEX",
        }
    }

    fn supports_if_not_exists(self) -> bool {
        matches!(
            self,
            Self::Key | Self::Index | Self::Vector | Self::Columnar
        )
    }
}

/// Source-shaped non-foreign-key `ast.Constraint` payload shared by CREATE
/// TABLE and `ALTER TABLE ... ADD`.
///
/// This is deliberately preparatory: current statement envelopes still use
/// their narrower execution-facing types until parser and executor migration
/// can land atomically. It nevertheless carries every index-specific field
/// Go's constraint AST restores, so that migration has no lossy bridge.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexConstraintDefinition {
    /// The exact Go constraint class.
    pub kind: IndexConstraintKind,
    /// `IF NOT EXISTS`, meaningful for the Go constraint kinds that retain
    /// it during restore.
    pub if_not_exists: bool,
    /// The optional constraint or inline index name.
    pub name: Option<String>,
    /// Go's `Constraint.IsEmptyIndex` marker. It forces the name separator
    /// even if the name is empty.
    pub is_empty_index: bool,
    /// Key columns or functional key expressions.
    pub parts: Vec<IndexPart>,
    /// All typed index options.
    pub options: IndexOptions,
}

impl IndexConstraintDefinition {
    /// Restores this Go constraint shape without an enclosing `ADD` prefix.
    pub fn restore(&self) -> String {
        let mut out = String::new();
        self.restore_into(&mut out);
        out
    }

    /// Restores this Go constraint shape with `context`.
    pub fn restore_with_context(&self, context: RestoreContext) -> String {
        let mut out = String::new();
        self.restore_into_with_context(&mut out, context);
        out
    }

    /// Appends the ordinary canonical constraint form.
    pub(crate) fn restore_into(&self, out: &mut String) {
        self.restore_into_with_context(out, RestoreContext::default());
    }

    pub(crate) fn restore_into_with_context(&self, out: &mut String, context: RestoreContext) {
        out.push_str(self.kind.sql());
        if self.if_not_exists && self.kind.supports_if_not_exists() {
            context.write_with_tidb_special_comment(out, FEATURE_TIDB, |out| {
                out.push_str(" IF NOT EXISTS");
            });
        }
        if let Some(name) = &self.name {
            out.push(' ');
            out.push_str(&back_quote(name));
        } else if self.is_empty_index {
            out.push(' ');
        }
        push_index_parts(out, &self.parts);
        let options = self.options.restore_with_context(context);
        if !self.options.is_empty_for_statement_restore() {
            out.push(' ');
            out.push_str(&options);
        }
    }
}

/// Go's `ast.MatchType` for a foreign-key reference.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ForeignKeyMatch {
    /// No `MATCH` clause.
    #[default]
    None,
    /// `MATCH FULL`.
    Full,
    /// `MATCH PARTIAL`.
    Partial,
    /// `MATCH SIMPLE`.
    Simple,
}

impl ForeignKeyMatch {
    fn sql(self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Full => Some("FULL"),
            Self::Partial => Some("PARTIAL"),
            Self::Simple => Some("SIMPLE"),
        }
    }
}

/// Go's `ast.ReferenceDef`, including index-part syntax in the referenced
/// key and the otherwise commonly dropped `MATCH` clause.
#[derive(Debug, Clone, PartialEq)]
pub struct ForeignKeyReference {
    /// Referenced table. `None` is retained for the Go AST's partial node
    /// shape even though normal SQL construction supplies a table.
    pub table: Option<Vec<String>>,
    /// Referenced key parts. `None` differs from `Some(vec![])` exactly as
    /// Go's nil slice differs from an explicitly empty parenthesized list.
    pub parts: Option<Vec<IndexPart>>,
    /// Optional match mode.
    pub match_type: ForeignKeyMatch,
    /// Parent-row delete action.
    pub on_delete: Option<ReferentialAction>,
    /// Parent-row update action.
    pub on_update: Option<ReferentialAction>,
}

impl ForeignKeyReference {
    /// Restores Go's canonical `REFERENCES` payload.
    pub fn restore(&self) -> String {
        let mut out = String::new();
        self.restore_into(&mut out);
        out
    }

    pub(crate) fn restore_into(&self, out: &mut String) {
        if let Some(table) = &self.table {
            out.push_str("REFERENCES ");
            push_name_path(out, table);
        }
        if let Some(parts) = &self.parts {
            push_index_parts(out, parts);
        }
        if let Some(match_type) = self.match_type.sql() {
            out.push_str(" MATCH ");
            out.push_str(match_type);
        }
        if let Some(on_delete) = &self.on_delete {
            out.push_str(" ON DELETE ");
            out.push_str(on_delete.sql());
        }
        if let Some(on_update) = &self.on_update {
            out.push_str(" ON UPDATE ");
            out.push_str(on_update.sql());
        }
    }
}

/// Full foreign-key `ast.Constraint` payload. Catalog execution deliberately
/// rejects the parts and MATCH forms it cannot yet represent before mutation.
#[derive(Debug, Clone, PartialEq)]
pub struct ForeignKeyConstraintDefinition {
    /// Optional constraint name.
    pub name: Option<String>,
    /// `FOREIGN KEY IF NOT EXISTS`.
    pub if_not_exists: bool,
    /// Referencing key parts.
    pub parts: Vec<IndexPart>,
    /// Referenced table/key/action payload.
    pub reference: ForeignKeyReference,
}

impl ForeignKeyConstraintDefinition {
    /// Restores this Go foreign-key constraint shape without an enclosing
    /// `ADD` prefix.
    pub fn restore(&self) -> String {
        let mut out = String::new();
        self.restore_into(&mut out);
        out
    }

    /// Restores this Go foreign-key constraint shape with `context`.
    pub fn restore_with_context(&self, context: RestoreContext) -> String {
        let mut out = String::new();
        self.restore_into_with_context(&mut out, context);
        out
    }

    pub(crate) fn restore_into(&self, out: &mut String) {
        self.restore_into_with_context(out, RestoreContext::default());
    }

    pub(crate) fn restore_into_with_context(&self, out: &mut String, context: RestoreContext) {
        out.push_str("CONSTRAINT ");
        if let Some(name) = &self.name {
            out.push_str(&back_quote(name));
            out.push(' ');
        }
        out.push_str("FOREIGN KEY ");
        if self.if_not_exists {
            context.write_with_tidb_special_comment(out, FEATURE_TIDB, |out| {
                out.push_str("IF NOT EXISTS ");
            });
        }
        push_index_parts(out, &self.parts);
        out.push(' ');
        self.reference.restore_into(out);
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for IndexKind {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Ordinary => {}
            Self::Unique => {}
            Self::Spatial => {}
            Self::Fulltext => {}
            Self::Vector => {}
            Self::Columnar => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for IndexVisibility {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Visible => {}
            Self::Invisible => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for IndexAlgorithm {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Copy => {}
            Self::Inplace => {}
            Self::Instant => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for IndexLock {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::None => {}
            Self::Shared => {}
            Self::Exclusive => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for IndexOnlineDdl {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { algorithm, lock } = self;
        if let Some(value) = algorithm.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = lock.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = algorithm;
        let _ = lock;
        visitor.leave(self)
    }
}

impl crate::Visitable for IndexPreSplitRegions {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Count(field_0) => {
                let _ = field_0;
            }
            Self::Boundaries(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for IndexOptions {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            key_block_size,
            index_type,
            parser_name,
            comment,
            visibility,
            primary_key_storage,
            global,
            pre_split_regions,
            secondary_engine_attribute,
            add_columnar_replica_on_demand,
            condition,
        } = self;
        if let Some(value) = index_type.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = visibility.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = primary_key_storage.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = pre_split_regions.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = condition.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = key_block_size;
        let _ = index_type;
        let _ = parser_name;
        let _ = comment;
        let _ = visibility;
        let _ = primary_key_storage;
        let _ = global;
        let _ = pre_split_regions;
        let _ = secondary_engine_attribute;
        let _ = add_columnar_replica_on_demand;
        let _ = condition;
        visitor.leave(self)
    }
}

impl crate::Visitable for IndexConstraintKind {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::PrimaryKey => {}
            Self::Key => {}
            Self::Index => {}
            Self::Unique => {}
            Self::UniqueKey => {}
            Self::UniqueIndex => {}
            Self::Fulltext => {}
            Self::Vector => {}
            Self::Columnar => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for IndexConstraintDefinition {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            kind,
            if_not_exists,
            name,
            is_empty_index,
            parts,
            options,
        } = self;
        if !crate::Visitable::accept(kind, visitor) {
            return false;
        }
        for value in parts.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if !crate::Visitable::accept(options, visitor) {
            return false;
        }
        let _ = kind;
        let _ = if_not_exists;
        let _ = name;
        let _ = is_empty_index;
        let _ = parts;
        let _ = options;
        visitor.leave(self)
    }
}

impl crate::Visitable for ForeignKeyMatch {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::None => {}
            Self::Full => {}
            Self::Partial => {}
            Self::Simple => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ForeignKeyReference {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            table,
            parts,
            match_type,
            on_delete,
            on_update,
        } = self;
        if let Some(value) = parts.as_mut() {
            for value in value.iter_mut() {
                if !crate::Visitable::accept(value, visitor) {
                    return false;
                }
            }
        }
        if !crate::Visitable::accept(match_type, visitor) {
            return false;
        }
        if let Some(value) = on_delete.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = on_update.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = table;
        let _ = parts;
        let _ = match_type;
        let _ = on_delete;
        let _ = on_update;
        visitor.leave(self)
    }
}

impl crate::Visitable for ForeignKeyConstraintDefinition {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            name,
            if_not_exists,
            parts,
            reference,
        } = self;
        for value in parts.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if !crate::Visitable::accept(reference, visitor) {
            return false;
        }
        let _ = name;
        let _ = if_not_exists;
        let _ = parts;
        let _ = reference;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AlterTableAction, AlterTableStmt, CreateIndexStmt, CreateTableStmt, CreateTableTemporary,
        IndexPart, TableConstraint,
    };

    fn column(name: &str) -> IndexPart {
        IndexPart::Column {
            name: name.to_string(),
            prefix_len: None,
            desc: false,
        }
    }

    fn restore_create_index(stmt: CreateIndexStmt) -> String {
        let mut out = String::new();
        stmt.restore_into(&mut out);
        out
    }

    fn restore_create_table(constraint: TableConstraint) -> String {
        let mut out = String::new();
        CreateTableStmt {
            temporary: CreateTableTemporary::None,
            on_commit_delete: false,
            if_not_exists: false,
            name: vec!["child".to_string()],
            like_table: None,
            columns: Vec::new(),
            table_constraints: vec![constraint],
            table_options: Vec::new(),
            partitioning: None,
            splits: Vec::new(),
            ctas: None,
        }
        .restore_into(&mut out);
        out
    }

    #[test]
    fn index_options_restore_in_go_canonical_order() {
        let options = IndexOptions {
            key_block_size: Some(16),
            index_type: Some(IndexType::Hnsw),
            parser_name: Some("ngram`parser".to_string()),
            comment: Some("a'b\\c".to_string()),
            visibility: Some(IndexVisibility::Invisible),
            primary_key_storage: Some(PrimaryKeyStorage::Clustered),
            global: true,
            pre_split_regions: Some(IndexPreSplitRegions::Boundaries(SplitOption::By(vec![
                vec![Expr::Int("1".to_string())],
            ]))),
            secondary_engine_attribute: Some("{\"engine\":\"x\"}".to_string()),
            add_columnar_replica_on_demand: 1,
            condition: Some(Expr::Column(vec!["a".to_string()])),
        };

        let out = options.restore();
        assert_eq!(
            out,
            "ADD_COLUMNAR_REPLICA_ON_DEMAND CLUSTERED KEY_BLOCK_SIZE=16 USING HNSW \
             WITH PARSER `ngram``parser` COMMENT 'a''b\\\\c' GLOBAL INVISIBLE \
             PRE_SPLIT_REGIONS = (BY (1)) SECONDARY_ENGINE_ATTRIBUTE = '{\"engine\":\"x\"}' \
             WHERE `a`"
        );
    }

    #[test]
    fn index_online_ddl_restores_algorithm_before_lock() {
        let online = IndexOnlineDdl {
            algorithm: Some(IndexAlgorithm::Inplace),
            lock: Some(IndexLock::Exclusive),
        };
        assert_eq!(online.restore(), "ALGORITHM = INPLACE LOCK = EXCLUSIVE");

        assert!(IndexOnlineDdl::default().restore().is_empty());
    }

    #[test]
    fn index_pre_split_regions_retains_the_count_form() {
        let options = IndexOptions {
            pre_split_regions: Some(IndexPreSplitRegions::Count(3)),
            ..IndexOptions::default()
        };
        assert_eq!(options.restore(), "PRE_SPLIT_REGIONS = 3");

        let zero = IndexOptions {
            pre_split_regions: Some(IndexPreSplitRegions::Count(0)),
            ..IndexOptions::default()
        };
        assert_eq!(zero.restore(), "PRE_SPLIT_REGIONS = ()");
    }

    #[test]
    fn every_index_kind_and_type_has_a_canonical_spelling() {
        assert_eq!(IndexKind::Ordinary.sql(), "INDEX");
        assert_eq!(IndexKind::Unique.sql(), "UNIQUE INDEX");
        assert_eq!(IndexKind::Spatial.sql(), "SPATIAL INDEX");
        assert_eq!(IndexKind::Fulltext.sql(), "FULLTEXT INDEX");
        assert_eq!(IndexKind::Vector.sql(), "VECTOR INDEX");
        assert_eq!(IndexKind::Columnar.sql(), "COLUMNAR INDEX");

        assert_eq!(IndexType::Btree.sql(), "BTREE");
        assert_eq!(IndexType::Hash.sql(), "HASH");
        assert_eq!(IndexType::Rtree.sql(), "RTREE");
        assert_eq!(IndexType::Hypo.sql(), "HYPO");
        assert_eq!(IndexType::Vector.sql(), "VECTOR");
        assert_eq!(IndexType::Inverted.sql(), "INVERTED");
        assert_eq!(IndexType::Hnsw.sql(), "HNSW");
        assert_eq!(IndexType::Fulltext.sql(), "FULLTEXT");
    }

    #[test]
    fn create_index_restores_go_unique_hash_row() {
        let out = restore_create_index(CreateIndexStmt {
            kind: IndexKind::Unique,
            if_not_exists: false,
            name: "ident".to_string(),
            table: vec!["d_n".to_string(), "t_n".to_string()],
            parts: vec![column("ident"), column("ident")],
            options: IndexOptions {
                index_type: Some(IndexType::Hash),
                ..IndexOptions::default()
            },
            online: IndexOnlineDdl::default(),
        });

        assert_eq!(
            out,
            "CREATE UNIQUE INDEX `ident` ON `d_n`.`t_n` (`ident`, `ident`) USING HASH"
        );
    }

    #[test]
    fn create_index_restores_go_vector_functional_row() {
        let out = restore_create_index(CreateIndexStmt {
            kind: IndexKind::Vector,
            if_not_exists: false,
            name: "idx".to_string(),
            table: vec!["t".to_string()],
            parts: vec![IndexPart::Expr {
                expr: Expr::Func {
                    name: "vec_cosine_distance".to_string(),
                    args: vec![Expr::Column(vec!["a".to_string()])],
                },
                desc: false,
            }],
            options: IndexOptions {
                index_type: Some(IndexType::Hnsw),
                ..IndexOptions::default()
            },
            online: IndexOnlineDdl::default(),
        });

        assert_eq!(
            out,
            "CREATE VECTOR INDEX `idx` ON `t` ((VEC_COSINE_DISTANCE(`a`))) USING HNSW"
        );
    }

    #[test]
    fn create_index_uses_go_option_emptiness_for_columnar_marker_only() {
        let out = restore_create_index(CreateIndexStmt {
            kind: IndexKind::Ordinary,
            if_not_exists: false,
            name: "i".to_string(),
            table: vec!["t".to_string()],
            parts: vec![column("a")],
            options: IndexOptions {
                add_columnar_replica_on_demand: 1,
                ..IndexOptions::default()
            },
            online: IndexOnlineDdl::default(),
        });

        assert_eq!(out, "CREATE INDEX `i` ON `t` (`a`)");
    }

    #[test]
    fn create_index_restores_go_online_ddl_row_in_canonical_order() {
        let out = restore_create_index(CreateIndexStmt {
            kind: IndexKind::Ordinary,
            if_not_exists: false,
            name: "idx".to_string(),
            table: vec!["t".to_string()],
            parts: vec![column("a")],
            options: IndexOptions::default(),
            online: IndexOnlineDdl {
                algorithm: Some(IndexAlgorithm::Inplace),
                lock: Some(IndexLock::Exclusive),
            },
        });

        assert_eq!(
            out,
            "CREATE INDEX `idx` ON `t` (`a`) ALGORITHM = INPLACE LOCK = EXCLUSIVE"
        );
    }

    #[test]
    fn index_constraint_restores_go_fulltext_and_primary_option_rows() {
        let fulltext = IndexConstraintDefinition {
            kind: IndexConstraintKind::Fulltext,
            if_not_exists: true,
            name: Some("full_id".to_string()),
            is_empty_index: false,
            parts: vec![column("parent_id")],
            options: IndexOptions::default(),
        };
        assert_eq!(fulltext.restore(), "FULLTEXT `full_id`(`parent_id`)");

        let primary = IndexConstraintDefinition {
            kind: IndexConstraintKind::PrimaryKey,
            if_not_exists: false,
            name: None,
            is_empty_index: false,
            parts: vec![column("id")],
            options: IndexOptions {
                key_block_size: Some(32),
                index_type: Some(IndexType::Hash),
                comment: Some("hello".to_string()),
                ..IndexOptions::default()
            },
        };
        assert_eq!(
            primary.restore(),
            "PRIMARY KEY(`id`) KEY_BLOCK_SIZE=32 USING HASH COMMENT 'hello'"
        );

        let marker_only = IndexConstraintDefinition {
            kind: IndexConstraintKind::Columnar,
            if_not_exists: false,
            name: None,
            is_empty_index: false,
            parts: vec![column("a")],
            options: IndexOptions {
                add_columnar_replica_on_demand: 1,
                ..IndexOptions::default()
            },
        };
        assert_eq!(marker_only.restore(), "COLUMNAR INDEX(`a`)");
    }

    #[test]
    fn foreign_key_definition_restores_go_match_and_index_part_rows() {
        let foreign_key = ForeignKeyConstraintDefinition {
            name: Some("fk_123".to_string()),
            if_not_exists: false,
            parts: vec![
                IndexPart::Column {
                    name: "parent_id".to_string(),
                    prefix_len: Some(2),
                    desc: false,
                },
                column("hello"),
            ],
            reference: ForeignKeyReference {
                table: Some(vec!["parent".to_string()]),
                parts: Some(vec![IndexPart::Expr {
                    expr: Expr::Binary(
                        crate::BinaryOp::Plus,
                        Box::new(Expr::Column(vec!["id".to_string()])),
                        Box::new(Expr::Int("1".to_string())),
                    ),
                    desc: false,
                }]),
                match_type: ForeignKeyMatch::Full,
                on_delete: Some(ReferentialAction::Cascade),
                on_update: Some(ReferentialAction::Restrict),
            },
        };

        assert_eq!(
            foreign_key.restore(),
            "CONSTRAINT `fk_123` FOREIGN KEY (`parent_id`(2), `hello`) REFERENCES `parent`((`id`+1)) MATCH FULL ON DELETE CASCADE ON UPDATE RESTRICT"
        );
    }

    #[test]
    fn table_constraint_envelope_keeps_go_index_and_foreign_key_shapes() {
        let fulltext = restore_create_table(TableConstraint::Index(IndexConstraintDefinition {
            kind: IndexConstraintKind::Fulltext,
            if_not_exists: false,
            name: Some("full_id".to_string()),
            is_empty_index: false,
            parts: vec![column("parent_id")],
            options: IndexOptions::default(),
        }));
        assert_eq!(
            fulltext,
            "CREATE TABLE `child` (FULLTEXT `full_id`(`parent_id`))"
        );

        let foreign_key = restore_create_table(TableConstraint::ForeignKey(
            ForeignKeyConstraintDefinition {
                name: None,
                if_not_exists: false,
                parts: vec![IndexPart::Column {
                    name: "parent_id".to_string(),
                    prefix_len: Some(2),
                    desc: false,
                }],
                reference: ForeignKeyReference {
                    table: Some(vec!["parent".to_string()]),
                    parts: Some(vec![column("id")]),
                    match_type: ForeignKeyMatch::Simple,
                    on_delete: Some(ReferentialAction::Cascade),
                    on_update: None,
                },
            },
        ));
        assert_eq!(
            foreign_key,
            "CREATE TABLE `child` (CONSTRAINT FOREIGN KEY (`parent_id`(2)) REFERENCES `parent`(`id`) MATCH SIMPLE ON DELETE CASCADE)"
        );
    }

    #[test]
    fn alter_table_add_constraint_envelopes_keep_go_constraint_kinds() {
        let stmt = AlterTableStmt {
            name: vec!["t".to_string()],
            actions: vec![
                AlterTableAction::AddIndexConstraint(IndexConstraintDefinition {
                    kind: IndexConstraintKind::Columnar,
                    if_not_exists: true,
                    name: Some("c_idx".to_string()),
                    is_empty_index: false,
                    parts: vec![column("a")],
                    options: IndexOptions {
                        index_type: Some(IndexType::Inverted),
                        comment: Some("a".to_string()),
                        ..IndexOptions::default()
                    },
                }),
                AlterTableAction::AddForeignKey(ForeignKeyConstraintDefinition {
                    name: Some("fk".to_string()),
                    if_not_exists: true,
                    parts: vec![column("a")],
                    reference: ForeignKeyReference {
                        table: Some(vec!["p".to_string()]),
                        parts: Some(vec![column("id")]),
                        match_type: ForeignKeyMatch::None,
                        on_delete: None,
                        on_update: Some(ReferentialAction::SetNull),
                    },
                }),
            ],
        };
        let mut out = String::new();
        stmt.restore_into(&mut out);
        assert_eq!(
            out,
            "ALTER TABLE `t` ADD COLUMNAR INDEX IF NOT EXISTS `c_idx`(`a`) USING INVERTED COMMENT 'a', ADD CONSTRAINT `fk` FOREIGN KEY IF NOT EXISTS (`a`) REFERENCES `p`(`id`) ON UPDATE SET NULL"
        );
    }
}
