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

//! Shared AST model values transcreated from `pkg/parser/ast/model.go`.

use std::hash::{Hash, Hasher};

use serde::de::Deserializer;
use serde::ser::{SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};

/// A string retaining both its original and case-insensitive forms.
#[derive(Debug, Clone, Default)]
pub struct CiString {
    original: String,
    lowercase: String,
}

impl CiString {
    /// Constructs both representations using Unicode lowercase conversion.
    pub fn new(value: impl Into<String>) -> Self {
        let original = value.into();
        let lowercase = original.to_lowercase();
        Self {
            original,
            lowercase,
        }
    }

    /// Returns the exact original spelling.
    pub fn original(&self) -> &str {
        &self.original
    }

    /// Returns the lower-case comparison key.
    pub fn lowercase(&self) -> &str {
        &self.lowercase
    }

    /// Returns the source-compatible owned-memory estimate.
    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<String>() * 2 + self.original.len() + self.lowercase.len()
    }
}

impl std::fmt::Display for CiString {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.original)
    }
}

impl PartialEq for CiString {
    fn eq(&self, other: &Self) -> bool {
        self.lowercase == other.lowercase
    }
}

impl Eq for CiString {}

impl Hash for CiString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.lowercase.hash(state);
    }
}

impl Serialize for CiString {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut value = serializer.serialize_struct("CiString", 2)?;
        value.serialize_field("O", &self.original)?;
        value.serialize_field("L", &self.lowercase)?;
        value.end()
    }
}

impl<'de> Deserialize<'de> for CiString {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Representation {
            Object {
                #[serde(rename = "O", default)]
                original: String,
                #[serde(rename = "L", default)]
                lowercase: String,
            },
            String(String),
        }
        Ok(match Representation::deserialize(deserializer)? {
            Representation::Object {
                original,
                lowercase,
            } => Self {
                original,
                lowercase,
            },
            Representation::String(original) => Self::new(original),
        })
    }
}

/// The type of a table lock (Go `ast.TableLockType`, a `byte`).
///
/// A value read back from a persisted `TableInfo` may have been written by a
/// different TiDB version, so the raw byte is kept rather than folded into a
/// closed set; see the compatibility warning on `ast.IndexType`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TableLockType(pub u8);

impl TableLockType {
    /// No lock mode was supplied (Go `TableLockNone`).
    pub const NONE: Self = Self(0);
    /// Shared read lock (Go `TableLockRead`).
    pub const READ: Self = Self(1);
    /// Unsupported local read lock (Go `TableLockReadLocal`).
    pub const READ_LOCAL: Self = Self(2);
    /// Persistent read-only table state (Go `TableLockReadOnly`).
    pub const READ_ONLY: Self = Self(3);
    /// Exclusive write lock (Go `TableLockWrite`).
    pub const WRITE: Self = Self(4);
    /// Write lock that permits other readers (Go `TableLockWriteLocal`).
    pub const WRITE_LOCAL: Self = Self(5);

    /// Returns the canonical source spelling, empty for an unnamed value
    /// (Go `TableLockType.String`).
    pub const fn sql(self) -> &'static str {
        match self {
            Self::NONE => "NONE",
            Self::READ => "READ",
            Self::READ_LOCAL => "READ LOCAL",
            Self::READ_ONLY => "READ ONLY",
            Self::WRITE => "WRITE",
            Self::WRITE_LOCAL => "WRITE LOCAL",
            _ => "",
        }
    }
}

/// A view's SQL algorithm characteristic (Go `ast.ViewAlgorithm`, an `int`).
///
/// Persisted in `TableInfo.View`, so an unrecognised value is preserved rather
/// than rejected or collapsed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ViewAlgorithm(pub i64);

impl ViewAlgorithm {
    /// Unspecified algorithm (Go `AlgorithmUndefined`).
    pub const UNDEFINED: Self = Self(0);
    /// Merge the view into its outer query (Go `AlgorithmMerge`).
    pub const MERGE: Self = Self(1);
    /// Materialize the view (Go `AlgorithmTemptable`).
    pub const TEMPTABLE: Self = Self(2);

    /// Returns the canonical source spelling; an unnamed value reads as
    /// `UNDEFINED`, matching Go's `default` arm.
    pub const fn sql(self) -> &'static str {
        match self {
            Self::MERGE => "MERGE",
            Self::TEMPTABLE => "TEMPTABLE",
            _ => "UNDEFINED",
        }
    }
}

/// A view's SQL security characteristic (Go `ast.ViewSecurity`, an `int`).
///
/// Persisted in `TableInfo.View`, so an unrecognised value is preserved.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ViewSecurity(pub i64);

impl ViewSecurity {
    /// Execute with definer privileges (Go `SecurityDefiner`).
    pub const DEFINER: Self = Self(0);
    /// Execute with invoker privileges (Go `SecurityInvoker`).
    pub const INVOKER: Self = Self(1);

    /// Returns the canonical source spelling; an unnamed value reads as
    /// `DEFINER`, matching Go's `default` arm.
    pub const fn sql(self) -> &'static str {
        match self {
            Self::INVOKER => "INVOKER",
            _ => "DEFINER",
        }
    }
}

/// A view's `WITH CHECK OPTION` scope (Go `ast.ViewCheckOption`, an `int`).
///
/// Persisted in `TableInfo.View`, so an unrecognised value is preserved.
/// `Default` is Go's zero value, `LOCAL`; the parser sets `CASCADED`
/// explicitly when the clause omits a scope, as Go's parser does.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ViewCheckOption(pub i64);

impl ViewCheckOption {
    /// Local view conditions only (Go `CheckOptionLocal`).
    pub const LOCAL: Self = Self(0);
    /// Include underlying view conditions (Go `CheckOptionCascaded`).
    pub const CASCADED: Self = Self(1);

    /// Returns the canonical source spelling; an unnamed value reads as
    /// `CASCADED`, matching Go's `default` arm.
    pub const fn sql(self) -> &'static str {
        match self {
            Self::LOCAL => "LOCAL",
            _ => "CASCADED",
        }
    }
}

/// A table partitioning method (Go `ast.PartitionType`, an `int`).
///
/// Persisted in `TableInfo.Partition.Type`. Collapsing an unrecognised value
/// to `NONE` would claim the table is not partitioned while its definitions
/// stay populated, so the raw value is preserved instead.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PartitionType(pub i64);

impl PartitionType {
    /// A temporary single-partition non-partitioned table
    /// (Go `PartitionTypeNone`).
    pub const NONE: Self = Self(0);
    /// Range partitioning (Go `PartitionTypeRange`).
    pub const RANGE: Self = Self(1);
    /// Hash partitioning (Go `PartitionTypeHash`).
    pub const HASH: Self = Self(2);
    /// List partitioning (Go `PartitionTypeList`).
    pub const LIST: Self = Self(3);
    /// Key partitioning (Go `PartitionTypeKey`).
    pub const KEY: Self = Self(4);
    /// MariaDB system-time partitioning (Go `PartitionTypeSystemTime`).
    pub const SYSTEM_TIME: Self = Self(5);

    /// Returns the canonical source spelling, empty for an unnamed value
    /// (Go `PartitionType.String`).
    pub const fn sql(self) -> &'static str {
        match self {
            Self::NONE => "NONE",
            Self::RANGE => "RANGE",
            Self::HASH => "HASH",
            Self::LIST => "LIST",
            Self::KEY => "KEY",
            Self::SYSTEM_TIME => "SYSTEM_TIME",
            _ => "",
        }
    }
}

/// An explicitly requested primary-key storage layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrimaryKeyStorage {
    /// Clustered primary key.
    Clustered,
    /// Non-clustered primary key.
    NonClustered,
}

impl PrimaryKeyStorage {
    /// Returns the canonical source spelling.
    pub const fn sql(self) -> &'static str {
        match self {
            Self::Clustered => "CLUSTERED",
            Self::NonClustered => "NONCLUSTERED",
        }
    }
}

/// The complete source primary-key type, including its zero value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PrimaryKeyType {
    /// No explicit storage choice.
    #[default]
    Default,
    /// Clustered primary key.
    Clustered,
    /// Non-clustered primary key.
    NonClustered,
}

impl PrimaryKeyType {
    /// Returns the source spelling, empty for the default.
    pub const fn sql(self) -> &'static str {
        match self {
            Self::Default => "",
            Self::Clustered => "CLUSTERED",
            Self::NonClustered => "NONCLUSTERED",
        }
    }
}

/// An index method (Go `ast.IndexType`, an `int`).
///
/// Go's declaration carries an explicit warning: the value is also used by
/// TiFlash and "may come from a previous version persisted in TableInfo. So
/// you must keep it compatible when modifying it." An unrecognised value is
/// therefore preserved verbatim across a decode/encode cycle.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IndexType(pub i64);

impl IndexType {
    /// Invalid source zero value (Go `IndexTypeInvalid`).
    pub const INVALID: Self = Self(0);
    /// B-tree index (Go `IndexTypeBtree`).
    pub const BTREE: Self = Self(1);
    /// Hash index (Go `IndexTypeHash`).
    pub const HASH: Self = Self(2);
    /// R-tree index (Go `IndexTypeRtree`).
    pub const RTREE: Self = Self(3);
    /// Hypothetical index (Go `IndexTypeHypo`).
    pub const HYPO: Self = Self(4);
    /// Vector index (Go `IndexTypeVector`).
    pub const VECTOR: Self = Self(5);
    /// Inverted index (Go `IndexTypeInverted`).
    pub const INVERTED: Self = Self(6);
    /// HNSW AST-only vector index (Go `IndexTypeHNSW`).
    pub const HNSW: Self = Self(7);
    /// Full-text index (Go `IndexTypeFulltext`).
    pub const FULLTEXT: Self = Self(8);

    /// Returns the source spelling, empty for the invalid value and for any
    /// unnamed value (Go `IndexType.String`).
    pub const fn sql(self) -> &'static str {
        match self {
            Self::BTREE => "BTREE",
            Self::HASH => "HASH",
            Self::RTREE => "RTREE",
            Self::HYPO => "HYPO",
            Self::VECTOR => "VECTOR",
            Self::INVERTED => "INVERTED",
            Self::HNSW => "HNSW",
            Self::FULLTEXT => "FULLTEXT",
            _ => "",
        }
    }
}

/// A foreign-key referential action.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReferentialAction {
    /// No clause was supplied.
    #[default]
    NoOption,
    /// Restrict the parent mutation.
    Restrict,
    /// Cascade the parent mutation.
    Cascade,
    /// Set child columns to null.
    SetNull,
    /// Perform no action.
    NoAction,
    /// Set child columns to defaults.
    SetDefault,
}

impl ReferentialAction {
    /// Returns the source spelling, empty for no option.
    pub const fn sql(self) -> &'static str {
        match self {
            Self::NoOption => "",
            Self::Restrict => "RESTRICT",
            Self::Cascade => "CASCADE",
            Self::SetNull => "SET NULL",
            Self::NoAction => "NO ACTION",
            Self::SetDefault => "SET DEFAULT",
        }
    }
}

/// A runaway-query action tag.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RunawayActionType {
    /// No action; source string conversion falls back to dry-run.
    #[default]
    None,
    /// Record only.
    DryRun,
    /// Throttle.
    Cooldown,
    /// Terminate.
    Kill,
    /// Switch resource group.
    SwitchGroup,
}

impl RunawayActionType {
    /// Returns the source spelling, including its zero-value fallback.
    pub const fn sql(self) -> &'static str {
        match self {
            Self::None | Self::DryRun => "DRYRUN",
            Self::Cooldown => "COOLDOWN",
            Self::Kill => "KILL",
            Self::SwitchGroup => "SWITCH_GROUP",
        }
    }
}

/// A runaway-query watch tag.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RunawayWatchType {
    /// No watch.
    #[default]
    None,
    /// Exact SQL.
    Exact,
    /// Similar SQL.
    Similar,
    /// Plan digest.
    Plan,
}

impl RunawayWatchType {
    /// Returns the canonical source spelling.
    pub const fn sql(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Exact => "EXACT",
            Self::Similar => "SIMILAR",
            Self::Plan => "PLAN",
        }
    }
}

/// The three resource-group runaway option families.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunawayOptionType {
    /// Threshold rule.
    Rule,
    /// Action.
    Action,
    /// Watch.
    Watch,
}

/// Which columns an analyze statement selects (Go `ast.ColumnChoice`, a
/// `byte`).
///
/// Persisted in `TableInfo.StatsOptions`, so an unrecognised value is
/// preserved rather than aborting the whole table's decode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ColumnChoice(pub u8);

impl ColumnChoice {
    /// Source default (Go `DefaultChoice`).
    pub const DEFAULT: Self = Self(0);
    /// Every column (Go `AllColumns`).
    pub const ALL: Self = Self(1);
    /// Predicate columns (Go `PredicateColumns`).
    pub const PREDICATE: Self = Self(2);
    /// Explicit list (Go `ColumnList`).
    pub const LIST: Self = Self(3);

    /// Returns the canonical source spelling; an unnamed value reads as
    /// `DEFAULT`, matching Go's `default` arm.
    pub const fn sql(self) -> &'static str {
        match self {
            Self::ALL => "ALL",
            Self::PREDICATE => "PREDICATE",
            Self::LIST => "LIST",
            _ => "DEFAULT",
        }
    }
}

/// Raw-value constructors for the persisted catalog enums, used by the
/// `tidb-model` serde adapters to carry an unnamed value through unchanged.
impl From<u8> for TableLockType {
    fn from(value: u8) -> Self {
        Self(value)
    }
}

impl From<i64> for ViewAlgorithm {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl From<i64> for ViewSecurity {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl From<i64> for ViewCheckOption {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl From<i64> for PartitionType {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl From<i64> for IndexType {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl From<u8> for ColumnChoice {
    fn from(value: u8) -> Self {
        Self(value)
    }
}

/// Low resource-group priority value.
pub const LOW_PRIORITY_VALUE: u64 = 1;
/// Medium resource-group priority value.
pub const MEDIUM_PRIORITY_VALUE: u64 = 8;
/// High resource-group priority value.
pub const HIGH_PRIORITY_VALUE: u64 = 16;

/// Converts a priority value to its source name; unknown values are medium.
pub const fn priority_value_to_name(value: u64) -> &'static str {
    match value {
        LOW_PRIORITY_VALUE => "LOW",
        HIGH_PRIORITY_VALUE => "HIGH",
        _ => "MEDIUM",
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for CiString {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            original,
            lowercase,
        } = self;
        let _ = original;
        let _ = lowercase;
        visitor.leave(self)
    }
}

impl crate::Visitable for TableLockType {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self(value) = self;
        let _ = value;
        visitor.leave(self)
    }
}

impl crate::Visitable for ViewAlgorithm {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self(value) = self;
        let _ = value;
        visitor.leave(self)
    }
}

impl crate::Visitable for ViewSecurity {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self(value) = self;
        let _ = value;
        visitor.leave(self)
    }
}

impl crate::Visitable for ViewCheckOption {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self(value) = self;
        let _ = value;
        visitor.leave(self)
    }
}

impl crate::Visitable for PartitionType {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self(value) = self;
        let _ = value;
        visitor.leave(self)
    }
}

impl crate::Visitable for PrimaryKeyStorage {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Clustered => {}
            Self::NonClustered => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for PrimaryKeyType {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Default => {}
            Self::Clustered => {}
            Self::NonClustered => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for IndexType {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self(value) = self;
        let _ = value;
        visitor.leave(self)
    }
}

impl crate::Visitable for ReferentialAction {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::NoOption => {}
            Self::Restrict => {}
            Self::Cascade => {}
            Self::SetNull => {}
            Self::NoAction => {}
            Self::SetDefault => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for RunawayActionType {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::None => {}
            Self::DryRun => {}
            Self::Cooldown => {}
            Self::Kill => {}
            Self::SwitchGroup => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for RunawayWatchType {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::None => {}
            Self::Exact => {}
            Self::Similar => {}
            Self::Plan => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for RunawayOptionType {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Rule => {}
            Self::Action => {}
            Self::Watch => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ColumnChoice {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self(value) = self;
        let _ = value;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    use super::*;

    #[test]
    fn test_t_ci_string_construction_hash_and_display() {
        let mixed = CiString::new("aBC");
        assert_eq!(mixed.original(), "aBC");
        assert_eq!(mixed.lowercase(), "abc");
        assert_eq!(mixed.to_string(), "aBC");
        assert_eq!(mixed, CiString::new("AbC"));
        let hash = |value: &CiString| {
            let mut hasher = DefaultHasher::new();
            value.hash(&mut hasher);
            hasher.finish()
        };
        assert_eq!(hash(&mixed), hash(&CiString::new("ABC")));
    }

    #[test]
    fn test_unmarshal_ci_str() {
        let from_string: CiString = serde_json::from_str(r#""aaBB""#).expect("string form");
        assert_eq!(from_string.original(), "aaBB");
        assert_eq!(from_string.lowercase(), "aabb");
        assert_eq!(
            serde_json::to_string(&from_string).expect("serialize"),
            r#"{"O":"aaBB","L":"aabb"}"#
        );
        let from_object: CiString =
            serde_json::from_str(r#"{"O":"aaBB","L":"aabb"}"#).expect("object form");
        assert_eq!(from_object, from_string);
        assert_eq!(from_object.original(), "aaBB");
    }

    #[test]
    fn every_source_enum_and_priority_spelling_is_exact() {
        assert_eq!(TableLockType::READ_ONLY.sql(), "READ ONLY");
        assert_eq!(ViewAlgorithm::UNDEFINED.sql(), "UNDEFINED");
        assert_eq!(ViewSecurity::INVOKER.sql(), "INVOKER");
        assert_eq!(ViewCheckOption::LOCAL.sql(), "LOCAL");
        assert_eq!(PartitionType::NONE.sql(), "NONE");
        assert_eq!(PrimaryKeyType::Default.sql(), "");
        assert_eq!(IndexType::INVALID.sql(), "");
        assert_eq!(ReferentialAction::NoOption.sql(), "");
        assert_eq!(RunawayActionType::None.sql(), "DRYRUN");
        assert_eq!(RunawayWatchType::None.sql(), "NONE");
        assert_eq!(ColumnChoice::DEFAULT.sql(), "DEFAULT");
        assert_eq!(priority_value_to_name(LOW_PRIORITY_VALUE), "LOW");
        assert_eq!(priority_value_to_name(MEDIUM_PRIORITY_VALUE), "MEDIUM");
        assert_eq!(priority_value_to_name(HIGH_PRIORITY_VALUE), "HIGH");
        assert_eq!(priority_value_to_name(999), "MEDIUM");
    }
}
