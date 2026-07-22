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

/// The type of a table lock.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TableLockType {
    /// No lock mode was supplied.
    #[default]
    None,
    /// Shared read lock.
    Read,
    /// Unsupported local read lock.
    ReadLocal,
    /// Persistent read-only table state.
    ReadOnly,
    /// Exclusive write lock.
    Write,
    /// Write lock that permits other readers.
    WriteLocal,
}

impl TableLockType {
    /// Returns the canonical source spelling.
    pub const fn sql(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Read => "READ",
            Self::ReadLocal => "READ LOCAL",
            Self::ReadOnly => "READ ONLY",
            Self::Write => "WRITE",
            Self::WriteLocal => "WRITE LOCAL",
        }
    }
}

/// A view's SQL algorithm characteristic.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ViewAlgorithm {
    /// Unspecified algorithm.
    #[default]
    Undefined,
    /// Merge the view into its outer query.
    Merge,
    /// Materialize the view.
    Temptable,
}

impl ViewAlgorithm {
    /// Returns the canonical source spelling.
    pub const fn sql(self) -> &'static str {
        match self {
            Self::Undefined => "UNDEFINED",
            Self::Merge => "MERGE",
            Self::Temptable => "TEMPTABLE",
        }
    }
}

/// A view's SQL security characteristic.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ViewSecurity {
    /// Execute with definer privileges.
    #[default]
    Definer,
    /// Execute with invoker privileges.
    Invoker,
}

impl ViewSecurity {
    /// Returns the canonical source spelling.
    pub const fn sql(self) -> &'static str {
        match self {
            Self::Definer => "DEFINER",
            Self::Invoker => "INVOKER",
        }
    }
}

/// A view's `WITH CHECK OPTION` scope.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ViewCheckOption {
    /// Local view conditions only.
    Local,
    /// Include underlying view conditions.
    #[default]
    Cascaded,
}

impl ViewCheckOption {
    /// Returns the canonical source spelling.
    pub const fn sql(self) -> &'static str {
        match self {
            Self::Local => "LOCAL",
            Self::Cascaded => "CASCADED",
        }
    }
}

/// A table partitioning method.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PartitionType {
    /// A temporary single-partition non-partitioned table.
    #[default]
    None,
    /// Range partitioning.
    Range,
    /// Hash partitioning.
    Hash,
    /// List partitioning.
    List,
    /// Key partitioning.
    Key,
    /// MariaDB system-time partitioning.
    SystemTime,
}

impl PartitionType {
    /// Returns the canonical source spelling.
    pub const fn sql(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Range => "RANGE",
            Self::Hash => "HASH",
            Self::List => "LIST",
            Self::Key => "KEY",
            Self::SystemTime => "SYSTEM_TIME",
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

/// An index method.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IndexType {
    /// Invalid source zero value.
    #[default]
    Invalid,
    /// B-tree index.
    Btree,
    /// Hash index.
    Hash,
    /// R-tree index.
    Rtree,
    /// Hypothetical index.
    Hypo,
    /// Vector index.
    Vector,
    /// Inverted index.
    Inverted,
    /// HNSW AST-only vector index.
    Hnsw,
    /// Full-text index.
    Fulltext,
}

impl IndexType {
    /// Returns the source spelling, empty for the invalid value.
    pub const fn sql(self) -> &'static str {
        match self {
            Self::Invalid => "",
            Self::Btree => "BTREE",
            Self::Hash => "HASH",
            Self::Rtree => "RTREE",
            Self::Hypo => "HYPO",
            Self::Vector => "VECTOR",
            Self::Inverted => "INVERTED",
            Self::Hnsw => "HNSW",
            Self::Fulltext => "FULLTEXT",
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

/// Which columns an analyze statement selects.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ColumnChoice {
    /// Source default.
    #[default]
    Default,
    /// Every column.
    All,
    /// Predicate columns.
    Predicate,
    /// Explicit list.
    List,
}

impl ColumnChoice {
    /// Returns the canonical source spelling.
    pub const fn sql(self) -> &'static str {
        match self {
            Self::Default => "DEFAULT",
            Self::All => "ALL",
            Self::Predicate => "PREDICATE",
            Self::List => "LIST",
        }
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
        match self {
            Self::None => {}
            Self::Read => {}
            Self::ReadLocal => {}
            Self::ReadOnly => {}
            Self::Write => {}
            Self::WriteLocal => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ViewAlgorithm {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Undefined => {}
            Self::Merge => {}
            Self::Temptable => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ViewSecurity {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Definer => {}
            Self::Invoker => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ViewCheckOption {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Local => {}
            Self::Cascaded => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for PartitionType {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::None => {}
            Self::Range => {}
            Self::Hash => {}
            Self::List => {}
            Self::Key => {}
            Self::SystemTime => {}
        }
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
        match self {
            Self::Invalid => {}
            Self::Btree => {}
            Self::Hash => {}
            Self::Rtree => {}
            Self::Hypo => {}
            Self::Vector => {}
            Self::Inverted => {}
            Self::Hnsw => {}
            Self::Fulltext => {}
        }
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
        match self {
            Self::Default => {}
            Self::All => {}
            Self::Predicate => {}
            Self::List => {}
        }
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
        assert_eq!(TableLockType::ReadOnly.sql(), "READ ONLY");
        assert_eq!(ViewAlgorithm::Undefined.sql(), "UNDEFINED");
        assert_eq!(ViewSecurity::Invoker.sql(), "INVOKER");
        assert_eq!(ViewCheckOption::Local.sql(), "LOCAL");
        assert_eq!(PartitionType::None.sql(), "NONE");
        assert_eq!(PrimaryKeyType::Default.sql(), "");
        assert_eq!(IndexType::Invalid.sql(), "");
        assert_eq!(ReferentialAction::NoOption.sql(), "");
        assert_eq!(RunawayActionType::None.sql(), "DRYRUN");
        assert_eq!(RunawayWatchType::None.sql(), "NONE");
        assert_eq!(ColumnChoice::Default.sql(), "DEFAULT");
        assert_eq!(priority_value_to_name(LOW_PRIORITY_VALUE), "LOW");
        assert_eq!(priority_value_to_name(MEDIUM_PRIORITY_VALUE), "MEDIUM");
        assert_eq!(priority_value_to_name(HIGH_PRIORITY_VALUE), "HIGH");
        assert_eq!(priority_value_to_name(999), "MEDIUM");
    }
}
