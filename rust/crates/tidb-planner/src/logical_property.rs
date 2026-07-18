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

//! Logical-property value shape from `pkg/planner/property/logical_property.go`.
//!
//! Go's LogicalProperty is the shared memo value containing optional
//! statistics, schema, functional dependencies, max-one-row state, possible
//! ordering properties, and a TiFlash capability bit. This leaf preserves the
//! zero-value constructor and nil-versus-present metadata through opaque
//! caller-owned adapters; expression schemas, statistics, FD derivation, and
//! memo/group lifecycle remain external planner boundaries.

/// Opaque statistics identity supplied by a planner owner.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct StatsIdentity(u64);

impl StatsIdentity {
    /// Creates a statistics identity token.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }
}

/// Opaque expression-schema identity supplied by a planner owner.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SchemaIdentity(u64);

impl SchemaIdentity {
    /// Creates a schema identity token.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }
}

/// Opaque functional-dependency-set identity supplied by a planner owner.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FdIdentity(u64);

impl FdIdentity {
    /// Creates an FD-set identity token.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }
}

/// Opaque expression-column identity in one possible ordering.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ColumnIdentity(u64);

impl ColumnIdentity {
    /// Creates a column identity token.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }
}

/// Logical properties shared by equivalent memo expressions.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct LogicalProperty {
    stats: Option<StatsIdentity>,
    schema: Option<SchemaIdentity>,
    fd: Option<FdIdentity>,
    max_one_row: bool,
    possible_props: Option<Vec<Vec<ColumnIdentity>>>,
    has_tiflash: bool,
}

impl LogicalProperty {
    /// Creates the source zero-value logical property.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            stats: None,
            schema: None,
            fd: None,
            max_one_row: false,
            possible_props: None,
            has_tiflash: false,
        }
    }

    /// Sets optional statistics metadata.
    #[must_use]
    pub const fn with_stats(mut self, stats: Option<StatsIdentity>) -> Self {
        self.stats = stats;
        self
    }

    /// Sets optional expression-schema metadata.
    #[must_use]
    pub const fn with_schema(mut self, schema: Option<SchemaIdentity>) -> Self {
        self.schema = schema;
        self
    }

    /// Sets optional functional-dependency metadata.
    #[must_use]
    pub const fn with_fd(mut self, fd: Option<FdIdentity>) -> Self {
        self.fd = fd;
        self
    }

    /// Sets the max-one-row property.
    #[must_use]
    pub const fn with_max_one_row(mut self, max_one_row: bool) -> Self {
        self.max_one_row = max_one_row;
        self
    }

    /// Sets nil-versus-present possible ordering properties.
    #[must_use]
    pub fn with_possible_props(mut self, possible_props: Option<Vec<Vec<ColumnIdentity>>>) -> Self {
        self.possible_props = possible_props;
        self
    }

    /// Sets the source TiFlash capability bit.
    #[must_use]
    pub const fn with_has_tiflash(mut self, has_tiflash: bool) -> Self {
        self.has_tiflash = has_tiflash;
        self
    }

    /// Returns optional statistics metadata.
    #[must_use]
    pub const fn stats(&self) -> Option<StatsIdentity> {
        self.stats
    }

    /// Returns optional expression-schema metadata.
    #[must_use]
    pub const fn schema(&self) -> Option<SchemaIdentity> {
        self.schema
    }

    /// Returns optional functional-dependency metadata.
    #[must_use]
    pub const fn fd(&self) -> Option<FdIdentity> {
        self.fd
    }

    /// Returns whether this property is known to emit at most one row.
    #[must_use]
    pub const fn max_one_row(&self) -> bool {
        self.max_one_row
    }

    /// Returns nil-versus-present possible ordering properties.
    #[must_use]
    pub fn possible_props(&self) -> Option<&[Vec<ColumnIdentity>]> {
        self.possible_props.as_deref()
    }

    /// Returns the source TiFlash capability bit.
    #[must_use]
    pub const fn has_tiflash(&self) -> bool {
        self.has_tiflash
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_logical_property_matches_go_zero_value() {
        let property = LogicalProperty::new();
        assert_eq!(property, LogicalProperty::default());
        assert_eq!(property.stats(), None);
        assert_eq!(property.schema(), None);
        assert_eq!(property.fd(), None);
        assert!(!property.max_one_row());
        assert_eq!(property.possible_props(), None);
        assert!(!property.has_tiflash());
    }

    #[test]
    fn test_logical_property_retains_optional_metadata() {
        let property = LogicalProperty::new()
            .with_stats(Some(StatsIdentity::new(1)))
            .with_schema(Some(SchemaIdentity::new(2)))
            .with_fd(Some(FdIdentity::new(3)))
            .with_max_one_row(true)
            .with_possible_props(Some(vec![vec![
                ColumnIdentity::new(4),
                ColumnIdentity::new(5),
            ]]))
            .with_has_tiflash(true);
        assert_eq!(property.stats(), Some(StatsIdentity::new(1)));
        assert_eq!(property.schema(), Some(SchemaIdentity::new(2)));
        assert_eq!(property.fd(), Some(FdIdentity::new(3)));
        assert!(property.max_one_row());
        assert_eq!(
            property.possible_props(),
            Some(&[vec![ColumnIdentity::new(4), ColumnIdentity::new(5)]][..])
        );
        assert!(property.has_tiflash());
    }

    #[test]
    fn test_possible_properties_nil_and_empty_are_distinct() {
        let nil = LogicalProperty::new();
        let empty = LogicalProperty::new().with_possible_props(Some(Vec::new()));
        assert_eq!(nil.possible_props(), None);
        assert_eq!(empty.possible_props(), Some(&[][..]));
        assert_ne!(nil, empty);
    }
}
