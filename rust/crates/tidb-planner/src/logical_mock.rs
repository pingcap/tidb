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

//! Test-only logical mock data source constructor from
//! `pkg/planner/core/operator/logicalop/logical_mock.go`.
//!
//! Go's `MockDataSource.Init` replaces the embedded BaseLogicalPlan with a
//! plan type of `mockDS` and query-block offset zero, retaining the caller's
//! PlanContext. This leaf keeps that constructor contract through an opaque
//! context token; BaseLogicalPlan IDs, task maps, schema, and physical mock
//! planning remain external planner boundaries.

/// Opaque identity for the source PlanContext retained by initialization.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PlanContextToken(u64);

impl PlanContextToken {
    /// Creates a context token for dependency-closed tests.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the opaque context identity.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

/// Dependency-closed subset of the embedded Go BaseLogicalPlan metadata.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct BaseLogicalPlanIdentity {
    plan_type: &'static str,
    query_block_offset: i32,
    context: PlanContextToken,
}

impl BaseLogicalPlanIdentity {
    /// Returns the source plan type label.
    #[must_use]
    pub const fn plan_type(self) -> &'static str {
        self.plan_type
    }

    /// Returns the source query-block offset.
    #[must_use]
    pub const fn query_block_offset(self) -> i32 {
        self.query_block_offset
    }

    /// Returns the retained context token.
    #[must_use]
    pub const fn context(self) -> PlanContextToken {
        self.context
    }
}

/// Test-only logical mock data source.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct MockDataSource {
    base: Option<BaseLogicalPlanIdentity>,
}

impl Default for MockDataSource {
    fn default() -> Self {
        Self { base: None }
    }
}

impl MockDataSource {
    /// Initializes the mock data source with Go's `mockDS`/offset-zero base.
    #[must_use]
    pub fn init(self, context: PlanContextToken) -> Self {
        Self {
            base: Some(BaseLogicalPlanIdentity {
                plan_type: "mockDS",
                query_block_offset: 0,
                context,
            }),
        }
    }

    /// Returns initialized base metadata, or `None` for the Go zero value.
    #[must_use]
    pub const fn base(&self) -> Option<BaseLogicalPlanIdentity> {
        self.base
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_preserves_context_and_source_metadata() {
        let context = PlanContextToken::new(42);
        let data_source = MockDataSource::default().init(context);
        let base = data_source.base().expect("initialized mock data source");
        assert_eq!(base.plan_type(), "mockDS");
        assert_eq!(base.query_block_offset(), 0);
        assert_eq!(base.context(), context);
        assert_eq!(base.context().raw(), 42);
    }

    #[test]
    fn test_init_replaces_existing_base_metadata() {
        let first = MockDataSource::default().init(PlanContextToken::new(1));
        let second = first.init(PlanContextToken::new(2));
        assert_eq!(
            second.base(),
            Some(BaseLogicalPlanIdentity {
                plan_type: "mockDS",
                query_block_offset: 0,
                context: PlanContextToken::new(2),
            })
        );
    }

    #[test]
    fn test_zero_value_has_no_initialized_base() {
        assert!(MockDataSource::default().base().is_none());
    }
}
