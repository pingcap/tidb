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

//! Column-pruning schema invariant from
//! `pkg/planner/core/rule/rule_column_pruning.go`.
//!
//! The Go checker walks concrete logical plans and compares schema pointers.
//! This leaf keeps the dependency-closed tree invariant over normalized schema
//! width plus explicit pointer-reuse/TableDual markers; real expressions,
//! schema columns, logical-plan mutation, and optimizer execution remain
//! external boundaries.

/// A normalized logical-plan schema node for the post-pruning invariant.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SchemaNode {
    column_count: usize,
    schema_reused_from_first_child: bool,
    is_table_dual: bool,
    children: Vec<Self>,
}

impl SchemaNode {
    /// Creates a leaf with a non-empty or empty schema.
    #[must_use]
    pub const fn leaf(column_count: usize, is_table_dual: bool) -> Self {
        Self {
            column_count,
            schema_reused_from_first_child: false,
            is_table_dual,
            children: Vec::new(),
        }
    }

    /// Creates a node with ordered children and source pointer-reuse marker.
    #[must_use]
    pub fn node(
        column_count: usize,
        schema_reused_from_first_child: bool,
        is_table_dual: bool,
        children: Vec<Self>,
    ) -> Self {
        Self {
            column_count,
            schema_reused_from_first_child,
            is_table_dual,
            children,
        }
    }

    /// Returns whether the source post-pruning schema invariant holds.
    #[must_use]
    pub fn no_unexpected_zero_column_schema(&self) -> bool {
        if self
            .children
            .iter()
            .any(|child| !child.no_unexpected_zero_column_schema())
        {
            return false;
        }
        if self.column_count != 0 {
            return true;
        }
        (self.schema_reused_from_first_child && !self.children.is_empty()) || self.is_table_dual
    }
}

/// Checks the source invariant for a normalized plan tree.
#[must_use]
pub fn no_unexpected_zero_column_schema(plan: &SchemaNode) -> bool {
    plan.no_unexpected_zero_column_schema()
}

#[cfg(test)]
mod tests {
    use super::{no_unexpected_zero_column_schema, SchemaNode};

    #[test]
    fn non_empty_schema_is_valid() {
        assert!(no_unexpected_zero_column_schema(&SchemaNode::leaf(
            1, false
        )));
    }

    #[test]
    fn zero_schema_is_valid_when_reused_from_first_child() {
        let child = SchemaNode::leaf(1, false);
        let node = SchemaNode::node(0, true, false, vec![child]);
        assert!(no_unexpected_zero_column_schema(&node));
    }

    #[test]
    fn table_dual_may_legitimately_have_zero_columns() {
        assert!(no_unexpected_zero_column_schema(&SchemaNode::leaf(0, true)));
    }

    #[test]
    fn unexpected_zero_schema_and_empty_reuse_are_invalid() {
        assert!(!no_unexpected_zero_column_schema(&SchemaNode::leaf(
            0, false
        )));
        let node = SchemaNode::node(0, true, false, Vec::new());
        assert!(!no_unexpected_zero_column_schema(&node));
    }

    #[test]
    fn invalid_child_propagates_to_parent() {
        let invalid_child = SchemaNode::leaf(0, false);
        let parent = SchemaNode::node(1, false, false, vec![invalid_child]);
        assert!(!no_unexpected_zero_column_schema(&parent));
    }
}
