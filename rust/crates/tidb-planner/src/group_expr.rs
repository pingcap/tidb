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

//! Memo group-expression identity from `pkg/planner/memo/group_expr.go`.
//!
//! This leaf keeps the source child-count/child-identity/plan-hash fingerprint
//! framing and applied-rule set over explicit byte and integer adapters. The
//! real logical-plan object, Group pointer, and schema/property ownership stay
//! outside the planner rewrite boundary.

use std::collections::HashSet;

use crate::explore_mark::ExploreMark;

/// A memo group expression over a caller-owned logical-plan hash.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GroupExpr {
    plan_hash: Vec<u8>,
    children: Vec<u64>,
    explore_mark: ExploreMark,
    fingerprint: Option<Vec<u8>>,
    applied_rules: HashSet<u64>,
}

impl GroupExpr {
    /// Creates a group expression from the source logical-plan hash bytes.
    #[must_use]
    pub fn new(plan_hash: impl Into<Vec<u8>>) -> Self {
        Self {
            plan_hash: plan_hash.into(),
            ..Self::default()
        }
    }

    /// Returns the caller-supplied logical-plan hash bytes.
    #[must_use]
    pub fn plan_hash(&self) -> &[u8] {
        &self.plan_hash
    }

    /// Returns child identity tokens in source order.
    #[must_use]
    pub fn children(&self) -> &[u64] {
        &self.children
    }

    /// Replaces child identities and invalidates the cached fingerprint.
    pub fn set_children(&mut self, children: impl IntoIterator<Item = u64>) {
        self.children = children.into_iter().collect();
        self.fingerprint = None;
    }

    /// Returns the source-shaped fingerprint bytes.
    #[must_use]
    pub fn fingerprint(&mut self) -> &[u8] {
        if self.fingerprint.is_none() {
            let child_count = u16::try_from(self.children.len()).unwrap_or(u16::MAX);
            let mut bytes = Vec::with_capacity(2 + self.children.len() * 8 + self.plan_hash.len());
            bytes.extend_from_slice(&child_count.to_be_bytes());
            for child in &self.children {
                bytes.extend_from_slice(&child.to_be_bytes());
            }
            bytes.extend_from_slice(&self.plan_hash);
            self.fingerprint = Some(bytes);
        }
        self.fingerprint
            .as_deref()
            .expect("fingerprint was initialized")
    }

    /// Marks one exploration round complete.
    pub fn set_explored(&mut self, round: usize) {
        self.explore_mark.set_explored(round);
    }

    /// Clears one exploration round.
    pub fn set_unexplored(&mut self, round: usize) {
        self.explore_mark.set_unexplored(round);
    }

    /// Reports whether one exploration round is complete.
    #[must_use]
    pub fn explored(&self, round: usize) -> bool {
        self.explore_mark.explored(round)
    }

    /// Adds a source transformation-rule identity.
    pub fn add_applied_rule(&mut self, rule_id: u64) {
        self.applied_rules.insert(rule_id);
    }

    /// Reports whether a transformation-rule identity was already applied.
    #[must_use]
    pub fn has_applied_rule(&self, rule_id: u64) -> bool {
        self.applied_rules.contains(&rule_id)
    }
}
