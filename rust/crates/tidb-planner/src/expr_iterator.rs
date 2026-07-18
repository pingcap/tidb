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

//! Source-shaped cascades expression iteration from
//! `pkg/planner/memo/expr_iterator.go`.
//!
//! The Go implementation walks intrusive list elements and memo groups. This
//! leaf keeps the matching and cartesian child-iteration semantics over owned
//! vectors, which makes the boundary deterministic without inventing a memo
//! allocator or logical-plan representation.

use crate::pattern::{Operand, Pattern};
use crate::pattern_engine::EngineType;

/// A source-shaped logical group expression.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupExpression {
    /// Operand classification of the logical expression.
    pub operand: Operand,
    /// Child groups referenced by this expression.
    pub children: Vec<Group>,
}

impl GroupExpression {
    /// Creates an expression with no child groups.
    #[must_use]
    pub const fn new(operand: Operand) -> Self {
        Self {
            operand,
            children: Vec::new(),
        }
    }

    /// Appends a child group in source order.
    pub fn with_child(mut self, child: Group) -> Self {
        self.children.push(child);
        self
    }
}

/// A group containing equivalent expressions and an execution engine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Group {
    /// Engine classification applied to every expression in this group.
    pub engine: EngineType,
    /// Equivalent expressions in source insertion order.
    pub equivalents: Vec<GroupExpression>,
}

impl Group {
    /// Creates an empty group for an engine.
    #[must_use]
    pub const fn new(engine: EngineType) -> Self {
        Self {
            engine,
            equivalents: Vec::new(),
        }
    }

    /// Inserts one equivalent expression.
    pub fn insert(&mut self, expression: GroupExpression) {
        self.equivalents.push(expression);
    }
}

/// A matched expression tree returned by [`ExprIter`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MatchedExpression {
    /// Operand selected by the pattern.
    pub operand: Operand,
    /// Engine of the matched group.
    pub engine: EngineType,
    /// Recursively matched child expressions.
    pub children: Vec<Self>,
}

/// A deterministic iterator over all expression trees matching a pattern.
#[derive(Clone, Debug)]
pub struct ExprIter {
    matches: Vec<MatchedExpression>,
    index: usize,
}

impl ExprIter {
    /// Returns whether the current position is matched.
    #[must_use]
    pub const fn matched(&self) -> bool {
        self.index < self.matches.len()
    }

    /// Returns the current matched expression, if any.
    #[must_use]
    pub fn current(&self) -> Option<&MatchedExpression> {
        self.matches.get(self.index)
    }

    /// Advances to the next match and reports whether one exists.
    pub fn advance(&mut self) -> bool {
        if self.matched() {
            self.index += 1;
        }
        self.matched()
    }

    /// Resets to the first match and reports whether one exists.
    pub fn reset(&mut self) -> bool {
        self.index = 0;
        self.matched()
    }

    /// Returns the number of available matches.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.matches.len()
    }

    /// Returns whether no matches are available.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.matches.is_empty()
    }
}

/// Builds an iterator over all equivalent expressions in a group.
#[must_use]
pub fn new_expr_iter_from_group(group: &Group, pattern: &Pattern) -> Option<ExprIter> {
    let matches = matching_group(group, pattern);
    if matches.is_empty() {
        None
    } else {
        Some(ExprIter { matches, index: 0 })
    }
}

/// Builds an iterator from one source-equivalent expression index.
#[must_use]
pub fn new_expr_iter_from_group_elem(
    group: &Group,
    expression_index: usize,
    pattern: &Pattern,
) -> Option<ExprIter> {
    let expression = group.equivalents.get(expression_index)?;
    let matches = matching_expression(group.engine, expression, pattern);
    (!matches.is_empty()).then_some(ExprIter { matches, index: 0 })
}

fn matching_group(group: &Group, pattern: &Pattern) -> Vec<MatchedExpression> {
    if pattern.matches_operand_any(group.engine) {
        return vec![MatchedExpression {
            operand: Operand::Any,
            engine: group.engine,
            children: Vec::new(),
        }];
    }

    group
        .equivalents
        .iter()
        .flat_map(|expression| matching_expression(group.engine, expression, pattern))
        .collect()
}

fn matching_expression(
    engine: EngineType,
    expression: &GroupExpression,
    pattern: &Pattern,
) -> Vec<MatchedExpression> {
    if !pattern.matches(expression.operand, engine) {
        return Vec::new();
    }
    if !pattern.children.is_empty() && pattern.children.len() != expression.children.len() {
        return Vec::new();
    }

    let child_matches = pattern
        .children
        .iter()
        .zip(&expression.children)
        .map(|(child_pattern, child_group)| matching_group(child_group, child_pattern))
        .collect::<Vec<_>>();

    let mut combinations = vec![Vec::new()];
    for choices in child_matches {
        if choices.is_empty() {
            return Vec::new();
        }
        combinations = combinations
            .into_iter()
            .flat_map(|prefix| {
                choices.iter().cloned().map(move |choice| {
                    let mut combined = prefix.clone();
                    combined.push(choice);
                    combined
                })
            })
            .collect();
    }

    combinations
        .into_iter()
        .map(|children| MatchedExpression {
            operand: expression.operand,
            engine,
            children,
        })
        .collect()
}
