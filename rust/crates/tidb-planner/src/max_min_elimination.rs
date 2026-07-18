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

//! Max/min aggregation eligibility from
//! `pkg/planner/core/rule/rule_max_min_eliminate.go`.
//!
//! Before constructing any replacement plan, the Go rule rejects grouped or
//! empty aggregations, non-Max/Min functions, and ENUM/SET arguments whose
//! value ordering differs between aggregation and index scans.  It then takes
//! a single-aggregate fast path or a multi-aggregate path that still requires
//! index checks.  This module preserves that dependency-closed classification
//! over caller-owned metadata; index/ranger checks and replacement-plan
//! construction remain external planner owners.

/// Aggregate function kinds relevant to max/min elimination.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AggregateKind {
    /// `MAX(...)`.
    Max,
    /// `MIN(...)`.
    Min,
    /// Any aggregate other than MAX/MIN.
    Other,
}

/// Value types whose ordering is relevant to the eligibility gate.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ValueType {
    /// A regular value whose index and aggregate ordering agree.
    Ordinary,
    /// ENUM values sort by name in aggregation, so elimination is rejected.
    Enum,
    /// SET values sort by name in aggregation, so elimination is rejected.
    Set,
}

/// Classification of the source replacement branch after eligibility checks.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MaxMinEliminationDecision {
    /// The aggregation must remain unchanged.
    Ineligible,
    /// One Max/Min function can use the single-aggregate path.
    Single,
    /// Multiple Max/Min functions need per-column index checks first.
    MultipleNeedsIndex,
}

/// Caller-owned aggregation metadata for the source eligibility gate.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct MaxMinAggregationShape {
    group_by_items: usize,
    aggregate_kinds: Vec<AggregateKind>,
    used_column_types: Vec<ValueType>,
}

impl MaxMinAggregationShape {
    /// Creates metadata from grouping-item count, aggregate kinds, and used
    /// column value types.
    #[must_use]
    pub fn new(
        group_by_items: usize,
        aggregate_kinds: Vec<AggregateKind>,
        used_column_types: Vec<ValueType>,
    ) -> Self {
        Self {
            group_by_items,
            aggregate_kinds,
            used_column_types,
        }
    }

    /// Returns the source grouping-item count.
    #[must_use]
    pub fn group_by_items(&self) -> usize {
        self.group_by_items
    }

    /// Returns aggregate kinds in source order.
    #[must_use]
    pub fn aggregate_kinds(&self) -> &[AggregateKind] {
        &self.aggregate_kinds
    }

    /// Returns used column value types.
    #[must_use]
    pub fn used_column_types(&self) -> &[ValueType] {
        &self.used_column_types
    }
}

/// Source-shaped max/min elimination classifier.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct MaxMinEliminator;

impl MaxMinEliminator {
    /// Classifies the replacement branch after source eligibility gates.
    #[must_use]
    pub fn classify(self, aggregation: &MaxMinAggregationShape) -> MaxMinEliminationDecision {
        classify_max_min(aggregation)
    }

    /// Returns the source rule registry name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        "max_min_eliminate"
    }
}

/// Applies the source eligibility checks and branch classification.
#[must_use]
pub fn classify_max_min(aggregation: &MaxMinAggregationShape) -> MaxMinEliminationDecision {
    if aggregation.group_by_items != 0 || aggregation.aggregate_kinds.is_empty() {
        return MaxMinEliminationDecision::Ineligible;
    }
    if aggregation
        .aggregate_kinds
        .iter()
        .any(|kind| !matches!(kind, AggregateKind::Max | AggregateKind::Min))
    {
        return MaxMinEliminationDecision::Ineligible;
    }
    if aggregation
        .used_column_types
        .iter()
        .any(|value_type| matches!(value_type, ValueType::Enum | ValueType::Set))
    {
        return MaxMinEliminationDecision::Ineligible;
    }

    if aggregation.aggregate_kinds.len() == 1 {
        MaxMinEliminationDecision::Single
    } else {
        MaxMinEliminationDecision::MultipleNeedsIndex
    }
}
