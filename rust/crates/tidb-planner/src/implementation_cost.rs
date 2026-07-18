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

//! Dependency-closed implementation cost arithmetic from
//! `pkg/planner/implementation/base.go`.
//!
//! The Go implementation also owns physical-plan attachment. This leaf keeps
//! only the source cost state and child-cost arithmetic, represented as
//! caller-supplied scalar costs so it does not invent a physical-plan or memo
//! interface.

/// Source-shaped base implementation cost state.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ImplementationCost {
    cost: f64,
}

impl ImplementationCost {
    /// Creates a base implementation with zero cost.
    #[must_use]
    pub const fn new() -> Self {
        Self { cost: 0.0 }
    }

    /// Replaces the current cost with the ordered sum of child costs.
    pub fn calc_cost(&mut self, child_costs: &[f64]) -> f64 {
        self.cost = child_costs.iter().copied().sum();
        self.cost
    }

    /// Stores an explicit implementation cost.
    pub fn set_cost(&mut self, cost: f64) {
        self.cost = cost;
    }

    /// Returns the current implementation cost.
    #[must_use]
    pub const fn cost(self) -> f64 {
        self.cost
    }

    /// Returns the unchanged cost limit for the base implementation.
    #[must_use]
    pub const fn scale_cost_limit(cost_limit: f64) -> f64 {
        cost_limit
    }

    /// Removes ordered child costs from a parent cost limit.
    #[must_use]
    pub fn get_cost_limit(cost_limit: f64, child_costs: &[f64]) -> f64 {
        cost_limit - child_costs.iter().copied().sum::<f64>()
    }
}
