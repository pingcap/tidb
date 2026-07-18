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

//! LogicalMaxOneRow identity from
//! `pkg/planner/core/operator/logicalop/logical_max_one_row.go` and its
//! generated Hash64/Equals implementation.
//!
//! The source operator has no attributes of its own. Its generated identity
//! hashes the `MaxOneRow` plan tag and the embedded BaseLogicalPlan ID, and
//! equality compares that same ID. This leaf preserves that identity over a
//! normalized plan ID; context, children, schema/statistics, predicate
//! behavior, and runtime row limiting remain explicit external boundaries.

use crate::hash_equaler::{new_hash_equaler, Hasher};

/// Minimal LogicalMaxOneRow identity.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct LogicalMaxOneRowIdentity {
    plan_id: i64,
}

impl LogicalMaxOneRowIdentity {
    /// Creates an identity from the source BaseLogicalPlan ID.
    #[must_use]
    pub const fn new(plan_id: i64) -> Self {
        Self { plan_id }
    }

    /// Returns the source plan ID used by BaseLogicalPlan Hash64/Equals.
    #[must_use]
    pub const fn plan_id(self) -> i64 {
        self.plan_id
    }

    /// Computes generated Hash64 in source field order.
    #[must_use]
    pub fn hash64(self) -> u64 {
        let mut hasher = new_hash_equaler();
        hasher.hash_string("MaxOneRow");
        hasher.hash_int(self.plan_id);
        hasher.sum64()
    }

    /// Compares generated Hash64/Equals identity fields.
    #[must_use]
    pub const fn equals(self, other: Self) -> bool {
        self.plan_id == other.plan_id
    }
}
