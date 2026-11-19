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

//! Go `pkg/sessionctx/stmtctx`: the statement-scoped state Go keeps below
//! the session variables, which `pkg/sessionctx/variable` imports.

pub mod process_plan;
pub mod used_stats;

pub use process_plan::{BriefPlanSource, PlanSource, ProcessPlanInfo, RunawayRuleKillAction, StoredPlan};

/// Go `stmtctx.ReservedRowIDAlloc`: the row ids a statement reserved as one
/// batch (`AddRecord` with `WithReserveAutoIDHint`), handed out in order.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReservedRowIdAlloc {
    base: i64,
    max: i64,
}

impl ReservedRowIdAlloc {
    /// Go `Reset`: the reserved ids become `(base, max]`.
    pub fn reset(&mut self, base: i64, max: i64) {
        self.base = base;
        self.max = max;
    }

    /// Go `Consume`: the next reserved id, or `None` once exhausted.
    pub fn consume(&mut self) -> Option<i64> {
        if self.base < self.max {
            self.base += 1;
            return Some(self.base);
        }
        None
    }

    /// Go `Current`: the reservation's `(base, max)`.
    #[must_use]
    pub fn current(&self) -> (i64, i64) {
        (self.base, self.max)
    }

    /// Go `Exhausted`.
    #[must_use]
    pub fn exhausted(&self) -> bool {
        self.base >= self.max
    }
}
