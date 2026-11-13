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

//! Go `pkg/planner/cascades`: the memo-based optimizer `doOptimize` runs
//! when `tidb_enable_cascades_planner` is on (`CascadesOptimize`).
//!
//! The optimizer copies the normalized logical plan into a [`memo::Memo`],
//! explores every group with the rules of [`rule::DEFAULT_RULE_SETS`]
//! (`cascades.go`, `task`), and implements the root group by the cheapest
//! physical task per required property (`impl_and_cost`).

pub mod impl_and_cost;
pub mod memo;
pub mod pattern;
pub mod rule;
pub mod task;
#[cfg(test)]
mod tests;

use tidb_planner_core_base::{PlanError, PlanIdAllocator};
use tidb_planner_core_operator_logicalop::LogicalPlan;

use memo::Memo;
use task::{OptGroupTask, SimpleTaskScheduler, Task};

/// Go `Optimizer` (`cascades.go`).
pub struct Optimizer<'a> {
    /// Go `logic`.
    logic: &'a LogicalPlan,
    /// Go `ctx`.
    ctx: Context<'a>,
}

impl<'a> Optimizer<'a> {
    /// Go `NewOptimizer(lp)`: the memo initialized from `lp`, with the root
    /// group's exploration scheduled.
    ///
    /// # Errors
    ///
    /// As [`Memo::init`].
    pub fn new(lp: &'a LogicalPlan, allocator: &'a PlanIdAllocator) -> Result<Self, PlanError> {
        let mut ctx = Context::new(allocator);
        let ge = ctx.mm.init(lp)?;
        let group = ctx.mm.ge(ge).get_group().expect("the root expression has a group");
        ctx.scheduler.push_task(OptGroupTask::new(group));
        Ok(Self { logic: lp, ctx })
    }

    /// Go `Execute()`.
    ///
    /// # Errors
    ///
    /// The first task's failure.
    pub fn execute(&mut self) -> Result<(), PlanError> {
        task::execute_tasks(&mut self.ctx)
    }

    /// Go `Destroy()`.
    pub fn destroy(&mut self) {
        self.ctx.destroy();
    }

    /// Go `GetMemo()`.
    #[must_use]
    pub const fn get_memo(&self) -> &Memo<'a> {
        &self.ctx.mm
    }

    /// Go `SetRules(ids)`.
    pub fn set_rules(&mut self, ids: &[u32]) {
        for id in ids {
            self.ctx.rule_mask |= 1 << id;
        }
    }

    /// The logical plan the optimizer was built over.
    #[must_use]
    pub const fn logic(&self) -> &'a LogicalPlan {
        self.logic
    }
}

/// Go `Context` (`cascades.go`, `base/cascadesctx`): the memo, the task
/// scheduler and the rule mask one optimization shares.
pub struct Context<'a> {
    /// Go `mm`.
    pub mm: Memo<'a>,
    /// Go `scheduler`.
    pub scheduler: SimpleTaskScheduler,
    /// Go `ruleMask`: every rule enabled.
    pub rule_mask: u64,
    /// The plan id allocator a rule's new operators draw from (Go's
    /// `ReAlloc4Cascades` reads it off the session).
    pub allocator: &'a PlanIdAllocator,
}

impl<'a> Context<'a> {
    /// Go `NewContext(pctx)`.
    #[must_use]
    pub fn new(allocator: &'a PlanIdAllocator) -> Self {
        Self {
            mm: Memo::new(),
            scheduler: SimpleTaskScheduler::new(),
            rule_mask: (1 << rule::Type::XFMaximumRuleLength as u32) - 1,
            allocator,
        }
    }

    /// Go `Destroy()`.
    pub fn destroy(&mut self) {
        self.mm.destroy();
        self.scheduler.destroy();
    }

    /// Go `PushTask(task)`.
    pub fn push_task(&mut self, task: Box<dyn Task>) {
        self.scheduler.push_task(task);
    }

    /// Go `GetMemo()`.
    #[must_use]
    pub const fn get_memo(&self) -> &Memo<'a> {
        &self.mm
    }

    /// Go `GetRuleMask()`.
    #[must_use]
    pub const fn get_rule_mask(&self) -> u64 {
        self.rule_mask
    }
}
