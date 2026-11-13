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

//! Go `pkg/planner/cascades/task` (and the `base` task contracts): the
//! exploration tasks and the stack scheduler that runs them.

use tidb_planner_core_base::PlanError;

use super::memo::{GeId, GroupId};
use super::pattern::get_operand;
use super::rule::{Binder, ListRules, Rule, DEFAULT_RULE_SETS};
use super::Context;

/// Go `base.Task`.
pub trait Task {
    /// Go `Execute()`.
    ///
    /// # Errors
    ///
    /// A rule's or the memo's failure.
    fn execute(&self, ctx: &mut Context<'_>) -> Result<(), PlanError>;

    /// Go `Desc(w)`.
    fn desc(&self, ctx: &Context<'_>, w: &mut String);
}

/// Go `task.Stack`.
#[derive(Default)]
pub struct Stack {
    tasks: Vec<Box<dyn Task>>,
}

impl Stack {
    /// Go `newTaskStackWithCap(c)`.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            tasks: Vec::with_capacity(capacity),
        }
    }

    /// Go `Push(one)`.
    pub fn push(&mut self, one: Box<dyn Task>) {
        self.tasks.push(one);
    }

    /// Go `Pop()`.
    pub fn pop(&mut self) -> Option<Box<dyn Task>> {
        self.tasks.pop()
    }

    /// Go `Empty()`.
    #[must_use]
    pub fn empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Go `Len()`.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Go `Destroy()`.
    pub fn destroy(&mut self) {
        self.tasks.clear();
    }

    /// Go `Desc(w)`: each task, bottom to top, one per line.
    pub fn desc(&self, ctx: &Context<'_>, w: &mut String) {
        for one in &self.tasks {
            one.desc(ctx, w);
            w.push('\n');
        }
    }
}

/// Go `SimpleTaskScheduler`: pops and runs tasks until the stack is empty.
#[derive(Default)]
pub struct SimpleTaskScheduler {
    pub(super) stack: Stack,
}

impl SimpleTaskScheduler {
    /// Go `NewSimpleTaskScheduler()`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            stack: Stack::with_capacity(4),
        }
    }

    /// Go `PushTask(task)`.
    pub fn push_task(&mut self, task: Box<dyn Task>) {
        self.stack.push(task);
    }

    /// Go `Destroy()`.
    pub fn destroy(&mut self) {
        self.stack.destroy();
    }
}

/// Go `SimpleTaskScheduler.ExecuteTasks()` over the context's scheduler.
///
/// # Errors
///
/// The first task's failure.
pub fn execute_tasks(ctx: &mut Context<'_>) -> Result<(), PlanError> {
    while let Some(task) = ctx.scheduler.stack.pop() {
        task.execute(ctx)?;
    }
    Ok(())
}

/// Go `OptGroupTask`: explores every expression of a group once.
#[derive(Debug)]
pub struct OptGroupTask {
    group: GroupId,
}

impl OptGroupTask {
    /// Go `NewOptGroupTask(ctx, g)`.
    #[must_use]
    pub fn new(group: GroupId) -> Box<dyn Task> {
        Box::new(Self { group })
    }
}

impl Task for OptGroupTask {
    fn execute(&self, ctx: &mut Context<'_>) -> Result<(), PlanError> {
        if ctx.mm.group(self.group).is_explored() {
            return Ok(());
        }
        let ges = ctx.mm.group(self.group).get_logical_expressions().to_vec();
        for ge in ges {
            ctx.push_task(OptGroupExpressionTask::new(ge));
        }
        ctx.mm.group_explored(self.group);
        Ok(())
    }

    fn desc(&self, ctx: &Context<'_>, w: &mut String) {
        w.push_str("OptGroupTask{group:");
        ctx.mm.group(self.group).string(w);
        w.push('}');
    }
}

/// Go `OptGroupExpressionTask`: applies the expression's rules, then
/// explores its input groups.
#[derive(Debug)]
pub struct OptGroupExpressionTask {
    group_expression: GeId,
}

impl OptGroupExpressionTask {
    /// Go `NewOptGroupExpressionTask(ctx, ge)`.
    #[must_use]
    pub fn new(group_expression: GeId) -> Box<dyn Task> {
        Box::new(Self { group_expression })
    }

    /// Go `getValidRules()`: the operand's rule set, filtered by the
    /// expression and the context's rule mask.
    fn get_valid_rules(&self, ctx: &Context<'_>) -> ListRules {
        let ge = ctx.mm.ge(self.group_expression);
        match DEFAULT_RULE_SETS.get(&get_operand(ge.get_wrapped_logical_plan())) {
            Some(operand_rules) => operand_rules.filter(ge).filter(ctx.rule_mask),
            None => ListRules::default(),
        }
    }
}

impl Task for OptGroupExpressionTask {
    fn execute(&self, ctx: &mut Context<'_>) -> Result<(), PlanError> {
        let list_rules = self.get_valid_rules(ctx);
        for one in list_rules.0 {
            ctx.push_task(ApplyRuleTask::new(self.group_expression, one));
        }
        let inputs = ctx.mm.ge(self.group_expression).inputs.clone();
        for input in inputs.into_iter().rev() {
            ctx.push_task(OptGroupTask::new(input));
        }
        Ok(())
    }

    fn desc(&self, ctx: &Context<'_>, w: &mut String) {
        w.push_str("OptGroupExpressionTask{ge:");
        ctx.mm.ge(self.group_expression).string(w);
        w.push('}');
    }
}

/// Go `ApplyRuleTask`: applies one rule to every match below one
/// expression.
pub struct ApplyRuleTask {
    ge: GeId,
    rule: &'static dyn Rule,
}

impl ApplyRuleTask {
    /// Go `NewApplyRuleTask(ctx, gE, r)`.
    #[must_use]
    pub fn new(ge: GeId, rule: &'static dyn Rule) -> Box<dyn Task> {
        Box::new(Self { ge, rule })
    }
}

impl Task for ApplyRuleTask {
    fn execute(&self, ctx: &mut Context<'_>) -> Result<(), PlanError> {
        if ctx.mm.ge(self.ge).is_explored(self.rule.id()) || ctx.mm.ge(self.ge).is_abandoned() {
            return Ok(());
        }
        let pa = self.rule.pattern();
        let mut binder = Binder::new(pa, self.ge);
        while let Some(holder) = binder.next(&ctx.mm).cloned() {
            if !self.rule.pre_check(&holder, &ctx.mm) {
                continue;
            }
            let (new_exprs, remove) = self.rule.xform(&holder, &ctx.mm, ctx.allocator)?;
            for ne in new_exprs {
                let group = ctx.mm.ge(self.ge).get_group();
                let new_group_expr = ctx.mm.copy_in(group, ne)?;
                ctx.push_task(OptGroupExpressionTask::new(new_group_expr));
            }
            if remove {
                if let Some(group) = ctx.mm.ge(self.ge).get_group() {
                    ctx.mm.remove_out(group, self.ge);
                }
            }
        }
        ctx.mm.ge_mut(self.ge).set_explored(self.rule.id());
        Ok(())
    }

    fn desc(&self, ctx: &Context<'_>, w: &mut String) {
        w.push_str("ApplyRuleTask{gE:");
        ctx.mm.ge(self.ge).string(w);
        w.push_str(", rule:");
        self.rule.string(w);
        w.push('}');
    }
}
