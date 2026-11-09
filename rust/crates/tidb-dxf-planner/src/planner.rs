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

//! Go `planner.go`.

use tidb_dxf_storage::{StorageError, TaskManager};

use crate::{LogicalPlan, PlanCtx, PlanError};

/// Go `Planner`: a distributed plan planner.
#[derive(Debug, Default)]
pub struct Planner;

impl Planner {
    /// Go `NewPlanner`.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Go `Run`: submits the plan as a task on the plan's session.
    ///
    /// # Errors
    /// The plan does not encode, the context has no session, or the task
    /// cannot be created.
    pub fn run(&self, plan_ctx: &PlanCtx<'_>, plan: &dyn LogicalPlan, _task_mgr: &TaskManager) -> Result<i64, PlanError> {
        let task_meta = plan.to_task_meta()?;

        let target_scope = tidb_dxf_handle::get_target_scope();

        let se = plan_ctx
            .session_ctx
            .ok_or_else(|| StorageError::Other("plan context has no session".to_owned()))?;
        Ok(TaskManager::create_task_with_session(
            &plan_ctx.ctx,
            se,
            &plan_ctx.task_key,
            &plan_ctx.task_type,
            &plan_ctx.keyspace,
            plan_ctx.thread_cnt,
            &target_scope,
            plan_ctx.max_node_cnt,
            &plan.get_task_extra_params(),
            &task_meta,
        )?)
    }
}
