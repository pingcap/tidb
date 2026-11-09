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

//! Go `plan.go`.

use std::collections::HashMap;
use std::sync::Arc;

use tidb_dxf::{ExtraParams, Step, TaskType};
use tidb_gocontext::Context;

use crate::PlanError;

/// Go `PlanCtx`: the context of planning.
#[derive(Clone, Default)]
pub struct PlanCtx<'a> {
    /// Go `Ctx`.
    pub ctx: Context,

    // integrate with current distribute framework
    /// Go `SessionCtx`.
    pub session_ctx: Option<&'a dyn tidb_dxf_storage::SessionContext>,
    /// Go `TaskID`.
    pub task_id: i64,
    /// Go `TaskKey`.
    pub task_key: String,
    /// Go `TaskType`.
    pub task_type: TaskType,
    /// Go `ThreadCnt`.
    pub thread_cnt: isize,
    /// Go `MaxNodeCnt`.
    pub max_node_cnt: isize,
    /// Go `Keyspace`.
    pub keyspace: String,

    /// Go `PreviousSubtaskMetas`: the subtask metas of previous steps. We can
    /// remove this field if we find a better way to pass the result between
    /// steps.
    pub previous_subtask_metas: HashMap<Step, Vec<Vec<u8>>>,
    /// Go `GlobalSort`.
    pub global_sort: bool,
    /// Go `NextTaskStep`.
    pub next_task_step: Step,
    /// Go `ExecuteNodesCnt`.
    pub execute_nodes_cnt: isize,

    /// Go `Store`.
    pub store: Option<Arc<dyn tidb_txnkv::kv_storage::Storage>>,
}

/// Go `LogicalPlan`: a logical plan of the framework. The framework's flow
/// is logical plan -> task meta -> physical plan -> subtask metas ->
/// pipelines.
pub trait LogicalPlan {
    /// Go `GetTaskExtraParams`.
    fn get_task_extra_params(&self) -> ExtraParams;
    /// Go `ToTaskMeta`.
    ///
    /// # Errors
    /// The plan does not encode.
    fn to_task_meta(&self) -> Result<Vec<u8>, PlanError>;
    /// Go `FromTaskMeta`.
    ///
    /// # Errors
    /// The meta does not decode.
    fn from_task_meta(&mut self, meta: &[u8]) -> Result<(), PlanError>;
    /// Go `ToPhysicalPlan`.
    ///
    /// # Errors
    /// The plan cannot be built.
    fn to_physical_plan(&self, ctx: &PlanCtx<'_>) -> Result<PhysicalPlan, PlanError>;
}

/// Go `PhysicalPlan`: a DAG of processors. Each processor is a node that
/// runs the task with a pipeline, and receives from or passes results to
/// other processors through its input and output links.
#[derive(Default)]
pub struct PhysicalPlan {
    /// Go `Processors`.
    pub processors: Vec<ProcessorSpec>,
}

impl PhysicalPlan {
    /// Go `AddProcessor`: adds a node to the DAG.
    pub fn add_processor(&mut self, processor: ProcessorSpec) {
        self.processors.push(processor);
    }

    /// Go `ToSubtaskMetas`: the subtask metas of the step's processors.
    ///
    /// # Errors
    /// A pipeline does not encode its meta.
    pub fn to_subtask_metas(&self, ctx: &PlanCtx<'_>, step: Step) -> Result<Vec<Vec<u8>>, PlanError> {
        let mut subtask_metas = Vec::with_capacity(self.processors.len());
        for processor in &self.processors {
            if processor.step != step {
                continue;
            }
            subtask_metas.push(processor.pipeline.to_subtask_meta(ctx)?);
        }
        Ok(subtask_metas)
    }
}

/// Go `ProcessorSpec`: a node of the DAG, with input links from other
/// processors, output links to others, and the pipeline that is its logic.
pub struct ProcessorSpec {
    /// Go `ID`.
    pub id: isize,
    /// Go `Input`.
    pub input: InputSpec,
    /// Go `Pipeline`.
    pub pipeline: Box<dyn PipelineSpec>,
    /// Go `Output`.
    pub output: OutputSpec,
    /// Go `Step`. We can remove this field if we find a better way to pass
    /// the result between steps.
    pub step: Step,
}

/// Go `InputSpec`.
#[derive(Debug, Clone, Default)]
pub struct InputSpec {
    /// Go `ColumnTypes`.
    pub column_types: Vec<u8>,
    /// Go `Links`.
    pub links: Vec<LinkSpec>,
}

/// Go `OutputSpec`.
#[derive(Debug, Clone, Default)]
pub struct OutputSpec {
    /// Go `Links`.
    pub links: Vec<LinkSpec>,
}

/// Go `LinkSpec`: connects pipelines on different nodes.
#[derive(Debug, Clone, Default)]
pub struct LinkSpec {
    /// Go `ProcessorID`.
    pub processor_id: isize,
}

/// Go `PipelineSpec`.
pub trait PipelineSpec {
    /// Go `ToSubtaskMeta`: the pipeline as a subtask meta.
    ///
    /// # Errors
    /// The meta does not encode.
    fn to_subtask_meta(&self, ctx: &PlanCtx<'_>) -> Result<Vec<u8>, PlanError>;
}

#[cfg(test)]
mod tests {
    use super::{PhysicalPlan, PipelineSpec, PlanCtx, ProcessorSpec};
    use crate::PlanError;

    /// Go `mock.MockPipelineSpec` returning `[]byte("mock")`.
    struct MockPipelineSpec;

    impl PipelineSpec for MockPipelineSpec {
        fn to_subtask_meta(&self, _: &PlanCtx<'_>) -> Result<Vec<u8>, PlanError> {
            Ok(b"mock".to_vec())
        }
    }

    /// Go `TestPhysicalPlan`.
    #[test]
    fn test_physical_plan() {
        let mut plan = PhysicalPlan::default();
        let plan_ctx = PlanCtx::default();
        plan.add_processor(ProcessorSpec {
            id: 0,
            input: Default::default(),
            pipeline: Box::new(MockPipelineSpec),
            output: Default::default(),
            step: tidb_dxf::Step(1),
        });
        let subtask_metas = plan.to_subtask_metas(&plan_ctx, tidb_dxf::Step(1)).unwrap();
        assert_eq!(subtask_metas, vec![b"mock".to_vec()]);
    }
}
