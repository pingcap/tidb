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

//! Go `scheduler.go`.

use std::error::Error as StdError;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::sync::{Arc, Weak};

use tidb_dxf::{step_2_str, Modification, Step, Task, TaskBase, STEP_DONE, STEP_INIT, STEP_ONE, STEP_TWO, TASK_TYPE_EXAMPLE};
use tidb_dxf_scheduler::{BaseScheduler, CleanUpRoutine, Error, Extension, Param, Scheduler};
use tidb_dxf_storage::TaskHandle;
use tidb_gocontext::Context;
use tidb_log::{Field, Value};

use crate::proto::{SubtaskMeta, TaskMeta};

/// Go `schedulerImpl`.
pub struct SchedulerImpl {
    base: BaseScheduler,
    logger: tidb_logutil::Logger,
    subtask_count: AtomicIsize,
    /// The scheduler itself, which `Init` makes the base's extension (Go's
    /// `s.BaseScheduler.Extension = s`).
    this: Weak<SchedulerImpl>,
}

/// Go `newScheduler`.
#[must_use]
pub fn new_scheduler(ctx: &Context, task: Task, param: Param) -> Arc<SchedulerImpl> {
    let task_id = task.base.id;
    Arc::new_cyclic(|this| SchedulerImpl {
        base: BaseScheduler::new(ctx, task, param),
        logger: tidb_logutil::bg_logger().with_fields(&[Field::new("taskID", Value::I64(task_id))]),
        subtask_count: AtomicIsize::new(0),
        this: this.clone(),
    })
}

/// Go `errors.Annotate(err, message)`.
#[derive(Debug)]
struct Annotated {
    message: &'static str,
    cause: serde_json::Error,
}

impl std::fmt::Display for Annotated {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.message, self.cause)
    }
}

impl StdError for Annotated {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.cause)
    }
}

impl Scheduler for SchedulerImpl {
    fn init(&self) -> Result<(), Error> {
        let task_meta: TaskMeta = serde_json::from_slice(&self.base.get_task().meta)
            .map_err(|cause| Arc::new(Annotated { message: "unmarshal task meta failed", cause }) as Error)?;

        self.subtask_count.store(task_meta.subtask_count, Ordering::SeqCst);

        self.base.set_extension(self.this.clone());
        self.base.init()
    }
    fn schedule_task(&self) {
        self.base.schedule_task();
    }
    fn close(&self) {
        self.base.close();
    }
    fn get_task(&self) -> Arc<Task> {
        self.base.get_task()
    }
}

impl Extension for SchedulerImpl {
    fn on_tick(&self, _: &Context, _: &Task) {
        self.logger.info("OnTick", &[]);
    }

    fn on_next_subtasks_batch(
        &self,
        _: &Context,
        _: &dyn TaskHandle,
        task: &mut Task,
        _: &[String],
        next_step: Step,
    ) -> Result<Vec<Vec<u8>>, Error> {
        if next_step != STEP_ONE && next_step != STEP_TWO {
            panic!("unexpected nextStep: {}", step_2_str(&TASK_TYPE_EXAMPLE, next_step));
        }
        (0..self.subtask_count.load(Ordering::SeqCst))
            .map(|i| {
                serde_json::to_vec(&SubtaskMeta {
                    message: format!("subtask {i} of step {}", step_2_str(&task.base.tp, next_step)),
                })
                .map_err(|err| Arc::new(err) as Error)
            })
            .collect()
    }

    fn on_done(&self, _: &Context, _: &dyn TaskHandle, _: &mut Task) -> Result<(), Error> {
        self.logger.info("OnDone", &[]);
        Ok(())
    }

    fn get_eligible_instances(&self, _: &Context, _: &Task) -> Result<Vec<String>, Error> {
        Ok(Vec::new())
    }

    fn is_retryable_err(&self, _: &(dyn StdError + 'static)) -> bool {
        true
    }

    fn get_next_step(&self, task: &TaskBase) -> Step {
        if task.step == STEP_INIT {
            STEP_ONE
        } else if task.step == STEP_ONE {
            STEP_TWO
        } else {
            // current step must be proto.StepTwo
            STEP_DONE
        }
    }

    fn on_prepare(&self, _: &Context, _: &dyn TaskHandle, _: &mut Task) -> Result<(), Error> {
        Ok(())
    }

    /// Go's `schedulerImpl` has no `ModifyMeta`: the method promoted from
    /// the embedded `Extension`, which is the scheduler itself, would call
    /// itself without end. The example's meta is never modified.
    fn modify_meta(&self, _: &[u8], _: &[Modification]) -> Result<Vec<u8>, Error> {
        Err(Arc::new(std::io::Error::other("the example task's meta cannot be modified")))
    }
}

/// Go `postCleanupImpl`.
pub struct PostCleanupImpl;

impl CleanUpRoutine for PostCleanupImpl {
    fn clean_up(&self, _: &Context, task: &mut Task) -> Result<(), Error> {
        tidb_logutil::bg_logger().info("clean up task", &[Field::new("taskID", Value::I64(task.base.id))]);
        Ok(())
    }
}
