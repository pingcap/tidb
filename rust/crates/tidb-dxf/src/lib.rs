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

//! Distributed execution value types from Go `pkg/dxf/framework/proto` and
//! scheduler status from Go `pkg/dxf/framework/schstatus`.

/// Declares one of Go's `string`-kinded named types.
///
/// Go writes `type TaskType string` and then both package-level `const`
/// values and arbitrary runtime conversions such as `TaskType("123")`. A
/// newtype over `Cow<'static, str>` keeps both: `from_static` is `const`, so
/// the Go constants stay constants, while `new` accepts any owned string.
macro_rules! go_string_type {
    ($(#[$attr:meta])* $name:ident) => {
        $(#[$attr])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        #[derive(serde::Serialize, serde::Deserialize)]
        #[serde(transparent)]
        pub struct $name(::std::borrow::Cow<'static, str>);

        impl $name {
            /// Wraps a static string, as Go's package-level constants do.
            #[must_use]
            pub(crate) const fn from_static(s: &'static str) -> Self {
                Self(::std::borrow::Cow::Borrowed(s))
            }

            /// Wraps a runtime string, as Go's `Type(s)` conversion does.
            #[must_use]
            pub fn new(s: impl Into<String>) -> Self {
                Self(::std::borrow::Cow::Owned(s.into()))
            }

        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

mod modify;
mod node;
/// Scheduler status and resource-tuning values from Go
/// `pkg/dxf/framework/schstatus`.
pub mod schstatus;
mod step;
mod subtask;
mod task;
mod task_type;

pub use modify::{
    Modification, ModificationType, ModifyParam, MODIFY_BATCH_SIZE, MODIFY_MAX_NODE_COUNT,
    MODIFY_MAX_WRITE_SPEED, MODIFY_REQUIRED_SLOTS,
};
pub use node::{ManagedNode, NodeResource, NODE_RESOURCE_FOR_TEST};
pub use step::{
    is_valid_business_step, is_valid_step, step2str, Step, BACKFILL_STEP_MERGE_SORT,
    BACKFILL_STEP_MERGE_TEMP_INDEX, BACKFILL_STEP_READ_INDEX, BACKFILL_STEP_WRITE_AND_INGEST,
    IMPORT_STEP_COLLECT_CONFLICTS, IMPORT_STEP_CONFLICT_RESOLUTION, IMPORT_STEP_ENCODE_AND_SORT,
    IMPORT_STEP_IMPORT, IMPORT_STEP_MERGE_SORT, IMPORT_STEP_POST_PROCESS,
    IMPORT_STEP_WRITE_AND_INGEST, STEP_DONE, STEP_INIT, STEP_ONE, STEP_PREPARED, STEP_THREE,
    STEP_TWO,
};
pub use subtask::{
    Allocatable, StepResource, Subtask, SubtaskBase, SubtaskState, SUBTASK_STATE_CANCELED,
    SUBTASK_STATE_FAILED, SUBTASK_STATE_PAUSED, SUBTASK_STATE_PENDING, SUBTASK_STATE_RUNNING,
    SUBTASK_STATE_SUCCEED,
};
pub use task::{
    get_max_concurrent_task, set_max_concurrent_task, set_max_concurrent_task_for_test,
    ExtraParams, MaxConcurrentTaskError, PrepareMode, Task, TaskBase, TaskState, TaskType,
    DEFAULT_MAX_CONCURRENT_TASK, EMPTY_META, MAX_CONCURRENT_TASK_UPPER_BOUND, NORMAL_PRIORITY,
    PREPARE_MODE_DISABLED, PREPARE_MODE_REQUIRED, TASK_ID_LABEL_NAME,
    TASK_STATE_AWAITING_RESOLUTION, TASK_STATE_CANCELLING, TASK_STATE_FAILED, TASK_STATE_MODIFYING,
    TASK_STATE_PAUSED, TASK_STATE_PAUSING, TASK_STATE_PENDING, TASK_STATE_RESUMING,
    TASK_STATE_REVERTED, TASK_STATE_REVERTING, TASK_STATE_RUNNING, TASK_STATE_SUCCEED,
};
pub use task_type::{int2type, type2int, BACKFILL, IMPORT_INTO, TASK_TYPE_EXAMPLE};
