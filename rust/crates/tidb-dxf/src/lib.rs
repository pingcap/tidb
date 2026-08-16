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

//! Go `pkg/dxf/framework/proto` lands as a complete package: the value layer
//! of the Distributed eXecution Framework — task and subtask records, their
//! state machines, the step vocabulary of every task type, and the node
//! resource accounting that turns slots into CPU and memory budgets.
//!
//! The Go package has zero internal TiDB imports; so does this crate. Its two
//! external Go imports are `docker/go-units` (byte formatting and the `GB`
//! constant) and the standard library.
//!
//! File mapping (one Rust module per Go file):
//! - [`task`] <- `task.go`
//! - [`subtask`] <- `subtask.go`
//! - [`step`] <- `step.go`
//! - [`node`] <- `node.go`
//! - [`modify`] <- `modify.go`
//! - [`task_type`] <- `type.go` (Rust reserves `type` as a keyword, so the
//!   module carries the name of what the Go file actually declares: the
//!   `TaskType` constants and their integer encoding)
//!
//! Narrowings, each also named at its own definition site:
//! - Go's four `string`-kinded named types ([`TaskState`], [`TaskType`],
//!   [`SubtaskState`], [`ModificationType`]) become newtypes over
//!   `Cow<'static, str>`. Go can write `TaskType("123")` for an arbitrary
//!   string and still have package-level `const` values; the `Cow` keeps both,
//!   with `from_static` for the constants and `new` for parsed values.
//! - Go's `time.Time` zero value marks "not started yet". Rust uses
//!   `Option<DateTime<Utc>>`, where `None` is that zero. Ordering is
//!   preserved: `None` sorts before every `Some`, exactly as Go's year-1 zero
//!   time sorts before every real timestamp, so [`TaskBase::compare`] is
//!   unchanged. [`TaskBase::to_string`] renders `None` as Go's
//!   `0001-01-01T00:00:00Z`.
//! - Go's `Task.Error error` becomes `Option<String>`. This package only
//!   carries the value between the task table and the scheduler; it never
//!   inspects, wraps, or matches on it, and the storage column is a plain
//!   `BLOB`.
//! - `units.BytesSize` is hand-rolled in [`step_resource_bytes_size`]: no
//!   byte-formatting crate is present in this offline workspace, and the
//!   function is 12 lines of `%.4g` over the binary abbreviations. The
//!   `units.GB` constant used by [`NODE_RESOURCE_FOR_TEST`] is go-units'
//!   decimal `10^9`, spelled out in [`node::GB`].
//! - Go's `SetMaxConcurrentTaskForTest` returns a `func()` restore closure
//!   invoked through `defer`. Rust returns the RAII guard
//!   [`MaxConcurrentTaskGuard`], which restores on drop — the same lifetime,
//!   expressed the way Rust spells `defer`.
//! - Go's `init()` seeding of the `maxConcurrentTask` global becomes the
//!   static's initializer; there is no separate initialization phase to
//!   observe.

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
            pub const fn from_static(s: &'static str) -> Self {
                Self(::std::borrow::Cow::Borrowed(s))
            }

            /// Wraps a runtime string, as Go's `Type(s)` conversion does.
            #[must_use]
            pub fn new(s: impl Into<String>) -> Self {
                Self(::std::borrow::Cow::Owned(s.into()))
            }

            /// Go's `String()` method: the underlying string, unchanged.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl PartialEq<str> for $name {
            fn eq(&self, other: &str) -> bool {
                self.0 == other
            }
        }
    };
}

mod modify;
mod node;
mod step;
mod subtask;
mod task;
mod task_type;

pub use modify::{
    Modification, ModificationType, ModifyParam, MODIFY_BATCH_SIZE, MODIFY_MAX_NODE_COUNT,
    MODIFY_MAX_WRITE_SPEED, MODIFY_REQUIRED_SLOTS,
};
pub use node::{step_resource_bytes_size, ManagedNode, NodeResource, GB, NODE_RESOURCE_FOR_TEST};
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
    ExtraParams, MaxConcurrentTaskError, MaxConcurrentTaskGuard, PrepareMode, Task, TaskBase,
    TaskState, TaskType, DEFAULT_MAX_CONCURRENT_TASK, EMPTY_META, MAX_CONCURRENT_TASK_UPPER_BOUND,
    NORMAL_PRIORITY, PREPARE_MODE_DISABLED, PREPARE_MODE_REQUIRED, TASK_ID_LABEL_NAME,
    TASK_STATE_AWAITING_RESOLUTION, TASK_STATE_CANCELLING, TASK_STATE_FAILED, TASK_STATE_MODIFYING,
    TASK_STATE_PAUSED, TASK_STATE_PAUSING, TASK_STATE_PENDING, TASK_STATE_RESUMING,
    TASK_STATE_REVERTED, TASK_STATE_REVERTING, TASK_STATE_RUNNING, TASK_STATE_SUCCEED,
};
pub use task_type::{int2type, type2int, BACKFILL, IMPORT_INTO, TASK_TYPE_EXAMPLE};
