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

//! Go `pkg/dxf/example`: an example application of DXF.
//!
//! add-index and import-into have integrated with DXF, but they might be too
//! complex for others to learn how to integrate with DXF, so this simple
//! example shows how to use DXF and how to integrate.
//!
//! The application has 2 steps, `StepOne` and `StepTwo`; each subtask of
//! those steps prints a message. Go's `TestExampleApplication` runs it (in
//! `tidb-session`'s unistore tests, as it needs a store).

mod proto;
mod scheduler;
mod task_executor;

pub use proto::TaskMeta;
pub use scheduler::{new_scheduler, PostCleanupImpl};
pub use task_executor::new_task_executor;
