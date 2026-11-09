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

//! Go `pkg/dxf/framework/scheduler`: on the owner node, the manager runs one
//! scheduler per task through its states, reserves the cluster's slots for
//! it, places its subtasks on the managed nodes and balances them.

mod interface;
#[cfg(test)]
mod mock;
mod nodes;
mod scheduler;
mod slots;
mod state_transform;

pub use interface::TaskManager;
pub use nodes::NodeManager;
pub use scheduler::{
    get_live_exec_ids, ServerInfoSource, CHECK_TASK_FINISHED_INTERVAL, RETRY_SQL_INTERVAL, RETRY_SQL_MAX_INTERVAL,
    RETRY_SQL_TIMES,
};
pub use slots::SlotManager;
pub use state_transform::verify_task_state_transform;
