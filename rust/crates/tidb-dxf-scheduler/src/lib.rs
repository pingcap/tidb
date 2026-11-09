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

mod autoscaler;
mod balancer;
mod interface;
#[cfg(test)]
mod mock;
mod nodes;
mod scheduler;
mod scheduler_manager;
mod slots;
mod state_transform;

pub use autoscaler::{calc_dist_sql_concurrency, get_exec_cpu_node, ResourceCalc};
pub use interface::{
    clear_cleaner_factory, clear_scheduler_factory, register_cleaner_factory, register_scheduler_factory,
    BatchCleaner, Cleaner, CleanerFactoryFn, Error, Extension, Param, SchedulerFactoryFn, TaskManager,
};
pub use nodes::NodeManager;
pub use scheduler_manager::{Manager, CHECK_TASK_RUNNING_INTERVAL, DEFAULT_CLEAN_UP_INTERVAL};
pub use scheduler::{
    get_live_exec_ids, is_cancelled_err, BaseScheduler, Scheduler, ServerInfoSource, CHECK_TASK_FINISHED_INTERVAL, RETRY_SQL_INTERVAL, RETRY_SQL_MAX_INTERVAL,
    RETRY_SQL_TIMES,
};
pub use slots::SlotManager;
pub use state_transform::verify_task_state_transform;
