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

//! Go master `pkg/mvservice`: the in-memory scheduler and executor of
//! materialized view refresh, MV log purge and MV log analyze tasks. The
//! scheduler is this crate; the [`Helper`] that runs each task's SQL on a
//! system session is the session layer's.

mod backpressure;
mod consistenthash;
mod executor;
mod server_maintainer;
mod service;
mod service_settings;
mod task_handler;
mod time_proxy;
mod utils;

#[cfg(test)]
mod utils_test;
#[cfg(test)]
mod test_helpers_test;
#[cfg(test)]
mod executor_test;
#[cfg(test)]
mod server_maintainer_test;
#[cfg(test)]
mod mvservice_test;

pub use backpressure::{
    CpuMemBackpressureController, TaskBackpressureController, DEFAULT_BACKPRESSURE_CPU_THRESHOLD,
    DEFAULT_BACKPRESSURE_MEM_THRESHOLD, DEFAULT_TASK_BACKPRESSURE_DELAY,
};
pub use consistenthash::ConsistentHash;
pub use executor::{TaskExecutor, TaskFn};
pub use server_maintainer::{int64_key_to_binary_bytes, AvailableKey, ServerConsistentHash, ServerDiscovery, ServerInfo};
pub use service::{
    classify_refresh_alert_level, snapshot_task_executor_metrics, Helper, MVMetricsReporter, MVService, MViewTask,
    MViewTaskState, MlogPurgeTask, RefreshAlertTask, ServiceMetrics, TaskBackpressureConfig, TaskExecutorMetricsSnapshot,
    DEFAULT_MV_PURGE_TASK_TIMEOUT, DEFAULT_MV_REFRESH_TASK_TIMEOUT,
};
pub use service_settings::{
    calc_retry_delay, default_mv_service_config, derive_history_gc_interval, new_mv_service, normalize_mv_service_config,
    split_task_concurrency, Config,
};
pub use task_handler::{
    err_mv_task_canceled_manually, is_mv_task_canceled_manually, MVTaskHandler, MlogAccumulationTask, MlogAnalyzeTask,
    MvsError, SessionPool, ERR_MV_LOG_PURGE_HANDLER_NOT_REGISTERED, ERR_MV_REFRESH_HANDLER_NOT_REGISTERED,
    ERR_MV_TASK_CANCELED_MANUALLY,
};
pub use time_proxy::{
    install_mock_time_module_for_test, mvs_now, mvs_since, mvs_sleep, mvs_unix, MockTimeGuard, MockTimeModule,
};
pub use utils::{Item, Lessable, Notifier, PriorityQueue};
