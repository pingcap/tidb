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

//! Go `pkg/statistics/handle/util`.

mod auto_analyze_proc_id_generator;
mod lease_getter;
mod pool;
mod table_info;
mod util;

pub use auto_analyze_proc_id_generator::{
    AutoAnalyzeProcIdGenerator, AutoAnalyzeProcessList, AutoAnalyzeTracker, Generator,
    GLOBAL_AUTO_ANALYZE_PROCESS_LIST,
};
pub use lease_getter::{LeaseGetter, StatsLease};
pub use pool::{Pool, StatsPool, StatsWorkerPool};
pub use table_info::{InfoSchema, TableInfoGetter, TableItem};
pub use util::{
    call_with_sctx, duration_to_ts, exec, exec_rows, exec_rows_with_ctx, exec_with_ctx,
    exec_with_opts, get_current_prune_mode, get_start_ts, is_special_global_index,
    update_sctx_vars_for_stats, wrap_txn, StatsSessionContext, FLAG_WRAP_TXN, STATS_CONTEXT,
    STATS_META_HISTORY_SOURCE_ANALYZE, STATS_META_HISTORY_SOURCE_EXTENDED_STATS,
    STATS_META_HISTORY_SOURCE_FLUSH_STATS, STATS_META_HISTORY_SOURCE_LOAD_STATS,
    STATS_META_HISTORY_SOURCE_SCHEMA_CHANGE, USE_CURRENT_SESSION_OPT,
};
