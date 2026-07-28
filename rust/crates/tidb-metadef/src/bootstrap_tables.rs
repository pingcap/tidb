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

//! `pkg/session`'s `systemTablesOfBaseNextGenVersion`: every `mysql.*` table a
//! fresh bootstrap creates, with the reserved ID it is created under and the
//! `CREATE TABLE` statement that defines it.
//!
//! The ID matters as much as the statement. These tables are NOT allocated out
//! of the ordinary global ID space; each has a fixed ID in the reserved range
//! above [`crate::MAX_USER_GLOBAL_ID`], so a bootstrap is reproducible and a
//! user table can never collide with one.
//!
//! The list is Go's own order, which is the order the IDs descend in and the
//! order bootstrap creates them.

use crate::system::*;
use crate::system_tables_def::*;

/// One `mysql.*` table a fresh bootstrap creates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BootstrapTable {
    /// The fixed reserved table ID.
    pub id: i64,
    /// The table name, without the `mysql.` qualifier.
    pub name: &'static str,
    /// The `CREATE TABLE` statement, verbatim from Go.
    pub create_sql: &'static str,
}

/// Go `systemTablesOfBaseNextGenVersion`.
pub const BOOTSTRAP_TABLES: &[BootstrapTable] = &[
    BootstrapTable {
        id: USER_TABLE_ID,
        name: "user",
        create_sql: CREATE_USER_TABLE,
    },
    BootstrapTable {
        id: PASSWORD_HISTORY_TABLE_ID,
        name: "password_history",
        create_sql: CREATE_PASSWORD_HISTORY_TABLE,
    },
    BootstrapTable {
        id: GLOBAL_PRIV_TABLE_ID,
        name: "global_priv",
        create_sql: CREATE_GLOBAL_PRIV_TABLE,
    },
    BootstrapTable {
        id: DBTABLE_ID,
        name: "db",
        create_sql: CREATE_DBTABLE,
    },
    BootstrapTable {
        id: TABLES_PRIV_TABLE_ID,
        name: "tables_priv",
        create_sql: CREATE_TABLES_PRIV_TABLE,
    },
    BootstrapTable {
        id: COLUMNS_PRIV_TABLE_ID,
        name: "columns_priv",
        create_sql: CREATE_COLUMNS_PRIV_TABLE,
    },
    BootstrapTable {
        id: GLOBAL_VARIABLES_TABLE_ID,
        name: "global_variables",
        create_sql: CREATE_GLOBAL_VARIABLES_TABLE,
    },
    BootstrapTable {
        id: TI_DBTABLE_ID,
        name: "tidb",
        create_sql: CREATE_TI_DBTABLE,
    },
    BootstrapTable {
        id: HELP_TOPIC_TABLE_ID,
        name: "help_topic",
        create_sql: CREATE_HELP_TOPIC_TABLE,
    },
    BootstrapTable {
        id: STATS_META_TABLE_ID,
        name: "stats_meta",
        create_sql: CREATE_STATS_META_TABLE,
    },
    BootstrapTable {
        id: STATS_HISTOGRAMS_TABLE_ID,
        name: "stats_histograms",
        create_sql: CREATE_STATS_HISTOGRAMS_TABLE,
    },
    BootstrapTable {
        id: STATS_BUCKETS_TABLE_ID,
        name: "stats_buckets",
        create_sql: CREATE_STATS_BUCKETS_TABLE,
    },
    BootstrapTable {
        id: GCDELETE_RANGE_TABLE_ID,
        name: "gc_delete_range",
        create_sql: CREATE_GCDELETE_RANGE_TABLE,
    },
    BootstrapTable {
        id: GCDELETE_RANGE_DONE_TABLE_ID,
        name: "gc_delete_range_done",
        create_sql: CREATE_GCDELETE_RANGE_DONE_TABLE,
    },
    BootstrapTable {
        id: STATS_FEEDBACK_TABLE_ID,
        name: "stats_feedback",
        create_sql: CREATE_STATS_FEEDBACK_TABLE,
    },
    BootstrapTable {
        id: ROLE_EDGES_TABLE_ID,
        name: "role_edges",
        create_sql: CREATE_ROLE_EDGES_TABLE,
    },
    BootstrapTable {
        id: DEFAULT_ROLES_TABLE_ID,
        name: "default_roles",
        create_sql: CREATE_DEFAULT_ROLES_TABLE,
    },
    BootstrapTable {
        id: BIND_INFO_TABLE_ID,
        name: "bind_info",
        create_sql: CREATE_BIND_INFO_TABLE,
    },
    BootstrapTable {
        id: STATS_TOP_NTABLE_ID,
        name: "stats_top_n",
        create_sql: CREATE_STATS_TOP_NTABLE,
    },
    BootstrapTable {
        id: EXPR_PUSHDOWN_BLACKLIST_TABLE_ID,
        name: "expr_pushdown_blacklist",
        create_sql: CREATE_EXPR_PUSHDOWN_BLACKLIST_TABLE,
    },
    BootstrapTable {
        id: OPT_RULE_BLACKLIST_TABLE_ID,
        name: "opt_rule_blacklist",
        create_sql: CREATE_OPT_RULE_BLACKLIST_TABLE,
    },
    BootstrapTable {
        id: STATS_EXTENDED_TABLE_ID,
        name: "stats_extended",
        create_sql: CREATE_STATS_EXTENDED_TABLE,
    },
    BootstrapTable {
        id: STATS_FMSKETCH_TABLE_ID,
        name: "stats_fm_sketch",
        create_sql: CREATE_STATS_FMSKETCH_TABLE,
    },
    BootstrapTable {
        id: GLOBAL_GRANTS_TABLE_ID,
        name: "global_grants",
        create_sql: CREATE_GLOBAL_GRANTS_TABLE,
    },
    BootstrapTable {
        id: CAPTURE_PLAN_BASELINES_BLACKLIST_TABLE_ID,
        name: "capture_plan_baselines_blacklist",
        create_sql: CREATE_CAPTURE_PLAN_BASELINES_BLACKLIST_TABLE,
    },
    BootstrapTable {
        id: COLUMN_STATS_USAGE_TABLE_ID,
        name: "column_stats_usage",
        create_sql: CREATE_COLUMN_STATS_USAGE_TABLE,
    },
    BootstrapTable {
        id: TABLE_CACHE_META_TABLE_ID,
        name: "table_cache_meta",
        create_sql: CREATE_TABLE_CACHE_META_TABLE,
    },
    BootstrapTable {
        id: ANALYZE_OPTIONS_TABLE_ID,
        name: "analyze_options",
        create_sql: CREATE_ANALYZE_OPTIONS_TABLE,
    },
    BootstrapTable {
        id: STATS_HISTORY_TABLE_ID,
        name: "stats_history",
        create_sql: CREATE_STATS_HISTORY_TABLE,
    },
    BootstrapTable {
        id: STATS_META_HISTORY_TABLE_ID,
        name: "stats_meta_history",
        create_sql: CREATE_STATS_META_HISTORY_TABLE,
    },
    BootstrapTable {
        id: ANALYZE_JOBS_TABLE_ID,
        name: "analyze_jobs",
        create_sql: CREATE_ANALYZE_JOBS_TABLE,
    },
    BootstrapTable {
        id: ADVISORY_LOCKS_TABLE_ID,
        name: "advisory_locks",
        create_sql: CREATE_ADVISORY_LOCKS_TABLE,
    },
    BootstrapTable {
        id: PLAN_REPLAYER_STATUS_TABLE_ID,
        name: "plan_replayer_status",
        create_sql: CREATE_PLAN_REPLAYER_STATUS_TABLE,
    },
    BootstrapTable {
        id: PLAN_REPLAYER_TASK_TABLE_ID,
        name: "plan_replayer_task",
        create_sql: CREATE_PLAN_REPLAYER_TASK_TABLE,
    },
    BootstrapTable {
        id: STATS_TABLE_LOCKED_TABLE_ID,
        name: "stats_table_locked",
        create_sql: CREATE_STATS_TABLE_LOCKED_TABLE,
    },
    BootstrapTable {
        id: TI_DBTTLTABLE_STATUS_TABLE_ID,
        name: "tidb_ttl_table_status",
        create_sql: CREATE_TI_DBTTLTABLE_STATUS_TABLE,
    },
    BootstrapTable {
        id: TI_DBTTLTASK_TABLE_ID,
        name: "tidb_ttl_task",
        create_sql: CREATE_TI_DBTTLTASK_TABLE,
    },
    BootstrapTable {
        id: TI_DBTTLJOB_HISTORY_TABLE_ID,
        name: "tidb_ttl_job_history",
        create_sql: CREATE_TI_DBTTLJOB_HISTORY_TABLE,
    },
    BootstrapTable {
        id: TI_DBGLOBAL_TASK_TABLE_ID,
        name: "tidb_global_task",
        create_sql: CREATE_TI_DBGLOBAL_TASK_TABLE,
    },
    BootstrapTable {
        id: TI_DBGLOBAL_TASK_HISTORY_TABLE_ID,
        name: "tidb_global_task_history",
        create_sql: CREATE_TI_DBGLOBAL_TASK_HISTORY_TABLE,
    },
    BootstrapTable {
        id: TI_DBIMPORT_JOBS_TABLE_ID,
        name: "tidb_import_jobs",
        create_sql: CREATE_TI_DBIMPORT_JOBS_TABLE,
    },
    BootstrapTable {
        id: TI_DBRUNAWAY_WATCH_TABLE_ID,
        name: "tidb_runaway_watch",
        create_sql: CREATE_TI_DBRUNAWAY_WATCH_TABLE,
    },
    BootstrapTable {
        id: TI_DBRUNAWAY_QUERIES_TABLE_ID,
        name: "tidb_runaway_queries",
        create_sql: CREATE_TI_DBRUNAWAY_QUERIES_TABLE,
    },
    BootstrapTable {
        id: TI_DBTIMERS_TABLE_ID,
        name: "tidb_timers",
        create_sql: CREATE_TI_DBTIMERS_TABLE,
    },
    BootstrapTable {
        id: TI_DBRUNAWAY_WATCH_DONE_TABLE_ID,
        name: "tidb_runaway_watch_done",
        create_sql: CREATE_TI_DBRUNAWAY_WATCH_DONE_TABLE,
    },
    BootstrapTable {
        id: DIST_FRAMEWORK_META_TABLE_ID,
        name: "dist_framework_meta",
        create_sql: CREATE_DIST_FRAMEWORK_META_TABLE,
    },
    BootstrapTable {
        id: REQUEST_UNIT_BY_GROUP_TABLE_ID,
        name: "request_unit_by_group",
        create_sql: CREATE_REQUEST_UNIT_BY_GROUP_TABLE,
    },
    BootstrapTable {
        id: TI_DBPITRIDMAP_TABLE_ID,
        name: "tidb_pitr_id_map",
        create_sql: CREATE_TI_DBPITRIDMAP_TABLE,
    },
    BootstrapTable {
        id: TI_DBRESTORE_REGISTRY_TABLE_ID,
        name: "tidb_restore_registry",
        create_sql: CREATE_TI_DBRESTORE_REGISTRY_TABLE,
    },
    BootstrapTable {
        id: INDEX_ADVISOR_RESULTS_TABLE_ID,
        name: "index_advisor_results",
        create_sql: CREATE_INDEX_ADVISOR_RESULTS_TABLE,
    },
    BootstrapTable {
        id: TI_DBKERNEL_OPTIONS_TABLE_ID,
        name: "tidb_kernel_options",
        create_sql: CREATE_TI_DBKERNEL_OPTIONS_TABLE,
    },
    BootstrapTable {
        id: TI_DBWORKLOAD_VALUES_TABLE_ID,
        name: "tidb_workload_values",
        create_sql: CREATE_TI_DBWORKLOAD_VALUES_TABLE,
    },
];
