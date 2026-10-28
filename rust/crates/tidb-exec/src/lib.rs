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
// See the License for the specific language governing permissions and
// limitations under the License.
//! The cluster and session subsystems: catalog load and watch, DDL job and
//! metadata plumbing, real-TiKV read/write, privileges, sysvars, `mysql.*`
//! bootstrap, statistics, statement summaries, process info, DAG/coprocessor request
//! building, and the MySQL result-metadata contracts those paths publish.
//!
//! IT IS NOT THE QUERY ENGINE. The live operator tree -- the one every TCP
//! connection and every in-process session executes -- is `tidb-executor`
//! (`Executor` trait, chunk-based, pull-driven; `hash_agg`/`sort`/`limit`/
//! `join`/`window`). `tidb-session` reaches it directly and uses this crate
//! for cluster and session support. `tidb-exec` also uses shared executor
//! storage/scan types; it does not provide a second query engine.
//!
//! SQL aggregation, DISTINCT, sorting and window execution are owned by
//! `tidb-executor`. This crate provides cluster storage and result metadata;
//! it does not keep a second operator or aggregate-state implementation.

pub use tidb_privilege::account_policy;
pub mod adapter;
pub use tidb_cluster::auto_id_client;
mod batch_cop_tasks;
pub use tidb_cluster::catalog_reload;
pub use tidb_cluster::catalog_watch;
pub use tidb_cluster::cluster_account_write;
pub mod bootstrap_publish;
pub mod cluster_analyze;
pub use tidb_cluster::cluster_auto_id;
pub use tidb_cluster::cluster_catalog;
pub use tidb_executor::cluster_config;
pub use tidb_ddl::cluster_ddl;
pub use tidb_executor::cluster_diagnostics;
pub mod cluster_discovery;
use tidb_cluster::cluster_http;
pub mod cluster_index_id;
pub mod cluster_load_stats;
pub use tidb_executor::cluster_peer;
pub mod cluster_predicate_column;
pub use tidb_cluster::cluster_privilege_load;
pub mod cluster_sequence;
pub mod cluster_stats_dump;
pub mod cluster_stats_load;
pub mod cluster_stats_lock;
pub mod cluster_stats_write;
pub use tidb_cluster::cluster_sysvar_load;
pub use tidb_cluster::cluster_sysvar_write;
pub use tidb_cluster::cluster_table_storage;
pub mod cop_client;
pub use tidb_executor::dag_request;
pub use tidb_ddl::ddl_affinity;
pub mod ddl_session_pool;
pub use tidb_ddl::ddl_delete_range;
pub use tidb_ddl::ddl_history_table;
pub use tidb_ddl::ddl_job_scheduler;
pub use tidb_ddl::ddl_job_submit;
pub use tidb_ddl::ddl_job_table;
pub use tidb_ddl::ddl_reorg_table;
pub use tidb_ddl::ddl_systable;
pub use tidb_ddl::foreign_key_build;
pub mod in_memory_meta;
pub use tidb_util::tiflashcompute as tiflash_compute;
pub mod mpp_client;
pub use tidb_ddl::tiflash_replica_manager;
pub use tidb_cluster::configure_deadlock_history;
pub mod distsql_recordset;
pub use tidb_util::execdetails::exec_details;
pub mod explain;
pub mod hint_updatable_vars;
pub use tidb_keydecoder as keydecoder;
pub mod label_delivery;
pub use tidb_cluster::mdl_info_load;
pub mod meta_txn;
pub mod mysql_bootstrap;
pub use tidb_cluster::mysql_system_tables;
pub mod pd_affinity_client;
pub mod pd_approximate_count;
pub use tidb_cluster::pessimistic_lock_error;
pub use tidb_ddl::placement_delivery;
pub mod pre_split;
pub mod real_tikv_analyze;
pub use tidb_cluster::real_tikv_catalog;
pub use tidb_ddl::real_tikv_ddl;
pub mod real_tikv_load_stats;
pub mod real_tikv_privileges;
pub use tidb_cluster::real_tikv_read;
pub mod real_tikv_stats;
pub mod real_tikv_stats_lock;
pub mod recordset_lifecycle;
mod result_metadata;
pub use tidb_util::execdetails::runtime_stats;
pub use tidb_executor::select_result_stats;
pub use tidb_cluster::schema_validator;
pub use tidb_cluster::session_commit_protocol;
pub use tidb_cluster::sql_query_error;
pub use tidb_executor::statement_cop_stats;
pub mod stats_watch;
pub use tidb_cluster::system_row_write;
pub use tidb_cluster::table_from_meta;
pub mod table_info_build;
pub(crate) mod table_write_policy;
pub use tidb_util::execdetails::tiflash_stats;
pub mod tikv_region_store;
pub use tidb_executor::txn_summary;
pub mod warning_publication;
pub use tidb_executor::wide_scan_selection;

pub use result_metadata::{
    col_names_to_result_fields, columns_from_adapted_fields, convert_result_field,
    AdaptedResultField, FieldNameMetadata, IdentifierMetadata, ResultFieldMetadata,
    ResultFieldTypeMetadata, MAX_ALIAS_IDENTIFIER_LEN, NOT_FIXED_DEC, NOT_NULL_FLAG, UNSIGNED_FLAG,
};
pub use warning_publication::{
    warnings_from_json, warnings_to_json, IgnoreWarnings, StatementWarning, StaticWarningHandler,
    WarningAppender, WarningHandler, WarningLevel, WarningPublication, WarningSummary,
};
