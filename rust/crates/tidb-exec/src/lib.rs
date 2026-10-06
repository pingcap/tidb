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
//! bootstrap, statistics, slow log, process info, DAG/coprocessor request
//! building, and the MySQL result-metadata contracts those paths publish.
//!
//! IT IS NOT THE QUERY ENGINE. The live operator tree -- the one every TCP
//! connection and every in-process session executes -- is `tidb-executor`
//! (`Executor` trait, chunk-based, pull-driven; `hash_agg`/`sort`/`limit`/
//! `join`/`window`). `tidb-session` reaches it directly and does not depend on
//! this crate at all. The edge runs `tidb-exec` -> `tidb-executor`, and only
//! for storage/scan seam types (`cluster_storage`, `remote_scan`,
//! `tikv_scan_spec`, `StorageError`) -- no operator ever crosses.
//!
//! SQL aggregation, DISTINCT, sorting and window execution are owned by
//! `tidb-executor`. This crate provides cluster storage and result metadata;
//! it does not keep a second operator or aggregate-state implementation.

pub mod account_policy;
pub mod adapter;
pub mod auto_pre_split;
pub mod catalog_reload;
pub mod catalog_watch;
pub mod cluster_account_write;
pub mod cluster_config;
pub mod cluster_discovery;
mod cluster_http;
pub mod cluster_analyze;
pub mod cluster_auto_id;
pub mod cluster_catalog;
pub mod cluster_ddl;
pub mod cluster_index_id;
pub mod cluster_load_stats;
pub mod cluster_predicate_column;
pub mod cluster_privilege_load;
pub mod cluster_sequence;
pub mod cluster_stats_dump;
pub mod cluster_stats_load;
pub mod cluster_stats_lock;
pub mod cluster_stats_write;
pub mod cluster_sysvar_load;
pub mod cluster_sysvar_write;
pub mod cluster_table_storage;
pub mod compiler;
pub mod tiflash_mpp_scan;
pub mod tiflash_replica_manager;
pub mod cop_scan;
pub mod dag_request;
pub mod foreign_key_build;
pub mod ddl_history_table;
pub mod ddl_job_scheduler;
pub mod ddl_job_submit;
pub mod ddl_job_table;
pub mod ddl_systable;
mod deadlock_recording;
pub mod mlog_purge_info_table;
pub mod mview_alert_table;
pub mod mview_build_engine;
pub mod mview_refresh_info_table;
pub mod mview_schedule_derive;
pub use deadlock_recording::configure_deadlock_history;
pub mod distsql_recordset;
mod error;
mod error_conversion;
pub mod exec_details;
pub mod explain;
pub mod hint_updatable_vars;
pub mod keydecoder;
pub mod label_delivery;
pub mod mdl_info_load;
pub mod multi_statement_transaction;
pub mod mysql_bootstrap;
pub mod mysql_system_tables;
pub mod nontransactional;
pub mod option_values;
pub mod pd_approximate_count;
pub mod pessimistic_lock_error;
pub mod placement_delivery;
pub mod process_info;
pub mod real_tikv_analyze;
pub mod real_tikv_catalog;
pub mod real_tikv_ddl;
pub mod real_tikv_dml;
pub mod real_tikv_load_stats;
pub mod real_tikv_privileges;
pub mod real_tikv_read;
pub mod real_tikv_stats;
pub mod real_tikv_stats_lock;
pub mod recordset_lifecycle;
mod result;
mod result_field_resolver;
mod result_metadata;
pub mod runtime_stats;
pub mod schema_validator;
pub mod session_commit_protocol;
mod slow_log_float;
pub mod slow_log_format;
pub mod slow_log_match;
pub mod slow_log_parse;
pub mod slow_log_rules;
pub mod slow_log_threshold;
mod statement_status;
pub mod stats_watch;
pub mod storage_class;
pub mod storage_reader;
pub mod system_row_write;
pub(crate) mod table_write_policy;
pub mod table_info_build;
pub mod tiflash_stats;
pub mod txn_summary;
pub mod upgrade_versions;
pub mod warning_publication;
pub mod wide_scan_selection;

pub use error::ExecError;
pub use error_conversion::{exec_error_descriptor, exec_error_kind, RenderedExecError};
pub use result::{Outcome, ResultSet, Row};
pub use result_field_resolver::{
    resolve_parsed_select_fields, resolve_result_fields, resolve_select_fields,
    ResolvedResultField, ResultFieldResolveError, ResultFieldSpec,
};
pub use result_metadata::{
    col_names_to_result_fields, columns_from_adapted_fields, convert_result_field,
    AdaptedResultField, FieldNameMetadata, IdentifierMetadata, ResultFieldMetadata,
    ResultFieldTypeMetadata, MAX_ALIAS_IDENTIFIER_LEN, NOT_FIXED_DEC, NOT_NULL_FLAG, UNSIGNED_FLAG,
};
pub use statement_status::{
    PublishedStatementStatus, StatementKind, StatementStatus, StatementWarning, WarningLevel,
};
pub use warning_publication::{
    warnings_from_json, warnings_to_json, IgnoreWarnings, StaticWarningHandler, WarningAppender,
    WarningHandler, WarningPublication, WarningSummary,
};
