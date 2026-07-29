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
//! for storage/scan seam types (`cluster_storage`, `pushdown_scan`,
//! `scan_pushdown`, `StorageError`) -- no operator ever crosses.
//!
//! A SECOND, EARLIER query engine (`Database`/`Cluster`/`Session`: datum-based
//! `Row = Vec<Datum>`, fully eager, linear-scan grouping, O(n^2) RANGE framing)
//! used to live here alongside those subsystems. It was reachable only from
//! tests, it duplicated SQL SEMANTICS rather than mechanics, and the two
//! engines could therefore disagree. Its Go-evidenced behaviors were harvested
//! onto the live path and it has been deleted.
//!
//! What remains of it is STATE, not operators, kept because the live path
//! borrows it:
//!
//! * [`order`]'s total order over datums and the configured/prepared
//!   `ORDER BY` key contracts, used by `tidb-server`'s
//!   `sorting_result_set`.
//! * [`aggregate`]'s per-kind partial states (`aggregate::runtime`) and tuple
//!   DISTINCT identity (`aggregate::aggregate_distinct`), used by
//!   `tidb-server`'s `aggregate_result_set` and `distinct_result_set`.
//! * `window::ranking_runtime`'s peer geometry, used by this crate's own
//!   source-shaped ranking states ([`cume_dist`], [`ntile`], [`lead_lag`]).
//!
//! ## Module layout
//!
//! Split by concern so unrelated features can be extended without touching the
//! same file. The cluster subsystems are named by their `cluster_*` and
//! `real_tikv_*` prefixes; the result-metadata contracts by `result_*`; the
//! aggregate and window partial states by their Go function names. This file
//! keeps only the crate-level vocabulary the rest builds on (`Row`,
//! `ResultSet`, `Outcome`, `ExecError`) and the re-export surface.

pub mod advisory_lock_state;
pub mod aggregate;
pub mod alternative_plan_signals;
pub mod analyze_panic_error;
pub mod apply_cache;
pub mod bit_agg;
pub mod broadcast_query_error;
pub mod catalog_reload;
pub mod catalog_watch;
pub mod charset_variable_groups;
pub mod chunk_alloc_status;
pub mod cluster_account_write;
pub mod cluster_analyze;
pub mod cluster_catalog;
pub mod cluster_ddl;
pub mod cluster_index_id;
pub mod cluster_privilege_load;
pub mod cluster_stats_load;
pub mod cluster_stats_write;
pub mod cluster_sysvar_load;
pub mod cluster_sysvar_write;
pub mod cluster_table_storage;
pub mod concurrent_entry_map;
pub mod config_int_json;
pub mod configured_inner_join;
pub mod configured_ordered_query;
pub mod configured_topn;
pub mod context_id;
pub mod cop_scan;
pub mod cte_first_error;
pub mod cume_dist;
pub mod cursor_tracker;
pub mod dag_request;
pub mod ddl_job_comments;
pub mod delete_rows_col_multiply;
pub mod distsql_recordset;
pub mod effective_auth_plugin;
mod error;
pub mod error_context;
mod error_conversion;
pub mod explain;
pub mod first_row;
pub mod global_sysvar_initial;
pub mod group_concat;
pub mod hash_join_version;
pub mod hint_updatable_vars;
pub mod insert_rows_col_multiply;
pub mod isolation_state;
pub mod join_table_meta;
pub mod json_arrayagg;
pub mod json_objectagg;
pub mod lack_handles;
pub mod lazy_txn_state;
pub mod lead_lag;
pub mod minmax_deque;
pub mod mock_global_accessor;
pub mod multi_statement_transaction;
pub mod mysql_bootstrap;
pub mod mysql_system_tables;
pub mod next_io_acc;
pub mod nextgen_readonly_vars;
pub mod nontransactional;
pub mod noop_read_only;
pub mod ntile;
pub mod option_values;
pub mod order;
pub mod ordered_apply_buffer;
pub mod password_validation;
pub mod pd_approximate_count;
pub mod percentile;
pub mod pessimistic_lock_error;
pub mod placement_labels;
pub mod plan_cache_params;
pub mod privilege_set;
pub mod process_info;
pub mod read_consistency;
pub mod readable_size;
pub mod real_tikv_analyze;
pub mod real_tikv_catalog;
pub mod real_tikv_ddl;
pub mod real_tikv_dml;
pub mod real_tikv_multi_read;
pub mod real_tikv_privileges;
pub mod real_tikv_read;
pub mod real_tikv_stats;
pub mod recordset_lifecycle;
pub mod removed_sysvar;
pub mod reserved_row_id;
mod result;
mod result_field_resolver;
mod result_metadata;
mod result_response;
mod result_schema;
mod result_schema_join_output;
mod result_schema_multi;
mod result_schema_projection;
pub mod retry_info;
pub mod sequence_state;
pub mod session_commit_protocol;
pub mod session_context_key;
pub mod session_metrics;
pub mod session_pool_capacity;
pub mod session_reuse_state;
pub mod session_status;
pub mod session_token_timing;
pub mod setvar_hint_restore;
pub mod slow_log_match;
pub mod slow_log_rules;
pub mod slow_log_split;
pub mod slow_log_threshold;
pub mod statement_pushdown;
pub mod statement_refcount;
pub mod statement_rows_reader;
mod statement_status;
pub mod stats_load_result;
pub mod stats_watch;
pub mod status_registry;
mod status_result;
pub mod stddevpop;
pub mod stddevsamp;
pub mod storage_reader;
pub mod system_db_filter;
pub mod system_row_write;
pub mod sysvar_error;
pub mod sysvar_scope;
pub mod sysvar_type;
pub mod table_info_build;
pub mod tagged_ptr;
pub mod traffic_form;
pub mod txn_read_ts;
pub mod txn_running_state;
pub mod txn_summary;
pub mod upgrade_versions;
pub mod used_stats;
pub mod varpop;
pub mod varsamp;
pub mod vec_group_checker_int;
pub mod warning_publication;
pub mod wide_scan_selection;
mod window;
pub mod window_value_int;

pub use error::ExecError;
pub use error_context::{
    resolve_err_level, ErrGroup, ErrorContext, ErrorContextFlags, ErrorDisposition, Level, LevelMap,
};
pub use error_conversion::{exec_error_descriptor, exec_error_kind, RenderedExecError};
pub use result::{Outcome, ResultSet, Row};
pub use result_field_resolver::{
    resolve_result_fields, resolve_select_fields, ResolvedResultField, ResultFieldResolveError,
    ResultFieldSpec,
};
pub use result_metadata::{
    col_names_to_result_fields, columns_from_adapted_fields, convert_result_field,
    AdaptedResultField, FieldNameMetadata, IdentifierMetadata, ResultFieldMetadata,
    ResultFieldTypeMetadata, MAX_ALIAS_IDENTIFIER_LEN, NOT_FIXED_DEC, NOT_NULL_FLAG, UNSIGNED_FLAG,
};
pub use result_response::{
    derive_tableless_select_columns, derive_tableless_select_result, resolve_query_result_columns,
    AutomaticResultResponse, AutomaticResultResponseError,
};
pub use result_schema::{
    resolve_catalog_select_fields, CatalogColumn, CatalogSchemaError, CatalogTableSchema,
};
pub use result_schema_join_output::{
    derive_join_output_metadata, JoinOutputChild, JoinOutputField, JoinOutputMetadata,
    JoinOutputOrigin, JoinOutputSchemaError, JoinOutputUnsupported,
};
pub use result_schema_multi::{resolve_catalog_relation_select_fields, CatalogRelationSchemaError};
pub use result_schema_projection::{project_join_output_fields, JoinProjectionError};
pub use statement_status::{
    PublishedStatementStatus, StatementKind, StatementStatus, StatementWarning, WarningLevel,
};
pub use status_result::{finish_and_snapshot, StatusResultSnapshot};
pub use warning_publication::{
    warnings_from_json, warnings_to_json, IgnoreWarnings, StaticWarningHandler, WarningAppender,
    WarningHandler, WarningPublication, WarningSummary, MAX_WARNING_COUNT,
};
