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

//! Ports of the authoritative `origin/master` `pkg/executor.part11` slice:
//! items 601–660 in the deterministic top-level `Test*` enumeration.
//!
//! The range splitter is a direct Rust-side behavior port. The other tests
//! exercise session-variable mutation, privilege checks, SHOW/ANALYZE catalog
//! readers, PD/infosync hooks, import-job formatting, or slow-log retrieval;
//! those Go production surfaces are not present in `tidb-executor`, so each is
//! retained as an explicit parity-gap test rather than an approximation.

use tidb_chunk::Chunk;
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::NoColumns;

use crate::shuffle::{PartitionRangeSplitter, PartitionSplitter};

/// Go `pkg/executor/shuffle_test.go:28::TestPartitionRangeSplitter` calls
/// `buildPartitionRangeSplitter` (`pkg/executor/shuffle.go:478`) and assigns
/// each contiguous group to two workers round-robin
/// (`pkg/executor/shuffle.go:490`).
#[test]
fn partition_range_splitter_assigns_contiguous_groups_round_robin() {
    let field_type = FieldType::new(FieldTypeCode::Varchar);
    let mut column = Column::new(1, field_type.clone());
    column.index = 0;
    let by_items = vec![Expression::Column(column)];

    let mut input = Chunk::new(&[field_type], 1024, 1024);
    for value in [
        "a", "a", "a", "a", "c", "c", "b", "b", "b", "q", "eee", "eee", "ddd",
    ] {
        input.append_string(0, value);
    }

    let mut splitter = PartitionRangeSplitter::new(2, by_items);
    let mut obtained = Vec::new();
    PartitionSplitter::<NoColumns>::split(&mut splitter, &NoColumns, &input, &mut obtained)
        .expect("range splitting should evaluate the VARCHAR grouping column");

    assert_eq!(obtained, vec![0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 0, 0, 1]);
}

/// Go `pkg/executor/set_internal_test.go:25::TestRedactSysVarValue` exercises
/// `redactSysVarValue` (`pkg/executor/set.go:286`) for embedding API keys,
/// ordinary values, and cloud-storage URLs.
#[test]
#[ignore = "go-parity-gap: redactSysVarValue is private to Go's SetExecutor and no executor-side sysvar redaction hook exists"]
fn redact_sys_var_value_matches_go_contract() {}

/// Go `pkg/executor/set_test.go:72::TestSetGCLifeTimeNotifiesExternalWorkloadWithEffectiveValue`
/// exercises `notifyExternalWorkloadGCLifeTime` (`pkg/executor/set.go:309`)
/// after `SetExecutor.setSysVariable` (`pkg/executor/set.go:117`), including
/// the ten-minute minimum and keyspace-level external-workload notification.
#[test]
#[ignore = "go-parity-gap: SetExecutor sysvar hooks and external-workload GC manager are not modeled by tidb-executor"]
fn set_gc_lifetime_notifies_external_workload_with_effective_value() {}

/// Go `pkg/executor/set_test.go:108::TestSetEmbeddingAPIKeyRedactedForAuditPlugin`
/// checks the audit callback value emitted by `SetExecutor.setSysVariable`
/// (`pkg/executor/set.go:117`) through `redactSysVarValue` (`:286`).
#[test]
#[ignore = "go-parity-gap: audit plugin callbacks and global sysvar mutation are outside tidb-executor"]
fn set_embedding_api_key_is_redacted_for_audit_plugin() {}

/// Go `pkg/executor/set_test.go:162::TestSetVar` covers `SetExecutor.Next`
/// (`pkg/executor/set.go:60`) for user variables, global/session variables,
/// defaults, charset/collation, warnings, and invalid assignments.
#[test]
#[ignore = "go-parity-gap: SetExecutor.Next and the session variable registry are not present in tidb-executor"]
fn set_var_assigns_and_validates_session_and_global_values() {}

/// Go `pkg/executor/set_test.go:1110::TestSetCollationAndCharset` checks
/// `SetExecutor.setCharset` (`pkg/executor/set.go:339`) and its session
/// charset/collation state updates.
#[test]
#[ignore = "go-parity-gap: session charset/collation state and SetExecutor.setCharset are unported"]
fn set_collation_and_charset_updates_the_session_pair() {}

/// Go `pkg/executor/set_test.go:1154::TestValidateSetVar` checks the variable
/// validation path reached by `SetExecutor.setSysVariable` (`pkg/executor/set.go:117`),
/// including wrong-type errors and truncation warnings.
#[test]
#[ignore = "go-parity-gap: Go's variable.ValidateSetVar chain and warning surface are not owned by tidb-executor"]
fn validate_set_var_reports_type_errors_and_truncation_warnings() {}

/// Go `pkg/executor/set_test.go:1551::TestSetConcurrency` checks executor
/// concurrency defaults and fallback values through `SetExecutor.Next`
/// (`pkg/executor/set.go:60`).
#[test]
#[ignore = "go-parity-gap: executor concurrency sysvars and their session hooks are unported"]
fn set_concurrency_uses_defaults_and_unset_fallbacks() {}

/// Go `pkg/executor/set_test.go:1657::TestEnableNoopFunctionsVar` checks the
/// `IsNoop` gate in `SetExecutor.setSysVariable` (`pkg/executor/set.go:150`).
#[test]
#[ignore = "go-parity-gap: IsNoop sysvar gating and global session-variable writes are unported"]
fn enable_noop_functions_gates_read_only_variables() {}

/// Go `pkg/executor/set_test.go:1762::TestSetClusterConfig` checks the
/// cluster-config dispatch in `SetExecutor.Next` (`pkg/executor/set.go:60`)
/// and `executeSetConfig` (`pkg/executor/set_config.go:63`).
#[test]
#[ignore = "go-parity-gap: SET CONFIG server discovery and HTTP fan-out are not modeled by tidb-executor"]
fn set_cluster_config_routes_by_server_type_and_fans_out() {}

/// Go `pkg/executor/set_test.go:1836::TestSetClusterConfigJSONData` checks
/// `ConvertConfigItem2JSON` (`pkg/executor/set_config.go:187`) for boolean,
/// numeric, string, NULL, and invalid constant values.
#[test]
#[ignore = "go-parity-gap: ConvertConfigItem2JSON has no tidb-executor counterpart"]
fn set_cluster_config_json_data_renders_constant_types() {}

/// Go `pkg/executor/set_test.go:1869::TestSetTopSQLVariables` checks the
/// TopSQL variable hooks reached from `SetExecutor.setSysVariable`
/// (`pkg/executor/set.go:117`).
#[test]
#[ignore = "go-parity-gap: TopSQL state and its sysvar hooks are not modeled by tidb-executor"]
fn set_top_sql_variables_clamp_into_their_ranges() {}

/// Go `pkg/executor/set_test.go:1918::TestDivPrecisionIncrement` checks the
/// `div_precision_increment` validation in `SetExecutor.setSysVariable`
/// (`pkg/executor/set.go:117`) and the resulting session value.
#[test]
#[ignore = "go-parity-gap: session sysvar validation for div_precision_increment is unported"]
fn div_precision_increment_clamps_to_zero_thirty() {}

/// Go `pkg/executor/set_test.go:1949::TestSetTiDBServiceScopeCaseInsensitive` checks
/// the service-scope hook in `SetExecutor.setSysVariable` (`pkg/executor/set.go:117`)
/// and the process-wide `vardef.ServiceScope` update.
#[test]
#[ignore = "go-parity-gap: tidb_service_scope's global sysvar hook and process-wide state are unported"]
fn set_tidb_service_scope_is_case_insensitive() {}

/// Go `pkg/executor/show_affinity_test.go:43::TestShowAffinity` checks
/// `ShowExec.fetchShowAffinity` (`pkg/executor/show_affinity.go:31`) over
/// affinity table and partition metadata.
#[test]
#[ignore = "go-parity-gap: SHOW AFFINITY and PD affinity metadata are not modeled by tidb-executor"]
fn show_affinity_lists_and_filters_affinity_objects() {}

/// Go `pkg/executor/show_affinity_test.go:190::TestShowAffinityColumns` checks
/// the eight-column row construction in `ShowExec.fetchShowAffinity`
/// (`pkg/executor/show_affinity.go:31`) using a mock PD client.
#[test]
#[ignore = "go-parity-gap: SHOW AFFINITY's PD client join and result columns are unported"]
fn show_affinity_renders_pd_state_columns() {}

/// Go `pkg/executor/show_affinity_test.go:252::TestShowAffinityNullStatus` checks
/// NULL status rendering in `ShowExec.fetchShowAffinity`
/// (`pkg/executor/show_affinity.go:31`) when PD has no affinity group.
#[test]
#[ignore = "go-parity-gap: SHOW AFFINITY missing-PD-group NULL rendering is unported"]
fn show_affinity_renders_null_status_without_a_pd_group() {}

/// Go `pkg/executor/show_ddl_jobs_test.go:26::TestShowCommentsFromJob` checks
/// `showCommentsFromJob` (`pkg/executor/show_ddl_jobs.go:302`) for analyze and
/// reorg labels in their source-defined order.
#[test]
#[ignore = "go-parity-gap: DDL job reorg metadata and showCommentsFromJob are not present in tidb-executor"]
fn show_comments_from_job_composes_reorg_labels_in_order() {}

/// Go `pkg/executor/show_ddl_jobs_test.go:115::TestShowCommentsFromSubJob` checks
/// `showCommentsFromSubjob` (`pkg/executor/show_ddl_jobs.go:362`) for ingest,
/// DXF, and cloud labels.
#[test]
#[ignore = "go-parity-gap: DDL subjob metadata and showCommentsFromSubjob are unported"]
fn show_comments_from_subjob_composes_ingest_dxf_and_cloud_labels() {}

/// Go `pkg/executor/show_placement_labels_test.go:26::TestShowPlacementLabelsBuilder`
/// checks `showPlacementLabelsResultBuilder` (`pkg/executor/show_placement.go:43`)
/// and its `AppendStoreLabels`/`BuildRows` methods.
#[test]
#[ignore = "go-parity-gap: placement-label aggregation and JSON row construction are not present in tidb-executor"]
fn show_placement_labels_builder_aggregates_and_sorts_store_labels() {}

/// Go `pkg/executor/show_placement_test.go:35::TestShowPlacement` checks
/// `ShowExec.fetchShowPlacement` (`pkg/executor/show_placement.go:243`) for
/// policy, database, table, and partition rows.
#[test]
#[ignore = "go-parity-gap: placement policy catalog rows and SHOW PLACEMENT are unported"]
fn show_placement_lists_policies_databases_tables_and_partitions() {}

/// Go `pkg/executor/show_placement_test.go:119::TestShowPlacementPrivilege` checks
/// privilege filtering in `ShowExec.fetchShowPlacement` (`pkg/executor/show_placement.go:243`).
#[test]
#[ignore = "go-parity-gap: privilege-filtered SHOW PLACEMENT is not modeled by tidb-executor"]
fn show_placement_hides_objects_without_privileges() {}

/// Go `pkg/executor/show_placement_test.go:184::TestShowPlacementForDB` checks
/// `ShowExec.fetchShowPlacementForDB` (`pkg/executor/show_placement.go:140`) and
/// its schedule-state output.
#[test]
#[ignore = "go-parity-gap: database placement policy lookup and scheduler state are unported"]
fn show_placement_for_db_reports_schedule_state() {}

/// Go `pkg/executor/show_placement_test.go:210::TestShowPlacementForTableAndPartition`
/// checks `ShowExec.fetchShowPlacementForTable` (`pkg/executor/show_placement.go:169`)
/// and the partition variant (`:193`).
#[test]
#[ignore = "go-parity-gap: table/partition placement rule resolution is unported"]
fn show_placement_for_table_and_partition_resolves_rules() {}

/// Go `pkg/executor/show_placement_test.go:289::TestShowPlacementForDBPrivilege` checks
/// database privilege enforcement in `ShowExec.fetchShowPlacementForDB`
/// (`pkg/executor/show_placement.go:140`).
#[test]
#[ignore = "go-parity-gap: database privilege enforcement for SHOW PLACEMENT is unported"]
fn show_placement_for_db_requires_database_privilege() {}

/// Go `pkg/executor/show_placement_test.go:370::TestShowPlacementForTableAndPartitionPrivilege`
/// checks table privilege enforcement in `ShowExec.fetchShowPlacementForTable`
/// (`pkg/executor/show_placement.go:169`) and its partition path.
#[test]
#[ignore = "go-parity-gap: table/partition privilege enforcement for SHOW PLACEMENT is unported"]
fn show_placement_for_table_requires_table_privilege() {}

/// Go `pkg/executor/show_placement_test.go:497::TestShowPlacementHandleRegionStatus`
/// checks PD region-state aggregation in `ShowExec.fetchShowPlacement`
/// (`pkg/executor/show_placement.go:243`).
#[test]
#[ignore = "go-parity-gap: PD region status and infosync hooks for SHOW PLACEMENT are unported"]
fn show_placement_derives_region_status_from_pd() {}

/// Go `pkg/executor/show_stats_test.go:32::TestShowStatsMeta` checks
/// `ShowExec.fetchShowStatsMeta` (`pkg/executor/show_stats.go:36`) after
/// ANALYZE and across its WHERE-filter matrix.
#[test]
#[ignore = "go-parity-gap: SHOW STATS_META's session catalog reader and mysql.stats_meta lifecycle are not owned by tidb-executor"]
fn show_stats_meta_lists_analyzed_tables_and_filters_by_where() {}

/// Go `pkg/executor/show_stats_test.go:112::TestShowStatsLocked` checks
/// `ShowExec.fetchShowStatsLocked` (`pkg/executor/show_stats.go:144`) and
/// physical-table lock metadata.
#[test]
#[ignore = "go-parity-gap: SHOW STATS_LOCKED and stats lock metadata are unported"]
fn show_stats_locked_lists_locked_tables() {}

/// Go `pkg/executor/show_stats_test.go:134::TestShowStatsHistograms` checks
/// `ShowExec.fetchShowStatsHistogram` (`pkg/executor/show_stats.go:204`) after
/// repeated ANALYZE operations.
#[test]
#[ignore = "go-parity-gap: SHOW STATS_HISTOGRAMS reads mysql.stats_* catalog rows not exposed here"]
fn show_stats_histograms_lists_analyzed_columns_and_indexes() {}

/// Go `pkg/executor/show_stats_test.go:165::TestShowStatsBuckets` checks
/// `ShowExec.fetchShowStatsBuckets` (`pkg/executor/show_stats.go:285`) for
/// version-2 column and index buckets.
#[test]
#[ignore = "go-parity-gap: SHOW STATS_BUCKETS catalog retrieval is unported"]
fn show_stats_buckets_lists_version_two_histogram_buckets() {}

/// Go `pkg/executor/show_stats_test.go:210::TestShowStatsBucketWithDateNullValue` checks
/// `ShowExec.fetchShowStatsBuckets` (`pkg/executor/show_stats.go:285`) together
/// with date/null histogram encoding and the related plan.
#[test]
#[ignore = "go-parity-gap: date/null stats-bucket catalog output and its session EXPLAIN path are unported"]
fn show_stats_buckets_preserves_date_and_null_values() {}

/// Go `pkg/executor/show_stats_test.go:229::TestShowStatsHasNullValue` checks
/// NULL exclusion and later histogram publication through
/// `ShowExec.fetchShowStatsBuckets` (`pkg/executor/show_stats.go:285`).
#[test]
#[ignore = "go-parity-gap: single-column NULL histogram publication through SHOW STATS_BUCKETS is unported"]
fn show_stats_buckets_handles_null_values() {}

/// Go `pkg/executor/show_stats_test.go:296::TestShowStatusSnapshot` checks
/// snapshot resolution in the SHOW table-status path (`pkg/executor/show.go:221`)
/// and `SetExecutor.setSysVariable` (`pkg/executor/set.go:117`) for `tidb_snapshot`.
#[test]
#[ignore = "go-parity-gap: session snapshot reads and SHOW TABLE STATUS are not modeled by tidb-executor"]
fn show_table_status_uses_tidb_snapshot() {}

/// Go `pkg/executor/show_stats_test.go:326::TestShowColumnStatsUsage` checks
/// the column-stats usage SHOW retriever (`pkg/executor/show.go:183` and
/// `pkg/executor/show_stats.go:36`) across global and partition physical IDs.
#[test]
#[ignore = "go-parity-gap: mysql.column_stats_usage persistence and SHOW retrieval are unported"]
fn show_column_stats_usage_lists_global_and_partition_rows() {}

/// Go `pkg/executor/show_stats_test.go:358::TestShowAnalyzeStatus` checks
/// the SHOW ANALYZE STATUS path (`pkg/executor/show.go:237`) and analyze-job
/// persistence, timing, and instance fields.
#[test]
#[ignore = "go-parity-gap: analyze-job persistence and SHOW ANALYZE STATUS are not modeled by tidb-executor"]
fn show_analyze_status_lists_finished_and_running_jobs() {}

/// Go `pkg/executor/show_test.go:39::TestFillOneImportJobInfo` checks the
/// exported formatter `FillOneImportJobInfo` (`pkg/executor/show.go:2505`)
/// for pending, finished, progress, conflict, and preparing states.
#[test]
#[ignore = "go-parity-gap: IMPORT INTO job metadata and FillOneImportJobInfo are not present in tidb-executor"]
fn fill_one_import_job_info_formats_all_job_states() {}

/// Go `pkg/executor/show_test.go:148::TestShow` checks `ShowExec.fetchShowTables`
/// (`pkg/executor/show.go:217`), the SHOW columns path (`pkg/executor/show.go:187`),
/// and visibility of temporary/global-temporary tables and variables.
#[test]
#[ignore = "go-parity-gap: the full session SHOW statement surface and temporary-table visibility are unported"]
fn show_tables_columns_and_variables_match_go_visibility() {}

/// Go `pkg/executor/show_test.go:183::TestAdminShowSlowIARemoteReadStats` checks
/// `ShowExec`'s ADMIN SHOW SLOW path (`pkg/executor/show_slow_queries.go:38`)
/// and its remote-read detail columns.
#[test]
#[ignore = "go-parity-gap: slow-query domain storage and ADMIN SHOW SLOW are not modeled by tidb-executor"]
fn admin_show_slow_includes_remote_read_statistics() {}

/// Go `pkg/executor/show_test.go:239::TestShowIndex` checks the SHOW INDEX
/// path (`pkg/executor/show.go:211`) after creating a secondary index.
#[test]
#[ignore = "go-parity-gap: SHOW INDEX is owned by the session SHOW arm, outside this executor crate's gate surface"]
fn show_index_lists_secondary_index_columns() {}

/// Go `pkg/executor/show_test.go:250::TestShowIndexWithGlobalIndex` checks
/// global-index rendering through `ShowExec.fetchShowIndex`
/// (`pkg/executor/show.go:211`) on a partitioned table.
#[test]
#[ignore = "go-parity-gap: global-index DDL and SHOW INDEX metadata are not available in tidb-executor"]
fn show_index_lists_global_indexes() {}

/// Go `pkg/executor/show_test.go:266::TestShowSessionStates` checks the
/// session-state SHOW path (`pkg/executor/show.go:231`) for authenticated and
/// unauthenticated sessions.
#[test]
#[ignore = "go-parity-gap: session registry and SHOW SESSION_STATES are not modeled by tidb-executor"]
fn show_session_states_reports_current_sessions() {}

/// Go `pkg/executor/simple_internal_test.go:29::TestAlterUserHasPrivilegedOptions`
/// checks `alterUserHasPrivilegedOptions` (`pkg/executor/simple.go:1713`) as
/// an allowlist over parsed ALTER USER statement-level options.
#[test]
#[ignore = "go-parity-gap: the Rust AST parses ALTER USER, but this private executor helper and its statement gate are not in tidb-executor"]
fn alter_user_privileged_options_cover_all_statement_option_families() {}

/// Go `pkg/executor/simple_test.go:33::TestRefreshTableStats` checks
/// `SimpleExec.executeRefreshStats` (`pkg/executor/simple.go:3175`) and its
/// lite/full statistics object replacement behavior.
#[test]
#[ignore = "go-parity-gap: REFRESH STATS execution and mutable StatsHandle lifecycle are not in tidb-executor"]
fn refresh_table_stats_reloads_lite_and_full_statistics() {}

/// Go `pkg/executor/simple_test.go:70::TestRefreshStatsWarningsForMissingObjects` checks
/// warning generation in `SimpleExec.executeRefreshStatsOnCurrentInstance`
/// (`pkg/executor/simple.go:3238`).
#[test]
#[ignore = "go-parity-gap: REFRESH STATS warning accumulation is part of the unported session executor"]
fn refresh_stats_warns_for_missing_objects() {}

/// Go `pkg/executor/simple_test.go:104::TestRefreshAllNonExistentTables` checks
/// no-op behavior in `SimpleExec.executeRefreshStatsOnCurrentInstance`
/// (`pkg/executor/simple.go:3238`) when all requested objects are absent.
#[test]
#[ignore = "go-parity-gap: REFRESH STATS object selection is not modeled by tidb-executor"]
fn refresh_stats_ignores_all_missing_objects_without_touching_existing_stats() {}

/// Go `pkg/executor/simple_test.go:126::TestRefreshStatsNoTables` checks the
/// wildcard no-table path in `SimpleExec.executeRefreshStats`
/// (`pkg/executor/simple.go:3175`).
#[test]
#[ignore = "go-parity-gap: wildcard REFRESH STATS has no tidb-executor statement path"]
fn refresh_stats_wildcard_with_no_tables_is_a_noop() {}

/// Go `pkg/executor/simple_test.go:135::TestRefreshStatsRequiresDefaultDB` checks
/// default-database validation in `SimpleExec.executeRefreshStats`
/// (`pkg/executor/simple.go:3175`).
#[test]
#[ignore = "go-parity-gap: session default-database validation for REFRESH STATS is unported"]
fn refresh_stats_bare_table_requires_a_default_database() {}

/// Go `pkg/executor/simple_test.go:142::TestRefreshStatsWhenDatabaseIsEmpty` checks
/// empty-database handling in `SimpleExec.executeRefreshStatsOnCurrentInstance`
/// (`pkg/executor/simple.go:3238`).
#[test]
#[ignore = "go-parity-gap: empty-database REFRESH STATS handling is not modeled by tidb-executor"]
fn refresh_stats_empty_database_has_no_warnings() {}

/// Go `pkg/executor/simple_test.go:154::TestRefreshStatsPrivilegeChecks` checks
/// privilege gates in `SimpleExec.executeRefreshStatsOnCurrentInstance`
/// (`pkg/executor/simple.go:3238`) at table, database, and global scopes.
#[test]
#[ignore = "go-parity-gap: privilege-aware REFRESH STATS execution is unported"]
fn refresh_stats_enforces_table_database_and_global_privileges() {}

/// Go `pkg/executor/simple_test.go:198::TestRefreshStatsWithRestoreAdmin` checks
/// the RESTORE_ADMIN exception in `SimpleExec.executeRefreshStats`
/// (`pkg/executor/simple.go:3175`).
#[test]
#[ignore = "go-parity-gap: RESTORE_ADMIN privilege and REFRESH STATS authorization are unported"]
fn refresh_stats_accepts_restore_admin() {}

/// Go `pkg/executor/simple_test.go:219::TestRefreshStatsWithFullMode` checks
/// full-mode reloads in `SimpleExec.executeRefreshStatsOnCurrentInstance`
/// (`pkg/executor/simple.go:3238`) and subsequent analyze/load versions.
#[test]
#[ignore = "go-parity-gap: full-mode REFRESH STATS and StatsHandle storage reload are unported"]
fn refresh_stats_full_mode_loads_index_buckets() {}

/// Go `pkg/executor/simple_test.go:284::TestRefreshStatsWithLiteMode` checks
/// lite/full transitions in `SimpleExec.executeRefreshStatsOnCurrentInstance`
/// (`pkg/executor/simple.go:3238`).
#[test]
#[ignore = "go-parity-gap: lite-mode REFRESH STATS state transitions are unported"]
fn refresh_stats_lite_mode_leaves_index_stats_unloaded() {}

/// Go `pkg/executor/simple_test.go:340::TestRefreshStatsConcurrently` checks
/// concurrent calls through `SimpleExec.executeRefreshStats`
/// (`pkg/executor/simple.go:3175`) and final full index-stat publication.
#[test]
#[ignore = "go-parity-gap: concurrent session REFRESH STATS scheduling is not modeled by tidb-executor"]
fn refresh_stats_concurrent_requests_converge_on_full_stats() {}

/// Go `pkg/executor/simple_test.go:411::TestFlushStatsDelta` checks
/// `SimpleExec.executeFlushStatsDelta` (`pkg/executor/simple.go:3324`) across
/// full, table, database, partition, privilege, and no-default-db scopes.
#[test]
#[ignore = "go-parity-gap: FLUSH STATS_DELTA and mysql.stats_meta mutation are unported"]
fn flush_stats_delta_updates_scoped_modify_counts() {}

/// Go `pkg/executor/slow_query_sql_test.go:45::TestSlowQueryWithoutSlowLog`
/// checks the `slowQueryRetriever` (`pkg/executor/slow_query.go:69`) empty-file
/// behavior through the information-schema slow-query table.
#[test]
#[ignore = "go-parity-gap: slowQueryRetriever and the information_schema slow-query table are not in tidb-executor"]
fn slow_query_without_a_slow_log_is_empty() {}

/// Go `pkg/executor/slow_query_sql_test.go:61::TestSlowQuerySensitiveQuery`
/// checks query redaction in `ExecStmt.LogSlowQuery` (`pkg/executor/adapter.go:1967`)
/// and slow-log retrieval by `slowQueryRetriever` (`pkg/executor/slow_query.go:69`).
#[test]
#[ignore = "go-parity-gap: slow-log file writing, sensitive-query redaction, and retrieval are unported"]
fn slow_query_redacts_sensitive_account_statements() {}

/// Go `pkg/executor/slow_query_sql_test.go:97::TestSlowQueryNonPrepared` checks
/// non-prepared plan-cache fields emitted by `ExecStmt.LogSlowQuery`
/// (`pkg/executor/adapter.go:1967`) and read by `slowQueryRetriever`
/// (`pkg/executor/slow_query.go:69`).
#[test]
#[ignore = "go-parity-gap: non-prepared plan-cache slow-log fields and information-schema retrieval are unported"]
fn slow_query_records_non_prepared_plan_cache_hits() {}

/// Go `pkg/executor/slow_query_sql_test.go:137::TestSlowQueryMisc` checks
/// prepared arguments, redact-log formatting, and stale-read fields emitted by
/// `ExecStmt.LogSlowQuery` (`pkg/executor/adapter.go:1967`).
#[test]
#[ignore = "go-parity-gap: slow-log prepared-argument, redaction, and stale-read session surfaces are unported"]
fn slow_query_records_prepared_arguments_and_stale_reads() {}

/// Go `pkg/executor/slow_query_sql_test.go:190::TestLogSlowLogIndex` checks
/// index-name capture in `ExecStmt.LogSlowQuery` (`pkg/executor/adapter.go:1967`)
/// and retrieval by `slowQueryRetriever` (`pkg/executor/slow_query.go:69`).
#[test]
#[ignore = "go-parity-gap: slow-log index metadata emission and retrieval are unported"]
fn slow_query_records_used_index_names() {}
