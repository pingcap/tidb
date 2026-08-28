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

//! Source-backed inventory for `pkg/executor.part20`, Go test items 1141–1200.
//!
//! The assigned tests are the remainder of `seqtest`, the `showtest` and
//! `simpletest` session suites, the `splittest` region suite, and the first
//! four behavior tests in `tiflashtest`. They are retained here as explicit
//! parity-gap tests because this crate is the executor-only SQL/catalog tier:
//! the Go cases require session variables and authentication, server/domain
//! state, PD/TiKV request plumbing, failpoints, transaction coordination,
//! region metadata, or TiFlash/MPP replicas. No unsupported behavior is
//! approximated by a local SQL query.

/// `pkg/executor/test/seqtest/seq_executor_test.go:866::TestPrepareMaxParamCountCheck`.
#[test]
#[ignore = "go-parity-gap: the placeholder-count limit is enforced by the session PREPARE/EXECUTE layer, not the executor catalog driver"]
fn prepare_max_param_count_check() {}

/// `seq_executor_test.go:893::TestCartesianProduct`.
#[test]
#[ignore = "go-parity-gap: Cartesian-product rejection is a planner session flag and error path absent from this executor-only driver"]
fn cartesian_product_is_rejected_when_disabled() {}

/// `seq_executor_test.go:910::TestBatchInsertDelete`.
#[test]
#[ignore = "go-parity-gap: batch DML, transaction-size limits, and session batch variables require tidb-session and KV transaction state"]
fn batch_insert_and_delete_respect_transaction_limits() {}

/// `seq_executor_test.go:1069::TestCoprocessorPriority`.
#[test]
#[ignore = "go-parity-gap: coprocessor request priority is carried through TiKV RPC context and has no local executor seam"]
fn coprocessor_requests_preserve_priority() {}

/// `seq_executor_test.go:1160::TestPessimisticConflictRetryAutoID`.
#[test]
#[ignore = "go-parity-gap: concurrent pessimistic transactions, auto-ID allocation, and duplicate-key retry require session/KV transaction coordination"]
fn pessimistic_conflict_retry_preserves_auto_id() {}

/// `seq_executor_test.go:1196::TestInsertFromSelectConflictRetryAutoID`.
#[test]
#[ignore = "go-parity-gap: concurrent INSERT SELECT conflict retries and auto-ID allocation are distributed transaction behavior"]
fn insert_from_select_conflict_retry_preserves_auto_id() {}

/// `seq_executor_test.go:1247::TestAutoRandRecoverTable`.
#[test]
#[ignore = "go-parity-gap: RECOVER TABLE, emulator GC, DDL history, failpoints, and auto-random rebasing require tidb-domain/DDL state"]
fn auto_random_recover_table_rebases_handles() {}

/// `seq_executor_test.go:1298::TestOOMPanicInHashJoinWhenFetchBuildRows`.
#[test]
#[ignore = "go-parity-gap: the Go test injects a join worker failpoint and observes a panic through the session result-set boundary"]
fn hash_join_build_fetch_oom_is_reported() {}

/// `seq_executor_test.go:1315::TestIssue18744`.
#[test]
#[ignore = "go-parity-gap: the regression depends on the index-hash-join worker failpoint and distributed worker cleanup"]
fn issue18744_index_hash_join_worker_error_is_returned() {}

/// `seq_executor_test.go:1364::TestAnalyzeNextRawErrorNoLeak`.
#[test]
#[ignore = "go-parity-gap: ANALYZE execution, distsql raw-result delivery, failpoints, and goroutine cleanup are not executor-local"]
fn analyze_raw_result_error_does_not_leak_workers() {}

/// `pkg/executor/test/showtest/main_test.go:26::TestMain`.
#[test]
#[ignore = "skipped-reason: Go TestMain only installs goleak/configuration hooks and has no SQL behavior to port"]
fn showtest_main_is_bootstrap_only() {}

/// `show_test.go:46::TestShowCreateTablePlacement`.
#[test]
#[ignore = "go-parity-gap: placement-policy DDL and SHOW CREATE TABLE placement rendering require session/domain metadata"]
fn show_create_table_renders_placement_policies() {}

/// `show_test.go:209::TestShowVisibility`.
#[test]
#[ignore = "go-parity-gap: SHOW DATABASES/TABLES visibility is governed by authenticated session privileges"]
fn show_visibility_tracks_table_privileges() {}

/// `show_test.go:245::TestShowWarnings`.
#[test]
#[ignore = "go-parity-gap: warning levels and SHOW WARNINGS are stored in the session statement context"]
fn show_warnings_renders_statement_diagnostics() {}

/// `show_test.go:279::TestShowWarningsForExprPushdown`.
#[test]
#[ignore = "go-parity-gap: TiFlash pushdown warnings require planner engine selection and session warning state"]
fn show_warnings_reports_unsupported_tiflash_pushdown() {}

/// `show_test.go:315::TestShowGrantsPrivilege`.
#[test]
#[ignore = "go-parity-gap: SHOW GRANTS and authenticated privilege checks are owned by tidb-session"]
fn show_grants_enforces_privilege_visibility() {}

/// `show_test.go:330::TestShowStatsPrivilege`.
#[test]
#[ignore = "go-parity-gap: SHOW STATS privilege checks require the session privilege manager and statistics domain"]
fn show_stats_enforces_privileges() {}

/// `show_test.go:364::TestShowStatsExtendedRemoved`.
#[test]
#[ignore = "go-parity-gap: SHOW statement dispatch and MySQL-compatible removed-feature errors are not exposed by the executor catalog driver"]
fn show_stats_extended_reports_removed_feature() {}

/// `show_test.go:371::TestIssue18878`.
#[test]
#[ignore = "go-parity-gap: current/authenticated user resolution and SHOW GRANTS account matching require tidb-session"]
fn issue18878_resolves_authenticated_grant_identity() {}

/// `show_test.go:390::TestIssue17794`.
#[test]
#[ignore = "go-parity-gap: host-pattern account matching and SHOW GRANTS require the authentication/privilege subsystem"]
fn issue17794_preserves_host_specific_grants() {}

/// `show_test.go:402::TestIssue10549`.
#[test]
#[ignore = "go-parity-gap: roles, default roles, database visibility, and SHOW GRANTS require session account metadata"]
fn issue10549_renders_role_grants_and_database_visibility() {}

/// `show_test.go:419::TestIssue11165`.
#[test]
#[ignore = "go-parity-gap: SET DEFAULT ROLE and authenticated role resolution are session account behavior"]
fn issue11165_updates_default_roles() {}

/// `show_test.go:434::TestShow2`.
#[test]
#[ignore = "go-parity-gap: this broad SHOW suite needs global/session variables, views, sequences, information_schema, and domain timestamps"]
fn show2_covers_metadata_and_variable_views() {}

/// `show_test.go:561::TestShowCreateUser`.
#[test]
#[ignore = "go-parity-gap: SHOW CREATE USER renders authentication plugins, TLS requirements, password policy, and account attributes"]
fn show_create_user_renders_account_metadata() {}

/// `show_test.go:669::TestUnprivilegedShow`.
#[test]
#[ignore = "go-parity-gap: SHOW TABLE STATUS visibility requires authenticated table privileges and domain metadata"]
fn unprivileged_show_hides_ungranted_tables() {}

/// `show_test.go:696::TestCollation`.
#[test]
#[ignore = "go-parity-gap: SHOW COLLATION result metadata and MySQL field types require the session SHOW executor"]
fn show_collation_reports_mysql_field_types() {}

/// `show_test.go:712::TestShowTableStatus`.
#[test]
#[ignore = "go-parity-gap: SHOW TABLE STATUS needs schema timestamps, partition metadata, and result-set/session formatting"]
fn show_table_status_renders_table_and_partition_metadata() {}

/// `show_test.go:778::TestAutoRandomBase`.
#[test]
#[ignore = "go-parity-gap: SHOW TABLE/NEXT_ROW_ID auto-random state is backed by domain auto-ID allocators"]
fn show_auto_random_base_reports_allocator_state() {}

/// `show_test.go:812::TestAutoRandomWithLargeSignedShowTableRegions`.
#[test]
#[ignore = "go-parity-gap: SHOW TABLE REGIONS and signed auto-random handles require TiKV region and allocator metadata"]
fn show_auto_random_regions_handles_large_signed_ids() {}

/// `show_test.go:832::TestShowEscape`.
#[test]
#[ignore = "go-parity-gap: SHOW output escaping is implemented by the session SHOW executor and protocol result formatting"]
fn show_escape_preserves_special_characters() {}

/// `show_test.go:862::TestShowClusterConfig`.
#[test]
#[ignore = "go-parity-gap: SHOW CLUSTER CONFIG requires server discovery and configuration RPC fan-out"]
fn show_cluster_config_reads_server_configuration() {}

/// `show_test.go:897::TestShowConfig`.
#[test]
#[ignore = "go-parity-gap: SHOW CONFIG requires server configuration state outside the executor catalog"]
fn show_config_reads_effective_configuration() {}

/// `show_test.go:916::TestShowCreateTableWithIntegerDisplayLengthWarnings`.
#[test]
#[ignore = "go-parity-gap: SHOW CREATE TABLE warnings are session statement diagnostics produced during DDL execution"]
fn show_create_table_reports_integer_display_length_warnings() {}

/// `show_test.go:1005::TestShowVar`.
#[test]
#[ignore = "go-parity-gap: SHOW VARIABLES/STATUS and sysvar scope are session-state surfaces"]
fn show_var_reads_session_and_global_variables() {}

/// `show_test.go:1060::TestShowCreatePlacementPolicy`.
#[test]
#[ignore = "go-parity-gap: placement-policy SHOW CREATE dispatch is not part of the executor-only catalog driver"]
fn show_create_placement_policy_renders_policy_options() {}

/// `show_test.go:1079::TestShowLimitReturnRow`.
#[test]
#[ignore = "go-parity-gap: SQL_SELECT_LIMIT, PREPARE/EXECUTE, and SHOW result filtering require tidb-session"]
fn show_limit_return_row_applies_to_show_and_select() {}

/// `pkg/executor/test/simpletest/main_test.go:23::TestMain`.
#[test]
#[ignore = "skipped-reason: Go TestMain only installs suite bootstrap hooks and has no SQL behavior to port"]
fn simpletest_main_is_bootstrap_only() {}

/// `simple_test.go:43::TestStarterUsernamePolicyInSimpleExec`.
#[test]
#[ignore = "go-parity-gap: starter deploy mode, keyspace configuration, users, roles, and privilege checks require tidb-session/server state"]
fn starter_username_policy_is_enforced() {}

/// `simple_test.go:109::TestUserWithSetNames`.
#[test]
#[ignore = "go-parity-gap: SET NAMES, authentication encoding, and account rename require session charset and privilege state"]
fn user_passwords_follow_set_names_encoding() {}

/// `simple_test.go:130::TestTransaction`.
#[test]
#[ignore = "go-parity-gap: BEGIN/COMMIT/ROLLBACK and implicit DDL commits require tidb-session transaction state"]
fn transaction_boundaries_commit_and_rollback_rows() {}

/// `simple_test.go:164::TestRole`.
#[test]
#[ignore = "go-parity-gap: role graph, default-role metadata, and SET ROLE are account/session behavior"]
fn role_grants_and_default_roles_are_maintained() {}

/// `simple_test.go:261::TestMaxUserConnections`.
#[test]
#[ignore = "go-parity-gap: max_user_connections is a global/session sysvar with privilege enforcement"]
fn max_user_connections_is_clamped_and_enforced() {}

/// `simple_test.go:322::TestUser`.
#[test]
#[ignore = "go-parity-gap: CREATE/ALTER/DROP USER, authentication plugins, warnings, and role metadata require tidb-session"]
fn user_account_ddl_matches_mysql_behavior() {}

/// `simple_test.go:572::TestSetPwd`.
#[test]
#[ignore = "go-parity-gap: SET PASSWORD and authentication-plugin privilege checks require session account state"]
fn set_password_updates_the_authenticated_account() {}

/// `simple_test.go:624::TestFlushPrivilegesPanic`.
#[test]
#[ignore = "go-parity-gap: FLUSH PRIVILEGES and SkipGrantTable bootstrap exercise session/domain lifecycle state"]
fn flush_privileges_is_safe_without_grant_tables() {}

/// `simple_test.go:647::TestDropPartitionStats`.
#[test]
#[ignore = "go-parity-gap: ANALYZE/DROP STATS and partition statistics require the statistics handle and domain"]
fn drop_partition_stats_removes_partition_histograms() {}

/// `simple_test.go:715::TestDropStats`.
#[test]
#[ignore = "go-parity-gap: ANALYZE/DROP STATS state is maintained by tidb-domain statistics handles"]
fn drop_stats_resets_table_statistics() {}

/// `simple_test.go:770::TestDropStatsForMultipleTable`.
#[test]
#[ignore = "go-parity-gap: multi-table statistics analysis and DROP STATS require domain statistics state"]
fn drop_stats_resets_multiple_table_statistics() {}

/// `simple_test.go:845::TestKillStmt`.
#[test]
#[ignore = "go-parity-gap: KILL dispatch requires tidb-server connection IDs, global-kill configuration, and session statements"]
fn kill_statement_routes_connection_requests() {}

/// `simple_test.go:899::TestSelectWhereInvalidDSTTime`.
#[test]
#[ignore = "go-parity-gap: session time zones, timestamp warnings, and warning retrieval are not executor-local"]
fn select_where_handles_invalid_dst_timestamps() {}

/// `pkg/executor/test/splittest/main_test.go:23::TestMain`.
#[test]
#[ignore = "skipped-reason: Go TestMain only installs suite bootstrap hooks and has no SQL behavior to port"]
fn splittest_main_is_bootstrap_only() {}

/// `split_table_test.go:41::TestClusterIndexShowTableRegion`.
#[test]
#[ignore = "go-parity-gap: SHOW TABLE REGIONS over clustered indexes requires TiKV region metadata and RPCs"]
fn cluster_index_show_table_region_reports_regions() {}

/// `split_table_test.go:84::TestShowTableRegion`.
#[test]
#[ignore = "go-parity-gap: SHOW TABLE REGIONS and split-region behavior require distributed storage metadata"]
fn show_table_region_reports_split_ranges() {}

/// `split_table_test.go:613::BenchmarkLocateRegion`.
#[test]
#[ignore = "skipped-reason: Go benchmark measures PD/TiKV region location latency, which has no executor-only equivalent"]
fn locate_region_benchmark_is_storage_bound() {}

/// `split_table_test.go:644::TestBenchDaily`.
#[test]
#[ignore = "go-parity-gap: split-table daily benchmark requires distributed region placement and storage RPCs"]
fn split_table_daily_benchmark_is_storage_bound() {}

/// `pkg/executor/test/tiflashtest/main_test.go:27::TestMain`.
#[test]
#[ignore = "skipped-reason: Go TestMain only configures TiFlash test bootstrap and goleak"]
fn tiflashtest_main_is_bootstrap_only() {}

/// `tiflash_test.go:68::TestNonsupportCharsetTable`.
#[test]
#[ignore = "go-parity-gap: TiFlash replica DDL and charset capability validation require domain replica metadata"]
fn nonsupport_charset_table_rejects_gbk_tiflash_replica() {}

/// `tiflash_test.go:83::TestReadPartitionTable`.
#[test]
#[ignore = "go-parity-gap: partition pruning, TiFlash replica reads, union scan, transactions, and batch cop require MPP/storage state"]
fn read_partition_table_uses_tiflash_and_union_scan() {}

/// `tiflash_test.go:122::TestAggPushDownApplyAll`.
#[test]
#[ignore = "go-parity-gap: aggregate pushdown, enforced MPP, TiFlash replicas, and correlated ALL subqueries require the TiFlash planner/storage path"]
fn agg_pushdown_apply_all_returns_matching_rows() {}

/// `tiflash_test.go:144::TestReadUnsigedPK`.
#[test]
#[ignore = "go-parity-gap: unsigned clustered-key TiFlash reads and MPP joins require replica/engine selection and distributed storage"]
fn read_unsigned_primary_keys_through_tiflash() {}
