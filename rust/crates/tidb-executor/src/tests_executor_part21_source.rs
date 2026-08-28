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

//! Source-anchored carriers for `pkg/executor.part21`, items 1201–1260 of the
//! deterministic `origin/master` executor-test enumeration. The Go sources
//! exercise session/domain transactions, accounts and privileges, statistics,
//! region metadata, and TiFlash/MPP execution in addition to executor calls.
//! This executor-only crate has no equivalent live session, distributed KV/PD,
//! TiFlash, or failpoint integration seam, so unsupported behavior is kept as
//! explicit ignored parity evidence rather than approximated with local tests.

/// `pkg/executor/test/seqtest/seq_executor_test.go:1196::TestInsertFromSelectConflictRetryAutoID`.
/// The Go body runs concurrent `INSERT ... SELECT ... ON DUPLICATE KEY UPDATE`
/// through `InsertValues.doBatchInsert` (`pkg/executor/insert_common.go:640`)
/// and conflict prefetch (`pkg/executor/insert.go:156`), requiring concurrent
/// session transaction retry and auto-ID allocation.
// go-parity-gap: concurrent INSERT SELECT conflict retries and auto-ID allocation are not modeled by tidb-executor.
#[test]
#[ignore = "go-parity-gap: concurrent INSERT SELECT conflict retries and auto-ID allocation are not modeled by tidb-executor"]
fn insert_from_select_conflict_retry_preserves_auto_id() {}

/// `pkg/executor/test/seqtest/seq_executor_test.go:1247::TestAutoRandRecoverTable`.
/// The Go body recovers a dropped table and checks auto-random rebasing through
/// the insert allocator (`pkg/executor/insert_common.go:1261`) and DDL recovery
/// (`pkg/ddl/executor.go:6865`), with emulator-GC and failpoint state.
// go-parity-gap: RECOVER TABLE, emulator GC, DDL history, failpoints, and auto-random rebasing require the session/domain DDL tier.
#[test]
#[ignore = "go-parity-gap: RECOVER TABLE, emulator GC, DDL history, failpoints, and auto-random rebasing require the session/domain DDL tier"]
fn auto_random_recover_table_rebases_handles() {}

/// `pkg/executor/test/seqtest/seq_executor_test.go:1298::TestOOMPanicInHashJoinWhenFetchBuildRows`.
/// The Go body injects `errorFetchBuildSideRowsMockOOMPanic` in the hash-join
/// build fetch path (`pkg/executor/join/hash_join_base.go:352`) and observes
/// the error through the session result-set boundary.
// go-parity-gap: the Go hash-join failpoint and session-level panic/error path are unavailable in tidb-executor.
#[test]
#[ignore = "go-parity-gap: the Go hash-join failpoint and session-level panic/error path are unavailable in tidb-executor"]
fn hash_join_build_fetch_oom_is_reported() {}

/// `pkg/executor/test/seqtest/seq_executor_test.go:1315::TestIssue18744`.
/// The Go body enables `testIndexHashJoinOuterWorkerErr` at the index-hash-join
/// outer worker (`pkg/executor/join/index_lookup_hash_join.go:380`) and checks
/// that the worker error reaches the query result.
// go-parity-gap: index-hash-join failpoint injection and distributed worker error propagation are unported.
#[test]
#[ignore = "go-parity-gap: index-hash-join failpoint injection and distributed worker error propagation are unported"]
fn issue18744_index_hash_join_worker_error_is_returned() {}

/// `pkg/executor/test/seqtest/seq_executor_test.go:1364::TestAnalyzeNextRawErrorNoLeak`.
/// The Go body executes `AnalyzeExec` (`pkg/executor/analyze.go:300`) with the
/// `distsql/mockNextRawError` failpoint and checks the returned raw-result error
/// plus worker cleanup.
// go-parity-gap: ANALYZE execution, distsql raw-result delivery, failpoints, and worker cleanup are not executor-local.
#[test]
#[ignore = "go-parity-gap: ANALYZE execution, distsql raw-result delivery, failpoints, and worker cleanup are not executor-local"]
fn analyze_raw_result_error_does_not_leak_workers() {}

/// `pkg/executor/test/showtest/main_test.go:26::TestMain` only sets global test
/// configuration, enables failpoints, and installs goleak before the suite.
// skipped-reason: Go TestMain is suite bootstrap and has no behavior to port.
#[test]
#[ignore = "skipped-reason: Go TestMain is suite bootstrap and has no behavior to port"]
fn showtest_main_is_bootstrap_only() {}

/// `pkg/executor/test/showtest/show_test.go:46::TestShowCreateTablePlacement`.
/// The Go body checks placement-policy rendering in `ShowExec` (`pkg/executor/show.go:132`)
/// after placement DDL has populated domain metadata.
// go-parity-gap: placement-policy DDL, domain metadata, and SHOW CREATE rendering require the session/domain tier.
#[test]
#[ignore = "go-parity-gap: placement-policy DDL, domain metadata, and SHOW CREATE rendering require the session/domain tier"]
fn show_create_table_renders_placement_policies() {}

/// `pkg/executor/test/showtest/show_test.go:209::TestShowVisibility`.
/// The Go body exercises `ShowExec.fetchShowDatabases` (`pkg/executor/show.go:446`)
/// and table visibility after authenticated grants are changed.
// go-parity-gap: authenticated users, grants, and SHOW DATABASES/TABLES visibility are not modeled by this crate.
#[test]
#[ignore = "go-parity-gap: authenticated users, grants, and SHOW DATABASES/TABLES visibility are not modeled by this crate"]
fn show_visibility_tracks_table_privileges() {}

/// `pkg/executor/test/showtest/show_test.go:245::TestShowWarnings`.
/// The Go body observes statement warnings produced by execution and consumed by
/// SHOW through `ShowExec.Next` (`pkg/executor/show.go:132`), including warning
/// count reset semantics.
// go-parity-gap: session statement diagnostics and SHOW WARNINGS state are not exposed by tidb-executor.
#[test]
#[ignore = "go-parity-gap: session statement diagnostics and SHOW WARNINGS state are not exposed by tidb-executor"]
fn show_warnings_renders_statement_diagnostics() {}

/// `pkg/executor/test/showtest/show_test.go:279::TestShowWarningsForExprPushdown`.
/// The Go body forces TiFlash planning and checks pushdown warnings emitted while
/// `ShowExec`/planner execution handles unsupported expressions (`pkg/executor/show.go:132`).
// go-parity-gap: TiFlash planner selection and session warning state have no equivalent Rust seam.
#[test]
#[ignore = "go-parity-gap: TiFlash planner selection and session warning state have no equivalent Rust seam"]
fn show_warnings_reports_unsupported_tiflash_pushdown() {}

/// `pkg/executor/test/showtest/show_test.go:315::TestShowGrantsPrivilege`.
/// The Go body checks `SHOW GRANTS` authorization in the simple-statement path
/// (`pkg/executor/simple.go:146`) for current and other users.
// go-parity-gap: authenticated account metadata and SHOW GRANTS privilege checks are owned by tidb-session.
#[test]
#[ignore = "go-parity-gap: authenticated account metadata and SHOW GRANTS privilege checks are owned by tidb-session"]
fn show_grants_enforces_privilege_visibility() {}

/// `pkg/executor/test/showtest/show_test.go:330::TestShowStatsPrivilege`.
/// The Go body checks authorization before the statistics SHOW executors are
/// run (`pkg/executor/show_stats.go:1`), then grants access to mysql statistics tables.
// go-parity-gap: session privilege management and domain statistics SHOW execution are unported.
#[test]
#[ignore = "go-parity-gap: session privilege management and domain statistics SHOW execution are unported"]
fn show_stats_enforces_privileges() {}

/// `pkg/executor/test/showtest/show_test.go:364::TestShowStatsExtendedRemoved`.
/// The Go body asserts the removed-feature error from SHOW dispatch in
/// `SimpleExec` (`pkg/executor/simple.go:146`).
// go-parity-gap: the Go SHOW dispatcher and its MySQL-compatible removed-feature error are not exposed here.
#[test]
#[ignore = "go-parity-gap: the Go SHOW dispatcher and its MySQL-compatible removed-feature error are not exposed here"]
fn show_stats_extended_reports_removed_feature() {}

/// `pkg/executor/test/showtest/show_test.go:371::TestIssue18878`.
/// The Go body changes authenticated identity and checks account matching in
/// `SHOW GRANTS`, implemented by the simple executor (`pkg/executor/simple.go:146`).
// go-parity-gap: authenticated/current-user identity and host-specific grant lookup require tidb-session.
#[test]
#[ignore = "go-parity-gap: authenticated/current-user identity and host-specific grant lookup require tidb-session"]
fn issue18878_resolves_authenticated_grant_identity() {}

/// `pkg/executor/test/showtest/show_test.go:390::TestIssue17794`.
/// The Go body selects grants by host pattern after authentication through the
/// simple statement executor (`pkg/executor/simple.go:146`).
// go-parity-gap: host-pattern account authentication and SHOW GRANTS are not modeled in tidb-executor.
#[test]
#[ignore = "go-parity-gap: host-pattern account authentication and SHOW GRANTS are not modeled in tidb-executor"]
fn issue17794_preserves_host_specific_grants() {}

/// `pkg/executor/test/showtest/show_test.go:402::TestIssue10549`.
/// The Go body creates roles, default roles, and grants, then reads them through
/// SHOW execution (`pkg/executor/simple.go:220` and `pkg/executor/show.go:132`).
// go-parity-gap: role graph, default-role metadata, authenticated visibility, and SHOW GRANTS require tidb-session.
#[test]
#[ignore = "go-parity-gap: role graph, default-role metadata, authenticated visibility, and SHOW GRANTS require tidb-session"]
fn issue10549_renders_role_grants_and_database_visibility() {}

/// `pkg/executor/test/showtest/show_test.go:419::TestIssue11165`.
/// The Go body exercises SET DEFAULT ROLE handlers (`pkg/executor/simple.go:220`)
/// for missing users and an authenticated manager.
// go-parity-gap: account roles and SET DEFAULT ROLE session behavior are unported.
#[test]
#[ignore = "go-parity-gap: account roles and SET DEFAULT ROLE session behavior are unported"]
fn issue11165_updates_default_roles() {}

/// `pkg/executor/test/showtest/show_test.go:434::TestShow2`.
/// The Go body combines global variables, columns, views, sequences, and table
/// status through `ShowExec` (`pkg/executor/show.go:132`) and domain infoschema.
// go-parity-gap: this broad SHOW suite requires session variables, domain metadata, views, sequences, and protocol formatting.
#[test]
#[ignore = "go-parity-gap: this broad SHOW suite requires session variables, domain metadata, views, sequences, and protocol formatting"]
fn show2_covers_metadata_and_variable_views() {}

/// `pkg/executor/test/showtest/show_test.go:561::TestShowCreateUser`.
/// The Go body checks authentication plugins, TLS requirements, account state,
/// password policy, and attributes rendered by SHOW (`pkg/executor/show.go:132`).
// go-parity-gap: account/authentication metadata and SHOW CREATE USER are session account surfaces.
#[test]
#[ignore = "go-parity-gap: account/authentication metadata and SHOW CREATE USER are session account surfaces"]
fn show_create_user_renders_account_metadata() {}

/// `pkg/executor/test/showtest/show_test.go:665::TestUnprivilegedShow`.
/// The Go body verifies table-status visibility after authentication and grant
/// changes via `ShowExec.fetchShowTableStatus` (`pkg/executor/show.go:636`).
// go-parity-gap: authenticated table privileges and domain table metadata are not available here.
#[test]
#[ignore = "go-parity-gap: authenticated table privileges and domain table metadata are not available here"]
fn unprivileged_show_hides_ungranted_tables() {}

/// `pkg/executor/test/showtest/show_test.go:692::TestCollation`.
/// The Go body checks SHOW COLLATION rows and MySQL result metadata through the
/// SHOW executor (`pkg/executor/show.go:132`).
// go-parity-gap: the session SHOW executor and protocol field metadata are unported.
#[test]
#[ignore = "go-parity-gap: the session SHOW executor and protocol field metadata are unported"]
fn show_collation_reports_mysql_field_types() {}

/// `pkg/executor/test/showtest/show_test.go:708::TestShowTableStatus`.
/// The Go body renders table timestamps, partition metadata, and comments through
/// `ShowExec.fetchShowTableStatus` (`pkg/executor/show.go:636`).
// go-parity-gap: schema lease/domain timestamps, partition metadata, and SHOW result formatting are unported.
#[test]
#[ignore = "go-parity-gap: schema lease/domain timestamps, partition metadata, and SHOW result formatting are unported"]
fn show_table_status_renders_table_and_partition_metadata() {}

/// `pkg/executor/test/showtest/show_test.go:774::TestAutoRandomBase`.
/// The Go body observes allocator metadata through SHOW TABLE/NEXT_ROW_ID and
/// the auto-random insert path (`pkg/executor/insert_common.go:1261`).
// go-parity-gap: domain auto-ID allocator state and SHOW NEXT_ROW_ID are not modeled by tidb-executor.
#[test]
#[ignore = "go-parity-gap: domain auto-ID allocator state and SHOW NEXT_ROW_ID are not modeled by tidb-executor"]
fn show_auto_random_base_reports_allocator_state() {}

/// `pkg/executor/test/showtest/show_test.go:808::TestAutoRandomWithLargeSignedShowTableRegions`.
/// The Go body combines signed auto-random handles with SHOW TABLE REGIONS and
/// distributed region metadata (`pkg/executor/show.go:132`).
// go-parity-gap: SHOW TABLE REGIONS, TiKV region metadata, and auto-ID allocator state are unported.
#[test]
#[ignore = "go-parity-gap: SHOW TABLE REGIONS, TiKV region metadata, and auto-ID allocator state are unported"]
fn show_auto_random_regions_handles_large_signed_ids() {}

/// `pkg/executor/test/showtest/show_test.go:828::TestShowEscape`.
/// The Go body checks escaping in SHOW output generated by `ShowExec.Next`
/// (`pkg/executor/show.go:132`).
// go-parity-gap: session SHOW output and protocol result formatting are not exposed by this crate.
#[test]
#[ignore = "go-parity-gap: session SHOW output and protocol result formatting are not exposed by this crate"]
fn show_escape_preserves_special_characters() {}

/// `pkg/executor/test/showtest/show_test.go:858::TestShowClusterConfig`.
/// The Go body reads configuration from discovered servers through SHOW
/// dispatch (`pkg/executor/show.go:132`).
// go-parity-gap: server discovery and cluster configuration RPC fan-out are unported.
#[test]
#[ignore = "go-parity-gap: server discovery and cluster configuration RPC fan-out are unported"]
fn show_cluster_config_reads_server_configuration() {}

/// `pkg/executor/test/showtest/show_test.go:893::TestShowConfig`.
/// The Go body reads effective server configuration through SHOW dispatch
/// (`pkg/executor/show.go:132`).
// go-parity-gap: server configuration state is outside the executor-only crate.
#[test]
#[ignore = "go-parity-gap: server configuration state is outside the executor-only crate"]
fn show_config_reads_effective_configuration() {}

/// `pkg/executor/test/showtest/show_test.go:912::TestShowCreateTableWithIntegerDisplayLengthWarnings`.
/// The Go body checks DDL warning publication and subsequent SHOW CREATE output
/// (`pkg/executor/show.go:132`).
// go-parity-gap: SQL mode/DDL warning state and SHOW CREATE session diagnostics are unported.
#[test]
#[ignore = "go-parity-gap: SQL mode/DDL warning state and SHOW CREATE session diagnostics are unported"]
fn show_create_table_reports_integer_display_length_warnings() {}

/// `pkg/executor/test/showtest/show_test.go:1001::TestShowVar`.
/// The Go body enumerates system variables and checks session/global scope through
/// SHOW dispatch (`pkg/executor/show.go:132`).
// go-parity-gap: session/global sysvars and their SHOW VARIABLES surface are unported.
#[test]
#[ignore = "go-parity-gap: session/global sysvars and their SHOW VARIABLES surface are unported"]
fn show_var_reads_session_and_global_variables() {}

/// `pkg/executor/test/showtest/show_test.go:1056::TestShowCreatePlacementPolicy`.
/// The Go body creates, alters, drops, and renders placement policies through
/// SHOW execution (`pkg/executor/show_placement.go:1777`).
// go-parity-gap: placement-policy DDL/domain state and SHOW CREATE PLACEMENT POLICY are unported.
#[test]
#[ignore = "go-parity-gap: placement-policy DDL/domain state and SHOW CREATE PLACEMENT POLICY are unported"]
fn show_create_placement_policy_renders_policy_options() {}

/// `pkg/executor/test/showtest/show_test.go:1075::TestShowLimitReturnRow`.
/// The Go body applies `sql_select_limit` to prepared SHOW and SELECT execution
/// through `ShowExec.Next` (`pkg/executor/show.go:132`).
// go-parity-gap: session SQL_SELECT_LIMIT and PREPARE/EXECUTE result filtering are unported.
#[test]
#[ignore = "go-parity-gap: session SQL_SELECT_LIMIT and PREPARE/EXECUTE result filtering are unported"]
fn show_limit_return_row_applies_to_show_and_select() {}

/// `pkg/executor/test/simpletest/main_test.go:23::TestMain` installs goleak and
/// suite hooks but does not assert SQL behavior.
// skipped-reason: Go TestMain is suite bootstrap and has no behavior to port.
#[test]
#[ignore = "skipped-reason: Go TestMain is suite bootstrap and has no behavior to port"]
fn simpletest_main_is_bootstrap_only() {}

/// `pkg/executor/test/simpletest/simple_test.go:43::TestStarterUsernamePolicyInSimpleExec`.
/// The Go body exercises user/role creation and policy checks in `SimpleExec`
/// (`pkg/executor/simple.go:146`) under starter deploy configuration.
// go-parity-gap: starter deploy mode, accounts, roles, and privilege checks require tidb-session/server state.
#[test]
#[ignore = "go-parity-gap: starter deploy mode, accounts, roles, and privilege checks require tidb-session/server state"]
fn starter_username_policy_is_enforced() {}

/// `pkg/executor/test/simpletest/simple_test.go:109::TestUserWithSetNames`.
/// The Go body changes session charset state before CREATE/ALTER/RENAME USER in
/// `SimpleExec` (`pkg/executor/simple.go:146`).
// go-parity-gap: SET NAMES, authentication encoding, and account DDL require session charset/privilege state.
#[test]
#[ignore = "go-parity-gap: SET NAMES, authentication encoding, and account DDL require session charset/privilege state"]
fn user_passwords_follow_set_names_encoding() {}

/// `pkg/executor/test/simpletest/simple_test.go:130::TestTransaction`.
/// The Go body checks BEGIN/COMMIT/ROLLBACK and implicit DDL commits through
/// `SimpleExec` (`pkg/executor/simple.go:146`) and session transaction state.
// go-parity-gap: session transaction state and implicit-commit coordination are not modeled by tidb-executor.
#[test]
#[ignore = "go-parity-gap: session transaction state and implicit-commit coordination are not modeled by tidb-executor"]
fn transaction_boundaries_commit_and_rollback_rows() {}

/// `pkg/executor/test/simpletest/simple_test.go:164::TestRole`.
/// The Go body exercises CREATE/DROP/GRANT/REVOKE/SET ROLE handlers in
/// `SimpleExec` (`pkg/executor/simple.go:220`) and mysql role tables.
// go-parity-gap: role graph, default-role metadata, and authenticated SET ROLE state are unported.
#[test]
#[ignore = "go-parity-gap: role graph, default-role metadata, and authenticated SET ROLE state are unported"]
fn role_grants_and_default_roles_are_maintained() {}

/// `pkg/executor/test/simpletest/simple_test.go:261::TestMaxUserConnections`.
/// The Go body checks the max-user-connections sysvar and account DDL handled by
/// `SimpleExec` (`pkg/executor/simple.go:146`).
// go-parity-gap: global/session sysvars, account metadata, and CREATE USER privilege enforcement are unported.
#[test]
#[ignore = "go-parity-gap: global/session sysvars, account metadata, and CREATE USER privilege enforcement are unported"]
fn max_user_connections_is_clamped_and_enforced() {}

/// `pkg/executor/test/simpletest/simple_test.go:322::TestUser`.
/// The Go body covers CREATE/ALTER/DROP USER, authentication plugins, warnings,
/// and roles in `SimpleExec` (`pkg/executor/simple.go:146`).
// go-parity-gap: account DDL, authentication plugins, warnings, and role metadata require tidb-session.
#[test]
#[ignore = "go-parity-gap: account DDL, authentication plugins, warnings, and role metadata require tidb-session"]
fn user_account_ddl_matches_mysql_behavior() {}

/// `pkg/executor/test/simpletest/simple_test.go:532::TestAlterUserPreservesRequire`.
/// The Go body checks TLS/token requirements survive attribute-only ALTER USER
/// operations in `SimpleExec` (`pkg/executor/simple.go:2022`).
// go-parity-gap: account authentication/TLS metadata and ALTER USER persistence are unported.
#[test]
#[ignore = "go-parity-gap: account authentication/TLS metadata and ALTER USER persistence are unported"]
fn alter_user_preserves_require_attributes() {}

/// `pkg/executor/test/simpletest/simple_test.go:572::TestSetPwd`.
/// The Go body checks SET PASSWORD handling and privilege errors in the simple
/// executor (`pkg/executor/simple.go:2937`).
// go-parity-gap: authenticated account state, password plugins, and privilege checks require tidb-session.
#[test]
#[ignore = "go-parity-gap: authenticated account state, password plugins, and privilege checks require tidb-session"]
fn set_password_updates_the_authenticated_account() {}

/// `pkg/executor/test/simpletest/simple_test.go:624::TestFlushPrivilegesPanic`.
/// The Go body boots a session with SkipGrantTable and executes FLUSH PRIVILEGES
/// through `SimpleExec` (`pkg/executor/simple.go:146`).
// go-parity-gap: grant-table bootstrap configuration and FLUSH PRIVILEGES lifecycle are not executor-local.
#[test]
#[ignore = "go-parity-gap: grant-table bootstrap configuration and FLUSH PRIVILEGES lifecycle are not executor-local"]
fn flush_privileges_is_safe_without_grant_tables() {}

/// `pkg/executor/test/simpletest/simple_test.go:647::TestDropPartitionStats`.
/// The Go body runs ANALYZE/DROP STATS against partition histograms through the
/// analyze executor (`pkg/executor/analyze.go:300`) and statistics domain.
// go-parity-gap: ANALYZE, persisted statistics histograms, partition stats, and DROP STATS require tidb-domain.
#[test]
#[ignore = "go-parity-gap: ANALYZE, persisted statistics histograms, partition stats, and DROP STATS require tidb-domain"]
fn drop_partition_stats_removes_partition_histograms() {}

/// `pkg/executor/test/simpletest/simple_test.go:715::TestDropStats`.
/// The Go body analyzes a table, drops its stats, and inspects the statistics
/// handle updated by the executor (`pkg/executor/analyze.go:300`).
// go-parity-gap: statistics handles, ANALYZE persistence, and DROP STATS are outside this crate boundary.
#[test]
#[ignore = "go-parity-gap: statistics handles, ANALYZE persistence, and DROP STATS are outside this crate boundary"]
fn drop_stats_resets_table_statistics() {}

/// `pkg/executor/test/simpletest/simple_test.go:770::TestDropStatsForMultipleTable`.
/// The Go body analyzes and drops statistics for multiple tables through the
/// analyze executor (`pkg/executor/analyze.go:300`).
// go-parity-gap: multi-table statistics analysis and DROP STATS domain state are unported.
#[test]
#[ignore = "go-parity-gap: multi-table statistics analysis and DROP STATS domain state are unported"]
fn drop_stats_resets_multiple_table_statistics() {}

/// `pkg/executor/test/simpletest/simple_test.go:845::TestKillStmt`.
/// The Go body routes KILL through the simple-statement executor
/// (`pkg/executor/simple.go:146`) and live server sessions.
// go-parity-gap: connection IDs, global-kill routing, and session statement cancellation are unported.
#[test]
#[ignore = "go-parity-gap: connection IDs, global-kill routing, and session statement cancellation are unported"]
fn kill_statement_routes_connection_requests() {}

/// `pkg/executor/test/simpletest/simple_test.go:899::TestSelectWhereInvalidDSTTime`.
/// The Go body checks timestamp conversion warnings during SELECT execution and
/// session time-zone handling in the query path (`pkg/executor/show.go:132`).
// go-parity-gap: session time zones, timestamp warning policy, and SHOW WARNINGS are not executor-local.
#[test]
#[ignore = "go-parity-gap: session time zones, timestamp warning policy, and SHOW WARNINGS are not executor-local"]
fn select_where_handles_invalid_dst_timestamps() {}

/// `pkg/executor/test/splittest/main_test.go:23::TestMain` only installs suite
/// configuration and leak-detection hooks.
// skipped-reason: Go TestMain is suite bootstrap and has no behavior to port.
#[test]
#[ignore = "skipped-reason: Go TestMain is suite bootstrap and has no behavior to port"]
fn splittest_main_is_bootstrap_only() {}

/// `pkg/executor/test/splittest/split_table_test.go:41::TestClusterIndexShowTableRegion`.
/// The Go body invokes split/table-region execution (`pkg/executor/split.go:222`)
/// and reads TiKV region metadata for clustered indexes.
// go-parity-gap: clustered-index region splitting, SHOW TABLE REGIONS, and TiKV metadata are unported.
#[test]
#[ignore = "go-parity-gap: clustered-index region splitting, SHOW TABLE REGIONS, and TiKV metadata are unported"]
fn cluster_index_show_table_region_reports_regions() {}

/// `pkg/executor/test/splittest/split_table_test.go:84::TestShowTableRegion`.
/// The Go body checks table/index/partition region output produced by split
/// executors (`pkg/executor/split.go:45` and `pkg/executor/split.go:222`).
// go-parity-gap: region split commands, PD/TiKV region metadata, and SHOW TABLE REGIONS are unported.
#[test]
#[ignore = "go-parity-gap: region split commands, PD/TiKV region metadata, and SHOW TABLE REGIONS are unported"]
fn show_table_region_reports_split_ranges() {}

/// `pkg/executor/test/splittest/split_table_test.go:613::BenchmarkLocateRegion`.
/// The Go benchmark measures region-cache range splitting after SplitTableRegion
/// operations (`pkg/executor/split.go:222`).
// skipped-reason: Go benchmark measures PD/TiKV region location latency and the assigned gate excludes benchmarks.
#[test]
#[ignore = "skipped-reason: Go benchmark measures PD/TiKV region location latency and the assigned gate excludes benchmarks"]
fn benchmark_locate_region_is_storage_bound() {}

/// `pkg/executor/test/splittest/split_table_test.go:644::TestBenchDaily` only
/// invokes the benchmark helper `BenchmarkLocateRegion`.
// skipped-reason: this Go test is a benchmark wrapper with no independent behavior to port.
#[test]
#[ignore = "skipped-reason: this Go test is a benchmark wrapper with no independent behavior to port"]
fn split_table_daily_benchmark_is_storage_bound() {}

/// `pkg/executor/test/tiflashtest/main_test.go:27::TestMain` configures TiFlash
/// test nodes, failpoints, and leak-detection hooks before the suite.
// skipped-reason: Go TestMain is suite bootstrap and has no behavior to port.
#[test]
#[ignore = "skipped-reason: Go TestMain is suite bootstrap and has no behavior to port"]
fn tiflashtest_main_is_bootstrap_only() {}

/// `pkg/executor/test/tiflashtest/tiflash_test.go:68::TestNonsupportCharsetTable`.
/// The Go body checks TiFlash replica DDL validation in the DDL executor
/// (`pkg/ddl/executor.go:4055`) for GBK and UTF-8 tables.
// go-parity-gap: TiFlash replica metadata, DDL capability validation, and domain state are unported.
#[test]
#[ignore = "go-parity-gap: TiFlash replica metadata, DDL capability validation, and domain state are unported"]
fn nonsupport_charset_table_rejects_gbk_tiflash_replica() {}

/// `pkg/executor/test/tiflashtest/tiflash_test.go:83::TestReadPartitionTable`.
/// The Go body reads partitioned rows from TiFlash with union scan, dynamic
/// pruning, and batch cop through MPP table reads (`pkg/executor/mpp_gather.go:37`).
// go-parity-gap: TiFlash replicas, MPP/storage reads, partition pruning, transactions, and union scan are unported.
#[test]
#[ignore = "go-parity-gap: TiFlash replicas, MPP/storage reads, partition pruning, transactions, and union scan are unported"]
fn read_partition_table_uses_tiflash_and_union_scan() {}

/// `pkg/executor/test/tiflashtest/tiflash_test.go:122::TestAggPushDownApplyAll`.
/// The Go body forces MPP and checks an aggregate/ALL query over TiFlash
/// replicas via `useMPPExecution` (`pkg/executor/mpp_gather.go:37`).
// go-parity-gap: TiFlash replica selection, aggregate pushdown, MPP, and correlated ALL execution are unported.
#[test]
#[ignore = "go-parity-gap: TiFlash replica selection, aggregate pushdown, MPP, and correlated ALL execution are unported"]
fn agg_pushdown_apply_all_returns_matching_rows() {}

/// `pkg/executor/test/tiflashtest/tiflash_test.go:144::TestReadUnsigedPK`.
/// The Go body reads unsigned clustered keys and range predicates from TiFlash
/// through MPP table reads (`pkg/executor/mpp_gather.go:37`).
// go-parity-gap: unsigned TiFlash clustered-key reads, replica metadata, and distributed joins are unported.
#[test]
#[ignore = "go-parity-gap: unsigned TiFlash clustered-key reads, replica metadata, and distributed joins are unported"]
fn read_unsigned_primary_keys_through_tiflash() {}

/// `pkg/executor/test/tiflashtest/tiflash_test.go:185::TestJoinRace`.
/// The Go body repeatedly executes grouped joins over TiFlash replicas through
/// MPP gathering (`pkg/executor/mpp_gather.go:37`) to pin stable results.
// go-parity-gap: TiFlash/MPP task scheduling, replica reads, and session planner variables are unported.
#[test]
#[ignore = "go-parity-gap: TiFlash/MPP task scheduling, replica reads, and session planner variables are unported"]
fn join_race_is_stable_under_tiflash_mpp() {}

/// `pkg/executor/test/tiflashtest/tiflash_test.go:215::TestMppExecution`.
/// The Go body exercises MPP joins, aggregation, projections, task IDs, and
/// decimal comparisons through `useMPPExecution` (`pkg/executor/mpp_gather.go:37`).
// go-parity-gap: MPP coordinator/task lifecycle, TiFlash replicas, and session execution state are unported.
#[test]
#[ignore = "go-parity-gap: MPP coordinator/task lifecycle, TiFlash replicas, and session execution state are unported"]
fn mpp_execution_covers_join_agg_and_task_ids() {}

/// `pkg/executor/test/tiflashtest/tiflash_test.go:303::TestInjectExtraProj`.
/// The Go body checks large-integer AVG and grouped projection results after
/// TiFlash planning via `useMPPExecution` (`pkg/executor/mpp_gather.go:37`).
// go-parity-gap: TiFlash replica execution and MPP projection injection are unported.
#[test]
#[ignore = "go-parity-gap: TiFlash replica execution and MPP projection injection are unported"]
fn inject_extra_projection_preserves_large_integer_avg() {}

/// `pkg/executor/test/tiflashtest/tiflash_test.go:324::TestTiFlashPartitionTableShuffledHashJoin` calls `t.Skip("too slow")`
/// before its TiFlash partitioned shuffled-hash-join body.
// skipped-reason: the authoritative Go test explicitly skips as too slow; no behavior is run to port.
#[test]
#[ignore = "skipped-reason: the authoritative Go test explicitly skips as too slow; no behavior is run to port"]
fn tiflash_partitioned_shuffled_hash_join_is_skipped_as_too_slow() {}
