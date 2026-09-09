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

//! Source-backed carriers for the local fallback slice of `pkg/session`.
//!
//! The assigned `origin/master` ref is not available in this offline worktree;
//! the receipt records that limitation. The checked-out Go snapshot enumerates
//! this slice as items 121--180. The upgrade tests require Go's versioned
//! bootstrap, storage/meta transaction, Domain, DDL, failpoint, and server
//! handler composition. The clustered-index tests require Go snapshot and row
//! encoding internals. The common tests require Go protocol/session metadata,
//! while the prepare-dedup tests require the Go PlanCacheStmt cache. None of
//! those complete seams is exposed by this Rust crate, so these tests are
//! explicit ignored gaps rather than approximations.

#![cfg(test)]

/// Go `pkg/session/test/bootstraptest/boot_test.go:398::TestAnalyzeVersionUpgradeFrom300To500`.
/// It drives `session.BootstrapSession` (`pkg/session/session.go:4291`),
/// `meta.Mutator.FinishBootstrap` (`pkg/meta/meta.go:2061`), and
/// `upgradeToVer80` (`pkg/session/upgrade_def.go:1397`).
#[test]
#[ignore = "go-parity-gap: versioned bootstrap storage and upgrade execution are not transcreated"]
fn test_analyze_version_upgrade_from300_to500() {}

/// Go `pkg/session/test/bootstraptest/boot_test.go:453::TestAnalyzeVersionUpgradeRewritesLegacyV1To2`.
/// It checks the upgrade mutation of `tidb_analyze_version` through
/// `session.BootstrapSession` (`pkg/session/session.go:4291`) and
/// `upgradeToVer255` (`pkg/session/upgrade_def.go:2100`).
#[test]
#[ignore = "go-parity-gap: versioned bootstrap variables and upgrade execution are not transcreated"]
fn test_analyze_version_upgrade_rewrites_legacy_v1_to2() {}

/// Go `pkg/session/test/bootstraptest/boot_test.go:507::TestIndexMergeUpgradeFrom300To540`.
/// It checks upgrade-time `tidb_enable_index_merge` persistence through
/// `session.BootstrapSession` (`pkg/session/session.go:4291`) and
/// `upgradeToVer81` (`pkg/session/upgrade_def.go:1406`).
#[test]
#[ignore = "go-parity-gap: versioned bootstrap variables and upgrade execution are not transcreated"]
fn test_index_merge_upgrade_from300_to540() {}

/// Go `pkg/session/test/bootstraptest/boot_test.go:562::TestIndexMergeUpgradeFrom400To540Enable`.
/// It exercises `session.BootstrapSession` (`pkg/session/session.go:4291`) and
/// the upgrade variable mutation in `upgradeToVer81`
/// (`pkg/session/upgrade_def.go:1406`).
#[test]
#[ignore = "go-parity-gap: versioned bootstrap variables and upgrade execution are not transcreated"]
fn test_index_merge_upgrade_from400_to540_enable() {}

/// Go `pkg/session/test/bootstraptest/boot_test.go:570::TestIndexMergeUpgradeFrom400To540Disable`.
/// It exercises `session.BootstrapSession` (`pkg/session/session.go:4291`) and
/// the upgrade variable mutation in `upgradeToVer81`
/// (`pkg/session/upgrade_def.go:1406`).
#[test]
#[ignore = "go-parity-gap: versioned bootstrap variables and upgrade execution are not transcreated"]
fn test_index_merge_upgrade_from400_to540_disable() {}

/// Go `pkg/session/test/bootstraptest/boot_test.go:641::TestTiDBOptRangeMaxSizeWhenUpgrading`.
/// It checks a missing global variable before and after
/// `session.BootstrapSession` (`pkg/session/session.go:4291`) through
/// `upgradeToVer97` (`pkg/session/upgrade_def.go:1499`).
#[test]
#[ignore = "go-parity-gap: versioned bootstrap variables and upgrade execution are not transcreated"]
fn test_tidb_opt_range_max_size_when_upgrading() {}

/// Go `pkg/session/test/bootstraptest/boot_test.go:703::TestTiDBOptAdvancedJoinHintWhenUpgrading`.
/// It checks the variable introduced by `session.BootstrapSession`
/// (`pkg/session/session.go:4291`) and `upgradeToVer135`
/// (`pkg/session/upgrade_def.go:1607`).
#[test]
#[ignore = "go-parity-gap: versioned bootstrap variables and upgrade execution are not transcreated"]
fn test_tidb_opt_advanced_join_hint_when_upgrading() {}

/// Go `pkg/session/test/bootstraptest/boot_test.go:765::TestTiDBCostModelUpgradeFrom300To650`.
/// It checks the upgrade result from `session.BootstrapSession`
/// (`pkg/session/session.go:4291`) and `upgradeToVer105`
/// (`pkg/session/upgrade_def.go:1548`).
#[test]
#[ignore = "go-parity-gap: versioned bootstrap variables and upgrade execution are not transcreated"]
fn test_tidb_cost_model_upgrade_from300_to650() {}

/// Go `pkg/session/test/bootstraptest/boot_test.go:819::TestTiDBCostModelUpgradeFrom610To650`.
/// It checks preservation of a global variable across
/// `session.BootstrapSession` (`pkg/session/session.go:4291`) and
/// `upgradeToVer105` (`pkg/session/upgrade_def.go:1548`).
#[test]
#[ignore = "go-parity-gap: versioned bootstrap variables and upgrade execution are not transcreated"]
fn test_tidb_cost_model_upgrade_from610_to650() {}

/// Go `pkg/session/test/bootstraptest/boot_test.go:893::TestIndexJoinMultiPatternByUpgrade650To840`.
/// It checks an upgrade-created optimizer variable through
/// `session.BootstrapSession` (`pkg/session/session.go:4291`) and
/// `upgradeToVer215` (`pkg/session/upgrade_def.go:1927`).
#[test]
#[ignore = "go-parity-gap: versioned bootstrap variables and upgrade execution are not transcreated"]
fn test_index_join_multi_pattern_by_upgrade650_to840() {}

/// Go `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go:52::TestUpgradeVersion83AndVersion84`.
/// It inspects bootstrap schemas after `session.BootstrapSession`
/// (`pkg/session/session.go:4291`) and `upgradeToVer83`/`upgradeToVer84`
/// (`pkg/session/upgrade_def.go:1416`, `:1420`).
#[test]
#[ignore = "go-parity-gap: bootstrap schema catalog and storage/meta upgrade path are not transcreated"]
fn test_upgrade_version83_and_version84() {}

/// Go `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go:113::TestMysqlTablesWithoutClusteredPK`.
/// It inspects the bootstrapped information schema produced by
/// `session.BootstrapSession` (`pkg/session/session.go:4291`), including
/// clustered-primary-key metadata.
#[test]
#[ignore = "go-parity-gap: bootstrap system-table catalog and clustered-key metadata are not transcreated"]
fn test_mysql_tables_without_clustered_pk() {}

/// Go `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go:184::TestUpgradeVersion66`.
/// It mutates `meta.Mutator.FinishBootstrap` (`pkg/meta/meta.go:2061`),
/// reruns `session.BootstrapSession` (`pkg/session/session.go:4291`), and
/// exercises `upgradeToVer66` (`pkg/session/upgrade_def.go:1337`).
#[test]
#[ignore = "go-parity-gap: versioned bootstrap storage and upgrade execution are not transcreated"]
fn test_upgrade_version66() {}

/// Go `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go:225::TestUpgradeVersion74`.
/// It checks the `upgradeToVer74` mutation (`pkg/session/upgrade_def.go:1366`)
/// after `session.BootstrapSession` (`pkg/session/session.go:4291`).
#[test]
#[ignore = "go-parity-gap: versioned bootstrap variables and upgrade execution are not transcreated"]
fn test_upgrade_version74() {}

/// Go `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go:279::TestUpgradeVersion75`.
/// It checks the schema mutation in `upgradeToVer75`
/// (`pkg/session/upgrade_def.go:1371`) after `session.BootstrapSession`
/// (`pkg/session/session.go:4291`).
#[test]
#[ignore = "go-parity-gap: versioned bootstrap DDL and storage/meta upgrade path are not transcreated"]
fn test_upgrade_version75() {}

/// Go `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go:327::TestUpgradeVersionMockLatest`.
/// It exercises the mock upgrade hooks and `session.BootstrapSession`
/// (`pkg/session/session.go:4291`) while checking generated system tables.
#[test]
#[ignore = "go-parity-gap: mock latest-version bootstrap and system-table DDL are not transcreated"]
fn test_upgrade_version_mock_latest() {}

/// Go `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go:391::TestUpgradeVersionWithUpgradeHTTPOp`.
/// It coordinates the upgrade handler with `session.BootstrapSession`
/// (`pkg/session/session.go:4291`) and the cluster upgrade state.
#[test]
#[ignore = "go-parity-gap: cluster upgrade handler and versioned bootstrap state are not transcreated"]
fn test_upgrade_version_with_upgrade_http_op() {}

/// Go `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go:445::TestUpgradeVersionWithoutUpgradeHTTPOp`.
/// It checks the no-handler branch around `session.BootstrapSession`
/// (`pkg/session/session.go:4291`) and cluster upgrade state.
#[test]
#[ignore = "go-parity-gap: cluster upgrade handler and versioned bootstrap state are not transcreated"]
fn test_upgrade_version_without_upgrade_http_op() {}

/// Go `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go:496::TestUpgradeVersionForPausedJob`.
/// It combines paused DDL jobs with `session.BootstrapSession`
/// (`pkg/session/session.go:4291`) and the upgrade runner
/// (`pkg/session/upgrade_run.go:32`).
#[test]
#[ignore = "go-parity-gap: DDL job state, failpoints, and bootstrap upgrade runner are not transcreated"]
fn test_upgrade_version_for_paused_job() {}

/// Go `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go:572::TestUpgradeVersionForSystemPausedJob`.
/// It checks system-paused DDL jobs across `session.BootstrapSession`
/// (`pkg/session/session.go:4291`) and the upgrade runner
/// (`pkg/session/upgrade_run.go:32`).
#[test]
#[ignore = "go-parity-gap: DDL job state, failpoints, and bootstrap upgrade runner are not transcreated"]
fn test_upgrade_version_for_system_paused_job() {}

/// Go `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go:638::TestUpgradeVersionForResumeJob`.
/// It checks resume ordering across `session.BootstrapSession`
/// (`pkg/session/session.go:4291`) and the upgrade runner
/// (`pkg/session/upgrade_run.go:32`).
#[test]
#[ignore = "go-parity-gap: DDL job state, failpoints, and bootstrap upgrade runner are not transcreated"]
fn test_upgrade_version_for_resume_job() {}

/// Go `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go:758::TestUpgradeWithPauseDDL`.
/// It checks system-versus-user DDL ordering during `session.BootstrapSession`
/// (`pkg/session/session.go:4291`) with the upgrade runner
/// (`pkg/session/upgrade_run.go:32`).
#[test]
#[ignore = "go-parity-gap: concurrent DDL scheduling, failpoints, and bootstrap upgrade runner are not transcreated"]
fn test_upgrade_with_pause_ddl() {}

/// Go `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go:959::TestUpgradeWithCrossJoinDisabled`.
/// It checks bootstrap while planner state is changed before
/// `session.BootstrapSession` (`pkg/session/session.go:4291`).
#[test]
#[ignore = "go-parity-gap: bootstrap upgrade composition with planner/session globals is not transcreated"]
fn test_upgrade_with_cross_join_disabled() {}

/// Go `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go:975::TestUpgradeBDRPrimary`.
/// It checks BDR role state across `session.BootstrapSession`
/// (`pkg/session/session.go:4291`) and `meta.Mutator.FinishBootstrap`
/// (`pkg/meta/meta.go:2061`).
#[test]
#[ignore = "go-parity-gap: BDR role metadata and versioned bootstrap are not transcreated"]
fn test_upgrade_bdr_primary() {}

/// Go `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go:1005::TestUpgradeBDRSecondary`.
/// It checks the secondary BDR branch across `session.BootstrapSession`
/// (`pkg/session/session.go:4291`) and `meta.Mutator.FinishBootstrap`
/// (`pkg/meta/meta.go:2061`).
#[test]
#[ignore = "go-parity-gap: BDR role metadata and versioned bootstrap are not transcreated"]
fn test_upgrade_bdr_secondary() {}

/// Go `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go:1035::TestUpgradeBindInfo`.
/// It checks bind-info state after `session.BootstrapSession`
/// (`pkg/session/session.go:4291`) and the bootstrap meta transaction
/// (`pkg/meta/meta.go:2061`).
#[test]
#[ignore = "go-parity-gap: bind-info metadata and versioned bootstrap are not transcreated"]
fn test_upgrade_bind_info() {}

/// Go `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go:1100::TestUpgradeVersion260MaskingPolicy`.
/// It drops and recreates a system table through `session.BootstrapSession`
/// (`pkg/session/session.go:4291`) and `upgradeToVer260`
/// (`pkg/session/upgrade_def.go:2144`).
#[test]
#[ignore = "go-parity-gap: system-table DDL and versioned bootstrap are not transcreated"]
fn test_upgrade_version260_masking_policy() {}

/// Go `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go:1157::TestUpgradeWithAnalyzeColumnOptions`.
/// It checks upgrade-time global-variable values through
/// `session.BootstrapSession` (`pkg/session/session.go:4291`) and
/// `upgradeToVer210` (`pkg/session/upgrade_def.go:1873`).
#[test]
#[ignore = "go-parity-gap: versioned bootstrap variables and upgrade execution are not transcreated"]
fn test_upgrade_with_analyze_column_options() {}

/// Go `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go:1343::TestAnalyzeDistsqlConcurrencyByUpgrade750To850`.
/// It checks a variable copied during `session.BootstrapSession`
/// (`pkg/session/session.go:4291`) and `upgradeToVer258`
/// (`pkg/session/upgrade_def.go:2130`).
#[test]
#[ignore = "go-parity-gap: versioned bootstrap variables and upgrade execution are not transcreated"]
fn test_analyze_distsql_concurrency_by_upgrade750_to850() {}

/// Go `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go:1393::TestAutoAnalyzeConcurrencyDefaultOnlyAffectsFreshBootstrap`.
/// It distinguishes fresh bootstrap from `session.BootstrapSession`
/// (`pkg/session/session.go:4291`) upgrade behavior.
#[test]
#[ignore = "go-parity-gap: fresh-versus-upgrade bootstrap variable state is not transcreated"]
fn test_auto_analyze_concurrency_default_only_affects_fresh_bootstrap() {}

/// Go `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go:1420::TestBootstrapInNextGenInvalidSystemTable`.
/// It checks invalid system-table handling during
/// `session.BootstrapSession` (`pkg/session/session.go:4291`).
#[test]
#[ignore = "go-parity-gap: next-gen bootstrap system-table validation is not transcreated"]
fn test_bootstrap_in_next_gen_invalid_system_table() {}

/// Go `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go:1443::TestUpgradeVersion256PlanCacheSkipStatsOnBinding`.
/// It checks binding and plan-cache state after `session.BootstrapSession`
/// (`pkg/session/session.go:4291`) and `upgradeToVer256`
/// (`pkg/session/upgrade_def.go:2121`).
#[test]
#[ignore = "go-parity-gap: binding metadata and versioned bootstrap are not transcreated"]
fn test_upgrade_version256_plan_cache_skip_stats_on_binding() {}

/// Go `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go:1490::TestDefaultAnalyzeBackgroundOnlyAffectsFreshBootstrap`.
/// It distinguishes fresh bootstrap defaults from upgrade behavior in
/// `session.BootstrapSession` (`pkg/session/session.go:4291`).
#[test]
#[ignore = "go-parity-gap: fresh-versus-upgrade bootstrap variable state is not transcreated"]
fn test_default_analyze_background_only_affects_fresh_bootstrap() {}

/// Go `pkg/session/test/bootstraptest/main_test.go:29::TestMain`.
/// It is the Go test harness that calls `testsetup.SetupForCommonTest` and
/// goleak setup, not a behavior test in `pkg/session/session.go`.
#[test]
#[ignore = "go-parity-gap: Go TestMain setup and goleak harness are not a Rust unit-test surface"]
fn test_bootstraptest_main() {}

/// Go `pkg/session/test/bootstraptest2/boot_test.go:32::TestWriteDDLTableVersionToMySQLTiDBWhenUpgradingTo178`.
/// It checks `meta.Mutator.FinishBootstrap` (`pkg/meta/meta.go:2061`),
/// `upgradeToVer178` (`pkg/session/upgrade_def.go:1796`), and the DDL-table
/// version written by `session.BootstrapSession` (`pkg/session/session.go:4291`).
#[test]
#[ignore = "go-parity-gap: versioned bootstrap storage and DDL metadata are not transcreated"]
fn test_write_ddl_table_version_to_mysql_tidb_when_upgrading_to178() {}

/// Go `pkg/session/test/bootstraptest2/boot_test.go:82::TestTiDBUpgradeToVer179`.
/// It checks the schema mutation in `upgradeToVer179`
/// (`pkg/session/upgrade_def.go:1800`) after `session.BootstrapSession`
/// (`pkg/session/session.go:4291`).
#[test]
#[ignore = "go-parity-gap: versioned bootstrap DDL and storage/meta upgrade path are not transcreated"]
fn test_tidb_upgrade_to_ver179() {}

/// Go `pkg/session/test/bootstraptest2/boot_test.go:174::TestTiDBUpgradeWithDistTaskEnable`.
/// It checks distributed-task bootstrap handling through
/// `session.BootstrapSession` (`pkg/session/session.go:4291`) and the
/// versioned upgrade path (`pkg/session/upgrade_run.go:32`).
#[test]
#[ignore = "go-parity-gap: distributed-task metadata and bootstrap upgrade path are not transcreated"]
fn test_tidb_upgrade_with_dist_task_enable() {}

/// Go `pkg/session/test/bootstraptest2/boot_test.go:182::TestTiDBUpgradeWithDistTaskRunning`.
/// It checks running and terminal distributed-task rows during
/// `session.BootstrapSession` (`pkg/session/session.go:4291`).
#[test]
#[ignore = "go-parity-gap: distributed-task metadata and bootstrap upgrade path are not transcreated"]
fn test_tidb_upgrade_with_dist_task_running() {}

/// Go `pkg/session/test/bootstraptest2/boot_test.go:206::TestTiDBUpgradeToVer211`.
/// It checks `upgradeToVer211` (`pkg/session/upgrade_def.go:1883`) after
/// `session.BootstrapSession` (`pkg/session/session.go:4291`).
#[test]
#[ignore = "go-parity-gap: versioned bootstrap DDL and storage/meta upgrade path are not transcreated"]
fn test_tidb_upgrade_to_ver211() {}

/// Go `pkg/session/test/bootstraptest2/boot_test.go:252::TestTiDBUpgradeToVer212`.
/// It checks the schema mutation in `upgradeToVer212`
/// (`pkg/session/upgrade_def.go:1887`) after `session.BootstrapSession`
/// (`pkg/session/session.go:4291`).
#[test]
#[ignore = "go-parity-gap: versioned bootstrap DDL and storage/meta upgrade path are not transcreated"]
fn test_tidb_upgrade_to_ver212() {}

/// Go `pkg/session/test/bootstraptest2/main_test.go:29::TestMain`.
/// It is the Go test harness and goleak setup, not a behavior test in
/// `pkg/session/session.go`.
#[test]
#[ignore = "go-parity-gap: Go TestMain setup and goleak harness are not a Rust unit-test surface"]
fn test_bootstraptest2_main() {}

/// Go `pkg/session/test/clusteredindextest/clustered_index_test.go:40::TestClusteredInsertIgnoreBatchGetKeyCount`.
/// It inspects the Go snapshot through the test-local `SnapCacheSizeGetter`
/// (`pkg/session/test/clusteredindextest/clustered_index_test.go:34`) after
/// session DML, a storage snapshot seam absent from this Rust crate.
#[test]
#[ignore = "go-parity-gap: Go transaction snapshot cache inspection is not transcreated"]
fn test_clustered_insert_ignore_batch_get_key_count() {}

/// Go `pkg/session/test/clusteredindextest/clustered_index_test.go:58::TestClusteredWithOldRowFormat`.
/// It toggles `SessionVars.RowEncoder.Enable` at the Go session boundary
/// (`pkg/session/test/clusteredindextest/clustered_index_test.go:62`) and
/// checks old-row-format DML and clustered indexes.
#[test]
#[ignore = "go-parity-gap: Go row-encoder session state and storage-backed clustered DML are not transcreated"]
fn test_clustered_with_old_row_format() {}

/// Go `pkg/session/test/clusteredindextest/clustered_index_test.go:122::TestPartitionTable`.
/// It compares partitioned clustered-primary-key reads through the Go
/// `testkit` session and partition executor (`pkg/session/test/clusteredindextest/clustered_index_test.go:122`).
#[test]
#[ignore = "go-parity-gap: Go storage-backed clustered partition DML and testkit session are not transcreated"]
fn test_partition_table() {}

/// Go `pkg/session/test/clusteredindextest/main_test.go:30::TestMain`.
/// It enables TiKV failpoints and configures the Go test harness, not a
/// behavior test in `pkg/session/session.go`.
#[test]
#[ignore = "go-parity-gap: Go TestMain, TiKV failpoints, and goleak harness are not a Rust unit-test surface"]
fn test_clusteredindextest_main() {}

/// Go `pkg/session/test/common/common_test.go:31::TestMiscs`.
/// It checks `session.String` (`pkg/session/session.go:1079`) and the
/// `SetValue`/`Value` session metadata APIs (`pkg/session/session.go:3361`).
#[test]
#[ignore = "go-parity-gap: Go session lifecycle metadata and protocol state are not transcreated"]
fn test_miscs() {}

/// Go `pkg/session/test/common/common_test.go:57::TestPrepare`.
/// It exercises `session.PrepareStmt` (`pkg/session/session.go:3153`) and
/// `session.ExecutePreparedStmt` (`pkg/session/session.go:3324`) through the
/// Go testkit session.
#[test]
#[ignore = "go-parity-gap: Go testkit prepared-session protocol and storage execution are not transcreated"]
fn test_prepare() {}

/// Go `pkg/session/test/common/common_test.go:100::TestIndexColumnLength`.
/// It checks `tables.FindIndexByColName` (`pkg/table/tables/tables.go:1565`)
/// against information-schema table metadata.
#[test]
#[ignore = "go-parity-gap: Go table metadata and information-schema index model are not transcreated at this boundary"]
fn test_index_column_length() {}

/// Go `pkg/session/test/common/common_test.go:120::TestTableInfoMeta`.
/// It checks `session.AffectedRows` (`pkg/session/session.go:389`) and
/// `session.LastInsertID` (`pkg/session/session.go:378`) after DML.
#[test]
#[ignore = "go-parity-gap: Go session status metadata over storage-backed DML is not transcreated"]
fn test_table_info_meta() {}

/// Go `pkg/session/test/common/common_test.go:154::TestLastMessage`.
/// It checks protocol last-message formatting after DML and
/// `SetClientCapability` (`pkg/session/session.go:393`).
#[test]
#[ignore = "go-parity-gap: Go protocol last-message and client-capability state are not transcreated"]
fn test_last_message() {}

/// Go `pkg/session/test/common/common_test.go:199::TestQueryString`.
/// It checks the session `QueryString` value while using
/// `PrepareStmt` (`pkg/session/session.go:3153`) and
/// `ExecutePreparedStmt` (`pkg/session/session.go:3324`).
#[test]
#[ignore = "go-parity-gap: Go session query-string metadata and prepared protocol are not transcreated"]
fn test_query_string() {}

/// Go `pkg/session/test/common/common_test.go:229::TestAffectedRows`.
/// It checks `session.AffectedRows` (`pkg/session/session.go:389`) for DML,
/// duplicate-key updates, and no-op updates.
#[test]
#[ignore = "go-parity-gap: Go affected-row protocol state over storage-backed DML is not transcreated"]
fn test_affected_rows() {}

/// Go `pkg/session/test/common/main_test.go:29::TestMain`.
/// It is the Go test setup and goleak harness, not a behavior test in
/// `pkg/session/session.go`.
#[test]
#[ignore = "go-parity-gap: Go TestMain setup and goleak harness are not a Rust unit-test surface"]
fn test_common_main() {}

/// Go `pkg/session/test/common/prepare_dedup_cache_test.go:29::TestPrepareStmtDedupCacheBasic`.
/// It checks the session dedup-cache branch in `PrepareStmt`
/// (`pkg/session/session.go:3184`) and the `PlanCacheStmt` cache insertion
/// (`pkg/session/session.go:3224`).
#[test]
#[ignore = "go-parity-gap: Go PlanCacheStmt prepare dedup cache is not transcreated"]
fn test_prepare_stmt_dedup_cache_basic() {}

/// Go `pkg/session/test/common/prepare_dedup_cache_test.go:58::TestPrepareStmtDedupCacheExecute`.
/// It executes statements returned by the dedup branch in `PrepareStmt`
/// (`pkg/session/session.go:3184`) through `ExecutePreparedStmt`
/// (`pkg/session/session.go:3324`).
#[test]
#[ignore = "go-parity-gap: Go PlanCacheStmt prepare dedup execution is not transcreated"]
fn test_prepare_stmt_dedup_cache_execute() {}

/// Go `pkg/session/test/common/prepare_dedup_cache_test.go:112::TestPrepareStmtDedupCacheSchemaChange`.
/// It checks schema invalidation and rebuild in `rebuildFromPrepareCache`
/// (`pkg/session/session.go:3243`) after a DDL schema change.
#[test]
#[ignore = "go-parity-gap: Go schema-versioned PlanCacheStmt invalidation is not transcreated"]
fn test_prepare_stmt_dedup_cache_schema_change() {}

/// Go `pkg/session/test/common/prepare_dedup_cache_test.go:150::TestPrepareStmtDedupCacheIsolatedByDB`.
/// It checks the database component of `PrepareDedupCacheKey`
/// (`pkg/sessionctx/variable/session.go:2938`) across `PrepareStmt`
/// (`pkg/session/session.go:3153`).
#[test]
#[ignore = "go-parity-gap: Go database-scoped PlanCacheStmt dedup cache is not transcreated"]
fn test_prepare_stmt_dedup_cache_isolated_by_db() {}

/// Go `pkg/session/test/common/prepare_dedup_cache_test.go:183::TestPrepareStmtDedupCachePrepareExecuteCloseLoop`.
/// It repeats `PrepareStmt` (`pkg/session/session.go:3153`),
/// `ExecutePreparedStmt` (`pkg/session/session.go:3324`), and close/drop
/// behavior over the Go PlanCacheStmt dedup cache.
#[test]
#[ignore = "go-parity-gap: Go prepare/execute/close PlanCacheStmt lifecycle is not transcreated"]
fn test_prepare_stmt_dedup_cache_prepare_execute_close_loop() {}

/// Go `pkg/session/test/main_test.go:29::TestMain`.
/// It is the package-level Go test setup and goleak harness, not a behavior
/// test in `pkg/session/session.go`.
#[test]
#[ignore = "go-parity-gap: Go TestMain setup and goleak harness are not a Rust unit-test surface"]
fn test_session_main() {}

/// Go `pkg/session/test/meta/main_test.go:29::TestMain`.
/// It is the meta-suite Go test setup and goleak harness, not a behavior test
/// in `pkg/session/session.go` or `pkg/meta/meta.go`.
#[test]
#[ignore = "go-parity-gap: Go TestMain setup and goleak harness are not a Rust unit-test surface"]
fn test_meta_main() {}
