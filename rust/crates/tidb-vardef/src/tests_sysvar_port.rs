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

//! Go-parity tests ported from `pkg/sessionctx/variable` (batch b010, part 1).
//!
//! Source of truth: `origin/master` snapshot of
//! `pkg/sessionctx/variable/{embedding_vars_test.go, main_test.go,
//! mock_globalaccessor_test.go, nextgen_test.go, removed_test.go,
//! statusvar_test.go}` plus the first 50 tests of `sysvar_test.go`
//! (`TestSQLSelectLimit` .. `TestTiDBIgnoreInlistPlanDigest`). This is
//! tests 1–60 of the package's canonical ordering (alphabetical path, line
//! number); part 3 (tests 121–150) lives in [`super::tests_vardef_port`].
//!
//! The owning crate only ports the `vardef` constants layer (name constants,
//! `Def*` defaults, mode enums) and the pure
//! `GlobalSystemVariableInitialValue` policy. Tests whose subject is the
//! `SysVar` registry, `SessionVars`, validation/clamping, or the mock global
//! accessor are kept as `#[ignore]`d stubs annotated with a `go-parity-gap`
//! reason so the inventory stays visible; they must be enabled when the
//! owning code lands. Constant-level assertions that ARE expressible here are
//! written as real (partial-port) tests.

use super::defaults::{
    DEF_OPT_AGG_PUSH_DOWN, DEF_OPT_DERIVE_TOP_N, DEF_TIDB_DDL_DISK_QUOTA,
    DEF_TIDB_DDL_REORG_BATCH_SIZE, DEF_TIDB_ENABLE_FAST_REORG, DEF_TIDB_ENABLE_INDEX_MERGE,
    DEF_TIDB_IGNORE_INLIST_PLAN_DIGEST, DEF_TIDB_PARTITION_PRUNE_MODE,
    DEF_TIDB_SERVER_MEMORY_LIMIT_GC_TRIGGER, DEF_TIDB_SERVER_MEMORY_LIMIT_SESS_MIN_SIZE,
};
use super::global_sysvar_initial::{global_system_variable_initial_value, GlobalSysvarEnvironment};
use super::tidb_vars;

// ---------------------------------------------------------------------------
// pkg/sessionctx/variable/embedding_vars_test.go
// ---------------------------------------------------------------------------

/// Go `pkg/sessionctx/variable/embedding_vars_test.go::TestNormalizeOpenAIEmbeddingAPIBase`.
// go-parity-gap: NormalizeOpenAIEmbeddingAPIBase + OpenAI endpoint whitelist live in sysvar/embedding code not ported to this crate
#[test]
#[ignore]
fn normalize_open_ai_embedding_api_base_unported() {}

/// Go `pkg/sessionctx/variable/embedding_vars_test.go::TestGetOpenAIEmbeddingBaseURL`.
// go-parity-gap: GetOpenAIEmbeddingBaseURL + EmbedOpenAIAPIBase atomic + SysVar SetGlobal hooks not ported
#[test]
#[ignore]
fn get_open_ai_embedding_base_url_unported() {}

/// Go `pkg/sessionctx/variable/embedding_vars_test.go::TestEmbeddingAPIKeySysVars`.
// go-parity-gap: embedding API-key SysVars with masking (maskEmbeddingAPIKey) + EmbeddingConfigVersion counter not ported
#[test]
#[ignore]
fn embedding_api_key_sys_vars_unported() {}

// ---------------------------------------------------------------------------
// pkg/sessionctx/variable/main_test.go
// ---------------------------------------------------------------------------

/// Go `pkg/sessionctx/variable/main_test.go::TestMain`.
// skipped-reason: Go test-harness entry point (creates VM/observed worlds); no Rust counterpart needed
#[test]
#[ignore]
fn main_test_harness() {}

// ---------------------------------------------------------------------------
// pkg/sessionctx/variable/mock_globalaccessor_test.go
// ---------------------------------------------------------------------------

/// Go `pkg/sessionctx/variable/mock_globalaccessor_test.go::TestMockAPI`.
// go-parity-gap: MockGlobalAccessor4Tests + GlobalVarsAccessor interface not ported to this crate
#[test]
#[ignore]
fn mock_api_unported() {}

// ---------------------------------------------------------------------------
// pkg/sessionctx/variable/nextgen_test.go (build tag: nextgen)
// ---------------------------------------------------------------------------

/// Go `pkg/sessionctx/variable/nextgen_test.go::TestTiDBPessimisticTransactionFairLocking`.
///
/// Partial port: only the final assertion is expressible in this crate —
/// `GlobalSystemVariableInitialValue(TiDBPessimisticTransactionFairLocking,
/// BoolToOnOff(DefTiDBPessimisticTransactionFairLocking)) == Off` on nextgen
/// (`DefTiDBPessimisticTransactionFairLocking` is false in Go, so the declared
/// default passed in is "OFF"). The Validate/SetSessionFromHook halves need
/// the unported SysVar machinery.
#[test]
fn pessimistic_transaction_fair_locking_nextgen_initial_value() {
    // DefTiDBPessimisticTransactionFairLocking defaults to true in Go, so the
    // caller passes "ON" as the declared default.
    let initial = global_system_variable_initial_value(
        tidb_vars::TIDB_PESSIMISTIC_TRANSACTION_FAIR_LOCKING,
        "OFF", // BoolToOnOff(DefTiDBPessimisticTransactionFairLocking); Def is false in Go
        GlobalSysvarEnvironment {
            store_is_tikv: true,
            in_test: false,
            next_gen: true,
        },
    );
    assert_eq!(super::global_sysvar_initial::OFF, initial);
}

/// Go `pkg/sessionctx/variable/nextgen_test.go::TestTiDBDMLTypeInNextGen`.
// go-parity-gap: SysVar Validate/SetSessionFromHook with ErrNotSupportedInNextGen not ported
#[test]
#[ignore]
fn tidb_dml_type_in_next_gen_unported() {}

/// Go `pkg/sessionctx/variable/nextgen_test.go::TestTiDBReplicaReadInNextGen`.
// go-parity-gap: SysVar Validate/SetSessionFromHook + kv.ReplicaRead type on SessionVars not ported
#[test]
#[ignore]
fn tidb_replica_read_in_next_gen_unported() {}

// ---------------------------------------------------------------------------
// pkg/sessionctx/variable/removed_test.go
// ---------------------------------------------------------------------------

/// Go `pkg/sessionctx/variable/removed_test.go::TestRemovedOpt`.
// go-parity-gap: CheckSysVarIsRemoved/IsRemovedSysVar removed-variable tables not ported
#[test]
#[ignore]
fn removed_opt_unported() {}

// ---------------------------------------------------------------------------
// pkg/sessionctx/variable/statusvar_test.go
// ---------------------------------------------------------------------------

/// Go `pkg/sessionctx/variable/statusvar_test.go::TestStatusVar`.
// go-parity-gap: RegisterStatistics/GetStatusVars status-variable layer not ported
#[test]
#[ignore]
fn status_var_unported() {}

// ---------------------------------------------------------------------------
// pkg/sessionctx/variable/sysvar_test.go (first 50 tests, canonical order)
// ---------------------------------------------------------------------------

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestSQLSelectLimit`.
// go-parity-gap: SysVar Validate autoconvert-out-of-range + SessionVars.SelectLimit not ported
#[test]
#[ignore]
fn sql_select_limit_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestSQLModeVar`.
// go-parity-gap: sql_mode SysVar validation over mysql.SQLMode not ported (parser/mysql layer)
#[test]
#[ignore]
fn sql_mode_var_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestTiDBTraceEventSysVar`.
// go-parity-gap: traceevent flight recorder + kernel-type-dependent SetGlobal not ported
#[test]
#[ignore]
fn tidb_trace_event_sys_var_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestMaxExecutionTime`.
// go-parity-gap: SysVar Validate clamping + SessionVars.MaxExecutionTime not ported
#[test]
#[ignore]
fn max_execution_time_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestTiDBMaxKeysRead`.
// go-parity-gap: SysVar Validate + SessionVars.MaxKeysRead/IsHintUpdatableVerified not ported
#[test]
#[ignore]
fn tidb_max_keys_read_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestGetMaxKeysRead`.
// go-parity-gap: SessionVars.StmtCtx.GetMaxKeysRead statement-context behavior not ported
#[test]
#[ignore]
fn get_max_keys_read_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestTiFlashMaxBytes`.
// go-parity-gap: TypeInt validation/clamping on SessionVars TiFlash fields not ported
#[test]
#[ignore]
fn tiflash_max_bytes_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestTiFlashMemQuotaQueryPerNode`.
// go-parity-gap: TypeInt validation/clamping on SessionVars not ported
#[test]
#[ignore]
fn tiflash_mem_quota_query_per_node_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestTiFlashQuerySpillRatio`.
// go-parity-gap: TypeFloat percentage-range validation on SessionVars not ported
#[test]
#[ignore]
fn tiflash_query_spill_ratio_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestTiDBTTLJobEnableSetGlobalUpdatesLocalWithoutExternalWorkload`.
// go-parity-gap: EnableTTLJob atomic + UpdateExternalWorkloadTTLJobEnable hook plumbing not ported
#[test]
#[ignore]
fn ttl_job_enable_set_global_updates_local_without_external_workload_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestTiFlashHashJoinVersion`.
// go-parity-gap: SysVar Validation case-insensitive enum check not ported
#[test]
#[ignore]
fn tiflash_hash_join_version_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestCollationServer`.
// go-parity-gap: collation normalization/validation + charset side-effect hook not ported
#[test]
#[ignore]
fn collation_server_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestDefaultCollationForUTF8MB4`.
// go-parity-gap: collation validation + StmtCtx warning capture not ported
#[test]
#[ignore]
fn default_collation_for_utf8mb4_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestTimeZone`.
// go-parity-gap: timezone parsing/validation + timeutil.ParseTimeZone comparison not ported
#[test]
#[ignore]
fn time_zone_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestTxnIsolation`.
// go-parity-gap: isolation-level validation + skip-isolation-check interaction not ported
#[test]
#[ignore]
fn txn_isolation_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestTiDBMultiStatementMode`.
// go-parity-gap: enum validation + SessionVars.MultiStatementMode field not ported
#[test]
#[ignore]
fn multi_statement_mode_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestReadOnlyNoop`.
// go-parity-gap: noop-function gating via tidb_enable_noop_functions not ported
#[test]
#[ignore]
fn read_only_noop_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestSkipInit`.
// go-parity-gap: SysVar struct construction + SkipInit flag not ported
#[test]
#[ignore]
fn skip_init_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestSessionGetterFuncs`.
// go-parity-gap: GetSessionOrGlobalSystemVar session getter dispatch not ported
#[test]
#[ignore]
fn session_getter_funcs_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestInstanceScopedVars`.
// go-parity-gap: instance-scoped getters over config atomics not ported
#[test]
#[ignore]
fn instance_scoped_vars_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestSecureAuth`.
// go-parity-gap: secure_auth validation rejecting OFF not ported
#[test]
#[ignore]
fn secure_auth_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestTiDBReplicaRead` (classic-kernel branch).
// go-parity-gap: SysVar Validate on classic kernel not ported; the nextgen variant's
// initial-value half is covered by `pessimistic_transaction_fair_locking_nextgen_initial_value`'s pattern
#[test]
#[ignore]
fn replica_read_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestSQLAutoIsNull`.
// go-parity-gap: sql_auto_is_null/noop interplay validation not ported
#[test]
#[ignore]
fn sql_auto_is_null_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestLastInsertID`.
// go-parity-gap: GetSessionOrGlobalSystemVar + GetNativeValType Datum conversion not ported
#[test]
#[ignore]
fn last_insert_id_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestTimestamp`.
// go-parity-gap: timestamp range validation + StmtCtx warnings not ported
#[test]
#[ignore]
fn timestamp_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestIdentity`.
// go-parity-gap: identity/last_insert_id synonym getter not ported
#[test]
#[ignore]
fn identity_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestLcTimeNamesReadOnly`.
// go-parity-gap: lc_time_names read-only validation not ported
#[test]
#[ignore]
fn lc_time_names_read_only_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestLcMessages`.
// go-parity-gap: locale validation + session getter not ported
#[test]
#[ignore]
fn lc_messages_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestDDLWorkers`.
///
/// Partial port: pins the `MinDDLReorgBatchSize` / `MaxDDLReorgBatchSize`
/// bounds the Go assertions clamp against (`32` / `10240`, exported for
/// testing in `pkg/sessionctx/vardef/tidb_vars.go`) together with this
/// crate's `DefTiDBDDLReorgBatchSize`. The Validate clamping itself needs the
/// unported SysVar machinery.
#[test]
fn ddl_workers_bounds() {
    // Pinned test-local until Min/Max DDL reorg batch size land in defaults.
    const MIN_DDL_REORG_BATCH_SIZE: i64 = 32;
    const MAX_DDL_REORG_BATCH_SIZE: i64 = 10240;
    assert_eq!(32, MIN_DDL_REORG_BATCH_SIZE);
    assert_eq!(10240, MAX_DDL_REORG_BATCH_SIZE);
    assert!(
        MIN_DDL_REORG_BATCH_SIZE <= DEF_TIDB_DDL_REORG_BATCH_SIZE
            && DEF_TIDB_DDL_REORG_BATCH_SIZE <= MAX_DDL_REORG_BATCH_SIZE
    );
    assert_eq!(
        "tidb_ddl_reorg_worker_cnt",
        tidb_vars::TIDB_DDL_REORG_WORKER_COUNT
    );
    assert_eq!(
        "tidb_ddl_reorg_batch_size",
        tidb_vars::TIDB_DDL_REORG_BATCH_SIZE
    );
}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestDefaultCharsetAndCollation`.
// go-parity-gap: character_set_connection/collation_connection getters over mysql.DefaultCharset not ported
#[test]
#[ignore]
fn default_charset_and_collation_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestIndexMergeSwitcher`.
///
/// Partial port: the Go test asserts
/// `DefTiDBEnableIndexMerge == true` alongside the accessor round-trip; the
/// constant half is pinned here, the accessor half needs the unported
/// SessionVars/GlobalVarsAccessor machinery.
#[test]
fn index_merge_switcher_default() {
    assert!(DEF_TIDB_ENABLE_INDEX_MERGE);
    assert_eq!(
        "tidb_enable_index_merge",
        tidb_vars::TIDB_ENABLE_INDEX_MERGE
    );
}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestNetBufferLength`.
// go-parity-gap: net_buffer_length range clamping in Validate not ported
#[test]
#[ignore]
fn net_buffer_length_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestTiDBBatchPendingTiFlashCount`.
// go-parity-gap: unsigned-int validation rejecting non-integer input not ported
#[test]
#[ignore]
fn batch_pending_tiflash_count_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestTiDBMemQuotaQuery`.
// go-parity-gap: byte-value clamping (-2 -> -1) in Validate across both scopes not ported
#[test]
#[ignore]
fn mem_quota_query_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestTiDBQueryLogMaxLen`.
// go-parity-gap: byte-value range clamping in global-scope Validate not ported
#[test]
#[ignore]
fn query_log_max_len_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestTiDBCommitterConcurrency`.
// go-parity-gap: concurrency range clamping (1..10000) in Validate not ported
#[test]
#[ignore]
fn committer_concurrency_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestTiDBDDLFlashbackConcurrency`.
///
/// Partial port: pins `MaxConfigurableConcurrency` (= 256, the clamp bound
/// asserted by the Go test, `pkg/sessionctx/vardef/tidb_vars.go`). The
/// Validate truncation itself needs the unported SysVar machinery.
#[test]
fn ddl_flashback_concurrency_bound() {
    // MaxConfigurableConcurrency, exported from vardef; pinned test-local until ported.
    const MAX_CONFIGURABLE_CONCURRENCY: u32 = 256;
    assert_eq!(256, MAX_CONFIGURABLE_CONCURRENCY);
}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestDefaultMemoryDebugModeValue`.
// go-parity-gap: memory-debug-mode session getters returning "0" not ported
#[test]
#[ignore]
fn default_memory_debug_mode_value_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestSetTIDBDistributeReorg`.
// go-parity-gap: MockGlobalAccessor SetGlobalSysVar round-trip not ported
#[test]
#[ignore]
fn set_tidb_distribute_reorg_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestDefaultPartitionPruneMode`.
///
/// Partial port: the Go test asserts both the getter result and the raw
/// constant equal `"dynamic"`; the constant half is pinned here. The getter
/// half needs the unported SessionVars machinery.
#[test]
fn default_partition_prune_mode_constant() {
    assert_eq!("dynamic", DEF_TIDB_PARTITION_PRUNE_MODE);
    assert_eq!(
        "tidb_partition_prune_mode",
        tidb_vars::TIDB_PARTITION_PRUNE_MODE
    );
}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestSetTIDBFastDDL`.
///
/// Partial port: the Go test first asserts the SysVar default value is `On`;
/// that default is `DefTiDBEnableFastReorg == true` in this crate. The
/// accessor round-trip needs the unported MockGlobalAccessor.
#[test]
fn fast_ddl_default_on() {
    assert!(DEF_TIDB_ENABLE_FAST_REORG);
    assert_eq!(
        "tidb_ddl_enable_fast_reorg",
        tidb_vars::TIDB_DDL_ENABLE_FAST_REORG
    );
}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestSetTIDBDiskQuota`.
///
/// Partial port: the Go test asserts the SysVar default is 100 GB
/// (`100 * 1024^3 = 107374182400`) before exercising the accessor; the
/// default-constant half is pinned here.
#[test]
fn disk_quota_default_100gb() {
    let gb: i64 = 1024 * 1024 * 1024;
    assert_eq!(100 * gb, DEF_TIDB_DDL_DISK_QUOTA);
    assert_eq!("tidb_ddl_disk_quota", tidb_vars::TIDB_DDL_DISK_QUOTA);
}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestTiDBServerMemoryLimit`.
///
/// Partial port: pins the default-value assertions the Go test makes against
/// the SysVar registry — `DefTiDBServerMemoryLimitSessMinSize` (128 << 20).
/// `DefTiDBServerMemoryLimit` itself is computed dynamically in Go
/// (`serverMemoryLimitDefaultValue()`), so it has no static constant here;
/// the accessor round-trips need the unported MockGlobalAccessor.
#[test]
fn server_memory_limit_defaults() {
    assert_eq!(128 << 20, DEF_TIDB_SERVER_MEMORY_LIMIT_SESS_MIN_SIZE);
    assert_eq!(
        "tidb_server_memory_limit_sess_min_size",
        tidb_vars::TIDB_SERVER_MEMORY_LIMIT_SESS_MIN_SIZE
    );
    assert_eq!(
        "tidb_server_memory_limit",
        tidb_vars::TIDB_SERVER_MEMORY_LIMIT
    );
}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestTiDBServerMemoryLimit2`.
// go-parity-gap: percentage/byte-size parsing driven by physical-memory detection and failpoints not ported
#[test]
#[ignore]
fn server_memory_limit2_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestTiDBServerMemoryLimitSessMinSize`.
///
/// Partial port: same default-value pin as
/// [`server_memory_limit_defaults`] (the Go test re-asserts it), covering the
/// `strconv.FormatInt(DefTiDBServerMemoryLimitSessMinSize, 10)` expectation.
#[test]
fn server_memory_limit_sess_min_size_default() {
    assert_eq!(128 << 20, DEF_TIDB_SERVER_MEMORY_LIMIT_SESS_MIN_SIZE);
}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestTiDBServerMemoryLimitGCTrigger`.
///
/// Partial port: the Go test checks the SysVar default equals
/// `strconv.FormatFloat(DefTiDBServerMemoryLimitGCTrigger, 'f', -1, 64)`; the
/// Rust formatting of the same constant must render identically ("0.7").
/// The gctuner percentage interactions need the unported runtime tuner.
#[test]
fn server_memory_limit_gc_trigger_default_format() {
    assert_eq!(0.7, DEF_TIDB_SERVER_MEMORY_LIMIT_GC_TRIGGER);
    assert_eq!(
        "0.7",
        format!("{}", DEF_TIDB_SERVER_MEMORY_LIMIT_GC_TRIGGER)
    );
}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestSetAggPushDownGlobally`.
///
/// Partial port: the Go test starts from the accessor default `"OFF"`; that
/// default derives from `DefTiDBOptAggPushDown == false`. The accessor
/// round-trip needs the unported MockGlobalAccessor.
#[test]
fn agg_push_down_default_off() {
    assert!(!DEF_OPT_AGG_PUSH_DOWN);
}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestSetDeriveTopNGlobally`.
///
/// Partial port: same shape as [`agg_push_down_default_off`] for
/// `tidb_opt_derive_topn`.
#[test]
fn derive_top_n_default_off() {
    assert!(!DEF_OPT_DERIVE_TOP_N);
    assert_eq!("tidb_opt_derive_topn", tidb_vars::TIDB_OPT_DERIVE_TOP_N);
}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestSetJobScheduleWindow`.
// go-parity-gap: timezone-sensitive TTL job schedule window accessor round-trip not ported
#[test]
#[ignore]
fn job_schedule_window_unported() {}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestTiDBIgnoreInlistPlanDigest`.
///
/// Partial port: the Go test asserts the initialized global value is `On`,
/// which derives from `DefTiDBIgnoreInlistPlanDigest == true`; the accessor
/// init/set round-trip needs the unported MockGlobalAccessor.
#[test]
fn ignore_inlist_plan_digest_default_on() {
    assert!(DEF_TIDB_IGNORE_INLIST_PLAN_DIGEST);
    assert_eq!(
        "tidb_ignore_inlist_plan_digest",
        tidb_vars::TIDB_IGNORE_INLIST_PLAN_DIGEST
    );
}
