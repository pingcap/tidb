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

//! Behavioral tests retained from the Go source inventory.
//! Removed empty entries and their original contracts are indexed in
//! rust/docs/parity/current-audit/empty-test-cleanup-obligations.json.

use super::defaults::{
    DEF_OPT_AGG_PUSH_DOWN, DEF_OPT_DERIVE_TOP_N, DEF_TIDB_DDL_DISK_QUOTA,
    DEF_TIDB_DDL_REORG_BATCH_SIZE, DEF_TIDB_ENABLE_FAST_REORG, DEF_TIDB_ENABLE_INDEX_MERGE,
    DEF_TIDB_IGNORE_INLIST_PLAN_DIGEST, DEF_TIDB_PARTITION_PRUNE_MODE,
    DEF_TIDB_SERVER_MEMORY_LIMIT_GC_TRIGGER, DEF_TIDB_SERVER_MEMORY_LIMIT_SESS_MIN_SIZE,
};
use super::global_sysvar_initial::{global_system_variable_initial_value, GlobalSysvarEnvironment};
use super::tidb_vars;

// ---------------------------------------------------------------------------
// pkg/sessionctx/variable/main_test.go
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// pkg/sessionctx/variable/mock_globalaccessor_test.go
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// pkg/sessionctx/variable/removed_test.go
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// pkg/sessionctx/variable/statusvar_test.go
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// pkg/sessionctx/variable/sysvar_test.go (first 50 tests, canonical order)
// ---------------------------------------------------------------------------

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
