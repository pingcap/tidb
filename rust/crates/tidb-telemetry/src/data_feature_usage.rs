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

//! Go `data_feature_usage.go`: which features the cluster uses, and how
//! often since the previous report.

use std::sync::atomic::Ordering;
use std::sync::{Mutex, PoisonError};

use serde::Serialize;
use tidb_log::{Field, Value};
use tikv_client::metrics::{get_txn_commit_counter, TxnCommitCounter};
use tidb_metrics::telemetry as m;
use tidb_model::{TableCacheStatusType, TempTableType};
use tidb_vardef::tidb_vars;

use crate::ttl::{get_ttl_usage_info, TtlUsageCounter};
use crate::Session;

/// Go `featureUsage`.
#[derive(Debug, Serialize)]
pub struct FeatureUsage {
    #[serde(rename = "txn")]
    pub txn: TxnUsage,
    #[serde(rename = "newClusterIndex")]
    pub new_cluster_index: NewClusterIndexUsage,
    #[serde(rename = "temporaryTable")]
    pub temporary_table: bool,
    #[serde(rename = "cte")]
    pub cte: m::CteUsageCounter,
    #[serde(rename = "accountLock")]
    pub account_lock: m::AccountLockCounter,
    #[serde(rename = "cachedTable")]
    pub cached_table: bool,
    #[serde(rename = "autoCapture")]
    pub auto_capture: bool,
    #[serde(rename = "placementPolicy")]
    pub placement_policy_usage: PlacementPolicyUsage,
    #[serde(rename = "nonTransactional")]
    pub non_transactional_usage: m::NonTransactionalStmtCounter,
    #[serde(rename = "globalKill")]
    pub global_kill: bool,
    #[serde(rename = "multiSchemaChange")]
    pub multi_schema_change: m::MultiSchemaChangeUsageCounter,
    #[serde(rename = "exchangePartition")]
    pub exchange_partition: m::ExchangePartitionUsageCounter,
    #[serde(rename = "tablePartition")]
    pub table_partition: m::TablePartitionUsageCounter,
    #[serde(rename = "logBackup")]
    pub log_backup: bool,
    #[serde(rename = "enablePaging")]
    pub enable_paging: bool,
    #[serde(rename = "enableCostModelVer2")]
    pub enable_cost_model_ver2: bool,
    #[serde(rename = "DDLUsageCounter")]
    pub ddl_usage_counter: m::DdlUsageCounter,
    #[serde(rename = "enableGlobalMemoryControl")]
    pub enable_global_memory_control: bool,
    #[serde(rename = "autoIDNoCache")]
    pub auto_id_no_cache: bool,
    #[serde(rename = "indexMergeUsageCounter")]
    pub index_merge_usage_counter: m::IndexMergeUsageCounter,
    #[serde(rename = "resourceControl")]
    pub resource_control_usage: ResourceControlUsage,
    #[serde(rename = "ttlUsage")]
    pub ttl_usage: TtlUsageCounter,
    #[serde(rename = "storeBatchCopr")]
    pub store_batch_copr_usage: m::StoreBatchCoprCounter,
}

/// Go `placementPolicyUsage`.
#[derive(Debug, Default, Serialize)]
pub struct PlacementPolicyUsage {
    #[serde(rename = "numPlacementPolicies")]
    pub num_placement_policies: u64,
    #[serde(rename = "numDBWithPolicies")]
    pub num_db_with_policies: u64,
    #[serde(rename = "numTableWithPolicies")]
    pub num_table_with_policies: u64,
    /// The number of partitions that policies are explicitly specified.
    #[serde(rename = "numPartitionWithExplicitPolicies")]
    pub num_partition_with_explicit_policies: u64,
}

/// Go `resourceControlUsage`.
#[derive(Debug, Default, Serialize)]
pub struct ResourceControlUsage {
    #[serde(rename = "resourceControlEnabled")]
    pub enabled: bool,
    #[serde(rename = "numResourceGroups")]
    pub num_resource_groups: u64,
}

/// Go `getFeatureUsage`.
pub fn get_feature_usage(sctx: &mut dyn Session) -> Result<FeatureUsage, String> {
    let new_cluster_index = match get_cluster_index_usage_info(sctx) {

        Ok(usage) => usage,
        Err(error) => {
            tidb_logutil::bg_logger().info(
                "Failed to get feature usage",
                &[Field::new("error", Value::Str(error.clone()))],
            );
            return Err(error);
        }
    };
    let sctx: &dyn Session = sctx;
    let mut usage = FeatureUsage {
        new_cluster_index,
        // transaction related feature
        txn: get_txn_usage_info(sctx),
        cte: get_cte_usage_info(),
        account_lock: get_account_lock_usage_info(),
        multi_schema_change: get_multi_schema_change_usage_info(),
        exchange_partition: get_exchange_partition_usage_info(),
        table_partition: get_table_partition_usage_info(),
        auto_capture: get_auto_capture_usage_info(sctx),
        temporary_table: false,
        cached_table: false,
        auto_id_no_cache: false,
        placement_policy_usage: PlacementPolicyUsage::default(),
        resource_control_usage: ResourceControlUsage::default(),
        non_transactional_usage: m::NonTransactionalStmtCounter::default(),
        global_kill: false,
        log_backup: false,
        enable_paging: false,
        enable_cost_model_ver2: false,
        ddl_usage_counter: m::DdlUsageCounter::default(),
        enable_global_memory_control: false,
        index_merge_usage_counter: m::IndexMergeUsageCounter::default(),
        ttl_usage: TtlUsageCounter::default(),
        store_batch_copr_usage: m::StoreBatchCoprCounter::default(),
    };
    collect_feature_usage_from_infoschema(sctx, &mut usage);
    usage.non_transactional_usage = get_non_transactional_usage();
    usage.global_kill = get_global_kill_usage_info();
    usage.log_backup = get_log_backup_usage_info();
    usage.enable_paging = get_paging_usage_info(sctx);
    usage.enable_cost_model_ver2 = get_cost_model_ver2_usage_info(sctx);
    usage.ddl_usage_counter = get_ddl_usage_info(sctx);
    usage.enable_global_memory_control = get_global_memory_control();
    usage.index_merge_usage_counter = get_index_merge_usage_info();
    usage.ttl_usage = get_ttl_usage_info(sctx);
    usage.store_batch_copr_usage = get_store_batch_usage(sctx);
    Ok(usage)
}

/// Go `collectFeatureUsageFromInfoschema`: temporary tables, cached tables,
/// placement policies and resource groups.
fn collect_feature_usage_from_infoschema(ctx: &dyn Session, usage: &mut FeatureUsage) {
    let is = ctx.info_schema();
    for db_info in is.stored_schemas() {
        let db_info = db_info.read();
        if db_info.placement_policy_ref.is_some() {
            usage.placement_policy_usage.num_db_with_policies += 1;
        }
        for tb_info in db_info.deprecated_tables.iter_deref() {
            let tb_info = tb_info.read();
            if tb_info.temp_table_type != TempTableType::NONE {
                usage.temporary_table = true;
            }
            if tb_info.table_cache_status_type != TableCacheStatusType::DISABLE {
                usage.cached_table = true;
            }
            if tb_info.placement_policy_ref.is_some() {
                usage.placement_policy_usage.num_table_with_policies += 1;
            }
            if tb_info.auto_id_cache == 1 {
                usage.auto_id_no_cache = true;
            }
            let Some(partitions) = tb_info.get_partition_info() else {
                continue;
            };
            for partition_info in partitions.read().definitions.snapshot() {
                if partition_info.placement_policy_ref.is_some() {
                    usage.placement_policy_usage.num_partition_with_explicit_policies += 1;
                }
            }
        }
    }
    usage.placement_policy_usage.num_placement_policies += is.policies().count() as u64;
    usage.resource_control_usage.num_resource_groups = is.resource_groups().count() as u64;
    usage.resource_control_usage.enabled = tidb_vardef::ENABLE_RESOURCE_CONTROL.load(Ordering::Relaxed);
}

/// Go `NewClusterIndexUsage`: the clustered-index usage of all the tables.
#[derive(Debug, Default, Serialize)]
pub struct NewClusterIndexUsage {
    /// The number of user tables with clustered index enabled.
    #[serde(rename = "numClusteredTables")]
    pub num_clustered_tables: u64,
    /// The number of user tables.
    #[serde(rename = "numTotalTables")]
    pub num_total_tables: u64,
}

/// Go `getClusterIndexUsageInfo`.
fn get_cluster_index_usage_info(sctx: &mut dyn Session) -> Result<NewClusterIndexUsage, String> {
    // query INFORMATION_SCHEMA.tables to get the latest table information about ClusterIndex
    let rows = sctx.exec_restricted_sql(
        "
		SELECT TIDB_PK_TYPE
		FROM information_schema.tables
		WHERE table_schema not in ('INFORMATION_SCHEMA', 'METRICS_SCHEMA', 'PERFORMANCE_SCHEMA', 'mysql')",
    )?;
    sctx.refresh_txn_ctx()?;
    let mut new_usage = NewClusterIndexUsage::default();
    // check ClusterIndex information for each table
    // row: 0 = TIDB_PK_TYPE
    for row in &rows {
        let Some(pk_type) = row.first() else {
            continue;
        };
        if pk_type.as_raw_bytes() == Some(b"CLUSTERED".as_slice()) {
            new_usage.num_clustered_tables += 1;
        }
    }
    new_usage.num_total_tables = rows.len() as u64;
    Ok(new_usage)
}

/// Go `TxnUsage`: async-commit, 1PC and the commits by protocol.
#[derive(Debug, Default, Serialize)]
pub struct TxnUsage {
    #[serde(rename = "asyncCommitUsed")]
    pub async_commit_used: bool,
    #[serde(rename = "onePCUsed")]
    pub one_pc_used: bool,
    #[serde(rename = "txnCommitCounter")]
    pub txn_commit_counter: TxnCommitCounter,
    #[serde(rename = "mutationCheckerUsed")]
    pub mutation_checker_used: bool,
    #[serde(rename = "assertionLevel")]
    pub assertion_level: String,
    #[serde(rename = "rcCheckTS")]
    pub rc_check_ts: bool,
    #[serde(rename = "rcWriteCheckTS")]
    pub rc_write_check_ts: bool,
    #[serde(rename = "fairLocking")]
    pub fair_locking: bool,
    #[serde(rename = "SavepointCounter")]
    pub savepoint_counter: i64,
    #[serde(rename = "lazyUniqueCheckSetCounter")]
    pub lazy_unique_check_set_counter: i64,
    #[serde(rename = "FairLockingUsageCounter")]
    pub fair_locking_usage_counter: m::FairLockingUsageCounter,
}

/// Go's `initial*Counter` package variables: each counter as of the last
/// report.
#[derive(Default)]
struct InitialCounters {
    txn_commit: TxnCommitCounter,
    cte: m::CteUsageCounter,
    account_lock: m::AccountLockCounter,
    non_transactional: m::NonTransactionalStmtCounter,
    multi_schema_change: m::MultiSchemaChangeUsageCounter,
    exchange_partition: m::ExchangePartitionUsageCounter,
    table_partition: m::TablePartitionUsageCounter,
    savepoint_stmt: i64,
    lazy_pessimistic_unique_check_set: i64,
    ddl_usage: m::DdlUsageCounter,
    index_merge: m::IndexMergeUsageCounter,
    store_batch_copr: m::StoreBatchCoprCounter,
    fair_locking_usage: m::FairLockingUsageCounter,
}

static INITIAL: Mutex<InitialCounters> = Mutex::new(InitialCounters {
    txn_commit: TxnCommitCounter { two_pc: 0, async_commit: 0, one_pc: 0 },
    cte: m::CteUsageCounter { non_recursive_cte_used: 0, recursive_used: 0, non_cte_used: 0 },
    account_lock: m::AccountLockCounter { lock_user: 0, unlock_user: 0, create_or_alter_user: 0 },
    non_transactional: m::NonTransactionalStmtCounter { delete_count: 0, update_count: 0, insert_count: 0 },
    multi_schema_change: m::MultiSchemaChangeUsageCounter { multi_schema_change_used: 0 },
    exchange_partition: m::ExchangePartitionUsageCounter { exchange_partition_cnt: 0 },
    table_partition: m::TablePartitionUsageCounter {
        table_partition_cnt: 0,
        table_partition_list_cnt: 0,
        table_partition_range_cnt: 0,
        table_partition_hash_cnt: 0,
        table_partition_range_columns_cnt: 0,
        table_partition_range_columns_gt1_cnt: 0,
        table_partition_range_columns_gt2_cnt: 0,
        table_partition_range_columns_gt3_cnt: 0,
        table_partition_list_columns_cnt: 0,
        table_partition_max_partitions_cnt: 0,
        table_partition_create_interval_partitions_cnt: 0,
        table_partition_add_interval_partitions_cnt: 0,
        table_partition_drop_interval_partitions_cnt: 0,
        table_partition_comact_cnt: 0,
        table_partition_reorganize_partition_cnt: 0,
    },
    savepoint_stmt: 0,
    lazy_pessimistic_unique_check_set: 0,
    ddl_usage: m::DdlUsageCounter {
        add_index_ingest_used: 0,
        metadata_lock_used: false,
        flashback_cluster_used: 0,
        dist_reorg_used: 0,
    },
    index_merge: m::IndexMergeUsageCounter { index_merge_used: 0 },
    store_batch_copr: m::StoreBatchCoprCounter {
        batch_size: 0,
        batched_query: 0,
        batched_query_task: 0,
        batched_count: 0,
        batched_fallback_count: 0,
    },
    fair_locking_usage: m::FairLockingUsageCounter { txn_fair_locking_used: 0, txn_fair_locking_effective: 0 },
});

fn initial() -> std::sync::MutexGuard<'static, InitialCounters> {
    INITIAL.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Go `GetGlobalSystemVar(...) == On`, false on an error.
fn global_on(ctx: &dyn Session, name: &str) -> bool {
    ctx.get_global_sys_var(name).is_ok_and(|value| value == tidb_vardef::global_sysvar_initial::ON)
}

/// Go `getTxnUsageInfo`.
pub fn get_txn_usage_info(ctx: &dyn Session) -> TxnUsage {
    let async_commit_used = global_on(ctx, tidb_vars::TIDB_ENABLE_ASYNC_COMMIT);
    let one_pc_used = global_on(ctx, tidb_vars::TIDB_ENABLE1_PC);
    let initial = initial();
    let diff = get_txn_commit_counter().subtract(initial.txn_commit);
    let mutation_checker_used = global_on(ctx, tidb_vars::TIDB_ENABLE_MUTATION_CHECKER);
    let assertion_used = ctx.get_global_sys_var(tidb_vars::TIDB_TXN_ASSERTION_LEVEL).unwrap_or_default();
    let rc_check_ts_used = global_on(ctx, tidb_vars::TIDB_RC_READ_CHECK_TS);
    let rc_write_check_ts_used = global_on(ctx, tidb_vars::TIDB_RC_WRITE_CHECK_TS);
    let fair_locking_used = global_on(ctx, tidb_vars::TIDB_PESSIMISTIC_TRANSACTION_FAIR_LOCKING);
    TxnUsage {
        async_commit_used,
        one_pc_used,
        txn_commit_counter: diff,
        mutation_checker_used,
        assertion_level: assertion_used,
        rc_check_ts: rc_check_ts_used,
        rc_write_check_ts: rc_write_check_ts_used,
        fair_locking: fair_locking_used,
        savepoint_counter: m::get_savepoint_stmt_counter() - initial.savepoint_stmt,
        lazy_unique_check_set_counter: m::get_lazy_pessimistic_unique_check_set_counter()
            - initial.lazy_pessimistic_unique_check_set,
        fair_locking_usage_counter: m::get_fair_locking_usage_counter().sub(initial.fair_locking_usage),
    }
}

/// Go `postReportTxnUsage`.
pub(crate) fn post_report_txn_usage() {
    initial().txn_commit = get_txn_commit_counter();
}

/// Go `postReportCTEUsage`.
pub(crate) fn post_report_cte_usage() {
    initial().cte = m::get_cte_counter();
}

/// Go `postReportAccountLockUsage`.
pub(crate) fn post_report_account_lock_usage() {
    initial().account_lock = m::get_account_lock_counter();
}

/// Go `PostSavepointCount`.
pub(crate) fn post_savepoint_count() {
    initial().savepoint_stmt = m::get_savepoint_stmt_counter();
}

/// Go `postReportLazyPessimisticUniqueCheckSetCount`.
pub(crate) fn post_report_lazy_pessimistic_unique_check_set_count() {
    initial().lazy_pessimistic_unique_check_set = m::get_lazy_pessimistic_unique_check_set_counter();
}

/// Go `postReportFairLockingUsageCounter`.
pub(crate) fn post_report_fair_locking_usage_counter() {
    initial().fair_locking_usage = m::get_fair_locking_usage_counter();
}

/// Go `getCTEUsageInfo`.
fn get_cte_usage_info() -> m::CteUsageCounter {
    m::get_cte_counter().sub(initial().cte)
}

/// Go `getAccountLockUsageInfo`.
fn get_account_lock_usage_info() -> m::AccountLockCounter {
    m::get_account_lock_counter().sub(initial().account_lock)
}

/// Go `postReportMultiSchemaChangeUsage`.
pub(crate) fn post_report_multi_schema_change_usage() {
    initial().multi_schema_change = m::get_multi_schema_counter();
}

/// Go `getMultiSchemaChangeUsageInfo`.
fn get_multi_schema_change_usage_info() -> m::MultiSchemaChangeUsageCounter {
    m::get_multi_schema_counter().sub(initial().multi_schema_change)
}

/// Go `postReportExchangePartitionUsage`.
pub(crate) fn post_report_exchange_partition_usage() {
    initial().exchange_partition = m::get_exchange_partition_counter();
}

/// Go `getExchangePartitionUsageInfo`.
fn get_exchange_partition_usage_info() -> m::ExchangePartitionUsageCounter {
    m::get_exchange_partition_counter().sub(initial().exchange_partition)
}

/// Go `postReportTablePartitionUsage`.
pub(crate) fn post_report_table_partition_usage() {
    let mut initial = initial();
    initial.table_partition = m::reset_table_partition_counter(initial.table_partition);
}

/// Go `postReportDDLUsage`.
pub(crate) fn post_report_ddl_usage() {
    initial().ddl_usage = m::get_ddl_usage_counter();
}

/// Go `getTablePartitionUsageInfo`.
fn get_table_partition_usage_info() -> m::TablePartitionUsageCounter {
    m::get_table_partition_counter().cal(initial().table_partition)
}

/// Go `getAutoCaptureUsageInfo`: the 'Auto Capture' usage.
fn get_auto_capture_usage_info(ctx: &dyn Session) -> bool {
    global_on(ctx, tidb_vars::TIDB_CAPTURE_PLAN_BASELINE)
}

/// Go `getNonTransactionalUsage`.
fn get_non_transactional_usage() -> m::NonTransactionalStmtCounter {
    m::get_non_transactional_stmt_counter().sub(initial().non_transactional)
}

/// Go `postReportNonTransactionalCounter`.
pub(crate) fn post_report_non_transactional_counter() {
    initial().non_transactional = m::get_non_transactional_stmt_counter();
}

/// Go `getGlobalKillUsageInfo`.
fn get_global_kill_usage_info() -> bool {
    tidb_config::config_tree::config::get_global_config().enable_global_kill
}

/// Go `getLogBackupUsageInfo`.
fn get_log_backup_usage_info() -> bool {
    tidb_br::utils::is_log_backup_in_use()
}

/// Go `getCostModelVer2UsageInfo`.
fn get_cost_model_ver2_usage_info(ctx: &dyn Session) -> bool {
    ctx.cost_model_version() == 2
}

/// Go `getPagingUsageInfo`: the session's `tidb_enable_paging`, which
/// defaults to true since v6.2.0.
fn get_paging_usage_info(ctx: &dyn Session) -> bool {
    ctx.enable_paging()
}

/// Go `getDDLUsageInfo`.
fn get_ddl_usage_info(ctx: &dyn Session) -> m::DdlUsageCounter {
    let mut diff = m::get_ddl_usage_counter().sub(initial().ddl_usage);
    if let Ok(is_enable) = ctx.get_global_sys_var(tidb_vars::TIDB_ENABLE_MDL) {
        diff.metadata_lock_used = is_enable == "ON";
    }
    diff
}

/// Go `getGlobalMemoryControl`.
fn get_global_memory_control() -> bool {
    tidb_memory::SERVER_MEMORY_LIMIT.load(Ordering::Relaxed) > 0
}

/// Go `postReportIndexMergeUsage`.
pub(crate) fn post_report_index_merge_usage() {
    initial().index_merge = m::get_index_merge_counter();
}

/// Go `getIndexMergeUsageInfo`.
fn get_index_merge_usage_info() -> m::IndexMergeUsageCounter {
    m::get_index_merge_counter().sub(initial().index_merge)
}

/// Go `getStoreBatchUsage`.
fn get_store_batch_usage(ctx: &dyn Session) -> m::StoreBatchCoprCounter {
    let mut diff = m::get_store_batch_copr_counter().sub(initial().store_batch_copr);
    if let Ok(value) = ctx.get_global_sys_var(tidb_vars::TIDB_STORE_BATCH_SIZE) {
        if let Ok(batch_size) = value.parse::<i64>() {
            diff.batch_size = batch_size;
        }
    }
    diff
}

/// Go `postStoreBatchUsage`.
pub(crate) fn post_store_batch_usage() {
    initial().store_batch_copr = m::get_store_batch_copr_counter();
}
