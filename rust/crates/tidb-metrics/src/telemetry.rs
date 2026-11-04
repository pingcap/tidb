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

//! Go `pkg/metrics/telemetry.go`: the usage counters `pkg/telemetry` reads.
//!
//! Go creates them in `InitTelemetryMetrics` and never registers them, so
//! they are absent from `/metrics`; only their readers below see them.

use std::sync::LazyLock;

use prometheus::{Counter, CounterVec, Opts};
use serde::Serialize;

fn counter(name: &str, help: &str) -> Counter {
    Counter::new(format!("tidb_telemetry_{name}"), help).expect("valid metric definition")
}

fn counter_vec(name: &str, help: &str, label: &str) -> CounterVec {
    CounterVec::new(Opts::new(format!("tidb_telemetry_{name}"), help), &[label])
        .expect("valid metric definition")
}

/// Go `LblCTEType`.
pub const LBL_CTE_TYPE: &str = "cte_type";
/// Go `LblAccountLock`.
pub const LBL_ACCOUNT_LOCK: &str = "account_lock";

/// Go `TelemetrySQLCTECnt`.
pub static TELEMETRY_SQL_CTE_CNT: LazyLock<CounterVec> =
    LazyLock::new(|| counter_vec("non_recursive_cte_usage", "Counter of usage of CTE", LBL_CTE_TYPE));
/// Go `TelemetryMultiSchemaChangeCnt`.
pub static TELEMETRY_MULTI_SCHEMA_CHANGE_CNT: LazyLock<Counter> =
    LazyLock::new(|| counter("multi_schema_change_usage", "Counter of usage of multi-schema change"));
/// Go `TelemetryTablePartitionCnt`.
pub static TELEMETRY_TABLE_PARTITION_CNT: LazyLock<Counter> = LazyLock::new(|| {
    counter("table_partition_usage", "Counter of CREATE TABLE which includes of table partitioning")
});
/// Go `TelemetryTablePartitionListCnt`.
pub static TELEMETRY_TABLE_PARTITION_LIST_CNT: LazyLock<Counter> = LazyLock::new(|| {
    counter("table_partition_list_usage", "Counter of CREATE TABLE which includes LIST partitioning")
});
/// Go `TelemetryTablePartitionRangeCnt`.
pub static TELEMETRY_TABLE_PARTITION_RANGE_CNT: LazyLock<Counter> = LazyLock::new(|| {
    counter("table_partition_range_usage", "Counter of CREATE TABLE which includes RANGE partitioning")
});
/// Go `TelemetryTablePartitionHashCnt`.
pub static TELEMETRY_TABLE_PARTITION_HASH_CNT: LazyLock<Counter> = LazyLock::new(|| {
    counter("table_partition_hash_usage", "Counter of CREATE TABLE which includes HASH partitioning")
});
/// Go `TelemetryTablePartitionRangeColumnsCnt`.
pub static TELEMETRY_TABLE_PARTITION_RANGE_COLUMNS_CNT: LazyLock<Counter> = LazyLock::new(|| {
    counter(
        "table_partition_range_columns_usage",
        "Counter of CREATE TABLE which includes RANGE COLUMNS partitioning",
    )
});
/// Go `TelemetryTablePartitionRangeColumnsGt1Cnt`.
pub static TELEMETRY_TABLE_PARTITION_RANGE_COLUMNS_GT1_CNT: LazyLock<Counter> = LazyLock::new(|| {
    counter(
        "table_partition_range_multi_columns_usage",
        "Counter of CREATE TABLE which includes RANGE COLUMNS partitioning with more than one partitioning column",
    )
});
/// Go `TelemetryTablePartitionRangeColumnsGt2Cnt`.
pub static TELEMETRY_TABLE_PARTITION_RANGE_COLUMNS_GT2_CNT: LazyLock<Counter> = LazyLock::new(|| {
    counter(
        "table_partition_range_multi_columns_usage",
        "Counter of CREATE TABLE which includes RANGE COLUMNS partitioning with more than two partitioning columns",
    )
});
/// Go `TelemetryTablePartitionRangeColumnsGt3Cnt`.
pub static TELEMETRY_TABLE_PARTITION_RANGE_COLUMNS_GT3_CNT: LazyLock<Counter> = LazyLock::new(|| {
    counter(
        "table_partition_range_multi_columns_usage",
        "Counter of CREATE TABLE which includes RANGE COLUMNS partitioning with more than three partitioning columns",
    )
});
/// Go `TelemetryTablePartitionListColumnsCnt`.
pub static TELEMETRY_TABLE_PARTITION_LIST_COLUMNS_CNT: LazyLock<Counter> = LazyLock::new(|| {
    counter(
        "table_partition_list_columns_usage",
        "Counter of CREATE TABLE which includes LIST COLUMNS partitioning",
    )
});
/// Go `TelemetryTablePartitionMaxPartitionsCnt`.
pub static TELEMETRY_TABLE_PARTITION_MAX_PARTITIONS_CNT: LazyLock<Counter> = LazyLock::new(|| {
    counter("table_partition_max_partition_usage", "Counter of partitions created by CREATE TABLE statements")
});
/// Go `TelemetryAccountLockCnt`.
pub static TELEMETRY_ACCOUNT_LOCK_CNT: LazyLock<CounterVec> =
    LazyLock::new(|| counter_vec("account_lock_usage", "Counter of locked/unlocked users", LBL_ACCOUNT_LOCK));
/// Go `TelemetryTablePartitionCreateIntervalPartitionsCnt`.
pub static TELEMETRY_TABLE_PARTITION_CREATE_INTERVAL_PARTITIONS_CNT: LazyLock<Counter> = LazyLock::new(|| {
    counter(
        "table_partition_create_interval_partition_usage",
        "Counter of partitions created by CREATE TABLE INTERVAL statements",
    )
});
/// Go `TelemetryTablePartitionAddIntervalPartitionsCnt`.
pub static TELEMETRY_TABLE_PARTITION_ADD_INTERVAL_PARTITIONS_CNT: LazyLock<Counter> = LazyLock::new(|| {
    counter(
        "table_partition_add_interval_partition_usage",
        "Counter of partitions added by ALTER TABLE LAST PARTITION statements",
    )
});
/// Go `TelemetryTablePartitionDropIntervalPartitionsCnt`.
pub static TELEMETRY_TABLE_PARTITION_DROP_INTERVAL_PARTITIONS_CNT: LazyLock<Counter> = LazyLock::new(|| {
    counter(
        "table_partition_drop_interval_partition_usage",
        "Counter of partitions added by ALTER TABLE FIRST PARTITION statements",
    )
});
/// Go `TelemetryExchangePartitionCnt`.
pub static TELEMETRY_EXCHANGE_PARTITION_CNT: LazyLock<Counter> =
    LazyLock::new(|| counter("exchange_partition_usage", "Counter of usage of exchange partition statements"));
/// Go `TelemetryAddIndexIngestCnt`.
pub static TELEMETRY_ADD_INDEX_INGEST_CNT: LazyLock<Counter> =
    LazyLock::new(|| counter("add_index_ingest_usage", "Counter of usage of add index acceleration solution"));
/// Go `TelemetryFlashbackClusterCnt`.
pub static TELEMETRY_FLASHBACK_CLUSTER_CNT: LazyLock<Counter> =
    LazyLock::new(|| counter("flashback_cluster_usage", "Counter of usage of flashback cluster"));
/// Go `TelemetryIndexMergeUsage`.
pub static TELEMETRY_INDEX_MERGE_USAGE: LazyLock<Counter> =
    LazyLock::new(|| counter("index_merge_usage", "Counter of usage of index merge"));
/// Go `TelemetryCompactPartitionCnt`.
pub static TELEMETRY_COMPACT_PARTITION_CNT: LazyLock<Counter> =
    LazyLock::new(|| counter("compact_partition_usage", "Counter of compact table partition"));
/// Go `TelemetryReorganizePartitionCnt`.
pub static TELEMETRY_REORGANIZE_PARTITION_CNT: LazyLock<Counter> =
    LazyLock::new(|| counter("reorganize_partition_usage", "Counter of alter table reorganize partition"));
/// Go `TelemetryDistReorgCnt`.
pub static TELEMETRY_DIST_REORG_CNT: LazyLock<Counter> =
    LazyLock::new(|| counter("distributed_reorg_count", "Counter of usage of distributed reorg DDL tasks count"));
/// Go `TelemetryStoreBatchedQueryCnt`.
pub static TELEMETRY_STORE_BATCHED_QUERY_CNT: LazyLock<Counter> = LazyLock::new(|| {
    counter("store_batched_query", "Counter of queries which use store batched coprocessor tasks")
});
/// Go `TelemetryBatchedQueryTaskCnt`.
pub static TELEMETRY_BATCHED_QUERY_TASK_CNT: LazyLock<Counter> =
    LazyLock::new(|| counter("batched_query_task", "Counter of coprocessor tasks in batched queries"));
/// Go `TelemetryStoreBatchedCnt`.
pub static TELEMETRY_STORE_BATCHED_CNT: LazyLock<Counter> =
    LazyLock::new(|| counter("store_batched", "Counter of store batched coprocessor tasks"));
/// Go `TelemetryStoreBatchedFallbackCnt`.
pub static TELEMETRY_STORE_BATCHED_FALLBACK_CNT: LazyLock<Counter> =
    LazyLock::new(|| counter("store_batched_fallback", "Counter of store batched fallback coprocessor tasks"));

/// Go `readCounter`: the counter's value, as Go's `int64(float64)`.
#[expect(clippy::cast_possible_truncation, reason = "Go int64(pb.GetCounter().GetValue())")]
fn read_counter(counter: &Counter) -> i64 {
    counter.get() as i64
}

/// Go `CTEUsageCounter`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct CteUsageCounter {
    #[serde(rename = "nonRecursiveCTEUsed")]
    pub non_recursive_cte_used: i64,
    #[serde(rename = "recursiveUsed")]
    pub recursive_used: i64,
    #[serde(rename = "nonCTEUsed")]
    pub non_cte_used: i64,
}

impl CteUsageCounter {
    /// Go `Sub`.
    #[must_use]
    pub const fn sub(self, rhs: Self) -> Self {
        Self {
            non_recursive_cte_used: self.non_recursive_cte_used - rhs.non_recursive_cte_used,
            recursive_used: self.recursive_used - rhs.recursive_used,
            non_cte_used: self.non_cte_used - rhs.non_cte_used,
        }
    }
}

/// Go `GetCTECounter`.
#[must_use]
pub fn get_cte_counter() -> CteUsageCounter {
    CteUsageCounter {
        non_recursive_cte_used: read_counter(&TELEMETRY_SQL_CTE_CNT.with_label_values(&["nonRecurCTE"])),
        recursive_used: read_counter(&TELEMETRY_SQL_CTE_CNT.with_label_values(&["recurCTE"])),
        non_cte_used: read_counter(&TELEMETRY_SQL_CTE_CNT.with_label_values(&["notCTE"])),
    }
}

/// Go `AccountLockCounter`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct AccountLockCounter {
    #[serde(rename = "lockUser")]
    pub lock_user: i64,
    #[serde(rename = "unlockUser")]
    pub unlock_user: i64,
    #[serde(rename = "createOrAlterUser")]
    pub create_or_alter_user: i64,
}

impl AccountLockCounter {
    /// Go `Sub`.
    #[must_use]
    pub const fn sub(self, rhs: Self) -> Self {
        Self {
            lock_user: self.lock_user - rhs.lock_user,
            unlock_user: self.unlock_user - rhs.unlock_user,
            create_or_alter_user: self.create_or_alter_user - rhs.create_or_alter_user,
        }
    }
}

/// Go `GetAccountLockCounter`.
#[must_use]
pub fn get_account_lock_counter() -> AccountLockCounter {
    AccountLockCounter {
        lock_user: read_counter(&TELEMETRY_ACCOUNT_LOCK_CNT.with_label_values(&["lockUser"])),
        unlock_user: read_counter(&TELEMETRY_ACCOUNT_LOCK_CNT.with_label_values(&["unlockUser"])),
        create_or_alter_user: read_counter(&TELEMETRY_ACCOUNT_LOCK_CNT.with_label_values(&["createOrAlterUser"])),
    }
}

/// Go `MultiSchemaChangeUsageCounter`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct MultiSchemaChangeUsageCounter {
    pub multi_schema_change_used: i64,
}

impl MultiSchemaChangeUsageCounter {
    /// Go `Sub`.
    #[must_use]
    pub const fn sub(self, rhs: Self) -> Self {
        Self {
            multi_schema_change_used: self.multi_schema_change_used - rhs.multi_schema_change_used,
        }
    }
}

/// Go `GetMultiSchemaCounter`.
#[must_use]
pub fn get_multi_schema_counter() -> MultiSchemaChangeUsageCounter {
    MultiSchemaChangeUsageCounter {
        multi_schema_change_used: read_counter(&TELEMETRY_MULTI_SCHEMA_CHANGE_CNT),
    }
}

/// Go `TablePartitionUsageCounter`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct TablePartitionUsageCounter {
    pub table_partition_cnt: i64,
    pub table_partition_list_cnt: i64,
    pub table_partition_range_cnt: i64,
    pub table_partition_hash_cnt: i64,
    pub table_partition_range_columns_cnt: i64,
    #[serde(rename = "table_partition_range_columns_gt_1_cnt")]
    pub table_partition_range_columns_gt1_cnt: i64,
    #[serde(rename = "table_partition_range_columns_gt_2_cnt")]
    pub table_partition_range_columns_gt2_cnt: i64,
    #[serde(rename = "table_partition_range_columns_gt_3_cnt")]
    pub table_partition_range_columns_gt3_cnt: i64,
    pub table_partition_list_columns_cnt: i64,
    pub table_partition_max_partitions_cnt: i64,
    pub table_partition_create_interval_partitions_cnt: i64,
    pub table_partition_add_interval_partitions_cnt: i64,
    pub table_partition_drop_interval_partitions_cnt: i64,
    /// Go `TablePartitionComactCnt`, with Go's JSON key.
    #[serde(rename = "table_TablePartitionComactCnt")]
    pub table_partition_comact_cnt: i64,
    #[serde(rename = "table_reorganize_partition_cnt")]
    pub table_partition_reorganize_partition_cnt: i64,
}

impl TablePartitionUsageCounter {
    /// Go `Cal`.
    #[must_use]
    pub fn cal(self, rhs: Self) -> Self {
        Self {
            table_partition_cnt: self.table_partition_cnt - rhs.table_partition_cnt,
            table_partition_list_cnt: self.table_partition_list_cnt - rhs.table_partition_list_cnt,
            table_partition_range_cnt: self.table_partition_range_cnt - rhs.table_partition_range_cnt,
            table_partition_hash_cnt: self.table_partition_hash_cnt - rhs.table_partition_hash_cnt,
            table_partition_range_columns_cnt: self.table_partition_range_columns_cnt
                - rhs.table_partition_range_columns_cnt,
            table_partition_range_columns_gt1_cnt: self.table_partition_range_columns_gt1_cnt
                - rhs.table_partition_range_columns_gt1_cnt,
            table_partition_range_columns_gt2_cnt: self.table_partition_range_columns_gt2_cnt
                - rhs.table_partition_range_columns_gt2_cnt,
            table_partition_range_columns_gt3_cnt: self.table_partition_range_columns_gt3_cnt
                - rhs.table_partition_range_columns_gt3_cnt,
            table_partition_list_columns_cnt: self.table_partition_list_columns_cnt
                - rhs.table_partition_list_columns_cnt,
            table_partition_max_partitions_cnt: (self.table_partition_max_partitions_cnt
                - rhs.table_partition_max_partitions_cnt)
                .max(rhs.table_partition_max_partitions_cnt),
            table_partition_create_interval_partitions_cnt: self.table_partition_create_interval_partitions_cnt
                - rhs.table_partition_create_interval_partitions_cnt,
            table_partition_add_interval_partitions_cnt: self.table_partition_add_interval_partitions_cnt
                - rhs.table_partition_add_interval_partitions_cnt,
            table_partition_drop_interval_partitions_cnt: self.table_partition_drop_interval_partitions_cnt
                - rhs.table_partition_drop_interval_partitions_cnt,
            table_partition_comact_cnt: self.table_partition_comact_cnt - rhs.table_partition_comact_cnt,
            table_partition_reorganize_partition_cnt: self.table_partition_reorganize_partition_cnt
                - rhs.table_partition_reorganize_partition_cnt,
        }
    }
}

/// Go `ResetTablePartitionCounter`: the interval, compact and add/drop
/// interval counters are left zero, as Go leaves them.
#[must_use]
pub fn reset_table_partition_counter(pre: TablePartitionUsageCounter) -> TablePartitionUsageCounter {
    TablePartitionUsageCounter {
        table_partition_cnt: read_counter(&TELEMETRY_TABLE_PARTITION_CNT),
        table_partition_list_cnt: read_counter(&TELEMETRY_TABLE_PARTITION_LIST_CNT),
        table_partition_range_cnt: read_counter(&TELEMETRY_TABLE_PARTITION_RANGE_CNT),
        table_partition_hash_cnt: read_counter(&TELEMETRY_TABLE_PARTITION_HASH_CNT),
        table_partition_range_columns_cnt: read_counter(&TELEMETRY_TABLE_PARTITION_RANGE_COLUMNS_CNT),
        table_partition_range_columns_gt1_cnt: read_counter(&TELEMETRY_TABLE_PARTITION_RANGE_COLUMNS_GT1_CNT),
        table_partition_range_columns_gt2_cnt: read_counter(&TELEMETRY_TABLE_PARTITION_RANGE_COLUMNS_GT2_CNT),
        table_partition_range_columns_gt3_cnt: read_counter(&TELEMETRY_TABLE_PARTITION_RANGE_COLUMNS_GT3_CNT),
        table_partition_list_columns_cnt: read_counter(&TELEMETRY_TABLE_PARTITION_LIST_COLUMNS_CNT),
        table_partition_max_partitions_cnt: (read_counter(&TELEMETRY_TABLE_PARTITION_MAX_PARTITIONS_CNT)
            - pre.table_partition_max_partitions_cnt)
            .max(pre.table_partition_max_partitions_cnt),
        table_partition_reorganize_partition_cnt: read_counter(&TELEMETRY_REORGANIZE_PARTITION_CNT),
        ..TablePartitionUsageCounter::default()
    }
}

/// Go `GetTablePartitionCounter`.
#[must_use]
pub fn get_table_partition_counter() -> TablePartitionUsageCounter {
    TablePartitionUsageCounter {
        table_partition_cnt: read_counter(&TELEMETRY_TABLE_PARTITION_CNT),
        table_partition_list_cnt: read_counter(&TELEMETRY_TABLE_PARTITION_LIST_CNT),
        table_partition_range_cnt: read_counter(&TELEMETRY_TABLE_PARTITION_RANGE_CNT),
        table_partition_hash_cnt: read_counter(&TELEMETRY_TABLE_PARTITION_HASH_CNT),
        table_partition_range_columns_cnt: read_counter(&TELEMETRY_TABLE_PARTITION_RANGE_COLUMNS_CNT),
        table_partition_range_columns_gt1_cnt: read_counter(&TELEMETRY_TABLE_PARTITION_RANGE_COLUMNS_GT1_CNT),
        table_partition_range_columns_gt2_cnt: read_counter(&TELEMETRY_TABLE_PARTITION_RANGE_COLUMNS_GT2_CNT),
        table_partition_range_columns_gt3_cnt: read_counter(&TELEMETRY_TABLE_PARTITION_RANGE_COLUMNS_GT3_CNT),
        table_partition_list_columns_cnt: read_counter(&TELEMETRY_TABLE_PARTITION_LIST_COLUMNS_CNT),
        table_partition_max_partitions_cnt: read_counter(&TELEMETRY_TABLE_PARTITION_MAX_PARTITIONS_CNT),
        table_partition_create_interval_partitions_cnt: read_counter(
            &TELEMETRY_TABLE_PARTITION_CREATE_INTERVAL_PARTITIONS_CNT,
        ),
        table_partition_add_interval_partitions_cnt: read_counter(
            &TELEMETRY_TABLE_PARTITION_ADD_INTERVAL_PARTITIONS_CNT,
        ),
        table_partition_drop_interval_partitions_cnt: read_counter(
            &TELEMETRY_TABLE_PARTITION_DROP_INTERVAL_PARTITIONS_CNT,
        ),
        table_partition_comact_cnt: read_counter(&TELEMETRY_COMPACT_PARTITION_CNT),
        table_partition_reorganize_partition_cnt: read_counter(&TELEMETRY_REORGANIZE_PARTITION_CNT),
    }
}

/// Go `ExchangePartitionUsageCounter`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct ExchangePartitionUsageCounter {
    pub exchange_partition_cnt: i64,
}

impl ExchangePartitionUsageCounter {
    /// Go `Sub`.
    #[must_use]
    pub const fn sub(self, rhs: Self) -> Self {
        Self {
            exchange_partition_cnt: self.exchange_partition_cnt - rhs.exchange_partition_cnt,
        }
    }
}

/// Go `GetExchangePartitionCounter`.
#[must_use]
pub fn get_exchange_partition_counter() -> ExchangePartitionUsageCounter {
    ExchangePartitionUsageCounter {
        exchange_partition_cnt: read_counter(&TELEMETRY_EXCHANGE_PARTITION_CNT),
    }
}

/// Go `NonTransactionalStmtCounter`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct NonTransactionalStmtCounter {
    #[serde(rename = "delete")]
    pub delete_count: i64,
    #[serde(rename = "update")]
    pub update_count: i64,
    #[serde(rename = "insert")]
    pub insert_count: i64,
}

impl NonTransactionalStmtCounter {
    /// Go `Sub`.
    #[must_use]
    pub const fn sub(self, rhs: Self) -> Self {
        Self {
            delete_count: self.delete_count - rhs.delete_count,
            update_count: self.update_count - rhs.update_count,
            insert_count: self.insert_count - rhs.insert_count,
        }
    }
}

/// Go `GetNonTransactionalStmtCounter`.
#[must_use]
pub fn get_non_transactional_stmt_counter() -> NonTransactionalStmtCounter {
    let count = crate::session::NON_TRANSACTIONAL_DML_COUNT.clone();
    NonTransactionalStmtCounter {
        delete_count: read_counter(&count.with_label_values(&["delete"])),
        update_count: read_counter(&count.with_label_values(&["update"])),
        insert_count: read_counter(&count.with_label_values(&["insert"])),
    }
}

/// Go `GetSavepointStmtCounter`: Go reads the `Savepoint` series of
/// `StmtNodeCounter` with an empty database and the default resource group.
#[must_use]
pub fn get_savepoint_stmt_counter() -> i64 {
    read_counter(&crate::executor::STATEMENT_TOTAL.with_label_values(&["", "default", "Savepoint"]))
}

/// Go `GetLazyPessimisticUniqueCheckSetCounter`.
#[must_use]
pub fn get_lazy_pessimistic_unique_check_set_counter() -> i64 {
    read_counter(&crate::session::LAZY_PESSIMISTIC_UNIQUE_CHECK_SET_COUNT)
}

/// Go `DDLUsageCounter`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct DdlUsageCounter {
    pub add_index_ingest_used: i64,
    pub metadata_lock_used: bool,
    pub flashback_cluster_used: i64,
    pub dist_reorg_used: i64,
}

impl DdlUsageCounter {
    /// Go `Sub`: `MetadataLockUsed` is left false.
    #[must_use]
    pub const fn sub(self, rhs: Self) -> Self {
        Self {
            add_index_ingest_used: self.add_index_ingest_used - rhs.add_index_ingest_used,
            metadata_lock_used: false,
            flashback_cluster_used: self.flashback_cluster_used - rhs.flashback_cluster_used,
            dist_reorg_used: self.dist_reorg_used - rhs.dist_reorg_used,
        }
    }
}

/// Go `GetDDLUsageCounter`.
#[must_use]
pub fn get_ddl_usage_counter() -> DdlUsageCounter {
    DdlUsageCounter {
        add_index_ingest_used: read_counter(&TELEMETRY_ADD_INDEX_INGEST_CNT),
        metadata_lock_used: false,
        flashback_cluster_used: read_counter(&TELEMETRY_FLASHBACK_CLUSTER_CNT),
        dist_reorg_used: read_counter(&TELEMETRY_DIST_REORG_CNT),
    }
}

/// Go `IndexMergeUsageCounter`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct IndexMergeUsageCounter {
    pub index_merge_used: i64,
}

impl IndexMergeUsageCounter {
    /// Go `Sub`.
    #[must_use]
    pub const fn sub(self, rhs: Self) -> Self {
        Self {
            index_merge_used: self.index_merge_used - rhs.index_merge_used,
        }
    }
}

/// Go `GetIndexMergeCounter`.
#[must_use]
pub fn get_index_merge_counter() -> IndexMergeUsageCounter {
    IndexMergeUsageCounter {
        index_merge_used: read_counter(&TELEMETRY_INDEX_MERGE_USAGE),
    }
}

/// Go `StoreBatchCoprCounter`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct StoreBatchCoprCounter {
    /// Go `BatchSize`: the global value of `tidb_store_batch_size`.
    pub batch_size: i64,
    #[serde(rename = "query")]
    pub batched_query: i64,
    #[serde(rename = "tasks")]
    pub batched_query_task: i64,
    #[serde(rename = "batched")]
    pub batched_count: i64,
    #[serde(rename = "batched_fallback")]
    pub batched_fallback_count: i64,
}

impl StoreBatchCoprCounter {
    /// Go `Sub`: `BatchSize` is left zero.
    #[must_use]
    pub const fn sub(self, rhs: Self) -> Self {
        Self {
            batch_size: 0,
            batched_query: self.batched_query - rhs.batched_query,
            batched_query_task: self.batched_query_task - rhs.batched_query_task,
            batched_count: self.batched_count - rhs.batched_count,
            batched_fallback_count: self.batched_fallback_count - rhs.batched_fallback_count,
        }
    }
}

/// Go `GetStoreBatchCoprCounter`.
#[must_use]
pub fn get_store_batch_copr_counter() -> StoreBatchCoprCounter {
    StoreBatchCoprCounter {
        batch_size: 0,
        batched_query: read_counter(&TELEMETRY_STORE_BATCHED_QUERY_CNT),
        batched_query_task: read_counter(&TELEMETRY_BATCHED_QUERY_TASK_CNT),
        batched_count: read_counter(&TELEMETRY_STORE_BATCHED_CNT),
        batched_fallback_count: read_counter(&TELEMETRY_STORE_BATCHED_FALLBACK_CNT),
    }
}

/// Go `FairLockingUsageCounter`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct FairLockingUsageCounter {
    pub txn_fair_locking_used: i64,
    pub txn_fair_locking_effective: i64,
}

impl FairLockingUsageCounter {
    /// Go `Sub`.
    #[must_use]
    pub const fn sub(self, rhs: Self) -> Self {
        Self {
            txn_fair_locking_used: self.txn_fair_locking_used - rhs.txn_fair_locking_used,
            txn_fair_locking_effective: self.txn_fair_locking_effective - rhs.txn_fair_locking_effective,
        }
    }
}

/// Go `GetFairLockingUsageCounter`.
#[must_use]
pub fn get_fair_locking_usage_counter() -> FairLockingUsageCounter {
    let usage = &crate::session::TRANSACTION_FAIR_LOCKING_USAGE;
    FairLockingUsageCounter {
        txn_fair_locking_used: read_counter(&usage.with_label_values(&["txn-used"])),
        txn_fair_locking_effective: read_counter(&usage.with_label_values(&["txn-effective"])),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `TablePartitionUsageCounter.Cal` keeps the larger of the maximum's
    /// growth and its previous value, and Go's JSON keys stay as written.
    #[test]
    fn table_partition_counters_keep_gos_max_rule_and_json_keys() {
        let current = TablePartitionUsageCounter {
            table_partition_cnt: 5,
            table_partition_max_partitions_cnt: 12,
            ..Default::default()
        };
        let previous = TablePartitionUsageCounter {
            table_partition_cnt: 2,
            table_partition_max_partitions_cnt: 10,
            ..Default::default()
        };
        let diff = current.cal(previous);
        assert_eq!(diff.table_partition_cnt, 3);
        assert_eq!(diff.table_partition_max_partitions_cnt, 10);
        let json = serde_json::to_string(&diff).unwrap();
        assert!(json.contains("\"table_TablePartitionComactCnt\":0"), "{json}");
        assert!(json.contains("\"table_partition_range_columns_gt_1_cnt\":0"), "{json}");
    }
}
