// Copyright 2021 PingCAP, Inc.
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

package metrics

import (
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Metrics
var (
	TelemetrySQLCTECnt = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "non_recursive_cte_usage",
			Help:      "Counter of usage of CTE",
		}, []string{LblCTEType})
	TelemetryMultiSchemaChangeCnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "multi_schema_change_usage",
			Help:      "Counter of usage of multi-schema change",
		})
	TelemetryTablePartitionCnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "table_partition_usage",
			Help:      "Counter of CREATE TABLE which includes of table partitioning",
		})
	TelemetryTablePartitionListCnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "table_partition_list_usage",
			Help:      "Counter of CREATE TABLE which includes LIST partitioning",
		})
	TelemetryTablePartitionRangeCnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "table_partition_range_usage",
			Help:      "Counter of CREATE TABLE which includes RANGE partitioning",
		})
	TelemetryTablePartitionHashCnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "table_partition_hash_usage",
			Help:      "Counter of CREATE TABLE which includes HASH partitioning",
		})
	TelemetryTablePartitionRangeColumnsCnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "table_partition_range_columns_usage",
			Help:      "Counter of CREATE TABLE which includes RANGE COLUMNS partitioning",
		})
	TelemetryTablePartitionRangeColumnsGt1Cnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "table_partition_range_multi_columns_usage",
			Help:      "Counter of CREATE TABLE which includes RANGE COLUMNS partitioning with more than one partitioning column",
		})
	TelemetryTablePartitionRangeColumnsGt2Cnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "table_partition_range_multi_columns_usage",
			Help:      "Counter of CREATE TABLE which includes RANGE COLUMNS partitioning with more than two partitioning columns",
		})
	TelemetryTablePartitionRangeColumnsGt3Cnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "table_partition_range_multi_columns_usage",
			Help:      "Counter of CREATE TABLE which includes RANGE COLUMNS partitioning with more than three partitioning columns",
		})
	TelemetryTablePartitionListColumnsCnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "table_partition_list_columns_usage",
			Help:      "Counter of CREATE TABLE which includes LIST COLUMNS partitioning",
		})
	TelemetryTablePartitionMaxPartitionsCnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "table_partition_max_partition_usage",
			Help:      "Counter of partitions created by CREATE TABLE statements",
		})
	TelemetryAccountLockCnt = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "account_lock_usage",
			Help:      "Counter of locked/unlocked users",
		}, []string{LblAccountLock})
	TelemetryTablePartitionCreateIntervalPartitionsCnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "table_partition_create_interval_partition_usage",
			Help:      "Counter of partitions created by CREATE TABLE INTERVAL statements",
		})
	TelemetryTablePartitionAddIntervalPartitionsCnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "table_partition_add_interval_partition_usage",
			Help:      "Counter of partitions added by ALTER TABLE LAST PARTITION statements",
		})
	TelemetryTablePartitionDropIntervalPartitionsCnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "table_partition_drop_interval_partition_usage",
			Help:      "Counter of partitions added by ALTER TABLE FIRST PARTITION statements",
		})
	TelemetryExchangePartitionCnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "exchange_partition_usage",
			Help:      "Counter of usage of exchange partition statements",
		})
	TelemetryAddIndexIngestCnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "add_index_ingest_usage",
			Help:      "Counter of usage of add index acceleration solution",
		})
	TelemetryFlashbackClusterCnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "flashback_cluster_usage",
			Help:      "Counter of usage of flashback cluster",
		})
	TelemetryIndexMergeUsage = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "index_merge_usage",
			Help:      "Counter of usage of index merge",
		})
	TelemetryCompactPartitionCnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "compact_partition_usage",
			Help:      "Counter of compact table partition",
		})
	TelemetryReorganizePartitionCnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "reorganize_partition_usage",
			Help:      "Counter of alter table reorganize partition",
		})
	TelemetryDistReorgCnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "distributed_reorg_count",
			Help:      "Counter of usage of distributed reorg DDL tasks count",
		})
	TelemetryStoreBatchedQueryCnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "store_batched_query",
			Help:      "Counter of queries which use store batched coprocessor tasks",
		})
	TelemetryBatchedQueryTaskCnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "batched_query_task",
			Help:      "Counter of coprocessor tasks in batched queries",
		})
	TelemetryStoreBatchedCnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "store_batched",
			Help:      "Counter of store batched coprocessor tasks",
		})
	TelemetryStoreBatchedFallbackCnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "telemetry",
			Name:      "store_batched_fallback",
			Help:      "Counter of store batched fallback coprocessor tasks",
		})
)

// readCounter reads the value of a prometheus.Counter.
// Returns -1 when failing to read the value.
func readCounter(m prometheus.Counter) int64 {
	// Actually, it's not recommended to read the value of prometheus metric types directly:
	// https://github.com/prometheus/client_golang/issues/486#issuecomment-433345239
	pb := &dto.Metric{}
	// It's impossible to return an error though.
	if err := m.Write(pb); err != nil {
		return -1
	}
	return int64(pb.GetCounter().GetValue())
}

// CTEUsageCounter records the usages of CTE.
type CTEUsageCounter struct {
	NonRecursiveCTEUsed int64 `json:"nonRecursiveCTEUsed"`
	RecursiveUsed       int64 `json:"recursiveUsed"`
	NonCTEUsed          int64 `json:"nonCTEUsed"`
}

// Sub returns the difference of two counters.
func (c CTEUsageCounter) Sub(rhs CTEUsageCounter) CTEUsageCounter {
	return CTEUsageCounter{
		NonRecursiveCTEUsed: c.NonRecursiveCTEUsed - rhs.NonRecursiveCTEUsed,
		RecursiveUsed:       c.RecursiveUsed - rhs.RecursiveUsed,
		NonCTEUsed:          c.NonCTEUsed - rhs.NonCTEUsed,
	}
}

// GetCTECounter gets the TxnCommitCounter.
func GetCTECounter() CTEUsageCounter {
	return CTEUsageCounter{
		NonRecursiveCTEUsed: readCounter(TelemetrySQLCTECnt.With(prometheus.Labels{LblCTEType: "nonRecurCTE"})),
		RecursiveUsed:       readCounter(TelemetrySQLCTECnt.With(prometheus.Labels{LblCTEType: "recurCTE"})),
		NonCTEUsed:          readCounter(TelemetrySQLCTECnt.With(prometheus.Labels{LblCTEType: "notCTE"})),
	}
}

// AccountLockCounter records the number of lock users/roles
type AccountLockCounter struct {
	LockUser          int64 `json:"lockUser"`
	UnlockUser        int64 `json:"unlockUser"`
	CreateOrAlterUser int64 `json:"createOrAlterUser"`
}

// Sub returns the difference of two counters.
func (c AccountLockCounter) Sub(rhs AccountLockCounter) AccountLockCounter {
	return AccountLockCounter{
		LockUser:          c.LockUser - rhs.LockUser,
		UnlockUser:        c.UnlockUser - rhs.UnlockUser,
		CreateOrAlterUser: c.CreateOrAlterUser - rhs.CreateOrAlterUser,
	}
}

// GetAccountLockCounter gets the AccountLockCounter
func GetAccountLockCounter() AccountLockCounter {
	return AccountLockCounter{
		LockUser:          readCounter(TelemetryAccountLockCnt.With(prometheus.Labels{LblAccountLock: "lockUser"})),
		UnlockUser:        readCounter(TelemetryAccountLockCnt.With(prometheus.Labels{LblAccountLock: "unlockUser"})),
		CreateOrAlterUser: readCounter(TelemetryAccountLockCnt.With(prometheus.Labels{LblAccountLock: "createOrAlterUser"})),
	}
}

// MultiSchemaChangeUsageCounter records the usages of multi-schema change.
type MultiSchemaChangeUsageCounter struct {
	MultiSchemaChangeUsed int64 `json:"multi_schema_change_used"`
}

// Sub returns the difference of two counters.
func (c MultiSchemaChangeUsageCounter) Sub(rhs MultiSchemaChangeUsageCounter) MultiSchemaChangeUsageCounter {
	return MultiSchemaChangeUsageCounter{
		MultiSchemaChangeUsed: c.MultiSchemaChangeUsed - rhs.MultiSchemaChangeUsed,
	}
}

// GetMultiSchemaCounter gets the TxnCommitCounter.
func GetMultiSchemaCounter() MultiSchemaChangeUsageCounter {
	return MultiSchemaChangeUsageCounter{
		MultiSchemaChangeUsed: readCounter(TelemetryMultiSchemaChangeCnt),
	}
}

// TablePartitionUsageCounter records the usages of table partition.
type TablePartitionUsageCounter struct {
	TablePartitionCnt                         int64 `json:"table_partition_cnt"`
	TablePartitionListCnt                     int64 `json:"table_partition_list_cnt"`
	TablePartitionRangeCnt                    int64 `json:"table_partition_range_cnt"`
	TablePartitionHashCnt                     int64 `json:"table_partition_hash_cnt"`
	TablePartitionRangeColumnsCnt             int64 `json:"table_partition_range_columns_cnt"`
	TablePartitionRangeColumnsGt1Cnt          int64 `json:"table_partition_range_columns_gt_1_cnt"`
	TablePartitionRangeColumnsGt2Cnt          int64 `json:"table_partition_range_columns_gt_2_cnt"`
	TablePartitionRangeColumnsGt3Cnt          int64 `json:"table_partition_range_columns_gt_3_cnt"`
	TablePartitionListColumnsCnt              int64 `json:"table_partition_list_columns_cnt"`
	TablePartitionMaxPartitionsCnt            int64 `json:"table_partition_max_partitions_cnt"`
	TablePartitionCreateIntervalPartitionsCnt int64 `json:"table_partition_create_interval_partitions_cnt"`
	TablePartitionAddIntervalPartitionsCnt    int64 `json:"table_partition_add_interval_partitions_cnt"`
	TablePartitionDropIntervalPartitionsCnt   int64 `json:"table_partition_drop_interval_partitions_cnt"`
	TablePartitionComactCnt                   int64 `json:"table_TablePartitionComactCnt"`
	TablePartitionReorganizePartitionCnt      int64 `json:"table_reorganize_partition_cnt"`
}

// ExchangePartitionUsageCounter records the usages of exchange partition.
type ExchangePartitionUsageCounter struct {
	ExchangePartitionCnt int64 `json:"exchange_partition_cnt"`
}

// Sub returns the difference of two counters.
func (c ExchangePartitionUsageCounter) Sub(rhs ExchangePartitionUsageCounter) ExchangePartitionUsageCounter {
	return ExchangePartitionUsageCounter{
		ExchangePartitionCnt: c.ExchangePartitionCnt - rhs.ExchangePartitionCnt,
	}
}

// GetExchangePartitionCounter gets the TxnCommitCounter.
func GetExchangePartitionCounter() ExchangePartitionUsageCounter {
	return ExchangePartitionUsageCounter{
		ExchangePartitionCnt: readCounter(TelemetryExchangePartitionCnt),
	}
}

// Cal returns the difference of two counters.
func (c TablePartitionUsageCounter) Cal(rhs TablePartitionUsageCounter) TablePartitionUsageCounter {
	return TablePartitionUsageCounter{
		TablePartitionCnt:                         c.TablePartitionCnt - rhs.TablePartitionCnt,
		TablePartitionListCnt:                     c.TablePartitionListCnt - rhs.TablePartitionListCnt,
		TablePartitionRangeCnt:                    c.TablePartitionRangeCnt - rhs.TablePartitionRangeCnt,
		TablePartitionHashCnt:                     c.TablePartitionHashCnt - rhs.TablePartitionHashCnt,
		TablePartitionRangeColumnsCnt:             c.TablePartitionRangeColumnsCnt - rhs.TablePartitionRangeColumnsCnt,
		TablePartitionRangeColumnsGt1Cnt:          c.TablePartitionRangeColumnsGt1Cnt - rhs.TablePartitionRangeColumnsGt1Cnt,
		TablePartitionRangeColumnsGt2Cnt:          c.TablePartitionRangeColumnsGt2Cnt - rhs.TablePartitionRangeColumnsGt2Cnt,
		TablePartitionRangeColumnsGt3Cnt:          c.TablePartitionRangeColumnsGt3Cnt - rhs.TablePartitionRangeColumnsGt3Cnt,
		TablePartitionListColumnsCnt:              c.TablePartitionListColumnsCnt - rhs.TablePartitionListColumnsCnt,
		TablePartitionMaxPartitionsCnt:            mathutil.Max(c.TablePartitionMaxPartitionsCnt-rhs.TablePartitionMaxPartitionsCnt, rhs.TablePartitionMaxPartitionsCnt),
		TablePartitionCreateIntervalPartitionsCnt: c.TablePartitionCreateIntervalPartitionsCnt - rhs.TablePartitionCreateIntervalPartitionsCnt,
		TablePartitionAddIntervalPartitionsCnt:    c.TablePartitionAddIntervalPartitionsCnt - rhs.TablePartitionAddIntervalPartitionsCnt,
		TablePartitionDropIntervalPartitionsCnt:   c.TablePartitionDropIntervalPartitionsCnt - rhs.TablePartitionDropIntervalPartitionsCnt,
		TablePartitionComactCnt:                   c.TablePartitionComactCnt - rhs.TablePartitionComactCnt,
		TablePartitionReorganizePartitionCnt:      c.TablePartitionReorganizePartitionCnt - rhs.TablePartitionReorganizePartitionCnt,
	}
}

// ResetTablePartitionCounter gets the TxnCommitCounter.
func ResetTablePartitionCounter(pre TablePartitionUsageCounter) TablePartitionUsageCounter {
	return TablePartitionUsageCounter{
		TablePartitionCnt:                    readCounter(TelemetryTablePartitionCnt),
		TablePartitionListCnt:                readCounter(TelemetryTablePartitionListCnt),
		TablePartitionRangeCnt:               readCounter(TelemetryTablePartitionRangeCnt),
		TablePartitionHashCnt:                readCounter(TelemetryTablePartitionHashCnt),
		TablePartitionRangeColumnsCnt:        readCounter(TelemetryTablePartitionRangeColumnsCnt),
		TablePartitionRangeColumnsGt1Cnt:     readCounter(TelemetryTablePartitionRangeColumnsGt1Cnt),
		TablePartitionRangeColumnsGt2Cnt:     readCounter(TelemetryTablePartitionRangeColumnsGt2Cnt),
		TablePartitionRangeColumnsGt3Cnt:     readCounter(TelemetryTablePartitionRangeColumnsGt3Cnt),
		TablePartitionListColumnsCnt:         readCounter(TelemetryTablePartitionListColumnsCnt),
		TablePartitionMaxPartitionsCnt:       mathutil.Max(readCounter(TelemetryTablePartitionMaxPartitionsCnt)-pre.TablePartitionMaxPartitionsCnt, pre.TablePartitionMaxPartitionsCnt),
		TablePartitionReorganizePartitionCnt: readCounter(TelemetryReorganizePartitionCnt),
	}
}

// GetTablePartitionCounter gets the TxnCommitCounter.
func GetTablePartitionCounter() TablePartitionUsageCounter {
	return TablePartitionUsageCounter{
		TablePartitionCnt:                         readCounter(TelemetryTablePartitionCnt),
		TablePartitionListCnt:                     readCounter(TelemetryTablePartitionListCnt),
		TablePartitionRangeCnt:                    readCounter(TelemetryTablePartitionRangeCnt),
		TablePartitionHashCnt:                     readCounter(TelemetryTablePartitionHashCnt),
		TablePartitionRangeColumnsCnt:             readCounter(TelemetryTablePartitionRangeColumnsCnt),
		TablePartitionRangeColumnsGt1Cnt:          readCounter(TelemetryTablePartitionRangeColumnsGt1Cnt),
		TablePartitionRangeColumnsGt2Cnt:          readCounter(TelemetryTablePartitionRangeColumnsGt2Cnt),
		TablePartitionRangeColumnsGt3Cnt:          readCounter(TelemetryTablePartitionRangeColumnsGt3Cnt),
		TablePartitionListColumnsCnt:              readCounter(TelemetryTablePartitionListColumnsCnt),
		TablePartitionMaxPartitionsCnt:            readCounter(TelemetryTablePartitionMaxPartitionsCnt),
		TablePartitionCreateIntervalPartitionsCnt: readCounter(TelemetryTablePartitionCreateIntervalPartitionsCnt),
		TablePartitionAddIntervalPartitionsCnt:    readCounter(TelemetryTablePartitionAddIntervalPartitionsCnt),
		TablePartitionDropIntervalPartitionsCnt:   readCounter(TelemetryTablePartitionDropIntervalPartitionsCnt),
		TablePartitionComactCnt:                   readCounter(TelemetryCompactPartitionCnt),
		TablePartitionReorganizePartitionCnt:      readCounter(TelemetryReorganizePartitionCnt),
	}
}

// NonTransactionalStmtCounter records the usages of non-transactional statements.
type NonTransactionalStmtCounter struct {
	DeleteCount int64 `json:"delete"`
	UpdateCount int64 `json:"update"`
	InsertCount int64 `json:"insert"`
}

// Sub returns the difference of two counters.
func (n NonTransactionalStmtCounter) Sub(rhs NonTransactionalStmtCounter) NonTransactionalStmtCounter {
	return NonTransactionalStmtCounter{
		DeleteCount: n.DeleteCount - rhs.DeleteCount,
		UpdateCount: n.UpdateCount - rhs.UpdateCount,
		InsertCount: n.InsertCount - rhs.InsertCount,
	}
}

// GetNonTransactionalStmtCounter gets the NonTransactionalStmtCounter.
func GetNonTransactionalStmtCounter() NonTransactionalStmtCounter {
	return NonTransactionalStmtCounter{
		DeleteCount: readCounter(NonTransactionalDMLCount.With(prometheus.Labels{LblType: "delete"})),
		UpdateCount: readCounter(NonTransactionalDMLCount.With(prometheus.Labels{LblType: "update"})),
		InsertCount: readCounter(NonTransactionalDMLCount.With(prometheus.Labels{LblType: "insert"})),
	}
}

// GetSavepointStmtCounter gets the savepoint statement executed counter.
func GetSavepointStmtCounter() int64 {
	return readCounter(StmtNodeCounter.With(prometheus.Labels{LblType: "Savepoint", LblDb: ""}))
}

// GetLazyPessimisticUniqueCheckSetCounter returns the counter of setting tidb_constraint_check_in_place_pessimistic to false.
func GetLazyPessimisticUniqueCheckSetCounter() int64 {
	return readCounter(LazyPessimisticUniqueCheckSetCount)
}

// DDLUsageCounter records the usages of DDL related features.
type DDLUsageCounter struct {
	AddIndexIngestUsed   int64 `json:"add_index_ingest_used"`
	MetadataLockUsed     bool  `json:"metadata_lock_used"`
	FlashbackClusterUsed int64 `json:"flashback_cluster_used"`
	DistReorgUsed        int64 `json:"dist_reorg_used"`
}

// Sub returns the difference of two counters.
func (a DDLUsageCounter) Sub(rhs DDLUsageCounter) DDLUsageCounter {
	return DDLUsageCounter{
		AddIndexIngestUsed:   a.AddIndexIngestUsed - rhs.AddIndexIngestUsed,
		FlashbackClusterUsed: a.FlashbackClusterUsed - rhs.FlashbackClusterUsed,
		DistReorgUsed:        a.DistReorgUsed - rhs.DistReorgUsed,
	}
}

// GetDDLUsageCounter gets the add index acceleration solution counts.
func GetDDLUsageCounter() DDLUsageCounter {
	return DDLUsageCounter{
		AddIndexIngestUsed:   readCounter(TelemetryAddIndexIngestCnt),
		FlashbackClusterUsed: readCounter(TelemetryFlashbackClusterCnt),
		DistReorgUsed:        readCounter(TelemetryDistReorgCnt),
	}
}

// IndexMergeUsageCounter records the usages of IndexMerge feature.
type IndexMergeUsageCounter struct {
	IndexMergeUsed int64 `json:"index_merge_used"`
}

// Sub returns the difference of two counters.
func (i IndexMergeUsageCounter) Sub(rhs IndexMergeUsageCounter) IndexMergeUsageCounter {
	return IndexMergeUsageCounter{
		IndexMergeUsed: i.IndexMergeUsed - rhs.IndexMergeUsed,
	}
}

// GetIndexMergeCounter gets the IndexMerge usage counter.
func GetIndexMergeCounter() IndexMergeUsageCounter {
	return IndexMergeUsageCounter{
		IndexMergeUsed: readCounter(TelemetryIndexMergeUsage),
	}
}

// StoreBatchCoprCounter records the usages of batch copr statements.
type StoreBatchCoprCounter struct {
	// BatchSize is the global value of `tidb_store_batch_size`
	BatchSize int `json:"batch_size"`
	// BatchedQuery is the counter of queries that use this feature.
	BatchedQuery int64 `json:"query"`
	// BatchedQueryTask is the counter of total tasks in queries above.
	BatchedQueryTask int64 `json:"tasks"`
	// BatchedCount is the counter of successfully batched tasks.
	BatchedCount int64 `json:"batched"`
	// BatchedFallbackCount is the counter of fallback batched tasks by region miss.
	BatchedFallbackCount int64 `json:"batched_fallback"`
}

// Sub returns the difference of two counters.
func (n StoreBatchCoprCounter) Sub(rhs StoreBatchCoprCounter) StoreBatchCoprCounter {
	return StoreBatchCoprCounter{
		BatchedQuery:         n.BatchedQuery - rhs.BatchedQuery,
		BatchedQueryTask:     n.BatchedQueryTask - rhs.BatchedQueryTask,
		BatchedCount:         n.BatchedCount - rhs.BatchedCount,
		BatchedFallbackCount: n.BatchedFallbackCount - rhs.BatchedFallbackCount,
	}
}

// GetStoreBatchCoprCounter gets the IndexMerge usage counter.
func GetStoreBatchCoprCounter() StoreBatchCoprCounter {
	return StoreBatchCoprCounter{
		BatchedQuery:         readCounter(TelemetryStoreBatchedQueryCnt),
		BatchedQueryTask:     readCounter(TelemetryBatchedQueryTaskCnt),
		BatchedCount:         readCounter(TelemetryStoreBatchedCnt),
		BatchedFallbackCount: readCounter(TelemetryStoreBatchedFallbackCnt),
	}
}

// AggressiveLockingUsageCounter records the usage of Aggressive Locking feature of pessimistic transaction.
type AggressiveLockingUsageCounter struct {
	TxnAggressiveLockingUsed      int64 `json:"txn_aggressive_locking_used"`
	TxnAggressiveLockingEffective int64 `json:"txn_aggressive_locking_effective"`
}

// Sub returns the difference of two counters.
func (i AggressiveLockingUsageCounter) Sub(rhs AggressiveLockingUsageCounter) AggressiveLockingUsageCounter {
	return AggressiveLockingUsageCounter{
		TxnAggressiveLockingUsed:      i.TxnAggressiveLockingUsed - rhs.TxnAggressiveLockingUsed,
		TxnAggressiveLockingEffective: i.TxnAggressiveLockingEffective - rhs.TxnAggressiveLockingEffective,
	}
}

// GetAggressiveLockingUsageCounter returns the Aggressive Locking usage counter.
func GetAggressiveLockingUsageCounter() AggressiveLockingUsageCounter {
	return AggressiveLockingUsageCounter{
		TxnAggressiveLockingUsed:      readCounter(AggressiveLockingUsageCount.WithLabelValues(LblAggressiveLockingTxnUsed)),
		TxnAggressiveLockingEffective: readCounter(AggressiveLockingUsageCount.WithLabelValues(LblAggressiveLockingTxnEffective)),
	}
}
