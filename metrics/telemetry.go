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
	TablePartitionUsed             int64 `json:"table_partition_used"`
	TablePartitionListUsed         int64 `json:"table_partition_list_used"`
	TablePartitionRangeUsed        int64 `json:"table_partition_range_used"`
	TablePartitionHashUsed         int64 `json:"table_partition_hash_used"`
	TablePartitionRangeColumnsUsed int64 `json:"table_partition_range_columns_used"`
	TablePartitionListColumnsUsed  int64 `json:"table_partition_list_columns_used"`
	TablePartitionMaxPartitionsNum int64 `json:"table_partition_max_partitions_num"`
}

// Cal returns the difference of two counters.
func (c TablePartitionUsageCounter) Cal(rhs TablePartitionUsageCounter) TablePartitionUsageCounter {
	return TablePartitionUsageCounter{
		TablePartitionUsed:             c.TablePartitionUsed - rhs.TablePartitionUsed,
		TablePartitionListUsed:         c.TablePartitionListUsed - rhs.TablePartitionListUsed,
		TablePartitionRangeUsed:        c.TablePartitionRangeUsed - rhs.TablePartitionRangeUsed,
		TablePartitionHashUsed:         c.TablePartitionHashUsed - rhs.TablePartitionHashUsed,
		TablePartitionRangeColumnsUsed: c.TablePartitionRangeColumnsUsed - rhs.TablePartitionRangeColumnsUsed,
		TablePartitionListColumnsUsed:  c.TablePartitionListColumnsUsed - rhs.TablePartitionListColumnsUsed,
		TablePartitionMaxPartitionsNum: mathutil.Max(c.TablePartitionMaxPartitionsNum-rhs.TablePartitionMaxPartitionsNum, rhs.TablePartitionMaxPartitionsNum),
	}
}

// ResetTablePartitionCounter gets the TxnCommitCounter.
func ResetTablePartitionCounter(pre TablePartitionUsageCounter) TablePartitionUsageCounter {
	return TablePartitionUsageCounter{
		TablePartitionUsed:             readCounter(TelemetryTablePartitionCnt),
		TablePartitionListUsed:         readCounter(TelemetryTablePartitionListCnt),
		TablePartitionRangeUsed:        readCounter(TelemetryTablePartitionRangeCnt),
		TablePartitionHashUsed:         readCounter(TelemetryTablePartitionHashCnt),
		TablePartitionRangeColumnsUsed: readCounter(TelemetryTablePartitionRangeColumnsCnt),
		TablePartitionListColumnsUsed:  readCounter(TelemetryTablePartitionListColumnsCnt),
		TablePartitionMaxPartitionsNum: mathutil.Max(readCounter(TelemetryTablePartitionMaxPartitionsCnt)-pre.TablePartitionMaxPartitionsNum, pre.TablePartitionMaxPartitionsNum),
	}
}

// GetTablePartitionCounter gets the TxnCommitCounter.
func GetTablePartitionCounter() TablePartitionUsageCounter {
	return TablePartitionUsageCounter{
		TablePartitionUsed:             readCounter(TelemetryTablePartitionCnt),
		TablePartitionListUsed:         readCounter(TelemetryTablePartitionListCnt),
		TablePartitionRangeUsed:        readCounter(TelemetryTablePartitionRangeCnt),
		TablePartitionHashUsed:         readCounter(TelemetryTablePartitionHashCnt),
		TablePartitionRangeColumnsUsed: readCounter(TelemetryTablePartitionRangeColumnsCnt),
		TablePartitionListColumnsUsed:  readCounter(TelemetryTablePartitionListColumnsCnt),
		TablePartitionMaxPartitionsNum: readCounter(TelemetryTablePartitionMaxPartitionsCnt),
	}
}

// NonTransactionalStmtCounter records the usages of non-transactional statements.
type NonTransactionalStmtCounter struct {
	DeleteCount int64 `json:"delete"`
}

// Sub returns the difference of two counters.
func (n NonTransactionalStmtCounter) Sub(rhs NonTransactionalStmtCounter) NonTransactionalStmtCounter {
	return NonTransactionalStmtCounter{
		DeleteCount: n.DeleteCount - rhs.DeleteCount,
	}
}

// GetNonTransactionalStmtCounter gets the NonTransactionalStmtCounter.
func GetNonTransactionalStmtCounter() NonTransactionalStmtCounter {
	return NonTransactionalStmtCounter{
		DeleteCount: readCounter(NonTransactionalDeleteCount),
	}
}
