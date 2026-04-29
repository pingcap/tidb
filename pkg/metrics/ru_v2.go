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

package metrics

import (
	metricscommon "github.com/pingcap/tidb/pkg/metrics/common"
	"github.com/prometheus/client_golang/prometheus"
)

// RUv2 metrics.
var (
	RUV2ResultChunkCells        prometheus.Counter
	RUV2ExecutorL1              *prometheus.CounterVec
	RUV2ExecutorL2              *prometheus.CounterVec
	RUV2ExecutorL3              *prometheus.CounterVec
	RUV2ExecutorL5InsertRows    prometheus.Counter
	RUV2PlanCnt                 prometheus.Counter
	RUV2PlanDeriveStatsPaths    prometheus.Counter
	RUV2ResourceManagerReadCnt  prometheus.Counter
	RUV2ResourceManagerWriteCnt prometheus.Counter
	RUV2SessionParserTotal      prometheus.Counter
	RUV2TxnCnt                  prometheus.Counter

	RUV2TiKVKVEngineCacheMiss             prometheus.Counter
	RUV2TiKVCoprocessorExecutorIterations prometheus.Counter
	RUV2TiKVCoprocessorResponseBytes      prometheus.Counter
	RUV2TiKVRaftstoreStoreWriteTriggerWB  prometheus.Counter
	RUV2TiKVStorageProcessedKeysBatchGet  prometheus.Counter
	RUV2TiKVStorageProcessedKeysGet       prometheus.Counter
	RUV2TiKVCoprocessorWorkTotal          *prometheus.CounterVec
)

var (
	ruv2ExecutorL1BatchPointGetExec prometheus.Counter
	ruv2ExecutorL1PointGetExecutor  prometheus.Counter
	ruv2ExecutorL1LimitExec         prometheus.Counter

	ruv2ExecutorL2HashAggExec        prometheus.Counter
	ruv2ExecutorL2HashJoinExec       prometheus.Counter
	ruv2ExecutorL2IndexLookUpJoin    prometheus.Counter
	ruv2ExecutorL2IndexLookUpExec    prometheus.Counter
	ruv2ExecutorL2IndexReaderExec    prometheus.Counter
	ruv2ExecutorL2MemTableReaderExec prometheus.Counter
	ruv2ExecutorL2SelectionExec      prometheus.Counter
	ruv2ExecutorL2TableDualExec      prometheus.Counter
	ruv2ExecutorL2TableReaderExec    prometheus.Counter
	ruv2ExecutorL2UnionScanExec      prometheus.Counter
	ruv2ExecutorL2SelectLockExec     prometheus.Counter

	ruv2ExecutorL3SortExec      prometheus.Counter
	ruv2ExecutorL3StreamAggExec prometheus.Counter

	ruv2TiKVCoprocessorWorkTotalBatchIndexScan    prometheus.Counter
	ruv2TiKVCoprocessorWorkTotalBatchTableScan    prometheus.Counter
	ruv2TiKVCoprocessorWorkTotalBatchSelection    prometheus.Counter
	ruv2TiKVCoprocessorWorkTotalBatchTopN         prometheus.Counter
	ruv2TiKVCoprocessorWorkTotalBatchLimit        prometheus.Counter
	ruv2TiKVCoprocessorWorkTotalBatchSimpleAggr   prometheus.Counter
	ruv2TiKVCoprocessorWorkTotalBatchFastHashAggr prometheus.Counter
)

// InitRUV2Metrics initializes RUv2 metrics.
func InitRUV2Metrics() {
	RUV2ResultChunkCells = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv2",
			Name:      "result_chunk_cells",
			Help:      "Counter of result chunk cells for RU v2.",
		},
	)

	RUV2ExecutorL1 = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv2",
			Name:      "executor_l1",
			Help:      "Counter of executor L1 input/output for RU v2.",
		}, []string{LblType},
	)

	RUV2ExecutorL2 = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv2",
			Name:      "executor_l2",
			Help:      "Counter of executor L2 input/output for RU v2.",
		}, []string{LblType},
	)

	RUV2ExecutorL3 = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv2",
			Name:      "executor_l3",
			Help:      "Counter of executor L3 input/output for RU v2.",
		}, []string{LblType},
	)

	RUV2ExecutorL5InsertRows = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv2",
			Name:      "executor_l5_insert_rows",
			Help:      "Counter of insert rows for RU v2.",
		},
	)

	RUV2PlanCnt = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv2",
			Name:      "plan_cnt",
			Help:      "Counter of plan builder executions for RU v2.",
		},
	)

	RUV2PlanDeriveStatsPaths = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv2",
			Name:      "plan_derive_stats_paths",
			Help:      "Counter of derive stats paths for RU v2.",
		},
	)

	RUV2ResourceManagerReadCnt = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv2",
			Name:      "resource_manager_read_cnt",
			Help:      "Counter of resource manager read requests for RU v2.",
		},
	)

	RUV2ResourceManagerWriteCnt = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv2",
			Name:      "resource_manager_write_cnt",
			Help:      "Counter of resource manager write requests for RU v2.",
		},
	)

	RUV2SessionParserTotal = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv2",
			Name:      "session_parser_total",
			Help:      "Counter of session parser executions for RU v2.",
		},
	)

	RUV2TxnCnt = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv2",
			Name:      "txn_cnt",
			Help:      "Counter of transactions for RU v2.",
		},
	)

	RUV2TiKVKVEngineCacheMiss = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv2",
			Name:      "tikv_kv_engine_cache_miss",
			Help:      "Counter of TiKV KV engine cache miss for RU v2.",
		},
	)

	RUV2TiKVCoprocessorExecutorIterations = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv2",
			Name:      "tikv_coprocessor_executor_iterations",
			Help:      "Counter of TiKV coprocessor executor iterations for RU v2.",
		},
	)

	RUV2TiKVCoprocessorResponseBytes = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv2",
			Name:      "tikv_coprocessor_response_bytes",
			Help:      "Counter of TiKV coprocessor response bytes for RU v2.",
		},
	)

	RUV2TiKVRaftstoreStoreWriteTriggerWB = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv2",
			Name:      "tikv_raftstore_store_write_trigger_wb_bytes",
			Help:      "Counter of TiKV raftstore write trigger WB bytes for RU v2.",
		},
	)

	RUV2TiKVStorageProcessedKeysBatchGet = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv2",
			Name:      "tikv_storage_processed_keys_batch_get",
			Help:      "Counter of TiKV storage processed keys (batch get) for RU v2.",
		},
	)

	RUV2TiKVStorageProcessedKeysGet = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv2",
			Name:      "tikv_storage_processed_keys_get",
			Help:      "Counter of TiKV storage processed keys (get) for RU v2.",
		},
	)

	RUV2TiKVCoprocessorWorkTotal = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ruv2",
			Name:      "tikv_coprocessor_executor_work_total",
			Help:      "Counter of TiKV coprocessor executor work for RU v2.",
		}, []string{LblType},
	)

	initRUV2CachedLabelCounters()
}

func initRUV2CachedLabelCounters() {
	ruv2ExecutorL1BatchPointGetExec = RUV2ExecutorL1.WithLabelValues("BatchPointGetExec")
	ruv2ExecutorL1PointGetExecutor = RUV2ExecutorL1.WithLabelValues("PointGetExecutor")
	ruv2ExecutorL1LimitExec = RUV2ExecutorL1.WithLabelValues("LimitExec")

	ruv2ExecutorL2HashAggExec = RUV2ExecutorL2.WithLabelValues("HashAggExec")
	ruv2ExecutorL2HashJoinExec = RUV2ExecutorL2.WithLabelValues("HashJoinExec")
	ruv2ExecutorL2IndexLookUpJoin = RUV2ExecutorL2.WithLabelValues("IndexLookUpJoin")
	ruv2ExecutorL2IndexLookUpExec = RUV2ExecutorL2.WithLabelValues("IndexLookUpExecutor")
	ruv2ExecutorL2IndexReaderExec = RUV2ExecutorL2.WithLabelValues("IndexReaderExecutor")
	ruv2ExecutorL2MemTableReaderExec = RUV2ExecutorL2.WithLabelValues("MemTableReaderExec")
	ruv2ExecutorL2SelectionExec = RUV2ExecutorL2.WithLabelValues("SelectionExec")
	ruv2ExecutorL2TableDualExec = RUV2ExecutorL2.WithLabelValues("TableDualExec")
	ruv2ExecutorL2TableReaderExec = RUV2ExecutorL2.WithLabelValues("TableReaderExecutor")
	ruv2ExecutorL2UnionScanExec = RUV2ExecutorL2.WithLabelValues("UnionScanExec")
	ruv2ExecutorL2SelectLockExec = RUV2ExecutorL2.WithLabelValues("SelectLockExec")

	ruv2ExecutorL3SortExec = RUV2ExecutorL3.WithLabelValues("SortExec")
	ruv2ExecutorL3StreamAggExec = RUV2ExecutorL3.WithLabelValues("StreamAggExec")

	ruv2TiKVCoprocessorWorkTotalBatchIndexScan = RUV2TiKVCoprocessorWorkTotal.WithLabelValues("BatchIndexScan")
	ruv2TiKVCoprocessorWorkTotalBatchTableScan = RUV2TiKVCoprocessorWorkTotal.WithLabelValues("BatchTableScan")
	ruv2TiKVCoprocessorWorkTotalBatchSelection = RUV2TiKVCoprocessorWorkTotal.WithLabelValues("BatchSelection")
	ruv2TiKVCoprocessorWorkTotalBatchTopN = RUV2TiKVCoprocessorWorkTotal.WithLabelValues("BatchTopN")
	ruv2TiKVCoprocessorWorkTotalBatchLimit = RUV2TiKVCoprocessorWorkTotal.WithLabelValues("BatchLimit")
	ruv2TiKVCoprocessorWorkTotalBatchSimpleAggr = RUV2TiKVCoprocessorWorkTotal.WithLabelValues("BatchSimpleAggr")
	ruv2TiKVCoprocessorWorkTotalBatchFastHashAggr = RUV2TiKVCoprocessorWorkTotal.WithLabelValues("BatchFastHashAggr")
}

// RUV2ExecutorCounter returns a cached RUv2 executor counter when the label is known.
func RUV2ExecutorCounter(level int, label string) prometheus.Counter {
	switch level {
	case 1:
		switch label {
		case "BatchPointGetExec":
			return ruv2ExecutorL1BatchPointGetExec
		case "PointGetExecutor":
			return ruv2ExecutorL1PointGetExecutor
		case "LimitExec":
			return ruv2ExecutorL1LimitExec
		default:
			return RUV2ExecutorL1.WithLabelValues(label)
		}
	case 2:
		switch label {
		case "HashAggExec":
			return ruv2ExecutorL2HashAggExec
		case "HashJoinExec":
			return ruv2ExecutorL2HashJoinExec
		case "IndexLookUpJoin":
			return ruv2ExecutorL2IndexLookUpJoin
		case "IndexLookUpExecutor":
			return ruv2ExecutorL2IndexLookUpExec
		case "IndexReaderExecutor":
			return ruv2ExecutorL2IndexReaderExec
		case "MemTableReaderExec":
			return ruv2ExecutorL2MemTableReaderExec
		case "SelectionExec":
			return ruv2ExecutorL2SelectionExec
		case "TableDualExec":
			return ruv2ExecutorL2TableDualExec
		case "TableReaderExecutor":
			return ruv2ExecutorL2TableReaderExec
		case "UnionScanExec":
			return ruv2ExecutorL2UnionScanExec
		case "SelectLockExec":
			return ruv2ExecutorL2SelectLockExec
		default:
			return RUV2ExecutorL2.WithLabelValues(label)
		}
	case 3:
		switch label {
		case "SortExec":
			return ruv2ExecutorL3SortExec
		case "StreamAggExec":
			return ruv2ExecutorL3StreamAggExec
		default:
			return RUV2ExecutorL3.WithLabelValues(label)
		}
	default:
		return nil
	}
}

// RUV2TiKVCoprocessorWorkTotalCounter returns a cached TiKV coprocessor RUv2 counter when the label is known.
func RUV2TiKVCoprocessorWorkTotalCounter(label string) prometheus.Counter {
	switch label {
	case "BatchIndexScan":
		return ruv2TiKVCoprocessorWorkTotalBatchIndexScan
	case "BatchTableScan":
		return ruv2TiKVCoprocessorWorkTotalBatchTableScan
	case "BatchSelection":
		return ruv2TiKVCoprocessorWorkTotalBatchSelection
	case "BatchTopN":
		return ruv2TiKVCoprocessorWorkTotalBatchTopN
	case "BatchLimit":
		return ruv2TiKVCoprocessorWorkTotalBatchLimit
	case "BatchSimpleAggr":
		return ruv2TiKVCoprocessorWorkTotalBatchSimpleAggr
	case "BatchFastHashAggr":
		return ruv2TiKVCoprocessorWorkTotalBatchFastHashAggr
	default:
		return RUV2TiKVCoprocessorWorkTotal.WithLabelValues(label)
	}
}
