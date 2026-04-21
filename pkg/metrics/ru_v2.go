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
}
