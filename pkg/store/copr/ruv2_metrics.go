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

package copr

import (
	"context"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/util/execdetails"
)

func updateRUV2MetricsFromExecDetailsV2(ctx context.Context, details *kvrpcpb.ExecDetailsV2) {
	if details == nil || details.RuV2 == nil {
		return
	}
	ru := details.RuV2
	ruv2Metrics := execdetails.RUV2MetricsFromContext(ctx)
	// Coprocessor responses only own cop-side ExecDetailsV2 fields here.
	// StorageProcessedKeysBatchGet/Get are collected by pkg/store/driver/txn for
	// Get/BatchGet responses and are not expected on cop responses.
	if ru.KvEngineCacheMiss != 0 {
		if ruv2Metrics != nil {
			ruv2Metrics.AddTiKVKVEngineCacheMiss(int64(ru.KvEngineCacheMiss))
		}
	}
	if ru.CoprocessorExecutorIterations != 0 {
		if ruv2Metrics != nil {
			ruv2Metrics.AddTiKVCoprocessorExecutorIterations(int64(ru.CoprocessorExecutorIterations))
		}
	}
	if ru.CoprocessorResponseBytes != 0 {
		if ruv2Metrics != nil {
			ruv2Metrics.AddTiKVCoprocessorResponseBytes(int64(ru.CoprocessorResponseBytes))
		}
	}
	if ru.RaftstoreStoreWriteTriggerWbBytes != 0 {
		if ruv2Metrics != nil {
			ruv2Metrics.AddTiKVRaftstoreStoreWriteTriggerWB(int64(ru.RaftstoreStoreWriteTriggerWbBytes))
		}
	}
	if inputs := ru.ExecutorInputs; inputs != nil {
		if inputs.TikvCoprocessorExecutorWorkTotalBatchIndexScan != 0 {
			if ruv2Metrics != nil {
				ruv2Metrics.AddTiKVCoprocessorWorkTotal("BatchIndexScan", int64(inputs.TikvCoprocessorExecutorWorkTotalBatchIndexScan))
			}
		}
		if inputs.TikvCoprocessorExecutorWorkTotalBatchTableScan != 0 {
			if ruv2Metrics != nil {
				ruv2Metrics.AddTiKVCoprocessorWorkTotal("BatchTableScan", int64(inputs.TikvCoprocessorExecutorWorkTotalBatchTableScan))
			}
		}
		if inputs.TikvCoprocessorExecutorWorkTotalBatchSelection != 0 {
			if ruv2Metrics != nil {
				ruv2Metrics.AddTiKVCoprocessorWorkTotal("BatchSelection", int64(inputs.TikvCoprocessorExecutorWorkTotalBatchSelection))
			}
		}
		if inputs.TikvCoprocessorExecutorWorkTotalBatchTopN != 0 {
			if ruv2Metrics != nil {
				ruv2Metrics.AddTiKVCoprocessorWorkTotal("BatchTopN", int64(inputs.TikvCoprocessorExecutorWorkTotalBatchTopN))
			}
		}
		if inputs.TikvCoprocessorExecutorWorkTotalBatchLimit != 0 {
			if ruv2Metrics != nil {
				ruv2Metrics.AddTiKVCoprocessorWorkTotal("BatchLimit", int64(inputs.TikvCoprocessorExecutorWorkTotalBatchLimit))
			}
		}
		if inputs.TikvCoprocessorExecutorWorkTotalBatchSimpleAggr != 0 {
			if ruv2Metrics != nil {
				ruv2Metrics.AddTiKVCoprocessorWorkTotal("BatchSimpleAggr", int64(inputs.TikvCoprocessorExecutorWorkTotalBatchSimpleAggr))
			}
		}
		if inputs.TikvCoprocessorExecutorWorkTotalBatchFastHashAggr != 0 {
			if ruv2Metrics != nil {
				ruv2Metrics.AddTiKVCoprocessorWorkTotal("BatchFastHashAggr", int64(inputs.TikvCoprocessorExecutorWorkTotalBatchFastHashAggr))
			}
		}
	}
}
