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

package txn

import (
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
)

const statementRUV2MetricsInterceptorName = "ruv2-statement-metrics"

// NewStatementRUV2RPCInterceptor creates an interceptor that collects
// statement-level RUv2 request counters and response-side metrics from
// ExecDetailsV2.RuV2.
func NewStatementRUV2RPCInterceptor(ruv2Metrics *execdetails.RUV2Metrics) interceptor.RPCInterceptor {
	if ruv2Metrics == nil {
		return nil
	}
	return newStatementRUV2RPCInterceptorWithGetter(func() *execdetails.RUV2Metrics {
		return ruv2Metrics
	})
}

// newStatementRUV2RPCInterceptorWithGetter creates an interceptor that collects
// statement-level RUv2 request counters and response-side metrics from
// ExecDetailsV2.RuV2 using the metrics returned by getter at request time.
func newStatementRUV2RPCInterceptorWithGetter(getter func() *execdetails.RUV2Metrics) interceptor.RPCInterceptor {
	if getter == nil {
		return nil
	}
	return interceptor.NewRPCInterceptor(statementRUV2MetricsInterceptorName, func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
		return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			resp, err := next(target, req)
			if err == nil && resp != nil {
				ruv2Metrics := getter()
				updateResourceManagerRUV2Metrics(ruv2Metrics, req)
				updateStorageProcessedKeysRUV2Metrics(ruv2Metrics, resp)
			}
			return resp, err
		}
	})
}

func updateResourceManagerRUV2Metrics(ruv2Metrics *execdetails.RUV2Metrics, req *tikvrpc.Request) {
	if ruv2Metrics == nil || req == nil || req.StoreTp != tikvrpc.TiKV || req.IsDebugReq() {
		return
	}
	if req.IsTxnWriteRequest() || req.IsRawWriteRequest() {
		ruv2Metrics.AddResourceManagerWriteCnt(1)
		return
	}
	ruv2Metrics.AddResourceManagerReadCnt(1)
}

func updateStorageProcessedKeysRUV2Metrics(ruv2Metrics *execdetails.RUV2Metrics, resp *tikvrpc.Response) {
	if ruv2Metrics == nil || resp == nil {
		return
	}
	details := resp.GetExecDetailsV2()
	if details == nil || details.RuV2 == nil {
		return
	}
	if details.RuV2.StorageProcessedKeysBatchGet != 0 {
		ruv2Metrics.AddTiKVStorageProcessedKeysBatchGet(int64(details.RuV2.StorageProcessedKeysBatchGet))
	}
	if details.RuV2.StorageProcessedKeysGet != 0 {
		ruv2Metrics.AddTiKVStorageProcessedKeysGet(int64(details.RuV2.StorageProcessedKeysGet))
	}
}
