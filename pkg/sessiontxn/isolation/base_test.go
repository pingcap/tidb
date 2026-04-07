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

package isolation

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestCurrentStatementRUV2RPCInterceptorCapturesMetrics(t *testing.T) {
	sessVars := variable.NewSessionVars(nil)
	metrics1 := execdetails.NewRUV2Metrics()
	metrics2 := execdetails.NewRUV2Metrics()
	sessVars.RUV2Metrics = metrics1

	it := currentStatementRUV2RPCInterceptor(sessVars)
	require.NotNil(t, it)

	// Simulate the next statement replacing SessionVars.RUV2Metrics while the old
	// statement still has async cleanup RPCs in flight.
	sessVars.RUV2Metrics = metrics2

	wrapFn := it.Wrap(func(_ string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
		switch req.Type {
		case tikvrpc.CmdBatchGet:
			return &tikvrpc.Response{
				Resp: &kvrpcpb.BatchGetResponse{
					ExecDetailsV2: &kvrpcpb.ExecDetailsV2{
						RuV2: &kvrpcpb.RUV2{
							StorageProcessedKeysBatchGet: 2,
						},
					},
				},
			}, nil
		case tikvrpc.CmdPrewrite:
			return &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{}}, nil
		default:
			return &tikvrpc.Response{}, nil
		}
	})

	_, err := wrapFn("tikv-1", &tikvrpc.Request{Type: tikvrpc.CmdBatchGet, StoreTp: tikvrpc.TiKV})
	require.NoError(t, err)
	_, err = wrapFn("tikv-1", &tikvrpc.Request{Type: tikvrpc.CmdPrewrite, StoreTp: tikvrpc.TiKV})
	require.NoError(t, err)

	require.Equal(t, int64(1), metrics1.ResourceManagerReadCnt())
	require.Equal(t, int64(1), metrics1.ResourceManagerWriteCnt())
	require.Equal(t, int64(2), metrics1.TiKVStorageProcessedKeysBatchGet())
	require.Equal(t, int64(0), metrics2.ResourceManagerReadCnt())
	require.Equal(t, int64(0), metrics2.ResourceManagerWriteCnt())
}
