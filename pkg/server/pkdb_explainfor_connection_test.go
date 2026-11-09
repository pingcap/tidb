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

package server

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	utilcodec "github.com/pingcap/tidb/pkg/util/codec"
	utilmock "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

// explainForConnectionTestClient captures the outgoing kv request and returns a mocked response.
type explainForConnectionTestClient struct {
	req       *kv.Request
	resp      kv.Response
	buildResp func(*kv.Request) kv.Response
}

func (c *explainForConnectionTestClient) Send(_ context.Context, req *kv.Request, _ any, _ *kv.ClientSendOption) kv.Response {
	c.req = req
	if c.buildResp != nil {
		return c.buildResp(req)
	}
	return c.resp
}

func (*explainForConnectionTestClient) IsRequestTypeSupported(int64, int64) bool {
	return true
}

// explainForConnectionTestResponse simulates the streaming kv.Response returned by DistSQL.
type explainForConnectionTestResponse struct {
	subsets []kv.ResultSubset
	err     error
	next    int
}

func (r *explainForConnectionTestResponse) Next(context.Context) (kv.ResultSubset, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.next >= len(r.subsets) {
		return nil, nil
	}
	subset := r.subsets[r.next]
	r.next++
	return subset, nil
}

func (*explainForConnectionTestResponse) Close() error { return nil }

// explainForConnectionTestSubset holds one encoded SelectResponse chunk.
type explainForConnectionTestSubset struct {
	data []byte
}

func (s *explainForConnectionTestSubset) GetData() []byte       { return s.data }
func (*explainForConnectionTestSubset) GetStartKey() kv.Key     { return nil }
func (s *explainForConnectionTestSubset) MemSize() int64        { return int64(len(s.data)) }
func (*explainForConnectionTestSubset) RespTime() time.Duration { return 0 }

// TestGetRemoteProcessInfoForExplainSuccess verifies the remote cop request and decoded process info on success.
func TestGetRemoteProcessInfoForExplainSuccess(t *testing.T) {
	client := &explainForConnectionTestClient{
		resp: &explainForConnectionTestResponse{
			subsets: []kv.ResultSubset{
				buildExplainForConnectionTestSubset(t, [][]string{{"root", "127.0.0.1", "encoded-plan"}}),
			},
		},
	}
	execCtx := newExplainForConnectionTestContext(client)

	pi, err := getRemoteProcessInfoForExplain(context.Background(), execCtx, 100, 42)
	require.NoError(t, err)
	require.NotNil(t, client.req)
	require.Equal(t, kv.TiDB, client.req.StoreType)
	require.Equal(t, uint64(100), client.req.TiDBServerID)
	require.Equal(t, uint64(math.MaxUint64), client.req.StartTs)

	dagReq := &tipb.DAGRequest{}
	require.NoError(t, dagReq.Unmarshal(client.req.Data))
	require.Len(t, dagReq.Executors, 1)
	require.Equal(t, tipb.ExecType_TypeExplainForConnection, dagReq.Executors[0].Tp)
	require.NotNil(t, dagReq.Executors[0].ExplainForConnection)
	require.Equal(t, uint64(42), dagReq.Executors[0].ExplainForConnection.GetConnectionId())

	require.Equal(t, &util.ProcessInfo{
		ID:              42,
		User:            "root",
		Host:            "127.0.0.1",
		BriefBinaryPlan: "encoded-plan",
	}, pi)
}

func TestGetRemoteProcessInfoForExplainChunkRPCDisabled(t *testing.T) {
	client := &explainForConnectionTestClient{}
	client.buildResp = func(req *kv.Request) kv.Response {
		t.Helper()

		dagReq := &tipb.DAGRequest{}
		require.NoError(t, dagReq.Unmarshal(req.Data))

		row := []string{"root", "127.0.0.1", "encoded-plan"}
		rowsData := make([]byte, 0)
		for _, offset := range dagReq.OutputOffsets {
			data, err := utilcodec.EncodeValue(time.UTC, nil, types.NewStringDatum(row[offset]))
			require.NoError(t, err)
			rowsData = append(rowsData, data...)
		}

		resp := &tipb.SelectResponse{
			EncodeType: dagReq.GetEncodeType(),
			Chunks:     []tipb.Chunk{{RowsData: rowsData}},
		}
		data, err := resp.Marshal()
		require.NoError(t, err)
		return &explainForConnectionTestResponse{
			subsets: []kv.ResultSubset{&explainForConnectionTestSubset{data: data}},
		}
	}

	execCtx := newExplainForConnectionTestContext(client)
	execCtx.GetSessionVars().EnableChunkRPC = false

	pi, err := getRemoteProcessInfoForExplain(context.Background(), execCtx, 100, 42)
	require.NoError(t, err)
	require.NotNil(t, client.req)

	dagReq := &tipb.DAGRequest{}
	require.NoError(t, dagReq.Unmarshal(client.req.Data))
	require.Equal(t, []uint32{0, 1, 2}, dagReq.OutputOffsets)
	require.Equal(t, &util.ProcessInfo{
		ID:              42,
		User:            "root",
		Host:            "127.0.0.1",
		BriefBinaryPlan: "encoded-plan",
	}, pi)
}

// TestGetRemoteProcessInfoForExplainErrors verifies not found and remote error paths.
func TestGetRemoteProcessInfoForExplainErrors(t *testing.T) {
	tests := []struct {
		name    string
		resp    kv.Response
		wantErr string
	}{
		{
			name: "not_found",
			resp: &explainForConnectionTestResponse{
				subsets: []kv.ResultSubset{buildExplainForConnectionTestSubset(t, nil)},
			},
		},
		{
			name:    "remote_error",
			resp:    &explainForConnectionTestResponse{err: errors.New("boom")},
			wantErr: "read explain for connection result",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &explainForConnectionTestClient{resp: tt.resp}
			execCtx := newExplainForConnectionTestContext(client)

			pi, err := getRemoteProcessInfoForExplain(context.Background(), execCtx, 101, 7)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
			}
			if tt.wantErr == "" {
				require.Nil(t, pi)
			}
		})
	}
}

func newExplainForConnectionTestContext(client kv.Client) *utilmock.Context {
	ctx := utilmock.NewContextDeprecated()
	ctx.Store = &utilmock.Store{Client: client}
	ctx.SetInfoSchema(infoschema.MockInfoSchema(nil))
	return ctx
}

func buildExplainForConnectionTestSubset(t *testing.T, rows [][]string) kv.ResultSubset {
	t.Helper()

	fieldTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeString),
	}
	chk := chunk.New(fieldTypes, len(rows), len(rows))
	for _, row := range rows {
		chk.AppendString(0, row[0])
		chk.AppendString(1, row[1])
		chk.AppendString(2, row[2])
	}

	resp := &tipb.SelectResponse{
		EncodeType: tipb.EncodeType_TypeChunk,
		OutputCounts: []int64{
			int64(len(rows)),
		},
	}
	if len(rows) > 0 {
		resp.Chunks = []tipb.Chunk{{RowsData: chunk.NewCodec(fieldTypes).Encode(chk)}}
	}
	data, err := resp.Marshal()
	require.NoError(t, err)
	return &explainForConnectionTestSubset{data: data}
}
