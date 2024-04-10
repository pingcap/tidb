// Copyright 2023 PingCAP, Inc.
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

package driver

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type mockTiKVClient struct {
	tikv.Client
	mock.Mock
}

func (c *mockTiKVClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	args := c.Called(ctx, addr, req, timeout)
	var resp *tikvrpc.Response
	if v := args.Get(0); v != nil {
		resp = v.(*tikvrpc.Response)
	}
	return resp, args.Error(1)
}

func TestInjectTracingClient(t *testing.T) {
	cases := []struct {
		name            string
		trace           *model.TraceInfo
		existSourceStmt *kvrpcpb.SourceStmt
	}{
		{
			name:  "trace is nil",
			trace: nil,
		},
		{
			name: "trace not nil",
			trace: &model.TraceInfo{
				ConnectionID: 123,
				SessionAlias: "alias123",
			},
		},
		{
			name: "only connection id in trace valid",
			trace: &model.TraceInfo{
				ConnectionID: 456,
			},
		},
		{
			name: "only session alias in trace valid and sourceStmt exists",
			trace: &model.TraceInfo{
				SessionAlias: "alias456",
			},
			existSourceStmt: &kvrpcpb.SourceStmt{},
		},
	}

	cli := &mockTiKVClient{}
	inject := injectTraceClient{Client: cli}
	for _, c := range cases {
		ctx := context.Background()
		if c.trace != nil {
			ctx = tracing.ContextWithTraceInfo(ctx, c.trace)
		}

		req := &tikvrpc.Request{}
		expectedResp := &tikvrpc.Response{}
		verifySendRequest := func(args mock.Arguments) {
			inj := args.Get(2).(*tikvrpc.Request)
			if c.trace == nil {
				require.Nil(t, inj.Context.SourceStmt, c.name)
			} else {
				require.NotNil(t, inj.Context.SourceStmt, c.name)
				require.Equal(t, c.trace.ConnectionID, inj.Context.SourceStmt.ConnectionId, c.name)
				require.Equal(t, c.trace.SessionAlias, inj.Context.SourceStmt.SessionAlias, c.name)
			}
		}

		cli.On("SendRequest", ctx, "addr1", req, time.Second).Return(expectedResp, nil).Once().Run(verifySendRequest)
		resp, err := inject.SendRequest(ctx, "addr1", req, time.Second)
		cli.AssertExpectations(t)
		require.NoError(t, err)
		require.Same(t, expectedResp, resp)

		expectedErr := errors.New("mockErr")
		cli.On("SendRequest", ctx, "addr2", req, time.Minute).Return(nil, expectedErr).Once().Run(verifySendRequest)
		resp, err = inject.SendRequest(ctx, "addr2", req, time.Minute)
		require.Same(t, expectedErr, err)
		require.Nil(t, resp)
	}
}
