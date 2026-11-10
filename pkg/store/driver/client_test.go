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
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// failedStorePDServer completes discovery but rejects TSO requests so Open fails
// after creating its RPC client, before a KVStore can take ownership of it.
type failedStorePDServer struct {
	pdpb.UnimplementedPDServer
	endpoint string
	tsoCalls atomic.Int64
}

// GetMembers supplies a reachable leader for the real PD client's discovery.
func (s *failedStorePDServer) GetMembers(context.Context, *pdpb.GetMembersRequest) (*pdpb.GetMembersResponse, error) {
	member := &pdpb.Member{MemberId: 1, ClientUrls: []string{s.endpoint}}
	return &pdpb.GetMembersResponse{
		Header:  &pdpb.ResponseHeader{ClusterId: 1463},
		Members: []*pdpb.Member{member},
		Leader:  member,
	}, nil
}

// GetClusterInfo selects the PD TSO service for the real client.
func (s *failedStorePDServer) GetClusterInfo(context.Context, *pdpb.GetClusterInfoRequest) (*pdpb.GetClusterInfoResponse, error) {
	return &pdpb.GetClusterInfoResponse{
		Header:       &pdpb.ResponseHeader{ClusterId: 1463},
		ServiceModes: []pdpb.ServiceMode{pdpb.ServiceMode_PD_SVC_MODE},
	}, nil
}

// Tso returns the leader error observed during physical-replication startup.
func (s *failedStorePDServer) Tso(stream pdpb.PD_TsoServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	s.tsoCalls.Add(1)
	return status.Error(codes.Unknown,
		"[PD:tso:ErrGenerateTimestamp]generate timestamp failed, requested pd is not leader of cluster")
}

func TestOpenStoreClosesRPCClientOnPDLeaderChange(t *testing.T) {
	// Serve real PD discovery/TSO RPCs, fail Open repeatedly during oracle
	// initialization, then verify that failed attempts leave no goroutines.
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	defer server.Stop()
	pdServer := &failedStorePDServer{endpoint: "http://" + listener.Addr().String()}
	pdpb.RegisterPDServer(server, pdServer)
	go func() {
		_ = server.Serve(listener)
	}()

	for range 3 {
		store, err := (TiKVDriver{}).Open("tikv://" + listener.Addr().String())
		require.Nil(t, store)
		require.ErrorContains(t, err, "requested pd is not leader of cluster")
	}
	require.Positive(t, pdServer.tsoCalls.Load())
}
