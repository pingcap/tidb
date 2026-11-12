// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestRPCServerUnsupportedMethods(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	s := NewRPCServer(config.NewConfig(), nil, nil)
	require.NotNil(t, s)
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- s.Serve(listener)
	}()
	t.Cleanup(func() {
		s.Stop()
		require.NoError(t, listener.Close())
		require.NoError(t, <-serveErr)
	})
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := tikvpb.NewTikvClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.KvGet(ctx, &kvrpcpb.GetRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	_, err = client.RawGet(ctx, &kvrpcpb.RawGetRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	stream, err := client.BatchCoprocessor(ctx, &coprocessor.BatchRequest{})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Equal(t, codes.Unimplemented, status.Code(err))

	// An implemented RPC still works after unsupported requests.
	batch, err := client.BatchCommands(ctx)
	require.NoError(t, err)
	require.NoError(t, batch.Send(&tikvpb.BatchCommandsRequest{
		RequestIds: []uint64{1},
		Requests: []*tikvpb.BatchCommandsRequest_Request{{
			Cmd: &tikvpb.BatchCommandsRequest_Request_Empty{
				Empty: &tikvpb.BatchCommandsEmptyRequest{TestId: 42},
			},
		}},
	}))
	resp, err := batch.Recv()
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, resp.RequestIds)
	require.Len(t, resp.Responses, 1)
	require.EqualValues(t, 42, resp.Responses[0].GetEmpty().TestId)
	require.NoError(t, batch.CloseSend())
}
