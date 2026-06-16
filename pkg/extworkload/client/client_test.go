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

package client

import (
	"context"
	"net"
	"testing"
	"time"

	pb "github.com/pingcap/kvproto/pkg/externalworkloadpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type stubServer struct {
	pb.UnimplementedExternalWorkloadControllerServer
	lastHeader    *pb.RequestHeader
	pingErr       *pb.Error
	gcv2RecycleTs uint64
}

func (s *stubServer) Ping(_ context.Context, _ *pb.PingRequest) (*pb.Response, error) {
	if s.pingErr != nil {
		return &pb.Response{Error: s.pingErr}, nil
	}
	return &pb.Response{}, nil
}

func (s *stubServer) RecycleGCV2(_ context.Context, req *pb.RecycleGCV2Request) (*pb.Response, error) {
	s.lastHeader = req.GetHeader()
	s.gcv2RecycleTs = req.GetSafePoint()
	return &pb.Response{}, nil
}

func startStubServer(t *testing.T, stub *stubServer) (Client, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	pb.RegisterExternalWorkloadControllerServer(srv, stub)
	go func() { _ = srv.Serve(ln) }()

	cli, err := New(&Option{
		KeyspaceID:     42,
		KeyspaceName:   "starter-ks",
		TiDBPool:       "starter-pool",
		ControllerAddr: "http://" + ln.Addr().String(),
	})
	require.NoError(t, err)

	cleanup := func() {
		_ = cli.Close()
		srv.GracefulStop()
		_ = ln.Close()
	}
	return cli, cleanup
}

func newTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestClientRoundTrip(t *testing.T) {
	stub := &stubServer{}
	cli, cleanup := startStubServer(t, stub)
	defer cleanup()

	ctx := newTestContext(t)

	require.NoError(t, cli.Ping(ctx))

	require.NoError(t, cli.RecycleGCV2(ctx, 1234))
	require.Equal(t, uint64(1234), stub.gcv2RecycleTs)
	require.Equal(t, uint32(42), stub.lastHeader.GetKeyspaceId())
	require.Equal(t, "starter-ks", stub.lastHeader.GetKeyspaceName())
	require.Equal(t, "starter-pool", stub.lastHeader.GetTidbPool())
}

func TestClientErrorMapping(t *testing.T) {
	stub := &stubServer{pingErr: &pb.Error{Type: pb.ErrorType_PAUSED, Message: "paused"}}
	cli, cleanup := startStubServer(t, stub)
	defer cleanup()

	require.ErrorIs(t, cli.Ping(newTestContext(t)), ErrControllerPaused)

	stub.pingErr = &pb.Error{Type: pb.ErrorType_UNKNOWN, Message: "boom"}
	err := cli.Ping(newTestContext(t))
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrControllerPaused)
	require.Contains(t, err.Error(), "boom")
}

func TestMapResponseNilResponse(t *testing.T) {
	err := mapResponse("NilResponse", nil, nil)
	require.ErrorContains(t, err, "external workload rpc NilResponse: empty response")
}

func TestNewClientValidation(t *testing.T) {
	_, err := New(nil)
	require.Error(t, err)

	_, err = New(&Option{ControllerAddr: ""})
	require.Error(t, err)

	_, err = New(&Option{ControllerAddr: "://bad"})
	require.Error(t, err)
}
