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

// stubServer is a minimal ExternalWorkload server used to validate the client wire
// surface. It records the last request header so tests can confirm the
// client populates keyspace identity on every call, and surfaces controlled
// responses (success, paused, generic error) without exercising the full
// service.
type stubServer struct {
	pb.UnimplementedExternalWorkloadControllerServer
	lastHeader  *pb.RequestHeader
	pingErr     *pb.Error
	gcRecycleTs uint64
	bgConfig    pb.GetBgTaskConfigResponse
}

func (s *stubServer) Ping(_ context.Context, _ *pb.PingRequest) (*pb.Response, error) {
	if s.pingErr != nil {
		return &pb.Response{Error: s.pingErr}, nil
	}
	return &pb.Response{}, nil
}

func (s *stubServer) RecycleGC(_ context.Context, req *pb.RecycleGCRequest) (*pb.Response, error) {
	s.lastHeader = req.GetHeader()
	s.gcRecycleTs = req.GetSafePoint()
	return &pb.Response{}, nil
}

func (s *stubServer) GetBgTaskConfig(_ context.Context, req *pb.GetBgTaskConfigRequest) (*pb.GetBgTaskConfigResponse, error) {
	s.lastHeader = req.GetHeader()
	return &s.bgConfig, nil
}

// startStubServer starts a stub ExternalWorkload on a bufconn-like real loopback
// listener and returns a ready Client plus a cleanup function.
func startStubServer(t *testing.T, stub *stubServer) (Client, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	pb.RegisterExternalWorkloadControllerServer(srv, stub)
	go func() { _ = srv.Serve(ln) }()

	dialCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := New(dialCtx, &Option{
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

func TestClientRoundTrip(t *testing.T) {
	stub := &stubServer{
		bgConfig: pb.GetBgTaskConfigResponse{WorkerCount: 3, AutoScaleEnabled: true},
	}
	cli, cleanup := startStubServer(t, stub)
	defer cleanup()

	ctx := context.Background()

	// Ping returns nil on Response.error == nil.
	require.NoError(t, cli.Ping(ctx))

	// Headers and payload propagate verbatim.
	require.NoError(t, cli.RecycleGC(ctx, 1234))
	require.Equal(t, uint64(1234), stub.gcRecycleTs)
	require.Equal(t, uint32(42), stub.lastHeader.GetKeyspaceId())
	require.Equal(t, "starter-ks", stub.lastHeader.GetKeyspaceName())
	require.Equal(t, "starter-pool", stub.lastHeader.GetTidbPool())

	// GetBgTaskConfig returns parsed scalar fields.
	count, autoScale, err := cli.GetBgTaskConfig(ctx, "ddl")
	require.NoError(t, err)
	require.Equal(t, 3, count)
	require.True(t, autoScale)
}

func TestClientErrorMapping(t *testing.T) {
	stub := &stubServer{pingErr: &pb.Error{Type: pb.ErrorType_PAUSED, Message: "paused"}}
	cli, cleanup := startStubServer(t, stub)
	defer cleanup()

	require.ErrorIs(t, cli.Ping(context.Background()), ErrControllerPaused)

	stub.pingErr = &pb.Error{Type: pb.ErrorType_UNKNOWN, Message: "boom"}
	err := cli.Ping(context.Background())
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrControllerPaused)
	require.Contains(t, err.Error(), "boom")
}

func TestNewClientValidation(t *testing.T) {
	_, err := New(context.Background(), nil)
	require.Error(t, err)

	_, err = New(context.Background(), &Option{ControllerAddr: ""})
	require.Error(t, err)

	_, err = New(context.Background(), &Option{ControllerAddr: "://bad"})
	require.Error(t, err)
}
