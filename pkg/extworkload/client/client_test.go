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
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/pingcap/kvproto/pkg/externalworkloadpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type stubServer struct {
	pb.UnimplementedExternalWorkloadControllerServer
	pingErr *pb.Error

	registerGCV2Req        *pb.RegisterGCV2Request
	recycleGCV2Req         *pb.RecycleGCV2Request
	updateGCLifeTimeReq    *pb.UpdateGCLifeTimeRequest
	registerTTLTaskReq     *pb.RegisterTTLTaskRequest
	deleteTTLTableInfoReq  *pb.DeleteTTLTableInfoRequest
	recycleTTLTaskReq      *pb.RecycleTTLTaskRequest
	updateTTLJobEnableReq  *pb.UpdateTTLJobEnableRequest
	registerAutoAnalyzeReq *pb.RegisterAutoAnalyzeRequest
	recycleAutoAnalyzeReq  *pb.RecycleAutoAnalyzeRequest
}

func (s *stubServer) Ping(_ context.Context, _ *pb.PingRequest) (*pb.Response, error) {
	if s.pingErr != nil {
		return &pb.Response{Error: s.pingErr}, nil
	}
	return &pb.Response{}, nil
}

func (s *stubServer) RegisterGCV2(_ context.Context, req *pb.RegisterGCV2Request) (*pb.Response, error) {
	s.registerGCV2Req = req
	return &pb.Response{}, nil
}

func (s *stubServer) RecycleGCV2(_ context.Context, req *pb.RecycleGCV2Request) (*pb.Response, error) {
	s.recycleGCV2Req = req
	return &pb.Response{}, nil
}

func (s *stubServer) UpdateGCLifeTime(_ context.Context, req *pb.UpdateGCLifeTimeRequest) (*pb.Response, error) {
	s.updateGCLifeTimeReq = req
	return &pb.Response{}, nil
}

func (s *stubServer) RegisterTTLTask(_ context.Context, req *pb.RegisterTTLTaskRequest) (*pb.Response, error) {
	s.registerTTLTaskReq = req
	return &pb.Response{}, nil
}

func (s *stubServer) DeleteTTLTableInfo(_ context.Context, req *pb.DeleteTTLTableInfoRequest) (*pb.Response, error) {
	s.deleteTTLTableInfoReq = req
	return &pb.Response{}, nil
}

func (s *stubServer) RecycleTTLTask(_ context.Context, req *pb.RecycleTTLTaskRequest) (*pb.Response, error) {
	s.recycleTTLTaskReq = req
	return &pb.Response{}, nil
}

func (s *stubServer) UpdateTTLJobEnable(_ context.Context, req *pb.UpdateTTLJobEnableRequest) (*pb.Response, error) {
	s.updateTTLJobEnableReq = req
	return &pb.Response{}, nil
}

func (s *stubServer) RegisterAutoAnalyze(_ context.Context, req *pb.RegisterAutoAnalyzeRequest) (*pb.Response, error) {
	s.registerAutoAnalyzeReq = req
	return &pb.Response{}, nil
}

func (s *stubServer) RecycleAutoAnalyze(_ context.Context, req *pb.RecycleAutoAnalyzeRequest) (*pb.Response, error) {
	s.recycleAutoAnalyzeReq = req
	return &pb.Response{}, nil
}

func startStubServer(t *testing.T, stub *stubServer) (Client, func()) {
	return startStubServerWithOption(t, stub, nil)
}

func startStubServerWithOption(t *testing.T, stub *stubServer, configure func(*Option)) (Client, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	pb.RegisterExternalWorkloadControllerServer(srv, stub)
	go func() { _ = srv.Serve(ln) }()

	opt := &Option{
		KeyspaceID:     42,
		KeyspaceName:   "starter-ks",
		TiDBPool:       "starter-pool",
		ControllerAddr: "http://" + ln.Addr().String(),
	}
	if configure != nil {
		configure(opt)
	}
	cli, err := New(opt)
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

	cases := []struct {
		name  string
		call  func() error
		check func()
	}{
		{
			name: "Ping",
			call: func() error { return cli.Ping(ctx) },
		},
		{
			name: "RegisterGCV2",
			call: func() error { return cli.RegisterGCV2(ctx, 12, 600) },
			check: func() {
				requireHeader(t, stub.registerGCV2Req.GetHeader())
				require.Equal(t, uint64(12), stub.registerGCV2Req.GetSafePoint())
				require.Equal(t, int64(600), stub.registerGCV2Req.GetGcLifeTime())
			},
		},
		{
			name: "RecycleGCV2",
			call: func() error { return cli.RecycleGCV2(ctx, 1234) },
			check: func() {
				requireHeader(t, stub.recycleGCV2Req.GetHeader())
				require.Equal(t, uint64(1234), stub.recycleGCV2Req.GetSafePoint())
			},
		},
		{
			name: "UpdateGCLifeTime",
			call: func() error { return cli.UpdateGCLifeTime(ctx, 3600) },
			check: func() {
				requireHeader(t, stub.updateGCLifeTimeReq.GetHeader())
				require.Equal(t, int64(3600), stub.updateGCLifeTimeReq.GetGcLifeTime())
			},
		},
		{
			name: "RegisterTTLTask",
			call: func() error { return cli.RegisterTTLTask(ctx, 11, true) },
			check: func() {
				requireHeader(t, stub.registerTTLTaskReq.GetHeader())
				require.Equal(t, int64(11), stub.registerTTLTaskReq.GetTableId())
				require.True(t, stub.registerTTLTaskReq.GetTtlJobEnable())
			},
		},
		{
			name: "DeleteTTLTableInfo",
			call: func() error { return cli.DeleteTTLTableInfo(ctx, 12) },
			check: func() {
				requireHeader(t, stub.deleteTTLTableInfoReq.GetHeader())
				require.Equal(t, int64(12), stub.deleteTTLTableInfoReq.GetTableId())
			},
		},
		{
			name: "RecycleTTLTask",
			call: func() error { return cli.RecycleTTLTask(ctx, 99) },
			check: func() {
				requireHeader(t, stub.recycleTTLTaskReq.GetHeader())
				require.Equal(t, uint64(99), stub.recycleTTLTaskReq.GetCompletedJobCreateTime())
			},
		},
		{
			name: "UpdateTTLJobEnable",
			call: func() error { return cli.UpdateTTLJobEnable(ctx, false) },
			check: func() {
				requireHeader(t, stub.updateTTLJobEnableReq.GetHeader())
				require.False(t, stub.updateTTLJobEnableReq.GetTtlJobEnable())
			},
		},
		{
			name: "RegisterAutoAnalyze",
			call: func() error { return cli.RegisterAutoAnalyze(ctx, 7) },
			check: func() {
				requireHeader(t, stub.registerAutoAnalyzeReq.GetHeader())
				require.Equal(t, uint64(7), stub.registerAutoAnalyzeReq.GetTaskId())
			},
		},
		{
			name: "RecycleAutoAnalyze",
			call: func() error { return cli.RecycleAutoAnalyze(ctx, 8) },
			check: func() {
				requireHeader(t, stub.recycleAutoAnalyzeReq.GetHeader())
				require.Equal(t, uint64(8), stub.recycleAutoAnalyzeReq.GetTaskId())
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.NoError(t, c.call())
			if c.check != nil {
				c.check()
			}
		})
	}
}

func requireHeader(t *testing.T, header *pb.RequestHeader) {
	t.Helper()
	require.Equal(t, uint32(42), header.GetKeyspaceId())
	require.Equal(t, "starter-ks", header.GetKeyspaceName())
	require.Equal(t, "starter-pool", header.GetTidbPool())
}

func TestClientInterceptor(t *testing.T) {
	stub := &stubServer{}
	var called atomic.Int32
	cli, cleanup := startStubServerWithOption(t, stub, func(opt *Option) {
		opt.Interceptors = []grpc.UnaryClientInterceptor{
			func(
				ctx context.Context,
				method string,
				req any,
				reply any,
				cc *grpc.ClientConn,
				invoker grpc.UnaryInvoker,
				opts ...grpc.CallOption,
			) error {
				called.Add(1)
				return invoker(ctx, method, req, reply, cc, opts...)
			},
		}
	})
	defer cleanup()

	require.NoError(t, cli.Ping(newTestContext(t)))
	require.Equal(t, int32(1), called.Load())
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
	err := mapResponse(nil, nil)
	require.ErrorContains(t, err, "empty response")
}

func TestNewClientValidation(t *testing.T) {
	_, err := New(nil)
	require.Error(t, err)

	_, err = New(&Option{ControllerAddr: ""})
	require.Error(t, err)

	_, err = New(&Option{ControllerAddr: "://bad"})
	require.Error(t, err)
}

func TestNormalizeAddr(t *testing.T) {
	addr, err := normalizeAddr("http://127.0.0.1:1234")
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:1234", addr)

	addr, err = normalizeAddr("127.0.0.1:1234")
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:1234", addr)

	_, err = normalizeAddr("http://")
	require.Error(t, err)
}
