// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"net"
	"testing"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	goctx "golang.org/x/net/context"
	"google.golang.org/grpc"
)

type testTikvServer struct {
}

func (s *testTikvServer) KvGet(ctx goctx.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	return &kvrpcpb.GetResponse{}, nil
}

func (s *testTikvServer) KvScan(ctx goctx.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	return &kvrpcpb.ScanResponse{}, nil
}

func (s *testTikvServer) KvPrewrite(ctx goctx.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	return &kvrpcpb.PrewriteResponse{}, nil
}

func (s *testTikvServer) KvCommit(ctx goctx.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	return &kvrpcpb.CommitResponse{}, nil
}

func (s *testTikvServer) KvCleanup(ctx goctx.Context, req *kvrpcpb.CleanupRequest) (*kvrpcpb.CleanupResponse, error) {
	return &kvrpcpb.CleanupResponse{}, nil
}

func (s *testTikvServer) KvBatchGet(ctx goctx.Context, req *kvrpcpb.BatchGetRequest) (*kvrpcpb.BatchGetResponse, error) {
	return &kvrpcpb.BatchGetResponse{}, nil
}

func (s *testTikvServer) KvBatchRollback(ctx goctx.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	return &kvrpcpb.BatchRollbackResponse{}, nil
}

func (s *testTikvServer) KvScanLock(ctx goctx.Context, req *kvrpcpb.ScanLockRequest) (*kvrpcpb.ScanLockResponse, error) {
	return &kvrpcpb.ScanLockResponse{}, nil
}

func (s *testTikvServer) KvResolveLock(ctx goctx.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	return &kvrpcpb.ResolveLockResponse{}, nil
}

func (s *testTikvServer) KvGC(ctx goctx.Context, req *kvrpcpb.GCRequest) (*kvrpcpb.GCResponse, error) {
	return &kvrpcpb.GCResponse{}, nil
}

func (s *testTikvServer) RawGet(ctx goctx.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	return &kvrpcpb.RawGetResponse{}, nil
}

func (s *testTikvServer) RawPut(ctx goctx.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	return &kvrpcpb.RawPutResponse{}, nil
}

func (s *testTikvServer) RawDelete(ctx goctx.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	return &kvrpcpb.RawDeleteResponse{}, nil
}

func (s *testTikvServer) Coprocessor(ctx goctx.Context, req *coprocessor.Request) (*coprocessor.Response, error) {
	return &coprocessor.Response{}, nil
}

func startServer(addr string, c *C, stopCh chan int) {
	listener, err := net.Listen("tcp", addr)
	c.Assert(err, IsNil)
	log.Debug("Start listenning on", addr)
	grpcServer := grpc.NewServer()
	tikvpb.RegisterTiKVServer(grpcServer, &testTikvServer{})
	go func() {
		grpcServer.Serve(listener)
		listener.Close()
	}()
	go func() {
		select {
		case <-stopCh:
			grpcServer.Stop()
		}
	}()
}

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testClientSuite{})

type testClientSuite struct {
}

// One normally `Send`.
func (s *testClientSuite) TestSimpleRPC(c *C) {
	addr := ":61234"
	stopCh := make(chan int, 1)
	startServer(addr, c, stopCh)
	defer func() {
		stopCh <- 1
	}()
	client := newRPCClient()
	ctx := goctx.Background()
	req := &kvrpcpb.GetRequest{}
	resp, err := client.KvGet(ctx, addr, req)
	c.Assert(err, IsNil)
	c.Assert(resp, NotNil)
}
