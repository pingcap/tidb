// Copyright 2020 PingCAP, Inc.
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

package unistore

import (
	"io"
	"math"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	us "github.com/ngaut/unistore/tikv"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/codec"
	"golang.org/x/net/context"
	"google.golang.org/grpc/metadata"
)

// For gofail injection.
var undeterminedErr = terror.ErrResultUndetermined

// RPCClient sends kv RPC calls to mock cluster. RPCClient mocks the behavior of
// a rpc client at tikv's side.
type RPCClient struct {
	usSvr      *us.Server
	cluster    *Cluster
	path       string
	rawHandler *rawHandler
	persistent bool
	closed     int32

	// rpcCli uses to redirects RPC request to TiDB rpc server, It is only use for test.
	// Mock TiDB rpc service will have circle import problem, so just use a real RPC client to send this RPC  server.
	// sync.Once uses to avoid concurrency initialize rpcCli.
	sync.Once
	rpcCli Client
}

// SendRequest sends a request to mock cluster.
func (c *RPCClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	failpoint.Inject("rpcServerBusy", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(tikvrpc.GenRegionErrorResp(req, &errorpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{}}))
		}
	})

	if req.StoreTp == kv.TiDB {
		return c.redirectRequestToRPCServer(ctx, addr, req, timeout)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if atomic.LoadInt32(&c.closed) != 0 {
		// Return `context.Canceled` can break Backoff.
		return nil, context.Canceled
	}

	resp := &tikvrpc.Response{}
	var err error
	switch req.Type {
	case tikvrpc.CmdGet:
		resp.Resp, err = c.usSvr.KvGet(ctx, req.Get())
	case tikvrpc.CmdScan:
		resp.Resp, err = c.usSvr.KvScan(ctx, req.Scan())
	case tikvrpc.CmdPrewrite:
		failpoint.Inject("rpcPrewriteResult", func(val failpoint.Value) {
			switch val.(string) {
			case "notLeader":
				failpoint.Return(&tikvrpc.Response{
					Resp: &kvrpcpb.PrewriteResponse{RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{}}},
				}, nil)
			}
		})

		r := req.Prewrite()
		c.cluster.handleDelay(r.StartVersion, r.Context.RegionId)
		resp.Resp, err = c.usSvr.KvPrewrite(ctx, r)
	case tikvrpc.CmdPessimisticLock:
		r := req.PessimisticLock()
		c.cluster.handleDelay(r.StartVersion, r.Context.RegionId)
		resp.Resp, err = c.usSvr.KvPessimisticLock(ctx, r)
	case tikvrpc.CmdPessimisticRollback:
		resp.Resp, err = c.usSvr.KVPessimisticRollback(ctx, req.PessimisticRollback())
	case tikvrpc.CmdCommit:
		failpoint.Inject("rpcCommitResult", func(val failpoint.Value) {
			switch val.(string) {
			case "timeout":
				failpoint.Return(nil, errors.New("timeout"))
			case "notLeader":
				failpoint.Return(&tikvrpc.Response{
					Resp: &kvrpcpb.CommitResponse{RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{}}},
				}, nil)
			case "keyError":
				failpoint.Return(&tikvrpc.Response{
					Resp: &kvrpcpb.CommitResponse{Error: &kvrpcpb.KeyError{}},
				}, nil)
			}
		})

		resp.Resp, err = c.usSvr.KvCommit(ctx, req.Commit())

		failpoint.Inject("rpcCommitTimeout", func(val failpoint.Value) {
			if val.(bool) {
				failpoint.Return(nil, undeterminedErr)
			}
		})
	case tikvrpc.CmdCleanup:
		resp.Resp, err = c.usSvr.KvCleanup(ctx, req.Cleanup())
	case tikvrpc.CmdCheckTxnStatus:
		resp.Resp, err = c.usSvr.KvCheckTxnStatus(ctx, req.CheckTxnStatus())
	case tikvrpc.CmdCheckSecondaryLocks:
		resp.Resp, err = c.usSvr.KvCheckSecondaryLocks(ctx, req.CheckSecondaryLocks())
	case tikvrpc.CmdTxnHeartBeat:
		resp.Resp, err = c.usSvr.KvTxnHeartBeat(ctx, req.TxnHeartBeat())
	case tikvrpc.CmdBatchGet:
		resp.Resp, err = c.usSvr.KvBatchGet(ctx, req.BatchGet())
	case tikvrpc.CmdBatchRollback:
		resp.Resp, err = c.usSvr.KvBatchRollback(ctx, req.BatchRollback())
	case tikvrpc.CmdScanLock:
		resp.Resp, err = c.usSvr.KvScanLock(ctx, req.ScanLock())
	case tikvrpc.CmdResolveLock:
		resp.Resp, err = c.usSvr.KvResolveLock(ctx, req.ResolveLock())
	case tikvrpc.CmdGC:
		resp.Resp, err = c.usSvr.KvGC(ctx, req.GC())
	case tikvrpc.CmdDeleteRange:
		resp.Resp, err = c.usSvr.KvDeleteRange(ctx, req.DeleteRange())
	case tikvrpc.CmdRawGet:
		resp.Resp, err = c.rawHandler.RawGet(ctx, req.RawGet())
	case tikvrpc.CmdRawBatchGet:
		resp.Resp, err = c.rawHandler.RawBatchGet(ctx, req.RawBatchGet())
	case tikvrpc.CmdRawPut:
		resp.Resp, err = c.rawHandler.RawPut(ctx, req.RawPut())
	case tikvrpc.CmdRawBatchPut:
		resp.Resp, err = c.rawHandler.RawBatchPut(ctx, req.RawBatchPut())
	case tikvrpc.CmdRawDelete:
		resp.Resp, err = c.rawHandler.RawDelete(ctx, req.RawDelete())
	case tikvrpc.CmdRawBatchDelete:
		resp.Resp, err = c.rawHandler.RawBatchDelete(ctx, req.RawBatchDelete())
	case tikvrpc.CmdRawDeleteRange:
		resp.Resp, err = c.rawHandler.RawDeleteRange(ctx, req.RawDeleteRange())
	case tikvrpc.CmdRawScan:
		resp.Resp, err = c.rawHandler.RawScan(ctx, req.RawScan())
	case tikvrpc.CmdCop:
		resp.Resp, err = c.usSvr.Coprocessor(ctx, req.Cop())
	case tikvrpc.CmdCopStream:
		resp.Resp, err = c.handleCopStream(ctx, req.Cop())
	case tikvrpc.CmdMvccGetByKey:
		resp.Resp, err = c.usSvr.MvccGetByKey(ctx, req.MvccGetByKey())
	case tikvrpc.CmdMvccGetByStartTs:
		resp.Resp, err = c.usSvr.MvccGetByStartTs(ctx, req.MvccGetByStartTs())
	case tikvrpc.CmdSplitRegion:
		resp.Resp, err = c.usSvr.SplitRegion(ctx, req.SplitRegion())
	case tikvrpc.CmdDebugGetRegionProperties:
		resp.Resp, err = c.handleDebugGetRegionProperties(ctx, req.DebugGetRegionProperties())
		return resp, err
	default:
		err = errors.Errorf("unsupport this request type %v", req.Type)
	}
	if err != nil {
		return nil, err
	}
	regErr, err := resp.GetRegionError()
	if err != nil {
		return nil, err
	}
	if regErr != nil {
		if regErr.EpochNotMatch != nil {
			for i, newReg := range regErr.EpochNotMatch.CurrentRegions {
				regErr.EpochNotMatch.CurrentRegions[i] = proto.Clone(newReg).(*metapb.Region)
			}
		}
	}
	return resp, nil
}

func (c *RPCClient) handleCopStream(ctx context.Context, req *coprocessor.Request) (*tikvrpc.CopStreamResponse, error) {
	copResp, err := c.usSvr.Coprocessor(ctx, req)
	if err != nil {
		return nil, err
	}
	return &tikvrpc.CopStreamResponse{
		Tikv_CoprocessorStreamClient: new(mockCopStreamClient),
		Response:                     copResp,
	}, nil
}

func (c *RPCClient) handleDebugGetRegionProperties(ctx context.Context, req *debugpb.GetRegionPropertiesRequest) (*debugpb.GetRegionPropertiesResponse, error) {
	region := c.cluster.GetRegion(req.RegionId)
	_, start, err := codec.DecodeBytes(region.StartKey, nil)
	if err != nil {
		return nil, err
	}
	_, end, err := codec.DecodeBytes(region.EndKey, nil)
	if err != nil {
		return nil, err
	}
	scanResp, err := c.usSvr.KvScan(ctx, &kvrpcpb.ScanRequest{
		Context: &kvrpcpb.Context{
			RegionId:    region.Id,
			RegionEpoch: region.RegionEpoch,
		},
		StartKey: start,
		EndKey:   end,
		Version:  math.MaxUint64,
		Limit:    math.MaxUint32,
	})
	if err != nil {
		return nil, err
	}
	if err := scanResp.GetRegionError(); err != nil {
		panic(err)
	}
	return &debugpb.GetRegionPropertiesResponse{
		Props: []*debugpb.Property{{
			Name:  "mvcc.num_rows",
			Value: strconv.Itoa(len(scanResp.Pairs)),
		}}}, nil
}

// Client is a client that sends RPC.
// This is same with tikv.Client, define again for avoid circle import.
type Client interface {
	// Close should release all data.
	Close() error
	// SendRequest sends Request.
	SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error)
}

// GRPCClientFactory is the GRPC client factory.
// Use global variable to avoid circle import.
// TODO: remove this global variable.
var GRPCClientFactory func() Client

// redirectRequestToRPCServer redirects RPC request to TiDB rpc server, It is only use for test.
// Mock TiDB rpc service will have circle import problem, so just use a real RPC client to send this RPC  server.
func (c *RPCClient) redirectRequestToRPCServer(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	c.Once.Do(func() {
		if GRPCClientFactory != nil {
			c.rpcCli = GRPCClientFactory()
		}
	})
	if c.rpcCli == nil {
		return nil, errors.Errorf("GRPCClientFactory is nil")
	}
	return c.rpcCli.SendRequest(ctx, addr, req, timeout)
}

// Close closes RPCClient and cleanup temporal resources.
func (c *RPCClient) Close() error {
	atomic.StoreInt32(&c.closed, 1)
	if c.usSvr != nil {
		c.usSvr.Stop()
	}
	if c.rpcCli != nil {
		err := c.rpcCli.Close()
		if err != nil {
			return err
		}
	}
	if !c.persistent && c.path != "" {
		err := os.RemoveAll(c.path)
		_ = err
	}
	return nil
}

type mockClientStream struct{}

// Header implements grpc.ClientStream interface
func (mockClientStream) Header() (metadata.MD, error) { return nil, nil }

// Trailer implements grpc.ClientStream interface
func (mockClientStream) Trailer() metadata.MD { return nil }

// CloseSend implements grpc.ClientStream interface
func (mockClientStream) CloseSend() error { return nil }

// Context implements grpc.ClientStream interface
func (mockClientStream) Context() context.Context { return nil }

// SendMsg implements grpc.ClientStream interface
func (mockClientStream) SendMsg(m interface{}) error { return nil }

// RecvMsg implements grpc.ClientStream interface
func (mockClientStream) RecvMsg(m interface{}) error { return nil }

type mockCopStreamClient struct {
	mockClientStream
}

func (mock *mockCopStreamClient) Recv() (*coprocessor.Response, error) {
	return nil, io.EOF
}
