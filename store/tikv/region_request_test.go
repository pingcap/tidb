// Copyright 2017 PingCAP, Inc.
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
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/storeutil"
	"google.golang.org/grpc"
)

type testRegionRequestSuite struct {
	cluster             *mocktikv.Cluster
	store               uint64
	peer                uint64
	region              uint64
	cache               *RegionCache
	bo                  *Backoffer
	regionRequestSender *RegionRequestSender
	mvccStore           mocktikv.MVCCStore
}

type testStoreLimitSuite struct {
	cluster             *mocktikv.Cluster
	storeIDs            []uint64
	peerIDs             []uint64
	regionID            uint64
	leaderPeer          uint64
	cache               *RegionCache
	bo                  *Backoffer
	regionRequestSender *RegionRequestSender
	mvccStore           mocktikv.MVCCStore
}

var _ = Suite(&testRegionRequestSuite{})
var _ = Suite(&testStoreLimitSuite{})

func (s *testRegionRequestSuite) SetUpTest(c *C) {
	s.cluster = mocktikv.NewCluster()
	s.store, s.peer, s.region = mocktikv.BootstrapWithSingleStore(s.cluster)
	pdCli := &codecPDClient{mocktikv.NewPDClient(s.cluster)}
	s.cache = NewRegionCache(pdCli)
	s.bo = NewNoopBackoff(context.Background())
	s.mvccStore = mocktikv.MustNewMVCCStore()
	client := mocktikv.NewRPCClient(s.cluster, s.mvccStore)
	s.regionRequestSender = NewRegionRequestSender(s.cache, client)
}

func (s *testStoreLimitSuite) SetUpTest(c *C) {
	s.cluster = mocktikv.NewCluster()
	s.storeIDs, s.peerIDs, s.regionID, s.leaderPeer = mocktikv.BootstrapWithMultiStores(s.cluster, 3)
	pdCli := &codecPDClient{mocktikv.NewPDClient(s.cluster)}
	s.cache = NewRegionCache(pdCli)
	s.bo = NewNoopBackoff(context.Background())
	s.mvccStore = mocktikv.MustNewMVCCStore()
	client := mocktikv.NewRPCClient(s.cluster, s.mvccStore)
	s.regionRequestSender = NewRegionRequestSender(s.cache, client)
}

func (s *testRegionRequestSuite) TearDownTest(c *C) {
	s.cache.Close()
}

func (s *testStoreLimitSuite) TearDownTest(c *C) {
	s.cache.Close()
}

type fnClient struct {
	fn func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error)
}

func (f *fnClient) Close() error {
	return nil
}

func (f *fnClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	return f.fn(ctx, addr, req, timeout)
}

func (s *testRegionRequestSuite) TestOnRegionError(c *C) {
	req := &tikvrpc.Request{Type: tikvrpc.CmdRawPut, RawPut: &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	}}
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)

	// test stale command retry.
	func() {
		oc := s.regionRequestSender.client
		defer func() {
			s.regionRequestSender.client = oc
		}()
		s.regionRequestSender.client = &fnClient{func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
			staleResp := &tikvrpc.Response{Get: &kvrpcpb.GetResponse{
				RegionError: &errorpb.Error{StaleCommand: &errorpb.StaleCommand{}},
			}}
			return staleResp, nil
		}}
		bo := NewBackoffer(context.Background(), 5)
		resp, err := s.regionRequestSender.SendReq(bo, req, region.Region, time.Second)
		c.Assert(err, NotNil)
		c.Assert(resp, IsNil)
	}()

}

func (s *testStoreLimitSuite) TestStoreTokenLimit(c *C) {
	req := &tikvrpc.Request{Type: tikvrpc.CmdPrewrite, Prewrite: &kvrpcpb.PrewriteRequest{}, Context: kvrpcpb.Context{}}
	region, err := s.cache.LocateRegionByID(s.bo, s.regionID)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	oldStoreLimit := storeutil.StoreLimit.Load()
	storeutil.StoreLimit.Store(500)
	s.cache.getStoreByStoreID(s.storeIDs[0]).tokenCount.Store(500)
	// cause there is only one region in this cluster, regionID maps this leader.
	resp, err := s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, NotNil)
	c.Assert(resp, IsNil)
	c.Assert(err.Error(), Equals, "[tikv:9008]Store token is up to the limit, store id = 1")
	storeutil.StoreLimit.Store(oldStoreLimit)
}

func (s *testRegionRequestSuite) TestOnSendFailedWithStoreRestart(c *C) {
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdRawPut,
		RawPut: &kvrpcpb.RawPutRequest{
			Key:   []byte("key"),
			Value: []byte("value"),
		},
	}
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	resp, err := s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp.RawPut, NotNil)

	// stop store.
	s.cluster.StopStore(s.store)
	_, err = s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, NotNil)

	// start store.
	s.cluster.StartStore(s.store)

	// locate region again is needed
	// since last request on the region failed and region's info had been cleared.
	region, err = s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	resp, err = s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp.RawPut, NotNil)
}

func (s *testRegionRequestSuite) TestOnSendFailedWithCloseKnownStoreThenUseNewOne(c *C) {
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdRawPut,
		RawPut: &kvrpcpb.RawPutRequest{
			Key:   []byte("key"),
			Value: []byte("value"),
		},
	}

	// add new store2 and make store2 as leader.
	store2 := s.cluster.AllocID()
	peer2 := s.cluster.AllocID()
	s.cluster.AddStore(store2, fmt.Sprintf("store%d", store2))
	s.cluster.AddPeer(s.region, store2, peer2)
	s.cluster.ChangeLeader(s.region, peer2)

	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	resp, err := s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp.RawPut, NotNil)

	// stop store2 and make store1 as new leader.
	s.cluster.StopStore(store2)
	s.cluster.ChangeLeader(s.region, s.peer)

	// send to store2 fail and send to new leader store1.
	bo2 := NewBackoffer(context.Background(), 100)
	resp, err = s.regionRequestSender.SendReq(bo2, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	regionErr, err := resp.GetRegionError()
	c.Assert(err, IsNil)
	c.Assert(regionErr, IsNil)
	c.Assert(resp.RawPut, NotNil)
}

func (s *testRegionRequestSuite) TestSendReqCtx(c *C) {
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdRawPut,
		RawPut: &kvrpcpb.RawPutRequest{
			Key:   []byte("key"),
			Value: []byte("value"),
		},
	}
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	resp, ctx, err := s.regionRequestSender.SendReqCtx(s.bo, req, region.Region, time.Second, kv.TiKV)
	c.Assert(err, IsNil)
	c.Assert(resp.RawPut, NotNil)
	c.Assert(ctx, NotNil)
	req.ReplicaRead = true
	resp, ctx, err = s.regionRequestSender.SendReqCtx(s.bo, req, region.Region, time.Second, kv.TiKV)
	c.Assert(err, IsNil)
	c.Assert(resp.RawPut, NotNil)
	c.Assert(ctx, NotNil)
}

func (s *testRegionRequestSuite) TestOnSendFailedWithCancelled(c *C) {
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdRawPut,
		RawPut: &kvrpcpb.RawPutRequest{
			Key:   []byte("key"),
			Value: []byte("value"),
		},
	}
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	resp, err := s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp.RawPut, NotNil)

	// set store to cancel state.
	s.cluster.CancelStore(s.store)
	// locate region again is needed
	// since last request on the region failed and region's info had been cleared.
	_, err = s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, NotNil)
	c.Assert(errors.Cause(err), Equals, context.Canceled)

	// set store to normal state.
	s.cluster.UnCancelStore(s.store)
	region, err = s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	resp, err = s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp.RawPut, NotNil)
}

func (s *testRegionRequestSuite) TestNoReloadRegionWhenCtxCanceled(c *C) {
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdRawPut,
		RawPut: &kvrpcpb.RawPutRequest{
			Key:   []byte("key"),
			Value: []byte("value"),
		},
	}
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)

	sender := s.regionRequestSender
	bo, cancel := s.bo.Fork()
	cancel()
	// Call SendKVReq with a canceled context.
	_, err = sender.SendReq(bo, req, region.Region, time.Second)
	// Check this kind of error won't cause region cache drop.
	c.Assert(errors.Cause(err), Equals, context.Canceled)
	c.Assert(sender.regionCache.getRegionByIDFromCache(s.region), NotNil)
}

// cancelContextClient wraps rpcClient and always cancels context before sending requests.
type cancelContextClient struct {
	Client
	redirectAddr string
}

func (c *cancelContextClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	childCtx, cancel := context.WithCancel(ctx)
	cancel()
	return c.Client.SendRequest(childCtx, c.redirectAddr, req, timeout)
}

// mockTikvGrpcServer mock a tikv gprc server for testing.
type mockTikvGrpcServer struct{}

// KvGet commands with mvcc/txn supported.
func (s *mockTikvGrpcServer) KvGet(context.Context, *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvScan(context.Context, *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvPrewrite(context.Context, *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvCommit(context.Context, *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvImport(context.Context, *kvrpcpb.ImportRequest) (*kvrpcpb.ImportResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvCleanup(context.Context, *kvrpcpb.CleanupRequest) (*kvrpcpb.CleanupResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvBatchGet(context.Context, *kvrpcpb.BatchGetRequest) (*kvrpcpb.BatchGetResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvBatchRollback(context.Context, *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvScanLock(context.Context, *kvrpcpb.ScanLockRequest) (*kvrpcpb.ScanLockResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvResolveLock(context.Context, *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvPessimisticLock(context.Context, *kvrpcpb.PessimisticLockRequest) (*kvrpcpb.PessimisticLockResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KVPessimisticRollback(context.Context, *kvrpcpb.PessimisticRollbackRequest) (*kvrpcpb.PessimisticRollbackResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvTxnHeartBeat(ctx context.Context, in *kvrpcpb.TxnHeartBeatRequest) (*kvrpcpb.TxnHeartBeatResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvGC(context.Context, *kvrpcpb.GCRequest) (*kvrpcpb.GCResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvDeleteRange(context.Context, *kvrpcpb.DeleteRangeRequest) (*kvrpcpb.DeleteRangeResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) RawGet(context.Context, *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) RawBatchGet(context.Context, *kvrpcpb.RawBatchGetRequest) (*kvrpcpb.RawBatchGetResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) RawPut(context.Context, *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) RawBatchPut(context.Context, *kvrpcpb.RawBatchPutRequest) (*kvrpcpb.RawBatchPutResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) RawDelete(context.Context, *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) RawBatchDelete(context.Context, *kvrpcpb.RawBatchDeleteRequest) (*kvrpcpb.RawBatchDeleteResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) RawScan(context.Context, *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) RawDeleteRange(context.Context, *kvrpcpb.RawDeleteRangeRequest) (*kvrpcpb.RawDeleteRangeResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) RawBatchScan(context.Context, *kvrpcpb.RawBatchScanRequest) (*kvrpcpb.RawBatchScanResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) UnsafeDestroyRange(context.Context, *kvrpcpb.UnsafeDestroyRangeRequest) (*kvrpcpb.UnsafeDestroyRangeResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) Coprocessor(context.Context, *coprocessor.Request) (*coprocessor.Response, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) Raft(tikvpb.Tikv_RaftServer) error {
	return errors.New("unreachable")
}
func (s *mockTikvGrpcServer) BatchRaft(tikvpb.Tikv_BatchRaftServer) error {
	return errors.New("unreachable")
}
func (s *mockTikvGrpcServer) Snapshot(tikvpb.Tikv_SnapshotServer) error {
	return errors.New("unreachable")
}
func (s *mockTikvGrpcServer) MvccGetByKey(context.Context, *kvrpcpb.MvccGetByKeyRequest) (*kvrpcpb.MvccGetByKeyResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) MvccGetByStartTs(context.Context, *kvrpcpb.MvccGetByStartTsRequest) (*kvrpcpb.MvccGetByStartTsResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) SplitRegion(context.Context, *kvrpcpb.SplitRegionRequest) (*kvrpcpb.SplitRegionResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) CoprocessorStream(*coprocessor.Request, tikvpb.Tikv_CoprocessorStreamServer) error {
	return errors.New("unreachable")
}
func (s *mockTikvGrpcServer) BatchCommands(tikvpb.Tikv_BatchCommandsServer) error {
	return errors.New("unreachable")
}
func (s *mockTikvGrpcServer) ReadIndex(context.Context, *kvrpcpb.ReadIndexRequest) (*kvrpcpb.ReadIndexResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *testRegionRequestSuite) TestNoReloadRegionForGrpcWhenCtxCanceled(c *C) {
	// prepare a mock tikv grpc server
	addr := "localhost:56341"
	lis, err := net.Listen("tcp", addr)
	c.Assert(err, IsNil)
	server := grpc.NewServer()
	tikvpb.RegisterTikvServer(server, &mockTikvGrpcServer{})
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		server.Serve(lis)
		wg.Done()
	}()

	client := newRPCClient(config.Security{})
	sender := NewRegionRequestSender(s.cache, client)
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdRawPut,
		RawPut: &kvrpcpb.RawPutRequest{
			Key:   []byte("key"),
			Value: []byte("value"),
		},
	}
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)

	bo, cancel := s.bo.Fork()
	cancel()
	_, err = sender.SendReq(bo, req, region.Region, 3*time.Second)
	c.Assert(errors.Cause(err), Equals, context.Canceled)
	c.Assert(s.cache.getRegionByIDFromCache(s.region), NotNil)

	// Just for covering error code = codes.Canceled.
	client1 := &cancelContextClient{
		Client:       newRPCClient(config.Security{}),
		redirectAddr: addr,
	}
	sender = NewRegionRequestSender(s.cache, client1)
	sender.SendReq(s.bo, req, region.Region, 3*time.Second)

	// cleanup
	server.Stop()
	wg.Wait()
}
