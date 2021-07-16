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
	"sync/atomic"
	"time"
	"unsafe"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	tikverr "github.com/pingcap/tidb/store/tikv/error"
	"github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/store/tikv/retry"

	"github.com/pingcap/tidb/store/tikv/config"
	"github.com/pingcap/tidb/store/tikv/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"google.golang.org/grpc"
)

type testRegionRequestToSingleStoreSuite struct {
	cluster             *mocktikv.Cluster
	store               uint64
	peer                uint64
	region              uint64
	cache               *RegionCache
	bo                  *Backoffer
	regionRequestSender *RegionRequestSender
	mvccStore           mocktikv.MVCCStore
}

type testRegionRequestToThreeStoresSuite struct {
	OneByOneSuite
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

var _ = Suite(&testRegionRequestToSingleStoreSuite{})
var _ = Suite(&testRegionRequestToThreeStoresSuite{})

func (s *testRegionRequestToSingleStoreSuite) SetUpTest(c *C) {
	s.cluster = mocktikv.NewCluster(mocktikv.MustNewMVCCStore())
	s.store, s.peer, s.region = mocktikv.BootstrapWithSingleStore(s.cluster)
	pdCli := &CodecPDClient{mocktikv.NewPDClient(s.cluster)}
	s.cache = NewRegionCache(pdCli)
	s.bo = retry.NewNoopBackoff(context.Background())
	s.mvccStore = mocktikv.MustNewMVCCStore()
	client := mocktikv.NewRPCClient(s.cluster, s.mvccStore, nil)
	s.regionRequestSender = NewRegionRequestSender(s.cache, client)
}

func (s *testRegionRequestToThreeStoresSuite) SetUpTest(c *C) {
	s.cluster = mocktikv.NewCluster(mocktikv.MustNewMVCCStore())
	s.storeIDs, s.peerIDs, s.regionID, s.leaderPeer = mocktikv.BootstrapWithMultiStores(s.cluster, 3)
	pdCli := &CodecPDClient{mocktikv.NewPDClient(s.cluster)}
	s.cache = NewRegionCache(pdCli)
	s.bo = retry.NewNoopBackoff(context.Background())
	s.mvccStore = mocktikv.MustNewMVCCStore()
	client := mocktikv.NewRPCClient(s.cluster, s.mvccStore, nil)
	s.regionRequestSender = NewRegionRequestSender(s.cache, client)
}

func (s *testRegionRequestToSingleStoreSuite) TearDownTest(c *C) {
	s.cache.Close()
}

func (s *testRegionRequestToThreeStoresSuite) TearDownTest(c *C) {
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

func (s *testRegionRequestToThreeStoresSuite) TestGetRPCContext(c *C) {
	// Load the bootstrapped region into the cache.
	_, err := s.cache.BatchLoadRegionsFromKey(s.bo, []byte{}, 1)
	c.Assert(err, IsNil)

	var seed uint32
	var regionID = RegionVerID{s.regionID, 0, 0}

	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{}, kv.ReplicaReadLeader, &seed)
	rpcCtx, err := s.regionRequestSender.getRPCContext(s.bo, req, regionID, tikvrpc.TiKV)
	c.Assert(err, IsNil)
	c.Assert(rpcCtx.Peer.Id, Equals, s.leaderPeer)

	req.ReplicaReadType = kv.ReplicaReadFollower
	rpcCtx, err = s.regionRequestSender.getRPCContext(s.bo, req, regionID, tikvrpc.TiKV)
	c.Assert(err, IsNil)
	c.Assert(rpcCtx.Peer.Id, Not(Equals), s.leaderPeer)

	req.ReplicaReadType = kv.ReplicaReadMixed
	rpcCtx, err = s.regionRequestSender.getRPCContext(s.bo, req, regionID, tikvrpc.TiKV)
	c.Assert(err, IsNil)
	c.Assert(rpcCtx.Peer.Id, Equals, s.leaderPeer)

	seed = 1
	rpcCtx, err = s.regionRequestSender.getRPCContext(s.bo, req, regionID, tikvrpc.TiKV)
	c.Assert(err, IsNil)
	c.Assert(rpcCtx.Peer.Id, Not(Equals), s.leaderPeer)
}

func (s *testRegionRequestToSingleStoreSuite) TestOnRegionError(c *C) {
	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
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
			staleResp := &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{
				RegionError: &errorpb.Error{StaleCommand: &errorpb.StaleCommand{}},
			}}
			return staleResp, nil
		}}
		bo := NewBackofferWithVars(context.Background(), 5, nil)
		resp, err := s.regionRequestSender.SendReq(bo, req, region.Region, time.Second)
		c.Assert(err, IsNil)
		c.Assert(resp, NotNil)
		regionErr, _ := resp.GetRegionError()
		c.Assert(regionErr, NotNil)
	}()
}

func (s *testRegionRequestToThreeStoresSuite) TestStoreTokenLimit(c *C) {
	req := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{}, kvrpcpb.Context{})
	region, err := s.cache.LocateRegionByID(s.bo, s.regionID)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	oldStoreLimit := kv.StoreLimit.Load()
	kv.StoreLimit.Store(500)
	s.cache.getStoreByStoreID(s.storeIDs[0]).tokenCount.Store(500)
	// cause there is only one region in this cluster, regionID maps this leader.
	resp, err := s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, NotNil)
	c.Assert(resp, IsNil)
	e, ok := errors.Cause(err).(*tikverr.ErrTokenLimit)
	c.Assert(ok, IsTrue)
	c.Assert(e.StoreID, Equals, uint64(1))
	kv.StoreLimit.Store(oldStoreLimit)
}

func (s *testRegionRequestToSingleStoreSuite) TestOnSendFailedWithStoreRestart(c *C) {
	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	resp, err := s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)
	c.Assert(s.regionRequestSender.rpcError, IsNil)

	// stop store.
	s.cluster.StopStore(s.store)
	_, err = s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, NotNil)
	// The RPC error shouldn't be nil since it failed to sent the request.
	c.Assert(s.regionRequestSender.rpcError, NotNil)

	// start store.
	s.cluster.StartStore(s.store)

	// locate region again is needed
	// since last request on the region failed and region's info had been cleared.
	region, err = s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	c.Assert(s.regionRequestSender.rpcError, NotNil)
	resp, err = s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)
}

func (s *testRegionRequestToSingleStoreSuite) TestOnSendFailedWithCloseKnownStoreThenUseNewOne(c *C) {
	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})

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
	c.Assert(resp.Resp, NotNil)

	// stop store2 and make store1 as new leader.
	s.cluster.StopStore(store2)
	s.cluster.ChangeLeader(s.region, s.peer)

	// send to store2 fail and send to new leader store1.
	bo2 := NewBackofferWithVars(context.Background(), 100, nil)
	resp, err = s.regionRequestSender.SendReq(bo2, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	regionErr, err := resp.GetRegionError()
	c.Assert(err, IsNil)
	c.Assert(regionErr, IsNil)
	c.Assert(resp.Resp, NotNil)
}

func (s *testRegionRequestToSingleStoreSuite) TestSendReqCtx(c *C) {
	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	resp, ctx, err := s.regionRequestSender.SendReqCtx(s.bo, req, region.Region, time.Second, tikvrpc.TiKV)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)
	c.Assert(ctx, NotNil)
	req.ReplicaRead = true
	resp, ctx, err = s.regionRequestSender.SendReqCtx(s.bo, req, region.Region, time.Second, tikvrpc.TiKV)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)
	c.Assert(ctx, NotNil)
}

func (s *testRegionRequestToSingleStoreSuite) TestOnSendFailedWithCancelled(c *C) {
	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	resp, err := s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)

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
	c.Assert(resp.Resp, NotNil)
}

func (s *testRegionRequestToSingleStoreSuite) TestNoReloadRegionWhenCtxCanceled(c *C) {
	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
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
func (s *mockTikvGrpcServer) KvCheckTxnStatus(ctx context.Context, in *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvCheckSecondaryLocks(ctx context.Context, in *kvrpcpb.CheckSecondaryLocksRequest) (*kvrpcpb.CheckSecondaryLocksResponse, error) {
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
func (s *mockTikvGrpcServer) IsAlive(context.Context, *mpp.IsAliveRequest) (*mpp.IsAliveResponse, error) {
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
func (s *mockTikvGrpcServer) RawGetKeyTTL(context.Context, *kvrpcpb.RawGetKeyTTLRequest) (*kvrpcpb.RawGetKeyTTLResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) UnsafeDestroyRange(context.Context, *kvrpcpb.UnsafeDestroyRangeRequest) (*kvrpcpb.UnsafeDestroyRangeResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) RegisterLockObserver(context.Context, *kvrpcpb.RegisterLockObserverRequest) (*kvrpcpb.RegisterLockObserverResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) CheckLockObserver(context.Context, *kvrpcpb.CheckLockObserverRequest) (*kvrpcpb.CheckLockObserverResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) RemoveLockObserver(context.Context, *kvrpcpb.RemoveLockObserverRequest) (*kvrpcpb.RemoveLockObserverResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) PhysicalScanLock(context.Context, *kvrpcpb.PhysicalScanLockRequest) (*kvrpcpb.PhysicalScanLockResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) Coprocessor(context.Context, *coprocessor.Request) (*coprocessor.Response, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) BatchCoprocessor(*coprocessor.BatchRequest, tikvpb.Tikv_BatchCoprocessorServer) error {
	return errors.New("unreachable")
}
func (s *mockTikvGrpcServer) RawCoprocessor(context.Context, *kvrpcpb.RawCoprocessorRequest) (*kvrpcpb.RawCoprocessorResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) DispatchMPPTask(context.Context, *mpp.DispatchTaskRequest) (*mpp.DispatchTaskResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) EstablishMPPConnection(*mpp.EstablishMPPConnectionRequest, tikvpb.Tikv_EstablishMPPConnectionServer) error {
	return errors.New("unreachable")
}
func (s *mockTikvGrpcServer) CancelMPPTask(context.Context, *mpp.CancelTaskRequest) (*mpp.CancelTaskResponse, error) {
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

func (s *mockTikvGrpcServer) CheckLeader(context.Context, *kvrpcpb.CheckLeaderRequest) (*kvrpcpb.CheckLeaderResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) GetStoreSafeTS(context.Context, *kvrpcpb.StoreSafeTSRequest) (*kvrpcpb.StoreSafeTSResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) RawCompareAndSwap(context.Context, *kvrpcpb.RawCASRequest) (*kvrpcpb.RawCASResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) GetLockWaitInfo(context.Context, *kvrpcpb.GetLockWaitInfoRequest) (*kvrpcpb.GetLockWaitInfoResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *testRegionRequestToSingleStoreSuite) TestNoReloadRegionForGrpcWhenCtxCanceled(c *C) {
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

	client := NewRPCClient(config.Security{})
	sender := NewRegionRequestSender(s.cache, client)
	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)

	bo, cancel := s.bo.Fork()
	cancel()
	_, err = sender.SendReq(bo, req, region.Region, 3*time.Second)
	c.Assert(errors.Cause(err), Equals, context.Canceled)
	c.Assert(s.cache.getRegionByIDFromCache(s.region), NotNil)

	// Just for covering error code = codes.Canceled.
	client1 := &cancelContextClient{
		Client:       NewRPCClient(config.Security{}),
		redirectAddr: addr,
	}
	sender = NewRegionRequestSender(s.cache, client1)
	sender.SendReq(s.bo, req, region.Region, 3*time.Second)

	// cleanup
	server.Stop()
	wg.Wait()
}

func (s *testRegionRequestToSingleStoreSuite) TestOnMaxTimestampNotSyncedError(c *C) {
	req := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)

	// test retry for max timestamp not synced
	func() {
		oc := s.regionRequestSender.client
		defer func() {
			s.regionRequestSender.client = oc
		}()
		count := 0
		s.regionRequestSender.client = &fnClient{func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
			count++
			var resp *tikvrpc.Response
			if count < 3 {
				resp = &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{
					RegionError: &errorpb.Error{MaxTimestampNotSynced: &errorpb.MaxTimestampNotSynced{}},
				}}
			} else {
				resp = &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{}}
			}
			return resp, nil
		}}
		bo := NewBackofferWithVars(context.Background(), 5, nil)
		resp, err := s.regionRequestSender.SendReq(bo, req, region.Region, time.Second)
		c.Assert(err, IsNil)
		c.Assert(resp, NotNil)
	}()
}

func (s *testRegionRequestToThreeStoresSuite) TestSwitchPeerWhenNoLeader(c *C) {
	var leaderAddr string
	s.regionRequestSender.client = &fnClient{func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
		if leaderAddr == "" {
			leaderAddr = addr
		}
		// Returns OK when switches to a different peer.
		if leaderAddr != addr {
			return &tikvrpc.Response{Resp: &kvrpcpb.RawPutResponse{}}, nil
		}
		return &tikvrpc.Response{Resp: &kvrpcpb.RawPutResponse{
			RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{}},
		}}, nil
	}}

	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})

	bo := NewBackofferWithVars(context.Background(), 5, nil)
	loc, err := s.cache.LocateKey(s.bo, []byte("key"))
	c.Assert(err, IsNil)
	resp, err := s.regionRequestSender.SendReq(bo, req, loc.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp, NotNil)
}

func (s *testRegionRequestToThreeStoresSuite) loadAndGetLeaderStore(c *C) (*Store, string) {
	region, err := s.regionRequestSender.regionCache.findRegionByKey(s.bo, []byte("a"), false)
	c.Assert(err, IsNil)
	leaderStore, leaderPeer, _, _ := region.WorkStorePeer(region.getStore())
	c.Assert(leaderPeer.Id, Equals, s.leaderPeer)
	leaderAddr, err := s.regionRequestSender.regionCache.getStoreAddr(s.bo, region, leaderStore)
	c.Assert(err, IsNil)
	return leaderStore, leaderAddr
}

func (s *testRegionRequestToThreeStoresSuite) TestForwarding(c *C) {
	s.regionRequestSender.regionCache.enableForwarding = true

	// First get the leader's addr from region cache
	leaderStore, leaderAddr := s.loadAndGetLeaderStore(c)

	bo := NewBackoffer(context.Background(), 10000)

	// Simulate that the leader is network-partitioned but can be accessed by forwarding via a follower
	innerClient := s.regionRequestSender.client
	s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
		if addr == leaderAddr {
			return nil, errors.New("simulated rpc error")
		}
		// MockTiKV doesn't support forwarding. Simulate forwarding here.
		if len(req.ForwardedHost) != 0 {
			addr = req.ForwardedHost
		}
		return innerClient.SendRequest(ctx, addr, req, timeout)
	}}
	var storeState uint32 = uint32(unreachable)
	s.regionRequestSender.regionCache.testingKnobs.mockRequestLiveness = func(s *Store, bo *Backoffer) livenessState {
		return livenessState(atomic.LoadUint32(&storeState))
	}

	loc, err := s.regionRequestSender.regionCache.LocateKey(bo, []byte("k"))
	c.Assert(err, IsNil)
	c.Assert(loc.Region.GetID(), Equals, s.regionID)
	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("k"),
		Value: []byte("v1"),
	})
	resp, ctx, err := s.regionRequestSender.SendReqCtx(bo, req, loc.Region, time.Second, tikvrpc.TiKV)
	c.Assert(err, IsNil)
	regionErr, err := resp.GetRegionError()
	c.Assert(err, IsNil)
	c.Assert(regionErr, IsNil)
	c.Assert(resp.Resp.(*kvrpcpb.RawPutResponse).Error, Equals, "")
	c.Assert(ctx.Addr, Equals, leaderAddr)
	c.Assert(ctx.ProxyStore, NotNil)
	c.Assert(ctx.ProxyAddr, Not(Equals), leaderAddr)
	c.Assert(ctx.ProxyAccessIdx, Not(Equals), ctx.AccessIdx)
	c.Assert(err, IsNil)

	// Simulate recovering to normal
	s.regionRequestSender.client = innerClient
	atomic.StoreUint32(&storeState, uint32(reachable))
	start := time.Now()
	for {
		if atomic.LoadInt32(&leaderStore.needForwarding) == 0 {
			break
		}
		if time.Since(start) > 3*time.Second {
			c.Fatal("store didn't recover to normal in time")
		}
		time.Sleep(time.Millisecond * 200)
	}
	atomic.StoreUint32(&storeState, uint32(unreachable))

	req = tikvrpc.NewRequest(tikvrpc.CmdRawGet, &kvrpcpb.RawGetRequest{Key: []byte("k")})
	resp, ctx, err = s.regionRequestSender.SendReqCtx(bo, req, loc.Region, time.Second, tikvrpc.TiKV)
	c.Assert(err, IsNil)
	regionErr, err = resp.GetRegionError()
	c.Assert(err, IsNil)
	c.Assert(regionErr, IsNil)
	c.Assert(resp.Resp.(*kvrpcpb.RawGetResponse).Value, BytesEquals, []byte("v1"))
	c.Assert(ctx.ProxyStore, IsNil)

	// Simulate server down
	s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
		if addr == leaderAddr || req.ForwardedHost == leaderAddr {
			return nil, errors.New("simulated rpc error")
		}

		// MockTiKV doesn't support forwarding. Simulate forwarding here.
		if len(req.ForwardedHost) != 0 {
			addr = req.ForwardedHost
		}
		return innerClient.SendRequest(ctx, addr, req, timeout)
	}}
	// The leader is changed after a store is down.
	newLeaderPeerID := s.peerIDs[0]
	if newLeaderPeerID == s.leaderPeer {
		newLeaderPeerID = s.peerIDs[1]
	}

	c.Assert(newLeaderPeerID, Not(Equals), s.leaderPeer)
	s.cluster.ChangeLeader(s.regionID, newLeaderPeerID)

	req = tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("k"),
		Value: []byte("v2"),
	})
	resp, ctx, err = s.regionRequestSender.SendReqCtx(bo, req, loc.Region, time.Second, tikvrpc.TiKV)
	c.Assert(err, IsNil)
	regionErr, err = resp.GetRegionError()
	c.Assert(err, IsNil)
	// After several retries, the region will be marked as needReload.
	// Then SendReqCtx will throw a pseudo EpochNotMatch to tell the caller to reload the region.
	c.Assert(regionErr.EpochNotMatch, NotNil)
	c.Assert(ctx, IsNil)
	c.Assert(len(s.regionRequestSender.failStoreIDs), Equals, 0)
	c.Assert(len(s.regionRequestSender.failProxyStoreIDs), Equals, 0)
	region := s.regionRequestSender.regionCache.GetCachedRegionWithRLock(loc.Region)
	c.Assert(region, NotNil)
	c.Assert(region.checkNeedReload(), IsTrue)

	loc, err = s.regionRequestSender.regionCache.LocateKey(bo, []byte("k"))
	c.Assert(err, IsNil)
	req = tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("k"),
		Value: []byte("v2"),
	})
	resp, ctx, err = s.regionRequestSender.SendReqCtx(bo, req, loc.Region, time.Second, tikvrpc.TiKV)
	c.Assert(err, IsNil)
	regionErr, err = resp.GetRegionError()
	c.Assert(err, IsNil)
	c.Assert(regionErr, IsNil)
	c.Assert(resp.Resp.(*kvrpcpb.RawPutResponse).Error, Equals, "")
	// Leader changed
	c.Assert(ctx.Store.storeID, Not(Equals), leaderStore.storeID)
	c.Assert(ctx.ProxyStore, IsNil)
}

func (s *testRegionRequestToSingleStoreSuite) TestGetRegionByIDFromCache(c *C) {
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)

	// test kv epochNotMatch return empty regions
	s.cache.OnRegionEpochNotMatch(s.bo, &RPCContext{Region: region.Region, Store: &Store{storeID: s.store}}, []*metapb.Region{})
	c.Assert(err, IsNil)
	r := s.cache.getRegionByIDFromCache(s.region)
	c.Assert(r, IsNil)

	// refill cache
	region, err = s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)

	// test kv load new region with new start-key and new epoch
	v2 := region.Region.confVer + 1
	r2 := metapb.Region{Id: region.Region.id, RegionEpoch: &metapb.RegionEpoch{Version: region.Region.ver, ConfVer: v2}, StartKey: []byte{1}}
	st := &Store{storeID: s.store}
	s.cache.insertRegionToCache(&Region{meta: &r2, store: unsafe.Pointer(st), lastAccess: time.Now().Unix()})
	region, err = s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	c.Assert(region.Region.confVer, Equals, v2)
	c.Assert(region.Region.ver, Equals, region.Region.ver)

	v3 := region.Region.confVer + 1
	r3 := metapb.Region{Id: region.Region.id, RegionEpoch: &metapb.RegionEpoch{Version: v3, ConfVer: region.Region.confVer}, StartKey: []byte{2}}
	st = &Store{storeID: s.store}
	s.cache.insertRegionToCache(&Region{meta: &r3, store: unsafe.Pointer(st), lastAccess: time.Now().Unix()})
	region, err = s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	c.Assert(region.Region.confVer, Equals, region.Region.confVer)
	c.Assert(region.Region.ver, Equals, v3)
}

func (s *testRegionRequestToThreeStoresSuite) TestReplicaSelector(c *C) {
	regionLoc, err := s.cache.LocateRegionByID(s.bo, s.regionID)
	c.Assert(err, IsNil)
	c.Assert(regionLoc, NotNil)
	region := s.cache.GetCachedRegionWithRLock(regionLoc.Region)
	regionStore := region.getStore()

	// Create a fake region and change its leader to the last peer.
	regionStore = regionStore.clone()
	regionStore.workTiKVIdx = AccessIndex(len(regionStore.stores) - 1)
	sidx, _ := regionStore.accessStore(TiKVOnly, regionStore.workTiKVIdx)
	regionStore.stores[sidx].epoch++
	regionStore.storeEpochs[sidx]++
	// Add a TiFlash peer to the region.
	peer := &metapb.Peer{Id: s.cluster.AllocID(), StoreId: s.cluster.AllocID()}
	regionStore.accessIndex[TiFlashOnly] = append(regionStore.accessIndex[TiFlashOnly], len(regionStore.stores))
	regionStore.stores = append(regionStore.stores, &Store{storeID: peer.StoreId, storeType: tikvrpc.TiFlash})
	regionStore.storeEpochs = append(regionStore.storeEpochs, 0)

	region = &Region{
		meta: region.GetMeta(),
	}
	region.lastAccess = time.Now().Unix()
	region.meta.Peers = append(region.meta.Peers, peer)
	atomic.StorePointer(&region.store, unsafe.Pointer(regionStore))

	cache := NewRegionCache(s.cache.pdClient)
	defer cache.Close()
	cache.insertRegionToCache(region)

	// Verify creating the replicaSelector.
	replicaSelector, err := newReplicaSelector(cache, regionLoc.Region)
	c.Assert(replicaSelector, NotNil)
	c.Assert(err, IsNil)
	c.Assert(replicaSelector.region, Equals, region)
	// Should only contains TiKV stores.
	c.Assert(len(replicaSelector.replicas), Equals, regionStore.accessStoreNum(TiKVOnly))
	c.Assert(len(replicaSelector.replicas), Equals, len(regionStore.stores)-1)
	c.Assert(replicaSelector.nextReplicaIdx == 0, IsTrue)
	c.Assert(replicaSelector.isExhausted(), IsFalse)

	// Verify that the store matches the peer and epoch.
	for _, replica := range replicaSelector.replicas {
		c.Assert(replica.store.storeID, Equals, replica.peer.GetStoreId())
		c.Assert(replica.peer, Equals, region.getPeerOnStore(replica.store.storeID))
		c.Assert(replica.attempts == 0, IsTrue)

		for i, store := range regionStore.stores {
			if replica.store == store {
				c.Assert(replica.epoch, Equals, regionStore.storeEpochs[i])
			}
		}
	}
	// Verify that the leader replica is at the head of replicas.
	leaderStore, leaderPeer, _, _ := region.WorkStorePeer(regionStore)
	leaderReplica := replicaSelector.replicas[0]
	c.Assert(leaderReplica.store, Equals, leaderStore)
	c.Assert(leaderReplica.peer, Equals, leaderPeer)

	assertRPCCtxEqual := func(rpcCtx *RPCContext, replica *replica) {
		c.Assert(rpcCtx.Store, Equals, replicaSelector.replicas[replicaSelector.nextReplicaIdx-1].store)
		c.Assert(rpcCtx.Peer, Equals, replicaSelector.replicas[replicaSelector.nextReplicaIdx-1].peer)
		c.Assert(rpcCtx.Addr, Equals, replicaSelector.replicas[replicaSelector.nextReplicaIdx-1].store.addr)
		c.Assert(rpcCtx.AccessMode, Equals, TiKVOnly)
	}

	// Verify the correctness of next()
	for i := 0; i < len(replicaSelector.replicas); i++ {
		rpcCtx, err := replicaSelector.next(s.bo)
		c.Assert(rpcCtx, NotNil)
		c.Assert(err, IsNil)
		c.Assert(rpcCtx.Region, Equals, regionLoc.Region)
		c.Assert(rpcCtx.Meta, Equals, region.meta)
		replica := replicaSelector.replicas[replicaSelector.nextReplicaIdx-1]
		assertRPCCtxEqual(rpcCtx, replica)
		c.Assert(replica.attempts, Equals, 1)
		c.Assert(replicaSelector.nextReplicaIdx, Equals, i+1)
	}
	c.Assert(replicaSelector.isExhausted(), IsTrue)
	rpcCtx, err := replicaSelector.next(s.bo)
	c.Assert(rpcCtx, IsNil)
	c.Assert(err, IsNil)
	// The region should be invalidated if runs out of all replicas.
	c.Assert(replicaSelector.region.isValid(), IsFalse)

	region.lastAccess = time.Now().Unix()
	replicaSelector, err = newReplicaSelector(cache, regionLoc.Region)
	c.Assert(err, IsNil)
	c.Assert(replicaSelector, NotNil)
	cache.testingKnobs.mockRequestLiveness = func(s *Store, bo *Backoffer) livenessState {
		return reachable
	}
	for i := 0; i < maxReplicaAttempt; i++ {
		rpcCtx, err := replicaSelector.next(s.bo)
		c.Assert(rpcCtx, NotNil)
		c.Assert(err, IsNil)
		nextIdx := replicaSelector.nextReplicaIdx
		// Verify that retry the same store if it's reachable.
		replicaSelector.onSendFailure(s.bo, nil)
		c.Assert(nextIdx, Equals, replicaSelector.nextReplicaIdx+1)
		c.Assert(replicaSelector.nextReplica().attempts, Equals, i+1)
	}
	// Verify the maxReplicaAttempt limit for each replica.
	rpcCtx, err = replicaSelector.next(s.bo)
	c.Assert(err, IsNil)
	assertRPCCtxEqual(rpcCtx, replicaSelector.replicas[1])
	c.Assert(replicaSelector.nextReplicaIdx, Equals, 2)

	// Verify updating leader.
	replicaSelector, _ = newReplicaSelector(cache, regionLoc.Region)
	replicaSelector.next(s.bo)
	// The leader is the 3rd replica. After updating leader, it should be the next.
	leader := replicaSelector.replicas[2]
	replicaSelector.updateLeader(leader.peer)
	c.Assert(replicaSelector.nextReplica(), Equals, leader)
	c.Assert(replicaSelector.nextReplicaIdx, Equals, 1)
	rpcCtx, _ = replicaSelector.next(s.bo)
	assertRPCCtxEqual(rpcCtx, leader)
	// Verify the regionStore is updated and the workTiKVIdx points to the leader.
	regionStore = region.getStore()
	leaderStore, leaderPeer, _, _ = region.WorkStorePeer(regionStore)
	c.Assert(leaderStore, Equals, leader.store)
	c.Assert(leaderPeer, Equals, leader.peer)

	replicaSelector, _ = newReplicaSelector(cache, regionLoc.Region)
	replicaSelector.next(s.bo)
	replicaSelector.next(s.bo)
	replicaSelector.next(s.bo)
	c.Assert(replicaSelector.isExhausted(), IsTrue)
	// The leader is the 1st replica. After updating leader, it should be the next and
	// the currnet replica is skipped.
	leader = replicaSelector.replicas[0]
	replicaSelector.updateLeader(leader.peer)
	// The leader should be the next replica.
	c.Assert(replicaSelector.nextReplica(), Equals, leader)
	c.Assert(replicaSelector.nextReplicaIdx, Equals, 2)
	rpcCtx, _ = replicaSelector.next(s.bo)
	c.Assert(replicaSelector.isExhausted(), IsTrue)
	assertRPCCtxEqual(rpcCtx, leader)
	// Verify the regionStore is updated and the workTiKVIdx points to the leader.
	regionStore = region.getStore()
	leaderStore, leaderPeer, _, _ = region.WorkStorePeer(regionStore)
	c.Assert(leaderStore, Equals, leader.store)
	c.Assert(leaderPeer, Equals, leader.peer)

	// Give the leader one more chance even if it exceeds the maxReplicaAttempt.
	replicaSelector, _ = newReplicaSelector(cache, regionLoc.Region)
	leader = replicaSelector.replicas[0]
	leader.attempts = maxReplicaAttempt
	replicaSelector.updateLeader(leader.peer)
	c.Assert(leader.attempts, Equals, maxReplicaAttempt-1)
	rpcCtx, _ = replicaSelector.next(s.bo)
	assertRPCCtxEqual(rpcCtx, leader)
	c.Assert(leader.attempts, Equals, maxReplicaAttempt)

	// Invalidate the region if the leader is not in the region.
	region.lastAccess = time.Now().Unix()
	replicaSelector.updateLeader(&metapb.Peer{Id: s.cluster.AllocID(), StoreId: s.cluster.AllocID()})
	c.Assert(region.isValid(), IsFalse)
	// Don't try next replica if the region is invalidated.
	rpcCtx, err = replicaSelector.next(s.bo)
	c.Assert(rpcCtx, IsNil)
	c.Assert(err, IsNil)

	// Verify on send success.
	region.lastAccess = time.Now().Unix()
	replicaSelector, _ = newReplicaSelector(cache, regionLoc.Region)
	replicaSelector.next(s.bo)
	rpcCtx, err = replicaSelector.next(s.bo)
	c.Assert(err, IsNil)
	replicaSelector.OnSendSuccess()
	// Verify the regionStore is updated and the workTiKVIdx points to the leader.
	leaderStore, leaderPeer, _, _ = region.WorkStorePeer(region.getStore())
	c.Assert(leaderStore, Equals, rpcCtx.Store)
	c.Assert(leaderPeer, Equals, rpcCtx.Peer)
}

// TODO(youjiali1995): Remove duplicated tests. This test may be duplicated with other
// tests but it's a dedicated one to test sending requests with the replica selector.
func (s *testRegionRequestToThreeStoresSuite) TestSendReqWithReplicaSelector(c *C) {
	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.regionID)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)

	reloadRegion := func() {
		s.regionRequestSender.leaderReplicaSelector.region.invalidate(Other)
		region, _ = s.cache.LocateRegionByID(s.bo, s.regionID)
	}

	hasFakeRegionError := func(resp *tikvrpc.Response) bool {
		if resp == nil {
			return false
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return false
		}
		return isFakeRegionError(regionErr)
	}

	// Normal
	bo := retry.NewBackoffer(context.Background(), -1)
	sender := s.regionRequestSender
	resp, err := sender.SendReq(bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp, NotNil)
	c.Assert(bo.GetTotalBackoffTimes() == 0, IsTrue)

	// Switch to the next Peer due to store failure and the leader is on the next peer.
	bo = retry.NewBackoffer(context.Background(), -1)
	s.cluster.ChangeLeader(s.regionID, s.peerIDs[1])
	s.cluster.StopStore(s.storeIDs[0])
	resp, err = sender.SendReq(bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp, NotNil)
	c.Assert(sender.leaderReplicaSelector.nextReplicaIdx, Equals, 2)
	c.Assert(bo.GetTotalBackoffTimes() == 1, IsTrue)
	s.cluster.StartStore(s.storeIDs[0])

	// Leader is updated because of send success, so no backoff.
	bo = retry.NewBackoffer(context.Background(), -1)
	resp, err = sender.SendReq(bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp, NotNil)
	c.Assert(sender.leaderReplicaSelector.nextReplicaIdx, Equals, 1)
	c.Assert(bo.GetTotalBackoffTimes() == 0, IsTrue)

	// Switch to the next peer due to leader failure but the new leader is not elected.
	// Region will be invalidated due to store epoch changed.
	reloadRegion()
	s.cluster.StopStore(s.storeIDs[1])
	bo = retry.NewBackoffer(context.Background(), -1)
	resp, err = sender.SendReq(bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(hasFakeRegionError(resp), IsTrue)
	c.Assert(sender.leaderReplicaSelector.isExhausted(), IsFalse)
	c.Assert(bo.GetTotalBackoffTimes(), Equals, 1)
	s.cluster.StartStore(s.storeIDs[1])

	// Leader is changed. No backoff.
	reloadRegion()
	s.cluster.ChangeLeader(s.regionID, s.peerIDs[0])
	bo = retry.NewBackoffer(context.Background(), -1)
	resp, err = sender.SendReq(bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp, NotNil)
	c.Assert(bo.GetTotalBackoffTimes(), Equals, 0)

	// No leader. Backoff for each replica and runs out all replicas.
	s.cluster.GiveUpLeader(s.regionID)
	bo = retry.NewBackoffer(context.Background(), -1)
	resp, err = sender.SendReq(bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(hasFakeRegionError(resp), IsTrue)
	c.Assert(bo.GetTotalBackoffTimes(), Equals, 3)
	c.Assert(sender.leaderReplicaSelector.isExhausted(), IsTrue)
	c.Assert(sender.leaderReplicaSelector.region.isValid(), IsFalse)
	s.cluster.ChangeLeader(s.regionID, s.peerIDs[0])

	// The leader store is alive but can't provide service.
	// Region will be invalidated due to running out of all replicas.
	s.regionRequestSender.regionCache.testingKnobs.mockRequestLiveness = func(s *Store, bo *Backoffer) livenessState {
		return reachable
	}
	reloadRegion()
	s.cluster.StopStore(s.storeIDs[0])
	bo = retry.NewBackoffer(context.Background(), -1)
	resp, err = sender.SendReq(bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(hasFakeRegionError(resp), IsTrue)
	c.Assert(sender.leaderReplicaSelector.isExhausted(), IsTrue)
	c.Assert(sender.leaderReplicaSelector.region.isValid(), IsFalse)
	c.Assert(bo.GetTotalBackoffTimes(), Equals, maxReplicaAttempt+2)
	s.cluster.StartStore(s.storeIDs[0])

	// Verify that retry the same replica when meets ServerIsBusy/MaxTimestampNotSynced/ReadIndexNotReady/ProposalInMergingMode.
	for _, regionErr := range []*errorpb.Error{
		// ServerIsBusy takes too much time to test.
		// {ServerIsBusy: &errorpb.ServerIsBusy{}},
		{MaxTimestampNotSynced: &errorpb.MaxTimestampNotSynced{}},
		{ReadIndexNotReady: &errorpb.ReadIndexNotReady{}},
		{ProposalInMergingMode: &errorpb.ProposalInMergingMode{}},
	} {
		func() {
			oc := sender.client
			defer func() {
				sender.client = oc
			}()
			s.regionRequestSender.client = &fnClient{func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
				// Return the specific region error when accesses the leader.
				if addr == s.cluster.GetStore(s.storeIDs[0]).Address {
					return &tikvrpc.Response{Resp: &kvrpcpb.RawPutResponse{RegionError: regionErr}}, nil
				}
				// Return the not leader error when accesses followers.
				return &tikvrpc.Response{Resp: &kvrpcpb.RawPutResponse{RegionError: &errorpb.Error{
					NotLeader: &errorpb.NotLeader{
						RegionId: region.Region.id, Leader: &metapb.Peer{Id: s.peerIDs[0], StoreId: s.storeIDs[0]},
					}}}}, nil

			}}
			reloadRegion()
			bo = retry.NewBackoffer(context.Background(), -1)
			resp, err := sender.SendReq(bo, req, region.Region, time.Second)
			c.Assert(err, IsNil)
			c.Assert(hasFakeRegionError(resp), IsTrue)
			c.Assert(sender.leaderReplicaSelector.isExhausted(), IsTrue)
			c.Assert(sender.leaderReplicaSelector.region.isValid(), IsFalse)
			c.Assert(bo.GetTotalBackoffTimes(), Equals, maxReplicaAttempt+2)
		}()
	}

	// Verify switch to the next peer immediately when meets StaleCommand.
	reloadRegion()
	func() {
		oc := sender.client
		defer func() {
			sender.client = oc
		}()
		s.regionRequestSender.client = &fnClient{func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
			return &tikvrpc.Response{Resp: &kvrpcpb.RawPutResponse{RegionError: &errorpb.Error{StaleCommand: &errorpb.StaleCommand{}}}}, nil
		}}
		reloadRegion()
		bo = retry.NewBackoffer(context.Background(), -1)
		resp, err := sender.SendReq(bo, req, region.Region, time.Second)
		c.Assert(err, IsNil)
		c.Assert(hasFakeRegionError(resp), IsTrue)
		c.Assert(sender.leaderReplicaSelector.isExhausted(), IsTrue)
		c.Assert(sender.leaderReplicaSelector.region.isValid(), IsFalse)
		c.Assert(bo.GetTotalBackoffTimes(), Equals, 0)
	}()

	// Verify don't invalidate region when meets unknown region errors.
	reloadRegion()
	func() {
		oc := sender.client
		defer func() {
			sender.client = oc
		}()
		s.regionRequestSender.client = &fnClient{func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
			return &tikvrpc.Response{Resp: &kvrpcpb.RawPutResponse{RegionError: &errorpb.Error{Message: ""}}}, nil
		}}
		reloadRegion()
		bo = retry.NewBackoffer(context.Background(), -1)
		resp, err := sender.SendReq(bo, req, region.Region, time.Second)
		c.Assert(err, IsNil)
		c.Assert(hasFakeRegionError(resp), IsTrue)
		c.Assert(sender.leaderReplicaSelector.isExhausted(), IsTrue)
		c.Assert(sender.leaderReplicaSelector.region.isValid(), IsFalse)
		c.Assert(bo.GetTotalBackoffTimes(), Equals, 0)
	}()

	// Verify invalidate region when meets StoreNotMatch/RegionNotFound/EpochNotMatch/NotLeader and can't find the leader in region.
	for i, regionErr := range []*errorpb.Error{
		{StoreNotMatch: &errorpb.StoreNotMatch{}},
		{RegionNotFound: &errorpb.RegionNotFound{}},
		{EpochNotMatch: &errorpb.EpochNotMatch{}},
		{NotLeader: &errorpb.NotLeader{Leader: &metapb.Peer{}}}} {
		func() {
			oc := sender.client
			defer func() {
				sender.client = oc
			}()
			s.regionRequestSender.client = &fnClient{func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
				return &tikvrpc.Response{Resp: &kvrpcpb.RawPutResponse{RegionError: regionErr}}, nil

			}}
			reloadRegion()
			bo = retry.NewBackoffer(context.Background(), -1)
			resp, err := sender.SendReq(bo, req, region.Region, time.Second)

			// Return a sendError when meets NotLeader and can't find the leader in the region.
			if i == 3 {
				c.Assert(err, IsNil)
				c.Assert(hasFakeRegionError(resp), IsTrue)
			} else {
				c.Assert(err, IsNil)
				c.Assert(resp, NotNil)
				regionErr, _ := resp.GetRegionError()
				c.Assert(regionErr, NotNil)
			}
			c.Assert(sender.leaderReplicaSelector.isExhausted(), IsFalse)
			c.Assert(sender.leaderReplicaSelector.region.isValid(), IsFalse)
			c.Assert(bo.GetTotalBackoffTimes(), Equals, 0)
		}()
	}

	// Runs out of all replicas and then returns a send error.
	s.regionRequestSender.regionCache.testingKnobs.mockRequestLiveness = func(s *Store, bo *Backoffer) livenessState {
		return unreachable
	}
	reloadRegion()
	for _, store := range s.storeIDs {
		s.cluster.StopStore(store)
	}
	bo = retry.NewBackoffer(context.Background(), -1)
	resp, err = sender.SendReq(bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(hasFakeRegionError(resp), IsTrue)
	c.Assert(bo.GetTotalBackoffTimes() == 3, IsTrue)
	c.Assert(sender.leaderReplicaSelector.region.isValid(), IsFalse)
	for _, store := range s.storeIDs {
		s.cluster.StartStore(store)
	}
}
