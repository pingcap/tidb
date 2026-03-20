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

package ddl

import (
	"bytes"
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPkdbCollectRaftLogIndexesRejectInvalidOptions(t *testing.T) {
	collector := &pkdbRaftIndexCollector{}
	onResult := func(context.Context, *debugpb.RegionInfoResponse) error { return nil }

	testCases := []struct {
		name    string
		options pkdbRaftIndexCollectOptions
		errMsg  string
	}{
		{
			name: "invalid page size",
			options: pkdbRaftIndexCollectOptions{
				pageSize:             0,
				readStoreConcurrency: 1,
				writePDConcurrency:   1,
			},
			errMsg: "pageSize must be greater than 0",
		},
		{
			name: "invalid read concurrency",
			options: pkdbRaftIndexCollectOptions{
				pageSize:             1,
				readStoreConcurrency: 0,
				writePDConcurrency:   1,
			},
			errMsg: "readStoreConcurrency must be greater than 0",
		},
		{
			name: "invalid write concurrency",
			options: pkdbRaftIndexCollectOptions{
				pageSize:             1,
				readStoreConcurrency: 1,
				writePDConcurrency:   0,
			},
			errMsg: "writePDConcurrency must be greater than 0",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := collector.collectRaftLogIndexes(context.Background(), tc.options, onResult)
			require.EqualError(t, err, tc.errMsg)
		})
	}
}

func TestPkdbFetchOneRetryStartKeyIsNonNilForMinStartKey(t *testing.T) {
	addr := startTestPkdbDebugServer(t, func(context.Context, *debugpb.RegionInfoRequest) (*debugpb.RegionInfoResponse, error) {
		return nil, status.Error(codes.Unavailable, "tikv unavailable")
	})

	var dialCount atomic.Int32
	pool := newTestPkdbDebugClientPool(&dialCount, 0)
	t.Cleanup(func() {
		closeTestDebugClientPool(t, pool)
	})

	collector := &pkdbRaftIndexCollector{debugCliPool: pool}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, retryFromKey, err := collector.fetchOne(ctx, pkdbFetchTask{
		region: pkdbScannedRegion{
			id:    101,
			epoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		},
		storeAddr: addr,
		scanKey:   nil,
	})
	require.Error(t, err)
	require.NotNil(t, retryFromKey)
	require.Len(t, retryFromKey, 0)
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestPkdbFetchOneWrapsEpochMismatch(t *testing.T) {
	addr := startTestPkdbDebugServer(t, func(context.Context, *debugpb.RegionInfoRequest) (*debugpb.RegionInfoResponse, error) {
		return newTestRegionInfoResponse(7, []byte("a"), []byte("z"), &metapb.RegionEpoch{ConfVer: 2, Version: 2}), nil
	})

	var dialCount atomic.Int32
	pool := newTestPkdbDebugClientPool(&dialCount, 0)
	t.Cleanup(func() {
		closeTestDebugClientPool(t, pool)
	})

	collector := &pkdbRaftIndexCollector{debugCliPool: pool}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	scanKey := []byte("a")
	_, retryFromKey, err := collector.fetchOne(ctx, pkdbFetchTask{
		region: pkdbScannedRegion{
			id:    7,
			epoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		},
		storeAddr: addr,
		scanKey:   scanKey,
	})
	require.Error(t, err)
	require.Equal(t, scanKey, retryFromKey)

	var mismatch *pkdbErrRegionEpochMismatch
	require.ErrorAs(t, err, &mismatch)
	require.EqualValues(t, 7, mismatch.regionID)
	require.EqualValues(t, 1, mismatch.expected.ConfVer)
	require.EqualValues(t, 2, mismatch.actual.ConfVer)
}

func TestPkdbCollectRoundStopsWritingAfterRetryBreakpoint(t *testing.T) {
	storeID := uint64(11)
	epochV1 := &metapb.RegionEpoch{ConfVer: 1, Version: 1}
	epochV2 := &metapb.RegionEpoch{ConfVer: 2, Version: 1}

	addr := startTestPkdbDebugServer(t, func(ctx context.Context, req *debugpb.RegionInfoRequest) (*debugpb.RegionInfoResponse, error) {
		switch req.GetRegionId() {
		case 1:
			return newTestRegionInfoResponse(1, []byte{}, []byte("g"), epochV1), nil
		case 2:
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
			return newTestRegionInfoResponse(2, []byte("g"), []byte("m"), epochV2), nil
		case 3:
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(80 * time.Millisecond):
			}
			return newTestRegionInfoResponse(3, []byte("m"), []byte{}, epochV1), nil
		default:
			return nil, status.Errorf(codes.NotFound, "unknown region id %d", req.GetRegionId())
		}
	})

	pdCli := &testPkdbCollectorPDClient{
		stores: []*metapb.Store{
			{Id: storeID, Address: addr},
		},
		regions: []*pd.Region{
			newTestRouterRegion(1, []byte{}, []byte("g"), storeID, epochV1),
			newTestRouterRegion(2, []byte("g"), []byte("m"), storeID, epochV1),
			newTestRouterRegion(3, []byte("m"), []byte{}, storeID, epochV1),
		},
	}

	var dialCount atomic.Int32
	pool := newTestPkdbDebugClientPool(&dialCount, 0)
	t.Cleanup(func() {
		closeTestDebugClientPool(t, pool)
	})

	collector := &pkdbRaftIndexCollector{
		pdCli:        pdCli,
		debugCliPool: pool,
	}

	var (
		mu          sync.Mutex
		handledIDs  []uint64
		handlerCall int
	)
	retryFromKey, err := collector.collectRound(context.Background(), pkdbRaftIndexCollectOptions{
		pageSize:             16,
		readStoreConcurrency: 2,
		writePDConcurrency:   1,
		retryWait:            0,
	}, func(_ context.Context, response *debugpb.RegionInfoResponse) error {
		mu.Lock()
		defer mu.Unlock()
		handlerCall++
		handledIDs = append(handledIDs, response.GetRegionLocalState().GetRegion().GetId())
		return nil
	})
	require.Error(t, err)
	require.Equal(t, []byte("g"), retryFromKey)

	var mismatch *pkdbErrRegionEpochMismatch
	require.ErrorAs(t, err, &mismatch)
	require.EqualValues(t, 2, mismatch.regionID)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, handlerCall)
	require.True(t, slices.Equal([]uint64{1}, handledIDs))
}

func TestSaveRaftAndRegionState2PDPassesAllStates(t *testing.T) {
	pdCli := &testSaveRaftStatePDClient{}
	onResult := saveRaftAndRegionState2PD(pdCli)

	raftLocal := &raft_serverpb.RaftLocalState{}
	raftApply := &raft_serverpb.RaftApplyState{AppliedIndex: 123}
	regionLocal := &raft_serverpb.RegionLocalState{
		Region: &metapb.Region{Id: 42},
	}

	err := onResult(context.Background(), &debugpb.RegionInfoResponse{
		RaftLocalState:   raftLocal,
		RaftApplyState:   raftApply,
		RegionLocalState: regionLocal,
	})
	require.NoError(t, err)

	pdCli.mu.Lock()
	defer pdCli.mu.Unlock()
	require.Equal(t, 1, pdCli.calls)
	require.Same(t, raftLocal, pdCli.lastRaftLocalState)
	require.Same(t, raftApply, pdCli.lastRaftApplyState)
	require.Same(t, regionLocal, pdCli.lastRegionLocalState)
}

func TestSaveRaftAndRegionState2PDRespectsContextCancel(t *testing.T) {
	pdCli := &testSaveRaftStatePDClient{err: errors.New("save failed")}
	onResult := saveRaftAndRegionState2PD(pdCli)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := onResult(ctx, &debugpb.RegionInfoResponse{
		RaftLocalState: &raft_serverpb.RaftLocalState{},
		RaftApplyState: &raft_serverpb.RaftApplyState{},
		RegionLocalState: &raft_serverpb.RegionLocalState{
			Region: &metapb.Region{Id: 7},
		},
	})
	require.ErrorIs(t, err, context.Canceled)
	pdCli.mu.Lock()
	defer pdCli.mu.Unlock()
	require.Equal(t, 1, pdCli.calls)
}

type testPkdbCollectorPDClient struct {
	pd.Client
	stores  []*metapb.Store
	regions []*pd.Region
}

func (c *testPkdbCollectorPDClient) GetAllStores(context.Context, ...pd.GetStoreOption) ([]*metapb.Store, error) {
	return c.stores, nil
}

func (c *testPkdbCollectorPDClient) GetStore(_ context.Context, storeID uint64) (*metapb.Store, error) {
	for _, store := range c.stores {
		if store.GetId() == storeID {
			return store, nil
		}
	}
	return nil, errors.New("store not found")
}

func (c *testPkdbCollectorPDClient) BatchScanRegions(_ context.Context, keyRanges []pd.KeyRange, limit int, _ ...pd.GetRegionOption) ([]*pd.Region, error) {
	startKey := []byte{}
	if len(keyRanges) > 0 {
		startKey = keyRanges[0].StartKey
	}

	regions := make([]*pd.Region, 0, len(c.regions))
	for _, region := range c.regions {
		endKey := region.Meta.GetEndKey()
		if len(endKey) != 0 && bytes.Compare(endKey, startKey) <= 0 {
			continue
		}
		regions = append(regions, region)
		if limit > 0 && len(regions) >= limit {
			break
		}
	}
	return regions, nil
}

type testSaveRaftStatePDClient struct {
	pd.Client

	mu                   sync.Mutex
	err                  error
	calls                int
	lastRaftLocalState   *raft_serverpb.RaftLocalState
	lastRaftApplyState   *raft_serverpb.RaftApplyState
	lastRegionLocalState *raft_serverpb.RegionLocalState
}

func (c *testSaveRaftStatePDClient) SaveRaftAndRegionState(
	_ context.Context,
	raftLocalState *raft_serverpb.RaftLocalState,
	raftApplyState *raft_serverpb.RaftApplyState,
	regionLocalState *raft_serverpb.RegionLocalState,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	c.lastRaftLocalState = raftLocalState
	c.lastRaftApplyState = raftApplyState
	c.lastRegionLocalState = regionLocalState
	return c.err
}

type testPkdbDebugServer struct {
	debugpb.UnimplementedDebugServer
	onRegionInfo func(context.Context, *debugpb.RegionInfoRequest) (*debugpb.RegionInfoResponse, error)
}

func (s *testPkdbDebugServer) RegionInfo(ctx context.Context, req *debugpb.RegionInfoRequest) (*debugpb.RegionInfoResponse, error) {
	return s.onRegionInfo(ctx, req)
}

func startTestPkdbDebugServer(t *testing.T, onRegionInfo func(context.Context, *debugpb.RegionInfoRequest) (*debugpb.RegionInfoResponse, error)) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	debugpb.RegisterDebugServer(server, &testPkdbDebugServer{
		onRegionInfo: onRegionInfo,
	})
	serveErrCh := make(chan error, 1)
	serveDone := make(chan struct{})

	go func() {
		defer close(serveDone)
		err := server.Serve(listener)
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			serveErrCh <- err
		}
	}()

	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		select {
		case <-serveDone:
		case <-time.After(3 * time.Second):
			t.Error("debug test server did not exit in time")
		}
		select {
		case err := <-serveErrCh:
			require.NoError(t, err)
		default:
		}
	})

	return listener.Addr().String()
}

func newTestRouterRegion(
	regionID uint64,
	startKey []byte,
	endKey []byte,
	storeID uint64,
	epoch *metapb.RegionEpoch,
) *pd.Region {
	leader := &metapb.Peer{
		Id:      regionID + 1000,
		StoreId: storeID,
	}
	return &pd.Region{
		Meta: &metapb.Region{
			Id:          regionID,
			StartKey:    append([]byte(nil), startKey...),
			EndKey:      append([]byte(nil), endKey...),
			RegionEpoch: epoch,
			Peers:       []*metapb.Peer{leader},
		},
		Leader: leader,
	}
}

func newTestRegionInfoResponse(
	regionID uint64,
	startKey []byte,
	endKey []byte,
	epoch *metapb.RegionEpoch,
) *debugpb.RegionInfoResponse {
	return &debugpb.RegionInfoResponse{
		RegionLocalState: &raft_serverpb.RegionLocalState{
			Region: &metapb.Region{
				Id:          regionID,
				StartKey:    append([]byte(nil), startKey...),
				EndKey:      append([]byte(nil), endKey...),
				RegionEpoch: epoch,
			},
		},
		RaftLocalState: &raft_serverpb.RaftLocalState{},
		RaftApplyState: &raft_serverpb.RaftApplyState{},
	}
}
