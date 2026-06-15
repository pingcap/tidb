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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/logreplicationpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/rawkv"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	testWaitDataSyncEpochConfVer uint64 = 10
	testWaitDataSyncEpochVersion uint64 = 10
)

func TestFetchRegionCommitIndexInBatch(t *testing.T) {
	oldRetryWait := tidbWaitDataSyncScanRetryWaitDuration
	tidbWaitDataSyncScanRetryWaitDuration = time.Millisecond
	t.Cleanup(func() { tidbWaitDataSyncScanRetryWaitDuration = oldRetryWait })

	oldScanBatchSize := tidbWaitDataSyncScanBatchSize
	tidbWaitDataSyncScanBatchSize = 3
	t.Cleanup(func() { tidbWaitDataSyncScanBatchSize = oldScanBatchSize })

	addr := startTestWaitDataSyncDebugServer(t, nil)

	pool := newPkdbDebugClientPool([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	})
	t.Cleanup(pool.Close)

	storeAddrs := map[uint64]string{1: addr}

	newRegion := func(regionID uint64, startKey, endKey string, leaderStoreID uint64) *pd.Region {
		region := &pd.Region{
			Meta: &metapb.Region{
				Id:       regionID,
				StartKey: []byte(startKey),
				EndKey:   []byte(endKey),
				RegionEpoch: &metapb.RegionEpoch{
					ConfVer: testWaitDataSyncEpochConfVer,
					Version: testWaitDataSyncEpochVersion,
				},
			},
		}
		if leaderStoreID != 0 {
			region.Leader = &metapb.Peer{StoreId: leaderStoreID}
		}
		return region
	}

	// Batch 1 (startKey=nil): keep retrying until the scan result is continuous
	// and each region has a leader store.
	//
	// Batch 2 (startKey="c"): simulate region merge, so the first returned region
	// may start before startKey.
	steps := []batchScanRegionsStep{
		// hole at batch beginning
		{startKey: nil, resp: []*pd.Region{newRegion(101, "a", "b", 1)}},
		// hole at batch middle
		{
			startKey: nil,
			resp: []*pd.Region{
				newRegion(201, "", "a", 1),
				newRegion(202, "b", "c", 1),
				newRegion(203, "c", "d", 1),
			},
		},
		// hole at batch end
		{
			startKey: nil,
			resp: []*pd.Region{
				newRegion(301, "", "a", 1),
				newRegion(302, "a", "b", 1),
				newRegion(303, "c", "d", 1),
			},
		},
		// missing leader store (middle region)
		{
			startKey: nil,
			resp: []*pd.Region{
				newRegion(401, "", "a", 1),
				newRegion(402, "a", "b", 0),
				newRegion(403, "b", "c", 1),
			},
		},
		// consistent batch 1
		{
			startKey: nil,
			resp: []*pd.Region{
				newRegion(501, "", "a", 1),
				newRegion(502, "a", "b", 1),
				newRegion(503, "b", "c", 1),
			},
		},
		// cross-batch gap (hole at the previous batch end) should retry
		{startKey: []byte("c"), resp: []*pd.Region{newRegion(5, "d", "", 1)}},
		// consistent batch 2: scan startKey is merged into the first region
		{
			startKey: []byte("c"),
			resp: []*pd.Region{
				newRegion(604, "b", "d", 1),
				newRegion(605, "d", "", 1),
			},
		},
	}

	scanner := &scriptedBatchScanRegionsClient{
		t:     t,
		steps: steps,
	}

	ctx := context.Background()

	var got [][]regionCommitIndex
	err := fetchRegionCommitIndexInBatch(ctx, scanner, storeAddrs, pool, func(batch []regionCommitIndex) error {
		got = append(got, batch)
		return nil
	})
	require.NoError(t, err)

	// After the first correct scan result, there should be no extra scan calls.
	require.Equal(t, len(steps), scanner.callCount())

	require.Len(t, got, 2)
	require.Len(t, got[0], 3)
	require.Equal(t, uint64(501), got[0][0].region.Id)
	require.Equal(t, uint64(502), got[0][1].region.Id)
	require.Equal(t, uint64(503), got[0][2].region.Id)

	require.Len(t, got[1], 2)
	require.Equal(t, uint64(604), got[1][0].region.Id)
	require.Equal(t, uint64(605), got[1][1].region.Id)
}

func TestFetchRegionCommitIndexInBatchReturnOrder(t *testing.T) {
	store2Returned := make(chan struct{})
	returnOrder := make(chan uint64, 2)
	var closeStore2Returned sync.Once

	// Force store 2 to return earlier than store 1, to ensure this test checks
	// that fetchRegionCommitIndexInBatch preserves the batch order regardless of
	// response timing.
	addr1 := startTestWaitDataSyncDebugServer(t, func(context.Context, *debugpb.RegionInfoRequest) {
		select {
		case <-store2Returned:
		case <-time.After(5 * time.Second):
			t.Errorf("timeout waiting for store2Returned")
		}
		returnOrder <- 1
	})
	addr2 := startTestWaitDataSyncDebugServer(t, func(context.Context, *debugpb.RegionInfoRequest) {
		returnOrder <- 2
		closeStore2Returned.Do(func() { close(store2Returned) })
	})

	pool := newPkdbDebugClientPool([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	})
	t.Cleanup(pool.Close)

	storeAddrs := map[uint64]string{
		1: addr1,
		2: addr2,
	}

	batch := []*pd.Region{
		{
			Meta:   &metapb.Region{Id: 1, StartKey: []byte{}, EndKey: []byte("b"), RegionEpoch: &metapb.RegionEpoch{ConfVer: testWaitDataSyncEpochConfVer, Version: testWaitDataSyncEpochVersion}},
			Leader: &metapb.Peer{StoreId: 1},
		},
		{
			Meta:   &metapb.Region{Id: 2, StartKey: []byte("b"), EndKey: []byte{}, RegionEpoch: &metapb.RegionEpoch{ConfVer: testWaitDataSyncEpochConfVer, Version: testWaitDataSyncEpochVersion}},
			Leader: &metapb.Peer{StoreId: 2},
		},
	}

	scanner := &scriptedBatchScanRegionsClient{
		t: t,
		steps: []batchScanRegionsStep{
			{startKey: nil, resp: batch},
		},
	}

	ctx := context.Background()

	var got [][]regionCommitIndex
	err := fetchRegionCommitIndexInBatch(ctx, scanner, storeAddrs, pool, func(commitIndexes []regionCommitIndex) error {
		got = append(got, commitIndexes)
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, 1, scanner.callCount())
	require.Len(t, got, 1)
	require.Len(t, got[0], 2)
	require.Equal(t, uint64(1), got[0][0].region.Id)
	require.Equal(t, uint64(2), got[0][1].region.Id)

	require.Equal(t, uint64(2), <-returnOrder)
	require.Equal(t, uint64(1), <-returnOrder)
}

func TestFetchRegionCommitIndexInBatchReturnsErrorWhenStoreAddrMissing(t *testing.T) {
	pool := newPkdbDebugClientPool([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	})
	t.Cleanup(pool.Close)

	// storeAddrs intentionally misses store 2.
	storeAddrs := map[uint64]string{1: "127.0.0.1:1234"}

	batch := []*pd.Region{
		{
			Meta: &metapb.Region{
				Id:       1,
				StartKey: []byte{},
				EndKey:   []byte{},
				RegionEpoch: &metapb.RegionEpoch{
					ConfVer: testWaitDataSyncEpochConfVer,
					Version: testWaitDataSyncEpochVersion,
				},
			},
			Leader: &metapb.Peer{StoreId: 2},
		},
	}
	scanner := &scriptedBatchScanRegionsClient{
		t: t,
		steps: []batchScanRegionsStep{
			{startKey: nil, resp: batch},
		},
	}

	var gotErr error
	require.NotPanics(t, func() {
		gotErr = fetchRegionCommitIndexInBatch(context.Background(), scanner, storeAddrs, pool, func([]regionCommitIndex) error {
			t.Fatal("unexpected onBatch call")
			return nil
		})
	})
	require.ErrorContains(t, gotErr, "store 2 not found")
	require.Equal(t, 1, scanner.callCount())
}

func TestFetchRegionCommitIndexInBatchRetriesOnRegionEpochMismatch(t *testing.T) {
	oldRetryWait := tidbWaitDataSyncScanRetryWaitDuration
	tidbWaitDataSyncScanRetryWaitDuration = time.Millisecond
	t.Cleanup(func() { tidbWaitDataSyncScanRetryWaitDuration = oldRetryWait })

	addr := startTestWaitDataSyncDebugServer(t, nil)

	pool := newPkdbDebugClientPool([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	})
	t.Cleanup(pool.Close)

	storeAddrs := map[uint64]string{1: addr}

	newRegion := func(regionID uint64, epochVersion uint64) *pd.Region {
		return &pd.Region{
			Meta: &metapb.Region{
				Id:       regionID,
				StartKey: []byte{},
				EndKey:   []byte{},
				RegionEpoch: &metapb.RegionEpoch{
					ConfVer: testWaitDataSyncEpochConfVer,
					Version: epochVersion,
				},
			},
			Leader: &metapb.Peer{StoreId: 1},
		}
	}

	// Step 1: PD meta has epoch mismatch with store, should retry.
	// Step 2: epoch matches, should succeed and emit one batch.
	steps := []batchScanRegionsStep{
		{startKey: nil, resp: []*pd.Region{newRegion(1, testWaitDataSyncEpochVersion-1)}},
		{startKey: nil, resp: []*pd.Region{newRegion(1, testWaitDataSyncEpochVersion)}},
	}
	scanner := &scriptedBatchScanRegionsClient{
		t:     t,
		steps: steps,
	}

	ctx := context.Background()

	var got [][]regionCommitIndex
	err := fetchRegionCommitIndexInBatch(ctx, scanner, storeAddrs, pool, func(batch []regionCommitIndex) error {
		got = append(got, batch)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, len(steps), scanner.callCount())
	require.Len(t, got, 1)
	require.Len(t, got[0], 1)
	require.Equal(t, uint64(1), got[0][0].region.Id)
}

func TestFetchRegionCommitIndexInBatchRescansOnRegionInfoNotFound(t *testing.T) {
	oldRetryWait := tidbWaitDataSyncScanRetryWaitDuration
	tidbWaitDataSyncScanRetryWaitDuration = time.Millisecond
	t.Cleanup(func() { tidbWaitDataSyncScanRetryWaitDuration = oldRetryWait })

	oldScanBatchSize := tidbWaitDataSyncScanBatchSize
	tidbWaitDataSyncScanBatchSize = 1
	t.Cleanup(func() { tidbWaitDataSyncScanBatchSize = oldScanBatchSize })

	var regionInfoCalls atomic.Int32
	addr := startTestWaitDataSyncDebugServerWithHandler(t, func(_ context.Context, req *debugpb.RegionInfoRequest) (*debugpb.RegionInfoResponse, error) {
		// Simulate a PD leader change / stale leader store ID: querying RegionInfo
		// on the stale store returns gRPC NOT_FOUND (TiKV doesn't have the region's
		// local meta), and waitDataSync should rescan regions from PD instead of
		// retrying the same store forever.
		if regionInfoCalls.Add(1) == 1 {
			return nil, status.Error(codes.NotFound, "info for region")
		}
		return &debugpb.RegionInfoResponse{
			RaftLocalState: &raft_serverpb.RaftLocalState{
				HardState: &eraftpb.HardState{Commit: req.RegionId},
			},
			RegionLocalState: &raft_serverpb.RegionLocalState{
				Region: &metapb.Region{
					Id: req.RegionId,
					RegionEpoch: &metapb.RegionEpoch{
						ConfVer: testWaitDataSyncEpochConfVer,
						Version: testWaitDataSyncEpochVersion,
					},
				},
			},
		}, nil
	})

	pool := newPkdbDebugClientPool([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	})
	t.Cleanup(pool.Close)

	storeAddrs := map[uint64]string{1: addr}

	region := &pd.Region{
		Meta: &metapb.Region{
			Id:       1,
			StartKey: []byte{},
			EndKey:   []byte{},
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: testWaitDataSyncEpochConfVer,
				Version: testWaitDataSyncEpochVersion,
			},
		},
		Leader: &metapb.Peer{StoreId: 1},
	}

	scanner := &scriptedBatchScanRegionsClient{
		t: t,
		steps: []batchScanRegionsStep{
			{startKey: nil, resp: []*pd.Region{region}},
			// One extra scan after NOT_FOUND. To simplify the case, we still return the same
			// region info and let the mock debug server return a success RPC for second try.
			{startKey: nil, resp: []*pd.Region{region}},
		},
	}

	ctx := context.Background()

	var got [][]regionCommitIndex
	err := fetchRegionCommitIndexInBatch(ctx, scanner, storeAddrs, pool, func(batch []regionCommitIndex) error {
		got = append(got, batch)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, scanner.callCount())
	require.Len(t, got, 1)
}

func TestStreamRegionCommitIndexBatchesStopsOnContextCancel(t *testing.T) {
	oldRetryWait := tidbWaitDataSyncScanRetryWaitDuration
	tidbWaitDataSyncScanRetryWaitDuration = time.Hour
	t.Cleanup(func() { tidbWaitDataSyncScanRetryWaitDuration = oldRetryWait })

	cancelErr := errors.New("test cancel")
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(nil) })

	scanner := &testBatchScanRegionsAlwaysErrorClient{
		err:    errors.New("scan error"),
		called: make(chan struct{}, 1),
	}

	done := make(chan error, 1)
	go func() {
		done <- fetchRegionCommitIndexInBatch(ctx, scanner, nil, nil, func([]regionCommitIndex) error {
			t.Fatal("unexpected onBatch call")
			return nil
		})
	}()

	select {
	case <-scanner.called:
	case <-time.After(5 * time.Second):
		t.Fatal("expected BatchScanRegions to be called")
	}
	cancel(cancelErr)
	select {
	case err := <-done:
		require.ErrorIs(t, err, cancelErr)
	case <-time.After(5 * time.Second):
		t.Fatal("fetchRegionCommitIndexInBatch did not return after cancel")
	}
}

type rawKVScanStep struct {
	startKey []byte
	endKey   []byte

	keys   [][]byte
	values [][]byte
	err    error
}

type rawKVReverseScanStep struct {
	startKey []byte
	endKey   []byte

	keys   [][]byte
	values [][]byte
	err    error
}

type scriptedRawKVScanClient struct {
	t *testing.T

	scanSteps        []rawKVScanStep
	reverseScanSteps []rawKVReverseScanStep

	scanCalls        int
	reverseScanCalls int

	gotFirstScanStart []byte
	gotFirstScanEnd   []byte
}

func (c *scriptedRawKVScanClient) Scan(
	_ context.Context,
	startKey, endKey []byte,
	_ int,
	_ ...rawkv.RawOption,
) (keys [][]byte, values [][]byte, err error) {
	c.t.Helper()

	if c.scanCalls == 0 {
		c.gotFirstScanStart = append([]byte{}, startKey...)
		c.gotFirstScanEnd = append([]byte{}, endKey...)
	}

	if c.scanCalls >= len(c.scanSteps) {
		c.t.Fatalf("unexpected extra Scan call, startKey=%q endKey=%q", startKey, endKey)
	}
	step := c.scanSteps[c.scanCalls]
	c.scanCalls++

	require.True(c.t, bytes.Equal(step.startKey, startKey), "unexpected Scan startKey")
	require.True(c.t, bytes.Equal(step.endKey, endKey), "unexpected Scan endKey")
	return step.keys, step.values, step.err
}

func (c *scriptedRawKVScanClient) ReverseScan(
	_ context.Context,
	startKey, endKey []byte,
	_ int,
	_ ...rawkv.RawOption,
) (keys [][]byte, values [][]byte, err error) {
	c.t.Helper()

	if c.reverseScanCalls >= len(c.reverseScanSteps) {
		c.t.Fatalf("unexpected extra ReverseScan call, startKey=%q endKey=%q", startKey, endKey)
	}
	step := c.reverseScanSteps[c.reverseScanCalls]
	c.reverseScanCalls++

	require.True(c.t, bytes.Equal(step.startKey, startKey), "unexpected ReverseScan startKey")
	require.True(c.t, bytes.Equal(step.endKey, endKey), "unexpected ReverseScan endKey")
	return step.keys, step.values, step.err
}

func (c *scriptedRawKVScanClient) assertDone() {
	c.t.Helper()
	require.Equal(c.t, len(c.scanSteps), c.scanCalls, "not all Scan steps consumed")
	require.Equal(c.t, len(c.reverseScanSteps), c.reverseScanCalls, "not all ReverseScan steps consumed")
}

func marshalReplStatesToScanKVs(t *testing.T, states []*logreplicationpb.LogReplicationState) (keys [][]byte, values [][]byte) {
	t.Helper()
	for _, state := range states {
		b, err := state.Marshal()
		require.NoError(t, err)
		keys = append(keys, append([]byte{}, state.StartKey...))
		values = append(values, b)
	}
	return keys, values
}

func push0(key []byte) []byte {
	return append(append([]byte{}, key...), 0)
}

func newRawKVScanClientForRange(
	t *testing.T,
	startKey, endKey []byte,
	states []*logreplicationpb.LogReplicationState,
) *scriptedRawKVScanClient {
	t.Helper()

	keys, values := marshalReplStatesToScanKVs(t, states)
	steps := []rawKVScanStep{
		{
			startKey: startKey,
			endKey:   endKey,
			keys:     keys,
			values:   values,
		},
	}
	if len(keys) > 0 {
		steps = append(steps, rawKVScanStep{
			startKey: push0(keys[len(keys)-1]),
			endKey:   endKey,
		})
	}
	return &scriptedRawKVScanClient{
		t:         t,
		scanSteps: steps,
	}
}

func TestIsSyncedReturnsTrueWhenAllCovered(t *testing.T) {
	regions := []regionCommitIndex{
		newTestRegionCommitIndex(1, "", "b", 1, 10),
		newTestRegionCommitIndex(2, "b", "d", 1, 20),
	}

	states := []*logreplicationpb.LogReplicationState{
		newTestReplState(1, "", "b", 1, 10),
		newTestReplState(2, "b", "d", 1, 20),
	}
	cli := newRawKVScanClientForRange(
		t,
		regions[0].region.StartKey,
		regions[len(regions)-1].region.EndKey,
		states,
	)

	synced, err := isSynced(context.Background(), cli, regions)
	require.NoError(t, err)
	require.True(t, synced)

	// Observability: isSynced only scans the batch key range.
	require.True(t, bytes.Equal(regions[0].region.StartKey, cli.gotFirstScanStart))
	require.True(t, bytes.Equal(regions[len(regions)-1].region.EndKey, cli.gotFirstScanEnd))
	cli.assertDone()
}

func TestIsSyncedReturnsFalseWhenStatesEmpty(t *testing.T) {
	commitIndexes := []regionCommitIndex{
		newTestRegionCommitIndex(1, "", "b", 1, 1),
	}

	cli := newRawKVScanClientForRange(
		t,
		commitIndexes[0].region.StartKey,
		commitIndexes[0].region.EndKey,
		nil,
	)

	synced, err := isSynced(context.Background(), cli, commitIndexes)
	require.NoError(t, err)
	require.False(t, synced)
	cli.assertDone()
}

func TestIsSyncedReverseScan4NonEmptyStartKey(t *testing.T) {
	commitIndexes := []regionCommitIndex{
		newTestRegionCommitIndex(1, "b", "d", 1, 1),
	}

	// The scanned state starts after the batch start key, and there is no
	// previous state covering it. This should be treated as a gap and keep
	// waiting (instead of returning an error).
	scannedStates := []*logreplicationpb.LogReplicationState{
		newTestReplState(1, "c", "d", 1, 1),
	}
	scannedKeys, scannedValues := marshalReplStatesToScanKVs(t, scannedStates)

	cli := &scriptedRawKVScanClient{
		t: t,
		scanSteps: []rawKVScanStep{
			{
				startKey: commitIndexes[0].region.StartKey,
				endKey:   commitIndexes[0].region.EndKey,
				keys:     scannedKeys,
				values:   scannedValues,
			},
			{
				startKey: push0(scannedKeys[len(scannedKeys)-1]),
				endKey:   commitIndexes[0].region.EndKey,
			},
		},
		reverseScanSteps: []rawKVReverseScanStep{
			{
				startKey: commitIndexes[0].region.StartKey,
				endKey:   nil,
			},
		},
	}

	synced, err := isSynced(context.Background(), cli, commitIndexes)
	require.NoError(t, err)
	require.False(t, synced)
	cli.assertDone()

	// a positive test
	reverseScanStates := []*logreplicationpb.LogReplicationState{
		newTestReplState(1, "b", "c", 1, 1),
	}
	reverseScannedKeys, reverseScannedValues := marshalReplStatesToScanKVs(t, reverseScanStates)

	cli2 := &scriptedRawKVScanClient{
		t: t,
		scanSteps: []rawKVScanStep{
			{
				startKey: commitIndexes[0].region.StartKey,
				endKey:   commitIndexes[0].region.EndKey,
				keys:     scannedKeys,
				values:   scannedValues,
			},
			{
				startKey: push0(scannedKeys[len(scannedKeys)-1]),
				endKey:   commitIndexes[0].region.EndKey,
			},
		},
		reverseScanSteps: []rawKVReverseScanStep{
			{
				startKey: commitIndexes[0].region.StartKey,
				endKey:   nil,
				keys:     reverseScannedKeys,
				values:   reverseScannedValues,
			},
		},
	}
	synced, err = isSynced(context.Background(), cli2, commitIndexes)
	require.NoError(t, err)
	require.True(t, synced)
	cli2.assertDone()

	// another positive test
	reverseScanStates = []*logreplicationpb.LogReplicationState{
		newTestReplState(1, "a", "c", 1, 1),
	}
	reverseScannedKeys, reverseScannedValues = marshalReplStatesToScanKVs(t, reverseScanStates)

	cli2 = &scriptedRawKVScanClient{
		t: t,
		scanSteps: []rawKVScanStep{
			{
				startKey: commitIndexes[0].region.StartKey,
				endKey:   commitIndexes[0].region.EndKey,
				keys:     scannedKeys,
				values:   scannedValues,
			},
			{
				startKey: push0(scannedKeys[len(scannedKeys)-1]),
				endKey:   commitIndexes[0].region.EndKey,
			},
		},
		reverseScanSteps: []rawKVReverseScanStep{
			{
				startKey: commitIndexes[0].region.StartKey,
				endKey:   nil,
				keys:     reverseScannedKeys,
				values:   reverseScannedValues,
			},
		},
	}
	synced, err = isSynced(context.Background(), cli2, commitIndexes)
	require.NoError(t, err)
	require.True(t, synced)
	cli2.assertDone()
}

func TestIsSyncedReturnsFalseOnGapAtRegionStart(t *testing.T) {
	commitIndexes := []regionCommitIndex{
		newTestRegionCommitIndex(1, "", "b", 1, 1),
	}

	states := []*logreplicationpb.LogReplicationState{
		newTestReplState(1, "a", "b", 1, 1),
	}
	cli := newRawKVScanClientForRange(t, commitIndexes[0].region.StartKey, commitIndexes[0].region.EndKey, states)

	synced, err := isSynced(context.Background(), cli, commitIndexes)
	require.NoError(t, err)
	require.False(t, synced)
	cli.assertDone()
}

func TestIsSyncedReturnsFalseOnGapWithinRegion(t *testing.T) {
	commitIndexes := []regionCommitIndex{
		newTestRegionCommitIndex(1, "", "c", 1, 1),
	}

	states := []*logreplicationpb.LogReplicationState{
		newTestReplState(1, "", "a", 1, 1),
		newTestReplState(1, "b", "c", 1, 1),
	}
	cli := newRawKVScanClientForRange(t, commitIndexes[0].region.StartKey, commitIndexes[0].region.EndKey, states)

	synced, err := isSynced(context.Background(), cli, commitIndexes)
	require.NoError(t, err)
	require.False(t, synced)
	cli.assertDone()
}

func TestIsSyncedReturnsFalseWhenAppliedIndexNotEnough(t *testing.T) {
	commitIndexes := []regionCommitIndex{
		newTestRegionCommitIndex(1, "", "b", 1, 10),
	}

	states := []*logreplicationpb.LogReplicationState{
		newTestReplState(1, "", "b", 1, 9),
	}
	cli := newRawKVScanClientForRange(t, commitIndexes[0].region.StartKey, commitIndexes[0].region.EndKey, states)

	synced, err := isSynced(context.Background(), cli, commitIndexes)
	require.NoError(t, err)
	require.False(t, synced)
	cli.assertDone()
}

func TestIsSyncedReturnsFalseWhenEpochVersionNotEnough(t *testing.T) {
	commitIndexes := []regionCommitIndex{
		newTestRegionCommitIndex(1, "", "b", 2, 10),
	}

	states := []*logreplicationpb.LogReplicationState{
		newTestReplState(1, "", "b", 1, 100),
	}
	cli := newRawKVScanClientForRange(t, commitIndexes[0].region.StartKey, commitIndexes[0].region.EndKey, states)

	synced, err := isSynced(context.Background(), cli, commitIndexes)
	require.NoError(t, err)
	require.False(t, synced)
	cli.assertDone()
}

func TestIsSyncedHandlesRegionKeyRangeChangeAfterSplitMerge(t *testing.T) {
	testCases := []struct {
		name       string
		regions    []regionCommitIndex
		states     []*logreplicationpb.LogReplicationState
		wantSynced bool
	}{
		{
			name: "split_true",
			regions: []regionCommitIndex{
				newTestRegionCommitIndex(1, "a", "d", 1, 10),
			},
			states: []*logreplicationpb.LogReplicationState{
				newTestReplState(10, "a", "b", 2, 11),
				newTestReplState(11, "b", "c", 2, 11),
				newTestReplState(12, "c", "d", 2, 11),
			},
			wantSynced: true,
		},
		{
			name: "split_false",
			regions: []regionCommitIndex{
				newTestRegionCommitIndex(1, "a", "b", 2, 10),
				newTestRegionCommitIndex(1, "b", "d", 2, 13),
			},
			states: []*logreplicationpb.LogReplicationState{
				newTestReplState(10, "a", "d", 1, 9),
			},
			wantSynced: false,
		},
		{
			name: "merge_true",
			regions: []regionCommitIndex{
				newTestRegionCommitIndex(1, "a", "b", 1, 10),
				newTestRegionCommitIndex(2, "b", "d", 1, 20),
			},
			states: []*logreplicationpb.LogReplicationState{
				newTestReplState(99, "a", "d", 4, 2),
			},
			wantSynced: true,
		},
		{
			name: "merge_false",
			regions: []regionCommitIndex{
				newTestRegionCommitIndex(1, "a", "d", 10, 10),
			},
			states: []*logreplicationpb.LogReplicationState{
				newTestReplState(99, "a", "b", 1, 15),
				newTestReplState(99, "b", "c", 3, 15),
				newTestReplState(99, "c", "d", 5, 15),
			},
			wantSynced: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cli := newRawKVScanClientForRange(
				t,
				tc.regions[0].region.StartKey,
				tc.regions[len(tc.regions)-1].region.EndKey,
				tc.states,
			)

			synced, err := isSynced(context.Background(), cli, tc.regions)
			require.NoError(t, err)
			require.Equal(t, tc.wantSynced, synced)
			cli.assertDone()
		})
	}
}

func TestScanReplStatesInRangeScansAllPagesAndAdvancesStartKey(t *testing.T) {
	startKey := []byte("a")
	endKey := []byte("d")

	statesPage1 := []*logreplicationpb.LogReplicationState{
		newTestReplState(1, "a", "b", 1, 1),
	}
	statesPage2 := []*logreplicationpb.LogReplicationState{
		newTestReplState(2, "b", "c", 1, 1),
	}

	keys1, values1 := marshalReplStatesToScanKVs(t, statesPage1)
	keys2, values2 := marshalReplStatesToScanKVs(t, statesPage2)
	cli := &scriptedRawKVScanClient{
		t: t,
		scanSteps: []rawKVScanStep{
			{
				startKey: startKey,
				endKey:   endKey,
				keys:     keys1,
				values:   values1,
			},
			{
				startKey: push0(keys1[len(keys1)-1]),
				endKey:   endKey,
				keys:     keys2,
				values:   values2,
			},
			{
				startKey: push0(keys2[len(keys2)-1]),
				endKey:   endKey,
			},
		},
	}

	got, err := scanReplStatesInRange(context.Background(), cli, startKey, endKey)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.True(t, bytes.Equal(got[0].StartKey, []byte("a")))
	require.True(t, bytes.Equal(got[1].StartKey, []byte("b")))
	cli.assertDone()
}

func TestScanReplStatesInRangePrependsPrevStateWhenItCoversStartKey(t *testing.T) {
	startKey := []byte("b")
	endKey := []byte("d")

	scanned := []*logreplicationpb.LogReplicationState{
		newTestReplState(2, "c", "d", 1, 1),
	}
	scannedKeys, scannedValues := marshalReplStatesToScanKVs(t, scanned)

	prev := []*logreplicationpb.LogReplicationState{
		newTestReplState(1, "a", "c", 1, 1), // covers startKey "b"
	}
	prevKeys, prevValues := marshalReplStatesToScanKVs(t, prev)

	cli := &scriptedRawKVScanClient{
		t: t,
		scanSteps: []rawKVScanStep{
			{
				startKey: startKey,
				endKey:   endKey,
				keys:     scannedKeys,
				values:   scannedValues,
			},
			{
				startKey: push0(scannedKeys[len(scannedKeys)-1]),
				endKey:   endKey,
			},
		},
		reverseScanSteps: []rawKVReverseScanStep{
			{
				startKey: startKey,
				endKey:   nil,
				keys:     prevKeys,
				values:   prevValues,
			},
		},
	}

	got, err := scanReplStatesInRange(context.Background(), cli, startKey, endKey)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.True(t, bytes.Equal(got[0].StartKey, []byte("a")))
	require.True(t, bytes.Equal(got[1].StartKey, []byte("c")))
	cli.assertDone()
}

type testBatchScanRegionsAlwaysErrorClient struct {
	err error

	mu     sync.Mutex
	called chan struct{}
}

func (c *testBatchScanRegionsAlwaysErrorClient) BatchScanRegions(
	_ context.Context,
	_ []pd.KeyRange,
	_ int,
	_ ...pd.GetRegionOption,
) ([]*pd.Region, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case c.called <- struct{}{}:
	default:
	}
	return nil, c.err
}

type batchScanRegionsStep struct {
	startKey []byte
	resp     []*pd.Region
	err      error
}

type scriptedBatchScanRegionsClient struct {
	t *testing.T

	mu    sync.Mutex
	calls int

	steps []batchScanRegionsStep
}

func (c *scriptedBatchScanRegionsClient) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *scriptedBatchScanRegionsClient) BatchScanRegions(
	_ context.Context,
	ranges []pd.KeyRange,
	limit int,
	opts ...pd.GetRegionOption,
) ([]*pd.Region, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	require.Len(c.t, opts, 0)
	require.Len(c.t, ranges, 1)
	require.Equal(c.t, tidbWaitDataSyncScanBatchSize, limit)

	if c.calls >= len(c.steps) {
		c.t.Fatalf("unexpected extra BatchScanRegions call, startKey=%q", ranges[0].StartKey)
	}

	step := c.steps[c.calls]
	c.calls++
	require.True(c.t, bytes.Equal(ranges[0].StartKey, step.startKey))
	return step.resp, step.err
}

func startTestWaitDataSyncDebugServer(
	t *testing.T,
	regionInfoHook func(context.Context, *debugpb.RegionInfoRequest),
) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	debugpb.RegisterDebugServer(server, &testWaitDataSyncDebugServer{
		regionInfoHook: regionInfoHook,
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

func startTestWaitDataSyncDebugServerWithHandler(
	t *testing.T,
	regionInfoHandler func(context.Context, *debugpb.RegionInfoRequest) (*debugpb.RegionInfoResponse, error),
) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	debugpb.RegisterDebugServer(server, &testWaitDataSyncDebugServer{
		regionInfoHandler: regionInfoHandler,
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

type testWaitDataSyncDebugServer struct {
	debugpb.UnimplementedDebugServer

	regionInfoHook    func(context.Context, *debugpb.RegionInfoRequest)
	regionInfoHandler func(context.Context, *debugpb.RegionInfoRequest) (*debugpb.RegionInfoResponse, error)
}

func (s *testWaitDataSyncDebugServer) RegionInfo(ctx context.Context, req *debugpb.RegionInfoRequest) (*debugpb.RegionInfoResponse, error) {
	if s.regionInfoHook != nil {
		s.regionInfoHook(ctx, req)
	}
	if s.regionInfoHandler != nil {
		return s.regionInfoHandler(ctx, req)
	}
	return &debugpb.RegionInfoResponse{
		RaftLocalState: &raft_serverpb.RaftLocalState{
			HardState: &eraftpb.HardState{Commit: req.RegionId},
		},
		RegionLocalState: &raft_serverpb.RegionLocalState{
			Region: &metapb.Region{
				Id: req.RegionId,
				RegionEpoch: &metapb.RegionEpoch{
					ConfVer: testWaitDataSyncEpochConfVer,
					Version: testWaitDataSyncEpochVersion,
				},
			},
		},
	}, nil
}

func newTestRegionCommitIndex(
	regionID uint64,
	startKey, endKey string,
	epochVersion uint64,
	commitIndex uint64,
) regionCommitIndex {
	return regionCommitIndex{
		region: &metapb.Region{
			Id:       regionID,
			StartKey: []byte(startKey),
			EndKey:   []byte(endKey),
			RegionEpoch: &metapb.RegionEpoch{
				Version: epochVersion,
			},
		},
		commitIndex: commitIndex,
	}
}

func newTestReplState(
	regionID uint64,
	startKey, endKey string,
	epochVersion uint64,
	appliedIndex uint64,
) *logreplicationpb.LogReplicationState {
	return &logreplicationpb.LogReplicationState{
		Region: &metapb.Region{
			Id:       regionID,
			StartKey: []byte(startKey),
			EndKey:   []byte(endKey),
			RegionEpoch: &metapb.RegionEpoch{
				Version: epochVersion,
			},
		},
		StartKey:     []byte(startKey),
		EndKey:       []byte(endKey),
		AppliedIndex: appliedIndex,
	}
}
