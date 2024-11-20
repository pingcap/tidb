// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.
package split

import (
	"bytes"
	"context"
	goerrors "errors"
	"fmt"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/pdtypes"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestScanRegionBackOfferWithSuccess(t *testing.T) {
	var counter int
	bo := NewWaitRegionOnlineBackoffer()

	err := utils.WithRetry(context.Background(), func() error {
		defer func() {
			counter++
		}()

		if counter == 3 {
			return nil
		}
		return berrors.ErrPDBatchScanRegion
	}, bo)
	require.NoError(t, err)
	require.Equal(t, counter, 4)
}

func TestScanRegionBackOfferWithFail(t *testing.T) {
	_ = failpoint.Enable("github.com/pingcap/tidb/br/pkg/restore/split/hint-scan-region-backoff", "return(true)")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/restore/split/hint-scan-region-backoff")
	}()

	var counter int
	bo := NewWaitRegionOnlineBackoffer()

	err := utils.WithRetry(context.Background(), func() error {
		defer func() {
			counter++
		}()
		return berrors.ErrPDBatchScanRegion
	}, bo)
	require.Error(t, err)
	require.Equal(t, counter, WaitRegionOnlineAttemptTimes)
}

func TestScanRegionBackOfferWithStopRetry(t *testing.T) {
	_ = failpoint.Enable("github.com/pingcap/tidb/br/pkg/restore/split/hint-scan-region-backoff", "return(true)")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/restore/split/hint-scan-region-backoff")
	}()

	var counter int
	bo := NewWaitRegionOnlineBackoffer()

	err := utils.WithRetry(context.Background(), func() error {
		defer func() {
			counter++
		}()

		if counter < 5 {
			return berrors.ErrPDBatchScanRegion
		}
		return berrors.ErrKVUnknown
	}, bo)
	require.Error(t, err)
	require.Equal(t, counter, 6)
}

type recordCntBackoffer struct {
	already int
}

func (b *recordCntBackoffer) NextBackoff(error) time.Duration {
	b.already++
	return 0
}

func (b *recordCntBackoffer) Attempt() int {
	return 100
}

func TestScatterSequentiallyRetryCnt(t *testing.T) {
	mockClient := NewMockPDClientForSplit()
	mockClient.scatterRegion.eachRegionFailBefore = 7
	client := pdClient{
		needScatterVal: true,
		client:         mockClient,
	}
	client.needScatterInit.Do(func() {})

	ctx := context.Background()
	regions := []*RegionInfo{
		{
			Region: &metapb.Region{
				Id: 1,
			},
		},
		{
			Region: &metapb.Region{
				Id: 2,
			},
		},
	}
	backoffer := &recordCntBackoffer{}
	client.scatterRegionsSequentially(
		ctx,
		regions,
		backoffer,
	)
	require.Equal(t, 7, backoffer.already)
}

func TestScatterBackwardCompatibility(t *testing.T) {
	mockClient := NewMockPDClientForSplit()
	mockClient.scatterRegions.notImplemented = true
	client := pdClient{
		needScatterVal: true,
		client:         mockClient,
	}
	client.needScatterInit.Do(func() {})

	ctx := context.Background()
	regions := []*RegionInfo{
		{
			Region: &metapb.Region{
				Id: 1,
			},
		},
		{
			Region: &metapb.Region{
				Id: 2,
			},
		},
	}
	err := client.scatterRegions(ctx, regions)
	require.NoError(t, err)
	require.Equal(t, map[uint64]int{1: 1, 2: 1}, client.client.(*MockPDClientForSplit).scatterRegion.count)
}

func TestWaitForScatterRegions(t *testing.T) {
	mockPDCli := NewMockPDClientForSplit()
	mockPDCli.scatterRegions.notImplemented = true
	client := pdClient{
		needScatterVal: true,
		client:         mockPDCli,
	}
	client.needScatterInit.Do(func() {})
	regionCnt := 6
	checkGetOperatorRespsDrained := func() {
		for i := 1; i <= regionCnt; i++ {
			require.Len(t, mockPDCli.getOperator.responses[uint64(i)], 0)
		}
	}
	checkNoRetry := func() {
		for i := 1; i <= regionCnt; i++ {
			require.Equal(t, 0, mockPDCli.scatterRegion.count[uint64(i)])
		}
	}

	ctx := context.Background()
	regions := make([]*RegionInfo, 0, regionCnt)
	for i := 1; i <= regionCnt; i++ {
		regions = append(regions, &RegionInfo{
			Region: &metapb.Region{
				Id: uint64(i),
			},
		})
	}

	mockPDCli.getOperator.responses = make(map[uint64][]*pdpb.GetOperatorResponse)
	mockPDCli.getOperator.responses[1] = []*pdpb.GetOperatorResponse{
		{Header: &pdpb.ResponseHeader{Error: &pdpb.Error{Type: pdpb.ErrorType_REGION_NOT_FOUND}}},
	}
	mockPDCli.getOperator.responses[2] = []*pdpb.GetOperatorResponse{
		{Desc: []byte("not-scatter-region")},
	}
	mockPDCli.getOperator.responses[3] = []*pdpb.GetOperatorResponse{
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_SUCCESS},
	}
	mockPDCli.getOperator.responses[4] = []*pdpb.GetOperatorResponse{
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_RUNNING},
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_TIMEOUT},
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_SUCCESS},
	}
	mockPDCli.getOperator.responses[5] = []*pdpb.GetOperatorResponse{
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_CANCEL},
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_CANCEL},
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_CANCEL},
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_RUNNING},
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_RUNNING},
		{Desc: []byte("not-scatter-region")},
	}
	// should trigger a retry
	mockPDCli.getOperator.responses[6] = []*pdpb.GetOperatorResponse{
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_REPLACE},
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_SUCCESS},
	}

	left, err := client.WaitRegionsScattered(ctx, regions)
	require.NoError(t, err)
	require.Equal(t, 0, left)
	for i := 1; i <= 3; i++ {
		require.Equal(t, 0, mockPDCli.scatterRegion.count[uint64(i)])
	}
	// OperatorStatus_TIMEOUT should trigger rescatter once
	require.Equal(t, 1, mockPDCli.scatterRegion.count[uint64(4)])
	// 3 * OperatorStatus_CANCEL should trigger 3 * rescatter
	require.Equal(t, 3, mockPDCli.scatterRegion.count[uint64(5)])
	// OperatorStatus_REPLACE should trigger rescatter once
	require.Equal(t, 1, mockPDCli.scatterRegion.count[uint64(6)])
	checkGetOperatorRespsDrained()

	// test non-retryable error

	mockPDCli.scatterRegion.count = make(map[uint64]int)
	mockPDCli.getOperator.responses = make(map[uint64][]*pdpb.GetOperatorResponse)
	mockPDCli.getOperator.responses[1] = []*pdpb.GetOperatorResponse{
		{Header: &pdpb.ResponseHeader{Error: &pdpb.Error{Type: pdpb.ErrorType_REGION_NOT_FOUND}}},
	}
	mockPDCli.getOperator.responses[2] = []*pdpb.GetOperatorResponse{
		{Desc: []byte("not-scatter-region")},
	}
	// mimic non-retryable error
	mockPDCli.getOperator.responses[3] = []*pdpb.GetOperatorResponse{
		{Header: &pdpb.ResponseHeader{Error: &pdpb.Error{Type: pdpb.ErrorType_DATA_COMPACTED}}},
	}
	left, err = client.WaitRegionsScattered(ctx, regions)
	require.ErrorContains(t, err, "get operator error: DATA_COMPACTED")
	require.Equal(t, 4, left) // region 3,4,5,6 is not scattered
	checkGetOperatorRespsDrained()
	checkNoRetry()

	// test backoff is timed-out

	backup := WaitRegionOnlineAttemptTimes
	WaitRegionOnlineAttemptTimes = 2
	t.Cleanup(func() {
		WaitRegionOnlineAttemptTimes = backup
	})

	mockPDCli.scatterRegion.count = make(map[uint64]int)
	mockPDCli.getOperator.responses = make(map[uint64][]*pdpb.GetOperatorResponse)
	mockPDCli.getOperator.responses[1] = []*pdpb.GetOperatorResponse{
		{Header: &pdpb.ResponseHeader{Error: &pdpb.Error{Type: pdpb.ErrorType_REGION_NOT_FOUND}}},
	}
	mockPDCli.getOperator.responses[2] = []*pdpb.GetOperatorResponse{
		{Desc: []byte("not-scatter-region")},
	}
	mockPDCli.getOperator.responses[3] = []*pdpb.GetOperatorResponse{
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_SUCCESS},
	}
	mockPDCli.getOperator.responses[4] = []*pdpb.GetOperatorResponse{
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_RUNNING},
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_RUNNING}, // first retry
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_RUNNING}, // second retry
	}
	mockPDCli.getOperator.responses[5] = []*pdpb.GetOperatorResponse{
		{Desc: []byte("not-scatter-region")},
	}
	mockPDCli.getOperator.responses[6] = []*pdpb.GetOperatorResponse{
		{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_SUCCESS},
	}
	left, err = client.WaitRegionsScattered(ctx, regions)
	require.ErrorContains(t, err, "the first unfinished region: id:4")
	require.Equal(t, 1, left)
	checkGetOperatorRespsDrained()
	checkNoRetry()
}

func TestBackoffMayNotCountBackoffer(t *testing.T) {
	b := NewBackoffMayNotCountBackoffer()
	initVal := b.Attempt()

	b.NextBackoff(ErrBackoffAndDontCount)
	require.Equal(t, initVal, b.Attempt())
	// test Annotate, which is the real usage in caller
	b.NextBackoff(errors.Annotate(ErrBackoffAndDontCount, "caller message"))
	require.Equal(t, initVal, b.Attempt())

	b.NextBackoff(ErrBackoff)
	require.Equal(t, initVal-1, b.Attempt())

	b.NextBackoff(goerrors.New("test"))
	require.Equal(t, 0, b.Attempt())
}

func TestSplitCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockCli := NewMockPDClientForSplit()
	mockCli.splitRegions.hijacked = func() (bool, *kvrpcpb.SplitRegionResponse, error) {
		cancel()
		resp := &kvrpcpb.SplitRegionResponse{
			Regions: []*metapb.Region{
				{Id: 1},
				{Id: 2},
			},
		}
		return false, resp, nil
	}
	client := pdClient{
		client: mockCli,
	}

	_, err := client.SplitWaitAndScatter(ctx, &RegionInfo{}, [][]byte{{1}})
	require.ErrorIs(t, err, context.Canceled)
}

func TestGetSplitKeyPerRegion(t *testing.T) {
	// test case moved from BR
	sortedKeys := [][]byte{
		[]byte("b"),
		[]byte("d"),
		[]byte("g"),
		[]byte("j"),
		[]byte("l"),
		[]byte("m"),
	}
	sortedRegions := []*RegionInfo{
		{
			Region: &metapb.Region{
				Id:       1,
				StartKey: []byte("a"),
				EndKey:   []byte("g"),
			},
		},
		{
			Region: &metapb.Region{
				Id:       2,
				StartKey: []byte("g"),
				EndKey:   []byte("k"),
			},
		},
		{
			Region: &metapb.Region{
				Id:       3,
				StartKey: []byte("k"),
				EndKey:   []byte("m"),
			},
		},
	}
	result := getSplitKeysOfRegions(sortedKeys, sortedRegions, false)
	require.Equal(t, 3, len(result))
	require.Equal(t, [][]byte{[]byte("b"), []byte("d")}, result[sortedRegions[0]])
	require.Equal(t, [][]byte{[]byte("g"), []byte("j")}, result[sortedRegions[1]])
	require.Equal(t, [][]byte{[]byte("l")}, result[sortedRegions[2]])

	// test case moved from lightning
	tableID := int64(1)
	keys := []int64{1, 10, 100, 1000, 10000, -1}
	sortedRegions = make([]*RegionInfo, 0, len(keys))
	start := tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(0))
	regionStart := codec.EncodeBytes([]byte{}, start)
	for i, end := range keys {
		var regionEndKey []byte
		if end >= 0 {
			endKey := tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(end))
			regionEndKey = codec.EncodeBytes([]byte{}, endKey)
		}
		region := &RegionInfo{
			Region: &metapb.Region{
				Id:       uint64(i),
				StartKey: regionStart,
				EndKey:   regionEndKey,
			},
		}
		sortedRegions = append(sortedRegions, region)
		regionStart = regionEndKey
	}

	checkKeys := map[int64]int{
		0:     -1,
		5:     1,
		6:     1,
		7:     1,
		50:    2,
		60:    2,
		70:    2,
		100:   -1,
		50000: 5,
	}
	expected := map[uint64][][]byte{}
	sortedKeys = make([][]byte, 0, len(checkKeys))

	for hdl, idx := range checkKeys {
		key := tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(hdl))
		sortedKeys = append(sortedKeys, key)
		if idx < 0 {
			continue
		}
		expected[uint64(idx)] = append(expected[uint64(idx)], key)
	}

	slices.SortFunc(sortedKeys, bytes.Compare)
	for i := range expected {
		slices.SortFunc(expected[i], bytes.Compare)
	}

	got := getSplitKeysOfRegions(sortedKeys, sortedRegions, false)
	require.Equal(t, len(expected), len(got))
	for region, gotKeys := range got {
		require.Equal(t, expected[region.Region.GetId()], gotKeys)
	}
}

func checkRegionsBoundaries(t *testing.T, regions []*RegionInfo, expected [][]byte) {
	require.Len(
		t, regions, len(expected)-1,
		"first region start key: %v, last region end key: %v, first expected key: %v, last expected key: %v",
		regions[0].Region.StartKey, regions[len(regions)-1].Region.EndKey,
		expected[0], expected[len(expected)-1],
	)
	for i := 1; i < len(expected); i++ {
		require.Equal(t, expected[i-1], regions[i-1].Region.StartKey)
		require.Equal(t, expected[i], regions[i-1].Region.EndKey)
	}
}

func TestPaginateScanRegion(t *testing.T) {
	ctx := context.Background()
	mockPDClient := NewMockPDClientForSplit()
	mockClient := &pdClient{
		client: mockPDClient,
	}

	backup := WaitRegionOnlineAttemptTimes
	WaitRegionOnlineAttemptTimes = 3
	t.Cleanup(func() {
		WaitRegionOnlineAttemptTimes = backup
	})

	// no region
	_, err := PaginateScanRegion(ctx, mockClient, []byte{}, []byte{}, 3)
	require.Error(t, err)
	require.True(t, berrors.ErrPDBatchScanRegion.Equal(err))
	require.ErrorContains(t, err, "scan region return empty result")

	// retry on error
	mockPDClient.scanRegions.errors = []error{
		status.Error(codes.Unavailable, "not leader"),
	}
	mockPDClient.SetRegions([][]byte{{}, {}})
	got, err := PaginateScanRegion(ctx, mockClient, []byte{}, []byte{}, 3)
	require.NoError(t, err)
	checkRegionsBoundaries(t, got, [][]byte{{}, {}})

	// test paginate
	boundaries := [][]byte{{}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {}}
	mockPDClient.SetRegions(boundaries)
	got, err = PaginateScanRegion(ctx, mockClient, []byte{}, []byte{}, 3)
	require.NoError(t, err)
	checkRegionsBoundaries(t, got, boundaries)
	got, err = PaginateScanRegion(ctx, mockClient, []byte{1}, []byte{}, 3)
	require.NoError(t, err)
	checkRegionsBoundaries(t, got, boundaries[1:])
	got, err = PaginateScanRegion(ctx, mockClient, []byte{}, []byte{2}, 8)
	require.NoError(t, err)
	checkRegionsBoundaries(t, got, boundaries[:3]) // [, 1), [1, 2)
	got, err = PaginateScanRegion(ctx, mockClient, []byte{4}, []byte{5}, 1)
	require.NoError(t, err)
	checkRegionsBoundaries(t, got, [][]byte{{4}, {5}})

	// test start == end
	_, err = PaginateScanRegion(ctx, mockClient, []byte{4}, []byte{4}, 1)
	require.ErrorContains(t, err, "scan region return empty result")

	// test start > end
	_, err = PaginateScanRegion(ctx, mockClient, []byte{5}, []byte{4}, 5)
	require.True(t, berrors.ErrInvalidRange.Equal(err))
	require.ErrorContains(t, err, "startKey > endKey")

	// test retry exhausted
	mockPDClient.scanRegions.errors = []error{
		status.Error(codes.Unavailable, "not leader"),
		status.Error(codes.Unavailable, "not leader"),
		status.Error(codes.Unavailable, "not leader"),
	}
	_, err = PaginateScanRegion(ctx, mockClient, []byte{4}, []byte{5}, 1)
	require.ErrorContains(t, err, "not leader")

	// test region not continuous
	mockPDClient.Regions = &pdtypes.RegionTree{}
	mockPDClient.Regions.SetRegion(&pdtypes.Region{
		Meta: &metapb.Region{
			Id:       1,
			StartKey: []byte{1},
			EndKey:   []byte{2},
		},
		Leader: &metapb.Peer{
			Id:      1,
			StoreId: 1,
		},
	})
	mockPDClient.Regions.SetRegion(&pdtypes.Region{
		Meta: &metapb.Region{
			Id:       4,
			StartKey: []byte{4},
			EndKey:   []byte{5},
		},
		Leader: &metapb.Peer{
			Id:      4,
			StoreId: 1,
		},
	})

	_, err = PaginateScanRegion(ctx, mockClient, []byte{1}, []byte{5}, 3)
	require.True(t, berrors.ErrPDBatchScanRegion.Equal(err))
	require.ErrorContains(t, err, "region 1's endKey not equal to next region 4's startKey")

	// test region becomes continuous slowly
	toAdd := []*pdtypes.Region{
		{
			Meta: &metapb.Region{
				Id:       2,
				StartKey: []byte{2},
				EndKey:   []byte{3},
			},
			Leader: &metapb.Peer{
				Id:      2,
				StoreId: 1,
			},
		},
		{
			Meta: &metapb.Region{
				Id:       3,
				StartKey: []byte{3},
				EndKey:   []byte{4},
			},
			Leader: &metapb.Peer{
				Id:      3,
				StoreId: 1,
			},
		},
	}
	mockPDClient.scanRegions.beforeHook = func() {
		mockPDClient.Regions.SetRegion(toAdd[0])
		toAdd = toAdd[1:]
	}
	got, err = PaginateScanRegion(ctx, mockClient, []byte{1}, []byte{5}, 100)
	require.NoError(t, err)
	checkRegionsBoundaries(t, got, [][]byte{{1}, {2}, {3}, {4}, {5}})
}

func TestRegionConsistency(t *testing.T) {
	cases := []struct {
		startKey []byte
		endKey   []byte
		err      string
		regions  []*RegionInfo
	}{
		{
			codec.EncodeBytes([]byte{}, []byte("a")),
			codec.EncodeBytes([]byte{}, []byte("a")),
			"scan region return empty result, startKey: (.*?), endKey: (.*?)",
			[]*RegionInfo{},
		},
		{
			codec.EncodeBytes([]byte{}, []byte("a")),
			codec.EncodeBytes([]byte{}, []byte("a")),
			"first region 1's startKey(.*?) > startKey(.*?)",
			[]*RegionInfo{
				{
					Region: &metapb.Region{
						Id:       1,
						StartKey: codec.EncodeBytes([]byte{}, []byte("b")),
						EndKey:   codec.EncodeBytes([]byte{}, []byte("d")),
					},
				},
			},
		},
		{
			codec.EncodeBytes([]byte{}, []byte("b")),
			codec.EncodeBytes([]byte{}, []byte("e")),
			"last region 100's endKey(.*?) < endKey(.*?)",
			[]*RegionInfo{
				{
					Region: &metapb.Region{
						Id:       100,
						StartKey: codec.EncodeBytes([]byte{}, []byte("b")),
						EndKey:   codec.EncodeBytes([]byte{}, []byte("d")),
					},
				},
			},
		},
		{
			codec.EncodeBytes([]byte{}, []byte("c")),
			codec.EncodeBytes([]byte{}, []byte("e")),
			"region 6's endKey not equal to next region 8's startKey(.*?)",
			[]*RegionInfo{
				{
					Leader: &metapb.Peer{
						Id:      6,
						StoreId: 1,
					},
					Region: &metapb.Region{
						Id:          6,
						StartKey:    codec.EncodeBytes([]byte{}, []byte("b")),
						EndKey:      codec.EncodeBytes([]byte{}, []byte("d")),
						RegionEpoch: nil,
					},
				},
				{
					Leader: &metapb.Peer{
						Id:      8,
						StoreId: 1,
					},
					Region: &metapb.Region{
						Id:       8,
						StartKey: codec.EncodeBytes([]byte{}, []byte("e")),
						EndKey:   codec.EncodeBytes([]byte{}, []byte("f")),
					},
				},
			},
		},
		{
			codec.EncodeBytes([]byte{}, []byte("c")),
			codec.EncodeBytes([]byte{}, []byte("e")),
			"region 6's leader is nil(.*?)",
			[]*RegionInfo{
				{
					Region: &metapb.Region{
						Id:          6,
						StartKey:    codec.EncodeBytes([]byte{}, []byte("c")),
						EndKey:      codec.EncodeBytes([]byte{}, []byte("d")),
						RegionEpoch: nil,
					},
				},
				{
					Region: &metapb.Region{
						Id:       8,
						StartKey: codec.EncodeBytes([]byte{}, []byte("d")),
						EndKey:   codec.EncodeBytes([]byte{}, []byte("e")),
					},
				},
			},
		},
		{
			codec.EncodeBytes([]byte{}, []byte("c")),
			codec.EncodeBytes([]byte{}, []byte("e")),
			"region 6's leader's store id is 0(.*?)",
			[]*RegionInfo{
				{
					Leader: &metapb.Peer{
						Id:      6,
						StoreId: 0,
					},
					Region: &metapb.Region{
						Id:          6,
						StartKey:    codec.EncodeBytes([]byte{}, []byte("c")),
						EndKey:      codec.EncodeBytes([]byte{}, []byte("d")),
						RegionEpoch: nil,
					},
				},
				{
					Leader: &metapb.Peer{
						Id:      6,
						StoreId: 0,
					},
					Region: &metapb.Region{
						Id:       8,
						StartKey: codec.EncodeBytes([]byte{}, []byte("d")),
						EndKey:   codec.EncodeBytes([]byte{}, []byte("e")),
					},
				},
			},
		},
	}
	for _, ca := range cases {
		err := checkRegionConsistency(ca.startKey, ca.endKey, ca.regions)
		require.Error(t, err)
		require.Regexp(t, ca.err, err.Error())
	}
}

func regionInfo(startKey, endKey string) *RegionInfo {
	return &RegionInfo{
		Region: &metapb.Region{
			StartKey: []byte(startKey),
			EndKey:   []byte(endKey),
		},
	}
}

func TestSplitCheckPartRegionConsistency(t *testing.T) {
	var (
		startKey []byte = []byte("a")
		endKey   []byte = []byte("f")
		err      error
	)
	err = checkPartRegionConsistency(startKey, endKey, nil)
	require.Error(t, err)
	err = checkPartRegionConsistency(startKey, endKey, []*RegionInfo{
		regionInfo("b", "c"),
	})
	require.Error(t, err)
	err = checkPartRegionConsistency(startKey, endKey, []*RegionInfo{
		regionInfo("a", "c"),
		regionInfo("d", "e"),
	})
	require.Error(t, err)
	err = checkPartRegionConsistency(startKey, endKey, []*RegionInfo{
		regionInfo("a", "c"),
		regionInfo("c", "d"),
	})
	require.NoError(t, err)
	err = checkPartRegionConsistency(startKey, endKey, []*RegionInfo{
		regionInfo("a", "c"),
		regionInfo("c", "d"),
		regionInfo("d", "f"),
	})
	require.NoError(t, err)
	err = checkPartRegionConsistency(startKey, endKey, []*RegionInfo{
		regionInfo("a", "c"),
		regionInfo("c", "z"),
	})
	require.NoError(t, err)
}

func TestScanRegionsWithRetry(t *testing.T) {
	ctx := context.Background()
	mockPDClient := NewMockPDClientForSplit()
	mockClient := &pdClient{
		client: mockPDClient,
	}

	{
		_, err := ScanRegionsWithRetry(ctx, mockClient, []byte("1"), []byte("0"), 0)
		require.Error(t, err)
	}

	{
		mockPDClient.SetRegions([][]byte{{}, []byte("1"), []byte("2"), []byte("3"), []byte("4"), {}})
		regions, err := ScanRegionsWithRetry(ctx, mockClient, []byte("1"), []byte("3"), 0)
		require.NoError(t, err)
		require.Len(t, regions, 2)
		require.Equal(t, []byte("1"), regions[0].Region.StartKey)
		require.Equal(t, []byte("2"), regions[1].Region.StartKey)
	}
}

func TestScanEmptyRegion(t *testing.T) {
	mockPDCli := NewMockPDClientForSplit()
	mockPDCli.SetRegions([][]byte{{}, {12}, {34}, {}})
	client := NewClient(mockPDCli, nil, nil, 100, 4)
	keys := initKeys()
	// make keys has only one
	keys = keys[0:1]
	regionSplitter := NewRegionSplitter(client)

	ctx := context.Background()
	err := regionSplitter.ExecuteSortedKeys(ctx, keys)
	// should not return error with only one range entry
	require.NoError(t, err)
}

func TestSplitEmptyRegion(t *testing.T) {
	mockPDCli := NewMockPDClientForSplit()
	mockPDCli.SetRegions([][]byte{{}, {12}, {34}, {}})
	client := NewClient(mockPDCli, nil, nil, 100, 4)
	regionSplitter := NewRegionSplitter(client)
	err := regionSplitter.ExecuteSortedKeys(context.Background(), nil)
	require.NoError(t, err)
}

// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
// range: [aaa, aae), [aae, aaz), [ccd, ccf), [ccf, ccj)
// rewrite rules: aa -> xx,  cc -> bb
// expected regions after split:
//
//	[, aay), [aay, bba), [bba, bbf), [bbf, bbh), [bbh, bbj),
//	[bbj, cca), [cca, xxe), [xxe, xxz), [xxz, )
func TestSplitAndScatter(t *testing.T) {
	rangeBoundaries := [][]byte{[]byte(""), []byte("aay"), []byte("bba"), []byte("bbh"), []byte("cca"), []byte("")}
	encodeBytes(rangeBoundaries)
	mockPDCli := NewMockPDClientForSplit()
	mockPDCli.SetRegions(rangeBoundaries)
	client := NewClient(mockPDCli, nil, nil, 100, 4)
	regionSplitter := NewRegionSplitter(client)
	ctx := context.Background()

	ranges := initRanges()
	rules := initRewriteRules()
	splitKeys := make([][]byte, 0, len(ranges))
	for _, rg := range ranges {
		tmp, err := restoreutils.RewriteRange(&rg, rules)
		require.NoError(t, err)
		splitKeys = append(splitKeys, tmp.EndKey)
	}
	sort.Slice(splitKeys, func(i, j int) bool {
		return bytes.Compare(splitKeys[i], splitKeys[j]) < 0
	})
	err := regionSplitter.ExecuteSortedKeys(ctx, splitKeys)
	require.NoError(t, err)
	regions := mockPDCli.Regions.ScanRange(nil, nil, 100)
	expected := [][]byte{[]byte(""), []byte("aay"), []byte("bba"), []byte("bbf"), []byte("bbh"), []byte("bbj"), []byte("cca"), []byte("xxe"), []byte("xxz"), []byte("")}
	encodeBytes(expected)
	require.Len(t, regions, len(expected)-1)
	for i, region := range regions {
		require.Equal(t, expected[i], region.Meta.StartKey)
		require.Equal(t, expected[i+1], region.Meta.EndKey)
	}
}

func encodeBytes(keys [][]byte) {
	for i := range keys {
		if len(keys[i]) == 0 {
			continue
		}
		keys[i] = codec.EncodeBytes(nil, keys[i])
	}
}

func TestRawSplit(t *testing.T) {
	// Fix issue #36490.
	splitKeys := [][]byte{{}}
	ctx := context.Background()
	rangeBoundaries := [][]byte{[]byte(""), []byte("aay"), []byte("bba"), []byte("bbh"), []byte("cca"), []byte("")}
	mockPDCli := NewMockPDClientForSplit()
	mockPDCli.SetRegions(rangeBoundaries)
	client := NewClient(mockPDCli, nil, nil, 100, 4, WithRawKV())

	regionSplitter := NewRegionSplitter(client)
	err := regionSplitter.ExecuteSortedKeys(ctx, splitKeys)
	require.NoError(t, err)

	regions := mockPDCli.Regions.ScanRange(nil, nil, 100)
	require.Len(t, regions, len(rangeBoundaries)-1)
	for i, region := range regions {
		require.Equal(t, rangeBoundaries[i], region.Meta.StartKey)
		require.Equal(t, rangeBoundaries[i+1], region.Meta.EndKey)
	}
}

// keys: aae, aaz, ccf, ccj
func initKeys() [][]byte {
	return [][]byte{
		[]byte("aae"),
		[]byte("aaz"),
		[]byte("ccf"),
		[]byte("ccj"),
	}
}

// range: [aaa, aae), [aae, aaz), [ccd, ccf), [ccf, ccj)
func initRanges() []rtree.Range {
	var ranges [4]rtree.Range
	ranges[0] = rtree.Range{
		StartKey: []byte("aaa"),
		EndKey:   []byte("aae"),
	}
	ranges[1] = rtree.Range{
		StartKey: []byte("aae"),
		EndKey:   []byte("aaz"),
	}
	ranges[2] = rtree.Range{
		StartKey: []byte("ccd"),
		EndKey:   []byte("ccf"),
	}
	ranges[3] = rtree.Range{
		StartKey: []byte("ccf"),
		EndKey:   []byte("ccj"),
	}
	return ranges[:]
}

func initRewriteRules() *restoreutils.RewriteRules {
	var rules [2]*import_sstpb.RewriteRule
	rules[0] = &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("aa"),
		NewKeyPrefix: []byte("xx"),
	}
	rules[1] = &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("cc"),
		NewKeyPrefix: []byte("bb"),
	}
	return &restoreutils.RewriteRules{
		Data: rules[:],
	}
}

func keyWithTablePrefix(tableID int64, key string) []byte {
	rawKey := append(tablecodec.GenTableRecordPrefix(tableID), []byte(key)...)
	return codec.EncodeBytes([]byte{}, rawKey)
}

func TestSplitPoint(t *testing.T) {
	ctx := context.Background()
	var oldTableID int64 = 50
	var tableID int64 = 100
	rewriteRules := &restoreutils.RewriteRules{
		Data: []*import_sstpb.RewriteRule{
			{
				OldKeyPrefix: tablecodec.EncodeTablePrefix(oldTableID),
				NewKeyPrefix: tablecodec.EncodeTablePrefix(tableID),
			},
		},
	}

	// range:     b   c d   e       g         i
	//            +---+ +---+       +---------+
	//          +-------------+----------+---------+
	// region:  a             f          h         j
	splitHelper := NewSplitHelper()
	splitHelper.Merge(Valued{Key: Span{StartKey: keyWithTablePrefix(oldTableID, "b"), EndKey: keyWithTablePrefix(oldTableID, "c")}, Value: Value{Size: 100, Number: 100}})
	splitHelper.Merge(Valued{Key: Span{StartKey: keyWithTablePrefix(oldTableID, "d"), EndKey: keyWithTablePrefix(oldTableID, "e")}, Value: Value{Size: 200, Number: 200}})
	splitHelper.Merge(Valued{Key: Span{StartKey: keyWithTablePrefix(oldTableID, "g"), EndKey: keyWithTablePrefix(oldTableID, "i")}, Value: Value{Size: 300, Number: 300}})
	client := NewFakeSplitClient()
	client.AppendRegion(keyWithTablePrefix(tableID, "a"), keyWithTablePrefix(tableID, "f"))
	client.AppendRegion(keyWithTablePrefix(tableID, "f"), keyWithTablePrefix(tableID, "h"))
	client.AppendRegion(keyWithTablePrefix(tableID, "h"), keyWithTablePrefix(tableID, "j"))
	client.AppendRegion(keyWithTablePrefix(tableID, "j"), keyWithTablePrefix(tableID+1, "a"))

	iter := NewSplitHelperIterator([]*RewriteSplitter{{tableID: tableID, rule: rewriteRules, splitter: splitHelper}})
	err := SplitPoint(ctx, iter, client, func(ctx context.Context, u uint64, o int64, ri *RegionInfo, v []Valued) error {
		require.Equal(t, u, uint64(0))
		require.Equal(t, o, int64(0))
		require.Equal(t, ri.Region.StartKey, keyWithTablePrefix(tableID, "a"))
		require.Equal(t, ri.Region.EndKey, keyWithTablePrefix(tableID, "f"))
		require.EqualValues(t, v[0].Key.StartKey, keyWithTablePrefix(tableID, "b"))
		require.EqualValues(t, v[0].Key.EndKey, keyWithTablePrefix(tableID, "c"))
		require.EqualValues(t, v[1].Key.StartKey, keyWithTablePrefix(tableID, "d"))
		require.EqualValues(t, v[1].Key.EndKey, keyWithTablePrefix(tableID, "e"))
		require.Equal(t, len(v), 2)
		return nil
	})
	require.NoError(t, err)
}

func getCharFromNumber(prefix string, i int) string {
	c := '1' + (i % 10)
	b := '1' + (i%100)/10
	a := '1' + i/100
	return fmt.Sprintf("%s%c%c%c", prefix, a, b, c)
}

func TestSplitPoint2(t *testing.T) {
	ctx := context.Background()
	var oldTableID int64 = 50
	var tableID int64 = 100
	rewriteRules := &restoreutils.RewriteRules{
		Data: []*import_sstpb.RewriteRule{
			{
				OldKeyPrefix: tablecodec.EncodeTablePrefix(oldTableID),
				NewKeyPrefix: tablecodec.EncodeTablePrefix(tableID),
			},
		},
	}

	// range:     b   c d   e f                 i j    k l        n
	//            +---+ +---+ +-----------------+ +----+ +--------+
	//          +---------------+--+.....+----+------------+---------+
	// region:  a               g   >128      h            m         o
	splitHelper := NewSplitHelper()
	splitHelper.Merge(Valued{Key: Span{StartKey: keyWithTablePrefix(oldTableID, "b"), EndKey: keyWithTablePrefix(oldTableID, "c")}, Value: Value{Size: 100, Number: 100}})
	splitHelper.Merge(Valued{Key: Span{StartKey: keyWithTablePrefix(oldTableID, "d"), EndKey: keyWithTablePrefix(oldTableID, "e")}, Value: Value{Size: 200, Number: 200}})
	splitHelper.Merge(Valued{Key: Span{StartKey: keyWithTablePrefix(oldTableID, "f"), EndKey: keyWithTablePrefix(oldTableID, "i")}, Value: Value{Size: 300, Number: 300}})
	splitHelper.Merge(Valued{Key: Span{StartKey: keyWithTablePrefix(oldTableID, "j"), EndKey: keyWithTablePrefix(oldTableID, "k")}, Value: Value{Size: 200, Number: 200}})
	splitHelper.Merge(Valued{Key: Span{StartKey: keyWithTablePrefix(oldTableID, "l"), EndKey: keyWithTablePrefix(oldTableID, "n")}, Value: Value{Size: 200, Number: 200}})
	client := NewFakeSplitClient()
	client.AppendRegion(keyWithTablePrefix(tableID, "a"), keyWithTablePrefix(tableID, "g"))
	client.AppendRegion(keyWithTablePrefix(tableID, "g"), keyWithTablePrefix(tableID, getCharFromNumber("g", 0)))
	for i := 0; i < 256; i++ {
		client.AppendRegion(keyWithTablePrefix(tableID, getCharFromNumber("g", i)), keyWithTablePrefix(tableID, getCharFromNumber("g", i+1)))
	}
	client.AppendRegion(keyWithTablePrefix(tableID, getCharFromNumber("g", 256)), keyWithTablePrefix(tableID, "h"))
	client.AppendRegion(keyWithTablePrefix(tableID, "h"), keyWithTablePrefix(tableID, "m"))
	client.AppendRegion(keyWithTablePrefix(tableID, "m"), keyWithTablePrefix(tableID, "o"))
	client.AppendRegion(keyWithTablePrefix(tableID, "o"), keyWithTablePrefix(tableID+1, "a"))

	firstSplit := true
	iter := NewSplitHelperIterator([]*RewriteSplitter{{tableID: tableID, rule: rewriteRules, splitter: splitHelper}})
	err := SplitPoint(ctx, iter, client, func(ctx context.Context, u uint64, o int64, ri *RegionInfo, v []Valued) error {
		if firstSplit {
			require.Equal(t, u, uint64(0))
			require.Equal(t, o, int64(0))
			require.Equal(t, ri.Region.StartKey, keyWithTablePrefix(tableID, "a"))
			require.Equal(t, ri.Region.EndKey, keyWithTablePrefix(tableID, "g"))
			require.EqualValues(t, v[0].Key.StartKey, keyWithTablePrefix(tableID, "b"))
			require.EqualValues(t, v[0].Key.EndKey, keyWithTablePrefix(tableID, "c"))
			require.EqualValues(t, v[1].Key.StartKey, keyWithTablePrefix(tableID, "d"))
			require.EqualValues(t, v[1].Key.EndKey, keyWithTablePrefix(tableID, "e"))
			require.EqualValues(t, v[2].Key.StartKey, keyWithTablePrefix(tableID, "f"))
			require.EqualValues(t, v[2].Key.EndKey, keyWithTablePrefix(tableID, "g"))
			require.Equal(t, v[2].Value.Size, uint64(1))
			require.Equal(t, v[2].Value.Number, int64(1))
			require.Equal(t, len(v), 3)
			firstSplit = false
		} else {
			require.Equal(t, u, uint64(1))
			require.Equal(t, o, int64(1))
			require.Equal(t, ri.Region.StartKey, keyWithTablePrefix(tableID, "h"))
			require.Equal(t, ri.Region.EndKey, keyWithTablePrefix(tableID, "m"))
			require.EqualValues(t, v[0].Key.StartKey, keyWithTablePrefix(tableID, "j"))
			require.EqualValues(t, v[0].Key.EndKey, keyWithTablePrefix(tableID, "k"))
			require.EqualValues(t, v[1].Key.StartKey, keyWithTablePrefix(tableID, "l"))
			require.EqualValues(t, v[1].Key.EndKey, keyWithTablePrefix(tableID, "m"))
			require.Equal(t, v[1].Value.Size, uint64(100))
			require.Equal(t, v[1].Value.Number, int64(100))
			require.Equal(t, len(v), 2)
		}
		return nil
	})
	require.NoError(t, err)
}
