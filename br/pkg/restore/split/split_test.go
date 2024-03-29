// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.
package split

import (
	"bytes"
	"context"
	goerrors "errors"
	"slices"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
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

	_, err := client.SplitWaitAndScatterOnRegion(ctx, &RegionInfo{}, [][]byte{{1}})
	require.ErrorIs(t, err, context.Canceled)
}

func TestGetSplitKeyPerRegion(t *testing.T) {
	// test case moved from lightning
	tableID := int64(1)
	keys := []int64{1, 10, 100, 1000, 10000, -1}
	sortedRegions := make([]*RegionInfo, 0, len(keys))
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
	sortedKeys := make([][]byte, 0, len(checkKeys))

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

func TestGetSplitKeyPerRegionSkipSmallRegions(t *testing.T) {
	// regions are [a, b), [b, ba), [ba, ca), [ca, da), [da, e)
	// keys are a, b, c, d, e
	// for a, b, e, they are in region boundary, so they should be skipped
	// for c, because [b, c) and [c, d) are already split, so it should be skipped
	// for d, because [c, d) and [d, e) are already split, so it should be skipped
	sortedKeys := [][]byte{
		[]byte("a"),
		[]byte("b"),
		[]byte("c"),
		[]byte("d"),
		[]byte("e"),
	}
	sortedRegions := []*RegionInfo{
		{
			Region: &metapb.Region{
				Id:       1,
				StartKey: []byte("a"),
				EndKey:   []byte("b"),
			},
		},
		{
			Region: &metapb.Region{
				Id:       2,
				StartKey: []byte("b"),
				EndKey:   []byte("ba"),
			},
		},
		{
			Region: &metapb.Region{
				Id:       3,
				StartKey: []byte("ba"),
				EndKey:   []byte("ca"),
			},
		},
		{
			Region: &metapb.Region{
				Id:       4,
				StartKey: []byte("ca"),
				EndKey:   []byte("da"),
			},
		},
		{
			Region: &metapb.Region{
				Id:       5,
				StartKey: []byte("da"),
				EndKey:   []byte("e"),
			},
		},
	}
	result := getSplitKeysOfRegions(sortedKeys, sortedRegions, true)
	require.Len(t, result, 0)
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
					Region: &metapb.Region{
						Id:          6,
						StartKey:    codec.EncodeBytes([]byte{}, []byte("b")),
						EndKey:      codec.EncodeBytes([]byte{}, []byte("d")),
						RegionEpoch: nil,
					},
				},
				{
					Region: &metapb.Region{
						Id:       8,
						StartKey: codec.EncodeBytes([]byte{}, []byte("e")),
						EndKey:   codec.EncodeBytes([]byte{}, []byte("f")),
					},
				},
			},
		},
	}
	for _, ca := range cases {
		err := CheckRegionConsistency(ca.startKey, ca.endKey, ca.regions)
		require.Error(t, err)
		require.Regexp(t, ca.err, err.Error())
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
	})
	mockPDClient.Regions.SetRegion(&pdtypes.Region{
		Meta: &metapb.Region{
			Id:       4,
			StartKey: []byte{4},
			EndKey:   []byte{5},
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
		},
		{
			Meta: &metapb.Region{
				Id:       3,
				StartKey: []byte{3},
				EndKey:   []byte{4},
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
