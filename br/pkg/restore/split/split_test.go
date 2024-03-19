// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.
package split

import (
	"context"
	goerrors "errors"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/stretchr/testify/require"
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
	mockClient := newMockPDClientForSplit()
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
	mockClient := newMockPDClientForSplit()
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
	require.Equal(t, map[uint64]int{1: 1, 2: 1}, client.client.(*mockPDClientForSplit).scatterRegion.count)
}

func TestWaitForScatterRegions(t *testing.T) {
	mockPDCli := newMockPDClientForSplit()
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
	mockCli := newMockPDClientForSplit()
	mockCli.splitRegions.fn = func() (bool, *kvrpcpb.SplitRegionResponse, error) {
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

	_, _, err := client.SplitWaitScatter(ctx, &RegionInfo{}, [][]byte{{1}})
	require.ErrorIs(t, err, context.Canceled)
}
