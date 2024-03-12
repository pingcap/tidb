// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.
package split

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
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

type mockScatterFailedPDClient struct {
	pd.Client
	failed       map[uint64]int
	failedBefore int
}

func (c *mockScatterFailedPDClient) ScatterRegion(ctx context.Context, regionID uint64) error {
	if c.failed == nil {
		c.failed = make(map[uint64]int)
	}
	c.failed[regionID]++
	if c.failed[regionID] > c.failedBefore {
		return nil
	}
	return status.Errorf(codes.Unknown, "region %d is not fully replicated", regionID)
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
	client := pdClient{
		needScatterVal: true,
		client:         &mockScatterFailedPDClient{failedBefore: 7},
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

type mockOldPDClient struct {
	pd.Client

	scattered map[uint64]struct{}
}

func (c *mockOldPDClient) ScatterRegion(_ context.Context, regionID uint64) error {
	if c.scattered == nil {
		c.scattered = make(map[uint64]struct{})
	}
	c.scattered[regionID] = struct{}{}
	return nil
}

func (c *mockOldPDClient) ScatterRegions(context.Context, []uint64, ...pd.RegionsOption) (*pdpb.ScatterRegionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Ah, yep")
}

func TestScatterBackwardCompatibility(t *testing.T) {
	client := pdClient{
		needScatterVal: true,
		client:         &mockOldPDClient{},
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
	err := client.ScatterRegions(ctx, regions)
	require.NoError(t, err)
	require.Equal(t, map[uint64]struct{}{1: {}, 2: {}}, client.client.(*mockOldPDClient).scattered)
}
