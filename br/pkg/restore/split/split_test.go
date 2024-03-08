// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.
package split

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
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

type mockScatterPDClient struct {
	pd.Client
	failed       map[uint64]int
	failedBefore int
}

func (c *mockScatterPDClient) ScatterRegion(ctx context.Context, regionID uint64) error {
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

func (b *recordCntBackoffer) NextBackoff(err error) time.Duration {
	b.already++
	return 0
}

func (b *recordCntBackoffer) Attempt() int {
	return 100
}

func TestScatterSequentiallyRetryCnt(t *testing.T) {
	client := pdClient{
		needScatterVal: true,
		client:         &mockScatterPDClient{failedBefore: 7},
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
