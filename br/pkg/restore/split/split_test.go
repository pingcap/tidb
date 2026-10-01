// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.
package split_test

import (
	"context"
	"testing"

	"github.com/pingcap/failpoint"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

type nilMetaPDClient struct {
	pd.Client
}

func (c *nilMetaPDClient) GetRegionByID(
	ctx context.Context,
	regionID uint64,
	opts ...pd.GetRegionOption,
) (*pd.Region, error) {
	return &pd.Region{}, nil
}

func TestGetRegionByIDWithNilMeta(t *testing.T) {
	client := split.NewSplitClient(&nilMetaPDClient{}, nil, false)

	region, err := client.GetRegionByID(context.Background(), 1)
	require.NoError(t, err)
	require.Nil(t, region)
}

func TestScanRegionBackOfferWithSuccess(t *testing.T) {
	var counter int
	bo := split.NewWaitRegionOnlineBackoffer()

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
	bo := split.NewWaitRegionOnlineBackoffer()

	err := utils.WithRetry(context.Background(), func() error {
		defer func() {
			counter++
		}()
		return berrors.ErrPDBatchScanRegion
	}, bo)
	require.Error(t, err)
	require.Equal(t, counter, split.WaitRegionOnlineAttemptTimes)
}

func TestScanRegionBackOfferWithStopRetry(t *testing.T) {
	_ = failpoint.Enable("github.com/pingcap/tidb/br/pkg/restore/split/hint-scan-region-backoff", "return(true)")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/restore/split/hint-scan-region-backoff")
	}()

	var counter int
	bo := split.NewWaitRegionOnlineBackoffer()

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
