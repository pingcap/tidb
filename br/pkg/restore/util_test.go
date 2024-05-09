// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"context"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/utils/utilstest"
	"github.com/stretchr/testify/require"
)

func TestGetTSWithRetry(t *testing.T) {
	t.Run("PD leader is healthy:", func(t *testing.T) {
		retryTimes := -1000
		pDClient := utilstest.NewFakePDClient(nil, false, &retryTimes)
		_, err := restore.GetTSWithRetry(context.Background(), pDClient)
		require.NoError(t, err)
	})

	t.Run("PD leader failure:", func(t *testing.T) {
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/utils/set-attempt-to-one", "1*return(true)"))
		defer func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/utils/set-attempt-to-one"))
		}()
		retryTimes := -1000
		pDClient := utilstest.NewFakePDClient(nil, true, &retryTimes)
		_, err := restore.GetTSWithRetry(context.Background(), pDClient)
		require.Error(t, err)
	})

	t.Run("PD leader switch successfully", func(t *testing.T) {
		retryTimes := 0
		pDClient := utilstest.NewFakePDClient(nil, true, &retryTimes)
		_, err := restore.GetTSWithRetry(context.Background(), pDClient)
		require.NoError(t, err)
	})
}

func regionInfo(startKey, endKey string) *split.RegionInfo {
	return &split.RegionInfo{
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
	err = split.CheckPartRegionConsistency(startKey, endKey, nil)
	require.Error(t, err)
	err = split.CheckPartRegionConsistency(startKey, endKey, []*split.RegionInfo{
		regionInfo("b", "c"),
	})
	require.Error(t, err)
	err = split.CheckPartRegionConsistency(startKey, endKey, []*split.RegionInfo{
		regionInfo("a", "c"),
		regionInfo("d", "e"),
	})
	require.Error(t, err)
	err = split.CheckPartRegionConsistency(startKey, endKey, []*split.RegionInfo{
		regionInfo("a", "c"),
		regionInfo("c", "d"),
	})
	require.NoError(t, err)
	err = split.CheckPartRegionConsistency(startKey, endKey, []*split.RegionInfo{
		regionInfo("a", "c"),
		regionInfo("c", "d"),
		regionInfo("d", "f"),
	})
	require.NoError(t, err)
	err = split.CheckPartRegionConsistency(startKey, endKey, []*split.RegionInfo{
		regionInfo("a", "c"),
		regionInfo("c", "z"),
	})
	require.NoError(t, err)
}
