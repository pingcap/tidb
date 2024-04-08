// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package split

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBatchSplit(t *testing.T) {
	backup := maxBatchSplitSize
	maxBatchSplitSize = 7
	t.Cleanup(func() {
		maxBatchSplitSize = backup
	})

	mockPDClient := NewMockPDClientForSplit()
	keys := [][]byte{[]byte(""), []byte("")}
	setRegions := mockPDClient.SetRegions(keys)
	require.Len(t, setRegions, 1)
	splitRegion := &RegionInfo{Region: setRegions[0]}
	mockClient := &pdClient{
		client:           mockPDClient,
		splitBatchKeyCnt: 100,
		isRawKv:          true, // make tests more readable
	}
	ctx := context.Background()

	splitKeys := [][]byte{
		[]byte("ba"), []byte("bb"), []byte("bc"),
		[]byte("bd"), []byte("be"), []byte("bf"),
		[]byte("bg"), []byte("bh"),
	}
	expectedBatchSplitCnt := 3

	_, err := mockClient.SplitWaitAndScatter(ctx, splitRegion, splitKeys)
	require.NoError(t, err)

	// check split ranges
	regions, err := PaginateScanRegion(ctx, mockClient, []byte{'b'}, []byte{'c'}, 5)
	require.NoError(t, err)
	expected := [][]byte{[]byte("")}
	expected = append(expected, splitKeys...)
	expected = append(expected, []byte(""))
	checkRegionsBoundaries(t, regions, expected)

	require.EqualValues(t, expectedBatchSplitCnt, mockPDClient.splitRegions.count)
	require.EqualValues(t, len(splitKeys), mockPDClient.scatterRegions.regionCount)
}

func TestScanRegionEmptyResult(t *testing.T) {
	backup := WaitRegionOnlineAttemptTimes
	WaitRegionOnlineAttemptTimes = 2
	backup2 := SplitRetryTimes
	SplitRetryTimes = 2
	t.Cleanup(func() {
		WaitRegionOnlineAttemptTimes = backup
		SplitRetryTimes = backup2
	})
	mockPDClient := NewMockPDClientForSplit()
	keys := [][]byte{[]byte(""), []byte("")}
	mockPDClient.SetRegions(keys)
	mockPDClient.scanRegions.errors = []error{nil, nil, nil, nil}
	mockClient := &pdClient{
		client:           mockPDClient,
		splitBatchKeyCnt: 100,
		splitConcurrency: 4,
		isRawKv:          true, // make tests more readable
	}
	ctx := context.Background()

	splitKeys := [][]byte{
		[]byte("ba"), []byte("bb"),
	}

	_, err := mockClient.SplitKeysAndScatter(ctx, splitKeys)
	require.ErrorContains(t, err, "scan region return empty result")
}
