// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package split

import (
	"testing"

	"github.com/pingcap/tidb/dumpling/context"
	"github.com/stretchr/testify/require"
)

func TestSplit(t *testing.T) {
	mockPDClient := newMockPDClientForSplit()
	keys := [][]byte{[]byte(""), []byte("aay"), []byte("bba"), []byte("bbh"), []byte("cca"), []byte("")}
	mockPDClient.SetRegions(keys)
	mockClient := &pdClient{
		client:           mockPDClient,
		splitConcurrency: 1,
		isRawKv:          true, // make tests more readable
	}
	ctx := context.Background()

	// ranges [b, ba), [ba, bb), [bb, bc), ... [by, bz)
	splitKeys := [][]byte{{'b'}}
	for i := byte('a'); i <= 'z'; i++ {
		splitKeys = append(splitKeys, []byte{'b', i})
	}

	regions, err := mockClient.SplitWaitAndScatter(ctx, splitKeys)
	require.NoError(t, err)

	// check split ranges
	regions, err = PaginateScanRegion(ctx, mockClient, []byte{'b'}, []byte{'c'}, 5)
	require.NoError(t, err)
	result := [][]byte{
		[]byte("b"), []byte("ba"), []byte("bb"), []byte("bba"), []byte("bbh"), []byte("bc"),
		[]byte("bd"), []byte("be"), []byte("bf"), []byte("bg"), []byte("bh"), []byte("bi"), []byte("bj"),
		[]byte("bk"), []byte("bl"), []byte("bm"), []byte("bn"), []byte("bo"), []byte("bp"), []byte("bq"),
		[]byte("br"), []byte("bs"), []byte("bt"), []byte("bu"), []byte("bv"), []byte("bw"), []byte("bx"),
		[]byte("by"), []byte("bz"), []byte("cca"),
	}
	checkRegionsBoundaries(t, regions, result)
}
