// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package split

import (
	"testing"

	"github.com/pingcap/tidb/dumpling/context"
	"github.com/stretchr/testify/require"
)

func TestSplit(t *testing.T) {
	backup := maxBatchSplitSize
	maxBatchSplitSize = 7
	t.Cleanup(func() {
		maxBatchSplitSize = backup
	})

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

	// so with a batch split key size of 6, there will be 9 time batch split
	// 1. region: [aay, bba), keys: [b, ba, bb]
	// 2. region: [bbh, cca), keys: [bc, bd, be]
	// 3. region: [bf, cca), keys: [bf, bg, bh]
	// 4. region: [bj, cca), keys: [bi, bj, bk]
	// 5. region: [bj, cca), keys: [bl, bm, bn]
	// 6. region: [bn, cca), keys: [bo, bp, bq]
	// 7. region: [bn, cca), keys: [br, bs, bt]
	// 8. region: [br, cca), keys: [bu, bv, bw]
	// 9. region: [bv, cca), keys: [bx, by, bz]

	require.EqualValues(t, 9, mockPDClient.splitRegions.count)
}
