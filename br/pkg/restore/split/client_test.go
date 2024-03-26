// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package split

import (
	"testing"

	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/stretchr/testify/require"
)

func TestSplit(t *testing.T) {
	mockClient := newMockPDClientForSplit()
	keys := [][]byte{[]byte(""), []byte("aay"), []byte("bba"), []byte("bbh"), []byte("cca"), []byte("")}

	client := initTestSplitClient(keys, hook)
	local := &Backend{
		splitCli: client,
		logger:   log.L(),
	}
	local.RegionSplitBatchSize = 4
	local.RegionSplitConcurrency = 4

	// current region ranges: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
	rangeStart := codec.EncodeBytes([]byte{}, []byte("b"))
	rangeEnd := codec.EncodeBytes([]byte{}, []byte("c"))
	regions, err := split.PaginateScanRegion(ctx, client, rangeStart, rangeEnd, 5)
	require.NoError(t, err)
	// regions is: [aay, bba), [bba, bbh), [bbh, cca)
	checkRegionRanges(t, regions, [][]byte{[]byte("aay"), []byte("bba"), []byte("bbh"), []byte("cca")})

	// generate:  ranges [b, ba), [ba, bb), [bb, bc), ... [by, bz)
	ranges := make([]common.Range, 0)
	start := []byte{'b'}
	for i := byte('a'); i <= 'z'; i++ {
		end := []byte{'b', i}
		ranges = append(ranges, common.Range{Start: start, End: end})
		start = end
	}

	err = local.SplitAndScatterRegionByRanges(ctx, ranges, true)
	if len(errPat) != 0 {
		require.Error(t, err)
		require.ErrorContains(t, err, errPat)
		return
	}
	require.NoError(t, err)
	splitHook.check(t, client)

	// check split ranges
	regions, err = split.PaginateScanRegion(ctx, client, rangeStart, rangeEnd, 5)
	require.NoError(t, err)
	result := [][]byte{
		[]byte("b"), []byte("ba"), []byte("bb"), []byte("bba"), []byte("bbh"), []byte("bc"),
		[]byte("bd"), []byte("be"), []byte("bf"), []byte("bg"), []byte("bh"), []byte("bi"), []byte("bj"),
		[]byte("bk"), []byte("bl"), []byte("bm"), []byte("bn"), []byte("bo"), []byte("bp"), []byte("bq"),
		[]byte("br"), []byte("bs"), []byte("bt"), []byte("bu"), []byte("bv"), []byte("bw"), []byte("bx"),
		[]byte("by"), []byte("bz"), []byte("cca"),
	}
	checkRegionRanges(t, regions, result)
}
