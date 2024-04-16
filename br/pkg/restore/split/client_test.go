// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package split

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
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

func TestSplitScatter(t *testing.T) {
	backup := maxBatchSplitSize
	maxBatchSplitSize = 100
	t.Cleanup(func() {
		maxBatchSplitSize = backup
	})

	stmtCtx := stmtctx.NewStmtCtx()
	tableID := int64(1)
	tableStartKey := tablecodec.EncodeTablePrefix(tableID)
	tableStartKey = codec.EncodeBytes(nil, tableStartKey)
	tableEndKey := tablecodec.EncodeTablePrefix(tableID + 1)
	tableEndKey = codec.EncodeBytes(nil, tableEndKey)
	keys := [][]byte{[]byte(""), tableStartKey}
	// pre split 3 regions [..., (0)), [(0), (1)), [(1), ...]
	for i := int64(0); i < 2; i++ {
		keyBytes, err := codec.EncodeKey(stmtCtx.TimeZone(), nil, types.NewIntDatum(i))
		require.NoError(t, err)
		h, err := kv.NewCommonHandle(keyBytes)
		require.NoError(t, err)
		key := tablecodec.EncodeRowKeyWithHandle(tableID, h)
		key = codec.EncodeBytes(nil, key)
		keys = append(keys, key)
	}
	keys = append(keys, tableEndKey, []byte(""))

	mockPDClient := NewMockPDClientForSplit()
	mockPDClient.SetRegions(keys)
	mockClient := &pdClient{
		client:           mockPDClient,
		splitConcurrency: 20,
		splitBatchKeyCnt: 100,
	}
	ctx := context.Background()

	// (0, 0), (0, 10000), ... (0, 90000), (1, 0), (1, 10000), ... (1, 90000)
	splitKeys := make([][]byte, 0, 20)
	for i := int64(0); i < 2; i++ {
		for j := int64(0); j < 10; j++ {
			keyBytes, err := codec.EncodeKey(stmtCtx.TimeZone(), nil, types.NewIntDatum(i), types.NewIntDatum(j*10000))
			require.NoError(t, err)
			h, err := kv.NewCommonHandle(keyBytes)
			require.NoError(t, err)
			key := tablecodec.EncodeRowKeyWithHandle(tableID, h)
			splitKeys = append(splitKeys, key)
		}
	}

	_, err := mockClient.SplitKeysAndScatter(ctx, splitKeys)
	require.NoError(t, err)

	// check split ranges
	regions, err := PaginateScanRegion(ctx, mockClient, tableStartKey, tableEndKey, 5)
	require.NoError(t, err)
	expected := make([][]byte, 0, 24)
	expected = append(expected, tableStartKey)
	expected = append(expected, keys[2])
	for _, k := range splitKeys[:10] {
		expected = append(expected, codec.EncodeBytes(nil, k))
	}
	expected = append(expected, keys[3])
	for _, k := range splitKeys[10:] {
		expected = append(expected, codec.EncodeBytes(nil, k))
	}
	expected = append(expected, tableEndKey)

	checkRegionsBoundaries(t, regions, expected)
}

func TestSplitScatterRawKV(t *testing.T) {
	backup := maxBatchSplitSize
	maxBatchSplitSize = 7
	t.Cleanup(func() {
		maxBatchSplitSize = backup
	})

	mockPDClient := NewMockPDClientForSplit()
	keys := [][]byte{[]byte(""), []byte("aay"), []byte("bba"), []byte("bbh"), []byte("cca"), []byte("")}
	mockPDClient.SetRegions(keys)
	mockClient := &pdClient{
		client:           mockPDClient,
		splitConcurrency: 10,
		splitBatchKeyCnt: 100,
		isRawKv:          true, // make tests more readable
	}
	ctx := context.Background()

	// ranges [b, ba), [ba, bb), [bb, bc), ... [by, bz)
	splitKeys := [][]byte{{'b'}}
	for i := byte('a'); i <= 'z'; i++ {
		splitKeys = append(splitKeys, []byte{'b', i})
	}

	_, err := mockClient.SplitKeysAndScatter(ctx, splitKeys)
	require.NoError(t, err)

	// check split ranges
	regions, err := PaginateScanRegion(ctx, mockClient, []byte{'b'}, []byte{'c'}, 5)
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
	// the old regions will not be scattered. They are [..., bba), [bba, bbh), [..., cca)
	require.Equal(t, len(result)-3, mockPDClient.scatterRegions.regionCount)
}

func TestSplitScatterEmptyEndKey(t *testing.T) {
	mockPDClient := NewMockPDClientForSplit()
	keys := [][]byte{[]byte(""), []byte("aay"), []byte("bba"), []byte("bbh"), []byte("cca"), []byte("")}
	mockPDClient.SetRegions(keys)
	mockClient := &pdClient{
		client:           mockPDClient,
		splitConcurrency: 10,
		splitBatchKeyCnt: 100,
		isRawKv:          true, // make tests more readable
	}
	ctx := context.Background()

	splitKeys := [][]byte{{'b'}, {'c'}, {}}

	_, err := mockClient.SplitKeysAndScatter(ctx, splitKeys)
	require.NoError(t, err)

	// check split ranges
	regions, err := PaginateScanRegion(ctx, mockClient, []byte{}, []byte{}, 5)
	require.NoError(t, err)
	result := [][]byte{
		[]byte(""), []byte("aay"),
		[]byte("b"), []byte("bba"), []byte("bbh"),
		[]byte("c"), []byte("cca"), []byte(""),
	}
	checkRegionsBoundaries(t, regions, result)

	// test only one split key which is empty
	_, err = mockClient.SplitKeysAndScatter(ctx, [][]byte{{}})
	require.NoError(t, err)
	regions, err = PaginateScanRegion(ctx, mockClient, []byte{}, []byte{}, 5)
	require.NoError(t, err)
	checkRegionsBoundaries(t, regions, result)
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

func TestSplitMeetErrorAndRetry(t *testing.T) {
	mockPDClient := NewMockPDClientForSplit()
	keys := [][]byte{[]byte(""), []byte("a"), []byte("")}
	mockPDClient.SetRegions(keys)
	mockClient := &pdClient{
		client:           mockPDClient,
		splitConcurrency: 1,
		splitBatchKeyCnt: 100,
		isRawKv:          true, // make tests more readable
	}
	ctx := context.Background()

	// mimic the epoch change after the scan region before split region

	mockPDClient.splitRegions.hijacked = func() (bool, *kvrpcpb.SplitRegionResponse, error) {
		// clear self
		mockPDClient.splitRegions.hijacked = nil
		return false, nil, errors.New("epoch not match")
	}

	_, err := mockClient.SplitKeysAndScatter(ctx, [][]byte{{'b'}})
	require.NoError(t, err)
	regions, err := PaginateScanRegion(ctx, mockClient, []byte{'a'}, []byte{}, 5)
	require.NoError(t, err)
	checkRegionsBoundaries(t, regions, [][]byte{{'a'}, {'b'}, {}})
	require.EqualValues(t, 2, mockPDClient.splitRegions.count)

	// mimic "no valid key" error

	mockPDClient.splitRegions.hijacked = func() (bool, *kvrpcpb.SplitRegionResponse, error) {
		// clear self
		mockPDClient.splitRegions.hijacked = nil
		return false, nil, errors.New("no valid key")
	}
	mockPDClient.splitRegions.count = 0

	_, err = mockClient.SplitKeysAndScatter(ctx, [][]byte{{'c'}})
	require.NoError(t, err)
	regions, err = PaginateScanRegion(ctx, mockClient, []byte{'b'}, []byte{}, 5)
	require.NoError(t, err)
	checkRegionsBoundaries(t, regions, [][]byte{{'b'}, {'c'}, {}})
	require.EqualValues(t, 2, mockPDClient.splitRegions.count)

	// test retry is also failed

	backup := SplitRetryTimes
	SplitRetryTimes = 2
	t.Cleanup(func() {
		SplitRetryTimes = backup
	})
	mockPDClient.splitRegions.hijacked = func() (bool, *kvrpcpb.SplitRegionResponse, error) {
		return false, nil, errors.New("no valid key")
	}
	_, err = mockClient.SplitKeysAndScatter(ctx, [][]byte{{'d'}})
	require.ErrorContains(t, err, "no valid key")
}
