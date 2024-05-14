// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils_test

import (
	"context"
	"testing"

	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

func TestScanEmptyRegion(t *testing.T) {
	mockPDCli := split.NewMockPDClientForSplit()
	mockPDCli.SetRegions([][]byte{{}, {12}, {34}, {}})
	client := split.NewClient(mockPDCli, nil, nil, 100, 4)
	ranges := initRanges()
	// make ranges has only one
	ranges = ranges[0:1]
	regionSplitter := utils.NewRegionSplitter(client)

	ctx := context.Background()
	err := regionSplitter.ExecuteSplit(ctx, ranges)
	// should not return error with only one range entry
	require.NoError(t, err)
}

// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
// range: [aaa, aae), [aae, aaz), [ccd, ccf), [ccf, ccj)
// rewrite rules: aa -> xx,  cc -> bb
// expected regions after split:
//
//	[, aay), [aay, bba), [bba, bbf), [bbf, bbh), [bbh, bbj),
//	[bbj, cca), [cca, xxe), [xxe, xxz), [xxz, )
func TestSplitAndScatter(t *testing.T) {
	rangeBoundaries := [][]byte{[]byte(""), []byte("aay"), []byte("bba"), []byte("bbh"), []byte("cca"), []byte("")}
	encodeBytes(rangeBoundaries)
	mockPDCli := split.NewMockPDClientForSplit()
	mockPDCli.SetRegions(rangeBoundaries)
	client := split.NewClient(mockPDCli, nil, nil, 100, 4)
	regionSplitter := utils.NewRegionSplitter(client)
	ctx := context.Background()

	ranges := initRanges()
	rules := initRewriteRules()
	for i, rg := range ranges {
		tmp, err := utils.RewriteRange(&rg, rules)
		require.NoError(t, err)
		ranges[i] = *tmp
	}
	err := regionSplitter.ExecuteSplit(ctx, ranges)
	require.NoError(t, err)
	regions := mockPDCli.Regions.ScanRange(nil, nil, 100)
	expected := [][]byte{[]byte(""), []byte("aay"), []byte("bba"), []byte("bbf"), []byte("bbh"), []byte("bbj"), []byte("cca"), []byte("xxe"), []byte("xxz"), []byte("")}
	encodeBytes(expected)
	require.Len(t, regions, len(expected)-1)
	for i, region := range regions {
		require.Equal(t, expected[i], region.Meta.StartKey)
		require.Equal(t, expected[i+1], region.Meta.EndKey)
	}
}

func encodeBytes(keys [][]byte) {
	for i := range keys {
		if len(keys[i]) == 0 {
			continue
		}
		keys[i] = codec.EncodeBytes(nil, keys[i])
	}
}

func TestRawSplit(t *testing.T) {
	// Fix issue #36490.
	ranges := []rtree.Range{
		{
			StartKey: []byte{0},
			EndKey:   []byte{},
		},
	}
	ctx := context.Background()
	rangeBoundaries := [][]byte{[]byte(""), []byte("aay"), []byte("bba"), []byte("bbh"), []byte("cca"), []byte("")}
	mockPDCli := split.NewMockPDClientForSplit()
	mockPDCli.SetRegions(rangeBoundaries)
	client := split.NewClient(mockPDCli, nil, nil, 100, 4, split.WithRawKV())

	regionSplitter := utils.NewRegionSplitter(client)
	err := regionSplitter.ExecuteSplit(ctx, ranges)
	require.NoError(t, err)

	regions := mockPDCli.Regions.ScanRange(nil, nil, 100)
	require.Len(t, regions, len(rangeBoundaries)-1)
	for i, region := range regions {
		require.Equal(t, rangeBoundaries[i], region.Meta.StartKey)
		require.Equal(t, rangeBoundaries[i+1], region.Meta.EndKey)
	}
}

// range: [aaa, aae), [aae, aaz), [ccd, ccf), [ccf, ccj)
func initRanges() []rtree.Range {
	var ranges [4]rtree.Range
	ranges[0] = rtree.Range{
		StartKey: []byte("aaa"),
		EndKey:   []byte("aae"),
	}
	ranges[1] = rtree.Range{
		StartKey: []byte("aae"),
		EndKey:   []byte("aaz"),
	}
	ranges[2] = rtree.Range{
		StartKey: []byte("ccd"),
		EndKey:   []byte("ccf"),
	}
	ranges[3] = rtree.Range{
		StartKey: []byte("ccf"),
		EndKey:   []byte("ccj"),
	}
	return ranges[:]
}

func initRewriteRules() *utils.RewriteRules {
	var rules [2]*import_sstpb.RewriteRule
	rules[0] = &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("aa"),
		NewKeyPrefix: []byte("xx"),
	}
	rules[1] = &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("cc"),
		NewKeyPrefix: []byte("bb"),
	}
	return &utils.RewriteRules{
		Data: rules[:],
	}
}
