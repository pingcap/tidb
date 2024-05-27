// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils_test

import (
	"context"
	"testing"

	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/br/pkg/restore/internal/utils"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/pkg/tablecodec"
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
		tmp, err := restoreutils.RewriteRange(&rg, rules)
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

func initRewriteRules() *restoreutils.RewriteRules {
	var rules [2]*import_sstpb.RewriteRule
	rules[0] = &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("aa"),
		NewKeyPrefix: []byte("xx"),
	}
	rules[1] = &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("cc"),
		NewKeyPrefix: []byte("bb"),
	}
	return &restoreutils.RewriteRules{
		Data: rules[:],
	}
}

func TestSortRange(t *testing.T) {
	dataRules := []*import_sstpb.RewriteRule{
		{OldKeyPrefix: tablecodec.GenTableRecordPrefix(1), NewKeyPrefix: tablecodec.GenTableRecordPrefix(4)},
		{OldKeyPrefix: tablecodec.GenTableRecordPrefix(2), NewKeyPrefix: tablecodec.GenTableRecordPrefix(5)},
	}
	rewriteRules := &restoreutils.RewriteRules{
		Data: dataRules,
	}
	ranges1 := []rtree.Range{
		{
			StartKey: append(tablecodec.GenTableRecordPrefix(1), []byte("aaa")...),
			EndKey:   append(tablecodec.GenTableRecordPrefix(1), []byte("bbb")...), Files: nil,
		},
	}
	for i, rg := range ranges1 {
		tmp, _ := restoreutils.RewriteRange(&rg, rewriteRules)
		ranges1[i] = *tmp
	}
	rs1, err := utils.SortRanges(ranges1)
	require.NoErrorf(t, err, "sort range1 failed: %v", err)
	rangeEquals(t, rs1, []rtree.Range{
		{
			StartKey: append(tablecodec.GenTableRecordPrefix(4), []byte("aaa")...),
			EndKey:   append(tablecodec.GenTableRecordPrefix(4), []byte("bbb")...), Files: nil,
		},
	})

	ranges2 := []rtree.Range{
		{
			StartKey: append(tablecodec.GenTableRecordPrefix(1), []byte("aaa")...),
			EndKey:   append(tablecodec.GenTableRecordPrefix(2), []byte("bbb")...), Files: nil,
		},
	}
	for _, rg := range ranges2 {
		_, err := restoreutils.RewriteRange(&rg, rewriteRules)
		require.Error(t, err)
		require.Regexp(t, "table id mismatch.*", err.Error())
	}

	ranges3 := []rtree.Range{
		{StartKey: []byte("aaa"), EndKey: []byte("aae")},
		{StartKey: []byte("aae"), EndKey: []byte("aaz")},
		{StartKey: []byte("ccd"), EndKey: []byte("ccf")},
		{StartKey: []byte("ccf"), EndKey: []byte("ccj")},
	}
	rewriteRules1 := &restoreutils.RewriteRules{
		Data: []*import_sstpb.RewriteRule{
			{
				OldKeyPrefix: []byte("aa"),
				NewKeyPrefix: []byte("xx"),
			}, {
				OldKeyPrefix: []byte("cc"),
				NewKeyPrefix: []byte("bb"),
			},
		},
	}
	for i, rg := range ranges3 {
		tmp, _ := restoreutils.RewriteRange(&rg, rewriteRules1)
		ranges3[i] = *tmp
	}
	rs3, err := utils.SortRanges(ranges3)
	require.NoErrorf(t, err, "sort range1 failed: %v", err)
	rangeEquals(t, rs3, []rtree.Range{
		{StartKey: []byte("bbd"), EndKey: []byte("bbf"), Files: nil},
		{StartKey: []byte("bbf"), EndKey: []byte("bbj"), Files: nil},
		{StartKey: []byte("xxa"), EndKey: []byte("xxe"), Files: nil},
		{StartKey: []byte("xxe"), EndKey: []byte("xxz"), Files: nil},
	})
}

func rangeEquals(t *testing.T, obtained, expected []rtree.Range) {
	require.Equal(t, len(expected), len(obtained))
	for i := range obtained {
		require.Equal(t, expected[i].StartKey, obtained[i].StartKey)
		require.Equal(t, expected[i].EndKey, obtained[i].EndKey)
	}
}
