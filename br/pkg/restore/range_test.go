// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/stretchr/testify/require"
)

func rangeEquals(t *testing.T, obtained, expected []rtree.Range) {
	require.Equal(t, len(expected), len(obtained))
	for i := range obtained {
		require.Equal(t, expected[i].StartKey, obtained[i].StartKey)
		require.Equal(t, expected[i].EndKey, obtained[i].EndKey)
	}
}

func TestSortRange(t *testing.T) {
	dataRules := []*import_sstpb.RewriteRule{
		{OldKeyPrefix: tablecodec.GenTableRecordPrefix(1), NewKeyPrefix: tablecodec.GenTableRecordPrefix(4)},
		{OldKeyPrefix: tablecodec.GenTableRecordPrefix(2), NewKeyPrefix: tablecodec.GenTableRecordPrefix(5)},
	}
	rewriteRules := &RewriteRules{
		Data: dataRules,
	}
	ranges1 := []rtree.Range{
		{
			StartKey: append(tablecodec.GenTableRecordPrefix(1), []byte("aaa")...),
			EndKey:   append(tablecodec.GenTableRecordPrefix(1), []byte("bbb")...), Files: nil,
		},
	}
	for i, rg := range ranges1 {
		tmp, _ := RewriteRange(&rg, rewriteRules)
		ranges1[i] = *tmp
	}
	rs1, err := SortRanges(ranges1)
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
		_, err := RewriteRange(&rg, rewriteRules)
		require.Error(t, err)
		require.Regexp(t, "table id mismatch.*", err.Error())
	}

	ranges3 := initRanges()
	rewriteRules1 := initRewriteRules()
	for i, rg := range ranges3 {
		tmp, _ := RewriteRange(&rg, rewriteRules1)
		ranges3[i] = *tmp
	}
	rs3, err := SortRanges(ranges3)
	require.NoErrorf(t, err, "sort range1 failed: %v", err)
	rangeEquals(t, rs3, []rtree.Range{
		{StartKey: []byte("bbd"), EndKey: []byte("bbf"), Files: nil},
		{StartKey: []byte("bbf"), EndKey: []byte("bbj"), Files: nil},
		{StartKey: []byte("xxa"), EndKey: []byte("xxe"), Files: nil},
		{StartKey: []byte("xxe"), EndKey: []byte("xxz"), Files: nil},
	})
}
