// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/tablecodec"
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
	rewriteRules := &restore.RewriteRules{
		Data: dataRules,
	}
	ranges1 := []rtree.Range{
		{
			StartKey: append(tablecodec.GenTableRecordPrefix(1), []byte("aaa")...),
			EndKey:   append(tablecodec.GenTableRecordPrefix(1), []byte("bbb")...), Files: nil,
		},
	}
	rs1, err := restore.SortRanges(ranges1, rewriteRules)
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
	_, err = restore.SortRanges(ranges2, rewriteRules)
	require.Error(t, err)
	require.Regexp(t, "table id mismatch.*", err.Error())

	ranges3 := initRanges()
	rewriteRules1 := initRewriteRules()
	rs3, err := restore.SortRanges(ranges3, rewriteRules1)
	require.NoErrorf(t, err, "sort range1 failed: %v", err)
	rangeEquals(t, rs3, []rtree.Range{
		{StartKey: []byte("bbd"), EndKey: []byte("bbf"), Files: nil},
		{StartKey: []byte("bbf"), EndKey: []byte("bbj"), Files: nil},
		{StartKey: []byte("xxa"), EndKey: []byte("xxe"), Files: nil},
		{StartKey: []byte("xxe"), EndKey: []byte("xxz"), Files: nil},
	})
}
