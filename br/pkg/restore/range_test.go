// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"bytes"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/tablecodec"
)

type testRangeSuite struct{}

var _ = Suite(&testRangeSuite{})

type rangeEquals struct {
	*CheckerInfo
}

var RangeEquals Checker = &rangeEquals{
	&CheckerInfo{Name: "RangeEquals", Params: []string{"obtained", "expected"}},
}

func (checker *rangeEquals) Check(params []interface{}, names []string) (result bool, error string) {
	obtained := params[0].([]rtree.Range)
	expected := params[1].([]rtree.Range)
	if len(obtained) != len(expected) {
		return false, ""
	}
	for i := range obtained {
		if !bytes.Equal(obtained[i].StartKey, expected[i].StartKey) ||
			!bytes.Equal(obtained[i].EndKey, expected[i].EndKey) {
			return false, ""
		}
	}
	return true, ""
}

func (s *testRangeSuite) TestSortRange(c *C) {
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
	c.Assert(err, IsNil, Commentf("sort range1 failed: %v", err))
	c.Assert(rs1, RangeEquals, []rtree.Range{
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
	c.Assert(err, ErrorMatches, "table id mismatch.*")

	ranges3 := initRanges()
	rewriteRules1 := initRewriteRules()
	rs3, err := restore.SortRanges(ranges3, rewriteRules1)
	c.Assert(err, IsNil, Commentf("sort range1 failed: %v", err))
	c.Assert(rs3, RangeEquals, []rtree.Range{
		{StartKey: []byte("bbd"), EndKey: []byte("bbf"), Files: nil},
		{StartKey: []byte("bbf"), EndKey: []byte("bbj"), Files: nil},
		{StartKey: []byte("xxa"), EndKey: []byte("xxe"), Files: nil},
		{StartKey: []byte("xxe"), EndKey: []byte("xxz"), Files: nil},
	})
}
