// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package rtree_test

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/br/pkg/rtree"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testRangeTreeSuite{})

type testRangeTreeSuite struct{}

func newRange(start, end []byte) *rtree.Range {
	return &rtree.Range{
		StartKey: start,
		EndKey:   end,
	}
}

func (s *testRangeTreeSuite) TestRangeTree(c *C) {
	rangeTree := rtree.NewRangeTree()
	c.Assert(rangeTree.Get(newRange([]byte(""), []byte(""))), IsNil)

	search := func(key []byte) *rtree.Range {
		rg := rangeTree.Get(newRange(key, []byte("")))
		if rg == nil {
			return nil
		}
		return rg.(*rtree.Range)
	}
	assertIncomplete := func(startKey, endKey []byte, ranges []rtree.Range) {
		incomplete := rangeTree.GetIncompleteRange(startKey, endKey)
		c.Logf("%#v %#v\n%#v\n%#v\n", startKey, endKey, incomplete, ranges)
		c.Assert(len(incomplete), Equals, len(ranges))
		for idx, rg := range incomplete {
			c.Assert(rg.StartKey, DeepEquals, ranges[idx].StartKey)
			c.Assert(rg.EndKey, DeepEquals, ranges[idx].EndKey)
		}
	}
	assertAllComplete := func() {
		for s := 0; s < 0xfe; s++ {
			for e := s + 1; e < 0xff; e++ {
				start := []byte{byte(s)}
				end := []byte{byte(e)}
				assertIncomplete(start, end, []rtree.Range{})
			}
		}
	}

	range0 := newRange([]byte(""), []byte("a"))
	rangeA := newRange([]byte("a"), []byte("b"))
	rangeB := newRange([]byte("b"), []byte("c"))
	rangeC := newRange([]byte("c"), []byte("d"))
	rangeD := newRange([]byte("d"), []byte(""))

	rangeTree.Update(*rangeA)
	c.Assert(rangeTree.Len(), Equals, 1)
	assertIncomplete([]byte("a"), []byte("b"), []rtree.Range{})
	assertIncomplete([]byte(""), []byte(""),
		[]rtree.Range{
			{StartKey: []byte(""), EndKey: []byte("a")},
			{StartKey: []byte("b"), EndKey: []byte("")},
		})

	rangeTree.Update(*rangeC)
	c.Assert(rangeTree.Len(), Equals, 2)
	assertIncomplete([]byte("a"), []byte("c"), []rtree.Range{
		{StartKey: []byte("b"), EndKey: []byte("c")},
	})
	assertIncomplete([]byte("b"), []byte("c"), []rtree.Range{
		{StartKey: []byte("b"), EndKey: []byte("c")},
	})
	assertIncomplete([]byte(""), []byte(""),
		[]rtree.Range{
			{StartKey: []byte(""), EndKey: []byte("a")},
			{StartKey: []byte("b"), EndKey: []byte("c")},
			{StartKey: []byte("d"), EndKey: []byte("")},
		})

	c.Assert(search([]byte{}), IsNil)
	c.Assert(search([]byte("a")), DeepEquals, rangeA)
	c.Assert(search([]byte("b")), IsNil)
	c.Assert(search([]byte("c")), DeepEquals, rangeC)
	c.Assert(search([]byte("d")), IsNil)

	rangeTree.Update(*rangeB)
	c.Assert(rangeTree.Len(), Equals, 3)
	c.Assert(search([]byte("b")), DeepEquals, rangeB)
	assertIncomplete([]byte(""), []byte(""),
		[]rtree.Range{
			{StartKey: []byte(""), EndKey: []byte("a")},
			{StartKey: []byte("d"), EndKey: []byte("")},
		})

	rangeTree.Update(*rangeD)
	c.Assert(rangeTree.Len(), Equals, 4)
	c.Assert(search([]byte("d")), DeepEquals, rangeD)
	assertIncomplete([]byte(""), []byte(""), []rtree.Range{
		{StartKey: []byte(""), EndKey: []byte("a")},
	})

	// None incomplete for any range after insert range 0
	rangeTree.Update(*range0)
	c.Assert(rangeTree.Len(), Equals, 5)

	// Overwrite range B and C.
	rangeBD := newRange([]byte("b"), []byte("d"))
	rangeTree.Update(*rangeBD)
	c.Assert(rangeTree.Len(), Equals, 4)
	assertAllComplete()

	// Overwrite range BD, c-d should be empty
	rangeTree.Update(*rangeB)
	c.Assert(rangeTree.Len(), Equals, 4)
	assertIncomplete([]byte(""), []byte(""), []rtree.Range{
		{StartKey: []byte("c"), EndKey: []byte("d")},
	})

	rangeTree.Update(*rangeC)
	c.Assert(rangeTree.Len(), Equals, 5)
	assertAllComplete()
}

func (s *testRangeTreeSuite) TestRangeIntersect(c *C) {
	rg := newRange([]byte("a"), []byte("c"))

	start, end, isIntersect := rg.Intersect([]byte(""), []byte(""))
	c.Assert(isIntersect, Equals, true)
	c.Assert(start, DeepEquals, []byte("a"))
	c.Assert(end, DeepEquals, []byte("c"))

	start, end, isIntersect = rg.Intersect([]byte(""), []byte("a"))
	c.Assert(isIntersect, Equals, false)
	c.Assert(start, DeepEquals, []byte(nil))
	c.Assert(end, DeepEquals, []byte(nil))

	start, end, isIntersect = rg.Intersect([]byte(""), []byte("b"))
	c.Assert(isIntersect, Equals, true)
	c.Assert(start, DeepEquals, []byte("a"))
	c.Assert(end, DeepEquals, []byte("b"))

	start, end, isIntersect = rg.Intersect([]byte("a"), []byte("b"))
	c.Assert(isIntersect, Equals, true)
	c.Assert(start, DeepEquals, []byte("a"))
	c.Assert(end, DeepEquals, []byte("b"))

	start, end, isIntersect = rg.Intersect([]byte("aa"), []byte("b"))
	c.Assert(isIntersect, Equals, true)
	c.Assert(start, DeepEquals, []byte("aa"))
	c.Assert(end, DeepEquals, []byte("b"))

	start, end, isIntersect = rg.Intersect([]byte("b"), []byte("c"))
	c.Assert(isIntersect, Equals, true)
	c.Assert(start, DeepEquals, []byte("b"))
	c.Assert(end, DeepEquals, []byte("c"))

	start, end, isIntersect = rg.Intersect([]byte(""), []byte{1})
	c.Assert(isIntersect, Equals, false)
	c.Assert(start, DeepEquals, []byte(nil))
	c.Assert(end, DeepEquals, []byte(nil))

	start, end, isIntersect = rg.Intersect([]byte("c"), []byte(""))
	c.Assert(isIntersect, Equals, false)
	c.Assert(start, DeepEquals, []byte(nil))
	c.Assert(end, DeepEquals, []byte(nil))
}

func BenchmarkRangeTreeUpdate(b *testing.B) {
	rangeTree := rtree.NewRangeTree()
	for i := 0; i < b.N; i++ {
		item := rtree.Range{
			StartKey: []byte(fmt.Sprintf("%20d", i)),
			EndKey:   []byte(fmt.Sprintf("%20d", i+1)),
		}
		rangeTree.Update(item)
	}
}
