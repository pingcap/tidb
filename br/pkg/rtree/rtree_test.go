// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package rtree_test

import (
	"fmt"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/stretchr/testify/require"
)

func newRange(start, end []byte) *rtree.Range {
	return &rtree.Range{
		StartKey: start,
		EndKey:   end,
	}
}

func TestRangeTree(t *testing.T) {
	rangeTree := rtree.NewRangeTree()
	require.Nil(t, rangeTree.Get(newRange([]byte(""), []byte(""))))

	search := func(key []byte) *rtree.Range {
		rg := rangeTree.Get(newRange(key, []byte("")))
		if rg == nil {
			return nil
		}
		return rg.(*rtree.Range)
	}
	assertIncomplete := func(startKey, endKey []byte, ranges []rtree.Range) {
		incomplete := rangeTree.GetIncompleteRange(startKey, endKey)
		t.Logf("%#v %#v\n%#v\n%#v\n", startKey, endKey, incomplete, ranges)
		require.Equal(t, len(ranges), len(incomplete))
		for idx, rg := range incomplete {
			require.Equalf(t, ranges[idx].StartKey, rg.StartKey, "idx=%d", idx)
			require.Equalf(t, ranges[idx].EndKey, rg.EndKey, "idx=%d", idx)
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

	assertIncomplete([]byte(""), []byte("b"), []rtree.Range{{StartKey: []byte(""), EndKey: []byte("b")}})
	assertIncomplete([]byte(""), []byte(""), []rtree.Range{{StartKey: []byte(""), EndKey: []byte("")}})
	assertIncomplete([]byte("b"), []byte(""), []rtree.Range{{StartKey: []byte("b"), EndKey: []byte("")}})

	range0 := newRange([]byte(""), []byte("a"))
	rangeA := newRange([]byte("a"), []byte("b"))
	rangeB := newRange([]byte("b"), []byte("c"))
	rangeC := newRange([]byte("c"), []byte("d"))
	rangeD := newRange([]byte("d"), []byte(""))

	rangeTree.Update(*rangeA)
	require.Equal(t, 1, rangeTree.Len())
	assertIncomplete([]byte("a"), []byte("b"), []rtree.Range{})
	assertIncomplete([]byte(""), []byte(""),
		[]rtree.Range{
			{StartKey: []byte(""), EndKey: []byte("a")},
			{StartKey: []byte("b"), EndKey: []byte("")},
		})
	assertIncomplete([]byte("b"), []byte(""), []rtree.Range{{StartKey: []byte("b"), EndKey: []byte("")}})

	rangeTree.Update(*rangeC)
	require.Equal(t, 2, rangeTree.Len())
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

	require.Nil(t, search([]byte{}))
	require.Equal(t, rangeA, search([]byte("a")))
	require.Nil(t, search([]byte("b")))
	require.Equal(t, rangeC, search([]byte("c")))
	require.Nil(t, search([]byte("d")))

	rangeTree.Update(*rangeB)
	require.Equal(t, 3, rangeTree.Len())
	require.Equal(t, rangeB, search([]byte("b")))
	assertIncomplete([]byte(""), []byte(""),
		[]rtree.Range{
			{StartKey: []byte(""), EndKey: []byte("a")},
			{StartKey: []byte("d"), EndKey: []byte("")},
		})

	rangeTree.Update(*rangeD)
	require.Equal(t, 4, rangeTree.Len())
	require.Equal(t, rangeD, search([]byte("d")))
	assertIncomplete([]byte(""), []byte(""), []rtree.Range{
		{StartKey: []byte(""), EndKey: []byte("a")},
	})

	// None incomplete for any range after insert range 0
	rangeTree.Update(*range0)
	require.Equal(t, 5, rangeTree.Len())

	// Overwrite range B and C.
	rangeBD := newRange([]byte("b"), []byte("d"))
	rangeTree.Update(*rangeBD)
	require.Equal(t, 4, rangeTree.Len())
	assertAllComplete()

	// Overwrite range BD, c-d should be empty
	rangeTree.Update(*rangeB)
	require.Equal(t, 4, rangeTree.Len())
	assertIncomplete([]byte(""), []byte(""), []rtree.Range{
		{StartKey: []byte("c"), EndKey: []byte("d")},
	})

	rangeTree.Update(*rangeC)
	require.Equal(t, 5, rangeTree.Len())
	assertAllComplete()
}

func TestRangeIntersect(t *testing.T) {
	rg := newRange([]byte("a"), []byte("c"))

	start, end, isIntersect := rg.Intersect([]byte(""), []byte(""))
	require.True(t, isIntersect)
	require.Equal(t, []byte("a"), start)
	require.Equal(t, []byte("c"), end)

	start, end, isIntersect = rg.Intersect([]byte(""), []byte("a"))
	require.False(t, isIntersect)
	require.Equal(t, []byte(nil), start)
	require.Equal(t, []byte(nil), end)

	start, end, isIntersect = rg.Intersect([]byte(""), []byte("b"))
	require.True(t, isIntersect)
	require.Equal(t, []byte("a"), start)
	require.Equal(t, []byte("b"), end)

	start, end, isIntersect = rg.Intersect([]byte("a"), []byte("b"))
	require.True(t, isIntersect)
	require.Equal(t, []byte("a"), start)
	require.Equal(t, []byte("b"), end)

	start, end, isIntersect = rg.Intersect([]byte("aa"), []byte("b"))
	require.True(t, isIntersect)
	require.Equal(t, []byte("aa"), start)
	require.Equal(t, []byte("b"), end)

	start, end, isIntersect = rg.Intersect([]byte("b"), []byte("c"))
	require.True(t, isIntersect)
	require.Equal(t, []byte("b"), start)
	require.Equal(t, []byte("c"), end)

	start, end, isIntersect = rg.Intersect([]byte(""), []byte{1})
	require.False(t, isIntersect)
	require.Equal(t, []byte(nil), start)
	require.Equal(t, []byte(nil), end)

	start, end, isIntersect = rg.Intersect([]byte("c"), []byte(""))
	require.False(t, isIntersect)
	require.Equal(t, []byte(nil), start)
	require.Equal(t, []byte(nil), end)
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

func encodeTableRecord(prefix kv.Key, rowID uint64) []byte {
	return tablecodec.EncodeRecordKey(prefix, kv.IntHandle(rowID))
}

func TestRangeTreeMerge(t *testing.T) {
	rangeTree := rtree.NewRangeTree()
	tablePrefix := tablecodec.GenTableRecordPrefix(1)
	for i := uint64(0); i < 10000; i += 1 {
		item := rtree.Range{
			StartKey: encodeTableRecord(tablePrefix, i),
			EndKey:   encodeTableRecord(tablePrefix, i+1),
			Files: []*backuppb.File{
				{
					Name:       fmt.Sprintf("%20d", i),
					TotalKvs:   1,
					TotalBytes: 1,
				},
			},
			Size: i,
		}
		rangeTree.Update(item)
	}
	sortedRanges := rangeTree.MergedRanges(10, 10)
	require.Equal(t, 1000, len(sortedRanges))
	for i, rg := range sortedRanges {
		require.Equal(t, encodeTableRecord(tablePrefix, uint64(i)*10), rg.StartKey)
		require.Equal(t, encodeTableRecord(tablePrefix, uint64(i+1)*10), rg.EndKey)
		require.Equal(t, uint64(i*10*10+45), rg.Size)
		require.Equal(t, 10, len(rg.Files))
		for j, file := range rg.Files {
			require.Equal(t, fmt.Sprintf("%20d", i*10+j), file.Name)
			require.Equal(t, uint64(1), file.TotalKvs)
			require.Equal(t, uint64(1), file.TotalBytes)
		}
	}
}

func buildProgressRange(startKey, endKey string) *rtree.ProgressRange {
	pr := &rtree.ProgressRange{
		Res: rtree.NewRangeTree(),
		Origin: rtree.Range{
			StartKey: []byte(startKey),
			EndKey:   []byte(endKey),
		},
	}
	return pr
}

func TestProgressRangeTree(t *testing.T) {
	prTree := rtree.NewProgressRangeTree()

	require.NoError(t, prTree.Insert(buildProgressRange("aa", "cc")))
	require.Error(t, prTree.Insert(buildProgressRange("bb", "cc")))
	require.Error(t, prTree.Insert(buildProgressRange("bb", "dd")))
	require.NoError(t, prTree.Insert(buildProgressRange("cc", "dd")))
	require.NoError(t, prTree.Insert(buildProgressRange("ee", "ff")))

	prIter := prTree.Iter()
	ranges := prIter.GetIncompleteRanges()
	require.Equal(t, rtree.Range{StartKey: []byte("aa"), EndKey: []byte("cc")}, ranges[0])
	require.Equal(t, rtree.Range{StartKey: []byte("cc"), EndKey: []byte("dd")}, ranges[1])
	require.Equal(t, rtree.Range{StartKey: []byte("ee"), EndKey: []byte("ff")}, ranges[2])

	pr, err := prTree.FindContained([]byte("aaa"), []byte("b"))
	require.NoError(t, err)
	pr.Res.Put([]byte("aaa"), []byte("b"), nil)

	pr, err = prTree.FindContained([]byte("cc"), []byte("dd"))
	require.NoError(t, err)
	pr.Res.Put([]byte("cc"), []byte("dd"), nil)

	ranges = prIter.GetIncompleteRanges()
	require.Equal(t, rtree.Range{StartKey: []byte("aa"), EndKey: []byte("aaa")}, ranges[0])
	require.Equal(t, rtree.Range{StartKey: []byte("b"), EndKey: []byte("cc")}, ranges[1])
	require.Equal(t, rtree.Range{StartKey: []byte("ee"), EndKey: []byte("ff")}, ranges[2])

	pr, err = prTree.FindContained([]byte("aa"), []byte("aaa"))
	require.NoError(t, err)
	pr.Res.Put([]byte("aa"), []byte("aaa"), nil)

	pr, err = prTree.FindContained([]byte("b"), []byte("cc"))
	require.NoError(t, err)
	pr.Res.Put([]byte("b"), []byte("cc"), nil)

	pr, err = prTree.FindContained([]byte("ee"), []byte("ff"))
	require.NoError(t, err)
	pr.Res.Put([]byte("ee"), []byte("ff"), nil)

	ranges = prIter.GetIncompleteRanges()
	require.Equal(t, 0, len(ranges))
}
