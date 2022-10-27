// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package rtree_test

import (
	"fmt"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/rtree"
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

func TestRangeSplitIntersect(t *testing.T) {
	/* tree structure:
	 * +------+   +------+   +-----+   +-----+
	 * b      d   f      i   j     m   p     t
	 */
	keysMap := map[string]rtree.Range{
		"b": {
			StartKey: []byte("b"),
			EndKey:   []byte("d"),
			Files:    []*backuppb.File{{Name: "b-d"}},
		},
		"j": {
			StartKey: []byte("j"),
			EndKey:   []byte("m"),
			Files:    []*backuppb.File{{Name: "j-m"}},
		},
		"f": {
			StartKey: []byte("f"),
			EndKey:   []byte("i"),
			Files:    []*backuppb.File{{Name: "f-i"}},
		},
		"p": {
			StartKey: []byte("p"),
			EndKey:   []byte("t"),
			Files:    []*backuppb.File{{Name: "p-t"}},
		},
	}

	tree := rtree.NewRangeTree()
	for _, v := range keysMap {
		tree.Put(v.StartKey, v.EndKey, v.Files)
	}

	cases := []struct {
		startKey   []byte
		endKey     []byte
		incomplete []rtree.Range
		existKeys  []string
	}{
		/* tree structure:
		 *   b     d   f      i   j     m   p     t
		 *   +-----+   +------+   +-----+   +-----+
		 * +---------+
		 * a         e
		 */
		{
			startKey: []byte("a"),
			endKey:   []byte("e"),
			incomplete: []rtree.Range{
				{StartKey: []byte("a"), EndKey: []byte("b")},
				{StartKey: []byte("d"), EndKey: []byte("e")},
			},
			existKeys: []string{"b"},
		},

		/* tree structure:
		 *   b     d   f      i   j     m   p    t
		 *   +-----+   +------+   +-----+   +----+
		 * +------------------------------+
		 * a                              n
		 */
		{
			startKey: []byte("a"),
			endKey:   []byte("n"),
			incomplete: []rtree.Range{
				{StartKey: []byte("a"), EndKey: []byte("b")},
				{StartKey: []byte("d"), EndKey: []byte("f")},
				{StartKey: []byte("i"), EndKey: []byte("j")},
				{StartKey: []byte("m"), EndKey: []byte("n")},
			},
			existKeys: []string{"b", "f", "j"},
		},

		/* tree structure:
		 *  b     d   f      i   j     m   p    t
		 *  +-----+   +------+   +-----+   +----+
		 *          +----------------------------+
		 *          e                            u
		 */
		{
			startKey: []byte("e"),
			endKey:   []byte("u"),
			incomplete: []rtree.Range{
				{StartKey: []byte("e"), EndKey: []byte("f")},
				{StartKey: []byte("i"), EndKey: []byte("j")},
				{StartKey: []byte("m"), EndKey: []byte("p")},
				{StartKey: []byte("t"), EndKey: []byte("u")},
			},
			existKeys: []string{"f", "j", "p"},
		},

		/* tree structure:
		 *  b     d   f      i   j     m   p    t
		 *  +-----+   +------+   +-----+   +----+
		 *            +--------------------------+
		 *            f                          u
		 */
		{
			startKey: []byte("f"),
			endKey:   []byte("u"),
			incomplete: []rtree.Range{
				{StartKey: []byte("i"), EndKey: []byte("j")},
				{StartKey: []byte("m"), EndKey: []byte("p")},
				{StartKey: []byte("t"), EndKey: []byte("u")},
			},
			existKeys: []string{"f", "j", "p"},
		},

		/* tree structure:
		 *  b     d   f      i   j     m
		 *  +-----+   +------+   +-----+
		 *          +------------------+
		 *          e                  m
		 */
		{
			startKey: []byte("e"),
			endKey:   []byte("m"),
			incomplete: []rtree.Range{
				{StartKey: []byte("e"), EndKey: []byte("f")},
				{StartKey: []byte("i"), EndKey: []byte("j")},
			},
			existKeys: []string{"f", "j"},
		},

		/* tree structure:
		 *  b     d   f      i   j     m
		 *  +-----+   +------+   +-----+
		 *          +--------+
		 *          e        i
		 */
		{
			startKey: []byte("e"),
			endKey:   []byte("i"),
			incomplete: []rtree.Range{
				{StartKey: []byte("e"), EndKey: []byte("f")},
			},
			existKeys: []string{"f"},
		},

		/* tree structure:
		 *  b     d   f      i   j     m
		 *  +-----+   +------+   +-----+
		 *          +------------+
		 *          e            j
		 */
		{
			startKey: []byte("e"),
			endKey:   []byte("j"),
			incomplete: []rtree.Range{
				{StartKey: []byte("e"), EndKey: []byte("f")},
				{StartKey: []byte("i"), EndKey: []byte("j")},
			},
			existKeys: []string{"f"},
		},

		/* tree structure:
		 *  b     d   f      i   j     m   p    t
		 *  +-----+   +------+   +-----+   +----+
		 *            +----------------+
		 *            f                m
		 */
		{
			startKey: []byte("f"),
			endKey:   []byte("m"),
			incomplete: []rtree.Range{
				{StartKey: []byte("i"), EndKey: []byte("j")},
			},
			existKeys: []string{"f", "j"},
		},
	}

	for _, cs := range cases {
		pr := &rtree.ProgressRange{
			Res: rtree.NewRangeTree(),
			Origin: rtree.Range{
				StartKey: cs.startKey,
				EndKey:   cs.endKey,
			},
		}
		tree.SplitIncompleteRange(pr)
		require.Equal(t, len(cs.incomplete), len(pr.Incomplete))
		for i, rg := range cs.incomplete {
			require.Equal(t, pr.Incomplete[i].StartKey, rg.StartKey)
			require.Equal(t, pr.Incomplete[i].EndKey, rg.EndKey)
		}

		require.Equal(t, pr.Res.Len(), len(cs.existKeys))
		for _, ks := range cs.existKeys {
			rg := keysMap[ks]
			item := pr.Res.Find(&rtree.Range{StartKey: []byte(ks)})
			require.Equal(t, item.EndKey, rg.EndKey)
			require.Equal(t, item.Files[0].Name, rg.Files[0].Name)
		}
	}

}
