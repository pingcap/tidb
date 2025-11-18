// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package rtree_test

import (
	"context"
	"fmt"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func newRange(start, end []byte) *rtree.Range {
	return &rtree.Range{
		KeyRange: rtree.KeyRange{
			StartKey: start,
			EndKey:   end,
		},
	}
}

func TestRangeTree(t *testing.T) {
	rangeTree := rtree.NewRangeTree()
	rg, _ := rangeTree.Get(newRange([]byte(""), []byte("")))
	require.Nil(t, rg)

	search := func(key []byte) *rtree.Range {
		rg, _ := rangeTree.Get(newRange(key, []byte("")))
		if rg == nil {
			return nil
		}
		return rg
	}
	assertIncomplete := func(startKey, endKey []byte, ranges []rtree.KeyRange) {
		incomplete := rangeTree.GetIncompleteRange(startKey, endKey)
		t.Logf("%#v %#v\n%#v\n%#v\n", startKey, endKey, incomplete, ranges)
		require.Equal(t, len(ranges), len(incomplete))
		for idx, rg := range incomplete {
			require.Equalf(t, ranges[idx].StartKey, rg.StartKey, "idx=%d", idx)
			require.Equalf(t, ranges[idx].EndKey, rg.EndKey, "idx=%d", idx)
		}
	}
	assertAllComplete := func() {
		for s := range 0xfe {
			for e := s + 1; e < 0xff; e++ {
				start := []byte{byte(s)}
				end := []byte{byte(e)}
				assertIncomplete(start, end, []rtree.KeyRange{})
			}
		}
	}

	assertIncomplete([]byte(""), []byte("b"), []rtree.KeyRange{{StartKey: []byte(""), EndKey: []byte("b")}})
	assertIncomplete([]byte(""), []byte(""), []rtree.KeyRange{{StartKey: []byte(""), EndKey: []byte("")}})
	assertIncomplete([]byte("b"), []byte(""), []rtree.KeyRange{{StartKey: []byte("b"), EndKey: []byte("")}})

	range0 := newRange([]byte(""), []byte("a"))
	rangeA := newRange([]byte("a"), []byte("b"))
	rangeB := newRange([]byte("b"), []byte("c"))
	rangeC := newRange([]byte("c"), []byte("d"))
	rangeD := newRange([]byte("d"), []byte(""))

	rangeTree.Update(*rangeA)
	require.Equal(t, 1, rangeTree.Len())
	assertIncomplete([]byte("a"), []byte("b"), []rtree.KeyRange{})
	assertIncomplete([]byte(""), []byte(""),
		[]rtree.KeyRange{
			{StartKey: []byte(""), EndKey: []byte("a")},
			{StartKey: []byte("b"), EndKey: []byte("")},
		})
	assertIncomplete([]byte("b"), []byte(""), []rtree.KeyRange{{StartKey: []byte("b"), EndKey: []byte("")}})

	rangeTree.Update(*rangeC)
	require.Equal(t, 2, rangeTree.Len())
	assertIncomplete([]byte("a"), []byte("c"), []rtree.KeyRange{
		{StartKey: []byte("b"), EndKey: []byte("c")},
	})
	assertIncomplete([]byte("b"), []byte("c"), []rtree.KeyRange{
		{StartKey: []byte("b"), EndKey: []byte("c")},
	})
	assertIncomplete([]byte(""), []byte(""),
		[]rtree.KeyRange{
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
		[]rtree.KeyRange{
			{StartKey: []byte(""), EndKey: []byte("a")},
			{StartKey: []byte("d"), EndKey: []byte("")},
		})

	rangeTree.Update(*rangeD)
	require.Equal(t, 4, rangeTree.Len())
	require.Equal(t, rangeD, search([]byte("d")))
	assertIncomplete([]byte(""), []byte(""), []rtree.KeyRange{
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
	assertIncomplete([]byte(""), []byte(""), []rtree.KeyRange{
		{StartKey: []byte("c"), EndKey: []byte("d")},
	})

	rangeTree.Update(*rangeC)
	require.Equal(t, 5, rangeTree.Len())
	assertAllComplete()
}

func TestRangeTreePutForce(t *testing.T) {
	type expectedRange struct {
		startKey []byte
		endKey   []byte
		filename string
	}
	check := func(t *testing.T, rangeTree rtree.RangeTree, expectedRanges []expectedRange) {
		require.Equal(t, rangeTree.Len(), len(expectedRanges))
		i := 0
		rangeTree.Ascend(func(item *rtree.Range) bool {
			require.Equal(t, expectedRanges[i].startKey, item.StartKey)
			require.Equal(t, expectedRanges[i].endKey, item.EndKey)
			require.Equal(t, expectedRanges[i].filename, item.Files[0].Name)
			i += 1
			return true
		})
	}
	rangeTree := rtree.NewRangeTree()
	require.True(t, rangeTree.PutForce([]byte("aa"), []byte("bb"), []*backuppb.File{{Name: "1.sst"}}, true))
	require.True(t, rangeTree.PutForce([]byte("ff"), []byte("hh"), []*backuppb.File{{Name: "2.sst"}}, false))
	check(t, rangeTree, []expectedRange{
		{startKey: []byte("aa"), endKey: []byte("bb"), filename: "1.sst"},
		{startKey: []byte("ff"), endKey: []byte("hh"), filename: "2.sst"},
	})

	// put with force
	require.True(t, rangeTree.PutForce([]byte("a"), []byte("ab"), []*backuppb.File{{Name: "3.sst"}}, true))
	check(t, rangeTree, []expectedRange{
		{startKey: []byte("a"), endKey: []byte("ab"), filename: "3.sst"},
		{startKey: []byte("ff"), endKey: []byte("hh"), filename: "2.sst"},
	})
	require.True(t, rangeTree.PutForce([]byte("aaa"), []byte("abc"), []*backuppb.File{{Name: "4.sst"}}, true))
	check(t, rangeTree, []expectedRange{
		{startKey: []byte("aaa"), endKey: []byte("abc"), filename: "4.sst"},
		{startKey: []byte("ff"), endKey: []byte("hh"), filename: "2.sst"},
	})
	require.True(t, rangeTree.PutForce([]byte("aaaa"), []byte("aaab"), []*backuppb.File{{Name: "5.sst"}}, true))
	check(t, rangeTree, []expectedRange{
		{startKey: []byte("aaaa"), endKey: []byte("aaab"), filename: "5.sst"},
		{startKey: []byte("ff"), endKey: []byte("hh"), filename: "2.sst"},
	})
	require.True(t, rangeTree.PutForce([]byte("aa"), []byte("bb"), []*backuppb.File{{Name: "6.sst"}}, true))
	check(t, rangeTree, []expectedRange{
		{startKey: []byte("aa"), endKey: []byte("bb"), filename: "6.sst"},
		{startKey: []byte("ff"), endKey: []byte("hh"), filename: "2.sst"},
	})

	// put without force
	require.False(t, rangeTree.PutForce([]byte("f"), []byte("fh"), []*backuppb.File{{Name: "7.sst"}}, false))
	check(t, rangeTree, []expectedRange{
		{startKey: []byte("aa"), endKey: []byte("bb"), filename: "6.sst"},
		{startKey: []byte("ff"), endKey: []byte("hh"), filename: "2.sst"},
	})
	require.False(t, rangeTree.PutForce([]byte("fff"), []byte("fhi"), []*backuppb.File{{Name: "8.sst"}}, false))
	check(t, rangeTree, []expectedRange{
		{startKey: []byte("aa"), endKey: []byte("bb"), filename: "6.sst"},
		{startKey: []byte("ff"), endKey: []byte("hh"), filename: "2.sst"},
	})
	require.False(t, rangeTree.PutForce([]byte("ffff"), []byte("fffh"), []*backuppb.File{{Name: "9.sst"}}, false))
	check(t, rangeTree, []expectedRange{
		{startKey: []byte("aa"), endKey: []byte("bb"), filename: "6.sst"},
		{startKey: []byte("ff"), endKey: []byte("hh"), filename: "2.sst"},
	})
	require.False(t, rangeTree.PutForce([]byte("ff"), []byte("hh"), []*backuppb.File{{Name: "10.sst"}}, false))
	check(t, rangeTree, []expectedRange{
		{startKey: []byte("aa"), endKey: []byte("bb"), filename: "6.sst"},
		{startKey: []byte("ff"), endKey: []byte("hh"), filename: "2.sst"},
	})

	// put with force bound
	require.True(t, rangeTree.PutForce([]byte("aa"), []byte("ab"), []*backuppb.File{{Name: "11.sst"}}, true))
	check(t, rangeTree, []expectedRange{
		{startKey: []byte("aa"), endKey: []byte("ab"), filename: "11.sst"},
		{startKey: []byte("ff"), endKey: []byte("hh"), filename: "2.sst"},
	})
	require.True(t, rangeTree.PutForce([]byte("aaa"), []byte("ab"), []*backuppb.File{{Name: "12.sst"}}, true))
	check(t, rangeTree, []expectedRange{
		{startKey: []byte("aaa"), endKey: []byte("ab"), filename: "12.sst"},
		{startKey: []byte("ff"), endKey: []byte("hh"), filename: "2.sst"},
	})

	// put without force bound
	require.False(t, rangeTree.PutForce([]byte("ff"), []byte("fh"), []*backuppb.File{{Name: "13.sst"}}, false))
	check(t, rangeTree, []expectedRange{
		{startKey: []byte("aaa"), endKey: []byte("ab"), filename: "12.sst"},
		{startKey: []byte("ff"), endKey: []byte("hh"), filename: "2.sst"},
	})
	require.False(t, rangeTree.PutForce([]byte("fh"), []byte("hh"), []*backuppb.File{{Name: "14.sst"}}, false))
	check(t, rangeTree, []expectedRange{
		{startKey: []byte("aaa"), endKey: []byte("ab"), filename: "12.sst"},
		{startKey: []byte("ff"), endKey: []byte("hh"), filename: "2.sst"},
	})

	// put outside bound
	require.True(t, rangeTree.PutForce([]byte("ab"), []byte("abc"), []*backuppb.File{{Name: "15.sst"}}, true))
	check(t, rangeTree, []expectedRange{
		{startKey: []byte("aaa"), endKey: []byte("ab"), filename: "12.sst"},
		{startKey: []byte("ab"), endKey: []byte("abc"), filename: "15.sst"},
		{startKey: []byte("ff"), endKey: []byte("hh"), filename: "2.sst"},
	})
	require.True(t, rangeTree.PutForce([]byte("aa"), []byte("aaa"), []*backuppb.File{{Name: "16.sst"}}, true))
	check(t, rangeTree, []expectedRange{
		{startKey: []byte("aa"), endKey: []byte("aaa"), filename: "16.sst"},
		{startKey: []byte("aaa"), endKey: []byte("ab"), filename: "12.sst"},
		{startKey: []byte("ab"), endKey: []byte("abc"), filename: "15.sst"},
		{startKey: []byte("ff"), endKey: []byte("hh"), filename: "2.sst"},
	})
	require.True(t, rangeTree.PutForce([]byte("hh"), []byte("hi"), []*backuppb.File{{Name: "17.sst"}}, false))
	check(t, rangeTree, []expectedRange{
		{startKey: []byte("aa"), endKey: []byte("aaa"), filename: "16.sst"},
		{startKey: []byte("aaa"), endKey: []byte("ab"), filename: "12.sst"},
		{startKey: []byte("ab"), endKey: []byte("abc"), filename: "15.sst"},
		{startKey: []byte("ff"), endKey: []byte("hh"), filename: "2.sst"},
		{startKey: []byte("hh"), endKey: []byte("hi"), filename: "17.sst"},
	})
	require.True(t, rangeTree.PutForce([]byte("ef"), []byte("ff"), []*backuppb.File{{Name: "18.sst"}}, false))
	check(t, rangeTree, []expectedRange{
		{startKey: []byte("aa"), endKey: []byte("aaa"), filename: "16.sst"},
		{startKey: []byte("aaa"), endKey: []byte("ab"), filename: "12.sst"},
		{startKey: []byte("ab"), endKey: []byte("abc"), filename: "15.sst"},
		{startKey: []byte("ef"), endKey: []byte("ff"), filename: "18.sst"},
		{startKey: []byte("ff"), endKey: []byte("hh"), filename: "2.sst"},
		{startKey: []byte("hh"), endKey: []byte("hi"), filename: "17.sst"},
	})
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
	for i := range b.N {
		item := rtree.Range{
			KeyRange: rtree.KeyRange{
				StartKey: fmt.Appendf(nil, "%20d", i),
				EndKey:   fmt.Appendf(nil, "%20d", i+1),
			},
		}
		rangeTree.Update(item)
	}
}

func encodeTableRecord(prefix kv.Key, rowID uint64) []byte {
	return tablecodec.EncodeRecordKey(prefix, kv.IntHandle(rowID))
}

func makeEncodeKeyspacedTableRecord(keyspace uint32) func(prefix kv.Key, rowID uint64) []byte {
	codec, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{
		Id:   keyspace,
		Name: "test",
	})
	if err != nil {
		panic(err)
	}
	return func(prefix kv.Key, rowID uint64) []byte {
		return codec.EncodeKey(tablecodec.EncodeRecordKey(prefix, kv.IntHandle(rowID)))
	}
}

func TestRangeTreeMerge(t0 *testing.T) {
	t0.Run("default-keyspace", func(t *testing.T) { testRangeTreeMerge(t, encodeTableRecord) })
	t0.Run("keyspaced", func(t *testing.T) {
		testRangeTreeMerge(t, makeEncodeKeyspacedTableRecord(1))
	})
}

func testRangeTreeMerge(t *testing.T, encodeTableRecord func(kv.Key, uint64) []byte) {
	rangeTree := rtree.NewRangeStatsTree()
	tablePrefix := tablecodec.GenTableRecordPrefix(1)
	for i := uint64(0); i < 10000; i += 1 {
		rangeTree.InsertRange(&rtree.Range{
			KeyRange: rtree.KeyRange{
				StartKey: encodeTableRecord(tablePrefix, i),
				EndKey:   encodeTableRecord(tablePrefix, i+1),
			},
			Files: []*backuppb.File{
				{
					Name:       fmt.Sprintf("%20d", i),
					TotalKvs:   1,
					TotalBytes: 1,
				},
			},
		}, i, 0)
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
		Origin: rtree.KeyRange{
			StartKey: []byte(startKey),
			EndKey:   []byte(endKey),
		},
	}
	return pr
}

func TestProgressRangeTree(t *testing.T) {
	prTree := rtree.NewProgressRangeTree(nil, false)

	require.NoError(t, prTree.Insert(buildProgressRange("aa", "cc")))
	require.Error(t, prTree.Insert(buildProgressRange("bb", "cc")))
	require.Error(t, prTree.Insert(buildProgressRange("bb", "dd")))
	require.NoError(t, prTree.Insert(buildProgressRange("cc", "dd")))
	require.NoError(t, prTree.Insert(buildProgressRange("ee", "ff")))

	ranges, err := prTree.GetIncompleteRanges()
	require.NoError(t, err)
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("aa"), EndKey: []byte("cc")}, ranges[0])
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("cc"), EndKey: []byte("dd")}, ranges[1])
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("ee"), EndKey: []byte("ff")}, ranges[2])

	pr, err := prTree.FindContained([]byte("aaa"), []byte("b"))
	require.NoError(t, err)
	pr.Res.Put([]byte("aaa"), []byte("b"), nil)

	pr, err = prTree.FindContained([]byte("cc"), []byte("dd"))
	require.NoError(t, err)
	pr.Res.Put([]byte("cc"), []byte("dd"), nil)

	ranges, err = prTree.GetIncompleteRanges()
	require.NoError(t, err)
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("aa"), EndKey: []byte("aaa")}, ranges[0])
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("b"), EndKey: []byte("cc")}, ranges[1])
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("ee"), EndKey: []byte("ff")}, ranges[2])

	pr, err = prTree.FindContained([]byte("aa"), []byte("aaa"))
	require.NoError(t, err)
	pr.Res.Put([]byte("aa"), []byte("aaa"), nil)

	pr, err = prTree.FindContained([]byte("b"), []byte("cc"))
	require.NoError(t, err)
	pr.Res.Put([]byte("b"), []byte("cc"), nil)

	pr, err = prTree.FindContained([]byte("ee"), []byte("ff"))
	require.NoError(t, err)
	pr.Res.Put([]byte("ee"), []byte("ff"), nil)

	ranges, err = prTree.GetIncompleteRanges()
	require.NoError(t, err)
	require.Equal(t, 0, len(ranges))
}

func TestProgreeRangeTreeCallBack(t *testing.T) {
	prTree := rtree.NewProgressRangeTree(nil, false)

	require.NoError(t, prTree.Insert(buildProgressRange("a", "b")))
	require.NoError(t, prTree.Insert(buildProgressRange("c", "d")))
	require.NoError(t, prTree.Insert(buildProgressRange("e", "f")))

	completeCount := 0
	prTree.SetCallBack(func() { completeCount += 1 })

	pr, err := prTree.FindContained([]byte("a"), []byte("b"))
	require.NoError(t, err)
	pr.Res.Put([]byte("a"), []byte("aa"), nil)
	ranges, err := prTree.GetIncompleteRanges()
	require.NoError(t, err)
	require.Equal(t, completeCount, 0)
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("aa"), EndKey: []byte("b")}, ranges[0])
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("c"), EndKey: []byte("d")}, ranges[1])
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("e"), EndKey: []byte("f")}, ranges[2])

	pr.Res.Put([]byte("a"), []byte("ab"), nil)
	ranges, err = prTree.GetIncompleteRanges()
	require.NoError(t, err)
	require.Equal(t, completeCount, 0)
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("ab"), EndKey: []byte("b")}, ranges[0])
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("c"), EndKey: []byte("d")}, ranges[1])
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("e"), EndKey: []byte("f")}, ranges[2])

	pr.Res.Put([]byte("ab"), []byte("b"), nil)
	ranges, err = prTree.GetIncompleteRanges()
	require.NoError(t, err)
	require.Equal(t, completeCount, 1)
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("c"), EndKey: []byte("d")}, ranges[0])
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("e"), EndKey: []byte("f")}, ranges[1])

	pr.Res.Put([]byte("a"), []byte("abc"), nil)
	ranges, err = prTree.GetIncompleteRanges()
	require.NoError(t, err)
	require.Equal(t, completeCount, 1)
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("c"), EndKey: []byte("d")}, ranges[0])
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("e"), EndKey: []byte("f")}, ranges[1])

	pr.Res.Put([]byte("cc"), []byte("cd"), nil)
	ranges, err = prTree.GetIncompleteRanges()
	require.NoError(t, err)
	require.Equal(t, completeCount, 1)
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("c"), EndKey: []byte("d")}, ranges[0])
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("e"), EndKey: []byte("f")}, ranges[1])
}

func buildProgressRangeWithPhysicalID(startKey, endKey string, physicalID int64) *rtree.ProgressRange {
	pr := &rtree.ProgressRange{
		Res: rtree.NewRangeTree(),
		Origin: rtree.KeyRange{
			StartKey: []byte(startKey),
			EndKey:   []byte(endKey),
		},
	}
	pr.Res.PhysicalID = physicalID
	return pr
}

func getFiles(startKey, endKey []byte, checksums [][3]uint64) []*backuppb.File {
	files := make([]*backuppb.File, 0, len(checksums))
	for _, checksum := range checksums {
		files = append(files, &backuppb.File{
			StartKey:   startKey,
			EndKey:     endKey,
			Crc64Xor:   checksum[0],
			TotalKvs:   checksum[1],
			TotalBytes: checksum[2],
		})
	}
	return files
}

func TestProgreeRangeTreeCallBack2(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	stg, err := storage.NewLocalStorage(base)
	require.NoError(t, err)
	metawriter := metautil.NewMetaWriter(stg, metautil.MetaFileSize, false, "temp.meta", nil)
	metawriter.StartWriteMetasAsync(ctx, metautil.AppendDataFile)
	defer func() {
		require.NoError(t, metawriter.FinishWriteMetas(ctx, metautil.AppendDataFile))
	}()
	prTree := rtree.NewProgressRangeTree(metawriter, false)

	require.NoError(t, prTree.Insert(buildProgressRangeWithPhysicalID("a", "b", 1)))
	require.NoError(t, prTree.Insert(buildProgressRangeWithPhysicalID("c", "d", 2)))
	require.NoError(t, prTree.Insert(buildProgressRangeWithPhysicalID("e", "f", 3)))

	completeCount := 0
	prTree.SetCallBack(func() { completeCount += 1 })

	pr, err := prTree.FindContained([]byte("a"), []byte("b"))
	require.NoError(t, err)
	pr.Res.Put([]byte("a"), []byte("aa"), getFiles([]byte("a"), []byte("aa"), [][3]uint64{{1, 1, 1}, {2, 2, 2}}))
	ranges, err := prTree.GetIncompleteRanges()
	require.NoError(t, err)
	require.Equal(t, completeCount, 0)
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("aa"), EndKey: []byte("b")}, ranges[0])
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("c"), EndKey: []byte("d")}, ranges[1])
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("e"), EndKey: []byte("f")}, ranges[2])

	pr.Res.Put([]byte("a"), []byte("ab"), getFiles([]byte("a"), []byte("ab"), [][3]uint64{{3, 3, 3}, {4, 4, 4}}))
	ranges, err = prTree.GetIncompleteRanges()
	require.NoError(t, err)
	require.Equal(t, completeCount, 0)
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("ab"), EndKey: []byte("b")}, ranges[0])
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("c"), EndKey: []byte("d")}, ranges[1])
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("e"), EndKey: []byte("f")}, ranges[2])

	pr.Res.Put([]byte("ab"), []byte("b"), getFiles([]byte("ab"), []byte("b"), [][3]uint64{{5, 5, 5}, {6, 6, 6}}))
	ranges, err = prTree.GetIncompleteRanges()
	require.NoError(t, err)
	require.Equal(t, completeCount, 1)
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("c"), EndKey: []byte("d")}, ranges[0])
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("e"), EndKey: []byte("f")}, ranges[1])
	cksm := prTree.GetChecksumMap()
	require.Len(t, cksm, 1)
	checksum := cksm[1]
	require.Equal(t, uint64(3^4^5^6), checksum.Crc64Xor)
	require.Equal(t, uint64(3+4+5+6), checksum.TotalKvs)
	require.Equal(t, uint64(3+4+5+6), checksum.TotalBytes)

	pr.Res.Put([]byte("a"), []byte("abc"), nil)
	ranges, err = prTree.GetIncompleteRanges()
	require.NoError(t, err)
	require.Equal(t, completeCount, 1)
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("c"), EndKey: []byte("d")}, ranges[0])
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("e"), EndKey: []byte("f")}, ranges[1])

	pr.Res.Put([]byte("cc"), []byte("cd"), nil)
	ranges, err = prTree.GetIncompleteRanges()
	require.NoError(t, err)
	require.Equal(t, completeCount, 1)
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("c"), EndKey: []byte("d")}, ranges[0])
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("e"), EndKey: []byte("f")}, ranges[1])
}
