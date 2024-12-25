// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package rtree_test

import (
	"fmt"
	"slices"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
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
	rangeTree.UpdateForce(*rangeB, true)
	require.Equal(t, 4, rangeTree.Len())
	assertIncomplete([]byte(""), []byte(""), []rtree.Range{
		{StartKey: []byte("c"), EndKey: []byte("d")},
	})

	rangeTree.Update(*rangeC)
	require.Equal(t, 5, rangeTree.Len())
	assertAllComplete()
}

func TestRangeTree2(t *testing.T) {
	rangeTree := rtree.NewRangeTreeWithPhysicalID(100)

	newRangeItem := func(tableID int64, rowStartID, rowEndID uint64, name string, physicalIDs ...int64) ([]byte, []byte, []*backuppb.File) {
		startKey := encodeTableRowKey(tableID, rowStartID)
		endKey := encodeTableRowKey(tableID, rowEndID)
		tableMetas := make([]*backuppb.TableMeta, 0, len(physicalIDs))
		for _, physicalID := range physicalIDs {
			tableMetas = append(tableMetas, &backuppb.TableMeta{
				PhysicalId: physicalID,
				Crc64Xor:   uint64(physicalID),
				TotalKvs:   uint64(physicalID),
				TotalBytes: uint64(physicalID),
			})
		}
		return startKey, endKey, []*backuppb.File{
			{
				Name:       name,
				StartKey:   startKey,
				EndKey:     endKey,
				TableMetas: tableMetas,
			},
		}
	}
	startKey, endKey, files1 := newRangeItem(100, 1, 100, "1.sst", 98, 99, 100)
	force := rangeTree.PutForce(startKey, endKey, files1, false)
	require.True(t, force)
	startKey, endKey, files2 := newRangeItem(100, 200, 300, "2.sst", 100)
	force = rangeTree.PutForce(startKey, endKey, files2, false)
	require.True(t, force)
	startKey, endKey, files3 := newRangeItem(100, 500, 600, "3.sst", 100, 101, 102)
	force = rangeTree.PutForce(startKey, endKey, files3, false)
	require.True(t, force)

	// [200, 300] -overlap-> [200, 300]
	startKey, endKey, files := newRangeItem(100, 200, 300, "4.sst", 100)
	force = rangeTree.PutForce(startKey, endKey, files, false)
	require.False(t, force)

	// [200, 300] -overlap-> [200, 300]
	startKey, endKey, files = newRangeItem(100, 200, 300, "4.sst", 100)
	force = rangeTree.PutForce(startKey, endKey, files, true)
	require.True(t, force)
	rg := rangeTree.Get(newRange(startKey, endKey)).(*rtree.Range)
	require.Len(t, rg.Files, 1)
	require.Equal(t, rg.Files[0].Name, "4.sst")

	// [150, 300] -overlap-> [200, 300]
	startKey, endKey, files = newRangeItem(100, 150, 300, "5.sst", 100)
	force = rangeTree.PutForce(startKey, endKey, files, false)
	require.True(t, force)
	rg = rangeTree.Get(newRange(startKey, endKey)).(*rtree.Range)
	require.Len(t, rg.Files, 1)
	require.Equal(t, rg.Files[0].Name, "5.sst")

	// [150, 350] -overlap-> [150, 300]
	startKey, endKey, files = newRangeItem(100, 150, 350, "6.sst", 100)
	force = rangeTree.PutForce(startKey, endKey, files, false)
	require.True(t, force)
	rg = rangeTree.Get(newRange(startKey, endKey)).(*rtree.Range)
	require.Len(t, rg.Files, 1)
	require.Equal(t, rg.Files[0].Name, "6.sst")

	// [140, 400] -overlap-> [150, 350]
	startKey, endKey, files = newRangeItem(100, 140, 400, "7.sst", 100)
	force = rangeTree.PutForce(startKey, endKey, files, false)
	require.True(t, force)
	rg = rangeTree.Get(newRange(startKey, endKey)).(*rtree.Range)
	require.Len(t, rg.Files, 1)
	require.Equal(t, rg.Files[0].Name, "7.sst")

	// [1, 100], [140, 400], [500, 600]
	// [1, 110] -overlap-> [1, 100]
	startKey, endKey, files = newRangeItem(100, 1, 110, "8.sst", 100)
	force = rangeTree.PutForce(startKey, endKey, files, false)
	require.True(t, force)
	rg = rangeTree.Get(newRange(startKey, endKey)).(*rtree.Range)
	require.Len(t, rg.Files, 1)
	require.Equal(t, rg.Files[0].Name, "8.sst")
	require.Len(t, files1[0].TableMetas, 3)
	require.Equal(t, files1[0].TableMetas[0].PhysicalId, int64(98))
	require.Equal(t, files1[0].TableMetas[1].PhysicalId, int64(99))
	require.Nil(t, files1[0].TableMetas[2])

	// [450, 600] -overlap-> [500, 600]
	startKey, endKey, files = newRangeItem(100, 450, 600, "9.sst", 100)
	force = rangeTree.PutForce(startKey, endKey, files, false)
	require.True(t, force)
	rg = rangeTree.Get(newRange(startKey, endKey)).(*rtree.Range)
	require.Len(t, rg.Files, 1)
	require.Equal(t, rg.Files[0].Name, "9.sst")
	require.Len(t, files3[0].TableMetas, 3)
	require.Nil(t, files3[0].TableMetas[0])
	require.Equal(t, files3[0].TableMetas[1].PhysicalId, int64(101))
	require.Equal(t, files3[0].TableMetas[2].PhysicalId, int64(102))

	// [50, 550] -overlap-> [1, 110], [140, 400], [450, 600]
	startKey, endKey, files = newRangeItem(100, 50, 550, "10.sst", 100)
	force = rangeTree.PutForce(startKey, endKey, files, false)
	require.True(t, force)
	rg = rangeTree.Get(newRange(startKey, endKey)).(*rtree.Range)
	require.Len(t, rg.Files, 1)
	require.Equal(t, rg.Files[0].Name, "10.sst")
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

func encodeTableRowKey(tableID int64, rowID uint64) []byte {
	tablePrefix := tablecodec.GenTableRecordPrefix(tableID)
	return tablecodec.EncodeRecordKey(tablePrefix, kv.IntHandle(rowID))
}

func encodeTableRecord(prefix kv.Key, rowID uint64) []byte {
	return tablecodec.EncodeRecordKey(prefix, kv.IntHandle(rowID))
}

func TestRangeTreeMerge(t *testing.T) {
	rangeTree := rtree.NewRangeStatsTree()
	tablePrefix := tablecodec.GenTableRecordPrefix(1)
	for i := uint64(0); i < 10000; i += 1 {
		rangeTree.InsertRange(&rtree.Range{
			StartKey: encodeTableRecord(tablePrefix, i),
			EndKey:   encodeTableRecord(tablePrefix, i+1),
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

	groupRanges := prTree.GetIncompleteRanges()
	require.Len(t, groupRanges, 1)
	require.Len(t, groupRanges[0].SubRanges, 3)
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("aa"), EndKey: []byte("cc")}, groupRanges[0].SubRanges[0])
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("cc"), EndKey: []byte("dd")}, groupRanges[0].SubRanges[1])
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("ee"), EndKey: []byte("ff")}, groupRanges[0].SubRanges[2])

	prs, err := prTree.FindContained([]byte("aaa"), []byte("b"))
	require.Len(t, prs, 1)
	require.NoError(t, err)
	prs[0].Res.Put([]byte("aaa"), []byte("b"), nil)

	// {[aa, aaa)}, {[b, cc), [cc, dd), [ee, ff)}
	groupRanges = prTree.GetIncompleteRanges()
	require.Len(t, groupRanges, 2)
	require.Len(t, groupRanges[0].SubRanges, 1)
	require.Len(t, groupRanges[1].SubRanges, 3)
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("aa"), EndKey: []byte("aaa")}, groupRanges[0].SubRanges[0])
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("b"), EndKey: []byte("cc")}, groupRanges[1].SubRanges[0])
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("cc"), EndKey: []byte("dd")}, groupRanges[1].SubRanges[1])
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("ee"), EndKey: []byte("ff")}, groupRanges[1].SubRanges[2])

	prs, err = prTree.FindContained([]byte("cc"), []byte("dd"))
	require.Len(t, prs, 1)
	require.NoError(t, err)
	prs[0].Res.Put([]byte("cc"), []byte("dd"), nil)

	// {[aa, aaa)}, {[b, cc)}, {[ee, ff)}
	groupRanges = prTree.GetIncompleteRanges()
	require.Len(t, groupRanges, 3)
	require.Len(t, groupRanges[0].SubRanges, 1)
	require.Len(t, groupRanges[1].SubRanges, 1)
	require.Len(t, groupRanges[2].SubRanges, 1)
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("aa"), EndKey: []byte("aaa")}, groupRanges[0].SubRanges[0])
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("b"), EndKey: []byte("cc")}, groupRanges[1].SubRanges[0])
	require.Equal(t, &kvrpcpb.KeyRange{StartKey: []byte("ee"), EndKey: []byte("ff")}, groupRanges[2].SubRanges[0])

	prs, err = prTree.FindContained([]byte("aa"), []byte("aaa"))
	require.Len(t, prs, 1)
	require.NoError(t, err)
	prs[0].Res.Put([]byte("aa"), []byte("aaa"), nil)

	prs, err = prTree.FindContained([]byte("b"), []byte("cc"))
	require.Len(t, prs, 1)
	require.NoError(t, err)
	prs[0].Res.Put([]byte("b"), []byte("cc"), nil)

	prs, err = prTree.FindContained([]byte("ee"), []byte("ff"))
	require.Len(t, prs, 1)
	require.NoError(t, err)
	prs[0].Res.Put([]byte("ee"), []byte("ff"), nil)

	groupRanges = prTree.GetIncompleteRanges()
	require.Equal(t, 0, len(groupRanges))
}

func removeNilTableMetas(tableMetas []*backuppb.TableMeta) []*backuppb.TableMeta {
	return slices.DeleteFunc(tableMetas, func(tableMeta *backuppb.TableMeta) bool {
		return tableMeta == nil
	})
}

func TestRemoveOverlappedTableMetas(t *testing.T) {
	rangeTree := rtree.NewRangeTreeWithPhysicalID(5)
	rg := &rtree.Range{
		StartKey: tablecodec.EncodeRecordKey(tablecodec.GenTableRecordPrefix(5), kv.IntHandle(1000)),
		EndKey:   tablecodec.EncodeRecordKey(tablecodec.GenTableRecordPrefix(5), kv.IntHandle(2000)),
	}
	skip := rtree.RemoveOverlappedTableMetas(&rangeTree, nil, rg, false)
	require.False(t, skip)
	skip = rtree.RemoveOverlappedTableMetas(&rangeTree, []*rtree.Range{
		{
			StartKey: tablecodec.EncodeRecordKey(tablecodec.GenTableRecordPrefix(5), kv.IntHandle(1000)),
			EndKey:   tablecodec.EncodeRecordKey(tablecodec.GenTableRecordPrefix(5), kv.IntHandle(2000)),
		},
	}, rg, false)
	require.True(t, skip)
	skip = rtree.RemoveOverlappedTableMetas(&rangeTree, []*rtree.Range{
		{
			StartKey: tablecodec.EncodeRecordKey(tablecodec.GenTableRecordPrefix(5), kv.IntHandle(500)),
			EndKey:   tablecodec.EncodeRecordKey(tablecodec.GenTableRecordPrefix(5), kv.IntHandle(2000)),
		},
	}, rg, false)
	require.True(t, skip)
	skip = rtree.RemoveOverlappedTableMetas(&rangeTree, []*rtree.Range{
		{
			StartKey: tablecodec.EncodeRecordKey(tablecodec.GenTableRecordPrefix(5), kv.IntHandle(1000)),
			EndKey:   tablecodec.EncodeRecordKey(tablecodec.GenTableRecordPrefix(5), kv.IntHandle(3000)),
		},
	}, rg, false)
	require.True(t, skip)

	overlaps := []*rtree.Range{
		{
			StartKey: tablecodec.EncodeRecordKey(tablecodec.GenTableRecordPrefix(5), kv.IntHandle(1)),
			EndKey:   tablecodec.EncodeRecordKey(tablecodec.GenTableRecordPrefix(5), kv.IntHandle(1500)),
			Files: []*backuppb.File{
				{
					TableMetas: []*backuppb.TableMeta{
						{PhysicalId: 4},
						{PhysicalId: 5},
					},
				},
			},
		},
	}
	skip = rtree.RemoveOverlappedTableMetas(&rangeTree, overlaps, rg, false)
	require.False(t, skip)
	tableMetas := removeNilTableMetas(overlaps[0].Files[0].TableMetas)
	require.Equal(t, []*backuppb.TableMeta{{PhysicalId: 4}}, tableMetas)
	overlaps = []*rtree.Range{
		{
			StartKey: tablecodec.EncodeRecordKey(tablecodec.GenTableRecordPrefix(5), kv.IntHandle(1)),
			EndKey:   tablecodec.EncodeRecordKey(tablecodec.GenTableRecordPrefix(5), kv.IntHandle(1500)),
			Files: []*backuppb.File{
				{
					TableMetas: []*backuppb.TableMeta{
						{PhysicalId: 4},
						{PhysicalId: 5},
					},
				},
				{
					TableMetas: []*backuppb.TableMeta{
						{PhysicalId: 4},
						{PhysicalId: 5},
					},
				},
			},
		},
		{
			StartKey: tablecodec.EncodeRecordKey(tablecodec.GenTableRecordPrefix(5), kv.IntHandle(1500)),
			EndKey:   tablecodec.EncodeRecordKey(tablecodec.GenTableRecordPrefix(5), kv.IntHandle(2500)),
			Files: []*backuppb.File{
				{
					TableMetas: []*backuppb.TableMeta{
						{PhysicalId: 5},
						{PhysicalId: 6},
					},
				},
				{
					TableMetas: []*backuppb.TableMeta{
						{PhysicalId: 5},
						{PhysicalId: 6},
					},
				},
			},
		},
	}
	skip = rtree.RemoveOverlappedTableMetas(&rangeTree, overlaps, rg, false)
	require.False(t, skip)
	tableMetas = removeNilTableMetas(overlaps[0].Files[0].TableMetas)
	require.Equal(t, []*backuppb.TableMeta{{PhysicalId: 4}}, tableMetas)
	tableMetas = removeNilTableMetas(overlaps[0].Files[1].TableMetas)
	require.Equal(t, []*backuppb.TableMeta{{PhysicalId: 4}}, tableMetas)
	tableMetas = removeNilTableMetas(overlaps[1].Files[0].TableMetas)
	require.Equal(t, []*backuppb.TableMeta{{PhysicalId: 6}}, tableMetas)
	tableMetas = removeNilTableMetas(overlaps[1].Files[1].TableMetas)
	require.Equal(t, []*backuppb.TableMeta{{PhysicalId: 6}}, tableMetas)
}
