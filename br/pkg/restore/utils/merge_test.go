// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils_test

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/conn"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

type fileBulder struct {
	tableID, startKeyOffset int64
}

func (fb *fileBulder) build(tableID, indexID, num, bytes, kv int) (files []*backuppb.File) {
	if num != 1 && num != 2 {
		panic("num must be 1 or 2")
	}

	// Rotate table ID
	if fb.tableID != int64(tableID) {
		fb.tableID = int64(tableID)
		fb.startKeyOffset = 0
	}

	low := codec.EncodeInt(nil, fb.startKeyOffset)
	fb.startKeyOffset += 10
	high := codec.EncodeInt(nil, fb.startKeyOffset)

	startKey := tablecodec.EncodeRowKey(fb.tableID, low)
	endKey := tablecodec.EncodeRowKey(fb.tableID, high)
	if indexID != 0 {
		lowVal := types.NewIntDatum(fb.startKeyOffset - 10)
		highVal := types.NewIntDatum(fb.startKeyOffset)
		sc := stmtctx.NewStmtCtxWithTimeZone(time.UTC)
		lowValue, err := codec.EncodeKey(sc.TimeZone(), nil, lowVal)
		err = sc.HandleError(err)
		if err != nil {
			panic(err)
		}
		highValue, err := codec.EncodeKey(sc.TimeZone(), nil, highVal)
		err = sc.HandleError(err)
		if err != nil {
			panic(err)
		}
		startKey = tablecodec.EncodeIndexSeekKey(int64(tableID), int64(indexID), lowValue)
		endKey = tablecodec.EncodeIndexSeekKey(int64(tableID), int64(indexID), highValue)
	}

	files = append(files, &backuppb.File{
		Name:       fmt.Sprint(rand.Int63n(math.MaxInt64), "_write.sst"),
		StartKey:   startKey,
		EndKey:     endKey,
		TotalKvs:   uint64(kv),
		TotalBytes: uint64(bytes),
		Cf:         "write",
	})
	if num == 1 {
		return
	}

	// To match TiKV's behavior.
	files[0].TotalKvs = 0
	files[0].TotalBytes = 0
	files = append(files, &backuppb.File{
		Name:       fmt.Sprint(rand.Int63n(math.MaxInt64), "_default.sst"),
		StartKey:   tablecodec.EncodeRowKey(fb.tableID, low),
		EndKey:     tablecodec.EncodeRowKey(fb.tableID, high),
		TotalKvs:   uint64(kv),
		TotalBytes: uint64(bytes),
		Cf:         "default",
	})
	return files
}

func TestMergeRanges(t *testing.T) {
	type Case struct {
		files  [][5]int // tableID, indexID num, bytes, kv
		merged []int    // length of each merged range
		stat   utils.MergeRangesStat
	}
	splitSizeBytes := int(conn.DefaultMergeRegionSizeBytes)
	splitKeyCount := int(conn.DefaultMergeRegionKeyCount)
	cases := []Case{
		// Empty backup.
		{
			files:  [][5]int{},
			merged: []int{},
			stat:   utils.MergeRangesStat{TotalRegions: 0, MergedRegions: 0},
		},

		// Do not merge big range.
		{
			files:  [][5]int{{1, 0, 1, splitSizeBytes, 1}, {1, 0, 1, 1, 1}},
			merged: []int{1, 1},
			stat:   utils.MergeRangesStat{TotalRegions: 2, MergedRegions: 2},
		},
		{
			files:  [][5]int{{1, 0, 1, 1, 1}, {1, 0, 1, splitSizeBytes, 1}},
			merged: []int{1, 1},
			stat:   utils.MergeRangesStat{TotalRegions: 2, MergedRegions: 2},
		},
		{
			files:  [][5]int{{1, 0, 1, 1, splitKeyCount}, {1, 0, 1, 1, 1}},
			merged: []int{1, 1},
			stat:   utils.MergeRangesStat{TotalRegions: 2, MergedRegions: 2},
		},
		{
			files:  [][5]int{{1, 0, 1, 1, 1}, {1, 0, 1, 1, splitKeyCount}},
			merged: []int{1, 1},
			stat:   utils.MergeRangesStat{TotalRegions: 2, MergedRegions: 2},
		},

		// 3 -> 1
		{
			files:  [][5]int{{1, 0, 1, 1, 1}, {1, 0, 1, 1, 1}, {1, 0, 1, 1, 1}},
			merged: []int{3},
			stat:   utils.MergeRangesStat{TotalRegions: 3, MergedRegions: 1},
		},
		// 3 -> 2, size: [split*1/3, split*1/3, split*1/2] -> [split*2/3, split*1/2]
		{
			files:  [][5]int{{1, 0, 1, splitSizeBytes / 3, 1}, {1, 0, 1, splitSizeBytes / 3, 1}, {1, 0, 1, splitSizeBytes / 2, 1}},
			merged: []int{2, 1},
			stat:   utils.MergeRangesStat{TotalRegions: 3, MergedRegions: 2},
		},
		// 4 -> 2, size: [split*1/3, split*1/3, split*1/2, 1] -> [split*2/3, split*1/2 +1]
		{
			files:  [][5]int{{1, 0, 1, splitSizeBytes / 3, 1}, {1, 0, 1, splitSizeBytes / 3, 1}, {1, 0, 1, splitSizeBytes / 2, 1}, {1, 0, 1, 1, 1}},
			merged: []int{2, 2},
			stat:   utils.MergeRangesStat{TotalRegions: 4, MergedRegions: 2},
		},
		// 5 -> 3, size: [split*1/3, split*1/3, split, split*1/2, 1] -> [split*2/3, split, split*1/2 +1]
		{
			files:  [][5]int{{1, 0, 1, splitSizeBytes / 3, 1}, {1, 0, 1, splitSizeBytes / 3, 1}, {1, 0, 1, splitSizeBytes, 1}, {1, 0, 1, splitSizeBytes / 2, 1}, {1, 0, 1, 1, 1}},
			merged: []int{2, 1, 2},
			stat:   utils.MergeRangesStat{TotalRegions: 5, MergedRegions: 3},
		},

		// Do not merge ranges from different tables
		// 2 -> 2, size: [1, 1] -> [1, 1], table ID: [1, 2] -> [1, 2]
		{
			files:  [][5]int{{1, 0, 1, 1, 1}, {2, 0, 1, 1, 1}},
			merged: []int{1, 1},
			stat:   utils.MergeRangesStat{TotalRegions: 2, MergedRegions: 2},
		},
		// 3 -> 2, size: [1@split*1/3, 2@split*1/3, 2@split*1/2] -> [1@split*1/3, 2@split*5/6]
		{
			files:  [][5]int{{1, 0, 1, splitSizeBytes / 3, 1}, {2, 0, 1, splitSizeBytes / 3, 1}, {2, 0, 1, splitSizeBytes / 2, 1}},
			merged: []int{1, 2},
			stat:   utils.MergeRangesStat{TotalRegions: 3, MergedRegions: 2},
		},

		// Do not merge ranges from different indexes.
		// 2 -> 2, size: [1, 1] -> [1, 1], index ID: [1, 2] -> [1, 2]
		{
			files:  [][5]int{{1, 1, 1, 1, 1}, {1, 2, 1, 1, 1}},
			merged: []int{1, 1},
			stat:   utils.MergeRangesStat{TotalRegions: 2, MergedRegions: 2},
		},
		// Index ID out of order.
		// 2 -> 2, size: [1, 1] -> [1, 1], index ID: [2, 1] -> [1, 2]
		{
			files:  [][5]int{{1, 2, 1, 1, 1}, {1, 1, 1, 1, 1}},
			merged: []int{1, 1},
			stat:   utils.MergeRangesStat{TotalRegions: 2, MergedRegions: 2},
		},
		// 3 -> 3, size: [1, 1, 1] -> [1, 1, 1]
		// (table ID, index ID): [(1, 0), (2, 1), (2, 2)] -> [(1, 0), (2, 1), (2, 2)]
		{
			files:  [][5]int{{1, 0, 1, 1, 1}, {2, 1, 1, 1, 1}, {2, 2, 1, 1, 1}},
			merged: []int{1, 1, 1},
			stat:   utils.MergeRangesStat{TotalRegions: 3, MergedRegions: 3},
		},
		// 4 -> 3, size: [1, 1, 1, 1] -> [1, 1, 2]
		// (table ID, index ID): [(1, 0), (2, 1), (2, 0), (2, 0)] -> [(1, 0), (2, 1), (2, 0)]
		{
			files:  [][5]int{{1, 0, 1, 1, 1}, {2, 1, 1, 1, 1}, {2, 0, 1, 1, 1}, {2, 0, 1, 1, 1}},
			merged: []int{1, 1, 2},
			stat:   utils.MergeRangesStat{TotalRegions: 4, MergedRegions: 3},
		},
		// Merge the same table ID and index ID.
		// 4 -> 3, size: [1, 1, 1, 1] -> [1, 2, 1]
		// (table ID, index ID): [(1, 0), (2, 1), (2, 1), (2, 0)] -> [(1, 0), (2, 1), (2, 0)]
		{
			files:  [][5]int{{1, 0, 1, 1, 1}, {2, 1, 1, 1, 1}, {2, 1, 1, 1, 1}, {2, 0, 1, 1, 1}},
			merged: []int{1, 2, 1},
			stat:   utils.MergeRangesStat{TotalRegions: 4, MergedRegions: 3},
		},
	}

	for i, cs := range cases {
		files := make([]*backuppb.File, 0)
		fb := fileBulder{}
		for _, f := range cs.files {
			files = append(files, fb.build(f[0], f[1], f[2], f[3], f[4])...)
		}
		rngs, stat, err := utils.MergeAndRewriteFileRanges(files, nil, conn.DefaultMergeRegionSizeBytes, conn.DefaultMergeRegionKeyCount)
		require.NoErrorf(t, err, "%+v", cs)
		require.Equalf(t, cs.stat.TotalRegions, stat.TotalRegions, "%+v", cs)
		require.Equalf(t, cs.stat.MergedRegions, stat.MergedRegions, "%+v", cs)
		require.Lenf(t, rngs, len(cs.merged), "case %d", i)
		for i, rg := range rngs {
			require.Lenf(t, rg.Files, cs.merged[i], "%+v", cs)
			// Files range must be in [Range.StartKey, Range.EndKey].
			for _, f := range rg.Files {
				require.LessOrEqual(t, bytes.Compare(rg.StartKey, f.StartKey), 0)
				require.GreaterOrEqual(t, bytes.Compare(rg.EndKey, f.EndKey), 0)
			}
		}
	}
}

func tableMeta(id int64, kvs, bytes, checksum uint64) *backuppb.TableMeta {
	return &backuppb.TableMeta{
		PhysicalId: id,
		TotalKvs:   kvs,
		TotalBytes: bytes,
		Crc64Xor:   checksum,
	}
}

func TestMergeRanges2(t *testing.T) {
	rules := map[int64]*utils.RewriteRules{
		1: tableDetailRewriteRule(1),
		3: tableDetailRewriteRule(3),
		4: tableDetailRewriteRule(4),
	}
	f1 := &backuppb.File{
		Name:     "1_write.sst",
		StartKey: tablecodec.GenTableRecordPrefix(1),
		EndKey:   tablecodec.EncodeTableIndexPrefix(2, 1),
		Cf:       utils.WriteCFName,
		TableMetas: []*backuppb.TableMeta{
			tableMeta(1, 1, 1, 1),
			tableMeta(2, 2, 2, 2),
		},
	}
	f2 := &backuppb.File{
		Name:     "1_default.sst",
		StartKey: tablecodec.GenTableRecordPrefix(1),
		EndKey:   tablecodec.EncodeTableIndexPrefix(2, 1),
		Cf:       utils.DefaultCFName,
		TableMetas: []*backuppb.TableMeta{
			tableMeta(1, 11, 11, 11),
			tableMeta(2, 22, 22, 22),
		},
	}
	files := []*backuppb.File{
		f1, f2, f1, f2,
		{
			Name:     "2_default.sst",
			StartKey: tablecodec.EncodeTableIndexPrefix(2, 1),
			EndKey:   tablecodec.EncodeTableIndexPrefix(4, 1),
			Cf:       utils.DefaultCFName,
			TableMetas: []*backuppb.TableMeta{
				tableMeta(2, 222, 222, 222),
				tableMeta(3, 333, 333, 333),
				tableMeta(4, 444, 444, 444),
			},
		},
		{
			Name:     "2_write.sst",
			StartKey: tablecodec.EncodeTableIndexPrefix(2, 1),
			EndKey:   tablecodec.EncodeTableIndexPrefix(4, 1),
			Cf:       utils.WriteCFName,
			TableMetas: []*backuppb.TableMeta{
				tableMeta(2, 2222, 2222, 2222),
				tableMeta(3, 3333, 3333, 3333),
				tableMeta(4, 4444, 4444, 4444),
			},
		},
		{
			Name:     "3_default.sst",
			StartKey: tablecodec.EncodeTableIndexPrefix(4, 1),
			EndKey:   tablecodec.GenTableRecordPrefix(4),
			Cf:       utils.DefaultCFName,
			TableMetas: []*backuppb.TableMeta{
				tableMeta(4, 44444, 44444, 44444),
			},
		},
		{
			Name:     "3_write.sst",
			StartKey: tablecodec.EncodeTableIndexPrefix(4, 1),
			EndKey:   tablecodec.GenTableRecordPrefix(4),
			Cf:       utils.WriteCFName,
			TableMetas: []*backuppb.TableMeta{
				tableMeta(4, 444444, 444444, 444444),
			},
		},
	}
	ranges, stats, err := utils.MergeAndRewriteFileRanges(files, rules, conn.DefaultMergeRegionSizeBytes, conn.DefaultMergeRegionKeyCount)
	require.NoError(t, err)
	require.Equal(t, ranges, stats)
}

func TestMergeRawKVRanges(t *testing.T) {
	files := make([]*backuppb.File, 0)
	fb := fileBulder{}
	files = append(files, fb.build(1, 0, 2, 1, 1)...)
	// RawKV does not have write cf
	files = files[1:]
	_, stat, err := utils.MergeAndRewriteFileRanges(
		files, nil, conn.DefaultMergeRegionSizeBytes, conn.DefaultMergeRegionKeyCount)
	require.NoError(t, err)
	require.Equal(t, 1, stat.TotalRegions)
	require.Equal(t, 1, stat.MergedRegions)
}

func TestInvalidRanges(t *testing.T) {
	files := make([]*backuppb.File, 0)
	fb := fileBulder{}
	files = append(files, fb.build(1, 0, 1, 1, 1)...)
	files[0].Name = "invalid.sst"
	files[0].Cf = "invalid"
	_, _, err := utils.MergeAndRewriteFileRanges(
		files, nil, conn.DefaultMergeRegionSizeBytes, conn.DefaultMergeRegionKeyCount)
	require.Error(t, err)
	require.Equal(t, berrors.ErrRestoreInvalidBackup, errors.Cause(err))
}

// Benchmark results on Intel(R) Xeon(R) CPU E5-2630 v4 @ 2.20GHz
//
// BenchmarkMergeRanges100-40          9676             114344 ns/op
// BenchmarkMergeRanges1k-40            345            3700739 ns/op
// BenchmarkMergeRanges10k-40             3          414097277 ns/op
// BenchmarkMergeRanges50k-40             1        17258177908 ns/op
// BenchmarkMergeRanges100k-40            1        73403873161 ns/op

func benchmarkMergeRanges(b *testing.B, filesCount int) {
	files := make([]*backuppb.File, 0)
	fb := fileBulder{}
	for i := 0; i < filesCount; i++ {
		files = append(files, fb.build(1, 0, 1, 1, 1)...)
	}
	var err error
	for i := 0; i < b.N; i++ {
		_, _, err = utils.MergeAndRewriteFileRanges(files, nil, conn.DefaultMergeRegionSizeBytes, conn.DefaultMergeRegionKeyCount)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkMergeRanges100(b *testing.B) {
	benchmarkMergeRanges(b, 100)
}

func BenchmarkMergeRanges1k(b *testing.B) {
	benchmarkMergeRanges(b, 1000)
}

func BenchmarkMergeRanges10k(b *testing.B) {
	benchmarkMergeRanges(b, 10000)
}

func BenchmarkMergeRanges50k(b *testing.B) {
	benchmarkMergeRanges(b, 50000)
}

func BenchmarkMergeRanges100k(b *testing.B) {
	benchmarkMergeRanges(b, 100000)
}

func TestGenerateNewKeyRange(t *testing.T) {
	file := &backuppb.File{
		StartKey: tablecodec.GenTableRecordPrefix(1),
		EndKey:   tablecodec.GenTableRecordPrefix(5),
		TableMetas: []*backuppb.TableMeta{
			{PhysicalId: 1},
			{PhysicalId: 2},
			{PhysicalId: 3},
			{PhysicalId: 4},
			{PhysicalId: 5},
		},
	}
	newRg, err := utils.GenerateNewKeyRange([]*backuppb.File{file}, map[int64]*utils.RewriteRules{
		1: tableRewriteRule(1),
		2: tableRewriteRule(2),
		3: tableRewriteRule(3),
		4: tableRewriteRule(4),
		5: tableRewriteRule(5),
	})
	require.NoError(t, err)
	require.Equal(t, []byte(tablecodec.GenTableRecordPrefix(1)), file.StartKey)
	require.Equal(t, []byte(tablecodec.GenTableRecordPrefix(5)), file.EndKey)
	require.Equal(t, []byte(tablecodec.GenTableRecordPrefix(1001)), newRg.StartKey)
	require.Equal(t, []byte(tablecodec.GenTableRecordPrefix(1005)), newRg.EndKey)
	require.Len(t, newRg.Files, 1)
	require.Equal(t, []byte(tablecodec.GenTableRecordPrefix(1)), newRg.Files[0].StartKey)
	require.Equal(t, []byte(tablecodec.GenTableRecordPrefix(5)), newRg.Files[0].EndKey)

	file = &backuppb.File{
		StartKey: tablecodec.GenTableRecordPrefix(1),
		EndKey:   tablecodec.GenTableRecordPrefix(5),
		TableMetas: []*backuppb.TableMeta{
			{PhysicalId: 2},
			{PhysicalId: 3},
			{PhysicalId: 4},
			{PhysicalId: 5},
		},
	}
	newRg, err = utils.GenerateNewKeyRange([]*backuppb.File{file}, map[int64]*utils.RewriteRules{
		2: tableRewriteRule(2),
		3: tableRewriteRule(3),
		4: tableRewriteRule(4),
		5: tableRewriteRule(5),
	})

	require.NoError(t, err)
	require.Equal(t, []byte(tablecodec.EncodeTableIndexPrefix(2, 0)), file.StartKey)
	require.Equal(t, []byte(tablecodec.GenTableRecordPrefix(5)), file.EndKey)
	require.Equal(t, []byte(tablecodec.EncodeTableIndexPrefix(1002, 0)), newRg.StartKey)
	require.Equal(t, []byte(tablecodec.GenTableRecordPrefix(1005)), newRg.EndKey)
	require.Len(t, newRg.Files, 1)
	require.Equal(t, []byte(tablecodec.EncodeTableIndexPrefix(2, 0)), newRg.Files[0].StartKey)
	require.Equal(t, []byte(tablecodec.GenTableRecordPrefix(5)), newRg.Files[0].EndKey)

	file = &backuppb.File{
		StartKey: tablecodec.GenTableRecordPrefix(1),
		EndKey:   tablecodec.GenTableRecordPrefix(5),
		TableMetas: []*backuppb.TableMeta{
			{PhysicalId: 1},
			{PhysicalId: 2},
			{PhysicalId: 3},
			{PhysicalId: 4},
		},
	}
	newRg, err = utils.GenerateNewKeyRange([]*backuppb.File{file}, map[int64]*utils.RewriteRules{
		1: tableRewriteRule(1),
		2: tableRewriteRule(2),
		3: tableRewriteRule(3),
		4: tableRewriteRule(4),
	})

	require.NoError(t, err)
	require.Equal(t, []byte(tablecodec.GenTableRecordPrefix(1)), file.StartKey)
	require.Equal(t, append([]byte(tablecodec.EncodeTablePrefix(4)), []byte("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF")...), file.EndKey)
	require.Equal(t, []byte(tablecodec.GenTableRecordPrefix(1001)), newRg.StartKey)
	require.Equal(t, append([]byte(tablecodec.EncodeTablePrefix(1004)), []byte("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF")...), newRg.EndKey)
	require.Len(t, newRg.Files, 1)
	require.Equal(t, []byte(tablecodec.GenTableRecordPrefix(1)), newRg.Files[0].StartKey)
	require.Equal(t, append([]byte(tablecodec.EncodeTablePrefix(4)), []byte("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF")...), newRg.Files[0].EndKey)

	file = &backuppb.File{
		StartKey: tablecodec.GenTableRecordPrefix(1),
		EndKey:   tablecodec.GenTableRecordPrefix(5),
		TableMetas: []*backuppb.TableMeta{
			{PhysicalId: 3},
			{PhysicalId: 4},
		},
	}
	newRg, err = utils.GenerateNewKeyRange([]*backuppb.File{file}, map[int64]*utils.RewriteRules{
		3: tableRewriteRule(3),
		4: tableRewriteRule(4),
	})

	require.NoError(t, err)
	require.Equal(t, []byte(tablecodec.EncodeTableIndexPrefix(3, 0)), file.StartKey)
	require.Equal(t, append([]byte(tablecodec.EncodeTablePrefix(4)), []byte("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF")...), file.EndKey)
	require.Equal(t, []byte(tablecodec.EncodeTableIndexPrefix(1003, 0)), newRg.StartKey)
	require.Equal(t, append([]byte(tablecodec.EncodeTablePrefix(1004)), []byte("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF")...), newRg.EndKey)
	require.Len(t, newRg.Files, 1)
	require.Equal(t, []byte(tablecodec.EncodeTableIndexPrefix(3, 0)), newRg.Files[0].StartKey)
	require.Equal(t, append([]byte(tablecodec.EncodeTablePrefix(4)), []byte("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF")...), newRg.Files[0].EndKey)
}
