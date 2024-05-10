// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package extsort

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"math/rand"
	"slices"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestDiskSorterCommon(t *testing.T) {
	sorter, err := OpenDiskSorter(t.TempDir(), &DiskSorterOptions{
		WriterBufferSize:    32 * 1024,
		CompactionThreshold: 4,
		MaxCompactionDepth:  4,
	})
	require.NoError(t, err)
	runCommonTest(t, sorter)
	require.NoError(t, sorter.Close())
}

func TestDiskSorterCommonParallel(t *testing.T) {
	sorter, err := OpenDiskSorter(t.TempDir(), &DiskSorterOptions{
		WriterBufferSize:    32 * 1024,
		CompactionThreshold: 4,
		MaxCompactionDepth:  4,
	})
	require.NoError(t, err)
	runCommonParallelTest(t, sorter)
	require.NoError(t, sorter.Close())
}

func TestDiskSorterReopen(t *testing.T) {
	dirname := t.TempDir()
	sorter, err := OpenDiskSorter(dirname, &DiskSorterOptions{
		WriterBufferSize:    32 * 1024,
		CompactionThreshold: 4,
		MaxCompactionDepth:  4,
	})
	require.NoError(t, err)

	rng := rand.New(rand.NewSource(0))
	kvs := genRandomKVs(rng, 2000, 256, 1024)
	batch1, batch2 := kvs[:1000], kvs[1000:]

	// Write the first batch.
	w, err := sorter.NewWriter(context.Background())
	require.NoError(t, err)
	for _, kv := range batch1 {
		require.NoError(t, w.Put(kv.key, kv.value))
	}
	require.NoError(t, w.Close())

	// Reopen the sorter.
	require.NoError(t, sorter.Close())
	sorter, err = OpenDiskSorter(dirname, &DiskSorterOptions{
		WriterBufferSize:    32 * 1024,
		CompactionThreshold: 4,
		MaxCompactionDepth:  4,
	})
	require.NoError(t, err)
	require.False(t, sorter.IsSorted())

	// Write the second batch.
	w, err = sorter.NewWriter(context.Background())
	require.NoError(t, err)
	for _, kv := range batch2 {
		require.NoError(t, w.Put(kv.key, kv.value))
	}
	require.NoError(t, w.Close())

	slices.SortFunc(kvs, func(a, b keyValue) int {
		return bytes.Compare(a.key, b.key)
	})
	verify := func() {
		iter, err := sorter.NewIterator(context.Background())
		require.NoError(t, err)
		kvCnt := 0
		for iter.First(); iter.Valid(); iter.Next() {
			require.Equal(t, kvs[kvCnt].key, iter.UnsafeKey())
			require.Equal(t, kvs[kvCnt].value, iter.UnsafeValue())
			kvCnt++
		}
		require.Equal(t, len(kvs), kvCnt)
		require.NoError(t, iter.Close())
	}

	// Sort and verify.
	require.NoError(t, sorter.Sort(context.Background()))
	verify()
	require.True(t, sorter.IsSorted())

	// Reopen the sorter again.
	require.NoError(t, sorter.Close())
	sorter, err = OpenDiskSorter(dirname, &DiskSorterOptions{
		WriterBufferSize: 32 * 1024,
	})
	require.NoError(t, err)
	require.True(t, sorter.IsSorted())
	verify()
}

func TestKVStatsCollector(t *testing.T) {
	kvs := []keyValue{
		{[]byte("aa"), []byte("11")},
		{[]byte("bb"), []byte("22")},
		{[]byte("cc"), []byte("33")},
		{[]byte("dd"), []byte("44")},
		{[]byte("ee"), []byte("55")},
	}
	testCases := []struct {
		bucketSize int
		expected   kvStats
	}{
		{
			bucketSize: 0,
			expected: kvStats{Histogram: []kvStatsBucket{
				{4, []byte("aa")},
				{4, []byte("bb")},
				{4, []byte("cc")},
				{4, []byte("dd")},
				{4, []byte("ee")},
			}},
		},
		{
			bucketSize: 4,
			expected: kvStats{Histogram: []kvStatsBucket{
				{4, []byte("aa")},
				{4, []byte("bb")},
				{4, []byte("cc")},
				{4, []byte("dd")},
				{4, []byte("ee")},
			}},
		},
		{
			bucketSize: 7,
			expected: kvStats{Histogram: []kvStatsBucket{
				{8, []byte("bb")},
				{8, []byte("dd")},
				{4, []byte("ee")},
			}},
		},
		{
			bucketSize: 50,
			expected: kvStats{Histogram: []kvStatsBucket{
				{20, []byte("ee")},
			}},
		},
	}

	for _, tc := range testCases {
		c := newKVStatsCollector(tc.bucketSize)
		for _, kv := range kvs {
			err := c.Add(sstable.InternalKey{UserKey: kv.key}, kv.value)
			require.NoError(t, err)
		}
		userProps := make(map[string]string)
		require.NoError(t, c.Finish(userProps))
		require.Len(t, userProps, 1)
		prop, ok := userProps[kvStatsPropKey]
		require.True(t, ok)
		var stats kvStats
		require.NoError(t, json.Unmarshal([]byte(prop), &stats))
		require.Equal(t, tc.expected, stats)
	}
}

func TestMakeFilename(t *testing.T) {
	testCases := []struct {
		dir      string
		fileNum  int
		expected string
	}{
		{
			dir:      "/tmp",
			fileNum:  1,
			expected: "/tmp/000001.sst",
		},
		{
			dir:      "/tmp",
			fileNum:  123,
			expected: "/tmp/000123.sst",
		},
		{
			dir:      "/tmp",
			fileNum:  666666,
			expected: "/tmp/666666.sst",
		},
		{
			dir:      "/tmp",
			fileNum:  7777777,
			expected: "/tmp/7777777.sst",
		},
	}

	fs := vfs.NewMem()
	for _, tc := range testCases {
		require.Equal(t, tc.expected, makeFilename(fs, tc.dir, tc.fileNum))
	}
}

func TestParseFilename(t *testing.T) {
	testCases := []struct {
		filename   string
		expOK      bool
		expFileNum int
	}{
		{
			filename:   "/tmp/1.sst",
			expOK:      true,
			expFileNum: 1,
		},
		{
			filename:   "/tmp/123.sst",
			expOK:      true,
			expFileNum: 123,
		},
		{
			filename:   "/tmp/000001.sst",
			expOK:      true,
			expFileNum: 1,
		},
		{
			filename:   "/tmp/000123.sst",
			expOK:      true,
			expFileNum: 123,
		},
		{
			filename:   "/tmp/666666.sst",
			expOK:      true,
			expFileNum: 666666,
		},
		{
			filename:   "/tmp/7777777.sst",
			expOK:      true,
			expFileNum: 7777777,
		},
		{
			filename: "/tmp/123.sst.tmp",
			expOK:    false,
		},
		{
			filename: diskSorterSortedFile,
			expOK:    false,
		},
	}

	fs := vfs.NewMem()
	for _, tc := range testCases {
		fileNum, ok := parseFilename(fs, tc.filename)
		require.Equal(t, tc.expOK, ok)
		if tc.expOK {
			require.Equal(t, tc.expFileNum, fileNum)
		}
	}
}

func TestSSTWriter(t *testing.T) {
	fs := vfs.Default
	dirname := t.TempDir()
	fileNum := 13

	var fileMeta *fileMetadata
	sw, err := newSSTWriter(fs, dirname, fileNum, 8, func(meta *fileMetadata) {
		fileMeta = meta
	})
	require.NoError(t, err)

	require.NoError(t, sw.Set([]byte("aa"), []byte("11")))
	require.NoError(t, sw.Set([]byte("bb"), []byte("22")))
	require.NoError(t, sw.Set([]byte("cc"), []byte("33")))
	require.NoError(t, sw.Set([]byte("dd"), []byte("44")))
	require.NoError(t, sw.Set([]byte("ee"), []byte("55")))
	require.NoError(t, sw.Close())
	require.FileExists(t, makeFilename(fs, dirname, fileNum))
	list, err := fs.List(dirname)
	require.NoError(t, err)
	require.Len(t, list, 1)

	require.NotNil(t, fileMeta)
	require.Equal(t, fileNum, fileMeta.fileNum)
	require.Equal(t, []byte("aa"), fileMeta.startKey)
	require.Equal(t, []byte("ee\x00"), fileMeta.endKey)
	require.Equal(t, []byte("ee"), fileMeta.lastKey)
	require.Equal(t, kvStats{Histogram: []kvStatsBucket{
		{8, []byte("bb")},
		{8, []byte("dd")},
		{4, []byte("ee")},
	}}, fileMeta.kvStats)
}

func TestSSTWriterEmpty(t *testing.T) {
	fs := vfs.Default
	dirname := t.TempDir()
	fileNum := 13

	var fileMeta *fileMetadata
	sw, err := newSSTWriter(fs, dirname, fileNum, 8, func(meta *fileMetadata) {
		fileMeta = meta
	})
	require.NoError(t, err)
	require.NoError(t, sw.Close())
	require.FileExists(t, makeFilename(fs, dirname, fileNum))
	list, err := fs.List(dirname)
	require.NoError(t, err)
	require.Len(t, list, 1)

	require.NotNil(t, fileMeta)
	require.Equal(t, fileNum, fileMeta.fileNum)
	require.Nil(t, fileMeta.startKey)
	require.Equal(t, []byte{0}, fileMeta.endKey)
	require.Nil(t, fileMeta.lastKey)
	require.Equal(t, kvStats{}, fileMeta.kvStats)
}

func TestSSTWriterError(t *testing.T) {
	fs := vfs.Default
	dirname := t.TempDir()
	fileNum := 13

	sw, err := newSSTWriter(fs, dirname, fileNum, 0, func(meta *fileMetadata) {})
	require.NoError(t, err)

	require.NoError(t, sw.Set([]byte("bb"), []byte("11")))
	require.Error(t, sw.Set([]byte("aa"), []byte("22")))
	require.Error(t, sw.Close())

	list, err := fs.List(dirname)
	require.NoError(t, err)
	require.Empty(t, list)
}

func TestSSTReaderPool(t *testing.T) {
	fs := vfs.Default
	dirname := t.TempDir()

	writeSSTFile(t, fs, dirname, 1, nil)

	pool := newSSTReaderPool(fs, dirname, pebble.NewCache(8<<20))

	r1, err := pool.get(1)
	require.NoError(t, err)
	r2, err := pool.get(1)
	require.NoError(t, err)
	require.True(t, r1 == r2, "should be the same reader")

	require.NoError(t, pool.unref(1))
	// reader is still referenced, so it should be valid.
	iter, err := r1.NewIter(nil, nil)
	require.NoError(t, err)
	require.NoError(t, iter.Close())

	require.NoError(t, pool.unref(1))
	// reader is no longer referenced, so it should be closed.
	iter, err = r1.NewIter(nil, nil)
	require.Error(t, err)

	// reader has been removed from the pool, unref should panic.
	require.Panics(t, func() {
		_ = pool.unref(1)
	})
}

func TestSSTReaderPoolParallel(t *testing.T) {
	fs := vfs.Default
	dirname := t.TempDir()

	writeSSTFile(t, fs, dirname, 1, nil)
	writeSSTFile(t, fs, dirname, 2, nil)
	writeSSTFile(t, fs, dirname, 3, nil)

	cache := pebble.NewCache(8 << 20)
	defer cache.Unref()
	pool := newSSTReaderPool(fs, dirname, cache)

	var wg sync.WaitGroup
	for i := 0; i <= 16; i++ {
		wg.Add(1)
		go func(fileNum int) {
			defer wg.Done()
			for j := 0; j < 10000; j++ {
				_, err := pool.get(fileNum)
				require.NoError(t, err)
				require.NoError(t, pool.unref(fileNum))
			}
		}(i%3 + 1)
	}
	wg.Wait()
}

// writeSSTFile writes an SST file with the given key/value pairs.
func writeSSTFile(
	t *testing.T,
	fs vfs.FS,
	dirname string,
	fileNum int,
	kvs []keyValue,
) *fileMetadata {
	var file *fileMetadata
	sw, err := newSSTWriter(fs, dirname, fileNum, 8, func(meta *fileMetadata) {
		file = meta
	})
	require.NoError(t, err)
	for _, kv := range kvs {
		require.NoError(t, sw.Set(kv.key, kv.value))
	}
	require.NoError(t, sw.Close())
	return file
}

func TestSSTIter(t *testing.T) {
	fs := vfs.Default
	dirname := t.TempDir()

	writeSSTFile(t, fs, dirname, 1, []keyValue{
		{[]byte("aa"), []byte("11")},
		{[]byte("bb"), []byte("22")},
		{[]byte("cc"), []byte("33")},
		{[]byte("dd"), []byte("44")},
		{[]byte("ee"), []byte("55")},
	})

	pool := newSSTReaderPool(fs, dirname, pebble.NewCache(8<<20))
	reader, err := pool.get(1)
	require.NoError(t, err)

	rawIter, err := reader.NewIter(nil, nil)
	require.NoError(t, err)

	iter := &sstIter{
		iter: rawIter,
		onClose: func() error {
			return pool.unref(1)
		},
	}

	require.True(t, iter.Seek([]byte("bc")))
	require.Equal(t, []byte("cc"), iter.UnsafeKey())
	require.Equal(t, []byte("33"), iter.UnsafeValue())
	require.True(t, iter.First())
	require.Equal(t, []byte("aa"), iter.UnsafeKey())
	require.Equal(t, []byte("11"), iter.UnsafeValue())
	require.True(t, iter.Next())
	require.Equal(t, []byte("bb"), iter.UnsafeKey())
	require.Equal(t, []byte("22"), iter.UnsafeValue())
	require.True(t, iter.Last())
	require.Equal(t, []byte("ee"), iter.UnsafeKey())
	require.Equal(t, []byte("55"), iter.UnsafeValue())
	require.False(t, iter.Next())
	require.False(t, iter.Valid())
	require.NoError(t, iter.Close())
}

func TestMergingIter(t *testing.T) {
	fs := vfs.Default
	dirname := t.TempDir()

	cache := pebble.NewCache(8 << 20)
	defer cache.Unref()
	readerPool := newSSTReaderPool(fs, dirname, cache)
	openIter := func(file *fileMetadata) (Iterator, error) {
		reader, err := readerPool.get(file.fileNum)
		if err != nil {
			return nil, errors.Trace(err)
		}
		iter, err := reader.NewIter(nil, nil)
		if err != nil {
			_ = reader.Close()
			return nil, errors.Trace(err)
		}
		return &sstIter{
			iter: iter,
			onClose: func() error {
				return readerPool.unref(file.fileNum)
			},
		}, nil
	}

	files := []*fileMetadata{
		writeSSTFile(t, fs, dirname, 1, []keyValue{
			{[]byte("a0"), []byte("va0")},
			{[]byte("a1"), []byte("va1")},
			{[]byte("e0"), []byte("ve0")},
			{[]byte("e1"), []byte("ve1")},
		}),
		writeSSTFile(t, fs, dirname, 2, []keyValue{
			{[]byte("b0"), []byte("vb0")},
			{[]byte("b1"), []byte("vb1")},
			{[]byte("d0"), []byte("vd0")},
			{[]byte("d1"), []byte("vd1")},
		}),
		writeSSTFile(t, fs, dirname, 3, []keyValue{
			{[]byte("c0"), []byte("vc0")},
			{[]byte("c1"), []byte("vc1")},
			{[]byte("g0"), []byte("vg0")},
			{[]byte("g1"), []byte("vg1")},
		}),
		writeSSTFile(t, fs, dirname, 4, []keyValue{
			{[]byte("f0"), []byte("vf0")},
			{[]byte("f1"), []byte("vf1")},
			{[]byte("h1"), []byte("vh1")},
		}),
		writeSSTFile(t, fs, dirname, 5, []keyValue{
			{[]byte("f0"), []byte("vf0")}, // duplicate key with file 4
			{[]byte("f2"), []byte("vf2")},
			{[]byte("h0"), []byte("vh0")},
		}),
		writeSSTFile(t, fs, dirname, 6, []keyValue{
			{[]byte("i0"), []byte("vi0")},
			{[]byte("i1"), []byte("vi1")},
			{[]byte("j0"), []byte("vj0")},
			{[]byte("j1"), []byte("vj1")},
		}),
	}
	slices.SortFunc(files, func(a, b *fileMetadata) int {
		return bytes.Compare(a.startKey, b.startKey)
	})

	iter := newMergingIter(files, openIter)
	var allKVs []keyValue
	for iter.First(); iter.Valid(); iter.Next() {
		allKVs = append(allKVs, keyValue{
			key:   slices.Clone(iter.UnsafeKey()),
			value: slices.Clone(iter.UnsafeValue()),
		})
	}
	require.NoError(t, iter.Error())

	require.Equal(t, []keyValue{
		{[]byte("a0"), []byte("va0")},
		{[]byte("a1"), []byte("va1")},
		{[]byte("b0"), []byte("vb0")},
		{[]byte("b1"), []byte("vb1")},
		{[]byte("c0"), []byte("vc0")},
		{[]byte("c1"), []byte("vc1")},
		{[]byte("d0"), []byte("vd0")},
		{[]byte("d1"), []byte("vd1")},
		{[]byte("e0"), []byte("ve0")},
		{[]byte("e1"), []byte("ve1")},
		{[]byte("f0"), []byte("vf0")},
		{[]byte("f1"), []byte("vf1")},
		{[]byte("f2"), []byte("vf2")},
		{[]byte("g0"), []byte("vg0")},
		{[]byte("g1"), []byte("vg1")},
		{[]byte("h0"), []byte("vh0")},
		{[]byte("h1"), []byte("vh1")},
		{[]byte("i0"), []byte("vi0")},
		{[]byte("i1"), []byte("vi1")},
		{[]byte("j0"), []byte("vj0")},
		{[]byte("j1"), []byte("vj1")},
	}, allKVs)

	// Seek to the first key.
	require.True(t, iter.Seek(nil))
	require.Equal(t, []byte("a0"), iter.UnsafeKey())
	require.Equal(t, []byte("va0"), iter.UnsafeValue())

	// Seek to the key after the last key.
	require.False(t, iter.Seek([]byte("k")))
	require.NoError(t, iter.Error())

	// Randomly seek to keys and check the result.
	seekIndexes := make([]int, len(allKVs))
	for i := range seekIndexes {
		seekIndexes[i] = i
	}
	rand.Shuffle(len(seekIndexes), func(i, j int) {
		seekIndexes[i], seekIndexes[j] = seekIndexes[j], seekIndexes[i]
	})
	for _, i := range seekIndexes {
		require.True(t, iter.Seek(allKVs[i].key))
		require.Equal(t, allKVs[i].key, iter.UnsafeKey())
		require.Equal(t, allKVs[i].value, iter.UnsafeValue())
	}

	// Test Last.
	require.True(t, iter.Last())
	require.Equal(t, []byte("j1"), iter.UnsafeKey())
	require.Equal(t, []byte("vj1"), iter.UnsafeValue())
	require.False(t, iter.Next())
	require.NoError(t, iter.Error())

	// Then moving to First should be ok.
	require.True(t, iter.First())
	require.Equal(t, []byte("a0"), iter.UnsafeKey())
	require.Equal(t, []byte("va0"), iter.UnsafeValue())

	require.NoError(t, iter.Close())
	// Check that no iterator is leaked.
	require.Zero(t, len(readerPool.mu.readers))
}

func TestPickCompactionFiles(t *testing.T) {
	testCases := []struct {
		allFiles            []*fileMetadata
		compactionThreshold int
		expected            []*fileMetadata
	}{
		{
			// maxDepth: 1
			// 1: a-b
			// 2:   b-c
			// 3:     c-d
			allFiles: []*fileMetadata{
				{fileNum: 1, startKey: []byte("a"), endKey: []byte("b")},
				{fileNum: 2, startKey: []byte("b"), endKey: []byte("c")},
				{fileNum: 3, startKey: []byte("c"), endKey: []byte("d")},
			},
			compactionThreshold: 2,
			expected:            nil,
		},
		{
			// maxDepth: 2
			// 1: a-b
			// 2:   b--d
			// 3:     c--e
			allFiles: []*fileMetadata{
				{fileNum: 1, startKey: []byte("a"), endKey: []byte("b")},
				{fileNum: 2, startKey: []byte("b"), endKey: []byte("d")},
				{fileNum: 3, startKey: []byte("c"), endKey: []byte("e")},
			},
			compactionThreshold: 2,
			expected: []*fileMetadata{
				{fileNum: 2, startKey: []byte("b"), endKey: []byte("d")},
				{fileNum: 3, startKey: []byte("c"), endKey: []byte("e")},
			},
		},
		{
			// maxDepth: 3
			// 1: a---c
			// 2:   b-------f
			// 3:       d------g
			// 4:         e--------i
			// 5:                h----j
			allFiles: []*fileMetadata{
				{fileNum: 1, startKey: []byte("a"), endKey: []byte("c")},
				{fileNum: 2, startKey: []byte("b"), endKey: []byte("f")},
				{fileNum: 3, startKey: []byte("d"), endKey: []byte("g")},
				{fileNum: 4, startKey: []byte("e"), endKey: []byte("i")},
				{fileNum: 5, startKey: []byte("h"), endKey: []byte("j")},
			},
			compactionThreshold: 2,
			expected: []*fileMetadata{
				{fileNum: 1, startKey: []byte("a"), endKey: []byte("c")},
				{fileNum: 2, startKey: []byte("b"), endKey: []byte("f")},
				{fileNum: 3, startKey: []byte("d"), endKey: []byte("g")},
				{fileNum: 4, startKey: []byte("e"), endKey: []byte("i")},
				{fileNum: 5, startKey: []byte("h"), endKey: []byte("j")},
			},
		},
		{
			// maxDepth: 3
			// 1: a---c
			// 2:   b-------f
			// 3:       d------g
			// 4:         e--------i
			// 5:                h----j
			allFiles: []*fileMetadata{
				{fileNum: 1, startKey: []byte("a"), endKey: []byte("c")},
				{fileNum: 2, startKey: []byte("b"), endKey: []byte("f")},
				{fileNum: 3, startKey: []byte("d"), endKey: []byte("g")},
				{fileNum: 4, startKey: []byte("e"), endKey: []byte("i")},
				{fileNum: 5, startKey: []byte("h"), endKey: []byte("j")},
			},
			compactionThreshold: 3,
			expected: []*fileMetadata{
				{fileNum: 2, startKey: []byte("b"), endKey: []byte("f")},
				{fileNum: 3, startKey: []byte("d"), endKey: []byte("g")},
				{fileNum: 4, startKey: []byte("e"), endKey: []byte("i")},
			},
		},
		{
			// maxDepth: 3
			// 1: a---c
			// 2:   b-------f
			// 3:       d------g
			// 4:         e--------i
			// 5:                h----j
			allFiles: []*fileMetadata{
				{fileNum: 1, startKey: []byte("a"), endKey: []byte("c")},
				{fileNum: 2, startKey: []byte("b"), endKey: []byte("f")},
				{fileNum: 3, startKey: []byte("d"), endKey: []byte("g")},
				{fileNum: 4, startKey: []byte("e"), endKey: []byte("i")},
				{fileNum: 5, startKey: []byte("h"), endKey: []byte("j")},
			},
			compactionThreshold: 4,
			expected:            nil,
		},
	}
	for _, tc := range testCases {
		actual := pickCompactionFiles(tc.allFiles, tc.compactionThreshold, zap.NewNop())
		slices.SortFunc(actual, func(a, b *fileMetadata) int {
			return cmp.Compare(a.fileNum, b.fileNum)
		})
		require.Equal(t, tc.expected, actual)
	}
}

func TestSplitCompactionFiles(t *testing.T) {
	testCases := []struct {
		files              []*fileMetadata
		maxCompactionDepth int
		expected           [][]*fileMetadata
	}{
		{
			// 1: a---c
			// 2:   b-------f
			// 3:       d------g
			// 4:         e--------i
			// 5:                h----j
			files: []*fileMetadata{
				{fileNum: 1, startKey: []byte("a"), endKey: []byte("c")},
				{fileNum: 2, startKey: []byte("b"), endKey: []byte("f")},
				{fileNum: 3, startKey: []byte("d"), endKey: []byte("g")},
				{fileNum: 4, startKey: []byte("e"), endKey: []byte("i")},
				{fileNum: 5, startKey: []byte("h"), endKey: []byte("j")},
			},
			maxCompactionDepth: 5,
			expected: [][]*fileMetadata{
				{
					{fileNum: 1, startKey: []byte("a"), endKey: []byte("c")},
					{fileNum: 2, startKey: []byte("b"), endKey: []byte("f")},
					{fileNum: 3, startKey: []byte("d"), endKey: []byte("g")},
					{fileNum: 4, startKey: []byte("e"), endKey: []byte("i")},
					{fileNum: 5, startKey: []byte("h"), endKey: []byte("j")},
				},
			},
		},
		{
			// 1: a---c
			// 2:   b-------f
			// 3:       d------g
			// 4:         e--------i
			// 5:                h----j
			files: []*fileMetadata{
				{fileNum: 1, startKey: []byte("a"), endKey: []byte("c")},
				{fileNum: 2, startKey: []byte("b"), endKey: []byte("f")},
				{fileNum: 3, startKey: []byte("d"), endKey: []byte("g")},
				{fileNum: 4, startKey: []byte("e"), endKey: []byte("i")},
				{fileNum: 5, startKey: []byte("h"), endKey: []byte("j")},
			},
			maxCompactionDepth: 4,
			expected: [][]*fileMetadata{
				{
					{fileNum: 1, startKey: []byte("a"), endKey: []byte("c")},
					{fileNum: 2, startKey: []byte("b"), endKey: []byte("f")},
					{fileNum: 3, startKey: []byte("d"), endKey: []byte("g")},
				},
				{
					{fileNum: 4, startKey: []byte("e"), endKey: []byte("i")},
					{fileNum: 5, startKey: []byte("h"), endKey: []byte("j")},
				},
			},
		},
		{
			// 1: a---c
			// 2:   b-------f
			// 3:       d-e
			// 4:              g---i
			// 5:                h----j
			files: []*fileMetadata{
				{fileNum: 1, startKey: []byte("a"), endKey: []byte("c")},
				{fileNum: 2, startKey: []byte("b"), endKey: []byte("f")},
				{fileNum: 3, startKey: []byte("d"), endKey: []byte("e")},
				{fileNum: 4, startKey: []byte("g"), endKey: []byte("i")},
				{fileNum: 5, startKey: []byte("h"), endKey: []byte("j")},
			},
			maxCompactionDepth: 3,
			expected: [][]*fileMetadata{
				{
					{fileNum: 1, startKey: []byte("a"), endKey: []byte("c")},
					{fileNum: 2, startKey: []byte("b"), endKey: []byte("f")},
					{fileNum: 3, startKey: []byte("d"), endKey: []byte("e")},
				},
				{
					{fileNum: 4, startKey: []byte("g"), endKey: []byte("i")},
					{fileNum: 5, startKey: []byte("h"), endKey: []byte("j")},
				},
			},
		},
	}
	for _, tc := range testCases {
		actual := splitCompactionFiles(tc.files, tc.maxCompactionDepth)
		require.Equal(t, tc.expected, actual)
	}
}

func TestBuildCompactions(t *testing.T) {
	testCases := []struct {
		files             []*fileMetadata
		maxCompactionSize int
		expected          []*compaction
	}{
		{
			files: []*fileMetadata{
				{
					fileNum:  1,
					startKey: []byte("a"),
					endKey:   []byte("c"),
				},
				{
					fileNum:  2,
					startKey: []byte("b"),
					endKey:   []byte("d"),
				},
			},
			maxCompactionSize: 20,
			expected: []*compaction{
				{
					startKey: []byte("a"),
					endKey:   []byte("d"),
					overlapFiles: []*fileMetadata{
						{fileNum: 1}, {fileNum: 2},
					},
				},
			},
		},
		{
			files: []*fileMetadata{
				{
					fileNum:  1,
					startKey: []byte("a"),
					endKey:   []byte("e\x00"),
					kvStats: kvStats{Histogram: []kvStatsBucket{
						{Size: 20, UpperBound: []byte("b")},
						{Size: 23, UpperBound: []byte("c")},
						{Size: 21, UpperBound: []byte("d")},
						{Size: 25, UpperBound: []byte("e")},
					}},
				},
			},
			maxCompactionSize: 20,
			expected: []*compaction{
				{
					startKey: []byte("a"),
					endKey:   []byte("b"),
					overlapFiles: []*fileMetadata{
						{fileNum: 1},
					},
				},
				{
					startKey: []byte("b"),
					endKey:   []byte("c"),
					overlapFiles: []*fileMetadata{
						{fileNum: 1},
					},
				},
				{
					startKey: []byte("c"),
					endKey:   []byte("d"),
					overlapFiles: []*fileMetadata{
						{fileNum: 1},
					},
				},
				{
					startKey: []byte("d"),
					endKey:   []byte("e\x00"),
					overlapFiles: []*fileMetadata{
						{fileNum: 1},
					},
				},
			},
		},
		{
			files: []*fileMetadata{
				{
					fileNum:  1,
					startKey: []byte("a"),
					endKey:   []byte("e\x00"),
					kvStats: kvStats{Histogram: []kvStatsBucket{
						{Size: 20, UpperBound: []byte("b")},
						{Size: 23, UpperBound: []byte("c")},
						{Size: 17, UpperBound: []byte("d")},
						{Size: 25, UpperBound: []byte("e")},
					}},
				},
			},
			maxCompactionSize: 50,
			expected: []*compaction{
				{
					startKey: []byte("a"),
					endKey:   []byte("d"),
					overlapFiles: []*fileMetadata{
						{fileNum: 1},
					},
				},
				{
					startKey: []byte("d"),
					endKey:   []byte("e\x00"),
					overlapFiles: []*fileMetadata{
						{fileNum: 1},
					},
				},
			},
		},
		{
			files: []*fileMetadata{
				{
					fileNum:  1,
					startKey: []byte("a"),
					endKey:   []byte("e\x00"),
					kvStats: kvStats{Histogram: []kvStatsBucket{
						{Size: 20, UpperBound: []byte("b")},
						{Size: 23, UpperBound: []byte("c")},
						{Size: 17, UpperBound: []byte("d")},
						{Size: 25, UpperBound: []byte("e")},
					}},
				},
				{
					fileNum:  2,
					startKey: []byte("c"),
					endKey:   []byte("g\x00"),
					kvStats: kvStats{Histogram: []kvStatsBucket{
						{Size: 21, UpperBound: []byte("d")},
						{Size: 22, UpperBound: []byte("f")},
						{Size: 20, UpperBound: []byte("g")},
					}},
				},
			},
			maxCompactionSize: 50,
			expected: []*compaction{
				{
					startKey: []byte("a"),
					endKey:   []byte("d"),
					overlapFiles: []*fileMetadata{
						{fileNum: 1}, {fileNum: 2},
					},
				},
				{
					startKey: []byte("d"),
					endKey:   []byte("g\x00"),
					overlapFiles: []*fileMetadata{
						{fileNum: 1}, {fileNum: 2},
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		actual := buildCompactions(tc.files, tc.maxCompactionSize)
		require.Equal(t, len(tc.expected), len(actual))
		for i := range tc.expected {
			require.Equal(t, len(tc.expected[i].overlapFiles), len(actual[i].overlapFiles))
			for j := range tc.expected[i].overlapFiles {
				require.Equal(t, tc.expected[i].overlapFiles[j].fileNum, actual[i].overlapFiles[j].fileNum)
			}
		}
	}
}
