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
	"encoding/json"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestDiskSorterCommon(t *testing.T) {
	sorter, err := OpenDiskSorter(t.TempDir(), &DiskSorterOptions{
		WriterBufferSize: 32 * 1024,
	})
	require.NoError(t, err)
	runCommonTest(t, sorter)
	require.NoError(t, sorter.Close())
}

func TestDiskSorterCommonParallel(t *testing.T) {
	sorter, err := OpenDiskSorter(t.TempDir(), &DiskSorterOptions{
		WriterBufferSize: 32 * 1024,
	})
	require.NoError(t, err)
	runCommonParallelTest(t, sorter)
	require.NoError(t, sorter.Close())
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

	pool := newSSTReaderPool(fs, dirname, pebble.NewCache(8<<20))

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

// touchSSTFile creates an empty SST file.
func writeSSTFile(
	t *testing.T,
	fs vfs.FS,
	dirname string,
	fileNum int,
	kvs []keyValue,
) {
	sw, err := newSSTWriter(fs, dirname, fileNum, 8, func(meta *fileMetadata) {})
	require.NoError(t, err)
	for _, kv := range kvs {
		require.NoError(t, sw.Set(kv.key, kv.value))
	}
	require.NoError(t, sw.Close())
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
	// TODO
}

func TestCompactFiles(t *testing.T) {
	// TODO
}

func TestPickCompactionFiles(t *testing.T) {
	// TODO
}

func TestSplitCompactionFiles(t *testing.T) {
	// TODO
}

func TestBuildCompactions(t *testing.T) {
	// TODO
}
