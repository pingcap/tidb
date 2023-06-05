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
	"testing"

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
