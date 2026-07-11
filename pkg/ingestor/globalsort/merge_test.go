// Copyright 2024 PingCAP, Inc.
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

package globalsort

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/ingestor/simplesst"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestSplitDataFiles(t *testing.T) {
	allPaths := make([]string, 0, 110)
	for i := range cap(allPaths) {
		allPaths = append(allPaths, fmt.Sprintf("%d", i))
	}
	cases := []struct {
		paths       []string
		concurrency int
		result      [][]string
	}{
		{
			paths:       allPaths[:1],
			concurrency: 1,
			result:      [][]string{allPaths[:1]},
		},
		{
			paths:       allPaths[:2],
			concurrency: 1,
			result:      [][]string{allPaths[:2]},
		},
		{
			paths:       allPaths[:2],
			concurrency: 4,
			result:      [][]string{allPaths[:2]},
		},
		{
			paths:       allPaths[:3],
			concurrency: 4,
			result:      [][]string{allPaths[:3]},
		},
		{
			paths:       allPaths[:4],
			concurrency: 4,
			result:      [][]string{allPaths[:2], allPaths[2:4]},
		},
		{
			paths:       allPaths[:5],
			concurrency: 4,
			result:      [][]string{allPaths[:3], allPaths[3:5]},
		},
		{
			paths:       allPaths[:6],
			concurrency: 4,
			result:      [][]string{allPaths[:2], allPaths[2:4], allPaths[4:6]},
		},
		{
			paths:       allPaths[:7],
			concurrency: 4,
			result:      [][]string{allPaths[:3], allPaths[3:5], allPaths[5:7]},
		},
		{
			paths:       allPaths[:15],
			concurrency: 4,
			result:      [][]string{allPaths[:4], allPaths[4:8], allPaths[8:12], allPaths[12:15]},
		},
		{
			paths:       allPaths[:83],
			concurrency: 4,
			result:      [][]string{allPaths[:21], allPaths[21:42], allPaths[42:63], allPaths[63:83]},
		},
		{
			paths:       allPaths[:100],
			concurrency: 4,
			result:      [][]string{allPaths[:25], allPaths[25:50], allPaths[50:75], allPaths[75:100]},
		},
		{
			paths:       allPaths[:100],
			concurrency: 8,
			result: [][]string{
				allPaths[:13], allPaths[13:26], allPaths[26:39], allPaths[39:52],
				allPaths[52:64], allPaths[64:76], allPaths[76:88], allPaths[88:100],
			},
		},
	}
	for i, c := range cases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			result := splitDataFiles(c.paths, c.concurrency)
			require.Equal(t, c.result, result)
		})
	}

	bak := MaxMergingFilesPerThread
	t.Cleanup(func() {
		MaxMergingFilesPerThread = bak
	})
	MaxMergingFilesPerThread = 10
	require.Equal(t, [][]string{
		allPaths[:10], allPaths[10:19], allPaths[19:28], allPaths[28:37],
		allPaths[37:46], allPaths[46:55], allPaths[55:64], allPaths[64:73],
		allPaths[73:82], allPaths[82:91],
	}, splitDataFiles(allPaths[:91], 8))
	require.Equal(t, [][]string{
		allPaths[:10], allPaths[10:20], allPaths[20:30], allPaths[30:40],
		allPaths[40:50], allPaths[50:60], allPaths[60:70], allPaths[70:80],
		allPaths[80:90], allPaths[90:99],
	}, splitDataFiles(allPaths[:99], 8))
	require.Equal(t, [][]string{
		allPaths[:10], allPaths[10:20], allPaths[20:29], allPaths[29:38],
		allPaths[38:47], allPaths[47:56], allPaths[56:65], allPaths[65:74],
		allPaths[74:83], allPaths[83:92], allPaths[92:101],
	}, splitDataFiles(allPaths[:101], 8))
}

func TestMergeOperator(t *testing.T) {
	oldMaxMergingFilesPerThread := MaxMergingFilesPerThread
	MaxMergingFilesPerThread = 2
	defer func() {
		MaxMergingFilesPerThread = oldMaxMergingFilesPerThread
	}()

	// test different error cause
	testcases := []struct {
		failpointValue string
		expectError    error
	}{
		{
			failpointValue: "return(0)",
			expectError:    nil,
		},
		{
			failpointValue: "return(1)",
			expectError:    errors.Errorf("mock error in mergeOverlappingFilesInternal"),
		},
		{
			failpointValue: "return(2)",
			expectError:    errors.Errorf("task panic: merge_sort, func info: mergeMinimalTask"),
		},
		{
			failpointValue: "return(3)",
			expectError:    context.DeadlineExceeded,
		},
	}

	for _, tc := range testcases {
		testfailpoint.Enable(t,
			"github.com/pingcap/tidb/pkg/ingestor/globalsort/mergeOverlappingFilesInternal",
			tc.failpointValue,
		)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		wctx := workerpool.NewContext(ctx)

		op := NewMergeOperator(
			wctx,
			nil,
			0,
			"",
			0,
			nil,
			nil,
			1,
			false,
			engineapi.OnDuplicateKeyIgnore,
		)

		datas := []string{
			"/tmp/1",
			"/tmp/2",
			"/tmp/3",
			"/tmp/4",
			"/tmp/5",
			"/tmp/6",
		}

		err := MergeOverlappingFiles(
			wctx,
			datas,
			1,
			op,
		)

		if tc.expectError != nil {
			require.True(t, errors.ErrorEqual(err, tc.expectError))
		} else {
			require.NoError(t, err)
		}

		cancel()
	}
}

func TestMergeOverlappingFilesInternal(t *testing.T) {
	changePropDist(t, simplesst.DefaultPropSizeDist, 2)
	// 1. Write to 3 files.
	// 2. merge 3 files into one file.
	// 3. read one file and check result.
	// 4. check duplicate key.
	var kvAndStats [][2]string
	ctx := context.Background()
	memStore := objstore.NewMemStorage()
	writer := simplesst.NewWriterBuilder().
		SetMemorySizeLimit(1000).
		SetOnCloseFunc(func(summary *simplesst.WriterSummary) { kvAndStats = summary.MultipleFilesStats[0].Filenames }).
		Build(memStore, "/test", "0")

	kvCount := 2000000
	kvSize := 0
	for i := range kvCount {
		v := i
		if v == kvCount/2 {
			v-- // insert a duplicate key.
		}
		key, val := []byte{byte(v)}, []byte{byte(v)}
		kvSize += len(key) + len(val)
		require.NoError(t, writer.WriteRow(ctx, key, val, kv.IntHandle(i)))
	}
	require.NoError(t, writer.Close(ctx))
	readBufSizeBak := simplesst.DefaultReadBufferSize
	memLimitBak := simplesst.DefaultOneWriterMemSizeLimit
	t.Cleanup(func() {
		simplesst.DefaultReadBufferSize = readBufSizeBak
		simplesst.DefaultOneWriterMemSizeLimit = memLimitBak
	})
	simplesst.DefaultReadBufferSize = 100
	simplesst.DefaultOneWriterMemSizeLimit = 1000

	collector := &execute.TestCollector{}

	dataFiles := make([]string, 0, len(kvAndStats))
	for _, f := range kvAndStats {
		dataFiles = append(dataFiles, f[0])
	}
	var onefile [2]string
	require.NoError(t, mergeOverlappingFilesInternal(
		ctx,
		dataFiles,
		memStore,
		int64(5*size.MB),
		"/test2",
		"mergeID",
		1000,
		func(summary *simplesst.WriterSummary) { onefile = summary.MultipleFilesStats[0].Filenames[0] },
		collector,
		true,
		engineapi.OnDuplicateKeyIgnore,
		1,
	))

	require.EqualValues(t, kvCount, collector.Rows.Load())
	require.EqualValues(t, kvSize, collector.ProcessedCnt.Load())

	kvs := make([]simplesst.KVPair, 0, kvCount)

	kvReader, err := simplesst.NewKVReader(ctx, onefile[0], memStore, 0, 100)
	require.NoError(t, err)
	for range kvCount {
		key, value, err := kvReader.NextKV()
		require.NoError(t, err)
		clonedKey := make([]byte, len(key))
		copy(clonedKey, key)
		clonedVal := make([]byte, len(value))
		copy(clonedVal, value)
		kvs = append(kvs, simplesst.KVPair{Key: clonedKey, Value: clonedVal})
	}
	_, _, err = kvReader.NextKV()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, kvReader.Close())

	data := &MemoryIngestData{
		kvs: kvs,
		ts:  123,
	}
	pool := membuf.NewPool()
	defer pool.Destroy()
	iter := data.NewIter(ctx, nil, nil, pool)

	for iter.First(); iter.Valid(); iter.Next() {
	}
	err = iter.Error()
	require.NoError(t, err)
}

func TestOnefileWriterManyRows(t *testing.T) {
	changePropDist(t, simplesst.DefaultPropSizeDist, 2)
	// 1. write into one file with sorted order.
	// 2. merge one file.
	// 3. read kv file and check the result.
	// 4. check the writeSummary.
	var kvAndStat [2]string
	ctx := context.Background()
	memStore := objstore.NewMemStorage()
	writer := simplesst.NewWriterBuilder().
		SetMemorySizeLimit(1000).
		SetOnCloseFunc(func(summary *simplesst.WriterSummary) { kvAndStat = summary.MultipleFilesStats[0].Filenames[0] }).
		BuildOneFile(memStore, "/test", "0")

	writer.InitPartSizeAndLogger(ctx, 5*1024*1024)

	kvCnt := 100000
	expectedTotalSize := 0
	kvs := make([]common.KvPair, kvCnt)
	for i := range kvCnt {
		randLen := rand.Intn(10) + 1
		kvs[i].Key = make([]byte, randLen)
		_, err := rand.Read(kvs[i].Key)
		expectedTotalSize += randLen

		require.NoError(t, err)
		randLen = rand.Intn(10) + 1
		kvs[i].Val = make([]byte, randLen)
		_, err = rand.Read(kvs[i].Val)
		require.NoError(t, err)
		expectedTotalSize += randLen
	}

	slices.SortFunc(kvs, func(i, j common.KvPair) int {
		return bytes.Compare(i.Key, j.Key)
	})

	for _, item := range kvs {
		require.NoError(t, writer.WriteRow(ctx, item.Key, item.Val))
	}
	require.NoError(t, writer.Close(ctx))

	var resSummary *simplesst.WriterSummary
	onClose := func(summary *simplesst.WriterSummary) {
		resSummary = summary
	}
	readBufSizeBak := simplesst.DefaultReadBufferSize
	memLimitBak := simplesst.DefaultOneWriterMemSizeLimit
	t.Cleanup(func() {
		simplesst.DefaultReadBufferSize = readBufSizeBak
		simplesst.DefaultOneWriterMemSizeLimit = memLimitBak
	})
	simplesst.DefaultReadBufferSize = 100
	simplesst.DefaultOneWriterMemSizeLimit = 1000
	require.NoError(t, mergeOverlappingFilesInternal(
		ctx,
		[]string{kvAndStat[0]},
		memStore,
		int64(5*size.MB),
		"/test2",
		"mergeID",
		1000,
		onClose,
		nil,
		true,
		engineapi.OnDuplicateKeyIgnore,
		1,
	))

	bufSize := rand.Intn(100) + 1
	kvAndStat2 := resSummary.MultipleFilesStats[0].Filenames[0]
	kvReader, err := simplesst.NewKVReader(ctx, kvAndStat2[0], memStore, 0, bufSize)
	require.NoError(t, err)
	for i := range kvCnt {
		key, value, err := kvReader.NextKV()
		require.NoError(t, err)
		require.Equal(t, kvs[i].Key, key)
		require.Equal(t, kvs[i].Val, value)
	}
	_, _, err = kvReader.NextKV()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, kvReader.Close())

	// check writerSummary.
	expected := simplesst.MultipleFilesStat{
		MinKey:            kvs[0].Key,
		MaxKey:            kvs[len(kvs)-1].Key,
		Filenames:         [][2]string{kvAndStat2},
		MaxOverlappingNum: 1,
	}
	require.EqualValues(t, expected.MinKey, resSummary.Min)
	require.EqualValues(t, expected.MaxKey, resSummary.Max)
	require.Equal(t, expected.Filenames, resSummary.MultipleFilesStats[0].Filenames)
	require.Equal(t, expected.MaxOverlappingNum, resSummary.MultipleFilesStats[0].MaxOverlappingNum)
	require.EqualValues(t, expectedTotalSize, resSummary.TotalSize)
	require.EqualValues(t, kvCnt, resSummary.TotalCnt)
}
