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

package external

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
)

func testGetFirstAndLastKey(
	t *testing.T,
	data engineapi.IngestData,
	lowerBound, upperBound []byte,
	expectedFirstKey, expectedLastKey []byte,
) {
	firstKey, lastKey, err := data.GetFirstAndLastKey(lowerBound, upperBound)
	require.NoError(t, err)
	require.Equal(t, expectedFirstKey, firstKey)
	require.Equal(t, expectedLastKey, lastKey)
}

func testNewIter(
	t *testing.T,
	data engineapi.IngestData,
	lowerBound, upperBound []byte,
	expectedKVs []KVPair,
) {
	ctx := context.Background()
	iter := data.NewIter(ctx, lowerBound, upperBound, nil)
	var kvs []KVPair
	for iter.First(); iter.Valid(); iter.Next() {
		require.NoError(t, iter.Error())
		kvs = append(kvs, KVPair{Key: iter.Key(), Value: iter.Value()})
	}
	require.NoError(t, iter.Error())
	require.NoError(t, iter.Close())
	require.Equal(t, expectedKVs, kvs)
}

func TestMemoryIngestData(t *testing.T) {
	kvs := []KVPair{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
		{Key: []byte("key4"), Value: []byte("value4")},
		{Key: []byte("key5"), Value: []byte("value5")},
	}
	data := &MemoryIngestData{
		kvs: kvs,
		ts:  123,
	}

	require.EqualValues(t, 123, data.GetTS())
	testGetFirstAndLastKey(t, data, nil, nil, []byte("key1"), []byte("key5"))
	testGetFirstAndLastKey(t, data, []byte("key1"), []byte("key6"), []byte("key1"), []byte("key5"))
	testGetFirstAndLastKey(t, data, []byte("key2"), []byte("key5"), []byte("key2"), []byte("key4"))
	testGetFirstAndLastKey(t, data, []byte("key25"), []byte("key35"), []byte("key3"), []byte("key3"))
	testGetFirstAndLastKey(t, data, []byte("key25"), []byte("key26"), nil, nil)
	testGetFirstAndLastKey(t, data, []byte("key0"), []byte("key1"), nil, nil)
	testGetFirstAndLastKey(t, data, []byte("key6"), []byte("key9"), nil, nil)

	testNewIter(t, data, nil, nil, kvs)
	testNewIter(t, data, []byte("key1"), []byte("key6"), kvs)
	testNewIter(t, data, []byte("key2"), []byte("key5"), kvs[1:4])
	testNewIter(t, data, []byte("key25"), []byte("key35"), kvs[2:3])
	testNewIter(t, data, []byte("key25"), []byte("key26"), nil)
	testNewIter(t, data, []byte("key0"), []byte("key1"), nil)
	testNewIter(t, data, []byte("key6"), []byte("key9"), nil)

	data = &MemoryIngestData{
		ts: 234,
	}
	encodedKVs := make([]KVPair, 0, len(kvs)*2)
	duplicatedKVs := make([]KVPair, 0, len(kvs)*2)

	for i := range kvs {
		encodedKey := slices.Clone(kvs[i].Key)
		encodedKVs = append(encodedKVs, KVPair{Key: encodedKey, Value: kvs[i].Value})
		if i%2 == 0 {
			continue
		}

		// duplicatedKeys will be like key2_0, key2_1, key4_0, key4_1
		duplicatedKVs = append(duplicatedKVs, KVPair{Key: encodedKey, Value: kvs[i].Value})

		encodedKey = slices.Clone(kvs[i].Key)
		newValues := make([]byte, len(kvs[i].Value)+1)
		copy(newValues, kvs[i].Value)
		newValues[len(kvs[i].Value)] = 1
		encodedKVs = append(encodedKVs, KVPair{Key: encodedKey, Value: newValues})
		duplicatedKVs = append(duplicatedKVs, KVPair{Key: encodedKey, Value: newValues})
	}
	data.kvs = encodedKVs

	require.EqualValues(t, 234, data.GetTS())
	testGetFirstAndLastKey(t, data, nil, nil, []byte("key1"), []byte("key5"))
	testGetFirstAndLastKey(t, data, []byte("key1"), []byte("key6"), []byte("key1"), []byte("key5"))
	testGetFirstAndLastKey(t, data, []byte("key2"), []byte("key5"), []byte("key2"), []byte("key4"))
	testGetFirstAndLastKey(t, data, []byte("key25"), []byte("key35"), []byte("key3"), []byte("key3"))
	testGetFirstAndLastKey(t, data, []byte("key25"), []byte("key26"), nil, nil)
	testGetFirstAndLastKey(t, data, []byte("key0"), []byte("key1"), nil, nil)
	testGetFirstAndLastKey(t, data, []byte("key6"), []byte("key9"), nil, nil)
}

func prepareKVFiles(t *testing.T, store storeapi.Storage, contents [][]KVPair) (dataFiles, statFiles []string) {
	ctx := context.Background()
	for i, c := range contents {
		var summary *WriterSummary
		// we want to create a file for each content, so make the below size larger.
		writer := NewWriterBuilder().SetPropKeysDistance(4).
			SetMemorySizeLimit(8*units.MiB).SetBlockSize(8*units.MiB).
			SetOnCloseFunc(func(s *WriterSummary) { summary = s }).
			Build(store, "/test", fmt.Sprintf("%d", i))
		for _, p := range c {
			require.NoError(t, writer.WriteRow(ctx, p.Key, p.Value, nil))
		}
		require.NoError(t, writer.Close(ctx))
		require.Len(t, summary.MultipleFilesStats, 1)
		require.Len(t, summary.MultipleFilesStats[0].Filenames, 1)
		require.Zero(t, summary.ConflictInfo.Count)
		require.Empty(t, summary.ConflictInfo.Files)
		dataFiles = append(dataFiles, summary.MultipleFilesStats[0].Filenames[0][0])
		statFiles = append(statFiles, summary.MultipleFilesStats[0].Filenames[0][1])
	}
	return
}

func getAllDataFromDataAndRanges(t *testing.T, dataAndRanges *engineapi.DataAndRanges) []KVPair {
	ctx := context.Background()
	iter := dataAndRanges.Data.NewIter(ctx, nil, nil, membuf.NewPool())
	var allKVs []KVPair
	for iter.First(); iter.Valid(); iter.Next() {
		allKVs = append(allKVs, KVPair{Key: iter.Key(), Value: iter.Value()})
	}
	require.NoError(t, iter.Close())
	return allKVs
}

func TestEngineOnDup(t *testing.T) {
	ctx := context.Background()
	contents := [][]KVPair{{
		{Key: []byte{4}, Value: []byte("bbb")},
		{Key: []byte{4}, Value: []byte("bbb")},
		{Key: []byte{1}, Value: []byte("aa")},
		{Key: []byte{1}, Value: []byte("aa")},
		{Key: []byte{1}, Value: []byte("aa")},
		{Key: []byte{2}, Value: []byte("vv")},
		{Key: []byte{3}, Value: []byte("sds")},
	}}

	getEngineFn := func(store storeapi.Storage, onDup engineapi.OnDuplicateKey, inDataFiles, inStatFiles []string) *Engine {
		return NewExternalEngine(
			ctx,
			store, inDataFiles, inStatFiles,
			[]byte{1}, []byte{5},
			[][]byte{{1}, {2}, {3}, {4}, {5}},
			[][]byte{{1}, {3}, {5}},
			10,
			123,
			456,
			789,
			true,
			16*units.GiB,
			onDup,
			"/",
		)
	}

	t.Run("on duplicate ignore", func(t *testing.T) {
		onDup := engineapi.OnDuplicateKeyIgnore
		store := objstore.NewMemStorage()
		dataFiles, statFiles := prepareKVFiles(t, store, contents)
		extEngine := getEngineFn(store, onDup, dataFiles, statFiles)
		loadDataCh := make(chan engineapi.DataAndRanges, 4)
		require.ErrorContains(t, extEngine.LoadIngestData(ctx, loadDataCh), "duplicate key found")
		t.Cleanup(func() {
			require.NoError(t, extEngine.Close())
		})
	})

	t.Run("on duplicate error", func(t *testing.T) {
		onDup := engineapi.OnDuplicateKeyError
		store := objstore.NewMemStorage()
		dataFiles, statFiles := prepareKVFiles(t, store, contents)
		extEngine := getEngineFn(store, onDup, dataFiles, statFiles)
		loadDataCh := make(chan engineapi.DataAndRanges, 4)
		require.ErrorContains(t, extEngine.LoadIngestData(ctx, loadDataCh), "[Lightning:Restore:ErrFoundDuplicateKey]found duplicate key '\x01', value 'aa'")
		t.Cleanup(func() {
			require.NoError(t, extEngine.Close())
		})
	})

	t.Run("on duplicate record or remove, no duplicates", func(t *testing.T) {
		for _, od := range []engineapi.OnDuplicateKey{engineapi.OnDuplicateKeyRecord, engineapi.OnDuplicateKeyRemove} {
			store := objstore.NewMemStorage()
			dfiles, sfiles := prepareKVFiles(t, store, [][]KVPair{{
				{Key: []byte{4}, Value: []byte("bbb")},
				{Key: []byte{1}, Value: []byte("aa")},
				{Key: []byte{2}, Value: []byte("vv")},
				{Key: []byte{3}, Value: []byte("sds")},
			}})
			extEngine := getEngineFn(store, od, dfiles, sfiles)
			loadDataCh := make(chan engineapi.DataAndRanges, 4)
			require.NoError(t, extEngine.LoadIngestData(ctx, loadDataCh))
			t.Cleanup(func() {
				require.NoError(t, extEngine.Close())
			})
			require.Len(t, loadDataCh, 1)
			dataAndRanges := <-loadDataCh
			allKVs := getAllDataFromDataAndRanges(t, &dataAndRanges)
			require.EqualValues(t, []KVPair{
				{Key: []byte{1}, Value: []byte("aa")},
				{Key: []byte{2}, Value: []byte("vv")},
				{Key: []byte{3}, Value: []byte("sds")},
				{Key: []byte{4}, Value: []byte("bbb")},
			}, allKVs)
			info := extEngine.ConflictInfo()
			require.Zero(t, info.Count)
			require.Empty(t, info.Files)
		}
	})

	t.Run("on duplicate record or remove, partial duplicated", func(t *testing.T) {
		contents2 := [][]KVPair{
			{{Key: []byte{1}, Value: []byte("aa")}, {Key: []byte{1}, Value: []byte("aa")}},
			{{Key: []byte{1}, Value: []byte("aa")}, {Key: []byte{2}, Value: []byte("vv")}, {Key: []byte{3}, Value: []byte("sds")}},
			{{Key: []byte{4}, Value: []byte("bbb")}, {Key: []byte{4}, Value: []byte("bbb")}},
		}
		for _, cont := range [][][]KVPair{contents, contents2} {
			for _, od := range []engineapi.OnDuplicateKey{engineapi.OnDuplicateKeyRecord, engineapi.OnDuplicateKeyRemove} {
				store := objstore.NewMemStorage()
				dataFiles, statFiles := prepareKVFiles(t, store, cont)
				extEngine := getEngineFn(store, od, dataFiles, statFiles)
				loadDataCh := make(chan engineapi.DataAndRanges, 4)
				require.NoError(t, extEngine.LoadIngestData(ctx, loadDataCh))
				t.Cleanup(func() {
					require.NoError(t, extEngine.Close())
				})
				require.Len(t, loadDataCh, 1)
				dataAndRanges := <-loadDataCh
				allKVs := getAllDataFromDataAndRanges(t, &dataAndRanges)
				require.EqualValues(t, []KVPair{
					{Key: []byte{2}, Value: []byte("vv")},
					{Key: []byte{3}, Value: []byte("sds")},
				}, allKVs)
				info := extEngine.ConflictInfo()
				if od == engineapi.OnDuplicateKeyRemove {
					require.Zero(t, info.Count)
					require.Empty(t, info.Files)
				} else {
					require.EqualValues(t, 5, info.Count)
					require.Len(t, info.Files, 1)
					dupPairs := readKVFile(t, store, info.Files[0])
					require.EqualValues(t, []KVPair{
						{Key: []byte{1}, Value: []byte("aa")},
						{Key: []byte{1}, Value: []byte("aa")},
						{Key: []byte{1}, Value: []byte("aa")},
						{Key: []byte{4}, Value: []byte("bbb")},
						{Key: []byte{4}, Value: []byte("bbb")},
					}, dupPairs)
				}
			}
		}
	})

	t.Run("on duplicate record or remove, all duplicated", func(t *testing.T) {
		for _, od := range []engineapi.OnDuplicateKey{engineapi.OnDuplicateKeyRecord, engineapi.OnDuplicateKeyRemove} {
			store := objstore.NewMemStorage()
			dfiles, sfiles := prepareKVFiles(t, store, [][]KVPair{{
				{Key: []byte{1}, Value: []byte("aaa")},
				{Key: []byte{1}, Value: []byte("aaa")},
				{Key: []byte{1}, Value: []byte("aaa")},
				{Key: []byte{1}, Value: []byte("aaa")},
			}})
			extEngine := getEngineFn(store, od, dfiles, sfiles)
			loadDataCh := make(chan engineapi.DataAndRanges, 4)
			require.NoError(t, extEngine.LoadIngestData(ctx, loadDataCh))
			t.Cleanup(func() {
				require.NoError(t, extEngine.Close())
			})
			require.Len(t, loadDataCh, 1)
			dataAndRanges := <-loadDataCh
			allKVs := getAllDataFromDataAndRanges(t, &dataAndRanges)
			require.Empty(t, allKVs)
			info := extEngine.ConflictInfo()
			if od == engineapi.OnDuplicateKeyRemove {
				require.Zero(t, info.Count)
				require.Empty(t, info.Files)
			} else {
				require.EqualValues(t, 4, info.Count)
				require.Len(t, info.Files, 1)
				dupPairs := readKVFile(t, store, info.Files[0])
				require.EqualValues(t, []KVPair{
					{Key: []byte{1}, Value: []byte("aaa")},
					{Key: []byte{1}, Value: []byte("aaa")},
					{Key: []byte{1}, Value: []byte("aaa")},
					{Key: []byte{1}, Value: []byte("aaa")},
				}, dupPairs)
			}
		}
	})
}

type dummyWorker struct{}

func (w *dummyWorker) Tune(int32, bool) {
}

func TestChangeEngineConcurrency(t *testing.T) {
	var (
		outCh     chan engineapi.DataAndRanges
		eg        errgroup.Group
		e         *Engine
		finished  atomic.Int32
		updatedCh chan struct{}
	)

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/lightning/backend/external/mockLoadBatchRegionData", "return(true)")

	resetFn := func() {
		outCh = make(chan engineapi.DataAndRanges, 4)
		updatedCh = make(chan struct{})
		eg = errgroup.Group{}
		finished.Store(0)
		e = &Engine{
			jobKeys:           make([][]byte, 64),
			workerConcurrency: *atomic.NewInt32(4),
			readyCh:           make(chan struct{}),
		}
		e.SetWorkerPool(&dummyWorker{})

		// Load and consume the data
		eg.Go(func() error {
			defer close(outCh)
			return e.LoadIngestData(context.Background(), outCh)
		})
		eg.Go(func() error {
			<-updatedCh
			for data := range outCh {
				data.Data.DecRef()
				finished.Add(1)
			}
			return nil
		})
	}

	t.Run("reduce concurrency", func(t *testing.T) {
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/lightning/backend/external/afterUpdateWorkerConcurrency", func() {
			updatedCh <- struct{}{}
		})

		resetFn()
		// Make sure update concurrency is triggered.
		require.Eventually(t, func() bool {
			return len(outCh) >= 4
		}, 5*time.Second, 10*time.Millisecond)
		require.NoError(t, e.UpdateResource(context.Background(), 1, 1024))
		require.NoError(t, eg.Wait())
	})

	t.Run("increase concurrency", func(t *testing.T) {
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/lightning/backend/external/afterUpdateWorkerConcurrency", func() {
			updatedCh <- struct{}{}
		})

		resetFn()
		// Make sure update concurrency is triggered.
		require.Eventually(t, func() bool {
			return len(outCh) >= 4
		}, 5*time.Second, 10*time.Millisecond)
		require.NoError(t, e.UpdateResource(context.Background(), 8, 1024))
		require.NoError(t, eg.Wait())
	})

	t.Run("increase concurrency after loading all data", func(t *testing.T) {
		resetFn()
		close(updatedCh)
		// Wait all the data being processed
		require.Eventually(t, func() bool {
			return finished.Load() >= 16
		}, 3*time.Second, 10*time.Millisecond)
		require.NoError(t, e.UpdateResource(context.Background(), 8, 1024))
		require.NoError(t, eg.Wait())
	})
}
