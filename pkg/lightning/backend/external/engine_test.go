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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"golang.org/x/sync/errgroup"
)

func testGetFirstAndLastKey(
	t *testing.T,
	data common.IngestData,
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
	data common.IngestData,
	lowerBound, upperBound []byte,
	expectedKVs []KVPair,
	bufPool *membuf.Pool,
) {
	ctx := context.Background()
	iter := data.NewIter(ctx, lowerBound, upperBound, bufPool)
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

	// MemoryIngestData without duplicate detection feature does not need pool
	testNewIter(t, data, nil, nil, kvs, nil)
	testNewIter(t, data, []byte("key1"), []byte("key6"), kvs, nil)
	testNewIter(t, data, []byte("key2"), []byte("key5"), kvs[1:4], nil)
	testNewIter(t, data, []byte("key25"), []byte("key35"), kvs[2:3], nil)
	testNewIter(t, data, []byte("key25"), []byte("key26"), nil, nil)
	testNewIter(t, data, []byte("key0"), []byte("key1"), nil, nil)
	testNewIter(t, data, []byte("key6"), []byte("key9"), nil, nil)

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

func prepareKVFiles(t *testing.T, store storage.ExternalStorage, contents [][]KVPair) (dataFiles, statFiles []string) {
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

func getAllDataFromDataAndRanges(t *testing.T, dataAndRanges *common.DataAndRanges) []KVPair {
	ctx := context.Background()
	iter := dataAndRanges.Data.NewIter(ctx, nil, nil, membuf.NewPool())
	var allKVs []KVPair
	for iter.First(); iter.Valid(); iter.Next() {
		allKVs = append(allKVs, KVPair{Key: iter.Key(), Value: iter.Value()})
	}
	require.NoError(t, iter.Close())
	return allKVs
}

func TestLoadRangeBatchReleasesReadersWhileWaitingForDownstream(t *testing.T) {
	t.Run("already released data still allows retry", func(t *testing.T) {
		extEngine := &Engine{
			dataReleaseCh: make(chan struct{}, 1),
		}
		extEngine.dataReleaseCh <- struct{}{}
		require.NoError(t, extEngine.waitIngestDataReleased(context.Background()))
	})

	t.Run("concurrent release between signal check and count check still allows retry", func(t *testing.T) {
		extEngine := &Engine{
			dataReleaseCh: make(chan struct{}, 1),
		}
		extEngine.inFlightDataCount.Store(1)
		const failpointName = "github.com/pingcap/tidb/pkg/lightning/backend/external/waitIngestDataReleasedBeforeCountCheck"
		require.NoError(t, failpoint.EnableCall(failpointName, func() {
			extEngine.onIngestDataReleased()
		}))
		t.Cleanup(func() {
			require.NoError(t, failpoint.Disable(failpointName))
		})
		require.NoError(t, extEngine.waitIngestDataReleased(context.Background()))
	})

	t.Run("wait log includes in-flight data count", func(t *testing.T) {
		core, logs := observer.New(zap.InfoLevel)
		ctx := context.WithValue(context.Background(), logutil.CtxLogKey, zap.New(core))
		extEngine := &Engine{
			dataReleaseCh: make(chan struct{}, 1),
		}
		extEngine.inFlightDataCount.Store(7)

		errCh := make(chan error, 1)
		go func() {
			errCh <- extEngine.waitIngestDataReleased(ctx)
		}()
		require.Eventually(t, func() bool {
			return logs.FilterMessage("wait for downstream to release loaded data before retrying read").Len() == 1
		}, time.Second, 10*time.Millisecond)
		extEngine.dataReleaseCh <- struct{}{}
		require.NoError(t, <-errCh)

		fields := logs.All()[0].ContextMap()
		require.EqualValues(t, 7, fields["inFlightDataCount"])
	})

	ctx := context.Background()
	store := &trackOpenMemStorage{MemStorage: storage.NewMemStorage()}
	dataFiles, statFiles := prepareKVFiles(t, store, [][]KVPair{{
		{Key: []byte{1}, Value: []byte("first")},
		{Key: []byte{2}, Value: []byte("second")},
	}})
	extEngine := NewExternalEngine(
		store,
		dataFiles,
		statFiles,
		[]byte{1},
		[]byte{3},
		[][]byte{{1}, {2}, {3}},
		[][]byte{{1}, {2}, {3}},
		1,
		123,
		2,
		2,
		true,
		4*units.MiB,
		common.OnDuplicateKeyIgnore,
		"/",
	)
	t.Cleanup(func() {
		require.NoError(t, extEngine.Close())
	})

	loadDataCh := make(chan common.DataAndRanges)
	errCh := make(chan error, 1)
	go func() {
		errCh <- extEngine.LoadIngestData(ctx, loadDataCh)
	}()

	first := <-loadDataCh
	first.Data.IncRef()
	firstReleased := false
	t.Cleanup(func() {
		if !firstReleased {
			first.Data.DecRef()
		}
	})

	// Each batch opens both stats and data files on this release branch. Four
	// opens prove that the second batch has reached the data reader.
	require.Eventually(t, func() bool {
		return store.totalOpened.Load() >= 4
	}, 3*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return store.opened.Load() == 0
	}, 3*time.Second, 10*time.Millisecond, "failed memory acquire should close readers before waiting for downstream release")

	first.Data.DecRef()
	firstReleased = true

	var second common.DataAndRanges
	require.Eventually(t, func() bool {
		select {
		case second = <-loadDataCh:
			return true
		default:
			return false
		}
	}, 3*time.Second, 10*time.Millisecond)
	second.Data.IncRef()
	defer second.Data.DecRef()

	require.Equal(t, []KVPair{{Key: []byte{2}, Value: []byte("second")}}, getAllDataFromDataAndRanges(t, &second))
	require.NoError(t, <-errCh)
}

type dummyWorker struct{}

func (w *dummyWorker) Tune(int32, bool) {
}

func TestChangeEngineConcurrency(t *testing.T) {
	var (
		outCh     chan common.DataAndRanges
		eg        errgroup.Group
		e         *Engine
		finished  atomic.Int32
		updatedCh chan struct{}
	)

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/lightning/backend/external/mockLoadBatchRegionData", "return(true)")

	resetFn := func() {
		outCh = make(chan common.DataAndRanges, 4)
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
