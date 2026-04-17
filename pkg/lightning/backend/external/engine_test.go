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
	"slices"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
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
