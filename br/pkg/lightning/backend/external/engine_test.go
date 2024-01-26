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
	"bytes"
	"context"
	"fmt"
	"path"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestIter(t *testing.T) {
	seed := time.Now().Unix()
	rand.Seed(uint64(seed))
	t.Logf("seed: %d", seed)

	totalKV := 300
	kvPairs := make([]common.KvPair, totalKV)
	for i := range kvPairs {
		keyBuf := make([]byte, rand.Intn(10)+1)
		rand.Read(keyBuf)
		// make sure the key is unique
		kvPairs[i].Key = append(keyBuf, byte(i/255), byte(i%255))
		valBuf := make([]byte, rand.Intn(10)+1)
		rand.Read(valBuf)
		kvPairs[i].Val = valBuf
	}

	sortedKVPairs := make([]common.KvPair, totalKV)
	copy(sortedKVPairs, kvPairs)
	slices.SortFunc(sortedKVPairs, func(i, j common.KvPair) int {
		return bytes.Compare(i.Key, j.Key)
	})

	ctx := context.Background()
	store := storage.NewMemStorage()

	for i := 0; i < 3; i++ {
		w := NewWriterBuilder().
			SetMemorySizeLimit(uint64(rand.Intn(100)+1)).
			SetPropSizeDistance(uint64(rand.Intn(50)+1)).
			SetPropKeysDistance(uint64(rand.Intn(10)+1)).
			Build(store, "/subtask", strconv.Itoa(i))
		kvStart := i * 100
		kvEnd := (i + 1) * 100
		for j := kvStart; j < kvEnd; j++ {
			err := w.WriteRow(ctx, kvPairs[j].Key, kvPairs[j].Val, nil)
			require.NoError(t, err)
		}
		err := w.Close(ctx)
		require.NoError(t, err)
	}

	dataFiles, statFiles, err := GetAllFileNames(ctx, store, "/subtask")
	require.NoError(t, err)

	engine := Engine{
		storage:    store,
		dataFiles:  dataFiles,
		statsFiles: statFiles,
	}
	iter, err := engine.createMergeIter(ctx, sortedKVPairs[0].Key)
	require.NoError(t, err)
	got := make([]common.KvPair, 0, totalKV)
	for iter.Next() {
		got = append(got, common.KvPair{
			Key: iter.Key(),
			Val: iter.Value(),
		})
	}
	require.NoError(t, iter.Error())
	require.Equal(t, sortedKVPairs, got)

	pickStartIdx := rand.Intn(len(sortedKVPairs))
	startKey := sortedKVPairs[pickStartIdx].Key
	iter, err = engine.createMergeIter(ctx, startKey)
	require.NoError(t, err)
	got = make([]common.KvPair, 0, totalKV)
	for iter.Next() {
		got = append(got, common.KvPair{
			Key: iter.Key(),
			Val: iter.Value(),
		})
	}
	require.NoError(t, iter.Error())
	// got keys must be ascending
	for i := 1; i < len(got); i++ {
		require.True(t, bytes.Compare(got[i-1].Key, got[i].Key) < 0)
	}
	// the first key must be less than or equal to startKey
	require.True(t, bytes.Compare(got[0].Key, startKey) <= 0)
}

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
	expectedKeys, expectedValues [][]byte,
	bufPool *membuf.Pool,
) {
	ctx := context.Background()
	iter := data.NewIter(ctx, lowerBound, upperBound, bufPool)
	var (
		keys, values [][]byte
	)
	for iter.First(); iter.Valid(); iter.Next() {
		require.NoError(t, iter.Error())
		keys = append(keys, iter.Key())
		values = append(values, iter.Value())
	}
	require.NoError(t, iter.Error())
	require.NoError(t, iter.Close())
	require.Equal(t, expectedKeys, keys)
	require.Equal(t, expectedValues, values)
}

func checkDupDB(t *testing.T, db *pebble.DB, expectedKeys, expectedValues [][]byte) {
	iter := db.NewIter(nil)
	var (
		gotKeys, gotValues [][]byte
	)
	for iter.First(); iter.Valid(); iter.Next() {
		key := make([]byte, len(iter.Key()))
		copy(key, iter.Key())
		gotKeys = append(gotKeys, key)
		value := make([]byte, len(iter.Value()))
		copy(value, iter.Value())
		gotValues = append(gotValues, value)
	}
	require.NoError(t, iter.Close())
	require.Equal(t, expectedKeys, gotKeys)
	require.Equal(t, expectedValues, gotValues)
	err := db.DeleteRange([]byte{0}, []byte{255}, nil)
	require.NoError(t, err)
}

func TestMemoryIngestData(t *testing.T) {
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("key4"),
		[]byte("key5"),
	}
	values := [][]byte{
		[]byte("value1"),
		[]byte("value2"),
		[]byte("value3"),
		[]byte("value4"),
		[]byte("value5"),
	}
	data := &MemoryIngestData{
		keyAdapter: common.NoopKeyAdapter{},
		keys:       keys,
		values:     values,
		ts:         123,
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
	testNewIter(t, data, nil, nil, keys, values, nil)
	testNewIter(t, data, []byte("key1"), []byte("key6"), keys, values, nil)
	testNewIter(t, data, []byte("key2"), []byte("key5"), keys[1:4], values[1:4], nil)
	testNewIter(t, data, []byte("key25"), []byte("key35"), keys[2:3], values[2:3], nil)
	testNewIter(t, data, []byte("key25"), []byte("key26"), nil, nil, nil)
	testNewIter(t, data, []byte("key0"), []byte("key1"), nil, nil, nil)
	testNewIter(t, data, []byte("key6"), []byte("key9"), nil, nil, nil)

	dir := t.TempDir()
	db, err := pebble.Open(path.Join(dir, "duplicate"), nil)
	require.NoError(t, err)
	keyAdapter := common.DupDetectKeyAdapter{}
	data = &MemoryIngestData{
		keyAdapter:         keyAdapter,
		duplicateDetection: true,
		duplicateDB:        db,
		ts:                 234,
	}
	encodedKeys := make([][]byte, 0, len(keys)*2)
	encodedValues := make([][]byte, 0, len(values)*2)
	encodedZero := codec.EncodeInt(nil, 0)
	encodedOne := codec.EncodeInt(nil, 1)
	duplicatedKeys := make([][]byte, 0, len(keys)*2)
	duplicatedValues := make([][]byte, 0, len(values)*2)

	for i := range keys {
		encodedKey := keyAdapter.Encode(nil, keys[i], encodedZero)
		encodedKeys = append(encodedKeys, encodedKey)
		encodedValues = append(encodedValues, values[i])
		if i%2 == 0 {
			continue
		}

		// duplicatedKeys will be like key2_0, key2_1, key4_0, key4_1
		duplicatedKeys = append(duplicatedKeys, encodedKey)
		duplicatedValues = append(duplicatedValues, values[i])

		encodedKey = keyAdapter.Encode(nil, keys[i], encodedOne)
		encodedKeys = append(encodedKeys, encodedKey)
		newValues := make([]byte, len(values[i])+1)
		copy(newValues, values[i])
		newValues[len(values[i])] = 1
		encodedValues = append(encodedValues, newValues)
		duplicatedKeys = append(duplicatedKeys, encodedKey)
		duplicatedValues = append(duplicatedValues, newValues)
	}
	data.keys = encodedKeys
	data.values = encodedValues

	require.EqualValues(t, 234, data.GetTS())
	testGetFirstAndLastKey(t, data, nil, nil, []byte("key1"), []byte("key5"))
	testGetFirstAndLastKey(t, data, []byte("key1"), []byte("key6"), []byte("key1"), []byte("key5"))
	testGetFirstAndLastKey(t, data, []byte("key2"), []byte("key5"), []byte("key2"), []byte("key4"))
	testGetFirstAndLastKey(t, data, []byte("key25"), []byte("key35"), []byte("key3"), []byte("key3"))
	testGetFirstAndLastKey(t, data, []byte("key25"), []byte("key26"), nil, nil)
	testGetFirstAndLastKey(t, data, []byte("key0"), []byte("key1"), nil, nil)
	testGetFirstAndLastKey(t, data, []byte("key6"), []byte("key9"), nil, nil)

	pool := membuf.NewPool()
	defer pool.Destroy()
	testNewIter(t, data, nil, nil, keys, values, pool)
	checkDupDB(t, db, duplicatedKeys, duplicatedValues)
	testNewIter(t, data, []byte("key1"), []byte("key6"), keys, values, pool)
	checkDupDB(t, db, duplicatedKeys, duplicatedValues)
	testNewIter(t, data, []byte("key1"), []byte("key3"), keys[:2], values[:2], pool)
	checkDupDB(t, db, duplicatedKeys[:2], duplicatedValues[:2])
	testNewIter(t, data, []byte("key2"), []byte("key5"), keys[1:4], values[1:4], pool)
	checkDupDB(t, db, duplicatedKeys, duplicatedValues)
	testNewIter(t, data, []byte("key25"), []byte("key35"), keys[2:3], values[2:3], pool)
	checkDupDB(t, db, nil, nil)
	testNewIter(t, data, []byte("key25"), []byte("key26"), nil, nil, pool)
	checkDupDB(t, db, nil, nil)
	testNewIter(t, data, []byte("key0"), []byte("key1"), nil, nil, pool)
	checkDupDB(t, db, nil, nil)
	testNewIter(t, data, []byte("key6"), []byte("key9"), nil, nil, pool)
	checkDupDB(t, db, nil, nil)
}

func TestSplit(t *testing.T) {
	cases := []struct {
		input    []int
		conc     int
		expected [][]int
	}{
		{
			input:    []int{1, 2, 3, 4, 5},
			conc:     1,
			expected: [][]int{{1, 2, 3, 4, 5}},
		},
		{
			input:    []int{1, 2, 3, 4, 5},
			conc:     2,
			expected: [][]int{{1, 2, 3}, {4, 5}},
		},
		{
			input:    []int{1, 2, 3, 4, 5},
			conc:     0,
			expected: [][]int{{1, 2, 3, 4, 5}},
		},
		{
			input:    []int{1, 2, 3, 4, 5},
			conc:     5,
			expected: [][]int{{1}, {2}, {3}, {4}, {5}},
		},
		{
			input:    []int{},
			conc:     5,
			expected: nil,
		},
		{
			input:    []int{1, 2, 3, 4, 5},
			conc:     100,
			expected: [][]int{{1}, {2}, {3}, {4}, {5}},
		},
	}

	for _, c := range cases {
		got := split(c.input, c.conc)
		require.Equal(t, c.expected, got)
	}
}

func TestGetAdjustedConcurrency(t *testing.T) {
	genFiles := func(n int) []string {
		files := make([]string, 0, n)
		for i := 0; i < n; i++ {
			files = append(files, fmt.Sprintf("file%d", i))
		}
		return files
	}
	e := &Engine{
		checkHotspot:      true,
		workerConcurrency: 32,
		dataFiles:         genFiles(100),
	}
	require.Equal(t, 8, e.getAdjustedConcurrency())
	e.dataFiles = genFiles(8000)
	require.Equal(t, 1, e.getAdjustedConcurrency())

	e.checkHotspot = false
	e.dataFiles = genFiles(10)
	require.Equal(t, 32, e.getAdjustedConcurrency())
	e.dataFiles = genFiles(100)
	require.Equal(t, 10, e.getAdjustedConcurrency())
	e.dataFiles = genFiles(10000)
	require.Equal(t, 1, e.getAdjustedConcurrency())
}
