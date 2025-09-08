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

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
	"github.com/stretchr/testify/require"
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
	expectedKVs []kvPair,
) {
	ctx := context.Background()
	iter := data.NewIter(ctx, lowerBound, upperBound, nil)
	var kvs []kvPair
	for iter.First(); iter.Valid(); iter.Next() {
		require.NoError(t, iter.Error())
		kvs = append(kvs, kvPair{key: iter.Key(), value: iter.Value()})
	}
	require.NoError(t, iter.Error())
	require.NoError(t, iter.Close())
	require.Equal(t, expectedKVs, kvs)
}

func TestMemoryIngestData(t *testing.T) {
	kvs := []kvPair{
		{key: []byte("key1"), value: []byte("value1")},
		{key: []byte("key2"), value: []byte("value2")},
		{key: []byte("key3"), value: []byte("value3")},
		{key: []byte("key4"), value: []byte("value4")},
		{key: []byte("key5"), value: []byte("value5")},
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
	encodedKVs := make([]kvPair, 0, len(kvs)*2)
	duplicatedKVs := make([]kvPair, 0, len(kvs)*2)

	for i := range kvs {
		encodedKey := slices.Clone(kvs[i].key)
		encodedKVs = append(encodedKVs, kvPair{key: encodedKey, value: kvs[i].value})
		if i%2 == 0 {
			continue
		}

		// duplicatedKeys will be like key2_0, key2_1, key4_0, key4_1
		duplicatedKVs = append(duplicatedKVs, kvPair{key: encodedKey, value: kvs[i].value})

		encodedKey = slices.Clone(kvs[i].key)
		newValues := make([]byte, len(kvs[i].value)+1)
		copy(newValues, kvs[i].value)
		newValues[len(kvs[i].value)] = 1
		encodedKVs = append(encodedKVs, kvPair{key: encodedKey, value: newValues})
		duplicatedKVs = append(duplicatedKVs, kvPair{key: encodedKey, value: newValues})
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

func prepareKVFiles(t *testing.T, store storage.ExternalStorage, contents [][]kvPair) (dataFiles, statFiles []string) {
	ctx := context.Background()
	for i, c := range contents {
		var summary *WriterSummary
		// we want to create a file for each content, so make the below size larger.
		writer := NewWriterBuilder().SetPropKeysDistance(4).
			SetMemorySizeLimit(8*units.MiB).SetBlockSize(8*units.MiB).
			SetOnCloseFunc(func(s *WriterSummary) { summary = s }).
			Build(store, "/test", fmt.Sprintf("%d", i))
		for _, p := range c {
			require.NoError(t, writer.WriteRow(ctx, p.key, p.value, nil))
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

func getAllDataFromDataAndRanges(t *testing.T, dataAndRanges *engineapi.DataAndRanges) []kvPair {
	ctx := context.Background()
	iter := dataAndRanges.Data.NewIter(ctx, nil, nil, membuf.NewPool())
	var allKVs []kvPair
	for iter.First(); iter.Valid(); iter.Next() {
		allKVs = append(allKVs, kvPair{key: iter.Key(), value: iter.Value()})
	}
	require.NoError(t, iter.Close())
	return allKVs
}

func TestEngineOnDup(t *testing.T) {
	ctx := context.Background()
	contents := [][]kvPair{{
		{key: []byte{4}, value: []byte("bbb")},
		{key: []byte{4}, value: []byte("bbb")},
		{key: []byte{1}, value: []byte("aa")},
		{key: []byte{1}, value: []byte("aa")},
		{key: []byte{1}, value: []byte("aa")},
		{key: []byte{2}, value: []byte("vv")},
		{key: []byte{3}, value: []byte("sds")},
	}}

	getEngineFn := func(store storage.ExternalStorage, onDup engineapi.OnDuplicateKey, inDataFiles, inStatFiles []string) *Engine {
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
			dummyReaderOnCloseFunc,
		)
	}

	t.Run("on duplicate ignore", func(t *testing.T) {
		onDup := engineapi.OnDuplicateKeyIgnore
		store := storage.NewMemStorage()
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
		store := storage.NewMemStorage()
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
			store := storage.NewMemStorage()
			dfiles, sfiles := prepareKVFiles(t, store, [][]kvPair{{
				{key: []byte{4}, value: []byte("bbb")},
				{key: []byte{1}, value: []byte("aa")},
				{key: []byte{2}, value: []byte("vv")},
				{key: []byte{3}, value: []byte("sds")},
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
			require.EqualValues(t, []kvPair{
				{key: []byte{1}, value: []byte("aa")},
				{key: []byte{2}, value: []byte("vv")},
				{key: []byte{3}, value: []byte("sds")},
				{key: []byte{4}, value: []byte("bbb")},
			}, allKVs)
			info := extEngine.ConflictInfo()
			require.Zero(t, info.Count)
			require.Empty(t, info.Files)
		}
	})

	t.Run("on duplicate record or remove, partial duplicated", func(t *testing.T) {
		contents2 := [][]kvPair{
			{{key: []byte{1}, value: []byte("aa")}, {key: []byte{1}, value: []byte("aa")}},
			{{key: []byte{1}, value: []byte("aa")}, {key: []byte{2}, value: []byte("vv")}, {key: []byte{3}, value: []byte("sds")}},
			{{key: []byte{4}, value: []byte("bbb")}, {key: []byte{4}, value: []byte("bbb")}},
		}
		for _, cont := range [][][]kvPair{contents, contents2} {
			for _, od := range []engineapi.OnDuplicateKey{engineapi.OnDuplicateKeyRecord, engineapi.OnDuplicateKeyRemove} {
				store := storage.NewMemStorage()
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
				require.EqualValues(t, []kvPair{
					{key: []byte{2}, value: []byte("vv")},
					{key: []byte{3}, value: []byte("sds")},
				}, allKVs)
				info := extEngine.ConflictInfo()
				if od == engineapi.OnDuplicateKeyRemove {
					require.Zero(t, info.Count)
					require.Empty(t, info.Files)
				} else {
					require.EqualValues(t, 5, info.Count)
					require.Len(t, info.Files, 1)
					dupPairs := readKVFile(t, store, info.Files[0])
					require.EqualValues(t, []kvPair{
						{key: []byte{1}, value: []byte("aa")},
						{key: []byte{1}, value: []byte("aa")},
						{key: []byte{1}, value: []byte("aa")},
						{key: []byte{4}, value: []byte("bbb")},
						{key: []byte{4}, value: []byte("bbb")},
					}, dupPairs)
				}
			}
		}
	})

	t.Run("on duplicate record or remove, all duplicated", func(t *testing.T) {
		for _, od := range []engineapi.OnDuplicateKey{engineapi.OnDuplicateKeyRecord, engineapi.OnDuplicateKeyRemove} {
			store := storage.NewMemStorage()
			dfiles, sfiles := prepareKVFiles(t, store, [][]kvPair{{
				{key: []byte{1}, value: []byte("aaa")},
				{key: []byte{1}, value: []byte("aaa")},
				{key: []byte{1}, value: []byte("aaa")},
				{key: []byte{1}, value: []byte("aaa")},
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
				require.EqualValues(t, []kvPair{
					{key: []byte{1}, value: []byte("aaa")},
					{key: []byte{1}, value: []byte("aaa")},
					{key: []byte{1}, value: []byte("aaa")},
					{key: []byte{1}, value: []byte("aaa")},
				}, dupPairs)
			}
		}
	})
}
