// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"math"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestGeneralProperties(t *testing.T) {
	seed := time.Now().Unix()
	rand.Seed(uint64(seed))
	t.Logf("seed: %d", seed)

	ctx := context.Background()
	memStore := storage.NewMemStorage()

	kvNum := rand.Intn(1000) + 100
	keys := make([][]byte, kvNum)
	values := make([][]byte, kvNum)
	for i := range keys {
		keyLen := rand.Intn(100) + 1
		valueLen := rand.Intn(100) + 1
		keys[i] = make([]byte, keyLen+2)
		values[i] = make([]byte, valueLen+2)
		rand.Read(keys[i][:keyLen])
		rand.Read(values[i][:valueLen])
		keys[i][keyLen] = byte(i / 255)
		keys[i][keyLen+1] = byte(i % 255)
		values[i][valueLen] = byte(i / 255)
		values[i][valueLen+1] = byte(i % 255)
	}

	dataFiles, statFiles, err := MockExternalEngine(memStore, keys, values)
	require.NoError(t, err)
	splitter, err := NewRangeSplitter(
		ctx, dataFiles, statFiles, memStore, 1000, 30, 1000, 1,
	)
	var lastEndKey []byte
notExhausted:
	endKey, dataFiles, statFiles, splitKeys, err := splitter.SplitOneRangesGroup()
	require.NoError(t, err)

	// endKey should be strictly greater than lastEndKey
	if lastEndKey != nil && endKey != nil {
		cmp := bytes.Compare(endKey, lastEndKey)
		require.Equal(t, 1, cmp, "endKey: %v, lastEndKey: %v", endKey, lastEndKey)
	}

	// check dataFiles and statFiles
	lenDataFiles := len(dataFiles)
	lenStatFiles := len(statFiles)
	require.Equal(t, lenDataFiles, lenStatFiles)
	require.Greater(t, lenDataFiles, 0)
	if len(splitKeys) > 0 {
		// splitKeys should be strictly increasing
		for i := 1; i < len(splitKeys); i++ {
			cmp := bytes.Compare(splitKeys[i], splitKeys[i-1])
			require.Equal(t, 1, cmp, "splitKeys: %v", splitKeys)
		}
		// first splitKeys should be strictly greater than lastEndKey
		cmp := bytes.Compare(splitKeys[0], lastEndKey)
		require.Equal(t, 1, cmp, "splitKeys: %v, lastEndKey: %v", splitKeys, lastEndKey)
		// last splitKeys should be strictly less than endKey
		if endKey != nil {
			cmp = bytes.Compare(splitKeys[len(splitKeys)-1], endKey)
			require.Equal(t, -1, cmp, "splitKeys: %v, endKey: %v", splitKeys, endKey)
		}
	}

	lastEndKey = endKey
	if endKey != nil {
		goto notExhausted
	}
	require.NoError(t, splitter.Close())
}

func TestOnlyOneGroup(t *testing.T) {
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	subDir := "/mock-test"

	writer := NewWriterBuilder().
		SetMemorySizeLimit(15).
		SetPropSizeDistance(1).
		SetPropKeysDistance(1).
		Build(memStore, subDir, "5")

	dataFiles, statFiles, err := MockExternalEngineWithWriter(memStore, writer, subDir, [][]byte{{1}, {2}}, [][]byte{{1}, {2}})
	require.NoError(t, err)

	splitter, err := NewRangeSplitter(
		ctx, dataFiles, statFiles, memStore, 1000, 30, 1000, 10,
	)
	require.NoError(t, err)
	endKey, dataFiles, statFiles, splitKeys, err := splitter.SplitOneRangesGroup()
	require.NoError(t, err)
	require.Nil(t, endKey)
	require.Len(t, dataFiles, 1)
	require.Len(t, statFiles, 1)
	require.Len(t, splitKeys, 0)
	require.NoError(t, splitter.Close())

	splitter, err = NewRangeSplitter(
		ctx, dataFiles, statFiles, memStore, 1000, 30, 1000, 1,
	)
	require.NoError(t, err)
	endKey, dataFiles, statFiles, splitKeys, err = splitter.SplitOneRangesGroup()
	require.NoError(t, err)
	require.Nil(t, endKey)
	require.Len(t, dataFiles, 1)
	require.Len(t, statFiles, 1)
	require.Equal(t, [][]byte{{2}}, splitKeys)
	require.NoError(t, splitter.Close())
}

func TestSortedData(t *testing.T) {
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	kvNum := 100

	keys := make([][]byte, kvNum)
	values := make([][]byte, kvNum)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key%03d", i))
		values[i] = []byte(fmt.Sprintf("value%03d", i))
	}

	dataFiles, statFiles, err := MockExternalEngine(memStore, keys, values)
	require.NoError(t, err)
	// we just need to make sure there are multiple files.
	require.Greater(t, len(dataFiles), 1)
	avgKVPerFile := math.Ceil(float64(kvNum) / float64(len(dataFiles)))
	rangesGroupKV := 30
	groupFileNumUpperBound := int(math.Ceil(float64(rangesGroupKV-1)/avgKVPerFile)) + 1

	splitter, err := NewRangeSplitter(
		ctx, dataFiles, statFiles, memStore, 1000, int64(rangesGroupKV), 1000, 10,
	)
	require.NoError(t, err)

notExhausted:
	endKey, dataFiles, statFiles, _, err := splitter.SplitOneRangesGroup()
	require.NoError(t, err)
	require.LessOrEqual(t, len(dataFiles), groupFileNumUpperBound)
	require.LessOrEqual(t, len(statFiles), groupFileNumUpperBound)
	if endKey != nil {
		goto notExhausted
	}
	require.NoError(t, splitter.Close())
}

func TestRangeSplitterStrictCase(t *testing.T) {
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	subDir := "/mock-test"

	writer1 := NewWriterBuilder().
		SetMemorySizeLimit(15). // slightly larger than len("key01") + len("value01")
		SetPropSizeDistance(1).
		SetPropKeysDistance(1).
		Build(memStore, subDir, "1")
	keys1 := [][]byte{
		[]byte("key01"), []byte("key11"), []byte("key21"),
	}
	values1 := [][]byte{
		[]byte("value01"), []byte("value11"), []byte("value21"),
	}
	dataFiles1, statFiles1, err := MockExternalEngineWithWriter(memStore, writer1, subDir, keys1, values1)
	require.NoError(t, err)
	require.Len(t, dataFiles1, 2)
	require.Len(t, statFiles1, 2)

	writer2 := NewWriterBuilder().
		SetMemorySizeLimit(15).
		SetPropSizeDistance(1).
		SetPropKeysDistance(1).
		Build(memStore, subDir, "2")
	keys2 := [][]byte{
		[]byte("key02"), []byte("key12"), []byte("key22"),
	}
	values2 := [][]byte{
		[]byte("value02"), []byte("value12"), []byte("value22"),
	}
	dataFiles12, statFiles12, err := MockExternalEngineWithWriter(memStore, writer2, subDir, keys2, values2)
	require.NoError(t, err)
	require.Len(t, dataFiles12, 4)
	require.Len(t, statFiles12, 4)

	writer3 := NewWriterBuilder().
		SetMemorySizeLimit(15).
		SetPropSizeDistance(1).
		SetPropKeysDistance(1).
		Build(memStore, subDir, "3")
	keys3 := [][]byte{
		[]byte("key03"), []byte("key13"), []byte("key23"),
	}
	values3 := [][]byte{
		[]byte("value03"), []byte("value13"), []byte("value23"),
	}
	dataFiles123, statFiles123, err := MockExternalEngineWithWriter(memStore, writer3, subDir, keys3, values3)
	require.NoError(t, err)
	require.Len(t, dataFiles123, 6)
	require.Len(t, statFiles123, 6)

	// "/mock-test/X/0" contains "key0X" and "key1X"
	// "/mock-test/X/1" contains "key2X"
	require.Equal(t, []string{
		"/mock-test/1/0", "/mock-test/1/1",
		"/mock-test/2/0", "/mock-test/2/1",
		"/mock-test/3/0", "/mock-test/3/1",
	}, dataFiles123)

	// group keys = 2, region keys = 1
	splitter, err := NewRangeSplitter(
		ctx, dataFiles123, statFiles123, memStore, 1000, 2, 1000, 1,
	)
	require.NoError(t, err)

	// [key01, key03), split at key02
	endKey, dataFiles, statFiles, splitKeys, err := splitter.SplitOneRangesGroup()
	require.NoError(t, err)
	require.EqualValues(t, kv.Key("key03"), endKey)
	require.Equal(t, []string{"/mock-test/1/0", "/mock-test/2/0"}, dataFiles)
	require.Equal(t, []string{"/mock-test/1_stat/0", "/mock-test/2_stat/0"}, statFiles)
	require.Equal(t, [][]byte{[]byte("key02")}, splitKeys)

	// [key03, key12), split at key11
	endKey, dataFiles, statFiles, splitKeys, err = splitter.SplitOneRangesGroup()
	require.NoError(t, err)
	require.EqualValues(t, kv.Key("key12"), endKey)
	require.Equal(t, []string{"/mock-test/1/0", "/mock-test/2/0", "/mock-test/3/0"}, dataFiles)
	require.Equal(t, []string{"/mock-test/1_stat/0", "/mock-test/2_stat/0", "/mock-test/3_stat/0"}, statFiles)
	require.Equal(t, [][]byte{[]byte("key11")}, splitKeys)

	// [key12, key21), split at key13. the last key of "/mock-test/1/0" is "key11",
	// so it's not used
	endKey, dataFiles, statFiles, splitKeys, err = splitter.SplitOneRangesGroup()
	require.NoError(t, err)
	require.EqualValues(t, kv.Key("key21"), endKey)
	require.Equal(t, []string{"/mock-test/2/0", "/mock-test/3/0"}, dataFiles)
	require.Equal(t, []string{"/mock-test/2_stat/0", "/mock-test/3_stat/0"}, statFiles)
	require.Equal(t, [][]byte{[]byte("key13")}, splitKeys)

	// [key21, key23), split at key22.
	// the last key of "/mock-test/2/0" is "key12", and the last key of "/mock-test/3/0" is "key13",
	// so they are not used
	endKey, dataFiles, statFiles, splitKeys, err = splitter.SplitOneRangesGroup()
	require.NoError(t, err)
	require.EqualValues(t, kv.Key("key23"), endKey)
	require.Equal(t, []string{"/mock-test/1/1", "/mock-test/2/1"}, dataFiles)
	require.Equal(t, []string{"/mock-test/1_stat/1", "/mock-test/2_stat/1"}, statFiles)
	require.Equal(t, [][]byte{[]byte("key22")}, splitKeys)

	// [key23, nil), no split key
	endKey, dataFiles, statFiles, splitKeys, err = splitter.SplitOneRangesGroup()
	require.NoError(t, err)
	require.Nil(t, endKey)
	require.Equal(t, []string{"/mock-test/3/1"}, dataFiles)
	require.Equal(t, []string{"/mock-test/3_stat/1"}, statFiles)
	require.Len(t, splitKeys, 0)

	// read after drain all data
	endKey, dataFiles, statFiles, splitKeys, err = splitter.SplitOneRangesGroup()
	require.NoError(t, err)
	require.Nil(t, endKey)
	require.Len(t, dataFiles, 0)
	require.Len(t, statFiles, 0)
	require.Len(t, splitKeys, 0)
	require.NoError(t, splitter.Close())
}

func TestExactlyKeyNum(t *testing.T) {
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	kvNum := 3

	keys := make([][]byte, kvNum)
	values := make([][]byte, kvNum)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key%03d", i))
		values[i] = []byte(fmt.Sprintf("value%03d", i))
	}

	subDir := "/mock-test"

	writer := NewWriterBuilder().
		SetMemorySizeLimit(15).
		SetPropSizeDistance(1).
		SetPropKeysDistance(1).
		Build(memStore, subDir, "5")

	dataFiles, statFiles, err := MockExternalEngineWithWriter(memStore, writer, subDir, keys, values)
	require.NoError(t, err)

	// maxRangeKeys = 3
	splitter, err := NewRangeSplitter(
		ctx, dataFiles, statFiles, memStore, 1000, 100, 1000, 3,
	)
	require.NoError(t, err)
	endKey, splitDataFiles, splitStatFiles, splitKeys, err := splitter.SplitOneRangesGroup()
	require.NoError(t, err)
	require.Nil(t, endKey)
	require.Equal(t, dataFiles, splitDataFiles)
	require.Equal(t, statFiles, splitStatFiles)
	require.Len(t, splitKeys, 0)

	// rangesGroupKeys = 3
	splitter, err = NewRangeSplitter(
		ctx, dataFiles, statFiles, memStore, 1000, 3, 1000, 1,
	)
	require.NoError(t, err)
	endKey, splitDataFiles, splitStatFiles, splitKeys, err = splitter.SplitOneRangesGroup()
	require.NoError(t, err)
	require.Nil(t, endKey)
	require.Equal(t, dataFiles, splitDataFiles)
	require.Equal(t, statFiles, splitStatFiles)
	require.Equal(t, [][]byte{[]byte("key001"), []byte("key002")}, splitKeys)
}
