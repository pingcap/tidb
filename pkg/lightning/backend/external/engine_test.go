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

	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/stretchr/testify/require"
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
	expectedKeys, expectedValues [][]byte,
) {
	ctx := context.Background()
	iter := data.NewIter(ctx, lowerBound, upperBound, nil)
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
		keys:   keys,
		values: values,
		ts:     123,
	}

	require.EqualValues(t, 123, data.GetTS())
	testGetFirstAndLastKey(t, data, nil, nil, []byte("key1"), []byte("key5"))
	testGetFirstAndLastKey(t, data, []byte("key1"), []byte("key6"), []byte("key1"), []byte("key5"))
	testGetFirstAndLastKey(t, data, []byte("key2"), []byte("key5"), []byte("key2"), []byte("key4"))
	testGetFirstAndLastKey(t, data, []byte("key25"), []byte("key35"), []byte("key3"), []byte("key3"))
	testGetFirstAndLastKey(t, data, []byte("key25"), []byte("key26"), nil, nil)
	testGetFirstAndLastKey(t, data, []byte("key0"), []byte("key1"), nil, nil)
	testGetFirstAndLastKey(t, data, []byte("key6"), []byte("key9"), nil, nil)

	testNewIter(t, data, nil, nil, keys, values)
	testNewIter(t, data, []byte("key1"), []byte("key6"), keys, values)
	testNewIter(t, data, []byte("key2"), []byte("key5"), keys[1:4], values[1:4])
	testNewIter(t, data, []byte("key25"), []byte("key35"), keys[2:3], values[2:3])
	testNewIter(t, data, []byte("key25"), []byte("key26"), nil, nil)
	testNewIter(t, data, []byte("key0"), []byte("key1"), nil, nil)
	testNewIter(t, data, []byte("key6"), []byte("key9"), nil, nil)

	data = &MemoryIngestData{
		ts: 234,
	}
	encodedKeys := make([][]byte, 0, len(keys)*2)
	encodedValues := make([][]byte, 0, len(values)*2)
	duplicatedKeys := make([][]byte, 0, len(keys)*2)
	duplicatedValues := make([][]byte, 0, len(values)*2)

	for i := range keys {
		encodedKey := slices.Clone(keys[i])
		encodedKeys = append(encodedKeys, encodedKey)
		encodedValues = append(encodedValues, values[i])
		if i%2 == 0 {
			continue
		}

		// duplicatedKeys will be like key2_0, key2_1, key4_0, key4_1
		duplicatedKeys = append(duplicatedKeys, encodedKey)
		duplicatedValues = append(duplicatedValues, values[i])

		encodedKey = slices.Clone(keys[i])
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
