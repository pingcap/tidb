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

package sortedmap_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/pingcap/tidb/util/sortedmap"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

type keyValue struct {
	key   []byte
	value []byte
}

func runCommonTest(t *testing.T, m sortedmap.SortedMap) {
	// Map is empty.
	iter := m.NewIterator()
	checkAll(t, m, nil)
	require.NoError(t, iter.Close())

	// Write arbitrary key-value pairs.
	var kvs []keyValue
	for i := 0; i < 100; i++ {
		kvs = append(kvs, keyValue{
			key:   []byte(fmt.Sprintf("key%04d", i)),
			value: []byte(fmt.Sprintf("value%04d", i)),
		})
	}
	rand.Shuffle(len(kvs), func(i, j int) {
		kvs[i], kvs[j] = kvs[j], kvs[i]
	})

	w := m.NewWriter()
	size := int64(0)
	for _, kv := range kvs {
		require.NoError(t, w.Put(kv.key, kv.value))
		size += int64(len(kv.key) + len(kv.value))
	}
	require.Equal(t, size, w.Size())
	require.NoError(t, w.Flush())
	require.Zero(t, w.Size())
	require.NoError(t, w.Close())

	// Check all key-value pairs.
	slices.SortFunc(kvs, func(a, b keyValue) bool {
		return bytes.Compare(a.key, b.key) < 0
	})
	checkAll(t, m, kvs)

	iter = m.NewIterator()
	// Seek to first key.
	require.True(t, iter.First())
	require.Equal(t, kvs[0].key, iter.UnsafeKey())
	// Seek to the last key.
	require.True(t, iter.Last())
	require.Equal(t, kvs[len(kvs)-1].key, iter.UnsafeKey())
	// Seek to middle key.
	require.True(t, iter.Seek(kvs[len(kvs)/2].key))
	require.Equal(t, kvs[len(kvs)/2].key, iter.UnsafeKey())
	// Close iterator.
	require.NoError(t, iter.Close())

	// Write more keys.
	w = m.NewWriter()
	for i := 100; i < 200; i++ {
		kvs = append(kvs, keyValue{
			key:   []byte(fmt.Sprintf("key%04d", i)),
			value: []byte(fmt.Sprintf("value%04d", i)),
		})
		require.NoError(t, w.Put(kvs[i].key, kvs[i].value))
	}
	require.NoError(t, w.Flush())
	require.NoError(t, w.Close())

	// Check all key-value pairs.
	checkAll(t, m, kvs)
}

func checkAll(t *testing.T, m sortedmap.SortedMap, kvs []keyValue) {
	iter := m.NewIterator()
	if len(kvs) == 0 {
		require.False(t, iter.First())
	} else {
		require.True(t, iter.First())
	}

	for i, kv := range kvs {
		require.True(t, iter.Valid())
		require.Equal(t, kv.key, iter.UnsafeKey())
		require.Equal(t, kv.value, iter.UnsafeValue())
		if i+1 == len(kvs) {
			require.False(t, iter.Next())
		} else {
			require.True(t, iter.Next())
		}
	}
	require.NoError(t, iter.Close())
}
