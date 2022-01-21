// Copyright 2021 PingCAP, Inc.
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

package local

import (
	"bytes"
	"context"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/tidb/br/pkg/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/stretchr/testify/require"
)

func TestDupDetectIterator(t *testing.T) {
	var pairs []common.KvPair
	prevRowMax := int64(0)
	// Unique pairs.
	for i := 0; i < 20; i++ {
		pairs = append(pairs, common.KvPair{
			Key:   randBytes(32),
			Val:   randBytes(128),
			RowID: prevRowMax,
		})
		prevRowMax++
	}
	// Duplicate pairs which repeat the same key twice.
	for i := 20; i < 40; i++ {
		key := randBytes(32)
		pairs = append(pairs, common.KvPair{
			Key:   key,
			Val:   randBytes(128),
			RowID: prevRowMax,
		})
		prevRowMax++
		pairs = append(pairs, common.KvPair{
			Key:   key,
			Val:   randBytes(128),
			RowID: prevRowMax,
		})
		prevRowMax++
	}
	// Duplicate pairs which repeat the same key three times.
	for i := 40; i < 50; i++ {
		key := randBytes(32)
		pairs = append(pairs, common.KvPair{
			Key:   key,
			Val:   randBytes(128),
			RowID: prevRowMax,
		})
		prevRowMax++
		pairs = append(pairs, common.KvPair{
			Key:   key,
			Val:   randBytes(128),
			RowID: prevRowMax,
		})
		prevRowMax++
		pairs = append(pairs, common.KvPair{
			Key:   key,
			Val:   randBytes(128),
			RowID: prevRowMax,
		})
		prevRowMax++
	}

	// Find duplicates from the generated pairs.
	var dupPairs []common.KvPair
	sort.Slice(pairs, func(i, j int) bool {
		return bytes.Compare(pairs[i].Key, pairs[j].Key) < 0
	})
	uniqueKeys := make([][]byte, 0)
	for i := 0; i < len(pairs); {
		j := i + 1
		for j < len(pairs) && bytes.Equal(pairs[j-1].Key, pairs[j].Key) {
			j++
		}
		uniqueKeys = append(uniqueKeys, pairs[i].Key)
		if i+1 == j {
			i++
			continue
		}
		for k := i; k < j; k++ {
			dupPairs = append(dupPairs, pairs[k])
		}
		i = j
	}

	keyAdapter := dupDetectKeyAdapter{}

	// Write pairs to db after shuffling the pairs.
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	rnd.Shuffle(len(pairs), func(i, j int) {
		pairs[i], pairs[j] = pairs[j], pairs[i]
	})
	storeDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(storeDir, "kv"), &pebble.Options{})
	require.NoError(t, err)
	wb := db.NewBatch()
	for _, p := range pairs {
		key := keyAdapter.Encode(nil, p.Key, p.RowID)
		require.NoError(t, wb.Set(key, p.Val, nil))
	}
	require.NoError(t, wb.Commit(pebble.Sync))

	dupDB, err := pebble.Open(filepath.Join(storeDir, "duplicates"), &pebble.Options{})
	require.NoError(t, err)
	var iter kv.Iter
	iter = newDupDetectIter(context.Background(), db, keyAdapter, &pebble.IterOptions{}, dupDB, log.L())
	sort.Slice(pairs, func(i, j int) bool {
		key1 := keyAdapter.Encode(nil, pairs[i].Key, pairs[i].RowID)
		key2 := keyAdapter.Encode(nil, pairs[j].Key, pairs[j].RowID)
		return bytes.Compare(key1, key2) < 0
	})

	// Verify first pair.
	require.True(t, iter.First())
	require.True(t, iter.Valid())
	require.Equal(t, pairs[0].Key, iter.Key())
	require.Equal(t, pairs[0].Val, iter.Value())

	// Verify last pair.
	require.True(t, iter.Last())
	require.True(t, iter.Valid())
	require.Equal(t, pairs[len(pairs)-1].Key, iter.Key())
	require.Equal(t, pairs[len(pairs)-1].Val, iter.Value())

	// Iterate all keys and check the count of unique keys.
	for iter.First(); iter.Valid(); iter.Next() {
		require.Equal(t, uniqueKeys[0], iter.Key())
		uniqueKeys = uniqueKeys[1:]
	}
	require.NoError(t, iter.Error())
	require.Equal(t, 0, len(uniqueKeys))
	require.NoError(t, iter.Close())
	require.NoError(t, db.Close())

	// Check duplicates detected by dupDetectIter.
	iter = newDupDBIter(dupDB, keyAdapter, &pebble.IterOptions{})
	var detectedPairs []common.KvPair
	for iter.First(); iter.Valid(); iter.Next() {
		detectedPairs = append(detectedPairs, common.KvPair{
			Key: append([]byte{}, iter.Key()...),
			Val: append([]byte{}, iter.Value()...),
		})
	}
	require.NoError(t, iter.Error())
	require.NoError(t, iter.Close())
	require.NoError(t, dupDB.Close())
	require.Equal(t, len(dupPairs), len(detectedPairs))

	sort.Slice(dupPairs, func(i, j int) bool {
		keyCmp := bytes.Compare(dupPairs[i].Key, dupPairs[j].Key)
		return keyCmp < 0 || keyCmp == 0 && bytes.Compare(dupPairs[i].Val, dupPairs[j].Val) < 0
	})
	sort.Slice(detectedPairs, func(i, j int) bool {
		keyCmp := bytes.Compare(detectedPairs[i].Key, detectedPairs[j].Key)
		return keyCmp < 0 || keyCmp == 0 && bytes.Compare(detectedPairs[i].Val, detectedPairs[j].Val) < 0
	})
	for i := 0; i < len(detectedPairs); i++ {
		require.Equal(t, dupPairs[i].Key, detectedPairs[i].Key)
		require.Equal(t, dupPairs[i].Val, detectedPairs[i].Val)
	}
}

func TestDupDetectIterSeek(t *testing.T) {
	pairs := []common.KvPair{
		{
			Key:   []byte{1, 2, 3, 0},
			Val:   randBytes(128),
			RowID: 1,
		},
		{
			Key:   []byte{1, 2, 3, 1},
			Val:   randBytes(128),
			RowID: 2,
		},
		{
			Key:   []byte{1, 2, 3, 1},
			Val:   randBytes(128),
			RowID: 3,
		},
		{
			Key:   []byte{1, 2, 3, 2},
			Val:   randBytes(128),
			RowID: 4,
		},
	}

	storeDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(storeDir, "kv"), &pebble.Options{})
	require.NoError(t, err)

	keyAdapter := dupDetectKeyAdapter{}
	wb := db.NewBatch()
	for _, p := range pairs {
		key := keyAdapter.Encode(nil, p.Key, p.RowID)
		require.NoError(t, wb.Set(key, p.Val, nil))
	}
	require.NoError(t, wb.Commit(pebble.Sync))

	dupDB, err := pebble.Open(filepath.Join(storeDir, "duplicates"), &pebble.Options{})
	require.NoError(t, err)
	iter := newDupDetectIter(context.Background(), db, keyAdapter, &pebble.IterOptions{}, dupDB, log.L())

	require.True(t, iter.Seek([]byte{1, 2, 3, 1}))
	require.Equal(t, pairs[1].Val, iter.Value())
	require.True(t, iter.Next())
	require.Equal(t, pairs[3].Val, iter.Value())
	require.NoError(t, iter.Close())
	require.NoError(t, db.Close())
	require.NoError(t, dupDB.Close())
}
