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
	"slices"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/storage"
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
			Build(store, i, "/subtask")
		kvStart := i * 100
		kvEnd := (i + 1) * 100
		err := w.AppendRows(ctx, nil, kv.MakeRowsFromKvPairs(kvPairs[kvStart:kvEnd]))
		require.NoError(t, err)
		_, err = w.Close(ctx)
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
