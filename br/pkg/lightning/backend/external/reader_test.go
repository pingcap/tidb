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
	"math"
	"slices"
	"testing"
	"time"

	"github.com/jfcg/sorty/v2"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	dbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func testReadAndCompare(
	t *testing.T,
	ctx context.Context,
	kvs []common.KvPair,
	store storage.ExternalStorage,
	memSizeLimit int) {
	datas, stats, err := GetAllFileNames(ctx, store, "")
	require.NoError(t, err)

	splitter, err := NewRangeSplitter(
		ctx,
		datas,
		stats,
		store,
		int64(memSizeLimit), // make the group small for testing
		math.MaxInt64,
		4*1024*1024*1024,
		math.MaxInt64,
		true,
	)
	require.NoError(t, err)

	bufPool := membuf.NewPool()
	loaded := &memKVsAndBuffers{}
	curStart := kvs[0].Key
	kvIdx := 0

	for {
		endKeyOfGroup, dataFilesOfGroup, statFilesOfGroup, _, err := splitter.SplitOneRangesGroup()
		require.NoError(t, err)
		curEnd := endKeyOfGroup
		if len(endKeyOfGroup) == 0 {
			curEnd = dbkv.Key(kvs[len(kvs)-1].Key).Next()
		}

		err = readAllData(
			ctx,
			store,
			dataFilesOfGroup,
			statFilesOfGroup,
			curStart,
			curEnd,
			bufPool,
			loaded,
		)

		require.NoError(t, err)
		// check kvs sorted
		sorty.MaxGor = uint64(8)
		sorty.Sort(len(loaded.keys), func(i, k, r, s int) bool {
			if bytes.Compare(loaded.keys[i], loaded.keys[k]) < 0 { // strict comparator like < or >
				if r != s {
					loaded.keys[r], loaded.keys[s] = loaded.keys[s], loaded.keys[r]
					loaded.values[r], loaded.values[s] = loaded.values[s], loaded.values[r]
				}
				return true
			}
			return false
		})
		for i, key := range loaded.keys {
			require.EqualValues(t, kvs[kvIdx].Key, key)
			require.EqualValues(t, kvs[kvIdx].Val, loaded.values[i])
			kvIdx++
		}

		// release
		loaded.keys = nil
		loaded.values = nil
		loaded.memKVBuffers = nil
		copy(curStart, curEnd)

		if len(endKeyOfGroup) == 0 {
			break
		}
	}
}

func TestReadAllDataBasic(t *testing.T) {
	seed := time.Now().Unix()
	rand.Seed(uint64(seed))
	t.Logf("seed: %d", seed)
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	memSizeLimit := (rand.Intn(10) + 1) * 400

	w := NewWriterBuilder().
		SetPropSizeDistance(100).
		SetPropKeysDistance(2).
		SetMemorySizeLimit(uint64(memSizeLimit)).
		SetBlockSize(memSizeLimit).
		Build(memStore, "/test", "0")

	writer := NewEngineWriter(w)
	kvCnt := rand.Intn(10) + 10000
	kvs := make([]common.KvPair, kvCnt)
	for i := 0; i < kvCnt; i++ {
		kvs[i] = common.KvPair{
			Key: []byte(fmt.Sprintf("key%05d", i)),
			Val: []byte("56789"),
		}
	}

	require.NoError(t, writer.AppendRows(ctx, nil, kv.MakeRowsFromKvPairs(kvs)))
	_, err := writer.Close(ctx)
	require.NoError(t, err)

	slices.SortFunc(kvs, func(i, j common.KvPair) int {
		return bytes.Compare(i.Key, j.Key)
	})

	testReadAndCompare(t, ctx, kvs, memStore, memSizeLimit)
}

func TestReadAllOneFile(t *testing.T) {
	seed := time.Now().Unix()
	rand.Seed(uint64(seed))
	t.Logf("seed: %d", seed)
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	memSizeLimit := (rand.Intn(10) + 1) * 400

	w := NewWriterBuilder().
		SetPropSizeDistance(100).
		SetPropKeysDistance(2).
		SetMemorySizeLimit(uint64(memSizeLimit)).
		BuildOneFile(memStore, "/test", "0")

	require.NoError(t, w.Init(ctx, int64(5*size.MB)))

	kvCnt := rand.Intn(10) + 10000
	kvs := make([]common.KvPair, kvCnt)
	for i := 0; i < kvCnt; i++ {
		kvs[i] = common.KvPair{
			Key: []byte(fmt.Sprintf("key%05d", i)),
			Val: []byte("56789"),
		}
		require.NoError(t, w.WriteRow(ctx, kvs[i].Key, kvs[i].Val))
	}

	err := w.Close(ctx)
	require.NoError(t, err)

	slices.SortFunc(kvs, func(i, j common.KvPair) int {
		return bytes.Compare(i.Key, j.Key)
	})
	// failed, need to figure out why.
	testReadAndCompare(t, ctx, kvs, memStore, memSizeLimit)
}
