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
	"slices"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

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

	datas, stats, err := GetAllFileNames(ctx, memStore, "")
	require.NoError(t, err)

	testReadAndCompare(ctx, t, kvs, memStore, datas, stats, kvs[0].Key, memSizeLimit)
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

	require.NoError(t, w.Close(ctx))

	slices.SortFunc(kvs, func(i, j common.KvPair) int {
		return bytes.Compare(i.Key, j.Key)
	})

	datas, stats, err := GetAllFileNames(ctx, memStore, "")
	require.NoError(t, err)

	testReadAndCompare(ctx, t, kvs, memStore, datas, stats, kvs[0].Key, memSizeLimit)
}

func TestReadLargeFile(t *testing.T) {
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	backup := ConcurrentReaderBufferSizePerConc
	t.Cleanup(func() {
		ConcurrentReaderBufferSizePerConc = backup
	})
	ConcurrentReaderBufferSizePerConc = 512 * 1024

	w := NewWriterBuilder().
		SetPropSizeDistance(128*1024).
		SetPropKeysDistance(1000).
		BuildOneFile(memStore, "/test", "0")

	require.NoError(t, w.Init(ctx, int64(5*size.MB)))

	val := make([]byte, 10000)
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		require.NoError(t, w.WriteRow(ctx, key, val))
	}
	require.NoError(t, w.Close(ctx))

	datas, stats, err := GetAllFileNames(ctx, memStore, "")
	require.NoError(t, err)
	require.Len(t, datas, 1)

	failpoint.Enable("github.com/pingcap/tidb/pkg/lightning/backend/external/assertReloadAtMostOnce", "return()")
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/lightning/backend/external/assertReloadAtMostOnce")

	smallBlockBufPool := membuf.NewPool(
		membuf.WithBlockNum(0),
		membuf.WithBlockSize(smallBlockSize),
	)
	largeBlockBufPool := membuf.NewPool(
		membuf.WithBlockNum(0),
		membuf.WithBlockSize(ConcurrentReaderBufferSizePerConc),
	)
	output := &memKVsAndBuffers{}
	startKey := []byte("key000000")
	maxKey := []byte("key004998")
	endKey := []byte("key004999")
	err = readAllData(ctx, memStore, datas, stats, startKey, endKey, smallBlockBufPool, largeBlockBufPool, output)
	require.NoError(t, err)
	output.build(ctx)
	require.Equal(t, startKey, output.keys[0])
	require.Equal(t, maxKey, output.keys[len(output.keys)-1])
}
