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
	"io"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
	"golang.org/x/exp/slices"
)

func TestWriter(t *testing.T) {
	seed := time.Now().Unix()
	rand.Seed(uint64(seed))
	t.Logf("seed: %d", seed)
	ctx := context.Background()
	memStore := storage.NewMemStorage()

	writer := NewWriterBuilder().
		SetPropSizeDistance(100).
		SetPropKeysDistance(2).
		Build(memStore, 0, "/test")

	kvCnt := rand.Intn(10) + 10
	kvs := make([]common.KvPair, kvCnt)
	for i := 0; i < kvCnt; i++ {
		randLen := rand.Intn(10) + 1
		kvs[i].Key = make([]byte, randLen)
		_, err := rand.Read(kvs[i].Key)
		require.NoError(t, err)
		randLen = rand.Intn(10) + 1
		kvs[i].Val = make([]byte, randLen)
		_, err = rand.Read(kvs[i].Val)
		require.NoError(t, err)
	}
	rows := kv.MakeRowsFromKvPairs(kvs)
	err := writer.AppendRows(ctx, nil, rows)
	require.NoError(t, err)

	_, err = writer.Close(ctx)
	require.NoError(t, err)

	slices.SortFunc(kvs, func(i, j common.KvPair) bool {
		return bytes.Compare(i.Key, j.Key) < 0
	})

	bufSize := rand.Intn(100) + 1
	kvReader, err := newKVReader(ctx, "/test/0", memStore, 0, bufSize)
	require.NoError(t, err)
	for i := 0; i < kvCnt; i++ {
		key, value, err := kvReader.nextKV()
		require.NoError(t, err)
		require.Equal(t, kvs[i].Key, key)
		require.Equal(t, kvs[i].Val, value)
	}
	_, _, err = kvReader.nextKV()
	require.Equal(t, io.EOF, err)

	statReader, err := newStatsReader(ctx, memStore, "/test_stat/0", bufSize)
	require.NoError(t, err)

	var keyCnt uint64 = 0
	for {
		p, err := statReader.nextProp()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		keyCnt += p.keys
	}
	require.Equal(t, uint64(kvCnt), keyCnt)
}

func TestWriterFlushMultiFileNames(t *testing.T) {
	seed := time.Now().Unix()
	rand.Seed(uint64(seed))
	t.Logf("seed: %d", seed)
	ctx := context.Background()
	memStore := storage.NewMemStorage()

	writer := NewWriterBuilder().
		SetPropKeysDistance(2).
		SetMemorySizeLimit(60).
		Build(memStore, 0, "/test")

	// 200 bytes key values.
	kvCnt := 10
	kvs := make([]common.KvPair, kvCnt)
	for i := 0; i < kvCnt; i++ {
		kvs[i].Key = make([]byte, 10)
		_, err := rand.Read(kvs[i].Key)
		require.NoError(t, err)
		kvs[i].Val = make([]byte, 10)
		_, err = rand.Read(kvs[i].Val)
		require.NoError(t, err)
	}
	rows := kv.MakeRowsFromKvPairs(kvs)
	err := writer.AppendRows(ctx, nil, rows)
	require.NoError(t, err)

	_, err = writer.Close(ctx)
	require.NoError(t, err)

	var dataFiles, statFiles []string
	err = memStore.WalkDir(ctx, &storage.WalkOption{SubDir: "/test"}, func(path string, size int64) error {
		if strings.Contains(path, "_stat") {
			statFiles = append(statFiles, path)
		} else {
			dataFiles = append(dataFiles, path)
		}
		return nil
	})
	require.NoError(t, err)
	require.Len(t, dataFiles, 4)
	require.Len(t, statFiles, 4)
	for i := 0; i < 4; i++ {
		require.Equal(t, dataFiles[i], fmt.Sprintf("/test/%d", i))
		require.Equal(t, statFiles[i], fmt.Sprintf("/test_stat/%d", i))
	}
}
