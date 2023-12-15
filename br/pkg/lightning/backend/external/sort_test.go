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
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/storage"
	dbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestGlobalSortLocalBasic(t *testing.T) {
	// 1. write data step
	seed := time.Now().Unix()
	rand.Seed(uint64(seed))
	t.Logf("seed: %d", seed)
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	memSizeLimit := (rand.Intn(10) + 1) * 400
	lastStepDatas := make([]string, 0, 10)
	lastStepStats := make([]string, 0, 10)
	var startKey, endKey dbkv.Key

	closeFn := func(s *WriterSummary) {
		for _, stat := range s.MultipleFilesStats {
			for i := range stat.Filenames {
				lastStepDatas = append(lastStepDatas, stat.Filenames[i][0])
				lastStepStats = append(lastStepStats, stat.Filenames[i][1])
			}
		}
		if len(startKey) == 0 && len(endKey) == 0 {
			startKey = s.Min.Clone()
			endKey = s.Max.Clone().Next()
		}
		startKey = BytesMin(startKey, s.Min.Clone())
		endKey = BytesMax(endKey, s.Max.Clone().Next())
	}

	w := NewWriterBuilder().
		SetPropSizeDistance(100).
		SetPropKeysDistance(2).
		SetMemorySizeLimit(uint64(memSizeLimit)).
		SetBlockSize(memSizeLimit).
		SetOnCloseFunc(closeFn).
		Build(memStore, "/test", "0")

	writer := NewEngineWriter(w)
	kvCnt := rand.Intn(10) + 10000
	kvs := make([]common.KvPair, kvCnt)
	for i := 0; i < kvCnt; i++ {
		kvs[i] = common.KvPair{
			Key: []byte(uuid.New().String()),
			Val: []byte("56789"),
		}
	}
	slices.SortFunc(kvs, func(i, j common.KvPair) int {
		return bytes.Compare(i.Key, j.Key)
	})

	require.NoError(t, writer.AppendRows(ctx, nil, kv.MakeRowsFromKvPairs(kvs)))
	_, err := writer.Close(ctx)
	require.NoError(t, err)

	// 2. read and sort step
	testReadAndCompare(ctx, t, kvs, memStore, lastStepDatas, lastStepStats, startKey, memSizeLimit)
}

func TestGlobalSortLocalWithMerge(t *testing.T) {
	// 1. write data step
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
			Key: []byte(uuid.New().String()),
			Val: []byte("56789"),
		}
	}

	slices.SortFunc(kvs, func(i, j common.KvPair) int {
		return bytes.Compare(i.Key, j.Key)
	})

	require.NoError(t, writer.AppendRows(ctx, nil, kv.MakeRowsFromKvPairs(kvs)))
	_, err := writer.Close(ctx)
	require.NoError(t, err)

	// 2. merge step
	datas, stats, err := GetAllFileNames(ctx, memStore, "")
	require.NoError(t, err)

	dataGroup, _ := splitDataAndStatFiles(datas, stats)

	lastStepDatas := make([]string, 0, 10)
	lastStepStats := make([]string, 0, 10)
	var startKey, endKey dbkv.Key

	closeFn := func(s *WriterSummary) {
		for _, stat := range s.MultipleFilesStats {
			for i := range stat.Filenames {
				lastStepDatas = append(lastStepDatas, stat.Filenames[i][0])
				lastStepStats = append(lastStepStats, stat.Filenames[i][1])
			}

		}
		if len(startKey) == 0 && len(endKey) == 0 {
			startKey = s.Min.Clone()
			endKey = s.Max.Clone().Next()
		}
		startKey = BytesMin(startKey, s.Min.Clone())
		endKey = BytesMax(endKey, s.Max.Clone().Next())
	}

	for _, group := range dataGroup {
		MergeOverlappingFiles(
			ctx,
			group,
			memStore,
			int64(5*size.MB),
			100,
			"/test2",
			100,
			8*1024,
			100,
			2,
			closeFn,
			1,
			true,
		)
	}

	// 3. read and sort step
	testReadAndCompare(ctx, t, kvs, memStore, lastStepDatas, lastStepStats, startKey, memSizeLimit)
}
