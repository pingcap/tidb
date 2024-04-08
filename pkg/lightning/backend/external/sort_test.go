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
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/storage"
	dbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func changePropDist(t *testing.T, sizeDist, keysDist uint64) {
	sizeDistBak := defaultPropSizeDist
	keysDistBak := defaultPropKeysDist
	t.Cleanup(func() {
		defaultPropSizeDist = sizeDistBak
		defaultPropKeysDist = keysDistBak
	})
	defaultPropSizeDist = sizeDist
	defaultPropKeysDist = keysDist
}

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
	changePropDist(t, 100, 2)
	// 1. write data step
	seed := time.Now().Unix()
	rand.Seed(uint64(seed))
	t.Logf("seed: %d", seed)
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	memSizeLimit := (rand.Intn(10) + 1) * 400

	w := NewWriterBuilder().
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
	mergeMemSize := (rand.Intn(10) + 1) * 100
	// use random mergeMemSize to test different memLimit of writer.
	// reproduce one bug, see https://github.com/pingcap/tidb/issues/49590
	bufSizeBak := defaultReadBufferSize
	memLimitBak := defaultOneWriterMemSizeLimit
	t.Cleanup(func() {
		defaultReadBufferSize = bufSizeBak
		defaultOneWriterMemSizeLimit = memLimitBak
	})
	defaultReadBufferSize = 100
	defaultOneWriterMemSizeLimit = uint64(mergeMemSize)
	for _, group := range dataGroup {
		require.NoError(t, MergeOverlappingFiles(
			ctx,
			group,
			memStore,
			int64(5*size.MB),
			"/test2",
			mergeMemSize,
			closeFn,
			1,
			true,
		))
	}

	// 3. read and sort step
	testReadAndCompare(ctx, t, kvs, memStore, lastStepDatas, lastStepStats, startKey, memSizeLimit)
}

func TestGlobalSortLocalWithMergeV2(t *testing.T) {
	// 1. write data step
	seed := time.Now().Unix()
	rand.Seed(uint64(seed))
	t.Logf("seed: %d", seed)
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	memSizeLimit := (rand.Intn(10) + 1) * 400
	multiStats := make([]MultipleFilesStat, 0, 100)
	randomSize := (rand.Intn(500) + 1) * 1000

	failpoint.Enable("github.com/pingcap/tidb/pkg/lightning/backend/external/mockRangesGroupSize",
		"return("+strconv.Itoa(randomSize)+")")
	t.Cleanup(func() {
		failpoint.Disable("github.com/pingcap/tidb/pkg/lightning/backend/external/mockRangesGroupSize")
	})
	datas := make([]string, 0, 100)
	stats := make([]string, 0, 100)
	// prepare meta for merge step.
	closeFn := func(s *WriterSummary) {
		multiStats = append(multiStats, s.MultipleFilesStats...)
		for _, stat := range s.MultipleFilesStats {
			for i := range stat.Filenames {
				datas = append(datas, stat.Filenames[i][0])
				stats = append(stats, stat.Filenames[i][1])
			}
		}
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

	// 2. merge step
	dataGroup, statGroup, startKeys, endKeys := splitDataStatAndKeys(datas, stats, multiStats)
	lastStepDatas := make([]string, 0, 10)
	lastStepStats := make([]string, 0, 10)
	var startKey, endKey dbkv.Key

	// prepare meta for last step.
	closeFn1 := func(s *WriterSummary) {
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

	for i, group := range dataGroup {
		require.NoError(t, MergeOverlappingFilesV2(
			ctx,
			mockOneMultiFileStat(group, statGroup[i]),
			memStore,
			startKeys[i],
			endKeys[i],
			int64(5*size.MB),
			"/test2",
			uuid.NewString(),
			100,
			8*1024,
			100,
			2,
			closeFn1,
			1,
			true))
	}

	// 3. read and sort step
	testReadAndCompare(ctx, t, kvs, memStore, lastStepDatas, lastStepStats, startKey, memSizeLimit)
}
