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
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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

func splitDataStatAndKeys(datas []string, stats []string, multiStats []MultipleFilesStat) ([][]string, [][]string, []dbkv.Key, []dbkv.Key) {
	startKeys := make([]dbkv.Key, 0, 10)
	endKeys := make([]dbkv.Key, 0, 10)
	i := 0
	for ; i < len(multiStats)-1; i += 2 {
		startKey := BytesMin(multiStats[i].MinKey, multiStats[i+1].MinKey)
		endKey := BytesMax(multiStats[i].MaxKey, multiStats[i+1].MaxKey)
		endKey = dbkv.Key(endKey).Next().Clone()
		startKeys = append(startKeys, startKey)
		endKeys = append(endKeys, endKey)
	}
	if i == len(multiStats)-1 {
		startKeys = append(startKeys, multiStats[i].MinKey.Clone())
		endKeys = append(endKeys, dbkv.Key(multiStats[i].MaxKey).Next().Clone())
	}

	dataGroup := make([][]string, 0, 10)
	statGroup := make([][]string, 0, 10)

	start := 0
	step := 1000
	for start < len(datas) {
		end := start + step
		if end > len(datas) {
			end = len(datas)
		}
		dataGroup = append(dataGroup, datas[start:end])
		statGroup = append(statGroup, stats[start:end])
		start = end
	}
	return dataGroup, statGroup, startKeys, endKeys
}

func TestGlobalSortLocalWithMergeV2(t *testing.T) {
	// 1. write data step
	seed := time.Now().Unix()
	rand.Seed(uint64(seed))
	t.Logf("seed: %d", seed)
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	// memSizeLimit := (rand.Intn(10) + 1) * 400
	memSizeLimit := 400

	multiStats := make([]MultipleFilesStat, 0, 100)
	closeFn := func(s *WriterSummary) {
		multiStats = append(multiStats, s.MultipleFilesStats...)
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
	datas, stats, err := GetAllFileNames(ctx, memStore, "")
	require.NoError(t, err)

	// ywq todo check it.
	dataGroup, statGroup, startKeys, endKeys := splitDataStatAndKeys(datas, stats, multiStats)

	for _, stat := range multiStats {
		logutil.BgLogger().Info("ywq test stat", zap.Binary("minKey", stat.MinKey), zap.Binary("maxKey", stat.MaxKey))
	}

	logutil.BgLogger().Info("ywq test actual min max", zap.Any("min", kvs[0].Key), zap.Any("max", kvs[len(kvs)-1].Key))

	// startKeys "000a94e1-edc7-4583-893c-4598bee2cde4" "b4a5ed60-16f5-4981-9b9c-3c4b7a5a1232"
	// endkeys "b49f03c6-4b0e-4788-808d-2e546e906fac\x00" "fff1a2c3-277e-444b-9a94-fa3d841fce21\x00"

	// multistats
	// min "000a94e1-edc7-4583-893c-4598bee2cde4" max "587ee8e5-7725-4353-91df-d8cbdd4672dc"
	//     "58846994-849b-4844-bf9f-8578590a5cab"     "b49f03c6-4b0e-4788-808d-2e546e906fac"
	//     "b4a5ed60-16f5-4981-9b9c-3c4b7a5a1232"     "fff1a2c3-277e-444b-9a94-fa3d841fce21"

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
	logutil.BgLogger().Info("ywq test start merge step")
	logutil.BgLogger().Info("ywq test see meta", zap.Any("startKeys", startKeys), zap.Any("endKeys", endKeys))

	for i, group := range dataGroup {
		MergeOverlappingFilesV2(
			ctx, group, statGroup[i], memStore, startKeys[i], endKeys[i], int64(5*size.MB), "/test2", uuid.NewString(), 100,
			8*1024, 100, 2, closeFn1, 1, true)
	}

	// 3. read and sort step
	logutil.BgLogger().Info("ywq test start last step")
	testReadAndCompare(ctx, t, kvs, memStore, lastStepDatas, lastStepStats, startKey, memSizeLimit)
}
