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
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/exp/rand"
)

func TestReadAllDataBasic(t *testing.T) {
	seed := time.Now().Unix()
	rand.Seed(uint64(seed))
	t.Logf("seed: %d", seed)
	ctx := context.Background()

	memStore := storage.NewMemStorage()

	w := NewWriterBuilder().
		SetPropSizeDistance(100).
		SetPropKeysDistance(2).
		Build(memStore, "/test", "0")

	writer := NewEngineWriter(w)

	kvCnt := rand.Intn(10) + 10000
	logutil.BgLogger().Info("ywq test kvcnt", zap.Any("cnt", kvCnt))
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

	require.NoError(t, writer.AppendRows(ctx, nil, kv.MakeRowsFromKvPairs(kvs)))
	_, err := writer.Close(ctx)
	require.NoError(t, err)

	slices.SortFunc(kvs, func(i, j common.KvPair) int {
		return bytes.Compare(i.Key, j.Key)
	})

	files, _, err := GetAllFileNames(ctx, memStore, "")
	logutil.BgLogger().Info("files", zap.Any("files", files))

	splitter, err := NewRangeSplitter(
		ctx,
		[]string{"/test/0/0"},
		[]string{"/test/0_stat/0"},
		memStore,
		4*1024*1024*1024,
		math.MaxInt64,
		4*1024*1024*1024,
		math.MaxInt64,
		true,
	)
	intest.AssertNoError(err)

	bufPool := membuf.NewPool()
	loaded := &memKVsAndBuffers{}
	curStart := kvs[0].Key
	kvIdx := 0

	for {
		endKeyOfGroup, dataFilesOfGroup, statFilesOfGroup, _, err := splitter.SplitOneRangesGroup()
		intest.AssertNoError(err)
		curEnd := endKeyOfGroup
		if len(endKeyOfGroup) == 0 {
			curEnd = dbkv.Key(kvs[len(kvs)-1].Key).Next()
		}

		logutil.BgLogger().Info("cur start, end",
			zap.Any("cur start", curStart),
			zap.Any("cur end", curEnd),
			zap.Any("kvs[0]", kvs[0].Key),
			zap.Any("kvs[len-1]", kvs[len(kvs)-1].Key))

		err = readAllData(
			ctx,
			memStore,
			dataFilesOfGroup,
			statFilesOfGroup,
			curStart,
			curEnd,
			bufPool,
			loaded,
		)

		intest.AssertNoError(err)
		logutil.Logger(ctx).Info("reading external storage in MergeOverlappingFiles", zap.Int("kv-num", len(loaded.keys)))
		// check kvs sorted
		curStart = curEnd
		// sort
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
			logutil.BgLogger().Info("ywq test i", zap.Any("i", i))
			require.EqualValues(t, key, kvs[kvIdx].Key)
			require.EqualValues(t, loaded.values[i], kvs[kvIdx].Val)
			kvIdx++
		}
		if len(endKeyOfGroup) == 0 {
			break
		}
	}

}
