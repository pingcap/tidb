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
	"testing"

	"github.com/jfcg/sorty/v2"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	dbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
)

func testReadAndCompare(
	ctx context.Context,
	t *testing.T,
	kvs []common.KvPair,
	store storage.ExternalStorage,
	datas []string,
	stats []string,
	startKey dbkv.Key,
	memSizeLimit int) {

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
	curStart := startKey.Clone()
	kvIdx := 0

	for {
		endKeyOfGroup, dataFilesOfGroup, statFilesOfGroup, _, err := splitter.SplitOneRangesGroup()
		require.NoError(t, err)
		curEnd := dbkv.Key(endKeyOfGroup).Clone()
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
		curStart = curEnd.Clone()

		if len(endKeyOfGroup) == 0 {
			break
		}
	}
	err = splitter.Close()
	require.NoError(t, err)
}

// split data and stat files into groups for merge step.
// like scheduler code for merge sort step in add index and import into.
func splitDataAndStatFiles(datas []string, stats []string) ([][]string, [][]string) {
	dataGroup := make([][]string, 0, 10)
	statGroup := make([][]string, 0, 10)

	start := 0
	step := 10
	for start < len(datas) {
		end := start + step
		if end > len(datas) {
			end = len(datas)
		}
		dataGroup = append(dataGroup, datas[start:end])
		statGroup = append(statGroup, stats[start:end])
		start = end
	}
	return dataGroup, statGroup
}
