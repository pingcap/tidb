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

package globalsort

import (
	"bytes"
	"context"
	"math"
	"testing"

	"github.com/jfcg/sorty/v2"
	"github.com/pingcap/tidb/pkg/ingestor/simplesst"
	dbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

func mockOneMultiFileStat(data, stat []string) []MultipleFilesStat {
	m := MultipleFilesStat{}
	for i := range data {
		m.Filenames = append(m.Filenames, [2]string{data[i], stat[i]})
	}
	return []MultipleFilesStat{m}
}

func testReadAndCompare(
	ctx context.Context,
	t *testing.T,
	kvs []common.KvPair,
	store storeapi.Storage,
	datas []string,
	stats []string,
	startKey dbkv.Key,
	memSizeLimit int) {
	splitter, err := NewRangeSplitter(
		ctx,
		mockOneMultiFileStat(datas, stats),
		store,
		int64(memSizeLimit), // make the group small for testing
		math.MaxInt64,
		4*1024*1024*1024,
		math.MaxInt64,
		math.MaxInt64,
		math.MaxInt64,
	)
	require.NoError(t, err)

	bufPool := membuf.NewPool()
	loaded := &memKVsAndBuffers{}
	curStart := startKey.Clone()
	kvIdx := 0

	for {
		endKeyOfGroup, dataFilesOfGroup, statFilesOfGroup, _, _, err := splitter.SplitOneRangesGroup()
		require.NoError(t, err)
		curEnd := dbkv.Key(endKeyOfGroup).Clone()
		if len(endKeyOfGroup) == 0 {
			curEnd = dbkv.Key(kvs[len(kvs)-1].Key).Next()
		}

		readRanges, err := simplesst.GetReadRangeFromProps(
			ctx, [][]byte{curStart, curEnd}, statFilesOfGroup, store)
		require.NoError(t, err)

		err = readAllData(
			ctx,
			store,
			dataFilesOfGroup,
			statFilesOfGroup,
			curStart,
			curEnd,
			readRanges[0],
			readRanges[1],
			bufPool,
			bufPool,
			loaded,
		)
		require.NoError(t, err)
		loaded.build(ctx)

		// check kvs sorted
		sorty.MaxGor = uint64(8)
		sorty.Sort(len(loaded.kvs), func(i, k, r, s int) bool {
			if bytes.Compare(loaded.kvs[i].Key, loaded.kvs[k].Key) < 0 { // strict comparator like < or >
				if r != s {
					loaded.kvs[r], loaded.kvs[s] = loaded.kvs[s], loaded.kvs[r]
				}
				return true
			}
			return false
		})
		for _, kv := range loaded.kvs {
			require.EqualValues(t, kvs[kvIdx].Key, kv.Key)
			require.EqualValues(t, kvs[kvIdx].Val, kv.Value)
			kvIdx++
		}
		curStart = curEnd.Clone()

		// release
		loaded.kvs = nil
		loaded.memKVBuffers = nil

		if len(endKeyOfGroup) == 0 {
			break
		}
	}
	err = splitter.Close()
	require.NoError(t, err)
}
