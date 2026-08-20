// Copyright 2026 PingCAP, Inc.
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

package export

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/planner/extstore"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/stretchr/testify/require"
)

func keys(ss ...string) []kv.Key {
	ks := make([]kv.Key, len(ss))
	for i, s := range ss {
		ks[i] = kv.Key(s)
	}
	return ks
}

func TestDivideSubtasks(t *testing.T) {
	// 40 chunks of chunkSize → total = 4 * subtaskSize.
	const n = 4 * subtaskSize / chunkSize
	chunks := make([]Chunk, n)
	for i := range chunks {
		chunks[i] = Chunk{Ordinal: i, Size: chunkSize}
	}
	assertCovers := func(groups [][]Chunk) {
		var seen []int
		for _, g := range groups {
			for _, c := range g {
				seen = append(seen, c.Ordinal)
			}
		}
		want := make([]int, n)
		for i := range want {
			want[i] = i
		}
		require.Equal(t, want, seen)
	}

	// nodeCount=1: count = round(total/subtaskSize) = 4.
	groups := divideSubtasks(chunks, 1)
	require.Len(t, groups, 4)
	assertCovers(groups)

	// nodeCount=3: 4 rounds up to the next multiple of 3 → 6 subtasks.
	groups = divideSubtasks(chunks, 3)
	require.Len(t, groups, 6)
	assertCovers(groups)

	// Data below subtaskSize stays a single subtask (count floored at 1).
	require.Len(t, divideSubtasks(chunks[:1], 1), 1)
	zeroSizeChunks := []Chunk{{Ordinal: 0}, {Ordinal: 1}, {Ordinal: 2}}
	require.Equal(t, [][]Chunk{zeroSizeChunks}, divideSubtasks(zeroSizeChunks, 3))
	require.Nil(t, divideSubtasks(nil, 2))
}

func TestSubtaskMetaExternal(t *testing.T) {
	store := objstore.NewMemStorage()
	extstore.SetGlobalExtStorageForTest(store)
	t.Cleanup(func() { extstore.SetGlobalExtStorageForTest(nil) })
	ctx := context.Background()
	chunks := []Chunk{
		{TableIdx: 1, PhysicalID: 100, Start: []byte("aaaa"), End: []byte("bbbb"), Size: chunkSize, Ordinal: 0},
		{TableIdx: 1, PhysicalID: 100, Start: []byte("bbbb"), End: []byte("cccc"), Size: chunkSize, Ordinal: 1},
	}
	preparedPath, err := writePreparedPlan(ctx, 1, chunks)
	require.NoError(t, err)
	require.Equal(t, "1/plan/prepared/meta.json", preparedPath)
	preparedChunks, err := readPreparedPlan(ctx, preparedPath)
	require.NoError(t, err)
	require.Equal(t, chunks, preparedChunks)
	metaJSON, err := json.Marshal(&TaskMeta{PreparedPlanPath: preparedPath})
	require.NoError(t, err)
	decodedMeta := &TaskMeta{}
	require.NoError(t, json.Unmarshal(metaJSON, decodedMeta))
	require.Equal(t, preparedPath, decodedMeta.PreparedPlanPath)
	_, err = readPreparedPlan(ctx, "")
	require.ErrorContains(t, err, "prepared plan path is empty")

	sm := &SubtaskMeta{Chunks: chunks}
	sm.ExternalPath = "1/plan/dump/1/meta"
	require.NoError(t, sm.WriteJSONToExternalStorage(ctx, store, sm))

	// The framework row keeps only the reference, not the chunk payload.
	row, err := sm.Marshal(sm)
	require.NoError(t, err)
	require.Contains(t, string(row), sm.ExternalPath)
	require.NotContains(t, string(row), "aaaa")

	// Reading the row back and then the external file reconstructs the chunks.
	got := &SubtaskMeta{}
	require.NoError(t, json.Unmarshal(row, got))
	require.Empty(t, got.Chunks)
	require.NoError(t, got.ReadJSONFromExternalStorage(ctx, store, got))
	require.Equal(t, chunks, got.Chunks)
}

func TestChunksBySize(t *testing.T) {
	start, end := kv.Key("a"), kv.Key("e")
	ends := keys("b", "c", "d", "e") // 4 regions over [a, e)
	half := int64(chunkSize / 2)
	// Two regions per chunk reach chunkSize → 2 chunks, real accumulated size.
	chunks, next := chunksBySize(2, 100, start, end, ends, []int64{half, half, half, half}, 0)
	require.Len(t, chunks, 2)
	require.Equal(t, 2, next)
	require.Equal(t, start, kv.Key(chunks[0].Start))
	require.Equal(t, chunks[0].End, chunks[1].Start)
	require.Equal(t, end, kv.Key(chunks[len(chunks)-1].End))
	require.Equal(t, int64(chunkSize), chunks[0].Size)
	require.Equal(t, 0, chunks[0].Ordinal)
	require.Equal(t, 2, chunks[0].TableIdx)
	require.Equal(t, int64(100), chunks[0].PhysicalID)

	// A region past the limit is its own chunk; the sub-limit tail still flushes.
	c2, _ := chunksBySize(0, 1, kv.Key("a"), kv.Key("c"), keys("b", "c"), []int64{2 * chunkSize, 1}, 0)
	require.Len(t, c2, 2)
	require.Equal(t, int64(2*chunkSize), c2[0].Size)
	require.Equal(t, int64(1), c2[1].Size)

	// Ordinal continues from startOrdinal.
	_, n := chunksBySize(0, 1, kv.Key("a"), kv.Key("b"), keys("b"), []int64{1}, 5)
	require.Equal(t, 6, n)
}

func TestChunksByCount(t *testing.T) {
	b := keys("a", "b", "c", "d", "e") // 4 regions
	// One chunk per ~chunkSize, rounded up, never more than the region count.
	require.Len(t, chunksOnly(chunksByCount(0, 1, b, 2*chunkSize, 0)), 2)
	require.Len(t, chunksOnly(chunksByCount(0, 1, b, 2*chunkSize+1, 0)), 3)
	// More chunks wanted than regions → capped at the region count.
	require.Len(t, chunksOnly(chunksByCount(0, 1, b, 100*chunkSize, 0)), 4)
	// Zero size still yields one chunk covering the whole span.
	require.Len(t, chunksOnly(chunksByCount(0, 1, b, 0, 0)), 1)

	const size = 4 * chunkSize // 4 regions → 4 chunks, one region each
	chunks, next := chunksByCount(2, 100, b, size, 0)
	require.Len(t, chunks, 4)
	require.Equal(t, 4, next)

	// Cover [a, e) with no gap or overlap, in key order.
	require.Equal(t, []byte("a"), chunks[0].Start)
	require.Equal(t, chunks[0].End, chunks[1].Start)
	require.Equal(t, []byte("e"), chunks[len(chunks)-1].End)
	// Table-local ordinals and metadata.
	require.Equal(t, 0, chunks[0].Ordinal)
	require.Equal(t, 2, chunks[0].TableIdx)
	require.Equal(t, int64(100), chunks[0].PhysicalID)
	// Size apportioned by region count (1 of 4 regions each).
	require.Equal(t, int64(size)/4, chunks[0].Size)

	// Deterministic.
	again, _ := chunksByCount(2, 100, b, size, 0)
	require.Equal(t, chunks, again)

	// Ordinal continues across partitions of the same table.
	more, next2 := chunksByCount(2, 200, keys("a", "z"), size, next)
	require.Equal(t, 4, more[0].Ordinal)
	require.Equal(t, int64(200), more[0].PhysicalID)
	require.Equal(t, 5, next2)
}

func chunksOnly(chunks []Chunk, _ int) []Chunk { return chunks }

func TestPhysicalTableRange(t *testing.T) {
	const pid = 100
	prefix := tablecodec.GenTableRecordPrefix(pid)
	end := prefix.PrefixNext()

	// Common handle: start is the bare record prefix.
	cs, ce := physicalTableRange(&model.TableInfo{IsCommonHandle: true}, pid)
	require.Equal(t, prefix, cs)
	require.Equal(t, end, ce)

	// Int handle: start is a well-formed MinInt64 record key under the table's
	// record prefix (a bare prefix start makes TiKV return nothing), and stays
	// within [prefix, end).
	is, ie := physicalTableRange(&model.TableInfo{IsCommonHandle: false}, pid)
	require.Equal(t, tablecodec.EncodeRowKeyWithHandle(pid, kv.IntHandle(math.MinInt64)), is)
	require.Equal(t, end, ie)
	require.True(t, bytes.HasPrefix(is, cs))
	require.True(t, bytes.Compare(is, ie) < 0)
}
