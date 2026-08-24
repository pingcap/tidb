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

	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
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

// assertCoversSet checks that groups together contain exactly the input
// chunks' ordinals, once each, regardless of order or which group they
// landed in — packSubtasks shuffles regular chunks, so output order is not
// guaranteed to follow the input order.
func assertCoversSet(t *testing.T, wantOrdinals []int, groups [][]Chunk) {
	t.Helper()
	var seen []int
	for _, g := range groups {
		for _, c := range g {
			seen = append(seen, c.Ordinal)
		}
	}
	require.ElementsMatch(t, wantOrdinals, seen)
}

func TestDivideSubtasks(t *testing.T) {
	// 40 chunks of chunkSize → total = 4 * subtaskSize.
	const n = 4 * subtaskSize / chunkSize
	chunks := make([]Chunk, n)
	ordinals := make([]int, n)
	for i := range chunks {
		chunks[i] = Chunk{Ordinal: i, Size: chunkSize}
		ordinals[i] = i
	}

	// nodeCount=1: count = round(total/subtaskSize) = 4.
	groups := divideSubtasks(chunks, 1)
	require.Len(t, groups, 4)
	assertCoversSet(t, ordinals, groups)
	for _, g := range groups {
		require.Len(t, g, n/4, "equal-weight chunks should split evenly across bins")
	}

	// nodeCount=3: 4 rounds up to the next multiple of 3 → 6 subtasks.
	groups = divideSubtasks(chunks, 3)
	require.Len(t, groups, 6)
	assertCoversSet(t, ordinals, groups)

	// Data below subtaskSize stays a single subtask (count floored at 1).
	require.Len(t, divideSubtasks(chunks[:1], 1), 1)
	zeroSizeChunks := []Chunk{{Ordinal: 0}, {Ordinal: 1}, {Ordinal: 2}}
	require.Len(t, divideSubtasks(zeroSizeChunks, 3), 3)
	assertCoversSet(t, []int{0, 1, 2}, divideSubtasks(zeroSizeChunks, 3))
	require.Nil(t, divideSubtasks(nil, 2))
}

func TestDivideSchemaSubtasks(t *testing.T) {
	want := make([]int, 10)
	for i := range want {
		want[i] = i
	}

	// count divides evenly: every group is the same size.
	groups := divideSchemaSubtasks(10, 5)
	require.Len(t, groups, 5)
	seen := make([]int, 0, 10)
	for _, g := range groups {
		require.Len(t, g, 2)
		seen = append(seen, g...)
	}
	require.ElementsMatch(t, want, seen)

	// count doesn't divide evenly: group sizes differ by at most 1.
	groups = divideSchemaSubtasks(10, 3)
	require.Len(t, groups, 3)
	seen = make([]int, 0, 10)
	for _, g := range groups {
		require.LessOrEqual(t, len(g), 4)
		require.GreaterOrEqual(t, len(g), 3)
		seen = append(seen, g...)
	}
	require.ElementsMatch(t, want, seen)

	// count is clamped to tableCount, not left empty.
	groups = divideSchemaSubtasks(2, 5)
	require.Len(t, groups, 2)

	// count <= 0 still yields at least 1 group.
	groups = divideSchemaSubtasks(3, 0)
	require.Len(t, groups, 1)
	require.ElementsMatch(t, []int{0, 1, 2}, groups[0])

	require.Nil(t, divideSchemaSubtasks(0, 3))
}

// TestPackSubtasksBalance checks the two properties packSubtasks relies on:
// oversized (>= chunkSize) chunks are equal weight so any split among bins is
// balanced, and the remaining, size-varying chunks are packed largest-first
// (LPT) so no bin ends up starved.
func TestPackSubtasksBalance(t *testing.T) {
	// All-regular chunks: greedy least-loaded packing must split them evenly.
	chunks := make([]Chunk, 100)
	for i := range chunks {
		chunks[i] = Chunk{Ordinal: i, Size: chunkSize}
	}
	groups := packSubtasks(chunks, 5)
	require.Len(t, groups, 5)
	for _, g := range groups {
		require.Len(t, g, 20)
	}

	// A skewed mix of a few large (irregular but sizable) chunks and many tiny
	// ones must still balance close to the mean, not dump all the big ones on
	// one bin, since LPT places the large items first while bins are empty.
	mixed := make([]Chunk, 0, 505)
	for i := range 5 {
		mixed = append(mixed, Chunk{Ordinal: i, Size: chunkSize - 1}) // irregular, large
	}
	for i := range 500 {
		mixed = append(mixed, Chunk{Ordinal: 100 + i, Size: 1}) // irregular, tiny
	}
	groups = packSubtasks(mixed, 5)
	require.Len(t, groups, 5)
	var total int64
	for _, c := range mixed {
		total += c.Size
	}
	mean := total / 5
	for _, g := range groups {
		var size int64
		for _, c := range g {
			size += c.Size
		}
		require.InDelta(t, mean, size, float64(chunkSize)/10,
			"bin size should stay close to the mean, not concentrate the large chunks")
	}
}

func TestNewSubtaskMeta(t *testing.T) {
	chunks := []Chunk{
		{Ordinal: 0, Size: 100},
		{Ordinal: 1, Size: 250},
	}
	sm := newSubtaskMeta(chunks)
	require.Equal(t, chunks, sm.Chunks)
	require.Equal(t, 2, sm.ChunkCount)
	require.Equal(t, int64(350), sm.TotalSize)

	empty := newSubtaskMeta(nil)
	require.Equal(t, 0, empty.ChunkCount)
	require.Equal(t, int64(0), empty.TotalSize)
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

func TestMarshalSubtasks(t *testing.T) {
	store := objstore.NewMemStorage()
	extstore.SetGlobalExtStorageForTest(store)
	t.Cleanup(func() { extstore.SetGlobalExtStorageForTest(nil) })
	ctx := context.Background()

	groups := [][]Chunk{
		{{Ordinal: 0, Size: 100}, {Ordinal: 1, Size: 200}},
		{{Ordinal: 2, Size: 50}},
	}
	metas, err := marshalSubtasks(ctx, 7, proto.ExportStepDump, groups)
	require.NoError(t, err)
	require.Len(t, metas, 2)

	// The row keeps the chunk-batch summary but not the chunk payload, so
	// stats stay queryable after the external file is cleaned up.
	var sm0 SubtaskMeta
	require.NoError(t, json.Unmarshal(metas[0], &sm0))
	require.Equal(t, "7/plan/dump/1/meta.json", sm0.ExternalPath)
	require.Equal(t, 2, sm0.ChunkCount)
	require.Equal(t, int64(300), sm0.TotalSize)
	require.Empty(t, sm0.Chunks)

	var sm1 SubtaskMeta
	require.NoError(t, json.Unmarshal(metas[1], &sm1))
	require.Equal(t, "7/plan/dump/2/meta.json", sm1.ExternalPath)
	require.Equal(t, 1, sm1.ChunkCount)
	require.Equal(t, int64(50), sm1.TotalSize)

	empty, err := marshalSubtasks(ctx, 7, proto.ExportStepDump, nil)
	require.NoError(t, err)
	require.Nil(t, empty)
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
