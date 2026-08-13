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
	"encoding/json"
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
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

func TestGroupBoundaries(t *testing.T) {
	b := keys("a", "b", "c", "d", "e") // 4 ranges

	// Two groups: contiguous, in key order, endpoints preserved, span-joined.
	groups := groupBoundaries(b, 2)
	require.Len(t, groups, 2)
	require.Equal(t, keys("a", "b", "c"), groups[0])
	require.Equal(t, keys("c", "d", "e"), groups[1])
	require.Equal(t, kv.Key("a"), groups[0][0])
	require.Equal(t, kv.Key("e"), groups[len(groups)-1][len(groups[len(groups)-1])-1])

	// Deterministic across calls.
	require.Equal(t, groups, groupBoundaries(b, 2))

	// One group covers the whole span.
	one := groupBoundaries(b, 1)
	require.Len(t, one, 1)
	require.Equal(t, b, one[0])

	// More groups than ranges: at most one range per group, none empty.
	many := groupBoundaries(b, 10)
	require.Len(t, many, 4)
	for _, g := range many {
		require.Len(t, g, 2)
	}
}

func TestSubtaskCntFor(t *testing.T) {
	// About one subtask per node.
	require.Equal(t, 4, subtaskCntFor(100, 4))
	// Fewer chunks than nodes → one subtask per chunk, no empties.
	require.Equal(t, 3, subtaskCntFor(3, 8))
	// Never zero.
	require.Equal(t, 1, subtaskCntFor(0, 0))
	// Capped: many chunks on one node still spread so no subtask exceeds the cap.
	got := subtaskCntFor(10*maxChunksPerSubtask, 1)
	require.GreaterOrEqual(t, got, 10)
}

func TestPackSubtasks(t *testing.T) {
	// 8 equal-sized chunks (1 GiB each), 4 nodes → engineSize = 8GiB/4 = 2GiB →
	// each subtask accumulates 2 chunks; covers all chunks in order once.
	const oneGiB = 1024 * 1024 * 1024
	chunks := make([]Chunk, 8)
	var total int64
	for i := range chunks {
		chunks[i] = Chunk{TableIdx: 0, Ordinal: i, Size: oneGiB}
		total += oneGiB
	}
	metas, err := packSubtasks(chunks, total, 4)
	require.NoError(t, err)
	require.Len(t, metas, 4)
	var seen []int
	for _, bs := range metas {
		st := &SubtaskMeta{}
		require.NoError(t, json.Unmarshal(bs, st))
		require.NotEmpty(t, st.Chunks)
		require.Len(t, st.Chunks, 2) // balanced by size
		for _, c := range st.Chunks {
			seen = append(seen, c.Ordinal)
		}
	}
	require.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7}, seen)

	// Uneven sizes: one big chunk forms its own subtask.
	uneven := []Chunk{
		{Ordinal: 0, Size: 4 * oneGiB},
		{Ordinal: 1, Size: oneGiB},
		{Ordinal: 2, Size: oneGiB},
	}
	m2, err := packSubtasks(uneven, 6*oneGiB, 2) // engineSize = 3GiB
	require.NoError(t, err)
	require.Len(t, m2, 2)

	empty, err := packSubtasks(nil, 0, 4)
	require.NoError(t, err)
	require.Nil(t, empty)
}

func TestBuildChunks(t *testing.T) {
	const oneGiB = 1024 * 1024 * 1024
	// 4 regions, 8 GiB → 8 chunks wanted but capped at 4 regions, so 4 chunks.
	require.Len(t, mustChunks(buildChunks(0, 1, keys("a", "b", "c", "d", "e"), 8*oneGiB, 0)), 4)
	// One chunk per ~chunkSize, rounded up, never more than the region count.
	require.Len(t, mustChunks(buildChunks(0, 1, keys("a", "b", "c", "d", "e"), 2*oneGiB, 0)), 2)
	require.Len(t, mustChunks(buildChunks(0, 1, keys("a", "b", "c", "d", "e"), 2*oneGiB+1, 0)), 3)
	// Zero size still yields one chunk covering the whole span.
	require.Len(t, mustChunks(buildChunks(0, 1, keys("a", "b", "c", "d", "e"), 0, 0)), 1)

	const size = 8 * oneGiB
	b := keys("a", "b", "c", "d", "e") // 4 regions → 4 chunks

	chunks, next := buildChunks(2, 100, b, size, 0)
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
	require.Equal(t, int64(size)*1/4, chunks[0].Size)

	// Deterministic.
	again, _ := buildChunks(2, 100, b, size, 0)
	require.Equal(t, chunks, again)

	// Ordinal continues across partitions of the same table.
	more, next2 := buildChunks(2, 200, keys("a", "z"), size, next)
	require.Equal(t, 4, more[0].Ordinal)
	require.Equal(t, int64(200), more[0].PhysicalID)
	require.Equal(t, 5, next2)
}

func mustChunks(chunks []Chunk, _ int) []Chunk { return chunks }

func TestPhysicalTableRange(t *testing.T) {
	const pid = 100
	prefix := tablecodec.GenTableRecordPrefix(pid)
	end := prefix.PrefixNext()

	// Common handle: start is the bare record prefix.
	cs, ce := physicalTableRange(&model.TableInfo{IsCommonHandle: true}, pid)
	require.Equal(t, kv.Key(prefix), cs)
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

func TestPhysicalIDs(t *testing.T) {
	require.Equal(t, []int64{100}, physicalIDs(&model.TableInfo{ID: 100}))

	part := &model.TableInfo{ID: 100, Partition: &model.PartitionInfo{
		Enable:      true,
		Definitions: []model.PartitionDefinition{{ID: 101}, {ID: 102}, {ID: 103}},
	}}
	require.Equal(t, []int64{101, 102, 103}, physicalIDs(part))
}
