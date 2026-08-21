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
	"container/heap"
	"context"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/pingcap/tidb/pkg/util/logutil"
)

const (
	loadRegionMaxRetry    = 8
	scanRegionBackoffBase = 200 * time.Millisecond
	scanRegionBackoffMax  = 2 * time.Second
	// chunkSize is the per-chunk work granularity.
	chunkSize = 10 * units.GiB
	// subtaskSize is the nominal size used to estimate the subtask count.
	subtaskSize = 200 * units.GiB
)

// generateChunks carves the task's tables into chunks and returns their total size.
func generateChunks(
	ctx context.Context,
	store kv.Storage,
	tableInfos map[int64]*model.TableInfo,
	meta *TaskMeta,
) ([]Chunk, int64, error) {
	refs, err := meta.tableRefs(tableInfos)
	if err != nil {
		return nil, 0, err
	}
	chunks := make([]Chunk, 0, len(refs))
	var total int64
	for tableIdx, ref := range refs {
		tableChunks, err := splitTable(ctx, store, ref.tableInfo, tableIdx)
		if err != nil {
			return nil, 0, err
		}
		for _, chunk := range tableChunks {
			total += chunk.Size
		}
		chunks = append(chunks, tableChunks...)
	}
	return chunks, total, nil
}

// splitTable carves one table into ~chunkSize key-ordered chunks, with a
// table-local ordinal spanning its partitions so file names stay unique.
func splitTable(
	ctx context.Context,
	store kv.Storage,
	tblInfo *model.TableInfo,
	tableIdx int,
) ([]Chunk, error) {
	pids := physicalIDs(tblInfo)
	chunks := make([]Chunk, 0, len(pids))
	ordinal := 0
	for _, pid := range pids {
		start, end := physicalTableRange(tblInfo, pid)
		endKeys, sizes, err := loadRegionSizes(ctx, store, start, end)
		if err != nil {
			return nil, err
		}
		tableChunks, nextOrdinal := chunksBySize(tableIdx, pid, start, end, endKeys, sizes, ordinal)
		ordinal = nextOrdinal
		chunks = append(chunks, tableChunks...)
	}
	return chunks, nil
}

// loadRegionSizes returns each region's end key and byte size over [start, end).
func loadRegionSizes(ctx context.Context, store kv.Storage, start, end kv.Key) (endKeys []kv.Key, sizes []int64, err error) {
	hStore, ok := store.(helper.Storage)
	if !ok {
		return nil, nil, errors.New("storage does not support region cache")
	}
	h := helper.NewHelper(hStore)
	backoffer := backoff.NewExponential(scanRegionBackoffBase, 2, scanRegionBackoffMax)
	err = handle.RunWithRetry(ctx, loadRegionMaxRetry, backoffer, logutil.BgLogger(), func(context.Context) (bool, error) {
		endKeys, sizes, err = h.RegionApproximateSizes(ctx, start, end)
		return isRetryablePlanningError(err), err
	})
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if len(sizes) == 0 {
		return nil, nil, errors.New("export: PD returned no regions for table range")
	}
	return endKeys, sizes, nil
}

// chunksBySize starts a new chunk each time the accumulated region size reaches
// chunkSize, so each chunk holds ~chunkSize of real data.
func chunksBySize(tableIdx int, pid int64, start, end kv.Key, endKeys []kv.Key, sizes []int64, startOrdinal int) ([]Chunk, int) {
	chunks := make([]Chunk, 0, len(sizes))
	ord := startOrdinal
	chunkStart := start
	var acc int64
	for i, s := range sizes {
		acc += s
		if acc < chunkSize && i < len(sizes)-1 {
			continue
		}
		chunkEnd := end
		if i < len(sizes)-1 {
			chunkEnd = endKeys[i]
		}
		chunks = append(chunks, Chunk{
			TableIdx:   tableIdx,
			PhysicalID: pid,
			Start:      chunkStart,
			End:        chunkEnd,
			Size:       acc,
			Ordinal:    ord,
		})
		ord++
		chunkStart = chunkEnd
		acc = 0
	}
	return chunks, ord
}

// divideSubtasks packs chunks into subtasks. The subtask count is estimated from
// subtaskSize and rounded up to a multiple of nodeCount so the framework can
// spread them evenly across nodes; see packSubtasks for how chunks are assigned.
func divideSubtasks(chunks []Chunk, nodeCount int) [][]Chunk {
	if len(chunks) == 0 {
		return nil
	}
	var total int64
	for _, c := range chunks {
		total += c.Size
	}
	count := int64(1)
	if total > subtaskSize {
		count = (total + subtaskSize/2) / subtaskSize
	}
	if nodeCount > 1 {
		n := int64(nodeCount)
		count = (count + n - 1) / n * n
	}
	return packSubtasks(chunks, int(count))
}

// packSubtasks assigns chunks to count subtasks by greedy least-loaded-bin
// (LPT) scheduling instead of sequential order, so a subtask's writes aren't
// all clustered on one db/table's S3 key prefix. Chunks at or above chunkSize
// are roughly equal weight (produced whenever a table's accumulated region
// size hits the cap), so shuffling them is free for balance and breaks up the
// table adjacency from the original generation order; the remaining,
// size-varying chunks (table tails and whole small tables) are packed
// largest-first, since LPT's balance guarantee only depends on relative order
// among differently-sized items.
func packSubtasks(chunks []Chunk, count int) [][]Chunk {
	regular := make([]Chunk, 0, len(chunks))
	irregular := make([]Chunk, 0)
	for _, c := range chunks {
		if c.Size >= chunkSize {
			regular = append(regular, c)
		} else {
			irregular = append(irregular, c)
		}
	}
	rand.Shuffle(len(regular), func(i, j int) { regular[i], regular[j] = regular[j], regular[i] })
	sort.Slice(irregular, func(i, j int) bool { return irregular[i].Size > irregular[j].Size })

	bins := make([]*subtaskBin, count)
	h := make(binHeap, count)
	for i := range bins {
		bins[i] = &subtaskBin{}
		h[i] = bins[i]
	}
	heap.Init(&h)
	assign := func(c Chunk) {
		b := heap.Pop(&h).(*subtaskBin)
		b.chunks = append(b.chunks, c)
		b.size += c.Size
		heap.Push(&h, b)
	}
	for _, c := range regular {
		assign(c)
	}
	for _, c := range irregular {
		assign(c)
	}

	subtasks := make([][]Chunk, 0, count)
	for _, b := range bins {
		if len(b.chunks) > 0 {
			subtasks = append(subtasks, b.chunks)
		}
	}
	return subtasks
}

// divideSchemaSubtasks splits tableCount table indices into up to count
// roughly equal groups, shuffled first so a subtask's schema-file writes
// aren't all clustered on adjacent tables' S3 key prefixes — same rationale
// as packSubtasks, but schema files are all near-equal (small) weight, so a
// plain shuffle-then-round-robin split is enough; no need for size-weighted
// bin packing.
func divideSchemaSubtasks(tableCount, count int) [][]int {
	if tableCount == 0 {
		return nil
	}
	idxs := make([]int, tableCount)
	for i := range idxs {
		idxs[i] = i
	}
	rand.Shuffle(len(idxs), func(i, j int) { idxs[i], idxs[j] = idxs[j], idxs[i] })

	count = min(max(count, 1), tableCount)
	groups := make([][]int, count)
	for i, idx := range idxs {
		groups[i%count] = append(groups[i%count], idx)
	}
	return groups
}

// subtaskBin accumulates the chunks assigned to one subtask by packSubtasks.
type subtaskBin struct {
	chunks []Chunk
	size   int64
}

// binHeap is a min-heap of subtaskBins ordered by accumulated size, so
// packSubtasks can always assign the next chunk to the least-loaded bin.
type binHeap []*subtaskBin

func (h binHeap) Len() int { return len(h) }
func (h binHeap) Less(i, j int) bool {
	if h[i].size != h[j].size {
		return h[i].size < h[j].size
	}
	// Break size ties (notably when every remaining chunk is zero-sized) by
	// chunk count, so assignment still rotates through bins instead of
	// getting stuck on one.
	return len(h[i].chunks) < len(h[j].chunks)
}
func (h binHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *binHeap) Push(x any)   { *h = append(*h, x.(*subtaskBin)) }
func (h *binHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func physicalIDs(tblInfo *model.TableInfo) []int64 {
	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		ids := make([]int64, 0, len(pi.Definitions))
		for _, def := range pi.Definitions {
			ids = append(ids, def.ID)
		}
		return ids
	}
	return []int64{tblInfo.ID}
}

// physicalTableRange returns the record-key range of one physical table. The
// int-handle start is a MinInt64 record key, not a bare "t<id>_r" prefix, which
// TiKV returns nothing for.
func physicalTableRange(tblInfo *model.TableInfo, pid int64) (start, end kv.Key) {
	prefix := tablecodec.GenTableRecordPrefix(pid)
	if tblInfo.IsCommonHandle {
		return prefix, prefix.PrefixNext()
	}
	return tablecodec.EncodeRowKeyWithHandle(pid, kv.IntHandle(math.MinInt64)), prefix.PrefixNext()
}
