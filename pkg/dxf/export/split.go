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
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/tikv/client-go/v2/tikv"
)

const (
	loadRegionMaxRetry = 8
	// defaultRegionSize approximates a TiKV region's size, used to size a chunk
	// by region count without querying PD. A later refinement can sum PD region
	// approximate sizes for an accurate estimate.
	defaultRegionSize = 96 * 1024 * 1024
	// chunkSize is the target key-range size of one chunk — the unit of work a
	// worker pulls and exports. It is deliberately larger than FileSize (the
	// within-chunk file-cut size): a worker reads a chunk and rotates a new
	// output file every FileSize, so e.g. a 1 GiB chunk with a 256 MiB FileSize
	// emits about four files.
	chunkSize = 1024 * 1024 * 1024
	// maxChunksPerSubtask caps how many chunks one subtask carries, so a schema
	// of many small tables still spreads across subtasks.
	maxChunksPerSubtask = 4000
)

// splitTableSet builds the Dump-step subtasks for the whole table set. It first
// carves every table into key-ordered chunks (each ≈ one output file), then
// packs the chunks into subtasks. A chunk is the atomic unit of work with a
// fixed, worker-independent file name; a subtask is a batch of chunks whose
// worker pool exports them concurrently. The result is deterministic given the
// same region layout.
func splitTableSet(ctx context.Context, store kv.Storage, meta *TaskMeta, nodeCnt int) ([][]byte, error) {
	var chunks []Chunk
	var totalSize int64
	for tableIdx := range meta.Tables {
		tableChunks, err := chunkTable(ctx, store, meta, tableIdx)
		if err != nil {
			return nil, err
		}
		for _, c := range tableChunks {
			totalSize += c.Size
		}
		chunks = append(chunks, tableChunks...)
	}
	return packSubtasks(chunks, totalSize, nodeCnt)
}

// chunkTable carves one table into key-ordered chunks of ≈ chunkSize each,
// stamping a running table-local Ordinal across all of its partitions so file
// names never collide within the table, and estimating each chunk's byte size.
// The sort + continuity check runs per physical table (cross-partition key gaps
// are expected).
//
// Chunk size is currently estimated as region count × defaultRegionSize; a
// follow-up replaces this with PD region approximate sizes.
func chunkTable(ctx context.Context, store kv.Storage, meta *TaskMeta, tableIdx int) ([]Chunk, error) {
	tblInfo := meta.Tables[tableIdx].TableInfo
	perChunk := regionsPerChunk()
	var chunks []Chunk
	ordinal := 0
	for _, pid := range physicalIDs(tblInfo) {
		start, end := physicalTableRange(tblInfo, pid)
		boundaries, err := loadRegionBoundaries(ctx, store, start, end)
		if err != nil {
			return nil, err
		}
		regionCnt := len(boundaries) - 1
		chunkCnt := max(1, (regionCnt+perChunk-1)/perChunk)
		for _, g := range groupBoundaries(boundaries, chunkCnt) {
			chunks = append(chunks, Chunk{
				TableIdx:   tableIdx,
				PhysicalID: pid,
				Start:      g[0],
				End:        g[len(g)-1],
				Size:       int64(len(g)-1) * defaultRegionSize,
				Ordinal:    ordinal,
			})
			ordinal++
		}
	}
	return chunks, nil
}

// packSubtasks groups the chunks into subtasks in key order so each subtask
// holds a similar amount of data, following IMPORT INTO's adjusted-engine-size
// approach: aim for about one subtask per node, target engineSize =
// ceil(totalSize/subtaskCnt), and accumulate chunks until a subtask reaches it.
func packSubtasks(chunks []Chunk, totalSize int64, nodeCnt int) ([][]byte, error) {
	if len(chunks) == 0 {
		return nil, nil
	}
	subtaskCnt := subtaskCntFor(len(chunks), nodeCnt)
	engineSize := (totalSize + int64(subtaskCnt) - 1) / int64(subtaskCnt)

	var subtasks [][]byte
	emit := func(batch []Chunk) error {
		bs, err := json.Marshal(&SubtaskMeta{Chunks: batch})
		if err != nil {
			return errors.Trace(err)
		}
		subtasks = append(subtasks, bs)
		return nil
	}
	var batch []Chunk
	var acc int64
	for _, c := range chunks {
		batch = append(batch, c)
		acc += c.Size
		// Cap the chunk count too, so a schema of many zero/small chunks still
		// spreads instead of collapsing into one subtask.
		if (engineSize > 0 && acc >= engineSize) || len(batch) >= maxChunksPerSubtask {
			if err := emit(batch); err != nil {
				return nil, err
			}
			batch, acc = nil, 0
		}
	}
	if len(batch) > 0 {
		if err := emit(batch); err != nil {
			return nil, err
		}
	}
	return subtasks, nil
}

// regionsPerChunk approximates how many regions make up one ≈ chunkSize chunk.
func regionsPerChunk() int {
	return max(1, chunkSize/defaultRegionSize)
}

// subtaskCntFor targets about one subtask per node, but caps chunks per subtask
// so a schema of many small tables does not collapse into a few huge subtasks.
func subtaskCntFor(chunkCnt, nodeCnt int) int {
	if nodeCnt <= 0 {
		nodeCnt = 1
	}
	cnt := min(nodeCnt, chunkCnt)
	minCnt := (chunkCnt + maxChunksPerSubtask - 1) / maxChunksPerSubtask
	return max(cnt, minCnt, 1)
}

// physicalIDs returns the physical table ids to export: one per partition for a
// partitioned table, otherwise the table id itself.
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

// physicalTableRange returns the record-key range of one physical table. For
// int-handle tables the start must be a well-formed record key (TiKV returns
// nothing for a bare "t<id>_r" prefix start), mirroring the table reader's
// FullIntRange.
func physicalTableRange(tblInfo *model.TableInfo, pid int64) (start, end kv.Key) {
	prefix := tablecodec.GenTableRecordPrefix(pid)
	if tblInfo.IsCommonHandle {
		return prefix, prefix.PrefixNext()
	}
	return tablecodec.EncodeRowKeyWithHandle(pid, kv.IntHandle(math.MinInt64)), prefix.PrefixNext()
}

// loadRegionBoundaries returns the sorted, continuous region boundaries covering
// [start, end), clamped to the range. The result has at least two elements:
// result[0] == start and result[len-1] == end. It errors if the regions are not
// continuous (a gap would silently drop rows).
func loadRegionBoundaries(ctx context.Context, store kv.Storage, start, end kv.Key) ([]kv.Key, error) {
	hStore, ok := store.(helper.Storage)
	if !ok {
		return nil, errors.New("storage does not support region cache")
	}
	var lastErr error
	for range loadRegionMaxRetry {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		regionCache := hStore.GetRegionCache()
		regions, err := regionCache.LoadRegionsInKeyRange(
			tikv.NewBackofferWithVars(ctx, 20000, nil), start, end)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(regions) == 0 {
			lastErr = errors.New("no region loaded for key range")
			continue
		}
		sort.Slice(regions, func(i, j int) bool {
			return bytes.Compare(regions[i].StartKey(), regions[j].StartKey()) < 0
		})
		continuous := true
		for i := 1; i < len(regions); i++ {
			if !bytes.Equal(regions[i-1].EndKey(), regions[i].StartKey()) {
				continuous = false
				break
			}
		}
		if !continuous {
			lastErr = errors.New("regions are not continuous")
			continue
		}
		boundaries := make([]kv.Key, 0, len(regions)+1)
		boundaries = append(boundaries, start)
		for _, r := range regions[:len(regions)-1] {
			k := kv.Key(r.EndKey())
			if bytes.Compare(k, start) > 0 && bytes.Compare(k, end) < 0 {
				boundaries = append(boundaries, k)
			}
		}
		boundaries = append(boundaries, end)
		return boundaries, nil
	}
	return nil, lastErr
}

// groupBoundaries splits the boundary list into at most groupCnt contiguous
// groups of roughly equal region count, in key order. Each returned group has
// its span endpoints as the first and last element.
func groupBoundaries(boundaries []kv.Key, groupCnt int) [][]kv.Key {
	rangeCnt := len(boundaries) - 1
	sizes := mathutil.Divide2Batches(rangeCnt, max(groupCnt, 1))
	groups := make([][]kv.Key, 0, len(sizes))
	lo := 0
	for _, size := range sizes {
		groups = append(groups, boundaries[lo:lo+size+1])
		lo += size
	}
	return groups
}
