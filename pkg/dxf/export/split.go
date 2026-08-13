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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/store/copr"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/tikv/client-go/v2/tikv"
)

const (
	loadRegionMaxRetry = 8
	defaultRegionSize  = 96 * 1024 * 1024
	// chunkSize is the work granularity, deliberately larger than FileSize (the
	// within-chunk file-cut size), so fewer, larger chunks mean fewer partial
	// tail files.
	chunkSize = 1024 * 1024 * 1024
	// maxChunksPerSubtask caps chunks per subtask so a schema of many small
	// tables still spreads across subtasks.
	maxChunksPerSubtask = 4000
)

// splitTableSet carves every table into ~chunkSize chunks, then packs the chunks
// into size-balanced subtasks. Deterministic given the same region layout.
func splitTableSet(ctx context.Context, store kv.Storage, meta *TaskMeta, nodeCnt int) ([][]byte, error) {
	var chunks []Chunk
	var totalSize int64
	for tableIdx := range meta.Tables {
		tableChunks, err := splitTable(ctx, store, meta, tableIdx)
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

// splitTable carves one table into ~chunkSize key-ordered chunks, with a
// table-local ordinal spanning its partitions so file names stay unique.
func splitTable(ctx context.Context, store kv.Storage, meta *TaskMeta, tableIdx int) ([]Chunk, error) {
	tblInfo := meta.Tables[tableIdx].TableInfo
	var chunks []Chunk
	ordinal := 0
	for _, pid := range physicalIDs(tblInfo) {
		start, end := physicalTableRange(tblInfo, pid)
		boundaries, err := loadRegionBoundaries(ctx, store, start, end)
		if err != nil {
			return nil, err
		}
		regionCnt := len(boundaries) - 1
		totalSize := estimatePhysicalSize(ctx, store, pid, regionCnt)
		var tableChunks []Chunk
		tableChunks, ordinal = assembleChunks(tableIdx, pid, boundaries, chunkCntFor(totalSize, regionCnt), totalSize, ordinal)
		chunks = append(chunks, tableChunks...)
	}
	return chunks, nil
}

// assembleChunks groups one physical table's boundaries into chunkCnt key-ordered
// chunks, apportioning totalSize by region count and continuing the table-local
// ordinal from startOrdinal. It returns the chunks and the next ordinal.
func assembleChunks(tableIdx int, pid int64, boundaries []kv.Key, chunkCnt int, totalSize int64, startOrdinal int) ([]Chunk, int) {
	regionCnt := len(boundaries) - 1
	ord := startOrdinal
	chunks := make([]Chunk, 0, chunkCnt)
	for _, g := range groupBoundaries(boundaries, chunkCnt) {
		chunkRegions := len(g) - 1
		chunks = append(chunks, Chunk{
			TableIdx:   tableIdx,
			PhysicalID: pid,
			Start:      g[0],
			End:        g[len(g)-1],
			Size:       totalSize * int64(chunkRegions) / int64(regionCnt),
			Ordinal:    ord,
		})
		ord++
	}
	return chunks, ord
}

// estimatePhysicalSize returns a physical table's byte size from PD region stats,
// falling back to region count × defaultRegionSize when PD is unavailable.
func estimatePhysicalSize(ctx context.Context, store kv.Storage, pid int64, regionCnt int) int64 {
	if hStore, ok := store.(helper.Storage); ok {
		h := helper.NewHelper(hStore)
		if stats, err := h.GetPDRegionStats(ctx, pid, true); err == nil && stats.StorageSize > 0 {
			return stats.StorageSize * 1024 * 1024
		}
	}
	return int64(regionCnt) * defaultRegionSize
}

// chunkCntFor is one chunk per ~chunkSize of data, never more than the region count.
func chunkCntFor(totalSize int64, regionCnt int) int {
	n := int((totalSize + chunkSize - 1) / chunkSize)
	return max(1, min(n, regionCnt))
}

// packSubtasks groups chunks into subtasks in key order so each holds a similar
// amount of data, following IMPORT INTO's adjusted-engine-size approach:
// engineSize = ceil(totalSize/subtaskCnt), accumulate until a subtask reaches it.
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

// subtaskCntFor targets about one subtask per node, capped so many small tables
// do not collapse into a few huge subtasks.
func subtaskCntFor(chunkCnt, nodeCnt int) int {
	if nodeCnt <= 0 {
		nodeCnt = 1
	}
	cnt := min(nodeCnt, chunkCnt)
	minCnt := (chunkCnt + maxChunksPerSubtask - 1) / maxChunksPerSubtask
	return max(cnt, minCnt, 1)
}

// physicalIDs returns one id per partition for a partitioned table, otherwise
// the table id.
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

// loadRegionBoundaries returns the sorted region boundaries covering [start, end),
// with result[0] == start and result[len-1] == end, retrying while the regions
// are not yet continuous.
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
		regions, err := copr.LoadSortedContinuousRegions(
			tikv.NewBackofferWithVars(ctx, 20000, nil), hStore.GetRegionCache(), start, end)
		if errors.ErrorEqual(err, copr.ErrRegionsNotContinuous) {
			lastErr = err
			continue
		}
		if err != nil {
			return nil, err
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
// groups of roughly equal region count, each group's first and last element
// being its span endpoints.
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
