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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/store/copr"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/tikv/client-go/v2/tikv"
)

const (
	loadRegionMaxRetry    = 8
	scanRegionBackoffBase = 200 * time.Millisecond
	scanRegionBackoffMax  = 2 * time.Second
	// chunkSize is the per-chunk work granularity, kept well above FileSize (the
	// within-chunk file-cut size) so each chunk leaves at most one partial tail
	// file: fewer, larger chunks mean fewer partial files.
	chunkSize = 10 * 1024 * 1024 * 1024
	// chunksPerWorker is how many chunks a subtask holds per worker slot, so a
	// subtask's worker pool has spare chunks to steal and stragglers even out.
	chunksPerWorker = 2
	// maxChunksPerSubtask is a hard ceiling on the chunk count per subtask (the
	// usual bound is the byte size in packSubtasks); it only bites for a schema of
	// many tiny tables, keeping a subtask's chunk list and its meta bounded.
	maxChunksPerSubtask = 4000
)

// splitTables carves every table into ~chunkSize chunks, then packs the chunks
// into subtasks sized to the worker concurrency. Deterministic given the same
// region layout.
func splitTables(ctx context.Context, store kv.Storage, meta *TaskMeta, concurrency int) ([][]byte, error) {
	var chunks []Chunk
	for tableIdx := range meta.Tables {
		tableChunks, err := splitTable(ctx, store, meta, tableIdx)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, tableChunks...)
	}
	return packSubtasks(chunks, concurrency)
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
		var tableChunks []Chunk
		tableChunks, ordinal = buildChunks(tableIdx, pid, boundaries, meta.PhysicalSizes[pid], ordinal)
		chunks = append(chunks, tableChunks...)
	}
	return chunks, nil
}

// buildChunks groups one physical table's boundaries into key-ordered chunks, one
// chunk per ~chunkSize of data (never more than the region count), apportioning
// totalSize by region count and continuing the table-local ordinal from
// startOrdinal. It returns the chunks and the next ordinal.
func buildChunks(tableIdx int, pid int64, boundaries []kv.Key, totalSize int64, startOrdinal int) ([]Chunk, int) {
	regionCnt := len(boundaries) - 1
	chunkCnt := max(1, min(int((totalSize+chunkSize-1)/chunkSize), regionCnt))
	sizes := mathutil.Divide2Batches(regionCnt, chunkCnt)
	chunks := make([]Chunk, 0, len(sizes))
	ord, lo := startOrdinal, 0
	for _, n := range sizes {
		chunks = append(chunks, Chunk{
			TableIdx:   tableIdx,
			PhysicalID: pid,
			Start:      boundaries[lo],
			End:        boundaries[lo+n],
			Size:       totalSize * int64(n) / int64(regionCnt),
			Ordinal:    ord,
		})
		lo += n
		ord++
	}
	return chunks, ord
}

// packSubtasks groups chunks into key-ordered subtasks, cutting a new subtask
// once it reaches chunksPerWorker*concurrency chunks worth of bytes (so a subtask
// fills a node's worker pool a few times over yet keeps the failover redo cost
// bounded), or the maxChunksPerSubtask ceiling.
func packSubtasks(chunks []Chunk, concurrency int) ([][]byte, error) {
	if len(chunks) == 0 {
		return nil, nil
	}
	maxSubtaskSize := int64(chunksPerWorker*max(concurrency, 1)) * chunkSize

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
		if acc >= maxSubtaskSize || len(batch) >= maxChunksPerSubtask {
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

// loadRegionBoundaries returns the sorted boundaries spanning [start, end] (both
// included), retrying with backoff while the regions are not continuous.
func loadRegionBoundaries(ctx context.Context, store kv.Storage, start, end kv.Key) ([]kv.Key, error) {
	hStore, ok := store.(helper.Storage)
	if !ok {
		return nil, errors.New("storage does not support region cache")
	}
	var boundaries []kv.Key
	backoffer := backoff.NewExponential(scanRegionBackoffBase, 2, scanRegionBackoffMax)
	err := handle.RunWithRetry(ctx, loadRegionMaxRetry, backoffer, logutil.BgLogger(), func(context.Context) (bool, error) {
		regions, err := copr.LoadSortedContinuousRegions(
			tikv.NewBackofferWithVars(ctx, 20000, nil), hStore.GetRegionCache(), start, end)
		if errors.ErrorEqual(err, copr.ErrRegionsNotContinuous) {
			return true, err
		}
		if err != nil {
			return false, err
		}
		boundaries = make([]kv.Key, 0, len(regions)+1)
		boundaries = append(boundaries, start)
		for _, r := range regions[:len(regions)-1] {
			k := kv.Key(r.EndKey())
			if bytes.Compare(k, start) > 0 && bytes.Compare(k, end) < 0 {
				boundaries = append(boundaries, k)
			}
		}
		boundaries = append(boundaries, end)
		return false, nil
	})
	return boundaries, err
}
