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
	"math"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/store/copr"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
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

// generateSubtasks carves the task's tables into chunks and groups them into subtasks.
func generateSubtasks(ctx context.Context, store kv.Storage, is infoschema.InfoSchema, meta *TaskMeta, nodeCount int) ([][]Chunk, error) {
	chunks := make([]Chunk, 0, len(meta.Tables))
	for tableIdx := range meta.Tables {
		tableChunks, err := splitTable(ctx, store, is, meta, tableIdx)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, tableChunks...)
	}
	return divideSubtasks(chunks, nodeCount), nil
}

func tableInfoByID(ctx context.Context, is infoschema.InfoSchema, id int64) (*model.TableInfo, error) {
	tbl, ok := is.TableByID(ctx, id)
	if !ok {
		return nil, errors.Errorf("export: table %d not found in snapshot infoschema", id)
	}
	return tbl.Meta(), nil
}

// estimateExportSize returns each physical table's estimated byte size and the
// total across the whole export task, from PD. It is the prepare-step data-volume
// estimate that sizes the task's resources and seeds the split fallback.
func estimateExportSize(ctx context.Context, store kv.Storage, is infoschema.InfoSchema, meta *TaskMeta) (map[int64]int64, int64, error) {
	hStore, ok := store.(helper.Storage)
	if !ok {
		return nil, 0, errors.New("storage does not support region cache")
	}
	h := helper.NewHelper(hStore)
	pdCli, err := h.TryGetPDHTTPClient()
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	sizes := make(map[int64]int64)
	var total int64
	for i := range meta.Tables {
		tblInfo, err := tableInfoByID(ctx, is, meta.Tables[i].TableID)
		if err != nil {
			return nil, 0, err
		}
		for _, pid := range physicalIDs(tblInfo) {
			start, end := physicalTableRange(tblInfo, pid)
			var size int64
			backoffer := backoff.NewExponential(scanRegionBackoffBase, 2, scanRegionBackoffMax)
			err = handle.RunWithRetry(ctx, loadRegionMaxRetry, backoffer, logutil.BgLogger(), func(context.Context) (bool, error) {
				size, err = h.EstimateKeyRangeSize(ctx, pdCli, start, end)
				return err != nil, err
			})
			if err != nil {
				return nil, 0, errors.Trace(err)
			}
			sizes[pid] = size
			total += size
		}
	}
	return sizes, total, nil
}

// splitTable carves one table into ~chunkSize key-ordered chunks, with a
// table-local ordinal spanning its partitions so file names stay unique.
func splitTable(ctx context.Context, store kv.Storage, is infoschema.InfoSchema, meta *TaskMeta, tableIdx int) ([]Chunk, error) {
	tblInfo, err := tableInfoByID(ctx, is, meta.Tables[tableIdx].TableID)
	if err != nil {
		return nil, err
	}
	pids := physicalIDs(tblInfo)
	chunks := make([]Chunk, 0, len(pids))
	ordinal := 0
	for _, pid := range pids {
		start, end := physicalTableRange(tblInfo, pid)
		var tableChunks []Chunk
		if endKeys, sizes, ok := loadRegionSizes(ctx, store, start, end); ok {
			tableChunks, ordinal = chunksBySize(tableIdx, pid, start, end, endKeys, sizes, ordinal)
		} else {
			boundaries, err := loadRegionBoundaries(ctx, store, start, end)
			if err != nil {
				return nil, err
			}
			tableChunks, ordinal = chunksByCount(tableIdx, pid, boundaries, meta.PhysicalSizes[pid], ordinal)
		}
		chunks = append(chunks, tableChunks...)
	}
	return chunks, nil
}

// loadRegionSizes returns each region's end key and byte size over [start, end)
// from a fresh PD estimate with best effort.
func loadRegionSizes(ctx context.Context, store kv.Storage, start, end kv.Key) (endKeys []kv.Key, sizes []int64, ok bool) {
	hStore, ok := store.(helper.Storage)
	if !ok {
		return nil, nil, false
	}
	h := helper.NewHelper(hStore)
	var err error
	backoffer := backoff.NewExponential(scanRegionBackoffBase, 2, scanRegionBackoffMax)
	err = handle.RunWithRetry(ctx, loadRegionMaxRetry, backoffer, logutil.BgLogger(), func(context.Context) (bool, error) {
		endKeys, sizes, err = h.RegionApproximateSizes(ctx, start, end)
		return err != nil, err
	})
	if err != nil {
		logutil.BgLogger().Warn("export: per-region size estimate failed, using region-count split", zap.Error(err))
		return nil, nil, false
	}
	if len(sizes) == 0 {
		return nil, nil, false
	}
	return endKeys, sizes, true
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

// chunksByCount is the fallback when per-region sizes are unavailable: equal
// region-count groups, apportioning totalSize by region count.
func chunksByCount(tableIdx int, pid int64, boundaries []kv.Key, totalSize int64, startOrdinal int) ([]Chunk, int) {
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

// divideSubtasks packs chunks into subtasks. The subtask count is estimated from
// subtaskSize and rounded up to a multiple of nodeCount so the framework can
// spread them evenly across nodes; chunks are then packed to the uniform budget.
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
	if n := int64(max(nodeCount, 1)); n > 1 {
		count = (count + n - 1) / n * n
	}
	budget := (total + count - 1) / count

	subtasks := make([][]Chunk, 0, count)
	batch := make([]Chunk, 0, budget/chunkSize+1)
	var acc int64
	for _, c := range chunks {
		batch = append(batch, c)
		acc += c.Size
		if acc >= budget {
			subtasks = append(subtasks, batch)
			batch, acc = make([]Chunk, 0, budget/chunkSize+1), 0
		}
	}
	if len(batch) > 0 {
		subtasks = append(subtasks, batch)
	}
	return subtasks
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
