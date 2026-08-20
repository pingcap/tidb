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
	"context"
	"math"
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
	chunks := make([]Chunk, 0, meta.tableCount())
	var total int64
	tableIdx := 0
	for i := range meta.DBs {
		for _, tid := range meta.DBs[i].TableIDs {
			tableChunks, err := splitTable(ctx, store, tableInfos, tid, tableIdx)
			if err != nil {
				return nil, 0, err
			}
			for _, chunk := range tableChunks {
				total += chunk.Size
			}
			chunks = append(chunks, tableChunks...)
			tableIdx++
		}
	}
	return chunks, total, nil
}

func tableInfoByID(tableInfos map[int64]*model.TableInfo, id int64) (*model.TableInfo, error) {
	tblInfo, ok := tableInfos[id]
	if !ok {
		return nil, errors.Errorf("export: table %d not found in snapshot infoschema", id)
	}
	return tblInfo, nil
}

// splitTable carves one table into ~chunkSize key-ordered chunks, with a
// table-local ordinal spanning its partitions so file names stay unique.
func splitTable(
	ctx context.Context,
	store kv.Storage,
	tableInfos map[int64]*model.TableInfo,
	tableID int64,
	tableIdx int,
) ([]Chunk, error) {
	tblInfo, err := tableInfoByID(tableInfos, tableID)
	if err != nil {
		return nil, err
	}
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
	budget := max(int64(1), (total+count-1)/count)

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
