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
	// maxRegionsPerSubtask caps the span size when auto-splitting.
	maxRegionsPerSubtask = 4000
)

// splitTableSet builds the Dump-step subtasks for the whole table set. It
// iterates the tables in order and region-splits each into key-ordered spans;
// each span becomes one subtask with a single unit, stamped with a table-local
// NameOrdinal. The sort + continuity check runs per table (cross-table key gaps
// are expected). The result is deterministic given the same region layout.
//
// The SubtaskMeta.Units list already supports packing several whole small tables
// into one subtask; that packing is added by a later increment and does not
// change this meta shape.
func splitTableSet(ctx context.Context, store kv.Storage, meta *TaskMeta, nodeCnt int) ([][]byte, error) {
	var subtasks [][]byte
	for tableIdx := range meta.Tables {
		tblInfo := meta.Tables[tableIdx].TableInfo
		// nameOrdinal is table-local and runs across all of the table's spans
		// (including partitions), so file names never collide within a table.
		nameOrdinal := 0
		for _, pid := range physicalIDs(tblInfo) {
			start, end := physicalTableRange(tblInfo, pid)
			boundaries, err := loadRegionBoundaries(ctx, store, start, end)
			if err != nil {
				return nil, err
			}
			regionCnt := len(boundaries) - 1
			groups := groupBoundaries(boundaries, spanCntFor(regionCnt, nodeCnt, meta.SubtaskRegions))
			var units []Unit
			units, nameOrdinal = spansToUnits(tableIdx, pid, groups, nameOrdinal)
			for i := range units {
				bs, err := json.Marshal(&SubtaskMeta{Units: units[i : i+1]})
				if err != nil {
					return nil, errors.Trace(err)
				}
				subtasks = append(subtasks, bs)
			}
		}
	}
	return subtasks, nil
}

// spansToUnits turns one physical table's grouped boundaries into span units in
// key order, continuing the table-local name ordinal from startOrdinal, and
// returns the next ordinal. Each group's first and last boundary are the unit's
// [Start, End).
func spansToUnits(tableIdx int, pid int64, groups [][]kv.Key, startOrdinal int) ([]Unit, int) {
	units := make([]Unit, 0, len(groups))
	ord := startOrdinal
	for _, g := range groups {
		units = append(units, Unit{
			TableIdx:    tableIdx,
			PhysicalID:  pid,
			Start:       g[0],
			End:         g[len(g)-1],
			NameOrdinal: ord,
		})
		ord++
	}
	return units, ord
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

// spanCntFor decides how many spans to emit for one physical table. The region
// batch mirrors add-index's CalculateRegionBatch (cloud branch):
// batch = min(maxRegionsPerSubtask, ceil(regionCnt/nodeCnt)).
func spanCntFor(regionCnt, nodeCnt, subtaskRegions int) int {
	batch := subtaskRegions
	if batch <= 0 {
		if nodeCnt <= 0 {
			nodeCnt = 1
		}
		avgTasksPerNode := (regionCnt + nodeCnt - 1) / nodeCnt
		batch = min(maxRegionsPerSubtask, avgTasksPerNode)
	}
	batch = max(batch, 1)
	return max(1, (regionCnt+batch-1)/batch)
}
