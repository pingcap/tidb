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

package ddl

import (
	"context"
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	kvutil "github.com/tikv/client-go/v2/util"
)

func maybeRebaseAutoIncrementIDForModifyColumn(jobCtx *jobContext, job *model.Job, tblInfo *model.TableInfo, newCol, oldCol *model.ColumnInfo) error {
	// Rebase is only needed when we are enabling AUTO_INCREMENT on an existing column.
	if mysql.HasAutoIncrementFlag(oldCol.GetFlag()) || !mysql.HasAutoIncrementFlag(newCol.GetFlag()) {
		return nil
	}

	tbl, err := getTable(jobCtx.getAutoIDRequirement(), job.SchemaID, tblInfo)
	if err != nil {
		return errors.Trace(err)
	}

	desiredNext, err := getDesiredAutoIncrementNextFromMaxColumnValue(jobCtx, job, tbl, newCol)
	if err != nil {
		return errors.Trace(err)
	}

	desiredNext, err = adjustNewBaseToNextGlobalID(nil, tbl, autoid.AutoIncrementType, desiredNext)
	if err != nil {
		return errors.Trace(err)
	}
	if mysql.HasUnsignedFlag(newCol.GetFlag()) {
		// For unsigned AUTO_INCREMENT columns, desiredNext might be represented as a negative int64 when the
		// unsigned value is larger than math.MaxInt64. Only 0 is invalid (AUTO_INCREMENT starts from 1).
		if uint64(desiredNext) == 0 {
			return errors.Trace(autoid.ErrAutoincReadFailed)
		}
	} else if desiredNext <= 0 {
		return errors.Trace(autoid.ErrAutoincReadFailed)
	}

	alloc := tbl.Allocators(nil).Get(autoid.AutoIncrementType)
	if alloc == nil {
		return errors.Errorf("auto_increment allocator not found for table %s", tblInfo.Name.O)
	}

	// The allocator's base is "last allocated value", so set it to next-1.
	if err := alloc.Rebase(context.Background(), desiredNext-1, false); err != nil {
		return errors.Trace(err)
	}
	tblInfo.AutoIncID = desiredNext
	return nil
}

func getDesiredAutoIncrementNextFromMaxColumnValue(jobCtx *jobContext, job *model.Job, tbl table.Table, colInfo *model.ColumnInfo) (int64, error) {
	jobCtxForScan := jobCtx.oldDDLCtx.jobContext(job.ID, job.ReorgMeta)
	colID := colInfo.ID
	colTps := map[int64]*types.FieldType{colID: &colInfo.FieldType}

	// When AUTO_INCREMENT is enabled on a NONCLUSTERED PRIMARY KEY column, the column value might not be stored
	// in the row value. Scan the PK index instead (O(1) per physical table via reverse seek).
	if pk := tables.FindPrimaryIndex(tbl.Meta()); pk != nil &&
		!tbl.Meta().PKIsHandle && !tbl.Meta().IsCommonHandle &&
		len(pk.Columns) == 1 && pk.Columns[0].Offset < len(tbl.Meta().Columns) &&
		tbl.Meta().Columns[pk.Columns[0].Offset].ID == colID {
		if mysql.HasUnsignedFlag(colInfo.GetFlag()) {
			maxVal, found, err := scanMaxOneColumnUint64FromIndex(jobCtxForScan, jobCtx.store, job.Priority, tbl, pk)
			if err != nil {
				return 0, errors.Trace(err)
			}
			if !found {
				return 1, nil
			}
			if maxVal == math.MaxUint64 {
				return 0, errors.Trace(autoid.ErrAutoincReadFailed)
			}
			return int64(maxVal + 1), nil
		}

		maxVal, found, err := scanMaxOneColumnInt64FromIndex(jobCtxForScan, jobCtx.store, job.Priority, tbl, pk)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if !found {
			return 1, nil
		}
		if maxVal < 0 {
			maxVal = 0
		}
		if maxVal == math.MaxInt64 {
			return 0, errors.Trace(autoid.ErrAutoincReadFailed)
		}
		return maxVal + 1, nil
	}

	if mysql.HasUnsignedFlag(colInfo.GetFlag()) {
		maxVal, err := scanMaxColumnUint64(jobCtxForScan, jobCtx.store, job.Priority, tbl, colID, colTps)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if maxVal == math.MaxUint64 {
			return 0, errors.Trace(autoid.ErrAutoincReadFailed)
		}
		return int64(maxVal + 1), nil
	}

	maxVal, err := scanMaxColumnInt64(jobCtxForScan, jobCtx.store, job.Priority, tbl, colID, colTps)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if maxVal < 0 {
		maxVal = 0
	}
	if maxVal == math.MaxInt64 {
		return 0, errors.Trace(autoid.ErrAutoincReadFailed)
	}
	return maxVal + 1, nil
}

func scanMaxColumnUint64(ctx *ReorgContext, store kv.Storage, priority int, tbl table.Table, colID int64, colTps map[int64]*types.FieldType) (uint64, error) {
	var maxVal uint64
	err := forEachPhysicalTable(tbl, func(physTbl table.PhysicalTable) error {
		return iterateSnapshotKeys(ctx, store, priority, physTbl.RecordPrefix(), kv.MaxVersion.Ver, nil, nil,
			func(_ kv.Handle, _ kv.Key, rawRow []byte) (bool, error) {
				rowMap, err := tablecodec.DecodeRowToDatumMap(rawRow, colTps, time.UTC)
				if err != nil {
					return false, errors.Trace(err)
				}
				d, ok := rowMap[colID]
				if !ok || d.IsNull() {
					return true, nil
				}
				var v uint64
				switch d.Kind() {
				case types.KindUint64:
					v = d.GetUint64()
				case types.KindInt64:
					v = uint64(d.GetInt64())
				default:
					v = uint64(d.GetInt64())
				}
				if v > maxVal {
					maxVal = v
				}
				return true, nil
			})
	})
	return maxVal, errors.Trace(err)
}

func scanMaxColumnInt64(ctx *ReorgContext, store kv.Storage, priority int, tbl table.Table, colID int64, colTps map[int64]*types.FieldType) (int64, error) {
	var maxVal int64
	var inited bool
	err := forEachPhysicalTable(tbl, func(physTbl table.PhysicalTable) error {
		return iterateSnapshotKeys(ctx, store, priority, physTbl.RecordPrefix(), kv.MaxVersion.Ver, nil, nil,
			func(_ kv.Handle, _ kv.Key, rawRow []byte) (bool, error) {
				rowMap, err := tablecodec.DecodeRowToDatumMap(rawRow, colTps, time.UTC)
				if err != nil {
					return false, errors.Trace(err)
				}
				d, ok := rowMap[colID]
				if !ok || d.IsNull() {
					return true, nil
				}
				v := d.GetInt64()
				if !inited || v > maxVal {
					maxVal = v
					inited = true
				}
				return true, nil
			})
	})
	return maxVal, errors.Trace(err)
}

func forEachPhysicalTable(t table.Table, fn func(table.PhysicalTable) error) error {
	if pt, ok := t.(table.PartitionedTable); ok {
		pi := t.Meta().GetPartitionInfo()
		if pi == nil {
			return errors.New("internal error: partitioned table without partition info")
		}
		for _, def := range pi.Definitions {
			p := pt.GetPartition(def.ID)
			if p == nil {
				return errors.Errorf("partition not found: %d", def.ID)
			}
			if err := fn(p); err != nil {
				return err
			}
		}
		return nil
	}

	physTbl, ok := t.(table.PhysicalTable)
	if !ok {
		return errors.New("internal error: table is not physical")
	}
	return fn(physTbl)
}

func newScanSnapshot(ctx *ReorgContext, store kv.Storage, priority int) kv.Snapshot {
	ver := kv.Version{Ver: kv.MaxVersion.Ver}
	snap := store.GetSnapshot(ver)
	snap.SetOption(kv.Priority, priority)
	snap.SetOption(kv.RequestSourceInternal, true)
	snap.SetOption(kv.RequestSourceType, ctx.ddlJobSourceType())
	snap.SetOption(kv.ExplicitRequestSourceType, kvutil.ExplicitTypeDDL)
	if tagger := ctx.getResourceGroupTaggerForTopSQL(); tagger != nil {
		snap.SetOption(kv.ResourceGroupTagger, tagger)
	}
	snap.SetOption(kv.ResourceGroupName, ctx.resourceGroupName)
	return snap
}

func scanMaxOneColumnInt64FromIndex(ctx *ReorgContext, store kv.Storage, priority int, tbl table.Table, idxInfo *model.IndexInfo) (int64, bool, error) {
	var maxVal int64
	var found bool
	scanOne := func(physicalID int64) error {
		v, ok, err := scanMaxOneColumnInt64FromIndexForPhysicalTable(ctx, store, priority, physicalID, idxInfo.ID)
		if err != nil {
			return err
		}
		if ok && (!found || v > maxVal) {
			maxVal = v
			found = true
		}
		return nil
	}

	if pi := tbl.Meta().GetPartitionInfo(); pi != nil {
		if idxInfo.Global {
			if err := scanOne(tbl.Meta().ID); err != nil {
				return 0, false, err
			}
		} else {
			for _, def := range pi.Definitions {
				if err := scanOne(def.ID); err != nil {
					return 0, false, err
				}
			}
		}
		return maxVal, found, nil
	}

	if err := scanOne(tbl.Meta().ID); err != nil {
		return 0, false, err
	}
	return maxVal, found, nil
}

func scanMaxOneColumnUint64FromIndex(ctx *ReorgContext, store kv.Storage, priority int, tbl table.Table, idxInfo *model.IndexInfo) (uint64, bool, error) {
	var maxVal uint64
	var found bool
	scanOne := func(physicalID int64) error {
		v, ok, err := scanMaxOneColumnUint64FromIndexForPhysicalTable(ctx, store, priority, physicalID, idxInfo.ID)
		if err != nil {
			return err
		}
		if ok && (!found || v > maxVal) {
			maxVal = v
			found = true
		}
		return nil
	}

	if pi := tbl.Meta().GetPartitionInfo(); pi != nil {
		if idxInfo.Global {
			if err := scanOne(tbl.Meta().ID); err != nil {
				return 0, false, err
			}
		} else {
			for _, def := range pi.Definitions {
				if err := scanOne(def.ID); err != nil {
					return 0, false, err
				}
			}
		}
		return maxVal, found, nil
	}

	if err := scanOne(tbl.Meta().ID); err != nil {
		return 0, false, err
	}
	return maxVal, found, nil
}

func scanMaxOneColumnInt64FromIndexForPhysicalTable(ctx *ReorgContext, store kv.Storage, priority int, physicalTableID, indexID int64) (int64, bool, error) {
	idxPrefix := tablecodec.EncodeTableIndexPrefix(physicalTableID, indexID)
	snap := newScanSnapshot(ctx, store, priority)
	it, err := snap.IterReverse(idxPrefix.PrefixNext(), idxPrefix)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	defer it.Close()
	if !it.Valid() || !it.Key().HasPrefix(idxPrefix) {
		return 0, false, nil
	}
	_, d, err := codec.DecodeOne(it.Key()[len(idxPrefix):])
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	return d.GetInt64(), true, nil
}

func scanMaxOneColumnUint64FromIndexForPhysicalTable(ctx *ReorgContext, store kv.Storage, priority int, physicalTableID, indexID int64) (uint64, bool, error) {
	idxPrefix := tablecodec.EncodeTableIndexPrefix(physicalTableID, indexID)
	snap := newScanSnapshot(ctx, store, priority)
	it, err := snap.IterReverse(idxPrefix.PrefixNext(), idxPrefix)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	defer it.Close()
	if !it.Valid() || !it.Key().HasPrefix(idxPrefix) {
		return 0, false, nil
	}
	_, d, err := codec.DecodeOne(it.Key()[len(idxPrefix):])
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	switch d.Kind() {
	case types.KindUint64:
		return d.GetUint64(), true, nil
	case types.KindInt64:
		return uint64(d.GetInt64()), true, nil
	default:
		return uint64(d.GetInt64()), true, nil
	}
}
