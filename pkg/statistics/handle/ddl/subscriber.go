// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/history"
	"github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
)

type handler struct {
	statsCache types.StatsCache
}

// NewHandlerAndRegister creates a new handler and registers it to the DDL
// notifier.
func NewHandlerAndRegister(statsCache types.StatsCache) {
	h := handler{statsCache: statsCache}
	notifier.RegisterHandler(notifier.StatsMetaHandlerID, h.handle)
}

func (h handler) handle(
	ctx context.Context,
	sctx sessionctx.Context,
	change *notifier.SchemaChangeEvent,
) error {
	switch change.GetType() {
	case model.ActionCreateTables:
		for _, info := range change.GetCreateTablesInfo() {
			ids, err := getPhysicalIDs(sctx, info)
			if err != nil {
				return err
			}
			for _, id := range ids {
				err = h.insertStats4PhysicalID(ctx, sctx, info, id)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	case model.ActionTruncateTable:
		newTableInfo, droppedTableInfo := change.GetTruncateTableInfo()
		ids, err := getPhysicalIDs(sctx, newTableInfo)
		if err != nil {
			return err
		}
		for _, id := range ids {
			err = h.insertStats4PhysicalID(ctx, sctx, newTableInfo, id)
			if err != nil {
				return errors.Trace(err)
			}
		}

		// Remove the old table stats.
		droppedIDs, err2 := getPhysicalIDs(sctx, droppedTableInfo)
		if err2 != nil {
			return err2
		}
		for _, id := range droppedIDs {
			err2 = h.delayedDeleteStats4PhysicalID(ctx, sctx, id)
			if err2 != nil {
				return errors.Trace(err2)
			}
		}
	case model.ActionDropTable:
		droppedTableInfo := change.GetDropTableInfo()
		ids, err := getPhysicalIDs(sctx, droppedTableInfo)
		if err != nil {
			return err
		}
		for _, id := range ids {
			err = h.delayedDeleteStats4PhysicalID(ctx, sctx, id)
			if err != nil {
				return errors.Trace(err)
			}
		}
	case model.ActionAddColumn:
		newTableInfo, newColumnInfo := change.GetAddColumnInfo()
		ids, err := getPhysicalIDs(sctx, newTableInfo)
		if err != nil {
			return errors.Trace(err)
		}
		for _, id := range ids {
			if err = h.insertStats4Col(ctx, sctx, id, newColumnInfo); err != nil {
				return errors.Trace(err)
			}
		}
	case model.ActionModifyColumn:
		newTableInfo, modifiedColumnInfo := change.GetModifyColumnInfo()
		ids, err := getPhysicalIDs(sctx, newTableInfo)
		if err != nil {
			return errors.Trace(err)
		}
		for _, id := range ids {
			if err = h.insertStats4Col(ctx, sctx, id, modifiedColumnInfo); err != nil {
				return errors.Trace(err)
			}
		}
	case model.ActionAddTablePartition:
		globalTableInfo, addedPartitionInfo := change.GetAddPartitionInfo()
		for _, def := range addedPartitionInfo.Definitions {
			if err := h.insertStats4PhysicalID(ctx, sctx, globalTableInfo, def.ID); err != nil {
				return errors.Trace(err)
			}
		}
	case model.ActionTruncateTablePartition:
		globalTableInfo, addedPartInfo, droppedPartInfo := change.GetTruncatePartitionInfo()
		// First, add the new stats meta record for the new partitions.
		for _, def := range addedPartInfo.Definitions {
			if err := h.insertStats4PhysicalID(ctx, sctx, globalTableInfo, def.ID); err != nil {
				return errors.Trace(err)
			}
		}

		// Second, clean up the old stats meta from global stats meta for the dropped partitions.
		if err := updateGlobalTableStats4TruncatePartition(
			ctx,
			sctx,
			globalTableInfo,
			droppedPartInfo,
		); err != nil {
			return errors.Trace(err)
		}

		// Third, clean up the old stats meta from partition stats meta for the dropped partitions.
		for _, def := range droppedPartInfo.Definitions {
			if err := h.delayedDeleteStats4PhysicalID(ctx, sctx, def.ID); err != nil {
				return errors.Trace(err)
			}
		}

		return nil
	case model.ActionDropTablePartition:
		globalTableInfo, droppedPartitionInfo := change.GetDropPartitionInfo()
		if err := updateGlobalTableStats4DropPartition(
			ctx,
			sctx,
			globalTableInfo,
			droppedPartitionInfo,
		); err != nil {
			return errors.Trace(err)
		}

		// Reset the partition stats.
		for _, def := range droppedPartitionInfo.Definitions {
			if err := h.delayedDeleteStats4PhysicalID(ctx, sctx, def.ID); err != nil {
				return errors.Trace(err)
			}
		}

		return nil
	case model.ActionExchangeTablePartition:
		globalTableInfo, originalPartInfo, originalTableInfo := change.GetExchangePartitionInfo()
		return errors.Trace(updateGlobalTableStats4ExchangePartition(
			ctx,
			sctx,
			globalTableInfo,
			originalPartInfo,
			originalTableInfo,
		))
	case model.ActionReorganizePartition:
		globalTableInfo, addedPartInfo, droppedPartitionInfo := change.GetReorganizePartitionInfo()
		// Avoid updating global stats as the data remains unchanged.
		// For new partitions, it's crucial to correctly insert the count and modify count correctly.
		// However, this is challenging due to the need to know the count of the new partitions.
		// Given that a partition can be split into two, determining the count of the new partitions is so hard.
		// It's acceptable to not update it immediately,
		// as the new partitions will be analyzed shortly due to the absence of statistics for them.
		// Therefore, the auto-analyze worker will handle them in the near future.
		for _, def := range addedPartInfo.Definitions {
			if err := h.insertStats4PhysicalID(ctx, sctx, globalTableInfo, def.ID); err != nil {
				return err
			}
		}

		// Reset the partition stats.
		for _, def := range droppedPartitionInfo.Definitions {
			if err := h.delayedDeleteStats4PhysicalID(ctx, sctx, def.ID); err != nil {
				return err
			}
		}

		return nil
	case model.ActionAlterTablePartitioning:
		oldSingleTableID, globalTableInfo, addedPartInfo := change.GetAddPartitioningInfo()
		// Add new partition stats.
		for _, def := range addedPartInfo.Definitions {
			if err := h.insertStats4PhysicalID(ctx, sctx, globalTableInfo, def.ID); err != nil {
				return errors.Trace(err)
			}
		}
		// Change id for global stats, since the data has not changed!
		// Note: This operation will update all tables related to statistics with the new ID.
		return errors.Trace(storage.ChangeGlobalStatsID(ctx, sctx, oldSingleTableID, globalTableInfo.ID))
	case model.ActionRemovePartitioning:
		// Change id for global stats, since the data has not changed!
		// Note: This operation will update all tables related to statistics with the new ID.
		oldTblID, newSingleTableInfo, droppedPartInfo := change.GetRemovePartitioningInfo()
		if err := storage.ChangeGlobalStatsID(ctx, sctx, oldTblID, newSingleTableInfo.ID); err != nil {
			return errors.Trace(err)
		}

		// Remove partition stats.
		for _, def := range droppedPartInfo.Definitions {
			if err := h.delayedDeleteStats4PhysicalID(ctx, sctx, def.ID); err != nil {
				return errors.Trace(err)
			}
		}
	case model.ActionFlashbackCluster:
		return errors.Trace(storage.UpdateStatsVersion(ctx, sctx))
	default:
		intest.Assert(false)
		logutil.StatsLogger().Error("Unhandled schema change event",
			zap.Stringer("type", change))
	}
	return nil
}

func (h handler) insertStats4PhysicalID(
	ctx context.Context,
	sctx sessionctx.Context,
	info *model.TableInfo,
	id int64,
) error {
	startTS, err := storage.InsertTableStats2KV(ctx, sctx, info, id)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(h.recordHistoricalStatsMeta(ctx, sctx, id, startTS))
}

func (h handler) recordHistoricalStatsMeta(
	ctx context.Context,
	sctx sessionctx.Context,
	id int64,
	startTS uint64,
) error {
	if startTS == 0 {
		return nil
	}
	enableHistoricalStats, err2 := getEnableHistoricalStats(sctx)
	if err2 != nil {
		return err2
	}
	if !enableHistoricalStats {
		return nil
	}

	tbl, ok := h.statsCache.Get(id)
	if !ok || !tbl.IsInitialized() {
		return nil
	}

	return history.RecordHistoricalStatsMeta(
		ctx,
		sctx,
		id,
		startTS,
		util.StatsMetaHistorySourceSchemaChange,
	)
}

func (h handler) delayedDeleteStats4PhysicalID(
	ctx context.Context,
	sctx sessionctx.Context,
	id int64,
) error {
	startTS, err2 := storage.UpdateStatsMetaVersionForGC(ctx, sctx, id)
	if err2 != nil {
		return errors.Trace(err2)
	}
	return errors.Trace(h.recordHistoricalStatsMeta(ctx, sctx, id, startTS))
}

func (h handler) insertStats4Col(
	ctx context.Context,
	sctx sessionctx.Context,
	physicalID int64,
	colInfos []*model.ColumnInfo,
) error {
	startTS, err := storage.InsertColStats2KV(ctx, sctx, physicalID, colInfos)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(h.recordHistoricalStatsMeta(ctx, sctx, physicalID, startTS))
}

func getPhysicalIDs(
	sctx sessionctx.Context,
	tblInfo *model.TableInfo,
) (ids []int64, err error) {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		return []int64{tblInfo.ID}, nil
	}
	ids = make([]int64, 0, len(pi.Definitions)+1)
	for _, def := range pi.Definitions {
		ids = append(ids, def.ID)
	}
	pruneMode, err := getCurrentPruneMode(sctx)
	if err != nil {
		return nil, err
	}
	if pruneMode == variable.Dynamic {
		ids = append(ids, tblInfo.ID)
	}
	return ids, nil
}

func getCurrentPruneMode(
	sctx sessionctx.Context,
) (variable.PartitionPruneMode, error) {
	pruneMode, err := sctx.GetSessionVars().
		GlobalVarsAccessor.
		GetGlobalSysVar(variable.TiDBPartitionPruneMode)
	return variable.PartitionPruneMode(pruneMode), errors.Trace(err)
}

func getEnableHistoricalStats(
	sctx sessionctx.Context,
) (bool, error) {
	val, err := sctx.GetSessionVars().
		GlobalVarsAccessor.
		GetGlobalSysVar(variable.TiDBEnableHistoricalStats)
	return variable.TiDBOptOn(val), errors.Trace(err)
}
