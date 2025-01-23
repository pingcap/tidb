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
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/history"
	"github.com/pingcap/tidb/pkg/statistics/handle/lockstats"
	"github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
)

type subscriber struct {
	statsCache types.StatsCache
}

// newSubscriber creates a new subscriber.
func newSubscriber(
	statsCache types.StatsCache,
) *subscriber {
	h := subscriber{statsCache: statsCache}
	return &h
}

func (h subscriber) handle(
	ctx context.Context,
	sctx sessionctx.Context,
	change *notifier.SchemaChangeEvent,
) error {
	switch change.GetType() {
	case model.ActionCreateTable:
		info := change.GetCreateTableInfo()
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
	// EXCHANGE PARTITION EVENT NOTES:
	//  1. When a partition is exchanged with a system table, we need to adjust the global statistics
	//     based on the count delta and modify count delta. However, due to the involvement of the system table,
	//     a complete update of the global statistics is not feasible. Therefore, we bypass the statistics update
	//     for the table in this scenario. Despite this, the table id still changes, so the statistics for the
	//     system table will still be visible.
	//  2. If the system table is a partitioned table, we will update the global statistics for the partitioned table.
	//     It is rare to exchange a partition from a system table, so we can ignore this case. In this case,
	//     the system table will have statistics, but this is not a significant issue.
	// So we decided to completely ignore the system table event.
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
	case model.ActionAddIndex:
		// No need to update the stats meta for the adding index event.
	case model.ActionDropSchema:
		miniDBInfo := change.GetDropSchemaInfo()
		intest.Assert(miniDBInfo != nil)
		for _, table := range miniDBInfo.Tables {
			// Try best effort to update the stats meta version for gc.
			if err := h.delayedDeleteStats4PhysicalID(ctx, sctx, table.ID); err != nil {
				logutil.StatsLogger().Error(
					"Failed to update stats meta version for gc",
					zap.Int64("tableID", table.ID),
					zap.Error(err),
				)
			}
		}
	default:
		intest.Assert(false)
		logutil.StatsLogger().Error("Unhandled schema change event",
			zap.Stringer("type", change))
	}
	return nil
}

func (h subscriber) insertStats4PhysicalID(
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

func (h subscriber) recordHistoricalStatsMeta(
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
		startTS,
		util.StatsMetaHistorySourceSchemaChange,
		id,
	)
}

func (h subscriber) delayedDeleteStats4PhysicalID(
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

func (h subscriber) insertStats4Col(
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

func updateGlobalTableStats4DropPartition(
	ctx context.Context,
	sctx sessionctx.Context,
	globalTableInfo *model.TableInfo,
	droppedPartitionInfo *model.PartitionInfo,
) error {
	count := int64(0)
	for _, def := range droppedPartitionInfo.Definitions {
		// Get the count and modify count of the partition.
		tableCount, _, _, err := storage.StatsMetaCountAndModifyCount(ctx, sctx, def.ID)
		if err != nil {
			return err
		}
		count += tableCount
	}
	if count == 0 {
		return nil
	}

	lockedTables, err := lockstats.QueryLockedTables(ctx, sctx)
	if err != nil {
		return errors.Trace(err)
	}
	isLocked := false
	if _, ok := lockedTables[globalTableInfo.ID]; ok {
		isLocked = true
	}
	startTS, err := util.GetStartTS(sctx)
	if err != nil {
		return errors.Trace(err)
	}

	// Because we drop the partition, we should subtract the count from the global stats.
	delta := -count
	return errors.Trace(storage.UpdateStatsMeta(
		ctx,
		sctx,
		startTS,
		storage.NewDeltaUpdate(globalTableInfo.ID, variable.TableDelta{Count: count, Delta: delta}, isLocked),
	))
}

func updateGlobalTableStats4ExchangePartition(
	ctx context.Context,
	sctx sessionctx.Context,
	globalTableInfo *model.TableInfo,
	originalPartInfo *model.PartitionInfo,
	originalTableInfo *model.TableInfo,
) error {
	partCount, partModifyCount, tableCount, tableModifyCount, err := getCountsAndModifyCounts(
		ctx,
		sctx,
		originalPartInfo.Definitions[0].ID,
		originalTableInfo.ID,
	)
	if err != nil {
		return errors.Trace(err)
	}

	// The count of the partition should be added to the table.
	// The formula is: total_count = original_table_count - original_partition_count + new_table_count.
	// So the delta is : new_table_count - original_partition_count.
	countDelta := tableCount - partCount
	// Initially, the sum of tableCount and partCount represents
	// the operation of deleting the partition and adding the table.
	// Therefore, they are considered as modifyCountDelta.
	// Next, since the old partition no longer belongs to the table,
	// the modify count of the partition should be subtracted.
	// The modify count of the table should be added as we are adding the table as a partition.
	modifyCountDelta := (tableCount + partCount) - partModifyCount + tableModifyCount

	if modifyCountDelta == 0 && countDelta == 0 {
		return nil
	}

	// Update the global stats.
	is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	globalTableSchema, ok := infoschema.SchemaByTable(is, globalTableInfo)
	if !ok {
		return errors.Errorf("schema not found for table %s", globalTableInfo.Name.O)
	}
	if err = updateStatsWithCountDeltaAndModifyCountDelta(
		ctx,
		sctx,
		globalTableInfo.ID, countDelta, modifyCountDelta,
	); err != nil {
		fields := exchangePartitionLogFields(
			globalTableSchema.Name.O,
			globalTableInfo,
			originalPartInfo.Definitions[0],
			originalTableInfo,
			countDelta, modifyCountDelta,
			partCount,
			partModifyCount,
			tableCount,
			tableModifyCount,
		)
		fields = append(fields, zap.Error(err))
		logutil.StatsLogger().Error(
			"Update global stats after exchange partition failed",
			fields...,
		)
		return errors.Trace(err)
	}
	logutil.StatsLogger().Info(
		"Update global stats after exchange partition",
		exchangePartitionLogFields(
			globalTableSchema.Name.O,
			globalTableInfo,
			originalPartInfo.Definitions[0],
			originalTableInfo,
			countDelta, modifyCountDelta,
			partCount,
			partModifyCount,
			tableCount,
			tableModifyCount,
		)...,
	)
	return nil
}

func getCountsAndModifyCounts(
	ctx context.Context,
	sctx sessionctx.Context,
	partitionID, tableID int64,
) (partCount, partModifyCount, tableCount, tableModifyCount int64, err error) {
	partCount, partModifyCount, _, err = storage.StatsMetaCountAndModifyCount(ctx, sctx, partitionID)
	if err != nil {
		return
	}

	tableCount, tableModifyCount, _, err = storage.StatsMetaCountAndModifyCount(ctx, sctx, tableID)
	if err != nil {
		return
	}

	return
}

func exchangePartitionLogFields(
	globalTableSchemaName string,
	globalTableInfo *model.TableInfo,
	originalPartDef model.PartitionDefinition,
	originalTableInfo *model.TableInfo,
	countDelta, modifyCountDelta,
	partCount, partModifyCount,
	tableCount, tableModifyCount int64,
) []zap.Field {
	return []zap.Field{
		zap.String("globalTableSchema", globalTableSchemaName),
		zap.Int64("globalTableID", globalTableInfo.ID),
		zap.String("globalTableName", globalTableInfo.Name.O),
		zap.Int64("countDelta", countDelta),
		zap.Int64("modifyCountDelta", modifyCountDelta),
		zap.Int64("partitionID", originalPartDef.ID),
		zap.String("partitionName", originalPartDef.Name.O),
		zap.Int64("partitionCount", partCount),
		zap.Int64("partitionModifyCount", partModifyCount),
		zap.Int64("tableID", originalTableInfo.ID),
		zap.String("tableName", originalTableInfo.Name.O),
		zap.Int64("tableCount", tableCount),
		zap.Int64("tableModifyCount", tableModifyCount),
	}
}

func updateGlobalTableStats4TruncatePartition(
	ctx context.Context,
	sctx sessionctx.Context,
	globalTableInfo *model.TableInfo,
	droppedPartInfo *model.PartitionInfo,
) error {
	count := int64(0)
	partitionIDs := make([]int64, 0, len(droppedPartInfo.Definitions))
	partitionNames := make([]string, 0, len(droppedPartInfo.Definitions))
	for _, def := range droppedPartInfo.Definitions {
		// Get the count and modify count of the partition.
		tableCount, _, _, err := storage.StatsMetaCountAndModifyCount(ctx, sctx, def.ID)
		if err != nil {
			return errors.Trace(err)
		}
		count += tableCount
		partitionIDs = append(partitionIDs, def.ID)
		partitionNames = append(partitionNames, def.Name.O)
	}

	if count == 0 {
		return nil
	}

	is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	globalTableSchema, ok := infoschema.SchemaByTable(is, globalTableInfo)
	if !ok {
		return errors.Errorf("schema not found for table %s", globalTableInfo.Name.O)
	}
	lockedTables, err := lockstats.QueryLockedTables(ctx, sctx)
	if err != nil {
		return errors.Trace(err)
	}
	isLocked := false
	if _, ok := lockedTables[globalTableInfo.ID]; ok {
		isLocked = true
	}
	startTS, err := util.GetStartTS(sctx)
	if err != nil {
		return errors.Trace(err)
	}

	// Because we drop the partition, we should subtract the count from the global stats.
	// Note: We don't need to subtract the modify count from the global stats.
	// For example:
	// 1. The partition has 100 rows.
	// 2. We deleted 100 rows from the partition.
	// 3. The global stats has `count - 100 rows` and 100 modify count.
	// 4. We drop the partition.
	// 5. The global stats should not be `count` and 0 modify count. We need to keep the modify count.
	delta := -count
	err = storage.UpdateStatsMeta(
		ctx,
		sctx,
		startTS,
		storage.NewDeltaUpdate(globalTableInfo.ID, variable.TableDelta{Count: count, Delta: delta}, isLocked),
	)
	if err != nil {
		fields := truncatePartitionsLogFields(
			globalTableSchema,
			globalTableInfo,
			partitionIDs,
			partitionNames,
			count,
			delta,
			startTS,
			isLocked,
		)
		fields = append(fields, zap.Error(err))
		logutil.StatsLogger().Error("Update global stats after truncate partition failed",
			fields...,
		)
		return errors.Trace(err)
	}

	logutil.StatsLogger().Info("Update global stats after truncate partition",
		truncatePartitionsLogFields(
			globalTableSchema,
			globalTableInfo,
			partitionIDs,
			partitionNames,
			count,
			delta,
			startTS,
			isLocked,
		)...,
	)
	return nil
}

func truncatePartitionsLogFields(
	globalTableSchema *model.DBInfo,
	globalTableInfo *model.TableInfo,
	partitionIDs []int64,
	partitionNames []string,
	count int64,
	delta int64,
	startTS uint64,
	isLocked bool,
) []zap.Field {
	return []zap.Field{
		zap.String("schema", globalTableSchema.Name.O),
		zap.Int64("tableID", globalTableInfo.ID),
		zap.String("tableName", globalTableInfo.Name.O),
		zap.Int64s("partitionIDs", partitionIDs),
		zap.Strings("partitionNames", partitionNames),
		zap.Int64("count", count),
		zap.Int64("delta", delta),
		zap.Uint64("startTS", startTS),
		zap.Bool("isLocked", isLocked),
	}
}
