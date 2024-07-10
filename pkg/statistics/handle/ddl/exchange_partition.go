// Copyright 2023 PingCAP, Inc.
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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"go.uber.org/zap"
)

func (h *ddlHandlerImpl) onExchangeAPartition(t *util.DDLEvent) error {
	globalTableInfo, originalPartInfo,
		originalTableInfo := t.GetExchangePartitionInfo()
	// Note: Put all the operations in a transaction.
	if err := util.CallWithSCtx(h.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		partCount, partModifyCount, tableCount, tableModifyCount, err := getCountsAndModifyCounts(
			sctx,
			originalPartInfo.Definitions[0].ID,
			originalTableInfo.ID,
		)
		if err != nil {
			return err
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

		// Update the global stats.
		if modifyCountDelta != 0 || countDelta != 0 {
			is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
			globalTableSchema, ok := infoschema.SchemaByTable(is, globalTableInfo)
			if !ok {
				return errors.Errorf("schema not found for table %s", globalTableInfo.Name.O)
			}
			if err := updateStatsWithCountDeltaAndModifyCountDelta(
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
				return err
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
		}
		return nil
	}, util.FlagWrapTxn); err != nil {
		return err
	}

	return nil
}

func getCountsAndModifyCounts(
	sctx sessionctx.Context,
	partitionID, tableID int64,
) (partCount, partModifyCount, tableCount, tableModifyCount int64, err error) {
	partCount, partModifyCount, _, err = storage.StatsMetaCountAndModifyCount(sctx, partitionID)
	if err != nil {
		return
	}

	tableCount, tableModifyCount, _, err = storage.StatsMetaCountAndModifyCount(sctx, tableID)
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
