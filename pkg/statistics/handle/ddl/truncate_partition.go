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
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/lockstats"
	"github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"go.uber.org/zap"
)

func (h *ddlHandlerImpl) onTruncatePartitions(t *util.DDLEvent) error {
	globalTableInfo, addedPartInfo, droppedPartInfo := t.GetTruncatePartitionInfo()
	// First, add the new stats meta record for the new partitions.
	for _, def := range addedPartInfo.Definitions {
		if err := h.statsWriter.InsertTableStats2KV(globalTableInfo, def.ID); err != nil {
			return err
		}
	}

	// Second, clean up the old stats meta from global stats meta for the dropped partitions.
	// Do not forget to put those operations in one transaction.
	if err := util.CallWithSCtx(h.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		count := int64(0)
		partitionIDs := make([]int64, 0, len(droppedPartInfo.Definitions))
		partitionNames := make([]string, 0, len(droppedPartInfo.Definitions))
		for _, def := range droppedPartInfo.Definitions {
			// Get the count and modify count of the partition.
			tableCount, _, _, err := storage.StatsMetaCountAndModifyCount(sctx, def.ID)
			if err != nil {
				return err
			}
			count += tableCount
			partitionIDs = append(partitionIDs, def.ID)
			partitionNames = append(partitionNames, def.Name.O)
		}

		if count != 0 {
			is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
			globalTableSchema, ok := infoschema.SchemaByTable(is, globalTableInfo)
			if !ok {
				return errors.Errorf("schema not found for table %s", globalTableInfo.Name.O)
			}
			lockedTables, err := lockstats.QueryLockedTables(sctx)
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
				sctx,
				startTS,
				variable.TableDelta{Count: count, Delta: delta},
				globalTableInfo.ID,
				isLocked,
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
				return err
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

		return nil
	}, util.FlagWrapTxn); err != nil {
		return err
	}

	// Third, clean up the old stats meta from partition stats meta for the dropped partitions.
	// It's OK to put those operations in different transactions. Because it will not affect the correctness.
	for _, def := range droppedPartInfo.Definitions {
		if err := h.statsWriter.UpdateStatsMetaVersionForGC(def.ID); err != nil {
			return err
		}
	}

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
