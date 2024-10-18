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
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/lockstats"
	"github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"go.uber.org/zap"
)

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
	// TODO: figure out why we can't use the globalTableInfo directly.
	tbl, _ := is.TableByID(ctx, globalTableInfo.ID)
	globalTableSchema, ok := infoschema.SchemaByTable(is, tbl.Meta())
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
