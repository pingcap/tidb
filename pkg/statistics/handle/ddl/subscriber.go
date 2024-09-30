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
		// TODO: implement me
	case model.ActionModifyColumn:
		// TODO: implement me
	case model.ActionAddTablePartition:
		// TODO: implement me
	case model.ActionTruncateTablePartition:
		// TODO: implement me
	case model.ActionDropTablePartition:
		// TODO: implement me
	case model.ActionExchangeTablePartition:
		// TODO: implement me
	case model.ActionReorganizePartition:
		// TODO: implement me
	case model.ActionAlterTablePartitioning:
		// TODO: implement me
	case model.ActionRemovePartitioning:
		// TODO: implement me
	case model.ActionFlashbackCluster:
		// TODO: implement me
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
