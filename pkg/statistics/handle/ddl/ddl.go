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
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/lockstats"
	"github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"go.uber.org/zap"
)

type ddlHandlerImpl struct {
	ddlEventCh         chan *util.DDLEvent
	statsWriter        types.StatsReadWriter
	statsHandler       types.StatsHandle
	globalStatsHandler types.StatsGlobal
}

// NewDDLHandler creates a new ddl handler.
func NewDDLHandler(
	statsWriter types.StatsReadWriter,
	statsHandler types.StatsHandle,
	globalStatsHandler types.StatsGlobal,
) types.DDL {
	return &ddlHandlerImpl{
		ddlEventCh:         make(chan *util.DDLEvent, 1000),
		statsWriter:        statsWriter,
		statsHandler:       statsHandler,
		globalStatsHandler: globalStatsHandler,
	}
}

// HandleDDLEvent begins to process a ddl task.
func (h *ddlHandlerImpl) HandleDDLEvent(t *util.DDLEvent) error {
	sctx, err := h.statsHandler.SPool().Get()
	if err != nil {
		return err
	}
	defer h.statsHandler.SPool().Put(sctx)

	// ActionFlashbackCluster will not create any new stats info
	// and it's SchemaID alwayws equals to 0, so skip check it.
	if t.GetType() != model.ActionFlashbackCluster {
		if isSysDB, err := t.IsMemOrSysDB(sctx.(sessionctx.Context)); err != nil {
			return err
		} else if isSysDB {
			// EXCHANGE PARTITION EVENT NOTES:
			// 1. When a partition is exchanged with a system table, we need to adjust the global statistics
			//    based on the count delta and modify count delta. However, due to the involvement of the system table,
			//    a complete update of the global statistics is not feasible. Therefore, we bypass the statistics update
			//    for the table in this scenario. Despite this, the table id still changes, so the statistics for the
			//    system table will still be visible.
			// 2. If the system table is a partitioned table, we will update the global statistics for the partitioned table.
			//    It is rare to exchange a partition from a system table, so we can ignore this case. In this case,
			//    the system table will have statistics, but this is not a significant issue.
			logutil.StatsLogger().Info("Skip handle system database ddl event", zap.Stringer("event", t))
			return nil
		}
	}
	logutil.StatsLogger().Info("Handle ddl event", zap.Stringer("event", t))

	switch t.GetType() {
	case model.ActionCreateTable:
		newTableInfo := t.GetCreateTableInfo()
		ids, err := h.getTableIDs(newTableInfo)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := h.statsWriter.InsertTableStats2KV(newTableInfo, id); err != nil {
				return err
			}
		}
	case model.ActionTruncateTable:
		newTableInfo, droppedTableInfo := t.GetTruncateTableInfo()
		ids, err := h.getTableIDs(newTableInfo)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := h.statsWriter.InsertTableStats2KV(newTableInfo, id); err != nil {
				return err
			}
		}

		// Remove the old table stats.
		droppedIDs, err := h.getTableIDs(droppedTableInfo)
		if err != nil {
			return err
		}
		for _, id := range droppedIDs {
			if err := h.statsWriter.UpdateStatsMetaVersionForGC(id); err != nil {
				return err
			}
		}
	case model.ActionDropTable:
		droppedTableInfo := t.GetDropTableInfo()
		ids, err := h.getTableIDs(droppedTableInfo)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := h.statsWriter.UpdateStatsMetaVersionForGC(id); err != nil {
				return err
			}
		}
	case model.ActionAddColumn:
		newTableInfo, newColumnInfo := t.GetAddColumnInfo()
		ids, err := h.getTableIDs(newTableInfo)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := h.statsWriter.InsertColStats2KV(id, newColumnInfo); err != nil {
				return err
			}
		}
	case model.ActionModifyColumn:
		newTableInfo, modifiedColumnInfo := t.GetModifyColumnInfo()

		ids, err := h.getTableIDs(newTableInfo)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := h.statsWriter.InsertColStats2KV(id, modifiedColumnInfo); err != nil {
				return err
			}
		}
	case model.ActionAddTablePartition:
		globalTableInfo, addedPartitionInfo := t.GetAddPartitionInfo()
		for _, def := range addedPartitionInfo.Definitions {
			if err := h.statsWriter.InsertTableStats2KV(globalTableInfo, def.ID); err != nil {
				return err
			}
		}
	case model.ActionTruncateTablePartition:
		if err := h.onTruncatePartitions(t); err != nil {
			return err
		}
	case model.ActionDropTablePartition:
		if err := h.onDropPartitions(t); err != nil {
			return err
		}
	case model.ActionExchangeTablePartition:
		if err := h.onExchangeAPartition(t); err != nil {
			return err
		}
	case model.ActionReorganizePartition:
		if err := h.onReorganizePartitions(t); err != nil {
			return err
		}
	case model.ActionAlterTablePartitioning:
		oldSingleTableID, globalTableInfo, addedPartInfo := t.GetAddPartitioningInfo()
		// Add new partition stats.
		for _, def := range addedPartInfo.Definitions {
			if err := h.statsWriter.InsertTableStats2KV(globalTableInfo, def.ID); err != nil {
				return err
			}
		}
		// Change id for global stats, since the data has not changed!
		// Note: This operation will update all tables related to statistics with the new ID.
		return h.statsWriter.ChangeGlobalStatsID(oldSingleTableID, globalTableInfo.ID)
	case model.ActionRemovePartitioning:
		// Change id for global stats, since the data has not changed!
		// Note: This operation will update all tables related to statistics with the new ID.
		oldTblID,
			newSingleTableInfo,
			droppedPartInfo := t.GetRemovePartitioningInfo()
		if err := h.statsWriter.ChangeGlobalStatsID(oldTblID, newSingleTableInfo.ID); err != nil {
			return err
		}

		// Remove partition stats.
		for _, def := range droppedPartInfo.Definitions {
			if err := h.statsWriter.UpdateStatsMetaVersionForGC(def.ID); err != nil {
				return err
			}
		}
	case model.ActionFlashbackCluster:
		return h.statsWriter.UpdateStatsVersion()
	}
	return nil
}

// updateStatsWithCountDeltaAndModifyCountDelta updates
// the global stats with the given count delta and modify count delta.
// Only used by some special DDLs, such as exchange partition.
func updateStatsWithCountDeltaAndModifyCountDelta(
	sctx sessionctx.Context,
	tableID int64,
	countDelta, modifyCountDelta int64,
) error {
	lockedTables, err := lockstats.QueryLockedTables(sctx)
	if err != nil {
		return errors.Trace(err)
	}
	isLocked := false
	if _, ok := lockedTables[tableID]; ok {
		isLocked = true
	}
	startTS, err := util.GetStartTS(sctx)
	if err != nil {
		return errors.Trace(err)
	}
	if isLocked {
		// For locked tables, it is possible that the record gets deleted. So it can be negative.
		_, err = util.Exec(
			sctx,
			"INSERT INTO mysql.stats_table_locked "+
				"(version, count, modify_count, table_id) "+
				"VALUES (%?, %?, %?, %?) "+
				"ON DUPLICATE KEY UPDATE "+
				"version = VALUES(version), "+
				"count = count + VALUES(count), "+
				"modify_count = modify_count + VALUES(modify_count)",
			startTS,
			countDelta,
			modifyCountDelta,
			tableID,
		)
		return err
	}

	// Because count can not be negative, so we need to get the current and calculate the delta.
	count, modifyCount, isNull, err := storage.StatsMetaCountAndModifyCount(sctx, tableID)
	if err != nil {
		return err
	}
	if isNull {
		_, err = util.Exec(
			sctx,
			"INSERT INTO mysql.stats_meta "+
				"(version, count, modify_count, table_id) "+
				"VALUES (%?, GREATEST(0, %?), GREATEST(0, %?), %?)",
			startTS,
			countDelta,
			modifyCountDelta,
			tableID,
		)
	} else {
		_, err = util.Exec(
			sctx,
			"UPDATE mysql.stats_meta SET "+
				"version = %?, "+
				"count = GREATEST(0, %?), "+
				"modify_count = GREATEST(0, %?) "+
				"WHERE table_id = %?",
			startTS,
			count+countDelta,
			modifyCount+modifyCountDelta,
			tableID,
		)
	}

	return err
}

func (h *ddlHandlerImpl) getTableIDs(tblInfo *model.TableInfo) (ids []int64, err error) {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		return []int64{tblInfo.ID}, nil
	}
	ids = make([]int64, 0, len(pi.Definitions)+1)
	for _, def := range pi.Definitions {
		ids = append(ids, def.ID)
	}
	pruneMode, err := util.GetCurrentPruneMode(h.statsHandler.SPool())
	if err != nil {
		return nil, err
	}
	if variable.PartitionPruneMode(pruneMode) == variable.Dynamic {
		ids = append(ids, tblInfo.ID)
	}
	return ids, nil
}

// DDLEventCh returns ddl events channel in handle.
func (h *ddlHandlerImpl) DDLEventCh() chan *util.DDLEvent {
	return h.ddlEventCh
}
