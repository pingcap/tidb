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
	"github.com/pingcap/tidb/pkg/statistics/handle/logutil"
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
	switch t.GetType() {
	case model.ActionCreateTable:
		newTableInfo := t.GetCreateTableInfo()
		ids, err := h.getInitStateTableIDs(newTableInfo)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := h.statsWriter.InsertTableStats2KV(newTableInfo, id); err != nil {
				return err
			}
		}
	case model.ActionTruncateTable:
		newTableInfo, _ := t.GetTruncateTableInfo()
		ids, err := h.getInitStateTableIDs(newTableInfo)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := h.statsWriter.InsertTableStats2KV(newTableInfo, id); err != nil {
				return err
			}
		}
	case model.ActionDropTable:
		droppedTableInfo := t.GetDropTableInfo()
		ids, err := h.getInitStateTableIDs(droppedTableInfo)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := h.statsWriter.ResetTableStats2KVForDrop(id); err != nil {
				return err
			}
		}
	case model.ActionAddColumn:
		newTableInfo, newColumnInfo := t.GetAddColumnInfo()
		ids, err := h.getInitStateTableIDs(newTableInfo)
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

		ids, err := h.getInitStateTableIDs(newTableInfo)
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
		globalTableInfo, addedPartInfo, _ := t.GetTruncatePartitionInfo()
		for _, def := range addedPartInfo.Definitions {
			if err := h.statsWriter.InsertTableStats2KV(globalTableInfo, def.ID); err != nil {
				return err
			}
		}
	case model.ActionDropTablePartition:
		globalTableInfo, droppedPartitionInfo := t.GetDropPartitionInfo()

		count := int64(0)
		for _, def := range droppedPartitionInfo.Definitions {
			// Get the count and modify count of the partition.
			stats := h.statsHandler.GetPartitionStats(globalTableInfo, def.ID)
			if stats.Pseudo {
				se, err := h.statsHandler.SPool().Get()
				if err != nil {
					return errors.Trace(err)
				}
				sctx := se.(sessionctx.Context)
				is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
				schema, _ := is.SchemaByTable(globalTableInfo)
				logutil.StatsLogger().Warn(
					"drop partition with pseudo stats, "+
						"usually it won't happen because we always load stats when initializing the handle",
					zap.String("schema", schema.Name.O),
					zap.String("table", globalTableInfo.Name.O),
					zap.String("partition", def.Name.O),
				)
			} else {
				count += stats.RealtimeCount
			}
			// Always reset the partition stats.
			if err := h.statsWriter.ResetTableStats2KVForDrop(def.ID); err != nil {
				return err
			}
		}
		if count != 0 {
			// Because we drop the partition, we should subtract the count from the global stats.
			delta := -count
			if err := h.statsWriter.UpdateStatsMetaDelta(
				globalTableInfo.ID, count, delta,
			); err != nil {
				return err
			}
		}
	case model.ActionReorganizePartition:
		globalTableInfo, addedPartInfo, _ := t.GetReorganizePartitionInfo()
		for _, def := range addedPartInfo.Definitions {
			// TODO: Should we trigger analyze instead of adding 0s?
			if err := h.statsWriter.InsertTableStats2KV(globalTableInfo, def.ID); err != nil {
				return err
			}
			// Do not update global stats, since the data have not changed!
		}
	case model.ActionAlterTablePartitioning:
		globalTableInfo, addedPartInfo := t.GetAddPartitioningInfo()
		// Add partitioning
		for _, def := range addedPartInfo.Definitions {
			// TODO: Should we trigger analyze instead of adding 0s?
			if err := h.statsWriter.InsertTableStats2KV(globalTableInfo, def.ID); err != nil {
				return err
			}
		}
		// Change id for global stats, since the data has not changed!
		// Note that globalTableInfo is the new table info
		// and addedPartInfo.NewTableID is actually the old table ID!
		// (see onReorganizePartition)
		return h.statsWriter.ChangeGlobalStatsID(addedPartInfo.NewTableID, globalTableInfo.ID)
	case model.ActionRemovePartitioning:
		// Change id for global stats, since the data has not changed!
		// Note that newSingleTableInfo is the new table info
		// and droppedPartInfo.NewTableID is actually the old table ID!
		// (see onReorganizePartition)
		newSingleTableInfo, droppedPartInfo := t.GetRemovePartitioningInfo()
		return h.statsWriter.ChangeGlobalStatsID(droppedPartInfo.NewTableID, newSingleTableInfo.ID)
	case model.ActionFlashbackCluster:
		return h.statsWriter.UpdateStatsVersion()
	}
	return nil
}

func (h *ddlHandlerImpl) getInitStateTableIDs(tblInfo *model.TableInfo) (ids []int64, err error) {
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
