// Copyright 2017 PingCAP, Inc.
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

package handle

import (
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

// HandleDDLEvent begins to process a ddl task.
func (h *Handle) HandleDDLEvent(t *util.Event) error {
	switch t.Tp {
	case model.ActionCreateTable, model.ActionTruncateTable:
		ids, err := h.getInitStateTableIDs(t.TableInfo)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := h.InsertTableStats2KV(t.TableInfo, id); err != nil {
				return err
			}
		}
	case model.ActionDropTable:
		ids, err := h.getInitStateTableIDs(t.TableInfo)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := h.ResetTableStats2KVForDrop(id); err != nil {
				return err
			}
		}
	case model.ActionAddColumn, model.ActionModifyColumn:
		ids, err := h.getInitStateTableIDs(t.TableInfo)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := h.InsertColStats2KV(id, t.ColumnInfos); err != nil {
				return err
			}
		}
	case model.ActionAddTablePartition, model.ActionTruncateTablePartition:
		for _, def := range t.PartInfo.Definitions {
			if err := h.InsertTableStats2KV(t.TableInfo, def.ID); err != nil {
				return err
			}
		}
	case model.ActionDropTablePartition:
		pruneMode, err := h.GetCurrentPruneMode()
		if err != nil {
			return err
		}
		if variable.PartitionPruneMode(pruneMode) == variable.Dynamic && t.PartInfo != nil {
			if err := h.UpdateGlobalStats(t.TableInfo); err != nil {
				return err
			}
		}
		for _, def := range t.PartInfo.Definitions {
			if err := h.ResetTableStats2KVForDrop(def.ID); err != nil {
				return err
			}
		}
	case model.ActionReorganizePartition:
		for _, def := range t.PartInfo.Definitions {
			// TODO: Should we trigger analyze instead of adding 0s?
			if err := h.InsertTableStats2KV(t.TableInfo, def.ID); err != nil {
				return err
			}
			// Do not update global stats, since the data have not changed!
		}
	case model.ActionAlterTablePartitioning:
		// Add partitioning
		for _, def := range t.PartInfo.Definitions {
			// TODO: Should we trigger analyze instead of adding 0s?
			if err := h.InsertTableStats2KV(t.TableInfo, def.ID); err != nil {
				return err
			}
		}
		fallthrough
	case model.ActionRemovePartitioning:
		// Change id for global stats, since the data has not changed!
		// Note that t.TableInfo is the current (new) table info
		// and t.PartInfo.NewTableID is actually the old table ID!
		// (see onReorganizePartition)
		return h.ChangeGlobalStatsID(t.PartInfo.NewTableID, t.TableInfo.ID)
	case model.ActionFlashbackCluster:
		return h.UpdateStatsVersion()
	}
	return nil
}

func (h *Handle) getInitStateTableIDs(tblInfo *model.TableInfo) (ids []int64, err error) {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		return []int64{tblInfo.ID}, nil
	}
	ids = make([]int64, 0, len(pi.Definitions)+1)
	for _, def := range pi.Definitions {
		ids = append(ids, def.ID)
	}
	pruneMode, err := h.GetCurrentPruneMode()
	if err != nil {
		return nil, err
	}
	if variable.PartitionPruneMode(pruneMode) == variable.Dynamic {
		ids = append(ids, tblInfo.ID)
	}
	return ids, nil
}

// DDLEventCh returns ddl events channel in handle.
func (h *Handle) DDLEventCh() chan *util.Event {
	return h.ddlEventCh
}
