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
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"fmt"
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/sqlexec"
)

type statsCache map[int64]*Table

// Handle can update stats info periodically.
type Handle struct {
	ctx         context.Context
	lastVersion uint64
	statsCache  atomic.Value
	// ddlEventCh is a channel to notify a ddl operation has happened. It is sent only by owner and read by stats handle.
	ddlEventCh chan *ddl.Event
}

// Clear the statsCache, only for test.
func (h *Handle) Clear() {
	h.statsCache.Store(statsCache{})
	h.lastVersion = 0
}

// NewHandle creates a Handle for update stats.
func NewHandle(ctx context.Context) *Handle {
	handle := &Handle{
		ctx:        ctx,
		ddlEventCh: make(chan *ddl.Event, 100),
	}
	handle.statsCache.Store(statsCache{})
	return handle
}

// Update reads stats meta from store and updates the stats map.
func (h *Handle) Update(is infoschema.InfoSchema) error {
	sql := fmt.Sprintf("SELECT version, table_id, count from mysql.stats_meta where version > %d order by version", h.lastVersion)
	rows, _, err := h.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(h.ctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	tables := make([]*Table, 0, len(rows))
	for _, row := range rows {
		version, tableID, count := row.Data[0].GetUint64(), row.Data[1].GetInt64(), row.Data[2].GetInt64()
		table, ok := is.TableByID(tableID)
		if !ok {
			log.Debugf("Unknown table ID %d in stats meta table, maybe it has been dropped", tableID)
			continue
		}
		tableInfo := table.Meta()
		tbl, err := h.TableStatsFromStorage(h.ctx, tableInfo, count)
		// Error is not nil may mean that there are some ddl changes on this table, we will not update it.
		if err != nil {
			log.Errorf("Error occurred when read table stats for table id %d. The error message is %s.", tableID, err.Error())
			continue
		}
		tables = append(tables, tbl)
		h.lastVersion = version
	}
	h.UpdateTableStats(tables)
	return nil
}

// GetTableStats retrieves the statistics table from cache, and the cache will be updated by a goroutine.
func (h *Handle) GetTableStats(tblInfo *model.TableInfo) *Table {
	tbl, ok := h.statsCache.Load().(statsCache)[tblInfo.ID]
	if !ok || tbl == nil {
		return PseudoTable(tblInfo)
	}
	// Here we check the TableInfo because there may be some ddl changes in the duration period.
	// Also, we rely on the fact that TableInfo will not be same if and only if there are ddl changes.
	// TODO: Remove this check.
	if tblInfo == tbl.Info {
		return tbl
	}
	return PseudoTable(tblInfo)
}

func (h *Handle) copyFromOldCache() statsCache {
	newCache := statsCache{}
	oldCache := h.statsCache.Load().(statsCache)
	for k, v := range oldCache {
		newCache[k] = v
	}
	return newCache
}

// UpdateTableStats updates the statistics table cache using copy on write.
func (h *Handle) UpdateTableStats(tables []*Table) {
	newCache := h.copyFromOldCache()
	for _, tbl := range tables {
		id := tbl.Info.ID
		newCache[id] = tbl
	}
	h.statsCache.Store(newCache)
}
