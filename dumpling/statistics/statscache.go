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
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/sqlexec"
)

type statsInfo struct {
	tbl     *Table
	version uint64
}

// Handle can update stats info periodically.
type Handle struct {
	ctx         context.Context
	lastVersion uint64
	cache       map[int64]*statsInfo
	m           sync.RWMutex
}

// Clear the statsTblCache, only for test.
func (h *Handle) Clear() {
	h.cache = map[int64]*statsInfo{}
	h.lastVersion = 0
}

// NewHandle creates a Handle for update stats.
func NewHandle(ctx context.Context) *Handle {
	return &Handle{
		ctx:   ctx,
		cache: map[int64]*statsInfo{},
	}
}

// Update reads stats meta from store and updates the stats map.
func (h *Handle) Update(is infoschema.InfoSchema) error {
	sql := fmt.Sprintf("SELECT version, table_id, count from mysql.stats_meta where version > %d order by version", h.lastVersion)
	rows, _, err := h.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(h.ctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	for _, row := range rows {
		version, tableID, count := row.Data[0].GetUint64(), row.Data[1].GetInt64(), row.Data[2].GetInt64()
		table, ok := is.TableByID(tableID)
		if !ok {
			log.Debugf("Unknown table ID %d in stats meta table, maybe it has been dropped", tableID)
			continue
		}
		tableInfo := table.Meta()
		tbl, err := h.TableStatsFromStorage(h.ctx, tableInfo, count)
		// Error is not nil may mean that there are some ddl changes on this table, so the origin
		// statistics can not be used any more, we give it a pseudo one.
		if err != nil {
			log.Errorf("Error occurred when read table stats for table id %d. The error message is %s.", tableID, err.Error())
			tbl = PseudoTable(tableInfo)
		}
		h.SetTableStats(tableID, tbl, version)
		h.lastVersion = version
	}
	return nil
}

// GetTableStats retrieves the statistics table from cache, and the cache will be updated by a goroutine.
func (h *Handle) GetTableStats(tblInfo *model.TableInfo) *Table {
	h.m.RLock()
	defer h.m.RUnlock()
	stats, ok := h.cache[tblInfo.ID]
	if !ok || stats == nil {
		return PseudoTable(tblInfo)
	}
	tbl := stats.tbl
	// Here we check the TableInfo because there may be some ddl changes in the duration period.
	// Also, we rely on the fact that TableInfo will not be same if and only if there are ddl changes.
	if tblInfo == tbl.Info {
		return tbl
	}
	return PseudoTable(tblInfo)
}

// SetTableStats sets the statistics table cache.
func (h *Handle) SetTableStats(id int64, statsTbl *Table, version uint64) {
	h.m.Lock()
	defer h.m.Unlock()
	stats, ok := h.cache[id]
	if !ok {
		si := &statsInfo{
			tbl:     statsTbl,
			version: version,
		}
		h.cache[id] = si
		return
	}
	if stats.version >= version {
		return
	}
	stats.tbl = statsTbl
	stats.version = version
}
