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

package statscache

import (
	"fmt"
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan/statistics"
	"github.com/pingcap/tidb/util/sqlexec"
)

type statsInfo struct {
	tbl     *statistics.Table
	version uint64
}

// Handle can update stats info periodically.
type Handle struct {
	ctx         context.Context
	lastVersion uint64
}

// NewHandle creates a Handle for update stats.
func NewHandle(ctx context.Context) *Handle {
	return &Handle{ctx: ctx}
}

// Update reads stats meta from store and updates the stats map.
func (h *Handle) Update(m *meta.Meta, is infoschema.InfoSchema) error {
	sql := fmt.Sprintf("SELECT version, table_id from mysql.stats_meta where version > %d order by version", h.lastVersion)
	rows, _, err := h.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(h.ctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	for _, row := range rows {
		version, tableID := row.Data[0].GetUint64(), row.Data[1].GetInt64()
		table, ok := is.TableByID(tableID)
		if !ok {
			log.Debugf("Unknown table ID %d in stats meta table, maybe it has been dropped", tableID)
			continue
		}
		tableInfo := table.Meta()
		tpb, err := m.GetTableStats(tableID)
		if err != nil {
			return errors.Trace(err)
		}
		var tbl *statistics.Table
		if tpb != nil {
			tbl, err = statistics.TableFromPB(tableInfo, tpb)
			// Error is not nil may mean that there are some ddl changes on this table, so the origin
			// statistics can not be used any more, we give it a nil one.
			if err != nil {
				log.Errorf("Error occured when convert pb table for table id %d", tableID)
			}
			SetStatisticsTableCache(tableID, tbl, version)
		}
		h.lastVersion = version
	}
	return nil
}

type statsCache struct {
	cache map[int64]*statsInfo
	m     sync.RWMutex
}

var statsTblCache = statsCache{cache: map[int64]*statsInfo{}}

// GetStatisticsTableCache retrieves the statistics table from cache, and the cache will be updated by a goroutine.
func GetStatisticsTableCache(tblInfo *model.TableInfo) *statistics.Table {
	statsTblCache.m.RLock()
	defer statsTblCache.m.RUnlock()
	stats, ok := statsTblCache.cache[tblInfo.ID]
	if !ok || stats == nil {
		return statistics.PseudoTable(tblInfo)
	}
	tbl := stats.tbl
	// Here we check the TableInfo because there may be some ddl changes in the duration period.
	// Also, we rely on the fact that TableInfo will not be same if and only if there are ddl changes.
	if tblInfo == tbl.Info {
		return tbl
	}
	return statistics.PseudoTable(tblInfo)
}

// SetStatisticsTableCache sets the statistics table cache.
func SetStatisticsTableCache(id int64, statsTbl *statistics.Table, version uint64) {
	statsTblCache.m.Lock()
	defer statsTblCache.m.Unlock()
	stats, ok := statsTblCache.cache[id]
	if !ok {
		si := &statsInfo{
			tbl:     statsTbl,
			version: version,
		}
		statsTblCache.cache[id] = si
		return
	}
	if stats.version >= version {
		return
	}
	stats.tbl = statsTbl
	stats.version = version
}
