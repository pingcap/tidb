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
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/sqlexec"
)

type statsCache map[int64]*Table

// Handle can update stats info periodically.
type Handle struct {
	ctx context.Context
	// LastVersion is the latest update version before last lease. Exported for test.
	LastVersion uint64
	// PrevLastVersion is the latest update version before two lease. Exported for test.
	// We need this because for two tables, the smaller version may write later than the one with larger version.
	// We can read the version with lastTwoVersion if the diff between commit time and version is less than one lease.
	// PrevLastVersion will be assigned by LastVersion every time Update is called.
	PrevLastVersion uint64
	statsCache      atomic.Value
	// ddlEventCh is a channel to notify a ddl operation has happened.
	// It is sent only by owner or the drop stats executor, and read by stats handle.
	ddlEventCh chan *util.Event
	// analyzeResultCh is a channel to notify an analyze index or column operation has ended.
	// We need this to avoid updating the stats simultaneously.
	analyzeResultCh chan *AnalyzeResult
	// listHead contains all the stats collector required by session.
	listHead *SessionStatsCollector
	// globalMap contains all the delta map from collectors when we dump them to KV.
	globalMap tableDeltaMap

	Lease time.Duration
}

// Clear the statsCache, only for test.
func (h *Handle) Clear() {
	h.statsCache.Store(statsCache{})
	h.LastVersion = 0
	h.PrevLastVersion = 0
	for len(h.ddlEventCh) > 0 {
		<-h.ddlEventCh
	}
	for len(h.analyzeResultCh) > 0 {
		<-h.analyzeResultCh
	}
	h.listHead = &SessionStatsCollector{mapper: make(tableDeltaMap)}
	h.globalMap = make(tableDeltaMap)
}

// NewHandle creates a Handle for update stats.
func NewHandle(ctx context.Context, lease time.Duration) *Handle {
	handle := &Handle{
		ctx:             ctx,
		ddlEventCh:      make(chan *util.Event, 100),
		analyzeResultCh: make(chan *AnalyzeResult, 100),
		listHead:        &SessionStatsCollector{mapper: make(tableDeltaMap)},
		globalMap:       make(tableDeltaMap),
		Lease:           lease,
	}
	handle.statsCache.Store(statsCache{})
	return handle
}

// AnalyzeResultCh returns analyze result channel in handle.
func (h *Handle) AnalyzeResultCh() chan *AnalyzeResult {
	return h.analyzeResultCh
}

// Update reads stats meta from store and updates the stats map.
func (h *Handle) Update(is infoschema.InfoSchema) error {
	sql := fmt.Sprintf("SELECT version, table_id, modify_count, count from mysql.stats_meta where version > %d order by version", h.PrevLastVersion)
	rows, _, err := h.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(h.ctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	h.PrevLastVersion = h.LastVersion
	tables := make([]*Table, 0, len(rows))
	deletedTableIDs := make([]int64, 0, len(rows))
	for _, row := range rows {
		version := row.GetUint64(0)
		tableID := row.GetInt64(1)
		modifyCount := row.GetInt64(2)
		count := row.GetInt64(3)
		h.LastVersion = version
		table, ok := is.TableByID(tableID)
		if !ok {
			log.Debugf("Unknown table ID %d in stats meta table, maybe it has been dropped", tableID)
			deletedTableIDs = append(deletedTableIDs, tableID)
			continue
		}
		tableInfo := table.Meta()
		tbl, err := h.tableStatsFromStorage(tableInfo)
		// Error is not nil may mean that there are some ddl changes on this table, we will not update it.
		if err != nil {
			log.Errorf("Error occurred when read table stats for table id %d. The error message is %s.", tableID, errors.ErrorStack(err))
			continue
		}
		if tbl == nil {
			deletedTableIDs = append(deletedTableIDs, tableID)
			continue
		}
		tbl.Version = version
		tbl.Count = count
		tbl.ModifyCount = modifyCount
		tables = append(tables, tbl)
	}
	h.UpdateTableStats(tables, deletedTableIDs)
	return nil
}

// GetTableStats retrieves the statistics table from cache, and the cache will be updated by a goroutine.
func (h *Handle) GetTableStats(tblID int64) *Table {
	tbl, ok := h.statsCache.Load().(statsCache)[tblID]
	if !ok {
		return PseudoTable(tblID)
	}
	return tbl
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
func (h *Handle) UpdateTableStats(tables []*Table, deletedIDs []int64) {
	newCache := h.copyFromOldCache()
	for _, tbl := range tables {
		id := tbl.TableID
		newCache[id] = tbl
	}
	for _, id := range deletedIDs {
		delete(newCache, id)
	}
	h.statsCache.Store(newCache)
}

// LoadNeededHistograms will load histograms for those needed columns.
func (h *Handle) LoadNeededHistograms() error {
	cols := histogramNeededColumns.allCols()
	for _, col := range cols {
		tbl := h.GetTableStats(col.tableID).copy()
		c, ok := tbl.Columns[col.columnID]
		if !ok || len(c.Buckets) > 0 {
			histogramNeededColumns.delete(col)
			continue
		}
		hg, err := histogramFromStorage(h.ctx, col.tableID, c.ID, &c.Info.FieldType, c.NDV, 0, c.LastUpdateVersion, c.NullCount)
		if err != nil {
			return errors.Trace(err)
		}
		cms, err := h.cmSketchFromStorage(col.tableID, 0, col.columnID)
		if err != nil {
			return errors.Trace(err)
		}
		tbl.Columns[c.ID] = &Column{Histogram: *hg, Info: c.Info, CMSketch: cms, Count: int64(hg.totalRowCount())}
		h.UpdateTableStats([]*Table{tbl}, nil)
		histogramNeededColumns.delete(col)
	}
	return nil
}

func (h *Handle) initStatsMeta(is infoschema.InfoSchema) (statsCache, error) {
	sql := fmt.Sprintf("select version, table_id, modify_count, count from mysql.stats_meta")
	rows, _, err := h.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(h.ctx, sql)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tables := make(statsCache, len(rows))
	for _, row := range rows {
		tableID := row.GetInt64(1)
		table, ok := is.TableByID(row.GetInt64(1))
		if !ok {
			log.Debugf("Unknown table ID %d in stats meta table, maybe it has been dropped", tableID)
			continue
		}
		tableInfo := table.Meta()
		tbl := &Table{
			TableID:     tableID,
			Columns:     make(map[int64]*Column, len(tableInfo.Columns)),
			Indices:     make(map[int64]*Index, len(tableInfo.Indices)),
			Count:       row.GetInt64(3),
			ModifyCount: row.GetInt64(2),
			Version:     row.GetUint64(0),
		}
		tables[tableID] = tbl
	}
	return tables, nil
}

func (h *Handle) initStatsHistograms(is infoschema.InfoSchema, tables statsCache) error {
	sql := fmt.Sprintf("select table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch from mysql.stats_histograms")
	rows, _, err := h.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(h.ctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	for _, row := range rows {
		table, ok := tables[row.GetInt64(0)]
		if !ok {
			continue
		}
		hist := Histogram{
			ID:                row.GetInt64(2),
			NDV:               row.GetInt64(3),
			NullCount:         row.GetInt64(5),
			LastUpdateVersion: row.GetUint64(4),
		}
		tbl, _ := is.TableByID(table.TableID)
		if row.GetInt64(1) > 0 {
			var idxInfo *model.IndexInfo
			for _, idx := range tbl.Meta().Indices {
				if idx.ID == hist.ID {
					idxInfo = idx
					break
				}
			}
			if idxInfo == nil {
				continue
			}
			cms, err := decodeCMSketch(row.GetBytes(6))
			if err != nil {
				cms = nil
				terror.Log(errors.Trace(err))
			}
			table.Indices[hist.ID] = &Index{Histogram: hist, CMSketch: cms, Info: idxInfo}
		} else {
			var colInfo *model.ColumnInfo
			for _, col := range tbl.Meta().Columns {
				if col.ID == hist.ID {
					colInfo = col
					break
				}
			}
			if colInfo == nil {
				continue
			}
			table.Columns[hist.ID] = &Column{Histogram: hist, Info: colInfo}
		}
	}
	return nil
}

func (h *Handle) initStatsBuckets(tables statsCache) error {
	sql := fmt.Sprintf("select table_id, is_index, hist_id, bucket_id, count, repeats, lower_bound, upper_bound from mysql.stats_buckets")
	rows, fields, err := h.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(h.ctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	for _, row := range rows {
		tableID, isIndex, histID, bucketID := row.GetInt64(0), row.GetInt64(1), row.GetInt64(2), row.GetInt64(3)
		table, ok := tables[tableID]
		if !ok {
			continue
		}
		var lower, upper types.Datum
		var hist *Histogram
		if isIndex > 0 {
			index, ok := table.Indices[histID]
			if !ok {
				continue
			}
			hist = &index.Histogram
			lower, upper = row.GetDatum(6, &fields[6].Column.FieldType), row.GetDatum(7, &fields[7].Column.FieldType)
		} else {
			column, ok := table.Columns[histID]
			if !ok {
				continue
			}
			column.Count += row.GetInt64(4)
			if !mysql.HasPriKeyFlag(column.Info.Flag) {
				continue
			}
			hist = &column.Histogram
			d := row.GetDatum(6, &fields[6].Column.FieldType)
			lower, err = d.ConvertTo(h.ctx.GetSessionVars().StmtCtx, &column.Info.FieldType)
			if err != nil {
				terror.Log(errors.Trace(err))
				delete(table.Columns, histID)
				continue
			}
			d = row.GetDatum(7, &fields[7].Column.FieldType)
			upper, err = d.ConvertTo(h.ctx.GetSessionVars().StmtCtx, &column.Info.FieldType)
			if err != nil {
				terror.Log(errors.Trace(err))
				delete(table.Columns, histID)
				continue
			}
		}
		for i := len(hist.Buckets); i <= int(bucketID); i++ {
			hist.Buckets = append(hist.Buckets, Bucket{})
		}
		lowerScalar, upperScalar, commonLength := preCalculateDatumScalar(&lower, &upper)
		hist.Buckets[bucketID] = Bucket{
			Count:        row.GetInt64(4),
			UpperBound:   upper,
			LowerBound:   lower,
			Repeats:      row.GetInt64(5),
			lowerScalar:  lowerScalar,
			upperScalar:  upperScalar,
			commonPfxLen: commonLength,
		}
	}
	for _, table := range tables {
		if h.LastVersion < table.Version {
			h.LastVersion = table.Version
		}
		for _, idx := range table.Indices {
			for i := 1; i < len(idx.Buckets); i++ {
				idx.Buckets[i].Count += idx.Buckets[i-1].Count
			}
		}
		for _, col := range table.Columns {
			for i := 1; i < len(col.Buckets); i++ {
				col.Buckets[i].Count += col.Buckets[i-1].Count
			}
		}
	}
	return nil
}

// InitStats will init the stats cache using a faster strategy than `Update` which is used for delta update.
func (h *Handle) InitStats(is infoschema.InfoSchema) error {
	tables, err := h.initStatsMeta(is)
	if err != nil {
		return errors.Trace(err)
	}
	err = h.initStatsHistograms(is, tables)
	if err != nil {
		return errors.Trace(err)
	}
	err = h.initStatsBuckets(tables)
	if err != nil {
		return errors.Trace(err)
	}
	h.statsCache.Store(tables)
	return nil
}
