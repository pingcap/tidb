// Copyright 2021 PingCAP, Inc.
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
	"runtime"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/timeutil"
	"go.uber.org/zap"
)

// TODO load idx histograms by need

type StatsLoad struct {
	sync.Mutex
	SubCtxs          []sessionctx.Context
	NeededColumnsCh  chan *NeededColumnTask
	TimeoutColumnsCh chan *NeededColumnTask
	workingColMap    map[model.TableColumnID]struct{}
}

// NeededColumnTask represents one needed column with expire time.
type NeededColumnTask struct {
	TableColumnID model.TableColumnID
	ToTimeout     time.Time
	Wg            *sync.WaitGroup
}

// SyncLoad sync waits loading of neededColumns and return false if timeout
func (h *Handle) SyncLoad(sc *stmtctx.StatementContext, neededColumns []model.TableColumnID, timeout time.Duration) bool {
	missingColumns := h.genHistMissingColumns(neededColumns)
	if len(missingColumns) <= 0 {
		return true
	}
	sc.StatsLoad.NeededColumns = missingColumns
	sc.StatsLoad.Wg = &sync.WaitGroup{}
	sc.StatsLoad.Wg.Add(len(missingColumns))
	for _, col := range missingColumns {
		h.appendNeededColumn(col, sc.StatsLoad.Wg, timeout)
	}
	metrics.SyncLoadCounter.Inc()
	t := time.Now()
	if util.WaitTimeout(sc.StatsLoad.Wg, timeout) {
		metrics.SyncLoadTimeoutCounter.Inc()
		return false
	} else {
		metrics.SyncLoadHistogram.Observe(float64(time.Since(t).Milliseconds()))
		return true
	}
}

// genHistMissingColumns generates hist-missing columns based on neededColumns and statsCache.
func (h *Handle) genHistMissingColumns(neededColumns []model.TableColumnID) []model.TableColumnID {
	statsCache := h.statsCache.Load().(statsCache)
	missingColumns := make([]model.TableColumnID, 0, len(neededColumns))
	for _, col := range neededColumns {
		tbl, ok := statsCache.tables[col.TableID]
		if !ok {
			continue
		}
		colHist, ok := tbl.Columns[col.ColumnID]
		if !ok {
			continue
		}
		if colHist.IsHistNeeded(tbl.Pseudo) {
			missingColumns = append(missingColumns, col)
		}
	}
	return missingColumns
}

// appendNeededColumn appends needed column to ch, if exists, do not append the duplicated one.
func (h *Handle) appendNeededColumn(c model.TableColumnID, wg *sync.WaitGroup, timeout time.Duration) {
	toTimout := time.Now().Local().Add(timeout)
	colTask := &NeededColumnTask{TableColumnID: c, ToTimeout: toTimout, Wg: wg}
	h.StatsLoad.NeededColumnsCh <- colTask
}

var ErrExit = errors.New("Stop loading since domain is closed.")

// SubLoadWorker loads hist data for each column
func (h *Handle) SubLoadWorker(ctx sessionctx.Context, exit chan struct{}, exitWg *sync.WaitGroup) error {
	defer func() {
		exitWg.Done()
		logutil.BgLogger().Info("SubLoadWorker exited.")
	}()
	reader, err0 := h.getStatsReader(0, ctx.(sqlexec.RestrictedSQLExecutor))
	if err0 != nil {
		return err0
	}
	defer func() {
		err1 := h.releaseStatsReader(reader, ctx.(sqlexec.RestrictedSQLExecutor))
		if err1 != nil && err0 == nil {
			logutil.BgLogger().Error("Fail to release stats loader: ", zap.Error(err1))
		}
	}()
	batched := 0
	for {
		batched += 1
		err := h.handleOneTask(reader, exit)
		if err != nil {
			switch err {
			case ErrExit:
				return nil
			default:
				time.Sleep(10 * time.Millisecond)
				continue
			}
		}
		if batched >= 100 {
			// refresh statsReader after a while for latest stats
			err = h.releaseStatsReader(reader, ctx.(sqlexec.RestrictedSQLExecutor))
			if err != nil {
				logutil.BgLogger().Error("Fail to release stats loader: ", zap.Error(err))
			}
			reader, err = h.getStatsReader(0, ctx.(sqlexec.RestrictedSQLExecutor))
			if err != nil {
				// TODO will begin/commit fail?
				logutil.BgLogger().Error("Fail to new stats loader: ", zap.Error(err))
			}
			batched = 0
		}
	}
}

// handleOneTask handles one column task.
func (h *Handle) handleOneTask(reader *statsReader, exit chan struct{}) error {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.BgLogger().Error("stats loading panicked", zap.String("stack", string(buf)))
			metrics.PanicCounter.WithLabelValues(metrics.LabelStatsLoadWorker).Inc()
		}
	}()
	task, err0 := h.drainColTask(exit)
	if err0 != nil && task == nil {
		logutil.BgLogger().Fatal("Fail to drain task for stats loading.")
		return err0
	}
	col := task.TableColumnID
	// to avoid duplicated handling in concurrent scenario
	if !h.setWorking(col) {
		return nil
	}
	oldCache := h.statsCache.Load().(statsCache)
	tbl, ok := oldCache.tables[col.TableID]
	if !ok {
		task.Wg.Done()
		return nil
	}
	c, ok := tbl.Columns[col.ColumnID]
	if !ok || c.Len() > 0 {
		task.Wg.Done()
		return nil
	}
	t := time.Now()
	hist, err := h.readStatsForOne(col, c, reader)
	if err != nil {
		h.StatsLoad.NeededColumnsCh <- task
		return err
	}
	metrics.ReadStatsHistogram.Observe(float64(time.Since(t).Milliseconds()))
	if hist != nil && h.updateCachedColumn(col, hist) {
		task.Wg.Done()
	}
	h.finishWorking(col)
	return nil
}

// readStatsForOne reads hist for one column, TODO load data via kv-get asynchronously
func (h *Handle) readStatsForOne(col model.TableColumnID, c *statistics.Column, reader *statsReader) (*statistics.Column, error) {
	hg, err := h.histogramFromStorage(reader, col.TableID, c.ID, &c.Info.FieldType, c.Histogram.NDV, 0, c.LastUpdateVersion, c.NullCount, c.TotColSize, c.Correlation)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cms, topN, err := h.cmSketchAndTopNFromStorage(reader, col.TableID, 0, col.ColumnID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	fms, err := h.fmSketchFromStorage(reader, col.TableID, 0, col.ColumnID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rows, _, err := reader.read("select stats_ver from mysql.stats_histograms where is_index = 0 and table_id = %? and hist_id = %?", col.TableID, col.ColumnID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rows) == 0 {
		logutil.BgLogger().Error("fail to get stats version for this histogram", zap.Int64("table_id", col.TableID), zap.Int64("hist_id", col.ColumnID))
	}
	colHist := &statistics.Column{
		PhysicalID: col.TableID,
		Histogram:  *hg,
		Info:       c.Info,
		CMSketch:   cms,
		TopN:       topN,
		FMSketch:   fms,
		IsHandle:   c.IsHandle,
		StatsVer:   rows[0].GetInt64(0),
	}
	// Column.Count is calculated by Column.TotalRowCount(). Hence, we don't set Column.Count when initializing colHist.
	colHist.Count = int64(colHist.TotalRowCount())
	return colHist, nil
}

// drainColTask will hang until a column task can return.
func (h *Handle) drainColTask(exit chan struct{}) (*NeededColumnTask, error) {
	timeout := time.Nanosecond * 100
	to := timeutil.NewGoodTimer(timeout)
	for {
		to.Reset(timeout)
		select { // select ColumnsCh firstly since the priority
		case task, ok := <-h.StatsLoad.NeededColumnsCh:
			if !ok {
				return nil, errors.New("drainColTask: cannot read from a closed ColumnsCh, maybe the chan is closed.")
			}
			if time.Now().After(task.ToTimeout) {
				h.StatsLoad.NeededColumnsCh <- task
				continue
			}
			return task, nil
		case <-to.C():
			to.SetRead()
			to.Reset(timeout)
			select { // select TimeoutColumnsCh if there's no task from ColumnsCh currently
			case task, ok := <-h.StatsLoad.TimeoutColumnsCh:
				if !ok {
					return nil, errors.New("drainColTask: cannot read from a closed TimeoutColumnsCh, maybe the chan is closed.")
				}
				return task, nil
			case <-to.C():
				to.SetRead()
				continue
			case <-exit:
				return nil, ErrExit
			}
		case <-exit:
			return nil, ErrExit
		}
	}
}

// updateCachedColumn updates the column hist to global statsCache.
func (h *Handle) updateCachedColumn(col model.TableColumnID, colHist *statistics.Column) (updated bool) {
	h.StatsLoad.Lock()
	defer h.StatsLoad.Unlock()
	// Reload the latest stats cache, otherwise the `updateStatsCache` may fail with high probability, because functions
	// like `GetPartitionStats` called in `fmSketchFromStorage` would have modified the stats cache already.
	oldCache := h.statsCache.Load().(statsCache)
	tbl, ok := oldCache.tables[col.TableID]
	if !ok {
		return true
	}
	c, ok := tbl.Columns[col.ColumnID]
	if !ok || c.Len() > 0 {
		return true
	}
	tbl = tbl.Copy()
	tbl.Columns[c.ID] = colHist
	return h.updateStatsCache(oldCache.update([]*statistics.Table{tbl}, nil, oldCache.version))
}

func (h *Handle) setWorking(col model.TableColumnID) bool {
	h.StatsLoad.Lock()
	defer h.StatsLoad.Unlock()
	if h.StatsLoad.workingColMap == nil {
		h.StatsLoad.workingColMap = map[model.TableColumnID]struct{}{}
	}
	if _, ok := h.StatsLoad.workingColMap[col]; ok {
		return false
	} else {
		h.StatsLoad.workingColMap[col] = struct{}{}
		return true
	}
}

func (h *Handle) finishWorking(col model.TableColumnID) {
	h.StatsLoad.Lock()
	defer h.StatsLoad.Unlock()
	delete(h.StatsLoad.workingColMap, col)
}
