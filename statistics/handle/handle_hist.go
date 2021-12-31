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
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

// StatsLoad is used to load stats concurrently
type StatsLoad struct {
	sync.Mutex
	SubCtxs          []sessionctx.Context
	NeededColumnsCh  chan *NeededColumnTask
	TimeoutColumnsCh chan *NeededColumnTask
	workingColMap    map[model.TableColumnID][]chan model.TableColumnID
}

// NeededColumnTask represents one needed column with expire time.
type NeededColumnTask struct {
	TableColumnID model.TableColumnID
	ToTimeout     time.Time
	ResultCh      chan model.TableColumnID
}

// SendLoadRequests send neededColumns requests
func (h *Handle) SendLoadRequests(sc *stmtctx.StatementContext, neededColumns []model.TableColumnID, timeout time.Duration) error {
	missingColumns := h.genHistMissingColumns(neededColumns)
	if len(missingColumns) <= 0 {
		return nil
	}
	sc.StatsLoad.Timeout = timeout
	sc.StatsLoad.NeededColumns = missingColumns
	sc.StatsLoad.ResultCh = make(chan model.TableColumnID, len(neededColumns))
	for _, col := range missingColumns {
		err := h.AppendNeededColumn(col, sc.StatsLoad.ResultCh, timeout)
		if err != nil {
			return err
		}
	}
	sc.StatsLoad.LoadStartTime = time.Now()
	return nil
}

// SyncWaitStatsLoad sync waits loading of neededColumns and return false if timeout
func (h *Handle) SyncWaitStatsLoad(sc *stmtctx.StatementContext) bool {
	if len(sc.StatsLoad.NeededColumns) <= 0 {
		return true
	}
	defer func() {
		if sc.StatsLoad.ResultCh != nil {
			close(sc.StatsLoad.ResultCh)
		}
		sc.StatsLoad.NeededColumns = nil
	}()
	resultCheckMap := map[model.TableColumnID]struct{}{}
	for _, col := range sc.StatsLoad.NeededColumns {
		resultCheckMap[col] = struct{}{}
	}
	metrics.SyncLoadCounter.Inc()
	timer := time.NewTimer(sc.StatsLoad.Timeout)
	defer timer.Stop()
	for {
		select {
		case result, ok := <-sc.StatsLoad.ResultCh:
			if ok {
				delete(resultCheckMap, result)
				if len(resultCheckMap) == 0 {
					metrics.SyncLoadHistogram.Observe(float64(time.Since(sc.StatsLoad.LoadStartTime).Milliseconds()))
					return true
				}
			}
		case <-timer.C:
			metrics.SyncLoadTimeoutCounter.Inc()
			return false
		}
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

// AppendNeededColumn appends needed column to ch, if exists, do not append the duplicated one.
func (h *Handle) AppendNeededColumn(c model.TableColumnID, resultCh chan model.TableColumnID, timeout time.Duration) error {
	toTimout := time.Now().Local().Add(timeout)
	colTask := &NeededColumnTask{TableColumnID: c, ToTimeout: toTimout, ResultCh: resultCh}
	return h.writeToChanWithTimeout(h.StatsLoad.NeededColumnsCh, colTask, timeout)
}

var errExit = errors.New("Stop loading since domain is closed")

type statsReaderContext struct {
	reader      *statsReader
	createdTime time.Time
}

// SubLoadWorker loads hist data for each column
func (h *Handle) SubLoadWorker(ctx sessionctx.Context, exit chan struct{}, exitWg *sync.WaitGroup) {
	readerCtx := &statsReaderContext{}
	defer func() {
		exitWg.Done()
		logutil.BgLogger().Info("SubLoadWorker exited.")
		if readerCtx.reader != nil {
			err := h.releaseStatsReader(readerCtx.reader, ctx.(sqlexec.RestrictedSQLExecutor))
			if err != nil {
				logutil.BgLogger().Error("Fail to release stats loader: ", zap.Error(err))
			}
		}
	}()
	for {
		err := h.handleOneTask(readerCtx, ctx.(sqlexec.RestrictedSQLExecutor), exit)
		if err != nil {
			switch err {
			case errExit:
				return
			default:
				time.Sleep(10 * time.Millisecond)
				continue
			}
		}
	}
}

// handleOneTask handles one column task.
func (h *Handle) handleOneTask(readerCtx *statsReaderContext, ctx sqlexec.RestrictedSQLExecutor, exit chan struct{}) (err error) {
	defer func() {
		// recover for each task, worker keeps working
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.BgLogger().Error("stats loading panicked", zap.String("stack", string(buf)))
		}
	}()
	h.getFreshStatsReader(readerCtx, ctx)
	task, err := h.drainColTask(exit)
	if err != nil {
		if err != errExit {
			logutil.BgLogger().Error("Fail to drain task for stats loading.", zap.Error(err))
		}
		return err
	}
	col := task.TableColumnID
	// to avoid duplicated handling in concurrent scenario
	if !h.setWorking(col, task.ResultCh) {
		return nil
	}
	oldCache := h.statsCache.Load().(statsCache)
	tbl, ok := oldCache.tables[col.TableID]
	if !ok {
		task.ResultCh <- col
		return nil
	}
	c, ok := tbl.Columns[col.ColumnID]
	if !ok || c.Len() > 0 {
		task.ResultCh <- col
		return nil
	}
	t := time.Now()
	hist, err := h.readStatsForOne(col, c, readerCtx.reader)
	if err != nil {
		h.StatsLoad.NeededColumnsCh <- task
		return err
	}
	metrics.ReadStatsHistogram.Observe(float64(time.Since(t).Milliseconds()))
	if hist != nil && h.updateCachedColumn(col, hist) {
		task.ResultCh <- col
	}
	h.finishWorking(col)
	return nil
}

func (h *Handle) getFreshStatsReader(readerCtx *statsReaderContext, ctx sqlexec.RestrictedSQLExecutor) {
	if readerCtx.reader == nil || readerCtx.createdTime.Add(h.Lease()).Before(time.Now()) {
		if readerCtx.reader != nil {
			err := h.releaseStatsReader(readerCtx.reader, ctx)
			if err != nil {
				logutil.BgLogger().Warn("Fail to release stats loader: ", zap.Error(err))
			}
		}
		for {
			newReader, err := h.getStatsReader(0, ctx)
			if err != nil {
				logutil.BgLogger().Error("Fail to new stats loader, retry after a while.", zap.Error(err))
				time.Sleep(time.Millisecond * 10)
			} else {
				readerCtx.reader = newReader
				readerCtx.createdTime = time.Now()
				return
			}
		}
	} else {
		return
	}
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
	// select NeededColumnsCh firstly, if no task, then select TimeoutColumnsCh
	for {
		select {
		case <-exit:
			return nil, errExit
		case task, ok := <-h.StatsLoad.NeededColumnsCh:
			if !ok {
				return nil, errors.New("drainColTask: cannot read from NeededColumnsCh, maybe the chan is closed")
			}
			// if the task has already timeout, no sql is sync-waiting for it,
			// so do not handle it just now, put it to another channel with lower priority
			if time.Now().After(task.ToTimeout) {
				h.writeToChanNonblocking(h.StatsLoad.TimeoutColumnsCh, task)
				continue
			}
			return task, nil
		case task, ok := <-h.StatsLoad.TimeoutColumnsCh:
			select {
			case <-exit:
				return nil, errExit
			case task0, ok0 := <-h.StatsLoad.NeededColumnsCh:
				if !ok0 {
					return nil, errors.New("drainColTask: cannot read from NeededColumnsCh, maybe the chan is closed")
				}
				// send task back to TimeoutColumnsCh and return the task drained from NeededColumnsCh
				h.writeToChanNonblocking(h.StatsLoad.TimeoutColumnsCh, task)
				return task0, nil
			default:
				if !ok {
					return nil, errors.New("drainColTask: cannot read from TimeoutColumnsCh, maybe the chan is closed")
				}
				// NeededColumnsCh is empty now, handle task from TimeoutColumnsCh
				return task, nil
			}
		}
	}
}

// writeToChanNonblocking writes in a nonblocking way, and if the channel queue is full, it's ok to drop the task.
func (h *Handle) writeToChanNonblocking(taskCh chan *NeededColumnTask, task *NeededColumnTask) {
	select {
	case taskCh <- task:
	default:
	}
}

// writeToChanWithTimeout writes a task to a channel and blocks until timeout.
func (h *Handle) writeToChanWithTimeout(taskCh chan *NeededColumnTask, task *NeededColumnTask, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case taskCh <- task:
	case <-timer.C:
		return errors.New("Channel is full and timeout writing to channel")
	}
	return nil
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

func (h *Handle) setWorking(col model.TableColumnID, resultCh chan model.TableColumnID) bool {
	h.StatsLoad.Lock()
	defer h.StatsLoad.Unlock()
	chList, ok := h.StatsLoad.workingColMap[col]
	if ok {
		h.StatsLoad.workingColMap[col] = append(chList, resultCh)
		return false
	}
	chList = []chan model.TableColumnID{}
	chList = append(chList, resultCh)
	h.StatsLoad.workingColMap[col] = chList
	return true
}

func (h *Handle) finishWorking(col model.TableColumnID) {
	h.StatsLoad.Lock()
	defer h.StatsLoad.Unlock()
	if chList, ok := h.StatsLoad.workingColMap[col]; ok {
		for _, ch := range chList {
			ch <- col
		}
	}
	delete(h.StatsLoad.workingColMap, col)
}
