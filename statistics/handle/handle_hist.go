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
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

type statsWrapper struct {
	col *statistics.Column
	idx *statistics.Index
}

// StatsLoad is used to load stats concurrently
type StatsLoad struct {
	sync.Mutex
	SubCtxs        []sessionctx.Context
	NeededItemsCh  chan *NeededItemTask
	TimeoutItemsCh chan *NeededItemTask
	WorkingColMap  map[model.TableItemID][]chan model.TableItemID
}

// NeededItemTask represents one needed column/indices with expire time.
type NeededItemTask struct {
	TableItemID model.TableItemID
	ToTimeout   time.Time
	ResultCh    chan model.TableItemID
}

// SendLoadRequests send neededColumns requests
func (h *Handle) SendLoadRequests(sc *stmtctx.StatementContext, neededHistItems []model.TableItemID, timeout time.Duration) error {
	remainedItems := h.removeHistLoadedColumns(neededHistItems)
	if len(remainedItems) <= 0 {
		return nil
	}
	sc.StatsLoad.Timeout = timeout
	sc.StatsLoad.NeededItems = remainedItems
	sc.StatsLoad.ResultCh = make(chan model.TableItemID, len(remainedItems))
	for _, col := range remainedItems {
		err := h.AppendNeededItem(col, sc.StatsLoad.ResultCh, timeout)
		if err != nil {
			return err
		}
	}
	sc.StatsLoad.LoadStartTime = time.Now()
	return nil
}

// SyncWaitStatsLoad sync waits loading of neededColumns and return false if timeout
func (h *Handle) SyncWaitStatsLoad(sc *stmtctx.StatementContext) bool {
	if len(sc.StatsLoad.NeededItems) <= 0 {
		return true
	}
	defer func() {
		sc.StatsLoad.NeededItems = nil
	}()
	resultCheckMap := map[model.TableItemID]struct{}{}
	for _, col := range sc.StatsLoad.NeededItems {
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
			} else {
				return false
			}
		case <-timer.C:
			metrics.SyncLoadTimeoutCounter.Inc()
			return false
		}
	}
}

// removeHistLoadedColumns removed having-hist columns based on neededColumns and statsCache.
func (h *Handle) removeHistLoadedColumns(neededItems []model.TableItemID) []model.TableItemID {
	statsCache := h.statsCache.Load().(statsCache)
	remainedItems := make([]model.TableItemID, 0, len(neededItems))
	for _, item := range neededItems {
		tbl, ok := statsCache.Get(item.TableID)
		if !ok {
			continue
		}
		if item.IsIndex {
			remainedItems = append(remainedItems, item)
			continue
		}
		colHist, ok := tbl.Columns[item.ID]
		if !ok {
			continue
		}
		if colHist.IsHistNeeded(tbl.Pseudo) {
			remainedItems = append(remainedItems, item)
		}
	}
	return remainedItems
}

// AppendNeededItem appends needed columns/indices to ch, if exists, do not append the duplicated one.
func (h *Handle) AppendNeededItem(item model.TableItemID, resultCh chan model.TableItemID, timeout time.Duration) error {
	toTimout := time.Now().Local().Add(timeout)
	task := &NeededItemTask{TableItemID: item, ToTimeout: toTimout, ResultCh: resultCh}
	return h.writeToChanWithTimeout(h.StatsLoad.NeededItemsCh, task, timeout)
}

var errExit = errors.New("Stop loading since domain is closed")

// StatsReaderContext exported for testing
type StatsReaderContext struct {
	reader      *statsReader
	createdTime time.Time
}

// SubLoadWorker loads hist data for each column
func (h *Handle) SubLoadWorker(ctx sessionctx.Context, exit chan struct{}, exitWg *util.WaitGroupWrapper) {
	readerCtx := &StatsReaderContext{}
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
	// if the last task is not successfully handled in last round for error or panic, pass it to this round to retry
	var lastTask *NeededItemTask
	for {
		task, err := h.HandleOneTask(lastTask, readerCtx, ctx.(sqlexec.RestrictedSQLExecutor), exit)
		lastTask = task
		if err != nil {
			switch err {
			case errExit:
				return
			default:
				time.Sleep(h.Lease() / 10)
				continue
			}
		}
	}
}

// HandleOneTask handles last task if not nil, else handle a new task from chan, and return current task if fail somewhere.
func (h *Handle) HandleOneTask(lastTask *NeededItemTask, readerCtx *StatsReaderContext, ctx sqlexec.RestrictedSQLExecutor, exit chan struct{}) (task *NeededItemTask, err error) {
	defer func() {
		// recover for each task, worker keeps working
		if r := recover(); r != nil {
			logutil.BgLogger().Error("stats loading panicked", zap.Any("error", r), zap.Stack("stack"))
			err = errors.Errorf("stats loading panicked: %v", r)
		}
	}()
	if lastTask == nil {
		task, err = h.drainColTask(exit)
		if err != nil {
			if err != errExit {
				logutil.BgLogger().Error("Fail to drain task for stats loading.", zap.Error(err))
			}
			return task, err
		}
	} else {
		task = lastTask
	}
	return h.handleOneItemTask(task, readerCtx, ctx)
}

func (h *Handle) handleOneItemTask(task *NeededItemTask, readerCtx *StatsReaderContext, ctx sqlexec.RestrictedSQLExecutor) (*NeededItemTask, error) {
	item := task.TableItemID
	oldCache := h.statsCache.Load().(statsCache)
	tbl, ok := oldCache.Get(item.TableID)
	if !ok {
		h.writeToResultChan(task.ResultCh, item)
		return nil, nil
	}
	var err error
	wrapper := &statsWrapper{}
	if item.IsIndex {
		index, ok := tbl.Indices[item.ID]
		if !ok || index.IsFullLoad() {
			h.writeToResultChan(task.ResultCh, item)
			return nil, nil
		}
		wrapper.idx = index
	} else {
		col, ok := tbl.Columns[item.ID]
		if !ok || col.IsFullLoad() {
			h.writeToResultChan(task.ResultCh, item)
			return nil, nil
		}
		wrapper.col = col
	}
	// to avoid duplicated handling in concurrent scenario
	working := h.setWorking(task.TableItemID, task.ResultCh)
	if !working {
		h.writeToResultChan(task.ResultCh, item)
		return nil, nil
	}
	// refresh statsReader to get latest stats
	h.getFreshStatsReader(readerCtx, ctx)
	t := time.Now()
	needUpdate := false
	wrapper, err = h.readStatsForOneItem(item, wrapper, readerCtx.reader)
	if err != nil {
		return task, err
	}
	if item.IsIndex {
		if wrapper.idx != nil {
			needUpdate = true
		}
	} else {
		if wrapper.col != nil {
			needUpdate = true
		}
	}
	metrics.ReadStatsHistogram.Observe(float64(time.Since(t).Milliseconds()))
	if needUpdate && h.updateCachedItem(item, wrapper.col, wrapper.idx) {
		h.writeToResultChan(task.ResultCh, item)
	}
	h.finishWorking(item)
	return nil, nil
}

func (h *Handle) getFreshStatsReader(readerCtx *StatsReaderContext, ctx sqlexec.RestrictedSQLExecutor) {
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
				time.Sleep(h.Lease() / 10)
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

// readStatsForOneItem reads hist for one column/index, TODO load data via kv-get asynchronously
func (h *Handle) readStatsForOneItem(item model.TableItemID, w *statsWrapper, reader *statsReader) (*statsWrapper, error) {
	failpoint.Inject("mockReadStatsForOnePanic", nil)
	failpoint.Inject("mockReadStatsForOneFail", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil, errors.New("gofail ReadStatsForOne error"))
		}
	})
	c := w.col
	index := w.idx
	loadFMSketch := config.GetGlobalConfig().Performance.EnableLoadFMSketch
	var hg *statistics.Histogram
	var err error
	isIndexFlag := int64(0)
	if item.IsIndex {
		isIndexFlag = 1
	}
	if item.IsIndex {
		hg, err = h.histogramFromStorage(reader, item.TableID, item.ID, types.NewFieldType(mysql.TypeBlob), index.Histogram.NDV, int(isIndexFlag), index.LastUpdateVersion, index.NullCount, index.TotColSize, index.Correlation)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		hg, err = h.histogramFromStorage(reader, item.TableID, item.ID, &c.Info.FieldType, c.Histogram.NDV, int(isIndexFlag), c.LastUpdateVersion, c.NullCount, c.TotColSize, c.Correlation)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	var cms *statistics.CMSketch
	var topN *statistics.TopN
	cms, topN, err = h.cmSketchAndTopNFromStorage(reader, item.TableID, isIndexFlag, item.ID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var fms *statistics.FMSketch
	if loadFMSketch {
		fms, err = h.fmSketchFromStorage(reader, item.TableID, isIndexFlag, item.ID)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	rows, _, err := reader.read("select stats_ver from mysql.stats_histograms where is_index = %? and table_id = %? and hist_id = %?", isIndexFlag, item.TableID, item.ID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rows) == 0 {
		logutil.BgLogger().Error("fail to get stats version for this histogram", zap.Int64("table_id", item.TableID), zap.Int64("hist_id", item.ID), zap.Bool("is_index", item.IsIndex))
		return nil, errors.Trace(errors.New(fmt.Sprintf("fail to get stats version for this histogram, table_id:%v, hist_id:%v, is_index:%v", item.TableID, item.ID, item.IsIndex)))
	}
	if item.IsIndex {
		idxHist := &statistics.Index{
			Histogram: *hg,
			CMSketch:  cms,
			TopN:      topN,
			FMSketch:  fms,
			Info:      index.Info,
			ErrorRate: index.ErrorRate,
			StatsVer:  rows[0].GetInt64(0), Flag: index.Flag,
			PhysicalID:        index.PhysicalID,
			StatsLoadedStatus: statistics.NewStatsFullLoadStatus(),
		}
		index.LastAnalyzePos.Copy(&idxHist.LastAnalyzePos)
		w.idx = idxHist
	} else {
		colHist := &statistics.Column{
			PhysicalID:        item.TableID,
			Histogram:         *hg,
			Info:              c.Info,
			CMSketch:          cms,
			TopN:              topN,
			FMSketch:          fms,
			IsHandle:          c.IsHandle,
			StatsVer:          rows[0].GetInt64(0),
			StatsLoadedStatus: statistics.NewStatsFullLoadStatus(),
		}
		// Column.Count is calculated by Column.TotalRowCount(). Hence, we don't set Column.Count when initializing colHist.
		colHist.Count = int64(colHist.TotalRowCount())
		w.col = colHist
	}
	return w, nil
}

// drainColTask will hang until a column task can return, and either task or error will be returned.
func (h *Handle) drainColTask(exit chan struct{}) (*NeededItemTask, error) {
	// select NeededColumnsCh firstly, if no task, then select TimeoutColumnsCh
	for {
		select {
		case <-exit:
			return nil, errExit
		case task, ok := <-h.StatsLoad.NeededItemsCh:
			if !ok {
				return nil, errors.New("drainColTask: cannot read from NeededColumnsCh, maybe the chan is closed")
			}
			// if the task has already timeout, no sql is sync-waiting for it,
			// so do not handle it just now, put it to another channel with lower priority
			if time.Now().After(task.ToTimeout) {
				h.writeToTimeoutChan(h.StatsLoad.TimeoutItemsCh, task)
				continue
			}
			return task, nil
		case task, ok := <-h.StatsLoad.TimeoutItemsCh:
			select {
			case <-exit:
				return nil, errExit
			case task0, ok0 := <-h.StatsLoad.NeededItemsCh:
				if !ok0 {
					return nil, errors.New("drainColTask: cannot read from NeededColumnsCh, maybe the chan is closed")
				}
				// send task back to TimeoutColumnsCh and return the task drained from NeededColumnsCh
				h.writeToTimeoutChan(h.StatsLoad.TimeoutItemsCh, task)
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

// writeToTimeoutChan writes in a nonblocking way, and if the channel queue is full, it's ok to drop the task.
func (h *Handle) writeToTimeoutChan(taskCh chan *NeededItemTask, task *NeededItemTask) {
	select {
	case taskCh <- task:
	default:
	}
}

// writeToChanWithTimeout writes a task to a channel and blocks until timeout.
func (h *Handle) writeToChanWithTimeout(taskCh chan *NeededItemTask, task *NeededItemTask, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case taskCh <- task:
	case <-timer.C:
		return errors.New("Channel is full and timeout writing to channel")
	}
	return nil
}

// writeToResultChan safe-writes with panic-recover so one write-fail will not have big impact.
func (h *Handle) writeToResultChan(resultCh chan model.TableItemID, rs model.TableItemID) {
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Error("writeToResultChan panicked", zap.Any("error", r), zap.Stack("stack"))
		}
	}()
	select {
	case resultCh <- rs:
	default:
	}
}

// updateCachedItem updates the column/index hist to global statsCache.
func (h *Handle) updateCachedItem(item model.TableItemID, colHist *statistics.Column, idxHist *statistics.Index) (updated bool) {
	h.StatsLoad.Lock()
	defer h.StatsLoad.Unlock()
	// Reload the latest stats cache, otherwise the `updateStatsCache` may fail with high probability, because functions
	// like `GetPartitionStats` called in `fmSketchFromStorage` would have modified the stats cache already.
	oldCache := h.statsCache.Load().(statsCache)
	tbl, ok := oldCache.Get(item.TableID)
	if !ok {
		return true
	}
	if !item.IsIndex && colHist != nil {
		c, ok := tbl.Columns[item.ID]
		if !ok || c.IsFullLoad() {
			return true
		}
		tbl = tbl.Copy()
		tbl.Columns[c.ID] = colHist
	} else if item.IsIndex && idxHist != nil {
		index, ok := tbl.Indices[item.ID]
		if !ok || index.IsFullLoad() {
			return true
		}
		tbl = tbl.Copy()
		tbl.Indices[item.ID] = idxHist
	}
	return h.updateStatsCache(oldCache.update([]*statistics.Table{tbl}, nil, oldCache.version, WithTableStatsByQuery()))
}

func (h *Handle) setWorking(item model.TableItemID, resultCh chan model.TableItemID) bool {
	h.StatsLoad.Lock()
	defer h.StatsLoad.Unlock()
	chList, ok := h.StatsLoad.WorkingColMap[item]
	if ok {
		if chList[0] == resultCh {
			return true // just return for duplicate setWorking
		}
		h.StatsLoad.WorkingColMap[item] = append(chList, resultCh)
		return false
	}
	chList = []chan model.TableItemID{}
	chList = append(chList, resultCh)
	h.StatsLoad.WorkingColMap[item] = chList
	return true
}

func (h *Handle) finishWorking(col model.TableItemID) {
	h.StatsLoad.Lock()
	defer h.StatsLoad.Unlock()
	if chList, ok := h.StatsLoad.WorkingColMap[col]; ok {
		list := chList[1:]
		for _, ch := range list {
			h.writeToResultChan(ch, col)
		}
	}
	delete(h.StatsLoad.WorkingColMap, col)
}
