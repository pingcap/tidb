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

package syncload

import (
	"fmt"
	"math/rand"
	"runtime"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

// RetryCount is the max retry count for a sync load task.
const RetryCount = 3

// GetSyncLoadConcurrencyByCPU returns the concurrency of sync load by CPU.
func GetSyncLoadConcurrencyByCPU() int {
	core := runtime.GOMAXPROCS(0)
	if core <= 8 {
		return 5
	} else if core <= 16 {
		return 6
	} else if core <= 32 {
		return 8
	}
	return 10
}

type statsSyncLoad struct {
	statsHandle statstypes.StatsHandle
	StatsLoad   statstypes.StatsLoad
}

var globalStatsSyncLoadSingleFlight singleflight.Group

// NewStatsSyncLoad creates a new StatsSyncLoad.
func NewStatsSyncLoad(statsHandle statstypes.StatsHandle) statstypes.StatsSyncLoad {
	s := &statsSyncLoad{statsHandle: statsHandle}
	cfg := config.GetGlobalConfig()
	s.StatsLoad.NeededItemsCh = make(chan *statstypes.NeededItemTask, cfg.Performance.StatsLoadQueueSize)
	s.StatsLoad.TimeoutItemsCh = make(chan *statstypes.NeededItemTask, cfg.Performance.StatsLoadQueueSize)
	return s
}

type statsWrapper struct {
	colInfo *model.ColumnInfo
	idxInfo *model.IndexInfo
	col     *statistics.Column
	idx     *statistics.Index
}

// SendLoadRequests send neededColumns requests
func (s *statsSyncLoad) SendLoadRequests(sc *stmtctx.StatementContext, neededHistItems []model.StatsLoadItem, timeout time.Duration) error {
	remainedItems := s.removeHistLoadedColumns(neededHistItems)

	failpoint.Inject("assertSyncLoadItems", func(val failpoint.Value) {
		if sc.OptimizeTracer != nil {
			count := val.(int)
			if len(remainedItems) != count {
				panic("remained items count wrong")
			}
		}
	})

	if len(remainedItems) <= 0 {
		return nil
	}
	sc.StatsLoad.Timeout = timeout
	sc.StatsLoad.NeededItems = remainedItems
	sc.StatsLoad.ResultCh = make([]<-chan singleflight.Result, 0, len(remainedItems))
	for _, item := range remainedItems {
		localItem := item
		resultCh := globalStatsSyncLoadSingleFlight.DoChan(localItem.Key(), func() (any, error) {
			timer := time.NewTimer(timeout)
			defer timer.Stop()
			task := &statstypes.NeededItemTask{
				Item:      localItem,
				ToTimeout: time.Now().Local().Add(timeout),
				ResultCh:  make(chan stmtctx.StatsLoadResult, 1),
			}
			select {
			case s.StatsLoad.NeededItemsCh <- task:
				result, ok := <-task.ResultCh
				intest.Assert(ok, "task.ResultCh cannot be closed")
				return result, nil
			case <-timer.C:
				return nil, errors.New("sync load stats channel is full and timeout sending task to channel")
			}
		})
		sc.StatsLoad.ResultCh = append(sc.StatsLoad.ResultCh, resultCh)
	}
	sc.StatsLoad.LoadStartTime = time.Now()
	return nil
}

// SyncWaitStatsLoad sync waits loading of neededColumns and return false if timeout
func (*statsSyncLoad) SyncWaitStatsLoad(sc *stmtctx.StatementContext) error {
	if len(sc.StatsLoad.NeededItems) <= 0 {
		return nil
	}
	var errorMsgs []string
	defer func() {
		if len(errorMsgs) > 0 {
			logutil.BgLogger().Warn("SyncWaitStatsLoad meets error",
				zap.Strings("errors", errorMsgs))
		}
		sc.StatsLoad.NeededItems = nil
	}()
	resultCheckMap := map[model.TableItemID]struct{}{}
	for _, col := range sc.StatsLoad.NeededItems {
		resultCheckMap[col.TableItemID] = struct{}{}
	}
	metrics.SyncLoadCounter.Inc()
	timer := time.NewTimer(sc.StatsLoad.Timeout)
	defer timer.Stop()
	for _, resultCh := range sc.StatsLoad.ResultCh {
		select {
		case result, ok := <-resultCh:
			if !ok {
				return errors.New("sync load stats channel closed unexpectedly")
			}
			// this error is from statsSyncLoad.SendLoadRequests which start to task and send task into worker,
			// not the stats loading error
			if result.Err != nil {
				errorMsgs = append(errorMsgs, result.Err.Error())
			} else {
				val := result.Val.(stmtctx.StatsLoadResult)
				// this error is from the stats loading error
				if val.HasError() {
					errorMsgs = append(errorMsgs, val.ErrorMsg())
				}
				delete(resultCheckMap, val.Item)
			}
		case <-timer.C:
			metrics.SyncLoadTimeoutCounter.Inc()
			return errors.New("sync load stats timeout")
		}
	}
	if len(resultCheckMap) == 0 {
		metrics.SyncLoadHistogram.Observe(float64(time.Since(sc.StatsLoad.LoadStartTime).Milliseconds()))
		return nil
	}
	return nil
}

// removeHistLoadedColumns removed having-hist columns based on neededColumns and statsCache.
func (s *statsSyncLoad) removeHistLoadedColumns(neededItems []model.StatsLoadItem) []model.StatsLoadItem {
	remainedItems := make([]model.StatsLoadItem, 0, len(neededItems))
	for _, item := range neededItems {
		tbl, ok := s.statsHandle.Get(item.TableID)
		if !ok {
			continue
		}
		if item.IsIndex {
			_, loadNeeded := tbl.IndexIsLoadNeeded(item.ID)
			if loadNeeded {
				remainedItems = append(remainedItems, item)
			}
			continue
		}
		_, loadNeeded, _ := tbl.ColumnIsLoadNeeded(item.ID, item.FullLoad)
		if loadNeeded {
			remainedItems = append(remainedItems, item)
		}
	}
	return remainedItems
}

// AppendNeededItem appends needed columns/indices to ch, it is only used for test
func (s *statsSyncLoad) AppendNeededItem(task *statstypes.NeededItemTask, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case s.StatsLoad.NeededItemsCh <- task:
	case <-timer.C:
		return errors.New("Channel is full and timeout writing to channel")
	}
	return nil
}

var errExit = errors.New("Stop loading since domain is closed")

// SubLoadWorker loads hist data for each column
func (s *statsSyncLoad) SubLoadWorker(sctx sessionctx.Context, exit chan struct{}, exitWg *util.WaitGroupEnhancedWrapper) {
	defer func() {
		exitWg.Done()
		logutil.BgLogger().Info("SubLoadWorker exited.")
	}()
	// if the last task is not successfully handled in last round for error or panic, pass it to this round to retry
	var lastTask *statstypes.NeededItemTask
	for {
		task, err := s.HandleOneTask(sctx, lastTask, exit)
		lastTask = task
		if err != nil {
			switch err {
			case errExit:
				return
			default:
				// To avoid the thundering herd effect
				// thundering herd effect: Everyone tries to retry a large number of requests simultaneously when a problem occurs.
				r := rand.Intn(500)
				time.Sleep(s.statsHandle.Lease()/10 + time.Duration(r)*time.Microsecond)
				continue
			}
		}
	}
}

// HandleOneTask handles last task if not nil, else handle a new task from chan, and return current task if fail somewhere.
//   - If the task is handled successfully, return nil, nil.
//   - If the task is timeout, return the task and nil. The caller should retry the timeout task without sleep.
//   - If the task is failed, return the task, error. The caller should retry the timeout task with sleep.
func (s *statsSyncLoad) HandleOneTask(sctx sessionctx.Context, lastTask *statstypes.NeededItemTask, exit chan struct{}) (task *statstypes.NeededItemTask, err error) {
	defer func() {
		// recover for each task, worker keeps working
		if r := recover(); r != nil {
			logutil.BgLogger().Error("stats loading panicked", zap.Any("error", r), zap.Stack("stack"))
			err = errors.Errorf("stats loading panicked: %v", r)
		}
	}()
	if lastTask == nil {
		task, err = s.drainColTask(sctx, exit)
		if err != nil {
			if err != errExit {
				logutil.BgLogger().Error("Fail to drain task for stats loading.", zap.Error(err))
			}
			return task, err
		}
	} else {
		task = lastTask
	}
	result := stmtctx.StatsLoadResult{Item: task.Item.TableItemID}
	err = s.handleOneItemTask(task)
	if err == nil {
		task.ResultCh <- result
		return nil, nil
	}
	if !isVaildForRetry(task) {
		result.Error = err
		task.ResultCh <- result
		return nil, nil
	}
	return task, err
}

func isVaildForRetry(task *statstypes.NeededItemTask) bool {
	task.Retry++
	return task.Retry <= RetryCount
}

func (s *statsSyncLoad) handleOneItemTask(task *statstypes.NeededItemTask) (err error) {
	se, err := s.statsHandle.SPool().Get()
	if err != nil {
		return err
	}
	sctx := se.(sessionctx.Context)
	sctx.GetSessionVars().StmtCtx.Priority = mysql.HighPriority
	defer func() {
		// recover for each task, worker keeps working
		if r := recover(); r != nil {
			logutil.BgLogger().Error("handleOneItemTask panicked", zap.Any("recover", r), zap.Stack("stack"))
			err = errors.Errorf("stats loading panicked: %v", r)
		}
		if err == nil { // only recycle when no error
			sctx.GetSessionVars().StmtCtx.Priority = mysql.NoPriority
			s.statsHandle.SPool().Put(se)
		}
	}()
	item := task.Item.TableItemID
	tbl, ok := s.statsHandle.Get(item.TableID)
	if !ok {
		return nil
	}
	wrapper := &statsWrapper{}
	if item.IsIndex {
		index, loadNeeded := tbl.IndexIsLoadNeeded(item.ID)
		if !loadNeeded {
			return nil
		}
		if index != nil {
			wrapper.idxInfo = index.Info
		} else {
			wrapper.idxInfo = tbl.ColAndIdxExistenceMap.GetIndex(item.ID)
		}
	} else {
		col, loadNeeded, analyzed := tbl.ColumnIsLoadNeeded(item.ID, task.Item.FullLoad)
		if !loadNeeded {
			return nil
		}
		if col != nil {
			wrapper.colInfo = col.Info
		} else {
			wrapper.colInfo = tbl.ColAndIdxExistenceMap.GetCol(item.ID)
		}
		// If this column is not analyzed yet and we don't have it in memory.
		// We create a fake one for the pseudo estimation.
		if loadNeeded && !analyzed {
			wrapper.col = &statistics.Column{
				PhysicalID: item.TableID,
				Info:       wrapper.colInfo,
				Histogram:  *statistics.NewHistogram(item.ID, 0, 0, 0, &wrapper.colInfo.FieldType, 0, 0),
				IsHandle:   tbl.IsPkIsHandle && mysql.HasPriKeyFlag(wrapper.colInfo.GetFlag()),
			}
			s.updateCachedItem(item, wrapper.col, wrapper.idx, task.Item.FullLoad)
			return nil
		}
	}
	t := time.Now()
	needUpdate := false
	wrapper, err = s.readStatsForOneItem(sctx, item, wrapper, tbl.IsPkIsHandle, task.Item.FullLoad)
	if err != nil {
		return err
	}
	if item.IsIndex {
		if wrapper.idxInfo != nil {
			needUpdate = true
		}
	} else {
		if wrapper.colInfo != nil {
			needUpdate = true
		}
	}
	metrics.ReadStatsHistogram.Observe(float64(time.Since(t).Milliseconds()))
	if needUpdate {
		s.updateCachedItem(item, wrapper.col, wrapper.idx, task.Item.FullLoad)
	}
	return nil
}

// readStatsForOneItem reads hist for one column/index, TODO load data via kv-get asynchronously
func (*statsSyncLoad) readStatsForOneItem(sctx sessionctx.Context, item model.TableItemID, w *statsWrapper, isPkIsHandle bool, fullLoad bool) (*statsWrapper, error) {
	failpoint.Inject("mockReadStatsForOnePanic", nil)
	failpoint.Inject("mockReadStatsForOneFail", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil, errors.New("gofail ReadStatsForOne error"))
		}
	})
	loadFMSketch := config.GetGlobalConfig().Performance.EnableLoadFMSketch
	var hg *statistics.Histogram
	var err error
	isIndexFlag := int64(0)
	hg, lastAnalyzePos, statsVer, flag, err := storage.HistMetaFromStorage(sctx, &item, w.colInfo)
	if err != nil {
		return nil, err
	}
	if hg == nil {
		logutil.BgLogger().Error("fail to get hist meta for this histogram, possibly a deleted one", zap.Int64("table_id", item.TableID),
			zap.Int64("hist_id", item.ID), zap.Bool("is_index", item.IsIndex))
		return nil, errors.Trace(fmt.Errorf("fail to get hist meta for this histogram, table_id:%v, hist_id:%v, is_index:%v", item.TableID, item.ID, item.IsIndex))
	}
	if item.IsIndex {
		isIndexFlag = 1
	}
	var cms *statistics.CMSketch
	var topN *statistics.TopN
	var fms *statistics.FMSketch
	if fullLoad {
		if item.IsIndex {
			hg, err = storage.HistogramFromStorage(sctx, item.TableID, item.ID, types.NewFieldType(mysql.TypeBlob), hg.NDV, int(isIndexFlag), hg.LastUpdateVersion, hg.NullCount, hg.TotColSize, hg.Correlation)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			hg, err = storage.HistogramFromStorage(sctx, item.TableID, item.ID, &w.colInfo.FieldType, hg.NDV, int(isIndexFlag), hg.LastUpdateVersion, hg.NullCount, hg.TotColSize, hg.Correlation)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		cms, topN, err = storage.CMSketchAndTopNFromStorage(sctx, item.TableID, isIndexFlag, item.ID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if loadFMSketch {
			fms, err = storage.FMSketchFromStorage(sctx, item.TableID, isIndexFlag, item.ID)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}
	if item.IsIndex {
		idxHist := &statistics.Index{
			Histogram:  *hg,
			CMSketch:   cms,
			TopN:       topN,
			FMSketch:   fms,
			Info:       w.idxInfo,
			StatsVer:   statsVer,
			Flag:       flag,
			PhysicalID: item.TableID,
		}
		if statsVer != statistics.Version0 {
			if fullLoad {
				idxHist.StatsLoadedStatus = statistics.NewStatsFullLoadStatus()
			} else {
				idxHist.StatsLoadedStatus = statistics.NewStatsAllEvictedStatus()
			}
		}
		lastAnalyzePos.Copy(&idxHist.LastAnalyzePos)
		w.idx = idxHist
	} else {
		colHist := &statistics.Column{
			PhysicalID: item.TableID,
			Histogram:  *hg,
			Info:       w.colInfo,
			CMSketch:   cms,
			TopN:       topN,
			FMSketch:   fms,
			IsHandle:   isPkIsHandle && mysql.HasPriKeyFlag(w.colInfo.GetFlag()),
			StatsVer:   statsVer,
		}
		if colHist.StatsAvailable() {
			if fullLoad {
				colHist.StatsLoadedStatus = statistics.NewStatsFullLoadStatus()
			} else {
				colHist.StatsLoadedStatus = statistics.NewStatsAllEvictedStatus()
			}
		}
		w.col = colHist
	}
	return w, nil
}

// drainColTask will hang until a column task can return, and either task or error will be returned.
func (s *statsSyncLoad) drainColTask(sctx sessionctx.Context, exit chan struct{}) (*statstypes.NeededItemTask, error) {
	// select NeededColumnsCh firstly, if no task, then select TimeoutColumnsCh
	for {
		select {
		case <-exit:
			return nil, errExit
		case task, ok := <-s.StatsLoad.NeededItemsCh:
			if !ok {
				return nil, errors.New("drainColTask: cannot read from NeededColumnsCh, maybe the chan is closed")
			}
			// if the task has already timeout, no sql is sync-waiting for it,
			// so do not handle it just now, put it to another channel with lower priority
			if time.Now().After(task.ToTimeout) {
				task.ToTimeout.Add(time.Duration(sctx.GetSessionVars().StatsLoadSyncWait.Load()) * time.Microsecond)
				s.writeToTimeoutChan(s.StatsLoad.TimeoutItemsCh, task)
				continue
			}
			return task, nil
		case task, ok := <-s.StatsLoad.TimeoutItemsCh:
			select {
			case <-exit:
				return nil, errExit
			case task0, ok0 := <-s.StatsLoad.NeededItemsCh:
				if !ok0 {
					return nil, errors.New("drainColTask: cannot read from NeededColumnsCh, maybe the chan is closed")
				}
				// send task back to TimeoutColumnsCh and return the task drained from NeededColumnsCh
				s.writeToTimeoutChan(s.StatsLoad.TimeoutItemsCh, task)
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
func (*statsSyncLoad) writeToTimeoutChan(taskCh chan *statstypes.NeededItemTask, task *statstypes.NeededItemTask) {
	select {
	case taskCh <- task:
	default:
	}
}

// writeToChanWithTimeout writes a task to a channel and blocks until timeout.
func (*statsSyncLoad) writeToChanWithTimeout(taskCh chan *statstypes.NeededItemTask, task *statstypes.NeededItemTask, timeout time.Duration) error {
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
func (*statsSyncLoad) writeToResultChan(resultCh chan stmtctx.StatsLoadResult, rs stmtctx.StatsLoadResult) {
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
func (s *statsSyncLoad) updateCachedItem(item model.TableItemID, colHist *statistics.Column, idxHist *statistics.Index, fullLoaded bool) (updated bool) {
	s.StatsLoad.Lock()
	defer s.StatsLoad.Unlock()
	// Reload the latest stats cache, otherwise the `updateStatsCache` may fail with high probability, because functions
	// like `GetPartitionStats` called in `fmSketchFromStorage` would have modified the stats cache already.
	tbl, ok := s.statsHandle.Get(item.TableID)
	if !ok {
		return false
	}
	if !item.IsIndex && colHist != nil {
		c, ok := tbl.Columns[item.ID]
		// - If the stats is fully loaded,
		// - If the stats is meta-loaded and we also just need the meta.
		if ok && (c.IsFullLoad() || !fullLoaded) {
			return false
		}
		tbl = tbl.Copy()
		tbl.Columns[item.ID] = colHist
		// If the column is analyzed we refresh the map for the possible change.
		if colHist.StatsAvailable() {
			tbl.ColAndIdxExistenceMap.InsertCol(item.ID, colHist.Info, true)
		}
		// All the objects shares the same stats version. Update it here.
		if colHist.StatsVer != statistics.Version0 {
			tbl.StatsVer = statistics.Version0
		}
	} else if item.IsIndex && idxHist != nil {
		index, ok := tbl.Indices[item.ID]
		// - If the stats is fully loaded,
		// - If the stats is meta-loaded and we also just need the meta.
		if ok && (index.IsFullLoad() || !fullLoaded) {
			return true
		}
		tbl = tbl.Copy()
		tbl.Indices[item.ID] = idxHist
		// If the index is analyzed we refresh the map for the possible change.
		if idxHist.IsAnalyzed() {
			tbl.ColAndIdxExistenceMap.InsertIndex(item.ID, idxHist.Info, true)
			// All the objects shares the same stats version. Update it here.
			tbl.StatsVer = statistics.Version0
		}
	}
	s.statsHandle.UpdateStatsCache([]*statistics.Table{tbl}, nil)
	return true
}
