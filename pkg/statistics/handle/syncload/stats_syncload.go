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
	stderrors "errors"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

// RetryCount is the max retry count for a sync load task.
// TODO(hawkingrei): There is no point in retrying sync load,
// because there will be other tasks requesting this task at the same time.
// As long as the subsequent tasks are normal, it will be fine. Too many retries
// will only cause congestion and delays
const RetryCount = 1

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

// statsSyncLoad is used to load statistics synchronously when needed by SQL queries.
//
// It maintains two channels for handling statistics load tasks:
// - NeededItemsCh: High priority channel for tasks that haven't timed out yet (Higher priority)
// - TimeoutItemsCh: Lower priority channel for tasks that exceeded their timeout (Lower priority)
//
// The main workflow:
// 1. collect_column_stats_usage rule requests statistics via SendLoadRequests
// 2. Tasks are created and placed in channels
// 3. Worker goroutines pick up tasks from channels
// 4. Statistics are loaded from storage
// 5. Loaded statistics are cached via updateCachedItem for future use
// 6. Results are checked and stats are used in the SQL query
//
// It uses singleflight pattern to deduplicate concurrent requests for the same statistics.
// Requests that exceed their timeout are moved to a lower priority channel to be processed
// when there are no urgent requests.
type statsSyncLoad struct {
	statsHandle    statstypes.StatsHandle
	neededItemsCh  chan *statstypes.NeededItemTask
	timeoutItemsCh chan *statstypes.NeededItemTask
	// This mutex protects the statsCache from concurrent modifications by multiple workers.
	// Since multiple workers may update the statsCache for the same table simultaneously,
	// the mutex ensures thread-safety during these updates.
	mutexForStatsCache sync.Mutex
}

var globalStatsSyncLoadSingleFlight singleflight.Group

// NewStatsSyncLoad creates a new StatsSyncLoad.
func NewStatsSyncLoad(statsHandle statstypes.StatsHandle) statstypes.StatsSyncLoad {
	s := &statsSyncLoad{statsHandle: statsHandle}
	cfg := config.GetGlobalConfig()
	s.neededItemsCh = make(chan *statstypes.NeededItemTask, cfg.Performance.StatsLoadQueueSize)
	s.timeoutItemsCh = make(chan *statstypes.NeededItemTask, cfg.Performance.StatsLoadQueueSize)
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
			case s.neededItemsCh <- task:
				metrics.SyncLoadDedupCounter.Inc()
				select {
				case <-timer.C:
					return nil, errors.New("sync load took too long to return")
				case result, ok := <-task.ResultCh:
					intest.Assert(ok, "task.ResultCh cannot be closed")
					return result, nil
				}
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
			statslogutil.StatsLogger().Warn("SyncWaitStatsLoad meets error",
				zap.Strings("errors", errorMsgs))
		}
		sc.StatsLoad.NeededItems = nil
	}()
	resultCheckMap := map[model.TableItemID]struct{}{}
	for _, col := range sc.StatsLoad.NeededItems {
		resultCheckMap[col.TableItemID] = struct{}{}
	}
	timer := time.NewTimer(sc.StatsLoad.Timeout)
	defer timer.Stop()
	for _, resultCh := range sc.StatsLoad.ResultCh {
		select {
		case result, ok := <-resultCh:
			metrics.SyncLoadCounter.Inc()
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
			metrics.SyncLoadCounter.Inc()
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
	case s.neededItemsCh <- task:
	case <-timer.C:
		return errors.New("Channel is full and timeout writing to channel")
	}
	return nil
}

var errExit = errors.New("Stop loading since domain is closed")

// SubLoadWorker loads hist data for each column
func (s *statsSyncLoad) SubLoadWorker(exit chan struct{}, exitWg *util.WaitGroupEnhancedWrapper) {
	defer func() {
		exitWg.Done()
		statslogutil.StatsLogger().Info("SubLoadWorker: exited.")
	}()
	// if the last task is not successfully handled in last round for error or panic, pass it to this round to retry
	var lastTask *statstypes.NeededItemTask
	for {
		task, err := s.HandleOneTask(lastTask, exit)
		lastTask = task
		if err != nil {
			switch err {
			case errExit:
				statslogutil.StatsLogger().Info("SubLoadWorker: exits now because the domain is closed.")
				return
			default:
				const msg = "SubLoadWorker: failed to handle one task"
				if task != nil {
					statslogutil.StatsErrVerboseSampleLogger().Warn(msg,
						zap.Error(err),
						zap.String("task", task.Item.Key()),
						zap.Int("retry", task.Retry),
					)
				} else {
					statslogutil.StatsErrVerboseSampleLogger().Warn(msg,
						zap.Error(err),
					)
				}
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
func (s *statsSyncLoad) HandleOneTask(lastTask *statstypes.NeededItemTask, exit chan struct{}) (task *statstypes.NeededItemTask, err error) {
	defer func() {
		// recover for each task, worker keeps working
		if r := recover(); r != nil {
			statslogutil.StatsLogger().Error("stats loading panicked", zap.Any("error", r), zap.Stack("stack"))
			err = errors.Errorf("stats loading panicked: %v", r)
		}
	}()
	if lastTask == nil {
		task, err = s.drainColTask(exit)
		if err != nil {
			if err != errExit {
				statslogutil.StatsLogger().Error("Fail to drain task for stats loading.", zap.Error(err))
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
	defer func() {
		// recover for each task, worker keeps working
		if r := recover(); r != nil {
			statslogutil.StatsLogger().Error("handleOneItemTask panicked", zap.Any("recover", r), zap.Stack("stack"))
			err = errors.Errorf("stats loading panicked: %v", r)
		}
	}()

	return s.statsHandle.SPool().WithSession(func(se *syssession.Session) error {
		return se.WithSessionContext(func(sctx sessionctx.Context) error {
			sctx.GetSessionVars().StmtCtx.Priority = mysql.HighPriority
			defer func() {
				sctx.GetSessionVars().StmtCtx.Priority = mysql.NoPriority
			}()
			return s.handleOneItemTaskWithSCtx(sctx, task)
		})
	})
}

// handleOneItemTaskWithSCtx contains the core business logic for handling one item task.
// This method preserves git blame history by keeping the original logic intact.
func (s *statsSyncLoad) handleOneItemTaskWithSCtx(sctx sessionctx.Context, task *statstypes.NeededItemTask) error {
	var skipTypes map[string]struct{}
	val, err := sctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(vardef.TiDBAnalyzeSkipColumnTypes)
	if err != nil {
		statslogutil.StatsLogger().Warn("failed to get global variable", zap.Error(err))
	} else {
		skipTypes = variable.ParseAnalyzeSkipColumnTypes(val)
	}

	item := task.Item.TableItemID
	statsTbl, ok := s.statsHandle.Get(item.TableID)

	if !ok {
		return nil
	}
	is := sctx.GetLatestInfoSchema().(infoschema.InfoSchema)
	tbl, ok := s.statsHandle.TableInfoByID(is, item.TableID)
	if !ok {
		return nil
	}
	tblInfo := tbl.Meta()
	isPkIsHandle := tblInfo.PKIsHandle
	wrapper := &statsWrapper{}
	if item.IsIndex {
		index, loadNeeded := statsTbl.IndexIsLoadNeeded(item.ID)
		if !loadNeeded {
			return nil
		}
		if index != nil {
			wrapper.idxInfo = index.Info
		} else {
			wrapper.idxInfo = tblInfo.FindIndexByID(item.ID)
		}
	} else {
		col, loadNeeded, analyzed := statsTbl.ColumnIsLoadNeeded(item.ID, task.Item.FullLoad)
		if !loadNeeded {
			return nil
		}
		if col != nil {
			wrapper.colInfo = col.Info
		} else {
			// Now, we cannot init the column info in the ColAndIdxExistenceMap when to disable lite-init-stats.
			// so we have to get the column info from the domain.
			wrapper.colInfo = tblInfo.GetColumnByID(item.ID)
		}
		if skipTypes != nil {
			_, skip := skipTypes[types.TypeToStr(wrapper.colInfo.FieldType.GetType(), wrapper.colInfo.FieldType.GetCharset())]
			if skip {
				return nil
			}
		}

		// If this column is not analyzed yet and we don't have it in memory.
		// We create a fake one for the pseudo estimation.
		// Otherwise, it will trigger the sync/async load again, even if the column has not been analyzed.
		if !analyzed {
			wrapper.col = statistics.EmptyColumn(item.TableID, isPkIsHandle, wrapper.colInfo)
			s.updateCachedItem(item, wrapper.col, wrapper.idx, task.Item.FullLoad)
			return nil
		}
	}
	failpoint.Eval(_curpkg_("handleOneItemTaskPanic"))
	t := time.Now()
	needUpdate := false
	wrapper, err = s.readStatsForOneItem(sctx, item, wrapper, isPkIsHandle, task.Item.FullLoad)
	if stderrors.Is(err, errGetHistMeta) {
		return nil
	}
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

var errGetHistMeta = errors.New("fail to get hist meta")

// readStatsForOneItem reads hist for one column/index, TODO load data via kv-get asynchronously
func (*statsSyncLoad) readStatsForOneItem(sctx sessionctx.Context, item model.TableItemID, w *statsWrapper, isPkIsHandle bool, fullLoad bool) (*statsWrapper, error) {
	failpoint.Eval(_curpkg_("mockReadStatsForOnePanic"))
	if val, _err_ := failpoint.Eval(_curpkg_("mockReadStatsForOneFail")); _err_ == nil {
		if val.(bool) {
			return nil, errors.New("gofail ReadStatsForOne error")
		}
	}
	var hg *statistics.Histogram
	var err error
	isIndexFlag := int64(0)
	hg, statsVer, err := storage.HistMetaFromStorageWithHighPriority(sctx, &item, w.colInfo)
	if err != nil {
		return nil, err
	}
	if hg == nil {
		statslogutil.StatsSampleLogger().Warn(
			"Histogram not found, possibly due to DDL event is not handled, please consider analyze the table",
			zap.Int64("tableID", item.TableID),
			zap.Int64("histID", item.ID),
			zap.Bool("isIndex", item.IsIndex),
		)
		return nil, errGetHistMeta
	}
	if item.IsIndex {
		isIndexFlag = 1
	}
	var cms *statistics.CMSketch
	var topN *statistics.TopN
	if fullLoad {
		if item.IsIndex {
			hg, err = storage.HistogramFromStorageWithPriority(sctx, item.TableID, item.ID, types.NewFieldType(mysql.TypeBlob), hg.NDV, int(isIndexFlag), hg.LastUpdateVersion, hg.NullCount, hg.TotColSize, hg.Correlation, kv.PriorityHigh)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			hg, err = storage.HistogramFromStorageWithPriority(sctx, item.TableID, item.ID, &w.colInfo.FieldType, hg.NDV, int(isIndexFlag), hg.LastUpdateVersion, hg.NullCount, hg.TotColSize, hg.Correlation, kv.PriorityHigh)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		cms, topN, err = storage.CMSketchAndTopNFromStorageWithHighPriority(sctx, item.TableID, isIndexFlag, item.ID, statsVer)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if item.IsIndex {
		idxHist := &statistics.Index{
			Histogram:  *hg,
			CMSketch:   cms,
			TopN:       topN,
			Info:       w.idxInfo,
			StatsVer:   statsVer,
			PhysicalID: item.TableID,
		}
		if statsVer != statistics.Version0 {
			if fullLoad {
				idxHist.StatsLoadedStatus = statistics.NewStatsFullLoadStatus()
			} else {
				idxHist.StatsLoadedStatus = statistics.NewStatsAllEvictedStatus()
			}
		}
		w.idx = idxHist
	} else {
		colHist := &statistics.Column{
			PhysicalID: item.TableID,
			Histogram:  *hg,
			Info:       w.colInfo,
			CMSketch:   cms,
			TopN:       topN,
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

// drainColTask will hang until a task can return, and either task or error will be returned.
// The task will be drained from NeededItemsCh first, if no task, then TimeoutItemsCh.
func (s *statsSyncLoad) drainColTask(exit chan struct{}) (*statstypes.NeededItemTask, error) {
	// select NeededItemsCh firstly, if no task, then select TimeoutColumnsCh
	for {
		select {
		case <-exit:
			return nil, errExit
		case task, ok := <-s.neededItemsCh:
			if !ok {
				return nil, errors.New("drainColTask: cannot read from NeededItemsCh, maybe the chan is closed")
			}
			// if the task has already timeout, no sql is sync-waiting for it,
			// so do not handle it just now, put it to another channel with lower priority
			if time.Now().After(task.ToTimeout) {
				s.writeToTimeoutChan(s.timeoutItemsCh, task)
				continue
			}
			return task, nil
		case task, ok := <-s.timeoutItemsCh:
			select {
			case <-exit:
				return nil, errExit
			case task0, ok0 := <-s.neededItemsCh:
				if !ok0 {
					return nil, errors.New("drainColTask: cannot read from NeededItemsCh, maybe the chan is closed")
				}
				// send task back to TimeoutItemsCh and return the task drained from NeededItemsCh
				s.writeToTimeoutChan(s.timeoutItemsCh, task)
				return task0, nil
			default:
				if !ok {
					return nil, errors.New("drainColTask: cannot read from TimeoutItemsCh, maybe the chan is closed")
				}
				// NeededItemsCh is empty now, handle task from TimeoutItemsCh
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

// updateCachedItem updates the column/index hist to global statsCache.
func (s *statsSyncLoad) updateCachedItem(item model.TableItemID, colHist *statistics.Column, idxHist *statistics.Index, fullLoaded bool) (updated bool) {
	s.mutexForStatsCache.Lock()
	defer s.mutexForStatsCache.Unlock()
	// Reload the latest stats cache, otherwise the `updateStatsCache` may fail with high probability, because functions
	// like `GetPartitionStats` called in `fmSketchFromStorage` would have modified the stats cache already.
	tbl, ok := s.statsHandle.Get(item.TableID)
	if !ok {
		return false
	}
	if !item.IsIndex && colHist != nil {
		c := tbl.GetCol(item.ID)
		// - If the stats is fully loaded,
		// - If the stats is meta-loaded and we also just need the meta.
		if c != nil && (c.IsFullLoad() || !fullLoaded) {
			return false
		}
		tbl = tbl.CopyAs(statistics.ColumnMapWritable)
		tbl.SetCol(item.ID, colHist)

		// If the column is analyzed we refresh the map for the possible change.
		if colHist.StatsAvailable() {
			tbl.ColAndIdxExistenceMap.InsertCol(item.ID, true)
		}
		// All the objects share the same stats version. Update it here.
		if statistics.IsAnalyzed(colHist.StatsVer) {
			// SAFETY: The stats version only has a limited range, it is safe to convert int64 to int here.
			tbl.StatsVer = int(colHist.StatsVer)
		}
		// we have to refresh the map for the possible change to ensure that the map information is not missing.
		tbl.ColAndIdxExistenceMap.InsertCol(item.ID, colHist.StatsAvailable())
	} else if item.IsIndex && idxHist != nil {
		index := tbl.GetIdx(item.ID)
		// - If the stats is fully loaded,
		// - If the stats is meta-loaded and we also just need the meta.
		if index != nil && (index.IsFullLoad() || !fullLoaded) {
			return true
		}
		tbl = tbl.CopyAs(statistics.IndexMapWritable)
		tbl.SetIdx(item.ID, idxHist)
		// If the index is analyzed we refresh the map for the possible change.
		if idxHist.IsAnalyzed() {
			tbl.ColAndIdxExistenceMap.InsertIndex(item.ID, true)
			// All the objects share the same stats version. Update it here.
			// SAFETY: The stats version only has a limited range, it is safe to convert int64 to int here.
			tbl.StatsVer = int(idxHist.StatsVer)
		}
	}
	s.statsHandle.UpdateStatsCache(statstypes.CacheUpdate{
		Updated: []*statistics.Table{tbl},
	})
	return true
}
