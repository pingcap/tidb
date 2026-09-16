// Copyright 2022 PingCAP, Inc.
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

package executor

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/memory"
	"go.uber.org/atomic"
)

const maxAnalyzeStatsPersistConcurrency = 1
const analyzeResultsBacklogFactor = 4
const largePartitionBatchThresholdFactor = 4

var activePartitionAnalyzeJobs = atomic.NewInt64(0)

func getBuildStatsConcurrency(ctx sessionctx.Context) (int, error) {
	sessionVars := ctx.GetSessionVars()
	concurrency, err := sessionVars.GetSessionOrGlobalSystemVar(context.Background(), variable.TiDBBuildStatsConcurrency)
	if err != nil {
		return 0, err
	}
	c, err := strconv.ParseInt(concurrency, 10, 64)
	return int(c), err
}

func beginAnalyzePartitionConcurrencyBudget(partitionTaskCount int) (activeJobCount int, release func()) {
	if partitionTaskCount <= 1 {
		return 0, func() {}
	}
	activeJobCount = int(activePartitionAnalyzeJobs.Inc())
	return activeJobCount, func() {
		activePartitionAnalyzeJobs.Dec()
	}
}

func getAnalyzePartitionBudget(ctx sessionctx.Context, activeJobCount int, partitionTaskCount int) (int, error) {
	budget, err := getBuildStatsConcurrency(ctx)
	if err != nil {
		return 0, err
	}
	partitionConcurrency := ctx.GetSessionVars().AnalyzePartitionConcurrency
	if partitionConcurrency > 0 && partitionConcurrency < budget {
		budget = partitionConcurrency
	}
	if activeJobCount > 1 {
		budget /= activeJobCount
	}
	budget = shrinkAnalyzePartitionBudgetForLargeWave(partitionTaskCount, budget)
	if budget < 1 {
		budget = 1
	}
	return budget, nil
}

func shrinkAnalyzePartitionBudgetForLargeWave(partitionTaskCount int, budget int) int {
	if partitionTaskCount <= 1 || budget <= 1 {
		return budget
	}
	threshold := budget * budget * largePartitionBatchThresholdFactor
	for partitionTaskCount > threshold && budget > 1 {
		budget = (budget + 1) / 2
		threshold *= largePartitionBatchThresholdFactor
	}
	if budget < 1 {
		budget = 1
	}
	return budget
}

func getAnalyzeTaskConcurrency(ctx sessionctx.Context, tasks []*analyzeTask) (int, error) {
	return getAnalyzeTaskConcurrencyWithBudget(ctx, tasks, 0)
}

func getAnalyzeTaskConcurrencyWithBudget(ctx sessionctx.Context, tasks []*analyzeTask, partitionBudget int) (int, error) {
	concurrency, err := getBuildStatsConcurrency(ctx)
	if err != nil {
		return 0, err
	}
	if len(tasks) < concurrency {
		concurrency = len(tasks)
	}
	if concurrency <= 1 {
		if concurrency < 1 {
			concurrency = 1
		}
		return concurrency, nil
	}

	partitionTaskCount := getMaxPartitionTaskCount(tasks)
	if partitionTaskCount > 1 {
		if partitionBudget > 0 {
			if partitionBudget < concurrency {
				concurrency = partitionBudget
			}
		} else {
			partitionConcurrency := ctx.GetSessionVars().AnalyzePartitionConcurrency
			if partitionConcurrency > 0 && partitionConcurrency < concurrency {
				concurrency = partitionConcurrency
			}
		}
	}
	if concurrency < 1 {
		concurrency = 1
	}
	return concurrency, nil
}

func getSamplingStatsConcurrency(ctx sessionctx.Context, tableID statistics.AnalyzeTableID, taskItemCount int) (int, error) {
	return getSamplingStatsConcurrencyWithBudget(ctx, tableID, taskItemCount, 0, 1)
}

func getSamplingStatsConcurrencyWithBudget(ctx sessionctx.Context, tableID statistics.AnalyzeTableID, taskItemCount int, partitionBudget int, taskConcurrency int) (int, error) {
	concurrency, err := getBuildStatsConcurrency(ctx)
	if err != nil {
		return 0, err
	}
	if tableID.IsPartitionTable() {
		if partitionBudget > 0 {
			if partitionBudget < concurrency {
				concurrency = partitionBudget
			}
			if taskConcurrency > 1 {
				taskBudget := partitionBudget / taskConcurrency
				if taskBudget < 1 {
					taskBudget = 1
				}
				if taskBudget < concurrency {
					concurrency = taskBudget
				}
			}
		} else {
			partitionConcurrency := ctx.GetSessionVars().AnalyzePartitionConcurrency
			if partitionConcurrency > 0 && partitionConcurrency < concurrency {
				concurrency = partitionConcurrency
			}
		}
	}
	if taskItemCount > 0 && taskItemCount < concurrency {
		concurrency = taskItemCount
	}
	if concurrency < 1 {
		concurrency = 1
	}
	return concurrency, nil
}

func getAnalyzeStatsPersistConcurrency(ctx sessionctx.Context, taskConcurrency int) int {
	concurrency := ctx.GetSessionVars().AnalyzePartitionConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	if taskConcurrency > 0 && taskConcurrency < concurrency {
		concurrency = taskConcurrency
	}
	if concurrency > maxAnalyzeStatsPersistConcurrency {
		concurrency = maxAnalyzeStatsPersistConcurrency
	}
	return concurrency
}

func getAnalyzeResultsChannelCapacity(ctx sessionctx.Context, tasks []*analyzeTask, taskConcurrency int) int {
	capacity := len(tasks)
	if capacity < 1 {
		return 1
	}
	if getMaxPartitionTaskCount(tasks) <= 1 {
		return capacity
	}
	saveStatsConcurrency := getAnalyzeStatsPersistConcurrency(ctx, taskConcurrency)
	backlogLimit := saveStatsConcurrency * analyzeResultsBacklogFactor
	if backlogLimit < 1 {
		backlogLimit = 1
	}
	if backlogLimit < capacity {
		capacity = backlogLimit
	}
	return capacity
}

func getAnalyzeGlobalStatsMergeConcurrency(ctx sessionctx.Context) int {
	return ctx.GetSessionVars().AnalyzePartitionMergeConcurrency
}

func getMaxPartitionTaskCount(tasks []*analyzeTask) int {
	partitionTaskCountByTable := make(map[int64]int)
	maxCount := 0
	for _, task := range tasks {
		tableID, ok := getAnalyzeTaskTableID(task)
		if !ok || !tableID.IsPartitionTable() {
			continue
		}
		partitionTaskCountByTable[tableID.TableID]++
		if partitionTaskCountByTable[tableID.TableID] > maxCount {
			maxCount = partitionTaskCountByTable[tableID.TableID]
		}
	}
	return maxCount
}

func getAnalyzeTaskTableID(task *analyzeTask) (statistics.AnalyzeTableID, bool) {
	if task == nil {
		return statistics.AnalyzeTableID{}, false
	}
	switch task.taskType {
	case colTask:
		if task.colExec != nil {
			return task.colExec.tableID, true
		}
	case idxTask:
		if task.idxExec != nil {
			return task.idxExec.tableID, true
		}
	case fastTask:
		if task.fastExec != nil {
			return task.fastExec.tableID, true
		}
	case pkIncrementalTask:
		if task.colIncrementalExec != nil {
			return task.colIncrementalExec.tableID, true
		}
	case idxIncrementalTask:
		if task.idxIncrementalExec != nil {
			return task.idxIncrementalExec.tableID, true
		}
	}
	return statistics.AnalyzeTableID{}, false
}

func setAnalyzeTaskConcurrencyBudget(task *analyzeTask, partitionBudget int, analyzeTaskConcurrency int) {
	if task == nil {
		return
	}
	switch task.taskType {
	case colTask:
		if task.colExec != nil {
			task.colExec.partitionConcurrencyBudget = partitionBudget
			task.colExec.analyzeTaskConcurrency = analyzeTaskConcurrency
		}
	case idxTask:
		if task.idxExec != nil {
			task.idxExec.partitionConcurrencyBudget = partitionBudget
			task.idxExec.analyzeTaskConcurrency = analyzeTaskConcurrency
		}
	case fastTask:
		if task.fastExec != nil {
			task.fastExec.partitionConcurrencyBudget = partitionBudget
			task.fastExec.analyzeTaskConcurrency = analyzeTaskConcurrency
		}
	case pkIncrementalTask:
		if task.colIncrementalExec != nil {
			task.colIncrementalExec.partitionConcurrencyBudget = partitionBudget
			task.colIncrementalExec.analyzeTaskConcurrency = analyzeTaskConcurrency
		}
	case idxIncrementalTask:
		if task.idxIncrementalExec != nil {
			task.idxIncrementalExec.partitionConcurrencyBudget = partitionBudget
			task.idxIncrementalExec.analyzeTaskConcurrency = analyzeTaskConcurrency
		}
	}
}

var errAnalyzeWorkerPanic = errors.New("analyze worker panic")
var errAnalyzeOOM = errors.Errorf("analyze panic due to memory quota exceeds, please try with smaller samplerate(refer to %d/count)", config.DefRowsForSampleRate)

func isAnalyzeWorkerPanic(err error) bool {
	return err == errAnalyzeWorkerPanic || err == errAnalyzeOOM
}

func getAnalyzePanicErr(r interface{}) error {
	if msg, ok := r.(string); ok {
		if msg == globalPanicAnalyzeMemoryExceed {
			return errAnalyzeOOM
		}
		if strings.Contains(msg, memory.PanicMemoryExceedWarnMsg) {
			return errors.Errorf(msg, errAnalyzeOOM)
		}
	}
	if err, ok := r.(error); ok {
		if err.Error() == globalPanicAnalyzeMemoryExceed {
			return errAnalyzeOOM
		}
		return err
	}
	return errAnalyzeWorkerPanic
}

// analyzeResultsNotifyWaitGroupWrapper is a wrapper for sync.WaitGroup
// Please add all goroutine count when to `Add` to avoid exiting in advance.
type analyzeResultsNotifyWaitGroupWrapper struct {
	sync.WaitGroup
	notify chan *statistics.AnalyzeResults
	cnt    atomic.Uint64
}

// NewAnalyzeResultsNotifyWaitGroupWrapper is to create analyzeResultsNotifyWaitGroupWrapper
func NewAnalyzeResultsNotifyWaitGroupWrapper(notify chan *statistics.AnalyzeResults) *analyzeResultsNotifyWaitGroupWrapper {
	return &analyzeResultsNotifyWaitGroupWrapper{
		notify: notify,
		cnt:    *atomic.NewUint64(0),
	}
}

// Run runs a function in a goroutine and calls done when function returns.
// Please DO NOT use panic in the cb function.
func (w *analyzeResultsNotifyWaitGroupWrapper) Run(exec func()) {
	old := w.cnt.Inc() - 1
	go func(cnt uint64) {
		defer func() {
			w.Done()
			if cnt == 0 {
				w.Wait()
				close(w.notify)
			}
		}()
		exec()
	}(old)
}

// notifyErrorWaitGroupWrapper is a wrapper for sync.WaitGroup
// Please add all goroutine count when to `Add` to avoid exiting in advance.
type notifyErrorWaitGroupWrapper struct {
	sync.WaitGroup
	notify chan error
	cnt    atomic.Uint64
}

// newNotifyErrorWaitGroupWrapper is to create notifyErrorWaitGroupWrapper
func newNotifyErrorWaitGroupWrapper(notify chan error) *notifyErrorWaitGroupWrapper {
	return &notifyErrorWaitGroupWrapper{
		notify: notify,
		cnt:    *atomic.NewUint64(0),
	}
}

// Run runs a function in a goroutine and calls done when function returns.
// Please DO NOT use panic in the cb function.
func (w *notifyErrorWaitGroupWrapper) Run(exec func()) {
	old := w.cnt.Inc() - 1
	go func(cnt uint64) {
		defer func() {
			w.Done()
			if cnt == 0 {
				w.Wait()
				close(w.notify)
			}
		}()
		exec()
	}(old)
}
