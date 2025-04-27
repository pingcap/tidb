// Copyright 2024 PingCAP, Inc.
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

package initstats

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

// InitStatsPercentage is the percentage of the table to load stats.
var InitStatsPercentage atomicutil.Float64

var (
	sampleLoggerFactory = logutil.SampleLoggerFactory(time.Minute, 1, zap.String(logutil.LogFieldCategory, "stats"))
)

// SingletonStatsSamplerLogger with category "stats" is used to log statistic related messages.
// It is used to sample the log to avoid too many logs.
// Do not use it to log the message that is not related to statistics.
func singletonStatsSamplerLogger() *zap.Logger {
	return sampleLoggerFactory()
}

// Task represents the range of the table for loading stats.
type Task struct {
	StartTid int64
	EndTid   int64
}

// RangeWorker is used to load stats concurrently by the range of table id.
//
//nolint:fieldalignment
type RangeWorker struct {
	progressLogger *zap.Logger

	taskName        string
	taskChan        chan Task
	processTask     func(task Task) error
	taskCnt         uint64
	completeTaskCnt atomic.Uint64

	totalPercentage     float64
	totalPercentageStep float64

	concurrency int
	wg          util.WaitGroupWrapper
}

// NewRangeWorker creates a new RangeWorker.
func NewRangeWorker(
	taskName string,
	processTask func(task Task) error,
	concurrency int,
	maxTid,
	initStatsStep uint64,
	totalPercentageStep float64,
) *RangeWorker {
	taskCnt := uint64(1)
	if maxTid > initStatsStep*2 {
		taskCnt = maxTid / initStatsStep
	}
	worker := &RangeWorker{
		taskName:            taskName,
		processTask:         processTask,
		concurrency:         concurrency,
		taskChan:            make(chan Task, 1),
		taskCnt:             taskCnt,
		totalPercentage:     InitStatsPercentage.Load(),
		totalPercentageStep: totalPercentageStep,
	}
	worker.progressLogger = singletonStatsSamplerLogger()
	return worker
}

// LoadStats loads stats concurrently when to init stats
func (ls *RangeWorker) LoadStats() {
	for range ls.concurrency {
		ls.wg.Run(func() {
			ls.loadStats()
		})
	}
}

func (ls *RangeWorker) loadStats() {
	for task := range ls.taskChan {
		if err := ls.processTask(task); err != nil {
			logutil.BgLogger().Error("load stats failed", zap.Error(err))
		}
		if ls.progressLogger != nil {
			completeTaskCnt := ls.completeTaskCnt.Add(1)
			taskPercentage := float64(completeTaskCnt)/float64(ls.taskCnt)*ls.totalPercentageStep + ls.totalPercentage
			InitStatsPercentage.Store(taskPercentage)
			ls.progressLogger.Info(fmt.Sprintf("load %s [%d/%d]", ls.taskName, completeTaskCnt, ls.taskCnt))
		}
	}
}

// SendTask sends a task to the load stats worker.
func (ls *RangeWorker) SendTask(task Task) {
	ls.taskChan <- task
}

// Wait closes the load stats worker.
func (ls *RangeWorker) Wait() {
	close(ls.taskChan)
	ls.wg.Wait()
}
