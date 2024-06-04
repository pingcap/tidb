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
	"sync"
	"sync/atomic"
	"time"

	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	initSamplerLoggerOnce sync.Once
	samplerLogger         *zap.Logger
)

// SingletonStatsSamplerLogger with category "stats" is used to log statistic related messages.
// It is used to sample the log to avoid too many logs.
// NOTE: Do not create a new logger for each log, it will cause the sampler not work.
// Because we need to record the log count with the same level and message in this specific logger.
// Do not use it to log the message that is not related to statistics.
func singletonStatsSamplerLogger() *zap.Logger {
	init := func() {
		if samplerLogger == nil {
			// Create a new zapcore sampler with options
			// This will log the first log entries with the same level and message in 1 minutes and ignore the rest of the logs.
			sampler := zap.WrapCore(func(core zapcore.Core) zapcore.Core {
				return zapcore.NewSamplerWithOptions(core, time.Minute, 1, 0)
			})
			samplerLogger = statslogutil.StatsLogger().WithOptions(sampler)
		}
	}

	initSamplerLoggerOnce.Do(init)
	return samplerLogger
}

// Task represents the range of the table for loading stats.
type Task struct {
	StartTid int64
	EndTid   int64
}

// RangeWorker is used to load stats concurrently by the range of table id.
type RangeWorker struct {
	dealFunc        func(task Task) error
	taskChan        chan Task
	logger          *zap.Logger
	taskName        string
	wg              util.WaitGroupWrapper
	taskCnt         uint64
	completeTaskCnt atomic.Uint64
}

// NewRangeWorker creates a new RangeWorker.
func NewRangeWorker(taskName string, dealFunc func(task Task) error, maxTid, initStatsStep uint64) *RangeWorker {
	taskCnt := uint64(1)
	if maxTid > initStatsStep*2 {
		taskCnt = maxTid / initStatsStep
	}
	worker := &RangeWorker{
		taskName: taskName,
		dealFunc: dealFunc,
		taskChan: make(chan Task, 1),
		taskCnt:  taskCnt,
	}
	worker.logger = singletonStatsSamplerLogger()
	return worker
}

// LoadStats loads stats concurrently when to init stats
func (ls *RangeWorker) LoadStats() {
	concurrency := getConcurrency()
	for n := 0; n < concurrency; n++ {
		ls.wg.Run(func() {
			ls.loadStats()
		})
	}
}

func (ls *RangeWorker) loadStats() {
	for task := range ls.taskChan {
		if err := ls.dealFunc(task); err != nil {
			logutil.BgLogger().Error("load stats failed", zap.Error(err))
		}
		if ls.logger != nil {
			completeTaskCnt := ls.completeTaskCnt.Add(1)
			ls.logger.Info(fmt.Sprintf("load %s [%d/%d]", ls.taskName, completeTaskCnt, ls.taskCnt))
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
