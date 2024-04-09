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
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// Task represents the range of the table for loading stats.
type Task struct {
	StartTid int64
	EndTid   int64
}

// RangeWorker is used to load stats concurrently by the range of table id.
type RangeWorker struct {
	dealFunc func(task Task) error
	taskChan chan Task

	wg util.WaitGroupWrapper
}

// NewRangeWorker creates a new RangeWorker.
func NewRangeWorker(dealFunc func(task Task) error) *RangeWorker {
	return &RangeWorker{
		dealFunc: dealFunc,
		taskChan: make(chan Task, 1),
	}
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
