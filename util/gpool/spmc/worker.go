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

package spmc

import (
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/resourcemanager/pooltask"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

// goWorker is the actual executor who runs the tasks,
// it starts a goroutine that accepts tasks and
// performs function calls.
type goWorker[T any, U any, C any, CT any, TF pooltask.Context[CT]] struct {
	// pool who owns this worker.
	pool *Pool[T, U, C, CT, TF]

	// taskBoxCh is a job should be done.
	taskBoxCh chan *pooltask.TaskBox[T, U, C, CT, TF]

	// recycleTime will be updated when putting a worker back into queue.
	recycleTime atomicutil.Time
}

// run starts a goroutine to repeat the process
// that performs the function calls.
func (w *goWorker[T, U, C, CT, TF]) run() {
	w.pool.addRunning(1)
	go func() {
		defer func() {
			w.pool.addRunning(-1)
			w.pool.workerCache.Put(w)
			if p := recover(); p != nil {
				if ph := w.pool.options.PanicHandler; ph != nil {
					ph(p)
				} else {
					log.Error("worker exits from a panic", zap.Any("recover", p), zap.Stack("stack"))
				}
			}
			// Call Signal() here in case there are goroutines waiting for available workers.
			w.pool.cond.Signal()
		}()

		for f := range w.taskBoxCh {
			if f == nil {
				return
			}
			if f.GetStatus() == pooltask.PendingTask {
				f.SetStatus(pooltask.RunningTask)
			}
			w.pool.subWaitingTask()
			ctx := f.GetContextFunc().GetContext()
			if f.GetResultCh() != nil {
				for t := range f.GetTaskCh() {
					if f.GetStatus() == pooltask.StopTask {
						f.Done()
						break
					}
					f.GetResultCh() <- w.pool.consumerFunc(t.Task, f.ConstArgs(), ctx)
					f.Done()
				}
			}
			w.pool.ExitSubTask(f.TaskID())
			f.Finish()
			if ok := w.pool.revertWorker(w); !ok {
				return
			}
		}
	}()
}
