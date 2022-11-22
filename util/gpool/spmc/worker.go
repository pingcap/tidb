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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/gpool"
	"go.uber.org/zap"
)

// goWorker is the actual executor who runs the tasks,
// it starts a goroutine that accepts tasks and
// performs function calls.
type goWorker[T any, U any, C any, CT any, TF gpool.Context[CT]] struct {
	// pool who owns this worker.
	pool *Pool[T, U, C, CT, TF]

	// taskBoxCh is a job should be done.
	taskBoxCh chan *gpool.TaskBox[T, U, C, CT, TF]

	exit chan struct{}

	// recycleTime will be updated when putting a worker back into queue.
	recycleTime time.Time
}

// run starts a goroutine to repeat the process
// that performs the function calls.
func (w *goWorker[T, U, C, CT, TF]) run() {
	w.pool.addRunning(1)
	go func() {
		//log.Info("worker start")
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
			switch f.GetStatus() {
			case gpool.PendingTask:
				f.SetStatus(gpool.RunningTask)
			case gpool.StopTask:
				continue
			case gpool.RunningTask:
				log.Error("worker got task running")
				continue
			}
			ctx := f.GetContextFunc().GetContext()
			if f.GetResultCh() != nil {
				for t := range f.GeTaskCh() {
					f.GetResultCh() <- w.pool.consumerFunc(t, f.ConstArgs(), ctx)
					f.Done()
					if f.GetStatus() == gpool.PendingTask {
						w.taskBoxCh <- f
						break
					}
				}
				f.SetStatus(gpool.StopTask)
			}
			if ok := w.pool.revertWorker(w); !ok {
				//log.Info("exit here")
				return
			}
		}
	}()
}
