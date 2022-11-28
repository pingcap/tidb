// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ttlworker

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/ttl"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type taskStatics struct {
	TotalRows   atomic.Uint64
	SuccessRows atomic.Uint64
	ErrorRows   atomic.Uint64
}

type scanTask struct {
	tbl        *ttl.PhysicalTable
	expire     time.Time
	rangeStart []types.Datum
	rangeEnd   []types.Datum
	statics    *taskStatics
}

type scanWorker struct {
	baseWorker
	task        *scanTask
	del         chan<- *delTask
	sessionPool sessionPool
}

func newScanWorker(del chan<- *delTask, sessPool sessionPool) (w *scanWorker) {
	w = &scanWorker{}
	w.init(w.scanLoop)
	w.del = del
	w.sessionPool = sessPool
	return
}

func (w *scanWorker) Idle() bool {
	w.Lock()
	defer w.Unlock()
	return w.task == nil
}

func (w *scanWorker) ScheduleTask(task *scanTask) bool {
	w.Lock()
	defer w.Unlock()

	if w.status != workerStatusRunning {
		return false
	}

	if w.task != nil {
		return false
	}

	select {
	case w.Send() <- task:
		w.task = task
		return true
	default:
		return false
	}
}

func (w *scanWorker) scanLoop() error {
	se, err := getSession(w.sessionPool)
	if err != nil {
		return err
	}
	defer se.Close()

	for {
		select {
		case <-w.ctx.Done():
			return nil
		case msg := <-w.ch:
			switch task := msg.(type) {
			case *scanTask:
				if err = w.executeScanTask(se, task); err != nil {
					terror.Log(err)
				}
			default:
				terror.Log(errors.Errorf("Cannot handle msg with type: %T", msg))
			}
		}
	}
}

func (w *scanWorker) executeScanTask(se *ttl.Session, task *scanTask) error {
	defer func() {
		w.Lock()
		defer w.Unlock()
		w.task = nil
	}()

	expire := task.expire.In(se.Sctx.GetSessionVars().TimeZone)
	generator, err := ttl.NewScanQueryGenerator(task.tbl, expire, task.rangeStart, task.rangeEnd)
	if err != nil {
		return err
	}

	limit := 5
	lastResult := make([][]types.Datum, 0, limit)

	sql := ""
	retryTimes := 0
	for w.Status() == workerStatusRunning {
		if totalRows := w.task.statics.TotalRows.Load(); totalRows > 1000 {
			errRows := w.task.statics.ErrorRows.Load()
			if float64(errRows)/float64(totalRows) > 0.6 {
				return errors.Errorf("failed")
			}
		}

		if _, err = se.ExecuteSQL(w.ctx, "set @@time_zone = @@global.time_zone"); err != nil {
			return err
		}
		generator.SetTimeZone(se.Sctx.GetSessionVars().TimeZone)

		if sql == "" {
			sql, err = generator.NextSQL(lastResult, limit)
			if sql == "" || err != nil {
				return err
			}
		}

		logutil.BgLogger().Info("TTL scan query", zap.String("sql", sql))
		rows, retryable, err := se.ExecuteSQLWithTTLCheck(w.ctx, w.task.tbl, sql)
		if err != nil {
			if !retryable || retryTimes >= 3 {
				return err
			}
			retryTimes += 1
			time.Sleep(time.Second * 10)
			terror.Log(err)
			continue
		}

		sql = ""
		retryTimes = 0
		w.task.statics.TotalRows.Add(uint64(len(rows)))
		deleteTask := &delTask{
			tbl:     task.tbl,
			expire:  task.expire,
			keys:    rows,
			statics: w.task.statics,
		}
		lastResult = rows

		select {
		case <-w.ctx.Done():
			return w.ctx.Err()
		case w.del <- deleteTask:
			continue
		}
	}
	return nil
}
