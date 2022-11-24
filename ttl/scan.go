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

package ttl

import (
	"time"

	"github.com/pingcap/tidb/types"

	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
)

type scanTask struct {
	tbl        *PhysicalTable
	par        model.PartitionDefinition
	expire     time.Time
	rangeStart []types.Datum
	rangeEnd   []types.Datum
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
	se, err := getWorkerSession(w.sessionPool)
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
				w.executeScanTask(se, task)
			default:
				terror.Log(errors.Errorf("Cannot handle msg with type: %T", msg))
			}
		}
	}
}

func (w *scanWorker) executeScanTask(se *session, task *scanTask) {
	defer func() {
		w.Lock()
		defer w.Unlock()
		w.task = nil
	}()

	expire := task.expire.In(se.GetSessionVars().TimeZone)
	generator, err := NewScanQueryGenerator(task.tbl, expire, task.rangeStart, task.rangeEnd)
	if err != nil {
		terror.Log(err)
		return
	}

	limit := 5
	lastResult := make([][]types.Datum, 0, limit)
	for w.Status() == workerStatusRunning {
		sql, err := generator.NextSQL(lastResult, limit)
		if err != nil {
			terror.Log(err)
			time.Sleep(10 * time.Second)
			continue
		}

		if sql == "" {
			return
		}

		logutil.BgLogger().Info("TTL scan query", zap.String("sql", sql))
		rows, err := executeSQL(w.ctx, se, sql)
		if err != nil {
			terror.Log(err)
			time.Sleep(10 * time.Second)
			continue
		}

		lastResult = lastResult[:0]
		for _, row := range rows {
			lastResult = append(lastResult, row.GetDatumRow(w.task.tbl.KeyFieldTypes))
		}

		w.del <- &delTask{
			tbl:    task.tbl,
			expire: task.expire,
			keys:   lastResult,
		}
	}
}
