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
	"time"

	"github.com/pingcap/tidb/ttl"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type delRetryInfo struct {
	task         *delTask
	retryBatches [][][]types.Datum
}

type delTask struct {
	tbl     *ttl.PhysicalTable
	expire  time.Time
	keys    [][]types.Datum
	statics *taskStatics
}

type delWorker struct {
	baseWorker
	sessPool   sessionPool
	taskNotify <-chan *delTask
}

func newDelWorker(ch <-chan *delTask, sessPool sessionPool) (w *delWorker) {
	w = &delWorker{}
	w.init(w.delLoop)
	w.sessPool = sessPool
	w.taskNotify = ch
	return
}

func (w *delWorker) delLoop() error {
	se, err := getSession(w.sessPool)
	if err != nil {
		return err
	}
	defer se.Close()

	for {
		select {
		case <-w.ctx.Done():
			return nil
		case task := <-w.taskNotify:
			w.executeDelTask(se, task)
		}
	}
}

func (w *delWorker) executeDelTask(se *ttl.Session, task *delTask) {
	totalRows := len(task.keys)
	batchSize := 2
	batch := make([][]types.Datum, 0, batchSize)
	expire := task.expire.In(se.Sctx.GetSessionVars().TimeZone)
	for i, row := range task.keys {
		batch = append(batch, row)
		if i == totalRows-1 || len(batch) == batchSize {
			sql, err := ttl.BuildDeleteSQL(task.tbl, batch, expire)
			if err != nil {
				logutil.BgLogger().Error("", zap.Error(err))
				return
			}

			batch = batch[:0]
			w.doDelete(se, sql)
		}
	}
}

func (w *delWorker) doDelete(se *ttl.Session, sql string) {
	maxRetry := 10
	for i := 0; i <= maxRetry && w.Status() == workerStatusRunning; i++ {
		logutil.BgLogger().Info("TTL delete query", zap.String("sql", sql))
		if _, err := se.ExecuteSQL(w.ctx, sql); err != nil {
			logutil.BgLogger().Error("", zap.Error(err))
			time.Sleep(time.Second)
			continue
		}
		return
	}
}
