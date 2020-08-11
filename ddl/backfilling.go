// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
)

type backfillWorkerType byte

const (
	typeAddIndexWorker     backfillWorkerType = 0
	typeUpdateColumnWroker backfillWorkerType = 1
)

type backfillWorker struct {
	id        int
	ddlWorker *worker
	batchCnt  int
	sessCtx   sessionctx.Context
	taskCh    chan *reorgIndexTask
	resultCh  chan *backfillResult
	table     table.Table
	closed    bool
	priority  int

	backfiller
}

func newBackfillWorker(sessCtx sessionctx.Context, worker *worker, id int, t table.PhysicalTable) *backfillWorker {
	return &backfillWorker{
		id:        id,
		table:     t,
		ddlWorker: worker,
		batchCnt:  int(variable.GetDDLReorgBatchSize()),
		sessCtx:   sessCtx,
		taskCh:    make(chan *reorgIndexTask, 1),
		resultCh:  make(chan *backfillResult, 1),
		priority:  kv.PriorityLow,
	}
}

func (w *backfillWorker) Close() {
	if !w.closed {
		w.closed = true
		close(w.taskCh)
	}
}

func closeBackfillWorkers(workers []*backfillWorker) {
	for _, worker := range workers {
		worker.Close()
	}
}

type backfiller interface {
	BackfillDataInTxn(handleRange reorgIndexTask) (taskCtx backfillTaskContext, errInTxn error)
}

// TODO: Move backfillWorker related code from "index.go" to this file.
