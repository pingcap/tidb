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
	"context"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
)

type Worker struct {
	ctx              context.Context
	ctxFactory       func() (pools.Resource, error)
	scanDispatchChan chan interface{}
	delDispatchChan  chan interface{}
	workers          []*taskWorker
	cancel           func()
}

func NewTTLWorker(ctxFactory func() (pools.Resource, error)) *Worker {
	return &Worker{
		ctx:              context.Background(),
		ctxFactory:       ctxFactory,
		scanDispatchChan: make(chan interface{}, 8),
		delDispatchChan:  make(chan interface{}, 8),
	}
}

func (w *Worker) Start() error {
	scanWorkerCnt := 2
	delWorkerCnt := 4
	taskWorkers := make([]*taskWorker, 0, scanWorkerCnt+delWorkerCnt)

	for i := 0; i < scanWorkerCnt; i++ {
		worker, err := w.createWorker(i, w.scanDispatchChan)
		if err != nil {
			return err
		}
		taskWorkers = append(taskWorkers, worker)
	}

	for i := 0; i < delWorkerCnt; i++ {
		worker, err := w.createWorker(i, w.delDispatchChan)
		if err != nil {
			return err
		}
		taskWorkers = append(taskWorkers, worker)
	}

	sctx, err := w.ctxFactory()
	if err != nil {
		return err
	}

	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.workers = taskWorkers

	go w.loop(sctx.(sessionctx.Context))
	for _, worker := range taskWorkers {
		go worker.loop()
	}
	return nil
}

func (w *Worker) Stop() {
	if w.cancel != nil {
		w.cancel()
		w.cancel = nil
	}

	for _, worker := range w.workers {
		worker.stop()
	}

	w.workers = nil
}

func (w *Worker) loop(sctx sessionctx.Context) {
	for {
		is := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()
		for _, db := range is.AllSchemas() {
			for _, tb := range is.SchemaTables(db.Name) {
				if !isTTLTable(tb.Meta()) {
					continue
				}

				select {
				case w.scanDispatchChan <- &scanTask{
					ident:     ast.Ident{Schema: db.Name, Name: tb.Meta().Name},
					delTaskCh: w.delDispatchChan,
				}:
				case <-w.ctx.Done():
					return
				}
			}
		}
		time.Sleep(time.Second * 5)
	}

}

func (w *Worker) createWorker(idx int, ch chan interface{}) (*taskWorker, error) {
	sctx, err := w.ctxFactory()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(w.ctx)
	return &taskWorker{
		idx:  idx,
		ctx:  ctx,
		sctx: sctx.(sessionctx.Context),
		recv: ch,
		stop: func() {
			cancel()
		},
	}, nil
}
