// Copyright 2017 PingCAP, Inc.
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

package executor

import (
	"sync"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
	goctx "golang.org/x/net/context"
)

var (
	_ Executor = &TableReaderExecutor{}
	_ Executor = &IndexReaderExecutor{}
	_ Executor = &IndexLookUpExecutor{}

	_ DataReader = &TableReaderExecutor{}
	_ DataReader = &IndexReaderExecutor{}
	_ DataReader = &IndexLookUpExecutor{}
)

// DataReader can send requests which ranges are constructed by datums.
type DataReader interface {
	Executor

	doRequestForDatums(goCtx goctx.Context, datums [][]types.Datum) error
}

// handleIsExtra checks whether this column is a extra handle column generated during plan building phase.
func handleIsExtra(col *expression.Column) bool {
	if col != nil && col.ID == model.ExtraHandleID {
		return true
	}
	return false
}

// indexWorker is used by IndexLookUpExecutor to maintain index lookup background goroutines.
type indexWorker struct {
	wg sync.WaitGroup
}

// fetchHandles fetches a batch of handles from index data and builds the index lookup tasks.
// The tasks are sent to workCh to be further processed by tableWorker, and sent to e.resultCh
// at the same time to keep data ordered.
func (worker *indexWorker) fetchHandles(e *IndexLookUpExecutor, result distsql.NewSelectResult, workCh chan<- *lookupTableTask, ctx goctx.Context, finished <-chan struct{}) {
	for {
		handles, finish, err := extractHandlesFromNewIndexResult(result)
		if err != nil {
			doneCh := make(chan error, 1)
			doneCh <- errors.Trace(err)
			e.resultCh <- &lookupTableTask{
				doneCh: doneCh,
			}
			return
		}
		if finish {
			return
		}
		tasks := e.buildTableTasks(handles)
		for _, task := range tasks {
			select {
			case <-ctx.Done():
				return
			case <-finished:
				return
			case workCh <- task:
				e.resultCh <- task
			}
		}
	}
}

func (worker *indexWorker) close() {
	worker.wg.Wait()
}

// tableWorker is used by IndexLookUpExecutor to maintain table lookup background goroutines.
type tableWorker struct {
	wg sync.WaitGroup
}

// pickAndExecTask picks tasks from workCh, and execute them.
func (worker *tableWorker) pickAndExecTask(e *IndexLookUpExecutor, workCh <-chan *lookupTableTask, ctx goctx.Context, finished <-chan struct{}) {
	for {
		// Don't check ctx.Done() on purpose. If background worker get the signal and all
		// exit immediately, session's goroutine doesn't know this and still calling Next(),
		// it may block reading task.doneCh forever.
		select {
		case task, ok := <-workCh:
			if !ok {
				return
			}
			e.executeTask(task, ctx)
		case <-finished:
			return
		}
	}
}

func (worker *tableWorker) close() {
	worker.wg.Wait()
}

type requestBuilder struct {
	kv.Request
	err error
}

func (builder *requestBuilder) Build() (*kv.Request, error) {
	return &builder.Request, errors.Trace(builder.err)
}

func (builder *requestBuilder) SetTableRanges(tid int64, tableRanges []types.IntColumnRange) *requestBuilder {
	builder.Request.KeyRanges = tableRangesToKVRanges(tid, tableRanges)
	return builder
}

func (builder *requestBuilder) SetIndexRanges(sc *variable.StatementContext, tid, idxID int64, ranges []*types.IndexRange, fieldTypes []*types.FieldType) *requestBuilder {
	if builder.err != nil {
		return builder
	}
	builder.Request.KeyRanges, builder.err = indexRangesToKVRanges(sc, tid, idxID, ranges, fieldTypes)
	return builder
}

func (builder *requestBuilder) SetTableHandles(tid int64, handles []int64) *requestBuilder {
	builder.Request.KeyRanges = tableHandlesToKVRanges(tid, handles)
	return builder
}

func (builder *requestBuilder) SetIndexValues(tid, idxID int64, values [][]types.Datum) *requestBuilder {
	if builder.err != nil {
		return builder
	}
	builder.Request.KeyRanges, builder.err = indexValuesToKVRanges(tid, idxID, values)
	return builder
}

func (builder *requestBuilder) SetDAGRequest(dag *tipb.DAGRequest) *requestBuilder {
	if builder.err != nil {
		return builder
	}

	builder.Request.Tp = kv.ReqTypeDAG
	builder.Request.StartTs = dag.StartTs
	builder.Request.Data, builder.err = dag.Marshal()
	return builder
}

func (builder *requestBuilder) SetAnalyzeRequest(ana *tipb.AnalyzeReq) *requestBuilder {
	if builder.err != nil {
		return builder
	}

	builder.Request.Tp = kv.ReqTypeAnalyze
	builder.Request.StartTs = ana.StartTs
	builder.Request.Data, builder.err = ana.Marshal()
	builder.Request.NotFillCache = true
	return builder
}

func (builder *requestBuilder) SetKeyRanges(keyRanges []kv.KeyRange) *requestBuilder {
	builder.Request.KeyRanges = keyRanges
	return builder
}

func (builder *requestBuilder) SetDesc(desc bool) *requestBuilder {
	builder.Request.Desc = desc
	return builder
}

func (builder *requestBuilder) SetKeepOrder(order bool) *requestBuilder {
	builder.Request.KeepOrder = order
	return builder
}

func (builder *requestBuilder) SetFromSessionVars(sv *variable.SessionVars) *requestBuilder {
	builder.Request.Concurrency = sv.DistSQLScanConcurrency
	builder.Request.IsolationLevel = getIsolationLevel(sv)
	builder.Request.NotFillCache = sv.StmtCtx.NotFillCache
	return builder
}

func (builder *requestBuilder) SetPriority(priority int) *requestBuilder {
	builder.Request.Priority = priority
	return builder
}
