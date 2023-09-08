// Copyright 2023 PingCAP, Inc.
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

package importinto

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/disttask/operator"
	"github.com/pingcap/tidb/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/resourcemanager/util"
	tidbutil "github.com/pingcap/tidb/util"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// encodeAndSortOperator is an operator that encodes and sorts data.
// this operator process data of a subtask, i.e. one engine, it contains a lot
// of data chunks, each chunk is a data file or part of it.
// we don't split into encode and sort operators of chunk level, we parallel
// them inside.
type encodeAndSortOperator struct {
	*operator.AsyncOperator[*importStepMinimalTask, workerpool.None]
	wg       tidbutil.WaitGroupWrapper
	firstErr atomic.Error

	ctx    context.Context
	cancel context.CancelFunc

	logger *zap.Logger
	errCh  chan error
}

var _ operator.Operator = (*encodeAndSortOperator)(nil)
var _ operator.WithSource[*importStepMinimalTask] = (*encodeAndSortOperator)(nil)
var _ operator.WithSink[workerpool.None] = (*encodeAndSortOperator)(nil)

func newEncodeAndSortOperator(ctx context.Context, concurrency int, logger *zap.Logger) *encodeAndSortOperator {
	subCtx, cancel := context.WithCancel(ctx)
	op := &encodeAndSortOperator{
		ctx:    subCtx,
		cancel: cancel,
		logger: logger,
		errCh:  make(chan error),
	}
	pool := workerpool.NewWorkerPool(
		"encodeAndSortOperator",
		util.ImportInto,
		concurrency,
		func() workerpool.Worker[*importStepMinimalTask, workerpool.None] {
			return &chunkWorker{
				ctx: subCtx,
				op:  op,
			}
		},
	)
	op.AsyncOperator = operator.NewAsyncOperator(subCtx, pool)
	return op
}

func (op *encodeAndSortOperator) Open() error {
	op.wg.Run(func() {
		for err := range op.errCh {
			if op.firstErr.CompareAndSwap(nil, err) {
				op.cancel()
			} else {
				if errors.Cause(err) != context.Canceled {
					op.logger.Error("error on encode and sort", zap.Error(err))
				}
			}
		}
	})
	return op.AsyncOperator.Open()
}

func (op *encodeAndSortOperator) Close() error {
	// TODO: handle close err after we separate wait part from close part.
	// right now AsyncOperator.Close always returns nil, ok to ignore it.
	// nolint:errcheck
	op.AsyncOperator.Close()
	op.cancel()
	close(op.errCh)
	op.wg.Wait()
	// see comments on interface definition, this Close is actually WaitAndClose.
	return op.firstErr.Load()
}

func (*encodeAndSortOperator) String() string {
	return "encodeAndSortOperator"
}

func (op *encodeAndSortOperator) hasError() bool {
	return op.firstErr.Load() != nil
}

func (op *encodeAndSortOperator) onError(err error) {
	op.errCh <- err
}

func (op *encodeAndSortOperator) Done() <-chan struct{} {
	return op.ctx.Done()
}

type chunkWorker struct {
	ctx context.Context
	op  *encodeAndSortOperator
}

func (w *chunkWorker) HandleTask(task *importStepMinimalTask, _ func(workerpool.None)) {
	if w.op.hasError() {
		return
	}
	// we don't use the input send function, it makes workflow more complex
	// we send result to errCh and handle it here.
	executor := newImportMinimalTaskExecutor(task)
	if err := executor.Run(w.ctx); err != nil {
		w.op.onError(err)
	}
}

func (*chunkWorker) Close() {
}
