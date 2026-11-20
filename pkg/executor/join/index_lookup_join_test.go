// Copyright 2026 PingCAP, Inc.
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

package join

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
)

type oversizedOuterReader struct {
	exec.BaseExecutor
	calls int
}

func (r *oversizedOuterReader) Next(_ context.Context, req *chunk.Chunk) error {
	req.Reset()
	r.calls++
	if r.calls == 1 {
		for row := range int64(8) {
			req.AppendInt64(0, row)
		}
		req.SetSel([]int{7, 6, 5, 4, 3, 2, 1, 0})
	}
	return nil
}

func TestAdaptiveLimitOuterWorkerOversizedChunk(t *testing.T) {
	newWorker := func() (*outerWorker, *oversizedOuterReader, *exec.AdaptiveLimitController) {
		ctx := mock.NewContext()
		col := &expression.Column{RetType: types.NewFieldType(mysql.TypeLonglong)}
		reader := &oversizedOuterReader{
			BaseExecutor: exec.NewBaseExecutor(ctx, expression.NewSchema(col), 1),
		}
		controller := exec.NewAdaptiveLimitOuterJoinController(exec.AdaptiveLimitConfig{
			DemandRows: 3, InitialOuterWindow: 3, MaxOuterWindow: 3,
		})
		worker := &outerWorker{
			OuterCtx: OuterCtx{RowTypes: reader.RetFieldTypes()},
			lookup: &IndexLookUpJoin{
				BaseExecutor:            exec.NewBaseExecutor(ctx, nil, 2, reader),
				AdaptiveLimitController: controller,
			},
			executor:         reader,
			maxBatchSize:     3,
			parentMemTracker: memory.NewTracker(-1, -1),
		}
		return worker, reader, controller
	}

	t.Run("drain", func(t *testing.T) {
		worker, reader, controller := newWorker()
		defer worker.releasePendingOuterChunk()
		var actual []int64
		for i, rows := range []int{3, 3, 2} {
			task, err := worker.buildTask(context.Background())
			require.NoError(t, err)
			require.NotNil(t, task)
			require.Equal(t, rows, task.outerResult.Len())
			for index := range task.outerResult.NumChunks() {
				chk := task.outerResult.GetChunk(index)
				for row := range chk.NumRows() {
					actual = append(actual, chk.GetRow(row).GetInt64(0))
				}
			}
			task.memTracker.Detach()
			controller.ObserveJoinProgress(rows, 0)
			if i < 2 {
				require.Equal(t, 1, reader.calls)
				require.Positive(t, worker.parentMemTracker.BytesConsumed())
			}
		}
		require.Equal(t, []int64{7, 6, 5, 4, 3, 2, 1, 0}, actual)
		require.Equal(t, uint64(8), controller.Snapshot().OuterFetched)
		task, err := worker.buildTask(context.Background())
		require.NoError(t, err)
		require.Nil(t, task)
		require.Zero(t, worker.parentMemTracker.BytesConsumed())
	})

	t.Run("cancel", func(t *testing.T) {
		worker, _, _ := newWorker()
		result := make(chan *lookUpJoinTask, 1)
		worker.resultCh = result
		worker.innerCh = make(chan *lookUpJoinTask, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var wg sync.WaitGroup
		wg.Add(1)
		go worker.run(ctx, &wg)
		task := <-result
		cancel()
		wg.Wait()
		require.NotNil(t, task)
		require.Equal(t, 3, task.outerResult.Len())
		require.Nil(t, worker.pendingOuterChunk)
		task.memTracker.Detach()
		require.Zero(t, worker.parentMemTracker.BytesConsumed())
	})
}

type innerResultExecutor struct {
	exec.BaseExecutor
	next       func(*chunk.Chunk) error
	closeCount int
}

func (e *innerResultExecutor) Next(_ context.Context, chk *chunk.Chunk) error {
	return e.next(chk)
}

func (e *innerResultExecutor) Close() error {
	e.closeCount++
	return nil
}

type innerResultExecutorBuilder struct {
	executor exec.Executor
}

func (b *innerResultExecutorBuilder) BuildExecutorForIndexJoin(context.Context, []*IndexJoinLookUpContent,
	[]*ranger.Range, []int, *physicalop.ColWithCmpFuncManager, bool, *memory.Tracker, *atomic.Value) (exec.Executor, error) {
	return b.executor, nil
}

func TestIndexJoinInnerResultCleanup(t *testing.T) {
	for _, mode := range []string{"oom", "error", "canceled", "eof", "batch"} {
		t.Run(mode, func(t *testing.T) {
			sctx := mock.NewContext()
			fieldType := types.NewFieldType(mysql.TypeLonglong)
			inner := &innerResultExecutor{
				BaseExecutor: exec.NewBaseExecutor(sctx, expression.NewSchema(&expression.Column{RetType: fieldType}), 0),
			}
			task := &lookUpJoinTask{memTracker: memory.NewTracker(-1, -1)}
			worker := &innerWorker{
				ctx:      sctx,
				lookup:   &IndexLookUpJoin{},
				InnerCtx: InnerCtx{ReaderBuilder: &innerResultExecutorBuilder{executor: inner}},
			}
			nextCalls := 0
			expectedErr := errors.New("inner next failed")
			inner.next = func(chk *chunk.Chunk) error {
				nextCalls++
				if mode == "error" {
					return expectedErr
				}
				if nextCalls == 1 {
					chk.AppendInt64(0, 1)
					if mode == "oom" {
						tracker := task.innerResult.GetMemTracker()
						tracker.SetBytesLimit(1)
						tracker.SetActionOnExceed(&memory.PanicOnExceed{Killer: &sqlkiller.SQLKiller{}})
					}
				}
				return nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "canceled" {
				cancel()
			}
			if mode == "batch" {
				worker.maxFetchSize = 1
			}
			fetch := func() error { return worker.fetchInnerResults(ctx, task, nil) }
			switch mode {
			case "oom":
				// Use the real List.Add -> Tracker.Consume -> PanicOnExceed path.
				require.Panics(t, func() { _ = fetch() })
			case "error":
				require.ErrorIs(t, fetch(), expectedErr)
			case "canceled":
				require.ErrorIs(t, fetch(), context.Canceled)
			case "batch":
				require.NoError(t, fetch())
				require.Same(t, inner, task.innerExec)
				require.Zero(t, inner.closeCount)
				require.Equal(t, 1, task.innerResult.Len())
				// A bounded fetch retains the reader for the next batch until EOF.
				require.NoError(t, fetch())
				require.Zero(t, task.innerResult.Len())
			default:
				require.NoError(t, fetch())
				require.Equal(t, 1, task.innerResult.Len())
			}
			require.Equal(t, 1, inner.closeCount)
			require.Nil(t, task.innerExec)
		})
	}
}
