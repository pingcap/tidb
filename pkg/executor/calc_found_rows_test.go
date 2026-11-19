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

package executor

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type foundRowsTestExecutor struct {
	exec.BaseExecutor
	values    []int64
	cursor    int
	batchSize int
	nextErr   error
	failAfter int
	onOpen    func(context.Context) error
	onNext    func(context.Context, *chunk.Chunk) error
	onClose   func() error
	opens     atomic.Int32
	closes    atomic.Int32
}

func newFoundRowsTestExecutor(ctx *mock.Context, values ...int64) *foundRowsTestExecutor {
	schema := expression.NewSchema(&expression.Column{RetType: types.NewFieldType(mysql.TypeLonglong)})
	return &foundRowsTestExecutor{BaseExecutor: exec.NewBaseExecutor(ctx, schema, 0), values: values, batchSize: 1}
}

func (e *foundRowsTestExecutor) Open(ctx context.Context) error {
	e.opens.Add(1)
	e.cursor = 0
	if e.onOpen != nil {
		return e.onOpen(ctx)
	}
	return nil
}

func (e *foundRowsTestExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.onNext != nil {
		return e.onNext(ctx, req)
	}
	return e.nextRows(req)
}

func (e *foundRowsTestExecutor) nextRows(req *chunk.Chunk) error {
	if e.nextErr != nil && e.cursor >= e.failAfter {
		return e.nextErr
	}
	end := min(len(e.values), e.cursor+e.batchSize)
	if e.nextErr != nil {
		end = min(end, e.failAfter)
	}
	for e.cursor < end {
		req.AppendInt64(0, e.values[e.cursor])
		e.cursor++
	}
	return nil
}

func (e *foundRowsTestExecutor) Close() error {
	e.closes.Add(1)
	if e.onClose != nil {
		return e.onClose()
	}
	return nil
}

func TestCalcFoundRowsExec(t *testing.T) {
	ctx := context.Background()
	newExec := func(values []int64, total int64) (*CalcFoundRowsExec, *foundRowsTestExecutor, *foundRowsTestExecutor) {
		sctx := mock.NewContext()
		data := newFoundRowsTestExecutor(sctx, values...)
		count := newFoundRowsTestExecutor(sctx, total)
		e := &CalcFoundRowsExec{BaseExecutor: exec.NewBaseExecutor(sctx, data.Schema(), 0, data, count)}
		return e, data, count
	}

	t.Run("completion and reopen", func(t *testing.T) {
		for _, total := range []int64{0, 9} {
			values := []int64{1, 2}
			if total == 0 {
				values = nil
			}
			e, data, count := newExec(values, total)
			var dataContext context.Context
			data.onNext = func(ctx context.Context, req *chunk.Chunk) error {
				dataContext = ctx
				return data.nextRows(req)
			}
			for round := range 2 {
				require.NoError(t, exec.Open(ctx, e))
				req := exec.NewFirstChunk(e)
				for range len(values) {
					require.NoError(t, exec.Next(ctx, e, req))
					require.Equal(t, 1, req.NumRows())
					require.NoError(t, dataContext.Err(), "data workers may retain the Next context between chunks")
					_, available := e.FoundRows()
					require.False(t, available)
					require.EqualValues(t, round, count.opens.Load())
				}
				require.NoError(t, exec.Next(ctx, e, req))
				require.Zero(t, req.NumRows())
				rows, available := e.FoundRows()
				require.True(t, available)
				require.Equal(t, uint64(total), rows)
				require.NoError(t, exec.Next(ctx, e, req))
				require.NoError(t, e.Close())
				require.EqualValues(t, round+1, data.closes.Load())
				require.EqualValues(t, round+1, count.opens.Load())
				require.EqualValues(t, round+1, count.closes.Load())
			}
		}
	})

	for _, phase := range []string{"open", "next", "close"} {
		t.Run("count error during "+phase, func(t *testing.T) {
			e, data, count := newExec(nil, 9)
			failure := errors.New("count " + phase)
			switch phase {
			case "open":
				count.onOpen = func(context.Context) error { return failure }
			case "next":
				count.nextErr = failure
			case "close":
				count.onClose = func() error { return failure }
			}
			require.NoError(t, exec.Open(ctx, e))
			require.ErrorIs(t, exec.Next(ctx, e, exec.NewFirstChunk(e)), failure)
			_, available := e.FoundRows()
			require.False(t, available)
			_ = e.Close() // The count child can itself return the injected close error.
			require.EqualValues(t, 1, data.closes.Load())
			require.EqualValues(t, 1, count.closes.Load())
		})
	}

	t.Run("early close", func(t *testing.T) {
		e, data, count := newExec([]int64{1, 2}, 9)
		require.NoError(t, exec.Open(ctx, e))
		require.NoError(t, exec.Next(ctx, e, exec.NewFirstChunk(e)))
		require.NoError(t, e.Close())
		require.ErrorIs(t, exec.Next(ctx, e, exec.NewFirstChunk(e)), context.Canceled)
		require.NoError(t, e.Close())
		require.EqualValues(t, 1, data.closes.Load())
		require.Zero(t, count.opens.Load())
	})

	t.Run("cancellation at handoff", func(t *testing.T) {
		e, data, count := newExec(nil, 9)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		fp := "github.com/pingcap/tidb/pkg/executor/afterCalcFoundRowsData"
		require.NoError(t, failpoint.EnableCall(fp, func(sessionctx.Context) { cancel() }))
		t.Cleanup(func() { require.NoError(t, failpoint.Disable(fp)) })
		require.NoError(t, exec.Open(ctx, e))
		require.ErrorIs(t, exec.Next(ctx, e, exec.NewFirstChunk(e)), context.Canceled)
		require.NoError(t, e.Close())
		require.Zero(t, count.opens.Load())
		require.EqualValues(t, 1, data.closes.Load())
		_, available := e.FoundRows()
		require.False(t, available)
	})

	t.Run("cancelled count does not publish", func(t *testing.T) {
		e, _, count := newExec(nil, 9)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		count.onNext = func(_ context.Context, req *chunk.Chunk) error {
			cancel()
			req.AppendInt64(0, 9)
			return nil
		}
		require.NoError(t, exec.Open(ctx, e))
		require.ErrorIs(t, exec.Next(ctx, e, exec.NewFirstChunk(e)), context.Canceled)
		require.NoError(t, e.Close())
		_, available := e.FoundRows()
		require.False(t, available)
		require.EqualValues(t, 1, count.closes.Load())
	})

	t.Run("close waits for next", func(t *testing.T) {
		e, data, count := newExec(nil, 9)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		entered := make(chan struct{})
		data.onNext = func(ctx context.Context, _ *chunk.Chunk) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}
		require.NoError(t, exec.Open(ctx, e))
		nextResult := make(chan error, 1)
		go func() { nextResult <- exec.Next(ctx, e, exec.NewFirstChunk(e)) }()
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("child did not enter Next")
		}
		// Next owns the lifecycle lock until the caller cancels its context.
		require.False(t, e.mu.TryLock())
		closeResult := make(chan error, 1)
		go func() { closeResult <- e.Close() }()
		cancel()
		select {
		case err := <-nextResult:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("Next did not return after caller cancellation")
		}
		select {
		case err := <-closeResult:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("Close did not return after Next")
		}
		require.EqualValues(t, 1, data.closes.Load())
		require.Zero(t, count.opens.Load())
	})

}

func TestRecordSetFoundRows(t *testing.T) {
	for _, batch := range []int{1, 2, 8} {
		t.Run(fmt.Sprintf("chunk size %d", batch), func(t *testing.T) {
			ctx := mock.NewContext()
			ctx.GetSessionVars().LastFoundRows = 13
			child := newFoundRowsTestExecutor(ctx, 1, 2, 3, 4, 5)
			child.batchSize = batch
			newResult := func() *recordSet {
				require.NoError(t, exec.Open(context.Background(), child))
				return &recordSet{executor: child, stmt: &ExecStmt{Ctx: ctx}}
			}
			rs := newResult()
			req := exec.NewFirstChunk(child)
			require.NoError(t, rs.Next(context.Background(), req))
			require.EqualValues(t, 13, ctx.GetSessionVars().LastFoundRows)
			require.NoError(t, rs.Finish())
			require.EqualValues(t, 13, ctx.GetSessionVars().LastFoundRows)

			failure := errors.New("result failed after two rows")
			child.nextErr, child.failAfter = failure, 2
			rs = newResult()
			for {
				err := rs.Next(context.Background(), req)
				if err != nil {
					require.ErrorIs(t, err, failure)
					break
				}
				require.NotZero(t, req.NumRows())
			}
			require.EqualValues(t, 13, ctx.GetSessionVars().LastFoundRows)
			require.Zero(t, ctx.GetSessionVars().StmtCtx.FoundRows())
			require.NoError(t, rs.Finish())

			child.nextErr = nil
			for range 2 {
				rs = newResult()
				for {
					require.NoError(t, rs.Next(context.Background(), req))
					if req.NumRows() == 0 {
						break
					}
				}
				require.EqualValues(t, 5, ctx.GetSessionVars().LastFoundRows)
				require.EqualValues(t, 5, ctx.GetSessionVars().StmtCtx.FoundRows())
				require.NoError(t, rs.Finish())
			}
			require.Equal(t, child.opens.Load(), child.closes.Load())
		})
	}

	t.Run("close at eof", func(t *testing.T) {
		ctx := mock.NewContext()
		child := newFoundRowsTestExecutor(ctx, 1)
		rs := &recordSet{executor: child, stmt: &ExecStmt{Ctx: ctx}}
		child.onNext = func(_ context.Context, req *chunk.Chunk) error {
			if child.cursor == len(child.values) {
				return rs.Finish()
			}
			return child.nextRows(req)
		}
		require.NoError(t, exec.Open(context.Background(), child))
		req := exec.NewFirstChunk(child)
		require.NoError(t, rs.Next(context.Background(), req))
		require.NoError(t, rs.Next(context.Background(), req))
		require.EqualValues(t, 1, ctx.GetSessionVars().LastFoundRows)
		require.Nil(t, rs.executor)
	})
}
