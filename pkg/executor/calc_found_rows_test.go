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
	"testing"

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
	opens     int
	closes    int
	openErr   error
	nextErr   error
	closeErr  error
	failAfter int
	onEOF     func() error
}

func newFoundRowsTestExecutor(ctx *mock.Context, values ...int64) *foundRowsTestExecutor {
	schema := expression.NewSchema(&expression.Column{RetType: types.NewFieldType(mysql.TypeLonglong)})
	return &foundRowsTestExecutor{BaseExecutor: exec.NewBaseExecutor(ctx, schema, 0), values: values, batchSize: 1}
}

func (e *foundRowsTestExecutor) Open(context.Context) error {
	e.opens++
	e.cursor = 0
	return e.openErr
}

func (e *foundRowsTestExecutor) Next(_ context.Context, req *chunk.Chunk) error {
	req.Reset()
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
	if req.NumRows() == 0 && e.onEOF != nil {
		return e.onEOF()
	}
	return nil
}

func (e *foundRowsTestExecutor) Close() error {
	e.closes++
	return e.closeErr
}

func TestCalcFoundRowsExec(t *testing.T) {
	ctx := context.Background()
	newExec := func() (*CalcFoundRowsExec, *foundRowsTestExecutor, *foundRowsTestExecutor) {
		sctx := mock.NewContext()
		data := newFoundRowsTestExecutor(sctx, 1, 2)
		count := newFoundRowsTestExecutor(sctx, 9)
		return &CalcFoundRowsExec{BaseExecutor: exec.NewBaseExecutor(sctx, data.Schema(), 0, data, count)}, data, count
	}
	t.Run("completion and reopen", func(t *testing.T) {
		e, data, count := newExec()
		for round, total := range []int64{9, 0} {
			if total == 0 {
				data.values = nil
				count.values = []int64{0}
			}
			require.NoError(t, exec.Open(ctx, e))
			req := exec.NewFirstChunk(e)
			var page []int64
			for {
				require.NoError(t, exec.Next(ctx, e, req))
				if req.NumRows() == 0 {
					break
				}
				page = append(page, req.GetRow(0).GetInt64(0))
				require.Equal(t, round, count.opens, "count must wait for data EOF")
				_, available := e.FoundRows()
				require.False(t, available)
			}
			require.Equal(t, data.values, page)
			rows, available := e.FoundRows()
			require.True(t, available)
			require.Equal(t, uint64(total), rows)
			require.NoError(t, e.Close())
			require.Equal(t, round+1, data.opens)
			require.Equal(t, round+1, data.closes)
			require.Equal(t, round+1, count.opens)
			require.Equal(t, round+1, count.closes)
		}
	})
	for _, phase := range []string{"open", "next", "close"} {
		t.Run("count error during "+phase, func(t *testing.T) {
			e, data, count := newExec()
			data.values = nil
			failure := errors.New("count " + phase)
			switch phase {
			case "open":
				count.openErr = failure
			case "next":
				count.nextErr = failure
			case "close":
				count.closeErr = failure
			}
			require.NoError(t, exec.Open(ctx, e))
			require.ErrorIs(t, exec.Next(ctx, e, exec.NewFirstChunk(e)), failure)
			_, available := e.FoundRows()
			require.False(t, available)
			_ = e.Close() // The count child can itself return the injected close error.
			require.Equal(t, 1, data.closes)
			require.Equal(t, 1, count.closes)
		})
	}
	t.Run("early close", func(t *testing.T) {
		e, data, count := newExec()
		require.NoError(t, exec.Open(ctx, e))
		require.NoError(t, exec.Next(ctx, e, exec.NewFirstChunk(e)))
		require.NoError(t, e.Close())
		require.ErrorIs(t, exec.Next(ctx, e, exec.NewFirstChunk(e)), context.Canceled)
		require.NoError(t, e.Close())
		require.Equal(t, 1, data.closes)
		require.Zero(t, count.opens)
	})
	t.Run("cancellation at handoff", func(t *testing.T) {
		e, data, count := newExec()
		data.values = nil
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		fp := "github.com/pingcap/tidb/pkg/executor/afterCalcFoundRowsData"
		require.NoError(t, failpoint.EnableCall(fp, func(sessionctx.Context) { cancel() }))
		t.Cleanup(func() { require.NoError(t, failpoint.Disable(fp)) })
		require.NoError(t, exec.Open(ctx, e))
		require.ErrorIs(t, exec.Next(ctx, e, exec.NewFirstChunk(e)), context.Canceled)
		require.NoError(t, e.Close())
		require.Zero(t, count.opens)
		require.Equal(t, 1, data.closes)
	})
}

func TestRecordSetFoundRows(t *testing.T) {
	for _, batch := range []int{1, 2, 8} {
		t.Run(fmt.Sprintf("chunk size %d", batch), func(t *testing.T) {
			ctx := mock.NewContext()
			ctx.GetSessionVars().LastFoundRows = 13
			child := newFoundRowsTestExecutor(ctx, 1, 2, 3, 4, 5)
			child.batchSize = batch
			for _, outcome := range []string{"early close", "error", "success", "success"} {
				child.nextErr = nil
				if outcome == "error" {
					child.nextErr, child.failAfter = errors.New("result failed after two rows"), 2
				}
				require.NoError(t, exec.Open(context.Background(), child))
				rs := &recordSet{executor: child, stmt: &ExecStmt{Ctx: ctx}}
				req := exec.NewFirstChunk(child)
				for {
					err := rs.Next(context.Background(), req)
					if err != nil {
						require.ErrorIs(t, err, child.nextErr)
						break
					}
					if req.NumRows() == 0 || outcome == "early close" {
						break
					}
				}
				if outcome == "success" {
					require.EqualValues(t, 5, ctx.GetSessionVars().LastFoundRows)
					require.EqualValues(t, 5, ctx.GetSessionVars().StmtCtx.FoundRows())
				} else {
					require.EqualValues(t, 13, ctx.GetSessionVars().LastFoundRows)
					require.Zero(t, ctx.GetSessionVars().StmtCtx.FoundRows())
				}
				require.NoError(t, rs.Finish())
			}
			require.Equal(t, child.opens, child.closes)
		})
	}
	t.Run("close at eof", func(t *testing.T) {
		ctx := mock.NewContext()
		child := newFoundRowsTestExecutor(ctx, 1)
		rs := &recordSet{executor: child, stmt: &ExecStmt{Ctx: ctx}}
		child.onEOF = rs.Finish
		require.NoError(t, exec.Open(context.Background(), child))
		req := exec.NewFirstChunk(child)
		require.NoError(t, rs.Next(context.Background(), req))
		require.NoError(t, rs.Next(context.Background(), req))
		require.EqualValues(t, 1, ctx.GetSessionVars().LastFoundRows)
		require.Nil(t, rs.executor)
	})
}
