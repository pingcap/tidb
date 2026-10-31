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
