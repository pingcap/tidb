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
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
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
		col := &expression.Column{RetType: types.NewFieldType(mysql.TypeLonglong)}
		reader := &oversizedOuterReader{
			BaseExecutor: exec.NewBaseExecutor(mock.NewContext(), expression.NewSchema(col), 1),
		}
		controller := exec.NewAdaptiveLimitOuterJoinController(exec.AdaptiveLimitConfig{
			DemandRows: 3, InitialOuterWindow: 3, MaxOuterWindow: 3,
		})
		worker := &outerWorker{
			OuterCtx:         OuterCtx{RowTypes: reader.RetFieldTypes()},
			lookup:           &IndexLookUpJoin{AdaptiveLimitController: controller},
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
