// Copyright 2025 PingCAP, Inc.
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

package ddl

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestBackfillWorkerSendsInFlightResultAfterClose(t *testing.T) {
	t.Run("worker downscale preserves result", func(t *testing.T) {
		workCtx, cancelWork := context.WithCancel(context.Background())
		t.Cleanup(cancelWork)
		resultCtx, cancelResult := context.WithCancel(context.Background())
		t.Cleanup(cancelResult)
		worker := newBackfillWorker(workCtx, resultCtx, nil)
		t.Cleanup(worker.Close)
		worker.resultCh = make(chan *backfillResult)
		worker.Close()
		require.ErrorIs(t, worker.workCtx.Err(), context.Canceled)
		cancelWork()
		require.NoError(t, worker.resultCtx.Err())

		want := &backfillResult{taskID: 1}
		go worker.sendResult(want)

		select {
		case got := <-worker.resultCh:
			require.Same(t, want, got)
		case <-time.After(time.Second):
			t.Fatal("in-flight backfill result was dropped after worker close")
		}
	})

	t.Run("executor shutdown can discard result", func(t *testing.T) {
		workCtx, cancelWork := context.WithCancel(context.Background())
		t.Cleanup(cancelWork)
		resultCtx, cancelResult := context.WithCancel(context.Background())
		t.Cleanup(cancelResult)
		worker := newBackfillWorker(workCtx, resultCtx, nil)
		t.Cleanup(worker.Close)
		worker.resultCh = make(chan *backfillResult)
		cancelResult()
		require.NoError(t, worker.workCtx.Err())
		require.ErrorIs(t, worker.resultCtx.Err(), context.Canceled)

		done := make(chan struct{})
		go func() {
			worker.sendResult(&backfillResult{taskID: 1})
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("backfill worker blocked while discarding a result during executor shutdown")
		}
	})
}

func TestTxnBackfillExecutorDownscalePreservesInFlightError(t *testing.T) {
	resultCtx, cancelResult := context.WithCancel(context.Background())
	t.Cleanup(cancelResult)
	resultCh := make(chan *backfillResult)
	workers := []*backfillWorker{
		newBackfillWorker(context.Background(), resultCtx, nil),
		newBackfillWorker(context.Background(), resultCtx, nil),
	}
	for _, worker := range workers {
		worker.resultCh = resultCh
	}

	exec := &txnBackfillExecutor{
		reorgInfo: &reorgInfo{Job: &model.Job{ReorgMeta: &model.DDLReorgMeta{}}},
		workers:   workers,
		resultCtx: resultCtx,
	}
	exec.reorgInfo.ReorgMeta.SetConcurrency(1)
	require.NoError(t, exec.adjustWorkerSize())
	require.Len(t, exec.workers, 1)

	want := errors.New("duplicate key found during backfill")
	go workers[1].sendResult(&backfillResult{taskID: 1, err: want})
	select {
	case result := <-resultCh:
		require.ErrorIs(t, result.err, want)
	case <-time.After(time.Second):
		t.Fatal("in-flight backfill error was dropped after executor downscale")
	}
}

func TestTxnBackfillExecutorForceCloseUnblocksResult(t *testing.T) {
	workCtx, cancelWork := context.WithCancel(context.Background())
	t.Cleanup(cancelWork)
	resultCtx, cancelResult := context.WithCancel(context.Background())
	worker := newBackfillWorker(workCtx, resultCtx, nil)
	worker.resultCh = make(chan *backfillResult)
	exec := &txnBackfillExecutor{
		workers:      []*backfillWorker{worker},
		taskCh:       make(chan *reorgBackfillTask),
		resultCh:     worker.resultCh,
		resultCtx:    resultCtx,
		cancelResult: cancelResult,
	}
	closed := make(chan struct{})
	t.Cleanup(func() {
		cancelResult()
		select {
		case <-closed:
		case <-time.After(time.Second):
			t.Error("executor did not stop after test cleanup canceled result delivery")
		}
	})

	// No collector receives from the unbuffered channel. The worker can only
	// finish sending when forced shutdown cancels result delivery before Wait.
	started := make(chan struct{})
	exec.wg.Add(1)
	go func() {
		defer exec.wg.Done()
		close(started)
		worker.sendResult(&backfillResult{taskID: 1})
	}()
	<-started
	go func() {
		exec.close(true)
		close(closed)
	}()

	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("forced executor close blocked waiting for result delivery")
	}
	require.ErrorIs(t, worker.workCtx.Err(), context.Canceled)
	require.ErrorIs(t, resultCtx.Err(), context.Canceled)
	select {
	case _, ok := <-exec.resultCh:
		require.False(t, ok)
	default:
		t.Fatal("result channel remains open after forced close")
	}
	select {
	case _, ok := <-exec.taskCh:
		require.False(t, ok)
	default:
		t.Fatal("task channel remains open after forced close")
	}
	exec.close(true)
}

func TestExpectedIngestWorkerCnt(t *testing.T) {
	tests := []struct {
		concurrency   int
		avgRowSize    int
		useGlobalSort bool
		expReader     int
		expWriter     int
	}{
		// Global sort path
		{10, 100, true, 10, 10},
		{20, 500, true, 20, 20},

		// Non-global sort path, avgRowSize = 0
		{10, 0, false, 5, 7},
		{40, 0, false, 16, 16},
		{1, 0, false, 1, 2},

		// Non-global sort path, various avgRowSize
		{10, 100, false, 5, 10},
		{10, 300, false, 10, 10},
		{10, 600, false, 20, 10},
		{10, 2000, false, 40, 10},
		{10, 5000, false, 80, 10},
	}

	for _, tt := range tests {
		reader, writer := expectedIngestWorkerCnt(tt.concurrency, tt.avgRowSize, tt.useGlobalSort)
		require.Equal(t, tt.expReader, reader, "concurrency: %d, avgRowSize: %d, useGlobalSort: %v", tt.concurrency, tt.avgRowSize, tt.useGlobalSort)
		require.Equal(t, tt.expWriter, writer, "concurrency: %d, avgRowSize: %d, useGlobalSort: %v", tt.concurrency, tt.avgRowSize, tt.useGlobalSort)
	}
}
