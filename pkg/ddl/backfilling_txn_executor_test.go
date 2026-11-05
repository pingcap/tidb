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

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	sqlexecmock "github.com/pingcap/tidb/pkg/util/sqlexec/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// TestBackfillWorkerSendsInFlightResultAfterClose separates retirement, complete-range execution and executor cancellation.
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

	duplicateErr := errors.New("duplicate key found during backfill")
	for _, tc := range []struct {
		name           string
		retire         bool
		cancelExecutor bool
		backfillErr    error
		wantErr        error
	}{
		{name: "in-flight range completes without downscale"},
		{name: "retirement finishes the complete in-flight range", retire: true},
		{name: "retirement preserves a genuine task error", retire: true, backfillErr: duplicateErr, wantErr: duplicateErr},
		{name: "executor cancellation interrupts in-flight work", cancelExecutor: true, wantErr: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			d := &ddlCtx{ctx: context.Background(), ownerManager: backfillWorkerTestOwner{}}
			d.reorgCtx.reorgCtxMap = make(map[int64]*reorgCtx)
			d.newReorgCtx(1, 0)
			started := make(chan struct{})
			release := make(chan struct{})
			calls := 0
			bf := &backfillWorkerTestBackfiller{backfill: func(ctx context.Context, task reorgBackfillTask) (backfillTaskContext, error) {
				calls++
				if calls == 1 {
					close(started)
					<-release
				}
				if err := ctx.Err(); err != nil {
					return backfillTaskContext{}, err
				}
				if tc.backfillErr != nil {
					return backfillTaskContext{}, tc.backfillErr
				}
				if calls == 1 {
					return backfillTaskContext{nextKey: []byte("b"), addedCount: 1, scanCount: 1}, nil
				}
				return backfillTaskContext{nextKey: task.endKey, addedCount: 1, scanCount: 1, done: true}, nil
			}}
			worker := newBackfillWorker(ctx, ctx, bf)
			t.Cleanup(worker.Close)
			done := make(chan *backfillResult, 1)
			go func() {
				done <- worker.handleBackfillTask(d, &reorgBackfillTask{
					physicalTable: backfillWorkerTestTable{}, jobID: 1, startKey: []byte("a"), endKey: []byte("c"),
				}, bf)
			}()
			<-started
			if tc.retire {
				worker.Close()
			}
			if tc.cancelExecutor {
				cancel()
			}
			close(release)
			select {
			case result := <-done:
				if tc.wantErr != nil {
					require.ErrorIs(t, result.err, tc.wantErr)
					require.Equal(t, 1, calls)
				} else {
					require.NoError(t, result.err)
					require.Equal(t, 2, calls)
					require.Equal(t, "c", string(result.nextKey))
					require.Equal(t, 2, result.addedCount)
					require.Equal(t, int64(2), d.getReorgCtx(1).getRowCount())
				}
			case <-time.After(time.Second):
				t.Fatal("in-flight task did not finish after releasing its batch")
			}
		})
	}
}

type backfillWorkerTestOwner struct{ owner.Manager }

// IsOwner removes lease changes from the lifecycle fixture.
func (backfillWorkerTestOwner) IsOwner() bool { return true }

type backfillWorkerTestTable struct{ table.PhysicalTable }

// GetPhysicalID identifies the fake range without creating a persistent table.
func (backfillWorkerTestTable) GetPhysicalID() int64 { return 1 }

type backfillWorkerTestBackfiller struct {
	backfiller
	backfill func(context.Context, reorgBackfillTask) (backfillTaskContext, error)
}

// BackfillData lets each case control batch completion and errors.
func (b *backfillWorkerTestBackfiller) BackfillData(ctx context.Context, task reorgBackfillTask) (backfillTaskContext, error) {
	return b.backfill(ctx, task)
}

// AddMetricInfo omits throughput metrics from the lifecycle fixture.
func (*backfillWorkerTestBackfiller) AddMetricInfo(float64) {}

// GetCtx supplies minimal worker metadata for the fake backfiller.
func (*backfillWorkerTestBackfiller) GetCtx() *backfillCtx { return &backfillCtx{} }

// String gives the fake a stable label in worker logs.
func (*backfillWorkerTestBackfiller) String() string { return "test" }

// backfillWorkerTestSession keeps the resize test independent of real SQL sessions.
type backfillWorkerTestSession struct {
	*mock.Context
	executor sqlexec.RestrictedSQLExecutor
}

// GetRestrictedSQLExecutor lets the release-8.5 variable loader run against the expected query.
func (s *backfillWorkerTestSession) GetRestrictedSQLExecutor() sqlexec.RestrictedSQLExecutor {
	return s.executor
}

// TestTxnBackfillExecutorDownscalePreservesInFlightError exercises resize and late-error delivery with a valid session pool.
func TestTxnBackfillExecutorDownscalePreservesInFlightError(t *testing.T) {
	ctrl := gomock.NewController(t)
	sqlExec := sqlexecmock.NewMockRestrictedSQLExecutor(ctrl)
	sqlExec.EXPECT().ExecRestrictedSQL(gomock.Any(), nil, gomock.Any(),
		variable.TiDBDDLReorgWorkerCount, variable.TiDBDDLReorgBatchSize, variable.TiDBRowFormatVersion).
		Return(nil, nil, nil).Times(1)
	resPool := pools.NewResourcePool(func() (pools.Resource, error) {
		return &backfillWorkerTestSession{Context: mock.NewContext(), executor: sqlExec}, nil
	}, 1, 1, 0)
	sessPool := sess.NewSessionPool(resPool)
	t.Cleanup(sessPool.Close)
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
		ctx:       resultCtx,
		sessPool:  sessPool,
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

// TestTxnBackfillExecutorForceCloseUnblocksResult verifies shutdown cancels delivery before waiting for workers.
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

// TestExpectedIngestWorkerCnt covers the release-8.5 reader fanout and concurrency caps.
func TestExpectedIngestWorkerCnt(t *testing.T) {
	tests := []struct {
		concurrency int
		avgRowSize  int
		expReader   int
		expWriter   int
	}{
		// Without row-size statistics, both worker counts are capped.
		{10, 0, 5, 7},
		{40, 0, 16, 16},
		{1, 0, 1, 2},

		// Row-size estimates vary reader fanout while retaining writer concurrency.
		{10, 100, 5, 10},
		{10, 300, 10, 10},
		{10, 600, 20, 10},
		{10, 2000, 40, 10},
		{10, 5000, 80, 10},
	}

	for _, tt := range tests {
		reader, writer := expectedIngestWorkerCnt(tt.concurrency, tt.avgRowSize)
		require.Equal(t, tt.expReader, reader, "concurrency: %d, avgRowSize: %d", tt.concurrency, tt.avgRowSize)
		require.Equal(t, tt.expWriter, writer, "concurrency: %d, avgRowSize: %d", tt.concurrency, tt.avgRowSize)
	}
}
