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
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
	pderr "github.com/tikv/pd/client/errs"
)

func TestGetAnalyzeTaskConcurrencyRespectsPartitionAdmission(t *testing.T) {
	ctx := mock.NewContext()
	require.NoError(t, ctx.GetSessionVars().SetSystemVar(variable.TiDBBuildStatsConcurrency, "4"))
	require.NoError(t, ctx.GetSessionVars().SetSystemVar(variable.TiDBAnalyzePartitionConcurrency, "2"))

	tasks := []*analyzeTask{
		newAnalyzePartitionColTask(42, 1001),
		newAnalyzePartitionColTask(42, 1002),
		newAnalyzePartitionColTask(42, 1003),
		newAnalyzePartitionColTask(42, 1004),
	}

	concurrency, err := getAnalyzeTaskConcurrency(ctx, tasks)
	require.NoError(t, err)
	require.Equal(t, 2, concurrency)
}

func TestGetAnalyzeTaskConcurrencyKeepsNonPartitionTasksAtBuildConcurrency(t *testing.T) {
	ctx := mock.NewContext()
	require.NoError(t, ctx.GetSessionVars().SetSystemVar(variable.TiDBBuildStatsConcurrency, "4"))
	require.NoError(t, ctx.GetSessionVars().SetSystemVar(variable.TiDBAnalyzePartitionConcurrency, "1"))

	tasks := []*analyzeTask{
		newAnalyzeTableColTask(42),
		newAnalyzeTableColTask(43),
		newAnalyzeTableColTask(44),
	}

	concurrency, err := getAnalyzeTaskConcurrency(ctx, tasks)
	require.NoError(t, err)
	require.Equal(t, 3, concurrency)
}

func TestGetSamplingStatsConcurrencyRespectsPartitionAdmission(t *testing.T) {
	ctx := mock.NewContext()
	require.NoError(t, ctx.GetSessionVars().SetSystemVar(variable.TiDBBuildStatsConcurrency, "4"))
	require.NoError(t, ctx.GetSessionVars().SetSystemVar(variable.TiDBAnalyzePartitionConcurrency, "2"))

	concurrency, err := getSamplingStatsConcurrency(ctx, statistics.AnalyzeTableID{TableID: 42, PartitionID: 1001}, 8)
	require.NoError(t, err)
	require.Equal(t, 2, concurrency)

	concurrency, err = getSamplingStatsConcurrency(ctx, statistics.AnalyzeTableID{TableID: 42, PartitionID: -1}, 8)
	require.NoError(t, err)
	require.Equal(t, 4, concurrency)
}

func TestGetAnalyzePartitionBudgetSplitsAcrossActiveJobs(t *testing.T) {
	ctx := mock.NewContext()
	require.NoError(t, ctx.GetSessionVars().SetSystemVar(variable.TiDBBuildStatsConcurrency, "8"))
	require.NoError(t, ctx.GetSessionVars().SetSystemVar(variable.TiDBAnalyzePartitionConcurrency, "8"))

	budget, err := getAnalyzePartitionBudget(ctx, 1, 8)
	require.NoError(t, err)
	require.Equal(t, 8, budget)

	budget, err = getAnalyzePartitionBudget(ctx, 2, 8)
	require.NoError(t, err)
	require.Equal(t, 4, budget)
}

func TestGetAnalyzePartitionBudgetShrinksLargePartitionWave(t *testing.T) {
	ctx := mock.NewContext()
	require.NoError(t, ctx.GetSessionVars().SetSystemVar(variable.TiDBBuildStatsConcurrency, "8"))
	require.NoError(t, ctx.GetSessionVars().SetSystemVar(variable.TiDBAnalyzePartitionConcurrency, "8"))

	budget, err := getAnalyzePartitionBudget(ctx, 1, 300)
	require.NoError(t, err)
	require.Equal(t, 4, budget)

	budget, err = getAnalyzePartitionBudget(ctx, 1, 2000)
	require.NoError(t, err)
	require.Equal(t, 2, budget)

	budget, err = getAnalyzePartitionBudget(ctx, 2, 2000)
	require.NoError(t, err)
	require.Equal(t, 1, budget)
}

func TestGetSamplingStatsConcurrencyRespectsStatementBudget(t *testing.T) {
	ctx := mock.NewContext()
	require.NoError(t, ctx.GetSessionVars().SetSystemVar(variable.TiDBBuildStatsConcurrency, "8"))
	require.NoError(t, ctx.GetSessionVars().SetSystemVar(variable.TiDBAnalyzePartitionConcurrency, "8"))

	concurrency, err := getSamplingStatsConcurrencyWithBudget(
		ctx,
		statistics.AnalyzeTableID{TableID: 42, PartitionID: 1001},
		8,
		8,
		8,
	)
	require.NoError(t, err)
	require.Equal(t, 1, concurrency)

	concurrency, err = getSamplingStatsConcurrencyWithBudget(
		ctx,
		statistics.AnalyzeTableID{TableID: 42, PartitionID: 1001},
		8,
		4,
		4,
	)
	require.NoError(t, err)
	require.Equal(t, 1, concurrency)

	concurrency, err = getSamplingStatsConcurrencyWithBudget(
		ctx,
		statistics.AnalyzeTableID{TableID: 42, PartitionID: 1001},
		8,
		8,
		1,
	)
	require.NoError(t, err)
	require.Equal(t, 8, concurrency)
}

func TestGetAnalyzeStatsPersistConcurrencyUsesIndependentBudget(t *testing.T) {
	ctx := mock.NewContext()
	require.NoError(t, ctx.GetSessionVars().SetSystemVar(variable.TiDBAnalyzePartitionConcurrency, "8"))

	concurrency := getAnalyzeStatsPersistConcurrency(ctx, 8)
	require.Equal(t, 1, concurrency)

	concurrency = getAnalyzeStatsPersistConcurrency(ctx, 0)
	require.Equal(t, 1, concurrency)
}

func TestGetAnalyzeResultsChannelCapacityBackpressuresPartitionSaveBacklog(t *testing.T) {
	ctx := mock.NewContext()
	require.NoError(t, ctx.GetSessionVars().SetSystemVar(variable.TiDBAnalyzePartitionConcurrency, "8"))

	tasks := []*analyzeTask{
		newAnalyzePartitionColTask(42, 1001),
		newAnalyzePartitionColTask(42, 1002),
		newAnalyzePartitionColTask(42, 1003),
		newAnalyzePartitionColTask(42, 1004),
		newAnalyzePartitionColTask(42, 1005),
		newAnalyzePartitionColTask(42, 1006),
		newAnalyzePartitionColTask(42, 1007),
		newAnalyzePartitionColTask(42, 1008),
	}

	capacity := getAnalyzeResultsChannelCapacity(ctx, tasks, 8)
	require.Equal(t, 4, capacity)
}

func TestGetAnalyzeResultsChannelCapacityKeepsSmallOrNonPartitionWavesUnchanged(t *testing.T) {
	ctx := mock.NewContext()
	require.NoError(t, ctx.GetSessionVars().SetSystemVar(variable.TiDBAnalyzePartitionConcurrency, "8"))

	smallPartitionTasks := []*analyzeTask{
		newAnalyzePartitionColTask(42, 1001),
		newAnalyzePartitionColTask(42, 1002),
	}
	capacity := getAnalyzeResultsChannelCapacity(ctx, smallPartitionTasks, 8)
	require.Equal(t, 2, capacity)

	nonPartitionTasks := []*analyzeTask{
		newAnalyzeTableColTask(42),
		newAnalyzeTableColTask(43),
		newAnalyzeTableColTask(44),
		newAnalyzeTableColTask(45),
		newAnalyzeTableColTask(46),
	}
	capacity = getAnalyzeResultsChannelCapacity(ctx, nonPartitionTasks, 8)
	require.Equal(t, 5, capacity)
}

func TestGetAnalyzeGlobalStatsMergeConcurrencyKeepsConfiguredValue(t *testing.T) {
	ctx := mock.NewContext()
	require.NoError(t, ctx.GetSessionVars().SetSystemVar(variable.TiDBMergePartitionStatsConcurrency, "1"))

	concurrency := getAnalyzeGlobalStatsMergeConcurrency(ctx)
	require.Equal(t, 1, concurrency)

	concurrency = getAnalyzeGlobalStatsMergeConcurrency(ctx)
	require.Equal(t, 1, concurrency)

	concurrency = getAnalyzeGlobalStatsMergeConcurrency(ctx)
	require.Equal(t, 1, concurrency)
}

func TestGetAnalyzeGlobalStatsMergeConcurrencyKeepsExplicitHigherSetting(t *testing.T) {
	ctx := mock.NewContext()
	require.NoError(t, ctx.GetSessionVars().SetSystemVar(variable.TiDBMergePartitionStatsConcurrency, "4"))

	concurrency := getAnalyzeGlobalStatsMergeConcurrency(ctx)
	require.Equal(t, 4, concurrency)
}

func TestHandleResultsErrorDrainsInterruptedResults(t *testing.T) {
	ctx := mock.NewContext()
	atomic.StoreUint32(&ctx.GetSessionVars().Killed, 1)
	exec := &AnalyzeExec{baseExecutor: baseExecutor{ctx: ctx}}

	resultsCh := make(chan *statistics.AnalyzeResults)
	sent := make(chan struct{})
	go func() {
		resultsCh <- &statistics.AnalyzeResults{Job: &statistics.AnalyzeJob{DBName: "repro", TableName: "t_part"}}
		resultsCh <- &statistics.AnalyzeResults{Job: &statistics.AnalyzeJob{DBName: "repro", TableName: "t_part"}}
		close(resultsCh)
		close(sent)
	}()

	err := exec.handleResultsError(context.Background(), 1, false, make(globalStatsMap), resultsCh)
	require.ErrorIs(t, err, ErrQueryInterrupted)
	select {
	case <-sent:
	case <-time.After(2 * time.Second):
		t.Fatal("interrupted drain should consume all analyze results")
	}
}

func TestHandleResultsErrorWithConcurrencyDrainsInterruptedResults(t *testing.T) {
	ctx := mock.NewContext()
	atomic.StoreUint32(&ctx.GetSessionVars().Killed, 1)
	exec := &AnalyzeExec{baseExecutor: baseExecutor{ctx: ctx}}

	resultsCh := make(chan *statistics.AnalyzeResults)
	sent := make(chan struct{})
	go func() {
		resultsCh <- &statistics.AnalyzeResults{Job: &statistics.AnalyzeJob{DBName: "repro", TableName: "t_part"}}
		resultsCh <- &statistics.AnalyzeResults{Job: &statistics.AnalyzeJob{DBName: "repro", TableName: "t_part"}}
		close(resultsCh)
		close(sent)
	}()

	subSctxs := []sessionctx.Context{mock.NewContext()}
	err := exec.handleResultsErrorWithConcurrency(context.Background(), 1, false, subSctxs, make(globalStatsMap), resultsCh)
	require.ErrorIs(t, err, ErrQueryInterrupted)
	select {
	case <-sent:
	case <-time.After(2 * time.Second):
		t.Fatal("interrupted concurrent drain should consume all analyze results")
	}
}

func TestHandleResultsErrorWithConcurrencyDrainsAfterSaveFailure(t *testing.T) {
	ctx := mock.NewContext()
	exec := &AnalyzeExec{baseExecutor: baseExecutor{ctx: ctx}}

	origPersist := persistAnalyzeTableStats
	t.Cleanup(func() {
		persistAnalyzeTableStats = origPersist
	})
	var saveCalls int32
	persistAnalyzeTableStats = func(sessionctx.Context, *statistics.AnalyzeResults, bool) error {
		if atomic.AddInt32(&saveCalls, 1) == 1 {
			return errors.New("save failed")
		}
		t.Fatalf("unexpected second save attempt after the first save failure")
		return nil
	}

	resultsCh := make(chan *statistics.AnalyzeResults)
	sent := make(chan struct{})
	go func() {
		resultsCh <- &statistics.AnalyzeResults{Job: &statistics.AnalyzeJob{DBName: "repro", TableName: "t_part", PartitionName: "p1350"}}
		resultsCh <- &statistics.AnalyzeResults{Job: &statistics.AnalyzeJob{DBName: "repro", TableName: "t_part", PartitionName: "p1351"}}
		close(resultsCh)
		close(sent)
	}()

	subSctxs := []sessionctx.Context{mock.NewContext()}
	done := make(chan error, 1)
	go func() {
		done <- exec.handleResultsErrorWithConcurrency(context.Background(), 1, false, subSctxs, make(globalStatsMap), resultsCh)
	}()

	select {
	case <-sent:
	case <-time.After(2 * time.Second):
		t.Fatal("save failure should not block draining later analyze results")
	}

	select {
	case err := <-done:
		require.ErrorContains(t, err, "save failed")
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent save path should return after draining later analyze results")
	}

	require.Equal(t, int32(1), atomic.LoadInt32(&saveCalls))
}

func TestSaveAnalyzeTableStatsWithRetryRetriesTransientFailure(t *testing.T) {
	ctx := mock.NewContext()
	origPersist := persistAnalyzeTableStats
	origRecordHistorical := recordAnalyzeHistoricalStats
	t.Cleanup(func() {
		persistAnalyzeTableStats = origPersist
		recordAnalyzeHistoricalStats = origRecordHistorical
	})
	recordAnalyzeHistoricalStats = func(sessionctx.Context, int64) error { return nil }
	var saveCalls int32
	persistAnalyzeTableStats = func(sessionctx.Context, *statistics.AnalyzeResults, bool) error {
		if atomic.AddInt32(&saveCalls, 1) == 1 {
			return pderr.ErrClientGetTSO.FastGenByArgs(
				"rpc error: code = Unknown desc = [PD:tso:ErrGenerateTimestamp]generate timestamp failed, requested pd is not leader of cluster",
			)
		}
		return nil
	}

	results := &statistics.AnalyzeResults{Job: &statistics.AnalyzeJob{DBName: "repro", TableName: "t_part"}}
	err := saveAnalyzeTableStatsWithRetry(context.Background(), ctx, results, false, &ctx.GetSessionVars().Killed)
	require.NoError(t, err)
	require.Equal(t, int32(2), atomic.LoadInt32(&saveCalls))
}

func TestSaveAnalyzeTableStatsWithRetryRetriesLeaderChangeStringError(t *testing.T) {
	ctx := mock.NewContext()
	origPersist := persistAnalyzeTableStats
	origRecordHistorical := recordAnalyzeHistoricalStats
	t.Cleanup(func() {
		persistAnalyzeTableStats = origPersist
		recordAnalyzeHistoricalStats = origRecordHistorical
	})
	recordAnalyzeHistoricalStats = func(sessionctx.Context, int64) error { return nil }
	var saveCalls int32
	persistAnalyzeTableStats = func(sessionctx.Context, *statistics.AnalyzeResults, bool) error {
		if atomic.AddInt32(&saveCalls, 1) == 1 {
			return errors.New("rpc error: code = Unknown desc = [PD:tso:ErrGenerateTimestamp]generate timestamp failed, requested pd is not leader of cluster")
		}
		return nil
	}

	results := &statistics.AnalyzeResults{Job: &statistics.AnalyzeJob{DBName: "repro", TableName: "t_part"}}
	err := saveAnalyzeTableStatsWithRetry(context.Background(), ctx, results, false, &ctx.GetSessionVars().Killed)
	require.NoError(t, err)
	require.Equal(t, int32(2), atomic.LoadInt32(&saveCalls))
}

func TestSaveAnalyzeTableStatsWithRetryKeepsNonTransientFailure(t *testing.T) {
	ctx := mock.NewContext()
	origPersist := persistAnalyzeTableStats
	origRecordHistorical := recordAnalyzeHistoricalStats
	t.Cleanup(func() {
		persistAnalyzeTableStats = origPersist
		recordAnalyzeHistoricalStats = origRecordHistorical
	})
	recordAnalyzeHistoricalStats = func(sessionctx.Context, int64) error { return nil }
	var saveCalls int32
	persistAnalyzeTableStats = func(sessionctx.Context, *statistics.AnalyzeResults, bool) error {
		atomic.AddInt32(&saveCalls, 1)
		return errors.New("disk quota exceeded")
	}

	results := &statistics.AnalyzeResults{Job: &statistics.AnalyzeJob{DBName: "repro", TableName: "t_part"}}
	err := saveAnalyzeTableStatsWithRetry(context.Background(), ctx, results, false, &ctx.GetSessionVars().Killed)
	require.ErrorContains(t, err, "disk quota exceeded")
	require.Equal(t, int32(1), atomic.LoadInt32(&saveCalls))
}

func TestSaveAnalyzeTableStatsWithRetryStopsAfterKill(t *testing.T) {
	ctx := mock.NewContext()
	origPersist := persistAnalyzeTableStats
	t.Cleanup(func() {
		persistAnalyzeTableStats = origPersist
	})
	var saveCalls int32
	persistAnalyzeTableStats = func(sessionctx.Context, *statistics.AnalyzeResults, bool) error {
		atomic.AddInt32(&saveCalls, 1)
		atomic.StoreUint32(&ctx.GetSessionVars().Killed, 1)
		return pderr.ErrClientGetTSO.FastGenByArgs("temporary TSO failure")
	}

	results := &statistics.AnalyzeResults{Job: &statistics.AnalyzeJob{DBName: "repro", TableName: "t_part"}}
	err := saveAnalyzeTableStatsWithRetry(context.Background(), ctx, results, false, &ctx.GetSessionVars().Killed)
	require.ErrorIs(t, err, ErrQueryInterrupted)
	require.Equal(t, int32(1), atomic.LoadInt32(&saveCalls))
}

func TestSaveAnalyzeTableStatsWithRetryStopsOnCanceledContext(t *testing.T) {
	sctx := mock.NewContext()
	origPersist := persistAnalyzeTableStats
	t.Cleanup(func() {
		persistAnalyzeTableStats = origPersist
	})
	var saveCalls int32
	persistAnalyzeTableStats = func(sessionctx.Context, *statistics.AnalyzeResults, bool) error {
		atomic.AddInt32(&saveCalls, 1)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	results := &statistics.AnalyzeResults{Job: &statistics.AnalyzeJob{DBName: "repro", TableName: "t_part"}}
	err := saveAnalyzeTableStatsWithRetry(ctx, sctx, results, false, &sctx.GetSessionVars().Killed)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, atomic.LoadInt32(&saveCalls))
}

func TestHandleResultsErrorWithConcurrencyRetriesTransientSaveFailure(t *testing.T) {
	ctx := mock.NewContext()
	exec := &AnalyzeExec{baseExecutor: baseExecutor{ctx: ctx}}

	origPersist := persistAnalyzeTableStats
	origRecordHistorical := recordAnalyzeHistoricalStats
	t.Cleanup(func() {
		persistAnalyzeTableStats = origPersist
		recordAnalyzeHistoricalStats = origRecordHistorical
	})
	recordAnalyzeHistoricalStats = func(sessionctx.Context, int64) error { return nil }
	var saveCalls int32
	persistAnalyzeTableStats = func(sessionctx.Context, *statistics.AnalyzeResults, bool) error {
		if atomic.AddInt32(&saveCalls, 1) == 1 {
			return pderr.ErrClientGetTSO.FastGenByArgs(
				"rpc error: code = Unknown desc = [PD:tso:ErrGenerateTimestamp]generate timestamp failed, requested pd is not leader of cluster",
			)
		}
		return nil
	}

	resultsCh := make(chan *statistics.AnalyzeResults)
	sent := make(chan struct{})
	go func() {
		resultsCh <- &statistics.AnalyzeResults{Job: &statistics.AnalyzeJob{DBName: "repro", TableName: "t_part", PartitionName: "p1350"}}
		resultsCh <- &statistics.AnalyzeResults{Job: &statistics.AnalyzeJob{DBName: "repro", TableName: "t_part", PartitionName: "p1351"}}
		close(resultsCh)
		close(sent)
	}()

	subSctxs := []sessionctx.Context{mock.NewContext()}
	done := make(chan error, 1)
	go func() {
		done <- exec.handleResultsErrorWithConcurrency(context.Background(), 1, false, subSctxs, make(globalStatsMap), resultsCh)
	}()

	select {
	case <-sent:
	case <-time.After(2 * time.Second):
		t.Fatal("transient save retry should not block later analyze results")
	}

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent save path should finish after retrying transient save failure")
	}

	require.Equal(t, int32(3), atomic.LoadInt32(&saveCalls))
}

func newAnalyzePartitionColTask(tableID, partitionID int64) *analyzeTask {
	return &analyzeTask{
		taskType: colTask,
		colExec: &AnalyzeColumnsExec{
			baseAnalyzeExec: baseAnalyzeExec{
				tableID: statistics.AnalyzeTableID{
					TableID:     tableID,
					PartitionID: partitionID,
				},
			},
		},
	}
}

func newAnalyzeTableColTask(tableID int64) *analyzeTask {
	return &analyzeTask{
		taskType: colTask,
		colExec: &AnalyzeColumnsExec{
			baseAnalyzeExec: baseAnalyzeExec{
				tableID: statistics.AnalyzeTableID{
					TableID:     tableID,
					PartitionID: -1,
				},
			},
		},
	}
}
