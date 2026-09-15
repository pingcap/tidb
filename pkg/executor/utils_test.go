// Copyright 2020 PingCAP, Inc.
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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
)

func TestBatchRetrieverHelper(t *testing.T) {
	rangeStarts := make([]int, 0)
	rangeEnds := make([]int, 0)
	collect := func(start, end int) error {
		rangeStarts = append(rangeStarts, start)
		rangeEnds = append(rangeEnds, end)
		return nil
	}

	r := &batchRetrieverHelper{}
	err := r.nextBatch(collect)
	require.NoError(t, err)
	require.Equal(t, rangeStarts, []int{})
	require.Equal(t, rangeEnds, []int{})

	r = &batchRetrieverHelper{
		retrieved: true,
		batchSize: 3,
		totalRows: 10,
	}
	err = r.nextBatch(collect)
	require.NoError(t, err)
	require.Equal(t, rangeStarts, []int{})
	require.Equal(t, rangeEnds, []int{})

	r = &batchRetrieverHelper{
		batchSize: 3,
		totalRows: 10,
	}
	err = r.nextBatch(func(start, end int) error {
		return errors.New("some error")
	})
	require.Error(t, err)
	require.True(t, r.retrieved)

	r = &batchRetrieverHelper{
		batchSize: 3,
		totalRows: 10,
	}
	for !r.retrieved {
		err = r.nextBatch(collect)
		require.NoError(t, err)
	}
	require.Equal(t, rangeStarts, []int{0, 3, 6, 9})
	require.Equal(t, rangeEnds, []int{3, 6, 9, 10})
	rangeStarts = rangeStarts[:0]
	rangeEnds = rangeEnds[:0]

	r = &batchRetrieverHelper{
		batchSize: 3,
		totalRows: 9,
	}
	for !r.retrieved {
		err = r.nextBatch(collect)
		require.NoError(t, err)
	}
	require.Equal(t, rangeStarts, []int{0, 3, 6})
	require.Equal(t, rangeEnds, []int{3, 6, 9})
	rangeStarts = rangeStarts[:0]
	rangeEnds = rangeEnds[:0]

	r = &batchRetrieverHelper{
		batchSize: 100,
		totalRows: 10,
	}
	for !r.retrieved {
		err = r.nextBatch(collect)
		require.NoError(t, err)
	}
	require.Equal(t, rangeStarts, []int{0})
	require.Equal(t, rangeEnds, []int{10})
}

func TestEqualDatumsAsBinary(t *testing.T) {
	tests := []struct {
		a    []any
		b    []any
		same bool
	}{
		// Positive cases
		{[]any{1}, []any{1}, true},
		{[]any{1, "aa"}, []any{1, "aa"}, true},
		{[]any{1, "aa", 1}, []any{1, "aa", 1}, true},

		// negative cases
		{[]any{1}, []any{2}, false},
		{[]any{1, "a"}, []any{1, "aaaaaa"}, false},
		{[]any{1, "aa", 3}, []any{1, "aa", 2}, false},

		// Corner cases
		{[]any{}, []any{}, true},
		{[]any{nil}, []any{nil}, true},
		{[]any{}, []any{1}, false},
		{[]any{1}, []any{1, 1}, false},
		{[]any{nil}, []any{1}, false},
	}
	ctx := coretestsdk.MockContext()
	base := exec.NewBaseExecutor(ctx, nil, 0)
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	e := &InsertValues{BaseExecutor: base}
	for _, tt := range tests {
		res, err := e.equalDatumsAsBinary(types.MakeDatums(tt.a...), types.MakeDatums(tt.b...))
		require.NoError(t, err)
		require.Equal(t, tt.same, res)
	}
}

func TestEncodePasswordWithPlugin(t *testing.T) {
	hashString := "*3D56A309CD04FA2EEF181462E59011F075C89548"
	u := &ast.UserSpec{
		User: &auth.UserIdentity{
			Username: "test",
		},
		AuthOpt: &ast.AuthOption{
			ByAuthString: false,
			AuthString:   "xxx",
			HashString:   hashString,
		},
	}

	p := &extension.AuthPlugin{
		ValidateAuthString: func(s string) bool {
			return false
		},
		GenerateAuthString: func(s string) (string, bool) {
			if s == "xxx" {
				return "xxxxxxx", true
			}
			return "", false
		},
	}

	u.AuthOpt.ByAuthString = false
	_, ok := encodePasswordWithPlugin(*u, p, "")
	require.False(t, ok)

	u.AuthOpt.AuthString = "xxx"
	u.AuthOpt.ByAuthString = true
	pwd, ok := encodePasswordWithPlugin(*u, p, "")
	require.True(t, ok)
	require.Equal(t, "xxxxxxx", pwd)

	u.AuthOpt = nil
	pwd, ok = encodePasswordWithPlugin(*u, p, "")
	require.True(t, ok)
	require.Equal(t, "", pwd)
}

func TestWorkerPool(t *testing.T) {
	var (
		list []int
		lock sync.Mutex
	)
	push := func(i int) {
		lock.Lock()
		list = append(list, i)
		lock.Unlock()
	}
	clean := func() {
		lock.Lock()
		list = list[:0]
		lock.Unlock()
	}
	sleep := func(ms int) {
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}

	t.Run("SingleWorker", func(t *testing.T) {
		clean()
		pool := &workerPool{
			needSpawn: func(workers, tasks uint32) bool {
				return workers < 1 && tasks > 0
			},
		}
		wg := sync.WaitGroup{}
		wg.Add(1)
		pool.submit(func() {
			push(1)
			wg.Add(1)
			pool.submit(func() {
				push(3)
				sleep(10)
				push(4)
				wg.Done()
			})
			sleep(1)
			push(2)
			wg.Done()
		})
		wg.Wait()
		require.Equal(t, []int{1, 2, 3, 4}, list)
	})

	t.Run("TwoWorkers", func(t *testing.T) {
		clean()
		pool := &workerPool{
			needSpawn: func(workers, tasks uint32) bool {
				return workers < 2 && tasks > 0
			},
		}
		secondWorkerStarted := make(chan struct{})
		finishSecondWorker := make(chan struct{})
		errCh := make(chan string, 1)
		wg := sync.WaitGroup{}
		wg.Add(1)
		pool.submit(func() {
			push(1)
			wg.Add(1)
			pool.submit(func() {
				push(3)
				close(secondWorkerStarted)
				<-finishSecondWorker
				push(4)
				wg.Done()
			})
			select {
			case <-secondWorkerStarted:
			case <-time.After(5 * time.Second):
				errCh <- "the second worker did not start the queued task"
				close(finishSecondWorker)
				wg.Done()
				return
			}
			push(2)
			close(finishSecondWorker)
			wg.Done()
		})
		wg.Wait()
		select {
		case err := <-errCh:
			require.Fail(t, err)
			return
		default:
		}
		require.Equal(t, []int{1, 3, 2, 4}, list)
	})

	t.Run("TolerateOnePendingTask", func(t *testing.T) {
		clean()
		pool := &workerPool{
			needSpawn: func(workers, tasks uint32) bool {
				return workers < 2 && tasks > 1
			},
		}
		wg := sync.WaitGroup{}
		wg.Add(1)
		pool.submit(func() {
			push(1)
			wg.Add(1)
			pool.submit(func() {
				push(3)
				sleep(10)
				push(4)
				wg.Done()
			})
			sleep(1)
			push(2)
			wg.Done()
		})
		wg.Wait()
		require.Equal(t, []int{1, 2, 3, 4}, list)
	})
}

func TestEncodedPassword(t *testing.T) {
	hashString := "*3D56A309CD04FA2EEF181462E59011F075C89548"
	hashCachingString := "0123456789012345678901234567890123456789012345678901234567890123456789"
	u := ast.UserSpec{
		User: &auth.UserIdentity{
			Username: "test",
		},
		AuthOpt: &ast.AuthOption{
			ByAuthString: false,
			AuthString:   "xxx",
			HashString:   hashString,
		},
	}
	pwd, ok := encodedPassword(&u, "")
	require.True(t, ok)
	require.Equal(t, u.AuthOpt.HashString, pwd)

	u.AuthOpt.HashString = "not-good-password-format"
	_, ok = encodedPassword(&u, "")
	require.False(t, ok)

	u.AuthOpt.ByAuthString = true
	// mysql_native_password
	pwd, ok = encodedPassword(&u, "")
	require.True(t, ok)
	require.Equal(t, hashString, pwd)
	// caching_sha2_password
	u.AuthOpt.HashString = hashCachingString
	pwd, ok = encodedPassword(&u, mysql.AuthCachingSha2Password)
	require.True(t, ok)
	require.Len(t, pwd, mysql.SHAPWDHashLen)

	u.AuthOpt.AuthString = ""
	pwd, ok = encodedPassword(&u, "")
	require.True(t, ok)
	require.Equal(t, "", pwd)
}

func TestCancelMaterializedViewJobPrecheckErrorNotRewritten(t *testing.T) {
	ctx := coretestsdk.MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	e := &CancelMaterializedViewJobExec{
		BaseExecutor: exec.NewBaseExecutor(ctx, nil, 0),
		stmt: &ast.CancelMaterializedViewJobStmt{
			Tp:    ast.CancelMaterializedViewJobType(255),
			JobID: 123,
		},
	}

	err := e.Next(context.Background(), nil)
	require.ErrorContains(t, err, "invalid materialized view job cancel type: 255")
}

type mockSQLExecutor struct {
	calls []struct {
		sql  string
		args []any
	}
}

func (m *mockSQLExecutor) Execute(context.Context, string) ([]sqlexec.RecordSet, error) {
	panic("unexpected Execute call")
}

func (m *mockSQLExecutor) ExecuteInternal(_ context.Context, sql string, args ...any) (sqlexec.RecordSet, error) {
	m.calls = append(m.calls, struct {
		sql  string
		args []any
	}{sql: sql, args: append([]any{}, args...)})
	return nil, nil
}

func (m *mockSQLExecutor) ExecuteStmt(context.Context, ast.StmtNode) (sqlexec.RecordSet, error) {
	panic("unexpected ExecuteStmt call")
}

func TestUpdateMaterializedViewLogPurgeInfoOnSuccessMonotonicCheckpoint(t *testing.T) {
	exec := &mockSQLExecutor{}
	lastPurgedTSO := uint64(200)
	nextPurgeUnixSeconds := int64(1_772_928_000)
	require.NoError(t, updateMaterializedViewLogPurgeInfoOnSuccess(context.Background(), exec, int64(123), &lastPurgedTSO, &nextPurgeUnixSeconds, true))
	require.Len(t, exec.calls, 2)
	require.Contains(t, exec.calls[0].sql, "LAST_PURGED_TSO = %?")
	require.Contains(t, exec.calls[0].sql, "LAST_PURGED_TSO IS NULL OR LAST_PURGED_TSO < %?")
	require.Equal(t, []any{uint64(200), int64(123), uint64(200)}, exec.calls[0].args)
	require.Contains(t, exec.calls[1].sql, "NEXT_PURGE_UNIX_SECONDS = %?")
	require.Equal(t, []any{int64(1_772_928_000), int64(123)}, exec.calls[1].args)
}

func TestMLogPurgeAdaptiveBatchSizeComputed(t *testing.T) {
	plan := &mlogPurgeThrottlePlan{targetRate: 2000}
	require.Equal(t, int64(8000), plan.effectiveDeleteBatchSize(10000))
	plan = &mlogPurgeThrottlePlan{targetRate: 100000}
	require.Equal(t, int64(10000), plan.effectiveDeleteBatchSize(10000))
}

func TestMLogPurgeAdaptiveBatchSizeReplannedAfterNoWait(t *testing.T) {
	plan := &mlogPurgeThrottlePlan{targetRate: 50000, pendingRows: 100000, effectiveBatchSize: 10000, minRate: 1, deadline: time.Now().Add(30 * time.Second), noWaitStreak: 1}
	require.NoError(t, plan.maybeSleep(context.Background(), time.Now().Add(-3*time.Second), 98000))
	require.Equal(t, int64(8000), plan.effectiveBatchSize)
	require.Zero(t, plan.noWaitStreak)
	require.Less(t, plan.targetRate, float64(50000))
}

func TestBuildMLogPurgeDeleteRowIDRanges(t *testing.T) {
	stats := mlogPurgePendingRowStats{pendingRows: 40000, minRowID: 1, maxRowID: 40000, hasRowIDBounds: true}
	require.Equal(t, []mlogPurgeDeleteRowIDRange{{startRowID: 1, endRowID: 8000}, {startRowID: 8001, endRowID: 16000}, {startRowID: 16001, endRowID: 24000}, {startRowID: 24001, endRowID: 32000}, {startRowID: 32001, endRowID: 40000}}, buildMLogPurgeDeleteRowIDRanges(stats, 0))
	stats.pendingRows, stats.maxRowID = 3500, 3500
	require.Equal(t, []mlogPurgeDeleteRowIDRange{{startRowID: 1, endRowID: 3500}}, buildMLogPurgeDeleteRowIDRanges(stats, 0))
	stats.pendingRows, stats.maxRowID = 32000, int64(1<<63-1)
	shardBucketSize := int64(1) << 59
	require.Equal(t, []mlogPurgeDeleteRowIDRange{{startRowID: 1, endRowID: 4*shardBucketSize - 1}, {startRowID: 4 * shardBucketSize, endRowID: 8*shardBucketSize - 1}, {startRowID: 8 * shardBucketSize, endRowID: 12*shardBucketSize - 1}, {startRowID: 12 * shardBucketSize, endRowID: int64(1<<63 - 1)}}, buildMLogPurgeDeleteRowIDRanges(stats, 4))
	stats.hasRowIDBounds = false
	require.Nil(t, buildMLogPurgeDeleteRowIDRanges(stats, 0))
}

func TestBuildPurgeMaterializedViewLogDeleteSQL(t *testing.T) {
	r := &mlogPurgeDeleteRowIDRange{startRowID: 10, endRowID: 20}
	sql := buildPurgeMaterializedViewLogDeleteSQL("test", "$mlog$t", 100, true, 200, r, 1000)
	require.Contains(t, sql, "_tidb_rowid >= 10 AND _tidb_rowid <= 20")
	require.Contains(t, sql, "_tidb_commit_ts > 100 AND _tidb_commit_ts <= 200")
	require.Contains(t, sql, "LIMIT 1000")
	sql = buildPurgeMaterializedViewLogDeleteSQL("test", "$mlog$t", 0, false, 200, r, 1000)
	require.Contains(t, sql, "_tidb_commit_ts <= 200")
	require.NotContains(t, sql, "_tidb_commit_ts >")
	sql = buildPurgeMaterializedViewLogDeleteSQL("test", "$mlog$t", 100, true, 200, nil, 1000)
	require.NotContains(t, sql, "_tidb_rowid")
}

func TestApplyMLogPurgeDeleteTiFlashThreads(t *testing.T) {
	sessVars := variable.NewSessionVars(nil)
	require.NoError(t, sessVars.SetSystemVar(vardef.TiDBMaxTiFlashThreads, "9"))
	restore, err := applyMLogPurgeDeleteTiFlashThreads(sessVars, 2, false)
	require.NoError(t, err)
	require.Equal(t, int64(2), sessVars.TiFlashMaxThreads)
	restore()
	require.Equal(t, int64(9), sessVars.TiFlashMaxThreads)
	restore, err = applyMLogPurgeDeleteTiFlashThreads(sessVars, 0, false)
	require.NoError(t, err)
	restore()
	require.Equal(t, int64(9), sessVars.TiFlashMaxThreads)
}

func TestApplyMLogPurgeMaintenanceSessionVarsEnablesAndRestoresMView(t *testing.T) {
	sessVars := variable.NewSessionVars(nil)
	require.False(t, sessVars.EnableMView)

	restore, err := applyMLogPurgeMaintenanceSessionVars(
		sessVars,
		sessVars.MemQuotaQuery,
		variable.GetIsolationReadEnginesString(sessVars),
		false,
	)
	require.NoError(t, err)
	require.True(t, sessVars.EnableMView)
	restore()
	require.False(t, sessVars.EnableMView)
}

func TestMVTaskCancelControllerIsManualCancelRequested(t *testing.T) {
	controller := newMVTaskCancelController(context.Background())
	require.False(t, controller.isManualCancelRequested())
	controller.requestManualCancelByRequester("'u'@'h'")
	require.True(t, controller.isManualCancelRequested())
}

func TestDeriveMLogPurgeThrottleDeadline(t *testing.T) {
	baseNow := time.Now().UTC()
	deadline, err := deriveMLogPurgeThrottleDeadline(context.Background(), nil, nil, false, "test", "t", nil)
	require.NoError(t, err)
	require.WithinDuration(t, baseNow.Add(mlogPurgeAdaptiveMaxBudget), *deadline, 2*time.Second)
	next := baseNow.Add(90 * time.Second)
	deadline, err = deriveMLogPurgeThrottleDeadline(context.Background(), nil, nil, false, "test", "t", &next)
	require.NoError(t, err)
	require.WithinDuration(t, next, *deadline, time.Second)
}
