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
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

// buildCloseCancelExec constructs a HashJoinV2Exec backed by the same 50K-row
// mock data sources used by the spill tests. partitionNumber=4 with
// concurrency=3 (set inside buildHashJoinV2Exec) forces the unbalanced branch
// of createTasks, which produces multiple build tasks.
func buildCloseCancelExec(_ *testing.T) *HashJoinV2Exec {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	leftDS, rightDS := buildLeftAndRightDataSource(ctx, leftCols, rightCols, false)
	leftDS.PrepareChunks()
	rightDS.PrepareChunks()

	intTp := types.NewFieldType(mysql.TypeLonglong)
	intTp.AddFlag(mysql.NotNullFlag)
	stringTp := types.NewFieldType(mysql.TypeVarString)
	stringTp.AddFlag(mysql.NotNullFlag)

	leftTypes := []*types.FieldType{intTp, intTp, intTp, stringTp, intTp}
	rightTypes := []*types.FieldType{intTp, intTp, stringTp, intTp, intTp}

	leftKeys := []*expression.Column{
		{Index: 1, RetType: leftTypes[1]},
		{Index: 3, RetType: leftTypes[3]},
	}
	rightKeys := []*expression.Column{
		{Index: 0, RetType: rightTypes[0]},
		{Index: 2, RetType: rightTypes[2]},
	}

	param := spillTestParam{
		rightAsBuildSide: true,
		leftKeys:         leftKeys,
		rightKeys:        rightKeys,
		leftTypes:        leftTypes,
		rightTypes:       rightTypes,
		leftUsed:         []int{0, 1, 3, 4},
		rightUsed:        []int{0, 2, 3, 4},
	}
	resultTypes := getReturnTypes(base.InnerJoin, param)

	info := &hashJoinInfo{
		ctx:              ctx,
		schema:           buildSchema(resultTypes),
		leftExec:         leftDS,
		rightExec:        rightDS,
		joinType:         base.InnerJoin,
		rightAsBuildSide: true,
		buildKeys:        rightKeys,
		probeKeys:        leftKeys,
		lUsed:            param.leftUsed,
		rUsed:            param.rightUsed,
	}
	return buildHashJoinV2Exec(info)
}

func drainNextInBackground(t *testing.T, e *HashJoinV2Exec) chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c := exec.NewFirstChunk(e)
		for {
			err := e.Next(context.Background(), c)
			if err != nil {
				return
			}
			if c.NumRows() == 0 {
				return
			}
			c.Reset()
		}
	}()
	return done
}

func awaitClose(t *testing.T, e *HashJoinV2Exec) time.Duration {
	t.Helper()
	closeDone := make(chan error, 1)
	start := time.Now()
	go func() { closeDone <- e.Close() }()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
		return time.Since(start)
	case <-time.After(10 * time.Second):
		t.Fatalf("Close did not return within 10s — cancel path is still deadlocked")
	}
	return 0
}

// TestHashJoinV2CloseUnblocksBuildPhase exercises G1+G2: when build workers are
// busy with slow tasks and createTasks is blocked on a full buildTaskCh, Close
// must propagate through both createTasks and the buildHashTable worker loop
// instead of waiting for every queued task to drain.
func TestHashJoinV2CloseUnblocksBuildPhase(t *testing.T) {
	// 1s per task is long enough that, without G2, draining all queued tasks
	// across 3 workers takes well over 2s. With G2, Close only waits for the
	// in-flight tasks to clear their current sleep before each worker returns.
	testfailpoint.Enable(t,
		"github.com/pingcap/tidb/pkg/executor/join/slowBuildTask", "return(1000)")

	e := buildCloseCancelExec(t)
	require.NoError(t, e.Open(context.Background()))
	nextDone := drainNextInBackground(t, e)

	// Let workers pick up their first task and enter the slow path.
	time.Sleep(100 * time.Millisecond)

	elapsed := awaitClose(t, e)
	require.Less(t, elapsed, 2500*time.Millisecond,
		"Close should not have to wait for all queued build tasks; took %v", elapsed)

	select {
	case <-nextDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Next goroutine did not exit after Close")
	}
}

// TestBuildWorkerExitsOnCloseCh is a focused regression test for G2. It pins
// the contract that BuildWorkerV2.buildHashTable short-circuits via closeCh
// without dereferencing any per-task state, so a slow build per task never
// keeps a worker alive after Close fires.
func TestBuildWorkerExitsOnCloseCh(t *testing.T) {
	closeCh := make(chan struct{})
	close(closeCh)

	ctx := &HashJoinCtxV2{}
	ctx.closeCh = closeCh
	worker := &BuildWorkerV2{HashJoinCtx: ctx}

	// Queue several tasks; do not close taskCh. Without the closeCh check,
	// the worker would call hashTableContext.build(task) on the first task
	// and nil-deref. With the check, the worker returns before touching it.
	taskCh := make(chan *buildTask, 5)
	for range 5 {
		taskCh <- &buildTask{}
	}

	done := make(chan any, 1)
	go func() {
		defer func() { done <- recover() }()
		err := worker.buildHashTable(taskCh)
		done <- err
	}()

	select {
	case result := <-done:
		require.Nil(t, result,
			"worker must exit cleanly via closeCh; observed %v", result)
	case <-time.After(time.Second):
		t.Fatal("worker did not honor closeCh; loop kept reading tasks")
	}
}

// TestFetchProbeSideUnblocksOnCloseChWhenBufferFull is a focused regression
// test for G3. It exercises the exact deadlock the user observed: fetcher
// holding a chunk while probeResultChs[i] is full and the consumer is gone.
// The buffer is pre-filled so the send blocks deterministically; closeCh
// must then unblock the fetcher via the new select case (without G3 the
// goroutine hangs forever, the test times out).
func TestFetchProbeSideUnblocksOnCloseChWhenBufferFull(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	intTp := types.NewFieldType(mysql.TypeLonglong)
	intTp.AddFlag(mysql.NotNullFlag)
	schema := expression.NewSchema(&expression.Column{Index: 0, RetType: intTp})
	mockSrc := testutil.BuildMockDataSource(testutil.MockDataSourceParameters{
		DataSchema: schema,
		Ctx:        ctx,
		Rows:       64,
		Ndvs:       []int{0},
	})
	mockSrc.PrepareChunks()

	closeCh := make(chan struct{})
	joinResultCh := make(chan *hashjoinWorkerResult, 4)
	buildFinished := make(chan error, 1)
	// Pre-send nil so wait4BuildSide returns immediately with buildSuccess=true.
	buildFinished <- nil

	hashJoinCtx := &hashJoinCtxBase{
		Concurrency:   1,
		closeCh:       closeCh,
		buildFinished: buildFinished,
		joinResultCh:  joinResultCh,
	}
	fetcher := &probeSideTupleFetcherBase{ProbeSideExec: mockSrc}
	fetcher.initializeForProbeBase(1, joinResultCh)

	// Pre-fill probeResultChs[0] (buffer == 1) to make the fetcher's dest
	// send block as soon as it has a chunk to deliver.
	fetcher.probeResultChs[0] <- mockSrc.NewChunk()

	done := make(chan struct{})
	go func() {
		defer close(done)
		fetcher.fetchProbeSideChunks(
			context.Background(),
			32,                          // maxChunkSize
			func() bool { return false }, // isBuildEmpty
			func() bool { return false }, // checkSpill
			true,                         // canSkipIfBuildEmpty
			false,                        // needScanAfterProbeDone
			false,                        // shouldLimitProbeFetchSize
			hashJoinCtx,
		)
	}()

	// Let the fetcher park on the dest send.
	time.Sleep(100 * time.Millisecond)
	close(closeCh)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("fetchProbeSideChunks did not unblock on closeCh; G3 missing")
	}
}

// TestHashJoinV2CloseUnblocksProbeFetcher exercises G3: after Close fires,
// probe workers honor closeCh in their receive select and exit immediately;
// the fetcher must also honor closeCh on its `dest <- result` send, otherwise
// the send blocks forever and prevents handleProbeSideFetcherPanic from
// running, which in turn keeps Close stuck at channel.Clear(probeResultChs[i]).
func TestHashJoinV2CloseUnblocksProbeFetcher(t *testing.T) {
	// Delay long enough that we can trigger Close while the fetcher is sleeping
	// just before its dest send, but short enough that the test is fast.
	testfailpoint.Enable(t,
		"github.com/pingcap/tidb/pkg/executor/join/holdProbeFetcherBeforeSend", "return(800)")

	e := buildCloseCancelExec(t)
	require.NoError(t, e.Open(context.Background()))

	// Run Next manually until we have consumed one result chunk, so the probe
	// phase is definitely running and the fetcher has at least one chunk
	// in-flight. After that we close in the background.
	c := exec.NewFirstChunk(e)
	require.NoError(t, e.Next(context.Background(), c))
	require.Greater(t, c.NumRows(), 0)

	// Start the rest of Next in the background; it will block reading
	// joinResultCh until Close shuts things down.
	nextDone := drainNextInBackground(t, e)

	elapsed := awaitClose(t, e)
	require.Less(t, elapsed, 2500*time.Millisecond,
		"Close stuck on probe fetcher dest send; took %v", elapsed)

	select {
	case <-nextDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Next goroutine did not exit after Close")
	}
}
