// Copyright 2023 PingCAP, Inc.
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

package sortexec_test

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/executor/sortexec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

// Test is successful if there is no hang
func executeInFailpoint(t *testing.T, exe *sortexec.SortExec, hardLimit int64, tracker *memory.Tracker) {
	tmpCtx := context.Background()
	err := exe.Open(tmpCtx)
	require.NoError(t, err)
	exe.IsUnparallel = false
	exe.InitInParallelModeForTest()

	goRoutineWaiter := sync.WaitGroup{}
	goRoutineWaiter.Add(1)
	defer goRoutineWaiter.Wait()

	once := sync.Once{}

	go func() {
		time.Sleep(time.Duration(rand.Int31n(300)) * time.Millisecond)
		once.Do(func() {
			exe.Close()
		})
		goRoutineWaiter.Done()
	}()

	chk := exec.NewFirstChunk(exe)
	for i := 0; i >= 0; i++ {
		err := exe.Next(tmpCtx, chk)
		if err != nil {
			once.Do(func() {
				err = exe.Close()
				require.Equal(t, nil, err)
			})
			break
		}
		if chk.NumRows() == 0 {
			break
		}

		if i == 10 && hardLimit > 0 {
			// Trigger the spill
			tracker.Consume(hardLimit)
			tracker.Consume(-hardLimit)
		}
	}
	once.Do(func() {
		err = exe.Close()
		require.Equal(t, nil, err)
	})
}

func parallelSortTest(t *testing.T, ctx *mock.Context, exe *sortexec.SortExec, schema *expression.Schema, dataSource *testutil.MockDataSource, sortCase *testutil.SortCase) {
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	// TODO use variable to choose parallel mode after system variable is added
	// ctx.GetSessionVars().EnableParallelSort = true

	if exe == nil {
		exe = buildSortExec(sortCase, dataSource)
	}
	dataSource.PrepareChunks()
	resultChunks := executeSortExecutor(t, exe, true)

	err := exe.Close()
	require.NoError(t, err)
	require.True(t, checkCorrectness(schema, exe, dataSource, resultChunks))
}

func failpointTest(t *testing.T, ctx *mock.Context, exe *sortexec.SortExec, sortCase *testutil.SortCase, dataSource *testutil.MockDataSource) {
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	// TODO use variable to choose parallel mode after system variable is added
	// ctx.GetSessionVars().EnableParallelSort = true
	if exe == nil {
		exe = buildSortExec(sortCase, dataSource)
	}
	dataSource.PrepareChunks()
	executeInFailpoint(t, exe, 0, nil)
}

func TestParallelSort(t *testing.T) {
	ctx := mock.NewContext()
	rowNum := 30000
	nvd := 100 // we have two column and should ensure that nvd*nvd is less than rowNum.
	sortCase := &testutil.SortCase{Rows: rowNum, OrderByIdx: []int{0, 1}, Ndvs: []int{nvd, nvd}, Ctx: ctx}
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/SlowSomeWorkers", `return(true)`)
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/SignalCheckpointForSort", `return(true)`)

	schema := expression.NewSchema(sortCase.Columns()...)
	dataSource := buildDataSource(sortCase, schema)
	exe := buildSortExec(sortCase, dataSource)
	for i := 0; i < 10; i++ {
		parallelSortTest(t, ctx, nil, schema, dataSource, sortCase)
		parallelSortTest(t, ctx, exe, schema, dataSource, sortCase)
	}
}

func TestFailpoint(t *testing.T) {
	ctx := mock.NewContext()
	rowNum := 65536
	sortCase := &testutil.SortCase{Rows: rowNum, OrderByIdx: []int{0, 1}, Ndvs: []int{0, 0}, Ctx: ctx}
	schema := expression.NewSchema(sortCase.Columns()...)
	dataSource := buildDataSource(sortCase, schema)
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/ParallelSortRandomFail", `return(true)`)
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/SlowSomeWorkers", `return(true)`)
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/SignalCheckpointForSort", `return(true)`)

	testNum := 30
	exe := buildSortExec(sortCase, dataSource)
	for i := 0; i < testNum; i++ {
		failpointTest(t, ctx, nil, sortCase, dataSource)
		failpointTest(t, ctx, exe, sortCase, dataSource)
	}
}
