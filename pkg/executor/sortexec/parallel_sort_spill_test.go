// Copyright 2024 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/executor/sortexec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

var hardLimit1 = int64(100000)
var hardLimit2 = hardLimit1 * 10

func oneSpillCase(t *testing.T, exe *sortexec.SortExec, sortCase *testutil.SortCase, schema *expression.Schema, dataSource *testutil.MockDataSource) {
	if exe == nil {
		exe = buildSortExec(sortCase, dataSource)
	}
	dataSource.PrepareChunks()
	resultChunks := executeSortExecutor(t, exe, true)

	require.True(t, exe.IsSpillTriggeredInParallelSortForTest())
	require.Equal(t, int64(sortCase.Rows), exe.GetSpilledRowNumInParallelSortForTest())

	err := exe.Close()
	require.NoError(t, err)

	require.True(t, checkCorrectness(schema, exe, dataSource, resultChunks))
}

func inMemoryThenSpill(t *testing.T, ctx *mock.Context, exe *sortexec.SortExec, sortCase *testutil.SortCase, schema *expression.Schema, dataSource *testutil.MockDataSource) {
	if exe == nil {
		exe = buildSortExec(sortCase, dataSource)
	}
	dataSource.PrepareChunks()
	resultChunks := executeSortExecutorAndManullyTriggerSpill(t, exe, hardLimit2, ctx.GetSessionVars().StmtCtx.MemTracker, true)

	require.True(t, exe.IsSpillTriggeredInParallelSortForTest())
	require.Greater(t, int64(sortCase.Rows), exe.GetSpilledRowNumInParallelSortForTest())
	err := exe.Close()
	require.NoError(t, err)

	require.True(t, checkCorrectness(schema, exe, dataSource, resultChunks))
}

func failpointNoMemoryDataTest(t *testing.T, exe *sortexec.SortExec, sortCase *testutil.SortCase, dataSource *testutil.MockDataSource) {
	if exe == nil {
		exe = buildSortExec(sortCase, dataSource)
	}
	dataSource.PrepareChunks()
	executeInFailpoint(t, exe, 0, nil)
}

func failpointDataInMemoryThenSpillTest(t *testing.T, ctx *mock.Context, exe *sortexec.SortExec, sortCase *testutil.SortCase, dataSource *testutil.MockDataSource) {
	if exe == nil {
		exe = buildSortExec(sortCase, dataSource)
	}
	dataSource.PrepareChunks()
	executeInFailpoint(t, exe, hardLimit2, ctx.GetSessionVars().MemTracker)
}

func TestParallelSortSpillDisk(t *testing.T) {
	sortexec.SetSmallSpillChunkSizeForTest()
	ctx := mock.NewContext()
	sortCase := &testutil.SortCase{Rows: 10000, OrderByIdx: []int{0, 1}, Ndvs: []int{0, 0}, Ctx: ctx}

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/SlowSomeWorkers", `return(true)`))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/executor/sortexec/SlowSomeWorkers")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/SignalCheckpointForSort", `return(true)`))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/executor/sortexec/SignalCheckpointForSort")

	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, hardLimit1)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	schema := expression.NewSchema(sortCase.Columns()...)
	dataSource := buildDataSource(sortCase, schema)
	exe := buildSortExec(sortCase, dataSource)
	for i := 0; i < 10; i++ {
		oneSpillCase(t, nil, sortCase, schema, dataSource)
		oneSpillCase(t, exe, sortCase, schema, dataSource)
	}

	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, hardLimit2)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	for i := 0; i < 10; i++ {
		inMemoryThenSpill(t, ctx, nil, sortCase, schema, dataSource)
		inMemoryThenSpill(t, ctx, exe, sortCase, schema, dataSource)
	}
}

func TestParallelSortSpillDiskFailpoint(t *testing.T) {
	sortexec.SetSmallSpillChunkSizeForTest()
	ctx := mock.NewContext()
	sortCase := &testutil.SortCase{Rows: 10000, OrderByIdx: []int{0, 1}, Ndvs: []int{0, 0}, Ctx: ctx}

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/SlowSomeWorkers", `return(true)`))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/executor/sortexec/SlowSomeWorkers")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/SignalCheckpointForSort", `return(true)`))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/executor/sortexec/SignalCheckpointForSort")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/ParallelSortRandomFail", `return(true)`))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/executor/sortexec/ParallelSortRandomFail")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/chunk/ChunkInDiskError", `return(true)`))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/util/chunk/ChunkInDiskError")

	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, hardLimit1)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	schema := expression.NewSchema(sortCase.Columns()...)
	dataSource := buildDataSource(sortCase, schema)
	exe := buildSortExec(sortCase, dataSource)
	for i := 0; i < 20; i++ {
		failpointNoMemoryDataTest(t, nil, sortCase, dataSource)
		failpointNoMemoryDataTest(t, exe, sortCase, dataSource)
	}

	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, hardLimit2)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	for i := 0; i < 20; i++ {
		failpointDataInMemoryThenSpillTest(t, ctx, nil, sortCase, dataSource)
		failpointDataInMemoryThenSpillTest(t, ctx, exe, sortCase, dataSource)
	}
}
