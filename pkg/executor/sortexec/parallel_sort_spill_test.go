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

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/executor/internal/util"
	"github.com/pingcap/tidb/pkg/executor/sortexec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
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
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
	})
	testFuncName := util.GetFunctionName()

	sortexec.SetSmallSpillChunkSizeForTest()
	ctx := mock.NewContext()
	sortCase := &testutil.SortCase{Rows: 10000, OrderByIdx: []int{0, 1}, Ndvs: []int{0, 0}, Ctx: ctx, FileNamePrefixForTest: testFuncName}

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/sortexec/SlowSomeWorkers", `return(true)`)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/sortexec/SignalCheckpointForSort", `return(true)`)

	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, hardLimit1)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	schema := expression.NewSchema(sortCase.Columns()...)
	dataSource := buildDataSource(sortCase, schema)
	exe := buildSortExec(sortCase, dataSource)
	for range 10 {
		oneSpillCase(t, nil, sortCase, schema, dataSource)
		oneSpillCase(t, exe, sortCase, schema, dataSource)
	}

	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, hardLimit2)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	for range 10 {
		inMemoryThenSpill(t, ctx, nil, sortCase, schema, dataSource)
		inMemoryThenSpill(t, ctx, exe, sortCase, schema, dataSource)
	}

	util.CheckNoLeakFiles(t, testFuncName)
}

func TestParallelSortSpillDiskFailpoint(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
	})
	testFuncName := util.GetFunctionName()

	sortexec.SetSmallSpillChunkSizeForTest()
	ctx := mock.NewContext()
	sortCase := &testutil.SortCase{Rows: 10000, OrderByIdx: []int{0, 1}, Ndvs: []int{0, 0}, Ctx: ctx, FileNamePrefixForTest: testFuncName}

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/sortexec/SlowSomeWorkers", `return(true)`)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/sortexec/SignalCheckpointForSort", `return(true)`)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/sortexec/ParallelSortRandomFail", `return(true)`)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/util/chunk/ChunkInDiskError", `return(true)`)

	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, hardLimit1)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	schema := expression.NewSchema(sortCase.Columns()...)
	dataSource := buildDataSource(sortCase, schema)
	exe := buildSortExec(sortCase, dataSource)
	for range 20 {
		failpointNoMemoryDataTest(t, nil, sortCase, dataSource)
		failpointNoMemoryDataTest(t, exe, sortCase, dataSource)
	}

	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, hardLimit2)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	for range 20 {
		failpointDataInMemoryThenSpillTest(t, ctx, nil, sortCase, dataSource)
		failpointDataInMemoryThenSpillTest(t, ctx, exe, sortCase, dataSource)
	}

	util.CheckNoLeakFiles(t, testFuncName)
}

func TestIssue59655(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
	})
	testFuncName := util.GetFunctionName()

	sortexec.SetSmallSpillChunkSizeForTest()
	ctx := mock.NewContext()
	sortCase := &testutil.SortCase{Rows: 10000, OrderByIdx: []int{0, 1}, Ndvs: []int{0, 0}, Ctx: ctx, FileNamePrefixForTest: testFuncName}

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/sortexec/Issue59655", `return(true)`)

	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().ExecutorConcurrency = sortexec.ResultChannelCapacity * 2
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, hardLimit1)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	schema := expression.NewSchema(sortCase.Columns()...)
	dataSource := buildDataSource(sortCase, schema)
	exe := buildSortExec(sortCase, dataSource)
	for range 20 {
		failpointNoMemoryDataTest(t, nil, sortCase, dataSource)
		failpointNoMemoryDataTest(t, exe, sortCase, dataSource)
	}

	util.CheckNoLeakFiles(t, testFuncName)
}

func TestIssue63216(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
	})
	testFuncName := util.GetFunctionName()

	sortexec.SetSmallSpillChunkSizeForTest()
	ctx := mock.NewContext()
	sortCase := &testutil.SortCase{Rows: 10000, OrderByIdx: []int{0, 1}, Ndvs: []int{0, 0}, Ctx: ctx, FileNamePrefixForTest: testFuncName}

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/sortexec/Issue63216", `return(true)`)

	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, hardLimit1)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	schema := expression.NewSchema(sortCase.Columns()...)
	dataSource := buildDataSource(sortCase, schema)
	exe := buildSortExec(sortCase, dataSource)
	failpointNoMemoryDataTest(t, exe, sortCase, dataSource)

	util.CheckNoLeakFiles(t, testFuncName)
}
