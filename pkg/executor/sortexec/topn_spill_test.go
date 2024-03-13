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

	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/executor/sortexec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
)

// No spill will be triggered in this test
func topNNoSpillTest(t *testing.T, ctx *mock.Context, exe *sortexec.SortExec, sortCase *testutil.SortCase, schema *expression.Schema, dataSource *testutil.MockDataSource) {

}

// Topn executor has two stage:
//  1. Building heap, in this stage all received rows will be inserted into heap.
//  2. Updating heap, in this stage only rows that is smaller than the heap top could be inserted and we will drop the heap top.
//
// Case1 means that we will trigger spill in stage 1
func topNSpillCase1Test(t *testing.T, ctx *mock.Context, exe *sortexec.SortExec, sortCase *testutil.SortCase, schema *expression.Schema, dataSource *testutil.MockDataSource) {

}

// Case2 means that we will trigger spill in stage 2
func topNSpillCase2Test(t *testing.T, ctx *mock.Context, exe *sortexec.SortExec, sortCase *testutil.SortCase, schema *expression.Schema, dataSource *testutil.MockDataSource) {

}

func TestTopNSpillDisk(t *testing.T) {
	sortexec.SetSmallSpillChunkSizeForTest()
	ctx := mock.NewContext()
	sortCase := &testutil.SortCase{Rows: 10000, OrderByIdx: []int{0, 1}, Ndvs: []int{0, 0}, Ctx: ctx}

	// TODO add slow random fail point for topn failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/SlowSomeWorkers", `return(true)`)

	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, hardLimit1)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	ctx.GetSessionVars().EnableParallelSort = true

	schema := expression.NewSchema(sortCase.Columns()...)
	dataSource := buildDataSource(ctx, sortCase, schema)
	exe := buildSortExec(ctx, sortCase, dataSource)
	for i := 0; i < 10; i++ {
		oneSpillCase(t, ctx, nil, sortCase, schema, dataSource)
		oneSpillCase(t, ctx, exe, sortCase, schema, dataSource)
	}

	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, hardLimit2)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	for i := 0; i < 10; i++ {
		inMemoryThenSpill(t, ctx, nil, sortCase, schema, dataSource)
		inMemoryThenSpill(t, ctx, exe, sortCase, schema, dataSource)
	}
}

func TestTopNSpillDiskFailpoint(t *testing.T) {
	// TODO spills in stage 1 and stage 2 are all need to be tested
}
