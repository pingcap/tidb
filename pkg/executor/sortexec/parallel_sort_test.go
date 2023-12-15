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
	"testing"

	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestParallelSort(t *testing.T) {
	ctx := mock.NewContext()
	rowNum := 65536
	sortCase := &testutil.SortCase{Rows: rowNum, OrderByIdx: []int{0, 1}, Ndvs: []int{0, 0}, Ctx: ctx}
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	ctx.GetSessionVars().EnableParallelSort = true
	schema := expression.NewSchema(sortCase.Columns()...)
	dataSource := buildDataSource(ctx, sortCase, schema)
	exe := buildSortExec(ctx, sortCase, dataSource)

	resultChunks := executeSortExecutor(t, exe)

	err := exe.Close()
	require.NoError(t, err)
	require.True(t, checkCorrectness(schema, exe, dataSource, resultChunks))
}

// TODO add failpoint
