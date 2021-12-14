// Copyright 2016 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	plannerutil "github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestLoadDataWithDifferentEscapeChar(t *testing.T) {
	tests := []struct {
		input      string
		escapeChar byte
		expected   []string
	}{
		{
			`"{""itemRangeType"":0,""itemContainType"":0,""shopRangeType"":1,""shopJson"":""[{\""id\"":\""A1234\"",\""shopName\"":\""AAAAAA\""}]""}"`,
			byte(0), // escaped by ''
			[]string{`{"itemRangeType":0,"itemContainType":0,"shopRangeType":1,"shopJson":"[{\"id\":\"A1234\",\"shopName\":\"AAAAAA\"}]"}`},
		},
	}

	for _, test := range tests {
		ldInfo := LoadDataInfo{
			FieldsInfo: &ast.FieldsClause{
				Enclosed:   '"',
				Terminated: ",",
				Escaped:    test.escapeChar,
			},
		}
		got, err := ldInfo.getFieldsFromLine([]byte(test.input))
		require.NoErrorf(t, err, "failed: %s", test.input)
		assertEqualStrings(t, got, test.expected)
	}
}

func TestSortSpillDisk(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.OOMUseTmpStorage = true
		conf.MemQuotaQuery = 1
	})
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testSortedRowContainerSpill", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/testSortedRowContainerSpill"))
	}()
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(-1, -1)
	cas := &sortCase{rows: 2048, orderByIdx: []int{0, 1}, ndvs: []int{0, 0}, ctx: ctx}
	opt := mockDataSourceParameters{
		schema: expression.NewSchema(cas.columns()...),
		rows:   cas.rows,
		ctx:    cas.ctx,
		ndvs:   cas.ndvs,
	}
	dataSource := buildMockDataSource(opt)
	exec := &SortExec{
		baseExecutor: newBaseExecutor(cas.ctx, dataSource.schema, 0, dataSource),
		ByItems:      make([]*plannerutil.ByItems, 0, len(cas.orderByIdx)),
		schema:       dataSource.schema,
	}
	for _, idx := range cas.orderByIdx {
		exec.ByItems = append(exec.ByItems, &plannerutil.ByItems{Expr: cas.columns()[idx]})
	}
	tmpCtx := context.Background()
	chk := newFirstChunk(exec)
	dataSource.prepareChunks()
	err := exec.Open(tmpCtx)
	require.NoError(t, err)
	for {
		err = exec.Next(tmpCtx, chk)
		require.NoError(t, err)
		if chk.NumRows() == 0 {
			break
		}
	}
	// Test only 1 partition and all data in memory.
	require.Len(t, exec.partitionList, 1)
	require.Equal(t, false, exec.partitionList[0].AlreadySpilledSafeForTest())
	require.Equal(t, 2048, exec.partitionList[0].NumRow())
	err = exec.Close()
	require.NoError(t, err)

	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(-1, 1)
	dataSource.prepareChunks()
	err = exec.Open(tmpCtx)
	require.NoError(t, err)
	for {
		err = exec.Next(tmpCtx, chk)
		require.NoError(t, err)
		if chk.NumRows() == 0 {
			break
		}
	}
	// Test 2 partitions and all data in disk.
	// Now spilling is in parallel.
	// Maybe the second add() will called before spilling, depends on
	// Golang goroutine scheduling. So the result has two possibilities.
	if len(exec.partitionList) == 2 {
		require.Len(t, exec.partitionList, 2)
		require.Equal(t, true, exec.partitionList[0].AlreadySpilledSafeForTest())
		require.Equal(t, true, exec.partitionList[1].AlreadySpilledSafeForTest())
		require.Equal(t, 1024, exec.partitionList[0].NumRow())
		require.Equal(t, 1024, exec.partitionList[1].NumRow())
	} else {
		require.Len(t, exec.partitionList, 1)
		require.Equal(t, true, exec.partitionList[0].AlreadySpilledSafeForTest())
		require.Equal(t, 2048, exec.partitionList[0].NumRow())
	}

	err = exec.Close()
	require.NoError(t, err)

	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(-1, 24000)
	dataSource.prepareChunks()
	err = exec.Open(tmpCtx)
	require.NoError(t, err)
	for {
		err = exec.Next(tmpCtx, chk)
		require.NoError(t, err)
		if chk.NumRows() == 0 {
			break
		}
	}
	// Test only 1 partition but spill disk.
	require.Len(t, exec.partitionList, 1)
	require.Equal(t, true, exec.partitionList[0].AlreadySpilledSafeForTest())
	require.Equal(t, 2048, exec.partitionList[0].NumRow())
	err = exec.Close()
	require.NoError(t, err)

	// Test partition nums.
	ctx = mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(-1, 16864*50)
	ctx.GetSessionVars().StmtCtx.MemTracker.Consume(16864 * 45)
	cas = &sortCase{rows: 20480, orderByIdx: []int{0, 1}, ndvs: []int{0, 0}, ctx: ctx}
	opt = mockDataSourceParameters{
		schema: expression.NewSchema(cas.columns()...),
		rows:   cas.rows,
		ctx:    cas.ctx,
		ndvs:   cas.ndvs,
	}
	dataSource = buildMockDataSource(opt)
	exec = &SortExec{
		baseExecutor: newBaseExecutor(cas.ctx, dataSource.schema, 0, dataSource),
		ByItems:      make([]*plannerutil.ByItems, 0, len(cas.orderByIdx)),
		schema:       dataSource.schema,
	}
	for _, idx := range cas.orderByIdx {
		exec.ByItems = append(exec.ByItems, &plannerutil.ByItems{Expr: cas.columns()[idx]})
	}
	tmpCtx = context.Background()
	chk = newFirstChunk(exec)
	dataSource.prepareChunks()
	err = exec.Open(tmpCtx)
	require.NoError(t, err)
	for {
		err = exec.Next(tmpCtx, chk)
		require.NoError(t, err)
		if chk.NumRows() == 0 {
			break
		}
	}
	// Don't spill too many partitions.
	require.True(t, len(exec.partitionList) <= 4)
	err = exec.Close()
	require.NoError(t, err)
}
