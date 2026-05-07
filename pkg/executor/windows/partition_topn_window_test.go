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

package windows

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestPartitionTopNWindowExecAcrossChunks(t *testing.T) {
	_, windowExec := buildPartitionTopNWindowExecForTest(
		2,
		[][]any{
			{int64(1), int64(1), int64(1), int64(2), int64(2), int64(3)},
			{int64(10), int64(11), int64(12), int64(20), int64(21), int64(30)},
		},
		2,
	)
	require.NoError(t, windowExec.Open(context.Background()))
	rows := drainPartitionTopNRows(t, windowExec)
	require.NoError(t, windowExec.Close())
	require.Equal(t, [][]int64{
		{1, 10, 1},
		{1, 11, 2},
		{2, 20, 1},
		{2, 21, 2},
		{3, 30, 1},
	}, rows)

	childExec, windowExec := buildPartitionTopNWindowExecForTest(
		4,
		[][]any{
			{int64(1), int64(1), int64(2), int64(2)},
			{int64(10), int64(11), int64(20), int64(21)},
		},
		2,
	)
	require.NoError(t, windowExec.Open(context.Background()))
	req := chunk.NewChunkWithCapacity(exec.RetTypes(windowExec), 1)
	require.NoError(t, exec.Next(context.Background(), windowExec, req))
	require.Equal(t, 1, req.NumRows())
	require.Equal(t, []int64{1, 10, 1}, []int64{
		req.GetRow(0).GetInt64(0),
		req.GetRow(0).GetInt64(1),
		req.GetRow(0).GetInt64(2),
	})
	require.NoError(t, windowExec.Close())

	childExec.PrepareChunks()
	require.NoError(t, windowExec.Open(context.Background()))
	rows = drainPartitionTopNRows(t, windowExec)
	require.NoError(t, windowExec.Close())
	require.Equal(t, [][]int64{
		{1, 10, 1},
		{1, 11, 2},
		{2, 20, 1},
		{2, 21, 2},
	}, rows)
}

func buildPartitionTopNWindowExecForTest(
	maxChunkSize int,
	datums [][]any,
	limitCount uint64,
) (*testutil.MockDataSource, *PartitionTopNWindowExec) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = maxChunkSize
	ctx.GetSessionVars().MaxChunkSize = maxChunkSize

	intTp := types.NewFieldType(mysql.TypeLonglong)
	partitionCol := &expression.Column{Index: 0, RetType: intTp}
	valueCol := &expression.Column{Index: 1, RetType: intTp}
	rowNumberCol := &expression.Column{Index: 2, RetType: intTp}
	childSchema := expression.NewSchema(partitionCol, valueCol)
	resultSchema := expression.NewSchema(partitionCol, valueCol, rowNumberCol)
	childExec := testutil.BuildMockDataSource(testutil.MockDataSourceParameters{
		Ctx:        ctx,
		DataSchema: childSchema,
		Rows:       len(datums[0]),
		Ndvs:       []int{-2, -2},
		Datums:     datums,
	})
	childExec.PrepareChunks()
	return childExec, BuildPartitionTopN(
		ctx,
		resultSchema,
		1,
		childExec,
		[]expression.Expression{partitionCol},
		2,
		limitCount,
	)
}

func drainPartitionTopNRows(t *testing.T, executor exec.Executor) [][]int64 {
	rows := make([][]int64, 0)
	for {
		chk := executor.NewChunk()
		require.NoError(t, exec.Next(context.Background(), executor, chk))
		if chk.NumRows() == 0 {
			return rows
		}
		for i := range chk.NumRows() {
			row := chk.GetRow(i)
			rows = append(rows, []int64{
				row.GetInt64(0),
				row.GetInt64(1),
				row.GetInt64(2),
			})
		}
	}
}
