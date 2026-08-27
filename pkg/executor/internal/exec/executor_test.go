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

package exec

import (
	"testing"

	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type mockNextIOAccExecutor struct {
	BaseExecutorV2
}

func newMockNextIOAccExecutor(children ...Executor) *mockNextIOAccExecutor {
	ctx := mock.NewContext()
	return &mockNextIOAccExecutor{
		BaseExecutorV2: NewBaseExecutorV2(ctx.GetSessionVars(), nil, 0, children...),
	}
}

func TestNextIOAccAddInputCountsRowsWithZeroCols(t *testing.T) {
	t.Run("add input counts rows with zero cols", func(t *testing.T) {
		acc := &nextIOAcc{}

		acc.addInput(3, 0)

		require.Equal(t, int64(3), acc.inRows)
		require.Equal(t, int64(0), acc.inCells)
	})

	t.Run("base executor reuses local accumulator state", func(t *testing.T) {
		exec := newMockNextIOAccExecutor()

		first := getReusableNextIOAcc(exec)
		first.addInput(4, 2)

		second := getReusableNextIOAcc(exec)
		require.Same(t, first, second)
		require.Equal(t, int64(0), second.inRows)
		require.Equal(t, int64(0), second.inCells)

		allocs := testing.AllocsPerRun(1000, func() {
			acc := getReusableNextIOAcc(exec)
			acc.addInput(1, 1)
		})
		require.Less(t, allocs, 1.)
	})

	t.Run("only executors with children need local accumulator", func(t *testing.T) {
		parentAcc := &nextIOAcc{}

		require.False(t, needNextIOAcc(true, nil, 0))
		require.True(t, needNextIOAcc(true, nil, 1))
		require.False(t, needNextIOAcc(false, parentAcc, 0))
		require.True(t, needNextIOAcc(false, parentAcc, 1))
	})
}

func TestRUV2ExecutorMetricByTypeIncludesConcreteExecutorTypes(t *testing.T) {
	cases := map[string]ruv2ExecutorMetric{
		"*aggregate.HashAggExec":        {level: 2, label: "HashAggExec", useCells: false},
		"*aggregate.StreamAggExec":      {level: 3, label: "StreamAggExec", useCells: false},
		"*executor.BatchPointGetExec":   {level: 1, label: "BatchPointGetExec", useCells: true},
		"*executor.ExpandExec":          {level: 2, label: "ExpandExec", useCells: false},
		"*executor.IndexLookUpExecutor": {level: 2, label: "IndexLookUpExecutor", useCells: false},
		"*executor.IndexReaderExecutor": {level: 2, label: "IndexReaderExecutor", useCells: false},
		"*executor.LimitExec":           {level: 1, label: "LimitExec", useCells: true},
		"*executor.MemTableReaderExec":  {level: 2, label: "MemTableReaderExec", useCells: false},
		"*executor.PointGetExecutor":    {level: 1, label: "PointGetExecutor", useCells: true},
		"*executor.ProjectionExec":      {level: 2, label: "ProjectionExec", useCells: true},
		"*executor.SelectLockExec":      {level: 2, label: "SelectLockExec", useCells: true},
		"*executor.SelectionExec":       {level: 2, label: "SelectionExec", useCells: false},
		"*executor.TableDualExec":       {level: 2, label: "TableDualExec", useCells: false},
		"*executor.TableReaderExecutor": {level: 2, label: "TableReaderExecutor", useCells: false},
		"*executor.UnionScanExec":       {level: 2, label: "UnionScanExec", useCells: false},
		"*join.HashJoinV1Exec":          {level: 2, label: "HashJoinV1Exec", useCells: false},
		"*join.HashJoinV2Exec":          {level: 2, label: "HashJoinV2Exec", useCells: false},
		"*join.IndexLookUpJoin":         {level: 2, label: "IndexLookUpJoin", useCells: true},
		"*join.IndexLookUpMergeJoin":    {level: 2, label: "IndexLookUpMergeJoin", useCells: true},
		"*join.IndexNestedLoopHashJoin": {level: 2, label: "IndexNestedLoopHashJoin", useCells: true},
		"*join.MergeJoinExec":           {level: 2, label: "MergeJoinExec", useCells: false},
		"*sortexec.SortExec":            {level: 3, label: "SortExec", useCells: true},
		"*sortexec.TopNExec":            {level: 2, label: "TopNExec", useCells: true},
		"*windows.OrderedWindowExec":    {level: 2, label: "WindowExec", useCells: false},
		"*windows.PipelinedWindowExec":  {level: 2, label: "WindowExec", useCells: false},
		"*windows.WindowExec":           {level: 2, label: "WindowExec", useCells: false},
	}

	for typ, expected := range cases {
		actual, ok := ruv2ExecutorMetricByType(typ)
		require.True(t, ok, typ)
		require.Equal(t, expected, actual)
	}

	for _, staleType := range []string{
		"*executor.HashJoinExec",
		"*executor.IndexLookUpJoin",
		"*executor.SortExec",
		"*executor.WindowExec",
	} {
		_, ok := ruv2ExecutorMetricByType(staleType)
		require.False(t, ok, staleType)
	}
}
