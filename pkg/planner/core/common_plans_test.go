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

package core

import (
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

var explainRUForestRowsSink [][]string

func newExplainRUForestFixture(cteCount, scalarCount int) (*FlatPhysicalPlan, *ExplainRUResult) {
	ctx := mock.NewContext()
	dual := physicalop.PhysicalTableDual{RowCount: 1}.Init(ctx, &property.StatsInfo{RowCount: 1}, 0)
	dual.SetID(101)
	newTree := func(size int) (FlatPlanTree, []ExplainRUOperatorResult) {
		tree := make(FlatPlanTree, size)
		result := make([]ExplainRUOperatorResult, size)
		for i := range tree {
			tree[i] = &FlatOperator{
				Origin:         dual,
				ChildrenEndIdx: size - 1,
				Depth:          uint32(i),
				IsRoot:         true,
				StoreType:      kv.TiDB,
				IsPhysicalPlan: true,
			}
			if i+1 < size {
				tree[i].ChildrenIdx = []int{i + 1}
			}
			result[i] = ExplainRUOperatorResult{
				PlanID:         dual.ID(),
				ExplainID:      tree[i].ExplainID().String(),
				ChildrenIdx:    append([]int(nil), tree[i].ChildrenIdx...),
				ChildrenEndIdx: tree[i].ChildrenEndIdx,
				Depth:          tree[i].Depth,
				IsRoot:         tree[i].IsRoot,
				StoreType:      tree[i].StoreType,
				IsPhysicalPlan: tree[i].IsPhysicalPlan,
				SelfRU:         1,
				CumRU:          float64(size - i),
			}
		}
		return tree, result
	}
	main, mainResult := newTree(3)
	flat := &FlatPhysicalPlan{
		Main:             main,
		CTEs:             make([]FlatPlanTree, cteCount),
		ScalarSubQueries: make([]FlatPlanTree, scalarCount),
	}
	result := &ExplainRUResult{
		TotalRU:          float64(3 + 3*cteCount + 3*scalarCount),
		Main:             mainResult,
		CTEs:             make([][]ExplainRUOperatorResult, cteCount),
		ScalarSubQueries: make([][]ExplainRUOperatorResult, scalarCount),
	}
	for i := range flat.CTEs {
		flat.CTEs[i], result.CTEs[i] = newTree(3)
	}
	for i := range flat.ScalarSubQueries {
		flat.ScalarSubQueries[i], result.ScalarSubQueries[i] = newTree(3)
	}
	return flat, result
}

func TestNewLineFieldsInfo(t *testing.T) {
	cases := []struct {
		sql      string
		expected LineFieldsInfo
	}{
		{
			"load data infile 'a' into table t",
			LineFieldsInfo{
				FieldsTerminatedBy: "\t",
				FieldsEnclosedBy:   "",
				FieldsEscapedBy:    "\\",
				FieldsOptEnclosed:  false,
				LinesStartingBy:    "",
				LinesTerminatedBy:  "\n",
			},
		},
		{
			"load data infile 'a' into table t fields terminated by 'a'",
			LineFieldsInfo{
				FieldsTerminatedBy: "a",
				FieldsEnclosedBy:   "",
				FieldsEscapedBy:    "\\",
				FieldsOptEnclosed:  false,
				LinesStartingBy:    "",
				LinesTerminatedBy:  "\n",
			},
		},
		{
			"load data infile 'a' into table t fields optionally enclosed by 'a'",
			LineFieldsInfo{
				FieldsTerminatedBy: "\t",
				FieldsEnclosedBy:   "a",
				FieldsEscapedBy:    "\\",
				FieldsOptEnclosed:  true,
				LinesStartingBy:    "",
				LinesTerminatedBy:  "\n",
			},
		},
		{
			"load data infile 'a' into table t fields enclosed by 'a'",
			LineFieldsInfo{
				FieldsTerminatedBy: "\t",
				FieldsEnclosedBy:   "a",
				FieldsEscapedBy:    "\\",
				FieldsOptEnclosed:  false,
				LinesStartingBy:    "",
				LinesTerminatedBy:  "\n",
			},
		},
		{
			"load data infile 'a' into table t fields escaped by 'a'",
			LineFieldsInfo{
				FieldsTerminatedBy: "\t",
				FieldsEnclosedBy:   "",
				FieldsEscapedBy:    "a",
				FieldsOptEnclosed:  false,
				LinesStartingBy:    "",
				LinesTerminatedBy:  "\n",
			},
		},
		{
			"load data infile 'a' into table t lines starting by 'a'",
			LineFieldsInfo{
				FieldsTerminatedBy: "\t",
				FieldsEnclosedBy:   "",
				FieldsEscapedBy:    "\\",
				FieldsOptEnclosed:  false,
				LinesStartingBy:    "a",
				LinesTerminatedBy:  "\n",
			},
		},
		{
			"load data infile 'a' into table t lines terminated by 'aa'",
			LineFieldsInfo{
				FieldsTerminatedBy: "\t",
				FieldsEnclosedBy:   "",
				FieldsEscapedBy:    "\\",
				FieldsOptEnclosed:  false,
				LinesStartingBy:    "",
				LinesTerminatedBy:  "aa",
			},
		},
	}
	p := parser.New()
	for _, c := range cases {
		stmt, err := p.ParseOneStmt(c.sql, "", "")
		require.NoError(t, err, c.sql)
		ldStmt := stmt.(*ast.LoadDataStmt)
		lineFieldsInfo := NewLineFieldsInfo(ldStmt.FieldsInfo, ldStmt.LinesInfo)
		require.Equal(t, c.expected, lineFieldsInfo)
	}
}

func TestExplainRUResultSnapshotContract(t *testing.T) {
	flat, result := newExplainRUForestFixture(1, 1)

	t.Run("setter deep copies and clears", func(t *testing.T) {
		explain := &Explain{}
		explain.SetRUResult(result)
		result.Main[0].SelfRU = 999
		result.Main[0].ChildrenIdx[0] = 999
		result.CTEs[0][0].CumRU = 999
		require.Equal(t, float64(1), explain.ruResult.Main[0].SelfRU)
		require.Equal(t, 1, explain.ruResult.Main[0].ChildrenIdx[0])
		require.Equal(t, float64(3), explain.ruResult.CTEs[0][0].CumRU)
		result.Main[0].ChildrenIdx[0] = 1

		explain.SetRUResult(nil)
		require.Nil(t, explain.ruResult)
		require.True(t, explain.ruResultSet)
	})

	t.Run("shape mismatch fails closed", func(t *testing.T) {
		coll := execdetails.NewRuntimeStatsColl(nil)
		coll.RegisterStats(101, &execdetails.ExplainRURuntimeStats{SelfRU: 99, CumRU: 99})
		mismatch := *result
		mismatch.Main = append([]ExplainRUOperatorResult(nil), result.Main...)
		mismatch.Main[0].PlanID++

		rows := explainFlatPlanInRUFormat(flat, coll, &mismatch)
		require.NotEmpty(t, rows)
		for _, row := range rows {
			require.Empty(t, row[3])
			require.Empty(t, row[4])
			require.Empty(t, row[5])
		}
	})

	t.Run("aliased plan IDs still require matching occurrence metadata", func(t *testing.T) {
		mismatch := *result
		mismatch.Main = append([]ExplainRUOperatorResult(nil), result.Main...)
		mismatch.Main[1].Label = BuildSide

		rows := explainFlatPlanInRUFormat(flat, nil, &mismatch)
		require.NotEmpty(t, rows)
		for _, row := range rows {
			require.Empty(t, row[3])
			require.Empty(t, row[4])
			require.Empty(t, row[5])
		}
	})
}

func BenchmarkExplainRUForestProjection(b *testing.B) {
	for _, scenario := range []struct {
		name        string
		cteCount    int
		scalarCount int
	}{
		{name: "ordinary"},
		{name: "one-scalar", scalarCount: 1},
		{name: "one-cte", cteCount: 1},
		{name: "multiple-cte-scalar", cteCount: 2, scalarCount: 2},
		{name: "large-forest", cteCount: 8, scalarCount: 8},
	} {
		b.Run(scenario.name, func(b *testing.B) {
			flat, result := newExplainRUForestFixture(scenario.cteCount, scenario.scalarCount)
			require.NotEmpty(b, explainFlatPlanInRUFormat(flat, nil, result))
			b.ResetTimer()
			// This timer covers occurrence-coordinate validation and EXPLAIN RU
			// row formatting only. It excludes calculation and SQL execution.
			b.ReportAllocs()
			for b.Loop() {
				explainRUForestRowsSink = explainFlatPlanInRUFormat(flat, nil, result)
			}
		})
	}
}
