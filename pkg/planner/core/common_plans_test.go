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
	newTree := func(size int) FlatPlanTree {
		tree := make(FlatPlanTree, size)
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
		}
		return tree
	}
	flat := &FlatPhysicalPlan{
		Main:             newTree(3),
		CTEs:             make([]FlatPlanTree, cteCount),
		ScalarSubQueries: make([]FlatPlanTree, scalarCount),
	}
	for i := range flat.CTEs {
		flat.CTEs[i] = newTree(3)
	}
	for i := range flat.ScalarSubQueries {
		flat.ScalarSubQueries[i] = newTree(3)
	}
	result := NewExplainRUResult(flat)
	fillTree := func(tree []ExplainRUOperatorResult) {
		for i := range tree {
			tree[i].SelfRU = 1
			tree[i].CumRU = float64(len(tree) - i)
		}
	}
	fillTree(result.Main)
	for i := range result.CTEs {
		fillTree(result.CTEs[i])
	}
	for i := range result.ScalarSubQueries {
		fillTree(result.ScalarSubQueries[i])
	}
	result.TotalRU = float64(3 + 3*cteCount + 3*scalarCount)
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

func TestExplainRUResultOwnershipContract(t *testing.T) {
	t.Run("setter owns exact forest and clears", func(t *testing.T) {
		flat, result := newExplainRUForestFixture(1, 1)
		explain := &Explain{}
		require.Same(t, flat.Main[0], result.Main[0].Operator)
		explain.SetRUResult(result)
		require.Same(t, result, explain.ruResult)
		require.Same(t, flat.CTEs[0][0], explain.ruResult.CTEs[0][0].Operator)

		explain.SetRUResult(nil)
		require.Nil(t, explain.ruResult)
		require.True(t, explain.ruResultSet)
	})

	t.Run("owned result forest renders without target reflatten", func(t *testing.T) {
		flat, result := newExplainRUForestFixture(1, 1)
		coll := execdetails.NewRuntimeStatsColl(nil)
		coll.RegisterStats(101, &execdetails.ExplainRURuntimeStats{SelfRU: 99, CumRU: 99})
		result.Main[1].SelfRU = 2
		explain := &Explain{TargetPlan: flat.Main[0].Origin, Format: "ru", RuntimeStatsColl: coll}
		explain.SetRUResult(result)
		require.NoError(t, explain.RenderResult())
		require.Len(t, explain.Rows, 9)
		require.Equal(t, []string{"1.00", "3.00", "33.33%"}, explain.Rows[0][3:6])
		require.Equal(t, []string{"2.00", "2.00", "22.22%"}, explain.Rows[1][3:6])
	})

	t.Run("invalid operator makes the whole result unavailable", func(t *testing.T) {
		flat, result := newExplainRUForestFixture(1, 1)
		result.Main[0].Operator = nil
		coll := execdetails.NewRuntimeStatsColl(nil)
		coll.RegisterStats(101, &execdetails.ExplainRURuntimeStats{SelfRU: 99, CumRU: 99})
		explain := &Explain{TargetPlan: flat.Main[0].Origin, Format: "ru", RuntimeStatsColl: coll}
		explain.SetRUResult(result)

		require.NoError(t, explain.RenderResult())
		require.Len(t, explain.Rows, 1)
		require.Empty(t, explain.Rows[0][3])
		require.Empty(t, explain.Rows[0][4])
		require.Empty(t, explain.Rows[0][5])
	})
}

func BenchmarkExplainRUForestRendering(b *testing.B) {
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
			_, result := newExplainRUForestFixture(scenario.cteCount, scenario.scalarCount)
			rows, ok := explainRUResultInRUFormat(nil, result)
			require.True(b, ok)
			require.NotEmpty(b, rows)
			b.ResetTimer()
			// This timer covers owned-result EXPLAIN RU row formatting only.
			// It excludes calculation and SQL execution.
			b.ReportAllocs()
			for b.Loop() {
				explainRUForestRowsSink, _ = explainRUResultInRUFormat(nil, result)
			}
		})
	}
}
