// Copyright 2020 PingCAP, Inc.
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

package core_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/pingcap/tidb/util/hint"
	"github.com/stretchr/testify/require"
)

func TestGroupNDVs(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int not null, b int not null, key(a,b))")
	tk.MustExec("insert into t1 values(1,1),(1,2),(2,1),(2,2),(1,1)")
	tk.MustExec("create table t2(a int not null, b int not null, key(a,b))")
	tk.MustExec("insert into t2 values(1,1),(1,2),(1,3),(2,1),(2,2),(2,3),(3,1),(3,2),(3,3),(1,1)")
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")

	ctx := context.Background()
	p := parser.New()
	var input []string
	var output []struct {
		SQL       string
		AggInput  string
		JoinInput string
	}
	statsSuiteData := core.GetStatsSuiteData()
	statsSuiteData.GetTestCases(t, &input, &output)
	for i, tt := range input {
		comment := fmt.Sprintf("case:%v sql: %s", i, tt)
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)
		ret := &core.PreprocessorReturn{}
		err = core.Preprocess(tk.Session(), stmt, core.WithPreprocessorReturn(ret))
		require.NoError(t, err)
		tk.Session().GetSessionVars().PlanColumnID = 0
		builder, _ := core.NewPlanBuilder().Init(tk.Session(), ret.InfoSchema, &hint.BlockHintProcessor{})
		p, err := builder.Build(ctx, stmt)
		require.NoError(t, err, comment)
		p, err = core.LogicalOptimize(ctx, builder.GetOptFlag(), p.(core.LogicalPlan))
		require.NoError(t, err, comment)
		lp := p.(core.LogicalPlan)
		_, err = core.RecursiveDeriveStats4Test(lp)
		require.NoError(t, err, comment)
		var agg *core.LogicalAggregation
		var join *core.LogicalJoin
		stack := make([]core.LogicalPlan, 0, 2)
		traversed := false
		for !traversed {
			switch v := lp.(type) {
			case *core.LogicalAggregation:
				agg = v
				lp = lp.Children()[0]
			case *core.LogicalJoin:
				join = v
				lp = v.Children()[0]
				stack = append(stack, v.Children()[1])
			case *core.LogicalApply:
				lp = lp.Children()[0]
				stack = append(stack, v.Children()[1])
			case *core.LogicalUnionAll:
				lp = lp.Children()[0]
				for i := 1; i < len(v.Children()); i++ {
					stack = append(stack, v.Children()[i])
				}
			case *core.DataSource:
				if len(stack) == 0 {
					traversed = true
				} else {
					lp = stack[0]
					stack = stack[1:]
				}
			default:
				lp = lp.Children()[0]
			}
		}
		aggInput := ""
		joinInput := ""
		if agg != nil {
			s := core.GetStats4Test(agg.Children()[0])
			aggInput = property.ToString(s.GroupNDVs)
		}
		if join != nil {
			l := core.GetStats4Test(join.Children()[0])
			r := core.GetStats4Test(join.Children()[1])
			joinInput = property.ToString(l.GroupNDVs) + ";" + property.ToString(r.GroupNDVs)
		}
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].AggInput = aggInput
			output[i].JoinInput = joinInput
		})
		require.Equal(t, aggInput, output[i].AggInput, comment)
		require.Equal(t, joinInput, output[i].JoinInput, comment)
	}
}

func TestNDVGroupCols(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int not null, b int not null, key(a,b))")
	tk.MustExec("insert into t1 values(1,1),(1,2),(2,1),(2,2)")
	tk.MustExec("create table t2(a int not null, b int not null, key(a,b))")
	tk.MustExec("insert into t2 values(1,1),(1,2),(1,3),(2,1),(2,2),(2,3),(3,1),(3,2),(3,3)")
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	statsSuiteData := core.GetStatsSuiteData()
	statsSuiteData.GetTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + tt).Rows())
		})
		// The test point is the row count estimation for aggregations and joins.
		tk.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].Plan...))
	}
}
