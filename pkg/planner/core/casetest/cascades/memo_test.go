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

package cascades

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/planner/cascades/memo"
	"github.com/pingcap/tidb/pkg/planner/cascades/util"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/stretchr/testify/require"
)

func TestDeriveStats(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
		SQL string
		Str []string
	}
	statsSuiteData := GetCascadesSuiteData()
	statsSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, tt)
		ret := &plannercore.PreprocessorReturn{}
		nodeW := resolve.NewNodeW(stmt)
		err = plannercore.Preprocess(context.Background(), tk.Session(), nodeW, plannercore.WithPreprocessorReturn(ret))
		require.NoError(t, err)
		tk.Session().GetSessionVars().PlanColumnID.Store(0)
		builder, _ := plannercore.NewPlanBuilder().Init(tk.Session().GetPlanCtx(), ret.InfoSchema, hint.NewQBHintHandler(nil))
		p, err := builder.Build(ctx, nodeW)
		p.SCtx().GetSessionVars().StmtCtx.OriginalSQL = tt
		require.NoError(t, err, tt)
		p, err = plannercore.LogicalOptimizeTest(ctx, builder.GetOptFlag()|rule.FlagCollectPredicateColumnsPoint, p.(base.LogicalPlan))
		require.NoError(t, err, tt)
		lp := p.(base.LogicalPlan)
		// after stats derive is done, which means the up-down propagation of group ndv is done, in bottom-up building phase
		// of memo, we don't have to expect the upper operator's group cols passing down anymore.
		mm := memo.NewMemo()
		_, err = mm.Init(lp)
		require.Nil(t, err)
		// check the stats state in memo group.
		b := &bytes.Buffer{}
		sb := util.NewStrBuffer(b)
		var strs []string
		mm.ForEachGroup(func(g *memo.Group) bool {
			b.Reset()
			// record group
			g.String(sb)
			sb.WriteString(", ")
			// record first ge
			g.ForEachGE(func(ge *memo.GroupExpression) bool {
				ge.String(sb)
				return false
			})
			sb.WriteString(", ")
			// record group stats
			logicProp := g.GetLogicalProperty()
			if logicProp == nil {
				sb.WriteString("logic prop:nil")
			} else {
				sb.WriteString("logic prop:{")
				if logicProp.Stats == nil {
					sb.WriteString("stats:nil,")
				} else {
					statsStr := fmt.Sprintf("count %v, ColNDVs %v, GroupNDVs %v", logicProp.Stats.RowCount, logicProp.Stats.ColNDVs, logicProp.Stats.GroupNDVs)
					sb.WriteString("stats:{" + statsStr + "}")
				}
				sb.WriteString(", ")
				if logicProp.Schema == nil {
					sb.WriteString("schema:nil")
				} else {
					sb.WriteString("schema:{" + logicProp.Schema.String() + "}")
				}
				sb.WriteString("}")
			}
			sb.Flush()
			strs = append(strs, b.String())
			return true
		})
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Str = strs
		})
		require.Equal(t, output[i].Str, strs, "case i:"+strconv.Itoa(i)+" "+tt)
	}
}

func TestGroupNDVCols(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int not null, b int not null, key(a,b))")
	tk.MustExec("insert into t1 values(1,1),(1,2),(2,1),(2,2)")
	tk.MustExec("create table t2(a int not null, b int not null, key(a,b))")
	tk.MustExec("insert into t2 values(1,1),(1,2),(1,3),(2,1),(2,2),(2,3),(3,1),(3,2),(3,3)")
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")

	// Default RPC encoding may cause statistics explain result differ and then the test unstable.
	tk.MustExec("set @@tidb_enable_chunk_rpc = on")

	ctx := context.Background()
	p := parser.New()
	var input []string
	var output []struct {
		SQL string
		Str []string
	}
	statsSuiteData := GetCascadesSuiteData()
	statsSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, tt)
		ret := &plannercore.PreprocessorReturn{}
		nodeW := resolve.NewNodeW(stmt)
		err = plannercore.Preprocess(context.Background(), tk.Session(), nodeW, plannercore.WithPreprocessorReturn(ret))
		require.NoError(t, err)
		tk.Session().GetSessionVars().PlanColumnID.Store(0)
		builder, _ := plannercore.NewPlanBuilder().Init(tk.Session().GetPlanCtx(), ret.InfoSchema, hint.NewQBHintHandler(nil))
		p, err := builder.Build(ctx, nodeW)
		require.NoError(t, err, tt)
		p, err = plannercore.LogicalOptimizeTest(ctx, builder.GetOptFlag()|rule.FlagCollectPredicateColumnsPoint, p.(base.LogicalPlan))
		require.NoError(t, err, tt)
		lp := p.(base.LogicalPlan)
		// after stats derive is done, which means the up-down propagation of group ndv is done, in bottom-up building phase
		// of memo, we don't have to expect the upper operator's group cols passing down anymore.
		mm := memo.NewMemo()
		mm.Init(lp)
		// check the stats state in memo group.
		b := &bytes.Buffer{}
		sb := util.NewStrBuffer(b)
		var strs []string
		mm.ForEachGroup(func(g *memo.Group) bool {
			b.Reset()
			// record group
			g.String(sb)
			sb.WriteString(", ")
			// record first ge
			g.ForEachGE(func(ge *memo.GroupExpression) bool {
				ge.String(sb)
				return false
			})
			sb.WriteString(", ")
			// record group stats
			logicProp := g.GetLogicalProperty()
			if logicProp == nil {
				sb.WriteString("logic prop:nil")
			} else {
				sb.WriteString("logic prop:{")
				if logicProp.Stats == nil {
					sb.WriteString("stats:nil,")
				} else {
					statsStr := fmt.Sprintf("count %v, ColNDVs %v, GroupNDVs %v", logicProp.Stats.RowCount, logicProp.Stats.ColNDVs, logicProp.Stats.GroupNDVs)
					sb.WriteString("stats:{" + statsStr + "}")
				}
				sb.WriteString(", ")
				if logicProp.Schema == nil {
					sb.WriteString("schema:nil")
				} else {
					sb.WriteString("schema:{" + logicProp.Schema.String() + "}")
				}
				sb.WriteString("}")
			}
			sb.Flush()
			strs = append(strs, b.String())
			return true
		})
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Str = strs
		})
		require.Equal(t, output[i].Str, strs, "case i:"+strconv.Itoa(i)+" "+tt)
	}
}
