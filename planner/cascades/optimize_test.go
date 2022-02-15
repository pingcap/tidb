// Copyright 2018 PingCAP, Inc.
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
	"context"
	"math"
	"testing"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/memo"
	"github.com/pingcap/tidb/planner/property"
	"github.com/stretchr/testify/require"
)

func TestImplGroupZeroCost(t *testing.T) {
	p := parser.New()
	ctx := plannercore.MockContext()
	is := infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockSignedTable()})
	domain.GetDomain(ctx).MockInfoCacheAndLoadInfoSchema(is)

	stmt, err := p.ParseOneStmt("select t1.a, t2.a from t as t1 left join t as t2 on t1.a = t2.a where t1.a < 1.0", "", "")
	require.NoError(t, err)

	plan, _, err := plannercore.BuildLogicalPlanForTest(context.Background(), ctx, stmt, is)
	require.NoError(t, err)

	logic, ok := plan.(plannercore.LogicalPlan)
	require.True(t, ok)

	rootGroup := memo.Convert2Group(logic)
	prop := &property.PhysicalProperty{
		ExpectedCnt: math.MaxFloat64,
	}
	impl, err := NewOptimizer().implGroup(rootGroup, prop, 0.0)
	require.NoError(t, err)
	require.Nil(t, impl)
}

func TestInitGroupSchema(t *testing.T) {
	p := parser.New()
	ctx := plannercore.MockContext()
	is := infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockSignedTable()})
	domain.GetDomain(ctx).MockInfoCacheAndLoadInfoSchema(is)

	stmt, err := p.ParseOneStmt("select a from t", "", "")
	require.NoError(t, err)

	plan, _, err := plannercore.BuildLogicalPlanForTest(context.Background(), ctx, stmt, is)
	require.NoError(t, err)

	logic, ok := plan.(plannercore.LogicalPlan)
	require.True(t, ok)

	g := memo.Convert2Group(logic)
	require.NotNil(t, g)
	require.NotNil(t, g.Prop)
	require.Equal(t, 1, g.Prop.Schema.Len())
	require.Nil(t, g.Prop.Stats)
}

func TestFillGroupStats(t *testing.T) {
	p := parser.New()
	ctx := plannercore.MockContext()
	is := infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockSignedTable()})
	domain.GetDomain(ctx).MockInfoCacheAndLoadInfoSchema(is)

	stmt, err := p.ParseOneStmt("select * from t t1 join t t2 on t1.a = t2.a", "", "")
	require.NoError(t, err)

	plan, _, err := plannercore.BuildLogicalPlanForTest(context.Background(), ctx, stmt, is)
	require.NoError(t, err)

	logic, ok := plan.(plannercore.LogicalPlan)
	require.True(t, ok)

	rootGroup := memo.Convert2Group(logic)
	err = NewOptimizer().fillGroupStats(rootGroup)
	require.NoError(t, err)
	require.NotNil(t, rootGroup.Prop.Stats)
}

func TestPreparePossibleProperties(t *testing.T) {
	p := parser.New()
	ctx := plannercore.MockContext()
	is := infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockSignedTable()})
	domain.GetDomain(ctx).MockInfoCacheAndLoadInfoSchema(is)
	optimizer := NewOptimizer()

	optimizer.ResetTransformationRules(map[memo.Operand][]Transformation{
		memo.OperandDataSource: {
			NewRuleEnumeratePaths(),
		},
	})
	defer func() {
		optimizer.ResetTransformationRules(DefaultRuleBatches...)
	}()

	stmt, err := p.ParseOneStmt("select f, sum(a) from t group by f", "", "")
	require.NoError(t, err)

	plan, _, err := plannercore.BuildLogicalPlanForTest(context.Background(), ctx, stmt, is)
	require.NoError(t, err)

	logic, ok := plan.(plannercore.LogicalPlan)
	require.True(t, ok)

	logic, err = optimizer.onPhasePreprocessing(ctx, logic)
	require.NoError(t, err)

	// collect the target columns: f, a
	ds, ok := logic.Children()[0].Children()[0].(*plannercore.DataSource)
	require.True(t, ok)

	var columnF, columnA *expression.Column
	for i, col := range ds.Columns {
		if col.Name.L == "f" {
			columnF = ds.Schema().Columns[i]
		} else if col.Name.L == "a" {
			columnA = ds.Schema().Columns[i]
		}
	}
	require.NotNil(t, columnF)
	require.NotNil(t, columnA)

	agg, ok := logic.Children()[0].(*plannercore.LogicalAggregation)
	require.True(t, ok)

	group := memo.Convert2Group(agg)
	require.NoError(t, optimizer.onPhaseExploration(ctx, group))

	// The memo looks like this:
	// Group#0 Schema:[Column#13,test.t.f]
	//   Aggregation_2 input:[Group#1], group by:test.t.f, funcs:sum(test.t.a), firstrow(test.t.f)
	// Group#1 Schema:[test.t.a,test.t.f]
	//   TiKVSingleGather_5 input:[Group#2], table:t
	//   TiKVSingleGather_9 input:[Group#3], table:t, index:f_g
	//   TiKVSingleGather_7 input:[Group#4], table:t, index:f
	// Group#2 Schema:[test.t.a,test.t.f]
	//   TableScan_4 table:t, pk col:test.t.a
	// Group#3 Schema:[test.t.a,test.t.f]
	//   IndexScan_8 table:t, index:f, g
	// Group#4 Schema:[test.t.a,test.t.f]
	//   IndexScan_6 table:t, index:f
	propMap := make(map[*memo.Group][][]*expression.Column)
	aggProp := preparePossibleProperties(group, propMap)
	// We only have one prop for Group0 : f
	require.Len(t, aggProp, 1)
	require.True(t, aggProp[0][0].Equal(nil, columnF))

	gatherGroup := group.Equivalents.Front().Value.(*memo.GroupExpr).Children[0]
	gatherProp, ok := propMap[gatherGroup]
	require.True(t, ok)
	// We have 2 props for Group1: [f], [a]
	require.Len(t, gatherProp, 2)
	for _, prop := range gatherProp {
		require.Len(t, prop, 1)
		require.True(t, prop[0].Equal(nil, columnA) || prop[0].Equal(nil, columnF))
	}
}

// fakeTransformation is used for TestAppliedRuleSet.
type fakeTransformation struct {
	baseRule
	appliedTimes int
}

// OnTransform implements Transformation interface.
func (rule *fakeTransformation) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	rule.appliedTimes++
	old.GetExpr().AddAppliedRule(rule)
	return []*memo.GroupExpr{old.GetExpr()}, true, false, nil
}

func TestAppliedRuleSet(t *testing.T) {
	p := parser.New()
	ctx := plannercore.MockContext()
	is := infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockSignedTable()})
	domain.GetDomain(ctx).MockInfoCacheAndLoadInfoSchema(is)
	optimizer := NewOptimizer()

	rule := fakeTransformation{}
	rule.pattern = memo.NewPattern(memo.OperandProjection, memo.EngineAll)
	optimizer.ResetTransformationRules(map[memo.Operand][]Transformation{
		memo.OperandProjection: {
			&rule,
		},
	})
	defer func() {
		optimizer.ResetTransformationRules(DefaultRuleBatches...)
	}()

	stmt, err := p.ParseOneStmt("select 1", "", "")
	require.NoError(t, err)

	plan, _, err := plannercore.BuildLogicalPlanForTest(context.Background(), ctx, stmt, is)
	require.NoError(t, err)

	logic, ok := plan.(plannercore.LogicalPlan)
	require.True(t, ok)

	group := memo.Convert2Group(logic)
	require.NoError(t, optimizer.onPhaseExploration(ctx, group))
	require.Equal(t, 1, rule.appliedTimes)
}
