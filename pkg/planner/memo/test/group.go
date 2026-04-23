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

package memo_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/memo"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/require"
)

func RunNewGroup(t *testing.T) {
	p := &logicalop.LogicalLimit{}
	expr := memo.ExportedNewGroupExpr(p)
	g := memo.ExportedNewGroupWithSchema(expr, expression.NewSchema())

	require.Equal(t, 1, g.Equivalents.Len())
	require.Equal(t, expr, g.Equivalents.Front().Value.(*memo.ExportedGroupExpr))
	require.Len(t, g.Fingerprints, 1)
	require.False(t, g.Explored(0))
}

func RunGroupInsert(t *testing.T) {
	p := &logicalop.LogicalLimit{}
	expr := memo.ExportedNewGroupExpr(p)
	g := memo.ExportedNewGroupWithSchema(expr, expression.NewSchema())
	require.False(t, g.Insert(expr))
	memo.ExportedSetSelfFingerprint(expr, "1")
	require.True(t, g.Insert(expr))
}

func RunGroupDelete(t *testing.T) {
	p := &logicalop.LogicalLimit{}
	expr := memo.ExportedNewGroupExpr(p)
	g := memo.ExportedNewGroupWithSchema(expr, expression.NewSchema())
	require.Equal(t, 1, g.Equivalents.Len())

	g.Delete(expr)
	require.Equal(t, 0, g.Equivalents.Len())

	g.Delete(expr)
	require.Equal(t, 0, g.Equivalents.Len())
}

func RunGroupDeleteAll(t *testing.T) {
	ctx := coretestsdk.MockContext()
	defer func() {
		do := domain.GetDomain(ctx)
		do.StatsHandle().Close()
	}()
	expr := memo.ExportedNewGroupExpr(logicalop.LogicalSelection{}.Init(ctx, 0))
	g := memo.ExportedNewGroupWithSchema(expr, expression.NewSchema())
	require.True(t, g.Insert(memo.ExportedNewGroupExpr(logicalop.LogicalLimit{}.Init(ctx, 0))))
	require.True(t, g.Insert(memo.ExportedNewGroupExpr(logicalop.LogicalProjection{}.Init(ctx, 0))))
	require.Equal(t, 3, g.Equivalents.Len())
	require.NotNil(t, g.GetFirstElem(pattern.OperandProjection))
	require.True(t, g.Exists(expr))

	g.DeleteAll()
	require.Equal(t, 0, g.Equivalents.Len())
	require.Nil(t, g.GetFirstElem(pattern.OperandProjection))
	require.False(t, g.Exists(expr))
}

func RunGroupExists(t *testing.T) {
	p := &logicalop.LogicalLimit{}
	expr := memo.ExportedNewGroupExpr(p)
	g := memo.ExportedNewGroupWithSchema(expr, expression.NewSchema())
	require.True(t, g.Exists(expr))

	g.Delete(expr)
	require.False(t, g.Exists(expr))
}

func RunGroupFingerPrint(t *testing.T) {
	vardef.SetEnableMDL(false)
	p := parser.New()
	stmt1, err := p.ParseOneStmt("select * from t where a > 1 and a < 100", "", "")
	require.NoError(t, err)

	is := infoschema.MockInfoSchema([]*model.TableInfo{coretestsdk.MockSignedTable()})
	ctx := coretestsdk.MockContext()
	defer func() {
		do := domain.GetDomain(ctx)
		do.StatsHandle().Close()
	}()
	nodeW := resolve.NewNodeW(stmt1)
	plan, err := plannercore.BuildLogicalPlanForTest(context.Background(), ctx, nodeW, is)
	require.NoError(t, err)
	logic1, ok := plan.(base.LogicalPlan)
	require.True(t, ok)

	// Plan tree should be: DataSource -> Selection -> Projection
	proj, ok := logic1.(*logicalop.LogicalProjection)
	require.True(t, ok)
	sel, ok := logic1.Children()[0].(*logicalop.LogicalSelection)
	require.True(t, ok)
	group1 := memo.ExportedConvert2Group(logic1)
	oldGroupExpr := group1.Equivalents.Front().Value.(*memo.ExportedGroupExpr)

	// Insert a GroupExpr with the same ExprNode.
	newGroupExpr := memo.ExportedNewGroupExpr(proj)
	newGroupExpr.SetChildren(oldGroupExpr.Children[0])
	group1.Insert(newGroupExpr)
	require.Equal(t, 1, group1.Equivalents.Len())

	// Insert a GroupExpr with different children。
	newGroupExpr2 := memo.ExportedNewGroupExpr(proj)
	newGroup := memo.ExportedNewGroupWithSchema(oldGroupExpr, group1.Prop.Schema)
	newGroupExpr2.SetChildren(newGroup)
	group1.Insert(newGroupExpr2)
	require.Equal(t, 2, group1.Equivalents.Len())

	// Insert a GroupExpr with different ExprNode.
	limit := logicalop.LogicalLimit{}.Init(proj.SCtx(), 0)
	newGroupExpr3 := memo.ExportedNewGroupExpr(limit)
	newGroupExpr3.SetChildren(oldGroupExpr.Children[0])
	group1.Insert(newGroupExpr3)
	require.Equal(t, 3, group1.Equivalents.Len())

	// Insert two LogicalSelections with same conditions but different order.
	require.Len(t, sel.Conditions, 2)
	newSelection := logicalop.LogicalSelection{
		Conditions: make([]expression.Expression, 2)}.Init(sel.SCtx(), sel.QueryBlockOffset())
	newSelection.Conditions[0], newSelection.Conditions[1] = sel.Conditions[1], sel.Conditions[0]
	newGroupExpr4 := memo.ExportedNewGroupExpr(sel)
	newGroupExpr5 := memo.ExportedNewGroupExpr(newSelection)
	newGroupExpr4.SetChildren(oldGroupExpr.Children[0])
	newGroupExpr5.SetChildren(oldGroupExpr.Children[0])
	group1.Insert(newGroupExpr4)
	require.Equal(t, 4, group1.Equivalents.Len())
	group1.Insert(newGroupExpr5)
	require.Equal(t, 4, group1.Equivalents.Len())
}

func RunGroupGetFirstElem(t *testing.T) {
	ctx := coretestsdk.MockContext()
	defer func() {
		do := domain.GetDomain(ctx)
		do.StatsHandle().Close()
	}()
	expr0 := memo.ExportedNewGroupExpr(logicalop.LogicalProjection{}.Init(ctx, 0))
	expr1 := memo.ExportedNewGroupExpr(logicalop.LogicalLimit{}.Init(ctx, 0))
	expr2 := memo.ExportedNewGroupExpr(logicalop.LogicalProjection{}.Init(ctx, 0))
	expr3 := memo.ExportedNewGroupExpr(logicalop.LogicalLimit{}.Init(ctx, 0))
	expr4 := memo.ExportedNewGroupExpr(logicalop.LogicalProjection{}.Init(ctx, 0))

	g := memo.ExportedNewGroupWithSchema(expr0, expression.NewSchema())
	g.Insert(expr1)
	g.Insert(expr2)
	g.Insert(expr3)
	g.Insert(expr4)

	require.Equal(t, expr0, g.GetFirstElem(pattern.OperandProjection).Value.(*memo.ExportedGroupExpr))
	require.Equal(t, expr1, g.GetFirstElem(pattern.OperandLimit).Value.(*memo.ExportedGroupExpr))
	require.Equal(t, expr0, g.GetFirstElem(pattern.OperandAny).Value.(*memo.ExportedGroupExpr))
}

type fakeImpl struct {
	plan base.PhysicalPlan
}

func (impl *fakeImpl) CalcCost(float64, ...memo.ExportedImplementation) float64 { return 0 }
func (impl *fakeImpl) SetCost(float64)                                          {}
func (impl *fakeImpl) GetCost() float64                                         { return 0 }
func (impl *fakeImpl) GetPlan() base.PhysicalPlan                               { return impl.plan }
func (impl *fakeImpl) AttachChildren(...memo.ExportedImplementation) memo.ExportedImplementation {
	return nil
}
func (impl *fakeImpl) GetCostLimit(float64, ...memo.ExportedImplementation) float64 { return 0 }

func RunGetInsertGroupImpl(t *testing.T) {
	ctx := coretestsdk.MockContext()
	g := memo.ExportedNewGroupWithSchema(memo.ExportedNewGroupExpr(logicalop.LogicalLimit{}.Init(ctx, 0)), expression.NewSchema())
	defer func() {
		do := domain.GetDomain(ctx)
		do.StatsHandle().Close()
	}()
	emptyProp := &property.PhysicalProperty{}
	require.Nil(t, g.GetImpl(emptyProp))

	impl := &fakeImpl{plan: &physicalop.PhysicalLimit{}}
	g.InsertImpl(emptyProp, impl)
	require.Equal(t, impl, g.GetImpl(emptyProp))

	orderProp := &property.PhysicalProperty{SortItems: []property.SortItem{{Col: &expression.Column{}}}}
	require.Nil(t, g.GetImpl(orderProp))
}

func RunFirstElemAfterDelete(t *testing.T) {
	ctx := coretestsdk.MockContext()
	defer func() {
		do := domain.GetDomain(ctx)
		do.StatsHandle().Close()
	}()
	oldExpr := memo.ExportedNewGroupExpr(logicalop.LogicalLimit{Count: 10}.Init(ctx, 0))
	g := memo.ExportedNewGroupWithSchema(oldExpr, expression.NewSchema())
	newExpr := memo.ExportedNewGroupExpr(logicalop.LogicalLimit{Count: 20}.Init(ctx, 0))
	g.Insert(newExpr)
	require.NotNil(t, g.GetFirstElem(pattern.OperandLimit))
	require.Equal(t, oldExpr, g.GetFirstElem(pattern.OperandLimit).Value)
	g.Delete(oldExpr)
	require.NotNil(t, g.GetFirstElem(pattern.OperandLimit))
	require.Equal(t, newExpr, g.GetFirstElem(pattern.OperandLimit).Value)
	g.Delete(newExpr)
	require.Nil(t, g.GetFirstElem(pattern.OperandLimit))
}

func RunBuildKeyInfo(t *testing.T) {
	vardef.SetEnableMDL(false)
	p := parser.New()
	ctx := coretestsdk.MockContext()
	defer func() {
		do := domain.GetDomain(ctx)
		do.StatsHandle().Close()
	}()
	is := infoschema.MockInfoSchema([]*model.TableInfo{coretestsdk.MockSignedTable()})
	domain.GetDomain(ctx).MockInfoCacheAndLoadInfoSchema(is)

	// case 1: primary key has constant constraint
	stmt1, err := p.ParseOneStmt("select a from t where a = 10", "", "")
	require.NoError(t, err)
	nodeW1 := resolve.NewNodeW(stmt1)
	p1, err := plannercore.BuildLogicalPlanForTest(context.Background(), ctx, nodeW1, is)
	require.NoError(t, err)
	logic1, ok := p1.(base.LogicalPlan)
	require.True(t, ok)
	group1 := memo.ExportedConvert2Group(logic1)
	group1.BuildKeyInfo()
	require.True(t, group1.Prop.MaxOneRow)
	require.Len(t, group1.Prop.Schema.PKOrUK, 1)

	// case 2: group by column is key
	stmt2, err := p.ParseOneStmt("select b, sum(a) from t group by b", "", "")
	require.NoError(t, err)
	nodeW2 := resolve.NewNodeW(stmt2)
	p2, err := plannercore.BuildLogicalPlanForTest(context.Background(), ctx, nodeW2, is)
	require.NoError(t, err)
	logic2, ok := p2.(base.LogicalPlan)
	require.True(t, ok)
	group2 := memo.ExportedConvert2Group(logic2)
	group2.BuildKeyInfo()
	require.False(t, group2.Prop.MaxOneRow)
	require.Len(t, group2.Prop.Schema.PKOrUK, 1)

	// case 3: build key info for new Group
	newSel := logicalop.LogicalSelection{}.Init(ctx, 0)
	newExpr1 := memo.ExportedNewGroupExpr(newSel)
	newExpr1.SetChildren(group2)
	newGroup1 := memo.ExportedNewGroupWithSchema(newExpr1, group2.Prop.Schema)
	newGroup1.BuildKeyInfo()
	require.Len(t, newGroup1.Prop.Schema.PKOrUK, 1)

	// case 4: build maxOneRow for new Group
	newLimit := logicalop.LogicalLimit{Count: 1}.Init(ctx, 0)
	newExpr2 := memo.ExportedNewGroupExpr(newLimit)
	newExpr2.SetChildren(group2)
	newGroup2 := memo.ExportedNewGroupWithSchema(newExpr2, group2.Prop.Schema)
	newGroup2.BuildKeyInfo()
	require.True(t, newGroup2.Prop.MaxOneRow)
}

func RunExploreMark(t *testing.T) {
	mark := memo.ExportedExploreMark(0)
	require.False(t, mark.Explored(0))
	require.False(t, mark.Explored(1))

	mark.SetExplored(0)
	mark.SetExplored(1)
	require.True(t, mark.Explored(0))
	require.True(t, mark.Explored(1))

	mark.SetUnexplored(1)
	require.True(t, mark.Explored(0))
	require.False(t, mark.Explored(1))
}
