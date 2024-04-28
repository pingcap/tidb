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

package memo

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/model"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/pattern"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

func TestNewGroup(t *testing.T) {
	p := &plannercore.LogicalLimit{}
	expr := NewGroupExpr(p)
	g := NewGroupWithSchema(expr, expression.NewSchema())

	require.Equal(t, 1, g.Equivalents.Len())
	require.Equal(t, expr, g.Equivalents.Front().Value.(*GroupExpr))
	require.Len(t, g.Fingerprints, 1)
	require.False(t, g.Explored(0))
}

func TestGroupInsert(t *testing.T) {
	p := &plannercore.LogicalLimit{}
	expr := NewGroupExpr(p)
	g := NewGroupWithSchema(expr, expression.NewSchema())
	require.False(t, g.Insert(expr))
	expr.selfFingerprint = "1"
	require.True(t, g.Insert(expr))
}

func TestGroupDelete(t *testing.T) {
	p := &plannercore.LogicalLimit{}
	expr := NewGroupExpr(p)
	g := NewGroupWithSchema(expr, expression.NewSchema())
	require.Equal(t, 1, g.Equivalents.Len())

	g.Delete(expr)
	require.Equal(t, 0, g.Equivalents.Len())

	g.Delete(expr)
	require.Equal(t, 0, g.Equivalents.Len())
}

func TestGroupDeleteAll(t *testing.T) {
	ctx := plannercore.MockContext()
	defer func() {
		do := domain.GetDomain(ctx)
		do.StatsHandle().Close()
	}()
	expr := NewGroupExpr(plannercore.LogicalSelection{}.Init(ctx, 0))
	g := NewGroupWithSchema(expr, expression.NewSchema())
	require.True(t, g.Insert(NewGroupExpr(plannercore.LogicalLimit{}.Init(ctx, 0))))
	require.True(t, g.Insert(NewGroupExpr(plannercore.LogicalProjection{}.Init(ctx, 0))))
	require.Equal(t, 3, g.Equivalents.Len())
	require.NotNil(t, g.GetFirstElem(pattern.OperandProjection))
	require.True(t, g.Exists(expr))

	g.DeleteAll()
	require.Equal(t, 0, g.Equivalents.Len())
	require.Nil(t, g.GetFirstElem(pattern.OperandProjection))
	require.False(t, g.Exists(expr))
}

func TestGroupExists(t *testing.T) {
	p := &plannercore.LogicalLimit{}
	expr := NewGroupExpr(p)
	g := NewGroupWithSchema(expr, expression.NewSchema())
	require.True(t, g.Exists(expr))

	g.Delete(expr)
	require.False(t, g.Exists(expr))
}

func TestGroupFingerPrint(t *testing.T) {
	variable.EnableMDL.Store(false)
	p := parser.New()
	stmt1, err := p.ParseOneStmt("select * from t where a > 1 and a < 100", "", "")
	require.NoError(t, err)

	is := infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockSignedTable()})
	ctx := plannercore.MockContext()
	defer func() {
		do := domain.GetDomain(ctx)
		do.StatsHandle().Close()
	}()
	plan, err := plannercore.BuildLogicalPlanForTest(context.Background(), ctx, stmt1, is)
	require.NoError(t, err)
	logic1, ok := plan.(base.LogicalPlan)
	require.True(t, ok)

	// Plan tree should be: DataSource -> Selection -> Projection
	proj, ok := logic1.(*plannercore.LogicalProjection)
	require.True(t, ok)
	sel, ok := logic1.Children()[0].(*plannercore.LogicalSelection)
	require.True(t, ok)
	group1 := Convert2Group(logic1)
	oldGroupExpr := group1.Equivalents.Front().Value.(*GroupExpr)

	// Insert a GroupExpr with the same ExprNode.
	newGroupExpr := NewGroupExpr(proj)
	newGroupExpr.SetChildren(oldGroupExpr.Children[0])
	group1.Insert(newGroupExpr)
	require.Equal(t, 1, group1.Equivalents.Len())

	// Insert a GroupExpr with different children。
	newGroupExpr2 := NewGroupExpr(proj)
	newGroup := NewGroupWithSchema(oldGroupExpr, group1.Prop.Schema)
	newGroupExpr2.SetChildren(newGroup)
	group1.Insert(newGroupExpr2)
	require.Equal(t, 2, group1.Equivalents.Len())

	// Insert a GroupExpr with different ExprNode.
	limit := plannercore.LogicalLimit{}.Init(proj.SCtx(), 0)
	newGroupExpr3 := NewGroupExpr(limit)
	newGroupExpr3.SetChildren(oldGroupExpr.Children[0])
	group1.Insert(newGroupExpr3)
	require.Equal(t, 3, group1.Equivalents.Len())

	// Insert two LogicalSelections with same conditions but different order.
	require.Len(t, sel.Conditions, 2)
	newSelection := plannercore.LogicalSelection{
		Conditions: make([]expression.Expression, 2)}.Init(sel.SCtx(), sel.QueryBlockOffset())
	newSelection.Conditions[0], newSelection.Conditions[1] = sel.Conditions[1], sel.Conditions[0]
	newGroupExpr4 := NewGroupExpr(sel)
	newGroupExpr5 := NewGroupExpr(newSelection)
	newGroupExpr4.SetChildren(oldGroupExpr.Children[0])
	newGroupExpr5.SetChildren(oldGroupExpr.Children[0])
	group1.Insert(newGroupExpr4)
	require.Equal(t, 4, group1.Equivalents.Len())
	group1.Insert(newGroupExpr5)
	require.Equal(t, 4, group1.Equivalents.Len())
}

func TestGroupGetFirstElem(t *testing.T) {
	ctx := plannercore.MockContext()
	defer func() {
		do := domain.GetDomain(ctx)
		do.StatsHandle().Close()
	}()
	expr0 := NewGroupExpr(plannercore.LogicalProjection{}.Init(ctx, 0))
	expr1 := NewGroupExpr(plannercore.LogicalLimit{}.Init(ctx, 0))
	expr2 := NewGroupExpr(plannercore.LogicalProjection{}.Init(ctx, 0))
	expr3 := NewGroupExpr(plannercore.LogicalLimit{}.Init(ctx, 0))
	expr4 := NewGroupExpr(plannercore.LogicalProjection{}.Init(ctx, 0))

	g := NewGroupWithSchema(expr0, expression.NewSchema())
	g.Insert(expr1)
	g.Insert(expr2)
	g.Insert(expr3)
	g.Insert(expr4)

	require.Equal(t, expr0, g.GetFirstElem(pattern.OperandProjection).Value.(*GroupExpr))
	require.Equal(t, expr1, g.GetFirstElem(pattern.OperandLimit).Value.(*GroupExpr))
	require.Equal(t, expr0, g.GetFirstElem(pattern.OperandAny).Value.(*GroupExpr))
}

type fakeImpl struct {
	plan base.PhysicalPlan
}

func (impl *fakeImpl) CalcCost(float64, ...Implementation) float64     { return 0 }
func (impl *fakeImpl) SetCost(float64)                                 {}
func (impl *fakeImpl) GetCost() float64                                { return 0 }
func (impl *fakeImpl) GetPlan() base.PhysicalPlan                      { return impl.plan }
func (impl *fakeImpl) AttachChildren(...Implementation) Implementation { return nil }
func (impl *fakeImpl) GetCostLimit(float64, ...Implementation) float64 { return 0 }

func TestGetInsertGroupImpl(t *testing.T) {
	ctx := plannercore.MockContext()
	g := NewGroupWithSchema(NewGroupExpr(plannercore.LogicalLimit{}.Init(ctx, 0)), expression.NewSchema())
	defer func() {
		do := domain.GetDomain(ctx)
		do.StatsHandle().Close()
	}()
	emptyProp := &property.PhysicalProperty{}
	require.Nil(t, g.GetImpl(emptyProp))

	impl := &fakeImpl{plan: &plannercore.PhysicalLimit{}}
	g.InsertImpl(emptyProp, impl)
	require.Equal(t, impl, g.GetImpl(emptyProp))

	orderProp := &property.PhysicalProperty{SortItems: []property.SortItem{{Col: &expression.Column{}}}}
	require.Nil(t, g.GetImpl(orderProp))
}

func TestFirstElemAfterDelete(t *testing.T) {
	ctx := plannercore.MockContext()
	defer func() {
		do := domain.GetDomain(ctx)
		do.StatsHandle().Close()
	}()
	oldExpr := NewGroupExpr(plannercore.LogicalLimit{Count: 10}.Init(ctx, 0))
	g := NewGroupWithSchema(oldExpr, expression.NewSchema())
	newExpr := NewGroupExpr(plannercore.LogicalLimit{Count: 20}.Init(ctx, 0))
	g.Insert(newExpr)
	require.NotNil(t, g.GetFirstElem(pattern.OperandLimit))
	require.Equal(t, oldExpr, g.GetFirstElem(pattern.OperandLimit).Value)
	g.Delete(oldExpr)
	require.NotNil(t, g.GetFirstElem(pattern.OperandLimit))
	require.Equal(t, newExpr, g.GetFirstElem(pattern.OperandLimit).Value)
	g.Delete(newExpr)
	require.Nil(t, g.GetFirstElem(pattern.OperandLimit))
}

func TestBuildKeyInfo(t *testing.T) {
	variable.EnableMDL.Store(false)
	p := parser.New()
	ctx := plannercore.MockContext()
	defer func() {
		do := domain.GetDomain(ctx)
		do.StatsHandle().Close()
	}()
	is := infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockSignedTable()})
	domain.GetDomain(ctx).MockInfoCacheAndLoadInfoSchema(is)

	// case 1: primary key has constant constraint
	stmt1, err := p.ParseOneStmt("select a from t where a = 10", "", "")
	require.NoError(t, err)
	p1, err := plannercore.BuildLogicalPlanForTest(context.Background(), ctx, stmt1, is)
	require.NoError(t, err)
	logic1, ok := p1.(base.LogicalPlan)
	require.True(t, ok)
	group1 := Convert2Group(logic1)
	group1.BuildKeyInfo()
	require.True(t, group1.Prop.MaxOneRow)
	require.Len(t, group1.Prop.Schema.Keys, 1)

	// case 2: group by column is key
	stmt2, err := p.ParseOneStmt("select b, sum(a) from t group by b", "", "")
	require.NoError(t, err)
	p2, err := plannercore.BuildLogicalPlanForTest(context.Background(), ctx, stmt2, is)
	require.NoError(t, err)
	logic2, ok := p2.(base.LogicalPlan)
	require.True(t, ok)
	group2 := Convert2Group(logic2)
	group2.BuildKeyInfo()
	require.False(t, group2.Prop.MaxOneRow)
	require.Len(t, group2.Prop.Schema.Keys, 1)

	// case 3: build key info for new Group
	newSel := plannercore.LogicalSelection{}.Init(ctx, 0)
	newExpr1 := NewGroupExpr(newSel)
	newExpr1.SetChildren(group2)
	newGroup1 := NewGroupWithSchema(newExpr1, group2.Prop.Schema)
	newGroup1.BuildKeyInfo()
	require.Len(t, newGroup1.Prop.Schema.Keys, 1)

	// case 4: build maxOneRow for new Group
	newLimit := plannercore.LogicalLimit{Count: 1}.Init(ctx, 0)
	newExpr2 := NewGroupExpr(newLimit)
	newExpr2.SetChildren(group2)
	newGroup2 := NewGroupWithSchema(newExpr2, group2.Prop.Schema)
	newGroup2.BuildKeyInfo()
	require.True(t, newGroup2.Prop.MaxOneRow)
}

func TestExploreMark(t *testing.T) {
	mark := ExploreMark(0)
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
