// Copyright 2025 PingCAP, Inc.
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

package physicalop

import (
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
)

// PhysicalExpand is used to expand underlying data sources to feed different grouping sets.
type PhysicalExpand struct {
	// data after repeat-OP will generate a new grouping-ID column to indicate what grouping set is it for.
	PhysicalSchemaProducer

	// generated grouping ID column itself.
	GroupingIDCol *expression.Column

	// GroupingSets is used to define what kind of group layout should the underlying data follow.
	// For simple case: select count(distinct a), count(distinct b) from t; the grouping expressions are [a] and [b].
	GroupingSets expression.GroupingSets

	// The level projections is generated from grouping sets，make execution more clearly.
	LevelExprs [][]expression.Expression

	// The generated column names. Eg: "grouping_id" and so on.
	ExtraGroupingColNames []string
}

// Init only assigns type and context.
func (p PhysicalExpand) Init(ctx base.PlanContext, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalExpand {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeExpand, &p, offset)
	p.SetChildrenReqProps(props)
	p.SetStats(stats)
	return &p
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalExpand) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	if len(p.LevelExprs) > 0 {
		return p.cloneV2(newCtx)
	}
	np := new(PhysicalExpand)
	np.SetSCtx(newCtx)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, np)
	if err != nil {
		return nil, errors.Trace(err)
	}
	np.PhysicalSchemaProducer = *base
	// clone ID cols.
	np.GroupingIDCol = p.GroupingIDCol.Clone().(*expression.Column)

	// clone grouping expressions.
	clonedGroupingSets := make([]expression.GroupingSet, 0, len(p.GroupingSets))
	for _, one := range p.GroupingSets {
		clonedGroupingSets = append(clonedGroupingSets, one.Clone())
	}
	np.GroupingSets = clonedGroupingSets
	return np, nil
}

func (p *PhysicalExpand) cloneV2(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	np := new(PhysicalExpand)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, np)
	if err != nil {
		return nil, errors.Trace(err)
	}
	np.PhysicalSchemaProducer = *base
	// clone level projection expressions.
	for _, oneLevelProjExprs := range p.LevelExprs {
		np.LevelExprs = append(np.LevelExprs, util.CloneExprs(oneLevelProjExprs))
	}

	// clone generated column names.
	for _, name := range p.ExtraGroupingColNames {
		np.ExtraGroupingColNames = append(np.ExtraGroupingColNames, strings.Clone(name))
	}
	return np, nil
}

// MemoryUsage return the memory usage of PhysicalExpand
func (p *PhysicalExpand) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + size.SizeOfSlice + int64(cap(p.GroupingSets))*size.SizeOfPointer
	for _, gs := range p.GroupingSets {
		sum += gs.MemoryUsage()
	}
	sum += p.GroupingIDCol.MemoryUsage()
	return
}

func (p *PhysicalExpand) explainInfoV2() string {
	sb := strings.Builder{}
	evalCtx := p.SCtx().GetExprCtx().GetEvalCtx()
	enableRedactLog := p.SCtx().GetSessionVars().EnableRedactLog
	for i, oneL := range p.LevelExprs {
		if i == 0 {
			sb.WriteString("level-projection:")
			sb.WriteString("[")
			sb.WriteString(expression.ExplainExpressionList(evalCtx, oneL, p.Schema(), enableRedactLog))
			sb.WriteString("]")
		} else {
			sb.WriteString(",[")
			sb.WriteString(expression.ExplainExpressionList(evalCtx, oneL, p.Schema(), enableRedactLog))
			sb.WriteString("]")
		}
	}
	sb.WriteString("; schema: [")
	colStrs := make([]string, 0, len(p.Schema().Columns))
	for _, col := range p.Schema().Columns {
		colStrs = append(colStrs, col.StringWithCtx(evalCtx, errors.RedactLogDisable))
	}
	sb.WriteString(strings.Join(colStrs, ","))
	sb.WriteString("]")
	return sb.String()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalExpand) ExplainInfo() string {
	ectx := p.SCtx().GetExprCtx().GetEvalCtx()
	if len(p.LevelExprs) > 0 {
		return p.explainInfoV2()
	}
	var str strings.Builder
	str.WriteString("group set num:")
	str.WriteString(strconv.FormatInt(int64(len(p.GroupingSets)), 10))
	if p.GroupingIDCol != nil {
		str.WriteString(", groupingID:")
		str.WriteString(p.GroupingIDCol.StringWithCtx(ectx, errors.RedactLogDisable))
		str.WriteString(", ")
	}
	str.WriteString(p.GroupingSets.StringWithCtx(ectx, errors.RedactLogDisable))
	return str.String()
}

// ResolveIndicesItself resolve indices for PhysicalPlan itself
func (p *PhysicalExpand) ResolveIndicesItself() (err error) {
	// for version 1
	for _, gs := range p.GroupingSets {
		for _, groupingExprs := range gs {
			for k, groupingExpr := range groupingExprs {
				gExpr, err := groupingExpr.ResolveIndices(p.Children()[0].Schema())
				if err != nil {
					return err
				}
				groupingExprs[k] = gExpr
			}
		}
	}
	// for version 2
	for i, oneLevel := range p.LevelExprs {
		for j, expr := range oneLevel {
			// expr in expand level-projections only contains column ref and literal constant projection.
			p.LevelExprs[i][j], err = expr.ResolveIndices(p.Children()[0].Schema())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ResolveIndices implements Plan interface.
func (p *PhysicalExpand) ResolveIndices() (err error) {
	err = p.PhysicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	return p.ResolveIndicesItself()
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalExpand) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	if len(p.LevelExprs) > 0 {
		return p.toPBV2(ctx, storeType)
	}
	client := ctx.GetClient()
	groupingSetsPB, err := p.GroupingSets.ToPB(ctx.GetExprCtx().GetEvalCtx(), client)
	if err != nil {
		return nil, err
	}
	expand := &tipb.Expand{
		GroupingSets: groupingSetsPB,
	}
	executorID := ""
	if storeType == kv.TiFlash {
		var err error
		expand.Child, err = p.Children()[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		executorID = p.ExplainID().String()
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeExpand, Expand: expand, ExecutorId: &executorID}, nil
}

func (p *PhysicalExpand) toPBV2(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	client := ctx.GetClient()
	projExprsPB := make([]*tipb.ExprSlice, 0, len(p.LevelExprs))
	evalCtx := ctx.GetExprCtx().GetEvalCtx()
	for _, exprs := range p.LevelExprs {
		expressionsPB, err := expression.ExpressionsToPBList(evalCtx, exprs, client)
		if err != nil {
			return nil, err
		}
		projExprsPB = append(projExprsPB, &tipb.ExprSlice{Exprs: expressionsPB})
	}
	expand2 := &tipb.Expand2{
		ProjExprs:            projExprsPB,
		GeneratedOutputNames: p.ExtraGroupingColNames,
	}
	executorID := ""
	if storeType == kv.TiFlash {
		var err error
		expand2.Child, err = p.Children()[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		executorID = p.ExplainID().String()
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeExpand2, Expand2: expand2, ExecutorId: &executorID}, nil
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalExpand) Attach2Task(tasks ...base.Task) base.Task {
	// for cycle import dependency, we need to ensure that the PhysicalExpand is attached to the task
	return utilfuncp.Attach2Task4PhysicalExpand(p, tasks...)
}

// ExhaustPhysicalPlans4LogicalExpand generates PhysicalExpand plan from LogicalExpand.
func ExhaustPhysicalPlans4LogicalExpand(p *logicalop.LogicalExpand, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	// under the mpp task type, if the sort item is not empty, refuse it, cause expanded data doesn't support any sort items.
	if !prop.IsSortItemEmpty() {
		// false, meaning we can add a sort enforcer.
		return nil, false, nil
	}
	// when TiDB Expand execution is introduced: we can deal with two kind of physical plans.
	// RootTaskType means expand should be run at TiDB node.
	//	(RootTaskType is the default option, we can also generate a mpp candidate for it)
	// MPPTaskType means expand should be run at TiFlash node.
	if prop.TaskTp != property.RootTaskType && prop.TaskTp != property.MppTaskType {
		return nil, true, nil
	}
	// now Expand mode can only be executed on TiFlash node.
	// Upper layer shouldn't expect any mpp partition from an Expand operator.
	// todo: data output from Expand operator should keep the origin data mpp partition.
	if prop.TaskTp == property.MppTaskType && prop.MPPPartitionTp != property.AnyType {
		return nil, true, nil
	}
	var physicalExpands []base.PhysicalPlan
	// for property.RootTaskType and property.MppTaskType with no partition option, we can give an MPP Expand.
	// we just remove whether subtree can be pushed to tiFlash check, and left child handle itself.
	if p.SCtx().GetSessionVars().IsMPPAllowed() {
		mppProp := prop.CloneEssentialFields()
		mppProp.TaskTp = property.MppTaskType
		expand := PhysicalExpand{
			GroupingSets:          p.RollupGroupingSets,
			LevelExprs:            p.LevelExprs,
			ExtraGroupingColNames: p.ExtraGroupingColNames,
		}.Init(p.SCtx(), p.StatsInfo().ScaleByExpectCnt(p.SCtx().GetSessionVars(), prop.ExpectedCnt), p.QueryBlockOffset(), mppProp)
		expand.SetSchema(p.Schema())
		physicalExpands = append(physicalExpands, expand)
		// when the MppTaskType is required, we can return the physical plan directly.
		if prop.TaskTp == property.MppTaskType {
			return physicalExpands, true, nil
		}
	}
	// for property.RootTaskType, we can give a TiDB Expand.
	{
		taskTypes := []property.TaskType{property.CopSingleReadTaskType, property.CopMultiReadTaskType, property.MppTaskType, property.RootTaskType}
		for _, taskType := range taskTypes {
			// require cop task type for children.F
			tidbProp := prop.CloneEssentialFields()
			tidbProp.TaskTp = taskType
			expand := PhysicalExpand{
				GroupingSets:          p.RollupGroupingSets,
				LevelExprs:            p.LevelExprs,
				ExtraGroupingColNames: p.ExtraGroupingColNames,
			}.Init(p.SCtx(), p.StatsInfo().ScaleByExpectCnt(p.SCtx().GetSessionVars(), prop.ExpectedCnt), p.QueryBlockOffset(), tidbProp)
			expand.SetSchema(p.Schema())
			physicalExpands = append(physicalExpands, expand)
		}
	}
	return physicalExpands, true, nil
}
