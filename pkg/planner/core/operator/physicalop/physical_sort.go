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
	"bytes"
	"fmt"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
	sliceutil "github.com/pingcap/tidb/pkg/util/slice"
	"github.com/pingcap/tipb/go-tipb"
)

// PhysicalSort is the physical operator of sort, which implements a memory sort.
type PhysicalSort struct {
	BasePhysicalPlan

	ByItems []*util.ByItems
	// whether this operator only need to sort the data of one partition.
	// it is true only if it is used to sort the sharded data of the window function.
	IsPartialSort bool
}

// Init initializes PhysicalSort.
func (p PhysicalSort) Init(ctx base.PlanContext, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalSort {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeSort, &p, offset)
	p.SetChildrenReqProps(props)
	p.SetStats(stats)
	return &p
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalSort) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalSort)
	cloned.SetSCtx(newCtx)
	cloned.IsPartialSort = p.IsPartialSort
	base, err := p.BasePhysicalPlan.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.BasePhysicalPlan = *base
	for _, it := range p.ByItems {
		cloned.ByItems = append(cloned.ByItems, it.Clone())
	}
	return cloned, nil
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *PhysicalSort) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.ByItems))
	for _, item := range p.ByItems {
		corCols = append(corCols, expression.ExtractCorColumns(item.Expr)...)
	}
	return corCols
}

// MemoryUsage return the memory usage of PhysicalSort
func (p *PhysicalSort) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.BasePhysicalPlan.MemoryUsage() + size.SizeOfSlice + int64(cap(p.ByItems))*size.SizeOfPointer +
		size.SizeOfBool
	for _, byItem := range p.ByItems {
		sum += byItem.MemoryUsage()
	}
	return
}

// ExplainInfo implements Plan interface.
func (p *PhysicalSort) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	buffer = util.ExplainByItems(p.SCtx().GetExprCtx().GetEvalCtx(), buffer, p.ByItems)
	if p.TiFlashFineGrainedShuffleStreamCount > 0 {
		fmt.Fprintf(buffer, ", stream_count: %d", p.TiFlashFineGrainedShuffleStreamCount)
	}
	return buffer.String()
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalSort) Attach2Task(tasks ...base.Task) base.Task {
	return utilfuncp.Attach2Task4PhysicalSort(p, tasks...)
}

// GetCost calculates the cost of the PhysicalSort operation based on the input count and schema.
func (p *PhysicalSort) GetCost(count float64, schema *expression.Schema) float64 {
	return utilfuncp.GetCost4PhysicalSort(p, count, schema)
}

// GetPlanCostVer1 calculates the cost of the PhysicalSort plan for a given task type and options.
func (p *PhysicalSort) GetPlanCostVer1(taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer14PhysicalSort(p, taskType, option)
}

// GetPlanCostVer2 calculates the cost of the PhysicalSort plan for a given task type and options, returning a CostVer2 object.
func (p *PhysicalSort) GetPlanCostVer2(taskType property.TaskType, option *costusage.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetPlanCostVer24PhysicalSort(p, taskType, option, isChildOfINL...)
}

// ToPB converts the PhysicalSort operator to a Protocol Buffer representation.
func (p *PhysicalSort) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	if !p.IsPartialSort {
		return nil, errors.Errorf("sort %s can't convert to pb, because it isn't a partial sort", p.Plan.ExplainID())
	}

	client := ctx.GetClient()

	sortExec := &tipb.Sort{}
	for _, item := range p.ByItems {
		sortExec.ByItems = append(sortExec.ByItems, expression.SortByItemToPB(ctx.GetExprCtx().GetEvalCtx(), client, item.Expr, item.Desc))
	}
	isPartialSort := p.IsPartialSort
	sortExec.IsPartialSort = &isPartialSort

	var err error
	sortExec.Child, err = p.Children()[0].ToPB(ctx, storeType)
	if err != nil {
		return nil, errors.Trace(err)
	}
	executorID := p.ExplainID().String()
	return &tipb.Executor{
		Tp:                            tipb.ExecType_TypeSort,
		Sort:                          sortExec,
		ExecutorId:                    &executorID,
		FineGrainedShuffleStreamCount: p.TiFlashFineGrainedShuffleStreamCount,
		FineGrainedShuffleBatchSize:   ctx.TiFlashFineGrainedShuffleBatchSize,
	}, nil
}

// ResolveIndices implements Plan interface.
func (p *PhysicalSort) ResolveIndices() (err error) {
	return resolveIndicesForSort(&p.BasePhysicalPlan)
}

// CloneForPlanCache implements the base.Plan interface.
// todo: after all physical are moved, this function can be generated by codegen.
func (p *PhysicalSort) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalSort)
	*cloned = *p
	basePlan, baseOK := p.BasePhysicalPlan.CloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.BasePhysicalPlan = *basePlan
	cloned.ByItems = sliceutil.DeepClone(p.ByItems)
	return cloned, true
}

// ExhaustPhysicalPlans4LogicalSort exhausts all possible physical plans for a LogicalSort operator
func ExhaustPhysicalPlans4LogicalSort(ls *logicalop.LogicalSort, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	switch prop.TaskTp {
	case property.RootTaskType:
		if MatchItems(prop, ls.ByItems) {
			ret := make([]base.PhysicalPlan, 0, 2)
			ret = append(ret, getPhysicalSort(ls, prop))
			ns := getNominalSort(ls, prop)
			if ns != nil {
				ret = append(ret, ns)
			}
			return ret, true, nil
		}
	case property.MppTaskType:
		// just enumerate mpp task type requirement for child.
		ps := getNominalSortSimple(ls, prop)
		if ps != nil {
			return []base.PhysicalPlan{ps}, true, nil
		}
	default:
		return nil, true, nil
	}
	return nil, true, nil
}

func getPhysicalSort(ls *logicalop.LogicalSort, prop *property.PhysicalProperty) base.PhysicalPlan {
	ps := PhysicalSort{ByItems: ls.ByItems}.Init(ls.SCtx(), ls.StatsInfo().ScaleByExpectCnt(ls.SCtx().GetSessionVars(), prop.ExpectedCnt), ls.QueryBlockOffset(),
		&property.PhysicalProperty{TaskTp: prop.TaskTp, ExpectedCnt: math.MaxFloat64,
			CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: prop.NoCopPushDown})
	return ps
}

func getNominalSort(ls *logicalop.LogicalSort, reqProp *property.PhysicalProperty) *NominalSort {
	prop, canPass, onlyColumn := GetPropByOrderByItemsContainScalarFunc(ls.ByItems)
	if !canPass {
		return nil
	}
	prop.ExpectedCnt = reqProp.ExpectedCnt
	prop.NoCopPushDown = reqProp.NoCopPushDown
	ps := NominalSort{OnlyColumn: onlyColumn, ByItems: ls.ByItems}.Init(
		ls.SCtx(), ls.StatsInfo().ScaleByExpectCnt(ls.SCtx().GetSessionVars(), prop.ExpectedCnt), ls.QueryBlockOffset(), prop)
	return ps
}

// GetPropByOrderByItemsContainScalarFunc will check if this sort property can be pushed or not. In order to simplify the
// problem, we only consider the case that all expression are columns or some special scalar functions.
func GetPropByOrderByItemsContainScalarFunc(items []*util.ByItems) (_ *property.PhysicalProperty, _, _ bool) {
	propItems := make([]property.SortItem, 0, len(items))
	onlyColumn := true
	for _, item := range items {
		switch expr := item.Expr.(type) {
		case *expression.Column:
			propItems = append(propItems, property.SortItem{Col: expr, Desc: item.Desc})
		case *expression.ScalarFunction:
			col, desc := expr.GetSingleColumn(item.Desc)
			if col == nil {
				return nil, false, false
			}
			propItems = append(propItems, property.SortItem{Col: col, Desc: desc})
			onlyColumn = false
		default:
			return nil, false, false
		}
	}
	return &property.PhysicalProperty{SortItems: propItems}, true, onlyColumn
}

func getNominalSortSimple(ls *logicalop.LogicalSort, reqProp *property.PhysicalProperty) *NominalSort {
	prop, canPass, onlyColumn := GetPropByOrderByItemsContainScalarFunc(ls.ByItems)
	if !canPass || !onlyColumn {
		return nil
	}
	newProp := reqProp.CloneEssentialFields()
	newProp.SortItems = prop.SortItems
	ps := NominalSort{OnlyColumn: true, ByItems: ls.ByItems}.Init(
		ls.SCtx(), ls.StatsInfo().ScaleByExpectCnt(ls.SCtx().GetSessionVars(), reqProp.ExpectedCnt), ls.QueryBlockOffset(), newProp)
	return ps
}

// MatchItems checks if this prop's columns can match by items totally.
func MatchItems(p *property.PhysicalProperty, items []*util.ByItems) bool {
	if len(items) < len(p.SortItems) {
		return false
	}
	for i, col := range p.SortItems {
		sortItem := items[i]
		if sortItem.Desc != col.Desc || !col.Col.EqualColumn(sortItem.Expr) {
			return false
		}
	}
	return true
}

// GetPropByOrderByItems will check if this sort property can be pushed or not. In order to simplify the problem, we only
// consider the case that all expression are columns.
func GetPropByOrderByItems(items []*util.ByItems) (*property.PhysicalProperty, bool) {
	propItems := make([]property.SortItem, 0, len(items))
	for _, item := range items {
		col, ok := item.Expr.(*expression.Column)
		if !ok {
			return nil, false
		}
		propItems = append(propItems, property.SortItem{Col: col, Desc: item.Desc})
	}
	return &property.PhysicalProperty{SortItems: propItems}, true
}

// resolveIndicesForSort is a helper function to resolve indices for sort operators.
func resolveIndicesForSort(pp base.PhysicalPlan) (err error) {
	p := pp.(*BasePhysicalPlan)
	err = p.ResolveIndices()
	if err != nil {
		return err
	}

	var byItems []*util.ByItems
	switch x := p.Self.(type) {
	case *PhysicalSort:
		byItems = x.ByItems
	case *NominalSort:
		byItems = x.ByItems
	default:
		return errors.Errorf("expect PhysicalSort or NominalSort, but got %s", p.TP())
	}
	for _, item := range byItems {
		item.Expr, err = item.Expr.ResolveIndices(p.Children()[0].Schema())
		if err != nil {
			return err
		}
	}
	return err
}
