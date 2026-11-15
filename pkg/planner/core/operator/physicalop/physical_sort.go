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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
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
	for i := 0; i < len(items); i++ {
		item := items[i]
		if col, nullsHigh, ok := matchIsNullPair(items, i); ok {
			propItems = append(propItems, property.SortItem{Col: col, Desc: items[i+1].Desc, NullsHigh: nullsHigh})
			i++
			continue
		}
		switch expr := item.Expr.(type) {
		case *expression.Column:
			propItems = append(propItems, property.SortItem{Col: expr, Desc: item.Desc})
		case *expression.ScalarFunction:
			col, desc := expr.GetSingleColumn(item.Desc)
			if col == nil {
				return nil, false, false
			}
			// Reversing value order does not reverse NULL order: -NULL is still
			// NULL. An opposite index scan cannot satisfy this sort for nullable columns.
			if desc != item.Desc && !mysql.HasNotNullFlag(col.RetType.GetFlag()) {
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
// The items are converted the same way as GetPropByOrderByItems, so an
// `ISNULL(col), col` pair matches a single NullsHigh (or plain) sort item on col.
// Only the prefix that p requires is converted, so later items that are not
// columns do not prevent a match.
func MatchItems(p *property.PhysicalProperty, items []*util.ByItems) bool {
	i := 0
	for _, propItem := range p.SortItems {
		if i >= len(items) {
			return false
		}
		var item property.SortItem
		if col, nullsHigh, ok := matchIsNullPair(items, i); ok {
			item = property.SortItem{Col: col, Desc: items[i+1].Desc, NullsHigh: nullsHigh}
			i += 2
		} else {
			col, ok := items[i].Expr.(*expression.Column)
			if !ok {
				return false
			}
			item = property.SortItem{Col: col, Desc: items[i].Desc}
			i++
		}
		if item.Desc != propItem.Desc || item.NullsHigh != propItem.NullsHigh || !item.Col.EqualColumn(propItem.Col) {
			return false
		}
	}
	return true
}

// GetPropByOrderByItems will check if this sort property can be pushed or not. In order to simplify the problem, we only
// consider the case that all expression are columns.
func GetPropByOrderByItems(items []*util.ByItems) (*property.PhysicalProperty, bool) {
	propItems := make([]property.SortItem, 0, len(items))
	for i := 0; i < len(items); i++ {
		item := items[i]
		if col, nullsHigh, ok := matchIsNullPair(items, i); ok {
			propItems = append(propItems, property.SortItem{Col: col, Desc: items[i+1].Desc, NullsHigh: nullsHigh})
			i++
			continue
		}
		col, ok := item.Expr.(*expression.Column)
		if !ok {
			return nil, false
		}
		propItems = append(propItems, property.SortItem{Col: col, Desc: item.Desc})
	}
	return &property.PhysicalProperty{SortItems: propItems}, true
}

// matchIsNullPair checks whether items[i] and items[i+1] are a NULL indicator
// of col followed by col, such as `ISNULL(col), col` or
// `CASE WHEN col IS NULL THEN 1 ELSE 0 END, col`, the usual ways to write
// NULLS LAST in MySQL (see nullIndicator). The pair
// orders exactly like col alone, except that NULLs sort high when the indicator
// puts NULLs in the same place as a NULLs-high order of col would.
func matchIsNullPair(items []*util.ByItems, i int) (col *expression.Column, nullsHigh, ok bool) {
	if i+1 >= len(items) {
		return nil, false, false
	}
	col, highForNull, ok := nullIndicator(items[i].Expr)
	if !ok || !col.EqualColumn(items[i+1].Expr) {
		return nil, false, false
	}
	// NULLs come last iff the indicator gives them the higher key in an ASC item,
	// or the lower key in a DESC item. A NULLs-high order of col puts NULLs last
	// iff col is ASC.
	nullsLast := highForNull != items[i].Desc
	return col, nullsLast != items[i+1].Desc, true
}

// nullIndicator checks whether expr takes exactly two distinct values, one for
// the rows where col is NULL and another for the rest. highForNull reports
// whether the value for NULL rows is the higher one in sort order. It accepts:
//   - ISNULL(col) and NOT(ISNULL(col)), that is col IS [NOT] NULL;
//   - CASE WHEN c THEN x [ELSE y] END and IF(c, x, y), where c is one of the
//     above and x, y are distinct literals. A missing ELSE is NULL, which sorts
//     lowest.
func nullIndicator(expr expression.Expression) (col *expression.Column, highForNull, ok bool) {
	if col, trueForNull, ok := nullTest(expr); ok {
		// TRUE (1) sorts higher than FALSE (0).
		return col, trueForNull, true
	}
	sf, isFunc := expr.(*expression.ScalarFunction)
	if !isFunc || (sf.FuncName.L != ast.Case && sf.FuncName.L != ast.If) {
		return nil, false, false
	}
	args := sf.GetArgs()
	if len(args) != 3 && (sf.FuncName.L == ast.If || len(args) != 2) {
		return nil, false, false
	}
	col, condTrueForNull, ok := nullTest(args[0])
	if !ok {
		return nil, false, false
	}
	thenVal, ok := literalForOrder(args[1], sf.RetType)
	if !ok {
		return nil, false, false
	}
	var elseVal types.Datum // NULL when there is no ELSE.
	if len(args) == 3 {
		if elseVal, ok = literalForOrder(args[2], sf.RetType); !ok {
			return nil, false, false
		}
	}
	cmp, err := thenVal.Compare(types.DefaultStmtNoWarningContext, &elseVal, collate.GetCollator(sf.RetType.GetCollate()))
	if err != nil || cmp == 0 {
		return nil, false, false
	}
	// The NULL rows get the THEN value when the condition is true for them.
	return col, (cmp > 0) == condTrueForNull, true
}

// nullTest checks whether expr is `col IS NULL` or `col IS NOT NULL`, and
// reports whether it is true for the rows where col is NULL.
func nullTest(expr expression.Expression) (col *expression.Column, trueForNull, ok bool) {
	sf, isFunc := expr.(*expression.ScalarFunction)
	if !isFunc {
		return nil, false, false
	}
	trueForNull = true
	if sf.FuncName.L == ast.UnaryNot {
		if sf, isFunc = sf.GetArgs()[0].(*expression.ScalarFunction); !isFunc {
			return nil, false, false
		}
		trueForNull = false
	}
	if sf.FuncName.L != ast.IsNull {
		return nil, false, false
	}
	col, ok = sf.GetArgs()[0].(*expression.Column)
	return col, trueForNull, ok
}

// literalForOrder returns the value of expr when it is a literal of the same
// evaluation type as the CASE/IF result, so that comparing the two branch
// values gives the order the result is sorted in. Parameters of prepared
// statements are rejected, so a cached plan does not depend on their values.
func literalForOrder(expr expression.Expression, retType *types.FieldType) (types.Datum, bool) {
	c, ok := expr.(*expression.Constant)
	if !ok || c.ParamMarker != nil || c.DeferredExpr != nil {
		return types.Datum{}, false
	}
	if c.Value.IsNull() {
		return c.Value, true
	}
	switch et := retType.EvalType(); et {
	case types.ETInt, types.ETReal, types.ETDecimal, types.ETString:
		return c.Value, c.RetType.EvalType() == et
	}
	return types.Datum{}, false
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
