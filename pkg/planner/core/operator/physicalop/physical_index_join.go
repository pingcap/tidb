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
	"strings"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/ranger"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
	"github.com/pingcap/tidb/pkg/util/size"
	"go.uber.org/zap"
)

// PhysicalIndexJoin represents the plan of index look up join.
// NOTICE: When adding any member variables, remember to modify the Clone method.
type PhysicalIndexJoin struct {
	BasePhysicalJoin

	InnerPlan base.PhysicalPlan

	// Ranges stores the IndexRanges when the inner plan is index scan.
	Ranges ranger.MutableRanges
	// KeyOff2IdxOff maps the offsets in join key to the offsets in the index.
	KeyOff2IdxOff []int
	// IdxColLens stores the length of each index column.
	IdxColLens []int
	// CompareFilters stores the filters for last column if those filters need to be evaluated during execution.
	// e.g. select * from t, t1 where t.a = t1.a and t.b > t1.b and t.b < t1.b+10
	//      If there's index(t.a, t.b). All the filters can be used to construct index range but t.b > t1.b and t.b < t1.b+10
	//      need to be evaluated after we fetch the data of t1.
	// This struct stores them and evaluate them to ranges.
	CompareFilters *ColWithCmpFuncManager
	// OuterHashKeys indicates the outer keys used to build hash table during
	// execution. OuterJoinKeys is the prefix of OuterHashKeys.
	OuterHashKeys []*expression.Column
	// InnerHashKeys indicates the inner keys used to build hash table during
	// execution. InnerJoinKeys is the prefix of InnerHashKeys.
	InnerHashKeys []*expression.Column
	// EqualConditions stores the equal conditions for logical join's original EqualConditions.
	EqualConditions []*expression.ScalarFunction `plan-cache-clone:"shallow"`
}

// Init initializes PhysicalIndexJoin.
func (p PhysicalIndexJoin) Init(ctx base.PlanContext, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalIndexJoin {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeIndexJoin, &p, offset)
	p.SetChildrenReqProps(props)
	p.SetStats(stats)
	return &p
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalIndexJoin) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalIndexJoin)
	cloned.SetSCtx(newCtx)
	base, err := p.BasePhysicalJoin.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.BasePhysicalJoin = *base
	if p.InnerPlan != nil {
		cloned.InnerPlan, err = p.InnerPlan.Clone(newCtx)
		if err != nil {
			return nil, err
		}
	}
	cloned.Ranges = p.Ranges.CloneForPlanCache() // this clone is deep copy
	cloned.KeyOff2IdxOff = make([]int, len(p.KeyOff2IdxOff))
	copy(cloned.KeyOff2IdxOff, p.KeyOff2IdxOff)
	cloned.IdxColLens = make([]int, len(p.IdxColLens))
	copy(cloned.IdxColLens, p.IdxColLens)
	cloned.CompareFilters = p.CompareFilters.cloneForPlanCache()
	cloned.OuterHashKeys = util.CloneCols(p.OuterHashKeys)
	cloned.InnerHashKeys = util.CloneCols(p.InnerHashKeys)
	return cloned, nil
}

// MemoryUsage return the memory usage of PhysicalIndexJoin
func (p *PhysicalIndexJoin) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.BasePhysicalJoin.MemoryUsage() + size.SizeOfInterface*2 + size.SizeOfSlice*4 +
		int64(cap(p.KeyOff2IdxOff)+cap(p.IdxColLens))*size.SizeOfInt + size.SizeOfPointer
	if p.InnerPlan != nil {
		sum += p.InnerPlan.MemoryUsage()
	}
	if p.CompareFilters != nil {
		sum += p.CompareFilters.MemoryUsage()
	}

	for _, col := range p.OuterHashKeys {
		sum += col.MemoryUsage()
	}
	for _, col := range p.InnerHashKeys {
		sum += col.MemoryUsage()
	}
	return
}

// explainJoinLeftSide is to explain the left side of join.
func explainJoinLeftSide(buffer *strings.Builder, isInnerJoin bool, normalized bool, leftSide base.PhysicalPlan) {
	if !isInnerJoin {
		buffer.WriteString(", left side:")
		if normalized {
			buffer.WriteString(leftSide.TP())
		} else {
			buffer.WriteString(leftSide.ExplainID().String())
		}
	}
}

// ExplainInfoInternal is the internal function for explain.
func (p *PhysicalIndexJoin) ExplainInfoInternal(normalized bool, isIndexMergeJoin bool) string {
	sortedExplainExpressionList := expression.SortedExplainExpressionList
	if normalized {
		sortedExplainExpressionList = func(_ expression.EvalContext, exprs []expression.Expression) []byte {
			return expression.SortedExplainNormalizedExpressionList(exprs)
		}
	}

	exprCtx := p.SCtx().GetExprCtx()
	evalCtx := exprCtx.GetEvalCtx()
	buffer := new(strings.Builder)
	buffer.WriteString(p.JoinType.String())
	buffer.WriteString(", inner:")
	if normalized {
		buffer.WriteString(p.Children()[p.InnerChildIdx].TP())
	} else {
		buffer.WriteString(p.Children()[p.InnerChildIdx].ExplainID().String())
	}
	explainJoinLeftSide(buffer, p.JoinType.IsInnerJoin(), normalized, p.Children()[0])
	if len(p.OuterJoinKeys) > 0 {
		buffer.WriteString(", outer key:")
		buffer.Write(expression.ExplainColumnList(evalCtx, p.OuterJoinKeys))
	}
	if len(p.InnerJoinKeys) > 0 {
		buffer.WriteString(", inner key:")
		buffer.Write(expression.ExplainColumnList(evalCtx, p.InnerJoinKeys))
	}

	if len(p.OuterHashKeys) > 0 && !isIndexMergeJoin {
		exprs := make([]expression.Expression, 0, len(p.OuterHashKeys))
		for i := range p.OuterHashKeys {
			expr, err := expression.NewFunctionBase(exprCtx, ast.EQ, types.NewFieldType(mysql.TypeLonglong), p.OuterHashKeys[i], p.InnerHashKeys[i])
			if err != nil {
				logutil.BgLogger().Warn("fail to NewFunctionBase", zap.Error(err))
			}
			exprs = append(exprs, expr)
		}
		buffer.WriteString(", equal cond:")
		buffer.Write(sortedExplainExpressionList(evalCtx, exprs))
	}
	if len(p.LeftConditions) > 0 {
		buffer.WriteString(", left cond:")
		buffer.Write(sortedExplainExpressionList(evalCtx, p.LeftConditions))
	}
	if len(p.RightConditions) > 0 {
		buffer.WriteString(", right cond:")
		buffer.Write(sortedExplainExpressionList(evalCtx, p.RightConditions))
	}
	if len(p.OtherConditions) > 0 {
		buffer.WriteString(", other cond:")
		buffer.Write(sortedExplainExpressionList(evalCtx, p.OtherConditions))
	}
	return buffer.String()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalIndexJoin) ExplainNormalizedInfo() string {
	return p.ExplainInfoInternal(true, false)
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexJoin) ExplainInfo() string {
	return p.ExplainInfoInternal(false, false)
}

// GetCost computes the cost of index join operator and its children.
func (p *PhysicalIndexJoin) GetCost(outerCnt, innerCnt, outerCost, innerCost float64, costFlag uint64) float64 {
	return utilfuncp.GetCost4PhysicalIndexJoin(p, outerCnt, innerCnt, outerCost, innerCost, costFlag)
}

// GetPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexJoin) GetPlanCostVer1(taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer14PhysicalIndexJoin(p, taskType, option)
}

// ColWithCmpFuncManager is used in index join to handle the column with compare functions(>=, >, <, <=).
// It stores the compare functions and build ranges in execution phase.
type ColWithCmpFuncManager struct {
	TargetCol         *expression.Column
	ColLength         int
	OpType            []string
	opArg             []expression.Expression
	TmpConstant       []*expression.Constant
	AffectedColSchema *expression.Schema  `plan-cache-clone:"shallow"`
	compareFuncs      []chunk.CompareFunc `plan-cache-clone:"shallow"`
}

func (cwc *ColWithCmpFuncManager) cloneForPlanCache() *ColWithCmpFuncManager {
	if cwc == nil {
		return nil
	}
	cloned := new(ColWithCmpFuncManager)
	if cwc.TargetCol != nil {
		if cwc.TargetCol.SafeToShareAcrossSession() {
			cloned.TargetCol = cwc.TargetCol
		} else {
			cloned.TargetCol = cwc.TargetCol.Clone().(*expression.Column)
		}
	}
	cloned.ColLength = cwc.ColLength
	cloned.OpType = make([]string, len(cwc.OpType))
	copy(cloned.OpType, cwc.OpType)
	cloned.opArg = utilfuncp.CloneExpressionsForPlanCache(cwc.opArg, nil)
	cloned.TmpConstant = utilfuncp.CloneConstantsForPlanCache(cwc.TmpConstant, nil)
	cloned.AffectedColSchema = cwc.AffectedColSchema
	cloned.compareFuncs = cwc.compareFuncs
	return cloned
}

// AppendNewExpr appends a new expression to ColWithCmpFuncManager.
func (cwc *ColWithCmpFuncManager) AppendNewExpr(opName string, arg expression.Expression, affectedCols []*expression.Column) {
	cwc.OpType = append(cwc.OpType, opName)
	cwc.opArg = append(cwc.opArg, arg)
	cwc.TmpConstant = append(cwc.TmpConstant, &expression.Constant{RetType: cwc.TargetCol.RetType})
	for _, col := range affectedCols {
		if cwc.AffectedColSchema.Contains(col) {
			continue
		}
		cwc.compareFuncs = append(cwc.compareFuncs, chunk.GetCompareFunc(col.RetType))
		cwc.AffectedColSchema.Append(col)
	}
}

// CompareRow compares the rows for deduplicate.
func (cwc *ColWithCmpFuncManager) CompareRow(lhs, rhs chunk.Row) int {
	for i, col := range cwc.AffectedColSchema.Columns {
		ret := cwc.compareFuncs[i](lhs, col.Index, rhs, col.Index)
		if ret != 0 {
			return ret
		}
	}
	return 0
}

// BuildRangesByRow will build range of the given row. It will eval each function's arg then call BuildRange.
func (cwc *ColWithCmpFuncManager) BuildRangesByRow(ctx *rangerctx.RangerContext, row chunk.Row) ([]*ranger.Range, error) {
	exprs := make([]expression.Expression, len(cwc.OpType))
	exprCtx := ctx.ExprCtx
	for i, opType := range cwc.OpType {
		constantArg, err := cwc.opArg[i].Eval(exprCtx.GetEvalCtx(), row)
		if err != nil {
			return nil, err
		}
		cwc.TmpConstant[i].Value = constantArg
		newExpr, err := expression.NewFunction(exprCtx, opType, types.NewFieldType(mysql.TypeTiny), cwc.TargetCol, cwc.TmpConstant[i])
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, newExpr) // nozero
	}
	// We already limit range mem usage when buildTemplateRange for inner table of IndexJoin in optimizer phase, so we
	// don't need and shouldn't limit range mem usage when we refill inner ranges during the execution phase.
	ranges, _, _, err := ranger.BuildColumnRange(exprs, ctx, cwc.TargetCol.RetType, cwc.ColLength, 0)
	if err != nil {
		return nil, err
	}
	return ranges, nil
}

func (cwc *ColWithCmpFuncManager) resolveIndices(schema *expression.Schema) (err error) {
	for i := range cwc.opArg {
		cwc.opArg[i], _, err = cwc.opArg[i].ResolveIndices(schema)
		if err != nil {
			return err
		}
	}
	return nil
}

// String implements Stringer interface.
func (cwc *ColWithCmpFuncManager) String() string {
	buffer := bytes.NewBufferString("")
	for i := range cwc.OpType {
		fmt.Fprintf(buffer, "%v(%v, %v)", cwc.OpType[i], cwc.TargetCol, cwc.opArg[i])
		if i < len(cwc.OpType)-1 {
			buffer.WriteString(" ")
		}
	}
	return buffer.String()
}

const emptyColWithCmpFuncManagerSize = int64(unsafe.Sizeof(ColWithCmpFuncManager{}))

// MemoryUsage return the memory usage of ColWithCmpFuncManager
func (cwc *ColWithCmpFuncManager) MemoryUsage() (sum int64) {
	if cwc == nil {
		return
	}

	sum = emptyColWithCmpFuncManagerSize + int64(cap(cwc.compareFuncs))*size.SizeOfFunc
	if cwc.TargetCol != nil {
		sum += cwc.TargetCol.MemoryUsage()
	}
	if cwc.AffectedColSchema != nil {
		sum += cwc.AffectedColSchema.MemoryUsage()
	}

	for _, str := range cwc.OpType {
		sum += int64(len(str))
	}
	for _, expr := range cwc.opArg {
		sum += expr.MemoryUsage()
	}
	for _, cst := range cwc.TmpConstant {
		sum += cst.MemoryUsage()
	}
	return
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + build-filter-cost +
// (probe-cost + probe-filter-cost) / concurrency
// probe-cost = probe-child-cost * build-rows / batchRatio
func (p *PhysicalIndexJoin) GetPlanCostVer2(taskType property.TaskType, option *costusage.PlanCostOption, _ ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetIndexJoinCostVer24PhysicalIndexJoin(p, taskType, option, 0)
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalIndexJoin) Attach2Task(tasks ...base.Task) base.Task {
	return utilfuncp.Attach2Task4PhysicalIndexJoin(p, tasks...)
}

// ResolveIndices implements Plan interface.
func (p *PhysicalIndexJoin) ResolveIndices() (err error) {
	err = p.PhysicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	lSchema := p.Children()[0].Schema()
	rSchema := p.Children()[1].Schema()
	for i := range p.InnerJoinKeys {
		newOuterKey, _, err := p.OuterJoinKeys[i].ResolveIndices(p.Children()[1-p.InnerChildIdx].Schema())
		if err != nil {
			return err
		}
		p.OuterJoinKeys[i] = newOuterKey.(*expression.Column)
		newInnerKey, _, err := p.InnerJoinKeys[i].ResolveIndices(p.Children()[p.InnerChildIdx].Schema())
		if err != nil {
			return err
		}
		p.InnerJoinKeys[i] = newInnerKey.(*expression.Column)
	}
	for i, expr := range p.LeftConditions {
		p.LeftConditions[i], _, err = expr.ResolveIndices(lSchema)
		if err != nil {
			return err
		}
	}
	for i, expr := range p.RightConditions {
		p.RightConditions[i], _, err = expr.ResolveIndices(rSchema)
		if err != nil {
			return err
		}
	}
	mergedSchema := expression.MergeSchema(lSchema, rSchema)
	for i, expr := range p.OtherConditions {
		p.OtherConditions[i], _, err = expr.ResolveIndices(mergedSchema)
		if err != nil {
			return err
		}
	}
	if p.CompareFilters != nil {
		err = p.CompareFilters.resolveIndices(p.Children()[1-p.InnerChildIdx].Schema())
		if err != nil {
			return err
		}
		for i := range p.CompareFilters.AffectedColSchema.Columns {
			resolvedCol, _, err1 := p.CompareFilters.AffectedColSchema.Columns[i].ResolveIndices(p.Children()[1-p.InnerChildIdx].Schema())
			if err1 != nil {
				return err1
			}
			p.CompareFilters.AffectedColSchema.Columns[i] = resolvedCol.(*expression.Column)
		}
	}
	for i := range p.OuterHashKeys {
		outerKey, _, err := p.OuterHashKeys[i].ResolveIndices(p.Children()[1-p.InnerChildIdx].Schema())
		if err != nil {
			return err
		}
		innerKey, _, err := p.InnerHashKeys[i].ResolveIndices(p.Children()[p.InnerChildIdx].Schema())
		if err != nil {
			return err
		}
		p.OuterHashKeys[i], p.InnerHashKeys[i] = outerKey.(*expression.Column), innerKey.(*expression.Column)
	}

	colsNeedResolving := p.Schema().Len()
	// The last output column of this two join is the generated column to indicate whether the row is matched or not.
	if p.JoinType == base.LeftOuterSemiJoin || p.JoinType == base.AntiLeftOuterSemiJoin {
		colsNeedResolving--
	}
	// To avoid that two plan shares the same column slice.
	shallowColSlice := make([]*expression.Column, p.Schema().Len())
	copy(shallowColSlice, p.Schema().Columns)
	p.SetSchema(expression.NewSchema(shallowColSlice...))
	foundCnt := 0
	// The two column sets are all ordered. And the colsNeedResolving is the subset of the mergedSchema.
	// So we can just move forward j if there's no matching is found.
	// We don't use the normal ResolvIndices here since there might be duplicate columns in the schema.
	//   e.g. The schema of child_0 is [col0, col0, col1]
	//        ResolveIndices will only resolve all col0 reference of the current plan to the first col0.
	for i, j := 0, 0; i < colsNeedResolving && j < len(mergedSchema.Columns); {
		if !p.Schema().Columns[i].EqualColumn(mergedSchema.Columns[j]) {
			j++
			continue
		}
		p.Schema().Columns[i] = p.Schema().Columns[i].Clone().(*expression.Column)
		p.Schema().Columns[i].Index = j
		i++
		j++
		foundCnt++
	}
	if foundCnt < colsNeedResolving {
		return errors.Errorf("Some columns of %v cannot find the reference from its child(ren)", p.ExplainID().String())
	}

	return
}

// IndexJoinInfo is generated by index join's inner ds, which will build their own index choice based
// the indexJoinProp pushed down by index join. While index join still need some feedback by this kind
// choice info to make index join runtime compatible, like IdxColLens will help truncate the index key
// to construct suitable lookup contents. KeyOff2IdxOff will help ds to quickly locate the index column
// from lookup contents. Ranges will be used to rebuild the underlying index range if there is any parameter
// affecting the index join's inner range. CompareFilters will be used to quickly evaluate the last-col's
// non-eq range.
// This kind of IndexJoinInfo will be wrapped as a part of CopTask or RootTask, which will be passed upward
// to targeted indexJoin to complete the physicalIndexJoin's detail: ref:
type IndexJoinInfo struct {
	// The following fields are used to keep index join aware of inner plan's index/pk choice.
	IdxColLens     []int
	KeyOff2IdxOff  []int
	Ranges         ranger.MutableRanges
	CompareFilters *ColWithCmpFuncManager
}
