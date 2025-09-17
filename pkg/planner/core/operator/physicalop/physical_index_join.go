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
	"slices"
	"strings"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	h "github.com/pingcap/tidb/pkg/util/hint"
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
func (p *PhysicalIndexJoin) GetPlanCostVer1(taskType property.TaskType, option *optimizetrace.PlanCostOption) (float64, error) {
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
		cwc.opArg[i], err = cwc.opArg[i].ResolveIndices(schema)
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
func (p *PhysicalIndexJoin) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption, _ ...bool) (costusage.CostVer2, error) {
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
		newOuterKey, err := p.OuterJoinKeys[i].ResolveIndices(p.Children()[1-p.InnerChildIdx].Schema())
		if err != nil {
			return err
		}
		p.OuterJoinKeys[i] = newOuterKey.(*expression.Column)
		newInnerKey, err := p.InnerJoinKeys[i].ResolveIndices(p.Children()[p.InnerChildIdx].Schema())
		if err != nil {
			return err
		}
		p.InnerJoinKeys[i] = newInnerKey.(*expression.Column)
	}
	for i, expr := range p.LeftConditions {
		p.LeftConditions[i], err = expr.ResolveIndices(lSchema)
		if err != nil {
			return err
		}
	}
	for i, expr := range p.RightConditions {
		p.RightConditions[i], err = expr.ResolveIndices(rSchema)
		if err != nil {
			return err
		}
	}
	mergedSchema := expression.MergeSchema(lSchema, rSchema)
	for i, expr := range p.OtherConditions {
		p.OtherConditions[i], err = expr.ResolveIndices(mergedSchema)
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
			resolvedCol, err1 := p.CompareFilters.AffectedColSchema.Columns[i].ResolveIndices(p.Children()[1-p.InnerChildIdx].Schema())
			if err1 != nil {
				return err1
			}
			p.CompareFilters.AffectedColSchema.Columns[i] = resolvedCol.(*expression.Column)
		}
	}
	for i := range p.OuterHashKeys {
		outerKey, err := p.OuterHashKeys[i].ResolveIndices(p.Children()[1-p.InnerChildIdx].Schema())
		if err != nil {
			return err
		}
		innerKey, err := p.InnerHashKeys[i].ResolveIndices(p.Children()[p.InnerChildIdx].Schema())
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

// tryToGetIndexJoin returns all available index join plans, and the second returned value indicates whether this plan is enforced by hints.
func tryToGetIndexJoin(p *logicalop.LogicalJoin, prop *property.PhysicalProperty) (indexJoins []base.PhysicalPlan, canForced bool) {
	// supportLeftOuter and supportRightOuter indicates whether this type of join
	// supports the left side or right side to be the outer side.
	var supportLeftOuter, supportRightOuter bool
	switch p.JoinType {
	case base.SemiJoin, base.AntiSemiJoin, base.LeftOuterSemiJoin, base.AntiLeftOuterSemiJoin, base.LeftOuterJoin:
		supportLeftOuter = true
	case base.RightOuterJoin:
		supportRightOuter = true
	case base.InnerJoin:
		supportLeftOuter, supportRightOuter = true, true
	}
	candidates := make([]base.PhysicalPlan, 0, 2)
	if supportLeftOuter {
		candidates = append(candidates, getIndexJoinByOuterIdx(p, prop, 0)...)
	}
	if supportRightOuter {
		candidates = append(candidates, getIndexJoinByOuterIdx(p, prop, 1)...)
	}

	// Handle hints and variables about index join.
	// The priority is: force hints like TIDB_INLJ > filter hints like NO_INDEX_JOIN > variables.
	// Handle hints conflict first.
	stmtCtx := p.SCtx().GetSessionVars().StmtCtx
	if p.PreferAny(h.PreferLeftAsINLJInner, h.PreferRightAsINLJInner) && p.PreferAny(h.PreferNoIndexJoin) {
		stmtCtx.SetHintWarning("Some INL_JOIN and NO_INDEX_JOIN hints conflict, NO_INDEX_JOIN may be ignored")
	}
	if p.PreferAny(h.PreferLeftAsINLHJInner, h.PreferRightAsINLHJInner) && p.PreferAny(h.PreferNoIndexHashJoin) {
		stmtCtx.SetHintWarning("Some INL_HASH_JOIN and NO_INDEX_HASH_JOIN hints conflict, NO_INDEX_HASH_JOIN may be ignored")
	}
	if p.PreferAny(h.PreferLeftAsINLMJInner, h.PreferRightAsINLMJInner) && p.PreferAny(h.PreferNoIndexMergeJoin) {
		stmtCtx.SetHintWarning("Some INL_MERGE_JOIN and NO_INDEX_MERGE_JOIN hints conflict, NO_INDEX_MERGE_JOIN may be ignored")
	}

	candidates, canForced = handleForceIndexJoinHints(p, prop, candidates)
	if canForced {
		return candidates, canForced
	}
	candidates = handleFilterIndexJoinHints(p, candidates)
	// todo: if any variables banned it, why bother to generate it first?
	return filterIndexJoinBySessionVars(p.SCtx(), candidates), false
}

// tryToEnumerateIndexJoin returns all available index join plans, which will require inner indexJoinProp downside
// compared with original tryToGetIndexJoin.
func tryToEnumerateIndexJoin(super base.LogicalPlan, prop *property.PhysicalProperty) []base.PhysicalPlan {
	_, p := base.GetGEAndLogical[*logicalop.LogicalJoin](super)
	// supportLeftOuter and supportRightOuter indicates whether this type of join
	// supports the left side or right side to be the outer side.
	var supportLeftOuter, supportRightOuter bool
	switch p.JoinType {
	case base.SemiJoin, base.AntiSemiJoin, base.LeftOuterSemiJoin, base.AntiLeftOuterSemiJoin, base.LeftOuterJoin:
		supportLeftOuter = true
	case base.RightOuterJoin:
		supportRightOuter = true
	case base.InnerJoin:
		supportLeftOuter, supportRightOuter = true, true
	}
	// according join type to enumerate index join with inner children's indexJoinProp.
	candidates := make([]base.PhysicalPlan, 0, 2)
	if supportLeftOuter {
		candidates = append(candidates, enumerateIndexJoinByOuterIdx(super, prop, 0)...)
	}
	if supportRightOuter {
		candidates = append(candidates, enumerateIndexJoinByOuterIdx(super, prop, 1)...)
	}
	// Pre-Handle hints and variables about index join, which try to detect the contradictory hint and variables
	// The priority is: force hints like TIDB_INLJ > filter hints like NO_INDEX_JOIN > variables and rec warns.
	stmtCtx := p.SCtx().GetSessionVars().StmtCtx
	if p.PreferAny(h.PreferLeftAsINLJInner, h.PreferRightAsINLJInner) && p.PreferAny(h.PreferNoIndexJoin) {
		stmtCtx.SetHintWarning("Some INL_JOIN and NO_INDEX_JOIN hints conflict, NO_INDEX_JOIN may be ignored")
	}
	if p.PreferAny(h.PreferLeftAsINLHJInner, h.PreferRightAsINLHJInner) && p.PreferAny(h.PreferNoIndexHashJoin) {
		stmtCtx.SetHintWarning("Some INL_HASH_JOIN and NO_INDEX_HASH_JOIN hints conflict, NO_INDEX_HASH_JOIN may be ignored")
	}
	if p.PreferAny(h.PreferLeftAsINLMJInner, h.PreferRightAsINLMJInner) && p.PreferAny(h.PreferNoIndexMergeJoin) {
		stmtCtx.SetHintWarning("Some INL_MERGE_JOIN and NO_INDEX_MERGE_JOIN hints conflict, NO_INDEX_MERGE_JOIN may be ignored")
	}
	// previously we will think about force index join hints here, but we have to wait the inner plans to be a valid
	// physical one/ones. Because indexJoinProp may not be admitted by its inner patterns, so we innovatively move all
	// hint related handling to the findBestTask function when we see the entire inner physical-ized plan tree. See xxx
	// for details.
	//
	// handleFilterIndexJoinHints is trying to avoid generating index join or index hash join when no-index-join related
	// hint is specified in the query. So we can do it in physic enumeration phase here.
	return handleFilterIndexJoinHints(p, candidates)
}

// handleFilterIndexJoinHints is trying to avoid generating index join or index hash join when no-index-join related
// hint is specified in the query. So we can do it in physic enumeration phase.
func handleFilterIndexJoinHints(p *logicalop.LogicalJoin, candidates []base.PhysicalPlan) []base.PhysicalPlan {
	if !p.PreferAny(h.PreferNoIndexJoin, h.PreferNoIndexHashJoin, h.PreferNoIndexMergeJoin) {
		return candidates // no filter index join hints
	}
	filtered := make([]base.PhysicalPlan, 0, len(candidates))
	for _, candidate := range candidates {
		_, joinMethod, ok := getIndexJoinSideAndMethod(candidate)
		if !ok {
			continue
		}
		if (p.PreferAny(h.PreferNoIndexJoin) && joinMethod == indexJoinMethod) ||
			(p.PreferAny(h.PreferNoIndexHashJoin) && joinMethod == indexHashJoinMethod) ||
			(p.PreferAny(h.PreferNoIndexMergeJoin) && joinMethod == indexMergeJoinMethod) {
			continue
		}
		filtered = append(filtered, candidate)
	}
	return filtered
}

// getIndexJoinByOuterIdx will generate index join by outerIndex. OuterIdx points out the outer child.
// First of all, we'll check whether the inner child is DataSource.
// Then, we will extract the join keys of p's equal conditions. Then check whether all of them are just the primary key
// or match some part of on index. If so we will choose the best one and construct a index join.
func getIndexJoinByOuterIdx(p *logicalop.LogicalJoin, prop *property.PhysicalProperty, outerIdx int) (joins []base.PhysicalPlan) {
	outerChild, innerChild := p.Children()[outerIdx], p.Children()[1-outerIdx]
	all, _ := prop.AllSameOrder()
	// If the order by columns are not all from outer child, index join cannot promise the order.
	if !prop.AllColsFromSchema(outerChild.Schema()) || !all {
		return nil
	}
	var (
		innerJoinKeys []*expression.Column
		outerJoinKeys []*expression.Column
	)
	if outerIdx == 0 {
		outerJoinKeys, innerJoinKeys, _, _ = p.GetJoinKeys()
	} else {
		innerJoinKeys, outerJoinKeys, _, _ = p.GetJoinKeys()
	}
	innerChildWrapper := extractIndexJoinInnerChildPattern(p, innerChild)
	if innerChildWrapper == nil {
		return nil
	}

	var avgInnerRowCnt float64
	if outerChild.StatsInfo().RowCount > 0 {
		avgInnerRowCnt = p.EqualCondOutCnt / outerChild.StatsInfo().RowCount
	}
	joins = buildIndexJoinInner2TableScan(p, prop, innerChildWrapper, innerJoinKeys, outerJoinKeys, outerIdx, avgInnerRowCnt)
	if joins != nil {
		return
	}
	return buildIndexJoinInner2IndexScan(p, prop, innerChildWrapper, innerJoinKeys, outerJoinKeys, outerIdx, avgInnerRowCnt)
}

// indexJoinInnerChildWrapper is a wrapper for the inner child of an index join.
// It contains the lowest DataSource operator and other inner child operator
// which is flattened into a list structure from tree structure .
// For example, the inner child of an index join is a tree structure like:
//
//	Projection
//	       Aggregation
//				Selection
//					DataSource
//
// The inner child wrapper will be:
// DataSource: the lowest DataSource operator.
// hasDitryWrite: whether the inner child contains dirty data.
// zippedChildren: [Projection, Aggregation, Selection]
type indexJoinInnerChildWrapper struct {
	ds             *logicalop.DataSource
	hasDitryWrite  bool
	zippedChildren []base.LogicalPlan
}

// buildIndexJoinInner2TableScan builds a TableScan as the inner child for an
// IndexJoin if possible.
// If the inner side of an index join is a TableScan, only one tuple will be
// fetched from the inner side for every tuple from the outer side. This will be
// promised to be no worse than building IndexScan as the inner child.
func buildIndexJoinInner2TableScan(
	p *logicalop.LogicalJoin,
	prop *property.PhysicalProperty, wrapper *indexJoinInnerChildWrapper,
	innerJoinKeys, outerJoinKeys []*expression.Column,
	outerIdx int, avgInnerRowCnt float64) (joins []base.PhysicalPlan) {
	ds := wrapper.ds
	var tblPath *util.AccessPath
	for _, path := range ds.PossibleAccessPaths {
		if path.IsTablePath() && path.StoreType == kv.TiKV {
			tblPath = path
			break
		}
	}
	if tblPath == nil {
		return nil
	}
	var keyOff2IdxOff []int
	var ranges ranger.MutableRanges = ranger.Ranges{}
	var innerTask, innerTask2 base.Task
	var indexJoinResult *indexJoinPathResult
	if ds.TableInfo.IsCommonHandle {
		indexJoinResult, keyOff2IdxOff = getBestIndexJoinPathResult(p, ds, innerJoinKeys, outerJoinKeys, func(path *util.AccessPath) bool { return path.IsCommonHandlePath })
		if indexJoinResult == nil {
			return nil
		}
		rangeInfo := indexJoinPathRangeInfo(p.SCtx(), outerJoinKeys, indexJoinResult)
		innerTask = constructInnerTableScanTask(p, prop, wrapper, indexJoinResult.chosenRanges.Range(), rangeInfo, false, false, avgInnerRowCnt)
		// The index merge join's inner plan is different from index join, so we
		// should construct another inner plan for it.
		// Because we can't keep order for union scan, if there is a union scan in inner task,
		// we can't construct index merge join.
		if !wrapper.hasDitryWrite {
			innerTask2 = constructInnerTableScanTask(p, prop, wrapper, indexJoinResult.chosenRanges.Range(), rangeInfo, true, !prop.IsSortItemEmpty() && prop.SortItems[0].Desc, avgInnerRowCnt)
		}
		ranges = indexJoinResult.chosenRanges
	} else {
		var (
			ok bool
			// note: pk col doesn't have mutableRanges, the global var(ranges) which will be handled as empty range in constructIndexJoin.
			localRanges ranger.Ranges
		)
		keyOff2IdxOff, outerJoinKeys, localRanges, _, ok = getIndexJoinIntPKPathInfo(ds, innerJoinKeys, outerJoinKeys, func(path *util.AccessPath) bool { return path.IsIntHandlePath })
		if !ok {
			return nil
		}
		rangeInfo := indexJoinIntPKRangeInfo(p.SCtx().GetExprCtx().GetEvalCtx(), outerJoinKeys)
		innerTask = constructInnerTableScanTask(p, prop, wrapper, localRanges, rangeInfo, false, false, avgInnerRowCnt)
		// The index merge join's inner plan is different from index join, so we
		// should construct another inner plan for it.
		// Because we can't keep order for union scan, if there is a union scan in inner task,
		// we can't construct index merge join.
		if !wrapper.hasDitryWrite {
			innerTask2 = constructInnerTableScanTask(p, prop, wrapper, localRanges, rangeInfo, true, !prop.IsSortItemEmpty() && prop.SortItems[0].Desc, avgInnerRowCnt)
		}
	}
	var (
		path       *util.AccessPath
		lastColMng *ColWithCmpFuncManager
	)
	if indexJoinResult != nil {
		path = indexJoinResult.chosenPath
		lastColMng = indexJoinResult.lastColManager
	}
	joins = make([]base.PhysicalPlan, 0, 3)
	failpoint.Inject("MockOnlyEnableIndexHashJoin", func(val failpoint.Value) {
		if val.(bool) && !p.SCtx().GetSessionVars().InRestrictedSQL {
			failpoint.Return(constructIndexHashJoin(p, prop, outerIdx, innerTask, nil, keyOff2IdxOff, path, lastColMng))
		}
	})
	joins = append(joins, constructIndexJoin(p, prop, outerIdx, innerTask, ranges, keyOff2IdxOff, path, lastColMng, true)...)
	// We can reuse the `innerTask` here since index nested loop hash join
	// do not need the inner child to promise the order.
	joins = append(joins, constructIndexHashJoin(p, prop, outerIdx, innerTask, ranges, keyOff2IdxOff, path, lastColMng)...)
	if innerTask2 != nil {
		joins = append(joins, constructIndexMergeJoin(p, prop, outerIdx, innerTask2, ranges, keyOff2IdxOff, path, lastColMng)...)
	}
	return joins
}

func buildIndexJoinInner2IndexScan(
	p *logicalop.LogicalJoin,
	prop *property.PhysicalProperty, wrapper *indexJoinInnerChildWrapper, innerJoinKeys, outerJoinKeys []*expression.Column,
	outerIdx int, avgInnerRowCnt float64) (joins []base.PhysicalPlan) {
	ds := wrapper.ds
	indexValid := func(path *util.AccessPath) bool {
		if path.IsTablePath() {
			return false
		}
		// if path is index path. index path currently include two kind of, one is normal, and the other is mv index.
		// for mv index like mvi(a, json, b), if driving condition is a=1, and we build a prefix scan with range [1,1]
		// on mvi, it will return many index rows which breaks handle-unique attribute here.
		//
		// the basic rule is that: mv index can be and can only be accessed by indexMerge operator. (embedded handle duplication)
		if !isMVIndexPath(path) {
			return true // not a MVIndex path, it can successfully be index join probe side.
		}
		return false
	}
	indexJoinResult, keyOff2IdxOff := getBestIndexJoinPathResult(p, ds, innerJoinKeys, outerJoinKeys, indexValid)
	if indexJoinResult == nil {
		return nil
	}
	joins = make([]base.PhysicalPlan, 0, 3)
	rangeInfo := indexJoinPathRangeInfo(p.SCtx(), outerJoinKeys, indexJoinResult)
	maxOneRow := false
	if indexJoinResult.chosenPath.Index.Unique && indexJoinResult.usedColsLen == len(indexJoinResult.chosenPath.FullIdxCols) {
		l := len(indexJoinResult.chosenAccess)
		if l == 0 {
			maxOneRow = true
		} else {
			sf, ok := indexJoinResult.chosenAccess[l-1].(*expression.ScalarFunction)
			maxOneRow = ok && (sf.FuncName.L == ast.EQ)
		}
	}
	innerTask := constructInnerIndexScanTask(p, prop, wrapper, indexJoinResult.chosenPath, indexJoinResult.chosenRanges.Range(), indexJoinResult.chosenRemained, indexJoinResult.idxOff2KeyOff, rangeInfo, false, false, avgInnerRowCnt, maxOneRow)
	failpoint.Inject("MockOnlyEnableIndexHashJoin", func(val failpoint.Value) {
		if val.(bool) && !p.SCtx().GetSessionVars().InRestrictedSQL && innerTask != nil {
			failpoint.Return(constructIndexHashJoin(p, prop, outerIdx, innerTask, indexJoinResult.chosenRanges, keyOff2IdxOff, indexJoinResult.chosenPath, indexJoinResult.lastColManager))
		}
	})
	if innerTask != nil {
		joins = append(joins, constructIndexJoin(p, prop, outerIdx, innerTask, indexJoinResult.chosenRanges, keyOff2IdxOff, indexJoinResult.chosenPath, indexJoinResult.lastColManager, true)...)
		// We can reuse the `innerTask` here since index nested loop hash join
		// do not need the inner child to promise the order.
		joins = append(joins, constructIndexHashJoin(p, prop, outerIdx, innerTask, indexJoinResult.chosenRanges, keyOff2IdxOff, indexJoinResult.chosenPath, indexJoinResult.lastColManager)...)
	}
	// The index merge join's inner plan is different from index join, so we
	// should construct another inner plan for it.
	// Because we can't keep order for union scan, if there is a union scan in inner task,
	// we can't construct index merge join.
	if !wrapper.hasDitryWrite {
		innerTask2 := constructInnerIndexScanTask(p, prop, wrapper, indexJoinResult.chosenPath, indexJoinResult.chosenRanges.Range(), indexJoinResult.chosenRemained, indexJoinResult.idxOff2KeyOff, rangeInfo, true, !prop.IsSortItemEmpty() && prop.SortItems[0].Desc, avgInnerRowCnt, maxOneRow)
		if innerTask2 != nil {
			joins = append(joins, constructIndexMergeJoin(p, prop, outerIdx, innerTask2, indexJoinResult.chosenRanges, keyOff2IdxOff, indexJoinResult.chosenPath, indexJoinResult.lastColManager)...)
		}
	}
	return joins
}

// When inner plan is TableReader, the parameter `ranges` will be nil. Because pk only have one column. So all of its range
// is generated during execution time.
func constructIndexJoin(
	p *logicalop.LogicalJoin,
	prop *property.PhysicalProperty,
	outerIdx int,
	innerTask base.Task,
	ranges ranger.MutableRanges,
	keyOff2IdxOff []int,
	path *util.AccessPath,
	compareFilters *ColWithCmpFuncManager,
	extractOtherEQ bool,
) []base.PhysicalPlan {
	if innerTask.Invalid() {
		return nil
	}
	if ranges == nil {
		ranges = ranger.Ranges{} // empty range
	}

	joinType := p.JoinType
	var (
		innerJoinKeys []*expression.Column
		outerJoinKeys []*expression.Column
		isNullEQ      []bool
		hasNullEQ     bool
	)
	if outerIdx == 0 {
		outerJoinKeys, innerJoinKeys, isNullEQ, hasNullEQ = p.GetJoinKeys()
	} else {
		innerJoinKeys, outerJoinKeys, isNullEQ, hasNullEQ = p.GetJoinKeys()
	}
	// TODO: support null equal join keys for index join
	if hasNullEQ {
		return nil
	}
	chReqProps := make([]*property.PhysicalProperty, 2)
	chReqProps[outerIdx] = &property.PhysicalProperty{TaskTp: property.RootTaskType, ExpectedCnt: math.MaxFloat64,
		SortItems: prop.SortItems, CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: prop.NoCopPushDown}
	if prop.ExpectedCnt < p.StatsInfo().RowCount {
		expCntScale := prop.ExpectedCnt / p.StatsInfo().RowCount
		chReqProps[outerIdx].ExpectedCnt = p.Children()[outerIdx].StatsInfo().RowCount * expCntScale
	}
	newInnerKeys := make([]*expression.Column, 0, len(innerJoinKeys))
	newOuterKeys := make([]*expression.Column, 0, len(outerJoinKeys))
	newIsNullEQ := make([]bool, 0, len(isNullEQ))
	newKeyOff := make([]int, 0, len(keyOff2IdxOff))
	newOtherConds := make([]expression.Expression, len(p.OtherConditions), len(p.OtherConditions)+len(p.EqualConditions))
	copy(newOtherConds, p.OtherConditions)
	for keyOff, idxOff := range keyOff2IdxOff {
		if keyOff2IdxOff[keyOff] < 0 {
			newOtherConds = append(newOtherConds, p.EqualConditions[keyOff])
			continue
		}
		newInnerKeys = append(newInnerKeys, innerJoinKeys[keyOff])
		newOuterKeys = append(newOuterKeys, outerJoinKeys[keyOff])
		newIsNullEQ = append(newIsNullEQ, isNullEQ[keyOff])
		newKeyOff = append(newKeyOff, idxOff)
	}

	var outerHashKeys, innerHashKeys []*expression.Column
	outerHashKeys, innerHashKeys = make([]*expression.Column, len(newOuterKeys)), make([]*expression.Column, len(newInnerKeys))
	copy(outerHashKeys, newOuterKeys)
	copy(innerHashKeys, newInnerKeys)
	// we can use the `col <eq> col` in `OtherCondition` to build the hashtable to avoid the unnecessary calculating.
	for i := len(newOtherConds) - 1; extractOtherEQ && i >= 0; i = i - 1 {
		switch c := newOtherConds[i].(type) {
		case *expression.ScalarFunction:
			if c.FuncName.L == ast.EQ {
				lhs, ok1 := c.GetArgs()[0].(*expression.Column)
				rhs, ok2 := c.GetArgs()[1].(*expression.Column)
				if ok1 && ok2 {
					if lhs.InOperand || rhs.InOperand {
						// if this other-cond is from a `[not] in` sub-query, do not convert it into eq-cond since
						// IndexJoin cannot deal with NULL correctly in this case; please see #25799 for more details.
						continue
					}
					outerSchema, innerSchema := p.Children()[outerIdx].Schema(), p.Children()[1-outerIdx].Schema()
					if outerSchema.Contains(lhs) && innerSchema.Contains(rhs) {
						outerHashKeys = append(outerHashKeys, lhs) // nozero
						innerHashKeys = append(innerHashKeys, rhs) // nozero
					} else if innerSchema.Contains(lhs) && outerSchema.Contains(rhs) {
						outerHashKeys = append(outerHashKeys, rhs) // nozero
						innerHashKeys = append(innerHashKeys, lhs) // nozero
					}
					newOtherConds = slices.Delete(newOtherConds, i, i+1)
				}
			}
		default:
			continue
		}
	}

	baseJoin := BasePhysicalJoin{
		InnerChildIdx:   1 - outerIdx,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: newOtherConds,
		JoinType:        joinType,
		OuterJoinKeys:   newOuterKeys,
		InnerJoinKeys:   newInnerKeys,
		IsNullEQ:        newIsNullEQ,
		DefaultValues:   p.DefaultValues,
	}

	join := PhysicalIndexJoin{
		BasePhysicalJoin: baseJoin,
		InnerPlan:        innerTask.Plan(),
		KeyOff2IdxOff:    newKeyOff,
		Ranges:           ranges,
		CompareFilters:   compareFilters,
		OuterHashKeys:    outerHashKeys,
		InnerHashKeys:    innerHashKeys,
	}.Init(p.SCtx(), p.StatsInfo().ScaleByExpectCnt(p.SCtx().GetSessionVars(), prop.ExpectedCnt), p.QueryBlockOffset(), chReqProps...)
	if path != nil {
		join.IdxColLens = path.IdxColLens
	}
	join.SetSchema(p.Schema())
	return []base.PhysicalPlan{join}
}

// handleForceIndexJoinHints handles the force index join hints and returns all plans that can satisfy the hints.
func handleForceIndexJoinHints(p *logicalop.LogicalJoin, prop *property.PhysicalProperty, candidates []base.PhysicalPlan) (indexJoins []base.PhysicalPlan, canForced bool) {
	if !p.PreferAny(h.PreferRightAsINLJInner, h.PreferRightAsINLHJInner, h.PreferRightAsINLMJInner,
		h.PreferLeftAsINLJInner, h.PreferLeftAsINLHJInner, h.PreferLeftAsINLMJInner) {
		return candidates, false // no force index join hints
	}
	forced := make([]base.PhysicalPlan, 0, len(candidates))
	for _, candidate := range candidates {
		innerSide, joinMethod, ok := getIndexJoinSideAndMethod(candidate)
		if !ok {
			continue
		}
		if (p.PreferAny(h.PreferLeftAsINLJInner) && innerSide == joinLeft && joinMethod == indexJoinMethod) ||
			(p.PreferAny(h.PreferRightAsINLJInner) && innerSide == joinRight && joinMethod == indexJoinMethod) ||
			(p.PreferAny(h.PreferLeftAsINLHJInner) && innerSide == joinLeft && joinMethod == indexHashJoinMethod) ||
			(p.PreferAny(h.PreferRightAsINLHJInner) && innerSide == joinRight && joinMethod == indexHashJoinMethod) ||
			(p.PreferAny(h.PreferLeftAsINLMJInner) && innerSide == joinLeft && joinMethod == indexMergeJoinMethod) ||
			(p.PreferAny(h.PreferRightAsINLMJInner) && innerSide == joinRight && joinMethod == indexMergeJoinMethod) {
			forced = append(forced, candidate)
		}
	}

	if len(forced) > 0 {
		return forced, true
	}
	// Cannot find any valid index join plan with these force hints.
	// Print warning message if any hints cannot work.
	// If the required property is not empty, we will enforce it and try the hint again.
	// So we only need to generate warning message when the property is empty.
	if prop.IsSortItemEmpty() {
		var indexJoinTables, indexHashJoinTables, indexMergeJoinTables []h.HintedTable
		if p.HintInfo != nil {
			t := p.HintInfo.IndexJoin
			indexJoinTables, indexHashJoinTables, indexMergeJoinTables = t.INLJTables, t.INLHJTables, t.INLMJTables
		}
		var errMsg string
		switch {
		case p.PreferAny(h.PreferLeftAsINLJInner, h.PreferRightAsINLJInner): // prefer index join
			errMsg = fmt.Sprintf("Optimizer Hint %s or %s is inapplicable", h.Restore2JoinHint(h.HintINLJ, indexJoinTables), h.Restore2JoinHint(h.TiDBIndexNestedLoopJoin, indexJoinTables))
		case p.PreferAny(h.PreferLeftAsINLHJInner, h.PreferRightAsINLHJInner): // prefer index hash join
			errMsg = fmt.Sprintf("Optimizer Hint %s is inapplicable", h.Restore2JoinHint(h.HintINLHJ, indexHashJoinTables))
		case p.PreferAny(h.PreferLeftAsINLMJInner, h.PreferRightAsINLMJInner): // prefer index merge join
			errMsg = fmt.Sprintf("Optimizer Hint %s is inapplicable", h.Restore2JoinHint(h.HintINLMJ, indexMergeJoinTables))
		}
		// Append inapplicable reason.
		if len(p.EqualConditions) == 0 {
			errMsg += " without column equal ON condition"
		}
		// Generate warning message to client.
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(errMsg)
	}
	return candidates, false
}

func filterIndexJoinBySessionVars(sc base.PlanContext, indexJoins []base.PhysicalPlan) []base.PhysicalPlan {
	if sc.GetSessionVars().EnableIndexMergeJoin {
		return indexJoins
	}
	return slices.DeleteFunc(indexJoins, func(indexJoin base.PhysicalPlan) bool {
		_, ok := indexJoin.(*PhysicalIndexMergeJoin)
		return ok
	})
}

// enumerateIndexJoinByOuterIdx will enumerate temporary index joins by index join prop required for its inner child.
func enumerateIndexJoinByOuterIdx(super base.LogicalPlan, prop *property.PhysicalProperty, outerIdx int) (joins []base.PhysicalPlan) {
	ge, p := base.GetGEAndLogical[*logicalop.LogicalJoin](super)
	stats0, stats1, schema0, schema1 := getJoinChildStatsAndSchema(ge, p)
	var outerSchema *expression.Schema
	var outerStats *property.StatsInfo
	if outerIdx == 0 {
		outerSchema = schema0
		outerStats = stats0
	} else {
		outerSchema = schema1
		outerStats = stats1
	}
	// need same order
	all, _ := prop.AllSameOrder()
	// If the order by columns are not all from outer child, index join cannot promise the order.
	if !prop.AllColsFromSchema(outerSchema) || !all {
		return nil
	}
	var (
		innerJoinKeys []*expression.Column
		outerJoinKeys []*expression.Column
	)
	if outerIdx == 0 {
		outerJoinKeys, innerJoinKeys, _, _ = p.GetJoinKeys()
	} else {
		innerJoinKeys, outerJoinKeys, _, _ = p.GetJoinKeys()
	}
	// computed the avgInnerRowCnt
	var avgInnerRowCnt float64
	if count := outerStats.RowCount; count > 0 {
		avgInnerRowCnt = p.EqualCondOutCnt / count
	}
	// for pk path
	indexJoinPropTS := &property.IndexJoinRuntimeProp{
		OtherConditions: p.OtherConditions,
		InnerJoinKeys:   innerJoinKeys,
		OuterJoinKeys:   outerJoinKeys,
		AvgInnerRowCnt:  avgInnerRowCnt,
		TableRangeScan:  true,
	}
	// for normal index path
	indexJoinPropIS := &property.IndexJoinRuntimeProp{
		OtherConditions: p.OtherConditions,
		InnerJoinKeys:   innerJoinKeys,
		OuterJoinKeys:   outerJoinKeys,
		AvgInnerRowCnt:  avgInnerRowCnt,
		TableRangeScan:  false,
	}
	indexJoins := constructIndexJoinStatic(p, prop, outerIdx, indexJoinPropTS, outerStats)
	indexJoins = append(indexJoins, constructIndexJoinStatic(p, prop, outerIdx, indexJoinPropIS, outerStats)...)
	indexJoins = append(indexJoins, constructIndexHashJoinStatic(p, prop, outerIdx, indexJoinPropTS, outerStats)...)
	indexJoins = append(indexJoins, constructIndexHashJoinStatic(p, prop, outerIdx, indexJoinPropIS, outerStats)...)
	return indexJoins
}

// constructIndexJoinStatic is used to enumerate current a physical index join with undecided inner plan. Via index join prop
// pushed down to the inner side, the inner plans will check the admission of valid indexJoinProp and enumerate admitted inner
// operator. This function is quite similar with constructIndexJoin. While differing in following part:
//
// Since constructIndexJoin will fill the physicalIndexJoin some runtime detail even for adjusting the keys, hash-keys, move
// eq condition into other conditions because the underlying ds couldn't use it or something. This is because previously the
// index join enumeration can see the complete index chosen result after inner task is built. But for the refactored one, the
// enumerated physical index here can only see the info it owns. That's why we call the function constructIndexJoinStatic.
//
// The indexJoinProp is passed down to the inner side, which contains the runtime constant inner key, which is used to build the
// underlying index/pk range. When the inner side is built bottom up, it will return the indexJoinInfo, which contains the runtime
// information that this physical index join wants. That's introduce second function called completePhysicalIndexJoin, which will
// fill physicalIndexJoin about all the runtime information it lacks in static enumeration phase.
func constructIndexJoinStatic(
	p *logicalop.LogicalJoin,
	prop *property.PhysicalProperty,
	outerIdx int,
	indexJoinProp *property.IndexJoinRuntimeProp,
	outerStats *property.StatsInfo,
) []base.PhysicalPlan {
	joinType := p.JoinType
	var (
		innerJoinKeys []*expression.Column
		outerJoinKeys []*expression.Column
		isNullEQ      []bool
		hasNullEQ     bool
	)
	if outerIdx == 0 {
		outerJoinKeys, innerJoinKeys, isNullEQ, hasNullEQ = p.GetJoinKeys()
	} else {
		innerJoinKeys, outerJoinKeys, isNullEQ, hasNullEQ = p.GetJoinKeys()
	}
	// TODO: support null equal join keys for index join
	if hasNullEQ {
		return nil
	}
	chReqProps := make([]*property.PhysicalProperty, 2)
	// outer side expected cnt will be amplified by the prop.ExpectedCnt / p.StatsInfo().RowCount with same ratio.
	chReqProps[outerIdx] = &property.PhysicalProperty{TaskTp: property.RootTaskType, ExpectedCnt: math.MaxFloat64,
		SortItems: prop.SortItems, CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: prop.NoCopPushDown}
	orderRatio := p.SCtx().GetSessionVars().OptOrderingIdxSelRatio
	// Record the variable usage for explain explore.
	p.SCtx().GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptOrderingIdxSelRatio)
	outerRowCount := outerStats.RowCount
	estimatedRowCount := p.StatsInfo().RowCount
	if (prop.ExpectedCnt < estimatedRowCount) ||
		(orderRatio > 0 && outerRowCount > estimatedRowCount && prop.ExpectedCnt < outerRowCount && estimatedRowCount > 0) {
		// Apply the orderRatio to recognize that a large outer table scan may
		// read additional rows before the inner table reaches the limit values
		rowsToMeetFirst := max(0.0, (outerRowCount-estimatedRowCount)*orderRatio)
		expCntScale := prop.ExpectedCnt / estimatedRowCount
		expectedCnt := (outerRowCount * expCntScale) + rowsToMeetFirst
		chReqProps[outerIdx].ExpectedCnt = expectedCnt
	}

	// inner side should pass down the indexJoinProp, which contains the runtime constant inner key, which is used to build the underlying index/pk range.
	chReqProps[1-outerIdx] = &property.PhysicalProperty{TaskTp: property.RootTaskType, ExpectedCnt: math.MaxFloat64,
		CTEProducerStatus: prop.CTEProducerStatus, IndexJoinProp: indexJoinProp, NoCopPushDown: prop.NoCopPushDown}

	// for old logic from constructIndexJoin like
	// 1. feeling the keyOff2IdxOffs' -1 and refill the eq condition back to other conditions and adjust inner or outer keys, we
	// move it to completeIndexJoin because it requires the indexJoinInfo which is generated by underlying ds and passed bottom-up
	// within the Task to be filled.
	// 2. extract the eq condition from new other conditions to build the hash join keys, this kind of eq can be used by hash key
	// mapping, we move it to completePhysicalIndexJoin because it requires the indexJoinInfo which is generated by underlying ds
	// and passed bottom-up within the Task to be filled as well.

	baseJoin := BasePhysicalJoin{
		InnerChildIdx:   1 - outerIdx,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		// for static enumeration here, we just pass down the original other conditions
		OtherConditions: p.OtherConditions,
		JoinType:        joinType,
		// for static enumeration here, we just pass down the original outerJoinKeys, innerJoinKeys, isNullEQ
		OuterJoinKeys: outerJoinKeys,
		InnerJoinKeys: innerJoinKeys,
		IsNullEQ:      isNullEQ,
		DefaultValues: p.DefaultValues,
	}

	join := PhysicalIndexJoin{
		BasePhysicalJoin: baseJoin,
		// for static enumeration here, we don't need to fill inner plan anymore.
		// for static enumeration here, the KeyOff2IdxOff, Ranges, CompareFilters, OuterHashKeys, InnerHashKeys are
		// waiting for attach2Task's complement after see the inner plan's indexJoinInfo returned by underlying ds.
		//
		// for static enumeration here, we just pass down the original equal condition for condition adjustment rather
		// depend on the original logical join node.
		EqualConditions: p.EqualConditions,
	}.Init(p.SCtx(), p.StatsInfo().ScaleByExpectCnt(p.SCtx().GetSessionVars(), prop.ExpectedCnt), p.QueryBlockOffset(), chReqProps...)
	join.SetSchema(p.Schema())
	return []base.PhysicalPlan{join}
}

func getIndexJoinSideAndMethod(join base.PhysicalPlan) (innerSide, joinMethod int, ok bool) {
	var innerIdx int
	switch ij := join.(type) {
	case *PhysicalIndexJoin:
		innerIdx = ij.GetInnerChildIdx()
		joinMethod = indexJoinMethod
	case *PhysicalIndexHashJoin:
		innerIdx = ij.GetInnerChildIdx()
		joinMethod = indexHashJoinMethod
	case *PhysicalIndexMergeJoin:
		innerIdx = ij.GetInnerChildIdx()
		joinMethod = indexMergeJoinMethod
	default:
		return 0, 0, false
	}
	ok = true
	innerSide = joinLeft
	if innerIdx == 1 {
		innerSide = joinRight
	}
	return
}

func extractIndexJoinInnerChildPattern(p *logicalop.LogicalJoin, innerChild base.LogicalPlan) *indexJoinInnerChildWrapper {
	wrapper := &indexJoinInnerChildWrapper{}
	nextChild := func(pp base.LogicalPlan) base.LogicalPlan {
		if len(pp.Children()) != 1 {
			return nil
		}
		return pp.Children()[0]
	}
childLoop:
	for curChild := innerChild; curChild != nil; curChild = nextChild(curChild) {
		switch child := curChild.(type) {
		case *logicalop.DataSource:
			wrapper.ds = child
			break childLoop
		case *logicalop.LogicalProjection, *logicalop.LogicalSelection, *logicalop.LogicalAggregation:
			if !p.SCtx().GetSessionVars().EnableINLJoinInnerMultiPattern {
				return nil
			}
			wrapper.zippedChildren = append(wrapper.zippedChildren, child)
		case *logicalop.LogicalUnionScan:
			wrapper.hasDitryWrite = true
			wrapper.zippedChildren = append(wrapper.zippedChildren, child)
		default:
			return nil
		}
	}
	if wrapper.ds == nil || wrapper.ds.PreferStoreType&h.PreferTiFlash != 0 {
		return nil
	}
	return wrapper
}

// buildDataSource2TableScanByIndexJoinProp builds a TableScan as the inner child for an
// IndexJoin if possible.
// If the inner side of an index join is a TableScan, only one tuple will be
// fetched from the inner side for every tuple from the outer side. This will be
// promised to be no worse than building IndexScan as the inner child.
func buildDataSource2TableScanByIndexJoinProp(
	ds *logicalop.DataSource,
	prop *property.PhysicalProperty) base.Task {
	var tblPath *util.AccessPath
	for _, path := range ds.PossibleAccessPaths {
		if path.IsTablePath() && path.StoreType == kv.TiKV { // old logic
			tblPath = path
			break
		}
	}
	if tblPath == nil {
		return base.InvalidTask
	}
	var keyOff2IdxOff []int
	var ranges ranger.MutableRanges = ranger.Ranges{}
	var innerTask base.Task
	var indexJoinResult *physicalop.indexJoinPathResult
	if ds.TableInfo.IsCommonHandle {
		// for the leaf datasource, we use old logic to get the indexJoinResult, which contain the chosen path and ranges.
		indexJoinResult, keyOff2IdxOff = physicalop.getBestIndexJoinPathResultByProp(ds, prop.IndexJoinProp, func(path *util.AccessPath) bool { return path.IsCommonHandlePath })
		// if there is no chosen info, it means the leaf datasource couldn't even leverage this indexJoinProp, return InvalidTask.
		if indexJoinResult == nil {
			return base.InvalidTask
		}
		// prepare the range info with outer join keys, it shows like: [xxx] decided by:
		rangeInfo := physicalop.indexJoinPathRangeInfo(ds.SCtx(), prop.IndexJoinProp.OuterJoinKeys, indexJoinResult)
		// construct the inner task with chosen path and ranges, note: it only for this leaf datasource.
		// like the normal way, we need to check whether the chosen path is matched with the prop, if so, we will set the `keepOrder` to true.
		if isMatchProp(ds, indexJoinResult.chosenPath, prop) {
			innerTask = constructDS2TableScanTask(ds, indexJoinResult.chosenRanges.Range(), rangeInfo, true, !prop.IsSortItemEmpty() && prop.SortItems[0].Desc, prop.IndexJoinProp.AvgInnerRowCnt)
		} else {
			innerTask = constructDS2TableScanTask(ds, indexJoinResult.chosenRanges.Range(), rangeInfo, false, false, prop.IndexJoinProp.AvgInnerRowCnt)
		}
		ranges = indexJoinResult.chosenRanges
	} else {
		var (
			ok               bool
			chosenPath       *util.AccessPath
			newOuterJoinKeys []*expression.Column
			// note: pk col doesn't have mutableRanges, the global var(ranges) which will be handled as empty range in constructIndexJoin.
			localRanges ranger.Ranges
		)
		keyOff2IdxOff, newOuterJoinKeys, localRanges, chosenPath, ok = physicalop.getIndexJoinIntPKPathInfo(ds, prop.IndexJoinProp.InnerJoinKeys, prop.IndexJoinProp.OuterJoinKeys, func(path *util.AccessPath) bool { return path.IsIntHandlePath })
		if !ok {
			return base.InvalidTask
		}
		rangeInfo := physicalop.indexJoinIntPKRangeInfo(ds.SCtx().GetExprCtx().GetEvalCtx(), newOuterJoinKeys)
		if !prop.IsSortItemEmpty() && isMatchProp(ds, chosenPath, prop) {
			innerTask = constructDS2TableScanTask(ds, localRanges, rangeInfo, true, prop.SortItems[0].Desc, prop.IndexJoinProp.AvgInnerRowCnt)
		} else {
			innerTask = constructDS2TableScanTask(ds, localRanges, rangeInfo, false, false, prop.IndexJoinProp.AvgInnerRowCnt)
		}
	}
	// since there is a possibility that inner task can't be built and the returned value is nil, we just return base.InvalidTask.
	if innerTask == nil {
		return base.InvalidTask
	}
	// prepare the index path chosen information and wrap them as IndexJoinInfo and fill back to CopTask.
	// here we don't need to construct physical index join here anymore, because we will encapsulate it bottom-up.
	// chosenPath and lastColManager of indexJoinResult should be returned to the caller (seen by index join to keep
	// index join aware of indexColLens and compareFilters).
	completeIndexJoinFeedBackInfo(innerTask.(*CopTask), indexJoinResult, ranges, keyOff2IdxOff)
	return innerTask
}

// completeIndexJoinFeedBackInfo completes the IndexJoinInfo for the innerTask.
// indexJoin
//
//	+--- outer child
//	+--- inner child (say: projection ------------> unionScan -------------> ds)
//	        <-------RootTask(IndexJoinInfo) <--RootTask(IndexJoinInfo) <--copTask(IndexJoinInfo)
//
// when we build the underlying datasource as table-scan, we will return wrap it and
// return as a CopTask, inside which the index join contains some index path chosen
// information which will be used in indexJoin execution runtime: ref IndexJoinInfo
// declaration for more information.
// the indexJoinInfo will be filled back to the innerTask, passed upward to RootTask
// once this copTask is converted to RootTask type, and finally end up usage in the
// indexJoin's attach2Task with calling completePhysicalIndexJoin.
func completeIndexJoinFeedBackInfo(innerTask *CopTask, indexJoinResult *indexJoinPathResult, ranges ranger.MutableRanges, keyOff2IdxOff []int) {
	info := innerTask.IndexJoinInfo
	if info == nil {
		info = &IndexJoinInfo{}
	}
	if indexJoinResult != nil {
		if indexJoinResult.chosenPath != nil {
			info.IdxColLens = indexJoinResult.chosenPath.IdxColLens
		}
		info.CompareFilters = indexJoinResult.lastColManager
	}
	info.Ranges = ranges
	info.KeyOff2IdxOff = keyOff2IdxOff
	// fill it back to the bottom-up Task.
	innerTask.IndexJoinInfo = info
}

// buildDataSource2IndexScanByIndexJoinProp builds an IndexScan as the inner child for an
// IndexJoin based on IndexJoinProp included in prop if possible.
//
// buildDataSource2IndexScanByIndexJoinProp differs with buildIndexJoinInner2IndexScan in that
// the first one is try to build a single table scan as the inner child of an index join then return
// this inner task(raw table scan) bottom-up, which will be attached with other inner parents of an
// index join in attach2Task when bottom-up of enumerating the physical plans;
//
// while the second is try to build a table scan as the inner child of an index join, then build
// entire inner subtree of a index join out as innerTask instantly according those validated and
// zipped inner patterns with calling constructInnerIndexScanTask. That's not done yet, it also
// tries to enumerate kinds of index join operators based on the finished innerTask and un-decided
// outer child which will be physical-ed in the future.
func buildDataSource2IndexScanByIndexJoinProp(
	ds *logicalop.DataSource,
	prop *property.PhysicalProperty) base.Task {
	indexValid := func(path *util.AccessPath) bool {
		if path.IsTablePath() {
			return false
		}
		// if path is index path. index path currently include two kind of, one is normal, and the other is mv index.
		// for mv index like mvi(a, json, b), if driving condition is a=1, and we build a prefix scan with range [1,1]
		// on mvi, it will return many index rows which breaks handle-unique attribute here.
		//
		// the basic rule is that: mv index can be and can only be accessed by indexMerge operator. (embedded handle duplication)
		if !isMVIndexPath(path) {
			return true // not a MVIndex path, it can successfully be index join probe side.
		}
		return false
	}
	indexJoinResult, keyOff2IdxOff := physicalop.getBestIndexJoinPathResultByProp(ds, prop.IndexJoinProp, indexValid)
	if indexJoinResult == nil {
		return base.InvalidTask
	}
	rangeInfo := physicalop.indexJoinPathRangeInfo(ds.SCtx(), prop.IndexJoinProp.OuterJoinKeys, indexJoinResult)
	maxOneRow := false
	if indexJoinResult.chosenPath.Index.Unique && indexJoinResult.usedColsLen == len(indexJoinResult.chosenPath.FullIdxCols) {
		l := len(indexJoinResult.chosenAccess)
		if l == 0 {
			maxOneRow = true
		} else {
			sf, ok := indexJoinResult.chosenAccess[l-1].(*expression.ScalarFunction)
			maxOneRow = ok && (sf.FuncName.L == ast.EQ)
		}
	}
	var innerTask base.Task
	if !prop.IsSortItemEmpty() && isMatchProp(ds, indexJoinResult.chosenPath, prop) {
		innerTask = constructDS2IndexScanTask(ds, indexJoinResult.chosenPath, indexJoinResult.chosenRanges.Range(), indexJoinResult.chosenRemained, indexJoinResult.idxOff2KeyOff, rangeInfo, true, prop.SortItems[0].Desc, prop.IndexJoinProp.AvgInnerRowCnt, maxOneRow)
	} else {
		innerTask = constructDS2IndexScanTask(ds, indexJoinResult.chosenPath, indexJoinResult.chosenRanges.Range(), indexJoinResult.chosenRemained, indexJoinResult.idxOff2KeyOff, rangeInfo, false, false, prop.IndexJoinProp.AvgInnerRowCnt, maxOneRow)
	}
	// since there is a possibility that inner task can't be built and the returned value is nil, we just return base.InvalidTask.
	if innerTask == nil {
		return base.InvalidTask
	}
	// prepare the index path chosen information and wrap them as IndexJoinInfo and fill back to CopTask.
	// here we don't need to construct physical index join here anymore, because we will encapsulate it bottom-up.
	// chosenPath and lastColManager of indexJoinResult should be returned to the caller (seen by index join to keep
	// index join aware of indexColLens and compareFilters).
	completeIndexJoinFeedBackInfo(innerTask.(*CopTask), indexJoinResult, indexJoinResult.chosenRanges, keyOff2IdxOff)
	return innerTask
}

// constructInnerTableScanTask is specially used to construct the inner plan for PhysicalIndexJoin.
func constructInnerTableScanTask(
	p *logicalop.LogicalJoin,
	prop *property.PhysicalProperty,
	wrapper *indexJoinInnerChildWrapper,
	ranges ranger.Ranges,
	rangeInfo string,
	keepOrder bool,
	desc bool,
	rowCount float64,
) base.Task {
	copTask := constructDS2TableScanTask(wrapper.ds, ranges, rangeInfo, keepOrder, desc, rowCount)
	if copTask == nil {
		return nil
	}
	return constructIndexJoinInnerSideTaskWithAggCheck(p, prop, copTask.(*CopTask), wrapper.ds, nil, wrapper)
}

// constructInnerIndexScanTask is specially used to construct the inner plan for PhysicalIndexJoin.
func constructInnerIndexScanTask(
	p *logicalop.LogicalJoin,
	prop *property.PhysicalProperty,
	wrapper *indexJoinInnerChildWrapper,
	path *util.AccessPath,
	ranges ranger.Ranges,
	filterConds []expression.Expression,
	idxOffset2joinKeyOffset []int,
	rangeInfo string,
	keepOrder bool,
	desc bool,
	rowCount float64,
	maxOneRow bool,
) base.Task {
	copTask := constructDS2IndexScanTask(wrapper.ds, path, ranges, filterConds, idxOffset2joinKeyOffset, rangeInfo, keepOrder, desc, rowCount, maxOneRow)
	if copTask == nil {
		return nil
	}
	return constructIndexJoinInnerSideTaskWithAggCheck(p, prop, copTask.(*CopTask), wrapper.ds, path, wrapper)
}
