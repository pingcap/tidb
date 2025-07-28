package physicalop

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
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

	innerPlan base.PhysicalPlan

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
	if p.innerPlan != nil {
		cloned.innerPlan, err = p.innerPlan.Clone(newCtx)
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
	if p.innerPlan != nil {
		sum += p.innerPlan.MemoryUsage()
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

// ExplainJoinLeftSide is to explain the left side of join.
func ExplainJoinLeftSide(buffer *strings.Builder, isInnerJoin bool, normalized bool, leftSide base.PhysicalPlan) {
	if !isInnerJoin {
		buffer.WriteString(", left side:")
		if normalized {
			buffer.WriteString(leftSide.TP())
		} else {
			buffer.WriteString(leftSide.ExplainID().String())
		}
	}
}

func (p *PhysicalIndexJoin) explainInfo(normalized bool, isIndexMergeJoin bool) string {
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
	ExplainJoinLeftSide(buffer, p.JoinType.IsInnerJoin(), normalized, p.Children()[0])
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
	return p.explainInfo(true, false)
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexJoin) ExplainInfo() string {
	return p.explainInfo(false, false)
}

// CloneForPlanCache implements the base.Plan interface.
func (op *PhysicalIndexJoin) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalIndexJoin)
	*cloned = *op
	basePlan, baseOK := op.BasePhysicalJoin.CloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.BasePhysicalJoin = *basePlan
	if op.innerPlan != nil {
		innerPlan, ok := op.innerPlan.CloneForPlanCache(newCtx)
		if !ok {
			return nil, false
		}
		cloned.innerPlan = innerPlan.(base.PhysicalPlan)
	}
	cloned.Ranges = op.Ranges.CloneForPlanCache()
	cloned.KeyOff2IdxOff = make([]int, len(op.KeyOff2IdxOff))
	copy(cloned.KeyOff2IdxOff, op.KeyOff2IdxOff)
	cloned.IdxColLens = make([]int, len(op.IdxColLens))
	copy(cloned.IdxColLens, op.IdxColLens)
	cloned.CompareFilters = op.CompareFilters.cloneForPlanCache()
	cloned.OuterHashKeys = utilfuncp.CloneColumnsForPlanCache(op.OuterHashKeys, nil)
	cloned.InnerHashKeys = utilfuncp.CloneColumnsForPlanCache(op.InnerHashKeys, nil)
	return cloned, true
}

// GetCost computes the cost of index join operator and its children.
func (p *PhysicalIndexJoin) GetCost(outerCnt, innerCnt, outerCost, innerCost float64, costFlag uint64) float64 {
	var cpuCost float64
	sessVars := p.SCtx().GetSessionVars()
	// Add the cost of evaluating outer filter, since inner filter of index join
	// is always empty, we can simply tell whether outer filter is empty using the
	// summed length of left/right conditions.
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		cpuCost += sessVars.GetCPUFactor() * outerCnt
		outerCnt *= cost.SelectionFactor
	}
	// Cost of extracting lookup keys.
	innerCPUCost := sessVars.GetCPUFactor() * outerCnt
	// Cost of sorting and removing duplicate lookup keys:
	// (outerCnt / batchSize) * (batchSize * Log2(batchSize) + batchSize) * CPUFactor
	batchSize := math.Min(float64(p.SCtx().GetSessionVars().IndexJoinBatchSize), outerCnt)
	if batchSize > 2 {
		innerCPUCost += outerCnt * (math.Log2(batchSize) + 1) * sessVars.GetCPUFactor()
	}
	// Add cost of building inner executors. CPU cost of building copTasks:
	// (outerCnt / batchSize) * (batchSize * DistinctFactor) * CPUFactor
	// Since we don't know the number of copTasks built, ignore these network cost now.
	innerCPUCost += outerCnt * cost.DistinctFactor * sessVars.GetCPUFactor()
	// CPU cost of building hash table for inner results:
	// (outerCnt / batchSize) * (batchSize * DistinctFactor) * innerCnt * CPUFactor
	innerCPUCost += outerCnt * cost.DistinctFactor * innerCnt * sessVars.GetCPUFactor()
	innerConcurrency := float64(p.SCtx().GetSessionVars().IndexLookupJoinConcurrency())
	cpuCost += innerCPUCost / innerConcurrency
	// Cost of probing hash table in main thread.
	numPairs := outerCnt * innerCnt
	if p.JoinType == logicalop.SemiJoin || p.JoinType == logicalop.AntiSemiJoin ||
		p.JoinType == logicalop.LeftOuterSemiJoin || p.JoinType == logicalop.AntiLeftOuterSemiJoin {
		if len(p.OtherConditions) > 0 {
			numPairs *= 0.5
		} else {
			numPairs = 0
		}
	}
	if costusage.HasCostFlag(costFlag, costusage.CostFlagUseTrueCardinality) {
		numPairs = getOperatorActRows(p)
	}
	probeCost := numPairs * sessVars.GetCPUFactor()
	// Cost of additional concurrent goroutines.
	cpuCost += probeCost + (innerConcurrency+1.0)*sessVars.GetConcurrencyFactor()
	// Memory cost of hash tables for inner rows. The computed result is the upper bound,
	// since the executor is pipelined and not all workers are always in full load.
	memoryCost := innerConcurrency * (batchSize * cost.DistinctFactor) * innerCnt * sessVars.GetMemoryFactor()
	// Cost of inner child plan, i.e, mainly I/O and network cost.
	innerPlanCost := outerCnt * innerCost
	if p.SCtx().GetSessionVars().CostModelVersion == 2 {
		// IndexJoin executes a batch of rows at a time, so the actual cost of this part should be
		//  `innerCostPerBatch * numberOfBatches` instead of `innerCostPerRow * numberOfOuterRow`.
		// Use an empirical value batchRatio to handle this now.
		// TODO: remove this empirical value.
		batchRatio := 30.0
		innerPlanCost /= batchRatio
	}
	return outerCost + innerPlanCost + cpuCost + memoryCost
}

// GetPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexJoin) GetPlanCostVer1(taskType property.TaskType, option *optimizetrace.PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.PlanCostInit && !hasCostFlag(costFlag, costusage.CostFlagRecalculate) {
		return p.PlanCost, nil
	}
	outerChild, innerChild := p.Children()[1-p.InnerChildIdx], p.Children()[p.InnerChildIdx]
	outerCost, err := outerChild.GetPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	innerCost, err := innerChild.GetPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	outerCnt := getCardinality(outerChild, costFlag)
	innerCnt := getCardinality(innerChild, costFlag)
	if hasCostFlag(costFlag, costusage.CostFlagUseTrueCardinality) && outerCnt > 0 {
		innerCnt /= outerCnt // corresponding to one outer row when calculating IndexJoin costs
		innerCost /= outerCnt
	}
	p.PlanCost = p.GetCost(outerCnt, innerCnt, outerCost, innerCost, costFlag)
	p.PlanCostInit = true
	return p.PlanCost, nil
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
