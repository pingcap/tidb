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
	"fmt"
	"math"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/join/joinversion"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
)

// CanUseHashJoinV2 returns true if current join is supported by hash join v2
func CanUseHashJoinV2(joinType base.JoinType, leftJoinKeys []*expression.Column, isNullEQ []bool, leftNAJoinKeys []*expression.Column) bool {
	if !IsGAForHashJoinV2(joinType, leftJoinKeys, isNullEQ, leftNAJoinKeys) && !joinversion.UseHashJoinV2ForNonGAJoin {
		return false
	}
	switch joinType {
	case base.LeftOuterJoin, base.RightOuterJoin, base.InnerJoin, base.LeftOuterSemiJoin,
		base.SemiJoin, base.AntiSemiJoin, base.AntiLeftOuterSemiJoin:
		// null aware join is not supported yet
		if len(leftNAJoinKeys) > 0 {
			return false
		}
		// cross join is not supported
		if len(leftJoinKeys) == 0 {
			return false
		}
		// NullEQ is not supported yet
		for _, value := range isNullEQ {
			if value {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// IsGAForHashJoinV2 judges if this hash join is GA
func IsGAForHashJoinV2(joinType base.JoinType, leftJoinKeys []*expression.Column, isNullEQ []bool, leftNAJoinKeys []*expression.Column) bool {
	// nullaware join
	if len(leftNAJoinKeys) > 0 {
		return false
	}
	// cross join
	if len(leftJoinKeys) == 0 {
		return false
	}
	// join with null equal condition
	for _, value := range isNullEQ {
		if value {
			return false
		}
	}
	switch joinType {
	case base.LeftOuterJoin, base.RightOuterJoin, base.InnerJoin, base.AntiSemiJoin, base.SemiJoin:
		return true
	default:
		return false
	}
}

// PhysicalHashJoin represents hash join implementation of LogicalJoin.
type PhysicalHashJoin struct {
	BasePhysicalJoin

	Concurrency     uint
	EqualConditions []*expression.ScalarFunction

	// null aware equal conditions
	NAEqualConditions []*expression.ScalarFunction

	// use the outer table to build a hash table when the outer table is smaller.
	UseOuterToBuild bool

	// on which store the join executes.
	StoreTp        kv.StoreType
	MppShuffleJoin bool

	// for runtime filter
	runtimeFilterList []*RuntimeFilter `plan-cache-clone:"must-nil"` // plan with runtime filter is not cached
}

// NewPhysicalHashJoin creates a new PhysicalHashJoin from LogicalJoin.
func NewPhysicalHashJoin(p *logicalop.LogicalJoin, innerIdx int, useOuterToBuild bool, newStats *property.StatsInfo, prop ...*property.PhysicalProperty) *PhysicalHashJoin {
	leftJoinKeys, rightJoinKeys, isNullEQ, _ := p.GetJoinKeys()
	leftNAJoinKeys, rightNAJoinKeys := p.GetNAJoinKeys()
	baseJoin := BasePhysicalJoin{
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		LeftJoinKeys:    leftJoinKeys,
		RightJoinKeys:   rightJoinKeys,
		// NA join keys
		LeftNAJoinKeys:  leftNAJoinKeys,
		RightNAJoinKeys: rightNAJoinKeys,
		IsNullEQ:        isNullEQ,
		JoinType:        p.JoinType,
		DefaultValues:   p.DefaultValues,
		InnerChildIdx:   innerIdx,
	}
	hashJoin := PhysicalHashJoin{
		BasePhysicalJoin:  baseJoin,
		EqualConditions:   p.EqualConditions,
		NAEqualConditions: p.NAEQConditions,
		Concurrency:       uint(p.SCtx().GetSessionVars().HashJoinConcurrency()),
		UseOuterToBuild:   useOuterToBuild,
	}.Init(p.SCtx(), newStats, p.QueryBlockOffset(), prop...)
	return hashJoin
}

// Init initializes PhysicalHashJoin.
func (p PhysicalHashJoin) Init(ctx base.PlanContext, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalHashJoin {
	tp := plancodec.TypeHashJoin
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, tp, &p, offset)
	p.SetChildrenReqProps(props)
	p.SetStats(stats)
	return &p
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalHashJoin) Attach2Task(tasks ...base.Task) base.Task {
	return utilfuncp.Attach2Task4PhysicalHashJoin(p, tasks...)
}

// CanUseHashJoinV2 returns true if current join is supported by hash join v2
func (p *PhysicalHashJoin) CanUseHashJoinV2() bool {
	return CanUseHashJoinV2(p.JoinType, p.LeftJoinKeys, p.IsNullEQ, p.LeftNAJoinKeys)
}

// CanTiFlashUseHashJoinV2 returns if current join is supported by hash join v2 in TiFlash
func (p *PhysicalHashJoin) CanTiFlashUseHashJoinV2(sctx base.PlanContext) bool {
	vars := sctx.GetSessionVars()
	if !joinversion.IsOptimizedVersion(vars.TiFlashHashJoinVersion) {
		return false
	}
	// spill is not supported yet
	if vars.TiFlashMaxBytesBeforeExternalJoin > 0 || (vars.TiFlashMaxQueryMemoryPerNode > 0 && vars.TiFlashQuerySpillRatio > 0) {
		return false
	}
	switch p.JoinType {
	case base.InnerJoin:
		// null aware join is not supported yet
		if len(p.LeftNAJoinKeys) > 0 {
			return false
		}
		// cross join is not supported
		if len(p.LeftJoinKeys) == 0 {
			return false
		}
		// NullEQ is not supported yet
		for _, value := range p.IsNullEQ {
			if value {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalHashJoin) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalHashJoin)
	cloned.SetSCtx(newCtx)
	base, err := p.BasePhysicalJoin.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.BasePhysicalJoin = *base
	cloned.Concurrency = p.Concurrency
	cloned.UseOuterToBuild = p.UseOuterToBuild
	for _, c := range p.EqualConditions {
		cloned.EqualConditions = append(cloned.EqualConditions, c.Clone().(*expression.ScalarFunction))
	}
	for _, c := range p.NAEqualConditions {
		cloned.NAEqualConditions = append(cloned.NAEqualConditions, c.Clone().(*expression.ScalarFunction))
	}
	for _, rf := range p.runtimeFilterList {
		clonedRF := rf.Clone()
		cloned.runtimeFilterList = append(cloned.runtimeFilterList, clonedRF)
	}
	return cloned, nil
}

// ExplainInfo implements Plan interface.
func (p *PhysicalHashJoin) ExplainInfo() string {
	return p.explainInfo(false)
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalHashJoin) ExplainNormalizedInfo() string {
	return p.explainInfo(true)
}

func (p *PhysicalHashJoin) explainInfo(normalized bool) string {
	sortedExplainExpressionList := expression.SortedExplainExpressionList
	if normalized {
		sortedExplainExpressionList = func(_ expression.EvalContext, exprs []expression.Expression) []byte {
			return expression.SortedExplainNormalizedExpressionList(exprs)
		}
	}
	redact := p.SCtx().GetSessionVars().EnableRedactLog
	buffer := new(strings.Builder)

	if len(p.EqualConditions) == 0 {
		if len(p.NAEqualConditions) == 0 {
			buffer.WriteString("CARTESIAN ")
		} else {
			buffer.WriteString("Null-aware ")
		}
	}

	buffer.WriteString(p.JoinType.String())
	explainJoinLeftSide(buffer, p.JoinType.IsInnerJoin(), normalized, p.Children()[0])
	evalCtx := p.SCtx().GetExprCtx().GetEvalCtx()
	if len(p.EqualConditions) > 0 {
		if normalized {
			buffer.WriteString(", equal:")
			buffer.Write(expression.SortedExplainNormalizedScalarFuncList(p.EqualConditions))
		} else {
			buffer.WriteString(", equal:[")
			for i, EqualConditions := range p.EqualConditions {
				if i != 0 {
					buffer.WriteString(" ")
				}
				buffer.WriteString(EqualConditions.StringWithCtx(evalCtx, redact))
			}
			buffer.WriteString("]")
		}
	}
	if len(p.NAEqualConditions) > 0 {
		if normalized {
			buffer.WriteString(", equal:")
			buffer.Write(expression.SortedExplainNormalizedScalarFuncList(p.NAEqualConditions))
		} else {
			buffer.WriteString(", equal:[")
			for i, NAEqualCondition := range p.NAEqualConditions {
				if i != 0 {
					buffer.WriteString(" ")
				}
				buffer.WriteString(NAEqualCondition.StringWithCtx(evalCtx, redact))
			}
			buffer.WriteString("]")
		}
	}
	if len(p.LeftConditions) > 0 {
		if normalized {
			buffer.WriteString(", left cond:")
			buffer.Write(expression.SortedExplainNormalizedExpressionList(p.LeftConditions))
		} else {
			buffer.WriteString(", left cond:[")
			for i, LeftConditions := range p.LeftConditions {
				if i != 0 {
					buffer.WriteString(" ")
				}
				buffer.WriteString(LeftConditions.StringWithCtx(evalCtx, redact))
			}
			buffer.WriteString("]")
		}
	}
	if len(p.RightConditions) > 0 {
		buffer.WriteString(", right cond:")
		buffer.Write(sortedExplainExpressionList(evalCtx, p.RightConditions))
	}
	if len(p.OtherConditions) > 0 {
		buffer.WriteString(", other cond:")
		buffer.Write(sortedExplainExpressionList(evalCtx, p.OtherConditions))
	}
	if p.TiFlashFineGrainedShuffleStreamCount > 0 {
		fmt.Fprintf(buffer, ", stream_count: %d", p.TiFlashFineGrainedShuffleStreamCount)
	}

	// for runtime filter
	if len(p.runtimeFilterList) > 0 {
		buffer.WriteString(", runtime filter:")
		for i, runtimeFilter := range p.runtimeFilterList {
			if i != 0 {
				buffer.WriteString(", ")
			}
			buffer.WriteString(runtimeFilter.ExplainInfo(true))
		}
	}
	return buffer.String()
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *PhysicalHashJoin) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.EqualConditions)+len(p.NAEqualConditions)+len(p.LeftConditions)+len(p.RightConditions)+len(p.OtherConditions))
	for _, fun := range p.EqualConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	for _, fun := range p.NAEqualConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	for _, fun := range p.LeftConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	for _, fun := range p.RightConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	for _, fun := range p.OtherConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	return corCols
}

// GetCost computes cost of hash join operator itself.
func (p *PhysicalHashJoin) GetCost(lCnt, rCnt float64, _ bool, costFlag uint64, op *optimizetrace.PhysicalOptimizeOp) float64 {
	return utilfuncp.GetCost4PhysicalHashJoin(p, lCnt, rCnt, costFlag, op)
}

// GetPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalHashJoin) GetPlanCostVer1(taskType property.TaskType,
	option *optimizetrace.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer14PhysicalHashJoin(p, taskType, option)
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + probe-child-cost +
// build-hash-cost + build-filter-cost +
// (probe-filter-cost + probe-hash-cost) / concurrency
func (p *PhysicalHashJoin) GetPlanCostVer2(taskType property.TaskType,
	option *optimizetrace.PlanCostOption, _ ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetPlanCostVer24PhysicalHashJoin(p, taskType, option)
}

// MemoryUsage return the memory usage of PhysicalHashJoin
func (p *PhysicalHashJoin) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.BasePhysicalJoin.MemoryUsage() + size.SizeOfUint + size.SizeOfSlice + size.SizeOfBool*2 + size.SizeOfUint8

	for _, expr := range p.EqualConditions {
		sum += expr.MemoryUsage()
	}
	for _, expr := range p.NAEqualConditions {
		sum += expr.MemoryUsage()
	}
	return
}

// RightIsBuildSide return true when right side is build side
func (p *PhysicalHashJoin) RightIsBuildSide() bool {
	if p.UseOuterToBuild {
		return p.InnerChildIdx == 0
	}
	return p.InnerChildIdx != 0
}

// ResolveIndicesItself resolve indices for PhyicalPlan itself
func (p *PhysicalHashJoin) ResolveIndicesItself() (err error) {
	lSchema := p.Children()[0].Schema()
	rSchema := p.Children()[1].Schema()
	ctx := p.SCtx()
	for i, fun := range p.EqualConditions {
		lArg, err := fun.GetArgs()[0].ResolveIndices(lSchema)
		if err != nil {
			return err
		}
		p.LeftJoinKeys[i] = lArg.(*expression.Column)
		rArg, err := fun.GetArgs()[1].ResolveIndices(rSchema)
		if err != nil {
			return err
		}
		p.RightJoinKeys[i] = rArg.(*expression.Column)
		p.EqualConditions[i] = expression.NewFunctionInternal(ctx.GetExprCtx(), fun.FuncName.L, fun.GetStaticType(), lArg, rArg).(*expression.ScalarFunction)
	}
	for i, fun := range p.NAEqualConditions {
		lArg, err := fun.GetArgs()[0].ResolveIndices(lSchema)
		if err != nil {
			return err
		}
		p.LeftNAJoinKeys[i] = lArg.(*expression.Column)
		rArg, err := fun.GetArgs()[1].ResolveIndices(rSchema)
		if err != nil {
			return err
		}
		p.RightNAJoinKeys[i] = rArg.(*expression.Column)
		p.NAEqualConditions[i] = expression.NewFunctionInternal(ctx.GetExprCtx(), fun.FuncName.L, fun.GetStaticType(), lArg, rArg).(*expression.ScalarFunction)
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

	// Here we want to resolve all join schema columns directly as a merged schema, and you know same name
	// col in join schema should be separately redirected to corresponded same col in child schema. But two
	// column sets are **NOT** always ordered, see comment: https://github.com/pingcap/tidb/pull/45831#discussion_r1481031471
	// we are using mapping mechanism instead of moving j forward.
	marked := make([]bool, mergedSchema.Len())
	for i := range colsNeedResolving {
		findIdx := -1
		for j := range mergedSchema.Columns {
			if !p.Schema().Columns[i].EqualColumn(mergedSchema.Columns[j]) || marked[j] {
				continue
			}
			// resolve to a same unique id one, and it not being marked.
			findIdx = j
			break
		}
		if findIdx != -1 {
			// valid one.
			p.Schema().Columns[i] = p.Schema().Columns[i].Clone().(*expression.Column)
			p.Schema().Columns[i].Index = findIdx
			marked[findIdx] = true
			foundCnt++
		}
	}
	if foundCnt < colsNeedResolving {
		return errors.Errorf("Some columns of %v cannot find the reference from its child(ren)", p.ExplainID().String())
	}
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalHashJoin) ResolveIndices() (err error) {
	err = p.PhysicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	return p.ResolveIndicesItself()
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalHashJoin) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	client := ctx.GetClient()

	if len(p.LeftJoinKeys) > 0 && len(p.LeftNAJoinKeys) > 0 {
		return nil, errors.Errorf("join key and na join key can not both exist")
	}

	isNullAwareSemiJoin := len(p.LeftNAJoinKeys) > 0
	var leftJoinKeys, rightJoinKeys []*expression.Column
	if isNullAwareSemiJoin {
		leftJoinKeys = p.LeftNAJoinKeys
		rightJoinKeys = p.RightNAJoinKeys
	} else {
		leftJoinKeys = p.LeftJoinKeys
		rightJoinKeys = p.RightJoinKeys
	}

	leftKeys := make([]expression.Expression, 0, len(leftJoinKeys))
	rightKeys := make([]expression.Expression, 0, len(rightJoinKeys))
	for _, leftKey := range leftJoinKeys {
		leftKeys = append(leftKeys, leftKey)
	}
	for _, rightKey := range rightJoinKeys {
		rightKeys = append(rightKeys, rightKey)
	}

	lChildren, err := p.Children()[0].ToPB(ctx, storeType)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rChildren, err := p.Children()[1].ToPB(ctx, storeType)
	if err != nil {
		return nil, errors.Trace(err)
	}

	evalCtx := ctx.GetExprCtx().GetEvalCtx()
	left, err := expression.ExpressionsToPBList(evalCtx, leftKeys, client)
	if err != nil {
		return nil, err
	}
	right, err := expression.ExpressionsToPBList(evalCtx, rightKeys, client)
	if err != nil {
		return nil, err
	}

	leftConditions, err := expression.ExpressionsToPBList(evalCtx, p.LeftConditions, client)
	if err != nil {
		return nil, err
	}
	rightConditions, err := expression.ExpressionsToPBList(evalCtx, p.RightConditions, client)
	if err != nil {
		return nil, err
	}

	var otherConditionsInJoin expression.CNFExprs
	var otherEqConditionsFromIn expression.CNFExprs
	/// For anti join, equal conditions from `in` clause requires additional processing,
	/// for example, treat `null` as true.
	if p.JoinType == base.AntiSemiJoin || p.JoinType == base.AntiLeftOuterSemiJoin || p.JoinType == base.LeftOuterSemiJoin {
		for _, condition := range p.OtherConditions {
			if expression.IsEQCondFromIn(condition) {
				otherEqConditionsFromIn = append(otherEqConditionsFromIn, condition)
			} else {
				otherConditionsInJoin = append(otherConditionsInJoin, condition)
			}
		}
	} else {
		otherConditionsInJoin = p.OtherConditions
	}
	otherConditions, err := expression.ExpressionsToPBList(evalCtx, otherConditionsInJoin, client)
	if err != nil {
		return nil, err
	}
	otherEqConditions, err := expression.ExpressionsToPBList(evalCtx, otherEqConditionsFromIn, client)
	if err != nil {
		return nil, err
	}

	pbJoinType := tipb.JoinType_TypeInnerJoin
	switch p.JoinType {
	case base.LeftOuterJoin:
		pbJoinType = tipb.JoinType_TypeLeftOuterJoin
	case base.RightOuterJoin:
		pbJoinType = tipb.JoinType_TypeRightOuterJoin
	case base.SemiJoin:
		pbJoinType = tipb.JoinType_TypeSemiJoin
	case base.AntiSemiJoin:
		pbJoinType = tipb.JoinType_TypeAntiSemiJoin
	case base.LeftOuterSemiJoin:
		pbJoinType = tipb.JoinType_TypeLeftOuterSemiJoin
	case base.AntiLeftOuterSemiJoin:
		pbJoinType = tipb.JoinType_TypeAntiLeftOuterSemiJoin
	}

	var equalConditions []*expression.ScalarFunction
	if isNullAwareSemiJoin {
		equalConditions = p.NAEqualConditions
	} else {
		equalConditions = p.EqualConditions
	}
	probeFiledTypes := make([]*tipb.FieldType, 0, len(equalConditions))
	buildFiledTypes := make([]*tipb.FieldType, 0, len(equalConditions))
	for _, equalCondition := range equalConditions {
		retType := equalCondition.RetType.Clone()
		chs, coll := equalCondition.CharsetAndCollation()
		retType.SetCharset(chs)
		retType.SetCollate(coll)
		ty, err := expression.ToPBFieldTypeWithCheck(retType, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		probeFiledTypes = append(probeFiledTypes, ty)
		buildFiledTypes = append(buildFiledTypes, ty)
	}

	// runtime filter
	rfListPB, err := RuntimeFilterListToPB(ctx, p.runtimeFilterList, client)
	if err != nil {
		return nil, errors.Trace(err)
	}
	join := &tipb.Join{
		JoinType:                pbJoinType,
		JoinExecType:            tipb.JoinExecType_TypeHashJoin,
		InnerIdx:                int64(p.InnerChildIdx),
		LeftJoinKeys:            left,
		RightJoinKeys:           right,
		ProbeTypes:              probeFiledTypes,
		BuildTypes:              buildFiledTypes,
		LeftConditions:          leftConditions,
		RightConditions:         rightConditions,
		OtherConditions:         otherConditions,
		OtherEqConditionsFromIn: otherEqConditions,
		Children:                []*tipb.Executor{lChildren, rChildren},
		IsNullAwareSemiJoin:     &isNullAwareSemiJoin,
		RuntimeFilterList:       rfListPB,
	}

	executorID := p.ExplainID().String()
	return &tipb.Executor{
		Tp:                            tipb.ExecType_TypeJoin,
		Join:                          join,
		ExecutorId:                    &executorID,
		FineGrainedShuffleStreamCount: p.TiFlashFineGrainedShuffleStreamCount,
		FineGrainedShuffleBatchSize:   ctx.TiFlashFineGrainedShuffleBatchSize,
	}, nil
}

func tryToGetMppHashJoin(super base.LogicalPlan, prop *property.PhysicalProperty, useBCJ bool) []base.PhysicalPlan {
	ge, p := base.GetGEAndLogical[*logicalop.LogicalJoin](super)
	if !prop.IsSortItemEmpty() {
		return nil
	}
	if prop.TaskTp != property.RootTaskType && prop.TaskTp != property.MppTaskType {
		return nil
	}

	if !expression.IsPushDownEnabled(p.JoinType.String(), kv.TiFlash) {
		p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because join type `" + p.JoinType.String() + "` is blocked by blacklist, check `table mysql.expr_pushdown_blacklist;` for more information.")
		return nil
	}

	if p.JoinType != base.InnerJoin && p.JoinType != base.LeftOuterJoin && p.JoinType != base.RightOuterJoin && p.JoinType != base.SemiJoin && p.JoinType != base.AntiSemiJoin && p.JoinType != base.LeftOuterSemiJoin && p.JoinType != base.AntiLeftOuterSemiJoin {
		p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because join type `" + p.JoinType.String() + "` is not supported now.")
		return nil
	}

	if len(p.EqualConditions) == 0 {
		if !useBCJ {
			p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because `Cartesian Product` is only supported by broadcast join, check value and documents of variables `tidb_broadcast_join_threshold_size` and `tidb_broadcast_join_threshold_count`.")
			return nil
		}
		if p.SCtx().GetSessionVars().AllowCartesianBCJ == 0 {
			p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because `Cartesian Product` is only supported by broadcast join, check value and documents of variable `tidb_opt_broadcast_cartesian_join`.")
			return nil
		}
	}
	if len(p.LeftConditions) != 0 && p.JoinType != base.LeftOuterJoin {
		p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because there is a join that is not `left join` but has left conditions, which is not supported by mpp now, see github.com/pingcap/tidb/issues/26090 for more information.")
		return nil
	}
	if len(p.RightConditions) != 0 && p.JoinType != base.RightOuterJoin {
		p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because there is a join that is not `right join` but has right conditions, which is not supported by mpp now.")
		return nil
	}

	if prop.MPPPartitionTp == property.BroadcastType {
		return nil
	}
	if !canExprsInJoinPushdown(p, kv.TiFlash) {
		return nil
	}
	lkeys, rkeys, _, _ := p.GetJoinKeys()
	lNAkeys, rNAKeys := p.GetNAJoinKeys()
	// check match property
	baseJoin := BasePhysicalJoin{
		JoinType:        p.JoinType,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		DefaultValues:   p.DefaultValues,
		LeftJoinKeys:    lkeys,
		RightJoinKeys:   rkeys,
		LeftNAJoinKeys:  lNAkeys,
		RightNAJoinKeys: rNAKeys,
	}
	// It indicates which side is the build side.
	forceLeftToBuild := ((p.PreferJoinType & h.PreferLeftAsHJBuild) > 0) || ((p.PreferJoinType & h.PreferRightAsHJProbe) > 0)
	forceRightToBuild := ((p.PreferJoinType & h.PreferRightAsHJBuild) > 0) || ((p.PreferJoinType & h.PreferLeftAsHJProbe) > 0)
	if forceLeftToBuild && forceRightToBuild {
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(
			"Some HASH_JOIN_BUILD and HASH_JOIN_PROBE hints are conflicts, please check the hints")
		forceLeftToBuild = false
		forceRightToBuild = false
	}
	preferredBuildIndex := 0
	fixedBuildSide := false // Used to indicate whether the build side for the MPP join is fixed or not.
	var stats0, stats1 *property.StatsInfo
	if ge != nil {
		stats0, stats1, _, _ = ge.GetJoinChildStatsAndSchema()
	} else {
		stats0, stats1, _, _ = p.GetJoinChildStatsAndSchema()
	}
	if p.JoinType == base.InnerJoin {
		if stats0.Count() > stats1.Count() {
			preferredBuildIndex = 1
		}
	} else if p.JoinType.IsSemiJoin() {
		if !useBCJ && !p.IsNAAJ() && len(p.EqualConditions) > 0 && (p.JoinType == base.SemiJoin || p.JoinType == base.AntiSemiJoin) {
			// TiFlash only supports Non-null_aware non-cross semi/anti_semi join to use both sides as build side
			preferredBuildIndex = 1
			// MPPOuterJoinFixedBuildSide default value is false
			// use MPPOuterJoinFixedBuildSide here as a way to disable using left table as build side
			if !p.SCtx().GetSessionVars().MPPOuterJoinFixedBuildSide && stats1.Count() > stats0.Count() {
				preferredBuildIndex = 0
			}
		} else {
			preferredBuildIndex = 1
			fixedBuildSide = true
		}
	}
	if p.JoinType == base.LeftOuterJoin || p.JoinType == base.RightOuterJoin {
		// TiFlash does not require that the build side must be the inner table for outer join.
		// so we can choose the build side based on the row count, except that:
		// 1. it is a broadcast join(for broadcast join, it makes sense to use the broadcast side as the build side)
		// 2. or session variable MPPOuterJoinFixedBuildSide is set to true
		// 3. or nullAware/cross joins
		if useBCJ || p.IsNAAJ() || len(p.EqualConditions) == 0 || p.SCtx().GetSessionVars().MPPOuterJoinFixedBuildSide {
			if !p.SCtx().GetSessionVars().MPPOuterJoinFixedBuildSide {
				// The hint has higher priority than variable.
				fixedBuildSide = true
			}
			if p.JoinType == base.LeftOuterJoin {
				preferredBuildIndex = 1
			}
		} else if stats0.Count() > stats1.Count() {
			preferredBuildIndex = 1
		}
	}

	if forceLeftToBuild || forceRightToBuild {
		match := (forceLeftToBuild && preferredBuildIndex == 0) || (forceRightToBuild && preferredBuildIndex == 1)
		if !match {
			if fixedBuildSide {
				// A warning will be generated if the build side is fixed, but we attempt to change it using the hint.
				p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(
					"Some HASH_JOIN_BUILD and HASH_JOIN_PROBE hints cannot be utilized for MPP joins, please check the hints")
			} else {
				// The HASH_JOIN_BUILD OR HASH_JOIN_PROBE hints can take effective.
				preferredBuildIndex = 1 - preferredBuildIndex
			}
		}
	}

	// set preferredBuildIndex for test
	failpoint.Inject("mockPreferredBuildIndex", func(val failpoint.Value) {
		if !p.SCtx().GetSessionVars().InRestrictedSQL {
			preferredBuildIndex = val.(int)
		}
	})

	baseJoin.InnerChildIdx = preferredBuildIndex
	childrenProps := make([]*property.PhysicalProperty, 2)
	if useBCJ {
		childrenProps[preferredBuildIndex] = &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, MPPPartitionTp: property.BroadcastType, CanAddEnforcer: true, CTEProducerStatus: prop.CTEProducerStatus}
		expCnt := math.MaxFloat64
		if prop.ExpectedCnt < p.StatsInfo().RowCount {
			expCntScale := prop.ExpectedCnt / p.StatsInfo().RowCount
			var targetStats *property.StatsInfo
			if 1-preferredBuildIndex == 0 {
				targetStats = stats0
			} else {
				targetStats = stats1
			}
			expCnt = targetStats.RowCount * expCntScale
		}
		if prop.MPPPartitionTp == property.HashType {
			lPartitionKeys, rPartitionKeys := p.GetPotentialPartitionKeys()
			hashKeys := rPartitionKeys
			if preferredBuildIndex == 1 {
				hashKeys = lPartitionKeys
			}
			matches := prop.IsSubsetOf(hashKeys)
			if len(matches) == 0 {
				return nil
			}
			childrenProps[1-preferredBuildIndex] = &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: expCnt, MPPPartitionTp: property.HashType, MPPPartitionCols: prop.MPPPartitionCols, CTEProducerStatus: prop.CTEProducerStatus}
		} else {
			childrenProps[1-preferredBuildIndex] = &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: expCnt, MPPPartitionTp: property.AnyType, CTEProducerStatus: prop.CTEProducerStatus}
		}
	} else {
		lPartitionKeys, rPartitionKeys := p.GetPotentialPartitionKeys()
		if prop.MPPPartitionTp == property.HashType {
			var matches []int
			switch p.JoinType {
			case base.InnerJoin:
				if matches = prop.IsSubsetOf(lPartitionKeys); len(matches) == 0 {
					matches = prop.IsSubsetOf(rPartitionKeys)
				}
			case base.RightOuterJoin:
				// for right out join, only the right partition keys can possibly matches the prop, because
				// the left partition keys will generate NULL values randomly
				// todo maybe we can add a null-sensitive flag in the MPPPartitionColumn to indicate whether the partition column is
				//  null-sensitive(used in aggregation) or null-insensitive(used in join)
				matches = prop.IsSubsetOf(rPartitionKeys)
			default:
				// for left out join, only the left partition keys can possibly matches the prop, because
				// the right partition keys will generate NULL values randomly
				// for semi/anti semi/left out semi/anti left out semi join, only left partition keys are returned,
				// so just check the left partition keys
				matches = prop.IsSubsetOf(lPartitionKeys)
			}
			if len(matches) == 0 {
				return nil
			}
			lPartitionKeys = property.ChoosePartitionKeys(lPartitionKeys, matches)
			rPartitionKeys = property.ChoosePartitionKeys(rPartitionKeys, matches)
		}
		childrenProps[0] = &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, MPPPartitionTp: property.HashType, MPPPartitionCols: lPartitionKeys, CanAddEnforcer: true, CTEProducerStatus: prop.CTEProducerStatus}
		childrenProps[1] = &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, MPPPartitionTp: property.HashType, MPPPartitionCols: rPartitionKeys, CanAddEnforcer: true, CTEProducerStatus: prop.CTEProducerStatus}
	}
	join := PhysicalHashJoin{
		BasePhysicalJoin:  baseJoin,
		Concurrency:       uint(p.SCtx().GetSessionVars().CopTiFlashConcurrencyFactor),
		EqualConditions:   p.EqualConditions,
		NAEqualConditions: p.NAEQConditions,
		StoreTp:           kv.TiFlash,
		MppShuffleJoin:    !useBCJ,
		// Mpp Join has quite heavy cost. Even limit might not suspend it in time, so we don't scale the count.
	}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset(), childrenProps...)
	join.SetSchema(p.Schema())
	return []base.PhysicalPlan{join}
}

func getHashJoins(super base.LogicalPlan, prop *property.PhysicalProperty) (joins []base.PhysicalPlan, forced bool) {
	ge, p := base.GetGEAndLogical[*logicalop.LogicalJoin](super)
	if !prop.IsSortItemEmpty() { // hash join doesn't promise any orders
		return
	}

	forceLeftToBuild := ((p.PreferJoinType & h.PreferLeftAsHJBuild) > 0) || ((p.PreferJoinType & h.PreferRightAsHJProbe) > 0)
	forceRightToBuild := ((p.PreferJoinType & h.PreferRightAsHJBuild) > 0) || ((p.PreferJoinType & h.PreferLeftAsHJProbe) > 0)
	if forceLeftToBuild && forceRightToBuild {
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning("Conflicting HASH_JOIN_BUILD and HASH_JOIN_PROBE hints detected. " +
			"Both sides cannot be specified to use the same table. Please review the hints")
		forceLeftToBuild = false
		forceRightToBuild = false
	}
	joins = make([]base.PhysicalPlan, 0, 2)
	switch p.JoinType {
	case base.SemiJoin, base.AntiSemiJoin:
		leftJoinKeys, _, isNullEQ, _ := p.GetJoinKeys()
		leftNAJoinKeys, _ := p.GetNAJoinKeys()
		if p.SCtx().GetSessionVars().UseHashJoinV2 && joinversion.IsHashJoinV2Supported() && CanUseHashJoinV2(p.JoinType, leftJoinKeys, isNullEQ, leftNAJoinKeys) {
			if !forceLeftToBuild {
				joins = append(joins, getHashJoin(ge, p, prop, 1, false))
			}
			if !forceRightToBuild {
				joins = append(joins, getHashJoin(ge, p, prop, 1, true))
			}
		} else {
			joins = append(joins, getHashJoin(ge, p, prop, 1, false))
			if forceLeftToBuild || forceRightToBuild {
				p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(fmt.Sprintf(
					"The HASH_JOIN_BUILD and HASH_JOIN_PROBE hints are not supported for %s with hash join version 1. "+
						"Please remove these hints",
					p.JoinType))
				forceLeftToBuild = false
				forceRightToBuild = false
			}
		}
	case base.LeftOuterSemiJoin, base.AntiLeftOuterSemiJoin:
		joins = append(joins, getHashJoin(ge, p, prop, 1, false))
		if forceLeftToBuild || forceRightToBuild {
			p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(fmt.Sprintf(
				"HASH_JOIN_BUILD and HASH_JOIN_PROBE hints are not supported for %s because the build side is fixed. "+
					"Please remove these hints",
				p.JoinType))
			forceLeftToBuild = false
			forceRightToBuild = false
		}
	case base.LeftOuterJoin:
		if !forceLeftToBuild {
			joins = append(joins, getHashJoin(ge, p, prop, 1, false))
		}
		if !forceRightToBuild {
			joins = append(joins, getHashJoin(ge, p, prop, 1, true))
		}
	case base.RightOuterJoin:
		if !forceLeftToBuild {
			joins = append(joins, getHashJoin(ge, p, prop, 0, true))
		}
		if !forceRightToBuild {
			joins = append(joins, getHashJoin(ge, p, prop, 0, false))
		}
	case base.InnerJoin:
		if forceLeftToBuild {
			joins = append(joins, getHashJoin(ge, p, prop, 0, false))
		} else if forceRightToBuild {
			joins = append(joins, getHashJoin(ge, p, prop, 1, false))
		} else {
			joins = append(joins, getHashJoin(ge, p, prop, 1, false))
			joins = append(joins, getHashJoin(ge, p, prop, 0, false))
		}
	}

	forced = (p.PreferJoinType&h.PreferHashJoin > 0) || forceLeftToBuild || forceRightToBuild
	shouldSkipHashJoin := ShouldSkipHashJoin(p)
	if !forced && shouldSkipHashJoin {
		return nil, false
	} else if forced && shouldSkipHashJoin {
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(
			"A conflict between the HASH_JOIN hint and the NO_HASH_JOIN hint, " +
				"or the tidb_opt_enable_hash_join system variable, the HASH_JOIN hint will take precedence.")
	}
	return
}

func getHashJoin(ge base.GroupExpression, p *logicalop.LogicalJoin, prop *property.PhysicalProperty, innerIdx int, useOuterToBuild bool) *PhysicalHashJoin {
	var stats0, stats1 *property.StatsInfo
	if ge != nil {
		stats0, stats1, _, _ = ge.GetJoinChildStatsAndSchema()
	} else {
		stats0, stats1, _, _ = p.GetJoinChildStatsAndSchema()
	}
	chReqProps := make([]*property.PhysicalProperty, 2)
	chReqProps[innerIdx] = &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: prop.NoCopPushDown}
	chReqProps[1-innerIdx] = &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: prop.NoCopPushDown}
	var outerStats *property.StatsInfo
	if 1-innerIdx == 0 {
		outerStats = stats0
	} else {
		outerStats = stats1
	}
	if prop.ExpectedCnt < p.StatsInfo().RowCount {
		expCntScale := prop.ExpectedCnt / p.StatsInfo().RowCount
		chReqProps[1-innerIdx].ExpectedCnt = outerStats.RowCount * expCntScale
	}
	hashJoin := NewPhysicalHashJoin(p, innerIdx, useOuterToBuild, p.StatsInfo().ScaleByExpectCnt(p.SCtx().GetSessionVars(), prop.ExpectedCnt), chReqProps...)
	hashJoin.SetSchema(p.Schema())
	return hashJoin
}

// GetHashJoin is public for cascades planner.
func GetHashJoin(ge base.GroupExpression, la *logicalop.LogicalApply, prop *property.PhysicalProperty) *PhysicalHashJoin {
	return getHashJoin(ge, &la.LogicalJoin, prop, 1, false)
}
