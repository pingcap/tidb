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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/join/joinversion"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
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
func (p *PhysicalHashJoin) GetCost(lCnt, rCnt float64, _ bool, costFlag uint64) float64 {
	return utilfuncp.GetCost4PhysicalHashJoin(p, lCnt, rCnt, costFlag)
}

// GetPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalHashJoin) GetPlanCostVer1(taskType property.TaskType,
	option *costusage.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer14PhysicalHashJoin(p, taskType, option)
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + probe-child-cost +
// build-hash-cost + build-filter-cost +
// (probe-filter-cost + probe-hash-cost) / concurrency
func (p *PhysicalHashJoin) GetPlanCostVer2(taskType property.TaskType,
	option *costusage.PlanCostOption, _ ...bool) (costusage.CostVer2, error) {
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
		lArg, _, err := fun.GetArgs()[0].ResolveIndices(lSchema)
		if err != nil {
			return err
		}
		p.LeftJoinKeys[i] = lArg.(*expression.Column)
		rArg, _, err := fun.GetArgs()[1].ResolveIndices(rSchema)
		if err != nil {
			return err
		}
		p.RightJoinKeys[i] = rArg.(*expression.Column)
		p.EqualConditions[i] = expression.NewFunctionInternal(ctx.GetExprCtx(), fun.FuncName.L, fun.GetStaticType(), lArg, rArg).(*expression.ScalarFunction)
	}
	for i, fun := range p.NAEqualConditions {
		lArg, _, err := fun.GetArgs()[0].ResolveIndices(lSchema)
		if err != nil {
			return err
		}
		p.LeftNAJoinKeys[i] = lArg.(*expression.Column)
		rArg, _, err := fun.GetArgs()[1].ResolveIndices(rSchema)
		if err != nil {
			return err
		}
		p.RightNAJoinKeys[i] = rArg.(*expression.Column)
		p.NAEqualConditions[i] = expression.NewFunctionInternal(ctx.GetExprCtx(), fun.FuncName.L, fun.GetStaticType(), lArg, rArg).(*expression.ScalarFunction)
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
