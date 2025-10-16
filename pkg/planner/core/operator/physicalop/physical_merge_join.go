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
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/types"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tidb/pkg/util/size"
)

// PhysicalMergeJoin represents merge join implementation of LogicalJoin.
type PhysicalMergeJoin struct {
	BasePhysicalJoin

	CompareFuncs []expression.CompareFunc `plan-cache-clone:"shallow"`
	// Desc means whether inner child keep desc order.
	Desc bool
}

// GetMergeJoin convert the logical join to physical merge join based on the physical property.
func GetMergeJoin(p *logicalop.LogicalJoin, prop *property.PhysicalProperty, schema *expression.Schema, statsInfo *property.StatsInfo, leftStatsInfo *property.StatsInfo, rightStatsInfo *property.StatsInfo) []base.PhysicalPlan {
	joins := make([]base.PhysicalPlan, 0, len(p.LeftProperties)+1)
	// The LeftProperties caches all the possible properties that are provided by its children.
	leftJoinKeys, rightJoinKeys, isNullEQ, hasNullEQ := p.GetJoinKeys()

	// EnumType/SetType Unsupported: merge join conflicts with index order.
	// ref: https://github.com/pingcap/tidb/issues/24473, https://github.com/pingcap/tidb/issues/25669
	for _, leftKey := range leftJoinKeys {
		if leftKey.RetType.GetType() == mysql.TypeEnum || leftKey.RetType.GetType() == mysql.TypeSet {
			return nil
		}
	}
	for _, rightKey := range rightJoinKeys {
		if rightKey.RetType.GetType() == mysql.TypeEnum || rightKey.RetType.GetType() == mysql.TypeSet {
			return nil
		}
	}

	// TODO: support null equal join keys for merge join
	if hasNullEQ {
		return nil
	}
	for _, lhsChildProperty := range p.LeftProperties {
		offsets := util.GetMaxSortPrefix(lhsChildProperty, leftJoinKeys)
		// If not all equal conditions hit properties. We ban merge join heuristically. Because in this case, merge join
		// may get a very low performance. In executor, executes join results before other conditions filter it.
		// And skip the cartesian join case, unless we force to use merge join.
		if len(offsets) < len(leftJoinKeys) || len(leftJoinKeys) == 0 {
			continue
		}

		leftKeys := lhsChildProperty[:len(offsets)]
		rightKeys := expression.NewSchema(rightJoinKeys...).ColumnsByIndices(offsets)
		newIsNullEQ := make([]bool, 0, len(offsets))
		for _, offset := range offsets {
			newIsNullEQ = append(newIsNullEQ, isNullEQ[offset])
		}

		prefixLen := findMaxPrefixLen(p.RightProperties, rightKeys)
		// right side should also be full match.
		if prefixLen < len(offsets) || prefixLen == 0 {
			continue
		}

		leftKeys = leftKeys[:prefixLen]
		rightKeys = rightKeys[:prefixLen]
		newIsNullEQ = newIsNullEQ[:prefixLen]
		if !checkJoinKeyCollation(leftKeys, rightKeys) {
			continue
		}
		offsets = offsets[:prefixLen]
		baseJoin := BasePhysicalJoin{
			JoinType:        p.JoinType,
			LeftConditions:  p.LeftConditions,
			RightConditions: p.RightConditions,
			DefaultValues:   p.DefaultValues,
			LeftJoinKeys:    leftKeys,
			RightJoinKeys:   rightKeys,
			IsNullEQ:        newIsNullEQ,
		}
		mergeJoin := PhysicalMergeJoin{BasePhysicalJoin: baseJoin}.Init(p.SCtx(), statsInfo.ScaleByExpectCnt(p.SCtx().GetSessionVars(), prop.ExpectedCnt), p.QueryBlockOffset())
		mergeJoin.SetSchema(schema)
		mergeJoin.OtherConditions = moveEqualToOtherConditions(p, offsets)
		mergeJoin.initCompareFuncs()
		if reqProps, ok := mergeJoin.tryToGetChildReqProp(prop); ok {
			// Adjust expected count for children nodes.
			if prop.ExpectedCnt < statsInfo.RowCount {
				expCntScale := prop.ExpectedCnt / statsInfo.RowCount
				reqProps[0].ExpectedCnt = leftStatsInfo.RowCount * expCntScale
				reqProps[1].ExpectedCnt = rightStatsInfo.RowCount * expCntScale
			}
			mergeJoin.SetChildrenReqProps(reqProps)
			_, desc := prop.AllSameOrder()
			mergeJoin.Desc = desc
			joins = append(joins, mergeJoin)
		}
	}

	if p.PreferJoinType&h.PreferNoMergeJoin > 0 {
		if p.PreferJoinType&h.PreferMergeJoin == 0 {
			return nil
		}
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(
			"Some MERGE_JOIN and NO_MERGE_JOIN hints conflict, NO_MERGE_JOIN is ignored")
	}

	// If TiDB_SMJ hint is existed, it should consider enforce merge join,
	// because we can't trust lhsChildProperty completely.
	if (p.PreferJoinType&h.PreferMergeJoin) > 0 ||
		ShouldSkipHashJoin(p) { // if hash join is not allowed, generate as many other types of join as possible to avoid 'cant-find-plan' error.
		joins = append(joins, getEnforcedMergeJoin(p, prop, schema, statsInfo)...)
	}

	return joins
}

func getEnforcedMergeJoin(p *logicalop.LogicalJoin, prop *property.PhysicalProperty, schema *expression.Schema, statsInfo *property.StatsInfo) []base.PhysicalPlan {
	// Check whether SMJ can satisfy the required property
	leftJoinKeys, rightJoinKeys, isNullEQ, hasNullEQ := p.GetJoinKeys()
	// TODO: support null equal join keys for merge join
	if hasNullEQ {
		return nil
	}
	offsets := make([]int, 0, len(leftJoinKeys))
	all, desc := prop.AllSameOrder()
	if !all {
		return nil
	}
	evalCtx := p.SCtx().GetExprCtx().GetEvalCtx()
	for _, item := range prop.SortItems {
		isExist, hasLeftColInProp, hasRightColInProp := false, false, false
		for joinKeyPos := range leftJoinKeys {
			var key *expression.Column
			if item.Col.Equal(evalCtx, leftJoinKeys[joinKeyPos]) {
				key = leftJoinKeys[joinKeyPos]
				hasLeftColInProp = true
			}
			if item.Col.Equal(evalCtx, rightJoinKeys[joinKeyPos]) {
				key = rightJoinKeys[joinKeyPos]
				hasRightColInProp = true
			}
			if key == nil {
				continue
			}
			if slices.Contains(offsets, joinKeyPos) {
				isExist = true
			}
			if !isExist {
				offsets = append(offsets, joinKeyPos)
			}
			isExist = true
			break
		}
		if !isExist {
			return nil
		}
		// If the output wants the order of the inner side. We should reject it since we might add null-extend rows of that side.
		if p.JoinType == base.LeftOuterJoin && hasRightColInProp {
			return nil
		}
		if p.JoinType == base.RightOuterJoin && hasLeftColInProp {
			return nil
		}
	}
	// Generate the enforced sort merge join
	leftKeys := getNewJoinKeysByOffsets(leftJoinKeys, offsets)
	rightKeys := getNewJoinKeysByOffsets(rightJoinKeys, offsets)
	newNullEQ := getNewNullEQByOffsets(isNullEQ, offsets)
	otherConditions := make([]expression.Expression, len(p.OtherConditions), len(p.OtherConditions)+len(p.EqualConditions))
	copy(otherConditions, p.OtherConditions)
	if !checkJoinKeyCollation(leftKeys, rightKeys) {
		// if the join keys' collation are conflicted, we use the empty join key
		// and move EqualConditions to OtherConditions.
		leftKeys = nil
		rightKeys = nil
		newNullEQ = nil
		otherConditions = append(otherConditions, expression.ScalarFuncs2Exprs(p.EqualConditions)...)
	}
	lProp := property.NewPhysicalProperty(property.RootTaskType, leftKeys, desc, math.MaxFloat64, true)
	lProp.NoCopPushDown = prop.NoCopPushDown
	rProp := property.NewPhysicalProperty(property.RootTaskType, rightKeys, desc, math.MaxFloat64, true)
	rProp.NoCopPushDown = prop.NoCopPushDown
	baseJoin := BasePhysicalJoin{
		JoinType:        p.JoinType,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		DefaultValues:   p.DefaultValues,
		LeftJoinKeys:    leftKeys,
		RightJoinKeys:   rightKeys,
		IsNullEQ:        newNullEQ,
		OtherConditions: otherConditions,
	}
	enforcedPhysicalMergeJoin := PhysicalMergeJoin{BasePhysicalJoin: baseJoin, Desc: desc}.Init(p.SCtx(), statsInfo.ScaleByExpectCnt(p.SCtx().GetSessionVars(), prop.ExpectedCnt), p.QueryBlockOffset())
	enforcedPhysicalMergeJoin.SetSchema(schema)
	enforcedPhysicalMergeJoin.SetChildrenReqProps([]*property.PhysicalProperty{lProp, rProp})
	enforcedPhysicalMergeJoin.initCompareFuncs()
	return []base.PhysicalPlan{enforcedPhysicalMergeJoin}
}

// ShouldSkipHashJoin is to judge whether to skip hash join
func ShouldSkipHashJoin(p *logicalop.LogicalJoin) bool {
	return (p.PreferJoinType&h.PreferNoHashJoin) > 0 || (p.SCtx().GetSessionVars().DisableHashJoin)
}

// BuildMergeJoinPlan builds a PhysicalMergeJoin from the given fields. Currently, it is only used for test purpose.
func BuildMergeJoinPlan(ctx base.PlanContext, joinType base.JoinType, leftKeys, rightKeys []*expression.Column) *PhysicalMergeJoin {
	baseJoin := BasePhysicalJoin{
		JoinType:      joinType,
		DefaultValues: []types.Datum{types.NewDatum(1), types.NewDatum(1)},
		LeftJoinKeys:  leftKeys,
		RightJoinKeys: rightKeys,
	}
	return PhysicalMergeJoin{BasePhysicalJoin: baseJoin}.Init(ctx, nil, 0)
}

// Init initializes PhysicalMergeJoin.
func (p PhysicalMergeJoin) Init(ctx base.PlanContext, stats *property.StatsInfo, offset int) *PhysicalMergeJoin {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeMergeJoin, &p, offset)
	p.SetStats(stats)
	return &p
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalMergeJoin) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalMergeJoin)
	cloned.SetSCtx(newCtx)
	base, err := p.BasePhysicalJoin.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.BasePhysicalJoin = *base
	cloned.CompareFuncs = append(cloned.CompareFuncs, p.CompareFuncs...)
	cloned.Desc = p.Desc
	return cloned, nil
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalMergeJoin) Attach2Task(tasks ...base.Task) base.Task {
	return utilfuncp.Attach2Task4PhysicalMergeJoin(p, tasks...)
}

// GetCost computes cost of merge join operator itself.
func (p *PhysicalMergeJoin) GetCost(lCnt, rCnt float64, costFlag uint64) float64 {
	return utilfuncp.GetCost4PhysicalMergeJoin(p, lCnt, rCnt, costFlag)
}

// GetPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalMergeJoin) GetPlanCostVer1(taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer14PhysicalMergeJoin(p, taskType, option)
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = left-child-cost + right-child-cost + filter-cost + group-cost
func (p *PhysicalMergeJoin) GetPlanCostVer2(taskType property.TaskType, option *costusage.PlanCostOption, _ ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetPlanCostVer24PhysicalMergeJoin(p, taskType, option)
}

// ExplainInfo implements Plan interface.
func (p *PhysicalMergeJoin) ExplainInfo() string {
	return p.explainInfo(false)
}

func (p *PhysicalMergeJoin) explainInfo(normalized bool) string {
	sortedExplainExpressionList := expression.SortedExplainExpressionList
	if normalized {
		sortedExplainExpressionList = func(_ expression.EvalContext, exprs []expression.Expression) []byte {
			return expression.SortedExplainNormalizedExpressionList(exprs)
		}
	}

	evalCtx := p.SCtx().GetExprCtx().GetEvalCtx()
	buffer := new(strings.Builder)
	buffer.WriteString(p.JoinType.String())
	explainJoinLeftSide(buffer, p.JoinType.IsInnerJoin(), normalized, p.Children()[0])
	if len(p.LeftJoinKeys) > 0 {
		fmt.Fprintf(buffer, ", left key:%s",
			expression.ExplainColumnList(evalCtx, p.LeftJoinKeys))
	}
	if len(p.RightJoinKeys) > 0 {
		fmt.Fprintf(buffer, ", right key:%s",
			expression.ExplainColumnList(evalCtx, p.RightJoinKeys))
	}
	if len(p.LeftConditions) > 0 {
		fmt.Fprintf(buffer, ", left cond:%s",
			sortedExplainExpressionList(evalCtx, p.LeftConditions))
	}
	if len(p.RightConditions) > 0 {
		fmt.Fprintf(buffer, ", right cond:%s",
			sortedExplainExpressionList(evalCtx, p.RightConditions))
	}
	if len(p.OtherConditions) > 0 {
		fmt.Fprintf(buffer, ", other cond:%s",
			sortedExplainExpressionList(evalCtx, p.OtherConditions))
	}
	return buffer.String()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalMergeJoin) ExplainNormalizedInfo() string {
	return p.explainInfo(true)
}

func (p *PhysicalMergeJoin) initCompareFuncs() {
	p.CompareFuncs = make([]expression.CompareFunc, 0, len(p.LeftJoinKeys))
	for i := range p.LeftJoinKeys {
		p.CompareFuncs = append(p.CompareFuncs, expression.GetCmpFunction(p.SCtx().GetExprCtx(), p.LeftJoinKeys[i], p.RightJoinKeys[i]))
	}
}

// MemoryUsage return the memory usage of PhysicalMergeJoin
func (p *PhysicalMergeJoin) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.BasePhysicalJoin.MemoryUsage() + size.SizeOfSlice + int64(cap(p.CompareFuncs))*size.SizeOfFunc + size.SizeOfBool
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalMergeJoin) ResolveIndices() (err error) {
	err = p.PhysicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	lSchema := p.Children()[0].Schema()
	rSchema := p.Children()[1].Schema()
	for i, col := range p.LeftJoinKeys {
		newKey, err := col.ResolveIndices(lSchema)
		if err != nil {
			return err
		}
		p.LeftJoinKeys[i] = newKey.(*expression.Column)
	}
	for i, col := range p.RightJoinKeys {
		newKey, err := col.ResolveIndices(rSchema)
		if err != nil {
			return err
		}
		p.RightJoinKeys[i] = newKey.(*expression.Column)
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

// Only if the input required prop is the prefix fo join keys, we can pass through this property.
func (p *PhysicalMergeJoin) tryToGetChildReqProp(prop *property.PhysicalProperty) ([]*property.PhysicalProperty, bool) {
	all, desc := prop.AllSameOrder()
	lProp := property.NewPhysicalProperty(property.RootTaskType, p.LeftJoinKeys, desc, math.MaxFloat64, false)
	rProp := property.NewPhysicalProperty(property.RootTaskType, p.RightJoinKeys, desc, math.MaxFloat64, false)
	lProp.CTEProducerStatus = prop.CTEProducerStatus
	lProp.NoCopPushDown = prop.NoCopPushDown
	rProp.CTEProducerStatus = prop.CTEProducerStatus
	rProp.NoCopPushDown = prop.NoCopPushDown
	if !prop.IsSortItemEmpty() {
		// sort merge join fits the cases of massive ordered data, so desc scan is always expensive.
		if !all {
			return nil, false
		}
		if !prop.IsPrefix(lProp) && !prop.IsPrefix(rProp) {
			return nil, false
		}
		if prop.IsPrefix(rProp) && p.JoinType == base.LeftOuterJoin {
			return nil, false
		}
		if prop.IsPrefix(lProp) && p.JoinType == base.RightOuterJoin {
			return nil, false
		}
	}

	return []*property.PhysicalProperty{lProp, rProp}, true
}

func findMaxPrefixLen(candidates [][]*expression.Column, keys []*expression.Column) int {
	maxLen := 0
	for _, candidateKeys := range candidates {
		matchedLen := 0
		for i := range keys {
			if !(i < len(candidateKeys) && keys[i].EqualColumn(candidateKeys[i])) {
				break
			}
			matchedLen++
		}
		if matchedLen > maxLen {
			maxLen = matchedLen
		}
	}
	return maxLen
}

func moveEqualToOtherConditions(p *logicalop.LogicalJoin, offsets []int) []expression.Expression {
	// Construct used equal condition set based on the equal condition offsets.
	usedEqConds := set.NewIntSet()
	for _, eqCondIdx := range offsets {
		usedEqConds.Insert(eqCondIdx)
	}

	// Construct otherConds, which is composed of the original other conditions
	// and the remained unused equal conditions.
	numOtherConds := len(p.OtherConditions) + len(p.EqualConditions) - len(usedEqConds)
	otherConds := make([]expression.Expression, len(p.OtherConditions), numOtherConds)
	copy(otherConds, p.OtherConditions)
	for eqCondIdx := range p.EqualConditions {
		if !usedEqConds.Exist(eqCondIdx) {
			otherConds = append(otherConds, p.EqualConditions[eqCondIdx])
		}
	}

	return otherConds
}

func checkJoinKeyCollation(leftKeys, rightKeys []*expression.Column) bool {
	// if a left key and its corresponding right key have different collation, don't use MergeJoin since
	// the their children may sort their records in different ways
	for i := range leftKeys {
		lt := leftKeys[i].RetType
		rt := rightKeys[i].RetType
		if (lt.EvalType() == types.ETString && rt.EvalType() == types.ETString) &&
			(leftKeys[i].RetType.GetCharset() != rightKeys[i].RetType.GetCharset() ||
				leftKeys[i].RetType.GetCollate() != rightKeys[i].RetType.GetCollate()) {
			return false
		}
	}
	return true
}

// Change JoinKeys order, by offsets array
// offsets array is generate by prop check
func getNewJoinKeysByOffsets(oldJoinKeys []*expression.Column, offsets []int) []*expression.Column {
	newKeys := make([]*expression.Column, 0, len(oldJoinKeys))
	for _, offset := range offsets {
		newKeys = append(newKeys, oldJoinKeys[offset])
	}
	for pos, key := range oldJoinKeys {
		isExist := slices.Contains(offsets, pos)
		if !isExist {
			newKeys = append(newKeys, key)
		}
	}
	return newKeys
}

func getNewNullEQByOffsets(oldNullEQ []bool, offsets []int) []bool {
	newNullEQ := make([]bool, 0, len(oldNullEQ))
	for _, offset := range offsets {
		newNullEQ = append(newNullEQ, oldNullEQ[offset])
	}
	for pos, key := range oldNullEQ {
		isExist := slices.Contains(offsets, pos)
		if !isExist {
			newNullEQ = append(newNullEQ, key)
		}
	}
	return newNullEQ
}
