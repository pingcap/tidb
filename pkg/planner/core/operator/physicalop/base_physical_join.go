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
	"math"
	"math/bits"
	"unsafe"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/types"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/size"
)

var (
	_ base.PhysicalJoin = &PhysicalHashJoin{}
	_ base.PhysicalJoin = &PhysicalMergeJoin{}
	_ base.PhysicalJoin = &PhysicalIndexJoin{}
	_ base.PhysicalJoin = &PhysicalIndexHashJoin{}
	_ base.PhysicalJoin = &PhysicalIndexMergeJoin{}
)

const (
	joinLeft             = 0
	joinRight            = 1
	indexJoinMethod      = 0
	indexHashJoinMethod  = 1
	indexMergeJoinMethod = 2
)

// BasePhysicalJoin is the base struct for all physical join operators.
// TODO: it is temporarily to be public for all physical join operators to embed it.
type BasePhysicalJoin struct {
	PhysicalSchemaProducer

	JoinType base.JoinType

	LeftConditions  expression.CNFExprs
	RightConditions expression.CNFExprs
	OtherConditions expression.CNFExprs

	InnerChildIdx int
	OuterJoinKeys []*expression.Column
	InnerJoinKeys []*expression.Column
	LeftJoinKeys  []*expression.Column
	RightJoinKeys []*expression.Column
	// IsNullEQ is used for cases like Except statement where null key should be matched with null key.
	// <1,null> is exactly matched with <1,null>, where the null value should not be filtered and
	// the null is exactly matched with null only. (while in NAAJ null value should also be matched
	// with other non-null item as well)
	IsNullEQ      []bool
	DefaultValues []types.Datum

	LeftNAJoinKeys  []*expression.Column
	RightNAJoinKeys []*expression.Column
}

// GetJoinType returns the type of the join operation.
func (p *BasePhysicalJoin) GetJoinType() base.JoinType {
	return p.JoinType
}

// PhysicalJoinImplement implements base.PhysicalJoin interface.
func (*BasePhysicalJoin) PhysicalJoinImplement() {}

// GetInnerChildIdx returns the index of the inner child in the join operator.
func (p *BasePhysicalJoin) GetInnerChildIdx() int {
	return p.InnerChildIdx
}

// CloneForPlanCacheWithSelf clones the BasePhysicalJoin for plan cache with self.
func (p *BasePhysicalJoin) CloneForPlanCacheWithSelf(newCtx base.PlanContext, newSelf base.PhysicalPlan) (*BasePhysicalJoin, bool) {
	cloned := new(BasePhysicalJoin)
	base, ok := p.PhysicalSchemaProducer.CloneForPlanCacheWithSelf(newCtx, newSelf)
	if !ok {
		return nil, false
	}
	cloned.PhysicalSchemaProducer = *base
	cloned.JoinType = p.JoinType
	cloned.LeftConditions = utilfuncp.CloneExpressionsForPlanCache(p.LeftConditions, nil)
	cloned.RightConditions = utilfuncp.CloneExpressionsForPlanCache(p.RightConditions, nil)
	cloned.OtherConditions = utilfuncp.CloneExpressionsForPlanCache(p.OtherConditions, nil)
	cloned.InnerChildIdx = p.InnerChildIdx
	cloned.OuterJoinKeys = utilfuncp.CloneColumnsForPlanCache(p.OuterJoinKeys, nil)
	cloned.InnerJoinKeys = utilfuncp.CloneColumnsForPlanCache(p.InnerJoinKeys, nil)
	cloned.LeftJoinKeys = utilfuncp.CloneColumnsForPlanCache(p.LeftJoinKeys, nil)
	cloned.RightJoinKeys = utilfuncp.CloneColumnsForPlanCache(p.RightJoinKeys, nil)
	cloned.IsNullEQ = make([]bool, len(p.IsNullEQ))
	copy(cloned.IsNullEQ, p.IsNullEQ)
	for _, d := range p.DefaultValues {
		cloned.DefaultValues = append(cloned.DefaultValues, *d.Clone())
	}
	cloned.LeftNAJoinKeys = utilfuncp.CloneColumnsForPlanCache(p.LeftNAJoinKeys, nil)
	cloned.RightNAJoinKeys = utilfuncp.CloneColumnsForPlanCache(p.RightNAJoinKeys, nil)
	return cloned, true
}

// CloneWithSelf clones the BasePhysicalJoin with self.
func (p *BasePhysicalJoin) CloneWithSelf(newCtx base.PlanContext, newSelf base.PhysicalPlan) (*BasePhysicalJoin, error) {
	cloned := new(BasePhysicalJoin)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, newSelf)
	if err != nil {
		return nil, err
	}
	cloned.PhysicalSchemaProducer = *base
	cloned.JoinType = p.JoinType
	cloned.LeftConditions = util.CloneExprs(p.LeftConditions)
	cloned.RightConditions = util.CloneExprs(p.RightConditions)
	cloned.OtherConditions = util.CloneExprs(p.OtherConditions)
	cloned.InnerChildIdx = p.InnerChildIdx
	cloned.OuterJoinKeys = util.CloneCols(p.OuterJoinKeys)
	cloned.InnerJoinKeys = util.CloneCols(p.InnerJoinKeys)
	cloned.LeftJoinKeys = util.CloneCols(p.LeftJoinKeys)
	cloned.RightJoinKeys = util.CloneCols(p.RightJoinKeys)
	cloned.LeftNAJoinKeys = util.CloneCols(p.LeftNAJoinKeys)
	cloned.RightNAJoinKeys = util.CloneCols(p.RightNAJoinKeys)
	for _, d := range p.DefaultValues {
		cloned.DefaultValues = append(cloned.DefaultValues, *d.Clone())
	}
	return cloned, nil
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *BasePhysicalJoin) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.LeftConditions)+len(p.RightConditions)+len(p.OtherConditions))
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

const emptyBasePhysicalJoinSize = int64(unsafe.Sizeof(BasePhysicalJoin{}))

// MemoryUsage return the memory usage of BasePhysicalJoin
func (p *BasePhysicalJoin) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = emptyBasePhysicalJoinSize + p.PhysicalSchemaProducer.MemoryUsage() + int64(cap(p.IsNullEQ))*size.SizeOfBool +
		int64(cap(p.LeftConditions)+cap(p.RightConditions)+cap(p.OtherConditions))*size.SizeOfInterface +
		int64(cap(p.OuterJoinKeys)+cap(p.InnerJoinKeys)+cap(p.LeftJoinKeys)+cap(p.RightNAJoinKeys)+cap(p.LeftNAJoinKeys)+
			cap(p.RightNAJoinKeys))*size.SizeOfPointer + int64(cap(p.DefaultValues))*types.EmptyDatumSize

	for _, cond := range p.LeftConditions {
		sum += cond.MemoryUsage()
	}
	for _, cond := range p.RightConditions {
		sum += cond.MemoryUsage()
	}
	for _, cond := range p.OtherConditions {
		sum += cond.MemoryUsage()
	}
	for _, col := range p.LeftJoinKeys {
		sum += col.MemoryUsage()
	}
	for _, col := range p.RightJoinKeys {
		sum += col.MemoryUsage()
	}
	for _, col := range p.InnerJoinKeys {
		sum += col.MemoryUsage()
	}
	for _, col := range p.OuterJoinKeys {
		sum += col.MemoryUsage()
	}
	for _, datum := range p.DefaultValues {
		sum += datum.MemUsage()
	}
	for _, col := range p.LeftNAJoinKeys {
		sum += col.MemoryUsage()
	}
	for _, col := range p.RightNAJoinKeys {
		sum += col.MemoryUsage()
	}
	return
}

// isJoinHintSupportedInMPPMode is used to check if the specified join hint is available under MPP mode.
func isJoinHintSupportedInMPPMode(preferJoinType uint) bool {
	if preferJoinType == 0 {
		return true
	}
	mppMask := h.PreferShuffleJoin ^ h.PreferBCJoin
	// Currently, TiFlash only supports HASH JOIN, so the hint for HASH JOIN is available while other join method hints are forbidden.
	joinMethodHintSupportedByTiflash := h.PreferHashJoin ^ h.PreferLeftAsHJBuild ^ h.PreferRightAsHJBuild ^ h.PreferLeftAsHJProbe ^ h.PreferRightAsHJProbe
	onesCount := bits.OnesCount(preferJoinType & ^joinMethodHintSupportedByTiflash & ^mppMask)
	return onesCount < 1
}

// it can generates hash join, index join and sort merge join.
// Firstly we check the hint, if hint is figured by user, we force to choose the corresponding physical plan.
// If the hint is not matched, it will get other candidates.
// If the hint is not figured, we will pick all candidates.
func ExhaustPhysicalPlans4LogicalJoin(super base.LogicalPlan, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	ge, p := base.GetGEAndLogical[*logicalop.LogicalJoin](super)
	failpoint.Inject("MockOnlyEnableIndexHashJoin", func(val failpoint.Value) {
		if val.(bool) && !p.SCtx().GetSessionVars().InRestrictedSQL {
			indexJoins, _ := tryToGetIndexJoin(p, prop)
			failpoint.Return(indexJoins, true, nil)
		}
	})

	if !isJoinHintSupportedInMPPMode(p.PreferJoinType) {
		if hasMPPJoinHints(p.PreferJoinType) {
			// If there are MPP hints but has some conflicts join method hints, all the join hints are invalid.
			p.SCtx().GetSessionVars().StmtCtx.SetHintWarning("The MPP join hints are in conflict, and you can only specify join method hints that are currently supported by MPP mode now")
			p.PreferJoinType = 0
		} else {
			// If there are no MPP hints but has some conflicts join method hints, the MPP mode will be blocked.
			p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because you have used hint to specify a join algorithm which is not supported by mpp now.")
			if prop.IsFlashProp() {
				return nil, false, nil
			}
		}
	}
	if prop.MPPPartitionTp == property.BroadcastType {
		return nil, false, nil
	}
	joins := make([]base.PhysicalPlan, 0, 8)
	// we lift the p.canPushToTiFlash check here, because we want to generate all the plans to be decided by the attachment layer.
	if p.SCtx().GetSessionVars().IsMPPAllowed() {
		// prefer hint should be handled in the attachment layer. because the enumerated mpp join may couldn't be built bottom-up.
		if hasMPPJoinHints(p.PreferJoinType) {
			// generate them all for later attachment prefer picking. cause underlying ds may not have tiFlash path.
			// even all mpp join is invalid, they can still resort to root joins as an alternative.
			joins = append(joins, tryToGetMppHashJoin(super, prop, true)...)
			joins = append(joins, tryToGetMppHashJoin(super, prop, false)...)
		} else {
			// join don't have a mpp join hints, only generate preferMppBCJ mpp joins.
			if preferMppBCJ(super) {
				joins = append(joins, tryToGetMppHashJoin(super, prop, true)...)
			} else {
				joins = append(joins, tryToGetMppHashJoin(super, prop, false)...)
			}
		}
	} else {
		hasMppHints := false
		var errMsg string
		if (p.PreferJoinType & h.PreferShuffleJoin) > 0 {
			errMsg = "The join can not push down to the MPP side, the shuffle_join() hint is invalid"
			hasMppHints = true
		}
		if (p.PreferJoinType & h.PreferBCJoin) > 0 {
			errMsg = "The join can not push down to the MPP side, the broadcast_join() hint is invalid"
			hasMppHints = true
		}
		if hasMppHints {
			p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(errMsg)
		}
	}
	if prop.IsFlashProp() {
		return joins, true, nil
	}

	if !p.IsNAAJ() {
		// naaj refuse merge join and index join.
		var stats0, stats1 *property.StatsInfo
		if ge != nil {
			stats0, stats1, _, _ = ge.GetJoinChildStatsAndSchema()
		} else {
			stats0, stats1, _, _ = p.GetJoinChildStatsAndSchema()
		}
		mergeJoins := GetMergeJoin(p, prop, p.Schema(), p.StatsInfo(), stats0, stats1)
		if (p.PreferJoinType&h.PreferMergeJoin) > 0 && len(mergeJoins) > 0 {
			return mergeJoins, true, nil
		}
		joins = append(joins, mergeJoins...)

		if p.SCtx().GetSessionVars().EnhanceIndexJoinBuildV2 {
			indexJoins := tryToEnumerateIndexJoin(super, prop)
			joins = append(joins, indexJoins...)

			failpoint.Inject("MockOnlyEnableIndexHashJoinV2", func(val failpoint.Value) {
				if val.(bool) && !p.SCtx().GetSessionVars().InRestrictedSQL {
					indexHashJoin := make([]base.PhysicalPlan, 0, len(indexJoins))
					for _, one := range indexJoins {
						if _, ok := one.(*PhysicalIndexHashJoin); ok {
							indexHashJoin = append(indexHashJoin, one)
						}
					}
					failpoint.Return(indexHashJoin, true, nil)
				}
			})
		} else {
			indexJoins, forced := tryToGetIndexJoin(p, prop)
			if forced {
				return indexJoins, true, nil
			}
			joins = append(joins, indexJoins...)
		}
	}

	hashJoins, forced := getHashJoins(super, prop)
	if forced && len(hashJoins) > 0 {
		return hashJoins, true, nil
	}
	joins = append(joins, hashJoins...)

	if p.PreferJoinType > 0 {
		// If we reach here, it means we have a hint that doesn't work.
		// It might be affected by the required property, so we enforce
		// this property and try the hint again.
		return joins, false, nil
	}
	return joins, true, nil
}

func hasMPPJoinHints(preferJoinType uint) bool {
	return (preferJoinType&h.PreferBCJoin > 0) || (preferJoinType&h.PreferShuffleJoin > 0)
}

func canExprsInJoinPushdown(p *logicalop.LogicalJoin, storeType kv.StoreType) bool {
	equalExprs := make([]expression.Expression, 0, len(p.EqualConditions))
	for _, eqCondition := range p.EqualConditions {
		if eqCondition.FuncName.L == ast.NullEQ {
			return false
		}
		equalExprs = append(equalExprs, eqCondition)
	}
	pushDownCtx := util.GetPushDownCtx(p.SCtx())
	if !expression.CanExprsPushDown(pushDownCtx, equalExprs, storeType) {
		return false
	}
	if !expression.CanExprsPushDown(pushDownCtx, p.LeftConditions, storeType) {
		return false
	}
	if !expression.CanExprsPushDown(pushDownCtx, p.RightConditions, storeType) {
		return false
	}
	if !expression.CanExprsPushDown(pushDownCtx, p.OtherConditions, storeType) {
		return false
	}
	return true
}

func isJoinChildFitMPPBCJ(p *logicalop.LogicalJoin, childIndexToBC int, mppStoreCnt int) bool {
	rowBC, szBC, hasSizeBC := calcBroadcastExchangeSize(p.Children()[childIndexToBC], mppStoreCnt)
	rowHash, szHash, hasSizeHash := calcHashExchangeSizeByChild(p.Children()[0], p.Children()[1], mppStoreCnt)

	if hasSizeBC && hasSizeHash {
		return szBC*float64(mppStoreCnt) <= szHash
	}
	return rowBC*float64(mppStoreCnt) <= rowHash
}

// If we can use mpp broadcast join, that's our first choice.
func preferMppBCJ(super base.LogicalPlan) bool {
	ge, p := base.GetGEAndLogical[*logicalop.LogicalJoin](super)
	if len(p.EqualConditions) == 0 && p.SCtx().GetSessionVars().AllowCartesianBCJ == 2 {
		return true
	}

	onlyCheckChild1 := p.JoinType == base.LeftOuterJoin || p.JoinType == base.SemiJoin || p.JoinType == base.AntiSemiJoin
	onlyCheckChild0 := p.JoinType == base.RightOuterJoin

	if p.SCtx().GetSessionVars().PreferBCJByExchangeDataSize {
		mppStoreCnt, err := p.SCtx().GetMPPClient().GetMPPStoreCount()

		// No need to exchange data if there is only ONE mpp store. But the behavior of optimizer is unexpected if use broadcast way forcibly, such as tpch q4.
		// TODO: always use broadcast way to exchange data if there is only ONE mpp store.

		if err == nil && mppStoreCnt > 0 {
			if !(onlyCheckChild1 || onlyCheckChild0) {
				return isJoinFitMPPBCJ(p, mppStoreCnt)
			}
			if mppStoreCnt > 1 {
				if onlyCheckChild1 {
					return isJoinChildFitMPPBCJ(p, 1, mppStoreCnt)
				} else if onlyCheckChild0 {
					return isJoinChildFitMPPBCJ(p, 0, mppStoreCnt)
				}
			}
			// If mppStoreCnt is ONE and only need to check one child plan, rollback to original way.
			// Otherwise, the plan of tpch q4 may be unexpected.
		}
	}
	var stats0, stats1 *property.StatsInfo
	var schema0, schema1 *expression.Schema
	if ge != nil {
		stats0, stats1, schema0, schema1 = ge.GetJoinChildStatsAndSchema()
	} else {
		stats0, stats1, schema0, schema1 = p.GetJoinChildStatsAndSchema()
	}
	pctx := p.SCtx()
	if onlyCheckChild1 {
		return checkChildFitBC(pctx, stats1, schema1)
	} else if onlyCheckChild0 {
		return checkChildFitBC(pctx, stats0, schema0)
	}
	return checkChildFitBC(pctx, stats0, schema0) || checkChildFitBC(pctx, stats1, schema1)
}

func checkChildFitBC(pctx base.PlanContext, stats *property.StatsInfo, schema *expression.Schema) bool {
	if stats.HistColl == nil {
		return pctx.GetSessionVars().BroadcastJoinThresholdCount == -1 || stats.Count() < pctx.GetSessionVars().BroadcastJoinThresholdCount
	}
	avg := cardinality.GetAvgRowSize(pctx, stats.HistColl, schema.Columns, false, false)
	sz := avg * float64(stats.Count())
	return pctx.GetSessionVars().BroadcastJoinThresholdSize == -1 || sz < float64(pctx.GetSessionVars().BroadcastJoinThresholdSize)
}

func calcHashExchangeSizeByChild(p1 base.Plan, p2 base.Plan, mppStoreCnt int) (row float64, size float64, hasSize bool) {
	row1, size1, hasSize1 := calcHashExchangeSize(p1, mppStoreCnt)
	row2, size2, hasSize2 := calcHashExchangeSize(p2, mppStoreCnt)

	// hash exchange size:
	//   Build: sizeof(BuildTable) * (mppStoreCnt - 1) / mppStoreCnt
	//   Probe: sizeof(ProbeTable) * (mppStoreCnt - 1) / mppStoreCnt

	if hasSize1 && hasSize2 {
		return row1 + row2, size1 + size2, true
	}
	return row1 + row2, 0, false
}

func calcHashExchangeSize(p base.Plan, mppStoreCnt int) (row float64, sz float64, hasSize bool) {
	s := p.StatsInfo()
	row = float64(s.Count()) * float64(mppStoreCnt-1) / float64(mppStoreCnt)
	if s.HistColl == nil {
		return row, 0, false
	}
	avg := cardinality.GetAvgRowSize(p.SCtx(), s.HistColl, p.Schema().Columns, false, false)
	sz = avg * row
	return row, sz, true
}

// The size of `Build` hash table when using broadcast join is about `X`.
// The size of `Build` hash table when using shuffle join is about `X / (mppStoreCnt)`.
// It will cost more time to construct `Build` hash table and search `Probe` while using broadcast join.
// Set a scale factor (`mppStoreCnt^*`) when estimating broadcast join in `isJoinFitMPPBCJ` and `isJoinChildFitMPPBCJ` (based on TPCH benchmark, it has been verified in Q9).

func isJoinFitMPPBCJ(p *logicalop.LogicalJoin, mppStoreCnt int) bool {
	rowBC, szBC, hasSizeBC := calcBroadcastExchangeSizeByChild(p.Children()[0], p.Children()[1], mppStoreCnt)
	rowHash, szHash, hasSizeHash := calcHashExchangeSizeByChild(p.Children()[0], p.Children()[1], mppStoreCnt)
	if hasSizeBC && hasSizeHash {
		return szBC*float64(mppStoreCnt) <= szHash
	}
	return rowBC*float64(mppStoreCnt) <= rowHash
}

func calcBroadcastExchangeSizeByChild(p1 base.Plan, p2 base.Plan, mppStoreCnt int) (row float64, size float64, hasSize bool) {
	row1, size1, hasSize1 := calcBroadcastExchangeSize(p1, mppStoreCnt)
	row2, size2, hasSize2 := calcBroadcastExchangeSize(p2, mppStoreCnt)

	// broadcast exchange size:
	//   Build: (mppStoreCnt - 1) * sizeof(BuildTable)
	//   Probe: 0
	// choose the child plan with the maximum approximate value as Probe

	if hasSize1 && hasSize2 {
		return math.Min(row1, row2), math.Min(size1, size2), true
	}

	return math.Min(row1, row2), 0, false
}

func calcBroadcastExchangeSize(p base.Plan, mppStoreCnt int) (row float64, size float64, hasSize bool) {
	s := p.StatsInfo()
	row = float64(s.Count()) * float64(mppStoreCnt-1)
	if s.HistColl == nil {
		return row, 0, false
	}
	avg := cardinality.GetAvgRowSize(p.SCtx(), s.HistColl, p.Schema().Columns, false, false)
	size = avg * row
	return row, size, true
}
