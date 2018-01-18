// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"math"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/ranger"
)

func (p *LogicalUnionScan) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	us := PhysicalUnionScan{Conditions: p.conditions}.init(p.ctx, p.stats, prop)
	return []PhysicalPlan{us}
}

func getPermutation(cols1, cols2 []*expression.Column) ([]int, []*expression.Column) {
	tmpSchema := expression.NewSchema(cols2...)
	permutation := make([]int, 0, len(cols1))
	for i, col1 := range cols1 {
		offset := tmpSchema.ColumnIndex(col1)
		if offset == -1 {
			return permutation, cols1[:i]
		}
		permutation = append(permutation, offset)
	}
	return permutation, cols1
}

func findMaxPrefixLen(candidates [][]*expression.Column, keys []*expression.Column) int {
	maxLen := 0
	for _, candidateKeys := range candidates {
		matchedLen := 0
		for i := range keys {
			if i < len(candidateKeys) && keys[i].Equal(candidateKeys[i], nil) {
				matchedLen++
			} else {
				break
			}
		}
		if matchedLen > maxLen {
			maxLen = matchedLen
		}
	}
	return maxLen
}

func (p *LogicalJoin) getEqAndOtherCondsByOffsets(offsets []int) ([]*expression.ScalarFunction, []expression.Expression) {
	var (
		eqConds    = make([]*expression.ScalarFunction, 0, len(p.EqualConditions))
		otherConds = make([]expression.Expression, len(p.OtherConditions))
	)
	copy(otherConds, p.OtherConditions)
	for i, eqCond := range p.EqualConditions {
		match := false
		for _, offset := range offsets {
			if i == offset {
				match = true
				break
			}
		}
		if !match {
			otherConds = append(otherConds, eqCond)
		} else {
			eqConds = append(eqConds, eqCond)
		}
	}
	return eqConds, otherConds
}

// Only if the input required prop is the prefix fo join keys, we can pass through this property.
func (p *PhysicalMergeJoin) tryToGetChildReqProp(prop *requiredProp) ([]*requiredProp, bool) {
	lProp := &requiredProp{taskTp: rootTaskType, cols: p.leftKeys, expectedCnt: math.MaxFloat64}
	rProp := &requiredProp{taskTp: rootTaskType, cols: p.rightKeys, expectedCnt: math.MaxFloat64}
	if !prop.isEmpty() {
		// sort merge join fits the cases of massive ordered data, so desc scan is always expensive.
		if prop.desc {
			return nil, false
		}
		if !prop.isPrefix(lProp) && !prop.isPrefix(rProp) {
			return nil, false
		}
		if prop.isPrefix(rProp) && p.JoinType == LeftOuterJoin {
			return nil, false
		}
		if prop.isPrefix(lProp) && p.JoinType == RightOuterJoin {
			return nil, false
		}
	}

	return []*requiredProp{lProp, rProp}, true
}

func (p *LogicalJoin) getMergeJoin(prop *requiredProp) []PhysicalPlan {
	joins := make([]PhysicalPlan, 0, len(p.leftProperties))
	// The leftProperties caches all the possible properties that are provided by its children.
	for _, leftCols := range p.leftProperties {
		offsets, leftKeys := getPermutation(leftCols, p.LeftJoinKeys)
		if len(offsets) == 0 {
			continue
		}
		rightKeys := expression.NewSchema(p.RightJoinKeys...).ColumnsByIndices(offsets)
		prefixLen := findMaxPrefixLen(p.rightProperties, rightKeys)
		if prefixLen == 0 {
			continue
		}
		leftKeys = leftKeys[:prefixLen]
		rightKeys = rightKeys[:prefixLen]
		offsets = offsets[:prefixLen]
		mergeJoin := PhysicalMergeJoin{
			JoinType:        p.JoinType,
			LeftConditions:  p.LeftConditions,
			RightConditions: p.RightConditions,
			DefaultValues:   p.DefaultValues,
			leftKeys:        leftKeys,
			rightKeys:       rightKeys,
		}.init(p.ctx, p.stats.scaleByExpectCnt(prop.expectedCnt))
		mergeJoin.SetSchema(p.schema)
		mergeJoin.EqualConditions, mergeJoin.OtherConditions = p.getEqAndOtherCondsByOffsets(offsets)
		if reqProps, ok := mergeJoin.tryToGetChildReqProp(prop); ok {
			mergeJoin.childrenReqProps = reqProps
			joins = append(joins, mergeJoin)
		}
	}
	return joins
}

func (p *LogicalJoin) getHashJoins(prop *requiredProp) []PhysicalPlan {
	if !prop.isEmpty() { // hash join doesn't promise any orders
		return nil
	}
	joins := make([]PhysicalPlan, 0, 2)
	switch p.JoinType {
	case SemiJoin, AntiSemiJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin, LeftOuterJoin:
		joins = append(joins, p.getHashJoin(prop, 1))
	case RightOuterJoin:
		joins = append(joins, p.getHashJoin(prop, 0))
	case InnerJoin:
		joins = append(joins, p.getHashJoin(prop, 1))
		joins = append(joins, p.getHashJoin(prop, 0))
	}
	return joins
}

func (p *LogicalJoin) getHashJoin(prop *requiredProp, innerIdx int) *PhysicalHashJoin {
	chReqProps := make([]*requiredProp, 2)
	chReqProps[innerIdx] = &requiredProp{expectedCnt: math.MaxFloat64}
	chReqProps[1-innerIdx] = &requiredProp{expectedCnt: prop.expectedCnt}
	hashJoin := PhysicalHashJoin{
		EqualConditions: p.EqualConditions,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		JoinType:        p.JoinType,
		Concurrency:     JoinConcurrency,
		DefaultValues:   p.DefaultValues,
		SmallChildIdx:   innerIdx,
	}.init(p.ctx, p.stats.scaleByExpectCnt(prop.expectedCnt), chReqProps...)
	hashJoin.SetSchema(p.schema)
	return hashJoin
}

// joinKeysMatchIndex checks if all keys match columns in index.
// It returns a slice a[] what a[i] means keys[i] is related with indexCols[a[i]].
func joinKeysMatchIndex(keys, indexCols []*expression.Column, colLengths []int) []int {
	if len(indexCols) < len(keys) {
		return nil
	}
	keyOff2IdxOff := make([]int, len(keys))
	for keyOff, key := range keys {
		idxOff := joinKeyMatchIndexCol(key, indexCols, colLengths)
		if idxOff == -1 {
			return nil
		}
		keyOff2IdxOff[keyOff] = idxOff
	}
	return keyOff2IdxOff
}

func joinKeyMatchIndexCol(key *expression.Column, indexCols []*expression.Column, colLengths []int) int {
	for idxOff, idxCol := range indexCols {
		if colLengths[idxOff] != types.UnspecifiedLength {
			continue
		}
		if idxCol.ColName.L == key.ColName.L {
			return idxOff
		}
	}
	return -1
}

// When inner plan is TableReader, the last two parameter will be nil
func (p *LogicalJoin) constructIndexJoin(prop *requiredProp, innerJoinKeys, outerJoinKeys []*expression.Column, outerIdx int,
	innerPlan PhysicalPlan, ranges []*ranger.NewRange, keyOff2IdxOff []int) []PhysicalPlan {
	joinType := p.JoinType
	outerSchema := p.children[outerIdx].Schema()
	// If the order by columns are not all from outer child, index join cannot promise the order.
	if !prop.allColsFromSchema(outerSchema) {
		return nil
	}
	chReqProps := make([]*requiredProp, 2)
	chReqProps[outerIdx] = &requiredProp{taskTp: rootTaskType, expectedCnt: prop.expectedCnt, cols: prop.cols, desc: prop.desc}
	join := PhysicalIndexJoin{
		OuterIndex:      outerIdx,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		JoinType:        joinType,
		OuterJoinKeys:   outerJoinKeys,
		InnerJoinKeys:   innerJoinKeys,
		DefaultValues:   p.DefaultValues,
		innerPlan:       innerPlan,
		KeyOff2IdxOff:   keyOff2IdxOff,
		Ranges:          ranges,
	}.init(p.ctx, p.stats.scaleByExpectCnt(prop.expectedCnt), chReqProps...)
	join.SetSchema(p.schema)
	return []PhysicalPlan{join}
}

// getIndexJoinByOuterIdx will generate index join by outerIndex. OuterIdx points out the outer child.
// First of all, we'll check whether the inner child is DataSource.
// Then, we will extract the join keys of p's equal conditions. Then check whether all of them are just the primary key
// or match some part of on index. If so we will choose the best one and construct a index join.
func (p *LogicalJoin) getIndexJoinByOuterIdx(prop *requiredProp, outerIdx int) []PhysicalPlan {
	innerChild := p.children[1-outerIdx]
	var (
		innerJoinKeys []*expression.Column
		outerJoinKeys []*expression.Column
	)
	if outerIdx == 0 {
		outerJoinKeys = p.LeftJoinKeys
		innerJoinKeys = p.RightJoinKeys
	} else {
		innerJoinKeys = p.LeftJoinKeys
		outerJoinKeys = p.RightJoinKeys
	}
	x, ok := innerChild.(*DataSource)
	if !ok {
		return nil
	}
	indices := x.availableIndices.indices
	includeTableScan := x.availableIndices.includeTableScan
	if includeTableScan && len(innerJoinKeys) == 1 {
		pkCol := x.getPKIsHandleCol()
		if pkCol != nil && innerJoinKeys[0].Equal(pkCol, nil) {
			innerPlan := x.forceToTableScan(pkCol)
			return p.constructIndexJoin(prop, innerJoinKeys, outerJoinKeys, outerIdx, innerPlan, nil, nil)
		}
	}
	var (
		bestIndexInfo  *model.IndexInfo
		rangesOfBest   []*ranger.NewRange
		maxUsedCols    int
		remainedOfBest []expression.Expression
		keyOff2IdxOff  []int
	)
	for _, indexInfo := range indices {
		ranges, remained, tmpKeyOff2IdxOff := p.buildRangeForIndexJoin(indexInfo, x, innerJoinKeys)
		// We choose the index by the number of used columns of the range, the much the better.
		// Notice that there may be the cases like `t1.a=t2.a and b > 2 and b < 1`. So ranges can be nil though the conditions are valid.
		// But obviously when the range is nil, we don't need index join.
		if len(ranges) > 0 && len(ranges[0].LowVal) > maxUsedCols {
			bestIndexInfo = indexInfo
			maxUsedCols = len(ranges[0].LowVal)
			rangesOfBest = ranges
			remainedOfBest = remained
			keyOff2IdxOff = tmpKeyOff2IdxOff
		}
	}
	if bestIndexInfo != nil {
		innerPlan := x.forceToIndexScan(bestIndexInfo, remainedOfBest)
		return p.constructIndexJoin(prop, innerJoinKeys, outerJoinKeys, outerIdx, innerPlan, rangesOfBest, keyOff2IdxOff)
	}
	return nil
}

// buildRangeForIndexJoin checks whether this index can be used for building index join and return the range if this index is ok.
// If this index is invalid, just return nil range.
func (p *LogicalJoin) buildRangeForIndexJoin(indexInfo *model.IndexInfo, innerPlan *DataSource, innerJoinKeys []*expression.Column) (
	[]*ranger.NewRange, []expression.Expression, []int) {
	idxCols, colLengths := expression.IndexInfo2Cols(innerPlan.Schema().Columns, indexInfo)
	if len(idxCols) == 0 {
		return nil, nil, nil
	}

	accesses, remained, keyOff2IdxOff := p.buildAccessCondsForIndexJoin(innerJoinKeys, idxCols, colLengths, innerPlan)

	// If there's no access condition, this index should be useless.
	if len(accesses) == 0 {
		return nil, nil, nil
	}

	ranges, err := ranger.BuildIndexRange(p.ctx.GetSessionVars().StmtCtx, idxCols, colLengths, accesses)
	if err != nil {
		terror.Log(errors.Trace(err))
		return nil, nil, nil
	}
	return ranges, remained, keyOff2IdxOff
}

func (p *LogicalJoin) buildAccessCondsForIndexJoin(keys, idxCols []*expression.Column, colLengths []int,
	innerPlan *DataSource) (accesses, remained []expression.Expression, keyOff2IdxOff []int) {
	// Check whether all join keys match one column from index.
	keyOff2IdxOff = joinKeysMatchIndex(keys, idxCols, colLengths)
	if keyOff2IdxOff == nil {
		return nil, nil, nil
	}

	// After predicate push down, the one side conditions of join must be the conditions that cannot be pushed down and
	// cannot calculate range either. So we only need the innerPlan.pushedDownConds and the eq conditions that we generate.
	// TODO: There may be a selection that block the index join.
	conds := make([]expression.Expression, 0, len(keys)+len(innerPlan.pushedDownConds))
	eqConds := make([]expression.Expression, 0, len(keys))
	// Construct a fake equal expression for calculating the range.
	for _, key := range keys {
		// Int datum 1 can convert to all column's type(numeric type, string type, json, time type, enum, set) safely.
		fakeConstant := &expression.Constant{Value: types.NewIntDatum(1), RetType: key.GetType()}
		eqFunc := expression.NewFunctionInternal(p.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), key, fakeConstant)
		conds = append(conds, eqFunc)
		eqConds = append(eqConds, eqFunc)
	}

	conds = append(conds, innerPlan.pushedDownConds...)
	// After constant propagation, there won'be cases that t1.a=t2.a and t2.a=1 occur in the same time.
	// And if there're cases like t1.a=t2.a and t1.a > 1, we can also guarantee that t1.a > 1 won't be chosen as access condition.
	// So DetachIndexConditions won't miss the equal conditions we generate.
	accesses, remained = ranger.DetachIndexConditions(conds, idxCols, colLengths)

	// We should guarantee that all the join's equal condition is used. Check that last one is in the access conditions is enough.
	// Here the last means that the corresponding index column's position is maximum.
	for _, eqCond := range eqConds {
		if !expression.Contains(accesses, eqCond) {
			return nil, nil, nil
		}
	}

	return accesses, remained, keyOff2IdxOff
}

// tryToGetIndexJoin will get index join by hints. If we can generate a valid index join by hint, the second return value
// will be true, which means we force to choose this index join. Otherwise we will select a join algorithm with min-cost.
func (p *LogicalJoin) tryToGetIndexJoin(prop *requiredProp) ([]PhysicalPlan, bool) {
	if len(p.EqualConditions) == 0 {
		return nil, false
	}
	plans := make([]PhysicalPlan, 0, 2)
	leftOuter := (p.preferJoinType & preferLeftAsIndexOuter) > 0
	rightOuter := (p.preferJoinType & preferRightAsIndexOuter) > 0
	switch p.JoinType {
	case SemiJoin, AntiSemiJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin, LeftOuterJoin:
		join := p.getIndexJoinByOuterIdx(prop, 0)
		if join != nil {
			// If the plan is not nil and matches the hint, return it directly.
			if leftOuter {
				return join, true
			}
			plans = append(plans, join...)
		}
	case RightOuterJoin:
		join := p.getIndexJoinByOuterIdx(prop, 1)
		if join != nil {
			// If the plan is not nil and matches the hint, return it directly.
			if rightOuter {
				return join, true
			}
			plans = append(plans, join...)
		}
	case InnerJoin:
		join := p.getIndexJoinByOuterIdx(prop, 0)
		if join != nil {
			// If the plan is not nil and matches the hint, return it directly.
			if leftOuter {
				return join, true
			}
			plans = append(plans, join...)
		}
		join = p.getIndexJoinByOuterIdx(prop, 1)
		if join != nil {
			// If the plan is not nil and matches the hint, return it directly.
			if rightOuter {
				return join, true
			}
			plans = append(plans, join...)
		}
	}
	return plans, false
}

// LogicalJoin can generates hash join, index join and sort merge join.
// Firstly we check the hint, if hint is figured by user, we force to choose the corresponding physical plan.
// If the hint is not matched, it will get other candidates.
// If the hint is not figured, we will pick all candidates.
func (p *LogicalJoin) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	mergeJoins := p.getMergeJoin(prop)
	if (p.preferJoinType&preferMergeJoin) > 0 && len(mergeJoins) > 0 {
		return mergeJoins
	}
	joins := make([]PhysicalPlan, 0, 5)
	joins = append(joins, mergeJoins...)

	indexJoins, forced := p.tryToGetIndexJoin(prop)
	if forced {
		return indexJoins
	}
	joins = append(joins, indexJoins...)

	hashJoins := p.getHashJoins(prop)
	if (p.preferJoinType & preferHashJoin) > 0 {
		return hashJoins
	}
	joins = append(joins, hashJoins...)
	return joins
}

// tryToGetChildProp will check if this sort property can be pushed or not.
// When a sort column will be replaced by scalar function, we refuse it.
// When a sort column will be replaced by a constant, we just remove it.
func (p *LogicalProjection) tryToGetChildProp(prop *requiredProp) (*requiredProp, bool) {
	newProp := &requiredProp{taskTp: rootTaskType, expectedCnt: prop.expectedCnt}
	newCols := make([]*expression.Column, 0, len(prop.cols))
	for _, col := range prop.cols {
		idx := p.schema.ColumnIndex(col)
		switch expr := p.Exprs[idx].(type) {
		case *expression.Column:
			newCols = append(newCols, expr)
		case *expression.ScalarFunction:
			return nil, false
		}
	}
	newProp.cols = newCols
	newProp.desc = prop.desc
	return newProp, true
}

func (p *LogicalProjection) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	newProp, ok := p.tryToGetChildProp(prop)
	if !ok {
		return nil
	}
	proj := PhysicalProjection{
		Exprs:            p.Exprs,
		CalculateNoDelay: p.calculateNoDelay,
	}.init(p.ctx, p.stats.scaleByExpectCnt(prop.expectedCnt), newProp)
	proj.SetSchema(p.schema)
	return []PhysicalPlan{proj}
}

func (lt *LogicalTopN) getPhysTopN() []PhysicalPlan {
	ret := make([]PhysicalPlan, 0, 3)
	for _, tp := range wholeTaskTypes {
		resultProp := &requiredProp{taskTp: tp, expectedCnt: math.MaxFloat64}
		topN := PhysicalTopN{
			ByItems: lt.ByItems,
			Count:   lt.Count,
			Offset:  lt.Offset,
			partial: lt.partial,
		}.init(lt.ctx, lt.stats, resultProp)
		ret = append(ret, topN)
	}
	return ret
}

func (lt *LogicalTopN) getPhysLimits() []PhysicalPlan {
	prop, canPass := getPropByOrderByItems(lt.ByItems)
	if !canPass {
		return nil
	}
	ret := make([]PhysicalPlan, 0, 3)
	for _, tp := range wholeTaskTypes {
		resultProp := &requiredProp{taskTp: tp, expectedCnt: float64(lt.Count + lt.Offset), cols: prop.cols, desc: prop.desc}
		limit := PhysicalLimit{
			Count:   lt.Count,
			Offset:  lt.Offset,
			partial: lt.partial,
		}.init(lt.ctx, lt.stats, resultProp)
		ret = append(ret, limit)
	}
	return ret
}

func (lt *LogicalTopN) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	if prop.matchItems(lt.ByItems) {
		return append(lt.getPhysTopN(), lt.getPhysLimits()...)
	}
	return nil
}

func (la *LogicalApply) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	if !prop.allColsFromSchema(la.children[0].Schema()) { // for convenient, we don't pass through any prop
		return nil
	}
	apply := PhysicalApply{
		PhysicalJoin:  la.getHashJoin(prop, 1),
		OuterSchema:   la.corCols,
		rightChOffset: la.children[0].Schema().Len(),
	}.init(la.ctx,
		la.stats.scaleByExpectCnt(prop.expectedCnt),
		&requiredProp{expectedCnt: math.MaxFloat64, cols: prop.cols, desc: prop.desc},
		&requiredProp{expectedCnt: math.MaxFloat64})
	apply.SetSchema(la.schema)
	return []PhysicalPlan{apply}
}

// genPhysPlansByReqProp is only for implementing interface. DataSource and Dual generate task in `convert2PhysicalPlan` directly.
func (p *baseLogicalPlan) genPhysPlansByReqProp(_ *requiredProp) []PhysicalPlan {
	panic("This function should not be called")
}

func (la *LogicalAggregation) getStreamAggs(prop *requiredProp) []PhysicalPlan {
	if len(la.possibleProperties) == 0 {
		return nil
	}
	for _, aggFunc := range la.AggFuncs {
		if aggFunc.Mode == aggregation.FinalMode {
			return nil
		}
	}
	// group by a + b is not interested in any order.
	if len(la.groupByCols) != len(la.GroupByItems) {
		return nil
	}
	streamAggs := make([]PhysicalPlan, 0, len(la.possibleProperties)*2)
	for _, cols := range la.possibleProperties {
		_, keys := getPermutation(cols, la.groupByCols)
		if len(keys) != len(la.groupByCols) {
			continue
		}
		for _, tp := range wholeTaskTypes {
			// Second read in the double can't meet the stream aggregation's require prop.
			if tp == copDoubleReadTaskType {
				continue
			}
			// Now we only support pushing down stream aggregation on mocktikv.
			// TODO: Remove it after TiKV supports stream aggregation.
			if tp == copSingleReadTaskType {
				client := la.ctx.GetClient()
				if !client.IsRequestTypeSupported(kv.ReqTypeDAG, kv.ReqSubTypeStreamAgg) {
					continue
				}
			}
			childProp := &requiredProp{
				taskTp:      tp,
				cols:        keys,
				desc:        prop.desc,
				expectedCnt: prop.expectedCnt * la.inputCount / la.stats.count,
			}
			if !prop.isPrefix(childProp) {
				continue
			}
			agg := basePhysicalAgg{
				GroupByItems: la.GroupByItems,
				AggFuncs:     la.AggFuncs,
			}.initForStream(la.ctx, la.stats.scaleByExpectCnt(prop.expectedCnt), childProp)
			agg.SetSchema(la.schema.Clone())
			streamAggs = append(streamAggs, agg)
		}
	}
	return streamAggs
}

func (la *LogicalAggregation) getHashAggs(prop *requiredProp) []PhysicalPlan {
	if !prop.isEmpty() {
		return nil
	}
	hashAggs := make([]PhysicalPlan, 0, len(wholeTaskTypes))
	for _, taskTp := range wholeTaskTypes {
		agg := basePhysicalAgg{
			GroupByItems: la.GroupByItems,
			AggFuncs:     la.AggFuncs,
		}.initForHash(la.ctx, la.stats.scaleByExpectCnt(prop.expectedCnt), &requiredProp{expectedCnt: math.MaxFloat64, taskTp: taskTp})
		agg.SetSchema(la.schema.Clone())
		hashAggs = append(hashAggs, agg)
	}
	return hashAggs
}

func (la *LogicalAggregation) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	aggs := make([]PhysicalPlan, 0, len(la.possibleProperties)+1)
	aggs = append(aggs, la.getHashAggs(prop)...)

	streamAggs := la.getStreamAggs(prop)
	aggs = append(aggs, streamAggs...)

	return aggs
}

func (p *LogicalSelection) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	sel := PhysicalSelection{
		Conditions: p.Conditions,
	}.init(p.ctx, p.stats.scaleByExpectCnt(prop.expectedCnt), prop)
	return []PhysicalPlan{sel}
}

func (p *LogicalLimit) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	if !prop.isEmpty() {
		return nil
	}
	ret := make([]PhysicalPlan, 0, len(wholeTaskTypes))
	for _, tp := range wholeTaskTypes {
		resultProp := &requiredProp{taskTp: tp, expectedCnt: float64(p.Count + p.Offset)}
		limit := PhysicalLimit{
			Offset:  p.Offset,
			Count:   p.Count,
			partial: p.partial,
		}.init(p.ctx, p.stats, resultProp)
		ret = append(ret, limit)
	}
	return ret
}

func (p *LogicalLock) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	lock := PhysicalLock{
		Lock: p.Lock,
	}.init(p.ctx, p.stats.scaleByExpectCnt(prop.expectedCnt), prop)
	return []PhysicalPlan{lock}
}

func (p *LogicalUnionAll) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	// TODO: UnionAll can not pass any order, but we can change it to sort merge to keep order.
	if !prop.isEmpty() {
		return nil
	}
	chReqProps := make([]*requiredProp, 0, len(p.children))
	for range p.children {
		chReqProps = append(chReqProps, &requiredProp{expectedCnt: prop.expectedCnt})
	}
	ua := PhysicalUnionAll{}.init(p.ctx, p.stats.scaleByExpectCnt(prop.expectedCnt), chReqProps...)
	return []PhysicalPlan{ua}
}

func (ls *LogicalSort) getPhysicalSort(prop *requiredProp) *PhysicalSort {
	ps := PhysicalSort{ByItems: ls.ByItems}.init(ls.ctx, ls.stats.scaleByExpectCnt(prop.expectedCnt), &requiredProp{expectedCnt: math.MaxFloat64})
	return ps
}

func (ls *LogicalSort) getNominalSort(reqProp *requiredProp) *NominalSort {
	prop, canPass := getPropByOrderByItems(ls.ByItems)
	if !canPass {
		return nil
	}
	prop.expectedCnt = reqProp.expectedCnt
	ps := NominalSort{}.init(ls.ctx, prop)
	return ps
}

func (ls *LogicalSort) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	if prop.matchItems(ls.ByItems) {
		ret := make([]PhysicalPlan, 0, 2)
		ret = append(ret, ls.getPhysicalSort(prop))
		ns := ls.getNominalSort(prop)
		if ns != nil {
			ret = append(ret, ns)
		}
		return ret
	}
	return nil
}

func (p *LogicalExists) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	if !prop.isEmpty() {
		return nil
	}
	exists := PhysicalExists{}.init(p.ctx, p.stats, &requiredProp{expectedCnt: 1})
	exists.SetSchema(p.schema)
	return []PhysicalPlan{exists}
}

func (p *LogicalMaxOneRow) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	if !prop.isEmpty() {
		return nil
	}
	mor := PhysicalMaxOneRow{}.init(p.ctx, p.stats, &requiredProp{expectedCnt: 2})
	return []PhysicalPlan{mor}
}
