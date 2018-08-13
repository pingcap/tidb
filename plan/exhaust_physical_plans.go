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
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/ranger"
)

func (p *LogicalUnionScan) exhaustPhysicalPlans(prop *requiredProp) []PhysicalPlan {
	us := PhysicalUnionScan{Conditions: p.conditions}.init(p.ctx, p.stats, prop)
	return []PhysicalPlan{us}
}

func getMaxSortPrefix(sortCols, allCols []*expression.Column) []int {
	tmpSchema := expression.NewSchema(allCols...)
	sortColOffsets := make([]int, 0, len(sortCols))
	for _, sortCol := range sortCols {
		offset := tmpSchema.ColumnIndex(sortCol)
		if offset == -1 {
			return sortColOffsets
		}
		sortColOffsets = append(sortColOffsets, offset)
	}
	return sortColOffsets
}

func findMaxPrefixLen(candidates [][]*expression.Column, keys []*expression.Column) int {
	maxLen := 0
	for _, candidateKeys := range candidates {
		matchedLen := 0
		for i := range keys {
			if i < len(candidateKeys) && keys[i].Equal(nil, candidateKeys[i]) {
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

func (p *LogicalJoin) moveEqualToOtherConditions(offsets []int) []expression.Expression {
	otherConds := make([]expression.Expression, len(p.OtherConditions))
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
		}
	}
	return otherConds
}

// Only if the input required prop is the prefix fo join keys, we can pass through this property.
func (p *PhysicalMergeJoin) tryToGetChildReqProp(prop *requiredProp) ([]*requiredProp, bool) {
	lProp := &requiredProp{taskTp: rootTaskType, cols: p.LeftKeys, expectedCnt: math.MaxFloat64}
	rProp := &requiredProp{taskTp: rootTaskType, cols: p.RightKeys, expectedCnt: math.MaxFloat64}
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
	for _, lhsChildProperty := range p.leftProperties {
		offsets := getMaxSortPrefix(lhsChildProperty, p.LeftJoinKeys)
		if len(offsets) == 0 {
			continue
		}

		leftKeys := lhsChildProperty[:len(offsets)]
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
			LeftKeys:        leftKeys,
			RightKeys:       rightKeys,
		}.init(p.ctx, p.stats.scaleByExpectCnt(prop.expectedCnt))
		mergeJoin.SetSchema(p.schema)
		mergeJoin.OtherConditions = p.moveEqualToOtherConditions(offsets)
		if reqProps, ok := mergeJoin.tryToGetChildReqProp(prop); ok {
			mergeJoin.childrenReqProps = reqProps
			joins = append(joins, mergeJoin)
		}
	}
	// If TiDB_SMJ hint is existed && no join keys in children property,
	// it should to enforce merge join.
	if len(joins) == 0 && (p.preferJoinType&preferMergeJoin) > 0 {
		return p.getEnforcedMergeJoin(prop)
	}

	return joins
}

// Change JoinKeys order, by offsets array
// offsets array is generate by prop check
func getNewJoinKeysByOffsets(oldJoinKeys []*expression.Column, offsets []int) []*expression.Column {
	newKeys := make([]*expression.Column, 0, len(oldJoinKeys))
	for _, offset := range offsets {
		newKeys = append(newKeys, oldJoinKeys[offset])
	}
	for pos, key := range oldJoinKeys {
		isExist := false
		for _, p := range offsets {
			if p == pos {
				isExist = true
				break
			}
		}
		if !isExist {
			newKeys = append(newKeys, key)
		}
	}
	return newKeys
}

// Change EqualConditions order, by offsets array
// offsets array is generate by prop check
func getNewEqualConditionsByOffsets(oldEqualCond []*expression.ScalarFunction, offsets []int) []*expression.ScalarFunction {
	newEqualCond := make([]*expression.ScalarFunction, 0, len(oldEqualCond))
	for _, offset := range offsets {
		newEqualCond = append(newEqualCond, oldEqualCond[offset])
	}
	for pos, condition := range oldEqualCond {
		isExist := false
		for _, p := range offsets {
			if p == pos {
				isExist = true
				break
			}
		}
		if !isExist {
			newEqualCond = append(newEqualCond, condition)
		}
	}
	return newEqualCond
}

func (p *LogicalJoin) getEnforcedMergeJoin(prop *requiredProp) []PhysicalPlan {
	// Check whether SMJ can satisfy the required property
	offsets := make([]int, 0, len(p.LeftJoinKeys))
	for _, col := range prop.cols {
		isExist := false
		for joinKeyPos := 0; joinKeyPos < len(p.LeftJoinKeys); joinKeyPos++ {
			var key *expression.Column
			if col.Equal(p.ctx, p.LeftJoinKeys[joinKeyPos]) {
				key = p.LeftJoinKeys[joinKeyPos]
			}
			if col.Equal(p.ctx, p.RightJoinKeys[joinKeyPos]) {
				key = p.RightJoinKeys[joinKeyPos]
			}
			if key == nil {
				continue
			}
			for i := 0; i < len(offsets); i++ {
				if offsets[i] == joinKeyPos {
					isExist = true
					break
				}
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
	}
	// Generate the enforced sort merge join
	leftKeys := getNewJoinKeysByOffsets(p.LeftJoinKeys, offsets)
	rightKeys := getNewJoinKeysByOffsets(p.RightJoinKeys, offsets)
	lProp := &requiredProp{taskTp: rootTaskType, cols: leftKeys, expectedCnt: math.MaxFloat64, enforced: true, desc: prop.desc}
	rProp := &requiredProp{taskTp: rootTaskType, cols: rightKeys, expectedCnt: math.MaxFloat64, enforced: true, desc: prop.desc}
	enforcedPhysicalMergeJoin := PhysicalMergeJoin{
		JoinType:        p.JoinType,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		DefaultValues:   p.DefaultValues,
		LeftKeys:        leftKeys,
		RightKeys:       rightKeys,
		OtherConditions: p.OtherConditions,
	}.init(p.ctx, p.stats.scaleByExpectCnt(prop.expectedCnt))
	enforcedPhysicalMergeJoin.SetSchema(p.schema)
	enforcedPhysicalMergeJoin.childrenReqProps = []*requiredProp{lProp, rProp}
	return []PhysicalPlan{enforcedPhysicalMergeJoin}
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
		Concurrency:     uint(p.ctx.GetSessionVars().HashJoinConcurrency),
		DefaultValues:   p.DefaultValues,
		InnerChildIdx:   innerIdx,
	}.init(p.ctx, p.stats.scaleByExpectCnt(prop.expectedCnt), chReqProps...)
	hashJoin.SetSchema(p.schema)
	return hashJoin
}

// joinKeysMatchIndex checks whether the join key is in the index.
// It returns a slice a[] what a[i] means keys[i] is related with indexCols[a[i]], -1 for no matching column.
// It will return nil if there's no column that matches index.
func joinKeysMatchIndex(keys, indexCols []*expression.Column, colLengths []int) []int {
	keyOff2IdxOff := make([]int, len(keys))
	for i := range keyOff2IdxOff {
		keyOff2IdxOff[i] = -1
	}
	// There should be at least one column in join keys which can match the index's column.
	matched := false
	tmpSchema := expression.NewSchema(keys...)
	for i, idxCol := range indexCols {
		if colLengths[i] != types.UnspecifiedLength {
			continue
		}
		keyOff := tmpSchema.ColumnIndex(idxCol)
		if keyOff == -1 {
			continue
		}
		matched = true
		keyOff2IdxOff[keyOff] = i
	}
	if !matched {
		return nil
	}
	return keyOff2IdxOff
}

// When inner plan is TableReader, the parameter `ranges` will be nil. Because pk only have one column. So all of its range
// is generated during execution time.
func (p *LogicalJoin) constructIndexJoin(prop *requiredProp, innerJoinKeys, outerJoinKeys []*expression.Column, outerIdx int,
	innerPlan PhysicalPlan, ranges []*ranger.Range, keyOff2IdxOff []int) []PhysicalPlan {
	joinType := p.JoinType
	outerSchema := p.children[outerIdx].Schema()
	// If the order by columns are not all from outer child, index join cannot promise the order.
	if !prop.allColsFromSchema(outerSchema) {
		return nil
	}
	chReqProps := make([]*requiredProp, 2)
	chReqProps[outerIdx] = &requiredProp{taskTp: rootTaskType, expectedCnt: prop.expectedCnt, cols: prop.cols, desc: prop.desc}
	newInnerKeys := make([]*expression.Column, 0, len(innerJoinKeys))
	newOuterKeys := make([]*expression.Column, 0, len(outerJoinKeys))
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
		newKeyOff = append(newKeyOff, idxOff)
	}
	join := PhysicalIndexJoin{
		OuterIndex:      outerIdx,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: newOtherConds,
		JoinType:        joinType,
		OuterJoinKeys:   newOuterKeys,
		InnerJoinKeys:   newInnerKeys,
		DefaultValues:   p.DefaultValues,
		innerPlan:       innerPlan,
		KeyOff2IdxOff:   newKeyOff,
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
	var tblPath *accessPath
	for _, path := range x.possibleAccessPaths {
		if path.isTablePath {
			tblPath = path
			break
		}
	}
	if pkCol := x.getPKIsHandleCol(); pkCol != nil && tblPath != nil {
		keyOff2IdxOff := make([]int, len(innerJoinKeys))
		pkCol := x.getPKIsHandleCol()
		pkMatched := false
		for i, key := range innerJoinKeys {
			if !key.Equal(nil, pkCol) {
				keyOff2IdxOff[i] = -1
				continue
			}
			pkMatched = true
			keyOff2IdxOff[i] = 0
		}
		if pkMatched {
			innerPlan := p.constructInnerTableScan(x, pkCol, outerJoinKeys)
			// Since the primary key means one value corresponding to exact one row, this will always be a no worse one
			// comparing to other index.
			return p.constructIndexJoin(prop, innerJoinKeys, outerJoinKeys, outerIdx, innerPlan, nil, keyOff2IdxOff)
		}
	}
	var (
		bestIndexInfo  *model.IndexInfo
		rangesOfBest   []*ranger.Range
		maxUsedCols    int
		remainedOfBest []expression.Expression
		keyOff2IdxOff  []int
	)
	for _, path := range x.possibleAccessPaths {
		if path.isTablePath {
			continue
		}
		indexInfo := path.index
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
		innerPlan := p.constructInnerIndexScan(x, bestIndexInfo, remainedOfBest, outerJoinKeys)
		return p.constructIndexJoin(prop, innerJoinKeys, outerJoinKeys, outerIdx, innerPlan, rangesOfBest, keyOff2IdxOff)
	}
	return nil
}

// constructInnerTableScan is specially used to construct the inner plan for PhysicalIndexJoin.
func (p *LogicalJoin) constructInnerTableScan(ds *DataSource, pk *expression.Column, outerJoinKeys []*expression.Column) PhysicalPlan {
	var ranges []*ranger.Range
	if pk != nil {
		ranges = ranger.FullIntRange(mysql.HasUnsignedFlag(pk.RetType.Flag))
	} else {
		ranges = ranger.FullIntRange(false)
	}
	ts := PhysicalTableScan{
		Table:           ds.tableInfo,
		Columns:         ds.Columns,
		TableAsName:     ds.TableAsName,
		DBName:          ds.DBName,
		filterCondition: ds.pushedDownConds,
		Ranges:          ranges,
		rangeDecidedBy:  outerJoinKeys,
	}.init(ds.ctx)
	ts.SetSchema(ds.schema)

	var rowCount float64
	pkHist, ok := ds.statisticTable.Columns[pk.ID]
	if ok && !ds.statisticTable.Pseudo {
		rowCount = pkHist.AvgCountPerValue(ds.statisticTable.Count)
	} else {
		rowCount = ds.statisticTable.PseudoAvgCountPerValue()
	}

	ts.stats = newSimpleStats(rowCount)
	ts.stats.usePseudoStats = ds.statisticTable.Pseudo

	copTask := &copTask{
		tablePlan:         ts,
		indexPlanFinished: true,
	}
	ts.addPushedDownSelection(copTask, ds.stats)
	t := finishCopTask(ds.ctx, copTask)
	return t.plan()
}

// constructInnerIndexScan is specially used to construct the inner plan for PhysicalIndexJoin.
func (p *LogicalJoin) constructInnerIndexScan(ds *DataSource, idx *model.IndexInfo, remainedConds []expression.Expression, outerJoinKeys []*expression.Column) PhysicalPlan {
	is := PhysicalIndexScan{
		Table:            ds.tableInfo,
		TableAsName:      ds.TableAsName,
		DBName:           ds.DBName,
		Columns:          ds.Columns,
		Index:            idx,
		dataSourceSchema: ds.schema,
		KeepOrder:        false,
		Ranges:           ranger.FullRange(),
		rangeDecidedBy:   outerJoinKeys,
	}.init(ds.ctx)
	is.filterCondition = remainedConds

	var rowCount float64
	idxHist, ok := ds.statisticTable.Indices[idx.ID]
	if ok && !ds.statisticTable.Pseudo {
		rowCount = idxHist.AvgCountPerValue(ds.statisticTable.Count)
	} else {
		rowCount = ds.statisticTable.PseudoAvgCountPerValue()
	}
	is.stats = newSimpleStats(rowCount)
	is.stats.usePseudoStats = ds.statisticTable.Pseudo

	cop := &copTask{
		indexPlan: is,
	}
	if !isCoveringIndex(is.Columns, is.Index.Columns, is.Table.PKIsHandle) {
		// On this way, it's double read case.
		ts := PhysicalTableScan{Columns: ds.Columns, Table: is.Table}.init(ds.ctx)
		ts.SetSchema(is.dataSourceSchema)
		cop.tablePlan = ts
	}

	is.initSchema(ds.id, idx, cop.tablePlan != nil)
	indexConds, tblConds := splitIndexFilterConditions(remainedConds, idx.Columns, ds.tableInfo)
	path := &accessPath{indexFilters: indexConds, tableFilters: tblConds, countAfterIndex: math.MaxFloat64}
	is.addPushedDownSelection(cop, ds, math.MaxFloat64, path)
	t := finishCopTask(ds.ctx, cop)
	return t.plan()
}

// buildRangeForIndexJoin checks whether this index can be used for building index join and return the range if this index is ok.
// If this index is invalid, just return nil range.
func (p *LogicalJoin) buildRangeForIndexJoin(indexInfo *model.IndexInfo, innerPlan *DataSource, innerJoinKeys []*expression.Column) (
	[]*ranger.Range, []expression.Expression, []int) {
	idxCols, colLengths := expression.IndexInfo2Cols(innerPlan.Schema().Columns, indexInfo)
	if len(idxCols) == 0 {
		return nil, nil, nil
	}

	conds, eqConds, keyOff2IdxOff := p.buildFakeEqCondsForIndexJoin(innerJoinKeys, idxCols, colLengths, innerPlan)

	if len(keyOff2IdxOff) == 0 {
		return nil, nil, nil
	}

	// After constant propagation, there won'be cases that t1.a=t2.a and t2.a=1 occur in the same time.
	// And if there're cases like t1.a=t2.a and t1.a > 1, we can also guarantee that t1.a > 1 won't be chosen as access condition.
	// So DetachCondAndBuildRangeForIndex won't miss the equal conditions we generate.
	ranges, accesses, remained, _, err := ranger.DetachCondAndBuildRangeForIndex(p.ctx, conds, idxCols, colLengths)
	if err != nil {
		terror.Log(errors.Trace(err))
		return nil, nil, nil
	}

	// We should guarantee that all the join's equal condition is used.
	for _, eqCond := range eqConds {
		if !expression.Contains(accesses, eqCond) {
			return nil, nil, nil
		}
	}

	return ranges, remained, keyOff2IdxOff
}

func (p *LogicalJoin) buildFakeEqCondsForIndexJoin(keys, idxCols []*expression.Column, colLengths []int,
	innerPlan *DataSource) (accesses, eqConds []expression.Expression, keyOff2IdxOff []int) {
	// Check whether all join keys match one column from index.
	keyOff2IdxOff = joinKeysMatchIndex(keys, idxCols, colLengths)
	if keyOff2IdxOff == nil {
		return nil, nil, nil
	}

	// After predicate push down, the one side conditions of join must be the conditions that cannot be pushed down and
	// cannot calculate range either. So we only need the innerPlan.pushedDownConds and the eq conditions that we generate.
	// TODO: There may be a selection that block the index join.
	conds := make([]expression.Expression, 0, len(keys)+len(innerPlan.pushedDownConds))
	eqConds = make([]expression.Expression, 0, len(keys))
	// Construct a fake equal expression for calculating the range.
	for i, key := range keys {
		if keyOff2IdxOff[i] < 0 {
			continue
		}
		// Int datum 1 can convert to all column's type(numeric type, string type, json, time type, enum, set) safely.
		fakeConstant := &expression.Constant{Value: types.NewIntDatum(1), RetType: key.GetType()}
		eqFunc := expression.NewFunctionInternal(p.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), key, fakeConstant)
		conds = append(conds, eqFunc)
		eqConds = append(eqConds, eqFunc)
	}

	conds = append(conds, innerPlan.pushedDownConds...)
	return conds, eqConds, keyOff2IdxOff
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
		lhsCardinality := p.Children()[0].statsInfo().Count()
		rhsCardinality := p.Children()[1].statsInfo().Count()

		leftJoins := p.getIndexJoinByOuterIdx(prop, 0)
		if leftOuter && leftJoins != nil {
			return leftJoins, true
		}

		rightJoins := p.getIndexJoinByOuterIdx(prop, 1)
		if rightOuter && rightJoins != nil {
			return rightJoins, true
		}

		if leftJoins != nil && lhsCardinality < rhsCardinality {
			return leftJoins, leftOuter
		}

		if rightJoins != nil && rhsCardinality < lhsCardinality {
			return rightJoins, rightOuter
		}

		plans = append(plans, leftJoins...)
		plans = append(plans, rightJoins...)
	}
	return plans, false
}

// LogicalJoin can generates hash join, index join and sort merge join.
// Firstly we check the hint, if hint is figured by user, we force to choose the corresponding physical plan.
// If the hint is not matched, it will get other candidates.
// If the hint is not figured, we will pick all candidates.
func (p *LogicalJoin) exhaustPhysicalPlans(prop *requiredProp) []PhysicalPlan {
	mergeJoins := p.getMergeJoin(prop)
	if (p.preferJoinType & preferMergeJoin) > 0 {
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

func (p *LogicalProjection) exhaustPhysicalPlans(prop *requiredProp) []PhysicalPlan {
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
			Count:  lt.Count,
			Offset: lt.Offset,
		}.init(lt.ctx, lt.stats, resultProp)
		ret = append(ret, limit)
	}
	return ret
}

func (lt *LogicalTopN) exhaustPhysicalPlans(prop *requiredProp) []PhysicalPlan {
	if prop.matchItems(lt.ByItems) {
		return append(lt.getPhysTopN(), lt.getPhysLimits()...)
	}
	return nil
}

func (la *LogicalApply) exhaustPhysicalPlans(prop *requiredProp) []PhysicalPlan {
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

// exhaustPhysicalPlans is only for implementing interface. DataSource and Dual generate task in `findBestTask` directly.
func (p *baseLogicalPlan) exhaustPhysicalPlans(_ *requiredProp) []PhysicalPlan {
	panic("baseLogicalPlan.exhaustPhysicalPlans() should never be called.")
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

	streamAggs := make([]PhysicalPlan, 0, len(la.possibleProperties)*(len(wholeTaskTypes)-1))
	childProp := &requiredProp{
		desc:        prop.desc,
		expectedCnt: math.Max(prop.expectedCnt*la.inputCount/la.stats.count, prop.expectedCnt),
	}

	for _, possibleChildProperty := range la.possibleProperties {
		sortColOffsets := getMaxSortPrefix(possibleChildProperty, la.groupByCols)
		if len(sortColOffsets) != len(la.groupByCols) {
			continue
		}

		childProp.cols = possibleChildProperty[:len(sortColOffsets)]
		if !prop.isPrefix(childProp) {
			continue
		}

		// The table read of "copDoubleReadTaskType" can't promises the sort
		// property that the stream aggregation required, no need to consider.
		for _, taskTp := range []taskType{copSingleReadTaskType, rootTaskType} {
			copiedChildProperty := new(requiredProp)
			*copiedChildProperty = *childProp // It's ok to not deep copy the "cols" field.
			copiedChildProperty.taskTp = taskTp

			agg := basePhysicalAgg{
				GroupByItems: la.GroupByItems,
				AggFuncs:     la.AggFuncs,
			}.initForStream(la.ctx, la.stats.scaleByExpectCnt(prop.expectedCnt), copiedChildProperty)
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

func (la *LogicalAggregation) exhaustPhysicalPlans(prop *requiredProp) []PhysicalPlan {
	aggs := make([]PhysicalPlan, 0, len(la.possibleProperties)+1)
	aggs = append(aggs, la.getHashAggs(prop)...)
	aggs = append(aggs, la.getStreamAggs(prop)...)
	return aggs
}

func (p *LogicalSelection) exhaustPhysicalPlans(prop *requiredProp) []PhysicalPlan {
	sel := PhysicalSelection{
		Conditions: p.Conditions,
	}.init(p.ctx, p.stats.scaleByExpectCnt(prop.expectedCnt), prop)
	return []PhysicalPlan{sel}
}

func (p *LogicalLimit) exhaustPhysicalPlans(prop *requiredProp) []PhysicalPlan {
	if !prop.isEmpty() {
		return nil
	}
	ret := make([]PhysicalPlan, 0, len(wholeTaskTypes))
	for _, tp := range wholeTaskTypes {
		resultProp := &requiredProp{taskTp: tp, expectedCnt: float64(p.Count + p.Offset)}
		limit := PhysicalLimit{
			Offset: p.Offset,
			Count:  p.Count,
		}.init(p.ctx, p.stats, resultProp)
		ret = append(ret, limit)
	}
	return ret
}

func (p *LogicalLock) exhaustPhysicalPlans(prop *requiredProp) []PhysicalPlan {
	lock := PhysicalLock{
		Lock: p.Lock,
	}.init(p.ctx, p.stats.scaleByExpectCnt(prop.expectedCnt), prop)
	return []PhysicalPlan{lock}
}

func (p *LogicalUnionAll) exhaustPhysicalPlans(prop *requiredProp) []PhysicalPlan {
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

func (ls *LogicalSort) exhaustPhysicalPlans(prop *requiredProp) []PhysicalPlan {
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

func (p *LogicalMaxOneRow) exhaustPhysicalPlans(prop *requiredProp) []PhysicalPlan {
	if !prop.isEmpty() {
		return nil
	}
	mor := PhysicalMaxOneRow{}.init(p.ctx, p.stats, &requiredProp{expectedCnt: 2})
	return []PhysicalPlan{mor}
}
