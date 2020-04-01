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

package core

import (
	"context"
	"fmt"
	"math"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/set"
	"go.uber.org/zap"
)

func (p *LogicalUnionScan) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	childProp := prop.Clone()
	us := PhysicalUnionScan{Conditions: p.conditions}.init(p.ctx, p.stats, childProp)
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

// Only if the input required prop is the prefix fo join keys, we can pass through this property.
func (p *PhysicalMergeJoin) tryToGetChildReqProp(prop *property.PhysicalProperty) ([]*property.PhysicalProperty, bool) {
	lProp := &property.PhysicalProperty{TaskTp: property.RootTaskType, Cols: p.LeftKeys, Desc: prop.Desc, ExpectedCnt: math.MaxFloat64}
	rProp := &property.PhysicalProperty{TaskTp: property.RootTaskType, Cols: p.RightKeys, Desc: prop.Desc, ExpectedCnt: math.MaxFloat64}
	if !prop.IsEmpty() {
		if !prop.IsPrefix(lProp) && !prop.IsPrefix(rProp) {
			return nil, false
		}
		if prop.IsPrefix(rProp) && p.JoinType == LeftOuterJoin {
			return nil, false
		}
		if prop.IsPrefix(lProp) && p.JoinType == RightOuterJoin {
			return nil, false
		}
	}

	return []*property.PhysicalProperty{lProp, rProp}, true
}

func (p *LogicalJoin) getMergeJoin(prop *property.PhysicalProperty) []PhysicalPlan {
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
		}.init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt))
		mergeJoin.SetSchema(p.schema)
		mergeJoin.OtherConditions = p.moveEqualToOtherConditions(offsets)
		mergeJoin.initCompareFuncs()
		if reqProps, ok := mergeJoin.tryToGetChildReqProp(prop); ok {
			// Adjust expected count for children nodes.
			if prop.ExpectedCnt < p.stats.RowCount {
				expCntScale := prop.ExpectedCnt / p.stats.RowCount
				reqProps[0].ExpectedCnt = p.children[0].statsInfo().RowCount * expCntScale
				reqProps[1].ExpectedCnt = p.children[1].statsInfo().RowCount * expCntScale
			}
			mergeJoin.childrenReqProps = reqProps
			mergeJoin.Desc = prop.Desc
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

func (p *LogicalJoin) getEnforcedMergeJoin(prop *property.PhysicalProperty) []PhysicalPlan {
	// Check whether SMJ can satisfy the required property
	offsets := make([]int, 0, len(p.LeftJoinKeys))
	for _, col := range prop.Cols {
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
	lProp := &property.PhysicalProperty{TaskTp: property.RootTaskType, Cols: leftKeys, ExpectedCnt: math.MaxFloat64, Enforced: true, Desc: prop.Desc}
	rProp := &property.PhysicalProperty{TaskTp: property.RootTaskType, Cols: rightKeys, ExpectedCnt: math.MaxFloat64, Enforced: true, Desc: prop.Desc}
	enforcedPhysicalMergeJoin := PhysicalMergeJoin{
		JoinType:        p.JoinType,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		DefaultValues:   p.DefaultValues,
		LeftKeys:        leftKeys,
		RightKeys:       rightKeys,
		OtherConditions: p.OtherConditions,
		Desc:            prop.Desc,
	}.init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt))
	enforcedPhysicalMergeJoin.SetSchema(p.schema)
	enforcedPhysicalMergeJoin.childrenReqProps = []*property.PhysicalProperty{lProp, rProp}
	enforcedPhysicalMergeJoin.initCompareFuncs()
	return []PhysicalPlan{enforcedPhysicalMergeJoin}
}

func (p *PhysicalMergeJoin) initCompareFuncs() {
	p.CompareFuncs = make([]expression.CompareFunc, 0, len(p.LeftKeys))
	for i := range p.LeftKeys {
		switch expression.GetAccurateCmpType(p.LeftKeys[i], p.RightKeys[i]) {
		case types.ETInt:
			p.CompareFuncs = append(p.CompareFuncs, expression.CompareInt)
		case types.ETReal:
			p.CompareFuncs = append(p.CompareFuncs, expression.CompareReal)
		case types.ETDecimal:
			p.CompareFuncs = append(p.CompareFuncs, expression.CompareDecimal)
		case types.ETString:
			p.CompareFuncs = append(p.CompareFuncs, expression.CompareString)
		case types.ETDuration:
			p.CompareFuncs = append(p.CompareFuncs, expression.CompareDuration)
		case types.ETDatetime, types.ETTimestamp:
			p.CompareFuncs = append(p.CompareFuncs, expression.CompareTime)
		case types.ETJson:
			p.CompareFuncs = append(p.CompareFuncs, expression.CompareJSON)
		}
	}
}

func (p *LogicalJoin) getHashJoins(prop *property.PhysicalProperty) []PhysicalPlan {
	if !prop.IsEmpty() { // hash join doesn't promise any orders
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

func (p *LogicalJoin) getHashJoin(prop *property.PhysicalProperty, innerIdx int) *PhysicalHashJoin {
	chReqProps := make([]*property.PhysicalProperty, 2)
	chReqProps[innerIdx] = &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	chReqProps[1-innerIdx] = &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	if prop.ExpectedCnt < p.stats.RowCount {
		expCntScale := prop.ExpectedCnt / p.stats.RowCount
		chReqProps[1-innerIdx].ExpectedCnt = p.children[1-innerIdx].statsInfo().RowCount * expCntScale
	}
	hashJoin := PhysicalHashJoin{
		EqualConditions: p.EqualConditions,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		JoinType:        p.JoinType,
		Concurrency:     uint(p.ctx.GetSessionVars().HashJoinConcurrency),
		DefaultValues:   p.DefaultValues,
		InnerChildIdx:   innerIdx,
	}.init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), chReqProps...)
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
func (p *LogicalJoin) constructIndexJoin(prop *property.PhysicalProperty, innerJoinKeys, outerJoinKeys []*expression.Column, outerIdx int,
	innerPlan PhysicalPlan, ranges []*ranger.Range, keyOff2IdxOff []int) []PhysicalPlan {
	joinType := p.JoinType
	outerSchema := p.children[outerIdx].Schema()
	// If the order by columns are not all from outer child, index join cannot promise the order.
	if !prop.AllColsFromSchema(outerSchema) {
		return nil
	}
	chReqProps := make([]*property.PhysicalProperty, 2)
	chReqProps[outerIdx] = &property.PhysicalProperty{TaskTp: property.RootTaskType, ExpectedCnt: math.MaxFloat64, Cols: prop.Cols, Desc: prop.Desc}
	if prop.ExpectedCnt < p.stats.RowCount {
		expCntScale := prop.ExpectedCnt / p.stats.RowCount
		chReqProps[outerIdx].ExpectedCnt = p.children[outerIdx].statsInfo().RowCount * expCntScale
	}
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
	}.init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), chReqProps...)
	join.SetSchema(p.schema)
	return []PhysicalPlan{join}
}

// getIndexJoinByOuterIdx will generate index join by outerIndex. OuterIdx points out the outer child.
// First of all, we'll check whether the inner child is DataSource.
// Then, we will extract the join keys of p's equal conditions. Then check whether all of them are just the primary key
// or match some part of on index. If so we will choose the best one and construct a index join.
func (p *LogicalJoin) getIndexJoinByOuterIdx(prop *property.PhysicalProperty, outerIdx int) []PhysicalPlan {
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
	ds, isDataSource := innerChild.(*DataSource)
	us, isUnionScan := innerChild.(*LogicalUnionScan)
	if !isDataSource && !isUnionScan {
		return nil
	}
	if isUnionScan {
		ds = us.Children()[0].(*DataSource)
	}
	var tblPath *accessPath
	for _, path := range ds.possibleAccessPaths {
		if path.isTablePath {
			tblPath = path
			break
		}
	}
	if pkCol := ds.getPKIsHandleCol(); pkCol != nil && tblPath != nil {
		keyOff2IdxOff := make([]int, len(innerJoinKeys))
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
			innerPlan := p.constructInnerTableScan(ds, pkCol, outerJoinKeys, us)
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
	for _, path := range ds.possibleAccessPaths {
		if path.isTablePath {
			continue
		}
		indexInfo := path.index
		ranges, remained, tmpKeyOff2IdxOff := p.buildRangeForIndexJoin(indexInfo, ds, innerJoinKeys)
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
		innerPlan := p.constructInnerIndexScan(ds, bestIndexInfo, remainedOfBest, outerJoinKeys, us)
		return p.constructIndexJoin(prop, innerJoinKeys, outerJoinKeys, outerIdx, innerPlan, rangesOfBest, keyOff2IdxOff)
	}
	return nil
}

// constructInnerTableScan is specially used to construct the inner plan for PhysicalIndexJoin.
func (p *LogicalJoin) constructInnerTableScan(ds *DataSource, pk *expression.Column, outerJoinKeys []*expression.Column, us *LogicalUnionScan) PhysicalPlan {
	ranges := ranger.FullIntRange(mysql.HasUnsignedFlag(pk.RetType.Flag))
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

	ts.stats = property.NewSimpleStats(1)
	ts.stats.StatsVersion = ds.statisticTable.Version
	if ds.statisticTable.Pseudo {
		ts.stats.StatsVersion = statistics.PseudoVersion
	}

	copTask := &copTask{
		tablePlan:         ts,
		indexPlanFinished: true,
	}
	selStats := ts.stats.Scale(selectionFactor)
	ts.addPushedDownSelection(copTask, selStats)
	t := finishCopTask(ds.ctx, copTask)
	reader := t.plan()
	return p.constructInnerUnionScan(us, reader)
}

func (p *LogicalJoin) constructInnerUnionScan(us *LogicalUnionScan, reader PhysicalPlan) PhysicalPlan {
	if us == nil {
		return reader
	}
	// Use `reader.stats` instead of `us.stats` because it should be more accurate. No need to specify
	// childrenReqProps now since we have got reader already.
	physicalUnionScan := PhysicalUnionScan{Conditions: us.conditions}.init(us.ctx, reader.statsInfo(), nil)
	physicalUnionScan.SetChildren(reader)
	return physicalUnionScan
}

// constructInnerIndexScan is specially used to construct the inner plan for PhysicalIndexJoin.
func (p *LogicalJoin) constructInnerIndexScan(ds *DataSource, idx *model.IndexInfo, remainedConds []expression.Expression, outerJoinKeys []*expression.Column, us *LogicalUnionScan) PhysicalPlan {
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
		isPartition:      ds.isPartition,
		physicalTableID:  ds.physicalTableID,
	}.init(ds.ctx)
	is.filterCondition = remainedConds

	var rowCount float64
	idxHist, ok := ds.statisticTable.Indices[idx.ID]
	if ok && !ds.statisticTable.Pseudo {
		rowCount = idxHist.AvgCountPerValue(ds.statisticTable.Count)
	} else {
		rowCount = ds.statisticTable.PseudoAvgCountPerValue()
	}
	is.stats = property.NewSimpleStats(rowCount)
	is.stats.StatsVersion = ds.statisticTable.Version
	if ds.statisticTable.Pseudo {
		is.stats.StatsVersion = statistics.PseudoVersion
	}

	cop := &copTask{
		indexPlan: is,
	}
	if !isCoveringIndex(ds.schema.Columns, is.Index.Columns, is.Table.PKIsHandle) {
		// On this way, it's double read case.
		ts := PhysicalTableScan{
			Columns:         ds.Columns,
			Table:           is.Table,
			isPartition:     ds.isPartition,
			physicalTableID: ds.physicalTableID,
		}.init(ds.ctx)
		ts.SetSchema(is.dataSourceSchema)
		cop.tablePlan = ts
	}

	is.initSchema(idx, cop.tablePlan != nil)
	indexConds, tblConds := splitIndexFilterConditions(remainedConds, idx.Columns, ds.tableInfo)
	path := &accessPath{
		indexFilters:     indexConds,
		tableFilters:     tblConds,
		countAfterAccess: rowCount,
	}
	// Assume equal conditions used by index join and other conditions are independent.
	if len(indexConds) > 0 {
		selectivity, err := ds.tableStats.HistColl.Selectivity(ds.ctx, indexConds)
		if err != nil {
			logutil.Logger(context.Background()).Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = selectionFactor
		}
		path.countAfterIndex = rowCount * selectivity
	}
	selectivity := ds.stats.RowCount / ds.tableStats.RowCount
	finalStats := ds.stats.ScaleByExpectCnt(selectivity * rowCount)
	is.addPushedDownSelection(cop, ds, path, finalStats)
	t := finishCopTask(ds.ctx, cop)
	reader := t.plan()
	return p.constructInnerUnionScan(us, reader)
}

// buildRangeForIndexJoin checks whether this index can be used for building index join and return the range if this index is ok.
// If this index is invalid, just return nil range.
func (p *LogicalJoin) buildRangeForIndexJoin(indexInfo *model.IndexInfo, innerPlan *DataSource, innerJoinKeys []*expression.Column) (
	[]*ranger.Range, []expression.Expression, []int) {
	idxCols, colLengths := expression.IndexInfo2Cols(innerPlan.Schema().Columns, indexInfo)
	if len(idxCols) == 0 {
		return nil, nil, nil
	}

	// Extract the filter to calculate access and the filters that must be remained ones.
	access, eqConds, remained, keyOff2IdxOff := p.buildFakeEqCondsForIndexJoin(innerJoinKeys, idxCols, colLengths, innerPlan.pushedDownConds)

	if len(keyOff2IdxOff) == 0 {
		return nil, nil, nil
	}

	// In `buildFakeEqCondsForIndexJoin`, we construct the equal conditions for join keys and remove filters that contain the join keys' column.
	// When t1.a = t2.a and t1.a > 1, we can also guarantee that t1.a > 1 won't be chosen as the access condition.
	// So the equal conditions we built can be successfully used to build a range if they can be used. They won't be affected by the existing filters.
	res, err := ranger.DetachCondAndBuildRangeForIndex(p.ctx, access, idxCols, colLengths)
	if err != nil {
		logutil.Logger(context.Background()).Debug("build range for index join", zap.Error(err))
		return nil, nil, nil
	}

	// We should guarantee that all the join's equal condition is used.
	for _, eqCond := range eqConds {
		if !expression.Contains(res.AccessConds, eqCond) {
			return nil, nil, nil
		}
	}

	return res.Ranges, append(remained, res.RemainedConds...), keyOff2IdxOff
}

func (p *LogicalJoin) buildFakeEqCondsForIndexJoin(keys, idxCols []*expression.Column, colLengths []int,
	innerFilters []expression.Expression) (accesses, eqConds, remained []expression.Expression, keyOff2IdxOff []int) {
	// Check whether all join keys match one column from index.
	keyOff2IdxOff = joinKeysMatchIndex(keys, idxCols, colLengths)
	if keyOff2IdxOff == nil {
		return nil, nil, nil, nil
	}

	usableKeys := make([]*expression.Column, 0, len(keys))

	conds := make([]expression.Expression, 0, len(keys)+len(innerFilters))
	eqConds = make([]expression.Expression, 0, len(keys))
	// Construct a fake equal expression for every join key for calculating the range.
	for i, key := range keys {
		if keyOff2IdxOff[i] < 0 {
			continue
		}
		usableKeys = append(usableKeys, key)
		// Int datum 1 can convert to all column's type(numeric type, string type, json, time type, enum, set) safely.
		fakeConstant := &expression.Constant{Value: types.NewIntDatum(1), RetType: key.GetType()}
		eqFunc := expression.NewFunctionInternal(p.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), key, fakeConstant)
		conds = append(conds, eqFunc)
		eqConds = append(eqConds, eqFunc)
	}

	// Look into every `innerFilter`, if it contains join keys' column, put this filter into `remained` part directly.
	remained = make([]expression.Expression, 0, len(innerFilters))
	for _, filter := range innerFilters {
		affectedCols := expression.ExtractColumns(filter)
		if expression.ColumnSliceIsIntersect(affectedCols, usableKeys) {
			remained = append(remained, filter)
			continue
		}
		conds = append(conds, filter)
	}

	return conds, eqConds, remained, keyOff2IdxOff
}

// tryToGetIndexJoin will get index join by hints. If we can generate a valid index join by hint, the second return value
// will be true, which means we force to choose this index join. Otherwise we will select a join algorithm with min-cost.
func (p *LogicalJoin) tryToGetIndexJoin(prop *property.PhysicalProperty) (indexJoins []PhysicalPlan, forced bool) {
	rightOuter := (p.preferJoinType & preferLeftAsIndexInner) > 0
	leftOuter := (p.preferJoinType & preferRightAsIndexInner) > 0
	hasIndexJoinHint := leftOuter || rightOuter

	defer func() {
		if !forced && hasIndexJoinHint {
			// Construct warning message prefix.
			errMsg := "Optimizer Hint TIDB_INLJ is inapplicable"
			if p.hintInfo != nil {
				errMsg = fmt.Sprintf("Optimizer Hint %s is inapplicable", restore2JoinHint(TiDBIndexNestedLoopJoin, p.hintInfo.indexNestedLoopJoinTables))
			}

			// Append inapplicable reason.
			if len(p.EqualConditions) == 0 {
				errMsg += " without column equal ON condition"
			}

			// Generate warning message to client.
			warning := ErrInternal.GenWithStack(errMsg)
			p.ctx.GetSessionVars().StmtCtx.AppendWarning(warning)
		}
	}()

	if len(p.EqualConditions) == 0 {
		return nil, false
	}

	switch p.JoinType {
	case SemiJoin, AntiSemiJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin, LeftOuterJoin:
		join := p.getIndexJoinByOuterIdx(prop, 0)
		return join, join != nil && leftOuter
	case RightOuterJoin:
		join := p.getIndexJoinByOuterIdx(prop, 1)
		return join, join != nil && rightOuter
	case InnerJoin:
		lhsCardinality := p.Children()[0].statsInfo().Count()
		rhsCardinality := p.Children()[1].statsInfo().Count()

		leftJoins := p.getIndexJoinByOuterIdx(prop, 0)
		if leftJoins != nil && leftOuter && !rightOuter {
			return leftJoins, true
		}

		rightJoins := p.getIndexJoinByOuterIdx(prop, 1)
		if rightJoins != nil && rightOuter && !leftOuter {
			return rightJoins, true
		}

		if leftJoins != nil && lhsCardinality < rhsCardinality {
			return leftJoins, leftOuter
		}

		if rightJoins != nil && rhsCardinality < lhsCardinality {
			return rightJoins, rightOuter
		}

		canForceLeft := leftJoins != nil && leftOuter
		canForceRight := rightJoins != nil && rightOuter
		forced = canForceLeft || canForceRight

		joins := append(leftJoins, rightJoins...)
		return joins, forced
	}

	return nil, false
}

// LogicalJoin can generates hash join, index join and sort merge join.
// Firstly we check the hint, if hint is figured by user, we force to choose the corresponding physical plan.
// If the hint is not matched, it will get other candidates.
// If the hint is not figured, we will pick all candidates.
func (p *LogicalJoin) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
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
func (p *LogicalProjection) tryToGetChildProp(prop *property.PhysicalProperty) (*property.PhysicalProperty, bool) {
	newProp := &property.PhysicalProperty{TaskTp: property.RootTaskType, ExpectedCnt: prop.ExpectedCnt}
	newCols := make([]*expression.Column, 0, len(prop.Cols))
	for _, col := range prop.Cols {
		idx := p.schema.ColumnIndex(col)
		switch expr := p.Exprs[idx].(type) {
		case *expression.Column:
			newCols = append(newCols, expr)
		case *expression.ScalarFunction:
			return nil, false
		}
	}
	newProp.Cols = newCols
	newProp.Desc = prop.Desc
	return newProp, true
}

func (p *LogicalProjection) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	newProp, ok := p.tryToGetChildProp(prop)
	if !ok {
		return nil
	}
	proj := PhysicalProjection{
		Exprs:                p.Exprs,
		CalculateNoDelay:     p.calculateNoDelay,
		AvoidColumnEvaluator: p.avoidColumnEvaluator,
	}.init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), newProp)
	proj.SetSchema(p.schema)
	return []PhysicalPlan{proj}
}

func (lt *LogicalTopN) getPhysTopN() []PhysicalPlan {
	ret := make([]PhysicalPlan, 0, 3)
	for _, tp := range wholeTaskTypes {
		resultProp := &property.PhysicalProperty{TaskTp: tp, ExpectedCnt: math.MaxFloat64}
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
		resultProp := &property.PhysicalProperty{TaskTp: tp, ExpectedCnt: float64(lt.Count + lt.Offset), Cols: prop.Cols, Desc: prop.Desc}
		limit := PhysicalLimit{
			Count:  lt.Count,
			Offset: lt.Offset,
		}.init(lt.ctx, lt.stats, resultProp)
		ret = append(ret, limit)
	}
	return ret
}

// Check if this prop's columns can match by items totally.
func matchItems(p *property.PhysicalProperty, items []*ByItems) bool {
	if len(items) < len(p.Cols) {
		return false
	}
	for i, col := range p.Cols {
		sortItem := items[i]
		if sortItem.Desc != p.Desc || !sortItem.Expr.Equal(nil, col) {
			return false
		}
	}
	return true
}

func (lt *LogicalTopN) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	if matchItems(prop, lt.ByItems) {
		return append(lt.getPhysTopN(), lt.getPhysLimits()...)
	}
	return nil
}

func (la *LogicalApply) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	if !prop.AllColsFromSchema(la.children[0].Schema()) { // for convenient, we don't pass through any prop
		return nil
	}
	apply := PhysicalApply{
		PhysicalJoin:  la.getHashJoin(prop, 1),
		OuterSchema:   la.corCols,
		rightChOffset: la.children[0].Schema().Len(),
	}.init(la.ctx,
		la.stats.ScaleByExpectCnt(prop.ExpectedCnt),
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, Cols: prop.Cols, Desc: prop.Desc},
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64})
	apply.SetSchema(la.schema)
	return []PhysicalPlan{apply}
}

// exhaustPhysicalPlans is only for implementing interface. DataSource and Dual generate task in `findBestTask` directly.
func (p *baseLogicalPlan) exhaustPhysicalPlans(_ *property.PhysicalProperty) []PhysicalPlan {
	panic("baseLogicalPlan.exhaustPhysicalPlans() should never be called.")
}

func (la *LogicalAggregation) getStreamAggs(prop *property.PhysicalProperty) []PhysicalPlan {
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
	childProp := &property.PhysicalProperty{
		Desc:        prop.Desc,
		ExpectedCnt: math.Max(prop.ExpectedCnt*la.inputCount/la.stats.RowCount, prop.ExpectedCnt),
	}

	for _, possibleChildProperty := range la.possibleProperties {
		sortColOffsets := getMaxSortPrefix(possibleChildProperty, la.groupByCols)
		if len(sortColOffsets) != len(la.groupByCols) {
			continue
		}

		childProp.Cols = possibleChildProperty[:len(sortColOffsets)]
		if !prop.IsPrefix(childProp) {
			continue
		}

		// The table read of "CopDoubleReadTaskType" can't promises the sort
		// property that the stream aggregation required, no need to consider.
		for _, taskTp := range []property.TaskType{property.CopSingleReadTaskType, property.RootTaskType} {
			copiedChildProperty := new(property.PhysicalProperty)
			*copiedChildProperty = *childProp // It's ok to not deep copy the "cols" field.
			copiedChildProperty.TaskTp = taskTp

			agg := basePhysicalAgg{
				GroupByItems: la.GroupByItems,
				AggFuncs:     la.AggFuncs,
			}.initForStream(la.ctx, la.stats.ScaleByExpectCnt(prop.ExpectedCnt), copiedChildProperty)
			agg.SetSchema(la.schema.Clone())
			streamAggs = append(streamAggs, agg)
		}
	}
	return streamAggs
}

func (la *LogicalAggregation) getHashAggs(prop *property.PhysicalProperty) []PhysicalPlan {
	if !prop.IsEmpty() {
		return nil
	}
	hashAggs := make([]PhysicalPlan, 0, len(wholeTaskTypes))
	for _, taskTp := range wholeTaskTypes {
		agg := basePhysicalAgg{
			GroupByItems: la.GroupByItems,
			AggFuncs:     la.AggFuncs,
		}.initForHash(la.ctx, la.stats.ScaleByExpectCnt(prop.ExpectedCnt), &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, TaskTp: taskTp})
		agg.SetSchema(la.schema.Clone())
		hashAggs = append(hashAggs, agg)
	}
	return hashAggs
}

func (la *LogicalAggregation) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	aggs := make([]PhysicalPlan, 0, len(la.possibleProperties)+1)
	aggs = append(aggs, la.getHashAggs(prop)...)
	aggs = append(aggs, la.getStreamAggs(prop)...)
	return aggs
}

func (p *LogicalSelection) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	childProp := prop.Clone()
	sel := PhysicalSelection{
		Conditions: p.Conditions,
	}.init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), childProp)
	return []PhysicalPlan{sel}
}

func (p *LogicalLimit) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	if !prop.IsEmpty() {
		return nil
	}
	ret := make([]PhysicalPlan, 0, len(wholeTaskTypes))
	for _, tp := range wholeTaskTypes {
		resultProp := &property.PhysicalProperty{TaskTp: tp, ExpectedCnt: float64(p.Count + p.Offset)}
		limit := PhysicalLimit{
			Offset: p.Offset,
			Count:  p.Count,
		}.init(p.ctx, p.stats, resultProp)
		ret = append(ret, limit)
	}
	return ret
}

func (p *LogicalLock) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	childProp := prop.Clone()
	lock := PhysicalLock{
		Lock: p.Lock,
	}.init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), childProp)
	return []PhysicalPlan{lock}
}

func (p *LogicalUnionAll) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	// TODO: UnionAll can not pass any order, but we can change it to sort merge to keep order.
	if !prop.IsEmpty() {
		return nil
	}
	chReqProps := make([]*property.PhysicalProperty, 0, len(p.children))
	for range p.children {
		chReqProps = append(chReqProps, &property.PhysicalProperty{ExpectedCnt: prop.ExpectedCnt})
	}
	ua := PhysicalUnionAll{}.init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), chReqProps...)
	ua.SetSchema(p.Schema())
	return []PhysicalPlan{ua}
}

func (ls *LogicalSort) getPhysicalSort(prop *property.PhysicalProperty) *PhysicalSort {
	ps := PhysicalSort{ByItems: ls.ByItems}.init(ls.ctx, ls.stats.ScaleByExpectCnt(prop.ExpectedCnt), &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64})
	return ps
}

func (ls *LogicalSort) getNominalSort(reqProp *property.PhysicalProperty) *NominalSort {
	prop, canPass := getPropByOrderByItems(ls.ByItems)
	if !canPass {
		return nil
	}
	prop.ExpectedCnt = reqProp.ExpectedCnt
	ps := NominalSort{}.init(ls.ctx, prop)
	return ps
}

func (ls *LogicalSort) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	if matchItems(prop, ls.ByItems) {
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

func (p *LogicalMaxOneRow) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	if !prop.IsEmpty() {
		return nil
	}
	mor := PhysicalMaxOneRow{}.init(p.ctx, p.stats, &property.PhysicalProperty{ExpectedCnt: 2})
	return []PhysicalPlan{mor}
}
