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
	"bytes"
	"context"
	"fmt"
	"math"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/set"
	"go.uber.org/zap"
)

func (p *LogicalUnionScan) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	childProp := prop.Clone()
	us := PhysicalUnionScan{Conditions: p.conditions}.Init(p.ctx, p.stats, childProp)
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
	all, desc := prop.AllSameOrder()
	lProp := property.NewPhysicalProperty(property.RootTaskType, p.LeftKeys, desc, math.MaxFloat64, false)
	rProp := property.NewPhysicalProperty(property.RootTaskType, p.RightKeys, desc, math.MaxFloat64, false)
	if !prop.IsEmpty() {
		// sort merge join fits the cases of massive ordered data, so desc scan is always expensive.
		if !all {
			return nil, false
		}
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
		}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt))
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
			_, desc := prop.AllSameOrder()
			mergeJoin.Desc = desc
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

func (p *LogicalJoin) getEnforcedMergeJoin(prop *property.PhysicalProperty) []PhysicalPlan {
	// Check whether SMJ can satisfy the required property
	offsets := make([]int, 0, len(p.LeftJoinKeys))
	all, desc := prop.AllSameOrder()
	if !all {
		return nil
	}
	for _, item := range prop.Items {
		isExist := false
		for joinKeyPos := 0; joinKeyPos < len(p.LeftJoinKeys); joinKeyPos++ {
			var key *expression.Column
			if item.Col.Equal(p.ctx, p.LeftJoinKeys[joinKeyPos]) {
				key = p.LeftJoinKeys[joinKeyPos]
			}
			if item.Col.Equal(p.ctx, p.RightJoinKeys[joinKeyPos]) {
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
	lProp := property.NewPhysicalProperty(property.RootTaskType, leftKeys, desc, math.MaxFloat64, true)
	rProp := property.NewPhysicalProperty(property.RootTaskType, rightKeys, desc, math.MaxFloat64, true)
	enforcedPhysicalMergeJoin := PhysicalMergeJoin{
		JoinType:        p.JoinType,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		DefaultValues:   p.DefaultValues,
		LeftKeys:        leftKeys,
		RightKeys:       rightKeys,
		OtherConditions: p.OtherConditions,
		Desc:            desc,
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt))
	enforcedPhysicalMergeJoin.SetSchema(p.schema)
	enforcedPhysicalMergeJoin.childrenReqProps = []*property.PhysicalProperty{lProp, rProp}
	enforcedPhysicalMergeJoin.initCompareFuncs()
	return []PhysicalPlan{enforcedPhysicalMergeJoin}
}

func (p *PhysicalMergeJoin) initCompareFuncs() {
	p.CompareFuncs = make([]expression.CompareFunc, 0, len(p.LeftKeys))
	for i := range p.LeftKeys {
		p.CompareFuncs = append(p.CompareFuncs, expression.GetCmpFunction(p.LeftKeys[i], p.RightKeys[i]))
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
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), chReqProps...)
	hashJoin.SetSchema(p.schema)
	return hashJoin
}

// When inner plan is TableReader, the parameter `ranges` will be nil. Because pk only have one column. So all of its range
// is generated during execution time.
func (p *LogicalJoin) constructIndexJoin(
	prop *property.PhysicalProperty,
	outerIdx int,
	innerPlan PhysicalPlan,
	ranges []*ranger.Range,
	keyOff2IdxOff []int,
	lens []int,
	compareFilters *ColWithCmpFuncManager,
) []PhysicalPlan {
	joinType := p.JoinType
	outerSchema := p.children[outerIdx].Schema()
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
	all, _ := prop.AllSameOrder()
	// If the order by columns are not all from outer child, index join cannot promise the order.
	if !prop.AllColsFromSchema(outerSchema) || !all {
		return nil
	}
	chReqProps := make([]*property.PhysicalProperty, 2)
	chReqProps[outerIdx] = &property.PhysicalProperty{TaskTp: property.RootTaskType, ExpectedCnt: math.MaxFloat64, Items: prop.Items}
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
		IdxColLens:      lens,
		Ranges:          ranges,
		CompareFilters:  compareFilters,
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), chReqProps...)
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
		// The child of union scan may be union all for partition table.
		ds, isDataSource = us.Children()[0].(*DataSource)
		if !isDataSource {
			return nil
		}
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
			return p.constructIndexJoin(prop, outerIdx, innerPlan, nil, keyOff2IdxOff, nil, nil)
		}
	}
	helper := &indexJoinBuildHelper{join: p}
	for _, path := range ds.possibleAccessPaths {
		if path.isTablePath {
			continue
		}
		indexInfo := path.index
		emptyRange, err := helper.analyzeLookUpFilters(indexInfo, ds, innerJoinKeys)
		if emptyRange {
			return nil
		}
		if err != nil {
			logutil.Logger(context.Background()).Warn("build index join failed", zap.Error(err))
		}
	}
	if helper.chosenIndexInfo != nil {
		keyOff2IdxOff := make([]int, len(innerJoinKeys))
		for i := range keyOff2IdxOff {
			keyOff2IdxOff[i] = -1
		}
		for idxOff, keyOff := range helper.idxOff2KeyOff {
			if keyOff != -1 {
				keyOff2IdxOff[keyOff] = idxOff
			}
		}
		idxCols, lens := expression.IndexInfo2Cols(ds.schema.Columns, helper.chosenIndexInfo)
		rangeInfo := helper.buildRangeDecidedByInformation(idxCols, outerJoinKeys)
		innerPlan := p.constructInnerIndexScan(ds, helper.chosenIndexInfo, helper.chosenRemained, outerJoinKeys, us, rangeInfo)
		return p.constructIndexJoin(prop, outerIdx, innerPlan, helper.chosenRanges, keyOff2IdxOff, lens, helper.lastColManager)
	}
	return nil
}

type indexJoinBuildHelper struct {
	join *LogicalJoin

	chosenIndexInfo *model.IndexInfo
	maxUsedCols     int
	chosenAccess    []expression.Expression
	chosenRemained  []expression.Expression
	idxOff2KeyOff   []int
	lastColManager  *ColWithCmpFuncManager
	chosenRanges    []*ranger.Range

	curPossibleUsedKeys []*expression.Column
	curNotUsedIndexCols []*expression.Column
	curNotUsedColLens   []int
	curIdxOff2KeyOff    []int
}

func (ijHelper *indexJoinBuildHelper) buildRangeDecidedByInformation(idxCols []*expression.Column, outerJoinKeys []*expression.Column) string {
	buffer := bytes.NewBufferString("[")
	isFirst := true
	for idxOff, keyOff := range ijHelper.idxOff2KeyOff {
		if keyOff == -1 {
			continue
		}
		if !isFirst {
			buffer.WriteString(" ")
		} else {
			isFirst = false
		}
		buffer.WriteString(fmt.Sprintf("eq(%v, %v)", idxCols[idxOff], outerJoinKeys[keyOff]))
	}
	for _, access := range ijHelper.chosenAccess {
		if !isFirst {
			buffer.WriteString(" ")
		} else {
			isFirst = false
		}
		buffer.WriteString(fmt.Sprintf("%v", access))
	}
	buffer.WriteString("]")
	return buffer.String()
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
	}.Init(ds.ctx)
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
	physicalUnionScan := PhysicalUnionScan{Conditions: us.conditions}.Init(us.ctx, reader.statsInfo(), nil)
	physicalUnionScan.SetChildren(reader)
	return physicalUnionScan
}

// constructInnerIndexScan is specially used to construct the inner plan for PhysicalIndexJoin.
func (p *LogicalJoin) constructInnerIndexScan(ds *DataSource, idx *model.IndexInfo, filterConds []expression.Expression,
	outerJoinKeys []*expression.Column, us *LogicalUnionScan, rangeInfo string) PhysicalPlan {
	is := PhysicalIndexScan{
		Table:            ds.tableInfo,
		TableAsName:      ds.TableAsName,
		DBName:           ds.DBName,
		Columns:          ds.Columns,
		Index:            idx,
		dataSourceSchema: ds.schema,
		KeepOrder:        false,
		Ranges:           ranger.FullRange(),
		rangeInfo:        rangeInfo,
		isPartition:      ds.isPartition,
		physicalTableID:  ds.physicalTableID,
	}.Init(ds.ctx)

	var rowCount float64
	idxHist, ok := ds.statisticTable.Indices[idx.ID]
	if ok && !ds.statisticTable.Pseudo {
		rowCount = idxHist.AvgCountPerNotNullValue(ds.statisticTable.Count)
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
		}.Init(ds.ctx)
		ts.SetSchema(is.dataSourceSchema)
		cop.tablePlan = ts
	}

	is.initSchema(idx, cop.tablePlan != nil)
	indexConds, tblConds := splitIndexFilterConditions(filterConds, idx.Columns, ds.tableInfo)
	path := &accessPath{
		indexFilters:     indexConds,
		tableFilters:     tblConds,
		countAfterAccess: rowCount,
	}
	// Assume equal conditions used by index join and other conditions are independent.
	if len(indexConds) > 0 {
		selectivity, _, err := ds.tableStats.HistColl.Selectivity(ds.ctx, indexConds)
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

var symmetricOp = map[string]string{
	ast.LT: ast.GT,
	ast.GE: ast.LE,
	ast.GT: ast.LT,
	ast.LE: ast.GE,
}

// ColWithCmpFuncManager is used in index join to handle the column with compare functions(>=, >, <, <=).
// It stores the compare functions and build ranges in execution phase.
type ColWithCmpFuncManager struct {
	TargetCol         *expression.Column
	colLength         int
	OpType            []string
	opArg             []expression.Expression
	TmpConstant       []*expression.Constant
	affectedColSchema *expression.Schema
	compareFuncs      []chunk.CompareFunc
}

func (cwc *ColWithCmpFuncManager) appendNewExpr(opName string, arg expression.Expression, affectedCols []*expression.Column) {
	cwc.OpType = append(cwc.OpType, opName)
	cwc.opArg = append(cwc.opArg, arg)
	cwc.TmpConstant = append(cwc.TmpConstant, &expression.Constant{RetType: cwc.TargetCol.RetType})
	for _, col := range affectedCols {
		if cwc.affectedColSchema.Contains(col) {
			continue
		}
		cwc.compareFuncs = append(cwc.compareFuncs, chunk.GetCompareFunc(col.RetType))
		cwc.affectedColSchema.Append(col)
	}
}

// CompareRow compares the rows for deduplicate.
func (cwc *ColWithCmpFuncManager) CompareRow(lhs, rhs chunk.Row) int {
	for i, col := range cwc.affectedColSchema.Columns {
		ret := cwc.compareFuncs[i](lhs, col.Index, rhs, col.Index)
		if ret != 0 {
			return ret
		}
	}
	return 0
}

// BuildRangesByRow will build range of the given row. It will eval each function's arg then call BuildRange.
func (cwc *ColWithCmpFuncManager) BuildRangesByRow(ctx sessionctx.Context, row chunk.Row) ([]*ranger.Range, error) {
	exprs := make([]expression.Expression, len(cwc.OpType))
	for i, opType := range cwc.OpType {
		constantArg, err := cwc.opArg[i].Eval(row)
		if err != nil {
			return nil, err
		}
		cwc.TmpConstant[i].Value = constantArg
		newExpr, err := expression.NewFunction(ctx, opType, types.NewFieldType(mysql.TypeTiny), cwc.TargetCol, cwc.TmpConstant[i])
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, newExpr)
	}
	ranges, err := ranger.BuildColumnRange(exprs, ctx.GetSessionVars().StmtCtx, cwc.TargetCol.RetType, cwc.colLength)
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
		buffer.WriteString(fmt.Sprintf("%v(%v, %v)", cwc.OpType[i], cwc.TargetCol, cwc.opArg[i]))
		if i < len(cwc.OpType)-1 {
			buffer.WriteString(" ")
		}
	}
	return buffer.String()
}

func (ijHelper *indexJoinBuildHelper) resetContextForIndex(innerKeys []*expression.Column, idxCols []*expression.Column, colLens []int) {
	tmpSchema := expression.NewSchema(innerKeys...)
	ijHelper.curIdxOff2KeyOff = make([]int, len(idxCols))
	ijHelper.curNotUsedIndexCols = make([]*expression.Column, 0, len(idxCols))
	ijHelper.curNotUsedColLens = make([]int, 0, len(idxCols))
	for i, idxCol := range idxCols {
		ijHelper.curIdxOff2KeyOff[i] = tmpSchema.ColumnIndex(idxCol)
		if ijHelper.curIdxOff2KeyOff[i] >= 0 {
			continue
		}
		ijHelper.curNotUsedIndexCols = append(ijHelper.curNotUsedIndexCols, idxCol)
		ijHelper.curNotUsedColLens = append(ijHelper.curNotUsedColLens, colLens[i])
	}
}

// findUsefulEqAndInFilters analyzes the pushedDownConds held by inner child and split them to three parts.
// usefulEqOrInFilters is the continuous eq/in conditions on current unused index columns.
// uselessFilters is the conditions which cannot be used for building ranges.
// remainingRangeCandidates is the other conditions for future use.
func (ijHelper *indexJoinBuildHelper) findUsefulEqAndInFilters(innerPlan *DataSource) (usefulEqOrInFilters, uselessFilters, remainingRangeCandidates []expression.Expression) {
	uselessFilters = make([]expression.Expression, 0, len(innerPlan.pushedDownConds))
	var remainedEqOrIn []expression.Expression
	// Extract the eq/in functions of possible join key.
	// you can see the comment of ExtractEqAndInCondition to get the meaning of the second return value.
	usefulEqOrInFilters, remainedEqOrIn, remainingRangeCandidates, _ = ranger.ExtractEqAndInCondition(
		innerPlan.ctx, innerPlan.pushedDownConds,
		ijHelper.curNotUsedIndexCols,
		ijHelper.curNotUsedColLens,
	)
	uselessFilters = append(uselessFilters, remainedEqOrIn...)
	return usefulEqOrInFilters, uselessFilters, remainingRangeCandidates
}

// buildLastColManager analyze the `OtherConditions` of join to see whether there're some filters can be used in manager.
// The returned value is just for outputting explain information
func (ijHelper *indexJoinBuildHelper) buildLastColManager(nextCol *expression.Column,
	innerPlan *DataSource, cwc *ColWithCmpFuncManager) []expression.Expression {
	var lastColAccesses []expression.Expression
loopOtherConds:
	for _, filter := range ijHelper.join.OtherConditions {
		sf, ok := filter.(*expression.ScalarFunction)
		if !ok || !(sf.FuncName.L == ast.LE || sf.FuncName.L == ast.LT || sf.FuncName.L == ast.GE || sf.FuncName.L == ast.GT) {
			continue
		}
		var funcName string
		var anotherArg expression.Expression
		if lCol, ok := sf.GetArgs()[0].(*expression.Column); ok && lCol.Equal(nil, nextCol) {
			anotherArg = sf.GetArgs()[1]
			funcName = sf.FuncName.L
		} else if rCol, ok := sf.GetArgs()[1].(*expression.Column); ok && rCol.Equal(nil, nextCol) {
			anotherArg = sf.GetArgs()[0]
			// The column manager always build expression in the form of col op arg1.
			// So we need use the symmetric one of the current function.
			funcName = symmetricOp[sf.FuncName.L]
		} else {
			continue
		}
		affectedCols := expression.ExtractColumns(anotherArg)
		if len(affectedCols) == 0 {
			continue
		}
		for _, col := range affectedCols {
			if innerPlan.schema.Contains(col) {
				continue loopOtherConds
			}
		}
		lastColAccesses = append(lastColAccesses, sf)
		cwc.appendNewExpr(funcName, anotherArg, affectedCols)
	}
	return lastColAccesses
}

// removeUselessEqAndInFunc removes the useless eq/in conditions. It's designed for the following case:
//   t1 join t2 on t1.a=t2.a and t1.c=t2.c where t1.b > t2.b-10 and t1.b < t2.b+10 there's index(a, b, c) on t1.
//   In this case the curIdxOff2KeyOff is [0 -1 1] and the notKeyEqAndIn is [].
//   It's clearly that the column c cannot be used to access data. So we need to remove it and reset the IdxOff2KeyOff to
//   [0 -1 -1].
//   So that we can use t1.a=t2.a and t1.b > t2.b-10 and t1.b < t2.b+10 to build ranges then access data.
func (ijHelper *indexJoinBuildHelper) removeUselessEqAndInFunc(
	idxCols []*expression.Column,
	notKeyEqAndIn []expression.Expression) (
	usefulEqAndIn, uselessOnes []expression.Expression,
) {
	ijHelper.curPossibleUsedKeys = make([]*expression.Column, 0, len(idxCols))
	for idxColPos, notKeyColPos := 0, 0; idxColPos < len(idxCols); idxColPos++ {
		if ijHelper.curIdxOff2KeyOff[idxColPos] != -1 {
			ijHelper.curPossibleUsedKeys = append(ijHelper.curPossibleUsedKeys, idxCols[idxColPos])
			continue
		}
		if notKeyColPos < len(notKeyEqAndIn) && ijHelper.curNotUsedIndexCols[notKeyColPos].Equal(nil, idxCols[idxColPos]) {
			notKeyColPos++
			continue
		}
		for i := idxColPos + 1; i < len(idxCols); i++ {
			ijHelper.curIdxOff2KeyOff[i] = -1
		}
		remained := make([]expression.Expression, 0, len(notKeyEqAndIn)-notKeyColPos)
		remained = append(remained, notKeyEqAndIn[notKeyColPos:]...)
		notKeyEqAndIn = notKeyEqAndIn[:notKeyColPos]
		return notKeyEqAndIn, remained
	}
	return notKeyEqAndIn, nil
}

func (ijHelper *indexJoinBuildHelper) analyzeLookUpFilters(indexInfo *model.IndexInfo, innerPlan *DataSource, innerJoinKeys []*expression.Column) (emptyRange bool, err error) {
	idxCols, colLengths := expression.IndexInfo2Cols(innerPlan.schema.Columns, indexInfo)
	if len(idxCols) == 0 {
		return false, nil
	}
	accesses := make([]expression.Expression, 0, len(idxCols))
	ijHelper.resetContextForIndex(innerJoinKeys, idxCols, colLengths)
	notKeyEqAndIn, remained, rangeFilterCandidates := ijHelper.findUsefulEqAndInFilters(innerPlan)
	var remainedEqAndIn []expression.Expression
	notKeyEqAndIn, remainedEqAndIn = ijHelper.removeUselessEqAndInFunc(idxCols, notKeyEqAndIn)
	matchedKeyCnt := len(ijHelper.curPossibleUsedKeys)
	// If no join key is matched while join keys actually are not empty. We don't choose index join for now.
	if matchedKeyCnt <= 0 && len(innerJoinKeys) > 0 {
		return false, nil
	}
	accesses = append(accesses, notKeyEqAndIn...)
	remained = append(remained, remainedEqAndIn...)
	lastColPos := matchedKeyCnt + len(notKeyEqAndIn)
	// There should be some equal conditions. But we don't need that there must be some join key in accesses here.
	// A more strict check is applied later.
	if lastColPos <= 0 {
		return false, nil
	}
	// If all the index columns are covered by eq/in conditions, we don't need to consider other conditions anymore.
	if lastColPos == len(idxCols) {
		// If there's join key matched index column. Then choose hash join is always a better idea.
		// e.g. select * from t1, t2 where t2.a=1 and t2.b=1. And t2 has index(a, b).
		//      If we don't have the following check, TiDB will build index join for this case.
		if matchedKeyCnt <= 0 {
			return false, nil
		}
		remained = append(remained, rangeFilterCandidates...)
		ranges, emptyRange, err := ijHelper.buildTemplateRange(matchedKeyCnt, notKeyEqAndIn, nil, false)
		if err != nil {
			return false, err
		}
		if emptyRange {
			return true, nil
		}
		ijHelper.updateBestChoice(ranges, indexInfo, accesses, remained, nil)
		return false, nil
	}
	lastPossibleCol := idxCols[lastColPos]
	lastColManager := &ColWithCmpFuncManager{
		TargetCol:         lastPossibleCol,
		colLength:         colLengths[lastColPos],
		affectedColSchema: expression.NewSchema(),
	}
	lastColAccess := ijHelper.buildLastColManager(lastPossibleCol, innerPlan, lastColManager)
	// If the column manager holds no expression, then we fallback to find whether there're useful normal filters
	if len(lastColAccess) == 0 {
		// If there's join key matched index column. Then choose hash join is always a better idea.
		// e.g. select * from t1, t2 where t2.a=1 and t2.b=1 and t2.c > 10 and t2.c < 20. And t2 has index(a, b, c).
		//      If we don't have the following check, TiDB will build index join for this case.
		if matchedKeyCnt <= 0 {
			return false, nil
		}
		colAccesses, colRemained := ranger.DetachCondsForColumn(ijHelper.join.ctx, rangeFilterCandidates, lastPossibleCol)
		var ranges, nextColRange []*ranger.Range
		var err error
		if len(colAccesses) > 0 {
			nextColRange, err = ranger.BuildColumnRange(colAccesses, ijHelper.join.ctx.GetSessionVars().StmtCtx, lastPossibleCol.RetType, colLengths[lastColPos])
			if err != nil {
				return false, err
			}
		}
		ranges, emptyRange, err = ijHelper.buildTemplateRange(matchedKeyCnt, notKeyEqAndIn, nextColRange, false)
		if err != nil {
			return false, err
		}
		if emptyRange {
			return true, nil
		}
		remained = append(remained, colRemained...)
		if colLengths[lastColPos] != types.UnspecifiedLength {
			remained = append(remained, colAccesses...)
		}
		accesses = append(accesses, colAccesses...)
		ijHelper.updateBestChoice(ranges, indexInfo, accesses, remained, nil)
		return false, nil
	}
	accesses = append(accesses, lastColAccess...)
	remained = append(remained, rangeFilterCandidates...)
	ranges, emptyRange, err := ijHelper.buildTemplateRange(matchedKeyCnt, notKeyEqAndIn, nil, true)
	if err != nil {
		return false, err
	}
	if emptyRange {
		return true, nil
	}
	ijHelper.updateBestChoice(ranges, indexInfo, accesses, remained, lastColManager)
	return false, nil
}

func (ijHelper *indexJoinBuildHelper) updateBestChoice(ranges []*ranger.Range, idxInfo *model.IndexInfo, accesses,
	remained []expression.Expression, lastColManager *ColWithCmpFuncManager) {
	// We choose the index by the number of used columns of the range, the much the better.
	// Notice that there may be the cases like `t1.a=t2.a and b > 2 and b < 1`. So ranges can be nil though the conditions are valid.
	// But obviously when the range is nil, we don't need index join.
	if len(ranges) > 0 && len(ranges[0].LowVal) > ijHelper.maxUsedCols {
		ijHelper.chosenIndexInfo = idxInfo
		ijHelper.maxUsedCols = len(ranges[0].LowVal)
		ijHelper.chosenRanges = ranges
		ijHelper.chosenAccess = accesses
		ijHelper.chosenRemained = remained
		ijHelper.idxOff2KeyOff = ijHelper.curIdxOff2KeyOff
		ijHelper.lastColManager = lastColManager
	}
}

func (ijHelper *indexJoinBuildHelper) buildTemplateRange(matchedKeyCnt int, eqAndInFuncs []expression.Expression, nextColRange []*ranger.Range, haveExtraCol bool) (ranges []*ranger.Range, emptyRange bool, err error) {
	pointLength := matchedKeyCnt + len(eqAndInFuncs)
	if nextColRange != nil {
		for _, colRan := range nextColRange {
			// The range's exclude status is the same with last col's.
			ran := &ranger.Range{
				LowVal:      make([]types.Datum, pointLength, pointLength+1),
				HighVal:     make([]types.Datum, pointLength, pointLength+1),
				LowExclude:  colRan.LowExclude,
				HighExclude: colRan.HighExclude,
			}
			ran.LowVal = append(ran.LowVal, colRan.LowVal[0])
			ran.HighVal = append(ran.HighVal, colRan.HighVal[0])
			ranges = append(ranges, ran)
		}
	} else if haveExtraCol {
		// Reserve a position for the last col.
		ranges = append(ranges, &ranger.Range{
			LowVal:  make([]types.Datum, pointLength+1, pointLength+1),
			HighVal: make([]types.Datum, pointLength+1, pointLength+1),
		})
	} else {
		ranges = append(ranges, &ranger.Range{
			LowVal:  make([]types.Datum, pointLength, pointLength),
			HighVal: make([]types.Datum, pointLength, pointLength),
		})
	}
	sc := ijHelper.join.ctx.GetSessionVars().StmtCtx
	for i, j := 0, 0; j < len(eqAndInFuncs); i++ {
		// This position is occupied by join key.
		if ijHelper.curIdxOff2KeyOff[i] != -1 {
			continue
		}
		oneColumnRan, err := ranger.BuildColumnRange([]expression.Expression{eqAndInFuncs[j]}, sc, ijHelper.curNotUsedIndexCols[j].RetType, ijHelper.curNotUsedColLens[j])
		if err != nil {
			return nil, false, err
		}
		if len(oneColumnRan) == 0 {
			return nil, true, nil
		}
		for _, ran := range ranges {
			ran.LowVal[i] = oneColumnRan[0].LowVal[0]
			ran.HighVal[i] = oneColumnRan[0].HighVal[0]
		}
		curRangeLen := len(ranges)
		for ranIdx := 1; ranIdx < len(oneColumnRan); ranIdx++ {
			newRanges := make([]*ranger.Range, 0, curRangeLen)
			for oldRangeIdx := 0; oldRangeIdx < curRangeLen; oldRangeIdx++ {
				newRange := ranges[oldRangeIdx].Clone()
				newRange.LowVal[i] = oneColumnRan[ranIdx].LowVal[0]
				newRange.HighVal[i] = oneColumnRan[ranIdx].HighVal[0]
				newRanges = append(newRanges, newRange)
			}
			ranges = append(ranges, newRanges...)
		}
		j++
	}
	return ranges, false, nil
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
	newCols := make([]property.Item, 0, len(prop.Items))
	for _, col := range prop.Items {
		idx := p.schema.ColumnIndex(col.Col)
		switch expr := p.Exprs[idx].(type) {
		case *expression.Column:
			newCols = append(newCols, property.Item{Col: expr, Desc: col.Desc})
		case *expression.ScalarFunction:
			return nil, false
		}
	}
	newProp.Items = newCols
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
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), newProp)
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
		}.Init(lt.ctx, lt.stats, resultProp)
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
		resultProp := &property.PhysicalProperty{TaskTp: tp, ExpectedCnt: float64(lt.Count + lt.Offset), Items: prop.Items}
		limit := PhysicalLimit{
			Count:  lt.Count,
			Offset: lt.Offset,
		}.Init(lt.ctx, lt.stats, resultProp)
		ret = append(ret, limit)
	}
	return ret
}

// Check if this prop's columns can match by items totally.
func matchItems(p *property.PhysicalProperty, items []*util.ByItems) bool {
	if len(items) < len(p.Items) {
		return false
	}
	for i, col := range p.Items {
		sortItem := items[i]
		if sortItem.Desc != col.Desc || !sortItem.Expr.Equal(nil, col.Col) {
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
	join := la.getHashJoin(prop, 1)
	apply := PhysicalApply{
		PhysicalHashJoin: *join,
		OuterSchema:      la.corCols,
		rightChOffset:    la.children[0].Schema().Len(),
	}.Init(la.ctx,
		la.stats.ScaleByExpectCnt(prop.ExpectedCnt),
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, Items: prop.Items},
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64})
	apply.SetSchema(la.schema)
	return []PhysicalPlan{apply}
}

func (p *LogicalWindow) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	var byItems []property.Item
	byItems = append(byItems, p.PartitionBy...)
	byItems = append(byItems, p.OrderBy...)
	childProperty := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, Items: byItems, Enforced: true}
	if !prop.IsPrefix(childProperty) {
		return nil
	}
	window := PhysicalWindow{
		WindowFuncDescs: p.WindowFuncDescs,
		PartitionBy:     p.PartitionBy,
		OrderBy:         p.OrderBy,
		Frame:           p.Frame,
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), childProperty)
	window.SetSchema(p.Schema())
	return []PhysicalPlan{window}
}

// exhaustPhysicalPlans is only for implementing interface. DataSource and Dual generate task in `findBestTask` directly.
func (p *baseLogicalPlan) exhaustPhysicalPlans(_ *property.PhysicalProperty) []PhysicalPlan {
	panic("baseLogicalPlan.exhaustPhysicalPlans() should never be called.")
}

func (la *LogicalAggregation) getStreamAggs(prop *property.PhysicalProperty) []PhysicalPlan {
	all, desc := prop.AllSameOrder()
	if len(la.possibleProperties) == 0 || !all {
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
		ExpectedCnt: math.Max(prop.ExpectedCnt*la.inputCount/la.stats.RowCount, prop.ExpectedCnt),
	}

	for _, possibleChildProperty := range la.possibleProperties {
		sortColOffsets := getMaxSortPrefix(possibleChildProperty, la.groupByCols)
		if len(sortColOffsets) != len(la.groupByCols) {
			continue
		}

		childProp.Items = property.ItemsFromCols(possibleChildProperty[:len(sortColOffsets)], desc)
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
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), childProp)
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
		}.Init(p.ctx, p.stats, resultProp)
		ret = append(ret, limit)
	}
	return ret
}

func (p *LogicalLock) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	childProp := prop.Clone()
	lock := PhysicalLock{
		Lock:             p.Lock,
		PartitionedTable: p.partitionedTable,
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), childProp)
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
	ua := PhysicalUnionAll{}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), chReqProps...)
	ua.SetSchema(p.Schema())
	return []PhysicalPlan{ua}
}

func (ls *LogicalSort) getPhysicalSort(prop *property.PhysicalProperty) *PhysicalSort {
	ps := PhysicalSort{ByItems: ls.ByItems}.Init(ls.ctx, ls.stats.ScaleByExpectCnt(prop.ExpectedCnt), &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64})
	return ps
}

func (ls *LogicalSort) getNominalSort(reqProp *property.PhysicalProperty) *NominalSort {
	prop, canPass := getPropByOrderByItems(ls.ByItems)
	if !canPass {
		return nil
	}
	prop.ExpectedCnt = reqProp.ExpectedCnt
	ps := NominalSort{}.Init(ls.ctx, prop)
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
	mor := PhysicalMaxOneRow{}.Init(p.ctx, p.stats, &property.PhysicalProperty{ExpectedCnt: 2})
	return []PhysicalPlan{mor}
}
