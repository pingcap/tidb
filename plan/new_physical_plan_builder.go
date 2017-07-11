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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/types"
)

// wholeTaskTypes records all possible kinds of task that a plan can return. For Agg, TopN and Limit, we will try to get
// these tasks one by one.
var wholeTaskTypes = [...]taskType{rootTaskType, copSingleReadTaskType, copDoubleReadTaskType}

var invalidTask = &rootTask{cst: math.MaxFloat64}

func (p *requiredProp) enforceProperty(task task, ctx context.Context, allocator *idAllocator) task {
	if p.isEmpty() {
		return task
	}
	// If task is invalid, keep it remained.
	if task.plan() == nil {
		return task
	}
	sort := Sort{ByItems: make([]*ByItems, 0, len(p.cols))}.init(allocator, ctx)
	for _, col := range p.cols {
		sort.ByItems = append(sort.ByItems, &ByItems{col, p.desc})
	}
	sort.SetSchema(task.plan().Schema())
	return sort.attach2Task(task)
}

// getChildrenPossibleProps will check if this sort property can be pushed or not.
// When a sort column will be replaced by scalar function, we refuse it.
// When a sort column will be replaced by a constant, we just remove it.
func (p *Projection) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	newProp := &requiredProp{taskTp: rootTaskType}
	newCols := make([]*expression.Column, 0, len(prop.cols))
	for _, col := range prop.cols {
		idx := p.schema.ColumnIndex(col)
		if idx == -1 {
			return nil
		}
		switch expr := p.Exprs[idx].(type) {
		case *expression.Column:
			newCols = append(newCols, expr)
		case *expression.ScalarFunction:
			return nil
		}
	}
	newProp.cols = newCols
	newProp.desc = prop.desc
	return [][]*requiredProp{{newProp}}
}

// joinKeysMatchIndex checks if all keys match columns in index.
func joinKeysMatchIndex(keys []*expression.Column, index *model.IndexInfo) []int {
	if len(index.Columns) < len(keys) {
		return nil
	}
	matchOffsets := make([]int, len(keys))
	for i, idxCol := range index.Columns {
		if idxCol.Length != types.UnspecifiedLength {
			return nil
		}
		found := false
		for j, key := range keys {
			if idxCol.Name.L == key.ColName.L {
				matchOffsets[i] = j
				found = true
				break
			}
		}
		if !found {
			return nil
		}
		if i+1 == len(keys) {
			break
		}
	}
	return matchOffsets
}

func (p *LogicalJoin) constructIndexJoin(innerJoinKeys, outerJoinKeys []*expression.Column, outerIdx int) *PhysicalIndexJoin {
	var rightConds, leftConds expression.CNFExprs
	if outerIdx == 0 {
		rightConds = p.RightConditions.Clone()
		leftConds = p.LeftConditions.Clone()
	} else {
		rightConds = p.LeftConditions.Clone()
		leftConds = p.RightConditions.Clone()
	}
	join := PhysicalIndexJoin{
		outerIndex:      outerIdx,
		LeftConditions:  leftConds,
		RightConditions: rightConds,
		OtherConditions: p.OtherConditions,
		Outer:           p.JoinType != InnerJoin,
		OuterJoinKeys:   outerJoinKeys,
		InnerJoinKeys:   innerJoinKeys,
		DefaultValues:   p.DefaultValues,
	}.init(p.allocator, p.ctx, p.children[outerIdx], p.children[1-outerIdx])
	return join
}

// getIndexJoinByOuterIdx will generate index join by outerIndex. OuterIdx points out the outer child,
// because we will swap the children of join when the right child is outer child.
// First of all, we will extract the join keys for p's equal conditions. If the join keys can match some of the indices or PK
// column of inner child, we can apply the index join.
func (p *LogicalJoin) getIndexJoinByOuterIdx(outerIdx int) PhysicalPlan {
	innerChild := p.children[1-outerIdx].(LogicalPlan)
	var (
		usedIndexInfo *model.IndexInfo
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
	indices, includeTableScan := availableIndices(x.indexHints, x.tableInfo)
	if includeTableScan && len(innerJoinKeys) == 1 {
		pkCol := x.getPKIsHandleCol()
		if pkCol != nil && innerJoinKeys[0].Equal(pkCol, nil) {
			return p.constructIndexJoin(innerJoinKeys, outerJoinKeys, outerIdx)
		}
	}
	for _, indexInfo := range indices {
		matchedOffsets := joinKeysMatchIndex(innerJoinKeys, indexInfo)
		if matchedOffsets == nil {
			continue
		}
		usedIndexInfo = indexInfo
		newOuterJoinKeys := make([]*expression.Column, len(outerJoinKeys))
		newInnerJoinKeys := make([]*expression.Column, len(innerJoinKeys))
		for i, offset := range matchedOffsets {
			newOuterJoinKeys[i] = outerJoinKeys[offset]
			newInnerJoinKeys[i] = innerJoinKeys[offset]
		}
		outerJoinKeys = newOuterJoinKeys
		innerJoinKeys = newInnerJoinKeys
		break
	}
	if usedIndexInfo != nil {
		return p.constructIndexJoin(innerJoinKeys, outerJoinKeys, outerIdx)
	}
	return nil
}

// For index join, we shouldn't require a root task which may let CBO framework select a sort operator in fact.
// We are not sure which way of index scanning we should choose, so we try both single read and double read and finally
// it will result in a best one.
func (p *PhysicalIndexJoin) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	if !prop.isEmpty() {
		return nil
	}
	requiredProps1 := make([]*requiredProp, 2)
	requiredProps1[p.outerIndex] = &requiredProp{taskTp: rootTaskType}
	requiredProps1[1-p.outerIndex] = &requiredProp{taskTp: copSingleReadTaskType, cols: p.InnerJoinKeys}
	requiredProps2 := make([]*requiredProp, 2)
	requiredProps2[p.outerIndex] = &requiredProp{taskTp: rootTaskType}
	requiredProps2[1-p.outerIndex] = &requiredProp{taskTp: copDoubleReadTaskType, cols: p.InnerJoinKeys}
	return [][]*requiredProp{requiredProps1, requiredProps2}
}

func (p *PhysicalMergeJoin) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	leftKey := p.EqualConditions[0].GetArgs()[0].(*expression.Column)
	rightKey := p.EqualConditions[0].GetArgs()[1].(*expression.Column)
	lProp := &requiredProp{taskTp: rootTaskType, cols: []*expression.Column{leftKey}}
	rProp := &requiredProp{taskTp: rootTaskType, cols: []*expression.Column{rightKey}}
	if !prop.isEmpty() {
		if !prop.equal(lProp) && !prop.equal(rProp) {
			return nil
		}
		if prop.equal(rProp) && p.JoinType == LeftOuterJoin {
			return nil
		}
		if prop.equal(lProp) && p.JoinType == RightOuterJoin {
			return nil
		}
	}

	return [][]*requiredProp{{lProp, rProp}}
}

// tryToGetIndexJoin will get index join by hints. If we can generate a valid index join by hint, the second return value
// will be true, which means we force to choose this index join. Otherwise we will select a join algorithm with min-cost.
func (p *LogicalJoin) tryToGetIndexJoin() ([]PhysicalPlan, bool) {
	if len(p.EqualConditions) == 0 {
		return nil, false
	}
	plans := make([]PhysicalPlan, 0, 2)
	leftOuter := (p.preferINLJ & preferLeftAsOuter) > 0
	if leftOuter && p.JoinType != RightOuterJoin {
		join := p.getIndexJoinByOuterIdx(0)
		if join != nil {
			plans = append(plans, join)
		}
	}
	rightOuter := (p.preferINLJ & preferRightAsOuter) > 0
	if rightOuter && p.JoinType != LeftOuterJoin {
		join := p.getIndexJoinByOuterIdx(1)
		if join != nil {
			plans = append(plans, join)
		}
	}
	if len(plans) > 0 {
		return plans, true
	}
	// We try to choose join without considering hints.
	if p.JoinType != RightOuterJoin {
		join := p.getIndexJoinByOuterIdx(0)
		if join != nil {
			plans = append(plans, join)
		}
	}
	if p.JoinType != LeftOuterJoin {
		join := p.getIndexJoinByOuterIdx(1)
		if join != nil {
			plans = append(plans, join)
		}
	}
	return plans, false
}

func (p *LogicalJoin) generatePhysicalPlans() []PhysicalPlan {
	switch p.JoinType {
	case SemiJoin, LeftOuterSemiJoin:
		return []PhysicalPlan{p.getSemiJoin()}
	default:
		mj := p.getMergeJoin()
		if p.preferUseMergeJoin() {
			return []PhysicalPlan{mj}
		}
		joins := make([]PhysicalPlan, 0, 5)
		if len(p.EqualConditions) == 1 {
			joins = append(joins, mj)
		}
		idxJoins, forced := p.tryToGetIndexJoin()
		if forced {
			return idxJoins
		}
		joins = append(joins, idxJoins...)
		if p.JoinType != RightOuterJoin {
			leftJoin := p.getHashJoin(1)
			joins = append(joins, leftJoin)
		}
		if p.JoinType != LeftOuterJoin {
			rightJoin := p.getHashJoin(0)
			joins = append(joins, rightJoin)
		}
		return joins
	}
}

func (p *LogicalJoin) preferUseMergeJoin() bool {
	return p.preferMergeJoin && len(p.EqualConditions) == 1
}

func (p *LogicalJoin) getMergeJoin() PhysicalPlan {
	mergeJoin := PhysicalMergeJoin{
		JoinType:        p.JoinType,
		EqualConditions: p.EqualConditions,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		DefaultValues:   p.DefaultValues,
	}.init(p.allocator, p.ctx)
	mergeJoin.SetSchema(p.schema)
	return mergeJoin
}

func (p *LogicalJoin) getSemiJoin() PhysicalPlan {
	semiJoin := PhysicalHashSemiJoin{
		WithAux:         LeftOuterSemiJoin == p.JoinType,
		EqualConditions: p.EqualConditions,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		Anti:            p.anti,
	}.init(p.allocator, p.ctx)
	semiJoin.SetSchema(p.schema)
	return semiJoin
}

func (p *LogicalJoin) getHashJoin(smallTable int) PhysicalPlan {
	hashJoin := PhysicalHashJoin{
		EqualConditions: p.EqualConditions,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		JoinType:        p.JoinType,
		Concurrency:     JoinConcurrency,
		DefaultValues:   p.DefaultValues,
		SmallTable:      smallTable,
	}.init(p.allocator, p.ctx)
	hashJoin.SetSchema(p.schema)
	return hashJoin
}

// getPropByOrderByItems will check if this sort property can be pushed or not. In order to simplify the problem, we only
// consider the case that all expression are columns and all of them are asc or desc.
func getPropByOrderByItems(items []*ByItems, taskTp taskType) (*requiredProp, bool) {
	desc := false
	cols := make([]*expression.Column, 0, len(items))
	for i, item := range items {
		col, ok := item.Expr.(*expression.Column)
		if !ok {
			return nil, false
		}
		cols = append(cols, col)
		desc = item.Desc
		if i > 0 && item.Desc != items[i-1].Desc {
			return nil, false
		}
	}
	return &requiredProp{cols, desc, taskTp}, true
}

// convert2NewPhysicalPlan implements PhysicalPlan interface.
// If this sort is a topN plan, we will try to push the sort down and leave the limit.
// TODO: If this is a sort plan and the coming prop is not nil, this plan is redundant and can be removed.
func (p *Sort) convert2NewPhysicalPlan(prop *requiredProp) (task, error) {
	task, err := p.getTask(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if task != nil {
		return task, nil
	}
	if prop.taskTp != rootTaskType {
		// TODO: This is a trick here, because an operator that can be pushed to Coprocessor can never be pushed across sort.
		// e.g. If an aggregation want to be pushed, the SQL is always like select count(*) from t order by ...
		// The Sort will on top of Aggregation. If the SQL is like select count(*) from (select * from s order by k).
		// The Aggregation will also be blocked by projection. In the future we will break this restriction.
		return invalidTask, p.storeTask(prop, invalidTask)
	}
	// enforce branch
	task, err = p.children[0].(LogicalPlan).convert2NewPhysicalPlan(&requiredProp{taskTp: rootTaskType})
	if err != nil {
		return nil, errors.Trace(err)
	}
	task = p.attach2Task(task)
	newProp, canPassProp := getPropByOrderByItems(p.ByItems, rootTaskType)
	if canPassProp {
		orderedTask, err := p.children[0].(LogicalPlan).convert2NewPhysicalPlan(newProp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if orderedTask.cost() < task.cost() {
			task = orderedTask
		}
	}
	task = prop.enforceProperty(task, p.ctx, p.allocator)
	return task, p.storeTask(prop, task)
}

// convert2NewPhysicalPlan implements LogicalPlan interface.
func (p *TopN) convert2NewPhysicalPlan(prop *requiredProp) (task, error) {
	task, err := p.getTask(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if task != nil {
		return task, nil
	}
	if prop.taskTp != rootTaskType {
		// TopN can only return rootTask.
		return invalidTask, p.storeTask(prop, invalidTask)
	}
	for _, taskTp := range wholeTaskTypes {
		// Try to enforce topN for child.
		optTask, err := p.children[0].(LogicalPlan).convert2NewPhysicalPlan(&requiredProp{taskTp: taskTp})
		if err != nil {
			return nil, errors.Trace(err)
		}
		optTask = p.attach2Task(optTask)
		// Try to enforce sort to child and add limit for it.
		newProp, canPassProp := getPropByOrderByItems(p.ByItems, taskTp)
		if canPassProp {
			orderedTask, err := p.children[0].(LogicalPlan).convert2NewPhysicalPlan(newProp)
			if err != nil {
				return nil, errors.Trace(err)
			}
			limit := Limit{Offset: p.Offset, Count: p.Count}.init(p.allocator, p.ctx)
			limit.SetSchema(p.schema)
			orderedTask = limit.attach2Task(orderedTask)
			if orderedTask.cost() < optTask.cost() {
				optTask = orderedTask
			}
		}
		optTask = prop.enforceProperty(optTask, p.ctx, p.allocator)
		if task == nil || task.cost() > optTask.cost() {
			task = optTask
		}
	}
	return task, p.storeTask(prop, task)
}

// convert2NewPhysicalPlan implements LogicalPlan interface.
func (p *baseLogicalPlan) convert2NewPhysicalPlan(prop *requiredProp) (task, error) {
	// look up the task map
	task, err := p.getTask(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if task != nil {
		return task, nil
	}
	task = invalidTask
	if prop.taskTp != rootTaskType {
		// Currently all plan cannot totally push down.
		return task, p.storeTask(prop, task)
	}
	// Now we only consider rootTask.
	if len(p.basePlan.children) == 0 {
		// When the children length is 0, we process it specially.
		task = &rootTask{p: p.basePlan.self.(PhysicalPlan)}
		task = prop.enforceProperty(task, p.basePlan.ctx, p.basePlan.allocator)
		return task, p.storeTask(prop, task)
	}
	// Else we suppose it only has one child.
	for _, pp := range p.basePlan.self.(LogicalPlan).generatePhysicalPlans() {
		// We consider to add enforcer firstly.
		task, err = p.getBestTask(task, prop, pp, true)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if prop.isEmpty() {
			continue
		}
		task, err = p.getBestTask(task, prop, pp, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return task, p.storeTask(prop, task)
}

func (p *baseLogicalPlan) getBestTask(bestTask task, prop *requiredProp, pp PhysicalPlan, enforced bool) (task, error) {
	var newProps [][]*requiredProp
	if enforced {
		newProps = pp.getChildrenPossibleProps(&requiredProp{taskTp: rootTaskType})
	} else {
		newProps = pp.getChildrenPossibleProps(prop)
	}
	for _, newProp := range newProps {
		tasks := make([]task, 0, len(p.basePlan.children))
		for i, child := range p.basePlan.children {
			childTask, err := child.(LogicalPlan).convert2NewPhysicalPlan(newProp[i])
			if err != nil {
				return nil, errors.Trace(err)
			}
			tasks = append(tasks, childTask)
		}
		resultTask := pp.attach2Task(tasks...)
		if enforced {
			resultTask = prop.enforceProperty(resultTask, p.basePlan.ctx, p.basePlan.allocator)
		}
		if resultTask.cost() < bestTask.cost() {
			bestTask = resultTask
		}
	}
	return bestTask, nil
}

func tryToAddUnionScan(cop *copTask, conds []expression.Expression, ctx context.Context, allocator *idAllocator) task {
	if ctx.Txn() == nil || ctx.Txn().IsReadOnly() {
		return cop
	}
	task := finishCopTask(cop, ctx, allocator)
	us := PhysicalUnionScan{
		Conditions: conds,
	}.init(allocator, ctx)
	us.SetSchema(task.plan().Schema())
	return us.attach2Task(task)
}

// tryToGetMemTask will check if this table is a mem table. If it is, it will produce a task and store it.
func (p *DataSource) tryToGetMemTask(prop *requiredProp) (task task, err error) {
	client := p.ctx.GetClient()
	memDB := infoschema.IsMemoryDB(p.DBName.L)
	isDistReq := !memDB && client != nil && client.IsRequestTypeSupported(kv.ReqTypeSelect, 0)
	if isDistReq {
		return nil, nil
	}
	memTable := PhysicalMemTable{
		DBName:      p.DBName,
		Table:       p.tableInfo,
		Columns:     p.Columns,
		TableAsName: p.TableAsName,
	}.init(p.allocator, p.ctx)
	memTable.SetSchema(p.schema)
	memTable.Ranges = ranger.FullIntRange()
	var retPlan PhysicalPlan = memTable
	if len(p.pushedDownConds) > 0 {
		sel := Selection{
			Conditions: p.pushedDownConds,
		}.init(p.allocator, p.ctx)
		sel.SetSchema(p.schema)
		sel.SetChildren(memTable)
		retPlan = sel
	}
	task = &rootTask{p: retPlan}
	task = prop.enforceProperty(task, p.ctx, p.allocator)
	return task, nil
}

// tryToGetDualTask will check if the push down predicate has false constant. If so, it will return table dual.
func (p *DataSource) tryToGetDualTask() (task, error) {
	for _, cond := range p.pushedDownConds {
		if _, ok := cond.(*expression.Constant); ok {
			result, err := expression.EvalBool([]expression.Expression{cond}, nil, p.ctx)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !result {
				dual := TableDual{}.init(p.allocator, p.ctx)
				dual.SetSchema(p.schema)
				return &rootTask{
					p: dual,
				}, nil
			}
		}
	}
	return nil, nil
}

// convert2NewPhysicalPlan implements the PhysicalPlan interface.
// It will enumerate all the available indices and choose a plan with least cost.
func (p *DataSource) convert2NewPhysicalPlan(prop *requiredProp) (task, error) {
	task, err := p.getTask(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if task != nil {
		return task, nil
	}
	task, err = p.tryToGetDualTask()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if task != nil {
		return task, p.storeTask(prop, task)
	}
	task, err = p.tryToGetMemTask(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if task != nil {
		return task, p.storeTask(prop, task)
	}
	// TODO: We have not checked if this table has a predicate. If not, we can only consider table scan.
	indices, includeTableScan := availableIndices(p.indexHints, p.tableInfo)
	task = invalidTask
	if includeTableScan {
		task, err = p.convertToTableScan(prop)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	for _, idx := range indices {
		idxTask, err := p.convertToIndexScan(prop, idx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if idxTask.cost() < task.cost() {
			task = idxTask
		}
	}
	return task, p.storeTask(prop, task)
}

// convertToIndexScan converts the DataSource to index scan with idx.
func (p *DataSource) convertToIndexScan(prop *requiredProp, idx *model.IndexInfo) (task task, err error) {
	is := PhysicalIndexScan{
		Table:            p.tableInfo,
		TableAsName:      p.TableAsName,
		DBName:           p.DBName,
		Columns:          p.Columns,
		Index:            idx,
		dataSourceSchema: p.schema,
	}.init(p.allocator, p.ctx)
	statsTbl := p.statisticTable
	rowCount := float64(statsTbl.Count)
	sc := p.ctx.GetSessionVars().StmtCtx
	idxCols, colLengths := expression.IndexInfo2Cols(p.Schema().Columns, idx)
	is.Ranges = ranger.FullIndexRange()
	if len(p.pushedDownConds) > 0 {
		conds := make([]expression.Expression, 0, len(p.pushedDownConds))
		for _, cond := range p.pushedDownConds {
			conds = append(conds, cond.Clone())
		}
		if len(idxCols) > 0 {
			var ranges []types.Range
			ranges, is.AccessCondition, is.filterCondition, err = ranger.BuildRange(sc, conds, ranger.IndexRangeType, idxCols, colLengths)
			if err != nil {
				return nil, errors.Trace(err)
			}
			is.Ranges = ranger.Ranges2IndexRanges(ranges)
			rowCount, err = statsTbl.GetRowCountByIndexRanges(sc, is.Index.ID, is.Ranges)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			is.filterCondition = conds
		}
	}
	cop := &copTask{
		cnt:       rowCount,
		cst:       rowCount * scanFactor,
		indexPlan: is,
	}
	if !isCoveringIndex(is.Columns, is.Index.Columns, is.Table.PKIsHandle) {
		// On this way, it's double read case.
		cop.tablePlan = PhysicalTableScan{Columns: p.Columns, Table: is.Table}.init(p.allocator, p.ctx)
		cop.tablePlan.SetSchema(p.schema)
		// If it's parent requires single read task, return max cost.
		if prop.taskTp == copSingleReadTaskType {
			return &copTask{cst: math.MaxFloat64}, nil
		}
	} else if prop.taskTp == copDoubleReadTaskType {
		// If it's parent requires double read task, return max cost.
		return &copTask{cst: math.MaxFloat64}, nil
	}
	var indexCols []*expression.Column
	for _, col := range idx.Columns {
		indexCols = append(indexCols, &expression.Column{FromID: p.id, Position: col.Offset})
	}
	if is.Table.PKIsHandle {
		for _, col := range is.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				indexCols = append(indexCols, &expression.Column{FromID: p.id, Position: col.Offset})
				break
			}
		}
	}
	is.SetSchema(expression.NewSchema(indexCols...))
	// Check if this plan matches the property.
	matchProperty := true
	if !prop.isEmpty() {
		for i, col := range idx.Columns {
			// not matched
			if col.Name.L == prop.cols[0].ColName.L {
				matchProperty = matchIndicesProp(idx.Columns[i:], prop.cols)
				break
			} else if i >= len(is.AccessCondition) || is.AccessCondition[i].(*expression.ScalarFunction).FuncName.L != ast.EQ {
				matchProperty = false
				break
			}
		}
	}
	if matchProperty && !prop.isEmpty() {
		if prop.desc {
			is.Desc = true
			cop.cst = rowCount * descScanFactor
		}
		is.addPushedDownSelection(cop)
		task = tryToAddUnionScan(cop, p.pushedDownConds, p.ctx, p.allocator)
	} else {
		is.OutOfOrder = true
		is.addPushedDownSelection(cop)
		task = tryToAddUnionScan(cop, p.pushedDownConds, p.ctx, p.allocator)
		task = prop.enforceProperty(task, p.ctx, p.allocator)
	}
	if prop.taskTp == rootTaskType {
		task = finishCopTask(task, p.ctx, p.allocator)
	} else if _, ok := task.(*rootTask); ok {
		return invalidTask, nil
	}
	return task, nil
}

func (is *PhysicalIndexScan) addPushedDownSelection(copTask *copTask) {
	// Add filter condition to table plan now.
	if len(is.filterCondition) > 0 {
		var indexConds, tableConds []expression.Expression
		if copTask.tablePlan != nil {
			tableConds, indexConds = splitConditionsByIndexColumns(is.filterCondition, is.schema)
		} else {
			indexConds = is.filterCondition
		}
		if indexConds != nil {
			indexSel := Selection{Conditions: indexConds}.init(is.allocator, is.ctx)
			indexSel.SetSchema(is.schema)
			indexSel.SetChildren(is)
			copTask.indexPlan = indexSel
			copTask.cst += copTask.cnt * cpuFactor
			copTask.cnt = copTask.cnt * selectionFactor
		}
		if tableConds != nil {
			copTask.finishIndexPlan()
			tableSel := Selection{Conditions: tableConds}.init(is.allocator, is.ctx)
			tableSel.SetSchema(copTask.tablePlan.Schema())
			tableSel.SetChildren(copTask.tablePlan)
			copTask.tablePlan = tableSel
			copTask.cst += copTask.cnt * cpuFactor
			copTask.cnt = copTask.cnt * selectionFactor
		}
	}
}

func matchIndicesProp(idxCols []*model.IndexColumn, propCols []*expression.Column) bool {
	if len(idxCols) < len(propCols) {
		return false
	}
	for i, col := range propCols {
		if idxCols[i].Length != types.UnspecifiedLength || col.ColName.L != idxCols[i].Name.L {
			return false
		}
	}
	return true
}

// convertToTableScan converts the DataSource to table scan.
func (p *DataSource) convertToTableScan(prop *requiredProp) (task task, err error) {
	if prop.taskTp == copDoubleReadTaskType {
		return &copTask{cst: math.MaxFloat64}, nil
	}
	ts := PhysicalTableScan{
		Table:       p.tableInfo,
		Columns:     p.Columns,
		TableAsName: p.TableAsName,
		DBName:      p.DBName,
	}.init(p.allocator, p.ctx)
	ts.SetSchema(p.schema)
	sc := p.ctx.GetSessionVars().StmtCtx
	ts.Ranges = ranger.FullIntRange()
	var pkCol *expression.Column
	if ts.Table.PKIsHandle {
		if pkColInfo := ts.Table.GetPkColInfo(); pkColInfo != nil {
			pkCol = expression.ColInfo2Col(ts.schema.Columns, pkColInfo)
		}
	}
	if len(p.pushedDownConds) > 0 {
		conds := make([]expression.Expression, 0, len(p.pushedDownConds))
		for _, cond := range p.pushedDownConds {
			conds = append(conds, cond.Clone())
		}
		if pkCol != nil {
			var ranges []types.Range
			ranges, ts.AccessCondition, ts.filterCondition, err = ranger.BuildRange(sc, conds, ranger.IntRangeType, []*expression.Column{pkCol}, nil)
			ts.Ranges = ranger.Ranges2IntRanges(ranges)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			ts.filterCondition = conds
		}
	}
	statsTbl := p.statisticTable
	rowCount := float64(statsTbl.Count)
	if pkCol != nil {
		rowCount, err = statsTbl.GetRowCountByIntColumnRanges(sc, pkCol.ID, ts.Ranges)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	cost := rowCount * scanFactor
	copTask := &copTask{
		cnt:               rowCount,
		tablePlan:         ts,
		cst:               cost,
		indexPlanFinished: true,
	}
	task = copTask
	if pkCol != nil && len(prop.cols) == 1 && prop.cols[0].Equal(pkCol, nil) {
		if prop.desc {
			ts.Desc = true
			copTask.cst = rowCount * descScanFactor
		}
		ts.KeepOrder = true
		ts.addPushedDownSelection(copTask)
		task = tryToAddUnionScan(copTask, p.pushedDownConds, p.ctx, p.allocator)
	} else {
		ts.addPushedDownSelection(copTask)
		task = tryToAddUnionScan(copTask, p.pushedDownConds, p.ctx, p.allocator)
		task = prop.enforceProperty(task, p.ctx, p.allocator)
	}
	if prop.taskTp == rootTaskType {
		task = finishCopTask(task, p.ctx, p.allocator)
	} else if _, ok := task.(*rootTask); ok {
		return invalidTask, nil
	}
	return task, nil
}

func (ts *PhysicalTableScan) addPushedDownSelection(copTask *copTask) {
	// Add filter condition to table plan now.
	if len(ts.filterCondition) > 0 {
		sel := Selection{Conditions: ts.filterCondition}.init(ts.allocator, ts.ctx)
		sel.SetSchema(ts.schema)
		sel.SetChildren(ts)
		copTask.tablePlan = sel
		copTask.cst += copTask.cnt * cpuFactor
		copTask.cnt = copTask.cnt * selectionFactor
	}
}

// splitConditionsByIndexColumns splits the conditions by index schema. If some condition only contain the index
// columns, it will be pushed to index plan.
func splitConditionsByIndexColumns(conditions []expression.Expression, schema *expression.Schema) (tableConds []expression.Expression, indexConds []expression.Expression) {
	for _, cond := range conditions {
		cols := expression.ExtractColumns(cond)
		indices := schema.ColumnsIndices(cols)
		if len(indices) == 0 {
			tableConds = append(tableConds, cond)
		} else {
			indexConds = append(indexConds, cond)
		}
	}
	return
}

func (p *LogicalApply) generatePhysicalPlans() []PhysicalPlan {
	var join PhysicalPlan
	if p.JoinType == SemiJoin || p.JoinType == LeftOuterSemiJoin {
		join = p.getSemiJoin()
	} else {
		join = p.getHashJoin(1)
	}
	apply := PhysicalApply{
		PhysicalJoin: join,
		OuterSchema:  p.corCols,
	}.init(p.allocator, p.ctx)
	apply.SetSchema(p.schema)
	return []PhysicalPlan{apply}
}

func (p *baseLogicalPlan) generatePhysicalPlans() []PhysicalPlan {
	np := p.basePlan.self.(PhysicalPlan).Copy()
	return []PhysicalPlan{np}
}

func (p *basePhysicalPlan) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	// By default, physicalPlan can always match the orders.
	props := make([]*requiredProp, 0, len(p.basePlan.children))
	for range p.basePlan.children {
		props = append(props, prop)
	}
	return [][]*requiredProp{props}
}

func (p *PhysicalHashJoin) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	if !prop.isEmpty() {
		return nil
	}
	return [][]*requiredProp{{&requiredProp{taskTp: rootTaskType}, &requiredProp{taskTp: rootTaskType}}}
}

func (p *PhysicalHashSemiJoin) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	lProp := &requiredProp{taskTp: rootTaskType, cols: prop.cols}
	for _, col := range lProp.cols {
		// FIXME: This condition may raise a panic, fix it in the future.
		if p.children[0].Schema().ColumnIndex(col) == -1 {
			return nil
		}
	}
	return [][]*requiredProp{{&requiredProp{taskTp: rootTaskType}, &requiredProp{taskTp: rootTaskType}}}
}

func (p *PhysicalApply) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	lProp := &requiredProp{taskTp: rootTaskType, cols: prop.cols}
	for _, col := range lProp.cols {
		// FIXME: This condition may raise a panic, fix it in the future.
		if p.children[0].Schema().ColumnIndex(col) == -1 {
			return nil
		}
	}
	return [][]*requiredProp{{lProp, &requiredProp{taskTp: rootTaskType}}}
}

func (p *Limit) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	if !prop.isEmpty() {
		return nil
	}
	props := make([][]*requiredProp, 0, len(wholeTaskTypes))
	for _, tp := range wholeTaskTypes {
		props = append(props, []*requiredProp{{taskTp: tp}})
	}
	return props
}

func (p *LogicalAggregation) generatePhysicalPlans() []PhysicalPlan {
	ha := PhysicalAggregation{
		GroupByItems: p.GroupByItems,
		AggFuncs:     p.AggFuncs,
		HasGby:       len(p.GroupByItems) > 0,
		AggType:      CompleteAgg,
	}.init(p.allocator, p.ctx)
	ha.SetSchema(p.schema)
	return []PhysicalPlan{ha}
}

func (p *PhysicalAggregation) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	if !prop.isEmpty() {
		return nil
	}
	props := make([][]*requiredProp, 0, len(wholeTaskTypes))
	for _, tp := range wholeTaskTypes {
		props = append(props, []*requiredProp{{taskTp: tp}})
	}
	return props
}
