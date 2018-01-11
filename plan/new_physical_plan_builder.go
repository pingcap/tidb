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

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/types"
)

// wholeTaskTypes records all possible kinds of task that a plan can return. For Agg, TopN and Limit, we will try to get
// these tasks one by one.
var wholeTaskTypes = [...]taskType{copSingleReadTaskType, copDoubleReadTaskType, rootTaskType}

var invalidTask = &rootTask{cst: math.MaxFloat64}

func (p *requiredProp) enforceProperty(task task, ctx context.Context, allocator *idAllocator) task {
	if p.isEmpty() {
		return task
	}
	// If task is invalid, keep it remained.
	if task.plan() == nil {
		return task
	}
	task = finishCopTask(task, ctx, allocator)
	sort := Sort{ByItems: make([]*ByItems, 0, len(p.cols))}.init(allocator, ctx)
	for _, col := range p.cols {
		sort.ByItems = append(sort.ByItems, &ByItems{col, p.desc})
	}
	sort.SetSchema(task.plan().Schema())
	sort.profile = task.plan().statsProfile()
	return sort.attach2Task(task)
}

// getChildrenPossibleProps will check if this sort property can be pushed or not.
// When a sort column will be replaced by scalar function, we refuse it.
// When a sort column will be replaced by a constant, we just remove it.
func (p *Projection) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	p.expectedCnt = prop.expectedCnt
	newProp := &requiredProp{taskTp: rootTaskType, expectedCnt: prop.expectedCnt}
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

func (p *LogicalJoin) constructIndexJoin(innerJoinKeys, outerJoinKeys []*expression.Column, outerIdx int, innerPlan PhysicalPlan) []PhysicalPlan {
	join := PhysicalIndexJoin{
		OuterIndex:      outerIdx,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		Outer:           p.JoinType != InnerJoin,
		OuterJoinKeys:   outerJoinKeys,
		InnerJoinKeys:   innerJoinKeys,
		DefaultValues:   p.DefaultValues,
		outerSchema:     p.children[outerIdx].Schema(),
		innerPlan:       innerPlan,
	}.init(p.allocator, p.ctx, p.children...)
	join.SetSchema(p.schema)
	join.profile = p.profile
	orderJoin := join.Copy().(*PhysicalIndexJoin)
	orderJoin.KeepOrder = true
	return []PhysicalPlan{join, orderJoin}
}

// getIndexJoinByOuterIdx will generate index join by OuterIndex. OuterIdx points out the outer child,
// because we will swap the children of join when the right child is outer child.
// First of all, we will extract the join keys for p's equal conditions. If the join keys can match some of the indices or PK
// column of inner child, we can apply the index join.
func (p *LogicalJoin) getIndexJoinByOuterIdx(outerIdx int) []PhysicalPlan {
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
	if !ok || x.unionScanSchema != nil {
		return nil
	}
	indices, includeTableScan := availableIndices(x.indexHints, x.tableInfo)
	if includeTableScan && len(innerJoinKeys) == 1 {
		pkCol := x.getPKIsHandleCol()
		if pkCol != nil && innerJoinKeys[0].Equal(pkCol, nil) {
			innerPlan := x.forceToTableScan()
			return p.constructIndexJoin(innerJoinKeys, outerJoinKeys, outerIdx, innerPlan)
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
		innerPlan := x.forceToIndexScan(usedIndexInfo)
		return p.constructIndexJoin(innerJoinKeys, outerJoinKeys, outerIdx, innerPlan)
	}
	return nil
}

// getChildrenPossibleProps gets children possible props.:
// For index join, we shouldn't require a root task which may let CBO framework select a sort operator in fact.
// We are not sure which way of index scanning we should choose, so we try both single read and double read and finally
// it will result in a best one.
func (p *PhysicalIndexJoin) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	p.expectedCnt = prop.expectedCnt
	if !prop.isEmpty() && !p.KeepOrder {
		return nil
	}
	for _, col := range prop.cols {
		if p.outerSchema.ColumnIndex(col) == -1 {
			return nil
		}
	}
	requiredProps1 := make([]*requiredProp, 2)
	requiredProps1[p.OuterIndex] = &requiredProp{taskTp: rootTaskType, expectedCnt: prop.expectedCnt, cols: prop.cols, desc: prop.desc}
	requiredProps1[1-p.OuterIndex] = &requiredProp{taskTp: rootTaskType, expectedCnt: math.MaxFloat64}
	return [][]*requiredProp{requiredProps1}
}

func (p *PhysicalMergeJoin) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	p.expectedCnt = prop.expectedCnt
	lProp := &requiredProp{taskTp: rootTaskType, cols: p.leftKeys, expectedCnt: math.MaxFloat64}
	rProp := &requiredProp{taskTp: rootTaskType, cols: p.rightKeys, expectedCnt: math.MaxFloat64}
	if !prop.isEmpty() {
		if prop.desc {
			return nil
		}
		if !prop.isPrefix(lProp) && !prop.isPrefix(rProp) {
			return nil
		}
		if prop.isPrefix(rProp) && p.JoinType == LeftOuterJoin {
			return nil
		}
		if prop.isPrefix(lProp) && p.JoinType == RightOuterJoin {
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
			plans = append(plans, join...)
		}
	}
	rightOuter := (p.preferINLJ & preferRightAsOuter) > 0
	if rightOuter && p.JoinType != LeftOuterJoin {
		join := p.getIndexJoinByOuterIdx(1)
		if join != nil {
			plans = append(plans, join...)
		}
	}
	if len(plans) > 0 {
		return plans, true
	}
	// We try to choose join without considering hints.
	if p.JoinType != RightOuterJoin {
		join := p.getIndexJoinByOuterIdx(0)
		if join != nil {
			plans = append(plans, join...)
		}
	}
	if p.JoinType != LeftOuterJoin {
		join := p.getIndexJoinByOuterIdx(1)
		if join != nil {
			plans = append(plans, join...)
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
		if p.preferMergeJoin && len(mj) > 0 {
			return mj
		}
		joins := make([]PhysicalPlan, 0, 5)
		if len(p.EqualConditions) == 1 {
			joins = append(joins, mj...)
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

func (p *LogicalJoin) getMergeJoin() []PhysicalPlan {
	joins := make([]PhysicalPlan, 0, len(p.leftProperties))
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
		}.init(p.allocator, p.ctx)
		mergeJoin.SetSchema(p.schema)
		mergeJoin.profile = p.profile
		mergeJoin.EqualConditions, mergeJoin.OtherConditions = p.getEqAndOtherCondsByOffsets(offsets)
		joins = append(joins, mergeJoin)
	}
	return joins
}

func (p *LogicalJoin) getSemiJoin() PhysicalPlan {
	semiJoin := PhysicalHashSemiJoin{
		WithAux:         LeftOuterSemiJoin == p.JoinType,
		EqualConditions: p.EqualConditions,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		Anti:            p.anti,
		rightChOffset:   p.children[0].Schema().Len(),
	}.init(p.allocator, p.ctx)
	semiJoin.SetSchema(p.schema)
	semiJoin.profile = p.profile
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
	hashJoin.profile = p.profile
	return hashJoin
}

// getPropByOrderByItems will check if this sort property can be pushed or not. In order to simplify the problem, we only
// consider the case that all expression are columns and all of them are asc or desc.
func getPropByOrderByItems(items []*ByItems) (*requiredProp, bool) {
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
	return &requiredProp{cols: cols, desc: desc}, true
}

func (p *TopN) generatePhysicalPlans() []PhysicalPlan {
	plans := []PhysicalPlan{p.Copy()}
	if prop, canPass := getPropByOrderByItems(p.ByItems); canPass {
		limit := Limit{
			Count:        p.Count,
			Offset:       p.Offset,
			partial:      p.partial,
			expectedProp: prop,
		}.init(p.allocator, p.ctx)
		limit.SetSchema(p.schema)
		limit.profile = p.profile
		plans = append(plans, limit)
	}
	return plans
}

// convert2NewPhysicalPlan implements PhysicalPlan interface.
// If this sort is a topN plan, we will try to push the sort down and leave the limit.
// TODO: If this is a sort plan and the coming prop is not nil, this plan is redundant and can be removed.
func (p *Sort) convert2NewPhysicalPlan(prop *requiredProp) (task, error) {
	t := p.getTask(prop)
	if t != nil {
		return t, nil
	}
	if prop.taskTp != rootTaskType {
		// TODO: This is a trick here, because an operator that can be pushed to Coprocessor can never be pushed across sort.
		// e.g. If an aggregation want to be pushed, the SQL is always like select count(*) from t order by ...
		// The Sort will on top of Aggregation. If the SQL is like select count(*) from (select * from s order by k).
		// The Aggregation will also be blocked by projection. In the future we will break this restriction.
		p.storeTask(prop, invalidTask)
		return invalidTask, nil
	}
	// enforce branch
	t, err := p.children[0].(LogicalPlan).convert2NewPhysicalPlan(&requiredProp{taskTp: rootTaskType, expectedCnt: math.MaxFloat64})
	if err != nil {
		return nil, errors.Trace(err)
	}
	t = p.attach2Task(t)
	newProp, canPassProp := getPropByOrderByItems(p.ByItems)
	if canPassProp {
		newProp.expectedCnt = prop.expectedCnt
		orderedTask, err := p.children[0].(LogicalPlan).convert2NewPhysicalPlan(newProp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if orderedTask.cost() < t.cost() {
			t = orderedTask
		}
	}
	t = prop.enforceProperty(t, p.ctx, p.allocator)
	p.storeTask(prop, t)
	return t, nil
}

// convert2NewPhysicalPlan implements LogicalPlan interface.
func (p *baseLogicalPlan) convert2NewPhysicalPlan(prop *requiredProp) (t task, err error) {
	// look up the task map
	t = p.getTask(prop)
	if t != nil {
		return t, nil
	}
	t = invalidTask
	if prop.taskTp != rootTaskType {
		// Currently all plan cannot totally push down.
		p.storeTask(prop, t)
		return t, nil
	}
	// Now we only consider rootTask.
	if len(p.basePlan.children) == 0 {
		// When the children length is 0, we process it specially.
		t = &rootTask{p: p.basePlan.self.(PhysicalPlan)}
		t = prop.enforceProperty(t, p.basePlan.ctx, p.basePlan.allocator)
		p.storeTask(prop, t)
		return t, nil
	}
	// Else we suppose it only has one child.
	for _, pp := range p.basePlan.self.(LogicalPlan).generatePhysicalPlans() {
		// We consider to add enforcer firstly.
		t, err = p.getBestTask(t, prop, pp, true)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if prop.isEmpty() {
			continue
		}
		t, err = p.getBestTask(t, prop, pp, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	p.storeTask(prop, t)
	return t, nil
}

func (p *baseLogicalPlan) getBestTask(bestTask task, prop *requiredProp, pp PhysicalPlan, enforced bool) (task, error) {
	var newProps [][]*requiredProp
	if enforced {
		newProps = pp.getChildrenPossibleProps(&requiredProp{taskTp: rootTaskType, expectedCnt: math.MaxFloat64})
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

func addUnionScan(cop *copTask, ds *DataSource) task {
	t := finishCopTask(cop, ds.ctx, ds.allocator)
	us := PhysicalUnionScan{
		Conditions:    ds.pushedDownConds,
		NeedColHandle: ds.NeedColHandle,
	}.init(ds.allocator, ds.ctx)
	us.SetSchema(ds.unionScanSchema)
	us.profile = t.plan().statsProfile()
	return us.attach2Task(t)
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
	memTable.profile = p.profile
	var retPlan PhysicalPlan = memTable
	if len(p.pushedDownConds) > 0 {
		sel := Selection{
			Conditions: p.pushedDownConds,
		}.init(p.allocator, p.ctx)
		sel.SetSchema(p.schema)
		sel.SetChildren(memTable)
		sel.profile = p.profile
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
				dual.profile = p.profile
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
	if prop == nil {
		return nil, nil
	}
	t := p.getTask(prop)
	if t != nil {
		return t, nil
	}
	t, err := p.tryToGetDualTask()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if t != nil {
		p.storeTask(prop, t)
		return t, nil
	}
	t, err = p.tryToGetMemTask(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if t != nil {
		p.storeTask(prop, t)
		return t, nil
	}
	// TODO: We have not checked if this table has a predicate. If not, we can only consider table scan.
	indices, includeTableScan := availableIndices(p.indexHints, p.tableInfo)
	t = invalidTask
	if includeTableScan {
		t, err = p.convertToTableScan(prop)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if !includeTableScan || len(p.pushedDownConds) > 0 || len(prop.cols) > 0 {
		for _, idx := range indices {
			idxTask, err := p.convertToIndexScan(prop, idx)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if idxTask.cost() < t.cost() {
				t = idxTask
			}
		}
	}
	p.storeTask(prop, t)
	return t, nil
}

func (p *DataSource) forceToIndexScan(idx *model.IndexInfo) PhysicalPlan {
	is := PhysicalIndexScan{
		Table:               p.tableInfo,
		TableAsName:         p.TableAsName,
		DBName:              p.DBName,
		Columns:             p.Columns,
		Index:               idx,
		dataSourceSchema:    p.schema,
		physicalTableSource: physicalTableSource{NeedColHandle: p.NeedColHandle},
		Ranges:              ranger.FullIndexRange(),
		OutOfOrder:          true,
	}.init(p.allocator, p.ctx)
	is.filterCondition = p.pushedDownConds
	is.profile = p.profile
	cop := &copTask{
		indexPlan: is,
	}
	if !isCoveringIndex(is.Columns, is.Index.Columns, is.Table.PKIsHandle) {
		// On this way, it's double read case.
		cop.tablePlan = PhysicalTableScan{Columns: p.Columns, Table: is.Table}.init(p.allocator, p.ctx)
		cop.tablePlan.SetSchema(is.dataSourceSchema)
	}
	is.initSchema(p.id, idx, cop.tablePlan != nil)
	is.addPushedDownSelection(cop, p, math.MaxFloat64)
	t := finishCopTask(cop, p.ctx, p.allocator)
	return t.plan()
}

// convertToIndexScan converts the DataSource to index scan with idx.
func (p *DataSource) convertToIndexScan(prop *requiredProp, idx *model.IndexInfo) (task task, err error) {
	is := PhysicalIndexScan{
		Table:               p.tableInfo,
		TableAsName:         p.TableAsName,
		DBName:              p.DBName,
		Columns:             p.Columns,
		Index:               idx,
		dataSourceSchema:    p.schema,
		physicalTableSource: physicalTableSource{NeedColHandle: p.NeedColHandle || p.unionScanSchema != nil},
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
			is.AccessCondition, is.filterCondition = ranger.DetachIndexConditions(conds, idxCols, colLengths)
			ranges, err = ranger.BuildRange(sc, is.AccessCondition, ranger.IndexRangeType, idxCols, colLengths)
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
	is.profile = p.getStatsProfileByFilter(p.pushedDownConds)

	cop := &copTask{
		indexPlan: is,
	}
	if !isCoveringIndex(is.Columns, is.Index.Columns, is.Table.PKIsHandle) {
		// On this way, it's double read case.
		cop.tablePlan = PhysicalTableScan{Columns: p.Columns, Table: is.Table}.init(p.allocator, p.ctx)
		cop.tablePlan.SetSchema(is.dataSourceSchema.Clone())
		// If it's parent requires single read task, return max cost.
		if prop.taskTp == copSingleReadTaskType {
			return &copTask{cst: math.MaxFloat64}, nil
		}
	} else if prop.taskTp == copDoubleReadTaskType {
		// If it's parent requires double read task, return max cost.
		return &copTask{cst: math.MaxFloat64}, nil
	}
	is.initSchema(p.id, idx, cop.tablePlan != nil)
	// Check if this plan matches the property.
	matchProperty := false
	if !prop.isEmpty() {
		for i, col := range idx.Columns {
			// not matched
			if col.Name.L == prop.cols[0].ColName.L {
				matchProperty = matchIndicesProp(idx.Columns[i:], prop.cols)
				break
			} else if i >= len(is.AccessCondition) {
				break
			} else if sf, ok := is.AccessCondition[i].(*expression.ScalarFunction); !ok || sf.FuncName.L != ast.EQ {
				break
			}
		}
	}
	if matchProperty && prop.expectedCnt < math.MaxFloat64 {
		selectivity, err := p.statisticTable.Selectivity(p.ctx, is.filterCondition)
		if err != nil {
			log.Warnf("An error happened: %v, we have to use the default selectivity", err.Error())
			selectivity = selectionFactor
		}
		rowCount = math.Min(prop.expectedCnt/selectivity, rowCount)
	}
	is.expectedCnt = rowCount
	cop.cst = rowCount * scanFactor
	task = cop
	if matchProperty {
		if prop.desc {
			is.Desc = true
			cop.cst = rowCount * descScanFactor
		}
		if !is.NeedColHandle && cop.tablePlan != nil {
			tblPlan := cop.tablePlan.(*PhysicalTableScan)
			tblPlan.Columns = append(tblPlan.Columns, &model.ColumnInfo{
				ID:   model.ExtraHandleID,
				Name: model.NewCIStr("_rowid"),
			})
		}
		cop.keepOrder = true
		is.addPushedDownSelection(cop, p, prop.expectedCnt)
		if p.unionScanSchema != nil {
			task = addUnionScan(cop, p)
		}
	} else {
		is.OutOfOrder = true
		expectedCnt := math.MaxFloat64
		if prop.isEmpty() {
			expectedCnt = prop.expectedCnt
		}
		is.addPushedDownSelection(cop, p, expectedCnt)
		if p.unionScanSchema != nil {
			task = addUnionScan(cop, p)
		}
		task = prop.enforceProperty(task, p.ctx, p.allocator)
	}
	if prop.taskTp == rootTaskType {
		task = finishCopTask(task, p.ctx, p.allocator)
	} else if _, ok := task.(*rootTask); ok {
		return invalidTask, nil
	}
	return task, nil
}

func (is *PhysicalIndexScan) initSchema(id int, idx *model.IndexInfo, isDoubleRead bool) {
	var indexCols []*expression.Column
	for _, col := range idx.Columns {
		indexCols = append(indexCols, &expression.Column{FromID: id, Position: col.Offset})
	}
	setHandle := false
	for _, col := range is.Columns {
		if (mysql.HasPriKeyFlag(col.Flag) && is.Table.PKIsHandle) || col.ID == model.ExtraHandleID {
			indexCols = append(indexCols, &expression.Column{FromID: id, ID: col.ID, Position: col.Offset})
			setHandle = true
			break
		}
	}
	// If it's double read case, the first index must return handle. So we should add extra handle column
	// if there isn't a handle column.
	if isDoubleRead && !setHandle {
		indexCols = append(indexCols, &expression.Column{FromID: id, ID: model.ExtraHandleID, Position: -1})
	}
	is.SetSchema(expression.NewSchema(indexCols...))
}

func (is *PhysicalIndexScan) addPushedDownSelection(copTask *copTask, p *DataSource, expectedCnt float64) {
	// Add filter condition to table plan now.
	if len(is.filterCondition) > 0 {
		var indexConds, tableConds []expression.Expression
		if copTask.tablePlan != nil {
			indexConds, tableConds = ranger.DetachIndexFilterConditions(is.filterCondition, is.Index.Columns, is.Table)
		} else {
			indexConds = is.filterCondition
		}
		if indexConds != nil {
			condsClone := make([]expression.Expression, 0, len(indexConds))
			for _, cond := range indexConds {
				condsClone = append(condsClone, cond.Clone())
			}
			indexSel := Selection{Conditions: condsClone}.init(is.allocator, is.ctx)
			indexSel.SetSchema(is.schema)
			indexSel.SetChildren(is)
			indexSel.profile = p.getStatsProfileByFilter(append(is.AccessCondition, indexConds...))
			// FIXME: It is not precise.
			indexSel.expectedCnt = expectedCnt
			copTask.indexPlan = indexSel
			copTask.cst += copTask.count() * cpuFactor
		}
		if tableConds != nil {
			copTask.finishIndexPlan()
			tableSel := Selection{Conditions: tableConds}.init(is.allocator, is.ctx)
			tableSel.SetSchema(copTask.tablePlan.Schema())
			tableSel.SetChildren(copTask.tablePlan)
			tableSel.profile = p.profile
			tableSel.expectedCnt = expectedCnt
			copTask.tablePlan = tableSel
			copTask.cst += copTask.count() * cpuFactor
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

func (p *DataSource) forceToTableScan() PhysicalPlan {
	ts := PhysicalTableScan{
		Table:               p.tableInfo,
		Columns:             p.Columns,
		TableAsName:         p.TableAsName,
		DBName:              p.DBName,
		physicalTableSource: physicalTableSource{NeedColHandle: p.NeedColHandle},
		Ranges:              ranger.FullIntRange(),
	}.init(p.allocator, p.ctx)
	ts.SetSchema(p.schema)
	ts.profile = p.profile
	ts.filterCondition = p.pushedDownConds
	copTask := &copTask{
		tablePlan:         ts,
		indexPlanFinished: true,
	}
	ts.addPushedDownSelection(copTask, p.profile, math.MaxFloat64)
	t := finishCopTask(copTask, p.ctx, p.allocator)
	return t.plan()
}

// convertToTableScan converts the DataSource to table scan.
func (p *DataSource) convertToTableScan(prop *requiredProp) (task task, err error) {
	if prop.taskTp == copDoubleReadTaskType {
		return &copTask{cst: math.MaxFloat64}, nil
	}
	ts := PhysicalTableScan{
		Table:               p.tableInfo,
		Columns:             p.Columns,
		TableAsName:         p.TableAsName,
		DBName:              p.DBName,
		physicalTableSource: physicalTableSource{NeedColHandle: p.NeedColHandle || p.unionScanSchema != nil},
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
			ts.AccessCondition, ts.filterCondition = ranger.DetachColumnConditions(conds, pkCol.ColName)
			ranges, err = ranger.BuildRange(sc, ts.AccessCondition, ranger.IntRangeType, []*expression.Column{pkCol}, nil)
			ts.Ranges = ranger.Ranges2IntRanges(ranges)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			ts.filterCondition = conds
		}
	}
	ts.profile = p.getStatsProfileByFilter(p.pushedDownConds)
	statsTbl := p.statisticTable
	rowCount := float64(statsTbl.Count)
	if pkCol != nil {
		// TODO: We can use p.getStatsProfileByFilter(accessConditions).
		rowCount, err = statsTbl.GetRowCountByIntColumnRanges(sc, pkCol.ID, ts.Ranges)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	copTask := &copTask{
		tablePlan:         ts,
		indexPlanFinished: true,
	}
	task = copTask
	matchProperty := len(prop.cols) == 1 && pkCol != nil && prop.cols[0].Equal(pkCol, nil)
	if matchProperty && prop.expectedCnt < math.MaxFloat64 {
		selectivity, err := p.statisticTable.Selectivity(p.ctx, ts.filterCondition)
		if err != nil {
			log.Warnf("An error happened: %v, we have to use the default selectivity", err.Error())
			selectivity = selectionFactor
		}
		rowCount = math.Min(prop.expectedCnt/selectivity, rowCount)
	}
	ts.expectedCnt = rowCount
	copTask.cst = rowCount * scanFactor
	if matchProperty {
		if prop.desc {
			ts.Desc = true
			copTask.cst = rowCount * descScanFactor
		}
		ts.KeepOrder = true
		copTask.keepOrder = true
		ts.addPushedDownSelection(copTask, p.profile, prop.expectedCnt)
		if p.unionScanSchema != nil {
			task = addUnionScan(copTask, p)
		}
	} else {
		expectedCnt := math.MaxFloat64
		if prop.isEmpty() {
			expectedCnt = prop.expectedCnt
		}
		ts.addPushedDownSelection(copTask, p.profile, expectedCnt)
		if p.unionScanSchema != nil {
			task = addUnionScan(copTask, p)
		}
		task = prop.enforceProperty(task, p.ctx, p.allocator)
	}
	if prop.taskTp == rootTaskType {
		task = finishCopTask(task, p.ctx, p.allocator)
	} else if _, ok := task.(*rootTask); ok {
		return invalidTask, nil
	}
	return task, nil
}

func (ts *PhysicalTableScan) addPushedDownSelection(copTask *copTask, profile *statsProfile, expectedCnt float64) {
	// Add filter condition to table plan now.
	if len(ts.filterCondition) > 0 {
		sel := Selection{Conditions: ts.filterCondition}.init(ts.allocator, ts.ctx)
		sel.SetSchema(ts.schema)
		sel.SetChildren(ts)
		sel.profile = profile
		sel.expectedCnt = expectedCnt
		copTask.tablePlan = sel
		// FIXME: It seems wrong...
		copTask.cst += copTask.count() * cpuFactor
	}
}

func (p *LogicalApply) generatePhysicalPlans() []PhysicalPlan {
	var join PhysicalPlan
	if p.JoinType == SemiJoin || p.JoinType == LeftOuterSemiJoin {
		join = p.getSemiJoin()
	} else {
		join = p.getHashJoin(1)
	}
	apply := PhysicalApply{
		PhysicalJoin:  join,
		OuterSchema:   p.corCols,
		rightChOffset: p.children[0].Schema().Len(),
	}.init(p.allocator, p.ctx)
	apply.SetSchema(p.schema)
	apply.profile = p.profile
	return []PhysicalPlan{apply}
}

func (p *baseLogicalPlan) generatePhysicalPlans() []PhysicalPlan {
	np := p.basePlan.self.(PhysicalPlan).Copy()
	return []PhysicalPlan{np}
}

func (p *basePhysicalPlan) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	p.basePlan.expectedCnt = prop.expectedCnt
	// By default, physicalPlan can always match the orders.
	props := make([]*requiredProp, 0, len(p.basePlan.children))
	for range p.basePlan.children {
		props = append(props, prop)
	}
	return [][]*requiredProp{props}
}

func (p *PhysicalHashJoin) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	p.expectedCnt = prop.expectedCnt
	if !prop.isEmpty() {
		return nil
	}
	return [][]*requiredProp{{&requiredProp{taskTp: rootTaskType, expectedCnt: prop.expectedCnt}, &requiredProp{taskTp: rootTaskType, expectedCnt: math.MaxFloat64}}}
}

func (p *PhysicalHashSemiJoin) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	p.expectedCnt = prop.expectedCnt
	lProp := &requiredProp{taskTp: rootTaskType, cols: prop.cols, expectedCnt: prop.expectedCnt, desc: prop.desc}
	for _, col := range lProp.cols {
		idx := p.Schema().ColumnIndex(col)
		if idx == -1 || idx >= p.rightChOffset {
			return nil
		}
	}
	return [][]*requiredProp{{lProp, &requiredProp{taskTp: rootTaskType, expectedCnt: math.MaxFloat64}}}
}

func (p *PhysicalApply) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	p.expectedCnt = prop.expectedCnt
	lProp := &requiredProp{taskTp: rootTaskType, cols: prop.cols, expectedCnt: prop.expectedCnt, desc: prop.desc}
	for _, col := range lProp.cols {
		idx := p.Schema().ColumnIndex(col)
		if idx == -1 || idx >= p.rightChOffset {
			return nil
		}
	}
	return [][]*requiredProp{{lProp, &requiredProp{taskTp: rootTaskType, expectedCnt: math.MaxFloat64}}}
}

func (p *Limit) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	p.expectedCnt = prop.expectedCnt
	if !prop.isEmpty() {
		return nil
	}
	props := make([][]*requiredProp, 0, len(wholeTaskTypes))
	for _, tp := range wholeTaskTypes {
		newProp := &requiredProp{taskTp: tp, expectedCnt: float64(p.Count + p.Offset)}
		if p.expectedProp != nil {
			newProp.cols = p.expectedProp.cols
			newProp.desc = p.expectedProp.desc
		}
		props = append(props, []*requiredProp{newProp})
	}
	return props
}

func (p *TopN) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	p.expectedCnt = prop.expectedCnt
	if !prop.isEmpty() {
		return nil
	}
	props := make([][]*requiredProp, 0, len(wholeTaskTypes))
	for _, tp := range wholeTaskTypes {
		props = append(props, []*requiredProp{{taskTp: tp, expectedCnt: math.MaxFloat64}})
	}
	return props
}

func (p *LogicalAggregation) getStreamAggs() []PhysicalPlan {
	if len(p.possibleProperties) == 0 {
		return nil
	}
	for _, aggFunc := range p.AggFuncs {
		if aggFunc.GetMode() == aggregation.FinalMode {
			return nil
		}
	}
	// group by a + b is not interested in any order.
	if len(p.groupByCols) != len(p.GroupByItems) {
		return nil
	}
	streamAggs := make([]PhysicalPlan, 0, len(p.possibleProperties))
	for _, cols := range p.possibleProperties {
		_, keys := getPermutation(cols, p.groupByCols)
		if len(keys) != len(p.groupByCols) {
			continue
		}
		agg := PhysicalAggregation{
			GroupByItems: p.GroupByItems,
			AggFuncs:     p.AggFuncs,
			HasGby:       len(p.GroupByItems) > 0,
			AggType:      StreamedAgg,
			propKeys:     cols,
			inputCount:   p.inputCount,
		}.init(p.allocator, p.ctx)
		agg.SetSchema(p.schema.Clone())
		agg.profile = p.profile
		streamAggs = append(streamAggs, agg)
	}
	return streamAggs
}

func (p *LogicalAggregation) generatePhysicalPlans() []PhysicalPlan {
	aggs := make([]PhysicalPlan, 0, len(p.possibleProperties)+1)
	agg := PhysicalAggregation{
		GroupByItems: p.GroupByItems,
		AggFuncs:     p.AggFuncs,
		HasGby:       len(p.GroupByItems) > 0,
		AggType:      CompleteAgg,
	}.init(p.allocator, p.ctx)
	agg.SetSchema(p.schema.Clone())
	agg.profile = p.profile
	aggs = append(aggs, agg)

	streamAggs := p.getStreamAggs()
	aggs = append(aggs, streamAggs...)

	return aggs
}

func (p *PhysicalAggregation) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	p.expectedCnt = prop.expectedCnt
	if p.AggType != StreamedAgg {
		if !prop.isEmpty() {
			return nil
		}
		props := make([][]*requiredProp, 0, len(wholeTaskTypes))
		for _, tp := range wholeTaskTypes {
			props = append(props, []*requiredProp{{taskTp: tp, expectedCnt: math.MaxFloat64}})
		}
		return props
	}

	reqProp := &requiredProp{taskTp: rootTaskType, cols: p.propKeys, expectedCnt: prop.expectedCnt * p.inputCount / p.profile.count, desc: prop.desc}
	if !prop.isEmpty() && !prop.isPrefix(reqProp) {
		return nil
	}
	return [][]*requiredProp{{reqProp}}
}
