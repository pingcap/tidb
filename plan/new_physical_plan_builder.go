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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

func (p *requiredProp) enforceProperty(task taskProfile, ctx context.Context, allocator *idAllocator) taskProfile {
	if p.isEmpty() {
		return task
	}
	sort := Sort{ByItems: make([]*ByItems, 0, len(p.cols))}.init(allocator, ctx)
	for _, col := range p.cols {
		sort.ByItems = append(sort.ByItems, &ByItems{col, p.desc})
	}
	sort.SetSchema(task.plan().Schema())
	return sort.attach2TaskProfile(task)
}

// getPushedProp will check if this sort property can be pushed or not.
// When a sort column will be replaced by scalar function, we refuse it.
// When a sort column will be replaced by a constant, we just remove it.
func (p *Projection) getPushedProp(prop *requiredProp) (*requiredProp, bool) {
	newProp := &requiredProp{}
	if prop.isEmpty() {
		return newProp, false
	}
	newCols := make([]*expression.Column, 0, len(prop.cols))
	for _, col := range prop.cols {
		idx := p.schema.ColumnIndex(col)
		if idx == -1 {
			return newProp, false
		}
		switch expr := p.Exprs[idx].(type) {
		case *expression.Column:
			newCols = append(newCols, expr)
		case *expression.ScalarFunction:
			return newProp, false
		}
	}
	newProp.cols = newCols
	newProp.desc = prop.desc
	return newProp, true
}

// convert2NewPhysicalPlan implements PhysicalPlan interface.
// If the Projection maps a scalar function to a sort column, it will refuse the prop.
// TODO: We can analyze the function dependence to propagate the required prop. e.g For a + 1 as b , we can take the order
// of b to a.
func (p *Projection) convert2NewPhysicalPlan(prop *requiredProp) (taskProfile, error) {
	task, err := p.getTaskProfile(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if task != nil {
		return task, nil
	}
	// enforceProperty task.
	task, err = p.children[0].(LogicalPlan).convert2NewPhysicalPlan(&requiredProp{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	task = p.attach2TaskProfile(task)
	task = prop.enforceProperty(task, p.ctx, p.allocator)

	newProp, canPassProp := p.getPushedProp(prop)
	if canPassProp {
		orderedTask, err := p.children[0].(LogicalPlan).convert2NewPhysicalPlan(newProp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		orderedTask = p.attach2TaskProfile(orderedTask)
		if orderedTask.cost() < task.cost() {
			task = orderedTask
		}
	}
	return task, p.storeTaskProfile(prop, task)
}

// convert2NewPhysicalPlan implements PhysicalPlan interface.
// Join has three physical operators: Hash Join, Merge Join and Index Look Up Join. We implement Hash Join at first.
func (p *LogicalJoin) convert2NewPhysicalPlan(prop *requiredProp) (taskProfile, error) {
	task, err := p.getTaskProfile(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if task != nil {
		return task, nil
	}
	switch p.JoinType {
	case SemiJoin, LeftOuterSemiJoin:
		task, err = p.convert2SemiJoin(prop)
	default:
		if p.preferUseMergeJoin() {
			task, err = p.convert2MergeJoin(prop)
		} else {
			// TODO: We will consider index look up join in the future.
			task, err = p.convert2HashJoin(prop)
		}
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return task, p.storeTaskProfile(prop, task)
}

func (p *LogicalJoin) preferUseMergeJoin() bool {
	return p.preferMergeJoin && len(p.EqualConditions) == 1
}

// TODO: Now we only process the case that the join has only one equal condition.
func (p *LogicalJoin) convert2MergeJoin(prop *requiredProp) (taskProfile, error) {
	lChild := p.children[0].(LogicalPlan)
	rChild := p.children[1].(LogicalPlan)
	mergeJoin := PhysicalMergeJoin{
		JoinType:        p.JoinType,
		EqualConditions: p.EqualConditions,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
	}.init(p.allocator, p.ctx)
	mergeJoin.SetSchema(p.schema)
	lJoinKey := p.EqualConditions[0].GetArgs()[0].(*expression.Column)
	lProp := &requiredProp{cols: []*expression.Column{lJoinKey}}
	lTask, err := lChild.convert2NewPhysicalPlan(lProp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rJoinKey := p.EqualConditions[0].GetArgs()[1].(*expression.Column)
	rProp := &requiredProp{cols: []*expression.Column{rJoinKey}}
	rTask, err := rChild.convert2NewPhysicalPlan(rProp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	task := mergeJoin.attach2TaskProfile(lTask, rTask)
	if prop.equal(lProp) && p.JoinType != RightOuterJoin {
		return task, nil
	}
	if prop.equal(rProp) && p.JoinType != LeftOuterJoin {
		return task, nil
	}
	task = prop.enforceProperty(task, p.ctx, p.allocator)
	return task, nil
}

func (p *LogicalJoin) convert2SemiJoin(prop *requiredProp) (taskProfile, error) {
	lChild := p.children[0].(LogicalPlan)
	rChild := p.children[1].(LogicalPlan)
	semiJoin := PhysicalHashSemiJoin{
		WithAux:         LeftOuterSemiJoin == p.JoinType,
		EqualConditions: p.EqualConditions,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		Anti:            p.anti,
	}.init(p.allocator, p.ctx)
	semiJoin.SetSchema(p.schema)
	lTask, err := lChild.convert2NewPhysicalPlan(&requiredProp{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	rTask, err := rChild.convert2NewPhysicalPlan(&requiredProp{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	task := semiJoin.attach2TaskProfile(lTask, rTask)
	// Because hash join is executed by multiple goroutines, it will not propagate physical property any more.
	// TODO: We will consider the problem of property again for parallel execution.
	task = prop.enforceProperty(task, p.ctx, p.allocator)
	return task, nil
}

func (p *LogicalJoin) convert2HashJoin(prop *requiredProp) (taskProfile, error) {
	lChild := p.children[0].(LogicalPlan)
	rChild := p.children[1].(LogicalPlan)
	hashJoin := PhysicalHashJoin{
		EqualConditions: p.EqualConditions,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		JoinType:        p.JoinType,
		Concurrency:     JoinConcurrency,
		DefaultValues:   p.DefaultValues,
	}.init(p.allocator, p.ctx)
	hashJoin.SetSchema(p.schema)
	lTask, err := lChild.convert2NewPhysicalPlan(&requiredProp{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	rTask, err := rChild.convert2NewPhysicalPlan(&requiredProp{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	switch p.JoinType {
	case LeftOuterJoin:
		hashJoin.SmallTable = 1
	case RightOuterJoin:
		hashJoin.SmallTable = 0
	case InnerJoin:
		// We will use right table as small table.
		if lTask.count() >= rTask.count() {
			hashJoin.SmallTable = 1
		}
	}
	task := hashJoin.attach2TaskProfile(lTask, rTask)
	task = prop.enforceProperty(task, p.ctx, p.allocator)
	return task, nil
}

// getPushedProp will check if this sort property can be pushed or not. In order to simplify the problem, we only
// consider the case that all expression are columns and all of them are asc or desc.
func (p *Sort) getPushedProp() (*requiredProp, bool) {
	desc := false
	cols := make([]*expression.Column, 0, len(p.ByItems))
	for i, item := range p.ByItems {
		col, ok := item.Expr.(*expression.Column)
		if !ok {
			return nil, false
		}
		cols = append(cols, col)
		desc = item.Desc
		if i > 0 && item.Desc != p.ByItems[i-1].Desc {
			return nil, false
		}
	}
	return &requiredProp{cols, desc}, true
}

// convert2NewPhysicalPlan implements PhysicalPlan interface.
// If this sort is a topN plan, we will try to push the sort down and leave the limit.
// TODO: If this is a sort plan and the coming prop is not nil, this plan is redundant and can be removed.
func (p *Sort) convert2NewPhysicalPlan(prop *requiredProp) (taskProfile, error) {
	task, err := p.getTaskProfile(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if task != nil {
		return task, nil
	}
	// enforce branch
	task, err = p.children[0].(LogicalPlan).convert2NewPhysicalPlan(&requiredProp{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	task = p.attach2TaskProfile(task)
	newProp, canPassProp := p.getPushedProp()
	if canPassProp {
		orderedTask, err := p.children[0].(LogicalPlan).convert2NewPhysicalPlan(newProp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// Leave the limit.
		if p.ExecLimit != nil {
			limit := Limit{Offset: p.ExecLimit.Offset, Count: p.ExecLimit.Count}.init(p.allocator, p.ctx)
			limit.SetSchema(p.schema)
			orderedTask = limit.attach2TaskProfile(orderedTask)
		} else if cop, ok := orderedTask.(*copTaskProfile); ok {
			orderedTask = finishCopTask(cop, p.ctx, p.allocator)
		}
		if orderedTask.cost() < task.cost() {
			task = orderedTask
		}
	}
	task = prop.enforceProperty(task, p.ctx, p.allocator)
	return task, p.storeTaskProfile(prop, task)
}

// convert2NewPhysicalPlan implements LogicalPlan interface.
func (p *baseLogicalPlan) convert2NewPhysicalPlan(prop *requiredProp) (taskProfile, error) {
	task, err := p.getTaskProfile(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if task != nil {
		return task, nil
	}
	if len(p.basePlan.children) == 0 {
		task = &rootTaskProfile{p: p.basePlan.self.(PhysicalPlan)}
	} else {
		// enforce branch
		task, err = p.basePlan.children[0].(LogicalPlan).convert2NewPhysicalPlan(&requiredProp{})
		if err != nil {
			return nil, errors.Trace(err)
		}
		task = p.basePlan.self.(PhysicalPlan).attach2TaskProfile(task)
	}
	task = prop.enforceProperty(task, p.basePlan.ctx, p.basePlan.allocator)
	if !prop.isEmpty() && len(p.basePlan.children) > 0 {
		orderedTask, err := p.basePlan.children[0].(LogicalPlan).convert2NewPhysicalPlan(prop)
		if err != nil {
			return nil, errors.Trace(err)
		}
		orderedTask = p.basePlan.self.(PhysicalPlan).attach2TaskProfile(orderedTask)
		if orderedTask.cost() < task.cost() {
			task = orderedTask
		}
	}
	return task, p.storeTaskProfile(prop, task)
}

func tryToAddUnionScan(cop *copTaskProfile, conds []expression.Expression, ctx context.Context, allocator *idAllocator) taskProfile {
	if ctx.Txn() == nil || ctx.Txn().IsReadOnly() {
		return cop
	}
	task := finishCopTask(cop, ctx, allocator)
	us := PhysicalUnionScan{
		Conditions: conds,
	}.init(allocator, ctx)
	us.SetSchema(task.plan().Schema())
	return us.attach2TaskProfile(task)
}

// tryToGetMemTask will check if this table is a mem table. If it is, it will produce a task and store it.
func (p *DataSource) tryToGetMemTask(prop *requiredProp) (task taskProfile, err error) {
	client := p.ctx.GetClient()
	memDB := infoschema.IsMemoryDB(p.DBName.L)
	isDistReq := !memDB && client != nil && client.SupportRequestType(kv.ReqTypeSelect, 0)
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
	rb := &rangeBuilder{sc: p.ctx.GetSessionVars().StmtCtx}
	memTable.Ranges = rb.buildTableRanges(fullRange)
	task = &rootTaskProfile{p: memTable}
	task = prop.enforceProperty(task, p.ctx, p.allocator)
	return task, nil
}

// tryToGetDualTask will check if the push down predicate has false constant. If so, it will return table dual.
func (p *DataSource) tryToGetDualTask() (taskProfile, error) {
	for _, cond := range p.pushedDownConds {
		if _, ok := cond.(*expression.Constant); ok {
			result, err := expression.EvalBool([]expression.Expression{cond}, nil, p.ctx)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !result {
				dual := TableDual{}.init(p.allocator, p.ctx)
				dual.SetSchema(p.schema)
				return &rootTaskProfile{
					p: dual,
				}, nil
			}
		}
	}
	return nil, nil
}

// convert2NewPhysicalPlan implements the PhysicalPlan interface.
// It will enumerate all the available indices and choose a plan with least cost.
func (p *DataSource) convert2NewPhysicalPlan(prop *requiredProp) (taskProfile, error) {
	task, err := p.getTaskProfile(prop)
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
		return task, p.storeTaskProfile(prop, task)
	}
	task, err = p.tryToGetMemTask(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if task != nil {
		return task, p.storeTaskProfile(prop, task)
	}
	// TODO: We have not checked if this table has a predicate. If not, we can only consider table scan.
	indices, includeTableScan := availableIndices(p.indexHints, p.tableInfo)
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
		if task == nil || idxTask.cost() < task.cost() {
			task = idxTask
		}
	}
	return task, p.storeTaskProfile(prop, task)
}

// convert2IndexScanner converts the DataSource to index scan with idx.
func (p *DataSource) convertToIndexScan(prop *requiredProp, idx *model.IndexInfo) (task taskProfile, err error) {
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
	if len(p.pushedDownConds) > 0 {
		conds := make([]expression.Expression, 0, len(p.pushedDownConds))
		for _, cond := range p.pushedDownConds {
			conds = append(conds, cond.Clone())
		}
		is.AccessCondition, is.filterCondition, is.accessEqualCount, is.accessInAndEqCount = DetachIndexScanConditions(conds, idx)
		is.Ranges, err = BuildIndexRange(sc, is.Table, is.Index, is.accessInAndEqCount, is.AccessCondition)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rowCount, err = statsTbl.GetRowCountByIndexRanges(sc, is.Index.ID, is.Ranges, is.accessInAndEqCount)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		rb := rangeBuilder{sc: sc}
		is.Ranges = rb.buildIndexRanges(fullRange, types.NewFieldType(mysql.TypeNull))
	}
	copTask := &copTaskProfile{
		cnt:       rowCount,
		cst:       rowCount * scanFactor,
		indexPlan: is,
	}
	if !isCoveringIndex(is.Columns, is.Index.Columns, is.Table.PKIsHandle) {
		// On this way, it's double read case.
		copTask.tablePlan = PhysicalTableScan{Columns: p.Columns, Table: is.Table}.init(p.allocator, p.ctx)
		copTask.tablePlan.SetSchema(p.schema)
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
			} else if i >= is.accessEqualCount {
				matchProperty = false
				break
			}
		}
	}
	if matchProperty && !prop.isEmpty() {
		if prop.desc {
			is.Desc = true
			copTask.cst = rowCount * descScanFactor
		}
		is.addPushedDownSelection(copTask)
		task = tryToAddUnionScan(copTask, p.pushedDownConds, p.ctx, p.allocator)
	} else {
		is.OutOfOrder = true
		is.addPushedDownSelection(copTask)
		task = tryToAddUnionScan(copTask, p.pushedDownConds, p.ctx, p.allocator)
		task = prop.enforceProperty(task, p.ctx, p.allocator)
	}
	return task, nil
}

func (is *PhysicalIndexScan) addPushedDownSelection(copTask *copTaskProfile) {
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
func (p *DataSource) convertToTableScan(prop *requiredProp) (task taskProfile, err error) {
	ts := PhysicalTableScan{
		Table:       p.tableInfo,
		Columns:     p.Columns,
		TableAsName: p.TableAsName,
		DBName:      p.DBName,
	}.init(p.allocator, p.ctx)
	ts.SetSchema(p.schema)
	sc := p.ctx.GetSessionVars().StmtCtx
	if len(p.pushedDownConds) > 0 {
		conds := make([]expression.Expression, 0, len(p.pushedDownConds))
		for _, cond := range p.pushedDownConds {
			conds = append(conds, cond.Clone())
		}
		ts.AccessCondition, ts.filterCondition = DetachTableScanConditions(conds, p.tableInfo)
		ts.Ranges, err = BuildTableRange(ts.AccessCondition, sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		ts.Ranges = []types.IntColumnRange{{math.MinInt64, math.MaxInt64}}
	}
	statsTbl := p.statisticTable
	rowCount := float64(statsTbl.Count)
	var pkCol *expression.Column
	if p.tableInfo.PKIsHandle {
		for i, colInfo := range ts.Columns {
			if mysql.HasPriKeyFlag(colInfo.Flag) {
				pkCol = p.Schema().Columns[i]
				break
			}
		}
		if pkCol != nil {
			rowCount, err = statsTbl.GetRowCountByIntColumnRanges(sc, pkCol.ID, ts.Ranges)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}
	cost := rowCount * scanFactor
	copTask := &copTaskProfile{
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
	return task, nil
}

func (p *Union) convert2NewPhysicalPlan(prop *requiredProp) (taskProfile, error) {
	task, err := p.getTaskProfile(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if task != nil {
		return task, nil
	}
	// Union is a sort blocker. We can only enforce it.
	tasks := make([]taskProfile, 0, len(p.children))
	for _, child := range p.children {
		task, err = child.(LogicalPlan).convert2NewPhysicalPlan(&requiredProp{})
		if err != nil {
			return nil, errors.Trace(err)
		}
		tasks = append(tasks, task)
	}
	task = p.attach2TaskProfile(tasks...)
	task = prop.enforceProperty(task, p.ctx, p.allocator)

	return task, p.storeTaskProfile(prop, task)
}

func (ts *PhysicalTableScan) addPushedDownSelection(copTask *copTaskProfile) {
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

func (p *LogicalAggregation) convert2NewPhysicalPlan(prop *requiredProp) (taskProfile, error) {
	task, err := p.getTaskProfile(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if task != nil {
		return task, nil
	}
	task, err = p.convert2HashAggregation(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return task, p.storeTaskProfile(prop, task)
}

func (p *LogicalAggregation) convert2HashAggregation(prop *requiredProp) (taskProfile, error) {
	task, err := p.children[0].(LogicalPlan).convert2NewPhysicalPlan(&requiredProp{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	ha := PhysicalAggregation{
		GroupByItems: p.GroupByItems,
		AggFuncs:     p.AggFuncs,
		HasGby:       len(p.GroupByItems) > 0,
		AggType:      CompleteAgg,
	}.init(p.allocator, p.ctx)
	ha.SetSchema(p.schema)
	task = ha.attach2TaskProfile(task)
	task = prop.enforceProperty(task, p.ctx, p.allocator)
	return task, nil
}

func (p *LogicalApply) convert2NewPhysicalPlan(prop *requiredProp) (taskProfile, error) {
	task, err := p.getTaskProfile(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if task != nil {
		return task, nil
	}
	// TODO: refine this code.
	if p.JoinType == SemiJoin || p.JoinType == LeftOuterSemiJoin {
		task, err = p.convert2SemiJoin(&requiredProp{})
	} else {
		task, err = p.convert2HashJoin(&requiredProp{})
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	apply := PhysicalApply{
		PhysicalJoin: task.plan(),
		OuterSchema:  p.corCols,
	}.init(p.allocator, p.ctx)
	apply.SetSchema(p.schema)
	newTask := task.(*rootTaskProfile)
	apply.children = newTask.p.Children()
	newTask.p = apply
	if err != nil {
		return nil, errors.Trace(err)
	}
	task = prop.enforceProperty(newTask, p.ctx, p.allocator)
	return task, p.storeTaskProfile(prop, task)
}
