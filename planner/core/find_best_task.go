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
	"math"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pkg/errors"
)

const (
	netWorkFactor      = 1.5
	netWorkStartFactor = 20.0
	scanFactor         = 2.0
	descScanFactor     = 2 * scanFactor
	memoryFactor       = 5.0
	// 0.5 is the looking up agg context factor.
	hashAggFactor      = 1.2 + 0.5
	selectionFactor    = 0.8
	distinctFactor     = 0.8
	cpuFactor          = 0.9
	distinctAggFactor  = 1.6
	createAggCtxFactor = 6
)

// wholeTaskTypes records all possible kinds of task that a plan can return. For Agg, TopN and Limit, we will try to get
// these tasks one by one.
var wholeTaskTypes = [...]property.TaskType{property.CopSingleReadTaskType, property.CopDoubleReadTaskType, property.RootTaskType}

var invalidTask = &rootTask{cst: math.MaxFloat64}

// getPropByOrderByItems will check if this sort property can be pushed or not. In order to simplify the problem, we only
// consider the case that all expression are columns and all of them are asc or desc.
func getPropByOrderByItems(items []*ByItems) (*property.PhysicalProperty, bool) {
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
	return &property.PhysicalProperty{Cols: cols, Desc: desc}, true
}

func (p *LogicalTableDual) findBestTask(prop *property.PhysicalProperty) (task, error) {
	if !prop.IsEmpty() {
		return invalidTask, nil
	}
	dual := PhysicalTableDual{RowCount: p.RowCount}.init(p.ctx, p.stats)
	dual.SetSchema(p.schema)
	return &rootTask{p: dual}, nil
}

// findBestTask implements LogicalPlan interface.
func (p *baseLogicalPlan) findBestTask(prop *property.PhysicalProperty) (bestTask task, err error) {
	// If p is an inner plan in an IndexJoin, the IndexJoin will generate an inner plan by itself,
	// and set inner child prop nil, so here we do nothing.
	if prop == nil {
		return nil, nil
	}
	// Look up the task with this prop in the task map.
	// It's used to reduce double counting.
	bestTask = p.getTask(prop)
	if bestTask != nil {
		return bestTask, nil
	}

	if prop.TaskTp != property.RootTaskType {
		// Currently all plan cannot totally push down.
		p.storeTask(prop, invalidTask)
		return invalidTask, nil
	}

	bestTask = invalidTask
	childTasks := make([]task, 0, len(p.children))

	// If prop.enforced is true, cols of prop as parameter in exhaustPhysicalPlans should be nil
	// And reset it for enforcing task prop and storing map<prop,task>
	oldPropCols := prop.Cols
	if prop.Enforced {
		// First, get the bestTask without enforced prop
		prop.Enforced = false
		bestTask, err = p.findBestTask(prop)
		if err != nil {
			return nil, errors.Trace(err)
		}
		prop.Enforced = true
		// Next, get the bestTask with enforced prop
		prop.Cols = []*expression.Column{}
	}
	physicalPlans := p.self.exhaustPhysicalPlans(prop)
	prop.Cols = oldPropCols

	for _, pp := range physicalPlans {
		// find best child tasks firstly.
		childTasks = childTasks[:0]
		for i, child := range p.children {
			childTask, err := child.findBestTask(pp.getChildReqProps(i))
			if err != nil {
				return nil, errors.Trace(err)
			}
			if childTask != nil && childTask.invalid() {
				break
			}
			childTasks = append(childTasks, childTask)
		}

		// This check makes sure that there is no invalid child task.
		if len(childTasks) != len(p.children) {
			continue
		}

		// combine best child tasks with parent physical plan.
		curTask := pp.attach2Task(childTasks...)

		// enforce curTask property
		if prop.Enforced {
			curTask = enforceProperty(prop, curTask, p.basePlan.ctx)
		}

		// get the most efficient one.
		if curTask.cost() < bestTask.cost() {
			bestTask = curTask
		}
	}

	p.storeTask(prop, bestTask)
	return bestTask, nil
}

// tryToGetMemTask will check if this table is a mem table. If it is, it will produce a task.
func (ds *DataSource) tryToGetMemTask(prop *property.PhysicalProperty) (task task, err error) {
	if !prop.IsEmpty() {
		return nil, nil
	}
	if !infoschema.IsMemoryDB(ds.DBName.L) {
		return nil, nil
	}

	memTable := PhysicalMemTable{
		DBName:      ds.DBName,
		Table:       ds.tableInfo,
		Columns:     ds.Columns,
		TableAsName: ds.TableAsName,
	}.init(ds.ctx, ds.stats)
	memTable.SetSchema(ds.schema)

	// Stop to push down these conditions.
	var retPlan PhysicalPlan = memTable
	if len(ds.pushedDownConds) > 0 {
		sel := PhysicalSelection{
			Conditions: ds.pushedDownConds,
		}.init(ds.ctx, ds.stats)
		sel.SetChildren(memTable)
		retPlan = sel
	}
	return &rootTask{p: retPlan}, nil
}

// tryToGetDualTask will check if the push down predicate has false constant. If so, it will return table dual.
func (ds *DataSource) tryToGetDualTask() (task, error) {
	for _, cond := range ds.pushedDownConds {
		if con, ok := cond.(*expression.Constant); ok && con.DeferredExpr == nil {
			result, err := expression.EvalBool(ds.ctx, []expression.Expression{cond}, chunk.Row{})
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !result {
				dual := PhysicalTableDual{}.init(ds.ctx, ds.stats)
				dual.SetSchema(ds.schema)
				return &rootTask{
					p: dual,
				}, nil
			}
		}
	}
	return nil, nil
}

// findBestTask implements the PhysicalPlan interface.
// It will enumerate all the available indices and choose a plan with least cost.
func (ds *DataSource) findBestTask(prop *property.PhysicalProperty) (t task, err error) {
	// If ds is an inner plan in an IndexJoin, the IndexJoin will generate an inner plan by itself,
	// and set inner child prop nil, so here we do nothing.
	if prop == nil {
		return nil, nil
	}

	t = ds.getTask(prop)
	if t != nil {
		return
	}

	// If prop.enforced is true, the prop.cols need to be set nil for ds.findBestTask.
	// Before function return, reset it for enforcing task prop and storing map<prop,task>.
	oldPropCols := prop.Cols
	if prop.Enforced {
		// First, get the bestTask without enforced prop
		prop.Enforced = false
		t, err = ds.findBestTask(prop)
		if err != nil {
			return nil, errors.Trace(err)
		}
		prop.Enforced = true
		if t != invalidTask {
			ds.storeTask(prop, t)
			return
		}
		// Next, get the bestTask with enforced prop
		prop.Cols = []*expression.Column{}
	}
	defer func() {
		if err != nil {
			return
		}
		if prop.Enforced {
			prop.Cols = oldPropCols
			t = enforceProperty(prop, t, ds.basePlan.ctx)
		}
		ds.storeTask(prop, t)
	}()

	t, err = ds.tryToGetDualTask()
	if err != nil || t != nil {
		return t, errors.Trace(err)
	}
	t, err = ds.tryToGetMemTask(prop)
	if err != nil || t != nil {
		return t, errors.Trace(err)
	}

	t = invalidTask

	for _, path := range ds.possibleAccessPaths {
		// if we already know the range of the scan is empty, just return a TableDual
		if len(path.ranges) == 0 && !ds.ctx.GetSessionVars().StmtCtx.UseCache {
			dual := PhysicalTableDual{}.init(ds.ctx, ds.stats)
			dual.SetSchema(ds.schema)
			return &rootTask{
				p: dual,
			}, nil
		}
		if path.isTablePath {
			tblTask, err := ds.convertToTableScan(prop, path)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if tblTask.cost() < t.cost() {
				t = tblTask
			}
			continue
		}
		// We will use index to generate physical plan if:
		// this path's access cond is not nil or
		// we have prop to match or
		// this index is forced to choose.
		if len(path.accessConds) > 0 || len(prop.Cols) > 0 || path.forced {
			idxTask, err := ds.convertToIndexScan(prop, path)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if idxTask.cost() < t.cost() {
				t = idxTask
			}
		}
	}
	return
}

func isCoveringIndex(columns []*model.ColumnInfo, indexColumns []*model.IndexColumn, pkIsHandle bool) bool {
	for _, colInfo := range columns {
		if pkIsHandle && mysql.HasPriKeyFlag(colInfo.Flag) {
			continue
		}
		if colInfo.ID == model.ExtraHandleID {
			continue
		}
		isIndexColumn := false
		for _, indexCol := range indexColumns {
			isFullLen := indexCol.Length == types.UnspecifiedLength || indexCol.Length == colInfo.Flen
			if colInfo.Name.L == indexCol.Name.L && isFullLen {
				isIndexColumn = true
				break
			}
		}
		if !isIndexColumn {
			return false
		}
	}
	return true
}

// If there is a table reader which needs to keep order, we should append a pk to table scan.
func (ts *PhysicalTableScan) appendExtraHandleCol(ds *DataSource) {
	if len(ds.schema.TblID2Handle) > 0 {
		return
	}
	pkInfo := model.NewExtraHandleColInfo()
	ts.Columns = append(ts.Columns, pkInfo)
	handleCol := ds.newExtraHandleSchemaCol()
	ts.schema.Append(handleCol)
	ts.schema.TblID2Handle[ds.tableInfo.ID] = []*expression.Column{handleCol}
}

// convertToIndexScan converts the DataSource to index scan with idx.
func (ds *DataSource) convertToIndexScan(prop *property.PhysicalProperty, path *accessPath) (task task, err error) {
	idx := path.index
	is := PhysicalIndexScan{
		Table:            ds.tableInfo,
		TableAsName:      ds.TableAsName,
		DBName:           ds.DBName,
		Columns:          ds.Columns,
		Index:            idx,
		IdxCols:          path.idxCols,
		IdxColLens:       path.idxColLens,
		AccessCondition:  path.accessConds,
		Ranges:           path.ranges,
		filterCondition:  path.indexFilters,
		dataSourceSchema: ds.schema,
		isPartition:      ds.isPartition,
		physicalTableID:  ds.physicalTableID,
	}.init(ds.ctx)
	statsTbl := ds.statisticTable
	if statsTbl.Indices[idx.ID] != nil {
		is.Hist = &statsTbl.Indices[idx.ID].Histogram
	}
	rowCount := path.countAfterAccess
	cop := &copTask{indexPlan: is}
	if !isCoveringIndex(is.Columns, is.Index.Columns, is.Table.PKIsHandle) {
		// If it's parent requires single read task, return max cost.
		if prop.TaskTp == property.CopSingleReadTaskType {
			return invalidTask, nil
		}
		// On this way, it's double read case.
		ts := PhysicalTableScan{
			Columns:         ds.Columns,
			Table:           is.Table,
			isPartition:     ds.isPartition,
			physicalTableID: ds.physicalTableID,
		}.init(ds.ctx)
		ts.SetSchema(ds.schema.Clone())
		cop.tablePlan = ts
	} else if prop.TaskTp == property.CopDoubleReadTaskType {
		// If it's parent requires double read task, return max cost.
		return invalidTask, nil
	}
	is.initSchema(ds.id, idx, cop.tablePlan != nil)
	// Check if this plan matches the property.
	matchProperty := false
	if !prop.IsEmpty() {
		for i, col := range idx.Columns {
			// not matched
			if col.Name.L == prop.Cols[0].ColName.L {
				matchProperty = matchIndicesProp(idx.Columns[i:], prop.Cols)
				break
			} else if i >= path.eqCondCount {
				break
			}
		}
	}
	// Only use expectedCnt when it's smaller than the count we calculated.
	// e.g. IndexScan(count1)->After Filter(count2). The `ds.stats.RowCount` is count2. count1 is the one we need to calculate
	// If expectedCnt and count2 are both zero and we go into the below `if` block, the count1 will be set to zero though it's shouldn't be.
	if (matchProperty || prop.IsEmpty()) && prop.ExpectedCnt < ds.stats.RowCount {
		selectivity := ds.stats.RowCount / path.countAfterAccess
		rowCount = math.Min(prop.ExpectedCnt/selectivity, rowCount)
	}
	is.stats = property.NewSimpleStats(rowCount)
	is.stats.UsePseudoStats = ds.statisticTable.Pseudo
	cop.cst = rowCount * scanFactor
	task = cop
	if matchProperty {
		if prop.Desc {
			is.Desc = true
			cop.cst = rowCount * descScanFactor
		}
		if cop.tablePlan != nil {
			cop.tablePlan.(*PhysicalTableScan).appendExtraHandleCol(ds)
		}
		cop.keepOrder = true
		is.KeepOrder = true
		is.addPushedDownSelection(cop, ds, prop.ExpectedCnt, path)
	} else {
		expectedCnt := math.MaxFloat64
		if prop.IsEmpty() {
			expectedCnt = prop.ExpectedCnt
		} else {
			return invalidTask, nil
		}
		is.addPushedDownSelection(cop, ds, expectedCnt, path)
	}
	if prop.TaskTp == property.RootTaskType {
		task = finishCopTask(ds.ctx, task)
	} else if _, ok := task.(*rootTask); ok {
		return invalidTask, nil
	}
	return task, nil
}

// TODO: refactor this part, we should not call Clone in fact.
func (is *PhysicalIndexScan) initSchema(id int, idx *model.IndexInfo, isDoubleRead bool) {
	indexCols := make([]*expression.Column, 0, len(idx.Columns))
	for _, col := range idx.Columns {
		colFound := is.dataSourceSchema.FindColumnByName(col.Name.L)
		if colFound == nil {
			colFound = &expression.Column{ColName: col.Name, UniqueID: is.ctx.GetSessionVars().AllocPlanColumnID()}
		} else {
			colFound = colFound.Clone().(*expression.Column)
		}
		indexCols = append(indexCols, colFound)
	}
	setHandle := false
	for _, col := range is.Columns {
		if (mysql.HasPriKeyFlag(col.Flag) && is.Table.PKIsHandle) || col.ID == model.ExtraHandleID {
			indexCols = append(indexCols, is.dataSourceSchema.FindColumnByName(col.Name.L))
			setHandle = true
			break
		}
	}
	// If it's double read case, the first index must return handle. So we should add extra handle column
	// if there isn't a handle column.
	if isDoubleRead && !setHandle {
		indexCols = append(indexCols, &expression.Column{ID: model.ExtraHandleID, ColName: model.ExtraHandleName, UniqueID: is.ctx.GetSessionVars().AllocPlanColumnID()})
	}
	is.SetSchema(expression.NewSchema(indexCols...))
}

func (is *PhysicalIndexScan) addPushedDownSelection(copTask *copTask, p *DataSource, expectedCnt float64, path *accessPath) {
	// Add filter condition to table plan now.
	indexConds, tableConds := path.indexFilters, path.tableFilters
	if indexConds != nil {
		copTask.cst += copTask.count() * cpuFactor
		count := path.countAfterAccess
		if count >= 1.0 {
			selectivity := path.countAfterIndex / path.countAfterAccess
			count = is.stats.RowCount * selectivity
		}
		stats := &property.StatsInfo{RowCount: count}
		indexSel := PhysicalSelection{Conditions: indexConds}.init(is.ctx, stats)
		indexSel.SetChildren(is)
		copTask.indexPlan = indexSel
	}
	if tableConds != nil {
		copTask.finishIndexPlan()
		copTask.cst += copTask.count() * cpuFactor
		tableSel := PhysicalSelection{Conditions: tableConds}.init(is.ctx, p.stats.ScaleByExpectCnt(expectedCnt))
		tableSel.SetChildren(copTask.tablePlan)
		copTask.tablePlan = tableSel
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

func splitIndexFilterConditions(conditions []expression.Expression, indexColumns []*model.IndexColumn,
	table *model.TableInfo) (indexConds, tableConds []expression.Expression) {
	var pkName model.CIStr
	if table.PKIsHandle {
		pkInfo := table.GetPkColInfo()
		if pkInfo != nil {
			pkName = pkInfo.Name
		}
	}
	var indexConditions, tableConditions []expression.Expression
	for _, cond := range conditions {
		if checkIndexCondition(cond, indexColumns, pkName) {
			indexConditions = append(indexConditions, cond)
		} else {
			tableConditions = append(tableConditions, cond)
		}
	}
	return indexConditions, tableConditions
}

// checkIndexCondition will check whether all columns of condition is index columns or primary key column.
func checkIndexCondition(condition expression.Expression, indexColumns []*model.IndexColumn, pkName model.CIStr) bool {
	cols := expression.ExtractColumns(condition)
	for _, col := range cols {
		if pkName.L == col.ColName.L {
			continue
		}
		isIndexColumn := false
		for _, indCol := range indexColumns {
			if col.ColName.L == indCol.Name.L && indCol.Length == types.UnspecifiedLength {
				isIndexColumn = true
				break
			}
		}
		if !isIndexColumn {
			return false
		}
	}
	return true
}

// convertToTableScan converts the DataSource to table scan.
func (ds *DataSource) convertToTableScan(prop *property.PhysicalProperty, path *accessPath) (task task, err error) {
	// It will be handled in convertToIndexScan.
	if prop.TaskTp == property.CopDoubleReadTaskType {
		return invalidTask, nil
	}

	ts := PhysicalTableScan{
		Table:           ds.tableInfo,
		Columns:         ds.Columns,
		TableAsName:     ds.TableAsName,
		DBName:          ds.DBName,
		isPartition:     ds.isPartition,
		physicalTableID: ds.physicalTableID,
	}.init(ds.ctx)
	ts.SetSchema(ds.schema)
	var pkCol *expression.Column
	if ts.Table.PKIsHandle {
		if pkColInfo := ts.Table.GetPkColInfo(); pkColInfo != nil {
			pkCol = expression.ColInfo2Col(ts.schema.Columns, pkColInfo)
			if ds.statisticTable.Columns[pkColInfo.ID] != nil {
				ts.Hist = &ds.statisticTable.Columns[pkColInfo.ID].Histogram
			}
		}
	}
	ts.Ranges = path.ranges
	ts.AccessCondition, ts.filterCondition = path.accessConds, path.tableFilters
	rowCount := path.countAfterAccess
	copTask := &copTask{
		tablePlan:         ts,
		indexPlanFinished: true,
	}
	task = copTask
	matchProperty := len(prop.Cols) == 1 && pkCol != nil && prop.Cols[0].Equal(nil, pkCol)
	// Only use expectedCnt when it's smaller than the count we calculated.
	// e.g. IndexScan(count1)->After Filter(count2). The `ds.stats.RowCount` is count2. count1 is the one we need to calculate
	// If expectedCnt and count2 are both zero and we go into the below `if` block, the count1 will be set to zero though it's shouldn't be.
	if (matchProperty || prop.IsEmpty()) && prop.ExpectedCnt < ds.stats.RowCount {
		selectivity := ds.stats.RowCount / rowCount
		rowCount = math.Min(prop.ExpectedCnt/selectivity, rowCount)
	}
	ts.stats = property.NewSimpleStats(rowCount)
	ts.stats.UsePseudoStats = ds.statisticTable.Pseudo
	copTask.cst = rowCount * scanFactor
	if matchProperty {
		if prop.Desc {
			ts.Desc = true
			copTask.cst = rowCount * descScanFactor
		}
		ts.KeepOrder = true
		copTask.keepOrder = true
		ts.addPushedDownSelection(copTask, ds.stats.ScaleByExpectCnt(prop.ExpectedCnt))
	} else {
		expectedCnt := math.MaxFloat64
		if prop.IsEmpty() {
			expectedCnt = prop.ExpectedCnt
		} else {
			return invalidTask, nil
		}
		ts.addPushedDownSelection(copTask, ds.stats.ScaleByExpectCnt(expectedCnt))
	}
	if prop.TaskTp == property.RootTaskType {
		task = finishCopTask(ds.ctx, task)
	} else if _, ok := task.(*rootTask); ok {
		return invalidTask, nil
	}
	return task, nil
}

func (ts *PhysicalTableScan) addPushedDownSelection(copTask *copTask, stats *property.StatsInfo) {
	// Add filter condition to table plan now.
	if len(ts.filterCondition) > 0 {
		copTask.cst += copTask.count() * cpuFactor
		sel := PhysicalSelection{Conditions: ts.filterCondition}.init(ts.ctx, stats)
		sel.SetChildren(ts)
		copTask.tablePlan = sel
	}
}
