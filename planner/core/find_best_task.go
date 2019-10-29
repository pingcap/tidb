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

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"golang.org/x/tools/container/intsets"
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
	dual := PhysicalTableDual{
		RowCount:    p.RowCount,
		placeHolder: p.placeHolder,
	}.init(p.ctx, p.stats)
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
		if _, ok := cond.(*expression.Constant); ok {
			result, _, err := expression.EvalBool(ds.ctx, []expression.Expression{cond}, chunk.Row{})
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

// candidatePath is used to maintain required info for skyline pruning.
type candidatePath struct {
	path         *accessPath
	columnSet    *intsets.Sparse // columnSet is the set of columns that occurred in the access conditions.
	isSingleScan bool
	isMatchProp  bool
}

// compareColumnSet will compares the two set. The last return value is used to indicate
// if they are comparable, it is false when both two sets have columns that do not occur in the other.
// When the second return value is true, the value of first:
// (1) -1 means that `l` is a strict subset of `r`;
// (2) 0 means that `l` equals to `r`;
// (3) 1 means that `l` is a strict superset of `r`.
func compareColumnSet(l, r *intsets.Sparse) (int, bool) {
	lLen, rLen := l.Len(), r.Len()
	if lLen < rLen {
		// -1 is meaningful only when l.SubsetOf(r) is true.
		return -1, l.SubsetOf(r)
	}
	if lLen == rLen {
		// 0 is meaningful only when l.SubsetOf(r) is true.
		return 0, l.SubsetOf(r)
	}
	// 1 is meaningful only when r.SubsetOf(l) is true.
	return 1, r.SubsetOf(l)
}

func compareBool(l, r bool) int {
	if l == r {
		return 0
	}
	if l == false {
		return -1
	}
	return 1
}

// compareCandidates is the core of skyline pruning. It compares the two candidate paths on three dimensions:
// (1): the set of columns that occurred in the access condition,
// (2): whether or not it matches the physical property
// (3): does it require a double scan.
// If `x` is not worse than `y` at all factors,
// and there exists one factor that `x` is better than `y`, then `x` is better than `y`.
func compareCandidates(lhs, rhs *candidatePath) int {
	setsResult, comparable := compareColumnSet(lhs.columnSet, rhs.columnSet)
	if !comparable {
		return 0
	}
	scanResult := compareBool(lhs.isSingleScan, rhs.isSingleScan)
	matchResult := compareBool(lhs.isMatchProp, rhs.isMatchProp)
	sum := setsResult + scanResult + matchResult
	if setsResult >= 0 && scanResult >= 0 && matchResult >= 0 && sum > 0 {
		return 1
	}
	if setsResult <= 0 && scanResult <= 0 && matchResult <= 0 && sum < 0 {
		return -1
	}
	return 0
}

func (ds *DataSource) getTableCandidate(path *accessPath, prop *property.PhysicalProperty) *candidatePath {
	candidate := &candidatePath{path: path}
	pkCol := ds.getPKIsHandleCol()
	candidate.isMatchProp = len(prop.Cols) == 1 && pkCol != nil && prop.Cols[0].Equal(nil, pkCol)
	candidate.columnSet = expression.ExtractColumnSet(path.accessConds)
	candidate.isSingleScan = true
	return candidate
}

func (ds *DataSource) getIndexCandidate(path *accessPath, prop *property.PhysicalProperty) *candidatePath {
	candidate := &candidatePath{path: path}
	// When the prop is empty, `isMatchProp` is better to be `false` because
	// it needs not to keep order for index scan.
	if !prop.IsEmpty() {
		for i, col := range path.index.Columns {
			if col.Name.L == prop.Cols[0].ColName.L {
				candidate.isMatchProp = matchIndicesProp(path.index.Columns[i:], prop.Cols)
				break
			} else if i >= path.eqCondCount {
				break
			}
		}
	}
	candidate.columnSet = expression.ExtractColumnSet(path.accessConds)
	candidate.isSingleScan = isCoveringIndex(ds.schema.Columns, path.index.Columns, ds.tableInfo.PKIsHandle)
	return candidate
}

// skylinePruning prunes access paths according to different factors. An access path can be pruned only if
// there exists a path that is not worse than it at all factors and there is at least one better factor.
func (ds *DataSource) skylinePruning(prop *property.PhysicalProperty) []*candidatePath {
	candidates := make([]*candidatePath, 0, 4)
	for _, path := range ds.possibleAccessPaths {
		// if we already know the range of the scan is empty, just return a TableDual
		if len(path.ranges) == 0 && !ds.ctx.GetSessionVars().StmtCtx.UseCache {
			return []*candidatePath{{path: path}}
		}
		var currentCandidate *candidatePath
		if path.isTablePath {
			currentCandidate = ds.getTableCandidate(path, prop)
		} else if len(path.accessConds) > 0 || !prop.IsEmpty() || path.forced {
			// We will use index to generate physical plan if:
			// this path's access cond is not nil or
			// we have prop to match or
			// this index is forced to choose.
			currentCandidate = ds.getIndexCandidate(path, prop)
		} else {
			continue
		}
		pruned := false
		for i := len(candidates) - 1; i >= 0; i-- {
			result := compareCandidates(candidates[i], currentCandidate)
			if result == 1 {
				pruned = true
				// We can break here because the current candidate cannot prune others anymore.
				break
			} else if result == -1 {
				candidates = append(candidates[:i], candidates[i+1:]...)
			}
		}
		if !pruned {
			candidates = append(candidates, currentCandidate)
		}
	}
	return candidates
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

	candidates := ds.skylinePruning(prop)
	for _, candidate := range candidates {
		path := candidate.path
		// if we already know the range of the scan is empty, just return a TableDual
		if len(path.ranges) == 0 && !ds.ctx.GetSessionVars().StmtCtx.UseCache {
			dual := PhysicalTableDual{}.init(ds.ctx, ds.stats)
			dual.SetSchema(ds.schema)
			return &rootTask{
				p: dual,
			}, nil
		}
		if path.isTablePath {
			tblTask, err := ds.convertToTableScan(prop, candidate)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if tblTask.cost() < t.cost() {
				t = tblTask
			}
			continue
		}
		idxTask, err := ds.convertToIndexScan(prop, candidate)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if idxTask.cost() < t.cost() {
			t = idxTask
		}
	}
	return
}

func isCoveringIndex(columns []*expression.Column, indexColumns []*model.IndexColumn, pkIsHandle bool) bool {
	for _, col := range columns {
		if pkIsHandle && mysql.HasPriKeyFlag(col.RetType.Flag) {
			continue
		}
		if col.ID == model.ExtraHandleID {
			continue
		}
		isIndexColumn := false
		for _, indexCol := range indexColumns {
			isFullLen := indexCol.Length == types.UnspecifiedLength || indexCol.Length == col.RetType.Flen
			// We use col.OrigColName instead of col.ColName.
			// Related issue: https://github.com/pingcap/tidb/issues/9636.
			if col.OrigColName.L == indexCol.Name.L && isFullLen {
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
func (ds *DataSource) convertToIndexScan(prop *property.PhysicalProperty, candidate *candidatePath) (task task, err error) {
	if !prop.IsEmpty() && !candidate.isMatchProp {
		return invalidTask, nil
	}
	path := candidate.path
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
	if !candidate.isSingleScan {
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
	is.initSchema(idx, cop.tablePlan != nil)
	// Only use expectedCnt when it's smaller than the count we calculated.
	// e.g. IndexScan(count1)->After Filter(count2). The `ds.stats.RowCount` is count2. count1 is the one we need to calculate
	// If expectedCnt and count2 are both zero and we go into the below `if` block, the count1 will be set to zero though it's shouldn't be.
	if (candidate.isMatchProp || prop.IsEmpty()) && prop.ExpectedCnt < ds.stats.RowCount {
		selectivity := ds.stats.RowCount / path.countAfterAccess
		rowCount = math.Min(prop.ExpectedCnt/selectivity, rowCount)
	}
	is.stats = property.NewSimpleStats(rowCount)
	is.stats.StatsVersion = ds.statisticTable.Version
	if ds.statisticTable.Pseudo {
		is.stats.StatsVersion = statistics.PseudoVersion
	}

	cop.cst = rowCount * scanFactor
	task = cop
	if candidate.isMatchProp {
		if prop.Desc {
			is.Desc = true
			cop.cst = rowCount * descScanFactor
		}
		if cop.tablePlan != nil {
			cop.tablePlan.(*PhysicalTableScan).appendExtraHandleCol(ds)
			cop.doubleReadNeedProj = true
		}
		cop.keepOrder = true
		is.KeepOrder = true
	}
	finalStats := ds.stats.ScaleByExpectCnt(prop.ExpectedCnt)
	is.addPushedDownSelection(cop, ds, path, finalStats)
	if prop.TaskTp == property.RootTaskType {
		task = finishCopTask(ds.ctx, task)
	} else if _, ok := task.(*rootTask); ok {
		return invalidTask, nil
	}
	return task, nil
}

// TODO: refactor this part, we should not call Clone in fact.
func (is *PhysicalIndexScan) initSchema( idx *model.IndexInfo, isDoubleRead bool) {
	indexCols := make([]*expression.Column, 0, len(idx.Columns))
	for _, col := range idx.Columns {
		colFound := is.dataSourceSchema.FindColumnByName(col.Name.L)
		if colFound == nil {
			colFound = &expression.Column{
				ColName:  col.Name,
				RetType:  &is.Table.Columns[col.Offset].FieldType,
				UniqueID: is.ctx.GetSessionVars().AllocPlanColumnID(),
			}
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

func (is *PhysicalIndexScan) addPushedDownSelection(copTask *copTask, p *DataSource, path *accessPath, finalStats *property.StatsInfo) {
	// Add filter condition to table plan now.
	indexConds, tableConds := path.indexFilters, path.tableFilters
	if indexConds != nil {
		copTask.cst += copTask.count() * cpuFactor
		var selectivity float64
		if path.countAfterAccess > 0 {
			selectivity = path.countAfterIndex / path.countAfterAccess
		}
		count := is.stats.RowCount * selectivity
		stats := &property.StatsInfo{RowCount: count}
		indexSel := PhysicalSelection{Conditions: indexConds}.init(is.ctx, stats)
		indexSel.SetChildren(is)
		copTask.indexPlan = indexSel
	}
	if tableConds != nil {
		copTask.finishIndexPlan()
		copTask.cst += copTask.count() * cpuFactor
		tableSel := PhysicalSelection{Conditions: tableConds}.init(is.ctx, finalStats)
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
	var indexConditions, tableConditions []expression.Expression
	for _, cond := range conditions {
		if isCoveringIndex(expression.ExtractColumns(cond), indexColumns, table.PKIsHandle) {
			indexConditions = append(indexConditions, cond)
		} else {
			tableConditions = append(tableConditions, cond)
		}
	}
	return indexConditions, tableConditions
}

// convertToTableScan converts the DataSource to table scan.
func (ds *DataSource) convertToTableScan(prop *property.PhysicalProperty, candidate *candidatePath) (task task, err error) {
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
	if ts.Table.PKIsHandle {
		if pkColInfo := ts.Table.GetPkColInfo(); pkColInfo != nil {
			if ds.statisticTable.Columns[pkColInfo.ID] != nil {
				ts.Hist = &ds.statisticTable.Columns[pkColInfo.ID].Histogram
			}
		}
	}
	path := candidate.path
	ts.Ranges = path.ranges
	ts.AccessCondition, ts.filterCondition = path.accessConds, path.tableFilters
	rowCount := path.countAfterAccess
	copTask := &copTask{
		tablePlan:         ts,
		indexPlanFinished: true,
	}
	task = copTask
	// Only use expectedCnt when it's smaller than the count we calculated.
	// e.g. IndexScan(count1)->After Filter(count2). The `ds.stats.RowCount` is count2. count1 is the one we need to calculate
	// If expectedCnt and count2 are both zero and we go into the below `if` block, the count1 will be set to zero though it's shouldn't be.
	if (candidate.isMatchProp || prop.IsEmpty()) && prop.ExpectedCnt < ds.stats.RowCount {
		selectivity := ds.stats.RowCount / rowCount
		rowCount = math.Min(prop.ExpectedCnt/selectivity, rowCount)
	}
	ts.stats = property.NewSimpleStats(rowCount)
	ts.stats.StatsVersion = ds.statisticTable.Version
	if ds.statisticTable.Pseudo {
		ts.stats.StatsVersion = statistics.PseudoVersion
	}

	copTask.cst = rowCount * scanFactor
	if candidate.isMatchProp {
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
