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
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/ranger"
	log "github.com/sirupsen/logrus"
)

const (
	netWorkFactor      = 1.5
	netWorkStartFactor = 20.0
	scanFactor         = 2.0
	descScanFactor     = 5 * scanFactor
	memoryFactor       = 5.0
	hashAggMemFactor   = 2.0
	selectionFactor    = 0.8
	distinctFactor     = 0.8
	cpuFactor          = 0.9
)

// JoinConcurrency means the number of goroutines that participate in joining.
var JoinConcurrency = 5

// wholeTaskTypes records all possible kinds of task that a plan can return. For Agg, TopN and Limit, we will try to get
// these tasks one by one.
var wholeTaskTypes = [...]taskType{copSingleReadTaskType, copDoubleReadTaskType, rootTaskType}

var invalidTask = &rootTask{cst: math.MaxFloat64}

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

func (p *LogicalTableDual) convert2PhysicalPlan(prop *requiredProp) (task, error) {
	if !prop.isEmpty() {
		return invalidTask, nil
	}
	dual := PhysicalTableDual{RowCount: p.RowCount}.init(p.ctx, p.stats)
	dual.SetSchema(p.schema)
	return &rootTask{p: dual}, nil
}

// convert2PhysicalPlan implements LogicalPlan interface.
func (p *baseLogicalPlan) convert2PhysicalPlan(prop *requiredProp) (t task, err error) {
	// Look up the task with this prop in the task map.
	// It's used to reduce double counting.
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
	for _, pp := range p.self.genPhysPlansByReqProp(prop) {
		t, err = p.getBestTask(t, pp)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	p.storeTask(prop, t)
	return t, nil
}

func (p *baseLogicalPlan) getBestTask(bestTask task, pp PhysicalPlan) (task, error) {
	tasks := make([]task, 0, len(p.children))
	for i, child := range p.children {
		childTask, err := child.convert2PhysicalPlan(pp.getChildReqProps(i))
		if err != nil {
			return nil, errors.Trace(err)
		}
		tasks = append(tasks, childTask)
	}
	resultTask := pp.attach2Task(tasks...)
	if resultTask.cost() < bestTask.cost() {
		bestTask = resultTask
	}
	return bestTask, nil
}

// tryToGetMemTask will check if this table is a mem table. If it is, it will produce a task.
func (ds *DataSource) tryToGetMemTask(prop *requiredProp) (task task, err error) {
	if !prop.isEmpty() {
		return nil, nil
	}
	client := ds.ctx.GetClient()
	memDB := infoschema.IsMemoryDB(ds.DBName.L)
	isDistReq := !memDB && client != nil && client.IsRequestTypeSupported(kv.ReqTypeSelect, 0)
	if isDistReq {
		return nil, nil
	}

	memTable := PhysicalMemTable{
		DBName:      ds.DBName,
		Table:       ds.tableInfo,
		Columns:     ds.Columns,
		TableAsName: ds.TableAsName,
	}.init(ds.ctx, ds.stats)
	memTable.SetSchema(ds.schema)
	memTable.Ranges = ranger.FullIntRange()

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
			result, err := expression.EvalBool([]expression.Expression{cond}, nil, ds.ctx)
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

// convert2PhysicalPlan implements the PhysicalPlan interface.
// It will enumerate all the available indices and choose a plan with least cost.
func (ds *DataSource) convert2PhysicalPlan(prop *requiredProp) (task, error) {
	// If ds is an inner plan in an IndexJoin, the IndexJoin will generate an inner plan by itself.
	// So here we do nothing.
	// TODO: Add a special prop to handle IndexJoin's inner plan.
	// Then we can remove forceToTableScan and forceToIndexScan.
	if prop == nil {
		return nil, nil
	}

	t := ds.getTask(prop)
	if t != nil {
		return t, nil
	}
	t, err := ds.tryToGetDualTask()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if t != nil {
		ds.storeTask(prop, t)
		return t, nil
	}
	t, err = ds.tryToGetMemTask(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if t != nil {
		ds.storeTask(prop, t)
		return t, nil
	}

	// TODO: We have not checked if this table has a predicate. If not, we can only consider table scan.
	indices := ds.availableIndices.indices
	includeTableScan := ds.availableIndices.includeTableScan
	t = invalidTask
	if includeTableScan {
		t, err = ds.convertToTableScan(prop)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if !includeTableScan || len(ds.pushedDownConds) > 0 || len(prop.cols) > 0 {
		for _, idx := range indices {
			idxTask, err := ds.convertToIndexScan(prop, idx)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if idxTask.cost() < t.cost() {
				t = idxTask
			}
		}
	}
	ds.storeTask(prop, t)
	return t, nil
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

func (ds *DataSource) forceToIndexScan(idx *model.IndexInfo, remainedConds []expression.Expression) PhysicalPlan {
	is := PhysicalIndexScan{
		Table:            ds.tableInfo,
		TableAsName:      ds.TableAsName,
		DBName:           ds.DBName,
		Columns:          ds.Columns,
		Index:            idx,
		dataSourceSchema: ds.schema,
		Ranges:           ranger.FullNewRange(),
		KeepOrder:        false,
	}.init(ds.ctx)
	is.filterCondition = remainedConds
	is.stats = ds.stats
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
	is.addPushedDownSelection(cop, ds, math.MaxFloat64)
	t := finishCopTask(cop, ds.ctx)
	return t.plan()
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
func (ds *DataSource) convertToIndexScan(prop *requiredProp, idx *model.IndexInfo) (task task, err error) {
	is := PhysicalIndexScan{
		Table:            ds.tableInfo,
		TableAsName:      ds.TableAsName,
		DBName:           ds.DBName,
		Columns:          ds.Columns,
		Index:            idx,
		dataSourceSchema: ds.schema,
	}.init(ds.ctx)
	statsTbl := ds.statisticTable
	if statsTbl.Indices[idx.ID] != nil {
		is.HistVersion = statsTbl.Indices[idx.ID].LastUpdateVersion
	}
	rowCount := float64(statsTbl.Count)
	sc := ds.ctx.GetSessionVars().StmtCtx
	idxCols, colLengths := expression.IndexInfo2Cols(ds.Schema().Columns, idx)
	is.Ranges = ranger.FullNewRange()
	if len(ds.pushedDownConds) > 0 {
		if len(idxCols) > 0 {
			is.AccessCondition, is.filterCondition = ranger.DetachIndexConditions(ds.pushedDownConds, idxCols, colLengths)
			is.Ranges, err = ranger.BuildIndexRange(sc, idxCols, colLengths, is.AccessCondition)
			if err != nil {
				return nil, errors.Trace(err)
			}
			rowCount, err = statsTbl.GetRowCountByIndexRanges(sc, is.Index.ID, is.Ranges)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			is.filterCondition = ds.pushedDownConds
		}
	}
	cop := &copTask{indexPlan: is}
	if !isCoveringIndex(is.Columns, is.Index.Columns, is.Table.PKIsHandle) {
		// On this way, it's double read case.
		ts := PhysicalTableScan{Columns: ds.Columns, Table: is.Table}.init(ds.ctx)
		ts.SetSchema(ds.schema.Clone())
		cop.tablePlan = ts
		// If it's parent requires single read task, return max cost.
		if prop.taskTp == copSingleReadTaskType {
			return &copTask{cst: math.MaxFloat64}, nil
		}
	} else if prop.taskTp == copDoubleReadTaskType {
		// If it's parent requires double read task, return max cost.
		return &copTask{cst: math.MaxFloat64}, nil
	}
	is.initSchema(ds.id, idx, cop.tablePlan != nil)
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
		selectivity, err := statsTbl.Selectivity(ds.ctx, is.filterCondition)
		if err != nil {
			log.Warnf("An error happened: %v, we have to use the default selectivity", err.Error())
			selectivity = selectionFactor
		}
		rowCount = math.Min(prop.expectedCnt/selectivity, rowCount)
	}
	is.stats = ds.stats.scaleByExpectCnt(rowCount)
	cop.cst = rowCount * scanFactor
	task = cop
	if matchProperty {
		if prop.desc {
			is.Desc = true
			cop.cst = rowCount * descScanFactor
		}
		if cop.tablePlan != nil {
			cop.tablePlan.(*PhysicalTableScan).appendExtraHandleCol(ds)
		}
		cop.keepOrder = true
		is.KeepOrder = true
		is.addPushedDownSelection(cop, ds, prop.expectedCnt)
	} else {
		expectedCnt := math.MaxFloat64
		if prop.isEmpty() {
			expectedCnt = prop.expectedCnt
		} else {
			return invalidTask, nil
		}
		is.addPushedDownSelection(cop, ds, expectedCnt)
	}
	if prop.taskTp == rootTaskType {
		task = finishCopTask(task, ds.ctx)
	} else if _, ok := task.(*rootTask); ok {
		return invalidTask, nil
	}
	return task, nil
}

func (is *PhysicalIndexScan) initSchema(id int, idx *model.IndexInfo, isDoubleRead bool) {
	indexCols := make([]*expression.Column, 0, len(idx.Columns))
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
			indexConds, tableConds = splitIndexFilterConditions(is.filterCondition, is.Index.Columns, is.Table)
		} else {
			indexConds = is.filterCondition
		}
		if indexConds != nil {
			indexSel := PhysicalSelection{Conditions: indexConds}.init(is.ctx,
				p.getStatsByFilter(append(is.AccessCondition, indexConds...)).scaleByExpectCnt(expectedCnt))
			indexSel.SetChildren(is)
			copTask.indexPlan = indexSel
			copTask.cst += copTask.count() * cpuFactor
		}
		if tableConds != nil {
			copTask.finishIndexPlan()
			tableSel := PhysicalSelection{Conditions: tableConds}.init(is.ctx, p.stats.scaleByExpectCnt(expectedCnt))
			tableSel.SetChildren(copTask.tablePlan)
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

func (ds *DataSource) forceToTableScan(pk *expression.Column) PhysicalPlan {
	var ranges []*ranger.NewRange
	if pk != nil {
		ranges = ranger.FullIntNewRange(mysql.HasUnsignedFlag(pk.RetType.Flag))
	} else {
		ranges = ranger.FullIntNewRange(false)
	}
	ts := PhysicalTableScan{
		Table:       ds.tableInfo,
		Columns:     ds.Columns,
		TableAsName: ds.TableAsName,
		DBName:      ds.DBName,
		Ranges:      ranges,
	}.init(ds.ctx)
	ts.SetSchema(ds.schema)
	ts.stats = ds.stats
	ts.filterCondition = ds.pushedDownConds
	copTask := &copTask{
		tablePlan:         ts,
		indexPlanFinished: true,
	}
	ts.addPushedDownSelection(copTask, ds.stats)
	t := finishCopTask(copTask, ds.ctx)
	return t.plan()
}

// convertToTableScan converts the DataSource to table scan.
func (ds *DataSource) convertToTableScan(prop *requiredProp) (task task, err error) {
	// It will be handled in convertToIndexScan.
	if prop.taskTp == copDoubleReadTaskType {
		return &copTask{cst: math.MaxFloat64}, nil
	}

	ts := PhysicalTableScan{
		Table:       ds.tableInfo,
		Columns:     ds.Columns,
		TableAsName: ds.TableAsName,
		DBName:      ds.DBName,
	}.init(ds.ctx)
	ts.SetSchema(ds.schema)
	sc := ds.ctx.GetSessionVars().StmtCtx
	var pkCol *expression.Column
	if ts.Table.PKIsHandle {
		if pkColInfo := ts.Table.GetPkColInfo(); pkColInfo != nil {
			pkCol = expression.ColInfo2Col(ts.schema.Columns, pkColInfo)
			if ds.statisticTable.Columns[pkColInfo.ID] != nil {
				ts.HistVersion = ds.statisticTable.Columns[pkColInfo.ID].LastUpdateVersion
			}
		}
	}
	if pkCol != nil {
		ts.Ranges = ranger.FullIntNewRange(mysql.HasUnsignedFlag(pkCol.RetType.Flag))
	} else {
		ts.Ranges = ranger.FullIntNewRange(false)
	}
	statsTbl := ds.statisticTable
	rowCount := float64(statsTbl.Count)
	if len(ds.pushedDownConds) > 0 {
		if pkCol != nil {
			ts.AccessCondition, ts.filterCondition = ranger.DetachCondsForTableRange(ds.ctx, ds.pushedDownConds, pkCol)
			ts.Ranges, err = ranger.BuildTableRange(ts.AccessCondition, sc, pkCol.RetType)
			if err != nil {
				return nil, errors.Trace(err)
			}
			// TODO: We can use ds.getStatsByFilter(accessConditions).
			rowCount, err = statsTbl.GetRowCountByIntColumnRanges(sc, pkCol.ID, ts.Ranges)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			ts.filterCondition = ds.pushedDownConds
		}
	}
	copTask := &copTask{
		tablePlan:         ts,
		indexPlanFinished: true,
	}
	task = copTask
	matchProperty := len(prop.cols) == 1 && pkCol != nil && prop.cols[0].Equal(pkCol, nil)
	if matchProperty && prop.expectedCnt < math.MaxFloat64 {
		selectivity, err := statsTbl.Selectivity(ds.ctx, ts.filterCondition)
		if err != nil {
			log.Warnf("An error happened: %v, we have to use the default selectivity", err.Error())
			selectivity = selectionFactor
		}
		rowCount = math.Min(prop.expectedCnt/selectivity, rowCount)
	}
	ts.stats = ds.stats.scaleByExpectCnt(rowCount)
	copTask.cst = rowCount * scanFactor
	if matchProperty {
		if prop.desc {
			ts.Desc = true
			copTask.cst = rowCount * descScanFactor
		}
		ts.KeepOrder = true
		copTask.keepOrder = true
		ts.addPushedDownSelection(copTask, ds.stats.scaleByExpectCnt(prop.expectedCnt))
	} else {
		expectedCnt := math.MaxFloat64
		if prop.isEmpty() {
			expectedCnt = prop.expectedCnt
		} else {
			return invalidTask, nil
		}
		ts.addPushedDownSelection(copTask, ds.stats.scaleByExpectCnt(expectedCnt))
	}
	if prop.taskTp == rootTaskType {
		task = finishCopTask(task, ds.ctx)
	} else if _, ok := task.(*rootTask); ok {
		return invalidTask, nil
	}
	return task, nil
}

func (ts *PhysicalTableScan) addPushedDownSelection(copTask *copTask, stats *statsInfo) {
	// Add filter condition to table plan now.
	if len(ts.filterCondition) > 0 {
		sel := PhysicalSelection{Conditions: ts.filterCondition}.init(ts.ctx, stats)
		sel.SetChildren(ts)
		copTask.tablePlan = sel
		// FIXME: It seems wrong...
		copTask.cst += copTask.count() * cpuFactor
	}
}
