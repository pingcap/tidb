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

func (p *LogicalTableDual) convert2NewPhysicalPlan(prop *requiredProp) (task, error) {
	if !prop.isEmpty() {
		return invalidTask, nil
	}
	dual := PhysicalTableDual{RowCount: p.RowCount}.init(p.ctx, p.stats)
	dual.SetSchema(p.schema)
	return &rootTask{p: dual}, nil
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
	tasks := make([]task, 0, len(p.basePlan.children))
	for i, child := range p.basePlan.children {
		childTask, err := child.(LogicalPlan).convert2NewPhysicalPlan(pp.getChildReqProps(i))
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

// tryToGetMemTask will check if this table is a mem table. If it is, it will produce a task and store it.
func (p *DataSource) tryToGetMemTask(prop *requiredProp) (task task, err error) {
	if !prop.isEmpty() {
		return nil, nil
	}
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
	}.init(p.ctx)
	memTable.SetSchema(p.schema)
	memTable.Ranges = ranger.FullIntRange()
	memTable.stats = p.stats
	var retPlan PhysicalPlan = memTable
	if len(p.pushedDownConds) > 0 {
		sel := PhysicalSelection{
			Conditions: p.pushedDownConds,
		}.init(p.ctx, p.stats)
		sel.SetSchema(p.schema)
		sel.SetChildren(memTable)
		retPlan = sel
	}
	task = &rootTask{p: retPlan}
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
				dual := PhysicalTableDual{}.init(p.ctx, p.stats)
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
	indices := p.availableIndices.indices
	includeTableScan := p.availableIndices.includeTableScan
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

func (p *DataSource) forceToIndexScan(idx *model.IndexInfo, remainedConds []expression.Expression) PhysicalPlan {
	is := PhysicalIndexScan{
		Table:            p.tableInfo,
		TableAsName:      p.TableAsName,
		DBName:           p.DBName,
		Columns:          p.Columns,
		Index:            idx,
		dataSourceSchema: p.schema,
		Ranges:           ranger.FullIndexRange(),
		OutOfOrder:       true,
	}.init(p.ctx)
	is.filterCondition = remainedConds
	is.stats = p.stats
	cop := &copTask{
		indexPlan: is,
	}
	if !isCoveringIndex(is.Columns, is.Index.Columns, is.Table.PKIsHandle) {
		// On this way, it's double read case.
		cop.tablePlan = PhysicalTableScan{Columns: p.Columns, Table: is.Table}.init(p.ctx)
		cop.tablePlan.SetSchema(is.dataSourceSchema)
	}
	is.initSchema(p.id, idx, cop.tablePlan != nil)
	is.addPushedDownSelection(cop, p, math.MaxFloat64)
	t := finishCopTask(cop, p.ctx)
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
func (p *DataSource) convertToIndexScan(prop *requiredProp, idx *model.IndexInfo) (task task, err error) {
	is := PhysicalIndexScan{
		Table:            p.tableInfo,
		TableAsName:      p.TableAsName,
		DBName:           p.DBName,
		Columns:          p.Columns,
		Index:            idx,
		dataSourceSchema: p.schema,
	}.init(p.ctx)
	statsTbl := p.statisticTable
	if statsTbl.Indices[idx.ID] != nil {
		is.HistVersion = statsTbl.Indices[idx.ID].LastUpdateVersion
	}
	rowCount := float64(statsTbl.Count)
	sc := p.ctx.GetSessionVars().StmtCtx
	idxCols, colLengths := expression.IndexInfo2Cols(p.Schema().Columns, idx)
	is.Ranges = ranger.FullIndexRange()
	if len(p.pushedDownConds) > 0 {
		if len(idxCols) > 0 {
			var ranges []ranger.Range
			is.AccessCondition, is.filterCondition = ranger.DetachIndexConditions(p.pushedDownConds, idxCols, colLengths)
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
			is.filterCondition = p.pushedDownConds
		}
	}
	cop := &copTask{
		indexPlan: is,
	}
	if !isCoveringIndex(is.Columns, is.Index.Columns, is.Table.PKIsHandle) {
		// On this way, it's double read case.
		cop.tablePlan = PhysicalTableScan{Columns: p.Columns, Table: is.Table}.init(p.ctx)
		cop.tablePlan.SetSchema(p.schema.Clone())
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
	is.stats = p.stats.scaleByExpectCnt(rowCount)
	cop.cst = rowCount * scanFactor
	task = cop
	if matchProperty {
		if prop.desc {
			is.Desc = true
			cop.cst = rowCount * descScanFactor
		}
		if cop.tablePlan != nil {
			cop.tablePlan.(*PhysicalTableScan).appendExtraHandleCol(p)
		}
		cop.keepOrder = true
		is.addPushedDownSelection(cop, p, prop.expectedCnt)
	} else {
		is.OutOfOrder = true
		expectedCnt := math.MaxFloat64
		if prop.isEmpty() {
			expectedCnt = prop.expectedCnt
		} else {
			return invalidTask, nil
		}
		is.addPushedDownSelection(cop, p, expectedCnt)
	}
	if prop.taskTp == rootTaskType {
		task = finishCopTask(task, p.ctx)
	} else if _, ok := task.(*rootTask); ok {
		return invalidTask, nil
	}
	return task, nil
}

// TODO: refine this.
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
			indexConds, tableConds = splitIndexFilterConditions(is.filterCondition, is.Index.Columns, is.Table)
		} else {
			indexConds = is.filterCondition
		}
		if indexConds != nil {
			indexSel := PhysicalSelection{Conditions: indexConds}.init(is.ctx,
				p.getStatsByFilter(append(is.AccessCondition, indexConds...)).scaleByExpectCnt(expectedCnt))
			indexSel.SetSchema(is.schema)
			indexSel.SetChildren(is)
			copTask.indexPlan = indexSel
			copTask.cst += copTask.count() * cpuFactor
		}
		if tableConds != nil {
			copTask.finishIndexPlan()
			tableSel := PhysicalSelection{Conditions: tableConds}.init(is.ctx, p.stats.scaleByExpectCnt(expectedCnt))
			tableSel.SetSchema(copTask.tablePlan.Schema())
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
		for _, colInfo := range table.Columns {
			if mysql.HasPriKeyFlag(colInfo.Flag) {
				pkName = colInfo.Name
				break
			}
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

func (p *DataSource) forceToTableScan() PhysicalPlan {
	ts := PhysicalTableScan{
		Table:       p.tableInfo,
		Columns:     p.Columns,
		TableAsName: p.TableAsName,
		DBName:      p.DBName,
		Ranges:      ranger.FullIntRange(),
	}.init(p.ctx)
	ts.SetSchema(p.schema)
	ts.stats = p.stats
	ts.filterCondition = p.pushedDownConds
	copTask := &copTask{
		tablePlan:         ts,
		indexPlanFinished: true,
	}
	ts.addPushedDownSelection(copTask, p.stats)
	t := finishCopTask(copTask, p.ctx)
	return t.plan()
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
	}.init(p.ctx)
	ts.SetSchema(p.schema)
	sc := p.ctx.GetSessionVars().StmtCtx
	ts.Ranges = ranger.FullIntRange()
	var pkCol *expression.Column
	if ts.Table.PKIsHandle {
		if pkColInfo := ts.Table.GetPkColInfo(); pkColInfo != nil {
			pkCol = expression.ColInfo2Col(ts.schema.Columns, pkColInfo)
			if p.statisticTable.Columns[pkColInfo.ID] != nil {
				ts.HistVersion = p.statisticTable.Columns[pkColInfo.ID].LastUpdateVersion
			}
		}
	}
	if len(p.pushedDownConds) > 0 {
		if pkCol != nil {
			var ranges []ranger.Range
			ts.AccessCondition, ts.filterCondition = ranger.DetachCondsForTableRange(p.ctx, p.pushedDownConds, pkCol)
			ranges, err = ranger.BuildRange(sc, ts.AccessCondition, ranger.IntRangeType, []*expression.Column{pkCol}, nil)
			ts.Ranges = ranger.Ranges2IntRanges(ranges)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			ts.filterCondition = p.pushedDownConds
		}
	}
	statsTbl := p.statisticTable
	rowCount := float64(statsTbl.Count)
	if pkCol != nil {
		// TODO: We can use p.getStatsByFilter(accessConditions).
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
	ts.stats = p.stats.scaleByExpectCnt(rowCount)
	copTask.cst = rowCount * scanFactor
	if matchProperty {
		if prop.desc {
			ts.Desc = true
			copTask.cst = rowCount * descScanFactor
		}
		ts.KeepOrder = true
		copTask.keepOrder = true
		ts.addPushedDownSelection(copTask, p.stats.scaleByExpectCnt(prop.expectedCnt))
	} else {
		expectedCnt := math.MaxFloat64
		if prop.isEmpty() {
			expectedCnt = prop.expectedCnt
		} else {
			return invalidTask, nil
		}
		ts.addPushedDownSelection(copTask, p.stats.scaleByExpectCnt(expectedCnt))
	}
	if prop.taskTp == rootTaskType {
		task = finishCopTask(task, p.ctx)
	} else if _, ok := task.(*rootTask); ok {
		return invalidTask, nil
	}
	return task, nil
}

func (ts *PhysicalTableScan) addPushedDownSelection(copTask *copTask, stats *statsInfo) {
	// Add filter condition to table plan now.
	if len(ts.filterCondition) > 0 {
		sel := PhysicalSelection{Conditions: ts.filterCondition}.init(ts.ctx, stats)
		sel.SetSchema(ts.schema)
		sel.SetChildren(ts)
		copTask.tablePlan = sel
		// FIXME: It seems wrong...
		copTask.cst += copTask.count() * cpuFactor
	}
}
