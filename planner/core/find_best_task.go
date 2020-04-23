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

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/set"
	"go.uber.org/zap"
	"golang.org/x/tools/container/intsets"
)

const (
	// SelectionFactor is the default factor of the selectivity.
	// For example, If we have no idea how to estimate the selectivity
	// of a Selection or a JoinCondition, we can use this default value.
	SelectionFactor = 0.8
	distinctFactor  = 0.8
)

var aggFuncFactor = map[string]float64{
	ast.AggFuncCount:       1.0,
	ast.AggFuncSum:         1.0,
	ast.AggFuncAvg:         2.0,
	ast.AggFuncFirstRow:    0.1,
	ast.AggFuncMax:         1.0,
	ast.AggFuncMin:         1.0,
	ast.AggFuncGroupConcat: 1.0,
	ast.AggFuncBitOr:       0.9,
	ast.AggFuncBitXor:      0.9,
	ast.AggFuncBitAnd:      0.9,
	ast.AggFuncVarPop:      3.0,
	ast.AggFuncVarSamp:     3.0,
	ast.AggFuncStddevPop:   3.0,
	ast.AggFuncStddevSamp:  3.0,
	"default":              1.5,
}

// wholeTaskTypes records all possible kinds of task that a plan can return. For Agg, TopN and Limit, we will try to get
// these tasks one by one.
var wholeTaskTypes = [...]property.TaskType{property.CopSingleReadTaskType, property.CopDoubleReadTaskType, property.RootTaskType}

var invalidTask = &rootTask{cst: math.MaxFloat64}

// GetPropByOrderByItems will check if this sort property can be pushed or not. In order to simplify the problem, we only
// consider the case that all expression are columns.
func GetPropByOrderByItems(items []*ByItems) (*property.PhysicalProperty, bool) {
	propItems := make([]property.Item, 0, len(items))
	for _, item := range items {
		col, ok := item.Expr.(*expression.Column)
		if !ok {
			return nil, false
		}
		propItems = append(propItems, property.Item{Col: col, Desc: item.Desc})
	}
	return &property.PhysicalProperty{Items: propItems}, true
}

// GetPropByOrderByItemsContainScalarFunc will check if this sort property can be pushed or not. In order to simplify the
// problem, we only consider the case that all expression are columns or some special scalar functions.
func GetPropByOrderByItemsContainScalarFunc(items []*ByItems) (*property.PhysicalProperty, bool, bool) {
	propItems := make([]property.Item, 0, len(items))
	onlyColumn := true
	for _, item := range items {
		switch expr := item.Expr.(type) {
		case *expression.Column:
			propItems = append(propItems, property.Item{Col: expr, Desc: item.Desc})
		case *expression.ScalarFunction:
			col, desc := expr.GetSingleColumn(item.Desc)
			if col == nil {
				return nil, false, false
			}
			propItems = append(propItems, property.Item{Col: col, Desc: desc})
			onlyColumn = false
		default:
			return nil, false, false
		}
	}
	return &property.PhysicalProperty{Items: propItems}, true, onlyColumn
}

func (p *LogicalTableDual) findBestTask(prop *property.PhysicalProperty) (task, error) {
	if !prop.IsEmpty() {
		return invalidTask, nil
	}
	dual := PhysicalTableDual{
		RowCount: p.RowCount,
	}.Init(p.ctx, p.stats, p.blockOffset)
	dual.SetSchema(p.schema)
	return &rootTask{p: dual}, nil
}

func (p *LogicalShow) findBestTask(prop *property.PhysicalProperty) (task, error) {
	if !prop.IsEmpty() {
		return invalidTask, nil
	}
	pShow := PhysicalShow{ShowContents: p.ShowContents}.Init(p.ctx)
	pShow.SetSchema(p.schema)
	return &rootTask{p: pShow}, nil
}

func (p *LogicalShowDDLJobs) findBestTask(prop *property.PhysicalProperty) (task, error) {
	if !prop.IsEmpty() {
		return invalidTask, nil
	}
	pShow := PhysicalShowDDLJobs{JobNumber: p.JobNumber}.Init(p.ctx)
	pShow.SetSchema(p.schema)
	return &rootTask{p: pShow}, nil
}

func (p *baseLogicalPlan) enumeratePhysicalPlans4Task(physicalPlans []PhysicalPlan, prop *property.PhysicalProperty) (task, error) {
	var bestTask task = invalidTask
	childTasks := make([]task, 0, len(p.children))
	for _, pp := range physicalPlans {
		// find best child tasks firstly.
		childTasks = childTasks[:0]
		for i, child := range p.children {
			childTask, err := child.findBestTask(pp.GetChildReqProps(i))
			if err != nil {
				return nil, err
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

		// optimize by shuffle executor to running in parallel manner.
		if prop.IsEmpty() {
			curTask = optimizeByShuffle(pp, curTask, p.basePlan.ctx)
		}

		// get the most efficient one.
		if curTask.cost() < bestTask.cost() || (bestTask.invalid() && !curTask.invalid()) {
			bestTask = curTask
		}
	}
	return bestTask, nil
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
	// prop should be read only because its cached hashcode might be not consistent
	// when it is changed. So we clone a new one for the temporary changes.
	newProp := prop.Clone()
	newProp.Enforced = prop.Enforced
	var plansFitsProp, plansNeedEnforce []PhysicalPlan
	var hintWorksWithProp bool
	// Maybe the plan can satisfy the required property,
	// so we try to get the task without the enforced sort first.
	plansFitsProp, hintWorksWithProp = p.self.exhaustPhysicalPlans(newProp)
	if !hintWorksWithProp && !newProp.IsEmpty() {
		// If there is a hint in the plan and the hint cannot satisfy the property,
		// we enforce this property and try to generate the PhysicalPlan again to
		// make sure the hint can work.
		newProp.Enforced = true
	}

	if newProp.Enforced {
		// Then, we use the empty property to get physicalPlans and
		// try to get the task with an enforced sort.
		newProp.Items = []property.Item{}
		newProp.ExpectedCnt = math.MaxFloat64
		var hintCanWork bool
		plansNeedEnforce, hintCanWork = p.self.exhaustPhysicalPlans(newProp)
		if hintCanWork && !hintWorksWithProp {
			// If the hint can work with the empty property, but cannot work with
			// the required property, we give up `plansFitProp` to make sure the hint
			// can work.
			plansFitsProp = nil
		}
		if !hintCanWork && !hintWorksWithProp && !prop.Enforced {
			// If the original property is not enforced and hint cannot
			// work anyway, we give up `plansNeedEnforce` for efficiency,
			plansNeedEnforce = nil
		}
		newProp.Items = prop.Items
		newProp.ExpectedCnt = prop.ExpectedCnt
	}

	newProp.Enforced = false
	if bestTask, err = p.enumeratePhysicalPlans4Task(plansFitsProp, newProp); err != nil {
		return nil, err
	}
	newProp.Enforced = true
	curTask, err := p.enumeratePhysicalPlans4Task(plansNeedEnforce, newProp)
	if err != nil {
		return nil, err
	}
	if curTask.cost() < bestTask.cost() || (bestTask.invalid() && !curTask.invalid()) {
		bestTask = curTask
	}

	p.storeTask(prop, bestTask)
	return bestTask, nil
}

func (p *LogicalMemTable) findBestTask(prop *property.PhysicalProperty) (t task, err error) {
	if !prop.IsEmpty() {
		return invalidTask, nil
	}
	memTable := PhysicalMemTable{
		DBName:         p.DBName,
		Table:          p.TableInfo,
		Columns:        p.TableInfo.Columns,
		Extractor:      p.Extractor,
		QueryTimeRange: p.QueryTimeRange,
	}.Init(p.ctx, p.stats, p.blockOffset)
	memTable.SetSchema(p.schema)
	return &rootTask{p: memTable}, nil
}

// tryToGetDualTask will check if the push down predicate has false constant. If so, it will return table dual.
func (ds *DataSource) tryToGetDualTask() (task, error) {
	for _, cond := range ds.pushedDownConds {
		if con, ok := cond.(*expression.Constant); ok && con.DeferredExpr == nil && con.ParamMarker == nil {
			result, _, err := expression.EvalBool(ds.ctx, []expression.Expression{cond}, chunk.Row{})
			if err != nil {
				return nil, err
			}
			if !result {
				dual := PhysicalTableDual{}.Init(ds.ctx, ds.stats, ds.blockOffset)
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
	path         *util.AccessPath
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
	if !l {
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

func (ds *DataSource) getTableCandidate(path *util.AccessPath, prop *property.PhysicalProperty) *candidatePath {
	candidate := &candidatePath{path: path}
	pkCol := ds.getPKIsHandleCol()
	if len(prop.Items) == 1 && pkCol != nil {
		candidate.isMatchProp = prop.Items[0].Col.Equal(nil, pkCol)
		if path.StoreType == kv.TiFlash {
			candidate.isMatchProp = candidate.isMatchProp && !prop.Items[0].Desc
		}
	}
	candidate.columnSet = expression.ExtractColumnSet(path.AccessConds)
	candidate.isSingleScan = true
	return candidate
}

func (ds *DataSource) getIndexCandidate(path *util.AccessPath, prop *property.PhysicalProperty, isSingleScan bool) *candidatePath {
	candidate := &candidatePath{path: path}
	all, _ := prop.AllSameOrder()
	// When the prop is empty or `all` is false, `isMatchProp` is better to be `false` because
	// it needs not to keep order for index scan.
	if !prop.IsEmpty() && all {
		for i, col := range path.IdxCols {
			if col.Equal(nil, prop.Items[0].Col) {
				candidate.isMatchProp = matchIndicesProp(path.IdxCols[i:], path.IdxColLens[i:], prop.Items)
				break
			} else if i >= path.EqCondCount {
				break
			}
		}
	}
	candidate.columnSet = expression.ExtractColumnSet(path.AccessConds)
	candidate.isSingleScan = isSingleScan
	return candidate
}

func (ds *DataSource) getIndexMergeCandidate(path *util.AccessPath) *candidatePath {
	candidate := &candidatePath{path: path}
	return candidate
}

// skylinePruning prunes access paths according to different factors. An access path can be pruned only if
// there exists a path that is not worse than it at all factors and there is at least one better factor.
func (ds *DataSource) skylinePruning(prop *property.PhysicalProperty) []*candidatePath {
	candidates := make([]*candidatePath, 0, 4)
	for _, path := range ds.possibleAccessPaths {
		if path.PartialIndexPaths != nil {
			candidates = append(candidates, ds.getIndexMergeCandidate(path))
			continue
		}
		// if we already know the range of the scan is empty, just return a TableDual
		if len(path.Ranges) == 0 && !ds.ctx.GetSessionVars().StmtCtx.UseCache {
			return []*candidatePath{{path: path}}
		}
		var currentCandidate *candidatePath
		if path.IsTablePath {
			currentCandidate = ds.getTableCandidate(path, prop)
		} else {
			coveredByIdx := isCoveringIndex(ds.schema.Columns, path.FullIdxCols, path.FullIdxColLens, ds.tableInfo.PKIsHandle)
			if len(path.AccessConds) > 0 || !prop.IsEmpty() || path.Forced || coveredByIdx {
				// We will use index to generate physical plan if any of the following conditions is satisfied:
				// 1. This path's access cond is not nil.
				// 2. We have a non-empty prop to match.
				// 3. This index is forced to choose.
				// 4. The needed columns are all covered by index columns(and handleCol).
				currentCandidate = ds.getIndexCandidate(path, prop, coveredByIdx)
			} else {
				continue
			}
		}
		pruned := false
		for i := len(candidates) - 1; i >= 0; i-- {
			if candidates[i].path.StoreType == kv.TiFlash {
				continue
			}
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
	oldPropCols := prop.Items
	if prop.Enforced {
		// First, get the bestTask without enforced prop
		prop.Enforced = false
		t, err = ds.findBestTask(prop)
		if err != nil {
			return nil, err
		}
		prop.Enforced = true
		if t != invalidTask {
			ds.storeTask(prop, t)
			return
		}
		// Next, get the bestTask with enforced prop
		prop.Items = []property.Item{}
	}
	defer func() {
		if err != nil {
			return
		}
		if prop.Enforced {
			prop.Items = oldPropCols
			t = enforceProperty(prop, t, ds.basePlan.ctx)
		}
		ds.storeTask(prop, t)
	}()

	t, err = ds.tryToGetDualTask()
	if err != nil || t != nil {
		return t, err
	}

	t = invalidTask
	candidates := ds.skylinePruning(prop)

	for _, candidate := range candidates {
		path := candidate.path
		if path.PartialIndexPaths != nil {
			idxMergeTask, err := ds.convertToIndexMergeScan(prop, candidate)
			if err != nil {
				return nil, err
			}
			if idxMergeTask.cost() < t.cost() {
				t = idxMergeTask
			}
			continue
		}
		// if we already know the range of the scan is empty, just return a TableDual
		if len(path.Ranges) == 0 && !ds.ctx.GetSessionVars().StmtCtx.UseCache {
			dual := PhysicalTableDual{}.Init(ds.ctx, ds.stats, ds.blockOffset)
			dual.SetSchema(ds.schema)
			return &rootTask{
				p: dual,
			}, nil
		}
		canConvertPointGet := (!ds.isPartition && len(path.Ranges) > 0) || (ds.isPartition && len(path.Ranges) == 1)
		canConvertPointGet = canConvertPointGet && candidate.path.StoreType != kv.TiFlash
		if !candidate.path.IsTablePath {
			canConvertPointGet = canConvertPointGet &&
				candidate.path.Index.Unique &&
				!candidate.path.Index.HasPrefixIndex() &&
				len(candidate.path.Ranges[0].LowVal) == len(candidate.path.Index.Columns)
		}
		if canConvertPointGet {
			allRangeIsPoint := true
			for _, ran := range path.Ranges {
				if !ran.IsPoint(ds.ctx.GetSessionVars().StmtCtx) {
					allRangeIsPoint = false
					break
				}
			}
			if allRangeIsPoint {
				var pointGetTask task
				if len(path.Ranges) == 1 {
					pointGetTask = ds.convertToPointGet(prop, candidate)
				} else {
					pointGetTask = ds.convertToBatchPointGet(prop, candidate)
				}
				if pointGetTask.cost() < t.cost() {
					t = pointGetTask
					continue
				}
			}
		}
		if path.IsTablePath {
			if ds.preferStoreType&preferTiFlash != 0 && path.StoreType == kv.TiKV {
				continue
			}
			if ds.preferStoreType&preferTiKV != 0 && path.StoreType == kv.TiFlash {
				continue
			}
			tblTask, err := ds.convertToTableScan(prop, candidate)
			if err != nil {
				return nil, err
			}
			if tblTask.cost() < t.cost() {
				t = tblTask
			}
			continue
		}
		// TiFlash storage do not support index scan.
		if ds.preferStoreType&preferTiFlash != 0 {
			continue
		}
		idxTask, err := ds.convertToIndexScan(prop, candidate)
		if err != nil {
			return nil, err
		}
		if idxTask.cost() < t.cost() {
			t = idxTask
		}
	}

	return
}

func (ds *DataSource) convertToIndexMergeScan(prop *property.PhysicalProperty, candidate *candidatePath) (task task, err error) {
	if prop.TaskTp != property.RootTaskType || !prop.IsEmpty() {
		return invalidTask, nil
	}
	path := candidate.path
	var totalCost, totalRowCount float64
	scans := make([]PhysicalPlan, 0, len(path.PartialIndexPaths))
	cop := &copTask{
		indexPlanFinished: true,
		tblColHists:       ds.TblColHists,
	}
	for _, partPath := range path.PartialIndexPaths {
		var scan PhysicalPlan
		var partialCost, rowCount float64
		if partPath.IsTablePath {
			scan, partialCost, rowCount = ds.convertToPartialTableScan(prop, partPath)
		} else {
			scan, partialCost, rowCount = ds.convertToPartialIndexScan(prop, partPath)
		}
		scans = append(scans, scan)
		totalCost += partialCost
		totalRowCount += rowCount
	}

	ts, partialCost := ds.buildIndexMergeTableScan(prop, path.TableFilters, totalRowCount)
	totalCost += partialCost
	cop.tablePlan = ts
	cop.idxMergePartPlans = scans
	cop.cst = totalCost
	task = finishCopTask(ds.ctx, cop)
	return task, nil
}

func (ds *DataSource) convertToPartialIndexScan(prop *property.PhysicalProperty, path *util.AccessPath) (
	indexPlan PhysicalPlan,
	partialCost float64,
	rowCount float64) {
	idx := path.Index
	is, partialCost, rowCount := ds.getOriginalPhysicalIndexScan(prop, path, false, false)
	rowSize := is.indexScanRowSize(idx, ds, false)
	// TODO: Consider using isCoveringIndex() to avoid another TableRead
	indexConds := path.IndexFilters
	sessVars := ds.ctx.GetSessionVars()
	if indexConds != nil {
		var selectivity float64
		partialCost += rowCount * sessVars.CopCPUFactor
		if path.CountAfterAccess > 0 {
			selectivity = path.CountAfterIndex / path.CountAfterAccess
		}
		rowCount = is.stats.RowCount * selectivity
		stats := &property.StatsInfo{RowCount: rowCount}
		stats.StatsVersion = ds.statisticTable.Version
		if ds.statisticTable.Pseudo {
			stats.StatsVersion = statistics.PseudoVersion
		}
		indexPlan := PhysicalSelection{Conditions: indexConds}.Init(is.ctx, stats, ds.blockOffset)
		indexPlan.SetChildren(is)
		partialCost += rowCount * rowSize * sessVars.NetworkFactor
		return indexPlan, partialCost, rowCount
	}
	partialCost += rowCount * rowSize * sessVars.NetworkFactor
	indexPlan = is
	return indexPlan, partialCost, rowCount
}

func (ds *DataSource) convertToPartialTableScan(prop *property.PhysicalProperty, path *util.AccessPath) (
	tablePlan PhysicalPlan,
	partialCost float64,
	rowCount float64) {
	ts, partialCost, rowCount := ds.getOriginalPhysicalTableScan(prop, path, false)
	rowSize := ds.TblColHists.GetAvgRowSize(ds.ctx, ds.TblCols, false, false)
	sessVars := ds.ctx.GetSessionVars()
	if len(ts.filterCondition) > 0 {
		selectivity, _, err := ds.tableStats.HistColl.Selectivity(ds.ctx, ts.filterCondition, nil)
		if err != nil {
			logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = SelectionFactor
		}
		tablePlan = PhysicalSelection{Conditions: ts.filterCondition}.Init(ts.ctx, ts.stats.ScaleByExpectCnt(selectivity*rowCount), ds.blockOffset)
		tablePlan.SetChildren(ts)
		partialCost += rowCount * sessVars.CopCPUFactor
		partialCost += selectivity * rowCount * rowSize * sessVars.NetworkFactor
		return tablePlan, partialCost, rowCount
	}
	partialCost += rowCount * rowSize * sessVars.NetworkFactor
	tablePlan = ts
	return tablePlan, partialCost, rowCount
}

func (ds *DataSource) buildIndexMergeTableScan(prop *property.PhysicalProperty, tableFilters []expression.Expression, totalRowCount float64) (PhysicalPlan, float64) {
	var partialCost float64
	sessVars := ds.ctx.GetSessionVars()
	ts := PhysicalTableScan{
		Table:           ds.tableInfo,
		Columns:         ds.Columns,
		TableAsName:     ds.TableAsName,
		DBName:          ds.DBName,
		isPartition:     ds.isPartition,
		physicalTableID: ds.physicalTableID,
	}.Init(ds.ctx, ds.blockOffset)
	ts.SetSchema(ds.schema.Clone())
	if ts.Table.PKIsHandle {
		if pkColInfo := ts.Table.GetPkColInfo(); pkColInfo != nil {
			if ds.statisticTable.Columns[pkColInfo.ID] != nil {
				ts.Hist = &ds.statisticTable.Columns[pkColInfo.ID].Histogram
			}
		}
	}
	rowSize := ds.TblColHists.GetTableAvgRowSize(ds.ctx, ds.TblCols, ts.StoreType, true)
	partialCost += totalRowCount * rowSize * sessVars.ScanFactor
	ts.stats = ds.tableStats.ScaleByExpectCnt(totalRowCount)
	if ds.statisticTable.Pseudo {
		ts.stats.StatsVersion = statistics.PseudoVersion
	}
	if len(tableFilters) > 0 {
		partialCost += totalRowCount * sessVars.CopCPUFactor
		selectivity, _, err := ds.tableStats.HistColl.Selectivity(ds.ctx, tableFilters, nil)
		if err != nil {
			logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = SelectionFactor
		}
		sel := PhysicalSelection{Conditions: tableFilters}.Init(ts.ctx, ts.stats.ScaleByExpectCnt(selectivity*totalRowCount), ts.blockOffset)
		sel.SetChildren(ts)
		return sel, partialCost
	}
	return ts, partialCost
}

func isCoveringIndex(columns, indexColumns []*expression.Column, idxColLens []int, pkIsHandle bool) bool {
	for _, col := range columns {
		if pkIsHandle && mysql.HasPriKeyFlag(col.RetType.Flag) {
			continue
		}
		if col.ID == model.ExtraHandleID {
			continue
		}
		isIndexColumn := false
		for i, indexCol := range indexColumns {
			isFullLen := idxColLens[i] == types.UnspecifiedLength || idxColLens[i] == col.RetType.Flen
			// We use col.OrigColName instead of col.ColName.
			// Related issue: https://github.com/pingcap/tidb/issues/9636.
			if indexCol != nil && col.Equal(nil, indexCol) && isFullLen {
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
func (ts *PhysicalTableScan) appendExtraHandleCol(ds *DataSource) (*expression.Column, bool) {
	handleCol := ds.handleCol
	if handleCol != nil {
		return handleCol, false
	}
	handleCol = ds.newExtraHandleSchemaCol()
	ts.schema.Append(handleCol)
	ts.Columns = append(ts.Columns, model.NewExtraHandleColInfo())
	return handleCol, true
}

// convertToIndexScan converts the DataSource to index scan with idx.
func (ds *DataSource) convertToIndexScan(prop *property.PhysicalProperty, candidate *candidatePath) (task task, err error) {
	if !candidate.isSingleScan {
		// If it's parent requires single read task, return max cost.
		if prop.TaskTp == property.CopSingleReadTaskType {
			return invalidTask, nil
		}
	} else if prop.TaskTp == property.CopDoubleReadTaskType {
		// If it's parent requires double read task, return max cost.
		return invalidTask, nil
	}
	if !prop.IsEmpty() && !candidate.isMatchProp {
		return invalidTask, nil
	}
	path := candidate.path
	is, cost, _ := ds.getOriginalPhysicalIndexScan(prop, path, candidate.isMatchProp, candidate.isSingleScan)
	cop := &copTask{
		indexPlan:   is,
		tblColHists: ds.TblColHists,
		tblCols:     ds.TblCols,
	}
	if !candidate.isSingleScan {
		// On this way, it's double read case.
		ts := PhysicalTableScan{
			Columns:         ds.Columns,
			Table:           is.Table,
			TableAsName:     ds.TableAsName,
			isPartition:     ds.isPartition,
			physicalTableID: ds.physicalTableID,
		}.Init(ds.ctx, is.blockOffset)
		ts.SetSchema(ds.schema.Clone())
		ts.Columns = ExpandVirtualColumn(ts.Columns, ts.schema, ts.Table.Columns)
		cop.tablePlan = ts
	}
	cop.cst = cost
	task = cop
	if candidate.isMatchProp {
		if cop.tablePlan != nil {
			col, isNew := cop.tablePlan.(*PhysicalTableScan).appendExtraHandleCol(ds)
			cop.extraHandleCol = col
			cop.doubleReadNeedProj = isNew
		}
		cop.keepOrder = true
	}
	// prop.IsEmpty() would always return true when coming to here,
	// so we can just use prop.ExpectedCnt as parameter of addPushedDownSelection.
	finalStats := ds.stats.ScaleByExpectCnt(prop.ExpectedCnt)
	is.addPushedDownSelection(cop, ds, path, finalStats)
	if prop.TaskTp == property.RootTaskType {
		task = finishCopTask(ds.ctx, task)
	} else if _, ok := task.(*rootTask); ok {
		return invalidTask, nil
	}
	return task, nil
}

func (is *PhysicalIndexScan) indexScanRowSize(idx *model.IndexInfo, ds *DataSource, isForScan bool) float64 {
	scanCols := make([]*expression.Column, 0, len(idx.Columns)+1)
	// If `initSchema` has already appended the handle column in schema, just use schema columns, otherwise, add extra handle column.
	if len(idx.Columns) == len(is.schema.Columns) {
		scanCols = append(scanCols, is.schema.Columns...)
		handleCol := ds.getPKIsHandleCol()
		if handleCol != nil {
			scanCols = append(scanCols, handleCol)
		}
	} else {
		scanCols = is.schema.Columns
	}
	if isForScan {
		return ds.TblColHists.GetIndexAvgRowSize(is.ctx, scanCols, is.Index.Unique)
	}
	return ds.TblColHists.GetAvgRowSize(is.ctx, scanCols, true, false)
}

func (is *PhysicalIndexScan) initSchema(idx *model.IndexInfo, idxExprCols []*expression.Column, isDoubleRead bool) {
	indexCols := make([]*expression.Column, len(is.IdxCols), len(idx.Columns)+1)
	copy(indexCols, is.IdxCols)
	for i := len(is.IdxCols); i < len(idx.Columns); i++ {
		if idxExprCols[i] != nil {
			indexCols = append(indexCols, idxExprCols[i])
		} else {
			// TODO: try to reuse the col generated when building the DataSource.
			indexCols = append(indexCols, &expression.Column{
				ID:       is.Table.Columns[idx.Columns[i].Offset].ID,
				RetType:  &is.Table.Columns[idx.Columns[i].Offset].FieldType,
				UniqueID: is.ctx.GetSessionVars().AllocPlanColumnID(),
			})
		}
	}
	setHandle := len(indexCols) > len(idx.Columns)
	if !setHandle {
		for i, col := range is.Columns {
			if (mysql.HasPriKeyFlag(col.Flag) && is.Table.PKIsHandle) || col.ID == model.ExtraHandleID {
				indexCols = append(indexCols, is.dataSourceSchema.Columns[i])
				setHandle = true
				break
			}
		}
	}
	// If it's double read case, the first index must return handle. So we should add extra handle column
	// if there isn't a handle column.
	if isDoubleRead && !setHandle {
		indexCols = append(indexCols, &expression.Column{
			RetType:  types.NewFieldType(mysql.TypeLonglong),
			ID:       model.ExtraHandleID,
			UniqueID: is.ctx.GetSessionVars().AllocPlanColumnID(),
		})
	}
	is.SetSchema(expression.NewSchema(indexCols...))
}

func (is *PhysicalIndexScan) addPushedDownSelection(copTask *copTask, p *DataSource, path *util.AccessPath, finalStats *property.StatsInfo) {
	// Add filter condition to table plan now.
	indexConds, tableConds := path.IndexFilters, path.TableFilters

	tableConds, copTask.rootTaskConds = SplitSelCondsWithVirtualColumn(tableConds)

	var newRootConds []expression.Expression
	indexConds, newRootConds = expression.PushDownExprs(is.ctx.GetSessionVars().StmtCtx, indexConds, is.ctx.GetClient(), kv.TiKV)
	copTask.rootTaskConds = append(copTask.rootTaskConds, newRootConds...)

	tableConds, newRootConds = expression.PushDownExprs(is.ctx.GetSessionVars().StmtCtx, tableConds, is.ctx.GetClient(), kv.TiKV)
	copTask.rootTaskConds = append(copTask.rootTaskConds, newRootConds...)

	sessVars := is.ctx.GetSessionVars()
	if indexConds != nil {
		copTask.cst += copTask.count() * sessVars.CopCPUFactor
		var selectivity float64
		if path.CountAfterAccess > 0 {
			selectivity = path.CountAfterIndex / path.CountAfterAccess
		}
		count := is.stats.RowCount * selectivity
		stats := p.tableStats.ScaleByExpectCnt(count)
		indexSel := PhysicalSelection{Conditions: indexConds}.Init(is.ctx, stats, is.blockOffset)
		indexSel.SetChildren(is)
		copTask.indexPlan = indexSel
	}
	if len(tableConds) > 0 {
		copTask.finishIndexPlan()
		copTask.cst += copTask.count() * sessVars.CopCPUFactor
		tableSel := PhysicalSelection{Conditions: tableConds}.Init(is.ctx, finalStats, is.blockOffset)
		tableSel.SetChildren(copTask.tablePlan)
		copTask.tablePlan = tableSel
	}
}

// SplitSelCondsWithVirtualColumn filter the select conditions which contain virtual column
func SplitSelCondsWithVirtualColumn(conds []expression.Expression) ([]expression.Expression, []expression.Expression) {
	var filterConds []expression.Expression
	for i := len(conds) - 1; i >= 0; i-- {
		if expression.ContainVirtualColumn(conds[i : i+1]) {
			filterConds = append(filterConds, conds[i])
			conds = append(conds[:i], conds[i+1:]...)
		}
	}
	return conds, filterConds
}

func matchIndicesProp(idxCols []*expression.Column, colLens []int, propItems []property.Item) bool {
	if len(idxCols) < len(propItems) {
		return false
	}
	for i, item := range propItems {
		if colLens[i] != types.UnspecifiedLength || !item.Col.Equal(nil, idxCols[i]) {
			return false
		}
	}
	return true
}

func splitIndexFilterConditions(conditions []expression.Expression, indexColumns []*expression.Column, idxColLens []int,
	table *model.TableInfo) (indexConds, tableConds []expression.Expression) {
	var indexConditions, tableConditions []expression.Expression
	for _, cond := range conditions {
		if isCoveringIndex(expression.ExtractColumns(cond), indexColumns, idxColLens, table.PKIsHandle) {
			indexConditions = append(indexConditions, cond)
		} else {
			tableConditions = append(tableConditions, cond)
		}
	}
	return indexConditions, tableConditions
}

// getMostCorrColFromExprs checks if column in the condition is correlated enough with handle. If the condition
// contains multiple columns, return nil and get the max correlation, which would be used in the heuristic estimation.
func getMostCorrColFromExprs(exprs []expression.Expression, histColl *statistics.Table, threshold float64) (*expression.Column, float64) {
	var cols []*expression.Column
	cols = expression.ExtractColumnsFromExpressions(cols, exprs, nil)
	if len(cols) == 0 {
		return nil, 0
	}
	colSet := set.NewInt64Set()
	var corr float64
	var corrCol *expression.Column
	for _, col := range cols {
		if colSet.Exist(col.UniqueID) {
			continue
		}
		colSet.Insert(col.UniqueID)
		hist, ok := histColl.Columns[col.ID]
		if !ok {
			continue
		}
		curCorr := math.Abs(hist.Correlation)
		if corrCol == nil || corr < curCorr {
			corrCol = col
			corr = curCorr
		}
	}
	if len(colSet) == 1 && corr >= threshold {
		return corrCol, corr
	}
	return nil, corr
}

// getColumnRangeCounts estimates row count for each range respectively.
func getColumnRangeCounts(sc *stmtctx.StatementContext, colID int64, ranges []*ranger.Range, histColl *statistics.Table, idxID int64) ([]float64, bool) {
	var err error
	var count float64
	rangeCounts := make([]float64, len(ranges))
	for i, ran := range ranges {
		if idxID >= 0 {
			idxHist := histColl.Indices[idxID]
			if idxHist == nil || idxHist.IsInvalid(false) {
				return nil, false
			}
			count, err = histColl.GetRowCountByIndexRanges(sc, idxID, []*ranger.Range{ran})
		} else {
			colHist, ok := histColl.Columns[colID]
			if !ok || colHist.IsInvalid(sc, false) {
				return nil, false
			}
			count, err = histColl.GetRowCountByColumnRanges(sc, colID, []*ranger.Range{ran})
		}
		if err != nil {
			return nil, false
		}
		rangeCounts[i] = count
	}
	return rangeCounts, true
}

// convertRangeFromExpectedCnt builds new ranges used to estimate row count we need to scan in table scan before finding specified
// number of tuples which fall into input ranges.
func convertRangeFromExpectedCnt(ranges []*ranger.Range, rangeCounts []float64, expectedCnt float64, desc bool) ([]*ranger.Range, float64, bool) {
	var i int
	var count float64
	var convertedRanges []*ranger.Range
	if desc {
		for i = len(ranges) - 1; i >= 0; i-- {
			if count+rangeCounts[i] >= expectedCnt {
				break
			}
			count += rangeCounts[i]
		}
		if i < 0 {
			return nil, 0, true
		}
		convertedRanges = []*ranger.Range{{LowVal: ranges[i].HighVal, HighVal: []types.Datum{types.MaxValueDatum()}, LowExclude: !ranges[i].HighExclude}}
	} else {
		for i = 0; i < len(ranges); i++ {
			if count+rangeCounts[i] >= expectedCnt {
				break
			}
			count += rangeCounts[i]
		}
		if i == len(ranges) {
			return nil, 0, true
		}
		convertedRanges = []*ranger.Range{{LowVal: []types.Datum{{}}, HighVal: ranges[i].LowVal, HighExclude: !ranges[i].LowExclude}}
	}
	return convertedRanges, count, false
}

// crossEstimateRowCount estimates row count of table scan using histogram of another column which is in TableFilters
// and has high order correlation with handle column. For example, if the query is like:
// `select * from tbl where a = 1 order by pk limit 1`
// if order of column `a` is strictly correlated with column `pk`, the row count of table scan should be:
// `1 + row_count(a < 1 or a is null)`
func (ds *DataSource) crossEstimateRowCount(path *util.AccessPath, expectedCnt float64, desc bool) (float64, bool, float64) {
	if ds.statisticTable.Pseudo || len(path.TableFilters) == 0 {
		return 0, false, 0
	}
	col, corr := getMostCorrColFromExprs(path.TableFilters, ds.statisticTable, ds.ctx.GetSessionVars().CorrelationThreshold)
	// If table scan is not full range scan, we cannot use histogram of other columns for estimation, because
	// the histogram reflects value distribution in the whole table level.
	if col == nil || len(path.AccessConds) > 0 {
		return 0, false, corr
	}
	colInfoID := col.ID
	colID := col.UniqueID
	colHist := ds.statisticTable.Columns[colInfoID]
	if colHist.Correlation < 0 {
		desc = !desc
	}
	accessConds, remained := ranger.DetachCondsForColumn(ds.ctx, path.TableFilters, col)
	if len(accessConds) == 0 {
		return 0, false, corr
	}
	sc := ds.ctx.GetSessionVars().StmtCtx
	ranges, err := ranger.BuildColumnRange(accessConds, sc, col.RetType, types.UnspecifiedLength)
	if len(ranges) == 0 || err != nil {
		return 0, err == nil, corr
	}
	idxID, idxExists := ds.stats.HistColl.ColID2IdxID[colID]
	if !idxExists {
		idxID = -1
	}
	rangeCounts, ok := getColumnRangeCounts(sc, colInfoID, ranges, ds.statisticTable, idxID)
	if !ok {
		return 0, false, corr
	}
	convertedRanges, count, isFull := convertRangeFromExpectedCnt(ranges, rangeCounts, expectedCnt, desc)
	if isFull {
		return path.CountAfterAccess, true, 0
	}
	var rangeCount float64
	if idxExists {
		rangeCount, err = ds.statisticTable.GetRowCountByIndexRanges(sc, idxID, convertedRanges)
	} else {
		rangeCount, err = ds.statisticTable.GetRowCountByColumnRanges(sc, colInfoID, convertedRanges)
	}
	if err != nil {
		return 0, false, corr
	}
	scanCount := rangeCount + expectedCnt - count
	if len(remained) > 0 {
		scanCount = scanCount / SelectionFactor
	}
	scanCount = math.Min(scanCount, path.CountAfterAccess)
	return scanCount, true, 0
}

// GetPhysicalScan returns PhysicalTableScan for the LogicalTableScan.
func (s *LogicalTableScan) GetPhysicalScan(schema *expression.Schema, stats *property.StatsInfo) *PhysicalTableScan {
	ds := s.Source
	ts := PhysicalTableScan{
		Table:           ds.tableInfo,
		Columns:         ds.Columns,
		TableAsName:     ds.TableAsName,
		DBName:          ds.DBName,
		isPartition:     ds.isPartition,
		physicalTableID: ds.physicalTableID,
		Ranges:          s.Ranges,
		AccessCondition: s.AccessConds,
	}.Init(s.ctx, s.blockOffset)
	ts.stats = stats
	ts.SetSchema(schema.Clone())
	if ts.Table.PKIsHandle {
		if pkColInfo := ts.Table.GetPkColInfo(); pkColInfo != nil {
			if ds.statisticTable.Columns[pkColInfo.ID] != nil {
				ts.Hist = &ds.statisticTable.Columns[pkColInfo.ID].Histogram
			}
		}
	}
	return ts
}

// GetPhysicalIndexScan returns PhysicalIndexScan for the logical IndexScan.
func (s *LogicalIndexScan) GetPhysicalIndexScan(schema *expression.Schema, stats *property.StatsInfo) *PhysicalIndexScan {
	ds := s.Source
	is := PhysicalIndexScan{
		Table:            ds.tableInfo,
		TableAsName:      ds.TableAsName,
		DBName:           ds.DBName,
		Columns:          s.Columns,
		Index:            s.Index,
		IdxCols:          s.IdxCols,
		IdxColLens:       s.IdxColLens,
		AccessCondition:  s.AccessConds,
		Ranges:           s.Ranges,
		dataSourceSchema: ds.schema,
		isPartition:      ds.isPartition,
		physicalTableID:  ds.physicalTableID,
	}.Init(ds.ctx, ds.blockOffset)
	is.stats = stats
	is.initSchema(s.Index, s.FullIdxCols, s.IsDoubleRead)
	return is
}

// convertToTableScan converts the DataSource to table scan.
func (ds *DataSource) convertToTableScan(prop *property.PhysicalProperty, candidate *candidatePath) (task task, err error) {
	// It will be handled in convertToIndexScan.
	if prop.TaskTp == property.CopDoubleReadTaskType {
		return invalidTask, nil
	}
	if !prop.IsEmpty() && !candidate.isMatchProp {
		return invalidTask, nil
	}
	ts, cost, _ := ds.getOriginalPhysicalTableScan(prop, candidate.path, candidate.isMatchProp)
	copTask := &copTask{
		tablePlan:         ts,
		indexPlanFinished: true,
		tblColHists:       ds.TblColHists,
		cst:               cost,
	}
	task = copTask
	if candidate.isMatchProp {
		copTask.keepOrder = true
	}
	ts.addPushedDownSelection(copTask, ds.stats.ScaleByExpectCnt(prop.ExpectedCnt))
	if prop.TaskTp == property.RootTaskType {
		task = finishCopTask(ds.ctx, task)
	} else if _, ok := task.(*rootTask); ok {
		return invalidTask, nil
	}
	return task, nil
}

func (ds *DataSource) convertToPointGet(prop *property.PhysicalProperty, candidate *candidatePath) task {
	if !prop.IsEmpty() && !candidate.isMatchProp {
		return invalidTask
	}
	if prop.TaskTp == property.CopDoubleReadTaskType && candidate.isSingleScan ||
		prop.TaskTp == property.CopSingleReadTaskType && !candidate.isSingleScan {
		return invalidTask
	}

	pointGetPlan := PointGetPlan{
		ctx:          ds.ctx,
		schema:       ds.schema.Clone(),
		dbName:       ds.DBName.L,
		TblInfo:      ds.TableInfo(),
		outputNames:  ds.OutputNames(),
		LockWaitTime: ds.ctx.GetSessionVars().LockWaitTimeout,
		Columns:      ds.Columns,
	}.Init(ds.ctx, ds.stats.ScaleByExpectCnt(1.0), ds.blockOffset)
	var partitionInfo *model.PartitionDefinition
	if ds.isPartition {
		if pi := ds.tableInfo.GetPartitionInfo(); pi != nil {
			for _, def := range pi.Definitions {
				if def.ID == ds.physicalTableID {
					partitionInfo = &def
					break
				}
			}
		}
		if partitionInfo == nil {
			return invalidTask
		}
	}
	rTsk := &rootTask{p: pointGetPlan}
	var cost float64
	if candidate.path.IsTablePath {
		pointGetPlan.Handle = candidate.path.Ranges[0].LowVal[0].GetInt64()
		pointGetPlan.UnsignedHandle = mysql.HasUnsignedFlag(ds.getHandleCol().RetType.Flag)
		pointGetPlan.PartitionInfo = partitionInfo
		cost = pointGetPlan.GetCost(ds.TblCols)
		// Add filter condition to table plan now.
		if len(candidate.path.TableFilters) > 0 {
			sessVars := ds.ctx.GetSessionVars()
			cost += pointGetPlan.stats.RowCount * sessVars.CPUFactor
			sel := PhysicalSelection{
				Conditions: candidate.path.TableFilters,
			}.Init(ds.ctx, ds.stats.ScaleByExpectCnt(prop.ExpectedCnt), ds.blockOffset)
			sel.SetChildren(pointGetPlan)
			rTsk.p = sel
		}
	} else {
		pointGetPlan.IndexInfo = candidate.path.Index
		pointGetPlan.IndexValues = candidate.path.Ranges[0].LowVal
		pointGetPlan.PartitionInfo = partitionInfo
		if candidate.isSingleScan {
			cost = pointGetPlan.GetCost(candidate.path.IdxCols)
		} else {
			cost = pointGetPlan.GetCost(ds.TblCols)
		}
		// Add index condition to table plan now.
		if len(candidate.path.IndexFilters)+len(candidate.path.TableFilters) > 0 {
			sessVars := ds.ctx.GetSessionVars()
			cost += pointGetPlan.stats.RowCount * sessVars.CPUFactor
			sel := PhysicalSelection{
				Conditions: append(candidate.path.IndexFilters, candidate.path.TableFilters...),
			}.Init(ds.ctx, ds.stats.ScaleByExpectCnt(prop.ExpectedCnt), ds.blockOffset)
			sel.SetChildren(pointGetPlan)
			rTsk.p = sel
		}
	}

	rTsk.cst = cost
	return rTsk
}

func (ds *DataSource) convertToBatchPointGet(prop *property.PhysicalProperty, candidate *candidatePath) task {
	if !prop.IsEmpty() && !candidate.isMatchProp {
		return invalidTask
	}
	if prop.TaskTp == property.CopDoubleReadTaskType && candidate.isSingleScan ||
		prop.TaskTp == property.CopSingleReadTaskType && !candidate.isSingleScan {
		return invalidTask
	}

	batchPointGetPlan := BatchPointGetPlan{
		ctx:       ds.ctx,
		TblInfo:   ds.TableInfo(),
		KeepOrder: !prop.IsEmpty(),
		Columns:   ds.Columns,
	}.Init(ds.ctx, ds.stats.ScaleByExpectCnt(float64(len(candidate.path.Ranges))), ds.schema.Clone(), ds.names, ds.blockOffset)
	if batchPointGetPlan.KeepOrder {
		batchPointGetPlan.Desc = prop.Items[0].Desc
	}
	rTsk := &rootTask{p: batchPointGetPlan}
	var cost float64
	if candidate.path.IsTablePath {
		for _, ran := range candidate.path.Ranges {
			batchPointGetPlan.Handles = append(batchPointGetPlan.Handles, ran.LowVal[0].GetInt64())
		}
		cost = batchPointGetPlan.GetCost(ds.TblCols)
		// Add filter condition to table plan now.
		if len(candidate.path.TableFilters) > 0 {
			sessVars := ds.ctx.GetSessionVars()
			cost += batchPointGetPlan.stats.RowCount * sessVars.CPUFactor
			sel := PhysicalSelection{
				Conditions: candidate.path.TableFilters,
			}.Init(ds.ctx, ds.stats.ScaleByExpectCnt(prop.ExpectedCnt), ds.blockOffset)
			sel.SetChildren(batchPointGetPlan)
			rTsk.p = sel
		}
	} else {
		batchPointGetPlan.IndexInfo = candidate.path.Index
		for _, ran := range candidate.path.Ranges {
			batchPointGetPlan.IndexValues = append(batchPointGetPlan.IndexValues, ran.LowVal)
		}
		if !prop.IsEmpty() {
			batchPointGetPlan.KeepOrder = true
			batchPointGetPlan.Desc = prop.Items[0].Desc
		}
		if candidate.isSingleScan {
			cost = batchPointGetPlan.GetCost(candidate.path.IdxCols)
		} else {
			cost = batchPointGetPlan.GetCost(ds.TblCols)
		}
		// Add index condition to table plan now.
		if len(candidate.path.IndexFilters)+len(candidate.path.TableFilters) > 0 {
			sessVars := ds.ctx.GetSessionVars()
			cost += batchPointGetPlan.stats.RowCount * sessVars.CPUFactor
			sel := PhysicalSelection{
				Conditions: append(candidate.path.IndexFilters, candidate.path.TableFilters...),
			}.Init(ds.ctx, ds.stats.ScaleByExpectCnt(prop.ExpectedCnt), ds.blockOffset)
			sel.SetChildren(batchPointGetPlan)
			rTsk.p = sel
		}
	}

	rTsk.cst = cost
	return rTsk
}

func (ts *PhysicalTableScan) addPushedDownSelection(copTask *copTask, stats *property.StatsInfo) {
	ts.filterCondition, copTask.rootTaskConds = SplitSelCondsWithVirtualColumn(ts.filterCondition)
	var newRootConds []expression.Expression
	ts.filterCondition, newRootConds = expression.PushDownExprs(ts.ctx.GetSessionVars().StmtCtx, ts.filterCondition, ts.ctx.GetClient(), ts.StoreType)
	copTask.rootTaskConds = append(copTask.rootTaskConds, newRootConds...)

	// Add filter condition to table plan now.
	sessVars := ts.ctx.GetSessionVars()
	if len(ts.filterCondition) > 0 {
		copTask.cst += copTask.count() * sessVars.CopCPUFactor
		sel := PhysicalSelection{Conditions: ts.filterCondition}.Init(ts.ctx, stats, ts.blockOffset)
		sel.SetChildren(ts)
		copTask.tablePlan = sel
	}
}

func (ds *DataSource) getOriginalPhysicalTableScan(prop *property.PhysicalProperty, path *util.AccessPath, isMatchProp bool) (*PhysicalTableScan, float64, float64) {
	ts := PhysicalTableScan{
		Table:           ds.tableInfo,
		Columns:         ds.Columns,
		TableAsName:     ds.TableAsName,
		DBName:          ds.DBName,
		isPartition:     ds.isPartition,
		physicalTableID: ds.physicalTableID,
		Ranges:          path.Ranges,
		AccessCondition: path.AccessConds,
		filterCondition: path.TableFilters,
		StoreType:       path.StoreType,
	}.Init(ds.ctx, ds.blockOffset)
	ts.SetSchema(ds.schema.Clone())
	if ts.Table.PKIsHandle {
		if pkColInfo := ts.Table.GetPkColInfo(); pkColInfo != nil {
			if ds.statisticTable.Columns[pkColInfo.ID] != nil {
				ts.Hist = &ds.statisticTable.Columns[pkColInfo.ID].Histogram
			}
		}
	}
	rowCount := path.CountAfterAccess
	if prop.ExpectedCnt < ds.stats.RowCount {
		count, ok, corr := ds.crossEstimateRowCount(path, prop.ExpectedCnt, isMatchProp && prop.Items[0].Desc)
		if ok {
			// TODO: actually, before using this count as the estimated row count of table scan, we need additionally
			// check if count < row_count(first_region | last_region), and use the larger one since we build one copTask
			// for one region now, so even if it is `limit 1`, we have to scan at least one region in table scan.
			// Currently, we can use `tikvrpc.CmdDebugGetRegionProperties` interface as `getSampRegionsRowCount()` does
			// to get the row count in a region, but that result contains MVCC old version rows, so it is not that accurate.
			// Considering that when this scenario happens, the execution time is close between IndexScan and TableScan,
			// we do not add this check temporarily.
			rowCount = count
		} else if corr < 1 {
			correlationFactor := math.Pow(1-corr, float64(ds.ctx.GetSessionVars().CorrelationExpFactor))
			selectivity := ds.stats.RowCount / rowCount
			rowCount = math.Min(prop.ExpectedCnt/selectivity/correlationFactor, rowCount)
		}
	}
	// We need NDV of columns since it may be used in cost estimation of join. Precisely speaking,
	// we should track NDV of each histogram bucket, and sum up the NDV of buckets we actually need
	// to scan, but this would only help improve accuracy of NDV for one column, for other columns,
	// we still need to assume values are uniformly distributed. For simplicity, we use uniform-assumption
	// for all columns now, as we do in `deriveStatsByFilter`.
	ts.stats = ds.tableStats.ScaleByExpectCnt(rowCount)
	var rowSize float64
	if ts.StoreType == kv.TiKV {
		rowSize = ds.TblColHists.GetTableAvgRowSize(ds.ctx, ds.TblCols, ts.StoreType, true)
	} else {
		// If `ds.handleCol` is nil, then the schema of tableScan doesn't have handle column.
		// This logic can be ensured in column pruning.
		rowSize = ds.TblColHists.GetTableAvgRowSize(ds.ctx, ts.Schema().Columns, ts.StoreType, ds.handleCol != nil)
	}
	sessVars := ds.ctx.GetSessionVars()
	cost := rowCount * rowSize * sessVars.ScanFactor
	if isMatchProp {
		if prop.Items[0].Desc {
			ts.Desc = true
			cost = rowCount * rowSize * sessVars.DescScanFactor
		}
		ts.KeepOrder = true
	}
	switch ts.StoreType {
	case kv.TiKV:
		cost += float64(len(ts.Ranges)) * sessVars.SeekFactor
	case kv.TiFlash:
		cost += float64(len(ts.Ranges)) * float64(len(ts.Columns)) * sessVars.SeekFactor
	}
	return ts, cost, rowCount
}

func (ds *DataSource) getOriginalPhysicalIndexScan(prop *property.PhysicalProperty, path *util.AccessPath, isMatchProp bool, isSingleScan bool) (*PhysicalIndexScan, float64, float64) {
	idx := path.Index
	is := PhysicalIndexScan{
		Table:            ds.tableInfo,
		TableAsName:      ds.TableAsName,
		DBName:           ds.DBName,
		Columns:          ds.Columns,
		Index:            idx,
		IdxCols:          path.IdxCols,
		IdxColLens:       path.IdxColLens,
		AccessCondition:  path.AccessConds,
		Ranges:           path.Ranges,
		dataSourceSchema: ds.schema,
		isPartition:      ds.isPartition,
		physicalTableID:  ds.physicalTableID,
	}.Init(ds.ctx, ds.blockOffset)
	statsTbl := ds.statisticTable
	if statsTbl.Indices[idx.ID] != nil {
		is.Hist = &statsTbl.Indices[idx.ID].Histogram
	}
	rowCount := path.CountAfterAccess
	is.initSchema(idx, path.FullIdxCols, !isSingleScan)
	// Only use expectedCnt when it's smaller than the count we calculated.
	// e.g. IndexScan(count1)->After Filter(count2). The `ds.stats.RowCount` is count2. count1 is the one we need to calculate
	// If expectedCnt and count2 are both zero and we go into the below `if` block, the count1 will be set to zero though it's shouldn't be.
	if (isMatchProp || prop.IsEmpty()) && prop.ExpectedCnt < ds.stats.RowCount {
		selectivity := ds.stats.RowCount / path.CountAfterAccess
		rowCount = math.Min(prop.ExpectedCnt/selectivity, rowCount)
	}
	is.stats = ds.tableStats.ScaleByExpectCnt(rowCount)
	rowSize := is.indexScanRowSize(idx, ds, true)
	sessVars := ds.ctx.GetSessionVars()
	cost := rowCount * rowSize * sessVars.ScanFactor
	if isMatchProp {
		if prop.Items[0].Desc {
			is.Desc = true
			cost = rowCount * rowSize * sessVars.DescScanFactor
		}
		is.KeepOrder = true
	}
	cost += float64(len(is.Ranges)) * sessVars.SeekFactor
	return is, cost, rowCount
}
