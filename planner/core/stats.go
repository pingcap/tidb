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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"go.uber.org/zap"
)

func (p *basePhysicalPlan) StatsCount() float64 {
	return p.stats.RowCount
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalTableDual) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	profile := &property.StatsInfo{
		RowCount:    float64(p.RowCount),
		Cardinality: make([]float64, p.Schema().Len()),
	}
	for i := range profile.Cardinality {
		profile.Cardinality[i] = float64(p.RowCount)
	}
	p.stats = profile
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalShow) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	// A fake count, just to avoid panic now.
	p.stats = getFakeStats(p.Schema().Len())
	return p.stats, nil
}

func getFakeStats(length int) *property.StatsInfo {
	profile := &property.StatsInfo{
		RowCount:    1,
		Cardinality: make([]float64, length),
	}
	for i := range profile.Cardinality {
		profile.Cardinality[i] = 1
	}
	return profile
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalShowDDLJobs) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	// A fake count, just to avoid panic now.
	p.stats = getFakeStats(p.Schema().Len())
	return p.stats, nil
}

func (p *baseLogicalPlan) recursiveDeriveStats() (*property.StatsInfo, error) {
	if p.stats != nil {
		return p.stats, nil
	}
	childStats := make([]*property.StatsInfo, len(p.children))
	for i, child := range p.children {
		childProfile, err := child.recursiveDeriveStats()
		if err != nil {
			return nil, err
		}
		childStats[i] = childProfile
	}
	return p.self.DeriveStats(childStats)
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *baseLogicalPlan) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	if len(childStats) == 1 {
		p.stats = childStats[0]
		return p.stats, nil
	}
	if len(childStats) > 1 {
		err := ErrInternal.GenWithStack("LogicalPlans with more than one child should implement their own DeriveStats().")
		return nil, err
	}
	profile := &property.StatsInfo{
		RowCount:    float64(1),
		Cardinality: make([]float64, p.self.Schema().Len()),
	}
	for i := range profile.Cardinality {
		profile.Cardinality[i] = float64(1)
	}
	p.stats = profile
	return profile, nil
}

// getColumnNDV computes estimated NDV of specified column using the original
// histogram of `DataSource` which is retrieved from storage(not the derived one).
func (ds *DataSource) getColumnNDV(colID int64) (ndv float64) {
	hist, ok := ds.statisticTable.Columns[colID]
	if ok && hist.Count > 0 {
		factor := float64(ds.statisticTable.Count) / float64(hist.Count)
		ndv = float64(hist.NDV) * factor
	} else {
		ndv = float64(ds.statisticTable.Count) * distinctFactor
	}
	return ndv
}

func (ds *DataSource) deriveStatsByFilter(conds expression.CNFExprs) {
	if ds.tableStats == nil {
		tableStats := &property.StatsInfo{
			RowCount:     float64(ds.statisticTable.Count),
			Cardinality:  make([]float64, len(ds.Columns)),
			HistColl:     ds.statisticTable.GenerateHistCollFromColumnInfo(ds.Columns, ds.schema.Columns),
			StatsVersion: ds.statisticTable.Version,
		}
		if ds.statisticTable.Pseudo {
			tableStats.StatsVersion = statistics.PseudoVersion
		}
		for i, col := range ds.Columns {
			tableStats.Cardinality[i] = ds.getColumnNDV(col.ID)
		}
		ds.tableStats = tableStats
		ds.TblColHists = ds.statisticTable.ID2UniqueID(ds.TblCols)
	}
	selectivity, nodes, err := ds.tableStats.HistColl.Selectivity(ds.ctx, conds)
	if err != nil {
		logutil.BgLogger().Debug("something wrong happened, use the default selectivity", zap.Error(err))
		selectivity = selectionFactor
	}
	ds.stats = ds.tableStats.Scale(selectivity)
	if ds.ctx.GetSessionVars().OptimizerSelectivityLevel >= 1 {
		ds.stats.HistColl = ds.stats.HistColl.NewHistCollBySelectivity(ds.ctx.GetSessionVars().StmtCtx, nodes)
	}
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (ds *DataSource) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	// PushDownNot here can convert query 'not (a != 1)' to 'a = 1'.
	for i, expr := range ds.pushedDownConds {
		ds.pushedDownConds[i] = expression.PushDownNot(nil, expr, false)
	}
	ds.deriveStatsByFilter(ds.pushedDownConds)
	for _, path := range ds.possibleAccessPaths {
		if path.isTablePath {
			noIntervalRanges, err := ds.deriveTablePathStats(path, ds.pushedDownConds, false)
			if err != nil {
				return nil, err
			}
			// If we have point or empty range, just remove other possible paths.
			if noIntervalRanges || len(path.ranges) == 0 {
				ds.possibleAccessPaths[0] = path
				ds.possibleAccessPaths = ds.possibleAccessPaths[:1]
				break
			}
			continue
		}
		noIntervalRanges, err := ds.deriveIndexPathStats(path, ds.pushedDownConds, false)
		if err != nil {
			return nil, err
		}
		// If we have empty range, or point range on unique index, just remove other possible paths.
		if (noIntervalRanges && path.index.Unique) || len(path.ranges) == 0 {
			ds.possibleAccessPaths[0] = path
			ds.possibleAccessPaths = ds.possibleAccessPaths[:1]
			break
		}
	}
	// Consider the IndexMergePath. Now, we just generate `IndexMergePath` in DNF case.
	if len(ds.pushedDownConds) > 0 && len(ds.possibleAccessPaths) > 1 && ds.ctx.GetSessionVars().GetEnableIndexMerge() {
		needConsiderIndexMerge := true
		for i := 1; i < len(ds.possibleAccessPaths); i++ {
			if len(ds.possibleAccessPaths[i].accessConds) != 0 {
				needConsiderIndexMerge = false
				break
			}
		}
		if needConsiderIndexMerge {
			ds.generateIndexMergeOrPaths()
		}
	}
	return ds.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (ts *TableScan) DeriveStats(childStats []*property.StatsInfo) (_ *property.StatsInfo, err error) {
	// PushDownNot here can convert query 'not (a != 1)' to 'a = 1'.
	for i, expr := range ts.AccessConds {
		// TODO The expressions may be shared by TableScan and several IndexScans, there would be redundant
		// `PushDownNot` function call in multiple `DeriveStats` then.
		ts.AccessConds[i] = expression.PushDownNot(nil, expr, false)
	}
	ts.Source.deriveStatsByFilter(ts.AccessConds)
	sc := ts.SCtx().GetSessionVars().StmtCtx
	ts.Ranges, err = ranger.BuildTableRange(ts.AccessConds, sc, ts.Handle.RetType)
	if err != nil {
		return nil, err
	}
	return ts.Source.stats, nil
}

// getIndexMergeOrPath generates all possible IndexMergeOrPaths.
func (ds *DataSource) generateIndexMergeOrPaths() {
	usedIndexCount := len(ds.possibleAccessPaths)
	for i, cond := range ds.pushedDownConds {
		sf, ok := cond.(*expression.ScalarFunction)
		if !ok || sf.FuncName.L != ast.LogicOr {
			continue
		}
		var partialPaths = make([]*accessPath, 0, usedIndexCount)
		dnfItems := expression.FlattenDNFConditions(sf)
		for _, item := range dnfItems {
			cnfItems := expression.SplitCNFItems(item)
			itemPaths := ds.accessPathsForConds(cnfItems, usedIndexCount)
			if len(itemPaths) == 0 {
				partialPaths = nil
				break
			}
			partialPath := ds.buildIndexMergePartialPath(itemPaths)
			if partialPath == nil {
				partialPaths = nil
				break
			}
			partialPaths = append(partialPaths, partialPath)
		}
		if len(partialPaths) > 1 {
			possiblePath := ds.buildIndexMergeOrPath(partialPaths, i)
			if possiblePath != nil {
				ds.possibleAccessPaths = append(ds.possibleAccessPaths, possiblePath)
			}
		}
	}
}

// accessPathsForConds generates all possible index paths for conditions.
func (ds *DataSource) accessPathsForConds(conditions []expression.Expression, usedIndexCount int) []*accessPath {
	var results = make([]*accessPath, 0, usedIndexCount)
	for i := 0; i < usedIndexCount; i++ {
		path := &accessPath{}
		if ds.possibleAccessPaths[i].isTablePath {
			path.isTablePath = true
			noIntervalRanges, err := ds.deriveTablePathStats(path, conditions, true)
			if err != nil {
				logutil.BgLogger().Debug("can not derive statistics of a path", zap.Error(err))
				continue
			}
			// If we have point or empty range, just remove other possible paths.
			if noIntervalRanges || len(path.ranges) == 0 {
				results[0] = path
				results = results[:1]
				break
			}
		} else {
			path.index = ds.possibleAccessPaths[i].index
			noIntervalRanges, err := ds.deriveIndexPathStats(path, conditions, true)
			if err != nil {
				logutil.BgLogger().Debug("can not derive statistics of a path", zap.Error(err))
				continue
			}
			// If we have empty range, or point range on unique index, just remove other possible paths.
			if (noIntervalRanges && path.index.Unique) || len(path.ranges) == 0 {
				results[0] = path
				results = results[:1]
				break
			}
		}
		// If accessConds is empty or tableFilter is not empty, we ignore the access path.
		// Now these conditions are too strict.
		// For example, a sql `select * from t where a > 1 or (b < 2 and c > 3)` and table `t` with indexes
		// on a and b separately. we can generate a `IndexMergePath` with table filter `a > 1 or (b < 2 and c > 3)`.
		// TODO: solve the above case
		if len(path.tableFilters) > 0 || len(path.accessConds) == 0 {
			continue
		}
		results = append(results, path)
	}
	return results
}

// buildIndexMergePartialPath chooses the best index path from all possible paths.
// Now we just choose the index with most columns.
// We should improve this strategy, because it is not always better to choose index
// with most columns, e.g, filter is c > 1 and the input indexes are c and c_d_e,
// the former one is enough, and it is less expensive in execution compared with the latter one.
// TODO: improve strategy of the partial path selection
func (ds *DataSource) buildIndexMergePartialPath(indexAccessPaths []*accessPath) *accessPath {
	if len(indexAccessPaths) == 1 {
		return indexAccessPaths[0]
	}

	maxColsIndex := 0
	maxCols := len(indexAccessPaths[0].idxCols)
	for i := 1; i < len(indexAccessPaths); i++ {
		current := len(indexAccessPaths[i].idxCols)
		if current > maxCols {
			maxColsIndex = i
			maxCols = current
		}
	}
	return indexAccessPaths[maxColsIndex]
}

// buildIndexMergeOrPath generates one possible IndexMergePath.
func (ds *DataSource) buildIndexMergeOrPath(partialPaths []*accessPath, current int) *accessPath {
	indexMergePath := &accessPath{partialIndexPaths: partialPaths}
	indexMergePath.tableFilters = append(indexMergePath.tableFilters, ds.pushedDownConds[:current]...)
	indexMergePath.tableFilters = append(indexMergePath.tableFilters, ds.pushedDownConds[current+1:]...)
	return indexMergePath
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalSelection) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	p.stats = childStats[0].Scale(selectionFactor)
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalUnionAll) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	p.stats = &property.StatsInfo{
		Cardinality: make([]float64, p.Schema().Len()),
	}
	for _, childProfile := range childStats {
		p.stats.RowCount += childProfile.RowCount
		for i := range p.stats.Cardinality {
			p.stats.Cardinality[i] += childProfile.Cardinality[i]
		}
	}
	return p.stats, nil
}

func deriveLimitStats(childProfile *property.StatsInfo, limitCount float64) *property.StatsInfo {
	stats := &property.StatsInfo{
		RowCount:    math.Min(limitCount, childProfile.RowCount),
		Cardinality: make([]float64, len(childProfile.Cardinality)),
	}
	for i := range stats.Cardinality {
		stats.Cardinality[i] = math.Min(childProfile.Cardinality[i], stats.RowCount)
	}
	return stats
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalLimit) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	p.stats = deriveLimitStats(childStats[0], float64(p.Count))
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (lt *LogicalTopN) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	lt.stats = deriveLimitStats(childStats[0], float64(lt.Count))
	return lt.stats, nil
}

// getCardinality will return the Cardinality of a couple of columns. We simply return the max one, because we cannot know
// the Cardinality for multi-dimension attributes properly. This is a simple and naive scheme of Cardinality estimation.
func getCardinality(cols []*expression.Column, schema *expression.Schema, profile *property.StatsInfo) float64 {
	cardinality := 1.0
	indices := schema.ColumnsIndices(cols)
	if indices == nil {
		logutil.BgLogger().Error("column not found in schema", zap.Any("columns", cols), zap.String("schema", schema.String()))
		return cardinality
	}
	for _, idx := range indices {
		// It is a very elementary estimation.
		cardinality = math.Max(cardinality, profile.Cardinality[idx])
	}
	return cardinality
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalProjection) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	childProfile := childStats[0]
	p.stats = &property.StatsInfo{
		RowCount:    childProfile.RowCount,
		Cardinality: make([]float64, len(p.Exprs)),
	}
	for i, expr := range p.Exprs {
		cols := expression.ExtractColumns(expr)
		p.stats.Cardinality[i] = getCardinality(cols, p.children[0].Schema(), childProfile)
	}
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (la *LogicalAggregation) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	childProfile := childStats[0]
	gbyCols := make([]*expression.Column, 0, len(la.GroupByItems))
	for _, gbyExpr := range la.GroupByItems {
		cols := expression.ExtractColumns(gbyExpr)
		gbyCols = append(gbyCols, cols...)
	}
	cardinality := getCardinality(gbyCols, la.children[0].Schema(), childProfile)
	la.stats = &property.StatsInfo{
		RowCount:    cardinality,
		Cardinality: make([]float64, la.schema.Len()),
	}
	// We cannot estimate the Cardinality for every output, so we use a conservative strategy.
	for i := range la.stats.Cardinality {
		la.stats.Cardinality[i] = cardinality
	}
	la.inputCount = childProfile.RowCount
	return la.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
// If the type of join is SemiJoin, the selectivity of it will be same as selection's.
// If the type of join is LeftOuterSemiJoin, it will not add or remove any row. The last column is a boolean value, whose Cardinality should be two.
// If the type of join is inner/outer join, the output of join(s, t) should be N(s) * N(t) / (V(s.key) * V(t.key)) * Min(s.key, t.key).
// N(s) stands for the number of rows in relation s. V(s.key) means the Cardinality of join key in s.
// This is a quite simple strategy: We assume every bucket of relation which will participate join has the same number of rows, and apply cross join for
// every matched bucket.
func (p *LogicalJoin) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	leftProfile, rightProfile := childStats[0], childStats[1]
	helper := &fullJoinRowCountHelper{
		cartesian:     0 == len(p.EqualConditions),
		leftProfile:   leftProfile,
		rightProfile:  rightProfile,
		leftJoinKeys:  p.LeftJoinKeys,
		rightJoinKeys: p.RightJoinKeys,
		leftSchema:    p.children[0].Schema(),
		rightSchema:   p.children[1].Schema(),
	}
	p.equalCondOutCnt = helper.estimate()
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin {
		p.stats = &property.StatsInfo{
			RowCount:    leftProfile.RowCount * selectionFactor,
			Cardinality: make([]float64, len(leftProfile.Cardinality)),
		}
		for i := range p.stats.Cardinality {
			p.stats.Cardinality[i] = leftProfile.Cardinality[i] * selectionFactor
		}
		return p.stats, nil
	}
	if p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		p.stats = &property.StatsInfo{
			RowCount:    leftProfile.RowCount,
			Cardinality: make([]float64, p.schema.Len()),
		}
		copy(p.stats.Cardinality, leftProfile.Cardinality)
		p.stats.Cardinality[len(p.stats.Cardinality)-1] = 2.0
		return p.stats, nil
	}
	count := p.equalCondOutCnt
	if p.JoinType == LeftOuterJoin {
		count = math.Max(count, leftProfile.RowCount)
	} else if p.JoinType == RightOuterJoin {
		count = math.Max(count, rightProfile.RowCount)
	}
	cardinality := make([]float64, 0, p.schema.Len())
	cardinality = append(cardinality, leftProfile.Cardinality...)
	cardinality = append(cardinality, rightProfile.Cardinality...)
	for i := range cardinality {
		cardinality[i] = math.Min(cardinality[i], count)
	}
	p.stats = &property.StatsInfo{
		RowCount:    count,
		Cardinality: cardinality,
	}
	return p.stats, nil
}

type fullJoinRowCountHelper struct {
	cartesian     bool
	leftProfile   *property.StatsInfo
	rightProfile  *property.StatsInfo
	leftJoinKeys  []*expression.Column
	rightJoinKeys []*expression.Column
	leftSchema    *expression.Schema
	rightSchema   *expression.Schema
}

func (h *fullJoinRowCountHelper) estimate() float64 {
	if h.cartesian {
		return h.leftProfile.RowCount * h.rightProfile.RowCount
	}
	leftKeyCardinality := getCardinality(h.leftJoinKeys, h.leftSchema, h.leftProfile)
	rightKeyCardinality := getCardinality(h.rightJoinKeys, h.rightSchema, h.rightProfile)
	count := h.leftProfile.RowCount * h.rightProfile.RowCount / math.Max(leftKeyCardinality, rightKeyCardinality)
	return count
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (la *LogicalApply) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	leftProfile := childStats[0]
	la.stats = &property.StatsInfo{
		RowCount:    leftProfile.RowCount,
		Cardinality: make([]float64, la.schema.Len()),
	}
	copy(la.stats.Cardinality, leftProfile.Cardinality)
	if la.JoinType == LeftOuterSemiJoin || la.JoinType == AntiLeftOuterSemiJoin {
		la.stats.Cardinality[len(la.stats.Cardinality)-1] = 2.0
	} else {
		for i := la.children[0].Schema().Len(); i < la.schema.Len(); i++ {
			la.stats.Cardinality[i] = leftProfile.RowCount
		}
	}
	return la.stats, nil
}

// Exists and MaxOneRow produce at most one row, so we set the RowCount of stats one.
func getSingletonStats(len int) *property.StatsInfo {
	ret := &property.StatsInfo{
		RowCount:    1.0,
		Cardinality: make([]float64, len),
	}
	for i := 0; i < len; i++ {
		ret.Cardinality[i] = 1
	}
	return ret
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalMaxOneRow) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	p.stats = getSingletonStats(p.Schema().Len())
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalWindow) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	childProfile := childStats[0]
	p.stats = &property.StatsInfo{
		RowCount:    childProfile.RowCount,
		Cardinality: make([]float64, p.schema.Len()),
	}
	childLen := p.schema.Len() - len(p.WindowFuncDescs)
	for i := 0; i < childLen; i++ {
		colIdx := p.children[0].Schema().ColumnIndex(p.schema.Columns[i])
		p.stats.Cardinality[i] = childProfile.Cardinality[colIdx]
	}
	for i := childLen; i < p.schema.Len(); i++ {
		p.stats.Cardinality[i] = childProfile.RowCount
	}
	return p.stats, nil
}
