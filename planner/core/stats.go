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
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"go.uber.org/zap"
)

func (p *basePhysicalPlan) StatsCount() float64 {
	return p.stats.RowCount
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalTableDual) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema) (*property.StatsInfo, error) {
	profile := &property.StatsInfo{
		RowCount:    float64(p.RowCount),
		Cardinality: make(map[int64]float64, selfSchema.Len()),
	}
	for _, col := range selfSchema.Columns {
		profile.Cardinality[col.UniqueID] = float64(p.RowCount)
	}
	p.stats = profile
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalMemTable) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema) (*property.StatsInfo, error) {
	statsTable := statistics.PseudoTable(p.TableInfo)
	stats := &property.StatsInfo{
		RowCount:     float64(statsTable.Count),
		Cardinality:  make(map[int64]float64, len(p.TableInfo.Columns)),
		HistColl:     statsTable.GenerateHistCollFromColumnInfo(p.TableInfo.Columns, p.schema.Columns),
		StatsVersion: statistics.PseudoVersion,
	}
	for _, col := range selfSchema.Columns {
		stats.Cardinality[col.UniqueID] = float64(statsTable.Count)
	}
	p.stats = stats
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalShow) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema) (*property.StatsInfo, error) {
	// A fake count, just to avoid panic now.
	p.stats = getFakeStats(selfSchema)
	return p.stats, nil
}

func getFakeStats(schema *expression.Schema) *property.StatsInfo {
	profile := &property.StatsInfo{
		RowCount:    1,
		Cardinality: make(map[int64]float64, schema.Len()),
	}
	for _, col := range schema.Columns {
		profile.Cardinality[col.UniqueID] = 1
	}
	return profile
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalShowDDLJobs) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema) (*property.StatsInfo, error) {
	// A fake count, just to avoid panic now.
	p.stats = getFakeStats(selfSchema)
	return p.stats, nil
}

func (p *baseLogicalPlan) recursiveDeriveStats() (*property.StatsInfo, error) {
	if p.stats != nil {
		return p.stats, nil
	}
	childStats := make([]*property.StatsInfo, len(p.children))
	childSchema := make([]*expression.Schema, len(p.children))
	for i, child := range p.children {
		childProfile, err := child.recursiveDeriveStats()
		if err != nil {
			return nil, err
		}
		childStats[i] = childProfile
		childSchema[i] = child.Schema()
	}
	return p.self.DeriveStats(childStats, p.self.Schema(), childSchema)
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *baseLogicalPlan) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema) (*property.StatsInfo, error) {
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
		Cardinality: make(map[int64]float64, selfSchema.Len()),
	}
	for _, col := range selfSchema.Columns {
		profile.Cardinality[col.UniqueID] = 1
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

func (ds *DataSource) initStats() {
	if ds.TableStats != nil {
		return
	}
	if ds.statisticTable == nil {
		ds.statisticTable = getStatsTable(ds.ctx, ds.tableInfo, ds.table.Meta().ID)
	}
	tableStats := &property.StatsInfo{
		RowCount:     float64(ds.statisticTable.Count),
		Cardinality:  make(map[int64]float64, ds.schema.Len()),
		HistColl:     ds.statisticTable.GenerateHistCollFromColumnInfo(ds.Columns, ds.schema.Columns),
		StatsVersion: ds.statisticTable.Version,
	}
	if ds.statisticTable.Pseudo {
		tableStats.StatsVersion = statistics.PseudoVersion
	}
	for _, col := range ds.schema.Columns {
		tableStats.Cardinality[col.UniqueID] = ds.getColumnNDV(col.ID)
	}
	ds.TableStats = tableStats
	ds.TblColHists = ds.statisticTable.ID2UniqueID(ds.TblCols)
}

func (ds *DataSource) deriveStatsByFilter(conds expression.CNFExprs, filledPaths []*util.AccessPath) *property.StatsInfo {
	ds.initStats()
	selectivity, nodes, err := ds.TableStats.HistColl.Selectivity(ds.ctx, conds, filledPaths)
	if err != nil {
		logutil.BgLogger().Debug("something wrong happened, use the default selectivity", zap.Error(err))
		selectivity = SelectionFactor
	}
	stats := ds.TableStats.Scale(selectivity)
	if ds.ctx.GetSessionVars().OptimizerSelectivityLevel >= 1 {
		stats.HistColl = stats.HistColl.NewHistCollBySelectivity(ds.ctx.GetSessionVars().StmtCtx, nodes)
	}
	return stats
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (ds *DataSource) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema) (*property.StatsInfo, error) {
	ds.initStats()
	// PushDownNot here can convert query 'not (a != 1)' to 'a = 1'.
	for i, expr := range ds.pushedDownConds {
		ds.pushedDownConds[i] = expression.PushDownNot(ds.ctx, expr)
	}
	for _, path := range ds.possibleAccessPaths {
		if path.IsTablePath {
			continue
		}
		err := ds.fillIndexPath(path, ds.pushedDownConds)
		if err != nil {
			return nil, err
		}
	}
	ds.stats = ds.deriveStatsByFilter(ds.pushedDownConds, ds.possibleAccessPaths)
	for _, path := range ds.possibleAccessPaths {
		if path.IsTablePath {
			noIntervalRanges, err := ds.deriveTablePathStats(path, ds.pushedDownConds, false)
			if err != nil {
				return nil, err
			}
			// If we have point or empty range, just remove other possible paths.
			if noIntervalRanges || len(path.Ranges) == 0 {
				ds.possibleAccessPaths[0] = path
				ds.possibleAccessPaths = ds.possibleAccessPaths[:1]
				break
			}
			continue
		}
		noIntervalRanges := ds.deriveIndexPathStats(path, ds.pushedDownConds, false)
		// If we have empty range, or point range on unique index, just remove other possible paths.
		if (noIntervalRanges && path.Index.Unique) || len(path.Ranges) == 0 {
			ds.possibleAccessPaths[0] = path
			ds.possibleAccessPaths = ds.possibleAccessPaths[:1]
			break
		}
	}

	// TODO: implement UnionScan + IndexMerge
	isReadOnlyTxn := true
	txn, err := ds.ctx.Txn(false)
	if err != nil {
		return nil, err
	}
	if txn.Valid() && !txn.IsReadOnly() {
		isReadOnlyTxn = false
	}
	// Consider the IndexMergePath. Now, we just generate `IndexMergePath` in DNF case.
	isPossibleIdxMerge := len(ds.pushedDownConds) > 0 && len(ds.possibleAccessPaths) > 1
	sessionAndStmtPermission := (ds.ctx.GetSessionVars().GetEnableIndexMerge() || ds.indexMergeHints != nil) && !ds.ctx.GetSessionVars().StmtCtx.NoIndexMergeHint
	// If there is an index path, we current do not consider `IndexMergePath`.
	needConsiderIndexMerge := true
	for i := 1; i < len(ds.possibleAccessPaths); i++ {
		if len(ds.possibleAccessPaths[i].AccessConds) != 0 {
			needConsiderIndexMerge = false
			break
		}
	}
	if isPossibleIdxMerge && sessionAndStmtPermission && needConsiderIndexMerge && isReadOnlyTxn {
		ds.generateAndPruneIndexMergePath(ds.indexMergeHints != nil)
	} else if ds.indexMergeHints != nil {
		ds.indexMergeHints = nil
		ds.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("IndexMerge is inapplicable or disabled"))
	}
	return ds.stats, nil
}

func (ds *DataSource) generateAndPruneIndexMergePath(needPrune bool) {
	regularPathCount := len(ds.possibleAccessPaths)
	ds.generateIndexMergeOrPaths()
	// If without hints, it means that `enableIndexMerge` is true
	if ds.indexMergeHints == nil {
		return
	}
	// With hints and without generated IndexMerge paths
	if regularPathCount == len(ds.possibleAccessPaths) {
		ds.indexMergeHints = nil
		ds.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("IndexMerge is inapplicable or disabled"))
		return
	}
	// Do not need to consider the regular paths in find_best_task().
	if needPrune {
		ds.possibleAccessPaths = ds.possibleAccessPaths[regularPathCount:]
	}
}

// DeriveStats implements LogicalPlan DeriveStats interface.
func (ts *LogicalTableScan) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema) (_ *property.StatsInfo, err error) {
	// PushDownNot here can convert query 'not (a != 1)' to 'a = 1'.
	for i, expr := range ts.AllConds {
		// TODO The expressions may be shared by TableScan and several IndexScans, there would be redundant
		// `PushDownNot` function call in multiple `DeriveStats` then.
		ts.AllConds[i] = expression.PushDownNot(ts.ctx, expr)
	}
	ts.stats = ts.Source.deriveStatsByFilter(ts.AllConds, nil)
	sc := ts.SCtx().GetSessionVars().StmtCtx

	// ts.Handle could be nil if PK is Handle, and PK column has been pruned.
	if ts.Handle != nil {
		ts.AccessConds, ts.FilterConds = ranger.DetachCondsForColumn(ts.SCtx(), ts.AllConds, ts.Handle)
		ts.Ranges, err = ranger.BuildTableRange(ts.AccessConds, sc, ts.Handle.RetType)
		if err != nil {
			return nil, err
		}
		ts.CountAfterAccess, err = ts.Source.statisticTable.GetRowCountByIntColumnRanges(sc, ts.Handle.ID, ts.Ranges)
		// If the `CountAfterAccess` is less than `stats.RowCount`, there must be some inconsistent stats info.
		// We prefer the `stats.RowCount` because it could use more stats info to calculate the selectivity.
		if ts.CountAfterAccess < ts.stats.RowCount {
			ts.CountAfterAccess = math.Min(ts.stats.RowCount/SelectionFactor, float64(ts.Source.statisticTable.Count))
		}
		// TODO: Handle conditions with CorrelatedColumn.
	} else {
		isUnsigned := false
		if ts.Source.tableInfo.PKIsHandle {
			if pkColInfo := ts.Source.tableInfo.GetPkColInfo(); pkColInfo != nil {
				isUnsigned = mysql.HasUnsignedFlag(pkColInfo.Flag)
			}
		}
		ts.Ranges = ranger.FullIntRange(isUnsigned)
		ts.CountAfterAccess = ts.Source.TableStats.RowCount
		ts.FilterConds = ts.AllConds
	}
	return ts.stats, nil
}

// DeriveStats implements LogicalPlan DeriveStats interface.
func (is *LogicalIndexScan) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema) (*property.StatsInfo, error) {
	for i, expr := range is.AllConds {
		is.AllConds[i] = expression.PushDownNot(is.ctx, expr)
	}
	is.stats = is.Source.deriveStatsByFilter(is.AllConds, nil)
	is.Ranges = ranger.FullRange()
	is.CountAfterAccess = float64(is.Source.statisticTable.Count)

	is.IdxCols, is.IdxColLens = expression.IndexInfo2PrefixCols(is.Columns, selfSchema.Columns, is.Index)
	is.FullIdxCols, is.FullIdxColLens = expression.IndexInfo2Cols(is.Columns, selfSchema.Columns, is.Index)
	if !is.Index.Unique && !is.Index.Primary && len(is.Index.Columns) == len(is.IdxCols) {
		handleCol := is.getPKIsHandleCol(selfSchema)
		if handleCol != nil && !mysql.HasUnsignedFlag(handleCol.RetType.Flag) {
			is.IdxCols = append(is.IdxCols, handleCol)
			is.IdxColLens = append(is.IdxColLens, types.UnspecifiedLength)
		}
	}

	if len(is.IdxCols) != 0 {
		res, err := ranger.DetachCondAndBuildRangeForIndex(is.SCtx(), is.AllConds, is.IdxCols, is.IdxColLens)
		if err != nil {
			return nil, err
		}
		is.Ranges = res.Ranges
		is.AccessConds = res.AccessConds
		is.FilterConds = res.RemainedConds
		is.EqCondCount = res.EqCondCount
		is.EqOrInCondCount = res.EqOrInCount
		// TODO: `res` still has an unused field: IsDNFCond.
		is.CountAfterAccess, err = is.Source.TableStats.HistColl.GetRowCountByIndexRanges(is.SCtx().GetSessionVars().StmtCtx, is.Index.ID, is.Ranges)
		if err != nil {
			return nil, err
		}
	} else {
		is.FilterConds = is.AllConds
	}
	return is.stats, nil
}

// getIndexMergeOrPath generates all possible IndexMergeOrPaths.
func (ds *DataSource) generateIndexMergeOrPaths() {
	usedIndexCount := len(ds.possibleAccessPaths)
	for i, cond := range ds.pushedDownConds {
		sf, ok := cond.(*expression.ScalarFunction)
		if !ok || sf.FuncName.L != ast.LogicOr {
			continue
		}
		var partialPaths = make([]*util.AccessPath, 0, usedIndexCount)
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

// isInIndexMergeHints checks whether current index or primary key is in IndexMerge hints.
func (ds *DataSource) isInIndexMergeHints(name string) bool {
	if ds.indexMergeHints == nil {
		return true
	}
	for _, hint := range ds.indexMergeHints {
		if hint.IndexNames == nil {
			return true
		}
		for _, index := range hint.IndexNames {
			if name == index.L {
				return true
			}
		}
	}
	return false
}

// accessPathsForConds generates all possible index paths for conditions.
func (ds *DataSource) accessPathsForConds(conditions []expression.Expression, usedIndexCount int) []*util.AccessPath {
	var results = make([]*util.AccessPath, 0, usedIndexCount)
	for i := 0; i < usedIndexCount; i++ {
		path := &util.AccessPath{}
		if ds.possibleAccessPaths[i].IsTablePath {
			if !ds.isInIndexMergeHints("primary") {
				continue
			}
			path.IsTablePath = true
			noIntervalRanges, err := ds.deriveTablePathStats(path, conditions, true)
			if err != nil {
				logutil.BgLogger().Debug("can not derive statistics of a path", zap.Error(err))
				continue
			}
			// If we have point or empty range, just remove other possible paths.
			if noIntervalRanges || len(path.Ranges) == 0 {
				if len(results) == 0 {
					results = append(results, path)
				} else {
					results[0] = path
					results = results[:1]
				}
				break
			}
		} else {
			path.Index = ds.possibleAccessPaths[i].Index
			if !ds.isInIndexMergeHints(path.Index.Name.L) {
				continue
			}
			err := ds.fillIndexPath(path, conditions)
			if err != nil {
				logutil.BgLogger().Debug("can not derive statistics of a path", zap.Error(err))
				continue
			}
			noIntervalRanges := ds.deriveIndexPathStats(path, conditions, true)
			// If we have empty range, or point range on unique index, just remove other possible paths.
			if (noIntervalRanges && path.Index.Unique) || len(path.Ranges) == 0 {
				if len(results) == 0 {
					results = append(results, path)
				} else {
					results[0] = path
					results = results[:1]
				}
				break
			}
		}
		// If AccessConds is empty or tableFilter is not empty, we ignore the access path.
		// Now these conditions are too strict.
		// For example, a sql `select * from t where a > 1 or (b < 2 and c > 3)` and table `t` with indexes
		// on a and b separately. we can generate a `IndexMergePath` with table filter `a > 1 or (b < 2 and c > 3)`.
		// TODO: solve the above case
		if len(path.TableFilters) > 0 || len(path.AccessConds) == 0 {
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
func (ds *DataSource) buildIndexMergePartialPath(indexAccessPaths []*util.AccessPath) *util.AccessPath {
	if len(indexAccessPaths) == 1 {
		return indexAccessPaths[0]
	}

	maxColsIndex := 0
	maxCols := len(indexAccessPaths[0].IdxCols)
	for i := 1; i < len(indexAccessPaths); i++ {
		current := len(indexAccessPaths[i].IdxCols)
		if current > maxCols {
			maxColsIndex = i
			maxCols = current
		}
	}
	return indexAccessPaths[maxColsIndex]
}

// buildIndexMergeOrPath generates one possible IndexMergePath.
func (ds *DataSource) buildIndexMergeOrPath(partialPaths []*util.AccessPath, current int) *util.AccessPath {
	indexMergePath := &util.AccessPath{PartialIndexPaths: partialPaths}
	indexMergePath.TableFilters = append(indexMergePath.TableFilters, ds.pushedDownConds[:current]...)
	indexMergePath.TableFilters = append(indexMergePath.TableFilters, ds.pushedDownConds[current+1:]...)
	return indexMergePath
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalSelection) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema) (*property.StatsInfo, error) {
	p.stats = childStats[0].Scale(SelectionFactor)
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalUnionAll) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema) (*property.StatsInfo, error) {
	p.stats = &property.StatsInfo{
		Cardinality: make(map[int64]float64, selfSchema.Len()),
	}
	for _, childProfile := range childStats {
		p.stats.RowCount += childProfile.RowCount
		for _, col := range selfSchema.Columns {
			p.stats.Cardinality[col.UniqueID] += childProfile.Cardinality[col.UniqueID]
		}
	}
	return p.stats, nil
}

func deriveLimitStats(childProfile *property.StatsInfo, limitCount float64) *property.StatsInfo {
	stats := &property.StatsInfo{
		RowCount:    math.Min(limitCount, childProfile.RowCount),
		Cardinality: make(map[int64]float64, len(childProfile.Cardinality)),
	}
	for id, c := range childProfile.Cardinality {
		stats.Cardinality[id] = math.Min(c, stats.RowCount)
	}
	return stats
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalLimit) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema) (*property.StatsInfo, error) {
	p.stats = deriveLimitStats(childStats[0], float64(p.Count))
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (lt *LogicalTopN) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema) (*property.StatsInfo, error) {
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
		col := schema.Columns[idx]
		cardinality = math.Max(cardinality, profile.Cardinality[col.UniqueID])
	}
	return cardinality
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalProjection) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema) (*property.StatsInfo, error) {
	childProfile := childStats[0]
	p.stats = &property.StatsInfo{
		RowCount:    childProfile.RowCount,
		Cardinality: make(map[int64]float64, len(p.Exprs)),
	}
	for i, expr := range p.Exprs {
		cols := expression.ExtractColumns(expr)
		p.stats.Cardinality[selfSchema.Columns[i].UniqueID] = getCardinality(cols, childSchema[0], childProfile)
	}
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (la *LogicalAggregation) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema) (*property.StatsInfo, error) {
	childProfile := childStats[0]
	gbyCols := make([]*expression.Column, 0, len(la.GroupByItems))
	for _, gbyExpr := range la.GroupByItems {
		cols := expression.ExtractColumns(gbyExpr)
		gbyCols = append(gbyCols, cols...)
	}
	cardinality := getCardinality(gbyCols, childSchema[0], childProfile)
	la.stats = &property.StatsInfo{
		RowCount:    cardinality,
		Cardinality: make(map[int64]float64, selfSchema.Len()),
	}
	// We cannot estimate the Cardinality for every output, so we use a conservative strategy.
	for _, col := range selfSchema.Columns {
		la.stats.Cardinality[col.UniqueID] = cardinality
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
func (p *LogicalJoin) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema) (*property.StatsInfo, error) {
	leftProfile, rightProfile := childStats[0], childStats[1]
	leftJoinKeys, rightJoinKeys := p.GetJoinKeys()
	helper := &fullJoinRowCountHelper{
		cartesian:     0 == len(p.EqualConditions),
		leftProfile:   leftProfile,
		rightProfile:  rightProfile,
		leftJoinKeys:  leftJoinKeys,
		rightJoinKeys: rightJoinKeys,
		leftSchema:    childSchema[0],
		rightSchema:   childSchema[1],
	}
	p.EqualCondOutCnt = helper.estimate()
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin {
		p.stats = &property.StatsInfo{
			RowCount:    leftProfile.RowCount * SelectionFactor,
			Cardinality: make(map[int64]float64, len(leftProfile.Cardinality)),
		}
		for id, c := range leftProfile.Cardinality {
			p.stats.Cardinality[id] = c * SelectionFactor
		}
		return p.stats, nil
	}
	if p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		p.stats = &property.StatsInfo{
			RowCount:    leftProfile.RowCount,
			Cardinality: make(map[int64]float64, selfSchema.Len()),
		}
		for id, c := range leftProfile.Cardinality {
			p.stats.Cardinality[id] = c
		}
		p.stats.Cardinality[selfSchema.Columns[selfSchema.Len()-1].UniqueID] = 2.0
		return p.stats, nil
	}
	count := p.EqualCondOutCnt
	if p.JoinType == LeftOuterJoin {
		count = math.Max(count, leftProfile.RowCount)
	} else if p.JoinType == RightOuterJoin {
		count = math.Max(count, rightProfile.RowCount)
	}
	cardinality := make(map[int64]float64, selfSchema.Len())
	for id, c := range leftProfile.Cardinality {
		cardinality[id] = math.Min(c, count)
	}
	for id, c := range rightProfile.Cardinality {
		cardinality[id] = math.Min(c, count)
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
func (la *LogicalApply) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema) (*property.StatsInfo, error) {
	leftProfile := childStats[0]
	la.stats = &property.StatsInfo{
		RowCount:    leftProfile.RowCount,
		Cardinality: make(map[int64]float64, selfSchema.Len()),
	}
	for id, c := range leftProfile.Cardinality {
		la.stats.Cardinality[id] = c
	}
	if la.JoinType == LeftOuterSemiJoin || la.JoinType == AntiLeftOuterSemiJoin {
		la.stats.Cardinality[selfSchema.Columns[selfSchema.Len()-1].UniqueID] = 2.0
	} else {
		for i := childSchema[0].Len(); i < selfSchema.Len(); i++ {
			la.stats.Cardinality[selfSchema.Columns[i].UniqueID] = leftProfile.RowCount
		}
	}
	return la.stats, nil
}

// Exists and MaxOneRow produce at most one row, so we set the RowCount of stats one.
func getSingletonStats(schema *expression.Schema) *property.StatsInfo {
	ret := &property.StatsInfo{
		RowCount:    1.0,
		Cardinality: make(map[int64]float64, schema.Len()),
	}
	for _, col := range schema.Columns {
		ret.Cardinality[col.UniqueID] = 1
	}
	return ret
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalMaxOneRow) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema) (*property.StatsInfo, error) {
	p.stats = getSingletonStats(selfSchema)
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalWindow) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema) (*property.StatsInfo, error) {
	childProfile := childStats[0]
	p.stats = &property.StatsInfo{
		RowCount:    childProfile.RowCount,
		Cardinality: make(map[int64]float64, selfSchema.Len()),
	}
	childLen := selfSchema.Len() - len(p.WindowFuncDescs)
	for i := 0; i < childLen; i++ {
		id := selfSchema.Columns[i].UniqueID
		p.stats.Cardinality[id] = childProfile.Cardinality[id]
	}
	for i := childLen; i < selfSchema.Len(); i++ {
		p.stats.Cardinality[selfSchema.Columns[i].UniqueID] = childProfile.RowCount
	}
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *TiKVDoubleGather) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema) (*property.StatsInfo, error) {
	if len(childStats) != 2 {
		err := ErrInternal.GenWithStack("There are no enough children stats during deriving stats for TiKVDoubleGather.")
		return nil, err
	}
	p.stats = childStats[1]
	return p.stats, nil
}
