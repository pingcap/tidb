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
	"sort"

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
func (p *LogicalTableDual) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.Column) (*property.StatsInfo, error) {
	if p.stats != nil {
		return p.stats, nil
	}
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
func (p *LogicalMemTable) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.Column) (*property.StatsInfo, error) {
	if p.stats != nil {
		return p.stats, nil
	}
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
func (p *LogicalShow) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.Column) (*property.StatsInfo, error) {
	if p.stats != nil {
		return p.stats, nil
	}
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
func (p *LogicalShowDDLJobs) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.Column) (*property.StatsInfo, error) {
	if p.stats != nil {
		return p.stats, nil
	}
	// A fake count, just to avoid panic now.
	p.stats = getFakeStats(selfSchema)
	return p.stats, nil
}

// RecursiveDeriveStats4Test is a exporter just for test.
func RecursiveDeriveStats4Test(p LogicalPlan) (*property.StatsInfo, error) {
	return p.recursiveDeriveStats(nil)
}

// GetStats4Test is a exporter just for test.
func GetStats4Test(p LogicalPlan) *property.StatsInfo {
	return p.statsInfo()
}

func (p *baseLogicalPlan) recursiveDeriveStats(colGroups [][]*expression.Column) (*property.StatsInfo, error) {
	childStats := make([]*property.StatsInfo, len(p.children))
	childSchema := make([]*expression.Schema, len(p.children))
	cumColGroups := p.self.ExtractColGroups(colGroups)
	for i, child := range p.children {
		childProfile, err := child.recursiveDeriveStats(cumColGroups)
		if err != nil {
			return nil, err
		}
		childStats[i] = childProfile
		childSchema[i] = child.Schema()
	}
	return p.self.DeriveStats(childStats, p.self.Schema(), childSchema, colGroups)
}

// ExtractColGroups implements LogicalPlan ExtractColGroups interface.
func (p *baseLogicalPlan) ExtractColGroups(_ [][]*expression.Column) [][]*expression.Column {
	return nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *baseLogicalPlan) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.Column) (*property.StatsInfo, error) {
	if len(childStats) == 1 {
		p.stats = childStats[0]
		return p.stats, nil
	}
	if len(childStats) > 1 {
		err := ErrInternal.GenWithStack("LogicalPlans with more than one child should implement their own DeriveStats().")
		return nil, err
	}
	if p.stats != nil {
		return p.stats, nil
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

func (ds *DataSource) getGroupNDVs(colGroups [][]*expression.Column) []property.GroupNDV {
	if colGroups == nil {
		return nil
	}
	tbl := ds.tableStats.HistColl
	ndvs := make([]property.GroupNDV, 0, len(colGroups))
	for idxID, idx := range tbl.Indices {
		colsLen := len(tbl.Idx2ColumnIDs[idxID])
		// tbl.Idx2ColumnIDs may only contain the prefix of index columns.
		if colsLen != len(idx.Info.Columns) {
			continue
		}
		idxCols := make([]int64, colsLen)
		copy(idxCols, tbl.Idx2ColumnIDs[idxID])
		sort.Slice(idxCols, func(i, j int) bool {
			return idxCols[i] < idxCols[j]
		})
		for _, g := range colGroups {
			// We only want those exact matches.
			if len(g) != colsLen {
				continue
			}
			match := true
			for i, col := range g {
				// Both slices are sorted according to UniqueID.
				if col.UniqueID != idxCols[i] {
					match = false
					break
				}
			}
			if match {
				ndv := property.GroupNDV{
					Cols: idxCols,
					NDV:  float64(idx.NDV),
				}
				ndvs = append(ndvs, ndv)
				break
			}
		}
	}
	return ndvs
}

func (ds *DataSource) initStats(colGroups [][]*expression.Column) {
	if ds.tableStats != nil {
		// Reload GroupNDVs since colGroups may have changed.
		ds.tableStats.GroupNDVs = ds.getGroupNDVs(colGroups)
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
	ds.tableStats = tableStats
	ds.tableStats.GroupNDVs = ds.getGroupNDVs(colGroups)
	ds.TblColHists = ds.statisticTable.ID2UniqueID(ds.TblCols)
}

func (ds *DataSource) deriveStatsByFilter(conds expression.CNFExprs, filledPaths []*util.AccessPath) *property.StatsInfo {
	selectivity, nodes, err := ds.tableStats.HistColl.Selectivity(ds.ctx, conds, filledPaths)
	if err != nil {
		logutil.BgLogger().Debug("something wrong happened, use the default selectivity", zap.Error(err))
		selectivity = SelectionFactor
	}
	stats := ds.tableStats.Scale(selectivity)
	if ds.ctx.GetSessionVars().OptimizerSelectivityLevel >= 1 {
		stats.HistColl = stats.HistColl.NewHistCollBySelectivity(ds.ctx.GetSessionVars().StmtCtx, nodes)
	}
	return stats
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (ds *DataSource) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, colGroups [][]*expression.Column) (*property.StatsInfo, error) {
	if ds.stats != nil && len(colGroups) == 0 {
		return ds.stats, nil
	}
	ds.initStats(colGroups)
	if ds.stats != nil {
		// Just reload the GroupNDVs.
		selectivity := ds.stats.RowCount / ds.tableStats.RowCount
		ds.stats = ds.tableStats.Scale(selectivity)
		return ds.stats, nil
	}
	// PushDownNot here can convert query 'not (a != 1)' to 'a = 1'.
	for i, expr := range ds.pushedDownConds {
		ds.pushedDownConds[i] = expression.PushDownNot(ds.ctx, expr)
	}
	for _, path := range ds.possibleAccessPaths {
		if path.IsTablePath() {
			continue
		}
		err := ds.fillIndexPath(path, ds.pushedDownConds)
		if err != nil {
			return nil, err
		}
	}
	ds.stats = ds.deriveStatsByFilter(ds.pushedDownConds, ds.possibleAccessPaths)
	for _, path := range ds.possibleAccessPaths {
		if path.IsTablePath() {
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
	sessionAndStmtPermission := (ds.ctx.GetSessionVars().GetEnableIndexMerge() || len(ds.indexMergeHints) > 0) && !ds.ctx.GetSessionVars().StmtCtx.NoIndexMergeHint
	// If there is an index path, we current do not consider `IndexMergePath`.
	needConsiderIndexMerge := true
	if len(ds.indexMergeHints) == 0 {
		for i := 1; i < len(ds.possibleAccessPaths); i++ {
			if len(ds.possibleAccessPaths[i].AccessConds) != 0 {
				needConsiderIndexMerge = false
				break
			}
		}
	}
	if isPossibleIdxMerge && sessionAndStmtPermission && needConsiderIndexMerge && isReadOnlyTxn {
		ds.generateAndPruneIndexMergePath(ds.indexMergeHints != nil)
	} else if len(ds.indexMergeHints) > 0 {
		ds.indexMergeHints = nil
		ds.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("IndexMerge is inapplicable or disabled"))
	}
	return ds.stats, nil
}

func (ds *DataSource) generateAndPruneIndexMergePath(needPrune bool) {
	regularPathCount := len(ds.possibleAccessPaths)
	ds.generateIndexMergeOrPaths()
	// If without hints, it means that `enableIndexMerge` is true
	if len(ds.indexMergeHints) == 0 {
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
func (ts *LogicalTableScan) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.Column) (_ *property.StatsInfo, err error) {
	ts.Source.initStats(nil)
	// PushDownNot here can convert query 'not (a != 1)' to 'a = 1'.
	for i, expr := range ts.AccessConds {
		// TODO The expressions may be shared by TableScan and several IndexScans, there would be redundant
		// `PushDownNot` function call in multiple `DeriveStats` then.
		ts.AccessConds[i] = expression.PushDownNot(ts.ctx, expr)
	}
	ts.stats = ts.Source.deriveStatsByFilter(ts.AccessConds, nil)
	sc := ts.SCtx().GetSessionVars().StmtCtx
	// ts.Handle could be nil if PK is Handle, and PK column has been pruned.
	// TODO: support clustered index.
	if ts.HandleCols != nil {
		ts.Ranges, err = ranger.BuildTableRange(ts.AccessConds, sc, ts.HandleCols.GetCol(0).RetType)
	} else {
		isUnsigned := false
		if ts.Source.tableInfo.PKIsHandle {
			if pkColInfo := ts.Source.tableInfo.GetPkColInfo(); pkColInfo != nil {
				isUnsigned = mysql.HasUnsignedFlag(pkColInfo.Flag)
			}
		}
		ts.Ranges = ranger.FullIntRange(isUnsigned)
	}
	if err != nil {
		return nil, err
	}
	return ts.stats, nil
}

// DeriveStats implements LogicalPlan DeriveStats interface.
func (is *LogicalIndexScan) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.Column) (*property.StatsInfo, error) {
	is.Source.initStats(nil)
	for i, expr := range is.AccessConds {
		is.AccessConds[i] = expression.PushDownNot(is.ctx, expr)
	}
	is.stats = is.Source.deriveStatsByFilter(is.AccessConds, nil)
	if len(is.AccessConds) == 0 {
		is.Ranges = ranger.FullRange()
	}
	is.IdxCols, is.IdxColLens = expression.IndexInfo2PrefixCols(is.Columns, selfSchema.Columns, is.Index)
	is.FullIdxCols, is.FullIdxColLens = expression.IndexInfo2Cols(is.Columns, selfSchema.Columns, is.Index)
	if !is.Index.Unique && !is.Index.Primary && len(is.Index.Columns) == len(is.IdxCols) {
		handleCol := is.getPKIsHandleCol(selfSchema)
		if handleCol != nil && !mysql.HasUnsignedFlag(handleCol.RetType.Flag) {
			is.IdxCols = append(is.IdxCols, handleCol)
			is.IdxColLens = append(is.IdxColLens, types.UnspecifiedLength)
		}
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

			accessConds := make([]expression.Expression, 0, len(partialPaths))
			for _, p := range partialPaths {
				accessConds = append(accessConds, p.AccessConds...)
			}
			accessDNF := expression.ComposeDNFCondition(ds.ctx, accessConds...)
			sel, _, err := ds.tableStats.HistColl.Selectivity(ds.ctx, []expression.Expression{accessDNF}, nil)
			if err != nil {
				logutil.BgLogger().Debug("something wrong happened, use the default selectivity", zap.Error(err))
				sel = SelectionFactor
			}
			possiblePath.CountAfterAccess = sel * ds.tableStats.RowCount
			ds.possibleAccessPaths = append(ds.possibleAccessPaths, possiblePath)
		}
	}
}

// isInIndexMergeHints checks whether current index or primary key is in IndexMerge hints.
func (ds *DataSource) isInIndexMergeHints(name string) bool {
	if len(ds.indexMergeHints) == 0 {
		return true
	}
	for _, hint := range ds.indexMergeHints {
		if hint.indexHint == nil || len(hint.indexHint.IndexNames) == 0 {
			return true
		}
		for _, hintName := range hint.indexHint.IndexNames {
			if name == hintName.String() {
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
		if ds.possibleAccessPaths[i].IsTablePath() {
			if !ds.isInIndexMergeHints("primary") {
				continue
			}
			if ds.tableInfo.IsCommonHandle {
				path.IsCommonHandlePath = true
				path.Index = ds.possibleAccessPaths[i].Index
			} else {
				path.IsIntHandlePath = true
			}
			noIntervalRanges, err := ds.deriveTablePathStats(path, conditions, true)
			if err != nil {
				logutil.BgLogger().Debug("can not derive statistics of a path", zap.Error(err))
				continue
			}
			if len(path.AccessConds) == 0 {
				// If AccessConds is empty, we ignore the access path.
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
			if len(path.AccessConds) == 0 {
				// If AccessConds is empty, we ignore the access path.
				continue
			}
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
	for _, path := range partialPaths {
		if len(path.TableFilters) > 0 {
			indexMergePath.TableFilters = append(indexMergePath.TableFilters, path.TableFilters...)
		}
	}
	return indexMergePath
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalSelection) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.Column) (*property.StatsInfo, error) {
	if p.stats != nil {
		return p.stats, nil
	}
	p.stats = childStats[0].Scale(SelectionFactor)
	p.stats.GroupNDVs = nil
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalUnionAll) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.Column) (*property.StatsInfo, error) {
	if p.stats != nil {
		return p.stats, nil
	}
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
func (p *LogicalLimit) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.Column) (*property.StatsInfo, error) {
	if p.stats != nil {
		return p.stats, nil
	}
	p.stats = deriveLimitStats(childStats[0], float64(p.Count))
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (lt *LogicalTopN) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.Column) (*property.StatsInfo, error) {
	if lt.stats != nil {
		return lt.stats, nil
	}
	lt.stats = deriveLimitStats(childStats[0], float64(lt.Count))
	return lt.stats, nil
}

func getGroupNDV4Cols(cols []*expression.Column, stats *property.StatsInfo) *property.GroupNDV {
	if len(cols) == 0 || len(stats.GroupNDVs) == 0 {
		return nil
	}
	cols = expression.SortColumns(cols)
	for _, groupNDV := range stats.GroupNDVs {
		if len(cols) != len(groupNDV.Cols) {
			continue
		}
		match := true
		for i, col := range groupNDV.Cols {
			if col != cols[i].UniqueID {
				match = false
				break
			}
		}
		if match {
			return &groupNDV
		}
	}
	return nil
}

// getCardinality returns the Cardinality of a couple of columns.
// If the columns match any GroupNDV maintained by child operator, we can get an accurate cardinality.
// Otherwise, we simply return the max cardinality among the columns, which is a lower bound.
func getCardinality(cols []*expression.Column, schema *expression.Schema, profile *property.StatsInfo) float64 {
	cardinality := 1.0
	if groupNDV := getGroupNDV4Cols(cols, profile); groupNDV != nil {
		return math.Max(groupNDV.NDV, cardinality)
	}
	indices := schema.ColumnsIndices(cols)
	if indices == nil {
		logutil.BgLogger().Error("column not found in schema", zap.Any("columns", cols), zap.String("schema", schema.String()))
		return cardinality
	}
	for _, idx := range indices {
		// It is a very naive estimation.
		col := schema.Columns[idx]
		cardinality = math.Max(cardinality, profile.Cardinality[col.UniqueID])
	}
	return cardinality
}

func (p *LogicalProjection) getGroupNDVs(colGroups [][]*expression.Column, childProfile *property.StatsInfo, selfSchema *expression.Schema) []property.GroupNDV {
	if len(colGroups) == 0 || len(childProfile.GroupNDVs) == 0 {
		return nil
	}
	exprCol2ProjCol := make(map[int64]int64)
	for i, expr := range p.Exprs {
		exprCol, ok := expr.(*expression.Column)
		if !ok {
			continue
		}
		exprCol2ProjCol[exprCol.UniqueID] = selfSchema.Columns[i].UniqueID
	}
	ndvs := make([]property.GroupNDV, 0, len(childProfile.GroupNDVs))
	for _, childGroupNDV := range childProfile.GroupNDVs {
		projCols := make([]int64, len(childGroupNDV.Cols))
		for i, col := range childGroupNDV.Cols {
			projCol, ok := exprCol2ProjCol[col]
			if !ok {
				projCols = nil
				break
			}
			projCols[i] = projCol
		}
		if projCols == nil {
			continue
		}
		sort.Slice(projCols, func(i, j int) bool {
			return projCols[i] < projCols[j]
		})
		groupNDV := property.GroupNDV{
			Cols: projCols,
			NDV:  childGroupNDV.NDV,
		}
		ndvs = append(ndvs, groupNDV)
	}
	return ndvs
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalProjection) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, colGroups [][]*expression.Column) (*property.StatsInfo, error) {
	childProfile := childStats[0]
	if p.stats != nil {
		// Reload GroupNDVs since colGroups may have changed.
		p.stats.GroupNDVs = p.getGroupNDVs(colGroups, childProfile, selfSchema)
		return p.stats, nil
	}
	p.stats = &property.StatsInfo{
		RowCount:    childProfile.RowCount,
		Cardinality: make(map[int64]float64, len(p.Exprs)),
	}
	for i, expr := range p.Exprs {
		cols := expression.ExtractColumns(expr)
		p.stats.Cardinality[selfSchema.Columns[i].UniqueID] = getCardinality(cols, childSchema[0], childProfile)
	}
	p.stats.GroupNDVs = p.getGroupNDVs(colGroups, childProfile, selfSchema)
	return p.stats, nil
}

// ExtractColGroups implements LogicalPlan ExtractColGroups interface.
func (p *LogicalProjection) ExtractColGroups(colGroups [][]*expression.Column) [][]*expression.Column {
	if len(colGroups) == 0 {
		return nil
	}
	extColGroups, _ := p.Schema().ExtractColGroups(colGroups)
	if len(extColGroups) == 0 {
		return nil
	}
	extracted := make([][]*expression.Column, 0, len(extColGroups))
	for _, cols := range extColGroups {
		exprs := make([]*expression.Column, len(cols))
		allCols := true
		for i, offset := range cols {
			col, ok := p.Exprs[offset].(*expression.Column)
			// TODO: for functional dependent projections like `col1 + 1` -> `col2`, we can maintain GroupNDVs actually.
			if !ok {
				allCols = false
				break
			}
			exprs[i] = col
		}
		if allCols {
			extracted = append(extracted, expression.SortColumns(exprs))
		}
	}
	return extracted
}

func (la *LogicalAggregation) getGroupNDVs(colGroups [][]*expression.Column, childProfile *property.StatsInfo, gbyCols []*expression.Column) []property.GroupNDV {
	if len(colGroups) == 0 {
		return nil
	}
	// Check if the child profile provides GroupNDV for the GROUP BY columns.
	// Note that gbyCols may not be the exact GROUP BY columns, e.g, GROUP BY a+b,
	// but we have no other approaches for the cardinality estimation of these cases
	// except for using the independent assumption, unless we can use stats of expression index.
	groupNDV := getGroupNDV4Cols(gbyCols, childProfile)
	if groupNDV == nil {
		return nil
	}
	return []property.GroupNDV{*groupNDV}
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (la *LogicalAggregation) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, colGroups [][]*expression.Column) (*property.StatsInfo, error) {
	childProfile := childStats[0]
	gbyCols := make([]*expression.Column, 0, len(la.GroupByItems))
	for _, gbyExpr := range la.GroupByItems {
		cols := expression.ExtractColumns(gbyExpr)
		gbyCols = append(gbyCols, cols...)
	}
	if la.stats != nil {
		// Reload GroupNDVs since colGroups may have changed.
		la.stats.GroupNDVs = la.getGroupNDVs(colGroups, childProfile, gbyCols)
		return la.stats, nil
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
	la.stats.GroupNDVs = la.getGroupNDVs(colGroups, childProfile, gbyCols)
	return la.stats, nil
}

// ExtractColGroups implements LogicalPlan ExtractColGroups interface.
func (la *LogicalAggregation) ExtractColGroups(_ [][]*expression.Column) [][]*expression.Column {
	// Parent colGroups would be dicarded, because aggregation would make NDV of colGroups
	// which does not match GroupByItems invalid.
	// Note that gbyCols may not be the exact GROUP BY columns, e.g, GROUP BY a+b,
	// but we have no other approaches for the cardinality estimation of these cases
	// except for using the independent assumption, unless we can use stats of expression index.
	gbyCols := make([]*expression.Column, 0, len(la.GroupByItems))
	for _, gbyExpr := range la.GroupByItems {
		cols := expression.ExtractColumns(gbyExpr)
		gbyCols = append(gbyCols, cols...)
	}
	if len(gbyCols) > 1 {
		return [][]*expression.Column{expression.SortColumns(gbyCols)}
	}
	return nil
}

func (p *LogicalJoin) getGroupNDVs(colGroups [][]*expression.Column, childStats []*property.StatsInfo) []property.GroupNDV {
	outerIdx := int(-1)
	if p.JoinType == LeftOuterJoin || p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		outerIdx = 0
	} else if p.JoinType == RightOuterJoin {
		outerIdx = 1
	}
	if outerIdx >= 0 && len(colGroups) > 0 {
		return childStats[outerIdx].GroupNDVs
	}
	return nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
// If the type of join is SemiJoin, the selectivity of it will be same as selection's.
// If the type of join is LeftOuterSemiJoin, it will not add or remove any row. The last column is a boolean value, whose Cardinality should be two.
// If the type of join is inner/outer join, the output of join(s, t) should be N(s) * N(t) / (V(s.key) * V(t.key)) * Min(s.key, t.key).
// N(s) stands for the number of rows in relation s. V(s.key) means the Cardinality of join key in s.
// This is a quite simple strategy: We assume every bucket of relation which will participate join has the same number of rows, and apply cross join for
// every matched bucket.
func (p *LogicalJoin) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, colGroups [][]*expression.Column) (*property.StatsInfo, error) {
	if p.stats != nil {
		// Reload GroupNDVs since colGroups may have changed.
		p.stats.GroupNDVs = p.getGroupNDVs(colGroups, childStats)
		return p.stats, nil
	}
	leftProfile, rightProfile := childStats[0], childStats[1]
	leftJoinKeys, rightJoinKeys, _, _ := p.GetJoinKeys()
	helper := &fullJoinRowCountHelper{
		cartesian:     0 == len(p.EqualConditions),
		leftProfile:   leftProfile,
		rightProfile:  rightProfile,
		leftJoinKeys:  leftJoinKeys,
		rightJoinKeys: rightJoinKeys,
		leftSchema:    childSchema[0],
		rightSchema:   childSchema[1],
	}
	p.equalCondOutCnt = helper.estimate()
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
		p.stats.GroupNDVs = p.getGroupNDVs(colGroups, childStats)
		return p.stats, nil
	}
	count := p.equalCondOutCnt
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
	p.stats.GroupNDVs = p.getGroupNDVs(colGroups, childStats)
	return p.stats, nil
}

// ExtractColGroups implements LogicalPlan ExtractColGroups interface.
func (p *LogicalJoin) ExtractColGroups(colGroups [][]*expression.Column) [][]*expression.Column {
	leftJoinKeys, rightJoinKeys, _, _ := p.GetJoinKeys()
	extracted := make([][]*expression.Column, 0, 2+len(colGroups))
	if len(leftJoinKeys) > 1 && (p.JoinType == InnerJoin || p.JoinType == LeftOuterJoin || p.JoinType == RightOuterJoin) {
		extracted = append(extracted, expression.SortColumns(leftJoinKeys), expression.SortColumns(rightJoinKeys))
	}
	var outerSchema *expression.Schema
	if p.JoinType == LeftOuterJoin || p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		outerSchema = p.Children()[0].Schema()
	} else if p.JoinType == RightOuterJoin {
		outerSchema = p.Children()[1].Schema()
	}
	if len(colGroups) == 0 || outerSchema == nil {
		return extracted
	}
	_, offsets := outerSchema.ExtractColGroups(colGroups)
	if len(offsets) == 0 {
		return extracted
	}
	for _, offset := range offsets {
		extracted = append(extracted, colGroups[offset])
	}
	return extracted
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

func (la *LogicalApply) getGroupNDVs(colGroups [][]*expression.Column, childStats []*property.StatsInfo) []property.GroupNDV {
	if len(colGroups) > 0 && (la.JoinType == LeftOuterSemiJoin || la.JoinType == AntiLeftOuterSemiJoin || la.JoinType == LeftOuterJoin) {
		return childStats[0].GroupNDVs
	}
	return nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (la *LogicalApply) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, colGroups [][]*expression.Column) (*property.StatsInfo, error) {
	if la.stats != nil {
		// Reload GroupNDVs since colGroups may have changed.
		la.stats.GroupNDVs = la.getGroupNDVs(colGroups, childStats)
		return la.stats, nil
	}
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
	la.stats.GroupNDVs = la.getGroupNDVs(colGroups, childStats)
	return la.stats, nil
}

// ExtractColGroups implements LogicalPlan ExtractColGroups interface.
func (la *LogicalApply) ExtractColGroups(colGroups [][]*expression.Column) [][]*expression.Column {
	var outerSchema *expression.Schema
	// Apply doesn't have RightOuterJoin.
	if la.JoinType == LeftOuterJoin || la.JoinType == LeftOuterSemiJoin || la.JoinType == AntiLeftOuterSemiJoin {
		outerSchema = la.Children()[0].Schema()
	}
	if len(colGroups) == 0 || outerSchema == nil {
		return nil
	}
	_, offsets := outerSchema.ExtractColGroups(colGroups)
	if len(offsets) == 0 {
		return nil
	}
	extracted := make([][]*expression.Column, len(offsets))
	for i, offset := range offsets {
		extracted[i] = colGroups[offset]
	}
	return extracted
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
func (p *LogicalMaxOneRow) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.Column) (*property.StatsInfo, error) {
	if p.stats != nil {
		return p.stats, nil
	}
	p.stats = getSingletonStats(selfSchema)
	return p.stats, nil
}

func (p *LogicalWindow) getGroupNDVs(colGroups [][]*expression.Column, childStats []*property.StatsInfo) []property.GroupNDV {
	if len(colGroups) > 0 {
		return childStats[0].GroupNDVs
	}
	return nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalWindow) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, colGroups [][]*expression.Column) (*property.StatsInfo, error) {
	if p.stats != nil {
		// Reload GroupNDVs since colGroups may have changed.
		p.stats.GroupNDVs = p.getGroupNDVs(colGroups, childStats)
		return p.stats, nil
	}
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
	p.stats.GroupNDVs = p.getGroupNDVs(colGroups, childStats)
	return p.stats, nil
}

// ExtractColGroups implements LogicalPlan ExtractColGroups interface.
func (p *LogicalWindow) ExtractColGroups(colGroups [][]*expression.Column) [][]*expression.Column {
	if len(colGroups) == 0 {
		return nil
	}
	childSchema := p.Children()[0].Schema()
	_, offsets := childSchema.ExtractColGroups(colGroups)
	if len(offsets) == 0 {
		return nil
	}
	extracted := make([][]*expression.Column, len(offsets))
	for i, offset := range offsets {
		extracted[i] = colGroups[offset]
	}
	return extracted
}
