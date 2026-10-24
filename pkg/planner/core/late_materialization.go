// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"fmt"
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/set"
)

// lateMaterializationName names the optimization in mysql.opt_rule_blacklist,
// which turns it off.
const lateMaterializationName = "late_materialization"

// Late materialization defers reading a table's rows until after the operators
// that discard rows.
//
// In a region of joins and filters, optionally topped by a Limit or TopN, a
// table's row is needed for two kinds of columns: those that decide which rows
// qualify and in what order (filters, join keys, ORDER BY items), and those
// that are only output. When the first kind is covered by one of the table's
// indexes, the region can run on index entries and handles alone, and the
// table is joined back on its handle above the region to fetch the output-only
// columns for the surviving rows:
//
//	Limit / Join (region root)               Projection (original columns)
//	└─Join                                   └─Sort (TopN only)
//	  ├─DataSource(t: a, b, payload)  =>       └─Join(t.handle = t'.handle)
//	  └─...                                      ├─Limit / Join
//	                                             │ └─Join
//	                                             │   ├─DataSource(t: a, b, handle)
//	                                             │   └─...
//	                                             └─DataSource(t': payload, handle)
//
// Rows that a join, a filter on another table or an OFFSET discards never have
// their table row read. The rewrite returns the same rows for any data, since
// every handle read from an index has a row in the same snapshot.
//
// Whether it pays depends on the physical plan, so it is decided during
// physical optimization: the normal plan is built first, a table is split only
// if that plan produces more of its rows than reach the fetch point, the
// rewritten plan is optimized again, and the cheaper of the two is kept. The
// rewritten plan may use a merge join only where the original plan already
// did: a merge join's cost assumes it stops early under a Limit while its
// readers fetch ahead, so a new merge join could look cheaper than it is. With
// the same merge joins in both plans, the difference in cost is the table rows
// not read.

// lateMaterializeTarget is one DataSource whose output-only columns may be
// fetched above its region.
type lateMaterializeTarget struct {
	ds        *logicalop.DataSource
	nullable  bool // the DataSource is on the null-supplying side of an outer join
	handle    *expression.Column
	aboveOnly map[int64]struct{} // UniqueIDs of the columns moved above the region
}

// lateMaterializeRegion is a maximal subtree of inner/outer joins, selections
// and column-only projections over DataSources, optionally topped by a Limit
// or TopN.
type lateMaterializeRegion struct {
	root base.LogicalPlan
	// parent and childIdx locate the root in the plan; parent is nil when the
	// root is the plan's root.
	parent   base.LogicalPlan
	childIdx int
	isLimit  bool // root is a Limit or TopN
	byItems  []*util.ByItems
	// nodes holds the joins and projections below the root, plus the root when
	// it is one of them, in post order.
	nodes   []base.LogicalPlan
	targets []*lateMaterializeTarget
}

func isLateMaterializeLimitRoot(p base.LogicalPlan) bool {
	switch x := p.(type) {
	case *logicalop.LogicalLimit:
		return len(x.PartitionBy) == 0
	case *logicalop.LogicalTopN:
		return len(x.PartitionBy) == 0
	}
	return false
}

func isColumnOnlyProjection(proj *logicalop.LogicalProjection) bool {
	for i, expr := range proj.Exprs {
		col, ok := expr.(*expression.Column)
		if !ok || col.UniqueID != proj.Schema().Columns[i].UniqueID {
			return false
		}
	}
	return true
}

func isLateMaterializeInterior(p base.LogicalPlan) bool {
	switch x := p.(type) {
	case *logicalop.LogicalJoin:
		return x.JoinType == base.InnerJoin || x.JoinType == base.LeftOuterJoin || x.JoinType == base.RightOuterJoin
	case *logicalop.LogicalSelection:
		return true
	case *logicalop.LogicalProjection:
		return isColumnOnlyProjection(x)
	}
	return false
}

// findLateMaterializeRegions collects the regions in the plan. A Limit or TopN
// always starts a region; a join, selection or column-only projection starts
// one when its parent is neither.
func findLateMaterializeRegions(p, parent base.LogicalPlan, childIdx int, regions *[]*lateMaterializeRegion) {
	isRoot := isLateMaterializeLimitRoot(p) ||
		(isLateMaterializeInterior(p) && (parent == nil ||
			(!isLateMaterializeInterior(parent) && !isLateMaterializeLimitRoot(parent))))
	if isRoot {
		if r := buildLateMaterializeRegion(p); r != nil {
			r.parent, r.childIdx = parent, childIdx
			*regions = append(*regions, r)
		}
	}
	for i, child := range p.Children() {
		findLateMaterializeRegions(child, p, i, regions)
	}
}

func buildLateMaterializeRegion(root base.LogicalPlan) *lateMaterializeRegion {
	r := &lateMaterializeRegion{root: root}
	belowUsed := make(map[int64]struct{})
	tree := root
	switch x := root.(type) {
	case *logicalop.LogicalLimit:
		r.isLimit = true
		tree = x.Children()[0]
	case *logicalop.LogicalTopN:
		r.isLimit = true
		r.byItems = x.ByItems
		tree = x.Children()[0]
		for _, item := range x.ByItems {
			for _, col := range expression.ExtractColumns(item.Expr) {
				belowUsed[col.UniqueID] = struct{}{}
			}
		}
	}
	var candidates []*lateMaterializeTarget
	if !collectLateMaterializeTree(tree, false, belowUsed, &candidates, &r.nodes) {
		return nil
	}
	if !slices.ContainsFunc(r.nodes, func(p base.LogicalPlan) bool {
		_, ok := p.(*logicalop.LogicalJoin)
		return ok
	}) {
		return nil
	}
	for _, t := range candidates {
		if prepareLateMaterializeTarget(t, belowUsed) {
			r.targets = append(r.targets, t)
		}
	}
	if len(r.targets) == 0 {
		return nil
	}
	return r
}

// collectLateMaterializeTree walks the subtree of a region. It accepts only
// inner/outer joins, selections, column-only projections (join reorder adds
// them to restore column order) and DataSources, and records every column that
// is evaluated inside the region in belowUsed. Joins and projections are
// collected in post order into nodes, whose schemas are rebuilt after the
// DataSources are split.
func collectLateMaterializeTree(p base.LogicalPlan, nullable bool, belowUsed map[int64]struct{},
	targets *[]*lateMaterializeTarget, nodes *[]base.LogicalPlan) bool {
	switch x := p.(type) {
	case *logicalop.DataSource:
		for _, col := range expression.ExtractColumnsFromExpressions(x.AllConds, nil) {
			belowUsed[col.UniqueID] = struct{}{}
		}
		for _, col := range expression.ExtractColumnsFromExpressions(x.PushedDownConds, nil) {
			belowUsed[col.UniqueID] = struct{}{}
		}
		*targets = append(*targets, &lateMaterializeTarget{ds: x, nullable: nullable})
		return true
	case *logicalop.LogicalSelection:
		for _, col := range expression.ExtractColumnsFromExpressions(x.Conditions, nil) {
			belowUsed[col.UniqueID] = struct{}{}
		}
		return collectLateMaterializeTree(x.Children()[0], nullable, belowUsed, targets, nodes)
	case *logicalop.LogicalProjection:
		if !isColumnOnlyProjection(x) {
			return false
		}
		if !collectLateMaterializeTree(x.Children()[0], nullable, belowUsed, targets, nodes) {
			return false
		}
		*nodes = append(*nodes, x)
		return true
	case *logicalop.LogicalJoin:
		// A join method hint asks for a specific plan; keep it as written.
		if x.PreferJoinType != 0 || x.LeftPreferJoinType != 0 || x.RightPreferJoinType != 0 {
			return false
		}
		leftNullable, rightNullable := nullable, nullable
		switch x.JoinType {
		case base.InnerJoin:
		case base.LeftOuterJoin:
			rightNullable = true
		case base.RightOuterJoin:
			leftNullable = true
		default:
			return false
		}
		// NAEQConditions only exist on null-aware anti joins, which are rejected above.
		conds := make([]expression.Expression, 0, len(x.EqualConditions)+
			len(x.LeftConditions)+len(x.RightConditions)+len(x.OtherConditions))
		for _, cond := range x.EqualConditions {
			conds = append(conds, cond)
		}
		conds = append(conds, x.LeftConditions...)
		conds = append(conds, x.RightConditions...)
		conds = append(conds, x.OtherConditions...)
		for _, col := range expression.ExtractColumnsFromExpressions(conds, nil) {
			belowUsed[col.UniqueID] = struct{}{}
		}
		if !collectLateMaterializeTree(x.Children()[0], leftNullable, belowUsed, targets, nodes) ||
			!collectLateMaterializeTree(x.Children()[1], rightNullable, belowUsed, targets, nodes) {
			return false
		}
		*nodes = append(*nodes, x)
		return true
	default:
		return false
	}
}

// prepareLateMaterializeTarget decides whether a DataSource benefits from the
// rewrite: it must have analyzed statistics, an int handle, at least one
// output-only column, and an index covering every column used inside the
// region.
func prepareLateMaterializeTarget(t *lateMaterializeTarget, belowUsed map[int64]struct{}) bool {
	ds := t.ds
	// Without analyzed statistics the row reduction that justifies the split is
	// a guess, so leave such tables alone.
	if ds.StatisticTable == nil || ds.StatisticTable.Pseudo || !ds.StatisticTable.IsAnalyzed() {
		return false
	}
	if ds.IsForUpdateRead || ds.SampleInfo != nil || ds.FtsPushDown != nil ||
		ds.TableInfo.GetPartitionInfo() != nil || ds.TableInfo.IsCommonHandle ||
		!ds.Table.Type().IsNormalTable() || ds.PreferStoreType&h.PreferTiFlash != 0 ||
		ds.UnMutableHandleCols == nil || !ds.UnMutableHandleCols.IsInt() {
		return false
	}
	t.handle = ds.UnMutableHandleCols.GetCol(0)
	t.aboveOnly = make(map[int64]struct{})
	belowCols := make([]*expression.Column, 0, ds.Schema().Len())
	for _, col := range ds.Schema().Columns {
		_, used := belowUsed[col.UniqueID]
		// Virtual generated columns are computed from other columns of the row,
		// and extra columns (handle, commit ts, ...) are not plain row columns.
		if used || col.UniqueID == t.handle.UniqueID || col.VirtualExpr != nil || col.ID <= 0 {
			belowCols = append(belowCols, col)
			continue
		}
		t.aboveOnly[col.UniqueID] = struct{}{}
	}
	if len(t.aboveOnly) == 0 {
		return false
	}
	for _, path := range ds.PossibleAccessPaths {
		if path.Index == nil || path.IsTablePath() || path.StoreType != kv.TiKV || path.Index.MVIndex ||
			path.Index.IsColumnarIndex() || path.Index.ConditionExprString != "" {
			continue
		}
		_, _, idxCols, idxColLens := util.IndexInfo2Cols(ds.Columns, ds.Schema().Columns, path.Index)
		if ds.IsIndexCoveringColumns(belowCols, idxCols, idxColLens) {
			return true
		}
	}
	return false
}

// splitLateMaterializeTarget removes the output-only columns from the
// DataSource inside the region, makes sure it outputs its handle, and returns a
// new DataSource over the same table that outputs those columns plus a fresh
// handle column to join back on.
func splitLateMaterializeTarget(t *lateMaterializeTarget) (*logicalop.DataSource, *expression.Column) {
	ds := t.ds
	sctx := ds.SCtx()

	keptCols := make([]*expression.Column, 0, ds.Schema().Len())
	keptInfos := make([]*model.ColumnInfo, 0, ds.Schema().Len())
	movedCols := make([]*expression.Column, 0, len(t.aboveOnly)+1)
	movedInfos := make([]*model.ColumnInfo, 0, len(t.aboveOnly)+1)
	for i, col := range ds.Schema().Columns {
		if _, ok := t.aboveOnly[col.UniqueID]; ok {
			movedCols = append(movedCols, col)
			movedInfos = append(movedInfos, ds.Columns[i])
			continue
		}
		keptCols = append(keptCols, col)
		keptInfos = append(keptInfos, ds.Columns[i])
	}
	handleInfo := lateMaterializeHandleInfo(ds, t.handle)
	if ds.Schema().ColumnIndex(t.handle) == -1 {
		keptCols = append(keptCols, t.handle)
		keptInfos = append(keptInfos, handleInfo)
	}
	ds.SetSchema(expression.NewSchema(keptCols...))
	ds.Columns = keptInfos
	ds.HandleCols = ds.UnMutableHandleCols
	reqFullLen := make([]*expression.Column, 0, len(ds.ColsRequiringFullLen))
	for _, col := range ds.ColsRequiringFullLen {
		if _, ok := t.aboveOnly[col.UniqueID]; !ok {
			reqFullLen = append(reqFullLen, col)
		}
	}
	ds.ColsRequiringFullLen = reqFullLen
	// Access paths may already have been derived (join reorder derives stats)
	// against the wider schema, marking covering indexes as double reads.
	// Clear the stats so the paths are derived again for the narrower schema.
	ds.SetStats(nil)

	newHandle := t.handle.Clone().(*expression.Column)
	newHandle.UniqueID = sctx.GetSessionVars().AllocPlanColumnID()
	movedCols = append(movedCols, newHandle)
	movedInfos = append(movedInfos, handleInfo)

	tblCols := make([]*expression.Column, 0, len(ds.TblCols))
	tblColsByID := make(map[int64]*expression.Column, len(ds.TblColsByID))
	for _, col := range ds.TblCols {
		if col.UniqueID == t.handle.UniqueID {
			col = newHandle
		}
		tblCols = append(tblCols, col)
		tblColsByID[col.ID] = col
	}
	var tablePaths []*util.AccessPath
	for _, path := range ds.AllPossibleAccessPaths {
		if path.IsIntHandlePath && path.StoreType == kv.TiKV {
			tablePaths = append(tablePaths, &util.AccessPath{IsIntHandlePath: true, StoreType: kv.TiKV})
			break
		}
	}
	if len(tablePaths) == 0 {
		tablePaths = append(tablePaths, &util.AccessPath{IsIntHandlePath: true, StoreType: kv.TiKV})
	}
	handleCols := util.NewIntHandleCols(newHandle)
	fetch := logicalop.DataSource{
		DBName:                 ds.DBName,
		TableAsName:            ds.TableAsName,
		Table:                  ds.Table,
		TableInfo:              ds.TableInfo,
		PhysicalTableID:        ds.PhysicalTableID,
		Columns:                movedInfos,
		StatisticTable:         ds.StatisticTable,
		TblColHists:            ds.TblColHists,
		AllPossibleAccessPaths: tablePaths,
		PossibleAccessPaths:    tablePaths,
		HandleCols:             handleCols,
		UnMutableHandleCols:    handleCols,
		TblCols:                tblCols,
		TblColsByID:            tblColsByID,
		PreferPartitions:       make(map[int][]ast.CIStr),
		PreferStoreType:        ds.PreferStoreType,
		IS:                     ds.IS,
	}.Init(sctx, ds.QueryBlockOffset())
	fetch.SetSchema(expression.NewSchema(movedCols...))
	return fetch, newHandle
}

// rebuildLateMaterializeProjection drops the moved columns from a column-only
// projection and passes the handles of the split DataSources through it.
func rebuildLateMaterializeProjection(proj *logicalop.LogicalProjection, targets []*lateMaterializeTarget) {
	childSchema := proj.Children()[0].Schema()
	cols := make([]*expression.Column, 0, proj.Schema().Len()+len(targets))
	exprs := make([]expression.Expression, 0, proj.Schema().Len()+len(targets))
	for i, col := range proj.Schema().Columns {
		moved := false
		for _, t := range targets {
			if _, ok := t.aboveOnly[col.UniqueID]; ok {
				moved = true
				break
			}
		}
		if !moved {
			cols = append(cols, col)
			exprs = append(exprs, proj.Exprs[i])
		}
	}
	for _, t := range targets {
		handle := childSchema.RetrieveColumn(t.handle)
		if handle == nil || expression.NewSchema(cols...).Contains(handle) {
			continue
		}
		cols = append(cols, handle)
		exprs = append(exprs, handle)
	}
	proj.Exprs = exprs
	proj.SetSchema(expression.NewSchema(cols...))
}

func lateMaterializeHandleInfo(ds *logicalop.DataSource, handle *expression.Column) *model.ColumnInfo {
	if handle.ID == model.ExtraHandleID {
		return model.NewExtraHandleColInfo()
	}
	for _, colInfo := range ds.TableInfo.Columns {
		if colInfo.ID == handle.ID {
			return colInfo
		}
	}
	return handle.ToInfo()
}

// rewriteLateMaterializeRegion splits the region's targets and joins them back
// above the region root. It edits the region in place, so it is applied to a
// clone. It returns false if the rewritten region cannot be wired up; the clone
// is then discarded.
func rewriteLateMaterializeRegion(r *lateMaterializeRegion) (base.LogicalPlan, bool) {
	root := r.root
	sctx := root.SCtx()
	originSchema := root.Schema().Clone()
	originNames := root.OutputNames()

	fetches := make([]*logicalop.DataSource, 0, len(r.targets))
	newHandles := make([]*expression.Column, 0, len(r.targets))
	for _, t := range r.targets {
		fetch, newHandle := splitLateMaterializeTarget(t)
		fetches = append(fetches, fetch)
		newHandles = append(newHandles, newHandle)
	}
	// nodes is in post order, so children schemas are rebuilt before parents.
	for _, node := range r.nodes {
		switch x := node.(type) {
		case *logicalop.LogicalJoin:
			x.MergeSchema()
		case *logicalop.LogicalProjection:
			rebuildLateMaterializeProjection(x, r.targets)
		}
	}

	rootHandles := make([]*expression.Column, 0, len(r.targets))
	if r.isLimit {
		// The Limit outputs what its parent used, minus the moved columns, plus
		// the handles to join back on and the ORDER BY columns re-applied by the
		// Sort above. A Limit with an inlined projection must keep its child's
		// column order, so the schema is built by filtering the child schema.
		keep := make(map[int64]struct{}, root.Schema().Len()+len(r.targets))
		for _, col := range root.Schema().Columns {
			keep[col.UniqueID] = struct{}{}
		}
		for _, t := range r.targets {
			for id := range t.aboveOnly {
				delete(keep, id)
			}
			keep[t.handle.UniqueID] = struct{}{}
		}
		for _, item := range r.byItems {
			for _, col := range expression.ExtractColumns(item.Expr) {
				keep[col.UniqueID] = struct{}{}
			}
		}
		childSchema := root.Children()[0].Schema()
		limitCols := make([]*expression.Column, 0, len(keep))
		for _, col := range childSchema.Columns {
			if _, ok := keep[col.UniqueID]; ok {
				limitCols = append(limitCols, col)
				delete(keep, col.UniqueID)
			}
		}
		if len(keep) > 0 {
			return nil, false
		}
		root.(interface{ SetSchema(*expression.Schema) }).SetSchema(expression.NewSchema(limitCols...))
	}
	for _, t := range r.targets {
		handle := root.Schema().RetrieveColumn(t.handle)
		if handle == nil {
			return nil, false
		}
		rootHandles = append(rootHandles, handle)
	}

	top := root
	for i, t := range r.targets {
		joinType := base.InnerJoin
		if t.nullable {
			joinType = base.LeftOuterJoin
		}
		join := logicalop.LogicalJoin{JoinType: joinType}.Init(sctx, root.QueryBlockOffset())
		join.SetChildren(top, fetches[i])
		eq := expression.NewFunctionInternal(sctx.GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny),
			rootHandles[i], newHandles[i]).(*expression.ScalarFunction)
		join.EqualConditions = []*expression.ScalarFunction{eq}
		join.MergeSchema()
		top = join
	}
	if len(r.byItems) > 0 {
		sortItems := make([]*util.ByItems, 0, len(r.byItems))
		for _, item := range r.byItems {
			sortItems = append(sortItems, item.Clone())
		}
		sort := logicalop.LogicalSort{ByItems: sortItems}.Init(sctx, root.QueryBlockOffset())
		sort.SetChildren(top)
		top = sort
	}
	// Restore the root's original output columns, so the handles added for the
	// join back do not leak into the result.
	proj := logicalop.LogicalProjection{Exprs: expression.Column2Exprs(originSchema.Columns)}.Init(sctx, root.QueryBlockOffset())
	proj.SetSchema(originSchema)
	proj.SetOutputNames(originNames)
	proj.SetChildren(top)
	return proj, true
}

// rewriteLateMaterializeClone rewrites a clone of the region, so the original
// subtree stays untouched for the original plan. chosen holds the positions of
// the targets to split. It returns false if the region cannot be cloned or the
// clone does not match the original.
func rewriteLateMaterializeClone(r *lateMaterializeRegion, chosen []int) (base.LogicalPlan, bool) {
	root, ok := cloneLogicalSubtree(r.root)
	if !ok {
		return nil, false
	}
	clone := buildLateMaterializeRegion(root)
	if clone == nil || len(clone.targets) != len(r.targets) {
		return nil, false
	}
	targets := make([]*lateMaterializeTarget, 0, len(chosen))
	for _, i := range chosen {
		relinkLateMaterializeAccessPaths(r.targets[i].ds, clone.targets[i].ds)
		targets = append(targets, clone.targets[i])
	}
	clone.targets = targets
	return rewriteLateMaterializeRegion(clone)
}

// relinkLateMaterializeAccessPaths makes the clone's PossibleAccessPaths point
// at its own AllPossibleAccessPaths, as they do in the original. Cloning copies
// the two lists separately; the split DataSource's paths are derived again, and
// derivation updates AllPossibleAccessPaths while physical optimization reads
// PossibleAccessPaths. Paths that are not in AllPossibleAccessPaths, such as
// index merge paths, are dropped; derivation generates them again.
func relinkLateMaterializeAccessPaths(orig, clone *logicalop.DataSource) {
	paths := make([]*util.AccessPath, 0, len(orig.PossibleAccessPaths))
	for _, path := range orig.PossibleAccessPaths {
		if i := slices.Index(orig.AllPossibleAccessPaths, path); i >= 0 {
			paths = append(paths, clone.AllPossibleAccessPaths[i])
		}
	}
	clone.PossibleAccessPaths = paths
}

// lateMaterializeReaderPath returns the path from the plan root to the root
// reader of the DataSource, or nil if it is not read exactly once by a TiKV
// table, index or index lookup reader.
func lateMaterializeReaderPath(plan base.PhysicalPlan, ds *logicalop.DataSource) []base.PhysicalPlan {
	var found []base.PhysicalPlan
	matches := 0
	var walk func(p base.PhysicalPlan, path []base.PhysicalPlan)
	walk = func(p base.PhysicalPlan, path []base.PhysicalPlan) {
		path = append(path, p)
		if lateMaterializeReaderReads(p, ds) {
			matches++
			found = slices.Clone(path)
			return
		}
		for _, child := range p.Children() {
			walk(child, path)
		}
	}
	walk(plan, nil)
	if matches != 1 {
		return nil
	}
	return found
}

func lateMaterializeReaderReads(p base.PhysicalPlan, ds *logicalop.DataSource) bool {
	var copPlans []base.PhysicalPlan
	switch x := p.(type) {
	case *physicalop.PhysicalTableReader:
		if x.StoreType != kv.TiKV {
			return false
		}
		copPlans = x.TablePlans
	case *physicalop.PhysicalIndexReader:
		copPlans = x.IndexPlans
	case *physicalop.PhysicalIndexLookUpReader:
		copPlans = x.IndexPlans
	default:
		return false
	}
	alias := ds.TableInfo.Name.L
	if ds.TableAsName != nil && ds.TableAsName.L != "" {
		alias = ds.TableAsName.L
	}
	for _, cp := range copPlans {
		var tbl *model.TableInfo
		var asName *ast.CIStr
		switch scan := cp.(type) {
		case *physicalop.PhysicalTableScan:
			tbl, asName = scan.Table, scan.TableAsName
		case *physicalop.PhysicalIndexScan:
			tbl, asName = scan.Table, scan.TableAsName
		default:
			continue
		}
		scanAlias := tbl.Name.L
		if asName != nil && asName.L != "" {
			scanAlias = asName.L
		}
		return tbl.ID == ds.TableInfo.ID && scanAlias == alias
	}
	return false
}

func isLateMaterializePhysicalInterior(p base.PhysicalPlan) bool {
	switch x := p.(type) {
	case *physicalop.PhysicalHashJoin, *physicalop.PhysicalMergeJoin, *physicalop.PhysicalIndexJoin,
		*physicalop.PhysicalIndexHashJoin, *physicalop.PhysicalIndexMergeJoin,
		*physicalop.PhysicalSelection, *physicalop.PhysicalSort, *physicalop.PhysicalLimit, *physicalop.PhysicalTopN:
		return true
	case *physicalop.PhysicalProjection:
		for _, expr := range x.Exprs {
			if _, ok := expr.(*expression.Column); !ok {
				return false
			}
		}
		return true
	}
	return false
}

func physicalIndexJoinInnerIdx(p base.PhysicalPlan) (int, bool) {
	switch x := p.(type) {
	case *physicalop.PhysicalIndexJoin:
		return x.InnerChildIdx, true
	case *physicalop.PhysicalIndexHashJoin:
		return x.InnerChildIdx, true
	case *physicalop.PhysicalIndexMergeJoin:
		return x.InnerChildIdx, true
	}
	return 0, false
}

// lateMaterializeMinReduction is how many times more rows a table must produce
// than reach the fetch point before splitting it is tried.
const lateMaterializeMinReduction = 1.2

// worthLateMaterializing reports whether, in plan, the target's table produces
// noticeably more rows than reach the point where its fetch would go. A table
// read on the inner side of an index join produces as many rows as that join
// outputs; any other table produces as many as its reader returns. The fetch
// point is the top of the region: the Limit or TopN, or the highest join,
// selection or column-only projection above the reader.
func worthLateMaterializing(plan base.PhysicalPlan, t *lateMaterializeTarget) bool {
	path := lateMaterializeReaderPath(plan, t.ds)
	if path == nil {
		return false
	}
	reader := path[len(path)-1]
	produced := reader.StatsInfo().RowCount
	fetch := reader
	innerFound := false
	for i := len(path) - 2; i >= 0; i-- {
		op := path[i]
		if !isLateMaterializePhysicalInterior(op) {
			break
		}
		if innerIdx, ok := physicalIndexJoinInnerIdx(op); ok && !innerFound && op.Children()[innerIdx] == path[i+1] {
			produced = op.StatsInfo().RowCount
			innerFound = true
		}
		fetch = op
		if _, ok := op.(*physicalop.PhysicalLimit); ok {
			break
		}
		if _, ok := op.(*physicalop.PhysicalTopN); ok {
			break
		}
	}
	return produced > fetch.StatsInfo().RowCount*lateMaterializeMinReduction
}

// lateMaterializeAllowedMergeJoins returns the IDs of the logical joins that the
// physical plan implements as merge joins. Joins are matched on their join key
// columns, which the rewrite leaves unchanged.
func lateMaterializeAllowedMergeJoins(logic base.LogicalPlan, plan base.PhysicalPlan) map[int]struct{} {
	keyOf := func(cols []*expression.Column) string {
		ids := make([]int64, 0, len(cols))
		for _, col := range cols {
			ids = append(ids, col.UniqueID)
		}
		slices.Sort(ids)
		return fmt.Sprint(ids)
	}
	mergeKeys := make(map[string]struct{})
	var walkPhysical func(p base.PhysicalPlan)
	walkPhysical = func(p base.PhysicalPlan) {
		if mj, ok := p.(*physicalop.PhysicalMergeJoin); ok {
			mergeKeys[keyOf(append(slices.Clone(mj.LeftJoinKeys), mj.RightJoinKeys...))] = struct{}{}
		}
		for _, child := range p.Children() {
			walkPhysical(child)
		}
	}
	walkPhysical(plan)
	allowed := make(map[int]struct{})
	var walkLogical func(p base.LogicalPlan)
	walkLogical = func(p base.LogicalPlan) {
		if join, ok := p.(*logicalop.LogicalJoin); ok && len(mergeKeys) > 0 {
			cols := make([]*expression.Column, 0, 2*len(join.EqualConditions))
			for _, eq := range join.EqualConditions {
				cols = append(cols, expression.ExtractColumns(eq)...)
			}
			if _, ok := mergeKeys[keyOf(cols)]; ok {
				allowed[join.ID()] = struct{}{}
			}
		}
		for _, child := range p.Children() {
			walkLogical(child)
		}
	}
	walkLogical(logic)
	return allowed
}

// resetLogicalTaskMaps drops the physical tasks cached on every logical node.
func resetLogicalTaskMaps(p base.LogicalPlan) {
	if r, ok := p.(interface{ ResetTaskMap() }); ok {
		r.ResetTaskMap()
	}
	for _, child := range p.Children() {
		resetLogicalTaskMaps(child)
	}
}

// tryLateMaterialization builds a late-materialized alternative to plan and
// returns whichever of the two is cheaper. warnStart is the number of
// statement warnings before plan was built, so the alternative's optimization
// warnings replace the original's rather than adding to them.
func tryLateMaterialization(logic base.LogicalPlan, plan base.PhysicalPlan, cost float64, warnStart int) (base.LogicalPlan, base.PhysicalPlan, float64) {
	sessVars := logic.SCtx().GetSessionVars()
	if !sessVars.StmtCtx.InSelectStmt || sessVars.IsMPPEnforced() ||
		DefaultDisabledLogicalRulesList.Load().(set.StringSet).Exist(lateMaterializationName) {
		return logic, plan, cost
	}
	var regions []*lateMaterializeRegion
	findLateMaterializeRegions(logic, nil, 0, &regions)
	type rewriteTask struct {
		region *lateMaterializeRegion
		chosen []int
	}
	var tasks []rewriteTask
	for _, r := range regions {
		var chosen []int
		for i, t := range r.targets {
			if worthLateMaterializing(plan, t) {
				chosen = append(chosen, i)
			}
		}
		if len(chosen) > 0 {
			tasks = append(tasks, rewriteTask{region: r, chosen: chosen})
		}
	}
	if len(tasks) == 0 {
		return logic, plan, cost
	}

	// Each region is rewritten as a clone and swapped in at its parent, so the
	// original plan's subtrees are never edited. When the original plan wins,
	// the subtrees are swapped back and the warnings and the plan and column ID
	// counters are restored, so trying the alternative leaves no trace in
	// EXPLAIN output.
	warns := slices.Clone(sessVars.StmtCtx.GetWarnings())
	planID, planColumnID := sessVars.PlanID.Load(), sessVars.PlanColumnID.Load()
	newLogic := logic
	swapped := make([]*lateMaterializeRegion, 0, len(tasks))
	restore := func() (base.LogicalPlan, base.PhysicalPlan, float64) {
		for _, r := range swapped {
			r.parent.SetChild(r.childIdx, r.root)
		}
		sessVars.StmtCtx.SetWarnings(warns)
		sessVars.PlanID.Store(planID)
		sessVars.PlanColumnID.Store(planColumnID)
		return logic, plan, cost
	}
	for _, task := range tasks {
		rewritten, ok := rewriteLateMaterializeClone(task.region, task.chosen)
		if !ok {
			return restore()
		}
		if task.region.parent == nil {
			newLogic = rewritten
			continue
		}
		task.region.parent.SetChild(task.region.childIdx, rewritten)
		swapped = append(swapped, task.region)
	}
	resetLogicalTaskMaps(newLogic)
	sessVars.StmtCtx.SetWarnings(slices.Clone(warns[:min(warnStart, len(warns))]))
	sessVars.StmtCtx.LateMaterializationMergeJoins = lateMaterializeAllowedMergeJoins(newLogic, plan)
	newPlan, newCost, err := physicalOptimize(newLogic)
	sessVars.StmtCtx.LateMaterializationMergeJoins = nil
	if err != nil || newCost >= cost {
		return restore()
	}
	// The rewrite must not change whether hints apply: reject it if it raised a
	// warning the original plan did not.
	before := make(map[string]struct{})
	for _, w := range warns[min(warnStart, len(warns)):] {
		before[w.Err.Error()] = struct{}{}
	}
	newWarns := sessVars.StmtCtx.GetWarnings()
	for _, w := range newWarns[min(warnStart, len(newWarns)):] {
		if _, ok := before[w.Err.Error()]; !ok {
			return restore()
		}
	}
	return newLogic, newPlan, newCost
}
