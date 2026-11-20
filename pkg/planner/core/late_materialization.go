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
	"context"
	"fmt"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
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
// Each ordinary physical plan proposes tables only when it produces more rows
// than reach the fetch point. For the ordinary cost winner, one dedicated round rebuilds
// the AST, applies that proposal after logical optimization, and competes on
// cost through the normal alternative framework. The
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
			if expression.IsMutableEffectsExpr(item.Expr) {
				return nil
			}
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
	newHandle.CleanHashCode()
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
// fresh AST build. A failed rewrite discards that alternative round.
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

// LateMaterializationAlternative owns the proposal for one statement. It is
// carried by the optimization context, never by a session or a logical node.
// Each ordinary round records a proposal; Enabled applies the winning one to an AST
// build in the dedicated alternative round.
type LateMaterializationAlternative struct {
	Enabled    bool
	recorded   bool
	tasks      []lateMaterializationTask
	mergeJoins map[string]struct{}
	warnings   map[string]struct{}
}

type lateMaterializationTask struct {
	region  int
	targets []lateMaterializationTargetID
	shape   []lateMaterializationRegionNode
}

// lateMaterializationTargetID identifies a table occurrence across AST builds.
// Eligibility can change when physical optimization prunes access paths, so
// neither a position in the eligible-target list nor a table ID is sufficient.
// Query blocks distinguish repeated aliases in separate derived tables.
type lateMaterializationTargetID struct {
	tableID          int64
	alias            string
	queryBlockOffset int
}

func (t *lateMaterializeTarget) identity() lateMaterializationTargetID {
	alias := t.ds.TableInfo.Name.L
	if t.ds.TableAsName != nil && t.ds.TableAsName.L != "" {
		alias = t.ds.TableAsName.L
	}
	return lateMaterializationTargetID{tableID: t.ds.TableInfo.ID, alias: alias, queryBlockOffset: t.ds.QueryBlockOffset()}
}

// lateMaterializationRegionNode records only table identities and binary join
// types in preorder. Unary operators and schemas do not participate. A join
// consumes two subtrees, so the sequence also distinguishes join nesting.
type lateMaterializationRegionNode struct {
	isJoin   bool
	tableID  int64
	joinType base.JoinType
}

func lateMaterializationRegionShape(root base.LogicalPlan) []lateMaterializationRegionNode {
	var shape []lateMaterializationRegionNode
	var walk func(base.LogicalPlan)
	walk = func(p base.LogicalPlan) {
		switch x := p.(type) {
		case *logicalop.DataSource:
			shape = append(shape, lateMaterializationRegionNode{tableID: x.TableInfo.ID})
		case *logicalop.LogicalJoin:
			shape = append(shape, lateMaterializationRegionNode{isJoin: true, joinType: x.JoinType})
		}
		for _, child := range p.Children() {
			walk(child)
		}
	}
	walk(root)
	return shape
}

// validate checks all regions and resolves target occurrences before any
// rewrite. The returned lists belong to the rebuilt plan, in proposal order;
// their positions in the current eligibility lists may differ from baseline.
func (a *LateMaterializationAlternative) validate(regions []*lateMaterializeRegion) ([][]*lateMaterializeTarget, error) {
	resolved := make([][]*lateMaterializeTarget, 0, len(a.tasks))
	for _, task := range a.tasks {
		if task.region >= len(regions) {
			return nil, errors.New("late materialization region missing from rebuilt plan")
		}
		r := regions[task.region]
		shape := lateMaterializationRegionShape(r.root)
		failpoint.InjectCall("lateMaterializationReplayShape", task.region, task.shape, shape)
		if !slices.Equal(task.shape, shape) {
			return nil, errors.New("late materialization region tables or join types changed during replay")
		}
		byIdentity := make(map[lateMaterializationTargetID]*lateMaterializeTarget, len(r.targets))
		for _, target := range r.targets {
			id := target.identity()
			if _, exists := byIdentity[id]; exists {
				return nil, errors.New("late materialization target occurrence is ambiguous in rebuilt plan")
			}
			byIdentity[id] = target
		}
		targets := make([]*lateMaterializeTarget, 0, len(task.targets))
		for _, id := range task.targets {
			target, exists := byIdentity[id]
			if !exists {
				return nil, errors.New("late materialization target missing from rebuilt plan")
			}
			targets = append(targets, target)
		}
		resolved = append(resolved, targets)
	}
	return resolved, nil
}

type lateMaterializationContextKey struct{}

// WithLateMaterializationAlternative attaches statement-local proposal state.
func WithLateMaterializationAlternative(ctx context.Context, alternative *LateMaterializationAlternative) context.Context {
	return context.WithValue(ctx, lateMaterializationContextKey{}, alternative)
}

// HasProposal reports whether the baseline plan justified an alternative.
func (a *LateMaterializationAlternative) HasProposal() bool { return len(a.tasks) > 0 }

func walkLateMaterializationJoins(p base.LogicalPlan, path string, visit func(string, *logicalop.LogicalJoin)) {
	if join, ok := p.(*logicalop.LogicalJoin); ok {
		visit(path, join)
	}
	for i, child := range p.Children() {
		walkLateMaterializationJoins(child, fmt.Sprintf("%s/%d", path, i), visit)
	}
}

func (a *LateMaterializationAlternative) record(logic base.LogicalPlan, plan base.PhysicalPlan) {
	a.recorded = true
	sv := logic.SCtx().GetSessionVars()
	if !sv.StmtCtx.InSelectStmt || sv.IsMPPEnforced() ||
		DefaultDisabledLogicalRulesList.Load().(set.StringSet).Exist(lateMaterializationName) {
		return
	}
	var regions []*lateMaterializeRegion
	findLateMaterializeRegions(logic, nil, 0, &regions)
	for i, r := range regions {
		var chosen []lateMaterializationTargetID
		for _, target := range r.targets {
			if worthLateMaterializing(plan, target) {
				chosen = append(chosen, target.identity())
			}
		}
		if len(chosen) > 0 {
			a.tasks = append(a.tasks, lateMaterializationTask{
				region: i, targets: chosen, shape: lateMaterializationRegionShape(r.root),
			})
		}
	}
	if !a.HasProposal() {
		return
	}
	allowed := lateMaterializeAllowedMergeJoins(logic, plan)
	a.mergeJoins = make(map[string]struct{})
	walkLateMaterializationJoins(logic, "r", func(path string, join *logicalop.LogicalJoin) {
		if _, ok := allowed[join.ID()]; ok {
			a.mergeJoins[path] = struct{}{}
		}
	})
	a.warnings = make(map[string]struct{})
	for _, warning := range sv.StmtCtx.GetWarnings() {
		a.warnings[warning.Err.Error()] = struct{}{}
	}
}

// apply runs after rebuilding and logically optimizing with the baseline
// round's flags and session setup. Positional mappings rely on that replay;
// validate checks table/join shape before mutation without comparing schemas.
// No plan or column allocator IDs cross builds.
func (a *LateMaterializationAlternative) apply(logic base.LogicalPlan) (base.LogicalPlan, error) {
	if !a.HasProposal() {
		return nil, errors.New("late materialization proposal has no targets")
	}
	// Resolve the baseline's allowed joins before adding the fetch joins.
	allowed := make(map[int]struct{})
	walkLateMaterializationJoins(logic, "r", func(path string, join *logicalop.LogicalJoin) {
		if _, ok := a.mergeJoins[path]; ok {
			allowed[join.ID()] = struct{}{}
		}
	})
	var regions []*lateMaterializeRegion
	findLateMaterializeRegions(logic, nil, 0, &regions)
	resolved, err := a.validate(regions)
	if err != nil {
		return nil, err
	}
	for i, task := range a.tasks {
		r := regions[task.region]
		r.targets = resolved[i]
		rewritten, ok := rewriteLateMaterializeRegion(r)
		if !ok {
			return nil, errors.New("cannot wire late materialization fetch joins")
		}
		if r.parent == nil {
			logic = rewritten
		} else {
			r.parent.SetChild(r.childIdx, rewritten)
		}
	}
	logic.SCtx().GetSessionVars().StmtCtx.LateMaterializationMergeJoins = allowed
	return logic, nil
}

// checkWarnings preserves hint applicability: a candidate with a new warning
// cannot displace the default plan, regardless of its estimated cost.
func (a *LateMaterializationAlternative) checkWarnings(logic base.LogicalPlan) error {
	for _, warning := range logic.SCtx().GetSessionVars().StmtCtx.GetWarnings() {
		if _, ok := a.warnings[warning.Err.Error()]; !ok {
			return errors.New("late materialization alternative introduced an optimization warning")
		}
	}
	return nil
}
