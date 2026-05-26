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
	"math"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/property"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

// TryTrivialPlan attempts to build a physical plan directly for trivial queries
// that don't benefit from the full optimization pipeline. This is a fast path
// for queries like "SELECT * FROM t" or "SELECT a, b FROM t" on tables where
// the only viable plan is a full table scan (no secondary indexes, no TiFlash
// replicas, no predicates, etc.).
//
// This avoids all logical optimization rules, stats loading, cost estimation,
// and post-optimization passes that are unnecessary when the plan is predetermined.
// It is designed for execution performance, not EXPLAIN accuracy.
func TryTrivialPlan(ctx base.PlanContext, node *resolve.NodeW) (base.Plan, types.NameSlice) {
	if checkStableResultMode(ctx) {
		return nil, nil
	}
	if fixcontrol.GetBoolWithDefault(ctx.GetSessionVars().OptimizerFixControl, fixcontrol.Fix52592, false) {
		return nil, nil
	}

	// Skip during EXPLAIN — the normal optimizer produces richer plan
	// metadata (costs, stats annotations) that EXPLAIN output depends on.
	if ctx.GetSessionVars().StmtCtx.InExplainStmt {
		return nil, nil
	}

	// When sql_select_limit is set, the full optimizer injects an extra LIMIT
	// via TryAddExtraLimit. The trivial plan bypasses that rewrite, so bail
	// out and let the normal path handle it.
	if ctx.GetSessionVars().SelectLimit != math.MaxUint64 {
		return nil, nil
	}

	sel, ok := node.Node.(*ast.SelectStmt)
	if !ok {
		return nil, nil
	}
	if !isTrivialSelect(sel) {
		return nil, nil
	}

	// Single table, no joins or subqueries.
	tblName, tblAlias := getSingleTableNameAndAlias(sel.From)
	if tblName == nil {
		return nil, nil
	}
	// Index hints (USE/FORCE/IGNORE INDEX) affect path selection.
	if len(tblName.IndexHints) > 0 {
		return nil, nil
	}
	// Explicit PARTITION clauses need the full planner: partitioned tables
	// need partition pruning, and non-partitioned tables must error.
	if len(tblName.PartitionNames) > 0 {
		return nil, nil
	}

	// Look up the already-resolved table info from the resolve context.
	resolveCtx := node.GetResolveContext()
	tnW := resolveCtx.GetTableName(tblName)
	if tnW == nil {
		return nil, nil
	}
	tblInfo := tnW.TableInfo
	if tblInfo == nil {
		return nil, nil
	}

	// Resolve database name.
	dbName := tblName.Schema
	if dbName.L == "" {
		dbName = ast.NewCIStr(ctx.GetSessionVars().CurrentDB)
	}

	// System/memory tables use special executors; skip fast path.
	if metadef.IsMemDB(dbName.L) {
		return nil, nil
	}

	// Views and sequences need the full optimizer to expand their definitions.
	if tblInfo.IsView() || tblInfo.IsSequence() {
		return nil, nil
	}

	if !isTrivialTable(tblInfo) {
		return nil, nil
	}

	// Local temp tables use a dummy reader that requires UnionScan to merge
	// buffered writes. Cached tables also need UnionScan for correctness.
	// Skip the fast path for both.
	if tblInfo.TempTableType == model.TempTableLocal ||
		tblInfo.TableCacheStatusType == model.TableCacheStatusEnable {
		return nil, nil
	}

	// When the transaction has uncommitted writes to this table, a UnionScan
	// is needed to merge the buffer. The trivial plan doesn't build one.
	if ctx.HasDirtyContent(tblInfo.ID) {
		return nil, nil
	}

	// Privilege check.
	if err := checkFastPlanPrivilege(ctx, dbName.L, tblInfo.Name.L, mysql.SelectPriv); err != nil {
		return nil, nil
	}

	// Build output schema and column names from the SELECT field list.
	// buildSchemaFromFields returns nil when the field list contains expressions,
	// function calls, or other constructs that need the full planner.
	schema, names := buildSchemaFromFields(dbName, tblInfo, tblAlias, sel.Fields.Fields)
	if schema == nil {
		return nil, nil
	}

	// Build the full set of table columns (for row-size estimation).
	allColumns := make([]*model.ColumnInfo, 0, len(tblInfo.Columns))
	tblCols := make([]*expression.Column, 0, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		if col.State == model.StatePublic {
			allColumns = append(allColumns, col)
			tblCols = append(tblCols, colInfoToColumn(col, i))
		}
	}
	colInfoByID := make(map[int64]*model.ColumnInfo, len(allColumns))
	for _, col := range allColumns {
		colInfoByID[col.ID] = col
	}

	// Collect SELECT column IDs once; used for the index-eligibility gate.
	selectColIDs := make([]int64, schema.Len())
	selectedSet := make(map[int64]struct{}, schema.Len())
	for i, col := range schema.Columns {
		selectColIDs[i] = col.ID
		selectedSet[col.ID] = struct{}{}
	}

	// Build the scan column set. It starts with SELECT cols in SELECT order so
	// that the executor's sequential OutputOffsets (0, 1, 2, …) line up with
	// the SELECT projection. When a WHERE clause exists, columns it references
	// but the SELECT does not are appended afterward, and a Projection is added
	// to drop them from the visible output.
	scanColInfos := make([]*model.ColumnInfo, 0, schema.Len())
	scanCols := make([]*expression.Column, 0, schema.Len())
	scanNames := make(types.NameSlice, 0, schema.Len())
	for i, sCol := range schema.Columns {
		colInfo := colInfoByID[sCol.ID]
		scanColInfos = append(scanColInfos, colInfo)
		scanCols = append(scanCols, colInfoToColumn(colInfo, i))
		scanNames = append(scanNames, &types.FieldName{
			DBName:      dbName,
			OrigTblName: tblInfo.Name,
			TblName:     tblAlias,
			OrigColName: colInfo.Name,
			ColName:     colInfo.Name,
		})
	}

	// Process the WHERE clause (if any) and extend the scan column set with
	// columns it references that aren't already projected.
	var conditions []expression.Expression
	refColIDs := make(map[int64]struct{})
	if sel.Where != nil {
		// AST-level pre-check: collect referenced column names and bail on
		// constructs the trivial path doesn't support (subqueries, aggregates,
		// window functions, session variables).
		vis := &whereColumnVisitor{names: make(map[string]struct{})}
		sel.Where.Accept(vis)
		if vis.disallowed {
			return nil, nil
		}

		// Append WHERE-only columns to the scan set. Look up by lowercased
		// name; whereColumnVisitor already lower-cased the keys.
		colByName := make(map[string]*model.ColumnInfo, len(allColumns))
		for _, c := range allColumns {
			colByName[c.Name.L] = c
		}
		for name := range vis.names {
			colInfo := colByName[name]
			if colInfo == nil {
				// Unknown column name; let the full planner produce the proper error.
				return nil, nil
			}
			if _, already := selectedSet[colInfo.ID]; already {
				continue
			}
			scanColInfos = append(scanColInfos, colInfo)
			scanCols = append(scanCols, colInfoToColumn(colInfo, len(scanCols)))
			scanNames = append(scanNames, &types.FieldName{
				DBName:      dbName,
				OrigTblName: tblInfo.Name,
				TblName:     tblAlias,
				OrigColName: colInfo.Name,
				ColName:     colInfo.Name,
			})
		}

		// Rewrite the WHERE clause against the scan schema; resulting Column
		// references have Index pointing into the scan schema.
		scanSchema := expression.NewSchema(scanCols...)
		whereExpr, err := rewriteAstExprWithPlanCtx(ctx, sel.Where, scanSchema, scanNames, false)
		if err != nil {
			return nil, nil
		}
		if expression.ContainCorrelatedColumn(whereExpr) {
			return nil, nil
		}
		conditions = expression.SplitCNFItems(whereExpr)

		// Every condition needs to serialize to TiKV PB, because we hand the
		// whole Selection to the coprocessor. The full planner splits
		// non-pushable conditions into a root-task Selection; the trivial path
		// has no such split, so bail when anything isn't pushable.
		if !expression.CanExprsPushDown(plannerutil.GetPushDownCtx(ctx), conditions, kv.TiKV) {
			return nil, nil
		}

		for _, c := range expression.ExtractColumnsFromExpressions(conditions, nil) {
			refColIDs[c.ID] = struct{}{}
		}
	}

	// Per-query gate: bail if any index could plausibly beat the table scan.
	if mayUseIndex(tblInfo, refColIDs, selectColIDs) {
		return nil, nil
	}

	// Row count estimate from stats cache (no synchronous loading).
	statsTbl := getTrivialStatsTable(ctx, tblInfo)
	rowCount := trivialRowCountFromStats(statsTbl)
	statsInfo := &property.StatsInfo{RowCount: rowCount}

	// The full planner's CollectPredicateColumnsPoint rule registers
	// predicate columns for sync/async stats loading. The trivial path
	// skips that rule, so fall back when WHERE references a column whose
	// stats need loading — letting the full planner trigger the load
	// preserves the observability and sync-wait contracts.
	if statsTbl != nil && !statsTbl.Pseudo && len(refColIDs) > 0 {
		for colID := range refColIDs {
			if _, loadNeeded, _ := statsTbl.ColumnIsLoadNeeded(colID, true); loadNeeded {
				return nil, nil
			}
		}
	}

	// Provide a HistColl so downstream callers (e.g. GetAvgRowSize for
	// network buffer sizing) don't hit a nil dereference.
	pseudoHist := statistics.PseudoHistColl(tblInfo.ID, false)

	ctx.GetSessionVars().PlanID.Store(0)
	ctx.GetSessionVars().PlanColumnID.Store(0)

	// Determine the correct full range based on handle type.
	var scanRanges ranger.Ranges
	if tblInfo.IsCommonHandle {
		scanRanges = ranger.FullRange()
	} else {
		isUnsigned := false
		if tblInfo.PKIsHandle {
			if pkColInfo := tblInfo.GetPkColInfo(); pkColInfo != nil {
				isUnsigned = mysql.HasUnsignedFlag(pkColInfo.GetFlag())
			}
		}
		scanRanges = ranger.FullIntRange(isUnsigned)
	}

	scanSchema := expression.NewSchema(scanCols...)

	// Build PhysicalTableScan.
	ts := physicalop.PhysicalTableScan{
		Table:           tblInfo,
		Columns:         scanColInfos,
		DBName:          dbName,
		TableAsName:     &tblAlias,
		PhysicalTableID: tblInfo.ID,
		Ranges:          scanRanges,
		AccessCondition: nil,
		StoreType:       kv.TiKV,
		TblCols:         tblCols,
		TblColHists:     &pseudoHist,
	}.Init(ctx, 0)
	ts.SetSchema(scanSchema)
	ts.SetStats(statsInfo)

	var topPlan base.PhysicalPlan = ts

	// Wrap a Selection on top when there is a predicate. Selection inherits
	// its schema from its child, so no SetSchema call is needed.
	if len(conditions) > 0 {
		psel := physicalop.PhysicalSelection{Conditions: conditions}.Init(ctx, statsInfo, 0)
		psel.SetChildren(topPlan)
		topPlan = psel
	}

	// If the scan column set is wider than the SELECT projection (because
	// WHERE referenced columns the SELECT does not), add a Projection that
	// keeps only the SELECT cols and exposes them as the externally-visible
	// schema.
	if len(scanCols) > schema.Len() {
		projExprs := make([]expression.Expression, schema.Len())
		for i := range schema.Len() {
			projExprs[i] = scanCols[i]
		}
		proj := physicalop.PhysicalProjection{Exprs: projExprs}.Init(ctx, statsInfo, 0)
		proj.SetSchema(schema)
		proj.SetChildren(topPlan)
		topPlan = proj
	}

	// Wrap in PhysicalTableReader.
	tr := physicalop.PhysicalTableReader{
		TablePlan:      topPlan,
		StoreType:      kv.TiKV,
		IsCommonHandle: tblInfo.IsCommonHandle,
	}.Init(ctx, 0)
	// Init() flattens TablePlan into TablePlans, copies schema, sets ReadReqType.
	tr.SetStats(statsInfo)

	// Record table access for privilege and lock checking.
	ctx.GetSessionVars().StmtCtx.Tables = []stmtctx.TableEntry{
		{DB: dbName.L, Table: tblInfo.Name.L},
	}

	return tr, names
}

// isTrivialSelect checks whether a SelectStmt is simple enough for the trivial
// fast path: single table, no ordering, no grouping, no limits, no hints, no
// locking, no CTEs, no DISTINCT, no INTO. A WHERE clause is allowed; it is
// validated separately to ensure it can't influence index selection.
func isTrivialSelect(sel *ast.SelectStmt) bool {
	return sel.Kind == ast.SelectStmtKindSelect &&
		sel.From != nil &&
		sel.GroupBy == nil &&
		sel.Having == nil &&
		sel.OrderBy == nil &&
		sel.Limit == nil &&
		sel.WindowSpecs == nil &&
		!sel.Distinct &&
		sel.SelectIntoOpt == nil &&
		sel.LockInfo == nil &&
		sel.With == nil &&
		len(sel.TableHints) == 0
}

// isTrivialTable checks whether a table's metadata allows the trivial plan
// fast path. The presence of secondary indexes is no longer disqualifying on
// its own: mayUseIndex applies a per-query check to see whether any index
// could plausibly be chosen for the actual SELECT and WHERE columns.
func isTrivialTable(tblInfo *model.TableInfo) bool {
	if tblInfo.GetPartitionInfo() != nil {
		return false
	}
	if tblInfo.TiFlashReplica != nil && tblInfo.TiFlashReplica.Available {
		return false
	}
	if len(tblInfo.ForeignKeys) > 0 {
		return false
	}
	for _, col := range tblInfo.Columns {
		if col.IsGenerated() && !col.GeneratedStored {
			return false
		}
		if col.Hidden {
			return false
		}
	}
	return true
}

// mayUseIndex reports whether some index on tblInfo could plausibly be chosen
// by the cost-based optimizer for a SELECT whose WHERE references refColIDs
// and whose output schema contains selectColIDs. It is intentionally
// conservative — when the answer might be "yes", we fall through to the full
// planner rather than commit to a table scan.
//
// The two situations where an index could win are:
//  1. The index's leading column is referenced by a predicate, enabling a
//     range scan that reads fewer rows than the full table.
//  2. The index covers every output column, enabling a covering scan that
//     reads fewer bytes per row than the table scan.
//
// References to the primary key (handle or clustered) are treated like the
// leading-column case, since they could similarly produce a range scan.
func mayUseIndex(tblInfo *model.TableInfo, refColIDs map[int64]struct{}, selectColIDs []int64) bool {
	if tblInfo.PKIsHandle {
		if pkCol := tblInfo.GetPkColInfo(); pkCol != nil {
			if _, ok := refColIDs[pkCol.ID]; ok {
				return true
			}
		}
	}
	for _, idx := range tblInfo.Indices {
		if idx.State != model.StatePublic {
			continue
		}
		if idx.Primary {
			if tblInfo.IsCommonHandle {
				for _, c := range idx.Columns {
					if _, ok := refColIDs[tblInfo.Columns[c.Offset].ID]; ok {
						return true
					}
				}
			}
			continue
		}
		leadingID := tblInfo.Columns[idx.Columns[0].Offset].ID
		if _, ok := refColIDs[leadingID]; ok {
			return true
		}
		if indexCoversCols(idx, tblInfo, selectColIDs) {
			return true
		}
	}
	return false
}

// indexCoversCols reports whether every column ID in colIDs appears in idx.
// An empty colIDs set is treated as not covering, so that "SELECT 1 FROM t"
// style queries (which buildSchemaFromFields already rejects today) wouldn't
// be mis-classified as covering by accident.
func indexCoversCols(idx *model.IndexInfo, tblInfo *model.TableInfo, colIDs []int64) bool {
	if len(colIDs) == 0 {
		return false
	}
	idxIDs := make(map[int64]struct{}, len(idx.Columns))
	for _, c := range idx.Columns {
		idxIDs[tblInfo.Columns[c.Offset].ID] = struct{}{}
	}
	for _, id := range colIDs {
		if _, ok := idxIDs[id]; !ok {
			return false
		}
	}
	return true
}

// whereColumnVisitor walks an AST WHERE clause to collect referenced column
// names and to detect constructs (subqueries, aggregates, window functions)
// that disqualify the trivial fast path.
type whereColumnVisitor struct {
	names      map[string]struct{}
	disallowed bool
}

// Enter implements ast.Visitor.
func (v *whereColumnVisitor) Enter(n ast.Node) (ast.Node, bool) {
	switch node := n.(type) {
	case *ast.ColumnNameExpr:
		v.names[node.Name.Name.L] = struct{}{}
	case *ast.SubqueryExpr, *ast.ExistsSubqueryExpr, *ast.CompareSubqueryExpr,
		*ast.AggregateFuncExpr, *ast.WindowFuncExpr, *ast.VariableExpr:
		v.disallowed = true
		return n, true
	}
	return n, false
}

// Leave implements ast.Visitor.
func (v *whereColumnVisitor) Leave(n ast.Node) (ast.Node, bool) {
	return n, !v.disallowed
}

// getTrivialStatsTable fetches the cached stats table for tblInfo, or nil
// when the stats handle is unavailable. It never triggers synchronous loading.
func getTrivialStatsTable(ctx base.PlanContext, tblInfo *model.TableInfo) *statistics.Table {
	statsHandle := domain.GetDomain(ctx).StatsHandle()
	if statsHandle == nil {
		return nil
	}
	return statsHandle.GetPhysicalTableStats(tblInfo.ID, tblInfo)
}

// trivialRowCountFromStats returns a row count estimate from the cached
// stats table, falling back to PseudoRowCount when no real count is available.
func trivialRowCountFromStats(statsTbl *statistics.Table) float64 {
	if statsTbl != nil && statsTbl.RealtimeCount > 0 {
		return float64(statsTbl.RealtimeCount)
	}
	return statistics.PseudoRowCount
}
