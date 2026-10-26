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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
)

// GeneratePKFilterOptimizer generates PK range filter conditions from secondary
// index lookups using MIN/MAX subqueries. For any query with predicates
// spanning secondary indexes on a clustered PK table, this rule derives
// `pk >= (SELECT MIN(pk) ...) AND pk <= (SELECT MAX(pk) ...)` conditions,
// narrowing the scan range on the table path. The conditions are stored in
// PKFilterConds (not PushedDownConds) so the cost model can inject them
// per-path without polluting index path estimation.
//
// Activation: hint `/*+ pk_filter(table_name, index_name, ...) */`
// or session variable `tidb_opt_generate_pk_filter` (default OFF).
type GeneratePKFilterOptimizer struct{}

// Optimize implements the base.LogicalOptRule interface.
func (*GeneratePKFilterOptimizer) Optimize(ctx context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error) {
	if !pkFilterStmtQualifies(p.SCtx()) {
		return p, false, nil
	}
	return generatePKFilterWalk(ctx, p, false)
}

// pkFilterStmtQualifies reports whether PK bounds computed by a separate
// snapshot read are guaranteed to cover every row the statement reads.
//
// The MIN/MAX probes are ordinary snapshot reads. DML in a pessimistic
// transaction reads at for_update_ts, and pessimistic READ-COMMITTED may
// retry a statement with a newer read timestamp without re-planning it. In
// both cases rows committed after the probe could fall outside the bounds and
// be silently missed, so the rule only applies to plain SELECT statements
// outside pessimistic READ-COMMITTED. Locking reads are excluded during the
// walk.
func pkFilterStmtQualifies(sctx base.PlanContext) bool {
	vars := sctx.GetSessionVars()
	sc := vars.StmtCtx
	if !sc.InSelectStmt || sc.InInsertStmt || sc.InUpdateStmt || sc.InDeleteStmt {
		return false
	}
	return !vars.IsPessimisticReadConsistency()
}

// Name implements the base.LogicalOptRule interface.
func (*GeneratePKFilterOptimizer) Name() string {
	return "generate_pk_filter"
}

// generatePKFilterWalk walks the plan tree top-down and applies the PK filter
// optimization to qualifying DataSources.
func generatePKFilterWalk(ctx context.Context, p base.LogicalPlan, underLock bool) (base.LogicalPlan, bool, error) {
	planChanged := false
	if _, ok := p.(*logicalop.LogicalLock); ok {
		// SELECT ... FOR UPDATE/SHARE reads the latest committed rows, which
		// a snapshot MIN/MAX probe may not see.
		underLock = true
	}
	for i, child := range p.Children() {
		newChild, changed, err := generatePKFilterWalk(ctx, child, underLock)
		if err != nil {
			return nil, false, err
		}
		if changed {
			planChanged = true
		}
		p.SetChild(i, newChild)
	}

	ds, ok := p.(*logicalop.DataSource)
	if !ok || underLock {
		return p, planChanged, nil
	}

	result, changed, err := tryGeneratePKFilter(ctx, ds)
	if err != nil {
		return nil, false, err
	}
	if changed {
		planChanged = true
	}
	return result, planChanged, nil
}

// tryGeneratePKFilter checks whether the DataSource qualifies for PK filter
// generation and generates the filter conditions if so. The rule evaluates
// MIN/MAX subqueries eagerly during optimization, producing concrete constant
// conditions like `pk >= 5 AND pk <= 999995`. These are stored in
// ds.PKFilterConds (not PushedDownConds) so the stats derivation phase can
// inject them into table path conditions without polluting index path cost
// estimation. The raw conditions are also added to AllConds to protect the PK
// column from being pruned by ColumnPruner.
func tryGeneratePKFilter(ctx context.Context, ds *logicalop.DataSource) (base.LogicalPlan, bool, error) {
	// Check activation: either hint or session variable
	hintMode := len(ds.PkFilterHints) > 0
	sessionMode := ds.SCtx().GetSessionVars().AllowGeneratePKFilter
	if !hintMode && !sessionMode {
		return ds, false, nil
	}

	// Must have a clustered PK (not just a rowid handle).
	// PKIsHandle covers single-integer clustered PKs; IsCommonHandle covers
	// composite or non-integer clustered PKs.
	if ds.HandleCols == nil || !ds.TableInfo.HasClusteredIndex() {
		return ds, false, nil
	}

	// Skip when the table has uncommitted modifications in the current
	// transaction. The MIN/MAX subqueries evaluate against the snapshot only
	// (no LogicalUnionScan), so dirty rows in the mem-buffer are invisible.
	// Using snapshot-only PK bounds would narrow the scan range and cause
	// UnionScanExec to miss dirty rows outside that range, violating
	// read-your-writes semantics.
	if tableHasDirtyContent(ds.SCtx(), ds.TableInfo) {
		return ds, false, nil
	}

	// Skip when fix-control 43817 is ON, which disables evaluating
	// non-correlated subqueries during the optimization phase.
	// EvalSubqueryFirstRow would hard-error, so bail out gracefully.
	if fixcontrol.GetBoolWithDefault(ds.SCtx().GetSessionVars().OptimizerFixControl, fixcontrol.Fix43817, false) {
		return ds, false, nil
	}

	// Must have pushed-down conditions
	if len(ds.PushedDownConds) == 0 {
		return ds, false, nil
	}

	// Find the target PK column
	pkCol := findTargetPKCol(ds)
	if pkCol == nil {
		return ds, false, nil
	}

	// Determine which indexes to generate filters from
	qualifyingIndexes := findQualifyingIndexPaths(ds, hintMode)
	if len(qualifyingIndexes) == 0 {
		return ds, false, nil
	}

	// An explicit pk_filter hint always probes. Otherwise use statistics to
	// skip probes that cannot pay for themselves and bounds that do not
	// narrow the scan.
	var gateColl *statistics.HistColl
	if !hintMode {
		gateColl = pkFilterGateHistColl(ds)
	}

	// Generate PK filter conditions and track source indexes.
	var pkFilterConds []util.PKFilterCondInfo
	var probes []*pkFilterProbe
	for _, idxPath := range qualifyingIndexes {
		eqConds := checkIndexQualifiesForPKFilter(ds, idxPath)
		if eqConds == nil {
			continue
		}
		if gateColl != nil && estimatePKFilterRows(ds, gateColl, eqConds) < pkFilterMinIndexRows {
			continue
		}
		conds, probe, err := buildPKFilterConditions(ctx, ds, idxPath, pkCol)
		if err != nil {
			return nil, false, err
		}
		if conds == nil {
			// Subquery returned no rows (NULL) — skip this index.
			continue
		}
		if gateColl != nil && estimatePKFilterRows(ds, gateColl, conds) >= pkFilterMaxCoverage*float64(gateColl.RealtimeCount) {
			continue
		}
		probes = append(probes, probe)
		for _, c := range conds {
			pkFilterConds = append(pkFilterConds, util.PKFilterCondInfo{
				Cond:          c,
				SourceIndexID: idxPath.Index.ID,
			})
		}
	}

	if len(pkFilterConds) == 0 {
		return ds, false, nil
	}

	// Ensure the PK column is in the DataSource's schema. It may have been
	// pruned (ColumnPruner runs before this rule) if the query doesn't
	// reference the PK column. The new ge/le conditions reference it, so
	// it must be present for resolveIndices during physical plan building.
	schema := ds.Schema()
	if schema.ColumnIndex(pkCol) == -1 {
		schema.Append(pkCol.Clone().(*expression.Column))
		ds.Columns = append(ds.Columns, findOrCreateColumnInfo(ds, pkCol))
	}

	// The bounds are deferred constants that re-run their probes when a cached
	// plan is reused. Probes are bound to this session, so plans using them
	// cannot go to the instance plan cache.
	sc := ds.SCtx().GetSessionVars().StmtCtx
	if instancePlanCacheEnabled(ctx) {
		sc.SetSkipPlanCache("PK filter bounds are evaluated by session-bound MIN/MAX probes")
	} else {
		for _, probe := range probes {
			sc.PlanCacheReuseHooks = append(sc.PlanCacheReuseHooks, probe.bindForReuse)
		}
	}

	// Store the PK filter conditions separately from PushedDownConds.
	// They will be injected into table path stats derivation later.
	ds.PKFilterConds = pkFilterConds
	// Add the raw conditions to AllConds so PruneColumns doesn't prune
	// the PK column (AllConds is used to determine which columns are needed).
	for _, pkf := range pkFilterConds {
		ds.AllConds = append(ds.AllConds, pkf.Cond)
	}
	return ds, true, nil
}

// findTargetPKCol determines the target PK column for the MIN/MAX filter.
// Returns the first PK column that does NOT have an equality filter in PushedDownConds.
func findTargetPKCol(ds *logicalop.DataSource) *expression.Column {
	handleCols := ds.HandleCols
	numCols := handleCols.NumCols()
	for i := range numCols {
		col := handleCols.GetCol(i)
		if col == nil {
			continue
		}
		if !hasEqCondForCol(ds.PushedDownConds, col) {
			return col
		}
	}
	return nil
}

// hasEqCondForCol checks if there is an equality condition (col = const) for a column.
func hasEqCondForCol(conds []expression.Expression, col *expression.Column) bool {
	for _, cond := range conds {
		sf, ok := cond.(*expression.ScalarFunction)
		if !ok {
			continue
		}
		if sf.FuncName.L != ast.EQ {
			continue
		}
		args := sf.GetArgs()
		if len(args) != 2 {
			continue
		}
		lCol, lIsCol := args[0].(*expression.Column)
		_, rIsConst := args[1].(*expression.Constant)
		rCol, rIsCol := args[1].(*expression.Column)
		_, lIsConst := args[0].(*expression.Constant)
		if lIsCol && lCol.EqualColumn(col) && rIsConst {
			return true
		}
		if rIsCol && rCol.EqualColumn(col) && lIsConst {
			return true
		}
	}
	return false
}

// findQualifyingIndexPaths determines which index paths to generate PK filters from.
func findQualifyingIndexPaths(ds *logicalop.DataSource, hintMode bool) []*util.AccessPath {
	if hintMode {
		// Check if any hint specifies explicit index names.
		hasExplicitIndexes := false
		for _, hint := range ds.PkFilterHints {
			if len(hint.IndexHint.IndexNames) > 0 {
				hasExplicitIndexes = true
				break
			}
		}
		if hasExplicitIndexes {
			return findHintedIndexPaths(ds)
		}
		// Form 2 (table, no indexes) or Form 3 (no args): use all qualifying secondary indexes.
		return findSecondaryIndexPaths(ds)
	}
	return findSecondaryIndexPaths(ds)
}

// findHintedIndexPaths returns access paths matching the indexes listed in PkFilterHints.
func findHintedIndexPaths(ds *logicalop.DataSource) []*util.AccessPath {
	var result []*util.AccessPath
	for _, hint := range ds.PkFilterHints {
		for _, idxName := range hint.IndexHint.IndexNames {
			for _, path := range ds.AllPossibleAccessPaths {
				if path.IsTablePath() || path.Index == nil {
					continue
				}
				if path.Index.Name.L == idxName.L {
					result = append(result, path)
					break
				}
			}
		}
	}
	return result
}

// findSecondaryIndexPaths returns all secondary index access paths.
func findSecondaryIndexPaths(ds *logicalop.DataSource) []*util.AccessPath {
	result := make([]*util.AccessPath, 0, len(ds.AllPossibleAccessPaths))
	for _, path := range ds.AllPossibleAccessPaths {
		if path.IsTablePath() || path.Index == nil {
			continue
		}
		result = append(result, path)
	}
	return result
}

// checkIndexQualifiesForPKFilter checks that ALL columns in the index have equality
// conditions (col = const) in the DataSource's PushedDownConds.
// Returns the matching equality conditions, or nil if the index doesn't qualify.
func checkIndexQualifiesForPKFilter(ds *logicalop.DataSource, path *util.AccessPath) []expression.Expression {
	idxCols := path.Index.Columns
	if len(idxCols) == 0 {
		return nil
	}
	eqConds := make([]expression.Expression, 0, len(idxCols))
	for _, idxCol := range idxCols {
		found := false
		for _, cond := range ds.PushedDownConds {
			sf, ok := cond.(*expression.ScalarFunction)
			if !ok {
				continue
			}
			if sf.FuncName.L != ast.EQ {
				continue
			}
			args := sf.GetArgs()
			if len(args) != 2 {
				continue
			}
			if matchesIndexColumn(args[0], args[1], idxCol, ds.TableInfo) ||
				matchesIndexColumn(args[1], args[0], idxCol, ds.TableInfo) {
				eqConds = append(eqConds, cond)
				found = true
				break
			}
		}
		if !found {
			return nil
		}
	}
	return eqConds
}

// matchesIndexColumn checks if colExpr is a column matching idxCol and valExpr is a constant.
func matchesIndexColumn(colExpr, valExpr expression.Expression, idxCol *model.IndexColumn, tblInfo *model.TableInfo) bool {
	if idxCol == nil || idxCol.Offset < 0 || idxCol.Offset >= len(tblInfo.Columns) {
		return false
	}
	col, ok := colExpr.(*expression.Column)
	if !ok {
		return false
	}
	_, isConst := valExpr.(*expression.Constant)
	if !isConst {
		return false
	}
	return int(col.ID) == int(tblInfo.Columns[idxCol.Offset].ID)
}

// findOrCreateColumnInfo returns the ColumnInfo for pkCol from ds.TableInfo.Columns.
// If the column is not found (e.g. ExtraHandle / _tidb_rowid which is synthetic
// and not in TableInfo.Columns), it falls back to pkCol.ToInfo() to create a
// temporary ColumnInfo, consistent with the pattern in physical_table_scan.go.
func findOrCreateColumnInfo(ds *logicalop.DataSource, pkCol *expression.Column) *model.ColumnInfo {
	if colInfo := model.FindColumnInfoByID(ds.TableInfo.Columns, pkCol.ID); colInfo != nil {
		return colInfo
	}
	return pkCol.ToInfo()
}

// buildPKFilterConditions creates `pk_col >= MIN(pk_col)` and `pk_col <= MAX(pk_col)`
// conditions from MIN/MAX probe sub-plans over the given index. The probes are
// evaluated now so the ranger and the cost model see concrete bounds, and the
// bounds stay deferred so a cached plan re-evaluates them for new parameters.
// Returns nil conditions if no row satisfies the probe conditions.
func buildPKFilterConditions(ctx context.Context, ds *logicalop.DataSource, idxPath *util.AccessPath, pkCol *expression.Column) ([]expression.Expression, *pkFilterProbe, error) {
	sctx := ds.SCtx()
	evalCtx := sctx.GetExprCtx()

	minPlan, retType, err := buildMinMaxProbePlan(ctx, ds, idxPath, pkCol, ast.AggFuncMin)
	if err != nil {
		return nil, nil, err
	}
	maxPlan, _, err := buildMinMaxProbePlan(ctx, ds, idxPath, pkCol, ast.AggFuncMax)
	if err != nil {
		return nil, nil, err
	}
	probe := &pkFilterProbe{
		sctx:    sctx,
		is:      ds.IS,
		ctx:     ctx,
		minPlan: minPlan,
		maxPlan: maxPlan,
		idxName: idxPath.Index.Name.O,
	}
	minVal, maxVal, err := probe.bounds()
	if err != nil {
		return nil, nil, err
	}
	if minVal.IsNull() || maxVal.IsNull() {
		return nil, nil, nil // no rows match the probe conditions
	}

	// pk >= MIN(pk) from index
	geFunc, err := expression.NewFunction(evalCtx, ast.GE, pkCol.GetType(evalCtx.GetEvalCtx()), pkCol.Clone(),
		newPKFilterBoundConstant(sctx, probe, false, minVal, retType))
	if err != nil {
		return nil, nil, err
	}
	// pk <= MAX(pk) from index
	leFunc, err := expression.NewFunction(evalCtx, ast.LE, pkCol.GetType(evalCtx.GetEvalCtx()), pkCol.Clone(),
		newPKFilterBoundConstant(sctx, probe, true, maxVal, retType))
	if err != nil {
		return nil, nil, err
	}
	return []expression.Expression{geFunc, leFunc}, probe, nil
}

// buildMinMaxProbePlan builds and optimizes a MIN or MAX sub-plan over the
// given index, returning the physical plan and the aggregate's result type.
func buildMinMaxProbePlan(ctx context.Context, ds *logicalop.DataSource, idxPath *util.AccessPath, pkCol *expression.Column, aggFuncName string) (base.PhysicalPlan, *types.FieldType, error) {
	sctx := ds.SCtx()

	// Clone the DataSource with the probe conditions
	// (needed by stats/ranger for access condition computation).
	probeConds := pkFilterProbeConds(ds.PushedDownConds)
	clonedDS := cloneDataSourceForPKFilter(ds, idxPath, pkCol, probeConds)

	// Also create a LogicalSelection carrying the probe conditions.
	// MaxMinEliminate's checkColCanUseIndex traverses LogicalSelection nodes
	// to collect conditions — it does NOT look at DataSource.PushedDownConds.
	// Without a Selection, MaxMinEliminate can't verify the index can provide
	// ordering after the equality prefix, resulting in IndexFullScan instead
	// of IndexRangeScan. Including all conditions (not just index eq conditions)
	// lets the optimizer use the PK prefix and other filters to narrow the scan.
	sel := logicalop.LogicalSelection{
		Conditions: make([]expression.Expression, len(probeConds)),
	}.Init(sctx, ds.QueryBlockOffset())
	copy(sel.Conditions, probeConds)
	sel.SetChildren(clonedDS)

	// Build the aggregation function
	aggFunc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), aggFuncName, []expression.Expression{pkCol.Clone()}, false)
	if err != nil {
		return nil, nil, err
	}

	// Build LogicalAggregation
	agg := logicalop.LogicalAggregation{
		AggFuncs: []*aggregation.AggFuncDesc{aggFunc},
	}.Init(sctx, ds.QueryBlockOffset())

	// Set schema for the aggregation: single column for the result
	aggCol := &expression.Column{
		UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  aggFunc.RetTp,
	}
	agg.SetSchema(expression.NewSchema(aggCol))
	agg.SetChildren(sel)

	// Prune columns on the sub-plan
	_, err = agg.PruneColumns([]*expression.Column{aggCol})
	if err != nil {
		return nil, nil, err
	}

	// Optimize the sub-plan to get a physical plan
	nthPlanBackup := sctx.GetSessionVars().StmtCtx.StmtHints.ForceNthPlan
	sctx.GetSessionVars().StmtCtx.StmtHints.ForceNthPlan = -1
	// Temporarily disable AllowGeneratePKFilter to prevent infinite recursion:
	// adjustOptimizationFlags always adds FlagGeneratePKFilter, and after
	// MaxMinEliminate adds a Limit node, the sub-plan's DataSource would
	// pass all checks in tryGeneratePKFilter, creating another sub-plan, etc.
	oldPKFilter := sctx.GetSessionVars().AllowGeneratePKFilter
	sctx.GetSessionVars().AllowGeneratePKFilter = false
	// Include FlagPredicatePushDown so the Selection's equality conditions
	// are properly pushed to the DataSource's PushedDownConds. Without PPD,
	// the manually-set PushedDownConds on the cloned DataSource can be lost
	// during sub-optimization, causing IndexFullScan instead of
	// IndexRangeScan. The equality conditions used here are simple
	// col = const expressions (verified by matchesIndexColumn), so
	// constant propagation within PPD is safe for these conditions.
	// Include FlagMaxMinEliminate so the optimizer can transform MIN/MAX(pk)
	// into a single-row ordered index scan instead of a full aggregation.
	optFlag := rule.FlagBuildKeyInfo | rule.FlagPruneColumns | rule.FlagMaxMinEliminate | rule.FlagPredicatePushDown
	physicalPlan, _, err := DoOptimize(ctx, sctx, optFlag, agg)
	sctx.GetSessionVars().AllowGeneratePKFilter = oldPKFilter
	sctx.GetSessionVars().StmtCtx.StmtHints.ForceNthPlan = nthPlanBackup
	if err != nil {
		return nil, nil, err
	}
	return physicalPlan, aggFunc.RetTp, nil
}

// pkFilterProbeConds returns the conditions the MIN/MAX probes may use. Every
// row the main query returns satisfies all of its conditions, so bounds
// computed over any subset of them still cover the result. Non-deterministic
// and correlated conditions are dropped: the former could make the probe see
// a different set of rows than the scan, and the latter cannot be evaluated
// outside the outer query.
func pkFilterProbeConds(conds []expression.Expression) []expression.Expression {
	result := make([]expression.Expression, 0, len(conds))
	for _, cond := range conds {
		if expression.IsMutableEffectsExpr(cond) || len(expression.ExtractCorColumns(cond)) > 0 {
			continue
		}
		result = append(result, cond)
	}
	return result
}

const (
	// pkFilterMinIndexRows is the estimated number of rows matching a
	// qualifying index below which the index is not probed when the rule runs
	// without a hint. For so few rows an IndexLookUp on that index costs about
	// as much as the two probe reads, so the probes cannot pay for
	// themselves. It is a heuristic, not a cost-model result.
	pkFilterMinIndexRows = 1000
	// pkFilterMaxCoverage is the estimated fraction of the table within the
	// PK bounds at or above which the bounds are dropped: they hardly narrow
	// a scan, but still cost filter evaluation and probe re-runs on plan-cache
	// reuse.
	pkFilterMaxCoverage = 0.9
)

// pkFilterGateHistColl returns the statistics used to gate PK filter
// generation, or nil when the table has no real statistics to gate on.
func pkFilterGateHistColl(ds *logicalop.DataSource) *statistics.HistColl {
	if ds.StatisticTable == nil || ds.StatisticTable.Pseudo || ds.StatisticTable.RealtimeCount <= 0 {
		return nil
	}
	return ds.StatisticTable.GenerateHistCollFromColumnInfo(ds.TableInfo, ds.TblCols)
}

// estimatePKFilterRows estimates how many rows of the table satisfy conds.
func estimatePKFilterRows(ds *logicalop.DataSource, coll *statistics.HistColl, conds []expression.Expression) float64 {
	sel, err := cardinality.Selectivity(ds.SCtx(), coll, conds, nil)
	if err != nil {
		// Without an estimate, don't let the gate reject anything.
		return float64(pkFilterMinIndexRows)
	}
	return sel * float64(coll.RealtimeCount)
}

// cloneDataSourceForPKFilter creates a clone of the DataSource with the probe
// conditions and a specific index path. It ensures the PK column is present
// in the schema (it may have been pruned from the original DataSource if the
// query doesn't reference it). Including all probe conditions (not just index
// equality conditions) lets the subquery optimizer use additional filters
// like PK prefix conditions to narrow the MIN/MAX scan.
func cloneDataSourceForPKFilter(ds *logicalop.DataSource, idxPath *util.AccessPath, pkCol *expression.Column, probeConds []expression.Expression) *logicalop.DataSource {
	newDs := *ds
	newDs.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ds.SCtx(), ds.TP(), &newDs, ds.QueryBlockOffset())
	schema := ds.Schema().Clone()
	newDs.Columns = make([]*model.ColumnInfo, len(ds.Columns))
	copy(newDs.Columns, ds.Columns)

	// Ensure the PK column is in the schema (it may have been pruned)
	if schema.ColumnIndex(pkCol) == -1 {
		schema.Append(pkCol.Clone().(*expression.Column))
		newDs.Columns = append(newDs.Columns, findOrCreateColumnInfo(ds, pkCol))
	}
	newDs.SetSchema(schema)

	// Include all probe conditions, not just the index equality conditions.
	// This ensures the subquery benefits from all available filters (e.g.
	// a1='abc' AND c=5 instead of just c=5), making the MIN/MAX scan much
	// cheaper when there are additional selective conditions beyond the
	// qualifying index columns.
	newConds := make([]expression.Expression, len(probeConds))
	copy(newConds, probeConds)
	newDs.PushedDownConds = newConds
	newDs.AllConds = make([]expression.Expression, len(probeConds))
	copy(newDs.AllConds, probeConds)

	// Keep the table path and the relevant index path
	newPaths := make([]*util.AccessPath, 0, 2)
	for _, path := range ds.AllPossibleAccessPaths {
		if path.IsTablePath() {
			newPath := *path
			newPaths = append(newPaths, &newPath)
		}
	}
	idxPathCopy := *idxPath
	newPaths = append(newPaths, &idxPathCopy)
	newDs.PossibleAccessPaths = newPaths
	newDs.AllPossibleAccessPaths = newPaths

	// Clear hints and PK filter state to avoid recursive optimization
	newDs.PkFilterHints = nil
	newDs.IndexMergeHints = nil
	newDs.PKFilterConds = nil

	return &newDs
}
