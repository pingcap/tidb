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
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/planner/util"
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
	return generatePKFilterWalk(ctx, p)
}

// Name implements the base.LogicalOptRule interface.
func (*GeneratePKFilterOptimizer) Name() string {
	return "generate_pk_filter"
}

// generatePKFilterWalk walks the plan tree top-down and applies the PK filter
// optimization to qualifying DataSources.
func generatePKFilterWalk(ctx context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error) {
	planChanged := false
	for i, child := range p.Children() {
		newChild, changed, err := generatePKFilterWalk(ctx, child)
		if err != nil {
			return nil, false, err
		}
		if changed {
			planChanged = true
		}
		p.SetChild(i, newChild)
	}

	ds, ok := p.(*logicalop.DataSource)
	if !ok {
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

	// Generate PK filter conditions and track source indexes.
	var pkFilterConds []util.PKFilterCondInfo
	for _, idxPath := range qualifyingIndexes {
		eqConds := checkIndexQualifiesForPKFilter(ds, idxPath)
		if eqConds == nil {
			continue
		}
		conds, err := buildPKFilterConditions(ctx, ds, idxPath, pkCol)
		if err != nil {
			return nil, false, err
		}
		if conds == nil {
			// Subquery returned no rows (NULL) — skip this index.
			continue
		}
		for _, c := range conds {
			pkFilterConds = append(pkFilterConds, util.PKFilterCondInfo{
				Cond:            c,
				SourceIndexID:   idxPath.Index.ID,
				SourceIndexName: idxPath.Index.Name.O,
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

	// The PK filter conditions contain eagerly-evaluated constants from
	// MIN/MAX subqueries. These constants are specific to the current
	// parameter values and must not be reused via plan cache.
	ds.SCtx().GetSessionVars().StmtCtx.SetSkipPlanCache("PK filter conditions contain eagerly-evaluated MIN/MAX constants")

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
		return findHintedIndexPaths(ds)
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
// conditions by eagerly evaluating MIN/MAX subqueries during optimization.
// Returns concrete constant conditions (e.g. `pk >= 5 AND pk <= 999995`) that
// the ranger can use as access conditions. Returns nil if the subquery yields
// no rows (the equality conditions match nothing in the index).
func buildPKFilterConditions(ctx context.Context, ds *logicalop.DataSource, idxPath *util.AccessPath, pkCol *expression.Column) ([]expression.Expression, error) {
	sctx := ds.SCtx()
	evalCtx := sctx.GetExprCtx()

	// Evaluate MIN eagerly
	minConst, err := buildAndEvalMinMax(ctx, ds, idxPath, pkCol, ast.AggFuncMin)
	if err != nil {
		return nil, err
	}
	if minConst == nil {
		return nil, nil // no rows match the equality conditions
	}

	// Evaluate MAX eagerly
	maxConst, err := buildAndEvalMinMax(ctx, ds, idxPath, pkCol, ast.AggFuncMax)
	if err != nil {
		return nil, err
	}
	if maxConst == nil {
		return nil, nil
	}

	var result []expression.Expression

	// pk >= MIN(pk) from index
	geFunc, err := expression.NewFunction(evalCtx, ast.GE, pkCol.GetType(evalCtx.GetEvalCtx()), pkCol.Clone(), minConst)
	if err != nil {
		return nil, err
	}
	result = append(result, geFunc)

	// pk <= MAX(pk) from index
	leFunc, err := expression.NewFunction(evalCtx, ast.LE, pkCol.GetType(evalCtx.GetEvalCtx()), pkCol.Clone(), maxConst)
	if err != nil {
		return nil, err
	}
	result = append(result, leFunc)

	return result, nil
}

// buildAndEvalMinMax builds a MIN or MAX sub-plan, optimizes it, evaluates it
// eagerly, and returns the result as an expression.Constant. Returns nil if the
// subquery produces no rows (e.g. no data matches the equality conditions).
func buildAndEvalMinMax(ctx context.Context, ds *logicalop.DataSource, idxPath *util.AccessPath, pkCol *expression.Column, aggFuncName string) (*expression.Constant, error) {
	sctx := ds.SCtx()

	// Clone the DataSource with all PushedDownConds
	// (needed by stats/ranger for access condition computation).
	clonedDS := cloneDataSourceForPKFilter(ds, idxPath, pkCol)

	// Also create a LogicalSelection carrying ALL the original conditions.
	// MaxMinEliminate's checkColCanUseIndex traverses LogicalSelection nodes
	// to collect conditions — it does NOT look at DataSource.PushedDownConds.
	// Without a Selection, MaxMinEliminate can't verify the index can provide
	// ordering after the equality prefix, resulting in IndexFullScan instead
	// of IndexRangeScan. Including all conditions (not just index eq conditions)
	// lets the optimizer use the PK prefix and other filters to narrow the scan.
	allConds := ds.PushedDownConds
	sel := logicalop.LogicalSelection{
		Conditions: make([]expression.Expression, len(allConds)),
	}.Init(sctx, ds.QueryBlockOffset())
	copy(sel.Conditions, allConds)
	sel.SetChildren(clonedDS)

	// Build the aggregation function
	aggFunc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), aggFuncName, []expression.Expression{pkCol.Clone()}, false)
	if err != nil {
		return nil, err
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
		return nil, err
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
		return nil, err
	}

	// Evaluate the sub-plan eagerly to get the concrete MIN/MAX value.
	// This follows the same pattern as expression_rewriter.go for evaluating
	// non-correlated scalar subqueries during the optimization phase.
	row, err := EvalSubqueryFirstRow(ctx, physicalPlan, ds.IS, sctx)
	if err != nil {
		return nil, err
	}
	if len(row) == 0 || row[0].IsNull() {
		return nil, nil // no matching rows
	}

	// Create a constant expression from the result datum
	result := &expression.Constant{
		Value:   row[0],
		RetType: aggFunc.RetTp,
	}
	return result, nil
}

// cloneDataSourceForPKFilter creates a clone of the DataSource with all the
// original PushedDownConds and a specific index path. It ensures the PK column
// is present in the schema (it may have been pruned from the original
// DataSource if the query doesn't reference it). Including all conditions
// (not just index equality conditions) lets the subquery optimizer use
// additional filters like PK prefix conditions to narrow the MIN/MAX scan.
func cloneDataSourceForPKFilter(ds *logicalop.DataSource, idxPath *util.AccessPath, pkCol *expression.Column) *logicalop.DataSource {
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

	// Include ALL PushedDownConds from the original DataSource, not just the
	// index equality conditions. This ensures the subquery benefits from all
	// available filters (e.g. a1='abc' AND c=5 instead of just c=5), making
	// the MIN/MAX scan much cheaper when there are additional selective
	// conditions beyond the qualifying index columns.
	newConds := make([]expression.Expression, len(ds.PushedDownConds))
	copy(newConds, ds.PushedDownConds)
	newDs.PushedDownConds = newConds
	newDs.AllConds = make([]expression.Expression, len(ds.PushedDownConds))
	copy(newDs.AllConds, ds.PushedDownConds)

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
