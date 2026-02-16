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
// index lookups using MIN/MAX subqueries. When a query has LIMIT (with or without
// ORDER BY) and multiple candidate indexes, this rule injects
// `pk >= (SELECT MIN(pk) ...) AND pk <= (SELECT MAX(pk) ...)` conditions derived
// from qualifying secondary indexes, narrowing the scan range on the chosen index.
//
// Activation: hint `/*+ pk_filter(table_name, index_name, ...) */`
// or session variable `tidb_opt_generate_pk_filter` (default OFF).
type GeneratePKFilterOptimizer struct{}

// Optimize implements the base.LogicalOptRule interface.
func (*GeneratePKFilterOptimizer) Optimize(ctx context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error) {
	return generatePKFilterWalk(ctx, p, false)
}

// Name implements the base.LogicalOptRule interface.
func (*GeneratePKFilterOptimizer) Name() string {
	return "generate_pk_filter"
}

// generatePKFilterWalk walks the plan tree top-down, tracking whether a LIMIT
// ancestor exists, and applies the PK filter optimization to qualifying DataSources.
func generatePKFilterWalk(ctx context.Context, p base.LogicalPlan, underLimit bool) (base.LogicalPlan, bool, error) {
	// Track whether we are under a LIMIT/TopN node
	switch p.(type) {
	case *logicalop.LogicalLimit, *logicalop.LogicalTopN:
		underLimit = true
	}

	planChanged := false
	for i, child := range p.Children() {
		newChild, changed, err := generatePKFilterWalk(ctx, child, underLimit)
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

	result, changed, err := tryGeneratePKFilter(ctx, ds, underLimit)
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
// conditions like `pk >= 5 AND pk <= 999995`. These constants are pushed into
// PushedDownConds and AllConds so the ranger can use them as access conditions
// on the table path, bounding the clustered PK scan range.
func tryGeneratePKFilter(ctx context.Context, ds *logicalop.DataSource, underLimit bool) (base.LogicalPlan, bool, error) {
	// Check activation: either hint or session variable
	hintMode := len(ds.PkFilterHints) > 0
	sessionMode := ds.SCtx().GetSessionVars().AllowGeneratePKFilter
	if !hintMode && !sessionMode {
		return ds, false, nil
	}

	// Must have a clustered PK
	if ds.HandleCols == nil {
		return ds, false, nil
	}

	// Must have a LIMIT ancestor
	if !underLimit {
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

	// Generate PK filter conditions and track which indexes produced filters.
	newConds := make([]expression.Expression, 0, len(qualifyingIndexes)*2)
	var usedIndexes []*util.AccessPath
	for _, idxPath := range qualifyingIndexes {
		eqConds := checkIndexQualifiesForPKFilter(ds, idxPath)
		if eqConds == nil {
			continue
		}
		conds, err := buildPKFilterConditions(ctx, ds, idxPath, pkCol, eqConds)
		if err != nil {
			return nil, false, err
		}
		if conds == nil {
			// Subquery returned no rows (NULL) — skip this index.
			continue
		}
		newConds = append(newConds, conds...)
		usedIndexes = append(usedIndexes, idxPath)
	}

	if len(newConds) == 0 {
		return ds, false, nil
	}

	// Remove the indexes used for PK filter derivation from the main
	// DataSource's access paths. Using the same index for both the main scan
	// and the PK filter subquery is redundant — the PK range only helps when
	// applied to a *different* access path (e.g. the clustered PK table scan).
	removeUsedIndexesFromPaths(ds, usedIndexes)

	// Ensure the PK column is in the DataSource's schema. It may have been
	// pruned (ColumnPruner runs before this rule) if the query doesn't
	// reference the PK column. The new ge/le conditions reference it, so
	// it must be present for resolveIndices during physical plan building.
	schema := ds.Schema()
	idx := schema.ColumnIndex(pkCol)
	if idx == -1 {
		schema.Append(pkCol.Clone().(*expression.Column))
		for _, colInfo := range ds.TableInfo.Columns {
			if colInfo.ID == pkCol.ID {
				ds.Columns = append(ds.Columns, colInfo)
				break
			}
		}
	}

	// Push the constant PK range conditions to PushedDownConds and AllConds.
	// Because the subqueries were evaluated eagerly, the conditions are simple
	// constant comparisons (e.g. pk >= 5 AND pk <= 999995) that can be pushed
	// to TiKV coprocessor. The ranger will use them as access conditions on
	// the table path, bounding the clustered PK scan range.
	ds.PushedDownConds = append(ds.PushedDownConds, newConds...)
	ds.AllConds = append(ds.AllConds, newConds...)
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

// removeUsedIndexesFromPaths removes the given index paths from the DataSource's
// PossibleAccessPaths and AllPossibleAccessPaths. This prevents the optimizer
// from choosing the same index for the main scan that was used for PK filter
// derivation, which would make the PK filter redundant.
func removeUsedIndexesFromPaths(ds *logicalop.DataSource, usedIndexes []*util.AccessPath) {
	isUsed := func(path *util.AccessPath) bool {
		if path.IsTablePath() || path.Index == nil {
			return false
		}
		for _, used := range usedIndexes {
			if path.Index.Name.L == used.Index.Name.L {
				return true
			}
		}
		return false
	}
	ds.PossibleAccessPaths = filterPaths(ds.PossibleAccessPaths, isUsed)
	ds.AllPossibleAccessPaths = filterPaths(ds.AllPossibleAccessPaths, isUsed)
}

// filterPaths returns a new slice with paths for which exclude returns false.
func filterPaths(paths []*util.AccessPath, exclude func(*util.AccessPath) bool) []*util.AccessPath {
	result := make([]*util.AccessPath, 0, len(paths))
	for _, p := range paths {
		if !exclude(p) {
			result = append(result, p)
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
	col, ok := colExpr.(*expression.Column)
	if !ok {
		return false
	}
	_, isConst := valExpr.(*expression.Constant)
	if !isConst {
		return false
	}
	if int(col.ID) == int(tblInfo.Columns[idxCol.Offset].ID) {
		return true
	}
	return false
}

// buildPKFilterConditions creates `pk_col >= MIN(pk_col)` and `pk_col <= MAX(pk_col)`
// conditions by eagerly evaluating MIN/MAX subqueries during optimization.
// Returns concrete constant conditions (e.g. `pk >= 5 AND pk <= 999995`) that
// the ranger can use as access conditions. Returns nil if the subquery yields
// no rows (the equality conditions match nothing in the index).
func buildPKFilterConditions(ctx context.Context, ds *logicalop.DataSource, idxPath *util.AccessPath, pkCol *expression.Column, eqConds []expression.Expression) ([]expression.Expression, error) {
	sctx := ds.SCtx()
	evalCtx := sctx.GetExprCtx()

	// Evaluate MIN eagerly
	minConst, err := buildAndEvalMinMax(ctx, ds, idxPath, pkCol, eqConds, ast.AggFuncMin)
	if err != nil {
		return nil, err
	}
	if minConst == nil {
		return nil, nil // no rows match the equality conditions
	}

	// Evaluate MAX eagerly
	maxConst, err := buildAndEvalMinMax(ctx, ds, idxPath, pkCol, eqConds, ast.AggFuncMax)
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
func buildAndEvalMinMax(ctx context.Context, ds *logicalop.DataSource, idxPath *util.AccessPath, pkCol *expression.Column, eqConds []expression.Expression, aggFuncName string) (*expression.Constant, error) {
	sctx := ds.SCtx()

	// Clone the DataSource, ensuring the PK column is in the schema
	clonedDS := cloneDataSourceForPKFilter(ds, idxPath, pkCol, eqConds)

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
	agg.SetChildren(clonedDS)

	// Prune columns on the sub-plan
	_, err = agg.PruneColumns([]*expression.Column{aggCol})
	if err != nil {
		return nil, err
	}

	// Optimize the sub-plan to get a physical plan
	nthPlanBackup := sctx.GetSessionVars().StmtCtx.StmtHints.ForceNthPlan
	sctx.GetSessionVars().StmtCtx.StmtHints.ForceNthPlan = -1
	// Do NOT include FlagPredicatePushDown: PPD would clear the conditions
	// we placed on the cloned DataSource (since the agg parent has none to push).
	// Include FlagMaxMinEliminate so the optimizer can transform MIN/MAX(pk)
	// into a single-row ordered index scan instead of a full aggregation.
	optFlag := rule.FlagBuildKeyInfo | rule.FlagPruneColumns | rule.FlagMaxMinEliminate
	physicalPlan, _, err := DoOptimize(ctx, sctx, optFlag, agg)
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
	if row == nil || len(row) == 0 || row[0].IsNull() {
		return nil, nil // no matching rows
	}

	// Create a constant expression from the result datum
	result := &expression.Constant{
		Value:   row[0],
		RetType: aggFunc.RetTp,
	}
	return result, nil
}

// cloneDataSourceForPKFilter creates a clone of the DataSource with only the
// qualifying equality conditions and a specific index path. It ensures the
// PK column is present in the schema (it may have been pruned from the
// original DataSource if the query doesn't reference it).
func cloneDataSourceForPKFilter(ds *logicalop.DataSource, idxPath *util.AccessPath, pkCol *expression.Column, eqConds []expression.Expression) *logicalop.DataSource {
	newDs := *ds
	newDs.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ds.SCtx(), ds.TP(), &newDs, ds.QueryBlockOffset())
	schema := ds.Schema().Clone()
	newDs.Columns = make([]*model.ColumnInfo, len(ds.Columns))
	copy(newDs.Columns, ds.Columns)

	// Ensure the PK column is in the schema (it may have been pruned)
	if schema.ColumnIndex(pkCol) == -1 {
		schema.Append(pkCol.Clone().(*expression.Column))
		// Also add the corresponding ColumnInfo
		for _, colInfo := range ds.TableInfo.Columns {
			if colInfo.ID == pkCol.ID {
				newDs.Columns = append(newDs.Columns, colInfo)
				break
			}
		}
	}
	newDs.SetSchema(schema)

	// Use only the equality conditions for the index.
	// Set both PushedDownConds and AllConds: PushedDownConds is used by stats
	// derivation to compute access conditions; AllConds is used by PruneColumns
	// to protect columns referenced by conditions from being pruned.
	newConds := make([]expression.Expression, len(eqConds))
	copy(newConds, eqConds)
	newDs.PushedDownConds = newConds
	newDs.AllConds = newConds

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

	// Clear hints to avoid recursive optimization
	newDs.PkFilterHints = nil
	newDs.IndexMergeHints = nil

	return &newDs
}
