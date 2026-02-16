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

	changed, err := tryGeneratePKFilter(ctx, ds, underLimit)
	if err != nil {
		return nil, false, err
	}
	if changed {
		planChanged = true
	}
	return p, planChanged, nil
}

// tryGeneratePKFilter checks whether the DataSource qualifies for PK filter
// generation and generates the filter conditions if so.
func tryGeneratePKFilter(ctx context.Context, ds *logicalop.DataSource, underLimit bool) (bool, error) {
	// Check activation: either hint or session variable
	hintMode := len(ds.PkFilterHints) > 0
	sessionMode := ds.SCtx().GetSessionVars().AllowGeneratePKFilter
	if !hintMode && !sessionMode {
		return false, nil
	}

	// Must have a clustered PK
	if ds.HandleCols == nil {
		return false, nil
	}

	// Must have a LIMIT ancestor
	if !underLimit {
		return false, nil
	}

	// Must have pushed-down conditions
	if len(ds.PushedDownConds) == 0 {
		return false, nil
	}

	// Find the target PK column
	pkCol := findTargetPKCol(ds)
	if pkCol == nil {
		return false, nil
	}

	// Determine which indexes to generate filters from
	qualifyingIndexes := findQualifyingIndexPaths(ds, hintMode)
	if len(qualifyingIndexes) == 0 {
		return false, nil
	}

	// Generate PK filter conditions
	newConds := make([]expression.Expression, 0, len(qualifyingIndexes)*2)
	for _, idxPath := range qualifyingIndexes {
		eqConds := checkIndexQualifiesForPKFilter(ds, idxPath)
		if eqConds == nil {
			continue
		}
		conds, err := buildPKFilterConditions(ctx, ds, idxPath, pkCol, eqConds)
		if err != nil {
			return false, err
		}
		newConds = append(newConds, conds...)
	}

	if len(newConds) == 0 {
		return false, nil
	}

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

	// Inject conditions into DataSource's PushedDownConds and AllConds.
	// AllConds must also be updated because a second ColumnPruner pass runs
	// after this rule (see optRuleList in optimizer.go). PruneColumns uses
	// AllConds to determine which columns to protect from pruning.
	ds.PushedDownConds = append(ds.PushedDownConds, newConds...)
	ds.AllConds = append(ds.AllConds, newConds...)
	return true, nil
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
// conditions using optimized sub-plans from a cloned DataSource.
func buildPKFilterConditions(ctx context.Context, ds *logicalop.DataSource, idxPath *util.AccessPath, pkCol *expression.Column, eqConds []expression.Expression) ([]expression.Expression, error) {
	sctx := ds.SCtx()
	evalCtx := sctx.GetExprCtx()

	// Build the MIN sub-plan
	minExpr, err := buildMinMaxScalarSubQuery(ctx, ds, idxPath, pkCol, eqConds, ast.AggFuncMin)
	if err != nil {
		return nil, err
	}

	// Build the MAX sub-plan
	maxExpr, err := buildMinMaxScalarSubQuery(ctx, ds, idxPath, pkCol, eqConds, ast.AggFuncMax)
	if err != nil {
		return nil, err
	}

	var result []expression.Expression

	// pk >= MIN(pk) from index
	geFunc, err := expression.NewFunction(evalCtx, ast.GE, pkCol.GetType(evalCtx.GetEvalCtx()), pkCol.Clone(), minExpr)
	if err != nil {
		return nil, err
	}
	result = append(result, geFunc)

	// pk <= MAX(pk) from index
	leFunc, err := expression.NewFunction(evalCtx, ast.LE, pkCol.GetType(evalCtx.GetEvalCtx()), pkCol.Clone(), maxExpr)
	if err != nil {
		return nil, err
	}
	result = append(result, leFunc)

	return result, nil
}

// buildMinMaxScalarSubQuery builds a scalar subquery expression for MIN or MAX
// of the PK column over a cloned DataSource filtered by the equality conditions.
func buildMinMaxScalarSubQuery(ctx context.Context, ds *logicalop.DataSource, idxPath *util.AccessPath, pkCol *expression.Column, eqConds []expression.Expression, aggFuncName string) (expression.Expression, error) {
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
	optFlag := rule.FlagPredicatePushDown | rule.FlagBuildKeyInfo | rule.FlagPruneColumns
	physicalPlan, _, err := DoOptimize(ctx, sctx, optFlag, agg)
	sctx.GetSessionVars().StmtCtx.StmtHints.ForceNthPlan = nthPlanBackup
	if err != nil {
		return nil, err
	}

	// Create ScalarSubQueryExpr wrapping the physical plan
	newColID := sctx.GetSessionVars().AllocPlanColumnID()
	subqueryCtx := ScalarSubqueryEvalCtx{
		scalarSubQuery: physicalPlan,
		ctx:            ctx,
		is:             ds.IS,
		outputColIDs:   []int64{newColID},
	}.Init(sctx, ds.QueryBlockOffset())

	scalarSubQ := &ScalarSubQueryExpr{
		scalarSubqueryColID: newColID,
		evalCtx:             subqueryCtx,
	}
	scalarSubQ.RetType = aggFunc.RetTp
	sctx.GetSessionVars().RegisterScalarSubQ(subqueryCtx)

	return scalarSubQ, nil
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

	// Use only the equality conditions for the index
	newConds := make([]expression.Expression, len(eqConds))
	copy(newConds, eqConds)
	newDs.PushedDownConds = newConds

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
