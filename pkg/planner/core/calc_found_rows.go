// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

// shouldCalcFoundRows reports whether this SELECT should build a count branch.
func shouldCalcFoundRows(ctx base.PlanContext, opts *ast.SelectStmtOpts) bool {
	return opts != nil && opts.CalcFoundRows && !ctx.GetSessionVars().StmtCtx.IgnoreCalcFoundRows
}

// GetFoundRowsPlan returns the logical count branch for SQL_CALC_FOUND_ROWS, or nil.
func (b *PlanBuilder) GetFoundRowsPlan() base.LogicalPlan {
	return b.foundRowsPlan
}

func (b *PlanBuilder) buildSelectWithFoundRows(ctx context.Context, sel *ast.SelectStmt) (base.Plan, error) {
	b.buildingFoundRows = true
	defer func() { b.buildingFoundRows = false }()
	countSel := *sel
	countSel.Limit = nil
	countSel.OrderBy = nil
	// Preserve the query's aggregation when it comes only from ORDER BY.
	// For example, TiDB accepts SELECT 1 FROM t ORDER BY COUNT(*) in permissive mode.
	// Removing ORDER BY changes its one-row result into one row per input row,
	// causing the count branch to report the wrong total.
	if b.detectSelectAgg(sel) && !b.detectSelectAgg(&countSel) {
		countSel.OrderBy = sel.OrderBy
	}
	fields := *sel.Fields
	fields.Fields = slices.Clone(sel.Fields.Fields)
	countSel.Fields = &fields
	// Build the data branch first so view-triggered noop fallback skips the
	// count branch altogether and never evaluates non-correlated subqueries twice.
	plan, err := b.buildSelect(ctx, sel)
	if err != nil {
		return nil, err
	}
	if b.ctx.GetSessionVars().StmtCtx.IgnoreCalcFoundRows {
		return plan, nil
	}
	// AST nodes are shared, but their column bindings must belong to each
	// independent plan. In particular, do not reuse correlated aggregate columns
	// extracted while building the data branch in the count branch.
	clear(b.colMapper)
	clear(b.correlatedAggMapper)
	b.currentBlockExpand = nil
	count, err := b.buildSelect(ctx, &countSel)
	if err != nil {
		return nil, err
	}
	count, _, err = b.buildAggregation(ctx, count, []*ast.AggregateFuncExpr{{
		F: ast.AggFuncCount, Args: []ast.ExprNode{ast.NewValueExpr(1, "", "")},
	}}, nil, nil)
	if err != nil {
		return nil, err
	}
	// buildAggregation also exposes FIRST_ROW expressions for its input columns.
	// Retain only COUNT, allowing normal column pruning to discard those inputs.
	projection := logicalop.LogicalProjection{
		Exprs: []expression.Expression{count.Schema().Columns[0]},
	}.Init(b.ctx, b.getSelectOffset())
	projection.SetChildren(count)
	projection.SetSchema(expression.NewSchema(count.Schema().Columns[0]))
	projection.SetOutputNames(count.OutputNames()[:1])
	b.foundRowsPlan = projection
	return plan, nil
}

// checkViewForFoundRows validates an expanded view definition while building
// a SQL_CALC_FOUND_ROWS plan.
func (b *PlanBuilder) checkViewForFoundRows(selectNode ast.Node) error {
	vars := b.ctx.GetSessionVars()
	if vars.StmtCtx.IgnoreCalcFoundRows {
		return nil
	}
	return checkFoundRowsView(vars, selectNode)
}

// handleUnsupportedCalcFoundRows applies tidb_enable_noop_functions when a
// valid SQL_CALC_FOUND_ROWS placement has no native count plan.
func handleUnsupportedCalcFoundRows(vars *variable.SessionVars, err error) error {
	switch vars.NoopFuncsMode {
	case variable.OnInt:
		vars.StmtCtx.IgnoreCalcFoundRows = true
		return nil
	case variable.WarnInt:
		vars.StmtCtx.IgnoreCalcFoundRows = true
		// Planning can preprocess or revalidate the same statement more than
		// once (for example, in the non-prepared plan cache), so record the
		// noop warning at most once per statement.
		warn := expression.ErrFunctionsNoopImpl.FastGenByArgs("SQL_CALC_FOUND_ROWS")
		for _, existing := range vars.StmtCtx.GetWarnings() {
			if existing.Err != nil && existing.Err.Error() == warn.Error() {
				return nil
			}
		}
		vars.StmtCtx.AppendWarning(warn)
		return nil
	default:
		return err
	}
}

func newFoundRowsPreprocessor(node ast.Node, p *preprocessor) foundRowsPreprocessor {
	query := node
	if explain, ok := query.(*ast.ExplainStmt); ok {
		query = explain.Stmt
	}
	v := foundRowsPreprocessor{
		preprocessor:  p,
		allowedSelect: findSQLCalcFoundRowsAllowedSelect(query),
	}
	if v.allowedSelect != query {
		v.unsupportedReason = "SQL_CALC_FOUND_ROWS outside a top-level SELECT"
		if _, ok := query.(*ast.SetOprStmt); ok {
			v.unsupportedReason = "SQL_CALC_FOUND_ROWS with set operations"
		}
	}
	return v
}

// preprocessSQLCalcFoundRows checks modifier placement in the same traversal as
// preprocessing, preserving its error order and skipped subtrees.
func preprocessSQLCalcFoundRows(node ast.Node, p *preprocessor) {
	v := newFoundRowsPreprocessor(node, p)
	ast.Walk(node, &v)
}

// revalidateSQLCalcFoundRows re-applies tidb_enable_noop_functions for
// prepared/cached statements whose AST was preprocessed in an earlier mode.
func revalidateSQLCalcFoundRows(sctx sessionctx.Context, node ast.Node) error {
	v := newFoundRowsPreprocessor(node, nil)
	if v.allowedSelect == nil || v.allowedSelect.SelectStmtOpts == nil ||
		!v.allowedSelect.SelectStmtOpts.CalcFoundRows {
		return nil
	}
	v.preprocessor = &preprocessor{sctx: sctx}
	return v.checkSelect(v.allowedSelect)
}

// foundRowsPreprocessor validates SELECT entry while retaining the preprocessor's
// Enter/Leave pairing, including lock-target checks on rejected SELECTs.
type foundRowsPreprocessor struct {
	*preprocessor
	allowedSelect     *ast.SelectStmt
	unsupportedReason string
}

func (v *foundRowsPreprocessor) Enter(node ast.Node) bool {
	if stmt, ok := node.(*ast.SelectStmt); ok {
		if err := v.checkSelect(stmt); err != nil {
			v.err = err
		}
	}
	return v.preprocessor.Enter(node)
}

func (v *foundRowsPreprocessor) checkSelect(stmt *ast.SelectStmt) error {
	// TABLE and VALUES also use SelectStmt, but have no SelectStmtOpts.
	if stmt.SelectStmtOpts == nil || !stmt.SelectStmtOpts.CalcFoundRows {
		return nil
	}
	if stmt != v.allowedSelect {
		return plannererrors.ErrCantUseOptionHere.GenWithStackByArgs("SQL_CALC_FOUND_ROWS")
	}
	vars := v.sctx.GetSessionVars()
	if v.unsupportedReason != "" {
		return handleUnsupportedCalcFoundRows(
			vars, plannererrors.ErrNotSupportedYet.GenWithStackByArgs(v.unsupportedReason))
	}
	return checkFoundRowsSelect(vars, stmt)
}

// findSQLCalcFoundRowsAllowedSelect returns the SELECT where the modifier is
// allowed, including statements whose counting semantics we do not yet support.
func findSQLCalcFoundRowsAllowedSelect(node ast.Node) *ast.SelectStmt {
	for node != nil {
		switch n := node.(type) {
		case *ast.SelectStmt:
			return n
		case *ast.SetOprStmt:
			node = n.SelectList
		case *ast.SetOprSelectList:
			if len(n.Selects) == 0 {
				return nil
			}
			node = n.Selects[0]
		case *ast.ExplainStmt:
			node = n.Stmt
		case *ast.InsertStmt:
			node = n.Select
		case *ast.CreateViewStmt:
			node = n.Select
		case *ast.TraceStmt:
			node = n.Stmt
		case *ast.PlanReplayerStmt:
			node = n.Stmt
		default:
			return nil
		}
	}
	return nil
}

func checkFoundRowsSelect(vars *variable.SessionVars, stmt *ast.SelectStmt) error {
	return checkFoundRowsAST(vars, stmt, stmt)
}

func checkFoundRowsView(vars *variable.SessionVars, selectNode ast.Node) error {
	return checkFoundRowsAST(vars, selectNode, nil)
}

func checkFoundRowsAST(vars *variable.SessionVars, node ast.Node, outerSelect *ast.SelectStmt) error {
	checker := &foundRowsChecker{outerSelect: outerSelect}
	ast.Walk(node, checker)
	if plannererrors.ErrNotSupportedYet.Equal(checker.err) {
		return handleUnsupportedCalcFoundRows(vars, checker.err)
	}
	return checker.err
}

// foundRowsChecker rejects unsupported SELECT shapes and known non-repeatable
// expressions.
// ORDER BY RAND() is permitted only in the outer query: dropping that ordering
// from the count does not change which rows are counted.
type foundRowsChecker struct {
	// outerSelect is the top-level SELECT carrying SQL_CALC_FOUND_ROWS, or nil
	// when checking an expanded view body.
	outerSelect *ast.SelectStmt
	depth       int
	outerOrder  bool
	err         error
}

func (v *foundRowsChecker) Enter(node ast.Node) bool {
	if v.err != nil {
		return true
	}
	reason := ""
	switch n := node.(type) {
	case *ast.SelectStmt:
		v.depth++
		switch {
		case v.outerSelect != nil && n != v.outerSelect && n.SelectStmtOpts != nil && n.SelectStmtOpts.CalcFoundRows:
			v.err = plannererrors.ErrCantUseOptionHere.GenWithStackByArgs("SQL_CALC_FOUND_ROWS")
			return true
		case n.LockInfo != nil && n.LockInfo.LockType != ast.SelectLockNone:
			reason = "SQL_CALC_FOUND_ROWS with locking reads"
		case n.SelectIntoOpt != nil:
			reason = "SQL_CALC_FOUND_ROWS with SELECT INTO"
		}
	case *ast.WindowFuncExpr:
		reason = "SQL_CALC_FOUND_ROWS with window functions"
	case *ast.OrderByClause:
		if v.outerSelect != nil && n == v.outerSelect.OrderBy {
			v.outerOrder = true
		}
	case *ast.VariableExpr:
		if n.Value != nil {
			reason = "SQL_CALC_FOUND_ROWS with variable assignments"
		}
	case *ast.FuncCallExpr:
		switch n.FnName.L {
		case "rand":
			if !v.outerOrder || v.depth != 1 {
				reason = "SQL_CALC_FOUND_ROWS with random expressions outside outer ORDER BY"
			}
		case "uuid", "uuid_v4", "uuid_v7", "uuid_short", "sysdate", "sleep", "benchmark",
			"get_lock", "is_free_lock", "is_used_lock", "release_lock", "release_all_locks",
			"nextval", "lastval", "setval", "setvar", "random_bytes", "embed_text":
			reason = "SQL_CALC_FOUND_ROWS with non-repeatable expressions"
		case "last_insert_id":
			if len(n.Args) != 0 {
				reason = "SQL_CALC_FOUND_ROWS with session-state changes"
			}
		}
	}
	if reason != "" {
		v.err = plannererrors.ErrNotSupportedYet.GenWithStackByArgs(reason)
	}
	return v.err != nil
}

func (v *foundRowsChecker) Leave(node ast.Node) bool {
	if _, ok := node.(*ast.SelectStmt); ok {
		v.depth--
	}
	if v.outerSelect != nil && node == v.outerSelect.OrderBy {
		v.outerOrder = false
	}
	return v.err == nil
}
