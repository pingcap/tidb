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
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

// nativeCalcFoundRows reports whether this SELECT should build a count branch.
func nativeCalcFoundRows(opts *ast.SelectStmtOpts) bool {
	return opts != nil && opts.CalcFoundRows && !opts.CalcFoundRowsNoop
}

// GetFoundRowsPlan returns the logical count branch for SQL_CALC_FOUND_ROWS, or nil.
func (b *PlanBuilder) GetFoundRowsPlan() base.LogicalPlan {
	return b.foundRowsPlan
}

func (b *PlanBuilder) buildSelectWithFoundRows(ctx context.Context, sel *ast.SelectStmt) (base.Plan, error) {
	b.buildingFoundRows = true
	b.foundRowsIgnored = false
	defer func() {
		b.buildingFoundRows = false
		b.foundRowsIgnored = false
	}()
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
	count, err := b.buildSelect(ctx, &countSel)
	if err != nil {
		return nil, err
	}
	if !b.foundRowsIgnored {
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
	}
	// AST nodes are shared, but their column bindings must belong to each
	// independent plan. In particular, do not reuse correlated aggregate columns
	// extracted while building the count branch in the data branch.
	clear(b.colMapper)
	clear(b.correlatedAggMapper)
	plan, err := b.buildSelect(ctx, sel)
	if b.foundRowsIgnored {
		// A view expanded above contains expressions that a second count scan
		// must not repeat. Ignore the modifier for this statement.
		if sel.SelectStmtOpts != nil {
			sel.SelectStmtOpts.CalcFoundRowsNoop = true
		}
		b.foundRowsPlan = nil
	}
	return plan, err
}

// keepNoopCalcFoundRows ignores SQL_CALC_FOUND_ROWS when a view expanded while
// building the count plan contains expressions that scan must not repeat.
// ON and WARN keep the behavior from before native support.
func (b *PlanBuilder) keepNoopCalcFoundRows(reason string) bool {
	if reason == "" || !b.buildingFoundRows {
		return false
	}
	mode := b.ctx.GetSessionVars().NoopFuncsMode
	if mode == variable.OffInt {
		return false
	}
	if !b.foundRowsIgnored && mode == variable.WarnInt {
		appendNoopCalcFoundRowsWarning(b.ctx.GetSessionVars())
	}
	b.foundRowsIgnored = true
	return true
}

// preprocessSQLCalcFoundRows checks modifier placement in the same traversal as
// preprocessing, preserving its error order and skipped subtrees.
func preprocessSQLCalcFoundRows(node ast.Node, p *preprocessor) {
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
	ast.Walk(node, &v)
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
	if v.unsupportedReason != "" {
		return v.useNoopCalcFoundRows(stmt, v.unsupportedReason)
	}
	checker := &foundRowsChecker{root: stmt}
	ast.Walk(stmt, checker)
	if checker.reason != "" {
		return v.useNoopCalcFoundRows(stmt, checker.reason)
	}
	return checker.err
}

// useNoopCalcFoundRows keeps statements that have no native count plan running
// when tidb_enable_noop_functions is ON or WARN. Before native support the
// modifier was ignored in those modes. OFF still reports the unsupported shape.
func (v *foundRowsPreprocessor) useNoopCalcFoundRows(stmt *ast.SelectStmt, reason string) error {
	switch v.sctx.GetSessionVars().NoopFuncsMode {
	case variable.OnInt:
		stmt.SelectStmtOpts.CalcFoundRowsNoop = true
		return nil
	case variable.WarnInt:
		stmt.SelectStmtOpts.CalcFoundRowsNoop = true
		appendNoopCalcFoundRowsWarning(v.sctx.GetSessionVars())
		return nil
	default:
		return plannererrors.ErrNotSupportedYet.GenWithStackByArgs(reason)
	}
}

// appendNoopCalcFoundRowsWarning records the historical noop warning once.
// Planning can preprocess the statement again for the non-prepared plan cache.
func appendNoopCalcFoundRowsWarning(vars *variable.SessionVars) {
	warn := expression.ErrFunctionsNoopImpl.GenWithStackByArgs("SQL_CALC_FOUND_ROWS")
	for _, existing := range vars.StmtCtx.GetWarnings() {
		if existing.Err != nil && existing.Err.Error() == warn.Error() {
			return
		}
	}
	vars.StmtCtx.AppendWarning(warn)
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

// foundRowsChecker rejects unsupported SELECT shapes and known non-repeatable
// expressions.
// ORDER BY RAND() is permitted only in the outer query: dropping that ordering
// from the count does not change which rows are counted.
type foundRowsChecker struct {
	root       *ast.SelectStmt
	depth      int
	outerOrder bool
	inView     bool
	reason     string
	err        error
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
		case !v.inView && n != v.root && n.SelectStmtOpts != nil && n.SelectStmtOpts.CalcFoundRows:
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
		if !v.inView && n == v.root.OrderBy {
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
		v.reason = reason
		v.err = plannererrors.ErrNotSupportedYet.GenWithStackByArgs(reason)
	}
	return v.err != nil
}

func (v *foundRowsChecker) Leave(node ast.Node) bool {
	if _, ok := node.(*ast.SelectStmt); ok {
		v.depth--
	}
	if !v.inView && node == v.root.OrderBy {
		v.outerOrder = false
	}
	return v.err == nil
}
