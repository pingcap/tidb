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
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

// GetFoundRowsPlan returns the logical count branch for SQL_CALC_FOUND_ROWS, or nil.
func (b *PlanBuilder) GetFoundRowsPlan() base.LogicalPlan {
	return b.foundRowsPlan
}

func (b *PlanBuilder) buildSelectWithFoundRows(ctx context.Context, sel *ast.SelectStmt) (base.Plan, error) {
	b.buildingFoundRows = true
	b.foundRowsViewNoop = false
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
	// View definitions are resolved during the data build. If one requires
	// fallback, keep this plan and skip building the unlimited branch.
	data, err := b.buildSelect(ctx, sel)
	if err != nil {
		return nil, err
	}
	if b.foundRowsViewNoop {
		if b.resolveCtx.CalcFoundRowsMode == resolve.CalcFoundRowsWarnNoop {
			appendCalcFoundRowsNoopWarning(b.ctx.GetSessionVars())
		}
		return data, nil
	}

	// AST nodes are shared, but column bindings must belong to each branch.
	clear(b.colMapper)
	clear(b.correlatedAggMapper)
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
	return data, nil
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
	p.resolveCtx.CalcFoundRowsMode = resolve.CalcFoundRowsNative
	if v.allowedSelect != nil && v.allowedSelect.SelectStmtOpts != nil && v.allowedSelect.SelectStmtOpts.CalcFoundRows {
		switch p.sctx.GetSessionVars().NoopFuncsMode {
		case variable.OnInt:
			p.resolveCtx.CalcFoundRowsMode = resolve.CalcFoundRowsAllowNoop
		case variable.WarnInt:
			p.resolveCtx.CalcFoundRowsMode = resolve.CalcFoundRowsWarnNoop
		}
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
			mode := v.resolveCtx.CalcFoundRowsMode
			if !plannererrors.ErrNotSupportedYet.Equal(err) || mode == resolve.CalcFoundRowsNative {
				v.err = err
			} else {
				if mode == resolve.CalcFoundRowsWarnNoop {
					appendCalcFoundRowsNoopWarning(v.sctx.GetSessionVars())
				}
				v.resolveCtx.CalcFoundRowsMode = resolve.CalcFoundRowsNoop
			}
		}
	}
	return v.preprocessor.Enter(node)
}

func appendCalcFoundRowsNoopWarning(vars *variable.SessionVars) {
	warning := expression.ErrFunctionsNoopImpl.FastGenByArgs("SQL_CALC_FOUND_ROWS")
	// A non-prepared cache miss can preprocess both the original and the
	// parameterized AST in the same statement.
	for _, existing := range vars.StmtCtx.GetWarnings() {
		if expression.ErrFunctionsNoopImpl.Equal(existing.Err) && existing.Err.Error() == warning.Error() {
			return
		}
	}
	vars.StmtCtx.AppendWarning(warning)
}

// omitCalcFoundRows copies the path to the leading SELECT for consumers that
// preprocess or persist a query outside the context that selected the fallback.
func omitCalcFoundRows(node ast.Node) ast.Node {
	switch n := node.(type) {
	case *ast.SelectStmt:
		if n.SelectStmtOpts != nil && n.SelectStmtOpts.CalcFoundRows {
			cloned := *n
			opts := *n.SelectStmtOpts
			opts.CalcFoundRows = false
			cloned.SelectStmtOpts = &opts
			return &cloned
		}
	case *ast.SetOprStmt:
		cloned := *n
		cloned.SelectList = omitCalcFoundRows(n.SelectList).(*ast.SetOprSelectList)
		return &cloned
	case *ast.SetOprSelectList:
		if len(n.Selects) > 0 {
			cloned := *n
			cloned.Selects = slices.Clone(n.Selects)
			cloned.Selects[0] = omitCalcFoundRows(n.Selects[0])
			return &cloned
		}
	case *ast.InsertStmt:
		if n.Select != nil {
			cloned := *n
			cloned.Select = omitCalcFoundRows(n.Select).(ast.ResultSetNode)
			return &cloned
		}
	case *ast.CreateViewStmt:
		cloned := *n
		cloned.Select = omitCalcFoundRows(n.Select).(ast.StmtNode)
		return &cloned
	case *ast.ExplainStmt:
		if n.Stmt != nil {
			cloned := *n
			cloned.Stmt = omitCalcFoundRows(n.Stmt).(ast.StmtNode)
			return &cloned
		}
	}
	return node
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
		return plannererrors.ErrNotSupportedYet.GenWithStackByArgs(v.unsupportedReason)
	}
	checker := &foundRowsChecker{root: stmt}
	ast.Walk(stmt, checker)
	return checker.err
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
