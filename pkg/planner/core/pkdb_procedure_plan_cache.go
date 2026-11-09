// Copyright 2026 PingCAP, Inc.

package core

import (
	"context"
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

const (
	procedurePlanCacheDisabledReason = "plan cache is disabled"
	procedurePlanCacheExecuteName    = "_tidb_sp_plan_cache"
	procedurePlanCacheWarningPrefix  = "skip prepared plan-cache: "
)

type procedurePlanCacheStmt struct {
	preparedStmt *PlanCacheStmt
	params       []procedurePlanCacheParam
	unsupported  bool
}

type procedurePlanCacheParam struct {
	name        string
	displayName string
	fieldType   *types.FieldType
}

func (p *executeBaseSQL) getProcedurePlanCacheExecuteStmt(
	ctx context.Context,
	sctx sessionctx.Context,
	stmt ast.StmtNode,
	stmtIdx int,
) (*ast.ExecuteStmt, error) {
	if p.cacheStmt == nil || p.context == nil || !sctx.GetSessionVars().EnablePreparedPlanCache || !sctx.GetSessionVars().EnableSPPlanCache {
		return nil, nil
	}
	if procedurePlanCacheAlwaysUnsupported(stmt) {
		p.markProcedurePlanCacheUnsupported(stmtIdx)
		return nil, nil
	}

	entry := p.procedurePlanCacheEntry(stmtIdx)
	if entry == nil || entry.unsupported {
		return nil, nil
	}
	if entry.preparedStmt == nil || entry.preparedStmt.UncacheableReason == procedurePlanCacheDisabledReason {
		if ok := p.buildProcedurePlanCacheStmt(ctx, sctx, stmt, entry); !ok {
			return nil, nil
		}
	}
	params, err := entry.buildParams(sctx)
	if err != nil {
		return nil, err
	}
	return &ast.ExecuteStmt{
		Name:       procedurePlanCacheExecuteName,
		PrepStmt:   entry.preparedStmt,
		BinaryArgs: params,
	}, nil
}

func (p *executeBaseSQL) procedurePlanCacheEntry(stmtIdx int) *procedurePlanCacheStmt {
	if stmtIdx < 0 {
		return nil
	}
	if len(p.cacheStmt.planCacheStmts) != len(p.cacheStmt.stmts) {
		p.cacheStmt.planCacheStmts = make([]*procedurePlanCacheStmt, len(p.cacheStmt.stmts))
	}
	if stmtIdx >= len(p.cacheStmt.planCacheStmts) {
		return nil
	}
	entry := p.cacheStmt.planCacheStmts[stmtIdx]
	if entry == nil {
		entry = &procedurePlanCacheStmt{}
		p.cacheStmt.planCacheStmts[stmtIdx] = entry
	}
	return entry
}

func (p *executeBaseSQL) markProcedurePlanCacheUnsupported(stmtIdx int) {
	entry := p.procedurePlanCacheEntry(stmtIdx)
	if entry != nil {
		entry.unsupported = true
		entry.preparedStmt = nil
		entry.params = nil
	}
}

func (p *executeBaseSQL) buildProcedurePlanCacheStmt(
	ctx context.Context,
	sctx sessionctx.Context,
	stmt ast.StmtNode,
	entry *procedurePlanCacheStmt,
) bool {
	paramSQL, paramStmt, params, unsupported, ok := parameterizeProcedurePlanCacheStmt(ctx, sctx, stmt, p.context)
	if !ok {
		if unsupported {
			entry.unsupported = true
		}
		return false
	}
	is := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()
	preparedStmt, err := generateProcedurePlanCacheStmt(ctx, sctx, paramSQL, paramStmt, len(params), is)
	if err != nil {
		return false
	}
	if !preparedStmt.StmtCacheable {
		if preparedStmt.UncacheableReason != procedurePlanCacheDisabledReason {
			entry.unsupported = true
		}
		return false
	}
	entry.preparedStmt = preparedStmt
	entry.params = params
	return true
}

func generateProcedurePlanCacheStmt(
	ctx context.Context,
	sctx sessionctx.Context,
	paramSQL string,
	paramStmt ast.StmtNode,
	paramCount int,
	is infoschema.InfoSchema,
) (*PlanCacheStmt, error) {
	vars := sctx.GetSessionVars()
	stmtCtx := vars.StmtCtx
	relatedTableIDs := stmtCtx.RelatedTableIDs
	savedWarnings := stmtCtx.CopyWarnings(nil)
	savedExtraWarnings := append([]stmtctx.SQLWarn(nil), stmtCtx.GetExtraWarnings()...)
	savedPlanCacheParams := append([]types.Datum(nil), vars.PlanCacheParams.AllParamValues()...)
	savedParamsHidden := vars.PlanCacheParams.String() == "" && len(savedPlanCacheParams) > 0
	// GeneratePlanCacheStmtWithAST uses RelatedTableIDs to build schema fences.
	// Routine internal statements share the outer CALL StmtCtx, so each hidden
	// prepared template must collect only its own tables.
	stmtCtx.RelatedTableIDs = make(map[int64]struct{})
	vars.PlanCacheParams.Reset()
	for i := 0; i < paramCount; i++ {
		var nullParam types.Datum
		nullParam.SetNull()
		vars.PlanCacheParams.Append(nullParam)
	}
	defer func() {
		stmtCtx.RelatedTableIDs = relatedTableIDs
		stmtCtx.SetWarnings(savedWarnings)
		stmtCtx.SetExtraWarnings(savedExtraWarnings)
		vars.PlanCacheParams.Reset()
		if len(savedPlanCacheParams) > 0 {
			vars.PlanCacheParams.Append(savedPlanCacheParams...)
			vars.PlanCacheParams.SetForNonPrepCache(savedParamsHidden)
		}
	}()

	preparedStmt, _, _, err := GeneratePlanCacheStmtWithAST(ctx, sctx, true, paramSQL, paramStmt, is)
	if preparedStmt != nil {
		preparedStmt.ReplayWarningsOnHit = true
	}
	return preparedStmt, err
}

func executeWithProcedurePlanCacheWarningIsolation(vars *variable.SessionVars, fn func() error) error {
	if vars == nil || vars.StmtCtx == nil {
		return fn()
	}
	originalStmtCtx := vars.StmtCtx
	savedWarnings := originalStmtCtx.CopyWarnings(nil)
	savedExtraWarnings := append([]stmtctx.SQLWarn(nil), originalStmtCtx.GetExtraWarnings()...)
	err := fn()
	currentStmtCtx := vars.StmtCtx
	if currentStmtCtx == nil {
		return err
	}
	if currentStmtCtx == originalStmtCtx {
		currentStmtCtx.SetWarnings(filterProcedurePlanCacheWarnings(savedWarnings, currentStmtCtx.GetWarnings()))
		currentStmtCtx.SetExtraWarnings(filterProcedurePlanCacheWarnings(savedExtraWarnings, currentStmtCtx.GetExtraWarnings()))
		return err
	}
	currentStmtCtx.SetWarnings(filterProcedurePlanCacheWarnings(nil, currentStmtCtx.GetWarnings()))
	currentStmtCtx.SetExtraWarnings(filterProcedurePlanCacheWarnings(nil, currentStmtCtx.GetExtraWarnings()))
	return err
}

func filterProcedurePlanCacheWarnings(base, current []stmtctx.SQLWarn) []stmtctx.SQLWarn {
	filtered := append([]stmtctx.SQLWarn(nil), base...)
	if len(current) <= len(base) {
		return filtered
	}
	for _, warn := range current[len(base):] {
		if warn.Err != nil && strings.HasPrefix(warn.Err.Error(), procedurePlanCacheWarningPrefix) {
			continue
		}
		filtered = append(filtered, warn)
	}
	return filtered
}

// IsProcedurePlanCacheExecuteStmt reports whether stmt is the hidden EXECUTE
// carrier used for stored-routine internal plan-cache reuse.
func IsProcedurePlanCacheExecuteStmt(stmt *ast.ExecuteStmt) bool {
	return stmt != nil && strings.EqualFold(stmt.Name, procedurePlanCacheExecuteName)
}

func (entry *procedurePlanCacheStmt) buildParams(sctx sessionctx.Context) ([]expression.Expression, error) {
	params := make([]expression.Expression, 0, len(entry.params))
	for _, param := range entry.params {
		_, datum, notFind := sctx.GetSessionVars().GetProcedureVariable(param.name)
		if notFind {
			return nil, plannererrors.ErrSpUndeclaredVar.GenWithStackByArgs(param.displayName)
		}
		retType := param.fieldType.Clone()
		value := datum.Clone()
		value.SetValue(value.GetValue(), retType)
		constant := &expression.Constant{Value: *value, RetType: retType}
		initConstantRepertoire(sctx.GetExprCtx().GetEvalCtx(), constant)
		params = append(params, constant)
	}
	return params, nil
}

func procedurePlanCacheAlwaysUnsupported(stmt ast.StmtNode) bool {
	switch stmt.(type) {
	case ast.DDLNode,
		*ast.PrepareStmt,
		*ast.ExecuteStmt,
		*ast.DeallocateStmt,
		*ast.ImportIntoStmt,
		*ast.LoadDataStmt,
		*ast.NonTransactionalDMLStmt,
		*ast.CreateProcedureInfo,
		*ast.AlterProcedureStmt,
		*ast.DropProcedureStmt,
		*ast.Signal,
		*ast.GetDiagnosticsStmt,
		*ast.CallStmt:
		return true
	}
	if showStmt, ok := stmt.(*ast.ShowStmt); ok {
		switch showStmt.Tp {
		case ast.ShowWarnings, ast.ShowErrors, ast.ShowSessionStates:
			// These statements inspect the current statement context directly, so
			// routing them through a hidden EXECUTE breaks the warnings/session
			// state they are supposed to surface.
			return true
		}
	}
	return false
}

func parameterizeProcedurePlanCacheStmt(
	ctx context.Context,
	sctx sessionctx.Context,
	stmt ast.StmtNode,
	procedureCtx *variable.ProcedureContext,
) (string, ast.StmtNode, []procedurePlanCacheParam, bool, bool) {
	clonedStmt, ok := cloneProcedurePlanCacheStmt(ctx, sctx, stmt)
	if !ok {
		return "", nil, nil, false, false
	}
	rewriter := &procedurePlanCacheParamRewriter{
		procedureCtx: procedureCtx,
		inTrigger:    sctx.GetSessionVars().StmtCtx.TriggerCtx.InTrigger,
	}
	clonedStmt.Accept(rewriter)
	if rewriter.err != nil || rewriter.unsupported {
		return "", nil, nil, rewriter.unsupported, false
	}
	paramSQL, ok := restoreProcedurePlanCacheStmt(clonedStmt)
	if !ok {
		return "", nil, nil, false, false
	}
	paramStmt, ok := parseOneProcedurePlanCacheStmt(ctx, sctx, paramSQL)
	if !ok {
		return "", nil, nil, false, false
	}
	return paramSQL, paramStmt, rewriter.params, false, true
}

func cloneProcedurePlanCacheStmt(ctx context.Context, sctx sessionctx.Context, stmt ast.StmtNode) (ast.StmtNode, bool) {
	sql, ok := restoreProcedurePlanCacheStmt(stmt)
	if !ok {
		return nil, false
	}
	return parseOneProcedurePlanCacheStmt(ctx, sctx, sql)
}

func restoreProcedurePlanCacheStmt(stmt ast.StmtNode) (string, bool) {
	var sb strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := stmt.Restore(restoreCtx); err != nil {
		return "", false
	}
	return sb.String(), true
}

func parseOneProcedurePlanCacheStmt(ctx context.Context, sctx sessionctx.Context, sql string) (ast.StmtNode, bool) {
	parser, ok := sctx.(sqlexec.SQLParser)
	if !ok {
		return nil, false
	}
	stmts, _, err := parser.ParseSQL(ctx, sql)
	if err != nil || len(stmts) != 1 {
		return nil, false
	}
	return stmts[0], true
}

type procedurePlanCacheParamRewriter struct {
	procedureCtx *variable.ProcedureContext
	params       []procedurePlanCacheParam
	err          error
	unsupported  bool
	inTrigger    bool
	aliasDepth   int
	limitDepth   int
}

func (r *procedurePlanCacheParamRewriter) Enter(in ast.Node) (ast.Node, bool) {
	if r.err != nil || r.unsupported || r.procedureCtx == nil {
		return in, true
	}
	switch node := in.(type) {
	case *ast.GroupByClause, *ast.HavingClause, *ast.OrderByClause:
		r.aliasDepth++
	case *ast.Limit:
		r.limitDepth++
	case *ast.SelectField:
		r.preserveSelectFieldAlias(node)
	case *ast.SelectIntoOption:
		return node, true
	case *ast.ProcedureVar:
		return r.replaceProcedureVar(node)
	case *ast.ColumnNameExpr:
		return r.replaceColumnNameExpr(node)
	}
	return in, false
}

func (r *procedurePlanCacheParamRewriter) preserveSelectFieldAlias(field *ast.SelectField) {
	if field.AsName.L != "" {
		return
	}
	col, ok := getInnerFromParenthesesAndUnaryPlus(field.Expr).(*ast.ColumnNameExpr)
	if !ok || col.Name.Table.L != "" {
		return
	}
	if _, _, notFind := r.procedureCtx.GetProcedureVariable(col.Name.Name.L); notFind {
		return
	}
	field.AsName = col.Name.Name
}

func (r *procedurePlanCacheParamRewriter) Leave(in ast.Node) (ast.Node, bool) {
	switch in.(type) {
	case *ast.GroupByClause, *ast.HavingClause, *ast.OrderByClause:
		if r.aliasDepth > 0 {
			r.aliasDepth--
		}
	case *ast.Limit:
		if r.limitDepth > 0 {
			r.limitDepth--
		}
	}
	return in, true
}

func (r *procedurePlanCacheParamRewriter) replaceProcedureVar(node *ast.ProcedureVar) (ast.Node, bool) {
	if r.limitDepth > 0 {
		r.unsupported = true
		return node, true
	}
	fieldType, _, notFind := r.procedureCtx.GetProcedureVariable(node.Name.L)
	if notFind {
		r.err = plannererrors.ErrSpUndeclaredVar.GenWithStackByArgs(node.Name.O)
		return node, true
	}
	if procedurePlanCacheTypeMustFallback(fieldType) {
		r.unsupported = true
		return node, true
	}
	return r.newParamMarker(node.Name, fieldType), true
}

func (r *procedurePlanCacheParamRewriter) replaceColumnNameExpr(node *ast.ColumnNameExpr) (ast.Node, bool) {
	if r.inTrigger && node.Name.IsTriggerPseudoRecord() {
		// Trigger pseudo records depend on the current row context. Reusing a
		// hidden prepared statement would freeze NEW/OLD from the first
		// execution, so these statements must stay on the normal routine path.
		r.unsupported = true
		return node, true
	}
	if node.Name.Table.L != "" {
		return node, false
	}
	fieldType, _, notFind := r.procedureCtx.GetProcedureVariable(node.Name.Name.L)
	if notFind {
		return node, false
	}
	// LIMIT procedure vars keep stored-routine semantics such as signed-to-
	// unsigned conversion and same-SQL mutability checks, so the hidden
	// prepared path must fall back instead of rewriting them as parameters.
	if r.limitDepth > 0 {
		r.unsupported = true
		return node, true
	}
	// GROUP BY, HAVING and ORDER BY can resolve select-list aliases before
	// stored routine variables. The rewriter is intentionally conservative
	// because it runs before planner name resolution can disambiguate those
	// meanings.
	if r.aliasDepth > 0 {
		r.unsupported = true
		return node, true
	}
	// Hidden EXECUTE currently mis-handles ENUM/SET routine variables in result
	// projections, so keep those statements on the plain routine execution path.
	if procedurePlanCacheTypeMustFallback(fieldType) {
		r.unsupported = true
		return node, true
	}
	return r.newParamMarker(node.Name.Name, fieldType), true
}

func procedurePlanCacheTypeMustFallback(fieldType *types.FieldType) bool {
	if fieldType == nil {
		return false
	}
	switch fieldType.GetType() {
	case mysql.TypeEnum, mysql.TypeSet:
		return true
	default:
		return false
	}
}

func (r *procedurePlanCacheParamRewriter) newParamMarker(name model.CIStr, fieldType *types.FieldType) ast.ExprNode {
	param := ast.NewParamMarkerExpr(len(r.params)).(*driver.ParamMarkerExpr)
	param.Offset = len(r.params)
	r.params = append(r.params, procedurePlanCacheParam{
		name:        name.L,
		displayName: name.O,
		fieldType:   fieldType.Clone(),
	})
	return param
}
