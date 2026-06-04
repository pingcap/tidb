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
	"sort"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

var explainRoutineMetadataFieldNames = []string{
	"ROUTINE_SCHEMA",
	"ROUTINE_NAME",
	"ROUTINE_TYPE",
	"STMT_ORDINAL",
	"BLOCK_PATH",
	"STMT_KIND",
	"SQL_TEXT",
	"EXPLAINABLE",
	"NOTE",
}

var explainRoutineRuntimeFieldNames = []string{
	"RUNTIME_SQL_TEXT",
	"EXEC_COUNT",
	"TOTAL_TIME",
	"AVG_TIME",
	"ROWS_PRODUCED",
	"PLAN_VARIANTS",
}

type explainRoutineSiteContextKey struct{}

type explainRoutineDrainRowsContextKey struct{}

// ExplainRoutineRuntimeSite identifies one observable execution site during
// EXPLAIN ANALYZE ROUTINE execution.
type ExplainRoutineRuntimeSite struct {
	StmtOrdinal int
}

// ExplainRoutineRuntimeStats stores runtime observations for one statement site.
type ExplainRoutineRuntimeStats struct {
	RuntimeSQLText string
	ExecCount      int
	TotalTime      time.Duration
	RowsProduced   int64
	PlanVariants   int
	Explainable    bool
}

// ExplainRoutineCatalogEntry describes one observable SQL statement inside a stored routine.
type ExplainRoutineCatalogEntry struct {
	StmtOrdinal int
	BlockPath   string
	StmtKind    string
	SQLText     string
	ExplainSQL  string
	Stmt        ast.StmtNode
	Context     *variable.ProcedureContext
	Explainable bool
	Note        string
}

// ExplainRoutine represents an explain-routine plan.
type ExplainRoutine struct {
	baseSchemaProducer

	Format               string
	Analyze              bool
	HasTargetStmtOrdinal bool
	TargetStmtOrdinal    uint64
	Call                 *CallStmt
	Catalog              []ExplainRoutineCatalogEntry
}

func (e *ExplainRoutine) prepareSchema() error {
	fieldNames, err := explainRoutineFieldNames(e.Format, e.Analyze, e.HasTargetStmtOrdinal)
	if err != nil {
		return err
	}
	cwn := &columnsWithNames{
		cols:  make([]*expression.Column, 0, len(fieldNames)),
		names: make([]*types.FieldName, 0, len(fieldNames)),
	}
	for _, fieldName := range fieldNames {
		cwn.Append(buildColumnWithName("", fieldName, mysql.TypeString, mysql.MaxBlobWidth))
	}
	e.SetSchema(cwn.col2Schema())
	e.SetOutputNames(cwn.names)
	return nil
}

func explainRoutineFieldNames(format string, analyze, hasTargetStmtOrdinal bool) ([]string, error) {
	format = strings.ToLower(format)
	switch {
	case analyze && hasTargetStmtOrdinal:
		return explainRoutineDrilldownFieldNames(format)
	case analyze:
		fieldNames := append([]string{}, explainRoutineMetadataFieldNames...)
		fieldNames = append(fieldNames, explainRoutineRuntimeFieldNames...)
		return fieldNames, nil
	default:
		fieldNames := append([]string{}, explainRoutineMetadataFieldNames...)
		switch format {
		case types.ExplainFormatROW, types.ExplainFormatBrief:
			fieldNames = append(fieldNames, "id", "estRows", "task", "access object", "operator info")
		case types.ExplainFormatVerbose:
			fieldNames = append(fieldNames, "id", "estRows", "estCost", "task", "access object", "operator info")
		case types.ExplainFormatTiDBJSON:
			fieldNames = append(fieldNames, "PLAN_JSON")
		default:
			return nil, errors.Errorf("explain routine format '%s' is not supported now", format)
		}
		return fieldNames, nil
	}
}

func explainRoutineDrilldownFieldNames(format string) ([]string, error) {
	switch format {
	case types.ExplainFormatROW, types.ExplainFormatBrief:
		return []string{"id", "estRows", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk"}, nil
	case types.ExplainFormatVerbose:
		return []string{"id", "estRows", "estCost", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk"}, nil
	case types.ExplainFormatTiDBJSON:
		return []string{"TiDB_JSON"}, nil
	default:
		return nil, errors.Errorf("explain analyze routine drill-down format '%s' is not supported now", format)
	}
}

// BuildExplainRoutineCatalog extracts observable static SQL statements from a stored routine plan.
func BuildExplainRoutineCatalog(procPlan *ProcedurePlan) []ExplainRoutineCatalogEntry {
	if procPlan == nil {
		return nil
	}
	catalog := make([]ExplainRoutineCatalogEntry, 0)
	for _, command := range procPlan.ProcedureExecPlan.ProcedureCommandList {
		switch x := command.(type) {
		case *executeBaseSQL:
			if x.cacheStmt == nil {
				continue
			}
			stmts := x.cacheStmt.GetStmts()
			if len(stmts) != 1 {
				continue
			}
			entry, ok := buildExplainRoutineCatalogEntry(
				explainRoutineCatalogStmtOrdinal(x.stmtOrdinal, len(catalog)+1),
				x.blockPath,
				strings.TrimSpace(x.cacheStmt.GetString()),
				stmts[0],
				x.context,
			)
			if !ok {
				continue
			}
			catalog = append(catalog, entry)
		case *OpenProcedurceCursor:
			if x.context == nil {
				continue
			}
			cursorRes, err := x.context.FindCurs(x.curName)
			if err != nil {
				continue
			}
			curInfo, ok := cursorRes.(*procedurceCurInfo)
			if !ok || curInfo == nil || curInfo.selectAst == nil {
				continue
			}
			entry, ok := buildExplainRoutineCatalogEntry(
				explainRoutineCatalogStmtOrdinal(x.stmtOrdinal, len(catalog)+1),
				x.blockPath,
				explainRoutineNodeText(curInfo.selectAst),
				curInfo.selectAst,
				curInfo.context,
			)
			if !ok {
				continue
			}
			catalog = append(catalog, entry)
		case *returnInst:
			if x.cacheStmt == nil {
				continue
			}
			stmts := x.cacheStmt.GetStmts()
			if len(stmts) != 1 {
				continue
			}
			retStmt, ok := stmts[0].(*ast.ProcedureReturnStmt)
			if !ok || !explainRoutineExprContainsPlanBearingPath(retStmt.ReturnExpr) {
				continue
			}
			catalog = append(catalog, ExplainRoutineCatalogEntry{
				StmtOrdinal: explainRoutineCatalogStmtOrdinal(x.stmtOrdinal, len(catalog)+1),
				BlockPath:   normalizeExplainRoutineBlockPath(x.blockPath),
				StmtKind:    "return",
				SQLText:     strings.TrimSpace(explainRoutineRestoredNodeText(retStmt)),
				ExplainSQL:  strings.TrimSpace(NewCacheExpr(true, explainRoutineRestoredNodeText(retStmt.ReturnExpr), nil).GetString()),
				Stmt:        retStmt,
				Context:     x.context,
				Explainable: true,
			})
		case *UpdateVariables:
			entry, ok := buildExplainRoutineExprCatalogEntry(
				explainRoutineCatalogStmtOrdinal(x.stmtOrdinal, len(catalog)+1),
				x.blockPath,
				"declare_default",
				x.exprNode,
				x.context,
			)
			if ok {
				catalog = append(catalog, entry)
			}
		case *ProcedureIfGo:
			entry, ok := buildExplainRoutineExprCatalogEntry(
				explainRoutineCatalogStmtOrdinal(x.stmtOrdinal, len(catalog)+1),
				x.blockPath,
				"condition",
				x.exprNode,
				x.context,
			)
			if ok {
				catalog = append(catalog, entry)
			}
		case *procedureSimpleCase:
			entry, ok := buildExplainRoutineExprCatalogEntry(
				explainRoutineCatalogStmtOrdinal(x.stmtOrdinal, len(catalog)+1),
				x.blockPath,
				"condition",
				x.conditionExprNode,
				x.context,
			)
			if ok {
				catalog = append(catalog, entry)
			}
			for _, dest := range x.dests {
				entry, ok := buildExplainRoutineExprCatalogEntry(
					explainRoutineCatalogStmtOrdinal(dest.stmtOrdinal, len(catalog)+1),
					dest.blockPath,
					"condition",
					dest.exprNode,
					x.context,
				)
				if ok {
					catalog = append(catalog, entry)
				}
			}
		case *procedureSearchCase:
			for _, dest := range x.dests {
				entry, ok := buildExplainRoutineExprCatalogEntry(
					explainRoutineCatalogStmtOrdinal(dest.stmtOrdinal, len(catalog)+1),
					dest.blockPath,
					"condition",
					dest.exprNode,
					x.context,
				)
				if ok {
					catalog = append(catalog, entry)
				}
			}
		}
	}
	sort.Slice(catalog, func(i, j int) bool {
		return catalog[i].StmtOrdinal < catalog[j].StmtOrdinal
	})
	return catalog
}

func explainRoutineCatalogStmtOrdinal(stmtOrdinal, fallback int) int {
	if stmtOrdinal > 0 {
		return stmtOrdinal
	}
	return fallback
}

func buildExplainRoutineExprCatalogEntry(stmtOrdinal int, blockPath, stmtKind string, expr ast.ExprNode, ctx *variable.ProcedureContext) (ExplainRoutineCatalogEntry, bool) {
	if expr == nil || !explainRoutineExprContainsPlanBearingPath(expr) {
		return ExplainRoutineCatalogEntry{}, false
	}
	exprText := strings.TrimSpace(explainRoutineRestoredNodeText(expr))
	return ExplainRoutineCatalogEntry{
		StmtOrdinal: stmtOrdinal,
		BlockPath:   normalizeExplainRoutineBlockPath(blockPath),
		StmtKind:    stmtKind,
		SQLText:     exprText,
		ExplainSQL:  strings.TrimSpace(NewCacheExpr(true, exprText, nil).GetString()),
		Context:     ctx,
		Explainable: true,
	}, true
}

func buildExplainRoutineCatalogEntry(stmtOrdinal int, blockPath, sqlText string, stmt ast.StmtNode, ctx *variable.ProcedureContext) (ExplainRoutineCatalogEntry, bool) {
	stmtKind, explainable, note, ok := explainRoutineStmtMeta(stmt)
	if !ok {
		return ExplainRoutineCatalogEntry{}, false
	}
	return ExplainRoutineCatalogEntry{
		StmtOrdinal: stmtOrdinal,
		BlockPath:   normalizeExplainRoutineBlockPath(blockPath),
		StmtKind:    stmtKind,
		SQLText:     strings.TrimSpace(sqlText),
		ExplainSQL:  strings.TrimSpace(sqlText),
		Stmt:        stmt,
		Context:     ctx,
		Explainable: explainable,
		Note:        note,
	}, true
}

func normalizeExplainRoutineBlockPath(blockPath string) string {
	blockPath = strings.TrimSpace(blockPath)
	if blockPath == "" {
		return "root"
	}
	return blockPath
}

func explainRoutineNodeText(node ast.Node) string {
	if node == nil {
		return ""
	}
	if text := strings.TrimSpace(node.Text()); text != "" {
		return text
	}
	var sb strings.Builder
	_ = node.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	return strings.TrimSpace(sb.String())
}

func explainRoutineRestoredNodeText(node ast.Node) string {
	if node == nil {
		return ""
	}
	var sb strings.Builder
	_ = node.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	return strings.TrimSpace(sb.String())
}

func explainRoutineStmtMeta(stmt ast.StmtNode) (stmtKind string, explainable bool, note string, ok bool) {
	switch x := stmt.(type) {
	case *ast.SelectStmt, *ast.SetOprStmt:
		return "select", true, "", true
	case *ast.InsertStmt:
		if x.IsReplace {
			return "replace", true, "", true
		}
		return "insert", true, "", true
	case *ast.UpdateStmt:
		return "update", true, "", true
	case *ast.DeleteStmt:
		return "delete", true, "", true
	case *ast.CallStmt:
		return "call", true, "", true
	case *ast.SetStmt:
		if ExplainRoutineSetStmtHasPlanBearingPath(x) {
			return "set", true, "", true
		}
		return "", false, "", false
	case *ast.PrepareStmt, *ast.ExecuteStmt:
		return "dynamic_sql", false, "dynamic SQL is not explainable in v1", true
	default:
		return "", false, "", false
	}
}

// ExplainRoutineSetStmtHasPlanBearingPath reports whether a SET statement contains
// an assignment expression that needs a query plan, such as a scalar subquery.
func ExplainRoutineSetStmtHasPlanBearingPath(stmt *ast.SetStmt) bool {
	if stmt == nil {
		return false
	}
	for _, variable := range stmt.Variables {
		if variable == nil || variable.Value == nil {
			continue
		}
		if explainRoutineExprContainsPlanBearingPath(variable.Value) {
			return true
		}
	}
	return false
}

func explainRoutineExprContainsPlanBearingPath(expr ast.ExprNode) bool {
	if expr == nil {
		return false
	}
	visitor := &explainRoutinePlanBearingExprVisitor{}
	expr.Accept(visitor)
	return visitor.found
}

type explainRoutinePlanBearingExprVisitor struct {
	found bool
}

func (v *explainRoutinePlanBearingExprVisitor) Enter(node ast.Node) (ast.Node, bool) {
	if v.found {
		return node, true
	}
	switch node.(type) {
	case *ast.SubqueryExpr, *ast.ExistsSubqueryExpr, *ast.CompareSubqueryExpr:
		v.found = true
		return node, true
	}
	return node, false
}

func (v *explainRoutinePlanBearingExprVisitor) Leave(node ast.Node) (ast.Node, bool) {
	return node, !v.found
}

func newExplainRoutineStmtFromAST(stmt *ast.ExplainRoutineStmt) *ast.CallStmt {
	return &ast.CallStmt{
		IsFunction: strings.EqualFold(stmt.RoutineType, ast.ExplainRoutineTypeFunction),
		Procedure: &ast.FuncCallExpr{
			Tp:     ast.FuncCallExprTypeGeneric,
			Schema: stmt.Name.Schema,
			FnName: stmt.Name.Name,
			Args:   stmt.Args,
		},
	}
}

func normalizeExplainRoutineFormat(format string) string {
	format = strings.ToLower(format)
	if format == "" || format == types.ExplainFormatTraditional {
		return types.ExplainFormatROW
	}
	return format
}

func validateExplainRoutineFormat(format string, analyze, hasTargetStmtOrdinal bool) error {
	if analyze {
		if hasTargetStmtOrdinal {
			switch format {
			case types.ExplainFormatROW, types.ExplainFormatTraditional, types.ExplainFormatBrief, types.ExplainFormatVerbose, types.ExplainFormatTiDBJSON:
				return nil
			default:
				return errors.Errorf("explain analyze routine drill-down format '%s' is not supported now", format)
			}
		}
		if format != types.ExplainFormatROW && format != types.ExplainFormatTraditional {
			return errors.Errorf("explain analyze routine summary only supports format 'row'")
		}
		return nil
	}
	switch format {
	case types.ExplainFormatROW, types.ExplainFormatBrief, types.ExplainFormatVerbose, types.ExplainFormatTiDBJSON:
		return nil
	default:
		return errors.Errorf("explain routine format '%s' is not supported now", format)
	}
}

// WithExplainRoutineRuntimeSite annotates the execution context with the current
// observable routine statement site.
func WithExplainRoutineRuntimeSite(ctx context.Context, site ExplainRoutineRuntimeSite) context.Context {
	if site.StmtOrdinal <= 0 {
		return ctx
	}
	return context.WithValue(ctx, explainRoutineSiteContextKey{}, site)
}

// ExplainRoutineRuntimeSiteFromContext returns the observable routine statement
// site stored in the context, if any.
func ExplainRoutineRuntimeSiteFromContext(ctx context.Context) (ExplainRoutineRuntimeSite, bool) {
	site, ok := ctx.Value(explainRoutineSiteContextKey{}).(ExplainRoutineRuntimeSite)
	return site, ok
}

// WithExplainRoutineDrainRows marks a routine statement execution whose caller
// only needs the statement to be fully drained, not materialized rows.
func WithExplainRoutineDrainRows(ctx context.Context) context.Context {
	return context.WithValue(ctx, explainRoutineDrainRowsContextKey{}, true)
}

// ExplainRoutineDrainRowsFromContext returns whether the current routine
// statement execution should avoid materializing result rows.
func ExplainRoutineDrainRowsFromContext(ctx context.Context) bool {
	drainRows, _ := ctx.Value(explainRoutineDrainRowsContextKey{}).(bool)
	return drainRows
}

func (b *PlanBuilder) fillExplainRoutineShapeOnlyArgs(ctx context.Context, callStmt *ast.CallStmt) error {
	routineSchema := callStmt.Procedure.Schema.O
	if routineSchema == "" {
		routineSchema = b.ctx.GetSessionVars().CurrentDB
		callStmt.Procedure.Schema = pmodel.NewCIStr(routineSchema)
	}
	if routineSchema == "" {
		return plannererrors.ErrNoDB
	}

	routineType := "PROCEDURE"
	if callStmt.IsFunction {
		routineType = "FUNCTION"
	}
	routineInfo, err := b.fetchProcdureInfo(callStmt.Procedure.FnName.O, routineSchema, routineType)
	if err != nil {
		return err
	}

	parser, ok := b.ctx.(sqlexec.SQLParser)
	if !ok {
		return errors.New("session context does not implement SQLParser")
	}
	stmtNodes, _, err := parser.ParseSQL(ctx, routineInfo.Procedurebody)
	if err != nil {
		return err
	}
	if len(stmtNodes) != 1 {
		return errors.New("parse procedure error")
	}

	createInfo, ok := stmtNodes[0].(*ast.CreateProcedureInfo)
	if !ok {
		return errors.Errorf("unexpected routine definition type %T", stmtNodes[0])
	}

	callStmt.Procedure.Args = make([]ast.ExprNode, 0, len(createInfo.ProcedureParam))
	for i, param := range createInfo.ProcedureParam {
		switch param.Paramstatus {
		case ast.ModeOut, ast.ModeInOut:
			callStmt.Procedure.Args = append(callStmt.Procedure.Args, &ast.VariableExpr{
				Name: fmt.Sprintf("__explain_routine_arg_%d", i+1),
			})
		default:
			callStmt.Procedure.Args = append(callStmt.Procedure.Args, ast.NewValueExpr(nil, "", ""))
		}
	}
	return nil
}
