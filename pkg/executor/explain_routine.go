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

package executor

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// ExplainRoutineExec renders EXPLAIN ROUTINE results for static SQL statements collected from
// a stored routine's command list.
type ExplainRoutineExec struct {
	exec.BaseExecutor

	explain  *plannercore.ExplainRoutine
	call     *plannercore.CallStmt
	catalog  []plannercore.ExplainRoutineCatalogEntry
	procExec *ProcedureExec

	rows   [][]*string
	cursor int
}

// Open implements the Executor Open interface.
func (*ExplainRoutineExec) Open(context.Context) error {
	return nil
}

// Close implements the Executor Close interface.
func (e *ExplainRoutineExec) Close() error {
	e.rows = nil
	e.cursor = 0
	return nil
}

// Next implements the Executor Next interface.
func (e *ExplainRoutineExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.rows == nil {
		rows, err := e.generateExplainRows(ctx)
		if err != nil {
			return err
		}
		e.rows = rows
	}

	req.GrowAndReset(e.MaxChunkSize())
	if e.cursor >= len(e.rows) {
		return nil
	}

	numCurRows := min(req.Capacity(), len(e.rows)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurRows; i++ {
		for j, value := range e.rows[i] {
			if value == nil {
				req.AppendNull(j)
				continue
			}
			req.AppendString(j, *value)
		}
	}
	e.cursor += numCurRows
	return nil
}

func (e *ExplainRoutineExec) generateExplainRows(ctx context.Context) (rows [][]*string, err error) {
	if err := e.validateAnalyzeExecution(); err != nil {
		return nil, err
	}
	restore, err := e.enterRoutineExplainContext(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		restore(err)
	}()

	if e.explain.Analyze {
		analyzer := newRoutineExplainAnalyzer(e.catalog, int(e.explain.TargetStmtOrdinal), e.explain.Format)
		if err := executeRoutineAnalyze(ctx, e.procExec, analyzer); err != nil {
			return nil, err
		}
		if e.explain.HasTargetStmtOrdinal {
			return e.generateAnalyzeDrilldownRows(analyzer)
		}
		return e.generateAnalyzeSummaryRows(analyzer), nil
	}

	rows = make([][]*string, 0, len(e.catalog))
	for _, entry := range e.catalog {
		stmtRows, stmtErr := e.renderExplainRowsForEntry(ctx, entry)
		if stmtErr != nil {
			stmtRows = [][]*string{e.buildExplainRoutineMessageRow(entry, false, "compile failed: "+stmtErr.Error())}
		}
		rows = append(rows, stmtRows...)
	}
	return rows, nil
}

func (e *ExplainRoutineExec) validateAnalyzeExecution() error {
	if !e.explain.Analyze {
		return nil
	}
	if e.explain.HasTargetStmtOrdinal {
		targetOrdinal := int(e.explain.TargetStmtOrdinal)
		if targetOrdinal <= 0 {
			return errors.New("invalid explain analyze routine statement ordinal")
		}
		targetFound := false
		for _, entry := range e.catalog {
			if entry.StmtOrdinal == targetOrdinal {
				targetFound = true
				break
			}
		}
		if !targetFound {
			return errors.Errorf("explain analyze routine STMT_ORDINAL %d not found", targetOrdinal)
		}
	}
	return nil
}

func (e *ExplainRoutineExec) generateAnalyzeSummaryRows(analyzer *routineExplainAnalyzer) [][]*string {
	rows := make([][]*string, 0, len(e.catalog))
	for _, entry := range e.catalog {
		stats, ok := analyzer.runtimeStats(entry.StmtOrdinal)
		rows = append(rows, e.buildExplainRoutineAnalyzeSummaryRow(entry, stats, ok))
	}
	return rows
}

func (e *ExplainRoutineExec) generateAnalyzeDrilldownRows(analyzer *routineExplainAnalyzer) ([][]*string, error) {
	targetOrdinal := int(e.explain.TargetStmtOrdinal)
	if targetOrdinal <= 0 {
		return nil, errors.New("invalid explain analyze routine statement ordinal")
	}
	if _, ok := analyzer.catalogByOrdinal[targetOrdinal]; !ok {
		return nil, errors.Errorf("explain analyze routine STMT_ORDINAL %d not found", targetOrdinal)
	}
	drilldownRows, err := analyzer.drilldownRowsForTarget()
	if err != nil {
		return nil, err
	}
	rows := make([][]*string, 0, len(drilldownRows))
	for _, explainRow := range drilldownRows {
		row := make([]*string, 0, len(explainRow))
		for _, value := range explainRow {
			row = append(row, strPtr(value))
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (e *ExplainRoutineExec) renderExplainRowsForEntry(ctx context.Context, entry plannercore.ExplainRoutineCatalogEntry) (rows [][]*string, err error) {
	if !entry.Explainable {
		return [][]*string{e.buildExplainRoutineMessageRow(entry, false, entry.Note)}, nil
	}

	restoreEntryContext, err := e.enterCatalogEntryContext(entry)
	if err != nil {
		return nil, err
	}
	defer func() {
		if restoreErr := restoreEntryContext(); err == nil && restoreErr != nil {
			err = restoreErr
		}
	}()

	stmt, err := e.parseExplainRoutineStmt(ctx, entry)
	if err != nil {
		return nil, err
	}
	defer resetStmtCtx(e.Ctx(), stmt)()

	if sel, ok := stmt.(*ast.SelectStmt); ok && sel.SelectIntoOpt != nil {
		return e.renderExplainRowsFromOptimizedPlan(ctx, entry, stmt)
	}

	explainStmt := &ast.ExplainStmt{
		Stmt:    stmt,
		Format:  e.explain.Format,
		Analyze: false,
	}
	exec, ok := e.Ctx().(sqlexec.RestrictedSQLExecutor)
	if !ok {
		return nil, errors.New("session context does not implement RestrictedSQLExecutor")
	}

	resultRows, resultFields, err := exec.ExecRestrictedStmt(ctx, explainStmt, sqlexec.ExecOptionUseCurSession)
	if err != nil {
		return nil, err
	}
	if len(resultRows) == 0 {
		return [][]*string{e.buildExplainRoutineMessageRow(entry, false, "statement plan is not explainable in v1")}, nil
	}

	rows = make([][]*string, 0, len(resultRows))
	for _, explainRow := range resultRows {
		row := e.buildExplainRoutinePrefixRow(entry, true, "")
		for i := range resultFields {
			if explainRow.IsNull(i) {
				row = append(row, nil)
				continue
			}
			row = append(row, strPtr(explainRow.GetString(i)))
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (e *ExplainRoutineExec) enterCatalogEntryContext(entry plannercore.ExplainRoutineCatalogEntry) (func() error, error) {
	if entry.Context == nil {
		return func() error { return nil }, nil
	}
	vars := e.Ctx().GetSessionVars()
	procCtx := vars.GetProcedureContext()
	if procCtx == nil {
		return nil, errors.New("missing procedure context for explain routine entry")
	}
	previousContext := procCtx.Context
	if previousContext == entry.Context {
		return func() error { return nil }, nil
	}
	if err := vars.SetProcedureContext(entry.Context); err != nil {
		return nil, err
	}
	return func() error {
		return vars.SetProcedureContext(previousContext)
	}, nil
}

func (e *ExplainRoutineExec) renderExplainRowsFromOptimizedPlan(ctx context.Context, entry plannercore.ExplainRoutineCatalogEntry, stmt ast.StmtNode) ([][]*string, error) {
	nodeW := resolve.NewNodeW(stmt)
	targetPlan, _, err := plannercore.OptimizeAstNode(ctx, e.Ctx(), nodeW, e.call.Is)
	if err != nil {
		return nil, err
	}
	if selectIntoPlan, ok := targetPlan.(*plannercore.SelectInto); ok && selectIntoPlan.TargetPlan != nil {
		targetPlan = selectIntoPlan.TargetPlan
	}

	explainPlan := &plannercore.Explain{
		TargetPlan: targetPlan,
		Format:     e.explain.Format,
		ExecStmt:   stmt,
	}
	explainPlan.SetSCtx(targetPlan.SCtx())
	if err := explainPlan.RenderResult(); err != nil {
		return nil, err
	}
	if len(explainPlan.Rows) == 0 {
		return [][]*string{e.buildExplainRoutineMessageRow(entry, false, "statement plan is not explainable in v1")}, nil
	}

	rows := make([][]*string, 0, len(explainPlan.Rows))
	for _, explainRow := range explainPlan.Rows {
		row := e.buildExplainRoutinePrefixRow(entry, true, "")
		for _, value := range explainRow {
			row = append(row, strPtr(value))
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (e *ExplainRoutineExec) parseExplainRoutineStmt(ctx context.Context, entry plannercore.ExplainRoutineCatalogEntry) (ast.StmtNode, error) {
	parser, ok := e.Ctx().(sqlexec.SQLParser)
	if !ok {
		return nil, errors.New("session context does not implement SQLParser")
	}

	sqlText := entry.SQLText
	if entry.ExplainSQL != "" {
		sqlText = entry.ExplainSQL
	}
	stmts, _, err := parser.ParseSQL(ctx, sqlText)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, errors.Errorf("unexpected statement count %d for explain routine entry", len(stmts))
	}
	return stmts[0], nil
}

func (e *ExplainRoutineExec) buildExplainRoutinePrefixRow(entry plannercore.ExplainRoutineCatalogEntry, explainable bool, note string) []*string {
	explainableText := "NO"
	if explainable {
		explainableText = "YES"
	}
	row := make([]*string, 0, 9+explainRoutineSuffixFieldCount(e.explain.Format))
	row = append(row,
		strPtr(e.routineSchema()),
		strPtr(e.routineName()),
		strPtr(e.routineType()),
		strPtr(strconv.Itoa(entry.StmtOrdinal)),
		strPtr(entry.BlockPath),
		strPtr(entry.StmtKind),
		strPtr(entry.SQLText),
		strPtr(explainableText),
		strPtr(note),
	)
	return row
}

func (e *ExplainRoutineExec) buildExplainRoutineMessageRow(entry plannercore.ExplainRoutineCatalogEntry, explainable bool, note string) []*string {
	row := e.buildExplainRoutinePrefixRow(entry, explainable, note)
	switch e.explain.Format {
	case types.ExplainFormatTiDBJSON:
		row = append(row, nil)
	default:
		for i := 0; i < explainRoutineSuffixFieldCount(e.explain.Format); i++ {
			row = append(row, strPtr(""))
		}
	}
	return row
}

func (e *ExplainRoutineExec) buildExplainRoutineAnalyzeSummaryRow(
	entry plannercore.ExplainRoutineCatalogEntry,
	stats plannercore.ExplainRoutineRuntimeStats,
	executed bool,
) []*string {
	explainable := entry.Explainable
	note := entry.Note
	if executed {
		explainable = stats.Explainable
		if stats.Explainable {
			note = ""
		}
	}
	row := e.buildExplainRoutinePrefixRow(entry, explainable, note)

	runtimeSQLText := ""
	if executed && stats.RuntimeSQLText != "" && !strings.EqualFold(stats.RuntimeSQLText, entry.SQLText) {
		runtimeSQLText = stats.RuntimeSQLText
	}
	execCount := "0"
	totalTime := ""
	avgTime := ""
	rowsProduced := "0"
	planVariants := "0"
	if executed {
		execCount = strconv.Itoa(stats.ExecCount)
		if stats.TotalTime > 0 {
			totalTime = stats.TotalTime.String()
			avgTime = (stats.TotalTime / time.Duration(stats.ExecCount)).String()
		}
		rowsProduced = strconv.FormatInt(stats.RowsProduced, 10)
		planVariants = strconv.Itoa(stats.PlanVariants)
	}
	row = append(row,
		strPtr(runtimeSQLText),
		strPtr(execCount),
		strPtr(totalTime),
		strPtr(avgTime),
		strPtr(rowsProduced),
		strPtr(planVariants),
	)
	return row
}

func (e *ExplainRoutineExec) routineSchema() string {
	if e.call.Callstmt.Procedure.Schema.O != "" {
		return e.call.Callstmt.Procedure.Schema.O
	}
	return e.Ctx().GetSessionVars().CurrentDB
}

func (e *ExplainRoutineExec) routineName() string {
	return e.call.Callstmt.Procedure.FnName.O
}

func (e *ExplainRoutineExec) routineType() string {
	if e.call.Callstmt.IsFunction {
		return "FUNCTION"
	}
	return "PROCEDURE"
}

func explainRoutineSuffixFieldCount(format string) int {
	switch format {
	case types.ExplainFormatROW, types.ExplainFormatBrief:
		return 5
	case types.ExplainFormatVerbose:
		return 6
	case types.ExplainFormatTiDBJSON:
		return 1
	default:
		return 0
	}
}

func strPtr(value string) *string {
	return &value
}

func (e *ExplainRoutineExec) enterRoutineExplainContext(ctx context.Context) (func(error), error) {
	vars := e.Ctx().GetSessionVars()
	vars.SetInCallProcedure()

	oldUser := vars.User
	oldRole := vars.ActiveRoles
	oldDB := vars.CurrentDB
	oldDBCI := vars.CurrentDBCI
	oldSQLMode := vars.SQLMode
	oldSQLModeVar, ok := vars.GetSystemVar(variable.SQLModeVar)
	if !ok {
		vars.OutCallProcedure(true)
		return nil, errors.New("can not find sql_mode")
	}

	pm := privilege.GetPrivilegeManager(e.Ctx())
	needChangeUser := e.procExec.securityType == "DEFINER" &&
		(oldUser == nil || e.procExec.definerUser != oldUser.AuthUsername || e.procExec.definerHost != oldUser.AuthHostname)
	if needChangeUser {
		if pm == nil {
			vars.OutCallProcedure(true)
			return nil, errors.New("missing privilege manager")
		}
		if e.procExec.definerUser != "" {
			definer := &auth.UserIdentity{Username: e.procExec.definerUser, Hostname: e.procExec.definerHost}
			var success bool
			definer.AuthUsername, definer.AuthHostname, success = pm.MatchIdentity(definer.Username, definer.Hostname, false)
			if !success || !pm.GetAuthWithoutVerification(e.procExec.definerUser, e.procExec.definerHost) {
				vars.OutCallProcedure(true)
				return nil, exeerrors.ErrNoSuchUser.GenWithStackByArgs(e.procExec.definerUser, e.procExec.definerHost)
			}
			vars.User = definer
			vars.ActiveRoles = pm.GetDefaultRoles(e.procExec.definerUser, e.procExec.definerHost)
		} else {
			vars.User = nil
			vars.ActiveRoles = nil
		}
	}

	if currentDB := e.call.Callstmt.Procedure.Schema.L; currentDB != "" {
		vars.CurrentDB = currentDB
		vars.CurrentDBCI = e.call.Callstmt.Procedure.Schema
	}

	if err := e.procExec.callParam(ctx, e.call.Callstmt); err != nil {
		if needChangeUser {
			vars.ActiveRoles = oldRole
			vars.User = oldUser
			if oldUser != nil {
				pm.AuthSuccess(oldUser.AuthUsername, oldUser.AuthHostname)
			} else {
				pm.AuthSuccess("", "")
			}
		}
		vars.CurrentDB = oldDB
		vars.CurrentDBCI = oldDBCI
		vars.OutCallProcedure(true)
		return nil, err
	}

	sqlMode, err := mysql.GetSQLMode(e.procExec.ProcedureSQLMod)
	if err != nil {
		if needChangeUser {
			vars.ActiveRoles = oldRole
			vars.User = oldUser
			if oldUser != nil {
				pm.AuthSuccess(oldUser.AuthUsername, oldUser.AuthHostname)
			} else {
				pm.AuthSuccess("", "")
			}
		}
		vars.CurrentDB = oldDB
		vars.CurrentDBCI = oldDBCI
		vars.SetProcedureContext(e.procExec.parentContext)
		vars.OutCallProcedure(true)
		return nil, err
	}
	if err := vars.SetSystemVar(variable.SQLModeVar, e.procExec.ProcedureSQLMod); err != nil {
		if needChangeUser {
			vars.ActiveRoles = oldRole
			vars.User = oldUser
			if oldUser != nil {
				pm.AuthSuccess(oldUser.AuthUsername, oldUser.AuthHostname)
			} else {
				pm.AuthSuccess("", "")
			}
		}
		vars.CurrentDB = oldDB
		vars.CurrentDBCI = oldDBCI
		vars.SetProcedureContext(e.procExec.parentContext)
		vars.OutCallProcedure(true)
		return nil, err
	}
	vars.SQLMode = sqlMode
	plannercore.ResetCallStatus(e.Ctx())

	return func(execErr error) {
		vars.SetProcedureContext(e.procExec.parentContext)
		_ = vars.SetSystemVar(variable.SQLModeVar, oldSQLModeVar)
		vars.SQLMode = oldSQLMode
		vars.CurrentDB = oldDB
		vars.CurrentDBCI = oldDBCI
		if needChangeUser {
			vars.ActiveRoles = oldRole
			vars.User = oldUser
			if pm != nil {
				if oldUser != nil {
					pm.AuthSuccess(oldUser.AuthUsername, oldUser.AuthHostname)
				} else {
					pm.AuthSuccess("", "")
				}
			}
		}
		vars.OutCallProcedure(execErr != nil)
	}, nil
}
