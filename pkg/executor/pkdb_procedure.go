// Copyright 2022-2023 PingCAP, Inc.

package executor

import (
	"context"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/schematracker"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

var (
	warnStr = "WARNING"
	errStr  = "ERROR"
)

// const loop type.
const (
	LoopWhile int = iota
	LoopFor
	LoopRepeat
	LoopDoWhile
	LoopEnd
)

const (
	blockExit       int = 1
	hasWarning      int = 1 << 1
	iterateContinue int = 1 << 2
	leaveExit       int = 1 << 3
)

// ProcedureExec create \call \drop procedure exec plan.
type ProcedureExec struct {
	exec.BaseExecutor
	done            bool
	IsStrict        bool                  // store SQL strict flag
	flag            int                   // store sql warnings status
	is              infoschema.InfoSchema // table definition
	Statement       ast.StmtNode          // sql ast tree
	ProcedureSQLMod string                // SQLMod that procedure created
	// Save the mapping relationship between stored procedure internal variables and user variables
	outVarParam         map[string]string         // save variable name. Wait for the stored procedure to finish executing and set the output value
	outLocalParam       map[string]string         // save local variable name.Wait for the stored procedure to finish executing and set the output value
	outTriggerParam     map[string]int            // save trigger pseudo record variable name. Wait for the stored procedure to finish executing and set the output value
	procedurePlan       plannercore.ProcedureExec // store execution plan
	cachedProcedurePlan *plannercore.RoutineCacahe
	definerUser         string
	definerHost         string
	securityType        string
	cache               []plannercore.NeedCloseCur
	parentContext       *variable.ProcedureContext

	buildPlan  func(base.Plan) exec.Executor
	isFunction bool
}

// Close ProcedureExec.
func (e *ProcedureExec) Close() error {
	e.outVarParam = nil
	e.outLocalParam = nil
	e.outTriggerParam = nil
	err := e.BaseExecutor.Close()
	if err != nil {
		return err
	}
	return nil
}

// Next implement stored procedures
func (e *ProcedureExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if e.done {
		return nil
	}
	switch x := e.Statement.(type) {
	case *ast.CallStmt:
		_, err = e.callProcedure(ctx, x)
	default:
		// This case occurs when the trigger body is combined with the procedure body.
		err = e.triggerCallProcedure(ctx)
	}

	e.done = true
	return err
}

func (e *ProcedureExec) triggerCallProcedure(ctx context.Context) (err error) {
	e.Ctx().GetSessionVars().SetInCallProcedure()
	defer func() {
		if e.isFunction && e.parentContext != nil {
			restoreErr := e.Ctx().GetSessionVars().SetProcedureContext(e.parentContext)
			if err == nil && restoreErr != nil {
				err = restoreErr
			}
		}
		e.Ctx().GetSessionVars().OutCallProcedure(err != nil)
	}()
	mockCallStmt := &ast.CallStmt{
		Procedure: &ast.FuncCallExpr{
			Schema: pmodel.NewCIStr(e.Ctx().GetSessionVars().CurrentDB),
			FnName: pmodel.NewCIStr(""),
			Args:   []ast.ExprNode{},
		},
	}
	_, err = e.callProcedure(ctx, mockCallStmt)
	return err
}

// procedureExistsInternal Query whether the stored procedure exists.
func procedureExistsInternal(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, name, db, tp string) (string, bool, error) {
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, `SELECT Definer FROM %n.%n WHERE route_schema=%? AND name=%? AND type=%? FOR UPDATE;`, mysql.SystemDB, mysql.Routines, db, name, tp)
	recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	if err != nil {
		return "", false, err
	}
	var rows []chunk.Row
	defer terror.Call(recordSet.Close)
	if rows, err = sqlexec.DrainRecordSet(ctx, recordSet, 8); err != nil {
		return "", false, err
	}
	if len(rows) > 0 {
		definer := rows[0].GetString(0)
		return definer, true, err
	}
	return "", false, err
}

// getProcedureInfo read stored procedure content.
func getProcedureInfo(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, name string, db string, tp ast.ShowStmtType) (*plannercore.ProcedurebodyInfo, error) {
	tpStr := "PROCEDURE"
	if tp == ast.ShowCreateFunction {
		tpStr = "FUNCTION"
	}
	sql := new(strings.Builder)
	//heads []string{"Procedure", "sql_mode", "Create Procedure", "character_set_client", "collation_connection", "Database Collation"}
	sqlescape.MustFormatSQL(sql, "select name, sql_mode, definition_utf8, parameter_str, character_set_client, connection_collation,")
	sqlescape.MustFormatSQL(sql, "schema_collation, comment, security_type, definer from %n.%n where route_schema = %? and name = %? and type = %? ", mysql.SystemDB, mysql.Routines, db, name, tpStr)

	recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	if err != nil {
		return nil, err
	}
	if recordSet != nil {
		defer recordSet.Close()
	}

	rows, err := sqlexec.DrainRecordSet(ctx, recordSet, 3)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, exeerrors.ErrSpDoesNotExist.GenWithStackByArgs(tpStr, db+"."+name)
	}
	if len(rows) != 1 {
		return nil, errors.New("Multiple stored procedures found in table " + mysql.Routines)
	}
	procedureBodyInfo := &plannercore.ProcedurebodyInfo{}
	procedureBodyInfo.Name = rows[0].GetString(0)
	procedureBodyInfo.Procedurebody = " CREATE "
	if len(rows[0].GetString(9)) != 0 {
		strs := strings.Split(rows[0].GetString(9), "@")
		if len(strs) != 2 {
			return nil, errors.Errorf("get definer `%s` failed, the definer must be the form of `user`@`host`", rows[0].GetString(9))
		}
		procedureBodyInfo.Procedurebody = procedureBodyInfo.Procedurebody + "DEFINER=" + "`" + strs[0] + "`@`" + strs[1] + "` "
	}
	procedureBodyInfo.Procedurebody = procedureBodyInfo.Procedurebody + tpStr + " `" + rows[0].GetString(0) + "`(" + rows[0].GetString(3) + ")\n"
	if len(rows[0].GetString(7)) != 0 {
		procedureBodyInfo.Procedurebody = procedureBodyInfo.Procedurebody + "COMMENT '" + rows[0].GetString(7) + "' \n"
	}
	if rows[0].GetEnum(8).String() == "INVOKER" {
		procedureBodyInfo.Procedurebody = procedureBodyInfo.Procedurebody + "SQL SECURITY INVOKER \n"
	}
	procedureBodyInfo.Procedurebody = procedureBodyInfo.Procedurebody + rows[0].GetString(2)
	procedureBodyInfo.SQLMode = rows[0].GetSet(1).String()
	procedureBodyInfo.CharacterSetClient = rows[0].GetString(4)
	procedureBodyInfo.CollationConnection = rows[0].GetString(5)
	procedureBodyInfo.ShemaCollation = rows[0].GetString(6)
	return procedureBodyInfo, nil
}

// getRowsProcedure read rows from table.
func (e *ShowExec) getRowsProcedure(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, showType string) error {
	sql := new(strings.Builder)
	var (
		fieldPatternsLike collate.WildcardPattern
		fieldFilter       string
	)
	if e.Extractor != nil {
		fieldFilter = e.Extractor.Field()
		fieldPatternsLike = e.Extractor.FieldPatternLike()
	}
	//head []string{"Db", "Name", "Type", "Definer", "Modified", "Created", "Security_type", "Comment", "character_set_client", "collation_connection", "Database Collation"}
	sqlescape.MustFormatSQL(sql, "select route_schema, name, type, definer, last_altered, created, security_type, comment,")
	sqlescape.MustFormatSQL(sql, "character_set_client, connection_collation, schema_collation from %n.%n where type = %?", mysql.SystemDB, mysql.Routines, showType)
	recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	if err != nil {
		return err
	}
	if recordSet != nil {
		defer recordSet.Close()
	}

	rows, err := sqlexec.DrainRecordSet(ctx, recordSet, 1024)
	if err != nil {
		return err
	}

	for _, row := range rows {
		if fieldFilter != "" && row.GetString(1) != fieldFilter {
			continue
		} else if fieldPatternsLike != nil && !fieldPatternsLike.DoMatch(row.GetString(1)) {
			continue
		}
		e.appendRow([]any{row.GetString(0), row.GetString(1), row.GetEnum(2).String(), row.GetString(3), row.GetTime(4), row.GetTime(5), row.GetEnum(6).String(),
			row.GetString(7), row.GetString(8), row.GetString(9), row.GetString(10)})
	}
	return nil
}

// fetchShowCreateProcdure query stored procedure.
func (e *ShowExec) fetchShowCreateProcdure(ctx context.Context, tp ast.ShowStmtType) error {
	if e.Procedure.Schema.O == "" {
		e.Procedure.Schema = pmodel.NewCIStr(e.Ctx().GetSessionVars().CurrentDB)
	}
	_, ok := e.is.SchemaByName(e.Procedure.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(e.DBName.O)
	}
	internalCtx := kv.WithInternalSourceType(ctx, kv.InternalTxnProcedure)
	sqlExecutor := e.Ctx().(sqlexec.SQLExecutor)
	procedureInfo, err := getProcedureInfo(internalCtx, sqlExecutor, e.Procedure.Name.String(), e.Procedure.Schema.O, tp)
	if err != nil {
		return err
	}
	//names = []string{"Procedure", "sql_mode", "Create Procedure", "character_set_client", "collation_connection", "Database Collation"}
	e.appendRow([]any{procedureInfo.Name, procedureInfo.SQLMode, procedureInfo.Procedurebody, procedureInfo.CharacterSetClient,
		procedureInfo.CollationConnection, procedureInfo.ShemaCollation})
	return nil
}

// fetchShowProcedureStatus implement SHOW PROCEDURE STATUS [LIKE 'pattern' | WHERE expr]
func (e *ShowExec) fetchShowProcedureStatus(ctx context.Context, showType string) error {
	internalCtx := kv.WithInternalSourceType(ctx, kv.InternalTxnProcedure)
	sqlExecutor := e.Ctx().(sqlexec.SQLExecutor)
	err := e.getRowsProcedure(internalCtx, sqlExecutor, showType)
	if err != nil {
		return err
	}
	return nil
}

// buildCallProcedure generate the execution plan of the call.
func (b *executorBuilder) buildCallProcedure(v *plannercore.CallStmt) exec.Executor {
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.SetInitCap(chunk.ZeroCapacity)
	var securityType string
	switch v.Plan.SecurityType {
	case "INVOKER":
		securityType = "INVOKER"
	case "DEFAULT", "DEFINER":
		securityType = "DEFINER"
	default:
		b.err = errors.Errorf("unsupport security type %s", v.Plan.SecurityType)
		return nil
	}
	e := &ProcedureExec{
		BaseExecutor:        base,
		Statement:           v.Callstmt,
		flag:                0,
		is:                  b.is,
		done:                false,
		ProcedureSQLMod:     v.ProcedureSQLMod,
		outVarParam:         make(map[string]string, 10),
		outLocalParam:       make(map[string]string, 10),
		outTriggerParam:     make(map[string]int, 10),
		procedurePlan:       v.Plan.ProcedureExecPlan,
		cachedProcedurePlan: v.CachedProcedurePlan,
		IsStrict:            v.IsStrictMode,
		definerHost:         v.Plan.DefinerHost,
		definerUser:         v.Plan.DefinerUser,
		securityType:        securityType,
		buildPlan:           b.build,
	}
	return e
}

func (b *executorBuilder) buildTriggerProcedure(v *plannercore.TriggerProcedure) exec.Executor {
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.SetInitCap(chunk.ZeroCapacity)
	sqlMode, ok := b.ctx.GetSessionVars().GetSystemVar(variable.SQLModeVar)
	if !ok {
		b.err = errors.New("unknown system var " + variable.SQLModeVar)
	}
	e := &ProcedureExec{
		BaseExecutor:        base,
		Statement:           nil,
		flag:                0,
		is:                  b.is,
		done:                false,
		ProcedureSQLMod:     sqlMode,
		outVarParam:         make(map[string]string, 10),
		outLocalParam:       make(map[string]string, 10),
		outTriggerParam:     make(map[string]int, 10),
		procedurePlan:       v.Plan.ProcedureExecPlan,
		cachedProcedurePlan: &plannercore.RoutineCacahe{},
		IsStrict:            b.ctx.GetSessionVars().SQLMode.HasStrictMode(),
		definerHost:         v.Plan.DefinerHost,
		definerUser:         v.Plan.DefinerUser,
		securityType:        "INVOKER",
		buildPlan:           b.build,
	}
	return e
}

func init() {
	expression.Eval4StoredFunc = func(sctx sessionctx.Context, iface any, s *ast.CallStmt) (*types.Datum, error) {
		if !variable.TiDBEnableProcedureValue.Load() {
			return nil, exeerrors.ErrProcedureDisabled
		}
		v := iface.(*plannercore.CallStmt)
		plan := v.Plan.DeepCopyForExecution()
		if plan == nil {
			return nil, errors.New("missing stored function execution plan")
		}
		var securityType string
		switch plan.SecurityType {
		case "INVOKER":
			securityType = "INVOKER"
		case "DEFAULT", "DEFINER":
			securityType = "DEFINER"
		}
		e := &ProcedureExec{
			BaseExecutor:        exec.NewBaseExecutor(sctx, nil, 0),
			Statement:           v.Callstmt,
			is:                  v.Is,
			flag:                0,
			done:                false,
			ProcedureSQLMod:     v.ProcedureSQLMod,
			outVarParam:         make(map[string]string, 10),
			outLocalParam:       make(map[string]string, 10),
			outTriggerParam:     make(map[string]int, 10),
			procedurePlan:       plan.ProcedureExecPlan,
			cachedProcedurePlan: v.CachedProcedurePlan,
			IsStrict:            v.IsStrictMode,
			definerHost:         plan.DefinerHost,
			definerUser:         plan.DefinerUser,
			securityType:        securityType,
			isFunction:          true,
		}
		return e.callProcedure(context.Background(), s)
	}
}

func (e *ProcedureExec) hasSet(flag int) bool {
	return e.flag&flag > 0
}

func (e *ProcedureExec) setFlag(flag int) {
	e.flag = e.flag | flag
}

func (e *ProcedureExec) unsetFlag(flag int) {
	e.flag = e.flag & (^flag)
}

// inParam handle input parameters.
func (e *ProcedureExec) inParam(ctx context.Context, param *plannercore.ProcedureParameterVal, inName string) error {
	// read data from user variable by name.
	datum, ok := e.Ctx().GetSessionVars().GetUserVarVal(inName)
	if !ok {
		datum = types.NewDatum("")
		datum.SetNull()
	}

	return plannercore.UpdateVariableVar(param.DeclName, datum, e.Ctx().GetSessionVars())
}

// callParam handle store procedure parameters.
func (e *ProcedureExec) callParam(ctx context.Context, s *ast.CallStmt) error {
	// When calling recursively, read and save the environment variables of the upper-level stored procedure.
	if e.Ctx().GetSessionVars().InOtherCall() {
		e.parentContext = e.Ctx().GetSessionVars().GetProcedureContext().Context
	}
	// Set environment variables for the current stored procedure.
	e.Ctx().GetSessionVars().SetProcedureContext(e.procedurePlan.ProcedureCtx)
	if len(e.procedurePlan.ProcedureParam) != len(s.Procedure.Args) {
		return exeerrors.ErrSpWrongNoOfArgs.GenWithStackByArgs("PROCEDURE", s.Procedure.Schema.String()+"."+s.Procedure.FnName.String(), len(e.procedurePlan.ProcedureParam), len(s.Procedure.Args))
	}
	plannercore.ResetCallStatus(e.Ctx())
	for i, param := range e.procedurePlan.ProcedureParam {
		switch v := s.Procedure.Args[i].(type) {
		// input variable is @name
		case *ast.VariableExpr:
			name := s.Procedure.Args[i].(*ast.VariableExpr).Name
			if (param.ParamType == ast.MODE_IN) || (param.ParamType == ast.MODE_INOUT) {
				err := e.inParam(ctx, param, name)
				if err != nil {
					return err
				}
			}
			if param.ParamType == ast.MODE_INOUT {
				e.outVarParam[param.DeclName] = name
			}
			if param.ParamType == ast.MODE_OUT {
				e.outVarParam[param.DeclName] = name
			}
		case *ast.ColumnNameExpr:
			sc := e.Ctx().GetSessionVars().StmtCtx
			if v.Name.IsTriggerPseudoRecord() && sc.TriggerCtx.InTrigger {
				datum, idx, err := getTriggerRowDatum(sc, v)
				if err != nil {
					return err
				}
				if param.ParamType == ast.MODE_OUT || param.ParamType == ast.MODE_INOUT {
					routineName := s.Procedure.Schema.String() + "." + s.Procedure.FnName.String()
					if v.Name.Table.L != "new" {
						return exeerrors.ErrSpNotVarArg.GenWithStackByArgs(i+1, routineName)
					}
					// MySQL semantics: OUT/INOUT arguments must be a variable or NEW pseudo-variable in BEFORE trigger.
					// TiDB uses TriggerCtx.NewData/Updated to indicate whether NEW exists / is writable.
					if len(sc.TriggerCtx.Updated) == 0 {
						return exeerrors.ErrSpNotVarArg.GenWithStackByArgs(i+1, routineName)
					}
				}
				if param.ParamType == ast.MODE_OUT {
					datum.SetNull()
				}
				err = plannercore.UpdateVariableVar(param.DeclName, datum, e.Ctx().GetSessionVars())
				if err != nil {
					return err
				}
				if param.ParamType == ast.MODE_OUT || param.ParamType == ast.MODE_INOUT {
					e.outTriggerParam[param.DeclName] = idx
				}
				break
			}

			if !e.Ctx().GetSessionVars().InOtherCall() {
				return exeerrors.ErrBadField.GenWithStackByArgs(s.Procedure.Args[i].(*ast.ColumnNameExpr).Name.Name.O, "field list")
			}

			_, datum, notFind := e.parentContext.GetProcedureVariable(s.Procedure.Args[i].(*ast.ColumnNameExpr).Name.Name.L)
			if notFind {
				return exeerrors.ErrBadField.GenWithStackByArgs(s.Procedure.Args[i].(*ast.ColumnNameExpr).Name.Name.O, "field list")
			}
			err := plannercore.UpdateVariableVar(param.DeclName, datum, e.Ctx().GetSessionVars())
			if err != nil {
				return err
			}
			if param.ParamType == ast.MODE_INOUT || param.ParamType == ast.MODE_OUT {
				e.outLocalParam[param.DeclName] = s.Procedure.Args[i].(*ast.ColumnNameExpr).Name.Name.O
			}
		case *driver.ValueExpr:
			// out variable must @name
			if (param.ParamType == ast.MODE_OUT) || (param.ParamType == ast.MODE_INOUT) {
				return exeerrors.ErrSpNotVarArg.GenWithStackByArgs(i+1, s.Procedure.Schema.String()+"."+s.Procedure.FnName.String())
			}
			err := plannercore.UpdateVariableVar(param.DeclName, v.Datum, e.Ctx().GetSessionVars())
			if err != nil {
				return err
			}
		default:
			// out variable must @name
			if (param.ParamType == ast.MODE_OUT) || (param.ParamType == ast.MODE_INOUT) {
				return exeerrors.ErrSpNotVarArg.GenWithStackByArgs(i+1, s.Procedure.Schema.String()+"."+s.Procedure.FnName.String())
			}
			exec := plannercore.ProcedureCompileAndExec(e.executeWithSameContext)
			if e.Ctx().GetSessionVars().InOtherCall() {
				e.Ctx().GetSessionVars().SetProcedureContext(e.parentContext)
				datum, err := plannercore.GetExprValue(ctx, plannercore.NewCacheExpr(true, plannercore.ExprNodeToString(s.Procedure.Args[i]), nil),
					param.DeclType, e.Ctx(), param.DeclName, exec)
				if err != nil {
					return err
				}
				e.Ctx().GetSessionVars().SetProcedureContext(e.procedurePlan.ProcedureCtx)
				err = plannercore.UpdateVariableVar(param.DeclName, datum, e.Ctx().GetSessionVars())
				if err != nil {
					return err
				}
			} else {
				e.Ctx().GetSessionVars().OutCallTemp()
				datum, err := plannercore.GetExprValue(ctx, plannercore.NewCacheExpr(true, plannercore.ExprNodeToString(s.Procedure.Args[i]), nil),
					param.DeclType, e.Ctx(), param.DeclName, exec)
				e.Ctx().GetSessionVars().RecoveryCall()
				e.Ctx().GetSessionVars().SetProcedureContext(e.procedurePlan.ProcedureCtx)
				if err != nil {
					return err
				}
				err = plannercore.UpdateVariableVar(param.DeclName, datum, e.Ctx().GetSessionVars())
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func ensureTriggerNewWritable(sc *stmtctx.StatementContext) error {
	triggerCtx := sc.TriggerCtx
	// MySQL semantics: NEW is only writable in BEFORE INSERT/UPDATE triggers.
	// TiDB uses TriggerCtx.NewData/Updated to indicate whether NEW exists / is writable.
	if exec, ok := triggerCtx.Exec.(*TriggerExec); ok {
		if exec.currentEvent() == ast.TriggerEventDelete {
			return dbterror.ErrTrgNoSuchRowInTrg.FastGenByArgs("NEW", exec.currentEvent().String())
		}
		if exec.currentTiming() == ast.TriggerTimingAfter {
			return dbterror.ErrTrgCantChangeRow.FastGenByArgs("NEW", exec.currentTiming().String()+" ")
		}
	}
	if len(triggerCtx.NewData) == 0 {
		// e.g. DELETE trigger.
		return dbterror.ErrTrgNoSuchRowInTrg.FastGenByArgs("NEW", "")
	}
	if len(triggerCtx.Updated) == 0 {
		// e.g. AFTER trigger.
		return dbterror.ErrTrgCantChangeRow.FastGenByArgs("NEW", "")
	}
	return nil
}

func getTriggerRowDatum(sc *stmtctx.StatementContext, v *ast.ColumnNameExpr) (types.Datum, int, error) {
	idx := -1
	for i, c := range sc.TriggerCtx.TableInfo.Columns {
		if c.Name.L == v.Name.Name.L {
			idx = i
			break
		}
	}
	if idx == -1 {
		return types.Datum{}, 0, exeerrors.ErrBadField.GenWithStackByArgs(v.Name.Name.O, "field list")
	}
	switch v.Name.Table.L {
	case "new":
		if len(sc.TriggerCtx.NewData) == 0 {
			if exec, ok := sc.TriggerCtx.Exec.(*TriggerExec); ok {
				return types.Datum{}, 0, dbterror.ErrTrgNoSuchRowInTrg.FastGenByArgs("NEW", exec.currentEvent().String())
			}
			return types.Datum{}, 0, dbterror.ErrTrgNoSuchRowInTrg.FastGenByArgs("NEW", "")
		}
		if idx >= len(sc.TriggerCtx.NewData) {
			return types.Datum{}, 0, errors.Errorf("trigger pseudo record index out of range: colIdx=%d, newLen=%d", idx, len(sc.TriggerCtx.NewData))
		}
		return sc.TriggerCtx.NewData[idx], idx, nil
	case "old":
		if len(sc.TriggerCtx.OldData) == 0 {
			if exec, ok := sc.TriggerCtx.Exec.(*TriggerExec); ok {
				return types.Datum{}, 0, dbterror.ErrTrgNoSuchRowInTrg.FastGenByArgs("OLD", exec.currentEvent().String())
			}
			return types.Datum{}, 0, dbterror.ErrTrgNoSuchRowInTrg.FastGenByArgs("OLD", "")
		}
		if idx >= len(sc.TriggerCtx.OldData) {
			return types.Datum{}, 0, errors.Errorf("trigger pseudo record index out of range: colIdx=%d, oldLen=%d", idx, len(sc.TriggerCtx.OldData))
		}
		return sc.TriggerCtx.OldData[idx], idx, nil
	}
	return types.Datum{}, 0, exeerrors.ErrBadField.GenWithStackByArgs(v.Name.Name.O, "field list")
}

// outParams handle out parameters.
func (e *ProcedureExec) outParams(ctx context.Context) error {
	plannercore.ResetCallStatus(e.Ctx())
	for key, v := range e.outVarParam {
		varType, datum, notFind := e.procedurePlan.ProcedureCtx.GetProcedureVariable(key)
		if notFind {
			continue
		}
		if datum.IsNull() {
			e.Ctx().GetSessionVars().UnsetUserVar(v)
		} else {
			e.Ctx().GetSessionVars().SetUserVarVal(v, datum)
			e.Ctx().GetSessionVars().SetUserVarType(v, varType)
		}
	}

	trigCtx := e.Ctx().GetSessionVars().StmtCtx.TriggerCtx
	if trigCtx.InTrigger && len(e.outTriggerParam) > 0 {
		if err := ensureTriggerNewWritable(e.Ctx().GetSessionVars().StmtCtx); err != nil {
			return err
		}
		for k, idx := range e.outTriggerParam {
			_, datum, notFind := e.procedurePlan.ProcedureCtx.GetProcedureVariable(k)
			if notFind {
				continue
			}
			casted, err := table.CastColumnValue(e.Ctx().GetExprCtx(), datum, trigCtx.TableInfo.Columns[idx], false, false)
			if err != nil {
				return err
			}
			if idx >= len(trigCtx.NewData) || idx >= len(trigCtx.Updated) {
				return errors.Errorf("trigger pseudo record index out of range: colIdx=%d, newLen=%d, updatedLen=%d", idx, len(trigCtx.NewData), len(trigCtx.Updated))
			}
			trigCtx.NewData[idx] = casted
			trigCtx.Updated[idx] = true
		}
	}

	e.Ctx().GetSessionVars().SetProcedureContext(e.parentContext)
	for key, v := range e.outLocalParam {
		_, datum, notFind := e.procedurePlan.ProcedureCtx.GetProcedureVariable(key)
		if notFind {
			continue
		}
		err := plannercore.UpdateVariableVar(v, datum, e.Ctx().GetSessionVars())
		if err != nil {
			return err
		}
	}

	return nil
}

// callProcedure implement call.
func (e *ProcedureExec) callProcedure(ctx context.Context, s *ast.CallStmt) (funcRet *types.Datum, err error) {
	if !variable.TiDBEnableProcedureValue.Load() {
		return nil, exeerrors.ErrProcedureDisabled
	}
	e.Ctx().GetSessionVars().SetInCallProcedure()
	defer func() {
		if e.isFunction && e.parentContext != nil {
			restoreErr := e.Ctx().GetSessionVars().SetProcedureContext(e.parentContext)
			if err == nil && restoreErr != nil {
				err = restoreErr
			}
		}
		e.Ctx().GetSessionVars().OutCallProcedure(err != nil)
	}()
	//The last FinishExecuteStmt is expected to be a call statement.
	var originalSQL string
	if e.Ctx().GetSessionVars().StmtCtx != nil {
		originalSQL = e.Ctx().GetSessionVars().StmtCtx.OriginalSQL
	}

	clientCapabilitySave := e.Ctx().GetSessionVars().ClientCapability
	e.Ctx().GetSessionVars().ClientCapability = (e.Ctx().GetSessionVars().ClientCapability | mysql.ClientMultiStatements)
	oldUser := e.Ctx().GetSessionVars().User
	needChangeUser := false
	if e.securityType == "DEFINER" && (oldUser == nil ||
		(e.definerUser != oldUser.AuthUsername || e.definerHost != oldUser.AuthHostname)) {
		needChangeUser = true
	}
	oldRole := e.Ctx().GetSessionVars().ActiveRoles
	pm := privilege.GetPrivilegeManager(e.Ctx())
	if needChangeUser {
		// SQL SECURITY is definer ,user definer user to execute
		if e.definerUser != "" {
			definer := &auth.UserIdentity{Username: e.definerUser, Hostname: e.definerHost}
			var success bool
			definer.AuthUsername, definer.AuthHostname, success = pm.MatchIdentity(definer.Username, definer.Hostname, false)
			if !success {
				return nil, exeerrors.ErrNoSuchUser.GenWithStackByArgs(e.definerUser, e.definerHost)
			}
			if !pm.GetAuthWithoutVerification(e.definerUser, e.definerHost) {
				return nil, exeerrors.ErrNoSuchUser.GenWithStackByArgs(e.definerUser, e.definerHost)
			}
			e.Ctx().GetSessionVars().User = definer
			e.Ctx().GetSessionVars().ActiveRoles = pm.GetDefaultRoles(e.definerUser, e.definerHost)
		} else {
			e.Ctx().GetSessionVars().User = nil
			e.Ctx().GetSessionVars().ActiveRoles = nil
		}
	}
	if e.cachedProcedurePlan.MyRecursionLevel > e.Ctx().GetSessionVars().MaxSpRecursionDepth {
		return nil, exeerrors.ErrSpRecursionLimit.FastGenByArgs(e.Ctx().GetSessionVars().MaxSpRecursionDepth, s.Procedure.FnName.String())
	}
	err = plannercore.CallPrepareCheck(ctx, e.procedurePlan.ProcedureCommandList, e.Ctx())
	if err != nil {
		return nil, err
	}
	e.cachedProcedurePlan.MyRecursionLevel++
	oldStr := e.Ctx().GetSessionVars().LastProcedureErrorStr
	defer func() {
		e.cachedProcedurePlan.MyRecursionLevel--
		e.Ctx().GetSessionVars().ClientCapability = clientCapabilitySave
		e.Ctx().GetSessionVars().LastProcedureErrorStr = oldStr
		if err == nil {
			// In order to clean up the display cache (sql, execution plan, hash value, etc.) rebuild the StatementContext
			// StatementContext some structures use sync.Once unable to update.
			sc := stmtctx.NewStmtCtx()
			if !e.Ctx().GetSessionVars().StmtCtx.TriggerCtx.InTrigger && !e.isFunction {
				sc.OriginalSQL = originalSQL
				e.Ctx().GetSessionVars().StmtCtx.CopyMuForCallProcedure(sc)
				e.Ctx().GetSessionVars().StmtCtx = sc
			}
		}
		if needChangeUser {
			e.Ctx().GetSessionVars().ActiveRoles = oldRole
			e.Ctx().GetSessionVars().User = oldUser
			if oldUser != nil {
				pm.AuthSuccess(oldUser.AuthUsername, oldUser.AuthHostname)
			} else {
				pm.AuthSuccess("", "")
			}
		}
	}()
	var val int
	// sp use routines database as default database.
	sysvar := variable.GetSysVar(variable.LowerCaseTableNames)
	val, err = strconv.Atoi(sysvar.Value)
	if err != nil {
		return nil, err
	}
	is := schematracker.NewSchemaTracker(val)
	key := is.InfoStore.CiStr2Key(s.Procedure.Schema)
	if key != e.Ctx().GetSessionVars().CurrentDB {
		oldDB := e.Ctx().GetSessionVars().CurrentDB
		oldDBCI := e.Ctx().GetSessionVars().CurrentDBCI
		defer func() {
			e.Ctx().GetSessionVars().CurrentDB = oldDB
			e.Ctx().GetSessionVars().CurrentDBCI = oldDBCI
		}()
		e.Ctx().GetSessionVars().CurrentDB = key
		e.Ctx().GetSessionVars().CurrentDBCI = pmodel.NewCIStr(key)
	}

	// handle in\out variable.
	err = e.callParam(ctx, s)
	if err != nil {
		return nil, err
	}
	sqlModeSave, ok := e.Ctx().GetSessionVars().GetSystemVar(variable.SQLModeVar)
	if !ok {
		return nil, errors.New("can not find sql_mode")
	}
	sqlMode, err := mysql.GetSQLMode(e.ProcedureSQLMod)
	if err != nil {
		return nil, errors.Trace(err)
	}
	originSQLMode := e.Ctx().GetSessionVars().SQLMode
	e.Ctx().GetSessionVars().SQLMode = sqlMode
	err = e.Ctx().GetSessionVars().SetSystemVar(variable.SQLModeVar, e.ProcedureSQLMod)
	defer func() {
		restoreErr := e.Ctx().GetSessionVars().SetSystemVar(variable.SQLModeVar, sqlModeSave)
		if restoreErr != nil {
			logutil.BgLogger().Error("call recover sql_mode failed. err:", zap.Error(restoreErr))
		}
		e.Ctx().GetSessionVars().SQLMode = originSQLMode
	}()
	if err != nil {
		return nil, err
	}
	// use sql_mode defined in creating procedure.
	plannercore.ResetCallStatus(e.Ctx())
	e.cache = make([]plannercore.NeedCloseCur, 0, 10)
	// actually execute the stored procedure.
	err = e.executeCall(ctx)
	e.cache = plannercore.ReleaseAll(e.cache)
	if err != nil {
		return nil, err
	}
	ret := e.procedurePlan.ProcedureCtx.GetReturnDatum()
	if ret != nil {
		return ret, nil
	}
	if e.isFunction {
		return nil, exeerrors.ErrSpNoreturnend.GenWithStackByArgs(s.Procedure.Schema.String() + "." + s.Procedure.FnName.String())
	}
	err = e.outParams(ctx)
	return nil, err
}

func (e *ProcedureExec) handleErrorCond(ctx context.Context, pContext *variable.ProcedureContext,
	err error, id *uint, cache []plannercore.NeedCloseCur) ([]plannercore.NeedCloseCur, error) {
	if pContext == nil {
		return cache, err
	}
	var errStatus string
	var errCode int
	var ok bool
	err1 := errors.Cause(err)
	switch v := err1.(type) {
	case *errors.Error:
		errCode = int(v.Code())
		errStatus, ok = mysql.MySQLState[uint16(errCode)]
		if !ok {
			errStatus = mysql.DefaultMySQLState
		}
	case *terror.TiDBError:
		errCode = int(v.MYSQLERRNO)
		errStatus = v.SQLSTATE
	default:
		return cache, err
	}
	baseHandle := pContext.TryFindHandle(errCode, errStatus, e.hasSet(hasWarning))
	if baseHandle == nil {
		return cache, err
	}
	if !e.hasSet(hasWarning) {
		n := len(e.Ctx().GetSessionVars().StmtCtx.GetWarnings()) - 1
		if n < 0 || e.Ctx().GetSessionVars().StmtCtx.GetWarnings()[n].Level != contextutil.WarnLevelError {
			e.Ctx().GetSessionVars().StmtCtx.AppendError(err)
		}
	}
	cache, err = e.procedurePlan.ProcedureCommandList[baseHandle.Operate].Execute(ctx, e.Ctx(), id, cache)
	if err != nil {
		return cache, err
	}
	return cache, nil
}

func (e *ProcedureExec) executeCall(ctx context.Context) error {
	var ip, oldIP uint
	var err error
	ip = 0
	maxID := uint(len(e.procedurePlan.ProcedureCommandList))
	vars := e.Ctx().GetSessionVars()
	for {
		if err := vars.SQLKiller.HandleSignal(); err != nil {
			return errors.Trace(exeerrors.ErrQueryInterrupted)
		}
		if ip >= maxID {
			break
		}
		oldIP = ip
		cmd := e.procedurePlan.ProcedureCommandList[ip]
		if vars.StmtCtx.TriggerCtx.InTrigger || e.isFunction {
			cmd.RegisterCompileAndExecFunc(e.executeWithSameContext)
		}

		e.cache, err = cmd.Execute(ctx, e.Ctx(), &ip, e.cache)
		warnings := vars.StmtCtx.GetWarnings()
		if err != nil || len(warnings) > 0 {
			pContext := e.procedurePlan.ProcedureCommandList[oldIP].GetContext()
			if err != nil {
				e.unsetFlag(hasWarning)
				e.cache, err = e.handleErrorCond(ctx, pContext, err, &ip, e.cache)
				if err != nil {
					return err
				}
				e.Ctx().GetSessionVars().LastProcedureErrorStr = e.procedurePlan.ProcedureCommandList[oldIP].GetString(e.Ctx(), errStr)
			} else if e.procedurePlan.ProcedureCommandList[oldIP].Hanlable() {
				e.setFlag(hasWarning)
				num := len(warnings)
				for i := num - 1; i >= 0; i-- {
					var err error
					e.cache, err = e.handleErrorCond(ctx, pContext, warnings[i].Err, &ip, e.cache)
					if err == nil {
						// handler warning and logging warning SQL
						e.Ctx().GetSessionVars().LastProcedureErrorStr = e.procedurePlan.ProcedureCommandList[oldIP].GetString(e.Ctx(), warnStr)
						break
					}
				}
			}
		}
	}
	return nil
}

func (e *ProcedureExec) executeWithSameContext(ctx context.Context, stmtNode ast.StmtNode) ([]chunk.Row, []*types.FieldType, error) {
	defer resetStmtCtx(e.Ctx(), stmtNode)()
	nodeW := resolve.NewNodeW(stmtNode)
	err := plannercore.Preprocess(ctx, e.Ctx(), nodeW)
	if err != nil {
		return nil, nil, err
	}

	p, _, err := planner.Optimize(ctx, e.Ctx(), nodeW, e.is)
	if err != nil {
		return nil, nil, err
	}

	builder := newExecutorBuilder(e.Ctx(), e.is, nil)
	if triggerCtx := e.Ctx().GetSessionVars().StmtCtx.TriggerCtx; triggerCtx.InTrigger {
		if trigExec, ok := triggerCtx.Exec.(*TriggerExec); ok {
			// Preserve the caller's latest executor to avoid leaking the inner statement's executor
			// into the outer trigger execution context (see TriggerExec.latestExec).
			origLatestExec := trigExec.latestExec
			defer func() { trigExec.latestExec = origLatestExec }()
			builder.triggerExec = trigExec
		}
	}
	newExec := builder.build(p)
	if builder.err != nil {
		return nil, nil, builder.err
	}
	if newExec == nil {
		return nil, nil, errors.New("failed to build executor for stored function")
	}

	if err := exec.Open(ctx, newExec); err != nil {
		terror.Log(exec.Close(newExec))
		return nil, nil, err
	}

	fieldTypes := make([]*types.FieldType, 0, len(newExec.Schema().Columns))
	for _, field := range newExec.Schema().Columns {
		fieldTypes = append(fieldTypes, field.RetType)
	}

	var rows []chunk.Row
	for {
		chk := exec.NewFirstChunk(newExec)
		err = exec.Next(ctx, newExec, chk)
		if err != nil {
			break
		}
		if chk.NumRows() == 0 {
			break
		}
		if rows == nil {
			rows = make([]chunk.Row, 0, chk.NumRows())
		}
		for i := range chk.NumRows() {
			rows = append(rows, chk.GetRow(i))
		}
	}

	closeErr := exec.Close(newExec)
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, nil, err
	}
	e.Ctx().StmtCommit(ctx)
	return rows, fieldTypes, nil
}

func resetStmtCtx(sctx sessionctx.Context, s ast.StmtNode) func() {
	vars := sctx.GetSessionVars()
	sc := vars.StmtCtx
	strictSQLMode := vars.SQLMode.HasStrictMode()
	errLevels := sc.ErrLevels()
	originalFlags := sc.TypeFlags()
	originalErrLevels := errLevels
	originalInInsertStmt := sc.InInsertStmt
	originalInUpdateStmt := sc.InUpdateStmt
	originalInDeleteStmt := sc.InDeleteStmt
	originalInSelectStmt := sc.InSelectStmt
	originalInLoadDataStmt := sc.InLoadDataStmt
	originalInCreateOrAlterStmt := sc.InCreateOrAlterStmt
	originalInSetSessionStatesStmt := sc.InSetSessionStatesStmt
	originalInShowWarning := sc.InShowWarning
	originalInDiagnostics := sc.InDiagnostics
	originalLastWarningNum := sc.LastWarningNum
	originalPriority := sc.Priority
	originalNotFillCache := sc.NotFillCache
	originalWeakConsistency := sc.WeakConsistency

	restore := func() {
		sc := sctx.GetSessionVars().StmtCtx
		sc.InInsertStmt = originalInInsertStmt
		sc.InUpdateStmt = originalInUpdateStmt
		sc.InDeleteStmt = originalInDeleteStmt
		sc.InSelectStmt = originalInSelectStmt
		sc.InLoadDataStmt = originalInLoadDataStmt
		sc.InCreateOrAlterStmt = originalInCreateOrAlterStmt
		sc.InSetSessionStatesStmt = originalInSetSessionStatesStmt
		sc.InShowWarning = originalInShowWarning
		sc.InDiagnostics = originalInDiagnostics
		sc.LastWarningNum = originalLastWarningNum
		sc.Priority = originalPriority
		sc.NotFillCache = originalNotFillCache
		sc.WeakConsistency = originalWeakConsistency
		sc.SetTypeFlags(originalFlags)
		sc.SetErrLevels(originalErrLevels)
	}

	sc.InInsertStmt = false
	sc.InUpdateStmt = false
	sc.InDeleteStmt = false
	sc.InSelectStmt = false
	sc.InLoadDataStmt = false
	sc.InCreateOrAlterStmt = false
	sc.InSetSessionStatesStmt = false
	sc.InShowWarning = false
	sc.InDiagnostics = false
	sc.LastWarningNum = 0
	sc.Priority = mysql.NoPriority
	sc.NotFillCache = false
	sc.WeakConsistency = false

	errLevels[errctx.ErrGroupDividedByZero] = errctx.LevelWarn
	switch stmt := s.(type) {
	// `ResetUpdateStmtCtx` and `ResetDeleteStmtCtx` may modify the flags, so we'll need to store them.
	case *ast.UpdateStmt:
		ResetUpdateStmtCtx(sc, stmt, vars)
		errLevels = sc.ErrLevels()
	case *ast.DeleteStmt:
		ResetDeleteStmtCtx(sc, stmt, vars)
		errLevels = sc.ErrLevels()
	case *ast.InsertStmt:
		sc.InInsertStmt = true
		// For insert statement (not for update statement), disabling the StrictSQLMode
		// should make TruncateAsWarning and DividedByZeroAsWarning,
		// but should not make DupKeyAsWarning.
		if stmt.IgnoreErr {
			errLevels[errctx.ErrGroupDupKey] = errctx.LevelWarn
			errLevels[errctx.ErrGroupAutoIncReadFailed] = errctx.LevelWarn
			errLevels[errctx.ErrGroupNoMatchedPartition] = errctx.LevelWarn
		}
		// For single-row INSERT statements, ignore non-strict mode
		// See https://dev.mysql.com/doc/refman/5.7/en/constraint-invalid-data.html
		isSingleInsert := len(stmt.Lists) == 1
		errLevels[errctx.ErrGroupBadNull] = errctx.ResolveErrLevel(false, (!strictSQLMode && !isSingleInsert) || stmt.IgnoreErr)
		errLevels[errctx.ErrGroupNoDefault] = errctx.ResolveErrLevel(false, !strictSQLMode || stmt.IgnoreErr)
		errLevels[errctx.ErrGroupDividedByZero] = errctx.ResolveErrLevel(
			!vars.SQLMode.HasErrorForDivisionByZeroMode(),
			!strictSQLMode || stmt.IgnoreErr,
		)
		sc.Priority = stmt.Priority
		sc.SetTypeFlags(util.GetTypeFlagsForInsert(sc.TypeFlags(), vars.SQLMode, stmt.IgnoreErr))
	case *ast.CreateTableStmt, *ast.AlterTableStmt:
		sc.InCreateOrAlterStmt = true
		sc.SetTypeFlags(sc.TypeFlags().
			WithTruncateAsWarning(!strictSQLMode).
			WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()).
			WithIgnoreZeroInDate(!vars.SQLMode.HasNoZeroInDateMode() || !strictSQLMode ||
				vars.SQLMode.HasAllowInvalidDatesMode()).
			WithIgnoreZeroDateErr(!vars.SQLMode.HasNoZeroDateMode() || !strictSQLMode))

	case *ast.LoadDataStmt:
		sc.InLoadDataStmt = true
		// return warning instead of error when load data meet no partition for value
		errLevels[errctx.ErrGroupNoMatchedPartition] = errctx.LevelWarn
	case *ast.ImportIntoStmt:
		sc.SetTypeFlags(util.GetTypeFlagsForImportInto(sc.TypeFlags(), vars.SQLMode))
	case *ast.SelectStmt:
		sc.InSelectStmt = true

		// Return warning for truncate error in selection.
		sc.SetTypeFlags(sc.TypeFlags().
			WithTruncateAsWarning(true).
			WithIgnoreZeroInDate(true).
			WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()))
		if opts := stmt.SelectStmtOpts; opts != nil {
			sc.Priority = opts.Priority
			sc.NotFillCache = !opts.SQLCache
		}
		sc.WeakConsistency = isWeakConsistencyRead(sctx, stmt)
	case *ast.SetOprStmt:
		sc.InSelectStmt = true
		sc.SetTypeFlags(sc.TypeFlags().
			WithTruncateAsWarning(true).
			WithIgnoreZeroInDate(true).
			WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()))
	case *ast.ShowStmt:
		sc.SetTypeFlags(sc.TypeFlags().
			WithIgnoreTruncateErr(true).
			WithIgnoreZeroInDate(true).
			WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()))
		if stmt.Tp == ast.ShowWarnings || stmt.Tp == ast.ShowErrors || stmt.Tp == ast.ShowSessionStates {
			sc.InShowWarning = true
			sc.SetWarnings(vars.StmtCtx.GetWarnings())
		}
	case *ast.SplitRegionStmt:
		sc.SetTypeFlags(sc.TypeFlags().
			WithIgnoreTruncateErr(false).
			WithIgnoreZeroInDate(true).
			WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()))
	case *ast.SetSessionStatesStmt:
		sc.InSetSessionStatesStmt = true
		sc.SetTypeFlags(sc.TypeFlags().
			WithIgnoreTruncateErr(true).
			WithIgnoreZeroInDate(true).
			WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()))
	case *ast.CallStmt, *ast.CreateProcedureInfo:
		sc.SetTypeFlags(sc.TypeFlags().
			WithTruncateAsWarning(!strictSQLMode).
			WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()).
			WithIgnoreZeroInDate(!vars.SQLMode.HasNoZeroInDateMode() ||
				!vars.SQLMode.HasNoZeroDateMode() ||
				!strictSQLMode ||
				vars.SQLMode.HasAllowInvalidDatesMode()))
		errLevels[errctx.ErrGroupDividedByZero] = errctx.ResolveErrLevel(
			!vars.SQLMode.HasErrorForDivisionByZeroMode(), !strictSQLMode)
	case *ast.Signal:
		sc.SetTypeFlags(sc.TypeFlags().WithTruncateAsWarning(!strictSQLMode))
	case *ast.GetDiagnosticsStmt:
		sc.InDiagnostics = true
		sc.LastWarningNum = len(vars.StmtCtx.GetWarnings())
		sc.SetTypeFlags(sc.TypeFlags().WithTruncateAsWarning(!strictSQLMode))
		sc.SetWarnings(vars.StmtCtx.GetWarnings())
	default:
		sc.SetTypeFlags(sc.TypeFlags().
			WithIgnoreTruncateErr(true).
			WithIgnoreZeroInDate(true).
			WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()))
	}
	if errLevels != sc.ErrLevels() {
		sc.SetErrLevels(errLevels)
	}

	sc.SetTypeFlags(sc.TypeFlags().
		WithSkipUTF8Check(vars.SkipUTF8Check).
		WithSkipSACIICheck(vars.SkipASCIICheck).
		// WithAllowNegativeToUnsigned with false value indicates values less than 0 should be clipped to 0 for unsigned integer types.
		// This is the case for `insert`, `update`, `alter table`, `create table` and `load data infile` statements, when not in strict SQL mode.
		// see https://dev.mysql.com/doc/refman/5.7/en/out-of-range-and-overflow.html
		WithAllowNegativeToUnsigned(!sc.InInsertStmt && !sc.InLoadDataStmt && !sc.InUpdateStmt && !sc.InCreateOrAlterStmt),
	)

	if sc.MemTracker == nil {
		sc.InitMemTracker(memory.LabelForSQLText, -1)
		sc.MemTracker.AttachTo(vars.MemTracker)
	}
	if sc.DiskTracker == nil {
		sc.InitDiskTracker(memory.LabelForSQLText, -1)
		if vars.DiskTracker != nil {
			sc.DiskTracker.AttachTo(vars.DiskTracker)
		}
	}

	return restore
}
