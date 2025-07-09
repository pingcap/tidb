// Copyright 2022-2023 PingCAP, Inc.

package executor

import (
	"context"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/schematracker"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
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

const (
	// MaxRoutineComment indicates routine comment max len
	MaxRoutineComment = 65535
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
	procedurePlan       plannercore.ProcedureExec // store execution plan
	cachedProcedurePlan *plannercore.RoutineCacahe
	definerUser         string
	definerHost         string
	securityType        string
	cache               []plannercore.NeedCloseCur
	parentContext       *variable.ProcedureContext
}

// buildCreateProcedure Create stored procedure create executor.
func (b *executorBuilder) buildCreateProcedure(v *plannercore.CreateProcedure) exec.Executor {
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.SetInitCap(chunk.ZeroCapacity)
	e := &ProcedureExec{
		BaseExecutor:  base,
		Statement:     v.CreateProcedureInfo,
		is:            b.is,
		done:          false,
		outVarParam:   make(map[string]string, 10),
		outLocalParam: make(map[string]string, 10),
	}
	return e
}

// buildCreateProcedure create stored procedure drop executor.
func (b *executorBuilder) buildDropProcedure(v *plannercore.DropProcedure) exec.Executor {
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.SetInitCap(chunk.ZeroCapacity)
	e := &ProcedureExec{
		BaseExecutor: base,
		Statement:    v.Procedure,
		is:           b.is,
		done:         false,
	}
	return e
}

// autoNewTxn Check if it is an implicit transaction.
func (e *ProcedureExec) autoNewTxn() bool {
	switch e.Statement.(type) {
	case *ast.CreateProcedureInfo:
		return true
	case *ast.DropProcedureStmt:
		return true
	case *ast.AlterProcedureStmt:
		return true
	}
	return false
}

// Close ProcedureExec.
func (e *ProcedureExec) Close() error {
	e.outVarParam = nil
	e.outLocalParam = nil
	err := e.BaseExecutor.Close()
	if err != nil {
		return err
	}
	return nil
}

func (e *ProcedureExec) setDefiner(definer string) error {
	if len(definer) != 0 {
		strs := strings.Split(definer, "@")
		if len(strs) != 2 {
			return errors.Errorf("get definer:%s error", definer)
		}
		e.definerUser, e.definerHost = strs[0], strs[1]
	}
	return nil
}

func (b *executorBuilder) buildAlterProcedure(v *plannercore.AlterProcedure) exec.Executor {
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.SetInitCap(chunk.ZeroCapacity)
	e := &ProcedureExec{
		BaseExecutor: base,
		Statement:    v.Procedure,
		is:           b.is,
		done:         false,
	}
	return e
}

// Next implement stored procedures
func (e *ProcedureExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if e.done {
		return nil
	}
	// implicit commit
	if e.autoNewTxn() {
		if err = sessiontxn.NewTxnInStmt(ctx, e.Ctx()); err != nil {
			return err
		}
		defer func() { e.Ctx().GetSessionVars().SetInTxn(false) }()
	}
	switch x := e.Statement.(type) {
	case *ast.CreateProcedureInfo:
		err = e.createProcedure(ctx, x)
	case *ast.DropProcedureStmt:
		err = e.dropProcedure(ctx, x)
	case *ast.CallStmt:
		err = e.callProcedure(ctx, x)
	case *ast.AlterProcedureStmt:
		err = e.alterProcedure(ctx, x)
	}

	e.done = true
	return err
}

// procedureExistsInternal Query whether the stored procedure exists.
func procedureExistsInternal(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, name string, db string) (string, bool, error) {
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, `SELECT Definer FROM %n.%n WHERE route_schema=%? AND name=%? AND type= 'PROCEDURE' FOR UPDATE;`, mysql.SystemDB, mysql.Routines, db, name)
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

// getProcedureinfo read stored procedure content.
func getProcedureinfo(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, name string, db string) (*plannercore.ProcedurebodyInfo, error) {
	sql := new(strings.Builder)
	//heads []string{"Procedure", "sql_mode", "Create Procedure", "character_set_client", "collation_connection", "Database Collation"}
	sqlescape.MustFormatSQL(sql, "select name, sql_mode, definition_utf8, parameter_str, character_set_client, connection_collation,")
	sqlescape.MustFormatSQL(sql, "schema_collation, comment, security_type, definer from %n.%n where route_schema = %?  and name = %? and type = 'PROCEDURE' ", mysql.SystemDB, mysql.Routines, db, name)

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
		return nil, exeerrors.ErrSpDoesNotExist.GenWithStackByArgs("PROCEDURE", db+"."+name)
	}
	if len(rows) != 1 {
		return nil, errors.New("Multiple stored procedures found in table " + mysql.Routines)
	}
	procedurebodyInfo := &plannercore.ProcedurebodyInfo{}
	procedurebodyInfo.Name = rows[0].GetString(0)
	procedurebodyInfo.Procedurebody = " CREATE "
	if len(rows[0].GetString(9)) != 0 {
		strs := strings.Split(rows[0].GetString(9), "@")
		if len(strs) != 2 {
			return nil, errors.Errorf("get definer `%s` failed, the definer must be the form of `user`@`host`", rows[0].GetString(9))

		}
		procedurebodyInfo.Procedurebody = procedurebodyInfo.Procedurebody + "DEFINER=" + "`" + strs[0] + "`@`" + strs[1] + "` "
	}
	procedurebodyInfo.Procedurebody = procedurebodyInfo.Procedurebody + "PROCEDURE `" + rows[0].GetString(0) + "`(" + rows[0].GetString(3) + ")\n"
	if len(rows[0].GetString(7)) != 0 {
		procedurebodyInfo.Procedurebody = procedurebodyInfo.Procedurebody + "COMMENT '" + rows[0].GetString(7) + "' \n"
	}
	if rows[0].GetEnum(8).String() == "INVOKER" {
		procedurebodyInfo.Procedurebody = procedurebodyInfo.Procedurebody + "SQL SECURITY INVOKER \n"
	}
	procedurebodyInfo.Procedurebody = procedurebodyInfo.Procedurebody + rows[0].GetString(2)
	procedurebodyInfo.SQLMode = rows[0].GetSet(1).String()
	procedurebodyInfo.CharacterSetClient = rows[0].GetString(4)
	procedurebodyInfo.CollationConnection = rows[0].GetString(5)
	procedurebodyInfo.ShemaCollation = rows[0].GetString(6)
	return procedurebodyInfo, nil
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

// createProcedure Save stored procedure content.
func (e *ProcedureExec) createProcedure(ctx context.Context, s *ast.CreateProcedureInfo) error {
	e.Ctx().GetSessionVars().SetInCallProcedure()
	defer func() {
		e.Ctx().GetSessionVars().OutCallProcedure(false)
	}()

	procedurceName := s.ProcedureName.Name.String()
	procedurceSchema := s.ProcedureName.Schema
	dbInfo, ok := e.is.SchemaByName(procedurceSchema)
	if !ok {
		return exeerrors.ErrBadDB.GenWithStackByArgs(procedurceSchema)
	}

	var comment string
	security := "DEFINER"
	for _, characteristic := range s.Characteristics {
		switch x := characteristic.(type) {
		case *ast.ProcedureComment:
			comment = x.Comment
			if utf8.RuneCountInString(comment) > MaxRoutineComment {
				return exeerrors.ErrTooLongRoutineComment.GenWithStackByArgs(comment, MaxRoutineComment)
			}
		case *ast.ProcedureSecurity:
			security = x.Security.String()
		default:
			errors.Errorf("Unsupported procedure characteristic type %T", characteristic)
		}
	}
	parameterStr := s.ProcedureParamStr
	bodyStr := s.ProcedureBody.Text()

	sqlMod, ok := e.Ctx().GetSessionVars().GetSystemVar(variable.SQLModeVar)
	if !ok {
		return errors.New("unknown system var " + variable.SQLModeVar)
	}
	chs, ok := e.Ctx().GetSessionVars().GetSystemVar(variable.CharacterSetClient)
	if !ok {
		return errors.New("unknown system var " + variable.CharacterSetClient)
	}
	var userInfo string
	//get create user info.
	if e.Ctx().GetSessionVars().User != nil {
		u := e.Ctx().GetSessionVars().User.AuthUsername
		h := e.Ctx().GetSessionVars().User.AuthHostname
		if !s.Definer.CurrentUser && (s.Definer.Username != u || s.Definer.Hostname != h) {
			checker := privilege.GetPrivilegeManager(e.Ctx())
			if checker != nil && !checker.RequestVerification(e.Ctx().GetSessionVars().ActiveRoles, "", "", "", mysql.SuperPriv) {
				return exeerrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER")
			}
			userInfo = s.Definer.Username + "@" + s.Definer.Hostname
		} else {
			userInfo = u + "@" + h
		}
	}
	_, sessionCollation := e.Ctx().GetSessionVars().GetCharsetInfo()
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, "insert into mysql.routines (route_schema, name, type, definition, definition_utf8, parameter_str,")
	sqlescape.MustFormatSQL(sql, "is_deterministic, sql_data_access, security_type, definer, sql_mode, character_set_client, connection_collation, schema_collation, created, last_altered, comment, ")
	sqlescape.MustFormatSQL(sql, " external_language) values (%?, %?, 'PROCEDURE', %?, %?, %?, 1, 'CONTAINS SQL', %?, %?, %?, %?, %?, %?, now(6), now(6),  %?, 'SQL') ;", procedurceSchema.String(), procedurceName,
		bodyStr, bodyStr, parameterStr, security, userInfo, sqlMod, chs, sessionCollation, dbInfo.Collate, comment)
	sysSession, err := e.GetSysSession()
	if err != nil {
		return err
	}
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnProcedure)
	defer e.ReleaseSysSession(internalCtx, sysSession)
	sqlExecutor := sysSession.(sqlexec.SQLExecutor)
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "BEGIN PESSIMISTIC"); err != nil {
		return err
	}
	// check if procedure exists.
	_, exists, err := procedureExistsInternal(internalCtx, sqlExecutor, procedurceName, procedurceSchema.L)
	if err != nil {
		return err
	}
	if exists {
		err = exeerrors.ErrSpAlreadyExists.GenWithStackByArgs("PROCEDURE", procedurceName)
		if s.IfNotExists {
			e.Ctx().GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}
	// insert procedure.
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
		return err
	}
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "commit"); err != nil {
		return err
	}
	if e.Ctx().GetSessionVars().User != nil && variable.AutomaticSPPrivileges.Load() && e.checkPriv(procedurceSchema.L, procedurceName) {
		sql.Reset()
		sqlescape.MustFormatSQL(sql, "GRANT EXECUTE, ALTER ROUTINE ON PROCEDURE %n.%n TO %?@%?",
			procedurceSchema.String(), procedurceName, e.Ctx().GetSessionVars().User.AuthUsername, e.Ctx().GetSessionVars().User.AuthHostname)
		if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			return err
		}
	}
	return nil
}

func (e *ProcedureExec) checkPriv(procedurceSchema string, ProcedureName string) bool {
	currentUser := e.Ctx().GetSessionVars().User
	checker := privilege.GetPrivilegeManager(e.Ctx())
	activeRoles := e.Ctx().GetSessionVars().ActiveRoles
	// check if need grant priv
	if currentUser != nil {
		if checker != nil && !checker.RequestProcedureVerification(activeRoles, procedurceSchema, ProcedureName, mysql.AlterRoutinePriv|mysql.ExecutePriv) {
			return true
		}
	}
	return false
}

// fetchShowCreateProcdure query stored procedure.
func (e *ShowExec) fetchShowCreateProcdure(ctx context.Context) error {
	if e.Procedure.Schema.O == "" {
		e.Procedure.Schema = model.NewCIStr(e.Ctx().GetSessionVars().CurrentDB)
	}
	_, ok := e.is.SchemaByName(e.Procedure.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(e.DBName.O)
	}
	internalCtx := kv.WithInternalSourceType(ctx, kv.InternalTxnProcedure)
	sqlExecutor := e.Ctx().(sqlexec.SQLExecutor)
	procedureInfo, err := getProcedureinfo(internalCtx, sqlExecutor, e.Procedure.Name.String(), e.Procedure.Schema.O)
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

// dropProcedure delete stored procedure.
func (e *ProcedureExec) dropProcedure(ctx context.Context, s *ast.DropProcedureStmt) error {
	procedurceName := s.ProcedureName.Name.String()
	procedurceSchema := s.ProcedureName.Schema
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnProcedure)
	sysSession, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(internalCtx, sysSession)
	sqlExecutor := sysSession.(sqlexec.SQLExecutor)

	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "BEGIN PESSIMISTIC"); err != nil {
		return err
	}
	definer, exists, err := procedureExistsInternal(internalCtx, sqlExecutor, procedurceName, procedurceSchema.String())
	if err != nil {
		return err
	}
	if !exists {
		err = exeerrors.ErrSpDoesNotExist.GenWithStackByArgs("PROCEDURE", procedurceSchema.O+"."+procedurceName)
		if s.IfExists {
			e.Ctx().GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}
	err = e.setDefiner(definer)
	if err != nil {
		return err
	}
	if e.Ctx().GetSessionVars().User != nil && definer != "" {
		u := e.Ctx().GetSessionVars().User.AuthUsername
		h := e.Ctx().GetSessionVars().User.AuthHostname
		if e.definerUser != u || e.definerHost != h {
			checker := privilege.GetPrivilegeManager(e.Ctx())
			if checker != nil && !checker.RequestVerification(e.Ctx().GetSessionVars().ActiveRoles, "", "", "", mysql.SuperPriv) {
				return exeerrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER")
			}
		}
	}
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, "delete from %n.%n where route_schema = %?  and name = %? and type = 'PROCEDURE' ", mysql.SystemDB,
		mysql.Routines, procedurceSchema.String(), procedurceName)
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
		return err
	}
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "commit"); err != nil {
		return err
	}
	return nil
}

func (e *ProcedureExec) alterProcedure(ctx context.Context, s *ast.AlterProcedureStmt) error {
	procedurceName := s.ProcedureName.Name.String()
	procedurceSchema := s.ProcedureName.Schema
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnProcedure)
	sysSession, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(internalCtx, sysSession)
	sqlExecutor := sysSession.(sqlexec.SQLExecutor)
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "BEGIN PESSIMISTIC"); err != nil {
		return err
	}
	// check if procedure exists.
	definer, exists, err := procedureExistsInternal(internalCtx, sqlExecutor, procedurceName, procedurceSchema.L)
	if err != nil {
		return err
	}
	if !exists {
		err = exeerrors.ErrSpDoesNotExist.GenWithStackByArgs("PROCEDURE", procedurceSchema.O+"."+procedurceName)
		return err
	}
	err = e.setDefiner(definer)
	if err != nil {
		return err
	}
	if e.Ctx().GetSessionVars().User != nil && definer != "" {
		u := e.Ctx().GetSessionVars().User.AuthUsername
		h := e.Ctx().GetSessionVars().User.AuthHostname
		if e.definerUser != u || e.definerHost != h {
			checker := privilege.GetPrivilegeManager(e.Ctx())
			if checker != nil && !checker.RequestVerification(e.Ctx().GetSessionVars().ActiveRoles, "", "", "", mysql.SuperPriv) {
				return exeerrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER")
			}
		}
	}
	type alterField struct {
		expr  string
		value any
	}
	var fields []*alterField
	for _, characteristic := range s.Characteristics {
		switch x := characteristic.(type) {
		case *ast.ProcedureComment:
			if utf8.RuneCountInString(x.Comment) > MaxRoutineComment {
				return exeerrors.ErrTooLongRoutineComment.GenWithStackByArgs(x.Comment, MaxRoutineComment)
			}
			fields = append(fields, &alterField{"comment = %? ", x.Comment})
		case *ast.ProcedureSecurity:
			fields = append(fields, &alterField{"security_type = %? ", x.Security.String()})
		default:
			errors.Errorf("Unsupported procedure characteristic type %T", characteristic)
		}
	}
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, "update %n.%n set last_altered = now(6) ", mysql.SystemDB, mysql.Routines)
	for _, field := range fields {
		sqlescape.MustFormatSQL(sql, ",")
		sqlescape.MustFormatSQL(sql, field.expr, field.value)
	}

	sqlescape.MustFormatSQL(sql, " where route_schema = %?  and name = %? and type = 'PROCEDURE' ", procedurceSchema.String(), procedurceName)
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
		return err
	}
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "commit"); err != nil {
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
		procedurePlan:       v.Plan.ProcedureExecPlan,
		cachedProcedurePlan: v.CachedProcedurePlan,
		IsStrict:            v.IsStrictMode,
		definerHost:         v.Plan.DefinerHost,
		definerUser:         v.Plan.DefinerUser,
		securityType:        securityType,
	}
	return e
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
		switch s.Procedure.Args[i].(type) {
		// input variable is @name
		case *ast.VariableExpr:
			name := s.Procedure.Args[i].(*ast.VariableExpr).Name
			name = strings.ToLower(name)
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
		// to do: support pseudo-variable in BEFORE trigger
		default:
			// out variable must @name
			if (param.ParamType == ast.MODE_OUT) || (param.ParamType == ast.MODE_INOUT) {
				return exeerrors.ErrSpNotVarArg.GenWithStackByArgs(i+1, s.Procedure.Schema.String()+"."+s.Procedure.FnName.String())
			}
			if e.Ctx().GetSessionVars().InOtherCall() {
				e.Ctx().GetSessionVars().SetProcedureContext(e.parentContext)
				datum, err := plannercore.GetExprValue(ctx, plannercore.NewCacheExpr(true, plannercore.ExprNodeToString(s.Procedure.Args[i]), nil),
					param.DeclType, e.Ctx(), param.DeclName)
				if err != nil {
					return err
				}
				e.Ctx().GetSessionVars().SetProcedureContext(e.procedurePlan.ProcedureCtx)
				err = plannercore.UpdateVariableVar(param.DeclName, datum, e.Ctx().GetSessionVars())
				if err != nil {
					return err
				}
			} else {
				datum, err := plannercore.GetExprValue(ctx, plannercore.NewCacheExpr(true, plannercore.ExprNodeToString(s.Procedure.Args[i]), nil),
					param.DeclType, e.Ctx(), param.DeclName)
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

// outParams handle out parameters.
func (e *ProcedureExec) outParams() error {
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
func (e *ProcedureExec) callProcedure(ctx context.Context, s *ast.CallStmt) (err error) {
	//The last FinishExecuteStmt is expected to be a call statement.
	originalSQL := e.Ctx().GetSessionVars().StmtCtx.OriginalSQL
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
				return exeerrors.ErrNoSuchUser.GenWithStackByArgs(e.definerUser, e.definerHost)
			}
			if !pm.GetAuthWithoutVerification(e.definerUser, e.definerHost) {
				return exeerrors.ErrNoSuchUser.GenWithStackByArgs(e.definerUser, e.definerHost)
			}
			e.Ctx().GetSessionVars().User = definer
			e.Ctx().GetSessionVars().ActiveRoles = pm.GetDefaultRoles(e.definerUser, e.definerHost)
		} else {
			e.Ctx().GetSessionVars().User = nil
			e.Ctx().GetSessionVars().ActiveRoles = nil
		}
	}
	if e.cachedProcedurePlan.MyRecursionLevel > e.Ctx().GetSessionVars().MaxSpRecursionDepth {
		return exeerrors.ErrSpRecursionLimit.FastGenByArgs(e.Ctx().GetSessionVars().MaxSpRecursionDepth, s.Procedure.FnName.String())
	}
	err = plannercore.CallPrepareCheck(ctx, e.procedurePlan.ProcedureCommandList, e.Ctx())
	if err != nil {
		return err
	}
	e.cachedProcedurePlan.MyRecursionLevel++
	oldStr := e.Ctx().GetSessionVars().LastProcedureErrorStr
	defer func() {
		e.cachedProcedurePlan.MyRecursionLevel--
		e.Ctx().GetSessionVars().ClientCapability = clientCapabilitySave
		e.Ctx().GetSessionVars().LastProcedureErrorStr = oldStr
		// set dashboard display
		if err == nil {
			// In order to clean up the display cache (sql, execution plan, hash value, etc.) rebuild the StatementContext
			// StatementContext some structures use sync.Once unable to update.
			sc := stmtctx.NewStmtCtx()
			sc.OriginalSQL = originalSQL
			e.Ctx().GetSessionVars().StmtCtx.CopyMuForCallProcedure(sc)
			e.Ctx().GetSessionVars().StmtCtx = sc
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
		e.Ctx().GetSessionVars().OutCallProcedure(err != nil)
	}()
	var val int
	// sp use routines database as default database.
	sysvar := variable.GetSysVar(variable.LowerCaseTableNames)
	val, err = strconv.Atoi(sysvar.Value)
	if err != nil {
		return err
	}
	is := schematracker.NewSchemaTracker(val)
	key := is.InfoStore.CiStr2Key(s.Procedure.Schema)
	if key != e.Ctx().GetSessionVars().CurrentDB {
		oldDB := e.Ctx().GetSessionVars().CurrentDB
		defer func() {
			e.Ctx().GetSessionVars().CurrentDB = oldDB
		}()
		e.Ctx().GetSessionVars().CurrentDB = key
	}

	// handle in\out variable.
	err = e.callParam(ctx, s)
	if err != nil {
		return err
	}
	sqlModeSave, ok := e.Ctx().GetSessionVars().GetSystemVar(variable.SQLModeVar)
	if !ok {
		return errors.New("can not find sql_mode")
	}
	sqlMode, err := mysql.GetSQLMode(e.ProcedureSQLMod)
	if err != nil {
		return errors.Trace(err)
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
		return err
	}
	// use sql_mode defined in creating procedure.
	plannercore.ResetCallStatus(e.Ctx())
	e.cache = make([]plannercore.NeedCloseCur, 0, 10)
	// actually execute the stored procedure.
	err = e.executeCall(ctx)
	e.cache = plannercore.ReleseAll(e.cache)
	if err != nil {
		return err
	}
	err = e.outParams()
	if err != nil {
		return err
	}
	return nil
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
		e.cache, err = e.procedurePlan.ProcedureCommandList[ip].Execute(ctx, e.Ctx(), &ip, e.cache)
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
