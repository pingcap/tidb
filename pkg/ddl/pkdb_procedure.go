// Copyright 2025 PingCAP, Inc.

package ddl

import (
	"context"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
)

const maxRoutineCommentLen = 65535

/// DDL Executor part.

func (e *executor) CreateProcedure(ctx sessionctx.Context, stmt *ast.CreateProcedureInfo) error {
	is := sessiontxn.GetTxnManager(ctx).GetTxnInfoSchema()
	if stmt.FunctionInfo.IsLoadable {
		return errors.New("CREATE FUNCTION SONAME is unimplemented")
	}

	procName := stmt.ProcedureName.Name
	procSchema := stmt.ProcedureName.Schema
	dbInfo, ok := is.SchemaByName(procSchema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(procSchema)
	}

	var comment string
	securityType := "DEFINER"
	for _, characteristic := range stmt.Characteristics {
		switch x := characteristic.(type) {
		case *ast.ProcedureComment:
			comment = x.Comment
			if utf8.RuneCountInString(comment) > maxRoutineCommentLen {
				return exeerrors.ErrTooLongRoutineComment.GenWithStackByArgs(comment, maxRoutineCommentLen)
			}
		case *ast.ProcedureSecurity:
			securityType = x.Security.String()
		default:
			_ = errors.Errorf("unsupported procedure characteristic type %T", characteristic)
		}
	}

	var parameterStr strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &parameterStr)
	for i, p := range stmt.ProcedureParam {
		if i > 0 {
			restoreCtx.WritePlain(", ")
		}
		if err := p.Restore(restoreCtx); err != nil {
			return err
		}
	}

	bodyStr := stmt.ProcedureBody.Text()
	routineType := "PROCEDURE"
	if stmt.FunctionInfo.RetType != nil {
		routineType = "FUNCTION"
		bodyStr = "RETURNS " + stmt.FunctionInfo.RetType.String() + " " + bodyStr
	}

	sqlMode, ok := ctx.GetSessionVars().GetSystemVar(variable.SQLModeVar)
	if !ok {
		return errors.New("unknown system var " + variable.SQLModeVar)
	}
	charsetClient, ok := ctx.GetSessionVars().GetSystemVar(variable.CharacterSetClient)
	if !ok {
		return errors.New("unknown system var " + variable.CharacterSetClient)
	}
	_, collConn := ctx.GetSessionVars().GetCharsetInfo()

	var definer string
	if ctx.GetSessionVars().User != nil {
		u := ctx.GetSessionVars().User.AuthUsername
		h := ctx.GetSessionVars().User.AuthHostname
		if !stmt.Definer.CurrentUser && (stmt.Definer.Username != u || stmt.Definer.Hostname != h) {
			checker := privilege.GetPrivilegeManager(ctx)
			if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, "", "", "", mysql.SuperPriv) {
				return exeerrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER")
			}
			definer = stmt.Definer.Username + "@" + stmt.Definer.Hostname
		} else {
			definer = u + "@" + h
		}
	}

	procInfo := &model.ProcedureInfo{
		Schema: procSchema,
		Name:   procName,
		Type:   routineType,

		Definition:     bodyStr,
		DefinitionUTF8: bodyStr,
		ParameterStr:   parameterStr.String(),

		IsDeterministic: 1,
		SQLDataAccess:   "CONTAINS SQL",
		SecurityType:    securityType,
		Definer:         definer,
		SQLMode:         sqlMode,

		CharacterSetClient:  charsetClient,
		CollationConnection: collConn,
		SchemaCollation:     dbInfo.Collate,

		Comment:          comment,
		ExternalLanguage: "SQL",
		State:            model.StateNone,
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       dbInfo.ID,
		SchemaName:     procSchema.L,
		TableName:      procName.L,
		Type:           model.ActionCreateProcedure,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Database: procSchema.L,
			Table:    procName.L,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	args := &model.CreateProcedureArgs{ProcedureInfo: procInfo}
	if err := e.doDDLJob2(ctx, job, args); err != nil {
		if stmt.IfNotExists && exeerrors.ErrSpAlreadyExists.Equal(err) {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return errors.Trace(err)
	}

	if ctx.GetSessionVars().User != nil && variable.AutomaticSPPrivileges.Load() {
		checker := privilege.GetPrivilegeManager(ctx)
		if checker != nil && !checker.RequestProcedureVerification(
			ctx.GetSessionVars().ActiveRoles,
			procSchema.L,
			procName.O,
			mysql.AlterRoutinePriv|mysql.ExecutePriv,
		) {
			var grantRoutineType string
			switch routineType {
			case "PROCEDURE":
				grantRoutineType = "PROCEDURE"
			case "FUNCTION":
				grantRoutineType = "FUNCTION"
			default:
				return errors.Errorf("unsupported routine type %q", routineType)
			}

			internalCtx := kv.WithInternalSourceType(e.ctx, kv.InternalTxnProcedure)
			if _, _, err := ctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(
				internalCtx,
				nil,
				"GRANT EXECUTE, ALTER ROUTINE ON "+grantRoutineType+" %n.%n TO %?@%?",
				procSchema.String(),
				procName.String(),
				ctx.GetSessionVars().User.AuthUsername,
				ctx.GetSessionVars().User.AuthHostname,
			); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (e *executor) DropProcedure(ctx sessionctx.Context, stmt *ast.DropProcedureStmt) error {
	if stmt.IsFunction {
		return errors.New("DROP FUNCTION is unimplemented")
	}

	routineSchema := stmt.Name.Schema
	routineName := stmt.Name.Name
	definer, exists, err := getRoutineDefiner(e.ctx, ctx, routineSchema.O, routineName.O, stmt.Type())
	if err != nil {
		return err
	}

	if !exists {
		err = exeerrors.ErrSpDoesNotExist.GenWithStackByArgs(stmt.Type(), routineSchema.O+"."+routineName.O)
		if stmt.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}
	if err := checkRoutineDefinerPrivilege(ctx, definer); err != nil {
		return err
	}

	is := sessiontxn.GetTxnManager(ctx).GetTxnInfoSchema()
	dbInfo, ok := is.SchemaByName(routineSchema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(routineSchema)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       dbInfo.ID,
		SchemaName:     routineSchema.L,
		TableName:      routineName.L,
		Type:           model.ActionDropProcedure,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Database: routineSchema.L,
			Table:    routineName.L,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	args := &model.DropProcedureArgs{
		Schema:   routineSchema,
		Name:     routineName,
		Type:     stmt.Type(),
		IfExists: stmt.IfExists,
	}
	if err := e.doDDLJob2(ctx, job, args); err != nil {
		if stmt.IfExists && exeerrors.ErrSpDoesNotExist.Equal(err) {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return errors.Trace(err)
	}
	return nil
}

func (e *executor) dropLoadableFunction(ctx sessionctx.Context, stmt *ast.DropProcedureStmt) error {
	return errors.New("DROP FUNCTION is unimplemented")
}

func (e *executor) AlterProcedure(ctx sessionctx.Context, stmt *ast.AlterProcedureStmt) error {
	routineSchema := stmt.ProcedureName.Schema
	routineName := stmt.ProcedureName.Name
	routineType := "PROCEDURE"
	if stmt.IsFunction {
		routineType = "FUNCTION"
	}

	definer, exists, err := getRoutineDefiner(e.ctx, ctx, routineSchema.O, routineName.O, routineType)
	if err != nil {
		return err
	}
	if !exists {
		return exeerrors.ErrSpDoesNotExist.GenWithStackByArgs(routineType, routineSchema.O+"."+routineName.O)
	}
	if err := checkRoutineDefinerPrivilege(ctx, definer); err != nil {
		return err
	}

	var (
		comment      *string
		securityType *string
	)
	for _, characteristic := range stmt.Characteristics {
		switch x := characteristic.(type) {
		case *ast.ProcedureComment:
			if utf8.RuneCountInString(x.Comment) > maxRoutineCommentLen {
				return exeerrors.ErrTooLongRoutineComment.GenWithStackByArgs(x.Comment, maxRoutineCommentLen)
			}
			v := x.Comment
			comment = &v
		case *ast.ProcedureSecurity:
			v := x.Security.String()
			securityType = &v
		default:
			_ = errors.Errorf("unsupported %s characteristic type %T", strings.ToLower(routineType), characteristic)
		}
	}

	is := sessiontxn.GetTxnManager(ctx).GetTxnInfoSchema()
	dbInfo, ok := is.SchemaByName(routineSchema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(routineSchema)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       dbInfo.ID,
		SchemaName:     routineSchema.L,
		TableName:      routineName.L,
		Type:           model.ActionAlterProcedure,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Database: routineSchema.L,
			Table:    routineName.L,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	args := &model.AlterProcedureArgs{
		Schema:       routineSchema,
		Name:         routineName,
		Type:         routineType,
		Comment:      comment,
		SecurityType: securityType,
	}
	return errors.Trace(e.doDDLJob2(ctx, job, args))
}

func getRoutineDefiner(
	execCtx context.Context,
	sctx sessionctx.Context,
	schemaName string,
	routineName string,
	routineType string,
) (definer string, exists bool, err error) {
	internalCtx := kv.WithInternalSourceType(execCtx, kv.InternalTxnProcedure)
	rows, _, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(
		internalCtx,
		nil,
		"SELECT definer FROM %n.%n WHERE route_schema=%? AND name=%? AND type=%? LIMIT 1",
		mysql.SystemDB,
		mysql.Routines,
		schemaName,
		routineName,
		routineType,
	)
	if err != nil {
		return "", false, err
	}
	if len(rows) == 0 {
		return "", false, nil
	}
	return rows[0].GetString(0), true, nil
}

func checkRoutineDefinerPrivilege(ctx sessionctx.Context, definer string) error {
	if ctx.GetSessionVars().User == nil || definer == "" {
		return nil
	}
	// Split on the last '@' to handle usernames containing '@'
	lastAtIdx := strings.LastIndex(definer, "@")
	if lastAtIdx == -1 {
		return errors.Errorf("get definer:%s error", definer)
	}
	user := definer[:lastAtIdx]
	host := definer[lastAtIdx+1:]

	u := ctx.GetSessionVars().User.AuthUsername
	h := ctx.GetSessionVars().User.AuthHostname
	if user != u || host != h {
		checker := privilege.GetPrivilegeManager(ctx)
		if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, "", "", "", mysql.SuperPriv) {
			return exeerrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER")
		}
	}
	return nil
}

/// DDL worker part.

func (w *worker) onCreateProcedure(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetCreateProcedureArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	if args.LoadableFunctionInfo != nil {
		job.State = model.JobStateCancelled
		return ver, errors.New("CREATE FUNCTION SONAME is unimplemented")
	}
	if args.ProcedureInfo == nil {
		job.State = model.JobStateCancelled
		return ver, errors.New("missing procedure info")
	}

	procInfo := args.ProcedureInfo
	procInfo.State = model.StateNone
	internalCtx := kv.WithInternalSourceType(jobCtx.stepCtx, kv.InternalTxnProcedure)

	// Check if routine exists.
	existsRows, err := w.sess.Execute(
		internalCtx,
		"SELECT definer FROM %n.%n WHERE route_schema=%? AND name=%? AND type=%? LIMIT 1",
		"create-procedure-check-exists",
		mysql.SystemDB,
		mysql.Routines,
		procInfo.Schema.O,
		procInfo.Name.O,
		procInfo.Type,
	)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if len(existsRows) > 0 {
		job.State = model.JobStateCancelled
		return ver, exeerrors.ErrSpAlreadyExists.GenWithStackByArgs(procInfo.Type, procInfo.Name.O)
	}

	// Insert routine.
	_, err = w.sess.Execute(
		internalCtx,
		`INSERT INTO %n.%n (
			route_schema, name, type, definition, definition_utf8, parameter_str,
			is_deterministic, sql_data_access, security_type, definer, sql_mode,
			character_set_client, connection_collation, schema_collation,
			created, last_altered, comment, external_language
		) VALUES (
			%?, %?, %?, %?, %?, %?,
			%?, %?, %?, %?, %?,
			%?, %?, %?,
			now(6), now(6), %?, %?
		)`,
		"create-procedure-insert",
		mysql.SystemDB,
		mysql.Routines,
		procInfo.Schema.O,
		procInfo.Name.O,
		procInfo.Type,
		procInfo.Definition,
		procInfo.DefinitionUTF8,
		procInfo.ParameterStr,
		procInfo.IsDeterministic,
		procInfo.SQLDataAccess,
		procInfo.SecurityType,
		procInfo.Definer,
		procInfo.SQLMode,
		procInfo.CharacterSetClient,
		procInfo.CollationConnection,
		procInfo.SchemaCollation,
		procInfo.Comment,
		procInfo.ExternalLanguage,
	)
	if err != nil {
		return ver, errors.Trace(err)
	}

	ver, err = updateSchemaVersion(jobCtx, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	job.State = model.JobStateDone
	job.SchemaState = model.StatePublic
	job.BinlogInfo.SchemaVersion = ver
	return ver, nil
}

func (w *worker) onCreateLoadableFunction(
	jobCtx *jobContext,
	job *model.Job,
	funcInfo *model.LoadableFunctionInfo,
) (ver int64, errRet error) {
	job.State = model.JobStateCancelled
	return ver, errors.New("CREATE FUNCTION SONAME is unimplemented")
}

func (w *worker) onDropProcedure(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetDropProcedureArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	if args.LoadableFunction {
		job.State = model.JobStateCancelled
		return ver, errors.New("DROP FUNCTION is unimplemented")
	}

	internalCtx := kv.WithInternalSourceType(jobCtx.stepCtx, kv.InternalTxnProcedure)
	_, err = w.sess.Execute(
		internalCtx,
		"DELETE FROM %n.%n WHERE route_schema=%? AND name=%? AND type=%?",
		"drop-procedure-delete",
		mysql.SystemDB,
		mysql.Routines,
		args.Schema.O,
		args.Name.O,
		args.Type,
	)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if w.sess.GetSessionVars().StmtCtx.AffectedRows() == 0 {
		if args.IfExists {
			job.State = model.JobStateDone
			job.SchemaState = model.StateNone
			return ver, nil
		}
		job.State = model.JobStateCancelled
		return ver, exeerrors.ErrSpDoesNotExist.GenWithStackByArgs(args.Type, args.Schema.O+"."+args.Name.O)
	}

	ver, err = updateSchemaVersion(jobCtx, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	job.State = model.JobStateDone
	job.SchemaState = model.StateNone
	job.BinlogInfo.SchemaVersion = ver
	return ver, nil
}

func (w *worker) onDropLoadableFunction(jobCtx *jobContext, job *model.Job, args *model.DropProcedureArgs) (ver int64, _ error) {
	job.State = model.JobStateCancelled
	return ver, errors.New("DROP FUNCTION is unimplemented")
}

func (w *worker) onAlterProcedure(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetAlterProcedureArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	internalCtx := kv.WithInternalSourceType(jobCtx.stepCtx, kv.InternalTxnProcedure)
	existsRows, err := w.sess.Execute(
		internalCtx,
		"SELECT 1 FROM %n.%n WHERE route_schema=%? AND name=%? AND type=%? LIMIT 1",
		"alter-procedure-check-exists",
		mysql.SystemDB,
		mysql.Routines,
		args.Schema.O,
		args.Name.O,
		args.Type,
	)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if len(existsRows) == 0 {
		job.State = model.JobStateCancelled
		return ver, exeerrors.ErrSpDoesNotExist.GenWithStackByArgs(args.Type, args.Schema.O+"."+args.Name.O)
	}

	updateSQL := strings.Builder{}
	updateSQL.WriteString("UPDATE %n.%n SET last_altered = now(6)")
	params := make([]any, 0, 7)
	params = append(params, mysql.SystemDB, mysql.Routines)
	if args.Comment != nil {
		updateSQL.WriteString(", comment = %?")
		params = append(params, *args.Comment)
	}
	if args.SecurityType != nil {
		updateSQL.WriteString(", security_type = %?")
		params = append(params, *args.SecurityType)
	}
	updateSQL.WriteString(" WHERE route_schema = %? AND name = %? AND type = %?")
	params = append(params, args.Schema.O, args.Name.O, args.Type)

	_, err = w.sess.Execute(
		internalCtx,
		updateSQL.String(),
		"alter-procedure-update",
		params...,
	)
	if err != nil {
		return ver, errors.Trace(err)
	}

	ver, err = updateSchemaVersion(jobCtx, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	job.State = model.JobStateDone
	job.SchemaState = model.StatePublic
	job.BinlogInfo.SchemaVersion = ver
	return ver, nil
}
