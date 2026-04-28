// Copyright 2025 PingCAP, Inc.

package ddl

import (
	"context"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/format"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
)

const maxRoutineCommentLen = 65535

func restoreRoutineCharacteristics(characteristics []ast.ProcedureCharacteristic) (*string, error) {
	var buf strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &buf)
	wrote := false
	for _, characteristic := range characteristics {
		if characteristic == nil {
			continue
		}
		if wrote {
			restoreCtx.WritePlain(" ")
		}
		if err := characteristic.Restore(restoreCtx); err != nil {
			return nil, err
		}
		wrote = true
	}
	if !wrote {
		return nil, nil
	}
	options := buf.String()
	return &options, nil
}

func extractRoutineCharacteristics(characteristics []ast.ProcedureCharacteristic) (comment, securityType string, isDeterministic int64, sqlDataAccess string, options *string, err error) {
	comment = ""
	securityType = "DEFINER"
	isDeterministic = 0
	sqlDataAccess = string(ast.RoutineContainsSQL)

	options, err = restoreRoutineCharacteristics(characteristics)
	if err != nil {
		return "", "", 0, "", nil, err
	}

	for _, characteristic := range characteristics {
		switch x := characteristic.(type) {
		case *ast.ProcedureComment:
			comment = x.Comment
			if utf8.RuneCountInString(comment) > maxRoutineCommentLen {
				return "", "", 0, "", nil, exeerrors.ErrTooLongRoutineComment.GenWithStackByArgs(comment, maxRoutineCommentLen)
			}
		case *ast.ProcedureSecurity:
			securityType = x.Security.String()
		case *ast.ProcedureDeterministic:
			if x.Deterministic {
				isDeterministic = 1
			} else {
				isDeterministic = 0
			}
		case *ast.ProcedureSQLDataAccess:
			sqlDataAccess = string(x.SQLDataAccess)
		case nil:
			continue
		default:
			_ = errors.Errorf("unsupported procedure characteristic type %T", characteristic)
		}
	}
	return comment, securityType, isDeterministic, sqlDataAccess, options, nil
}

func parseRoutineCharacteristics(options string) ([]ast.ProcedureCharacteristic, error) {
	if strings.TrimSpace(options) == "" {
		return nil, nil
	}
	p := parser.New()
	stmt, err := p.ParseOneStmt("create function p() returns int "+options+" return 1", "", "")
	if err != nil {
		return nil, err
	}
	createFn, ok := stmt.(*ast.CreateProcedureInfo)
	if !ok {
		return nil, errors.Errorf("unexpected statement type %T", stmt)
	}
	characteristics := make([]ast.ProcedureCharacteristic, 0, len(createFn.Characteristics))
	for _, characteristic := range createFn.Characteristics {
		if characteristic != nil {
			characteristics = append(characteristics, characteristic)
		}
	}
	return characteristics, nil
}

func withRoutineComment(characteristics []ast.ProcedureCharacteristic, comment string) []ast.ProcedureCharacteristic {
	out := make([]ast.ProcedureCharacteristic, 0, len(characteristics)+1)
	found := false
	for _, characteristic := range characteristics {
		if _, ok := characteristic.(*ast.ProcedureComment); ok {
			found = true
			if comment != "" {
				out = append(out, &ast.ProcedureComment{Type: ast.ProcedureCommentType, Comment: comment})
			}
			continue
		}
		out = append(out, characteristic)
	}
	if comment != "" && !found {
		out = append(out, &ast.ProcedureComment{Type: ast.ProcedureCommentType, Comment: comment})
	}
	return out
}

func withRoutineSecurity(characteristics []ast.ProcedureCharacteristic, securityType string) []ast.ProcedureCharacteristic {
	out := make([]ast.ProcedureCharacteristic, 0, len(characteristics)+1)
	found := false
	for _, characteristic := range characteristics {
		security, ok := characteristic.(*ast.ProcedureSecurity)
		if !ok {
			out = append(out, characteristic)
			continue
		}
		found = true
		if strings.EqualFold(securityType, "INVOKER") {
			out = append(out, &ast.ProcedureSecurity{Type: ast.ProcedureSecurityType, Security: pmodel.SecurityInvoker})
		}
		_ = security
	}
	if !found && strings.EqualFold(securityType, "INVOKER") {
		out = append(out, &ast.ProcedureSecurity{Type: ast.ProcedureSecurityType, Security: pmodel.SecurityInvoker})
	}
	return out
}

func withRoutineDeterministic(characteristics []ast.ProcedureCharacteristic, isDeterministic int64) []ast.ProcedureCharacteristic {
	out := make([]ast.ProcedureCharacteristic, 0, len(characteristics)+1)
	found := false
	for _, characteristic := range characteristics {
		if _, ok := characteristic.(*ast.ProcedureDeterministic); ok {
			found = true
			out = append(out, &ast.ProcedureDeterministic{
				Type:          ast.ProcedureDeterministicType,
				Deterministic: isDeterministic == 1,
			})
			continue
		}
		out = append(out, characteristic)
	}
	if !found {
		out = append(out, &ast.ProcedureDeterministic{
			Type:          ast.ProcedureDeterministicType,
			Deterministic: isDeterministic == 1,
		})
	}
	return out
}

func withRoutineSQLDataAccess(characteristics []ast.ProcedureCharacteristic, sqlDataAccess string) []ast.ProcedureCharacteristic {
	out := make([]ast.ProcedureCharacteristic, 0, len(characteristics)+1)
	found := false
	for _, characteristic := range characteristics {
		if _, ok := characteristic.(*ast.ProcedureSQLDataAccess); ok {
			found = true
			out = append(out, &ast.ProcedureSQLDataAccess{
				Type:          ast.ProcedureSQLDataAccessType,
				SQLDataAccess: ast.RoutineSQLDataAccess(sqlDataAccess),
			})
			continue
		}
		out = append(out, characteristic)
	}
	if !found {
		out = append(out, &ast.ProcedureSQLDataAccess{
			Type:          ast.ProcedureSQLDataAccessType,
			SQLDataAccess: ast.RoutineSQLDataAccess(sqlDataAccess),
		})
	}
	return out
}

/// DDL Executor part.

func (e *executor) CreateProcedure(ctx sessionctx.Context, stmt *ast.CreateProcedureInfo) error {
	is := sessiontxn.GetTxnManager(ctx).GetTxnInfoSchema()
	if stmt.FunctionInfo.IsLoadable {
		return dbterror.ErrNotSupportedYet.GenWithStackByArgs("CREATE FUNCTION ... SONAME")
	}

	if err := validateRoutineTypes(stmt); err != nil {
		return err
	}

	procName := stmt.ProcedureName.Name
	procSchema := stmt.ProcedureName.Schema
	dbInfo, ok := is.SchemaByName(procSchema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(procSchema)
	}

	comment, securityType, isDeterministic, sqlDataAccess, options, err := extractRoutineCharacteristics(stmt.Characteristics)
	if err != nil {
		return err
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
		retTypeStr, err := formatRoutineReturnType(stmt.FunctionInfo.RetType)
		if err != nil {
			return err
		}
		routineType = "FUNCTION"
		bodyStr = "RETURNS " + retTypeStr + " " + bodyStr
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

		IsDeterministic: isDeterministic,
		SQLDataAccess:   sqlDataAccess,
		SecurityType:    securityType,
		Definer:         definer,
		SQLMode:         sqlMode,

		CharacterSetClient:  charsetClient,
		CollationConnection: collConn,
		SchemaCollation:     dbInfo.Collate,

		Comment:          comment,
		Options:          options,
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

func formatRoutineReturnType(tp *types.FieldType) (string, error) {
	retType := tp.CompactStr()
	switch {
	case mysql.HasUnsignedFlag(tp.GetFlag()) && tp.GetType() != mysql.TypeBit && tp.GetType() != mysql.TypeYear:
		retType += " unsigned"
	}
	if mysql.HasZerofillFlag(tp.GetFlag()) {
		retType += " zerofill"
	}
	if mysql.HasBinaryFlag(tp.GetFlag()) && tp.GetType() != mysql.TypeString {
		retType += " binary"
	}
	return retType, nil
}

type routineTypeValidator struct {
	err error
}

func (v *routineTypeValidator) Enter(in ast.Node) (ast.Node, bool) {
	if v.err != nil {
		return in, true
	}
	switch x := in.(type) {
	case *ast.StoreParameter:
		v.err = validateRoutineFieldType(x.ParamName, x.ParamType)
	case *ast.ProcedureDecl:
		for _, name := range x.DeclNames {
			if v.err = validateRoutineFieldType(name, x.DeclType); v.err != nil {
				break
			}
		}
	}
	return in, v.err != nil
}

func (*routineTypeValidator) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func validateRoutineTypes(stmt *ast.CreateProcedureInfo) error {
	validator := &routineTypeValidator{}
	stmt.Accept(validator)
	if validator.err != nil {
		return validator.err
	}
	if stmt.FunctionInfo.RetType != nil {
		return validateRoutineFieldType("", stmt.FunctionInfo.RetType)
	}
	return nil
}

func validateRoutineFieldType(name string, tp *types.FieldType) error {
	if tp == nil {
		return nil
	}
	if tp.GetFlen() > math.MaxUint32 {
		return types.ErrTooBigDisplayWidth.GenWithStack("Display width out of range for column '%s' (max = %d)", name, math.MaxUint32)
	}

	switch tp.GetType() {
	case mysql.TypeString:
		if tp.GetFlen() != types.UnspecifiedLength && tp.GetFlen() > mysql.MaxFieldCharLength {
			return types.ErrTooBigFieldLength.GenWithStack("Column length too big for column '%s' (max = %d); use BLOB or TEXT instead", name, mysql.MaxFieldCharLength)
		}
	case mysql.TypeVarchar:
		if len(tp.GetCharset()) != 0 {
			if err := types.IsVarcharTooBigFieldLength(tp.GetFlen(), name, tp.GetCharset()); err != nil {
				return err
			}
		}
	case mysql.TypeFloat, mysql.TypeDouble:
		if tp.GetDecimal() == types.UnspecifiedLength {
			if tp.GetType() == mysql.TypeFloat && tp.GetFlen() > mysql.MaxDoublePrecisionLength {
				return types.ErrWrongFieldSpec.GenWithStackByArgs(name)
			}
		} else {
			if tp.GetDecimal() > mysql.MaxFloatingTypeScale {
				return types.ErrTooBigScale.GenWithStackByArgs(tp.GetDecimal(), name, mysql.MaxFloatingTypeScale)
			}
			if tp.GetFlen() > mysql.MaxFloatingTypeWidth || tp.GetFlen() == 0 {
				return types.ErrTooBigDisplayWidth.GenWithStackByArgs(name, mysql.MaxFloatingTypeWidth)
			}
			if tp.GetFlen() < tp.GetDecimal() {
				return types.ErrMBiggerThanD.GenWithStackByArgs(name)
			}
		}
	case mysql.TypeSet:
		if len(tp.GetElems()) > mysql.MaxTypeSetMembers {
			return types.ErrTooBigSet.GenWithStack("Too many strings for column %s and SET", name)
		}
		for _, str := range tp.GetElems() {
			if strings.Contains(str, ",") {
				return types.ErrIllegalValueForType.GenWithStackByArgs(types.TypeStr(tp.GetType()), str)
			}
		}
	case mysql.TypeNewDecimal:
		if tp.GetDecimal() > mysql.MaxDecimalScale {
			return types.ErrTooBigScale.GenWithStackByArgs(tp.GetDecimal(), name, mysql.MaxDecimalScale)
		}
		if tp.GetFlen() > mysql.MaxDecimalWidth {
			return types.ErrTooBigPrecision.GenWithStackByArgs(tp.GetFlen(), name, mysql.MaxDecimalWidth)
		}
		if tp.GetFlen() < tp.GetDecimal() {
			return types.ErrMBiggerThanD.GenWithStackByArgs(name)
		}
	case mysql.TypeBit:
		if tp.GetFlen() <= 0 {
			return types.ErrInvalidFieldSize.GenWithStackByArgs(name)
		}
		if tp.GetFlen() > mysql.MaxBitDisplayWidth {
			return types.ErrTooBigDisplayWidth.GenWithStackByArgs(name, mysql.MaxBitDisplayWidth)
		}
	}

	if err := checkColumnAttributes(name, tp); err != nil {
		return err
	}

	collation := tp.GetCollate()
	if collation == "" && tp.GetCharset() != "" {
		defaultCollation, err := charset.GetDefaultCollation(tp.GetCharset())
		if err != nil {
			return err
		}
		collation = defaultCollation
	}
	col := table.ToColumn(&model.ColumnInfo{
		Name:      pmodel.NewCIStr(name),
		FieldType: *tp.Clone(),
	})
	if err := checkColumnFieldLength(col); err != nil {
		return err
	}
	if collation == "" {
		collation = col.GetCollate()
	}
	if collation == "" {
		collation = mysql.DefaultCollationName
	}
	return checkColumnValueConstraint(col, collation)
}

func (e *executor) DropProcedure(ctx sessionctx.Context, stmt *ast.DropProcedureStmt) error {
	routineSchema := stmt.Name.Schema
	routineName := stmt.Name.Name

	var (
		definer string
		exists  bool
		err     error
	)
	if stmt.IsFunction && routineSchema.O == "" {
		currentDB := ctx.GetSessionVars().CurrentDB
		if currentDB == "" {
			if stmt.IfExists {
				udfExists, err := getLoadableFunctionExists(e.ctx, ctx, routineName.L)
				if err != nil {
					return err
				}
				if !udfExists {
					err = exeerrors.ErrSpDoesNotExist.GenWithStackByArgs("FUNCTION (UDF)", routineName.O)
					ctx.GetSessionVars().StmtCtx.AppendNote(err)
					return nil
				}
			}
			return e.dropLoadableFunction(ctx, stmt)
		}
		routineSchema = pmodel.NewCIStr(currentDB)
		definer, exists, err = getRoutineDefiner(e.ctx, ctx, routineSchema.O, routineName.O, stmt.Type())
		if err != nil {
			return err
		}
		if !exists {
			if stmt.IfExists {
				udfExists, err := getLoadableFunctionExists(e.ctx, ctx, routineName.L)
				if err != nil {
					return err
				}
				if !udfExists {
					err = exeerrors.ErrSpDoesNotExist.GenWithStackByArgs(stmt.Type(), routineSchema.O+"."+routineName.O)
					ctx.GetSessionVars().StmtCtx.AppendNote(err)
					return nil
				}
			}
			return e.dropLoadableFunction(ctx, stmt)
		}
	} else {
		definer, exists, err = getRoutineDefiner(e.ctx, ctx, routineSchema.O, routineName.O, stmt.Type())
		if err != nil {
			return err
		}
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

func (e *executor) dropLoadableFunction(sessionctx.Context, *ast.DropProcedureStmt) error {
	return dbterror.ErrNotSupportedYet.GenWithStackByArgs("DROP FUNCTION (UDF)")
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
		comment         *string
		securityType    *string
		isDeterministic *int64
		sqlDataAccess   *string
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
		case *ast.ProcedureDeterministic:
			v := int64(0)
			if x.Deterministic {
				v = 1
			}
			isDeterministic = &v
		case *ast.ProcedureSQLDataAccess:
			v := string(x.SQLDataAccess)
			sqlDataAccess = &v
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
		Schema:          routineSchema,
		Name:            routineName,
		Type:            routineType,
		Comment:         comment,
		SecurityType:    securityType,
		IsDeterministic: isDeterministic,
		SQLDataAccess:   sqlDataAccess,
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

func getLoadableFunctionExists(execCtx context.Context, sctx sessionctx.Context, funcName string) (bool, error) {
	internalCtx := kv.WithInternalSourceType(execCtx, kv.InternalTxnProcedure)
	rows, _, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(
		internalCtx,
		nil,
		"SELECT 1 FROM %n.%n WHERE name=%? AND type='function' LIMIT 1",
		mysql.SystemDB,
		"func",
		funcName,
	)
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
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
		return w.onCreateLoadableFunction(jobCtx, job, args.LoadableFunctionInfo)
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
	var options any
	if procInfo.Options != nil {
		options = *procInfo.Options
	}
	_, err = w.sess.Execute(
		internalCtx,
		`INSERT INTO %n.%n (
			route_schema, name, type, definition, definition_utf8, parameter_str,
			is_deterministic, sql_data_access, security_type, definer, sql_mode,
			character_set_client, connection_collation, schema_collation,
			created, last_altered, comment, options, external_language
		) VALUES (
			%?, %?, %?, %?, %?, %?,
			%?, %?, %?, %?, %?,
			%?, %?, %?,
			now(6), now(6), %?, %?, %?
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
		options,
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
	if funcInfo == nil {
		job.State = model.JobStateCancelled
		return ver, errors.New("missing loadable function info")
	}

	internalCtx := kv.WithInternalSourceType(jobCtx.stepCtx, kv.InternalTxnProcedure)
	existsRows, err := w.sess.Execute(
		internalCtx,
		"SELECT 1 FROM %n.%n WHERE name=%? LIMIT 1",
		"create-loadable-function-check-exists",
		mysql.SystemDB,
		"func",
		funcInfo.Name.L,
	)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if len(existsRows) > 0 {
		job.State = model.JobStateCancelled
		return ver, exeerrors.ErrUdfExists.FastGenByArgs(funcInfo.Name.O)
	}

	funcDef, err := expression.LoadUDF(funcInfo.SoName, funcInfo.Name.L, funcInfo.ReturnType)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	defer funcDef.Drop()

	if err := expression.ValidateLoadableFunctionDef(funcDef); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	// Only validate dlopen/dlsym + name collision here. The global registry is updated
	// during infoschema reload based on mysql.func after schema version is published.
	funcDef.Drop()

	failpoint.Inject("pauseAfterCreateLoadableFunctionRegistered", func() {})

	inserted := false
	defer func() {
		if errRet != nil && inserted {
			// Best-effort cleanup: when the DDL job is cancelled/failed after inserting the row,
			// ensure mysql.func does not keep a residue entry that may be loaded on later reload.
			_, _ = w.sess.Execute(
				internalCtx,
				"DELETE FROM %n.%n WHERE name=%? AND type='function'",
				"create-loadable-function-delete-on-error",
				mysql.SystemDB,
				"func",
				funcInfo.Name.L,
			)
		}
	}()

	_, err = w.sess.Execute(
		internalCtx,
		"INSERT INTO %n.%n (name, ret, dl, type) VALUES (%?, %?, %?, 'function')",
		"create-loadable-function-insert",
		mysql.SystemDB,
		"func",
		funcInfo.Name.L,
		expression.CastEvalTypeToUDFArgTypeInt(funcInfo.ReturnType),
		funcInfo.SoName,
	)
	if err != nil {
		return ver, errors.Trace(err)
	}
	inserted = true

	failpoint.Inject("failAfterCreateLoadableFunctionInserted", func(val failpoint.Value) {
		if val.(bool) {
			job.State = model.JobStateCancelled
			failpoint.Return(ver, errors.New("failpoint: failAfterCreateLoadableFunctionInserted"))
		}
	})

	ver, err = updateSchemaVersion(jobCtx, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	job.State = model.JobStateDone
	job.SchemaState = model.StatePublic
	job.BinlogInfo.SchemaVersion = ver
	return ver, nil
}

func (w *worker) onDropProcedure(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetDropProcedureArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	if args.LoadableFunction {
		return w.onDropLoadableFunction(jobCtx, job, args)
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

func (w *worker) onDropLoadableFunction(jobCtx *jobContext, job *model.Job, args *model.DropProcedureArgs) (ver int64, errRet error) {
	internalCtx := kv.WithInternalSourceType(jobCtx.stepCtx, kv.InternalTxnProcedure)
	existsRows, err := w.sess.Execute(
		internalCtx,
		"SELECT ret, dl, type FROM %n.%n WHERE name=%? LIMIT 1",
		"drop-loadable-function-check-exists",
		mysql.SystemDB,
		"func",
		args.Name.L,
	)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if len(existsRows) == 0 {
		job.State = model.JobStateCancelled
		return ver, exeerrors.ErrSpDoesNotExist.GenWithStackByArgs("FUNCTION (UDF)", args.Name.O)
	}
	ret := existsRows[0].GetInt64(0)
	dl := existsRows[0].GetString(1)
	funcType := existsRows[0].GetEnum(2).String()
	if funcType != "function" {
		job.State = model.JobStateCancelled
		return ver, exeerrors.ErrSpDoesNotExist.GenWithStackByArgs("FUNCTION (UDF)", args.Name.O)
	}

	deleted := false
	defer func() {
		if errRet != nil && deleted {
			// Best-effort cleanup: if the DDL job is cancelled/failed after deleting the row,
			// restore mysql.func so UDF visibility can still be driven by infoschema reload.
			_, _ = w.sess.Execute(
				internalCtx,
				"INSERT IGNORE INTO %n.%n (name, ret, dl, type) VALUES (%?, %?, %?, 'function')",
				"drop-loadable-function-restore-on-error",
				mysql.SystemDB,
				"func",
				args.Name.L,
				ret,
				dl,
			)
		}
	}()

	_, err = w.sess.Execute(
		internalCtx,
		"DELETE FROM %n.%n WHERE name=%? AND type='function'",
		"drop-loadable-function-delete",
		mysql.SystemDB,
		"func",
		args.Name.L,
	)
	if err != nil {
		return ver, errors.Trace(err)
	}
	deleted = true

	failpoint.Inject("pauseAfterDropLoadableFunctionRemoved", func() {})
	failpoint.Inject("failAfterDropLoadableFunctionRemoved", func(val failpoint.Value) {
		if val.(bool) {
			job.State = model.JobStateCancelled
			failpoint.Return(ver, errors.New("failpoint: failAfterDropLoadableFunctionRemoved"))
		}
	})

	ver, err = updateSchemaVersion(jobCtx, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	job.State = model.JobStateDone
	job.SchemaState = model.StateNone
	job.BinlogInfo.SchemaVersion = ver
	return ver, nil
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
		"SELECT options FROM %n.%n WHERE route_schema=%? AND name=%? AND type=%? LIMIT 1",
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

	updateOptions := args.Comment != nil || args.SecurityType != nil || args.IsDeterministic != nil || args.SQLDataAccess != nil
	var options *string
	if updateOptions {
		var characteristics []ast.ProcedureCharacteristic
		if !existsRows[0].IsNull(0) {
			characteristics, err = parseRoutineCharacteristics(existsRows[0].GetString(0))
			if err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
		}
		if args.Comment != nil {
			characteristics = withRoutineComment(characteristics, *args.Comment)
		}
		if args.SecurityType != nil {
			characteristics = withRoutineSecurity(characteristics, *args.SecurityType)
		}
		if args.IsDeterministic != nil {
			characteristics = withRoutineDeterministic(characteristics, *args.IsDeterministic)
		}
		if args.SQLDataAccess != nil {
			characteristics = withRoutineSQLDataAccess(characteristics, *args.SQLDataAccess)
		}
		options, err = restoreRoutineCharacteristics(characteristics)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}

	updateSQL := strings.Builder{}
	updateSQL.WriteString("UPDATE %n.%n SET last_altered = now(6)")
	params := make([]any, 0, 11)
	params = append(params, mysql.SystemDB, mysql.Routines)
	if args.Comment != nil {
		updateSQL.WriteString(", comment = %?")
		params = append(params, *args.Comment)
	}
	if args.SecurityType != nil {
		updateSQL.WriteString(", security_type = %?")
		params = append(params, *args.SecurityType)
	}
	if args.IsDeterministic != nil {
		updateSQL.WriteString(", is_deterministic = %?")
		params = append(params, *args.IsDeterministic)
	}
	if args.SQLDataAccess != nil {
		updateSQL.WriteString(", sql_data_access = %?")
		params = append(params, *args.SQLDataAccess)
	}
	if updateOptions {
		updateSQL.WriteString(", options = %?")
		if options == nil {
			params = append(params, nil)
		} else {
			params = append(params, *options)
		}
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
