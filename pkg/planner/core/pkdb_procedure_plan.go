// Copyright 2023 PingCAP, Inc.
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
	"strings"
	"unicode/utf8"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

const (
	// MaxNameCharLen indicates ident max len
	MaxNameCharLen = 64
)

// RoutineCacahe caches routine Execution Plan
type RoutineCacahe struct {
	ProcedurePlan
	MyRecursionLevel int
	cacaheTime       types.Time
}

// CallPrepareCheck Stored procedure pre-execution checks.
func CallPrepareCheck(ctx context.Context, commands []ProcedureExecPlan, sctx sessionctx.Context) error {
	for _, command := range commands {
		if base, ok := command.(*executeBaseSQL); ok {
			err := base.checkProcedureStatus(ctx, sctx.GetPlanCtx())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func checkRoutineName(name string) error {
	if len(name) == 0 || name[0] == 0 || name[len(name)-1] == ' ' {
		return plannererrors.ErrSpWrongName.GenWithStackByArgs(name)
	}

	if utf8.RuneCountInString(name) > MaxNameCharLen {
		return plannererrors.ErrTooLongIdent.GenWithStackByArgs(name)
	}
	return nil
}

// buildCreateProcedure Generate create stored procedure plan.
func (b *PlanBuilder) buildCreateProcedure(ctx context.Context, node *ast.CreateProcedureInfo) (base.Plan, error) {
	p := &CreateProcedure{CreateProcedureInfo: node, is: b.is}
	procedurceSchema := node.ProcedureName.Schema.O
	if procedurceSchema == "" {
		procedurceSchema = b.ctx.GetSessionVars().CurrentDB
		node.ProcedureName.Schema = model.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
	}
	if procedurceSchema == "" {
		return nil, plannererrors.ErrNoDB
	}
	if len(procedurceSchema) == 0 {
		return nil, dbterror.ErrWrongDBName.GenWithStackByArgs(procedurceSchema)
	}

	if len(node.Definer.Username) > auth.UserNameMaxLength {
		return nil, exeerrors.ErrWrongStringLength.GenWithStackByArgs(node.Definer.Username, "user name", auth.UserNameMaxLength)
	}
	if len(node.Definer.Hostname) > auth.HostNameMaxLength {
		return nil, exeerrors.ErrWrongStringLength.GenWithStackByArgs(node.Definer.Hostname, "host name", auth.HostNameMaxLength)
	}
	currentUser := b.ctx.GetSessionVars().User
	checker := privilege.GetPrivilegeManager(b.ctx)
	activeRoles := b.ctx.GetSessionVars().ActiveRoles
	if currentUser != nil {
		if checker != nil && !checker.RequestProcedureVerification(activeRoles, procedurceSchema, node.ProcedureName.Name.O, mysql.CreateRoutinePriv) {
			return nil, exeerrors.ErrDBaccessDenied.FastGenByArgs(currentUser.AuthUsername, currentUser.AuthHostname, procedurceSchema)
		}
	}
	err := checkRoutineName(node.ProcedureName.Name.O)
	if err != nil {
		return nil, err
	}
	_, collate := b.ctx.GetSessionVars().GetCharsetInfo()
	err = b.prepareCallParam(ctx, node, collate)
	if err != nil {
		return nil, err
	}

	// Check stored procedure legality.
	err = b.buildCallBodyPlan(ctx, node, collate)
	if err != nil {
		return nil, err
	}

	return p, nil
}

// prepareCallParam creates a root procedure context.
func (b *PlanBuilder) prepareCallParam(ctx context.Context, stmtNodes *ast.CreateProcedureInfo, collate string) error {
	var err error
	procedureCon := variable.NewProcedureContext(variable.BLOCKLABEL)
	b.procedureNowContext = procedureCon
	b.procedurePlan.ProcedureCtx = procedureCon
	for _, spParam := range stmtNodes.ProcedureParam {
		if utf8.RuneCountInString(spParam.ParamName) > MaxNameCharLen {
			return dbterror.ErrTooLongIdent.GenWithStackByArgs(spParam.ParamName)
		}
		spParam.ParamType, err = b.setDefaultLengthAndCharset(spParam.ParamType, collate)
		if err != nil {
			return err
		}
		spParam.ParamName = strings.ToLower(spParam.ParamName)
		if procedureCon.CheckVarName(spParam.ParamName) {
			return exeerrors.ErrSpDupParam.GenWithStackByArgs(spParam.ParamName)
		}
		vars := variable.NewProcedureVars(spParam.ParamName, spParam.ParamType)
		procedureCon.Vars = append(procedureCon.Vars, vars)
	}
	return nil
}

// buildDropProcedure Generate drop stored procedure plan.
func (b *PlanBuilder) buildDropProcedure(ctx context.Context, node *ast.DropProcedureStmt) (base.Plan, error) {
	p := &DropProcedure{Procedure: node}
	procedurceSchema := node.ProcedureName.Schema.O
	if procedurceSchema == "" {
		procedurceSchema = b.ctx.GetSessionVars().CurrentDB
		node.ProcedureName.Schema = model.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
	}
	if procedurceSchema == "" {
		return nil, plannererrors.ErrNoDB
	}
	if len(procedurceSchema) == 0 {
		return nil, dbterror.ErrWrongDBName.GenWithStackByArgs(procedurceSchema)
	}
	currentUser := b.ctx.GetSessionVars().User
	checker := privilege.GetPrivilegeManager(b.ctx)
	activeRoles := b.ctx.GetSessionVars().ActiveRoles
	// User with SuperPriv can see all rows.
	if currentUser != nil {
		if checker != nil && !checker.RequestProcedureVerification(activeRoles, procedurceSchema, node.ProcedureName.Name.O, mysql.AlterRoutinePriv) {
			return nil, plannererrors.ErrProcaccessDenied.FastGenByArgs("alter routine", currentUser.AuthUsername, currentUser.AuthHostname, procedurceSchema+"."+node.ProcedureName.Name.O)
		}
	}
	return p, nil
}

// ProcedureBaseBody base store procedure structure.
type ProcedureBaseBody any

// ProcedurebodyInfo Store stored procedure content read from the table.
type ProcedurebodyInfo struct {
	Name                string
	Procedurebody       string
	SQLMode             string
	CharacterSetClient  string
	CollationConnection string
	ShemaCollation      string
	DefinerUser         string
	DefinerHost         string
	SecurityType        string
	lastChangeTime      types.Time
}

// ProcedureParameterVal Store stored procedure parameter.
type ProcedureParameterVal struct {
	DeclName  string
	DeclType  *types.FieldType
	ParamType int
}

// ProcedurePlan store call plan.
type ProcedurePlan struct {
	IfNotExists       bool
	ProcedureName     *ast.TableName
	Procedurebody     ProcedureBaseBody
	ProcedureExecPlan ProcedureExec
	DefinerHost       string
	DefinerUser       string
	SecurityType      string
}

// IsSqlstateValid Sanity check for SQLSTATEs.
func IsSqlstateValid(sqlState string) bool {
	if len(sqlState) != 5 {
		return false
	}
	for _, c := range sqlState {
		if (c < '0' || '9' < c) && (c < 'A' || 'Z' < c) {
			return false
		}
	}
	return true
}

// IsSQLStateCompletion check if the specified SQL-state-string defines COMPLETION condition.
func IsSQLStateCompletion(sqlState string) bool {
	return sqlState[0] == '0' && sqlState[1] == '0'
}

// buildCallParamPlan Handles stored procedure parameters.
func (b *PlanBuilder) buildCallParamPlan(ctx context.Context, stmtNodes *ast.CreateProcedureInfo, node *ast.CallStmt, collate string) ([]*ProcedureParameterVal, error) {
	params := make([]*ProcedureParameterVal, 0, len(stmtNodes.ProcedureParam))
	procedureCon := variable.NewProcedureContext(variable.BLOCKLABEL)
	b.procedureNowContext = procedureCon
	b.procedurePlan.ProcedureCtx = procedureCon
	var err error
	for _, spParam := range stmtNodes.ProcedureParam {
		// Set the default length when no default length is setted.
		spParam.ParamType, err = b.setDefaultLengthAndCharset(spParam.ParamType, collate)
		if err != nil {
			return nil, err
		}
		if procedureCon.CheckVarName(spParam.ParamName) {
			return nil, plannererrors.ErrSpDupParam.GenWithStackByArgs(spParam.ParamName)
		}
		vars := variable.NewProcedureVars(spParam.ParamName, spParam.ParamType)
		procedureCon.Vars = append(procedureCon.Vars, vars)
		param := &ProcedureParameterVal{
			DeclName:  spParam.ParamName,
			DeclType:  spParam.ParamType,
			ParamType: spParam.Paramstatus,
		}
		params = append(params, param)
	}
	return params, nil
}

func (procPlan *ProcedurePlan) deepCopy() *ProcedurePlan {
	contextMap := make(map[*variable.ProcedureContext]*variable.ProcedureContext)
	newExec := ProcedureExec{ProcedureParam: procPlan.ProcedureExecPlan.ProcedureParam,
		Labels: procPlan.ProcedureExecPlan.Labels,
	}

	// copy variable.ProcedureContext data structure.
	newExec.ProcedureCtx = procPlan.ProcedureExecPlan.ProcedureCtx.CopyContext(contextMap)
	// Cache Correspondence between old and new context.
	contextMap[procPlan.ProcedureExecPlan.ProcedureCtx] = newExec.ProcedureCtx

	// Copy the new command list.
	newExec.ProcedureCommandList = make([]ProcedureExecPlan, 0, len(procPlan.ProcedureExecPlan.ProcedureCommandList))
	for _, command := range procPlan.ProcedureExecPlan.ProcedureCommandList {
		newExec.ProcedureCommandList = append(newExec.ProcedureCommandList, command.CloneStructure(contextMap))
	}
	return &ProcedurePlan{
		IfNotExists:       procPlan.IfNotExists,
		ProcedureName:     procPlan.ProcedureName,
		Procedurebody:     procPlan.Procedurebody,
		ProcedureExecPlan: newExec,
		DefinerHost:       procPlan.DefinerHost,
		DefinerUser:       procPlan.DefinerUser,
		SecurityType:      procPlan.SecurityType,
	}

}

func (b *PlanBuilder) savePlanToCache(routineName model.CIStr, plan *ProcedurePlan, lastChange types.Time) (*RoutineCacahe, error) {
	key := b.ctx.GetSessionVars().CurrentDB + "." + routineName.O
	newPlan := plan.deepCopy()
	cachePlan := &RoutineCacahe{ProcedurePlan: *newPlan, MyRecursionLevel: 0, cacaheTime: lastChange}
	b.ctx.GetSessionVars().ProcedurePlanCache[key] = cachePlan
	return cachePlan, nil
}

func (b *PlanBuilder) getplanCache(ctx context.Context, routineName model.CIStr, lastChange types.Time) (*ProcedurePlan, *RoutineCacahe, error) {
	key := b.ctx.GetSessionVars().CurrentDB + "." + routineName.O
	cache, ok := b.ctx.GetSessionVars().ProcedurePlanCache[key]
	if !ok {
		return nil, nil, nil
	}
	cachePlan, ok := cache.(*RoutineCacahe)
	if !ok {
		return nil, nil, errors.Errorf("routine cache unspport %T type", cachePlan)
	}

	// Treat stored procedure modification times as version numbers
	// Stored procedure cache expiration conditions:
	// 1. The modification time is inconsistent with the cache time. (lastChange.Compare(cachePlan.cacaheTime) != 0 )
	// 2. The current cache is not used (cachePlan.MyRecursionLevel ==0)
	if lastChange.Compare(cachePlan.cacaheTime) != 0 && cachePlan.MyRecursionLevel <= 0 {
		delete(b.ctx.GetSessionVars().ProcedurePlanCache, key)
		return nil, nil, nil
	}
	newPlan := cachePlan.deepCopy()
	return newPlan, cachePlan, nil
}

func (b *PlanBuilder) makePlanForCallProcedure(ctx context.Context, procedureInfo *ProcedurebodyInfo, sqlModeSave string, node *ast.CallStmt) (*ProcedurePlan, error) {
	b.procedurePlan.initProcedureExec()
	originSQLMode := b.ctx.GetSessionVars().SQLMode
	spSQLMode, err := mysql.GetSQLMode(procedureInfo.SQLMode)
	if err != nil {
		return nil, errors.Trace(err)
	}
	b.ctx.GetSessionVars().SQLMode = spSQLMode
	err = b.ctx.GetSessionVars().SetSystemVar(variable.SQLModeVar, procedureInfo.SQLMode)
	defer func() {
		restoreErr := b.ctx.GetSessionVars().SetSystemVar(variable.SQLModeVar, sqlModeSave)
		if restoreErr != nil {
			logutil.BgLogger().Error("call recover sql_mode failed. err:", zap.Error(restoreErr))
		}
		b.ctx.GetSessionVars().SQLMode = originSQLMode
	}()
	if err != nil {
		return nil, err
	}
	exec := b.ctx.(sqlexec.SQLParser)
	stmtNodes, _, err := exec.ParseSQL(ctx, procedureInfo.Procedurebody)
	if err != nil {
		return nil, err
	}
	// Generate call execution plan.
	plan, err := b.getCallPlan(ctx, stmtNodes, node, procedureInfo.CollationConnection)
	if err != nil {
		return nil, err
	}
	return plan, err
}

// buildCallProcedure Generate call command execution plan.
func (b *PlanBuilder) buildCallProcedure(ctx context.Context, node *ast.CallStmt) (outplan base.Plan, err error) {
	p := &CallStmt{Callstmt: node, Is: b.is}
	// get database name.
	procedurceSchema := node.Procedure.Schema.O
	if procedurceSchema == "" {
		procedurceSchema = b.ctx.GetSessionVars().CurrentDB
		node.Procedure.Schema = model.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
	}
	if procedurceSchema == "" {
		return nil, plannererrors.ErrNoDB
	}
	if len(procedurceSchema) == 0 {
		return nil, dbterror.ErrWrongDBName.GenWithStackByArgs(procedurceSchema)
	}
	// get stored procedure name.
	procedurceName := node.Procedure.FnName.String()
	// Check if database exists
	if utf8.RuneCountInString(procedurceName) > MaxNameCharLen {
		return nil, plannererrors.ErrTooLongIdent.GenWithStackByArgs(procedurceName)
	}

	_, ok := b.is.SchemaByName(node.Procedure.Schema)
	if !ok {
		return nil, plannererrors.ErrBadDB.GenWithStackByArgs(procedurceSchema)
	}

	currentUser := b.ctx.GetSessionVars().User
	checker := privilege.GetPrivilegeManager(b.ctx)
	activeRoles := b.ctx.GetSessionVars().ActiveRoles
	// User with SuperPriv can see all rows.
	if currentUser != nil {
		if checker != nil && !checker.RequestProcedureVerification(activeRoles, procedurceSchema, node.Procedure.FnName.O, mysql.ExecutePriv) {
			return nil, plannererrors.ErrProcaccessDenied.FastGenByArgs("execute", currentUser.AuthUsername, currentUser.AuthHostname, procedurceSchema+"."+node.Procedure.FnName.O)
		}
	}
	// get stored procedure structure.
	procedureInfo, err := b.fetchProcdureInfo(procedurceName, procedurceSchema)
	if err != nil {
		return nil, err
	}
	sqlModeSave, ok := b.ctx.GetSessionVars().GetSystemVar(variable.SQLModeVar)
	if !ok {
		return nil, errors.Errorf("can not find %s", variable.SQLModeVar)
	}
	p.ProcedureSQLMod = procedureInfo.SQLMode
	p.IsStrictMode = b.ctx.GetSessionVars().SQLMode.HasStrictMode()
	b.ctx.GetSessionVars().SetInCallProcedure()
	defer func() {
		if err != nil {
			// The current stored procedure is not actually executed and there is no actual backup StmtCtx.
			// No need to clean up
			b.ctx.GetSessionVars().OutCallProcedure(false)
		}
	}()
	plan, routineCacahe, err := b.getplanCache(ctx, node.Procedure.FnName, procedureInfo.lastChangeTime)
	if err != nil {
		return nil, err
	}
	if plan == nil {
		plan, err = b.makePlanForCallProcedure(ctx, procedureInfo, sqlModeSave, node)
		if err != nil {
			return nil, err
		}
		routineCacahe, err = b.savePlanToCache(node.Procedure.FnName, plan, procedureInfo.lastChangeTime)
		if err != nil {
			return nil, err
		}
	}

	plan.DefinerHost, plan.DefinerUser = procedureInfo.DefinerHost, procedureInfo.DefinerUser
	plan.SecurityType = procedureInfo.SecurityType
	p.Plan = plan
	p.CachedProcedurePlan = routineCacahe
	return p, nil
}

// fetchProcdureInfo read the system table to get the stored procedure structure.
func (b *PlanBuilder) fetchProcdureInfo(name, db string) (*ProcedurebodyInfo, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBindInfo)
	restrictedCtx, err := b.getSysSession()
	if err != nil {
		return nil, err
	}
	defer b.releaseSysSession(ctx, restrictedCtx)
	exec := restrictedCtx.(sqlexec.SQLExecutor)
	sql := new(strings.Builder)
	//names = []string{"Procedure", "sql_mode", "Create Procedure", "character_set_client", "collation_connection", "Database Collation"}
	sqlescape.MustFormatSQL(sql, "select name, sql_mode ,definition_utf8, parameter_str, character_set_client, connection_collation,")
	sqlescape.MustFormatSQL(sql, "schema_collation, definer, security_type, last_altered from %n.%n where route_schema = %?  and name = %? and type = 'PROCEDURE' ", mysql.SystemDB, mysql.Routines, db, name)
	rs, err := exec.ExecuteInternal(ctx, sql.String())
	if rs == nil {
		return nil, plannererrors.ErrSpDoesNotExist.GenWithStackByArgs("PROCEDURE", db+"."+name)
	}
	if err != nil {
		return nil, err
	}

	var rows []chunk.Row
	defer terror.Call(rs.Close)
	if rows, err = sqlexec.DrainRecordSet(ctx, rs, 8); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plannererrors.ErrSpDoesNotExist.GenWithStackByArgs("PROCEDURE", db+"."+name)
	}
	if len(rows) != 1 {
		return nil, errors.New("Multiple stored procedures found in table " + mysql.Routines)
	}
	procedurebodyInfo := &ProcedurebodyInfo{}
	procedurebodyInfo.Name = rows[0].GetString(0)
	procedurebodyInfo.Procedurebody = " CREATE PROCEDURE " + rows[0].GetString(0) + "(" + rows[0].GetString(3) + ") \n" + rows[0].GetString(2)
	procedurebodyInfo.SQLMode = rows[0].GetSet(1).String()
	procedurebodyInfo.CharacterSetClient = rows[0].GetString(4)
	procedurebodyInfo.CollationConnection = rows[0].GetString(5)
	procedurebodyInfo.ShemaCollation = rows[0].GetString(6)
	definer := rows[0].GetString(7)
	if len(definer) != 0 {
		strs := strings.Split(definer, "@")
		if len(strs) != 2 {
			return nil, errors.Errorf("get definer:%s error", definer)
		}
		procedurebodyInfo.DefinerUser, procedurebodyInfo.DefinerHost = strs[0], strs[1]
	}
	procedurebodyInfo.SecurityType = rows[0].GetEnum(8).String()
	procedurebodyInfo.lastChangeTime = rows[0].GetTime(9)
	return procedurebodyInfo, nil
}

// setDefaultLengthAndCharset set FieldType default len.
func (b *PlanBuilder) setDefaultLengthAndCharset(tp *types.FieldType, collate string) (*types.FieldType, error) {
	if tp.GetCollate() != "" && tp.GetCharset() == "" {
		return tp, plannererrors.ErrNotSupportedYet.FastGenByArgs("COLLATE with no CHARACTER SET in SP parameters, RETURNS, DECLARE")
	}
	if typesNeedCharset(tp.GetType()) {
		err := b.setFieldCharset(tp, collate)
		if err != nil {
			return tp, err
		}
	} else {
		tp.SetCharset(charset.CharsetBin)
		tp.SetCollate(charset.CharsetBin)
	}
	// If neither is UnspecifiedLength, it means that the user has specified the precision,
	// and the precision can be inherited
	if tp.GetFlen() != types.UnspecifiedLength || tp.GetDecimal() != types.UnspecifiedLength {
		switch tp.GetType() {
		case mysql.TypeFloat:
			tp1 := types.NewFieldType(mysql.TypeDouble)
			tp1.SetFlen(tp.GetFlen())
			tp1.SetDecimal(tp.GetDecimal())
			tp1.SetFlag(tp.GetFlag())
			tp = tp1
		}

		return tp, nil
	}
	defaultFlen, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(tp.GetType())
	switch tp.GetType() {
	// For uninitialized data types, the precision is initialized according to the data type
	// TypeDouble ==> (22,-1); TypeFloat ==> (12,-1)
	case mysql.TypeDouble, mysql.TypeFloat:
		tp1 := types.NewFieldType(mysql.TypeDouble)
		tp1.SetFlen(defaultFlen)
		tp1.SetDecimal(defaultDecimal)
		tp1.SetFlag(tp.GetFlag())
		tp = tp1

	default:
		tp.SetFlen(defaultFlen)
		tp.SetDecimal(defaultDecimal)

	}

	return tp, nil
}

// typesNeedCharset check if tp need charset.
func typesNeedCharset(tp byte) bool {
	switch tp {
	case mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob,
		mysql.TypeEnum, mysql.TypeSet:
		return true
	}
	return false
}

// setFieldCharset set charset.
// When both charset and collate are empty, read the session default value.
// When collate is empty and charset is specified, the default comparison set of charset is read.
// When charset is empty and collate is specified, an error will be reported in the previous process.
// When both collate and charset are specified, verify whether they match.
func (b *PlanBuilder) setFieldCharset(tp *types.FieldType, collate string) error {
	if tp.GetCharset() == "" && tp.GetCollate() == "" {
		// read default charset and collate.
		tp.SetCollate(collate)
		cs, err := charset.GetCollationByName(tp.GetCollate())
		if err != nil {
			return err
		}
		tp.SetCharset(cs.CharsetName)
		return nil
	}
	if tp.GetCharset() != "" {
		// Both collate and charset exist, check if match.
		if tp.GetCollate() != "" {
			collation, err := charset.GetCollationByName(tp.GetCollate())
			if err != nil {
				return err
			}
			if collation.CharsetName != tp.GetCharset() {
				return charset.ErrCollationCharsetMismatch.GenWithStackByArgs(tp.GetCollate(), tp.GetCharset())
			}
			return nil
		}
		// Collate is empty, using default collate.
		collation, err := charset.GetDefaultCollation(tp.GetCharset())
		if err != nil {
			return errors.Trace(err)
		}
		tp.SetCollate(collation)
	}
	return nil
}

// getSysSession get sySsession pool.
func (b *PlanBuilder) getSysSession() (sessionctx.Context, error) {
	dom := domain.GetDomain(b.ctx)
	sysSessionPool := dom.SysSessionPool()
	ctx, err := sysSessionPool.Get()
	if err != nil {
		return nil, err
	}
	restrictedCtx := ctx.(sessionctx.Context)
	restrictedCtx.GetSessionVars().InRestrictedSQL = true
	return restrictedCtx, nil
}

// releaseSysSession release sySsession pool.
func (b *PlanBuilder) releaseSysSession(ctx context.Context, sctx sessionctx.Context) {
	if sctx == nil {
		return
	}
	dom := domain.GetDomain(b.ctx)
	sysSessionPool := dom.SysSessionPool()
	if _, err := sctx.(sqlexec.SQLExecutor).ExecuteInternal(ctx, "rollback"); err != nil {
		sctx.(pools.Resource).Close()
		return
	}
	sysSessionPool.Put(sctx.(pools.Resource))
}

// buildAlterProcedure Generate alter Procedure execution plan.
func (b *PlanBuilder) buildAlterProcedure(ctx context.Context, node *ast.AlterProcedureStmt) (outplan base.Plan, err error) {
	p := &AlterProcedure{Procedure: node}
	// get database name.
	procedurceSchema := node.ProcedureName.Schema.O
	if procedurceSchema == "" {
		procedurceSchema = b.ctx.GetSessionVars().CurrentDB
		node.ProcedureName.Schema = model.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
	}
	if procedurceSchema == "" {
		return nil, plannererrors.ErrNoDB
	}
	// Check if database exists
	_, ok := b.is.SchemaByName(node.ProcedureName.Schema)
	if !ok {
		return nil, plannererrors.ErrBadDB.GenWithStackByArgs(procedurceSchema)
	}
	currentUser := b.ctx.GetSessionVars().User
	checker := privilege.GetPrivilegeManager(b.ctx)
	activeRoles := b.ctx.GetSessionVars().ActiveRoles
	// User with SuperPriv can see all rows.
	if currentUser != nil {
		if checker != nil && !checker.RequestProcedureVerification(activeRoles, procedurceSchema, node.ProcedureName.Name.O, mysql.AlterRoutinePriv) {
			return nil, plannererrors.ErrProcaccessDenied.FastGenByArgs("alter routine", currentUser.AuthUsername, currentUser.AuthHostname, procedurceSchema+"."+node.ProcedureName.Name.O)
		}
	}
	return p, nil
}
