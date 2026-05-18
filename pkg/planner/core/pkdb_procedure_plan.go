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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/charset"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/hint"
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

func (b *PlanBuilder) ensureRoutineSchema(name *ast.TableName) (string, error) {
	routineSchema := name.Schema.O
	if routineSchema == "" {
		routineSchema = b.ctx.GetSessionVars().CurrentDB
		name.Schema = pmodel.NewCIStr(routineSchema)
	}
	if routineSchema == "" {
		return "", plannererrors.ErrNoDB
	}
	return routineSchema, nil
}

func (b *PlanBuilder) preprocessCreateProcedure(ctx context.Context, node *ast.CreateProcedureInfo) error {
	// CREATE FUNCTION ... SONAME ... (loadable UDF) has no body / params and is stored in mysql.func.
	// The regular stored routine validation path (buildCallBodyPlan, schema checks, etc.) doesn't apply.
	if node.FunctionInfo.IsLoadable {
		currentUser := b.ctx.GetSessionVars().User
		checker := privilege.GetPrivilegeManager(b.ctx)
		activeRoles := b.ctx.GetSessionVars().ActiveRoles
		if currentUser != nil {
			// Align with MySQL: creating loadable UDF requires INSERT privilege on mysql.func.
			if checker != nil && !checker.RequestVerification(activeRoles, mysql.SystemDB, "func", "", mysql.InsertPriv) {
				return exeerrors.ErrTableaccessDenied.GenWithStackByArgs("INSERT", currentUser.AuthUsername, currentUser.AuthHostname, "func")
			}
		}
		return checkRoutineName(node.ProcedureName.Name.O)
	}

	routineSchema, err := b.ensureRoutineSchema(node.ProcedureName)
	if err != nil {
		return err
	}
	if node.Definer != nil {
		if len(node.Definer.Username) > auth.UserNameMaxLength {
			return exeerrors.ErrWrongStringLength.GenWithStackByArgs(node.Definer.Username, "user name", auth.UserNameMaxLength)
		}
		if len(node.Definer.Hostname) > auth.HostNameMaxLength {
			return exeerrors.ErrWrongStringLength.GenWithStackByArgs(node.Definer.Hostname, "host name", auth.HostNameMaxLength)
		}
	}
	currentUser := b.ctx.GetSessionVars().User
	checker := privilege.GetPrivilegeManager(b.ctx)
	activeRoles := b.ctx.GetSessionVars().ActiveRoles
	if currentUser != nil {
		if checker != nil && !checker.RequestProcedureVerification(activeRoles, routineSchema, node.ProcedureName.Name.O, mysql.CreateRoutinePriv) {
			return exeerrors.ErrDBaccessDenied.FastGenByArgs(currentUser.AuthUsername, currentUser.AuthHostname, routineSchema)
		}
	}
	if err := checkRoutineName(node.ProcedureName.Name.O); err != nil {
		return err
	}
	_, collate := b.ctx.GetSessionVars().GetCharsetInfo()
	if err := b.prepareCallParam(ctx, node, collate); err != nil {
		return err
	}

	// Check stored procedure legality.
	return b.buildCallBodyPlan(ctx, node, collate)
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

func (b *PlanBuilder) preprocessDropProcedure(node *ast.DropProcedureStmt) error {
	routineName := node.Name.Name.O
	if err := checkRoutineName(routineName); err != nil {
		return err
	}

	currentUser := b.ctx.GetSessionVars().User
	checker := privilege.GetPrivilegeManager(b.ctx)
	activeRoles := b.ctx.GetSessionVars().ActiveRoles

	checkDropStoredRoutinePrivilege := func(routineSchema string) error {
		if currentUser != nil {
			if checker != nil && !checker.RequestProcedureVerification(activeRoles, routineSchema, routineName, mysql.AlterRoutinePriv) {
				return plannererrors.ErrProcaccessDenied.FastGenByArgs("alter routine", currentUser.AuthUsername, currentUser.AuthHostname, routineSchema+"."+routineName)
			}
		}
		return nil
	}

	checkDropLoadableFunctionPrivilege := func() error {
		if currentUser != nil {
			// Align with MySQL: dropping loadable UDF requires DELETE privilege on mysql.func.
			if checker != nil && !checker.RequestVerification(activeRoles, mysql.SystemDB, "func", "", mysql.DeletePriv) {
				return exeerrors.ErrTableaccessDenied.GenWithStackByArgs("DELETE", currentUser.AuthUsername, currentUser.AuthHostname, "func")
			}
		}
		return nil
	}

	// DROP FUNCTION can target either:
	// 1) stored routine FUNCTION (mysql.routines), or
	// 2) loadable UDF (mysql.func).
	//
	// Keep the schema part untouched for the unqualified DROP FUNCTION case, so the executor can
	// decide the target consistently (see ddl.executor.DropProcedure).
		if node.IsFunction {
			routineSchema := node.Name.Schema.O
			// Qualified name always targets stored routine.
			if routineSchema == "" {
				currentDB := b.ctx.GetSessionVars().CurrentDB
				if currentDB != "" {
					// If a stored function exists in current DB, treat it as stored routine; otherwise fall back to UDF.
					exists, err := b.checkRoutineExists(currentDB, node.Name.Name.O, node.Type())
					if err != nil {
						return err
					}
					if exists {
						routineSchema = currentDB
					}
				}
			}

		// Unqualified & no routine found => loadable UDF (mysql.func).
		if routineSchema == "" {
			return checkDropLoadableFunctionPrivilege()
		}

		// Stored routine FUNCTION: require ALTER ROUTINE privilege.
		return checkDropStoredRoutinePrivilege(routineSchema)
	}

	// DROP PROCEDURE only targets stored routine PROCEDURE, so it requires a schema (current DB if omitted).
	routineSchema, err := b.ensureRoutineSchema(node.Name)
	if err != nil {
		return err
	}
	return checkDropStoredRoutinePrivilege(routineSchema)
}

func (b *PlanBuilder) checkRoutineExists(db, name, tp string) (bool, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBindInfo)
	restrictedCtx, err := b.getSysSession()
	if err != nil {
		return false, err
	}
	defer b.releaseSysSession(ctx, restrictedCtx)

	exec := restrictedCtx.(sqlexec.SQLExecutor)
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, "select 1 from %n.%n where route_schema = %? and name = %? and type = %? limit 1", mysql.SystemDB, mysql.Routines, db, name, tp)
	rs, err := exec.ExecuteInternal(ctx, sql.String())
	if err != nil {
		return false, err
	}
	if rs == nil {
		return false, nil
	}
	defer terror.Call(rs.Close)
	rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
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
	if b.procedureNowContext != nil {
		procedureCon = b.procedureNowContext
	}
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

// DeepCopyForExecution clones a procedure plan for an independent execution, including the
// underlying ProcedureCtx and command list structure.
func (procPlan *ProcedurePlan) DeepCopyForExecution() *ProcedurePlan {
	if procPlan == nil {
		return nil
	}
	return procPlan.deepCopy()
}

func routineCacheKey(schema, name pmodel.CIStr) string {
	return schema.L + "." + name.L
}

func (b *PlanBuilder) savePlanToCache(routineSchema, routineName pmodel.CIStr, plan *ProcedurePlan, lastChange types.Time) (*RoutineCacahe, error) {
	key := routineCacheKey(routineSchema, routineName)
	newPlan := plan.deepCopy()
	cachePlan := &RoutineCacahe{ProcedurePlan: *newPlan, MyRecursionLevel: 0, cacaheTime: lastChange}
	b.ctx.GetSessionVars().ProcedurePlanCache[key] = cachePlan
	return cachePlan, nil
}

func (b *PlanBuilder) getplanCache(ctx context.Context, routineSchema, routineName pmodel.CIStr, lastChange types.Time) (*ProcedurePlan, *RoutineCacahe, error) {
	key := routineCacheKey(routineSchema, routineName)
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

func (b *PlanBuilder) tryBuildProcedureInstant(ctx context.Context, node ast.Node) (base.Plan, error) {
	if !b.ctx.GetSessionVars().StmtCtx.TriggerCtx.InTrigger {
		return nil, nil
	}
	v, ok := node.(ast.StmtNode)
	if !ok {
		return nil, nil
	}
	// TODO(trigger): filter out non-procedure statements.
	collate, ok := b.ctx.GetSessionVars().GetSystemVar(variable.CollationConnection)
	if !ok {
		collate = mysql.DefaultCollationName
	}
	b.procedurePlan.initProcedureExec()
	err := b.procedureNodePlan(ctx, v, collate)
	if err != nil {
		return nil, errors.Trace(err)
	}

	plan := &ProcedurePlan{}
	plan.ProcedureExecPlan = b.procedurePlan
	return &TriggerProcedure{Plan: plan}, nil
}

// buildCallProcedure Generate call command execution plan.
func (b *PlanBuilder) buildCallProcedure(ctx context.Context, node *ast.CallStmt, skipPlanCache bool) (outplan base.Plan, err error) {
	p := &CallStmt{Callstmt: node, Is: b.is}
	// get database name.
	procedurceSchema := node.Procedure.Schema.O
	if procedurceSchema == "" {
		procedurceSchema = b.ctx.GetSessionVars().CurrentDB
		node.Procedure.Schema = pmodel.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
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
	tp := "PROCEDURE"
	if node.IsFunction {
		tp = "FUNCTION"
	}
	procedureInfo, err := b.fetchProcdureInfo(procedurceName, procedurceSchema, tp)
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
	defer b.ctx.GetSessionVars().OutCallProcedure(false)
	var (
		plan          *ProcedurePlan
		routineCacahe *RoutineCacahe
	)
	if !skipPlanCache {
		plan, routineCacahe, err = b.getplanCache(ctx, node.Procedure.Schema, node.Procedure.FnName, procedureInfo.lastChangeTime)
		if err != nil {
			return nil, err
		}
	}
	if plan == nil {
		plan, err = b.makePlanForCallProcedure(ctx, procedureInfo, sqlModeSave, node)
		if err != nil {
			return nil, err
		}
		routineCacahe, err = b.savePlanToCache(node.Procedure.Schema, node.Procedure.FnName, plan, procedureInfo.lastChangeTime)
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

func getCallStmt4StoredFuncExpr(
	ctx context.Context,
	sctx sessionctx.Context,
	node *ast.CallStmt,
) (any, error) {
	originPlannerSelectBlockAsName := sctx.GetSessionVars().PlannerSelectBlockAsName.Load()
	defer sctx.GetSessionVars().PlannerSelectBlockAsName.Store(originPlannerSelectBlockAsName)

	b := NewPlanBuilder()
	b, _ = b.Init(sctx.GetPlanCtx(), sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema(), hint.NewQBHintHandler(nil))
	if pc := sctx.GetSessionVars().GetProcedureContext(); pc != nil {
		pc2 := variable.NewProcedureContext(variable.BLOCKLABEL)
		pc2.SetRoot(pc.Context)
		b.procedureNowContext = pc2
	}
	p, err := b.buildCallProcedure(ctx, node, true)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func init() {
	expression.GetCallStmt4StoredFuncExpr = getCallStmt4StoredFuncExpr
}

// fetchProcdureInfo read the system table to get the stored procedure structure.
func (b *PlanBuilder) fetchProcdureInfo(name, db, tp string) (*ProcedurebodyInfo, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBindInfo)
	restrictedCtx, err := b.getSysSession()
	if err != nil {
		return nil, err
	}
	defer b.releaseSysSession(ctx, restrictedCtx)
	exec := restrictedCtx.(sqlexec.SQLExecutor)
	sql := new(strings.Builder)
	dbLookup := pmodel.NewCIStr(db).L
	//names = []string{"Procedure", "sql_mode", "Create Procedure", "character_set_client", "collation_connection", "Database Collation"}
	sqlescape.MustFormatSQL(sql, "select name, sql_mode ,definition_utf8, parameter_str, character_set_client, connection_collation,")
	sqlescape.MustFormatSQL(sql, "schema_collation, definer, security_type, last_altered from %n.%n where lower(route_schema) = %?  and name = %? and type = %? ", mysql.SystemDB, mysql.Routines, dbLookup, name, tp)
	rs, err := exec.ExecuteInternal(ctx, sql.String())
	if rs == nil {
		return nil, plannererrors.ErrSpDoesNotExist.GenWithStackByArgs(tp, db+"."+name)
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
		return nil, plannererrors.ErrSpDoesNotExist.GenWithStackByArgs(tp, db+"."+name)
	}
	if len(rows) != 1 {
		return nil, errors.New("Multiple stored procedures found in table " + mysql.Routines)
	}
	procedureBodyInfo := &ProcedurebodyInfo{}
	procedureBodyInfo.Name = rows[0].GetString(0)
	procedureBodyInfo.Procedurebody = " CREATE " + tp + " " + rows[0].GetString(0) + "(" + rows[0].GetString(3) + ") \n" + rows[0].GetString(2)
	procedureBodyInfo.SQLMode = rows[0].GetSet(1).String()
	procedureBodyInfo.CharacterSetClient = rows[0].GetString(4)
	procedureBodyInfo.CollationConnection = rows[0].GetString(5)
	procedureBodyInfo.ShemaCollation = rows[0].GetString(6)
	definer := rows[0].GetString(7)
	if len(definer) != 0 {
		// Split on the last '@' to handle usernames containing '@'
		lastAtIdx := strings.LastIndex(definer, "@")
		if lastAtIdx == -1 {
			return nil, errors.Errorf("get definer:%s error", definer)
		}
		procedureBodyInfo.DefinerUser = definer[:lastAtIdx]
		procedureBodyInfo.DefinerHost = definer[lastAtIdx+1:]
	}
	procedureBodyInfo.SecurityType = rows[0].GetEnum(8).String()
	procedureBodyInfo.lastChangeTime = rows[0].GetTime(9)
	return procedureBodyInfo, nil
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
	if tp.GetType() == mysql.TypeNewDecimal && tp.GetDecimal() == types.UnspecifiedLength {
		if tp.GetFlen() == 0 || tp.GetFlen() == types.UnspecifiedLength {
			defaultFlen, _ := mysql.GetDefaultFieldLengthAndDecimal(tp.GetType())
			tp.SetFlen(defaultFlen)
		}
		tp.SetDecimal(0)
		return tp, nil
	}
	if useDefaultLengthForRoutineNumeric(tp) {
		defaultFlen, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(tp.GetType())
		tp.SetFlen(defaultFlen)
		tp.SetDecimal(defaultDecimal)
		return tp, nil
	}
	if tp.GetFlen() != types.UnspecifiedLength || tp.GetDecimal() != types.UnspecifiedLength {
		return tp, nil
	}
	defaultFlen, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(tp.GetType())
	tp.SetFlen(defaultFlen)
	tp.SetDecimal(defaultDecimal)
	return tp, nil
}

func useDefaultLengthForRoutineNumeric(tp *types.FieldType) bool {
	switch tp.GetType() {
	case mysql.TypeNewDecimal:
		return tp.GetFlen() == 0 && (tp.GetDecimal() == 0 || tp.GetDecimal() == types.UnspecifiedLength)
	case mysql.TypeFloat, mysql.TypeDouble:
		return tp.GetFlen() == 0 && tp.GetDecimal() == types.UnspecifiedLength
	default:
		return false
	}
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

func (b *PlanBuilder) preprocessAlterProcedure(node *ast.AlterProcedureStmt) error {
	if err := checkRoutineName(node.ProcedureName.Name.O); err != nil {
		return err
	}
	routineSchema, err := b.ensureRoutineSchema(node.ProcedureName)
	if err != nil {
		return err
	}
	// Check if database exists
	_, ok := b.is.SchemaByName(node.ProcedureName.Schema)
	if !ok {
		return plannererrors.ErrBadDB.GenWithStackByArgs(routineSchema)
	}
	currentUser := b.ctx.GetSessionVars().User
	checker := privilege.GetPrivilegeManager(b.ctx)
	activeRoles := b.ctx.GetSessionVars().ActiveRoles
	// User with SuperPriv can see all rows.
	if currentUser != nil {
		if checker != nil && !checker.RequestProcedureVerification(activeRoles, routineSchema, node.ProcedureName.Name.O, mysql.AlterRoutinePriv) {
			return plannererrors.ErrProcaccessDenied.FastGenByArgs("alter routine", currentUser.AuthUsername, currentUser.AuthHostname, routineSchema+"."+node.ProcedureName.Name.O)
		}
	}
	return nil
}
