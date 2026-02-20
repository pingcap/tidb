// Copyright 2016 PingCAP, Inc.
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
	"math"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	executor_metrics "github.com/pingcap/tidb/pkg/executor/metrics"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/plugin"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/globalconn"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	pwdValidator "github.com/pingcap/tidb/pkg/util/password-validation"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

const notSpecified = -1

// SimpleExec represents simple statement executor.
// For statements do simple execution.
// includes `UseStmt`, 'SetStmt`, `DoStmt`,
// `BeginStmt`, `CommitStmt`, `RollbackStmt`.
// TODO: list all simple statements.
type SimpleExec struct {
	exec.BaseExecutor

	Statement  ast.StmtNode
	ResolveCtx *resolve.Context
	// IsFromRemote indicates whether the statement IS FROM REMOTE TiDB instance in cluster,
	//   and executing in coprocessor.
	//   Used for `global kill`. See https://github.com/pingcap/tidb/blob/master/docs/design/2020-06-01-global-kill.md.
	IsFromRemote bool
	done         bool
	is           infoschema.InfoSchema

	// staleTxnStartTS is the StartTS that is used to execute the staleness txn during a read-only begin statement.
	staleTxnStartTS uint64
}

// resourceOptionsInfo represents the resource infomations to limit user.
// It contains 'MAX_QUERIES_PER_HOUR', 'MAX_UPDATES_PER_HOUR', 'MAX_CONNECTIONS_PER_HOUR' and 'MAX_USER_CONNECTIONS'.
// It only implements the option of 'MAX_USER_CONNECTIONS' now.
// To do: implement the other three options.
type resourceOptionsInfo struct {
	maxQueriesPerHour     int64
	maxUpdatesPerHour     int64
	maxConnectionsPerHour int64
	maxUserConnections    int64
}

type passwordOrLockOptionsInfo struct {
	lockAccount                 string
	passwordExpired             string
	passwordLifetime            any
	passwordHistory             int64
	passwordHistoryChange       bool
	passwordReuseInterval       int64
	passwordReuseIntervalChange bool
	failedLoginAttempts         int64
	passwordLockTime            int64
	failedLoginAttemptsChange   bool
	passwordLockTimeChange      bool
}

type passwordReuseInfo struct {
	passwordHistory       int64
	passwordReuseInterval int64
}

type userInfo struct {
	host       string
	user       string
	pLI        *passwordOrLockOptionsInfo
	pwd        string
	authString string
}

// Next implements the Executor Next interface.
func (e *SimpleExec) Next(ctx context.Context, _ *chunk.Chunk) (err error) {
	if e.done {
		return nil
	}

	if e.autoNewTxn() {
		// Commit the old transaction, like DDL.
		if err := sessiontxn.NewTxnInStmt(ctx, e.Ctx()); err != nil {
			return err
		}
		defer func() { e.Ctx().GetSessionVars().SetInTxn(false) }()
	}

	switch x := e.Statement.(type) {
	case *ast.GrantRoleStmt:
		err = e.executeGrantRole(ctx, x)
	case *ast.UseStmt:
		err = e.executeUse(x)
	case *ast.FlushStmt:
		err = e.executeFlush(ctx, x)
	case *ast.AlterInstanceStmt:
		err = e.executeAlterInstance(x)
	case *ast.BeginStmt:
		err = e.executeBegin(ctx, x)
	case *ast.CommitStmt:
		e.executeCommit()
	case *ast.SavepointStmt:
		err = e.executeSavepoint(x)
	case *ast.ReleaseSavepointStmt:
		err = e.executeReleaseSavepoint(x)
	case *ast.RollbackStmt:
		err = e.executeRollback(x)
	case *ast.CreateUserStmt:
		err = e.executeCreateUser(ctx, x)
	case *ast.AlterUserStmt:
		err = e.executeAlterUser(ctx, x)
	case *ast.DropUserStmt:
		err = e.executeDropUser(ctx, x)
	case *ast.RenameUserStmt:
		err = e.executeRenameUser(x)
	case *ast.SetPwdStmt:
		err = e.executeSetPwd(ctx, x)
	case *ast.SetSessionStatesStmt:
		err = e.executeSetSessionStates(ctx, x)
	case *ast.KillStmt:
		err = e.executeKillStmt(ctx, x)
	case *ast.RefreshStatsStmt:
		err = e.executeRefreshStats(ctx, x)
	case *ast.BinlogStmt:
		// We just ignore it.
		return nil
	case *ast.DropStatsStmt:
		err = e.executeDropStats(ctx, x)
	case *ast.SetRoleStmt:
		err = e.executeSetRole(ctx, x)
	case *ast.RevokeRoleStmt:
		err = e.executeRevokeRole(ctx, x)
	case *ast.SetDefaultRoleStmt:
		err = e.executeSetDefaultRole(ctx, x)
	case *ast.ShutdownStmt:
		err = e.executeShutdown()
	case *ast.AdminStmt:
		err = e.executeAdmin(x)
	case *ast.SetResourceGroupStmt:
		err = e.executeSetResourceGroupName(x)
	case *ast.AlterRangeStmt:
		err = e.executeAlterRange(x)
	case *ast.DropQueryWatchStmt:
		err = e.executeDropQueryWatch(x)
	}
	e.done = true
	return err
}

func (e *SimpleExec) dbAccessDenied(dbname string) error {
	user := e.Ctx().GetSessionVars().User
	u := user.Username
	h := user.Hostname
	if len(user.AuthUsername) > 0 && len(user.AuthHostname) > 0 {
		u = user.AuthUsername
		h = user.AuthHostname
	}
	return exeerrors.ErrDBaccessDenied.GenWithStackByArgs(u, h, dbname)
}

func (e *SimpleExec) executeUse(s *ast.UseStmt) error {
	dbname := ast.NewCIStr(s.DBName)

	checker := privilege.GetPrivilegeManager(e.Ctx())
	if checker != nil && e.Ctx().GetSessionVars().User != nil {
		if !checker.DBIsVisible(e.Ctx().GetSessionVars().ActiveRoles, dbname.String()) {
			return e.dbAccessDenied(dbname.O)
		}
	}

	dbinfo, exists := e.is.SchemaByName(dbname)
	if !exists {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbname)
	}
	e.Ctx().GetSessionVars().CurrentDBChanged = dbname.O != e.Ctx().GetSessionVars().CurrentDB
	e.Ctx().GetSessionVars().CurrentDB = dbname.O
	sessionVars := e.Ctx().GetSessionVars()
	dbCollate := dbinfo.Collate
	if dbCollate == "" {
		dbCollate = getDefaultCollate(dbinfo.Charset)
	}
	// If new collations are enabled, switch to the default
	// collation if this one is not supported.
	// The SetSystemVar will also update the CharsetDatabase
	dbCollate = collate.SubstituteMissingCollationToDefault(dbCollate)
	return sessionVars.SetSystemVarWithoutValidation(vardef.CollationDatabase, dbCollate)
}

func (e *SimpleExec) executeBegin(ctx context.Context, s *ast.BeginStmt) error {
	// If `START TRANSACTION READ ONLY` is the first statement in TxnCtx, we should
	// always create a new Txn instead of reusing it.
	if s.ReadOnly {
		noopFuncsMode := e.Ctx().GetSessionVars().NoopFuncsMode
		if s.AsOf == nil && noopFuncsMode != variable.OnInt {
			err := expression.ErrFunctionsNoopImpl.FastGenByArgs("READ ONLY")
			if noopFuncsMode == variable.OffInt {
				return errors.Trace(err)
			}
			e.Ctx().GetSessionVars().StmtCtx.AppendWarning(err)
		}
		if s.AsOf != nil {
			// start transaction read only as of failed due to we set tx_read_ts before
			if e.Ctx().GetSessionVars().TxnReadTS.PeakTxnReadTS() > 0 {
				return errors.New("start transaction read only as of is forbidden after set transaction read only as of")
			}
		}
	}

	return sessiontxn.GetTxnManager(e.Ctx()).EnterNewTxn(ctx, &sessiontxn.EnterNewTxnRequest{
		Type:                  sessiontxn.EnterNewTxnWithBeginStmt,
		TxnMode:               s.Mode,
		CausalConsistencyOnly: s.CausalConsistencyOnly,
		StaleReadTS:           e.staleTxnStartTS,
	})
}

// ErrSavepointNotSupportedWithBinlog export for testing.
var ErrSavepointNotSupportedWithBinlog = errors.New("SAVEPOINT is not supported when binlog is enabled")

func (e *SimpleExec) executeSavepoint(s *ast.SavepointStmt) error {
	sessVars := e.Ctx().GetSessionVars()
	txnCtx := sessVars.TxnCtx
	if !sessVars.InTxn() && sessVars.IsAutocommit() {
		return nil
	}
	if !sessVars.ConstraintCheckInPlacePessimistic && sessVars.TxnCtx.IsPessimistic {
		return errors.New("savepoint is not supported in pessimistic transactions when in-place constraint check is disabled")
	}
	txn, err := e.Ctx().Txn(true)
	if err != nil {
		return err
	}
	memDBCheckpoint := txn.GetMemDBCheckpoint()
	txnCtx.AddSavepoint(s.Name, memDBCheckpoint)
	return nil
}

func (e *SimpleExec) executeReleaseSavepoint(s *ast.ReleaseSavepointStmt) error {
	deleted := e.Ctx().GetSessionVars().TxnCtx.ReleaseSavepoint(s.Name)
	if !deleted {
		return exeerrors.ErrSavepointNotExists.GenWithStackByArgs("SAVEPOINT", s.Name)
	}
	return nil
}

func (e *SimpleExec) setCurrentUser(users []*auth.UserIdentity) {
	sessionVars := e.Ctx().GetSessionVars()
	for i, user := range users {
		if user.CurrentUser {
			users[i].Username = sessionVars.User.AuthUsername
			users[i].Hostname = sessionVars.User.AuthHostname
		}
	}
}

func (e *SimpleExec) executeCommit() {
	e.Ctx().GetSessionVars().SetInTxn(false)
}

func (e *SimpleExec) executeRollback(s *ast.RollbackStmt) error {
	sessVars := e.Ctx().GetSessionVars()
	logutil.BgLogger().Debug("execute rollback statement", zap.Uint64("conn", sessVars.ConnectionID))
	txn, err := e.Ctx().Txn(false)
	if err != nil {
		return err
	}
	if s.SavepointName != "" {
		if !txn.Valid() {
			return exeerrors.ErrSavepointNotExists.GenWithStackByArgs("SAVEPOINT", s.SavepointName)
		}
		savepointRecord := sessVars.TxnCtx.RollbackToSavepoint(s.SavepointName)
		if savepointRecord == nil {
			return exeerrors.ErrSavepointNotExists.GenWithStackByArgs("SAVEPOINT", s.SavepointName)
		}
		txn.RollbackMemDBToCheckpoint(savepointRecord.MemDBCheckpoint)
		return nil
	}

	sessVars.SetInTxn(false)
	if txn.Valid() {
		duration := time.Since(sessVars.TxnCtx.CreateTime).Seconds()
		isInternal := false
		if internal := txn.GetOption(kv.RequestSourceInternal); internal != nil && internal.(bool) {
			isInternal = true
		}
		if isInternal && sessVars.TxnCtx.IsPessimistic {
			executor_metrics.TransactionDurationPessimisticRollbackInternal.Observe(duration)
		} else if isInternal && !sessVars.TxnCtx.IsPessimistic {
			executor_metrics.TransactionDurationOptimisticRollbackInternal.Observe(duration)
		} else if !isInternal && sessVars.TxnCtx.IsPessimistic {
			executor_metrics.TransactionDurationPessimisticRollbackGeneral.Observe(duration)
		} else if !isInternal && !sessVars.TxnCtx.IsPessimistic {
			executor_metrics.TransactionDurationOptimisticRollbackGeneral.Observe(duration)
		}
		sessVars.TxnCtx.ClearDelta()
		return txn.Rollback()
	}
	return nil
}


func (e *SimpleExec) executeSetPwd(ctx context.Context, s *ast.SetPwdStmt) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnPrivilege)
	sysSession, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(ctx, sysSession)

	sqlExecutor := sysSession.GetSQLExecutor()
	// session isolation level changed to READ-COMMITTED.
	// When tidb is at the RR isolation level, executing `begin` will obtain a consistent state.
	// When operating the same user concurrently, it may happen that historical versions are read.
	// In order to avoid this risk, change the isolation level to RC.
	_, err = sqlExecutor.ExecuteInternal(ctx, "set tx_isolation = 'READ-COMMITTED'")
	if err != nil {
		return err
	}
	if _, err := sqlExecutor.ExecuteInternal(ctx, "BEGIN PESSIMISTIC"); err != nil {
		return err
	}

	var u, h string
	disableSandboxMode := false
	if s.User == nil || s.User.CurrentUser {
		if e.Ctx().GetSessionVars().User == nil {
			return errors.New("Session error is empty")
		}
		u = e.Ctx().GetSessionVars().User.AuthUsername
		h = e.Ctx().GetSessionVars().User.AuthHostname
	} else {
		u = s.User.Username
		h = s.User.Hostname

		checker := privilege.GetPrivilegeManager(e.Ctx())
		activeRoles := e.Ctx().GetSessionVars().ActiveRoles
		if checker != nil && !checker.RequestVerification(activeRoles, "", "", "", mysql.SuperPriv) {
			currUser := e.Ctx().GetSessionVars().User
			return exeerrors.ErrDBaccessDenied.GenWithStackByArgs(currUser.Username, currUser.Hostname, "mysql")
		}
	}
	exists, authplugin, err := userExistsInternal(ctx, sqlExecutor, u, h)
	if err != nil {
		return err
	}
	if !exists {
		return errors.Trace(exeerrors.ErrPasswordNoMatch)
	}
	if e.Ctx().InSandBoxMode() {
		if !(s.User == nil || s.User.CurrentUser ||
			e.Ctx().GetSessionVars().User.AuthUsername == u && e.Ctx().GetSessionVars().User.AuthHostname == strings.ToLower(h)) {
			return exeerrors.ErrMustChangePassword.GenWithStackByArgs()
		}
		disableSandboxMode = true
	}

	if e.isValidatePasswordEnabled() {
		if err := pwdValidator.ValidatePassword(e.Ctx().GetSessionVars(), s.Password); err != nil {
			return err
		}
	}
	extensions, err := extension.GetExtensions()
	if err != nil {
		return exeerrors.ErrPluginIsNotLoaded.GenWithStackByArgs(err.Error())
	}
	authPlugins := extensions.GetAuthPlugins()
	var pwd string
	switch authplugin {
	case mysql.AuthCachingSha2Password, mysql.AuthTiDBSM3Password:
		pwd = auth.NewHashPassword(s.Password, authplugin)
	case mysql.AuthSocket:
		e.Ctx().GetSessionVars().StmtCtx.AppendNote(exeerrors.ErrSetPasswordAuthPlugin.FastGenByArgs(u, h))
		pwd = ""
	default:
		if pluginImpl, ok := authPlugins[authplugin]; ok {
			if pwd, ok = pluginImpl.GenerateAuthString(s.Password); !ok {
				return exeerrors.ErrPasswordFormat.GenWithStackByArgs()
			}
		} else {
			pwd = auth.EncodePassword(s.Password)
		}
	}

	// for Support Password Reuse Policy.
	plOptions := &passwordOrLockOptionsInfo{
		lockAccount:                 "",
		passwordHistory:             notSpecified,
		passwordReuseInterval:       notSpecified,
		passwordHistoryChange:       false,
		passwordReuseIntervalChange: false,
	}
	// The empty password does not count in the password history and is subject to reuse at any time.
	// https://dev.mysql.com/doc/refman/8.0/en/password-management.html#password-reuse-policy
	if len(pwd) != 0 {
		userDetail := &userInfo{
			host:       h,
			user:       u,
			pLI:        plOptions,
			pwd:        pwd,
			authString: s.Password,
		}
		err := checkPasswordReusePolicy(ctx, sqlExecutor, userDetail, e.Ctx(), authplugin, authPlugins)
		if err != nil {
			return err
		}
	}
	// update mysql.user
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, `UPDATE %n.%n SET authentication_string=%?,password_expired='N',password_last_changed=current_timestamp() WHERE User=%? AND Host=%?;`, mysql.SystemDB, mysql.UserTable, pwd, u, strings.ToLower(h))
	_, err = sqlExecutor.ExecuteInternal(ctx, sql.String())
	if err != nil {
		return err
	}
	if _, err := sqlExecutor.ExecuteInternal(ctx, "commit"); err != nil {
		return err
	}
	err = domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege([]string{u})
	if err != nil {
		return err
	}
	if disableSandboxMode {
		e.Ctx().DisableSandBoxMode()
	}
	return nil
}

func (e *SimpleExec) executeKillStmt(ctx context.Context, s *ast.KillStmt) error {
	if x, ok := s.Expr.(*ast.FuncCallExpr); ok {
		if x.FnName.L == ast.ConnectionID {
			sm := e.Ctx().GetSessionManager()
			sm.Kill(e.Ctx().GetSessionVars().ConnectionID, s.Query, false, false)
			return nil
		}
		return errors.New("Invalid operation. Please use 'KILL TIDB [CONNECTION | QUERY] [connectionID | CONNECTION_ID()]' instead")
	}
	if !config.GetGlobalConfig().EnableGlobalKill {
		conf := config.GetGlobalConfig()
		if s.TiDBExtension || conf.CompatibleKillQuery {
			sm := e.Ctx().GetSessionManager()
			if sm == nil {
				return nil
			}
			sm.Kill(s.ConnectionID, s.Query, false, false)
		} else {
			err := errors.NewNoStackError("Invalid operation. Please use 'KILL TIDB [CONNECTION | QUERY] [connectionID | CONNECTION_ID()]' instead")
			e.Ctx().GetSessionVars().StmtCtx.AppendWarning(err)
		}
		return nil
	}

	sm := e.Ctx().GetSessionManager()
	if sm == nil {
		return nil
	}
	if e.IsFromRemote {
		logutil.BgLogger().Info("Killing connection in current instance redirected from remote TiDB", zap.Uint64("conn", s.ConnectionID), zap.Bool("query", s.Query),
			zap.String("sourceAddr", e.Ctx().GetSessionVars().SourceAddr.IP.String()))
		sm.Kill(s.ConnectionID, s.Query, false, false)
		return nil
	}

	gcid, isTruncated, err := globalconn.ParseConnID(s.ConnectionID)
	if err != nil {
		err1 := errors.NewNoStackError("Parse ConnectionID failed: " + err.Error())
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(err1)
		return nil
	}
	if isTruncated {
		message := "Kill failed: Received a 32bits truncated ConnectionID, expect 64bits. Please execute 'KILL [CONNECTION | QUERY] ConnectionID' to send a Kill without truncating ConnectionID."
		logutil.BgLogger().Warn(message, zap.Uint64("conn", s.ConnectionID))
		// Notice that this warning cannot be seen if KILL is triggered by "CTRL-C" of mysql client,
		//   as the KILL is sent by a new connection.
		err := errors.NewNoStackError(message)
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(err)
		return nil
	}

	if gcid.ServerID != sm.ServerID() {
		if err := killRemoteConn(ctx, e.Ctx(), &gcid, s.Query); err != nil {
			err1 := errors.NewNoStackError("KILL remote connection failed: " + err.Error())
			e.Ctx().GetSessionVars().StmtCtx.AppendWarning(err1)
		}
	} else {
		sm.Kill(s.ConnectionID, s.Query, false, false)
	}

	return nil
}

func killRemoteConn(ctx context.Context, sctx sessionctx.Context, gcid *globalconn.GCID, query bool) error {
	if gcid.ServerID == 0 {
		return errors.New("Unexpected ZERO ServerID. Please file a bug to the TiDB Team")
	}

	killExec := &tipb.Executor{
		Tp:   tipb.ExecType_TypeKill,
		Kill: &tipb.Kill{ConnID: gcid.ToConnID(), Query: query},
	}

	dagReq := &tipb.DAGRequest{}
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(sctx.GetSessionVars().Location())
	sc := sctx.GetSessionVars().StmtCtx
	if sc.RuntimeStatsColl != nil {
		collExec := true
		dagReq.CollectExecutionSummaries = &collExec
	}
	dagReq.Flags = sc.PushDownFlags()
	dagReq.Executors = []*tipb.Executor{killExec}

	var builder distsql.RequestBuilder
	kvReq, err := builder.
		SetDAGRequest(dagReq).
		SetFromSessionVars(sctx.GetDistSQLCtx()).
		SetFromInfoSchema(sctx.GetInfoSchema()).
		SetStoreType(kv.TiDB).
		SetTiDBServerID(gcid.ServerID).
		SetStartTS(math.MaxUint64). // To make check visibility success.
		Build()
	if err != nil {
		return err
	}
	resp := sctx.GetClient().Send(ctx, kvReq, sctx.GetSessionVars().KVVars, &kv.ClientSendOption{})
	if resp == nil {
		err := errors.New("client returns nil response")
		return err
	}

	// Must consume & close the response, otherwise coprocessor task will leak.
	defer func() {
		_ = resp.Close()
	}()
	if _, err := resp.Next(ctx); err != nil {
		return errors.Trace(err)
	}

	logutil.BgLogger().Info("Killed remote connection", zap.Uint64("serverID", gcid.ServerID),
		zap.Uint64("conn", gcid.ToConnID()), zap.Bool("query", query))
	return err
}

func (e *SimpleExec) executeRefreshStats(ctx context.Context, s *ast.RefreshStatsStmt) error {
	intest.AssertFunc(func() bool {
		for _, obj := range s.RefreshObjects {
			switch obj.RefreshObjectScope {
			case ast.RefreshObjectScopeDatabase, ast.RefreshObjectScopeTable:
				if obj.DBName.L == "" {
					return false
				}
			}
		}
		return true
	}, "Refresh stats broadcast requires database-qualified names")
	// Note: Restore the statement to a SQL string so we can broadcast fully qualified
	// table names to every instance. For example, `REFRESH STATS tbl` executed in
	// database `db` must be sent as `REFRESH STATS db.tbl`; otherwise a peer without
	// that current database would skip the table.
	sql, err := restoreRefreshStatsSQL(s)
	if err != nil {
		statslogutil.StatsErrVerboseLogger().Error("Failed to format refresh stats statement", zap.Error(err))
		return err
	}
	if e.IsFromRemote {
		if err := e.executeRefreshStatsOnCurrentInstance(ctx, s); err != nil {
			statslogutil.StatsErrVerboseLogger().Error("Failed to refresh stats from remote", zap.String("sql", sql), zap.Error(err))
			return err
		}
		statslogutil.StatsLogger().Info("Successfully refreshed statistics from remote", zap.String("sql", sql))
		return nil
	}
	if s.IsClusterWide {
		if err := broadcast(ctx, e.Ctx(), sql); err != nil {
			statslogutil.StatsErrVerboseLogger().Error("Failed to broadcast refresh stats command", zap.String("sql", sql), zap.Error(err))
			return err
		}
		logutil.BgLogger().Info("Successfully broadcast query", zap.String("sql", sql))
		return nil
	}
	if err := e.executeRefreshStatsOnCurrentInstance(ctx, s); err != nil {
		statslogutil.StatsErrVerboseLogger().Error("Failed to refresh stats on the current instance", zap.String("sql", sql), zap.Error(err))
		return err
	}
	statslogutil.StatsLogger().Info("Successfully refreshed statistics on the current instance", zap.String("sql", sql))
	return nil
}

func restoreRefreshStatsSQL(s *ast.RefreshStatsStmt) (string, error) {
	var sb strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := s.Restore(restoreCtx); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func (e *SimpleExec) executeRefreshStatsOnCurrentInstance(ctx context.Context, s *ast.RefreshStatsStmt) error {
	intest.Assert(len(s.RefreshObjects) > 0, "RefreshObjects should not be empty")
	intest.AssertFunc(func() bool {
		origCount := len(s.RefreshObjects)
		s.Dedup()
		return origCount == len(s.RefreshObjects)
	}, "RefreshObjects should be deduplicated in the building phase")
	tableIDs := make([]int64, 0, len(s.RefreshObjects))
	isGlobalScope := len(s.RefreshObjects) == 1 && s.RefreshObjects[0].RefreshObjectScope == ast.RefreshObjectScopeGlobal
	is := sessiontxn.GetTxnManager(e.Ctx()).GetTxnInfoSchema()
	if !isGlobalScope {
		for _, refreshObject := range s.RefreshObjects {
			switch refreshObject.RefreshObjectScope {
			case ast.RefreshObjectScopeDatabase:
				exists := is.SchemaExists(refreshObject.DBName)
				if !exists {
					e.Ctx().GetSessionVars().StmtCtx.AppendWarning(infoschema.ErrDatabaseNotExists.FastGenByArgs(refreshObject.DBName))
					statslogutil.StatsLogger().Warn("Failed to find database when refreshing stats", zap.String("db", refreshObject.DBName.O))
					continue
				}
				tables, err := is.SchemaTableInfos(ctx, refreshObject.DBName)
				if err != nil {
					return errors.Trace(err)
				}
				if len(tables) == 0 {
					// Note: We do not warn about databases without tables because we cannot issue a warning
					// for every such database when refreshing with `REFRESH STATS *.*`.(Technically, we can, but no point to do so.)
					// Instead, we simply log the information to remain consistent across all cases.
					statslogutil.StatsLogger().Info("No table in the database when refreshing stats", zap.String("db", refreshObject.DBName.O))
					continue
				}
				for _, table := range tables {
					tableIDs = append(tableIDs, table.ID)
				}
			case ast.RefreshObjectScopeTable:
				table, err := is.TableInfoByName(refreshObject.DBName, refreshObject.TableName)
				if err != nil {
					if infoschema.ErrTableNotExists.Equal(err) {
						e.Ctx().GetSessionVars().StmtCtx.AppendWarning(infoschema.ErrTableNotExists.FastGenByArgs(refreshObject.DBName, refreshObject.TableName))
						statslogutil.StatsLogger().Warn("Failed to find table when refreshing stats", zap.String("db", refreshObject.DBName.O), zap.String("table", refreshObject.TableName.O))
						continue
					}
					return errors.Trace(err)
				}
				if table == nil {
					intest.Assert(false, "Table should not be nil here")
					e.Ctx().GetSessionVars().StmtCtx.AppendWarning(infoschema.ErrTableNotExists.FastGenByArgs(refreshObject.DBName, refreshObject.TableName))
					statslogutil.StatsLogger().Warn("Failed to find table when refreshing stats", zap.String("db", refreshObject.DBName.O), zap.String("table", refreshObject.TableName.O))
					continue
				}
				tableIDs = append(tableIDs, table.ID)
			default:
				intest.Assert(false, "No other scopes should be here")
			}
		}
		// If all specified databases or tables do not exist, we do nothing.
		if len(tableIDs) == 0 {
			statslogutil.StatsLogger().Info("No valid database or table to refresh stats")
			return nil
		}
	}
	// Note: tableIDs is empty means to refresh all tables.
	h := domain.GetDomain(e.Ctx()).StatsHandle()
	if s.RefreshMode != nil {
		if *s.RefreshMode == ast.RefreshStatsModeLite {
			return h.InitStatsLite(ctx, tableIDs...)
		}
		return h.InitStats(ctx, is, tableIDs...)
	}
	liteInitStats := config.GetGlobalConfig().Performance.LiteInitStats
	if liteInitStats {
		return h.InitStatsLite(ctx, tableIDs...)
	}
	return h.InitStats(ctx, is, tableIDs...)
}

func broadcast(ctx context.Context, sctx sessionctx.Context, sql string) error {
	broadcastExec := &tipb.Executor{
		Tp: tipb.ExecType_TypeBroadcastQuery,
		BroadcastQuery: &tipb.BroadcastQuery{
			Query: &sql,
		},
	}
	dagReq := &tipb.DAGRequest{}
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(sctx.GetSessionVars().Location())
	sc := sctx.GetSessionVars().StmtCtx
	if sc.RuntimeStatsColl != nil {
		collExec := true
		dagReq.CollectExecutionSummaries = &collExec
	}
	dagReq.Flags = sc.PushDownFlags()
	dagReq.Executors = []*tipb.Executor{broadcastExec}

	var builder distsql.RequestBuilder
	kvReq, err := builder.
		SetDAGRequest(dagReq).
		SetFromSessionVars(sctx.GetDistSQLCtx()).
		SetFromInfoSchema(sctx.GetInfoSchema()).
		SetStoreType(kv.TiDB).
		// Send to all TiDB instances.
		SetTiDBServerID(0).
		SetStartTS(math.MaxUint64).
		Build()
	if err != nil {
		return err
	}
	resp := sctx.GetClient().Send(ctx, kvReq, sctx.GetSessionVars().KVVars, &kv.ClientSendOption{})
	if resp == nil {
		err := errors.New("client returns nil response")
		return err
	}

	// Must consume & close the response, otherwise coprocessor task will leak.
	defer func() {
		_ = resp.Close()
	}()
	for {
		subset, err := resp.Next(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if subset == nil {
			break // all remote tasks finished cleanly
		}
	}

	return nil
}

func (e *SimpleExec) executeFlush(ctx context.Context, s *ast.FlushStmt) error {
	switch s.Tp {
	case ast.FlushTables:
		if s.ReadLock {
			return errors.New("FLUSH TABLES WITH READ LOCK is not supported.  Please use @@tidb_snapshot")
		}
	case ast.FlushPrivileges:
		dom := domain.GetDomain(e.Ctx())
		return dom.NotifyUpdateAllUsersPrivilege()
	case ast.FlushTiDBPlugin:
		dom := domain.GetDomain(e.Ctx())
		for _, pluginName := range s.Plugins {
			err := plugin.NotifyFlush(dom, pluginName)
			if err != nil {
				return err
			}
		}
	case ast.FlushClientErrorsSummary:
		errno.FlushStats()
	case ast.FlushStatsDelta:
		h := domain.GetDomain(e.Ctx()).StatsHandle()
		if e.IsFromRemote {
			err := h.DumpStatsDeltaToKV(true)
			if err != nil {
				statslogutil.StatsErrVerboseLogger().Error("Failed to dump stats delta to KV from remote", zap.Error(err))
			} else {
				statslogutil.StatsLogger().Info("Successfully dumped stats delta to KV from remote")
			}
			return err
		}
		if s.IsCluster {
			var sb strings.Builder
			restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
			if err := s.Restore(restoreCtx); err != nil {
				statslogutil.StatsErrVerboseLogger().Error("Failed to format flush stats delta statement", zap.Error(err))
				return err
			}
			sql := sb.String()
			if err := broadcast(ctx, e.Ctx(), sql); err != nil {
				statslogutil.StatsErrVerboseLogger().Error("Failed to broadcast flush stats delta command", zap.String("sql", sql), zap.Error(err))
				return err
			}
			logutil.BgLogger().Info("Successfully broadcast query", zap.String("sql", sql))
			return nil
		}
		return h.DumpStatsDeltaToKV(true)
	}
	return nil
}

