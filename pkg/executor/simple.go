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
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	executor_metrics "github.com/pingcap/tidb/pkg/executor/metrics"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	pwdValidator "github.com/pingcap/tidb/pkg/util/password-validation"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
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

