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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/domain/resourcegroup"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/querywatch"
	executor_metrics "github.com/pingcap/tidb/pkg/executor/metrics"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/plugin"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/globalconn"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	pwdValidator "github.com/pingcap/tidb/pkg/util/password-validation"
	"github.com/pingcap/tidb/pkg/util/sem"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tidb/pkg/util/tls"
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

	Statement ast.StmtNode
	// IsFromRemote indicates whether the statement IS FROM REMOTE TiDB instance in cluster,
	//   and executing in coprocessor.
	//   Used for `global kill`. See https://github.com/pingcap/tidb/blob/master/docs/design/2020-06-01-global-kill.md.
	IsFromRemote bool
	done         bool
	is           infoschema.InfoSchema

	// staleTxnStartTS is the StartTS that is used to execute the staleness txn during a read-only begin statement.
	staleTxnStartTS uint64
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

// clearSysSession close the session does not return the session.
// Since the environment variables in the session are changed, the session object is not returned.
func clearSysSession(ctx context.Context, sctx sessionctx.Context) {
	if sctx == nil {
		return
	}
	_, _ = sctx.GetSQLExecutor().ExecuteInternal(ctx, "rollback")
	sctx.(pools.Resource).Close()
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
		err = e.executeFlush(x)
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
	case *ast.BinlogStmt:
		// We just ignore it.
		return nil
	case *ast.DropStatsStmt:
		err = e.executeDropStats(x)
	case *ast.SetRoleStmt:
		err = e.executeSetRole(x)
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

func (e *SimpleExec) setDefaultRoleNone(s *ast.SetDefaultRoleStmt) error {
	restrictedCtx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	defer e.ReleaseSysSession(ctx, restrictedCtx)
	sqlExecutor := restrictedCtx.GetSQLExecutor()
	if _, err := sqlExecutor.ExecuteInternal(ctx, "begin"); err != nil {
		return err
	}
	sql := new(strings.Builder)
	for _, u := range s.UserList {
		if u.Hostname == "" {
			u.Hostname = "%"
		}
		sql.Reset()
		sqlescape.MustFormatSQL(sql, "DELETE IGNORE FROM mysql.default_roles WHERE USER=%? AND HOST=%?;", u.Username, u.Hostname)
		if _, err := sqlExecutor.ExecuteInternal(ctx, sql.String()); err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
			if _, rollbackErr := sqlExecutor.ExecuteInternal(ctx, "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
	}
	if _, err := sqlExecutor.ExecuteInternal(ctx, "commit"); err != nil {
		return err
	}
	return nil
}

func (e *SimpleExec) setDefaultRoleRegular(ctx context.Context, s *ast.SetDefaultRoleStmt) error {
	for _, user := range s.UserList {
		exists, err := userExists(ctx, e.Ctx(), user.Username, user.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return exeerrors.ErrCannotUser.GenWithStackByArgs("SET DEFAULT ROLE", user.String())
		}
	}
	for _, role := range s.RoleList {
		exists, err := userExists(ctx, e.Ctx(), role.Username, role.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return exeerrors.ErrCannotUser.GenWithStackByArgs("SET DEFAULT ROLE", role.String())
		}
	}

	restrictedCtx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	defer e.ReleaseSysSession(internalCtx, restrictedCtx)
	sqlExecutor := restrictedCtx.GetSQLExecutor()
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "begin"); err != nil {
		return err
	}
	sql := new(strings.Builder)
	for _, user := range s.UserList {
		if user.Hostname == "" {
			user.Hostname = "%"
		}
		sql.Reset()
		sqlescape.MustFormatSQL(sql, "DELETE IGNORE FROM mysql.default_roles WHERE USER=%? AND HOST=%?;", user.Username, user.Hostname)
		if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
			if _, rollbackErr := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
		for _, role := range s.RoleList {
			checker := privilege.GetPrivilegeManager(e.Ctx())
			ok := checker.FindEdge(e.Ctx(), role, user)
			if !ok {
				if _, rollbackErr := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); rollbackErr != nil {
					return rollbackErr
				}
				return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(role.String(), user.String())
			}
			sql.Reset()
			sqlescape.MustFormatSQL(sql, "INSERT IGNORE INTO mysql.default_roles values(%?, %?, %?, %?);", user.Hostname, user.Username, role.Hostname, role.Username)
			if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
				logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
				if _, rollbackErr := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); rollbackErr != nil {
					return rollbackErr
				}
				return err
			}
		}
	}
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "commit"); err != nil {
		return err
	}
	return nil
}

func (e *SimpleExec) setDefaultRoleAll(ctx context.Context, s *ast.SetDefaultRoleStmt) error {
	for _, user := range s.UserList {
		exists, err := userExists(ctx, e.Ctx(), user.Username, user.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return exeerrors.ErrCannotUser.GenWithStackByArgs("SET DEFAULT ROLE", user.String())
		}
	}
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	restrictedCtx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(internalCtx, restrictedCtx)
	sqlExecutor := restrictedCtx.GetSQLExecutor()
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "begin"); err != nil {
		return err
	}
	sql := new(strings.Builder)
	for _, user := range s.UserList {
		if user.Hostname == "" {
			user.Hostname = "%"
		}
		sql.Reset()
		sqlescape.MustFormatSQL(sql, "DELETE IGNORE FROM mysql.default_roles WHERE USER=%? AND HOST=%?;", user.Username, user.Hostname)
		if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
			if _, rollbackErr := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
		sql.Reset()
		sqlescape.MustFormatSQL(sql, "INSERT IGNORE INTO mysql.default_roles(HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER) SELECT TO_HOST,TO_USER,FROM_HOST,FROM_USER FROM mysql.role_edges WHERE TO_HOST=%? AND TO_USER=%?;", user.Hostname, user.Username)
		if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
			if _, rollbackErr := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
	}
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "commit"); err != nil {
		return err
	}
	return nil
}

func (e *SimpleExec) setDefaultRoleForCurrentUser(s *ast.SetDefaultRoleStmt) (err error) {
	checker := privilege.GetPrivilegeManager(e.Ctx())
	user := s.UserList[0]
	if user.Hostname == "" {
		user.Hostname = "%"
	}
	restrictedCtx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	defer e.ReleaseSysSession(ctx, restrictedCtx)
	sqlExecutor := restrictedCtx.GetSQLExecutor()

	if _, err := sqlExecutor.ExecuteInternal(ctx, "begin"); err != nil {
		return err
	}

	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, "DELETE IGNORE FROM mysql.default_roles WHERE USER=%? AND HOST=%?;", user.Username, user.Hostname)
	if _, err := sqlExecutor.ExecuteInternal(ctx, sql.String()); err != nil {
		logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
		if _, rollbackErr := sqlExecutor.ExecuteInternal(ctx, "rollback"); rollbackErr != nil {
			return rollbackErr
		}
		return err
	}

	sql.Reset()
	switch s.SetRoleOpt {
	case ast.SetRoleNone:
		sqlescape.MustFormatSQL(sql, "DELETE IGNORE FROM mysql.default_roles WHERE USER=%? AND HOST=%?;", user.Username, user.Hostname)
	case ast.SetRoleAll:
		sqlescape.MustFormatSQL(sql, "INSERT IGNORE INTO mysql.default_roles(HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER) SELECT TO_HOST,TO_USER,FROM_HOST,FROM_USER FROM mysql.role_edges WHERE TO_HOST=%? AND TO_USER=%?;", user.Hostname, user.Username)
	case ast.SetRoleRegular:
		sqlescape.MustFormatSQL(sql, "INSERT IGNORE INTO mysql.default_roles values")
		for i, role := range s.RoleList {
			if i > 0 {
				sqlescape.MustFormatSQL(sql, ",")
			}
			ok := checker.FindEdge(e.Ctx(), role, user)
			if !ok {
				return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(role.String(), user.String())
			}
			sqlescape.MustFormatSQL(sql, "(%?, %?, %?, %?)", user.Hostname, user.Username, role.Hostname, role.Username)
		}
	}

	if _, err := sqlExecutor.ExecuteInternal(ctx, sql.String()); err != nil {
		logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
		if _, rollbackErr := sqlExecutor.ExecuteInternal(ctx, "rollback"); rollbackErr != nil {
			return rollbackErr
		}
		return err
	}
	if _, err := sqlExecutor.ExecuteInternal(ctx, "commit"); err != nil {
		return err
	}
	return nil
}

func (e *SimpleExec) executeSetDefaultRole(ctx context.Context, s *ast.SetDefaultRoleStmt) (err error) {
	sessionVars := e.Ctx().GetSessionVars()
	checker := privilege.GetPrivilegeManager(e.Ctx())
	if checker == nil {
		return errors.New("miss privilege checker")
	}

	if len(s.UserList) == 1 && sessionVars.User != nil {
		u, h := s.UserList[0].Username, s.UserList[0].Hostname
		if u == sessionVars.User.Username && h == sessionVars.User.AuthHostname {
			err = e.setDefaultRoleForCurrentUser(s)
			if err != nil {
				return err
			}
			return domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege()
		}
	}

	activeRoles := sessionVars.ActiveRoles
	if !checker.RequestVerification(activeRoles, mysql.SystemDB, mysql.DefaultRoleTable, "", mysql.UpdatePriv) {
		if !checker.RequestVerification(activeRoles, "", "", "", mysql.CreateUserPriv) {
			return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
		}
	}

	switch s.SetRoleOpt {
	case ast.SetRoleAll:
		err = e.setDefaultRoleAll(ctx, s)
	case ast.SetRoleNone:
		err = e.setDefaultRoleNone(s)
	case ast.SetRoleRegular:
		err = e.setDefaultRoleRegular(ctx, s)
	}
	if err != nil {
		return
	}
	return domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege()
}

func (e *SimpleExec) setRoleRegular(s *ast.SetRoleStmt) error {
	// Deal with SQL like `SET ROLE role1, role2;`
	checkDup := make(map[string]*auth.RoleIdentity, len(s.RoleList))
	// Check whether RoleNameList contain duplicate role name.
	for _, r := range s.RoleList {
		key := r.String()
		checkDup[key] = r
	}
	roleList := make([]*auth.RoleIdentity, 0, 10)
	for _, v := range checkDup {
		roleList = append(roleList, v)
	}

	checker := privilege.GetPrivilegeManager(e.Ctx())
	ok, roleName := checker.ActiveRoles(e.Ctx(), roleList)
	if !ok {
		u := e.Ctx().GetSessionVars().User
		return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return nil
}

func (e *SimpleExec) setRoleAll() error {
	// Deal with SQL like `SET ROLE ALL;`
	checker := privilege.GetPrivilegeManager(e.Ctx())
	user, host := e.Ctx().GetSessionVars().User.AuthUsername, e.Ctx().GetSessionVars().User.AuthHostname
	roles := checker.GetAllRoles(user, host)
	ok, roleName := checker.ActiveRoles(e.Ctx(), roles)
	if !ok {
		u := e.Ctx().GetSessionVars().User
		return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return nil
}

func (e *SimpleExec) setRoleAllExcept(s *ast.SetRoleStmt) error {
	// Deal with SQL like `SET ROLE ALL EXCEPT role1, role2;`
	for _, r := range s.RoleList {
		if r.Hostname == "" {
			r.Hostname = "%"
		}
	}
	checker := privilege.GetPrivilegeManager(e.Ctx())
	user, host := e.Ctx().GetSessionVars().User.AuthUsername, e.Ctx().GetSessionVars().User.AuthHostname
	roles := checker.GetAllRoles(user, host)

	filter := func(arr []*auth.RoleIdentity, f func(*auth.RoleIdentity) bool) []*auth.RoleIdentity {
		i, j := 0, 0
		for i = 0; i < len(arr); i++ {
			if f(arr[i]) {
				arr[j] = arr[i]
				j++
			}
		}
		return arr[:j]
	}
	banned := func(r *auth.RoleIdentity) bool {
		for _, ban := range s.RoleList {
			if ban.Hostname == r.Hostname && ban.Username == r.Username {
				return false
			}
		}
		return true
	}

	afterExcept := filter(roles, banned)
	ok, roleName := checker.ActiveRoles(e.Ctx(), afterExcept)
	if !ok {
		u := e.Ctx().GetSessionVars().User
		return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return nil
}

func (e *SimpleExec) setRoleDefault() error {
	// Deal with SQL like `SET ROLE DEFAULT;`
	checker := privilege.GetPrivilegeManager(e.Ctx())
	user, host := e.Ctx().GetSessionVars().User.AuthUsername, e.Ctx().GetSessionVars().User.AuthHostname
	roles := checker.GetDefaultRoles(user, host)
	ok, roleName := checker.ActiveRoles(e.Ctx(), roles)
	if !ok {
		u := e.Ctx().GetSessionVars().User
		return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return nil
}

func (e *SimpleExec) setRoleNone() error {
	// Deal with SQL like `SET ROLE NONE;`
	checker := privilege.GetPrivilegeManager(e.Ctx())
	roles := make([]*auth.RoleIdentity, 0)
	ok, roleName := checker.ActiveRoles(e.Ctx(), roles)
	if !ok {
		u := e.Ctx().GetSessionVars().User
		return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return nil
}

func (e *SimpleExec) executeSetRole(s *ast.SetRoleStmt) error {
	switch s.SetRoleOpt {
	case ast.SetRoleRegular:
		return e.setRoleRegular(s)
	case ast.SetRoleAll:
		return e.setRoleAll()
	case ast.SetRoleAllExcept:
		return e.setRoleAllExcept(s)
	case ast.SetRoleNone:
		return e.setRoleNone()
	case ast.SetRoleDefault:
		return e.setRoleDefault()
	}
	return nil
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
	dbname := model.NewCIStr(s.DBName)

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
	return sessionVars.SetSystemVarWithoutValidation(variable.CollationDatabase, dbCollate)
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
	if sessVars.BinlogClient != nil {
		return ErrSavepointNotSupportedWithBinlog
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

func (e *SimpleExec) executeRevokeRole(ctx context.Context, s *ast.RevokeRoleStmt) error {
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)

	//Fix revoke role from current_user results error.
	e.setCurrentUser(s.Users)

	for _, role := range s.Roles {
		exists, err := userExists(ctx, e.Ctx(), role.Username, role.Hostname)
		if err != nil {
			return errors.Trace(err)
		}
		if !exists {
			return exeerrors.ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", role.String())
		}
	}

	restrictedCtx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(internalCtx, restrictedCtx)
	sqlExecutor := restrictedCtx.GetSQLExecutor()

	// begin a transaction to insert role graph edges.
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "begin"); err != nil {
		return errors.Trace(err)
	}
	sql := new(strings.Builder)
	// when an active role of current user is revoked,
	// it should be removed from activeRoles
	activeRoles, curUser, curHost := e.Ctx().GetSessionVars().ActiveRoles, "", ""
	if user := e.Ctx().GetSessionVars().User; user != nil {
		curUser, curHost = user.AuthUsername, user.AuthHostname
	}
	for _, user := range s.Users {
		exists, err := userExists(ctx, e.Ctx(), user.Username, user.Hostname)
		if err != nil {
			return errors.Trace(err)
		}
		if !exists {
			if _, err := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); err != nil {
				return errors.Trace(err)
			}
			return exeerrors.ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", user.String())
		}
		for _, role := range s.Roles {
			if role.Hostname == "" {
				role.Hostname = "%"
			}
			sql.Reset()
			sqlescape.MustFormatSQL(sql, `DELETE IGNORE FROM %n.%n WHERE FROM_HOST=%? and FROM_USER=%? and TO_HOST=%? and TO_USER=%?`, mysql.SystemDB, mysql.RoleEdgeTable, role.Hostname, role.Username, user.Hostname, user.Username)
			if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
				if _, err := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); err != nil {
					return errors.Trace(err)
				}
				return exeerrors.ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", role.String())
			}

			sql.Reset()
			sqlescape.MustFormatSQL(sql, `DELETE IGNORE FROM %n.%n WHERE DEFAULT_ROLE_HOST=%? and DEFAULT_ROLE_USER=%? and HOST=%? and USER=%?`, mysql.SystemDB, mysql.DefaultRoleTable, role.Hostname, role.Username, user.Hostname, user.Username)
			if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
				if _, err := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); err != nil {
					return errors.Trace(err)
				}
				return exeerrors.ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", role.String())
			}

			// delete from activeRoles
			if curUser == user.Username && curHost == user.Hostname {
				for i := 0; i < len(activeRoles); i++ {
					if activeRoles[i].Username == role.Username && activeRoles[i].Hostname == role.Hostname {
						activeRoles = append(activeRoles[:i], activeRoles[i+1:]...)
						break
					}
				}
			}
		}
	}
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "commit"); err != nil {
		return err
	}
	checker := privilege.GetPrivilegeManager(e.Ctx())
	if checker == nil {
		return errors.New("miss privilege checker")
	}
	if ok, roleName := checker.ActiveRoles(e.Ctx(), activeRoles); !ok {
		u := e.Ctx().GetSessionVars().User
		return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege()
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

func whetherSavePasswordHistory(plOptions *passwordOrLockOptionsInfo) bool {
	var passwdSaveNum, passwdSaveTime int64
	// If the user specifies a default, read the global variable.
	if plOptions.passwordHistoryChange && plOptions.passwordHistory != notSpecified {
		passwdSaveNum = plOptions.passwordHistory
	} else {
		passwdSaveNum = variable.PasswordHistory.Load()
	}
	if plOptions.passwordReuseIntervalChange && plOptions.passwordReuseInterval != notSpecified {
		passwdSaveTime = plOptions.passwordReuseInterval
	} else {
		passwdSaveTime = variable.PasswordReuseInterval.Load()
	}
	return passwdSaveTime > 0 || passwdSaveNum > 0
}

type alterUserPasswordLocking struct {
	failedLoginAttempts            int64
	passwordLockTime               int64
	failedLoginAttemptsNotFound    bool
	passwordLockTimeChangeNotFound bool
	// containsNoOthers indicates whether User_attributes only contains one "Password_locking" element.
	containsNoOthers bool
}

func (info *passwordOrLockOptionsInfo) loadOptions(plOption []*ast.PasswordOrLockOption) error {
	if length := len(plOption); length > 0 {
		// If "PASSWORD EXPIRE ..." appears many times,
		// only the last declaration takes effect.
	Loop:
		for i := length - 1; i >= 0; i-- {
			switch plOption[i].Type {
			case ast.PasswordExpire:
				info.passwordExpired = "Y"
				break Loop
			case ast.PasswordExpireDefault:
				info.passwordLifetime = nil
				break Loop
			case ast.PasswordExpireNever:
				info.passwordLifetime = 0
				break Loop
			case ast.PasswordExpireInterval:
				if plOption[i].Count == 0 || plOption[i].Count > math.MaxUint16 {
					return types.ErrWrongValue2.GenWithStackByArgs("DAY", fmt.Sprintf("%v", plOption[i].Count))
				}
				info.passwordLifetime = plOption[i].Count
				break Loop
			}
		}
	}
	// only the last declaration takes effect.
	for _, option := range plOption {
		switch option.Type {
		case ast.Lock:
			info.lockAccount = "Y"
		case ast.Unlock:
			info.lockAccount = "N"
		case ast.FailedLoginAttempts:
			info.failedLoginAttempts = min(option.Count, math.MaxInt16)
			info.failedLoginAttemptsChange = true
		case ast.PasswordLockTime:
			info.passwordLockTime = min(option.Count, math.MaxInt16)
			info.passwordLockTimeChange = true
		case ast.PasswordLockTimeUnbounded:
			info.passwordLockTime = -1
			info.passwordLockTimeChange = true
		case ast.PasswordHistory:
			info.passwordHistory = min(option.Count, math.MaxUint16)
			info.passwordHistoryChange = true
		case ast.PasswordHistoryDefault:
			info.passwordHistory = notSpecified
			info.passwordHistoryChange = true
		case ast.PasswordReuseInterval:
			info.passwordReuseInterval = min(option.Count, math.MaxUint16)
			info.passwordReuseIntervalChange = true
		case ast.PasswordReuseDefault:
			info.passwordReuseInterval = notSpecified
			info.passwordReuseIntervalChange = true
		}
	}
	return nil
}

func createUserFailedLoginJSON(info *passwordOrLockOptionsInfo) string {
	// Record only when either failedLoginAttempts and passwordLockTime is not 0
	if (info.failedLoginAttemptsChange && info.failedLoginAttempts != 0) || (info.passwordLockTimeChange && info.passwordLockTime != 0) {
		return fmt.Sprintf("\"Password_locking\": {\"failed_login_attempts\": %d,\"password_lock_time_days\": %d}",
			info.failedLoginAttempts, info.passwordLockTime)
	}
	return ""
}

func alterUserFailedLoginJSON(info *alterUserPasswordLocking, lockAccount string) string {
	// alterUserPasswordLocking is the user's actual configuration.
	var passwordLockingArray []string
	if info.failedLoginAttempts != 0 || info.passwordLockTime != 0 {
		if lockAccount == "N" {
			passwordLockingArray = append(passwordLockingArray,
				fmt.Sprintf("\"auto_account_locked\": \"%s\"", lockAccount),
				fmt.Sprintf("\"auto_locked_last_changed\": \"%s\"", time.Now().Format(time.UnixDate)),
				fmt.Sprintf("\"failed_login_count\": %d", 0))
		}
		passwordLockingArray = append(passwordLockingArray,
			fmt.Sprintf("\"failed_login_attempts\": %d", info.failedLoginAttempts),
			fmt.Sprintf("\"password_lock_time_days\": %d", info.passwordLockTime))
	}
	if len(passwordLockingArray) > 0 {
		return fmt.Sprintf("\"Password_locking\": {%s}", strings.Join(passwordLockingArray, ","))
	}
	return ""
}

func readPasswordLockingInfo(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, name string, host string, pLO *passwordOrLockOptionsInfo) (aUPL *alterUserPasswordLocking, err error) {
	alterUserInfo := &alterUserPasswordLocking{
		failedLoginAttempts:            0,
		passwordLockTime:               0,
		failedLoginAttemptsNotFound:    false,
		passwordLockTimeChangeNotFound: false,
		containsNoOthers:               false,
	}
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, `SELECT JSON_UNQUOTE(JSON_EXTRACT(user_attributes, '$.Password_locking.failed_login_attempts')),
        JSON_UNQUOTE(JSON_EXTRACT(user_attributes, '$.Password_locking.password_lock_time_days')),
	    JSON_LENGTH(JSON_REMOVE(user_attributes, '$.Password_locking')) FROM %n.%n WHERE User=%? AND Host=%?;`,
		mysql.SystemDB, mysql.UserTable, name, strings.ToLower(host))
	recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := recordSet.Close(); closeErr != nil {
			err = closeErr
		}
	}()
	rows, err := sqlexec.DrainRecordSet(ctx, recordSet, 3)
	if err != nil {
		return nil, err
	}

	// Configuration priority is User Changes > User History
	if pLO.failedLoginAttemptsChange {
		alterUserInfo.failedLoginAttempts = pLO.failedLoginAttempts
	} else if !rows[0].IsNull(0) {
		str := rows[0].GetString(0)
		alterUserInfo.failedLoginAttempts, err = strconv.ParseInt(str, 10, 64)
		if err != nil {
			return nil, err
		}
		alterUserInfo.failedLoginAttempts = max(alterUserInfo.failedLoginAttempts, 0)
		alterUserInfo.failedLoginAttempts = min(alterUserInfo.failedLoginAttempts, math.MaxInt16)
	} else {
		alterUserInfo.failedLoginAttemptsNotFound = true
	}

	if pLO.passwordLockTimeChange {
		alterUserInfo.passwordLockTime = pLO.passwordLockTime
	} else if !rows[0].IsNull(1) {
		str := rows[0].GetString(1)
		alterUserInfo.passwordLockTime, err = strconv.ParseInt(str, 10, 64)
		if err != nil {
			return nil, err
		}
		alterUserInfo.passwordLockTime = max(alterUserInfo.passwordLockTime, -1)
		alterUserInfo.passwordLockTime = min(alterUserInfo.passwordLockTime, math.MaxInt16)
	} else {
		alterUserInfo.passwordLockTimeChangeNotFound = true
	}

	alterUserInfo.containsNoOthers = rows[0].IsNull(2) || rows[0].GetInt64(2) == 0
	return alterUserInfo, nil
}

// deletePasswordLockingAttribute deletes "$.Password_locking" in "User_attributes" when failedLoginAttempts and passwordLockTime both 0.
func deletePasswordLockingAttribute(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, name string, host string, alterUser *alterUserPasswordLocking) error {
	// No password_locking information.
	if alterUser.failedLoginAttemptsNotFound && alterUser.passwordLockTimeChangeNotFound {
		return nil
	}
	// Password_locking information is still in used.
	if alterUser.failedLoginAttempts != 0 || alterUser.passwordLockTime != 0 {
		return nil
	}
	sql := new(strings.Builder)
	if alterUser.containsNoOthers {
		// If we use JSON_REMOVE(user_attributes, '$.Password_locking') directly here, the result is not compatible with MySQL.
		sqlescape.MustFormatSQL(sql, `UPDATE %n.%n SET user_attributes=NULL`, mysql.SystemDB, mysql.UserTable)
	} else {
		sqlescape.MustFormatSQL(sql, `UPDATE %n.%n SET user_attributes=JSON_REMOVE(user_attributes, '$.Password_locking') `, mysql.SystemDB, mysql.UserTable)
	}
	sqlescape.MustFormatSQL(sql, " WHERE Host=%? and User=%?;", host, name)
	_, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	return err
}

func (e *SimpleExec) isValidatePasswordEnabled() bool {
	validatePwdEnable, err := e.Ctx().GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.ValidatePasswordEnable)
	if err != nil {
		return false
	}
	return variable.TiDBOptOn(validatePwdEnable)
}

func (e *SimpleExec) executeCreateUser(ctx context.Context, s *ast.CreateUserStmt) error {
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	// Check `CREATE USER` privilege.
	if !config.GetGlobalConfig().Security.SkipGrantTable {
		checker := privilege.GetPrivilegeManager(e.Ctx())
		if checker == nil {
			return errors.New("miss privilege checker")
		}
		activeRoles := e.Ctx().GetSessionVars().ActiveRoles
		if !checker.RequestVerification(activeRoles, mysql.SystemDB, mysql.UserTable, "", mysql.InsertPriv) {
			if s.IsCreateRole {
				if !checker.RequestVerification(activeRoles, "", "", "", mysql.CreateRolePriv) &&
					!checker.RequestVerification(activeRoles, "", "", "", mysql.CreateUserPriv) {
					return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE ROLE or CREATE USER")
				}
			}
			if !s.IsCreateRole && !checker.RequestVerification(activeRoles, "", "", "", mysql.CreateUserPriv) {
				return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE User")
			}
		}
	}

	privData, err := tlsOption2GlobalPriv(s.AuthTokenOrTLSOptions)
	if err != nil {
		return err
	}

	plOptions := &passwordOrLockOptionsInfo{
		lockAccount:                 "N",
		passwordExpired:             "N",
		passwordLifetime:            nil,
		passwordHistory:             notSpecified,
		passwordReuseInterval:       notSpecified,
		failedLoginAttemptsChange:   false,
		passwordLockTimeChange:      false,
		passwordHistoryChange:       false,
		passwordReuseIntervalChange: false,
	}
	err = plOptions.loadOptions(s.PasswordOrLockOptions)
	if err != nil {
		return err
	}
	passwordLocking := createUserFailedLoginJSON(plOptions)
	if s.IsCreateRole {
		plOptions.lockAccount = "Y"
		plOptions.passwordExpired = "Y"
	}

	var userAttributes []string
	if s.CommentOrAttributeOption != nil {
		if s.CommentOrAttributeOption.Type == ast.UserCommentType {
			userAttributes = append(userAttributes, fmt.Sprintf("\"metadata\": {\"comment\": \"%s\"}", s.CommentOrAttributeOption.Value))
		} else if s.CommentOrAttributeOption.Type == ast.UserAttributeType {
			userAttributes = append(userAttributes, fmt.Sprintf("\"metadata\": %s", s.CommentOrAttributeOption.Value))
		}
	}

	if s.ResourceGroupNameOption != nil {
		if !variable.EnableResourceControl.Load() {
			return infoschema.ErrResourceGroupSupportDisabled
		}

		resourceGroupName := strings.ToLower(s.ResourceGroupNameOption.Value)

		// check if specified resource group exists
		if resourceGroupName != resourcegroup.DefaultResourceGroupName && resourceGroupName != "" {
			_, exists := e.is.ResourceGroupByName(model.NewCIStr(resourceGroupName))
			if !exists {
				return infoschema.ErrResourceGroupNotExists.GenWithStackByArgs(resourceGroupName)
			}
		}
		userAttributes = append(userAttributes, fmt.Sprintf("\"resource_group\": \"%s\"", resourceGroupName))
	}
	// If FAILED_LOGIN_ATTEMPTS and PASSWORD_LOCK_TIME are both specified to 0, a string of 0 length is generated.
	// When inserting the attempts into json, an error occurs. This requires special handling.
	if passwordLocking != "" {
		userAttributes = append(userAttributes, passwordLocking)
	}
	userAttributesStr := fmt.Sprintf("{%s}", strings.Join(userAttributes, ","))

	tokenIssuer := ""
	for _, authTokenOption := range s.AuthTokenOrTLSOptions {
		if authTokenOption.Type == ast.TokenIssuer {
			tokenIssuer = authTokenOption.Value
		}
	}

	sql := new(strings.Builder)
	sqlPasswordHistory := new(strings.Builder)
	passwordInit := true
	// Get changed user password reuse info.
	savePasswdHistory := whetherSavePasswordHistory(plOptions)
	sqlTemplate := "INSERT INTO %n.%n (Host, User, authentication_string, plugin, user_attributes, Account_locked, Token_issuer, Password_expired, Password_lifetime,  Password_reuse_time, Password_reuse_history) VALUES "
	valueTemplate := "(%?, %?, %?, %?, %?, %?, %?, %?, %?"

	sqlescape.MustFormatSQL(sql, sqlTemplate, mysql.SystemDB, mysql.UserTable)
	if savePasswdHistory {
		sqlescape.MustFormatSQL(sqlPasswordHistory, `INSERT INTO %n.%n (Host, User, Password) VALUES `, mysql.SystemDB, mysql.PasswordHistoryTable)
	}

	users := make([]*auth.UserIdentity, 0, len(s.Specs))
	for _, spec := range s.Specs {
		if len(spec.User.Username) > auth.UserNameMaxLength {
			return exeerrors.ErrWrongStringLength.GenWithStackByArgs(spec.User.Username, "user name", auth.UserNameMaxLength)
		}
		if len(spec.User.Username) == 0 && plOptions.passwordExpired == "Y" {
			return exeerrors.ErrPasswordExpireAnonymousUser.GenWithStackByArgs()
		}
		if len(spec.User.Hostname) > auth.HostNameMaxLength {
			return exeerrors.ErrWrongStringLength.GenWithStackByArgs(spec.User.Hostname, "host name", auth.HostNameMaxLength)
		}
		if len(users) > 0 {
			sqlescape.MustFormatSQL(sql, ",")
		}
		exists, err1 := userExists(ctx, e.Ctx(), spec.User.Username, spec.User.Hostname)
		if err1 != nil {
			return err1
		}
		if exists {
			user := fmt.Sprintf(`'%s'@'%s'`, spec.User.Username, spec.User.Hostname)
			if !s.IfNotExists {
				if s.IsCreateRole {
					return exeerrors.ErrCannotUser.GenWithStackByArgs("CREATE ROLE", user)
				}
				return exeerrors.ErrCannotUser.GenWithStackByArgs("CREATE USER", user)
			}
			err := infoschema.ErrUserAlreadyExists.FastGenByArgs(user)
			e.Ctx().GetSessionVars().StmtCtx.AppendNote(err)
			continue
		}
		authPlugin := mysql.AuthNativePassword
		if spec.AuthOpt != nil && spec.AuthOpt.AuthPlugin != "" {
			authPlugin = spec.AuthOpt.AuthPlugin
		}
		// Validate the strength of the password if necessary
		if e.isValidatePasswordEnabled() && !s.IsCreateRole && mysql.IsAuthPluginClearText(authPlugin) {
			pwd := ""
			if spec.AuthOpt != nil {
				pwd = spec.AuthOpt.AuthString
			}
			if err := pwdValidator.ValidatePassword(e.Ctx().GetSessionVars(), pwd); err != nil {
				return err
			}
		}
		pwd, ok := spec.EncodedPassword()

		if !ok {
			return errors.Trace(exeerrors.ErrPasswordFormat)
		}

		switch authPlugin {
		case mysql.AuthNativePassword, mysql.AuthCachingSha2Password, mysql.AuthTiDBSM3Password, mysql.AuthSocket, mysql.AuthTiDBAuthToken, mysql.AuthLDAPSimple, mysql.AuthLDAPSASL:
		default:
			return exeerrors.ErrPluginIsNotLoaded.GenWithStackByArgs(spec.AuthOpt.AuthPlugin)
		}

		recordTokenIssuer := tokenIssuer
		if len(recordTokenIssuer) > 0 && authPlugin != mysql.AuthTiDBAuthToken {
			err := fmt.Errorf("TOKEN_ISSUER is not needed for '%s' user", authPlugin)
			e.Ctx().GetSessionVars().StmtCtx.AppendWarning(err)
			recordTokenIssuer = ""
		} else if len(recordTokenIssuer) == 0 && authPlugin == mysql.AuthTiDBAuthToken {
			err := fmt.Errorf("TOKEN_ISSUER is needed for 'tidb_auth_token' user, please use 'alter user' to declare it")
			e.Ctx().GetSessionVars().StmtCtx.AppendWarning(err)
		}

		hostName := strings.ToLower(spec.User.Hostname)
		sqlescape.MustFormatSQL(sql, valueTemplate, hostName, spec.User.Username, pwd, authPlugin, userAttributesStr, plOptions.lockAccount, recordTokenIssuer, plOptions.passwordExpired, plOptions.passwordLifetime)
		// add Password_reuse_time value.
		if plOptions.passwordReuseIntervalChange && (plOptions.passwordReuseInterval != notSpecified) {
			sqlescape.MustFormatSQL(sql, `, %?`, plOptions.passwordReuseInterval)
		} else {
			sqlescape.MustFormatSQL(sql, `, %?`, nil)
		}
		// add Password_reuse_history value.
		if plOptions.passwordHistoryChange && (plOptions.passwordHistory != notSpecified) {
			sqlescape.MustFormatSQL(sql, `, %?`, plOptions.passwordHistory)
		} else {
			sqlescape.MustFormatSQL(sql, `, %?`, nil)
		}
		sqlescape.MustFormatSQL(sql, `)`)
		// The empty password does not count in the password history and is subject to reuse at any time.
		// AuthTiDBAuthToken is the token login method on the cloud,
		// and the Password Reuse Policy does not take effect.
		if savePasswdHistory && len(pwd) != 0 && !strings.EqualFold(authPlugin, mysql.AuthTiDBAuthToken) {
			if !passwordInit {
				sqlescape.MustFormatSQL(sqlPasswordHistory, ",")
			} else {
				passwordInit = false
			}
			sqlescape.MustFormatSQL(sqlPasswordHistory, `( %?, %?, %?)`, hostName, spec.User.Username, pwd)
		}
		users = append(users, spec.User)
	}
	if len(users) == 0 {
		return nil
	}

	restrictedCtx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(internalCtx, restrictedCtx)
	sqlExecutor := restrictedCtx.GetSQLExecutor()

	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "begin"); err != nil {
		return errors.Trace(err)
	}
	_, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String())
	if err != nil {
		logutil.BgLogger().Warn("Fail to create user", zap.String("sql", sql.String()))
		if _, rollbackErr := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); rollbackErr != nil {
			return rollbackErr
		}
		return err
	}

	if savePasswdHistory && !passwordInit {
		_, err = sqlExecutor.ExecuteInternal(internalCtx, sqlPasswordHistory.String())
		if err != nil {
			if _, rollbackErr := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); rollbackErr != nil {
				return errors.Trace(rollbackErr)
			}
			return errors.Trace(err)
		}
	}

	if len(privData) != 0 {
		sql.Reset()
		sqlescape.MustFormatSQL(sql, "INSERT IGNORE INTO %n.%n (Host, User, Priv) VALUES ", mysql.SystemDB, mysql.GlobalPrivTable)
		for i, user := range users {
			if i > 0 {
				sqlescape.MustFormatSQL(sql, ",")
			}
			sqlescape.MustFormatSQL(sql, `(%?, %?, %?)`, user.Hostname, user.Username, string(hack.String(privData)))
		}
		_, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String())
		if err != nil {
			if _, rollbackErr := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
	}
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "commit"); err != nil {
		return errors.Trace(err)
	}
	return domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege()
}

func getUserPasswordLimit(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, name string, host string, plOptions *passwordOrLockOptionsInfo) (pRI *passwordReuseInfo, err error) {
	res := &passwordReuseInfo{notSpecified, notSpecified}
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, `SELECT Password_reuse_history,Password_reuse_time FROM %n.%n WHERE User=%? AND Host=%?;`,
		mysql.SystemDB, mysql.UserTable, name, strings.ToLower(host))
	// Query the specified user password reuse rules.
	recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := recordSet.Close(); closeErr != nil {
			err = closeErr
		}
	}()
	rows, err := sqlexec.DrainRecordSet(ctx, recordSet, 3)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		if !row.IsNull(0) {
			res.passwordHistory = int64(row.GetUint64(0))
		} else {
			res.passwordHistory = variable.PasswordHistory.Load()
		}
		if !row.IsNull(1) {
			res.passwordReuseInterval = int64(row.GetUint64(1))
		} else {
			res.passwordReuseInterval = variable.PasswordReuseInterval.Load()
		}
	}
	if plOptions.passwordHistoryChange {
		// If the user specifies a default, the global variable needs to be re-read.
		if plOptions.passwordHistory != notSpecified {
			res.passwordHistory = plOptions.passwordHistory
		} else {
			res.passwordHistory = variable.PasswordHistory.Load()
		}
	}
	if plOptions.passwordReuseIntervalChange {
		// If the user specifies a default, the global variable needs to be re-read.
		if plOptions.passwordReuseInterval != notSpecified {
			res.passwordReuseInterval = plOptions.passwordReuseInterval
		} else {
			res.passwordReuseInterval = variable.PasswordReuseInterval.Load()
		}
	}
	return res, nil
}

// getValidTime get the boundary of password valid time.
func getValidTime(sctx sessionctx.Context, passwordReuse *passwordReuseInfo) string {
	nowTime := time.Now().In(sctx.GetSessionVars().TimeZone)
	nowTimeS := nowTime.Unix()
	beforeTimeS := nowTimeS - passwordReuse.passwordReuseInterval*24*int64(time.Hour/time.Second)
	if beforeTimeS < 0 {
		beforeTimeS = 0
	}
	return time.Unix(beforeTimeS, 0).Format("2006-01-02 15:04:05.999999999")
}

// deleteHistoricalData delete useless password history.
// The deleted password must meet the following conditions at the same time.
// 1. Exceeded the maximum number of saves.
// 2. The password has exceeded the prohibition time.
func deleteHistoricalData(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, userDetail *userInfo, maxDelRows int64, passwordReuse *passwordReuseInfo, sctx sessionctx.Context) error {
	//never times out or no row need delete.
	if (passwordReuse.passwordReuseInterval > math.MaxInt32) || maxDelRows == 0 {
		return nil
	}
	sql := new(strings.Builder)
	// no prohibition time.
	if passwordReuse.passwordReuseInterval == 0 {
		deleteTemplate := `DELETE from %n.%n WHERE User= %? AND Host= %? order by Password_timestamp ASC LIMIT `
		deleteTemplate = deleteTemplate + strconv.FormatInt(maxDelRows, 10)
		sqlescape.MustFormatSQL(sql, deleteTemplate, mysql.SystemDB, mysql.PasswordHistoryTable,
			userDetail.user, strings.ToLower(userDetail.host))
		_, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
		if err != nil {
			return err
		}
	} else {
		beforeDate := getValidTime(sctx, passwordReuse)
		// Deletion must satisfy 1. Exceed the prohibition time 2. Exceed the maximum number of saved records.
		deleteTemplate := `DELETE from %n.%n WHERE User= %? AND Host= %? AND Password_timestamp < %? order by Password_timestamp ASC LIMIT `
		deleteTemplate = deleteTemplate + strconv.FormatInt(maxDelRows, 10)
		sql.Reset()
		sqlescape.MustFormatSQL(sql, deleteTemplate, mysql.SystemDB, mysql.PasswordHistoryTable,
			userDetail.user, strings.ToLower(userDetail.host), beforeDate)
		_, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
		if err != nil {
			return err
		}
	}
	return nil
}

func addHistoricalData(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, userDetail *userInfo, passwordReuse *passwordReuseInfo) error {
	if passwordReuse.passwordHistory <= 0 && passwordReuse.passwordReuseInterval <= 0 {
		return nil
	}
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, `INSERT INTO %n.%n (Host, User, Password) VALUES (%?, %?, %?) `, mysql.SystemDB, mysql.PasswordHistoryTable, strings.ToLower(userDetail.host), userDetail.user, userDetail.pwd)
	_, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// checkPasswordsMatch used to compare whether the password encrypted with mysql.AuthCachingSha2Password or mysql.AuthTiDBSM3Password is repeated.
func checkPasswordsMatch(rows []chunk.Row, oldPwd, authPlugin string) (bool, error) {
	for _, row := range rows {
		if !row.IsNull(0) {
			pwd := row.GetString(0)
			authok, err := auth.CheckHashingPassword([]byte(pwd), oldPwd, authPlugin)
			if err != nil {
				logutil.BgLogger().Error("Failed to check caching_sha2_password", zap.Error(err))
				return false, err
			}
			if authok {
				return false, nil
			}
		}
	}
	return true, nil
}

func getUserPasswordNum(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, userDetail *userInfo) (deleteNum int64, err error) {
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, `SELECT count(*) FROM %n.%n WHERE User=%? AND Host=%?;`, mysql.SystemDB, mysql.PasswordHistoryTable, userDetail.user, strings.ToLower(userDetail.host))
	recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	if err != nil {
		return 0, err
	}
	defer func() {
		if closeErr := recordSet.Close(); closeErr != nil {
			err = closeErr
		}
	}()
	rows, err := sqlexec.DrainRecordSet(ctx, recordSet, 3)
	if err != nil {
		return 0, err
	}
	if len(rows) != 1 {
		err := fmt.Errorf("`%s`@`%s` is not unique, please confirm the mysql.password_history table structure", userDetail.user, strings.ToLower(userDetail.host))
		return 0, err
	}

	return rows[0].GetInt64(0), nil
}

func fullRecordCheck(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, userDetail *userInfo, authPlugin string) (canUse bool, err error) {
	switch authPlugin {
	case mysql.AuthNativePassword, "":
		sql := new(strings.Builder)
		sqlescape.MustFormatSQL(sql, `SELECT count(*) FROM %n.%n WHERE User= %? AND Host= %? AND Password = %?;`, mysql.SystemDB, mysql.PasswordHistoryTable, userDetail.user, strings.ToLower(userDetail.host), userDetail.pwd)
		recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
		if err != nil {
			return false, err
		}
		defer func() {
			if closeErr := recordSet.Close(); closeErr != nil {
				err = closeErr
			}
		}()
		rows, err := sqlexec.DrainRecordSet(ctx, recordSet, 3)
		if err != nil {
			return false, err
		}
		if rows[0].GetInt64(0) == 0 {
			return true, nil
		}
		return false, nil
	case mysql.AuthCachingSha2Password, mysql.AuthTiDBSM3Password:
		sql := new(strings.Builder)
		sqlescape.MustFormatSQL(sql, `SELECT Password FROM %n.%n WHERE User= %? AND Host= %? ;`, mysql.SystemDB, mysql.PasswordHistoryTable, userDetail.user, strings.ToLower(userDetail.host))
		recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
		if err != nil {
			return false, err
		}
		defer func() {
			if closeErr := recordSet.Close(); closeErr != nil {
				err = closeErr
			}
		}()
		rows, err := sqlexec.DrainRecordSet(ctx, recordSet, variable.DefMaxChunkSize)
		if err != nil {
			return false, err
		}
		return checkPasswordsMatch(rows, userDetail.authString, authPlugin)
	default:
		return false, exeerrors.ErrPluginIsNotLoaded.GenWithStackByArgs(authPlugin)
	}
}

func checkPasswordHistoryRule(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, userDetail *userInfo, passwordReuse *passwordReuseInfo, authPlugin string) (canUse bool, err error) {
	switch authPlugin {
	case mysql.AuthNativePassword, "":
		sql := new(strings.Builder)
		// Exceeded the maximum number of saved items, only check the ones within the limit.
		checkRows := `SELECT count(*) FROM (SELECT Password FROM %n.%n WHERE User=%? AND Host=%? ORDER BY Password_timestamp DESC LIMIT `
		checkRows = checkRows + strconv.FormatInt(passwordReuse.passwordHistory, 10)
		checkRows = checkRows + ` ) as t where t.Password = %? `
		sqlescape.MustFormatSQL(sql, checkRows, mysql.SystemDB, mysql.PasswordHistoryTable, userDetail.user, strings.ToLower(userDetail.host), userDetail.pwd)
		recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
		if err != nil {
			return false, err
		}
		defer func() {
			if closeErr := recordSet.Close(); closeErr != nil {
				err = closeErr
			}
		}()
		rows, err := sqlexec.DrainRecordSet(ctx, recordSet, 3)
		if err != nil {
			return false, err
		}
		if rows[0].GetInt64(0) != 0 {
			return false, nil
		}
		return true, nil
	case mysql.AuthCachingSha2Password, mysql.AuthTiDBSM3Password:
		sql := new(strings.Builder)
		checkRows := `SELECT Password FROM %n.%n WHERE User=%? AND Host=%? ORDER BY Password_timestamp DESC LIMIT `
		checkRows = checkRows + strconv.FormatInt(passwordReuse.passwordHistory, 10)
		sqlescape.MustFormatSQL(sql, checkRows, mysql.SystemDB, mysql.PasswordHistoryTable, userDetail.user, strings.ToLower(userDetail.host))
		recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
		if err != nil {
			return false, err
		}
		defer func() {
			if closeErr := recordSet.Close(); closeErr != nil {
				err = closeErr
			}
		}()
		rows, err := sqlexec.DrainRecordSet(ctx, recordSet, variable.DefMaxChunkSize)
		if err != nil {
			return false, err
		}
		return checkPasswordsMatch(rows, userDetail.authString, authPlugin)
	default:
		return false, exeerrors.ErrPluginIsNotLoaded.GenWithStackByArgs(authPlugin)
	}
}

func checkPasswordTimeRule(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, userDetail *userInfo, passwordReuse *passwordReuseInfo,
	sctx sessionctx.Context, authPlugin string) (canUse bool, err error) {
	beforeDate := getValidTime(sctx, passwordReuse)
	switch authPlugin {
	case mysql.AuthNativePassword, "":
		sql := new(strings.Builder)
		sqlescape.MustFormatSQL(sql, `SELECT count(*) FROM %n.%n WHERE User=%? AND Host=%? AND Password = %? AND Password_timestamp >= %?;`,
			mysql.SystemDB, mysql.PasswordHistoryTable, userDetail.user, strings.ToLower(userDetail.host), userDetail.pwd, beforeDate)
		recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
		if err != nil {
			return false, err
		}
		defer func() {
			if closeErr := recordSet.Close(); closeErr != nil {
				err = closeErr
			}
		}()
		rows, err := sqlexec.DrainRecordSet(ctx, recordSet, 3)
		if err != nil {
			return false, err
		}
		if rows[0].GetInt64(0) == 0 {
			return true, nil
		}
	case mysql.AuthCachingSha2Password, mysql.AuthTiDBSM3Password:
		sql := new(strings.Builder)
		sqlescape.MustFormatSQL(sql, `SELECT Password FROM %n.%n WHERE User=%? AND Host=%? AND Password_timestamp >= %?;`, mysql.SystemDB, mysql.PasswordHistoryTable, userDetail.user, strings.ToLower(userDetail.host), beforeDate)
		recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
		if err != nil {
			return false, err
		}
		defer func() {
			if closeErr := recordSet.Close(); closeErr != nil {
				err = closeErr
			}
		}()
		rows, err := sqlexec.DrainRecordSet(ctx, recordSet, variable.DefMaxChunkSize)
		if err != nil {
			return false, err
		}
		return checkPasswordsMatch(rows, userDetail.authString, authPlugin)
	default:
		return false, exeerrors.ErrPluginIsNotLoaded.GenWithStackByArgs(authPlugin)
	}
	return false, nil
}

func passwordVerification(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, userDetail *userInfo, passwordReuse *passwordReuseInfo, sctx sessionctx.Context, authPlugin string) (bool, int64, error) {
	passwordNum, err := getUserPasswordNum(ctx, sqlExecutor, userDetail)
	if err != nil {
		return false, 0, err
	}

	// the maximum number of records that can be deleted.
	canDeleteNum := passwordNum - passwordReuse.passwordHistory + 1
	if canDeleteNum < 0 {
		canDeleteNum = 0
	}

	if passwordReuse.passwordHistory <= 0 && passwordReuse.passwordReuseInterval <= 0 {
		return true, canDeleteNum, nil
	}

	// The maximum number of saves has not been exceeded.
	// There are too many retention days, and it is impossible to time out in one's lifetime.
	if (passwordNum <= passwordReuse.passwordHistory) || (passwordReuse.passwordReuseInterval > math.MaxInt32) {
		passChecking, err := fullRecordCheck(ctx, sqlExecutor, userDetail, authPlugin)
		return passChecking, canDeleteNum, err
	}

	if passwordReuse.passwordHistory > 0 {
		passChecking, err := checkPasswordHistoryRule(ctx, sqlExecutor, userDetail, passwordReuse, authPlugin)
		if err != nil || !passChecking {
			return false, 0, err
		}
	}
	if passwordReuse.passwordReuseInterval > 0 {
		passChecking, err := checkPasswordTimeRule(ctx, sqlExecutor, userDetail, passwordReuse, sctx, authPlugin)
		if err != nil || !passChecking {
			return false, 0, err
		}
	}
	return true, canDeleteNum, nil
}

func checkPasswordReusePolicy(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, userDetail *userInfo, sctx sessionctx.Context, authPlugin string) error {
	if strings.EqualFold(authPlugin, mysql.AuthTiDBAuthToken) || strings.EqualFold(authPlugin, mysql.AuthLDAPSASL) || strings.EqualFold(authPlugin, mysql.AuthLDAPSimple) {
		// AuthTiDBAuthToken is the token login method on the cloud,
		// and the Password Reuse Policy does not take effect.
		return nil
	}
	// read password reuse info from mysql.user and global variables.
	passwdReuseInfo, err := getUserPasswordLimit(ctx, sqlExecutor, userDetail.user, userDetail.host, userDetail.pLI)
	if err != nil {
		return err
	}
	// check whether password can be used.
	res, maxDelNum, err := passwordVerification(ctx, sqlExecutor, userDetail, passwdReuseInfo, sctx, authPlugin)
	if err != nil {
		return err
	}
	if !res {
		return exeerrors.ErrExistsInHistoryPassword.GenWithStackByArgs(userDetail.user, userDetail.host)
	}
	err = deleteHistoricalData(ctx, sqlExecutor, userDetail, maxDelNum, passwdReuseInfo, sctx)
	if err != nil {
		return err
	}
	// insert password history.
	err = addHistoricalData(ctx, sqlExecutor, userDetail, passwdReuseInfo)
	if err != nil {
		return err
	}
	return nil
}

func (e *SimpleExec) executeAlterUser(ctx context.Context, s *ast.AlterUserStmt) error {
	disableSandBoxMode := false
	var err error
	if e.Ctx().InSandBoxMode() {
		if err = e.checkSandboxMode(s.Specs); err != nil {
			return err
		}
		disableSandBoxMode = true
	}
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnPrivilege)
	if s.CurrentAuth != nil {
		user := e.Ctx().GetSessionVars().User
		if user == nil {
			return errors.New("Session user is empty")
		}
		// Use AuthHostname to search the user record, set Hostname as AuthHostname.
		userCopy := *user
		userCopy.Hostname = userCopy.AuthHostname
		spec := &ast.UserSpec{
			User:    &userCopy,
			AuthOpt: s.CurrentAuth,
		}
		s.Specs = []*ast.UserSpec{spec}
	}

	plOptions := passwordOrLockOptionsInfo{
		lockAccount:                 "",
		passwordExpired:             "",
		passwordLifetime:            notSpecified,
		passwordHistory:             notSpecified,
		passwordReuseInterval:       notSpecified,
		failedLoginAttemptsChange:   false,
		passwordLockTimeChange:      false,
		passwordHistoryChange:       false,
		passwordReuseIntervalChange: false,
	}
	err = plOptions.loadOptions(s.PasswordOrLockOptions)
	if err != nil {
		return err
	}

	privData, err := tlsOption2GlobalPriv(s.AuthTokenOrTLSOptions)
	if err != nil {
		return err
	}

	failedUsers := make([]string, 0, len(s.Specs))
	needRollback := false
	checker := privilege.GetPrivilegeManager(e.Ctx())
	if checker == nil {
		return errors.New("could not load privilege checker")
	}
	activeRoles := e.Ctx().GetSessionVars().ActiveRoles
	hasCreateUserPriv := checker.RequestVerification(activeRoles, "", "", "", mysql.CreateUserPriv)
	hasSystemUserPriv := checker.RequestDynamicVerification(activeRoles, "SYSTEM_USER", false)
	hasRestrictedUserPriv := checker.RequestDynamicVerification(activeRoles, "RESTRICTED_USER_ADMIN", false)
	hasSystemSchemaPriv := checker.RequestVerification(activeRoles, mysql.SystemDB, mysql.UserTable, "", mysql.UpdatePriv)

	var authTokenOptions []*ast.AuthTokenOrTLSOption
	for _, authTokenOrTLSOption := range s.AuthTokenOrTLSOptions {
		if authTokenOrTLSOption.Type == ast.TokenIssuer {
			authTokenOptions = append(authTokenOptions, authTokenOrTLSOption)
		}
	}

	sysSession, err := e.GetSysSession()
	defer clearSysSession(ctx, sysSession)
	if err != nil {
		return err
	}
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

	for _, spec := range s.Specs {
		user := e.Ctx().GetSessionVars().User
		if spec.User.CurrentUser || ((user != nil) && (user.Username == spec.User.Username) && (user.AuthHostname == spec.User.Hostname)) {
			spec.User.Username = user.Username
			spec.User.Hostname = user.AuthHostname
		} else {
			// The user executing the query (user) does not match the user specified (spec.User)
			// The MySQL manual states:
			// "In most cases, ALTER USER requires the global CREATE USER privilege, or the UPDATE privilege for the mysql system schema"
			//
			// This is true unless the user being modified has the SYSTEM_USER dynamic privilege.
			// See: https://mysqlserverteam.com/the-system_user-dynamic-privilege/
			//
			// In the current implementation of DYNAMIC privileges, SUPER can be used as a substitute for any DYNAMIC privilege
			// (unless SEM is enabled; in which case RESTRICTED_* privileges will not use SUPER as a substitute). This is intentional
			// because visitInfo can not accept OR conditions for permissions and in many cases MySQL permits SUPER instead.

			// Thus, any user with SUPER can effectively ALTER/DROP a SYSTEM_USER, and
			// any user with only CREATE USER can not modify the properties of users with SUPER privilege.
			// We extend this in TiDB with SEM, where SUPER users can not modify users with RESTRICTED_USER_ADMIN.
			// For simplicity: RESTRICTED_USER_ADMIN also counts for SYSTEM_USER here.

			if !(hasCreateUserPriv || hasSystemSchemaPriv) {
				return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
			}
			if checker.RequestDynamicVerificationWithUser("SYSTEM_USER", false, spec.User) && !(hasSystemUserPriv || hasRestrictedUserPriv) {
				return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SYSTEM_USER or SUPER")
			}
			if sem.IsEnabled() && checker.RequestDynamicVerificationWithUser("RESTRICTED_USER_ADMIN", false, spec.User) && !hasRestrictedUserPriv {
				return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("RESTRICTED_USER_ADMIN")
			}
		}

		exists, err := userExistsInternal(ctx, sqlExecutor, spec.User.Username, spec.User.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			user := fmt.Sprintf(`'%s'@'%s'`, spec.User.Username, spec.User.Hostname)
			failedUsers = append(failedUsers, user)
			continue
		}

		type AuthTokenOptionHandler int
		const (
			// noNeedAuthTokenOptions means the final auth plugin is NOT tidb_auth_plugin
			noNeedAuthTokenOptions AuthTokenOptionHandler = iota
			// OptionalAuthTokenOptions means the final auth_plugin is tidb_auth_plugin,
			// and whether to declare AuthTokenOptions or not is ok.
			OptionalAuthTokenOptions
			// RequireAuthTokenOptions means the final auth_plugin is tidb_auth_plugin and need AuthTokenOptions here
			RequireAuthTokenOptions
		)
		authTokenOptionHandler := noNeedAuthTokenOptions
		currentAuthPlugin, err := privilege.GetPrivilegeManager(e.Ctx()).GetAuthPlugin(spec.User.Username, spec.User.Hostname)
		if err != nil {
			return err
		}
		if currentAuthPlugin == mysql.AuthTiDBAuthToken {
			authTokenOptionHandler = OptionalAuthTokenOptions
		}

		type alterField struct {
			expr  string
			value any
		}
		var fields []alterField
		if spec.AuthOpt != nil {
			fields = append(fields, alterField{"password_last_changed=current_timestamp()", nil})
			if spec.AuthOpt.AuthPlugin == "" {
				spec.AuthOpt.AuthPlugin = currentAuthPlugin
			}
			switch spec.AuthOpt.AuthPlugin {
			case mysql.AuthNativePassword, mysql.AuthCachingSha2Password, mysql.AuthTiDBSM3Password, mysql.AuthSocket, mysql.AuthLDAPSimple, mysql.AuthLDAPSASL, "":
				authTokenOptionHandler = noNeedAuthTokenOptions
			case mysql.AuthTiDBAuthToken:
				if authTokenOptionHandler != OptionalAuthTokenOptions {
					authTokenOptionHandler = RequireAuthTokenOptions
				}
			default:
				return exeerrors.ErrPluginIsNotLoaded.GenWithStackByArgs(spec.AuthOpt.AuthPlugin)
			}
			// changing the auth method prunes history.
			if spec.AuthOpt.AuthPlugin != currentAuthPlugin {
				// delete password history from mysql.password_history.
				sql := new(strings.Builder)
				sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, mysql.PasswordHistoryTable, spec.User.Hostname, spec.User.Username)
				if _, err := sqlExecutor.ExecuteInternal(ctx, sql.String()); err != nil {
					failedUsers = append(failedUsers, spec.User.String())
					needRollback = true
					break
				}
			}
			if e.isValidatePasswordEnabled() && spec.AuthOpt.ByAuthString && mysql.IsAuthPluginClearText(spec.AuthOpt.AuthPlugin) {
				if err := pwdValidator.ValidatePassword(e.Ctx().GetSessionVars(), spec.AuthOpt.AuthString); err != nil {
					return err
				}
			}
			pwd, ok := spec.EncodedPassword()
			if !ok {
				return errors.Trace(exeerrors.ErrPasswordFormat)
			}
			// for Support Password Reuse Policy.
			// The empty password does not count in the password history and is subject to reuse at any time.
			// https://dev.mysql.com/doc/refman/8.0/en/password-management.html#password-reuse-policy
			if len(pwd) != 0 {
				userDetail := &userInfo{
					host:       spec.User.Hostname,
					user:       spec.User.Username,
					pLI:        &plOptions,
					pwd:        pwd,
					authString: spec.AuthOpt.AuthString,
				}
				err := checkPasswordReusePolicy(ctx, sqlExecutor, userDetail, e.Ctx(), spec.AuthOpt.AuthPlugin)
				if err != nil {
					return err
				}
			}
			fields = append(fields, alterField{"authentication_string=%?", pwd})
			if spec.AuthOpt.AuthPlugin != "" {
				fields = append(fields, alterField{"plugin=%?", spec.AuthOpt.AuthPlugin})
			}
			if spec.AuthOpt.ByAuthString || spec.AuthOpt.ByHashString {
				if plOptions.passwordExpired == "" {
					plOptions.passwordExpired = "N"
				}
			}
		}

		if len(plOptions.lockAccount) != 0 {
			fields = append(fields, alterField{"account_locked=%?", plOptions.lockAccount})
		}

		// support alter Password_reuse_history and Password_reuse_time.
		if plOptions.passwordHistoryChange {
			if plOptions.passwordHistory == notSpecified {
				fields = append(fields, alterField{"Password_reuse_history = NULL ", ""})
			} else {
				fields = append(fields, alterField{"Password_reuse_history = %? ", strconv.FormatInt(plOptions.passwordHistory, 10)})
			}
		}
		if plOptions.passwordReuseIntervalChange {
			if plOptions.passwordReuseInterval == notSpecified {
				fields = append(fields, alterField{"Password_reuse_time = NULL ", ""})
			} else {
				fields = append(fields, alterField{"Password_reuse_time = %? ", strconv.FormatInt(plOptions.passwordReuseInterval, 10)})
			}
		}

		passwordLockingInfo, err := readPasswordLockingInfo(ctx, sqlExecutor, spec.User.Username, spec.User.Hostname, &plOptions)
		if err != nil {
			return err
		}
		passwordLockingStr := alterUserFailedLoginJSON(passwordLockingInfo, plOptions.lockAccount)

		if len(plOptions.passwordExpired) != 0 {
			if len(spec.User.Username) == 0 && plOptions.passwordExpired == "Y" {
				return exeerrors.ErrPasswordExpireAnonymousUser.GenWithStackByArgs()
			}
			fields = append(fields, alterField{"password_expired=%?", plOptions.passwordExpired})
		}
		if plOptions.passwordLifetime != notSpecified {
			fields = append(fields, alterField{"password_lifetime=%?", plOptions.passwordLifetime})
		}

		var newAttributes []string
		if s.CommentOrAttributeOption != nil {
			if s.CommentOrAttributeOption.Type == ast.UserCommentType {
				newAttributes = append(newAttributes, fmt.Sprintf(`"metadata": {"comment": "%s"}`, s.CommentOrAttributeOption.Value))
			} else {
				newAttributes = append(newAttributes, fmt.Sprintf(`"metadata": %s`, s.CommentOrAttributeOption.Value))
			}
		}
		if s.ResourceGroupNameOption != nil {
			if !variable.EnableResourceControl.Load() {
				return infoschema.ErrResourceGroupSupportDisabled
			}

			// check if specified resource group exists
			resourceGroupName := strings.ToLower(s.ResourceGroupNameOption.Value)
			if resourceGroupName != resourcegroup.DefaultResourceGroupName && s.ResourceGroupNameOption.Value != "" {
				_, exists := e.is.ResourceGroupByName(model.NewCIStr(resourceGroupName))
				if !exists {
					return infoschema.ErrResourceGroupNotExists.GenWithStackByArgs(resourceGroupName)
				}
			}

			newAttributes = append(newAttributes, fmt.Sprintf(`"resource_group": "%s"`, resourceGroupName))
		}
		if passwordLockingStr != "" {
			newAttributes = append(newAttributes, passwordLockingStr)
		}
		if length := len(newAttributes); length > 0 {
			if length > 1 || passwordLockingStr == "" {
				passwordLockingInfo.containsNoOthers = false
			}
			newAttributesStr := fmt.Sprintf("{%s}", strings.Join(newAttributes, ","))
			fields = append(fields, alterField{"user_attributes=json_merge_patch(coalesce(user_attributes, '{}'), %?)", newAttributesStr})
		}

		switch authTokenOptionHandler {
		case noNeedAuthTokenOptions:
			if len(authTokenOptions) > 0 {
				err := errors.NewNoStackError("TOKEN_ISSUER is not needed for the auth plugin")
				e.Ctx().GetSessionVars().StmtCtx.AppendWarning(err)
			}
		case OptionalAuthTokenOptions:
			if len(authTokenOptions) > 0 {
				for _, authTokenOption := range authTokenOptions {
					fields = append(fields, alterField{authTokenOption.Type.String() + "=%?", authTokenOption.Value})
				}
			}
		case RequireAuthTokenOptions:
			if len(authTokenOptions) > 0 {
				for _, authTokenOption := range authTokenOptions {
					fields = append(fields, alterField{authTokenOption.Type.String() + "=%?", authTokenOption.Value})
				}
			} else {
				err := errors.NewNoStackError("Auth plugin 'tidb_auth_plugin' needs TOKEN_ISSUER")
				e.Ctx().GetSessionVars().StmtCtx.AppendWarning(err)
			}
		}

		if len(fields) > 0 {
			sql := new(strings.Builder)
			sqlescape.MustFormatSQL(sql, "UPDATE %n.%n SET ", mysql.SystemDB, mysql.UserTable)
			for i, f := range fields {
				sqlescape.MustFormatSQL(sql, f.expr, f.value)
				if i < len(fields)-1 {
					sqlescape.MustFormatSQL(sql, ",")
				}
			}
			sqlescape.MustFormatSQL(sql, " WHERE Host=%? and User=%?;", spec.User.Hostname, spec.User.Username)
			_, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
			if err != nil {
				failedUsers = append(failedUsers, spec.User.String())
				needRollback = true
				continue
			}
		}

		// Remove useless Password_locking from User_attributes.
		err = deletePasswordLockingAttribute(ctx, sqlExecutor, spec.User.Username, spec.User.Hostname, passwordLockingInfo)
		if err != nil {
			failedUsers = append(failedUsers, spec.User.String())
			needRollback = true
			continue
		}

		if len(privData) > 0 {
			sql := new(strings.Builder)
			sqlescape.MustFormatSQL(sql, "INSERT INTO %n.%n (Host, User, Priv) VALUES (%?,%?,%?) ON DUPLICATE KEY UPDATE Priv = values(Priv)", mysql.SystemDB, mysql.GlobalPrivTable, spec.User.Hostname, spec.User.Username, string(hack.String(privData)))
			_, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
			if err != nil {
				failedUsers = append(failedUsers, spec.User.String())
				needRollback = true
			}
		}
	}
	if len(failedUsers) > 0 {
		// Compatible with MySQL 8.0, `ALTER USER` realizes atomic operation.
		if !s.IfExists || needRollback {
			return exeerrors.ErrCannotUser.GenWithStackByArgs("ALTER USER", strings.Join(failedUsers, ","))
		}
		for _, user := range failedUsers {
			err := infoschema.ErrUserDropExists.FastGenByArgs(user)
			e.Ctx().GetSessionVars().StmtCtx.AppendNote(err)
		}
	}
	if _, err := sqlExecutor.ExecuteInternal(ctx, "commit"); err != nil {
		return err
	}
	if err = domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege(); err != nil {
		return err
	}
	if disableSandBoxMode {
		e.Ctx().DisableSandBoxMode()
	}
	return nil
}

func (e *SimpleExec) checkSandboxMode(specs []*ast.UserSpec) error {
	for _, spec := range specs {
		if spec.AuthOpt == nil {
			continue
		}
		if spec.AuthOpt.ByAuthString || spec.AuthOpt.ByHashString {
			if spec.User.CurrentUser || e.Ctx().GetSessionVars().User.Username == spec.User.Username {
				return nil
			}
		}
	}
	return exeerrors.ErrMustChangePassword.GenWithStackByArgs()
}

func (e *SimpleExec) executeGrantRole(ctx context.Context, s *ast.GrantRoleStmt) error {
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)

	e.setCurrentUser(s.Users)

	for _, role := range s.Roles {
		exists, err := userExists(ctx, e.Ctx(), role.Username, role.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return exeerrors.ErrGrantRole.GenWithStackByArgs(role.String())
		}
	}
	for _, user := range s.Users {
		exists, err := userExists(ctx, e.Ctx(), user.Username, user.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return exeerrors.ErrCannotUser.GenWithStackByArgs("GRANT ROLE", user.String())
		}
	}

	restrictedCtx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(internalCtx, restrictedCtx)
	sqlExecutor := restrictedCtx.GetSQLExecutor()

	// begin a transaction to insert role graph edges.
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "begin"); err != nil {
		return err
	}

	sql := new(strings.Builder)
	for _, user := range s.Users {
		for _, role := range s.Roles {
			sql.Reset()
			sqlescape.MustFormatSQL(sql, `INSERT IGNORE INTO %n.%n (FROM_HOST, FROM_USER, TO_HOST, TO_USER) VALUES (%?,%?,%?,%?)`, mysql.SystemDB, mysql.RoleEdgeTable, role.Hostname, role.Username, user.Hostname, user.Username)
			if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
				logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
				if _, err := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); err != nil {
					return err
				}
				return exeerrors.ErrCannotUser.GenWithStackByArgs("GRANT ROLE", user.String())
			}
		}
	}
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "commit"); err != nil {
		return err
	}
	return domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege()
}

// Should cover same internal mysql.* tables as DROP USER, so this function is very similar
func (e *SimpleExec) executeRenameUser(s *ast.RenameUserStmt) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	var failedUser string
	sysSession, err := e.GetSysSession()
	defer e.ReleaseSysSession(ctx, sysSession)
	if err != nil {
		return err
	}
	sqlExecutor := sysSession.GetSQLExecutor()

	if _, err := sqlExecutor.ExecuteInternal(ctx, "BEGIN PESSIMISTIC"); err != nil {
		return err
	}
	for _, userToUser := range s.UserToUsers {
		oldUser, newUser := userToUser.OldUser, userToUser.NewUser
		if len(newUser.Username) > auth.UserNameMaxLength {
			return exeerrors.ErrWrongStringLength.GenWithStackByArgs(newUser.Username, "user name", auth.UserNameMaxLength)
		}
		if len(newUser.Hostname) > auth.HostNameMaxLength {
			return exeerrors.ErrWrongStringLength.GenWithStackByArgs(newUser.Hostname, "host name", auth.HostNameMaxLength)
		}
		exists, err := userExistsInternal(ctx, sqlExecutor, oldUser.Username, oldUser.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			failedUser = oldUser.String() + " TO " + newUser.String() + " old did not exist"
			break
		}

		exists, err = userExistsInternal(ctx, sqlExecutor, newUser.Username, newUser.Hostname)
		if err != nil {
			return err
		}
		if exists {
			// MySQL reports the old user, even when the issue is the new user.
			failedUser = oldUser.String() + " TO " + newUser.String() + " new did exist"
			break
		}

		if err = renameUserHostInSystemTable(sqlExecutor, mysql.UserTable, "User", "Host", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.UserTable + " error"
			break
		}

		// rename privileges from mysql.global_priv
		if err = renameUserHostInSystemTable(sqlExecutor, mysql.GlobalPrivTable, "User", "Host", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.GlobalPrivTable + " error"
			break
		}

		// rename privileges from mysql.db
		if err = renameUserHostInSystemTable(sqlExecutor, mysql.DBTable, "User", "Host", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.DBTable + " error"
			break
		}

		// rename privileges from mysql.tables_priv
		if err = renameUserHostInSystemTable(sqlExecutor, mysql.TablePrivTable, "User", "Host", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.TablePrivTable + " error"
			break
		}

		// rename relationship from mysql.role_edges
		if err = renameUserHostInSystemTable(sqlExecutor, mysql.RoleEdgeTable, "TO_USER", "TO_HOST", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.RoleEdgeTable + " (to) error"
			break
		}

		if err = renameUserHostInSystemTable(sqlExecutor, mysql.RoleEdgeTable, "FROM_USER", "FROM_HOST", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.RoleEdgeTable + " (from) error"
			break
		}

		// rename relationship from mysql.default_roles
		if err = renameUserHostInSystemTable(sqlExecutor, mysql.DefaultRoleTable, "DEFAULT_ROLE_USER", "DEFAULT_ROLE_HOST", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.DefaultRoleTable + " (default role user) error"
			break
		}

		if err = renameUserHostInSystemTable(sqlExecutor, mysql.DefaultRoleTable, "USER", "HOST", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.DefaultRoleTable + " error"
			break
		}

		// rename passwordhistory from  PasswordHistoryTable.
		if err = renameUserHostInSystemTable(sqlExecutor, mysql.PasswordHistoryTable, "USER", "HOST", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.PasswordHistoryTable + " error"
			break
		}

		// rename relationship from mysql.global_grants
		// TODO: add global_grants into the parser
		// TODO: need update columns_priv once we implement columns_priv functionality.
		// When that is added, please refactor both executeRenameUser and executeDropUser to use an array of tables
		// to loop over, so it is easier to maintain.
		if err = renameUserHostInSystemTable(sqlExecutor, "global_grants", "User", "Host", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " mysql.global_grants error"
			break
		}
	}

	if failedUser != "" {
		if _, err := sqlExecutor.ExecuteInternal(ctx, "rollback"); err != nil {
			return err
		}
		return exeerrors.ErrCannotUser.GenWithStackByArgs("RENAME USER", failedUser)
	}
	if _, err := sqlExecutor.ExecuteInternal(ctx, "commit"); err != nil {
		return err
	}
	return domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege()
}

func renameUserHostInSystemTable(sqlExecutor sqlexec.SQLExecutor, tableName, usernameColumn, hostColumn string, users *ast.UserToUser) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, `UPDATE %n.%n SET %n = %?, %n = %? WHERE %n = %? and %n = %?;`,
		mysql.SystemDB, tableName,
		usernameColumn, users.NewUser.Username, hostColumn, strings.ToLower(users.NewUser.Hostname),
		usernameColumn, users.OldUser.Username, hostColumn, strings.ToLower(users.OldUser.Hostname))
	_, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	return err
}

func (e *SimpleExec) executeDropQueryWatch(s *ast.DropQueryWatchStmt) error {
	return querywatch.ExecDropQueryWatch(e.Ctx(), s.IntValue)
}

func (e *SimpleExec) executeDropUser(ctx context.Context, s *ast.DropUserStmt) error {
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	// Check privileges.
	// Check `CREATE USER` privilege.
	checker := privilege.GetPrivilegeManager(e.Ctx())
	if checker == nil {
		return errors.New("miss privilege checker")
	}
	activeRoles := e.Ctx().GetSessionVars().ActiveRoles
	if !checker.RequestVerification(activeRoles, mysql.SystemDB, mysql.UserTable, "", mysql.DeletePriv) {
		if s.IsDropRole {
			if !checker.RequestVerification(activeRoles, "", "", "", mysql.DropRolePriv) &&
				!checker.RequestVerification(activeRoles, "", "", "", mysql.CreateUserPriv) {
				return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("DROP ROLE or CREATE USER")
			}
		}
		if !s.IsDropRole && !checker.RequestVerification(activeRoles, "", "", "", mysql.CreateUserPriv) {
			return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
		}
	}
	hasSystemUserPriv := checker.RequestDynamicVerification(activeRoles, "SYSTEM_USER", false)
	hasRestrictedUserPriv := checker.RequestDynamicVerification(activeRoles, "RESTRICTED_USER_ADMIN", false)
	failedUsers := make([]string, 0, len(s.UserList))
	sysSession, err := e.GetSysSession()
	defer e.ReleaseSysSession(internalCtx, sysSession)
	if err != nil {
		return err
	}
	sqlExecutor := sysSession.GetSQLExecutor()

	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "begin"); err != nil {
		return err
	}

	sql := new(strings.Builder)
	for _, user := range s.UserList {
		exists, err := userExists(ctx, e.Ctx(), user.Username, user.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			if !s.IfExists {
				failedUsers = append(failedUsers, user.String())
				break
			}
			e.Ctx().GetSessionVars().StmtCtx.AppendNote(infoschema.ErrUserDropExists.FastGenByArgs(user))
		}

		// Certain users require additional privileges in order to be modified.
		// If this is the case, we need to rollback all changes and return a privilege error.
		// Because in TiDB SUPER can be used as a substitute for any dynamic privilege, this effectively means that
		// any user with SUPER requires a user with SUPER to be able to DROP the user.
		// We also allow RESTRICTED_USER_ADMIN to count for simplicity.
		if checker.RequestDynamicVerificationWithUser("SYSTEM_USER", false, user) && !(hasSystemUserPriv || hasRestrictedUserPriv) {
			if _, err := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); err != nil {
				return err
			}
			return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SYSTEM_USER or SUPER")
		}

		// begin a transaction to delete a user.
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, mysql.UserTable, strings.ToLower(user.Hostname), user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete password history from mysql.password_history.
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, mysql.PasswordHistoryTable, strings.ToLower(user.Hostname), user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete privileges from mysql.global_priv
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, mysql.GlobalPrivTable, user.Hostname, user.Username)
		if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			if _, err := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); err != nil {
				return err
			}
			continue
		}

		// delete privileges from mysql.db
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, mysql.DBTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete privileges from mysql.tables_priv
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, mysql.TablePrivTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete privileges from mysql.columns_priv
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, mysql.ColumnPrivTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete relationship from mysql.role_edges
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE TO_HOST = %? and TO_USER = %?;`, mysql.SystemDB, mysql.RoleEdgeTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE FROM_HOST = %? and FROM_USER = %?;`, mysql.SystemDB, mysql.RoleEdgeTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete relationship from mysql.default_roles
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE DEFAULT_ROLE_HOST = %? and DEFAULT_ROLE_USER = %?;`, mysql.SystemDB, mysql.DefaultRoleTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE HOST = %? and USER = %?;`, mysql.SystemDB, mysql.DefaultRoleTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete relationship from mysql.global_grants
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, "global_grants", user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete from activeRoles
		if s.IsDropRole {
			for i := 0; i < len(activeRoles); i++ {
				if activeRoles[i].Username == user.Username && activeRoles[i].Hostname == user.Hostname {
					activeRoles = append(activeRoles[:i], activeRoles[i+1:]...)
					break
				}
			}
		} // TODO: need delete columns_priv once we implement columns_priv functionality.
	}

	if len(failedUsers) != 0 {
		if _, err := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); err != nil {
			return err
		}
		if s.IsDropRole {
			return exeerrors.ErrCannotUser.GenWithStackByArgs("DROP ROLE", strings.Join(failedUsers, ","))
		}
		return exeerrors.ErrCannotUser.GenWithStackByArgs("DROP USER", strings.Join(failedUsers, ","))
	}
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "commit"); err != nil {
		return err
	}
	if s.IsDropRole {
		// apply new activeRoles
		if ok, roleName := checker.ActiveRoles(e.Ctx(), activeRoles); !ok {
			u := e.Ctx().GetSessionVars().User
			return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
		}
	}
	return domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege()
}

func userExists(ctx context.Context, sctx sessionctx.Context, name string, host string) (bool, error) {
	exec := sctx.GetRestrictedSQLExecutor()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnPrivilege)
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, `SELECT * FROM %n.%n WHERE User=%? AND Host=%?;`, mysql.SystemDB, mysql.UserTable, name, strings.ToLower(host))
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}

// use the same internal executor to read within the same transaction, otherwise same as userExists
func userExistsInternal(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, name string, host string) (bool, error) {
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, `SELECT * FROM %n.%n WHERE User=%? AND Host=%? FOR UPDATE;`, mysql.SystemDB, mysql.UserTable, name, strings.ToLower(host))
	recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	if err != nil {
		return false, err
	}
	req := recordSet.NewChunk(nil)
	err = recordSet.Next(ctx, req)
	var rows = 0
	if err == nil {
		rows = req.NumRows()
	}
	errClose := recordSet.Close()
	if errClose != nil {
		return false, errClose
	}
	return rows > 0, err
}

func (e *SimpleExec) executeSetPwd(ctx context.Context, s *ast.SetPwdStmt) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnPrivilege)
	sysSession, err := e.GetSysSession()
	defer clearSysSession(ctx, sysSession)
	if err != nil {
		return err
	}

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
			return exeerrors.ErrDBaccessDenied.GenWithStackByArgs(u, h, "mysql")
		}
	}
	exists, err := userExistsInternal(ctx, sqlExecutor, u, h)
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

	authplugin, err := privilege.GetPrivilegeManager(e.Ctx()).GetAuthPlugin(u, h)
	if err != nil {
		return err
	}
	if e.isValidatePasswordEnabled() {
		if err := pwdValidator.ValidatePassword(e.Ctx().GetSessionVars(), s.Password); err != nil {
			return err
		}
	}
	var pwd string
	switch authplugin {
	case mysql.AuthCachingSha2Password, mysql.AuthTiDBSM3Password:
		pwd = auth.NewHashPassword(s.Password, authplugin)
	case mysql.AuthSocket:
		e.Ctx().GetSessionVars().StmtCtx.AppendNote(exeerrors.ErrSetPasswordAuthPlugin.FastGenByArgs(u, h))
		pwd = ""
	default:
		pwd = auth.EncodePassword(s.Password)
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
		err := checkPasswordReusePolicy(ctx, sqlExecutor, userDetail, e.Ctx(), authplugin)
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
	err = domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege()
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
			sm.Kill(e.Ctx().GetSessionVars().ConnectionID, s.Query, false)
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
			sm.Kill(s.ConnectionID, s.Query, false)
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
		sm.Kill(s.ConnectionID, s.Query, false)
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
		sm.Kill(s.ConnectionID, s.Query, false)
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

func (e *SimpleExec) executeFlush(s *ast.FlushStmt) error {
	switch s.Tp {
	case ast.FlushTables:
		if s.ReadLock {
			return errors.New("FLUSH TABLES WITH READ LOCK is not supported.  Please use @@tidb_snapshot")
		}
	case ast.FlushPrivileges:
		dom := domain.GetDomain(e.Ctx())
		return dom.NotifyUpdatePrivilege()
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
	}
	return nil
}

func (e *SimpleExec) executeAlterInstance(s *ast.AlterInstanceStmt) error {
	if s.ReloadTLS {
		logutil.BgLogger().Info("execute reload tls", zap.Bool("NoRollbackOnError", s.NoRollbackOnError))
		sm := e.Ctx().GetSessionManager()
		tlsCfg, _, err := util.LoadTLSCertificates(
			variable.GetSysVar("ssl_ca").Value,
			variable.GetSysVar("ssl_key").Value,
			variable.GetSysVar("ssl_cert").Value,
			config.GetGlobalConfig().Security.AutoTLS,
			config.GetGlobalConfig().Security.RSAKeySize,
		)
		if err != nil {
			if !s.NoRollbackOnError || tls.RequireSecureTransport.Load() {
				return err
			}
			logutil.BgLogger().Warn("reload TLS fail but keep working without TLS due to 'no rollback on error'")
		}
		sm.UpdateTLSConfig(tlsCfg)
	}
	return nil
}

func (e *SimpleExec) executeDropStats(s *ast.DropStatsStmt) (err error) {
	h := domain.GetDomain(e.Ctx()).StatsHandle()
	var statsIDs []int64
	// TODO: GLOBAL option will be deprecated. Also remove this condition when the syntax is removed
	if s.IsGlobalStats {
		statsIDs = []int64{s.Tables[0].TableInfo.ID}
	} else {
		if len(s.PartitionNames) == 0 {
			for _, table := range s.Tables {
				partitionStatIDs, _, err := core.GetPhysicalIDsAndPartitionNames(table.TableInfo, nil)
				if err != nil {
					return err
				}
				statsIDs = append(statsIDs, partitionStatIDs...)
				statsIDs = append(statsIDs, table.TableInfo.ID)
			}
		} else {
			// TODO: drop stats for specific partition is deprecated. Also remove this condition when the syntax is removed
			if statsIDs, _, err = core.GetPhysicalIDsAndPartitionNames(s.Tables[0].TableInfo, s.PartitionNames); err != nil {
				return err
			}
		}
	}
	if err := h.DeleteTableStatsFromKV(statsIDs); err != nil {
		return err
	}
	return h.Update(e.Ctx().GetInfoSchema().(infoschema.InfoSchema))
}

func (e *SimpleExec) autoNewTxn() bool {
	// Some statements cause an implicit commit
	// See https://dev.mysql.com/doc/refman/5.7/en/implicit-commit.html
	switch e.Statement.(type) {
	// Data definition language (DDL) statements that define or modify database objects.
	// (handled in DDL package)
	// Statements that implicitly use or modify tables in the mysql database.
	case *ast.CreateUserStmt, *ast.AlterUserStmt, *ast.DropUserStmt, *ast.RenameUserStmt, *ast.RevokeRoleStmt, *ast.GrantRoleStmt:
		return true
	// Transaction-control and locking statements.  BEGIN, LOCK TABLES, SET autocommit = 1 (if the value is not already 1), START TRANSACTION, UNLOCK TABLES.
	// (handled in other place)
	// Data loading statements. LOAD DATA
	// (handled in other place)
	// Administrative statements. TODO: ANALYZE TABLE, CACHE INDEX, CHECK TABLE, FLUSH, LOAD INDEX INTO CACHE, OPTIMIZE TABLE, REPAIR TABLE, RESET (but not RESET PERSIST).
	case *ast.FlushStmt:
		return true
	}
	return false
}

func (e *SimpleExec) executeShutdown() error {
	sessVars := e.Ctx().GetSessionVars()
	logutil.BgLogger().Info("execute shutdown statement", zap.Uint64("conn", sessVars.ConnectionID))
	p, err := os.FindProcess(os.Getpid())
	if err != nil {
		return err
	}

	// Call with async
	go asyncDelayShutdown(p, time.Second)

	return nil
}

// #14239 - https://github.com/pingcap/tidb/issues/14239
// Need repair 'shutdown' command behavior.
// Response of TiDB is different to MySQL.
// This function need to run with async model, otherwise it will block main coroutine
func asyncDelayShutdown(p *os.Process, delay time.Duration) {
	time.Sleep(delay)
	// Send SIGTERM instead of SIGKILL to allow graceful shutdown and cleanups to work properly.
	err := p.Signal(syscall.SIGTERM)
	if err != nil {
		panic(err)
	}

	// Sending SIGKILL should not be needed as SIGTERM should cause a graceful shutdown after
	// n seconds as configured by the GracefulWaitBeforeShutdown. This is here in case that doesn't
	// work for some reason.
	graceTime := config.GetGlobalConfig().GracefulWaitBeforeShutdown

	// The shutdown is supposed to start at graceTime and is allowed to take up to 10s.
	time.Sleep(time.Second * time.Duration(graceTime+10))
	logutil.BgLogger().Info("Killing process as grace period is over", zap.Int("pid", p.Pid), zap.Int("graceTime", graceTime))
	err = p.Kill()
	if err != nil {
		panic(err)
	}
}

func (e *SimpleExec) executeSetSessionStates(ctx context.Context, s *ast.SetSessionStatesStmt) error {
	var sessionStates sessionstates.SessionStates
	decoder := json.NewDecoder(bytes.NewReader([]byte(s.SessionStates)))
	decoder.UseNumber()
	if err := decoder.Decode(&sessionStates); err != nil {
		return errors.Trace(err)
	}
	return e.Ctx().DecodeSessionStates(ctx, e.Ctx(), &sessionStates)
}

func (e *SimpleExec) executeAdmin(s *ast.AdminStmt) error {
	switch s.Tp {
	case ast.AdminReloadStatistics:
		return e.executeAdminReloadStatistics(s)
	case ast.AdminFlushPlanCache:
		return e.executeAdminFlushPlanCache(s)
	case ast.AdminSetBDRRole:
		return e.executeAdminSetBDRRole(s)
	case ast.AdminUnsetBDRRole:
		return e.executeAdminUnsetBDRRole()
	}
	return nil
}

func (e *SimpleExec) executeAdminReloadStatistics(s *ast.AdminStmt) error {
	if s.Tp != ast.AdminReloadStatistics {
		return errors.New("This AdminStmt is not ADMIN RELOAD STATS_EXTENDED")
	}
	if !e.Ctx().GetSessionVars().EnableExtendedStats {
		return errors.New("Extended statistics feature is not generally available now, and tidb_enable_extended_stats is OFF")
	}
	return domain.GetDomain(e.Ctx()).StatsHandle().ReloadExtendedStatistics()
}

func (e *SimpleExec) executeAdminFlushPlanCache(s *ast.AdminStmt) error {
	if s.Tp != ast.AdminFlushPlanCache {
		return errors.New("This AdminStmt is not ADMIN FLUSH PLAN_CACHE")
	}
	if s.StatementScope == ast.StatementScopeGlobal {
		return errors.New("Do not support the 'admin flush global scope.'")
	}
	if !e.Ctx().GetSessionVars().EnablePreparedPlanCache {
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("The plan cache is disable. So there no need to flush the plan cache"))
		return nil
	}
	now := types.NewTime(types.FromGoTime(time.Now().In(e.Ctx().GetSessionVars().StmtCtx.TimeZone())), mysql.TypeTimestamp, 3)
	e.Ctx().GetSessionVars().LastUpdateTime4PC = now
	e.Ctx().GetSessionPlanCache().DeleteAll()
	if s.StatementScope == ast.StatementScopeInstance {
		// Record the timestamp. When other sessions want to use the plan cache,
		// it will check the timestamp first to decide whether the plan cache should be flushed.
		domain.GetDomain(e.Ctx()).SetExpiredTimeStamp4PC(now)
	}
	return nil
}

func (e *SimpleExec) executeAdminSetBDRRole(s *ast.AdminStmt) error {
	if s.Tp != ast.AdminSetBDRRole {
		return errors.New("This AdminStmt is not ADMIN SET BDR_ROLE")
	}

	txn, err := e.Ctx().Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(meta.NewMeta(txn).SetBDRRole(string(s.BDRRole)))
}

func (e *SimpleExec) executeAdminUnsetBDRRole() error {
	txn, err := e.Ctx().Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(meta.NewMeta(txn).ClearBDRRole())
}

func (e *SimpleExec) executeSetResourceGroupName(s *ast.SetResourceGroupStmt) error {
	originalResourceGroup := e.Ctx().GetSessionVars().ResourceGroupName
	if s.Name.L != "" {
		if _, ok := e.is.ResourceGroupByName(s.Name); !ok {
			return infoschema.ErrResourceGroupNotExists.GenWithStackByArgs(s.Name.O)
		}
		e.Ctx().GetSessionVars().ResourceGroupName = s.Name.L
	} else {
		e.Ctx().GetSessionVars().ResourceGroupName = resourcegroup.DefaultResourceGroupName
	}
	newResourceGroup := e.Ctx().GetSessionVars().ResourceGroupName
	if originalResourceGroup != newResourceGroup {
		metrics.ConnGauge.WithLabelValues(originalResourceGroup).Dec()
		metrics.ConnGauge.WithLabelValues(newResourceGroup).Inc()
	}
	return nil
}

// executeAlterRange is used to alter range configuration. currently, only config placement policy.
func (e *SimpleExec) executeAlterRange(s *ast.AlterRangeStmt) error {
	if s.RangeName.L != placement.KeyRangeGlobal && s.RangeName.L != placement.KeyRangeMeta {
		return errors.New("range name is not supported")
	}
	if s.PlacementOption.Tp != ast.PlacementOptionPolicy {
		return errors.New("only support alter range policy")
	}
	bundle := &placement.Bundle{}
	policyName := model.NewCIStr(s.PlacementOption.StrValue)
	if policyName.L != placement.DefaultKwd {
		policy, ok := e.is.PolicyByName(policyName)
		if !ok {
			return infoschema.ErrPlacementPolicyNotExists.GenWithStackByArgs(policyName.O)
		}
		tmpBundle, err := placement.NewBundleFromOptions(policy.PlacementSettings)
		if err != nil {
			return err
		}
		// reset according range
		bundle = tmpBundle.RebuildForRange(s.RangeName.L, policyName.L)
	} else {
		// delete all rules
		bundle = bundle.RebuildForRange(s.RangeName.L, policyName.L)
		bundle = &placement.Bundle{ID: bundle.ID}
	}

	return infosync.PutRuleBundlesWithDefaultRetry(context.Background(), []*placement.Bundle{bundle})
}
