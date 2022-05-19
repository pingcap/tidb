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
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/core"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sem"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tidb/util/tls"
	"github.com/pingcap/tipb/go-tipb"
	tikvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

var (
	transactionDurationPessimisticRollback = metrics.TransactionDuration.WithLabelValues(metrics.LblPessimistic, metrics.LblRollback)
	transactionDurationOptimisticRollback  = metrics.TransactionDuration.WithLabelValues(metrics.LblOptimistic, metrics.LblRollback)
)

// SimpleExec represents simple statement executor.
// For statements do simple execution.
// includes `UseStmt`, 'SetStmt`, `DoStmt`,
// `BeginStmt`, `CommitStmt`, `RollbackStmt`.
// TODO: list all simple statements.
type SimpleExec struct {
	baseExecutor

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

func (e *baseExecutor) getSysSession() (sessionctx.Context, error) {
	dom := domain.GetDomain(e.ctx)
	sysSessionPool := dom.SysSessionPool()
	ctx, err := sysSessionPool.Get()
	if err != nil {
		return nil, err
	}
	restrictedCtx := ctx.(sessionctx.Context)
	restrictedCtx.GetSessionVars().InRestrictedSQL = true
	return restrictedCtx, nil
}

func (e *baseExecutor) releaseSysSession(ctx sessionctx.Context) {
	if ctx == nil {
		return
	}
	dom := domain.GetDomain(e.ctx)
	sysSessionPool := dom.SysSessionPool()
	if _, err := ctx.(sqlexec.SQLExecutor).ExecuteInternal(context.TODO(), "rollback"); err != nil {
		ctx.(pools.Resource).Close()
		return
	}
	sysSessionPool.Put(ctx.(pools.Resource))
}

// Next implements the Executor Next interface.
func (e *SimpleExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if e.done {
		return nil
	}

	if e.autoNewTxn() {
		// Commit the old transaction, like DDL.
		if err := sessiontxn.NewTxnInStmt(ctx, e.ctx); err != nil {
			return err
		}
		defer func() { e.ctx.GetSessionVars().SetInTxn(false) }()
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
		e.executeCommit(x)
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
		err = e.executeShutdown(x)
	case *ast.AdminStmt:
		err = e.executeAdmin(x)
	}
	e.done = true
	return err
}

func (e *SimpleExec) setDefaultRoleNone(s *ast.SetDefaultRoleStmt) error {
	restrictedCtx, err := e.getSysSession()
	if err != nil {
		return err
	}
	defer e.releaseSysSession(restrictedCtx)
	sqlExecutor := restrictedCtx.(sqlexec.SQLExecutor)
	if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "begin"); err != nil {
		return err
	}
	sql := new(strings.Builder)
	for _, u := range s.UserList {
		if u.Hostname == "" {
			u.Hostname = "%"
		}
		sql.Reset()
		sqlexec.MustFormatSQL(sql, "DELETE IGNORE FROM mysql.default_roles WHERE USER=%? AND HOST=%?;", u.Username, u.Hostname)
		if _, err := sqlExecutor.ExecuteInternal(context.TODO(), sql.String()); err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
			if _, rollbackErr := sqlExecutor.ExecuteInternal(context.TODO(), "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
	}
	if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "commit"); err != nil {
		return err
	}
	return nil
}

func (e *SimpleExec) setDefaultRoleRegular(ctx context.Context, s *ast.SetDefaultRoleStmt) error {
	for _, user := range s.UserList {
		exists, err := userExists(ctx, e.ctx, user.Username, user.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return ErrCannotUser.GenWithStackByArgs("SET DEFAULT ROLE", user.String())
		}
	}
	for _, role := range s.RoleList {
		exists, err := userExists(ctx, e.ctx, role.Username, role.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return ErrCannotUser.GenWithStackByArgs("SET DEFAULT ROLE", role.String())
		}
	}

	restrictedCtx, err := e.getSysSession()
	if err != nil {
		return err
	}
	defer e.releaseSysSession(restrictedCtx)
	sqlExecutor := restrictedCtx.(sqlexec.SQLExecutor)
	if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "begin"); err != nil {
		return err
	}
	sql := new(strings.Builder)
	for _, user := range s.UserList {
		if user.Hostname == "" {
			user.Hostname = "%"
		}
		sql.Reset()
		sqlexec.MustFormatSQL(sql, "DELETE IGNORE FROM mysql.default_roles WHERE USER=%? AND HOST=%?;", user.Username, user.Hostname)
		if _, err := sqlExecutor.ExecuteInternal(context.TODO(), sql.String()); err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
			if _, rollbackErr := sqlExecutor.ExecuteInternal(context.TODO(), "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
		for _, role := range s.RoleList {
			checker := privilege.GetPrivilegeManager(e.ctx)
			ok := checker.FindEdge(e.ctx, role, user)
			if ok {
				sql.Reset()
				sqlexec.MustFormatSQL(sql, "INSERT IGNORE INTO mysql.default_roles values(%?, %?, %?, %?);", user.Hostname, user.Username, role.Hostname, role.Username)
				if _, err := sqlExecutor.ExecuteInternal(context.TODO(), sql.String()); err != nil {
					logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
					if _, rollbackErr := sqlExecutor.ExecuteInternal(context.TODO(), "rollback"); rollbackErr != nil {
						return rollbackErr
					}
					return err
				}
			} else {
				if _, rollbackErr := sqlExecutor.ExecuteInternal(context.TODO(), "rollback"); rollbackErr != nil {
					return rollbackErr
				}
				return ErrRoleNotGranted.GenWithStackByArgs(role.String(), user.String())
			}
		}
	}
	if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "commit"); err != nil {
		return err
	}
	return nil
}

func (e *SimpleExec) setDefaultRoleAll(ctx context.Context, s *ast.SetDefaultRoleStmt) error {
	for _, user := range s.UserList {
		exists, err := userExists(ctx, e.ctx, user.Username, user.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return ErrCannotUser.GenWithStackByArgs("SET DEFAULT ROLE", user.String())
		}
	}
	restrictedCtx, err := e.getSysSession()
	if err != nil {
		return err
	}
	defer e.releaseSysSession(restrictedCtx)
	sqlExecutor := restrictedCtx.(sqlexec.SQLExecutor)
	if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "begin"); err != nil {
		return err
	}
	sql := new(strings.Builder)
	for _, user := range s.UserList {
		if user.Hostname == "" {
			user.Hostname = "%"
		}
		sql.Reset()
		sqlexec.MustFormatSQL(sql, "DELETE IGNORE FROM mysql.default_roles WHERE USER=%? AND HOST=%?;", user.Username, user.Hostname)
		if _, err := sqlExecutor.ExecuteInternal(context.TODO(), sql.String()); err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
			if _, rollbackErr := sqlExecutor.ExecuteInternal(context.TODO(), "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
		sql.Reset()
		sqlexec.MustFormatSQL(sql, "INSERT IGNORE INTO mysql.default_roles(HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER) SELECT TO_HOST,TO_USER,FROM_HOST,FROM_USER FROM mysql.role_edges WHERE TO_HOST=%? AND TO_USER=%?;", user.Hostname, user.Username)
		if _, err := sqlExecutor.ExecuteInternal(context.TODO(), sql.String()); err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
			if _, rollbackErr := sqlExecutor.ExecuteInternal(context.TODO(), "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
	}
	if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "commit"); err != nil {
		return err
	}
	return nil
}

func (e *SimpleExec) setDefaultRoleForCurrentUser(s *ast.SetDefaultRoleStmt) (err error) {
	checker := privilege.GetPrivilegeManager(e.ctx)
	user := s.UserList[0]
	if user.Hostname == "" {
		user.Hostname = "%"
	}
	restrictedCtx, err := e.getSysSession()
	if err != nil {
		return err
	}
	defer e.releaseSysSession(restrictedCtx)
	sqlExecutor := restrictedCtx.(sqlexec.SQLExecutor)

	if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "begin"); err != nil {
		return err
	}

	sql := new(strings.Builder)
	sqlexec.MustFormatSQL(sql, "DELETE IGNORE FROM mysql.default_roles WHERE USER=%? AND HOST=%?;", user.Username, user.Hostname)
	if _, err := sqlExecutor.ExecuteInternal(context.TODO(), sql.String()); err != nil {
		logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
		if _, rollbackErr := sqlExecutor.ExecuteInternal(context.TODO(), "rollback"); rollbackErr != nil {
			return rollbackErr
		}
		return err
	}

	sql.Reset()
	switch s.SetRoleOpt {
	case ast.SetRoleNone:
		sqlexec.MustFormatSQL(sql, "DELETE IGNORE FROM mysql.default_roles WHERE USER=%? AND HOST=%?;", user.Username, user.Hostname)
	case ast.SetRoleAll:
		sqlexec.MustFormatSQL(sql, "INSERT IGNORE INTO mysql.default_roles(HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER) SELECT TO_HOST,TO_USER,FROM_HOST,FROM_USER FROM mysql.role_edges WHERE TO_HOST=%? AND TO_USER=%?;", user.Hostname, user.Username)
	case ast.SetRoleRegular:
		sqlexec.MustFormatSQL(sql, "INSERT IGNORE INTO mysql.default_roles values")
		for i, role := range s.RoleList {
			if i > 0 {
				sqlexec.MustFormatSQL(sql, ",")
			}
			ok := checker.FindEdge(e.ctx, role, user)
			if !ok {
				return ErrRoleNotGranted.GenWithStackByArgs(role.String(), user.String())
			}
			sqlexec.MustFormatSQL(sql, "(%?, %?, %?, %?)", user.Hostname, user.Username, role.Hostname, role.Username)
		}
	}

	if _, err := sqlExecutor.ExecuteInternal(context.TODO(), sql.String()); err != nil {
		logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
		if _, rollbackErr := sqlExecutor.ExecuteInternal(context.TODO(), "rollback"); rollbackErr != nil {
			return rollbackErr
		}
		return err
	}
	if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "commit"); err != nil {
		return err
	}
	return nil
}

func (e *SimpleExec) executeSetDefaultRole(ctx context.Context, s *ast.SetDefaultRoleStmt) (err error) {
	sessionVars := e.ctx.GetSessionVars()
	checker := privilege.GetPrivilegeManager(e.ctx)
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
			return domain.GetDomain(e.ctx).NotifyUpdatePrivilege()
		}
	}

	activeRoles := sessionVars.ActiveRoles
	if !checker.RequestVerification(activeRoles, mysql.SystemDB, mysql.DefaultRoleTable, "", mysql.UpdatePriv) {
		if !checker.RequestVerification(activeRoles, "", "", "", mysql.CreateUserPriv) {
			return core.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
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
	return domain.GetDomain(e.ctx).NotifyUpdatePrivilege()
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

	checker := privilege.GetPrivilegeManager(e.ctx)
	ok, roleName := checker.ActiveRoles(e.ctx, roleList)
	if !ok {
		u := e.ctx.GetSessionVars().User
		return ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return nil
}

func (e *SimpleExec) setRoleAll(s *ast.SetRoleStmt) error {
	// Deal with SQL like `SET ROLE ALL;`
	checker := privilege.GetPrivilegeManager(e.ctx)
	user, host := e.ctx.GetSessionVars().User.AuthUsername, e.ctx.GetSessionVars().User.AuthHostname
	roles := checker.GetAllRoles(user, host)
	ok, roleName := checker.ActiveRoles(e.ctx, roles)
	if !ok {
		u := e.ctx.GetSessionVars().User
		return ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
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
	checker := privilege.GetPrivilegeManager(e.ctx)
	user, host := e.ctx.GetSessionVars().User.AuthUsername, e.ctx.GetSessionVars().User.AuthHostname
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
	ok, roleName := checker.ActiveRoles(e.ctx, afterExcept)
	if !ok {
		u := e.ctx.GetSessionVars().User
		return ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return nil
}

func (e *SimpleExec) setRoleDefault(s *ast.SetRoleStmt) error {
	// Deal with SQL like `SET ROLE DEFAULT;`
	checker := privilege.GetPrivilegeManager(e.ctx)
	user, host := e.ctx.GetSessionVars().User.AuthUsername, e.ctx.GetSessionVars().User.AuthHostname
	roles := checker.GetDefaultRoles(user, host)
	ok, roleName := checker.ActiveRoles(e.ctx, roles)
	if !ok {
		u := e.ctx.GetSessionVars().User
		return ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return nil
}

func (e *SimpleExec) setRoleNone(s *ast.SetRoleStmt) error {
	// Deal with SQL like `SET ROLE NONE;`
	checker := privilege.GetPrivilegeManager(e.ctx)
	roles := make([]*auth.RoleIdentity, 0)
	ok, roleName := checker.ActiveRoles(e.ctx, roles)
	if !ok {
		u := e.ctx.GetSessionVars().User
		return ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return nil
}

func (e *SimpleExec) executeSetRole(s *ast.SetRoleStmt) error {
	switch s.SetRoleOpt {
	case ast.SetRoleRegular:
		return e.setRoleRegular(s)
	case ast.SetRoleAll:
		return e.setRoleAll(s)
	case ast.SetRoleAllExcept:
		return e.setRoleAllExcept(s)
	case ast.SetRoleNone:
		return e.setRoleNone(s)
	case ast.SetRoleDefault:
		return e.setRoleDefault(s)
	}
	return nil
}

func (e *SimpleExec) dbAccessDenied(dbname string) error {
	user := e.ctx.GetSessionVars().User
	u := user.Username
	h := user.Hostname
	if len(user.AuthUsername) > 0 && len(user.AuthHostname) > 0 {
		u = user.AuthUsername
		h = user.AuthHostname
	}
	return ErrDBaccessDenied.GenWithStackByArgs(u, h, dbname)
}

func (e *SimpleExec) executeUse(s *ast.UseStmt) error {
	dbname := model.NewCIStr(s.DBName)

	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker != nil && e.ctx.GetSessionVars().User != nil {
		if !checker.DBIsVisible(e.ctx.GetSessionVars().ActiveRoles, dbname.String()) {
			return e.dbAccessDenied(dbname.O)
		}
	}

	dbinfo, exists := e.is.SchemaByName(dbname)
	if !exists {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbname)
	}
	e.ctx.GetSessionVars().CurrentDBChanged = dbname.O != e.ctx.GetSessionVars().CurrentDB
	e.ctx.GetSessionVars().CurrentDB = dbname.O
	sessionVars := e.ctx.GetSessionVars()
	dbCollate := dbinfo.Collate
	if dbCollate == "" {
		dbCollate = getDefaultCollate(dbinfo.Charset)
	}
	// If new collations are enabled, switch to the default
	// collation if this one is not supported.
	// The SetSystemVar will also update the CharsetDatabase
	dbCollate = collate.SubstituteMissingCollationToDefault(dbCollate)
	return sessionVars.SetSystemVar(variable.CollationDatabase, dbCollate)
}

func (e *SimpleExec) executeBegin(ctx context.Context, s *ast.BeginStmt) error {
	// If `START TRANSACTION READ ONLY` is the first statement in TxnCtx, we should
	// always create a new Txn instead of reusing it.
	if s.ReadOnly {
		noopFuncsMode := e.ctx.GetSessionVars().NoopFuncsMode
		if s.AsOf == nil && noopFuncsMode != variable.OnInt {
			err := expression.ErrFunctionsNoopImpl.GenWithStackByArgs("READ ONLY")
			if noopFuncsMode == variable.OffInt {
				return err
			}
			e.ctx.GetSessionVars().StmtCtx.AppendWarning(err)
		}
		if s.AsOf != nil {
			// start transaction read only as of failed due to we set tx_read_ts before
			if e.ctx.GetSessionVars().TxnReadTS.PeakTxnReadTS() > 0 {
				return errors.New("start transaction read only as of is forbidden after set transaction read only as of")
			}
		}
	}

	return sessiontxn.GetTxnManager(e.ctx).EnterNewTxn(ctx, &sessiontxn.EnterNewTxnRequest{
		Type:                  sessiontxn.EnterNewTxnWithBeginStmt,
		TxnMode:               s.Mode,
		CausalConsistencyOnly: s.CausalConsistencyOnly,
		StaleReadTS:           e.staleTxnStartTS,
	})
}

func (e *SimpleExec) executeRevokeRole(ctx context.Context, s *ast.RevokeRoleStmt) error {
	for _, role := range s.Roles {
		exists, err := userExists(ctx, e.ctx, role.Username, role.Hostname)
		if err != nil {
			return errors.Trace(err)
		}
		if !exists {
			return ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", role.String())
		}
	}

	restrictedCtx, err := e.getSysSession()
	if err != nil {
		return err
	}
	defer e.releaseSysSession(restrictedCtx)
	sqlExecutor := restrictedCtx.(sqlexec.SQLExecutor)

	// begin a transaction to insert role graph edges.
	if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "begin"); err != nil {
		return errors.Trace(err)
	}
	sql := new(strings.Builder)
	// when an active role of current user is revoked,
	// it should be removed from activeRoles
	activeRoles, curUser, curHost := e.ctx.GetSessionVars().ActiveRoles, "", ""
	if user := e.ctx.GetSessionVars().User; user != nil {
		curUser, curHost = user.AuthUsername, user.AuthHostname
	}
	for _, user := range s.Users {
		exists, err := userExists(ctx, e.ctx, user.Username, user.Hostname)
		if err != nil {
			return errors.Trace(err)
		}
		if !exists {
			if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "rollback"); err != nil {
				return errors.Trace(err)
			}
			return ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", user.String())
		}
		for _, role := range s.Roles {
			if role.Hostname == "" {
				role.Hostname = "%"
			}
			sql.Reset()
			sqlexec.MustFormatSQL(sql, `DELETE IGNORE FROM %n.%n WHERE FROM_HOST=%? and FROM_USER=%? and TO_HOST=%? and TO_USER=%?`, mysql.SystemDB, mysql.RoleEdgeTable, role.Hostname, role.Username, user.Hostname, user.Username)
			if _, err := sqlExecutor.ExecuteInternal(context.TODO(), sql.String()); err != nil {
				if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "rollback"); err != nil {
					return errors.Trace(err)
				}
				return ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", role.String())
			}

			sql.Reset()
			sqlexec.MustFormatSQL(sql, `DELETE IGNORE FROM %n.%n WHERE DEFAULT_ROLE_HOST=%? and DEFAULT_ROLE_USER=%? and HOST=%? and USER=%?`, mysql.SystemDB, mysql.DefaultRoleTable, role.Hostname, role.Username, user.Hostname, user.Username)
			if _, err := sqlExecutor.ExecuteInternal(context.TODO(), sql.String()); err != nil {
				if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "rollback"); err != nil {
					return errors.Trace(err)
				}
				return ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", role.String())
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
	if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "commit"); err != nil {
		return err
	}
	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker == nil {
		return errors.New("miss privilege checker")
	}
	if ok, roleName := checker.ActiveRoles(e.ctx, activeRoles); !ok {
		u := e.ctx.GetSessionVars().User
		return ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return domain.GetDomain(e.ctx).NotifyUpdatePrivilege()
}

func (e *SimpleExec) executeCommit(s *ast.CommitStmt) {
	e.ctx.GetSessionVars().SetInTxn(false)
}

func (e *SimpleExec) executeRollback(s *ast.RollbackStmt) error {
	sessVars := e.ctx.GetSessionVars()
	logutil.BgLogger().Debug("execute rollback statement", zap.Uint64("conn", sessVars.ConnectionID))
	sessVars.SetInTxn(false)
	txn, err := e.ctx.Txn(false)
	if err != nil {
		return err
	}
	if txn.Valid() {
		duration := time.Since(sessVars.TxnCtx.CreateTime).Seconds()
		if sessVars.TxnCtx.IsPessimistic {
			transactionDurationPessimisticRollback.Observe(duration)
		} else {
			transactionDurationOptimisticRollback.Observe(duration)
		}
		sessVars.TxnCtx.ClearDelta()
		return txn.Rollback()
	}
	return nil
}

func (e *SimpleExec) executeCreateUser(ctx context.Context, s *ast.CreateUserStmt) error {
	// Check `CREATE USER` privilege.
	if !config.GetGlobalConfig().Security.SkipGrantTable {
		checker := privilege.GetPrivilegeManager(e.ctx)
		if checker == nil {
			return errors.New("miss privilege checker")
		}
		activeRoles := e.ctx.GetSessionVars().ActiveRoles
		if !checker.RequestVerification(activeRoles, mysql.SystemDB, mysql.UserTable, "", mysql.InsertPriv) {
			if s.IsCreateRole {
				if !checker.RequestVerification(activeRoles, "", "", "", mysql.CreateRolePriv) &&
					!checker.RequestVerification(activeRoles, "", "", "", mysql.CreateUserPriv) {
					return core.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE ROLE or CREATE USER")
				}
			}
			if !s.IsCreateRole && !checker.RequestVerification(activeRoles, "", "", "", mysql.CreateUserPriv) {
				return core.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE User")
			}
		}
	}

	privData, err := tlsOption2GlobalPriv(s.TLSOptions)
	if err != nil {
		return err
	}

	sql := new(strings.Builder)
	if s.IsCreateRole {
		sqlexec.MustFormatSQL(sql, `INSERT INTO %n.%n (Host, User, authentication_string, plugin, Account_locked) VALUES `, mysql.SystemDB, mysql.UserTable)
	} else {
		sqlexec.MustFormatSQL(sql, `INSERT INTO %n.%n (Host, User, authentication_string, plugin) VALUES `, mysql.SystemDB, mysql.UserTable)
	}

	users := make([]*auth.UserIdentity, 0, len(s.Specs))
	for _, spec := range s.Specs {
		if len(spec.User.Username) > auth.UserNameMaxLength {
			return ErrWrongStringLength.GenWithStackByArgs(spec.User.Username, "user name", auth.UserNameMaxLength)
		}
		if len(spec.User.Hostname) > auth.HostNameMaxLength {
			return ErrWrongStringLength.GenWithStackByArgs(spec.User.Hostname, "host name", auth.HostNameMaxLength)
		}
		if len(users) > 0 {
			sqlexec.MustFormatSQL(sql, ",")
		}
		exists, err1 := userExists(ctx, e.ctx, spec.User.Username, spec.User.Hostname)
		if err1 != nil {
			return err1
		}
		if exists {
			user := fmt.Sprintf(`'%s'@'%s'`, spec.User.Username, spec.User.Hostname)
			if !s.IfNotExists {
				if s.IsCreateRole {
					return ErrCannotUser.GenWithStackByArgs("CREATE ROLE", user)
				}
				return ErrCannotUser.GenWithStackByArgs("CREATE USER", user)
			}
			err := infoschema.ErrUserAlreadyExists.GenWithStackByArgs(user)
			e.ctx.GetSessionVars().StmtCtx.AppendNote(err)
			continue
		}
		pwd, ok := spec.EncodedPassword()

		if !ok {
			return errors.Trace(ErrPasswordFormat)
		}
		authPlugin := mysql.AuthNativePassword
		if spec.AuthOpt != nil && spec.AuthOpt.AuthPlugin != "" {
			authPlugin = spec.AuthOpt.AuthPlugin
		}

		switch authPlugin {
		case mysql.AuthNativePassword, mysql.AuthCachingSha2Password, mysql.AuthSocket:
		default:
			return ErrPluginIsNotLoaded.GenWithStackByArgs(spec.AuthOpt.AuthPlugin)
		}

		hostName := strings.ToLower(spec.User.Hostname)
		if s.IsCreateRole {
			sqlexec.MustFormatSQL(sql, `(%?, %?, %?, %?, %?)`, hostName, spec.User.Username, pwd, authPlugin, "Y")
		} else {
			sqlexec.MustFormatSQL(sql, `(%?, %?, %?, %?)`, hostName, spec.User.Username, pwd, authPlugin)
		}
		users = append(users, spec.User)
	}
	if len(users) == 0 {
		return nil
	}

	restrictedCtx, err := e.getSysSession()
	if err != nil {
		return err
	}
	defer e.releaseSysSession(restrictedCtx)
	sqlExecutor := restrictedCtx.(sqlexec.SQLExecutor)

	if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "begin"); err != nil {
		return errors.Trace(err)
	}
	_, err = sqlExecutor.ExecuteInternal(context.TODO(), sql.String())
	if err != nil {
		if _, rollbackErr := sqlExecutor.ExecuteInternal(context.TODO(), "rollback"); rollbackErr != nil {
			return rollbackErr
		}
		return err
	}
	if len(privData) != 0 {
		sql.Reset()
		sqlexec.MustFormatSQL(sql, "INSERT IGNORE INTO %n.%n (Host, User, Priv) VALUES ", mysql.SystemDB, mysql.GlobalPrivTable)
		for i, user := range users {
			if i > 0 {
				sqlexec.MustFormatSQL(sql, ",")
			}
			sqlexec.MustFormatSQL(sql, `(%?, %?, %?)`, user.Hostname, user.Username, string(hack.String(privData)))
		}
		_, err = sqlExecutor.ExecuteInternal(context.TODO(), sql.String())
		if err != nil {
			if _, rollbackErr := sqlExecutor.ExecuteInternal(context.TODO(), "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
	}
	if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "commit"); err != nil {
		return errors.Trace(err)
	}
	return domain.GetDomain(e.ctx).NotifyUpdatePrivilege()
}

func (e *SimpleExec) executeAlterUser(ctx context.Context, s *ast.AlterUserStmt) error {
	if s.CurrentAuth != nil {
		user := e.ctx.GetSessionVars().User
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

	privData, err := tlsOption2GlobalPriv(s.TLSOptions)
	if err != nil {
		return err
	}

	failedUsers := make([]string, 0, len(s.Specs))
	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker == nil {
		return errors.New("could not load privilege checker")
	}
	activeRoles := e.ctx.GetSessionVars().ActiveRoles
	hasCreateUserPriv := checker.RequestVerification(activeRoles, "", "", "", mysql.CreateUserPriv)
	hasSystemUserPriv := checker.RequestDynamicVerification(activeRoles, "SYSTEM_USER", false)
	hasRestrictedUserPriv := checker.RequestDynamicVerification(activeRoles, "RESTRICTED_USER_ADMIN", false)
	hasSystemSchemaPriv := checker.RequestVerification(activeRoles, mysql.SystemDB, mysql.UserTable, "", mysql.UpdatePriv)

	for _, spec := range s.Specs {
		user := e.ctx.GetSessionVars().User
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
				return plannercore.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
			}
			if checker.RequestDynamicVerificationWithUser("SYSTEM_USER", false, spec.User) && !(hasSystemUserPriv || hasRestrictedUserPriv) {
				return plannercore.ErrSpecificAccessDenied.GenWithStackByArgs("SYSTEM_USER or SUPER")
			}
			if sem.IsEnabled() && checker.RequestDynamicVerificationWithUser("RESTRICTED_USER_ADMIN", false, spec.User) && !hasRestrictedUserPriv {
				return plannercore.ErrSpecificAccessDenied.GenWithStackByArgs("RESTRICTED_USER_ADMIN")
			}
		}

		exists, err := userExists(ctx, e.ctx, spec.User.Username, spec.User.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			user := fmt.Sprintf(`'%s'@'%s'`, spec.User.Username, spec.User.Hostname)
			failedUsers = append(failedUsers, user)
			continue
		}

		exec := e.ctx.(sqlexec.RestrictedSQLExecutor)
		if spec.AuthOpt != nil {
			if spec.AuthOpt.AuthPlugin == "" {
				authplugin, err := e.userAuthPlugin(spec.User.Username, spec.User.Hostname)
				if err != nil {
					return err
				}
				spec.AuthOpt.AuthPlugin = authplugin
			}
			switch spec.AuthOpt.AuthPlugin {
			case mysql.AuthNativePassword, mysql.AuthCachingSha2Password, mysql.AuthSocket, "":
			default:
				return ErrPluginIsNotLoaded.GenWithStackByArgs(spec.AuthOpt.AuthPlugin)
			}
			pwd, ok := spec.EncodedPassword()
			if !ok {
				return errors.Trace(ErrPasswordFormat)
			}
			_, _, err := exec.ExecRestrictedSQL(ctx, nil,
				`UPDATE %n.%n SET authentication_string=%?, plugin=%? WHERE Host=%? and User=%?;`,
				mysql.SystemDB, mysql.UserTable, pwd, spec.AuthOpt.AuthPlugin, strings.ToLower(spec.User.Hostname), spec.User.Username,
			)
			if err != nil {
				failedUsers = append(failedUsers, spec.User.String())
			}
		}

		if len(privData) > 0 {
			_, _, err := exec.ExecRestrictedSQL(ctx, nil, "INSERT INTO %n.%n (Host, User, Priv) VALUES (%?,%?,%?) ON DUPLICATE KEY UPDATE Priv = values(Priv)", mysql.SystemDB, mysql.GlobalPrivTable, spec.User.Hostname, spec.User.Username, string(hack.String(privData)))
			if err != nil {
				failedUsers = append(failedUsers, spec.User.String())
			}
		}
	}
	if len(failedUsers) > 0 {
		// Commit the transaction even if we returns error
		txn, err := e.ctx.Txn(true)
		if err != nil {
			return err
		}
		err = txn.Commit(tikvutil.SetSessionID(context.TODO(), e.ctx.GetSessionVars().ConnectionID))
		if err != nil {
			return err
		}
		if !s.IfExists {
			return ErrCannotUser.GenWithStackByArgs("ALTER USER", strings.Join(failedUsers, ","))
		}
		for _, user := range failedUsers {
			err := infoschema.ErrUserDropExists.GenWithStackByArgs(user)
			e.ctx.GetSessionVars().StmtCtx.AppendNote(err)
		}
	}
	return domain.GetDomain(e.ctx).NotifyUpdatePrivilege()
}

func (e *SimpleExec) executeGrantRole(ctx context.Context, s *ast.GrantRoleStmt) error {
	sessionVars := e.ctx.GetSessionVars()
	for i, user := range s.Users {
		if user.CurrentUser {
			s.Users[i].Username = sessionVars.User.AuthUsername
			s.Users[i].Hostname = sessionVars.User.AuthHostname
		}
	}

	for _, role := range s.Roles {
		exists, err := userExists(ctx, e.ctx, role.Username, role.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return ErrGrantRole.GenWithStackByArgs(role.String())
		}
	}
	for _, user := range s.Users {
		exists, err := userExists(ctx, e.ctx, user.Username, user.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return ErrCannotUser.GenWithStackByArgs("GRANT ROLE", user.String())
		}
	}

	restrictedCtx, err := e.getSysSession()
	if err != nil {
		return err
	}
	defer e.releaseSysSession(restrictedCtx)
	sqlExecutor := restrictedCtx.(sqlexec.SQLExecutor)

	// begin a transaction to insert role graph edges.
	if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "begin"); err != nil {
		return err
	}

	sql := new(strings.Builder)
	for _, user := range s.Users {
		for _, role := range s.Roles {
			sql.Reset()
			sqlexec.MustFormatSQL(sql, `INSERT IGNORE INTO %n.%n (FROM_HOST, FROM_USER, TO_HOST, TO_USER) VALUES (%?,%?,%?,%?)`, mysql.SystemDB, mysql.RoleEdgeTable, role.Hostname, role.Username, user.Hostname, user.Username)
			if _, err := sqlExecutor.ExecuteInternal(context.TODO(), sql.String()); err != nil {
				logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
				if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "rollback"); err != nil {
					return err
				}
				return ErrCannotUser.GenWithStackByArgs("GRANT ROLE", user.String())
			}
		}
	}
	if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "commit"); err != nil {
		return err
	}
	return domain.GetDomain(e.ctx).NotifyUpdatePrivilege()
}

// Should cover same internal mysql.* tables as DROP USER, so this function is very similar
func (e *SimpleExec) executeRenameUser(s *ast.RenameUserStmt) error {

	var failedUser string
	sysSession, err := e.getSysSession()
	defer e.releaseSysSession(sysSession)
	if err != nil {
		return err
	}
	sqlExecutor := sysSession.(sqlexec.SQLExecutor)

	if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "begin"); err != nil {
		return err
	}

	for _, userToUser := range s.UserToUsers {
		oldUser, newUser := userToUser.OldUser, userToUser.NewUser
		if len(newUser.Username) > auth.UserNameMaxLength {
			return ErrWrongStringLength.GenWithStackByArgs(newUser.Username, "user name", auth.UserNameMaxLength)
		}
		if len(newUser.Hostname) > auth.HostNameMaxLength {
			return ErrWrongStringLength.GenWithStackByArgs(newUser.Hostname, "host name", auth.HostNameMaxLength)
		}
		exists, err := userExistsInternal(sqlExecutor, oldUser.Username, oldUser.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			failedUser = oldUser.String() + " TO " + newUser.String() + " old did not exist"
			break
		}

		exists, err = userExistsInternal(sqlExecutor, newUser.Username, newUser.Hostname)
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

		// rename relationship from mysql.global_grants
		// TODO: add global_grants into the parser
		if err = renameUserHostInSystemTable(sqlExecutor, "global_grants", "User", "Host", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " mysql.global_grants error"
			break
		}

		//TODO: need update columns_priv once we implement columns_priv functionality.
		// When that is added, please refactor both executeRenameUser and executeDropUser to use an array of tables
		// to loop over, so it is easier to maintain.
	}

	if failedUser == "" {
		if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "commit"); err != nil {
			return err
		}
	} else {
		if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "rollback"); err != nil {
			return err
		}
		return ErrCannotUser.GenWithStackByArgs("RENAME USER", failedUser)
	}
	return domain.GetDomain(e.ctx).NotifyUpdatePrivilege()
}

func renameUserHostInSystemTable(sqlExecutor sqlexec.SQLExecutor, tableName, usernameColumn, hostColumn string, users *ast.UserToUser) error {
	sql := new(strings.Builder)
	sqlexec.MustFormatSQL(sql, `UPDATE %n.%n SET %n = %?, %n = %? WHERE %n = %? and %n = %?;`,
		mysql.SystemDB, tableName,
		usernameColumn, users.NewUser.Username, hostColumn, strings.ToLower(users.NewUser.Hostname),
		usernameColumn, users.OldUser.Username, hostColumn, strings.ToLower(users.OldUser.Hostname))
	_, err := sqlExecutor.ExecuteInternal(context.TODO(), sql.String())
	return err
}

func (e *SimpleExec) executeDropUser(ctx context.Context, s *ast.DropUserStmt) error {
	// Check privileges.
	// Check `CREATE USER` privilege.
	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker == nil {
		return errors.New("miss privilege checker")
	}
	activeRoles := e.ctx.GetSessionVars().ActiveRoles
	if !checker.RequestVerification(activeRoles, mysql.SystemDB, mysql.UserTable, "", mysql.DeletePriv) {
		if s.IsDropRole {
			if !checker.RequestVerification(activeRoles, "", "", "", mysql.DropRolePriv) &&
				!checker.RequestVerification(activeRoles, "", "", "", mysql.CreateUserPriv) {
				return core.ErrSpecificAccessDenied.GenWithStackByArgs("DROP ROLE or CREATE USER")
			}
		}
		if !s.IsDropRole && !checker.RequestVerification(activeRoles, "", "", "", mysql.CreateUserPriv) {
			return core.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
		}
	}
	hasSystemUserPriv := checker.RequestDynamicVerification(activeRoles, "SYSTEM_USER", false)
	hasRestrictedUserPriv := checker.RequestDynamicVerification(activeRoles, "RESTRICTED_USER_ADMIN", false)
	failedUsers := make([]string, 0, len(s.UserList))
	sysSession, err := e.getSysSession()
	defer e.releaseSysSession(sysSession)
	if err != nil {
		return err
	}
	sqlExecutor := sysSession.(sqlexec.SQLExecutor)

	if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "begin"); err != nil {
		return err
	}

	sql := new(strings.Builder)
	for _, user := range s.UserList {
		exists, err := userExists(ctx, e.ctx, user.Username, user.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			if s.IfExists {
				e.ctx.GetSessionVars().StmtCtx.AppendNote(infoschema.ErrUserDropExists.GenWithStackByArgs(user))
			} else {
				failedUsers = append(failedUsers, user.String())
				break
			}
		}

		// Certain users require additional privileges in order to be modified.
		// If this is the case, we need to rollback all changes and return a privilege error.
		// Because in TiDB SUPER can be used as a substitute for any dynamic privilege, this effectively means that
		// any user with SUPER requires a user with SUPER to be able to DROP the user.
		// We also allow RESTRICTED_USER_ADMIN to count for simplicity.
		if checker.RequestDynamicVerificationWithUser("SYSTEM_USER", false, user) && !(hasSystemUserPriv || hasRestrictedUserPriv) {
			if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "rollback"); err != nil {
				return err
			}
			return plannercore.ErrSpecificAccessDenied.GenWithStackByArgs("SYSTEM_USER or SUPER")
		}

		// begin a transaction to delete a user.
		sql.Reset()
		sqlexec.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, mysql.UserTable, strings.ToLower(user.Hostname), user.Username)
		if _, err = sqlExecutor.ExecuteInternal(context.TODO(), sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete privileges from mysql.global_priv
		sql.Reset()
		sqlexec.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, mysql.GlobalPrivTable, user.Hostname, user.Username)
		if _, err := sqlExecutor.ExecuteInternal(context.TODO(), sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "rollback"); err != nil {
				return err
			}
			continue
		}

		// delete privileges from mysql.db
		sql.Reset()
		sqlexec.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, mysql.DBTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(context.TODO(), sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete privileges from mysql.tables_priv
		sql.Reset()
		sqlexec.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, mysql.TablePrivTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(context.TODO(), sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete relationship from mysql.role_edges
		sql.Reset()
		sqlexec.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE TO_HOST = %? and TO_USER = %?;`, mysql.SystemDB, mysql.RoleEdgeTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(context.TODO(), sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		sql.Reset()
		sqlexec.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE FROM_HOST = %? and FROM_USER = %?;`, mysql.SystemDB, mysql.RoleEdgeTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(context.TODO(), sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete relationship from mysql.default_roles
		sql.Reset()
		sqlexec.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE DEFAULT_ROLE_HOST = %? and DEFAULT_ROLE_USER = %?;`, mysql.SystemDB, mysql.DefaultRoleTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(context.TODO(), sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		sql.Reset()
		sqlexec.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE HOST = %? and USER = %?;`, mysql.SystemDB, mysql.DefaultRoleTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(context.TODO(), sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete relationship from mysql.global_grants
		sql.Reset()
		sqlexec.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, "global_grants", user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(context.TODO(), sql.String()); err != nil {
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
		}

		//TODO: need delete columns_priv once we implement columns_priv functionality.
	}

	if len(failedUsers) == 0 {
		if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "commit"); err != nil {
			return err
		}
		if s.IsDropRole {
			// apply new activeRoles
			if ok, roleName := checker.ActiveRoles(e.ctx, activeRoles); !ok {
				u := e.ctx.GetSessionVars().User
				return ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
			}
		}
	} else {
		if _, err := sqlExecutor.ExecuteInternal(context.TODO(), "rollback"); err != nil {
			return err
		}
		if s.IsDropRole {
			return ErrCannotUser.GenWithStackByArgs("DROP ROLE", strings.Join(failedUsers, ","))
		}
		return ErrCannotUser.GenWithStackByArgs("DROP USER", strings.Join(failedUsers, ","))
	}
	return domain.GetDomain(e.ctx).NotifyUpdatePrivilege()
}

func userExists(ctx context.Context, sctx sessionctx.Context, name string, host string) (bool, error) {
	exec := sctx.(sqlexec.RestrictedSQLExecutor)
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, `SELECT * FROM %n.%n WHERE User=%? AND Host=%?;`, mysql.SystemDB, mysql.UserTable, name, strings.ToLower(host))
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}

// use the same internal executor to read within the same transaction, otherwise same as userExists
func userExistsInternal(sqlExecutor sqlexec.SQLExecutor, name string, host string) (bool, error) {
	sql := new(strings.Builder)
	sqlexec.MustFormatSQL(sql, `SELECT * FROM %n.%n WHERE User=%? AND Host=%?;`, mysql.SystemDB, mysql.UserTable, name, strings.ToLower(host))
	recordSet, err := sqlExecutor.ExecuteInternal(context.TODO(), sql.String())
	if err != nil {
		return false, err
	}
	req := recordSet.NewChunk(nil)
	err = recordSet.Next(context.TODO(), req)
	var rows int = 0
	if err == nil {
		rows = req.NumRows()
	}
	errClose := recordSet.Close()
	if errClose != nil {
		return false, errClose
	}
	return rows > 0, err
}

func (e *SimpleExec) userAuthPlugin(name string, host string) (string, error) {
	pm := privilege.GetPrivilegeManager(e.ctx)
	authplugin, err := pm.GetAuthPlugin(name, host)
	if err != nil {
		return "", err
	}
	return authplugin, nil
}

func (e *SimpleExec) executeSetPwd(ctx context.Context, s *ast.SetPwdStmt) error {
	var u, h string
	if s.User == nil || s.User.CurrentUser {
		if e.ctx.GetSessionVars().User == nil {
			return errors.New("Session error is empty")
		}
		u = e.ctx.GetSessionVars().User.AuthUsername
		h = e.ctx.GetSessionVars().User.AuthHostname
	} else {
		checker := privilege.GetPrivilegeManager(e.ctx)
		activeRoles := e.ctx.GetSessionVars().ActiveRoles
		if checker != nil && !checker.RequestVerification(activeRoles, "", "", "", mysql.SuperPriv) {
			return ErrDBaccessDenied.GenWithStackByArgs(u, h, "mysql")
		}
		u = s.User.Username
		h = s.User.Hostname
	}
	exists, err := userExists(ctx, e.ctx, u, h)
	if err != nil {
		return err
	}
	if !exists {
		return errors.Trace(ErrPasswordNoMatch)
	}

	authplugin, err := e.userAuthPlugin(u, h)
	if err != nil {
		return err
	}
	var pwd string
	switch authplugin {
	case mysql.AuthCachingSha2Password:
		pwd = auth.NewSha2Password(s.Password)
	case mysql.AuthSocket:
		e.ctx.GetSessionVars().StmtCtx.AppendNote(ErrSetPasswordAuthPlugin.GenWithStackByArgs(u, h))
		pwd = ""
	default:
		pwd = auth.EncodePassword(s.Password)
	}

	// update mysql.user
	exec := e.ctx.(sqlexec.RestrictedSQLExecutor)
	_, _, err = exec.ExecRestrictedSQL(ctx, nil, `UPDATE %n.%n SET authentication_string=%? WHERE User=%? AND Host=%?;`, mysql.SystemDB, mysql.UserTable, pwd, u, strings.ToLower(h))
	if err != nil {
		return err
	}
	return domain.GetDomain(e.ctx).NotifyUpdatePrivilege()
}

func (e *SimpleExec) executeKillStmt(ctx context.Context, s *ast.KillStmt) error {
	if !config.GetGlobalConfig().EnableGlobalKill {
		conf := config.GetGlobalConfig()
		if s.TiDBExtension || conf.CompatibleKillQuery {
			sm := e.ctx.GetSessionManager()
			if sm == nil {
				return nil
			}
			sm.Kill(s.ConnectionID, s.Query)
		} else {
			err := errors.New("Invalid operation. Please use 'KILL TIDB [CONNECTION | QUERY] connectionID' instead")
			e.ctx.GetSessionVars().StmtCtx.AppendWarning(err)
		}
		return nil
	}

	sm := e.ctx.GetSessionManager()
	if sm == nil {
		return nil
	}
	if e.IsFromRemote {
		logutil.BgLogger().Info("Killing connection in current instance redirected from remote TiDB", zap.Uint64("connID", s.ConnectionID), zap.Bool("query", s.Query),
			zap.String("sourceAddr", e.ctx.GetSessionVars().SourceAddr.IP.String()))
		sm.Kill(s.ConnectionID, s.Query)
		return nil
	}

	connID, isTruncated, err := util.ParseGlobalConnID(s.ConnectionID)
	if err != nil {
		err1 := errors.New("Parse ConnectionID failed: " + err.Error())
		e.ctx.GetSessionVars().StmtCtx.AppendWarning(err1)
		return nil
	}
	if isTruncated {
		message := "Kill failed: Received a 32bits truncated ConnectionID, expect 64bits. Please execute 'KILL [CONNECTION | QUERY] ConnectionID' to send a Kill without truncating ConnectionID."
		logutil.BgLogger().Warn(message, zap.Uint64("connID", s.ConnectionID))
		// Notice that this warning cannot be seen if KILL is triggered by "CTRL-C" of mysql client,
		//   as the KILL is sent by a new connection.
		err := errors.New(message)
		e.ctx.GetSessionVars().StmtCtx.AppendWarning(err)
		return nil
	}

	if connID.ServerID != sm.ServerID() {
		if err := killRemoteConn(ctx, e.ctx, &connID, s.Query); err != nil {
			err1 := errors.New("KILL remote connection failed: " + err.Error())
			e.ctx.GetSessionVars().StmtCtx.AppendWarning(err1)
		}
	} else {
		sm.Kill(s.ConnectionID, s.Query)
	}

	return nil
}

func killRemoteConn(ctx context.Context, sctx sessionctx.Context, connID *util.GlobalConnID, query bool) error {
	if connID.ServerID == 0 {
		return errors.New("Unexpected ZERO ServerID. Please file a bug to the TiDB Team")
	}

	killExec := &tipb.Executor{
		Tp:   tipb.ExecType_TypeKill,
		Kill: &tipb.Kill{ConnID: connID.ID(), Query: query},
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
		SetFromSessionVars(sctx.GetSessionVars()).
		SetFromInfoSchema(sctx.GetInfoSchema()).
		SetStoreType(kv.TiDB).
		SetTiDBServerID(connID.ServerID).
		Build()
	if err != nil {
		return err
	}
	resp := sctx.GetClient().Send(ctx, kvReq, sctx.GetSessionVars().KVVars, &kv.ClientSendOption{})
	if resp == nil {
		err := errors.New("client returns nil response")
		return err
	}

	logutil.BgLogger().Info("Killed remote connection", zap.Uint64("serverID", connID.ServerID),
		zap.Uint64("connID", connID.ID()), zap.Bool("query", query))
	return err
}

func (e *SimpleExec) executeFlush(s *ast.FlushStmt) error {
	switch s.Tp {
	case ast.FlushTables:
		if s.ReadLock {
			return errors.New("FLUSH TABLES WITH READ LOCK is not supported.  Please use @@tidb_snapshot")
		}
	case ast.FlushPrivileges:
		dom := domain.GetDomain(e.ctx)
		return dom.NotifyUpdatePrivilege()
	case ast.FlushTiDBPlugin:
		dom := domain.GetDomain(e.ctx)
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
		sm := e.ctx.GetSessionManager()
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
	h := domain.GetDomain(e.ctx).StatsHandle()
	var statsIDs []int64
	if s.IsGlobalStats {
		statsIDs = []int64{s.Table.TableInfo.ID}
	} else {
		if statsIDs, _, err = core.GetPhysicalIDsAndPartitionNames(s.Table.TableInfo, s.PartitionNames); err != nil {
			return err
		}
		if len(s.PartitionNames) == 0 {
			statsIDs = append(statsIDs, s.Table.TableInfo.ID)
		}
	}
	if err := h.DeleteTableStatsFromKV(statsIDs); err != nil {
		return err
	}
	return h.Update(e.ctx.GetInfoSchema().(infoschema.InfoSchema))
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

func (e *SimpleExec) executeShutdown(s *ast.ShutdownStmt) error {
	sessVars := e.ctx.GetSessionVars()
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

func (e *SimpleExec) executeAdmin(s *ast.AdminStmt) error {
	switch s.Tp {
	case ast.AdminReloadStatistics:
		return e.executeAdminReloadStatistics(s)
	case ast.AdminFlushPlanCache:
		return e.executeAdminFlushPlanCache(s)
	}
	return nil
}

func (e *SimpleExec) executeAdminReloadStatistics(s *ast.AdminStmt) error {
	if s.Tp != ast.AdminReloadStatistics {
		return errors.New("This AdminStmt is not ADMIN RELOAD STATS_EXTENDED")
	}
	if !e.ctx.GetSessionVars().EnableExtendedStats {
		return errors.New("Extended statistics feature is not generally available now, and tidb_enable_extended_stats is OFF")
	}
	return domain.GetDomain(e.ctx).StatsHandle().ReloadExtendedStatistics()
}

func (e *SimpleExec) executeAdminFlushPlanCache(s *ast.AdminStmt) error {
	if s.Tp != ast.AdminFlushPlanCache {
		return errors.New("This AdminStmt is not ADMIN FLUSH PLAN_CACHE")
	}
	if s.StatementScope == ast.StatementScopeGlobal {
		return errors.New("Do not support the 'admin flush global scope.'")
	}
	if !plannercore.PreparedPlanCacheEnabled() {
		e.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.New("The plan cache is disable. So there no need to flush the plan cache"))
		return nil
	}
	now := types.NewTime(types.FromGoTime(time.Now().In(e.ctx.GetSessionVars().StmtCtx.TimeZone)), mysql.TypeTimestamp, 3)
	e.ctx.GetSessionVars().LastUpdateTime4PC = now
	e.ctx.PreparedPlanCache().DeleteAll()
	if s.StatementScope == ast.StatementScopeInstance {
		// Record the timestamp. When other sessions want to use the plan cache,
		// it will check the timestamp first to decide whether the plan cache should be flushed.
		domain.GetDomain(e.ctx).SetExpiredTimeStamp4PC(now)
	}
	return nil
}
