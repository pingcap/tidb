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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
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
	done      bool
	is        infoschema.InfoSchema
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
	if _, err := ctx.(sqlexec.SQLExecutor).Execute(context.Background(), "rollback"); err != nil {
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
		if err := e.ctx.NewTxn(ctx); err != nil {
			return err
		}
		defer func() { e.ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, false) }()
	}

	switch x := e.Statement.(type) {
	case *ast.GrantRoleStmt:
		err = e.executeGrantRole(x)
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
		err = e.executeAlterUser(x)
	case *ast.DropUserStmt:
		err = e.executeDropUser(x)
	case *ast.SetPwdStmt:
		err = e.executeSetPwd(x)
	case *ast.KillStmt:
		err = e.executeKillStmt(x)
	case *ast.BinlogStmt:
		// We just ignore it.
		return nil
	case *ast.DropStatsStmt:
		err = e.executeDropStats(x)
	case *ast.SetRoleStmt:
		err = e.executeSetRole(x)
	case *ast.RevokeRoleStmt:
		err = e.executeRevokeRole(x)
	case *ast.SetDefaultRoleStmt:
		err = e.executeSetDefaultRole(x)
	case *ast.ShutdownStmt:
		err = e.executeShutdown(x)
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
	if _, err := sqlExecutor.Execute(context.Background(), "begin"); err != nil {
		return err
	}
	for _, u := range s.UserList {
		if u.Hostname == "" {
			u.Hostname = "%"
		}
		sql := fmt.Sprintf("DELETE IGNORE FROM mysql.default_roles WHERE USER='%s' AND HOST='%s';", u.Username, u.Hostname)
		if _, err := sqlExecutor.Execute(context.Background(), sql); err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
			if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
	}
	if _, err := sqlExecutor.Execute(context.Background(), "commit"); err != nil {
		return err
	}
	return nil
}

func (e *SimpleExec) setDefaultRoleRegular(s *ast.SetDefaultRoleStmt) error {
	for _, user := range s.UserList {
		exists, err := userExists(e.ctx, user.Username, user.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return ErrCannotUser.GenWithStackByArgs("SET DEFAULT ROLE", user.String())
		}
	}
	for _, role := range s.RoleList {
		exists, err := userExists(e.ctx, role.Username, role.Hostname)
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
	if _, err := sqlExecutor.Execute(context.Background(), "begin"); err != nil {
		return err
	}
	for _, user := range s.UserList {
		if user.Hostname == "" {
			user.Hostname = "%"
		}
		sql := fmt.Sprintf("DELETE IGNORE FROM mysql.default_roles WHERE USER='%s' AND HOST='%s';", user.Username, user.Hostname)
		if _, err := sqlExecutor.Execute(context.Background(), sql); err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
			if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
		for _, role := range s.RoleList {
			sql := fmt.Sprintf("INSERT IGNORE INTO mysql.default_roles values('%s', '%s', '%s', '%s');", user.Hostname, user.Username, role.Hostname, role.Username)
			checker := privilege.GetPrivilegeManager(e.ctx)
			ok := checker.FindEdge(e.ctx, role, user)
			if ok {
				if _, err := sqlExecutor.Execute(context.Background(), sql); err != nil {
					logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
					if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
						return rollbackErr
					}
					return err
				}
			} else {
				if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
					return rollbackErr
				}
				return ErrRoleNotGranted.GenWithStackByArgs(role.String(), user.String())
			}
		}
	}
	if _, err := sqlExecutor.Execute(context.Background(), "commit"); err != nil {
		return err
	}
	return nil
}

func (e *SimpleExec) setDefaultRoleAll(s *ast.SetDefaultRoleStmt) error {
	for _, user := range s.UserList {
		exists, err := userExists(e.ctx, user.Username, user.Hostname)
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
	if _, err := sqlExecutor.Execute(context.Background(), "begin"); err != nil {
		return err
	}
	for _, user := range s.UserList {
		if user.Hostname == "" {
			user.Hostname = "%"
		}
		sql := fmt.Sprintf("DELETE IGNORE FROM mysql.default_roles WHERE USER='%s' AND HOST='%s';", user.Username, user.Hostname)
		if _, err := sqlExecutor.Execute(context.Background(), sql); err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
			if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
		sql = fmt.Sprintf("INSERT IGNORE INTO mysql.default_roles(HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER) "+
			"SELECT TO_HOST,TO_USER,FROM_HOST,FROM_USER FROM mysql.role_edges WHERE TO_HOST='%s' AND TO_USER='%s';", user.Hostname, user.Username)
		if _, err := sqlExecutor.Execute(context.Background(), sql); err != nil {
			if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
	}
	if _, err := sqlExecutor.Execute(context.Background(), "commit"); err != nil {
		return err
	}
	return nil
}

func (e *SimpleExec) setDefaultRoleForCurrentUser(s *ast.SetDefaultRoleStmt) (err error) {
	checker := privilege.GetPrivilegeManager(e.ctx)
	user, sql := s.UserList[0], ""
	if user.Hostname == "" {
		user.Hostname = "%"
	}
	switch s.SetRoleOpt {
	case ast.SetRoleNone:
		sql = fmt.Sprintf("DELETE IGNORE FROM mysql.default_roles WHERE USER='%s' AND HOST='%s';", user.Username, user.Hostname)
	case ast.SetRoleAll:
		sql = fmt.Sprintf("INSERT IGNORE INTO mysql.default_roles(HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER) "+
			"SELECT TO_HOST,TO_USER,FROM_HOST,FROM_USER FROM mysql.role_edges WHERE TO_HOST='%s' AND TO_USER='%s';", user.Hostname, user.Username)
	case ast.SetRoleRegular:
		sql = "INSERT IGNORE INTO mysql.default_roles values"
		for i, role := range s.RoleList {
			ok := checker.FindEdge(e.ctx, role, user)
			if !ok {
				return ErrRoleNotGranted.GenWithStackByArgs(role.String(), user.String())
			}
			sql += fmt.Sprintf("('%s', '%s', '%s', '%s')", user.Hostname, user.Username, role.Hostname, role.Username)
			if i != len(s.RoleList)-1 {
				sql += ","
			}
		}
	}

	restrictedCtx, err := e.getSysSession()
	if err != nil {
		return err
	}
	defer e.releaseSysSession(restrictedCtx)
	sqlExecutor := restrictedCtx.(sqlexec.SQLExecutor)

	if _, err := sqlExecutor.Execute(context.Background(), "begin"); err != nil {
		return err
	}

	deleteSQL := fmt.Sprintf("DELETE IGNORE FROM mysql.default_roles WHERE USER='%s' AND HOST='%s';", user.Username, user.Hostname)
	if _, err := sqlExecutor.Execute(context.Background(), deleteSQL); err != nil {
		logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
		if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
			return rollbackErr
		}
		return err
	}

	if _, err := sqlExecutor.Execute(context.Background(), sql); err != nil {
		logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
		if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
			return rollbackErr
		}
		return err
	}
	if _, err := sqlExecutor.Execute(context.Background(), "commit"); err != nil {
		return err
	}
	return nil
}

func (e *SimpleExec) executeSetDefaultRole(s *ast.SetDefaultRoleStmt) (err error) {
	sessionVars := e.ctx.GetSessionVars()
	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker == nil {
		return errors.New("miss privilege checker")
	}

	if len(s.UserList) == 1 && sessionVars.User != nil {
		u, h := s.UserList[0].Username, s.UserList[0].Hostname
		if u == sessionVars.User.Username && h == sessionVars.User.AuthHostname {
			err = e.setDefaultRoleForCurrentUser(s)
			domain.GetDomain(e.ctx).NotifyUpdatePrivilege(e.ctx)
			return
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
		err = e.setDefaultRoleAll(s)
	case ast.SetRoleNone:
		err = e.setDefaultRoleNone(s)
	case ast.SetRoleRegular:
		err = e.setDefaultRoleRegular(s)
	}
	if err != nil {
		return
	}
	domain.GetDomain(e.ctx).NotifyUpdatePrivilege(e.ctx)
	return
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
	e.ctx.GetSessionVars().CurrentDB = dbname.O
	// character_set_database is the character set used by the default database.
	// The server sets this variable whenever the default database changes.
	// See http://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_character_set_database
	sessionVars := e.ctx.GetSessionVars()
	err := sessionVars.SetSystemVar(variable.CharsetDatabase, dbinfo.Charset)
	if err != nil {
		return err
	}
	dbCollate := dbinfo.Collate
	if dbCollate == "" {
		// Since we have checked the charset, the dbCollate here shouldn't be "".
		dbCollate = getDefaultCollate(dbinfo.Charset)
	}
	return sessionVars.SetSystemVar(variable.CollationDatabase, dbCollate)
}

func (e *SimpleExec) executeBegin(ctx context.Context, s *ast.BeginStmt) error {
	// If BEGIN is the first statement in TxnCtx, we can reuse the existing transaction, without the
	// need to call NewTxn, which commits the existing transaction and begins a new one.
	txnCtx := e.ctx.GetSessionVars().TxnCtx
	if txnCtx.History != nil {
		err := e.ctx.NewTxn(ctx)
		if err != nil {
			return err
		}
	}
	// With START TRANSACTION, autocommit remains disabled until you end
	// the transaction with COMMIT or ROLLBACK. The autocommit mode then
	// reverts to its previous state.
	e.ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, true)
	// Call ctx.Txn(true) to active pending txn.
	pTxnConf := config.GetGlobalConfig().PessimisticTxn
	if pTxnConf.Enable {
		txnMode := s.Mode
		if txnMode == "" {
			txnMode = e.ctx.GetSessionVars().TxnMode
		}
		if txnMode == ast.Pessimistic {
			e.ctx.GetSessionVars().TxnCtx.IsPessimistic = true
		}
	}
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}
	if e.ctx.GetSessionVars().TxnCtx.IsPessimistic {
		txn.SetOption(kv.Pessimistic, true)
	}
	return nil
}

func (e *SimpleExec) executeRevokeRole(s *ast.RevokeRoleStmt) error {
	for _, role := range s.Roles {
		exists, err := userExists(e.ctx, role.Username, role.Hostname)
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
	if _, err := sqlExecutor.Execute(context.Background(), "begin"); err != nil {
		return errors.Trace(err)
	}
	for _, user := range s.Users {
		exists, err := userExists(e.ctx, user.Username, user.Hostname)
		if err != nil {
			return errors.Trace(err)
		}
		if !exists {
			if _, err := sqlExecutor.Execute(context.Background(), "rollback"); err != nil {
				return errors.Trace(err)
			}
			return ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", user.String())
		}
		for _, role := range s.Roles {
			if role.Hostname == "" {
				role.Hostname = "%"
			}
			sql := fmt.Sprintf(`DELETE IGNORE FROM %s.%s WHERE FROM_HOST='%s' and FROM_USER='%s' and TO_HOST='%s' and TO_USER='%s'`, mysql.SystemDB, mysql.RoleEdgeTable, role.Hostname, role.Username, user.Hostname, user.Username)
			if _, err := sqlExecutor.Execute(context.Background(), sql); err != nil {
				if _, err := sqlExecutor.Execute(context.Background(), "rollback"); err != nil {
					return errors.Trace(err)
				}
				return ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", role.String())
			}
			sql = fmt.Sprintf(`DELETE IGNORE FROM %s.%s WHERE DEFAULT_ROLE_HOST='%s' and DEFAULT_ROLE_USER='%s' and HOST='%s' and USER='%s'`, mysql.SystemDB, mysql.DefaultRoleTable, role.Hostname, role.Username, user.Hostname, user.Username)
			if _, err := sqlExecutor.Execute(context.Background(), sql); err != nil {
				if _, err := sqlExecutor.Execute(context.Background(), "rollback"); err != nil {
					return errors.Trace(err)
				}
				return ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", role.String())
			}
		}
	}
	if _, err := sqlExecutor.Execute(context.Background(), "commit"); err != nil {
		return err
	}
	domain.GetDomain(e.ctx).NotifyUpdatePrivilege(e.ctx)
	return nil
}

func (e *SimpleExec) executeCommit(s *ast.CommitStmt) {
	e.ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, false)
}

func (e *SimpleExec) executeRollback(s *ast.RollbackStmt) error {
	sessVars := e.ctx.GetSessionVars()
	logutil.BgLogger().Debug("execute rollback statement", zap.Uint64("conn", sessVars.ConnectionID))
	sessVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
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

	users := make([]string, 0, len(s.Specs))
	privs := make([]string, 0, len(s.Specs))
	for _, spec := range s.Specs {
		exists, err1 := userExists(e.ctx, spec.User.Username, spec.User.Hostname)
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
		user := fmt.Sprintf(`('%s', '%s', '%s')`, spec.User.Hostname, spec.User.Username, pwd)
		if s.IsCreateRole {
			user = fmt.Sprintf(`('%s', '%s', '%s', 'Y')`, spec.User.Hostname, spec.User.Username, pwd)
		}
		users = append(users, user)

		if len(privData) != 0 {
			priv := fmt.Sprintf(`('%s', '%s', '%s')`, spec.User.Hostname, spec.User.Username, hack.String(privData))
			privs = append(privs, priv)
		}
	}
	if len(users) == 0 {
		return nil
	}

	sql := fmt.Sprintf(`INSERT INTO %s.%s (Host, User, authentication_string) VALUES %s;`, mysql.SystemDB, mysql.UserTable, strings.Join(users, ", "))
	if s.IsCreateRole {
		sql = fmt.Sprintf(`INSERT INTO %s.%s (Host, User, authentication_string, Account_locked) VALUES %s;`, mysql.SystemDB, mysql.UserTable, strings.Join(users, ", "))
	}

	restrictedCtx, err := e.getSysSession()
	if err != nil {
		return err
	}
	defer e.releaseSysSession(restrictedCtx)
	sqlExecutor := restrictedCtx.(sqlexec.SQLExecutor)

	if _, err := sqlExecutor.Execute(context.Background(), "begin"); err != nil {
		return errors.Trace(err)
	}
	_, err = sqlExecutor.Execute(context.Background(), sql)
	if err != nil {
		if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
			return rollbackErr
		}
		return err
	}
	if len(privs) != 0 {
		sql = fmt.Sprintf("INSERT IGNORE INTO %s.%s (Host, User, Priv) VALUES %s", mysql.SystemDB, mysql.GlobalPrivTable, strings.Join(privs, ", "))
		_, err = sqlExecutor.Execute(context.Background(), sql)
		if err != nil {
			if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
	}
	if _, err := sqlExecutor.Execute(context.Background(), "commit"); err != nil {
		return errors.Trace(err)
	}
	domain.GetDomain(e.ctx).NotifyUpdatePrivilege(e.ctx)
	return err
}

func (e *SimpleExec) executeAlterUser(s *ast.AlterUserStmt) error {
	if s.CurrentAuth != nil {
		user := e.ctx.GetSessionVars().User
		if user == nil {
			return errors.New("Session user is empty")
		}
		spec := &ast.UserSpec{
			User:    user,
			AuthOpt: s.CurrentAuth,
		}
		s.Specs = []*ast.UserSpec{spec}
	}

	privData, err := tlsOption2GlobalPriv(s.TLSOptions)
	if err != nil {
		return err
	}

	failedUsers := make([]string, 0, len(s.Specs))
	for _, spec := range s.Specs {
		exists, err := userExists(e.ctx, spec.User.Username, spec.User.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			user := fmt.Sprintf(`'%s'@'%s'`, spec.User.Username, spec.User.Hostname)
			failedUsers = append(failedUsers, user)
			continue
		}
		pwd, ok := spec.EncodedPassword()
		if !ok {
			return errors.Trace(ErrPasswordFormat)
		}
		sql := fmt.Sprintf(`UPDATE %s.%s SET authentication_string = '%s' WHERE Host = '%s' and User = '%s';`,
			mysql.SystemDB, mysql.UserTable, pwd, spec.User.Hostname, spec.User.Username)
		_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(sql)
		if err != nil {
			failedUsers = append(failedUsers, spec.User.String())
		}

		if len(privData) > 0 {
			sql = fmt.Sprintf("INSERT INTO %s.%s (Host, User, Priv) VALUES ('%s','%s','%s') ON DUPLICATE KEY UPDATE Priv = values(Priv)",
				mysql.SystemDB, mysql.GlobalPrivTable, spec.User.Hostname, spec.User.Username, hack.String(privData))
			_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(sql)
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
		err = txn.Commit(sessionctx.SetCommitCtx(context.Background(), e.ctx))
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
	domain.GetDomain(e.ctx).NotifyUpdatePrivilege(e.ctx)
	return nil
}

func (e *SimpleExec) executeGrantRole(s *ast.GrantRoleStmt) error {
	sessionVars := e.ctx.GetSessionVars()
	for i, user := range s.Users {
		if user.CurrentUser {
			s.Users[i].Username = sessionVars.User.AuthUsername
			s.Users[i].Hostname = sessionVars.User.AuthHostname
		}
	}

	for _, role := range s.Roles {
		exists, err := userExists(e.ctx, role.Username, role.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return ErrCannotUser.GenWithStackByArgs("GRANT ROLE", role.String())
		}
	}
	for _, user := range s.Users {
		exists, err := userExists(e.ctx, user.Username, user.Hostname)
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
	if _, err := sqlExecutor.Execute(context.Background(), "begin"); err != nil {
		return err
	}

	for _, user := range s.Users {
		for _, role := range s.Roles {
			sql := fmt.Sprintf(`INSERT IGNORE INTO %s.%s (FROM_HOST, FROM_USER, TO_HOST, TO_USER) VALUES ('%s','%s','%s','%s')`, mysql.SystemDB, mysql.RoleEdgeTable, role.Hostname, role.Username, user.Hostname, user.Username)
			if _, err := sqlExecutor.Execute(context.Background(), sql); err != nil {
				logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
				if _, err := sqlExecutor.Execute(context.Background(), "rollback"); err != nil {
					return err
				}
				return ErrCannotUser.GenWithStackByArgs("GRANT ROLE", user.String())
			}
		}
	}
	if _, err := sqlExecutor.Execute(context.Background(), "commit"); err != nil {
		return err
	}
	domain.GetDomain(e.ctx).NotifyUpdatePrivilege(e.ctx)
	return nil
}

func (e *SimpleExec) executeDropUser(s *ast.DropUserStmt) error {
	// Check privileges.
	// Check `CREATE USER` privilege.
	if !config.GetGlobalConfig().Security.SkipGrantTable {
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
	}

	failedUsers := make([]string, 0, len(s.UserList))
	sysSession, err := e.getSysSession()
	defer e.releaseSysSession(sysSession)
	if err != nil {
		return err
	}
	sqlExecutor := sysSession.(sqlexec.SQLExecutor)

	if _, err := sqlExecutor.Execute(context.Background(), "begin"); err != nil {
		return err
	}

	for _, user := range s.UserList {
		exists, err := userExists(e.ctx, user.Username, user.Hostname)
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

		// begin a transaction to delete a user.
		sql := fmt.Sprintf(`DELETE FROM %s.%s WHERE Host = '%s' and User = '%s';`, mysql.SystemDB, mysql.UserTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.Execute(context.Background(), sql); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete privileges from mysql.global_priv
		sql = fmt.Sprintf(`DELETE FROM %s.%s WHERE Host = '%s' and User = '%s';`, mysql.SystemDB, mysql.GlobalPrivTable, user.Hostname, user.Username)
		if _, err := sqlExecutor.Execute(context.Background(), sql); err != nil {
			failedUsers = append(failedUsers, user.String())
			if _, err := sqlExecutor.Execute(context.Background(), "rollback"); err != nil {
				return err
			}
			continue
		}

		// delete privileges from mysql.db
		sql = fmt.Sprintf(`DELETE FROM %s.%s WHERE Host = '%s' and User = '%s';`, mysql.SystemDB, mysql.DBTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.Execute(context.Background(), sql); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete privileges from mysql.tables_priv
		sql = fmt.Sprintf(`DELETE FROM %s.%s WHERE Host = '%s' and User = '%s';`, mysql.SystemDB, mysql.TablePrivTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.Execute(context.Background(), sql); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete relationship from mysql.role_edges
		sql = fmt.Sprintf(`DELETE FROM %s.%s WHERE TO_HOST = '%s' and TO_USER = '%s';`, mysql.SystemDB, mysql.RoleEdgeTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.Execute(context.Background(), sql); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		sql = fmt.Sprintf(`DELETE FROM %s.%s WHERE FROM_HOST = '%s' and FROM_USER = '%s';`, mysql.SystemDB, mysql.RoleEdgeTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.Execute(context.Background(), sql); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete relationship from mysql.default_roles
		sql = fmt.Sprintf(`DELETE FROM %s.%s WHERE DEFAULT_ROLE_HOST = '%s' and DEFAULT_ROLE_USER = '%s';`, mysql.SystemDB, mysql.DefaultRoleTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.Execute(context.Background(), sql); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		sql = fmt.Sprintf(`DELETE FROM %s.%s WHERE HOST = '%s' and USER = '%s';`, mysql.SystemDB, mysql.DefaultRoleTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.Execute(context.Background(), sql); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}
		//TODO: need delete columns_priv once we implement columns_priv functionality.
	}

	if len(failedUsers) == 0 {
		if _, err := sqlExecutor.Execute(context.Background(), "commit"); err != nil {
			return err
		}
	} else {
		if _, err := sqlExecutor.Execute(context.Background(), "rollback"); err != nil {
			return err
		}
		if s.IsDropRole {
			return ErrCannotUser.GenWithStackByArgs("DROP ROLE", strings.Join(failedUsers, ","))
		}
		return ErrCannotUser.GenWithStackByArgs("DROP USER", strings.Join(failedUsers, ","))
	}
	domain.GetDomain(e.ctx).NotifyUpdatePrivilege(e.ctx)
	return nil
}

func userExists(ctx sessionctx.Context, name string, host string) (bool, error) {
	sql := fmt.Sprintf(`SELECT * FROM %s.%s WHERE User='%s' AND Host='%s';`, mysql.SystemDB, mysql.UserTable, name, host)
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(sql)
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}

func (e *SimpleExec) executeSetPwd(s *ast.SetPwdStmt) error {
	var u, h string
	if s.User == nil {
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
	exists, err := userExists(e.ctx, u, h)
	if err != nil {
		return err
	}
	if !exists {
		return errors.Trace(ErrPasswordNoMatch)
	}

	// update mysql.user
	sql := fmt.Sprintf(`UPDATE %s.%s SET authentication_string='%s' WHERE User='%s' AND Host='%s';`, mysql.SystemDB, mysql.UserTable, auth.EncodePassword(s.Password), u, h)
	_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(sql)
	domain.GetDomain(e.ctx).NotifyUpdatePrivilege(e.ctx)
	return err
}

func (e *SimpleExec) executeKillStmt(s *ast.KillStmt) error {
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

func (e *SimpleExec) executeFlush(s *ast.FlushStmt) error {
	switch s.Tp {
	case ast.FlushTables:
		if s.ReadLock {
			return errors.New("FLUSH TABLES WITH READ LOCK is not supported.  Please use @@tidb_snapshot")
		}
	case ast.FlushPrivileges:
		// If skip-grant-table is configured, do not flush privileges.
		// Because LoadPrivilegeLoop does not run and the privilege Handle is nil,
		// Call dom.PrivilegeHandle().Update would panic.
		if config.GetGlobalConfig().Security.SkipGrantTable {
			return nil
		}

		dom := domain.GetDomain(e.ctx)
		sysSessionPool := dom.SysSessionPool()
		ctx, err := sysSessionPool.Get()
		if err != nil {
			return err
		}
		defer sysSessionPool.Put(ctx)
		err = dom.PrivilegeHandle().Update(ctx.(sessionctx.Context))
		return err
	case ast.FlushTiDBPlugin:
		dom := domain.GetDomain(e.ctx)
		for _, pluginName := range s.Plugins {
			err := plugin.NotifyFlush(dom, pluginName)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *SimpleExec) executeAlterInstance(s *ast.AlterInstanceStmt) error {
	if s.ReloadTLS {
		logutil.BgLogger().Info("execute reload tls", zap.Bool("NoRollbackOnError", s.NoRollbackOnError))
		sm := e.ctx.GetSessionManager()
		tlsCfg, err := util.LoadTLSCertificates(
			variable.SysVars["ssl_ca"].Value,
			variable.SysVars["ssl_key"].Value,
			variable.SysVars["ssl_cert"].Value,
		)
		if err != nil {
			if !s.NoRollbackOnError || config.GetGlobalConfig().Security.RequireSecureTransport {
				return err
			}
			logutil.BgLogger().Warn("reload TLS fail but keep working without TLS due to 'no rollback on error'")
		}
		sm.UpdateTLSConfig(tlsCfg)
	}
	return nil
}

func (e *SimpleExec) executeDropStats(s *ast.DropStatsStmt) error {
	h := domain.GetDomain(e.ctx).StatsHandle()
	err := h.DeleteTableStatsFromKV(s.Table.TableInfo.ID)
	if err != nil {
		return err
	}
	return h.Update(infoschema.GetInfoSchema(e.ctx))
}

func (e *SimpleExec) autoNewTxn() bool {
	switch e.Statement.(type) {
	case *ast.CreateUserStmt, *ast.AlterUserStmt, *ast.DropUserStmt:
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
	err := p.Kill()
	if err != nil {
		panic(err)
	}
}
