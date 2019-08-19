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
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
	"golang.org/x/net/context"
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

// Next implements the Executor Next interface.
func (e *SimpleExec) Next(ctx context.Context, chk *chunk.Chunk) (err error) {
	if e.done {
		return nil
	}

	if e.autoNewTxn() {
		// Commit the old transaction, like DDL.
		if err := e.ctx.NewTxn(); err != nil {
			return err
		}
		defer func() { e.ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, false) }()
	}

	switch x := e.Statement.(type) {
	case *ast.UseStmt:
		err = e.executeUse(x)
	case *ast.FlushStmt:
		err = e.executeFlush(x)
	case *ast.BeginStmt:
		err = e.executeBegin(x)
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
	}
	e.done = true
	return errors.Trace(err)
}

func (e *SimpleExec) executeUse(s *ast.UseStmt) error {
	dbname := model.NewCIStr(s.DBName)
	dbinfo, exists := e.is.SchemaByName(dbname)
	if !exists {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbname)
	}
	e.ctx.GetSessionVars().CurrentDB = dbname.O
	// character_set_database is the character set used by the default database.
	// The server sets this variable whenever the default database changes.
	// See http://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_character_set_database
	sessionVars := e.ctx.GetSessionVars()
	terror.Log(errors.Trace(sessionVars.SetSystemVar(variable.CharsetDatabase, dbinfo.Charset)))
	terror.Log(errors.Trace(sessionVars.SetSystemVar(variable.CollationDatabase, dbinfo.Collate)))
	return nil
}

func (e *SimpleExec) executeBegin(s *ast.BeginStmt) error {
	// If BEGIN is the first statement in TxnCtx, we can reuse the existing transaction, without the
	// need to call NewTxn, which commits the existing transaction and begins a new one.
	txnCtx := e.ctx.GetSessionVars().TxnCtx
	if txnCtx.History != nil {
		err := e.ctx.NewTxn()
		if err != nil {
			return errors.Trace(err)
		}
	}
	// With START TRANSACTION, autocommit remains disabled until you end
	// the transaction with COMMIT or ROLLBACK. The autocommit mode then
	// reverts to its previous state.
	e.ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, true)
	// Call ctx.Txn(true) to active pending txn.
	if _, err := e.ctx.Txn(true); err != nil {
		return err
	}
	return nil
}

func (e *SimpleExec) executeCommit(s *ast.CommitStmt) {
	e.ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, false)
}

func (e *SimpleExec) executeRollback(s *ast.RollbackStmt) error {
	sessVars := e.ctx.GetSessionVars()
	logutil.Logger(context.Background()).Debug("execute rollback statement", zap.Uint64("conn", sessVars.ConnectionID))
	sessVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
	txn, err := e.ctx.Txn(false)
	if err != nil {
		return errors.Trace(err)
	}
	if txn.Valid() {
		e.ctx.GetSessionVars().TxnCtx.ClearDelta()
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
			if s.IsCreateRole && !checker.RequestVerification(activeRoles, "", "", "", mysql.CreateRolePriv) {
				return core.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE ROLE")
			}
			if !s.IsCreateRole && !checker.RequestVerification(activeRoles, "", "", "", mysql.CreateUserPriv) {
				return core.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE User")
			}
		}
	}

	users := make([]string, 0, len(s.Specs))
	for _, spec := range s.Specs {
		exists, err1 := userExists(e.ctx, spec.User.Username, spec.User.Hostname)
		if err1 != nil {
			return errors.Trace(err1)
		}
		if exists {
			if !s.IfNotExists {
				return errors.New("Duplicate user")
			}
			continue
		}
		pwd, ok := spec.EncodedPassword()
		if !ok {
			return errors.Trace(ErrPasswordFormat)
		}
		user := fmt.Sprintf(`('%s', '%s', '%s')`, spec.User.Hostname, spec.User.Username, pwd)
		users = append(users, user)
	}
	if len(users) == 0 {
		return nil
	}

	sql := fmt.Sprintf(`INSERT INTO %s.%s (Host, User, Password) VALUES %s;`, mysql.SystemDB, mysql.UserTable, strings.Join(users, ", "))
<<<<<<< HEAD
	_, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql)
=======
	if s.IsCreateRole {
		sql = fmt.Sprintf(`INSERT INTO %s.%s (Host, User, Password, Account_locked) VALUES %s;`, mysql.SystemDB, mysql.UserTable, strings.Join(users, ", "))
	}
	_, _, err := e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
>>>>>>> 8a4b60c... executor, privileges: fix privilege check fail for `CREATE USER` and `DROP USER` (#11589)
	if err != nil {
		return errors.Trace(err)
	}
	domain.GetDomain(e.ctx).NotifyUpdatePrivilege(e.ctx)
	return errors.Trace(err)
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

	failedUsers := make([]string, 0, len(s.Specs))
	for _, spec := range s.Specs {
		exists, err := userExists(e.ctx, spec.User.Username, spec.User.Hostname)
		if err != nil {
			return errors.Trace(err)
		}
		if !exists {
			failedUsers = append(failedUsers, spec.User.String())
			if s.IfExists {
				// TODO: Make this error as a warning.
			}
			continue
		}
		pwd := ""
		if spec.AuthOpt != nil {
			if spec.AuthOpt.ByAuthString {
				pwd = auth.EncodePassword(spec.AuthOpt.AuthString)
			} else {
				pwd = auth.EncodePassword(spec.AuthOpt.HashString)
			}
		}
		sql := fmt.Sprintf(`UPDATE %s.%s SET Password = '%s' WHERE Host = '%s' and User = '%s';`,
			mysql.SystemDB, mysql.UserTable, pwd, spec.User.Hostname, spec.User.Username)
		_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
		if err != nil {
			failedUsers = append(failedUsers, spec.User.String())
		}
	}
	if len(failedUsers) > 0 {
		// Commit the transaction even if we returns error
		txn, err := e.ctx.Txn(true)
		if err != nil {
			return errors.Trace(err)
		}
		err = txn.Commit(sessionctx.SetCommitCtx(context.Background(), e.ctx))
		if err != nil {
			return errors.Trace(err)
		}
		return ErrCannotUser.GenWithStackByArgs("ALTER USER", strings.Join(failedUsers, ","))
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
			if s.IsDropRole && !checker.RequestVerification(activeRoles, "", "", "", mysql.DropRolePriv) {
				return core.ErrSpecificAccessDenied.GenWithStackByArgs("DROP ROLE")
			}
			if !s.IsDropRole && !checker.RequestVerification(activeRoles, "", "", "", mysql.CreateUserPriv) {
				return core.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
			}
		}
	}

	failedUsers := make([]string, 0, len(s.UserList))
<<<<<<< HEAD
=======
	notExistUsers := make([]string, 0, len(s.UserList))
	sysSession, err := e.getSysSession()
	defer e.releaseSysSession(sysSession)
	if err != nil {
		return err
	}
	sqlExecutor := sysSession.(sqlexec.SQLExecutor)

>>>>>>> 8a4b60c... executor, privileges: fix privilege check fail for `CREATE USER` and `DROP USER` (#11589)
	for _, user := range s.UserList {
		exists, err := userExists(e.ctx, user.Username, user.Hostname)
		if err != nil {
			return errors.Trace(err)
		}
		if !exists {
			if !s.IfExists {
				failedUsers = append(failedUsers, user.String())
			}
			continue
		}

		// begin a transaction to delete a user.
<<<<<<< HEAD
		if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), "begin"); err != nil {
			return errors.Trace(err)
=======
		if _, err := sqlExecutor.Execute(context.Background(), "begin"); err != nil {
			return err
>>>>>>> 8a4b60c... executor, privileges: fix privilege check fail for `CREATE USER` and `DROP USER` (#11589)
		}
		sql := fmt.Sprintf(`DELETE FROM %s.%s WHERE Host = '%s' and User = '%s';`, mysql.SystemDB, mysql.UserTable, user.Hostname, user.Username)
		if _, err := sqlExecutor.Execute(context.Background(), sql); err != nil {
			failedUsers = append(failedUsers, user.String())
<<<<<<< HEAD
			if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), "rollback"); err != nil {
				return errors.Trace(err)
=======
			if _, err := sqlExecutor.Execute(context.Background(), "rollback"); err != nil {
				return err
>>>>>>> 8a4b60c... executor, privileges: fix privilege check fail for `CREATE USER` and `DROP USER` (#11589)
			}
			continue
		}

		// delete privileges from mysql.db
		sql = fmt.Sprintf(`DELETE FROM %s.%s WHERE Host = '%s' and User = '%s';`, mysql.SystemDB, mysql.DBTable, user.Hostname, user.Username)
		if _, err := sqlExecutor.Execute(context.Background(), sql); err != nil {
			failedUsers = append(failedUsers, user.String())
<<<<<<< HEAD
			if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), "rollback"); err != nil {
				return errors.Trace(err)
=======
			if _, err := sqlExecutor.Execute(context.Background(), "rollback"); err != nil {
				return err
>>>>>>> 8a4b60c... executor, privileges: fix privilege check fail for `CREATE USER` and `DROP USER` (#11589)
			}
			continue
		}

		// delete privileges from mysql.tables_priv
		sql = fmt.Sprintf(`DELETE FROM %s.%s WHERE Host = '%s' and User = '%s';`, mysql.SystemDB, mysql.TablePrivTable, user.Hostname, user.Username)
		if _, err := sqlExecutor.Execute(context.Background(), sql); err != nil {
			failedUsers = append(failedUsers, user.String())
<<<<<<< HEAD
			if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), "rollback"); err != nil {
				return errors.Trace(err)
=======
			if _, err := sqlExecutor.Execute(context.Background(), "rollback"); err != nil {
				return err
			}
			continue
		}

		// delete relationship from mysql.role_edges
		sql = fmt.Sprintf(`DELETE FROM %s.%s WHERE TO_HOST = '%s' and TO_USER = '%s';`, mysql.SystemDB, mysql.RoleEdgeTable, user.Hostname, user.Username)
		if _, err := sqlExecutor.Execute(context.Background(), sql); err != nil {
			failedUsers = append(failedUsers, user.String())
			if _, err := sqlExecutor.Execute(context.Background(), "rollback"); err != nil {
				return err
			}
			continue
		}

		sql = fmt.Sprintf(`DELETE FROM %s.%s WHERE FROM_HOST = '%s' and FROM_USER = '%s';`, mysql.SystemDB, mysql.RoleEdgeTable, user.Hostname, user.Username)
		if _, err := sqlExecutor.Execute(context.Background(), sql); err != nil {
			failedUsers = append(failedUsers, user.String())
			if _, err := sqlExecutor.Execute(context.Background(), "rollback"); err != nil {
				return err
			}
			continue
		}

		// delete relationship from mysql.default_roles
		sql = fmt.Sprintf(`DELETE FROM %s.%s WHERE DEFAULT_ROLE_HOST = '%s' and DEFAULT_ROLE_USER = '%s';`, mysql.SystemDB, mysql.DefaultRoleTable, user.Hostname, user.Username)
		if _, err := sqlExecutor.Execute(context.Background(), sql); err != nil {
			failedUsers = append(failedUsers, user.String())
			if _, err := sqlExecutor.Execute(context.Background(), "rollback"); err != nil {
				return err
			}
			continue
		}

		sql = fmt.Sprintf(`DELETE FROM %s.%s WHERE HOST = '%s' and USER = '%s';`, mysql.SystemDB, mysql.DefaultRoleTable, user.Hostname, user.Username)
		if _, err := sqlExecutor.Execute(context.Background(), sql); err != nil {
			failedUsers = append(failedUsers, user.String())
			if _, err := sqlExecutor.Execute(context.Background(), "rollback"); err != nil {
				return err
>>>>>>> 8a4b60c... executor, privileges: fix privilege check fail for `CREATE USER` and `DROP USER` (#11589)
			}
			continue
		}

		//TODO: need delete columns_priv once we implement columns_priv functionality.
		if _, err := sqlExecutor.Execute(context.Background(), "commit"); err != nil {
			failedUsers = append(failedUsers, user.String())
		}
	}
	if len(failedUsers) > 0 {
		return ErrCannotUser.GenWithStackByArgs("DROP USER", strings.Join(failedUsers, ","))
	}
	domain.GetDomain(e.ctx).NotifyUpdatePrivilege(e.ctx)
	return nil
}

func userExists(ctx sessionctx.Context, name string, host string) (bool, error) {
	sql := fmt.Sprintf(`SELECT * FROM %s.%s WHERE User='%s' AND Host='%s';`, mysql.SystemDB, mysql.UserTable, name, host)
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		return false, errors.Trace(err)
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
		if checker != nil && !checker.RequestVerification("", "", "", mysql.SuperPriv) {
			return ErrDBaccessDenied.GenWithStackByArgs(u, h, "mysql")
		}
		u = s.User.Username
		h = s.User.Hostname
	}
	exists, err := userExists(e.ctx, u, h)
	if err != nil {
		return errors.Trace(err)
	}
	if !exists {
		return errors.Trace(ErrPasswordNoMatch)
	}

	// update mysql.user
	sql := fmt.Sprintf(`UPDATE %s.%s SET password="%s" WHERE User="%s" AND Host="%s";`, mysql.SystemDB, mysql.UserTable, auth.EncodePassword(s.Password), u, h)
	_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
	domain.GetDomain(e.ctx).NotifyUpdatePrivilege(e.ctx)
	return errors.Trace(err)
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
		// TODO: A dummy implement
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
			return errors.Trace(err)
		}
		defer sysSessionPool.Put(ctx)
		err = dom.PrivilegeHandle().Update(ctx.(sessionctx.Context))
		return errors.Trace(err)
	case ast.FlushTiDBPlugin:
		dom := domain.GetDomain(e.ctx)
		for _, pluginName := range s.Plugins {
			err := plugin.NotifyFlush(dom, pluginName)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (e *SimpleExec) executeDropStats(s *ast.DropStatsStmt) error {
	h := domain.GetDomain(e.ctx).StatsHandle()
	err := h.DeleteTableStatsFromKV(s.Table.TableInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(h.Update(GetInfoSchema(e.ctx)))
}

func (e *SimpleExec) autoNewTxn() bool {
	switch e.Statement.(type) {
	case *ast.CreateUserStmt, *ast.AlterUserStmt, *ast.DropUserStmt:
		return true
	}
	return false
}
