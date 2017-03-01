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

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/sqlexec"
)

// SimpleExec represents simple statement executor.
// For statements do simple execution.
// includes `UseStmt`, 'SetStmt`, `DoStmt`,
// `BeginStmt`, `CommitStmt`, `RollbackStmt`.
// TODO: list all simple statements.
type SimpleExec struct {
	Statement ast.StmtNode
	ctx       context.Context
	done      bool
	is        infoschema.InfoSchema
}

// Schema implements the Executor Schema interface.
func (e *SimpleExec) Schema() *expression.Schema {
	return expression.NewSchema()
}

// Next implements Execution Next interface.
func (e *SimpleExec) Next() (*Row, error) {
	if e.done {
		return nil, nil
	}
	var err error
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
		err = e.executeCreateUser(x)
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
		return nil, nil
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	e.done = true
	return nil, nil
}

// Close implements the Executor Close interface.
func (e *SimpleExec) Close() error {
	return nil
}

func (e *SimpleExec) executeUse(s *ast.UseStmt) error {
	dbname := model.NewCIStr(s.DBName)
	dbinfo, exists := e.is.SchemaByName(dbname)
	if !exists {
		return infoschema.ErrDatabaseNotExists.GenByArgs(dbname)
	}
	e.ctx.GetSessionVars().CurrentDB = dbname.O
	// character_set_database is the character set used by the default database.
	// The server sets this variable whenever the default database changes.
	// See http://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_character_set_database
	sessionVars := e.ctx.GetSessionVars()
	sessionVars.Systems[variable.CharsetDatabase] = dbinfo.Charset
	sessionVars.Systems[variable.CollationDatabase] = dbinfo.Collate
	return nil
}

func (e *SimpleExec) executeBegin(s *ast.BeginStmt) error {
	// If BEGIN is the first statement in TxnCtx, we can reuse the existing transaction, without the
	// need to call NewTxn, which commits the existing transaction and begins a new one.
	txnCtx := e.ctx.GetSessionVars().TxnCtx
	if txnCtx.Histroy != nil {
		err := e.ctx.NewTxn()
		if err != nil {
			return errors.Trace(err)
		}
	}
	// With START TRANSACTION, autocommit remains disabled until you end
	// the transaction with COMMIT or ROLLBACK. The autocommit mode then
	// reverts to its previous state.
	e.ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, true)
	return nil
}

func (e *SimpleExec) executeCommit(s *ast.CommitStmt) {
	e.ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, false)
}

func (e *SimpleExec) executeRollback(s *ast.RollbackStmt) error {
	sessVars := e.ctx.GetSessionVars()
	log.Infof("[%d] execute rollback statement", sessVars.ConnectionID)
	sessVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
	if e.ctx.Txn().Valid() {
		return e.ctx.Txn().Rollback()
	}
	return nil
}

func (e *SimpleExec) executeCreateUser(s *ast.CreateUserStmt) error {
	users := make([]string, 0, len(s.Specs))
	for _, spec := range s.Specs {
		userName, host := parseUser(spec.User)
		exists, err1 := userExists(e.ctx, userName, host)
		if err1 != nil {
			return errors.Trace(err1)
		}
		if exists {
			if !s.IfNotExists {
				return errors.New("Duplicate user")
			}
			continue
		}
		pwd := ""
		if spec.AuthOpt != nil {
			if spec.AuthOpt.ByAuthString {
				pwd = util.EncodePassword(spec.AuthOpt.AuthString)
			} else {
				pwd = util.EncodePassword(spec.AuthOpt.HashString)
			}
		}
		user := fmt.Sprintf(`("%s", "%s", "%s")`, host, userName, pwd)
		users = append(users, user)
	}
	if len(users) == 0 {
		return nil
	}
	sql := fmt.Sprintf(`INSERT INTO %s.%s (Host, User, Password) VALUES %s;`, mysql.SystemDB, mysql.UserTable, strings.Join(users, ", "))
	_, err := e.ctx.(sqlexec.SQLExecutor).Execute(sql)
	if err != nil {
		return errors.Trace(err)
	}

	// Flush privileges.
	dom := sessionctx.GetDomain(e.ctx)
	err = dom.PrivilegeHandle().Update()
	return errors.Trace(err)
}

func (e *SimpleExec) executeAlterUser(s *ast.AlterUserStmt) error {
	if s.CurrentAuth != nil {
		user := e.ctx.GetSessionVars().User
		if len(user) == 0 {
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
		userName, host := parseUser(spec.User)
		exists, err := userExists(e.ctx, userName, host)
		if err != nil {
			return errors.Trace(err)
		}
		if !exists {
			failedUsers = append(failedUsers, spec.User)
			if s.IfExists {
				// TODO: Make this error as a warning.
			}
			continue
		}
		pwd := ""
		if spec.AuthOpt != nil {
			if spec.AuthOpt.ByAuthString {
				pwd = util.EncodePassword(spec.AuthOpt.AuthString)
			} else {
				pwd = util.EncodePassword(spec.AuthOpt.HashString)
			}
		}
		sql := fmt.Sprintf(`UPDATE %s.%s SET Password = "%s" WHERE Host = "%s" and User = "%s";`,
			mysql.SystemDB, mysql.UserTable, pwd, host, userName)
		_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
		if err != nil {
			failedUsers = append(failedUsers, spec.User)
		}
	}
	if len(failedUsers) > 0 {
		// Commit the transaction even if we returns error
		err := e.ctx.Txn().Commit()
		if err != nil {
			return errors.Trace(err)
		}
		errMsg := "Operation ALTER USER failed for " + strings.Join(failedUsers, ",")
		return terror.ClassExecutor.New(CodeCannotUser, errMsg)
	}
	return nil
}

func (e *SimpleExec) executeDropUser(s *ast.DropUserStmt) error {
	failedUsers := make([]string, 0, len(s.UserList))
	for _, user := range s.UserList {
		userName, host := parseUser(user)
		exists, err := userExists(e.ctx, userName, host)
		if err != nil {
			return errors.Trace(err)
		}
		if !exists {
			if !s.IfExists {
				failedUsers = append(failedUsers, user)
			}
			continue
		}
		sql := fmt.Sprintf(`DELETE FROM %s.%s WHERE Host = "%s" and User = "%s";`, mysql.SystemDB, mysql.UserTable, host, userName)
		_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
		if err != nil {
			failedUsers = append(failedUsers, user)
		}
	}
	if len(failedUsers) > 0 {
		// Commit the transaction even if we returns error
		err := e.ctx.Txn().Commit()
		if err != nil {
			return errors.Trace(err)
		}
		errMsg := "Operation DROP USER failed for " + strings.Join(failedUsers, ",")
		return terror.ClassExecutor.New(CodeCannotUser, errMsg)
	}
	return nil
}

// parse user string into username and host
// root@localhost -> root, localhost
func parseUser(user string) (string, string) {
	strs := strings.Split(user, "@")
	return strs[0], strs[1]
}

func userExists(ctx context.Context, name string, host string) (bool, error) {
	sql := fmt.Sprintf(`SELECT * FROM %s.%s WHERE User="%s" AND Host="%s";`, mysql.SystemDB, mysql.UserTable, name, host)
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		return false, errors.Trace(err)
	}
	return len(rows) > 0, nil
}

func (e *SimpleExec) executeSetPwd(s *ast.SetPwdStmt) error {
	if len(s.User) == 0 {
		vars := e.ctx.GetSessionVars()
		s.User = vars.User
		if len(s.User) == 0 {
			return errors.New("Session error is empty")
		}
	}
	userName, host := parseUser(s.User)
	exists, err := userExists(e.ctx, userName, host)
	if err != nil {
		return errors.Trace(err)
	}
	if !exists {
		return errors.Trace(ErrPasswordNoMatch)
	}

	// update mysql.user
	sql := fmt.Sprintf(`UPDATE %s.%s SET password="%s" WHERE User="%s" AND Host="%s";`, mysql.SystemDB, mysql.UserTable, util.EncodePassword(s.Password), userName, host)
	_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
	return errors.Trace(err)
}

func (e *SimpleExec) executeKillStmt(s *ast.KillStmt) error {
	// TODO: Implement it.
	return nil
}

func (e *SimpleExec) executeFlush(s *ast.FlushStmt) error {
	switch s.Tp {
	case ast.FlushTables:
		// TODO: A dummy implement
	case ast.FlushPrivileges:
		dom := sessionctx.GetDomain(e.ctx)
		err := dom.PrivilegeHandle().Update()
		return errors.Trace(err)
	}
	return nil
}
