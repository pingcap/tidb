// Copyright 2017 PingCAP, Inc.
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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	goctx "golang.org/x/net/context"
)

/***
 * Revoke Statement
 * See https://dev.mysql.com/doc/refman/5.7/en/revoke.html
 ************************************************************************************/
var (
	_ Executor = (*RevokeExec)(nil)
)

// RevokeExec executes RevokeStmt.
type RevokeExec struct {
	baseExecutor

	Privs      []*ast.PrivElem
	ObjectType ast.ObjectTypeType
	Level      *ast.GrantLevel
	Users      []*ast.UserSpec

	ctx  context.Context
	is   infoschema.InfoSchema
	done bool
}

// Next implements Execution Next interface.
func (e *RevokeExec) Next(goCtx goctx.Context) (Row, error) {
	if e.done {
		return nil, nil
	}
	e.done = true
	return nil, errors.Trace(e.run(goCtx))
}

// NextChunk implements the Executor NextChunk interface.
func (e *RevokeExec) NextChunk(goCtx goctx.Context, chk *chunk.Chunk) error {
	if e.done {
		return nil
	}
	e.done = true
	return errors.Trace(e.run(goCtx))
}

func (e *RevokeExec) run(goCtx goctx.Context) error {
	// Revoke for each user
	for _, user := range e.Users {
		// Check if user exists.
		exists, err := userExists(e.ctx, user.User.Username, user.User.Hostname)
		if err != nil {
			return errors.Trace(err)
		}
		if !exists {
			return errors.Errorf("Unknown user: %s", user.User)
		}

		err = e.revokeOneUser(user.User.Username, user.User.Hostname)
		if err != nil {
			return errors.Trace(err)
		}
	}
	domain.GetDomain(e.ctx).NotifyUpdatePrivilege(e.ctx)
	return nil
}

func (e *RevokeExec) revokeOneUser(user, host string) error {
	dbName := e.Level.DBName
	if len(dbName) == 0 {
		dbName = e.ctx.GetSessionVars().CurrentDB
	}

	// If there is no privilege entry in corresponding table, insert a new one.
	// DB scope:		mysql.DB
	// Table scope:		mysql.Tables_priv
	// Column scope:	mysql.Columns_priv
	switch e.Level.Level {
	case ast.GrantLevelDB:
		ok, err := dbUserExists(e.ctx, user, host, dbName)
		if err != nil {
			return errors.Trace(err)
		}
		if !ok {
			return errors.Errorf("There is no such grant defined for user '%s' on host '%s' on database %s", user, host, dbName)
		}
	case ast.GrantLevelTable:
		ok, err := tableUserExists(e.ctx, user, host, dbName, e.Level.TableName)
		if err != nil {
			return errors.Trace(err)
		}
		if !ok {
			return errors.Errorf("There is no such grant defined for user '%s' on host '%s' on table %s.%s", user, host, dbName, e.Level.TableName)
		}
	}

	for _, priv := range e.Privs {
		err := e.revokePriv(priv, user, host)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *RevokeExec) revokePriv(priv *ast.PrivElem, user, host string) error {
	switch e.Level.Level {
	case ast.GrantLevelGlobal:
		return e.revokeGlobalPriv(priv, user, host)
	case ast.GrantLevelDB:
		return e.revokeDBPriv(priv, user, host)
	case ast.GrantLevelTable:
		if len(priv.Cols) == 0 {
			return e.revokeTablePriv(priv, user, host)
		}
		return e.revokeColumnPriv(priv, user, host)
	}
	return errors.Errorf("Unknown revoke level: %#v", e.Level)
}

func (e *RevokeExec) revokeGlobalPriv(priv *ast.PrivElem, user, host string) error {
	asgns, err := composeGlobalPrivUpdate(priv.Priv, "N")
	if err != nil {
		return errors.Trace(err)
	}
	sql := fmt.Sprintf(`UPDATE %s.%s SET %s WHERE User="%s" AND Host="%s"`, mysql.SystemDB, mysql.UserTable, asgns, user, host)
	_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
	return errors.Trace(err)
}

func (e *RevokeExec) revokeDBPriv(priv *ast.PrivElem, userName, host string) error {
	dbName := e.Level.DBName
	if len(dbName) == 0 {
		dbName = e.ctx.GetSessionVars().CurrentDB
	}
	asgns, err := composeDBPrivUpdate(priv.Priv, "N")
	if err != nil {
		return errors.Trace(err)
	}
	sql := fmt.Sprintf(`UPDATE %s.%s SET %s WHERE User="%s" AND Host="%s" AND DB="%s";`, mysql.SystemDB, mysql.DBTable, asgns, userName, host, dbName)
	_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
	return errors.Trace(err)
}

func (e *RevokeExec) revokeTablePriv(priv *ast.PrivElem, user, host string) error {
	dbName, tbl, err := getTargetSchemaAndTable(e.ctx, e.Level.DBName, e.Level.TableName, e.is)
	if err != nil {
		return errors.Trace(err)
	}
	asgns, err := composeTablePrivUpdateForRevoke(e.ctx, priv.Priv, user, host, dbName, tbl.Meta().Name.O)
	if err != nil {
		return errors.Trace(err)
	}
	sql := fmt.Sprintf(`UPDATE %s.%s SET %s WHERE User="%s" AND Host="%s" AND DB="%s" AND Table_name="%s";`, mysql.SystemDB, mysql.TablePrivTable, asgns, user, host, dbName, tbl.Meta().Name.O)
	_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
	return errors.Trace(err)
}

func (e *RevokeExec) revokeColumnPriv(priv *ast.PrivElem, user, host string) error {
	dbName, tbl, err := getTargetSchemaAndTable(e.ctx, e.Level.DBName, e.Level.TableName, e.is)
	if err != nil {
		return errors.Trace(err)
	}
	for _, c := range priv.Cols {
		col := table.FindCol(tbl.Cols(), c.Name.L)
		if col == nil {
			return errors.Errorf("Unknown column: %s", c)
		}
		asgns, err := composeColumnPrivUpdateForRevoke(e.ctx, priv.Priv, user, host, dbName, tbl.Meta().Name.O, col.Name.O)
		if err != nil {
			return errors.Trace(err)
		}
		sql := fmt.Sprintf(`UPDATE %s.%s SET %s WHERE User="%s" AND Host="%s" AND DB="%s" AND Table_name="%s" AND Column_name="%s";`, mysql.SystemDB, mysql.ColumnPrivTable, asgns, user, host, dbName, tbl.Meta().Name.O, col.Name.O)
		_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
