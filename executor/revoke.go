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
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
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

	ctx  sessionctx.Context
	is   infoschema.InfoSchema
	done bool
}

// Next implements the Executor Next interface.
func (e *RevokeExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.done {
		return nil
	}
	e.done = true

	// Commit the old transaction, like DDL.
	if err := e.ctx.NewTxn(ctx); err != nil {
		return err
	}
	defer func() { e.ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, false) }()

	// Create internal session to start internal transaction.
	isCommit := false
	internalSession, err := e.getSysSession()
	if err != nil {
		return err
	}
	defer func() {
		if !isCommit {
			_, err := internalSession.(sqlexec.SQLExecutor).Execute(context.Background(), "rollback")
			if err != nil {
				logutil.BgLogger().Error("rollback error occur at grant privilege", zap.Error(err))
			}
		}
		e.releaseSysSession(internalSession)
	}()

	_, err = internalSession.(sqlexec.SQLExecutor).Execute(context.Background(), "begin")
	if err != nil {
		return err
	}

	// Revoke for each user.
	for _, user := range e.Users {
		// Check if user exists.
		exists, err := userExists(e.ctx, user.User.Username, user.User.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return errors.Errorf("Unknown user: %s", user.User)
		}

		err = e.revokeOneUser(internalSession, user.User.Username, user.User.Hostname)
		if err != nil {
			return err
		}
	}

	_, err = internalSession.(sqlexec.SQLExecutor).Execute(context.Background(), "commit")
	if err != nil {
		return err
	}
	isCommit = true
	domain.GetDomain(e.ctx).NotifyUpdatePrivilege(e.ctx)
	return nil
}

func (e *RevokeExec) revokeOneUser(internalSession sessionctx.Context, user, host string) error {
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
		ok, err := dbUserExists(internalSession, user, host, dbName)
		if err != nil {
			return err
		}
		if !ok {
			return errors.Errorf("There is no such grant defined for user '%s' on host '%s' on database %s", user, host, dbName)
		}
	case ast.GrantLevelTable:
		ok, err := tableUserExists(internalSession, user, host, dbName, e.Level.TableName)
		if err != nil {
			return err
		}
		if !ok {
			return errors.Errorf("There is no such grant defined for user '%s' on host '%s' on table %s.%s", user, host, dbName, e.Level.TableName)
		}
	}

	for _, priv := range e.Privs {
		err := e.revokePriv(internalSession, priv, user, host)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *RevokeExec) revokePriv(internalSession sessionctx.Context, priv *ast.PrivElem, user, host string) error {
	switch e.Level.Level {
	case ast.GrantLevelGlobal:
		return e.revokeGlobalPriv(internalSession, priv, user, host)
	case ast.GrantLevelDB:
		return e.revokeDBPriv(internalSession, priv, user, host)
	case ast.GrantLevelTable:
		if len(priv.Cols) == 0 {
			return e.revokeTablePriv(internalSession, priv, user, host)
		}
		return e.revokeColumnPriv(internalSession, priv, user, host)
	}
	return errors.Errorf("Unknown revoke level: %#v", e.Level)
}

func (e *RevokeExec) revokeGlobalPriv(internalSession sessionctx.Context, priv *ast.PrivElem, user, host string) error {
	asgns, err := composeGlobalPrivUpdate(priv.Priv, "N")
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(`UPDATE %s.%s SET %s WHERE User='%s' AND Host='%s'`, mysql.SystemDB, mysql.UserTable, asgns, user, host)
	_, err = internalSession.(sqlexec.SQLExecutor).Execute(context.Background(), sql)
	return err
}

func (e *RevokeExec) revokeDBPriv(internalSession sessionctx.Context, priv *ast.PrivElem, userName, host string) error {
	dbName := e.Level.DBName
	if len(dbName) == 0 {
		dbName = e.ctx.GetSessionVars().CurrentDB
	}
	asgns, err := composeDBPrivUpdate(priv.Priv, "N")
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(`UPDATE %s.%s SET %s WHERE User='%s' AND Host='%s' AND DB='%s';`, mysql.SystemDB, mysql.DBTable, asgns, userName, host, dbName)
	_, err = internalSession.(sqlexec.SQLExecutor).Execute(context.Background(), sql)
	return err
}

func (e *RevokeExec) revokeTablePriv(internalSession sessionctx.Context, priv *ast.PrivElem, user, host string) error {
	dbName, tbl, err := getTargetSchemaAndTable(e.ctx, e.Level.DBName, e.Level.TableName, e.is)
	if err != nil {
		return err
	}
	asgns, err := composeTablePrivUpdateForRevoke(internalSession, priv.Priv, user, host, dbName, tbl.Meta().Name.O)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(`UPDATE %s.%s SET %s WHERE User='%s' AND Host='%s' AND DB='%s' AND Table_name='%s';`, mysql.SystemDB, mysql.TablePrivTable, asgns, user, host, dbName, tbl.Meta().Name.O)
	_, err = internalSession.(sqlexec.SQLExecutor).Execute(context.Background(), sql)
	return err
}

func (e *RevokeExec) revokeColumnPriv(internalSession sessionctx.Context, priv *ast.PrivElem, user, host string) error {
	dbName, tbl, err := getTargetSchemaAndTable(e.ctx, e.Level.DBName, e.Level.TableName, e.is)
	if err != nil {
		return err
	}
	for _, c := range priv.Cols {
		col := table.FindCol(tbl.Cols(), c.Name.L)
		if col == nil {
			return errors.Errorf("Unknown column: %s", c)
		}
		asgns, err := composeColumnPrivUpdateForRevoke(internalSession, priv.Priv, user, host, dbName, tbl.Meta().Name.O, col.Name.O)
		if err != nil {
			return err
		}
		sql := fmt.Sprintf(`UPDATE %s.%s SET %s WHERE User='%s' AND Host='%s' AND DB='%s' AND Table_name='%s' AND Column_name='%s';`, mysql.SystemDB, mysql.ColumnPrivTable, asgns, user, host, dbName, tbl.Meta().Name.O, col.Name.O)
		_, err = internalSession.(sqlexec.SQLExecutor).Execute(context.Background(), sql)
		if err != nil {
			return err
		}
	}
	return nil
}
