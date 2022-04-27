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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
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
	if err := sessiontxn.NewTxnInStmt(ctx, e.ctx); err != nil {
		return err
	}
	defer func() { e.ctx.GetSessionVars().SetInTxn(false) }()

	// Create internal session to start internal transaction.
	isCommit := false
	internalSession, err := e.getSysSession()
	if err != nil {
		return err
	}
	defer func() {
		if !isCommit {
			_, err := internalSession.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), "rollback")
			if err != nil {
				logutil.BgLogger().Error("rollback error occur at grant privilege", zap.Error(err))
			}
		}
		e.releaseSysSession(internalSession)
	}()

	_, err = internalSession.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), "begin")
	if err != nil {
		return err
	}

	sessVars := e.ctx.GetSessionVars()
	// Revoke for each user.
	for _, user := range e.Users {
		if user.User.CurrentUser {
			user.User.Username = sessVars.User.AuthUsername
			user.User.Hostname = sessVars.User.AuthHostname
		}

		// Check if user exists.
		exists, err := userExists(ctx, e.ctx, user.User.Username, user.User.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return errors.Errorf("Unknown user: %s", user.User)
		}
		err = e.checkDynamicPrivilegeUsage()
		if err != nil {
			return err
		}
		err = e.revokeOneUser(internalSession, user.User.Username, user.User.Hostname)
		if err != nil {
			return err
		}
	}

	_, err = internalSession.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), "commit")
	if err != nil {
		return err
	}
	isCommit = true
	return domain.GetDomain(e.ctx).NotifyUpdatePrivilege()
}

// Checks that dynamic privileges are only of global scope.
// Returns the mysql-correct error when not the case.
func (e *RevokeExec) checkDynamicPrivilegeUsage() error {
	var dynamicPrivs []string
	for _, priv := range e.Privs {
		if priv.Priv == mysql.ExtendedPriv {
			dynamicPrivs = append(dynamicPrivs, strings.ToUpper(priv.Name))
		}
	}
	if len(dynamicPrivs) > 0 && e.Level.Level != ast.GrantLevelGlobal {
		return ErrIllegalPrivilegeLevel.GenWithStackByArgs(strings.Join(dynamicPrivs, ","))
	}
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

func (e *RevokeExec) revokeDynamicPriv(internalSession sessionctx.Context, privName string, user, host string) error {
	privName = strings.ToUpper(privName)
	if !privilege.GetPrivilegeManager(e.ctx).IsDynamicPrivilege(privName) { // for MySQL compatibility
		e.ctx.GetSessionVars().StmtCtx.AppendWarning(ErrDynamicPrivilegeNotRegistered.GenWithStackByArgs(privName))
	}
	_, err := internalSession.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), "DELETE FROM mysql.global_grants WHERE user = %? AND host = %? AND priv = %?", user, host, privName)
	return err
}

func (e *RevokeExec) revokeGlobalPriv(internalSession sessionctx.Context, priv *ast.PrivElem, user, host string) error {
	if priv.Priv == mysql.ExtendedPriv {
		return e.revokeDynamicPriv(internalSession, priv.Name, user, host)
	}
	if priv.Priv == mysql.AllPriv { // If ALL, also revoke dynamic privileges
		_, err := internalSession.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), "DELETE FROM mysql.global_grants WHERE user = %? AND host = %?", user, host)
		if err != nil {
			return err
		}
	}
	sql := new(strings.Builder)
	sqlexec.MustFormatSQL(sql, "UPDATE %n.%n SET ", mysql.SystemDB, mysql.UserTable)
	err := composeGlobalPrivUpdate(sql, priv.Priv, "N")
	if err != nil {
		return err
	}
	sqlexec.MustFormatSQL(sql, " WHERE User=%? AND Host=%?", user, strings.ToLower(host))

	_, err = internalSession.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql.String())
	return err
}

func (e *RevokeExec) revokeDBPriv(internalSession sessionctx.Context, priv *ast.PrivElem, userName, host string) error {
	dbName := e.Level.DBName
	if len(dbName) == 0 {
		dbName = e.ctx.GetSessionVars().CurrentDB
	}

	sql := new(strings.Builder)
	sqlexec.MustFormatSQL(sql, "UPDATE %n.%n SET ", mysql.SystemDB, mysql.DBTable)
	err := composeDBPrivUpdate(sql, priv.Priv, "N")
	if err != nil {
		return err
	}
	sqlexec.MustFormatSQL(sql, " WHERE User=%? AND Host=%? AND DB=%?", userName, host, dbName)

	_, err = internalSession.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql.String())
	return err
}

func (e *RevokeExec) revokeTablePriv(internalSession sessionctx.Context, priv *ast.PrivElem, user, host string) error {
	dbName, tbl, err := getTargetSchemaAndTable(e.ctx, e.Level.DBName, e.Level.TableName, e.is)
	if err != nil && !terror.ErrorEqual(err, infoschema.ErrTableNotExists) {
		return err
	}

	// Allow REVOKE on non-existent table, see issue #28533
	tblName := e.Level.TableName
	if tbl != nil {
		tblName = tbl.Meta().Name.O
	}
	sql := new(strings.Builder)
	sqlexec.MustFormatSQL(sql, "UPDATE %n.%n SET ", mysql.SystemDB, mysql.TablePrivTable)
	err = composeTablePrivUpdateForRevoke(internalSession, sql, priv.Priv, user, host, dbName, tblName)
	if err != nil {
		return err
	}
	sqlexec.MustFormatSQL(sql, " WHERE User=%? AND Host=%? AND DB=%? AND Table_name=%?", user, host, dbName, tblName)

	_, err = internalSession.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql.String())
	return err
}

func (e *RevokeExec) revokeColumnPriv(internalSession sessionctx.Context, priv *ast.PrivElem, user, host string) error {
	dbName, tbl, err := getTargetSchemaAndTable(e.ctx, e.Level.DBName, e.Level.TableName, e.is)
	if err != nil {
		return err
	}
	sql := new(strings.Builder)
	for _, c := range priv.Cols {
		col := table.FindCol(tbl.Cols(), c.Name.L)
		if col == nil {
			return errors.Errorf("Unknown column: %s", c)
		}

		sql.Reset()
		sqlexec.MustFormatSQL(sql, "UPDATE %n.%n SET ", mysql.SystemDB, mysql.ColumnPrivTable)
		err = composeColumnPrivUpdateForRevoke(internalSession, sql, priv.Priv, user, host, dbName, tbl.Meta().Name.O, col.Name.O)
		if err != nil {
			return err
		}
		sqlexec.MustFormatSQL(sql, " WHERE User=%? AND Host=%? AND DB=%? AND Table_name=%? AND Column_name=%?", user, host, dbName, tbl.Meta().Name.O, col.Name.O)

		_, err = internalSession.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql.String())
		if err != nil {
			return err
		}
	}
	return nil
}

func privUpdateForRevoke(cur []string, priv mysql.PrivilegeType) ([]string, error) {
	p, ok := mysql.Priv2SetStr[priv]
	if !ok {
		return nil, errors.Errorf("Unknown priv: %v", priv)
	}
	cur = deleteFromSet(cur, p)
	return cur, nil
}

func composeTablePrivUpdateForRevoke(ctx sessionctx.Context, sql *strings.Builder, priv mysql.PrivilegeType, name string, host string, db string, tbl string) error {
	var newTablePriv, newColumnPriv []string

	currTablePriv, currColumnPriv, err := getTablePriv(ctx, name, host, db, tbl)
	if err != nil {
		return err
	}

	if priv == mysql.AllPriv {
		// Revoke ALL does not revoke the Grant option,
		// so we only need to check if the user previously had this.
		tmp := SetFromString(currTablePriv)
		for _, p := range tmp {
			if p == mysql.Priv2SetStr[mysql.GrantPriv] {
				newTablePriv = []string{mysql.Priv2SetStr[mysql.GrantPriv]}
			}
		}
	} else {
		newTablePriv = SetFromString(currTablePriv)
		newTablePriv, err = privUpdateForRevoke(newTablePriv, priv)
		if err != nil {
			return err
		}

		newColumnPriv = SetFromString(currColumnPriv)
		newColumnPriv, err = privUpdateForRevoke(newColumnPriv, priv)
		if err != nil {
			return err
		}
	}

	sqlexec.MustFormatSQL(sql, `Table_priv=%?, Column_priv=%?, Grantor=%?`, strings.Join(newTablePriv, ","), strings.Join(newColumnPriv, ","), ctx.GetSessionVars().User.String())
	return nil
}

func composeColumnPrivUpdateForRevoke(ctx sessionctx.Context, sql *strings.Builder, priv mysql.PrivilegeType, name string, host string, db string, tbl string, col string) error {
	var newColumnPriv []string

	if priv != mysql.AllPriv {
		currColumnPriv, err := getColumnPriv(ctx, name, host, db, tbl, col)
		if err != nil {
			return err
		}

		newColumnPriv = SetFromString(currColumnPriv)
		newColumnPriv, err = privUpdateForRevoke(newColumnPriv, priv)
		if err != nil {
			return err
		}
	}

	sqlexec.MustFormatSQL(sql, `Column_priv=%?`, strings.Join(newColumnPriv, ","))
	return nil
}
