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
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

/***
 * Revoke Statement
 * See https://dev.mysql.com/doc/refman/5.7/en/revoke.html
 ************************************************************************************/
var (
	_ exec.Executor = (*RevokeExec)(nil)
)

// RevokeExec executes RevokeStmt.
type RevokeExec struct {
	exec.BaseExecutor

	Privs      []*ast.PrivElem
	ObjectType ast.ObjectTypeType
	Level      *ast.GrantLevel
	Users      []*ast.UserSpec

	ctx  sessionctx.Context
	is   infoschema.InfoSchema
	done bool
}

// Next implements the Executor Next interface.
func (e *RevokeExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	if e.done {
		return nil
	}
	e.done = true
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)

	// Commit the old transaction, like DDL.
	if err := sessiontxn.NewTxnInStmt(ctx, e.Ctx()); err != nil {
		return err
	}
	defer func() { e.Ctx().GetSessionVars().SetInTxn(false) }()

	// Create internal session to start internal transaction.
	isCommit := false
	internalSession, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer func() {
		if !isCommit {
			_, err := internalSession.GetSQLExecutor().ExecuteInternal(internalCtx, "rollback")
			if err != nil {
				logutil.BgLogger().Error("rollback error occur at grant privilege", zap.Error(err))
			}
		}
		e.ReleaseSysSession(internalCtx, internalSession)
	}()

	_, err = internalSession.GetSQLExecutor().ExecuteInternal(internalCtx, "begin")
	if err != nil {
		return err
	}

	sessVars := e.Ctx().GetSessionVars()
	// Revoke for each user.
	for _, user := range e.Users {
		if user.User.CurrentUser {
			user.User.Username = sessVars.User.AuthUsername
			user.User.Hostname = sessVars.User.AuthHostname
		}

		// Check if user exists.
		exists, err := userExists(ctx, e.Ctx(), user.User.Username, user.User.Hostname)
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
		err = e.revokeOneUser(ctx, internalSession, user.User.Username, user.User.Hostname)
		if err != nil {
			return err
		}
	}

	_, err = internalSession.GetSQLExecutor().ExecuteInternal(internalCtx, "commit")
	if err != nil {
		return err
	}
	isCommit = true
	users := userSpecToUserList(e.Users)
	return domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege(users)
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
		return exeerrors.ErrIllegalPrivilegeLevel.GenWithStackByArgs(strings.Join(dynamicPrivs, ","))
	}
	return nil
}

func (e *RevokeExec) revokeOneUser(ctx context.Context, internalSession sessionctx.Context, user, host string) error {
	dbName := e.Level.DBName
	if len(dbName) == 0 {
		dbName = e.Ctx().GetSessionVars().CurrentDB
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
		err := e.revokePriv(ctx, internalSession, priv, user, host)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *RevokeExec) revokePriv(ctx context.Context, internalSession sessionctx.Context, priv *ast.PrivElem, user, host string) error {
	if priv.Priv == mysql.UsagePriv {
		return nil
	}
	switch e.Level.Level {
	case ast.GrantLevelGlobal:
		return e.revokeGlobalPriv(internalSession, priv, user, host)
	case ast.GrantLevelDB:
		return e.revokeDBPriv(internalSession, priv, user, host)
	case ast.GrantLevelTable:
		if len(priv.Cols) == 0 {
			return e.revokeTablePriv(ctx, internalSession, priv, user, host, true)
		}
		return e.revokeColumnPriv(ctx, internalSession, priv, user, host, true)
	}
	return errors.Errorf("Unknown revoke level: %#v", e.Level)
}

func (e *RevokeExec) revokeDynamicPriv(internalSession sessionctx.Context, privName string, user, host string) error {
	privName = strings.ToUpper(privName)
	if !privilege.GetPrivilegeManager(e.Ctx()).IsDynamicPrivilege(privName) { // for MySQL compatibility
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(exeerrors.ErrDynamicPrivilegeNotRegistered.FastGenByArgs(privName))
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	_, err := internalSession.GetSQLExecutor().ExecuteInternal(ctx, "DELETE FROM mysql.global_grants WHERE user = %? AND host = %? AND priv = %?", user, host, privName)
	return err
}

func (e *RevokeExec) revokeGlobalPriv(internalSession sessionctx.Context, priv *ast.PrivElem, user, host string) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	if priv.Priv == mysql.ExtendedPriv {
		return e.revokeDynamicPriv(internalSession, priv.Name, user, host)
	}
	if priv.Priv == mysql.AllPriv { // If ALL, also revoke dynamic privileges
		_, err := internalSession.GetSQLExecutor().ExecuteInternal(ctx, "DELETE FROM mysql.global_grants WHERE user = %? AND host = %?", user, host)
		if err != nil {
			return err
		}
	}
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, "UPDATE %n.%n SET ", mysql.SystemDB, mysql.UserTable)
	err := composeGlobalPrivUpdate(sql, priv.Priv, "N")
	if err != nil {
		return err
	}
	sqlescape.MustFormatSQL(sql, " WHERE User=%? AND Host=%?", user, strings.ToLower(host))

	_, err = internalSession.GetSQLExecutor().ExecuteInternal(ctx, sql.String())
	return err
}

func (e *RevokeExec) revokeDBPriv(internalSession sessionctx.Context, priv *ast.PrivElem, userName, host string) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	dbName := e.Level.DBName
	if len(dbName) == 0 {
		dbName = e.Ctx().GetSessionVars().CurrentDB
	}

	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, "UPDATE %n.%n SET ", mysql.SystemDB, mysql.DBTable)
	err := composeDBPrivUpdate(sql, priv.Priv, "N")
	if err != nil {
		return err
	}
	sqlescape.MustFormatSQL(sql, " WHERE User=%? AND Host=%? AND DB=%?", userName, host, dbName)

	_, err = internalSession.GetSQLExecutor().ExecuteInternal(ctx, sql.String())
	if err != nil {
		return err
	}

	sql = new(strings.Builder)
	sqlescape.MustFormatSQL(sql, "DELETE FROM %n.%n WHERE User=%? AND Host=%? AND DB=%?", mysql.SystemDB, mysql.DBTable, userName, host, dbName)

	for _, v := range append(mysql.AllDBPrivs, mysql.GrantPriv) {
		sqlescape.MustFormatSQL(sql, " AND %n='N'", v.ColumnString())
	}
	_, err = internalSession.GetSQLExecutor().ExecuteInternal(ctx, sql.String())
	return err
}

// checkColumnPrivTbl indicates whether we should remove records in mysql.columns_priv if corresponding column privileges exist
func (e *RevokeExec) revokeTablePriv(ctx context.Context, internalSession sessionctx.Context, priv *ast.PrivElem,
	user, host string, checkColumnPrivTbl bool) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnPrivilege)
	dbName, tbl, err := getTargetSchemaAndTable(ctx, e.Ctx(), e.Level.DBName, e.Level.TableName, e.is)
	if err != nil && !terror.ErrorEqual(err, infoschema.ErrTableNotExists) {
		return err
	}

	// Allow REVOKE on non-existent table, see issue #28533
	tblName := e.Level.TableName
	if tbl != nil {
		tblName = tbl.Meta().Name.O
	}

	// 1. update mysql.tables_priv
	columnPrivUpdated, err := removePrivFromTablePriv(ctx, internalSession, priv.Priv, host, user, dbName, tblName, checkColumnPrivTbl)
	if err != nil {
		return err
	}

	// 2. update mysql.columns_priv to delete all records related to this privilege
	if checkColumnPrivTbl && columnPrivUpdated {
		p := &ast.PrivElem{Priv: priv.Priv, Cols: []*ast.ColumnName{}}
		if err = e.revokeColumnPriv(ctx, internalSession, p, user, host, false); err != nil {
			return err
		}
	}
	return err
}

// Remove priv from Table_priv and Column_priv in mysql.tables_priv
// It always removes from Column_priv. If removeTableScopePriv is true, it removes from Table_priv.
// The return value columnPrivUpdated indicates whether some Column_priv have been updated.
func removePrivFromTablePriv(ctx context.Context, sctx sessionctx.Context,
	priv mysql.PrivilegeType, host, user, db, table string, removeTableScopePriv bool) (columnPrivUpdated bool, err error) {
	sql := new(strings.Builder)
	if priv == mysql.AllPriv {
		if removeTableScopePriv {
			// Revoke ALL does not revoke the Grant option,
			// so we only need to check if the user previously had this.
			sqlescape.MustFormatSQL(sql,
				"UPDATE %n.%n SET Table_priv = '' WHERE User=%? AND Host=%? AND DB=%? AND Table_name=%? AND find_in_set('Grant', Table_priv) = 0",
				mysql.SystemDB, mysql.TablePrivTable, user, host, db, table)
			_, err = sctx.GetSQLExecutor().ExecuteInternal(ctx, sql.String())
			if err != nil {
				return
			}
			sql.Reset()
			sqlescape.MustFormatSQL(sql,
				"UPDATE %n.%n SET Table_priv = 'Grant' WHERE User=%? AND Host=%? AND DB=%? AND Table_name=%? AND find_in_set('Grant', Table_priv) > 0",
				mysql.SystemDB, mysql.TablePrivTable, user, host, db, table)
			_, err = sctx.GetSQLExecutor().ExecuteInternal(ctx, sql.String())
			if err != nil {
				return
			}
		}

		sql.Reset()
		sqlescape.MustFormatSQL(sql,
			"UPDATE %n.%n SET Column_priv = '' WHERE User=%? AND Host=%? AND DB=%? AND Table_name=%?",
			mysql.SystemDB, mysql.TablePrivTable, user, host, db, table)
		_, err = sctx.GetSQLExecutor().ExecuteInternal(ctx, sql.String())
		if err != nil {
			return
		}
		columnPrivUpdated = sctx.GetSessionVars().StmtCtx.AffectedRows() > 0
	} else {
		if removeTableScopePriv {
			s := strings.Join([]string{"UPDATE %n.%n SET Table_priv = TRIM(BOTH ',' FROM REPLACE(CONCAT(',', Table_priv, ','),',", mysql.Priv2SetStr[priv], ",',',')) WHERE User=%? AND Host=%? AND DB=%? AND Table_name=%?"}, "")
			sqlescape.MustFormatSQL(sql, s,
				mysql.SystemDB, mysql.TablePrivTable, user, host, db, table)
			_, err = sctx.GetSQLExecutor().ExecuteInternal(ctx, sql.String())
			if err != nil {
				return
			}
		}

		sql.Reset()
		s := strings.Join([]string{"UPDATE %n.%n SET Column_priv = TRIM(BOTH ',' FROM REPLACE(CONCAT(',', Column_priv, ','),',", mysql.Priv2SetStr[priv], ",',',')) WHERE User=%? AND Host=%? AND DB=%? AND Table_name=%?"}, "")
		sqlescape.MustFormatSQL(sql, s,
			mysql.SystemDB, mysql.TablePrivTable, user, host, db, table)
		_, err = sctx.GetSQLExecutor().ExecuteInternal(ctx, sql.String())
		if err != nil {
			return
		}
		columnPrivUpdated = sctx.GetSessionVars().StmtCtx.AffectedRows() > 0
	}

	sql.Reset()
	sqlescape.MustFormatSQL(sql, "DELETE FROM %n.%n WHERE User=%? AND Host=%? AND DB=%? AND Table_name=%? AND Table_priv='' AND Column_priv=''",
		mysql.SystemDB, mysql.TablePrivTable, user, host, db, table)
	_, err = sctx.GetSQLExecutor().ExecuteInternal(ctx, sql.String())
	return
}

// remove priv from Column_priv in mysql.columns_priv
func removePrivFromColumnPriv(ctx context.Context, e sqlexec.SQLExecutor,
	priv mysql.PrivilegeType, host, user, db, table, col string) (err error) {
	sql := new(strings.Builder)
	if priv == mysql.AllPriv {
		sqlescape.MustFormatSQL(sql, "UPDATE %n.%n SET Column_priv = '' WHERE User=%? AND Host=%? AND DB=%? AND Table_name=%?",
			mysql.SystemDB, mysql.ColumnPrivTable, user, host, db, table)
	} else {
		s := strings.Join([]string{"UPDATE %n.%n SET Column_priv = TRIM(BOTH ',' FROM REPLACE(CONCAT(',', Column_priv, ','),',", mysql.Priv2SetStr[priv], ",',',')) WHERE User=%? AND Host=%? AND DB=%? AND Table_name=%?"}, "")
		sqlescape.MustFormatSQL(sql, s,
			mysql.SystemDB, mysql.ColumnPrivTable, user, host, db, table)
	}
	if len(col) > 0 {
		sqlescape.MustFormatSQL(sql, " AND Column_name=%?", col)
	}
	_, err = e.ExecuteInternal(ctx, sql.String())
	if err != nil {
		return err
	}
	sql.Reset()
	sqlescape.MustFormatSQL(sql, "DELETE FROM %n.%n WHERE User=%? AND Host=%? AND DB=%? AND Table_name=%? AND Column_priv=''",
		mysql.SystemDB, mysql.ColumnPrivTable, user, host, db, table)
	_, err = e.ExecuteInternal(ctx, sql.String())
	return err
}

// checkTablePrivTbl indicates whether we should check Column_priv in mysql.tables_priv if no corresponding column privileges
// exists in mysql.columns_priv anymore.
func (e *RevokeExec) revokeColumnPriv(ctx context.Context, internalSession sessionctx.Context, priv *ast.PrivElem, user, host string, checkTablePrivTbl bool) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnPrivilege)
	dbName, tbl, err := getTargetSchemaAndTable(ctx, e.Ctx(), e.Level.DBName, e.Level.TableName, e.is)
	if err != nil && !terror.ErrorEqual(err, infoschema.ErrTableNotExists) {
		return err
	}
	cols := priv.Cols
	if len(cols) == 0 && tbl != nil {
		// It is used by e.revokeTablePriv(), to revoke all columns after revoke the corresponding table privilege.
		for _, col := range tbl.VisibleCols() {
			cols = append(cols, &ast.ColumnName{Name: col.Name})
		}
	}
	if len(cols) > 0 {
		for _, c := range cols {
			if tbl != nil {
				if table.FindCol(tbl.Cols(), c.Name.L) == nil {
					return infoschema.ErrColumnNotExists.GenWithStackByArgs(c.Name.L, tbl.Meta().Name.L)
				}
			}

			err = removePrivFromColumnPriv(ctx, internalSession.GetSQLExecutor(), priv.Priv, host, user, dbName, e.Level.TableName, c.Name.O)
			if err != nil {
				return err
			}

			//TODO Optimized for batch, one-shot.
		}
	} else {
		// FIXME(cbc): should not reach here?
		err = removePrivFromColumnPriv(ctx, internalSession.GetSQLExecutor(), priv.Priv, host, user, dbName, e.Level.TableName, "")
		if err != nil {
			return err
		}
	}

	if checkTablePrivTbl {
		sql := strings.Join([]string{"SELECT * FROM %n.%n WHERE User=%? AND Host=%? AND DB=%? AND Table_name=%? AND Column_priv LIKE '%%", priv.Priv.String(), "%%';"}, "")
		exists, err := recordExists(internalSession, sql, mysql.SystemDB, mysql.ColumnPrivTable, user, host, e.Level.DBName, e.Level.TableName)
		if err != nil {
			return err
		}
		if !exists {
			// We should delete the row in mysql.tables_priv if there is no column privileges left.
			p := &ast.PrivElem{Priv: priv.Priv}
			if err = e.revokeTablePriv(ctx, internalSession, p, user, host, false); err != nil {
				return err
			}
		}
	}
	return nil
}
