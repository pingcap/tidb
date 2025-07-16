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
	oldUsr := internalSession.GetSessionVars().User
	defer func() {
		if !isCommit {
			_, err := internalSession.GetSQLExecutor().ExecuteInternal(internalCtx, "rollback")
			if err != nil {
				logutil.BgLogger().Error("rollback error occur at grant privilege", zap.Error(err))
			}
		}
		internalSession.GetSessionVars().User = oldUsr
		e.ReleaseSysSession(internalCtx, internalSession)
	}()

	internalSession.GetSessionVars().User = e.ctx.GetSessionVars().User
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
	return domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege()
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
		switch e.ObjectType {
		case ast.ObjectTypeNone, ast.ObjectTypeTable:
			ok, err := tableUserExists(internalSession, user, host, dbName, e.Level.TableName)
			if err != nil {
				return err
			}
			if !ok {
				return errors.Errorf("There is no such grant defined for user '%s' on host '%s' on table %s.%s", user, host, dbName, e.Level.TableName)
			}
		case ast.ObjectTypeFunction, ast.ObjectTypeProcedure:
			var routineType string
			if e.ObjectType == ast.ObjectTypeFunction {
				routineType = "FOUCTION"
			} else if e.ObjectType == ast.ObjectTypeProcedure {
				routineType = "PROCEDURE"
			}
			ok, err := procedureUserExists(internalSession, user, host, dbName, e.Level.TableName, routineType)
			if err != nil {
				return err
			}
			if !ok {
				return errors.Errorf("There is no such grant defined for user '%s' on host '%s' on procedure %s.%s", user, host, dbName, e.Level.TableName)
			}
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

func prepareRevokeRoutinePriv(ctx context.Context, internalSession sessionctx.Context, priv *ast.PrivElem, user, host, dbName, routineName, routineType string, sql *strings.Builder) error {
	sqlescape.MustFormatSQL(sql, "UPDATE %n.%n SET ", mysql.SystemDB, mysql.ProcsPriv)
	isDelRow, err := composeProcPrivUpdateForRevoke(internalSession, sql, priv.Priv, user, host, dbName, routineName, routineType)
	if err != nil {
		return err
	}

	sqlescape.MustFormatSQL(sql, " WHERE User=%? AND Host=%? AND DB=%? AND Routine_name=%? AND Routine_type=%?", user, host, dbName, routineName, routineType)
	_, err = internalSession.(sqlexec.SQLExecutor).ExecuteInternal(ctx, sql.String())
	if err != nil {
		return err
	}

	if isDelRow {
		sql.Reset()
		sqlescape.MustFormatSQL(sql, "DELETE FROM %n.%n WHERE User=%? AND Host=%? AND DB=%? AND Routine_name=%? AND Routine_type=%?",
			mysql.SystemDB, mysql.ProcsPriv, user, host, dbName, routineName, routineType)
		_, err = internalSession.(sqlexec.SQLExecutor).ExecuteInternal(ctx, sql.String())
		return err
	}
	return nil
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
	sql := new(strings.Builder)
	switch e.ObjectType {
	case ast.ObjectTypeNone, ast.ObjectTypeTable:
		// 1. update mysql.tables_priv
		updateColumnPriv, err := removePrivFromTablePriv(ctx, internalSession, priv.Priv, host, user, dbName, tblName, checkColumnPrivTbl)
		if err != nil {
			return err
		}

		// 2. update mysql.columns_priv
		if checkColumnPrivTbl && updateColumnPriv {
			p := &ast.PrivElem{Priv: priv.Priv, Cols: []*ast.ColumnName{}}
			// remove corresponding Column_priv in mysql.columns_priv
			if err = e.revokeColumnPriv(ctx, internalSession, p, user, host, false); err != nil {
				return err
			}
		}
	case ast.ObjectTypeProcedure:
		err := prepareRevokeRoutinePriv(ctx, internalSession, priv, user, host, dbName, tblName, "PROCEDURE", sql)
		if err != nil {
			return err
		}
	case ast.ObjectTypeFunction:
		err := prepareRevokeRoutinePriv(ctx, internalSession, priv, user, host, dbName, tblName, "FUNCTION", sql)
		if err != nil {
			return err
		}
	default:
		return errors.Errorf("revoke unsupport ObjectType:%T", e.ObjectType)
	}
	return nil
}

// Remove priv from Table_priv and Column_priv in mysql.tables_priv
func removePrivFromTablePriv(ctx context.Context, sctx sessionctx.Context,
	priv mysql.PrivilegeType, host, user, db, table string, removeTablePriv bool) (updateColumnPriv bool, err error) {
	sql := new(strings.Builder)
	if priv == mysql.AllPriv {
		if removeTablePriv {
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
		updateColumnPriv = sctx.GetSessionVars().StmtCtx.AffectedRows() > 0
	} else {
		if removeTablePriv {
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
		updateColumnPriv = sctx.GetSessionVars().StmtCtx.AffectedRows() > 0
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

// checkTable indicates whether we should check Column_priv in mysql.tables_priv if no corresponding column privileges
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
					return errors.Errorf("Unknown column: %s", c.Name.L)
				}
			}

			err = removePrivFromColumnPriv(ctx, internalSession.GetSQLExecutor(), priv.Priv, host, user, dbName, e.Level.TableName, c.Name.O)
			if err != nil {
				return err
			}

			//TODO Optimized for batch, one-shot.
		}
	} else {
		err = removePrivFromColumnPriv(ctx, internalSession.GetSQLExecutor(), priv.Priv, host, user, dbName, e.Level.TableName, "")
		if err != nil {
			return err
		}
	}

	if checkTablePrivTbl {
		exists, err := otherColumnPrivEntryExists(internalSession, user, host, e.Level.DBName, e.Level.TableName, priv.Priv.String())
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

// remove `priv` from `cur`
func privUpdateForRevoke(cur []string, priv mysql.PrivilegeType) ([]string, error) {
	p, ok := mysql.Priv2SetStr[priv]
	if !ok {
		return nil, errors.Errorf("Unknown priv: %v", priv)
	}
	cur = deleteFromSet(cur, p)
	return cur, nil
}

func composeProcPrivUpdateForRevoke(ctx sessionctx.Context, sql *strings.Builder, priv mysql.PrivilegeType, name string, host string, db string, tbl, routineType string) (bool, error) {
	var newProcPriv []string

	currProcPriv, err := getRoutinePriv(ctx, name, host, db, tbl, routineType)
	if err != nil {
		return false, err
	}

	if priv == mysql.AllPriv {
		// Revoking `ALL` does not revoke the Grant option,
		// so we only need to check if the user had this previously.
		tmp := SetFromString(currProcPriv)
		for _, p := range tmp {
			if p == mysql.Priv2SetStr[mysql.GrantPriv] {
				newProcPriv = []string{mysql.Priv2SetStr[mysql.GrantPriv]}
			}
		}
	} else {
		newProcPriv = SetFromString(currProcPriv)
		newProcPriv, err = privUpdateForRevoke(newProcPriv, priv)
		if err != nil {
			return false, err
		}
	}
	sqlescape.MustFormatSQL(sql, `Proc_priv=%?, Grantor=%?`, strings.Join(newProcPriv, ","), ctx.GetSessionVars().User.String())
	return len(newProcPriv) == 0, nil
}

// otherColumnPrivEntryExists checks if there is another entry with key user-host-db-tbl and column_priv in mysql.Columns_priv.
func otherColumnPrivEntryExists(ctx sessionctx.Context, name string, host string, db string, tbl string, colPriv string) (bool, error) {
	sql := strings.Join([]string{"SELECT * FROM %n.%n WHERE User=%? AND Host=%? AND DB=%? AND Table_name=%? AND Column_priv LIKE '%%", colPriv, "%%';"}, "")
	return recordExists(ctx, sql, mysql.SystemDB, mysql.ColumnPrivTable, name, host, db, tbl)
}
