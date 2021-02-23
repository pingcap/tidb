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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"golang.org/x/net/context"
)

/***
 * Grant Statement
 * See https://dev.mysql.com/doc/refman/5.7/en/grant.html
 ************************************************************************************/
var (
	_ Executor = (*GrantExec)(nil)
)

// GrantExec executes GrantStmt.
type GrantExec struct {
	baseExecutor

	Privs      []*ast.PrivElem
	ObjectType ast.ObjectTypeType
	Level      *ast.GrantLevel
	Users      []*ast.UserSpec
	WithGrant  bool

	is   infoschema.InfoSchema
	done bool
}

// Next implements the Executor Next interface.
func (e *GrantExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	if e.done {
		return nil
	}
	e.done = true

	dbName := e.Level.DBName
	if len(dbName) == 0 {
		dbName = e.ctx.GetSessionVars().CurrentDB
	}
	// Grant for each user
	for idx, user := range e.Users {
		// Check if user exists.
		exists, err := userExists(e.ctx, user.User.Username, user.User.Hostname)
		if err != nil {
			return errors.Trace(err)
		}
		if !exists {
			pwd, ok := user.EncodedPassword()
			if !ok {
				return errors.Trace(ErrPasswordFormat)
			}
			sql := sqlexec.MustEscapeSQL(`INSERT INTO %n.%n (Host, User, Password) VALUES (%?,%?,%?);`, mysql.SystemDB, mysql.UserTable, user.User.Hostname, user.User.Username, pwd)
			_, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
			if err != nil {
				return errors.Trace(err)
			}
		}

		// If there is no privilege entry in corresponding table, insert a new one.
		// DB scope:		mysql.DB
		// Table scope:		mysql.Tables_priv
		// Column scope:	mysql.Columns_priv
		switch e.Level.Level {
		case ast.GrantLevelDB:
			err := checkAndInitDBPriv(e.ctx, dbName, e.is, user.User.Username, user.User.Hostname)
			if err != nil {
				return errors.Trace(err)
			}
		case ast.GrantLevelTable:
			err := checkAndInitTablePriv(e.ctx, dbName, e.Level.TableName, e.is, user.User.Username, user.User.Hostname)
			if err != nil {
				return errors.Trace(err)
			}
		}
		privs := e.Privs
		if e.WithGrant {
			privs = append(privs, &ast.PrivElem{Priv: mysql.GrantPriv})
		}

		if idx == 0 {
			// Commit the old transaction, like DDL.
			if err := e.ctx.NewTxn(); err != nil {
				return err
			}
			defer func() { e.ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, false) }()
		}

		// Grant each priv to the user.
		for _, priv := range privs {
			if len(priv.Cols) > 0 {
				// Check column scope privilege entry.
				// TODO: Check validity before insert new entry.
				err := e.checkAndInitColumnPriv(user.User.Username, user.User.Hostname, priv.Cols)
				if err != nil {
					return errors.Trace(err)
				}
			}
			err := e.grantPriv(priv, user)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	domain.GetDomain(e.ctx).NotifyUpdatePrivilege(e.ctx)
	return nil
}

// checkAndInitDBPriv checks if DB scope privilege entry exists in mysql.DB.
// If unexists, insert a new one.
func checkAndInitDBPriv(ctx sessionctx.Context, dbName string, is infoschema.InfoSchema, user string, host string) error {
	ok, err := dbUserExists(ctx, user, host, dbName)
	if err != nil {
		return errors.Trace(err)
	}
	if ok {
		return nil
	}
	// Entry does not exist for user-host-db. Insert a new entry.
	return initDBPrivEntry(ctx, user, host, dbName)
}

// checkAndInitTablePriv checks if table scope privilege entry exists in mysql.Tables_priv.
// If unexists, insert a new one.
func checkAndInitTablePriv(ctx sessionctx.Context, dbName, tblName string, is infoschema.InfoSchema, user string, host string) error {
	ok, err := tableUserExists(ctx, user, host, dbName, tblName)
	if err != nil {
		return errors.Trace(err)
	}
	if ok {
		return nil
	}
	// Entry does not exist for user-host-db-tbl. Insert a new entry.
	return initTablePrivEntry(ctx, user, host, dbName, tblName)
}

// checkAndInitColumnPriv checks if column scope privilege entry exists in mysql.Columns_priv.
// If unexists, insert a new one.
func (e *GrantExec) checkAndInitColumnPriv(user string, host string, cols []*ast.ColumnName) error {
	dbName, tbl, err := getTargetSchemaAndTable(e.ctx, e.Level.DBName, e.Level.TableName, e.is)
	if err != nil {
		return errors.Trace(err)
	}
	for _, c := range cols {
		col := table.FindCol(tbl.Cols(), c.Name.L)
		if col == nil {
			return errors.Errorf("Unknown column: %s", c.Name.O)
		}
		ok, err := columnPrivEntryExists(e.ctx, user, host, dbName, tbl.Meta().Name.O, col.Name.O)
		if err != nil {
			return errors.Trace(err)
		}
		if ok {
			continue
		}
		// Entry does not exist for user-host-db-tbl-col. Insert a new entry.
		err = initColumnPrivEntry(e.ctx, user, host, dbName, tbl.Meta().Name.O, col.Name.O)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// initDBPrivEntry inserts a new row into mysql.DB with empty privilege.
func initDBPrivEntry(ctx sessionctx.Context, user string, host string, db string) error {
	sql := sqlexec.MustEscapeSQL(`INSERT INTO %n.%n (Host, User, DB) VALUES (%?, %?, %?)`, mysql.SystemDB, mysql.DBTable, host, user, db)
	_, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	return errors.Trace(err)
}

// initTablePrivEntry inserts a new row into mysql.Tables_priv with empty privilege.
func initTablePrivEntry(ctx sessionctx.Context, user string, host string, db string, tbl string) error {
	sql := sqlexec.MustEscapeSQL(`INSERT INTO %n.%n (Host, User, DB, Table_name, Table_priv, Column_priv) VALUES (%?, %?, %?, %?, '', '')`, mysql.SystemDB, mysql.TablePrivTable, host, user, db, tbl)
	_, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	return errors.Trace(err)
}

// initColumnPrivEntry inserts a new row into mysql.Columns_priv with empty privilege.
func initColumnPrivEntry(ctx sessionctx.Context, user string, host string, db string, tbl string, col string) error {
	sql := sqlexec.MustEscapeSQL(`INSERT INTO %n.%n (Host, User, DB, Table_name, Column_name, Column_priv) VALUES (%?, %?, %?, %?, %?, '')`, mysql.SystemDB, mysql.ColumnPrivTable, host, user, db, tbl, col)
	_, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	return errors.Trace(err)
}

// grantPriv grants priv to user in s.Level scope.
func (e *GrantExec) grantPriv(priv *ast.PrivElem, user *ast.UserSpec) error {
	switch e.Level.Level {
	case ast.GrantLevelGlobal:
		return e.grantGlobalPriv(priv, user)
	case ast.GrantLevelDB:
		return e.grantDBPriv(priv, user)
	case ast.GrantLevelTable:
		if len(priv.Cols) == 0 {
			return e.grantTablePriv(priv, user)
		}
		return e.grantColumnPriv(priv, user)
	default:
		return errors.Errorf("Unknown grant level: %#v", e.Level)
	}
}

// grantGlobalPriv manipulates mysql.user table.
func (e *GrantExec) grantGlobalPriv(priv *ast.PrivElem, user *ast.UserSpec) error {
	if priv.Priv == 0 {
		return nil
	}
	sql := new(strings.Builder)
	sqlexec.MustFormatSQL(sql, `UPDATE %n.%n SET `, mysql.SystemDB, mysql.UserTable)
	err := composeGlobalPrivUpdate(sql, priv.Priv, "Y")
	if err != nil {
		return errors.Trace(err)
	}
	sqlexec.MustFormatSQL(sql, ` WHERE User=%? AND Host=%?`, user.User.Username, user.User.Hostname)
	_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql.String())
	return errors.Trace(err)
}

// grantDBPriv manipulates mysql.db table.
func (e *GrantExec) grantDBPriv(priv *ast.PrivElem, user *ast.UserSpec) error {
	dbName := e.Level.DBName
	if len(dbName) == 0 {
		dbName = e.ctx.GetSessionVars().CurrentDB
	}
	sql := new(strings.Builder)
	sqlexec.MustFormatSQL(sql, `UPDATE %n.%n SET `, mysql.SystemDB, mysql.DBTable)
	err := composeDBPrivUpdate(sql, priv.Priv, "Y")
	if err != nil {
		return errors.Trace(err)
	}
	sqlexec.MustFormatSQL(sql, ` WHERE User=%? AND Host=%? AND DB=%?;`, user.User.Username, user.User.Hostname, dbName)
	_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql.String())
	return errors.Trace(err)
}

// grantTablePriv manipulates mysql.tables_priv table.
func (e *GrantExec) grantTablePriv(priv *ast.PrivElem, user *ast.UserSpec) error {
	dbName := e.Level.DBName
	if len(dbName) == 0 {
		dbName = e.ctx.GetSessionVars().CurrentDB
	}
	tblName := e.Level.TableName
	sql := new(strings.Builder)
	sqlexec.MustFormatSQL(sql, `UPDATE %n.%n SET `, mysql.SystemDB, mysql.TablePrivTable)
	err := composeTablePrivUpdateForGrant(e.ctx, sql, priv.Priv, user.User.Username, user.User.Hostname, dbName, tblName)
	if err != nil {
		return errors.Trace(err)
	}
	sqlexec.MustFormatSQL(sql, ` WHERE User=%? AND Host=%? AND DB=%? AND Table_name=%?;`, user.User.Username, user.User.Hostname, dbName, tblName)
	_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql.String())
	return err
}

// grantColumnPriv manipulates mysql.tables_priv table.
func (e *GrantExec) grantColumnPriv(priv *ast.PrivElem, user *ast.UserSpec) error {
	dbName, tbl, err := getTargetSchemaAndTable(e.ctx, e.Level.DBName, e.Level.TableName, e.is)
	if err != nil {
		return errors.Trace(err)
	}

	sql := new(strings.Builder)
	for _, c := range priv.Cols {
		col := table.FindCol(tbl.Cols(), c.Name.L)
		if col == nil {
			return errors.Errorf("Unknown column: %s", c)
		}
		sql.Reset()
		sqlexec.MustFormatSQL(sql, "UPDATE %n.%n SET ", mysql.SystemDB, mysql.ColumnPrivTable)
		err := composeColumnPrivUpdateForGrant(e.ctx, sql, priv.Priv, user.User.Username, user.User.Hostname, dbName, tbl.Meta().Name.O, col.Name.O)
		if err != nil {
			return errors.Trace(err)
		}
		sqlexec.MustFormatSQL(sql, ` WHERE User=%? AND Host=%? AND DB=%? AND Table_name=%? AND Column_name=%?;`, user.User.Username, user.User.Hostname, dbName, tbl.Meta().Name.O, col.Name.O)
		_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql.String())
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// composeGlobalPrivUpdate composes update stmt assignment list string for global scope privilege update.
func composeGlobalPrivUpdate(sql *strings.Builder, priv mysql.PrivilegeType, value string) error {
	if priv != mysql.AllPriv {
		col, ok := mysql.Priv2UserCol[priv]
		if !ok {
			return errors.Errorf("Unknown priv: %v", priv)
		}
		sqlexec.MustFormatSQL(sql, "%n=%?", col, value)
		return nil
	}

	for i, v := range mysql.AllGlobalPrivs {
		if i > 0 {
			sqlexec.MustFormatSQL(sql, ",")
		}

		k, ok := mysql.Priv2UserCol[v]
		if !ok {
			return errors.Errorf("Unknown priv %v", priv)
		}

		sqlexec.MustFormatSQL(sql, "%n=%?", k, value)
	}
	return nil
}

// composeDBPrivUpdate composes update stmt assignment list for db scope privilege update.
func composeDBPrivUpdate(sql *strings.Builder, priv mysql.PrivilegeType, value string) error {
	if priv != mysql.AllPriv {
		col, ok := mysql.Priv2UserCol[priv]
		if !ok {
			return errors.Errorf("Unknown priv: %v", priv)
		}
		sqlexec.MustFormatSQL(sql, "%n=%?", col, value)
		return nil
	}

	for i, p := range mysql.AllDBPrivs {
		if i > 0 {
			sqlexec.MustFormatSQL(sql, ",")
		}

		v, ok := mysql.Priv2UserCol[p]
		if !ok {
			return errors.Errorf("Unknown priv %v", priv)
		}

		sqlexec.MustFormatSQL(sql, "%n=%?", v, value)
	}
	return nil
}

func privUpdateForGrant(cur []string, priv mysql.PrivilegeType) ([]string, error) {
	p, ok := mysql.Priv2SetStr[priv]
	if !ok {
		return nil, errors.Errorf("Unknown priv: %v", priv)
	}
	cur = addToSet(cur, p)
	return cur, nil
}

// composeTablePrivUpdateForGrant composes update stmt assignment list for table scope privilege update.
func composeTablePrivUpdateForGrant(ctx sessionctx.Context, sql *strings.Builder, priv mysql.PrivilegeType, name string, host string, db string, tbl string) error {
	var newTablePriv, newColumnPriv []string
	var tblPrivs, colPrivs []mysql.PrivilegeType
	if priv != mysql.AllPriv {
		currTablePriv, currColumnPriv, err := getTablePriv(ctx, name, host, db, tbl)
		if err != nil {
			return err
		}
		newTablePriv = setFromString(currTablePriv)
		newColumnPriv = setFromString(currColumnPriv)
		tblPrivs = []mysql.PrivilegeType{priv}
		for _, cp := range mysql.AllColumnPrivs {
			// in case it is not a column priv
			if cp == priv {
				colPrivs = []mysql.PrivilegeType{priv}
				break
			}
		}
	} else {
		tblPrivs = mysql.AllTablePrivs
		colPrivs = mysql.AllColumnPrivs
	}

	var err error
	for _, p := range tblPrivs {
		newTablePriv, err = privUpdateForGrant(newTablePriv, p)
		if err != nil {
			return err
		}
	}

	for _, p := range colPrivs {
		newColumnPriv, err = privUpdateForGrant(newColumnPriv, p)
		if err != nil {
			return err
		}
	}

	sqlexec.MustFormatSQL(sql, `Table_priv=%?, Column_priv=%?, Grantor=%?`, strings.Join(newTablePriv, ","), strings.Join(newColumnPriv, ","), ctx.GetSessionVars().User.String())
	return nil
}

// composeColumnPrivUpdateForGrant composes update stmt assignment list for column scope privilege update.
func composeColumnPrivUpdateForGrant(ctx sessionctx.Context, sql *strings.Builder, priv mysql.PrivilegeType, name string, host string, db string, tbl string, col string) error {
	var newColumnPriv []string
	var colPrivs []mysql.PrivilegeType
	if priv != mysql.AllPriv {
		currColumnPriv, err := getColumnPriv(ctx, name, host, db, tbl, col)
		if err != nil {
			return err
		}
		newColumnPriv = setFromString(currColumnPriv)
		colPrivs = []mysql.PrivilegeType{priv}
	} else {
		colPrivs = mysql.AllColumnPrivs
	}

	var err error
	for _, p := range colPrivs {
		newColumnPriv, err = privUpdateForGrant(newColumnPriv, p)
		if err != nil {
			return err
		}
	}

	sqlexec.MustFormatSQL(sql, `Column_priv=%?`, strings.Join(newColumnPriv, ","))
	return nil
}

// recordExists is a helper function to check if the sql returns any row.
func recordExists(ctx sessionctx.Context, sql string) (bool, error) {
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		return false, errors.Trace(err)
	}
	return len(rows) > 0, nil
}

// dbUserExists checks if there is an entry with key user-host-db in mysql.DB.
func dbUserExists(ctx sessionctx.Context, name string, host string, db string) (bool, error) {
	sql := sqlexec.MustEscapeSQL(`SELECT * FROM %n.%n WHERE User=%? AND Host=%? AND DB=%?;`, mysql.SystemDB, mysql.DBTable, name, host, db)
	return recordExists(ctx, sql)
}

// tableUserExists checks if there is an entry with key user-host-db-tbl in mysql.Tables_priv.
func tableUserExists(ctx sessionctx.Context, name string, host string, db string, tbl string) (bool, error) {
	sql := sqlexec.MustEscapeSQL(`SELECT * FROM %n.%n WHERE User=%? AND Host=%? AND DB=%? AND Table_name=%?;`, mysql.SystemDB, mysql.TablePrivTable, name, host, db, tbl)
	return recordExists(ctx, sql)
}

// columnPrivEntryExists checks if there is an entry with key user-host-db-tbl-col in mysql.Columns_priv.
func columnPrivEntryExists(ctx sessionctx.Context, name string, host string, db string, tbl string, col string) (bool, error) {
	sql := sqlexec.MustEscapeSQL(`SELECT * FROM %n.%n WHERE User=%? AND Host=%? AND DB=%? AND Table_name=%? AND Column_name=%?;`, mysql.SystemDB, mysql.ColumnPrivTable, name, host, db, tbl, col)
	return recordExists(ctx, sql)
}

// getTablePriv gets current table scope privilege set from mysql.Tables_priv.
// Return Table_priv and Column_priv.
func getTablePriv(ctx sessionctx.Context, name string, host string, db string, tbl string) (string, string, error) {
	sql := sqlexec.MustEscapeSQL(`SELECT Table_priv, Column_priv FROM %n.%n WHERE User=%? AND Host=%? AND DB=%? AND Table_name=%?;`, mysql.SystemDB, mysql.TablePrivTable, name, host, db, tbl)
	rows, fields, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		return "", "", errors.Trace(err)
	}
	if len(rows) < 1 {
		return "", "", errors.Errorf("get table privilege fail for %s %s %s %s", name, host, db, tbl)
	}
	var tPriv, cPriv string
	row := rows[0]
	if fields[0].Column.Tp == mysql.TypeSet {
		tablePriv := row.GetSet(0)
		tPriv = tablePriv.Name
	}
	if fields[1].Column.Tp == mysql.TypeSet {
		columnPriv := row.GetSet(1)
		cPriv = columnPriv.Name
	}
	return tPriv, cPriv, nil
}

// getColumnPriv gets current column scope privilege set from mysql.Columns_priv.
// Return Column_priv.
func getColumnPriv(ctx sessionctx.Context, name string, host string, db string, tbl string, col string) (string, error) {
	sql := sqlexec.MustEscapeSQL(`SELECT Column_priv FROM %n.%n WHERE User=%? AND Host=%? AND DB=%? AND Table_name=%? AND Column_name=%?;`, mysql.SystemDB, mysql.ColumnPrivTable, name, host, db, tbl, col)
	rows, fields, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		return "", errors.Trace(err)
	}
	if len(rows) < 1 {
		return "", errors.Errorf("get column privilege fail for %s %s %s %s %s", name, host, db, tbl, col)
	}
	cPriv := ""
	if fields[0].Column.Tp == mysql.TypeSet {
		setVal := rows[0].GetSet(0)
		cPriv = setVal.Name
	}
	return cPriv, nil
}

// getTargetSchemaAndTable finds the schema and table by dbName and tableName.
func getTargetSchemaAndTable(ctx sessionctx.Context, dbName, tableName string, is infoschema.InfoSchema) (string, table.Table, error) {
	if len(dbName) == 0 {
		dbName = ctx.GetSessionVars().CurrentDB
		if len(dbName) == 0 {
			return "", nil, errors.New("miss DB name for grant privilege")
		}
	}
	name := model.NewCIStr(tableName)
	tbl, err := is.TableByName(model.NewCIStr(dbName), name)
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	return dbName, tbl, nil
}
