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
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/auth"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	goctx "golang.org/x/net/context"
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

// Next implements Execution Next interface.
func (e *GrantExec) Next(goCtx goctx.Context) (Row, error) {
	if e.done {
		return nil, nil
	}
	e.done = true
	return nil, errors.Trace(e.run(goCtx))
}

// NextChunk implements the Executor NextChunk interface.
func (e *GrantExec) NextChunk(goCtx goctx.Context, chk *chunk.Chunk) error {
	if e.done {
		return nil
	}
	e.done = true
	return errors.Trace(e.run(goCtx))
}

func (e *GrantExec) run(goCtx goctx.Context) error {
	dbName := e.Level.DBName
	if len(dbName) == 0 {
		dbName = e.ctx.GetSessionVars().CurrentDB
	}
	// Grant for each user
	for _, user := range e.Users {
		// Check if user exists.
		exists, err := userExists(e.ctx, user.User.Username, user.User.Hostname)
		if err != nil {
			return errors.Trace(err)
		}
		if !exists {
			pwd := ""
			if user.AuthOpt != nil {
				if user.AuthOpt.ByAuthString {
					pwd = auth.EncodePassword(user.AuthOpt.AuthString)
				} else {
					if len(user.AuthOpt.HashString) == 41 && strings.HasPrefix(user.AuthOpt.HashString, "*") {
						pwd = user.AuthOpt.HashString
					} else {
						return errors.Trace(ErrPasswordFormat)
					}
				}
			}

			user := fmt.Sprintf(`("%s", "%s", "%s")`, user.User.Hostname, user.User.Username, pwd)
			sql := fmt.Sprintf(`INSERT INTO %s.%s (Host, User, Password) VALUES %s;`, mysql.SystemDB, mysql.UserTable, user)
			_, err := e.ctx.(sqlexec.SQLExecutor).Execute(goctx.TODO(), sql)
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
func checkAndInitDBPriv(ctx context.Context, dbName string, is infoschema.InfoSchema, user string, host string) error {
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
func checkAndInitTablePriv(ctx context.Context, dbName, tblName string, is infoschema.InfoSchema, user string, host string) error {
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
func initDBPrivEntry(ctx context.Context, user string, host string, db string) error {
	sql := fmt.Sprintf(`INSERT INTO %s.%s (Host, User, DB) VALUES ("%s", "%s", "%s")`, mysql.SystemDB, mysql.DBTable, host, user, db)
	_, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	return errors.Trace(err)
}

// initTablePrivEntry inserts a new row into mysql.Tables_priv with empty privilege.
func initTablePrivEntry(ctx context.Context, user string, host string, db string, tbl string) error {
	sql := fmt.Sprintf(`INSERT INTO %s.%s (Host, User, DB, Table_name, Table_priv, Column_priv) VALUES ("%s", "%s", "%s", "%s", "", "")`, mysql.SystemDB, mysql.TablePrivTable, host, user, db, tbl)
	_, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	return errors.Trace(err)
}

// initColumnPrivEntry inserts a new row into mysql.Columns_priv with empty privilege.
func initColumnPrivEntry(ctx context.Context, user string, host string, db string, tbl string, col string) error {
	sql := fmt.Sprintf(`INSERT INTO %s.%s (Host, User, DB, Table_name, Column_name, Column_priv) VALUES ("%s", "%s", "%s", "%s", "%s", "")`, mysql.SystemDB, mysql.ColumnPrivTable, host, user, db, tbl, col)
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
	asgns, err := composeGlobalPrivUpdate(priv.Priv, "Y")
	if err != nil {
		return errors.Trace(err)
	}
	sql := fmt.Sprintf(`UPDATE %s.%s SET %s WHERE User="%s" AND Host="%s"`, mysql.SystemDB, mysql.UserTable, asgns, user.User.Username, user.User.Hostname)
	_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
	return errors.Trace(err)
}

// grantDBPriv manipulates mysql.db table.
func (e *GrantExec) grantDBPriv(priv *ast.PrivElem, user *ast.UserSpec) error {
	dbName := e.Level.DBName
	if len(dbName) == 0 {
		dbName = e.ctx.GetSessionVars().CurrentDB
	}
	asgns, err := composeDBPrivUpdate(priv.Priv, "Y")
	if err != nil {
		return errors.Trace(err)
	}
	sql := fmt.Sprintf(`UPDATE %s.%s SET %s WHERE User="%s" AND Host="%s" AND DB="%s";`, mysql.SystemDB, mysql.DBTable, asgns, user.User.Username, user.User.Hostname, dbName)
	_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
	return errors.Trace(err)
}

// grantTablePriv manipulates mysql.tables_priv table.
func (e *GrantExec) grantTablePriv(priv *ast.PrivElem, user *ast.UserSpec) error {
	dbName := e.Level.DBName
	if len(dbName) == 0 {
		dbName = e.ctx.GetSessionVars().CurrentDB
	}
	tblName := e.Level.TableName
	asgns, err := composeTablePrivUpdateForGrant(e.ctx, priv.Priv, user.User.Username, user.User.Hostname, dbName, tblName)
	if err != nil {
		return errors.Trace(err)
	}
	sql := fmt.Sprintf(`UPDATE %s.%s SET %s WHERE User="%s" AND Host="%s" AND DB="%s" AND Table_name="%s";`, mysql.SystemDB, mysql.TablePrivTable, asgns, user.User.Username, user.User.Hostname, dbName, tblName)
	_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
	return errors.Trace(err)
}

// grantColumnPriv manipulates mysql.tables_priv table.
func (e *GrantExec) grantColumnPriv(priv *ast.PrivElem, user *ast.UserSpec) error {
	dbName, tbl, err := getTargetSchemaAndTable(e.ctx, e.Level.DBName, e.Level.TableName, e.is)
	if err != nil {
		return errors.Trace(err)
	}

	for _, c := range priv.Cols {
		col := table.FindCol(tbl.Cols(), c.Name.L)
		if col == nil {
			return errors.Errorf("Unknown column: %s", c)
		}
		asgns, err := composeColumnPrivUpdateForGrant(e.ctx, priv.Priv, user.User.Username, user.User.Hostname, dbName, tbl.Meta().Name.O, col.Name.O)
		if err != nil {
			return errors.Trace(err)
		}
		sql := fmt.Sprintf(`UPDATE %s.%s SET %s WHERE User="%s" AND Host="%s" AND DB="%s" AND Table_name="%s" AND Column_name="%s";`, mysql.SystemDB, mysql.ColumnPrivTable, asgns, user.User.Username, user.User.Hostname, dbName, tbl.Meta().Name.O, col.Name.O)
		_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// composeGlobalPrivUpdate composes update stmt assignment list string for global scope privilege update.
func composeGlobalPrivUpdate(priv mysql.PrivilegeType, value string) (string, error) {
	if priv == mysql.AllPriv {
		strs := make([]string, 0, len(mysql.Priv2UserCol))
		for _, v := range mysql.Priv2UserCol {
			strs = append(strs, fmt.Sprintf(`%s="%s"`, v, value))
		}
		return strings.Join(strs, ", "), nil
	}
	col, ok := mysql.Priv2UserCol[priv]
	if !ok {
		return "", errors.Errorf("Unknown priv: %v", priv)
	}
	return fmt.Sprintf(`%s="%s"`, col, value), nil
}

// composeDBPrivUpdate composes update stmt assignment list for db scope privilege update.
func composeDBPrivUpdate(priv mysql.PrivilegeType, value string) (string, error) {
	if priv == mysql.AllPriv {
		strs := make([]string, 0, len(mysql.AllDBPrivs))
		for _, p := range mysql.AllDBPrivs {
			v, ok := mysql.Priv2UserCol[p]
			if !ok {
				return "", errors.Errorf("Unknown db privilege %v", priv)
			}
			strs = append(strs, fmt.Sprintf(`%s="%s"`, v, value))
		}
		return strings.Join(strs, ", "), nil
	}
	col, ok := mysql.Priv2UserCol[priv]
	if !ok {
		return "", errors.Errorf("Unknown priv: %v", priv)
	}
	return fmt.Sprintf(`%s="%s"`, col, value), nil
}

// composeTablePrivUpdateForGrant composes update stmt assignment list for table scope privilege update.
func composeTablePrivUpdateForGrant(ctx context.Context, priv mysql.PrivilegeType, name string, host string, db string, tbl string) (string, error) {
	var newTablePriv, newColumnPriv string
	if priv == mysql.AllPriv {
		for _, p := range mysql.AllTablePrivs {
			v, ok := mysql.Priv2SetStr[p]
			if !ok {
				return "", errors.Errorf("Unknown table privilege %v", p)
			}
			newTablePriv = addToSet(newTablePriv, v)
		}
		for _, p := range mysql.AllColumnPrivs {
			v, ok := mysql.Priv2SetStr[p]
			if !ok {
				return "", errors.Errorf("Unknown column privilege %v", p)
			}
			newColumnPriv = addToSet(newColumnPriv, v)
		}
	} else {
		currTablePriv, currColumnPriv, err := getTablePriv(ctx, name, host, db, tbl)
		if err != nil {
			return "", errors.Trace(err)
		}
		p, ok := mysql.Priv2SetStr[priv]
		if !ok {
			return "", errors.Errorf("Unknown priv: %v", priv)
		}
		newTablePriv = addToSet(currTablePriv, p)

		for _, cp := range mysql.AllColumnPrivs {
			if priv == cp {
				newColumnPriv = addToSet(currColumnPriv, p)
				break
			}
		}
	}
	return fmt.Sprintf(`Table_priv="%s", Column_priv="%s", Grantor="%s"`, newTablePriv, newColumnPriv, ctx.GetSessionVars().User), nil
}

func composeTablePrivUpdateForRevoke(ctx context.Context, priv mysql.PrivilegeType, name string, host string, db string, tbl string) (string, error) {
	var newTablePriv, newColumnPriv string
	if priv == mysql.AllPriv {
		newTablePriv = ""
		newColumnPriv = ""
	} else {
		currTablePriv, currColumnPriv, err := getTablePriv(ctx, name, host, db, tbl)
		if err != nil {
			return "", errors.Trace(err)
		}
		p, ok := mysql.Priv2SetStr[priv]
		if !ok {
			return "", errors.Errorf("Unknown priv: %v", priv)
		}
		newTablePriv = deleteFromSet(currTablePriv, p)

		for _, cp := range mysql.AllColumnPrivs {
			if priv == cp {
				newColumnPriv = deleteFromSet(currColumnPriv, p)
				break
			}
		}
	}
	return fmt.Sprintf(`Table_priv="%s", Column_priv="%s", Grantor="%s"`, newTablePriv, newColumnPriv, ctx.GetSessionVars().User), nil
}

// addToSet add a value to the set, e.g:
// addToSet("Select,Insert", "Update") returns "Select,Insert,Update".
func addToSet(set string, value string) string {
	if set == "" {
		return value
	}
	return fmt.Sprintf("%s,%s", set, value)
}

// deleteFromSet delete the value from the set, e.g:
// deleteFromSet("Select,Insert,Update", "Update") returns "Select,Insert".
func deleteFromSet(set string, value string) string {
	sets := strings.Split(set, ",")
	res := make([]string, 0, len(sets))
	for _, v := range sets {
		if v != value {
			res = append(res, v)
		}
	}
	return strings.Join(res, ",")
}

// composeColumnPrivUpdateForGrant composes update stmt assignment list for column scope privilege update.
func composeColumnPrivUpdateForGrant(ctx context.Context, priv mysql.PrivilegeType, name string, host string, db string, tbl string, col string) (string, error) {
	newColumnPriv := ""
	if priv == mysql.AllPriv {
		for _, p := range mysql.AllColumnPrivs {
			v, ok := mysql.Priv2SetStr[p]
			if !ok {
				return "", errors.Errorf("Unknown column privilege %v", p)
			}
			newColumnPriv = addToSet(newColumnPriv, v)
		}
	} else {
		currColumnPriv, err := getColumnPriv(ctx, name, host, db, tbl, col)
		if err != nil {
			return "", errors.Trace(err)
		}
		p, ok := mysql.Priv2SetStr[priv]
		if !ok {
			return "", errors.Errorf("Unknown priv: %v", priv)
		}
		newColumnPriv = addToSet(currColumnPriv, p)
	}
	return fmt.Sprintf(`Column_priv="%s"`, newColumnPriv), nil
}

func composeColumnPrivUpdateForRevoke(ctx context.Context, priv mysql.PrivilegeType, name string, host string, db string, tbl string, col string) (string, error) {
	newColumnPriv := ""
	if priv == mysql.AllPriv {
		newColumnPriv = ""
	} else {
		currColumnPriv, err := getColumnPriv(ctx, name, host, db, tbl, col)
		if err != nil {
			return "", errors.Trace(err)
		}
		p, ok := mysql.Priv2SetStr[priv]
		if !ok {
			return "", errors.Errorf("Unknown priv: %v", priv)
		}
		newColumnPriv = deleteFromSet(currColumnPriv, p)
	}
	return fmt.Sprintf(`Column_priv="%s"`, newColumnPriv), nil
}

// recordExists is a helper function to check if the sql returns any row.
func recordExists(ctx context.Context, sql string) (bool, error) {
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		return false, errors.Trace(err)
	}
	return len(rows) > 0, nil
}

// dbUserExists checks if there is an entry with key user-host-db in mysql.DB.
func dbUserExists(ctx context.Context, name string, host string, db string) (bool, error) {
	sql := fmt.Sprintf(`SELECT * FROM %s.%s WHERE User="%s" AND Host="%s" AND DB="%s";`, mysql.SystemDB, mysql.DBTable, name, host, db)
	return recordExists(ctx, sql)
}

// tableUserExists checks if there is an entry with key user-host-db-tbl in mysql.Tables_priv.
func tableUserExists(ctx context.Context, name string, host string, db string, tbl string) (bool, error) {
	sql := fmt.Sprintf(`SELECT * FROM %s.%s WHERE User="%s" AND Host="%s" AND DB="%s" AND Table_name="%s";`, mysql.SystemDB, mysql.TablePrivTable, name, host, db, tbl)
	return recordExists(ctx, sql)
}

// columnPrivEntryExists checks if there is an entry with key user-host-db-tbl-col in mysql.Columns_priv.
func columnPrivEntryExists(ctx context.Context, name string, host string, db string, tbl string, col string) (bool, error) {
	sql := fmt.Sprintf(`SELECT * FROM %s.%s WHERE User="%s" AND Host="%s" AND DB="%s" AND Table_name="%s" AND Column_name="%s";`, mysql.SystemDB, mysql.ColumnPrivTable, name, host, db, tbl, col)
	return recordExists(ctx, sql)
}

// getTablePriv gets current table scope privilege set from mysql.Tables_priv.
// Return Table_priv and Column_priv.
func getTablePriv(ctx context.Context, name string, host string, db string, tbl string) (string, string, error) {
	sql := fmt.Sprintf(`SELECT Table_priv, Column_priv FROM %s.%s WHERE User="%s" AND Host="%s" AND DB="%s" AND Table_name="%s";`, mysql.SystemDB, mysql.TablePrivTable, name, host, db, tbl)
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
func getColumnPriv(ctx context.Context, name string, host string, db string, tbl string, col string) (string, error) {
	sql := fmt.Sprintf(`SELECT Column_priv FROM %s.%s WHERE User="%s" AND Host="%s" AND DB="%s" AND Table_name="%s" AND Column_name="%s";`, mysql.SystemDB, mysql.ColumnPrivTable, name, host, db, tbl, col)
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
func getTargetSchemaAndTable(ctx context.Context, dbName, tableName string, is infoschema.InfoSchema) (string, table.Table, error) {
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
