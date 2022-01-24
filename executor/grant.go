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
	"encoding/json"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
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
	TLSOptions []*ast.TLSOption

	is   infoschema.InfoSchema
	done bool
}

// Next implements the Executor Next interface.
func (e *GrantExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.done {
		return nil
	}
	e.done = true

	dbName := e.Level.DBName
	if len(dbName) == 0 {
		dbName = e.ctx.GetSessionVars().CurrentDB
	}

	// Make sure the table exist.
	if e.Level.Level == ast.GrantLevelTable {
		dbNameStr := model.NewCIStr(dbName)
		schema := infoschema.GetInfoSchema(e.ctx)
		tbl, err := schema.TableByName(dbNameStr, model.NewCIStr(e.Level.TableName))
		if err != nil {
			return err
		}
		err = infoschema.ErrTableNotExists.GenWithStackByArgs(dbName, e.Level.TableName)
		// Note the table name compare is case sensitive here.
		if tbl.Meta().Name.String() != e.Level.TableName {
			return err
		}
		if len(e.Level.DBName) > 0 {
			// The database name should also match.
			db, succ := schema.SchemaByName(dbNameStr)
			if !succ || db.Name.String() != dbName {
				return err
			}
		}
	}

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
				logutil.Logger(context.Background()).Error("rollback error occur at grant privilege", zap.Error(err))
			}
		}
		e.releaseSysSession(internalSession)
	}()

	_, err = internalSession.(sqlexec.SQLExecutor).Execute(context.Background(), "begin")
	if err != nil {
		return err
	}

	// Check which user is not exist.
	for _, user := range e.Users {
		exists, err := userExists(e.ctx, user.User.Username, user.User.Hostname)
		if err != nil {
			return err
		}
		if !exists && e.ctx.GetSessionVars().SQLMode.HasNoAutoCreateUserMode() {
			return ErrCantCreateUserWithGrant
		} else if !exists {
			pwd, ok := user.EncodedPassword()
			if !ok {
				return errors.Trace(ErrPasswordFormat)
			}
			sql := sqlexec.MustEscapeSQL(`INSERT INTO %n.%n (Host, User, Password) VALUES (%?,%?,%?);`, mysql.SystemDB, mysql.UserTable, user.User.Hostname, user.User.Username, pwd)
			_, err := internalSession.(sqlexec.SQLExecutor).Execute(ctx, sql)
			if err != nil {
				return err
			}
		}
	}

	// Grant for each user
	for _, user := range e.Users {
		// If there is no privilege entry in corresponding table, insert a new one.
		// Global scope:		mysql.global_priv
		// DB scope:			mysql.DB
		// Table scope:			mysql.Tables_priv
		// Column scope:		mysql.Columns_priv
		if e.TLSOptions != nil {
			err = checkAndInitGlobalPriv(internalSession, user.User.Username, user.User.Hostname)
			if err != nil {
				return err
			}
		}
		switch e.Level.Level {
		case ast.GrantLevelDB:
			err := checkAndInitDBPriv(internalSession, dbName, e.is, user.User.Username, user.User.Hostname)
			if err != nil {
				return err
			}
		case ast.GrantLevelTable:
			err := checkAndInitTablePriv(internalSession, dbName, e.Level.TableName, e.is, user.User.Username, user.User.Hostname)
			if err != nil {
				return err
			}
		}
		privs := e.Privs
		if e.WithGrant {
			privs = append(privs, &ast.PrivElem{Priv: mysql.GrantPriv})
		}

		// Grant global priv to user.
		err = e.grantGlobalPriv(internalSession, user)
		if err != nil {
			return err
		}
		// Grant each priv to the user.
		for _, priv := range privs {
			if len(priv.Cols) > 0 {
				// Check column scope privilege entry.
				// TODO: Check validity before insert new entry.
				err := e.checkAndInitColumnPriv(user.User.Username, user.User.Hostname, priv.Cols, internalSession)
				if err != nil {
					return err
				}
			}
			err := e.grantLevelPriv(priv, user, internalSession)
			if err != nil {
				return err
			}
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

// checkAndInitGlobalPriv checks if global scope privilege entry exists in mysql.global_priv.
// If not exists, insert a new one.
func checkAndInitGlobalPriv(ctx sessionctx.Context, user string, host string) error {
	ok, err := globalPrivEntryExists(ctx, user, host)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	// Entry does not exist for user-host-db. Insert a new entry.
	return initGlobalPrivEntry(ctx, user, host)
}

// checkAndInitDBPriv checks if DB scope privilege entry exists in mysql.DB.
// If unexists, insert a new one.
func checkAndInitDBPriv(ctx sessionctx.Context, dbName string, is infoschema.InfoSchema, user string, host string) error {
	ok, err := dbUserExists(ctx, user, host, dbName)
	if err != nil {
		return err
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
		return err
	}
	if ok {
		return nil
	}
	// Entry does not exist for user-host-db-tbl. Insert a new entry.
	return initTablePrivEntry(ctx, user, host, dbName, tblName)
}

// checkAndInitColumnPriv checks if column scope privilege entry exists in mysql.Columns_priv.
// If unexists, insert a new one.
func (e *GrantExec) checkAndInitColumnPriv(user string, host string, cols []*ast.ColumnName, internalSession sessionctx.Context) error {
	dbName, tbl, err := getTargetSchemaAndTable(e.ctx, e.Level.DBName, e.Level.TableName, e.is)
	if err != nil {
		return err
	}
	for _, c := range cols {
		col := table.FindCol(tbl.Cols(), c.Name.L)
		if col == nil {
			return errors.Errorf("Unknown column: %s", c.Name.O)
		}
		ok, err := columnPrivEntryExists(internalSession, user, host, dbName, tbl.Meta().Name.O, col.Name.O)
		if err != nil {
			return err
		}
		if ok {
			continue
		}
		// Entry does not exist for user-host-db-tbl-col. Insert a new entry.
		err = initColumnPrivEntry(internalSession, user, host, dbName, tbl.Meta().Name.O, col.Name.O)
		if err != nil {
			return err
		}
	}
	return nil
}

// initGlobalPrivEntry inserts a new row into mysql.DB with empty privilege.
func initGlobalPrivEntry(ctx sessionctx.Context, user string, host string) error {
	sql := sqlexec.MustEscapeSQL(`INSERT INTO %n.%n (Host, User, PRIV) VALUES (%?, %?, %?)`, mysql.SystemDB, mysql.GlobalPrivTable, host, user, "{}")
	_, err := ctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql)
	return err
}

// initDBPrivEntry inserts a new row into mysql.DB with empty privilege.
func initDBPrivEntry(ctx sessionctx.Context, user string, host string, db string) error {
	sql := sqlexec.MustEscapeSQL(`INSERT INTO %n.%n (Host, User, DB) VALUES (%?, %?, %?)`, mysql.SystemDB, mysql.DBTable, host, user, db)
	_, err := ctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql)
	return err
}

// initTablePrivEntry inserts a new row into mysql.Tables_priv with empty privilege.
func initTablePrivEntry(ctx sessionctx.Context, user string, host string, db string, tbl string) error {
	sql := sqlexec.MustEscapeSQL(`INSERT INTO %n.%n (Host, User, DB, Table_name, Table_priv, Column_priv) VALUES (%?, %?, %?, %?, '', '')`, mysql.SystemDB, mysql.TablePrivTable, host, user, db, tbl)
	_, err := ctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql)
	return err
}

// initColumnPrivEntry inserts a new row into mysql.Columns_priv with empty privilege.
func initColumnPrivEntry(ctx sessionctx.Context, user string, host string, db string, tbl string, col string) error {
	sql := sqlexec.MustEscapeSQL(`INSERT INTO %n.%n (Host, User, DB, Table_name, Column_name, Column_priv) VALUES (%?, %?, %?, %?, %?, '')`, mysql.SystemDB, mysql.ColumnPrivTable, host, user, db, tbl, col)
	_, err := ctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql)
	return err
}

// grantGlobalPriv grants priv to user in global scope.
func (e *GrantExec) grantGlobalPriv(ctx sessionctx.Context, user *ast.UserSpec) error {
	if len(e.TLSOptions) == 0 {
		return nil
	}
	priv, err := tlsOption2GlobalPriv(e.TLSOptions)
	if err != nil {
		return errors.Trace(err)
	}
	sql := sqlexec.MustEscapeSQL(`UPDATE %n.%n SET PRIV=%? WHERE User=%? AND Host=%?`, mysql.SystemDB, mysql.GlobalPrivTable, priv, user.User.Username, user.User.Hostname)
	_, err = ctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql)
	return err
}

var emptyGP = privileges.GlobalPrivValue{SSLType: privileges.SslTypeNotSpecified}

func tlsOption2GlobalPriv(tlsOptions []*ast.TLSOption) (priv []byte, err error) {
	if len(tlsOptions) == 0 {
		priv = []byte("{}")
		return
	}
	dupSet := make(map[int]struct{})
	for _, opt := range tlsOptions {
		if _, dup := dupSet[opt.Type]; dup {
			var typeName string
			switch opt.Type {
			case ast.Cipher:
				typeName = "CIPHER"
			case ast.Issuer:
				typeName = "ISSUER"
			case ast.Subject:
				typeName = "SUBJECT"
			}
			err = errors.Errorf("Duplicate require %s clause", typeName)
			return
		}
		dupSet[opt.Type] = struct{}{}
	}
	gp := privileges.GlobalPrivValue{SSLType: privileges.SslTypeNotSpecified}
	for _, tlsOpt := range tlsOptions {
		switch tlsOpt.Type {
		case ast.TslNone:
			gp.SSLType = privileges.SslTypeNone
		case ast.Ssl:
			gp.SSLType = privileges.SslTypeAny
		case ast.X509:
			gp.SSLType = privileges.SslTypeX509
		case ast.Cipher:
			gp.SSLType = privileges.SslTypeSpecified
			if len(tlsOpt.Value) > 0 {
				if _, ok := util.SupportCipher[tlsOpt.Value]; !ok {
					err = errors.Errorf("Unsupported cipher suit: %s", tlsOpt.Value)
					return
				}
				gp.SSLCipher = tlsOpt.Value
			}
		case ast.Issuer:
			err = util.CheckSupportX509NameOneline(tlsOpt.Value)
			if err != nil {
				return
			}
			gp.SSLType = privileges.SslTypeSpecified
			gp.X509Issuer = tlsOpt.Value
		case ast.Subject:
			err = util.CheckSupportX509NameOneline(tlsOpt.Value)
			if err != nil {
				return
			}
			gp.SSLType = privileges.SslTypeSpecified
			gp.X509Subject = tlsOpt.Value
		default:
			err = errors.Errorf("Unknown ssl type: %#v", tlsOpt.Type)
			return
		}
	}
	if gp == emptyGP {
		return
	}
	priv, err = json.Marshal(&gp)
	if err != nil {
		return
	}
	return
}

// grantLevelPriv grants priv to user in s.Level scope.
func (e *GrantExec) grantLevelPriv(priv *ast.PrivElem, user *ast.UserSpec, internalSession sessionctx.Context) error {
	switch e.Level.Level {
	case ast.GrantLevelGlobal:
		return e.grantGlobalLevel(priv, user, internalSession)
	case ast.GrantLevelDB:
		return e.grantDBLevel(priv, user, internalSession)
	case ast.GrantLevelTable:
		if len(priv.Cols) == 0 {
			return e.grantTableLevel(priv, user, internalSession)
		}
		return e.grantColumnLevel(priv, user, internalSession)
	default:
		return errors.Errorf("Unknown grant level: %#v", e.Level)
	}
}

// grantGlobalLevel manipulates mysql.user table.
func (e *GrantExec) grantGlobalLevel(priv *ast.PrivElem, user *ast.UserSpec, internalSession sessionctx.Context) error {
	if priv.Priv == 0 {
		return nil
	}
	sql := new(strings.Builder)
	sqlexec.MustFormatSQL(sql, `UPDATE %n.%n SET `, mysql.SystemDB, mysql.UserTable)
	err := composeGlobalPrivUpdate(sql, priv.Priv, "Y")
	if err != nil {
		return err
	}
	sqlexec.MustFormatSQL(sql, ` WHERE User=%? AND Host=%?`, user.User.Username, user.User.Hostname)
	_, err = internalSession.(sqlexec.SQLExecutor).Execute(context.Background(), sql.String())
	return err
}

// grantDBLevel manipulates mysql.db table.
func (e *GrantExec) grantDBLevel(priv *ast.PrivElem, user *ast.UserSpec, internalSession sessionctx.Context) error {
	dbName := e.Level.DBName
	if len(dbName) == 0 {
		dbName = e.ctx.GetSessionVars().CurrentDB
	}
	sql := new(strings.Builder)
	sqlexec.MustFormatSQL(sql, `UPDATE %n.%n SET `, mysql.SystemDB, mysql.DBTable)
	err := composeDBPrivUpdate(sql, priv.Priv, "Y")
	if err != nil {
		return err
	}
	sqlexec.MustFormatSQL(sql, ` WHERE User=%? AND Host=%? AND DB=%?;`, user.User.Username, user.User.Hostname, dbName)
	_, err = internalSession.(sqlexec.SQLExecutor).Execute(context.Background(), sql.String())
	return err
}

// grantTableLevel manipulates mysql.tables_priv table.
func (e *GrantExec) grantTableLevel(priv *ast.PrivElem, user *ast.UserSpec, internalSession sessionctx.Context) error {
	dbName := e.Level.DBName
	if len(dbName) == 0 {
		dbName = e.ctx.GetSessionVars().CurrentDB
	}
	tblName := e.Level.TableName
	sql := new(strings.Builder)
	sqlexec.MustFormatSQL(sql, `UPDATE %n.%n SET `, mysql.SystemDB, mysql.TablePrivTable)
	err := composeTablePrivUpdateForGrant(internalSession, sql, priv.Priv, user.User.Username, user.User.Hostname, dbName, tblName)
	if err != nil {
		return err
	}
	sqlexec.MustFormatSQL(sql, ` WHERE User=%? AND Host=%? AND DB=%? AND Table_name=%?;`, user.User.Username, user.User.Hostname, dbName, tblName)
	_, err = internalSession.(sqlexec.SQLExecutor).Execute(context.Background(), sql.String())
	return err
}

// grantColumnLevel manipulates mysql.tables_priv table.
func (e *GrantExec) grantColumnLevel(priv *ast.PrivElem, user *ast.UserSpec, internalSession sessionctx.Context) error {
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
		err := composeColumnPrivUpdateForGrant(internalSession, sql, priv.Priv, user.User.Username, user.User.Hostname, dbName, tbl.Meta().Name.O, col.Name.O)
		if err != nil {
			return err
		}
		sqlexec.MustFormatSQL(sql, ` WHERE User=%? AND Host=%? AND DB=%? AND Table_name=%? AND Column_name=%?;`, user.User.Username, user.User.Hostname, dbName, tbl.Meta().Name.O, col.Name.O)
		_, err = internalSession.(sqlexec.SQLExecutor).Execute(context.Background(), sql.String())
		if err != nil {
			return err
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
	recordSets, err := ctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql)
	if err != nil {
		return false, err
	}
	rows, _, err := getRowsAndFields(ctx, recordSets)
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}

// globalPrivEntryExists checks if there is an entry with key user-host in mysql.global_priv.
func globalPrivEntryExists(ctx sessionctx.Context, name string, host string) (bool, error) {
	sql := sqlexec.MustEscapeSQL(`SELECT * FROM %n.%n WHERE User=%? AND Host=%?;`, mysql.SystemDB, mysql.GlobalPrivTable, name, host)
	return recordExists(ctx, sql)
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
	rs, err := ctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql)
	if err != nil {
		return "", "", err
	}
	if len(rs) < 1 {
		return "", "", errors.Errorf("get table privilege fail for %s %s %s %s", name, host, db, tbl)
	}
	var tPriv, cPriv string
	rows, fields, err := getRowsAndFields(ctx, rs)
	if err != nil {
		return "", "", err
	}
	if len(rows) < 1 {
		return "", "", errors.Errorf("get table privilege fail for %s %s %s %s", name, host, db, tbl)
	}
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
	rs, err := ctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql)
	if err != nil {
		return "", err
	}
	if len(rs) < 1 {
		return "", errors.Errorf("get column privilege fail for %s %s %s %s", name, host, db, tbl)
	}
	rows, fields, err := getRowsAndFields(ctx, rs)
	if err != nil {
		return "", err
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
		return "", nil, err
	}
	return dbName, tbl, nil
}

// getRowsAndFields is used to extract rows from record sets.
func getRowsAndFields(ctx sessionctx.Context, recordSets []sqlexec.RecordSet) ([]chunk.Row, []*ast.ResultField, error) {
	var (
		rows   []chunk.Row
		fields []*ast.ResultField
	)

	for i, rs := range recordSets {
		tmp, err := getRowFromRecordSet(context.Background(), ctx, rs)
		if err != nil {
			return nil, nil, err
		}
		if err = rs.Close(); err != nil {
			return nil, nil, err
		}

		if i == 0 {
			rows = tmp
			fields = rs.Fields()
		}
	}
	return rows, fields, nil
}

func getRowFromRecordSet(ctx context.Context, se sessionctx.Context, rs sqlexec.RecordSet) ([]chunk.Row, error) {
	var rows []chunk.Row
	req := rs.NewChunk()
	for {
		err := rs.Next(ctx, req)
		if err != nil || req.NumRows() == 0 {
			return rows, err
		}
		iter := chunk.NewIterator4Chunk(req)
		for r := iter.Begin(); r != iter.End(); r = iter.Next() {
			rows = append(rows, r)
		}
		req = chunk.Renew(req, se.GetSessionVars().MaxChunkSize)
	}
}
