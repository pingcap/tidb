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

package privileges

import (
	"strings"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tidb/util/types"
)

const (
	userTablePrivilegeMask = mysql.SelectPriv | mysql.InsertPriv | mysql.UpdatePriv | mysql.DeletePriv | mysql.CreatePriv | mysql.DropPriv | mysql.GrantPriv | mysql.IndexPriv | mysql.AlterPriv | mysql.ShowDBPriv | mysql.ExecutePriv | mysql.CreateUserPriv
	dbTablePrivilegeMask   = mysql.SelectPriv | mysql.InsertPriv | mysql.UpdatePriv | mysql.DeletePriv | mysql.CreatePriv | mysql.DropPriv | mysql.GrantPriv | mysql.IndexPriv | mysql.AlterPriv
	tablePrivMask          = mysql.SelectPriv | mysql.InsertPriv | mysql.UpdatePriv | mysql.DeletePriv | mysql.CreatePriv | mysql.DropPriv | mysql.GrantPriv | mysql.IndexPriv | mysql.AlterPriv
	columnPrivMask         = mysql.SelectPriv | mysql.InsertPriv | mysql.UpdatePriv
)

type userRecord struct {
	Host       string // max length 60, primary key
	User       string // max length 16, primary key
	Password   string // max length 41
	Privileges mysql.PrivilegeType

	// Compiled from Host, cached for pattern match performance.
	patChars []byte
	patTypes []byte
}

type dbRecord struct {
	Host       string
	DB         string
	User       string
	Privileges mysql.PrivilegeType

	// Compiled from Host, cached for pattern match performance.
	patChars []byte
	patTypes []byte
}

type tablesPrivRecord struct {
	Host       string
	DB         string
	User       string
	TableName  string
	Grantor    string
	Timestamp  time.Time
	TablePriv  mysql.PrivilegeType
	ColumnPriv mysql.PrivilegeType

	// Compiled from Host, cached for pattern match performance.
	patChars []byte
	patTypes []byte
}

type columnsPrivRecord struct {
	Host       string
	DB         string
	User       string
	TableName  string
	ColumnName string
	Timestamp  time.Time
	ColumnPriv mysql.PrivilegeType

	// Compiled from Host, cached for pattern match performance.
	patChars []byte
	patTypes []byte
}

// MySQLPrivilege is the in-memory cache of mysql privilege tables.
type MySQLPrivilege struct {
	User        []userRecord
	DB          []dbRecord
	TablesPriv  []tablesPrivRecord
	ColumnsPriv []columnsPrivRecord
}

// LoadAll loads the tables from database to memory.
func (p *MySQLPrivilege) LoadAll(ctx context.Context) error {
	err := p.LoadUserTable(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = p.LoadDBTable(ctx)
	if err != nil {
		if !noSuchTable(err) {
			return errors.Trace(err)
		}
		log.Warn("mysql.db maybe missing")
	}

	err = p.LoadTablesPrivTable(ctx)
	if err != nil {
		if !noSuchTable(err) {
			return errors.Trace(err)
		}
		log.Warn("mysql.tables_priv missing")
	}

	err = p.LoadColumnsPrivTable(ctx)
	if err != nil {
		if !noSuchTable(err) {
			return errors.Trace(err)
		}
		log.Warn("mysql.columns_priv missing")
	}
	return nil
}

func noSuchTable(err error) bool {
	e1 := errors.Cause(err)
	if e2, ok := e1.(*terror.Error); ok {
		if e2.Code() == terror.ErrCode(mysql.ErrNoSuchTable) {
			return true
		}
	}
	return false
}

// LoadUserTable loads the mysql.user table from database.
func (p *MySQLPrivilege) LoadUserTable(ctx context.Context) error {
	return p.loadTable(ctx, "select Host,User,Password,Select_priv,Insert_priv,Update_priv,Delete_priv,Create_priv,Drop_priv,Grant_priv,Alter_priv,Show_db_priv,Execute_priv,Index_priv,Create_user_priv from mysql.user order by host, user;", p.decodeUserTableRow)
}

// LoadDBTable loads the mysql.db table from database.
func (p *MySQLPrivilege) LoadDBTable(ctx context.Context) error {
	return p.loadTable(ctx, "select Host,DB,User,Select_priv,Insert_priv,Update_priv,Delete_priv,Create_priv,Drop_priv,Grant_priv,Index_priv,Alter_priv,Execute_priv from mysql.db order by host, db, user;", p.decodeDBTableRow)
}

// LoadTablesPrivTable loads the mysql.tables_priv table from database.
func (p *MySQLPrivilege) LoadTablesPrivTable(ctx context.Context) error {
	return p.loadTable(ctx, "select Host,DB,User,Table_name,Grantor,Timestamp,Table_priv,Column_priv from mysql.tables_priv", p.decodeTablesPrivTableRow)
}

// LoadColumnsPrivTable loads the mysql.columns_priv table from database.
func (p *MySQLPrivilege) LoadColumnsPrivTable(ctx context.Context) error {
	return p.loadTable(ctx, "select Host,DB,User,Table_name,Column_name,Timestamp,Column_priv from mysql.columns_priv", p.decodeColumnsPrivTableRow)
}

func (p *MySQLPrivilege) loadTable(ctx context.Context, sql string,
	decodeTableRow func(*ast.Row, []*ast.ResultField) error) error {
	tmp, err := ctx.(sqlexec.SQLExecutor).Execute(sql)
	if err != nil {
		return errors.Trace(err)
	}
	rs := tmp[0]
	defer rs.Close()

	fs, err := rs.Fields()
	if err != nil {
		return errors.Trace(err)
	}
	for {
		row, err := rs.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			break
		}

		err = decodeTableRow(row, fs)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (p *MySQLPrivilege) decodeUserTableRow(row *ast.Row, fs []*ast.ResultField) error {
	var value userRecord
	for i, f := range fs {
		d := row.Data[i]
		switch {
		case f.ColumnAsName.L == "user":
			value.User = d.GetString()
		case f.ColumnAsName.L == "host":
			value.Host = d.GetString()
			value.patChars, value.patTypes = stringutil.CompilePattern(value.Host, '\\')
		case f.ColumnAsName.L == "password":
			value.Password = d.GetString()
		case d.Kind() == types.KindMysqlEnum:
			ed := d.GetMysqlEnum()
			if ed.String() != "Y" {
				continue
			}
			priv, ok := mysql.Col2PrivType[f.ColumnAsName.O]
			if !ok {
				return errInvalidPrivilegeType.Gen("Unknown Privilege Type!")
			}
			value.Privileges |= priv
		}
	}
	p.User = append(p.User, value)
	return nil
}

func (p *MySQLPrivilege) decodeDBTableRow(row *ast.Row, fs []*ast.ResultField) error {
	var value dbRecord
	for i, f := range fs {
		d := row.Data[i]
		switch {
		case f.ColumnAsName.L == "user":
			value.User = d.GetString()
		case f.ColumnAsName.L == "host":
			value.Host = d.GetString()
			value.patChars, value.patTypes = stringutil.CompilePattern(value.Host, '\\')
		case f.ColumnAsName.L == "db":
			value.DB = d.GetString()
		case d.Kind() == types.KindMysqlEnum:
			ed := d.GetMysqlEnum()
			if ed.String() != "Y" {
				continue
			}
			priv, ok := mysql.Col2PrivType[f.ColumnAsName.O]
			if !ok {
				return errInvalidPrivilegeType.Gen("Unknown Privilege Type!")
			}
			value.Privileges |= priv
		}
	}
	p.DB = append(p.DB, value)
	return nil
}

func (p *MySQLPrivilege) decodeTablesPrivTableRow(row *ast.Row, fs []*ast.ResultField) error {
	var value tablesPrivRecord
	for i, f := range fs {
		d := row.Data[i]
		switch {
		case f.ColumnAsName.L == "user":
			value.User = d.GetString()
		case f.ColumnAsName.L == "host":
			value.Host = d.GetString()
			value.patChars, value.patTypes = stringutil.CompilePattern(value.Host, '\\')
		case f.ColumnAsName.L == "db":
			value.DB = d.GetString()
		case f.ColumnAsName.L == "table_name":
			value.TableName = d.GetString()
		case f.ColumnAsName.L == "table_priv":
			value.TablePriv = decodeSetToPrivilege(d.GetMysqlSet())
		case f.ColumnAsName.L == "column_priv":
			value.ColumnPriv = decodeSetToPrivilege(d.GetMysqlSet())
		}
	}
	p.TablesPriv = append(p.TablesPriv, value)
	return nil
}

func (p *MySQLPrivilege) decodeColumnsPrivTableRow(row *ast.Row, fs []*ast.ResultField) error {
	var value columnsPrivRecord
	for i, f := range fs {
		d := row.Data[i]
		switch {
		case f.ColumnAsName.L == "user":
			value.User = d.GetString()
		case f.ColumnAsName.L == "host":
			value.Host = d.GetString()
			value.patChars, value.patTypes = stringutil.CompilePattern(value.Host, '\\')
		case f.ColumnAsName.L == "db":
			value.DB = d.GetString()
		case f.ColumnAsName.L == "table_name":
			value.TableName = d.GetString()
		case f.ColumnAsName.L == "column_name":
			value.ColumnName = d.GetString()
		case f.ColumnAsName.L == "timestamp":
			value.Timestamp, _ = d.GetMysqlTime().Time.GoTime(time.Local)
		case f.ColumnAsName.L == "column_priv":
			value.ColumnPriv = decodeSetToPrivilege(d.GetMysqlSet())
		}
	}
	p.ColumnsPriv = append(p.ColumnsPriv, value)
	return nil
}

func decodeSetToPrivilege(s types.Set) mysql.PrivilegeType {
	var ret mysql.PrivilegeType
	if s.Name == "" {
		return ret
	}
	for _, str := range strings.Split(s.Name, ",") {
		priv, ok := mysql.SetStr2Priv[str]
		if !ok {
			log.Warn("unsupported privilege type:", str)
			continue
		}
		ret |= priv
	}
	return ret
}

func (record *userRecord) match(user, host string) bool {
	return record.User == user && patternMatch(host, record.patChars, record.patTypes)
}

func (record *dbRecord) match(user, host, db string) bool {
	return record.User == user && strings.EqualFold(record.DB, db) &&
		patternMatch(host, record.patChars, record.patTypes)
}

func (record *tablesPrivRecord) match(user, host, db, table string) bool {
	return record.User == user && strings.EqualFold(record.DB, db) &&
		strings.EqualFold(record.TableName, table) && patternMatch(host, record.patChars, record.patTypes)
}

func (record *columnsPrivRecord) match(user, host, db, table, col string) bool {
	return record.User == user && strings.EqualFold(record.DB, db) &&
		strings.EqualFold(record.TableName, table) &&
		strings.EqualFold(record.ColumnName, col) &&
		patternMatch(host, record.patChars, record.patTypes)
}

// patternMatch matches "%" the same way as ".*" in regular expression, for example,
// "10.0.%" would match "10.0.1" "10.0.1.118" ...
func patternMatch(str string, patChars, patTypes []byte) bool {
	return stringutil.DoMatch(str, patChars, patTypes)
}

// connectionVerification verifies the connection have access to TiDB server.
func (p *MySQLPrivilege) connectionVerification(user, host string) *userRecord {
	for i := 0; i < len(p.User); i++ {
		record := &p.User[i]
		if record.match(user, host) {
			return record
		}
	}
	return nil
}

func (p *MySQLPrivilege) matchUser(user, host string) *userRecord {
	for i := 0; i < len(p.User); i++ {
		record := &p.User[i]
		if record.match(user, host) {
			return record
		}
	}
	return nil
}

func (p *MySQLPrivilege) matchDB(user, host, db string) *dbRecord {
	for i := 0; i < len(p.DB); i++ {
		record := &p.DB[i]
		if record.match(user, host, db) {
			return record
		}
	}
	return nil
}

func (p *MySQLPrivilege) matchTables(user, host, db, table string) *tablesPrivRecord {
	for i := 0; i < len(p.TablesPriv); i++ {
		record := &p.TablesPriv[i]
		if record.match(user, host, db, table) {
			return record
		}
	}
	return nil
}

func (p *MySQLPrivilege) matchColumns(user, host, db, table, column string) *columnsPrivRecord {
	for i := 0; i < len(p.ColumnsPriv); i++ {
		record := &p.ColumnsPriv[i]
		if record.match(user, host, db, table, column) {
			return record
		}
	}
	return nil
}

// RequestVerification checks whether the user have sufficient privileges to do the operation.
func (p *MySQLPrivilege) RequestVerification(user, host, db, table, column string, priv mysql.PrivilegeType) bool {
	record1 := p.matchUser(user, host)
	if record1 != nil && record1.Privileges&priv > 0 {
		return true
	}

	record2 := p.matchDB(user, host, db)
	if record2 != nil && record2.Privileges&priv > 0 {
		return true
	}

	record3 := p.matchTables(user, host, db, table)
	if record3 != nil {
		if record3.TablePriv&priv > 0 {
			return true
		}
		if column != "" && record3.ColumnPriv&priv > 0 {
			return true
		}
	}

	record4 := p.matchColumns(user, host, db, table, column)
	if record4 != nil && record4.ColumnPriv&priv > 0 {
		return true
	}

	return false
}

// Handle wraps MySQLPrivilege providing thread safe access.
type Handle struct {
	ctx  context.Context
	priv atomic.Value
}

// NewHandle returns a Handle.
func NewHandle(ctx context.Context) *Handle {
	return &Handle{
		ctx: ctx,
	}
}

// Get the MySQLPrivilege for read.
func (h *Handle) Get() *MySQLPrivilege {
	return h.priv.Load().(*MySQLPrivilege)
}

// Update loads all the privilege info from kv storage.
func (h *Handle) Update() error {
	var priv MySQLPrivilege
	err := priv.LoadAll(h.ctx)
	if err != nil {
		return errors.Trace(err)
	}

	h.priv.Store(&priv)
	return nil
}
