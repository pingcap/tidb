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
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/stringutil"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

var (
	userTablePrivilegeMask = computePrivMask(mysql.AllGlobalPrivs)
	dbTablePrivilegeMask   = computePrivMask(mysql.AllDBPrivs)
	tablePrivMask          = computePrivMask(mysql.AllTablePrivs)
	columnPrivMask         = computePrivMask(mysql.AllColumnPrivs)
)

const globalDBVisible = mysql.CreatePriv | mysql.SelectPriv | mysql.InsertPriv | mysql.UpdatePriv | mysql.DeletePriv | mysql.ShowDBPriv | mysql.DropPriv | mysql.AlterPriv | mysql.IndexPriv | mysql.CreateViewPriv | mysql.ShowViewPriv | mysql.GrantPriv

func computePrivMask(privs []mysql.PrivilegeType) mysql.PrivilegeType {
	var mask mysql.PrivilegeType
	for _, p := range privs {
		mask |= p
	}
	return mask
}

// UserRecord is used to represent a user record in privilege cache.
type UserRecord struct {
	Host       string // max length 60, primary key
	User       string // max length 16, primary key
	Password   string // max length 41
	Privileges mysql.PrivilegeType

	// patChars is compiled from Host, cached for pattern match performance.
	patChars []byte
	patTypes []byte
}

type dbRecord struct {
	Host       string
	DB         string
	User       string
	Privileges mysql.PrivilegeType

	// patChars is compiled from Host and DB, cached for pattern match performance.
	hostPatChars []byte
	hostPatTypes []byte

	dbPatChars []byte
	dbPatTypes []byte
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

	// patChars is compiled from Host, cached for pattern match performance.
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

	// patChars is compiled from Host, cached for pattern match performance.
	patChars []byte
	patTypes []byte
}

// MySQLPrivilege is the in-memory cache of mysql privilege tables.
type MySQLPrivilege struct {
	User        []UserRecord
	DB          []dbRecord
	TablesPriv  []tablesPrivRecord
	ColumnsPriv []columnsPrivRecord
}

// LoadAll loads the tables from database to memory.
func (p *MySQLPrivilege) LoadAll(ctx sessionctx.Context) error {
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
func (p *MySQLPrivilege) LoadUserTable(ctx sessionctx.Context) error {
	err := p.loadTable(ctx, "select HIGH_PRIORITY Host,User,Password,Select_priv,Insert_priv,Update_priv,Delete_priv,Create_priv,Drop_priv,Process_priv,Grant_priv,References_priv,Alter_priv,Show_db_priv,Super_priv,Execute_priv,Index_priv,Create_user_priv,Trigger_priv from mysql.user;", p.decodeUserTableRow)
	if err != nil {
		return errors.Trace(err)
	}
	// See https://dev.mysql.com/doc/refman/8.0/en/connection-access.html
	// When multiple matches are possible, the server must determine which of them to use. It resolves this issue as follows:
	// 1. Whenever the server reads the user table into memory, it sorts the rows.
	// 2. When a client attempts to connect, the server looks through the rows in sorted order.
	// 3. The server uses the first row that matches the client host name and user name.
	// The server uses sorting rules that order rows with the most-specific Host values first.
	p.SortUserTable()
	return nil
}

type sortedUserRecord []UserRecord

func (s sortedUserRecord) Len() int {
	return len(s)
}

func (s sortedUserRecord) Less(i, j int) bool {
	x := s[i]
	y := s[j]

	// Compare two item by user's host first.
	c1 := compareHost(x.Host, y.Host)
	if c1 < 0 {
		return true
	}
	if c1 > 0 {
		return false
	}

	// Then, compare item by user's name value.
	return x.User < y.User
}

// compareHost compares two host string using some special rules, return value 1, 0, -1 means > = <.
// TODO: Check how MySQL do it exactly, instead of guess its rules.
func compareHost(x, y string) int {
	// The more-specific, the smaller it is.
	// The pattern '%' means “any host” and is least specific.
	if y == `%` {
		if x == `%` {
			return 0
		}
		return -1
	}

	// The empty string '' also means “any host” but sorts after '%'.
	if y == "" {
		if x == "" {
			return 0
		}
		return -1
	}

	// One of them end with `%`.
	xEnd := strings.HasSuffix(x, `%`)
	yEnd := strings.HasSuffix(y, `%`)
	if xEnd || yEnd {
		switch {
		case !xEnd && yEnd:
			return -1
		case xEnd && !yEnd:
			return 1
		case xEnd && yEnd:
			// 192.168.199.% smaller than 192.168.%
			// A not very accurate comparison, compare them by length.
			if len(x) > len(y) {
				return -1
			}
		}
		return 0
	}

	// For other case, the order is nondeterministic.
	switch x < y {
	case true:
		return -1
	case false:
		return 1
	}
	return 0
}

func (s sortedUserRecord) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// SortUserTable sorts p.User in the MySQLPrivilege struct.
func (p MySQLPrivilege) SortUserTable() {
	sort.Sort(sortedUserRecord(p.User))
}

// LoadDBTable loads the mysql.db table from database.
func (p *MySQLPrivilege) LoadDBTable(ctx sessionctx.Context) error {
	return p.loadTable(ctx, "select HIGH_PRIORITY Host,DB,User,Select_priv,Insert_priv,Update_priv,Delete_priv,Create_priv,Drop_priv,Grant_priv,Index_priv,Alter_priv,Execute_priv from mysql.db order by host, db, user;", p.decodeDBTableRow)
}

// LoadTablesPrivTable loads the mysql.tables_priv table from database.
func (p *MySQLPrivilege) LoadTablesPrivTable(ctx sessionctx.Context) error {
	return p.loadTable(ctx, "select HIGH_PRIORITY Host,DB,User,Table_name,Grantor,Timestamp,Table_priv,Column_priv from mysql.tables_priv", p.decodeTablesPrivTableRow)
}

// LoadColumnsPrivTable loads the mysql.columns_priv table from database.
func (p *MySQLPrivilege) LoadColumnsPrivTable(ctx sessionctx.Context) error {
	return p.loadTable(ctx, "select HIGH_PRIORITY Host,DB,User,Table_name,Column_name,Timestamp,Column_priv from mysql.columns_priv", p.decodeColumnsPrivTableRow)
}

func (p *MySQLPrivilege) loadTable(sctx sessionctx.Context, sql string,
	decodeTableRow func(chunk.Row, []*ast.ResultField) error) error {
	ctx := context.Background()
	tmp, err := sctx.(sqlexec.SQLExecutor).Execute(ctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	rs := tmp[0]
	defer terror.Call(rs.Close)

	fs := rs.Fields()
	chk := rs.NewChunk()
	for {
		err = rs.Next(context.TODO(), chk)
		if err != nil {
			return errors.Trace(err)
		}
		if chk.NumRows() == 0 {
			return nil
		}
		it := chunk.NewIterator4Chunk(chk)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			err = decodeTableRow(row, fs)
			if err != nil {
				return errors.Trace(err)
			}
		}
		// NOTE: decodeTableRow decodes data from a chunk Row, that is a shallow copy.
		// The result will reference memory in the chunk, so the chunk must not be reused
		// here, otherwise some werid bug will happen!
		chk = chunk.Renew(chk, sctx.GetSessionVars().MaxChunkSize)
	}
}

func (p *MySQLPrivilege) decodeUserTableRow(row chunk.Row, fs []*ast.ResultField) error {
	var value UserRecord
	for i, f := range fs {
		switch {
		case f.ColumnAsName.L == "user":
			value.User = row.GetString(i)
		case f.ColumnAsName.L == "host":
			value.Host = row.GetString(i)
			value.patChars, value.patTypes = stringutil.CompilePattern(value.Host, '\\')
		case f.ColumnAsName.L == "password":
			value.Password = row.GetString(i)
		case f.Column.Tp == mysql.TypeEnum:
			if row.GetEnum(i).String() != "Y" {
				continue
			}
			priv, ok := mysql.Col2PrivType[f.ColumnAsName.O]
			if !ok {
				return errInvalidPrivilegeType.GenWithStack("Unknown Privilege Type!")
			}
			value.Privileges |= priv
		}
	}
	p.User = append(p.User, value)
	return nil
}

func (p *MySQLPrivilege) decodeDBTableRow(row chunk.Row, fs []*ast.ResultField) error {
	var value dbRecord
	for i, f := range fs {
		switch {
		case f.ColumnAsName.L == "user":
			value.User = row.GetString(i)
		case f.ColumnAsName.L == "host":
			value.Host = row.GetString(i)
			value.hostPatChars, value.hostPatTypes = stringutil.CompilePattern(value.Host, '\\')
		case f.ColumnAsName.L == "db":
			value.DB = row.GetString(i)
			value.dbPatChars, value.dbPatTypes = stringutil.CompilePattern(strings.ToUpper(value.DB), '\\')
		case f.Column.Tp == mysql.TypeEnum:
			if row.GetEnum(i).String() != "Y" {
				continue
			}
			priv, ok := mysql.Col2PrivType[f.ColumnAsName.O]
			if !ok {
				return errInvalidPrivilegeType.GenWithStack("Unknown Privilege Type!")
			}
			value.Privileges |= priv
		}
	}
	p.DB = append(p.DB, value)
	return nil
}

func (p *MySQLPrivilege) decodeTablesPrivTableRow(row chunk.Row, fs []*ast.ResultField) error {
	var value tablesPrivRecord
	for i, f := range fs {
		switch {
		case f.ColumnAsName.L == "user":
			value.User = row.GetString(i)
		case f.ColumnAsName.L == "host":
			value.Host = row.GetString(i)
			value.patChars, value.patTypes = stringutil.CompilePattern(value.Host, '\\')
		case f.ColumnAsName.L == "db":
			value.DB = row.GetString(i)
		case f.ColumnAsName.L == "table_name":
			value.TableName = row.GetString(i)
		case f.ColumnAsName.L == "table_priv":
			value.TablePriv = decodeSetToPrivilege(row.GetSet(i))
		case f.ColumnAsName.L == "column_priv":
			value.ColumnPriv = decodeSetToPrivilege(row.GetSet(i))
		}
	}
	p.TablesPriv = append(p.TablesPriv, value)
	return nil
}

func (p *MySQLPrivilege) decodeColumnsPrivTableRow(row chunk.Row, fs []*ast.ResultField) error {
	var value columnsPrivRecord
	for i, f := range fs {
		switch {
		case f.ColumnAsName.L == "user":
			value.User = row.GetString(i)
		case f.ColumnAsName.L == "host":
			value.Host = row.GetString(i)
			value.patChars, value.patTypes = stringutil.CompilePattern(value.Host, '\\')
		case f.ColumnAsName.L == "db":
			value.DB = row.GetString(i)
		case f.ColumnAsName.L == "table_name":
			value.TableName = row.GetString(i)
		case f.ColumnAsName.L == "column_name":
			value.ColumnName = row.GetString(i)
		case f.ColumnAsName.L == "timestamp":
			var err error
			value.Timestamp, err = row.GetTime(i).Time.GoTime(time.Local)
			if err != nil {
				return errors.Trace(err)
			}
		case f.ColumnAsName.L == "column_priv":
			value.ColumnPriv = decodeSetToPrivilege(row.GetSet(i))
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

func (record *UserRecord) match(user, host string) bool {
	return record.User == user && patternMatch(host, record.patChars, record.patTypes)
}

func (record *dbRecord) match(user, host, db string) bool {
	return record.User == user &&
		patternMatch(strings.ToUpper(db), record.dbPatChars, record.dbPatTypes) &&
		patternMatch(host, record.hostPatChars, record.hostPatTypes)
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
func (p *MySQLPrivilege) connectionVerification(user, host string) *UserRecord {
	for i := 0; i < len(p.User); i++ {
		record := &p.User[i]
		if record.match(user, host) {
			return record
		}
	}
	return nil
}

func (p *MySQLPrivilege) matchUser(user, host string) *UserRecord {
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

	return priv == 0
}

// DBIsVisible checks whether the user can see the db.
func (p *MySQLPrivilege) DBIsVisible(user, host, db string) bool {
	if record := p.matchUser(user, host); record != nil {
		if record.Privileges&globalDBVisible > 0 {
			return true
		}
	}

	// INFORMATION_SCHEMA is visible to all users.
	if strings.EqualFold(db, "INFORMATION_SCHEMA") {
		return true
	}

	if record := p.matchDB(user, host, db); record != nil {
		if record.Privileges > 0 {
			return true
		}
	}

	for _, record := range p.TablesPriv {
		if record.User == user &&
			patternMatch(host, record.patChars, record.patTypes) &&
			strings.EqualFold(record.DB, db) {
			if record.TablePriv != 0 || record.ColumnPriv != 0 {
				return true
			}
		}
	}

	for _, record := range p.ColumnsPriv {
		if record.User == user &&
			patternMatch(host, record.patChars, record.patTypes) &&
			strings.EqualFold(record.DB, db) {
			if record.ColumnPriv != 0 {
				return true
			}
		}
	}

	return false
}

func (p *MySQLPrivilege) showGrants(user, host string) []string {
	var gs []string
	// Show global grants
	for _, record := range p.User {
		if record.User == user && record.Host == host {
			g := userPrivToString(record.Privileges)
			if len(g) > 0 {
				s := fmt.Sprintf(`GRANT %s ON *.* TO '%s'@'%s'`, g, record.User, record.Host)
				gs = append(gs, s)
			}
			break // it's unique
		}
	}

	// Show db scope grants
	for _, record := range p.DB {
		if record.User == user && record.Host == host {
			g := dbPrivToString(record.Privileges)
			if len(g) > 0 {
				s := fmt.Sprintf(`GRANT %s ON %s.* TO '%s'@'%s'`, g, record.DB, record.User, record.Host)
				gs = append(gs, s)
			}
		}
	}

	// Show table scope grants
	for _, record := range p.TablesPriv {
		if record.User == user && record.Host == host {
			g := tablePrivToString(record.TablePriv)
			if len(g) > 0 {
				s := fmt.Sprintf(`GRANT %s ON %s.%s TO '%s'@'%s'`, g, record.DB, record.TableName, record.User, record.Host)
				gs = append(gs, s)
			}
		}
	}
	return gs
}

func userPrivToString(privs mysql.PrivilegeType) string {
	if privs == userTablePrivilegeMask {
		return mysql.AllPrivilegeLiteral
	}
	return privToString(privs, mysql.AllGlobalPrivs, mysql.Priv2Str)
}

func dbPrivToString(privs mysql.PrivilegeType) string {
	if privs == dbTablePrivilegeMask {
		return mysql.AllPrivilegeLiteral
	}
	return privToString(privs, mysql.AllDBPrivs, mysql.Priv2SetStr)
}

func tablePrivToString(privs mysql.PrivilegeType) string {
	if privs == tablePrivMask {
		return mysql.AllPrivilegeLiteral
	}
	return privToString(privs, mysql.AllTablePrivs, mysql.Priv2Str)
}

func privToString(priv mysql.PrivilegeType, allPrivs []mysql.PrivilegeType, allPrivNames map[mysql.PrivilegeType]string) string {
	pstrs := make([]string, 0, 20)
	for _, p := range allPrivs {
		if priv&p == 0 {
			continue
		}
		s, _ := allPrivNames[p]
		pstrs = append(pstrs, s)
	}
	return strings.Join(pstrs, ",")
}

// UserPrivilegesTable provide data for INFORMATION_SCHEMA.USERS_PRIVILEGE table.
func (p *MySQLPrivilege) UserPrivilegesTable() [][]types.Datum {
	var rows [][]types.Datum
	for _, user := range p.User {
		rows = appendUserPrivilegesTableRow(rows, user)
	}
	return rows
}

func appendUserPrivilegesTableRow(rows [][]types.Datum, user UserRecord) [][]types.Datum {
	var isGrantable string
	if user.Privileges&mysql.GrantPriv > 0 {
		isGrantable = "YES"
	} else {
		isGrantable = "NO"
	}
	guarantee := fmt.Sprintf("'%s'@'%s'", user.User, user.Host)

	for _, priv := range mysql.AllGlobalPrivs {
		if priv == mysql.GrantPriv {
			continue
		}
		if user.Privileges&priv > 0 {
			privilegeType := mysql.Priv2Str[priv]
			// +---------------------------+---------------+-------------------------+--------------+
			// | GRANTEE                   | TABLE_CATALOG | PRIVILEGE_TYPE          | IS_GRANTABLE |
			// +---------------------------+---------------+-------------------------+--------------+
			// | 'root'@'localhost'        | def           | SELECT                  | YES          |
			record := types.MakeDatums(guarantee, "def", privilegeType, isGrantable)
			rows = append(rows, record)
		}
	}
	return rows
}

// Handle wraps MySQLPrivilege providing thread safe access.
type Handle struct {
	priv atomic.Value
}

// NewHandle returns a Handle.
func NewHandle() *Handle {
	return &Handle{}
}

// Get the MySQLPrivilege for read.
func (h *Handle) Get() *MySQLPrivilege {
	return h.priv.Load().(*MySQLPrivilege)
}

// Update loads all the privilege info from kv storage.
func (h *Handle) Update(ctx sessionctx.Context) error {
	var priv MySQLPrivilege
	err := priv.LoadAll(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	h.priv.Store(&priv)
	return nil
}
