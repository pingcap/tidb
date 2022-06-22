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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package privileges

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sem"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/stringutil"
	"go.uber.org/zap"
)

var (
	userTablePrivilegeMask = computePrivMask(mysql.AllGlobalPrivs)
	dbTablePrivilegeMask   = computePrivMask(mysql.AllDBPrivs)
	tablePrivMask          = computePrivMask(mysql.AllTablePrivs)
)

const globalDBVisible = mysql.CreatePriv | mysql.SelectPriv | mysql.InsertPriv | mysql.UpdatePriv | mysql.DeletePriv | mysql.ShowDBPriv | mysql.DropPriv | mysql.AlterPriv | mysql.IndexPriv | mysql.CreateViewPriv | mysql.ShowViewPriv | mysql.GrantPriv | mysql.TriggerPriv | mysql.ReferencesPriv | mysql.ExecutePriv

const (
	sqlLoadRoleGraph        = "SELECT HIGH_PRIORITY FROM_USER, FROM_HOST, TO_USER, TO_HOST FROM mysql.role_edges"
	sqlLoadGlobalPrivTable  = "SELECT HIGH_PRIORITY Host,User,Priv FROM mysql.global_priv"
	sqlLoadDBTable          = "SELECT HIGH_PRIORITY Host,DB,User,Select_priv,Insert_priv,Update_priv,Delete_priv,Create_priv,Drop_priv,Grant_priv,Index_priv,References_priv,Lock_tables_priv,Create_tmp_table_priv,Event_priv,Create_routine_priv,Alter_routine_priv,Alter_priv,Execute_priv,Create_view_priv,Show_view_priv,Trigger_priv FROM mysql.db ORDER BY host, db, user"
	sqlLoadTablePrivTable   = "SELECT HIGH_PRIORITY Host,DB,User,Table_name,Grantor,Timestamp,Table_priv,Column_priv FROM mysql.tables_priv"
	sqlLoadColumnsPrivTable = "SELECT HIGH_PRIORITY Host,DB,User,Table_name,Column_name,Timestamp,Column_priv FROM mysql.columns_priv"
	sqlLoadDefaultRoles     = "SELECT HIGH_PRIORITY HOST, USER, DEFAULT_ROLE_HOST, DEFAULT_ROLE_USER FROM mysql.default_roles"
	// list of privileges from mysql.Priv2UserCol
	sqlLoadUserTable = `SELECT HIGH_PRIORITY Host,User,authentication_string,
	Create_priv, Select_priv, Insert_priv, Update_priv, Delete_priv, Show_db_priv, Super_priv,
	Create_user_priv,Create_tablespace_priv,Trigger_priv,Drop_priv,Process_priv,Grant_priv,
	References_priv,Alter_priv,Execute_priv,Index_priv,Create_view_priv,Show_view_priv,
	Create_role_priv,Drop_role_priv,Create_tmp_table_priv,Lock_tables_priv,Create_routine_priv,
	Alter_routine_priv,Event_priv,Shutdown_priv,Reload_priv,File_priv,Config_priv,Repl_client_priv,Repl_slave_priv,
	account_locked,plugin FROM mysql.user`
	sqlLoadGlobalGrantsTable = `SELECT HIGH_PRIORITY Host,User,Priv,With_Grant_Option FROM mysql.global_grants`
)

func computePrivMask(privs []mysql.PrivilegeType) mysql.PrivilegeType {
	var mask mysql.PrivilegeType
	for _, p := range privs {
		mask |= p
	}
	return mask
}

// baseRecord is used to represent a base record in privilege cache,
// it only store Host and User field, and it should be nested in other record type.
type baseRecord struct {
	Host string // max length 60, primary key
	User string // max length 32, primary key

	// patChars is compiled from Host, cached for pattern match performance.
	patChars []byte
	patTypes []byte

	// IPv4 with netmask, cached for host match performance.
	hostIPNet *net.IPNet
}

// UserRecord is used to represent a user record in privilege cache.
type UserRecord struct {
	baseRecord

	AuthenticationString string
	Privileges           mysql.PrivilegeType
	AccountLocked        bool // A role record when this field is true
	AuthPlugin           string
}

// NewUserRecord return a UserRecord, only use for unit test.
func NewUserRecord(host, user string) UserRecord {
	return UserRecord{
		baseRecord: baseRecord{
			Host: host,
			User: user,
		},
	}
}

type globalPrivRecord struct {
	baseRecord

	Priv   GlobalPrivValue
	Broken bool
}

type dynamicPrivRecord struct {
	baseRecord

	PrivilegeName string
	GrantOption   bool
}

// SSLType is enum value for GlobalPrivValue.SSLType.
// the value is compatible with MySQL storage json value.
type SSLType int

const (
	// SslTypeNotSpecified indicates .
	SslTypeNotSpecified SSLType = iota - 1
	// SslTypeNone indicates not require use ssl.
	SslTypeNone
	// SslTypeAny indicates require use ssl but not validate cert.
	SslTypeAny
	// SslTypeX509 indicates require use ssl and validate cert.
	SslTypeX509
	// SslTypeSpecified indicates require use ssl and validate cert's subject or issuer.
	SslTypeSpecified
)

// GlobalPrivValue is store json format for priv column in mysql.global_priv.
type GlobalPrivValue struct {
	SSLType     SSLType                   `json:"ssl_type,omitempty"`
	SSLCipher   string                    `json:"ssl_cipher,omitempty"`
	X509Issuer  string                    `json:"x509_issuer,omitempty"`
	X509Subject string                    `json:"x509_subject,omitempty"`
	SAN         string                    `json:"san,omitempty"`
	SANs        map[util.SANType][]string `json:"-"`
}

// RequireStr returns describe string after `REQUIRE` clause.
func (g *GlobalPrivValue) RequireStr() string {
	require := "NONE"
	switch g.SSLType {
	case SslTypeAny:
		require = "SSL"
	case SslTypeX509:
		require = "X509"
	case SslTypeSpecified:
		var s []string
		if len(g.SSLCipher) > 0 {
			s = append(s, "CIPHER")
			s = append(s, "'"+g.SSLCipher+"'")
		}
		if len(g.X509Issuer) > 0 {
			s = append(s, "ISSUER")
			s = append(s, "'"+g.X509Issuer+"'")
		}
		if len(g.X509Subject) > 0 {
			s = append(s, "SUBJECT")
			s = append(s, "'"+g.X509Subject+"'")
		}
		if len(g.SAN) > 0 {
			s = append(s, "SAN")
			s = append(s, "'"+g.SAN+"'")
		}
		if len(s) > 0 {
			require = strings.Join(s, " ")
		}
	}
	return require
}

type dbRecord struct {
	baseRecord

	DB         string
	Privileges mysql.PrivilegeType

	dbPatChars []byte
	dbPatTypes []byte
}

type tablesPrivRecord struct {
	baseRecord

	DB         string
	TableName  string
	Grantor    string
	Timestamp  time.Time
	TablePriv  mysql.PrivilegeType
	ColumnPriv mysql.PrivilegeType
}

type columnsPrivRecord struct {
	baseRecord

	DB         string
	TableName  string
	ColumnName string
	Timestamp  time.Time
	ColumnPriv mysql.PrivilegeType
}

// defaultRoleRecord is used to cache mysql.default_roles
type defaultRoleRecord struct {
	baseRecord

	DefaultRoleUser string
	DefaultRoleHost string
}

// roleGraphEdgesTable is used to cache relationship between and role.
type roleGraphEdgesTable struct {
	roleList map[string]*auth.RoleIdentity
}

// Find method is used to find role from table
func (g roleGraphEdgesTable) Find(user, host string) bool {
	if host == "" {
		host = "%"
	}
	key := user + "@" + host
	if g.roleList == nil {
		return false
	}
	_, ok := g.roleList[key]
	return ok
}

// MySQLPrivilege is the in-memory cache of mysql privilege tables.
type MySQLPrivilege struct {
	// In MySQL, a user identity consists of a user + host.
	// Either portion of user or host can contain wildcards,
	// requiring the privileges system to use a list-like
	// structure instead of a hash.

	// TiDB contains a sensible behavior difference from MySQL,
	// which is that usernames can not contain wildcards.
	// This means that DB-records are organized in both a
	// slice (p.DB) and a Map (p.DBMap).

	// This helps in the case that there are a number of users with
	// non-full privileges (i.e. user.db entries).
	User          []UserRecord
	UserMap       map[string][]UserRecord // Accelerate User searching
	Global        map[string][]globalPrivRecord
	Dynamic       map[string][]dynamicPrivRecord
	DB            []dbRecord
	DBMap         map[string][]dbRecord // Accelerate DB searching
	TablesPriv    []tablesPrivRecord
	TablesPrivMap map[string][]tablesPrivRecord // Accelerate TablesPriv searching
	ColumnsPriv   []columnsPrivRecord
	DefaultRoles  []defaultRoleRecord
	RoleGraph     map[string]roleGraphEdgesTable
}

// FindAllUserEffectiveRoles is used to find all effective roles grant to this user.
// This method will filter out the roles that are not granted to the user but are still in activeRoles
func (p *MySQLPrivilege) FindAllUserEffectiveRoles(user, host string, activeRoles []*auth.RoleIdentity) []*auth.RoleIdentity {
	grantedActiveRoles := make([]*auth.RoleIdentity, 0, len(activeRoles))
	for _, role := range activeRoles {
		if p.FindRole(user, host, role) {
			grantedActiveRoles = append(grantedActiveRoles, role)
		}
	}
	return p.FindAllRole(grantedActiveRoles)
}

// FindAllRole is used to find all roles grant to this user.
func (p *MySQLPrivilege) FindAllRole(activeRoles []*auth.RoleIdentity) []*auth.RoleIdentity {
	queue, head := make([]*auth.RoleIdentity, 0, len(activeRoles)), 0
	queue = append(queue, activeRoles...)
	// Using breadth first search to find all roles grant to this user.
	visited, ret := make(map[string]bool), make([]*auth.RoleIdentity, 0)
	for head < len(queue) {
		role := queue[head]
		if _, ok := visited[role.String()]; !ok {
			visited[role.String()] = true
			ret = append(ret, role)
			key := role.Username + "@" + role.Hostname
			if edgeTable, ok := p.RoleGraph[key]; ok {
				for _, v := range edgeTable.roleList {
					if _, ok := visited[v.String()]; !ok {
						queue = append(queue, v)
					}
				}
			}
		}
		head += 1
	}
	return ret
}

// FindRole is used to detect whether there is edges between users and roles.
func (p *MySQLPrivilege) FindRole(user string, host string, role *auth.RoleIdentity) bool {
	rec := p.matchUser(user, host)
	r := p.matchUser(role.Username, role.Hostname)
	if rec != nil && r != nil {
		key := rec.User + "@" + rec.Host
		return p.RoleGraph[key].Find(role.Username, role.Hostname)
	}
	return false
}

// LoadAll loads the tables from database to memory.
func (p *MySQLPrivilege) LoadAll(ctx sessionctx.Context) error {
	err := p.LoadUserTable(ctx)
	if err != nil {
		logutil.BgLogger().Warn("load mysql.user fail", zap.Error(err))
		return errLoadPrivilege.FastGen("mysql.user")
	}

	err = p.LoadGlobalPrivTable(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = p.LoadGlobalGrantsTable(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = p.LoadDBTable(ctx)
	if err != nil {
		if !noSuchTable(err) {
			logutil.BgLogger().Warn("load mysql.db fail", zap.Error(err))
			return errLoadPrivilege.FastGen("mysql.db")
		}
		logutil.BgLogger().Warn("mysql.db maybe missing")
	}

	err = p.LoadTablesPrivTable(ctx)
	if err != nil {
		if !noSuchTable(err) {
			logutil.BgLogger().Warn("load mysql.tables_priv fail", zap.Error(err))
			return errLoadPrivilege.FastGen("mysql.tables_priv")
		}
		logutil.BgLogger().Warn("mysql.tables_priv missing")
	}

	err = p.LoadDefaultRoles(ctx)
	if err != nil {
		if !noSuchTable(err) {
			logutil.BgLogger().Warn("load mysql.roles", zap.Error(err))
			return errLoadPrivilege.FastGen("mysql.roles")
		}
		logutil.BgLogger().Warn("mysql.default_roles missing")
	}

	err = p.LoadColumnsPrivTable(ctx)
	if err != nil {
		if !noSuchTable(err) {
			logutil.BgLogger().Warn("load mysql.columns_priv", zap.Error(err))
			return errLoadPrivilege.FastGen("mysql.columns_priv")
		}
		logutil.BgLogger().Warn("mysql.columns_priv missing")
	}

	err = p.LoadRoleGraph(ctx)
	if err != nil {
		if !noSuchTable(err) {
			logutil.BgLogger().Warn("load mysql.role_edges", zap.Error(err))
			return errLoadPrivilege.FastGen("mysql.role_edges")
		}
		logutil.BgLogger().Warn("mysql.role_edges missing")
	}
	return nil
}

func noSuchTable(err error) bool {
	e1 := errors.Cause(err)
	if e2, ok := e1.(*terror.Error); ok {
		if terror.ErrCode(e2.Code()) == terror.ErrCode(mysql.ErrNoSuchTable) {
			return true
		}
	}
	return false
}

// LoadRoleGraph loads the mysql.role_edges table from database.
func (p *MySQLPrivilege) LoadRoleGraph(ctx sessionctx.Context) error {
	p.RoleGraph = make(map[string]roleGraphEdgesTable)
	err := p.loadTable(ctx, sqlLoadRoleGraph, p.decodeRoleEdgesTable)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// LoadUserTable loads the mysql.user table from database.
func (p *MySQLPrivilege) LoadUserTable(ctx sessionctx.Context) error {
	err := p.loadTable(ctx, sqlLoadUserTable, p.decodeUserTableRow)
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
	p.buildUserMap()
	return nil
}

func (p *MySQLPrivilege) buildUserMap() {
	userMap := make(map[string][]UserRecord, len(p.User))
	for _, record := range p.User {
		userMap[record.User] = append(userMap[record.User], record)
	}
	p.UserMap = userMap
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

// LoadGlobalPrivTable loads the mysql.global_priv table from database.
func (p *MySQLPrivilege) LoadGlobalPrivTable(ctx sessionctx.Context) error {
	return p.loadTable(ctx, sqlLoadGlobalPrivTable, p.decodeGlobalPrivTableRow)
}

// LoadGlobalGrantsTable loads the mysql.global_priv table from database.
func (p *MySQLPrivilege) LoadGlobalGrantsTable(ctx sessionctx.Context) error {
	return p.loadTable(ctx, sqlLoadGlobalGrantsTable, p.decodeGlobalGrantsTableRow)
}

// LoadDBTable loads the mysql.db table from database.
func (p *MySQLPrivilege) LoadDBTable(ctx sessionctx.Context) error {
	err := p.loadTable(ctx, sqlLoadDBTable, p.decodeDBTableRow)
	if err != nil {
		return err
	}
	p.buildDBMap()
	return nil
}

func (p *MySQLPrivilege) buildDBMap() {
	dbMap := make(map[string][]dbRecord, len(p.DB))
	for _, record := range p.DB {
		dbMap[record.User] = append(dbMap[record.User], record)
	}
	p.DBMap = dbMap
}

// LoadTablesPrivTable loads the mysql.tables_priv table from database.
func (p *MySQLPrivilege) LoadTablesPrivTable(ctx sessionctx.Context) error {
	err := p.loadTable(ctx, sqlLoadTablePrivTable, p.decodeTablesPrivTableRow)
	if err != nil {
		return err
	}
	p.buildTablesPrivMap()
	return nil
}

func (p *MySQLPrivilege) buildTablesPrivMap() {
	tablesPrivMap := make(map[string][]tablesPrivRecord, len(p.TablesPriv))
	for _, record := range p.TablesPriv {
		tablesPrivMap[record.User] = append(tablesPrivMap[record.User], record)
	}
	p.TablesPrivMap = tablesPrivMap
}

// LoadColumnsPrivTable loads the mysql.columns_priv table from database.
func (p *MySQLPrivilege) LoadColumnsPrivTable(ctx sessionctx.Context) error {
	return p.loadTable(ctx, sqlLoadColumnsPrivTable, p.decodeColumnsPrivTableRow)
}

// LoadDefaultRoles loads the mysql.columns_priv table from database.
func (p *MySQLPrivilege) LoadDefaultRoles(ctx sessionctx.Context) error {
	return p.loadTable(ctx, sqlLoadDefaultRoles, p.decodeDefaultRoleTableRow)
}

func (p *MySQLPrivilege) loadTable(sctx sessionctx.Context, sql string,
	decodeTableRow func(chunk.Row, []*ast.ResultField) error) error {
	ctx := context.Background()
	rs, err := sctx.(sqlexec.SQLExecutor).ExecuteInternal(ctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	defer terror.Call(rs.Close)
	fs := rs.Fields()
	req := rs.NewChunk(nil)
	for {
		err = rs.Next(context.TODO(), req)
		if err != nil {
			return errors.Trace(err)
		}
		if req.NumRows() == 0 {
			return nil
		}
		it := chunk.NewIterator4Chunk(req)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			err = decodeTableRow(row, fs)
			if err != nil {
				return errors.Trace(err)
			}
		}
		// NOTE: decodeTableRow decodes data from a chunk Row, that is a shallow copy.
		// The result will reference memory in the chunk, so the chunk must not be reused
		// here, otherwise some werid bug will happen!
		req = chunk.Renew(req, sctx.GetSessionVars().MaxChunkSize)
	}
}

// parseHostIPNet parses an IPv4 address and its subnet mask (e.g. `127.0.0.0/255.255.255.0`),
// return the `IPNet` struct which represent the IP range info (e.g. `127.0.0.1 ~ 127.0.0.255`).
// `IPNet` is used to check if a giving IP (e.g. `127.0.0.1`) is in its IP range by call `IPNet.Contains(ip)`.
func parseHostIPNet(s string) *net.IPNet {
	i := strings.IndexByte(s, '/')
	if i < 0 {
		return nil
	}
	hostIP := net.ParseIP(s[:i]).To4()
	if hostIP == nil {
		return nil
	}
	maskIP := net.ParseIP(s[i+1:]).To4()
	if maskIP == nil {
		return nil
	}
	mask := net.IPv4Mask(maskIP[0], maskIP[1], maskIP[2], maskIP[3])
	// We must ensure that: <host_ip> & <netmask> == <host_ip>
	// e.g. `127.0.0.1/255.0.0.0` is an illegal string,
	// because `127.0.0.1` & `255.0.0.0` == `127.0.0.0`, but != `127.0.0.1`
	// see https://dev.mysql.com/doc/refman/5.7/en/account-names.html
	if !hostIP.Equal(hostIP.Mask(mask)) {
		return nil
	}
	return &net.IPNet{
		IP:   hostIP,
		Mask: mask,
	}
}

func (record *baseRecord) assignUserOrHost(row chunk.Row, i int, f *ast.ResultField) {
	switch f.ColumnAsName.L {
	case "user":
		record.User = row.GetString(i)
	case "host":
		record.Host = row.GetString(i)
		record.patChars, record.patTypes = stringutil.CompilePatternBytes(record.Host, '\\')
		record.hostIPNet = parseHostIPNet(record.Host)
	}
}

func (p *MySQLPrivilege) decodeUserTableRow(row chunk.Row, fs []*ast.ResultField) error {
	var value UserRecord
	for i, f := range fs {
		switch {
		case f.ColumnAsName.L == "authentication_string":
			value.AuthenticationString = row.GetString(i)
		case f.ColumnAsName.L == "account_locked":
			if row.GetEnum(i).String() == "Y" {
				value.AccountLocked = true
			}
		case f.ColumnAsName.L == "plugin":
			if row.GetString(i) != "" {
				value.AuthPlugin = row.GetString(i)
			} else {
				value.AuthPlugin = mysql.AuthNativePassword
			}
		case f.Column.GetType() == mysql.TypeEnum:
			if row.GetEnum(i).String() != "Y" {
				continue
			}
			priv, ok := mysql.Col2PrivType[f.ColumnAsName.O]
			if !ok {
				return errInvalidPrivilegeType.GenWithStack(f.ColumnAsName.O)
			}
			value.Privileges |= priv
		default:
			value.assignUserOrHost(row, i, f)
		}
	}
	p.User = append(p.User, value)
	return nil
}

func (p *MySQLPrivilege) decodeGlobalPrivTableRow(row chunk.Row, fs []*ast.ResultField) error {
	var value globalPrivRecord
	for i, f := range fs {
		switch {
		case f.ColumnAsName.L == "priv":
			privData := row.GetString(i)
			if len(privData) > 0 {
				var privValue GlobalPrivValue
				err := json.Unmarshal(hack.Slice(privData), &privValue)
				if err != nil {
					logutil.BgLogger().Error("one user global priv data is broken, forbidden login until data be fixed",
						zap.String("user", value.User), zap.String("host", value.Host))
					value.Broken = true
				} else {
					value.Priv.SSLType = privValue.SSLType
					value.Priv.SSLCipher = privValue.SSLCipher
					value.Priv.X509Issuer = privValue.X509Issuer
					value.Priv.X509Subject = privValue.X509Subject
					value.Priv.SAN = privValue.SAN
					if len(value.Priv.SAN) > 0 {
						value.Priv.SANs, err = util.ParseAndCheckSAN(value.Priv.SAN)
						if err != nil {
							value.Broken = true
						}
					}
				}
			}
		default:
			value.assignUserOrHost(row, i, f)
		}
	}
	if p.Global == nil {
		p.Global = make(map[string][]globalPrivRecord)
	}
	p.Global[value.User] = append(p.Global[value.User], value)
	return nil
}

func (p *MySQLPrivilege) decodeGlobalGrantsTableRow(row chunk.Row, fs []*ast.ResultField) error {
	var value dynamicPrivRecord
	for i, f := range fs {
		switch {
		case f.ColumnAsName.L == "priv":
			value.PrivilegeName = strings.ToUpper(row.GetString(i))
		case f.ColumnAsName.L == "with_grant_option":
			value.GrantOption = row.GetEnum(i).String() == "Y"
		default:
			value.assignUserOrHost(row, i, f)
		}
	}
	if p.Dynamic == nil {
		p.Dynamic = make(map[string][]dynamicPrivRecord)
	}
	p.Dynamic[value.User] = append(p.Dynamic[value.User], value)
	return nil
}

func (p *MySQLPrivilege) decodeDBTableRow(row chunk.Row, fs []*ast.ResultField) error {
	var value dbRecord
	for i, f := range fs {
		switch {
		case f.ColumnAsName.L == "db":
			value.DB = row.GetString(i)
			value.dbPatChars, value.dbPatTypes = stringutil.CompilePatternBytes(strings.ToUpper(value.DB), '\\')
		case f.Column.GetType() == mysql.TypeEnum:
			if row.GetEnum(i).String() != "Y" {
				continue
			}
			priv, ok := mysql.Col2PrivType[f.ColumnAsName.O]
			if !ok {
				return errInvalidPrivilegeType.GenWithStack("Unknown Privilege Type!")
			}
			value.Privileges |= priv
		default:
			value.assignUserOrHost(row, i, f)
		}
	}
	p.DB = append(p.DB, value)
	return nil
}

func (p *MySQLPrivilege) decodeTablesPrivTableRow(row chunk.Row, fs []*ast.ResultField) error {
	var value tablesPrivRecord
	for i, f := range fs {
		switch {
		case f.ColumnAsName.L == "db":
			value.DB = row.GetString(i)
		case f.ColumnAsName.L == "table_name":
			value.TableName = row.GetString(i)
		case f.ColumnAsName.L == "table_priv":
			value.TablePriv = decodeSetToPrivilege(row.GetSet(i))
		case f.ColumnAsName.L == "column_priv":
			value.ColumnPriv = decodeSetToPrivilege(row.GetSet(i))
		default:
			value.assignUserOrHost(row, i, f)
		}
	}
	p.TablesPriv = append(p.TablesPriv, value)
	return nil
}

func (p *MySQLPrivilege) decodeRoleEdgesTable(row chunk.Row, fs []*ast.ResultField) error {
	var fromUser, fromHost, toHost, toUser string
	for i, f := range fs {
		switch {
		case f.ColumnAsName.L == "from_host":
			fromHost = row.GetString(i)
		case f.ColumnAsName.L == "from_user":
			fromUser = row.GetString(i)
		case f.ColumnAsName.L == "to_host":
			toHost = row.GetString(i)
		case f.ColumnAsName.L == "to_user":
			toUser = row.GetString(i)
		}
	}
	fromKey := fromUser + "@" + fromHost
	toKey := toUser + "@" + toHost
	roleGraph, ok := p.RoleGraph[toKey]
	if !ok {
		roleGraph = roleGraphEdgesTable{roleList: make(map[string]*auth.RoleIdentity)}
		p.RoleGraph[toKey] = roleGraph
	}
	roleGraph.roleList[fromKey] = &auth.RoleIdentity{Username: fromUser, Hostname: fromHost}
	return nil
}

func (p *MySQLPrivilege) decodeDefaultRoleTableRow(row chunk.Row, fs []*ast.ResultField) error {
	var value defaultRoleRecord
	for i, f := range fs {
		switch {
		case f.ColumnAsName.L == "default_role_host":
			value.DefaultRoleHost = row.GetString(i)
		case f.ColumnAsName.L == "default_role_user":
			value.DefaultRoleUser = row.GetString(i)
		default:
			value.assignUserOrHost(row, i, f)
		}
	}
	p.DefaultRoles = append(p.DefaultRoles, value)
	return nil
}

func (p *MySQLPrivilege) decodeColumnsPrivTableRow(row chunk.Row, fs []*ast.ResultField) error {
	var value columnsPrivRecord
	for i, f := range fs {
		switch {
		case f.ColumnAsName.L == "db":
			value.DB = row.GetString(i)
		case f.ColumnAsName.L == "table_name":
			value.TableName = row.GetString(i)
		case f.ColumnAsName.L == "column_name":
			value.ColumnName = row.GetString(i)
		case f.ColumnAsName.L == "timestamp":
			var err error
			value.Timestamp, err = row.GetTime(i).GoTime(time.Local)
			if err != nil {
				return errors.Trace(err)
			}
		case f.ColumnAsName.L == "column_priv":
			value.ColumnPriv = decodeSetToPrivilege(row.GetSet(i))
		default:
			value.assignUserOrHost(row, i, f)
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
			logutil.BgLogger().Warn("unsupported privilege", zap.String("type", str))
			continue
		}
		ret |= priv
	}
	return ret
}

// hostMatch checks if giving IP is in IP range of hostname.
// In MySQL, the hostname of user can be set to `<IPv4>/<netmask>`
// e.g. `127.0.0.0/255.255.255.0` represent IP range from `127.0.0.1` to `127.0.0.255`,
// only IP addresses that satisfy this condition range can be login with this user.
// See https://dev.mysql.com/doc/refman/5.7/en/account-names.html
func (record *baseRecord) hostMatch(s string) bool {
	if record.hostIPNet == nil {
		if record.Host == "localhost" && net.ParseIP(s).IsLoopback() {
			return true
		}
		return false
	}
	ip := net.ParseIP(s).To4()
	if ip == nil {
		return false
	}
	return record.hostIPNet.Contains(ip)
}

func (record *baseRecord) match(user, host string) bool {
	return record.User == user && (patternMatch(host, record.patChars, record.patTypes) ||
		record.hostMatch(host))
}

func (record *baseRecord) fullyMatch(user, host string) bool {
	return record.User == user && record.Host == host
}

func (record *dbRecord) match(user, host, db string) bool {
	return record.baseRecord.match(user, host) &&
		patternMatch(strings.ToUpper(db), record.dbPatChars, record.dbPatTypes)
}

func (record *tablesPrivRecord) match(user, host, db, table string) bool {
	return record.baseRecord.match(user, host) &&
		strings.EqualFold(record.DB, db) &&
		strings.EqualFold(record.TableName, table)
}

func (record *columnsPrivRecord) match(user, host, db, table, col string) bool {
	return record.baseRecord.match(user, host) &&
		strings.EqualFold(record.DB, db) &&
		strings.EqualFold(record.TableName, table) &&
		strings.EqualFold(record.ColumnName, col)
}

// patternMatch matches "%" the same way as ".*" in regular expression, for example,
// "10.0.%" would match "10.0.1" "10.0.1.118" ...
func patternMatch(str string, patChars, patTypes []byte) bool {
	return stringutil.DoMatchBytes(str, patChars, patTypes)
}

// matchIdentity finds an identity to match a user + host
// using the correct rules according to MySQL.
func (p *MySQLPrivilege) matchIdentity(user, host string, skipNameResolve bool) *UserRecord {
	for i := 0; i < len(p.User); i++ {
		record := &p.User[i]
		if record.match(user, host) {
			return record
		}
	}

	// If skip-name resolve is not enabled, and the host is not localhost
	// we can fallback and try to resolve with all addrs that match.
	// TODO: this is imported from previous code in session.Auth(), and can be improved in future.
	if !skipNameResolve && host != variable.DefHostname {
		addrs, err := net.LookupAddr(host)
		if err != nil {
			logutil.BgLogger().Warn(
				"net.LookupAddr returned an error during auth check",
				zap.String("host", host),
				zap.Error(err),
			)
			return nil
		}
		for _, addr := range addrs {
			for i := 0; i < len(p.User); i++ {
				record := &p.User[i]
				if record.match(user, addr) {
					return record
				}
			}
		}
	}
	return nil
}

// connectionVerification verifies the username + hostname according to exact
// match from the mysql.user privilege table. call matchIdentity() first if you
// do not have an exact match yet.
func (p *MySQLPrivilege) connectionVerification(user, host string) *UserRecord {
	records, exists := p.UserMap[user]
	if exists {
		for i := 0; i < len(records); i++ {
			record := &records[i]
			if record.Host == host { // exact match
				return record
			}
		}
	}
	return nil
}

func (p *MySQLPrivilege) matchGlobalPriv(user, host string) *globalPrivRecord {
	uGlobal, exists := p.Global[user]
	if !exists {
		return nil
	}
	for i := 0; i < len(uGlobal); i++ {
		record := &uGlobal[i]
		if record.match(user, host) {
			return record
		}
	}
	return nil
}

func (p *MySQLPrivilege) matchUser(user, host string) *UserRecord {
	records, exists := p.UserMap[user]
	if exists {
		for i := 0; i < len(records); i++ {
			record := &records[i]
			if record.match(user, host) {
				return record
			}
		}
	}
	return nil
}

func (p *MySQLPrivilege) matchDB(user, host, db string) *dbRecord {
	records, exists := p.DBMap[user]
	if exists {
		for i := 0; i < len(records); i++ {
			record := &records[i]
			if record.match(user, host, db) {
				return record
			}
		}
	}
	return nil
}

func (p *MySQLPrivilege) matchTables(user, host, db, table string) *tablesPrivRecord {
	records, exists := p.TablesPrivMap[user]
	if exists {
		for i := 0; i < len(records); i++ {
			record := &records[i]
			if record.match(user, host, db, table) {
				return record
			}
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

// HasExplicitlyGrantedDynamicPrivilege checks if a user has a DYNAMIC privilege
// without accepting SUPER privilege as a fallback.
func (p *MySQLPrivilege) HasExplicitlyGrantedDynamicPrivilege(activeRoles []*auth.RoleIdentity, user, host, privName string, withGrant bool) bool {
	privName = strings.ToUpper(privName)
	roleList := p.FindAllUserEffectiveRoles(user, host, activeRoles)
	roleList = append(roleList, &auth.RoleIdentity{Username: user, Hostname: host})
	// Loop through each of the roles and return on first match
	// If grantable is required, ensure the record has the GrantOption set.
	for _, r := range roleList {
		u := r.Username
		h := r.Hostname
		for _, record := range p.Dynamic[u] {
			if record.match(u, h) {
				if withGrant && !record.GrantOption {
					continue
				}
				if record.PrivilegeName == privName {
					return true
				}
			}
		}
	}
	return false
}

// RequestDynamicVerification checks all roles for a specific DYNAMIC privilege.
func (p *MySQLPrivilege) RequestDynamicVerification(activeRoles []*auth.RoleIdentity, user, host, privName string, withGrant bool) bool {
	privName = strings.ToUpper(privName)
	if p.HasExplicitlyGrantedDynamicPrivilege(activeRoles, user, host, privName, withGrant) {
		return true
	}
	// If SEM is enabled, and the privilege is of type restricted, do not fall through
	// To using SUPER as a replacement privilege.
	if sem.IsEnabled() && sem.IsRestrictedPrivilege(privName) {
		return false
	}
	// For compatibility reasons, the SUPER privilege also has all DYNAMIC privileges granted to it (dynamic privs are a super replacement)
	// This may be changed in future, but will require a bootstrap task to assign all dynamic privileges
	// to users with SUPER, otherwise tasks such as BACKUP and ROLE_ADMIN will start to fail.
	// The visitInfo system will also need modification to support OR conditions.
	if withGrant && !p.RequestVerification(activeRoles, user, host, "", "", "", mysql.GrantPriv) {
		return false
	}
	return p.RequestVerification(activeRoles, user, host, "", "", "", mysql.SuperPriv)
}

// RequestVerification checks whether the user have sufficient privileges to do the operation.
func (p *MySQLPrivilege) RequestVerification(activeRoles []*auth.RoleIdentity, user, host, db, table, column string, priv mysql.PrivilegeType) bool {
	if priv == mysql.UsagePriv {
		return true
	}

	roleList := p.FindAllUserEffectiveRoles(user, host, activeRoles)
	roleList = append(roleList, &auth.RoleIdentity{Username: user, Hostname: host})

	var userPriv, dbPriv, tablePriv, columnPriv mysql.PrivilegeType
	for _, r := range roleList {
		userRecord := p.matchUser(r.Username, r.Hostname)
		if userRecord != nil {
			userPriv |= userRecord.Privileges
		}
	}
	if userPriv&priv > 0 {
		return true
	}

	for _, r := range roleList {
		dbRecord := p.matchDB(r.Username, r.Hostname, db)
		if dbRecord != nil {
			dbPriv |= dbRecord.Privileges
		}
	}
	if dbPriv&priv > 0 {
		return true
	}

	for _, r := range roleList {
		tableRecord := p.matchTables(r.Username, r.Hostname, db, table)
		if tableRecord != nil {
			tablePriv |= tableRecord.TablePriv
			if column != "" {
				columnPriv |= tableRecord.ColumnPriv
			}
		}
	}
	if tablePriv&priv > 0 || columnPriv&priv > 0 {
		return true
	}

	columnPriv = 0
	for _, r := range roleList {
		columnRecord := p.matchColumns(r.Username, r.Hostname, db, table, column)
		if columnRecord != nil {
			columnPriv |= columnRecord.ColumnPriv
		}
	}
	if columnPriv&priv > 0 {
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
		// For metrics_schema, `PROCESS` can also work.
		if record.Privileges&mysql.ProcessPriv > 0 && strings.EqualFold(db, util.MetricSchemaName.O) {
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
		if record.baseRecord.match(user, host) &&
			strings.EqualFold(record.DB, db) {
			if record.TablePriv != 0 || record.ColumnPriv != 0 {
				return true
			}
		}
	}

	for _, record := range p.ColumnsPriv {
		if record.baseRecord.match(user, host) &&
			strings.EqualFold(record.DB, db) {
			if record.ColumnPriv != 0 {
				return true
			}
		}
	}

	return false
}

func (p *MySQLPrivilege) showGrants(user, host string, roles []*auth.RoleIdentity) []string {
	var gs []string // nolint: prealloc
	var sortFromIdx int
	var hasGlobalGrant = false
	// Some privileges may granted from role inheritance.
	// We should find these inheritance relationship.
	allRoles := p.FindAllUserEffectiveRoles(user, host, roles)
	// Show global grants.
	var currentPriv mysql.PrivilegeType
	var userExists = false
	// Check whether user exists.
	if userList, ok := p.UserMap[user]; ok {
		for _, record := range userList {
			if record.fullyMatch(user, host) {
				userExists = true
				break
			}
		}
		if !userExists {
			return gs
		}
	}
	var g string
	for _, record := range p.User {
		if record.fullyMatch(user, host) {
			hasGlobalGrant = true
			currentPriv |= record.Privileges
		} else {
			for _, r := range allRoles {
				if record.baseRecord.match(r.Username, r.Hostname) {
					hasGlobalGrant = true
					currentPriv |= record.Privileges
				}
			}
		}
	}
	g = userPrivToString(currentPriv)
	if len(g) > 0 {
		var s string
		if (currentPriv & mysql.GrantPriv) > 0 {
			s = fmt.Sprintf(`GRANT %s ON *.* TO '%s'@'%s' WITH GRANT OPTION`, g, user, host)
		} else {
			s = fmt.Sprintf(`GRANT %s ON *.* TO '%s'@'%s'`, g, user, host)

		}
		gs = append(gs, s)
	}

	// This is a mysql convention.
	if len(gs) == 0 && hasGlobalGrant {
		var s string
		if (currentPriv & mysql.GrantPriv) > 0 {
			s = fmt.Sprintf("GRANT USAGE ON *.* TO '%s'@'%s' WITH GRANT OPTION", user, host)
		} else {
			s = fmt.Sprintf("GRANT USAGE ON *.* TO '%s'@'%s'", user, host)
		}
		gs = append(gs, s)
	}

	// Show db scope grants.
	sortFromIdx = len(gs)
	dbPrivTable := make(map[string]mysql.PrivilegeType)
	for _, record := range p.DB {
		if record.fullyMatch(user, host) {
			dbPrivTable[record.DB] |= record.Privileges
		} else {
			for _, r := range allRoles {
				if record.baseRecord.match(r.Username, r.Hostname) {
					dbPrivTable[record.DB] |= record.Privileges
				}
			}
		}
	}
	for dbName, priv := range dbPrivTable {
		g := dbPrivToString(priv)
		if len(g) > 0 {
			var s string
			if (priv & mysql.GrantPriv) > 0 {
				s = fmt.Sprintf(`GRANT %s ON %s.* TO '%s'@'%s' WITH GRANT OPTION`, g, dbName, user, host)
			} else {
				s = fmt.Sprintf(`GRANT %s ON %s.* TO '%s'@'%s'`, g, dbName, user, host)
			}
			gs = append(gs, s)
		} else if len(g) == 0 && (priv&mysql.GrantPriv) > 0 {
			// We have GRANT OPTION on the db, but no privilege granted.
			// Wo we need to print a special USAGE line.
			s := fmt.Sprintf(`GRANT USAGE ON %s.* TO '%s'@'%s' WITH GRANT OPTION`, dbName, user, host)
			gs = append(gs, s)
		}
	}
	sort.Strings(gs[sortFromIdx:])

	// Show table scope grants.
	sortFromIdx = len(gs)
	tablePrivTable := make(map[string]mysql.PrivilegeType)
	for _, record := range p.TablesPriv {
		recordKey := record.DB + "." + record.TableName
		if user == record.User && host == record.Host {
			tablePrivTable[recordKey] |= record.TablePriv
		} else {
			for _, r := range allRoles {
				if record.baseRecord.match(r.Username, r.Hostname) {
					tablePrivTable[recordKey] |= record.TablePriv
				}
			}
		}
	}
	for k, priv := range tablePrivTable {
		g := tablePrivToString(priv)
		if len(g) > 0 {
			var s string
			if (priv & mysql.GrantPriv) > 0 {
				s = fmt.Sprintf(`GRANT %s ON %s TO '%s'@'%s' WITH GRANT OPTION`, g, k, user, host)
			} else {
				s = fmt.Sprintf(`GRANT %s ON %s TO '%s'@'%s'`, g, k, user, host)
			}
			gs = append(gs, s)
		} else if len(g) == 0 && (priv&mysql.GrantPriv) > 0 {
			// We have GRANT OPTION on the table, but no privilege granted.
			// Wo we need to print a special USAGE line.
			s := fmt.Sprintf(`GRANT USAGE ON %s TO '%s'@'%s' WITH GRANT OPTION`, k, user, host)
			gs = append(gs, s)
		}
	}
	sort.Strings(gs[sortFromIdx:])

	// Show column scope grants, column and table are combined.
	// A map of "DB.Table" => Priv(col1, col2 ...)
	sortFromIdx = len(gs)
	columnPrivTable := make(map[string]privOnColumns)
	for i := range p.ColumnsPriv {
		record := p.ColumnsPriv[i]
		if !collectColumnGrant(&record, user, host, columnPrivTable) {
			for _, r := range allRoles {
				collectColumnGrant(&record, r.Username, r.Hostname, columnPrivTable)
			}
		}
	}
	for k, v := range columnPrivTable {
		privCols := privOnColumnsToString(v)
		s := fmt.Sprintf(`GRANT %s ON %s TO '%s'@'%s'`, privCols, k, user, host)
		gs = append(gs, s)
	}
	sort.Strings(gs[sortFromIdx:])

	// Show role grants.
	graphKey := user + "@" + host
	edgeTable, ok := p.RoleGraph[graphKey]
	g = ""
	if ok {
		sortedRes := make([]string, 0, 10)
		for k := range edgeTable.roleList {
			role := strings.Split(k, "@")
			roleName, roleHost := role[0], role[1]
			tmp := fmt.Sprintf("'%s'@'%s'", roleName, roleHost)
			sortedRes = append(sortedRes, tmp)
		}
		sort.Strings(sortedRes)
		for i, r := range sortedRes {
			g += r
			if i != len(sortedRes)-1 {
				g += ", "
			}
		}
		s := fmt.Sprintf(`GRANT %s TO '%s'@'%s'`, g, user, host)
		gs = append(gs, s)
	}

	// If the SHOW GRANTS is for the current user, there might be activeRoles (allRoles)
	// The convention is to merge the Dynamic privileges assigned to the user with
	// inherited dynamic privileges from those roles
	dynamicPrivsMap := make(map[string]bool) // privName, grantable
	for _, record := range p.Dynamic[user] {
		if record.fullyMatch(user, host) {
			dynamicPrivsMap[record.PrivilegeName] = record.GrantOption
		}
	}
	for _, r := range allRoles {
		for _, record := range p.Dynamic[r.Username] {
			if record.fullyMatch(r.Username, r.Hostname) {
				// If the record already exists in the map and it's grantable
				// skip doing anything, because we might inherit a non-grantable permission
				// from a role, and don't want to clobber the existing privilege.
				if grantable, ok := dynamicPrivsMap[record.PrivilegeName]; ok && grantable {
					continue
				}
				dynamicPrivsMap[record.PrivilegeName] = record.GrantOption
			}
		}
	}

	// Convert the map to a slice so it can be sorted to be deterministic and joined
	var dynamicPrivs, grantableDynamicPrivs []string
	for privName, grantable := range dynamicPrivsMap {
		if grantable {
			grantableDynamicPrivs = append(grantableDynamicPrivs, privName)
		} else {
			dynamicPrivs = append(dynamicPrivs, privName)
		}
	}

	// Merge the DYNAMIC privs into a line for non-grantable and then grantable.
	if len(dynamicPrivs) > 0 {
		sort.Strings(dynamicPrivs)
		s := fmt.Sprintf("GRANT %s ON *.* TO '%s'@'%s'", strings.Join(dynamicPrivs, ","), user, host)
		gs = append(gs, s)
	}
	if len(grantableDynamicPrivs) > 0 {
		sort.Strings(grantableDynamicPrivs)
		s := fmt.Sprintf("GRANT %s ON *.* TO '%s'@'%s' WITH GRANT OPTION", strings.Join(grantableDynamicPrivs, ","), user, host)
		gs = append(gs, s)
	}
	return gs
}

type columnStr = string
type columnStrs = []columnStr
type privOnColumns = map[mysql.PrivilegeType]columnStrs

func privOnColumnsToString(p privOnColumns) string {
	var buf bytes.Buffer
	idx := 0
	for _, priv := range mysql.AllColumnPrivs {
		v, ok := p[priv]
		if !ok || len(v) == 0 {
			continue
		}

		if idx > 0 {
			buf.WriteString(", ")
		}
		privStr := PrivToString(priv, mysql.AllColumnPrivs, mysql.Priv2Str)
		fmt.Fprintf(&buf, "%s(", privStr)
		for i, col := range v {
			if i > 0 {
				fmt.Fprintf(&buf, ", ")
			}
			buf.WriteString(col)
		}
		buf.WriteString(")")
		idx++
	}
	return buf.String()
}

func collectColumnGrant(record *columnsPrivRecord, user, host string, columnPrivTable map[string]privOnColumns) bool {
	if record.baseRecord.match(user, host) {
		recordKey := record.DB + "." + record.TableName
		privColumns, ok := columnPrivTable[recordKey]
		if !ok {
			privColumns = make(map[mysql.PrivilegeType]columnStrs)
		}

		for _, priv := range mysql.AllColumnPrivs {
			if priv&record.ColumnPriv > 0 {
				old := privColumns[priv]
				privColumns[priv] = append(old, record.ColumnName)
				columnPrivTable[recordKey] = privColumns
			}
		}
		return true
	}
	return false
}

func userPrivToString(privs mysql.PrivilegeType) string {
	if (privs & ^mysql.GrantPriv) == userTablePrivilegeMask {
		return mysql.AllPrivilegeLiteral
	}
	return PrivToString(privs, mysql.AllGlobalPrivs, mysql.Priv2Str)
}

func dbPrivToString(privs mysql.PrivilegeType) string {
	if (privs & ^mysql.GrantPriv) == dbTablePrivilegeMask {
		return mysql.AllPrivilegeLiteral
	}
	return PrivToString(privs, mysql.AllDBPrivs, mysql.Priv2SetStr)
}

func tablePrivToString(privs mysql.PrivilegeType) string {
	if (privs & ^mysql.GrantPriv) == tablePrivMask {
		return mysql.AllPrivilegeLiteral
	}
	return PrivToString(privs, mysql.AllTablePrivs, mysql.Priv2Str)
}

// PrivToString converts the privileges to string.
func PrivToString(priv mysql.PrivilegeType, allPrivs []mysql.PrivilegeType, allPrivNames map[mysql.PrivilegeType]string) string {
	pstrs := make([]string, 0, 20)
	for _, p := range allPrivs {
		if priv&p == 0 {
			continue
		}
		s := strings.ToUpper(allPrivNames[p])
		pstrs = append(pstrs, s)
	}
	return strings.Join(pstrs, ",")
}

// UserPrivilegesTable provide data for INFORMATION_SCHEMA.USERS_PRIVILEGES table.
func (p *MySQLPrivilege) UserPrivilegesTable(activeRoles []*auth.RoleIdentity, user, host string) [][]types.Datum {
	// Seeing all users requires SELECT ON * FROM mysql.*
	// The SUPER privilege (or any other dynamic privilege) doesn't help here.
	// This is verified against MySQL.
	showOtherUsers := p.RequestVerification(activeRoles, user, host, mysql.SystemDB, "", "", mysql.SelectPriv)
	var rows [][]types.Datum
	for _, u := range p.User {
		if showOtherUsers || u.match(user, host) {
			rows = appendUserPrivilegesTableRow(rows, u)
		}
	}
	for _, dynamicPrivs := range p.Dynamic {
		for _, dynamicPriv := range dynamicPrivs {
			if showOtherUsers || dynamicPriv.match(user, host) {
				rows = appendDynamicPrivRecord(rows, dynamicPriv)
			}
		}
	}
	return rows
}

func appendDynamicPrivRecord(rows [][]types.Datum, user dynamicPrivRecord) [][]types.Datum {
	isGrantable := "NO"
	if user.GrantOption {
		isGrantable = "YES"
	}
	grantee := fmt.Sprintf("'%s'@'%s'", user.User, user.Host)
	record := types.MakeDatums(grantee, "def", user.PrivilegeName, isGrantable)
	return append(rows, record)
}

func appendUserPrivilegesTableRow(rows [][]types.Datum, user UserRecord) [][]types.Datum {
	var isGrantable string
	if user.Privileges&mysql.GrantPriv > 0 {
		isGrantable = "YES"
	} else {
		isGrantable = "NO"
	}
	grantee := fmt.Sprintf("'%s'@'%s'", user.User, user.Host)
	if user.Privileges <= 1 {
		// The "USAGE" row only appears if the user has no non-DYNAMIC privileges.
		// This behavior was observed in MySQL.
		record := types.MakeDatums(grantee, "def", "USAGE", "NO")
		return append(rows, record)
	}
	for _, priv := range mysql.AllGlobalPrivs {
		if user.Privileges&priv > 0 {
			privilegeType := strings.ToUpper(mysql.Priv2Str[priv])
			// +---------------------------+---------------+-------------------------+--------------+
			// | GRANTEE                   | TABLE_CATALOG | PRIVILEGE_TYPE          | IS_GRANTABLE |
			// +---------------------------+---------------+-------------------------+--------------+
			// | 'root'@'localhost'        | def           | SELECT                  | YES          |
			record := types.MakeDatums(grantee, "def", privilegeType, isGrantable)
			rows = append(rows, record)
		}
	}
	return rows
}

func (p *MySQLPrivilege) getDefaultRoles(user, host string) []*auth.RoleIdentity {
	ret := make([]*auth.RoleIdentity, 0)
	for _, r := range p.DefaultRoles {
		if r.match(user, host) {
			ret = append(ret, &auth.RoleIdentity{Username: r.DefaultRoleUser, Hostname: r.DefaultRoleHost})
		}
	}
	return ret
}

func (p *MySQLPrivilege) getAllRoles(user, host string) []*auth.RoleIdentity {
	key := user + "@" + host
	edgeTable, ok := p.RoleGraph[key]
	ret := make([]*auth.RoleIdentity, 0, len(edgeTable.roleList))
	if ok {
		for _, r := range edgeTable.roleList {
			ret = append(ret, r)
		}
	}
	return ret
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
		return err
	}

	h.priv.Store(&priv)
	return nil
}
