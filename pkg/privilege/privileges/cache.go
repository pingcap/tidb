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
	"cmp"
	"context"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	sem "github.com/pingcap/tidb/pkg/util/sem/compat"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"go.uber.org/zap"
)

var (
	userTablePrivilegeMask = computePrivMask(mysql.AllGlobalPrivs)
	dbTablePrivilegeMask   = computePrivMask(mysql.AllDBPrivs)
	tablePrivMask          = computePrivMask(mysql.AllTablePrivs)
)

const globalDBVisible = mysql.CreatePriv | mysql.SelectPriv | mysql.InsertPriv | mysql.UpdatePriv | mysql.DeletePriv | mysql.ShowDBPriv | mysql.DropPriv | mysql.AlterPriv | mysql.IndexPriv | mysql.CreateViewPriv | mysql.ShowViewPriv | mysql.GrantPriv | mysql.TriggerPriv | mysql.ReferencesPriv | mysql.ExecutePriv | mysql.CreateTMPTablePriv

const (
	sqlLoadRoleGraph        = "SELECT HIGH_PRIORITY FROM_USER, FROM_HOST, TO_USER, TO_HOST FROM mysql.role_edges"
	sqlLoadGlobalPrivTable  = "SELECT HIGH_PRIORITY Host,User,Priv FROM mysql.global_priv"
	sqlLoadDBTable          = "SELECT HIGH_PRIORITY Host,DB,User,Select_priv,Insert_priv,Update_priv,Delete_priv,Create_priv,Drop_priv,Grant_priv,Index_priv,References_priv,Lock_tables_priv,Create_tmp_table_priv,Event_priv,Create_routine_priv,Alter_routine_priv,Alter_priv,Execute_priv,Create_view_priv,Show_view_priv,Trigger_priv FROM mysql.db"
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
	Account_locked,Plugin,Token_issuer,User_attributes,password_expired,password_last_changed,password_lifetime,max_user_connections FROM mysql.user`
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

// MetadataInfo is the User_attributes->>"$.metadata".
type MetadataInfo struct {
	Email string
}

// UserAttributesInfo is the 'User_attributes' in privilege cache.
type UserAttributesInfo struct {
	MetadataInfo
	PasswordLocking
}

// UserRecord is used to represent a user record in privilege cache.
type UserRecord struct {
	baseRecord
	UserAttributesInfo

	AuthenticationString string
	Privileges           mysql.PrivilegeType
	AccountLocked        bool // A role record when this field is true
	AuthPlugin           string
	AuthTokenIssuer      string
	PasswordExpired      bool
	PasswordLastChanged  time.Time
	PasswordLifeTime     int64
	MaxUserConnections   int64
	ResourceGroup        string
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
	roleList map[auth.RoleIdentity]*auth.RoleIdentity
}

// Find method is used to find role from table
func (g roleGraphEdgesTable) Find(user, host string) bool {
	if g.roleList == nil {
		return false
	}
	key := auth.RoleIdentity{
		Username: user,
		Hostname: host,
	}
	_, ok := g.roleList[key]
	if !ok && key.Hostname == "" {
		key.Hostname = "%"
		_, ok = g.roleList[key]
	}
	return ok
}

type itemUser struct {
	username string
	data     []UserRecord
}

func compareItemUser(a, b itemUser) bool {
	return a.username < b.username
}

type itemDB struct {
	username string
	data     []dbRecord
}

func compareItemDB(a, b itemDB) bool {
	return a.username < b.username
}

type itemTablesPriv struct {
	username string
	data     []tablesPrivRecord
}

func compareItemTablesPriv(a, b itemTablesPriv) bool {
	return a.username < b.username
}

type itemColumnsPriv struct {
	username string
	data     []columnsPrivRecord
}

func compareItemColumnsPriv(a, b itemColumnsPriv) bool {
	return a.username < b.username
}

type itemDefaultRole struct {
	username string
	data     []defaultRoleRecord
}

func compareItemDefaultRole(a, b itemDefaultRole) bool {
	return a.username < b.username
}

type itemGlobalPriv struct {
	username string
	data     []globalPrivRecord
}

func compareItemGlobalPriv(a, b itemGlobalPriv) bool {
	return a.username < b.username
}

type itemDynamicPriv struct {
	username string
	data     []dynamicPrivRecord
}

func compareItemDynamicPriv(a, b itemDynamicPriv) bool {
	return a.username < b.username
}

type bTree[T any] struct {
	*btree.BTreeG[T]
	sync.Mutex
}

// Clone provides the concurrent-safe operation by wraping the original Clone.
func (bt *bTree[T]) Clone() *btree.BTreeG[T] {
	bt.Lock()
	defer bt.Unlock()
	return bt.BTreeG.Clone()
}

// MySQLPrivilege is the in-memory cache of mysql privilege tables.
type MySQLPrivilege struct {
	globalVars variable.GlobalVarAccessor

	// In MySQL, a user identity consists of a user + host.
	// Either portion of user or host can contain wildcards,
	// requiring the privileges system to use a list-like
	// structure instead of a hash.

	// TiDB contains a sensible behavior difference from MySQL,
	// which is that usernames can not contain wildcards.
	// This means that DB-records are organized in both a
	// slice (p.DB) and a Map (p.DBMap).

	user         bTree[itemUser]
	db           bTree[itemDB]
	tablesPriv   bTree[itemTablesPriv]
	columnsPriv  bTree[itemColumnsPriv]
	defaultRoles bTree[itemDefaultRole]

	globalPriv  bTree[itemGlobalPriv]
	dynamicPriv bTree[itemDynamicPriv]
	roleGraph   map[auth.RoleIdentity]roleGraphEdgesTable
}

func newMySQLPrivilege() *MySQLPrivilege {
	var p MySQLPrivilege
	p.user = bTree[itemUser]{BTreeG: btree.NewG(8, compareItemUser)}
	p.db = bTree[itemDB]{BTreeG: btree.NewG(8, compareItemDB)}
	p.tablesPriv = bTree[itemTablesPriv]{BTreeG: btree.NewG(8, compareItemTablesPriv)}
	p.columnsPriv = bTree[itemColumnsPriv]{BTreeG: btree.NewG(8, compareItemColumnsPriv)}
	p.defaultRoles = bTree[itemDefaultRole]{BTreeG: btree.NewG(8, compareItemDefaultRole)}
	p.globalPriv = bTree[itemGlobalPriv]{BTreeG: btree.NewG(8, compareItemGlobalPriv)}
	p.dynamicPriv = bTree[itemDynamicPriv]{BTreeG: btree.NewG(8, compareItemDynamicPriv)}
	return &p
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
			key := *role
			if edgeTable, ok := p.roleGraph[key]; ok {
				for _, v := range edgeTable.roleList {
					if _, ok := visited[v.String()]; !ok {
						queue = append(queue, v)
					}
				}
			}
		}
		head++
	}
	return ret
}

// FindRole is used to detect whether there is edges between users and roles.
func (p *MySQLPrivilege) FindRole(user string, host string, role *auth.RoleIdentity) bool {
	rec := p.matchUser(user, host)
	r := p.matchUser(role.Username, role.Hostname)
	if rec != nil && r != nil {
		key := auth.RoleIdentity{
			Username: rec.User,
			Hostname: rec.Host,
		}
		return p.roleGraph[key].Find(role.Username, role.Hostname)
	}
	return false
}

func findRole(ctx context.Context, h *Handle, user string, host string, role *auth.RoleIdentity) bool {
	terror.Log(h.ensureActiveUser(ctx, user))
	terror.Log(h.ensureActiveUser(ctx, role.Username))
	mysqlPrivilege := h.Get()
	return mysqlPrivilege.FindRole(user, host, role)
}

// LoadAll loads the tables from database to memory.
func (p *MySQLPrivilege) LoadAll(ctx sqlexec.SQLExecutor) error {
	err := p.LoadUserTable(ctx)
	if err != nil {
		logutil.BgLogger().Warn("load mysql.user fail", zap.Error(err))
		return errLoadPrivilege.FastGen("mysql.user")
	}
	if l := p.user.Len(); l > 1024 {
		logutil.BgLogger().Warn("load all called and user list is long, suggest enabling @@global.tidb_accelerate_user_creation_update", zap.Int("len", l))
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

func findUserAndAllRoles(userList []string, roleGraph map[auth.RoleIdentity]roleGraphEdgesTable) map[string]struct{} {
	// Including the user list and also their roles
	all := make(map[string]struct{}, len(userList))
	queue := make([]string, 0, len(userList))

	// Initialize the queue with the initial user list
	for _, user := range userList {
		all[user] = struct{}{}
		queue = append(queue, user)
	}

	// Process the queue using BFS
	for len(queue) > 0 {
		user := queue[0]
		queue = queue[1:]
		for userHost, value := range roleGraph {
			if userHost.Username == user {
				for _, role := range value.roleList {
					if _, ok := all[role.Username]; !ok {
						all[role.Username] = struct{}{}
						queue = append(queue, role.Username)
					}
				}
			}
		}
	}
	return all
}

func (p *MySQLPrivilege) loadSomeUsers(ctx sqlexec.SQLExecutor, userList map[string]struct{}) error {
	err := loadTable(ctx, addUserFilterCondition(sqlLoadUserTable, userList), p.decodeUserTableRow(userList))
	if err != nil {
		return errors.Trace(err)
	}

	err = loadTable(ctx, addUserFilterCondition(sqlLoadGlobalPrivTable, userList), p.decodeGlobalPrivTableRow(userList))
	if err != nil {
		return errors.Trace(err)
	}

	err = loadTable(ctx, addUserFilterCondition(sqlLoadGlobalGrantsTable, userList), p.decodeGlobalGrantsTableRow(userList))
	if err != nil {
		return errors.Trace(err)
	}

	err = loadTable(ctx, addUserFilterCondition(sqlLoadDBTable, userList), p.decodeDBTableRow(userList))
	if err != nil {
		return errors.Trace(err)
	}

	err = loadTable(ctx, addUserFilterCondition(sqlLoadTablePrivTable, userList), p.decodeTablesPrivTableRow(userList))
	if err != nil {
		return errors.Trace(err)
	}

	err = loadTable(ctx, addUserFilterCondition(sqlLoadDefaultRoles, userList), p.decodeDefaultRoleTableRow(userList))
	if err != nil {
		return errors.Trace(err)
	}

	err = loadTable(ctx, addUserFilterCondition(sqlLoadColumnsPrivTable, userList), p.decodeColumnsPrivTableRow(userList))
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// merge construct a new MySQLPrivilege by merging the data of the two objects.
func (p *MySQLPrivilege) merge(diff *MySQLPrivilege, userList map[string]struct{}) *MySQLPrivilege {
	ret := newMySQLPrivilege()
	user := p.user.Clone()
	for u := range userList {
		itm, ok := diff.user.Get(itemUser{username: u})
		if !ok {
			user.Delete(itemUser{username: u})
		} else {
			slices.SortFunc(itm.data, compareUserRecord)
			user.ReplaceOrInsert(itm)
		}
	}
	ret.user.BTreeG = user

	db := p.db.Clone()
	for u := range userList {
		itm, ok := diff.db.Get(itemDB{username: u})
		if !ok {
			db.Delete(itemDB{username: u})
		} else {
			slices.SortFunc(itm.data, compareDBRecord)
			db.ReplaceOrInsert(itm)
		}
	}
	ret.db.BTreeG = db

	tablesPriv := p.tablesPriv.Clone()
	for u := range userList {
		itm, ok := diff.tablesPriv.Get(itemTablesPriv{username: u})
		if !ok {
			tablesPriv.Delete(itemTablesPriv{username: u})
		} else {
			slices.SortFunc(itm.data, compareTablesPrivRecord)
			tablesPriv.ReplaceOrInsert(itm)
		}
	}
	ret.tablesPriv.BTreeG = tablesPriv

	columnsPriv := p.columnsPriv.Clone()
	for u := range userList {
		itm, ok := diff.columnsPriv.Get(itemColumnsPriv{username: u})
		if !ok {
			columnsPriv.Delete(itemColumnsPriv{username: u})
		} else {
			slices.SortFunc(itm.data, compareColumnsPrivRecord)
			columnsPriv.ReplaceOrInsert(itm)
		}
	}
	ret.columnsPriv.BTreeG = columnsPriv

	defaultRoles := p.defaultRoles.Clone()
	for u := range userList {
		itm, ok := diff.defaultRoles.Get(itemDefaultRole{username: u})
		if !ok {
			defaultRoles.Delete(itemDefaultRole{username: u})
		} else {
			slices.SortFunc(itm.data, compareDefaultRoleRecord)
			defaultRoles.ReplaceOrInsert(itm)
		}
	}
	ret.defaultRoles.BTreeG = defaultRoles

	dynamicPriv := p.dynamicPriv.Clone()
	for u := range userList {
		itm, ok := diff.dynamicPriv.Get(itemDynamicPriv{username: u})
		if !ok {
			dynamicPriv.Delete(itemDynamicPriv{username: u})
		} else {
			slices.SortFunc(itm.data, compareDynamicPrivRecord)
			dynamicPriv.ReplaceOrInsert(itm)
		}
	}
	ret.dynamicPriv.BTreeG = dynamicPriv

	globalPriv := p.globalPriv.Clone()
	for u := range userList {
		itm, ok := diff.globalPriv.Get(itemGlobalPriv{username: u})
		if !ok {
			globalPriv.Delete(itemGlobalPriv{username: u})
		} else {
			slices.SortFunc(itm.data, compareGlobalPrivRecord)
			globalPriv.ReplaceOrInsert(itm)
		}
	}
	ret.globalPriv.BTreeG = globalPriv

	ret.roleGraph = diff.roleGraph
	return ret
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
func (p *MySQLPrivilege) LoadRoleGraph(exec sqlexec.SQLExecutor) error {
	p.roleGraph = make(map[auth.RoleIdentity]roleGraphEdgesTable)
	err := loadTable(exec, sqlLoadRoleGraph, p.decodeRoleEdgesTable)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// LoadUserTable loads the mysql.user table from database.
func (p *MySQLPrivilege) LoadUserTable(exec sqlexec.SQLExecutor) error {
	err := loadTable(exec, sqlLoadUserTable, p.decodeUserTableRow(nil))
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

func compareBaseRecord(x, y *baseRecord) int {
	// Compare two item by user's host first.
	c1 := compareHost(x.Host, y.Host)
	if c1 != 0 {
		return c1
	}
	// Then, compare item by user's name value.
	return cmp.Compare(x.User, y.User)
}

func compareUserRecord(x, y UserRecord) int {
	return compareBaseRecord(&x.baseRecord, &y.baseRecord)
}

func compareDefaultRoleRecord(x, y defaultRoleRecord) int {
	return compareBaseRecord(&x.baseRecord, &y.baseRecord)
}

func compareGlobalPrivRecord(x, y globalPrivRecord) int {
	return compareBaseRecord(&x.baseRecord, &y.baseRecord)
}

func compareDynamicPrivRecord(x, y dynamicPrivRecord) int {
	return compareBaseRecord(&x.baseRecord, &y.baseRecord)
}

func compareColumnsPrivRecord(x, y columnsPrivRecord) int {
	cmp := compareBaseRecord(&x.baseRecord, &y.baseRecord)
	if cmp != 0 {
		return cmp
	}
	switch {
	case x.DB > y.DB:
		return 1
	case x.DB < y.DB:
		return -1
	}
	switch {
	case x.TableName > y.TableName:
		return 1
	case x.TableName < y.TableName:
		return -1
	}
	switch {
	case x.ColumnName > y.ColumnName:
		return 1
	case x.ColumnName < y.ColumnName:
		return -1
	}
	return 0
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

// SortUserTable sorts p.User in the MySQLPrivilege struct.
func (p *MySQLPrivilege) SortUserTable() {
	p.user.Ascend(func(itm itemUser) bool {
		slices.SortFunc(itm.data, compareUserRecord)
		return true
	})
}

// LoadGlobalPrivTable loads the mysql.global_priv table from database.
func (p *MySQLPrivilege) LoadGlobalPrivTable(exec sqlexec.SQLExecutor) error {
	if err := loadTable(exec, sqlLoadGlobalPrivTable, p.decodeGlobalPrivTableRow(nil)); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// LoadGlobalGrantsTable loads the mysql.global_priv table from database.
func (p *MySQLPrivilege) LoadGlobalGrantsTable(exec sqlexec.SQLExecutor) error {
	if err := loadTable(exec, sqlLoadGlobalGrantsTable, p.decodeGlobalGrantsTableRow(nil)); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// LoadDBTable loads the mysql.db table from database.
func (p *MySQLPrivilege) LoadDBTable(exec sqlexec.SQLExecutor) error {
	err := loadTable(exec, sqlLoadDBTable, p.decodeDBTableRow(nil))
	if err != nil {
		return err
	}
	p.db.Ascend(func(itm itemDB) bool {
		slices.SortFunc(itm.data, compareDBRecord)
		return true
	})
	return nil
}

func compareDBRecord(x, y dbRecord) int {
	ret := compareBaseRecord(&x.baseRecord, &y.baseRecord)
	if ret != 0 {
		return ret
	}

	return strings.Compare(x.DB, y.DB)
}

func compareTablesPrivRecord(x, y tablesPrivRecord) int {
	ret := compareBaseRecord(&x.baseRecord, &y.baseRecord)
	if ret != 0 {
		return ret
	}

	ret = strings.Compare(x.DB, y.DB)
	if ret != 0 {
		return ret
	}

	return strings.Compare(x.TableName, y.TableName)
}

// LoadTablesPrivTable loads the mysql.tables_priv table from database.
func (p *MySQLPrivilege) LoadTablesPrivTable(exec sqlexec.SQLExecutor) error {
	err := loadTable(exec, sqlLoadTablePrivTable, p.decodeTablesPrivTableRow(nil))
	if err != nil {
		return err
	}
	return nil
}

// LoadColumnsPrivTable loads the mysql.columns_priv table from database.
func (p *MySQLPrivilege) LoadColumnsPrivTable(exec sqlexec.SQLExecutor) error {
	return loadTable(exec, sqlLoadColumnsPrivTable, p.decodeColumnsPrivTableRow(nil))
}

// LoadDefaultRoles loads the mysql.columns_priv table from database.
func (p *MySQLPrivilege) LoadDefaultRoles(exec sqlexec.SQLExecutor) error {
	return loadTable(exec, sqlLoadDefaultRoles, p.decodeDefaultRoleTableRow(nil))
}

func addUserFilterCondition(sql string, userList map[string]struct{}) string {
	if len(userList) == 0 || len(userList) > 1024 {
		return sql
	}
	var b strings.Builder
	b.WriteString(sql)
	b.WriteString(" WHERE ")
	first := true
	for user := range userList {
		if !first {
			b.WriteString(" OR ")
		} else {
			first = false
		}
		fmt.Fprintf(&b, "USER = '%s'", sqlescape.EscapeString(user))
	}
	return b.String()
}

// loadTable loads the table data by executing the sql and decoding the result data.
// NOTE: the chunk Row passed to decodeTableRow function is reused, so decodeTableRow should clone when necessary.
func loadTable(exec sqlexec.SQLExecutor, sql string,
	decodeTableRow func(chunk.Row, []*resolve.ResultField) error) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	// Do not use sctx.ExecRestrictedSQL() here deliberately.
	// The result set can be extremely large, so this streaming API is important to
	// reduce memory cost.
	rs, err := exec.ExecuteInternal(ctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	defer terror.Call(rs.Close)
	fs := rs.Fields()
	req := rs.NewChunk(nil)
	for {
		err = rs.Next(ctx, req)
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
		req.GrowAndReset(1024)
	}
}

// parseHostIPNet parses an IPv4 address and its subnet mask (e.g. `127.0.0.0/255.255.255.0`),
// return the `IPNet` struct which represent the IP range info (e.g. `127.0.0.1 ~ 127.0.0.255`).
// `IPNet` is used to check if a giving IP (e.g. `127.0.0.1`) is in its IP range by call `IPNet.Contains(ip)`.

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
	return stringutil.DoMatchBinary(str, patChars, patTypes)
}

// matchIdentity finds an identity to match a user + host
// using the correct rules according to MySQL.
func (p *MySQLPrivilege) matchIdentity(user, host string, skipNameResolve bool) *UserRecord {
	item, ok := p.user.Get(itemUser{username: user})
	if !ok {
		return nil
	}

	for i := range item.data {
		record := &item.data[i]
		if record.match(user, host) {
			return record
		}
	}

	// If skip-name resolve is not enabled, and the host is not localhost
	// we can fallback and try to resolve with all addrs that match.
	// TODO: this is imported from previous code in session.Auth(), and can be improved in future.
	if !skipNameResolve && host != vardef.DefHostname {
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
			for i := range item.data {
				record := &item.data[i]
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
	records, exists := p.user.Get(itemUser{username: user})
	if exists {
		for i := range records.data {
			record := &records.data[i]
			if record.Host == host { // exact match
				return record
			}
		}
	}
	return nil
}

func (p *MySQLPrivilege) matchGlobalPriv(user, host string) *globalPrivRecord {
	item, exists := p.globalPriv.Get(itemGlobalPriv{username: user})
	if !exists {
		return nil
	}
	uGlobal := item.data
	for i := range uGlobal {
		record := &uGlobal[i]
		if record.match(user, host) {
			return record
		}
	}
	return nil
}

func (p *MySQLPrivilege) matchUser(user, host string) *UserRecord {
	item, exists := p.user.Get(itemUser{username: user})
	if exists {
		records := item.data
		for i := range records {
			record := &records[i]
			if record.match(user, host) {
				return record
			}
		}
	}
	return nil
}

func (p *MySQLPrivilege) matchDB(user, host, db string) *dbRecord {
	item, exists := p.db.Get(itemDB{username: user})
	if exists {
		records := item.data
		for i := range records {
			record := &records[i]
			if record.match(user, host, db) {
				return record
			}
		}
	}
	return nil
}

func (p *MySQLPrivilege) matchTables(user, host, db, table string) *tablesPrivRecord {
	item, exists := p.tablesPriv.Get(itemTablesPriv{username: user})
	if exists {
		records := item.data
		for i := range records {
			record := &records[i]
			if record.match(user, host, db, table) {
				return record
			}
		}
	}
	return nil
}

func (p *MySQLPrivilege) matchColumns(user, host, db, table, column string) *columnsPrivRecord {
	item, exists := p.columnsPriv.Get(itemColumnsPriv{username: user})
	if exists {
		for i := range item.data {
			record := &item.data[i]
			if record.match(user, host, db, table, column) {
				return record
			}
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
		item, exists := p.dynamicPriv.Get(itemDynamicPriv{username: u})
		if exists {
			for _, record := range item.data {
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
	}
	return false
}

// RequestDynamicVerification checks all roles for a specific DYNAMIC privilege.
func (p *MySQLPrivilege) RequestDynamicVerification(activeRoles []*auth.RoleIdentity, user, host string, privName string, withGrant bool) bool {
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


