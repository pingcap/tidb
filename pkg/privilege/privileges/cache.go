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
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/btree"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sem"
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

const globalDBVisible = mysql.CreatePriv | mysql.SelectPriv | mysql.InsertPriv | mysql.UpdatePriv | mysql.DeletePriv | mysql.ShowDBPriv | mysql.DropPriv | mysql.AlterPriv | mysql.IndexPriv | mysql.CreateViewPriv | mysql.ShowViewPriv | mysql.GrantPriv | mysql.TriggerPriv | mysql.ReferencesPriv | mysql.ExecutePriv

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
	Account_locked,Plugin,Token_issuer,User_attributes,password_expired,password_last_changed,password_lifetime FROM mysql.user`
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
	if host == "" {
		host = "%"
	}
	key := auth.RoleIdentity{user, host}
	if g.roleList == nil {
		return false
	}
	_, ok := g.roleList[key]
	return ok
}

type immutable struct {
	user         []UserRecord
	db           []dbRecord
	tablesPriv   []tablesPrivRecord
	columnsPriv  []columnsPrivRecord
	defaultRoles []defaultRoleRecord

	globalPriv  []globalPrivRecord
	dynamicPriv []dynamicPrivRecord
	roleGraph   map[string]roleGraphEdgesTable
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
			key := auth.RoleIdentity{role.Username, role.Hostname}
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
		key := auth.RoleIdentity{rec.User, rec.Host}
		return p.roleGraph[key].Find(role.Username, role.Hostname)
	}
	return false
}

func findRole(h *Handle, user string, host string, role *auth.RoleIdentity) bool {
	terror.Log(h.ensureActiveUser(user))
	terror.Log(h.ensureActiveUser(role.Username))
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
		// NOTE: decodeTableRow decodes data from a chunk Row, that is a shallow copy.
		// The result will reference memory in the chunk, so the chunk must not be reused
		// here, otherwise some werid bug will happen!
		req = chunk.Renew(req, 1024)
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

func (record *baseRecord) assignUserOrHost(row chunk.Row, i int, f *resolve.ResultField) {
	switch f.ColumnAsName.L {
	case "user":
		record.User = strings.Clone(row.GetString(i))
	case "host":
		record.Host = strings.Clone(row.GetString(i))
		record.patChars, record.patTypes = stringutil.CompilePatternBinary(record.Host, '\\')
		record.hostIPNet = parseHostIPNet(record.Host)
	}
}

func (p *MySQLPrivilege) decodeUserTableRow(userList map[string]struct{}) func(chunk.Row, []*resolve.ResultField) error {
	return func(row chunk.Row, fs []*resolve.ResultField) error {
		var value UserRecord
		defaultAuthPlugin := ""
		if p.globalVars != nil {
			val, err := p.globalVars.GetGlobalSysVar(variable.DefaultAuthPlugin)
			if err == nil {
				defaultAuthPlugin = val
			}
		}
		if defaultAuthPlugin == "" {
			defaultAuthPlugin = mysql.AuthNativePassword
		}
		for i, f := range fs {
			switch {
			case f.ColumnAsName.L == "authentication_string":
				value.AuthenticationString = strings.Clone(row.GetString(i))
			case f.ColumnAsName.L == "account_locked":
				if row.GetEnum(i).String() == "Y" {
					value.AccountLocked = true
				}
			case f.ColumnAsName.L == "plugin":
				if row.GetString(i) != "" {
					value.AuthPlugin = strings.Clone(row.GetString(i))
				} else {
					value.AuthPlugin = defaultAuthPlugin
				}
			case f.ColumnAsName.L == "token_issuer":
				value.AuthTokenIssuer = strings.Clone(row.GetString(i))
			case f.ColumnAsName.L == "user_attributes":
				if row.IsNull(i) {
					continue
				}
				bj := row.GetJSON(i)
				pathExpr, err := types.ParseJSONPathExpr("$.metadata.email")
				if err != nil {
					return err
				}
				if emailBJ, found := bj.Extract([]types.JSONPathExpression{pathExpr}); found {
					email, err := emailBJ.Unquote()
					if err != nil {
						return err
					}
					value.Email = strings.Clone(email)
				}
				pathExpr, err = types.ParseJSONPathExpr("$.resource_group")
				if err != nil {
					return err
				}
				if resourceGroup, found := bj.Extract([]types.JSONPathExpression{pathExpr}); found {
					resourceGroup, err := resourceGroup.Unquote()
					if err != nil {
						return err
					}
					value.ResourceGroup = strings.Clone(resourceGroup)
				}
				passwordLocking := PasswordLocking{}
				if err := passwordLocking.ParseJSON(bj); err != nil {
					return err
				}
				value.FailedLoginAttempts = passwordLocking.FailedLoginAttempts
				value.PasswordLockTimeDays = passwordLocking.PasswordLockTimeDays
				value.FailedLoginCount = passwordLocking.FailedLoginCount
				value.AutoLockedLastChanged = passwordLocking.AutoLockedLastChanged
				value.AutoAccountLocked = passwordLocking.AutoAccountLocked
			case f.ColumnAsName.L == "password_expired":
				if row.GetEnum(i).String() == "Y" {
					value.PasswordExpired = true
				}
			case f.ColumnAsName.L == "password_last_changed":
				t := row.GetTime(i)
				gotime, err := t.GoTime(time.Local)
				if err != nil {
					return err
				}
				value.PasswordLastChanged = gotime
			case f.ColumnAsName.L == "password_lifetime":
				if row.IsNull(i) {
					value.PasswordLifeTime = -1
					continue
				}
				value.PasswordLifeTime = row.GetInt64(i)
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
		old, ok := p.user.Get(itemUser{username: value.User})
		if !ok {
			old.username = value.User
		}
		old.data = append(old.data, value)
		p.user.ReplaceOrInsert(old)
		return nil
	}
}

func (p *MySQLPrivilege) decodeGlobalPrivTableRow(userList map[string]struct{}) func(chunk.Row, []*resolve.ResultField) error {
	return func(row chunk.Row, fs []*resolve.ResultField) error {
		var value globalPrivRecord
		for i, f := range fs {
			if f.ColumnAsName.L == "priv" {
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
						value.Priv.SSLCipher = strings.Clone(privValue.SSLCipher)
						value.Priv.X509Issuer = strings.Clone(privValue.X509Issuer)
						value.Priv.X509Subject = strings.Clone(privValue.X509Subject)
						value.Priv.SAN = strings.Clone(privValue.SAN)
						if len(value.Priv.SAN) > 0 {
							value.Priv.SANs, err = util.ParseAndCheckSAN(value.Priv.SAN)
							if err != nil {
								value.Broken = true
							}
						}
					}
				}
			} else {
				value.assignUserOrHost(row, i, f)
			}
		}
		if userList != nil {
			if _, ok := userList[value.User]; !ok {
				return nil
			}
		}

		old, ok := p.globalPriv.Get(itemGlobalPriv{username: value.User})
		if !ok {
			old.username = value.User
		}
		old.data = append(old.data, value)
		p.globalPriv.ReplaceOrInsert(old)
		return nil
	}
}

func (p *MySQLPrivilege) decodeGlobalGrantsTableRow(userList map[string]struct{}) func(chunk.Row, []*resolve.ResultField) error {
	return func(row chunk.Row, fs []*resolve.ResultField) error {
		var value dynamicPrivRecord
		for i, f := range fs {
			switch f.ColumnAsName.L {
			case "priv":
				value.PrivilegeName = strings.ToUpper(row.GetString(i))
			case "with_grant_option":
				value.GrantOption = row.GetEnum(i).String() == "Y"
			default:
				value.assignUserOrHost(row, i, f)
			}
		}
		if userList != nil {
			if _, ok := userList[value.User]; !ok {
				return nil
			}
		}

		old, ok := p.dynamicPriv.Get(itemDynamicPriv{username: value.User})
		if !ok {
			old.username = value.User
		}
		old.data = append(old.data, value)
		p.dynamicPriv.ReplaceOrInsert(old)
		return nil
	}
}

func (p *MySQLPrivilege) decodeDBTableRow(userList map[string]struct{}) func(chunk.Row, []*resolve.ResultField) error {
	return func(row chunk.Row, fs []*resolve.ResultField) error {
		var value dbRecord
		for i, f := range fs {
			switch {
			case f.ColumnAsName.L == "db":
				value.DB = row.GetString(i)
				value.dbPatChars, value.dbPatTypes = stringutil.CompilePatternBinary(strings.ToUpper(value.DB), '\\')
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
		if userList != nil {
			if _, ok := userList[value.User]; !ok {
				return nil
			}
		}

		old, ok := p.db.Get(itemDB{username: value.User})
		if !ok {
			old.username = value.User
		}
		old.data = append(old.data, value)
		p.db.ReplaceOrInsert(old)
		return nil
	}
}

func (p *MySQLPrivilege) decodeTablesPrivTableRow(userList map[string]struct{}) func(chunk.Row, []*resolve.ResultField) error {
	return func(row chunk.Row, fs []*resolve.ResultField) error {
		var value tablesPrivRecord
		for i, f := range fs {
			switch f.ColumnAsName.L {
			case "db":
				value.DB = row.GetString(i)
			case "table_name":
				value.TableName = row.GetString(i)
			case "table_priv":
				value.TablePriv = decodeSetToPrivilege(row.GetSet(i))
			case "column_priv":
				value.ColumnPriv = decodeSetToPrivilege(row.GetSet(i))
			default:
				value.assignUserOrHost(row, i, f)
			}
		}
		if userList != nil {
			if _, ok := userList[value.User]; !ok {
				return nil
			}
		}

		old, ok := p.tablesPriv.Get(itemTablesPriv{username: value.User})
		if !ok {
			old.username = value.User
		}
		old.data = append(old.data, value)
		p.tablesPriv.ReplaceOrInsert(old)
		return nil
	}
}

func (p *MySQLPrivilege) decodeRoleEdgesTable(row chunk.Row, fs []*resolve.ResultField) error {
	var fromUser, fromHost, toHost, toUser string
	for i, f := range fs {
		switch f.ColumnAsName.L {
		case "from_host":
			fromHost = row.GetString(i)
		case "from_user":
			fromUser = row.GetString(i)
		case "to_host":
			toHost = row.GetString(i)
		case "to_user":
			toUser = row.GetString(i)
		}
	}
	fromKey := auth.RoleIdentity{fromUser, fromHost}
	toKey := auth.RoleIdentity{toUser, toHost}
	roleGraph, ok := p.roleGraph[toKey]
	if !ok {
		roleGraph = roleGraphEdgesTable{roleList: make(map[auth.RoleIdentity]*auth.RoleIdentity)}
		p.roleGraph[toKey] = roleGraph
	}
	roleGraph.roleList[fromKey] = &auth.RoleIdentity{Username: fromUser, Hostname: fromHost}
	return nil
}

func (p *MySQLPrivilege) decodeDefaultRoleTableRow(userList map[string]struct{}) func(chunk.Row, []*resolve.ResultField) error {
	return func(row chunk.Row, fs []*resolve.ResultField) error {
		var value defaultRoleRecord
		for i, f := range fs {
			switch f.ColumnAsName.L {
			case "default_role_host":
				value.DefaultRoleHost = row.GetString(i)
			case "default_role_user":
				value.DefaultRoleUser = row.GetString(i)
			default:
				value.assignUserOrHost(row, i, f)
			}
		}
		if userList != nil {
			if _, ok := userList[value.User]; !ok {
				return nil
			}
		}

		old, ok := p.defaultRoles.Get(itemDefaultRole{username: value.User})
		if !ok {
			old.username = value.User
		}
		old.data = append(old.data, value)
		p.defaultRoles.ReplaceOrInsert(old)
		return nil
	}
}

func (p *MySQLPrivilege) decodeColumnsPrivTableRow(userList map[string]struct{}) func(chunk.Row, []*resolve.ResultField) error {
	return func(row chunk.Row, fs []*resolve.ResultField) error {
		var value columnsPrivRecord
		for i, f := range fs {
			switch f.ColumnAsName.L {
			case "db":
				value.DB = row.GetString(i)
			case "table_name":
				value.TableName = row.GetString(i)
			case "column_name":
				value.ColumnName = row.GetString(i)
			case "timestamp":
				var err error
				value.Timestamp, err = row.GetTime(i).GoTime(time.Local)
				if err != nil {
					return errors.Trace(err)
				}
			case "column_priv":
				value.ColumnPriv = decodeSetToPrivilege(row.GetSet(i))
			default:
				value.assignUserOrHost(row, i, f)
			}
		}
		if userList != nil {
			if _, ok := userList[value.User]; !ok {
				return nil
			}
		}

		old, ok := p.columnsPriv.Get(itemColumnsPriv{username: value.User})
		if !ok {
			old.username = value.User
		}
		old.data = append(old.data, value)
		p.columnsPriv.ReplaceOrInsert(old)
		return nil
	}
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
	return stringutil.DoMatchBinary(str, patChars, patTypes)
}

// matchIdentity finds an identity to match a user + host
// using the correct rules according to MySQL.
func (p *MySQLPrivilege) matchIdentity(user, host string, skipNameResolve bool) *UserRecord {
	item, ok := p.user.Get(itemUser{username: user})
	if !ok {
		return nil
	}

	for i := 0; i < len(item.data); i++ {
		record := &item.data[i]
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
			for i := 0; i < len(item.data); i++ {
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
		for i := 0; i < len(records.data); i++ {
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
	for i := 0; i < len(uGlobal); i++ {
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
	item, exists := p.db.Get(itemDB{username: user})
	if exists {
		records := item.data
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
	item, exists := p.tablesPriv.Get(itemTablesPriv{username: user})
	if exists {
		records := item.data
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
	item, exists := p.columnsPriv.Get(itemColumnsPriv{username: user})
	if exists {
		for i := 0; i < len(item.data); i++ {
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

	if item, exists := p.tablesPriv.Get(itemTablesPriv{username: user}); exists {
		for _, record := range item.data {
			if record.baseRecord.match(user, host) &&
				strings.EqualFold(record.DB, db) {
				if record.TablePriv != 0 || record.ColumnPriv != 0 {
					return true
				}
			}
		}
	}

	if item, exists := p.columnsPriv.Get(itemColumnsPriv{username: user}); exists {
		for _, record := range item.data {
			if record.baseRecord.match(user, host) &&
				strings.EqualFold(record.DB, db) {
				if record.ColumnPriv != 0 {
					return true
				}
			}
		}
	}

	return false
}

func (p *MySQLPrivilege) showGrants(ctx sessionctx.Context, user, host string, roles []*auth.RoleIdentity) []string {
	var gs []string //nolint: prealloc
	var sortFromIdx int
	var hasGlobalGrant = false
	// Some privileges may granted from role inheritance.
	// We should find these inheritance relationship.
	allRoles := p.FindAllUserEffectiveRoles(user, host, roles)
	// Show global grants.
	var currentPriv mysql.PrivilegeType
	var userExists = false
	// Check whether user exists.
	if userList, ok := p.user.Get(itemUser{username: user}); ok {
		for _, record := range userList.data {
			if record.fullyMatch(user, host) {
				userExists = true
				hasGlobalGrant = true
				currentPriv |= record.Privileges
				break
			}
		}
		if !userExists {
			return gs
		}
	}

	for _, r := range allRoles {
		if userList, ok := p.user.Get(itemUser{username: r.Username}); ok {
			for _, record := range userList.data {
				if record.fullyMatch(r.Username, r.Hostname) {
					hasGlobalGrant = true
					currentPriv |= record.Privileges
				}
			}
		}
	}
	g := userPrivToString(currentPriv)
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
	p.db.Ascend(func(itm itemDB) bool {
		for _, record := range itm.data {
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
		return true
	})

	sqlMode := ctx.GetSessionVars().SQLMode
	for dbName, priv := range dbPrivTable {
		dbName = stringutil.Escape(dbName, sqlMode)
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
	slices.Sort(gs[sortFromIdx:])

	// Show table scope grants.
	sortFromIdx = len(gs)
	tablePrivTable := make(map[string]mysql.PrivilegeType)
	p.tablesPriv.Ascend(func(itm itemTablesPriv) bool {
		for _, record := range itm.data {
			recordKey := stringutil.Escape(record.DB, sqlMode) + "." + stringutil.Escape(record.TableName, sqlMode)
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
		return true
	})
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
	slices.Sort(gs[sortFromIdx:])

	// Show column scope grants, column and table are combined.
	// A map of "DB.Table" => Priv(col1, col2 ...)
	sortFromIdx = len(gs)
	columnPrivTable := make(map[string]privOnColumns)
	p.columnsPriv.Ascend(func(itm itemColumnsPriv) bool {
		for _, record := range itm.data {
			if !collectColumnGrant(&record, user, host, columnPrivTable, sqlMode) {
				for _, r := range allRoles {
					collectColumnGrant(&record, r.Username, r.Hostname, columnPrivTable, sqlMode)
				}
			}
		}
		return true
	})
	for k, v := range columnPrivTable {
		privCols := privOnColumnsToString(v)
		s := fmt.Sprintf(`GRANT %s ON %s TO '%s'@'%s'`, privCols, k, user, host)
		gs = append(gs, s)
	}
	slices.Sort(gs[sortFromIdx:])

	// Show role grants.
	graphKey := auth.RoleIdentity{user, host}
	edgeTable, ok := p.roleGraph[graphKey]
	g = ""
	if ok {
		sortedRes := make([]string, 0, 10)
		for k := range edgeTable.roleList {
			tmp := fmt.Sprintf("'%s'@'%s'", k.Username, k.Hostname)
			sortedRes = append(sortedRes, tmp)
		}
		slices.Sort(sortedRes)
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
	if item, exists := p.dynamicPriv.Get(itemDynamicPriv{username: user}); exists {
		for _, record := range item.data {
			if record.fullyMatch(user, host) {
				dynamicPrivsMap[record.PrivilegeName] = record.GrantOption
			}
		}
	}
	for _, r := range allRoles {
		if item, exists := p.dynamicPriv.Get(itemDynamicPriv{username: r.Username}); exists {
			for _, record := range item.data {
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
		slices.Sort(dynamicPrivs)
		s := fmt.Sprintf("GRANT %s ON *.* TO '%s'@'%s'", strings.Join(dynamicPrivs, ","), user, host)
		gs = append(gs, s)
	}
	if len(grantableDynamicPrivs) > 0 {
		slices.Sort(grantableDynamicPrivs)
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

func collectColumnGrant(record *columnsPrivRecord, user, host string, columnPrivTable map[string]privOnColumns, sqlMode mysql.SQLMode) bool {
	if record.baseRecord.match(user, host) {
		recordKey := stringutil.Escape(record.DB, sqlMode) + "." + stringutil.Escape(record.TableName, sqlMode)

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
	p.user.Ascend(func(itm itemUser) bool {
		for _, u := range itm.data {
			if showOtherUsers || u.match(user, host) {
				rows = appendUserPrivilegesTableRow(rows, u)
			}
		}
		return true
	})
	p.dynamicPriv.Ascend(func(itm itemDynamicPriv) bool {
		for _, dynamicPriv := range itm.data {
			if showOtherUsers || dynamicPriv.match(user, host) {
				rows = appendDynamicPrivRecord(rows, dynamicPriv)
			}
		}
		return true
	})
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
	if item, exists := p.defaultRoles.Get(itemDefaultRole{username: user}); exists {
		for _, r := range item.data {
			if r.match(user, host) {
				ret = append(ret, &auth.RoleIdentity{Username: r.DefaultRoleUser, Hostname: r.DefaultRoleHost})
			}
		}
	}
	return ret
}

func (p *MySQLPrivilege) getAllRoles(user, host string) []*auth.RoleIdentity {
	key := auth.RoleIdentity{user, host}
	edgeTable, ok := p.roleGraph[key]
	ret := make([]*auth.RoleIdentity, 0, len(edgeTable.roleList))
	if ok {
		for _, r := range edgeTable.roleList {
			ret = append(ret, r)
		}
	}
	return ret
}

// SetGlobalVarsAccessor is only used for test.
func (p *MySQLPrivilege) SetGlobalVarsAccessor(globalVars variable.GlobalVarAccessor) {
	p.globalVars = globalVars
}

// Handle wraps MySQLPrivilege providing thread safe access.
type Handle struct {
	sctx util.SessionPool
	priv atomic.Pointer[MySQLPrivilege]
	// Only load the active user's data to save memory
	// username => struct{}
	activeUsers sync.Map
	fullData    atomic.Bool
	globalVars  variable.GlobalVarAccessor
}

// NewHandle returns a Handle.
func NewHandle(sctx util.SessionPool, globalVars variable.GlobalVarAccessor) *Handle {
	priv := newMySQLPrivilege()
	ret := &Handle{}
	ret.sctx = sctx
	ret.globalVars = globalVars
	ret.priv.Store(priv)
	return ret
}

// ensureActiveUser ensure that the specific user data is loaded in-memory.
func (h *Handle) ensureActiveUser(user string) error {
	if h.fullData.Load() {
		// All users data are in-memory, nothing to do
		return nil
	}

	_, exist := h.activeUsers.Load(user)
	if exist {
		return nil
	}
	return h.updateUsers([]string{user})
}

func (h *Handle) merge(data *MySQLPrivilege, userList map[string]struct{}) {
	for {
		old := h.Get()
		swapped := h.priv.CompareAndSwap(old, old.merge(data, userList))
		if swapped {
			break
		}
	}
	for user := range userList {
		h.activeUsers.Store(user, struct{}{})
	}
}

// Get the MySQLPrivilege for read.
func (h *Handle) Get() *MySQLPrivilege {
	return h.priv.Load()
}

// UpdateAll loads all the users' privilege info from kv storage.
func (h *Handle) UpdateAll() error {
	priv := newMySQLPrivilege()
	res, err := h.sctx.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer h.sctx.Put(res)
	exec := res.(sqlexec.SQLExecutor)

	err = priv.LoadAll(exec)
	if err != nil {
		return errors.Trace(err)
	}
	h.priv.Store(priv)
	h.fullData.Store(true)
	return nil
}

// UpdateAllActive loads all the active users' privilege info from kv storage.
func (h *Handle) UpdateAllActive() error {
	h.fullData.Store(false)
	userList := make([]string, 0, 20)
	h.activeUsers.Range(func(key, _ any) bool {
		userList = append(userList, key.(string))
		return true
	})
	metrics.ActiveUser.Set(float64(len(userList)))
	return h.updateUsers(userList)
}

// Update loads the privilege info from kv storage for the list of users.
func (h *Handle) Update(userList []string) error {
	h.fullData.Store(false)
	if len(userList) > 100 {
		logutil.BgLogger().Warn("update user list is long", zap.Int("len", len(userList)))
	}
	needReload := false
	for _, user := range userList {
		if _, ok := h.activeUsers.Load(user); ok {
			needReload = true
			break
		}
	}
	if !needReload {
		return nil
	}

	return h.updateUsers(userList)
}

func (h *Handle) updateUsers(userList []string) error {
	res, err := h.sctx.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer h.sctx.Put(res)
	exec := res.(sqlexec.SQLExecutor)

	p := newMySQLPrivilege()
	p.globalVars = h.globalVars
	// Load the full role edge table first.
	p.roleGraph = make(map[auth.RoleIdentity]roleGraphEdgesTable)
	err = loadTable(exec, sqlLoadRoleGraph, p.decodeRoleEdgesTable)
	if err != nil {
		return errors.Trace(err)
	}

	// Including the user and also their roles
	userAndRoles := findUserAndAllRoles(userList, p.roleGraph)
	err = p.loadSomeUsers(exec, userAndRoles)
	if err != nil {
		return err
	}
	h.merge(p, userAndRoles)
	return nil
}
