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
	"fmt"
	"slices"
	"strings"

	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

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
		if record.Privileges&mysql.ProcessPriv > 0 && strings.EqualFold(db, metadef.MetricSchemaName.O) {
			return true
		}
	}

	// INFORMATION_SCHEMA is visible to all users.
	if strings.EqualFold(db, metadef.InformationSchemaName.O) {
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
	graphKey := auth.RoleIdentity{
		Username: user,
		Hostname: host,
	}
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
	key := auth.RoleIdentity{
		Username: user,
		Hostname: host,
	}
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
