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
	"encoding/json"
	"net"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"go.uber.org/zap"
)

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
			val, err := p.globalVars.GetGlobalSysVar(vardef.DefaultAuthPlugin)
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
			case f.ColumnAsName.L == "max_user_connections":
				value.MaxUserConnections = row.GetInt64(i)
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
				// When all characters are upper, strings.ToUpper returns a reference instead of a new copy.
				// so strings.Clone is required here.
				tmp := strings.Clone(row.GetString(i))
				value.PrivilegeName = strings.ToUpper(tmp)
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
				value.DB = strings.Clone(row.GetString(i))
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
				value.DB = strings.Clone(row.GetString(i))
			case "table_name":
				value.TableName = strings.Clone(row.GetString(i))
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
			fromHost = strings.Clone(row.GetString(i))
		case "from_user":
			fromUser = strings.Clone(row.GetString(i))
		case "to_host":
			toHost = strings.Clone(row.GetString(i))
		case "to_user":
			toUser = strings.Clone(row.GetString(i))
		}
	}
	fromKey := auth.RoleIdentity{
		Username: fromUser,
		Hostname: fromHost,
	}
	toKey := auth.RoleIdentity{
		Username: toUser,
		Hostname: toHost,
	}
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
				value.DefaultRoleHost = strings.Clone(row.GetString(i))
			case "default_role_user":
				value.DefaultRoleUser = strings.Clone(row.GetString(i))
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
				value.DB = strings.Clone(row.GetString(i))
			case "table_name":
				value.TableName = strings.Clone(row.GetString(i))
			case "column_name":
				value.ColumnName = strings.Clone(row.GetString(i))
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
