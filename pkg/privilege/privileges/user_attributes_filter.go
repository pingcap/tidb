// Copyright 2026 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
)

type attrVisMode int

const (
	attrVisAll attrVisMode = iota
	attrVisNonSystem
	attrVisSelf
)

// UserAttrFilter filters USER_ATTRIBUTES rows by MySQL 8.0.22+ visibility rules.
type UserAttrFilter interface {
	Visible(user, host string) bool
}

type userAttrFilter struct {
	priv       *MySQLPrivilege
	viewerUser string
	viewerHost string
	mode       attrVisMode
}

func (f *userAttrFilter) Visible(user, host string) bool {
	switch f.mode {
	case attrVisAll:
		return true
	case attrVisSelf, attrVisNonSystem:
		record := f.priv.matchUser(user, host)
		if record != nil && record.match(f.viewerUser, f.viewerHost) {
			return true
		}
		if f.mode == attrVisSelf {
			return false
		}
		return !f.priv.RequestDynamicVerification(nil, user, host, "SYSTEM_USER", false)
	default:
		return false
	}
}

// NewUserAttrFilter builds a row filter for INFORMATION_SCHEMA.USER_ATTRIBUTES.
// See https://dev.mysql.com/doc/refman/8.4/en/information-schema-user-attributes-table.html
func NewUserAttrFilter(
	activeRoles []*auth.RoleIdentity,
	viewerUser, viewerHost string,
	pm privilege.Manager,
) UserAttrFilter {
	if SkipWithGrant || pm == nil || (viewerUser == "" && viewerHost == "") {
		return &userAttrFilter{mode: attrVisAll}
	}
	userPriv, ok := pm.(*UserPrivileges)
	if !ok {
		return &userAttrFilter{mode: attrVisAll}
	}
	priv := userPriv.Handle.Get()

	// If the viewer has SELECT or UPDATE privileges on the "user" table, return a filter that allows all rows.
	if priv.RequestVerification(activeRoles, viewerUser, viewerHost, mysql.SystemDB, "user", "", mysql.SelectPriv) ||
		priv.RequestVerification(activeRoles, viewerUser, viewerHost, mysql.SystemDB, "user", "", mysql.UpdatePriv) {
		return &userAttrFilter{priv: priv, mode: attrVisAll}
	}

	if priv.RequestVerification(activeRoles, viewerUser, viewerHost, "", "", "", mysql.CreateUserPriv) {
		// If the viewer has CREATE_USER and SYSTEM_USER privileges, return a filter that allows all rows.
		if priv.RequestDynamicVerification(activeRoles, viewerUser, viewerHost, "SYSTEM_USER", false) {
			return &userAttrFilter{priv: priv, mode: attrVisAll}
		}
		// If the viewer has CREATE_USER but no SYSTEM_USER privileges, return a filter that allows rows for non-system users.
		return &userAttrFilter{
			priv:       priv,
			viewerUser: viewerUser,
			viewerHost: viewerHost,
			mode:       attrVisNonSystem,
		}
	}

	// If the viewer has no privileges above, return a filter that allows self-only.
	return &userAttrFilter{
		priv:       priv,
		viewerUser: viewerUser,
		viewerHost: viewerHost,
		mode:       attrVisSelf,
	}
}
