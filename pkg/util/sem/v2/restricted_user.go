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

package sem

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

// CheckRestrictedUserStmt returns a non-nil error when stmt performs account or
// role management against a SEM-protected user or role. Protected identities
// cannot be dropped, renamed, or have their role membership changed regardless
// of the caller's privileges, so callers must run this check before any
// RESTRICTED_SQL_ADMIN bypass.
func CheckRestrictedUserStmt(stmt ast.Node) error {
	sem := globalSem.Load()
	if sem == nil {
		return nil
	}
	return sem.checkRestrictedUserStmt(stmt)
}

func (s *semImpl) isRestrictedUser(userName, hostname string) bool {
	return isProtectedName(s.restrictedUsers, userName, hostname)
}

func (s *semImpl) isRestrictedRole(roleName, hostname string) bool {
	return isProtectedName(s.restrictedRoles, roleName, hostname)
}

// isProtectedName matches a user/role against a protected set. The keyspace
// username policy strips the tenant prefix first, and only host '%' identities
// are matched, mirroring how the protected accounts are provisioned.
func isProtectedName(set map[string]struct{}, name, hostname string) bool {
	if len(set) == 0 {
		return false
	}
	originalName := keyspace.GetUsernamePolicy().GetOriginalUsername(name)
	if originalName == "" || hostname != "%" {
		return false
	}
	_, ok := set[originalName]
	return ok
}

func (s *semImpl) checkRestrictedUserStmt(stmt ast.Node) error {
	if len(s.restrictedUsers) == 0 && len(s.restrictedRoles) == 0 {
		return nil
	}
	switch x := stmt.(type) {
	case *ast.DropUserStmt:
		for _, user := range x.UserList {
			if s.isRestrictedUser(user.Username, user.Hostname) {
				return notSupported(fmt.Sprintf("DROP USER %s", user))
			}
		}
	case *ast.RenameUserStmt:
		for _, pair := range x.UserToUsers {
			if s.isRestrictedUser(pair.OldUser.Username, pair.OldUser.Hostname) {
				return notSupported(fmt.Sprintf("RENAME USER %s", pair.OldUser))
			}
		}
	case *ast.GrantRoleStmt:
		for _, role := range x.Roles {
			if s.isRestrictedRole(role.Username, role.Hostname) {
				return notSupported(fmt.Sprintf("GRANT ROLE %s", role))
			}
		}
		for _, user := range x.Users {
			if s.isRestrictedUser(user.Username, user.Hostname) {
				return notSupported(fmt.Sprintf("GRANT ROLE TO %s", user))
			}
		}
	case *ast.RevokeRoleStmt:
		for _, role := range x.Roles {
			if s.isRestrictedRole(role.Username, role.Hostname) {
				return notSupported(fmt.Sprintf("REVOKE %s", role))
			}
		}
		for _, user := range x.Users {
			if s.isRestrictedUser(user.Username, user.Hostname) {
				return notSupported(fmt.Sprintf("REVOKE FROM %s", user))
			}
		}
	case *ast.SetRoleStmt:
		return s.checkSetRole(x)
	case *ast.SetDefaultRoleStmt:
		return s.checkSetDefaultRole(x)
	}
	return nil
}

func (s *semImpl) checkSetRole(stmt *ast.SetRoleStmt) error {
	// The wildcard forms can activate a protected role when it has been granted,
	// bypassing the explicit RoleList check below, so they are rejected outright.
	switch stmt.SetRoleOpt {
	case ast.SetRoleDefault:
		return notSupported("SET ROLE DEFAULT")
	case ast.SetRoleAll:
		return notSupported("SET ROLE ALL")
	case ast.SetRoleAllExcept:
		return notSupported("SET ROLE ALL EXCEPT")
	}
	for _, role := range stmt.RoleList {
		if s.isRestrictedRole(role.Username, role.Hostname) {
			return notSupported(fmt.Sprintf("SET ROLE %s", role))
		}
	}
	return nil
}

func (s *semImpl) checkSetDefaultRole(stmt *ast.SetDefaultRoleStmt) error {
	for _, user := range stmt.UserList {
		if s.isRestrictedUser(user.Username, user.Hostname) {
			return notSupported(fmt.Sprintf("SET DEFAULT ROLE TO %s", user))
		}
	}
	if stmt.SetRoleOpt == ast.SetRoleAll {
		return notSupported("SET DEFAULT ROLE ALL")
	}
	for _, role := range stmt.RoleList {
		if s.isRestrictedRole(role.Username, role.Hostname) {
			return notSupported(fmt.Sprintf("SET DEFAULT ROLE %s", role))
		}
	}
	return nil
}

func notSupported(feature string) error {
	return plannererrors.ErrNotSupportedWithSem.GenWithStackByArgs(feature)
}
