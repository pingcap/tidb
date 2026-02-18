// Copyright 2019 PingCAP, Inc.
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

package executor

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/querywatch"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

func (e *SimpleExec) setDefaultRoleNone(s *ast.SetDefaultRoleStmt) error {
	restrictedCtx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	defer e.ReleaseSysSession(ctx, restrictedCtx)
	sqlExecutor := restrictedCtx.GetSQLExecutor()
	if _, err := sqlExecutor.ExecuteInternal(ctx, "begin"); err != nil {
		return err
	}
	sql := new(strings.Builder)
	for _, u := range s.UserList {
		sql.Reset()
		sqlescape.MustFormatSQL(sql, "DELETE IGNORE FROM mysql.default_roles WHERE USER=%? AND HOST=%?;", u.Username, u.Hostname)
		if _, err := sqlExecutor.ExecuteInternal(ctx, sql.String()); err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
			if _, rollbackErr := sqlExecutor.ExecuteInternal(ctx, "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
	}
	if _, err := sqlExecutor.ExecuteInternal(ctx, "commit"); err != nil {
		return err
	}
	return nil
}

func (e *SimpleExec) setDefaultRoleRegular(ctx context.Context, s *ast.SetDefaultRoleStmt) error {
	for _, user := range s.UserList {
		exists, err := userExists(ctx, e.Ctx(), user.Username, user.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return exeerrors.ErrCannotUser.GenWithStackByArgs("SET DEFAULT ROLE", user.String())
		}
	}
	for _, role := range s.RoleList {
		exists, err := userExists(ctx, e.Ctx(), role.Username, role.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return exeerrors.ErrCannotUser.GenWithStackByArgs("SET DEFAULT ROLE", role.String())
		}
	}

	restrictedCtx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	defer e.ReleaseSysSession(internalCtx, restrictedCtx)
	sqlExecutor := restrictedCtx.GetSQLExecutor()
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "begin"); err != nil {
		return err
	}
	sql := new(strings.Builder)
	for _, user := range s.UserList {
		sql.Reset()
		sqlescape.MustFormatSQL(sql, "DELETE IGNORE FROM mysql.default_roles WHERE USER=%? AND HOST=%?;", user.Username, user.Hostname)
		if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
			if _, rollbackErr := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
		for _, role := range s.RoleList {
			checker := privilege.GetPrivilegeManager(e.Ctx())
			ok := checker.FindEdge(ctx, role, user)
			if !ok {
				if _, rollbackErr := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); rollbackErr != nil {
					return rollbackErr
				}
				return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(role.String(), user.String())
			}
			sql.Reset()
			sqlescape.MustFormatSQL(sql, "INSERT IGNORE INTO mysql.default_roles values(%?, %?, %?, %?);", user.Hostname, user.Username, role.Hostname, role.Username)
			if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
				logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
				if _, rollbackErr := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); rollbackErr != nil {
					return rollbackErr
				}
				return err
			}
		}
	}
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "commit"); err != nil {
		return err
	}
	return nil
}

func (e *SimpleExec) setDefaultRoleAll(ctx context.Context, s *ast.SetDefaultRoleStmt) error {
	for _, user := range s.UserList {
		exists, err := userExists(ctx, e.Ctx(), user.Username, user.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return exeerrors.ErrCannotUser.GenWithStackByArgs("SET DEFAULT ROLE", user.String())
		}
	}
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	restrictedCtx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(internalCtx, restrictedCtx)
	sqlExecutor := restrictedCtx.GetSQLExecutor()
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "begin"); err != nil {
		return err
	}
	sql := new(strings.Builder)
	for _, user := range s.UserList {
		sql.Reset()
		sqlescape.MustFormatSQL(sql, "DELETE IGNORE FROM mysql.default_roles WHERE USER=%? AND HOST=%?;", user.Username, user.Hostname)
		if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
			if _, rollbackErr := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
		sql.Reset()
		sqlescape.MustFormatSQL(sql, "INSERT IGNORE INTO mysql.default_roles(HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER) SELECT TO_HOST,TO_USER,FROM_HOST,FROM_USER FROM mysql.role_edges WHERE TO_HOST=%? AND TO_USER=%?;", user.Hostname, user.Username)
		if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
			if _, rollbackErr := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
	}
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "commit"); err != nil {
		return err
	}
	return nil
}

func (e *SimpleExec) setDefaultRoleForCurrentUser(ctx context.Context, s *ast.SetDefaultRoleStmt) (err error) {
	checker := privilege.GetPrivilegeManager(e.Ctx())
	user := s.UserList[0]
	restrictedCtx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnPrivilege)
	defer e.ReleaseSysSession(ctx, restrictedCtx)
	sqlExecutor := restrictedCtx.GetSQLExecutor()

	if _, err := sqlExecutor.ExecuteInternal(ctx, "begin"); err != nil {
		return err
	}

	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, "DELETE IGNORE FROM mysql.default_roles WHERE USER=%? AND HOST=%?;", user.Username, user.Hostname)
	if _, err := sqlExecutor.ExecuteInternal(ctx, sql.String()); err != nil {
		logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
		if _, rollbackErr := sqlExecutor.ExecuteInternal(ctx, "rollback"); rollbackErr != nil {
			return rollbackErr
		}
		return err
	}

	sql.Reset()
	switch s.SetRoleOpt {
	case ast.SetRoleNone:
		sqlescape.MustFormatSQL(sql, "DELETE IGNORE FROM mysql.default_roles WHERE USER=%? AND HOST=%?;", user.Username, user.Hostname)
	case ast.SetRoleAll:
		sqlescape.MustFormatSQL(sql, "INSERT IGNORE INTO mysql.default_roles(HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER) SELECT TO_HOST,TO_USER,FROM_HOST,FROM_USER FROM mysql.role_edges WHERE TO_HOST=%? AND TO_USER=%?;", user.Hostname, user.Username)
	case ast.SetRoleRegular:
		sqlescape.MustFormatSQL(sql, "INSERT IGNORE INTO mysql.default_roles values")
		for i, role := range s.RoleList {
			if i > 0 {
				sqlescape.MustFormatSQL(sql, ",")
			}
			ok := checker.FindEdge(ctx, role, user)
			if !ok {
				return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(role.String(), user.String())
			}
			sqlescape.MustFormatSQL(sql, "(%?, %?, %?, %?)", user.Hostname, user.Username, role.Hostname, role.Username)
		}
	}

	if _, err := sqlExecutor.ExecuteInternal(ctx, sql.String()); err != nil {
		logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
		if _, rollbackErr := sqlExecutor.ExecuteInternal(ctx, "rollback"); rollbackErr != nil {
			return rollbackErr
		}
		return err
	}
	if _, err := sqlExecutor.ExecuteInternal(ctx, "commit"); err != nil {
		return err
	}
	return nil
}

func userIdentityToUserList(specs []*auth.UserIdentity) []string {
	users := make([]string, 0, len(specs))
	for _, user := range specs {
		users = append(users, user.Username)
	}
	return users
}

func (e *SimpleExec) executeSetDefaultRole(ctx context.Context, s *ast.SetDefaultRoleStmt) (err error) {
	sessionVars := e.Ctx().GetSessionVars()
	checker := privilege.GetPrivilegeManager(e.Ctx())
	if checker == nil {
		return errors.New("miss privilege checker")
	}

	if len(s.UserList) == 1 && sessionVars.User != nil {
		u, h := s.UserList[0].Username, s.UserList[0].Hostname
		if u == sessionVars.User.Username && h == sessionVars.User.AuthHostname {
			err = e.setDefaultRoleForCurrentUser(ctx, s)
			if err != nil {
				return err
			}
			users := userIdentityToUserList(s.UserList)
			return domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege(users)
		}
	}

	activeRoles := sessionVars.ActiveRoles
	if !checker.RequestVerification(activeRoles, mysql.SystemDB, mysql.DefaultRoleTable, "", mysql.UpdatePriv) {
		if !checker.RequestVerification(activeRoles, "", "", "", mysql.CreateUserPriv) {
			return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
		}
	}

	switch s.SetRoleOpt {
	case ast.SetRoleAll:
		err = e.setDefaultRoleAll(ctx, s)
	case ast.SetRoleNone:
		err = e.setDefaultRoleNone(s)
	case ast.SetRoleRegular:
		err = e.setDefaultRoleRegular(ctx, s)
	}
	if err != nil {
		return
	}
	users := userIdentityToUserList(s.UserList)
	return domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege(users)
}

func (e *SimpleExec) setRoleRegular(ctx context.Context, s *ast.SetRoleStmt) error {
	// Deal with SQL like `SET ROLE role1, role2;`
	checkDup := make(map[string]*auth.RoleIdentity, len(s.RoleList))
	// Check whether RoleNameList contain duplicate role name.
	for _, r := range s.RoleList {
		key := r.String()
		checkDup[key] = r
	}
	roleList := make([]*auth.RoleIdentity, 0, 10)
	for _, v := range checkDup {
		roleList = append(roleList, v)
	}

	checker := privilege.GetPrivilegeManager(e.Ctx())
	ok, roleName := checker.ActiveRoles(ctx, e.Ctx(), roleList)
	if !ok {
		u := e.Ctx().GetSessionVars().User
		return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return nil
}

func (e *SimpleExec) setRoleAll(ctx context.Context) error {
	// Deal with SQL like `SET ROLE ALL;`
	checker := privilege.GetPrivilegeManager(e.Ctx())
	user, host := e.Ctx().GetSessionVars().User.AuthUsername, e.Ctx().GetSessionVars().User.AuthHostname
	roles := checker.GetAllRoles(user, host)
	ok, roleName := checker.ActiveRoles(ctx, e.Ctx(), roles)
	if !ok {
		u := e.Ctx().GetSessionVars().User
		return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return nil
}

func (e *SimpleExec) setRoleAllExcept(ctx context.Context, s *ast.SetRoleStmt) error {
	// Deal with SQL like `SET ROLE ALL EXCEPT role1, role2;`
	for _, r := range s.RoleList {
		if r.Hostname == "" {
			r.Hostname = "%"
		}
	}
	checker := privilege.GetPrivilegeManager(e.Ctx())
	user, host := e.Ctx().GetSessionVars().User.AuthUsername, e.Ctx().GetSessionVars().User.AuthHostname
	roles := checker.GetAllRoles(user, host)

	filter := func(arr []*auth.RoleIdentity, f func(*auth.RoleIdentity) bool) []*auth.RoleIdentity {
		i, j := 0, 0
		for i = range arr {
			if f(arr[i]) {
				arr[j] = arr[i]
				j++
			}
		}
		return arr[:j]
	}
	banned := func(r *auth.RoleIdentity) bool {
		for _, ban := range s.RoleList {
			if ban.Hostname == r.Hostname && ban.Username == r.Username {
				return false
			}
		}
		return true
	}

	afterExcept := filter(roles, banned)
	ok, roleName := checker.ActiveRoles(ctx, e.Ctx(), afterExcept)
	if !ok {
		u := e.Ctx().GetSessionVars().User
		return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return nil
}

func (e *SimpleExec) setRoleDefault(ctx context.Context) error {
	// Deal with SQL like `SET ROLE DEFAULT;`
	checker := privilege.GetPrivilegeManager(e.Ctx())
	user, host := e.Ctx().GetSessionVars().User.AuthUsername, e.Ctx().GetSessionVars().User.AuthHostname
	roles := checker.GetDefaultRoles(ctx, user, host)
	ok, roleName := checker.ActiveRoles(ctx, e.Ctx(), roles)
	if !ok {
		u := e.Ctx().GetSessionVars().User
		return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return nil
}

func (e *SimpleExec) setRoleNone(ctx context.Context) error {
	// Deal with SQL like `SET ROLE NONE;`
	checker := privilege.GetPrivilegeManager(e.Ctx())
	roles := make([]*auth.RoleIdentity, 0)
	ok, roleName := checker.ActiveRoles(ctx, e.Ctx(), roles)
	if !ok {
		u := e.Ctx().GetSessionVars().User
		return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return nil
}

func (e *SimpleExec) executeSetRole(ctx context.Context, s *ast.SetRoleStmt) error {
	switch s.SetRoleOpt {
	case ast.SetRoleRegular:
		return e.setRoleRegular(ctx, s)
	case ast.SetRoleAll:
		return e.setRoleAll(ctx)
	case ast.SetRoleAllExcept:
		return e.setRoleAllExcept(ctx, s)
	case ast.SetRoleNone:
		return e.setRoleNone(ctx)
	case ast.SetRoleDefault:
		return e.setRoleDefault(ctx)
	}
	return nil
}
func (e *SimpleExec) executeRevokeRole(ctx context.Context, s *ast.RevokeRoleStmt) error {
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)

	//Fix revoke role from current_user results error.
	e.setCurrentUser(s.Users)

	for _, role := range s.Roles {
		exists, err := userExists(ctx, e.Ctx(), role.Username, role.Hostname)
		if err != nil {
			return errors.Trace(err)
		}
		if !exists {
			return exeerrors.ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", role.String())
		}
	}

	restrictedCtx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(internalCtx, restrictedCtx)
	sqlExecutor := restrictedCtx.GetSQLExecutor()

	// begin a transaction to insert role graph edges.
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "begin"); err != nil {
		return errors.Trace(err)
	}
	sql := new(strings.Builder)
	// when an active role of current user is revoked,
	// it should be removed from activeRoles
	activeRoles, curUser, curHost := e.Ctx().GetSessionVars().ActiveRoles, "", ""
	if user := e.Ctx().GetSessionVars().User; user != nil {
		curUser, curHost = user.AuthUsername, user.AuthHostname
	}
	for _, user := range s.Users {
		exists, err := userExists(ctx, e.Ctx(), user.Username, user.Hostname)
		if err != nil {
			return errors.Trace(err)
		}
		if !exists {
			if _, err := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); err != nil {
				return errors.Trace(err)
			}
			return exeerrors.ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", user.String())
		}
		for _, role := range s.Roles {
			if role.Hostname == "" {
				role.Hostname = "%"
			}
			sql.Reset()
			sqlescape.MustFormatSQL(sql, `DELETE IGNORE FROM %n.%n WHERE FROM_HOST=%? and FROM_USER=%? and TO_HOST=%? and TO_USER=%?`, mysql.SystemDB, mysql.RoleEdgeTable, role.Hostname, role.Username, user.Hostname, user.Username)
			if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
				if _, err := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); err != nil {
					return errors.Trace(err)
				}
				return exeerrors.ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", role.String())
			}

			sql.Reset()
			sqlescape.MustFormatSQL(sql, `DELETE IGNORE FROM %n.%n WHERE DEFAULT_ROLE_HOST=%? and DEFAULT_ROLE_USER=%? and HOST=%? and USER=%?`, mysql.SystemDB, mysql.DefaultRoleTable, role.Hostname, role.Username, user.Hostname, user.Username)
			if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
				if _, err := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); err != nil {
					return errors.Trace(err)
				}
				return exeerrors.ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", role.String())
			}

			// delete from activeRoles
			if curUser == user.Username && curHost == user.Hostname {
				for i := range activeRoles {
					if activeRoles[i].Username == role.Username && activeRoles[i].Hostname == role.Hostname {
						activeRoles = slices.Delete(activeRoles, i, i+1)
						break
					}
				}
			}
		}
	}
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "commit"); err != nil {
		return err
	}
	checker := privilege.GetPrivilegeManager(e.Ctx())
	if checker == nil {
		return errors.New("miss privilege checker")
	}
	if ok, roleName := checker.ActiveRoles(ctx, e.Ctx(), activeRoles); !ok {
		u := e.Ctx().GetSessionVars().User
		return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	userList := userIdentityToUserList(s.Users)
	return domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege(userList)
}
func (e *SimpleExec) executeGrantRole(ctx context.Context, s *ast.GrantRoleStmt) error {
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)

	e.setCurrentUser(s.Users)

	for _, role := range s.Roles {
		exists, err := userExists(ctx, e.Ctx(), role.Username, role.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return exeerrors.ErrGrantRole.GenWithStackByArgs(role.String())
		}
	}
	for _, user := range s.Users {
		exists, err := userExists(ctx, e.Ctx(), user.Username, user.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return exeerrors.ErrCannotUser.GenWithStackByArgs("GRANT ROLE", user.String())
		}
	}

	restrictedCtx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(internalCtx, restrictedCtx)
	sqlExecutor := restrictedCtx.GetSQLExecutor()

	// begin a transaction to insert role graph edges.
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "begin"); err != nil {
		return err
	}

	sql := new(strings.Builder)
	for _, user := range s.Users {
		for _, role := range s.Roles {
			sql.Reset()
			sqlescape.MustFormatSQL(sql, `INSERT IGNORE INTO %n.%n (FROM_HOST, FROM_USER, TO_HOST, TO_USER) VALUES (%?,%?,%?,%?)`, mysql.SystemDB, mysql.RoleEdgeTable, role.Hostname, role.Username, user.Hostname, user.Username)
			if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
				logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
				if _, err := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); err != nil {
					return err
				}
				return exeerrors.ErrCannotUser.GenWithStackByArgs("GRANT ROLE", user.String())
			}
		}
	}
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "commit"); err != nil {
		return err
	}
	userList := userIdentityToUserList(s.Users)
	return domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege(userList)
}

// Should cover same internal mysql.* tables as DROP USER, so this function is very similar
func (e *SimpleExec) executeRenameUser(s *ast.RenameUserStmt) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	var failedUser string
	sysSession, err := e.GetSysSession()
	defer e.ReleaseSysSession(ctx, sysSession)
	if err != nil {
		return err
	}
	sqlExecutor := sysSession.GetSQLExecutor()

	if _, err := sqlExecutor.ExecuteInternal(ctx, "BEGIN PESSIMISTIC"); err != nil {
		return err
	}
	for _, userToUser := range s.UserToUsers {
		oldUser, newUser := userToUser.OldUser, userToUser.NewUser
		if len(newUser.Username) > auth.UserNameMaxLength {
			return exeerrors.ErrWrongStringLength.GenWithStackByArgs(newUser.Username, "user name", auth.UserNameMaxLength)
		}
		if len(newUser.Hostname) > auth.HostNameMaxLength {
			return exeerrors.ErrWrongStringLength.GenWithStackByArgs(newUser.Hostname, "host name", auth.HostNameMaxLength)
		}
		exists, _, err := userExistsInternal(ctx, sqlExecutor, oldUser.Username, oldUser.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			failedUser = oldUser.String() + " TO " + newUser.String() + " old did not exist"
			break
		}

		exists, _, err = userExistsInternal(ctx, sqlExecutor, newUser.Username, newUser.Hostname)
		if err != nil {
			return err
		}
		if exists {
			// MySQL reports the old user, even when the issue is the new user.
			failedUser = oldUser.String() + " TO " + newUser.String() + " new did exist"
			break
		}

		if err = renameUserHostInSystemTable(sqlExecutor, mysql.UserTable, "User", "Host", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.UserTable + " error"
			break
		}

		// rename privileges from mysql.global_priv
		if err = renameUserHostInSystemTable(sqlExecutor, mysql.GlobalPrivTable, "User", "Host", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.GlobalPrivTable + " error"
			break
		}

		// rename privileges from mysql.db
		if err = renameUserHostInSystemTable(sqlExecutor, mysql.DBTable, "User", "Host", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.DBTable + " error"
			break
		}

		// rename privileges from mysql.tables_priv
		if err = renameUserHostInSystemTable(sqlExecutor, mysql.TablePrivTable, "User", "Host", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.TablePrivTable + " error"
			break
		}

		// rename relationship from mysql.role_edges
		if err = renameUserHostInSystemTable(sqlExecutor, mysql.RoleEdgeTable, "TO_USER", "TO_HOST", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.RoleEdgeTable + " (to) error"
			break
		}

		if err = renameUserHostInSystemTable(sqlExecutor, mysql.RoleEdgeTable, "FROM_USER", "FROM_HOST", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.RoleEdgeTable + " (from) error"
			break
		}

		// rename relationship from mysql.default_roles
		if err = renameUserHostInSystemTable(sqlExecutor, mysql.DefaultRoleTable, "DEFAULT_ROLE_USER", "DEFAULT_ROLE_HOST", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.DefaultRoleTable + " (default role user) error"
			break
		}

		if err = renameUserHostInSystemTable(sqlExecutor, mysql.DefaultRoleTable, "USER", "HOST", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.DefaultRoleTable + " error"
			break
		}

		// rename passwordhistory from  PasswordHistoryTable.
		if err = renameUserHostInSystemTable(sqlExecutor, mysql.PasswordHistoryTable, "USER", "HOST", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.PasswordHistoryTable + " error"
			break
		}

		// rename relationship from mysql.global_grants
		// TODO: add global_grants into the parser
		// TODO: need update columns_priv once we implement columns_priv functionality.
		// When that is added, please refactor both executeRenameUser and executeDropUser to use an array of tables
		// to loop over, so it is easier to maintain.
		if err = renameUserHostInSystemTable(sqlExecutor, "global_grants", "User", "Host", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " mysql.global_grants error"
			break
		}
	}

	if failedUser != "" {
		if _, err := sqlExecutor.ExecuteInternal(ctx, "rollback"); err != nil {
			return err
		}
		return exeerrors.ErrCannotUser.GenWithStackByArgs("RENAME USER", failedUser)
	}
	if _, err := sqlExecutor.ExecuteInternal(ctx, "commit"); err != nil {
		return err
	}

	userList := make([]string, 0, len(s.UserToUsers)*2)
	for _, users := range s.UserToUsers {
		userList = append(userList, users.OldUser.Username)
		userList = append(userList, users.NewUser.Username)
	}
	return domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege(userList)
}

func renameUserHostInSystemTable(sqlExecutor sqlexec.SQLExecutor, tableName, usernameColumn, hostColumn string, users *ast.UserToUser) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, `UPDATE %n.%n SET %n = %?, %n = %? WHERE %n = %? and %n = %?;`,
		mysql.SystemDB, tableName,
		usernameColumn, users.NewUser.Username, hostColumn, strings.ToLower(users.NewUser.Hostname),
		usernameColumn, users.OldUser.Username, hostColumn, strings.ToLower(users.OldUser.Hostname))
	_, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	return err
}

func (e *SimpleExec) executeDropQueryWatch(s *ast.DropQueryWatchStmt) error {
	return querywatch.ExecDropQueryWatch(e.Ctx(), s)
}

func (e *SimpleExec) executeDropUser(ctx context.Context, s *ast.DropUserStmt) error {
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	// Check privileges.
	// Check `CREATE USER` privilege.
	checker := privilege.GetPrivilegeManager(e.Ctx())
	if checker == nil {
		return errors.New("miss privilege checker")
	}
	activeRoles := e.Ctx().GetSessionVars().ActiveRoles
	if !checker.RequestVerification(activeRoles, mysql.SystemDB, mysql.UserTable, "", mysql.DeletePriv) {
		if s.IsDropRole {
			if !checker.RequestVerification(activeRoles, "", "", "", mysql.DropRolePriv) &&
				!checker.RequestVerification(activeRoles, "", "", "", mysql.CreateUserPriv) {
				return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("DROP ROLE or CREATE USER")
			}
		}
		if !s.IsDropRole && !checker.RequestVerification(activeRoles, "", "", "", mysql.CreateUserPriv) {
			return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
		}
	}
	hasSystemUserPriv := checker.RequestDynamicVerification(activeRoles, "SYSTEM_USER", false)
	hasRestrictedUserPriv := checker.RequestDynamicVerification(activeRoles, "RESTRICTED_USER_ADMIN", false)
	failedUsers := make([]string, 0, len(s.UserList))
	sysSession, err := e.GetSysSession()
	defer e.ReleaseSysSession(internalCtx, sysSession)
	if err != nil {
		return err
	}
	sqlExecutor := sysSession.GetSQLExecutor()

	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "begin"); err != nil {
		return err
	}

	sql := new(strings.Builder)
	for _, user := range s.UserList {
		exists, err := userExists(ctx, e.Ctx(), user.Username, user.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			if !s.IfExists {
				failedUsers = append(failedUsers, user.String())
				break
			}
			e.Ctx().GetSessionVars().StmtCtx.AppendNote(infoschema.ErrUserDropExists.FastGenByArgs(user))
		}

		// Certain users require additional privileges in order to be modified.
		// If this is the case, we need to rollback all changes and return a privilege error.
		// Because in TiDB SUPER can be used as a substitute for any dynamic privilege, this effectively means that
		// any user with SUPER requires a user with SUPER to be able to DROP the user.
		// We also allow RESTRICTED_USER_ADMIN to count for simplicity.
		if !(hasSystemUserPriv || hasRestrictedUserPriv) && checker.RequestDynamicVerificationWithUser(ctx, "SYSTEM_USER", false, user) {
			if _, err := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); err != nil {
				return err
			}
			return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SYSTEM_USER or SUPER")
		}

		// begin a transaction to delete a user.
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, mysql.UserTable, strings.ToLower(user.Hostname), user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete password history from mysql.password_history.
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, mysql.PasswordHistoryTable, strings.ToLower(user.Hostname), user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete privileges from mysql.global_priv
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, mysql.GlobalPrivTable, user.Hostname, user.Username)
		if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			if _, err := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); err != nil {
				return err
			}
			continue
		}

		// delete privileges from mysql.db
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, mysql.DBTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete privileges from mysql.tables_priv
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, mysql.TablePrivTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete privileges from mysql.columns_priv
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, mysql.ColumnPrivTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete relationship from mysql.role_edges
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE TO_HOST = %? and TO_USER = %?;`, mysql.SystemDB, mysql.RoleEdgeTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE FROM_HOST = %? and FROM_USER = %?;`, mysql.SystemDB, mysql.RoleEdgeTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete relationship from mysql.default_roles
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE DEFAULT_ROLE_HOST = %? and DEFAULT_ROLE_USER = %?;`, mysql.SystemDB, mysql.DefaultRoleTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE HOST = %? and USER = %?;`, mysql.SystemDB, mysql.DefaultRoleTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete relationship from mysql.global_grants
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, "global_grants", user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete from activeRoles
		if s.IsDropRole {
			for i := range activeRoles {
				if activeRoles[i].Username == user.Username && activeRoles[i].Hostname == user.Hostname {
					activeRoles = slices.Delete(activeRoles, i, i+1)
					break
				}
			}
		} // TODO: need delete columns_priv once we implement columns_priv functionality.
	}

	if len(failedUsers) != 0 {
		if _, err := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); err != nil {
			return err
		}
		if s.IsDropRole {
			return exeerrors.ErrCannotUser.GenWithStackByArgs("DROP ROLE", strings.Join(failedUsers, ","))
		}
		return exeerrors.ErrCannotUser.GenWithStackByArgs("DROP USER", strings.Join(failedUsers, ","))
	}
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "commit"); err != nil {
		return err
	}
	if s.IsDropRole {
		// apply new activeRoles
		if ok, roleName := checker.ActiveRoles(ctx, e.Ctx(), activeRoles); !ok {
			u := e.Ctx().GetSessionVars().User
			return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
		}
	}
	userList := userIdentityToUserList(s.UserList)
	return domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege(userList)
}

func userExists(ctx context.Context, sctx sessionctx.Context, name string, host string) (bool, error) {
	exec := sctx.GetRestrictedSQLExecutor()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnPrivilege)
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, `SELECT * FROM %n.%n WHERE User=%? AND Host=%?;`, mysql.SystemDB, mysql.UserTable, name, strings.ToLower(host))
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}

// use the same internal executor to read within the same transaction, otherwise same as userExists
func userExistsInternal(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, name string, host string) (bool, string, error) {
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, `SELECT * FROM %n.%n WHERE User=%? AND Host=%? FOR UPDATE;`, mysql.SystemDB, mysql.UserTable, name, strings.ToLower(host))
	recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	if err != nil {
		return false, "", err
	}
	req := recordSet.NewChunk(nil)
	err = recordSet.Next(ctx, req)
	var rows = 0
	if err == nil {
		rows = req.NumRows()
	}

	var authPlugin string
	colIdx := -1
	for i, f := range recordSet.Fields() {
		if f.ColumnAsName.L == "plugin" {
			colIdx = i
		}
	}
	if rows == 1 {
		// rows can only be 0 or 1
		// When user + host does not exist, the rows is 0
		// When user + host exists, the rows is 1 because user + host is primary key of the table.
		row := req.GetRow(0)
		authPlugin = row.GetString(colIdx)
	}

	errClose := recordSet.Close()
	if errClose != nil {
		return false, "", errClose
	}
	return rows > 0, authPlugin, err
}
