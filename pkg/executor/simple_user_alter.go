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
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/hack"
	pwdValidator "github.com/pingcap/tidb/pkg/util/password-validation"
	sem "github.com/pingcap/tidb/pkg/util/sem/compat"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
)

func (e *SimpleExec) executeAlterUser(ctx context.Context, s *ast.AlterUserStmt) error {
	disableSandBoxMode := false
	var err error
	if e.Ctx().InSandBoxMode() {
		if err = e.checkSandboxMode(s.Specs); err != nil {
			return err
		}
		disableSandBoxMode = true
	}
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnPrivilege)
	if s.CurrentAuth != nil {
		user := e.Ctx().GetSessionVars().User
		if user == nil {
			return errors.New("Session user is empty")
		}
		// Use AuthHostname to search the user record, set Hostname as AuthHostname.
		userCopy := *user
		userCopy.Hostname = userCopy.AuthHostname
		spec := &ast.UserSpec{
			User:    &userCopy,
			AuthOpt: s.CurrentAuth,
		}
		s.Specs = []*ast.UserSpec{spec}
	}

	userResource := &resourceOptionsInfo{
		maxQueriesPerHour:     0,
		maxUpdatesPerHour:     0,
		maxConnectionsPerHour: 0,
		// can't set 0 to maxUserConnections as default, because user could set 0 to this field.
		// so we use -1(invalid value) as default.
		maxUserConnections: -1,
	}

	err = userResource.loadResourceOptions(s.ResourceOptions)
	if err != nil {
		return err
	}

	plOptions := passwordOrLockOptionsInfo{
		lockAccount:                 "",
		passwordExpired:             "",
		passwordLifetime:            notSpecified,
		passwordHistory:             notSpecified,
		passwordReuseInterval:       notSpecified,
		failedLoginAttemptsChange:   false,
		passwordLockTimeChange:      false,
		passwordHistoryChange:       false,
		passwordReuseIntervalChange: false,
	}
	err = plOptions.loadOptions(s.PasswordOrLockOptions)
	if err != nil {
		return err
	}

	privData, err := tlsOption2GlobalPriv(s.AuthTokenOrTLSOptions)
	if err != nil {
		return err
	}

	failedUsers := make([]string, 0, len(s.Specs))
	needRollback := false
	checker := privilege.GetPrivilegeManager(e.Ctx())
	if checker == nil {
		return errors.New("could not load privilege checker")
	}
	activeRoles := e.Ctx().GetSessionVars().ActiveRoles
	hasCreateUserPriv := checker.RequestVerification(activeRoles, "", "", "", mysql.CreateUserPriv)
	hasSystemUserPriv := checker.RequestDynamicVerification(activeRoles, "SYSTEM_USER", false)
	hasRestrictedUserPriv := checker.RequestDynamicVerification(activeRoles, "RESTRICTED_USER_ADMIN", false)
	hasSystemSchemaPriv := checker.RequestVerification(activeRoles, mysql.SystemDB, mysql.UserTable, "", mysql.UpdatePriv)

	var authTokenOptions []*ast.AuthTokenOrTLSOption
	for _, authTokenOrTLSOption := range s.AuthTokenOrTLSOptions {
		if authTokenOrTLSOption.Type == ast.TokenIssuer {
			authTokenOptions = append(authTokenOptions, authTokenOrTLSOption)
		}
	}

	sysSession, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(ctx, sysSession)
	sqlExecutor := sysSession.GetSQLExecutor()
	// session isolation level changed to READ-COMMITTED.
	// When tidb is at the RR isolation level, executing `begin` will obtain a consistent state.
	// When operating the same user concurrently, it may happen that historical versions are read.
	// In order to avoid this risk, change the isolation level to RC.
	_, err = sqlExecutor.ExecuteInternal(ctx, "set tx_isolation = 'READ-COMMITTED'")
	if err != nil {
		return err
	}
	if _, err := sqlExecutor.ExecuteInternal(ctx, "BEGIN PESSIMISTIC"); err != nil {
		return err
	}

	for _, spec := range s.Specs {
		user := e.Ctx().GetSessionVars().User
		alterCurrentUser := spec.User.CurrentUser || ((user != nil) && (user.Username == spec.User.Username) && (user.AuthHostname == spec.User.Hostname))
		alterPassword := false
		if spec.AuthOpt != nil && spec.AuthOpt.AuthPlugin == "" {
			if len(s.AuthTokenOrTLSOptions) == 0 && len(s.ResourceOptions) == 0 && len(s.PasswordOrLockOptions) == 0 {
				alterPassword = true
			}
		}
		if alterCurrentUser && alterPassword {
			spec.User.Username = user.Username
			spec.User.Hostname = user.AuthHostname
		} else {
			// The user executing the query (user) does not match the user specified (spec.User)
			// The MySQL manual states:
			// "In most cases, ALTER USER requires the global CREATE USER privilege, or the UPDATE privilege for the mysql system schema"
			//
			// This is true unless the user being modified has the SYSTEM_USER dynamic privilege.
			// See: https://mysqlserverteam.com/the-system_user-dynamic-privilege/
			//
			// In the current implementation of DYNAMIC privileges, SUPER can be used as a substitute for any DYNAMIC privilege
			// (unless SEM is enabled; in which case RESTRICTED_* privileges will not use SUPER as a substitute). This is intentional
			// because visitInfo can not accept OR conditions for permissions and in many cases MySQL permits SUPER instead.

			// Thus, any user with SUPER can effectively ALTER/DROP a SYSTEM_USER, and
			// any user with only CREATE USER can not modify the properties of users with SUPER privilege.
			// We extend this in TiDB with SEM, where SUPER users can not modify users with RESTRICTED_USER_ADMIN.
			// For simplicity: RESTRICTED_USER_ADMIN also counts for SYSTEM_USER here.

			if !(hasCreateUserPriv || hasSystemSchemaPriv) {
				return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
			}
			if !(hasSystemUserPriv || hasRestrictedUserPriv) && checker.RequestDynamicVerificationWithUser(ctx, "SYSTEM_USER", false, spec.User) {
				return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SYSTEM_USER or SUPER")
			}
			if sem.IsEnabled() && !hasRestrictedUserPriv && checker.RequestDynamicVerificationWithUser(ctx, "RESTRICTED_USER_ADMIN", false, spec.User) {
				return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("RESTRICTED_USER_ADMIN")
			}
		}

		exists, currentAuthPlugin, err := userExistsInternal(ctx, sqlExecutor, spec.User.Username, spec.User.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			user := fmt.Sprintf(`'%s'@'%s'`, spec.User.Username, spec.User.Hostname)
			failedUsers = append(failedUsers, user)
			continue
		}

		type AuthTokenOptionHandler int
		const (
			// noNeedAuthTokenOptions means the final auth plugin is NOT tidb_auth_plugin
			noNeedAuthTokenOptions AuthTokenOptionHandler = iota
			// OptionalAuthTokenOptions means the final auth_plugin is tidb_auth_plugin,
			// and whether to declare AuthTokenOptions or not is ok.
			OptionalAuthTokenOptions
			// RequireAuthTokenOptions means the final auth_plugin is tidb_auth_plugin and need AuthTokenOptions here
			RequireAuthTokenOptions
		)
		authTokenOptionHandler := noNeedAuthTokenOptions
		if currentAuthPlugin == mysql.AuthTiDBAuthToken {
			authTokenOptionHandler = OptionalAuthTokenOptions
		}

		type alterField struct {
			expr  string
			value any
		}
		var fields []alterField
		if spec.AuthOpt != nil {
			fields = append(fields, alterField{"password_last_changed=current_timestamp()", nil})
			if spec.AuthOpt.AuthPlugin == "" {
				spec.AuthOpt.AuthPlugin = currentAuthPlugin
			}
			extensions, err := extension.GetExtensions()
			if err != nil {
				return exeerrors.ErrPluginIsNotLoaded.GenWithStackByArgs(err.Error())
			}
			authPlugins := extensions.GetAuthPlugins()
			var authPluginImpl *extension.AuthPlugin
			switch spec.AuthOpt.AuthPlugin {
			case mysql.AuthNativePassword, mysql.AuthCachingSha2Password, mysql.AuthTiDBSM3Password, mysql.AuthSocket, mysql.AuthLDAPSimple, mysql.AuthLDAPSASL, "":
				authTokenOptionHandler = noNeedAuthTokenOptions
			case mysql.AuthTiDBAuthToken:
				if authTokenOptionHandler != OptionalAuthTokenOptions {
					authTokenOptionHandler = RequireAuthTokenOptions
				}
			default:
				found := false
				if authPluginImpl, found = authPlugins[spec.AuthOpt.AuthPlugin]; !found {
					return exeerrors.ErrPluginIsNotLoaded.GenWithStackByArgs(spec.AuthOpt.AuthPlugin)
				}
			}
			// changing the auth method prunes history.
			if spec.AuthOpt.AuthPlugin != currentAuthPlugin {
				// delete password history from mysql.password_history.
				sql := new(strings.Builder)
				sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, mysql.PasswordHistoryTable, spec.User.Hostname, spec.User.Username)
				if _, err := sqlExecutor.ExecuteInternal(ctx, sql.String()); err != nil {
					failedUsers = append(failedUsers, spec.User.String())
					needRollback = true
					break
				}
			}
			if e.isValidatePasswordEnabled() && spec.AuthOpt.ByAuthString && mysql.IsAuthPluginClearText(spec.AuthOpt.AuthPlugin) {
				if err := pwdValidator.ValidatePassword(e.Ctx().GetSessionVars(), spec.AuthOpt.AuthString); err != nil {
					return err
				}
			}
			// we have assigned the currentAuthPlugin to spec.AuthOpt.AuthPlugin if the latter is empty, so keep the incomming argument defaultPlugin empty is ok.
			pwd, ok := encodePasswordWithPlugin(*spec, authPluginImpl, "")
			if !ok {
				return errors.Trace(exeerrors.ErrPasswordFormat)
			}
			// for Support Password Reuse Policy.
			// The empty password does not count in the password history and is subject to reuse at any time.
			// https://dev.mysql.com/doc/refman/8.0/en/password-management.html#password-reuse-policy
			if len(pwd) != 0 {
				userDetail := &userInfo{
					host:       spec.User.Hostname,
					user:       spec.User.Username,
					pLI:        &plOptions,
					pwd:        pwd,
					authString: spec.AuthOpt.AuthString,
				}
				err := checkPasswordReusePolicy(ctx, sqlExecutor, userDetail, e.Ctx(), spec.AuthOpt.AuthPlugin, authPlugins)
				if err != nil {
					return err
				}
			}
			fields = append(fields, alterField{"authentication_string=%?", pwd})
			if spec.AuthOpt.AuthPlugin != "" {
				fields = append(fields, alterField{"plugin=%?", spec.AuthOpt.AuthPlugin})
			}
			if spec.AuthOpt.ByAuthString || spec.AuthOpt.ByHashString {
				if plOptions.passwordExpired == "" {
					plOptions.passwordExpired = "N"
				}
			}
		}

		if len(plOptions.lockAccount) != 0 {
			fields = append(fields, alterField{"account_locked=%?", plOptions.lockAccount})
		}

		// support alter Password_reuse_history and Password_reuse_time.
		if plOptions.passwordHistoryChange {
			if plOptions.passwordHistory == notSpecified {
				fields = append(fields, alterField{"Password_reuse_history = NULL ", ""})
			} else {
				fields = append(fields, alterField{"Password_reuse_history = %? ", strconv.FormatInt(plOptions.passwordHistory, 10)})
			}
		}
		if plOptions.passwordReuseIntervalChange {
			if plOptions.passwordReuseInterval == notSpecified {
				fields = append(fields, alterField{"Password_reuse_time = NULL ", ""})
			} else {
				fields = append(fields, alterField{"Password_reuse_time = %? ", strconv.FormatInt(plOptions.passwordReuseInterval, 10)})
			}
		}

		passwordLockingInfo, err := readPasswordLockingInfo(ctx, sqlExecutor, spec.User.Username, spec.User.Hostname, &plOptions)
		if err != nil {
			return err
		}
		passwordLockingStr := alterUserFailedLoginJSON(passwordLockingInfo, plOptions.lockAccount)

		if len(plOptions.passwordExpired) != 0 {
			if len(spec.User.Username) == 0 && plOptions.passwordExpired == "Y" {
				return exeerrors.ErrPasswordExpireAnonymousUser.GenWithStackByArgs()
			}
			fields = append(fields, alterField{"password_expired=%?", plOptions.passwordExpired})
		}
		if plOptions.passwordLifetime != notSpecified {
			fields = append(fields, alterField{"password_lifetime=%?", plOptions.passwordLifetime})
		}

		if userResource.maxUserConnections >= 0 {
			// need `CREATE USER` privilege for the operation of modifying max_user_connections.
			if !hasCreateUserPriv {
				return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
			}
			fields = append(fields, alterField{"max_user_connections=%?", userResource.maxUserConnections})
		}

		var newAttributes []string
		if s.CommentOrAttributeOption != nil {
			if s.CommentOrAttributeOption.Type == ast.UserCommentType {
				newAttributes = append(newAttributes, fmt.Sprintf(`"metadata": {"comment": "%s"}`, s.CommentOrAttributeOption.Value))
			} else {
				newAttributes = append(newAttributes, fmt.Sprintf(`"metadata": %s`, s.CommentOrAttributeOption.Value))
			}
		}
		if s.ResourceGroupNameOption != nil {
			if !vardef.EnableResourceControl.Load() {
				return infoschema.ErrResourceGroupSupportDisabled
			}
			is, err := isRole(ctx, sqlExecutor, spec.User.Username, spec.User.Hostname)
			if err != nil {
				return err
			}
			if is {
				return infoschema.ErrResourceGroupInvalidForRole
			}

			// check if specified resource group exists
			resourceGroupName := strings.ToLower(s.ResourceGroupNameOption.Value)
			if resourceGroupName != resourcegroup.DefaultResourceGroupName && s.ResourceGroupNameOption.Value != "" {
				_, exists := e.is.ResourceGroupByName(ast.NewCIStr(resourceGroupName))
				if !exists {
					return infoschema.ErrResourceGroupNotExists.GenWithStackByArgs(resourceGroupName)
				}
			}

			newAttributes = append(newAttributes, fmt.Sprintf(`"resource_group": "%s"`, resourceGroupName))
		}
		if passwordLockingStr != "" {
			newAttributes = append(newAttributes, passwordLockingStr)
		}
		if length := len(newAttributes); length > 0 {
			if length > 1 || passwordLockingStr == "" {
				passwordLockingInfo.containsNoOthers = false
			}
			newAttributesStr := fmt.Sprintf("{%s}", strings.Join(newAttributes, ","))
			fields = append(fields, alterField{"user_attributes=json_merge_patch(coalesce(user_attributes, '{}'), %?)", newAttributesStr})
		}

		switch authTokenOptionHandler {
		case noNeedAuthTokenOptions:
			if len(authTokenOptions) > 0 {
				err := errors.NewNoStackError("TOKEN_ISSUER is not needed for the auth plugin")
				e.Ctx().GetSessionVars().StmtCtx.AppendWarning(err)
			}
		case OptionalAuthTokenOptions:
			if len(authTokenOptions) > 0 {
				for _, authTokenOption := range authTokenOptions {
					fields = append(fields, alterField{authTokenOption.Type.String() + "=%?", authTokenOption.Value})
				}
			}
		case RequireAuthTokenOptions:
			if len(authTokenOptions) > 0 {
				for _, authTokenOption := range authTokenOptions {
					fields = append(fields, alterField{authTokenOption.Type.String() + "=%?", authTokenOption.Value})
				}
			} else {
				err := errors.NewNoStackError("Auth plugin 'tidb_auth_plugin' needs TOKEN_ISSUER")
				e.Ctx().GetSessionVars().StmtCtx.AppendWarning(err)
			}
		}

		if len(fields) > 0 {
			sql := new(strings.Builder)
			sqlescape.MustFormatSQL(sql, "UPDATE %n.%n SET ", mysql.SystemDB, mysql.UserTable)
			for i, f := range fields {
				sqlescape.MustFormatSQL(sql, f.expr, f.value)
				if i < len(fields)-1 {
					sqlescape.MustFormatSQL(sql, ",")
				}
			}
			sqlescape.MustFormatSQL(sql, " WHERE Host=%? and User=%?;", spec.User.Hostname, spec.User.Username)
			_, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
			if err != nil {
				failedUsers = append(failedUsers, spec.User.String())
				needRollback = true
				continue
			}
		}

		// Remove useless Password_locking from User_attributes.
		err = deletePasswordLockingAttribute(ctx, sqlExecutor, spec.User.Username, spec.User.Hostname, passwordLockingInfo)
		if err != nil {
			failedUsers = append(failedUsers, spec.User.String())
			needRollback = true
			continue
		}

		if len(privData) > 0 {
			sql := new(strings.Builder)
			sqlescape.MustFormatSQL(sql, "INSERT INTO %n.%n (Host, User, Priv) VALUES (%?,%?,%?) ON DUPLICATE KEY UPDATE Priv = values(Priv)", mysql.SystemDB, mysql.GlobalPrivTable, spec.User.Hostname, spec.User.Username, string(hack.String(privData)))
			_, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
			if err != nil {
				failedUsers = append(failedUsers, spec.User.String())
				needRollback = true
			}
		}
	}
	if len(failedUsers) > 0 {
		// Compatible with MySQL 8.0, `ALTER USER` realizes atomic operation.
		if !s.IfExists || needRollback {
			return exeerrors.ErrCannotUser.GenWithStackByArgs("ALTER USER", strings.Join(failedUsers, ","))
		}
		for _, user := range failedUsers {
			err := infoschema.ErrUserDropExists.FastGenByArgs(user)
			e.Ctx().GetSessionVars().StmtCtx.AppendNote(err)
		}
	}
	if _, err := sqlExecutor.ExecuteInternal(ctx, "commit"); err != nil {
		return err
	}
	users := userSpecToUserList(s.Specs)
	if err = domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege(users); err != nil {
		return err
	}
	if disableSandBoxMode {
		e.Ctx().DisableSandBoxMode()
	}
	return nil
}

func (e *SimpleExec) checkSandboxMode(specs []*ast.UserSpec) error {
	for _, spec := range specs {
		if spec.AuthOpt == nil {
			continue
		}
		if spec.AuthOpt.ByAuthString || spec.AuthOpt.ByHashString {
			if spec.User.CurrentUser || e.Ctx().GetSessionVars().User.Username == spec.User.Username {
				return nil
			}
		}
	}
	return exeerrors.ErrMustChangePassword.GenWithStackByArgs()
}
