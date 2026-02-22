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
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	pwdValidator "github.com/pingcap/tidb/pkg/util/password-validation"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)


func (e *SimpleExec) isValidatePasswordEnabled() bool {
	validatePwdEnable, err := e.Ctx().GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(vardef.ValidatePasswordEnable)
	if err != nil {
		return false
	}
	return variable.TiDBOptOn(validatePwdEnable)
}

func (e *SimpleExec) executeCreateUser(ctx context.Context, s *ast.CreateUserStmt) error {
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	// Check `CREATE USER` privilege.
	if !config.GetGlobalConfig().Security.SkipGrantTable {
		checker := privilege.GetPrivilegeManager(e.Ctx())
		if checker == nil {
			return errors.New("miss privilege checker")
		}
		activeRoles := e.Ctx().GetSessionVars().ActiveRoles
		if !checker.RequestVerification(activeRoles, mysql.SystemDB, mysql.UserTable, "", mysql.InsertPriv) {
			if s.IsCreateRole {
				if !checker.RequestVerification(activeRoles, "", "", "", mysql.CreateRolePriv) &&
					!checker.RequestVerification(activeRoles, "", "", "", mysql.CreateUserPriv) {
					return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE ROLE or CREATE USER")
				}
			}
			if !s.IsCreateRole && !checker.RequestVerification(activeRoles, "", "", "", mysql.CreateUserPriv) {
				return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE User")
			}
		}
	}

	privData, err := tlsOption2GlobalPriv(s.AuthTokenOrTLSOptions)
	if err != nil {
		return err
	}

	userResource := &resourceOptionsInfo{
		maxQueriesPerHour:     0,
		maxUpdatesPerHour:     0,
		maxConnectionsPerHour: 0,
		maxUserConnections:    0,
	}

	err = userResource.loadResourceOptions(s.ResourceOptions)
	if err != nil {
		return err
	}

	plOptions := &passwordOrLockOptionsInfo{
		lockAccount:                 "N",
		passwordExpired:             "N",
		passwordLifetime:            nil,
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
	passwordLocking := createUserFailedLoginJSON(plOptions)
	if s.IsCreateRole {
		plOptions.lockAccount = "Y"
		plOptions.passwordExpired = "Y"
	}

	var userAttributes []string
	if s.CommentOrAttributeOption != nil {
		if s.CommentOrAttributeOption.Type == ast.UserCommentType {
			userAttributes = append(userAttributes, fmt.Sprintf("\"metadata\": {\"comment\": \"%s\"}", s.CommentOrAttributeOption.Value))
		} else if s.CommentOrAttributeOption.Type == ast.UserAttributeType {
			userAttributes = append(userAttributes, fmt.Sprintf("\"metadata\": %s", s.CommentOrAttributeOption.Value))
		}
	}

	if s.ResourceGroupNameOption != nil {
		if !vardef.EnableResourceControl.Load() {
			return infoschema.ErrResourceGroupSupportDisabled
		}
		if s.IsCreateRole {
			return infoschema.ErrResourceGroupInvalidForRole
		}

		resourceGroupName := strings.ToLower(s.ResourceGroupNameOption.Value)

		// check if specified resource group exists
		if resourceGroupName != resourcegroup.DefaultResourceGroupName && resourceGroupName != "" {
			_, exists := e.is.ResourceGroupByName(ast.NewCIStr(resourceGroupName))
			if !exists {
				return infoschema.ErrResourceGroupNotExists.GenWithStackByArgs(resourceGroupName)
			}
		}
		userAttributes = append(userAttributes, fmt.Sprintf("\"resource_group\": \"%s\"", resourceGroupName))
	}
	// If FAILED_LOGIN_ATTEMPTS and PASSWORD_LOCK_TIME are both specified to 0, a string of 0 length is generated.
	// When inserting the attempts into json, an error occurs. This requires special handling.
	if passwordLocking != "" {
		userAttributes = append(userAttributes, passwordLocking)
	}
	userAttributesStr := fmt.Sprintf("{%s}", strings.Join(userAttributes, ","))

	tokenIssuer := ""
	for _, authTokenOption := range s.AuthTokenOrTLSOptions {
		if authTokenOption.Type == ast.TokenIssuer {
			tokenIssuer = authTokenOption.Value
		}
	}

	sql := new(strings.Builder)
	sqlPasswordHistory := new(strings.Builder)
	passwordInit := true
	// Get changed user password reuse info.
	savePasswdHistory := whetherSavePasswordHistory(plOptions)
	sqlTemplate := "INSERT INTO %n.%n (Host, User, authentication_string, plugin, user_attributes, Account_locked, Token_issuer, Password_expired, Password_lifetime, Max_user_connections, Password_reuse_time, Password_reuse_history) VALUES "
	valueTemplate := "(%?, %?, %?, %?, %?, %?, %?, %?, %?, %?"

	sqlescape.MustFormatSQL(sql, sqlTemplate, mysql.SystemDB, mysql.UserTable)
	if savePasswdHistory {
		sqlescape.MustFormatSQL(sqlPasswordHistory, `INSERT INTO %n.%n (Host, User, Password) VALUES `, mysql.SystemDB, mysql.PasswordHistoryTable)
	}
	defaultAuthPlugin, err := e.Ctx().GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(vardef.DefaultAuthPlugin)
	if err != nil {
		return errors.Trace(err)
	}

	users := make([]*auth.UserIdentity, 0, len(s.Specs))
	for _, spec := range s.Specs {
		if len(spec.User.Username) > auth.UserNameMaxLength {
			return exeerrors.ErrWrongStringLength.GenWithStackByArgs(spec.User.Username, "user name", auth.UserNameMaxLength)
		}
		if len(spec.User.Username) == 0 && plOptions.passwordExpired == "Y" {
			return exeerrors.ErrPasswordExpireAnonymousUser.GenWithStackByArgs()
		}
		if len(spec.User.Hostname) > auth.HostNameMaxLength {
			return exeerrors.ErrWrongStringLength.GenWithStackByArgs(spec.User.Hostname, "host name", auth.HostNameMaxLength)
		}
		if len(users) > 0 {
			sqlescape.MustFormatSQL(sql, ",")
		}
		exists, err1 := userExists(ctx, e.Ctx(), spec.User.Username, spec.User.Hostname)
		if err1 != nil {
			return err1
		}
		if exists {
			user := fmt.Sprintf(`'%s'@'%s'`, spec.User.Username, spec.User.Hostname)
			if !s.IfNotExists {
				if s.IsCreateRole {
					return exeerrors.ErrCannotUser.GenWithStackByArgs("CREATE ROLE", user)
				}
				return exeerrors.ErrCannotUser.GenWithStackByArgs("CREATE USER", user)
			}
			err := infoschema.ErrUserAlreadyExists.FastGenByArgs(user)
			e.Ctx().GetSessionVars().StmtCtx.AppendNote(err)
			continue
		}
		authPlugin := defaultAuthPlugin
		if spec.AuthOpt != nil && spec.AuthOpt.AuthPlugin != "" {
			authPlugin = spec.AuthOpt.AuthPlugin
		}
		// Validate the strength of the password if necessary
		if e.isValidatePasswordEnabled() && !s.IsCreateRole && mysql.IsAuthPluginClearText(authPlugin) {
			pwd := ""
			if spec.AuthOpt != nil {
				pwd = spec.AuthOpt.AuthString
			}
			if err := pwdValidator.ValidatePassword(e.Ctx().GetSessionVars(), pwd); err != nil {
				return err
			}
		}
		var pluginImpl *extension.AuthPlugin

		switch authPlugin {
		case mysql.AuthNativePassword, mysql.AuthCachingSha2Password, mysql.AuthTiDBSM3Password, mysql.AuthSocket, mysql.AuthTiDBAuthToken, mysql.AuthLDAPSimple, mysql.AuthLDAPSASL:
		default:
			found := false
			if extensions, err := extension.GetExtensions(); err != nil {
				return exeerrors.ErrPluginIsNotLoaded.GenWithStack(err.Error())
			} else if pluginImpl, found = extensions.GetAuthPlugins()[authPlugin]; !found {
				// If the plugin is not a registered extension auth plugin, return error
				return exeerrors.ErrPluginIsNotLoaded.GenWithStackByArgs(spec.AuthOpt.AuthPlugin)
			}
		}

		pwd, ok := encodePasswordWithPlugin(*spec, pluginImpl, defaultAuthPlugin)
		if !ok {
			return errors.Trace(exeerrors.ErrPasswordFormat)
		}

		recordTokenIssuer := tokenIssuer
		if len(recordTokenIssuer) > 0 && authPlugin != mysql.AuthTiDBAuthToken {
			err := fmt.Errorf("TOKEN_ISSUER is not needed for '%s' user", authPlugin)
			e.Ctx().GetSessionVars().StmtCtx.AppendWarning(err)
			recordTokenIssuer = ""
		} else if len(recordTokenIssuer) == 0 && authPlugin == mysql.AuthTiDBAuthToken {
			err := fmt.Errorf("TOKEN_ISSUER is needed for 'tidb_auth_token' user, please use 'alter user' to declare it")
			e.Ctx().GetSessionVars().StmtCtx.AppendWarning(err)
		}

		hostName := strings.ToLower(spec.User.Hostname)
		sqlescape.MustFormatSQL(sql, valueTemplate, hostName, spec.User.Username, pwd, authPlugin, userAttributesStr, plOptions.lockAccount, recordTokenIssuer, plOptions.passwordExpired, plOptions.passwordLifetime, userResource.maxUserConnections)
		// add Password_reuse_time value.
		if plOptions.passwordReuseIntervalChange && (plOptions.passwordReuseInterval != notSpecified) {
			sqlescape.MustFormatSQL(sql, `, %?`, plOptions.passwordReuseInterval)
		} else {
			sqlescape.MustFormatSQL(sql, `, %?`, nil)
		}
		// add Password_reuse_history value.
		if plOptions.passwordHistoryChange && (plOptions.passwordHistory != notSpecified) {
			sqlescape.MustFormatSQL(sql, `, %?`, plOptions.passwordHistory)
		} else {
			sqlescape.MustFormatSQL(sql, `, %?`, nil)
		}
		sqlescape.MustFormatSQL(sql, `)`)
		// The empty password does not count in the password history and is subject to reuse at any time.
		// AuthTiDBAuthToken is the token login method on the cloud,
		// and the Password Reuse Policy does not take effect.
		if savePasswdHistory && len(pwd) != 0 && !strings.EqualFold(authPlugin, mysql.AuthTiDBAuthToken) {
			if !passwordInit {
				sqlescape.MustFormatSQL(sqlPasswordHistory, ",")
			} else {
				passwordInit = false
			}
			sqlescape.MustFormatSQL(sqlPasswordHistory, `( %?, %?, %?)`, hostName, spec.User.Username, pwd)
		}
		users = append(users, spec.User)
	}
	if len(users) == 0 {
		return nil
	}

	restrictedCtx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(internalCtx, restrictedCtx)
	sqlExecutor := restrictedCtx.GetSQLExecutor()

	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "begin"); err != nil {
		return errors.Trace(err)
	}
	_, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String())
	if err != nil {
		logutil.BgLogger().Warn("Fail to create user", zap.String("sql", sql.String()))
		if _, rollbackErr := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); rollbackErr != nil {
			return rollbackErr
		}
		return err
	}

	if savePasswdHistory && !passwordInit {
		_, err = sqlExecutor.ExecuteInternal(internalCtx, sqlPasswordHistory.String())
		if err != nil {
			if _, rollbackErr := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); rollbackErr != nil {
				return errors.Trace(rollbackErr)
			}
			return errors.Trace(err)
		}
	}

	if len(privData) != 0 {
		sql.Reset()
		sqlescape.MustFormatSQL(sql, "INSERT IGNORE INTO %n.%n (Host, User, Priv) VALUES ", mysql.SystemDB, mysql.GlobalPrivTable)
		for i, user := range users {
			if i > 0 {
				sqlescape.MustFormatSQL(sql, ",")
			}
			sqlescape.MustFormatSQL(sql, `(%?, %?, %?)`, user.Hostname, user.Username, string(hack.String(privData)))
		}
		_, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String())
		if err != nil {
			if _, rollbackErr := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
	}
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "commit"); err != nil {
		return errors.Trace(err)
	}
	userList := userIdentityToUserList(users)
	return domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege(userList)
}

func isRole(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, name, host string) (bool, error) {
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, `SELECT 1 FROM %n.%n WHERE User=%? AND Host=%? AND Account_locked="Y" AND Password_expired="Y";`,
		mysql.SystemDB, mysql.UserTable, name, strings.ToLower(host))
	recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	if err != nil {
		return false, err
	}
	rows, err := sqlexec.DrainRecordSetAndClose(ctx, recordSet, 1)
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}

func getUserPasswordLimit(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, name string, host string, plOptions *passwordOrLockOptionsInfo) (pRI *passwordReuseInfo, err error) {
	res := &passwordReuseInfo{notSpecified, notSpecified}
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, `SELECT Password_reuse_history,Password_reuse_time FROM %n.%n WHERE User=%? AND Host=%?;`,
		mysql.SystemDB, mysql.UserTable, name, strings.ToLower(host))
	// Query the specified user password reuse rules.
	recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	if err != nil {
		return nil, err
	}
	rows, err := sqlexec.DrainRecordSetAndClose(ctx, recordSet, 3)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		if !row.IsNull(0) {
			res.passwordHistory = int64(row.GetUint64(0))
		} else {
			res.passwordHistory = vardef.PasswordHistory.Load()
		}
		if !row.IsNull(1) {
			res.passwordReuseInterval = int64(row.GetUint64(1))
		} else {
			res.passwordReuseInterval = vardef.PasswordReuseInterval.Load()
		}
	}
	if plOptions.passwordHistoryChange {
		// If the user specifies a default, the global variable needs to be re-read.
		if plOptions.passwordHistory != notSpecified {
			res.passwordHistory = plOptions.passwordHistory
		} else {
			res.passwordHistory = vardef.PasswordHistory.Load()
		}
	}
	if plOptions.passwordReuseIntervalChange {
		// If the user specifies a default, the global variable needs to be re-read.
		if plOptions.passwordReuseInterval != notSpecified {
			res.passwordReuseInterval = plOptions.passwordReuseInterval
		} else {
			res.passwordReuseInterval = vardef.PasswordReuseInterval.Load()
		}
	}
	return res, nil
}

// getValidTime get the boundary of password valid time.
func getValidTime(sctx sessionctx.Context, passwordReuse *passwordReuseInfo) string {
	nowTime := time.Now().In(sctx.GetSessionVars().TimeZone)
	nowTimeS := nowTime.Unix()
	beforeTimeS := max(nowTimeS-passwordReuse.passwordReuseInterval*24*int64(time.Hour/time.Second), 0)
	return time.Unix(beforeTimeS, 0).Format("2006-01-02 15:04:05.999999999")
}

// deleteHistoricalData delete useless password history.
// The deleted password must meet the following conditions at the same time.
// 1. Exceeded the maximum number of saves.
// 2. The password has exceeded the prohibition time.
func deleteHistoricalData(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, userDetail *userInfo, maxDelRows int64, passwordReuse *passwordReuseInfo, sctx sessionctx.Context) error {
	//never times out or no row need delete.
	if (passwordReuse.passwordReuseInterval > math.MaxInt32) || maxDelRows == 0 {
		return nil
	}
	sql := new(strings.Builder)
	// no prohibition time.
	if passwordReuse.passwordReuseInterval == 0 {
		deleteTemplate := `DELETE from %n.%n WHERE User= %? AND Host= %? order by Password_timestamp ASC LIMIT `
		deleteTemplate = deleteTemplate + strconv.FormatInt(maxDelRows, 10)
		sqlescape.MustFormatSQL(sql, deleteTemplate, mysql.SystemDB, mysql.PasswordHistoryTable,
			userDetail.user, strings.ToLower(userDetail.host))
		_, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
		if err != nil {
			return err
		}
	} else {
		beforeDate := getValidTime(sctx, passwordReuse)
		// Deletion must satisfy 1. Exceed the prohibition time 2. Exceed the maximum number of saved records.
		deleteTemplate := `DELETE from %n.%n WHERE User= %? AND Host= %? AND Password_timestamp < %? order by Password_timestamp ASC LIMIT `
		deleteTemplate = deleteTemplate + strconv.FormatInt(maxDelRows, 10)
		sql.Reset()
		sqlescape.MustFormatSQL(sql, deleteTemplate, mysql.SystemDB, mysql.PasswordHistoryTable,
			userDetail.user, strings.ToLower(userDetail.host), beforeDate)
		_, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
		if err != nil {
			return err
		}
	}
	return nil
}


