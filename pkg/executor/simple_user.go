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
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	pwdValidator "github.com/pingcap/tidb/pkg/util/password-validation"
	sem "github.com/pingcap/tidb/pkg/util/sem/compat"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

func (info *resourceOptionsInfo) loadResourceOptions(userResource []*ast.ResourceOption) error {
	for _, option := range userResource {
		switch option.Type {
		case ast.MaxQueriesPerHour:
			info.maxQueriesPerHour = min(option.Count, math.MaxInt16)
		case ast.MaxUpdatesPerHour:
			info.maxUpdatesPerHour = min(option.Count, math.MaxInt16)
		case ast.MaxConnectionsPerHour:
			info.maxConnectionsPerHour = min(option.Count, math.MaxInt16)
		case ast.MaxUserConnections:
			info.maxUserConnections = min(option.Count, math.MaxInt16)
		}
	}
	return nil
}

func whetherSavePasswordHistory(plOptions *passwordOrLockOptionsInfo) bool {
	var passwdSaveNum, passwdSaveTime int64
	// If the user specifies a default, read the global variable.
	if plOptions.passwordHistoryChange && plOptions.passwordHistory != notSpecified {
		passwdSaveNum = plOptions.passwordHistory
	} else {
		passwdSaveNum = vardef.PasswordHistory.Load()
	}
	if plOptions.passwordReuseIntervalChange && plOptions.passwordReuseInterval != notSpecified {
		passwdSaveTime = plOptions.passwordReuseInterval
	} else {
		passwdSaveTime = vardef.PasswordReuseInterval.Load()
	}
	return passwdSaveTime > 0 || passwdSaveNum > 0
}

type alterUserPasswordLocking struct {
	failedLoginAttempts            int64
	passwordLockTime               int64
	failedLoginAttemptsNotFound    bool
	passwordLockTimeChangeNotFound bool
	// containsNoOthers indicates whether User_attributes only contains one "Password_locking" element.
	containsNoOthers bool
}

func (info *passwordOrLockOptionsInfo) loadOptions(plOption []*ast.PasswordOrLockOption) error {
	if length := len(plOption); length > 0 {
		// If "PASSWORD EXPIRE ..." appears many times,
		// only the last declaration takes effect.
	Loop:
		for i := length - 1; i >= 0; i-- {
			switch plOption[i].Type {
			case ast.PasswordExpire:
				info.passwordExpired = "Y"
				break Loop
			case ast.PasswordExpireDefault:
				info.passwordLifetime = nil
				break Loop
			case ast.PasswordExpireNever:
				info.passwordLifetime = 0
				break Loop
			case ast.PasswordExpireInterval:
				if plOption[i].Count == 0 || plOption[i].Count > math.MaxUint16 {
					return types.ErrWrongValue2.GenWithStackByArgs("DAY", fmt.Sprintf("%v", plOption[i].Count))
				}
				info.passwordLifetime = plOption[i].Count
				break Loop
			}
		}
	}
	// only the last declaration takes effect.
	for _, option := range plOption {
		switch option.Type {
		case ast.Lock:
			info.lockAccount = "Y"
		case ast.Unlock:
			info.lockAccount = "N"
		case ast.FailedLoginAttempts:
			info.failedLoginAttempts = min(option.Count, math.MaxInt16)
			info.failedLoginAttemptsChange = true
		case ast.PasswordLockTime:
			info.passwordLockTime = min(option.Count, math.MaxInt16)
			info.passwordLockTimeChange = true
		case ast.PasswordLockTimeUnbounded:
			info.passwordLockTime = -1
			info.passwordLockTimeChange = true
		case ast.PasswordHistory:
			info.passwordHistory = min(option.Count, math.MaxUint16)
			info.passwordHistoryChange = true
		case ast.PasswordHistoryDefault:
			info.passwordHistory = notSpecified
			info.passwordHistoryChange = true
		case ast.PasswordReuseInterval:
			info.passwordReuseInterval = min(option.Count, math.MaxUint16)
			info.passwordReuseIntervalChange = true
		case ast.PasswordReuseDefault:
			info.passwordReuseInterval = notSpecified
			info.passwordReuseIntervalChange = true
		}
	}
	return nil
}

func createUserFailedLoginJSON(info *passwordOrLockOptionsInfo) string {
	// Record only when either failedLoginAttempts and passwordLockTime is not 0
	if (info.failedLoginAttemptsChange && info.failedLoginAttempts != 0) || (info.passwordLockTimeChange && info.passwordLockTime != 0) {
		return fmt.Sprintf("\"Password_locking\": {\"failed_login_attempts\": %d,\"password_lock_time_days\": %d}",
			info.failedLoginAttempts, info.passwordLockTime)
	}
	return ""
}

func alterUserFailedLoginJSON(info *alterUserPasswordLocking, lockAccount string) string {
	// alterUserPasswordLocking is the user's actual configuration.
	var passwordLockingArray []string
	if info.failedLoginAttempts != 0 || info.passwordLockTime != 0 {
		if lockAccount == "N" {
			passwordLockingArray = append(passwordLockingArray,
				fmt.Sprintf("\"auto_account_locked\": \"%s\"", lockAccount),
				fmt.Sprintf("\"auto_locked_last_changed\": \"%s\"", time.Now().Format(time.UnixDate)),
				fmt.Sprintf("\"failed_login_count\": %d", 0))
		}
		passwordLockingArray = append(passwordLockingArray,
			fmt.Sprintf("\"failed_login_attempts\": %d", info.failedLoginAttempts),
			fmt.Sprintf("\"password_lock_time_days\": %d", info.passwordLockTime))
	}
	if len(passwordLockingArray) > 0 {
		return fmt.Sprintf("\"Password_locking\": {%s}", strings.Join(passwordLockingArray, ","))
	}
	return ""
}

func readPasswordLockingInfo(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, name string, host string, pLO *passwordOrLockOptionsInfo) (aUPL *alterUserPasswordLocking, err error) {
	alterUserInfo := &alterUserPasswordLocking{
		failedLoginAttempts:            0,
		passwordLockTime:               0,
		failedLoginAttemptsNotFound:    false,
		passwordLockTimeChangeNotFound: false,
		containsNoOthers:               false,
	}
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, `SELECT JSON_UNQUOTE(JSON_EXTRACT(user_attributes, '$.Password_locking.failed_login_attempts')),
        JSON_UNQUOTE(JSON_EXTRACT(user_attributes, '$.Password_locking.password_lock_time_days')),
	    JSON_LENGTH(JSON_REMOVE(user_attributes, '$.Password_locking')) FROM %n.%n WHERE User=%? AND Host=%?;`,
		mysql.SystemDB, mysql.UserTable, name, strings.ToLower(host))
	recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	if err != nil {
		return nil, err
	}
	rows, err := sqlexec.DrainRecordSetAndClose(ctx, recordSet, 3)
	if err != nil {
		return nil, err
	}

	// Configuration priority is User Changes > User History
	if pLO.failedLoginAttemptsChange {
		alterUserInfo.failedLoginAttempts = pLO.failedLoginAttempts
	} else if !rows[0].IsNull(0) {
		str := rows[0].GetString(0)
		alterUserInfo.failedLoginAttempts, err = strconv.ParseInt(str, 10, 64)
		if err != nil {
			return nil, err
		}
		alterUserInfo.failedLoginAttempts = max(alterUserInfo.failedLoginAttempts, 0)
		alterUserInfo.failedLoginAttempts = min(alterUserInfo.failedLoginAttempts, math.MaxInt16)
	} else {
		alterUserInfo.failedLoginAttemptsNotFound = true
	}

	if pLO.passwordLockTimeChange {
		alterUserInfo.passwordLockTime = pLO.passwordLockTime
	} else if !rows[0].IsNull(1) {
		str := rows[0].GetString(1)
		alterUserInfo.passwordLockTime, err = strconv.ParseInt(str, 10, 64)
		if err != nil {
			return nil, err
		}
		alterUserInfo.passwordLockTime = max(alterUserInfo.passwordLockTime, -1)
		alterUserInfo.passwordLockTime = min(alterUserInfo.passwordLockTime, math.MaxInt16)
	} else {
		alterUserInfo.passwordLockTimeChangeNotFound = true
	}

	alterUserInfo.containsNoOthers = rows[0].IsNull(2) || rows[0].GetInt64(2) == 0
	return alterUserInfo, nil
}

// deletePasswordLockingAttribute deletes "$.Password_locking" in "User_attributes" when failedLoginAttempts and passwordLockTime both 0.
func deletePasswordLockingAttribute(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, name string, host string, alterUser *alterUserPasswordLocking) error {
	// No password_locking information.
	if alterUser.failedLoginAttemptsNotFound && alterUser.passwordLockTimeChangeNotFound {
		return nil
	}
	// Password_locking information is still in used.
	if alterUser.failedLoginAttempts != 0 || alterUser.passwordLockTime != 0 {
		return nil
	}
	sql := new(strings.Builder)
	if alterUser.containsNoOthers {
		// If we use JSON_REMOVE(user_attributes, '$.Password_locking') directly here, the result is not compatible with MySQL.
		sqlescape.MustFormatSQL(sql, `UPDATE %n.%n SET user_attributes=NULL`, mysql.SystemDB, mysql.UserTable)
	} else {
		sqlescape.MustFormatSQL(sql, `UPDATE %n.%n SET user_attributes=JSON_REMOVE(user_attributes, '$.Password_locking') `, mysql.SystemDB, mysql.UserTable)
	}
	sqlescape.MustFormatSQL(sql, " WHERE Host=%? and User=%?;", host, name)
	_, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	return err
}

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
