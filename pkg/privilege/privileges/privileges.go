// Copyright 2015 PingCAP, Inc.
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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	jwtRepo "github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/lestrrat-go/jwx/v2/jwt/openid"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/privilege/conn"
	"github.com/pingcap/tidb/pkg/privilege/privileges/ldap"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/sem"
	"go.uber.org/zap"
)

// SkipWithGrant causes the server to start without using the privilege system at all.
var SkipWithGrant = false

var _ privilege.Manager = (*UserPrivileges)(nil)
var dynamicPrivs = []string{
	"BACKUP_ADMIN",
	"RESTORE_ADMIN",
	"SYSTEM_USER",
	"SYSTEM_VARIABLES_ADMIN",
	"ROLE_ADMIN",
	"CONNECTION_ADMIN",
	"PLACEMENT_ADMIN",                 // Can Create/Drop/Alter PLACEMENT POLICY
	"DASHBOARD_CLIENT",                // Can login to the TiDB-Dashboard.
	"RESTRICTED_TABLES_ADMIN",         // Can see system tables when SEM is enabled
	"RESTRICTED_STATUS_ADMIN",         // Can see all status vars when SEM is enabled.
	"RESTRICTED_VARIABLES_ADMIN",      // Can see all variables when SEM is enabled
	"RESTRICTED_USER_ADMIN",           // User can not have their access revoked by SUPER users.
	"RESTRICTED_CONNECTION_ADMIN",     // Can not be killed by PROCESS/CONNECTION_ADMIN privilege
	"RESTRICTED_REPLICA_WRITER_ADMIN", // Can write to the sever even when tidb_restriced_read_only is turned on.
	"RESOURCE_GROUP_ADMIN",            // Create/Drop/Alter RESOURCE GROUP
}
var dynamicPrivLock sync.Mutex
var defaultTokenLife = 15 * time.Minute

// UserPrivileges implements privilege.Manager interface.
// This is used to check privilege for the current user.
type UserPrivileges struct {
	user string
	host string
	*Handle
	extensionAccessCheckFuncs []extension.AccessCheckFunc
}

// NewUserPrivileges creates a new UserPrivileges
func NewUserPrivileges(handle *Handle, extension *extension.Extensions) *UserPrivileges {
	return &UserPrivileges{
		Handle:                    handle,
		extensionAccessCheckFuncs: extension.GetAccessCheckFuncs(),
	}
}

// RequestDynamicVerificationWithUser implements the Manager interface.
func (p *UserPrivileges) RequestDynamicVerificationWithUser(privName string, grantable bool, user *auth.UserIdentity) bool {
	if SkipWithGrant {
		return true
	}

	if user == nil {
		return false
	}

	mysqlPriv := p.Handle.Get()
	roles := mysqlPriv.getDefaultRoles(user.Username, user.Hostname)
	return mysqlPriv.RequestDynamicVerification(roles, user.Username, user.Hostname, privName, grantable)
}

// HasExplicitlyGrantedDynamicPrivilege checks if a user has a DYNAMIC privilege
// without accepting SUPER privilege as a fallback.
func (p *UserPrivileges) HasExplicitlyGrantedDynamicPrivilege(activeRoles []*auth.RoleIdentity, privName string, grantable bool) bool {
	if SkipWithGrant {
		return true
	}
	if p.user == "" && p.host == "" {
		return true
	}

	mysqlPriv := p.Handle.Get()
	return mysqlPriv.HasExplicitlyGrantedDynamicPrivilege(activeRoles, p.user, p.host, privName, grantable)
}

// RequestDynamicVerification implements the Manager interface.
func (p *UserPrivileges) RequestDynamicVerification(activeRoles []*auth.RoleIdentity, privName string, grantable bool) bool {
	if SkipWithGrant {
		return true
	}
	if p.user == "" && p.host == "" {
		return true
	}

	mysqlPriv := p.Handle.Get()
	return mysqlPriv.RequestDynamicVerification(activeRoles, p.user, p.host, privName, grantable)
}

// RequestVerification implements the Manager interface.
func (p *UserPrivileges) RequestVerification(activeRoles []*auth.RoleIdentity, db, table, column string, priv mysql.PrivilegeType) bool {
	if SkipWithGrant {
		return true
	}

	if p.user == "" && p.host == "" {
		return true
	}

	// Skip check for system databases.
	// See https://dev.mysql.com/doc/refman/5.7/en/information-schema.html
	dbLowerName := strings.ToLower(db)
	tblLowerName := strings.ToLower(table)

	// If SEM is enabled and the user does not have the RESTRICTED_TABLES_ADMIN privilege
	// There are some hard rules which overwrite system tables and schemas as read-only at most.
	semEnabled := sem.IsEnabled()
	if semEnabled && !p.RequestDynamicVerification(activeRoles, "RESTRICTED_TABLES_ADMIN", false) {
		if sem.IsInvisibleTable(dbLowerName, tblLowerName) {
			return false
		}
		if util.IsMemOrSysDB(dbLowerName) {
			switch priv {
			case mysql.CreatePriv, mysql.AlterPriv, mysql.DropPriv, mysql.IndexPriv, mysql.CreateViewPriv,
				mysql.InsertPriv, mysql.UpdatePriv, mysql.DeletePriv:
				return false
			}
		}
	}

	if util.IsMemDB(dbLowerName) {
		switch priv {
		case mysql.CreatePriv, mysql.AlterPriv, mysql.DropPriv, mysql.IndexPriv, mysql.CreateViewPriv,
			mysql.InsertPriv, mysql.UpdatePriv, mysql.DeletePriv, mysql.ReferencesPriv, mysql.ExecutePriv,
			mysql.ShowViewPriv, mysql.LockTablesPriv:
			return false
		}
		if dbLowerName == util.InformationSchemaName.L {
			return true
		} else if dbLowerName == util.MetricSchemaName.L {
			// PROCESS is the same with SELECT for metrics_schema.
			if priv == mysql.SelectPriv && infoschema.IsMetricTable(table) {
				priv |= mysql.ProcessPriv
			}
		}
	}

	for _, fn := range p.extensionAccessCheckFuncs {
		for _, dynPriv := range fn(db, table, column, priv, semEnabled) {
			if !p.RequestDynamicVerification(activeRoles, dynPriv, false) {
				return false
			}
		}
	}

	mysqlPriv := p.Handle.Get()
	return mysqlPriv.RequestVerification(activeRoles, p.user, p.host, db, table, column, priv)
}

// RequestVerificationWithUser implements the Manager interface.
func (p *UserPrivileges) RequestVerificationWithUser(db, table, column string, priv mysql.PrivilegeType, user *auth.UserIdentity) bool {
	if SkipWithGrant {
		return true
	}
	if user == nil {
		return false
	}
	if user.Username == "" && user.Hostname == "" {
		return true
	}

	// Skip check for INFORMATION_SCHEMA database.
	// See https://dev.mysql.com/doc/refman/5.7/en/information-schema.html
	if strings.EqualFold(db, "INFORMATION_SCHEMA") {
		return true
	}

	mysqlPriv := p.Handle.Get()
	roles := mysqlPriv.getDefaultRoles(user.Username, user.Hostname)
	return mysqlPriv.RequestVerification(roles, user.Username, user.Hostname, db, table, column, priv)
}

func (p *UserPrivileges) isValidHash(record *UserRecord) bool {
	pwd := record.AuthenticationString
	if pwd == "" {
		return true
	}
	switch record.AuthPlugin {
	case mysql.AuthNativePassword:
		if len(pwd) == mysql.PWDHashLen+1 {
			return true
		}
		logutil.BgLogger().Error("the password from the mysql.user table does not match the definition of a mysql_native_password", zap.String("user", record.User), zap.String("plugin", record.AuthPlugin), zap.Int("hash_length", len(pwd)))
		return false
	case mysql.AuthCachingSha2Password:
		if len(pwd) == mysql.SHAPWDHashLen {
			return true
		}
		logutil.BgLogger().Error("the password from the mysql.user table does not match the definition of a caching_sha2_password", zap.String("user", record.User), zap.String("plugin", record.AuthPlugin), zap.Int("hash_length", len(pwd)))
		return false
	case mysql.AuthTiDBSM3Password:
		if len(pwd) == mysql.SM3PWDHashLen {
			return true
		}
		logutil.BgLogger().Error("the password from the mysql.user table does not match the definition of a tidb_sm3_password", zap.String("user", record.User), zap.String("plugin", record.AuthPlugin), zap.Int("hash_length", len(pwd)))
		return false
	case mysql.AuthSocket:
		return true
	case mysql.AuthTiDBAuthToken:
		return true
	case mysql.AuthLDAPSimple, mysql.AuthLDAPSASL:
		return true
	}

	logutil.BgLogger().Error("user password from the mysql.user table not like a known hash format", zap.String("user", record.User), zap.String("plugin", record.AuthPlugin), zap.Int("hash_length", len(pwd)))
	return false
}

// GetEncodedPassword implements the Manager interface.
func (p *UserPrivileges) GetEncodedPassword(user, host string) string {
	mysqlPriv := p.Handle.Get()
	record := mysqlPriv.connectionVerification(user, host)
	if record == nil {
		logutil.BgLogger().Error("get user privilege record fail",
			zap.String("user", user), zap.String("host", host))
		return ""
	}
	if p.isValidHash(record) {
		return record.AuthenticationString
	}
	return ""
}

// GetAuthPluginForConnection gets the authentication plugin used in connection establishment.
func (p *UserPrivileges) GetAuthPluginForConnection(user, host string) (string, error) {
	if SkipWithGrant {
		return mysql.AuthNativePassword, nil
	}

	mysqlPriv := p.Handle.Get()
	record := mysqlPriv.connectionVerification(user, host)
	if record == nil {
		return "", errors.New("Failed to get user record")
	}
	switch record.AuthPlugin {
	case mysql.AuthTiDBAuthToken, mysql.AuthLDAPSASL, mysql.AuthLDAPSimple:
		return record.AuthPlugin, nil
	}

	// zero-length auth string means no password for native and caching_sha2 auth.
	// but for auth_socket it means there should be a 1-to-1 mapping between the TiDB user
	// and the OS user.
	if record.AuthenticationString == "" && record.AuthPlugin != mysql.AuthSocket {
		return "", nil
	}
	if p.isValidHash(record) {
		return record.AuthPlugin, nil
	}
	return "", errors.New("Failed to get plugin for user")
}

// GetAuthPlugin gets the authentication plugin for the account identified by the user and host
func (p *UserPrivileges) GetAuthPlugin(user, host string) (string, error) {
	if SkipWithGrant {
		return mysql.AuthNativePassword, nil
	}
	mysqlPriv := p.Handle.Get()
	record := mysqlPriv.connectionVerification(user, host)
	if record == nil {
		return "", errors.New("Failed to get user record")
	}
	if !p.isValidHash(record) {
		return "", errors.New("Failed to get plugin for user")
	}
	return record.AuthPlugin, nil
}

// MatchIdentity implements the Manager interface.
func (p *UserPrivileges) MatchIdentity(user, host string, skipNameResolve bool) (u string, h string, success bool) {
	if SkipWithGrant {
		return user, host, true
	}
	mysqlPriv := p.Handle.Get()
	record := mysqlPriv.matchIdentity(user, host, skipNameResolve)
	if record != nil {
		return record.User, record.Host, true
	}
	return "", "", false
}

// MatchUserResourceGroupName implements the Manager interface.
func (p *UserPrivileges) MatchUserResourceGroupName(resourceGroupName string) (u string, success bool) {
	mysqlPriv := p.Handle.Get()
	record := mysqlPriv.matchResoureGroup(resourceGroupName)
	if record != nil {
		return record.User, true
	}
	return "", false
}

// GetAuthWithoutVerification implements the Manager interface.
func (p *UserPrivileges) GetAuthWithoutVerification(user, host string) (success bool) {
	if SkipWithGrant {
		p.user = user
		p.host = host
		success = true
		return
	}

	mysqlPriv := p.Handle.Get()
	record := mysqlPriv.connectionVerification(user, host)
	if record == nil {
		logutil.BgLogger().Error("get user privilege record fail",
			zap.String("user", user), zap.String("host", host))
		return
	}

	p.user = user
	p.host = record.Host
	success = true
	return
}

func checkAuthTokenClaims(claims map[string]any, record *UserRecord, tokenLife time.Duration) error {
	if sub, ok := claims[jwtRepo.SubjectKey]; !ok {
		return errors.New("lack 'sub'")
	} else if sub != record.User {
		return fmt.Errorf("Wrong 'sub': %s", sub)
	}

	if email, ok := claims[openid.EmailKey]; !ok {
		return errors.New("lack 'email'")
	} else if email != record.Email {
		return fmt.Errorf("Wrong 'email': %s", email)
	}

	now := time.Now()
	val, ok := claims[jwtRepo.IssuedAtKey]
	if !ok {
		return errors.New("lack 'iat'")
	} else if iat, ok := val.(time.Time); !ok {
		return fmt.Errorf("iat: %v is not a value of time.Time", val)
	} else if now.After(iat.Add(tokenLife)) {
		return errors.New("the token has been out of its life time")
	} else if now.Before(iat) {
		return errors.New("the token is issued at a future time")
	}

	if val, ok = claims[jwtRepo.ExpirationKey]; !ok {
		return errors.New("lack 'exp'")
	} else if exp, ok := val.(time.Time); !ok {
		return fmt.Errorf("exp: %v is not a value of time.Time", val)
	} else if now.After(exp) {
		return errors.New("the token has been expired")
	}

	// `iss` is not required if `token_issuer` is empty in `mysql.user`
	if iss, ok := claims[jwtRepo.IssuerKey]; ok && iss != record.AuthTokenIssuer {
		return fmt.Errorf("Wrong 'iss': %s", iss)
	} else if !ok && len(record.AuthTokenIssuer) > 0 {
		return errors.New("lack 'iss'")
	}

	return nil
}

// CheckPasswordExpired checks whether the password has been expired.
func (*UserPrivileges) CheckPasswordExpired(sessionVars *variable.SessionVars, record *UserRecord) (bool, error) {
	isSandBoxModeEnabled := variable.IsSandBoxModeEnabled.Load()
	if record.PasswordExpired {
		if isSandBoxModeEnabled {
			return true, nil
		}
		return false, ErrMustChangePasswordLogin.GenWithStackByArgs()
	}
	if record.PasswordLifeTime != 0 {
		lifeTime := record.PasswordLifeTime
		if lifeTime == -1 {
			pwdLifeTimeStr, err := sessionVars.GlobalVarsAccessor.GetGlobalSysVar(variable.DefaultPasswordLifetime)
			if err != nil {
				return false, err
			}
			lifeTime, err = strconv.ParseInt(pwdLifeTimeStr, 10, 64)
			if err != nil {
				return false, err
			}
		}
		if lifeTime > 0 && record.PasswordLastChanged.AddDate(0, 0, int(lifeTime)).Before(time.Now()) {
			if isSandBoxModeEnabled {
				return true, nil
			}
			return false, ErrMustChangePasswordLogin.GenWithStackByArgs()
		}
	}
	return false, nil
}

// GenerateAccountAutoLockErr implements the Manager interface.
func GenerateAccountAutoLockErr(failedLoginAttempts int64,
	user, host, lockTime, remainTime string) error {
	logutil.BgLogger().Error(fmt.Sprintf("Access denied for user '%s'@'%s'."+
		" Account is blocked for %s day(s) (%s day(s) remaining) due to %d "+
		"consecutive failed logins.", user, host, lockTime,
		remainTime, failedLoginAttempts))
	return ErUserAccessDeniedForUserAccountBlockedByPasswordLock.FastGenByArgs(user, host,
		lockTime, remainTime, failedLoginAttempts)
}

// VerifyAccountAutoLockInMemory implements the Manager interface.
func (p *UserPrivileges) VerifyAccountAutoLockInMemory(user string, host string) (bool, error) {
	mysqlPriv := p.Handle.Get()
	record := mysqlPriv.matchUser(user, host)
	if record == nil {
		logutil.BgLogger().Error("get authUser privilege record fail",
			zap.String("authUser", user), zap.String("authHost", host))
		return false, ErrAccessDenied.FastGenByArgs(user, host)
	}

	if record.AutoAccountLocked {
		// If it is locked, need to check whether it can be automatically unlocked.
		lockTime := record.PasswordLockTimeDays
		if lockTime == -1 {
			return record.AutoAccountLocked, GenerateAccountAutoLockErr(record.FailedLoginAttempts, user, host, "unlimited", "unlimited")
		}
		lastChanged := record.AutoLockedLastChanged
		d := time.Now().Unix() - lastChanged
		if d > lockTime*24*60*60 {
			return record.AutoAccountLocked, nil
		}
		lds := strconv.FormatInt(lockTime, 10)
		rds := strconv.FormatInt(int64(math.Ceil(float64(lockTime)-float64(d)/(24*60*60))), 10)
		return record.AutoAccountLocked, GenerateAccountAutoLockErr(record.FailedLoginAttempts, user, host, lds, rds)
	}
	return record.AutoAccountLocked, nil
}

// IsAccountAutoLockEnabled implements the Manager interface.
func (p *UserPrivileges) IsAccountAutoLockEnabled(user string, host string) bool {
	// If the service is started using skip-grant-tables, the system ignores whether
	// to enable the automatic account locking feature after continuous login failure.
	if SkipWithGrant {
		p.user = user
		p.host = host
		return false
	}
	mysqlPriv := p.Handle.Get()
	record := mysqlPriv.matchUser(user, host)
	if record == nil {
		return false
	}
	// For failed-login tracking and temporary locking to occur, an account's FAILED_LOGIN_ATTEMPTS
	// and PASSWORD_LOCK_TIME options both must be nonzero.
	// https://dev.mysql.com/doc/refman/8.0/en/create-user.html
	if record.FailedLoginAttempts == 0 || record.PasswordLockTimeDays == 0 {
		return false
	}
	return true
}

// BuildSuccessPasswordLockingJSON builds success PasswordLocking JSON string.
func BuildSuccessPasswordLockingJSON(failedLoginAttempts, passwordLockTimeDays int64) string {
	return BuildPasswordLockingJSON(failedLoginAttempts, passwordLockTimeDays, "N", 0, time.Now().Format(time.UnixDate))
}

// BuildPasswordLockingJSON builds PasswordLocking JSON string.
func BuildPasswordLockingJSON(failedLoginAttempts int64,
	passwordLockTimeDays int64, autoAccountLocked string, failedLoginCount int64, autoLockedLastChanged string) string {
	var passwordLockingArray []string
	passwordLockingArray = append(passwordLockingArray, fmt.Sprintf("\"failed_login_count\": %d", failedLoginCount))
	passwordLockingArray = append(passwordLockingArray, fmt.Sprintf("\"failed_login_attempts\": %d", failedLoginAttempts))
	passwordLockingArray = append(passwordLockingArray, fmt.Sprintf("\"password_lock_time_days\": %d", passwordLockTimeDays))
	if autoAccountLocked != "" {
		passwordLockingArray = append(passwordLockingArray, fmt.Sprintf("\"auto_account_locked\": \"%s\"", autoAccountLocked))
	}
	if autoLockedLastChanged != "" {
		passwordLockingArray = append(passwordLockingArray, fmt.Sprintf("\"auto_locked_last_changed\": \"%s\"", autoLockedLastChanged))
	}

	newAttributesStr := fmt.Sprintf("{\"Password_locking\": {%s}}", strings.Join(passwordLockingArray, ","))
	return newAttributesStr
}

// ConnectionVerification implements the Manager interface.
func (p *UserPrivileges) ConnectionVerification(user *auth.UserIdentity, authUser, authHost string, authentication, salt []byte, sessionVars *variable.SessionVars, authConn conn.AuthConn) (info privilege.VerificationInfo, err error) {
	if SkipWithGrant {
		p.user = authUser
		p.host = authHost
		return
	}

	hasPassword := "YES"
	if len(authentication) == 0 {
		hasPassword = "NO"
	}

	mysqlPriv := p.Handle.Get()
	record := mysqlPriv.connectionVerification(authUser, authHost)

	if record == nil {
		logutil.BgLogger().Error("get authUser privilege record fail",
			zap.String("authUser", authUser), zap.String("authHost", authHost))
		return info, ErrAccessDenied.FastGenByArgs(user.Username, user.Hostname, hasPassword)
	}

	globalPriv := mysqlPriv.matchGlobalPriv(authUser, authHost)
	if globalPriv != nil {
		if !p.checkSSL(globalPriv, sessionVars.TLSConnectionState) {
			logutil.BgLogger().Error("global priv check ssl fail",
				zap.String("authUser", authUser), zap.String("authHost", authHost))
			return info, ErrAccessDenied.FastGenByArgs(user.Username, user.Hostname, hasPassword)
		}
	}

	pwd := record.AuthenticationString
	if !p.isValidHash(record) {
		return info, ErrAccessDenied.FastGenByArgs(user.Username, user.Hostname, hasPassword)
	}

	// If the user uses session token to log in, skip checking record.AuthPlugin.
	if user.AuthPlugin == mysql.AuthTiDBSessionToken {
		if err = sessionstates.ValidateSessionToken(authentication, user.Username); err != nil {
			logutil.BgLogger().Warn("verify session token failed", zap.String("username", user.Username), zap.Error(err))
			return info, ErrAccessDenied.FastGenByArgs(user.Username, user.Hostname, hasPassword)
		}
	} else if record.AuthPlugin == mysql.AuthTiDBAuthToken {
		if len(authentication) == 0 {
			logutil.BgLogger().Error("empty authentication")
			return info, ErrAccessDenied.FastGenByArgs(user.Username, user.Hostname, hasPassword)
		}
		tokenString := string(hack.String(authentication[:len(authentication)-1]))
		var (
			claims map[string]any
		)
		if claims, err = GlobalJWKS.checkSigWithRetry(tokenString, 1); err != nil {
			logutil.BgLogger().Error("verify JWT failed", zap.Error(err))
			return info, ErrAccessDenied.FastGenByArgs(user.Username, user.Hostname, hasPassword)
		}
		if err = checkAuthTokenClaims(claims, record, defaultTokenLife); err != nil {
			logutil.BgLogger().Error("check claims failed", zap.Error(err))
			return info, ErrAccessDenied.FastGenByArgs(user.Username, user.Hostname, hasPassword)
		}
	} else if record.AuthPlugin == mysql.AuthLDAPSASL {
		if err = ldap.LDAPSASLAuthImpl.AuthLDAPSASL(authUser, pwd, authentication, authConn); err != nil {
			// though the pwd stores only `dn` for LDAP SASL, it could be unsafe to print it out.
			// for example, someone may alter the auth plugin name but forgot to change the password...
			logutil.BgLogger().Warn("verify through LDAP SASL failed", zap.String("username", user.Username), zap.Error(err))
			return info, ErrAccessDenied.FastGenByArgs(user.Username, user.Hostname, hasPassword)
		}
	} else if record.AuthPlugin == mysql.AuthLDAPSimple {
		if err = ldap.LDAPSimpleAuthImpl.AuthLDAPSimple(authUser, pwd, authentication); err != nil {
			logutil.BgLogger().Warn("verify through LDAP Simple failed", zap.String("username", user.Username), zap.Error(err))
			return info, ErrAccessDenied.FastGenByArgs(user.Username, user.Hostname, hasPassword)
		}
	} else if len(pwd) > 0 && len(authentication) > 0 {
		switch record.AuthPlugin {
		// NOTE: If the checking of the clear-text password fails, please set `info.FailedDueToWrongPassword = true`.
		case mysql.AuthNativePassword:
			hpwd, err := auth.DecodePassword(pwd)
			if err != nil {
				logutil.BgLogger().Error("decode password string failed", zap.Error(err))
				info.FailedDueToWrongPassword = true
				return info, ErrAccessDenied.FastGenByArgs(user.Username, user.Hostname, hasPassword)
			}

			if !auth.CheckScrambledPassword(salt, hpwd, authentication) {
				info.FailedDueToWrongPassword = true
				return info, ErrAccessDenied.FastGenByArgs(user.Username, user.Hostname, hasPassword)
			}
		case mysql.AuthCachingSha2Password, mysql.AuthTiDBSM3Password:
			authok, err := auth.CheckHashingPassword([]byte(pwd), string(authentication), record.AuthPlugin)
			if err != nil {
				logutil.BgLogger().Error("Failed to check caching_sha2_password", zap.Error(err))
			}

			if !authok {
				info.FailedDueToWrongPassword = true
				return info, ErrAccessDenied.FastGenByArgs(user.Username, user.Hostname, hasPassword)
			}
		case mysql.AuthSocket:
			if string(authentication) != authUser && string(authentication) != pwd {
				logutil.BgLogger().Error("Failed socket auth", zap.String("authUser", authUser),
					zap.String("socket_user", string(authentication)),
					zap.String("authentication_string", pwd))
				return info, ErrAccessDenied.FastGenByArgs(user.Username, user.Hostname, hasPassword)
			}
		default:
			logutil.BgLogger().Error("unknown authentication plugin", zap.String("authUser", authUser), zap.String("plugin", record.AuthPlugin))
			return info, ErrAccessDenied.FastGenByArgs(user.Username, user.Hostname, hasPassword)
		}
	} else if len(pwd) > 0 || len(authentication) > 0 {
		if record.AuthPlugin != mysql.AuthSocket {
			info.FailedDueToWrongPassword = true
			return info, ErrAccessDenied.FastGenByArgs(user.Username, user.Hostname, hasPassword)
		}
	}

	// Login a locked account is not allowed.
	locked := record.AccountLocked
	if locked {
		logutil.BgLogger().Error(fmt.Sprintf("Access denied for authUser '%s'@'%s'. Account is locked.", authUser, authHost))
		return info, errAccountHasBeenLocked.FastGenByArgs(user.Username, user.Hostname)
	}

	// special handling to existing users or root user initialized with insecure
	if record.ResourceGroup != "" {
		info.ResourceGroupName = record.ResourceGroup
	}
	// Skip checking password expiration if the session is migrated from another session.
	// Otherwise, the user cannot log in or execute statements after migration.
	if user.AuthPlugin != mysql.AuthTiDBSessionToken {
		info.InSandBoxMode, err = p.CheckPasswordExpired(sessionVars, record)
	}
	return
}

// AuthSuccess is to make the permission take effect.
func (p *UserPrivileges) AuthSuccess(authUser, authHost string) {
	p.user = authUser
	p.host = authHost
}

type checkResult int

const (
	notCheck checkResult = iota
	pass
	fail
)

func (p *UserPrivileges) checkSSL(priv *globalPrivRecord, tlsState *tls.ConnectionState) bool {
	if priv.Broken {
		logutil.BgLogger().Info("ssl check failure, due to broken global_priv record",
			zap.String("user", priv.User), zap.String("host", priv.Host))
		return false
	}
	switch priv.Priv.SSLType {
	case SslTypeNotSpecified, SslTypeNone:
		return true
	case SslTypeAny:
		r := tlsState != nil
		if !r {
			logutil.BgLogger().Info("ssl check failure, require ssl but not use ssl",
				zap.String("user", priv.User), zap.String("host", priv.Host))
		}
		return r
	case SslTypeX509:
		if tlsState == nil {
			logutil.BgLogger().Info("ssl check failure, require x509 but not use ssl",
				zap.String("user", priv.User), zap.String("host", priv.Host))
			return false
		}
		hasCert := false
		for _, chain := range tlsState.VerifiedChains {
			if len(chain) > 0 {
				hasCert = true
				break
			}
		}
		if !hasCert {
			logutil.BgLogger().Info("ssl check failure, require x509 but no verified cert",
				zap.String("user", priv.User), zap.String("host", priv.Host))
		}
		return hasCert
	case SslTypeSpecified:
		if tlsState == nil {
			logutil.BgLogger().Info("ssl check failure, require subject/issuer/cipher but not use ssl",
				zap.String("user", priv.User), zap.String("host", priv.Host))
			return false
		}
		if len(priv.Priv.SSLCipher) > 0 && priv.Priv.SSLCipher != util.TLSCipher2String(tlsState.CipherSuite) {
			logutil.BgLogger().Info("ssl check failure for cipher", zap.String("user", priv.User), zap.String("host", priv.Host),
				zap.String("require", priv.Priv.SSLCipher), zap.String("given", util.TLSCipher2String(tlsState.CipherSuite)))
			return false
		}
		var (
			hasCert      = false
			matchIssuer  checkResult
			matchSubject checkResult
			matchSAN     checkResult
		)
		for _, chain := range tlsState.VerifiedChains {
			if len(chain) == 0 {
				continue
			}
			cert := chain[0]
			if len(priv.Priv.X509Issuer) > 0 {
				given := util.X509NameOnline(cert.Issuer)
				if priv.Priv.X509Issuer == given {
					matchIssuer = pass
				} else if matchIssuer == notCheck {
					matchIssuer = fail
					logutil.BgLogger().Info("ssl check failure for issuer", zap.String("user", priv.User), zap.String("host", priv.Host),
						zap.String("require", priv.Priv.X509Issuer), zap.String("given", given))
				}
			}
			if len(priv.Priv.X509Subject) > 0 {
				given := util.X509NameOnline(cert.Subject)
				if priv.Priv.X509Subject == given {
					matchSubject = pass
				} else if matchSubject == notCheck {
					matchSubject = fail
					logutil.BgLogger().Info("ssl check failure for subject", zap.String("user", priv.User), zap.String("host", priv.Host),
						zap.String("require", priv.Priv.X509Subject), zap.String("given", given))
				}
			}
			if len(priv.Priv.SANs) > 0 {
				matchOne := checkCertSAN(priv, cert, priv.Priv.SANs)
				if matchOne {
					matchSAN = pass
				} else if matchSAN == notCheck {
					matchSAN = fail
				}
			}
			hasCert = true
		}
		checkResult := hasCert && matchIssuer != fail && matchSubject != fail && matchSAN != fail
		if !checkResult && !hasCert {
			logutil.BgLogger().Info("ssl check failure, require issuer/subject/SAN but no verified cert",
				zap.String("user", priv.User), zap.String("host", priv.Host))
		}
		return checkResult
	default:
		panic(fmt.Sprintf("support ssl_type: %d", priv.Priv.SSLType))
	}
}

func checkCertSAN(priv *globalPrivRecord, cert *x509.Certificate, sans map[util.SANType][]string) (r bool) {
	r = true
	for typ, requireOr := range sans {
		var (
			unsupported bool
			given       []string
		)
		switch typ {
		case util.URI:
			for _, uri := range cert.URIs {
				given = append(given, uri.String())
			}
		case util.DNS:
			given = cert.DNSNames
		case util.IP:
			for _, ip := range cert.IPAddresses {
				given = append(given, ip.String())
			}
		default:
			unsupported = true
		}
		if unsupported {
			logutil.BgLogger().Warn("skip unsupported SAN type", zap.String("type", string(typ)),
				zap.String("user", priv.User), zap.String("host", priv.Host))
			continue
		}
		var givenMatchOne bool
		for _, req := range requireOr {
			for _, give := range given {
				if req == give {
					givenMatchOne = true
					break
				}
			}
		}
		if !givenMatchOne {
			logutil.BgLogger().Info("ssl check failure for subject", zap.String("user", priv.User), zap.String("host", priv.Host),
				zap.String("require", priv.Priv.SAN), zap.Strings("given", given), zap.String("type", string(typ)))
			r = false
			return
		}
	}
	return
}

// DBIsVisible implements the Manager interface.
func (p *UserPrivileges) DBIsVisible(activeRoles []*auth.RoleIdentity, db string) bool {
	if SkipWithGrant {
		return true
	}
	// If SEM is enabled, respect hard rules about certain schemas being invisible
	// Before checking if the user has permissions granted to them.
	if sem.IsEnabled() && !p.RequestDynamicVerification(activeRoles, "RESTRICTED_TABLES_ADMIN", false) {
		if sem.IsInvisibleSchema(db) {
			return false
		}
	}
	mysqlPriv := p.Handle.Get()
	if mysqlPriv.DBIsVisible(p.user, p.host, db) {
		return true
	}
	allRoles := mysqlPriv.FindAllUserEffectiveRoles(p.user, p.host, activeRoles)
	for _, role := range allRoles {
		if mysqlPriv.DBIsVisible(role.Username, role.Hostname, db) {
			return true
		}
	}
	return false
}

// UserPrivilegesTable implements the Manager interface.
func (p *UserPrivileges) UserPrivilegesTable(activeRoles []*auth.RoleIdentity, user, host string) [][]types.Datum {
	mysqlPriv := p.Handle.Get()
	return mysqlPriv.UserPrivilegesTable(activeRoles, user, host)
}

// ShowGrants implements privilege.Manager ShowGrants interface.
func (p *UserPrivileges) ShowGrants(ctx sessionctx.Context, user *auth.UserIdentity, roles []*auth.RoleIdentity) (grants []string, err error) {
	if SkipWithGrant {
		return nil, ErrNonexistingGrant.GenWithStackByArgs("root", "%")
	}
	mysqlPrivilege := p.Handle.Get()
	u := user.Username
	h := user.Hostname
	if len(user.AuthUsername) > 0 && len(user.AuthHostname) > 0 {
		u = user.AuthUsername
		h = user.AuthHostname
	}
	grants = mysqlPrivilege.showGrants(ctx, u, h, roles)
	if len(grants) == 0 {
		err = ErrNonexistingGrant.GenWithStackByArgs(u, h)
	}

	return
}

// ActiveRoles implements privilege.Manager ActiveRoles interface.
func (p *UserPrivileges) ActiveRoles(ctx sessionctx.Context, roleList []*auth.RoleIdentity) (bool, string) {
	if SkipWithGrant {
		return true, ""
	}
	mysqlPrivilege := p.Handle.Get()
	u := p.user
	h := p.host
	for _, r := range roleList {
		ok := mysqlPrivilege.FindRole(u, h, r)
		if !ok {
			logutil.BgLogger().Error("find role failed", zap.Stringer("role", r))
			return false, r.String()
		}
	}
	ctx.GetSessionVars().ActiveRoles = roleList
	return true, ""
}

// FindEdge implements privilege.Manager FindRelationship interface.
func (p *UserPrivileges) FindEdge(ctx sessionctx.Context, role *auth.RoleIdentity, user *auth.UserIdentity) bool {
	if SkipWithGrant {
		return false
	}
	mysqlPrivilege := p.Handle.Get()
	ok := mysqlPrivilege.FindRole(user.Username, user.Hostname, role)
	if !ok {
		logutil.BgLogger().Error("find role failed", zap.Stringer("role", role))
		return false
	}
	return true
}

// GetDefaultRoles returns all default roles for certain user.
func (p *UserPrivileges) GetDefaultRoles(user, host string) []*auth.RoleIdentity {
	if SkipWithGrant {
		return make([]*auth.RoleIdentity, 0, 10)
	}
	mysqlPrivilege := p.Handle.Get()
	ret := mysqlPrivilege.getDefaultRoles(user, host)
	return ret
}

// GetAllRoles return all roles of user.
func (p *UserPrivileges) GetAllRoles(user, host string) []*auth.RoleIdentity {
	if SkipWithGrant {
		return make([]*auth.RoleIdentity, 0, 10)
	}

	mysqlPrivilege := p.Handle.Get()
	return mysqlPrivilege.getAllRoles(user, host)
}

// IsDynamicPrivilege returns true if the DYNAMIC privilege is built-in or has been registered by a plugin
func (p *UserPrivileges) IsDynamicPrivilege(privName string) bool {
	privNameInUpper := strings.ToUpper(privName)
	for _, priv := range dynamicPrivs {
		if privNameInUpper == priv {
			return true
		}
	}
	return false
}

// RegisterDynamicPrivilege is used by plugins to add new privileges to TiDB
func RegisterDynamicPrivilege(privName string) error {
	if len(privName) == 0 {
		return errors.New("privilege name should not be empty")
	}

	privNameInUpper := strings.ToUpper(privName)
	if len(privNameInUpper) > 32 {
		return errors.New("privilege name is longer than 32 characters")
	}
	dynamicPrivLock.Lock()
	defer dynamicPrivLock.Unlock()
	for _, priv := range dynamicPrivs {
		if privNameInUpper == priv {
			return errors.New("privilege is already registered")
		}
	}
	dynamicPrivs = append(dynamicPrivs, privNameInUpper)
	return nil
}

// GetDynamicPrivileges returns the list of registered DYNAMIC privileges
// for use in meta data commands (i.e. SHOW PRIVILEGES)
func GetDynamicPrivileges() []string {
	dynamicPrivLock.Lock()
	defer dynamicPrivLock.Unlock()

	privCopy := make([]string, len(dynamicPrivs))
	copy(privCopy, dynamicPrivs)
	return privCopy
}

// RemoveDynamicPrivilege is used for test only
func RemoveDynamicPrivilege(privName string) bool {
	privNameInUpper := strings.ToUpper(privName)
	dynamicPrivLock.Lock()
	defer dynamicPrivLock.Unlock()
	for idx, priv := range dynamicPrivs {
		if privNameInUpper == priv {
			dynamicPrivs = append(dynamicPrivs[:idx], dynamicPrivs[idx+1:]...)
			return true
		}
	}
	return false
}

func init() {
	extension.RegisterDynamicPrivilege = RegisterDynamicPrivilege
	extension.RemoveDynamicPrivilege = RemoveDynamicPrivilege
}

// PasswordLocking is the User_attributes->>"$.Password_locking".
// It records information about failed-login tracking and temporary account locking.
type PasswordLocking struct {
	FailedLoginCount      int64
	PasswordLockTimeDays  int64
	AutoAccountLocked     bool
	AutoLockedLastChanged int64
	FailedLoginAttempts   int64
}

// ParseJSON parses information about PasswordLocking.
func (passwordLocking *PasswordLocking) ParseJSON(passwordLockingJSON types.BinaryJSON) error {
	var err error

	passwordLocking.FailedLoginAttempts, err =
		extractInt64FromJSON(passwordLockingJSON, "$.Password_locking.failed_login_attempts")
	if err != nil {
		return err
	}
	passwordLocking.FailedLoginAttempts = min(passwordLocking.FailedLoginAttempts, math.MaxInt16)
	passwordLocking.FailedLoginAttempts = mathutil.Max(passwordLocking.FailedLoginAttempts, 0)

	passwordLocking.PasswordLockTimeDays, err =
		extractInt64FromJSON(passwordLockingJSON, "$.Password_locking.password_lock_time_days")
	if err != nil {
		return err
	}
	passwordLocking.PasswordLockTimeDays = min(passwordLocking.PasswordLockTimeDays, math.MaxInt16)
	passwordLocking.PasswordLockTimeDays = mathutil.Max(passwordLocking.PasswordLockTimeDays, -1)

	passwordLocking.FailedLoginCount, err =
		extractInt64FromJSON(passwordLockingJSON, "$.Password_locking.failed_login_count")
	if err != nil {
		return err
	}

	passwordLocking.AutoLockedLastChanged, err =
		extractTimeUnixFromJSON(passwordLockingJSON, "$.Password_locking.auto_locked_last_changed")
	if err != nil {
		return err
	}

	passwordLocking.AutoAccountLocked, err =
		extractBoolFromJSON(passwordLockingJSON, "$.Password_locking.auto_account_locked")
	if err != nil {
		return err
	}
	return nil
}

func extractInt64FromJSON(json types.BinaryJSON, pathExpr string) (val int64, err error) {
	jsonPath, err := types.ParseJSONPathExpr(pathExpr)
	if err != nil {
		return 0, err
	}
	if BJ, found := json.Extract([]types.JSONPathExpression{jsonPath}); found {
		return BJ.GetInt64(), nil
	}
	return 0, nil
}

func extractTimeUnixFromJSON(json types.BinaryJSON, pathExpr string) (int64, error) {
	jsonPath, err := types.ParseJSONPathExpr(pathExpr)
	if err != nil {
		return -1, err
	}
	if BJ, found := json.Extract([]types.JSONPathExpression{jsonPath}); found {
		value, err := BJ.Unquote()
		if err != nil {
			return -1, err
		}
		t, err := time.ParseInLocation(time.UnixDate, value, time.Local)
		if err != nil {
			return -1, err
		}
		return t.Unix(), nil
	}
	return 0, nil
}

func extractBoolFromJSON(json types.BinaryJSON, pathExpr string) (bool, error) {
	jsonPath, err := types.ParseJSONPathExpr(pathExpr)
	if err != nil {
		return false, err
	}
	if BJ, found := json.Extract([]types.JSONPathExpression{jsonPath}); found {
		value, err := BJ.Unquote()
		if err != nil {
			return false, err
		}
		if value == "Y" {
			return true, nil
		}
	}
	return false, nil
}
