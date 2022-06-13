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
// See the License for the specific language governing permissions and
// limitations under the License.

package privileges

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
<<<<<<< HEAD
	"github.com/pingcap/tidb/infoschema/perfschema"
=======
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/mysql"
>>>>>>> 395ccbe22... privilege: limit the privileges in memory schemas (#35260)
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sem"
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
	"RESTRICTED_TABLES_ADMIN",     // Can see system tables when SEM is enabled
	"RESTRICTED_STATUS_ADMIN",     // Can see all status vars when SEM is enabled.
	"RESTRICTED_VARIABLES_ADMIN",  // Can see all variables when SEM is enabled
	"RESTRICTED_USER_ADMIN",       // User can not have their access revoked by SUPER users.
	"RESTRICTED_CONNECTION_ADMIN", // Can not be killed by PROCESS/CONNECTION_ADMIN privilege
}
var dynamicPrivLock sync.Mutex

// UserPrivileges implements privilege.Manager interface.
// This is used to check privilege for the current user.
type UserPrivileges struct {
	user string
	host string
	*Handle
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
	if sem.IsEnabled() && !p.RequestDynamicVerification(activeRoles, "RESTRICTED_TABLES_ADMIN", false) {
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
<<<<<<< HEAD
		return true
	// We should be very careful of limiting privileges, so ignore `mysql` for now.
	case util.PerformanceSchemaName.L, util.MetricSchemaName.L:
		if (dbLowerName == util.PerformanceSchemaName.L && perfschema.IsPredefinedTable(table)) ||
			(dbLowerName == util.MetricSchemaName.L && infoschema.IsMetricTable(table)) {
			switch priv {
			case mysql.CreatePriv, mysql.AlterPriv, mysql.DropPriv, mysql.IndexPriv, mysql.InsertPriv, mysql.UpdatePriv, mysql.DeletePriv:
				return false
			case mysql.SelectPriv:
				return true
=======
		if dbLowerName == util.InformationSchemaName.L {
			return true
		} else if dbLowerName == util.MetricSchemaName.L {
			// PROCESS is the same with SELECT for metrics_schema.
			if priv == mysql.SelectPriv && infoschema.IsMetricTable(table) {
				priv |= mysql.ProcessPriv
>>>>>>> 395ccbe22... privilege: limit the privileges in memory schemas (#35260)
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

	// Skip check for INFORMATION_SCHEMA database.
	// See https://dev.mysql.com/doc/refman/5.7/en/information-schema.html
	if strings.EqualFold(db, "INFORMATION_SCHEMA") {
		return true
	}

	mysqlPriv := p.Handle.Get()
	roles := mysqlPriv.getDefaultRoles(user.Username, user.Hostname)
	return mysqlPriv.RequestVerification(roles, user.Username, user.Hostname, db, table, column, priv)
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
	pwd := record.AuthenticationString
	switch len(pwd) {
	case 0:
		return pwd
	case mysql.PWDHashLen + 1: // mysql_native_password
		return pwd
	case 70: // caching_sha2_password
		return pwd
	}
	logutil.BgLogger().Error("user password from system DB not like a known hash format", zap.String("user", user), zap.Int("hash_length", len(pwd)))
	return ""
}

// GetAuthWithoutVerification implements the Manager interface.
func (p *UserPrivileges) GetAuthWithoutVerification(user, host string) (u string, h string, success bool) {
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

	u = record.User
	h = record.Host
	p.user = user
	p.host = h
	success = true
	return
}

// ConnectionVerification implements the Manager interface.
func (p *UserPrivileges) ConnectionVerification(user, host string, authentication, salt []byte, tlsState *tls.ConnectionState) (u string, h string, success bool) {
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

	u = record.User
	h = record.Host

	globalPriv := mysqlPriv.matchGlobalPriv(user, host)
	if globalPriv != nil {
		if !p.checkSSL(globalPriv, tlsState) {
			logutil.BgLogger().Error("global priv check ssl fail",
				zap.String("user", user), zap.String("host", host))
			success = false
			return
		}
	}

	// Login a locked account is not allowed.
	locked := record.AccountLocked
	if locked {
		logutil.BgLogger().Error("try to login a locked account",
			zap.String("user", user), zap.String("host", host))
		success = false
		return
	}

	pwd := record.AuthenticationString
	if len(pwd) != 0 && len(pwd) != mysql.PWDHashLen+1 {
		logutil.BgLogger().Error("user password from system DB not like sha1sum", zap.String("user", user))
		return
	}

	// empty password
	if len(pwd) == 0 && len(authentication) == 0 {
		p.user = user
		p.host = h
		success = true
		return
	}

	if len(pwd) == 0 || len(authentication) == 0 {
		return
	}

	hpwd, err := auth.DecodePassword(pwd)
	if err != nil {
		logutil.BgLogger().Error("decode password string failed", zap.Error(err))
		return
	}

	if !auth.CheckScrambledPassword(salt, hpwd, authentication) {
		return
	}

	p.user = user
	p.host = h
	success = true
	return
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
	allRoles := mysqlPriv.FindAllRole(activeRoles)
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
	grants = mysqlPrivilege.showGrants(u, h, roles)
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
