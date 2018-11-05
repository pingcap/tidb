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
	"strings"

	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	log "github.com/sirupsen/logrus"
)

// SkipWithGrant causes the server to start without using the privilege system at all.
var SkipWithGrant = false

var _ privilege.Manager = (*UserPrivileges)(nil)

// UserPrivileges implements privilege.Manager interface.
// This is used to check privilege for the current user.
type UserPrivileges struct {
	user string
	host string
	*Handle
}

// RequestVerification implements the Manager interface.
func (p *UserPrivileges) RequestVerification(db, table, column string, priv mysql.PrivilegeType) bool {
	if SkipWithGrant {
		return true
	}

	if p.user == "" && p.host == "" {
		return true
	}

	// Skip check for INFORMATION_SCHEMA database.
	// See https://dev.mysql.com/doc/refman/5.7/en/information-schema.html
	if strings.EqualFold(db, "INFORMATION_SCHEMA") {
		return true
	}

	mysqlPriv := p.Handle.Get()
	return mysqlPriv.RequestVerification(p.user, p.host, db, table, column, priv)
}

// ConnectionVerification implements the Manager interface.
func (p *UserPrivileges) ConnectionVerification(user, host string, authentication, salt []byte) (u string, h string, success bool) {

	if SkipWithGrant {
		p.user = user
		p.host = host
		success = true
		return
	}

	mysqlPriv := p.Handle.Get()
	record := mysqlPriv.connectionVerification(user, host)
	if record == nil {
		log.Errorf("Get user privilege record fail: user %v, host %v", user, host)
		return
	}

	u = record.User
	h = record.Host

	pwd := record.Password
	if len(pwd) != 0 && len(pwd) != mysql.PWDHashLen+1 {
		log.Errorf("User [%s] password from SystemDB not like a sha1sum", user)
		return
	}

	// empty password
	if len(pwd) == 0 && len(authentication) == 0 {
		p.user = user
		p.host = host
		success = true
		return
	}

	if len(pwd) == 0 || len(authentication) == 0 {
		return
	}

	hpwd, err := auth.DecodePassword(pwd)
	if err != nil {
		log.Errorf("Decode password string error %v", err)
		return
	}

	if !auth.CheckScrambledPassword(salt, hpwd, authentication) {
		return
	}

	p.user = user
	p.host = host
	success = true
	return
}

// DBIsVisible implements the Manager interface.
func (p *UserPrivileges) DBIsVisible(db string) bool {
	if SkipWithGrant {
		return true
	}
	mysqlPriv := p.Handle.Get()
	return mysqlPriv.DBIsVisible(p.user, p.host, db)
}

// UserPrivilegesTable implements the Manager interface.
func (p *UserPrivileges) UserPrivilegesTable() [][]types.Datum {
	mysqlPriv := p.Handle.Get()
	return mysqlPriv.UserPrivilegesTable()
}

// ShowGrants implements privilege.Manager ShowGrants interface.
func (p *UserPrivileges) ShowGrants(ctx sessionctx.Context, user *auth.UserIdentity) (grants []string, err error) {
	mysqlPrivilege := p.Handle.Get()
	u := user.Username
	h := user.Hostname
	if len(user.AuthUsername) > 0 && len(user.AuthHostname) > 0 {
		u = user.AuthUsername
		h = user.AuthHostname
	}
	grants = mysqlPrivilege.showGrants(u, h)
	if len(grants) == 0 {
		err = errNonexistingGrant.GenWithStackByArgs(u, h)
	}

	return
}
