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
	"bytes"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/types"
)

// Enable enables the new privilege check feature.
var Enable = true

// SkipWithGrant causes the server to start without using the privilege system at all.
var SkipWithGrant = false

// privilege error codes.
const (
	codeInvalidPrivilegeType  terror.ErrCode = 1
	codeInvalidUserNameFormat                = 2
)

var (
	errInvalidPrivilegeType  = terror.ClassPrivilege.New(codeInvalidPrivilegeType, "unknown privilege type")
	errInvalidUserNameFormat = terror.ClassPrivilege.New(codeInvalidUserNameFormat, "wrong username format")
)

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
	if !Enable || SkipWithGrant {
		return true
	}

	if p.user == "" && p.host == "" {
		return true
	}

	mysqlPriv := p.Handle.Get()
	return mysqlPriv.RequestVerification(p.user, p.host, db, table, column, priv)
}

// PWDHashLen is the length of password's hash.
const PWDHashLen = 40

// ConnectionVerification implements the Manager interface.
func (p *UserPrivileges) ConnectionVerification(user, host string, auth, salt []byte) bool {
	if SkipWithGrant {
		p.user = user
		p.host = host
		return true
	}

	mysqlPriv := p.Handle.Get()
	record := mysqlPriv.connectionVerification(user, host)
	if record == nil {
		log.Errorf("Get user privilege record fail: user %v, host %v", user, host)
		return false
	}

	pwd := record.Password
	if len(pwd) != 0 && len(pwd) != PWDHashLen {
		log.Errorf("User [%s] password from SystemDB not like a sha1sum", user)
		return false
	}
	hpwd, err := util.DecodePassword(pwd)
	if err != nil {
		log.Errorf("Decode password string error %v", err)
		return false
	}
	checkAuth := util.CalcPassword(salt, hpwd)
	if !bytes.Equal(auth, checkAuth) {
		return false
	}
	p.user = user
	p.host = host
	return true
}

// DBIsVisible implements the Manager interface.
func (p *UserPrivileges) DBIsVisible(db string) bool {
	if !Enable || SkipWithGrant {
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
func (p *UserPrivileges) ShowGrants(ctx context.Context, user string) ([]string, error) {
	strs := strings.Split(user, "@")
	if len(strs) != 2 {
		return nil, errors.Errorf("Invalid format for user: %s", user)
	}
	user, host := strs[0], strs[1]
	mysqlPrivilege := p.Handle.Get()
	return mysqlPrivilege.showGrants(user, host), nil
}
