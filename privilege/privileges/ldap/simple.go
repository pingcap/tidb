// Copyright 2023-2023 PingCAP, Inc.
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

package ldap

import (
	"github.com/pingcap/errors"
)

type ldapSimplAuthImpl struct {
	*ldapAuthImpl
}

// AuthLDAPSimple authenticates the user through LDAP Simple Bind
// password is expected to be a nul-terminated string
func (impl *ldapSimplAuthImpl) AuthLDAPSimple(userName string, dn string, password []byte) error {
	impl.RLock()
	defer impl.RUnlock()

	if len(password) == 0 {
		return errors.New("invalid password")
	}
	if password[len(password)-1] != 0 {
		return errors.New("invalid password")
	}
	passwordStr := string(password[:len(password)-1])

	var err error
	if len(dn) == 0 {
		dn, err = impl.searchUser(userName)
		if err != nil {
			return err
		}
	} else {
		dn = impl.canonicalizeDN(userName, dn)
	}

	ldapConn, err := impl.getConnection()
	if err != nil {
		return errors.Wrap(err, "create LDAP connection")
	}
	defer impl.putConnection(ldapConn)
	err = ldapConn.Bind(dn, passwordStr)
	if err != nil {
		return errors.Wrap(err, "bind LDAP")
	}

	return nil
}

// LDAPSimpleAuthImpl is the implementation of authentication with LDAP clear text password
var LDAPSimpleAuthImpl = &ldapSimplAuthImpl{
	&ldapAuthImpl{},
}
