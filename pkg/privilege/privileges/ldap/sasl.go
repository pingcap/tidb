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
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/privilege/conn"
)

type ldapSASLAuthImpl struct {
	*ldapAuthImplBuilder

	saslAuthMethod string
}

// AuthLDAPSASL authenticates the user through LDAP SASL
func (impl *ldapSASLAuthImpl) AuthLDAPSASL(userName string, dn string, clientCred []byte, authConn conn.AuthConn) error {
	var err error
	ldapImpl := impl.ldapAuthImplBuilder.build()

	if len(dn) == 0 {
		dn, err = ldapImpl.searchUser(userName)
		if err != nil {
			return err
		}
	} else {
		dn = ldapImpl.canonicalizeDN(userName, dn)
	}

	ldapConn, err := ldapImpl.getConnection()
	if err != nil {
		return errors.Wrap(err, "create LDAP connection")
	}
	defer ldapImpl.putConnection(ldapConn)

	for {
		resultCode, serverCred, err := ldapConn.ServerBindStep(clientCred, dn, impl.saslAuthMethod, nil)
		if err != nil {
			return err
		}

		// The client is still waiting for the last serverCred, so cannot break the loop right now even if the `resultCode`
		// is 0
		err = authConn.WriteAuthMoreData(serverCred)
		if err != nil {
			return err
		}

		// In TiDB, all `context.Context` before `dispatch` is all `context.Background()` with some KV used for log, so
		// it's just fine to use `context.Background()` here.
		err = authConn.Flush(context.Background())
		if err != nil {
			return err
		}

		// `resultCode == 0` means the result is `LDAP_SUCCESS`, the authentication succeeds and we could go further now.
		if resultCode == 0 {
			break
		}

		clientCred, err = authConn.ReadPacket()
		if err != nil {
			return err
		}
	}

	return nil
}

// SetSASLAuthMethod sets the authentication method used by SASL
func (impl *ldapSASLAuthImpl) SetSASLAuthMethod(saslAuthMethod string) {
	impl.Lock()
	defer impl.Unlock()

	impl.saslAuthMethod = saslAuthMethod
}

// GetSASLAuthMethod returns the authentication method used by SASL
func (impl *ldapSASLAuthImpl) GetSASLAuthMethod() string {
	impl.Lock()
	defer impl.Unlock()

	return impl.saslAuthMethod
}

// LDAPSASLAuthImpl is the implementation of authentication with LDAP SASL
var LDAPSASLAuthImpl = &ldapSASLAuthImpl{
	&ldapAuthImplBuilder{},
	"",
}
