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

	"github.com/go-ldap/ldap/v3"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/privilege/conn"
)

type ldapSASLAuthImpl struct {
	*ldapAuthImpl

	saslAuthMethod string
}

// AuthLDAPSASL authenticates the user through LDAP SASL
func (impl *ldapSASLAuthImpl) AuthLDAPSASL(userName string, dn string, clientCred []byte, authConn conn.AuthConn) error {
	dn, ldapConn, err := func() (string, *ldap.Conn, error) {
		// It's fine to just `RLock` here, even we're fetching resources from the pool, because the resource pool can be
		// accessed concurrently. The `RLock` here is to protect the configurations.
		//
		// It's a bad idea to lock through the whole function, as this function will write/read to interact with the client.
		// If the client somehow died and don't send responds, this function will not end for a long time (until connection
		// timeout) and this lock will not be released. Therefore, we only `RLock` the configurations in the
		impl.RLock()
		defer impl.RUnlock()

		var err error
		if len(dn) == 0 {
			dn, err = impl.searchUser(userName)
			if err != nil {
				return "", nil, err
			}
		} else {
			dn = impl.canonicalizeDN(userName, dn)
		}

		ldapConn, err := impl.getConnection()
		if err != nil {
			return "", nil, errors.Wrap(err, "create LDAP connection")
		}

		return dn, ldapConn, nil
	}()
	if err != nil {
		return err
	}

	defer impl.putConnection(ldapConn)
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
	&ldapAuthImpl{},
	"",
}
