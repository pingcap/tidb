// Copyright 2023-2023 PingCAP Xingchen (Beijing) Technology Co., Ltd.
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
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/go-ldap/ldap/v3"
	"github.com/stretchr/testify/require"
)

const listenPortRangeStart = 11310
const listenPortRangeLength = 256

func TestCanonicalizeDN(t *testing.T) {
	impl := &ldapAuthImpl{
		searchAttr: "cn",
	}
	require.Equal(t, impl.canonicalizeDN("yka", "cn=y,dc=ping,dc=cap"), "cn=y,dc=ping,dc=cap")
	require.Equal(t, impl.canonicalizeDN("yka", "+dc=ping,dc=cap"), "cn=yka,dc=ping,dc=cap")
}

func getConnectionsWithinTimeout(t *testing.T, ch chan net.Conn, timeout time.Duration, count int) []net.Conn {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result := []net.Conn{}
	for count > 0 {
		count--

		select {
		case conn := <-ch:
			result = append(result, conn)
		case <-ctx.Done():
			require.Fail(t, "fail to get connections")
		}
	}

	return result
}

func TestLDAPConnectionPool(t *testing.T) {
	// allocate a random port between the port range
	port := rand.Int()%listenPortRangeLength + listenPortRangeStart
	ldapServer := NewMockLDAPServer(t)
	ldapServer.Listen(port)
	defer ldapServer.Close()
	conns := ldapServer.GetConnections()

	impl := &ldapAuthImpl{ldapServerHost: "localhost", ldapServerPort: port}
	impl.SetInitCapacity(256)
	impl.SetMaxCapacity(1024)
	conn, err := impl.getConnection()
	require.NoError(t, err)
	impl.putConnection(conn)

	getConnectionsWithinTimeout(t, conns, time.Second, 1)

	// test allocating 255 more connections
	var clientConnections []*ldap.Conn
	for i := 0; i < 256; i++ {
		conn, err := impl.getConnection()
		require.NoError(t, err)

		clientConnections = append(clientConnections, conn)
	}
	getConnectionsWithinTimeout(t, conns, time.Second, 255)
	for _, conn := range clientConnections {
		impl.putConnection(conn)
	}

	clientConnections = clientConnections[:]

	// now, the max capacity is somehow meaningless
	// TODO: auto scalling the capacity of LDAP connection pool
}
