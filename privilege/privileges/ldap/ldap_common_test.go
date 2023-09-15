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
	"bufio"
	"crypto/tls"
	"crypto/x509"
	_ "embed"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

//go:embed test/ca.crt
var tlsCAStr []byte

//go:embed test/ldap.crt
var tlsCrtStr []byte

//go:embed test/ldap.key
var tlsKeyStr []byte

func TestCanonicalizeDN(t *testing.T) {
	impl := &ldapAuthImpl{
		searchAttr: "cn",
	}
	require.Equal(t, impl.canonicalizeDN("yka", "cn=y,dc=ping,dc=cap"), "cn=y,dc=ping,dc=cap")
	require.Equal(t, impl.canonicalizeDN("yka", "+dc=ping,dc=cap"), "cn=yka,dc=ping,dc=cap")
}

func TestConnectThrough636(t *testing.T) {
	var ln net.Listener

	startListen := make(chan struct{})

	// this test only tests whether the LDAP with LTS enabled will fallback from StartTLS
	randomTLSServicePort := rand.Int()%10000 + 10000
	serverWg := &sync.WaitGroup{}
	serverWg.Add(1)
	go func() {
		defer close(startListen)
		defer serverWg.Done()

		cert, err := tls.X509KeyPair(tlsCrtStr, tlsKeyStr)
		require.NoError(t, err)
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
		ln, err = tls.Listen("tcp", fmt.Sprintf("localhost:%d", randomTLSServicePort), tlsConfig)
		require.NoError(t, err)
		startListen <- struct{}{}

		for {
			conn, err := ln.Accept()
			if err != nil {
				break
			}

			// handling one connection at a time is enough for test
			func() {
				defer func() {
					require.NoError(t, conn.Close())
				}()

				r := bufio.NewReader(conn)
				for {
					_, err := r.ReadByte()
					if err != nil {
						break
					}
				}
			}()
		}
	}()

	<-startListen
	defer func() {
		require.NoError(t, ln.Close())
		serverWg.Wait()
	}()

	impl := &ldapAuthImpl{}
	impl.SetEnableTLS(true)
	impl.SetLDAPServerHost("localhost")
	impl.SetLDAPServerPort(randomTLSServicePort)

	impl.caPool = x509.NewCertPool()
	require.True(t, impl.caPool.AppendCertsFromPEM(tlsCAStr))

	conn, err := impl.connectionFactory()
	require.NoError(t, err)
	defer conn.Close()
}
