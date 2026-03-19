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
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

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
	var randomTLSServiceAddress string
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
		ln, err = tls.Listen("tcp", ":0", tlsConfig)

		require.NoError(t, err)

		randomTLSServiceAddress = ln.Addr().String()
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
	_, port, err := net.SplitHostPort(randomTLSServiceAddress)
	require.NoError(t, err)
	p, err := strconv.Atoi(port)
	require.NoError(t, err)
	impl.SetLDAPServerPort(p)

	impl.caPool = x509.NewCertPool()
	require.True(t, impl.caPool.AppendCertsFromPEM(tlsCAStr))

	conn, err := impl.connectionFactory()
	require.NoError(t, err)
	defer conn.Close()
}

func TestConnectWithTLS11(t *testing.T) {
	var ln net.Listener

	startListen := make(chan error, 1)
	serverWg := &sync.WaitGroup{}
	serverWg.Add(1)
	go func() {
		defer serverWg.Done()

		cert, err := tls.X509KeyPair(tlsCrtStr, tlsKeyStr)
		if err != nil {
			startListen <- err
			return
		}
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			MaxVersion:   tls.VersionTLS11,
		}
		ln, err = tls.Listen("tcp", "localhost:0", tlsConfig)
		startListen <- err
		if err != nil {
			return
		}

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

	require.NoError(t, <-startListen)
	defer func() {
		require.NoError(t, ln.Close())
		serverWg.Wait()
	}()

	impl := &ldapAuthImpl{}
	impl.SetEnableTLS(true)
	impl.SetLDAPServerHost("localhost")
	_, port, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)
	p, err := strconv.Atoi(port)
	require.NoError(t, err)
	impl.SetLDAPServerPort(p)

	impl.caPool = x509.NewCertPool()
	require.True(t, impl.caPool.AppendCertsFromPEM(tlsCAStr))

	_, err = impl.connectionFactory()
	require.ErrorContains(t, err, "protocol version not supported")
}

func TestLDAPStartTLSTimeout(t *testing.T) {
	originalTimeout := ldapTimeout
	ldapTimeout = time.Second * 2
	skipTLSForTest = true
	defer func() {
		ldapTimeout = originalTimeout
		skipTLSForTest = false
	}()

	var ln net.Listener
	startListen := make(chan struct{})
	afterTimeout := make(chan struct{})
	defer close(afterTimeout)

	// this test only tests whether the LDAP with LTS enabled will fallback from StartTLS
	startListenErr := make(chan error, 1)
	serverWg := &sync.WaitGroup{}
	serverWg.Add(1)
	go func() {
		var err error
		defer serverWg.Done()

		ln, err = net.Listen("tcp", "localhost:0")
		startListenErr <- err
		if err != nil {
			return
		}
		startListen <- struct{}{}

		conn, err := ln.Accept()
		require.NoError(t, err)

		<-afterTimeout
		require.NoError(t, conn.Close())

		// close the server
		require.NoError(t, ln.Close())
	}()

	require.NoError(t, <-startListenErr)
	<-startListen
	defer func() {
		serverWg.Wait()
	}()

	impl := &ldapAuthImpl{}
	impl.SetEnableTLS(true)
	impl.SetLDAPServerHost("localhost")
	_, port, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)
	p, err := strconv.Atoi(port)
	require.NoError(t, err)
	impl.SetLDAPServerPort(p)

	impl.caPool = x509.NewCertPool()
	require.True(t, impl.caPool.AppendCertsFromPEM(tlsCAStr))
	impl.SetInitCapacity(1)
	impl.SetMaxCapacity(1)

	now := time.Now()
	_, err = impl.connectionFactory()
	afterTimeout <- struct{}{}
	dur := time.Since(now)
	require.Greater(t, dur, 2*time.Second)
	require.Less(t, dur, 3*time.Second)
	require.ErrorContains(t, err, "connection timed out")
}
