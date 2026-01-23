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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/go-ldap/ldap/v3"
	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const getConnectionMaxRetry = 10
const getConnectionRetryInterval = 500 * time.Millisecond

// ldapTimeout is set to 10s. It works on both the TCP/TLS dialing timeout, and the LDAP request timeout. For connection with TLS, the
// user may find that it fails after 2*ldapTimeout, because TiDB will try to connect through both `StartTLS` (from a normal TCP connection)
// and `TLS`, therefore the total time is 2*ldapTimeout.
var ldapTimeout = 10 * time.Second

// skipTLSForTest is used to skip trying to connect with TLS directly in tests. If it's set to false, connection will only try to
// use `StartTLS`
var skipTLSForTest = false

// ldapAuthImplBuilder builds a new `*ldapAuthImpl` from the current configuration.
type ldapAuthImplBuilder struct {
	sync.RWMutex
	// the following attributes are used to search the users
	bindBaseDN  string
	bindRootDN  string
	bindRootPWD string
	searchAttr  string

	// the following attributes are used to connect to LDAP server
	ldapServerHost string
	ldapServerPort int
	enableTLS      bool
	caPath         string
	initCapacity   int
	maxCapacity    int

	caPool *x509.CertPool

	ldapConnectionPool *pools.ResourcePool
}

func (builder *ldapAuthImplBuilder) build() *ldapAuthImpl {
	builder.RLock()
	defer builder.RUnlock()

	return &ldapAuthImpl{
		bindBaseDN:         builder.bindBaseDN,
		bindRootDN:         builder.bindRootDN,
		bindRootPWD:        builder.bindRootPWD,
		searchAttr:         builder.searchAttr,
		ldapConnectionPool: builder.ldapConnectionPool,
	}
}

func (builder *ldapAuthImplBuilder) initializeCAPool() error {
	if builder.caPath == "" {
		builder.caPool = nil
		return nil
	}

	builder.caPool = x509.NewCertPool()
	caCert, err := os.ReadFile(builder.caPath)
	if err != nil {
		return errors.Wrapf(err, "read ca certificate at %s", caCert)
	}

	ok := builder.caPool.AppendCertsFromPEM(caCert)
	if !ok {
		return errors.New("fail to parse ca certificate")
	}

	return nil
}

func tryConnectLDAPThroughStartTLS(address string, tlsConfig *tls.Config) (*ldap.Conn, error) {
	ldapConnection, err := ldap.DialURL("ldap://"+address, ldap.DialWithDialer(&net.Dialer{
		Timeout: ldapTimeout,
	}))
	if err != nil {
		return nil, err
	}
	ldapConnection.SetTimeout(ldapTimeout)

	err = ldapConnection.StartTLS(tlsConfig)
	if err != nil {
		ldapConnection.Close()

		return nil, err
	}
	return ldapConnection, nil
}

func tryConnectLDAPThroughTLS(address string, tlsConfig *tls.Config) (*ldap.Conn, error) {
	ldapConnection, err := ldap.DialURL("ldaps://"+address, ldap.DialWithTLSDialer(tlsConfig, &net.Dialer{
		Timeout: ldapTimeout,
	}))
	if err != nil {
		return nil, err
	}
	ldapConnection.SetTimeout(ldapTimeout)

	return ldapConnection, nil
}

func ldapConnectionFactory(address string, tlsConfig *tls.Config) func() (pools.Resource, error) {
	if tlsConfig != nil {
		return func() (pools.Resource, error) {
			ldapConnection, err := tryConnectLDAPThroughStartTLS(address, tlsConfig)
			if err != nil {
				if intest.InTest && skipTLSForTest {
					return nil, errors.Wrap(err, "create ldap connection")
				}

				ldapConnection, err = tryConnectLDAPThroughTLS(address, tlsConfig)
				if err != nil {
					return nil, errors.Wrap(err, "create ldap connection")
				}
			}

			return ldapConnection, nil
		}
	}

	return func() (pools.Resource, error) {
		ldapConnection, err := ldap.DialURL("ldap://"+address, ldap.DialWithDialer(
			&net.Dialer{
				Timeout: ldapTimeout,
			},
		))
		if err != nil {
			return nil, errors.Wrap(err, "create ldap connection")
		}

		return ldapConnection, nil
	}
}

func (builder *ldapAuthImplBuilder) initializePool() {
	// skip re-initialization when the variables are not correct
	if builder.initCapacity > 0 && builder.maxCapacity >= builder.initCapacity {
		if builder.ldapConnectionPool != nil {
			builder.ldapConnectionPool.Close()
		}

		address := net.JoinHostPort(builder.ldapServerHost, strconv.FormatUint(uint64(builder.ldapServerPort), 10))
		var tlsConfig *tls.Config
		if builder.enableTLS {
			tlsConfig = &tls.Config{
				RootCAs:    builder.caPool,
				ServerName: builder.ldapServerHost,
				MinVersion: tls.VersionTLS12,
			}
		}

		builder.ldapConnectionPool = pools.NewResourcePool(ldapConnectionFactory(address, tlsConfig), builder.initCapacity, builder.maxCapacity, 0)
	}
}

// SetBindBaseDN updates the BaseDN used to search the user
func (builder *ldapAuthImplBuilder) SetBindBaseDN(bindBaseDN string) {
	builder.Lock()
	defer builder.Unlock()

	builder.bindBaseDN = bindBaseDN
}

// SetBindRootDN updates the RootDN. Before searching the users, the connection will bind
// this root user.
func (builder *ldapAuthImplBuilder) SetBindRootDN(bindRootDN string) {
	builder.Lock()
	defer builder.Unlock()

	builder.bindRootDN = bindRootDN
}

// SetBindRootPW updates the password of the user specified by `rootDN`.
func (builder *ldapAuthImplBuilder) SetBindRootPW(bindRootPW string) {
	builder.Lock()
	defer builder.Unlock()

	builder.bindRootPWD = bindRootPW
}

// SetSearchAttr updates the search attributes.
func (builder *ldapAuthImplBuilder) SetSearchAttr(searchAttr string) {
	builder.Lock()
	defer builder.Unlock()

	builder.searchAttr = searchAttr
}

// SetLDAPServerHost updates the host of LDAP server
func (builder *ldapAuthImplBuilder) SetLDAPServerHost(ldapServerHost string) {
	builder.Lock()
	defer builder.Unlock()

	if ldapServerHost != builder.ldapServerHost {
		builder.ldapServerHost = ldapServerHost
		builder.initializePool()
	}
}

// SetLDAPServerPort updates the port of LDAP server
func (builder *ldapAuthImplBuilder) SetLDAPServerPort(ldapServerPort int) {
	builder.Lock()
	defer builder.Unlock()

	if ldapServerPort != builder.ldapServerPort {
		builder.ldapServerPort = ldapServerPort
		builder.initializePool()
	}
}

// SetEnableTLS sets whether to enable StartTLS for LDAP connection
func (builder *ldapAuthImplBuilder) SetEnableTLS(enableTLS bool) {
	builder.Lock()
	defer builder.Unlock()

	if enableTLS != builder.enableTLS {
		builder.enableTLS = enableTLS
		builder.initializePool()
	}
}

// SetCAPath sets the path of CA certificate used to connect to LDAP server
func (builder *ldapAuthImplBuilder) SetCAPath(path string) error {
	builder.Lock()
	defer builder.Unlock()

	if path != builder.caPath {
		builder.caPath = path
		err := builder.initializeCAPool()
		if err != nil {
			return err
		}
		builder.initializePool()
	}

	return nil
}

func (builder *ldapAuthImplBuilder) SetInitCapacity(initCapacity int) {
	builder.Lock()
	defer builder.Unlock()

	if initCapacity != builder.initCapacity {
		builder.initCapacity = initCapacity
		builder.initializePool()
	}
}

func (builder *ldapAuthImplBuilder) SetMaxCapacity(maxCapacity int) {
	builder.Lock()
	defer builder.Unlock()

	if maxCapacity != builder.maxCapacity {
		builder.maxCapacity = maxCapacity
		builder.initializePool()
	}
}

// GetBindBaseDN returns the BaseDN used to search the user
func (builder *ldapAuthImplBuilder) GetBindBaseDN() string {
	builder.RLock()
	defer builder.RUnlock()

	return builder.bindBaseDN
}

// GetBindRootDN returns the RootDN. Before searching the users, the connection will bind
// this root user.
func (builder *ldapAuthImplBuilder) GetBindRootDN() string {
	builder.RLock()
	defer builder.RUnlock()

	return builder.bindRootDN
}

// GetBindRootPW returns the password of the user specified by `rootDN`.
func (builder *ldapAuthImplBuilder) GetBindRootPW() string {
	builder.RLock()
	defer builder.RUnlock()

	return builder.bindRootPWD
}

// GetSearchAttr returns the search attributes.
func (builder *ldapAuthImplBuilder) GetSearchAttr() string {
	builder.RLock()
	defer builder.RUnlock()

	return builder.searchAttr
}

// GetLDAPServerHost returns the host of LDAP server
func (builder *ldapAuthImplBuilder) GetLDAPServerHost() string {
	builder.RLock()
	defer builder.RUnlock()

	return builder.ldapServerHost
}

// GetLDAPServerPort returns the port of LDAP server
func (builder *ldapAuthImplBuilder) GetLDAPServerPort() int {
	builder.RLock()
	defer builder.RUnlock()

	return builder.ldapServerPort
}

// GetEnableTLS sets whether to enable StartTLS for LDAP connection
func (builder *ldapAuthImplBuilder) GetEnableTLS() bool {
	builder.RLock()
	defer builder.RUnlock()

	return builder.enableTLS
}

// GetCAPath returns the path of CA certificate used to connect to LDAP server
func (builder *ldapAuthImplBuilder) GetCAPath() string {
	builder.RLock()
	defer builder.RUnlock()

	return builder.caPath
}

func (builder *ldapAuthImplBuilder) GetInitCapacity() int {
	builder.RLock()
	defer builder.RUnlock()

	return builder.initCapacity
}

func (builder *ldapAuthImplBuilder) GetMaxCapacity() int {
	builder.RLock()
	defer builder.RUnlock()

	return builder.maxCapacity
}

// ldapAuthImpl gives the internal utilities of authentication with LDAP.
type ldapAuthImpl struct {
	bindBaseDN  string
	bindRootDN  string
	bindRootPWD string
	searchAttr  string

	ldapConnectionPool *pools.ResourcePool
}

func (impl *ldapAuthImpl) searchUser(userName string) (dn string, err error) {
	var l *ldap.Conn

	l, err = impl.getConnection()
	if err != nil {
		logutil.BgLogger().Error("fail to create ldap connection", zap.Error(err))
		return "", err
	}
	defer impl.putConnection(l)

	err = l.Bind(impl.bindRootDN, impl.bindRootPWD)
	if err != nil {
		return "", errors.Wrap(err, "bind root dn to search user")
	}

	result, err := l.Search(&ldap.SearchRequest{
		BaseDN: impl.bindBaseDN,
		Scope:  ldap.ScopeWholeSubtree,
		Filter: fmt.Sprintf("(%s=%s)", impl.searchAttr, userName),
	})
	if err != nil {
		return
	}

	if len(result.Entries) == 0 {
		return "", errors.New("LDAP user not found")
	}

	dn = result.Entries[0].DN
	return
}

// canonicalizeDN turns the `dn` provided in database to the `dn` recognized by LDAP server
// If the first byte of `dn` is `+`, it'll be converted into "${searchAttr}=${username},..."
// both `userName` and `dn` should be non-empty
func (impl *ldapAuthImpl) canonicalizeDN(userName string, dn string) string {
	if dn[0] == '+' {
		return fmt.Sprintf("%s=%s,%s", impl.searchAttr, userName, dn[1:])
	}

	return dn
}

func (impl *ldapAuthImpl) getConnection() (*ldap.Conn, error) {
	retryCount := 0
	for {
		conn, err := impl.ldapConnectionPool.Get()
		if err != nil {
			return nil, err
		}

		// try to bind root user. It has two meanings:
		// 1. Clear the state of previous binding, to avoid security leaks. (Though it's not serious, because even the current
		//   connection has binded to other users, the following authentication will still fail. But the ACL for root
		//   user and a valid user could be different, so it's better to bind back to root user here.
		// 2. Detect whether this connection is still valid to use, in case the server has closed this connection.
		ldapConnection := conn.(*ldap.Conn)
		_, err = ldapConnection.SimpleBind(&ldap.SimpleBindRequest{
			Username: impl.bindRootDN,
			Password: impl.bindRootPWD,
		})
		if err != nil {
			logutil.BgLogger().Warn("fail to use LDAP connection bind to anonymous user. Retrying", zap.Error(err),
				zap.Duration("backoff", getConnectionRetryInterval))
			ldapConnection.Close()

			// fail to bind to anonymous user, just release this connection and try to get a new one
			impl.ldapConnectionPool.Put(nil)

			retryCount++
			if retryCount >= getConnectionMaxRetry {
				return nil, errors.Wrap(err, "fail to bind to anonymous user")
			}

			time.Sleep(getConnectionRetryInterval)
			continue
		}

		return conn.(*ldap.Conn), nil
	}
}

func (impl *ldapAuthImpl) putConnection(conn *ldap.Conn) {
	impl.ldapConnectionPool.Put(conn)
}
