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

// ldapTimeout is set to 10s. It works on both the TCP/TLS dialing timeout, and the LDAP request timeout. For connection with TLS, the
// user may find that it fails after 2*ldapTimeout, because TiDB will try to connect through both `StartTLS` (from a normal TCP connection)
// and `TLS`, therefore the total time is 2*ldapTimeout.
var ldapTimeout = 10 * time.Second

// skipTLSForTest is used to skip trying to connect with TLS directly in tests. If it's set to false, connection will only try to
// use `StartTLS`
var skipTLSForTest = false

// ldapAuthImpl gives the internal utilities of authentication with LDAP.
// The getter and setter methods will lock the mutex inside, while all other methods don't, so all other method call
// should be protected by `impl.Lock()`.
type ldapAuthImpl struct {
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

func (impl *ldapAuthImpl) searchUser(userName string) (dn string, err error) {
	var l *ldap.Conn

	l, err = impl.getConnection()
	if err != nil {
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

func (impl *ldapAuthImpl) initializeCAPool() error {
	if impl.caPath == "" {
		impl.caPool = nil
		return nil
	}

	impl.caPool = x509.NewCertPool()
	caCert, err := os.ReadFile(impl.caPath)
	if err != nil {
		return errors.Wrapf(err, "read ca certificate at %s", caCert)
	}

	ok := impl.caPool.AppendCertsFromPEM(caCert)
	if !ok {
		return errors.New("fail to parse ca certificate")
	}

	return nil
}

func (impl *ldapAuthImpl) tryConnectLDAPThroughStartTLS(address string) (*ldap.Conn, error) {
	ldapConnection, err := ldap.DialURL("ldap://"+address, ldap.DialWithDialer(&net.Dialer{
		Timeout: ldapTimeout,
	}))
	if err != nil {
		return nil, err
	}
	ldapConnection.SetTimeout(ldapTimeout)

	err = ldapConnection.StartTLS(&tls.Config{
		RootCAs:    impl.caPool,
		ServerName: impl.ldapServerHost,
		MinVersion: tls.VersionTLS12,
	})
	if err != nil {
		ldapConnection.Close()

		return nil, err
	}

	return ldapConnection, nil
}

func (impl *ldapAuthImpl) tryConnectLDAPThroughTLS(address string) (*ldap.Conn, error) {
	tlsConfig := &tls.Config{
		RootCAs:    impl.caPool,
		ServerName: impl.ldapServerHost,
		MinVersion: tls.VersionTLS12,
	}
	ldapConnection, err := ldap.DialURL("ldaps://"+address, ldap.DialWithTLSDialer(tlsConfig, &net.Dialer{
		Timeout: ldapTimeout,
	}))
	if err != nil {
		return nil, err
	}
	ldapConnection.SetTimeout(ldapTimeout)

	return ldapConnection, nil
}

func (impl *ldapAuthImpl) connectionFactory() (pools.Resource, error) {
	address := net.JoinHostPort(impl.ldapServerHost, strconv.FormatUint(uint64(impl.ldapServerPort), 10))

	// It's fine to load these two TLS configurations one-by-one (but not guarded by a single lock), because there isn't
	// a way to set two variables atomically.
	if impl.enableTLS {
		ldapConnection, err := impl.tryConnectLDAPThroughStartTLS(address)
		if err != nil {
			if intest.InTest && skipTLSForTest {
				return nil, err
			}

			ldapConnection, err = impl.tryConnectLDAPThroughTLS(address)
			if err != nil {
				return nil, errors.Wrap(err, "create ldap connection")
			}
		}

		return ldapConnection, nil
	}
	ldapConnection, err := ldap.DialURL("ldap://"+address, ldap.DialWithDialer(&net.Dialer{
		Timeout: ldapTimeout,
	}))
	if err != nil {
		return nil, errors.Wrap(err, "create ldap connection")
	}
	ldapConnection.SetTimeout(ldapTimeout)

	return ldapConnection, nil
}

const getConnectionMaxRetry = 10
const getConnectionRetryInterval = 500 * time.Millisecond

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

			// fail to bind to anonymous user, just release this connection and try to get a new one
			impl.ldapConnectionPool.Put(nil)

			retryCount++
			if retryCount >= getConnectionMaxRetry {
				return nil, errors.Wrap(err, "fail to bind to anonymous user")
			}
			// Be careful that it's still holding the lock of the system variables, so it's not good to sleep here.
			// TODO: refactor the `RWLock` to avoid the problem of holding the lock.
			time.Sleep(getConnectionRetryInterval)
			continue
		}

		return conn.(*ldap.Conn), nil
	}
}

func (impl *ldapAuthImpl) putConnection(conn *ldap.Conn) {
	impl.ldapConnectionPool.Put(conn)
}

func (impl *ldapAuthImpl) initializePool() {
	// skip re-initialization when the variables are not correct
	if impl.initCapacity > 0 && impl.maxCapacity >= impl.initCapacity {
		if impl.ldapConnectionPool != nil {
			impl.ldapConnectionPool.Close()
		}

		impl.ldapConnectionPool = pools.NewResourcePool(impl.connectionFactory, impl.initCapacity, impl.maxCapacity, 0)
	}
}

// SetBindBaseDN updates the BaseDN used to search the user
func (impl *ldapAuthImpl) SetBindBaseDN(bindBaseDN string) {
	impl.Lock()
	defer impl.Unlock()

	impl.bindBaseDN = bindBaseDN
}

// SetBindRootDN updates the RootDN. Before searching the users, the connection will bind
// this root user.
func (impl *ldapAuthImpl) SetBindRootDN(bindRootDN string) {
	impl.Lock()
	defer impl.Unlock()

	impl.bindRootDN = bindRootDN
}

// SetBindRootPW updates the password of the user specified by `rootDN`.
func (impl *ldapAuthImpl) SetBindRootPW(bindRootPW string) {
	impl.Lock()
	defer impl.Unlock()

	impl.bindRootPWD = bindRootPW
}

// SetSearchAttr updates the search attributes.
func (impl *ldapAuthImpl) SetSearchAttr(searchAttr string) {
	impl.Lock()
	defer impl.Unlock()

	impl.searchAttr = searchAttr
}

// SetLDAPServerHost updates the host of LDAP server
func (impl *ldapAuthImpl) SetLDAPServerHost(ldapServerHost string) {
	impl.Lock()
	defer impl.Unlock()

	if ldapServerHost != impl.ldapServerHost {
		impl.ldapServerHost = ldapServerHost
		impl.initializePool()
	}
}

// SetLDAPServerPort updates the port of LDAP server
func (impl *ldapAuthImpl) SetLDAPServerPort(ldapServerPort int) {
	impl.Lock()
	defer impl.Unlock()

	if ldapServerPort != impl.ldapServerPort {
		impl.ldapServerPort = ldapServerPort
		impl.initializePool()
	}
}

// SetEnableTLS sets whether to enable StartTLS for LDAP connection
func (impl *ldapAuthImpl) SetEnableTLS(enableTLS bool) {
	impl.Lock()
	defer impl.Unlock()

	if enableTLS != impl.enableTLS {
		impl.enableTLS = enableTLS
		impl.initializePool()
	}
}

// SetCAPath sets the path of CA certificate used to connect to LDAP server
func (impl *ldapAuthImpl) SetCAPath(path string) error {
	impl.Lock()
	defer impl.Unlock()

	if path != impl.caPath {
		impl.caPath = path
		err := impl.initializeCAPool()
		if err != nil {
			return err
		}
	}

	return nil
}

func (impl *ldapAuthImpl) SetInitCapacity(initCapacity int) {
	impl.Lock()
	defer impl.Unlock()

	if initCapacity != impl.initCapacity {
		impl.initCapacity = initCapacity
		impl.initializePool()
	}
}

func (impl *ldapAuthImpl) SetMaxCapacity(maxCapacity int) {
	impl.Lock()
	defer impl.Unlock()

	if maxCapacity != impl.maxCapacity {
		impl.maxCapacity = maxCapacity
		impl.initializePool()
	}
}

// GetBindBaseDN returns the BaseDN used to search the user
func (impl *ldapAuthImpl) GetBindBaseDN() string {
	impl.RLock()
	defer impl.RUnlock()

	return impl.bindBaseDN
}

// GetBindRootDN returns the RootDN. Before searching the users, the connection will bind
// this root user.
func (impl *ldapAuthImpl) GetBindRootDN() string {
	impl.RLock()
	defer impl.RUnlock()

	return impl.bindRootDN
}

// GetBindRootPW returns the password of the user specified by `rootDN`.
func (impl *ldapAuthImpl) GetBindRootPW() string {
	impl.RLock()
	defer impl.RUnlock()

	return impl.bindRootPWD
}

// GetSearchAttr returns the search attributes.
func (impl *ldapAuthImpl) GetSearchAttr() string {
	impl.RLock()
	defer impl.RUnlock()

	return impl.searchAttr
}

// GetLDAPServerHost returns the host of LDAP server
func (impl *ldapAuthImpl) GetLDAPServerHost() string {
	impl.RLock()
	defer impl.RUnlock()

	return impl.ldapServerHost
}

// GetLDAPServerPort returns the port of LDAP server
func (impl *ldapAuthImpl) GetLDAPServerPort() int {
	impl.RLock()
	defer impl.RUnlock()

	return impl.ldapServerPort
}

// GetEnableTLS sets whether to enable StartTLS for LDAP connection
func (impl *ldapAuthImpl) GetEnableTLS() bool {
	impl.RLock()
	defer impl.RUnlock()

	return impl.enableTLS
}

// GetCAPath returns the path of CA certificate used to connect to LDAP server
func (impl *ldapAuthImpl) GetCAPath() string {
	impl.RLock()
	defer impl.RUnlock()

	return impl.caPath
}

func (impl *ldapAuthImpl) GetInitCapacity() int {
	impl.RLock()
	defer impl.RUnlock()

	return impl.initCapacity
}

func (impl *ldapAuthImpl) GetMaxCapacity() int {
	impl.RLock()
	defer impl.RUnlock()

	return impl.maxCapacity
}
