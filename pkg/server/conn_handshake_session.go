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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os/user"
	"sync/atomic"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
	servererr "github.com/pingcap/tidb/pkg/server/err"
	"github.com/pingcap/tidb/pkg/server/internal/column"
	"github.com/pingcap/tidb/pkg/server/internal/handshake"
	util2 "github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

func (cc *clientConn) SessionStatusToString() string {
	status := cc.ctx.Status()
	inTxn, autoCommit := 0, 0
	if status&mysql.ServerStatusInTrans > 0 {
		inTxn = 1
	}
	if status&mysql.ServerStatusAutocommit > 0 {
		autoCommit = 1
	}
	return fmt.Sprintf("inTxn:%d, autocommit:%d",
		inTxn, autoCommit,
	)
}

func (cc *clientConn) openSession() error {
	var tlsStatePtr *tls.ConnectionState
	if cc.tlsConn != nil {
		tlsState := cc.tlsConn.ConnectionState()
		tlsStatePtr = &tlsState
	}
	ctx, err := cc.server.driver.OpenCtx(cc.connectionID, cc.capability, cc.collation, cc.dbname, tlsStatePtr, cc.extensions)
	if err != nil {
		return err
	}
	cc.SetCtx(ctx)

	err = cc.server.checkConnectionCount()
	if err != nil {
		return err
	}
	return nil
}

func (cc *clientConn) openSessionAndDoAuth(authData []byte, authPlugin string, zstdLevel int) error {
	// Open a context unless this was done before.
	if ctx := cc.getCtx(); ctx == nil {
		err := cc.openSession()
		if err != nil {
			return err
		}
	}

	hasPassword := "YES"
	if len(authData) == 0 {
		hasPassword = "NO"
	}

	host, port, err := cc.PeerHost(hasPassword, false)
	if err != nil {
		return err
	}

	err = cc.checkUserConnectionCount(host)
	if err != nil {
		return err
	}

	if !cc.isUnixSocket && authPlugin == mysql.AuthSocket {
		return servererr.ErrAccessDeniedNoPassword.FastGenByArgs(cc.user, host)
	}

	userIdentity := &auth.UserIdentity{Username: cc.user, Hostname: host, AuthPlugin: authPlugin}
	if err = cc.ctx.Auth(userIdentity, authData, cc.salt, cc); err != nil {
		return err
	}
	cc.ctx.SetPort(port)
	cc.ctx.SetCompressionLevel(zstdLevel)
	if cc.dbname != "" {
		_, err = cc.useDB(context.Background(), cc.dbname)
		if err != nil {
			return err
		}
	}
	cc.ctx.SetSessionManager(cc.server)
	return nil
}

// mockOSUserForAuthSocketTest should only be used in test
var mockOSUserForAuthSocketTest atomic.Pointer[string]

// Check if the Authentication Plugin of the server, client and user configuration matches
func (cc *clientConn) checkAuthPlugin(ctx context.Context, resp *handshake.Response41) ([]byte, error) {
	// Open a context unless this was done before.
	if ctx := cc.getCtx(); ctx == nil {
		err := cc.openSession()
		if err != nil {
			return nil, err
		}
	}

	authData := resp.Auth
	// tidb_session_token is always permitted and skips stored user plugin.
	if resp.AuthPlugin == mysql.AuthTiDBSessionToken {
		return authData, nil
	}
	hasPassword := "YES"
	if len(authData) == 0 {
		hasPassword = "NO"
	}

	host, _, err := cc.PeerHost(hasPassword, false)
	if err != nil {
		return nil, err
	}
	// Find the identity of the user based on username and peer host.
	identity, err := cc.ctx.MatchIdentity(ctx, cc.user, host)
	if err != nil {
		return nil, servererr.ErrAccessDenied.FastGenByArgs(cc.user, host, hasPassword)
	}
	// Get the plugin for the identity.
	userplugin, err := cc.ctx.AuthPluginForUser(ctx, identity)
	if err != nil {
		logutil.Logger(ctx).Warn("Failed to get authentication method for user",
			zap.String("user", cc.user), zap.String("host", host))
	}
	failpoint.Inject("FakeUser", func(val failpoint.Value) {
		//nolint:forcetypeassert
		userplugin = val.(string)
	})
	if userplugin == mysql.AuthSocket {
		if !cc.isUnixSocket {
			return nil, servererr.ErrAccessDenied.FastGenByArgs(cc.user, host, hasPassword)
		}
		resp.AuthPlugin = mysql.AuthSocket
		user, err := user.LookupId(fmt.Sprint(cc.socketCredUID))
		if err != nil {
			return nil, err
		}
		uname := user.Username

		if intest.InTest {
			if p := mockOSUserForAuthSocketTest.Load(); p != nil {
				uname = *p
			}
		}

		return []byte(uname), nil
	}
	if len(userplugin) == 0 {
		// No user plugin set, assuming MySQL Native Password
		// This happens if the account doesn't exist or if the account doesn't have
		// a password set.
		if resp.AuthPlugin != mysql.AuthNativePassword && resp.Capability&mysql.ClientPluginAuth > 0 {
			resp.AuthPlugin = mysql.AuthNativePassword
			authData, err := cc.authSwitchRequest(ctx, mysql.AuthNativePassword)
			if err != nil {
				return nil, err
			}
			return authData, nil
		}
		return nil, nil
	}

	// If the authentication method send by the server (cc.authPlugin) doesn't match
	// the plugin configured for the user account in the mysql.user.plugin column
	// or if the authentication method send by the server doesn't match the authentication
	// method send by the client (*authPlugin) then we need to switch the authentication
	// method to match the one configured for that specific user.
	if (cc.authPlugin != userplugin) || (cc.authPlugin != resp.AuthPlugin) {
		if userplugin == mysql.AuthTiDBAuthToken {
			userplugin = mysql.AuthMySQLClearPassword
		}
		if resp.Capability&mysql.ClientPluginAuth > 0 {
			authData, err := cc.authSwitchRequest(ctx, userplugin)
			if err != nil {
				return nil, err
			}
			resp.AuthPlugin = userplugin
			return authData, nil
		} else if userplugin != mysql.AuthNativePassword {
			// MySQL 5.1 and older don't support authentication plugins yet
			return nil, servererr.ErrNotSupportedAuthMode
		}
	}

	return nil, nil
}

func (cc *clientConn) PeerHost(hasPassword string, update bool) (host, port string, err error) {
	// already get peer host
	if len(cc.peerHost) > 0 {
		// Proxy protocol enabled and not update
		if cc.ppEnabled && !update {
			return cc.peerHost, cc.peerPort, nil
		}
		// Proxy protocol not enabled
		if !cc.ppEnabled {
			return cc.peerHost, cc.peerPort, nil
		}
	}
	host = vardef.DefHostname
	if cc.isUnixSocket {
		cc.peerHost = host
		cc.serverHost = host
		return
	}
	addr := cc.bufReadConn.RemoteAddr().String()
	host, port, err = net.SplitHostPort(addr)
	if err != nil {
		err = servererr.ErrAccessDenied.GenWithStackByArgs(cc.user, addr, hasPassword)
		return
	}
	cc.peerHost = host
	cc.peerPort = port

	serverAddr := cc.bufReadConn.LocalAddr().String()
	serverHost, _, err := net.SplitHostPort(serverAddr)
	if err != nil {
		err = servererr.ErrAccessDenied.GenWithStackByArgs(cc.user, addr, hasPassword)
		return
	}
	cc.serverHost = serverHost

	return
}

// skipInitConnect follows MySQL's rules of when init-connect should be skipped.
// In 5.7 it is any user with SUPER privilege, but in 8.0 it is:
// - SUPER or the CONNECTION_ADMIN dynamic privilege.
// - (additional exception) users with expired passwords (not yet supported)
// In TiDB CONNECTION_ADMIN is satisfied by SUPER, so we only need to check once.
func (cc *clientConn) skipInitConnect() bool {
	checker := privilege.GetPrivilegeManager(cc.ctx.Session)
	activeRoles := cc.ctx.GetSessionVars().ActiveRoles
	return checker != nil && checker.RequestDynamicVerification(activeRoles, "CONNECTION_ADMIN", false)
}

// initResultEncoder initialize the result encoder for current connection.
func (cc *clientConn) initResultEncoder(ctx context.Context) {
	chs, err := cc.ctx.GetSessionVars().GetSessionOrGlobalSystemVar(context.Background(), vardef.CharacterSetResults)
	if err != nil {
		chs = ""
		logutil.Logger(ctx).Warn("get character_set_results system variable failed", zap.Error(err))
	}
	cc.rsEncoder = column.NewResultEncoder(chs)
}

func (cc *clientConn) initInputEncoder(ctx context.Context) {
	chs, err := cc.ctx.GetSessionVars().GetSessionOrGlobalSystemVar(context.Background(), vardef.CharacterSetClient)
	if err != nil {
		chs = ""
		logutil.Logger(ctx).Warn("get character_set_client system variable failed", zap.Error(err))
	}
	cc.inputDecoder = util2.NewInputDecoder(chs)
}

// initConnect runs the initConnect SQL statement if it has been specified.
// The semantics are MySQL compatible.
func (cc *clientConn) initConnect(ctx context.Context) error {
	val, err := cc.ctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(vardef.InitConnect)
	if err != nil {
		return err
	}
	if val == "" || cc.skipInitConnect() {
		return nil
	}
	logutil.Logger(ctx).Debug("init_connect starting")
	stmts, err := cc.ctx.Parse(ctx, val)
	if err != nil {
		return err
	}
	for _, stmt := range stmts {
		rs, err := cc.ctx.ExecuteStmt(ctx, stmt)
		if err != nil {
			return err
		}
		// init_connect does not care about the results,
		// but they need to be drained because of lazy loading.
		if rs != nil {
			req := rs.NewChunk(nil)
			for {
				if err = rs.Next(ctx, req); err != nil {
					return err
				}
				if req.NumRows() == 0 {
					break
				}
			}
			rs.Close()
		}
	}
	logutil.Logger(ctx).Debug("init_connect complete")
	return nil
}
