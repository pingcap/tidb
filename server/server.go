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

// The MIT License (MIT)
//
// Copyright (c) 2014 wandoulabs
// Copyright (c) 2014 siddontang
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http" //nolint:goimports
	// For pprof
	_ "net/http/pprof" // #nosec G108
	"os"
	"os/user"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/blacktear23/go-proxyprotocol"
	"github.com/pingcap/errors"
	autoid "github.com/pingcap/tidb/autoid_service"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/extension"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/session/txninfo"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/fastrand"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sys/linux"
	"github.com/pingcap/tidb/util/timeutil"
	uatomic "go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	serverPID int
	osUser    string
	osVersion string
	// RunInGoTest represents whether we are run code in test.
	RunInGoTest bool
)

func init() {
	serverPID = os.Getpid()
	currentUser, err := user.Current()
	if err != nil {
		osUser = ""
	} else {
		osUser = currentUser.Name
	}
	osVersion, err = linux.OSVersion()
	if err != nil {
		osVersion = ""
	}
}

var (
	errUnknownFieldType        = dbterror.ClassServer.NewStd(errno.ErrUnknownFieldType)
	errInvalidSequence         = dbterror.ClassServer.NewStd(errno.ErrInvalidSequence)
	errInvalidType             = dbterror.ClassServer.NewStd(errno.ErrInvalidType)
	errNotAllowedCommand       = dbterror.ClassServer.NewStd(errno.ErrNotAllowedCommand)
	errAccessDenied            = dbterror.ClassServer.NewStd(errno.ErrAccessDenied)
	errAccessDeniedNoPassword  = dbterror.ClassServer.NewStd(errno.ErrAccessDeniedNoPassword)
	errConCount                = dbterror.ClassServer.NewStd(errno.ErrConCount)
	errSecureTransportRequired = dbterror.ClassServer.NewStd(errno.ErrSecureTransportRequired)
	errMultiStatementDisabled  = dbterror.ClassServer.NewStd(errno.ErrMultiStatementDisabled)
	errNewAbortingConnection   = dbterror.ClassServer.NewStd(errno.ErrNewAbortingConnection)
	errNotSupportedAuthMode    = dbterror.ClassServer.NewStd(errno.ErrNotSupportedAuthMode)
	errNetPacketTooLarge       = dbterror.ClassServer.NewStd(errno.ErrNetPacketTooLarge)
	errMustChangePassword      = dbterror.ClassServer.NewStd(errno.ErrMustChangePassword)
)

// DefaultCapability is the capability of the server when it is created using the default configuration.
// When server is configured with SSL, the server will have extra capabilities compared to DefaultCapability.
const defaultCapability = mysql.ClientLongPassword | mysql.ClientLongFlag |
	mysql.ClientConnectWithDB | mysql.ClientProtocol41 |
	mysql.ClientTransactions | mysql.ClientSecureConnection | mysql.ClientFoundRows |
	mysql.ClientMultiStatements | mysql.ClientMultiResults | mysql.ClientLocalFiles |
	mysql.ClientConnectAtts | mysql.ClientPluginAuth | mysql.ClientInteractive | mysql.ClientDeprecateEOF

// Server is the MySQL protocol server
type Server struct {
	cfg               *config.Config
	tlsConfig         unsafe.Pointer // *tls.Config
	driver            IDriver
	listener          net.Listener
	socket            net.Listener
	concurrentLimiter *TokenLimiter

	rwlock  sync.RWMutex
	clients map[uint64]*clientConn

	capability   uint32
	dom          *domain.Domain
	globalConnID util.GlobalConnID

	statusAddr     string
	statusListener net.Listener
	statusServer   *http.Server
	grpcServer     *grpc.Server
	inShutdownMode *uatomic.Bool
	health         *uatomic.Bool

	sessionMapMutex     sync.Mutex
	internalSessions    map[interface{}]struct{}
	autoIDService       *autoid.Service
	authTokenCancelFunc context.CancelFunc
	wg                  sync.WaitGroup
	printMDLLogTime     time.Time
}

// ConnectionCount gets current connection count.
func (s *Server) ConnectionCount() int {
	s.rwlock.RLock()
	cnt := len(s.clients)
	s.rwlock.RUnlock()
	return cnt
}

func (s *Server) getToken() *Token {
	start := time.Now()
	tok := s.concurrentLimiter.Get()
	metrics.TokenGauge.Inc()
	// Note that data smaller than one microsecond is ignored, because that case can be viewed as non-block.
	metrics.GetTokenDurationHistogram.Observe(float64(time.Since(start).Nanoseconds() / 1e3))
	return tok
}

func (s *Server) releaseToken(token *Token) {
	s.concurrentLimiter.Put(token)
	metrics.TokenGauge.Dec()
}

// SetDomain use to set the server domain.
func (s *Server) SetDomain(dom *domain.Domain) {
	s.dom = dom
}

// InitGlobalConnID initialize global connection id.
func (s *Server) InitGlobalConnID(serverIDGetter func() uint64) {
	s.globalConnID = util.NewGlobalConnIDWithGetter(serverIDGetter, true)
}

// newConn creates a new *clientConn from a net.Conn.
// It allocates a connection ID and random salt data for authentication.
func (s *Server) newConn(conn net.Conn) *clientConn {
	cc := newClientConn(s)
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetKeepAlive(s.cfg.Performance.TCPKeepAlive); err != nil {
			logutil.BgLogger().Error("failed to set tcp keep alive option", zap.Error(err))
		}
		if err := tcpConn.SetNoDelay(s.cfg.Performance.TCPNoDelay); err != nil {
			logutil.BgLogger().Error("failed to set tcp no delay option", zap.Error(err))
		}
	}
	cc.setConn(conn)
	cc.salt = fastrand.Buf(20)
	return cc
}

// NewServer creates a new Server.
func NewServer(cfg *config.Config, driver IDriver) (*Server, error) {
	s := &Server{
		cfg:               cfg,
		driver:            driver,
		concurrentLimiter: NewTokenLimiter(cfg.TokenLimit),
		clients:           make(map[uint64]*clientConn),
		globalConnID:      util.NewGlobalConnID(0, true),
		internalSessions:  make(map[interface{}]struct{}, 100),
		health:            uatomic.NewBool(true),
		inShutdownMode:    uatomic.NewBool(false),
		printMDLLogTime:   time.Now(),
	}
	s.capability = defaultCapability
	setTxnScope()
	setSystemTimeZoneVariable()

	tlsConfig, autoReload, err := util.LoadTLSCertificates(
		s.cfg.Security.SSLCA, s.cfg.Security.SSLKey, s.cfg.Security.SSLCert,
		s.cfg.Security.AutoTLS, s.cfg.Security.RSAKeySize)

	// LoadTLSCertificates will auto generate certificates if autoTLS is enabled.
	// It only returns an error if certificates are specified and invalid.
	// In which case, we should halt server startup as a misconfiguration could
	// lead to a connection downgrade.
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Automatically reload auto-generated certificates.
	// The certificates are re-created every 30 days and are valid for 90 days.
	if autoReload {
		go func() {
			for range time.Tick(time.Hour * 24 * 30) { // 30 days
				logutil.BgLogger().Info("Rotating automatically created TLS Certificates")
				tlsConfig, _, err = util.LoadTLSCertificates(
					s.cfg.Security.SSLCA, s.cfg.Security.SSLKey, s.cfg.Security.SSLCert,
					s.cfg.Security.AutoTLS, s.cfg.Security.RSAKeySize)
				if err != nil {
					logutil.BgLogger().Warn("TLS Certificate rotation failed", zap.Error(err))
				}
				atomic.StorePointer(&s.tlsConfig, unsafe.Pointer(tlsConfig))
			}
		}()
	}

	if tlsConfig != nil {
		setSSLVariable(s.cfg.Security.SSLCA, s.cfg.Security.SSLKey, s.cfg.Security.SSLCert)
		atomic.StorePointer(&s.tlsConfig, unsafe.Pointer(tlsConfig))
		logutil.BgLogger().Info("mysql protocol server secure connection is enabled",
			zap.Bool("client verification enabled", len(variable.GetSysVar("ssl_ca").Value) > 0))
	}
	if s.tlsConfig != nil {
		s.capability |= mysql.ClientSSL
	}

	if s.cfg.Host != "" && (s.cfg.Port != 0 || RunInGoTest) {
		addr := net.JoinHostPort(s.cfg.Host, strconv.Itoa(int(s.cfg.Port)))
		tcpProto := "tcp"
		if s.cfg.EnableTCP4Only {
			tcpProto = "tcp4"
		}
		if s.listener, err = net.Listen(tcpProto, addr); err != nil {
			return nil, errors.Trace(err)
		}
		logutil.BgLogger().Info("server is running MySQL protocol", zap.String("addr", addr))
		if RunInGoTest && s.cfg.Port == 0 {
			s.cfg.Port = uint(s.listener.Addr().(*net.TCPAddr).Port)
		}
	}

	if s.cfg.Socket != "" {
		if err := cleanupStaleSocket(s.cfg.Socket); err != nil {
			return nil, errors.Trace(err)
		}

		if s.socket, err = net.Listen("unix", s.cfg.Socket); err != nil {
			return nil, errors.Trace(err)
		}
		logutil.BgLogger().Info("server is running MySQL protocol", zap.String("socket", s.cfg.Socket))
	}

	if s.socket == nil && s.listener == nil {
		err = errors.New("Server not configured to listen on either -socket or -host and -port")
		return nil, errors.Trace(err)
	}

	if s.cfg.ProxyProtocol.Networks != "" {
		proxyTarget := s.listener
		if proxyTarget == nil {
			proxyTarget = s.socket
		}
		ppListener, err := proxyprotocol.NewLazyListener(proxyTarget, s.cfg.ProxyProtocol.Networks,
			int(s.cfg.ProxyProtocol.HeaderTimeout), s.cfg.ProxyProtocol.Fallbackable)
		if err != nil {
			logutil.BgLogger().Error("ProxyProtocol networks parameter invalid")
			return nil, errors.Trace(err)
		}
		if s.listener != nil {
			s.listener = ppListener
			logutil.BgLogger().Info("server is running MySQL protocol (through PROXY protocol)", zap.String("host", s.cfg.Host))
		} else {
			s.socket = ppListener
			logutil.BgLogger().Info("server is running MySQL protocol (through PROXY protocol)", zap.String("socket", s.cfg.Socket))
		}
	}

	if s.cfg.Status.ReportStatus {
		if err = s.listenStatusHTTPServer(); err != nil {
			return nil, errors.Trace(err)
		}
	}

	// Automatically reload JWKS for tidb_auth_token.
	if len(s.cfg.Security.AuthTokenJWKS) > 0 {
		var (
			timeInterval time.Duration
			err          error
			ctx          context.Context
		)
		if timeInterval, err = time.ParseDuration(s.cfg.Security.AuthTokenRefreshInterval); err != nil {
			logutil.BgLogger().Error("Fail to parse security.auth-token-refresh-interval. Use default value",
				zap.String("security.auth-token-refresh-interval", s.cfg.Security.AuthTokenRefreshInterval))
			timeInterval = config.DefAuthTokenRefreshInterval
		}
		ctx, s.authTokenCancelFunc = context.WithCancel(context.Background())
		if err = privileges.GlobalJWKS.LoadJWKS4AuthToken(ctx, &s.wg, s.cfg.Security.AuthTokenJWKS, timeInterval); err != nil {
			logutil.BgLogger().Error("Fail to load JWKS from the path", zap.String("jwks", s.cfg.Security.AuthTokenJWKS))
		}
	}

	// Init rand seed for randomBuf()
	rand.Seed(time.Now().UTC().UnixNano())

	variable.RegisterStatistics(s)

	return s, nil
}

func cleanupStaleSocket(socket string) error {
	sockStat, err := os.Stat(socket)
	if err != nil {
		return nil
	}

	if sockStat.Mode().Type() != os.ModeSocket {
		return fmt.Errorf(
			"the specified socket file %s is a %s instead of a socket file",
			socket, sockStat.Mode().String())
	}

	if _, err = net.Dial("unix", socket); err == nil {
		return fmt.Errorf("unix socket %s exists and is functional, not removing it", socket)
	}

	if err2 := os.Remove(socket); err2 != nil {
		return fmt.Errorf("failed to cleanup stale Unix socket file %s: %w", socket, err)
	}

	return nil
}

func setSSLVariable(ca, key, cert string) {
	variable.SetSysVar("have_openssl", "YES")
	variable.SetSysVar("have_ssl", "YES")
	variable.SetSysVar("ssl_cert", cert)
	variable.SetSysVar("ssl_key", key)
	variable.SetSysVar("ssl_ca", ca)
}

func setTxnScope() {
	variable.SetSysVar(variable.TiDBTxnScope, func() string {
		if !variable.EnableLocalTxn.Load() {
			return kv.GlobalTxnScope
		}
		if txnScope := config.GetTxnScopeFromConfig(); txnScope == kv.GlobalTxnScope {
			return kv.GlobalTxnScope
		}
		return kv.LocalTxnScope
	}())
}

// Export config-related metrics
func (s *Server) reportConfig() {
	metrics.ConfigStatus.WithLabelValues("token-limit").Set(float64(s.cfg.TokenLimit))
	metrics.ConfigStatus.WithLabelValues("max_connections").Set(float64(s.cfg.Instance.MaxConnections))
}

// Run runs the server.
func (s *Server) Run() error {
	metrics.ServerEventCounter.WithLabelValues(metrics.EventStart).Inc()
	s.reportConfig()

	// Start HTTP API to report tidb info such as TPS.
	if s.cfg.Status.ReportStatus {
		s.startStatusHTTP()
	}
	// If error should be reported and exit the server it can be sent on this
	// channel. Otherwise, end with sending a nil error to signal "done"
	errChan := make(chan error, 2)
	go s.startNetworkListener(s.listener, false, errChan)
	go s.startNetworkListener(s.socket, true, errChan)
	err := <-errChan
	if err != nil {
		return err
	}
	return <-errChan
}

func (s *Server) startNetworkListener(listener net.Listener, isUnixSocket bool, errChan chan error) {
	if listener == nil {
		errChan <- nil
		return
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok {
				if opErr.Err.Error() == "use of closed network connection" {
					if s.inShutdownMode.Load() {
						errChan <- nil
					} else {
						errChan <- err
					}
					return
				}
			}

			// If we got PROXY protocol error, we should continue to accept.
			if proxyprotocol.IsProxyProtocolError(err) {
				logutil.BgLogger().Error("PROXY protocol failed", zap.Error(err))
				continue
			}

			logutil.BgLogger().Error("accept failed", zap.Error(err))
			errChan <- err
			return
		}

		logutil.BgLogger().Debug("accept new connection success")

		clientConn := s.newConn(conn)
		if isUnixSocket {
			var (
				uc *net.UnixConn
				ok bool
			)
			if clientConn.ppEnabled {
				// Using reflect to get Raw Conn object from proxy protocol wrapper connection object
				ppv := reflect.ValueOf(conn)
				vconn := ppv.Elem().FieldByName("Conn")
				rconn := vconn.Interface()
				uc, ok = rconn.(*net.UnixConn)
			} else {
				uc, ok = conn.(*net.UnixConn)
			}
			if !ok {
				logutil.BgLogger().Error("Expected UNIX socket, but got something else")
				return
			}

			clientConn.isUnixSocket = true
			clientConn.peerHost = "localhost"
			clientConn.socketCredUID, err = linux.GetSockUID(*uc)
			if err != nil {
				logutil.BgLogger().Error("Failed to get UNIX socket peer credentials", zap.Error(err))
				return
			}
		}

		err = nil
		if !clientConn.ppEnabled {
			// Check audit plugins when ProxyProtocol not enabled
			err = s.checkAuditPlugin(clientConn)
		}
		if err != nil {
			continue
		}

		if s.dom != nil && s.dom.IsLostConnectionToPD() {
			logutil.BgLogger().Warn("reject connection due to lost connection to PD")
			terror.Log(clientConn.Close())
			continue
		}

		go s.onConn(clientConn)
	}
}

func (s *Server) checkAuditPlugin(clientConn *clientConn) error {
	return plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
		authPlugin := plugin.DeclareAuditManifest(p.Manifest)
		if authPlugin.OnConnectionEvent == nil {
			return nil
		}
		host, _, err := clientConn.PeerHost("", false)
		if err != nil {
			logutil.BgLogger().Error("get peer host failed", zap.Error(err))
			terror.Log(clientConn.Close())
			return errors.Trace(err)
		}
		if err = authPlugin.OnConnectionEvent(context.Background(), plugin.PreAuth,
			&variable.ConnectionInfo{Host: host}); err != nil {
			logutil.BgLogger().Info("do connection event failed", zap.Error(err))
			terror.Log(clientConn.Close())
			return errors.Trace(err)
		}
		return nil
	})
}

func (s *Server) startShutdown() {
	logutil.BgLogger().Info("setting tidb-server to report unhealthy (shutting-down)")
	s.health.Store(false)
	// give the load balancer a chance to receive a few unhealthy health reports
	// before acquiring the s.rwlock and blocking connections.
	waitTime := time.Duration(s.cfg.GracefulWaitBeforeShutdown) * time.Second
	if waitTime > 0 {
		logutil.BgLogger().Info("waiting for stray connections before starting shutdown process", zap.Duration("waitTime", waitTime))
		time.Sleep(waitTime)
	}
}

func (s *Server) closeListener() {
	if s.listener != nil {
		err := s.listener.Close()
		terror.Log(errors.Trace(err))
		s.listener = nil
	}
	if s.socket != nil {
		err := s.socket.Close()
		terror.Log(errors.Trace(err))
		s.socket = nil
	}
	if s.statusServer != nil {
		err := s.statusServer.Close()
		terror.Log(errors.Trace(err))
		s.statusServer = nil
	}
	if s.grpcServer != nil {
		s.grpcServer.Stop()
		s.grpcServer = nil
	}
	if s.autoIDService != nil {
		s.autoIDService.Close()
	}
	if s.authTokenCancelFunc != nil {
		s.authTokenCancelFunc()
	}
	s.wg.Wait()
	metrics.ServerEventCounter.WithLabelValues(metrics.EventClose).Inc()
}

var gracefulCloseConnectionsTimeout = 15 * time.Second

// Close closes the server.
func (s *Server) Close() {
	s.startShutdown()
	s.rwlock.Lock() // // prevent new connections
	defer s.rwlock.Unlock()
	s.inShutdownMode.Store(true)
	s.closeListener()
}

func (s *Server) registerConn(conn *clientConn) bool {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	connections := len(s.clients)

	logger := logutil.BgLogger()
	if s.inShutdownMode.Load() {
		logger.Info("close connection directly when shutting down")
		terror.Log(closeConn(conn, connections))
		return false
	}
	s.clients[conn.connectionID] = conn
	connections = len(s.clients)
	metrics.ConnGauge.Set(float64(connections))
	return true
}

// onConn runs in its own goroutine, handles queries from this connection.
func (s *Server) onConn(conn *clientConn) {
	// init the connInfo
	_, _, err := conn.PeerHost("", false)
	if err != nil {
		logutil.BgLogger().With(zap.Uint64("conn", conn.connectionID)).
			Error("get peer host failed", zap.Error(err))
		terror.Log(conn.Close())
		return
	}

	extensions, err := extension.GetExtensions()
	if err != nil {
		logutil.BgLogger().With(zap.Uint64("conn", conn.connectionID)).
			Error("error in get extensions", zap.Error(err))
		terror.Log(conn.Close())
		return
	}

	if sessExtensions := extensions.NewSessionExtensions(); sessExtensions != nil {
		conn.extensions = sessExtensions
		conn.onExtensionConnEvent(extension.ConnConnected, nil)
		defer func() {
			conn.onExtensionConnEvent(extension.ConnDisconnected, nil)
		}()
	}

	ctx := logutil.WithConnID(context.Background(), conn.connectionID)

	if err := conn.handshake(ctx); err != nil {
		conn.onExtensionConnEvent(extension.ConnHandshakeRejected, err)
		if plugin.IsEnable(plugin.Audit) && conn.getCtx() != nil {
			conn.getCtx().GetSessionVars().ConnectionInfo = conn.connectInfo()
			err = plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
				authPlugin := plugin.DeclareAuditManifest(p.Manifest)
				if authPlugin.OnConnectionEvent != nil {
					pluginCtx := context.WithValue(context.Background(), plugin.RejectReasonCtxValue{}, err.Error())
					return authPlugin.OnConnectionEvent(pluginCtx, plugin.Reject, conn.ctx.GetSessionVars().ConnectionInfo)
				}
				return nil
			})
			terror.Log(err)
		}
		switch errors.Cause(err) {
		case io.EOF:
			// `EOF` means the connection is closed normally, we do not treat it as a noticeable error and log it in 'DEBUG' level.
			logutil.BgLogger().With(zap.Uint64("conn", conn.connectionID)).
				Debug("EOF", zap.String("remote addr", conn.bufReadConn.RemoteAddr().String()))
		case errConCount:
			if err := conn.writeError(ctx, err); err != nil {
				logutil.BgLogger().With(zap.Uint64("conn", conn.connectionID)).
					Warn("error in writing errConCount", zap.Error(err),
						zap.String("remote addr", conn.bufReadConn.RemoteAddr().String()))
			}
		default:
			metrics.HandShakeErrorCounter.Inc()
			logutil.BgLogger().With(zap.Uint64("conn", conn.connectionID)).
				Warn("Server.onConn handshake", zap.Error(err),
					zap.String("remote addr", conn.bufReadConn.RemoteAddr().String()))
		}
		terror.Log(conn.Close())
		return
	}

	logutil.Logger(ctx).Debug("new connection", zap.String("remoteAddr", conn.bufReadConn.RemoteAddr().String()))

	defer func() {
		terror.Log(conn.Close())
		logutil.Logger(ctx).Debug("connection closed")
	}()

	if !s.registerConn(conn) {
		return
	}

	sessionVars := conn.ctx.GetSessionVars()
	sessionVars.ConnectionInfo = conn.connectInfo()
	conn.onExtensionConnEvent(extension.ConnHandshakeAccepted, nil)
	err = plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
		authPlugin := plugin.DeclareAuditManifest(p.Manifest)
		if authPlugin.OnConnectionEvent != nil {
			return authPlugin.OnConnectionEvent(context.Background(), plugin.Connected, sessionVars.ConnectionInfo)
		}
		return nil
	})
	if err != nil {
		return
	}

	connectedTime := time.Now()
	conn.Run(ctx)

	err = plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
		authPlugin := plugin.DeclareAuditManifest(p.Manifest)
		if authPlugin.OnConnectionEvent != nil {
			sessionVars.ConnectionInfo.Duration = float64(time.Since(connectedTime)) / float64(time.Millisecond)
			err := authPlugin.OnConnectionEvent(context.Background(), plugin.Disconnect, sessionVars.ConnectionInfo)
			if err != nil {
				logutil.BgLogger().Warn("do connection event failed", zap.String("plugin", authPlugin.Name), zap.Error(err))
			}
		}
		return nil
	})
	if err != nil {
		return
	}
}

func (cc *clientConn) connectInfo() *variable.ConnectionInfo {
	connType := variable.ConnTypeSocket
	if cc.isUnixSocket {
		connType = variable.ConnTypeUnixSocket
	} else if cc.tlsConn != nil {
		connType = variable.ConnTypeTLS
	}
	connInfo := &variable.ConnectionInfo{
		ConnectionID:      cc.connectionID,
		ConnectionType:    connType,
		Host:              cc.peerHost,
		ClientIP:          cc.peerHost,
		ClientPort:        cc.peerPort,
		ServerID:          1,
		ServerIP:          cc.serverHost,
		ServerPort:        int(cc.server.cfg.Port),
		User:              cc.user,
		ServerOSLoginUser: osUser,
		OSVersion:         osVersion,
		ServerVersion:     mysql.TiDBReleaseVersion,
		SSLVersion:        "v1.2.0", // for current go version
		PID:               serverPID,
		DB:                cc.dbname,
	}
	return connInfo
}

func (s *Server) checkConnectionCount() error {
	// When the value of Instance.MaxConnections is 0, the number of connections is unlimited.
	if int(s.cfg.Instance.MaxConnections) == 0 {
		return nil
	}

	s.rwlock.RLock()
	conns := len(s.clients)
	s.rwlock.RUnlock()

	if conns >= int(s.cfg.Instance.MaxConnections) {
		logutil.BgLogger().Error("too many connections",
			zap.Uint32("max connections", s.cfg.Instance.MaxConnections), zap.Error(errConCount))
		return errConCount
	}
	return nil
}

// ShowProcessList implements the SessionManager interface.
func (s *Server) ShowProcessList() map[uint64]*util.ProcessInfo {
	rs := make(map[uint64]*util.ProcessInfo)
	for connID, pi := range s.getUserProcessList() {
		rs[connID] = pi
	}
	if s.dom != nil {
		for connID, pi := range s.dom.SysProcTracker().GetSysProcessList() {
			rs[connID] = pi
		}
	}
	return rs
}

func (s *Server) getUserProcessList() map[uint64]*util.ProcessInfo {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	rs := make(map[uint64]*util.ProcessInfo)
	for _, client := range s.clients {
		if pi := client.ctx.ShowProcess(); pi != nil {
			rs[pi.ID] = pi
		}
	}
	return rs
}

// ShowTxnList shows all txn info for displaying in `TIDB_TRX`
func (s *Server) ShowTxnList() []*txninfo.TxnInfo {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	rs := make([]*txninfo.TxnInfo, 0, len(s.clients))
	for _, client := range s.clients {
		if client.ctx.Session != nil {
			info := client.ctx.Session.TxnInfo()
			if info != nil {
				rs = append(rs, info)
			}
		}
	}
	return rs
}

// GetProcessInfo implements the SessionManager interface.
func (s *Server) GetProcessInfo(id uint64) (*util.ProcessInfo, bool) {
	s.rwlock.RLock()
	conn, ok := s.clients[id]
	s.rwlock.RUnlock()
	if !ok {
		if s.dom != nil {
			if pinfo, ok2 := s.dom.SysProcTracker().GetSysProcessList()[id]; ok2 {
				return pinfo, true
			}
		}
		return &util.ProcessInfo{}, false
	}
	return conn.ctx.ShowProcess(), ok
}

// Kill implements the SessionManager interface.
func (s *Server) Kill(connectionID uint64, query bool) {
	logutil.BgLogger().Info("kill", zap.Uint64("conn", connectionID), zap.Bool("query", query))
	metrics.ServerEventCounter.WithLabelValues(metrics.EventKill).Inc()

	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	conn, ok := s.clients[connectionID]
	if !ok && s.dom != nil {
		s.dom.SysProcTracker().KillSysProcess(connectionID)
		return
	}

	if !query {
		// Mark the client connection status as WaitShutdown, when clientConn.Run detect
		// this, it will end the dispatch loop and exit.
		atomic.StoreInt32(&conn.status, connStatusWaitShutdown)
	}
	killQuery(conn)
}

// UpdateTLSConfig implements the SessionManager interface.
func (s *Server) UpdateTLSConfig(cfg *tls.Config) {
	atomic.StorePointer(&s.tlsConfig, unsafe.Pointer(cfg))
}

func (s *Server) getTLSConfig() *tls.Config {
	return (*tls.Config)(atomic.LoadPointer(&s.tlsConfig))
}

func killQuery(conn *clientConn) {
	sessVars := conn.ctx.GetSessionVars()
	atomic.StoreUint32(&sessVars.Killed, 1)
	conn.mu.RLock()
	cancelFunc := conn.mu.cancelFunc
	conn.mu.RUnlock()

	if cancelFunc != nil {
		cancelFunc()
	}
	if conn.bufReadConn != nil {
		if err := conn.bufReadConn.SetReadDeadline(time.Now()); err != nil {
			logutil.BgLogger().Warn("error setting read deadline for kill.", zap.Error(err))
		}
	}
}

// KillSysProcesses kill sys processes such as auto analyze.
func (s *Server) KillSysProcesses() {
	if s.dom == nil {
		return
	}
	sysProcTracker := s.dom.SysProcTracker()
	for connID := range sysProcTracker.GetSysProcessList() {
		sysProcTracker.KillSysProcess(connID)
	}
}

// KillAllConnections implements the SessionManager interface.
// KillAllConnections kills all connections.
func (s *Server) KillAllConnections() {
	logutil.BgLogger().Info("[server] kill all connections.")

	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	for _, conn := range s.clients {
		atomic.StoreInt32(&conn.status, connStatusShutdown)
		if err := conn.closeWithoutLock(); err != nil {
			terror.Log(err)
		}
		killQuery(conn)
	}

	s.KillSysProcesses()
}

// DrainClients drain all connections in drainWait.
// After drainWait duration, we kill all connections still not quit explicitly and wait for cancelWait.
func (s *Server) DrainClients(drainWait time.Duration, cancelWait time.Duration) {
	logger := logutil.BgLogger()
	logger.Info("start drain clients")

	conns := make(map[uint64]*clientConn)

	s.rwlock.Lock()
	for k, v := range s.clients {
		conns[k] = v
	}
	s.rwlock.Unlock()

	allDone := make(chan struct{})
	quitWaitingForConns := make(chan struct{})
	defer close(quitWaitingForConns)
	go func() {
		defer close(allDone)
		for _, conn := range conns {
			select {
			case <-conn.quit:
			case <-quitWaitingForConns:
				return
			}
		}
	}()

	select {
	case <-allDone:
		logger.Info("all sessions quit in drain wait time")
	case <-time.After(drainWait):
		logger.Info("timeout waiting all sessions quit")
	}

	s.KillAllConnections()

	select {
	case <-allDone:
	case <-time.After(cancelWait):
		logger.Warn("some sessions do not quit in cancel wait time")
	}
}

// ServerID implements SessionManager interface.
func (s *Server) ServerID() uint64 {
	return s.dom.ServerID()
}

// StoreInternalSession implements SessionManager interface.
// @param addr	The address of a session.session struct variable
func (s *Server) StoreInternalSession(se interface{}) {
	s.sessionMapMutex.Lock()
	s.internalSessions[se] = struct{}{}
	s.sessionMapMutex.Unlock()
}

// DeleteInternalSession implements SessionManager interface.
// @param addr	The address of a session.session struct variable
func (s *Server) DeleteInternalSession(se interface{}) {
	s.sessionMapMutex.Lock()
	delete(s.internalSessions, se)
	s.sessionMapMutex.Unlock()
}

// GetInternalSessionStartTSList implements SessionManager interface.
func (s *Server) GetInternalSessionStartTSList() []uint64 {
	s.sessionMapMutex.Lock()
	defer s.sessionMapMutex.Unlock()
	tsList := make([]uint64, 0, len(s.internalSessions))
	analyzeProcID := util.GetAutoAnalyzeProcID(s.ServerID)
	for se := range s.internalSessions {
		if ts, processInfoID := session.GetStartTSFromSession(se); ts != 0 {
			if processInfoID == analyzeProcID {
				continue
			}
			tsList = append(tsList, ts)
		}
	}
	return tsList
}

// InternalSessionExists is used for test
func (s *Server) InternalSessionExists(se interface{}) bool {
	s.sessionMapMutex.Lock()
	_, ok := s.internalSessions[se]
	s.sessionMapMutex.Unlock()
	return ok
}

// setSysTimeZoneOnce is used for parallel run tests. When several servers are running,
// only the first will actually do setSystemTimeZoneVariable, thus we can avoid data race.
var setSysTimeZoneOnce = &sync.Once{}

func setSystemTimeZoneVariable() {
	setSysTimeZoneOnce.Do(func() {
		tz, err := timeutil.GetSystemTZ()
		if err != nil {
			logutil.BgLogger().Error(
				"Error getting SystemTZ, use default value instead",
				zap.Error(err),
				zap.String("default system_time_zone", variable.GetSysVar("system_time_zone").Value))
			return
		}
		variable.SetSysVar("system_time_zone", tz)
	})
}

// CheckOldRunningTxn implements SessionManager interface.
func (s *Server) CheckOldRunningTxn(job2ver map[int64]int64, job2ids map[int64]string) {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()

	printLog := false
	if time.Since(s.printMDLLogTime) > 10*time.Second {
		printLog = true
		s.printMDLLogTime = time.Now()
	}
	for _, client := range s.clients {
		if client.ctx.Session != nil {
			session.RemoveLockDDLJobs(client.ctx.Session, job2ver, job2ids, printLog)
		}
	}
}

// KillNonFlashbackClusterConn implements SessionManager interface.
func (s *Server) KillNonFlashbackClusterConn() {
	s.rwlock.RLock()
	connIDs := make([]uint64, 0, len(s.clients))
	for _, client := range s.clients {
		if client.ctx.Session != nil {
			processInfo := client.ctx.Session.ShowProcess()
			ddl, ok := processInfo.StmtCtx.GetPlan().(*core.DDL)
			if !ok {
				connIDs = append(connIDs, client.connectionID)
				continue
			}
			_, ok = ddl.Statement.(*ast.FlashBackToTimestampStmt)
			if !ok {
				connIDs = append(connIDs, client.connectionID)
				continue
			}
		}
	}
	s.rwlock.RUnlock()
	for _, id := range connIDs {
		s.Kill(id, false)
	}
}
