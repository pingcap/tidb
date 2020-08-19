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
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"unsafe"

	// For pprof
	_ "net/http/pprof"
	"os"
	"os/user"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blacktear23/go-proxyprotocol"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/fastrand"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sys/linux"
	"github.com/pingcap/tidb/util/timeutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	baseConnID  uint32
	serverPID   int
	osUser      string
	osVersion   string
	runInGoTest bool
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
	runInGoTest = flag.Lookup("test.v") != nil || flag.Lookup("check.v") != nil
}

var (
	errUnknownFieldType        = terror.ClassServer.New(errno.ErrUnknownFieldType, errno.MySQLErrName[errno.ErrUnknownFieldType])
	errInvalidSequence         = terror.ClassServer.New(errno.ErrInvalidSequence, errno.MySQLErrName[errno.ErrInvalidSequence])
	errInvalidType             = terror.ClassServer.New(errno.ErrInvalidType, errno.MySQLErrName[errno.ErrInvalidType])
	errNotAllowedCommand       = terror.ClassServer.New(errno.ErrNotAllowedCommand, errno.MySQLErrName[errno.ErrNotAllowedCommand])
	errAccessDenied            = terror.ClassServer.New(errno.ErrAccessDenied, errno.MySQLErrName[errno.ErrAccessDenied])
	errConCount                = terror.ClassServer.New(errno.ErrConCount, errno.MySQLErrName[errno.ErrConCount])
	errSecureTransportRequired = terror.ClassServer.New(errno.ErrSecureTransportRequired, errno.MySQLErrName[errno.ErrSecureTransportRequired])
)

// DefaultCapability is the capability of the server when it is created using the default configuration.
// When server is configured with SSL, the server will have extra capabilities compared to DefaultCapability.
const defaultCapability = mysql.ClientLongPassword | mysql.ClientLongFlag |
	mysql.ClientConnectWithDB | mysql.ClientProtocol41 |
	mysql.ClientTransactions | mysql.ClientSecureConnection | mysql.ClientFoundRows |
	mysql.ClientMultiStatements | mysql.ClientMultiResults | mysql.ClientLocalFiles |
	mysql.ClientConnectAtts | mysql.ClientPluginAuth | mysql.ClientInteractive

// Server is the MySQL protocol server
type Server struct {
	cfg               *config.Config
	tlsConfig         unsafe.Pointer // *tls.Config
	driver            IDriver
	listener          net.Listener
	socket            net.Listener
	rwlock            sync.RWMutex
	concurrentLimiter *TokenLimiter
	clients           map[uint32]*clientConn
	capability        uint32
	dom               *domain.Domain

	statusAddr     string
	statusListener net.Listener
	statusServer   *http.Server
	grpcServer     *grpc.Server
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
	// Note that data smaller than one microsecond is ignored, because that case can be viewed as non-block.
	metrics.GetTokenDurationHistogram.Observe(float64(time.Since(start).Nanoseconds() / 1e3))
	return tok
}

func (s *Server) releaseToken(token *Token) {
	s.concurrentLimiter.Put(token)
}

// SetDomain use to set the server domain.
func (s *Server) SetDomain(dom *domain.Domain) {
	s.dom = dom
}

// newConn creates a new *clientConn from a net.Conn.
// It allocates a connection ID and random salt data for authentication.
func (s *Server) newConn(conn net.Conn) *clientConn {
	cc := newClientConn(s)
	if s.cfg.Performance.TCPKeepAlive {
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			if err := tcpConn.SetKeepAlive(true); err != nil {
				logutil.BgLogger().Error("failed to set tcp keep alive option", zap.Error(err))
			}
		}
	}
	cc.setConn(conn)
	cc.salt = fastrand.Buf(20)
	return cc
}

func (s *Server) isUnixSocket() bool {
	return s.cfg.Socket != ""
}

func (s *Server) forwardUnixSocketToTCP() {
	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	for {
		if s.listener == nil {
			return // server shutdown has started
		}
		if uconn, err := s.socket.Accept(); err == nil {
			logutil.BgLogger().Info("server socket forwarding", zap.String("from", s.cfg.Socket), zap.String("to", addr))
			go s.handleForwardedConnection(uconn, addr)
		} else {
			if s.listener != nil {
				logutil.BgLogger().Error("server failed to forward", zap.String("from", s.cfg.Socket), zap.String("to", addr), zap.Error(err))
			}
		}
	}
}

func (s *Server) handleForwardedConnection(uconn net.Conn, addr string) {
	defer terror.Call(uconn.Close)
	if tconn, err := net.Dial("tcp", addr); err == nil {
		go func() {
			if _, err := io.Copy(uconn, tconn); err != nil {
				logutil.BgLogger().Warn("copy server to socket failed", zap.Error(err))
			}
		}()
		if _, err := io.Copy(tconn, uconn); err != nil {
			logutil.BgLogger().Warn("socket forward copy failed", zap.Error(err))
		}
	} else {
		logutil.BgLogger().Warn("socket forward failed: could not connect", zap.String("addr", addr), zap.Error(err))
	}
}

// NewServer creates a new Server.
func NewServer(cfg *config.Config, driver IDriver) (*Server, error) {
	s := &Server{
		cfg:               cfg,
		driver:            driver,
		concurrentLimiter: NewTokenLimiter(cfg.TokenLimit),
		clients:           make(map[uint32]*clientConn),
	}

	tlsConfig, err := util.LoadTLSCertificates(s.cfg.Security.SSLCA, s.cfg.Security.SSLKey, s.cfg.Security.SSLCert)
	if err != nil {
		logutil.BgLogger().Error("secure connection cert/key/ca load fail", zap.Error(err))
	}
	if tlsConfig != nil {
		setSSLVariable(s.cfg.Security.SSLCA, s.cfg.Security.SSLKey, s.cfg.Security.SSLCert)
		atomic.StorePointer(&s.tlsConfig, unsafe.Pointer(tlsConfig))
		logutil.BgLogger().Info("mysql protocol server secure connection is enabled", zap.Bool("client verification enabled", len(variable.SysVars["ssl_ca"].Value) > 0))
	} else if cfg.Security.RequireSecureTransport {
		return nil, errSecureTransportRequired.FastGenByArgs()
	}

	setSystemTimeZoneVariable()

	s.capability = defaultCapability
	if s.tlsConfig != nil {
		s.capability |= mysql.ClientSSL
	}

	if s.cfg.Host != "" && (s.cfg.Port != 0 || runInGoTest) {
		addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
		if s.listener, err = net.Listen("tcp", addr); err == nil {
			logutil.BgLogger().Info("server is running MySQL protocol", zap.String("addr", addr))
			if cfg.Socket != "" {
				if s.socket, err = net.Listen("unix", s.cfg.Socket); err == nil {
					logutil.BgLogger().Info("server redirecting", zap.String("from", s.cfg.Socket), zap.String("to", addr))
					go s.forwardUnixSocketToTCP()
				}
			}
			if runInGoTest && s.cfg.Port == 0 {
				s.cfg.Port = uint(s.listener.Addr().(*net.TCPAddr).Port)
			}
		}
	} else if cfg.Socket != "" {
		if s.listener, err = net.Listen("unix", cfg.Socket); err == nil {
			logutil.BgLogger().Info("server is running MySQL protocol", zap.String("socket", cfg.Socket))
		}
	} else {
		err = errors.New("Server not configured to listen on either -socket or -host and -port")
	}

	if cfg.ProxyProtocol.Networks != "" {
		pplistener, errProxy := proxyprotocol.NewListener(s.listener, cfg.ProxyProtocol.Networks,
			int(cfg.ProxyProtocol.HeaderTimeout))
		if errProxy != nil {
			logutil.BgLogger().Error("ProxyProtocol networks parameter invalid")
			return nil, errors.Trace(errProxy)
		}
		logutil.BgLogger().Info("server is running MySQL protocol (through PROXY protocol)", zap.String("host", s.cfg.Host))
		s.listener = pplistener
	}

	if s.cfg.Status.ReportStatus && err == nil {
		err = s.listenStatusHTTPServer()
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Init rand seed for randomBuf()
	rand.Seed(time.Now().UTC().UnixNano())
	return s, nil
}

func setSSLVariable(ca, key, cert string) {
	variable.SysVars["have_openssl"].Value = "YES"
	variable.SysVars["have_ssl"].Value = "YES"
	variable.SysVars["ssl_cert"].Value = cert
	variable.SysVars["ssl_key"].Value = key
	variable.SysVars["ssl_ca"].Value = ca
}

// Run runs the server.
func (s *Server) Run() error {
	metrics.ServerEventCounter.WithLabelValues(metrics.EventStart).Inc()

	// Start HTTP API to report tidb info such as TPS.
	if s.cfg.Status.ReportStatus {
		s.startStatusHTTP()
	}
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok {
				if opErr.Err.Error() == "use of closed network connection" {
					return nil
				}
			}

			// If we got PROXY protocol error, we should continue accept.
			if proxyprotocol.IsProxyProtocolError(err) {
				logutil.BgLogger().Error("PROXY protocol failed", zap.Error(err))
				continue
			}

			logutil.BgLogger().Error("accept failed", zap.Error(err))
			return errors.Trace(err)
		}

		clientConn := s.newConn(conn)

		err = plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
			authPlugin := plugin.DeclareAuditManifest(p.Manifest)
			if authPlugin.OnConnectionEvent != nil {
				host, err := clientConn.PeerHost("")
				if err != nil {
					logutil.BgLogger().Error("get peer host failed", zap.Error(err))
					terror.Log(clientConn.Close())
					return errors.Trace(err)
				}
				err = authPlugin.OnConnectionEvent(context.Background(), plugin.PreAuth, &variable.ConnectionInfo{Host: host})
				if err != nil {
					logutil.BgLogger().Info("do connection event failed", zap.Error(err))
					terror.Log(clientConn.Close())
					return errors.Trace(err)
				}
			}
			return nil
		})
		if err != nil {
			continue
		}

		go s.onConn(clientConn)
	}
}

// Close closes the server.
func (s *Server) Close() {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()

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
	metrics.ServerEventCounter.WithLabelValues(metrics.EventClose).Inc()
}

// onConn runs in its own goroutine, handles queries from this connection.
func (s *Server) onConn(conn *clientConn) {
	ctx := logutil.WithConnID(context.Background(), conn.connectionID)
	if err := conn.handshake(ctx); err != nil {
		if plugin.IsEnable(plugin.Audit) && conn.ctx != nil {
			conn.ctx.GetSessionVars().ConnectionInfo = conn.connectInfo()
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
		// Some keep alive services will send request to TiDB and disconnect immediately.
		// So we only record metrics.
		metrics.HandShakeErrorCounter.Inc()
		err = conn.Close()
		terror.Log(errors.Trace(err))
		return
	}

	logutil.Logger(ctx).Info("new connection", zap.String("remoteAddr", conn.bufReadConn.RemoteAddr().String()))

	defer func() {
		logutil.Logger(ctx).Info("connection closed")
	}()
	s.rwlock.Lock()
	s.clients[conn.connectionID] = conn
	connections := len(s.clients)
	s.rwlock.Unlock()
	metrics.ConnGauge.Set(float64(connections))

	sessionVars := conn.ctx.GetSessionVars()
	if plugin.IsEnable(plugin.Audit) {
		sessionVars.ConnectionInfo = conn.connectInfo()
	}
	type vars struct {
		killed *uint32
	}
	err := plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
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
	connType := "Socket"
	if cc.server.isUnixSocket() {
		connType = "UnixSocket"
	} else if cc.tlsConn != nil {
		connType = "SSL/TLS"
	}
	connInfo := &variable.ConnectionInfo{
		ConnectionID:      cc.connectionID,
		ConnectionType:    connType,
		Host:              cc.peerHost,
		ClientIP:          cc.peerHost,
		ClientPort:        cc.peerPort,
		ServerID:          1,
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
	// When the value of MaxServerConnections is 0, the number of connections is unlimited.
	if int(s.cfg.MaxServerConnections) == 0 {
		return nil
	}

	s.rwlock.RLock()
	conns := len(s.clients)
	s.rwlock.RUnlock()

	if conns >= int(s.cfg.MaxServerConnections) {
		logutil.BgLogger().Error("too many connections",
			zap.Uint32("max connections", s.cfg.MaxServerConnections), zap.Error(errConCount))
		return errConCount
	}
	return nil
}

// ShowProcessList implements the SessionManager interface.
func (s *Server) ShowProcessList() map[uint64]*util.ProcessInfo {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	rs := make(map[uint64]*util.ProcessInfo, len(s.clients))
	for _, client := range s.clients {
		if atomic.LoadInt32(&client.status) == connStatusWaitShutdown {
			continue
		}
		if pi := client.ctx.ShowProcess(); pi != nil {
			rs[pi.ID] = pi
		}
	}
	return rs
}

// GetProcessInfo implements the SessionManager interface.
func (s *Server) GetProcessInfo(id uint64) (*util.ProcessInfo, bool) {
	s.rwlock.RLock()
	conn, ok := s.clients[uint32(id)]
	s.rwlock.RUnlock()
	if !ok || atomic.LoadInt32(&conn.status) == connStatusWaitShutdown {
		return &util.ProcessInfo{}, false
	}
	return conn.ctx.ShowProcess(), ok
}

// Kill implements the SessionManager interface.
func (s *Server) Kill(connectionID uint64, query bool) {
	logutil.BgLogger().Info("kill", zap.Uint64("connID", connectionID), zap.Bool("query", query))
	metrics.ServerEventCounter.WithLabelValues(metrics.EventKill).Inc()

	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	conn, ok := s.clients[uint32(connectionID)]
	if !ok {
		return
	}

	if !query {
		// Mark the client connection status as WaitShutdown, when the goroutine detect
		// this, it will end the dispatch loop and exit.
		atomic.StoreInt32(&conn.status, connStatusWaitShutdown)
	}
	killConn(conn)
}

// UpdateTLSConfig implements the SessionManager interface.
func (s *Server) UpdateTLSConfig(cfg *tls.Config) {
	atomic.StorePointer(&s.tlsConfig, unsafe.Pointer(cfg))
}

func (s *Server) getTLSConfig() *tls.Config {
	return (*tls.Config)(atomic.LoadPointer(&s.tlsConfig))
}

func killConn(conn *clientConn) {
	sessVars := conn.ctx.GetSessionVars()
	atomic.StoreUint32(&sessVars.Killed, 1)
}

// KillAllConnections kills all connections when server is not gracefully shutdown.
func (s *Server) KillAllConnections() {
	logutil.BgLogger().Info("[server] kill all connections.")

	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	for _, conn := range s.clients {
		atomic.StoreInt32(&conn.status, connStatusShutdown)
		if err := conn.closeWithoutLock(); err != nil {
			terror.Log(err)
		}
		killConn(conn)
	}
}

var gracefulCloseConnectionsTimeout = 15 * time.Second

// TryGracefulDown will try to gracefully close all connection first with timeout. if timeout, will close all connection directly.
func (s *Server) TryGracefulDown() {
	ctx, cancel := context.WithTimeout(context.Background(), gracefulCloseConnectionsTimeout)
	defer cancel()
	done := make(chan struct{})
	go func() {
		s.GracefulDown(ctx, done)
	}()
	select {
	case <-ctx.Done():
		s.KillAllConnections()
	case <-done:
		return
	}
}

// GracefulDown waits all clients to close.
func (s *Server) GracefulDown(ctx context.Context, done chan struct{}) {
	logutil.Logger(ctx).Info("[server] graceful shutdown.")
	metrics.ServerEventCounter.WithLabelValues(metrics.EventGracefulDown).Inc()

	count := s.ConnectionCount()
	for i := 0; count > 0; i++ {
		s.kickIdleConnection()

		count = s.ConnectionCount()
		if count == 0 {
			break
		}
		// Print information for every 30s.
		if i%30 == 0 {
			logutil.Logger(ctx).Info("graceful shutdown...", zap.Int("conn count", count))
		}
		ticker := time.After(time.Second)
		select {
		case <-ctx.Done():
			return
		case <-ticker:
		}
	}
	close(done)
}

func (s *Server) kickIdleConnection() {
	var conns []*clientConn
	s.rwlock.RLock()
	for _, cc := range s.clients {
		if cc.ShutdownOrNotify() {
			// Shutdowned conn will be closed by us, and notified conn will exist themselves.
			conns = append(conns, cc)
		}
	}
	s.rwlock.RUnlock()

	for _, cc := range conns {
		err := cc.Close()
		if err != nil {
			logutil.BgLogger().Error("close connection", zap.Error(err))
		}
	}
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
				zap.String("default system_time_zone", variable.SysVars["system_time_zone"].Value))
			return
		}
		variable.SysVars["system_time_zone"].Value = tz
	})
}

// Server error codes.
const (
	codeUnknownFieldType = 1
	codeInvalidSequence  = 3
	codeInvalidType      = 4
)
