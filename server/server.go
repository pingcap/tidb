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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
	// For pprof
	_ "net/http/pprof"

	"github.com/blacktear23/go-proxyprotocol"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/arena"
	log "github.com/sirupsen/logrus"
)

var (
	baseConnID uint32
)

var (
	errUnknownFieldType  = terror.ClassServer.New(codeUnknownFieldType, "unknown field type")
	errInvalidPayloadLen = terror.ClassServer.New(codeInvalidPayloadLen, "invalid payload length")
	errInvalidSequence   = terror.ClassServer.New(codeInvalidSequence, "invalid sequence")
	errInvalidType       = terror.ClassServer.New(codeInvalidType, "invalid type")
	errNotAllowedCommand = terror.ClassServer.New(codeNotAllowedCommand, "the used command is not allowed with this TiDB version")
	errAccessDenied      = terror.ClassServer.New(codeAccessDenied, mysql.MySQLErrName[mysql.ErrAccessDenied])
)

// DefaultCapability is the capability of the server when it is created using the default configuration.
// When server is configured with SSL, the server will have extra capabilities compared to DefaultCapability.
const defaultCapability = mysql.ClientLongPassword | mysql.ClientLongFlag |
	mysql.ClientConnectWithDB | mysql.ClientProtocol41 |
	mysql.ClientTransactions | mysql.ClientSecureConnection | mysql.ClientFoundRows |
	mysql.ClientMultiStatements | mysql.ClientMultiResults | mysql.ClientLocalFiles |
	mysql.ClientConnectAtts | mysql.ClientPluginAuth

// Server is the MySQL protocol server
type Server struct {
	cfg               *config.Config
	tlsConfig         *tls.Config
	driver            IDriver
	listener          net.Listener
	rwlock            *sync.RWMutex
	concurrentLimiter *TokenLimiter
	clients           map[uint32]*clientConn
	capability        uint32

	// When a critical error occurred, we don't want to exit the process, because there may be
	// a supervisor automatically restart it, then new client connection will be created, but we can't server it.
	// So we just stop the listener and store to force clients to chose other TiDB servers.
	stopListenerCh chan struct{}
}

// ConnectionCount gets current connection count.
func (s *Server) ConnectionCount() int {
	var cnt int
	s.rwlock.RLock()
	cnt = len(s.clients)
	s.rwlock.RUnlock()
	return cnt
}

func (s *Server) getToken() *Token {
	return s.concurrentLimiter.Get()
}

func (s *Server) releaseToken(token *Token) {
	s.concurrentLimiter.Put(token)
}

// newConn creates a new *clientConn from a net.Conn.
// It allocates a connection ID and random salt data for authentication.
func (s *Server) newConn(conn net.Conn) *clientConn {
	cc := &clientConn{
		server:       s,
		connectionID: atomic.AddUint32(&baseConnID, 1),
		collation:    mysql.DefaultCollationID,
		alloc:        arena.NewAllocator(32 * 1024),
		status:       connStatusDispatching,
	}
	log.Infof("[%d] new connection %s", cc.connectionID, conn.RemoteAddr().String())
	if s.cfg.Performance.TCPKeepAlive {
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			if err := tcpConn.SetKeepAlive(true); err != nil {
				log.Error("failed to set tcp keep alive option:", err)
			}
		}
	}
	cc.setConn(conn)
	cc.salt = util.RandomBuf(20)
	return cc
}

func (s *Server) skipAuth() bool {
	return s.cfg.Socket != ""
}

// NewServer creates a new Server.
func NewServer(cfg *config.Config, driver IDriver) (*Server, error) {
	s := &Server{
		cfg:               cfg,
		driver:            driver,
		concurrentLimiter: NewTokenLimiter(cfg.TokenLimit),
		rwlock:            &sync.RWMutex{},
		clients:           make(map[uint32]*clientConn),
		stopListenerCh:    make(chan struct{}, 1),
	}
	s.loadTLSCertificates()

	s.capability = defaultCapability
	if s.tlsConfig != nil {
		s.capability |= mysql.ClientSSL
	}

	var err error
	if cfg.Socket != "" {
		if s.listener, err = net.Listen("unix", cfg.Socket); err == nil {
			log.Infof("Server is running MySQL Protocol through Socket [%s]", cfg.Socket)
		}
	} else {
		addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
		if s.listener, err = net.Listen("tcp", addr); err == nil {
			log.Infof("Server is running MySQL Protocol at [%s]", addr)
		}
	}

	if cfg.ProxyProtocol.Networks != "" {
		pplistener, errProxy := proxyprotocol.NewListener(s.listener, cfg.ProxyProtocol.Networks, cfg.ProxyProtocol.HeaderTimeout)
		if errProxy != nil {
			log.Error("ProxyProtocol Networks parameter invalid")
			return nil, errors.Trace(errProxy)
		}
		log.Infof("Server is running MySQL Protocol (through PROXY Protocol) at [%s]", s.cfg.Host)
		s.listener = pplistener
	}

	if err != nil {
		return nil, errors.Trace(err)
	}

	// Init rand seed for randomBuf()
	rand.Seed(time.Now().UTC().UnixNano())
	return s, nil
}

func (s *Server) loadTLSCertificates() {
	defer func() {
		if s.tlsConfig != nil {
			log.Infof("Secure connection is enabled (client verification enabled = %v)", len(variable.SysVars["ssl_ca"].Value) > 0)
			variable.SysVars["have_openssl"].Value = "YES"
			variable.SysVars["have_ssl"].Value = "YES"
			variable.SysVars["ssl_cert"].Value = s.cfg.Security.SSLCert
			variable.SysVars["ssl_key"].Value = s.cfg.Security.SSLKey
		} else {
			log.Warn("Secure connection is NOT ENABLED")
		}
	}()

	if len(s.cfg.Security.SSLCert) == 0 || len(s.cfg.Security.SSLKey) == 0 {
		s.tlsConfig = nil
		return
	}

	tlsCert, err := tls.LoadX509KeyPair(s.cfg.Security.SSLCert, s.cfg.Security.SSLKey)
	if err != nil {
		log.Warn(errors.ErrorStack(err))
		s.tlsConfig = nil
		return
	}

	// Try loading CA cert.
	clientAuthPolicy := tls.NoClientCert
	var certPool *x509.CertPool
	if len(s.cfg.Security.SSLCA) > 0 {
		caCert, err := ioutil.ReadFile(s.cfg.Security.SSLCA)
		if err != nil {
			log.Warn(errors.ErrorStack(err))
		} else {
			certPool = x509.NewCertPool()
			if certPool.AppendCertsFromPEM(caCert) {
				clientAuthPolicy = tls.VerifyClientCertIfGiven
			}
			variable.SysVars["ssl_ca"].Value = s.cfg.Security.SSLCA
		}
	}
	s.tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		ClientCAs:    certPool,
		ClientAuth:   clientAuthPolicy,
		MinVersion:   0,
	}
}

// Run runs the server.
func (s *Server) Run() error {
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
				log.Errorf("PROXY protocol error: %s", err.Error())
				continue
			}

			log.Errorf("accept error %s", err.Error())
			return errors.Trace(err)
		}
		if s.shouldStopListener() {
			err = conn.Close()
			terror.Log(errors.Trace(err))
			break
		}
		go s.onConn(conn)
	}
	err := s.listener.Close()
	terror.Log(errors.Trace(err))
	s.listener = nil
	for {
		log.Errorf("listener stopped, waiting for manual kill.")
		time.Sleep(time.Minute)
	}
}

func (s *Server) shouldStopListener() bool {
	select {
	case <-s.stopListenerCh:
		return true
	default:
		return false
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
}

// onConn runs in its own goroutine, handles queries from this connection.
func (s *Server) onConn(c net.Conn) {
	conn := s.newConn(c)
	defer func() {
		log.Infof("[%d] close connection", conn.connectionID)
	}()

	if err := conn.handshake(); err != nil {
		// Some keep alive services will send request to TiDB and disconnect immediately.
		// So we use info log level.
		log.Infof("handshake error %s", errors.ErrorStack(err))
		err = c.Close()
		terror.Log(errors.Trace(err))
		return
	}

	s.rwlock.Lock()
	s.clients[conn.connectionID] = conn
	connections := len(s.clients)
	s.rwlock.Unlock()
	connGauge.Set(float64(connections))

	conn.Run()
}

// ShowProcessList implements the SessionManager interface.
func (s *Server) ShowProcessList() []util.ProcessInfo {
	var rs []util.ProcessInfo
	s.rwlock.RLock()
	for _, client := range s.clients {
		if atomic.LoadInt32(&client.status) == connStatusWaitShutdown {
			continue
		}
		rs = append(rs, client.ctx.ShowProcess())
	}
	s.rwlock.RUnlock()
	return rs
}

// Kill implements the SessionManager interface.
func (s *Server) Kill(connectionID uint64, query bool) {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()

	conn, ok := s.clients[uint32(connectionID)]
	if !ok {
		return
	}

	conn.mu.RLock()
	cancelFunc := conn.mu.cancelFunc
	conn.mu.RUnlock()
	if cancelFunc != nil {
		cancelFunc()
	}

	if !query {
		// Mark the client connection status as WaitShutdown, when the goroutine detect
		// this, it will end the dispatch loop and exit.
		atomic.StoreInt32(&conn.status, connStatusWaitShutdown)
	}
}

// GracefulDown waits all clients to close.
func (s *Server) GracefulDown() {
	log.Info("graceful shutdown.")

	count := s.ConnectionCount()
	for i := 0; count > 0; i++ {
		time.Sleep(time.Second)
		s.kickIdleConnection()

		count = s.ConnectionCount()
		// Print information for every 30s.
		if i%30 == 0 {
			log.Infof("graceful shutdown...connection count %d\n", count)
		}
	}
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
			log.Error("close connection error:", err)
		}
	}
}

// Server error codes.
const (
	codeUnknownFieldType  = 1
	codeInvalidPayloadLen = 2
	codeInvalidSequence   = 3
	codeInvalidType       = 4

	codeNotAllowedCommand = 1148
	codeAccessDenied      = mysql.ErrAccessDenied
)

func init() {
	serverMySQLErrCodes := map[terror.ErrCode]uint16{
		codeNotAllowedCommand: mysql.ErrNotAllowedCommand,
		codeAccessDenied:      mysql.ErrAccessDenied,
	}
	terror.ErrClassToMySQLCodes[terror.ClassServer] = serverMySQLErrCodes
}
