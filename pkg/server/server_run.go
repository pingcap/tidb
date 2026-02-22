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
	"io"
	"net"
	"reflect"
	"time"

	"github.com/blacktear23/go-proxyprotocol"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/mppcoordmanager"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/plugin"
	servererr "github.com/pingcap/tidb/pkg/server/err"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sys/linux"
	"go.uber.org/zap"
)

// Run runs the server.
func (s *Server) Run(dom *domain.Domain) error {
	metrics.ServerEventCounter.WithLabelValues(metrics.ServerStart).Inc()
	s.reportConfig()

	// Start HTTP API to report tidb info such as TPS.
	if s.cfg.Status.ReportStatus {
		err := s.startStatusHTTP()
		if err != nil {
			log.Error("failed to create the server", zap.Error(err), zap.Stack("stack"))
			return err
		}
		mppcoordmanager.InstanceMPPCoordinatorManager.InitServerAddr(s.GetStatusServerAddr())
	}
	if config.GetGlobalConfig().Performance.ForceInitStats && dom != nil {
		<-dom.StatsHandle().InitStatsDone
	}
	// If error should be reported and exit the server it can be sent on this
	// channel. Otherwise, end with sending a nil error to signal "done"
	errChan := make(chan error, 2)
	err := s.initTiDBListener()
	if err != nil {
		log.Error("failed to create the server", zap.Error(err), zap.Stack("stack"))
		return err
	}
	// Register error API is not thread-safe, the caller MUST NOT register errors after initialization.
	// To prevent misuse, set a flag to indicate that register new error will panic immediately.
	// For regression of issue like https://github.com/pingcap/tidb/issues/28190
	terror.RegisterFinish()
	go s.startNetworkListener(s.listener, false, errChan)
	go s.startNetworkListener(s.socket, true, errChan)
	if RunInGoTest && !isClosed(RunInGoTestChan) {
		close(RunInGoTestChan)
	}
	s.health.Store(true)
	err = <-errChan
	if err != nil {
		return err
	}
	return <-errChan
}

// isClosed is to check if the channel is closed
func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
	}

	return false
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

func (*Server) checkAuditPlugin(clientConn *clientConn) error {
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
	if statusServer := s.statusServer.Load(); statusServer != nil {
		err := statusServer.Close()
		terror.Log(errors.Trace(err))
		s.statusServer.Store(nil)
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
	metrics.ServerEventCounter.WithLabelValues(metrics.ServerStop).Inc()
}

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

	logger := logutil.BgLogger()
	if s.inShutdownMode.Load() {
		logger.Info("close connection directly when shutting down")
		terror.Log(closeConn(conn))
		return false
	}
	s.clients[conn.connectionID] = conn
	return true
}

// onConn runs in its own goroutine, handles queries from this connection.
func (s *Server) onConn(conn *clientConn) {
	if s.StandbyController != nil {
		s.StandbyController.OnConnActive()
	}

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
		case servererr.ErrConCount:
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

	if err := conn.increaseUserConnectionsCount(); err != nil {
		logutil.BgLogger().With(zap.Uint64("conn", conn.connectionID)).
			Warn("failed to increase the count of connections", zap.Error(err),
				zap.String("remote addr", conn.bufReadConn.RemoteAddr().String()))
		return
	}
	defer conn.decreaseUserConnectionCount()

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

