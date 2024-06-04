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

// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	goerr "errors"
	"fmt"
	"io"
	"net"
	"os/user"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/domain/resourcegroup"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/plugin"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/privilege/conn"
	"github.com/pingcap/tidb/pkg/privilege/privileges/ldap"
	servererr "github.com/pingcap/tidb/pkg/server/err"
	"github.com/pingcap/tidb/pkg/server/handler/tikvhandler"
	"github.com/pingcap/tidb/pkg/server/internal"
	"github.com/pingcap/tidb/pkg/server/internal/column"
	"github.com/pingcap/tidb/pkg/server/internal/dump"
	"github.com/pingcap/tidb/pkg/server/internal/handshake"
	"github.com/pingcap/tidb/pkg/server/internal/parse"
	"github.com/pingcap/tidb/pkg/server/internal/resultset"
	util2 "github.com/pingcap/tidb/pkg/server/internal/util"
	server_metrics "github.com/pingcap/tidb/pkg/server/metrics"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/arena"
	"github.com/pingcap/tidb/pkg/util/chunk"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/resourcegrouptag"
	tlsutil "github.com/pingcap/tidb/pkg/util/tls"
	"github.com/pingcap/tidb/pkg/util/topsql"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

const (
	connStatusDispatching int32 = iota
	connStatusReading
	connStatusShutdown     = variable.ConnStatusShutdown // Closed by server.
	connStatusWaitShutdown = 3                           // Notified by server to close.
)

var (
	statusCompression          = "Compression"
	statusCompressionAlgorithm = "Compression_algorithm"
	statusCompressionLevel     = "Compression_level"
)

var (
	// ConnectionInMemCounterForTest is a variable to count live connection object
	ConnectionInMemCounterForTest = atomic.Int64{}
)

// newClientConn creates a *clientConn object.
func newClientConn(s *Server) *clientConn {
	cc := &clientConn{
		server:       s,
		connectionID: s.dom.NextConnID(),
		collation:    mysql.DefaultCollationID,
		alloc:        arena.NewAllocator(32 * 1024),
		chunkAlloc:   chunk.NewAllocator(),
		status:       connStatusDispatching,
		lastActive:   time.Now(),
		authPlugin:   mysql.AuthNativePassword,
		quit:         make(chan struct{}),
		ppEnabled:    s.cfg.ProxyProtocol.Networks != "",
	}

	if intest.InTest {
		ConnectionInMemCounterForTest.Add(1)
		runtime.SetFinalizer(cc, func(*clientConn) {
			ConnectionInMemCounterForTest.Add(-1)
		})
	}
	return cc
}

// clientConn represents a connection between server and client, it maintains connection specific state,
// handles client query.
type clientConn struct {
	pkt          *internal.PacketIO      // a helper to read and write data in packet format.
	bufReadConn  *util2.BufferedReadConn // a buffered-read net.Conn or buffered-read tls.Conn.
	tlsConn      *tls.Conn               // TLS connection, nil if not TLS.
	server       *Server                 // a reference of server instance.
	capability   uint32                  // client capability affects the way server handles client request.
	connectionID uint64                  // atomically allocated by a global variable, unique in process scope.
	user         string                  // user of the client.
	dbname       string                  // default database name.
	salt         []byte                  // random bytes used for authentication.
	alloc        arena.Allocator         // an memory allocator for reducing memory allocation.
	chunkAlloc   chunk.Allocator
	lastPacket   []byte // latest sql query string, currently used for logging error.
	// ShowProcess() and mysql.ComChangeUser both visit this field, ShowProcess() read information through
	// the TiDBContext and mysql.ComChangeUser re-create it, so a lock is required here.
	ctx struct {
		sync.RWMutex
		*TiDBContext // an interface to execute sql statements.
	}
	attrs         map[string]string     // attributes parsed from client handshake response.
	serverHost    string                // server host
	peerHost      string                // peer host
	peerPort      string                // peer port
	status        int32                 // dispatching/reading/shutdown/waitshutdown
	lastCode      uint16                // last error code
	collation     uint8                 // collation used by client, may be different from the collation used by database.
	lastActive    time.Time             // last active time
	authPlugin    string                // default authentication plugin
	isUnixSocket  bool                  // connection is Unix Socket file
	closeOnce     sync.Once             // closeOnce is used to make sure clientConn closes only once
	rsEncoder     *column.ResultEncoder // rsEncoder is used to encode the string result to different charsets
	inputDecoder  *util2.InputDecoder   // inputDecoder is used to decode the different charsets of incoming strings to utf-8
	socketCredUID uint32                // UID from the other end of the Unix Socket
	// mu is used for cancelling the execution of current transaction.
	mu struct {
		sync.RWMutex
		cancelFunc context.CancelFunc
	}
	// quit is close once clientConn quit Run().
	quit       chan struct{}
	extensions *extension.SessionExtensions

	// Proxy Protocol Enabled
	ppEnabled bool
}

func (cc *clientConn) getCtx() *TiDBContext {
	cc.ctx.RLock()
	defer cc.ctx.RUnlock()
	return cc.ctx.TiDBContext
}

func (cc *clientConn) SetCtx(ctx *TiDBContext) {
	cc.ctx.Lock()
	cc.ctx.TiDBContext = ctx
	cc.ctx.Unlock()
}

func (cc *clientConn) String() string {
	// MySQL converts a collation from u32 to char in the protocol, so the value could be wrong. It works fine for the
	// default parameters (and libmysql seems not to provide any way to specify the collation other than the default
	// one), so it's not a big problem.
	collationStr := mysql.Collations[uint16(cc.collation)]
	return fmt.Sprintf("id:%d, addr:%s status:%b, collation:%s, user:%s",
		cc.connectionID, cc.bufReadConn.RemoteAddr(), cc.ctx.Status(), collationStr, cc.user,
	)
}

func (cc *clientConn) setStatus(status int32) {
	atomic.StoreInt32(&cc.status, status)
	if ctx := cc.getCtx(); ctx != nil {
		atomic.StoreInt32(&ctx.GetSessionVars().ConnectionStatus, status)
	}
}

func (cc *clientConn) getStatus() int32 {
	return atomic.LoadInt32(&cc.status)
}

func (cc *clientConn) CompareAndSwapStatus(oldStatus, newStatus int32) bool {
	return atomic.CompareAndSwapInt32(&cc.status, oldStatus, newStatus)
}

// authSwitchRequest is used by the server to ask the client to switch to a different authentication
// plugin. MySQL 8.0 libmysqlclient based clients by default always try `caching_sha2_password`, even
// when the server advertises the its default to be `mysql_native_password`. In addition to this switching
// may be needed on a per user basis as the authentication method is set per user.
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_request.html
// https://bugs.mysql.com/bug.php?id=93044
func (cc *clientConn) authSwitchRequest(ctx context.Context, plugin string) ([]byte, error) {
	clientPlugin := plugin
	if plugin == mysql.AuthLDAPSASL {
		clientPlugin += "_client"
	} else if plugin == mysql.AuthLDAPSimple {
		clientPlugin = mysql.AuthMySQLClearPassword
	}
	failpoint.Inject("FakeAuthSwitch", func() {
		failpoint.Return([]byte(clientPlugin), nil)
	})
	enclen := 1 + len(clientPlugin) + 1 + len(cc.salt) + 1
	data := cc.alloc.AllocWithLen(4, enclen)
	data = append(data, mysql.AuthSwitchRequest) // switch request
	data = append(data, []byte(clientPlugin)...)
	data = append(data, byte(0x00)) // requires null
	if plugin == mysql.AuthLDAPSASL {
		// append sasl auth method name
		data = append(data, []byte(ldap.LDAPSASLAuthImpl.GetSASLAuthMethod())...)
		data = append(data, byte(0x00))
	} else {
		data = append(data, cc.salt...)
		data = append(data, 0)
	}
	err := cc.writePacket(data)
	if err != nil {
		logutil.Logger(ctx).Debug("write response to client failed", zap.Error(err))
		return nil, err
	}
	err = cc.flush(ctx)
	if err != nil {
		logutil.Logger(ctx).Debug("flush response to client failed", zap.Error(err))
		return nil, err
	}
	resp, err := cc.readPacket()
	if err != nil {
		err = errors.SuspendStack(err)
		if errors.Cause(err) == io.EOF {
			logutil.Logger(ctx).Warn("authSwitchRequest response fail due to connection has be closed by client-side")
		} else {
			logutil.Logger(ctx).Warn("authSwitchRequest response fail", zap.Error(err))
		}
		return nil, err
	}
	cc.authPlugin = plugin
	return resp, nil
}

// handshake works like TCP handshake, but in a higher level, it first writes initial packet to client,
// during handshake, client and server negotiate compatible features and do authentication.
// After handshake, client can send sql query to server.
func (cc *clientConn) handshake(ctx context.Context) error {
	if err := cc.writeInitialHandshake(ctx); err != nil {
		if errors.Cause(err) == io.EOF {
			logutil.Logger(ctx).Debug("Could not send handshake due to connection has be closed by client-side")
		} else {
			logutil.Logger(ctx).Debug("Write init handshake to client fail", zap.Error(errors.SuspendStack(err)))
		}
		return err
	}
	if err := cc.readOptionalSSLRequestAndHandshakeResponse(ctx); err != nil {
		err1 := cc.writeError(ctx, err)
		if err1 != nil {
			logutil.Logger(ctx).Debug("writeError failed", zap.Error(err1))
		}
		return err
	}

	// MySQL supports an "init_connect" query, which can be run on initial connection.
	// The query must return a non-error or the client is disconnected.
	if err := cc.initConnect(ctx); err != nil {
		logutil.Logger(ctx).Warn("init_connect failed", zap.Error(err))
		initErr := servererr.ErrNewAbortingConnection.FastGenByArgs(cc.connectionID, "unconnected", cc.user, cc.peerHost, "init_connect command failed")
		if err1 := cc.writeError(ctx, initErr); err1 != nil {
			terror.Log(err1)
		}
		return initErr
	}

	data := cc.alloc.AllocWithLen(4, 32)
	data = append(data, mysql.OKHeader)
	data = append(data, 0, 0)
	if cc.capability&mysql.ClientProtocol41 > 0 {
		data = dump.Uint16(data, mysql.ServerStatusAutocommit)
		data = append(data, 0, 0)
	}

	err := cc.writePacket(data)
	cc.pkt.SetSequence(0)
	if err != nil {
		err = errors.SuspendStack(err)
		logutil.Logger(ctx).Debug("write response to client failed", zap.Error(err))
		return err
	}

	err = cc.flush(ctx)
	if err != nil {
		err = errors.SuspendStack(err)
		logutil.Logger(ctx).Debug("flush response to client failed", zap.Error(err))
		return err
	}

	// With mysql --compression-algorithms=zlib,zstd both flags are set, the result is Zlib
	if cc.capability&mysql.ClientCompress > 0 {
		cc.pkt.SetCompressionAlgorithm(mysql.CompressionZlib)
		cc.ctx.SetCompressionAlgorithm(mysql.CompressionZlib)
	} else if cc.capability&mysql.ClientZstdCompressionAlgorithm > 0 {
		cc.pkt.SetCompressionAlgorithm(mysql.CompressionZstd)
		cc.ctx.SetCompressionAlgorithm(mysql.CompressionZstd)
	}

	return err
}

func (cc *clientConn) Close() error {
	// Be careful, this function should be re-entrant. It might be called more than once for a single connection.
	// Any logic which is not idempotent should be in closeConn() and wrapped with `cc.closeOnce.Do`, like decresing
	// metrics, releasing resources, etc.
	//
	// TODO: avoid calling this function multiple times. It's not intuitive that a connection can be closed multiple
	// times.
	cc.server.rwlock.Lock()
	delete(cc.server.clients, cc.connectionID)
	cc.server.rwlock.Unlock()
	return closeConn(cc)
}

// closeConn is idempotent and thread-safe.
// It will be called on the same `clientConn` more than once to avoid connection leak.
func closeConn(cc *clientConn) error {
	var err error
	cc.closeOnce.Do(func() {
		if cc.connectionID > 0 {
			cc.server.dom.ReleaseConnID(cc.connectionID)
			cc.connectionID = 0
		}
		if cc.bufReadConn != nil {
			err := cc.bufReadConn.Close()
			if err != nil {
				// We need to expect connection might have already disconnected.
				// This is because closeConn() might be called after a connection read-timeout.
				logutil.Logger(context.Background()).Debug("could not close connection", zap.Error(err))
			}
		}
		// Close statements and session
		// At first, it'll decrese the count of connections in the resource group, update the corresponding gauge.
		// Then it'll close the statements and session, which release advisory locks, row locks, etc.
		if ctx := cc.getCtx(); ctx != nil {
			resourceGroupName := ctx.GetSessionVars().ResourceGroupName
			metrics.ConnGauge.WithLabelValues(resourceGroupName).Dec()

			err = ctx.Close()
		}
	})
	return err
}

func (cc *clientConn) closeWithoutLock() error {
	delete(cc.server.clients, cc.connectionID)
	return closeConn(cc)
}

// writeInitialHandshake sends server version, connection ID, server capability, collation, server status
// and auth salt to the client.
func (cc *clientConn) writeInitialHandshake(ctx context.Context) error {
	data := make([]byte, 4, 128)

	// min version 10
	data = append(data, 10)
	// server version[00]
	data = append(data, mysql.ServerVersion...)
	data = append(data, 0)
	// connection id
	data = append(data, byte(cc.connectionID), byte(cc.connectionID>>8), byte(cc.connectionID>>16), byte(cc.connectionID>>24))
	// auth-plugin-data-part-1
	data = append(data, cc.salt[0:8]...)
	// filler [00]
	data = append(data, 0)
	// capability flag lower 2 bytes, using default capability here
	data = append(data, byte(cc.server.capability), byte(cc.server.capability>>8))
	// charset
	if cc.collation == 0 {
		cc.collation = uint8(mysql.DefaultCollationID)
	}
	data = append(data, cc.collation)
	// status
	data = dump.Uint16(data, mysql.ServerStatusAutocommit)
	// below 13 byte may not be used
	// capability flag upper 2 bytes, using default capability here
	data = append(data, byte(cc.server.capability>>16), byte(cc.server.capability>>24))
	// length of auth-plugin-data
	data = append(data, byte(len(cc.salt)+1))
	// reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	// auth-plugin-data-part-2
	data = append(data, cc.salt[8:]...)
	data = append(data, 0)
	// auth-plugin name
	if ctx := cc.getCtx(); ctx == nil {
		if err := cc.openSession(); err != nil {
			return err
		}
	}
	defAuthPlugin, err := cc.ctx.GetSessionVars().GetGlobalSystemVar(context.Background(), variable.DefaultAuthPlugin)
	if err != nil {
		return err
	}
	cc.authPlugin = defAuthPlugin
	data = append(data, []byte(defAuthPlugin)...)

	// Close the session to force this to be re-opened after we parse the response. This is needed
	// to ensure we use the collation and client flags from the response for the session.
	if err = cc.ctx.Close(); err != nil {
		return err
	}
	cc.SetCtx(nil)

	data = append(data, 0)
	if err = cc.writePacket(data); err != nil {
		return err
	}
	return cc.flush(ctx)
}

func (cc *clientConn) readPacket() ([]byte, error) {
	if cc.getCtx() != nil {
		cc.pkt.SetMaxAllowedPacket(cc.ctx.GetSessionVars().MaxAllowedPacket)
	}
	return cc.pkt.ReadPacket()
}

func (cc *clientConn) writePacket(data []byte) error {
	failpoint.Inject("FakeClientConn", func() {
		if cc.pkt == nil {
			failpoint.Return(nil)
		}
	})
	return cc.pkt.WritePacket(data)
}

func (cc *clientConn) getWaitTimeout(ctx context.Context) uint64 {
	sessVars := cc.ctx.GetSessionVars()
	if sessVars.InTxn() && sessVars.IdleTransactionTimeout > 0 {
		return uint64(sessVars.IdleTransactionTimeout)
	}
	return cc.getSessionVarsWaitTimeout(ctx)
}

// getSessionVarsWaitTimeout get session variable wait_timeout
func (cc *clientConn) getSessionVarsWaitTimeout(ctx context.Context) uint64 {
	valStr, exists := cc.ctx.GetSessionVars().GetSystemVar(variable.WaitTimeout)
	if !exists {
		return variable.DefWaitTimeout
	}
	waitTimeout, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		logutil.Logger(ctx).Warn("get sysval wait_timeout failed, use default value", zap.Error(err))
		// if get waitTimeout error, use default value
		return variable.DefWaitTimeout
	}
	return waitTimeout
}

func (cc *clientConn) readOptionalSSLRequestAndHandshakeResponse(ctx context.Context) error {
	// Read a packet. It may be a SSLRequest or HandshakeResponse.
	data, err := cc.readPacket()
	if err != nil {
		err = errors.SuspendStack(err)
		if errors.Cause(err) == io.EOF {
			logutil.Logger(ctx).Debug("wait handshake response fail due to connection has be closed by client-side")
		} else {
			logutil.Logger(ctx).Debug("wait handshake response fail", zap.Error(err))
		}
		return err
	}

	var resp handshake.Response41
	var pos int

	if len(data) < 2 {
		logutil.Logger(ctx).Error("got malformed handshake response", zap.ByteString("packetData", data))
		return mysql.ErrMalformPacket
	}

	capability := uint32(binary.LittleEndian.Uint16(data[:2]))
	if capability&mysql.ClientProtocol41 <= 0 {
		logutil.Logger(ctx).Error("ClientProtocol41 flag is not set, please upgrade client")
		return servererr.ErrNotSupportedAuthMode
	}
	pos, err = parse.HandshakeResponseHeader(ctx, &resp, data)
	if err != nil {
		terror.Log(err)
		return err
	}

	// After read packets we should update the client's host and port to grab
	// real client's IP and port from PROXY Protocol header if PROXY Protocol is enabled.
	_, _, err = cc.PeerHost("", true)
	if err != nil {
		terror.Log(err)
		return err
	}
	// If enable proxy protocol check audit plugins after update real IP
	if cc.ppEnabled {
		err = cc.server.checkAuditPlugin(cc)
		if err != nil {
			return err
		}
	}

	if resp.Capability&mysql.ClientSSL > 0 {
		tlsConfig := (*tls.Config)(atomic.LoadPointer(&cc.server.tlsConfig))
		if tlsConfig != nil {
			// The packet is a SSLRequest, let's switch to TLS.
			if err = cc.upgradeToTLS(tlsConfig); err != nil {
				return err
			}
			// Read the following HandshakeResponse packet.
			data, err = cc.readPacket()
			if err != nil {
				logutil.Logger(ctx).Warn("read handshake response failure after upgrade to TLS", zap.Error(err))
				return err
			}
			pos, err = parse.HandshakeResponseHeader(ctx, &resp, data)
			if err != nil {
				terror.Log(err)
				return err
			}
		}
	} else if tlsutil.RequireSecureTransport.Load() && !cc.isUnixSocket {
		// If it's not a socket connection, we should reject the connection
		// because TLS is required.
		err := servererr.ErrSecureTransportRequired.FastGenByArgs()
		terror.Log(err)
		return err
	}

	// Read the remaining part of the packet.
	err = parse.HandshakeResponseBody(ctx, &resp, data, pos)
	if err != nil {
		terror.Log(err)
		return err
	}

	cc.capability = resp.Capability & cc.server.capability
	cc.user = resp.User
	cc.dbname = resp.DBName
	cc.collation = resp.Collation
	cc.attrs = resp.Attrs
	cc.pkt.SetZstdLevel(zstd.EncoderLevelFromZstd(resp.ZstdLevel))

	err = cc.handleAuthPlugin(ctx, &resp)
	if err != nil {
		return err
	}

	switch resp.AuthPlugin {
	case mysql.AuthCachingSha2Password:
		resp.Auth, err = cc.authSha(ctx, resp)
		if err != nil {
			return err
		}
	case mysql.AuthTiDBSM3Password:
		resp.Auth, err = cc.authSM3(ctx, resp)
		if err != nil {
			return err
		}
	case mysql.AuthNativePassword:
	case mysql.AuthSocket:
	case mysql.AuthTiDBSessionToken:
	case mysql.AuthTiDBAuthToken:
	case mysql.AuthMySQLClearPassword:
	case mysql.AuthLDAPSASL:
	case mysql.AuthLDAPSimple:
	default:
		return errors.New("Unknown auth plugin")
	}

	err = cc.openSessionAndDoAuth(resp.Auth, resp.AuthPlugin, resp.ZstdLevel)
	if err != nil {
		logutil.Logger(ctx).Warn("open new session or authentication failure", zap.Error(err))
	}
	return err
}

func (cc *clientConn) handleAuthPlugin(ctx context.Context, resp *handshake.Response41) error {
	if resp.Capability&mysql.ClientPluginAuth > 0 {
		newAuth, err := cc.checkAuthPlugin(ctx, resp)
		if err != nil {
			logutil.Logger(ctx).Warn("failed to check the user authplugin", zap.Error(err))
			return err
		}
		if len(newAuth) > 0 {
			resp.Auth = newAuth
		}

		switch resp.AuthPlugin {
		case mysql.AuthCachingSha2Password:
		case mysql.AuthTiDBSM3Password:
		case mysql.AuthNativePassword:
		case mysql.AuthSocket:
		case mysql.AuthTiDBSessionToken:
		case mysql.AuthMySQLClearPassword:
		case mysql.AuthLDAPSASL:
		case mysql.AuthLDAPSimple:
		default:
			logutil.Logger(ctx).Warn("Unknown Auth Plugin", zap.String("plugin", resp.AuthPlugin))
		}
	} else {
		// MySQL 5.1 and older clients don't support authentication plugins.
		logutil.Logger(ctx).Warn("Client without Auth Plugin support; Please upgrade client")
		_, err := cc.checkAuthPlugin(ctx, resp)
		if err != nil {
			return err
		}
		resp.AuthPlugin = mysql.AuthNativePassword
	}
	return nil
}

// authSha implements the caching_sha2_password specific part of the protocol.
func (cc *clientConn) authSha(ctx context.Context, resp handshake.Response41) ([]byte, error) {
	const (
		shaCommand       = 1
		requestRsaPubKey = 2 // Not supported yet, only TLS is supported as secure channel.
		fastAuthOk       = 3
		fastAuthFail     = 4
	)

	// If no password is specified, we don't send the FastAuthFail to do the full authentication
	// as that doesn't make sense without a password and confuses the client.
	// https://github.com/pingcap/tidb/issues/40831
	if len(resp.Auth) == 0 {
		return []byte{}, nil
	}

	// Currently we always send a "FastAuthFail" as the cached part of the protocol isn't implemented yet.
	// This triggers the client to send the full response.
	err := cc.writePacket([]byte{0, 0, 0, 0, shaCommand, fastAuthFail})
	if err != nil {
		logutil.Logger(ctx).Error("authSha packet write failed", zap.Error(err))
		return nil, err
	}
	err = cc.flush(ctx)
	if err != nil {
		logutil.Logger(ctx).Error("authSha packet flush failed", zap.Error(err))
		return nil, err
	}

	data, err := cc.readPacket()
	if err != nil {
		logutil.Logger(ctx).Error("authSha packet read failed", zap.Error(err))
		return nil, err
	}
	return bytes.Trim(data, "\x00"), nil
}

// authSM3 implements the tidb_sm3_password specific part of the protocol.
// tidb_sm3_password is very similar to caching_sha2_password.
func (cc *clientConn) authSM3(ctx context.Context, resp handshake.Response41) ([]byte, error) {
	// If no password is specified, we don't send the FastAuthFail to do the full authentication
	// as that doesn't make sense without a password and confuses the client.
	// https://github.com/pingcap/tidb/issues/40831
	if len(resp.Auth) == 0 {
		return []byte{}, nil
	}

	err := cc.writePacket([]byte{0, 0, 0, 0, 1, 4}) // fastAuthFail
	if err != nil {
		logutil.Logger(ctx).Error("authSM3 packet write failed", zap.Error(err))
		return nil, err
	}
	err = cc.flush(ctx)
	if err != nil {
		logutil.Logger(ctx).Error("authSM3 packet flush failed", zap.Error(err))
		return nil, err
	}

	data, err := cc.readPacket()
	if err != nil {
		logutil.Logger(ctx).Error("authSM3 packet read failed", zap.Error(err))
		return nil, err
	}
	return bytes.Trim(data, "\x00"), nil
}

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
	identity, err := cc.ctx.MatchIdentity(cc.user, host)
	if err != nil {
		return nil, servererr.ErrAccessDenied.FastGenByArgs(cc.user, host, hasPassword)
	}
	// Get the plugin for the identity.
	userplugin, err := cc.ctx.AuthPluginForUser(identity)
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
		return []byte(user.Username), nil
	}
	if len(userplugin) == 0 {
		// No user plugin set, assuming MySQL Native Password
		// This happens if the account doesn't exist or if the account doesn't have
		// a password set.
		if resp.AuthPlugin != mysql.AuthNativePassword {
			if resp.Capability&mysql.ClientPluginAuth > 0 {
				resp.AuthPlugin = mysql.AuthNativePassword
				authData, err := cc.authSwitchRequest(ctx, mysql.AuthNativePassword)
				if err != nil {
					return nil, err
				}
				return authData, nil
			}
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
	host = variable.DefHostname
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
	chs, err := cc.ctx.GetSessionVars().GetSessionOrGlobalSystemVar(context.Background(), variable.CharacterSetResults)
	if err != nil {
		chs = ""
		logutil.Logger(ctx).Warn("get character_set_results system variable failed", zap.Error(err))
	}
	cc.rsEncoder = column.NewResultEncoder(chs)
}

func (cc *clientConn) initInputEncoder(ctx context.Context) {
	chs, err := cc.ctx.GetSessionVars().GetSessionOrGlobalSystemVar(context.Background(), variable.CharacterSetClient)
	if err != nil {
		chs = ""
		logutil.Logger(ctx).Warn("get character_set_client system variable failed", zap.Error(err))
	}
	cc.inputDecoder = util2.NewInputDecoder(chs)
}

// initConnect runs the initConnect SQL statement if it has been specified.
// The semantics are MySQL compatible.
func (cc *clientConn) initConnect(ctx context.Context) error {
	val, err := cc.ctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.InitConnect)
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

// Run reads client query and writes query result to client in for loop, if there is a panic during query handling,
// it will be recovered and log the panic error.
// This function returns and the connection is closed if there is an IO error or there is a panic.
func (cc *clientConn) Run(ctx context.Context) {
	defer func() {
		r := recover()
		if r != nil {
			logutil.Logger(ctx).Error("connection running loop panic",
				zap.Stringer("lastSQL", getLastStmtInConn{cc}),
				zap.String("err", fmt.Sprintf("%v", r)),
				zap.Stack("stack"),
			)
			err := cc.writeError(ctx, fmt.Errorf("%v", r))
			terror.Log(err)
			metrics.PanicCounter.WithLabelValues(metrics.LabelSession).Inc()
		}
		if cc.getStatus() != connStatusShutdown {
			err := cc.Close()
			terror.Log(err)
		}

		close(cc.quit)
	}()

	parentCtx := ctx
	var traceInfo *model.TraceInfo
	// Usually, client connection status changes between [dispatching] <=> [reading].
	// When some event happens, server may notify this client connection by setting
	// the status to special values, for example: kill or graceful shutdown.
	// The client connection would detect the events when it fails to change status
	// by CAS operation, it would then take some actions accordingly.
	for {
		sessVars := cc.ctx.GetSessionVars()
		if alias := sessVars.SessionAlias; traceInfo == nil || traceInfo.SessionAlias != alias {
			// We should reset the context trace info when traceInfo not inited or session alias changed.
			traceInfo = &model.TraceInfo{
				ConnectionID: cc.connectionID,
				SessionAlias: alias,
			}
			ctx = logutil.WithSessionAlias(parentCtx, sessVars.SessionAlias)
			ctx = tracing.ContextWithTraceInfo(ctx, traceInfo)
		}

		// Close connection between txn when we are going to shutdown server.
		// Note the current implementation when shutting down, for an idle connection, the connection may block at readPacket()
		// consider provider a way to close the connection directly after sometime if we can not read any data.
		if cc.server.inShutdownMode.Load() {
			if !sessVars.InTxn() {
				return
			}
		}

		if !cc.CompareAndSwapStatus(connStatusDispatching, connStatusReading) ||
			// The judge below will not be hit by all means,
			// But keep it stayed as a reminder and for the code reference for connStatusWaitShutdown.
			cc.getStatus() == connStatusWaitShutdown {
			return
		}

		cc.alloc.Reset()
		// close connection when idle time is more than wait_timeout
		// default 28800(8h), FIXME: should not block at here when we kill the connection.
		waitTimeout := cc.getWaitTimeout(ctx)
		cc.pkt.SetReadTimeout(time.Duration(waitTimeout) * time.Second)
		start := time.Now()
		data, err := cc.readPacket()
		if err != nil {
			if terror.ErrorNotEqual(err, io.EOF) {
				if netErr, isNetErr := errors.Cause(err).(net.Error); isNetErr && netErr.Timeout() {
					if cc.getStatus() == connStatusWaitShutdown {
						logutil.Logger(ctx).Info("read packet timeout because of killed connection")
					} else {
						idleTime := time.Since(start)
						logutil.Logger(ctx).Info("read packet timeout, close this connection",
							zap.Duration("idle", idleTime),
							zap.Uint64("waitTimeout", waitTimeout),
							zap.Error(err),
						)
					}
				} else if errors.ErrorEqual(err, servererr.ErrNetPacketTooLarge) {
					err := cc.writeError(ctx, err)
					if err != nil {
						terror.Log(err)
					}
				} else {
					errStack := errors.ErrorStack(err)
					if !strings.Contains(errStack, "use of closed network connection") {
						logutil.Logger(ctx).Warn("read packet failed, close this connection",
							zap.Error(errors.SuspendStack(err)))
					}
				}
			}
			server_metrics.DisconnectByClientWithError.Inc()
			return
		}

		// Should check InTxn() to avoid execute `begin` stmt.
		if cc.server.inShutdownMode.Load() {
			if !cc.ctx.GetSessionVars().InTxn() {
				return
			}
		}

		if !cc.CompareAndSwapStatus(connStatusReading, connStatusDispatching) {
			return
		}

		startTime := time.Now()
		err = cc.dispatch(ctx, data)
		cc.ctx.GetSessionVars().ClearAlloc(&cc.chunkAlloc, err != nil)
		cc.chunkAlloc.Reset()
		if err != nil {
			cc.audit(plugin.Error) // tell the plugin API there was a dispatch error
			if terror.ErrorEqual(err, io.EOF) {
				cc.addMetrics(data[0], startTime, nil)
				server_metrics.DisconnectNormal.Inc()
				return
			} else if terror.ErrResultUndetermined.Equal(err) {
				logutil.Logger(ctx).Error("result undetermined, close this connection", zap.Error(err))
				server_metrics.DisconnectErrorUndetermined.Inc()
				return
			} else if terror.ErrCritical.Equal(err) {
				metrics.CriticalErrorCounter.Add(1)
				logutil.Logger(ctx).Fatal("critical error, stop the server", zap.Error(err))
			}
			var txnMode string
			if ctx := cc.getCtx(); ctx != nil {
				txnMode = ctx.GetSessionVars().GetReadableTxnMode()
			}
			vars := cc.getCtx().GetSessionVars()
			for _, dbName := range session.GetDBNames(vars) {
				metrics.ExecuteErrorCounter.WithLabelValues(metrics.ExecuteErrorToLabel(err), dbName, vars.ResourceGroupName).Inc()
			}

			if storeerr.ErrLockAcquireFailAndNoWaitSet.Equal(err) {
				logutil.Logger(ctx).Debug("Expected error for FOR UPDATE NOWAIT", zap.Error(err))
			} else {
				var timestamp uint64
				if ctx := cc.getCtx(); ctx != nil && ctx.GetSessionVars() != nil && ctx.GetSessionVars().TxnCtx != nil {
					timestamp = ctx.GetSessionVars().TxnCtx.StartTS
					if timestamp == 0 && ctx.GetSessionVars().TxnCtx.StaleReadTs > 0 {
						// for state-read query.
						timestamp = ctx.GetSessionVars().TxnCtx.StaleReadTs
					}
				}
				logutil.Logger(ctx).Info("command dispatched failed",
					zap.String("connInfo", cc.String()),
					zap.String("command", mysql.Command2Str[data[0]]),
					zap.String("status", cc.SessionStatusToString()),
					zap.Stringer("sql", getLastStmtInConn{cc}),
					zap.String("txn_mode", txnMode),
					zap.Uint64("timestamp", timestamp),
					zap.String("err", errStrForLog(err, cc.ctx.GetSessionVars().EnableRedactLog)),
				)
			}
			err1 := cc.writeError(ctx, err)
			terror.Log(err1)
		}
		cc.addMetrics(data[0], startTime, err)
		cc.pkt.SetSequence(0)
		cc.pkt.SetCompressedSequence(0)
	}
}

func errStrForLog(err error, redactMode string) string {
	if redactMode != errors.RedactLogDisable {
		// currently, only ErrParse is considered when enableRedactLog because it may contain sensitive information like
		// password or accesskey
		if parser.ErrParse.Equal(err) {
			return "fail to parse SQL, and must redact the whole error when enable log redaction"
		}
	}
	var ret string
	if kv.ErrKeyExists.Equal(err) || parser.ErrParse.Equal(err) || infoschema.ErrTableNotExists.Equal(err) {
		// Do not log stack for duplicated entry error.
		ret = err.Error()
	} else {
		ret = errors.ErrorStack(err)
	}
	return ret
}

func (cc *clientConn) addMetrics(cmd byte, startTime time.Time, err error) {
	if cmd == mysql.ComQuery && cc.ctx.Value(sessionctx.LastExecuteDDL) != nil {
		// Don't take DDL execute time into account.
		// It's already recorded by other metrics in ddl package.
		return
	}

	vars := cc.getCtx().GetSessionVars()
	resourceGroupName := vars.ResourceGroupName
	var counter prometheus.Counter
	if len(resourceGroupName) == 0 || resourceGroupName == resourcegroup.DefaultResourceGroupName {
		if err != nil && int(cmd) < len(server_metrics.QueryTotalCountErr) {
			counter = server_metrics.QueryTotalCountErr[cmd]
		} else if err == nil && int(cmd) < len(server_metrics.QueryTotalCountOk) {
			counter = server_metrics.QueryTotalCountOk[cmd]
		}
	}

	if counter != nil {
		counter.Inc()
	} else {
		label := server_metrics.CmdToString(cmd)
		if err != nil {
			metrics.QueryTotalCounter.WithLabelValues(label, "Error", resourceGroupName).Inc()
		} else {
			metrics.QueryTotalCounter.WithLabelValues(label, "OK", resourceGroupName).Inc()
		}
	}

	cost := time.Since(startTime)
	sessionVar := cc.ctx.GetSessionVars()
	affectedRows := cc.ctx.AffectedRows()
	cc.ctx.GetTxnWriteThroughputSLI().FinishExecuteStmt(cost, affectedRows, sessionVar.InTxn())

	stmtType := sessionVar.StmtCtx.StmtType
	sqlType := metrics.LblGeneral
	if stmtType != "" {
		sqlType = stmtType
	}

	switch sqlType {
	case "Insert":
		server_metrics.AffectedRowsCounterInsert.Add(float64(affectedRows))
	case "Replace":
		server_metrics.AffectedRowsCounterReplace.Add(float64(affectedRows))
	case "Delete":
		server_metrics.AffectedRowsCounterDelete.Add(float64(affectedRows))
	case "Update":
		server_metrics.AffectedRowsCounterUpdate.Add(float64(affectedRows))
	}

	for _, dbName := range session.GetDBNames(vars) {
		metrics.QueryDurationHistogram.WithLabelValues(sqlType, dbName, vars.StmtCtx.ResourceGroupName).Observe(cost.Seconds())
	}
}

// dispatch handles client request based on command which is the first byte of the data.
// It also gets a token from server which is used to limit the concurrently handling clients.
// The most frequently used command is ComQuery.
func (cc *clientConn) dispatch(ctx context.Context, data []byte) error {
	defer func() {
		// reset killed for each request
		cc.ctx.GetSessionVars().SQLKiller.Reset()
	}()
	t := time.Now()
	if (cc.ctx.Status() & mysql.ServerStatusInTrans) > 0 {
		server_metrics.ConnIdleDurationHistogramInTxn.Observe(t.Sub(cc.lastActive).Seconds())
	} else {
		server_metrics.ConnIdleDurationHistogramNotInTxn.Observe(t.Sub(cc.lastActive).Seconds())
	}

	cfg := config.GetGlobalConfig()
	if cfg.OpenTracing.Enable {
		var r tracing.Region
		r, ctx = tracing.StartRegionWithNewRootSpan(ctx, "server.dispatch")
		defer r.End()
	}

	var cancelFunc context.CancelFunc
	ctx, cancelFunc = context.WithCancel(ctx)
	cc.mu.Lock()
	cc.mu.cancelFunc = cancelFunc
	cc.mu.Unlock()

	cc.lastPacket = data
	cmd := data[0]
	data = data[1:]
	if topsqlstate.TopSQLEnabled() {
		defer pprof.SetGoroutineLabels(ctx)
	}
	if variable.EnablePProfSQLCPU.Load() {
		label := getLastStmtInConn{cc}.PProfLabel()
		if len(label) > 0 {
			defer pprof.SetGoroutineLabels(ctx)
			ctx = pprof.WithLabels(ctx, pprof.Labels("sql", label))
			pprof.SetGoroutineLabels(ctx)
		}
	}
	if trace.IsEnabled() {
		lc := getLastStmtInConn{cc}
		sqlType := lc.PProfLabel()
		if len(sqlType) > 0 {
			var task *trace.Task
			ctx, task = trace.NewTask(ctx, sqlType)
			defer task.End()

			trace.Log(ctx, "sql", lc.String())
			ctx = logutil.WithTraceLogger(ctx, tracing.TraceInfoFromContext(ctx))

			taskID := *(*uint64)(unsafe.Pointer(task))
			ctx = pprof.WithLabels(ctx, pprof.Labels("trace", strconv.FormatUint(taskID, 10)))
			pprof.SetGoroutineLabels(ctx)
		}
	}
	token := cc.server.getToken()
	defer func() {
		// if handleChangeUser failed, cc.ctx may be nil
		if ctx := cc.getCtx(); ctx != nil {
			ctx.SetProcessInfo("", t, mysql.ComSleep, 0)
		}

		cc.server.releaseToken(token)
		cc.lastActive = time.Now()
	}()

	vars := cc.ctx.GetSessionVars()
	// reset killed for each request
	vars.SQLKiller.Reset()
	if cmd < mysql.ComEnd {
		cc.ctx.SetCommandValue(cmd)
	}

	dataStr := string(hack.String(data))
	switch cmd {
	case mysql.ComPing, mysql.ComStmtClose, mysql.ComStmtSendLongData, mysql.ComStmtReset,
		mysql.ComSetOption, mysql.ComChangeUser:
		cc.ctx.SetProcessInfo("", t, cmd, 0)
	case mysql.ComInitDB:
		cc.ctx.SetProcessInfo("use "+dataStr, t, cmd, 0)
	}

	switch cmd {
	case mysql.ComQuit:
		return io.EOF
	case mysql.ComInitDB:
		node, err := cc.useDB(ctx, dataStr)
		cc.onExtensionStmtEnd(node, false, err)
		if err != nil {
			return err
		}
		return cc.writeOK(ctx)
	case mysql.ComQuery: // Most frequently used command.
		// For issue 1989
		// Input payload may end with byte '\0', we didn't find related mysql document about it, but mysql
		// implementation accept that case. So trim the last '\0' here as if the payload an EOF string.
		// See http://dev.mysql.com/doc/internals/en/com-query.html
		if len(data) > 0 && data[len(data)-1] == 0 {
			data = data[:len(data)-1]
			dataStr = string(hack.String(data))
		}
		return cc.handleQuery(ctx, dataStr)
	case mysql.ComFieldList:
		return cc.handleFieldList(ctx, dataStr)
	// ComCreateDB, ComDropDB
	case mysql.ComRefresh:
		return cc.handleRefresh(ctx, data[0])
	case mysql.ComShutdown: // redirect to SQL
		if err := cc.handleQuery(ctx, "SHUTDOWN"); err != nil {
			return err
		}
		return cc.writeOK(ctx)
	case mysql.ComStatistics:
		return cc.writeStats(ctx)
	// ComProcessInfo, ComConnect, ComProcessKill, ComDebug
	case mysql.ComPing:
		return cc.writeOK(ctx)
	case mysql.ComChangeUser:
		return cc.handleChangeUser(ctx, data)
	// ComBinlogDump, ComTableDump, ComConnectOut, ComRegisterSlave
	case mysql.ComStmtPrepare:
		// For issue 39132, same as ComQuery
		if len(data) > 0 && data[len(data)-1] == 0 {
			data = data[:len(data)-1]
			dataStr = string(hack.String(data))
		}
		return cc.HandleStmtPrepare(ctx, dataStr)
	case mysql.ComStmtExecute:
		return cc.handleStmtExecute(ctx, data)
	case mysql.ComStmtSendLongData:
		return cc.handleStmtSendLongData(data)
	case mysql.ComStmtClose:
		return cc.handleStmtClose(data)
	case mysql.ComStmtReset:
		return cc.handleStmtReset(ctx, data)
	case mysql.ComSetOption:
		return cc.handleSetOption(ctx, data)
	case mysql.ComStmtFetch:
		return cc.handleStmtFetch(ctx, data)
	// ComDaemon, ComBinlogDumpGtid
	case mysql.ComResetConnection:
		return cc.handleResetConnection(ctx)
	// ComEnd
	default:
		return mysql.NewErrf(mysql.ErrUnknown, "command %d not supported now", nil, cmd)
	}
}

func (cc *clientConn) writeStats(ctx context.Context) error {
	var err error
	var uptime int64
	info := tikvhandler.ServerInfo{}
	info.ServerInfo, err = infosync.GetServerInfo()
	if err != nil {
		logutil.BgLogger().Error("Failed to get ServerInfo for uptime status", zap.Error(err))
	} else {
		uptime = int64(time.Since(time.Unix(info.ServerInfo.StartTimestamp, 0)).Seconds())
	}
	msg := []byte(fmt.Sprintf("Uptime: %d  Threads: 0  Questions: 0  Slow queries: 0  Opens: 0  Flush tables: 0  Open tables: 0  Queries per second avg: 0.000",
		uptime))
	data := cc.alloc.AllocWithLen(4, len(msg))
	data = append(data, msg...)

	err = cc.writePacket(data)
	if err != nil {
		return err
	}

	return cc.flush(ctx)
}

func (cc *clientConn) useDB(ctx context.Context, db string) (node ast.StmtNode, err error) {
	// if input is "use `SELECT`", mysql client just send "SELECT"
	// so we add `` around db.
	stmts, err := cc.ctx.Parse(ctx, "use `"+db+"`")
	if err != nil {
		return nil, err
	}
	_, err = cc.ctx.ExecuteStmt(ctx, stmts[0])
	if err != nil {
		return stmts[0], err
	}
	cc.dbname = db
	return stmts[0], err
}

func (cc *clientConn) flush(ctx context.Context) error {
	var (
		stmtDetail *execdetails.StmtExecDetails
		startTime  time.Time
	)
	if stmtDetailRaw := ctx.Value(execdetails.StmtExecDetailKey); stmtDetailRaw != nil {
		//nolint:forcetypeassert
		stmtDetail = stmtDetailRaw.(*execdetails.StmtExecDetails)
		startTime = time.Now()
	}
	defer func() {
		if stmtDetail != nil {
			stmtDetail.WriteSQLRespDuration += time.Since(startTime)
		}
		trace.StartRegion(ctx, "FlushClientConn").End()
		if ctx := cc.getCtx(); ctx != nil && ctx.WarningCount() > 0 {
			for _, err := range ctx.GetWarnings() {
				var warn *errors.Error
				if ok := goerr.As(err.Err, &warn); ok {
					code := uint16(warn.Code())
					errno.IncrementWarning(code, cc.user, cc.peerHost)
				}
			}
		}
	}()
	failpoint.Inject("FakeClientConn", func() {
		if cc.pkt == nil {
			failpoint.Return(nil)
		}
	})
	return cc.pkt.Flush()
}

func (cc *clientConn) writeOK(ctx context.Context) error {
	return cc.writeOkWith(ctx, mysql.OKHeader, true, cc.ctx.Status())
}

func (cc *clientConn) writeOkWith(ctx context.Context, header byte, flush bool, status uint16) error {
	msg := cc.ctx.LastMessage()
	affectedRows := cc.ctx.AffectedRows()
	lastInsertID := cc.ctx.LastInsertID()
	warnCnt := cc.ctx.WarningCount()

	enclen := 0
	if len(msg) > 0 {
		enclen = util2.LengthEncodedIntSize(uint64(len(msg))) + len(msg)
	}

	data := cc.alloc.AllocWithLen(4, 32+enclen)
	data = append(data, header)
	data = dump.LengthEncodedInt(data, affectedRows)
	data = dump.LengthEncodedInt(data, lastInsertID)
	if cc.capability&mysql.ClientProtocol41 > 0 {
		data = dump.Uint16(data, status)
		data = dump.Uint16(data, warnCnt)
	}
	if enclen > 0 {
		// although MySQL manual says the info message is string<EOF>(https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html),
		// it is actually string<lenenc>
		data = dump.LengthEncodedString(data, []byte(msg))
	}

	err := cc.writePacket(data)
	if err != nil {
		return err
	}

	if flush {
		return cc.flush(ctx)
	}

	return nil
}

func (cc *clientConn) writeError(ctx context.Context, e error) error {
	var (
		m  *mysql.SQLError
		te *terror.Error
		ok bool
	)
	originErr := errors.Cause(e)
	if te, ok = originErr.(*terror.Error); ok {
		m = terror.ToSQLError(te)
	} else {
		e := errors.Cause(originErr)
		switch y := e.(type) {
		case *terror.Error:
			m = terror.ToSQLError(y)
		default:
			m = mysql.NewErrf(mysql.ErrUnknown, "%s", nil, e.Error())
		}
	}

	cc.lastCode = m.Code
	defer errno.IncrementError(m.Code, cc.user, cc.peerHost)
	data := cc.alloc.AllocWithLen(4, 16+len(m.Message))
	data = append(data, mysql.ErrHeader)
	data = append(data, byte(m.Code), byte(m.Code>>8))
	if cc.capability&mysql.ClientProtocol41 > 0 {
		data = append(data, '#')
		data = append(data, m.State...)
	}

	data = append(data, m.Message...)

	err := cc.writePacket(data)
	if err != nil {
		return err
	}
	return cc.flush(ctx)
}

// writeEOF writes an EOF packet or if ClientDeprecateEOF is set it
// writes an OK packet with EOF indicator.
// Note this function won't flush the stream because maybe there are more
// packets following it.
// serverStatus, a flag bit represents server information in the packet.
// Note: it is callers' responsibility to ensure correctness of serverStatus.
func (cc *clientConn) writeEOF(ctx context.Context, serverStatus uint16) error {
	if cc.capability&mysql.ClientDeprecateEOF > 0 {
		return cc.writeOkWith(ctx, mysql.EOFHeader, false, serverStatus)
	}

	data := cc.alloc.AllocWithLen(4, 9)

	data = append(data, mysql.EOFHeader)
	if cc.capability&mysql.ClientProtocol41 > 0 {
		data = dump.Uint16(data, cc.ctx.WarningCount())
		data = dump.Uint16(data, serverStatus)
	}

	err := cc.writePacket(data)
	return err
}

func (cc *clientConn) writeReq(ctx context.Context, filePath string) error {
	data := cc.alloc.AllocWithLen(4, 5+len(filePath))
	data = append(data, mysql.LocalInFileHeader)
	data = append(data, filePath...)

	err := cc.writePacket(data)
	if err != nil {
		return err
	}

	return cc.flush(ctx)
}

// getDataFromPath gets file contents from file path.
func (cc *clientConn) getDataFromPath(ctx context.Context, path string) ([]byte, error) {
	err := cc.writeReq(ctx, path)
	if err != nil {
		return nil, err
	}
	var prevData, curData []byte
	for {
		curData, err = cc.readPacket()
		if err != nil && terror.ErrorNotEqual(err, io.EOF) {
			return nil, err
		}
		if len(curData) == 0 {
			break
		}
		prevData = append(prevData, curData...)
	}
	return prevData, nil
}

// handleLoadStats does the additional work after processing the 'load stats' query.
// It sends client a file path, then reads the file content from client, loads it into the storage.
func (cc *clientConn) handleLoadStats(ctx context.Context, loadStatsInfo *executor.LoadStatsInfo) error {
	// If the server handles the load data request, the client has to set the ClientLocalFiles capability.
	if cc.capability&mysql.ClientLocalFiles == 0 {
		return servererr.ErrNotAllowedCommand
	}
	if loadStatsInfo == nil {
		return errors.New("load stats: info is empty")
	}
	data, err := cc.getDataFromPath(ctx, loadStatsInfo.Path)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	return loadStatsInfo.Update(data)
}

// handleIndexAdvise does the index advise work and returns the advise result for index.
func (cc *clientConn) handleIndexAdvise(ctx context.Context, indexAdviseInfo *executor.IndexAdviseInfo) error {
	if cc.capability&mysql.ClientLocalFiles == 0 {
		return servererr.ErrNotAllowedCommand
	}
	if indexAdviseInfo == nil {
		return errors.New("Index Advise: info is empty")
	}

	data, err := cc.getDataFromPath(ctx, indexAdviseInfo.Path)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return errors.New("Index Advise: infile is empty")
	}

	if err := indexAdviseInfo.GetIndexAdvice(data); err != nil {
		return err
	}

	// TODO: Write the rss []ResultSet. It will be done in another PR.
	return nil
}

func (cc *clientConn) handlePlanReplayerLoad(ctx context.Context, planReplayerLoadInfo *executor.PlanReplayerLoadInfo) error {
	if cc.capability&mysql.ClientLocalFiles == 0 {
		return servererr.ErrNotAllowedCommand
	}
	if planReplayerLoadInfo == nil {
		return errors.New("plan replayer load: info is empty")
	}
	data, err := cc.getDataFromPath(ctx, planReplayerLoadInfo.Path)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	return planReplayerLoadInfo.Update(data)
}

func (cc *clientConn) handlePlanReplayerDump(ctx context.Context, e *executor.PlanReplayerDumpInfo) error {
	if cc.capability&mysql.ClientLocalFiles == 0 {
		return servererr.ErrNotAllowedCommand
	}
	if e == nil {
		return errors.New("plan replayer dump: executor is empty")
	}
	data, err := cc.getDataFromPath(ctx, e.Path)
	if err != nil {
		logutil.BgLogger().Error(err.Error())
		return err
	}
	if len(data) == 0 {
		return nil
	}
	return e.DumpSQLsFromFile(ctx, data)
}

func (cc *clientConn) audit(eventType plugin.GeneralEvent) {
	err := plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
		audit := plugin.DeclareAuditManifest(p.Manifest)
		if audit.OnGeneralEvent != nil {
			cmd := mysql.Command2Str[byte(atomic.LoadUint32(&cc.ctx.GetSessionVars().CommandValue))]
			ctx := context.WithValue(context.Background(), plugin.ExecStartTimeCtxKey, cc.ctx.GetSessionVars().StartTime)
			audit.OnGeneralEvent(ctx, cc.ctx.GetSessionVars(), eventType, cmd)
		}
		return nil
	})
	if err != nil {
		terror.Log(err)
	}
}

// handleQuery executes the sql query string and writes result set or result ok to the client.
// As the execution time of this function represents the performance of TiDB, we do time log and metrics here.
// Some special queries like `load data` that does not return result, which is handled in handleFileTransInConn.
func (cc *clientConn) handleQuery(ctx context.Context, sql string) (err error) {
	defer trace.StartRegion(ctx, "handleQuery").End()
	sessVars := cc.ctx.GetSessionVars()
	sc := sessVars.StmtCtx
	prevWarns := sc.GetWarnings()
	var stmts []ast.StmtNode
	cc.ctx.GetSessionVars().SetAlloc(cc.chunkAlloc)
	if stmts, err = cc.ctx.Parse(ctx, sql); err != nil {
		cc.onExtensionSQLParseFailed(sql, err)
		return err
	}

	if len(stmts) == 0 {
		return cc.writeOK(ctx)
	}

	warns := sc.GetWarnings()
	parserWarns := warns[len(prevWarns):]

	var pointPlans []base.Plan
	cc.ctx.GetSessionVars().InMultiStmts = false
	if len(stmts) > 1 {
		// The client gets to choose if it allows multi-statements, and
		// probably defaults OFF. This helps prevent against SQL injection attacks
		// by early terminating the first statement, and then running an entirely
		// new statement.

		capabilities := cc.ctx.GetSessionVars().ClientCapability
		if capabilities&mysql.ClientMultiStatements < 1 {
			// The client does not have multi-statement enabled. We now need to determine
			// how to handle an unsafe situation based on the multiStmt sysvar.
			switch cc.ctx.GetSessionVars().MultiStatementMode {
			case variable.OffInt:
				err = servererr.ErrMultiStatementDisabled
				return err
			case variable.OnInt:
				// multi statement is fully permitted, do nothing
			default:
				warn := contextutil.SQLWarn{Level: contextutil.WarnLevelWarning, Err: servererr.ErrMultiStatementDisabled}
				parserWarns = append(parserWarns, warn)
			}
		}
		cc.ctx.GetSessionVars().InMultiStmts = true

		// Only pre-build point plans for multi-statement query
		pointPlans, err = cc.prefetchPointPlanKeys(ctx, stmts, sql)
		if err != nil {
			for _, stmt := range stmts {
				cc.onExtensionStmtEnd(stmt, false, err)
			}
			return err
		}
		metrics.NumOfMultiQueryHistogram.Observe(float64(len(stmts)))
	}
	if len(pointPlans) > 0 {
		defer cc.ctx.ClearValue(plannercore.PointPlanKey)
	}
	var retryable bool
	var lastStmt ast.StmtNode
	var expiredStmtTaskID uint64
	for i, stmt := range stmts {
		if lastStmt != nil {
			cc.onExtensionStmtEnd(lastStmt, true, nil)
		}
		lastStmt = stmt

		// expiredTaskID is the task ID of the previous statement. When executing a stmt,
		// the StmtCtx will be reinit and the TaskID will change. We can compare the StmtCtx.TaskID
		// with the previous one to determine whether StmtCtx has been inited for the current stmt.
		expiredStmtTaskID = sessVars.StmtCtx.TaskID

		if len(pointPlans) > 0 {
			// Save the point plan in Session, so we don't need to build the point plan again.
			cc.ctx.SetValue(plannercore.PointPlanKey, plannercore.PointPlanVal{Plan: pointPlans[i]})
		}
		retryable, err = cc.handleStmt(ctx, stmt, parserWarns, i == len(stmts)-1)
		if err != nil {
			action, txnErr := sessiontxn.GetTxnManager(&cc.ctx).OnStmtErrorForNextAction(ctx, sessiontxn.StmtErrAfterQuery, err)
			if txnErr != nil {
				err = txnErr
				break
			}

			if retryable && action == sessiontxn.StmtActionRetryReady {
				cc.ctx.GetSessionVars().RetryInfo.Retrying = true
				_, err = cc.handleStmt(ctx, stmt, parserWarns, i == len(stmts)-1)
				cc.ctx.GetSessionVars().RetryInfo.Retrying = false
				if err != nil {
					break
				}
				continue
			}
			if !retryable || !errors.ErrorEqual(err, storeerr.ErrTiFlashServerTimeout) {
				break
			}
			_, allowTiFlashFallback := cc.ctx.GetSessionVars().AllowFallbackToTiKV[kv.TiFlash]
			if !allowTiFlashFallback {
				break
			}
			// When the TiFlash server seems down, we append a warning to remind the user to check the status of the TiFlash
			// server and fallback to TiKV.
			warns := append(parserWarns, contextutil.SQLWarn{Level: contextutil.WarnLevelError, Err: err})
			delete(cc.ctx.GetSessionVars().IsolationReadEngines, kv.TiFlash)
			_, err = cc.handleStmt(ctx, stmt, warns, i == len(stmts)-1)
			cc.ctx.GetSessionVars().IsolationReadEngines[kv.TiFlash] = struct{}{}
			if err != nil {
				break
			}
		}
	}

	if lastStmt != nil {
		cc.onExtensionStmtEnd(lastStmt, sessVars.StmtCtx.TaskID != expiredStmtTaskID, err)
	}

	return err
}

// prefetchPointPlanKeys extracts the point keys in multi-statement query,
// use BatchGet to get the keys, so the values will be cached in the snapshot cache, save RPC call cost.
// For pessimistic transaction, the keys will be batch locked.
func (cc *clientConn) prefetchPointPlanKeys(ctx context.Context, stmts []ast.StmtNode, sqls string) ([]base.Plan, error) {
	txn, err := cc.ctx.Txn(false)
	if err != nil {
		return nil, err
	}
	if !txn.Valid() {
		// Only prefetch in-transaction query for simplicity.
		// Later we can support out-transaction multi-statement query.
		return nil, nil
	}
	vars := cc.ctx.GetSessionVars()
	if vars.TxnCtx.IsPessimistic {
		if vars.IsIsolation(ast.ReadCommitted) {
			// TODO: to support READ-COMMITTED, we need to avoid getting new TS for each statement in the query.
			return nil, nil
		}
		if vars.TxnCtx.GetForUpdateTS() != vars.TxnCtx.StartTS {
			// Do not handle the case that ForUpdateTS is changed for simplicity.
			return nil, nil
		}
	}
	pointPlans := make([]base.Plan, len(stmts))
	var idxKeys []kv.Key //nolint: prealloc
	var rowKeys []kv.Key //nolint: prealloc
	isCommonHandle := make(map[string]bool, 0)

	handlePlan := func(sctx sessionctx.Context, p base.PhysicalPlan, resetStmtCtxFn func()) error {
		var tableID int64
		switch v := p.(type) {
		case *plannercore.PointGetPlan:
			v.PrunePartitions(sctx)
			tableID = executor.GetPhysID(v.TblInfo, v.PartitionIdx)
			if v.IndexInfo != nil {
				resetStmtCtxFn()
				idxKey, err1 := plannercore.EncodeUniqueIndexKey(cc.getCtx(), v.TblInfo, v.IndexInfo, v.IndexValues, tableID)
				if err1 != nil {
					return err1
				}
				idxKeys = append(idxKeys, idxKey)
				isCommonHandle[string(hack.String(idxKey))] = v.TblInfo.IsCommonHandle
			} else {
				rowKeys = append(rowKeys, tablecodec.EncodeRowKeyWithHandle(tableID, v.Handle))
			}
		case *plannercore.BatchPointGetPlan:
			_, isTableDual := v.PrunePartitionsAndValues(sctx)
			if isTableDual {
				return nil
			}
			pi := v.TblInfo.GetPartitionInfo()
			getPhysID := func(i int) int64 {
				if pi == nil || i >= len(v.PartitionIdxs) {
					return v.TblInfo.ID
				}
				return executor.GetPhysID(v.TblInfo, &v.PartitionIdxs[i])
			}
			if v.IndexInfo != nil {
				resetStmtCtxFn()
				for i, idxVals := range v.IndexValues {
					idxKey, err1 := plannercore.EncodeUniqueIndexKey(cc.getCtx(), v.TblInfo, v.IndexInfo, idxVals, getPhysID(i))
					if err1 != nil {
						return err1
					}
					idxKeys = append(idxKeys, idxKey)
					isCommonHandle[string(hack.String(idxKey))] = v.TblInfo.IsCommonHandle
				}
			} else {
				for i, handle := range v.Handles {
					rowKeys = append(rowKeys, tablecodec.EncodeRowKeyWithHandle(getPhysID(i), handle))
				}
			}
		}
		return nil
	}

	sc := vars.StmtCtx
	for i, stmt := range stmts {
		if _, ok := stmt.(*ast.UseStmt); ok {
			// If there is a "use db" statement, we shouldn't cache even if it's possible.
			// Consider the scenario where there are statements that could execute on multiple
			// schemas, but the schema is actually different.
			return nil, nil
		}
		// TODO: the preprocess is run twice, we should find some way to avoid do it again.
		if err = plannercore.Preprocess(ctx, cc.getCtx(), stmt); err != nil {
			// error might happen, see https://github.com/pingcap/tidb/issues/39664
			return nil, nil
		}
		p := plannercore.TryFastPlan(cc.ctx.Session.GetPlanCtx(), stmt)
		pointPlans[i] = p
		if p == nil {
			continue
		}
		// Only support Update and Delete for now.
		// TODO: support other point plans.
		switch x := p.(type) {
		case *plannercore.Update:
			//nolint:forcetypeassert
			updateStmt, ok := stmt.(*ast.UpdateStmt)
			if !ok {
				logutil.BgLogger().Warn("unexpected statement type for Update plan",
					zap.String("type", fmt.Sprintf("%T", stmt)))
				continue
			}
			err = handlePlan(cc.ctx.Session, x.SelectPlan, func() {
				executor.ResetUpdateStmtCtx(sc, updateStmt, vars)
			})
			if err != nil {
				return nil, err
			}
		case *plannercore.Delete:
			deleteStmt, ok := stmt.(*ast.DeleteStmt)
			if !ok {
				logutil.BgLogger().Warn("unexpected statement type for Delete plan",
					zap.String("type", fmt.Sprintf("%T", stmt)))
				continue
			}
			err = handlePlan(cc.ctx.Session, x.SelectPlan, func() {
				executor.ResetDeleteStmtCtx(sc, deleteStmt, vars)
			})
			if err != nil {
				return nil, err
			}
		}
	}
	if len(idxKeys) == 0 && len(rowKeys) == 0 {
		return pointPlans, nil
	}
	snapshot := txn.GetSnapshot()
	setResourceGroupTaggerForMultiStmtPrefetch(snapshot, sqls)
	idxVals, err1 := snapshot.BatchGet(ctx, idxKeys)
	if err1 != nil {
		return nil, err1
	}
	for idxKey, idxVal := range idxVals {
		h, err2 := tablecodec.DecodeHandleInIndexValue(idxVal)
		if err2 != nil {
			return nil, err2
		}
		tblID := tablecodec.DecodeTableID(hack.Slice(idxKey))
		rowKeys = append(rowKeys, tablecodec.EncodeRowKeyWithHandle(tblID, h))
	}
	if vars.TxnCtx.IsPessimistic {
		allKeys := append(rowKeys, idxKeys...)
		err = executor.LockKeys(ctx, cc.getCtx(), vars.LockWaitTimeout, allKeys...)
		if err != nil {
			// suppress the lock error, we are not going to handle it here for simplicity.
			err = nil
			logutil.BgLogger().Warn("lock keys error on prefetch", zap.Error(err))
		}
	} else {
		_, err = snapshot.BatchGet(ctx, rowKeys)
		if err != nil {
			return nil, err
		}
	}
	return pointPlans, nil
}

func setResourceGroupTaggerForMultiStmtPrefetch(snapshot kv.Snapshot, sqls string) {
	if !topsqlstate.TopSQLEnabled() {
		return
	}
	normalized, digest := parser.NormalizeDigest(sqls)
	topsql.AttachAndRegisterSQLInfo(context.Background(), normalized, digest, false)
	snapshot.SetOption(kv.ResourceGroupTagger, tikvrpc.ResourceGroupTagger(func(req *tikvrpc.Request) {
		if req == nil {
			return
		}
		if len(normalized) == 0 {
			return
		}
		req.ResourceGroupTag = resourcegrouptag.EncodeResourceGroupTag(digest, nil,
			resourcegrouptag.GetResourceGroupLabelByKey(resourcegrouptag.GetFirstKeyFromRequest(req)))
	}))
}

// The first return value indicates whether the call of handleStmt has no side effect and can be retried.
// Currently, the first return value is used to fall back to TiKV when TiFlash is down.
func (cc *clientConn) handleStmt(
	ctx context.Context, stmt ast.StmtNode,
	warns []contextutil.SQLWarn, lastStmt bool,
) (bool, error) {
	ctx = context.WithValue(ctx, execdetails.StmtExecDetailKey, &execdetails.StmtExecDetails{})
	ctx = context.WithValue(ctx, util.ExecDetailsKey, &util.ExecDetails{})
	ctx = context.WithValue(ctx, util.RUDetailsCtxKey, util.NewRUDetails())
	reg := trace.StartRegion(ctx, "ExecuteStmt")
	cc.audit(plugin.Starting)

	// if stmt is load data stmt, store the channel that reads from the conn
	// into the ctx for executor to use
	if s, ok := stmt.(*ast.LoadDataStmt); ok {
		if s.FileLocRef == ast.FileLocClient {
			err := cc.preprocessLoadDataLocal(ctx)
			defer cc.postprocessLoadDataLocal()
			if err != nil {
				return false, err
			}
		}
	}

	rs, err := cc.ctx.ExecuteStmt(ctx, stmt)
	reg.End()
	// - If rs is not nil, the statement tracker detachment from session tracker
	//   is done in the `rs.Close` in most cases.
	// - If the rs is nil and err is not nil, the detachment will be done in
	//   the `handleNoDelay`.
	if rs != nil {
		defer rs.Close()
	}

	if err != nil {
		// If error is returned during the planner phase or the executor.Open
		// phase, the rs will be nil, and StmtCtx.MemTracker StmtCtx.DiskTracker
		// will not be detached. We need to detach them manually.
		if sv := cc.ctx.GetSessionVars(); sv != nil && sv.StmtCtx != nil {
			sv.StmtCtx.DetachMemDiskTracker()
		}
		return true, err
	}

	status := cc.ctx.Status()
	if lastStmt {
		cc.ctx.GetSessionVars().StmtCtx.AppendWarnings(warns)
	} else {
		status |= mysql.ServerMoreResultsExists
	}

	if rs != nil {
		if cc.getStatus() == connStatusShutdown {
			return false, exeerrors.ErrQueryInterrupted
		}
		if retryable, err := cc.writeResultSet(ctx, rs, false, status, 0); err != nil {
			return retryable, err
		}
		return false, nil
	}

	handled, err := cc.handleFileTransInConn(ctx, status)
	if handled {
		if execStmt := cc.ctx.Value(session.ExecStmtVarKey); execStmt != nil {
			//nolint:forcetypeassert
			execStmt.(*executor.ExecStmt).FinishExecuteStmt(0, err, false)
		}
	}
	return false, err
}

// Preprocess LOAD DATA. Load data from a local file requires reading from the connection.
// The function pass a builder to build the connection reader to the context,
// which will be used in LoadDataExec.
func (cc *clientConn) preprocessLoadDataLocal(ctx context.Context) error {
	if cc.capability&mysql.ClientLocalFiles == 0 {
		return servererr.ErrNotAllowedCommand
	}

	var readerBuilder executor.LoadDataReaderBuilder = func(filepath string) (
		io.ReadCloser, error,
	) {
		err := cc.writeReq(ctx, filepath)
		if err != nil {
			return nil, err
		}

		drained := false
		r, w := io.Pipe()

		go func() {
			var errOccurred error

			defer func() {
				if errOccurred != nil {
					// Continue reading packets to drain the connection
					for !drained {
						data, err := cc.readPacket()
						if err != nil {
							logutil.Logger(ctx).Error(
								"drain connection failed in load data",
								zap.Error(err),
							)
							break
						}
						if len(data) == 0 {
							drained = true
						}
					}
				}
				err := w.CloseWithError(errOccurred)
				if err != nil {
					logutil.Logger(ctx).Error(
						"close pipe failed in `load data`",
						zap.Error(err),
					)
				}
			}()

			for {
				data, err := cc.readPacket()
				if err != nil {
					errOccurred = err
					return
				}

				if len(data) == 0 {
					drained = true
					return
				}

				// Write all content in `data`
				for len(data) > 0 {
					n, err := w.Write(data)
					if err != nil {
						errOccurred = err
						return
					}
					data = data[n:]
				}
			}
		}()

		return r, nil
	}

	cc.ctx.SetValue(executor.LoadDataReaderBuilderKey, readerBuilder)

	return nil
}

func (cc *clientConn) postprocessLoadDataLocal() {
	cc.ctx.ClearValue(executor.LoadDataReaderBuilderKey)
}

func (cc *clientConn) handleFileTransInConn(ctx context.Context, status uint16) (bool, error) {
	handled := false

	loadStats := cc.ctx.Value(executor.LoadStatsVarKey)
	if loadStats != nil {
		handled = true
		defer cc.ctx.SetValue(executor.LoadStatsVarKey, nil)
		//nolint:forcetypeassert
		if err := cc.handleLoadStats(ctx, loadStats.(*executor.LoadStatsInfo)); err != nil {
			return handled, err
		}
	}

	indexAdvise := cc.ctx.Value(executor.IndexAdviseVarKey)
	if indexAdvise != nil {
		handled = true
		defer cc.ctx.SetValue(executor.IndexAdviseVarKey, nil)
		//nolint:forcetypeassert
		if err := cc.handleIndexAdvise(ctx, indexAdvise.(*executor.IndexAdviseInfo)); err != nil {
			return handled, err
		}
	}

	planReplayerLoad := cc.ctx.Value(executor.PlanReplayerLoadVarKey)
	if planReplayerLoad != nil {
		handled = true
		defer cc.ctx.SetValue(executor.PlanReplayerLoadVarKey, nil)
		//nolint:forcetypeassert
		if err := cc.handlePlanReplayerLoad(ctx, planReplayerLoad.(*executor.PlanReplayerLoadInfo)); err != nil {
			return handled, err
		}
	}

	planReplayerDump := cc.ctx.Value(executor.PlanReplayerDumpVarKey)
	if planReplayerDump != nil {
		handled = true
		defer cc.ctx.SetValue(executor.PlanReplayerDumpVarKey, nil)
		//nolint:forcetypeassert
		if err := cc.handlePlanReplayerDump(ctx, planReplayerDump.(*executor.PlanReplayerDumpInfo)); err != nil {
			return handled, err
		}
	}
	return handled, cc.writeOkWith(ctx, mysql.OKHeader, true, status)
}

// handleFieldList returns the field list for a table.
// The sql string is composed of a table name and a terminating character \x00.
func (cc *clientConn) handleFieldList(ctx context.Context, sql string) (err error) {
	parts := strings.Split(sql, "\x00")
	columns, err := cc.ctx.FieldList(parts[0])
	if err != nil {
		return err
	}
	data := cc.alloc.AllocWithLen(4, 1024)
	cc.initResultEncoder(ctx)
	defer cc.rsEncoder.Clean()
	for _, column := range columns {
		data = data[0:4]
		data = column.DumpWithDefault(data, cc.rsEncoder)
		if err := cc.writePacket(data); err != nil {
			return err
		}
	}
	if err := cc.writeEOF(ctx, cc.ctx.Status()); err != nil {
		return err
	}
	return cc.flush(ctx)
}

// writeResultSet writes data into a result set and uses rs.Next to get row data back.
// If binary is true, the data would be encoded in BINARY format.
// serverStatus, a flag bit represents server information.
// fetchSize, the desired number of rows to be fetched each time when client uses cursor.
// retryable indicates whether the call of writeResultSet has no side effect and can be retried to correct error. The call
// has side effect in cursor mode or once data has been sent to client. Currently retryable is used to fallback to TiKV when
// TiFlash is down.
func (cc *clientConn) writeResultSet(ctx context.Context, rs resultset.ResultSet, binary bool, serverStatus uint16, fetchSize int) (retryable bool, runErr error) {
	defer func() {
		// close ResultSet when cursor doesn't exist
		r := recover()
		if r == nil {
			return
		}
		recoverdErr, ok := r.(error)
		if !ok || !(exeerrors.ErrMemoryExceedForQuery.Equal(recoverdErr) ||
			exeerrors.ErrMemoryExceedForInstance.Equal(recoverdErr) ||
			exeerrors.ErrQueryInterrupted.Equal(recoverdErr) ||
			exeerrors.ErrMaxExecTimeExceeded.Equal(recoverdErr)) {
			panic(r)
		}
		runErr = recoverdErr
		// TODO(jianzhang.zj: add metrics here)
		logutil.Logger(ctx).Error("write query result panic", zap.Stringer("lastSQL", getLastStmtInConn{cc}), zap.Stack("stack"), zap.Any("recover", r))
	}()
	cc.initResultEncoder(ctx)
	defer cc.rsEncoder.Clean()
	if mysql.HasCursorExistsFlag(serverStatus) {
		crs, ok := rs.(resultset.CursorResultSet)
		if !ok {
			// this branch is actually unreachable
			return false, errors.New("this cursor is not a resultSet")
		}
		if err := cc.writeChunksWithFetchSize(ctx, crs, serverStatus, fetchSize); err != nil {
			return false, err
		}
		return false, cc.flush(ctx)
	}
	if retryable, err := cc.writeChunks(ctx, rs, binary, serverStatus); err != nil {
		return retryable, err
	}

	return false, cc.flush(ctx)
}

func (cc *clientConn) writeColumnInfo(columns []*column.Info) error {
	data := cc.alloc.AllocWithLen(4, 1024)
	data = dump.LengthEncodedInt(data, uint64(len(columns)))
	if err := cc.writePacket(data); err != nil {
		return err
	}
	for _, v := range columns {
		data = data[0:4]
		data = v.Dump(data, cc.rsEncoder)
		if err := cc.writePacket(data); err != nil {
			return err
		}
	}
	return nil
}

// writeChunks writes data from a Chunk, which filled data by a ResultSet, into a connection.
// binary specifies the way to dump data. It throws any error while dumping data.
// serverStatus, a flag bit represents server information
// The first return value indicates whether error occurs at the first call of ResultSet.Next.
func (cc *clientConn) writeChunks(ctx context.Context, rs resultset.ResultSet, binary bool, serverStatus uint16) (bool, error) {
	data := cc.alloc.AllocWithLen(4, 1024)
	req := rs.NewChunk(cc.chunkAlloc)
	gotColumnInfo := false
	firstNext := true
	validNextCount := 0
	var start time.Time
	var stmtDetail *execdetails.StmtExecDetails
	stmtDetailRaw := ctx.Value(execdetails.StmtExecDetailKey)
	if stmtDetailRaw != nil {
		//nolint:forcetypeassert
		stmtDetail = stmtDetailRaw.(*execdetails.StmtExecDetails)
	}
	for {
		failpoint.Inject("fetchNextErr", func(value failpoint.Value) {
			//nolint:forcetypeassert
			switch value.(string) {
			case "firstNext":
				failpoint.Return(firstNext, storeerr.ErrTiFlashServerTimeout)
			case "secondNext":
				if !firstNext {
					failpoint.Return(firstNext, storeerr.ErrTiFlashServerTimeout)
				}
			case "secondNextAndRetConflict":
				if !firstNext && validNextCount > 1 {
					failpoint.Return(firstNext, kv.ErrWriteConflict)
				}
			}
		})
		// Here server.tidbResultSet implements Next method.
		err := rs.Next(ctx, req)
		if err != nil {
			return firstNext, err
		}
		if !gotColumnInfo {
			// We need to call Next before we get columns.
			// Otherwise, we will get incorrect columns info.
			columns := rs.Columns()
			if stmtDetail != nil {
				start = time.Now()
			}
			if err = cc.writeColumnInfo(columns); err != nil {
				return false, err
			}
			if cc.capability&mysql.ClientDeprecateEOF == 0 {
				// metadata only needs EOF marker for old clients without ClientDeprecateEOF
				if err = cc.writeEOF(ctx, serverStatus); err != nil {
					return false, err
				}
			}
			if stmtDetail != nil {
				stmtDetail.WriteSQLRespDuration += time.Since(start)
			}
			gotColumnInfo = true
		}
		rowCount := req.NumRows()
		if rowCount == 0 {
			break
		}
		validNextCount++
		firstNext = false
		reg := trace.StartRegion(ctx, "WriteClientConn")
		if stmtDetail != nil {
			start = time.Now()
		}
		for i := 0; i < rowCount; i++ {
			data = data[0:4]
			if binary {
				data, err = column.DumpBinaryRow(data, rs.Columns(), req.GetRow(i), cc.rsEncoder)
			} else {
				data, err = column.DumpTextRow(data, rs.Columns(), req.GetRow(i), cc.rsEncoder)
			}
			if err != nil {
				reg.End()
				return false, err
			}
			if err = cc.writePacket(data); err != nil {
				reg.End()
				return false, err
			}
		}
		reg.End()
		if stmtDetail != nil {
			stmtDetail.WriteSQLRespDuration += time.Since(start)
		}
	}
	if err := rs.Finish(); err != nil {
		return false, err
	}

	if stmtDetail != nil {
		start = time.Now()
	}

	err := cc.writeEOF(ctx, serverStatus)
	if stmtDetail != nil {
		stmtDetail.WriteSQLRespDuration += time.Since(start)
	}
	return false, err
}

// writeChunksWithFetchSize writes data from a Chunk, which filled data by a ResultSet, into a connection.
// binary specifies the way to dump data. It throws any error while dumping data.
// serverStatus, a flag bit represents server information.
// fetchSize, the desired number of rows to be fetched each time when client uses cursor.
func (cc *clientConn) writeChunksWithFetchSize(ctx context.Context, rs resultset.CursorResultSet, serverStatus uint16, fetchSize int) error {
	var (
		stmtDetail *execdetails.StmtExecDetails
		err        error
		start      time.Time
	)
	data := cc.alloc.AllocWithLen(4, 1024)
	stmtDetailRaw := ctx.Value(execdetails.StmtExecDetailKey)
	if stmtDetailRaw != nil {
		//nolint:forcetypeassert
		stmtDetail = stmtDetailRaw.(*execdetails.StmtExecDetails)
	}
	if stmtDetail != nil {
		start = time.Now()
	}

	iter := rs.GetRowContainerReader()
	// send the rows to the client according to fetchSize.
	for i := 0; i < fetchSize && iter.Current() != iter.End(); i++ {
		row := iter.Current()

		data = data[0:4]
		data, err = column.DumpBinaryRow(data, rs.Columns(), row, cc.rsEncoder)
		if err != nil {
			return err
		}
		if err = cc.writePacket(data); err != nil {
			return err
		}

		iter.Next()
	}
	if iter.Error() != nil {
		return iter.Error()
	}

	// tell the client COM_STMT_FETCH has finished by setting proper serverStatus,
	// and close ResultSet.
	if iter.Current() == iter.End() {
		serverStatus &^= mysql.ServerStatusCursorExists
		serverStatus |= mysql.ServerStatusLastRowSend
	}

	// don't include the time consumed by `cl.OnFetchReturned()` in the `WriteSQLRespDuration`
	if stmtDetail != nil {
		stmtDetail.WriteSQLRespDuration += time.Since(start)
	}

	if cl, ok := rs.(resultset.FetchNotifier); ok {
		cl.OnFetchReturned()
	}

	start = time.Now()
	err = cc.writeEOF(ctx, serverStatus)
	if stmtDetail != nil {
		stmtDetail.WriteSQLRespDuration += time.Since(start)
	}
	return err
}

func (cc *clientConn) setConn(conn net.Conn) {
	cc.bufReadConn = util2.NewBufferedReadConn(conn)
	if cc.pkt == nil {
		cc.pkt = internal.NewPacketIO(cc.bufReadConn)
	} else {
		// Preserve current sequence number.
		cc.pkt.SetBufferedReadConn(cc.bufReadConn)
	}
}

func (cc *clientConn) upgradeToTLS(tlsConfig *tls.Config) error {
	// Important: read from buffered reader instead of the original net.Conn because it may contain data we need.
	tlsConn := tls.Server(cc.bufReadConn, tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		return err
	}
	cc.setConn(tlsConn)
	cc.tlsConn = tlsConn
	return nil
}

func (cc *clientConn) handleChangeUser(ctx context.Context, data []byte) error {
	user, data := util2.ParseNullTermString(data)
	cc.user = string(hack.String(user))
	if len(data) < 1 {
		return mysql.ErrMalformPacket
	}
	passLen := int(data[0])
	data = data[1:]
	if passLen > len(data) {
		return mysql.ErrMalformPacket
	}
	pass := data[:passLen]
	data = data[passLen:]
	dbName, data := util2.ParseNullTermString(data)
	cc.dbname = string(hack.String(dbName))
	pluginName := ""
	if len(data) > 0 {
		// skip character set
		if cc.capability&mysql.ClientProtocol41 > 0 && len(data) >= 2 {
			data = data[2:]
		}
		if cc.capability&mysql.ClientPluginAuth > 0 && len(data) > 0 {
			pluginNameB, _ := util2.ParseNullTermString(data)
			pluginName = string(hack.String(pluginNameB))
		}
	}

	if err := cc.ctx.Close(); err != nil {
		logutil.Logger(ctx).Debug("close old context failed", zap.Error(err))
	}
	// session was closed by `ctx.Close` and should `openSession` explicitly to renew session.
	// `openSession` won't run again in `openSessionAndDoAuth` because ctx is not nil.
	err := cc.openSession()
	if err != nil {
		return err
	}
	fakeResp := &handshake.Response41{
		Auth:       pass,
		AuthPlugin: pluginName,
		Capability: cc.capability,
	}
	if fakeResp.AuthPlugin != "" {
		failpoint.Inject("ChangeUserAuthSwitch", func(val failpoint.Value) {
			failpoint.Return(errors.Errorf("%v", val))
		})
		newpass, err := cc.checkAuthPlugin(ctx, fakeResp)
		if err != nil {
			return err
		}
		if len(newpass) > 0 {
			fakeResp.Auth = newpass
		}
	}
	if err := cc.openSessionAndDoAuth(fakeResp.Auth, fakeResp.AuthPlugin, fakeResp.ZstdLevel); err != nil {
		return err
	}
	return cc.handleCommonConnectionReset(ctx)
}

func (cc *clientConn) handleResetConnection(ctx context.Context) error {
	user := cc.ctx.GetSessionVars().User
	err := cc.ctx.Close()
	if err != nil {
		logutil.Logger(ctx).Debug("close old context failed", zap.Error(err))
	}
	var tlsStatePtr *tls.ConnectionState
	if cc.tlsConn != nil {
		tlsState := cc.tlsConn.ConnectionState()
		tlsStatePtr = &tlsState
	}
	tidbCtx, err := cc.server.driver.OpenCtx(cc.connectionID, cc.capability, cc.collation, cc.dbname, tlsStatePtr, cc.extensions)
	if err != nil {
		return err
	}
	cc.SetCtx(tidbCtx)
	if !cc.ctx.AuthWithoutVerification(user) {
		return errors.New("Could not reset connection")
	}
	if cc.dbname != "" { // Restore the current DB
		_, err = cc.useDB(context.Background(), cc.dbname)
		if err != nil {
			return err
		}
	}
	cc.ctx.SetSessionManager(cc.server)

	return cc.handleCommonConnectionReset(ctx)
}

func (cc *clientConn) handleCommonConnectionReset(ctx context.Context) error {
	connectionInfo := cc.connectInfo()
	cc.ctx.GetSessionVars().ConnectionInfo = connectionInfo

	cc.onExtensionConnEvent(extension.ConnReset, nil)
	err := plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
		authPlugin := plugin.DeclareAuditManifest(p.Manifest)
		if authPlugin.OnConnectionEvent != nil {
			connInfo := cc.ctx.GetSessionVars().ConnectionInfo
			err := authPlugin.OnConnectionEvent(context.Background(), plugin.ChangeUser, connInfo)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return cc.writeOK(ctx)
}

// safe to noop except 0x01 "FLUSH PRIVILEGES"
func (cc *clientConn) handleRefresh(ctx context.Context, subCommand byte) error {
	if subCommand == 0x01 {
		if err := cc.handleQuery(ctx, "FLUSH PRIVILEGES"); err != nil {
			return err
		}
	}
	return cc.writeOK(ctx)
}

var _ fmt.Stringer = getLastStmtInConn{}

type getLastStmtInConn struct {
	*clientConn
}

func (cc getLastStmtInConn) String() string {
	if len(cc.lastPacket) == 0 {
		return ""
	}
	cmd, data := cc.lastPacket[0], cc.lastPacket[1:]
	switch cmd {
	case mysql.ComInitDB:
		return "Use " + string(data)
	case mysql.ComFieldList:
		return "ListFields " + string(data)
	case mysql.ComQuery, mysql.ComStmtPrepare:
		sql := string(hack.String(data))
		sql = parser.Normalize(sql, cc.ctx.GetSessionVars().EnableRedactLog)
		return executor.FormatSQL(sql).String()
	case mysql.ComStmtExecute, mysql.ComStmtFetch:
		stmtID := binary.LittleEndian.Uint32(data[0:4])
		return executor.FormatSQL(cc.preparedStmt2String(stmtID)).String()
	case mysql.ComStmtClose, mysql.ComStmtReset:
		stmtID := binary.LittleEndian.Uint32(data[0:4])
		return mysql.Command2Str[cmd] + " " + strconv.Itoa(int(stmtID))
	default:
		if cmdStr, ok := mysql.Command2Str[cmd]; ok {
			return cmdStr
		}
		return string(hack.String(data))
	}
}

// PProfLabel return sql label used to tag pprof.
func (cc getLastStmtInConn) PProfLabel() string {
	if len(cc.lastPacket) == 0 {
		return ""
	}
	cmd, data := cc.lastPacket[0], cc.lastPacket[1:]
	switch cmd {
	case mysql.ComInitDB:
		return "UseDB"
	case mysql.ComFieldList:
		return "ListFields"
	case mysql.ComStmtClose:
		return "CloseStmt"
	case mysql.ComStmtReset:
		return "ResetStmt"
	case mysql.ComQuery, mysql.ComStmtPrepare:
		return parser.Normalize(executor.FormatSQL(string(hack.String(data))).String(), errors.RedactLogEnable)
	case mysql.ComStmtExecute, mysql.ComStmtFetch:
		stmtID := binary.LittleEndian.Uint32(data[0:4])
		return executor.FormatSQL(cc.preparedStmt2StringNoArgs(stmtID)).String()
	default:
		return ""
	}
}

var _ conn.AuthConn = &clientConn{}

// WriteAuthMoreData implements `conn.AuthConn` interface
func (cc *clientConn) WriteAuthMoreData(data []byte) error {
	// See https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_more_data.html
	// the `AuthMoreData` packet is just an arbitrary binary slice with a byte 0x1 as prefix.
	return cc.writePacket(append([]byte{0, 0, 0, 0, 1}, data...))
}

// ReadPacket implements `conn.AuthConn` interface
func (cc *clientConn) ReadPacket() ([]byte, error) {
	return cc.readPacket()
}

// Flush implements `conn.AuthConn` interface
func (cc *clientConn) Flush(ctx context.Context) error {
	return cc.flush(ctx)
}

type compressionStats struct{}

// Stats returns the connection statistics.
func (*compressionStats) Stats(vars *variable.SessionVars) (map[string]any, error) {
	m := make(map[string]any, 3)

	switch vars.CompressionAlgorithm {
	case mysql.CompressionNone:
		m[statusCompression] = "OFF"
		m[statusCompressionAlgorithm] = ""
		m[statusCompressionLevel] = 0
	case mysql.CompressionZlib:
		m[statusCompression] = "ON"
		m[statusCompressionAlgorithm] = "zlib"
		m[statusCompressionLevel] = mysql.ZlibCompressDefaultLevel
	case mysql.CompressionZstd:
		m[statusCompression] = "ON"
		m[statusCompressionAlgorithm] = "zstd"
		m[statusCompressionLevel] = vars.CompressionLevel
	default:
		logutil.BgLogger().Debug(
			"unexpected compression algorithm value",
			zap.Int("algorithm", vars.CompressionAlgorithm),
		)
		m[statusCompression] = "OFF"
		m[statusCompressionAlgorithm] = ""
		m[statusCompressionLevel] = 0
	}

	return m, nil
}

// GetScope gets the status variables scope.
func (*compressionStats) GetScope(_ string) variable.ScopeFlag {
	return variable.ScopeSession
}

func init() {
	variable.RegisterStatistics(&compressionStats{})
}
