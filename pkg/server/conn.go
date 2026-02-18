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
	"context"
	"crypto/tls"
	"encoding/binary"
	goerr "errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/plugin"
	"github.com/pingcap/tidb/pkg/privilege/conn"
	"github.com/pingcap/tidb/pkg/resourcegroup"
	servererr "github.com/pingcap/tidb/pkg/server/err"
	"github.com/pingcap/tidb/pkg/server/handler/tikvhandler"
	"github.com/pingcap/tidb/pkg/server/internal"
	"github.com/pingcap/tidb/pkg/server/internal/column"
	"github.com/pingcap/tidb/pkg/server/internal/dump"
	"github.com/pingcap/tidb/pkg/server/internal/handshake"
	util2 "github.com/pingcap/tidb/pkg/server/internal/util"
	server_metrics "github.com/pingcap/tidb/pkg/server/metrics"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/arena"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	tlsutil "github.com/pingcap/tidb/pkg/util/tls"
	"github.com/pingcap/tidb/pkg/util/topsql"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tidb/pkg/util/traceevent"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	connStatusDispatching int32 = iota
	connStatusReading
	connStatusShutdown     = variable.ConnStatusShutdown // Closed by server.
	connStatusWaitShutdown = 3                           // Notified by server to close.

	tidbGatewayAttrsConnKey = "TiDB-Gateway-ConnID"
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

type userResourceLimits struct {
	connections int
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
		util.WithRecovery(
			func() {
				if cc.getStatus() != connStatusShutdown {
					err := cc.Close()
					terror.Log(err)
				}
			}, nil)

		close(cc.quit)
	}()

	cc.addConnMetrics()

	var traceInfo *tracing.TraceInfo
	trace := traceevent.NewTrace()
	ctx = tracing.WithFlightRecorder(ctx, trace)

	// Usually, client connection status changes between [dispatching] <=> [reading].
	// When some event happens, server may notify this client connection by setting
	// the status to special values, for example: kill or graceful shutdown.
	// The client connection would detect the events when it fails to change status
	// by CAS operation, it would then take some actions accordingly.
	parentCtx := ctx
	for {
		sessVars := cc.ctx.GetSessionVars()
		if alias := sessVars.SessionAlias; traceInfo == nil || traceInfo.SessionAlias != alias {
			// We should reset the context trace info when traceInfo not inited or session alias changed.
			traceInfo = &tracing.TraceInfo{
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
						tidbGatewayConnID := cc.attrs[tidbGatewayAttrsConnKey]
						cc.server.SetNormalClosedConn(keyspace.GetKeyspaceNameBySettings(), tidbGatewayConnID, "read packet timeout")
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

		// It should be CAS before checking the `inShutdownMode` to avoid the following scenario:
		// 1. The connection checks the `inShutdownMode` and it's false.
		// 2. The server sets the `inShutdownMode` to true. The `DrainClients` process ignores this connection
		//   because the connection is in the `connStatusReading` status.
		// 3. The connection changes its status to `connStatusDispatching` and starts to execute the command.
		if !cc.CompareAndSwapStatus(connStatusReading, connStatusDispatching) {
			return
		}

		// Should check InTxn() to avoid execute `begin` stmt and allow executing statements in the not committed txn.
		if cc.server.inShutdownMode.Load() {
			if !cc.ctx.GetSessionVars().InTxn() {
				return
			}
		}

		startTime := time.Now()
		err = cc.dispatch(ctx, data)
		cc.ctx.GetSessionVars().ClearAlloc(&cc.chunkAlloc, err != nil)
		cc.chunkAlloc.Reset()
		trace.DiscardOrFlush(ctx)

		if err != nil {
			cc.audit(context.Background(), plugin.Error) // tell the plugin API there was a dispatch error
			if terror.ErrorEqual(err, io.EOF) {
				cc.addQueryMetrics(data[0], startTime, nil)
				server_metrics.DisconnectNormal.Inc()
				return
			} else if terror.ErrResultUndetermined.Equal(err) {
				logutil.Logger(ctx).Warn("result undetermined, close this connection", zap.Error(err))
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
				logutil.Logger(ctx).Warn("command dispatched failed",
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
		cc.addQueryMetrics(data[0], startTime, err)
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

// Per connection metrics
func (cc *clientConn) addConnMetrics() {
	if cc.tlsConn != nil {
		connState := cc.tlsConn.ConnectionState()
		metrics.TLSVersion.WithLabelValues(
			tlsutil.VersionName(connState.Version),
		).Inc()
		metrics.TLSCipher.WithLabelValues(
			tlsutil.CipherSuiteName(connState.CipherSuite),
		).Inc()
	}
}

// Per query metrics
func (cc *clientConn) addQueryMetrics(cmd byte, startTime time.Time, err error) {
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

	for _, dbName := range session.GetDBNames(vars) {
		metrics.QueryDurationHistogram.WithLabelValues(sqlType, dbName, vars.StmtCtx.ResourceGroupName).Observe(cost.Seconds())
		metrics.QueryRPCHistogram.WithLabelValues(sqlType, dbName).Observe(float64(vars.StmtCtx.GetExecDetails().RequestCount))
		if vars.StmtCtx.GetExecDetails().ScanDetail != nil {
			metrics.QueryProcessedKeyHistogram.WithLabelValues(sqlType, dbName).Observe(float64(vars.StmtCtx.GetExecDetails().ScanDetail.ProcessedKeys))
		}
	}
}

// dispatch handles client request based on command which is the first byte of the data.
// It also gets a token from server which is used to limit the concurrently handling clients.
// The most frequently used command is ComQuery.
func (cc *clientConn) dispatch(ctx context.Context, data []byte) error {
	defer func() {
		// reset killed for each request
		cc.ctx.GetSessionVars().SQLKiller.Reset()
		cc.ctx.GetSessionVars().InPacketBytes.Store(0)
		cc.ctx.GetSessionVars().OutPacketBytes.Store(0)
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
	if topsqlstate.TopProfilingEnabled() {
		rawCtx := ctx
		defer pprof.SetGoroutineLabels(rawCtx)
		sqlID := cc.ctx.GetSessionVars().SQLCPUUsages.AllocNewSQLID()
		ctx = topsql.AttachAndRegisterProcessInfo(ctx, cc.connectionID, sqlID)
	}
	if vardef.EnablePProfSQLCPU.Load() {
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
		if cc.server.StandbyController != nil {
			cc.server.StandbyController.OnConnActive()
		}
	}()

	vars := cc.ctx.GetSessionVars()
	// reset killed for each request
	vars.SQLKiller.Reset()
	vars.SQLCPUUsages.ResetCPUTimes()
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
		if cc.server.health.Load() {
			return cc.writeOK(ctx)
		}
		return servererr.ErrServerShutdown
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
	msg := fmt.Appendf(nil, "Uptime: %d  Threads: 0  Questions: 0  Slow queries: 0  Opens: 0  Flush tables: 0  Open tables: 0  Queries per second avg: 0.000",
		uptime)
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
	oldResourceGroup := cc.currentResourceGroupName()
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
	cc.moveResourceGroupCounter(oldResourceGroup)
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
	oldResourceGroup := cc.currentResourceGroupName()
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
	cc.moveResourceGroupCounter(oldResourceGroup)
	if !cc.ctx.AuthWithoutVerification(ctx, user) {
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
func (*compressionStats) GetScope(_ string) vardef.ScopeFlag {
	return vardef.ScopeSession
}

func init() {
	variable.RegisterStatistics(&compressionStats{})
}
