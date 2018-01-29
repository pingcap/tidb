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
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tidb/util/auth"
	"github.com/pingcap/tidb/util/hack"
	log "github.com/sirupsen/logrus"
	goctx "golang.org/x/net/context"
)

// clientConn represents a connection between server and client, it maintains connection specific state,
// handles client query.
type clientConn struct {
	pkt          *packetIO         // a helper to read and write data in packet format.
	bufReadConn  *bufferedReadConn // a buffered-read net.Conn or buffered-read tls.Conn.
	tlsConn      *tls.Conn         // TLS connection, nil if not TLS.
	server       *Server           // a reference of server instance.
	capability   uint32            // client capability affects the way server handles client request.
	connectionID uint32            // atomically allocated by a global variable, unique in process scope.
	collation    uint8             // collation used by client, may be different from the collation used by database.
	user         string            // user of the client.
	dbname       string            // default database name.
	salt         []byte            // random bytes used for authentication.
	alloc        arena.Allocator   // an memory allocator for reducing memory allocation.
	lastCmd      string            // latest sql query string, currently used for logging error.
	ctx          QueryCtx          // an interface to execute sql statements.
	attrs        map[string]string // attributes parsed from client handshake response, not used for now.
	status       int32             // dispatching/reading/shutdown/waitshutdown

	// mu is used for cancelling the execution of current transaction.
	mu struct {
		sync.RWMutex
		cancelFunc goctx.CancelFunc
	}
}

const (
	connStatusDispatching int32 = iota
	connStatusReading
	connStatusShutdown     // Closed by server.
	connStatusWaitShutdown // Notified by server to close.
)

func (cc *clientConn) String() string {
	collationStr := mysql.Collations[cc.collation]
	return fmt.Sprintf("id:%d, addr:%s status:%d, collation:%s, user:%s",
		cc.connectionID, cc.bufReadConn.RemoteAddr(), cc.ctx.Status(), collationStr, cc.user,
	)
}

// handshake works like TCP handshake, but in a higher level, it first writes initial packet to client,
// during handshake, client and server negotiate compatible features and do authentication.
// After handshake, client can send sql query to server.
func (cc *clientConn) handshake() error {
	if err := cc.writeInitialHandshake(); err != nil {
		return errors.Trace(err)
	}
	if err := cc.readOptionalSSLRequestAndHandshakeResponse(); err != nil {
		err1 := cc.writeError(err)
		terror.Log(errors.Trace(err1))
		return errors.Trace(err)
	}
	data := cc.alloc.AllocWithLen(4, 32)
	data = append(data, mysql.OKHeader)
	data = append(data, 0, 0)
	if cc.capability&mysql.ClientProtocol41 > 0 {
		data = dumpUint16(data, mysql.ServerStatusAutocommit)
		data = append(data, 0, 0)
	}

	err := cc.writePacket(data)
	cc.pkt.sequence = 0
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(cc.flush())
}

func (cc *clientConn) Close() error {
	cc.server.rwlock.Lock()
	delete(cc.server.clients, cc.connectionID)
	connections := len(cc.server.clients)
	cc.server.rwlock.Unlock()
	connGauge.Set(float64(connections))
	err := cc.bufReadConn.Close()
	terror.Log(errors.Trace(err))
	if cc.ctx != nil {
		return cc.ctx.Close()
	}
	return nil
}

// writeInitialHandshake sends server version, connection ID, server capability, collation, server status
// and auth salt to the client.
func (cc *clientConn) writeInitialHandshake() error {
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
	data = dumpUint16(data, mysql.ServerStatusAutocommit)
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
	data = append(data, []byte("mysql_native_password")...)
	data = append(data, 0)
	err := cc.writePacket(data)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(cc.flush())
}

func (cc *clientConn) readPacket() ([]byte, error) {
	return cc.pkt.readPacket()
}

func (cc *clientConn) writePacket(data []byte) error {
	return cc.pkt.writePacket(data)
}

type handshakeResponse41 struct {
	Capability uint32
	Collation  uint8
	User       string
	DBName     string
	Auth       []byte
	Attrs      map[string]string
}

// parseHandshakeResponseHeader parses the common header of SSLRequest and HandshakeResponse41.
func parseHandshakeResponseHeader(packet *handshakeResponse41, data []byte) (parsedBytes int, err error) {
	// Ensure there are enough data to read:
	// http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::SSLRequest
	if len(data) < 4+4+1+23 {
		log.Errorf("Got malformed handshake response, packet data: %v", data)
		return 0, mysql.ErrMalformPacket
	}

	offset := 0
	// capability
	capability := binary.LittleEndian.Uint32(data[:4])
	packet.Capability = capability
	offset += 4
	// skip max packet size
	offset += 4
	// charset, skip, if you want to use another charset, use set names
	packet.Collation = data[offset]
	offset++
	// skip reserved 23[00]
	offset += 23

	return offset, nil
}

// parseHandshakeResponseBody parse the HandshakeResponse (except the common header part).
func parseHandshakeResponseBody(packet *handshakeResponse41, data []byte, offset int) (err error) {
	defer func() {
		// Check malformat packet cause out of range is disgusting, but don't panic!
		if r := recover(); r != nil {
			log.Errorf("handshake panic, packet data: %v", data)
			err = mysql.ErrMalformPacket
		}
	}()
	// user name
	packet.User = string(data[offset : offset+bytes.IndexByte(data[offset:], 0)])
	offset += len(packet.User) + 1

	if packet.Capability&mysql.ClientPluginAuthLenencClientData > 0 {
		// MySQL client sets the wrong capability, it will set this bit even server doesn't
		// support ClientPluginAuthLenencClientData.
		// https://github.com/mysql/mysql-server/blob/5.7/sql-common/client.c#L3478
		num, null, off := parseLengthEncodedInt(data[offset:])
		offset += off
		if !null {
			packet.Auth = data[offset : offset+int(num)]
			offset += int(num)
		}
	} else if packet.Capability&mysql.ClientSecureConnection > 0 {
		// auth length and auth
		authLen := int(data[offset])
		offset++
		packet.Auth = data[offset : offset+authLen]
		offset += authLen
	} else {
		packet.Auth = data[offset : offset+bytes.IndexByte(data[offset:], 0)]
		offset += len(packet.Auth) + 1
	}

	if packet.Capability&mysql.ClientConnectWithDB > 0 {
		if len(data[offset:]) > 0 {
			idx := bytes.IndexByte(data[offset:], 0)
			packet.DBName = string(data[offset : offset+idx])
			offset = offset + idx + 1
		}
	}

	if packet.Capability&mysql.ClientPluginAuth > 0 {
		// TODO: Support mysql.ClientPluginAuth, skip it now
		idx := bytes.IndexByte(data[offset:], 0)
		offset = offset + idx + 1
	}

	if packet.Capability&mysql.ClientConnectAtts > 0 {
		if len(data[offset:]) == 0 {
			// Defend some ill-formated packet, connection attribute is not important and can be ignored.
			return nil
		}
		if num, null, off := parseLengthEncodedInt(data[offset:]); !null {
			offset += off
			row := data[offset : offset+int(num)]
			attrs, err := parseAttrs(row)
			if err != nil {
				log.Warn("parse attrs error:", errors.ErrorStack(err))
				return nil
			}
			packet.Attrs = attrs
		}
	}

	return nil
}

func parseAttrs(data []byte) (map[string]string, error) {
	attrs := make(map[string]string)
	pos := 0
	for pos < len(data) {
		key, _, off, err := parseLengthEncodedBytes(data[pos:])
		if err != nil {
			return attrs, errors.Trace(err)
		}
		pos += off
		value, _, off, err := parseLengthEncodedBytes(data[pos:])
		if err != nil {
			return attrs, errors.Trace(err)
		}
		pos += off

		attrs[string(key)] = string(value)
	}
	return attrs, nil
}

func (cc *clientConn) readOptionalSSLRequestAndHandshakeResponse() error {
	// Read a packet. It may be a SSLRequest or HandshakeResponse.
	data, err := cc.readPacket()
	if err != nil {
		return errors.Trace(err)
	}

	var resp handshakeResponse41

	pos, err := parseHandshakeResponseHeader(&resp, data)
	if err != nil {
		return errors.Trace(err)
	}

	if (resp.Capability&mysql.ClientSSL > 0) && cc.server.tlsConfig != nil {
		// The packet is a SSLRequest, let's switch to TLS.
		if err = cc.upgradeToTLS(cc.server.tlsConfig); err != nil {
			return errors.Trace(err)
		}
		// Read the following HandshakeResponse packet.
		data, err = cc.readPacket()
		if err != nil {
			return errors.Trace(err)
		}
		pos, err = parseHandshakeResponseHeader(&resp, data)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Read the remaining part of the packet.
	if err = parseHandshakeResponseBody(&resp, data, pos); err != nil {
		return errors.Trace(err)
	}

	cc.capability = resp.Capability & cc.server.capability
	cc.user = resp.User
	cc.dbname = resp.DBName
	cc.collation = resp.Collation
	cc.attrs = resp.Attrs

	// Open session and do auth.
	var tlsStatePtr *tls.ConnectionState
	if cc.tlsConn != nil {
		tlsState := cc.tlsConn.ConnectionState()
		tlsStatePtr = &tlsState
	}
	cc.ctx, err = cc.server.driver.OpenCtx(uint64(cc.connectionID), cc.capability, cc.collation, cc.dbname, tlsStatePtr)
	if err != nil {
		return errors.Trace(err)
	}
	if !cc.server.skipAuth() {
		// Do Auth.
		addr := cc.bufReadConn.RemoteAddr().String()
		host, _, err1 := net.SplitHostPort(addr)
		if err1 != nil {
			return errors.Trace(errAccessDenied.GenByArgs(cc.user, addr, "YES"))
		}
		if !cc.ctx.Auth(&auth.UserIdentity{Username: cc.user, Hostname: host}, resp.Auth, cc.salt) {
			return errors.Trace(errAccessDenied.GenByArgs(cc.user, host, "YES"))
		}
	}
	if cc.dbname != "" {
		err = cc.useDB(goctx.Background(), cc.dbname)
		if err != nil {
			return errors.Trace(err)
		}
	}
	cc.ctx.SetSessionManager(cc.server)
	if cc.server.cfg.EnableChunk {
		cc.ctx.EnableChunk()
	}
	return nil
}

// Run reads client query and writes query result to client in for loop, if there is a panic during query handling,
// it will be recovered and log the panic error.
// This function returns and the connection is closed if there is an IO error or there is a panic.
func (cc *clientConn) Run() {
	const size = 4096
	closedOutside := false
	defer func() {
		r := recover()
		if r != nil {
			buf := make([]byte, size)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			log.Errorf("lastCmd %s, %v, %s", cc.lastCmd, r, buf)
			panicCounter.Add(1)
		}
		if !closedOutside {
			err := cc.Close()
			terror.Log(errors.Trace(err))
		}
	}()

	// Usually, client connection status changes between [dispatching] <=> [reading].
	// When some event happens, server may notify this client connection by setting
	// the status to special values, for example: kill or graceful shutdown.
	// The client connection would detect the events when it fails to change status
	// by CAS operation, it would then take some actions accordingly.
	for {
		if atomic.CompareAndSwapInt32(&cc.status, connStatusDispatching, connStatusReading) == false {
			if atomic.LoadInt32(&cc.status) == connStatusShutdown {
				closedOutside = true
			}
			return
		}

		cc.alloc.Reset()
		data, err := cc.readPacket()
		if err != nil {
			if terror.ErrorNotEqual(err, io.EOF) {
				errStack := errors.ErrorStack(err)
				if !strings.Contains(errStack, "use of closed network connection") {
					log.Errorf("[%d] read packet error, close this connection %s",
						cc.connectionID, errStack)
				}
			}
			return
		}

		if atomic.CompareAndSwapInt32(&cc.status, connStatusReading, connStatusDispatching) == false {
			if atomic.LoadInt32(&cc.status) == connStatusShutdown {
				closedOutside = true
			}
			return
		}

		startTime := time.Now()
		if err = cc.dispatch(data); err != nil {
			if terror.ErrorEqual(err, io.EOF) {
				cc.addMetrics(data[0], startTime, nil)
				return
			} else if terror.ErrResultUndetermined.Equal(err) {
				log.Errorf("[%d] result undetermined error, close this connection %s",
					cc.connectionID, errors.ErrorStack(err))
				return
			} else if terror.ErrCritical.Equal(err) {
				log.Errorf("[%d] critical error, stop the server listener %s",
					cc.connectionID, errors.ErrorStack(err))
				criticalErrorCounter.Add(1)
				select {
				case cc.server.stopListenerCh <- struct{}{}:
				default:
				}
				return
			}
			log.Warnf("[%d] dispatch error:\n%s\n%q\n%s",
				cc.connectionID, cc, queryStrForLog(string(data[1:])), errStrForLog(err))
			err1 := cc.writeError(err)
			terror.Log(errors.Trace(err1))
		}
		cc.addMetrics(data[0], startTime, err)
		cc.pkt.sequence = 0
	}
}

// ShutdownOrNotify will Shutdown this client connection, or do its best to notify.
func (cc *clientConn) ShutdownOrNotify() bool {
	if (cc.ctx.Status() & mysql.ServerStatusInTrans) > 0 {
		return false
	}
	// If the client connection status is reading, it's safe to shutdown it.
	if atomic.CompareAndSwapInt32(&cc.status, connStatusReading, connStatusShutdown) {
		return true
	}
	// If the client connection status is dispatching, we can't shutdown it immediately,
	// so set the status to WaitShutdown as a notification, the client will detect it
	// and then exit.
	atomic.StoreInt32(&cc.status, connStatusWaitShutdown)
	return false
}

func queryStrForLog(query string) string {
	const size = 4096
	if len(query) > size {
		return query[:size] + fmt.Sprintf("(len: %d)", len(query))
	}
	return query
}

func errStrForLog(err error) string {
	if kv.ErrKeyExists.Equal(err) {
		// Do not log stack for duplicated entry error.
		return err.Error()
	}
	return errors.ErrorStack(err)
}

func (cc *clientConn) addMetrics(cmd byte, startTime time.Time, err error) {
	var label string
	switch cmd {
	case mysql.ComSleep:
		label = "Sleep"
	case mysql.ComQuit:
		label = "Quit"
	case mysql.ComQuery:
		if cc.ctx.Value(context.LastExecuteDDL) != nil {
			// Don't take DDL execute time into account.
			// It's already recorded by other metrics in ddl package.
			return
		}
		label = "Query"
	case mysql.ComPing:
		label = "Ping"
	case mysql.ComInitDB:
		label = "InitDB"
	case mysql.ComFieldList:
		label = "FieldList"
	case mysql.ComStmtPrepare:
		label = "StmtPrepare"
	case mysql.ComStmtExecute:
		label = "StmtExecute"
	case mysql.ComStmtClose:
		label = "StmtClose"
	case mysql.ComStmtSendLongData:
		label = "StmtSendLongData"
	case mysql.ComStmtReset:
		label = "StmtReset"
	case mysql.ComSetOption:
		label = "SetOption"
	default:
		label = strconv.Itoa(int(cmd))
	}
	if err != nil {
		queryCounter.WithLabelValues(label, "Error").Inc()
	} else {
		queryCounter.WithLabelValues(label, "OK").Inc()
	}
	queryHistogram.Observe(time.Since(startTime).Seconds())
}

// dispatch handles client request based on command which is the first byte of the data.
// It also gets a token from server which is used to limit the concurrently handling clients.
// The most frequently used command is ComQuery.
func (cc *clientConn) dispatch(data []byte) error {
	span := opentracing.StartSpan("server.dispatch")
	goCtx := opentracing.ContextWithSpan(goctx.Background(), span)

	goCtx1, cancelFunc := goctx.WithCancel(goCtx)
	cc.mu.Lock()
	cc.mu.cancelFunc = cancelFunc
	cc.mu.Unlock()

	cmd := data[0]
	data = data[1:]
	cc.lastCmd = hack.String(data)
	token := cc.server.getToken()
	defer func() {
		cc.server.releaseToken(token)
		span.Finish()
	}()

	switch cmd {
	case mysql.ComSleep:
		// TODO: According to mysql document, this command is supposed to be used only internally.
		// So it's just a temp fix, not sure if it's done right.
		// Investigate this command and write test case later.
		return nil
	case mysql.ComQuit:
		return io.EOF
	case mysql.ComQuery: // Most frequently used command.
		// For issue 1989
		// Input payload may end with byte '\0', we didn't find related mysql document about it, but mysql
		// implementation accept that case. So trim the last '\0' here as if the payload an EOF string.
		// See http://dev.mysql.com/doc/internals/en/com-query.html
		if len(data) > 0 && data[len(data)-1] == 0 {
			data = data[:len(data)-1]
		}
		return cc.handleQuery(goCtx1, hack.String(data))
	case mysql.ComPing:
		return cc.writeOK()
	case mysql.ComInitDB:
		if err := cc.useDB(goCtx1, hack.String(data)); err != nil {
			return errors.Trace(err)
		}
		return cc.writeOK()
	case mysql.ComFieldList:
		return cc.handleFieldList(hack.String(data))
	case mysql.ComStmtPrepare:
		return cc.handleStmtPrepare(hack.String(data))
	case mysql.ComStmtExecute:
		return cc.handleStmtExecute(goCtx1, data)
	case mysql.ComStmtClose:
		return cc.handleStmtClose(data)
	case mysql.ComStmtSendLongData:
		return cc.handleStmtSendLongData(data)
	case mysql.ComStmtReset:
		return cc.handleStmtReset(data)
	case mysql.ComSetOption:
		return cc.handleSetOption(data)
	default:
		return mysql.NewErrf(mysql.ErrUnknown, "command %d not supported now", cmd)
	}
}

func (cc *clientConn) useDB(goCtx goctx.Context, db string) (err error) {
	// if input is "use `SELECT`", mysql client just send "SELECT"
	// so we add `` around db.
	_, err = cc.ctx.Execute(goCtx, "use `"+db+"`")
	if err != nil {
		return errors.Trace(err)
	}
	cc.dbname = db
	return
}

func (cc *clientConn) flush() error {
	return cc.pkt.flush()
}

func (cc *clientConn) writeOK() error {
	data := cc.alloc.AllocWithLen(4, 32)
	data = append(data, mysql.OKHeader)
	data = dumpLengthEncodedInt(data, cc.ctx.AffectedRows())
	data = dumpLengthEncodedInt(data, cc.ctx.LastInsertID())
	if cc.capability&mysql.ClientProtocol41 > 0 {
		data = dumpUint16(data, cc.ctx.Status())
		data = dumpUint16(data, cc.ctx.WarningCount())
	}

	err := cc.writePacket(data)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(cc.flush())
}

func (cc *clientConn) writeError(e error) error {
	var (
		m  *mysql.SQLError
		te *terror.Error
		ok bool
	)
	originErr := errors.Cause(e)
	if te, ok = originErr.(*terror.Error); ok {
		m = te.ToSQLError()
	} else {
		m = mysql.NewErrf(mysql.ErrUnknown, "%s", e.Error())
	}

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
		return errors.Trace(err)
	}
	return errors.Trace(cc.flush())
}

// writeEOF writes an EOF packet.
// Note this function won't flush the stream because maybe there are more
// packets following it, the "more" argument would indicates that case.
// If "more" is true, a mysql.ServerMoreResultsExists bit would be set
// in the packet.
func (cc *clientConn) writeEOF(more bool) error {
	data := cc.alloc.AllocWithLen(4, 9)

	data = append(data, mysql.EOFHeader)
	if cc.capability&mysql.ClientProtocol41 > 0 {
		data = dumpUint16(data, cc.ctx.WarningCount())
		status := cc.ctx.Status()
		if more {
			status |= mysql.ServerMoreResultsExists
		}
		data = dumpUint16(data, status)
	}

	err := cc.writePacket(data)
	return errors.Trace(err)
}

func (cc *clientConn) writeReq(filePath string) error {
	data := cc.alloc.AllocWithLen(4, 5+len(filePath))
	data = append(data, mysql.LocalInFileHeader)
	data = append(data, filePath...)

	err := cc.writePacket(data)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(cc.flush())
}

var defaultLoadDataBatchCnt uint64 = 20000

func insertDataWithCommit(goCtx goctx.Context, prevData, curData []byte, loadDataInfo *executor.LoadDataInfo) ([]byte, error) {
	var err error
	var reachLimit bool
	for {
		prevData, reachLimit, err = loadDataInfo.InsertData(prevData, curData)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !reachLimit {
			break
		}
		// Make sure that there are no retries when committing.
		if err = loadDataInfo.Ctx.RefreshTxnCtx(goCtx); err != nil {
			return nil, errors.Trace(err)
		}
		curData = prevData
		prevData = nil
	}
	return prevData, nil
}

// handleLoadData does the additional work after processing the 'load data' query.
// It sends client a file path, then reads the file content from client, inserts data into database.
func (cc *clientConn) handleLoadData(goCtx goctx.Context, loadDataInfo *executor.LoadDataInfo) error {
	// If the server handles the load data request, the client has to set the ClientLocalFiles capability.
	if cc.capability&mysql.ClientLocalFiles == 0 {
		return errNotAllowedCommand
	}
	if loadDataInfo == nil {
		return errors.New("load data info is empty")
	}

	err := cc.writeReq(loadDataInfo.Path)
	if err != nil {
		return errors.Trace(err)
	}

	var shouldBreak bool
	var prevData, curData []byte
	// TODO: Make the loadDataRowCnt settable.
	loadDataInfo.SetMaxRowsInBatch(defaultLoadDataBatchCnt)
	err = loadDataInfo.Ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	for {
		curData, err = cc.readPacket()
		if err != nil {
			if terror.ErrorNotEqual(err, io.EOF) {
				log.Error(errors.ErrorStack(err))
				break
			}
		}
		if len(curData) == 0 {
			shouldBreak = true
			if len(prevData) == 0 {
				break
			}
		}
		prevData, err = insertDataWithCommit(goCtx, prevData, curData, loadDataInfo)
		if err != nil {
			break
		}
		if shouldBreak {
			break
		}
	}

	txn := loadDataInfo.Ctx.Txn()
	terror.Log(loadDataInfo.Ctx.StmtCommit())
	if err != nil {
		if txn != nil && txn.Valid() {
			if err1 := txn.Rollback(); err1 != nil {
				log.Errorf("load data rollback failed: %v", err1)
			}
		}
		return errors.Trace(err)
	}
	return errors.Trace(txn.Commit(goCtx))
}

// handleQuery executes the sql query string and writes result set or result ok to the client.
// As the execution time of this function represents the performance of TiDB, we do time log and metrics here.
// There is a special query `load data` that does not return result, which is handled differently.
func (cc *clientConn) handleQuery(goCtx goctx.Context, sql string) (err error) {
	rs, err := cc.ctx.Execute(goCtx, sql)
	if err != nil {
		executeErrorCounter.WithLabelValues(executeErrorToLabel(err)).Inc()
		return errors.Trace(err)
	}
	if rs != nil {
		if len(rs) == 1 {
			err = cc.writeResultset(goCtx, rs[0], false, false)
		} else {
			err = cc.writeMultiResultset(goCtx, rs, false)
		}
	} else {
		loadDataInfo := cc.ctx.Value(executor.LoadDataVarKey)
		if loadDataInfo != nil {
			defer cc.ctx.SetValue(executor.LoadDataVarKey, nil)
			if err = cc.handleLoadData(goCtx, loadDataInfo.(*executor.LoadDataInfo)); err != nil {
				return errors.Trace(err)
			}
		}
		err = cc.writeOK()
	}
	return errors.Trace(err)
}

// handleFieldList returns the field list for a table.
// The sql string is composed of a table name and a terminating character \x00.
func (cc *clientConn) handleFieldList(sql string) (err error) {
	parts := strings.Split(sql, "\x00")
	columns, err := cc.ctx.FieldList(parts[0])
	if err != nil {
		return errors.Trace(err)
	}
	data := make([]byte, 4, 1024)
	for _, v := range columns {
		data = data[0:4]
		data = v.Dump(data)
		if err := cc.writePacket(data); err != nil {
			return errors.Trace(err)
		}
	}
	if err := cc.writeEOF(false); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(cc.flush())
}

// writeResultset writes a resultset.
// If binary is true, the data would be encoded in BINARY format.
// If more is true, a flag bit would be set to indicate there are more
// resultsets, it's used to support the MULTI_RESULTS capability in mysql protocol.
func (cc *clientConn) writeResultset(goCtx goctx.Context, rs ResultSet, binary bool, more bool) error {
	defer terror.Call(rs.Close)
	if cc.server.cfg.EnableChunk && rs.SupportChunk() {
		columns := rs.Columns()
		err := cc.writeColumnInfo(columns)
		if err != nil {
			return errors.Trace(err)
		}
		err = cc.writeChunks(goCtx, rs, binary, more)
		if err != nil {
			return errors.Trace(err)
		}
		return errors.Trace(cc.flush())
	}
	// We need to call Next before we get columns.
	// Otherwise, we will get incorrect columns info.
	row, err := rs.Next(goCtx)
	if err != nil {
		return errors.Trace(err)
	}
	columns := rs.Columns()
	err = cc.writeColumnInfo(columns)
	if err != nil {
		return errors.Trace(err)
	}
	data := make([]byte, 4, 1024)
	for {
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			break
		}
		data = data[0:4]
		if binary {
			data, err = dumpBinaryRow(data, columns, row)
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			data, err = dumpTextRow(data, columns, row)
			if err != nil {
				return errors.Trace(err)
			}
		}

		if err = cc.writePacket(data); err != nil {
			return errors.Trace(err)
		}
		row, err = rs.Next(goCtx)
	}

	err = cc.writeEOF(more)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(cc.flush())
}

func (cc *clientConn) writeColumnInfo(columns []*ColumnInfo) error {
	data := make([]byte, 4, 1024)
	data = dumpLengthEncodedInt(data, uint64(len(columns)))
	if err := cc.writePacket(data); err != nil {
		return errors.Trace(err)
	}
	for _, v := range columns {
		data = data[0:4]
		data = v.Dump(data)
		if err := cc.writePacket(data); err != nil {
			return errors.Trace(err)
		}
	}
	if err := cc.writeEOF(false); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (cc *clientConn) writeChunks(goCtx goctx.Context, rs ResultSet, binary bool, more bool) error {
	data := make([]byte, 4, 1024)
	chk := rs.NewChunk()
	for {
		err := rs.NextChunk(goCtx, chk)
		if err != nil {
			return errors.Trace(err)
		}
		rowCount := chk.NumRows()
		if rowCount == 0 {
			break
		}
		for i := 0; i < rowCount; i++ {
			data = data[0:4]
			if binary {
				data, err = dumpBinaryRow(data, rs.Columns(), chk.GetRow(i))
			} else {
				data, err = dumpTextRow(data, rs.Columns(), chk.GetRow(i))
			}
			if err != nil {
				return errors.Trace(err)
			}
			if err = cc.writePacket(data); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return errors.Trace(cc.writeEOF(more))
}

func (cc *clientConn) writeMultiResultset(goCtx goctx.Context, rss []ResultSet, binary bool) error {
	for _, rs := range rss {
		if err := cc.writeResultset(goCtx, rs, binary, true); err != nil {
			return errors.Trace(err)
		}
	}
	return cc.writeOK()
}

func (cc *clientConn) setConn(conn net.Conn) {
	cc.bufReadConn = newBufferedReadConn(conn)
	if cc.pkt == nil {
		cc.pkt = newPacketIO(cc.bufReadConn)
	} else {
		// Preserve current sequence number.
		cc.pkt.setBufferedReadConn(cc.bufReadConn)
	}
}

func (cc *clientConn) upgradeToTLS(tlsConfig *tls.Config) error {
	// Important: read from buffered reader instead of the original net.Conn because it may contain data we need.
	tlsConn := tls.Server(cc.bufReadConn, tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		return errors.Trace(err)
	}
	cc.setConn(tlsConn)
	cc.tlsConn = tlsConn
	return nil
}
