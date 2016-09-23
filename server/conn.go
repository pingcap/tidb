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
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tidb/util/hack"
)

var defaultCapability = mysql.ClientLongPassword | mysql.ClientLongFlag |
	mysql.ClientConnectWithDB | mysql.ClientProtocol41 |
	mysql.ClientTransactions | mysql.ClientSecureConnection | mysql.ClientFoundRows |
	mysql.ClientMultiStatements | mysql.ClientMultiResults |
	mysql.ClientConnectAtts

type clientConn struct {
	pkg          *packetIO
	conn         net.Conn
	server       *Server
	capability   uint32
	connectionID uint32
	collation    uint8
	charset      string
	user         string
	dbname       string
	salt         []byte
	alloc        arena.Allocator
	lastCmd      string
	ctx          IContext
	attrs        map[string]string
}

func (cc *clientConn) String() string {
	return fmt.Sprintf("conn: %s, status: %d, charset: %s, user: %s, lastInsertId: %d",
		cc.conn.RemoteAddr(), cc.ctx.Status(), cc.charset, cc.user, cc.ctx.LastInsertID(),
	)
}

func (cc *clientConn) handshake() error {
	if err := cc.writeInitialHandshake(); err != nil {
		return errors.Trace(err)
	}
	if err := cc.readHandshakeResponse(); err != nil {
		cc.writeError(err)
		return errors.Trace(err)
	}
	data := cc.alloc.AllocWithLen(4, 32)
	data = append(data, mysql.OKHeader)
	data = append(data, 0, 0)
	if cc.capability&mysql.ClientProtocol41 > 0 {
		data = append(data, dumpUint16(mysql.ServerStatusAutocommit)...)
		data = append(data, 0, 0)
	}

	err := cc.writePacket(data)
	cc.pkg.sequence = 0
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
	cc.conn.Close()
	if cc.ctx != nil {
		return cc.ctx.Close()
	}
	return nil
}

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
	data = append(data, byte(defaultCapability), byte(defaultCapability>>8))
	// charset, utf-8 default
	data = append(data, uint8(mysql.DefaultCollationID))
	//status
	data = append(data, dumpUint16(mysql.ServerStatusAutocommit)...)
	// below 13 byte may not be used
	// capability flag upper 2 bytes, using default capability here
	data = append(data, byte(defaultCapability>>16), byte(defaultCapability>>24))
	// filler [0x15], for wireshark dump, value is 0x15
	data = append(data, 0x15)
	// reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	// auth-plugin-data-part-2
	data = append(data, cc.salt[8:]...)
	// filler [00]
	data = append(data, 0)
	err := cc.writePacket(data)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(cc.flush())
}

func (cc *clientConn) readPacket() ([]byte, error) {
	return cc.pkg.readPacket()
}

func (cc *clientConn) writePacket(data []byte) error {
	return cc.pkg.writePacket(data)
}

type handshakeResponse41 struct {
	Capability uint32
	Collation  uint8
	User       string
	DBName     string
	Auth       []byte
	Attrs      map[string]string
}

func handshakeResponseFromData(packet *handshakeResponse41, data []byte) error {
	pos := 0
	// capability
	capability := binary.LittleEndian.Uint32(data[:4])
	packet.Capability = capability
	pos += 4
	// skip max packet size
	pos += 4
	// charset, skip, if you want to use another charset, use set names
	packet.Collation = data[pos]
	pos++
	// skip reserved 23[00]
	pos += 23
	// user name
	packet.User = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
	pos += len(packet.User) + 1

	if capability&mysql.ClientPluginAuthLenencClientData > 0 {
		// TODO: Support mysql.ClientPluginAuthLenencClientData, skip it now
		if num, null, off := parseLengthEncodedInt(data[pos:]); !null {
			pos = pos + off + int(num)
		}
	} else if capability&mysql.ClientSecureConnection > 0 {
		// auth length and auth
		authLen := int(data[pos])
		pos++
		packet.Auth = data[pos : pos+authLen]
		pos += authLen
	} else {
		packet.Auth = data[pos : pos+bytes.IndexByte(data[pos:], 0)]
		pos += len(packet.Auth) + 1
	}

	if capability&mysql.ClientConnectWithDB > 0 {
		if len(data[pos:]) > 0 {
			idx := bytes.IndexByte(data[pos:], 0)
			packet.DBName = string(data[pos : pos+idx])
			pos = pos + idx + 1
		}
	}

	if capability&mysql.ClientPluginAuth > 0 {
		// TODO: Support mysql.ClientPluginAuth, skip it now
		idx := bytes.IndexByte(data[pos:], 0)
		pos = pos + idx + 1
	}

	if capability&mysql.ClientConnectAtts > 0 {
		if num, null, off := parseLengthEncodedInt(data[pos:]); !null {
			pos += off
			kv := data[pos : pos+int(num)]
			attrs, err := parseAttrs(kv)
			if err != nil {
				return errors.Trace(err)
			}
			packet.Attrs = attrs
			pos += int(num)
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

func (cc *clientConn) readHandshakeResponse() error {
	data, err := cc.readPacket()
	if err != nil {
		return errors.Trace(err)
	}

	var p handshakeResponse41
	if err = handshakeResponseFromData(&p, data); err != nil {
		return errors.Trace(err)
	}
	cc.capability = p.Capability & defaultCapability
	cc.user = p.User
	cc.dbname = p.DBName
	cc.collation = p.Collation
	cc.attrs = p.Attrs

	// Open session and do auth
	cc.ctx, err = cc.server.driver.OpenCtx(uint64(cc.connectionID), cc.capability, uint8(cc.collation), cc.dbname)
	if err != nil {
		cc.Close()
		return errors.Trace(err)
	}
	if !cc.server.skipAuth() {
		// Do Auth
		addr := cc.conn.RemoteAddr().String()
		host, _, err1 := net.SplitHostPort(addr)
		if err1 != nil {
			return errors.Trace(mysql.NewErr(mysql.ErrAccessDenied, cc.user, addr, "Yes"))
		}
		user := fmt.Sprintf("%s@%s", cc.user, host)
		if !cc.ctx.Auth(user, p.Auth, cc.salt) {
			return errors.Trace(mysql.NewErr(mysql.ErrAccessDenied, cc.user, host, "Yes"))
		}
	}
	return nil
}

func (cc *clientConn) Run() {
	defer func() {
		r := recover()
		if r != nil {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			log.Errorf("lastCmd %s, %v, %s", cc.lastCmd, r, buf)
		}
		cc.Close()
	}()

	for {
		cc.alloc.Reset()
		data, err := cc.readPacket()
		if err != nil {
			if terror.ErrorNotEqual(err, io.EOF) {
				log.Error(errors.ErrorStack(err))
			}
			return
		}

		if err := cc.dispatch(data); err != nil {
			if terror.ErrorEqual(err, io.EOF) {
				return
			}
			log.Warnf("dispatch error %s, %s", errors.ErrorStack(err), cc)
			log.Warnf("cmd: %s", string(data[1:]))
			cc.writeError(err)
		}

		cc.pkg.sequence = 0
	}
}

func (cc *clientConn) dispatch(data []byte) error {
	cmd := data[0]
	data = data[1:]
	cc.lastCmd = hack.String(data)

	token := cc.server.getToken()

	startTS := time.Now()
	defer func() {
		cc.server.releaseToken(token)
		log.Debugf("[TIME_CMD] %v %d", time.Since(startTS), cmd)
	}()

	switch cmd {
	case mysql.ComSleep:
		// TODO: According to mysql document, this command is supposed to be used only internally.
		// So it's just a temp fix, not sure if it's done right.
		// Investigate this command and write test case later.
		return nil
	case mysql.ComQuit:
		return io.EOF
	case mysql.ComQuery:
		return cc.handleQuery(hack.String(data))
	case mysql.ComPing:
		return cc.writeOK()
	case mysql.ComInitDB:
		log.Debug("init db", hack.String(data))
		if err := cc.useDB(hack.String(data)); err != nil {
			return errors.Trace(err)
		}
		return cc.writeOK()
	case mysql.ComFieldList:
		return cc.handleFieldList(hack.String(data))
	case mysql.ComStmtPrepare:
		return cc.handleStmtPrepare(hack.String(data))
	case mysql.ComStmtExecute:
		return cc.handleStmtExecute(data)
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

func (cc *clientConn) useDB(db string) (err error) {
	_, err = cc.ctx.Execute("use " + db)
	if err != nil {
		return errors.Trace(err)
	}
	cc.dbname = db
	return
}

func (cc *clientConn) flush() error {
	return cc.pkg.flush()
}

func (cc *clientConn) writeOK() error {
	data := cc.alloc.AllocWithLen(4, 32)
	data = append(data, mysql.OKHeader)
	data = append(data, dumpLengthEncodedInt(uint64(cc.ctx.AffectedRows()))...)
	data = append(data, dumpLengthEncodedInt(uint64(cc.ctx.LastInsertID()))...)
	if cc.capability&mysql.ClientProtocol41 > 0 {
		data = append(data, dumpUint16(cc.ctx.Status())...)
		data = append(data, dumpUint16(cc.ctx.WarningCount())...)
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
		m = mysql.NewErrf(mysql.ErrUnknown, e.Error())
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
		data = append(data, dumpUint16(cc.ctx.WarningCount())...)
		status := cc.ctx.Status()
		if more {
			status |= mysql.ServerMoreResultsExists
		}
		data = append(data, dumpUint16(cc.ctx.Status())...)
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

func (cc *clientConn) handleLoadData(loadDataInfo *executor.LoadDataInfo) error {
	var err error
	defer func() {
		cc.ctx.SetValue(executor.LoadDataVarKey, nil)
		if err == nil {
			err = cc.ctx.CommitTxn()
		}
		if err == nil {
			return
		}
		err1 := cc.ctx.RollbackTxn()
		if err1 != nil {
			log.Errorf("load data rollback failed: %v", err1)
		}
	}()

	if loadDataInfo == nil {
		err = errors.New("load data info is empty")
		return errors.Trace(err)
	}
	err = cc.writeReq(loadDataInfo.Path)
	if err != nil {
		return errors.Trace(err)
	}

	var prevData []byte
	var curData []byte
	var shouldBreak bool
	for {
		curData, err = cc.readPacket()
		if err != nil {
			if terror.ErrorNotEqual(err, io.EOF) {
				log.Error(errors.ErrorStack(err))
				return errors.Trace(err)
			}
		}
		if len(curData) == 0 {
			shouldBreak = true
			if len(prevData) == 0 {
				break
			}
		}
		prevData, err = loadDataInfo.InsertData(prevData, curData)
		if err != nil {
			return errors.Trace(err)
		}
		if shouldBreak {
			break
		}
	}

	return nil
}

func (cc *clientConn) handleQuery(sql string) (err error) {
	startTS := time.Now()
	defer func() {
		// Add metrics
		queryHistogram.Observe(time.Since(startTS).Seconds())
		label := querySucc
		if err != nil {
			label = queryFailed
		}
		queryCounter.WithLabelValues(label).Inc()
	}()

	rs, err := cc.ctx.Execute(sql)
	if err != nil {
		return errors.Trace(err)
	}
	if rs != nil {
		if len(rs) == 1 {
			err = cc.writeResultset(rs[0], false, false)
		} else {
			err = cc.writeMultiResultset(rs, false)
		}
	} else {
		loadDataInfo := cc.ctx.Value(executor.LoadDataVarKey)
		if loadDataInfo != nil {
			if err = cc.handleLoadData(loadDataInfo.(*executor.LoadDataInfo)); err != nil {
				return errors.Trace(err)
			}
		}
		err = cc.writeOK()
	}
	costTime := time.Since(startTS)
	if len(sql) > 1024 {
		sql = sql[:1024]
	}
	if costTime < time.Second {
		log.Debugf("[TIME_QUERY] %v %s", costTime, sql)
	} else {
		log.Warnf("[TIME_QUERY] %v %s", costTime, sql)
	}
	return errors.Trace(err)
}

func (cc *clientConn) handleFieldList(sql string) (err error) {
	parts := strings.Split(sql, "\x00")
	columns, err := cc.ctx.FieldList(parts[0])
	if err != nil {
		return errors.Trace(err)
	}
	data := make([]byte, 4, 1024)
	for _, v := range columns {
		data = data[0:4]
		data = append(data, v.Dump(cc.alloc)...)
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
func (cc *clientConn) writeResultset(rs ResultSet, binary bool, more bool) error {
	defer rs.Close()
	// We need to call Next before we get columns.
	// Otherwise, we will get incorrect columns info.
	row, err := rs.Next()
	if err != nil {
		return errors.Trace(err)
	}

	columns, err := rs.Columns()
	if err != nil {
		return errors.Trace(err)
	}
	columnLen := dumpLengthEncodedInt(uint64(len(columns)))
	data := cc.alloc.AllocWithLen(4, 1024)
	data = append(data, columnLen...)
	if err = cc.writePacket(data); err != nil {
		return errors.Trace(err)
	}

	for _, v := range columns {
		data = data[0:4]
		data = append(data, v.Dump(cc.alloc)...)
		if err = cc.writePacket(data); err != nil {
			return errors.Trace(err)
		}
	}

	if err = cc.writeEOF(false); err != nil {
		return errors.Trace(err)
	}

	for {
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			break
		}
		data = data[0:4]
		if binary {
			var rowData []byte
			rowData, err = dumpRowValuesBinary(cc.alloc, columns, row)
			if err != nil {
				return errors.Trace(err)
			}
			data = append(data, rowData...)
		} else {
			for i, value := range row {
				if value.IsNull() {
					data = append(data, 0xfb)
					continue
				}
				var valData []byte
				valData, err = dumpTextValue(columns[i].Type, value)
				if err != nil {
					return errors.Trace(err)
				}
				data = append(data, dumpLengthEncodedString(valData, cc.alloc)...)
			}
		}

		if err = cc.writePacket(data); err != nil {
			return errors.Trace(err)
		}
		row, err = rs.Next()
	}

	err = cc.writeEOF(more)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(cc.flush())
}

func (cc *clientConn) writeMultiResultset(rss []ResultSet, binary bool) error {
	for _, rs := range rss {
		if err := cc.writeResultset(rs, binary, true); err != nil {
			return errors.Trace(err)
		}
	}
	return cc.writeOK()
}
