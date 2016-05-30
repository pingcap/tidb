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
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/types"
)

var defaultCapability = mysql.ClientLongPassword | mysql.ClientLongFlag |
	mysql.ClientConnectWithDB | mysql.ClientProtocol41 |
	mysql.ClientTransactions | mysql.ClientSecureConnection | mysql.ClientFoundRows

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
	cc.server.rwlock.Unlock()
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

func (cc *clientConn) readHandshakeResponse() error {
	data, err := cc.readPacket()
	if err != nil {
		return errors.Trace(err)
	}

	pos := 0
	// capability
	cc.capability = binary.LittleEndian.Uint32(data[:4])
	pos += 4
	// skip max packet size
	pos += 4
	// charset, skip, if you want to use another charset, use set names
	cc.collation = data[pos]
	pos++
	// skip reserved 23[00]
	pos += 23
	// user name
	cc.user = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
	pos += len(cc.user) + 1
	// auth length and auth
	authLen := int(data[pos])
	pos++
	auth := data[pos : pos+authLen]
	pos += authLen
	if cc.capability&mysql.ClientConnectWithDB > 0 {
		if len(data[pos:]) > 0 {
			idx := bytes.IndexByte(data[pos:], 0)
			cc.dbname = string(data[pos : pos+idx])
		}
	}
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
		if !cc.ctx.Auth(user, auth, cc.salt) {
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
				log.Error(err)
			}
			return
		}

		if err := cc.dispatch(data); err != nil {
			if terror.ErrorEqual(err, io.EOF) {
				return
			}
			log.Errorf("dispatch error %s, %s", errors.ErrorStack(err), cc)
			log.Errorf("cmd: %s", string(data[1:]))
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

	startTs := time.Now()
	defer func() {
		cc.server.releaseToken(token)
		log.Debugf("[TIME_CMD] %v %d", time.Now().Sub(startTs), cmd)
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

	data := make([]byte, 4, 16+len(m.Message))
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

func (cc *clientConn) writeEOF() error {
	data := cc.alloc.AllocWithLen(4, 9)

	data = append(data, mysql.EOFHeader)
	if cc.capability&mysql.ClientProtocol41 > 0 {
		data = append(data, dumpUint16(cc.ctx.WarningCount())...)
		data = append(data, dumpUint16(cc.ctx.Status())...)
	}

	err := cc.writePacket(data)
	return errors.Trace(err)
}

func (cc *clientConn) handleQuery(sql string) (err error) {
	startTs := time.Now()
	rs, err := cc.ctx.Execute(sql)
	if err != nil {
		return errors.Trace(err)
	}
	if rs != nil {
		err = cc.writeResultset(rs, false)
	} else {
		err = cc.writeOK()
	}
	log.Debugf("[TIME_QUERY] %v %s", time.Now().Sub(startTs), sql)
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
	if err := cc.writeEOF(); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(cc.flush())
}

func (cc *clientConn) writeResultset(rs ResultSet, binary bool) error {
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

	if err = cc.writeEOF(); err != nil {
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
				if value.Kind() == types.KindNull {
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

	err = cc.writeEOF()
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(cc.flush())
}
