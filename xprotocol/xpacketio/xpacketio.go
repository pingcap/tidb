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

// Copyright 2017 PingCAP, Inc.
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

package xpacketio

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"strconv"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tipb/go-mysqlx"
)

const (
	defaultXReaderSize = 4 + 4*1024*1024
	defaultXWriterSize = 4 + 4*1024*1024
)

// XPacketIO is a helper to read and write data in packet format.
type XPacketIO struct {
	rb *bufio.Reader
	wb *bufio.Writer
}

// NewXPacketIO is the init function for XPacketIO
func NewXPacketIO(conn net.Conn) *XPacketIO {
	p := &XPacketIO{
		rb: bufio.NewReaderSize(conn, defaultXReaderSize),
		wb: bufio.NewWriterSize(conn, defaultXWriterSize),
	}

	return p
}

// ReadPacket reads messages. The message struct is like:
// -------------------------------------------------------------------------------
// | header                         | payload                                    |
// -------------------------------------------------------------------------------
// | 4 bytes length (little endian) | 1 byte message type | message (length - 1) |
// -------------------------------------------------------------------------------
// message needs to be decoded by protobuf.
// See: https://dev.mysql.com/doc/internals/en/x-protocol-messages-messages.html
// ReadPacket reads a full size request in X Protocol.
func (p *XPacketIO) ReadPacket() (Mysqlx.ClientMessages_Type, []byte, error) {
	payload, err := p.readPacket()
	if err != nil {
		return 0, nil, err
	}
	return Mysqlx.ClientMessages_Type(payload[0]), payload[1:], nil
}

// WritePacket is the packet writer for XPacketIO.
func (p *XPacketIO) WritePacket(msgType Mysqlx.ServerMessages_Type, message []byte) error {
	return p.writePacket(append([]byte{byte(msgType)}, message...))
}

func (p *XPacketIO) readPacket() ([]byte, error) {
	header := make([]byte, 4)

	if _, err := io.ReadFull(p.rb, header); err != nil {
		return nil, errors.Trace(err)
	}

	length := binary.LittleEndian.Uint32(header)

	data := make([]byte, length)
	n, err := io.ReadFull(p.rb, data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if max, err := strconv.Atoi(variable.GetSysVar(variable.MaxAllowedPacket).Value); err != nil {
		return nil, err
	} else if n >= max {
		return nil, errors.New("packet for query is too large. Try adjusting the 'max_allowed_packet' variable on the server")
	}
	return data, nil
}

func (p *XPacketIO) writePacket(data []byte) error {
	length := len(data)
	packet := make([]byte, 4)

	binary.LittleEndian.PutUint32(packet, uint32(length))
	packet = append(packet, data...)
	if max, err := strconv.Atoi(variable.GetSysVar(variable.MaxAllowedPacket).Value); err != nil {
		return err
	} else if len(packet) >= max {
		return errors.New("packet for query is too large. Try adjusting the 'max_allowed_packet' variable on the server")
	}
	if _, err := p.wb.Write(packet); err != nil {
		return errors.Trace(mysql.ErrBadConn)
	}
	return p.Flush()
}

// Flush flushes bufferIO.
func (p *XPacketIO) Flush() error {
	return p.wb.Flush()
}
