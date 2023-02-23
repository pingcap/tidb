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
	"bufio"
	"io"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/variable"
)

const defaultWriterSize = 16 * 1024

// packetIO is a helper to read and write data in packet format.
// MySQL Packets: https://dev.mysql.com/doc/internals/en/mysql-packet.html
type packetIO struct {
	bufReadConn *bufferedReadConn
	bufWriter   *bufio.Writer
	sequence    uint8
	readTimeout time.Duration
	// maxAllowedPacket is the maximum size of one packet in readPacket.
	maxAllowedPacket uint64
	// accumulatedLength count the length of totally received 'payload' in readPacket.
	accumulatedLength uint64
}

func newPacketIO(bufReadConn *bufferedReadConn) *packetIO {
	p := &packetIO{sequence: 0}
	p.setBufferedReadConn(bufReadConn)
	p.setMaxAllowedPacket(variable.DefMaxAllowedPacket)
	return p
}

func (p *packetIO) setBufferedReadConn(bufReadConn *bufferedReadConn) {
	p.bufReadConn = bufReadConn
	p.bufWriter = bufio.NewWriterSize(bufReadConn, defaultWriterSize)
}

func (p *packetIO) setReadTimeout(timeout time.Duration) {
	p.readTimeout = timeout
}

func (p *packetIO) readOnePacket() ([]byte, error) {
	var header [4]byte
	if p.readTimeout > 0 {
		if err := p.bufReadConn.SetReadDeadline(time.Now().Add(p.readTimeout)); err != nil {
			return nil, err
		}
	}
	if _, err := io.ReadFull(p.bufReadConn, header[:]); err != nil {
		return nil, errors.Trace(err)
	}

	sequence := header[3]
	if sequence != p.sequence {
		return nil, errInvalidSequence.GenWithStack("invalid sequence %d != %d", sequence, p.sequence)
	}

	p.sequence++

	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)

	// Accumulated payload length exceeds the limit.
	if p.accumulatedLength += uint64(length); p.accumulatedLength > p.maxAllowedPacket {
		terror.Log(errNetPacketTooLarge)
		return nil, errNetPacketTooLarge
	}

	data := make([]byte, length)
	if p.readTimeout > 0 {
		if err := p.bufReadConn.SetReadDeadline(time.Now().Add(p.readTimeout)); err != nil {
			return nil, err
		}
	}
	if _, err := io.ReadFull(p.bufReadConn, data); err != nil {
		return nil, errors.Trace(err)
	}
	return data, nil
}

func (p *packetIO) setMaxAllowedPacket(maxAllowedPacket uint64) {
	p.maxAllowedPacket = maxAllowedPacket
}

func (p *packetIO) readPacket() ([]byte, error) {
	p.accumulatedLength = 0
	if p.readTimeout == 0 {
		if err := p.bufReadConn.SetReadDeadline(time.Time{}); err != nil {
			return nil, errors.Trace(err)
		}
	}
	data, err := p.readOnePacket()
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(data) < mysql.MaxPayloadLen {
		readPacketBytes.Add(float64(len(data)))
		return data, nil
	}

	// handle multi-packet
	for {
		buf, err := p.readOnePacket()
		if err != nil {
			return nil, errors.Trace(err)
		}

		data = append(data, buf...)

		if len(buf) < mysql.MaxPayloadLen {
			break
		}
	}

	readPacketBytes.Add(float64(len(data)))
	return data, nil
}

// writePacket writes data that already have header
func (p *packetIO) writePacket(data []byte) error {
	length := len(data) - 4
	writePacketBytes.Add(float64(len(data)))

	for length >= mysql.MaxPayloadLen {
		data[3] = p.sequence
		data[0] = 0xff
		data[1] = 0xff
		data[2] = 0xff

		if n, err := p.bufWriter.Write(data[:4+mysql.MaxPayloadLen]); err != nil {
			return errors.Trace(mysql.ErrBadConn)
		} else if n != (4 + mysql.MaxPayloadLen) {
			return errors.Trace(mysql.ErrBadConn)
		} else {
			p.sequence++
			length -= mysql.MaxPayloadLen
			data = data[mysql.MaxPayloadLen:]
		}
	}
	data[3] = p.sequence
	data[0] = byte(length)
	data[1] = byte(length >> 8)
	data[2] = byte(length >> 16)

	if n, err := p.bufWriter.Write(data); err != nil {
		terror.Log(errors.Trace(err))
		return errors.Trace(mysql.ErrBadConn)
	} else if n != len(data) {
		return errors.Trace(mysql.ErrBadConn)
	} else {
		p.sequence++
		return nil
	}
}

func (p *packetIO) flush() error {
	err := p.bufWriter.Flush()
	if err != nil {
		return errors.Trace(err)
	}
	return err
}
