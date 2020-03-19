// Copyright 2019 PingCAP, Inc.
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
	"bufio"
	"bytes"
	"net"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
)

type PacketIOTestSuite struct {
}

var _ = Suite(new(PacketIOTestSuite))

func (s *PacketIOTestSuite) TestWrite(c *C) {
	// Test write one packet
	var outBuffer bytes.Buffer
	pkt := &packetIO{bufWriter: bufio.NewWriter(&outBuffer)}
	err := pkt.writePacket([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03})
	c.Assert(err, IsNil)
	err = pkt.flush()
	c.Assert(err, IsNil)
	c.Assert(outBuffer.Bytes(), DeepEquals, []byte{0x03, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03})

	// Test write more than one packet
	outBuffer.Reset()
	largeInput := make([]byte, mysql.MaxPayloadLen+4)
	pkt = &packetIO{bufWriter: bufio.NewWriter(&outBuffer)}
	err = pkt.writePacket(largeInput)
	c.Assert(err, IsNil)
	err = pkt.flush()
	c.Assert(err, IsNil)
	res := outBuffer.Bytes()
	c.Assert(res[0], Equals, byte(0xff))
	c.Assert(res[1], Equals, byte(0xff))
	c.Assert(res[2], Equals, byte(0xff))
	c.Assert(res[3], Equals, byte(0))
}

func (s *PacketIOTestSuite) TestRead(c *C) {
	var inBuffer bytes.Buffer
	_, err := inBuffer.Write([]byte{0x01, 0x00, 0x00, 0x00, 0x01})
	c.Assert(err, IsNil)
	// Test read one packet
	brc := newBufferedReadConn(&bytesConn{inBuffer})
	pkt := newPacketIO(brc)
	bytes, err := pkt.readPacket()
	c.Assert(err, IsNil)
	c.Assert(pkt.sequence, Equals, uint8(1))
	c.Assert(bytes, DeepEquals, []byte{0x01})

	inBuffer.Reset()
	buf := make([]byte, mysql.MaxPayloadLen+9)
	buf[0] = 0xff
	buf[1] = 0xff
	buf[2] = 0xff
	buf[3] = 0
	buf[2+mysql.MaxPayloadLen] = 0x00
	buf[3+mysql.MaxPayloadLen] = 0x00
	buf[4+mysql.MaxPayloadLen] = 0x01
	buf[7+mysql.MaxPayloadLen] = 0x01
	buf[8+mysql.MaxPayloadLen] = 0x0a

	_, err = inBuffer.Write(buf)
	c.Assert(err, IsNil)
	// Test read multiple packets
	brc = newBufferedReadConn(&bytesConn{inBuffer})
	pkt = newPacketIO(brc)
	bytes, err = pkt.readPacket()
	c.Assert(err, IsNil)
	c.Assert(pkt.sequence, Equals, uint8(2))
	c.Assert(len(bytes), Equals, mysql.MaxPayloadLen+1)
	c.Assert(bytes[mysql.MaxPayloadLen], DeepEquals, byte(0x0a))
}

type bytesConn struct {
	b bytes.Buffer
}

func (c *bytesConn) Read(b []byte) (n int, err error) {
	return c.b.Read(b)
}

func (c *bytesConn) Write(b []byte) (n int, err error) {
	return 0, nil
}

func (c *bytesConn) Close() error {
	return nil
}

func (c *bytesConn) LocalAddr() net.Addr {
	return nil
}

func (c *bytesConn) RemoteAddr() net.Addr {
	return nil
}

func (c *bytesConn) SetDeadline(t time.Time) error {
	return nil
}

func (c *bytesConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *bytesConn) SetWriteDeadline(t time.Time) error {
	return nil
}
