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

package server

import (
	"bytes"
	"net"
	"time"

	. "github.com/pingcap/check"
)

type ProxyProtocolDecoderTestSuite struct{}

var _ = Suite(ProxyProtocolDecoderTestSuite{})

type mockBufferConn struct {
	*bytes.Buffer
}

func newMockBufferConn(buffer *bytes.Buffer) net.Conn {
	return &mockBufferConn{
		Buffer: buffer,
	}
}

func (c *mockBufferConn) Close() error {
	return nil
}

func (c *mockBufferConn) RemoteAddr() net.Addr {
	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:12345")
	return addr
}

func (c *mockBufferConn) LocalAddr() net.Addr {
	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:4000")
	return addr
}

func (c *mockBufferConn) SetDeadline(t time.Time) error {
	return nil
}

func (c *mockBufferConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *mockBufferConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func (ts ProxyProtocolDecoderTestSuite) TestProxyProtocolCheckAllowed(c *C) {
	ppd, _ := newProxyProtocolDecoder("*", 5)
	raddr, _ := net.ResolveTCPAddr("tcp4", "192.168.1.100:8080")
	c.Assert(ppd.checkAllowed(raddr), IsTrue)
	ppd, _ = newProxyProtocolDecoder("192.168.1.0/24,192.168.2.0/24", 5)
	for _, ipstr := range []string{"192.168.1.100:8080", "192.168.2.100:8080"} {
		raddr, _ := net.ResolveTCPAddr("tcp4", ipstr)
		c.Assert(ppd.checkAllowed(raddr), IsTrue)
	}
	for _, ipstr := range []string{"192.168.3.100:8080", "192.168.4.100:8080"} {
		raddr, _ := net.ResolveTCPAddr("tcp4", ipstr)
		c.Assert(ppd.checkAllowed(raddr), IsFalse)
	}
}

func (ts ProxyProtocolDecoderTestSuite) TestProxyProtocolV1ReadHeaderMustNotReadAnyDataAfterCLRF(c *C) {
	buffer := []byte("PROXY TCP4 192.168.1.100 192.168.1.50 5678 3306\r\nOther Data")
	expectHeader := []byte("PROXY TCP4 192.168.1.100 192.168.1.50 5678 3306\r\n")
	conn := newMockBufferConn(bytes.NewBuffer(buffer))
	ppd, _ := newProxyProtocolDecoder("*", 5)
	header, err := ppd.readHeaderV1(conn)
	c.Assert(err, IsNil)
	c.Assert(string(header), Equals, string(expectHeader))
	restBuf := make([]byte, 256)
	n, err := conn.Read(restBuf)
	expectedString := "Other Data"
	c.Assert(err, IsNil)
	c.Assert(n, Equals, len(expectedString))
	c.Assert(string(restBuf[0:n]), Equals, expectedString)
}

func (ts ProxyProtocolDecoderTestSuite) TestProxyProtocolV1ExtractClientIP(c *C) {
	craddr, _ := net.ResolveTCPAddr("tcp4", "192.168.1.51:8080")
	tests := []struct {
		buffer      []byte
		expectedIP  string
		expectedErr bool
	}{
		{
			buffer:      []byte("PROXY TCP4 192.168.1.100 192.168.1.50 5678 3306\r\nOther Data"),
			expectedIP:  "192.168.1.100:5678",
			expectedErr: false,
		},
		{
			buffer:      []byte("PROXY UNKNOWN\r\n"),
			expectedIP:  "192.168.1.51:8080",
			expectedErr: false,
		},
		{
			buffer:      []byte("PROXY UNKNOWN 192.168.1.100 192.168.1.50 5678 3306\r\n"),
			expectedIP:  "192.168.1.51:8080",
			expectedErr: false,
		},
		{
			buffer:      []byte("PROXY TCP 192.168.1.100 192.168.1.50 5678 3306 3307\r\n"),
			expectedIP:  "",
			expectedErr: true,
		},
		{
			buffer:      []byte("PROXY TCP4 192.168.1.100 192.168.1.50 5678 3306 jkasdjfkljaksldfjklajsdkfjsklafjldsafa"),
			expectedIP:  "",
			expectedErr: true,
		},
		{
			buffer:      []byte("PROXY TCP4 192.168.1.100 192.168.1.50 5678 3306785478934785738275489275843728954782598345"),
			expectedIP:  "",
			expectedErr: true,
		},
		{
			buffer:      []byte("PROXY TCP6 2001:0db8:85a3:0000:0000:8a2e:0370:7334 2001:0db8:85a3:0000:0000:8a2e:0390:7334 5678 3306\r\n"),
			expectedIP:  "[2001:db8:85a3::8a2e:370:7334]:5678",
			expectedErr: false,
		},
		{
			buffer:      []byte("this is a invalid header"),
			expectedIP:  "",
			expectedErr: true,
		},
		{
			buffer:      []byte("PROXY"),
			expectedIP:  "",
			expectedErr: true,
		},
		{
			buffer:      []byte("PROXY MCP3 192.168.1.100 192.168.1.50 5678 3306\r\nOther Data"),
			expectedIP:  "",
			expectedErr: true,
		},
	}

	ppd, _ := newProxyProtocolDecoder("*", 5)
	for _, t := range tests {
		conn := newMockBufferConn(bytes.NewBuffer(t.buffer))
		clientIP, err := ppd.parseHeaderV1(conn, craddr)
		if err == nil {
			if t.expectedErr {
				c.Assert(false, IsTrue, Commentf(
					"Buffer:%s\nExpect Error", string(t.buffer)))
			}
			c.Assert(clientIP.String(), Equals, t.expectedIP, Commentf(
				"Buffer:%s\nExpect: %s Got: %s", string(t.buffer), t.expectedIP, clientIP.String()))
		} else {
			if !t.expectedErr {
				c.Assert(false, IsTrue, Commentf(
					"Buffer:%s\nExpect %s But got Error: %v", string(t.buffer), t.expectedIP, err))
			}
		}
	}
}
