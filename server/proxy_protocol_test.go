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

	. "github.com/pingcap/check"
)

type ProxyProtocolDecoderTestSuite struct{}

var _ = Suite(ProxyProtocolDecoderTestSuite{})

func (ts ProxyProtocolDecoderTestSuite) TestProxyProtocolCheckAllowed(c *C) {
	ppd, _ := newProxyProtocolDecoder("*")
	raddr, _ := net.ResolveTCPAddr("tcp4", "192.168.1.100:8080")
	c.Assert(ppd.checkAllowed(raddr), IsTrue)
	ppd, _ = newProxyProtocolDecoder("192.168.1.0/24,192.168.2.0/24")
	for _, ipstr := range []string{"192.168.1.100:8080", "192.168.2.100:8080"} {
		raddr, _ := net.ResolveTCPAddr("tcp4", ipstr)
		c.Assert(ppd.checkAllowed(raddr), IsTrue)
	}
	for _, ipstr := range []string{"192.168.3.100:8080", "192.168.4.100:8080"} {
		raddr, _ := net.ResolveTCPAddr("tcp4", ipstr)
		c.Assert(ppd.checkAllowed(raddr), IsFalse)
	}
}

func (ts ProxyProtocolDecoderTestSuite) TestProxyProtocolV1ReadHeader(c *C) {
	buffer := []byte("PROXY TCP4 192.168.1.100 192.168.1.50 5678 3306\r\nOther Data")
	expectHeader := []byte("PROXY TCP4 192.168.1.100 192.168.1.50 5678 3306\r\n")
	reader := bytes.NewBuffer(buffer)
	ppd, _ := newProxyProtocolDecoder("*")
	header, err := ppd.readHeaderV1(reader)
	c.Assert(err, IsNil)
	c.Assert(string(header), Equals, string(expectHeader))
}

func (ts ProxyProtocolDecoderTestSuite) TestProxyProtocolV1ExtractClientIP(c *C) {
	buffer := []byte("PROXY TCP4 192.168.1.100 192.168.1.50 5678 3306\r\nOther Data")
	expectClientIP := "192.168.1.100:5678"
	reader := bytes.NewBuffer(buffer)
	ppd, _ := newProxyProtocolDecoder("*")
	clientIP, err := ppd.parseHeaderV1(reader)
	c.Assert(err, IsNil)
	c.Assert(clientIP.String(), Equals, expectClientIP)
}
