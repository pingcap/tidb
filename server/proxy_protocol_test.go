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

type ProxyProtocolEncoderTestSuite struct{}

var _ = Suite(ProxyProtocolEncoderTestSuite{})

func (ts ProxyProtocolEncoderTestSuite) TestProxyProtocolCheckAllowed(c *C) {
	ppe, _ := NewProxyProtocolEncoder("*")
	raddr, _ := net.ResolveTCPAddr("tcp4", "192.168.1.100:8080")
	c.Assert(ppe.checkAllowed(raddr), IsTrue)
	ppe, _ = NewProxyProtocolEncoder("192.168.1.0/24,192.168.2.0/24")
	for _, ipstr := range []string{"192.168.1.100:8080", "192.168.2.100:8080"} {
		raddr, _ := net.ResolveTCPAddr("tcp4", ipstr)
		c.Assert(ppe.checkAllowed(raddr), IsTrue)
	}
	for _, ipstr := range []string{"192.168.3.100:8080", "192.168.4.100:8080"} {
		raddr, _ := net.ResolveTCPAddr("tcp4", ipstr)
		c.Assert(ppe.checkAllowed(raddr), IsFalse)
	}
}

func (ts ProxyProtocolEncoderTestSuite) TestProxyProtocolV1ReadHeader(c *C) {
	buffer := []byte("PROXY TCP4 192.168.1.100 192.168.1.50 5678 3306\r\nOther Data")
	expectHeader := []byte("PROXY TCP4 192.168.1.100 192.168.1.50 5678 3306\r\n")
	reader := bytes.NewBuffer(buffer)
	ppe, _ := NewProxyProtocolEncoder("*")
	header, err := ppe.readHeaderV1(reader)
	c.Assert(err, IsNil)
	c.Assert(string(header), Equals, string(expectHeader))
}

func (ts ProxyProtocolEncoderTestSuite) TestProxyProtocolV1ExtractClientIP(c *C) {
	buffer := []byte("PROXY TCP4 192.168.1.100 192.168.1.50 5678 3306\r\nOther Data")
	expectClientIP := "192.168.1.100:5678"
	reader := bytes.NewBuffer(buffer)
	ppe, _ := NewProxyProtocolEncoder("*")
	clientIP, err := ppe.parseHeaderV1(reader)
	c.Assert(err, IsNil)
	c.Assert(clientIP.String(), Equals, expectClientIP)
}
