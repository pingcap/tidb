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
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/juju/errors"
)

// Ref: https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt .
const (
	proxyProtocolV1MaxHeaderLen = 108
)

var (
	errProxyProtocolV1HeaderInvalid = errors.New("PROXY Protocol header is invalid")
	errProxyAddressNotAllowed       = errors.New("Proxy address is not allowed")

	_ net.Conn = &proxyProtocolConn{}
)

type proxyProtocolConnBuilder struct {
	allowAll          bool
	allowedNets       []*net.IPNet
	headerReadTimeout int // Unit is second
}

func newProxyProtocolConnBuilder(allowedIPs string, headerReadTimeout int) (*proxyProtocolConnBuilder, error) {
	allowAll := false
	allowedNets := []*net.IPNet{}
	if allowedIPs == "*" {
		allowAll = true
	} else {
		for _, aip := range strings.Split(allowedIPs, ",") {
			saip := strings.TrimSpace(aip)
			_, ipnet, err := net.ParseCIDR(saip)
			if err == nil {
				allowedNets = append(allowedNets, ipnet)
				continue
			}
			psaip := fmt.Sprintf("%s/32", saip)
			_, ipnet, err = net.ParseCIDR(psaip)
			if err != nil {
				return nil, errors.Trace(err)
			}
			allowedNets = append(allowedNets, ipnet)
		}
	}
	return &proxyProtocolConnBuilder{
		allowAll:          allowAll,
		allowedNets:       allowedNets,
		headerReadTimeout: headerReadTimeout,
	}, nil
}

func (b *proxyProtocolConnBuilder) wrapConn(conn net.Conn) (*proxyProtocolConn, error) {
	ppconn := &proxyProtocolConn{
		Conn:    conn,
		builder: b,
	}
	err := ppconn.readClientAddrBehindProxy()
	return ppconn, errors.Trace(err)
}

func (b *proxyProtocolConnBuilder) checkAllowed(raddr net.Addr) bool {
	if b.allowAll {
		return true
	}
	taddr, ok := raddr.(*net.TCPAddr)
	if !ok {
		return false
	}
	cip := taddr.IP
	for _, ipnet := range b.allowedNets {
		if ipnet.Contains(cip) {
			return true
		}
	}
	return false
}

type proxyProtocolConn struct {
	net.Conn
	builder            *proxyProtocolConnBuilder
	clientIP           net.Addr
	exceedBuffer       []byte
	exceedBufferStart  int
	exceedBufferLen    int
	exceedBufferReaded bool
}

func (c *proxyProtocolConn) readClientAddrBehindProxy() error {
	connRemoteAddr := c.Conn.RemoteAddr()
	allowed := c.builder.checkAllowed(connRemoteAddr)
	if !allowed {
		return errProxyAddressNotAllowed
	}
	return c.parseHeaderV1(connRemoteAddr)
}

func (c *proxyProtocolConn) parseHeaderV1(connRemoteAddr net.Addr) error {
	buffer, err := c.readHeaderV1()
	if err != nil {
		return errors.Trace(err)
	}
	raddr, err := c.extractClientIPV1(buffer, connRemoteAddr)
	if err != nil {
		return errors.Trace(err)
	}
	c.clientIP = raddr
	return nil
}

func (c *proxyProtocolConn) extractClientIPV1(buffer []byte, connRemoteAddr net.Addr) (net.Addr, error) {
	header := string(buffer)
	parts := strings.Split(header, " ")
	if len(parts) != 6 {
		if len(parts) > 1 && parts[1] == "UNKNOWN\r\n" {
			return connRemoteAddr, nil
		}
		return nil, errProxyProtocolV1HeaderInvalid
	}
	clientIPStr := parts[2]
	clientPortStr := parts[4]
	iptype := parts[1]
	switch iptype {
	case "TCP4":
		addrStr := fmt.Sprintf("%s:%s", clientIPStr, clientPortStr)
		return net.ResolveTCPAddr("tcp4", addrStr)
	case "TCP6":
		addrStr := fmt.Sprintf("[%s]:%s", clientIPStr, clientPortStr)
		return net.ResolveTCPAddr("tcp6", addrStr)
	case "UNKNOWN":
		return connRemoteAddr, nil
	default:
		return nil, errProxyProtocolV1HeaderInvalid
	}
}

func (c *proxyProtocolConn) RemoteAddr() net.Addr {
	return c.clientIP
}

func (c *proxyProtocolConn) Read(buffer []byte) (int, error) {
	if c.exceedBufferReaded {
		return c.Conn.Read(buffer)
	}
	if c.exceedBufferLen == 0 || c.exceedBufferStart >= c.exceedBufferLen {
		c.exceedBufferReaded = true
		return c.Conn.Read(buffer)
	}

	buflen := len(buffer)
	nExceedRead := c.exceedBufferLen - c.exceedBufferStart
	// buffer length is less or equals than exceedBuffer length
	if nExceedRead >= buflen {
		copy(buffer[0:], c.exceedBuffer[c.exceedBufferStart:c.exceedBufferStart+buflen])
		c.exceedBufferStart += buflen
		return buflen, nil
	}
	// buffer length is great than exceedBuffer length
	copy(buffer[0:nExceedRead], c.exceedBuffer[c.exceedBufferStart:])
	n, err := c.Conn.Read(buffer[nExceedRead-1:])
	if err == nil {
		// If read is success set buffer start to buffer length
		// If fail make rest buffer can be read in next time
		c.exceedBufferStart = c.exceedBufferLen
		return n + nExceedRead - 1, nil
	}
	return 0, errors.Trace(err)
}

func (c *proxyProtocolConn) readHeaderV1() ([]byte, error) {
	buf := make([]byte, proxyProtocolV1MaxHeaderLen)
	// This mean all header data should be read in headerReadTimeout seconds.
	c.Conn.SetReadDeadline(time.Now().Add(time.Duration(c.builder.headerReadTimeout) * time.Second))
	// When function return clean read deadline.
	defer c.Conn.SetReadDeadline(time.Time{})
	n, err := c.Conn.Read(buf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if n >= 5 {
		if string(buf[0:5]) != "PROXY" {
			return nil, errProxyProtocolV1HeaderInvalid
		}
		pos := bytes.IndexByte(buf, byte(13))
		if pos == -1 {
			return nil, errProxyProtocolV1HeaderInvalid
		}
		if buf[pos+1] != byte(10) {
			return nil, errProxyProtocolV1HeaderInvalid
		}
		endPos := pos + 1
		if n > endPos {
			c.exceedBuffer = buf[endPos+1:]
			c.exceedBufferLen = n - endPos
		}
		return buf[0 : endPos+1], nil
	}
	return nil, errProxyProtocolV1HeaderInvalid
}
