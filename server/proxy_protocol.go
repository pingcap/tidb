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
)

type proxyProtocolDecoder struct {
	allowAll          bool
	allowedNets       []*net.IPNet
	headerReadTimeout int // Unit is second
}

func newProxyProtocolDecoder(allowedIPs string, headerReadTimeout int) (*proxyProtocolDecoder, error) {
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
	return &proxyProtocolDecoder{
		allowAll:          allowAll,
		allowedNets:       allowedNets,
		headerReadTimeout: headerReadTimeout,
	}, nil
}

func (d *proxyProtocolDecoder) checkAllowed(raddr net.Addr) bool {
	if d.allowAll {
		return true
	}
	taddr, ok := raddr.(*net.TCPAddr)
	if !ok {
		return false
	}
	cip := taddr.IP
	for _, ipnet := range d.allowedNets {
		if ipnet.Contains(cip) {
			return true
		}
	}
	return false
}

func (d *proxyProtocolDecoder) readClientAddrBehindProxy(conn net.Conn) (net.Addr, error) {
	connRemoteAddr := conn.RemoteAddr()
	allowed := d.checkAllowed(connRemoteAddr)
	if !allowed {
		return nil, errProxyAddressNotAllowed
	}
	return d.parseHeaderV1(conn, connRemoteAddr)
}

func (d *proxyProtocolDecoder) parseHeaderV1(conn net.Conn, connRemoteAddr net.Addr) (net.Addr, error) {
	buffer, err := d.readHeaderV1(conn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	raddr, err := d.extractClientIPV1(buffer, connRemoteAddr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return raddr, nil
}

func (d *proxyProtocolDecoder) extractClientIPV1(buffer []byte, connRemoteAddr net.Addr) (net.Addr, error) {
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

func (d *proxyProtocolDecoder) readHeaderV1(conn net.Conn) ([]byte, error) {
	buf := make([]byte, proxyProtocolV1MaxHeaderLen)
	var pre, cur byte
	var i int
	// This mean all header data should be read in headerReadTimeout seconds.
	conn.SetReadDeadline(time.Now().Add(time.Duration(d.headerReadTimeout) * time.Second))
	// When function return clean read deadline.
	defer conn.SetReadDeadline(time.Time{})
	for i = 0; i < proxyProtocolV1MaxHeaderLen; i++ {
		_, err := conn.Read(buf[i : i+1])
		if err != nil {
			return nil, err
		}
		cur = buf[i]
		if i > 0 {
			pre = buf[i-1]
		} else {
			pre = buf[i]
			if buf[i] != 0x50 {
				return nil, errProxyProtocolV1HeaderInvalid
			}
		}
		if i == 5 {
			if string(buf[0:5]) != "PROXY" {
				return nil, errProxyProtocolV1HeaderInvalid
			}
		}
		// We got \r\n so finished here
		if pre == 13 && cur == 10 {
			break
		}
	}
	if pre != 13 && cur != 10 {
		return nil, errProxyProtocolV1HeaderInvalid
	}
	return buf[0 : i+1], nil
}
