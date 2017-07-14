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
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/ngaut/log"
)

const (
	ProxyProtocolV1MaxHeaderLen = 108
)

var (
	errProxyProtocolV1HeaderNotValid = errors.New("PROXY Protocol header not valid")
)

type ProxyProtocolEncoder struct {
	allowAll    bool
	allowedNets []*net.IPNet
}

func NewProxyProtocolEncoder(allowedIPs string) (*ProxyProtocolEncoder, error) {
	var allowAll bool = false
	var allowedNets []*net.IPNet = []*net.IPNet{}
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
				return nil, err
			}
			allowedNets = append(allowedNets, ipnet)
		}
	}
	return &ProxyProtocolEncoder{
		allowAll:    allowAll,
		allowedNets: allowedNets,
	}, nil
}

func (e *ProxyProtocolEncoder) checkAllowed(raddr net.Addr) bool {
	if e.allowAll {
		return true
	}
	taddr, ok := raddr.(*net.TCPAddr)
	if !ok {
		return false
	}
	cip := taddr.IP
	for _, ipnet := range e.allowedNets {
		if ipnet.Contains(cip) {
			return true
		}
	}
	return false
}

func (e *ProxyProtocolEncoder) GetRealClientAddr(conn net.Conn) net.Addr {
	connRemoteAddr := conn.RemoteAddr()
	allowed := e.checkAllowed(connRemoteAddr)
	if !allowed {
		return connRemoteAddr
	}
	raddr, err := e.parseHeaderV1(conn)
	if err != nil {
		log.Infof("%v", err)
		return connRemoteAddr
	}
	return raddr
}

func (e *ProxyProtocolEncoder) parseHeaderV1(conn io.Reader) (net.Addr, error) {
	buffer, err := e.readHeaderV1(conn)
	if err != nil {
		return nil, err
	}
	raddr, err := e.extractClientIPV1(buffer)
	if err != nil {
		return nil, err
	}
	return raddr, nil
}

func (e *ProxyProtocolEncoder) extractClientIPV1(buffer []byte) (net.Addr, error) {
	header := string(buffer)
	parts := strings.Split(header, " ")
	if len(parts) < 5 {
		return nil, errProxyProtocolV1HeaderNotValid
	}
	clientIPStr := parts[2]
	clientPortStr := parts[4]
	iptype := parts[1]
	addrStr := fmt.Sprintf("%s:%s", clientIPStr, clientPortStr)
	switch iptype {
	case "TCP4":
		return net.ResolveTCPAddr("tcp4", addrStr)
	case "TCP6":
		return net.ResolveTCPAddr("tcp6", addrStr)
	default:
		return net.ResolveTCPAddr("tcp", addrStr)
	}
}

func (e *ProxyProtocolEncoder) readHeaderV1(conn io.Reader) ([]byte, error) {
	buf := make([]byte, ProxyProtocolV1MaxHeaderLen)
	var pre, cur byte
	var i int
	for i = 0; i < ProxyProtocolV1MaxHeaderLen; i++ {
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
				return nil, errProxyProtocolV1HeaderNotValid
			}
		}
		if i == 5 {
			if string(buf[0:5]) != "PROXY" {
				return nil, errProxyProtocolV1HeaderNotValid
			}
		}
		// We got \r\n so finished here
		if pre == 13 && cur == 10 {
			break
		}
	}
	return buf[0 : i+1], nil
}
