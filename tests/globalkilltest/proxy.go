// Copyright 2020 PingCAP, Inc.
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

package globalkilltest

import (
	"net"

	log "github.com/sirupsen/logrus"
	"inet.af/tcpproxy"
)

// pdProxy used to simulate "lost connection" between TiDB and PD.
// Add "close existed connection" to `tcpproxy.Proxy`, which support closing listener only.
type pdProxy struct {
	tcpproxy.Proxy
	dialProxies []*pdDialProxy
}

// AddRoute implements the Proxy interface.
func (p *pdProxy) AddRoute(ipPort string, dest tcpproxy.Target) {
	if dp, ok := dest.(*pdDialProxy); ok {
		p.dialProxies = append(p.dialProxies, dp)
	}
	p.Proxy.AddRoute(ipPort, dest)
}

func (p *pdProxy) closeAllConnections() {
	for _, dp := range p.dialProxies {
		dp.closeAllConnections()
	}
}

// pdDialProxy add "close existed connections" to `tcpproxy.DialProxy`,
//   which support closing listener only.
type pdDialProxy struct {
	tcpproxy.DialProxy
	connections []net.Conn
}

// HandleConn implements the Target interface.
func (dp *pdDialProxy) HandleConn(src net.Conn) {
	dp.connections = append(dp.connections, tcpproxy.UnderlyingConn(src))
	dp.DialProxy.HandleConn(src)
}

func (dp *pdDialProxy) closeAllConnections() {
	for _, conn := range dp.connections {
		if err := conn.Close(); err != nil { // Notice: will close a connection twice. Ignore for test purpose.
			log.Errorf("closeAllConnections err: %v", err)
		}
	}
}

// to is shorthand way of new pdDialProxy.
func to(addr string) *pdDialProxy {
	return &pdDialProxy{
		DialProxy: tcpproxy.DialProxy{
			Addr:            addr,
			KeepAlivePeriod: -1,
		},
	}
}
