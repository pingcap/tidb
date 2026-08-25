// Copyright 2026 PingCAP, Inc.
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

package server

import (
	"bytes"
	"net"
	"sync"
	"sync/atomic"

	proxyprotocol "github.com/blacktear23/go-proxyprotocol"
)

var proxyProtocolV2Signature = [...]byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A}

type proxyHostState struct {
	peer           string
	rawConn        net.Conn
	trackingConn   *proxyHostTrackingConn
	headerDetected atomic.Bool
	headerParsed   atomic.Bool
}

type proxyHostTrackingListener struct {
	net.Listener
}

func (l *proxyHostTrackingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	state := &proxyHostState{rawConn: conn}
	if peerAddr := conn.RemoteAddr(); peerAddr != nil {
		state.peer = peerAddr.String()
	}
	trackingConn := &proxyHostTrackingConn{Conn: conn, state: state}
	state.trackingConn = trackingConn
	return trackingConn, nil
}

type proxyHostTrackingConn struct {
	net.Conn
	state     *proxyHostState
	firstRead sync.Once
}

func (c *proxyHostTrackingConn) Read(buffer []byte) (int, error) {
	n, err := c.Conn.Read(buffer)
	c.firstRead.Do(func() {
		c.state.headerDetected.Store(hasProxyProtocolSignature(buffer[:n]))
	})
	return n, err
}

func (c *proxyHostTrackingConn) LocalAddr() net.Addr {
	// go-proxyprotocol forwards LocalAddr, so it can carry connection state
	// through the dependency without relying on its unexported wrapper type.
	return &proxyHostTrackingAddr{
		Addr:  c.Conn.LocalAddr(),
		state: c.state,
	}
}

type proxyHostTrackingAddr struct {
	net.Addr
	state *proxyHostState
}

func hasProxyProtocolSignature(buffer []byte) bool {
	if len(buffer) >= 5 && string(buffer[:5]) == "PROXY" {
		return true
	}
	return len(buffer) >= 16 &&
		bytes.Equal(buffer[:len(proxyProtocolV2Signature)], proxyProtocolV2Signature[:]) &&
		buffer[12]&0xF0 == 0x20
}

type proxyHostResultListener struct {
	net.Listener
}

func (l *proxyHostResultListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	trackingAddr, ok := conn.LocalAddr().(*proxyHostTrackingAddr)
	if !ok {
		return conn, nil
	}
	state := trackingAddr.state
	// Sources outside allowed networks are returned unchanged by go-proxyprotocol.
	// Restore their raw connection so fallback/bypass behavior remains transparent.
	if conn == state.trackingConn {
		return state.rawConn, nil
	}
	return &proxyHostResultConn{Conn: conn, state: state}, nil
}

type proxyHostResultConn struct {
	net.Conn
	state     *proxyHostState
	firstRead sync.Once
}

func (c *proxyHostResultConn) Read(buffer []byte) (int, error) {
	n, err := c.Conn.Read(buffer)
	c.firstRead.Do(func() {
		// A signature is authoritative only when go-proxyprotocol did not reject
		// it; malformed lookalikes cannot publish the saved transport peer.
		parsed := c.state.headerDetected.Load() && !proxyprotocol.IsProxyProtocolError(err)
		c.state.headerParsed.Store(parsed)
	})
	return n, err
}

func newProxyProtocolListener(listener net.Listener, allowedIPs string, headerReadTimeout int, fallbackable bool) (net.Listener, error) {
	// Observe the bytes consumed by the lazy parser while preserving the
	// dependency's parsing, fallback, and allowed-network behavior.
	trackingListener := &proxyHostTrackingListener{Listener: listener}
	listener, err := proxyprotocol.NewLazyListener(trackingListener, allowedIPs, headerReadTimeout, fallbackable)
	if err != nil {
		return nil, err
	}
	return &proxyHostResultListener{Listener: listener}, nil
}

func (cc *clientConn) captureProxyHost(conn net.Conn) {
	if !cc.ppEnabled {
		return
	}

	trackingAddr, ok := conn.LocalAddr().(*proxyHostTrackingAddr)
	if ok {
		cc.proxyHostState = trackingAddr.state
	}
}

func (cc *clientConn) setProxyHost(ctx *TiDBContext) {
	if ctx == nil || ctx.Session == nil {
		return
	}

	// A matching wire signature becomes authoritative only after the dependency
	// parsed it successfully and the connection advanced far enough to open a session.
	if cc.proxyHostState != nil && cc.proxyHostState.headerParsed.Load() {
		cc.proxyHost = cc.proxyHostState.peer
	}

	// Store the immutable transport metadata before publishing the session context.
	// Each ProcessInfo snapshot can then carry it without changing snapshot identity.
	ctx.GetSessionVars().ProxyHost = cc.proxyHost
}

func unwrapProxyHostTrackingConn(conn net.Conn) net.Conn {
	if resultConn, ok := conn.(*proxyHostResultConn); ok {
		return resultConn.state.rawConn
	}
	if trackingConn, ok := conn.(*proxyHostTrackingConn); ok {
		return trackingConn.Conn
	}
	return conn
}
