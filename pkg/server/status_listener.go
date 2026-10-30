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
	"net"
	"sync"
)

// statusListener closes accepted connections along with the listening socket.
// HTTP and gRPC cannot close connections that cmux is still classifying.
type statusListener struct {
	net.Listener
	mu     sync.Mutex
	conns  map[*statusConn]struct{}
	closed bool
}

func newStatusListener(listener net.Listener) *statusListener {
	return &statusListener{Listener: listener, conns: make(map[*statusConn]struct{})}
}

func (l *statusListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		_ = conn.Close()
		return nil, net.ErrClosed
	}
	c := &statusConn{Conn: conn, listener: l}
	l.conns[c] = struct{}{}
	l.mu.Unlock()
	return c, nil
}

func (l *statusListener) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	conns := l.conns
	l.conns = nil
	l.mu.Unlock()

	err := l.Listener.Close()
	// Close outside the mutex: statusConn.Close removes itself from the map.
	for conn := range conns {
		_ = conn.Close()
	}
	return err
}

type statusConn struct {
	net.Conn
	listener *statusListener
}

func (c *statusConn) Close() error {
	err := c.Conn.Close()
	c.listener.mu.Lock()
	delete(c.listener.conns, c)
	c.listener.mu.Unlock()
	return err
}
