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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"bufio"
	"crypto/tls"
	"net"
	"reflect"
	"sync"
	"time"
)

// DefaultReaderSize is the default size of bufio.Reader.
const DefaultReaderSize = 16 * 1024

type socketProbeState int

const (
	socketUnknown socketProbeState = iota - 1
	socketClosed
	socketIdle
	socketReadable
)

// BufferedReadConn is a net.Conn compatible structure that reads from bufio.Reader.
type BufferedReadConn struct {
	net.Conn
	rb *bufio.Reader
	// IsAlive calls can come from different execution checkpoints. Serialize
	// their buffer access, socket probes and fallback deadline changes.
	mu *sync.Mutex
	// Wrapped connections must still run their reader to check buffered TLS
	// records and protocol errors, even when the underlying socket is idle.
	probe          func() socketProbeState
	rawConn        net.Conn
	probeNeedsRead bool
}

// NewBufferedReadConn creates a BufferedReadConn.
func NewBufferedReadConn(conn net.Conn) *BufferedReadConn {
	rawConn, needsRead := connectionProbeSource(conn)
	return &BufferedReadConn{
		mu:             &sync.Mutex{},
		Conn:           conn,
		rb:             bufio.NewReaderSize(conn, DefaultReaderSize),
		probe:          newConnectionAliveProbe(rawConn),
		rawConn:        rawConn,
		probeNeedsRead: needsRead,
	}
}

// connectionProbeSource resolves only known wrappers, once at construction.
// Unknown wrappers retain the timed Read-based probe; their buffers are opaque.
func connectionProbeSource(conn net.Conn) (net.Conn, bool) {
	switch c := conn.(type) {
	case *BufferedReadConn:
		return c.rawConn, true
	case *tls.Conn:
		raw, _ := connectionProbeSource(c.NetConn())
		return raw, true
	}
	// go-proxyprotocol exposes its underlying connection as an exported Conn
	// field, but the wrapper type is unexported and has no accessor. This is
	// also how the server obtains Unix peer credentials through that wrapper.
	// Keep this guarded and off the hot path; a changed wrapper falls back.
	v := reflect.ValueOf(conn)
	if v.Kind() == reflect.Pointer && !v.IsNil() {
		v = v.Elem()
		if v.Kind() == reflect.Struct && v.Type().PkgPath() == "github.com/blacktear23/go-proxyprotocol" {
			field := v.FieldByName("Conn")
			if field.IsValid() && field.CanInterface() {
				if inner, ok := field.Interface().(net.Conn); ok {
					raw, _ := connectionProbeSource(inner)
					return raw, true
				}
			}
		}
	}
	return conn, false
}

// Read reads data from the connection.
func (conn BufferedReadConn) Read(b []byte) (n int, err error) {
	return conn.rb.Read(b)
}

// Peek peeks from the connection.
func (conn BufferedReadConn) Peek(n int) ([]byte, error) {
	return conn.rb.Peek(n)
}

// IsAlive detects the connection is alive or not.
// return value < 0, means unknown
// return value = 0, means not alive
// return value = 1, means still alive
func (conn BufferedReadConn) IsAlive() int {
	if conn.mu.TryLock() {
		defer conn.mu.Unlock()
		if conn.rb.Buffered() > 0 {
			return 1
		}
		idle := false
		if conn.probe != nil {
			state := conn.probe()
			if !conn.probeNeedsRead && state != socketUnknown {
				if state == socketClosed {
					return 0
				}
				return 1
			}
			idle = state == socketIdle
		}
		// An idle socket has no pending bytes or EOF. Peek must still drain
		// wrapper buffers (notably TLS close_notify), but need not wait for
		// future network traffic. An expired deadline avoids a timer.
		deadline := time.Unix(1, 0)
		if !idle {
			deadline = time.Now().Add(30 * time.Microsecond)
		}
		err := conn.SetReadDeadline(deadline)
		if err != nil {
			return -1
		}
		// nolint:errcheck
		defer conn.SetReadDeadline(time.Time{})
		// At the TCP level, a successful `Peek` operation doesn't guarantee
		// the connection remains active. However, in the MySQL protocol,
		// clients shouldn't send new data while the server is processing SQL.
		// Therefore, we can safely assume `Peek` won't intercept any data
		// during this period. Even if `Peek` does capture data, it only means
		// the liveness check might be inaccurate - this won't impact the
		// actual connection state or its operations.
		_, err = conn.Peek(1)
		if err == nil {
			return 1
		}
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return 1
		}
		// EOF, reset and TLS protocol errors all prevent the client from
		// receiving the statement result. Timeouts are the healthy case above.
		return 0
	}
	return -1
}
