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
	"net"
	"reflect"
	"syscall"
)

// DefaultReaderSize is the default size of bufio.Reader.
const DefaultReaderSize = 16 * 1024

// BufferedReadConn is a net.Conn compatible structure that reads from bufio.Reader.
type BufferedReadConn struct {
	net.Conn
	rb *bufio.Reader
}

// NewBufferedReadConn creates a BufferedReadConn.
func NewBufferedReadConn(conn net.Conn) *BufferedReadConn {
	return &BufferedReadConn{
		Conn: conn,
		rb:   bufio.NewReaderSize(conn, DefaultReaderSize),
	}
}

// Read reads data from the connection.
func (conn BufferedReadConn) Read(b []byte) (n int, err error) {
	return conn.rb.Read(b)
}

// IsAlive detects the connection is alive or not.
// return value < 0, means unknown
// return value = 0, means not alive
// return value = 1, means still alive
//
// It probes the underlying socket with a non-consuming peek instead of reading
// through the shared bufio.Reader. This matters because the probe is invoked
// from SQLKiller checkpoints on arbitrary goroutines and therefore runs
// concurrently with the connection read loop: touching rb from both goroutines
// corrupts the reader (see pingcap/tidb#71852), and even a synchronized peek
// would consume protocol bytes the read loop still needs.
func (conn BufferedReadConn) IsAlive() int {
	sc := unwrapSyscallConn(conn.Conn)
	if sc == nil {
		return -1
	}
	return probeConnAlive(sc)
}

// unwrapSyscallConn returns a syscall.Conn for c so that the peer-liveness probe
// can work on the underlying file descriptor. TiDB wraps the raw socket with
// BufferedReadConn and, when TLS is enabled, with a *tls.Conn, so unwrap those
// wrappers before giving up. It returns nil when no probed connection is found.
func unwrapSyscallConn(conn net.Conn) syscall.Conn {
	c := conn
	for depth := 0; c != nil && depth < 16; depth++ {
		if sc, ok := c.(syscall.Conn); ok {
			return sc
		}
		switch v := c.(type) {
		case interface{ NetConn() net.Conn }: // *tls.Conn
			c = v.NetConn()
		case *BufferedReadConn:
			c = v.Conn
		default:
			// Third-party wrappers such as the PROXY-protocol conn embed a
			// net.Conn without promoting SyscallConn or NetConn, so they are
			// not caught by the cases above. Unwrap the embedded field when
			// one is present; otherwise the fd is not reachable.
			c = embeddedConn(c)
			if c == nil {
				return nil
			}
		}
	}
	return nil
}

// embeddedConn returns the net.Conn embedded in c, if any. Some third-party
// net.Conn wrappers (for example the PROXY-protocol conn used when
// proxy-protocol is enabled) embed a net.Conn rather than forwarding
// SyscallConn or NetConn, so unwrapping that field is the only way to reach the
// underlying file descriptor. It returns nil when c is not such a wrapper.
func embeddedConn(c net.Conn) net.Conn {
	v := reflect.ValueOf(c)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return nil
	}
	v = v.Elem()
	if v.Kind() != reflect.Struct {
		return nil
	}
	for i := range v.NumField() {
		f := v.Field(i)
		if !f.CanInterface() {
			continue
		}
		if nc, ok := f.Interface().(net.Conn); ok && nc != nil {
			return nc
		}
	}
	return nil
}
