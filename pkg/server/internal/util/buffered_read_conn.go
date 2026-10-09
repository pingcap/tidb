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
func unwrapSyscallConn(c net.Conn) syscall.Conn {
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
			return nil
		}
	}
	return nil
}
