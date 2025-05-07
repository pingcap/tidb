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
	"io"
	"net"
	"sync"
	"time"
)

// DefaultReaderSize is the default size of bufio.Reader.
const DefaultReaderSize = 16 * 1024

// BufferedReadConn is a net.Conn compatible structure that reads from bufio.Reader.
type BufferedReadConn struct {
	net.Conn
	rb *bufio.Reader
	// `mu` is for `IsAlive()` function.
	// We use this to ensure that `SetReadDeadline` is not called concurrently.
	mu *sync.Mutex
}

// NewBufferedReadConn creates a BufferedReadConn.
func NewBufferedReadConn(conn net.Conn) *BufferedReadConn {
	return &BufferedReadConn{
		mu:   &sync.Mutex{},
		Conn: conn,
		rb:   bufio.NewReaderSize(conn, DefaultReaderSize),
	}
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
// return value < 0, means unknow
// return value = 0, means not alive
// return value = 1, means still alive
func (conn BufferedReadConn) IsAlive() int {
	if conn.mu.TryLock() {
		defer conn.mu.Unlock()
		err := conn.SetReadDeadline(time.Now().Add(30 * time.Microsecond))
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
		if err == io.EOF {
			return 0
		} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return 1
		}
	}
	return -1
}
