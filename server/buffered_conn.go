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
	"bufio"
	"net"
)

const (
	defaultReaderSize = 16 * 1024
	defaultWriterSize = 16 * 1024
)

// bufferedConn is a net.Conn compatible structure that reads from bufio.Reader and
// optionally writes to bufio.Writer.
type bufferedConn struct {
	net.Conn
	rb *bufio.Reader
	wb *bufio.Writer
}

func (conn bufferedConn) Read(b []byte) (n int, err error) {
	return conn.rb.Read(b)
}

func (conn bufferedConn) BufferedWrite(p []byte) (nn int, err error) {
	return conn.wb.Write(p)
}

func (conn bufferedConn) Flush() error {
	return conn.wb.Flush()
}

func newBufferedConn(conn net.Conn) *bufferedConn {
	return &bufferedConn{
		Conn: conn,
		rb:   bufio.NewReaderSize(conn, defaultReaderSize),
		wb:   bufio.NewWriterSize(conn, defaultWriterSize),
	}
}
