// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.
//
// Copyright 2016 PingCAP, Inc.
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

package tikv

import (
	"bufio"
	"net"
	"time"

	"github.com/juju/errors"
)

const defaultBufSize = 4 * 1024

// Conn is a simple wrapper of net.Conn.
type Conn struct {
	addr   string
	nc     net.Conn
	closed bool
	r      *bufio.Reader
	w      *bufio.Writer
}

// NewConnection creates a Conn with dial timeout.
func NewConnection(addr string, dialTimeout time.Duration) (*Conn, error) {
	return NewConnectionWithSize(addr, dialTimeout, defaultBufSize, defaultBufSize)
}

// NewConnectionWithSize creates a Conn with dial timeout and read/write buffer size.
func NewConnectionWithSize(addr string, dialTimeout time.Duration, readSize int, writeSize int) (*Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Conn{
		addr:   addr,
		nc:     conn,
		closed: false,
		r:      bufio.NewReaderSize(conn, readSize),
		w:      bufio.NewWriterSize(conn, writeSize),
	}, nil
}

// Flush writes buffered data to net.Conn.
func (c *Conn) Flush() error {
	return c.w.Flush()
}

// Write writes data to the bufio.Writer.
func (c *Conn) Write(p []byte) (int, error) {
	return c.w.Write(p)
}

// BufioReader returns a bufio.Reader for writing.
func (c *Conn) BufioReader() *bufio.Reader {
	return c.r
}

// SetReadDeadline sets the deadline for future Read calls.
func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.nc.SetReadDeadline(t)
}

// SetWriteDeadline sets the deadline for future Write calls.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.nc.SetWriteDeadline(t)
}

// Close closes the net.Conn.
func (c *Conn) Close() {
	if c.closed {
		return
	}
	c.closed = true
	c.nc.Close()
}
