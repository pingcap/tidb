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
	"github.com/ngaut/deadline"
)

const (
	DefaultBufSize = 4 * 1024
)

//not thread-safe
type Conn struct {
	addr       string
	nc         net.Conn
	closed     bool
	r          *bufio.Reader
	w          *bufio.Writer
	netTimeout int //second
}

func NewConnection(addr string, netTimeout int) (*Conn, error) {
	return NewConnectionWithSize(addr, netTimeout, DefaultBufSize, DefaultBufSize)
}

func NewConnectionWithSize(addr string, netTimeout int, readSize int, writeSize int) (*Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, time.Duration(netTimeout)*time.Second)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Conn{
		addr:       addr,
		nc:         conn,
		closed:     false,
		r:          bufio.NewReaderSize(conn, readSize),
		w:          bufio.NewWriterSize(deadline.NewDeadlineWriter(conn, time.Duration(netTimeout)*time.Second), writeSize),
		netTimeout: netTimeout,
	}, nil
}

//require read to use bufio
func (c *Conn) Read(p []byte) (int, error) {
	panic("not allowed")
}

func (c *Conn) Flush() error {
	return c.w.Flush()
}

func (c *Conn) Write(p []byte) (int, error) {
	return c.w.Write(p)
}

func (c *Conn) BufioReader() *bufio.Reader {
	return c.r
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.nc.SetWriteDeadline(t)
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.nc.SetReadDeadline(t)
}

func (c *Conn) SetDeadline(t time.Time) error {
	return c.nc.SetDeadline(t)
}

func (c *Conn) Close() {
	if c.closed {
		return
	}
	c.closed = true
	c.nc.Close()
}
