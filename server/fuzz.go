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

// Build only when actually fuzzing
// +build gofuzz

package server

import (
	"bytes"
	"net"
	"time"
)

type mockBufferConn struct {
	*bytes.Buffer
	raddr net.Addr
}

func newMockBufferConn(buffer *bytes.Buffer, raddr net.Addr) net.Conn {
	return &mockBufferConn{
		Buffer: buffer,
		raddr:  raddr,
	}
}

func (c *mockBufferConn) Close() error {
	return nil
}

func (c *mockBufferConn) RemoteAddr() net.Addr {
	if c.raddr != nil {
		return c.raddr
	}
	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:12345")
	return addr
}

func (c *mockBufferConn) LocalAddr() net.Addr {
	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:4000")
	return addr
}

func (c *mockBufferConn) SetDeadline(t time.Time) error {
	return nil
}

func (c *mockBufferConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *mockBufferConn) SetWriteDeadline(t time.Time) error {
	return nil
}

// Fuzz proxy protocol Conn with github.com/dvyukov/go-fuzz:
//
//     go-fuzz-build github.com/pingcap/tidb/server
//     go-fuzz -bin server-fuzz.zip -workdir fuzz
//
// Further input samples should go in the folder fuzz/corpus.
func Fuzz(in []byte) int {
	conn := newMockBufferConn(bytes.NewBuffer(in), nil)
	ppb, _ := newProxyProtocolConnBuilder("*", 5)
	_, err := ppb.wrapConn(conn)
	if err != nil {
		return 0
	}
	return 1
}
