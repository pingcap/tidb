// Copyright 2023 PingCAP, Inc.
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

package testutil

import (
	"bytes"
	"net"
	"time"
)

// BytesConn is a net.Conn implementation, which reads data from a bytes.Buffer.
type BytesConn struct {
	bytes.Buffer
}

// Read implements the net.Conn interface.
func (c *BytesConn) Read(b []byte) (n int, err error) {
	return c.Buffer.Read(b)
}

// Write implements the net.Conn interface.
func (*BytesConn) Write([]byte) (n int, err error) {
	return 0, nil
}

// Close implements the net.Conn interface.
func (*BytesConn) Close() error {
	return nil
}

// LocalAddr implements the net.Conn interface.
func (*BytesConn) LocalAddr() net.Addr {
	return nil
}

// RemoteAddr implements the net.Conn interface.
func (*BytesConn) RemoteAddr() net.Addr {
	return nil
}

// SetDeadline implements the net.Conn interface.
func (*BytesConn) SetDeadline(time.Time) error {
	return nil
}

// SetReadDeadline implements the net.Conn interface.
func (*BytesConn) SetReadDeadline(time.Time) error {
	return nil
}

// SetWriteDeadline implements the net.Conn interface.
func (*BytesConn) SetWriteDeadline(time.Time) error {
	return nil
}

// GetPortFromTCPAddr gets the port from a net.Addr.
func GetPortFromTCPAddr(addr net.Addr) uint {
	return uint(addr.(*net.TCPAddr).Port)
}
