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

type BytesConn struct {
	bytes.Buffer
}

func (c *BytesConn) Read(b []byte) (n int, err error) {
	return c.Read(b)
}

func (c *BytesConn) Write(b []byte) (n int, err error) {
	return 0, nil
}

func (c *BytesConn) Close() error {
	return nil
}

func (c *BytesConn) LocalAddr() net.Addr {
	return nil
}

func (c *BytesConn) RemoteAddr() net.Addr {
	return nil
}

func (c *BytesConn) SetDeadline(_ time.Time) error {
	return nil
}

func (c *BytesConn) SetReadDeadline(_ time.Time) error {
	return nil
}

func (c *BytesConn) SetWriteDeadline(_ time.Time) error {
	return nil
}
