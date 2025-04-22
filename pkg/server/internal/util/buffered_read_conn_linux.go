// Copyright 2025 PingCAP, Inc.
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

//go:build darwin || linux

package util

import (
	"crypto/tls"
	"net"
	"syscall"
)

// IsAlive use syscall to detect the connection is alive or not.
// return value < 0, means unknow
// return value = 0, means not alive
// return value = 1, means still alive
func (conn BufferedReadConn) IsAlive() int {
	var tcp *net.TCPConn
	if tlsConn, ok := conn.Conn.(*tls.Conn); ok {
		tcp, _ = tlsConn.NetConn().(*BufferedReadConn).Conn.(*net.TCPConn)
	} else {
		tcp, _ = conn.Conn.(*net.TCPConn)
	}

	if tcp == nil {
		return -1
	}

	f, err := tcp.File()
	if err != nil {
		return -1
	}

	var n int
	b := []byte{0}
	n, _, err = syscall.Recvfrom(int(f.Fd()), b, syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
	if (n == 0 && err == nil) || err == syscall.ECONNRESET {
		return 0
	}
	return 1
}
