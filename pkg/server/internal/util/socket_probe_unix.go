// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build linux || darwin

package util

import (
	"net"
	"syscall"
)

// socketProbe is owned by one BufferedReadConn and used under its IsAlive mutex.
// Cache the callback and buffer once per connection to avoid hot-path allocation.
type socketProbe struct {
	raw      syscall.RawConn
	callback func(uintptr)
	buf      [1]byte
	state    socketProbeState
}

func newConnectionAliveProbe(conn net.Conn) func() socketProbeState {
	var socket syscall.Conn
	switch c := conn.(type) {
	case *net.TCPConn:
		socket = c
	case *net.UnixConn:
		if c.LocalAddr().Network() != "unix" {
			return nil
		}
		socket = c
	default:
		// The caller resolves known wrappers while preserving their reader.
		// Arbitrary syscall.Conn implementations may have their own buffers.
		return nil
	}
	raw, err := socket.SyscallConn()
	if err != nil {
		return nil
	}
	probe := &socketProbe{raw: raw}
	probe.callback = probe.peek
	return probe.check
}

func (p *socketProbe) check() socketProbeState {
	p.state = socketUnknown
	// Control keeps the descriptor valid only for the duration of the callback.
	// MSG_DONTWAIT avoids waiting in the runtime poller or changing socket flags.
	if err := p.raw.Control(p.callback); err != nil {
		return socketUnknown
	}
	return p.state
}

func (p *socketProbe) peek(fd uintptr) {
	n, _, err := syscall.Recvfrom(int(fd), p.buf[:], syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
	switch {
	case err == syscall.EAGAIN || err == syscall.EWOULDBLOCK:
		p.state = socketIdle
	case err == nil && n > 0:
		// Distinguish readable data from an idle socket so wrapped readers
		// get a chance to decode complete TLS records or protocol errors.
		p.state = socketReadable
	case err == nil && n == 0, err == syscall.ECONNRESET, err == syscall.ENOTCONN:
		p.state = socketClosed
	default:
		// Unexpected errors (including EINTR) use the protocol-aware fallback.
		p.state = socketUnknown
	}
}
