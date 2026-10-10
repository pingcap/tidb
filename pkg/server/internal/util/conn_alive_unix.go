// Copyright 2026 PingCAP, Inc.
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

//go:build linux || darwin || freebsd

package util

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// probeConnAlive checks whether the peer of sc is still connected, using a
// non-blocking, non-consuming recv(MSG_PEEK) on the underlying file descriptor.
//
// It returns 1 if the peer is alive, 0 if the peer is known to be gone, and -1
// if liveness cannot be determined. Because the peek does not dequeue data, it
// is safe to run concurrently with the connection read loop.
func probeConnAlive(sc syscall.Conn) int {
	raw, err := sc.SyscallConn()
	if err != nil {
		return -1
	}
	alive := -1
	// Control runs the callback with the raw fd without waiting for readiness,
	// which is exactly what a poll-style liveness probe needs.
	err = raw.Control(func(fd uintptr) {
		alive = peekConnAlive(int(fd))
	})
	if err != nil {
		return -1
	}
	return alive
}

func peekConnAlive(fd int) int {
	var buf [1]byte
	n, _, err := unix.Recvfrom(fd, buf[:], unix.MSG_PEEK|unix.MSG_DONTWAIT)
	switch {
	case err == unix.EAGAIN || err == unix.EWOULDBLOCK:
		// The socket is open but no data is currently available.
		return 1
	case err == unix.EINTR:
		// The receive was interrupted by a signal before any data was
		// available, which says nothing about the peer, so liveness is unknown.
		return -1
	case err != nil:
		// The peer reset the connection or the socket hit a fatal error.
		return 0
	case n == 0:
		// recv reports EOF: the peer performed an orderly shutdown (FIN).
		return 0
	default:
		// Data is pending; MSG_PEEK leaves it in the receive buffer for Read.
		return 1
	}
}
