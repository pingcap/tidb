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

package util

import (
	"bytes"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTCPConnPair(t *testing.T) (client, server net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	serverCh := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		serverCh <- c
	}()

	client, err = net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	server = <-serverCh
	require.NotNil(t, server)

	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return client, server
}

// requireRawSocketProbeSupport skips the caller on platforms that have no
// raw-socket liveness probe. The supported set mirrors the build tag on
// conn_alive_unix.go. Keying the skip on the platform rather than on IsAlive's
// return value keeps a broken probe a hard test failure instead of silently
// skipping the assertions.
func requireRawSocketProbeSupport(t *testing.T) {
	t.Helper()
	switch runtime.GOOS {
	case "linux", "darwin", "freebsd":
		return
	default:
		t.Skipf("raw socket liveness probe is not supported on %s", runtime.GOOS)
	}
}

func TestBufferedReadConnIsAlive(t *testing.T) {
	requireRawSocketProbeSupport(t)
	client, server := newTCPConnPair(t)
	c := NewBufferedReadConn(server)
	require.Equal(t, 1, c.IsAlive(), "an idle but connected peer must be alive")

	// Pending data must be reported as alive and must NOT be consumed by the
	// probe, otherwise the connection read loop would lose protocol bytes.
	payload := []byte("ping")
	_, err := client.Write(payload)
	require.NoError(t, err)
	require.Equal(t, 1, c.IsAlive(), "a peer with pending data must be alive")
	buf := make([]byte, len(payload))
	_, err = io.ReadFull(c, buf)
	require.NoError(t, err)
	require.Equal(t, payload, buf, "the probe must not consume buffered data")

	// A closed peer must be reported as not alive.
	require.NoError(t, client.Close())
	require.Eventually(t, func() bool { return c.IsAlive() == 0 }, 2*time.Second, 10*time.Millisecond,
		"a closed peer must be reported as not alive")
}

// TestBufferedReadConnConcurrentReadAndIsAlive is the regression test for
// pingcap/tidb#71852: the liveness probe used to Peek through the same
// bufio.Reader as Read, which corrupted the reader and panicked with
// "slice bounds out of range [:32768] with capacity 16384". Run with -race.
func TestBufferedReadConnConcurrentReadAndIsAlive(t *testing.T) {
	client, server := newTCPConnPair(t)
	c := NewBufferedReadConn(server)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Keep the server socket busy so the connection read loop is active.
	wg.Add(1)
	go func() {
		defer wg.Done()
		payload := bytes.Repeat([]byte("x"), 4096)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := client.Write(payload); err != nil {
				return
			}
		}
	}()

	// The connection read loop.
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 1024)
		for {
			if _, err := c.Read(buf); err != nil {
				return
			}
			select {
			case <-stop:
				return
			default:
			}
		}
	}()

	// The SQLKiller liveness probe, running concurrently with Read.
	deadline := time.Now().Add(500 * time.Millisecond)
	probes := 0
	for time.Now().Before(deadline) {
		c.IsAlive()
		probes++
	}

	close(stop)
	_ = client.Close()
	wg.Wait()
	require.Greater(t, probes, 0)
}

// netConnWrapper mirrors third-party net.Conn wrappers that embed a net.Conn
// and expose it through NetConn() net.Conn instead of promoting SyscallConn:
// *tls.Conn and, since go-proxyprotocol v1.0.8, the PROXY-protocol conn used
// when proxy-protocol is enabled. Its method set is net.Conn's plus NetConn.
type netConnWrapper struct {
	net.Conn
}

func (w *netConnWrapper) NetConn() net.Conn { return w.Conn }

// embeddedConnWrapper mirrors a net.Conn wrapper that only embeds net.Conn and
// promotes neither SyscallConn nor NetConn, so its file descriptor is not
// reachable and the unwrap must report it as unwrappable.
type embeddedConnWrapper struct {
	net.Conn
}

func TestUnwrapSyscallConn(t *testing.T) {
	_, server := newTCPConnPair(t)

	require.NotNil(t, unwrapSyscallConn(server),
		"a raw syscall.Conn must unwrap to itself")
	require.NotNil(t, unwrapSyscallConn(NewBufferedReadConn(server)),
		"a BufferedReadConn must unwrap to its embedded conn")
	require.NotNil(t, unwrapSyscallConn(&netConnWrapper{Conn: server}),
		"a wrapper exposing NetConn must unwrap to the syscall.Conn")

	// A wrapper that exposes neither SyscallConn nor NetConn hides the fd and
	// must be reported as unwrappable.
	require.Nil(t, unwrapSyscallConn(&embeddedConnWrapper{Conn: server}),
		"a wrapper with only an embedded net.Conn exposes no syscall.Conn")
}

// TestBufferedReadConnIsAliveThroughNetConnWrapper is the regression test for
// proxy-protocol connections: the liveness probe must reach the syscall.Conn
// hidden behind a wrapper that only exposes NetConn (the PROXY-protocol conn
// provides it since go-proxyprotocol v1.0.8, replacing the reflection-based
// unwrap).
func TestBufferedReadConnIsAliveThroughNetConnWrapper(t *testing.T) {
	requireRawSocketProbeSupport(t)
	client, server := newTCPConnPair(t)
	c := NewBufferedReadConn(&netConnWrapper{Conn: server})

	require.Equal(t, 1, c.IsAlive(),
		"a live peer behind a NetConn wrapper must be alive")

	require.NoError(t, client.Close())
	require.Eventually(t, func() bool { return c.IsAlive() == 0 },
		2*time.Second, 10*time.Millisecond,
		"a closed peer behind a NetConn wrapper must be reported as dead")
}
