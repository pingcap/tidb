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

package util

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"math/big"
	"net"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/blacktear23/go-proxyprotocol"
	"github.com/stretchr/testify/require"
)

func newAliveTestConn(t testing.TB, transport string) (*BufferedReadConn, net.Conn) {
	t.Helper()
	network, address := "tcp", "127.0.0.1:0"
	if transport == "unix" {
		network, address = "unix", filepath.Join(t.TempDir(), "s")
	}
	listener, err := net.Listen(network, address)
	require.NoError(t, err)
	if transport == "proxy" || transport == "tls-proxy" {
		listener, err = proxyprotocol.NewLazyListener(listener, "*", 1, false)
		require.NoError(t, err)
	}
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	client, err := net.Dial(network, listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	server, err := listener.Accept()
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })
	if transport == "proxy" || transport == "tls-proxy" {
		_, err = io.WriteString(client, "PROXY TCP4 127.0.0.1 127.0.0.1 12345 4000\r\nx")
		require.NoError(t, err)
		buf := make([]byte, 1)
		_, err = io.ReadFull(server, buf)
		require.NoError(t, err)
		require.Equal(t, "x", string(buf))
	}
	if transport == "tls" || transport == "tls-proxy" {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		cert := &x509.Certificate{
			SerialNumber: big.NewInt(1),
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}
		der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
		require.NoError(t, err)
		// Match the server's TLS-over-BufferedReadConn layering.
		tlsServer := tls.Server(NewBufferedReadConn(server), &tls.Config{
			Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		})
		tlsClient := tls.Client(client, &tls.Config{InsecureSkipVerify: true})
		require.NoError(t, server.SetDeadline(time.Now().Add(5*time.Second)))
		require.NoError(t, client.SetDeadline(time.Now().Add(5*time.Second)))
		done := make(chan error, 1)
		go func() { done <- tlsServer.Handshake() }()
		require.NoError(t, tlsClient.Handshake())
		require.NoError(t, <-done)
		require.NoError(t, server.SetDeadline(time.Time{}))
		require.NoError(t, client.SetDeadline(time.Time{}))
		server, client = tlsServer, tlsClient
	}
	if transport == "opaque" {
		server = &opaqueBufferedConn{Conn: server, reader: bufio.NewReader(server)}
	}
	return NewBufferedReadConn(server), client
}

// Unknown wrappers must never be bypassed just because they embed a net.Conn.
type opaqueBufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *opaqueBufferedConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func TestBufferedReadConnLiveness(t *testing.T) {
	for _, transport := range []string{"tcp", "unix", "tls", "proxy", "tls-proxy", "opaque"} {
		t.Run(transport, func(t *testing.T) {
			server, client := newAliveTestConn(t, transport)
			for range 10 {
				require.NotZero(t, server.IsAlive())
			}
			// A health check must not consume data or poison subsequent TLS reads.
			_, err := io.WriteString(client, "request")
			require.NoError(t, err)
			require.NotZero(t, server.IsAlive())
			buf, err := server.Peek(7)
			require.NoError(t, err)
			require.Equal(t, "request", string(buf))
			require.NoError(t, client.Close())
			// Honor data already buffered above the underlying socket's EOF.
			require.NotZero(t, server.IsAlive())
			buf = make([]byte, 7)
			_, err = io.ReadFull(server, buf)
			require.NoError(t, err)
			require.Equal(t, "request", string(buf))
			require.Eventually(t, func() bool { return server.IsAlive() == 0 }, time.Second, time.Millisecond)
		})
	}
}

func TestBufferedReadConnReset(t *testing.T) {
	server, client := newAliveTestConn(t, "tcp")
	require.NoError(t, client.(*net.TCPConn).SetLinger(0))
	require.NoError(t, client.Close())
	require.Eventually(t, func() bool { return server.IsAlive() == 0 }, time.Second, time.Millisecond)
}

func TestBufferedReadConnTLSCloseNotify(t *testing.T) {
	for _, transport := range []string{"tls", "tls-proxy"} {
		for _, buffered := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/buffered=%t", transport, buffered), func(t *testing.T) {
				server, client := newAliveTestConn(t, transport)
				// CloseWrite sends close_notify without closing the TCP connection.
				require.NoError(t, client.(*tls.Conn).CloseWrite())
				if buffered {
					// The TCP socket can be idle while the close record is already
					// buffered below TLS. Its reader still has to process the record.
					inner := server.Conn.(*tls.Conn).NetConn().(*BufferedReadConn)
					_, err := inner.Peek(1)
					require.NoError(t, err)
				}
				require.Eventually(t, func() bool { return server.IsAlive() == 0 }, time.Second, time.Millisecond)
			})
		}
	}
}

func TestBufferedReadConnTLSBufferedPlaintext(t *testing.T) {
	server, client := newAliveTestConn(t, "tls")
	_, err := io.WriteString(client, "request")
	require.NoError(t, err)
	buf := make([]byte, 1)
	_, err = io.ReadFull(server.Conn, buf)
	require.NoError(t, err)
	require.Equal(t, "r", string(buf))
	// The remaining plaintext lives inside tls.Conn, not on the TCP socket.
	require.NotZero(t, server.IsAlive())
	buf = make([]byte, 6)
	_, err = io.ReadFull(server, buf)
	require.NoError(t, err)
	require.Equal(t, "equest", string(buf))
}

func TestBufferedReadConnConcurrentProbes(t *testing.T) {
	server, _ := newAliveTestConn(t, "tcp")
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				require.NotZero(t, server.IsAlive())
			}
		})
	}
	wg.Wait()
}

func TestBufferedReadConnProbeAllocations(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("nonblocking probes are only implemented on Linux and macOS")
	}
	server, _ := newAliveTestConn(t, "tcp")
	var alive int
	allocations := testing.AllocsPerRun(100, func() { alive = server.IsAlive() })
	require.Equal(t, 1, alive)
	require.Zero(t, allocations)
}

func TestBufferedReadConnProbePreservesDeadline(t *testing.T) {
	server, client := newAliveTestConn(t, "tcp")
	if server.probe == nil {
		t.Skip("nonblocking socket probe unavailable")
	}
	require.NoError(t, server.SetReadDeadline(time.Unix(1, 0)))
	require.Equal(t, 1, server.IsAlive())
	_, err := client.Write([]byte("x"))
	require.NoError(t, err)
	// A raw healthy probe must not clear a deadline belonging to its caller.
	_, err = server.Read(make([]byte, 1))
	require.Error(t, err)
	var netErr net.Error
	require.ErrorAs(t, err, &netErr)
	require.True(t, netErr.Timeout())
}

func BenchmarkBufferedReadConnIsAlive(b *testing.B) {
	for _, transport := range []string{"tcp", "unix", "tls", "proxy", "tls-proxy"} {
		b.Run(transport, func(b *testing.B) {
			server, _ := newAliveTestConn(b, transport)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if server.IsAlive() == 0 {
					b.Fatal("healthy connection reported disconnected")
				}
			}
		})
	}
}
