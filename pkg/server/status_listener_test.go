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

package server

import (
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

type statusReadListener struct {
	net.Listener
	reading chan struct{}
}

func (l *statusReadListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &statusReadConn{Conn: conn, reading: l.reading}, nil
}

type statusReadConn struct {
	net.Conn
	reading chan struct{}
}

func (c *statusReadConn) Read(b []byte) (int, error) {
	select {
	case c.reading <- struct{}{}:
	default:
	}
	return c.Conn.Read(b)
}

func TestStatusServerClose(t *testing.T) {
	for _, tc := range []struct {
		name   string
		tls    bool
		prefix string
	}{
		{name: "silent TCP"},
		{name: "partial HTTP", prefix: "GET"},
		{name: "silent TLS", tls: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
			cfg := util.NewTestConfig()
			cfg.Status.StatusPort = 0
			s := NewTestServer(cfg)
			require.NoError(t, s.listenStatusHTTPServer())
			if tc.tls {
				fixture := httptest.NewTLSServer(http.NotFoundHandler())
				tlsConfig := fixture.TLS.Clone()
				fixture.Close()
				listener := s.statusListener.(*statusListener)
				listener.Listener = tls.NewListener(listener.Listener, tlsConfig)
			}
			reading := make(chan struct{}, 1)
			s.statusListener = &statusReadListener{Listener: s.statusListener, reading: reading}
			servingDone := make(chan struct{})
			go func() {
				s.startStatusServerAndRPCServer(http.NewServeMux())
				close(servingDone)
			}()
			defer func() {
				s.Close()
				select {
				case <-servingDone:
				case <-time.After(5 * time.Second):
					t.Error("status serving did not finish")
				}
			}()
			conn, err := net.DialTimeout("tcp", s.statusListener.Addr().String(), 5*time.Second)
			require.NoError(t, err)
			defer conn.Close()
			if tc.prefix != "" {
				_, err = conn.Write([]byte(tc.prefix))
				require.NoError(t, err)
			}
			// Wait until cmux is reading the connection, rather than merely queued in Accept.
			select {
			case <-reading:
			case <-time.After(5 * time.Second):
				t.Fatal("status connection was not read")
			}
			closed := make(chan struct{})
			go func() {
				s.Close()
				close(closed)
			}()
			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				t.Fatal("status shutdown did not finish")
			}
			require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
			_, err = conn.Read(make([]byte, 1))
			require.Error(t, err)
			if netErr, ok := err.(net.Error); ok {
				require.False(t, netErr.Timeout(), "shutdown left the status connection open")
			}
		})
	}
}

type delayedStatusListener struct {
	net.Listener
	conn    net.Conn
	entered chan struct{}
	release chan struct{}
}

func (l *delayedStatusListener) Accept() (net.Conn, error) {
	close(l.entered)
	<-l.release
	return l.conn, nil
}

func TestStatusListenerCloseDuringAccept(t *testing.T) {
	root, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	require.NoError(t, clientConn.SetReadDeadline(time.Now().Add(5*time.Second)))
	inner := &delayedStatusListener{
		Listener: root,
		conn:     serverConn,
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
	}
	l := newStatusListener(inner)
	defer l.Close()
	acceptDone := make(chan error, 1)
	go func() {
		conn, err := l.Accept()
		if conn != nil {
			_ = conn.Close()
		}
		acceptDone <- err
	}()
	<-inner.entered
	require.NoError(t, l.Close())
	close(inner.release)
	select {
	case err := <-acceptDone:
		require.ErrorIs(t, err, net.ErrClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("Accept did not finish after shutdown")
	}
	_, err = clientConn.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, l.Close())
}
