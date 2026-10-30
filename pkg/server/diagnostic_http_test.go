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
	"crypto/x509"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/diagnosticmode"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestDiagnosticHTTP(t *testing.T) {
	t.Cleanup(diagnosticmode.SetForTest(true))
	previous := RunInGoTest
	RunInGoTest = true
	t.Cleanup(func() { RunInGoTest = previous })
	for _, useTLS := range []bool{false, true} {
		name := "plain"
		if useTLS {
			name = "tls"
		}
		t.Run(name, func(t *testing.T) {
			cfg := config.NewConfig()
			cfg.Status.StatusHost = "127.0.0.1"
			cfg.Status.StatusPort = 0
			cfg.Status.ReportStatus = true
			transport := &http.Transport{}
			t.Cleanup(transport.CloseIdleConnections)
			scheme := "http"
			if useTLS {
				// Reuse httptest's localhost certificate, including its trusted root.
				fixture := httptest.NewTLSServer(http.NotFoundHandler())
				certificate := fixture.TLS.Certificates[0]
				fixture.Close()
				certPath := filepath.Join(t.TempDir(), "cert.pem")
				keyPath := filepath.Join(t.TempDir(), "key.pem")
				require.NoError(t, os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]}), 0600))
				key, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0600))
				cfg.Security.ClusterSSLCA = certPath
				cfg.Security.ClusterSSLCert = certPath
				cfg.Security.ClusterSSLKey = keyPath
				roots := x509.NewCertPool()
				cert, err := x509.ParseCertificate(certificate.Certificate[0])
				require.NoError(t, err)
				roots.AddCert(cert)
				transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12, NextProtos: []string{"h2", "http/1.1"}}
				scheme = "https"
			}
			s := &Server{cfg: cfg, health: atomic.NewBool(true)}
			require.NoError(t, s.startStatusHTTP())
			t.Cleanup(s.closeListener)
			require.Nil(t, s.grpcServer)
			enabled, addr := s.GetStatusServerAddr()
			require.False(t, enabled)
			require.Empty(t, addr)
			client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
			address := s.statusListener.Addr().String()
			for _, tc := range []struct {
				method, path string
				code         int
			}{
				{http.MethodGet, "/status", http.StatusOK},
				{http.MethodGet, "/metrics", http.StatusOK},
				{http.MethodGet, "/debug/pprof/", http.StatusOK},
				{http.MethodGet, "/debug/pprof/cmdline", http.StatusOK},
				{http.MethodGet, "/info", http.StatusNotFound},
				{http.MethodGet, "/settings", http.StatusNotFound},
				{http.MethodPost, "/status", http.StatusMethodNotAllowed},
				{http.MethodPost, "/debug/pprof/symbol", http.StatusMethodNotAllowed},
			} {
				req, err := http.NewRequest(tc.method, scheme+"://"+address+tc.path, nil)
				require.NoError(t, err)
				resp, err := client.Do(req)
				require.NoError(t, err, tc.path)
				_, readErr := io.Copy(io.Discard, resp.Body)
				require.NoError(t, resp.Body.Close())
				require.NoError(t, readErr)
				require.Equal(t, tc.code, resp.StatusCode, tc.path)
				if useTLS {
					require.Equal(t, "http/1.1", resp.TLS.NegotiatedProtocol)
				}
			}
			s.closeListener()
			require.Nil(t, s.statusServer.Load())
			conn, err := net.DialTimeout("tcp", address, time.Second)
			if conn != nil {
				require.NoError(t, conn.Close())
			}
			require.Error(t, err)
		})
	}
}

func TestDiagnosticStatusDisabled(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		restore := diagnosticmode.SetForTest(enabled)
		cfg := config.NewConfig()
		cfg.Status.ReportStatus = false
		s := &Server{cfg: cfg}
		require.NoError(t, s.startStatusHTTP())
		require.Nil(t, s.statusListener)
		require.False(t, s.fullStatusServerEnabled())
		cfg.Status.ReportStatus = true
		require.Equal(t, !enabled, s.fullStatusServerEnabled())
		restore()
	}
}
