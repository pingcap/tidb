// Copyright 2024 PingCAP, Inc.
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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	mrand "math/rand"
	"net"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/cpuprofile"
	"github.com/stretchr/testify/require"
)

func generateCertificates(t *testing.T, certPath string, keyPath string) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	notBefore := time.Now()
	notAfter := notBefore.Add(365 * 24 * time.Hour)

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber: serialNumber,
		NotBefore:    notBefore,
		NotAfter:     notAfter,

		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	require.NoError(t, err)

	certOut, err := os.Create(certPath)
	require.NoError(t, err)
	defer certOut.Close()

	err = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	require.NoError(t, err)

	keyOut, err := os.Create(keyPath)
	require.NoError(t, err)
	defer keyOut.Close()

	privBytes, err := x509.MarshalECPrivateKey(priv)
	require.NoError(t, err)

	err = pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: privBytes})
	require.NoError(t, err)
}

func TestInitialStatusServer(t *testing.T) {
	// it's required for the `/debug/pprof/profile` to work.
	err := cpuprofile.StartCPUProfiler()
	terror.MustNil(err)
	defer cpuprofile.StopCPUProfiler()

	testInitialStatusServer := func(httpClient *http.Client, url string) {
		// Wait for the server to start
		require.Eventually(t, func() bool {
			resp, err := httpClient.Get(url + "/debug/pprof/cmdline")
			if resp != nil {
				defer resp.Body.Close()
			}

			return err == nil && resp.StatusCode == http.StatusOK
		}, time.Second, time.Millisecond*100)

		// `/debug` endpoints should work
		endpoints := []string{
			"/debug/pprof/cmdline",
			"/debug/pprof/profile?seconds=1",
			"/debug/pprof/symbol",
			"/debug/pprof/trace?seconds=1",
			"/debug/pprof/goroutine",
			"/debug/pprof/",
		}

		for _, endpoint := range endpoints {
			resp, err := httpClient.Get(url + endpoint)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode, "endpoint %s should return 200", endpoint)

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NotEmpty(t, body, "endpoint %s should return non-empty body", endpoint)
			resp.Body.Close()
		}
	}

	t.Run("test start server", func(t *testing.T) {
		randPort := (mrand.Int() % 40000) + 20000

		cfg := &config.Config{
			Status: config.Status{
				StatusHost: "127.0.0.1",
				StatusPort: uint(randPort),
			},
		}

		err := StartInitialStatusServer(cfg)
		require.NoError(t, err)
		require.NotNil(t, initialStatusServer)
		require.Equal(t, fmt.Sprintf("127.0.0.1:%d", randPort), initialStatusServer.Addr)

		url := "http://" + initialStatusServer.Addr
		testInitialStatusServer(http.DefaultClient, url)

		StopInitialStatusServer()

		// Then the port should have been released
		resp, err := http.Get(url + "/debug/pprof/cmdline")
		require.Error(t, err)
		if resp != nil {
			// linter requires to close the body
			defer resp.Body.Close()
		}

		// then another server can listen to the same port
		wg := &sync.WaitGroup{}
		wg.Add(1)
		server := &http.Server{
			Addr: initialStatusServer.Addr,
		}
		go func() {
			defer wg.Done()

			err := server.ListenAndServe()
			require.Equal(t, http.ErrServerClosed, err, "unexpected error: %v", err)
		}()
		time.Sleep(time.Millisecond * 100)
		server.Close()
		wg.Wait()
	})

	t.Run("test start server with tls", func(t *testing.T) {
		randPort := (mrand.Int() % 40000) + 20000

		certPath := t.TempDir() + "/cert.pem"
		keyPath := t.TempDir() + "/key.pem"
		generateCertificates(t, certPath, keyPath)
		// to keep it simple, use the cert as CA directly
		caPool := x509.NewCertPool()
		certData, err := os.ReadFile(certPath)
		require.NoError(t, err)
		caPool.AppendCertsFromPEM(certData)

		cfg := &config.Config{
			Status: config.Status{
				StatusHost: "127.0.0.1",
				StatusPort: uint(randPort),
			},
			Security: config.Security{
				ClusterSSLCA:   certPath,
				ClusterSSLCert: certPath,
				ClusterSSLKey:  keyPath,
			},
		}

		err = StartInitialStatusServer(cfg)
		require.NoError(t, err)
		require.NotNil(t, initialStatusServer)
		require.Equal(t, fmt.Sprintf("127.0.0.1:%d", randPort), initialStatusServer.Addr)

		url := "https://" + initialStatusServer.Addr
		testInitialStatusServer(&http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					RootCAs: caPool,
				},
			},
		}, url)

		StopInitialStatusServer()
	})
}
