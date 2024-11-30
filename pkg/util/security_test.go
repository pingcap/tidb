// Copyright 2022 PingCAP, Inc.
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

package util_test

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestInvalidTLS(t *testing.T) {
	tempDir := t.TempDir()

	caPath := filepath.Join(tempDir, "ca.pem")
	_, err := util.NewTLS(caPath, "", "", "localhost", nil)
	require.Regexp(t, "could not read ca certificate:.*", err.Error())

	err = os.WriteFile(caPath, []byte("invalid ca content"), 0644)
	require.NoError(t, err)
	_, err = util.NewTLS(caPath, "", "", "localhost", nil)
	require.Regexp(t, "failed to append ca certs", err.Error())

	certPath := filepath.Join(tempDir, "test.pem")
	keyPath := filepath.Join(tempDir, "test.key")
	_, err = util.NewTLS(caPath, certPath, keyPath, "localhost", nil)
	require.Regexp(t, "could not load client key pair: open.*", err.Error())

	err = os.WriteFile(certPath, []byte("invalid cert content"), 0644)
	require.NoError(t, err)
	err = os.WriteFile(keyPath, []byte("invalid key content"), 0600)
	require.NoError(t, err)
	_, err = util.NewTLS(caPath, certPath, keyPath, "localhost", nil)
	require.Regexp(t, "could not load client key pair: tls.*", err.Error())
}

func TestVerifyCommonNameAndRotate(t *testing.T) {
	caData, certs, keys := generateCerts(t, []string{"server", "client1", "client2"})
	serverCert, serverKey := certs[0], keys[0]
	client1Cert, client1Key := certs[1], keys[1]
	client2Cert, client2Key := certs[2], keys[2]

	// only allow client1 to visit
	serverTLS, err := util.NewTLSConfig(
		util.WithCAContent(caData),
		util.WithCertAndKeyContent(serverCert, serverKey),
		util.WithVerifyCommonName([]string{"client1"}),
	)
	require.NoError(t, err)
	server, port := runServer(serverTLS, t)
	defer func() {
		server.Close()
	}()
	url := fmt.Sprintf("https://127.0.0.1:%d", port)

	clientTLS1, err := util.NewTLSConfig(
		util.WithCAContent(caData),
		util.WithCertAndKeyContent(client1Cert, client1Key),
	)
	require.NoError(t, err)
	resp, err := util.ClientWithTLS(clientTLS1).Get(url)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "This an example server", string(body))
	require.NoError(t, resp.Body.Close())

	// client1 also check server's Common Name
	clientTLS1Verify, err := util.NewTLSConfig(
		util.WithCAContent(caData),
		util.WithCertAndKeyContent(client1Cert, client1Key),
		util.WithVerifyCommonName([]string{"server"}),
	)
	require.NoError(t, err)
	resp, err = util.ClientWithTLS(clientTLS1Verify).Get(url)
	require.NoError(t, err)
	body, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "This an example server", string(body))
	require.NoError(t, resp.Body.Close())

	// client2 can't visit server
	dir := t.TempDir()
	certPath := filepath.Join(dir, "client.pem")
	keyPath := filepath.Join(dir, "client.key")
	err = os.WriteFile(certPath, client2Cert, 0600)
	require.NoError(t, err)
	err = os.WriteFile(keyPath, client2Key, 0600)
	require.NoError(t, err)

	clientTLS2, err := util.NewTLSConfig(
		util.WithCAContent(caData),
		util.WithCertAndKeyPath(certPath, keyPath),
	)
	require.NoError(t, err)
	client2 := util.ClientWithTLS(clientTLS2)
	resp, err = client2.Get(url)
	require.ErrorContains(t, err, "tls: bad certificate")
	if resp != nil {
		require.NoError(t, resp.Body.Close())
	}

	// test certificate rotation
	err = os.WriteFile(certPath, client1Cert, 0600)
	require.NoError(t, err)
	err = os.WriteFile(keyPath, client1Key, 0600)
	require.NoError(t, err)

	resp, err = client2.Get(url)
	require.NoError(t, err)
	body, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "This an example server", string(body))
	require.NoError(t, resp.Body.Close())
}

func TestTLSVersion(t *testing.T) {
	caData, certs, keys := generateCerts(t, []string{"server", "client"})
	serverCert, serverKey := certs[0], keys[0]
	clientCert, clientKey := certs[1], keys[1]

	serverTLS, err := util.NewTLSConfig(
		util.WithCAContent(caData),
		util.WithCertAndKeyContent(serverCert, serverKey),
	)
	require.NoError(t, err)
	server, port := runServer(serverTLS, t)
	defer func() {
		server.Close()
	}()
	url := fmt.Sprintf("https://127.0.0.1:%d", port)

	clientTLS1, err := util.NewTLSConfig(
		util.WithCAContent(caData),
		util.WithCertAndKeyContent(clientCert, clientKey),
	)
	require.NoError(t, err)

	type testCase struct {
		version uint16
		succ    bool
	}
	testCases := []testCase{
		{tls.VersionTLS10, false},
		{tls.VersionTLS11, false},
		{tls.VersionTLS12, true},
		{tls.VersionTLS13, true},
	}
	for _, c := range testCases {
		clientTLS1.MinVersion = c.version
		clientTLS1.MaxVersion = c.version
		resp, err := util.ClientWithTLS(clientTLS1).Get(url)
		if c.succ {
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
		} else {
			require.Error(t, err)
		}
	}

	// test with min tls version
	clientTLS2, err := util.NewTLSConfig(
		util.WithCAContent(caData),
		util.WithCertAndKeyContent(clientCert, clientKey),
		util.WithMinTLSVersion(tls.VersionTLS13),
	)
	require.NoError(t, err)
	require.Equal(t, uint16(tls.VersionTLS13), clientTLS2.MinVersion)
}

func TestCA(t *testing.T) {
	caData, certs, keys := generateCerts(t, []string{"server", "client"})
	serverCert, serverKey := certs[0], keys[0]
	clientCert, clientKey := certs[1], keys[1]

	caData2, _, _ := generateCerts(t, nil)

	serverTLS, err := util.NewTLSConfig(
		util.WithCAContent(caData),
		util.WithCertAndKeyContent(serverCert, serverKey),
	)
	require.NoError(t, err)
	server, port := runServer(serverTLS, t)
	defer func() {
		server.Close()
	}()
	url := fmt.Sprintf("https://127.0.0.1:%d", port)

	// test only CA
	clientTLS1, err := util.NewTLSConfig(
		util.WithCAContent(caData),
	)
	require.NoError(t, err)
	resp, err := util.ClientWithTLS(clientTLS1).Get(url)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "This an example server", string(body))
	require.NoError(t, resp.Body.Close())

	// test without CA
	clientTLS2, err := util.NewTLSConfig(
		util.WithCertAndKeyContent(clientCert, clientKey),
	)
	require.NoError(t, err)
	// inject CA to imitate our generated CA is a trusted CA
	clientTLS2.RootCAs = clientTLS1.RootCAs
	resp, err = util.ClientWithTLS(clientTLS2).Get(url)
	require.NoError(t, err)
	body, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "This an example server", string(body))
	require.NoError(t, resp.Body.Close())

	// test wrong CA should fail
	clientTLS3, err := util.NewTLSConfig(
		util.WithCAContent(caData2),
	)
	require.NoError(t, err)
	// inject CA to imitate our generated CA is a trusted CA
	certPool := x509.NewCertPool()
	ok := certPool.AppendCertsFromPEM(caData)
	require.True(t, ok)
	ok = certPool.AppendCertsFromPEM(caData2)
	require.True(t, ok)
	clientTLS3.RootCAs = certPool
	resp, err = util.ClientWithTLS(clientTLS3).Get(url)
	require.ErrorContains(t, err, "different CA is used")
	if resp != nil {
		require.NoError(t, resp.Body.Close())
	}
}

func handler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte("This an example server"))
}

func runServer(tlsCfg *tls.Config, t *testing.T) (*http.Server, int) {
	http.HandleFunc("/", handler)
	server := &http.Server{Addr: ":0", Handler: nil}

	conn, err := net.Listen("tcp", server.Addr)
	if err != nil {
		require.NoError(t, err)
	}
	port := conn.Addr().(*net.TCPAddr).Port

	tlsListener := tls.NewListener(conn, tlsCfg)
	go server.Serve(tlsListener)
	return server, port
}

// generateCerts returns the PEM contents of a CA certificate and some certificates and private keys per Common Name in
// commonNames.
// thanks to https://shaneutt.com/blog/golang-ca-and-signed-cert-go/.
func generateCerts(t *testing.T, commonNames []string) (caCert []byte, certs [][]byte, keys [][]byte) {
	caPrivKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(2019),
		Subject: pkix.Name{
			Organization: []string{"test"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &caPrivKey.PublicKey, caPrivKey)
	require.NoError(t, err)

	caPEM := new(bytes.Buffer)
	err = pem.Encode(caPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caBytes,
	})
	require.NoError(t, err)

	caPrivKeyPEM := new(bytes.Buffer)
	err = pem.Encode(caPrivKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(caPrivKey),
	})
	require.NoError(t, err)

	caBytes = caPEM.Bytes()

	for _, cn := range commonNames {
		cert := &x509.Certificate{
			SerialNumber: big.NewInt(1658),
			Subject: pkix.Name{
				Organization: []string{"test"},
				CommonName:   cn,
			},
			IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
			NotBefore:    time.Now(),
			NotAfter:     time.Now().AddDate(10, 0, 0),
			SubjectKeyId: []byte{1, 2, 3, 4, 6},
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
			KeyUsage:     x509.KeyUsageDigitalSignature,
		}

		certPrivKey, err2 := rsa.GenerateKey(rand.Reader, 4096)
		require.NoError(t, err2)

		certBytes, err2 := x509.CreateCertificate(rand.Reader, cert, ca, &certPrivKey.PublicKey, caPrivKey)
		require.NoError(t, err2)

		certPEM := new(bytes.Buffer)
		err2 = pem.Encode(certPEM, &pem.Block{
			Type:  "CERTIFICATE",
			Bytes: certBytes,
		})
		require.NoError(t, err2)

		certPrivKeyPEM := new(bytes.Buffer)
		err2 = pem.Encode(certPrivKeyPEM, &pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(certPrivKey),
		})
		require.NoError(t, err2)
		certs = append(certs, certPEM.Bytes())
		keys = append(keys, certPrivKeyPEM.Bytes())
	}

	return caBytes, certs, keys
}
